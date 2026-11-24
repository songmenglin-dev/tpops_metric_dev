# Wave 3 审查报告（维度：数据正确性 + 变异验证）—— wave-3-scheduling-testing

- **波次**：wave-3-scheduling-testing（任务 3.1 调度器 / 3.2 健康端点 / 3.3 入口装配 / 3.4 回归夹具 / 3.5 全量门禁）
- **base**：`356d52f`（wave-2b pass receipt 的 head）
- **head**：`3a71cb6`
- **审查范围**：本波新增/改动的生产代码（`tpops_kafka_collector/` 下的 `scheduler*.py`、`schedule.py`、`lifecycle.py`、`main.py`、`logging_setup.py`、`platform_profile.py`、`executors.py`、`ids.py`、`health.py`、`collectors/command_center.py`）与对应测试（`tests/unit/test_scheduler*.py`、`tests/integration/test_{scheduler,main,kill_restart_drill,round_latency_budget}.py`、`tests/unit/test_{platform_profile,logging_setup}.py`、`tests/regression/test_collector_coverage.py`）
- **审查维度**：数据正确性深挖 + 变异验证（合同/规格逐条核对由另一名审查者负责，本报告不重复）
- **结论**：**verdict = fail**

| 严重度 | 计数 |
|---|---|
| Critical | 0 |
| Important | 4 |
| Minor | 5 |

- **变异验证覆盖**：**15/15 完成**（简报必做 7 条 + 补充 8 条）。其中 **2 条变异后测试仍全绿**（M11、M14）→ 见「发现」第 1、2 条。
- **基线**：工作树全量 `pytest -q` = **1560 passed**；变异沙箱（`/tmp/w3-correct-mut`）还原后同样 **1560 passed**（每次变异后都还原并重跑全量确认恢复绿）。

---

## 专项深挖逐条判定

| # | 深挖项 | 判定 | 证据（file:line / 实测输出） |
|---|---|---|---|
| 1 | **水位推进的时序** —— 投递失败时水位确实没推进？批次原子推进是真的？重启后续传会不会丢？ | **部分通过** | 指标通道 ✅：`scheduler_delivery.py:96-110` 先 `produce_metrics` 成功 → 才 `remember_key` + `advance_metric`；失败分支直接 return，不推进。变异 M1（把推进挪到投递之前）→ **4 个测试红**（`test_watermark_advances_only_after_successful_delivery` / `test_failed_batch_is_retried_in_the_next_round` / `test_watermark_does_not_advance_when_delivery_fails` / `test_failed_batch_does_not_advance_the_watermark_and_is_retried`）。变异 M10（幂等键提前登记）→ **3 个测试红**。<br>告警通道 ❌：**实现正确但零守卫** —— 变异 M11（投递失败也推进告警水位）→ **全量 1561 passed，无一测试变红**。见 Important #1。<br>⚠️ 运行期不落盘：全包唯一的 `watermark.save()` 在 `lifecycle.py:158`（退出前）。见 Important #3。 |
| 2 | **幂等键** —— 跨轮持久化了？重启后仍能去重？键里确实含 instance_id？ | **部分通过** | 持久化 ✅：`watermark.py:193-202` + `ids.py:95-137`（LRU），`snapshot()` → `idempotency_keys` 进水位文件；`test_idempotency_keys_survive_a_restart` 断言落盘与恢复。<br>含 instance_id ✅：`scheduler_types.py:167-182` `metric_key(instance_id, …)` → `ids.py:70-85` `instance_id` 是键的第一段；告警键 `ids.py:48-67` 含 `instance_id`。<br>重启后去重 ✅：`tests/integration/test_kill_restart_drill.py::test_already_delivered_messages_are_not_delivered_again_after_a_restart`（真 `WatermarkStore.load()` 后重投为 0）。<br>❌ **告警侧的幂等去重无守卫**：变异 M14（删掉告警的 `seen` + `has_key` 两道去重）→ **全量 1561 passed**。实测（干净树 vs M14）：同一告警连投两轮，干净树 `ALARM BATCHES DELIVERED = 1`，M14 下 `= 2`。见 Important #2。 |
| 3 | **差分基线** —— 作用域带 instance_id？跨 master 不互相覆盖？重启后能算出速率吗？ | **通过** | 作用域 = `f'{instance_id}:{node_id}'`：`collectors/command_center.py:306-322`（`_delta_store` 视图缓存键 + `ScopedCounterDeltaStore` 作用域）；`mapping/rules_deltas.py:35-44` `scoped_dimension_key` 把作用域并进维度键（键仍是二元组，水位文件形状不变）。<br>跨 master 隔离 ✅：变异 M3（`scoped_dimension_key` 去掉作用域）→ **9 个测试红**，含**回归层** `tests/regression/test_collector_coverage.py::test_every_master_computes_its_own_rate_in_the_regression_layer`（M-N4 补测到位）。<br>跨实例隔离 ✅：`tests/integration/test_collector_command_center_multi_instance.py::test_two_instances_sharing_node_id_keep_separate_baselines`（共用 node_id 的两实例速率 10.0 / 60.0 互不串扰）。<br>重启后能算速率 ✅：`scheduler.py:292-309` `export_baselines_into`（**整体搬运**）+ `scheduler.py:311-315` `import_baselines_from`；`test_rates_survive_restore_from_watermark` 断言恢复后仍算出速率；变异 M13（完全不恢复基线）→ **2 个测试红**。<br>⚠️ 但简报必做变异 #4「`import_baselines_from` → `put_baseline`」**测不出来**（见变异表 M4 与 Minor #4）。 |
| 4 | **并发** —— 「组 × 实例两维」是真的两维吗？线程池用对了吗？有没有共享可变状态导致串扰？ | **通过** | 两维 ✅：`scheduler.py:206-212` `_build_tasks` = 通道 × 实例笛卡尔积；`scheduler.py:216-249` 一个 `ThreadPoolExecutor` 收全部任务（不是每通道一个池）。守卫用 `threading.Barrier`（确定性）：`tests/unit/test_scheduler.py::test_due_groups_run_concurrently_not_serially`（3 方）、`::test_two_dimensions_are_concurrent_groups_and_instances`（6 方）、`tests/integration/test_round_latency_budget.py::test_a_round_runs_the_channels_concurrently_not_serially`。变异 M5（改成组内串行）→ **3 个测试红**（`BrokenBarrierError`）。<br>共享可变状态审查：采集在 worker 线程、**投递与水位在主线程**（`scheduler.py:266-288` 顺序循环）→ `WatermarkStore` / `IdempotencyKeyLRU` 无并发写风险（`ids.py:95-137` 的 LRU 未加锁但只在主线程用）。跨线程共享的只有 `CommandCenterCollector` 的 `_deltas`（基线字典，键含 `instance:node` → 不串扰）、`_scoped_deltas`（视图缓存，视图无状态）、`_page_size`（并发缩页只影响性能，不影响正确性）。`LoadedMapping` 无可变缓存（`mapping/loader.py` 无 `self._x[...] =` 写入）。**未发现串扰**。 |
| 5 | **优雅退出** —— 「超时 30 秒放弃等待」真的实现了吗？退出码 0？水位文件完整？ | **通过** | 实现 ✅：`lifecycle.py:132-153` `_wait_for_tick` —— 首次观测到 shutdown 时记 `shutdown_at`，`remaining = deadline_seconds - (now - shutdown_at)`，`<= 0` 即 `return False`（放弃等待）。`lifecycle.py:49` `SHUTDOWN_DEADLINE_SECONDS = 30`。**不是靠测试里的假超时蒙混**：生产默认常量就是 30，测试注入 0.3 秒只是为了不真等（`tests/integration/test_main.py::test_default_shutdown_deadline_is_thirty_seconds` 断言常量本身）。<br>退出码 0 ✅：`lifecycle.py:188-189` 返回 `EXIT_OK`；变异 M6（`EXIT_OK = 7`）→ **4 个测试红**（含真进程 `test_real_process_receives_sigterm_and_exits_zero`）。<br>水位文件完整 ✅：变异 M9（去掉退出时的 `save`）→ **4 个测试红**；`test_real_process_receives_sigterm_and_exits_zero` 断言文件能被 `WatermarkStore.load()` 读回且 `version == 1`。 |
| 6 | **平台画像** —— 「固化文件已存在 → 不重新发现」（断言平台列表接口没被调用）？核对不一致时记 ERROR / WARNING？ | **部分通过（文档与实现不一致）** | 固化优先 ✅：`platform_profile.py:200-215`，画像文件存在 → 走 `load()`，**不重写文件**（`test_reconcile_does_not_add_new_instance_to_profile`）。<br>❌ **「平台列表接口一次都不调用」这句是假的**：`platform_profile.py:193-195` 的 docstring 这么写，但 `main.py:216-219` 传的是 `reconcile=True` → `platform_profile.py:206-207` → `reconcile()` → `fetch_instances(client)`，**每次启动都会调平台列表接口**。守卫测试 `tests/unit/test_platform_profile.py::test_existing_profile_file_is_not_rediscovered` 断言 `client.calls == []`，但它**没传 reconcile** → 断言的是另一条路径。变异 M12（画像存在也重新发现）→ **1 个测试红**（该测试确实有守卫力，但守的不是生产路径）。见 Minor #1。<br>核对出声 ✅：`platform_profile.py:261-291` —— 画像里的实例平台没了 → **ERROR**；平台新增 → **WARNING**；`engine_name` 变 → **WARNING**；`master_count` 变 → **WARNING**（4 个测试逐一断言）。`master_count` 按唯一 `node_id` 计数 ✅（`platform_profile.py:305-329`，`test_master_count_counts_unique_node_ids`）。 |
| 7 | **日志** —— `log_level=DEBUG` 真能输出 INFO？`log_format=json` 真能逐行 `json.loads`？凭据 / mini-token / 业务 SQL 有没有漏进日志？ | **通过** | `log_level=DEBUG` 输出 INFO ✅：`logging_setup.py:158-161` `root.setLevel(numeric)` + `handler.setLevel(numeric)`；`tests/unit/test_logging_setup.py::test_debug_level_really_outputs_info_records`。变异 M7（`_make_formatter` 硬编码 text）→ **2 个测试红**（`test_json_format_emits_one_json_object_per_line` 抛 `JSONDecodeError`）。<br>`log_format=json` 逐行 `json.loads` ✅：`logging_setup.py:100-111` `JsonLogFormatter.format` 每行一个 JSON 对象；`test_json_format_emits_one_json_object_per_line` 真的 `json.loads(line)`。<br>幂等 ✅：`logging_setup.py:121-163`（`HANDLER_MARKER` 自认领 handler，不叠加）。<br>凭据 / token / SQL 未漏 ✅：全包 `LOGGER.*` 调用逐条扫过，**无一处**打印 password / token / `sql_text`；`tpops/client.py:333-340` 的传输层异常只带 `method` + `path`，不带请求体；`tpops/client.py:180-230` 认证失败消息里明确不含密码与 token；`config.py:115` 的配置错误只报**变量名**。全包 `print(` 数量 = **0**。 |
| 8 | **空结果 / 空分组** —— 是不是静默返回空列表？ | **通过** | 空分组 ✅：`collectors/monitor.py:133-164` —— 12 个分组码逐个查目录，空分组记 **WARNING**（并解释「平台对错误分组码返回 200 + 空 series」），全空记 **ERROR**，目录里缺指标记 WARNING。命令中心取数失败 ✅：`collectors/command_center.py:266-270` 记 **ERROR** 并放弃本轮；执行器据此给 `end_time_ms=None`（`executors.py:174-180`）→ 不推进水位。<br>空结果不静默 ✅：`scheduler_delivery.py:87-95` —— 非告警通道一条消息都没产出时记 **WARNING**；告警通道豁免（`tests/unit/test_scheduler_delivery_channels.py` 3 个用例守住了这个豁免，避免每 30 秒刷一条噪音）。<br>⚠️ 但「全部消息都是重复」与「完全没数据」在日志上不可区分（见 Minor #3）。 |

### 简报之外、同类风险的自查

| 风险 | 判定 | 证据 |
|---|---|---|
| **A 类多组件指标会不会算出同一个幂等键** | ✅ 不会 | `scheduler_types.py:167-182` `metric_key` 把 `tags.node_id` 与 `tags.component_id` 并入维度键；`mapping/message.py:135-136` 确认 `component_id` 进了 `tags`。变异 M8（删掉 node_id / component_id）→ **2 个测试红**（`test_metrics_of_different_components_have_different_keys`、`test_a_class_multi_component_messages_are_all_delivered`）。 |
| **实例间隔离**（两实例共用 node_id） | ✅ 三处都带 instance_id | 水位键 `scheduler_types.py:154-164`；幂等键 `ids.py:66/84`；差分作用域 `collectors/command_center.py:317`。`test_each_instance_gets_its_own_watermark` + `test_two_instances_sharing_node_id_keep_separate_baselines`。 |
| **`state <> ''` 在 GaussDB 方言恒为假** | ✅ 没踩 | `tpops_kafka_collector/mapping/data/merged_sql.json:247` 用的是 `where state is not null`；全包 `grep "state <> ''"` = **0 命中**。 |
| **命令中心结束判据用 `statistics.running == 0`（不是 `job_status`）** | ✅ | `tpops/command_center.py:285-302` —— 明确注释「结束判据不看 `job_status`」，`running is None` 抛 `ProtocolError`。 |
| **`result` 有没有二次 `json.loads`** | ✅ | `tpops/command_center_result.py:89` `payload = json.loads(text)`（`result` 是 JSON 字符串，必须再解一次）。 |

---

## 变异验证记录

沙箱：`/tmp/w3-correct-mut`（工作树 `tpops_kafka_collector/` + `tests/` + `pytest.ini` + `data/` + `tools/` + `docs/` + xlsx 的副本）。
每次变异：**改坏 → 跑测试 → 还原（`cp` 回干净副本）→ 重跑全量确认恢复绿**。
变异脚本在 `/tmp/w3-mut-tool/_mut.py`（沙箱外，避免被 `test_py37_compat.py` 扫成第 1561 个用例）。

| # | 变异内容 | 预期红 | 实际结果 | 测试文件::用例名 | 输出摘要 | 已改回 |
|---|---|---|---|---|---|---|
| **M1** | `scheduler_delivery._deliver_metrics`：把 `_advance_metric` + `remember_key` 挪到 `produce_metrics` **之前** | 红 | **红（4 failed / 1557 passed）** | `tests/unit/test_scheduler.py::test_watermark_advances_only_after_successful_delivery`；`::test_failed_batch_is_retried_in_the_next_round`；`tests/integration/test_scheduler.py::test_watermark_does_not_advance_when_delivery_fails`；`tests/integration/test_kill_restart_drill.py::test_failed_batch_does_not_advance_the_watermark_and_is_retried` | `AssertionError: 投递失败却推进了水位`；`assert len(producer.metric_batches) == 2`（失败那批没重发） | ✅ 还原后 1561 passed |
| **M2** | `scheduler_types.metric_stream` / `alarm_stream`：去掉 `instance_id`（`f'{instance_id}:{channel}'` → `'{channel}'`） | 红 | **红（4 failed / 1557 passed）** | `tests/unit/test_scheduler.py::test_watermark_stream_keys_contain_the_instance_id`；`tests/integration/test_scheduler.py::test_a_full_round_delivers_messages_and_advances_watermarks`；`tests/integration/test_kill_restart_drill.py::test_failed_batch_does_not_advance_the_watermark_and_is_retried`；`::test_already_delivered_messages_are_not_delivered_again_after_a_restart` | `AssertionError: assert 'monitor' == 'inst-a:monitor'` | ✅ 还原后 1561 passed |
| **M3** | `mapping/rules_deltas.scoped_dimension_key`：去掉作用域（`scope:key` → `key`） | 红（**回归层也要红**） | **红（9 failed / 1552 passed）** | `tests/regression/test_collector_coverage.py::test_every_master_computes_its_own_rate_in_the_regression_layer`（← **回归层确实红了**）；`tests/integration/test_collector_command_center_deltas.py`（3 条）；`tests/integration/test_collector_command_center_multi_instance.py`（2 条）；`tests/unit/test_mapping_rules_gaps.py::test_scoped_store_forget_only_drops_its_own_scope`；`tests/unit/test_scheduler.py::test_delta_baselines_are_restored_with_their_scope`；`::test_put_baseline_style_keys_are_not_used` | `AssertionError: assert (['t=x'] and False)`（基线键丢了作用域） | ✅ 还原后 1561 passed |
| **M4** | `scheduler._persist_baselines`：`import_baselines_from(delta_store)` → 逐条 `watermark.put_baseline(code, dim, value, stamp)` | 红 | 🟢 **绿（1561 passed，无一测试红）** —— **但这是「变异不可区分」，不是「守卫缺失」**：`WatermarkStore.put_baseline`（`watermark.py:166-173`）写的键形状与 `_baselines` **完全相同**（`dimension_key` 原样存，作用域本来就在 dimension_key 里），两者语义等价。控制器给出的理由「`put_baseline` 写的键没有作用域」在当前实现下**不成立**。真正能被抓的是「落盘时剥掉作用域」（见 M4b）。 | — | 见 Minor #4 | ✅ 还原后 1561 passed |
| **M4b**（补充） | `scheduler._persist_baselines`：逐条 `put_baseline` 且**把维度键的作用域剥掉**（`dim.split(':',1)[-1]`） | 红 | **红（1 failed / 1560 passed）** | `tests/unit/test_scheduler.py::test_put_baseline_style_keys_are_not_used` | `AssertionError: assert (['node-1:t=x', 't=x'] and False)` | ✅ 还原后 1561 passed |
| **M5** | `scheduler._collect_all`：把 `ThreadPoolExecutor` 换成**顺序 for 循环**（组内串行） | 红 | **红（3 failed / 1558 passed）** | `tests/unit/test_scheduler.py::test_due_groups_run_concurrently_not_serially`；`::test_two_dimensions_are_concurrent_groups_and_instances`；`tests/integration/test_round_latency_budget.py::test_a_round_runs_the_channels_concurrently_not_serially` | `threading.BrokenBarrierError`（串行时后一个任务永远到不了屏障） | ✅ 还原后 1561 passed |
| **M6** | `lifecycle.EXIT_OK = 0` → `= 7` | 红 | **红（4 failed / 1557 passed）** | `tests/integration/test_main.py::test_sigterm_during_main_loop_exits_zero_with_complete_watermark`；`::test_in_flight_request_beyond_deadline_gives_up_and_exits_zero`；`::test_real_process_receives_sigterm_and_exits_zero`；`tests/integration/test_kill_restart_drill.py::test_sigterm_exits_zero_with_a_complete_watermark_file_and_restart_resumes` | `assert 7 == 0` | ✅ 还原后 1561 passed |
| **M7** | `logging_setup._make_formatter`：硬编码返回 `logging.Formatter(TEXT_FORMAT)`（忽略 `log_format`） | 红 | **红（2 failed / 1559 passed）** | `tests/unit/test_logging_setup.py::test_json_format_emits_one_json_object_per_line`；`::test_json_format_flattens_key_value_summary_into_fields` | `json.decoder.JSONDecodeError: Extra data: line 1 column 5` | ✅ 还原后 1561 passed |
| **M8**（补充） | `scheduler_types.metric_key`：维度里不再并入 `node_id` / `component_id` | 红 | **红（2 failed / 1559 passed）** | `tests/unit/test_scheduler.py::test_metrics_of_different_components_have_different_keys`；`tests/integration/test_scheduler.py::test_a_class_multi_component_messages_are_all_delivered` | `assert 'inst-a\|metric-1\|\|1000' != 'inst-a\|metric-1\|\|1000'`（A 类多组件塌成同一个键 → 后一个组件被当重复丢弃） | ✅ 还原后 1561 passed |
| **M9**（补充） | `lifecycle.shutdown_runtime`：去掉 `runtime.watermark.save()` | 红 | **红（4 failed / 1557 passed）** | `tests/integration/test_main.py::test_sigterm_during_main_loop_exits_zero_with_complete_watermark`；`::test_in_flight_request_beyond_deadline_gives_up_and_exits_zero`；`::test_real_process_receives_sigterm_and_exits_zero`；`tests/integration/test_kill_restart_drill.py::test_sigterm_exits_zero_with_a_complete_watermark_file_and_restart_resumes` | `AssertionError: assert os.path.exists(watermark_file)` | ✅ 还原后 1561 passed |
| **M10**（补充） | `scheduler_delivery._deliver_metrics`：`remember_key` 挪到 `produce_metrics` **之前** | 红 | **红（3 failed / 1558 passed）** | `tests/unit/test_scheduler.py::test_watermark_advances_only_after_successful_delivery`；`::test_failed_batch_is_retried_in_the_next_round`；`tests/integration/test_kill_restart_drill.py::test_failed_batch_does_not_advance_the_watermark_and_is_retried` | 投递失败那批被记成「已投递」→ 下轮跳过 → 静默丢数据 | ✅ 还原后 1561 passed |
| **M11**（补充） | `scheduler_delivery._deliver_alarms`：告警投递**失败**的 except 分支里也调 `_advance_alarm(...)` | 红 | 🟢 **绿（1561 passed，无一测试红）** → **重要发现**（Important #1）。<br>行为实测（同一脚本，干净树 vs M11）：<br>干净树 `ALARM WATERMARK AFTER FAILED DELIVERY: None`<br>M11 `ALARM WATERMARK AFTER FAILED DELIVERY: '1700000000000'` | — | — | ✅ 还原后 1561 passed |
| **M12**（补充） | `platform_profile.load_or_discover`：把 `if os.path.exists(path)` 改成 `if False`（固化后仍重新发现） | 红 | **红（1 failed / 1560 passed）** | `tests/unit/test_platform_profile.py::test_existing_profile_file_is_not_rediscovered` | `assert ['/rds/v3.2/instances'] == []` | ✅ 还原后 1561 passed |
| **M13**（补充） | `scheduler._restore_baselines`：`export_baselines_into` 改成 `pass`（完全不恢复基线） | 红 | **红（2 failed / 1559 passed）** | `tests/unit/test_scheduler.py::test_delta_baselines_are_restored_with_their_scope`；`::test_put_baseline_style_keys_are_not_used` | `AssertionError: 基线没恢复（重启后速率永远算不出来）` | ✅ 还原后 1561 passed |
| **M14**（补充） | `scheduler_delivery._deliver_alarms`：删掉 `seen` 批内去重 + `has_key` 跨轮去重 | 红 | 🟢 **绿（1561 passed，无一测试红）** → **重要发现**（Important #2）。<br>行为实测（同一脚本，干净树 vs M14）：<br>干净树 `ALARM BATCHES DELIVERED (same alarm, 2 rounds): 1`<br>M14 `... : 2` | — | — | ✅ 还原后 1561 passed |
| **M15**（补充） | `watermark.advance_metric`：去掉「只前进不回退」判断（`end_time_ms > current`） | 红 | **红（1 failed / 1560 passed）** | `tests/unit/test_watermark.py::test_metric_watermark_never_moves_backwards` | `AssertionError`（水位被回退） | ✅ 还原后 1561 passed |

> **必做清单覆盖**：M1 ✓ / M2 ✓ / M3 ✓（含回归层） / M4 ✓（绿，但见 Minor #4 与 M4b） / M5 ✓ / M6 ✓ / M7 ✓ —— **7/7 完成**。
> **补充**：M4b、M5、M8~M15 —— **8/8 完成**。
> **每条变异后都执行了「还原 → 重跑全量」**，还原后均为 **1561 passed**（沙箱比工作树多 1 个用例是 `_mut.py` 被 `test_py37_compat.py` 扫到；把变异脚本移出沙箱后，工作树与沙箱基线**同为 1560 passed**）。

### 覆盖率佐证：告警路径确实是「零守卫」

`pytest --cov=tpops_kafka_collector --cov-report=term-missing` 的输出：

```
tpops_kafka_collector/scheduler_delivery.py    77    5   94%   124, 127, 134-139
```

**未覆盖的 5 行正好是告警通道的两处去重（124 `continue`、127 `continue`）与整个投递失败分支（134-139）** —— 与 M11 / M14 全绿的结论互相印证：这两条路径**一条测试都没走过**。

---

## 发现

### Critical

（无。生产实现本身在本轮审查范围内未发现「不报错但数据错」的实际缺陷；下面 4 条 Important 全部落在**守卫层 / 持久化时机 / 硬约束**上。）

### Important

#### 1. 告警通道「投递失败不推进告警水位」**零守卫** —— 变异后全量测试仍全绿

- **File:line**：`tpops_kafka_collector/scheduler_delivery.py:134-139`（告警投递失败分支）；对应测试缺口在 `tests/unit/test_scheduler.py`（只有指标侧的失败用例）与 `tests/integration/test_kill_restart_drill.py`（也只有指标侧）
- **问题是什么**：指标通道有 `test_watermark_advances_only_after_successful_delivery` / `test_watermark_does_not_advance_when_delivery_fails` 守着「投递失败不推进水位」；**告警通道一条对应的守卫都没有**。变异 M11（在告警失败分支里也推进告警水位）→ **1561 passed，无一测试变红**。覆盖率佐证：`scheduler_delivery.py` 未覆盖行 = `124, 127, 134-139`，正是告警去重 + 告警失败分支。
- **触发条件**：Kafka 抖动 / broker 不可用导致 `produce_alarms` 抛错，**同时**未来某次改动把 `_advance_alarm` 挪进失败分支。
- **后果**：告警水位被推进 → 下一轮 `_alarm_executor`（`executors.py:265-266`）读到推进后的游标 → `collect_alarms` 的 `_is_new_or_changed`（`collectors/alarm.py:287-302`）把**从未投出去的告警**判成「不新」→ **告警整条静默丢失**，日志里只有一条 `LOGGER.exception`，没有任何计数能暴露它。这正是本项目最贵的一类缺陷形态。
- **实测证据**：干净树 `ALARM WATERMARK AFTER FAILED DELIVERY: None`；M11 变异后 `ALARM WATERMARK AFTER FAILED DELIVERY: '1700000000000'`（水位确实被推进了）。
- **怎么修**：补一条守卫测试（与指标侧对称），例如
  ```python
  def test_alarm_watermark_is_not_advanced_when_delivery_fails(tmp_path):
      store = WatermarkStore(str(tmp_path / 'wm.json'))
      producer = FakeProducer(fail_alarm_batches={0})
      scheduler = make_scheduler({CHANNEL_ALARM: lambda i: TaskOutcome(alarms=(alarm_message(),))},
                                 producer=producer, store=store,
                                 plans=make_plans(((CHANNEL_ALARM, 30),)))
      scheduler.tick()
      assert store.alarm_watermark(alarm_stream(INSTANCE_A)) is None, '告警投递失败却推进了告警水位'
  ```
  并在 `scheduler_delivery._deliver_alarms` 的 except 分支上加注释指明这条不变量。

#### 2. 告警幂等去重（批内 + 跨轮）**零守卫** —— 变异后全量测试仍全绿

- **File:line**：`tpops_kafka_collector/scheduler_delivery.py:121-129`（`seen` 批内去重 + `has_key` 跨轮去重）
- **问题是什么**：变异 M14（删掉这两道去重）→ **1561 passed，无一测试变红**。现有测试 `test_alarm_watermark_advances_after_delivery`（`tests/unit/test_scheduler.py:680-693`）只跑**一轮**、只断言水位，不碰去重。
- **触发条件**：任何对 `_deliver_alarms` 的重构（哪怕只是把 `has_key` 判断挪位置）。
- **后果**：**告警重复投递** —— 同一条告警每 30 秒重发一次，客户侧告警面板被同一条刷屏；而告警是 257 条定义的持续事件流，重复量级远高于指标。
- **实测证据**：干净树同一条告警连投两轮 → `ALARM BATCHES DELIVERED = 1`；M14 → `= 2`。
- **怎么修**：补两条守卫测试：① 同一批里两条**相同幂等键**的告警只投一条（对照指标侧的 `test_duplicate_keys_inside_one_batch_are_delivered_once`）；② 已投递过的告警键在下一轮被跳过（对照 `test_already_delivered_messages_are_skipped`）。

#### 3. 运行期**从不落盘**水位 / 幂等键 —— 非优雅退出会丢掉整个进程生命周期的去重状态

- **File:line**：`tpops_kafka_collector/lifecycle.py:156-161`（`shutdown_runtime`）是全包**唯一**的 `watermark.save()` 调用点（`grep -rn "\.save()" tpops_kafka_collector/` 只命中这一处）；`scheduler.py:160-176` 的 `tick()` 全程不落盘
- **问题是什么**：水位与幂等键只在**进程退出时**写盘。规格「水位与差分基线的持久化 → 重启续传：进程重启后从持久化的水位继续，**不重复投递已成功的数据**」在**优雅退出（SIGTERM）**路径上成立，在**非优雅退出**上不成立。
- **触发条件**：SIGKILL / OOM Killer / 宿主机重启 / 容器被驱逐 —— 也就是真实生产里最常见的「杀进程」。
- **后果**：重启后内存里的幂等键 LRU 与水位**全部丢失** → 采集端自身的去重彻底失效 → 整个进程生命周期（可能是数小时）的数据被**整段重投**。A 类时序指标的键含平台数据点时间戳 → 客户侧能按幂等键去重；**B 类快照指标（82/98 条）的 `clock` 是采集时刻 → 键每轮都变 → 客户侧也无法去重**，等于把整段快照历史重发一遍。
- **佐证**：合同完成标准 #4「杀进程 → 重启演练：不重复、不丢失」目前**只有 SIGTERM 版本**（`tests/integration/test_kill_restart_drill.py::test_sigterm_exits_zero_with_a_complete_watermark_file_and_restart_resumes` 用 `signal.SIGTERM`；`tests/fixtures/graceful_shutdown_stubs/` 名字里就写着 graceful）。没有 `SIGKILL` 的演练。
- **怎么修**（二选一）：
  - **推荐**：每轮 tick 结束后（或每 N 轮 / 每 M 秒）调用一次 `watermark.save()` —— 原子写已经实现好（`watermark.py:316-344`），成本只是一次小 JSON 落盘；这样崩溃最多丢一轮。
  - 或者：显式在规格 / 开发方案里写明「水位只在优雅退出时落盘；非优雅退出后按 at-least-once 全量重投，客户侧按幂等键去重」，并**把该口径加进交付文档**（现在是隐含行为）。
  - 无论哪种，都建议补一个 **SIGKILL 版**的杀进程演练脚本/测试，把行为钉死。

#### 4. `platform_profile.reconcile` **55 行**，越过合同「函数 < 50 行」硬约束；且实施方报告的「最长函数 49 行」不实

- **File:line**：`tpops_kafka_collector/platform_profile.py:243-298`（`reconcile`，AST 实测 span = 55 行；含装饰器 56 行）
- **问题是什么**：简报 §四「硬约束（违反即 Important）」明写「单文件 < 400 行、**函数 < 50 行**（生产文件）」。全包 AST 扫描（`ast.FunctionDef` 的 `end_lineno - lineno + 1`）结果：**只有 `reconcile` 一个函数越线**；全包**没有**文件 ≥ 400 行。实施方报告 `task-3.3-report.md:492` 声称「最长函数 **49 行**（`build_runtime`）—— < 50」，与实测不符（`build_runtime` 47 行没错，但漏了同批次新增的 `platform_profile.reconcile` 55 行）。
- **触发条件**：静态，不需触发条件。
- **后果**：硬约束违反（合同层面）；报告数字不准会让控制器误判「函数长度全部合规」。
- **怎么修**：把 `reconcile` 里的「逐实例比对」与「新增实例扫描」拆成两个私有函数（例如 `_compare_instance(...)` 与 `_scan_added(...)`），主函数只负责汇总与组装 `ProfileReconciliation`；或在实施方报告里更正该行数字。

### Minor

#### 1. `platform_profile.load_or_discover` 的 docstring 与实现不符（「平台列表接口一次都不调用」是假的）

- **File:line**：`tpops_kafka_collector/platform_profile.py:193-195`（docstring）vs `tpops_kafka_collector/main.py:216-219`（`reconcile=True`）→ `platform_profile.py:206-207`
- **问题是什么**：docstring 写「**固化文件存在 → 不重新发现**（平台列表接口一次都不调用）」，但生产装配传了 `reconcile=True` → 每次启动都会调 `GET /rds/v3.2/instances` 做核对。守卫测试 `tests/unit/test_platform_profile.py::test_existing_profile_file_is_not_rediscovered` 断言 `client.calls == []`，但**没传 `reconcile`**，守的不是生产路径。
- **后果**：文档误导（后来者会以为生产路径不碰平台列表）；「固化文件存在 → 不重新发现」这条口径的真实含义是「**不重建、不改写**画像文件」，而不是「不调平台接口」。
- **怎么修**：把 docstring 改成「固化文件存在 → **不重新发现、不改写**（画像以文件为准）；`reconcile=True` 时会**额外**调一次平台列表做核对（只记日志、不改文件）」；并在 `test_existing_profile_file_is_not_rediscovered` 里补一个 `reconcile=True` 的变体，断言「平台列表**被调了**，但画像文件**没被改**」。

#### 2. 告警幂等键取自平台行的 `instance_id`，与指标幂等键取自调度器 `instance_id` 的口径不一致

- **File:line**：`tpops_kafka_collector/collectors/alarm.py:196-201`（`row.get('instance_id')`）vs `tpops_kafka_collector/scheduler_types.py:167-182`（调度器传入的 `instance_id`）
- **问题是什么**：指标键用「调度器认定的实例」，告警键用「平台行自带的实例」。若平台收敛行缺 `instance_id`，`collectors/alarm.py:176-177` 的 `row.get('instance_id') != instance_id` 会把该告警**整条过滤掉**（不是产生跨实例冲突键）。当前 wave-2 实测数据里 `instance_id` 存在，所以不会触发；但两处口径不一致，属于「不报错但数据没有」的潜在入口。
- **后果**：一旦平台收敛行不带 `instance_id`，告警会**静默全灭**（一条 WARNING 都没有）。
- **怎么修**：在 `_build_messages` 里对缺 `instance_id` 的行显式记 WARNING（并按「无法归属实例」跳过），或统一用调度器传入的 `instance_id` 参与告警键的计算。

#### 3. 「全部消息都是重复」与「完全没数据」在日志上不可区分，且 `delivery_rate` 会报 1.000

- **File:line**：`tpops_kafka_collector/scheduler_delivery.py:87-95`（空结果 WARNING 的条件是 `not outcome.messages`）、`tpops_kafka_collector/scheduler_report.py:39-40`（`rate = (produced / planned) if planned else 1.0`）
- **问题是什么**：当采集器产出了消息、但**全部**被幂等键过滤掉时：`records` 为空 → `outcome.messages` 非空 → **不记任何 WARNING**；汇总日志是 `mapped=0 produced=0 delivery_rate=1.000` —— 看上去完美健康。现有测试 `test_already_delivered_messages_are_skipped` 恰恰把这个静默行为**固化**了下来（断言 `produced == 0 and mapped == 0`）。
- **后果**：某条通道因平台侧数据停滞而**长期一条都投不出去**时，值班从汇总日志上看不出任何异常（`delivery_rate=1.000` 反而误导）。
- **怎么修**：在 `_deliver_metrics` / `_deliver_alarms` 里区分「采集器没产出消息」与「产出但全被去重」，后者记一条 WARNING（如「本轮 N 条消息全部命中幂等键（已投递过），本轮无新增」）；`delivery_rate` 在 `planned == 0` 时建议输出 `-` 或 `n/a` 而不是 `1.000`。

#### 4. `WatermarkStore.put_baseline` 与 `import_baselines_from` 的键形状完全相同 —— 控制器口径 #2 的理由与实现不符

- **File:line**：`tpops_kafka_collector/watermark.py:166-173`（`put_baseline` 存的键就是 `(metric_code, dimension_key)`，**dimension_key 原样存**，作用域本来就在 dimension_key 里）vs `tpops_kafka_collector/scheduler.py:293-298` 的 docstring（「`put_baseline` 写的键没有作用域（`scope:key`），采集器按带作用域的键去找，**永远找不到**」）
- **问题是什么**：docstring 与控制器口径所说的「`put_baseline` 会丢作用域」在当前实现下**不成立** —— 所以简报必做的变异 #4「`import_baselines_from` → `put_baseline`」**测不出任何东西**（M4 全绿）。真正有意义的守卫是「落盘/恢复时**不要剥掉**作用域」（M4b → 红，由 `test_put_baseline_style_keys_are_not_used` 抓住）。
- **后果**：守卫测试 `test_put_baseline_style_keys_are_not_used` 的**名字与它实际守的东西对不上**（它守的是「键里有冒号」，不是「没用 `put_baseline`」）；docstring 会让后来者以为 `put_baseline` 有坑而绕道，也让人误以为已有守卫覆盖了口径 #2。
- **怎么修**：把 `scheduler.py:293-298` 的 docstring 改成「**只用整体搬运**（`export_baselines_into` / `import_baselines_from`）—— 整体替换能同时清掉已消失的基线；逐条 `put_baseline` 只会**合并**、不会删除陈旧基线」；把守卫测试改名为 `test_baselines_keep_their_scope_in_the_watermark_file`。

#### 5. `TickResult.mapped` 的文档口径与实际值不一致；「本轮丢弃 N 个不可提供指标」是静态计数

- **File:line**：`tpops_kafka_collector/scheduler_types.py:121`（「`mapped`: 转换后待投递的**消息总数**」）—— 实际是**过滤掉幂等重复之后**「真正尝试投递的条数」（`scheduler_delivery.py:79-86`）；`tpops_kafka_collector/scheduler_report.py:59-63`（「本轮丢弃 %d 个不可提供指标」—— 该值来自 `count_unsupported_metrics`（静态 = 3），**不是本轮真丢的**）
- **问题是什么**：两处措辞与实际语义有偏差。`mapped` 小于「转换后的总数」时，值班拿 `mapped` 与 `fetched` 对比会得出错误结论；「本轮丢弃 3 个」会被理解为每轮有 3 条消息被丢弃（实际是客户指标库里 3 条**永远无法提供**的指标，与轮次无关）。
- **怎么修**：`scheduler_types.py:121` 改成「转换后**实际尝试投递**的消息数（已滤掉幂等重复）」；`scheduler_report.py:61` 改成「不可提供指标 %d 条（本轮不产出，累计 %d）」。

---

## 实施方报告中的不准确断言

| # | 报告位置 | 声称 | 实测 | 差异说明 |
|---|---|---|---|---|
| 1 | `task-3.3-report.md:492`（「最长函数 **49 行**（`build_runtime`）—— < 50」） | 全包最长函数 49 行 | `platform_profile.reconcile` **55 行**（`ast` 实测 `end_lineno - lineno + 1 = 55`；含装饰器 56） | 报告只扫了自己装配的 5 个文件，漏了同批次新增的 `platform_profile.py`；**这是唯一越 50 行的生产函数**（全包无 ≥400 行的文件） |
| 2 | `task-3.1-report.md:22`（「现存最大文件 393 行，**最长函数 < 40 行**」）、`task-3.1-report.md:540`（「最长函数 **< 40 行**」） | 最长函数 < 40 行 | 本波生产文件里最长的是 `scheduler_delivery._deliver_metrics` **40 行**（刚好触线），另有 `scheduler_report.log_round_summary` 37 / `executors.build_executors` 37 / `scheduler._collect_all` 34 | 就 3.1 自己那批文件而言基本准确（最长的 `_deliver_metrics` 正好 40 行），但**该口径不适用于 3.3 的 `platform_profile.reconcile`（55 行）** —— 两个报告的数字拼起来才是全包真相，而两个都写「最长 < 40/49 行」，与全包实测（55 行）不符 |
| 3 | `task-3.3-report.md:390`（「把 `main._load_profile` 里的 `reconcile=True` 改成 `False` 即可」）+ `platform_profile.py:193-195` docstring | 固化文件存在 → 平台列表接口一次都不调用 | 生产路径 `main.py:218` 传 `reconcile=True` → **每次启动都会调 `GET /rds/v3.2/instances`** | docstring 与「口径」描述与实际装配不符（见 Minor #1） |
| 4 | 控制器口径 #2（以及 `scheduler.py:296` 的 docstring）：「`put_baseline` 写的键没有作用域 → 重启后速率永远算不出来，且不报错」 | `put_baseline` 会丢作用域 | `watermark.py:166-173` 的 `put_baseline` 存的键与 `import_baselines_from` **完全一致**（dimension_key 原样存，作用域本就在里面） | 简报必做变异 #4 因此**不可区分**（M4 全绿）；真正能测的是「落盘时剥作用域」（M4b → 红）。见 Minor #4 |

> 其余实施方报告的数字（1560 全绿、`ruff` 0、`test_py37_compat.py` 147 passed、映射层覆盖率、TOTAL 95%）**本次独立复核全部属实**：
> `ruff check tpops_kafka_collector/` → `All checks passed!`；`pytest tests/test_py37_compat.py -q` → `147 passed`；`pytest --cov=tpops_kafka_collector` → `TOTAL 4670 228 95%`（wave-3 新增模块：executors 99% / lifecycle 95% / logging_setup 96% / main 92% / platform_profile 91% / scheduler 87% / scheduler_delivery 94%）。

---

## 收尾

- 变异沙箱 `/tmp/w3-correct-mut` 与变异脚本 `/tmp/w3-mut-tool` 已删除（收尾清理）。
- **工作树未被改动**：`git status --porcelain` 只显示另一名审查者的两份报告文件（`...-contract.md` / `...-quality.md`），本波生产代码与测试**零改动**，未执行任何 `git commit` / `add` / `checkout` / `reset`。

签发 receipt 的命令：

```
ssf execution review changes/tpops-kafka-collector \
  --wave wave-3-scheduling-testing --base 356d52f --head 3a71cb6 \
  --report .superpowers/sdd/reviews/wave-3-scheduling-testing.md --verdict fail
```

> 说明：本报告写到 `.../reviews/wave-3-scheduling-testing-correctness.md`（本审查者维度）；
> 若控制器按简报 §七 的路径统一签发 receipt，请把 `--report` 指向控制器约定的合并报告文件。
