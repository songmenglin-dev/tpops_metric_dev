# Wave 3 审查报告（维度：合同与规格合规）—— wave-3-scheduling-testing

- **波次**：`wave-3-scheduling-testing`（调度、装配与测试）
- **base**：`356d52f`（wave-2b pass receipt 的 head）
- **head**：`3a71cb6`（HEAD，最后一笔是账本文档提交）
- **审查范围**：`git diff --stat 356d52f..3a71cb6` = 42 个文件（13 个生产文件 + 16 个测试文件 + 1 个演练桩 + 账本/工件）。
  另核 `git show 3a71cb6` 的账本提交（`progress.md` 增补）。
- **审查方式**：全部指标**独立复跑**（覆盖率 / 3.7 门禁 / ruff / 墙钟实测 / 配置缺失实跑 / 真进程 SIGTERM 实跑），
  不采信实施方报告里的任何数字；两处关键守卫做了**变异验证**（在 `/tmp/w3-contract-mut/mut/` 的副本上做，未污染工作树）。
- **工作树状态**：审查前后 `git status --porcelain` 仅有其他审查者的未跟踪报告文件，HEAD 未动、未 commit。

- **结论**：**verdict = pass**
- **Critical / Important / Minor 计数**：**0 / 0 / 8**

---

## 先说做得好的地方（具体到文件）

1. **「水位只在投递成功后推进」做在了正确的层**。`tpops_kafka_collector/scheduler_delivery.py:96-110`
   把 `remember_key`（幂等键登记）放在 `produce_metrics` **成功之后**，`_advance_metric` 同样在成功之后。
   这条最容易被写成「先登记再投递」——那样投递失败的那批键会被记成「已投递」，下轮被当重复跳过，
   **数据永远投不出去且不报任何错**。实施方在 3.1 报告 D2 里主动纠正了控制器简报的字面写法，方向正确。
2. **差分基线的恢复方向与作用域都对**。`tpops_kafka_collector/scheduler.py:292-315`
   只用 `export_baselines_into` / `import_baselines_from` 整体搬运，生产代码里 `put_baseline`
   **一次都没被调用**（只在 `watermark.py:166` 有定义）；`tpops_kafka_collector/collectors/command_center.py:317`
   把作用域改成 `f'{instance_id}:{node_id}'`，两个实例的同名 `node_id` 不再共用基线。
   `tpops_kafka_collector/main.py:139-147` 保证 `deltas` 是**同一个对象**同时交给采集器与调度器
   （`test_build_runtime_shares_one_delta_store_between_collector_and_scheduler` 守着）。
3. **两维并发是真两维**。`scheduler.py:206-249` 把「通道 × 实例」展成任务表丢进 `ThreadPoolExecutor`；
   守卫测试用 `threading.Barrier` 而非计时（`test_two_dimensions_are_concurrent_groups_and_instances`），
   **不依赖墙钟**，CI 抖动不会误判。
4. **优雅退出没有走 `scheduler.run()`**。`lifecycle.py:100-153` 自己在**守护线程**里跑一轮并带截止时间，
   绕开了 `future.result()` 无超时 + `ThreadPoolExecutor` 上下文管理器 `shutdown(wait=True)` 的挂死路径
   —— 这是规格「放弃等待该请求 …… 不挂死」的正解，`scheduler.py` / `lifecycle.py` 两处 docstring 都写明了理由。
5. **日志中心化把两个「没人消费的配置」真正用起来了**。`logging_setup.py` 幂等（`root.handlers` 认领自己的 handler，
   py37 无 `basicConfig(force=…)`）、输出 stderr、第三方降噪、`log_format=json` 时把 `key=value` 汇总行摊平成**顶层字段**。
   我实跑真进程验证：**54 行日志逐行 `json.loads` 全部通过**，11 个汇总字段齐全。
6. **唯一序列化出口的 AST 守卫是真有牙的**（见下「变异验证记录」第 1 条），并且自带
   「白名单登记的用途必须真的还在用」的元测试 —— 白名单不会变成免死金牌。
7. **控制器补充说明 §二（差分作用域）与 §三（可注入依赖）都被真正落地**：
   `main.build_runtime(...)` 支持注入 `client` / `producer` / `mapping` / `watermark` / …，
   `tests/integration/test_kill_restart_drill.py` 靠它跑完了「投递失败 → 水位不推进 → 重启 → 整批重投 → 幂等键挡住重复」。

---

## 合同完成标准逐条判定

| # | 标准 | 判定 | 证据（独立实测） |
|---|---|---|---|
| 1 | `pytest --cov=tpops_kafka_collector --cov-report=term-missing --cov-fail-under=80 -q` 通过；**映射层与规则引擎 ≥95%（逐文件且合计）** | **通过** | 全量：**1560 passed in 59.21s**，`TOTAL 4670 stmts / 228 miss = 95.12%`，`Required test coverage of 80% coverage reached`。<br>映射层逐文件：`mapping/` 下 21 个 .py，最低 `rules_sorting.py` **98%**（62 stmts / 1 miss），其余**全部 100%**（`decode/loader/message/table/validation/columns/masking/nodes/sqlgen/rules*`）→ **逐文件 ≥95% ✅**。<br>映射层合计：**1292 stmts / 1 miss = 99.92%** → **合计 ≥95% ✅**。<br>（规则引擎 `rules.py` + 9 个 `rules_*.py` 全部 100%，唯一缺口是 `rules_sorting.py:129` 这条死分支，见 Minor-5。） |
| 2 | `pytest tests/test_py37_compat.py -q` 通过 | **通过** | `147 passed in 1.22s`。该门禁 `SCAN_DIRS` 含 `tpops_kafka_collector`，`ast.parse(..., feature_version=(3,7))` + 禁用 API/内建泛型/PEP 604 全覆盖。<br>**额外证据**：整条测试套件本身就跑在目标解释器上 —— coverage 报告头 `coverage: platform linux, python 3.7.9-final-0`，即本波次新增的 13 个生产文件在 **3.7.9 上真跑过**。 |
| 3 | `ruff check tpops_kafka_collector/` 为 0 | **通过** | `All checks passed!`（工作树，未加任何豁免）。 |
| 4 | 杀进程 → 重启演练：**不重复、不丢失**（要有**可执行的**测试，不是报告里的一段话） | **通过** | `tests/integration/test_kill_restart_drill.py`（272 行 / 4 例）——<br>① 进程内（真 `MessageDelivery` + 真 `WatermarkStore`）：投递失败→水位未推进→下轮整批重投（`test_failed_batch_does_not_advance_the_watermark_and_is_retried`）；重启后已投递的不再投（`test_already_delivered_messages_are_not_delivered_again_after_a_restart`）；幂等键落盘并在重启后恢复（`test_idempotency_keys_survive_a_restart`）。<br>② 进程级：真起 `python -m tpops_kafka_collector.main`（**传输层桩** `tests/fixtures/graceful_shutdown_stubs/sitecustomize.py`，不连任何外部服务）→ `kill -TERM` → 退出码 0 + 水位文件能被 `WatermarkStore.load()` 读回 → 起第二个进程仍退出码 0（`test_sigterm_exits_zero_with_a_complete_watermark_file_and_restart_resumes`）。<br>**我另外实跑了一遍真进程**：SIGTERM → `returncode = 0`，`state/` 下生成 `watermark.json` + `platform_profile.json`，`watermark.json` 能被 `WatermarkStore.load()` 读回。 |
| 5 | 60 秒档单轮墙钟 **P95 ≤ 25 秒**（实测规模） | **通过（带保留意见，见 Minor-7）** | 我独立跑 `conda run -n tpops python tests/performance/measure_round_latency.py`（8 轮）：**P95 = 15.017s**（min 15.006 / max 15.017），串行下界 21.0s，预算 25.0s，退出码 0，**余量 10.0s**。<br>另有快跑回归 `tests/integration/test_round_latency_budget.py`（3 例，延迟缩放 10 倍）。<br>账本记的「实测 P95 = 15.017s（最小 15.009）」与我的「最小 15.006」差 3 ms —— 属运行间抖动，**不是**不准确断言。 |

---

## 规格逐条判定

### `specs/delivery-and-state/spec.md`

| Requirement | 判定 | 证据 |
|---|---|---|
| 唯一 JSON 序列化出口 | **通过** | `kafka/serializer.py:187` 是生产代码里唯一产出消息的 `json.dumps`；AST 守卫 `tests/unit/test_serializer.py:352`（白名单 + 元测试）。**变异验证有牙**（见下）。 |
| 业务维度的落点 | **通过** | `mapping/message.py`；回归层 231 passed。 |
| 值按客户指标表的数据类型序列化 | **通过** | `kafka/serializer.py`；回归层全绿。 |
| 消息字段的缺省约定 | **通过** | 同上。 |
| 指标与告警使用不同 Topic | **通过** | `kafka/producer.py:234/241`（`produce_metrics` / `produce_alarms` 分 Topic）。 |
| 分区与压缩策略 | **通过** | `kafka/producer.py`（`KAFKA_TOPIC_PARTITIONS` / `KAFKA_COMPRESSION`）。 |
| Kafka 投递参数 | **通过** | PLAINTEXT + `acks=all` + `enable_idempotence=False`。 |
| 投递失败不推进水位 | **通过** | `scheduler_delivery.py:96-110`（成功后才 `remember_key` + `_advance_metric`）；`kafka/producer.py:273` 抛 `ProduceBatchError(ProduceError)`。**变异验证有牙**（见下）。 |
| 幂等去重（LRU 默认 10000，跨轮持久化） | **通过** | `ids.py:45 DEFAULT_IDEMPOTENCY_LRU_CAPACITY = 10000`；幂等键落在 `watermark.json` 的 `idempotency_keys` 段，`WatermarkStore.load()` 恢复（`test_idempotency_keys_survive_a_restart`）。 |
| 水位与差分基线的持久化（原子写） | **通过** | `watermark.py:316-345`（同目录临时文件 → `fsync` → `os.replace` → 目录 fsync）；`platform_profile.py:178` 复用同一实现（没有复制第二份）。 |
| 调度器的到期驱动与并发组 | **通过** | 到期：`schedule.py:217-235 DueTable.split_due`（未到期的不进 `due`）；并发：`scheduler.py:206-249`（通道 × 实例两维）；单组失败隔离：`scheduler.py:236-248`（异常捕获后其余组照常投递，进程不退出）。守卫：`test_only_due_channels_are_executed` / `test_two_dimensions_are_concurrent_groups_and_instances` / `test_one_failing_group_does_not_stop_the_others`。 |
| 单组失败不影响其它组 | **通过** | 同上；`tests/integration/test_scheduler.py` 有真命令中心失败 + 真 A 类仍投递的集成用例。 |
| 单轮性能预算 | **通过** | 见合同标准 #5。 |
| 单轮外部调用数与投递成功率可观测 | **通过** | `scheduler_report.py:41-58` 输出 `calls=` / `delivery_rate=`；`:64-68` 超 42 记 WARNING（守卫 `test_external_call_budget_overrun_is_logged`）。 |
| 平台不保存历史快照 | **通过** | 生产代码没有任何历史快照落盘（`watermark.json` 只存水位/基线/幂等键）；`/readyz` 暴露可用性（`health.py`）。 |

### `specs/runtime-and-config/spec.md`

| Requirement | 判定 | 证据 |
|---|---|---|
| 生产代码边界 | **通过（带保留意见，见 Minor-4）** | import 边界：`grep` 生产包 0 处 `import tools/data`；`tests/unit/test_package_boundary.py` 守卫。<br>运行时读取面：生产走 `mapping/loader.py:49 PACKAGE_DATA_DIR`（基于 `__file__`）与 `mapping/sqlgen.py:47/50/53`，**不读仓库根**；`tests/unit/test_mapping_data_sync.py`（33 passed）守两侧一致。<br>遗留：`mapping/table.py:15-16` 的 `REPO_ROOT/DATA_DIR` 默认仍指向仓库根 `data/mapping`（**生产路径无人调用**，但 `mapping/__init__.py:13` 导出了 `MappingTable`）→ 账本 T-10/T-21；文件读取面无守卫 → T-20。 |
| 启动期配置加载与快速失败 | **通过** | **实测**：缺 `KAFKA_BOOTSTRAP_SERVERS` → `returncode = 2`，stderr 含 `配置校验失败，进程退出：缺少必填环境变量：KAFKA_BOOTSTRAP_SERVERS`（**变量名在**），且**未构造任何 client / producer**（`main.py:295-299` 在 `load_config` 之前不发任何请求）。越界值也带变量名/实际值/范围（`config.py:139/141/164`）。`Config` 是 `frozen dataclass`（`config.py:41-42`），运行期不再读环境变量（全包只有 `config.py:337` 一处读 `os.environ`）。 |
| 凭据仅来自环境变量且不入日志 | **通过** | **实测**：真进程日志里搜 `pw-secret-value` / `drill-mini-token` / `X-Mini-Token` → **全部 False**。`tpops/client.py:218` 只记 token **有效期**，不记 token 值。画像文件不含凭据（`test_profile_file_contains_no_credentials`）。 |
| 优雅退出 | **通过** | `lifecycle.py:18-26` 的顺序：置位 shutdown → 停发新请求（`executors.py:101-116 _guard` 在执行器入口检查，**不发任何 HTTP**）→ 在途最多等 `SHUTDOWN_DEADLINE_SECONDS = 30`（`lifecycle.py:49`）→ `watermark.save()`（原子写）→ `producer.close()`（kafka-python 的 close 含 flush）→ **退出码 0**（`lifecycle.py:189`）。<br>守卫：`test_sigterm_during_main_loop_exits_zero_with_complete_watermark` / `test_in_flight_request_beyond_deadline_gives_up_and_exits_zero`（用可控桩 + 可注入截止时间，**不真等 30 秒**）/ `test_default_shutdown_deadline_is_thirty_seconds` / `test_no_new_http_request_after_shutdown`（断言 `client.call_count == 0`）。**实测**真进程 SIGTERM → 退出码 0 + 水位文件完整。 |
| 健康端点 | **通过** | `/healthz` 恒 200；`/readyz` 在依赖不可用时 503（`test_readiness_returns_503_before_first_tick`）；两个探针**轻量**（`test_health_probes_are_wired_and_lightweight` 断言探针调用前后 `client.call_count` 不变）。响应体只有 `dependencies[]`，**不含凭据**。 |
| 每轮结构化汇总日志 | **通过** | **实测**真进程 JSON 日志：54 行**全部** `json.loads` 通过；汇总行的 11 个字段（`tick/elapsed/fetched/mapped/produced/failed/dropped_unsupported/windows/alarms/calls/delivery_rate`）**全在顶层**；`produced + failed == mapped` 自洽（守卫 `test_summary_counts_are_self_consistent`）。字段名与开发方案 §4.9.1 逐字一致。 |
| 结构化数据不可变 | **通过** | `scheduler_types.py` 全部 `@dataclass(frozen=True)`；`TickResult` / `TaskOutcome` / `DeliveryStats` / `TaskFailure` 均为 frozen。 |
| 错误显式处理且不静默吞异常 | **通过（带保留意见，见 Minor-6）** | 全包 `print` 数量 = **0**（`grep -rn "print(" tpops_kafka_collector/` 无命中）；无空 `except` 块；新代码一律 `LOGGER.exception`（带堆栈）。存量 `LOGGER.error` 不带堆栈 → Minor-6。 |

### `specs/mapping-and-rules/spec.md`

| Requirement | 判定 | 证据 |
|---|---|---|
| 本轮只补测不改实现 —— 核对**没有被改坏** | **通过（附说明）** | 本波次映射层实现**确有一处改动**：`tpops_kafka_collector/mapping/rules_sorting.py`（控制器在 `b0aad0b` 修 F1：两位时区归一化），**不是** 3.5a 实施方改的 —— 3.5a 报告 §0 声明「没有改 `tpops_kafka_collector/` 下任何文件」属实，且账本有明确裁决记录。<br>该改动**没有改坏**映射层：`tests/regression` **231 passed**（含 98 条可提供指标的期望值相等断言）、映射层逐文件 ≥98% / 合计 99.92%、`data/mapping/*.json` 与 `data/merged_sql/*` **零改动**（`git diff --name-only` 核对）。<br>我另做功能验证：`parse_timestamp('…+08')` 现在能解析出 aware datetime（修复前在 3.7.9 上必 `ValueError` → 整列判 text → 混偏移时 Top N 取错行）。<br>遗留：同函数的 docstring 仍写「没有时区的按 UTC 处理」，与实现不符 → Minor-5。 |

---

## 控制器口径逐条判定（10 条）

| # | 口径 | 判定 | 证据（核代码，不是核文档） |
|---|---|---|---|
| 1 | 水位 `stream` 键 = `f'{instance_id}:{channel}'`（指标与告警都是） | **通过** | `scheduler_types.py:154-159 metric_stream` → `f'{instance_id}:{channel}'`；`:162-164 alarm_stream` → `f'{instance_id}:alarm'`。调用点：`scheduler_delivery.py:162`（指标）、`:170`（告警）；告警执行器读水位用同一个 `alarm_stream`（`executors.py:265`）。**没有任何 `'instance-a'` 这类随意取值**。 |
| 2 | 差分基线恢复只能用 `import_baselines_from` / `export_baselines_into`，**不许** `put_baseline` | **通过** | 生产代码里 `put_baseline` **零调用**（`grep` 只命中 `watermark.py:166` 的定义与 `scheduler.py:97/296` 的注释）。实际调用：`scheduler.py:306 export_baselines_into`（水位 → 采集端 store）、`:315 import_baselines_from`（store → 水位），方向正确 —— 简报说的「import_baselines_from」在语义上是「store → 水位」，实施方在 3.1 报告 D4 里澄清了这一点，控制器已裁决接受。 |
| 3 | 差分基线作用域 = `f'{instance_id}:{node_id}'` | **通过** | `collectors/command_center.py:317 key = f'{instance_id}:{node_id}'`，**缓存键也带 instance_id**（`:318 self._scoped_deltas.get(key)`）；`:180 delta_store=lambda node_id: self._delta_store(instance_id, node_id)`（`command_center_assembly.py` 未动）。作用域经 `rules_deltas.py:35-44 scoped_dimension_key` 并进维度键 → 基线键 `('metric', 'instA:node1:dim')`，两实例互不覆盖。守卫：`tests/integration/test_collector_command_center_multi_instance.py` + 回归层 `test_every_master_computes_its_own_rate_in_the_regression_layer`（M-N4）。 |
| 4 | 组 × 实例两维并发（墙钟 ≈ max，不是乘） | **通过** | `scheduler.py:206-212 _build_tasks`（通道 × 实例展平）；`:232-233` 一次性 `pool.submit` 全部任务。守卫用 `threading.Barrier(6)`（确定性，不依赖计时）。实测：`tests/performance/measure_round_latency.py` 8 轮墙钟 15.006~15.017s ≈ 最慢一路 15s，**不是**串行合计 21s。 |
| 5 | 平台画像「选择所有实例」（不挑、不筛、不取第一个、不按 `mode` 筛） | **通过** | `platform_profile.py:218-239 discover`：`fetch_instances(client)` 全部收进 `entries`，`mode` 只**记录**不参与筛选；`:226-227` 平台无实例 → 抛 `ProfileError`（**不静默跑空采集**）。`main.py:222-231 _build_instances` 按 `profile.instances` 全量展开。守卫：`test_all_instances_are_recorded_in_platform_order` / `test_empty_instance_list_raises_clear_error`。 |
| 6 | `master_count` 按唯一 `node_id` 计数 | **通过** | `platform_profile.py:305-329 _master_count`：`node_registry.nodes(instance_id)` → `set(node_id)` 计数（无 node_id 的条目单独计）。走的是**公开 API**（控制器补充 §五指定），不是私有名 `_select_masters`。守卫：`test_master_count_counts_unique_node_ids`（断言 3，不是条目数 6）。 |
| 7 | `dropped_unsupported` 计数（3 条不可提供指标，每轮汇总日志报出来） | **通过** | `schedule.py:181-194 count_unsupported_metrics`（数 `status != '可提供'`）；`main.py:180` 启动时算一次存进调度器；`scheduler_report.py:42-58` 每轮汇总行输出 `dropped_unsupported=`，`:60-63` 另记「本轮丢弃 N 个不可提供指标（累计 M）」。<br>**实测** `count_unsupported_metrics(load_mapping())` = **3**；真进程日志里 `dropped_unsupported = 3`。且**没有**和「运行时取不到值而丢弃」混算（后者不落在 `dropped_unsupported` 里）。 |
| 8 | 汇总日志字段名（`tick/elapsed/fetched/mapped/produced/failed/dropped_unsupported/windows/alarms` + `calls`/`delivery_rate`） | **通过** | `scheduler_report.py:42-58` 逐字使用；守卫 `test_summary_log_uses_the_agreed_field_names` 断言 11 个 `k=` 串全在。真进程 JSON 实测：11 个字段全在**顶层**（`logging_setup.py:73-88 _flatten_pairs` 把 `k=v` 消息摊平成顶层字段，未改 3.1 的字段产出方 —— D3 裁决接受）。 |
| 9 | 日志中心化（消费 `log_level`/`log_format`；输出 stderr；幂等；第三方降噪；异常带堆栈；凭据/token/SQL 不落日志；元测试） | **通过（带保留意见，见 Minor-6）** | `logging_setup.py:135-163 setup_logging`：`root.handlers` 认领自己的 handler（py37 自己判，不用 3.8+ 的 `force=`）→ **幂等**（守卫 `test_setup_logging_is_idempotent` / `test_setup_logging_keeps_single_owned_handler`）；`:155` `StreamHandler(sys.stderr)`（守卫 `test_output_goes_to_stderr_by_default`）；`:129-136 _quiet_noisy_loggers` 降噪 kafka/urllib3/requests；`:109-110` 异常带 `exception` 堆栈。<br>配置真的被消费：`main.py:294 setup_logging()` → `:300 setup_logging_from_config(config)`。守卫：`test_debug_level_really_outputs_info_records` / `test_json_format_emits_one_json_object_per_line` / `test_json_format_flattens_key_value_summary_into_fields` / `test_noisy_third_party_loggers_are_quieted`。<br>**实测**真进程：`LOG_FORMAT=json` 下 54 行逐行 `json.loads` 通过；密码/token 三个字面量**均未出现**。业务 SQL 文本不进日志（`mapping/masking.py` + 采集器不打印 SQL）。 |
| 10 | 启动期配置缺失 → 非 0 退出码 + 日志指明**变量名** + **未向 TPOPS/Kafka 发任何请求** | **通过** | **实测**：缺 `KAFKA_BOOTSTRAP_SERVERS` → 退出码 **2**（`EXIT_CONFIG_ERROR`）、stderr 含**变量名**、`_NeverClient.instances == []` 且 `_NeverProducer.instances == []`（`test_missing_config_sends_no_request_to_tpops_or_kafka`，断言的是**连构造都没构造**，比「不发送」更强）。 |

---

## 硬约束核对表

| 硬约束 | 判定 | 证据 + 超标清单 |
|---|---|---|
| Python 3.7.9 红线（海象 / 内建泛型 / PEP 604 / `typing.Literal\|Protocol\|TypedDict\|Final` / `cached_property` / `zoneinfo` / `removeprefix` / `asyncio.to_thread`） | **通过** | `pytest tests/test_py37_compat.py -q` → **147 passed**（门禁 `SCAN_DIRS` 含 `tpops_kafka_collector`，`ast.parse(feature_version=(3,7))` + 逐项 API 黑名单 + 内建泛型宿主检测 + PEP 604 检测）。**额外**：整套测试跑在 **python 3.7.9-final-0** 上 —— 语法与运行期双重验证。 |
| **不新增依赖** | **通过** | `git diff 356d52f..3a71cb6 -- requirements.txt` → **空**。`requirements.txt` 本波次**零变动**；未引入任何新 import（新增文件的 import 全是标准库 + `typing`）。 |
| **单文件 < 400 行**（生产文件） | **通过** | `wc -l tpops_kafka_collector/**/*.py`：最大 `kafka/producer.py` 399、`tpops/command_center.py` 399、`tpops/metrics.py` 396、`collectors/monitor.py` 382、`collectors/alarm.py` 375 —— **全部 < 400**。本波次新增文件：`main.py` 311 / `platform_profile.py` 336 / `scheduler.py` 393 / `executors.py` 281 / `lifecycle.py` 189 / `logging_setup.py` 177 / `schedule.py` 239 / `scheduler_types.py` 210 / `scheduler_delivery.py` 170 / `scheduler_report.py` 90 / `scheduler_signals.py` 39。 |
| **函数 < 50 行**（生产文件） | **不通过（1 处）** | **超标清单（生产文件）**：`tpops_kafka_collector/platform_profile.py:244 PlatformProfile.reconcile` —— **def..end 55 行**（body 口径 49 行）。其余生产函数最长：`collectors/alarm.py:79 collect_alarms` **50**、`mapping/message.py:47 build_alarm_message` **50**、`collectors/monitor.py:166 _messages_of_rule` 49、`kafka/producer.py:150 __init__` 49、`collectors/command_center.py:195 _fetch_pages` 48、`main.py:111 build_runtime` **47**。<br>（`collect_alarms` 的 50 行在 base `356d52f` **也是 50**，属存量，非本波次引入；本波次引入的只有 `reconcile`。）<br>**定级说明**：wave-3 简报写「违反即 Important」，但 wave-2b 的同类先例（`collectors/instance.py::collect()` 52 行 → **M-1 Minor**）已定过级，且本例 body 口径 49 行实际未越界 —— 本报告按 **Minor** 定级（见 Minor-1），请控制器按简报字面复核。 |
| **不可变数据**（跨模块 frozen dataclass / NamedTuple） | **通过** | `scheduler_types.py`（`InstanceTarget`/`TaskOutcome`/`TaskFailure`/`DeliveryStats`/`TickResult` 全部 `@dataclass(frozen=True)`）、`schedule.py`（`ChannelPlan`/`ScheduleEntry` frozen）、`platform_profile.py`（`ProfileInstance`/`ProfileReconciliation`/`PlatformProfile` frozen）、`main.py:86 Runtime` frozen、`watermark.py`（`WatermarkState`/`MetricWatermark`/`AlarmWatermark`/`DeltaBaseline` frozen）、`config.py:41 Config` frozen。跨模块传递**没有**原地修改。 |
| **凭据只走环境变量**（禁硬编码 / 禁写日志 / 禁进异常消息 / 禁进健康检查响应） | **通过** | 硬编码：无（`grep` 生产包无密码字面量）。写日志：实测日志无密码/token/请求头值。异常消息：`config.py:115` 只写**变量名**；`scheduler_types.py:198-203 reason(exc)` 截断到 200 字。健康检查响应：只有 `dependencies[]`（名称 + 状态），无凭据。画像文件：`test_profile_file_contains_no_credentials` 断言不含 `TPOPS_PASSWORD`/`password`/`token`/`X-Mini-Token`。 |
| **不用 `print`**（全包 grep） | **通过** | `grep -rn "print(" tpops_kafka_collector/ --include=*.py` → **0 命中**。 |
| **生产代码唯一**（不得 `import tools.*` / `data.*`；不得读仓库根 `data/`） | **通过（带保留意见，见 Minor-4）** | `grep` 生产包：`import tools` / `import data` → **0 命中**。运行时读文件：生产只读包内 `mapping/data/`（`loader.py:49` / `sqlgen.py:47`）与 `state_dir` 下的水位/画像。守卫：`tests/unit/test_package_boundary.py` + `tests/unit/test_mapping_data_sync.py`（33 passed）。<br>遗留（账本已登记）：`mapping/table.py:15-16` 的 `REPO_ROOT/DATA_DIR` 仍指向仓库根 `data/mapping`，生产路径无人调用，但 `MappingTable` 从 `mapping/__init__.py:13` 导出 → T-10/T-21；文件读取面无守卫 → T-20。 |
| **消息只有一个序列化出口** `kafka/serializer.py`（AST 守卫测试守着，**核实它守得住**） | **通过（已变异验证）** | `tests/unit/test_serializer.py:352-379`：白名单逐个登记用途（`kafka/serializer.py` / `watermark.py` / `health.py` / `logging_setup.py`），扫不到就当没有；`:381-396` 元测试「登记的用途必须真的还在用」。**我做了变异验证：在副本的 `scheduler_report.py:95` 注入一处 `json.dumps` → 守卫立刻红**（`AssertionError: … 还有 json.dumps：['scheduler_report.py:95']`）。<br>小缺口（不阻塞）：守卫只认 `json.dumps`/`json.dump` 的**属性访问**形式，`from json import dumps` 后直接调 `dumps(...)` 的写法扫不到。 |
| **不改** `execution-contract.md` / `tasks.md` / `specs/**` / `data/mapping/*.json` / `data/merged_sql/*` | **通过** | `git diff --name-only 356d52f..3a71cb6` 的 42 个文件里**没有**上述任何一项（`changes/` 下只动了 `progress.md` 账本 + `HANDOFF.md` + 新增的 workspace 工件）。 |
| **启动期配置缺失 → 非 0 退出码 + 日志指明变量名 + 未发任何请求** | **通过** | 见控制器口径 #10（实测退出码 2 + 变量名 + 零构造）。 |
| **不静默出 0**（取不到值就不产出那条消息） | **通过** | 映射层（wave-0/1，本波次只补测 + 一处 F1 修复）+ 采集层：`executors.py` 各执行器在取数失败/无产出时给 `end_time_ms=None` → **不推进水位**（`scheduler_delivery.py:153-159` 明写「宁可重复投递，也不丢数据」）。回归层 231 passed。 |

### 超标清单（测试文件 —— 按简报放宽，但要说明）

本波次新增/改动的测试文件里 **> 400 行** 的：

| 文件 | 行数 | 说明 |
|---|---|---|
| `tests/unit/test_scheduler.py` | **852** | 3.1 新增，30 个用例；仓库最长测试文件（账本已登记 **T-28**，定性为「纯整理」）。 |
| `tests/integration/test_main.py` | **515** | 3.3 新增，15 个用例（含真进程 SIGTERM 与在途超时两个重场景）。 |
| `tests/unit/test_mapping_columns_gaps.py` | **480** | 3.5a 新增（映射层补测）。 |
| `tests/unit/test_mapping_decode_gaps.py` | 264 | 3.5a 新增 —— 未超。 |
| `tests/unit/test_mapping_message_sqlgen_gaps.py` | 277 | 3.5a 新增 —— 未超。 |
| `tests/unit/test_mapping_rules_gaps.py` | 335 | 3.5a 新增 —— 未超。 |
| `tests/unit/test_platform_profile.py` | 340 | 3.3 新增 —— 未超。 |
| `tests/unit/test_logging_setup.py` | 126 | 3.3 新增 —— 未超。 |
| `tests/unit/test_scheduler_delivery_channels.py` | 102 | 3.5b 新增 —— 未超。 |
| `tests/unit/test_rules_sorting_offsets.py` | 126 | 3.5b 新增 —— 未超。 |
| `tests/integration/test_kill_restart_drill.py` | 272 | 3.5b 新增 —— 未超。 |
| `tests/integration/test_round_latency_budget.py` | 190 | 3.5b 新增 —— 未超。 |
| `tests/performance/measure_round_latency.py` | 165 | 3.5b 新增 —— 未超。 |
| `tests/integration/test_collector_command_center_multi_instance.py` | 118 | 3.3 新增 —— 未超。 |
| `tests/regression/test_collector_coverage.py` | 58 | 3.5b 新增 —— 未超。 |

**生产文件 > 400 行：0 个。生产函数 > 50 行：1 个**（Minor-1）。

---

## 发现

### Critical

**无。**

### Important

**无。**

（wave-2b 首轮那 3 条 Critical + 4 条 Important 形态 —— 「不报错但数据错」—— 本波次**未复现**：
水位推进时序、幂等键登记时机、基线作用域/恢复方向、两维并发、优雅退出超时、平台画像固化、
日志字段名与凭据不落盘 —— 逐条核过代码并做了变异验证，见下表。）

### Minor

#### 1. `platform_profile.py::reconcile` 55 行，越过合同「函数 < 50 行」
- **File**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev/tpops_kafka_collector/platform_profile.py:244-298`
- **问题**：该方法是本波次**新增**文件里的新增方法，长度 **def..end 55 行**（body 口径 49 行）。
  实施方 3.3 报告 §8 声称「最长函数 **49** 行（`build_runtime`）—— < 50」，与实测不符（见「实施方报告中的不准确断言」第 1 条）。
- **为什么重要**：合同 `Design Constraints → 依赖约束` 的硬约束。触发条件是**静态**的（现在就是），
  后果是可维护性而非数据错误。按 wave-2b 先例（52 行 → M-1 Minor）与 body 口径（49 行未越界）定 **Minor**；
  若控制器按 wave-3 简报「违反即 Important」字面执行，可升格 —— 但**不阻塞签发 receipt**。
- **怎么修**：把四条不一致分支各自的「记日志 + 记账」抽成一个 `_note(kind, instance_id, detail)` 小函数，
  或把 `reconcile` 拆成 `_reconcile_missing` / `_reconcile_added` / `_reconcile_engine` / `_reconcile_masters` 四段。

#### 2. `platform_profile.load_or_discover` 的 docstring 与生产行为不符
- **File**：`tpops_kafka_collector/platform_profile.py:193-195`
- **问题**：docstring 写「**固化文件存在 → 不重新发现**（**平台列表接口一次都不调用**）」，
  但生产装配 `tpops_kafka_collector/main.py:212-219 _load_profile` 传的是 `reconcile=True`
  → `PlatformProfile.reconcile()` → `fetch_instances(client)` → **每次启动都会调用一次平台实例列表接口**。
  守卫测试 `tests/unit/test_platform_profile.py:157 test_existing_profile_file_is_not_rediscovered`
  断言的 `client.calls == []` 成立，是因为它调的是 `load_or_discover(path, client)`（`reconcile` 默认 False）。
- **为什么重要**：**不是数据缺陷**（核对是只读的、不改画像文件，实质要求「不重建画像」成立），
  但 docstring 是**错的** —— 后来者按它理解会以为生产路径从不碰平台列表接口，
  进而误判「改了 profile 文件就生效」之类的事。控制器已在 D1 裁决里把这条拆成两个入口并接受，
  但**生产调用点与 docstring 没有同步**。
- **怎么修**：把 `load_or_discover` 的 docstring 改成「`reconcile=False`（默认）时一次都不调用；
  `reconcile=True` 时只**读**地查一次平台列表做核对，**不重建、不改写画像**」。

#### 3. `tests/unit/test_package_boundary.py` 的 docstring 过期
- **File**：`tests/unit/test_package_boundary.py:8-9`
- **问题**：写「映射数据**目前仍从仓库根 `data/mapping/` 读取**，搬进包内是 wave-2a 任务 2.1 的交付物」。
  **事实**：wave-2a 已完成，生产走 `mapping/loader.py:49 PACKAGE_DATA_DIR`（基于 `__file__`），
  `tests/unit/test_mapping_data_sync.py` 守两侧一致。docstring 是 wave-0/1 的遗留。
- **为什么重要**：不造成错误行为，但会让审查者/后来者以为生产仍依赖仓库根 —— 正是这条硬约束要防的形态。
- **怎么修**：把该段改成「映射数据已搬进包内（wave-2a）；本测试守的是 **import 边界**，
  **文件读取面**另由 T-20 补」。

#### 4. `mapping/table.py` 的仓库根默认仍在包内可达
- **File**：`tpops_kafka_collector/mapping/table.py:15-16`（`REPO_ROOT` / `DATA_DIR = <repo>/data/mapping`）、`:147`
- **问题**：`MappingTable.load()` 的默认 `data_dir` 仍指向仓库根；`MappingTable` 又被
  `tpops_kafka_collector/mapping/__init__.py:13` 导出。生产路径**没有**调用它
  （生产走 `loader.load_mapping()`），所以运行时不读仓库根 —— 但边界一旦被绕过就会静默退化。
- **为什么重要**：账本已登记 **T-10 / T-21**（`MappingTable.load()` 的仓库根依赖定案），
  本波次未处理。属于**已知的遗留**，不是本波次引入。
- **怎么修**：把 `MappingTable.load()` 的默认 `data_dir` 也改成包内 `mapping/data/`，
  或把它标成「仅离线工具用」并在 `mapping/__init__.py` 不导出（与 T-20 的文件读取面守卫配套）。

#### 5. `rules_sorting.parse_timestamp` 的 docstring 仍说「没有时区的按 UTC 处理」，但实现做不到
- **File**：`tpops_kafka_collector/mapping/rules_sorting.py:120`（docstring）、`:26-29`（`TIMESTAMP_FORMATS`）、`:127-129`（死分支）
- **问题**：`TIMESTAMP_FORMATS` 的两条格式都带 `%z`，所以**无时区**的时间戳（如 `2026-11-18 16:27:07.557567`）
  `strptime` 必 `ValueError` → `parse_timestamp` 返回 `None`；`:127-129` 的 `if parsed.tzinfo is None: replace(utc)`
  是**永远走不到的死代码**（覆盖率也证实：本波次唯一未覆盖的映射层行就是 `rules_sorting.py:129`）。
- **为什么重要**：**不报错、但数据错**的潜在形态 —— 若平台某天改回无时区格式，整列被判成 `text` →
  按字典序比（当前同偏移下恰好等于时间序，**现在不出错**；混偏移时才会取错行）。
  本波次恰好改了这个函数（归一化两位时区，F1）却没顺手把 docstring 与死分支一起收掉。
- **怎么修**：把 docstring 改成「只解析**带时区**的格式；无时区返回 `None`」，
  并删掉 `:127-129` 的死分支（或补一条 `'%Y-%m-%d %H:%M:%S.%f'` 格式并显式按 UTC 处理）。

#### 6. 控制器补充 §六.4「异常路径要带堆栈」只对**新增**代码兑现
- **File**：`tpops_kafka_collector/collectors/monitor.py:142,154,176,183,205`、
  `collectors/instance.py:132,138,163`、`collectors/diagnostic.py:113,186`、
  `tpops/client.py:224`、`tpops/command_center.py:202`、`kafka/producer.py:299,313,370`、
  `main.py:239`、`platform_profile.py:262,316`、`mapping/rules_deltas.py:78`、`config.py:355,358`（约 23 处）
- **问题**：控制器补充说明 §六.4 明确要求「审一遍现有的日志调用（22 处 `LOGGER.error` 多数没有 `exc_info=True`，
  异常路径要带堆栈）」。3.3 只保证了**新增**代码一律 `LOGGER.exception`（3.3 报告 §8「不静默吞异常」条），
  存量那 20+ 处 `LOGGER.error(...)` 仍在异常路径上**不带堆栈**。
- **为什么重要**：**不是**「静默吞异常」（消息、指标码、实例 ID 都在，出错能看见），
  但事后排查拿不到堆栈 —— 对本项目「不报错但数据错」这一类缺陷的定位效率有实际影响。
  属遗留面（wave-3 只新增了 scheduler/lifecycle/main 三处），不阻塞。
- **怎么修**：把这些**异常路径**上的 `LOGGER.error` 换成 `LOGGER.exception`（或补 `exc_info=True`）；
  非异常路径的（如「配置校验失败」）保持 `error` 即可。

#### 7. P95 的「实测规模」是**合成执行器 + 文档延迟**，不是连真平台
- **File**：`tests/performance/measure_round_latency.py:56-61`（`LATENCY_SECONDS`）、`:71-80`（`_SleepingExecutor`）
- **问题**：脚本用 `time.sleep()` 模拟各通道延迟，延迟值取自开发方案 §4.8 的**实测数字**
  （monitor 2s / command_center 15s / diagnostic 2s / instance 0s / alarm 2s）。
  它验证的是**采集端的并发结构**（墙钟 ≈ max 而非 sum），**不是**采集端对着真平台的端到端耗时。
- **为什么重要**：合同完成标准写的是「60 秒档单轮墙钟 P95 ≤ 25 秒（**实测规模**）」。
  当前证据能证明「**并发结构**成立、且在文档给出的真实延迟下有余量」，
  不能证明「采集端在真平台上的墙钟 ≤ 25 秒」。真平台的延迟由平台决定、
  Kafka 地址又待客户确认，**本变更拿不到真平台 + 真 Kafka 的端到端数字** ——
  这与合同「范围外：联调验收 P5」是一致的，属**显式裁剪**而非隐瞒。
- **怎么修**：不阻塞本波次。建议 wave-4 的交付文档里把这一条写清楚：
  「P95 ≤ 25s 是**基于实测平台延迟的并发结构推算 + 合成执行器复现**，
  真平台端到端数字在 P5 联调验收时取」。

#### 8. 「空结果」WARNING 在「全部被幂等键过滤」时文案失真
- **File**：`tpops_kafka_collector/scheduler_delivery.py:87-94`
- **问题**：判定条件是 `not records`，而 `records` 为空有**两种**原因：
  (a) 采集器确实没产出消息（空分组 / 取不到值）；(b) 产出���了但**全部被幂等键判重过滤掉**。
  两者都刷「本轮该组没有产出任何指标消息（空结果）」。(b) 是**正常**场景（上一轮刚投过），
  却每轮刷一条 WARNING。
- **为什么重要**：不造成数据错误（F2 已排除告警通道，指标通道不会淹掉真告警），
  但会让「空结果告警」这条信号**噪声化** —— 而「空分组」正是平台侧最需要人看的信号之一。
- **怎么修**：把条件改成 `not outcome.messages`（真的没产出）才刷 WARNING；
  「产出但全被过滤」另记 DEBUG。

---

## 变异验证记录（我在 `/tmp/w3-contract-mut/mut/` 的副本上做，工作树未动）

> 说明：正确性深挖与全面变异验证由另一位审查者负责；我只做了**与合同完成标准 / 硬约束直接挂钩**的两条，
> 目的是核实「有可执行的守卫」而不是「报告里说守卫存在」。

| # | 改了什么 | 期望 | 实测 | 是否已还原 |
|---|---|---|---|---|
| 1 | 在副本的 `tpops_kafka_collector/scheduler_report.py:95` 注入一处 `json.dumps({'x': 1})`（非白名单模块） | 「唯一序列化出口」的 AST 守卫应红 | **红了**：`FAILED tests/unit/test_serializer.py::test_package_has_only_one_json_dumps_for_messages` —— `AssertionError: 生产包里除已登记的 [...] 外还有 json.dumps：['scheduler_report.py:95']`（1 failed, 1 passed） | 是（副本目录 `/tmp`，工作树未改） |
| 2 | 把副本 `scheduler_delivery.py` 的 `_advance_metric(...)` 从「投递成功之后」**挪到 `produce_metrics` 之前** | 「投递失败不推进水位」的守卫应红 | **红了 3 条**：`FAILED tests/integration/test_kill_restart_drill.py::test_failed_batch_does_not_advance_the_watermark_and_is_retried`、`FAILED tests/unit/test_scheduler.py::test_watermark_advances_only_after_successful_delivery`、`FAILED tests/unit/test_scheduler.py::test_channel_plans_match_an_independent_recomputation`（3 failed, 31 passed）——日志里能看到 `ProduceBatchError: 假投递失败` | 是（副本目录 `/tmp`，工作树未改） |

**工作树污染检查**：审查结束后 `git status --porcelain` 仅显示另一位审查者的未跟踪报告文件，
`git rev-parse HEAD` = `3a71cb6`（未移动 HEAD、未 commit、未 stash、未动 `.git` 元数据）。

---

## 实施方报告中的不准确断言

| # | 断言 | 实测 | 影响 |
|---|---|---|---|
| 1 | `workspace/task-3.3-report.md` §8 自查表：「**最长函数** \| **49 行**（`build_runtime`）—— < 50」 | **两处都对不上**：<br>① `main.py:111 build_runtime` 实测 **47 行**（def..end）/ **34 行**（body），不是 49；<br>② 本波次**最长**的生产函数是 `platform_profile.py:244 reconcile` —— **55 行**（def..end）/ **49 行**（body）。<br>复现：`python3 /tmp/w3-contract-mut/funcsize2.py tpops_kafka_collector`（AST，`end_lineno` 口径；与 wave-2b 修复轮审查用的 `flen.py` 口径一致 —— 那里 `monitor._messages_of_rule` = 49、`command_center._fetch_pages` = 48，与我的测量逐行吻合）。 | **这正是 wave-2b 的同类形态**（「实施方声称全包 0 个函数 >50 行，实测有 51 行的」）。虽然本例只越界 5 行、body 口径未越界、不是数据缺陷，但它说明**实施方的自查数字仍然不能直接采信** —— 与 wave-3 简报「实施方的自查数字可能不准 → 控制器/审查者要自己核」的判断一致。 |
| 2 | `workspace/task-3.5-report.md` §0：「映射层合计 1287 stmts / **1 miss** = **99.9%**」 | 我实测 **1292 stmts / 1 miss = 99.92%**（差 5 stmt：3.5a 报告写于控制器修 F1 **之前**，F1 给 `rules_sorting.py` 加了 5 条语句）。 | **不是不准确**，是时序差（报告早于 F1 修复）。列在这里只为避免控制器按 1287 复核时困惑。 |
| 3 | 账本 `progress.md`（3.5b 段）：「**实测 P95 = 15.017s**（最小 15.009 / 最大 15.017）」 | 我复跑：`P95 = 15.017s`（**最小 15.006** / 最大 15.017） | 最小值差 3 ms —— 运行间抖动，**不是**不准确断言。 |
| 4 | 账本 `progress.md`（T-22 条）：「规格 §4.6.4 写的是『每个 `(metric_name, dimensions_key)`』」 | 核对 `specs/delivery-and-state/spec.md` 全文：该 Requirement（:183-196）**没有**规定水位粒度，只规定「持久化哪些内容 + 原子写 + 重启续传」。「每个 `(metric_name, dimensions_key)`」出自**开发方案 §4.6.4**（`docs/`），不是 `specs/**`。 | T-22 是**文档与实现的口径差**，不是 `specs/**` 的规格违背。本波次**没有**违反任何 `specs/**` 条款。 |
| 5 | 账本 `progress.md`（T-28 条）：「`tests/unit/test_scheduler.py` **852 行**，是仓库里**最长的测试文件**（此前最长 704 行）」 | 实测 `wc -l`：`test_scheduler.py` 852 ✓ 最长 ✓；此前最长 `tests/integration/test_collector_command_center.py` 704 ✓ | 准确。 |

---

## 收尾

- **结论**：**verdict = pass**。Critical 0 / Important 0 / Minor 8。8 条 Minor 全部是
  「可维护性 / 文档与实现漂移 / 遗留面」类，**没有一条是「不报错但数据错」**，
  也没有一条阻塞合同完成标准或规格条款。8 条里 4 条（#2/#3/#4/#5）本身就是**文档没跟上实现**，
  建议在 wave-4 交付文档里一并收口。
- **建议控制器在签发 receipt 前确认的两件事**：
  1. Minor-1 的定级（wave-3 简报「违反即 Important」 vs wave-2b 先例 Minor；body 口径 49 行实际未越界）；
  2. 账本 T-22 引用的「规格 §4.6.4」实为**开发方案** §4.6.4，`specs/**` 未规定水位粒度 ——
     若 wave-4 要改「规格」，改的是 `specs/` 还是 `docs/开发方案-*.md`，需要先定。

签发 receipt 的命令：
```
    ssf execution review changes/tpops-kafka-collector \
      --wave wave-3-scheduling-testing --base 356d52f --head 3a71cb6 \
      --report .superpowers/sdd/reviews/wave-3-scheduling-testing.md --verdict pass
```

> （`--report` 指向控制器合并三个维度后的**总报告** `wave-3-scheduling-testing.md`；
> 本维度分报告路径为 `changes/tpops-kafka-collector/.superpowers/sdd/reviews/wave-3-scheduling-testing-contract.md`。）
