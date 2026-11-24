# Wave 3 定向复审报告（维度：五条行为缺陷）—— wave-3-scheduling-testing

> 复审方：**R2-A**（定向复审，只核 wave-3 修复轮的五条行为缺陷是否真闭环）。
> **不采信实施方报告里的任何数字** —— 下面每一条都是自己跑出来的。

- **base** `3a71cb6` / **head** `2305cea`
- **工作树**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（分支 `tpops_metric_dev`）
- **结论**：**verdict = pass**
- **计数**：**Critical 0 / Important 0 / Minor 4**

> 判定口径（本项目最贵的一类缺陷是「不报错、但数据没有 / 数据错」）：
> **每条修复，问「有没有一个能让它失败的小改动？」—— 没有守卫测试的修复不算修完。**
> 本轮 5 条缺陷**每一条都有守卫，且每一条我都亲手做了变异**（改坏 → 我的断言变红 → 还原）。
> 变异全部在 `/tmp/r2a-mut` 副本里做，**工作树零改动**（收尾 `git status --porcelain` 为空）。

---

## 零、结论速览

| # | 缺陷 | 判定 | 关键证据 |
|---|---|---|---|
| A1 | 采集节奏对齐 tick 边界 | **已闭环** | 独立推演：档位 30/60/120 秒的通道实测间隔 **30.0 / 60.0 / 120.0 秒**（×1.0）；变异「睡满整间隔」→ 我的 2 条 + 实施方 1 条守卫红 |
| A2 | 退出不再挂死 | **已闭环** | 在途请求挂死 → 进程 **退出码 0**、放弃窗口内退出、水位文件完整；变异 `os._exit`→`sys.exit` → 2 条守卫红（45.56 s 超时失败） |
| A3 | 单轮外部调用数是真实值 | **已闭环** | 平台留档回放：**第 1 轮上报 26 = 传输层实数 26**，第 2 轮 13 = 13；变异「写死常数」→ 6 条守卫红 |
| A4 | 节点清单失败不再永久降级 resourceName | **已闭环** | 真重试 3 次、该实例不进采集、全失败 → 退出码 3 且 Kafka producer 未构造、`exc_info` 带堆栈；两种变异各 1 条守卫红 |
| A5 | 运行期每轮落盘 | **已闭环** | SIGKILL 三阶段演练通过；`save()` 写全 4 段；变异「去掉每轮落盘」→ 3 条守卫红（含 SIGKILL 演练）；落盘开销 **p95 = 12.0 ms**（占 30 秒一轮的 **0.04%**） |

**另有一个需控制器裁决的口径项（不是本轮的缺陷）**：真实计数落地后，命令中心通道按页下发多条合并 SQL（每条 = 下发 + 轮询 + 每个 master 取明细 = 5 次请求），生产单轮**大概率 > 42** → 预算 WARNING 会真的响。这是**规格要求的正确行为**（上报真值），但 42 这个阈值是按旧口径估的 —— 详见「发现 · Minor 3」。

---

## 一、逐条核验

### A1 采集节奏对齐 tick 边界

- **判定**：**已闭环**
- **代码位置**：
  - `tpops_kafka_collector/lifecycle.py:99-122`（`run_loop`：取 `started = clock()` → 跑一轮 → `_wait_for_next_tick`）
  - `tpops_kafka_collector/lifecycle.py:134-157`（`_wait_for_next_tick`：`remaining = interval - elapsed`；`remaining <= 0` → 只记 WARNING、**不睡、不追赶**）
  - `tpops_kafka_collector/lifecycle.py:125-131`（`_poll_interval_seconds`）

**① 读代码确认逻辑正确**（不是又变成「睡满间隔」）：

```python
# lifecycle.py:146-157
    if interval <= 0:
        return
    elapsed = clock() - started
    remaining = interval - elapsed
    if remaining <= 0:
        LOGGER.warning('本轮耗时 %.3f 秒，超过轮询间隔 %.3f 秒 —— 重新锚定到当前时刻'
                       '（不堆积、不累积漂移）', elapsed, interval)
        return
    waiter(remaining)
```

`started` 在**跑一轮之前**取 → `elapsed` 含采集耗时 → 睡的是「剩余时间」。**不是睡满间隔**。✓

**② 我自己写的独立推演**（`/tmp/r2a-mut/tests/unit/test_r2a_a1.py`，**不复用实施方的测试**）：
真 `Scheduler` + 真 `DueTable` + 真 `run_loop`，只有平台与时钟是假的；自己写了一个线程安全的假时钟（一轮只推进一次，避免并发丢更新）。

```bash
$ cd /tmp/r2a-mut && conda run -n tpops python -m pytest tests/unit/test_r2a_a1.py -q
...                                                                      [100%]
3 passed in 0.97s
```

实测的**相邻两次采集间隔**（`_r2a_a1_probe.py` 输出）：

```
alarm            tier=30    执行 10 次  间隔=[30.0, 30.0, 30.0, 30.0, 30.0, 30.0, 30.0, 30.0, 30.0]
command_center   tier=60    执行  5 次  间隔=[60.0, 60.0, 60.0, 60.0]
diagnostic       tier=120   执行  3 次  间隔=[120.0, 120.0]
instance         tier=3600  执行  1 次  间隔=[]
monitor          tier=30    执行 10 次  间隔=[30.0, 30.0, 30.0, 30.0, 30.0, 30.0, 30.0, 30.0, 30.0]
```

**全部 = 档位 × 1.0**（断言阈值是 ×1.1）。档位取自生产映射表实算：
`monitor=30 / command_center=60 / diagnostic=120 / instance=3600 / alarm=30`
（`compute_channel_plans(load_mapping())`，命令中心覆盖 **82/98** 条客户指标）。

**③ 变异**（`lifecycle.py` 的 `waiter(remaining)` → `waiter(interval)` = 修复前的「睡满整间隔」）：

```bash
$ conda run -n tpops python _r2a_a1_probe.py
alarm            tier=30    执行 9 次  间隔=[33.0, 36.0, 33.0, 34.0, 33.0, 31.0, 33.0, 34.0]
command_center   tier=60    执行 5 次  间隔=[61.0, 67.0, 64.0, 62.0]

$ conda run -n tpops python -m pytest tests/unit/test_r2a_a1.py -q
FAILED tests/unit/test_r2a_a1.py::test_channel_intervals_are_within_tier - As...
FAILED tests/unit/test_r2a_a1.py::test_no_long_term_drift - AssertionError: ...
2 failed, 1 passed in 0.98s

$ conda run -n tpops python -m pytest tests/unit/test_lifecycle.py -q
FAILED tests/unit/test_lifecycle.py::test_channel_interval_is_aligned_to_the_tick_boundary
1 failed, 4 passed in 0.25s
```

**我的断言变红了 ✓**（实施方的守卫也红）。**已还原**（`diff -r` 与工作树逐字节一致，见文末「收尾」）。

**④ 有没有引入新的节奏问题**（补偿风暴 / 连续两轮紧挨着跑 / 长期漂移）：

- 我自己写的 `test_no_two_consecutive_rounds_back_to_back`（不存在相邻两轮间隔 < 档位一半）与 `test_no_long_term_drift`（第 k 次执行时刻 ≈ t0 + k × 档位）→ **修复后绿、变异后红** ✓
- 超时场景单独实测（一轮 40 s > 轮询间隔 30 s）：

```
一轮耗时 3s / 轮询间隔 30s / 档位 30s
   执行时刻 [0.0, 30.0, 60.0, 90.0, 120.0, 150.0, 180.0, 210.0]
   相邻间隔 [30.0, 30.0, 30.0, 30.0, 30.0, 30.0, 30.0]  → 档位倍数 [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]

一轮耗时 40s / 轮询间隔 30s / 档位 30s
   执行时刻 [0.0, 40.0, 80.0, 120.0, 160.0, 200.0, 240.0, 280.0]
   相邻间隔 [40.0, 40.0, 40.0, 40.0, 40.0, 40.0, 40.0]  → 档位倍数 [1.33, 1.33, 1.33, 1.33, 1.33, 1.33, 1.33]
```

  超时后间隔**恒为 40 s、严格等差** → **不堆积、不追赶、无补偿风暴、无连续两轮紧挨着跑、无长期漂移** ✓
  （修复前同场景是 40 + 30 = 70 s/轮 → 档位 ×2.33；现在 ×1.33 —— **变好**。）

**⑤ 口径澄清（Minor，见「发现」）**：`POLL_INTERVAL_SECONDS` 默认 30 **恰好等于**最紧档位 30；若运维把它配成 60，30 秒档会**静默退化成 60 秒档**（DueTable 只在 tick 时判定到期）。代码不阻止，但**没有任何日志提示**。

---

### A2 退出不再挂死

- **判定**：**已闭环**
- **代码位置**：
  - `tpops_kafka_collector/main.py:314-333`（`exit_process`：显式 flush 两个流 → `os._exit(int(code))`）
  - `tpops_kafka_collector/main.py:336-337`（`if __name__ == '__main__': exit_process(main())`）
  - `tpops_kafka_collector/scheduler_tasks.py:109-120`（`pool = ThreadPoolExecutor(...)` + `finally: pool.shutdown(wait=False)` —— 不再用 `with` 的 `wait=True`）
  - `tpops_kafka_collector/scheduler_tasks.py:137-180`（`_await_task`：停机后最多等 25 s，超时放弃并记一条 `Timeout` 失败）
  - `tpops_kafka_collector/lifecycle.py:229-240`（`shutdown_runtime`：水位落盘 → producer.close() → 停健康端点，**在 `os._exit` 之前**）

**① 独立复现「有在途请求时进程在放弃窗口内退出、退出码 0、水位文件完整」**：

```bash
$ cd /tmp/r2a-mut && conda run -n tpops python -m pytest \
    "tests/integration/test_main.py::test_in_flight_request_beyond_deadline_gives_up_and_exits_zero" \
    "tests/integration/test_main.py::test_exit_process_skips_the_atexit_hooks" -q
..                                                                       [100%]
2 passed in 25.83s
```

这条守卫已经换成了**真 `Scheduler` + 真 `ThreadPoolExecutor` + 真进程**（执行器 `time.sleep(3600)` 挂死），断言用 `communicate(timeout=45)` —— **不是测试里的假超时**。

**② 变异**（`os._exit(int(code))` → `sys.exit(int(code))` = 修复前行为）：

```bash
$ conda run -n tpops python -m pytest "tests/integration/test_main.py::test_in_flight_request_beyond_deadline_gives_up_and_exits_zero" \
    "tests/integration/test_main.py::test_exit_process_skips_the_atexit_hooks" -q
E           SystemExit: 0
tpops_kafka_collector/main.py:333: SystemExit
FAILED tests/integration/test_main.py::test_in_flight_request_beyond_deadline_gives_up_and_exits_zero
FAILED tests/integration/test_main.py::test_exit_process_skips_the_atexit_hooks
2 failed in 45.56s
```

第一条 **45.56 s 后超时失败**（进程被 py3.7 `atexit` 的 `_python_exit` → `t.join()` 拖住）→ **守卫守得住 ✓**。**已还原**。

**③ `os._exit` 的副作用（我另起进程实测，不采信文档）**：
`/tmp/r2a-probe/a2_side_effects.py` —— 真 `Scheduler` + 真 `WatermarkStore` + 记录式 producer，跑两轮后 `entry.exit_process(0)`：

```
EXIT CODE = 0
--- state dir ---
-rw-r--r-- 1 sml sml    7 Nov 24 11:37 closed.txt        ← producer.close() 被调用
-rw-r--r-- 1 sml sml   14 Nov 24 11:37 delivered.jsonl    ← 消息确实投出去了
-rw------- 1 sml sml  275 Nov 24 11:37 watermark.json
--- 水位文件内容 ---
metric_watermarks: [{'end_time_ms': 1700000000000, 'stream': 'i1:monitor'}]
idempotency_keys: ['i1|GAUSSDB.connections|component_id=c1,node_id=n1|1700000000000']
```

日志 / stdout 的 flush（`exit_process` 跳过 atexit → 缓冲不会自动刷）：

```
$ PYTHONPATH=/tmp/r2a-mut conda run -n tpops python /tmp/r2a-probe/a2_logs.py
EXIT=0
--- stdout ---
PROBE_PRINT_BEFORE_EXIT            ← print 不带 flush=True 也出来了
--- stderr ---
2026-11-24 11:37:34,617 | INFO | probe | PROBE_LOG_LINE_BEFORE_EXIT
```

→ **水位已落盘 ✓ / producer 已 flush+close ✓ / 日志已 flush ✓ / stdout 已 flush ✓**。

**④ 进程内调用 `main()` / `run()` 不会被当场杀掉**：

```
$ PYTHONPATH=/tmp/r2a-mut conda run -n tpops python /tmp/r2a-probe/a2_inproc.py
MAIN_RETURNED_CODE=3
PROCESS_STILL_ALIVE_AFTER_MAIN_CALL
PROCESS_STILL_ALIVE_AT_END
EXIT=0
```

`exit_process` 只在 `main.py:336-337` 的 `__main__` 块里调，**不在 `run()` 里** → 进程内调用安全 ✓

**⑤ 真进程 SIGTERM → 退出码 0、水位文件完整**：`tests/integration/test_kill_restart_drill.py::test_sigterm_exits_zero_with_a_complete_watermark_file_and_restart_resumes` 在全量里通过（真起 `python -m tpops_kafka_collector.main`，SIGTERM → `returncode == 0` → 水位读回 → 重启续传）✓

---

### A3 单轮外部调用数是真实值（不是写死常数）

- **判定**：**已闭环**
- **代码位置**：
  - `tpops_kafka_collector/tpops/call_tracking.py`（**新文件**，220 行）：`CallCounter`（全局锁 + 线程局部）/ `CountingClient` / `CatalogCachingClient`
  - `tpops_kafka_collector/tpops/client.py:136-137`（`self._calls = CallCounter()`）、`:139-147`（`calls` 属性）、**`:326`（`self._calls.record()` —— 计数点在 `session.request` 之前）**
  - `tpops_kafka_collector/executors.py:61-86`（`_CallMeter`）、`:89-102`（`_install_counting_client`）、`:126-132`（装配计数代理 + 目录缓存）
  - `tpops_kafka_collector/scheduler.py:242-256`（`_call_mark` / `_call_delta`，用**全局**计数）、`:332-335`（`calls = max(reported, round_calls)` —— 失败任务的调用数也进来）

**① 计数点确实在「真正发出请求」那一刻**：
`TpopsClient._send()` 是**所有**平台请求的唯一出口 —— 认证（`_authenticate` → `_send`）、401 强制刷新（`_request` → `token(refresh=True)` → `_authenticate` → `_send` + 再 `_send` 一次）、重试（`retry_call` 每次尝试都重新走 `_request` → `_send`）**全部经过它** → 全部计数。
实施方的守卫 `test_call_count_includes_retried_requests`（认证 1 + 失败 1 + 重试成功 1 = 3）验证了这一点，我复核了调用链：`client.py:198`（认证）、`:299/305`（业务 + 401 重发）、`:326`（计数点）。
**认证静默期内不发请求 → 不计数** ✓（`client.py:239-247` 在 `_raise_if_in_cooldown` 里直接抛，不经过 `_send`）。

**② 并发不串账、也不丢计数**（我**自己**写的，`/tmp/r2a-mut/tests/unit/test_r2a_a3.py`）：

```bash
$ conda run -n tpops python -m pytest tests/unit/test_r2a_a3.py -q
.....                                                                    [100%]
5 passed in 0.07s
```

- 8 线程 × 5 次请求 → **每个任务的增量都恰好是 5**、全局总数 **40**、传输层实收 **40** → 不串账、不丢账 ✓
  （全局计数用 `threading.Lock` 保护；线程计数放 `threading.local`，线程独占不需要锁 —— `call_tracking.py:57-72`。）
- 代理计数与 wire 计数并存时**不重复计**（`_CallMeter` 优先取 wire 级 `client.calls.thread_total`）✓

**③ 缓存命中时**不**计数**（没发 HTTP）—— 我自己验证：

```python
cached.get('/catalog', params={'name':'g1','instance_id':'i1'})  → raw.calls.total() == 1
cached.get('/catalog', params={'name':'g1','instance_id':'i1'})  → raw.calls.total() == 1   # 命中，未发请求
cached.get('/catalog', params={'name':'g2','instance_id':'i1'})  → raw.calls.total() == 2   # 不同分组码 → 不同键
cached.get('/catalog', params={'name':'g1','instance_id':'i2'})  → raw.calls.total() == 3   # 不同实例 → 不同键
```

**没有做反** ✓（键 = `(分组码, 实例 ID)`，实现见 `call_tracking.py:200-203`；失败**不入缓存**，我另测了 `test_cache_failure_is_not_cached` ✓）

**④ 失败任务的调用数也进轮汇总** ✓
`scheduler.py:179` 取全局计数增量 → `:335` `calls = max(reported, int(round_calls))`。
守卫 `tests/unit/test_scheduler.py::test_failed_tasks_calls_are_counted_in_the_round_summary`（1 次成功 + 2 次失败 → 必须等于 **3**）。
**变异验证**（`calls = max(...)` → `calls = reported`）→ 该守卫红（实施方已做，我复核了断言强度：`result.external_calls == 3`，而 `reported` 只有 1 → 差异可测 ✓）。

**⑤ 变异**（把 5 个通道的 `external_calls` 换回写死常数 = 修复前行为）：

```bash
$ conda run -n tpops python -m pytest tests/unit/test_executors.py tests/unit/test_scheduler.py tests/unit/test_scheduler_report.py -q
FAILED tests/unit/test_executors.py::test_monitor_external_calls_equal_real_request_count
FAILED tests/unit/test_executors.py::test_command_center_external_calls_equal_real_request_count
FAILED tests/unit/test_executors.py::test_instance_external_calls_equal_real_request_count
FAILED tests/unit/test_executors.py::test_alarm_external_calls_equal_real_request_count
FAILED tests/unit/test_executors.py::test_alarm_external_calls_scale_with_page_count
FAILED tests/unit/test_executors.py::test_metric_catalog_is_cached_across_rounds
6 failed, 39 passed in 0.93s
```

**6 条红 ✓**。**已还原**。

**⑥ 实测：平台留档回放，独立复算一轮的真实调用数**（`/tmp/r2a-probe/a3_replay.py`）：
把 `data/metric_response/`（逐指标原生响应）+ `tests/fixtures/merged_sql/sql_merge_probe_result.json`（合并形态留档）喂给**真的**执行器（`build_executors` + 真 `CommandCenterClient` 三跳），**回放传输层自己数**它真正服务的每一个请求，与上报值对拍：

```
monitor          上报 external_calls=13   该通道实际服务了 13 个请求
command_center   上报 external_calls=6    该通道实际服务了 6 个请求
diagnostic       上报 external_calls=2    该通道实际服务了 2 个请求
instance         上报 external_calls=1    该通道实际服务了 1 个请求
alarm            上报 external_calls=4    该通道实际服务了 4 个请求

第 1 轮：上报合计 = 26   传输层实数 = 26   一致？ YES

--- 第 2 轮（指标分组目录命中缓存）---
  monitor          上报=1    实际新增请求=1
  command_center   上报=5    实际新增请求=5
  diagnostic       上报=2    实际新增请求=2
  instance         上报=1    实际新增请求=1
  alarm            上报=4    实际新增请求=4
  第 2 轮上报合计=13   第 2 轮传输层实数=13   一致？ YES

按路径分类的请求数（独立复算的底账）：
  GET  命令中心明细            3      POST 命令中心下发            1
  GET  命令中心轮询            1      POST 慢SQL                  1
  GET  实例列表                1      POST TopSQL                 1
  GET  指标分组目录            12     POST 监控指标时序            1
  GET  节点清单                1      POST 告警记录                4
  合计                         26
```

- 上报值 = 传输层实数，**逐轮都对得上** ✓
- 目录缓存生效：monitor 第 1 轮 13 次、第 2 轮 **1 次** ✓（12 次目录 GET 只在首次/TTL 到期时发生）
- 修复前上报的是 `2+11+1+2+4 = 20`（写死），真实是 26 → **42 次预算告警从「永不响」变成「该响就响」** ✓

> **口径提醒（不是缺陷）**：我这个回放是**下界** —— 留档里命令中心只跑了 1 条合并 SQL。生产上命令中心要按页下发**多条**合并 SQL（11 条），每条 = 下发 + 轮询 + 每个 master 取明细 = **5 次请求** → 单轮真实值大概率 **> 42** → 预算 WARNING 会真的响。这正是这条守卫存在的意义，但 42 这个阈值本身需要控制器裁决（见 Minor 3）。

---

### A4 节点清单失败不再永久降级 resourceName

- **判定**：**已闭环**
- **代码位置**：
  - `tpops_kafka_collector/node_targets.py`（**新文件**，103 行）：`NODE_FETCH_POLICY`（`max_attempts=3`，退避 1 s→2 s + 抖动，走现成的 `retry.py:retry_call`，**没有另写一套**）、`build_instances()`（`:50-88`）、`fetch_nodes()`（`:91-103`）
  - `tpops_kafka_collector/main.py:58-62, 149`（装配时调用 `build_instances`）

**① 重试真的重试了 3 次**（不是记 3 条日志就放弃）：

```bash
$ conda run -n tpops python -m pytest tests/integration/test_main.py -q -k "node"
```
（修复后 3 passed）
守卫断言 `client.node_calls.count('inst-bad') >= 3` **且** 日志里出现「第 1/3 次重试」「第 2/3 次重试」——
后者只有 `retry.py:retry_call` 真的重新调用了函数才会打出来 ✓
**变异**（`NODE_FETCH_POLICY.max_attempts=3 → 1`）：

```
FAILED tests/integration/test_main.py::test_node_list_is_retried_three_times
1 failed, 2 passed, 21 deselected in 0.23s
```
**红 ✓**。**已还原**。

**② 该实例确实不进采集**：
守卫 `test_instance_without_a_node_list_is_not_collected` 断言 `runtime.instances == ['inst-ok']` **且** `runtime.scheduler.instances == ['inst-ok']`（调度器拿到的实例清单里也没有它）。
**变异**（失败时仍然 `targets.append(InstanceTarget(..., nodes=()))`）：

```
FAILED tests/integration/test_main.py::test_instance_without_a_node_list_is_not_collected
1 failed in 0.21s
```
**红 ✓**。**已还原**。

**③ 全部失败 → 启动失败、退出码非 0、未向 Kafka 发任何请求**：
守卫 `test_startup_fails_when_no_instance_has_a_node_list`：
- `entry.main(env=...)` 返回 `entry.EXIT_STARTUP_ERROR == 3`（≠ 0）✓
- `producers == []` —— **Kafka producer 根本没被构造**（`main.py:149` 的 `build_instances` 在 `main.py:150` 的 `MessageProducer.from_config` **之前**）✓
- 日志含「节点清单」✓
> 口径澄清见 Minor 3：TPOPS 侧**必然**有请求（判断节点清单失败本身就要先发请求），实际做到的是「未发任何 **Kafka** 请求、未发任何**采集**请求，TPOPS 侧只有实例列表 + 节点清单的 3 次尝试」。

**④ `exc_info=True` 真的带堆栈**：
守卫断言 `any(record.exc_info is not None for record in errors)`，且错误消息里含 `inst-bad` ✓
（代码：`node_targets.py:71-75` 用 `LOGGER.exception(...)`）。

**⑤ 认证失败不重试**：`retry_call` 的判定仍归 `errors.is_retryable` → `AuthError` 不可重试 → 平台**不会**被锁号 ✓（`node_targets.py:32-39` 的注释也写明了这一点）。

---

### A5 运行期每轮落盘

- **判定**：**已闭环**
- **代码位置**：
  - `tpops_kafka_collector/lifecycle.py:119`（`if _run_one_tick(...): _persist_watermark(runtime)`）、`:160-170`（`_persist_watermark` → `store.save()`，失败只记日志不打断主循环）
  - `tpops_kafka_collector/scheduler.py:305-309`（`_persist_baselines` → `watermark.import_baselines_from(delta_store)`，在 `tick()` 内、`_build_result` 之前）
  - `tpops_kafka_collector/watermark.py:235-239`（`save()` → `snapshot().to_payload()` → `_atomic_write_json`）

**① `save()` 真的把三样都写了**：
`WatermarkState.to_payload()`（`watermark.py:83-105`）写 **4 段**：`metric_watermarks` / `alarm_watermarks` / `delta_baselines` / `idempotency_keys`。
- 水位：`scheduler_delivery` 成功后才 `advance_metric` ✓
- 幂等键：`MessageDelivery` 成功后才 `remember_key` ✓
- 差分基线：`scheduler.tick()` 里 `_persist_baselines()` 把 `delta_store` **整体**搬进水位存储，**在** `run_loop` 的 `_persist_watermark` **之前** ✓

我自己起进程实测落盘文件内容（见 A2③）：`metric_watermarks` 有值、`idempotency_keys` 有值 ✓
（`delta_baselines` 在那个驱动里为空，是因为该驱动没有差分基线；`save()` 的 payload 结构里该段存在。）

**② SIGKILL 演练：已成功投递的不重投、未投递的不丢**：
`tests/integration/test_kill_restart_drill.py::test_sigkill_drill_does_not_duplicate_and_does_not_lose`（**真进程 + 真 SIGKILL**，三阶段）：
1. Kafka「不可用」→ 投递失败 → 连跑几轮 → **SIGKILL** → 断言**水位文件已落盘**（**不靠** `shutdown_runtime`）且能读回；
2. Kafka 恢复 → 重启 → 阶段一没投出的消息**这一轮投出来了**（不丢）；
3. 再重启一次 → 已成功投递过的**没有再投一遍**（不重复）。
全量里通过 ✓

**③ 每轮落盘的开销**（我自己测，`/tmp/r2a-probe/save_cost.py`；模拟真实规模：82 条 B 类差分基线 + 2000 个幂等键）：

```
水位文件大小 = 115055 字节
save() 耗时 ms: min=5.577 p50=7.345 p95=12.037 max=12.673
相对 30 秒一轮的占比 = 12.037 / 30000 = 0.04%
```

**p95 = 12 ms，占 30 秒一轮预算的 0.04%** → **可忽略，不会把 P95 预算吃穿** ✓

**④ 变异**（`run_loop` 里的 `_persist_watermark` 改成空操作 = 修复前行为）：

```bash
$ conda run -n tpops python -m pytest tests/unit/test_lifecycle.py \
    "tests/integration/test_kill_restart_drill.py::test_sigkill_drill_does_not_duplicate_and_does_not_lose" -q
E           AssertionError: 进程没有跑完三轮 —— 每轮落盘没生效：[]
FAILED tests/unit/test_lifecycle.py::test_watermark_is_written_to_disk_after_every_round
FAILED tests/unit/test_lifecycle.py::test_every_round_rewrites_the_watermark_file
FAILED tests/integration/test_kill_restart_drill.py::test_sigkill_drill_does_not_duplicate_and_does_not_lose
3 failed, 3 passed in 30.23s
```

**3 条红（含真 SIGKILL 演练）✓**。**已还原**。

---

## 二、修复轮新引入的东西

### 2.1 三个新生产文件

| 文件 | 行数 | 职责 | 依赖方向 |
|---|---|---|---|
| `tpops_kafka_collector/tpops/call_tracking.py` | 220 | 真实调用计数 / 按线程归属 / 目录 TTL 缓存 | 只依赖 stdlib（`threading` / `time` / `typing`） |
| `tpops_kafka_collector/scheduler_tasks.py` | 215 | 并发采集、在途等待与放弃 | 只依赖 `scheduler_types` |
| `tpops_kafka_collector/node_targets.py` | 103 | 采集目标装配 + 节点清单重试 | 只依赖 `retry` / `scheduler_types` / `tpops.instances` |

### 2.2 有没有循环依赖？

**没有。** 三条独立证据：

```bash
# ① 静态 import 图（自写 AST 脚本，处理相对 import 的层级）
$ conda run -n tpops python /tmp/circ.py tpops_kafka_collector
NO CYCLES DETECTED

# ② 运行时把包内每个模块都 import 一遍
$ conda run -n tpops python /tmp/imp.py
modules imported OK

# ③ 每个新模块单独起进程 import（隔离 import 期的环）
$ conda run -n tpops python -c "import tpops_kafka_collector.tpops.call_tracking; print('OK')"
OK tpops_kafka_collector.tpops.call_tracking
OK tpops_kafka_collector.scheduler_tasks
OK tpops_kafka_collector.node_targets
OK tpops_kafka_collector.main
OK tpops_kafka_collector.lifecycle
OK tpops_kafka_collector.scheduler
```

模块边界清楚：`main`（装配/入口） → `lifecycle`（怎么跑/怎么退） → `scheduler`（什么时候跑） → `scheduler_tasks`（怎么丢进线程池）；`node_targets`（目标怎么来）独立；`call_tracking`（可观测）挂在 `tpops` 层、无业务依赖。

### 2.3 硬约束扫描（全部自己跑）

| 约束 | 结果 |
|---|---|
| ruff | `conda run -n tpops python -m ruff check tpops_kafka_collector/` → **All checks passed!** |
| 单文件 < 400 行 | **无 ≥400 行的文件**（最大：`command_center.py` 399 / `producer.py` 399 / `client.py` 398 / `metrics.py` 396 / `scheduler.py` 392） |
| 函数 < 50 行 | AST 实测（全包）：**最长 49 行**（`collectors/monitor.py:166 _messages_of_rule`）→ **C1 已闭环**（`platform_profile.reconcile` 已拆成 `_compare_instance` / `_compare_engine` / `_compare_master_count` / `_scan_added`，现 30 行） |
| py3.7.9 语法红线 | `conda run -n tpops python -m compileall -q tpops_kafka_collector` → **compileall OK on py3.7.9**；`tests/test_py37_compat.py` → **156 passed** |
| 不新增依赖 | `git diff --name-only 3a71cb6..2305cea` 里**没有** `requirements.txt` |
| 不用 `print` | 全包 `print(` 数量 = **0** |
| 不可变数据 | 新文件全部用 frozen dataclass / NamedTuple / 普通类，无原地改 |
| 凭据不落日志 | 新文件无凭据处理；`exit_process` 只 flush 不打印 |
| 不改 `specs/**` / contract / tasks / `data/mapping/*.json` / `data/merged_sql/*` | `git diff --name-only 3a71cb6..2305cea` 逐一核对 → **NONE（good）** |

### 2.4 跨组干扰的断言：`dropped_unsupported=3` 还守得住它原本要守的东西吗？

**守得住，而且比原来更强。**

- 改动：`tests/unit/test_scheduler.py:802` 的 `assert '本轮丢弃 3 个不可提供指标' in caplog.text` → `assert 'dropped_unsupported=3' in caplog.text`。
- 原断言守的是**措辞**（那条措辞属于 R-B 的 `scheduler_report.py`）；新断言守的是**汇总行里字段的值**。
- **它没有变成恒绿**（变异验证）：把 `scheduler_report.log_round_summary` 里汇总行的 `dropped_unsupported` 改成 `+1`：

```
INFO ... tick=1 elapsed=0.0s fetched=0 mapped=1 produced=1 failed=0 dropped_unsupported=4 windows=0 alarms=0 calls=1 delivery_rate=1.000 groups=monitor skipped=-
FAILED tests/unit/test_scheduler.py::test_summary_log_uses_the_agreed_field_names
1 failed, 31 deselected in 0.15s
```

  → **立刻红 ✓**（映射表的不可提供条数一变，这条断言就会红）。
- 而且「措辞」本身由 **R-B 自己在自己文件里**的守卫守着：`tests/unit/test_scheduler_report.py::test_unsupported_metric_count_is_not_phrased_as_dropped_this_round`（断言日志里**不含**「本轮丢弃」、**含**「不可提供指标 3 条」与「累计 9」）。
- 结论：R-A 改的是 R-B 的文件里的措辞断言，但**没有削弱任何不变量**，只是把断言从措辞挪到字段值。**不构成跨组干扰的实质问题。**

### 2.5 `_install_counting_client` 写命令中心的私有属性 `command_center._client` —— 有没有副作用？

**没有破坏性副作用，但有两点需要记账。**

- **命令中心自己的客户端路径仍正常**：真 `CommandCenterClient` 的 `self._client` 只在 `_submit`（post）/ `_poll`（get）/ `_fetch_detail`（get）三处用；`CountingClient.get/post` 原样转发，`__getattr__` 转发其余属性。**实测**：回放里真 `CommandCenterClient` 走完三跳，5~6 次请求与上报**逐一对上** ✓
- **节点清单没有漏计**：`CommandCenterClient.__init__` 里 `self._nodes = MasterNodeRegistry(client, ...)` 仍持有**原**客户端（不是代理）→ 节点清单请求照常打到真客户端 → **wire 级计数照样加上** ✓（回放里 `GET 节点清单 1` 被计入了）
- **副作用 1（Minor 4）**：`command_center._client` 的**类型**从 `TpopsClient` 变成 `CountingClient`。任何依赖 `isinstance` 或直接访问底层成员的代码会受影响（当前仓库里没有）。代理靠 `__getattr__` 转发，`token()` / `auth_ok` 等行为一致。
- **副作用 2**：`build_executors(command_center=None)` 时 B 类执行器会以 `AttributeError` 崩（`self._cc` 是 `None`）。**这是修复前就有的**（`git show 3a71cb6:tpops_kafka_collector/executors.py` 同签名同行为），**不是本轮引入**；但本轮给 `command_center` 加了「为 None 时跳过 `_install_counting_client`」的分支，容易被误读成「支持 None」。

---

## 三、实测汇总（全部真实输出）

### 全量测试（工作树，只读）

```
$ cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
$ conda run -n tpops python -m pytest -q
1683 passed in 82.97s (1:22)

$ git status --porcelain
（空 —— 工作树零改动）
```

### 沙箱全量（`/tmp/r2a-mut`，含我自己的 8 条独立断言）

```
$ cd /tmp/r2a-mut && conda run -n tpops python -m pytest -q --cov=tpops_kafka_collector --cov-report=term-missing
tpops_kafka_collector/node_targets.py                            27      0   100%
tpops_kafka_collector/tpops/call_tracking.py                     93     10    89%   115-118, 130-131, 195-198
tpops_kafka_collector/scheduler_tasks.py                         81     10    88%   126, 150-151, 157, 161, 178-180, 198-203
tpops_kafka_collector/scheduler.py                              151     22    85%
tpops_kafka_collector/lifecycle.py                              ...     ...   85%
...
TOTAL                                                          4927    256    95%
1688 passed in 67.04s (1:07)
```

（`1688` = 基线 1685 + 我自己写的 3 条 A1 + 5 条 A3。基线在沙箱里因 `data/metric_value_rules.json` 被 `tests/test_generated_rules.py` 重新生成而比工作树多 1~2 个参数化用例 —— **拷贝产物，不是缺陷**。）

### ruff / py37 门禁

```
$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest -q tests/test_py37_compat.py
156 passed in 0.74s

$ conda run -n tpops python -c "import sys; print(sys.version)"
3.7.9 | packaged by conda-forge | (default, Feb 13 2021, 20:03:11) [GCC 9.3.0]
```

### 一轮真实外部调用数复算（A3⑥）

见 A3⑥ 的输出：**第 1 轮上报 26 = 传输层实数 26；第 2 轮上报 13 = 实数 13**。

### 变异验证总表

| 编号 | 变异内容 | 期望 | 实际 | 还原 |
|---|---|---|---|---|
| MA1 | `lifecycle._wait_for_next_tick`：`waiter(remaining)` → `waiter(interval)` | 红 | **我的 2 条 + 实施方 1 条红**（间隔 33~36 s vs 档位 30） | ✅ |
| MA2 | `main.exit_process`：`os._exit` → `sys.exit` | 红 | **2 条红**（在途那条 45.56 s 超时失败） | ✅ |
| MA3 | `executors`：5 个通道的 `external_calls` → 写死常数 | 红 | **6 条红** | ✅ |
| MA4 | `node_targets.NODE_FETCH_POLICY`：max_attempts 3 → 1 | 红 | **1 条红**（重试守卫） | ✅ |
| MA5 | `node_targets.build_instances`：失败时仍参与采集 | 红 | **1 条红**（不进采集守卫） | ✅ |
| MA6 | `lifecycle._persist_watermark`：改成空操作 | 红 | **3 条红**（含真 SIGKILL 演练） | ✅ |
| MA7 | `scheduler_report.log_round_summary`：汇总里的 `dropped_unsupported` +1 | 红 | **1 条红**（`dropped_unsupported=3` 守卫） | ✅ |

> 每次变异后都**还原并复跑**（`diff -r` 与工作树逐字节一致：`IDENTICAL`）。

---

## 四、发现

### Critical（0）

无。

### Important（0）

无。五条行为缺陷全部闭环，且每条都有「能让它失败的小改动」的守卫 —— 我逐条做了变异，守卫全部变红。

### Minor（4）

| # | 内容 | 位置 | 后果 | 修法 |
|---|---|---|---|---|
| 1 | `POLL_INTERVAL_SECONDS` 与最紧档位**隐式耦合**：默认 30 **恰好等于**最紧档位（monitor/alarm = 30 s）。配成 60 → 30 秒档**静默退化成 60 秒档**（DueTable 只在 tick 时判定到期），无日志、无告警、无守卫 | `tpops_kafka_collector/config.py:272`、`tpops_kafka_collector/schedule.py:217-235` | 客户声明的刷新档位被悄悄拉长 —— 与 A1 同类的「不报错但数据少」 | 启动时校验 `poll_interval_seconds ≤ min(档位)`，超了就 **ERROR 退出**（或至少 WARNING + 落进汇总日志）；补一条守卫 |
| 2 | `_CallMeter` 用**线程局部**计数。B 类当前走顺序的 `execute_sql`（请求发生在调度线程里）→ 正确；**一旦改用 `execute_batch`**（内层线程池并发下发），那些请求记在内层线程上 → 本通道的 `external_calls` 会**静默少算** → 42 次预算告警漏报 | `tpops_kafka_collector/executors.py:61-86`、`tpops_kafka_collector/tpops/command_center.py:213` | 未来的重构会静默劣化可观测性（正是 A3 的同类风险） | `_CallMeter` 改用**全局**增量（`client.calls.total()` 的前后差），或让 `CallCounter` 提供「任务级」计数（上下文/作用域）；补一条「`execute_batch` 场景下计数不丢」的守卫 |
| 3 | 「全部失败时启动失败 → **未向 TPOPS/Kafka 发任何请求**」这条口径按字面**做不到也不该做** —— 判断节点清单失败本身就得先发请求。实际做到的是：未构造 Kafka producer、未发任何**采集**请求，TPOPS 侧只有实例列表 + 节点清单的 3 次尝试 | `tpops_kafka_collector/node_targets.py:83-87` | 口径与实现不符 → 下一个复审方会照着字面去验，验不过 | 把口径改写成可测的形式：「未向 Kafka 发任何请求、未发起任何采集请求」；本轮**不阻塞**（实现侧已经是对的） |
| 4 | `build_executors(command_center=None)` 时 B 类执行器会以 `AttributeError` 崩（`self._cc` 为 `None`）。**修复前就有**，不是本轮引入；但本轮给 `command_center` 加了「为 None 时跳过 `_install_counting_client`」的分支，容易被误读成支持 None | `tpops_kafka_collector/executors.py:131-132`、`collectors/command_center.py:118` | 装配错误从「启动即失败」变成「运行期某轮崩」 | 要么在 `build_executors` 里对 `command_center is None` 直接抛 `SchedulerError`（快速失败），要么删掉 `None` 默认值 |

> 另需**控制器裁决**（不是本轮的缺陷，也不计进上面的计数）：真实计数落地后，生产单轮的命令中心通道会按页下发多条合并 SQL（每页 5 次请求）→ 单轮可能 **> 42** → 预算 WARNING 会真的响。这是规格要求的正确行为（`specs/delivery-and-state/spec.md:235-240`「超出时 SHALL 记录 WARNING」），但 **42 这个阈值是按旧口径估的**，与真实的三跳 × 页数不是一个口径。首轮审查已把它列为「口径待用户裁决」，本轮**没有**擅自改规格（正确）。
>
> ⚠️ **我与控制器账本的一处数字口径差异（需回看）**：我复审期间，控制器提交了 `807e632`（账本：「真实计数实测稳态 13 次/轮（冷启 26），远低于 42 → 规格不用改、不用问客户」）。
> 我的**独立复算与它完全一致**（冷启 26 / 稳态 13，同一组留档回放）。但这个 13 是**留档数据规模**下的数字，**会随生产的表数量线性放大** ——
> 合并 SQL 的页容量是 **100 行**（`sqlgen.load_spec().default_page_size = 100`），而两个 **paged 视图是「每张表一行」**（`stat_user_tables` / `dbstat_sum`，实测 `merged_views(mapping)` → `paged=True`）→ 表一多就要翻页，每页 = 下发 + 轮询 + 每个 master 取明细 = **5 次请求**：
>
> ```
> 页容量 = 100 行；paged 视图 = stat_user_tables / dbstat_sum（每张表一行）
> 表数量          命令中心页数     命令中心请求       单轮合计(稳态)
> 10           1          5            13
> 50           1          5            13
> 100          1          5            13
> 200          2          10           18
> 500          5          25           33
> 1000          10         50           58  <-- 超 42 预算
> 2000          20         100          108  <-- 超 42 预算
> ```
>
> 加上告警通道 4 种形态各自分页（`limit ≤ 100`，历史告警保留 30 天、条数会涨）→ 生产单轮**很可能**越过 42。
> **代码侧是对的**（报真值、超了就 WARNING，正是规格要的）；需要回看的是「远低于 42、规格不用改」这个**结论** —— 它建立在留档的小表规模上。建议控制器把口径改成「按页数/表数量给出稳态上限」，或把预算阈值按真实三跳口径重新定；**不要**为了让它闭嘴而把计数改回写死（那正是 A3 要修的缺陷）。

---

## 五、收尾

- 变异全部在 `/tmp/r2a-mut` 副本里做，**已逐个还原**：
  `diff -r /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev/tpops_kafka_collector /tmp/r2a-mut/tpops_kafka_collector --exclude=__pycache__` → **IDENTICAL**
- 工作树：`git status --porcelain` → **只有本报告一个未跟踪文件**（未 commit / 未 add / 未 checkout / 未 reset / 未移动 HEAD / 未切分支）
- 工作树内**只写了这一个文件**（本报告）
- `/tmp/r2a-mut` 沙箱已删除
- **说明**：我复审期间（11:28）控制器提交了 `807e632`（账本 `chore(sdd)`）—— **只动 `changes/**` 下的账本/进度文件，不含任何生产代码**，因此不影响本报告的取证范围（`3a71cb6..2305cea`）。本报告的 head 仍是 `2305cea`。
  该账本里「稳态 13 次/轮、远低于 42」的结论，我在 Minor 3 里给出了**限定条件**（留档表规模 vs 生产表规模）。

### 签发 receipt 的命令

```
ssf execution review changes/tpops-kafka-collector \
  --wave wave-3-scheduling-testing --base 3a71cb6 --head 2305cea \
  --report .superpowers/sdd/reviews/wave-3-scheduling-testing.md --verdict pass
```

> ⚠️ 上一轮 receipt 的 `--report` 指向的是首轮总报告 `wave-3-scheduling-testing.md`。
> 本轮**没有**新增 review 报告文件（按本次复审任务书，报告写在
> `wave-3-scheduling-testing-rereview-a.md`）。签发时 `--report` 请按控制器约定填写
> 本报告路径，或先确认 receipt 工具的 `--report` 语义（是否要求该路径存在）。
