# 修复轮 R-A 报告 —— 运行时（节奏 / 退出 / 节点清单 / 落盘 / 保命分支）

> 变更：`tpops-kafka-collector`；波次：修复轮 R-A（wave-3 审查报告的 Important A1/A2/A4/A5 + B3）。
> **实施方不 commit** —— 由控制器统一提交。
> 工作树：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`，分支 `tpops_metric_dev`。

---

## 逐条修复

### A1 采集节奏比客户声明的档位长约 29%（82/98 指标受影响）

- **改了什么**
  - `tpops_kafka_collector/lifecycle.py:99-123`（`run_loop`）：一轮跑完后**只睡剩余时间**
    （`轮询间隔 - 本轮耗时`），不再睡满整间隔；本轮耗时 > 间隔时**不睡、不追赶**，
    下一轮从当前时刻**重新锚定**（不堆积、不累积漂移）。
    新增可注入的 `wait` 参数（测试用假等待，不真睡）。
  - `tpops_kafka_collector/lifecycle.py:125-131`（`_poll_interval_seconds`）、
    `:134-157`（`_wait_for_next_tick`）：节奏对齐的两个私有函数。
  - `tpops_kafka_collector/lifecycle.py:251-273`（`run`）：把 `wait` 透传给 `run_loop`。
- **改动前**（`lifecycle.py:93-97`，原样）：

  ```python
  while not shutdown.is_set():
      _run_one_tick(runtime.scheduler, shutdown, deadline_seconds, clock)
      if shutdown.is_set():
          break
      shutdown.wait(runtime.config.poll_interval_seconds)   # ← 睡满整间隔
  ```

- **改动后**（`lifecycle.py:99-123`）：

  ```python
  while not shutdown.is_set():
      started = clock()
      if _run_one_tick(runtime.scheduler, shutdown, deadline_seconds, clock):
          _persist_watermark(runtime)
      if shutdown.is_set():
          break
      _wait_for_next_tick(waiter, clock, started, interval)
  ```

  ```python
  elapsed = clock() - started
  remaining = interval - elapsed
  if remaining <= 0:
      LOGGER.warning('本轮耗时 %.3f 秒，超过轮询间隔 %.3f 秒 —— 重新锚定到当前时刻…')
      return
  waiter(remaining)
  ```

  **到期表（`DueTable`）不用改**：tick 落在边界上时，`split_due(now)` 里的
  `next_due_at = now + 间隔` 恰好把通道周期钉回档位（30 秒档每 30 秒到期一次）。
- **守卫测试**：`tests/unit/test_lifecycle.py::test_channel_interval_is_aligned_to_the_tick_boundary`
  （真 `Scheduler` + 真 `DueTable` + 假平台 + 假时钟；断言「相邻两次实际采集间隔 ≤ 档位 × 1.1」）
  与 `::test_a_slow_round_does_not_accumulate_drift`（超时轮不累积漂移）。
- **修复后输出（绿）**：

  ```
  $ conda run -n tpops python -m pytest -q tests/unit/test_lifecycle.py
  .....                                                                    [100%]
  5 passed in 0.66s
  ```

- **变异验证（改回坏的样子 → 红）**：把 `_wait_for_next_tick` 的
  `remaining = interval - elapsed` 改成 `remaining = interval`（= 修复前「睡满整间隔」的行为）：

  ```
  $ conda run -n tpops python -m pytest -q tests/unit/test_lifecycle.py
  E           AssertionError: 通道 monitor 的实际采集间隔 38.0 秒超过档位 30 秒 —— 节奏没对齐到 tick 边界：[38.0, 38.0, 38.0]
  E           assert 38.0 <= (30 * 1.1)
  tests/unit/test_lifecycle.py:140: AssertionError
  ...
  2 failed, 3 passed in 0.88s
  ```

  38.0 秒与审查报告里的实测 38.7 秒一致 —— 守得住。
  还原后：`5 passed in 0.67s`。

---

### A2 「在途请求超过 30 秒 → 放弃等待 → 退出码 0、不挂死」在真实路径上不成立

- **改了什么**
  1. **退出路径跳过 atexit**：`tpops_kafka_collector/main.py:314-339` 新增 `exit_process(code)`
     —— 收尾（水位落盘 + producer flush）完成后 `os._exit(code)`，**跳过 atexit 钩子**
     （py3.7 的 `concurrent.futures.thread._python_exit` 会 `join()` 所有工作线程）。
     `main.py:336-337` 的 `__main__` 块改为 `exit_process(main())`。
     退出码仍是 0；`producer.close()` 之前已 flush（`MessageProducer._produce` 逐批 flush，
     `close()` 再 flush 一遍）—— 见 `lifecycle.py:229-243` 的 docstring。
  2. **tick 线程不再被卡在等 worker**：`tpops_kafka_collector/scheduler_tasks.py`（**新建**）
     承载并发采集与在途等待：
     - `collect()` 用**显式** `pool = ThreadPoolExecutor(...)` + `finally: pool.shutdown(wait=False)`
       （不再用 `with` 的 `shutdown(wait=True)`）；
     - `_await_task()` 在**收到停机信号后**最多再等 `shutdown_grace_seconds`（默认 25 秒，
       比生命周期的 30 秒放弃窗口早收手），超时就**放弃等待**并记一条 `Timeout` 失败
       （该组数据留待下轮重投）。
     `scheduler.py:228-239` 的 `_collect_all` 变成一行委托。
  3. `scheduler.py` 新增 `shutdown_event` / `shutdown_grace_seconds` 两个构造参数，
     `main.py:184-207`（`_build_scheduler`）把 `stop` 传进去（与生产一致）。
- **守卫测试**：`tests/integration/test_main.py::test_in_flight_request_beyond_deadline_gives_up_and_exits_zero`
  —— 换成**真的 `Scheduler` + 真的 `ThreadPoolExecutor`**：驱动脚本（写在 `tmp_path` 里，
  真进程）里某个执行器 `time.sleep(3600)` 挂死；测试 SIGTERM 后断言
  ① 进程**在放弃窗口内**退出（`communicate(timeout=45)` 超时即失败）、
  ② **退出码 = 0**、③ **水位文件完整可读回**。
  另有 `::test_exit_process_skips_the_atexit_hooks`（断言走的是 `os._exit`）。
- **修复后输出（绿）**：

  ```
  $ conda run -n tpops python -m pytest -q tests/integration/test_main.py
  ........................                                                 [100%]
  24 passed in 31.59s
  ```

  单条 A2 用例：`1 passed in 26.58s`（进程约 26 秒退出 —— 25 秒的放弃窗口 + 收尾）。
- **变异验证（改回坏的样子 → 红）**：把 `exit_process` 的 `os._exit(int(code))`
  改成 `sys.exit(int(code))`（= 修复前：不跳过 atexit）：

  ```
  E           AssertionError: 进程在放弃窗口内没有退出 —— 挂死了（atexit 的 join 把进程拖住了）
  tests/integration/test_main.py:423: AssertionError
  1 failed in 46.41s
  ```

  还原后：`1 passed in 26.58s`。

---

### A4 节点清单只在启动时取一次；取失败就永久降级 `resourceName`，零测试、无堆栈

- **改了什么**
  - **新建** `tpops_kafka_collector/node_targets.py`（把「采集目标怎么来」从 `main.py` 里拆出来）：
    - `NODE_FETCH_POLICY`（`max_attempts=3`，退避 1s → 2s + 抖动）——
      **用现有 `tpops_kafka_collector/retry.py` 的 `retry_call`**，没有另写一套；
      判定仍归 `errors.is_retryable`（**认证失败绝不重试**，平台会锁号）；
    - `build_instances()`（`:50-88`）：逐实例取节点清单，
      **3 次仍失败 → 该实例不参与采集**（不进 `targets`），
      记 **ERROR + `exc_info=True`（带堆栈）** + 指明 `instance_id`；
      **所有实例都取不到 → 抛 `NodeListUnavailable` → 启动失败**（非 0 退出码 + 明确日志）；
    - `fetch_nodes()`（`:91-103`）：退避重试的薄封装。
  - `main.py`：`build_runtime` 用 `build_instances(...)`（`:146`），
    `main()`（`:292-311`）的 `except Exception → EXIT_STARTUP_ERROR`（= 3）覆盖「启动失败」；
    `main.py:172-182` 新增 `_build_clients`（把 `build_runtime` 压回 < 50 行）。
  - **不引入每轮重取节点清单** —— 仍然只在启动时取一次。
- **守卫测试**（`tests/integration/test_main.py`）：
  - `::test_instance_without_a_node_list_is_not_collected` —— 一个实例取不到 →
    **该实例不进 `runtime.instances` / `runtime.scheduler.instances`**，
    且日志是 ERROR + **带堆栈** + 指明实例；
  - `::test_node_list_is_retried_three_times` —— **退避重试确实发生了两次**
    （日志里有「第 1/3 次重试」「第 2/3 次重试」）；
  - `::test_startup_fails_when_no_instance_has_a_node_list` —— 全部取不到 →
    **退出码 = `EXIT_STARTUP_ERROR`（3）** + **Kafka 生产者压根没被构造**（未向 Kafka 发请求）。
- **修复后输出（绿）**：

  ```
  $ conda run -n tpops python -m pytest -q tests/integration/test_main.py
  ........................                                                 [100%]
  24 passed in 31.59s
  ```

- **变异验证（三条，各自改回坏的样子 → 红）**：

  | 变异 | 输出 |
  |---|---|
  | `if profile.instances and not targets:` → `if False:`（不启动失败） | `E AssertionError: 启动失败前不该构造 Kafka 生产者` / `1 failed` |
  | 取不到节点清单时仍然 `targets.append(...)`（仍参与采集） | `E AssertionError: 取不到节点清单的实例不该参与采集：['inst-ok', 'inst-bad']` / `1 failed` |
  | `NODE_FETCH_POLICY` 的 `max_attempts=3` → `1`（不重试） | `E AssertionError: ['inst-ok', 'inst-bad', 'inst-ok', 'inst-bad']` / `1 failed` |

  三条还原后全绿。

---

### A5 运行期从不落盘水位 / 幂等键

- **改了什么**
  - `tpops_kafka_collector/lifecycle.py:160-171`（`_persist_watermark`）：
    **每轮 tick 正常结束后**调用一次 `watermark.save()`（原子写已实现好，成本只是一次小 JSON 落盘）。
    放在 `lifecycle` 层（`run_loop` 里）而不是 `Scheduler.tick()` 里 —— 保持 scheduler 纯粹。
  - `lifecycle.py:99-123`：`run_loop` 在 `_run_one_tick` 返回 True（本轮跑完）后落盘；
    **本轮被放弃（超时）时不落盘**（状态可能是半截的）。
  - `watermark.save()` 的覆盖面已确认：`WatermarkStore.snapshot()` 同时含
    **指标水位 / 告警水位 / 差分基线 / 幂等键**（`watermark.py:204-220`），
    `to_payload()` 四段全写（`watermark.py:83-105`）—— **水位、幂等键与差分基线都落盘**，
    不需要额外处理。差分基线在 tick 末尾由 `scheduler._persist_baselines()`
    （`import_baselines_from`，整体搬运）先搬进水位存储，再由本轮落盘写出去。
- **守卫测试**
  - `tests/unit/test_lifecycle.py::test_watermark_is_written_to_disk_after_every_round`
    —— **一轮 tick 后磁盘上已有水位文件**（且能读回）；
  - `tests/unit/test_lifecycle.py::test_every_round_rewrites_the_watermark_file`
    —— 连跑三轮 → `save()` 被调用 **3 次**；
  - `tests/integration/test_kill_restart_drill.py::test_sigkill_drill_does_not_duplicate_and_does_not_lose`
    —— **SIGKILL 版**杀进程演练（三阶段，见下）。
- **修复后输出（绿）**：

  ```
  $ conda run -n tpops python -m pytest -q tests/integration/test_kill_restart_drill.py
  .....                                                                    [100%]
  5 passed in 18.23s
  ```

  SIGKILL 演练的三阶段：
  1. **Kafka「不可用」** → 投递失败 → 连跑几轮 → **SIGKILL** →
     断言**水位文件已经落盘**（**不靠** `shutdown_runtime`）且完整可读回；
  2. **Kafka 恢复 → 重启** → 断言阶段一没投出去的消息**这一轮投出来了**（不丢未投递的数据）；
  3. **再重启一次** → 断言已成功投递过的消息**没有再投一遍**（不重复）。
- **变异验证（改回坏的样子 → 红）**：把 `run_loop` 里的 `_persist_watermark(runtime)` 去掉：

  ```
  $ conda run -n tpops python -m pytest -q tests/integration/test_kill_restart_drill.py
  E           AssertionError: 进程没有跑完三轮 —— 每轮落盘没生效：[]
  tests/integration/test_kill_restart_drill.py:443: AssertionError
  1 failed, 4 passed in 42.64s
  ```

  还原后：`5 passed in 18.34s`。

---

### B3 三个「保命」的异常分支没有测试

三条守卫逐条列出（全部**新增**）：

1. **一轮采集抛异常 → 进程继续跑，下轮重试**（`lifecycle.py:197-203` `_tick_safely`）
   - 守卫：`tests/unit/test_lifecycle.py::test_a_failing_round_does_not_stop_the_loop`
     —— 第一轮抛异常 → 主循环继续，第二、三轮照跑；日志里有「本轮调度异常」。
   - 修复后：`5 passed in 0.66s`。
   - 变异（把 `_tick_safely` 的 `try/except` 去掉）：
     `E AssertionError:` / `RuntimeError: 这轮炸了` / `1 failed, 4 passed in 0.82s` → **守得住**。
2. **装配失败 → 退出码 3 + 堆栈**（`main.py:304-308`）
   - 守卫：`tests/integration/test_main.py::test_startup_error_returns_exit_code_three_with_a_stack_trace`
     —— 断言 `code == EXIT_STARTUP_ERROR == 3`、日志含「装配失败」、且该 ERROR 记录
     **`exc_info is not None`**（带堆栈）。
   - 变异（`return EXIT_STARTUP_ERROR` → `return EXIT_OK`）：
     `tests/integration/test_main.py:765: AssertionError` / `1 failed in 0.83s` → **守得住**。
3. **`/readyz` 的「认证失败 / 未认证 / 跑了但全挂」分支**（`main.py:263-284`）
   - 守卫：`::test_tpops_probe_reports_an_authentication_failure`、
     `::test_tpops_probe_reports_not_authenticated_yet`、
     `::test_scheduler_probe_reports_a_stale_last_success`、
     `::test_readiness_returns_503_when_the_last_round_failed`
     （最后一条是「**跑了但全挂**」→ `/readyz` 返回 **503**，
     补上了原先只测「还没跑过一轮」的缺口）。
   - 变异（`_tpops_state` 不再报认证失败）：
     `E AssertionError: TPOPS 尚未完成认证` / `1 failed in 0.83s` → **守得住**。

---

### 追加：R-B 转交的「失败任务的外部调用数不进轮汇总」

> 控制器 2026-11-24 转交：失败任务的请求**也真实发出去了**，
> 但 `scheduler._build_result` 只累加**成功**任务的 `external_calls`
> → 轮汇总偏小 → 42 次预算告警**漏报**。

- **改了什么**
  - `tpops_kafka_collector/scheduler.py:170-186`（`tick`）：采集前后取**全局调用计数**的基线与增量；
  - `scheduler.py:242-256`（`_call_mark` / `_call_delta`）；
  - `scheduler.py:313-345`（`_build_result`）：`calls = max(各组自报之和, 本轮真实增量)`
    —— **失败任务的调用数也在里面**（请求确实发出去了）；
  - `scheduler.py` 新增构造参数 `call_counter`（= `client.calls`），
    `main.py:184-207`（`_build_scheduler`）把 `getattr(tpops, 'calls', None)` 传进去。
- **守卫测试**：`tests/unit/test_scheduler.py::test_failed_tasks_calls_are_counted_in_the_round_summary`
  —— 一个成功任务（1 次调用）+ 一个失败任务（2 次调用）→ 轮汇总的 `external_calls` 必须是 **3**。
  另有 `::test_round_call_count_is_not_smaller_than_the_reported_one`（没有计数器时的下限）。
- **修复后输出（绿）**：`32 passed in 1.20s`。
- **变异验证**：`calls = max(reported, int(round_calls))` → `calls = reported`（修复前的老行为）：

  ```
  E           AssertionError: 失败任务的 2 次调用没进轮汇总 —— 汇总偏小 → 预算告警漏报：1
  1 failed
  ```

---

## 实测结果

> 全部在 `/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`、
> conda 环境 `tpops`（python 3.7.9）下跑的**真实输出**。

### 全量测试

```
$ conda run -n tpops python -m pytest -q
1683 passed in 81.00s (0:01:21)
```

（基线 1560 → **1683**：R-A 新增 12 例 + R-B/R-C 并行新增。**无回归**。）

### ruff

```
$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!
```

### py37 兼容门禁

```
$ conda run -n tpops python -m pytest -q tests/test_py37_compat.py
154 passed in 1.24s
```

（基线 147 → 154；R-A 未新增语法，红线未被破坏。）

### 覆盖率

```
$ conda run -n tpops python -m pytest -q --cov=tpops_kafka_collector --cov-report=term-missing | tail -30
tpops_kafka_collector/lifecycle.py                               99     15    85%   88-91, 147, 189-194, 218, 222-225, 240, 247-248
tpops_kafka_collector/main.py                                   124      9    93%   225, 232, 270, 275, 285, 311, 331-332, 337
tpops_kafka_collector/node_targets.py                            27      0   100%
tpops_kafka_collector/scheduler.py                              151     22    85%   152, 157, 196-200, 208-214, 255, 298-299, 379, 388-392
tpops_kafka_collector/scheduler_tasks.py                         81     10    88%   126, 150-151, 157, 161, 178-180, 198-203
...
TOTAL                                                          4927    256    95%

1683 passed in 87.36s (0:01:27)
```

### 硬约束自查

```
$ find tpops_kafka_collector -name "*.py" -exec wc -l {} + | sort -rn | head -5
   399 tpops_kafka_collector/tpops/command_center.py
   399 tpops_kafka_collector/kafka/producer.py
   398 tpops_kafka_collector/tpops/client.py
   396 tpops_kafka_collector/tpops/metrics.py
   393 tpops_kafka_collector/health.py

$ python3 - <<'EOF'   # AST 实测函数跨度
import ast, pathlib
for p in pathlib.Path('tpops_kafka_collector').rglob('*.py'):
    t = ast.parse(p.read_text(encoding='utf-8'))
    for n in ast.walk(t):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            span = (n.end_lineno or n.lineno) - n.lineno + 1
            if span > 50: print('OVER-50', p, n.name, n.lineno, span)
EOF
over-50 count: 0

$ grep -rn "print(" tpops_kafka_collector/ | wc -l
0
```

**无 ≥400 行的文件、无 >50 行的函数、无 `print`。**
（`command_center.py` / `producer.py` / `client.py` / `metrics.py` 是 R-B 的文件，本组未改。）

---

## 发现但未修的问题

1. **R-B 改了 `scheduler_report.py` 的汇总措辞**（「本轮丢弃 N 个不可提供指标」→
   「不可提供指标 N 条（本轮不产出，累计 N）」），**打挂了本组文件
   `tests/unit/test_scheduler.py` 里的同名断言**。已把该断言改成**与措辞无关**的
   `assert 'dropped_unsupported=3' in caplog.text`（措辞归 `scheduler_report.py`，R-B 的文件）。
   —— 这是跨组干扰，不是缺陷。
2. **B 类快照指标的幂等键含采集时刻**（`clock` 每轮都变）→ 客户侧也无法去重。
   本组的 A5 修复只保证**采集端自身**的幂等键不丢；「键跨进程稳定」需要改
   `ids.py` / 消息组装（`mapping/message.py`），**超出本组文件归属**，未改。
   已在 A5 的 SIGKILL 演练里用**固定 `clock`** 的假消息把这个语义钉死，
   真实 B 类指标的键稳定性仍需 R-B/控制器裁决。
3. **`/readyz` 的 503 分支只覆盖了两个探针**（`tpops` / `scheduler`）。
   Kafka 本身的连通性没有探针（规格未要求），未加。
4. **`main.py` 的 `EXIT_STARTUP_ERROR`（3）与 `NodeListUnavailable` 都是「平台不可达」的信号**，
   但前者也覆盖「映射表加载失败」等其他装配错误 —— 日志里能区分，退出码不区分。未改。

---

## 偏离说明

1. **`os._exit` 放在 `main.py` 的 `exit_process()` + `__main__` 块，而不是 `main.run()` 内部。**
   - 原因：`run()` / `main()` 都被**进程内**的调用方直接调用（`tests/integration/test_main.py`
     的 `entry.run(runtime, ...)`、`entry.main(env=env)`）。把 `os._exit` 放进 `run()`/`main()`
     会**当场杀掉 pytest 进程**（退出码 0 → 整个测试会话被判成功退出、后续用例全不跑）。
   - 落地：`python -m tpops_kafka_collector.main` → `exit_process(main())` →
     `main()` → `run(runtime)`（内部完成 `shutdown_runtime`：水位落盘 + producer flush）
     → 返回 0 → `os._exit(0)`。**生产路径的退出码仍是 0，且跳过 atexit。**
2. **新建了两个生产文件**（超出「只改清单内文件」的口径，理由同 R-B 的
   `tpops/call_tracking.py`）：
   - `tpops_kafka_collector/scheduler_tasks.py`（215 行）——
     `scheduler.py` 加了「在途等待与放弃」后是 **483 行（> 400）**，
     不拆文件就过不了合同硬约束。拆出去的是「并发采集 + 在途等待」这一段，
     调度器（到期判定 / 投递 / 汇总）留在 `scheduler.py`。
   - `tpops_kafka_collector/node_targets.py`（103 行）——
     `main.py` 加了「节点清单重试与失败处置」后是 **401 行（> 400）**，
     `build_runtime` 也涨到 **52 行（> 50）**。拆出去的是「采集目标怎么来」这一段，
     入口（`main` / `exit_process` / 就绪探针）留在 `main.py`。
   - 两个文件都在 `tpops_kafka_collector/` 包内、**没有引入任何新依赖**、
     没有碰 R-B / R-C 的文件。
3. **停机后的在途放弃窗口取 25 秒**（`scheduler_tasks.DEFAULT_SHUTDOWN_GRACE_SECONDS`），
   比生命周期的 30 秒放弃窗口早 5 秒 —— 让 tick 线程能**干净地**返回，
   本轮已投递的部分能落盘，进程也能在 30 秒窗口内退出。
   规格「最多等 30 秒」是**上限**，等 25 秒合规。
4. **A2 的守卫测试用真进程 + 真 30 秒窗口**（不注入假超时），
   代价是该用例约 **26 秒**。这是按控制器要求（「不要靠测试里的假超时蒙混」）刻意保留的。
5. **A5 的 SIGKILL 演练用「写在 tmp_path 里的驱动脚本」**而不是
   `python -m tpops_kafka_collector.main` + `tests/fixtures/graceful_shutdown_stubs/`：
   - 桩（`sitecustomize.py`）**不在本组文件归属内**，改它会踩到别的组；
   - 演练需要**能挂死的执行器**与**跨进程可观测的投递记录**，
     桩里的假 Kafka 生产者把消息记在**进程内存**里，跨进程看不见。
   - 驱动脚本走的是**同一套真代码**：`entry.run(runtime)`（真生命周期 + 真 `Scheduler`
     + 真 `ThreadPoolExecutor` + 真 `WatermarkStore` 原子写）+ `entry.exit_process()`。
     平台与 Kafka 是假的（不连任何外部服务）。
6. **`run_loop` 新增可注入的 `wait` 参数**（默认仍是 `shutdown.wait`）——
   为了让节奏守卫能用**假时钟 + 假等待**跑（真等 30 秒 × 几轮会让单元测试没法用）。
   生产行为不变。
7. **A1 的守卫用真 `Scheduler` + 真 `DueTable`**（只有平台是假的）——
   「到期才跑」的语义正是把节奏拉长的那一环，用假调度器测不到。
