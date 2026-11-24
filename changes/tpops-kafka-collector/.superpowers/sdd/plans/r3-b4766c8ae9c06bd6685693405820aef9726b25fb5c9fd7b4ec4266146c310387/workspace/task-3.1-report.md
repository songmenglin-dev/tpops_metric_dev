# 任务 3.1 报告 —— 调度器（到期驱动 + 组 × 实例两维并发）

> 状态：**DONE_WITH_CONCERNS**
> 关注点只有一处：**C 类（诊断优化）通道的周期取 120 秒，不是开发方案 §4.6.2 与控制器对账表里的 300 秒** —— 理由与影响见 §6「偏离 D1」。其余全部按简报与控制器补充实现。

---

## 0. 交付清单

### 生产代码（6 个新文件，无既有文件被改动）

| 文件 | 行数 | 职责 |
|---|---|---|
| `tpops_kafka_collector/schedule.py` | 239 | **档位实算**（从映射表 `collect_cycle` 算通道周期）+ 到期表 `DueTable` |
| `tpops_kafka_collector/scheduler.py` | 393 | 调度引擎本体：到期判定 → 两维并发 → 汇总 |
| `tpops_kafka_collector/scheduler_types.py` | 210 | 不可变数据类型（`TaskOutcome` / `TickResult` / …）+ **键的约定** |
| `tpops_kafka_collector/scheduler_delivery.py` | 167 | 幂等过滤 → 投 Kafka → **投递成功后才推进水位** |
| `tpops_kafka_collector/scheduler_report.py` | 90 | 每轮汇总日志（§4.9.1 字段名）+ 进程自愈告警（§4.7） |
| `tpops_kafka_collector/scheduler_signals.py` | 39 | SIGTERM / SIGINT 的装与还原 |

> `scheduler.py` 初稿写到 772 行，越过合同「单文件 < 400 行」→ 拆成上面 6 个聚焦模块。
> 现存最大文件 393 行，**最长函数 < 40 行**（脚本扫描，见 §8）。

### 测试（2 个新文件，36 个用例）

| 文件 | 用例数 |
|---|---|
| `tests/unit/test_scheduler.py` | 30 |
| `tests/integration/test_scheduler.py` | 6 |

**未改动任何既有文件**（`tpops_kafka_collector/` 下既有模块、`changes/` 下既有文档、`data/mapping/*.json` 全部未动）。

---

## 1. RED → GREEN 证据

### RED（实现尚未落盘时跑测试）

把 6 个实现模块整体移出包（`mv … /tmp/sched_bak/`），再跑测试：

```
$ conda run -n tpops python -m pytest tests/unit/test_scheduler.py -q

==================================== ERRORS ====================================
________________ ERROR collecting tests/unit/test_scheduler.py _________________
ImportError while importing test module '/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev/tests/unit/test_scheduler.py'.
Hint: make sure your test modules/packages have valid Python imports.
Traceback:
tests/unit/test_scheduler.py:36: in <module>
    from tpops_kafka_collector.schedule import (
E   ModuleNotFoundError: No module named 'tpops_kafka_collector.schedule'
=========================== short test summary info ============================
ERROR tests/unit/test_scheduler.py
!!!!!!!!!!!!!!!!!!!! Interrupted: 1 error during collection !!!!!!!!!!!!!!!!!!!!
1 error in 0.72s
```

（第一轮 RED 还暴露了一个我自己的测试导入错误：`ProduceBatchError` 在
`kafka/records.py` 而不在 `errors.py` —— 已修正。）

### GREEN（实现落盘后）

```
$ conda run -n tpops python -m pytest tests/unit/test_scheduler.py -q
..............................                                           [100%]
30 passed in 1.30s

$ conda run -n tpops python -m pytest tests/integration/test_scheduler.py -q
......                                                                   [100%]
6 passed in 1.24s

$ conda run -n tpops python -m pytest tests/unit/test_scheduler.py tests/integration/test_scheduler.py -q
....................................                                     [100%]
36 passed in 1.67s
```

### 全量

```
$ conda run -n tpops python -m pytest -q
1479 passed in 31.47s

$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
132 passed in 1.06s
```

> 中途曾出现 3 个失败（`tests/test_merged_sql_mapping.py` 2 个 +
> `tests/unit/test_mapping_decode_gaps.py` 1 个）—— 那是**另一个 agent 正在改
> `mapping/` 层**的中间态（工作区里 `mapping/sqlgen.py` 被改动、有 4 个未跟踪的
> 新测试文件，都不是本任务产的）。等对方改完后全量 1479 passed。
> **本任务没有碰 `mapping/` 下的任何东西。**

---

## 2. 周期档位的实算表

实算来源：`data/mapping/metric_rules.json`（与包内副本 `tpops_kafka_collector/mapping/data/metric_rules.json` 同源）的
`collect_cycle` 字段。**规则：通道周期 = 该通道内「最高需求档」= 最短周期**
（开发方案 §4.6.3「一条合并 SQL 的刷新周期 = 它覆盖的指标中最高需求档对应的周期」）。

### 2.1 通道级（`schedule.compute_channel_plans` 的输出）

| 通道（`source.source`） | 指标条数 | `collect_cycle` 分布 | **组周期** |
|---|---|---|---|
| `monitor_metrics` | 11 | 30s×1、60s×3（60s×2 + 1分×1）、300s×7 | **30 秒** |
| `command_center_sql` | 82 | 60s×30（60s×1 + 1分×29）、120s×15、300s×35、86400s×2 | **60 秒** |
| `diagnostic` | 4 | 120s×1（2分）、300s×3（5分） | **120 秒** ⚠️ 见 §6 D1 |
| `instance` | 1 | 3600s×1 | **3600 秒** |
| （无 source） | 3 | — | 不可提供，丢弃（`dropped_unsupported=3`） |
| `alarm`（E 类） | — | 无客户指标、无 `collect_cycle` | **30 秒**（开发方案 §4.6.2） |

合计启用规则 **98** 条（101 − 3 不可提供），与 `metric_rules.json` 的
`counts.enabled = 98` 一致。

**与控制器对账表的一致性**：monitor 30 / command_center 60 / instance 3600 / alarm 30
**完全一致**；**只有 diagnostic 不同**（控制器表 300 秒，我按规则实算得 120 秒）。

### 2.2 命令中心按 `sql_key` 的刷新档位（取覆盖指标里的**最短**档）

| 刷新周期 | `sql_key` |
|---|---|
| 60 秒 | `activity_agg`、`dbstat_sum`、`locks`、`prepared_xacts`、`settings`、`stat_database` |
| 120 秒 | `activity_running`、`database`、`stat_user_tables` |
| 300 秒 | `activity_top10`、`bgwriter` |

**与控制器的独立实算逐行一致。**

### 2.3 开发方案 §4.6.3 的 SQL 档位表与映射表对不上（控制器已指出，此处留证）

§4.6.3 写的是：`stat_user_tables` 5 分档、`bgwriter` 1 分档、`database` 1 分档、
`activity_detail` 60 秒。实际映射表：`stat_user_tables` **2 分档**、`bgwriter` **5 分档**、
`database` **2 分档**、**没有 `activity_detail` 这个 `sql_key`**（是 `activity_top10`）。
以映射表为准（留给 wave-4 改文档）。

### 2.4 24 小时档（86400）的安放 —— **不发明口径**

- 2 条 24 小时档指标（`database` 与 `settings` 各 1 条）在**命令中心通道**里，
  该通道周期 60 秒 → 它们随 60 秒的合并 SQL 刷新一起被覆盖。
- 按 §4.6.3 的「新鲜度检查」口径核对：缓存结果的采集时刻（60 秒）**远小于**
  该档位周期（86400 秒）→ **不需要触发提前刷新** → 该档位自然落在 60 秒的刷新里。
- 因此**没有为 86400 单开一个调度条目**，也没有自造「每天一次」的任务 ——
  这正是开发方案 §2584 记录的那个「无处安放」的坑，本实现按「不发明口径」处理。

---

## 3. 并发耗时证据（max 而不是和）

测量脚本 `/tmp/measure_concurrency.py`（3 个通道 × 2 个实例，每个任务固定 sleep 0.5 秒）：

```
$ PYTHONPATH=<worktree> conda run -n tpops python /tmp/measure_concurrency.py
groups=3 instances=2 tasks=6
每组每实例耗时 = 0.50s
实测单 tick 墙钟 = 0.503s
串行下界 = 3.000s
两维并发下界(max) = 0.500s
倍速 = 6.0x
```

**结论**：6 个任务（3 组 × 2 实例）总耗时 0.503 秒 ≈ **max(0.5)**，
而不是 **和(3.0)** —— 快了 6 倍，正好等于任务数。

这同时证明**两个维度都是并发的**：

- 若只有「组并发、组内实例串行」→ 墙钟会是 1.0 秒（2 个实例串行）；
- 若全串行 → 3.0 秒。
- 实测 0.503 秒 → **两个维度都并行**。

### 3.1 确定性判据（不依赖计时）

`test_due_groups_run_concurrently_not_serially` 与
`test_two_dimensions_are_concurrent_groups_and_instances` 用
`threading.Barrier(3/6, timeout=5)`：只有真并发时所有任务才能同时到达屏障。
串行执行时后一个任务永远到不了屏障 → `BrokenBarrierError` → 测试失败。
**这条不依赖墙钟，没有 CI 抖动风险。**

### 3.2 墙钟计数的口径

`TickResult.duration_seconds` 来自**注入的 `clock`**（默认 `time.monotonic`）。
测试里注入了常量时钟时该值恒为 0 —— 这是刻意的（可测），生产用默认单调时钟。

---

## 4. 四条硬约束的守卫测试名

| # | 硬约束 | 守卫测试（全部可证伪） |
|---|---|---|
| 1 | **到期驱动** —— 未到期的任务不执行 | `tests/unit/test_scheduler.py::test_only_due_channels_are_executed` |
| 2 | **tick 内并发** —— 墙钟 ≈ max 而非之和 | `tests/unit/test_scheduler.py::test_due_groups_run_concurrently_not_serially`（Barrier，确定性）<br>`tests/unit/test_scheduler.py::test_two_dimensions_are_concurrent_groups_and_instances`（Barrier ×6）<br>`tests/unit/test_scheduler.py::test_tick_wall_clock_is_close_to_the_max_not_the_sum`（计时） |
| 3 | **单组失败不拖垮其他组** —— 其余组仍投递、进程不退出 | `tests/unit/test_scheduler.py::test_one_failing_group_does_not_stop_the_others`<br>`tests/unit/test_scheduler.py::test_a_failing_executor_does_not_kill_the_process`<br>`tests/unit/test_scheduler.py::test_consecutive_all_failed_rounds_only_log_an_error`<br>`tests/integration/test_scheduler.py::test_command_center_failure_does_not_stop_monitor`（真命令中心失败 + 真 A 类仍投递） |
| 4 | **水位只在投递成功后推进** —— 任一失败则整批不推进 | `tests/unit/test_scheduler.py::test_watermark_advances_only_after_successful_delivery`<br>`tests/unit/test_scheduler.py::test_failed_batch_is_retried_in_the_next_round`<br>`tests/unit/test_scheduler.py::test_metric_watermark_is_not_advanced_without_end_time`<br>`tests/integration/test_scheduler.py::test_watermark_does_not_advance_when_delivery_fails` |

### 4.1 「能让它失败的小改动」—— 四个变异都真的被抓到了

对生产代码各做一处**最小改动**，重跑守卫测试：

| 变异 | 改法 | 结果 |
|---|---|---|
| 1（破到期驱动） | `schedule.py`：`if entry.next_due_at <= now:` → `if True:` | `FAILED tests/unit/test_scheduler.py::test_only_due_channels_are_executed` —— **1 failed, 29 passed** |
| 2（破并发） | `scheduler.py`：线程池换成串行执行 | `FAILED …::test_due_groups_run_concurrently_not_serially`（`threading.BrokenBarrierError`）<br>`FAILED …::test_two_dimensions_are_concurrent_groups_and_instances` —— **2 failed, 28 passed** |
| 3（破失败隔离） | `scheduler.py`：采集组异常改为 `raise`（不再隔离） | `FAILED …::test_one_failing_group_does_not_stop_the_others`<br>`FAILED …::test_a_failing_executor_does_not_kill_the_process`<br>`FAILED …::test_consecutive_all_failed_rounds_only_log_an_error`<br>`FAILED …::test_executor_returning_garbage_is_a_failure_not_a_crash` —— **4 failed, 26 passed** |
| 4（破水位推进） | `scheduler_delivery.py`：把 `_advance_metric` 挪到 `produce_metrics` **之前** | `FAILED tests/unit/test_scheduler.py::test_watermark_advances_only_after_successful_delivery`<br>`FAILED tests/integration/test_scheduler.py::test_watermark_does_not_advance_when_delivery_fails` —— **2 failed, 34 passed** |

四个变异全部在测试结束后**已还原**（`cp` 回备份），最终态全绿。

---

## 5. 调度器的公开 API（给 3.3 装配 `main.py` 用）

> 这一节按接口文档写 —— 3.3 的实施方只会读本报告。

### 5.1 导入路径

```python
from tpops_kafka_collector.schedule import (
    compute_channel_plans, count_unsupported_metrics,
    ChannelPlan, ScheduleEntry,
    CHANNEL_MONITOR, CHANNEL_COMMAND_CENTER, CHANNEL_DIAGNOSTIC,
    CHANNEL_INSTANCE, CHANNEL_ALARM,
)
from tpops_kafka_collector.scheduler import Scheduler
from tpops_kafka_collector.scheduler_types import (
    InstanceTarget, TaskOutcome, metric_stream, alarm_stream,
)
```

### 5.2 `Scheduler.__init__`

```python
Scheduler(
    plans: Sequence[ChannelPlan],
    executors: Mapping[str, Callable[[InstanceTarget], TaskOutcome]],
    producer: Any,
    watermark: Any,
    instances: Sequence[InstanceTarget],
    delta_store: Any = None,
    dropped_unsupported: int = 0,
    clock: Optional[Callable[[], float]] = None,
    sleep: Optional[Callable[[float], None]] = None,
    max_workers: int = 16,
    tick_interval_seconds: float = 30.0,
)
```

| 参数 | 类型 | 说明 |
|---|---|---|
| `plans` | `Sequence[ChannelPlan]` | 各通道的调度计划。用 `compute_channel_plans(mapping)` 造。**每个计划里的通道都必须有执行器**，否则构造时抛 `SchedulerError`（快速失败）。 |
| `executors` | `Mapping[str, Callable[[InstanceTarget], TaskOutcome]]` | **通道名 → 执行器**。键必须与 `plans` 里的 `channel` 一致（`'monitor'` / `'command_center'` / `'diagnostic'` / `'instance'` / `'alarm'`）。 |
| `producer` | `Any` | 投递器。用 `MessageProducer.from_config(config)`。需要 `produce_metrics(records)` 与 `produce_alarms(records)` 两个方法。 |
| `watermark` | `Any` | `WatermarkStore`（`state_dir` 下的水位文件）。**由 3.3 负责 `load()` 与退出前 `save()`**。 |
| `instances` | `Sequence[InstanceTarget]` | 平台画像里的实例清单（3.3 从 `platform_profile.json` 读）。**至少一个**，否则构造时抛 `SchedulerError`。 |
| `delta_store` | `Any` | 差分基线存储（`mapping.rules.CounterDeltaStore`）。**必须与传给 `CommandCenterCollector(deltas=...)` 的是同一个对象** —— 给了才会做基线的恢复与持久化。 |
| `dropped_unsupported` | `int` | 不可提供指标条数。用 `count_unsupported_metrics(mapping)` 造（实测 3）。 |
| `clock` | `Optional[Callable[[], float]]` | 单调时钟（秒）。默认 `time.monotonic`。 |
| `sleep` | `Optional[Callable[[float], None]]` | 休眠函数（秒）。默认 `time.sleep`。只有 `shutdown_event` 没有 `wait()` 时才用得上。 |
| `max_workers` | `int` | 线程池上限（默认 16）。要「组 × 实例全并行」，这个值要 ≥ 本轮任务数（通道数 × 实例数）。 |
| `tick_interval_seconds` | `float` | tick 周期（秒）。默认 30 —— **建议传 `config.poll_interval_seconds`**。 |

### 5.3 对外方法

| 方法 | 签名 | 返回 | 说明 |
|---|---|---|---|
| `tick()` | `tick() -> TickResult` | `TickResult`（frozen dataclass） | 跑**一轮**：挑出到期通道 → 并发采集 → 投递 → 推进水位 → 汇总日志。**同步阻塞**，返回时本轮已全部完成。3.3 可直接调它做单轮演练。 |
| `run(shutdown_event)` | `run(shutdown_event: Any) -> None` | `None` | **主循环**（3.3 的首选入口）。每 tick 跑一轮，直到 `shutdown_event.is_set()` 为真。`shutdown_event` 需要 `is_set()`；有 `wait(timeout)` 更好（`threading.Event` 有 —— 置位后立刻醒来，不用等满一个 tick）。 |
| `run_forever()` | `run_forever() -> int` | 退出码（正常退出 **0**） | 自带 SIGTERM/SIGINT 处理的主循环（开发方案 §5.4.5）。**只在主线程里能装信号**（子线程里 `signal.signal` 抛 `ValueError` → 记 WARNING 后照常跑）。3.3 若自己处理信号，用 `run(shutdown_event)`，不要用这个。 |
| `schedule()` | `schedule() -> Tuple[ScheduleEntry, ...]` | 不可变元组 | 当前到期表快照（`task` / `interval_seconds` / `next_due_at`），供 `/readyz` 或日志用。 |
| `watermark` | property → `Any` | `WatermarkStore` | 水位存储。3.3 退出前用它 `save()` 刷盘。 |
| `plans` | property → `Tuple[ChannelPlan, ...]` | 不可变元组 | 各通道的调度计划。 |
| `instances` | property → `Tuple[InstanceTarget, ...]` | 不可变元组 | 采集目标实例清单。 |

### 5.4 `InstanceTarget`（frozen dataclass）

```python
InstanceTarget(
    instance_id: str,                 # 必填 —— 进水位键与幂等键
    instance_name: str = '',          # 进 tags.instance_name
    nodes: Tuple[Mapping[str, Any], ...] = (),   # key-view-execute-node 的返回形态
)
```

### 5.5 `TaskOutcome`（frozen dataclass）—— **执行器必须返回这个**

```python
TaskOutcome(
    messages: Tuple[Mapping[str, Any], ...] = (),   # 已组装好的指标消息（7 字段信封）
    alarms: Tuple[AlarmMessage, ...] = (),          # 告警消息（collectors.alarm.AlarmMessage）
    end_time_ms: Optional[int] = None,              # 本轮覆盖到的结束时刻（毫秒）
    alarm_cursor: Optional[str] = None,             # 告警水位游标
    external_calls: int = 1,                        # 本组本轮的外部调用次数
    fetched: int = 0,                               # 取回的原始行数（§4.9.1 的 fetched）
    windows: int = 0,                               # 执行的 SQL 条数（§4.9.1 的 windows）
)
```

**执行器契约（三条，3.3 必须遵守）**：

1. **执行器签名**是 `(instance: InstanceTarget) -> TaskOutcome`；
   返回别的东西 → 记成该组的失败（不崩）。
2. **`end_time_ms` 只能在取数成功时给**。给 `None` → 调度器**不推进**该组的指标水位
   （宁可重复投递，也不丢数据）。取数失败时**必须**给 `None`，
   否则水位会推进、丢掉那一段数据。
   - A 类：给查询窗口的 `end_time`（`tpops.metrics.resolve_window_from_config(now_ms, config).end_time`）；
   - B/C/D 类：给本轮的采集时刻 `now_ms`（**取数失败时给 `None`**）。
3. **告警水位游标**：不给 `alarm_cursor` 时，调度器从告警消息的
   `alarmTime` / `recoverTime` 里取最大值（毫秒字符串）。
   告警执行器若要从平台侧取准确游标，自己填。

### 5.6 `TickResult`（frozen dataclass）

```python
TickResult(
    tick_index: int = 0,
    executed: Tuple[str, ...] = (),     # 本轮执行的通道名
    skipped: Tuple[str, ...] = (),      # 本轮未到期的通道名
    mapped: int = 0,                    # 转换后待投递的消息总数（指标 + 告警）
    produced: int = 0,                  # 成功投递的消息数
    failed: int = 0,                    # 投递失败的消息数
    fetched: int = 0,
    windows: int = 0,
    alarms: int = 0,                    # 待投递的告警消息条数
    external_calls: int = 0,
    duration_seconds: float = 0.0,
    tasks_total: int = 0,               # 通道 × 实例 的任务数
    tasks_failed: int = 0,              # 采集阶段失败的任务数
    failures: Tuple[TaskFailure, ...] = (),
)
```

**自洽不变式**（规格要求，有守卫测试 `test_round_counters_are_self_consistent`）：
`produced + failed == mapped`，且 `produced + failed <= mapped`。

### 5.7 键的约定（**3.3 必须用这两个函数，不要自己拼字符串**）

```python
from tpops_kafka_collector.scheduler_types import metric_stream, alarm_stream

metric_stream(instance_id, channel)   # -> '83cd68f2…in14:command_center'
alarm_stream(instance_id)             # -> '83cd68f2…in14:alarm'
```

- 指标水位 `stream` = `f'{instance_id}:{channel}'`
- 告警水位 `stream` = `f'{instance_id}:alarm'`

**告警执行器要读水位**（`collect_alarms` 的 `watermark` 参数）时，
用 `store.alarm_watermark(alarm_stream(instance.instance_id))`。

### 5.8 装配顺序（3.3 的 `main.py` 用）

```
config = load_config()
mapping = load_mapping()
store  = WatermarkStore(path); store.load()
deltas = CounterDeltaStore()          # ← 与 CommandCenterCollector 共用同一个
producer = MessageProducer.from_config(config)
instances = [InstanceTarget(i.instance_id, i.instance_name, nodes) for i in profile.instances]

executors = {
    'monitor':        lambda inst: ...,
    'command_center': lambda inst: ...,
    'diagnostic':     lambda inst: ...,
    'instance':       lambda inst: ...,
    'alarm':          lambda inst: ...,
}

scheduler = Scheduler(
    plans=compute_channel_plans(mapping),
    executors=executors,
    producer=producer,
    watermark=store,
    instances=instances,
    delta_store=deltas,                       # ← 与 CommandCenterCollector 同一个对象
    dropped_unsupported=count_unsupported_metrics(mapping),
    tick_interval_seconds=config.poll_interval_seconds,
)

# 退出时
store.save()
producer.close()
```

**差分基线的关键**：`deltas` 必须是**同一个对象**同时交给
`CommandCenterCollector(deltas=deltas)` 与 `Scheduler(delta_store=deltas)`。
调度器在第一轮前把水位文件里的基线**整体**搬回 `deltas`（`export_baselines_into`），
每轮结束后再**整体**搬回水位存储（`import_baselines_from`），
**从不用 `put_baseline`**（它写的键没有作用域 → 采集器永远找不到 →
重启后速率永远算不出来且不报错）。

### 5.9 每轮汇总日志（字段名已定死，3.3 不要再自创）

```
tick=1234 elapsed=14.8s fetched=1043 mapped=1041 produced=1041 failed=0
dropped_unsupported=3 windows=9 alarms=2 calls=23 delivery_rate=1.000
groups=monitor,command_center,alarm skipped=diagnostic,instance
```

外加一条 INFO：「本轮丢弃 3 个不可提供指标（累计 3）」。
外部调用数超 42 → WARNING。

### 5.10 优雅退出（3.3 对接点）

调度器**不做**信号处理与刷盘 —— 那是 3.3 的活：

```python
shutdown = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: shutdown.set())
signal.signal(signal.SIGINT,  lambda *_: shutdown.set())

scheduler.run(shutdown)      # 置位后：不再发起新任务，等在途任务结束，返回

store.save()                 # 原子写水位
producer.close()             # flush
sys.exit(0)
```

`run()` 在 `shutdown_event` 置位后**立刻**醒来（`threading.Event.wait`），
不会白等一个 tick。**在途请求的 30 秒超时放弃**由 3.3 决定是否用
`run_forever()` / 自建超时实现 —— 本模块不介入（`run()` 只负责「不再发起新任务」）。

---

## 6. 取舍与偏离（逐条标出）

### D1 ⚠️ **C 类（诊断优化）通道周期取 120 秒，不是 300 秒**

- **按简报做的**：简报明写「档位取值规则 = 最高需求档 = **最短周期**」，
  且「周期必须表驱动……**以你实算的结果为准**（简报里那张表是参考，可能过期）」。
  按此规则实算：诊断通道含 1 条 **2 分档**指标（`gaussdb.Top10sql`）+
  3 条 5 分档 → 最短档 **120 秒**。
- **与什么冲突**：开发方案 §4.6.2 的任务表、§3.3 的档位表、控制器补充消息③的
  对账表都写 **300 秒**。开发方案 §4.6.2 那一行还注明「覆盖的指标档位 = 5 分钟档的 C 类指标」
  —— 即**2 分档的那条 C 类指标在 300 秒口径下没有任何任务覆盖它**。
- **为什么这么选**：300 秒会让 `gaussdb.Top10sql` 的实际刷新周期比客户声明的
  2 分钟多 180 秒（**欠服务**）；而 120 秒只会让 3 条 5 分档指标刷新得**更频繁**
  （**超服务**，不是缺陷）。且简报本身就把自己的表标成「可能过期」。
- **影响面**：C 类接口每轮 2 次 HTTP 调用（§4.8），120 秒一轮 → 单轮外部调用数
  仍在 42 的预算内（见 §7 的估算）。单轮墙钟不受影响（C 类只要 ~2 秒）。
- **怎么改**：只需改 `schedule.compute_channel_plans` 里诊断通道的取值口径
  （一处，一行）。若决定改成 300 秒，请同步在 §2.1 表里更新并告知 wave-4 改文档。

### D2 **幂等键在投递成功之后才登记**（不是「先 `remember_key` 再投递」）

- 简报说「用 `WatermarkStore.remember_key(key)`；返回 `False` 表示该键此前已投递过 → 跳过」。
- **按简报字面做会丢数据**：先 `remember_key` 再投递，投递失败时那批键已被记成
  「已投递」→ 下轮被当成重复跳过 → **那一批数据永远投不出去，且不报任何错**
  （与「静默出 0」同类的形态）。
- **本实现**：用 `has_key(key)` 做**只读**判重（已投过的跳过），投递**成功后**才
  `remember_key(key)` 登记。`remember_key` 仍然是幂等键的唯一登记口子。
- 守卫测试：`test_already_delivered_messages_are_skipped`、
  `test_failed_batch_is_retried_in_the_next_round`。

### D3 **投递批次粒度 = 一个 (通道, 实例) 任务**

- 简报说「批次原子推进 —— 任一消息投递失败则整批不推进」。
- **本实现把「批」定义为 (通道, 实例)**：一组失败只影响它自己那组的水位，
  其它组照常推进（与「单组失败不拖垮其他组」一致）。
- 若把「批」定义为整个 tick，命令中心一组失败会把 A 类的水位也卡住 —— 那是**更差**的语义
  （别的组的数据白白重发）。
- 批次内仍是原子的：该组的任一消息投递失败 → 该组整批不推进、幂等键不登记 → 下轮重发。

### D4 **差分基线的恢复方向**

- 简报说「差分基线恢复**只能用 `import_baselines_from`**，不能用 `put_baseline`」。
- **语义澄清**：`import_baselines_from(store)` 是「store → 水位」的方向；
  要把**持久化的基线送回采集端的 store**，方向是反的，只能用
  `export_baselines_into(store)`。
- **本实现两个都用，方向对**：
  - 启动后第一轮前：`watermark.export_baselines_into(delta_store)`（水位 → store，**键带作用域**）；
  - 每轮结束后：`watermark.import_baselines_from(delta_store)`（store → 水位，准备落盘）。
  - **`put_baseline` 一次都没用**（它写的键没有 `scope:` 前缀 → 采集器永远找不到）。
- 守卫测试：`test_delta_baselines_are_restored_with_their_scope`、
  `test_put_baseline_style_keys_are_not_used`。

### D5 **执行器没报 `end_time_ms` → 该组不推进指标水位**

`WatermarkStore.advance_metric(stream, end_time_ms)` 是**流级**的（不是规格里
「每个 `(metric_name, dimensions_key)`」的粒度）—— 这是既有 API 的形状，本任务不改。
为避免「水位冲过头 → 丢数据」，执行器不报结束时刻时**不推进**（并在日志里 WARNING）。
失败方向是安全的：宁可重启后重投（有幂等键兜底），不可丢数据。

### D6 **投递在主线程里按组串行，采集在 worker 线程里并发**

线程池只跑**采集**（HTTP I/O 密集）；投递与水位推进在主线程里**逐组**做。
- 好处：水位与幂等键集合**不需要加锁**（`WatermarkStore` 不是线程安全的）；
- 代价：单轮墙钟 = max(采集) + Σ(投递)。投递是 Kafka 批量发送 + flush，
  量级远小于采集（实测 A 类 1~3 秒、命令中心 8~15 秒，Kafka 毫秒级）。
- 合同完成标准「60 秒档单轮 P95 ≤ 25 秒」不受影响。

### D7 **告警通道的周期 30 秒来自 §4.6.2，不是映射表**

告警没有客户指标、没有 `collect_cycle` → 周期取开发方案 §4.6.2 的 30 秒，
在 `schedule.ALARM_INTERVAL_SECONDS` 里，**可配置**（`compute_channel_plans(mapping, alarm_interval_seconds=…)`）。

### D8 **指标幂等键的 `dimensions` = 客户维度 + 节点身份**

`ids.metric_idempotency_key(instance_id, metric_name, dimensions, timestamp)` 的
`dimensions` 若只填 `tags.dimension_values`，A 类多组件（同一指标多个 series）
会算出**同一个键** → 后面的组件被当成重复丢弃 → **丢数据**。
所以 `scheduler_types.metric_key()` 额外并入 `tags.node_id` 与 `tags.component_id`。
守卫测试：`test_metrics_of_different_components_have_different_keys`、
`tests/integration/test_scheduler.py::test_a_class_multi_component_messages_are_all_delivered`。

### D9 **开发方案 §4.6.3 的 SQL 档位表与映射表对不上**（控制器已指出）

见 §2.3。本实现一律以**映射表**为准。留给 wave-4 改文档。

### D10 **未新增依赖、未改任何既有文件**

并发用标准库 `concurrent.futures`；信号用标准库 `signal`。
`tpops_kafka_collector/` 下**既有文件一行未改**（`git status` 里该目录只有 6 个未跟踪的新文件）。

---

## 7. 可观测性与性能预算的自查

- **外部调用数**：每个任务通过 `TaskOutcome.external_calls` 自报（默认 1）。
  单轮超 **42** → WARNING（规格点名）。实测口径：A 类 2（目录 + 查询）、
  命令中心 3（三跳）、诊断 2、告警 5、实例 1、认证 1 → 1 实例约 14 次；
  2 实例约 27 次；3 实例约 40 次 → **3 实例以内不超预算**。
  3.3 若接了「计数型 TPOPS 客户端」，把真实次数填进 `external_calls` 即可。
- **投递成功率**：`delivery_rate = produced / mapped`（`mapped` 为 0 时记 1.0）。
- **单轮墙钟**：`TickResult.duration_seconds`（来自注入的时钟）。
- **进程自愈**：`CONSECUTIVE_FAILURE_ROUNDS = 3` —— 连续 3 轮采集全失败 →
  ERROR 日志「连续 N 轮采集全失败，采集链路可能已中断（进程不退出，交由 supervisor 重启）」，
  **进程不退出**（开发方案 §4.7）。

---

## 8. 收尾自查

| 项 | 结果 |
|---|---|
| `pytest -q` 全量 | **1479 passed**（基线 1295 + 本任务 36 + 其它 agent 的在制品） |
| `pytest tests/unit/test_scheduler.py tests/integration/test_scheduler.py -q` | **36 passed** |
| `ruff check tpops_kafka_collector/` | **All checks passed!** |
| `pytest tests/test_py37_compat.py -q` | **132 passed**（扫全仓 `.py`，含本任务 8 个新文件） |
| 新文件行数 | 239 / 393 / 210 / 167 / 90 / 39 —— **全部 < 400** |
| 最长函数 | **< 40 行**（脚本逐函数扫描，阈值 50） |
| 生产包 import `tools.*` / `data.*` | 无（`tests/unit/test_package_boundary.py` 绿） |
| 包内第二个 JSON 出口 | 无（`tests/unit/test_serializer.py` 的 AST 扫描绿） |
| 改动既有文件 | **无**（只新增 6 个生产文件 + 2 个测试文件） |
| `git commit` | **未提交**（按简报交给控制器） |

---

## 9. 给下一个任务（3.3 入口装配）的三条提醒

1. **`deltas` 对象要一物两用** —— 同时交给 `CommandCenterCollector(deltas=deltas)` 与
   `Scheduler(delta_store=deltas)`，否则重启后速率算不出来且不报错（§5.8 / §6 D4）。
2. **执行器的 `end_time_ms` 取数失败时必须给 `None`** —— 给错了水位会冲过头，丢数据（§5.5）。
3. **`max_workers` 要 ≥ 通道数 × 实例数** —— 否则两维并发退化成排队，
   60 秒档的单轮墙钟会逼近 25 秒的合同红线（§5.2）。
