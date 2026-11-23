"""调度器 —— **到期驱动** + 组 × 实例**两维并发** + 水位推进。

对应规格 ``specs/delivery-and-state/spec.md`` 的「调度器的到期驱动与并发组」
「投递失败不推进水位」「单轮外部调用数与投递成功率可观测」，
开发方案 §4.6（调度模型 / 任务与周期 / 水位 / 优雅退出）与 §4.9（可观测性）。

三条不可让渡的口径：

1. **到期驱动** —— 每个通道有自己的下次到期时刻；**未到期的不执行**；
2. **两维并发** —— 「通道 × 实例」的任务**一起**丢进线程池，墙钟是
   ``max(通道耗时, 实例耗时)`` 而不是两者相乘（合同完成标准：60 秒档
   单轮 P95 ≤ 25 秒 —— 实例变多时只有两维并发守得住）；
3. **水位只在投递成功后推进** —— 批次原子推进：任一消息投递失败则整批
   不推进，下轮重发（客户侧按幂等键去重）。

**采集器不推进水位** —— 那是本模块的职责（守卫测试
``tests/integration/test_collectors_do_not_advance_watermarks.py``）。

模块拆分（同一条流水线上的三段）：

============================  =========================================
模块                            职责
============================  =========================================
``schedule.py``                 档位**实算** + 到期表
``scheduler_types.py``          不可变数据类型 + 键的约定
``scheduler_delivery.py``       幂等过滤 → 投递 → 推进水位
``scheduler.py``（本模块）      到期判定 → 两维并发 → 汇总
============================  =========================================
"""
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from .schedule import ChannelPlan, DueTable, ScheduleEntry
from .scheduler_delivery import MessageDelivery
from .scheduler_report import (
    CONSECUTIVE_FAILURE_ROUNDS,
    EXTERNAL_CALL_BUDGET,
    log_round_summary,
    note_round_health,
)
from .scheduler_signals import install_signal_handlers, restore_signal_handlers
from .scheduler_types import (
    DeliveryStats,
    InstanceTarget,
    SchedulerError,
    TaskFailure,
    TaskOutcome,
    TickResult,
    alarm_stream,
    metric_stream,
    reason,
)

LOGGER = logging.getLogger(__name__)

__all__ = [
    'CONSECUTIVE_FAILURE_ROUNDS',
    'DEFAULT_MAX_WORKERS',
    'EXTERNAL_CALL_BUDGET',
    'Scheduler',
    'SchedulerError',
    'alarm_stream',
    'metric_stream',
]

#: 线程池默认上限（「通道 × 实例」任务数的上限）。
DEFAULT_MAX_WORKERS = 16


@dataclass(frozen=True)
class _Task:
    """一个待执行的 (通道, 实例) 任务（不可变）。"""

    channel: str
    instance: InstanceTarget

    @property
    def key(self) -> Tuple[str, str]:
        return (self.channel, self.instance.instance_id)


class Scheduler:
    """到期驱动的调度器。

    :param plans: 各通道的调度计划（``schedule.compute_channel_plans`` 的返回值）。
    :param executors: 通道名 → 执行器。执行器签名
        ``(InstanceTarget) -> TaskOutcome``。**每个计划里的通道都必须有执行器**，
        否则装配错误（快速失败，不等到跑起来才发现）。
    :param producer: 投递器（``kafka.producer.MessageProducer``）。
    :param watermark: 水位存储（``watermark.WatermarkStore``）。
    :param instances: 待采集的实例清单（``InstanceTarget`` 元组）。
    :param delta_store: 差分基线存储（``mapping.rules.CounterDeltaStore``）——
        给了才会做基线的恢复与持久化（**只用整体搬运，不用 ``put_baseline``**）。
    :param dropped_unsupported: 不可提供指标条数（§4.9.1 的 ``dropped_unsupported``）。
    :param clock: 单调时钟（返回秒）。
    :param sleep: 休眠函数（秒）。
    :param max_workers: 线程池上限。
    :param tick_interval_seconds: tick 周期（秒）。
    """

    def __init__(
        self,
        plans: Sequence[ChannelPlan],
        executors: Mapping[str, Callable[[InstanceTarget], TaskOutcome]],
        producer: Any,
        watermark: Any,
        instances: Sequence[InstanceTarget],
        delta_store: Any = None,
        dropped_unsupported: int = 0,
        clock: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], None]] = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        tick_interval_seconds: float = 30.0,
    ) -> None:
        self._plans = tuple(plans)
        self._executors = dict(executors)
        self._watermark = watermark
        self._instances = tuple(instances)
        self._delta_store = delta_store
        self._dropped_unsupported = int(dropped_unsupported)
        self._clock = clock if clock is not None else time.monotonic
        self._sleep = sleep if sleep is not None else time.sleep
        self._max_workers = max(1, int(max_workers))
        self._tick_interval = float(tick_interval_seconds)
        self._tick_index = 0
        self._consecutive_failures = 0
        self._dropped_cumulative = 0
        self._baselines_restored = False
        self._delivery = MessageDelivery(producer, watermark)
        self._validate()
        self._due = DueTable(self._plans, self._clock())

    # -- 只读属性 ---------------------------------------------------------

    @property
    def watermark(self) -> Any:
        """水位存储（3.3 退出前用它 ``save()`` 刷盘）。"""
        return self._watermark

    @property
    def plans(self) -> Tuple[ChannelPlan, ...]:
        """各通道的调度计划。"""
        return self._plans

    @property
    def instances(self) -> Tuple[InstanceTarget, ...]:
        """待采集的实例清单。"""
        return self._instances

    def schedule(self) -> Tuple[ScheduleEntry, ...]:
        """当前的到期表（不可变快照）—— 供可观测与排障。"""
        return self._due.entries()

    # -- 主循环 -----------------------------------------------------------

    def tick(self) -> TickResult:
        """跑一轮：挑出到期通道 → 并发采集 → 投递 → 推进水位 → 汇总。"""
        started = self._clock()
        self._tick_index += 1
        self._restore_baselines()
        due, skipped = self._due.split_due(started)
        tasks = self._build_tasks(due)
        outcomes, collect_failures = self._collect_all(tasks)
        totals, delivery_failures = self._deliver_all(outcomes)
        self._persist_baselines()
        result = self._build_result(
            due, skipped, totals, collect_failures, delivery_failures,
            started, outcomes,
        )
        self._note_round_health(result)
        self._log_summary(result)
        return result

    def run(self, shutdown_event: Any) -> None:
        """主循环：每 tick 一轮，直到 ``shutdown_event`` 被置位。

        ``shutdown_event`` 需要有 ``is_set()``；有 ``wait(timeout)`` 更好
        （``threading.Event`` 有 —— 置位后立刻醒来，不用等满一个 tick）。
        """
        while not shutdown_event.is_set():
            self.tick()
            if shutdown_event.is_set():
                break
            self._wait_for_next_tick(shutdown_event)

    def run_forever(self) -> int:
        """自带信号处理的主循环（开发方案 §5.4.5）—— 正常退出返回 **0**。

        只在**主线程**里装信号处理（``signal.signal`` 在子线程里会抛
        ``ValueError``）—— 装不上就记 WARNING 并照常跑。
        """
        shutdown = threading.Event()
        previous = install_signal_handlers(shutdown)
        try:
            self.run(shutdown)
        finally:
            restore_signal_handlers(previous)
        return 0

    # -- 到期判定 ---------------------------------------------------------

    def _build_tasks(self, due: Sequence[str]) -> List[_Task]:
        """到期的通道 × 画像里的实例 —— **两维**展开成任务清单。"""
        tasks = []
        for channel in due:
            for instance in self._instances:
                tasks.append(_Task(channel=channel, instance=instance))
        return tasks

    # -- 并发采集 ---------------------------------------------------------

    def _collect_all(
        self, tasks: Sequence[_Task]
    ) -> Tuple[Dict[Tuple[str, str], TaskOutcome], List[TaskFailure]]:
        """并发跑完本轮所有任务；**单组失败不影响其它组**。"""
        outcomes = {}  # type: Dict[Tuple[str, str], TaskOutcome]
        failures = []  # type: List[TaskFailure]
        if not tasks:
            return outcomes, failures
        workers = min(len(tasks), self._max_workers)
        LOGGER.info(
            '本轮到期 %d 个通道 × %d 个实例 = %d 个任务，线程池并发 %d 个',
            len(set(task.channel for task in tasks)),
            len(self._instances),
            len(tasks),
            workers,
        )
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [(pool.submit(self._run_task, task), task) for task in tasks]
            for future, task in futures:
                try:
                    outcomes[task.key] = future.result()
                except Exception as exc:  # noqa: B902 —— 单组失败要隔离
                    failures.append(TaskFailure(
                        channel=task.channel,
                        instance_id=task.instance.instance_id,
                        error_type=type(exc).__name__,
                        reason=reason(exc),
                    ))
                    LOGGER.exception(
                        '采集组失败（该组数据留待下轮，其余组不受影响）：'
                        'channel=%s instance=%s',
                        task.channel, task.instance.instance_id,
                    )
        return outcomes, failures

    def _run_task(self, task: _Task) -> TaskOutcome:
        """跑一个 (通道, 实例) 任务。"""
        executor = self._executors.get(task.channel)
        if executor is None:
            raise SchedulerError(f'通道 {task.channel} 没有执行器')
        outcome = executor(task.instance)
        if not isinstance(outcome, TaskOutcome):
            raise SchedulerError(
                f'执行器没有返回 TaskOutcome：channel={task.channel} '
                f'instance={task.instance.instance_id}'
            )
        return outcome

    # -- 投递 -------------------------------------------------------------

    def _deliver_all(
        self, outcomes: Mapping[Tuple[str, str], TaskOutcome]
    ) -> Tuple[DeliveryStats, List[TaskFailure]]:
        """把本轮采集到的消息逐组投出去（**逐组隔离**：一组失败不影响别的）。"""
        planned = delivered = failed = alarms = 0
        failures = []  # type: List[TaskFailure]
        for key in sorted(outcomes):
            channel, instance_id = key
            stats, group_failures = self._delivery.deliver(
                channel, instance_id, outcomes[key],
            )
            planned += stats.planned
            delivered += stats.delivered
            failed += stats.failed
            alarms += stats.alarms
            failures.extend(group_failures)
        return (
            DeliveryStats(
                planned=planned, delivered=delivered,
                failed=failed, alarms=alarms,
            ),
            failures,
        )

    # -- 差分基线 ---------------------------------------------------------

    def _restore_baselines(self) -> None:
        """把持久化的差分基线**整体**搬回采集端的基线存储。

        **只能用整体搬运**（``export_baselines_into`` / ``import_baselines_from``）——
        ``put_baseline`` 写的键没有作用域（``scope:key``），采集器按带作用域的
        键去找，**永远找不到** → 重启后速率永远算不出来，而且不报任何错。
        """
        if self._delta_store is None or self._baselines_restored:
            return
        self._baselines_restored = True
        baselines = self._watermark.snapshot().delta_baselines
        if not baselines:
            LOGGER.info('水位文件里没有差分基线 —— 首轮只建基线，不产出速率')
            return
        self._watermark.export_baselines_into(self._delta_store)
        LOGGER.info(
            '差分基线已从水位文件恢复到采集端：%d 条（含作用域键）', len(baselines),
        )

    def _persist_baselines(self) -> None:
        """把本轮更新后的差分基线**整体**搬回水位存储（准备落盘）。"""
        if self._delta_store is None:
            return
        self._watermark.import_baselines_from(self._delta_store)

    # -- 汇总与可观测 -----------------------------------------------------

    def _build_result(
        self,
        due: Sequence[str],
        skipped: Sequence[str],
        totals: DeliveryStats,
        collect_failures: Sequence[TaskFailure],
        delivery_failures: Sequence[TaskFailure],
        started: float,
        outcomes: Mapping[Tuple[str, str], TaskOutcome],
    ) -> TickResult:
        duration = max(self._clock() - started, 0.0)
        fetched = 0
        windows = 0
        calls = 0
        for outcome in outcomes.values():
            fetched += max(int(outcome.fetched), 0)
            windows += max(int(outcome.windows), 0)
            calls += max(int(outcome.external_calls), 0)
        return TickResult(
            tick_index=self._tick_index,
            executed=tuple(due),
            skipped=tuple(skipped),
            mapped=totals.planned,
            produced=totals.delivered,
            failed=totals.failed,
            fetched=fetched,
            windows=windows,
            alarms=totals.alarms,
            external_calls=calls,
            duration_seconds=duration,
            tasks_total=len(due) * len(self._instances),
            tasks_failed=len(collect_failures),
            failures=tuple(collect_failures) + tuple(delivery_failures),
        )

    def _note_round_health(self, result: TickResult) -> None:
        """进程自愈：连续 N 轮全失败 → ERROR 日警（**进程不退出**）。"""
        self._consecutive_failures = note_round_health(
            result, self._consecutive_failures,
        )

    def _log_summary(self, result: TickResult) -> None:
        """每轮汇总日志 —— 字段名与开发方案 §4.9.1 逐字一致。"""
        self._dropped_cumulative = log_round_summary(
            result, self._dropped_unsupported, self._dropped_cumulative,
        )

    # -- 内部 -------------------------------------------------------------

    def _validate(self) -> None:
        """装配校验 —— 通道缺执行器直接失败（快速失败，不留到运行期）。"""
        missing = sorted(
            plan.channel for plan in self._plans
            if plan.channel not in self._executors
        )
        if missing:
            raise SchedulerError(
                f'这些通道没有执行器：{", ".join(missing)}'
            )
        for plan in self._plans:
            if plan.interval_seconds <= 0:
                raise SchedulerError(
                    f'通道 {plan.channel} 的周期必须为正整数，'
                    f'实际 {plan.interval_seconds!r}'
                )
        if not self._instances:
            raise SchedulerError('没有待采集的实例 —— 调度器无事可做')

    def _wait_for_next_tick(self, shutdown_event: Any) -> None:
        """等到下一个 tick 边界（置位后立刻醒来，不等满一个周期）。"""
        waiter = getattr(shutdown_event, 'wait', None)
        if callable(waiter):
            waiter(self._tick_interval)
            return
        self._sleep(self._tick_interval)
