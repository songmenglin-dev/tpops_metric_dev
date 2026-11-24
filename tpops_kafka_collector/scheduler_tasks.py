"""**并发采集**（调度器的线程池那一段）—— 任务展开、在途等待与放弃。

从 :mod:`tpops_kafka_collector.scheduler` 里拆出来 —— 调度器管「什么
时候跑哪些通道」，本模块管「怎么把它们丢进线程池、怎么等它们」。

两条不可让渡的口径：

1. **两维并发** —— 「通道 × 实例」的任务**一起**丢进线程池，墙钟是
   ``max(通道耗时, 实例耗时)`` 而不是两者相乘（合同完成标准：60 秒档
   单轮 P95 ≤ 25 秒 —— 实例变多时只有两维并发守得住）；
2. **挂死不拖住本轮** —— 停机后在途任务最多再等
   :data:`DEFAULT_SHUTDOWN_GRACE_SECONDS` 秒，超时就**放弃等待**
   （规格「放弃等待该请求 …… 不挂死」）；线程池**不等待回收**
   （``shutdown(wait=False)``），不让本轮卡在等 worker 上。
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from .scheduler_types import (
    InstanceTarget,
    SchedulerError,
    TaskFailure,
    TaskOutcome,
    reason,
)

LOGGER = logging.getLogger(__name__)

__all__ = [
    'DEFAULT_SHUTDOWN_GRACE_SECONDS',
    'SHUTDOWN_POLL_SECONDS',
    'Task',
    'TaskCollector',
]

#: 停机后在途任务的**放弃等待**上限（秒）。
#:
#: 比生命周期的放弃窗口（``lifecycle.SHUTDOWN_DEADLINE_SECONDS`` = 30 秒）
#: **早收手**：让 tick 线程能**干净地**返回（而不是被生命周期半路丢下），
#: 于是本轮已投递的部分能落盘，进程也能在 30 秒窗口内退出。
DEFAULT_SHUTDOWN_GRACE_SECONDS = 25

#: 等在途任务时的轮询切片（秒）—— 停机置位后立刻醒来。
SHUTDOWN_POLL_SECONDS = 0.5


@dataclass(frozen=True)
class Task:
    """一个待执行的 (通道, 实例) 任务（不可变）。"""

    channel: str
    instance: InstanceTarget

    @property
    def key(self) -> Tuple[str, str]:
        return (self.channel, self.instance.instance_id)


class TaskCollector:
    """把任务丢进线程池并发跑完；**单组失败不影响其它组**。

    :param executors: 通道名 → 执行器（签名 ``(InstanceTarget) -> TaskOutcome``）。
    :param instances: 待采集的实例清单。
    :param max_workers: 线程池上限。
    :param clock: 单调时钟（返回秒）。
    :param shutdown_event: 停机信号（置位后在途任务不再无限等）。
    :param shutdown_grace_seconds: 停机后在途任务的放弃等待上限（秒）。
    """

    def __init__(
        self,
        executors: Mapping[str, Callable[[InstanceTarget], TaskOutcome]],
        instances: Sequence[InstanceTarget],
        max_workers: int = 16,
        clock: Optional[Callable[[], float]] = None,
        shutdown_event: Any = None,
        shutdown_grace_seconds: float = DEFAULT_SHUTDOWN_GRACE_SECONDS,
    ) -> None:
        self._executors = dict(executors)
        self._instances = tuple(instances)
        self._max_workers = max(1, int(max_workers))
        self._clock = clock if clock is not None else time.monotonic
        self._shutdown_event = shutdown_event
        self._shutdown_grace = float(shutdown_grace_seconds)

    def collect(
        self, tasks: Sequence[Task]
    ) -> Tuple[Dict[Tuple[str, str], TaskOutcome], List[TaskFailure]]:
        """并发跑完本轮所有任务。

        :returns: ``(各组的结果, 失败清单)``。
        """
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
        pool = ThreadPoolExecutor(max_workers=workers)
        try:
            futures = [(pool.submit(self._run_task, task), task) for task in tasks]
            for future, task in futures:
                outcome, failure = self._await_task(future, task)
                if failure is not None:
                    failures.append(failure)
                else:
                    outcomes[task.key] = outcome
        finally:
            pool.shutdown(wait=False)
        return outcomes, failures

    def _run_task(self, task: Task) -> TaskOutcome:
        """跑一个 (通道, 实例) 任务。"""
        executor = self._executors.get(task.channel)
        if executor is None:
            raise SchedulerTaskError(f'通道 {task.channel} 没有执行器')
        outcome = executor(task.instance)
        if not isinstance(outcome, TaskOutcome):
            raise SchedulerTaskError(
                f'执行器没有返回 TaskOutcome：channel={task.channel} '
                f'instance={task.instance.instance_id}'
            )
        return outcome

    # -- 在途等待 ---------------------------------------------------------

    def _await_task(
        self, future: Any, task: Task
    ) -> Tuple[Optional[TaskOutcome], Optional[TaskFailure]]:
        """等一个任务结束。

        **停机后不再无限等** —— 收到停机信号后，在途任务最多再等
        ``shutdown_grace_seconds`` 秒，超时就**放弃等待**（该组数据留待下轮）。

        :returns: ``(结果, 失败)`` —— 两者恰有一个非 ``None``。
        """
        deadline = None  # type: Optional[float]
        while True:
            if deadline is None and self._is_shutting_down():
                deadline = self._clock() + self._shutdown_grace
                LOGGER.warning(
                    '收到停机信号 —— 在途任务最多再等 %s 秒，超时就放弃等待',
                    self._shutdown_grace,
                )
            timeout = self._wait_slice(deadline)
            if timeout is not None and timeout <= 0:
                return None, self._timeout_failure(task)
            try:
                return future.result(timeout=timeout), None
            except FutureTimeoutError:
                continue
            except Exception as exc:  # noqa: B902 —— 单组失败要隔离
                return None, self._task_failure(task, exc)

    def _is_shutting_down(self) -> bool:
        """是否已经收到停机信号。"""
        event = self._shutdown_event
        return bool(event is not None and event.is_set())

    def _wait_slice(self, deadline: Optional[float]) -> Optional[float]:
        """下一次等任务的时长（秒）；``None`` = 一直等。

        没有停机信号 → 一直等（正常采集不受影响）；
        收到停机信号 → 按剩余的放弃窗口切片等。
        """
        if self._shutdown_event is None:
            return None
        if deadline is None:
            return SHUTDOWN_POLL_SECONDS
        return deadline - self._clock()

    def _task_failure(self, task: Task, exc: BaseException) -> TaskFailure:
        """一个采集组失败 —— 记失败清单（其余组不受影响）。"""
        LOGGER.exception(
            '采集组失败（该组数据留待下轮，其余组不受影响）：'
            'channel=%s instance=%s',
            task.channel, task.instance.instance_id,
        )
        return TaskFailure(
            channel=task.channel,
            instance_id=task.instance.instance_id,
            error_type=type(exc).__name__,
            reason=reason(exc),
        )

    def _timeout_failure(self, task: Task) -> TaskFailure:
        """在途任务超时被放弃 —— 记一条失败（该组数据留待下轮重投）。"""
        LOGGER.error(
            '在途任务超过 %s 秒仍未结束 —— 放弃等待（该组数据留待下轮重投）：'
            'channel=%s instance=%s',
            self._shutdown_grace, task.channel, task.instance.instance_id,
        )
        return TaskFailure(
            channel=task.channel,
            instance_id=task.instance.instance_id,
            error_type='Timeout',
            reason=f'在途任务超过 {self._shutdown_grace} 秒未结束 —— 已放弃等待',
        )


class SchedulerTaskError(SchedulerError):
    """任务本身不合法（通道没有执行器 / 执行器没返回 ``TaskOutcome``）。

    继承 :class:`SchedulerError` —— 装配不对，跑起来只会一直错（不可重试）。
    """
