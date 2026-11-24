"""主循环与**优雅退出**（任务 3.3）。

从 :mod:`tpops_kafka_collector.main` 里拆出来 —— 入口装配与「怎么跑、
怎么退」是两件事，拆开后两个文件都在合同「单文件 < 400 行」之内。

两条不可让渡的口径：

1. **不用 ``scheduler.run()`` / ``run_forever()``** ——
   它们内部 ``future.result()`` 没有超时，且线程池的上下文管理器会
   ``shutdown(wait=True)``：只要有一个在途 HTTP 调用挂住，进程就永远不返回，
   违反规格「放弃等待该请求 …… 不挂死」。
   本模块自己跑主循环，每轮放在**守护线程**里跑并带截止时间。
2. **停机后不再发起新的 HTTP 请求** —— 调度器只保证「不再发起新任务」，
   **不保证**「不再发起新的 HTTP 请求」（本轮已排进线程池但还没开始跑的
   任务仍会启动）→ 停机检查落在执行器那一层
   （见 :mod:`tpops_kafka_collector.executors`）。

优雅退出的顺序（规格「优雅退出」）::

    收到 SIGTERM / SIGINT
      → 置位 shutdown（执行器不再发起新的 HTTP 请求）
      → 停发新任务；在途请求最多等 30 秒（**超时就放弃，不挂死**）
      → 原子写水位
      → flush / 关闭 Kafka producer
      → 退出码 0

三条**运行时**口径：

1. **节奏对齐到 tick 边界** —— 一轮跑完后只睡「剩余时间」
   （``轮询间隔 - 本轮耗时``），实际采集周期 **= 轮询间隔**，
   不是「采集耗时 + 间隔」（后者会让 82/98 条命令中心通道的刷新周期
   比客户声明的档位长约 29%）。本轮超时（耗时 > 间隔）时**不堆积、
   不追赶**，下一轮从当前时刻重新锚定。
2. **每轮结束就落盘** —— 水位 / 幂等键 / 差分基线每轮写一次盘
   （崩溃最多丢一轮），而不是只在进程退出时写。
3. **退出码 0 且不挂死** —— 收尾（水位落盘 + producer flush）全部完成后
   由 :mod:`tpops_kafka_collector.main` 显式 ``os._exit(0)``，
   **跳过 atexit 钩子**（py3.7 的 ``concurrent.futures.thread`` 在 atexit
   里 ``join()`` 所有工作线程 —— 有一个在途 HTTP 挂住就会把进程拖死）。
"""
import logging
import threading
import time
from typing import Any, Callable, Optional

from .scheduler import Scheduler
from .scheduler_signals import install_signal_handlers, restore_signal_handlers
from .scheduler_types import TickResult

LOGGER = logging.getLogger(__name__)

__all__ = [
    'POLL_INTERVAL_SECONDS',
    'SCHEDULER_STALE_SECONDS',
    'SHUTDOWN_DEADLINE_SECONDS',
    'TrackedScheduler',
    'run',
    'run_loop',
    'shutdown_runtime',
]

#: 收到停机信号后，**最多**再等在途请求多少秒（超时就放弃等待）。
SHUTDOWN_DEADLINE_SECONDS = 30

#: 就绪探针的「最近一次成功采集」上限（秒）—— 超过就算不就绪。
SCHEDULER_STALE_SECONDS = 180

#: 主循环里等在途任务时的轮询间隔（秒）—— 置位后立刻醒来。
POLL_INTERVAL_SECONDS = 0.1

#: 正常退出码。
EXIT_OK = 0


class TrackedScheduler(Scheduler):
    """记「最近一次成功 tick 时刻」的调度器。

    ``/readyz`` 的 ``scheduler`` 探针要读这个时刻（**轻量**：读一个属性，
    不发网络请求）。规格：最近 3 分钟内至少成功完成 1 轮 → 就绪。
    """

    def __init__(self, *args: Any, wall_clock: Any = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._wall_clock = wall_clock if wall_clock is not None else time.time
        self._last_tick_ok_at = None  # type: Optional[float]

    def tick(self) -> TickResult:
        result = super().tick()
        if result.tasks_failed == 0:
            self._last_tick_ok_at = self._wall_clock()
        return result

    @property
    def last_tick_ok_at(self) -> Optional[float]:
        """最近一次**成功**完成的 tick 时刻（epoch 秒）；没有则 ``None``。"""
        return self._last_tick_ok_at


def run_loop(
    runtime: Any,
    shutdown: threading.Event,
    deadline_seconds: float = SHUTDOWN_DEADLINE_SECONDS,
    monotonic: Optional[Callable[[], float]] = None,
    wait: Optional[Callable[[float], Any]] = None,
) -> None:
    """主循环：每轮跑一次 tick；**对齐到 tick 边界**；收到停机信号后最多再等
    ``deadline_seconds``。

    :param monotonic: 单调时钟（秒）—— 测试可注入假时钟。
    :param wait: 等待函数（参数是秒，返回是否被置位）—— 默认用
        ``shutdown.wait``；测试可注入假等待（不真的睡）。
    """
    clock = monotonic if monotonic is not None else time.monotonic
    waiter = wait if wait is not None else shutdown.wait
    interval = _poll_interval_seconds(runtime)
    while not shutdown.is_set():
        started = clock()
        if _run_one_tick(runtime.scheduler, shutdown, deadline_seconds, clock):
            _persist_watermark(runtime)
        if shutdown.is_set():
            break
        _wait_for_next_tick(waiter, clock, started, interval)


def _poll_interval_seconds(runtime: Any) -> float:
    """轮询间隔（秒）—— 从配置里取，取不到按 0 处理（不睡）。"""
    raw = getattr(getattr(runtime, 'config', None), 'poll_interval_seconds', 0)
    try:
        return max(float(raw), 0.0)
    except (TypeError, ValueError):  # pragma: no cover —— 配置坏了不该崩主循环
        return 0.0


def _wait_for_next_tick(
    waiter: Callable[[float], Any],
    clock: Callable[[], float],
    started: float,
    interval: float,
) -> None:
    """睡到**下一个 tick 边界**（本轮耗时算在里面）。

    - 本轮耗时 < 间隔 → 只睡剩下的（实际周期 = 间隔，**不累积漂移**）；
    - 本轮耗时 > 间隔 → **不睡、不追赶**，下一轮从当前时刻重新锚定
      （否则会越欠越多，最后变成补偿风暴）。
    """
    if interval <= 0:
        return
    elapsed = clock() - started
    remaining = interval - elapsed
    if remaining <= 0:
        LOGGER.warning(
            '本轮耗时 %.3f 秒，超过轮询间隔 %.3f 秒 —— 重新锚定到当前时刻'
            '（不堆积、不累积漂移）',
            elapsed, interval,
        )
        return
    waiter(remaining)


def _persist_watermark(runtime: Any) -> None:
    """**每轮结束落盘**水位 / 幂等键 / 差分基线。

    崩溃（SIGKILL / OOM / 宿主机重启 / 容器驱逐）时最多丢一轮 ——
    只在退出时落盘的话，整个进程生命周期的去重状态会全丢，
    重启后那段数据被整段重投。
    """
    store = getattr(runtime, 'watermark', None)
    if store is None:
        return
    _safe(store.save, '本轮水位落盘失败 —— 崩溃时会丢掉这一轮的去重状态')


def _run_one_tick(
    scheduler: Any,
    shutdown: threading.Event,
    deadline_seconds: float,
    monotonic: Callable[[], float],
) -> bool:
    """跑一轮（**带截止时间**）；超时放弃等待 → 返回 ``False``。

    本轮放在**守护线程**里跑 —— 放弃等待后进程仍能退出（守护线程不拦解释器退出）。
    """
    worker = threading.Thread(
        target=_tick_safely, args=(scheduler,), name='scheduler-tick', daemon=True,
    )
    worker.start()
    if _wait_for_tick(worker, shutdown, deadline_seconds, monotonic):
        return True
    LOGGER.warning(
        '在途请求超过 %d 秒仍未结束 —— 放弃等待本轮结果'
        '（未投出的消息水位不推进，下轮重投）',
        deadline_seconds,
    )
    return False


def _tick_safely(scheduler: Any) -> None:
    """跑一轮并兜住异常（进程不退出，下轮重试）。"""
    try:
        scheduler.tick()
    except Exception:  # noqa: B902 —— 一轮失败不该带走进程
        LOGGER.exception('本轮调度异常 —— 进程继续运行，下轮重试')


def _wait_for_tick(
    worker: threading.Thread,
    shutdown: threading.Event,
    deadline_seconds: float,
    monotonic: Callable[[], float],
) -> bool:
    """等本轮结束；收到停机信号后最多再等 ``deadline_seconds`` 秒。

    :returns: 本轮是否正常结束（``False`` = 超时放弃等待）。
    """
    shutdown_at = None  # type: Optional[float]
    while worker.is_alive():
        if shutdown_at is None and shutdown.is_set():
            shutdown_at = monotonic()
        if shutdown_at is None:
            worker.join(timeout=POLL_INTERVAL_SECONDS)
            continue
        remaining = deadline_seconds - (monotonic() - shutdown_at)
        if remaining <= 0:
            return False
        worker.join(timeout=min(POLL_INTERVAL_SECONDS, remaining))
    return True


def shutdown_runtime(runtime: Any) -> None:
    """退出前的收尾：原子写水位 → flush / 关闭 producer → 停健康端点。

    ``producer.close()`` 会把剩余消息 flush 掉再断开
    （且 :class:`tpops_kafka_collector.kafka.producer.MessageProducer`
    本身是**逐批 flush** 的，收尾时没有积压）——
    所以这一步必须在 ``os._exit`` **之前**做完（``os._exit`` 不跑 atexit）。
    """
    _safe(runtime.watermark.save, '水位落盘失败 —— 退出前未能写完水位')
    _safe(runtime.producer.close, '关闭 Kafka 生产者失败')
    if runtime.health is not None:
        _safe(runtime.health.stop, '停止健康端点失败')


def _safe(action: Callable[[], Any], message: str) -> None:
    """执行一个收尾动作；失败只记日志（带堆栈），不影响退出码。"""
    try:
        action()
    except Exception:  # noqa: B902 —— 收尾失败不该改变退出码
        LOGGER.exception(message)


def run(
    runtime: Any,
    shutdown: Optional[threading.Event] = None,
    deadline_seconds: float = SHUTDOWN_DEADLINE_SECONDS,
    monotonic: Optional[Callable[[], float]] = None,
    wait: Optional[Callable[[float], Any]] = None,
) -> int:
    """跑主循环，收到停机信号后优雅退出；返回**退出码**（正常退出 0）。"""
    stop = shutdown if shutdown is not None else getattr(runtime, 'shutdown', None)
    if stop is None:  # pragma: no cover —— 兜底
        stop = threading.Event()
    previous = install_signal_handlers(stop)
    try:
        run_loop(
            runtime, stop, deadline_seconds=deadline_seconds,
            monotonic=monotonic, wait=wait,
        )
    finally:
        restore_signal_handlers(previous)
        shutdown_runtime(runtime)
    LOGGER.info('采集端已优雅退出（退出码 %d）', EXIT_OK)
    return EXIT_OK
