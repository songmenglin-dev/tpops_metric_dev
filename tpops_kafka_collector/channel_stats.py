"""**每通道每轮**的统计聚合（wave-2 任务 2.5）。

为什么单独成文件：``scheduler.py`` 已经贴着「单文件 < 400 行」的红线，
而「按通道归集计数」是**纯聚合逻辑** —— 与调度无关，放这里才好测。

两条口径（与规格一致）：

1. **各通道之和 == 整体行的同名字段** —— ``fetched`` / ``mapped`` /
   ``produced`` / ``failed`` / ``windows`` 都能按通道加起来复原；
2. **没跑完的通道不出汇总行** —— 本轮一个实例都没跑成的通道不出行
   （它的失败由整体行体现），未到期的通道当然也不出。

``calls`` 是唯一的例外：整体行的 ``calls`` 取「任务自报调用数」与
「客户端真实调用数」的**较大者**（失败任务的调用也算进去），因此
整体行可能**大于**各通道之和 —— 差值就是失败任务消耗掉的调用。
"""
from typing import Any, Callable, Dict, Mapping, Tuple

from .scheduler_types import ChannelRoundStats, DeliveryStats, TaskOutcome

__all__ = ['TaskTimer', 'aggregate_channel_stats']

#: 计时记录：``(通道, 实例 ID) -> (开始时刻, 结束时刻)``。
Span = Tuple[float, float]


class TaskTimer:
    """记录本轮各 ``(通道, 实例)`` 任务的起止时刻。

    通道的墙钟耗时 = 该通道**最早开始**到**最晚结束**的那一段
    （各通道的任务是并发跑的，所以不能简单相加）。
    """

    def __init__(self, clock: Callable[[], float]) -> None:
        self._clock = clock
        self._spans = {}  # type: Dict[Tuple[str, str], Span]

    def reset(self) -> None:
        """清空上一轮的计时（每轮开始时调）。"""
        self._spans = {}

    def wrap(
        self, channel: str, executor: Callable[[Any], TaskOutcome]
    ) -> Callable[[Any], TaskOutcome]:
        """给执行器套一层计时，返回包装后的执行器。"""

        def _timed(instance: Any) -> TaskOutcome:
            started = self._clock()
            try:
                return executor(instance)
            finally:
                ended = self._clock()
                key = (channel, getattr(instance, 'instance_id', ''))
                self._spans[key] = (started, ended)

        return _timed

    def spans(self) -> Dict[Tuple[str, str], Span]:
        """本轮各任务的起止时刻（快照）。"""
        return dict(self._spans)


def _channel_elapsed(spans: Mapping[Tuple[str, str], Span], channel: str) -> float:
    """该通道本轮的墙钟耗时（秒）。"""
    starts = []
    ends = []
    for (name, _), (started, ended) in spans.items():
        if name != channel:
            continue
        starts.append(started)
        ends.append(ended)
    if not starts:
        return 0.0
    return max(max(ends) - min(starts), 0.0)


def aggregate_channel_stats(
    tick: int,
    outcomes: Mapping[Tuple[str, str], TaskOutcome],
    deliveries: Mapping[str, DeliveryStats],
    spans: Mapping[Tuple[str, str], Span],
    instance_counts: Mapping[str, int],
) -> Tuple[ChannelRoundStats, ...]:
    """按通道归集本轮统计。

    :param tick: 轮次序号。
    :param outcomes: ``(通道, 实例 ID) -> TaskOutcome``（**只有跑成功的任务**）。
    :param deliveries: ``通道 -> DeliveryStats``（按通道归集的投递统计）。
    :param spans: ``(通道, 实例 ID) -> (开始, 结束)``（计时器给出的起止时刻）。
    :param instance_counts: ``通道 -> 该通道本轮参与的实例数``。
    :returns: 每个「跑成了」的通道一行统计（按通道名排序）。
    """
    channels = sorted({
        channel for channel, _ in outcomes
    })
    rows = []
    for channel in channels:
        fetched = windows = calls = 0
        for (name, _), outcome in outcomes.items():
            if name != channel:
                continue
            fetched += max(int(outcome.fetched), 0)
            windows += max(int(outcome.windows), 0)
            calls += max(int(outcome.external_calls), 0)
        stats = deliveries.get(channel) or DeliveryStats()
        mapped = int(stats.planned)
        produced = int(stats.delivered)
        rows.append(ChannelRoundStats(
            channel=channel,
            tick=int(tick),
            instances=int(instance_counts.get(channel, 0)),
            fetched=fetched,
            mapped=mapped,
            produced=produced,
            failed=int(stats.failed),
            windows=windows,
            calls=calls,
            elapsed=_channel_elapsed(spans, channel),
            delivery_rate=(produced / mapped) if mapped else 1.0,
        ))
    return tuple(rows)
