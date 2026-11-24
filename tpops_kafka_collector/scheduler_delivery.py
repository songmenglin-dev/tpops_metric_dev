"""投递 —— 幂等过滤 → 投 Kafka → **投递成功后才推进水位**。

拆出来是因为 ``scheduler.py`` 越过合同「单文件 < 400 行」，
而投递逻辑与调度逻辑是两件事（调度决定「谁跑」，投递决定「怎么落」）。

三条不可让渡的口径：

1. **批次原子推进** —— 任一消息投递失败则整批不推进，下轮重发
   （客户侧按幂等键去重）；
2. **幂等键在投递成功之后才登记** —— 先登记后投递的话，投递失败的那批键
   已经被记成「已投递」→ 下轮被当成重复跳过 → **静默丢数据**；
3. **投递失败要隔离** —— 某组投递失败不影响其它组（进程不退出）。
"""
import logging
from typing import Any, List, Optional, Tuple

from .kafka.records import alarm_record, metric_record
from .schedule import CHANNEL_ALARM
from .scheduler_types import (
    DeliveryStats,
    TaskFailure,
    TaskOutcome,
    alarm_cursor,
    alarm_stream,
    metric_key,
    metric_stream,
    reason,
)

LOGGER = logging.getLogger(__name__)

__all__ = ['MessageDelivery']


class MessageDelivery:
    """把一个 (通道, 实例) 的采集结果投出去。

    :param producer: 投递器（``kafka.producer.MessageProducer``）——
        需要 ``produce_metrics(records)`` 与 ``produce_alarms(records)``。
    :param watermark: 水位存储（``watermark.WatermarkStore``）。
    """

    def __init__(self, producer: Any, watermark: Any) -> None:
        self._producer = producer
        self._watermark = watermark

    def deliver(
        self, channel: str, instance_id: str, outcome: TaskOutcome
    ) -> Tuple[DeliveryStats, List[TaskFailure]]:
        """投递一个 (通道, 实例) 的结果。

        :returns: ``(统计, 失败清单)`` —— 失败清单为空表示这组全投出去了。
        """
        metric_stats, metric_failure = self._deliver_metrics(
            channel, instance_id, outcome,
        )
        alarm_stats, alarm_failure = self._deliver_alarms(
            channel, instance_id, outcome,
        )
        stats = DeliveryStats(
            planned=metric_stats.planned + alarm_stats.planned,
            delivered=metric_stats.delivered + alarm_stats.delivered,
            failed=metric_stats.failed + alarm_stats.failed,
            alarms=alarm_stats.planned,
        )
        failures = [item for item in (metric_failure, alarm_failure) if item]
        return stats, failures

    # -- 指标 -------------------------------------------------------------

    def _deliver_metrics(
        self, channel: str, instance_id: str, outcome: TaskOutcome
    ) -> Tuple[DeliveryStats, Optional[TaskFailure]]:
        """投递指标消息 —— **成功才推进指标水位**。"""
        records = []
        keys = []
        seen = set()
        for message in outcome.messages:
            key = metric_key(instance_id, message)
            if key in seen:
                continue
            seen.add(key)
            if self._watermark.has_key(key):
                continue
            records.append(metric_record(message))
            keys.append(key)
        if not records:
            if not outcome.messages and channel != CHANNEL_ALARM:
                # **告警通道本来就不产出指标消息**（告警走 ``outcome.alarms``），
                # 不排除它 → 每 30 秒一条无意义的 WARNING，会把真告警淹掉。
                LOGGER.warning(
                    '本轮该组没有产出任何指标消息（空结果）—— channel=%s instance=%s',
                    channel, instance_id,
                )
            return DeliveryStats(), None
        try:
            self._producer.produce_metrics(records)
        except Exception as exc:  # noqa: B902 —— 投递失败要隔离
            # **不变量**：指标水位**只能在投递成功之后**推进（与告警侧同一条）。
            LOGGER.exception(
                '指标投递失败 —— 整批不推进水位，下轮重投：channel=%s instance=%s',
                channel, instance_id,
            )
            return (
                DeliveryStats(planned=len(records), failed=len(records)),
                TaskFailure(channel, instance_id, type(exc).__name__, reason(exc)),
            )
        for key in keys:
            self._watermark.remember_key(key)
        self._advance_metric(channel, instance_id, outcome.end_time_ms)
        return DeliveryStats(planned=len(records), delivered=len(records)), None

    # -- 告警 -------------------------------------------------------------

    def _deliver_alarms(
        self, channel: str, instance_id: str, outcome: TaskOutcome
    ) -> Tuple[DeliveryStats, Optional[TaskFailure]]:
        """投递告警消息 —— **成功才推进告警水位**。"""
        records = []
        keys = []
        seen = set()
        for item in outcome.alarms:
            key = item.idempotency_key
            if key in seen:
                continue
            seen.add(key)
            if self._watermark.has_key(key):
                continue
            records.append(alarm_record(item.message))
            keys.append(key)
        if not records:
            return DeliveryStats(), None
        try:
            self._producer.produce_alarms(records)
        except Exception as exc:  # noqa: B902 —— 投递失败要隔离
            # **不变量**：告警水位**只能在投递成功之后**推进。
            # 在这里推进水位 → 下一轮的游标过滤会把**从未投出**的告警判成
            # 「不新」→ 告警整条**静默丢失**（日志里只剩这一条 exception，
            # 没有任何计数能暴露它）。守卫见
            # ``tests/unit/test_scheduler_alarm_guards.py``。
            LOGGER.exception(
                '告警投递失败 —— 整批不推进水位，下轮重投：channel=%s instance=%s',
                channel, instance_id,
            )
            return (
                DeliveryStats(planned=len(records), failed=len(records)),
                TaskFailure(channel, instance_id, type(exc).__name__, reason(exc)),
            )
        for key in keys:
            self._watermark.remember_key(key)
        self._advance_alarm(instance_id, outcome)
        return DeliveryStats(planned=len(records), delivered=len(records)), None

    # -- 水位推进 ---------------------------------------------------------

    def _advance_metric(
        self, channel: str, instance_id: str, end_time_ms: Optional[int]
    ) -> None:
        """推进指标水位 —— **只在投递成功之后**。"""
        if end_time_ms is None:
            LOGGER.warning(
                '该组没有报告结束时刻 —— 本轮不推进指标水位'
                '（宁可重复投递，也不丢数据）：channel=%s instance=%s',
                channel, instance_id,
            )
            return
        self._watermark.advance_metric(
            metric_stream(instance_id, channel), int(end_time_ms),
        )

    def _advance_alarm(self, instance_id: str, outcome: TaskOutcome) -> None:
        """推进告警水位 —— **只在投递成功之后**。"""
        cursor = outcome.alarm_cursor or alarm_cursor(outcome.alarms)
        if not cursor:
            return
        self._watermark.advance_alarm(alarm_stream(instance_id), str(cursor))
