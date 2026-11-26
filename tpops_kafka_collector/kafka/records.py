"""投递记录：待投递的消息、投递结果与 Key 规则。

从 :mod:`tpops_kafka_collector.kafka.producer` 拆出来的 —— 生产者只管
「怎么投」，**Key 取什么、消息怎么序列化**的口径落在这里。

两条不可改的口径：

- **Key**：指标用 ``tags.instance_id``、告警用 ``alarmId`` ——
  同 Key 落同一分区，同一实例的消息严格有序；
- **序列化**：``payload`` 一律由 :mod:`tpops_kafka_collector.kafka.serializer`
  产出（全仓唯一的 ``json.dumps`` 出口），本模块不自己拼 JSON。
"""
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Tuple
from loguru import logger

from ..errors import ProduceError
from .serializer import serialize_alarm, serialize_metric

LOGGER = logger

__all__ = [
    'BatchResult',
    'FailedRecord',
    'ProduceBatchError',
    'ProduceRecord',
    'alarm_record',
    'metric_record',
]


@dataclass(frozen=True)
class ProduceRecord:
    """一条待投递的消息（不可变）。

    :param key: 分区键 —— 指标用 ``instance_id``、告警用 ``alarm_id``。
    :param payload: **已序列化**的 JSON 文本（由 ``kafka.serializer`` 产出）。
    """

    key: str
    payload: str


@dataclass(frozen=True)
class FailedRecord:
    """一条没投出去的消息及其原因。"""

    record: ProduceRecord
    reason: str


@dataclass(frozen=True)
class BatchResult:
    """一批投递的结果（不可变）。

    全部成功时由生产者的 ``produce_metrics`` / ``produce_alarms`` 返回；
    有失败时随 :class:`ProduceBatchError` 抛出。
    """

    topic: str
    succeeded: Tuple[ProduceRecord, ...] = ()
    failed: Tuple[FailedRecord, ...] = ()

    @property
    def is_success(self) -> bool:
        """这一批是否全部投出去了。"""
        return not self.failed

    @property
    def succeeded_count(self) -> int:
        return len(self.succeeded)

    @property
    def failed_count(self) -> int:
        return len(self.failed)


class ProduceBatchError(ProduceError):
    """一批消息里有没投出去的。

    **它是 :class:`ProduceError` 的子类** —— 调用方 ``except ProduceError``
    照样抓住，``errors.is_retryable`` 照样判为可重试。
    额外的 ``result`` / ``failed`` 只是把「哪几条没投出去」告诉调用方，
    好让它们下轮重投。
    """

    def __init__(self, message: str, result: BatchResult) -> None:
        super().__init__(message)
        self.result = result
        self.failed = result.failed


def metric_record(message: Mapping[str, Any], value_type: Optional[str] = None) -> ProduceRecord:
    """指标消息 → 待投递记录。

    ``payload`` 走唯一序列化出口；**Key = ``tags.instance_id``**。
    """
    payload = serialize_metric(message, value_type)
    return ProduceRecord(key=_metric_key(message), payload=payload)


def alarm_record(message: Mapping[str, Any]) -> ProduceRecord:
    """告警消息 → 待投递记录。

    ``payload`` 走唯一序列化出口；**Key = ``alarmId``**。
    """
    payload = serialize_alarm(message)
    return ProduceRecord(key=_alarm_key(message), payload=payload)


def _metric_key(message: Mapping[str, Any]) -> str:
    """取指标的分区键：``tags.instance_id``。取不到就显式报错。"""
    tags = message.get('tags')
    if not isinstance(tags, Mapping):
        raise ValueError('指标消息缺少 tags，无法取分区键 instance_id')
    key = tags.get('instance_id')
    if not key:
        raise ValueError('指标消息的 tags.instance_id 为空，无法取分区键')
    return str(key)


def _alarm_key(message: Mapping[str, Any]) -> str:
    """取告警的分区键：``alarmId``。取不到就显式报错。"""
    key = message.get('alarmId')
    if not key:
        raise ValueError('告警消息缺少 alarmId，无法取分区键')
    return str(key)
