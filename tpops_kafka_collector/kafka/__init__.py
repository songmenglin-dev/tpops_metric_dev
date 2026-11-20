"""投递层：唯一 JSON 序列化出口 + Kafka 生产者。

``serializer`` 是整个生产代码里**唯一**用 ``json.dumps`` 产出 ITSCADA 消息的地方；
``producer`` 只负责把已序列化的消息投出去，不碰消息体。
"""

from .serializer import (
    ALARM_FIELDS,
    ALARM_TAG_FIELDS,
    METRIC_FIELDS,
    METRIC_TAG_FIELDS,
    SerializerError,
    apply_value_type,
    dumps,
    serialize_alarm,
    serialize_metric,
    to_epoch_millis,
)

__all__ = [
    'ALARM_FIELDS',
    'ALARM_TAG_FIELDS',
    'METRIC_FIELDS',
    'METRIC_TAG_FIELDS',
    'SerializerError',
    'dumps',
    'serialize_alarm',
    'serialize_metric',
    'apply_value_type',
    'to_epoch_millis',
]
