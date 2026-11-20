"""投递层：唯一 JSON 序列化出口 + Kafka 生产者。

``serializer`` 是整个生产代码里**唯一**用 ``json.dumps`` 产出 ITSCADA 消息的地方；
``producer`` 只负责把已序列化的消息投出去，不碰消息体。
"""

from .producer import (
    ACKS,
    ENABLE_IDEMPOTENCE,
    SECURITY_PROTOCOL,
    BatchResult,
    FailedRecord,
    MessageProducer,
    ProduceBatchError,
    ProduceRecord,
    alarm_record,
    build_producer_config,
    create_producer,
    metric_record,
    supported_kwargs,
)
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
    'ACKS',
    'ALARM_FIELDS',
    'ALARM_TAG_FIELDS',
    'ENABLE_IDEMPOTENCE',
    'METRIC_FIELDS',
    'METRIC_TAG_FIELDS',
    'SECURITY_PROTOCOL',
    'BatchResult',
    'FailedRecord',
    'MessageProducer',
    'ProduceBatchError',
    'ProduceRecord',
    'SerializerError',
    'alarm_record',
    'apply_value_type',
    'build_producer_config',
    'create_producer',
    'dumps',
    'metric_record',
    'serialize_alarm',
    'serialize_metric',
    'supported_kwargs',
    'to_epoch_millis',
]
