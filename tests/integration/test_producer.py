"""Kafka 生产者（任务 1.5）。

**不连真实 Kafka** —— 全部用假 producer 对象注入。

守住四件事：

1. **投递参数**：PLAINTEXT、``acks=all``、``enable_idempotence=False``、
   压缩算法与分区数可配；
2. **分 Topic**：指标走指标 Topic、告警走告警 Topic，绝不混投；
   Key 分别是 ``instance_id`` 与 ``alarm_id``；
3. **失败语义**：任一条没投出去就抛 ``ProduceError``（其子类），
   调用方据此**不推进水位**，失败的消息能被重投；
4. **返回值**：批量投递的结果能区分「哪些成功、哪些失败」。
"""

import pytest

from tpops_kafka_collector.errors import ProduceError, is_retryable
from tpops_kafka_collector.kafka.producer import (
    ACKS,
    ENABLE_IDEMPOTENCE,
    SECURITY_PROTOCOL,
    BatchResult,
    MessageProducer,
    ProduceBatchError,
    alarm_record,
    build_producer_config,
    metric_record,
    supported_kwargs,
)


# ---------------------------------------------------------------------------
# 假 producer（不碰网络）
# ---------------------------------------------------------------------------

class FakeFuture:
    """假 FutureRecordMetadata：``get`` 可能抛错。"""

    def __init__(self, error=None):
        self._error = error
        self.value = None

    def get(self, timeout=None):
        if self._error is not None:
            raise self._error
        return self.value


class FakeProducer:
    """假 KafkaProducer：记录发送，模拟发送阶段 / 确认阶段的失败。"""

    def __init__(self, ack_errors=None, send_errors=None):
        self.sent = []
        self.flush_count = 0
        self.closed = False
        self._ack_errors = dict(ack_errors or {})
        self._send_errors = dict(send_errors or {})

    def send(self, topic, value=None, key=None, **kwargs):
        if key in self._send_errors:
            raise self._send_errors[key]
        self.sent.append((topic, key, value))
        return FakeFuture(self._ack_errors.get(key))

    def flush(self, timeout=None):
        self.flush_count += 1

    def close(self, timeout=None):
        self.closed = True


class FakeAdmin:
    """假 KafkaAdminClient：记录创建的 Topic。"""

    def __init__(self, existing=()):
        self.created = []
        self.existing = set(existing)
        self.closed = False

    def create_topics(self, new_topics, validate_only=False):
        for topic in new_topics:
            if topic.name in self.existing:
                continue
            self.created.append((topic.name, topic.num_partitions))
            self.existing.add(topic.name)

    def close(self):
        self.closed = True


class FakeConfig:
    """最小配置替身（只带 Kafka 相关项）。"""

    def __init__(self, **overrides):
        self.kafka_bootstrap_servers = ('kafka-1:9092', 'kafka-2:9092')
        self.kafka_topic_metrics = 'gaussdb.metrics'
        self.kafka_topic_alarms = 'gaussdb.alarms'
        self.kafka_topic_partitions = 4
        self.kafka_compression = 'lz4'
        for name, value in overrides.items():
            setattr(self, name, value)


def _producer(fake=None, **overrides):
    """装配一个用假 producer 的 MessageProducer。"""
    fake = fake if fake is not None else FakeProducer()
    kwargs = {
        'bootstrap_servers': ('kafka-1:9092',),
        'topic_metrics': 'gaussdb.metrics',
        'topic_alarms': 'gaussdb.alarms',
        'compression': 'lz4',
        'partitions': 4,
        'producer': fake,
    }
    kwargs.update(overrides)
    return MessageProducer(**kwargs), fake


def _metric_message(instance_id='inst-1'):
    return {
        'metricName': 'gaussdb.Tps',
        'metricCode': 'GAUSSDB.Tps',
        'resourceName': '10.0.0.1_node-1',
        'value': 12.5,
        'unit': 'tps',
        'clock': 1760000000000,
        'tags': {'instance_id': instance_id},
    }


def _alarm_message(alarm_id='alarm-1'):
    return {
        'alarmId': alarm_id,
        'alarmName': '连接数超阈值',
        'alarmContent': 'too many connections',
        'priority': '2',
        'alarmTime': 1760000000000,
        'status': 'active',
        'recoverTime': None,
        'resourceName': '10.0.0.1_node-1',
        'metricName': 'gaussdb.Connection',
        'metricCode': 'GAUSSDB.Connection',
        'value': '900',
        'unit': None,
        'tags': {'instance_id': 'inst-1'},
    }


# ---------------------------------------------------------------------------
# 投递参数
# ---------------------------------------------------------------------------

def test_producer_config_declares_plaintext_acks_all_and_idempotence_off():
    """PLAINTEXT + acks=all + 显式关闭幂等，三项都写进生产者配置。"""
    config = build_producer_config(('kafka-1:9092',), compression='lz4')

    assert config['security_protocol'] == SECURITY_PROTOCOL == 'PLAINTEXT'
    assert config['acks'] == ACKS == 'all'
    assert config['enable_idempotence'] is ENABLE_IDEMPOTENCE is False
    assert config['compression_type'] == 'lz4'
    assert config['bootstrap_servers'] == ['kafka-1:9092']


def test_producer_config_has_no_sasl_or_ssl_options():
    """客户环境是 PLAINTEXT —— 不加 SASL / SSL 相关配置项。"""
    config = build_producer_config(('kafka-1:9092',))

    forbidden = [name for name in config if name.startswith(('sasl_', 'ssl_'))]
    assert forbidden == [], f'PLAINTEXT 环境不该带 SASL/SSL 配置：{forbidden}'


def test_compression_and_partitions_are_configurable():
    """压缩算法与分区数来自配置，改配置不改代码。"""
    config = build_producer_config(('kafka-1:9092',), compression='gzip')
    assert config['compression_type'] == 'gzip'

    producer, _ = _producer(compression='gzip', partitions=8)
    assert producer.partitions == 8
    assert producer.compression == 'gzip'


def test_supported_kwargs_only_contains_options_known_to_kafka_python():
    """过滤后每个键都必须是 kafka-python 认识的 —— 否则构造时会抛配置错误。"""
    from kafka import KafkaProducer

    kwargs = supported_kwargs(build_producer_config(('kafka-1:9092',)))

    unknown = sorted(set(kwargs) - set(KafkaProducer.DEFAULT_CONFIG))
    assert unknown == [], f'kafka-python 不认识的配置项：{unknown}'


def test_idempotence_is_never_enabled_even_if_client_supports_it():
    """即便将来换成支持幂等的客户端，传下去的值也必须是 False。"""
    config = build_producer_config(('kafka-1:9092',))

    assert config['enable_idempotence'] is False


def test_from_config_uses_configured_topics_and_servers():
    """Topic 名、地址、压缩、分区数全部来自配置。"""
    config = FakeConfig(
        kafka_bootstrap_servers=('kafka-9:9092',),
        kafka_topic_metrics='m.topic',
        kafka_topic_alarms='a.topic',
        kafka_topic_partitions=8,
        kafka_compression='snappy',
    )
    producer = MessageProducer.from_config(config, producer=FakeProducer())

    assert producer.topic_metrics == 'm.topic'
    assert producer.topic_alarms == 'a.topic'
    assert producer.partitions == 8
    assert producer.compression == 'snappy'


# ---------------------------------------------------------------------------
# Key 规则
# ---------------------------------------------------------------------------

def test_metric_record_key_is_instance_id():
    """指标消息的 Key = tags.instance_id。"""
    record = metric_record(_metric_message('inst-42'))

    assert record.key == 'inst-42'
    assert '"metricName":"gaussdb.Tps"' in record.payload


def test_alarm_record_key_is_alarm_id():
    """告警消息的 Key = alarmId。"""
    record = alarm_record(_alarm_message('alarm-77'))

    assert record.key == 'alarm-77'
    assert '"alarmId":"alarm-77"' in record.payload


def test_record_without_key_raises_instead_of_silently_dropping():
    """取不到 Key 显式报错，不静默丢消息。"""
    message = _metric_message()
    message['tags'] = {}

    with pytest.raises(ValueError):
        metric_record(message)


# ---------------------------------------------------------------------------
# 分 Topic 投递
# ---------------------------------------------------------------------------

def test_metrics_go_to_metrics_topic_with_instance_id_key():
    """指标消息进指标 Topic，Key 是 instance_id。"""
    producer, fake = _producer()
    record = metric_record(_metric_message('inst-1'))

    producer.produce_metrics([record])

    assert len(fake.sent) == 1
    topic, key, value = fake.sent[0]
    assert topic == 'gaussdb.metrics'
    assert key == b'inst-1'
    assert value == record.payload.encode('utf-8')


def test_alarms_go_to_alarms_topic_with_alarm_id_key():
    """告警消息进告警 Topic，Key 是 alarm_id。"""
    producer, fake = _producer()
    record = alarm_record(_alarm_message('alarm-9'))

    producer.produce_alarms([record])

    topic, key, _ = fake.sent[0]
    assert topic == 'gaussdb.alarms'
    assert key == b'alarm-9'


def test_metrics_and_alarms_are_never_mixed_into_one_topic():
    """指标与告警分 Topic，不混投。"""
    producer, fake = _producer()

    producer.produce_metrics([metric_record(_metric_message())])
    producer.produce_alarms([alarm_record(_alarm_message())])

    topics = [topic for topic, _, _ in fake.sent]
    assert topics == ['gaussdb.metrics', 'gaussdb.alarms']


def test_same_key_goes_to_same_partition():
    """同一实例的多条消息用同一个 Key —— 由 broker 落同一分区，顺序不乱。"""
    producer, fake = _producer()

    producer.produce_metrics([
        metric_record(_metric_message('inst-1')),
        metric_record(_metric_message('inst-1')),
    ])

    keys = [key for _, key, _ in fake.sent]
    assert keys == [b'inst-1', b'inst-1']


# ---------------------------------------------------------------------------
# 成功路径
# ---------------------------------------------------------------------------

def test_successful_batch_returns_result_with_all_records():
    """全部成功 → 返回结果，failed 为空。"""
    producer, fake = _producer()
    records = [metric_record(_metric_message('inst-1')),
               metric_record(_metric_message('inst-2'))]

    result = producer.produce_metrics(records)

    assert isinstance(result, BatchResult)
    assert result.topic == 'gaussdb.metrics'
    assert result.is_success is True
    assert result.succeeded == tuple(records)
    assert result.failed == ()
    assert fake.flush_count >= 1


def test_empty_batch_sends_nothing_and_succeeds():
    """空批次不发送任何消息。"""
    producer, fake = _producer()

    result = producer.produce_metrics([])

    assert fake.sent == []
    assert result.is_success is True
    assert result.succeeded_count == 0


# ---------------------------------------------------------------------------
# 失败路径 —— 抛 ProduceError，调用方据此不推进水位
# ---------------------------------------------------------------------------

def test_partial_failure_raises_produce_error_with_failed_records():
    """一条失败 → 抛 ProduceError，并带上没投出去的消息（供重投）。"""
    boom = RuntimeError('broker unavailable')
    fake = FakeProducer(ack_errors={b'inst-2': boom})
    producer, _ = _producer(fake)
    records = [metric_record(_metric_message('inst-1')),
               metric_record(_metric_message('inst-2'))]

    with pytest.raises(ProduceError) as excinfo:
        producer.produce_metrics(records)

    error = excinfo.value
    assert isinstance(error, ProduceBatchError)
    assert error.result.failed_count == 1
    assert error.result.succeeded_count == 1
    assert error.result.failed[0].record.key == 'inst-2'
    reason = str(error.failed[0].reason)
    assert 'inst-2' in reason or 'broker unavailable' in reason


def test_send_stage_failure_also_raises_produce_error():
    """发送阶段就抛错（缓冲区满/超时）同样算没投出去。"""
    boom = RuntimeError('metadata timeout')
    fake = FakeProducer(send_errors={b'inst-1': boom})
    producer, _ = _producer(fake)

    with pytest.raises(ProduceError):
        producer.produce_metrics([metric_record(_metric_message('inst-1'))])


def test_all_failed_raises_produce_error():
    """整批失败也要抛，不能悄悄吞掉。"""
    fake = FakeProducer(ack_errors={b'inst-1': RuntimeError('nope'),
                                    b'inst-2': RuntimeError('nope')})
    producer, _ = _producer(fake)
    records = [metric_record(_metric_message('inst-1')),
               metric_record(_metric_message('inst-2'))]

    with pytest.raises(ProduceError) as excinfo:
        producer.produce_metrics(records)

    assert excinfo.value.result.failed_count == 2
    assert excinfo.value.result.succeeded_count == 0


def test_produce_error_is_retryable():
    """ProduceError 可重试 —— 重试框架据此退避重投。"""
    assert is_retryable(ProduceError('boom')) is True
    assert is_retryable(ProduceBatchError('boom', BatchResult(topic='t'))) is True


def test_failure_never_gets_swallowed_into_another_exception_type():
    """失败必须是 ProduceError（子类也算），不能被转成别的类型。"""
    fake = FakeProducer(ack_errors={b'inst-1': RuntimeError('nope')})
    producer, _ = _producer(fake)

    with pytest.raises(ProduceError):
        producer.produce_alarms([alarm_record(_alarm_message('inst-1'))])


# ---------------------------------------------------------------------------
# 生命周期与 Topic 建立
# ---------------------------------------------------------------------------

def test_close_closes_underlying_producer():
    """关闭生产者时把底层 producer 一起关掉。"""
    producer, fake = _producer()

    producer.close()

    assert fake.closed is True


def test_ensure_topics_creates_topics_with_configured_partitions():
    """按配置的分区数建 Topic（已存在则跳过）。"""
    producer, _ = _producer(partitions=8)
    admin = FakeAdmin()

    created = producer.ensure_topics(admin=admin)

    assert created == ('gaussdb.metrics', 'gaussdb.alarms')
    assert sorted(admin.created) == [('gaussdb.alarms', 8), ('gaussdb.metrics', 8)]


def test_ensure_topics_skips_existing_topics():
    """Topic 已存在时不重复创建。"""
    producer, _ = _producer(partitions=4)
    admin = FakeAdmin(existing=['gaussdb.metrics'])

    producer.ensure_topics(admin=admin)

    assert admin.created == [('gaussdb.alarms', 4)]


def test_ensure_topics_creates_each_topic_independently():
    """两个 Topic 逐个建 —— 一个已存在不影响另一个。"""
    producer, _ = _producer(partitions=4)
    admin = FakeAdmin(existing=['gaussdb.metrics'])

    producer.ensure_topics(admin=admin)

    assert ('gaussdb.metrics', 4) not in admin.created
    assert ('gaussdb.alarms', 4) in admin.created


def test_broker_failure_at_startup_is_retryable_transient_error():
    """broker 不可达 → 包装成可重试的 TransientError，不把客户端异常原样抛给上层。"""
    from kafka import KafkaProducer as RealProducer
    from kafka.errors import NoBrokersAvailable

    from tpops_kafka_collector.errors import TransientError
    from tpops_kafka_collector.kafka import producer as producer_module

    original = producer_module.KafkaProducer

    class Boom:
        DEFAULT_CONFIG = RealProducer.DEFAULT_CONFIG

        def __init__(self, **kwargs):
            raise NoBrokersAvailable()

    producer_module.KafkaProducer = Boom
    try:
        with pytest.raises(TransientError) as excinfo:
            producer_module.create_producer(
                producer_module.build_producer_config(('127.0.0.1:19092',))
            )
    finally:
        producer_module.KafkaProducer = original

    assert is_retryable(excinfo.value) is True
    assert 'Kafka' in str(excinfo.value)


def test_topic_creation_failure_is_retryable_transient_error():
    """建 Topic 失败 → 可重试的 TransientError。"""
    from kafka.errors import KafkaError

    from tpops_kafka_collector.errors import TransientError

    class BrokenAdmin:
        def create_topics(self, new_topics, validate_only=False):
            raise KafkaError('broker down')

        def close(self):
            pass

    producer, _ = _producer()

    with pytest.raises(TransientError):
        producer.ensure_topics(admin=BrokenAdmin())

