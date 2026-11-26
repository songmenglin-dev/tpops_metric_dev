"""Kafka 生产者（任务 1.5）。

把**已序列化**的消息投到 Kafka。本模块**不碰消息体** —— JSON 的产出只有
:mod:`tpops_kafka_collector.kafka.serializer` 一处。

冻结口径：客户环境是 **PLAINTEXT**（配置里**没有** SASL/SSL 项）；投递确认级别是
``acks=all``（不是默认的 ``acks=1``）；**显式关闭** ``enable_idempotence``
（幂等生产者要求单分区 + 特定重试配置，与客户环境的分区数冲突）；
指标与告警**分 Topic**、绝不混投，Key 分别是 ``instance_id`` 与 ``alarm_id``；
压缩与分区数由 ``KAFKA_COMPRESSION`` / ``KAFKA_TOPIC_PARTITIONS`` 控制。

失败语义（**关键**）：任一条消息没投出去就抛 :class:`ProduceBatchError`
（``ProduceError`` 的子类）。调用方据此**不推进水位**，下一轮重投 ——
至少一次语义。失败**不**被吞掉，也**不**被转成别的异常类型。

记录 / 结果 / Key 规则在 :mod:`tpops_kafka_collector.kafka.records`。
"""
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger

from ..errors import TransientError
from .records import (
    BatchResult,
    FailedRecord,
    ProduceBatchError,
    ProduceRecord,
    alarm_record,
    metric_record,
)

LOGGER = logger

__all__ = [
    'ACKS',
    'DEFAULT_CLIENT_ID',
    'DEFAULT_LINGER_MS',
    'DEFAULT_RETRIES',
    'DEFAULT_SEND_TIMEOUT',
    'ENABLE_IDEMPOTENCE',
    'SECURITY_PROTOCOL',
    'BatchResult',
    'FailedRecord',
    'MessageProducer',
    'ProduceBatchError',
    'ProduceRecord',
    'alarm_record',
    'build_producer_config',
    'create_producer',
    'metric_record',
    'supported_kwargs',
]

#: 客户环境要求 PLAINTEXT —— 不加 SASL / SSL 配置项。
SECURITY_PROTOCOL = 'PLAINTEXT'

#: 客户要求的投递确认级别：所有 ISR 副本都确认才算成功。
ACKS = 'all'

#: 幂等生产者**显式关闭**（不依赖客户端默认值）。
ENABLE_IDEMPOTENCE = False

#: 单条消息等待 broker 确认的超时（秒）。
DEFAULT_SEND_TIMEOUT = 30.0

#: 客户端标识（便于在 broker 侧辨认本采集端）。
DEFAULT_CLIENT_ID = 'tpops-kafka-collector'

#: 客户端内部重试次数（不含应用层退避 —— 应用层重试由 ``retry`` 模块负责）。
DEFAULT_RETRIES = 5

#: 攒批等待时间（毫秒）。
DEFAULT_LINGER_MS = 5

#: 每连接在途请求数。取 1 是为了「同实例消息严格有序」：重试时在途请求可能被
#: 后来的消息超越；幂等已关闭，这个约束必须自己扛。
DEFAULT_MAX_IN_FLIGHT = 1


# --- 生产者配置 ---------------------------------------------------------

def build_producer_config(
    bootstrap_servers: Sequence[str],
    compression: str = 'gzip',
    client_id: str = DEFAULT_CLIENT_ID,
    retries: int = DEFAULT_RETRIES,
    linger_ms: int = DEFAULT_LINGER_MS,
) -> Dict[str, Any]:
    """构造生产者配置（**逻辑口径**，含 ``enable_idempotence``）。

    这是「我们希望的生产者长什么样」，与客户端认不认这些键无关 ——
    过滤交给 :func:`supported_kwargs`。
    """
    if not bootstrap_servers:
        raise ValueError('缺少 Kafka bootstrap servers')
    return {
        'bootstrap_servers': list(bootstrap_servers),
        'acks': ACKS,
        # 显式关闭幂等：幂等生产者要求单分区 + 特定重试配置，与客户环境的分区数冲突。
        'enable_idempotence': ENABLE_IDEMPOTENCE,
        'security_protocol': SECURITY_PROTOCOL,
        'compression_type': compression,
        'client_id': client_id,
        'retries': retries,
        'linger_ms': linger_ms,
        'max_in_flight_requests_per_connection': DEFAULT_MAX_IN_FLIGHT,
    }


def supported_kwargs(config: Mapping[str, Any]) -> Dict[str, Any]:
    """按已安装的 ``kafka-python`` 版本过滤配置。

    ``kafka-python==2.0.2`` **没有**幂等生产者（``KafkaProducer.DEFAULT_CONFIG``
    里没有 ``enable_idempotence``），传入未知键会抛 ``KafkaConfigurationError``。
    幂等在 2.0.2 上本就**无法开启**，过滤掉它与「显式关闭」语义一致；若将来换成
    支持幂等的客户端，这一键会被原样传下去（值恒为 ``False``）。
    """
    known = set(KafkaProducer.DEFAULT_CONFIG)
    dropped = sorted(set(config) - known)
    if dropped:
        LOGGER.info('当前 kafka-python 不支持的配置项已忽略：{}', dropped)
    return {name: value for name, value in config.items() if name in known}


def create_producer(config: Mapping[str, Any]) -> Any:
    """按配置创建真实的 ``KafkaProducer``（本模块唯一会碰网络的入口）。

    broker 不可达等客户端级故障 → 包装成 :class:`TransientError`（可重试），
    不把客户端的原始异常直接抛给调度层。
    """
    try:
        return KafkaProducer(**supported_kwargs(config))
    except KafkaError as exc:
        raise TransientError(f'Kafka 生产者创建失败：{exc}') from exc


# --- 生产者 -------------------------------------------------------------

class MessageProducer:
    """把已序列化的消息投到 Kafka。

    - 指标与告警**分 Topic**（``produce_metrics`` / ``produce_alarms``）；
    - 任一条没投出去 → 抛 :class:`ProduceBatchError`（``ProduceError`` 子类），
      调用方据此**不推进水位**，下一轮重投；
    - 底层 ``KafkaProducer`` 可注入 —— 测试**不连真实 Kafka**。
    """

    def __init__(
        self,
        bootstrap_servers: Sequence[str],
        topic_metrics: str,
        topic_alarms: str,
        compression: str = 'gzip',
        partitions: int = 4,
        producer: Optional[Any] = None,
        send_timeout: float = DEFAULT_SEND_TIMEOUT,
        client_id: str = DEFAULT_CLIENT_ID,
        retries: int = DEFAULT_RETRIES,
        linger_ms: int = DEFAULT_LINGER_MS,
        replication_factor: int = 1,
    ) -> None:
        """装配生产者。

        :param bootstrap_servers: broker 地址列表；:param topic_metrics: 指标 Topic；
        :param topic_alarms: 告警 Topic；:param compression: 压缩算法
            （``none``/``gzip`` —— 只有这两种零额外依赖）；:param partitions:
            建 Topic 时的分区数；:param producer: 注入的底层生产者（测试用），
            不给则真实创建；:param send_timeout: 单条消息等待 broker 确认的
            超时（秒）；:param replication_factor: 建 Topic 时的副本数。
        """
        if not bootstrap_servers:
            raise ValueError('缺少 Kafka bootstrap servers')
        if not topic_metrics or not topic_alarms:
            raise ValueError('缺少 Kafka Topic 名')
        if partitions < 1:
            raise ValueError('Kafka 分区数至少为 1')

        self._bootstrap_servers = tuple(bootstrap_servers)
        self._topic_metrics = topic_metrics
        self._topic_alarms = topic_alarms
        self._compression = compression
        self._partitions = int(partitions)
        self._send_timeout = send_timeout
        self._client_id = client_id
        self._replication_factor = replication_factor
        self._producer = producer
        if self._producer is None:
            self._producer = create_producer(
                build_producer_config(
                    self._bootstrap_servers,
                    compression=compression,
                    client_id=client_id,
                    retries=retries,
                    linger_ms=linger_ms,
                )
            )

    @classmethod
    def from_config(
        cls,
        config: Any,
        producer: Optional[Any] = None,
        **kwargs: Any
    ) -> 'MessageProducer':
        """从 :class:`tpops_kafka_collector.config.Config` 装配生产者。"""
        return cls(
            bootstrap_servers=config.kafka_bootstrap_servers,
            topic_metrics=config.kafka_topic_metrics,
            topic_alarms=config.kafka_topic_alarms,
            compression=config.kafka_compression,
            partitions=config.kafka_topic_partitions,
            producer=producer,
            **kwargs
        )

    @property
    def topic_metrics(self) -> str:
        return self._topic_metrics

    @property
    def topic_alarms(self) -> str:
        return self._topic_alarms

    @property
    def partitions(self) -> int:
        return self._partitions

    @property
    def compression(self) -> str:
        return self._compression

    def produce_metrics(self, records: Sequence[ProduceRecord]) -> BatchResult:
        """把指标消息投到指标 Topic（Key 为 ``instance_id``）。

        全部成功返回投递结果；任一条没投出去抛 :class:`ProduceBatchError`。
        """
        return self._produce(self._topic_metrics, records)

    def produce_alarms(self, records: Sequence[ProduceRecord]) -> BatchResult:
        """把告警消息投到告警 Topic（Key 为 ``alarm_id``）。

        全部成功返回投递结果；任一条没投出去抛 :class:`ProduceBatchError`。
        """
        return self._produce(self._topic_alarms, records)

    def _produce(self, topic: str, records: Sequence[ProduceRecord]) -> BatchResult:
        """投一批消息；任一条失败 → 抛 :class:`ProduceBatchError`。"""
        items = tuple(records)
        if not items:
            return BatchResult(topic=topic)

        outcomes = self._send_all(topic, items)
        self._flush()

        succeeded: List[ProduceRecord] = []
        failed: List[FailedRecord] = []
        for record, future, error in outcomes:
            if error is None:
                error = _ack_error(future, self._send_timeout)
            if error is None:
                succeeded.append(record)
            else:
                failed.append(FailedRecord(record=record, reason=error))

        result = BatchResult(
            topic=topic,
            succeeded=tuple(succeeded),
            failed=tuple(failed),
        )
        if result.failed:
            raise ProduceBatchError(
                f'Kafka 投递失败：{result.failed_count}/{len(items)} 条没投出去'
                f'（topic={topic}），本轮不推进水位，下轮重投',
                result,
            )
        return result

    def _send_all(
        self, topic: str, items: Sequence[ProduceRecord]
    ) -> List[Tuple[ProduceRecord, Any, Optional[str]]]:
        """逐条发送，返回 ``(记录, future, 发送阶段错误)`` 三元组列表。

        发送阶段就抛错的（缓冲区满、元数据超时等）记为该条失败，不影响同批其它消息。
        """
        outcomes: List[Tuple[ProduceRecord, Any, Optional[str]]] = []
        for record in items:
            future = None
            error = None
            try:
                future = self._producer.send(
                    topic,
                    key=_to_bytes(record.key),
                    value=_to_bytes(record.payload),
                )
            except Exception as exc:  # noqa: B902 —— 逐条兜住，转成失败原因
                error = _reason(exc)
                LOGGER.error(
                    'Kafka 发送阶段失败：topic={} key={}：{}', topic, record.key, error
                )
            outcomes.append((record, future, error))
        return outcomes

    def _flush(self) -> None:
        """等一批消息被 broker 确认。

        ``flush`` 抛错不抛给调用方 —— 逐条确认结果在 ``future.get`` 里已拿到。
        """
        try:
            self._producer.flush()
        except Exception as exc:  # noqa: B902 —— flush 失败由逐条确认结果体现
            LOGGER.error('Kafka flush 失败：{}', _reason(exc))

    def ensure_topics(self, admin: Optional[Any] = None) -> Tuple[str, ...]:
        """按配置的分区数建两个 Topic（已存在则跳过）。

        :param admin: 注入的 ``KafkaAdminClient``（测试用）；不给则真实创建。
        :returns: 建好的 Topic 名。
        """
        from kafka.admin import KafkaAdminClient

        client = admin
        if client is None:
            try:
                client = KafkaAdminClient(
                    bootstrap_servers=list(self._bootstrap_servers),
                    client_id=self._client_id,
                )
            except KafkaError as exc:
                raise TransientError(f'Kafka 管理客户端创建失败：{exc}') from exc
        try:
            self._create_topics(client)
        finally:
            if admin is None:
                client.close()
        return (self._topic_metrics, self._topic_alarms)

    def _create_topics(self, client: Any) -> None:
        """建 Topic；已存在不算失败。

        **逐个建** —— 一次建多个时只要有一个已存在就整批报错，另一个就再也建不上。
        """
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError

        for name in (self._topic_metrics, self._topic_alarms):
            topic = NewTopic(
                name=name,
                num_partitions=self._partitions,
                replication_factor=self._replication_factor,
            )
            try:
                client.create_topics([topic], validate_only=False)
            except TopicAlreadyExistsError:
                LOGGER.info('Topic 已存在，跳过创建：{}', name)
            except KafkaError as exc:
                raise TransientError(f'创建 Kafka Topic 失败（{name}）：{exc}') from exc

    def close(self) -> None:
        """关闭底层生产者（flush 剩余消息后断开）。"""
        if self._producer is None:
            return
        close = getattr(self._producer, 'close', None)
        if close is None:
            return
        try:
            close()
        except Exception as exc:  # noqa: B902 —— 关闭失败只记日志，不影响退出
            LOGGER.error('关闭 Kafka 生产者失败：{}', _reason(exc))


# --- 内部实现 ------------------------------------------------------------

def _to_bytes(value: str) -> bytes:
    """字符串 → UTF-8 字节。

    ``kafka-python`` 2.0.2 在不配 ``value_serializer`` 时**不做**编码，
    传 ``str`` 会被断言拦下 —— 所以这里自己编码。
    """
    if isinstance(value, bytes):
        return value
    return str(value).encode('utf-8')


def _ack_error(future: Any, timeout: float) -> Optional[str]:
    """等 broker 确认；失败返回原因字符串，成功返回 ``None``。"""
    if future is None:
        return '消息未进入发送队列'
    try:
        future.get(timeout=timeout)
    except Exception as exc:  # noqa: B902 —— 确认失败按失败处理
        return _reason(exc)
    return None


def _reason(exc: BaseException) -> str:
    """把异常转成可读的原因（带类型名，便于定位）。"""
    return f'{type(exc).__name__}: {exc}'
