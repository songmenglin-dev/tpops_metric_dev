"""手工演练用的**桩**（通过 ``PYTHONPATH`` 里的 ``sitecustomize`` 注入）。

用途：让 ``python -m tpops_kafka_collector.main`` 在**不连任何外部服务**的
前提下真跑起来 —— 这样演练走的是**真实的装配与信号处理代码路径**，
而不是把生产代码换掉。

注入点选在**传输层**（而不是 ``main.py`` 的模块属性上）：

- ``requests.Session.request`` —— TPOPS 的 HTTP 请求全部走它；
- ``tpops_kafka_collector.kafka.producer.KafkaProducer`` —— Kafka 生产者。

> ``python -m pkg.mod`` 会把模块**再执行一遍**（``__main__`` 与
> ``pkg.mod`` 是两个模块对象），所以直接改 ``tpops_kafka_collector.main``
> 的属性**对 ``-m`` 入口无效** —— 传输层注入才走得通。
"""
import json

#: 桩平台上的实例列表。
INSTANCES = [{
    'id': 'drill-instance-1',
    'name': 'gauss-drill',
    'mode': 'Combined',
    'engine_name': 'gaussdbv5',
    'engine_version': 'V2.0-10.0.0',
    'instance_status': 'normal',
}]

#: 桩平台上的节点清单（一个 master）。
NODES = [{
    'node_id': 'drill-node-1',
    'role': 'master',
    'component_id': 'drill-cn-1',
    'host_ip': '10.0.0.1',
    'type': 'CN',
}]


class FakeResponse:
    """``requests`` 响应的替身。"""

    def __init__(self, status_code=200, headers=None, payload=None):
        self.status_code = status_code
        self.headers = dict(headers or {})
        self._payload = {} if payload is None else payload

    @property
    def text(self):
        return json.dumps(self._payload)

    def json(self):
        return self._payload


def _payload_for(url):
    """按 URL 决定桩响应体。"""
    if 'mini-token' in url:
        return FakeResponse(
            200,
            {'X-Mini-Token': 'drill-mini-token'},
            {'session_timeout': 1440},
        )
    if 'key-view-execute-node' in url:
        return FakeResponse(200, {}, {'nodes': NODES})
    if url.endswith('/instances'):
        return FakeResponse(200, {}, {'instances': INSTANCES, 'total_count': 1})
    return FakeResponse(200, {}, {})


def _fake_request(self, method, url, **kwargs):
    """替掉 ``requests.Session.request`` —— 不发任何真实请求。"""
    return _payload_for(url)


class FakeFuture:
    """Kafka 发送 future 的替身（永远确认成功）。"""

    def get(self, timeout=None):
        return None


class FakeKafkaProducer:
    """Kafka 生产者的替身（不连 broker）。"""

    #: 生产者模块按真生产者的 ``DEFAULT_CONFIG`` 过滤参数 —— 桩也得有。
    DEFAULT_CONFIG = {}

    def __init__(self, *args, **kwargs):
        self.sent = []
        self.closed = False

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))
        return FakeFuture()

    def flush(self, timeout=None):
        return None

    def close(self, timeout=None):
        self.closed = True


def _install():
    """把桩装上（装不上也不该让解释器起不来）。"""
    try:
        import requests
        requests.Session.request = _fake_request
    except Exception:  # pragma: no cover —— 桩坏了不该拖垮演练
        pass
    try:
        from kafka import KafkaProducer as _RealKafkaProducer
        FakeKafkaProducer.DEFAULT_CONFIG = _RealKafkaProducer.DEFAULT_CONFIG
    except Exception:  # pragma: no cover
        pass
    try:
        import tpops_kafka_collector.kafka.producer as producer_module
        producer_module.KafkaProducer = FakeKafkaProducer
    except Exception:  # pragma: no cover
        pass


_install()
