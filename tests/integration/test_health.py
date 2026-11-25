"""健康端点（任务 3.2）。

守住三件事：

1. **存活与就绪分开** —— ``/healthz`` 只回答「进程还在吗」，``/readyz``
   回答「现在能接活吗」。依赖挂了 ``/healthz`` 仍然是 200：
   两者混为一谈会让编排器在依赖故障时误重启进程。
2. **依赖不可用时 ``/readyz`` 返回 503**。
3. **响应体不泄露凭据** —— 口令、令牌、平台地址、Kafka 地址（含其中的
   用户名）都不出现在响应体里。

真的起一个 HTTP 服务（端口 0，临时端口），用 ``urllib`` 打真实请求 ——
依赖探针是注入的假对象，不碰 TPOPS 与 Kafka。
"""

import json
import urllib.error
import urllib.request

import pytest

from tpops_kafka_collector.config import Config
from tpops_kafka_collector.health import (
    HEALTH_PATH,
    READY_PATH,
    DependencyProbe,
    DependencyStatus,
    HealthEndpoints,
    HealthServer,
    ReadinessMonitor,
    secret_values_from_config,
)

#: 假凭据：出现在任何地方都算泄露。
PASSWORD = 'sup3r-s3cret-pwd'
TOKEN = 'eyJhbGciOiJIUzI1NiJ9.mini-token.signature'
KAFKA_USER = 'kafka-reader'
KAFKA_PASSWORD = 'kafka-s3cret'


def _get(server, path):
    """打一个真实 HTTP 请求，返回 ``(状态码, 响应体字节)``。"""
    host, port = server.address
    url = f'http://{host}:{port}{path}'
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.getcode(), response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def _json_body(raw):
    return json.loads(raw.decode('utf-8'))


@pytest.fixture
def serve():
    """起健康端点的辅助函数（端口 0，用完关掉）。"""
    servers = []

    def _serve(monitor=None, secrets=()):
        server = HealthServer(host='127.0.0.1', port=0, monitor=monitor, secrets=secrets)
        server.start()
        servers.append(server)
        return server

    yield _serve

    for server in servers:
        server.stop()


# ---------------------------------------------------------------------------
# 存活 / 就绪的语义
# ---------------------------------------------------------------------------

def test_healthz_returns_200_even_when_dependency_is_down(serve):
    """依赖挂掉时 ``/healthz`` 仍然是 200 —— 存活只看进程本身。"""
    monitor = ReadinessMonitor((DependencyProbe('kafka', lambda: False),))
    server = serve(monitor=monitor)

    status, body = _get(server, HEALTH_PATH)

    assert status == 200
    assert _json_body(body)['status'] == 'ok'


def test_healthz_does_not_call_dependency_probes(serve):
    """``/healthz`` 不碰依赖探针 —— 探针不该成为故障源。"""
    calls = []

    def _probe():
        calls.append(1)
        raise AssertionError('存活探针不应调用依赖探针')

    server = serve(monitor=ReadinessMonitor((DependencyProbe('tpops', _probe),)))

    status, _ = _get(server, HEALTH_PATH)

    assert status == 200
    assert calls == []


def test_readyz_returns_200_when_all_dependencies_are_ready(serve):
    """依赖全好 → ``/readyz`` 200。"""
    monitor = ReadinessMonitor((
        DependencyProbe('tpops_auth', lambda: True),
        DependencyProbe('kafka', lambda: True),
    ))
    server = serve(monitor=monitor)

    status, body = _get(server, READY_PATH)

    assert status == 200
    payload = _json_body(body)
    assert payload['status'] == 'ok'
    assert payload['check'] == 'readiness'


def test_readyz_returns_503_when_a_dependency_is_not_ready(serve):
    """依赖不可用 → ``/readyz`` 503，并点名是哪个依赖。"""
    monitor = ReadinessMonitor((
        DependencyProbe('tpops_auth', lambda: True),
        DependencyProbe('kafka', lambda: False),
    ))
    server = serve(monitor=monitor)

    status, body = _get(server, READY_PATH)

    assert status == 503
    payload = _json_body(body)
    assert payload['status'] == 'unavailable'
    by_name = {item['name']: item for item in payload['dependencies']}
    assert by_name['kafka']['ready'] is False
    assert by_name['tpops_auth']['ready'] is True


def test_readyz_returns_503_when_probe_raises(serve):
    """探针抛异常 → 503，且**不回显异常消息**（消息里可能带凭据）。"""
    def _boom():
        raise RuntimeError(f'连接 tpops.example 失败，口令 {PASSWORD}')

    server = serve(monitor=ReadinessMonitor((DependencyProbe('tpops_auth', _boom),)))

    status, body = _get(server, READY_PATH)

    assert status == 503
    raw = body.decode('utf-8')
    assert PASSWORD not in raw
    assert 'tpops.example' not in raw


def test_readyz_is_ready_when_no_probe_registered(serve):
    """没注册探针 = 没有可检查的依赖 → 就绪。"""
    server = serve(monitor=ReadinessMonitor())

    status, body = _get(server, READY_PATH)

    assert status == 200
    assert _json_body(body)['dependencies'] == []


def test_unknown_path_returns_404(serve):
    """未注册的路径 404，不泄露任何东西。"""
    server = serve()

    status, body = _get(server, '/metrics')

    assert status == 404
    assert _json_body(body)['status'] == 'not_found'


# ---------------------------------------------------------------------------
# 响应体结构
# ---------------------------------------------------------------------------

def test_readyz_payload_has_expected_fields(serve):
    """响应体字段固定：status / check / timestamp / dependencies。"""
    monitor = ReadinessMonitor((DependencyProbe('tpops_auth', lambda: True),))
    server = serve(monitor=monitor)

    status, body = _get(server, READY_PATH)

    assert status == 200
    payload = _json_body(body)
    assert set(payload) == {'status', 'check', 'timestamp', 'dependencies'}
    assert isinstance(payload['timestamp'], int)
    dependency = payload['dependencies'][0]
    assert set(dependency) == {'name', 'ready', 'reason'}


def test_probe_can_report_a_short_reason(serve):
    """探针可以自带简短原因（不就绪时），进响应体的 reason 字段。"""
    monitor = ReadinessMonitor((
        DependencyProbe('tpops_auth', lambda: '认证失败'),
    ))
    server = serve(monitor=monitor)

    status, body = _get(server, READY_PATH)

    assert status == 503
    assert _json_body(body)['dependencies'][0]['reason'] == '认证失败'


# ---------------------------------------------------------------------------
# 不泄露凭据
# ---------------------------------------------------------------------------

def test_readyz_body_never_contains_credentials(serve):
    """即便探针原因里混进了口令，响应体也必须把它抹掉。"""
    monitor = ReadinessMonitor((
        DependencyProbe('tpops_auth', lambda: f'口令 {PASSWORD} 无效'),
    ))
    server = serve(monitor=monitor, secrets=(PASSWORD,))

    status, body = _get(server, READY_PATH)

    assert status == 503
    raw = body.decode('utf-8')
    assert PASSWORD not in raw
    assert '***' in raw


def test_response_headers_do_not_contain_credentials(serve):
    """响应头里也不能有凭据。"""
    monitor = ReadinessMonitor((
        DependencyProbe('tpops_auth', lambda: f'口令 {PASSWORD} 无效'),
    ))
    server = serve(monitor=monitor, secrets=(PASSWORD,))

    host, port = server.address
    url = f'http://{host}:{port}{READY_PATH}'
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            headers = dict(response.headers)
            status = response.getcode()
    except urllib.error.HTTPError as exc:
        headers = dict(exc.headers)
        status = exc.code

    assert status == 503
    assert PASSWORD not in json.dumps(headers)


def test_body_does_not_leak_platform_or_kafka_addresses(serve):
    """平台地址、Kafka 地址（含其中的用户名口令）都不进响应体。"""
    config = Config(
        tpops_user='tpops-admin',
        tpops_password=PASSWORD,
        tpops_base_url='https://tpops.example:8002',
        kafka_bootstrap_servers=(f'{KAFKA_USER}:{KAFKA_PASSWORD}@10.0.0.9:9092',),
    )
    secrets = secret_values_from_config(config)
    assert PASSWORD in secrets
    assert f'{KAFKA_USER}:{KAFKA_PASSWORD}' in secrets

    server = serve(monitor=ReadinessMonitor((DependencyProbe('tpops_auth', lambda: True),)),
                   secrets=secrets)

    status, body = _get(server, READY_PATH)

    assert status == 200
    raw = body.decode('utf-8')
    for leaked in (PASSWORD, TOKEN, KAFKA_USER, KAFKA_PASSWORD, 'tpops.example', '10.0.0.9'):
        assert leaked not in raw


def test_liveness_body_does_not_include_dependency_details(serve):
    """存活响应里不掺依赖信息 —— 免得泄露依赖地址。"""
    monitor = ReadinessMonitor((
        DependencyProbe('kafka', lambda: f'broker 10.0.0.9:{KAFKA_PASSWORD}'),
    ))
    server = serve(monitor=monitor, secrets=(KAFKA_PASSWORD,))

    status, body = _get(server, HEALTH_PATH)

    assert status == 200
    raw = body.decode('utf-8')
    assert '10.0.0.9' not in raw
    assert 'kafka' not in raw


# ---------------------------------------------------------------------------
# 探针接口本身
# ---------------------------------------------------------------------------

def test_monitor_snapshot_lists_every_probe():
    """``snapshot()`` 逐个探针出一条状态，顺序稳定。"""
    monitor = ReadinessMonitor((
        DependencyProbe('a', lambda: True),
        DependencyProbe('b', lambda: False),
        DependencyProbe('c', lambda: True),
    ))

    snapshot = monitor.snapshot()

    assert snapshot.is_ready is False
    assert [item.name for item in snapshot.dependencies] == ['a', 'b', 'c']
    assert [item.is_ready for item in snapshot.dependencies] == [True, False, True]


def test_monitor_treats_raising_probe_as_not_ready():
    """探针异常按不就绪处理，不向上抛。"""
    def _boom():
        raise RuntimeError('boom')

    snapshot = ReadinessMonitor((DependencyProbe('x', _boom),)).snapshot()

    assert snapshot.is_ready is False
    assert snapshot.dependencies[0].is_ready is False


def test_monitor_accepts_dependency_status_object():
    """探针可以直接返回 ``DependencyStatus``（自定义原因）。"""
    status = DependencyStatus(name='x', is_ready=False, reason='自定义')

    snapshot = ReadinessMonitor((DependencyProbe('x', lambda: status),)).snapshot()

    assert snapshot.dependencies[0].reason == '自定义'


def test_endpoints_can_be_used_without_a_server():
    """``HealthEndpoints`` 可脱离 HTTP 直接用（便于 3.3 装配与自检）。"""
    endpoints = HealthEndpoints(monitor=ReadinessMonitor((DependencyProbe('a', lambda: True),)))

    status, body = endpoints.readiness()

    assert status == 200
    assert _json_body(body)['status'] == 'ok'


def test_port_in_use_error_tells_how_to_find_the_leftover_process():
    """端口被占用时，报错要直接告诉运维**怎么找到那个占着的进程**。

    现场最常见的两种原因：优雅退出还没走完（最多 30 秒）就重启、
    或者 Ctrl-Z 挂起（SIGTSTP）的进程还活着。运维不该靠猜。
    """
    import socket

    from tpops_kafka_collector.health import HealthServerError

    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    sock.listen(1)
    port = sock.getsockname()[1]
    try:
        server = HealthServer(host='127.0.0.1', port=port,
                              monitor=None, secrets=())
        with pytest.raises(HealthServerError) as excinfo:
            server.start()
        message = str(excinfo.value)
        assert 'Address already in use' in message
        assert 'ss -tlunp' in message, '要告诉运维怎么找出占端口的 pid'
        assert 'kill' in message
    finally:
        sock.close()
