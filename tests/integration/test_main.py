"""入口装配与信号处理的集成测试（任务 3.3）。

三条**硬性场景**（简报与控制器补充说明点名的）：

1. **启动期配置缺失** → 非 0 退出码 + 日志指明变量名 + **未发任何请求**；
2. **主循环中收到 SIGTERM** → 退出码 0 + 水位文件完整 + producer 已 flush；
3. **在途请求超过 30 秒** → **放弃等待**，仍以退出码 0 退出（**不挂死**）。

外加两条控制器补充的守卫：

4. **停机后不再发起新的 HTTP 请求**（执行器兜底）；
5. **告警消息进 ``outcome.alarms``**，不进 ``outcome.messages``。
"""
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time

import pytest

from tpops_kafka_collector import main as entry
from tpops_kafka_collector.config import load_config
from tpops_kafka_collector.executors import build_executors
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.scheduler_types import InstanceTarget, TaskOutcome
from tpops_kafka_collector.watermark import WatermarkStore

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STUB_DIR = os.path.join(REPO_ROOT, 'tests', 'fixtures', 'graceful_shutdown_stubs')


# ---------------------------------------------------------------------------
# 假依赖
# ---------------------------------------------------------------------------

class FakeClient:
    """只实现 ``get`` / ``post`` 的假 TPOPS 客户端（记录调用次数）。"""

    def __init__(self, instances=(), nodes=None):
        self.instances = list(instances)
        self.nodes = dict(nodes or {})
        self.get_calls = []
        self.post_calls = []

    def get(self, path, params=None):
        self.get_calls.append(path)
        if 'key-view-execute-node' in path:
            instance_id = path.split('/instances/')[-1].split('/')[0]
            return {'nodes': list(self.nodes.get(instance_id, ()))}
        if path.endswith('/instances'):
            return {'instances': list(self.instances), 'total_count': len(self.instances)}
        return {}

    def post(self, path, payload=None):
        self.post_calls.append(path)
        return {}

    @property
    def call_count(self):
        return len(self.get_calls) + len(self.post_calls)


class FakeProducer:
    """假 Kafka 生产者（记录 flush/close）。"""

    def __init__(self):
        self.closed = False
        self.metrics = []
        self.alarms = []

    def produce_metrics(self, records):
        self.metrics.append(list(records))

    def produce_alarms(self, records):
        self.alarms.append(list(records))

    def close(self):
        self.closed = True


class ScriptedScheduler:
    """按脚本跑的假调度器。"""

    def __init__(self, on_tick=None, tick=None):
        self.on_tick = on_tick
        self.tick_impl = tick
        self.ticks = 0

    def tick(self):
        self.ticks += 1
        if self.on_tick is not None:
            self.on_tick()
        if self.tick_impl is not None:
            return self.tick_impl()
        return None


def _instance_row(instance_id='inst-1', name='gauss-drill'):
    return {
        'id': instance_id,
        'name': name,
        'mode': 'Combined',
        'engine_name': 'gaussdbv5',
        'engine_version': 'V2.0-10.0.0',
    }


def _node_rows():
    return [{
        'node_id': 'n1', 'role': 'master', 'component_id': 'cn1',
        'host_ip': '10.0.0.1', 'type': 'CN',
    }]


def _env(tmp_path, **overrides):
    env = {
        'TPOPS_USER': 'drill-user',
        'TPOPS_PASSWORD': 'drill-pass',
        'KAFKA_BOOTSTRAP_SERVERS': '127.0.0.1:19092',
        'STATE_DIR': str(tmp_path),
        'HEALTH_HOST': '127.0.0.1',
        'HEALTH_PORT': '18099',
        'POLL_INTERVAL_SECONDS': '1',
    }
    env.update(overrides)
    return env


def _config(tmp_path, **overrides):
    return load_config(_env(tmp_path, **overrides))


def _runtime(tmp_path, scheduler, producer=None, watermark=None, config=None):
    """手工拼一个 Runtime（不装配真实外部依赖）。"""
    store = watermark if watermark is not None else WatermarkStore(
        os.path.join(str(tmp_path), 'watermark.json'),
    )
    return entry.Runtime(
        config=config if config is not None else _config(tmp_path),
        mapping=load_mapping(),
        client=None,
        profile=None,
        producer=producer if producer is not None else FakeProducer(),
        watermark=store,
        scheduler=scheduler,
        health=None,
        instances=(),
        deltas=None,
        shutdown=None,
    )


# ---------------------------------------------------------------------------
# 场景一：启动期配置缺失
# ---------------------------------------------------------------------------

def test_missing_config_exits_non_zero_and_names_the_variable(tmp_path, caplog):
    """缺少必填配置 → 非 0 退出码 + 日志指明变量名。"""
    env = _env(tmp_path)
    env.pop('KAFKA_BOOTSTRAP_SERVERS')

    with caplog.at_level('ERROR'):
        code = entry.main(env=env)

    assert code != 0
    messages = [record.getMessage() for record in caplog.records]
    assert any('KAFKA_BOOTSTRAP_SERVERS' in message for message in messages), messages


def test_missing_config_sends_no_request_to_tpops_or_kafka(tmp_path, monkeypatch):
    """配置缺失 → **未向 TPOPS/Kafka 发起任何请求**（用 mock 断言调用次数 == 0）。"""
    env = _env(tmp_path)
    env.pop('KAFKA_BOOTSTRAP_SERVERS')

    class _NeverClient:
        instances = []

        def __init__(self, *args, **kwargs):
            _NeverClient.instances.append(self)

        @classmethod
        def from_config(cls, config, **kwargs):
            return cls()

        def get(self, path, params=None):
            raise AssertionError('配置缺失时不该发任何 TPOPS 请求：%s' % path)

        def post(self, path, payload=None):
            raise AssertionError('配置缺失时不该发任何 TPOPS 请求：%s' % path)

    class _NeverProducer:
        instances = []

        def __init__(self, *args, **kwargs):
            _NeverProducer.instances.append(self)

        @classmethod
        def from_config(cls, config, **kwargs):
            return cls()

        def produce_metrics(self, records):
            raise AssertionError('配置缺失时不该投递 Kafka')

        def produce_alarms(self, records):
            raise AssertionError('配置缺失时不该投递 Kafka')

    monkeypatch.setattr(entry, 'TpopsClient', _NeverClient)
    monkeypatch.setattr(entry, 'MessageProducer', _NeverProducer)

    code = entry.main(env=env)

    assert code != 0
    assert _NeverClient.instances == []
    assert _NeverProducer.instances == []


def test_config_error_exit_code_is_stable(tmp_path):
    """配置错误的退出码是固定的非 0 值。"""
    env = _env(tmp_path)
    env.pop('TPOPS_USER')
    code = entry.main(env=env)
    assert code == entry.EXIT_CONFIG_ERROR
    assert code != 0


# ---------------------------------------------------------------------------
# 场景二：SIGTERM → 优雅退出
# ---------------------------------------------------------------------------

def test_sigterm_during_main_loop_exits_zero_with_complete_watermark(tmp_path):
    """主循环中收到停机信号 → 退出码 0 + 水位文件完整 + producer 已 flush。"""
    shutdown = threading.Event()
    store = WatermarkStore(os.path.join(str(tmp_path), 'watermark.json'))
    store.advance_metric('inst-1:monitor', 12345)
    producer = FakeProducer()
    scheduler = ScriptedScheduler(on_tick=lambda: shutdown.set())

    runtime = _runtime(tmp_path, scheduler, producer=producer, watermark=store)
    code = entry.run(runtime, shutdown=shutdown, deadline_seconds=5)

    assert code == 0
    assert producer.closed, 'producer 必须被 close（flush）'

    reloaded = WatermarkStore(os.path.join(str(tmp_path), 'watermark.json'))
    reloaded.load()
    assert reloaded.metric_watermark('inst-1:monitor') == 12345


def test_shutdown_stops_issuing_new_ticks(tmp_path):
    """停机后不再发起新的采集轮次。"""
    shutdown = threading.Event()
    scheduler = ScriptedScheduler(on_tick=lambda: shutdown.set())
    runtime = _runtime(tmp_path, scheduler)

    entry.run(runtime, shutdown=shutdown, deadline_seconds=5)

    assert scheduler.ticks == 1, scheduler.ticks


def test_no_new_http_request_after_shutdown(tmp_path):
    """置位 shutdown 后调用执行器 → TPOPS 一次都没被调用，且结果是空的。"""
    client = FakeClient([_instance_row()], {'inst-1': _node_rows()})
    shutdown = threading.Event()
    shutdown.set()

    executors = build_executors(
        client=client,
        mapping=load_mapping(),
        config=_config(tmp_path),
        command_center=None,
        node_registry=None,
        deltas=None,
        watermark=WatermarkStore(os.path.join(str(tmp_path), 'watermark.json')),
        shutdown=shutdown,
    )

    for channel, executor in executors.items():
        outcome = executor(InstanceTarget('inst-1', 'gauss-drill'))
        assert isinstance(outcome, TaskOutcome), channel
        assert outcome.messages == (), channel
        assert outcome.alarms == (), channel

    assert client.call_count == 0, client.get_calls + client.post_calls


# ---------------------------------------------------------------------------
# 场景三：在途请求超过 30 秒 → 放弃等待
# ---------------------------------------------------------------------------

def test_in_flight_request_beyond_deadline_gives_up_and_exits_zero(tmp_path):
    """在途请求超过截止时间 → 放弃等待，仍以退出码 0 退出，**不挂死**。

    用**可控的阻塞桩 + 可注入的截止时间**（不真等 30 秒）。
    """
    shutdown = threading.Event()
    started = threading.Event()
    release = threading.Event()

    def _blocking_tick():
        started.set()
        release.wait(30)
        return None

    scheduler = ScriptedScheduler(tick=_blocking_tick)
    producer = FakeProducer()
    store = WatermarkStore(os.path.join(str(tmp_path), 'watermark.json'))
    store.advance_metric('inst-1:monitor', 999)
    runtime = _runtime(tmp_path, scheduler, producer=producer, watermark=store)

    result = {}

    def _run():
        result['code'] = entry.run(
            runtime, shutdown=shutdown, deadline_seconds=0.3,
        )

    worker = threading.Thread(target=_run)
    worker.start()
    assert started.wait(5), '本轮采集没有开始'
    shutdown.set()
    worker.join(timeout=10)

    assert not worker.is_alive(), '放弃等待后不该挂死'
    assert result.get('code') == 0
    assert producer.closed

    reloaded = WatermarkStore(os.path.join(str(tmp_path), 'watermark.json'))
    reloaded.load()
    assert reloaded.metric_watermark('inst-1:monitor') == 999


def test_default_shutdown_deadline_is_thirty_seconds():
    """默认的放弃等待上限是 30 秒。"""
    assert entry.SHUTDOWN_DEADLINE_SECONDS == 30


# ---------------------------------------------------------------------------
# 告警通道的出口
# ---------------------------------------------------------------------------

def test_alarm_messages_go_to_outcome_alarms_not_messages(tmp_path, monkeypatch):
    """告警消息必须放进 ``outcome.alarms``（不是 ``outcome.messages``）。"""
    from tpops_kafka_collector.collectors.alarm import AlarmCollectionResult, AlarmMessage

    message = AlarmMessage(
        message={'alarmId': 'a-1', 'alarmTime': 1700000000000},
        idempotency_key='key-1',
        alarm_id='a-1',
    )

    def _fake_collect_alarms(*args, **kwargs):
        return AlarmCollectionResult(
            messages=(message,), convergence_count=1, detail_count=2,
        )

    monkeypatch.setattr(
        'tpops_kafka_collector.executors.collect_alarms', _fake_collect_alarms,
    )

    executors = build_executors(
        client=FakeClient(),
        mapping=load_mapping(),
        config=_config(tmp_path),
        command_center=None,
        node_registry=None,
        deltas=None,
        watermark=WatermarkStore(os.path.join(str(tmp_path), 'watermark.json')),
        shutdown=threading.Event(),
    )

    outcome = executors['alarm'](InstanceTarget('inst-1', 'gauss-drill'))
    assert outcome.messages == ()
    assert list(outcome.alarms) == [message]
    assert outcome.fetched == 3


# ---------------------------------------------------------------------------
# 装配层：可注入外部依赖
# ---------------------------------------------------------------------------

def test_build_runtime_accepts_injected_client_and_producer(tmp_path):
    """装配层能注入假的 TPOPS 客户端与假的 producer（3.5b 的演练要用）。"""
    client = FakeClient([_instance_row()], {'inst-1': _node_rows()})
    producer = FakeProducer()

    runtime = entry.build_runtime(
        _config(tmp_path), client=client, producer=producer, start_health=False,
    )

    assert runtime.client is client
    assert runtime.producer is producer
    assert len(runtime.instances) == 1
    assert runtime.instances[0].instance_id == 'inst-1'
    assert runtime.scheduler is not None
    assert os.path.exists(os.path.join(str(tmp_path), 'platform_profile.json'))


def test_build_runtime_shares_one_delta_store_between_collector_and_scheduler(tmp_path):
    """差分基线存储必须是**同一个对象**（否则重启后速率算不出来）。"""
    client = FakeClient([_instance_row()], {'inst-1': _node_rows()})
    runtime = entry.build_runtime(
        _config(tmp_path), client=client, producer=FakeProducer(), start_health=False,
    )
    assert runtime.deltas is not None
    assert runtime.scheduler._delta_store is runtime.deltas


def test_health_probes_are_wired_and_lightweight(tmp_path):
    """``/readyz`` 装两个探针（tpops / scheduler），**探针里不发网络请求**。"""
    client = FakeClient([_instance_row()], {'inst-1': _node_rows()})
    runtime = entry.build_runtime(
        _config(tmp_path), client=client, producer=FakeProducer(), start_health=False,
    )
    probes = runtime.health.endpoints.monitor.probes
    assert [probe.name for probe in probes] == ['tpops', 'scheduler']

    before = client.call_count
    states = [probe.check() for probe in probes]
    assert client.call_count == before, '探针里不许发 HTTP 请求'
    # 还没跑过一轮 → scheduler 探针不就绪；tpops 探针读认证标志位。
    assert states[1] != True, states[1]


def test_readiness_returns_503_before_first_tick(tmp_path):
    """没跑过一轮时 ``/readyz`` 返回 503。"""
    client = FakeClient([_instance_row()], {'inst-1': _node_rows()})
    runtime = entry.build_runtime(
        _config(tmp_path), client=client, producer=FakeProducer(), start_health=False,
    )
    status, body = runtime.health.endpoints.readiness()
    assert status == 503
    assert b'"name": "scheduler"' in body or b'"name":"scheduler"' in body


def test_liveness_always_returns_200(tmp_path):
    """``/healthz`` 只看进程能不能应答（不看依赖）。"""
    client = FakeClient([_instance_row()], {'inst-1': _node_rows()})
    runtime = entry.build_runtime(
        _config(tmp_path), client=client, producer=FakeProducer(), start_health=False,
    )
    status, _body = runtime.health.endpoints.liveness()
    assert status == 200


# ---------------------------------------------------------------------------
# 手工演练的等价物：真起一个进程，SIGTERM，退出码 0
# ---------------------------------------------------------------------------

def _free_port():
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.mark.skipif(
    not os.path.isdir(STUB_DIR), reason='演练桩目录缺失'
)
def test_real_process_receives_sigterm_and_exits_zero(tmp_path):
    """真起进程 → SIGTERM → 退出码 0 + 水位文件完整（**不连任何外部服务**）。

    桩通过 ``PYTHONPATH`` 里的 ``sitecustomize`` 注入：HTTP 传输层与
    Kafka 生产者都被替换掉，因此 ``python -m tpops_kafka_collector.main``
    走的是**真实的装配与信号处理代码路径**。
    """
    state_dir = tmp_path / 'state'
    state_dir.mkdir()
    env = dict(os.environ)
    env.update({
        'PYTHONPATH': os.pathsep.join([STUB_DIR, REPO_ROOT]),
        'TPOPS_USER': 'drill-user',
        'TPOPS_PASSWORD': 'drill-pass',
        'TPOPS_BASE_URL': 'https://127.0.0.1:18002',
        'KAFKA_BOOTSTRAP_SERVERS': '127.0.0.1:19092',
        'STATE_DIR': str(state_dir),
        'HEALTH_HOST': '127.0.0.1',
        'HEALTH_PORT': str(_free_port()),
        'POLL_INTERVAL_SECONDS': '1',
        'LOG_LEVEL': 'INFO',
    })
    proc = subprocess.Popen(
        [sys.executable, '-m', 'tpops_kafka_collector.main'],
        cwd=str(tmp_path), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        deadline = time.time() + 20
        watermark_file = os.path.join(str(state_dir), 'watermark.json')
        while time.time() < deadline:
            if proc.poll() is not None:
                break
            if os.path.exists(os.path.join(str(state_dir), 'platform_profile.json')):
                break
            time.sleep(0.2)
        assert proc.poll() is None, '进程提前退出了'
        proc.send_signal(signal.SIGTERM)
        out, err = proc.communicate(timeout=30)
    finally:
        if proc.poll() is None:  # pragma: no cover —— 兜底
            proc.kill()
            proc.communicate()

    assert proc.returncode == 0, (
        '退出码不是 0\nstdout=%s\nstderr=%s' % (out.decode(), err.decode())
    )
    assert os.path.exists(watermark_file), os.listdir(str(state_dir))
    store = WatermarkStore(watermark_file)
    store.load()  # 能读回来 = 文件完整
    payload = json.load(open(watermark_file, encoding='utf-8'))
    assert payload['version'] == 1
