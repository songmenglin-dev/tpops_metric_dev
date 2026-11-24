"""入口装配与信号处理的集成测试（任务 3.3）。

三条**硬性场景**（简报与控制器补充说明点名的）：

1. **启动期配置缺失** → 非 0 退出码 + 日志指明变量名 + **未发任何请求**；
2. **主循环中收到 SIGTERM** → 退出码 0 + 水位文件完整 + producer 已 flush；
3. **在途请求超过 30 秒** → **放弃等待**，仍以退出码 0 退出（**不挂死**）。

外加两条控制器补充的守卫：

4. **停机后不再发起新的 HTTP 请求**（执行器兜底）；
5. **告警消息进 ``outcome.alarms``**，不进 ``outcome.messages``。

修复轮 R-A 追加的守卫：

6. **节点清单取不到** → 该实例不参与采集；全部取不到 → **启动失败**；
7. 三个「保命」的异常分支（采集异常不带走进程 / 装配失败退 3 /
   「跑了但全挂」时的就绪探针）。
"""
import json
import logging
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
from tpops_kafka_collector.errors import TransientError
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

#: 演练驱动：**真的调度器 + 真的线程池**，某个执行器**挂死**（模拟一个
#: 永远不返回的 HTTP 调用）。
#:
#: 退出路径与生产入口一致：``entry.run(runtime)`` → ``entry.exit_process(code)``。
DRIVER_SOURCE = '''
"""演练驱动 —— 真调度器 + 真线程池 + 挂死的执行器。"""
import os
import sys
import threading
import time

sys.path.insert(0, os.environ['REPO_ROOT'])

from tpops_kafka_collector import main as entry
from tpops_kafka_collector.schedule import ChannelPlan
from tpops_kafka_collector.scheduler import Scheduler
from tpops_kafka_collector.scheduler_types import InstanceTarget, TaskOutcome
from tpops_kafka_collector.watermark import WatermarkStore

STATE = os.environ['DRILL_STATE']
MARKER = os.path.join(STATE, 'hang-started')
WATERMARK = os.path.join(STATE, 'watermark.json')

#: 水位里预置的指标水位（用来断言「水位文件完整可读回」）。
SEED_END_TIME_MS = 1700000000000


class HangingExecutor(object):
    """挂死的执行器 —— 模拟一个永远不返回的 HTTP 调用。"""

    def __call__(self, instance):
        with open(MARKER, 'w') as handle:
            handle.write('started')
        time.sleep(3600)
        return TaskOutcome()


class Producer(object):
    """假投递器（一条都没投出去 —— 这一轮卡在采集阶段）。"""

    def produce_metrics(self, records):
        return None

    def produce_alarms(self, records):
        return None

    def close(self):
        pass


class Config(object):
    poll_interval_seconds = 30


def main():
    stop = threading.Event()
    store = WatermarkStore(WATERMARK)
    store.advance_metric('inst-1:monitor', SEED_END_TIME_MS)
    scheduler = Scheduler(
        plans=(ChannelPlan(channel='monitor', interval_seconds=30,
                            tier_seconds=30, metric_count=1),),
        executors={'monitor': HangingExecutor()},
        producer=Producer(),
        watermark=store,
        instances=[InstanceTarget('inst-1', 'gauss-drill')],
        shutdown_event=stop,
    )
    runtime = entry.Runtime(
        config=Config(), mapping=None, client=None, profile=None,
        producer=Producer(), watermark=store, scheduler=scheduler,
        health=None, instances=(), deltas=None, shutdown=stop,
    )
    return entry.run(runtime, shutdown=stop)


if __name__ == '__main__':
    entry.exit_process(main())
'''


def test_in_flight_request_beyond_deadline_gives_up_and_exits_zero(tmp_path):
    """在途请求挂死 → **放弃等待**，进程仍以退出码 0 退出（**不挂死**）。

    用**真的调度器 + 真的线程池**：某个执行器挂住（模拟永远不返回的 HTTP）。
    进程在**放弃窗口（30 秒）内**退出 —— 靠 subprocess 的超时断言，
    不靠测试里的假超时蒙混。

    为什么必须真起一个进程：py3.7 的 ``concurrent.futures.thread`` 在
    ``atexit`` 里 ``join()`` **所有**工作线程 —— 进程内跑的话，
    pytest 自己的解释器退出也会被同一个挂死的工作线程拖住。
    """
    state_dir = tmp_path / 'state'
    state_dir.mkdir()
    driver = tmp_path / 'driver.py'
    driver.write_text(DRIVER_SOURCE, encoding='utf-8')

    env = dict(os.environ)
    env.update({
        'PYTHONPATH': REPO_ROOT,
        'REPO_ROOT': REPO_ROOT,
        'DRILL_STATE': str(state_dir),
    })
    proc = subprocess.Popen(
        [sys.executable, str(driver)],
        cwd=str(tmp_path), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    marker = state_dir / 'hang-started'
    out = err = b''
    try:
        deadline = time.time() + 30
        while time.time() < deadline and not marker.exists():
            time.sleep(0.1)
        assert marker.exists(), '在途请求没有开始（驱动没跑起来）'
        proc.send_signal(signal.SIGTERM)
        # 放弃窗口是 30 秒 —— 留 15 秒余量给收尾（落盘 + flush）。
        out, err = proc.communicate(timeout=45)
    except subprocess.TimeoutExpired:  # pragma: no cover —— 这就是「挂死」
        proc.kill()
        proc.communicate()
        raise AssertionError(
            '进程在放弃窗口内没有退出 —— 挂死了（atexit 的 join 把进程拖住了）'
        )
    finally:
        if proc.poll() is None:  # pragma: no cover —— 兜底
            proc.kill()
            proc.communicate()

    assert proc.returncode == 0, (
        '退出码不是 0\nstdout=%s\nstderr=%s'
        % (out.decode(), err.decode())
    )
    watermark = state_dir / 'watermark.json'
    assert watermark.exists(), '退出后没有写出水位文件：%s' % os.listdir(str(state_dir))
    store = WatermarkStore(str(watermark))
    store.load()
    assert store.metric_watermark('inst-1:monitor') == 1700000000000


def test_default_shutdown_deadline_is_thirty_seconds():
    """默认的放弃等待上限是 30 秒。"""
    assert entry.SHUTDOWN_DEADLINE_SECONDS == 30


def test_exit_process_skips_the_atexit_hooks(tmp_path, monkeypatch):
    """``exit_process`` 走 ``os._exit``（**跳过 atexit**），退出码照旧。

    atexit 里挂着 py3.7 的 ``concurrent.futures.thread._python_exit`` ——
    它会 ``join()`` 所有工作线程，有一个在途 HTTP 挂住就会把进程拖死。
    """
    calls = []
    monkeypatch.setattr(entry.os, '_exit', lambda code: calls.append(code))
    entry.exit_process(0)
    assert calls == [0], calls


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


# ---------------------------------------------------------------------------
# 节点清单取不到（A4）
# ---------------------------------------------------------------------------

class _NodeListClient(FakeClient):
    """节点清单接口会对指定实例**抛可重试异常**的假客户端。"""

    def __init__(self, instances=(), nodes=None, failing=()):
        FakeClient.__init__(self, instances, nodes)
        self.failing = set(failing)
        self.node_calls = []

    def get(self, path, params=None):
        if 'key-view-execute-node' in path:
            instance_id = path.split('/instances/')[-1].split('/')[0]
            self.node_calls.append(instance_id)
            if instance_id in self.failing:
                raise TransientError('平台抖了：节点清单取不到')
        return FakeClient.get(self, path, params)


def test_instance_without_a_node_list_is_not_collected(tmp_path, caplog):
    """节点清单取不到 → **该实例不参与采集**（不产出错的 resourceName）。

    节点清单决定 ``resourceName``（``{host_ip}_{node_id}``）——
    一次网络抖动就让整个进程生命周期的资源名退化成「只有 node_id」，
    客户按台账查不到资源 → **数据看着有、但对不上台账**。
    """
    client = _NodeListClient(
        [_instance_row('inst-ok', 'gauss-ok'), _instance_row('inst-bad', 'gauss-bad')],
        {'inst-ok': _node_rows()},
        failing=('inst-bad',),
    )
    with caplog.at_level('ERROR'):
        runtime = entry.build_runtime(
            _config(tmp_path), client=client, producer=FakeProducer(),
            start_health=False, node_retry_sleep=lambda seconds: None,
        )

    collected = [item.instance_id for item in runtime.instances]
    assert collected == ['inst-ok'], (
        '取不到节点清单的实例不该参与采集：%r' % (collected,)
    )
    assert [item.instance_id for item in runtime.scheduler.instances] == ['inst-ok']

    # 取不到节点清单的实例要记 **ERROR + 堆栈**，并指明是哪个实例。
    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert any('inst-bad' in record.getMessage() for record in errors), caplog.text
    assert any(record.exc_info is not None for record in errors), (
        '节点清单失败的日志必须带堆栈'
    )


def test_node_list_is_retried_three_times(tmp_path, caplog):
    """节点清单取不到 → **退避重试 3 次**（不是一次失败就永久降级）。"""
    client = _NodeListClient(
        [_instance_row('inst-ok', 'gauss-ok'), _instance_row('inst-bad', 'gauss-bad')],
        {'inst-ok': _node_rows()},
        failing=('inst-bad',),
    )
    with caplog.at_level('WARNING'):
        entry.build_runtime(
            _config(tmp_path), client=client, producer=FakeProducer(),
            start_health=False, node_retry_sleep=lambda seconds: None,
        )
    # 平台画像发现时也会取一次节点清单（数 master）—— 所以总次数 ≥ 3；
    # 真正要钉死的是**退避重试确实发生了两次**。
    assert client.node_calls.count('inst-bad') >= 3, client.node_calls
    assert '第 1/3 次重试' in caplog.text, caplog.text
    assert '第 2/3 次重试' in caplog.text, caplog.text


def test_startup_fails_when_no_instance_has_a_node_list(tmp_path, monkeypatch, caplog):
    """**所有**实例的节点清单都取不到 → **启动失败**（非 0 退出码）。

    那等于平台不可达 —— 与其投出对不上台账的数据，不如启动失败。
    """
    client = _NodeListClient(
        [_instance_row('inst-1', 'gauss-drill')], {}, failing=('inst-1',),
    )
    producers = []

    class _NeverProducer(object):
        def __init__(self, *args, **kwargs):
            producers.append(self)

        @classmethod
        def from_config(cls, config, **kwargs):
            return cls()

        def produce_metrics(self, records):
            raise AssertionError('启动失败时不该投递 Kafka')

        def produce_alarms(self, records):
            raise AssertionError('启动失败时不该投递 Kafka')

        def close(self):
            pass

    class _StubClient(object):
        @classmethod
        def from_config(cls, config, **kwargs):
            return client

    monkeypatch.setattr(entry, 'TpopsClient', _StubClient)
    monkeypatch.setattr(entry, 'MessageProducer', _NeverProducer)

    with caplog.at_level('ERROR'):
        code = entry.main(env=_env(tmp_path))

    assert code == entry.EXIT_STARTUP_ERROR, code
    assert code != 0
    assert producers == [], '启动失败前不该构造 Kafka 生产者'
    assert '节点清单' in caplog.text, caplog.text


# ---------------------------------------------------------------------------
# 保命分支（B3）
# ---------------------------------------------------------------------------

def test_startup_error_returns_exit_code_three_with_a_stack_trace(tmp_path, monkeypatch, caplog):
    """装配失败 → 退出码 3 + **带堆栈**的日志（不要裸崩）。"""

    def _boom(*args, **kwargs):
        raise RuntimeError('依赖装不起来')

    monkeypatch.setattr(entry, 'build_runtime', _boom)
    with caplog.at_level('ERROR'):
        code = entry.main(env=_env(tmp_path))

    assert code == entry.EXIT_STARTUP_ERROR
    assert code == 3
    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert any('装配失败' in record.getMessage() for record in errors), caplog.text
    assert any(record.exc_info is not None for record in errors), (
        '装配失败的日志必须带堆栈'
    )


def test_tpops_probe_reports_an_authentication_failure():
    """认证失败 → 就绪探针不就绪，并说清原因。"""

    class _AuthFailedClient(object):
        _auth_failed_at = 1700000000.0

    state = entry._tpops_state(_AuthFailedClient())
    assert state is not True, state
    assert '认证失败' in state, state


def test_tpops_probe_reports_not_authenticated_yet():
    """还没认证过 → 就绪探针不就绪，并说清原因。"""

    class _FreshClient(object):
        pass

    state = entry._tpops_state(_FreshClient())
    assert state is not True, state
    assert '认证' in state, state


def test_scheduler_probe_reports_a_stale_last_success():
    """最近一次成功采集太久以前 → 就绪探针不就绪（跑了但全挂）。"""

    class _StaleScheduler(object):
        last_tick_ok_at = time.time() - (entry.SCHEDULER_STALE_SECONDS + 10)

    state = entry._scheduler_state(_StaleScheduler())
    assert state is not True, state
    assert str(entry.SCHEDULER_STALE_SECONDS) in state, state


def test_scheduler_probe_reports_ready_after_a_successful_tick():
    """刚成功跑完一轮 → scheduler 探针**就绪**（成功路径）。"""

    class _FreshScheduler(object):
        last_tick_ok_at = time.time()

    state = entry._scheduler_state(_FreshScheduler())
    assert state is True, state


def test_scheduler_probe_reports_never_successful_yet():
    """一轮都没跑过 → scheduler 探针不就绪，并说清原因。"""

    class _NeverTickedScheduler(object):
        last_tick_ok_at = None

    state = entry._scheduler_state(_NeverTickedScheduler())
    assert state is not True, state
    assert '还没有成功完成过一轮' in state, state


def test_tpops_probe_reports_ready_when_authenticated():
    """认证正常 → tpops 探针**就绪**（成功路径）。"""

    class _AuthedClient(object):
        auth_ok = True

    state = entry._tpops_state(_AuthedClient())
    assert state is True, state


def _readiness_dependencies(body):
    """把就绪响应体解析成 ``{探针名: 该探针的条目}``。"""
    payload = json.loads(body.decode('utf-8'))
    return dict((item['name'], item) for item in payload['dependencies'])


def test_readiness_returns_503_when_the_last_round_failed(tmp_path):
    """跑过一轮但**全挂** → ``/readyz`` 返回 503 —— 且 503 **只能**来自 scheduler 探针。

    **TPOPS 探针必须已认证**：否则 503 来自 tpops 探针（客户端从没认证过），
    跟本用例要守的 scheduler 闸门无关 —— 那样的断言恒绿。
    断言也必须落到**具体探针的状态文本**，不能只查「响应体里有 scheduler 这个名字」
    （每个探针的名字都在响应体里，那条断言什么也证明不了）。
    """
    client = FakeClient([_instance_row()], {'inst-1': _node_rows()})
    client.auth_ok = True
    runtime = entry.build_runtime(
        _config(tmp_path), client=client, producer=FakeProducer(), start_health=False,
    )
    runtime.scheduler._last_tick_ok_at = time.time() - (entry.SCHEDULER_STALE_SECONDS + 10)

    status, body = runtime.health.endpoints.readiness()

    assert status == 503
    deps = _readiness_dependencies(body)
    assert deps['tpops']['ready'] is True, (
        f'TPOPS 探针不就绪 —— 503 的归因就不唯一了：{deps!r}'
    )
    assert deps['scheduler']['ready'] is False, deps
    assert str(entry.SCHEDULER_STALE_SECONDS) in deps['scheduler']['reason'], deps


def test_readiness_returns_200_when_both_probes_are_ready(tmp_path):
    """tpops 与 scheduler **都**就绪 → ``/readyz`` 返回 200（正向对照）。

    正反合起来才锁住归因：只有 503 的用例时，把 scheduler 探针改成恒真
    也能靠 tpops 探针凑出 503 —— 那条守卫守不住 scheduler 这一侧。
    """
    client = FakeClient([_instance_row()], {'inst-1': _node_rows()})
    client.auth_ok = True
    runtime = entry.build_runtime(
        _config(tmp_path), client=client, producer=FakeProducer(), start_health=False,
    )
    runtime.scheduler._last_tick_ok_at = time.time()

    status, body = runtime.health.endpoints.readiness()

    assert status == 200
    payload = json.loads(body.decode('utf-8'))
    assert payload['status'] == 'ok', payload
    deps = _readiness_dependencies(body)
    assert deps['tpops']['ready'] is True, deps
    assert deps['scheduler']['ready'] is True, deps
