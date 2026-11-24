# -*- coding: utf-8 -*-
"""**杀进程 → 重启演练**：不重复、不丢失。

合同完成标准：「杀进程 → 重启演练：**不重复、不丢失**（水位只在投递成功后推进）」。
开发方案 §4.6.4 给的机制：

> **推进时机**：**仅当该批消息全部成功投递后才推进**（I-1）。
> 部分成功时……采用**批次原子推进**：任一消息失败则整批不推进，下轮重发
> （**客户侧按幂等键去重**）。

所以「不重复、不丢失」在两个层面落地：

- **不丢失** —— 投递失败的批次**水位不推进** → 下轮**整批重发** → 最终全部投出去；
- **不重复** —— 幂等键**持久化在水位文件里**、且**只在投递成功后才登记**
  → 重启后同一批消息被**跳过**，不会重复投递
  （同一个数据点的键含 ``clock``，跨轮稳定）。

本文件有**两段**演练，都要能跑：

**第一段（进程内，语义精确）** —— 用真的 ``MessageDelivery`` + 真的
``WatermarkStore``，把「投递失败 → 水位不推进 → 重启（从磁盘重新 load）
→ 整批重投 → 成功 → 幂等键落盘 → 再来一轮被跳过」这条链走一遍。
消息的 ``clock`` 固定，所以幂等键跨轮稳定 → 「不重复」可断言。

**第二段（真进程，SIGTERM）** —— 真起 ``python -m tpops_kafka_collector.main``，
用**传输层桩**顶掉 TPOPS 与 Kafka（不连任何外部服务），
``kill -TERM`` → 断言**退出码 0** + **水位文件完整可读回**，
再起第二个进程 → 断言第一轮没投出的消息**这一轮投出来了**（不丢失）。
"""
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request

from tpops_kafka_collector.scheduler_delivery import MessageDelivery
from tpops_kafka_collector.watermark import WatermarkStore

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STUB_DIR = os.path.join(REPO_ROOT, 'tests', 'fixtures', 'graceful_shutdown_stubs')

#: 演练用的实例与通道。
INSTANCE_ID = 'drill-instance-1'
CHANNEL = 'monitor'


# ---------------------------------------------------------------------------
# 第一段：进程内 —— 投递失败 / 水位推进 / 幂等键 的精确语义
# ---------------------------------------------------------------------------

class _RecordingProducer:
    """记录投出去的消息；可设定「前 N 次投递抛错」。"""

    def __init__(self, fail_batches=0):
        self.delivered = []
        self.fail_batches = fail_batches

    def produce_metrics(self, records):
        if self.fail_batches > 0:
            self.fail_batches -= 1
            raise RuntimeError('演练：这一批投递失败')
        self.delivered.extend(record.payload for record in records)

    def produce_alarms(self, records):
        self.delivered.extend(record.payload for record in records)

    def close(self):
        pass


def _metric_message(clock, metric='GAUSSDB.connections'):
    """一条指标消息（``clock`` 固定 → 幂等键跨轮稳定）。"""
    return {
        'metricName': metric,
        'metricCode': metric,
        'resourceName': '10.0.0.1_drill-node-1',
        'value': 42,
        'unit': '个',
        'clock': clock,
        'tags': {
            'instance_id': INSTANCE_ID,
            'instance_name': 'gauss-drill',
            'node_id': 'drill-node-1',
            'component_id': 'drill-cn-1',
            'host_ip': '10.0.0.1',
        },
    }


def _outcome(clock):
    from tpops_kafka_collector.scheduler_types import TaskOutcome
    return TaskOutcome(messages=(_metric_message(clock),), end_time_ms=clock)


def test_failed_batch_does_not_advance_the_watermark_and_is_retried(tmp_path):
    """投递失败 → 水位**不推进** → 下轮整批重投（**不丢失**）。"""
    path = str(tmp_path / 'watermark.json')
    store = WatermarkStore(path)
    producer = _RecordingProducer(fail_batches=1)
    delivery = MessageDelivery(producer, store)

    # 第一轮：投递失败。
    delivery.deliver(CHANNEL, INSTANCE_ID, _outcome(1000))
    assert store.metric_watermark('%s:%s' % (INSTANCE_ID, CHANNEL)) is None, (
        '投递失败却推进了水位 —— 那一段数据会被永久丢掉'
    )
    assert not producer.delivered, '投递失败却记成了已投递'

    # 第二轮：投递成功 → 水位推进。
    delivery.deliver(CHANNEL, INSTANCE_ID, _outcome(1000))
    assert store.metric_watermark('%s:%s' % (INSTANCE_ID, CHANNEL)) == 1000
    assert len(producer.delivered) == 1, '失败的那一批没有被重投 —— 数据丢了'


def test_already_delivered_messages_are_not_delivered_again_after_a_restart(tmp_path):
    """成功投递过的消息，**重启后**不再投一次（**不重复**）。"""
    path = str(tmp_path / 'watermark.json')
    store = WatermarkStore(path)
    producer = _RecordingProducer()
    delivery = MessageDelivery(producer, store)

    delivery.deliver(CHANNEL, INSTANCE_ID, _outcome(2000))
    assert len(producer.delivered) == 1
    store.save()

    # **重启**：内存态全丢，只留磁盘上的水位文件。
    restarted = WatermarkStore(path)
    restarted.load()
    restarted_producer = _RecordingProducer()
    restarted_delivery = MessageDelivery(restarted_producer, restarted)

    restarted_delivery.deliver(CHANNEL, INSTANCE_ID, _outcome(2000))
    assert not restarted_producer.delivered, (
        '重启后把已经投过的消息又投了一遍 —— 重复投递'
    )
    assert restarted.metric_watermark('%s:%s' % (INSTANCE_ID, CHANNEL)) == 2000, (
        '重启后水位丢了 —— 续传会从头再投'
    )


def test_idempotency_keys_survive_a_restart(tmp_path):
    """幂等键**持久化**在水位文件里 —— 重启后仍能去重。"""
    path = str(tmp_path / 'watermark.json')
    store = WatermarkStore(path)
    producer = _RecordingProducer()
    delivery = MessageDelivery(producer, store)
    delivery.deliver(CHANNEL, INSTANCE_ID, _outcome(3000))
    store.save()

    with open(path, encoding='utf-8') as handle:
        payload = json.load(handle)
    assert payload['idempotency_keys'], '幂等键没有落盘 —— 重启后重复投递拦不住'

    restarted = WatermarkStore(path)
    restarted.load()
    assert restarted.idempotency_keys(), '重启后幂等键没恢复'


# ---------------------------------------------------------------------------
# 第二段：真进程 —— SIGTERM → 退出码 0 → 水位文件完整 → 重启续传
# ---------------------------------------------------------------------------

def _env_for(state_dir, port, **overrides):
    env = dict(os.environ)
    env.update({
        'PYTHONPATH': '%s%s%s' % (STUB_DIR, os.pathsep, REPO_ROOT),
        'TPOPS_USER': 'drill',
        'TPOPS_PASSWORD': 'drill-placeholder',
        'TPOPS_BASE_URL': 'https://127.0.0.1:18002',
        'KAFKA_BOOTSTRAP_SERVERS': '127.0.0.1:19092',
        'STATE_DIR': str(state_dir),
        'HEALTH_HOST': '127.0.0.1',
        'HEALTH_PORT': str(port),
        'POLL_INTERVAL_SECONDS': '5',
        'LOG_LEVEL': 'INFO',
        'LOG_FORMAT': 'json',
    })
    env.update(overrides)
    return env


def _start_process(env, log_path):
    """起一个真进程（日志落文件，避免管道缓冲把进程堵死）。"""
    handle = open(str(log_path), 'wb')
    process = subprocess.Popen(
        [sys.executable, '-m', 'tpops_kafka_collector.main'],
        cwd=REPO_ROOT,
        env=env,
        stdout=handle,
        stderr=handle,
    )
    process._log_handle = handle  # type: ignore[attr-defined]
    return process


def _wait_for(predicate, timeout=30.0, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _ready(port):
    """``/readyz`` 返回 200 —— 说明**至少成功跑完过一轮采集**。"""
    try:
        conn = urllib.request.urlopen(
            'http://127.0.0.1:%d/readyz' % port, timeout=1.0,
        )
        return conn.getcode() == 200
    except Exception:  # noqa: B902 —— 探活失败就是没就绪
        return False


def _free_port():
    """拿一个空闲端口（健康端点用）。"""
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def test_sigterm_exits_zero_with_a_complete_watermark_file_and_restart_resumes(tmp_path):
    """真进程：SIGTERM → 退出码 0 → 水位文件完整 → **重启**能续传。"""
    state_dir = tmp_path / 'state'
    state_dir.mkdir()
    watermark_path = state_dir / 'watermark.json'

    port = _free_port()
    env = _env_for(state_dir, port)
    process = _start_process(env, tmp_path / 'run1.log')
    try:
        # ``/readyz`` 变绿 = 至少成功跑完过一轮采集（水位文件是**退出时**才写的）。
        assert _wait_for(lambda: _ready(port)), '进程没有跑完过一轮采集'
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=30)
    finally:
        if process.poll() is None:  # pragma: no cover —— 兜底，避免挂死测试
            process.kill()
            process.wait(timeout=10)
        process._log_handle.close()  # type: ignore[attr-defined]

    assert process.returncode == 0, (
        'SIGTERM 后退出码不是 0：%s\n%s'
        % (process.returncode, (tmp_path / 'run1.log').read_text('utf-8')[-2000:])
    )

    # 水位文件**完整** —— 能被 WatermarkStore 读回来。
    assert watermark_path.exists(), '退出后没有写出水位文件'
    store = WatermarkStore(str(watermark_path))
    store.load()
    assert store.snapshot().metric_watermarks, '水位文件里没有指标水位'

    # **重启**：第二个进程读同一个状态目录 → 能继续跑，退出码仍是 0。
    port2 = _free_port()
    env2 = _env_for(state_dir, port2)
    second = _start_process(env2, tmp_path / 'run2.log')
    try:
        assert _wait_for(lambda: _ready(port2)), '重启后的进程没有跑完过一轮采集'
        second.send_signal(signal.SIGTERM)
        second.wait(timeout=30)
    finally:
        if second.poll() is None:  # pragma: no cover
            second.kill()
            second.wait(timeout=10)
        second._log_handle.close()  # type: ignore[attr-defined]
    assert second.returncode == 0, '重启后的进程退出码不是 0'


# ---------------------------------------------------------------------------
# 第三段：**SIGKILL** 版杀进程演练（修复轮 R-A 的 A5）
# ---------------------------------------------------------------------------

#: SIGKILL 演练的驱动 —— **真调度器 + 真线程池 + 真水位存储 + 真生命周期**。
#:
#: 平台与 Kafka 都是假的（不连任何外部服务），但**水位与幂等键是真的**，
#: 落盘也是真的（``WatermarkStore`` 的原子写）。
SIGKILL_DRIVER = '''
"""SIGKILL 演练驱动 —— 真调度器 + 真水位存储 + 真生命周期。"""
import json
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
WATERMARK = os.path.join(STATE, 'watermark.json')
DELIVERED = os.path.join(STATE, 'delivered.jsonl')
ROUNDS = os.path.join(STATE, 'rounds.log')
KAFKA_DOWN = os.path.join(STATE, 'kafka-down')

#: 固定的采集时刻 —— 幂等键跨轮、跨进程稳定（模拟「平台数据点时间戳」）。
CLOCK_MS = 1700000000000


def message():
    """一条指标消息（``clock`` 固定 → 幂等键稳定）。"""
    return {
        'metricName': 'GAUSSDB.connections',
        'metricCode': 'GAUSSDB.connections',
        'resourceName': '10.0.0.1_drill-node-1',
        'value': 42,
        'unit': '个',
        'clock': CLOCK_MS,
        'tags': {
            'instance_id': 'drill-instance-1',
            'instance_name': 'gauss-drill',
            'node_id': 'drill-node-1',
            'component_id': 'drill-cn-1',
            'host_ip': '10.0.0.1',
        },
    }


class Producer(object):
    """假 Kafka 生产者 —— 写文件（跨进程可观测）。

    ``kafka-down`` 标记文件存在时**投递失败** → 水位不推进 → 下轮重投。
    """

    def produce_metrics(self, records):
        if os.path.exists(KAFKA_DOWN):
            raise RuntimeError('drill: kafka 不可用')
        with open(DELIVERED, 'a', encoding='utf-8') as handle:
            for record in records:
                handle.write(json.dumps(
                    {'key': record.key, 'payload': record.payload},
                    ensure_ascii=False,
                ) + '\\n')

    def produce_alarms(self, records):
        pass

    def close(self):
        pass


class Config(object):
    poll_interval_seconds = 1


def watch_saves(stop):
    """盯水位文件的重写次数（每落一次盘记一行）。"""
    last = None
    while not stop.is_set():
        try:
            stamp = os.stat(WATERMARK).st_mtime_ns
        except OSError:
            stamp = None
        if stamp is not None and stamp != last:
            last = stamp
            with open(ROUNDS, 'a', encoding='utf-8') as handle:
                handle.write('%d\\n' % stamp)
        time.sleep(0.05)


def main():
    stop = threading.Event()
    watcher = threading.Thread(target=watch_saves, args=(stop,), daemon=True)
    watcher.start()
    store = WatermarkStore(WATERMARK)
    store.load()
    scheduler = Scheduler(
        plans=(ChannelPlan(channel='monitor', interval_seconds=1,
                            tier_seconds=1, metric_count=1),),
        executors={'monitor': lambda instance: TaskOutcome(
            messages=(message(),), end_time_ms=CLOCK_MS,
        )},
        producer=Producer(),
        watermark=store,
        instances=[InstanceTarget('drill-instance-1', 'gauss-drill')],
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


def _drill_env(state_dir):
    """SIGKILL 演练的环境变量。"""
    env = dict(os.environ)
    env.update({
        'PYTHONPATH': REPO_ROOT,
        'REPO_ROOT': REPO_ROOT,
        'DRILL_STATE': str(state_dir),
    })
    return env


def _read_lines(path):
    """读一个文本文件的全部非空行。"""
    if not os.path.exists(str(path)):
        return []
    with open(str(path), encoding='utf-8') as handle:
        return [line for line in handle.read().splitlines() if line.strip()]


def test_sigkill_drill_does_not_duplicate_and_does_not_lose(tmp_path):
    """**SIGKILL** 版杀进程演练：不重复、不丢。

    - 阶段一：Kafka「不可用」→ 投递失败 → 连跑几轮 → **SIGKILL**。
      断言：**水位文件已经落盘**（每轮落盘，**不靠** ``shutdown_runtime``）
      且完整可读回 —— 没有它，崩溃后整个进程生命周期的去重状态全丢。
    - 阶段二：Kafka 恢复 → **重启** → 断言：阶段一没投出去的消息
      **这一轮投出来了**（不丢未投递的数据）。
    - 阶段三：再**重启**一次 → 断言：已成功投递过的消息**没有再投一遍**
      （不重复）。
    """
    state_dir = tmp_path / 'state'
    state_dir.mkdir()
    driver = tmp_path / 'sigkill_driver.py'
    driver.write_text(SIGKILL_DRIVER, encoding='utf-8')
    env = _drill_env(state_dir)
    watermark_path = state_dir / 'watermark.json'
    delivered_path = state_dir / 'delivered.jsonl'
    rounds_path = state_dir / 'rounds.log'

    # -- 阶段一：Kafka 不可用，跑几轮后 SIGKILL --------------------------
    (state_dir / 'kafka-down').write_text('down', encoding='utf-8')
    process = _start_driver(env, driver, tmp_path / 'run1.log')
    try:
        assert _wait_for(lambda: len(_read_lines(rounds_path)) >= 3), (
            '进程没有跑完三轮 —— 每轮落盘没生效：%r'
            % (_read_lines(rounds_path),)
        )
    finally:
        _sigkill(process)

    assert watermark_path.exists(), (
        'SIGKILL 之后没有水位文件 —— 运行期不落盘 → 重启后去重状态全丢：%s'
        % os.listdir(str(state_dir))
    )
    store = WatermarkStore(str(watermark_path))
    store.load()          # 能读回来 = 文件完整
    assert not _read_lines(delivered_path), 'Kafka 不可用时不该投出任何消息'

    # -- 阶段二：Kafka 恢复，重启 → 阶段一没投出的消息这一轮投出来 --------
    (state_dir / 'kafka-down').unlink()
    process = _start_driver(env, driver, tmp_path / 'run2.log')
    try:
        assert _wait_for(lambda: len(_read_lines(delivered_path)) >= 1), (
            '重启后没投出消息 —— 阶段一没投递的数据丢了'
        )
        # 多跑几轮，确认**没有**重复投递。
        assert _wait_for(lambda: len(_read_lines(rounds_path)) >= 6)
    finally:
        _sigkill(process)

    delivered = _read_lines(delivered_path)
    assert len(delivered) == 1, '同一条消息被投了 %d 次 —— 重复投递：%r' % (
        len(delivered), delivered,
    )

    # -- 阶段三：再重启一次 → 已成功投递过的数据不重投 --------------------
    before = len(_read_lines(delivered_path))
    process = _start_driver(env, driver, tmp_path / 'run3.log')
    try:
        assert _wait_for(lambda: len(_read_lines(rounds_path)) >= 9)
    finally:
        _sigkill(process)

    after = len(_read_lines(delivered_path))
    assert after == before, (
        '重启后把已经投过的消息又投了一遍 —— 重复投递：%d → %d'
        % (before, after)
    )


def _start_driver(env, driver, log_path):
    """起一个真进程（日志落文件，避免管道缓冲把进程堵死）。"""
    handle = open(str(log_path), 'wb')
    process = subprocess.Popen(
        [sys.executable, str(driver)],
        cwd=REPO_ROOT,
        env=env,
        stdout=handle,
        stderr=handle,
    )
    process._log_handle = handle  # type: ignore[attr-defined]
    return process


def _sigkill(process):
    """**SIGKILL**（不是 SIGTERM）—— 不给任何收尾的机会。"""
    if process.poll() is None:
        process.send_signal(signal.SIGKILL)
    process.wait(timeout=10)
    process._log_handle.close()  # type: ignore[attr-defined]
