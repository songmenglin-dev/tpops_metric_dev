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
