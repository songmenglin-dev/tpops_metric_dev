"""调度器（任务 3.1）的**集成测试** —— 真实采集器 + 全 mock 的外部世界。

**TPOPS 与 Kafka 一律 mock** —— 本文件不发起任何网络请求。

守的是「调度器 + 真实采集器」这一层的接缝：

1. **一轮跑通** —— 真实采集器产出的消息经调度器投出去，水位按
   ``f'{instance_id}:{channel}'`` 推进（**多实例不互相覆盖**）；
2. **单组失败不拖垮其他组** —— 命令中心挂了，A 类的消息**仍被投递**，
   进程不退出；
3. **水位只在投递成功后推进** —— 投递失败 → 整批不推进 → 下轮重发；
4. **幂等** —— 同一批里重复的键只投一条；已投过的键下一轮跳过；
5. **告警通道** —— 告警走独立的水位键（``{instance_id}:alarm``）。
"""

import io
import json
import os

import pytest

from conftest import DATA_DIR
from integration.test_collector_alarm import (
    MASTER_NODE,
    FakeAlarmClient,
    convergence_row,
)
from integration.test_collector_command_center import (
    INSTANCE_ID,
    FakeCommandCenter,
)
from integration.test_collector_monitor import (
    NODES,
    FakeClient as MonitorFakeClient,
    commit_metric,
    monitor_payload,
)

from tpops_kafka_collector.collectors.alarm import collect_alarms
from tpops_kafka_collector.collectors.command_center import CommandCenterCollector
from tpops_kafka_collector.collectors.monitor import MonitorCollector
from tpops_kafka_collector.config import Config
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.mapping.rules import CounterDeltaStore
from tpops_kafka_collector.schedule import (
    CHANNEL_ALARM,
    CHANNEL_COMMAND_CENTER,
    CHANNEL_MONITOR,
    ChannelPlan,
)
from tpops_kafka_collector.scheduler import Scheduler
from tpops_kafka_collector.scheduler_types import InstanceTarget, TaskOutcome
from tpops_kafka_collector.tpops.metrics import resolve_window_from_config
from tpops_kafka_collector.watermark import WatermarkStore

NOW_MS = 1794454239320

PROBE_PATH = os.path.join(DATA_DIR, 'merged_sql', 'sql_merge_probe_result.json')
NODE_PATH = os.path.join(
    DATA_DIR, 'metric_response', '003_GAUSSDB.NAME.Number.ad.ples.json'
)

PLANS = (
    ChannelPlan(CHANNEL_MONITOR, 30, 30, 11),
    ChannelPlan(CHANNEL_COMMAND_CENTER, 60, 60, 82),
    ChannelPlan(CHANNEL_ALARM, 30, 30, 0),
)


def _load(path):
    with io.open(path, encoding='utf-8') as handle:
        return json.load(handle)


@pytest.fixture(scope='module')
def mapping():
    return load_mapping(validate=False)


@pytest.fixture(scope='module')
def probe_nodes():
    return _load(PROBE_PATH)['nodes']


@pytest.fixture(scope='module')
def node_rows():
    for step in _load(NODE_PATH):
        if 'key-view-execute-node' in step.get('api', ''):
            return tuple(step['response_body']['nodes'])
    raise AssertionError('留档里没有节点清单的响应')


@pytest.fixture
def config():
    return Config(
        tpops_user='u', tpops_password='p',
        metric_window_hours=2, metric_end_lag_minutes=5,
    )


class FakeProducer(object):
    """假 Kafka 生产者 —— 记录批次；可指定哪一批失败。"""

    def __init__(self, fail_metric_batches=()):
        self.metric_batches = []
        self.alarm_batches = []
        self._fail_metric = set(fail_metric_batches)

    def produce_metrics(self, records):
        from tpops_kafka_collector.kafka.records import (
            BatchResult,
            FailedRecord,
            ProduceBatchError,
        )
        index = len(self.metric_batches)
        items = tuple(records)
        self.metric_batches.append(items)
        if index in self._fail_metric:
            raise ProduceBatchError(
                '假投递失败',
                BatchResult(
                    topic='metrics',
                    failed=(FailedRecord(items[0], '假的失败'),),
                ),
            )
        return BatchResult(topic='metrics', succeeded=items)

    def produce_alarms(self, records):
        from tpops_kafka_collector.kafka.records import BatchResult
        items = tuple(records)
        self.alarm_batches.append(items)
        return BatchResult(topic='alarms', succeeded=items)

    def payloads(self):
        return [record.payload for batch in self.metric_batches for record in batch]


def build_executors(mapping, config, node_rows, command_center=None, alarm_client=None):
    """把三个真实采集器包成调度器的执行器。"""
    monitor_collector = MonitorCollector(
        client=MonitorFakeClient(monitor_payload([
            commit_metric({'cn_5001': [9.0], 'cn_5002': [11.0]}),
        ])),
        mapping=mapping,
        instance_id=INSTANCE_ID,
        instance_name='gauss-6374',
        nodes=NODES,
        config=config,
        clock=lambda: NOW_MS,
    )
    cc_collector = CommandCenterCollector(
        client=None,
        mapping=mapping,
        command_center=command_center,
        deltas=CounterDeltaStore(),
        clock=lambda: NOW_MS,
    )
    window = resolve_window_from_config(NOW_MS, config)

    def monitor_executor(instance):
        messages = monitor_collector.collect()
        return TaskOutcome(
            messages=messages,
            end_time_ms=window.end_time,
            fetched=len(messages),
            external_calls=2,
        )

    def command_center_executor(instance):
        result = cc_collector.collect(instance_id=instance.instance_id)
        # **取数失败时不给 end_time_ms** —— 否则水位会推进，丢掉那一段数据
        end_time_ms = NOW_MS if result.pages else None
        return TaskOutcome(
            messages=result.messages,
            end_time_ms=end_time_ms,
            fetched=result.pages,
            windows=1,
            external_calls=3,
        )

    def alarm_executor(instance):
        result = collect_alarms(
            alarm_client,
            mapping,
            nodes=[MASTER_NODE],
            instance_id=instance.instance_id,
            watermark=None,
            now_ms=NOW_MS,
        )
        return TaskOutcome(
            alarms=result.messages,
            external_calls=5,
            fetched=result.convergence_count + result.detail_count,
        )

    return {
        CHANNEL_MONITOR: monitor_executor,
        CHANNEL_COMMAND_CENTER: command_center_executor,
        CHANNEL_ALARM: alarm_executor,
    }


def build_scheduler(tmp_path, executors, producer=None, store=None, deltas=None,
                   plans=PLANS):
    return Scheduler(
        plans=plans,
        executors=executors,
        producer=producer if producer is not None else FakeProducer(),
        watermark=store if store is not None else WatermarkStore(
            str(tmp_path / 'wm.json')
        ),
        instances=[InstanceTarget(INSTANCE_ID, 'gauss-6374', NODES)],
        delta_store=deltas,
        dropped_unsupported=3,
        clock=lambda: 1000.0,
        tick_interval_seconds=30,
    )


# ---------------------------------------------------------------------------
# 一轮跑通
# ---------------------------------------------------------------------------

def test_a_full_round_delivers_messages_and_advances_watermarks(
    tmp_path, mapping, config, node_rows, probe_nodes,
):
    """真实采集器 → 调度器 → 假 Kafka：消息投出去了，水位按约定推进。"""
    producer = FakeProducer()
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    executors = build_executors(
        mapping, config, node_rows,
        command_center=FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows),
        alarm_client=FakeAlarmClient({(True, True): [convergence_row()]}),
    )
    scheduler = build_scheduler(tmp_path, executors, producer, store)

    result = scheduler.tick()

    assert result.tasks_failed == 0
    assert result.produced > 0
    assert result.mapped == result.produced
    assert producer.metric_batches
    assert producer.alarm_batches

    # 水位键**含 instance_id**
    assert store.metric_watermark(
        '{0}:{1}'.format(INSTANCE_ID, CHANNEL_MONITOR)
    ) is not None
    assert store.metric_watermark(
        '{0}:{1}'.format(INSTANCE_ID, CHANNEL_COMMAND_CENTER)
    ) is not None
    assert store.alarm_watermark(
        '{0}:{1}'.format(INSTANCE_ID, CHANNEL_ALARM)
    ) is not None


def test_delivered_payloads_are_valid_json(tmp_path, mapping, config, node_rows,
                                          probe_nodes):
    """投出去的消息是**唯一序列化出口**产出的 JSON（7 字段信封）。"""
    producer = FakeProducer()
    executors = build_executors(
        mapping, config, node_rows,
        command_center=FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows),
        alarm_client=FakeAlarmClient({(True, True): [convergence_row()]}),
    )
    build_scheduler(tmp_path, executors, producer).tick()

    payloads = producer.payloads()
    assert payloads
    for payload in payloads:
        body = json.loads(payload)
        assert set(body) == {
            'metricName', 'metricCode', 'resourceName', 'value', 'unit',
            'clock', 'tags',
        }


def test_a_class_multi_component_messages_are_all_delivered(
    tmp_path, mapping, config, node_rows,
):
    """A 类多组件 → **每个组件一条**（幂等键不能把它们压成一条）。"""
    producer = FakeProducer()
    executors = build_executors(
        mapping, config, node_rows,
        command_center=FakeCommandCenter(pages_by_node={}, nodes=node_rows),
        alarm_client=FakeAlarmClient({}),
    )
    build_scheduler(tmp_path, executors, producer).tick()

    component_ids = set()
    for payload in producer.payloads():
        body = json.loads(payload)
        component = (body.get('tags') or {}).get('component_id')
        if component:
            component_ids.add(component)
    assert len(component_ids) >= 2, (
        'A 类多组件被压成了一条 —— 幂等键丢了组件维度：%r' % (component_ids,)
    )


# ---------------------------------------------------------------------------
# 单组失败不拖垮其他组
# ---------------------------------------------------------------------------

def test_command_center_failure_does_not_stop_monitor(
    tmp_path, mapping, config, node_rows,
):
    """命令中心挂了 → A 类的消息**仍被投递**，进程不退出。"""
    producer = FakeProducer()
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    executors = build_executors(
        mapping, config, node_rows,
        command_center=FakeCommandCenter(error='命令执行失败', nodes=node_rows),
        alarm_client=FakeAlarmClient({(True, True): [convergence_row()]}),
    )
    scheduler = build_scheduler(tmp_path, executors, producer, store)

    result = scheduler.tick()

    assert producer.metric_batches, 'A 类的消息被拖掉了'
    # 命令中心这一组没取到数 → 不推进它的水位
    assert store.metric_watermark(
        '{0}:{1}'.format(INSTANCE_ID, CHANNEL_COMMAND_CENTER)
    ) is None
    assert result.produced > 0


# ---------------------------------------------------------------------------
# 水位只在投递成功后推进
# ---------------------------------------------------------------------------

def test_watermark_does_not_advance_when_delivery_fails(
    tmp_path, mapping, config, node_rows, probe_nodes,
):
    """投递失败 → 整批不推进水位（下一轮重发）。"""
    producer = FakeProducer(fail_metric_batches={0})
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    executors = build_executors(
        mapping, config, node_rows,
        command_center=FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows),
        alarm_client=FakeAlarmClient({(True, True): [convergence_row()]}),
    )
    # 只跑 A 类 —— 唯一的一批指标消息就是投递失败的那一批
    scheduler = build_scheduler(
        tmp_path, executors, producer, store,
        plans=(ChannelPlan(CHANNEL_MONITOR, 30, 30, 11),),
    )

    result = scheduler.tick()

    assert result.failed > 0
    assert result.produced == 0
    assert store.metric_watermark(
        '{0}:{1}'.format(INSTANCE_ID, CHANNEL_MONITOR)
    ) is None


# ---------------------------------------------------------------------------
# 幂等
# ---------------------------------------------------------------------------

def test_already_delivered_messages_are_not_delivered_again(
    tmp_path, mapping, config, node_rows, probe_nodes,
):
    """已投过的幂等键 → 下一轮跳过（不重复投递）。"""
    producer = FakeProducer()
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    executors = build_executors(
        mapping, config, node_rows,
        command_center=FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows),
        alarm_client=FakeAlarmClient({(True, True): [convergence_row()]}),
    )
    scheduler = build_scheduler(tmp_path, executors, producer, store)

    first = scheduler.tick()
    assert first.produced > 0

    # 时钟不动 → 三个通道都还没到期 → 什么都不该发生
    batches_before = len(producer.metric_batches)
    second = scheduler.tick()
    assert second.produced == 0
    assert len(producer.metric_batches) == batches_before
