# -*- coding: utf-8 -*-
"""采集器**不推进水位**（那是 wave 3 调度器的职责）—— 行为断言。

原来的断言是 ``not hasattr(collector, 'watermark')`` —— **不可证伪**：
属性名可以任意取（换个名字就绕过去了），而且**只有 A 类有**这一条，
B/C/D/E 四个采集器连同类断言都没有。

这里改成两条**可证伪**的断言：

1. **按类型查**（不是按属性名）—— 采集器对象里**不能有任何**水位存储
   （``WatermarkStore`` 实例），换个属性名也绕不过去；
2. **行为断言** —— E 类采集器**收到**水位（``watermark`` 参数），
   一轮采集后水位文件**逐字节不变**（它只读不写）。
"""

import io
import json
import os

import pytest

from integration.test_collector_alarm import (
    MASTER_NODE,
    FakeAlarmClient,
    convergence_row,
)
from integration.test_collector_command_center import (
    INSTANCE_ID,
    NODE_NAMES,
    FakeCommandCenter,
    _collector,
)

from tpops_kafka_collector.collectors.alarm import collect_alarms
from tpops_kafka_collector.collectors.command_center import CommandCenterCollector
from tpops_kafka_collector.collectors.diagnostic import DiagnosticCollector
from tpops_kafka_collector.collectors.instance import InstanceCollector
from tpops_kafka_collector.collectors.monitor import MonitorCollector
from tpops_kafka_collector.config import Config
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.mapping.rules import CounterDeltaStore
from tpops_kafka_collector.watermark import WatermarkStore

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(REPO_ROOT, 'data')
PROBE_PATH = os.path.join(DATA_DIR, 'merged_sql', 'sql_merge_probe_result.json')
NODE_PATH = os.path.join(
    DATA_DIR, 'metric_response', '003_GAUSSDB.NAME.Number.ad.ples.json'
)
INSTANCE_BODY_PATH = os.path.join(
    DATA_DIR, 'metric_response', '061_gaussdb.server_version.json'
)


def _load(path):
    with io.open(path, encoding='utf-8') as handle:
        return json.load(handle)


@pytest.fixture(scope='module')
def mapping():
    return load_mapping()


@pytest.fixture(scope='module')
def node_rows():
    for step in _load(NODE_PATH):
        if 'key-view-execute-node' in step.get('api', ''):
            return tuple(step['response_body']['nodes'])
    raise AssertionError('留档里没有节点清单的响应')


def _probe_nodes():
    return _load(PROBE_PATH)['nodes']


def _all_collectors(mapping, node_rows):
    """五个采集器实例（各用最简的假依赖）。"""
    class _ArchiveClient(object):
        """只回放实例列表（其它接口回空对象 —— 这一轮只关心水位没被动）。"""

        def get(self, path, params=None):
            if path.startswith('/rds/v3.2/instances'):
                return _load(INSTANCE_BODY_PATH)[0]['response_body']
            return {}

        def post(self, path, payload=None):
            return {}

    class _NoopRegistry(object):
        def nodes(self, instance_id):
            return node_rows

    config = Config(tpops_user='u', tpops_password='p',
                    metric_window_hours=2, metric_end_lag_minutes=5)
    monitor = MonitorCollector(
        client=_ArchiveClient(), mapping=mapping, instance_id=INSTANCE_ID,
        nodes=node_rows, config=config, clock=lambda: 1794454239320,
    )
    command_center = CommandCenterCollector(
        client=None, mapping=mapping,
        command_center=FakeCommandCenter(pages_by_node=_probe_nodes(),
                                         nodes=node_rows),
        deltas=CounterDeltaStore(), clock=lambda: 1794454239320,
    )
    diagnostic = DiagnosticCollector(
        client=_ArchiveClient(), mapping=mapping,
        sql_sanitizer=lambda text: text, clock=lambda: 1794454239320,
    )
    instance = InstanceCollector(
        client=_ArchiveClient(), mapping=mapping,
        node_registry=_NoopRegistry(), clock=lambda: 1794454239320,
    )
    return [
        ('monitor', monitor),
        ('command_center', command_center),
        ('diagnostic', diagnostic),
        ('instance', instance),
    ]


def test_no_collector_holds_a_writable_watermark_state(mapping, node_rows):
    """采集器对象里**不能有任何**水位存储（按**类型**查，不按属性名）。"""
    for name, collector in _all_collectors(mapping, node_rows):
        for attribute, value in vars(collector).items():
            assert not isinstance(value, WatermarkStore), (
                '%s 采集器持有了水位状态（属性 %s）—— 水位推进是调度器的职责',
                name, attribute,
            )


def test_alarm_collector_reads_the_watermark_but_never_writes_it(
    mapping, tmp_path
):
    """E 类采集器**收到**水位也**只读不写** —— 水位文件逐字节不变。"""
    path = str(tmp_path / 'watermark.json')
    store = WatermarkStore(path)
    store.advance_alarm('gauss-6374', '2026-11-20 10:00:00')
    store.save()
    before = io.open(path, 'rb').read()

    client = FakeAlarmClient({(True, True): [convergence_row()]})
    result = collect_alarms(
        client, mapping,
        nodes=[MASTER_NODE],
        instance_id=INSTANCE_ID,
        watermark=store.alarm_watermark('gauss-6374'),
    )

    assert result.messages, '水位过滤后仍应有告警产出（记录比水位新）'
    assert io.open(path, 'rb').read() == before, '采集器改了水位文件'


def test_a_collector_round_does_not_touch_the_watermark_state(
    mapping, node_rows, tmp_path
):
    """跑一整轮采集，**水位状态一字不变**（水位推进是调度器的职责）。"""
    path = str(tmp_path / 'watermark.json')
    store = WatermarkStore(path)
    store.advance_metric('b-class', 1000)
    store.advance_alarm('b-class', '2026-11-20 10:00:00')
    store.save()
    before = store.snapshot()
    bytes_before = io.open(path, 'rb').read()

    monitor, command_center, diagnostic, instance = (
        collector for _name, collector in _all_collectors(mapping, node_rows)
    )
    monitor.collect()
    command_center.collect(instance_id=INSTANCE_ID)
    diagnostic.collect(instance_id=INSTANCE_ID, nodes=node_rows)
    instance.collect(INSTANCE_ID)

    assert store.snapshot() == before, '采集轮次改了水位状态'
    assert io.open(path, 'rb').read() == bytes_before, '采集轮次改了水位文件'


def test_command_center_result_carries_no_watermark_state(mapping, node_rows):
    """B 类采集的**结果对象**里没有水位字段（结果只是消息 + 档位 + 页数）。"""
    collector = CommandCenterCollector(
        client=None, mapping=mapping,
        command_center=FakeCommandCenter(pages_by_node=_probe_nodes(),
                                         nodes=node_rows),
        deltas=CounterDeltaStore(), clock=lambda: 1794454239320,
    )
    result = collector.collect(instance_id=INSTANCE_ID)

    assert set(vars(result)) == {
        'messages', 'tier_seconds', 'tier_label', 'pages', 'page_size',
    }
    assert len(NODE_NAMES) == 3
