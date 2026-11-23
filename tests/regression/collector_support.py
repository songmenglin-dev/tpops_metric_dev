# -*- coding: utf-8 -*-
"""采集器层回归的**公共件**（假客户端、夹具、比对辅助）。

不是测试文件（文件名不以 ``test_`` 开头，pytest 不收集）——
给 ``tests/regression/test_collector_coverage*.py`` 共用。

**为什么回放实测留档**：采集器层的行为取决于平台返回的**真实形态**
（合并 SQL 的一行 N 个 ``json_agg`` 列、监控指标的按组件展开的 series、
诊断优化的集合结构、实例列表的字段名）—— 造一份假数据守不住真实路径。
"""

import json
import os

import pytest

from regression.conftest import (
    EXPECTED_NON_B_PATH,
    LIVE_EXPECTED_VALUES_PATH,
    MERGED_PROBE_PATH,
    METRIC_RESPONSE_DIR,
    load_json,
)

from tpops_kafka_collector.collectors.monitor import MonitorCollector
from tpops_kafka_collector.config import Config
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.mapping.message import serialize_value

#: 目标实例（分布式版 gauss-6374，3 个 CN 均为 master）。
INSTANCE_ID = '83cd68f2e4cb40b4b7e69240479ffa26in14'

#: 客户指标库里的「可提供」状态字面量。
PROVIDED_STATUS = '可提供'

#: 留档清单里的通道名（中文）。
MONITOR_CHANNEL = '监控指标接口'
DIAGNOSTIC_CHANNEL = '诊断优化接口'
INSTANCE_CHANNEL = '实例接口'
COMMAND_CENTER_CHANNEL = '命令中心SQL'

#: 采集端滞后回退（毫秒）—— 与 ``Config.metric_end_lag_minutes`` 一致。
END_LAG_MS = 5 * 60 * 1000

#: 期望值表的推导时刻（``expected_values_non_b.json`` 的 ``now_ms``）——
#: **期望值就是在这个采集时刻推导出来的**，比对必须用同一个时刻
#: （``gaussdb.Uptimestr`` 这类「按采集时刻算」的指标才比得上）。
EXPECTED_NOW_MS = 1794500000000

#: 节点清单的留档文件（里面有 ``key-view-execute-node`` 那一步）。
NODE_FILE = '003_GAUSSDB.NAME.Number.ad.ples.json'


# ---------------------------------------------------------------------------
# 夹具
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def manifest():
    """留档清单（98 个客户指标 → 通道 / 文件）。"""
    return load_json(os.path.join(METRIC_RESPONSE_DIR, '000manifest.json'))


@pytest.fixture(scope='module')
def mapping():
    return load_mapping()


@pytest.fixture(scope='module')
def nodes():
    """节点清单（留档里的 ``key-view-execute-node`` 返回）。"""
    for step in load_json(os.path.join(METRIC_RESPONSE_DIR, NODE_FILE)):
        if 'key-view-execute-node' in step.get('api', ''):
            return tuple(step['response_body']['nodes'])
    raise AssertionError('留档里没有节点清单的响应')


@pytest.fixture(scope='module')
def expected_non_b():
    """A/C/D 16 条指标的期望值表。"""
    return load_json(EXPECTED_NON_B_PATH)['expected']


@pytest.fixture(scope='module')
def expected_b():
    """B 类 82 条指标的期望值表。"""
    return load_json(LIVE_EXPECTED_VALUES_PATH)['expected']


@pytest.fixture(scope='module')
def providable_codes(mapping):
    """客户指标库里判为「可提供」的指标码（98 条）。"""
    return set(
        metric.metric_code for metric in mapping.customer_metrics
        if metric.status == PROVIDED_STATUS
    )


def channel_metrics(manifest, channel):
    """清单里属于某通道的客户指标。"""
    return [item for item in manifest['metrics'] if item['channel'] == channel]


def monitor_response(sample_file):
    """A 类留档里的监控指标响应体（``is_latest_point_only=false`` 那一步）。"""
    for step in load_json(os.path.join(METRIC_RESPONSE_DIR, sample_file)):
        body = (step.get('request') or {}).get('body') or {}
        if str(body.get('is_latest_point_only')).lower() == 'false':
            return step['response_body'], body
    raise AssertionError('留档里没有 is_latest_point_only=false 的调用：%s' % sample_file)


# ---------------------------------------------------------------------------
# 假客户端
# ---------------------------------------------------------------------------

class FakeMonitorClient:
    """A 类：目录 GET + 时序 POST（回放留档响应体）。"""

    def __init__(self, payload, metric_names):
        self._payload = payload
        self._names = list(metric_names)
        self.posts = []

    def get(self, path, params=None):
        group = (params or {}).get('name')
        names = self._names + ['dummy_%s' % group]
        return {'series': [{'name': group, 'metrics': [{'name': n} for n in names]}]}

    def post(self, path, payload=None):
        self.posts.append((path, dict(payload or {})))
        return self._payload


class FakeDiagnosticClient:
    """C 类：慢 SQL / Top SQL 两个接口各回放一份留档响应。"""

    def __init__(self, slow_body, top_body):
        self._slow_body = slow_body
        self._top_body = top_body

    def post(self, path, payload=None):
        if path.endswith('/slow-sql-list'):
            return self._slow_body
        if path.endswith('/top-sql-list'):
            return self._top_body
        raise AssertionError('未预期的请求路径：%s' % path)

    def get(self, path, params=None):  # pragma: no cover - C 类不用 GET
        raise AssertionError('C 类不应发起 GET 请求：%s' % path)


class FakeInstanceClient:
    """D 类：实例列表接口回放留档响应。"""

    def __init__(self, body):
        self._body = body

    def get(self, path, params=None):
        return self._body


class FakeNodeRegistry:
    """D 类：节点清单来源。"""

    def __init__(self, nodes):
        self._nodes = tuple(nodes)

    def nodes(self, instance_id):
        return self._nodes


# ---------------------------------------------------------------------------
# 取值辅助
# ---------------------------------------------------------------------------

def canon(value):
    """值的规范化形态（比较用）—— 与映射层回归同一套口径。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else round(value, 6)
    if isinstance(value, int):
        return value
    if isinstance(value, list):
        return [canon(item) for item in value]
    if isinstance(value, dict):
        return dict((key, canon(item)) for key, item in value.items())
    return value


def matches(message, metric_code, dimensions):
    """消息是不是某条期望条目对应的那一条。

    - ``COMPONENT_ID`` 是**平台序列键**（在 ``tags.component_id`` 里，
      不是客户维度），比对时单独比；
    - 其余维度在 ``tags.dimension_values`` 里。
    """
    if message['metricCode'] != metric_code:
        return False
    tags = message.get('tags') or {}
    dimensions = dimensions or {}
    expected_dims = dict(
        (key, value) for key, value in dimensions.items()
        if key != 'COMPONENT_ID'
    )
    if (tags.get('dimension_values') or {}) != expected_dims:
        return False
    component_id = dimensions.get('COMPONENT_ID')
    if component_id is not None and tags.get('component_id') != component_id:
        return False
    return True


def find(messages, metric_code, dimensions):
    """找出与期望条目对应的消息（找不到返回 ``None``）。"""
    for message in messages:
        if matches(message, metric_code, dimensions):
            return message
    return None


def expected_value(mapping, metric_code, value):
    """期望值按客户表【数据类型】**定型**（与消息里的值同一套定型）。"""
    rule = mapping.metric_rule(metric_code)
    value_type = rule.value_type if rule is not None else None
    return serialize_value(value, value_type)


def collect_monitor(mapping, nodes, sample_file):
    """用一份留档驱动 A 类采集器（返回消息与采集时刻）。"""
    payload, request = monitor_response(sample_file)
    names = request.get('metric_names') or []
    now_ms = EXPECTED_NOW_MS
    client = FakeMonitorClient(payload, names)
    collector = MonitorCollector(
        client=client,
        mapping=mapping,
        instance_id=INSTANCE_ID,
        instance_name='gauss-6374',
        nodes=nodes,
        config=Config(
            tpops_user='u', tpops_password='p',
            metric_window_hours=2, metric_end_lag_minutes=5,
        ),
        clock=lambda: now_ms,
    )
    return collector.collect()


def template_code(message):
    """消息的 ``metricCode`` → **模板码**（把维度值换回 ``{#XXX}``）。"""
    code = message['metricCode']
    for placeholder, value in (message['tags'].get('dimension_values') or {}).items():
        code = code.replace(str(value), '{#%s}' % placeholder)
    return code
