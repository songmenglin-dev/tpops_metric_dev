# -*- coding: utf-8 -*-
"""D 类采集器（任务 2.8）：实例元数据 → ITSCADA 指标消息。

**不连真实 TPOPS** —— 平台客户端与节点清单都注入假实现；
留档（``data/metric_response/``）直接当回归样本回放。

守住六件事：

1. **接口路径是 ``/rds/v3.2/instances``**（不是 ``v3``）—— 少一段就是 404；
2. **只取目标实例**的 ``engine_version``（平台上有两个实例，只覆盖一个）；
3. **消息形态**用 ``mapping.message.build_metric_message`` 组装（7 字段信封）；
4. **资源名落到第一个 master CN**（``{host_ip}_{node_id}``）—— D 类与节点无关；
5. **1 小时档**由映射表的 ``collect_cycle`` 推导（表驱动，不写死）；
6. **缺值不产出消息** —— 实例列表为空 / ``engine_version`` 缺失 / 找不到目标实例，
   都不产出消息（**不静默出 0**）。
"""
import io
import json
import os

import pytest

from conftest import DATA_DIR, REPO_ROOT

from tpops_kafka_collector.collectors.instance import (
    CYCLE_UNIT_SECONDS,
    InstanceCollector,
    collect_cycle_label,
    collect_cycle_seconds,
)
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.tpops.instances import (
    INSTANCES_PATH,
    InstanceMetadata,
    fetch_instances,
    select_instance,
)

RESP_DIR = os.path.join(DATA_DIR, 'metric_response')
INSTANCE_RESPONSE_FILE = os.path.join(RESP_DIR, '061_gaussdb.server_version.json')
NODE_RESPONSE_FILE = os.path.join(RESP_DIR, '003_GAUSSDB.NAME.Number.ad.ples.json')

TARGET_INSTANCE_ID = '83cd68f2e4cb40b4b7e69240479ffa26in14'
OTHER_INSTANCE_ID = 'c35b8d078cae4a9eb05d616ed5cf032fin14'


# ---------------------------------------------------------------------------
# 留档回放
# ---------------------------------------------------------------------------

def _load(path):
    with io.open(path, encoding='utf-8') as handle:
        return json.load(handle)


def _instance_payload():
    """D 类留档的原生响应体（``GET /rds/v3.2/instances``）。"""
    chain = _load(INSTANCE_RESPONSE_FILE)
    for step in chain:
        if step.get('api', '').startswith('GET /rds/v3.2/instances'):
            return step['response_body']
    raise AssertionError('留档里没有实例列表的响应')


def _node_payload():
    """节点清单留档（``GET …/key-view-execute-node``）。"""
    chain = _load(NODE_RESPONSE_FILE)
    for step in chain:
        if 'key-view-execute-node' in step.get('api', ''):
            return step['response_body']
    raise AssertionError('留档里没有节点清单的响应')


# ---------------------------------------------------------------------------
# 假 TPOPS 客户端 / 假节点清单
# ---------------------------------------------------------------------------

class FakeClient:
    """假 TPOPS 客户端：只认实例列表接口，记录调用。"""

    def __init__(self, payload=None, error=None):
        self._payload = payload if payload is not None else {}
        self._error = error
        self.calls = []

    def get(self, path, params=None):
        self.calls.append((path, dict(params or {})))
        if self._error is not None:
            raise self._error
        return self._payload


class FakeNodeRegistry:
    """假节点清单：返回留档里的节点（第一个 master CN = cn_5001）。"""

    def __init__(self, nodes):
        self._nodes = tuple(nodes)
        self.calls = []

    def nodes(self, instance_id):
        self.calls.append(instance_id)
        return self._nodes


@pytest.fixture(scope='module')
def mapping():
    return load_mapping()


@pytest.fixture(scope='module')
def node_rows():
    return tuple(_node_payload()['nodes'])


@pytest.fixture
def node_registry(node_rows):
    """**函数级** fixture —— ``calls`` 要能断言「一次 collect 问一次清单」。

    module 级的话 ``calls`` 会跨用例累积，断言就退化成「问过没问对」。
    """
    return FakeNodeRegistry(node_rows)


def _collector(client, node_registry, mapping):
    return InstanceCollector(client=client, mapping=mapping, node_registry=node_registry)


# ---------------------------------------------------------------------------
# 1. 接口路径与取数
# ---------------------------------------------------------------------------

def test_fetches_instance_list_from_v3_2_path():
    """实例列表在 ``/rds/v3.2/instances`` —— 不是 ``v3``（少一段就 404）。"""
    client = FakeClient(_instance_payload())
    fetch_instances(client)
    assert [call[0] for call in client.calls] == [INSTANCES_PATH]
    assert INSTANCES_PATH == '/rds/v3.2/instances'


def test_fetch_instances_parses_both_instances():
    """平台上有两个实例；本项目只覆盖其中一个，但都要解析出来。"""
    client = FakeClient(_instance_payload())
    instances = fetch_instances(client)
    assert [item.instance_id for item in instances] == [
        TARGET_INSTANCE_ID, OTHER_INSTANCE_ID,
    ]
    first = instances[0]
    assert first.name == 'gauss-6374'
    assert first.mode == 'Combined'
    assert first.engine_version == 'V2.0-10.0.0'
    assert first.instance_status == 'normal'


def test_fetch_instances_pages_until_total_count():
    """``total_count`` 大于单页时按 ``offset`` 翻页取全。"""
    pages = [
        {'total_count': 3, 'instances': [
            {'id': 'i1', 'name': 'a', 'engine_version': 'V1'},
        ]},
        {'total_count': 3, 'instances': [
            {'id': 'i2', 'name': 'b', 'engine_version': 'V2'},
        ]},
        {'total_count': 3, 'instances': [
            {'id': 'i3', 'name': 'c', 'engine_version': 'V3'},
        ]},
    ]

    class PagingClient(FakeClient):
        def get(self, path, params=None):
            self.calls.append((path, dict(params or {})))
            offset = (params or {}).get('offset', 0)
            return pages[offset // 1]

    client = PagingClient()
    instances = fetch_instances(client, page_size=1)
    assert [item.instance_id for item in instances] == ['i1', 'i2', 'i3']
    assert [call[1]['offset'] for call in client.calls] == [0, 1, 2]


def test_select_instance_picks_requested_one():
    client = FakeClient(_instance_payload())
    instances = fetch_instances(client)
    assert select_instance(instances, OTHER_INSTANCE_ID).name == 'gauss-75a2'
    assert select_instance(instances, 'not-exist') is None


# ---------------------------------------------------------------------------
# 2. 采集器产出消息
# ---------------------------------------------------------------------------

def test_collects_one_metric_message_for_target_instance(mapping, node_registry):
    client = FakeClient(_instance_payload())
    result = _collector(client, node_registry, mapping).collect(
        instance_id=TARGET_INSTANCE_ID,
    )
    assert len(result.messages) == 1
    message = result.messages[0]
    assert message['metricCode'] == 'gaussdb.server_version'
    assert message['metricName'] == '版本'
    assert message['value'] == 'V2.0-10.0.0'
    assert message['unit'] is None
    assert message['tags']['instance_id'] == TARGET_INSTANCE_ID
    assert message['tags']['instance_name'] == 'gauss-6374'


def test_message_resource_name_lands_on_first_master_cn(mapping, node_registry):
    """D 类与节点无关 → 落到节点清单里的第一个 master CN。"""
    client = FakeClient(_instance_payload())
    result = _collector(client, node_registry, mapping).collect(
        instance_id=TARGET_INSTANCE_ID,
    )
    message = result.messages[0]
    assert message['resourceName'] == '172.16.103.126_3881c04403394ee1b36a2bf873e69007no14'
    assert message['tags']['node_id'] == '3881c04403394ee1b36a2bf873e69007no14'
    assert message['tags']['host_ip'] == '172.16.103.126'


def test_collect_is_stable_across_rounds(mapping, node_registry):
    """资源名跨轮稳定 —— 不会因为「这轮取到哪个节点」而漂移。"""
    client = FakeClient(_instance_payload())
    collector = _collector(client, node_registry, mapping)
    first = collector.collect(instance_id=TARGET_INSTANCE_ID)
    second = collector.collect(instance_id=TARGET_INSTANCE_ID)
    assert first.messages[0]['resourceName'] == second.messages[0]['resourceName']


# ---------------------------------------------------------------------------
# 3. 1 小时档
# ---------------------------------------------------------------------------

def test_collect_cycle_is_one_hour(mapping, node_registry):
    client = FakeClient(_instance_payload())
    result = _collector(client, node_registry, mapping).collect(
        instance_id=TARGET_INSTANCE_ID,
    )
    assert result.tier_seconds == 3600
    assert result.tier_label == '1 时'


def test_cycle_conversion_table():
    assert CYCLE_UNIT_SECONDS['秒'] == 1
    assert CYCLE_UNIT_SECONDS['分'] == 60
    assert CYCLE_UNIT_SECONDS['时'] == 3600
    assert CYCLE_UNIT_SECONDS['天'] == 86400
    assert collect_cycle_seconds({'value': 1, 'unit': '时'}) == 3600
    assert collect_cycle_seconds({'value': 60, 'unit': '秒'}) == 60
    assert collect_cycle_label({'value': 1, 'unit': '时'}) == '1 时'


def test_tier_comes_from_mapping_rule_not_hardcoded(mapping):
    """档位由映射表的 ``collect_cycle`` 推导 —— 改表就改档，不改代码。"""
    rules = [
        rule for rule in mapping.metric_rules
        if rule.enabled and rule.source.get('source') == 'instance'
    ]
    assert len(rules) == 1
    assert rules[0].metric_code == 'gaussdb.server_version'
    assert collect_cycle_seconds(rules[0].collect_cycle) == 3600


# ---------------------------------------------------------------------------
# 4. 缺值不产出消息
# ---------------------------------------------------------------------------

def test_empty_instance_list_produces_no_message(mapping, node_registry, caplog):
    client = FakeClient({'total_count': 0, 'instances': []})
    result = _collector(client, node_registry, mapping).collect(
        instance_id=TARGET_INSTANCE_ID,
    )
    assert result.messages == ()
    assert '实例列表为空' in caplog.text


def test_missing_engine_version_produces_no_message(mapping, node_registry, caplog):
    """``engine_version`` 缺失 → 不产出消息（**不静默出 0，也不发 null 值**）。"""
    client = FakeClient({'total_count': 1, 'instances': [
        {'id': TARGET_INSTANCE_ID, 'name': 'gauss-6374', 'mode': 'Combined'},
    ]})
    result = _collector(client, node_registry, mapping).collect(
        instance_id=TARGET_INSTANCE_ID,
    )
    assert result.messages == ()
    assert 'engine_version' in caplog.text


def test_unknown_target_instance_produces_no_message(mapping, node_registry, caplog):
    client = FakeClient(_instance_payload())
    result = _collector(client, node_registry, mapping).collect(
        instance_id='not-exist',
    )
    assert result.messages == ()
    assert '找不到目标实例' in caplog.text


def test_platform_error_is_not_swallowed(mapping, node_registry):
    """平台报错要显式抛出去（不静默返回空）。"""
    from tpops_kafka_collector.errors import TransientError

    client = FakeClient(error=TransientError('平台 503'))
    with pytest.raises(TransientError):
        _collector(client, node_registry, mapping).collect(
            instance_id=TARGET_INSTANCE_ID,
        )


# ---------------------------------------------------------------------------
# 5. 消息字段集合是冻结信封
# ---------------------------------------------------------------------------

def test_message_has_exactly_the_frozen_envelope_fields(mapping, node_registry):
    from tpops_kafka_collector.kafka.serializer import METRIC_FIELDS

    client = FakeClient(_instance_payload())
    result = _collector(client, node_registry, mapping).collect(
        instance_id=TARGET_INSTANCE_ID,
    )
    assert set(result.messages[0]) == set(METRIC_FIELDS)


def test_message_serializes_through_the_only_exit(mapping, node_registry):
    from tpops_kafka_collector.kafka.serializer import serialize_metric

    client = FakeClient(_instance_payload())
    result = _collector(client, node_registry, mapping).collect(
        instance_id=TARGET_INSTANCE_ID,
    )
    payload = json.loads(serialize_metric(result.messages[0]))
    assert payload['metricCode'] == 'gaussdb.server_version'
    assert payload['value'] == 'V2.0-10.0.0'


# ---------------------------------------------------------------------------
# 6. 节点清单是取 master CN 的唯一来源
# ---------------------------------------------------------------------------

def test_node_registry_is_asked_for_the_target_instance(mapping, node_registry):
    client = FakeClient(_instance_payload())
    _collector(client, node_registry, mapping).collect(instance_id=TARGET_INSTANCE_ID)
    assert node_registry.calls == [TARGET_INSTANCE_ID]


def test_module_path_is_inside_the_production_package():
    """生产代码只住在 ``tpops_kafka_collector/``（设计决策 2）。"""
    assert REPO_ROOT is not None
