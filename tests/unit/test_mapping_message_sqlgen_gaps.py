"""消息组装与合并 SQL 渲染的边界（补测）。

守的是**「缺值不产出假数据」与「渲染失败要炸」**这两条线：

- 告警的严重级别没在映射表里 → 回落定义里的默认优先级（不是 None）；
- 浮点列的值转不成数 → ``None``（不是 0.0）；
- 告警正文按「明细里的描述 → 记录的描述 → 记录的原因 → 告警名」依次回落；
- 合并 SQL 声明缺文件 / 坏 JSON / 缺 ``batches`` 段 / 视图缺 ``sql_key``
  → 一律 ``MappingError``，不是空规格。

每条测试都能被一个具体的小改动打红（见报告的「自检结论」）。
"""

import json

import pytest

from tpops_kafka_collector.errors import MappingError
from tpops_kafka_collector.mapping.message import (
    build_alarm_message,
    build_metric_message,
    serialize_value,
)
from tpops_kafka_collector.mapping.sqlgen import load_spec
from tpops_kafka_collector.mapping.table import AlarmDefinition, MetricRule

FIELD_MAP = {
    'resource_name': {
        'template': '{host_ip}_{node_id}',
        'fallback_template': ['{host_ip}', '{node_id}'],
    },
}


def _metric_rule(value_type='浮点', unit='秒'):
    payload = {
        'metric_code': 'gaussdb.demo',
        'metric_name': 'demo',
        'resource_type': 'GAUSSDB实例',
        'category': '事务信息',
        'value_type': value_type,
        'unit': unit,
        'collect_cycle': {'value': 5, 'unit': '分'},
        'status': '可提供',
        'match_level': '直接对应',
        'enabled': True,
        'source': {'source': 'command_center_sql', 'sql_key': 'view'},
        'value_rule': {'type': 'raw', 'params': {}},
        'placeholders': [],
        'resource_granularity': 'instance',
        'master_view': None,
        'node_scoped': True,
        'dimension_bindings': {},
        'note': '',
        'needs_confirmation': False,
    }
    return MetricRule.from_dict(payload)


def _alarm_definition(value_source='alarm_items_sum', default_priority=3):
    return AlarmDefinition(
        alarm_id='1',
        alarm_name='定义里的名字',
        alarm_kind='event',
        default_level='major',
        default_priority=default_priority,
        threshold='80',
        metric_code='gaussdb.alarm.1',
        metric_name='gaussdb.alarm.1',
        value_source=value_source,
        linked_customer_metric=None,
    )


# -- 告警消息 -------------------------------------------------------------

def test_alarm_priority_falls_back_to_the_definition_default():
    """严重级别不在映射表里 → 用定义里的默认优先级（不是 None）。"""
    message = build_alarm_message(
        FIELD_MAP,
        {'alarm_id': '1', 'severity': 'minor', 'status': 'active',
         'alarm_items_sum': 5},
        _alarm_definition(default_priority=3),
        {'instance_id': 'i1', 'instance_name': 'inst'},
        {'major': 2},
        {'active': 1},
    )
    assert message['priority'] == 3


def test_alarm_priority_uses_the_severity_mapping_when_it_exists():
    message = build_alarm_message(
        FIELD_MAP,
        {'alarm_id': '1', 'severity': 'major', 'status': 'active'},
        _alarm_definition(default_priority=3),
        {'instance_id': 'i1'},
        {'major': 2},
        {'active': 1},
    )
    assert message['priority'] == 2


def test_alarm_content_falls_back_to_the_cause():
    """记录没有描述但有原因 → 正文用原因。"""
    message = build_alarm_message(
        FIELD_MAP,
        {'alarm_id': '1', 'severity': 'major', 'status': 'active',
         'description': '', 'cause': '磁盘空间不足'},
        _alarm_definition(),
        {'instance_id': 'i1'},
        {'major': 2},
        {'active': 1},
    )
    assert message['alarmContent'] == '磁盘空间不足'


def test_alarm_content_falls_back_to_the_alarm_name():
    """描述与原因都没有 → 用告警名。"""
    message = build_alarm_message(
        FIELD_MAP,
        {'alarm_id': '1', 'severity': 'major', 'status': 'active',
         'alarm_name': '平台告警名'},
        _alarm_definition(),
        {'instance_id': 'i1'},
        {'major': 2},
        {'active': 1},
    )
    assert message['alarmContent'] == '平台告警名'


def test_alarm_content_prefers_the_detail_description():
    """明细里有描述 → 用明细的。"""
    message = build_alarm_message(
        FIELD_MAP,
        {'alarm_id': '1', 'severity': 'major', 'status': 'active',
         'description': '记录的描述'},
        _alarm_definition(),
        {'instance_id': 'i1'},
        {'major': 2},
        {'active': 1},
        details=[{'description': '明细的描述'}],
    )
    assert message['alarmContent'] == '明细的描述'


def test_alarm_value_follows_the_declared_value_source():
    """``latest_metric_value`` 取传入的指标值，否则取记录里的和。"""
    definition = _alarm_definition(value_source='latest_metric_value')
    message = build_alarm_message(
        FIELD_MAP,
        {'alarm_id': '1', 'severity': 'major', 'status': 'active',
         'alarm_items_sum': 5},
        definition,
        {'instance_id': 'i1'},
        {'major': 2},
        {'active': 1},
        metric_value=42,
    )
    assert message['value'] == '42'


def test_alarm_tags_always_carry_the_details_array():
    """明细缺失时输出空数组（不省略字段）。"""
    message = build_alarm_message(
        FIELD_MAP,
        {'alarm_id': '1', 'severity': 'major', 'status': 'active'},
        _alarm_definition(),
        {'instance_id': 'i1'},
        {'major': 2},
        {'active': 1},
    )
    assert message['tags']['details'] == []


def test_metric_message_carries_the_dimension_values_in_the_tags():
    """客户维度进 ``tags.dimension_values``；``COMPONENT_ID`` 是资源属性。"""
    message = build_metric_message(
        FIELD_MAP,
        _metric_rule(),
        {'NAME': 't1', 'COMPONENT_ID': 'cn_1'},
        3.5,
        {'instance_id': 'i1', 'instance_name': 'inst', 'node_id': 'n1',
         'host_ip': '1.2.3.4'},
        1700000000000,
    )
    assert message['tags']['dimension_values'] == {'NAME': 't1'}
    assert message['tags']['node_id'] == 'n1'
    assert message['value'] == 3.5


def test_metric_message_of_an_integer_typed_metric_truncates():
    message = build_metric_message(
        FIELD_MAP,
        _metric_rule(value_type='整数'),
        {},
        3.7,
        {'instance_id': 'i1'},
        1,
    )
    assert message['value'] == 3


# -- 取值序列化 -----------------------------------------------------------

def test_serialize_value_of_a_float_keeps_two_decimals():
    assert serialize_value('1.234', '浮点') == 1.23


def test_serialize_value_of_an_unparsable_float_is_none():
    """转不成数 → ``None``（**不是 0.0**）。"""
    assert serialize_value('abc', '浮点') is None


def test_serialize_value_of_a_missing_value_is_none():
    assert serialize_value(None, '浮点') is None
    assert serialize_value(None, '整数') is None


def test_serialize_value_of_an_integer_typed_value():
    assert serialize_value('3.7', '整数') == 3


def test_serialize_value_of_a_list_keeps_it_as_a_list():
    assert serialize_value([1, 2], '字符') == [1, 2]


# -- 合并 SQL 声明的加载 --------------------------------------------------

def test_load_spec_reports_a_missing_file(tmp_path):
    with pytest.raises(MappingError) as excinfo:
        load_spec(str(tmp_path / 'nope.json'))
    assert '读取合并 SQL 声明失败' in str(excinfo.value)


def test_load_spec_reports_broken_json(tmp_path):
    path = tmp_path / 'broken.json'
    with open(str(path), 'w', encoding='utf-8') as handle:
        handle.write('{ 这不是 JSON')
    with pytest.raises(MappingError) as excinfo:
        load_spec(str(path))
    assert '不是合法 JSON' in str(excinfo.value)


def test_load_spec_rejects_a_payload_without_a_batches_section(tmp_path):
    path = tmp_path / 'no_batches.json'
    with open(str(path), 'w', encoding='utf-8') as handle:
        handle.write(json.dumps({'paging': {}}))
    with pytest.raises(MappingError) as excinfo:
        load_spec(str(path))
    assert '缺 batches 段' in str(excinfo.value)


def test_load_spec_rejects_an_empty_batches_section(tmp_path):
    path = tmp_path / 'empty_batches.json'
    with open(str(path), 'w', encoding='utf-8') as handle:
        handle.write(json.dumps({'batches': []}))
    with pytest.raises(MappingError) as excinfo:
        load_spec(str(path))
    assert '缺 batches 段' in str(excinfo.value)


def test_load_spec_rejects_a_view_without_a_sql_key(tmp_path):
    path = tmp_path / 'no_key.json'
    with open(str(path), 'w', encoding='utf-8') as handle:
        handle.write(json.dumps({'batches': [{'sql': 'select 1'}]}))
    with pytest.raises(MappingError) as excinfo:
        load_spec(str(path))
    assert '缺 sql_key/sql' in str(excinfo.value)


def test_load_spec_rejects_a_view_without_sql(tmp_path):
    path = tmp_path / 'no_sql.json'
    with open(str(path), 'w', encoding='utf-8') as handle:
        handle.write(json.dumps({'batches': [{'sql_key': 'v'}]}))
    with pytest.raises(MappingError) as excinfo:
        load_spec(str(path))
    assert '缺 sql_key/sql' in str(excinfo.value)
