"""来源列校验 / 启动期校验 / 映射表加载与查表（补测）。

守的是**「声明与事实对不上就启动失败」**这条线：

- 实测留档里某个视图的实际返回列 → 多节点取**并集**（某节点这轮没返回行
  不代表列不存在）；
- 留档结构不对（坏 JSON / 行数不对 / 视图列不是行列表）→ ``MappingError``；
- 规则要的列不在留档里 → 问题清单非空 → 启动失败；
- 客户指标库、占位符、维度绑定三类一致性校验的报错文案；
- 映射表缺文件 / 坏 JSON → 报错，不是空表。

每条测试都能被一个具体的小改动打红（见报告的「自检结论」）。
"""

import json
import os
import shutil

import pytest

from tpops_kafka_collector.errors import MappingError, ProtocolError
from tpops_kafka_collector.mapping import loader
from tpops_kafka_collector.mapping.columns import (
    merged_archive_view_columns,
    required_source_columns,
    source_column_problems,
    validate_source_columns,
)
from tpops_kafka_collector.mapping.loader import CustomerMetric, ObservedColumns
from tpops_kafka_collector.mapping.table import MappingTable, MetricRule
from tpops_kafka_collector.mapping.validation import (
    check_placeholder_bindings,
    check_providable_coverage,
    check_rule_entries,
    check_source_columns,
)

from conftest import REPO_ROOT

REPO_MAPPING_DIR = os.path.join(REPO_ROOT, 'data', 'mapping')
PACKAGE_DATA_DIR = os.path.join(
    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data',
)


def _rule(value_rule, source=None, dimension_bindings=None,
          metric_code='test.metric', enabled=True, placeholders=None):
    """按声明造一条规则（不依赖映射表）。"""
    payload = {
        'metric_code': metric_code,
        'metric_name': metric_code,
        'resource_type': 'GAUSSDB实例',
        'category': '事务信息',
        'value_type': '整数',
        'unit': None,
        'collect_cycle': {'value': 5, 'unit': '分'},
        'status': '可提供',
        'match_level': '直接对应',
        'enabled': enabled,
        'source': source or {
            'api': 'POST /rds/gaussdb/v3/customer-commands',
            'source': 'command_center_sql',
            'tpops_source': None,
            'value_source_expr': None,
            'sql_key': 'view',
            'method': '命令中心SQL',
        },
        'value_rule': value_rule,
        'placeholders': placeholders or [],
        'resource_granularity': 'instance',
        'master_view': None,
        'node_scoped': True,
        'dimension_bindings': dimension_bindings or {},
        'note': '',
        'needs_confirmation': False,
    }
    return MetricRule.from_dict(payload)


def _node_result(view_payloads):
    """合并形态留档里一个节点的 ``result``（解析后恰好一行）。"""
    return json.dumps([view_payloads], ensure_ascii=False)


# -- 实测留档 → 实际返回列 ------------------------------------------------

def test_merged_archive_view_columns_unions_the_columns_of_every_node():
    """多节点的列取**并集** —— 某节点这轮没返回该行不代表列不存在。"""
    payload = {'nodes': {
        'node-a': {'result': _node_result({'v': '[{"a": 1}]'})},
        'node-b': {'result': _node_result({'v': '[{"b": 2}]'})},
    }}
    assert merged_archive_view_columns(payload) == {'v': {'a', 'b'}}


def test_merged_archive_view_columns_skips_a_node_without_a_result():
    """没有 result 的节点跳过（不是报错，也不是把它当空视图）。"""
    payload = {'nodes': {
        'node-a': {},
        'node-b': {'result': _node_result({'v': '[{"a": 1}]'})},
    }}
    assert merged_archive_view_columns(payload) == {'v': {'a'}}


def test_merged_archive_view_columns_rejects_a_node_result_that_is_not_json():
    payload = {'nodes': {'node-a': {'result': '[{坏'}}}
    with pytest.raises(MappingError) as excinfo:
        merged_archive_view_columns(payload)
    assert '不是合法 JSON' in str(excinfo.value)


def test_merged_archive_view_columns_rejects_a_node_result_with_the_wrong_row_count():
    payload = {'nodes': {'node-a': {'result': '[]'}}}
    with pytest.raises(MappingError) as excinfo:
        merged_archive_view_columns(payload)
    assert '期望 1 行' in str(excinfo.value)


def test_merged_archive_view_columns_treats_a_null_view_as_no_columns():
    """视图列是 null（零行）→ 该视图存在但没有列。"""
    payload = {'nodes': {'node-a': {'result': _node_result({'v': None})}}}
    columns = merged_archive_view_columns(payload)
    assert columns == {'v': set()}


def test_merged_archive_view_columns_treats_an_empty_view_as_no_columns():
    payload = {'nodes': {'node-a': {'result': _node_result({'v': ''})}}}
    assert merged_archive_view_columns(payload) == {'v': set()}


def test_merged_archive_view_columns_rejects_a_view_that_is_not_json():
    payload = {'nodes': {'node-a': {'result': _node_result({'v': '[{坏'})}}}
    with pytest.raises(MappingError) as excinfo:
        merged_archive_view_columns(payload)
    assert '不是合法 JSON' in str(excinfo.value)


def test_merged_archive_view_columns_rejects_a_view_that_is_not_a_row_list():
    payload = {'nodes': {'node-a': {'result': _node_result({'v': '"abc"'})}}}
    with pytest.raises(MappingError) as excinfo:
        merged_archive_view_columns(payload)
    assert '期望行列表' in str(excinfo.value)


# -- 规则要的列 vs 留档里的列 --------------------------------------------

def test_source_column_problems_reports_a_view_that_is_not_in_the_archive():
    rules = [_rule({'type': 'raw', 'params': {'column': 'c'}})]
    problems = source_column_problems(rules, {})
    assert problems == ['视图 view 不在实测留档里']


def test_source_column_problems_reports_a_column_that_the_view_does_not_have():
    rules = [_rule({'type': 'raw', 'params': {'column': 'c'}})]
    problems = source_column_problems(rules, {'view': {'other'}})
    assert len(problems) == 1
    assert '要取的列 c 不在实测留档视图 view 的实际列里' in problems[0]


def test_source_column_problems_is_empty_when_every_column_is_present():
    rules = [_rule({'type': 'raw', 'params': {'column': 'c'}})]
    assert source_column_problems(rules, {'view': {'c'}}) == []


def test_validate_source_columns_raises_on_a_missing_column():
    rules = [_rule({'type': 'raw', 'params': {'column': 'c'}})]
    with pytest.raises(MappingError) as excinfo:
        validate_source_columns(rules, {'view': {'other'}})
    assert '来源列校验不通过' in str(excinfo.value)


def test_required_source_columns_collects_the_ratio_sides():
    """跨视图比率：分子分母各来自一个视图。"""
    rules = [{
        'metric_code': 'm',
        'source': {'source': 'command_center_sql', 'sql_key': 'ignored'},
        'value_rule': {'type': 'ratio_across', 'params': {
            'numerator': {'sql_key': 'num', 'value_column': 'v'},
            'denominator': {'sql_key': 'den', 'value_column': 'v'},
        }},
    }]
    assert required_source_columns(rules) == {
        'num': {('m', 'v')},
        'den': {('m', 'v')},
    }


def test_required_source_columns_skips_a_side_without_a_column():
    """某一侧没声明列 → 不登记（不是登记一个空列名）。"""
    rules = [{
        'metric_code': 'm',
        'source': {'source': 'command_center_sql', 'sql_key': 'ignored'},
        'value_rule': {'type': 'ratio_across', 'params': {
            'numerator': {'sql_key': 'num', 'value_column': 'v'},
            'denominator': {'sql_key': 'den'},
        }},
    }]
    assert required_source_columns(rules) == {'num': {('m', 'v')}}


def test_required_source_columns_ignores_the_other_channels():
    """非 B 类通道的列不在合并留档里，不登记。"""
    rules = [{
        'metric_code': 'm',
        'source': {'source': 'diagnostic'},
        'value_rule': {'type': 'raw', 'params': {'column': 'c'}},
    }]
    assert required_source_columns(rules) == {}


# -- 启动期校验：其余三个通道 --------------------------------------------

def _observed(**kwargs):
    base = {
        'merged_views': {},
        'collections': {},
        'instance_fields': frozenset(),
        'series_metrics': frozenset(),
    }
    base.update(kwargs)
    return ObservedColumns(
        merged_views=base['merged_views'],
        collections=base['collections'],
        instance_fields=base['instance_fields'],
        series_metrics=base['series_metrics'],
    )


def test_dimension_binding_of_a_view_outside_the_archive_is_not_reported_twice():
    """视图本身不在留档里 → 只报「视图不在留档里」，不再报维度绑定。"""
    rules = [_rule({'type': 'raw', 'params': {'column': 'c'}},
                   dimension_bindings={'N': 'col'})]
    problems = check_source_columns(rules, _observed(merged_views={}))
    assert not [p for p in problems if '维度绑定' in p]


def test_dimension_binding_of_a_missing_column_is_reported():
    rules = [_rule({'type': 'raw', 'params': {'column': 'c'}},
                   dimension_bindings={'N': 'col'})]
    problems = check_source_columns(
        rules, _observed(merged_views={'view': {'c'}}),
    )
    assert len(problems) == 1
    assert '维度绑定 N→col' in problems[0]


def test_a_diagnostic_rule_with_an_unknown_collection_is_reported():
    rules = [_rule({'type': 'raw', 'params': {'collection': 'nope', 'column': 'c'}},
                   source={'source': 'diagnostic'})]
    problems = check_source_columns(rules, _observed(collections={}))
    assert len(problems) == 1
    assert '引用的集合 nope 不在实测留档里' in problems[0]


def test_a_diagnostic_rule_with_a_missing_filter_column_is_reported():
    """``filter`` 的 ``xxx_contains`` 引的是列 ``xxx``。"""
    rules = [_rule({'type': 'raw', 'params': {
        'collection': 'slow_sql_infos', 'column': 'sql_id',
        'filter': {'state_contains': ['active']},
    }}, source={'source': 'diagnostic'})]
    problems = check_source_columns(
        rules, _observed(collections={'slow_sql_infos': {'sql_id'}}),
    )
    assert len(problems) == 1
    assert '要取的列 state 不在实测留档集合 slow_sql_infos 的实际列里' in problems[0]


def test_a_diagnostic_rule_with_a_missing_group_by_column_is_reported():
    rules = [_rule({'type': 'aggregate', 'params': {
        'collection': 'slow_sql_infos', 'value_column': 'sql_id',
        'group_by': ['relname'],
    }}, source={'source': 'diagnostic'})]
    problems = check_source_columns(
        rules, _observed(collections={'slow_sql_infos': {'sql_id'}}),
    )
    assert len(problems) == 1
    assert '要取的列 relname' in problems[0]


def test_a_monitor_rule_with_an_unknown_platform_metric_is_reported():
    rules = [_rule({'type': 'series_point', 'params': {'column': 'nope'}},
                   source={'source': 'monitor_metrics'})]
    problems = check_source_columns(rules, _observed(series_metrics=frozenset({'x'})))
    assert len(problems) == 1
    assert '要取的平台指标名 nope 不在实测留档里' in problems[0]


def test_an_instance_rule_with_an_unknown_field_is_reported():
    rules = [_rule({'type': 'instance_field', 'params': {'field': 'nope'}},
                   source={'source': 'instance'})]
    problems = check_source_columns(rules, _observed(instance_fields=frozenset({'x'})))
    assert len(problems) == 1
    assert '要取的实例字段 nope 不在实测留档里' in problems[0]


# -- 启动期校验：指标库与占位符 -------------------------------------------

def test_providable_coverage_reports_a_missing_rule():
    metrics = [CustomerMetric('m1', '名', '类', None, '可提供', '直接对应')]
    rules = [_rule({'type': 'raw', 'params': {}}, metric_code='other')]
    problems = check_providable_coverage(rules, metrics)
    assert '可提供指标 m1 在映射表里没有启用的规则' in problems


def test_providable_coverage_reports_a_rule_for_a_non_providable_metric():
    metrics = [CustomerMetric('m1', '名', '类', None, '无法提供', '直接对应')]
    rules = [_rule({'type': 'raw', 'params': {}}, metric_code='m1')]
    problems = check_providable_coverage(rules, metrics)
    assert problems == ['映射表里 m1 是启用的，但客户指标库判定它不是可提供']


def test_a_disabled_rule_does_not_count_as_provided():
    metrics = [CustomerMetric('m1', '名', '类', None, '可提供', '直接对应')]
    rules = [_rule({'type': 'raw', 'params': {}}, metric_code='m1', enabled=False)]
    assert check_providable_coverage(rules, metrics) == [
        '可提供指标 m1 在映射表里没有启用的规则',
    ]


def test_rule_entries_reports_a_metric_that_is_not_in_the_customer_library():
    metrics = [CustomerMetric('m1', '名', '类', None, '可提供', '直接对应')]
    rules = [_rule({'type': 'raw', 'params': {}}, metric_code='ghost')]
    assert check_rule_entries(rules, metrics) == [
        '映射表里的条目 ghost 在客户指标库里不存在',
    ]


def test_placeholder_bindings_reports_a_mismatch():
    """占位符与维度绑定不是一一对应 → 报出来。"""
    rules = [_rule({'type': 'raw', 'params': {}}, metric_code='m.{#OTHER}',
                   dimension_bindings={'N': 'col'}, placeholders=['OTHER'])]
    problems = check_placeholder_bindings(rules)
    assert len(problems) == 1
    assert '不是一一对应' in problems[0]


def test_placeholder_bindings_reports_a_name_that_does_not_match():
    """占位符与指标名里的 ``{#XXX}`` 不一致 → 报出来。"""
    rules = [_rule({'type': 'raw', 'params': {}}, metric_code='m.{#OTHER}',
                   dimension_bindings={'N': 'col'}, placeholders=['N'])]
    problems = check_placeholder_bindings(rules)
    assert len(problems) == 1
    assert '与指标名里的占位符' in problems[0]


# -- 映射表加载与查表 -----------------------------------------------------

@pytest.fixture()
def mapping_sandbox(tmp_path):
    """映射数据的一份可改副本。"""
    target = tmp_path / 'mapping'
    shutil.copytree(REPO_MAPPING_DIR, str(target))
    return str(target)


def _write_json(path, payload):
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write(json.dumps(payload, ensure_ascii=False))


def test_loader_reports_a_metric_rule_entry_with_a_missing_field():
    """规则结构不对 → ``ProtocolError``（不是 KeyError 漏出去）。"""
    with pytest.raises(ProtocolError) as excinfo:
        loader._parse_rules({'rules': [{'metric_code': 'm'}]})
    assert '规则结构不对' in str(excinfo.value)


def test_loader_reports_a_metric_rules_payload_without_a_rules_array():
    with pytest.raises(ProtocolError) as excinfo:
        loader._parse_rules({'items': []})
    assert 'rules' in str(excinfo.value)


def test_loader_reports_a_customer_metric_payload_without_an_items_array():
    with pytest.raises(ProtocolError) as excinfo:
        loader._parse_customer_metrics({})
    assert 'items' in str(excinfo.value)


def test_loader_reports_a_customer_metric_entry_without_a_code():
    with pytest.raises(ProtocolError) as excinfo:
        loader._parse_customer_metrics({'items': [{'customer_metric_name': '名'}]})
    assert 'customer_metric' in str(excinfo.value)


def test_loader_reports_a_broken_observed_columns_payload():
    """``merged_views`` 的值不是可迭代的集合 → ``ProtocolError``。"""
    with pytest.raises(ProtocolError) as excinfo:
        loader._parse_observed({'merged_views': {'v': 5}})
    assert '结构不对' in str(excinfo.value)


def test_loader_read_json_rejects_a_top_level_list(tmp_path):
    """顶层不是对象 → ``ProtocolError``（不是 dict 假设被打破）。"""
    with open(os.path.join(str(tmp_path), 'itscada_field_map.json'),
                 'w', encoding='utf-8') as handle:
        handle.write('[1, 2]')
    with pytest.raises(ProtocolError) as excinfo:
        loader._read_json(str(tmp_path), 'field_map')
    assert '顶层不是对象' in str(excinfo.value)


def test_loader_read_json_reports_a_missing_file(tmp_path):
    with pytest.raises(MappingError) as excinfo:
        loader._read_json(str(tmp_path), 'field_map')
    assert '映射文件不存在' in str(excinfo.value)


def test_loader_read_json_reports_broken_json(tmp_path):
    with open(os.path.join(str(tmp_path), 'itscada_field_map.json'),
                 'w', encoding='utf-8') as handle:
        handle.write('{ 这不是 JSON')
    with pytest.raises(MappingError) as excinfo:
        loader._read_json(str(tmp_path), 'field_map')
    assert '不是合法 JSON' in str(excinfo.value)


# -- MappingTable 的查表接口 ----------------------------------------------

def _table(field_map=None):
    return MappingTable(
        field_map or {
            'metric_message': {'fields': [{'name': 'metricName'}],
                               'required_fields': ['metricName', 'value']},
            'alarm_message': {'fields': [{'name': 'alarmId'}],
                              'required_fields': ['alarmId']},
            'resource_name': {'template': '{host_ip}_{node_id}',
                              'fallback_template': ['{host_ip}', '{node_id}']},
        },
        (),
        {},
        {'monitor_metrics': {'a': 1}},
    )


def test_required_fields_of_an_unknown_message_type_is_rejected():
    with pytest.raises(MappingError) as excinfo:
        _table().required_fields('nope')
    assert '字段映射表里没有 nope_message' in str(excinfo.value)


def test_required_fields_returns_the_declared_field_names():
    assert _table().required_fields('metric') == ('metricName', 'value')


def test_message_fields_returns_the_declared_fields():
    assert _table().message_fields('alarm') == ({'name': 'alarmId'},)


def test_resource_name_template_and_fallback():
    table = _table()
    assert table.resource_name_template == '{host_ip}_{node_id}'
    assert table.resource_name_fallback == ['{host_ip}', '{node_id}']


def test_category_mappings_exposes_every_category():
    assert _table().category_mappings == {'monitor_metrics': {'a': 1}}


def test_mapping_table_load_reports_a_missing_file(mapping_sandbox):
    os.remove(os.path.join(mapping_sandbox, 'itscada_field_map.json'))
    with pytest.raises(MappingError) as excinfo:
        MappingTable.load(mapping_sandbox, validate_columns=False)
    assert '映射文件不存在' in str(excinfo.value)


def test_mapping_table_load_reports_broken_json(mapping_sandbox):
    with open(os.path.join(mapping_sandbox, 'itscada_field_map.json'),
                 'w', encoding='utf-8') as handle:
        handle.write('{ 这不是 JSON')
    with pytest.raises(MappingError) as excinfo:
        MappingTable.load(mapping_sandbox, validate_columns=False)
    assert '不是合法 JSON' in str(excinfo.value)


def test_mapping_table_load_reads_the_real_tables(mapping_sandbox):
    table = MappingTable.load(mapping_sandbox, validate_columns=False)
    assert len(table.metric_rules) == 101
    assert table.metric_rule('gaussdb.Tps') is not None
    assert table.metrics_of_sql_key('activity_agg')
