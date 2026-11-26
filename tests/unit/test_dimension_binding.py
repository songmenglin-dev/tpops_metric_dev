# -*- coding: utf-8 -*-
"""维度绑定：点号组合绑定与多列分组。

表级指标的 ``{#NAME}`` 要绑成 ``schemaname.relname``（组合绑定），
分组要同时按 ``schemaname`` 与 ``relname``（多列分组）——
否则不同 schema 的同名表（``public.t1`` / ``other.t1``）会撞成同一个维度值，
两条消息在幂等层撞键、后一条被**静默丢弃**。

本文件守三件事：

1. **组合绑定** —— ``{"NAME": "schemaname.relname"}`` 绑出 ``public.t1``；
2. **多列分组** —— 按两列分组，``public.t1`` 与 ``other.t1`` 落进不同的组；
3. **单列绑定不回归** —— 老口径（``{"NAME": "relname"}``）行为不变。
"""
from __future__ import absolute_import

import io
import json
import os

import pytest

from conftest import REPO_ROOT

from tpops_kafka_collector.ids import metric_idempotency_key  # noqa: E402
from tpops_kafka_collector.mapping.message import build_metric_message  # noqa: E402
from tpops_kafka_collector.mapping.rules_context import (  # noqa: E402
    RuleContext,
    RuleError,
    grouped_items,
    row_dimensions,
)
from tpops_kafka_collector.mapping.rules_engine import evaluate  # noqa: E402
from tpops_kafka_collector.mapping.table import MetricRule  # noqa: E402

FIELD_MAP_PATH = os.path.join(
    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'itscada_field_map.json')

#: 采集端的资源上下文（表级指标与节点无关 → 落到第一个 master CN）。
RESOURCE_CONTEXT = {
    'instance_id': 'inst-1',
    'instance_name': 'gauss-6374',
    'node_id': 'node-1',
    'host_ip': '172.16.103.126',
    'component_id': None,
}


def _rule(dimension_bindings, metric_code='gaussdb.{#NAME}.Number.lete.ples'):
    return MetricRule.from_dict({
        'metric_code': metric_code,
        'metric_name': '({#NAME})被删除的行总数（包括已回收行）',
        'resource_type': 'GAUSSDB实例',
        'category': '表信息',
        'value_type': '整数',
        'status': '可提供',
        'enabled': True,
        'placeholders': list(dimension_bindings.keys()),
        'dimension_bindings': dimension_bindings,
    })


# -- 组合绑定 -------------------------------------------------------------
def test_dotted_binding_combines_the_column_values():
    rule = _rule({'NAME': 'schemaname.relname'})
    row = {'schemaname': 'public', 'relname': 't1'}
    assert row_dimensions(rule, row) == {'NAME': 'public.t1'}


def test_dotted_binding_keeps_the_schema_prefix():
    rule = _rule({'NAME': 'schemaname.relname'})
    row = {'schemaname': 'other', 'relname': 't1'}
    assert row_dimensions(rule, row) == {'NAME': 'other.t1'}


def test_dotted_binding_raises_when_a_column_is_missing():
    """取不到列值 → 抛 ``RuleError``，不静默留 ``{#NAME}``。"""
    rule = _rule({'NAME': 'schemaname.relname'})
    row = {'schemaname': 'public'}
    with pytest.raises(RuleError):
        row_dimensions(rule, row)


# -- 单列绑定不回归 -------------------------------------------------------
def test_single_column_binding_is_unchanged():
    rule = _rule({'NAME': 'relname'})
    row = {'schemaname': 'public', 'relname': 't1'}
    assert row_dimensions(rule, row) == {'NAME': 't1'}


def test_single_column_binding_still_allows_missing_values():
    """单列绑定的老行为：取不到就是 ``None``（不抛异常）。"""
    rule = _rule({'NAME': 'relname'})
    assert row_dimensions(rule, {}) == {'NAME': None}


# -- 多列分组 -------------------------------------------------------------
def _rows():
    return [
        {'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'},
        {'schemaname': 'other', 'relname': 't1', 'n_tup_del': '5'},
    ]


def test_group_by_two_columns_splits_same_named_tables():
    rule = _rule({'NAME': 'schemaname.relname'})
    params = {'group_by': ['schemaname', 'relname']}
    items = grouped_items(rule, params, _rows())
    assert len(items) == 2
    assert sorted(dims['NAME'] for dims, _row in items) == ['other.t1', 'public.t1']


def test_group_by_two_columns_keeps_the_row_value():
    rule = _rule({'NAME': 'schemaname.relname'})
    params = {'group_by': ['schemaname', 'relname']}
    items = grouped_items(rule, params, _rows())
    values = dict((dims['NAME'], row['n_tup_del']) for dims, row in items)
    assert values == {'public.t1': '3', 'other.t1': '5'}


def test_group_by_must_cover_every_bound_column():
    """分组键缺少绑定里的列 → ``RuleError``（声明自相矛盾）。"""
    rule = _rule({'NAME': 'schemaname.relname'})
    params = {'group_by': ['relname']}
    with pytest.raises(RuleError):
        grouped_items(rule, params, _rows())


def test_single_column_group_by_still_works():
    rule = _rule({'NAME': 'relname'})
    params = {'group_by': ['relname']}
    items = grouped_items(rule, params, [{'relname': 't1'}, {'relname': 't2'}])
    assert sorted(dims['NAME'] for dims, _row in items) == ['t1', 't2']


# -- 幂等不碰撞（任务 1.6）------------------------------------------------
def _table_rule():
    """一条真实的表级规则（来自生效映射表），带组合绑定。"""
    path = os.path.join(
        REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'metric_rules.json')
    with io.open(path, encoding='utf-8') as handle:
        payload = json.load(handle)
    for rule in payload['rules']:
        if rule.get('dimension_bindings') == {'NAME': 'schemaname.relname'}:
            return MetricRule.from_dict(rule)
    raise AssertionError('生效映射表里没有表级规则')


def _field_map():
    with io.open(FIELD_MAP_PATH, encoding='utf-8') as handle:
        return json.load(handle)


def _collect(rule, rows, now_ms=1700000000000):
    """跑一次取值 → 出 [(维度, 值, 时间戳)]。"""
    ctx = RuleContext(rows=tuple(rows), now_ms=now_ms)
    return evaluate(rule, ctx)


def _messages(rule, rows, now_ms=1700000000000):
    """跑一次取值并组装成 ITSCADA 指标消息。"""
    field_map = _field_map()
    results = []
    for dims, value, timestamp in _collect(rule, rows, now_ms):
        message = build_metric_message(
            field_map, rule, dims, value, RESOURCE_CONTEXT, timestamp)
        results.append((dims, message, timestamp))
    return results


def test_same_named_tables_in_different_schemas_get_different_messages():
    rule = _table_rule()
    rows = [
        {'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'},
        {'schemaname': 'other', 'relname': 't1', 'n_tup_del': '5'},
    ]
    results = _messages(rule, rows)
    assert len(results) == 2
    names = set(message['metricName'] for _dims, message, _ts in results)
    codes = set(message['metricCode'] for _dims, message, _ts in results)
    assert len(names) == 2, '同名表不同 schema 的 metricName 撞了：%s' % names
    assert len(codes) == 2, '同名表不同 schema 的 metricCode 撞了：%s' % codes


def test_dimension_values_carry_the_schema_prefix():
    rule = _table_rule()
    rows = [
        {'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'},
        {'schemaname': 'other', 'relname': 't1', 'n_tup_del': '5'},
    ]
    results = _messages(rule, rows)
    values = set(
        message['tags']['dimension_values']['NAME']
        for _dims, message, _ts in results
    )
    assert values == {'public.t1', 'other.t1'}


def test_same_named_tables_in_different_schemas_get_different_idempotency_keys():
    rule = _table_rule()
    rows = [
        {'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'},
        {'schemaname': 'other', 'relname': 't1', 'n_tup_del': '5'},
    ]
    results = _messages(rule, rows)
    keys = set(
        metric_idempotency_key(
            RESOURCE_CONTEXT['instance_id'], message['metricName'], dims, timestamp)
        for dims, message, timestamp in results
    )
    assert len(keys) == 2, '同名表不同 schema 的幂等键撞了：%s' % keys


def test_the_same_table_collected_twice_gets_the_same_idempotency_key():
    """同一张表重复采集 → 幂等键相同（仍然去重，只投一条）。"""
    rule = _table_rule()
    rows = [{'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'}]
    first = _messages(rule, rows)
    second = _messages(rule, rows)
    def _key(result):
        return metric_idempotency_key(
            RESOURCE_CONTEXT['instance_id'], result[1]['metricName'],
            result[0], result[2])

    assert _key(first[0]) == _key(second[0])


def test_resource_name_does_not_contain_the_table_name():
    """表名不是资源名的一部分（资源名只表示节点）。"""
    rule = _table_rule()
    rows = [{'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'}]
    results = _messages(rule, rows)
    resource_name = results[0][1]['resourceName']
    assert 't1' not in resource_name
    assert 'public' not in resource_name
