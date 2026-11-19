# -*- coding: utf-8 -*-
"""取值规则引擎与 ITSCADA 消息组装。"""
import json
import os

import pytest

from conftest import DATA_DIR
from tpops_kafka_collector.mapping import (
    CounterDeltaStore,
    MappingTable,
    RuleContext,
    RuleError,
    build_alarm_message,
    build_metric_message,
    evaluate,
)
from tpops_kafka_collector.mapping.rules import build_resource_name, substitute_placeholders


@pytest.fixture(scope='module')
def table():
    return MappingTable.load()


# 节点清单样本（key-view-execute-node 的形状）：3 个 CN master + 各 DN
NODES = [
    {'node_id': '3881c04403394ee1b36a2bf873e69007no14', 'role': 'master',
     'type': 'CN', 'component_id': 'cn_5001', 'node_ip': '172.16.103.126'},
    {'node_id': '40abc563810e462a97b065e718189a59no14', 'role': 'master',
     'type': 'CN', 'component_id': 'cn_5002', 'node_ip': '172.16.103.127'},
    {'node_id': '5a3cd027ad5c4f9bbb67d728c6d279c0no14', 'role': 'master',
     'type': 'CN', 'component_id': 'cn_5003', 'node_ip': '172.16.103.128'},
    {'node_id': '3881c04403394ee1b36a2bf873e69007no14', 'role': 'master',
     'type': 'DN', 'component_id': 'dn_6001', 'node_ip': '172.16.103.126'},
]


def _rule(table, code):
    rule = table.metric_rule(code)
    assert rule is not None, code
    return rule


# -- 占位符与资源名 ------------------------------------------------------
def test_substitute_placeholders_replaces_dimension_values():
    assert substitute_placeholders(
        'GAUSSDB.[{#DATNAME}]_cache_hit_ratio', {'DATNAME': 'postgres'}
    ) == 'GAUSSDB.[postgres]_cache_hit_ratio'


def test_substitute_placeholders_replaces_chinese_name():
    assert substitute_placeholders(
        '[{#DATNAME}] 缓存命中率', {'DATNAME': 'postgres'}
    ) == '[postgres] 缓存命中率'


def test_substitute_placeholders_missing_dimension_raises():
    with pytest.raises(RuleError):
        substitute_placeholders('{#NAME}_status', {})


def test_build_resource_name_uses_template():
    context = {'host_ip': '172.16.103.151', 'node_id': 'd5284a09no14'}
    assert build_resource_name('{host_ip}_{node_id}', context) == '172.16.103.151_d5284a09no14'


def test_build_resource_name_missing_node_falls_back():
    context = {'host_ip': '172.16.103.151', 'node_id': None}
    assert build_resource_name('{host_ip}_{node_id}', context) == '172.16.103.151_'


def test_build_resource_name_uses_fallback_template_when_node_missing():
    context = {'host_ip': '172.16.103.151', 'node_id': None}
    rendered = build_resource_name(
        '{host_ip}_{node_id}', context, fallback_template='{host_ip}'
    )
    assert rendered == '172.16.103.151'


def test_build_resource_name_tries_fallback_templates_in_order():
    """回退模板给一串时，取第一个渲染出非空结果的。"""
    context = {'host_ip': None, 'node_id': 'node-1'}
    rendered = build_resource_name(
        '{host_ip}_{node_id}', context, fallback_template=['{host_ip}', '{node_id}']
    )
    assert rendered == 'node-1'


def test_metric_message_falls_back_to_master_node(table):
    """与节点无关的指标：resourceName 落到节点清单里的第一个 master CN。"""
    rule = _rule(table, 'gaussdb.server_version')
    message = build_metric_message(
        table.field_map,
        rule,
        {},
        'V2.0-10.0.0',
        {'instance_name': 'gauss-75a2'},
        1,
        nodes=NODES,
    )
    assert message['resourceName'] == '172.16.103.126_3881c04403394ee1b36a2bf873e69007no14'
    assert message['tags']['node_id'] == '3881c04403394ee1b36a2bf873e69007no14'


def test_metric_message_carries_resource_tags(table):
    rule = _rule(table, 'GAUSSDB.[{#DATNAME}]_cache_hit_ratio')
    message = build_metric_message(
        table.field_map,
        rule,
        {'DATNAME': 'postgres'},
        99.5,
        {
            'instance_id': 'iid',
            'instance_name': 'gauss-75a2',
            'node_id': 'node-1',
            'component_id': 'cn_5002',
            'host_ip': '172.16.103.151',
        },
        1793871051000,
    )
    assert message['tags']['instance_id'] == 'iid'
    assert message['tags']['node_id'] == 'node-1'
    assert message['tags']['dimension_values'] == {'DATNAME': 'postgres'}
    # resourceName 只表示节点 → 节点 IP + 节点 ID
    assert message['resourceName'] == '172.16.103.151_node-1'
    # 库名这类业务维度不再进资源名
    assert 'postgres' not in message['resourceName']


# -- 取值规则 ------------------------------------------------------------
def test_raw_sum_policy_adds_up_database_rows(table):
    rule = _rule(table, 'gaussdb.Numbackends')
    ctx = RuleContext(rows=({'numbackends': 3}, {'numbackends': 4}))
    assert evaluate(rule, ctx) == [({}, 7, ctx.now_ms)]


def test_raw_per_row_policy_emits_one_value_per_row(table):
    rule = _rule(table, 'GAUSSDB.[{#DATNAME}]_committed_transactions')
    ctx = RuleContext(rows=({'datname': 'postgres', 'xact_commit': 10}, {'datname': 'template1', 'xact_commit': 5}))
    assert evaluate(rule, ctx) == [
        ({'DATNAME': 'postgres'}, 10, ctx.now_ms),
        ({'DATNAME': 'template1'}, 5, ctx.now_ms),
    ]


def test_raw_filtered_picks_matching_row(table):
    rule = _rule(table, 'GAUSSDB.maximum_connection')
    ctx = RuleContext(rows=(
        {'name': 'max_connections', 'setting': '200'},
        {'name': 'port', 'setting': '5432'},
    ))
    assert evaluate(rule, ctx) == [({}, 200, ctx.now_ms)]


def test_aggregate_filters_by_state(table):
    rule = _rule(table, 'GAUSSDB.Idle_connections')
    ctx = RuleContext(rows=(
        {'state': 'idle', 'cnt': 4},
        {'state': 'active', 'cnt': 2},
        {'state': 'idle', 'cnt': 3},
    ))
    assert evaluate(rule, ctx) == [({}, 7, ctx.now_ms)]


def test_aggregate_groups_by_dimension(table):
    rule = _rule(table, 'gaussdb.locks.accessexclusive[{#DBNAME}]')
    ctx = RuleContext(rows=(
        {'datname': 'postgres', 'mode': 'AccessExclusiveLock', 'cnt': 2},
        {'datname': 'postgres', 'mode': 'AccessExclusiveLock', 'cnt': 1},
        {'datname': 'template1', 'mode': 'AccessExclusiveLock', 'cnt': 5},
    ))
    values = dict((dims['DBNAME'], value) for dims, value, _clock in evaluate(rule, ctx))
    assert values == {'postgres': 3, 'template1': 5}


def test_aggregate_contains_filter(table):
    rule = _rule(table, 'gaussdb.Waiting_conn')
    ctx = RuleContext(rows=(
        {'state': 'active', 'cnt': 1},
        {'state': 'waiting for lock', 'cnt': 4},
    ))
    assert evaluate(rule, ctx) == [({}, 4, ctx.now_ms)]


def test_counter_delta_first_round_has_no_value(table):
    rule = _rule(table, 'GAUSSDB.[{#DATNAME}]_insert_records_per_second')
    deltas = CounterDeltaStore()
    ctx = RuleContext(rows=({'datname': 'postgres', 'tup_inserted': 100},), now_ms=1000)
    result = evaluate(rule, ctx, deltas)
    assert result == [({'DATNAME': 'postgres'}, None, 1000)]


def test_counter_delta_computes_rate_on_second_round(table):
    rule = _rule(table, 'GAUSSDB.[{#DATNAME}]_insert_records_per_second')
    deltas = CounterDeltaStore()
    evaluate(rule, RuleContext(rows=({'datname': 'postgres', 'tup_inserted': 100},), now_ms=1000), deltas)
    result = evaluate(
        rule, RuleContext(rows=({'datname': 'postgres', 'tup_inserted': 160},), now_ms=61000), deltas
    )
    assert result == [({'DATNAME': 'postgres'}, 1.0, 61000)]


def test_counter_delta_drops_value_when_counter_resets(table):
    """实例重启导致累计值归零 → 丢弃本轮差分，不产生负速率。"""
    rule = _rule(table, 'GAUSSDB.[{#DATNAME}]_insert_records_per_second')
    deltas = CounterDeltaStore()
    evaluate(rule, RuleContext(rows=({'datname': 'postgres', 'tup_inserted': 100},), now_ms=1000), deltas)
    result = evaluate(
        rule, RuleContext(rows=({'datname': 'postgres', 'tup_inserted': 0},), now_ms=61000), deltas
    )
    assert result == [({'DATNAME': 'postgres'}, None, 61000)]


def test_counter_delta_without_store_raises(table):
    rule = _rule(table, 'GAUSSDB.[{#DATNAME}]_insert_records_per_second')
    with pytest.raises(RuleError):
        evaluate(rule, RuleContext(rows=({'tup_inserted': 1})), None)


def test_series_point_takes_last_non_null_value(table):
    rule = _rule(table, 'gaussdb.Tps')
    ctx = RuleContext(series={'cn_5002': [1.0, 2.0, None, 4.0]})
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_5002'}, 4.0, ctx.now_ms)]


def test_enum_map_translates_known_value(table):
    rule = _rule(table, 'GAUSSDB.Service_availability_testing')
    ctx = RuleContext(series={'cn_5002': [1.0]})
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_5002'}, '正常', ctx.now_ms)]


def test_enum_map_passes_unknown_value_through(table):
    rule = _rule(table, 'GAUSSDB.Service_availability_testing')
    ctx = RuleContext(series={'cn_5002': [7.0]})
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_5002'}, 7.0, ctx.now_ms)]


def test_gauge_computes_elapsed_seconds(table):
    rule = _rule(table, 'gaussdb.Uptimestr')
    ctx = RuleContext(series={'cn_5002': [1000.0]}, now_ms=61000 * 1000)
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_5002'}, 60000.0, ctx.now_ms)]


def test_top_n_keeps_sorted_rows(table):
    rule = _rule(table, 'GAUSSDB.sessions_active_top10')
    ctx = RuleContext(rows=(
        {'datname': 'postgres', 'usename': 'u1', 'query_start': 300},
        {'datname': 'postgres', 'usename': 'u2', 'query_start': 100},
    ))
    values = evaluate(rule, ctx)
    assert len(values) == 1
    rows = values[0][1]
    assert [row['usename'] for row in rows] == ['u2', 'u1']


def test_raw_rows_returns_all_rows(table):
    rule = _rule(table, 'gaussdb.get.dbstat.sum')
    ctx = RuleContext(rows=({'a': 1}, {'a': 2}))
    assert evaluate(rule, ctx) == [({}, [{'a': 1}, {'a': 2}], ctx.now_ms)]


def test_collection_count_counts_slow_sql_entries(table):
    rule = _rule(table, 'GAUSSDB.Total_slow_queries')
    ctx = RuleContext(collections={'slow_sql_infos': ({'sql_id': '1'}, {'sql_id': '2'})})
    assert evaluate(rule, ctx) == [({}, 2, ctx.now_ms)]


def test_text_prefix_count_classifies_select(table):
    rule = _rule(table, 'GAUSSDB.Select_slow_queries')
    ctx = RuleContext(collections={'slow_sql_infos': (
        {'sql_text': 'SELECT * FROM t'},
        {'sql_text': 'with x as (select 1) select * from x'},
        {'sql_text': 'INSERT INTO t VALUES (1)'},
    )})
    assert evaluate(rule, ctx) == [({}, 2, ctx.now_ms)]


def test_ratio_across_two_sources(table):
    rule = _rule(table, 'GAUSSDB.session_usage')
    ctx = RuleContext(rows_by_key={
        'activity_agg': ({'cnt': 5}, {'cnt': 5}),
        'stat_database': ({'numbackends': 20},),
    })
    assert evaluate(rule, ctx) == [({}, 0.5, ctx.now_ms)]


def test_unknown_rule_type_raises(table):
    rule = _rule(table, 'gaussdb.Tps')
    broken = type(rule)(**dict(
        (field, getattr(rule, field)) for field in rule.__dataclass_fields__
    ))
    object.__setattr__(broken, 'value_rule', {'type': '不存在的类型'})
    with pytest.raises(RuleError):
        evaluate(broken, RuleContext())


# -- 消息组装 ------------------------------------------------------------
def test_metric_message_has_exactly_the_documented_fields(table):
    rule = _rule(table, 'GAUSSDB.Number_of_ansactions_mmitted_r_cond')
    message = build_metric_message(
        table.field_map,
        rule,
        {},
        12,
        {'instance_name': 'gauss-75a2', 'node_id': 'node-1',
         'host_ip': '172.16.103.151'},
        1793871051000,
    )
    assert set(message) == {
        'metricName', 'metricCode', 'resourceName', 'value', 'unit', 'clock', 'tags',
    }
    assert message['metricName'] == '每秒提交事务数'
    assert message['metricCode'] == 'GAUSSDB.Number_of_ansactions_mmitted_r_cond'
    # A 类监控指标的数据按组件（节点）展开 → 资源名指向那个节点
    assert message['resourceName'] == '172.16.103.151_node-1'
    assert message['value'] == 12
    assert message['unit'] == 'tps'
    assert message['clock'] == 1793871051000


def test_metric_message_substitutes_placeholders(table):
    rule = _rule(table, 'GAUSSDB.[{#DATNAME}]_cache_hit_ratio')
    message = build_metric_message(
        table.field_map,
        rule,
        {'DATNAME': 'postgres'},
        99.5,
        {'instance_name': 'gauss-75a2', 'node_id': 'node-1',
         'host_ip': '172.16.103.151'},
        1793871051000,
    )
    assert message['metricName'] == '[postgres]缓存命中率'
    assert message['metricCode'] == 'GAUSSDB.[postgres]_cache_hit_ratio'
    # 库级维度不进资源名（resourceName 只能表示节点），走 metricName/tags
    assert message['resourceName'] == '172.16.103.151_node-1'
    assert message['tags']['dimension_values'] == {'DATNAME': 'postgres'}


def test_metric_message_keeps_missing_value_as_null(table):
    rule = _rule(table, 'gaussdb.Tps')
    message = build_metric_message(
        table.field_map, rule, {}, None, {'instance_name': 'i'}, 1
    )
    assert message['value'] is None


def test_metric_message_serializes_value_by_customer_type(table):
    rule = _rule(table, 'gaussdb.QPS')  # 浮点
    message = build_metric_message(
        table.field_map, rule, {}, 12.3456, {'instance_name': 'i'}, 1
    )
    assert message['value'] == 12.35


def test_alarm_message_has_exactly_the_documented_fields(table):
    definition = table.alarm_definition('2007010001')
    record = {
        'alarm_id': '2007010001',
        'alarm_name': '节点NTP状态异常',
        'severity': 'major',
        'status': 'activated',
        'first_alarm_time': 1793758083000,
        'alarm_items_sum': 2,
        'instance_id': 'iid',
        'instance_name': 'gauss-6374',
        'node_id': 'node-1',
        'host_ip': '172.16.103.151',
        'description': '172.16.103.128节点NTP服务状态异常。',
    }
    message = build_alarm_message(
        table.field_map,
        record,
        definition,
        {'instance_name': 'gauss-6374', 'node_id': 'node-1',
         'host_ip': '172.16.103.151'},
        table.alarm_rules['severity_to_priority'],
        table.alarm_rules['status_to_itscada'],
    )
    assert set(message) == {
        'alarmId', 'alarmName', 'alarmContent', 'priority', 'alarmTime', 'status',
        'recoverTime', 'resourceName', 'metricName', 'metricCode', 'value', 'unit', 'tags',
    }
    assert message['alarmId'] == '2007010001'
    assert message['priority'] == 2
    assert message['status'] == 1
    assert message['recoverTime'] is None
    assert message['metricCode'] == 'gaussdb.alarm.2007010001'
    assert message['metricName'] == '节点NTP状态异常'
    assert message['value'] == '2'
    assert message['resourceName'] == '172.16.103.151_node-1'


def test_alarm_message_without_node_falls_back_to_master_node(table):
    """收敛条目不带节点 → 落到节点清单里的第一个 master CN。"""
    definition = table.alarm_definition('2007010001')
    record = {
        'alarm_id': '2007010001',
        'alarm_name': '节点NTP状态异常',
        'severity': 'major',
        'status': 'activated',
        'first_alarm_time': 1793758083000,
        'alarm_items_sum': 2,
        'instance_id': 'iid',
        'instance_name': 'gauss-6374',
    }
    message = build_alarm_message(
        table.field_map,
        record,
        definition,
        {'instance_name': 'gauss-6374'},
        table.alarm_rules['severity_to_priority'],
        table.alarm_rules['status_to_itscada'],
        nodes=NODES,
    )
    assert message['resourceName'] == '172.16.103.126_3881c04403394ee1b36a2bf873e69007no14'


def test_alarm_message_recovered_carries_recover_time(table):
    definition = table.alarm_definition('2007010001')
    record = {
        'alarm_id': '2007010001',
        'alarm_name': '节点NTP状态异常',
        'severity': 'critical',
        'status': 'recovered',
        'first_alarm_time': 1793758083000,
        'clear_alarm_time': 1793771169000,
        'alarm_items_sum': 1,
        'instance_name': 'gauss-6374',
    }
    message = build_alarm_message(
        table.field_map,
        record,
        definition,
        {'instance_name': 'gauss-6374'},
        table.alarm_rules['severity_to_priority'],
        table.alarm_rules['status_to_itscada'],
    )
    assert message['status'] == 0
    assert message['recoverTime'] == 1793771169000
    assert message['priority'] == 1


def test_alarm_message_uses_linked_customer_metric(table):
    """能对应到客户指标的告警，metricCode 用客户指标英文名。"""
    definition = table.alarm_definition('2001050018')
    assert definition.linked_customer_metric == 'gaussdb.Tps'
    assert definition.value_source == 'latest_metric_value'
    message = build_alarm_message(
        table.field_map,
        {
            'alarm_id': '2001050018',
            'alarm_name': '每秒处理的事务数过多',
            'severity': 'major',
            'status': 'activated',
            'first_alarm_time': 1,
            'instance_name': 'gauss-6374',
        },
        definition,
        {'instance_name': 'gauss-6374'},
        table.alarm_rules['severity_to_priority'],
        table.alarm_rules['status_to_itscada'],
        metric_value=42.5,
    )
    assert message['metricCode'] == 'gaussdb.Tps'
    assert message['value'] == '42.5'


# -- 与实测样本对拍 ------------------------------------------------------
def test_series_point_matches_real_response_sample(table):
    """用 data/metric_response 的原生响应回放 A 类取值。"""
    path = os.path.join(
        DATA_DIR, 'metric_response', '001_GAUSSDB.Number_of_ansactions_mmitted_r_cond.json'
    )
    sample = json.load(open(path, encoding='utf-8'))
    rule = _rule(table, 'GAUSSDB.Number_of_ansactions_mmitted_r_cond')
    checked = 0
    for call in sample:
        body = call.get('response_body') or {}
        for info in body.get('metric_infos') or []:
            for metric in info.get('metrics') or []:
                ctx = RuleContext(series=metric['series'])
                values = evaluate(rule, ctx)
                assert values, metric.get('name')
                for _dims, value, _clock in values:
                    assert isinstance(value, float)
                checked += 1
    assert checked, '样本里没有可回放的指标点'


def test_dimension_metrics_are_always_per_row(table):
    """带维度的指标必须逐行取值，否则只会发出第一条。"""
    for rule in table.enabled_metric_rules():
        if not rule.dimension_bindings:
            continue
        value_rule = rule.value_rule or {}
        if value_rule.get('type') not in ('raw', 'counter_delta'):
            continue
        params = value_rule.get('params', {})
        assert params.get('row_policy') == 'per_row', rule.metric_code
        assert params.get('group_by'), rule.metric_code


def test_ratio_metrics_are_per_row(table):
    """比率列是按行算出来的，带维度的必须逐行取。"""
    for code in (
        'GAUSSDB.[{#DATNAME}]_cache_hit_ratio',
        'GAUSSDB.{#NAME}.garbage.Ratio',
        'gaussdb.{#NAME}.garbage.Ratio',
    ):
        rule = _rule(table, code)
        assert rule.value_rule['params']['row_policy'] == 'per_row', code
        assert rule.value_rule['params']['column'] in ('cache_hit_ratio', 'dead_tup_ratio')


def test_ratio_metric_emits_one_value_per_database(table):
    rule = _rule(table, 'GAUSSDB.[{#DATNAME}]_cache_hit_ratio')
    ctx = RuleContext(rows=(
        {'datname': 'postgres', 'cache_hit_ratio': 99.5},
        {'datname': 'template1', 'cache_hit_ratio': 100.0},
    ))
    assert evaluate(rule, ctx) == [
        ({'DATNAME': 'postgres'}, 99.5, ctx.now_ms),
        ({'DATNAME': 'template1'}, 100.0, ctx.now_ms),
    ]


def test_command_center_rows_match_real_probe_result(table):
    """用 data/merged_sql/probe_result.json 的实测列校验 raw 规则列名。"""
    catalog = json.load(
        open(os.path.join(DATA_DIR, 'merged_sql', 'merged_sql.json'), encoding='utf-8')
    )
    columns = {}
    for batch in catalog['batches']:
        for verified in batch.get('verified', {}).values():
            columns[batch['sql_key']] = verified['columns']
            break
    for rule in table.enabled_metric_rules():
        sql_key = rule.source.get('sql_key')
        if not sql_key or not rule.value_rule:
            continue
        rule_type = rule.value_rule['type']
        if rule_type not in ('raw', 'counter_delta'):
            continue
        column = rule.value_rule['params'].get('column')
        assert column in columns[sql_key], (rule.metric_code, column, sql_key)
