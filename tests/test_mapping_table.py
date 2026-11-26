# -*- coding: utf-8 -*-
"""映射表加载与查表。"""
import json
import os

import pytest

from conftest import DATA_DIR, MAPPING_DIR
from tpops_kafka_collector.mapping import MappingTable

# 两份 ITSCADA 对接方案里的字段表（原文抄录，作为断言基准）。
DOC_METRIC_FIELDS = [
    'metricName', 'metricCode', 'resourceName', 'value', 'unit', 'clock', 'tags',
]
DOC_ALARM_FIELDS = [
    'alarmId', 'alarmName', 'alarmContent', 'priority', 'alarmTime', 'status',
    'recoverTime', 'resourceName', 'metricName', 'metricCode', 'value', 'unit', 'tags',
]


@pytest.fixture(scope='module')
def table():
    return MappingTable.load(MAPPING_DIR)


def test_metric_rules_cover_the_effective_customer_metrics(table):
    """生效口径 = 87 条（客户指标库 101 条 − 排除清单 14 条）。"""
    assert len(table.metric_rules) == 87
    assert len(table.enabled_metric_rules()) == 87


def test_metric_rules_match_customer_xlsx_names(table):
    """指标英文名/中文名必须与客户指标表逐字一致。"""
    import openpyxl

    workbook = openpyxl.load_workbook(
        os.path.join(
            os.path.dirname(DATA_DIR),
            '高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx',
        ),
        data_only=True,
    )
    sheet = workbook['指标库']
    expected = {}
    for row in list(sheet.iter_rows(values_only=True))[1:]:
        if row[3]:
            expected[row[3]] = row[2]
    assert len(expected) == 101
    for rule in table.metric_rules:
        assert rule.metric_name == expected[rule.metric_code]


def test_metric_field_set_matches_document(table):
    assert [f['name'] for f in table.message_fields('metric')] == DOC_METRIC_FIELDS


def test_alarm_field_set_matches_document(table):
    assert [f['name'] for f in table.message_fields('alarm')] == DOC_ALARM_FIELDS


def test_required_fields_are_declared(table):
    assert set(table.required_fields('metric')) == {
        'metricName', 'metricCode', 'resourceName', 'value', 'clock',
    }
    assert 'alarmId' in table.required_fields('alarm')
    assert 'recoverTime' not in table.required_fields('alarm')


def test_unknown_message_type_raises(table):
    from tpops_kafka_collector.mapping import MappingError

    with pytest.raises(MappingError):
        table.message_fields('unknown')


def test_alarm_definitions_cover_both_catalogs(table):
    metric_catalog = json.load(
        open(os.path.join(DATA_DIR, 'alarm', 'catalog_metric.json'), encoding='utf-8')
    )
    event_catalog = json.load(
        open(os.path.join(DATA_DIR, 'alarm', 'catalog_event.json'), encoding='utf-8')
    )
    expected = set()
    for catalog in (metric_catalog, event_catalog):
        for item in catalog['items']:
            expected.add(str(item['告警ID']))
    assert len(expected) == 257
    for alarm_id in expected:
        assert table.alarm_definition(alarm_id) is not None


def test_alarm_definition_missing_returns_none(table):
    assert table.alarm_definition('不存在的告警ID') is None


def test_severity_and_status_maps_cover_platform_values(table):
    assert table.severity_to_priority('critical') == 1
    assert table.severity_to_priority('major') == 2
    assert table.severity_to_priority('minor') == 3
    assert table.severity_to_priority('notice') == 4
    assert table.severity_to_priority('unknown') is None
    assert table.status_to_itscada('activated') == 1
    assert table.status_to_itscada('recovered') == 0
    assert table.status_to_itscada('cleared') == 0


def test_metrics_of_sql_key_partition_command_center_metrics(table):
    """82 个命令中心指标应能被 11 条合并 SQL 全量覆盖，且互不重叠。"""
    command_center = table.metrics_of_channel('command_center_sql')
    assert len(command_center) == 71
    covered = []
    for sql_key in (
        'stat_database', 'stat_user_tables', 'activity_agg', 'activity_running',
        'activity_top10', 'locks', 'bgwriter', 'settings', 'database',
        'prepared_xacts', 'dbstat_sum',
    ):
        covered.extend(table.metrics_of_sql_key(sql_key))
    assert len(covered) == 71
    assert len(set(rule.metric_code for rule in covered)) == 71


def test_unsupported_metrics_are_excluded_not_disabled(table):
    """3 条无法提供的指标**不在生效映射表里**（构建期就被排除了）。

    它们改由排除清单记录（``reason=unsupported`` + 理由）——
    生效映射表里不再有 ``enabled=false`` 的规则。
    """
    unsupported = [
        rule for rule in table.metric_rules if not rule.enabled
    ]
    assert not unsupported, '生效映射表里不该有 enabled=false 的规则'
    codes = set(rule.metric_code for rule in table.metric_rules)
    assert not codes & {
        'gaussdb.Fastpath_function_call',
        'gaussdb.Disabled_conn',
        'gaussdb.sendAgentName',
    }
