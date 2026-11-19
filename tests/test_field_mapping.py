# -*- coding: utf-8 -*-
"""分类字段映射表：必须与 ITSCADA 文档、与 TPOPS 实测原始结构对得上。"""
import io
import json
import os
import re
import sys

import pytest

from conftest import DATA_DIR, REPO_ROOT
from tpops_kafka_collector.mapping import MappingError, MappingTable, RuleContext, evaluate

sys.path.insert(0, os.path.join(REPO_ROOT, 'tools'))

import build_field_mapping_doc as doc_builder  # noqa: E402

MAPPING_DIR = os.path.join(DATA_DIR, 'mapping')
DOC_PATH = os.path.join(REPO_ROOT, 'docs', '映射表-ITSCADA字段对照.md')

# ITSCADA 两份方案的字段表原文
DOC_METRIC_FIELDS = [
    'metricName', 'metricCode', 'resourceName', 'value', 'unit', 'clock', 'tags',
]
DOC_ALARM_FIELDS = [
    'alarmId', 'alarmName', 'alarmContent', 'priority', 'alarmTime', 'status',
    'recoverTime', 'resourceName', 'metricName', 'metricCode', 'value', 'unit', 'tags',
]
CATEGORIES = ['monitor_metrics', 'command_center_sql', 'diagnostic', 'instance']


@pytest.fixture(scope='module')
def table():
    return MappingTable.load()


# 分类映射表已归档（2026-11-13），这里仍校验它们与客户方案字段表一致
MAPPING_FILES = {
    'monitor_metrics': 'archive/metric_mapping_monitor_metrics.json',
    'command_center_sql': 'archive/metric_mapping_command_center_sql.json',
    'diagnostic': 'archive/metric_mapping_diagnostic.json',
    'instance': 'archive/metric_mapping_instance.json',
    'alarm': 'archive/alarm_mapping.json',
}


def _mapping(category):
    path = os.path.join(MAPPING_DIR, MAPPING_FILES[category])
    with io.open(path, encoding='utf-8') as handle:
        return json.load(handle)


def test_each_category_maps_all_seven_customer_fields():
    for category in CATEGORIES:
        mapping = _mapping(category)
        names = [f['customer_field'] for f in mapping['field_mapping']]
        assert names == DOC_METRIC_FIELDS, category


def test_alarm_maps_all_thirteen_customer_fields():
    mapping = _mapping('alarm')
    names = [f['customer_field'] for f in mapping['field_mapping']]
    assert names == DOC_ALARM_FIELDS


def test_each_mapping_declares_tpops_fields_and_transforms():
    for category in CATEGORIES + ['alarm']:
        mapping = _mapping(category)
        assert mapping['tpops_fields'], category
        for item in mapping['tpops_fields']:
            assert item['field'] and item['desc'], (category, item)
        for item in mapping['field_mapping']:
            assert item['source'], (category, item['customer_field'])
            assert item['transform'], (category, item['customer_field'])
            assert item['source_type'] in ('tpops', 'mapping_table', 'assembled')


def test_metric_name_and_code_come_from_mapping_table_not_tpops():
    """metricName/metricCode 用的是客户指标表，不能拿 TPOPS 的平台中文名。"""
    for category in CATEGORIES:
        mapping = _mapping(category)
        for name in ('metricName', 'metricCode'):
            field = [f for f in mapping['field_mapping'] if f['customer_field'] == name][0]
            assert field['source_type'] == 'mapping_table', (category, name)
            assert 'metric_rules' in str(field['source'])
            assert field['source_field'] == name.replace('metricName', 'metric_name').replace('metricCode', 'metric_code')
            assert field['source_lookup']


def test_command_center_value_mentions_result_parsing():
    mapping = _mapping('command_center_sql')
    field = [f for f in mapping['field_mapping'] if f['customer_field'] == 'value'][0]
    assert 'result' in str(field['source'])
    assert '字符串' in field['transform']


def test_monitor_value_uses_series_and_clock_uses_same_index():
    mapping = _mapping('monitor_metrics')
    value = [f for f in mapping['field_mapping'] if f['customer_field'] == 'value'][0]
    clock = [f for f in mapping['field_mapping'] if f['customer_field'] == 'clock'][0]
    assert 'series' in str(value['source'])
    assert 'timestamps' in str(clock['source'])
    assert '同一下标' in clock['transform']


def _sources(item):
    source = item.get('source')
    if isinstance(source, (list, tuple)):
        return list(source)
    return [source]


def test_alarm_raw_fields_exist_in_captured_alarm_data():
    """映射表里引用的原始字段名，必须在实测告警数据里真实存在。"""
    known = set()
    for name in ('active.json', 'active_detail.json', 'history.json', 'history_detail.json'):
        records = json.load(
            io.open(os.path.join(DATA_DIR, 'alarm', name), encoding='utf-8')
        )['records']
        for record in records:
            known.update(record)

    mapping = _mapping('alarm')
    for field in mapping['field_mapping']:
        for source in _sources(field):
            for name in re.findall(r'records\[\]\.(\w+)', source):
                assert name in known, (field['customer_field'], name)


def test_alarm_tpops_fields_exist_in_captured_alarm_data():
    known = set()
    for name in ('active.json', 'active_detail.json', 'history.json', 'history_detail.json'):
        records = json.load(
            io.open(os.path.join(DATA_DIR, 'alarm', name), encoding='utf-8')
        )['records']
        for record in records:
            known.update(record)

    mapping = _mapping('alarm')
    for item in mapping['tpops_fields']:
        for name in re.findall(r'records\[\]\.(\w+)', item['field']):
            assert name in known, item['field']


def test_series_point_clock_is_the_timestamp_of_the_picked_point():
    """A 类：clock 必须取与 value 同一下标的时间戳，不能用最后一个时间戳。"""
    table = MappingTable.load()
    rule = table.metric_rule('gaussdb.Tps')
    timestamps = (1000, 2000, 3000)
    ctx = RuleContext(
        series={'cn_5002': [1.0, 2.0, None]},
        timestamps=timestamps,
        now_ms=9999,
    )
    results = evaluate(rule, ctx)
    assert results == [({'COMPONENT_ID': 'cn_5002'}, 2.0, 2000)]


def test_series_point_clock_falls_back_to_collect_time_without_timestamps():
    table = MappingTable.load()
    rule = table.metric_rule('gaussdb.Tps')
    ctx = RuleContext(series={'cn_5002': [1.0]}, now_ms=9999)
    results = evaluate(rule, ctx)
    assert results == [({'COMPONENT_ID': 'cn_5002'}, 1.0, 9999)]


def test_table_exposes_category_mappings(table):
    for category in CATEGORIES + ['alarm']:
        assert table.category_mapping(category)['field_mapping']
    with pytest.raises(MappingError):
        table.category_mapping('不存在的分类')


def test_command_center_rows_are_strings_in_real_data():
    """命令中心返回的行值是字符串 —— 映射表里写明了，转换规则要能处理。"""
    path = os.path.join(
        DATA_DIR, 'metric_response', '006_GAUSSDB.NAME.Number.t.date.ples.json'
    )
    calls = json.load(io.open(path, encoding='utf-8'))
    results = [
        (call.get('response_body') or {}).get('result')
        for call in calls
        if 'result' in (call.get('response_body') or {})
    ]
    assert results, '样本里没有命令中心的结果行'
    for result in results:
        assert isinstance(result, str)
        assert isinstance(json.loads(result), list)


def test_generated_doc_is_up_to_date():
    """docs/映射表-ITSCADA字段对照.md 必须与分类映射文件一致。"""
    if not os.path.isfile(DOC_PATH):
        pytest.skip('对照文档尚未生成')
    with io.open(DOC_PATH, encoding='utf-8') as handle:
        stored = handle.read()
    lines = [
        '# ITSCADA 字段映射表',
        '',
        '> **开发的直接依据**：左边是 TPOPS 取回来的原始字段，右边是客户 ITSCADA 要求的 Kafka 字段。',
        '> 指标按采集分类分节，告警单独一节。',
        '> 数据源：`data/mapping/` 下的分类映射文件（本文件由 '
        '`tools/build_field_mapping_doc.py` 生成，勿手改）。',
        '',
        '依据文档：`docs/ITSCADA第三方指标数据接入对接方案V2.docx`、'
        '`docs/ITSCADA第三方告警数据接入对接方案V2.docx`。',
        '',
        '## 1. 指标字段映射表',
        '',
    ]
    for filename, title in doc_builder.METRIC_FILES:
        lines.extend(doc_builder.render_metric_section(filename, title))
    lines.append('## 2. 告警字段映射表')
    lines.append('')
    lines.extend(doc_builder.render_alarm_section())
    lines.append('## 3. 待客户确认')
    lines.append('')
    lines.extend(doc_builder.render_open_items())
    assert stored == '\n'.join(lines) + '\n'
