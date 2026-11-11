#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""由 data/mapping/ 下的分类映射文件渲染可读的字段对照表。

输出 docs/映射表-ITSCADA字段对照.md：
  1. 指标字段映射表（按采集分类分节：监控指标接口 / 命令中心 SQL / 诊断优化 / 实例接口）
  2. 告警字段映射表

用法::

    python tools/build_field_mapping_doc.py
"""
from __future__ import print_function

import io
import json
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAPPING_DIR = os.path.join(BASE_DIR, 'data', 'mapping')
OUT_PATH = os.path.join(BASE_DIR, 'docs', '映射表-ITSCADA字段对照.md')

METRIC_FILES = [
    ('metric_mapping_monitor_metrics.json', 'A 类　监控指标接口'),
    ('metric_mapping_command_center_sql.json', 'B 类　命令中心 SQL'),
    ('metric_mapping_diagnostic.json', 'C 类　诊断优化接口'),
    ('metric_mapping_instance.json', 'D 类　实例接口'),
]
ALARM_FILE = 'alarm_mapping.json'
OPEN_ITEMS_FILE = 'open_items.json'
REQUIRED_LABEL = {True: '必填', False: '选填', 'conditional': '条件必填'}
SOURCE_TYPE_LABEL = {
    'tpops': 'TPOPS 字段',
    'mapping_table': '映射表',
    'assembled': '组装',
}


def _read_json(name):
    with io.open(os.path.join(MAPPING_DIR, name), encoding='utf-8') as handle:
        return json.load(handle)


def _cell(text):
    if text is None:
        return '—'
    return str(text).replace('|', '\\|').replace('\n', '<br>')


def _render_tpops_fields(items):
    has_record_type = any('in' in item for item in items)
    lines = ['**TPOPS 侧取到的字段**', '']
    if has_record_type:
        lines.append('| 字段 | 记录类型 | 含义 |')
        lines.append('|---|---|---|')
        for item in items:
            lines.append('| `%s` | %s | %s |' % (
                _cell(item['field']), _cell(item.get('in', '')), _cell(item['desc'])))
    else:
        lines.append('| 字段 | 含义 |')
        lines.append('|---|---|')
        for item in items:
            lines.append('| `%s` | %s |' % (_cell(item['field']), _cell(item['desc'])))
    lines.append('')
    return lines


def _render_field_mapping(items):
    has_source_in = any('source_in' in item for item in items)
    has_lookup = any(item.get('source_lookup') for item in items)
    lines = ['**映射到客户 Kafka 字段**', '']
    header = ['客户字段', '类型', '必填', '来源']
    divider = ['---'] * len(header)
    if has_source_in:
        header.append('取自哪种记录')
        divider.append('---')
    header += ['原始字段 / 取哪个字段']
    divider.append('---')
    if has_lookup:
        header.append('怎么定位')
        divider.append('---')
    header.append('转换规则')
    divider.append('---')
    lines.append('| ' + ' | '.join(header) + ' |')
    lines.append('| ' + ' | '.join(divider) + ' |')
    for item in items:
        source = item.get('source')
        if isinstance(source, (list, tuple)):
            source = ' / '.join(source)
        source_field = item.get('source_field')
        if source_field:
            source = '%s → %s' % (source, source_field)
        row = [
            '`%s`' % item['customer_field'],
            item.get('customer_type', ''),
            REQUIRED_LABEL.get(item.get('required'), item.get('required')),
            SOURCE_TYPE_LABEL.get(item.get('source_type'), item.get('source_type')),
        ]
        if has_source_in:
            row.append(_cell(item.get('source_in', '')))
        row.append('`%s`' % _cell(source))
        if has_lookup:
            row.append(_cell(item.get('source_lookup', '')))
        row.append(_cell(item.get('transform')))
        lines.append('| ' + ' | '.join(row) + ' |')
    lines.append('')
    return lines


def render_metric_section(filename, title):
    payload = _read_json(filename)
    lines = ['#### %s' % title, '']
    lines.append('接口：`%s`' % payload.get('api'))
    lines.append('')
    lines.extend(_render_tpops_fields(payload['tpops_fields']))
    lines.extend(_render_field_mapping(payload['field_mapping']))
    return lines


def render_alarm_section():
    payload = _read_json(ALARM_FILE)
    lines = ['#### 告警', '']
    lines.append('接口：`%s`' % payload.get('api'))
    lines.append('')
    for key, item in payload.get('records', {}).items():
        lines.append('- **%s**（请求参数 `%s`）：%s' % (key, item['request'], item['desc']))
    lines.append('')
    if payload.get('note'):
        lines.append('> %s' % payload['note'])
        lines.append('')
    for key, value in payload.get('raw_sources', {}).items():
        lines.append('- %s：%s' % (key, value))
    lines.append('')
    lines.extend(_render_tpops_fields(payload['tpops_fields']))
    lines.extend(_render_field_mapping(payload['field_mapping']))
    unused = payload.get('unused_raw_fields')
    if unused:
        lines.append('**ITSCADA 字段表里没有的原始字段**（统一放进 `tags.details`）')
        lines.append('')
        lines.append('| 字段 | 处置 |')
        lines.append('|---|---|')
        for name, note in unused.items():
            lines.append('| `%s` | %s |' % (name, _cell(note)))
        lines.append('')
    return lines


def render_open_items():
    payload = _read_json(OPEN_ITEMS_FILE)
    lines = ['## 待客户确认', '']
    lines.append('| # | 事项 | 当前默认 | 说明 |')
    lines.append('|---|---|---|---|')
    for index, item in enumerate(payload['items'], 1):
        lines.append('| %d | %s | %s | %s |' % (
            index, _cell(item['topic']), _cell(item['default']), _cell(item['detail'])
        ))
    lines.append('')
    return lines


def main():
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
    for filename, title in METRIC_FILES:
        lines.extend(render_metric_section(filename, title))
    lines.append('## 2. 告警字段映射表')
    lines.append('')
    lines.extend(render_alarm_section())
    lines.append('## 3. 待客户确认')
    lines.append('')
    lines.extend(render_open_items())

    with io.open(OUT_PATH, 'w', encoding='utf-8') as handle:
        handle.write('\n'.join(lines))
        handle.write('\n')
    print('wrote %s' % OUT_PATH)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
