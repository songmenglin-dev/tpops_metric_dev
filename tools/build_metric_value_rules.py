#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从开发方案 §4.3 的逐指标表生成 data/metric_value_rules.json。

开发方案里的「§4.3 指标 → 取数方法映射」是人工维护的权威表，
本脚本把其中三张表（A/B/C/D 类）解析成机器可读的取值规则，
避免映射规则文件靠人工转抄。

用法::

    python tools/build_metric_value_rules.py
"""
from __future__ import print_function

import io
import json
import os
import re
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLAN_PATH = os.path.join(BASE_DIR, 'docs', '开发方案-GaussDB指标与告警采集.md')
OUT_PATH = os.path.join(BASE_DIR, 'data', 'metric_value_rules.json')

# 开发方案 §4.3.2 的 SQL 序号 → 合并 SQL 的 sql_key（§4.4 目录）。
SQL_KEY_BY_NUMBER = {
    'SQL-1': 'stat_database',
    'SQL-2': 'stat_user_tables',
    'SQL-3': 'activity_agg',
    'SQL-4': 'activity_running',
    'SQL-5': 'locks',
    'SQL-6': 'bgwriter',
    'SQL-7': 'settings',
    'SQL-8': 'database',
    'SQL-9': 'prepared_xacts',
    'SQL-10': 'dbstat_sum',
}

# §4.3.2 把「会话明细」3 个指标归在 SQL-4，§4.4 拆成两条 SQL；
# get.connections.sum 归入 activity_agg（其 covers 文本注明「含 get.connections.sum 全量行」）。
SQL_KEY_OVERRIDE = {
    'GAUSSDB.sessions_active_top10': 'activity_top10',
    'gaussdb.running.sql.limit.20': 'activity_running',
    'gaussdb.get.connections.sum': 'activity_agg',
}

BACKTICK_RE = re.compile(r'^`(.+)`$')
NO_DIM = '—'


def _read_plan():
    with io.open(PLAN_PATH, encoding='utf-8') as handle:
        return handle.read()


def _section(text, start_marker, end_marker):
    """截取两个标题之间的正文。"""
    start = text.index(start_marker)
    end = text.index(end_marker, start)
    return text[start:end]


def _cells(line):
    """把一行 markdown 表格切成单元格列表。"""
    stripped = line.strip()
    if not stripped.startswith('|') or not stripped.endswith('|'):
        return None
    return [cell.strip() for cell in stripped[1:-1].split('|')]


def _is_metric_cell(cell):
    match = BACKTICK_RE.match(cell)
    return match is not None


def _parse_a_class(text):
    """§4.3.1 A 类：监控指标接口。"""
    rows = []
    body = _section(text, '#### 4.3.1', '#### 4.3.2')
    for line in body.splitlines():
        cells = _cells(line)
        if not cells or len(cells) != 5:
            continue
        if not _is_metric_cell(cells[0]):
            continue
        rows.append({
            'customer_metric': BACKTICK_RE.match(cells[0]).group(1),
            'customer_metric_name': cells[1],
            'tpops_metric': BACKTICK_RE.match(cells[2]).group(1) if _is_metric_cell(cells[2]) else cells[2],
            'match_level': cells[3],
            'plan_value_expr': cells[4],
        })
    return rows


def _parse_b_class(text):
    """§4.3.2 B 类：命令中心 SQL（明细表在 <details> 里）。"""
    rows = []
    body = _section(text, '#### 4.3.2', '#### 4.3.3')
    for line in body.splitlines():
        cells = _cells(line)
        if not cells or len(cells) != 7:
            continue
        if not cells[0].startswith('SQL-'):
            continue
        if not _is_metric_cell(cells[1]):
            continue
        rows.append({
            'customer_metric': BACKTICK_RE.match(cells[1]).group(1),
            'customer_metric_name': cells[2],
            'sql_number': cells[0],
            'source_expr': BACKTICK_RE.match(cells[3]).group(1) if _is_metric_cell(cells[3]) else cells[3],
            'plan_rule': BACKTICK_RE.match(cells[4]).group(1) if _is_metric_cell(cells[4]) else cells[4],
            'dimensions': [] if cells[5] == NO_DIM else [d.strip() for d in cells[5].split(',') if d.strip()],
            'plan_note': cells[6],
        })
    return rows


def _parse_c_class(text):
    """§4.3.3 C 类：诊断优化接口。"""
    rows = []
    body = _section(text, '#### 4.3.3', '#### 4.3.4')
    for line in body.splitlines():
        cells = _cells(line)
        if not cells or len(cells) != 5:
            continue
        if not _is_metric_cell(cells[0]):
            continue
        rows.append({
            'customer_metric': BACKTICK_RE.match(cells[0]).group(1),
            'customer_metric_name': cells[1],
            'api': BACKTICK_RE.match(cells[2]).group(1) if _is_metric_cell(cells[2]) else cells[2],
            'plan_value_expr': cells[3],
            'plan_note': cells[4],
        })
    return rows


def _parse_d_class(text):
    """§4.3.4 D 类：实例接口。"""
    rows = []
    body = _section(text, '#### 4.3.4', '#### 4.3.5')
    for line in body.splitlines():
        cells = _cells(line)
        if not cells or len(cells) != 4:
            continue
        if not _is_metric_cell(cells[0]):
            continue
        rows.append({
            'customer_metric': BACKTICK_RE.match(cells[0]).group(1),
            'customer_metric_name': cells[1],
            'plan_value_expr': cells[2],
            'plan_refresh_cycle': cells[3],
        })
    return rows


def _build_metrics(text):
    metrics = {}
    for row in _parse_a_class(text):
        metrics[row['customer_metric']] = {
            'channel': 'A',
            'channel_label': '监控指标接口',
            'tpops_metric': row['tpops_metric'],
            'match_level': row['match_level'],
            'plan_value_expr': row['plan_value_expr'],
            'sql_key': None,
            'source_expr': None,
            'plan_rule': 'raw',
            'dimensions': [],
            'plan_note': '',
        }
    for row in _parse_b_class(text):
        sql_key = SQL_KEY_OVERRIDE.get(row['customer_metric'], SQL_KEY_BY_NUMBER[row['sql_number']])
        metrics[row['customer_metric']] = {
            'channel': 'B',
            'channel_label': '命令中心SQL',
            'tpops_metric': None,
            'match_level': '直接对应',
            'plan_value_expr': row['source_expr'],
            'sql_key': sql_key,
            'source_expr': row['source_expr'],
            'plan_rule': row['plan_rule'],
            'dimensions': row['dimensions'],
            'plan_note': row['plan_note'],
        }
    for row in _parse_c_class(text):
        metrics[row['customer_metric']] = {
            'channel': 'C',
            'channel_label': '诊断优化接口',
            'tpops_metric': None,
            'match_level': '可统计',
            'plan_value_expr': row['plan_value_expr'],
            'sql_key': None,
            'source_expr': None,
            'plan_rule': 'collection',
            'dimensions': [],
            'plan_note': row['plan_note'],
        }
    for row in _parse_d_class(text):
        metrics[row['customer_metric']] = {
            'channel': 'D',
            'channel_label': '实例接口',
            'tpops_metric': None,
            'match_level': '直接对应',
            'plan_value_expr': row['plan_value_expr'],
            'sql_key': None,
            'source_expr': None,
            'plan_rule': 'raw',
            'dimensions': [],
            'plan_note': row['plan_refresh_cycle'],
        }
    return metrics


def _merged_sql_keys():
    """data/merged_sql/merged_sql.json 里的权威 sql_key 集合。"""
    path = os.path.join(BASE_DIR, 'data', 'merged_sql', 'merged_sql.json')
    if not os.path.exists(path):
        sys.exit('找不到 %s —— 合并 SQL 目录是本脚本的输入，缺了无法校验' % path)
    with io.open(path, encoding='utf-8') as handle:
        payload = json.load(handle)
    return set(batch['sql_key'] for batch in payload.get('batches', []))


def check_sql_key_consistency(metrics):
    """本脚本的 sql_key 体系必须与 merged_sql.json 完全一致。

    §4.3.2 把会话明细归在 SQL-4，§4.4 把它拆成 activity_agg / activity_running / activity_top10
    三条合并 SQL，所以 sql_key 有 11 个而 SQL 序号只有 10 个；这里显式校验两边一致，
    避免「改了 merged_sql 忘了改这里」或反之。
    """
    authority = _merged_sql_keys()
    declared = set(SQL_KEY_BY_NUMBER.values()) | set(SQL_KEY_OVERRIDE.values())
    used = set(m['sql_key'] for m in metrics.values() if m.get('sql_key'))
    if used - authority:
        sys.exit('ERROR: 这些 sql_key 不在 merged_sql.json 里：%s' % sorted(used - authority))
    if authority - declared:
        sys.exit('ERROR: merged_sql.json 里的 sql_key 没有在本脚本声明：%s（请补 SQL_KEY_BY_NUMBER / SQL_KEY_OVERRIDE）'
                 % sorted(authority - declared))
    if authority - used:
        sys.exit('ERROR: 这些 sql_key 没有被任何指标用到：%s' % sorted(authority - used))
    return authority


def main():
    text = _read_plan()
    metrics = _build_metrics(text)
    check_sql_key_consistency(metrics)
    payload = {
        'version': '1.0',
        'generated_by': 'tools/build_metric_value_rules.py',
        'generated_from': 'docs/开发方案-GaussDB指标与告警采集.md §4.3',
        'sql_key_by_number': SQL_KEY_BY_NUMBER,
        'sql_key_override': SQL_KEY_OVERRIDE,
        'counts': {
            'A': len([m for m in metrics.values() if m['channel'] == 'A']),
            'B': len([m for m in metrics.values() if m['channel'] == 'B']),
            'C': len([m for m in metrics.values() if m['channel'] == 'C']),
            'D': len([m for m in metrics.values() if m['channel'] == 'D']),
            'total': len(metrics),
        },
        'metrics': metrics,
    }
    with io.open(OUT_PATH, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=False)
        handle.write('\n')
    print('wrote %s: %s' % (OUT_PATH, payload['counts']))
    if payload['counts']['total'] != 98:
        print('ERROR: 期望 98 条可提供指标，实际 %d' % payload['counts']['total'], file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
