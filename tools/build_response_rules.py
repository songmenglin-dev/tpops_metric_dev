#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从实测 response_body 生成 ``data/mapping/response_rules.json``。

**为什么要有这份文件**：``metric_rules.json`` 写的是"取哪个平台的哪个列"
（业务语义），但采集端拿到的是 HTTP 响应体 —— 中间隔着
「命令中心的 result 是 JSON 字符串」「监控指标是按组件展开的 series」
「诊断接口返回的是列表」这些结构差异。缺了这层映射，
采集器只能各写一遍解包，规则里的列名也无从校验。

本脚本把每条指标的映射条件拆成三段写进 JSON：

    extract   取数定位：响应体的哪一段 → 哪一列（形状 + 定位）
    business  业务规则：取值方式、作用域、维度、缺值与幂等语义
    evidence  实测证据：这份条件是在哪个样本文件、哪一步、哪些列上验证的

生成时当场校验：定位里的列必须真实存在于实测响应体的列集合里。
这就是开发方案 §5.6.4 要求的"启动期校验"，在生成期就做掉。

用法::

    python tools/build_response_rules.py          # 生成并写盘
    python tools/build_response_rules.py --check  # 只校验是否最新，不写
"""
from __future__ import print_function

import io
import json
import os
import re
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAPPING_DIR = os.path.join(REPO_ROOT, 'data', 'mapping')
RESPONSE_DIR = os.path.join(REPO_ROOT, 'data', 'metric_response')
MERGED_SQL_PATH = os.path.join(REPO_ROOT, 'data', 'merged_sql', 'merged_sql.json')
METRIC_RULES_PATH = os.path.join(MAPPING_DIR, 'metric_rules.json')
OUTPUT_PATH = os.path.join(MAPPING_DIR, 'response_rules.json')

CN_KEY_RE = re.compile(r'^cn_\d+$')
DN_KEY_RE = re.compile(r'^dn_\d+$')
IP_DN_KEY_RE = re.compile(r'^\d+\.\d+\.\d+\.\d+-dn_\d+$')

# 通道 → 命令中心链路的固定步骤。中间跳不含业务数据，但写进文件，
# 采集端照着这个顺序走，不用再回翻开发方案。
CHANNELS = {
    'command_center_sql': {
        'label': '命令中心 SQL（合并 SQL 主通道，82 个客户指标）',
        'api_chain': [
            {
                'step': 'nodes',
                'api': 'GET /rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node',
                'purpose': '取 master 节点的 node_id（按 node_id 去重）',
            },
            {
                'step': 'submit',
                'api': 'POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}',
                'purpose': '按 sql_key 下发合并 SQL，一次可带全部 master',
            },
            {
                'step': 'poll',
                'api': 'GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records?job_id={job_id}',
                'purpose': '轮询；结束判据是 statistics.running == 0，不是 job_status',
            },
            {
                'step': 'detail',
                'api': 'GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}/task-record/{task_id}/detail',
                'purpose': '取 result（JSON 字符串，二次解析才是行列表）',
            },
        ],
        'terminal': 'detail',
    },
    'monitor_metrics': {
        'label': '监控指标接口（A 类时序，11 个客户指标）',
        'api_chain': [
            {
                'step': 'query',
                'api': 'POST /rds/v3/monitor/metrics',
                'purpose': '按 12 个分组码查询；series 按组件展开',
            },
        ],
        'terminal': 'query',
    },
    'diagnostic': {
        'label': '诊断优化接口（C 类，4 个客户指标）',
        'api_chain': [
            {
                'step': 'slow_sql',
                'api': 'POST /rds/gaussdb/v3/instances/{instance_id}/slow-sql-list',
                'purpose': '慢查询清单',
            },
            {
                'step': 'top_sql',
                'api': 'POST /rds/gaussdb/v3/instances/{instance_id}/top-sql-list',
                'purpose': 'Top SQL 清单',
            },
        ],
        'terminal': 'slow_sql_or_top_sql',
    },
    'instance': {
        'label': '实例接口（D 类，1 个客户指标）',
        'api_chain': [
            {
                'step': 'instances',
                'api': 'GET /rds/v3.2/instances',
                'purpose': '实例元数据',
            },
        ],
        'terminal': 'instances',
    },
}

IDEMPOTENCY = {
    'metric': ['instance_id', 'metric_name', 'dimensions', 'timestamp'],
    'alarm': ['alarm_id', 'instance_id', 'node_id', 'first_alarm_time', 'last_alarm_time'],
}


class BuildError(Exception):
    """生成期校验不通过。"""


def _read_json(path):
    with io.open(path, encoding='utf-8') as fp:
        return json.load(fp)


def _series_key_kind(key):
    """把 series 键归成形态：cn / dn / ip_dn / value。"""
    if CN_KEY_RE.match(key):
        return 'cn'
    if DN_KEY_RE.match(key):
        return 'dn'
    if IP_DN_KEY_RE.match(key):
        return 'ip_dn'
    return key


def _series_shape_id(kinds):
    if not kinds:
        return 'mon_series_single'
    if len(kinds) == 1:
        return 'mon_series_%s' % sorted(kinds)[0]
    return 'mon_series_%s' % '_'.join(sorted(kinds))


def _find_terminal_step(steps, channel):
    """按通道挑出"含业务数据"的那一步。"""
    if channel == 'command_center_sql':
        hits = [
            (i, s) for i, s in enumerate(steps)
            if '/task-record/' in s['api'] and s['api'].rstrip('/').endswith('detail')
        ]
        if not hits:
            raise BuildError('命令中心链路里没有 task-record detail 步骤')
        return hits[0]
    if channel == 'monitor_metrics':
        hits = [
            (i, s) for i, s in enumerate(steps)
            if _api_path(s['api']) == 'POST /rds/v3/monitor/metrics'
        ]
        if not hits:
            raise BuildError('没有 monitor/metrics 步骤')
        # 取采样点最多的那一步（窗口查询），最新点那步作对照
        best = max(hits, key=lambda item: _series_point_count(item[1]['response_body']))
        return best
    if channel == 'diagnostic':
        for i, s in enumerate(steps):
            if 'slow-sql-list' in s['api'] or 'top-sql-list' in s['api']:
                return i, s
        raise BuildError('没有诊断优化接口的步骤')
    if channel == 'instance':
        for i, s in enumerate(steps):
            if _api_path(s['api']) == 'GET /rds/v3.2/instances':
                return i, s
        raise BuildError('没有实例列表步骤')
    raise BuildError('未知通道：%s' % channel)


def _api_path(api):
    """去掉查询串，只留路径部分。"""
    return api.split('?', 1)[0]


def _series_point_count(body):
    total = 0
    for info in body.get('metric_infos') or ():
        for metric in info.get('metrics') or ():
            for values in (metric.get('series') or {}).values():
                total += len(values)
    return total


def _observed_of_rows(body):
    """命令中心任务明细：result 是 JSON 字符串。"""
    raw = body.get('result')
    rows = []
    if raw:
        try:
            rows = json.loads(raw)
        except ValueError:
            rows = []
    columns = []
    for row in rows:
        if isinstance(row, dict):
            for key in row:
                if key not in columns:
                    columns.append(key)
    return rows, sorted(columns)


def _build_shapes_and_rules(metric_rules, samples, merged_sql):
    shapes = {}
    rules = []
    for rule in metric_rules:
        if not rule.get('enabled'):
            continue
        code = rule['metric_code']
        channel = rule['source'].get('source')
        if channel not in CHANNELS:
            raise BuildError('%s 的通道不认识：%s' % (code, channel))
        sample = samples.get(code)
        if sample is None:
            raise BuildError('%s 没有实测样本' % code)
        steps = sample['steps']
        step_index, step = _find_terminal_step(steps, channel)
        body = step['response_body']
        params = rule.get('value_rule', {}).get('params', {}) or {}

        locate = {}
        evidence = {
            'sample_file': sample['file'],
            'terminal_step': step_index,
            'api': step['api'],
        }
        shape = {}

        if channel == 'command_center_sql':
            sql_key = rule['source'].get('sql_key')
            if not sql_key:
                raise BuildError('%s 缺 sql_key' % code)
            shape_id = 'cmd_rows_%s' % sql_key
            rows, columns = _observed_of_rows(body)
            master_count = len([
                s for s in steps
                if '/task-record/' in s['api'] and s['api'].rstrip('/').endswith('detail')
            ])
            shape = {
                'channel': channel,
                'kind': 'rows_from_result',
                'payload_path': ['result'],
                'payload_encoding': 'json',
                'sql_ref': {
                    'catalog': 'data/merged_sql/merged_sql.json',
                    'sql_key': sql_key,
                    'sql': merged_sql.get(sql_key),
                },
            }
            locate = {
                'sql_key': sql_key,
                'column': params.get('column'),
                'value_column': params.get('value_column', params.get('column')),
                'match': params.get('filter', {}) or {},
                'group_by': params.get('group_by', []) or [],
            }
            evidence['observed'] = {
                'row_count': len(rows),
                'columns': columns,
                'master_nodes': master_count,
            }
            _assert_columns_exist(code, locate, columns)
            # 带过滤条件的规则：在实测样本里未必命中（采集时刻没有那种锁/会话）。
            # 命中 0 行说明这份映射条件无法用留档证明，要显式记下来。
            matched = len([r for r in rows if _match_row(r, locate.get('match') or {})])
            evidence['observed']['matched_rows'] = matched
            if locate.get('match') and matched == 0:
                evidence['verified_on_sample'] = False
                evidence['note'] = (
                    '样本中无匹配行（过滤条件 %s 在采集时刻不成立），'
                    '这条映射条件无法用留档验证' % (locate['match'],)
                )

        elif channel == 'monitor_metrics':
            platform_metric = rule['source'].get('tpops_source')
            series_keys = []
            kinds = set()
            point_counts = []
            zero_timestamps = 0
            timestamps = 0
            frequencies = set()
            for info in body.get('metric_infos') or ():
                for metric in info.get('metrics') or ():
                    if metric.get('name') != platform_metric:
                        continue
                    for key, values in (metric.get('series') or {}).items():
                        if key not in series_keys:
                            series_keys.append(key)
                        kinds.add(_series_key_kind(key))
                        point_counts.append(len(values))
                    points = metric.get('timestamps') or []
                    timestamps += len(points)
                    zero_timestamps += len([t for t in points if not t])
                    if metric.get('collection_frequency'):
                        frequencies.add(metric.get('collection_frequency'))
            if not series_keys:
                raise BuildError(
                    '%s 在实测响应里找不到 series（平台指标 %s）' % (code, platform_metric)
                )
            shape_id = _series_shape_id(kinds)
            shape = {
                'channel': channel,
                'kind': 'series_from_metric_infos',
                'payload_path': ['metric_infos'],
                'match_field': 'metrics[].name',
            }
            locate = {
                'metric': platform_metric,
                'series_keys': series_keys,
                'position': params.get('position', 'last_non_null'),
            }
            evidence['observed'] = {
                'series_keys': series_keys,
                'point_counts': sorted(set(point_counts)),
                'collection_frequency': sorted(frequencies),
                'timestamps': timestamps,
                'zero_timestamps': zero_timestamps,
            }

        elif channel == 'diagnostic':
            collection = params.get('collection')
            if not collection:
                raise BuildError('%s 的诊断规则缺 collection' % code)
            shape_id = 'diag_%s' % collection
            items = body.get(collection) or []
            columns = sorted(items[0].keys()) if items and isinstance(items[0], dict) else []
            shape = {
                'channel': channel,
                'kind': 'rows_from_collection',
                'collection': collection,
                'payload_path': [collection],
            }
            locate = {
                'collection': collection,
                'field': params.get('field'),
                'prefixes': params.get('prefixes', []) or [],
            }
            evidence['observed'] = {
                'item_count': len(items),
                'columns': columns,
            }

        elif channel == 'instance':
            collection = 'instances'
            shape_id = 'inst_list'
            items = body.get(collection) or []
            columns = sorted(items[0].keys()) if items and isinstance(items[0], dict) else []
            field = params.get('field')
            if field and columns and field not in columns:
                raise BuildError('%s 要取字段 %s，但响应里只有 %s' % (code, field, columns))
            shape = {
                'channel': channel,
                'kind': 'instance_from_list',
                'collection': collection,
                'payload_path': [collection],
            }
            locate = {'collection': collection, 'field': field}
            evidence['observed'] = {'item_count': len(items), 'columns': columns}
        else:
            raise BuildError('未知通道：%s' % channel)

        shape.setdefault('observed', {})
        shapes.setdefault(shape_id, shape)

        rules.append({
            'metric_code': code,
            'metric_name': rule['metric_name'],
            'enabled': True,
            'channel': channel,
            'shape': shape_id,
            'locate': locate,
            'business': _business_of(rule),
            'evidence': evidence,
            'needs_confirmation': bool(rule.get('needs_confirmation')),
            'note': rule.get('note', ''),
        })

    return shapes, rules


def _match_row(row, conditions):
    """行是否满足过滤条件（与规则引擎 _match_filter 同语义）。"""
    for key, expected in conditions.items():
        if isinstance(expected, (list, tuple)):
            if row.get(key) not in expected:
                return False
        elif row.get(key) != expected:
            return False
    return True


def _assert_columns_exist(code, locate, columns):
    """定位里要取的列必须真实出现在实测响应体的列集合里（§5.6.4）。"""
    wanted = [locate.get('column'), locate.get('value_column')]
    for name in wanted:
        if name and columns and name not in columns:
            raise BuildError(
                '%s 要取的列 %s 不在实测响应里，实测列有 %s' % (code, name, columns)
            )


def _business_of(rule):
    """业务规则段：取值方式 + 作用域 + 维度 + 缺值与幂等语义。"""
    params = rule.get('value_rule', {}).get('params', {}) or {}
    rule_type = (rule.get('value_rule') or {}).get('type')
    row_policy = params.get('row_policy')
    if row_policy is None:
        if rule_type in ('aggregate',):
            row_policy = 'sum'
        elif rule_type in ('counter_delta',):
            row_policy = 'per_row'
        else:
            row_policy = 'first'
    scope = {
        'row': {
            'policy': row_policy,
            'group_by': params.get('group_by', []) or [],
            'note': '行作用域：per_row=一行一条消息；sum=全表聚合成一条；first=取第一行',
        },
        'node': {
            'policy': 'per_master',
            'note': '命令中心同一 sql_key 在每个 master 各返一份，计数类差分基线按 master 分开',
        },
        'series': {
            'policy': 'not_applicable',
        },
    }
    if rule['source'].get('source') == 'monitor_metrics':
        scope['series'] = {
            'policy': 'per_component',
            'note': 'A 类多 series 归集口径待客户确认（open_items.json / a-class-multi-series）',
            'candidates': ['sum', 'min', 'expand_by_dn'],
        }
    time_policy = {
        'source': 'collect_time',
        'fallback': 'collect_time',
    }
    if rule['source'].get('source') == 'monitor_metrics':
        time_policy = {
            'source': 'series_timestamp',
            'fallback': 'collect_time',
            'note': '取与 value 同一下标的 timestamps[i]；timestamps 为 0 或缺失时回退到采集时刻',
        }
    return {
        'value_rule': rule.get('value_rule') or {},
        'scope': scope,
        'dimensions': rule.get('dimension_bindings', {}) or {},
        'placeholders': rule.get('placeholders', []) or [],
        'value_type': rule.get('value_type'),
        'unit': rule.get('unit'),
        'collect_cycle': rule.get('collect_cycle', {}) or {},
        'time_policy': time_policy,
        'missing': 'null',
        'idempotency': IDEMPOTENCY['metric'],
    }


def _merged_view_names(sql_text):
    """从合并 SQL 里解析出视图名（``json_agg(t) ... as <视图名>`` 的别名）。

    视图列的写法是 ``(select json_agg(t) from ( <内层 SQL> ) t) as <视图名>``，
    内层 SQL 里也有括号，正则按括号配平扫一遍才准。
    """
    names = []
    marker = 'json_agg(t)'
    position = 0
    while True:
        start = sql_text.find(marker, position)
        if start < 0:
            break
        position = start + len(marker)
        depth = 0
        index = position
        while index < len(sql_text):
            char = sql_text[index]
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth < 0:
                    break
            index += 1
        match = re.match(r'\s+as\s+([a-z_][a-z0-9_]*)', sql_text[index + 1:])
        if not match:
            raise BuildError('合并 SQL 里有一处 json_agg 没有 as 别名')
        names.append(match.group(1))
    return names


def _build_merged_views(merged):
    """生成 ``merged_views`` 段：合并 SQL 的 11 个视图。

    视图名以 ``merged_sql.json`` 的 ``batches[].sql_key`` 为准，
    并与 ``sql_merge.sql`` 里实际的列别名**交叉校验** —— 对不上就报错退出，
    绝不静默产出一份与 SQL 不匹配的映射表。

    ``paged`` 取自 ``merged_sql.json`` 的 ``paging.views[].paged``，**不重新发明**；
    没在该段里出现的视图按 ``paged = false`` 处理（固定视图每页全量重复取，
    累加会翻倍）。
    """
    batches = merged.get('batches', [])
    sql_keys = [batch['sql_key'] for batch in batches]
    if len(set(sql_keys)) != len(sql_keys):
        raise BuildError('merged_sql.json 的 sql_key 有重复：%s' % sql_keys)

    merge_sql_path = os.path.join(REPO_ROOT, 'data', 'merged_sql', 'sql_merge.sql')
    with io.open(merge_sql_path, encoding='utf-8') as fp:
        sql_text = fp.read()
    sql_view_names = _merged_view_names(sql_text)
    if sorted(sql_view_names) != sorted(sql_keys):
        raise BuildError(
            'sql_merge.sql 的视图名与 merged_sql.json 的 sql_key 对不上：'
            'SQL 里是 %s，JSON 里是 %s' % (sorted(sql_view_names), sorted(sql_keys))
        )

    paging_views = (merged.get('paging') or {}).get('views') or {}
    views = {}
    for sql_key in sql_keys:
        views[sql_key] = {
            'json_column': sql_key,
            'paged': bool((paging_views.get(sql_key) or {}).get('paged', False)),
            'sql_ref': {
                'catalog': 'data/merged_sql/sql_merge.sql',
                'sql_key': sql_key,
            },
        }
    return views


def build():
    metric_rules = _read_json(METRIC_RULES_PATH)['rules']
    manifest = _read_json(os.path.join(RESPONSE_DIR, '000manifest.json'))
    merged = _read_json(MERGED_SQL_PATH)
    merged_sql = dict((b['sql_key'], b.get('sql')) for b in merged.get('batches', []))

    samples = {}
    for item in manifest['metrics']:
        path = os.path.join(RESPONSE_DIR, item['file'])
        samples[item['customer_metric']] = {
            'file': item['file'],
            'steps': _read_json(path),
        }

    shapes, rules = _build_shapes_and_rules(metric_rules, samples, merged_sql)
    merged_views = _build_merged_views(merged)
    return {
        'version': '1.0',
        'generated_by': 'tools/build_response_rules.py',
        'generated_from': {
            'metric_rules': 'data/mapping/metric_rules.json',
            'response_samples': 'data/metric_response/',
            'merged_sql': 'data/merged_sql/merged_sql.json',
        },
        'note': (
            '以实测 response_body 为基准的映射条件。extract 段（shape + locate）'
            '回答"从响应体的哪一段取哪一列"，business 段回答"取到之后怎么算"；'
            'evidence 段记录这份条件是在哪个样本、哪些列上验证的。'
            '列名与响应体的匹配在生成期已校验（开发方案 §5.6.4）。'
        ),
        'channels': CHANNELS,
        'shapes': shapes,
        'rules': rules,
        'merged_views': merged_views,
    }


def main(argv):
    check_only = '--check' in argv
    payload = build()
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + '\n'
    if check_only:
        if not os.path.exists(OUTPUT_PATH):
            print('response_rules.json 不存在')
            return 1
        with io.open(OUTPUT_PATH, encoding='utf-8') as fp:
            current = fp.read()
        if current != text:
            print('response_rules.json 与生成器输出不一致，'
                  '请重新运行 tools/build_response_rules.py')
            return 1
        print('response_rules.json 是最新的')
        return 0
    with io.open(OUTPUT_PATH, 'w', encoding='utf-8') as fp:
        fp.write(text)
    print('写出 %s：%d 条规则 / %d 种形状' % (
        os.path.relpath(OUTPUT_PATH, REPO_ROOT), len(payload['rules']), len(payload['shapes'])
    ))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv[1:]))
    except BuildError as exc:
        print('生成失败：%s' % exc)
        sys.exit(2)
