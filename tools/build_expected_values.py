#!/usr/bin/env python3
"""生成 ``data/mapping/expected_values.json`` —— B 类 82 个指标的期望值表。

**这张表是什么**：「实测留档的原始行 → 每个 B 类指标应该等于多少」。
它是**生成物**，不手抄：跑一遍本脚本就得到，重跑应当逐字节一致。

**为什么需要它**：合并 SQL 返回形态下，取值口径错了**不报错**——
列名对、聚合口径错，产出的就是一个「看起来合法的 0」。
合同「Wave 0 完成标准」第 2 条要求「82 个 B 类指标在合并形态下的值**等于期望值表**」，
列存在性校验挡不住这类缺陷，只有值相等才能挡。

**期望值怎么来的**：由 ``tools/expected_deriver.py`` 从留档原始行 + 规则声明
独立推导，**不经过规则引擎**（引擎是被测对象，借它的输出当期望值就是自证）。

用法::

    python tools/build_expected_values.py          # 生成并写盘
    python tools/build_expected_values.py --check  # 只校验是否最新，不写
"""
import json
import logging
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS_DIR = os.path.join(REPO_ROOT, 'tools')
for _path in (TOOLS_DIR, REPO_ROOT):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from expected_deriver import (  # noqa: E402  （路径要在 sys.path 调整之后）
    ArchiveError,
    counter_baselines,
    derive_metric,
    parse_archive,
)

LOGGER = logging.getLogger('build_expected_values')

MAPPING_DIR = os.path.join(REPO_ROOT, 'data', 'mapping')
METRIC_RULES_PATH = os.path.join(MAPPING_DIR, 'metric_rules.json')
RESPONSE_RULES_PATH = os.path.join(MAPPING_DIR, 'response_rules.json')
PROBE_PATH = os.path.join(
    REPO_ROOT, 'data', 'merged_sql', 'sql_merge_probe_result.json'
)
OUTPUT_PATH = os.path.join(MAPPING_DIR, 'expected_values.json')

B_CLASS = 'command_center_sql'
# 累计计数器的差分基线按 **维度** 分开（每个库一行），键是占位符维度值。
COUNTER_VIEW = 'stat_database'
COUNTER_DELTA = 1000
ROUND1_NOW_MS = 1760000000000
ROUND2_NOW_MS = ROUND1_NOW_MS + 60000


class BuildError(Exception):
    """生成失败。"""


def build():
    """推出整张期望值表（返回可 JSON 序列化的 dict）。"""
    metric_rules = _read_json(METRIC_RULES_PATH)
    response_rules = _read_json(RESPONSE_RULES_PATH)
    probe = _read_json(PROBE_PATH)

    sql_keys = _sql_keys_by_metric(response_rules)
    views_by_node = _views_by_node(probe)

    expected = {}
    for rule in metric_rules['rules']:
        if rule.get('source', {}).get('source') != B_CLASS:
            continue
        code = rule['metric_code']
        if code not in sql_keys:
            raise BuildError(f'映射表里没有 {code} 的取数视图声明')
        expected[code] = _expected_of(rule, sql_keys[code], views_by_node)

    return {
        'version': 1,
        'generated_by': 'tools/build_expected_values.py',
        'deriver': 'tools/expected_deriver.py',
        'sources': {
            'probe': os.path.relpath(PROBE_PATH, REPO_ROOT),
            'metric_rules': os.path.relpath(METRIC_RULES_PATH, REPO_ROOT),
            'response_rules': os.path.relpath(RESPONSE_RULES_PATH, REPO_ROOT),
        },
        'counter_delta_plan': _counter_plan(),
        'metric_count': len(expected),
        'expected': expected,
    }


def _expected_of(rule, sql_key, views_by_node):
    """一个指标的期望值：第 1 轮（实测留档）+ 累计计数器的第 2 轮。"""
    code = rule['metric_code']
    value_rule = rule.get('value_rule') or {}
    bindings = rule.get('dimension_bindings') or {}
    entry = {
        'value_rule_type': value_rule.get('type'),
        'sql_key': sql_key,
        'round1': {},
    }
    baselines = {}  # type: dict
    for node, views in views_by_node.items():
        rows = views.get(sql_key) or []
        entry['round1'][node] = _dump(derive_metric(
            code, value_rule, bindings, rows, views, ROUND1_NOW_MS
        ))
        if value_rule.get('type') == 'counter_delta':
            baselines[node] = counter_baselines(value_rule, bindings, rows, ROUND1_NOW_MS)

    if value_rule.get('type') == 'counter_delta':
        entry['round2'] = _counter_round2(rule, sql_key, views_by_node, baselines)
    return entry


def _counter_round2(rule, sql_key, views_by_node, baselines):
    """第 2 轮：把累计计数列按计划加量后的合成留档 → 期望速率。"""
    value_rule = rule.get('value_rule') or {}
    bindings = rule.get('dimension_bindings') or {}
    deltas = _counter_plan()['deltas']
    result = {}
    for node, views in views_by_node.items():
        bumped = _bump_counters(views, deltas)
        result[node] = _dump(derive_metric(
            rule['metric_code'], value_rule, bindings,
            bumped.get(sql_key) or [], bumped, ROUND2_NOW_MS, baselines.get(node),
        ))
    return result


def _bump_counters(views, deltas):
    """合成第 2 轮留档：累计计数列按计划加量，其余原样。"""
    bumped = {}
    for name, rows in views.items():
        if name != COUNTER_VIEW:
            bumped[name] = rows
            continue
        new_rows = []
        for row in rows:
            new_row = dict(row)
            for column, delta in deltas.items():
                value = new_row.get(column)
                if value is not None:
                    new_row[column] = value + delta
            new_rows.append(new_row)
        bumped[name] = new_rows
    return bumped


def _counter_plan():
    return {
        'round1_now_ms': ROUND1_NOW_MS,
        'round2_now_ms': ROUND2_NOW_MS,
        'elapsed_seconds': (ROUND2_NOW_MS - ROUND1_NOW_MS) / 1000.0,
        'view': COUNTER_VIEW,
        'deltas': {
            'tup_fetched': COUNTER_DELTA,
            'tup_inserted': COUNTER_DELTA,
            'tup_returned': COUNTER_DELTA,
            'tup_updated': COUNTER_DELTA,
            'tup_deleted': COUNTER_DELTA,
        },
    }


def _sql_keys_by_metric(response_rules):
    """指标 → 取数视图名。"""
    shapes = response_rules['shapes']
    result = {}
    for item in response_rules['rules']:
        if item.get('channel') != B_CLASS:
            continue
        shape = shapes.get(item['shape']) or {}
        sql_key = (shape.get('sql_ref') or {}).get('sql_key')
        if not sql_key:
            raise BuildError(
                f"{item['metric_code']} 的形状 {item['shape']} 没有 sql_key"
            )
        result[item['metric_code']] = sql_key
    return result


def _views_by_node(probe):
    """节点名 → 该节点的视图行（自己解析留档，不借生产包的解码）。"""
    nodes = probe.get('nodes') or {}
    if not nodes:
        raise BuildError('留档里没有节点')
    result = {}
    for node in sorted(nodes):
        payload = nodes[node]
        result[node] = parse_archive(payload['result'])
    return result


def _dump(values):
    """``[(维度, 值)]`` → 可 JSON 序列化的结构。"""
    return [{'dimensions': dims, 'value': value} for dims, value in values]


def _read_json(path):
    with open(path, encoding='utf-8') as handle:
        return json.load(handle)


def _write_json(path, payload):
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        handle.write('\n')


def main(argv):
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    check_only = '--check' in argv
    try:
        payload = build()
    except (ArchiveError, BuildError) as exc:
        LOGGER.error('生成失败：%s', exc)
        return 2
    if check_only:
        current = _read_json(OUTPUT_PATH)
        if current != payload:
            LOGGER.error('expected_values.json 过期，请运行 tools/build_expected_values.py')
            return 1
        LOGGER.info('expected_values.json 是最新的（%s 条）', payload['metric_count'])
        return 0
    _write_json(OUTPUT_PATH, payload)
    LOGGER.info('已写出 %s（%s 条 B 类指标）', OUTPUT_PATH, payload['metric_count'])
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
