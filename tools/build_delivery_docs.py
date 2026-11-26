# -*- coding: utf-8 -*-
"""生成 wave-4 的三份客户交付文档。

生成方式（表驱动 —— 内容全部来自映射表与客户指标表，不写死在文档里）：

    python tools/build_delivery_docs.py

产出：

  1. ``docs/交付-指标口径说明.md``          —— 98 个可提供指标各一句口径
  2. ``docs/交付-不可提供指标清单.md``      —— 3 个不可提供指标 + 书面理由
  3. ``docs/交付-客户指标英文名核对清单.md`` —— OCR 疑似错字清单（给客户核对）

脚本**幂等**：同样的输入必然产出同样的文件（重跑 ``git diff`` 为空）。
所有取值口径来自 ``tpops_kafka_collector/mapping/data/metric_rules.json``
（生效口径 87 条 = 客户指标库 101 条 − 排除清单 14 条；排除清单见
``mapping/data/excluded_metrics.json``），这是唯一的真相源。

Python 3.7.9 兼容（见 CLAUDE.md「Python 3.7.9 红线」）。
"""
from __future__ import print_function

import difflib
import json
import os
import re
import sys
from collections import OrderedDict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAPPING_DATA = os.path.join(REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data')
DOCS_DIR = os.path.join(REPO_ROOT, 'docs')
XLSX_PATH = os.path.join(
    REPO_ROOT, '高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx'
)

METRIC_RULES_PATH = os.path.join(MAPPING_DATA, 'metric_rules.json')
MERGED_SQL_PATH = os.path.join(MAPPING_DATA, 'merged_sql.json')
EXCLUDED_PATH = os.path.join(MAPPING_DATA, 'excluded_metrics.json')

SPEC_DOC = os.path.join(DOCS_DIR, '交付-指标口径说明.md')
UNSUPPORTED_DOC = os.path.join(DOCS_DIR, '交付-不可提供指标清单.md')
OCR_DOC = os.path.join(DOCS_DIR, '交付-客户指标英文名核对清单.md')

#: 表格在 markdown 里的定位标记 —— 守卫测试靠它精确取表，不靠猜行号
TABLE_MARKS = {
    'metrics': '<!-- TABLE:metrics -->',
    'unsupported': '<!-- TABLE:unsupported -->',
    'ocr': '<!-- TABLE:ocr -->',
    'ocr_desc': '<!-- TABLE:ocr-desc -->',
    'ocr_auto': '<!-- TABLE:ocr-auto -->',
    'ocr_sub': '<!-- TABLE:ocr-sub -->',
}

GRANULARITY_LABEL = {
    'instance': '实例级',
    'table': '表级',
    'database': '库级',
    'port': '端口级',
}

CYCLE_UNIT_LABEL = {'分': '分钟', '秒': '秒', '时': '小时'}

#: 秒 → 人话（「30 秒」/「1 小时」）。
def format_seconds(seconds: int) -> str:
    """把秒数渲染成人话（用于「实际产出频率」列）。"""
    if seconds % 3600 == 0:
        return '%d 小时' % (seconds // 3600)
    if seconds % 60 == 0:
        return '%d 分钟' % (seconds // 60)
    return '%d 秒' % seconds


def format_int(value: int) -> str:
    """千分位（152664 → 152,664）。"""
    return '{:,}'.format(value)


#: 归集口径的**通道**文案 —— 只有命令中心（B 类）才谈 master。
#:
#: A 类（监控指标接口）走 ``rules_series.eval_series_point``：**每条 series 一条
#: 消息**，维度键是 ``COMPONENT_ID``，**不跨组件合成**；``node_scoped`` 对 A 类
#: 不参与归集（采集器只用它决定「组件换算不到节点时要不要告警」）。
#: C / D 类与节点无关，整条落到节点清单里的第一个 master CN。
COLLECTION_TEXT_MONITOR = (
    '组件级：逐组件各出一条（每条 series 一条消息，`COMPONENT_ID` 归入 `tags`，'
    '不跨组件合成）'
)
COLLECTION_TEXT_INSTANCE = (
    '实例级：与节点无关，整条落到节点清单里的第一个 master CN'
)


#: 备注覆写 —— 映射表的 ``note`` 与 ``value_rule.note`` 口径打架时，以本表为准。
#:
#: 背景：``gaussdb.Total_pct`` 的 ``note`` 写「活跃会话数 / 总连接数」，但它的
#: ``value_rule.note`` 与合并 SQL 都表明分子是 ``activity_agg`` 的
#: ``count(*)`` 之和（``group by datname, state``，**含 idle 等全部状态**）——
#: 「活跃会话数」是错的。映射表本身归另一个波次所有，本轮不改它，只在生成器
#: 里如实覆写，并留一条守卫盯着。
NOTE_OVERRIDE = {
    'gaussdb.Total_pct': (
        '全部会话数 / 总连接数（分子=activity_agg 的 cnt 之和，含 idle 等全部状态）'
    ),
    'GAUSSDB.session_usage': (
        '全部会话数 / 总连接数（分子=activity_agg 的 cnt 之和，含 idle 等全部状态）'
    ),
}


# --------------------------------------------------------------------------
# 数据读取
# --------------------------------------------------------------------------
def load_json(path: str) -> Any:
    """读 JSON 文件。"""
    with open(path, 'r', encoding='utf-8') as handle:
        return json.load(handle)


def load_metric_rules() -> Dict[str, Any]:
    """读映射表（生效口径的指标规则）。"""
    return load_json(METRIC_RULES_PATH)


def load_excluded_metrics() -> Dict[str, Any]:
    """读排除清单（被排除的客户指标、理由与口径条数）。"""
    return load_json(EXCLUDED_PATH)


def customer_scope_rules(
    rules: Sequence[Dict[str, Any]],
    customer_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """客户指标库的**全部**指标（生效口径 + 被排除的那些）。

    OCR 核对清单的对象是**客户指标库**（101 个指标）—— 被排除的 14 条也在
    客户表里，英文名同样可能有 OCR 错字。所以检测器跑在「全量」上，
    而不是只跑生效口径。
    """
    scope = list(rules)
    known = set(rule['metric_code'] for rule in rules)
    by_code = dict((row['metric_code'], row) for row in customer_rows)
    for entry in load_excluded_metrics().get('excluded') or ():
        code = entry.get('metric_code')
        if not code or code in known:
            continue
        row = by_code.get(code) or {}
        scope.append({
            'metric_code': code,
            'metric_name': entry.get('metric_name') or row.get('metric_name') or '',
        })
    return scope


def unsupported_items(
    excluded: Sequence[Dict[str, Any]],
    customer_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """排除清单里「无法提供」的条目 → 与指标规则同形的条目。

    排除清单只有「指标码 / 中文名 / 理由」三样；清单表还要展示分类、
    数据类型、采集周期 —— 那些从**客户指标库 xlsx**（客户原件，只读）取，
    不在数据文件里重复一份。
    """
    by_code = dict((row['metric_code'], row) for row in customer_rows)
    items = []  # type: List[Dict[str, Any]]
    for entry in excluded:
        if entry.get('reason') != 'unsupported':
            continue
        code = entry.get('metric_code') or ''
        row = by_code.get(code) or {}
        detail = entry.get('detail') or ''
        items.append({
            'metric_code': code,
            'metric_name': entry.get('metric_name') or row.get('metric_name') or '',
            'category': row.get('category') or '',
            'value_type': row.get('value_type') or '',
            'unit': row.get('unit') or '',
            'collect_cycle': row.get('collect_cycle') or {},
            'unsupported_reason': detail,
            'note': detail,
        })
    return items


def load_merged_sql() -> Dict[str, Dict[str, Any]]:
    """读合并 SQL 目录 → ``{sql_key: 视图信息}``。"""
    data = load_json(MERGED_SQL_PATH)
    views = {}
    for batch in data.get('batches') or ():
        key = batch.get('sql_key')
        if key:
            views[key] = batch
    return views


def load_customer_table() -> List[Dict[str, Any]]:
    """读客户指标库 xlsx 的「指标库」sheet（按表头名取列，不按列序号）。"""
    import openpyxl

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from xlsx_columns import column_map, require  # noqa: E402  (需要 sys.path 先就位)

    workbook = openpyxl.load_workbook(XLSX_PATH, read_only=True, data_only=True)
    sheet = workbook['指标库']
    rows = list(sheet.iter_rows(values_only=True))
    if not rows:
        raise ValueError('客户指标表「指标库」sheet 是空的')
    mapping = column_map(rows[0])
    name_col = require(mapping, '英文名')
    cn_col = require(mapping, '中文名')
    cat_col = mapping.get('指标分类')
    type_col = mapping.get('数据类型')
    unit_col = mapping.get('指标单位')
    cycle_col = mapping.get('数据更新周期')
    cycle_unit_col = mapping.get('采集更新周期单位')
    result = []
    for row in rows[1:]:
        code = row[name_col]
        if not code:
            continue
        cycle = row[cycle_col] if cycle_col is not None else None
        result.append({
            'metric_code': str(code).strip(),
            'metric_name': str(row[cn_col] or '').strip(),
            'category': str(row[cat_col] or '').strip() if cat_col is not None else '',
            'value_type': str(row[type_col] or '').strip() if type_col is not None else '',
            'unit': str(row[unit_col] or '').strip() if unit_col is not None else '',
            'collect_cycle': {
                'value': int(cycle) if isinstance(cycle, (int, float)) else cycle,
                'unit': (str(row[cycle_unit_col] or '').strip()
                         if cycle_unit_col is not None else ''),
            },
        })
    workbook.close()
    return result


def enabled_rules(rules: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """可提供的指标规则（enabled=True）。"""
    return [rule for rule in rules if rule.get('enabled')]


def disabled_rules(rules: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """不可提供的指标规则（enabled=False）。"""
    return [rule for rule in rules if not rule.get('enabled')]


# --------------------------------------------------------------------------
# 通道周期：**从采集端实现实算**，不写死
# --------------------------------------------------------------------------
def load_schedule():
    """加载采集端的调度实算（通道周期）。

    直接调用生产代码 ``tpops_kafka_collector.schedule.compute_channel_plans``
    —— 文档里的「实际产出频率」因此**与实现同源**：实现改了，文档必须重新
    生成（守卫 ``test_docs_match_generator_output`` 会红）。

    :returns: ``(通道周期秒数, source.source → 调度通道, 已加载的映射表)``
    """
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
    from tpops_kafka_collector.mapping.loader import load_mapping
    from tpops_kafka_collector.schedule import compute_channel_plans

    mapping = load_mapping()
    periods = {}
    for plan in compute_channel_plans(mapping):
        periods[plan.channel] = plan.interval_seconds
    return periods, mapping


def channel_schedule(rules: Sequence[Dict[str, Any]]):
    """每条规则的调度通道与该通道的周期。

    :returns: ``({metric_code: 通道周期秒数}, {通道周期秒数: 通道名})``
    """
    periods, mapping = load_schedule()
    from tpops_kafka_collector.schedule import SOURCE_TO_CHANNEL

    by_code = {}
    for rule in mapping.metric_rules:
        if not rule.enabled:
            continue
        source = rule.source or {}
        channel = SOURCE_TO_CHANNEL.get(source.get('source'))
        if channel is None:
            continue
        by_code[rule.metric_code] = periods.get(channel)
    return by_code, periods


def source_method_map(rules: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    """``source.source`` → ``source.method``（从映射表数据里推，不写死）。"""
    mapping = {}
    for rule in rules:
        source = rule.get('source') or {}
        raw = source.get('source')
        method = source.get('method')
        if raw and method:
            mapping.setdefault(raw, method)
    return mapping


def daily_message_volume(rules: Sequence[Dict[str, Any]],
                         by_code: Dict[str, Optional[int]]) -> int:
    """按「每条规则每轮一条」估的**日均消息条数 / 实例**（下界）。

    A 类多 series、表级 / 库级维度展开都会让实际条数**更多**。
    """
    total = 0
    for rule in rules:
        if not rule.get('enabled'):
            continue
        period = by_code.get(rule.get('metric_code'))
        if not period:
            continue
        total += 86400 // period
    return total


# --------------------------------------------------------------------------
# 取值规则的人话描述
# --------------------------------------------------------------------------
def _join_codes(items: Iterable[Any]) -> str:
    """把一组标识符渲染成 `` `a`/`b` `` 的形式。"""
    return '/'.join('`%s`' % item for item in items)


def _describe_row_rule(kind, params):
    """行级取值规则的人话口径；不认识的 kind 返回 None（交给下一段）。"""
    if kind == 'raw':
        column = params.get('column')
        policy = params.get('row_policy', 'first')
        if policy == 'sum':
            return '取列 `%s`，对该视图所有行求和' % column
        if policy == 'per_row':
            group_by = params.get('group_by') or []
            return '取列 `%s`，一行一条，按 %s 分维度展开' % (
                column, _join_codes(group_by) if group_by else '结果行的维度')
        return '取列 `%s` 的第一行' % column
    if kind == 'raw_filtered':
        text = '按 %s 过滤后取首个匹配行的 `%s`' % (
            json.dumps(params.get('filter') or {}, ensure_ascii=False),
            params.get('column'))
        if params.get('cast') == 'int':
            text += '（转整数）'
        return text
    if kind == 'aggregate':
        group_by = params.get('group_by') or []
        text = '按 %s 过滤后对 `%s` 求和' % (
            json.dumps(params.get('filter') or {}, ensure_ascii=False),
            params.get('value_column', 'cnt'))
        if group_by:
            text += '，按 %s 分组各出一条' % _join_codes(group_by)
        else:
            text += '，整体一条'
        return text
    return None


def _describe_cross_rule(kind, params):
    """时序 / 集合级取值规则的人话口径；不认识的 kind 返回 None。"""
    if kind == 'counter_delta':
        text = '累计值 `%s` 跨轮差分算速率：本次值减上轮值再除以实际采集间隔秒数' % (
            params.get('column'))
        text += '；首轮只建基线不产出，实例重启导致计数回退时本轮丢弃并记错误日志'
        return text
    if kind == 'ratio_across':
        numerator = params.get('numerator') or {}
        denominator = params.get('denominator') or {}
        return ('%s 视图的 `%s` ÷ %s 视图的 `%s`，分母取不到时本轮不产出' % (
            numerator.get('sql_key'), numerator.get('value_column'),
            denominator.get('sql_key'), denominator.get('value_column')))
    if kind == 'series_point':
        position = params.get('position', 'last_non_null')
        label = {
            'last_non_null': '取时序上最后一个非空点',
            'first_non_null': '取时序上第一个非空点',
            'last': '取时序最后一个点',
            'first': '取时序第一个点',
        }.get(position, '取时序点（%s）' % position)
        return '%s（平台按组件给多条 series，每个组件各出一条）' % label
    if kind == 'enum_map':
        mapping = params.get('mapping') or {}
        pairs = '、'.join('%s→%s' % (key, value) for key, value in sorted(mapping.items()))
        return '时序点的值按枚举表翻译（%s）；未命中时原样透传并记告警日志' % pairs
    if kind == 'gauge':
        return '由平台给的启动时刻推算已运行时长（当前时刻减启动时刻，单位 %s）' % (
            params.get('unit', '秒'))
    if kind == 'top_n':
        text = '按 `%s` %s 排序取前 %s 条' % (
            params.get('sort_by'), params.get('order', 'desc'), params.get('limit', 10))
        if params.get('collection'):
            text += '，数据来自诊断优化接口的 `%s` 集合' % params['collection']
        if params.get('columns'):
            text += '，只保留字段 %s' % _join_codes(params['columns'])
        return text
    if kind == 'raw_rows':
        return '把该视图的整行列表原样带出（明细型指标）'
    if kind == 'collection_count':
        return '统计诊断优化接口 `%s` 集合的条数' % params.get('collection')
    if kind == 'text_prefix_count':
        return '统计 `%s` 集合里 `%s` 以 %s 开头的条数' % (
            params.get('collection'), params.get('field', 'sql_text'),
            _join_codes(params.get('prefixes') or ()))
    if kind == 'instance_field':
        return '取实例元数据里的 `%s` 字段' % params.get('field')
    return None


def describe_value_rule(rule: Optional[Dict[str, Any]]) -> str:
    """把 ``value_rule`` 翻译成一句人能读懂的口径。"""
    rule = rule or {}
    params = rule.get('params') or {}
    kind = rule.get('type')
    for describe in (_describe_row_rule, _describe_cross_rule):
        text = describe(kind, params)
        if text is not None:
            return text
    return '规则类型 %s' % kind


def describe_value_rule_generic(kind: str) -> str:
    """取值规则类型的通用口径（不带具体列名，用在「规则类型说明」表）。"""
    generic = {
        'raw': '直接取某一列的值（求和 / 一行一条 / 取第一行，由 `row_policy` 决定）',
        'raw_filtered': '按条件过滤后取**首个**匹配行的某一列',
        'aggregate': '按条件过滤后分组求和（分母/分子类的条件聚合）',
        'counter_delta': '累计计数**跨轮差分**算速率（本次值减上轮值 ÷ 实际间隔秒数）',
        'ratio_across': '跨视图的分子 ÷ 分母（分母取不到就不产出）',
        'series_point': '取时序上的一个点（平台按组件给多条 series）',
        'enum_map': '时序点的值按枚举表翻译（未命中原样透传并告警）',
        'gauge': '由平台给的启动时刻推算已运行时长',
        'top_n': '按排序列取前 N 条（可投影字段）',
        'raw_rows': '整行列表原样透传（明细型指标）',
        'collection_count': '统计诊断优化接口某个集合的条数',
        'text_prefix_count': '按 SQL 文本前缀给集合分类计数',
        'instance_field': '取实例元数据里的一个字段',
    }
    return generic.get(kind, '规则类型 %s' % kind)


def describe_source(rule: Dict[str, Any], views: Dict[str, Dict[str, Any]]) -> str:
    """一句话说清「从哪拿的」。"""
    source = rule.get('source') or {}
    method = source.get('method')
    api = source.get('api')
    sql_key = source.get('sql_key')
    tpops_source = source.get('tpops_source')
    if method == '命令中心SQL':
        view = views.get(sql_key) or {}
        text = '统一命令中心执行合并 SQL 视图 `%s`' % sql_key
        if view.get('source_view'):
            text += '（%s）' % view['source_view']
        if view.get('refresh_tier'):
            text += '，视图刷新档位 %s' % view['refresh_tier']
        return text
    if method == '监控指标接口':
        text = '监控指标接口 `%s`' % api
        if tpops_source:
            text += ' 的平台指标 `%s`' % tpops_source
        return text
    if method == '诊断优化接口':
        text = '诊断优化接口 `%s`' % api
        if tpops_source:
            text += ' 的 `%s`' % tpops_source
        return text
    if method == '实例接口':
        text = '实例接口 `%s`' % api
        if tpops_source:
            text += ' 的 `%s`' % tpops_source
        return text
    return str(method)


def describe_collection(rule: Dict[str, Any]) -> str:
    """一句话说清「归集口径」—— **按通道分支**。

    口径**不是**统一的：

    * **A 类（监控指标接口）** —— 平台按**组件**给多条 series，
      ``mapping/rules_series.eval_series_point`` 对 ``ctx.series`` 的**每个键
      各出一条**消息，维度键是 ``COMPONENT_ID``。**不跨组件合成**，
      ``node_scoped`` 对 A 类不参与归集。
    * **B 类（命令中心SQL）** —— 合并 SQL 在每个 master 各跑一次，
      ``node_scoped=true`` 的各 master 各自出一条（不合并、不求和），
      ``false`` 的只取承载资源名的第一个 master CN 的一条。
    * **C / D 类（诊断优化 / 实例接口）** —— 与节点无关，整条落到
      节点清单里的第一个 master CN。
    """
    source = (rule.get('source') or {}).get('source')
    if source == 'monitor_metrics':
        return COLLECTION_TEXT_MONITOR
    if source in ('diagnostic', 'instance'):
        return COLLECTION_TEXT_INSTANCE
    parts = []
    if rule.get('node_scoped'):
        parts.append('节点级：每个 master 各出一条（不跨 master 合并、不求和）')
    else:
        parts.append('实例级：只取第一个 master CN 的一条（值与资源名同源）')
    master_view = rule.get('master_view')
    if master_view == 'identical':
        parts.append('各 master 返回值一致')
    elif master_view == 'divergent':
        parts.append('各 master 返回值不同')
    return '；'.join(parts)


def describe_note(rule: Dict[str, Any]) -> str:
    """备注列（``note``；``NOTE_OVERRIDE`` 优先）。"""
    code = rule.get('metric_code')
    if code in NOTE_OVERRIDE:
        return NOTE_OVERRIDE[code]
    return (rule.get('note') or '').replace('|', '/')



def describe_cycle(rule: Dict[str, Any]) -> str:
    """刷新周期（人话）。"""
    cycle = rule.get('collect_cycle') or {}
    value = cycle.get('value')
    unit = CYCLE_UNIT_LABEL.get(cycle.get('unit'), cycle.get('unit') or '')
    return '%s %s' % (value, unit)


def describe_granularity(rule: Dict[str, Any]) -> str:
    """资源粒度（人话）。"""
    label = GRANULARITY_LABEL.get(rule.get('resource_granularity'), '实例级')
    placeholders = rule.get('placeholders') or []
    if placeholders:
        label += '（按 %s 展开）' % _join_codes(
            ['{#%s}' % item for item in placeholders])
    return label


def dimension_cell(rule: Dict[str, Any]) -> str:
    """维度列（占位符，写成指标名里实际的样子）。"""
    placeholders = rule.get('placeholders') or []
    if not placeholders:
        return '—'
    return _join_codes(['{#%s}' % item for item in placeholders])


# --------------------------------------------------------------------------
# 文档 1：指标口径说明
# --------------------------------------------------------------------------
def _group_by_channel(usable):
    """按取数通道分组，并固定成 A/B/C/D 的顺序（不随数据顺序漂移）。"""
    grouped = OrderedDict()
    for rule in usable:
        method = (rule.get('source') or {}).get('method') or '其他'
        grouped.setdefault(method, []).append(rule)
    channel_order = ['命令中心SQL', '监控指标接口', '诊断优化接口', '实例接口']
    ordered = OrderedDict()
    for method in channel_order:
        if method in grouped:
            ordered[method] = grouped[method]
    for method in grouped:
        if method not in ordered:
            ordered[method] = grouped[method]
    return ordered


def _spec_intro(lines: List[str], rules: Sequence[Dict[str, Any]],
                usable: Sequence[Dict[str, Any]],
                counts: Mapping[str, Any] = None) -> None:
    """文档开头：这份文档是什么、各列怎么读。"""
    lines.append('# GaussDB 指标口径说明（客户交付版）')
    lines.append('')
    lines.append('> 本文档回答客户最关心的问题：**这 %d 个指标，各自是从哪拿的、怎么算的、什么口径。**'
                 % len(usable))
    lines.append('>')
    lines.append('> - 数据来源：`tpops_kafka_collector/mapping/data/metric_rules.json`'
                 '（%d 条指标规则，全部可提供）' % len(rules))
    lines.append('> - 客户指标基准：`高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx`「指标库」sheet')
    lines.append('> - 生成方式：`python tools/build_delivery_docs.py`（表驱动，重跑输出不变）')
    lines.append('> - 配套文档：《交付-不可提供指标清单.md》《交付-客户指标英文名核对清单.md》')
    lines.append('')
    counts = counts or {}
    client_total = counts.get('client_total') or len(rules)
    excluded_total = counts.get('total') or (client_total - len(usable))
    duplicate_total = counts.get('duplicate') or 0
    unsupported_total = counts.get('unsupported') or 0
    lines.append('客户指标库共 %d 个指标：**%d 个可提供**（本文档逐条说明口径），'
                 '**%d 个不在生效口径内**（%d 条重复的英文变体 + %d 个无法提供 —— '
                 '已在**构建期**从生效口径移除，运行期不产出消息；'
                 '见《交付-不可提供指标清单.md》与 `excluded_metrics.json`）。' % (
                     client_total, len(usable), excluded_total,
                     duplicate_total, unsupported_total))
    lines.append('')
    lines.append('## 1. 怎么读这份表')
    lines.append('')
    lines.append('| 列 | 含义 |')
    lines.append('|---|---|')
    lines.append('| 客户指标英文名 | 就是客户指标库「英文名」列的值，投递到 Kafka 的 `metricCode` 字段 |')
    lines.append('| 中文名 | 客户指标库「中文名」列的值，投递到 Kafka 的 `metricName` 字段 |')
    lines.append('| 取数通道 | 走 TPOPS 的哪一条接口 |')
    lines.append('| 来源 | 具体接口 / 平台指标名 / 合并 SQL 视图 |')
    lines.append('| 取值规则 | 数据拿到之后**怎么变成客户的值**（求和？差分？取最后一个点？） |')
    lines.append('| 声明周期 | 客户指标库「数据更新周期」列声明的**需求档**（= 映射表的 `collect_cycle`） |')
    lines.append('| 实际产出频率 | 这条指标**实际**多久产出一条 —— 取所属通道的节奏（见 3.7），**不等于**声明周期 |')
    lines.append('| 资源粒度 | 一条消息代表整个实例，还是按表 / 库 / 端口展开 |')
    lines.append('| 归集口径 | 多 master 场景下这条消息出几条、值从哪来（见第 3 节） |')
    lines.append('| 维度 | 指标名里的占位符，运行时会被替换成实际维度值（表名 / 库名 / 端口） |')
    lines.append('| 备注 | 口径上的注意点与限制 |')
    lines.append('')
    lines.append('> **占位符说明**：客户指标名里的 `{#NAME}` / `{#DATNAME}` / `{#PORT}` 是维度占位符，'
                 '投递时替换成真实的表名 / 库名 / 端口号'
                 '（例如 `gaussdb.{#NAME}.number.Of.index.scan` 里的 `{#NAME}` 会变成实际的表名），'
                 '实际值同时放进 `tags.dimension_values`。')
    lines.append('')


def _spec_channel_overview(lines: List[str], by_channel: OrderedDict,
                           periods: Dict[str, int],
                           method_to_period: Dict[str, int]) -> None:
    """第 2 节：取数通道总览。"""
    lines.append('## 2. 取数通道总览')
    lines.append('')
    lines.append('| 取数通道 | TPOPS 接口 | 指标数 | 实际产出频率 | 说明 |')
    lines.append('|---|---|---|---|---|')
    channel_desc = {
        '命令中心SQL': ('`POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}`',
                    '在 master 节点上执行合并 SQL（11 条合并 SQL 覆盖 %d 个指标），返回**当前快照**'
                    % len(by_channel.get('命令中心SQL') or ())),
        '监控指标接口': ('`POST /rds/v3/monitor/metrics`',
                    '按时间窗口取**时序**数据，平台侧已是速率口径的指标直接取点'),
        '诊断优化接口': ('`POST /rds/gaussdb/v3/instances/{instance_id}/slow-sql-list`'
                    ' / `top-sql-list`', '慢 SQL 与 Top10 SQL 的集合统计与明细'),
        '实例接口': ('`GET /rds/v3.2/instances`', '实例元数据（版本号等）'),
    }
    for method, items in by_channel.items():
        api, desc = channel_desc.get(method, ('-', '-'))
        period = method_to_period.get(method)
        freq = format_seconds(period) if period else '-'
        lines.append('| %s | %s | %d | %s | %s |' % (method, api, len(items), freq, desc))
    lines.append('')
    lines.append('> **「实际产出频率」是通道级的**：同一个通道里的**所有**指标都按这个节奏产出，'
                 '与各自的「声明周期」无关 —— 详见 3.7。')
    lines.append('')


def _spec_granularity(lines: List[str], usable: Sequence[Dict[str, Any]]) -> None:
    """第 3.1 节：资源粒度。"""
    lines.append('## 3. 资源粒度与多 master 归集口径')
    lines.append('')
    lines.append('这一节回答「**一条消息代表什么**」「**同一个实例有多个 master 时会出几条消息**」。')
    lines.append('')
    lines.append('### 3.1 资源粒度')
    lines.append('')
    lines.append('`resourceName` 按客户口径**只能表示一个节点**，固定为「节点 IP + 节点 ID」'
                 '（模板 `{host_ip}_{node_id}`）。因此不同粒度的指标落在不同的地方：')
    lines.append('')
    lines.append('| 资源粒度 | 指标数 | 消息形态 | 维度 |')
    lines.append('|---|---|---|---|')
    gran_count = OrderedDict()
    for rule in usable:
        gran_count[rule.get('resource_granularity')] = \
            gran_count.get(rule.get('resource_granularity'), 0) + 1
    for gran in ('instance', 'table', 'database', 'port'):
        if gran not in gran_count:
            continue
        shape = {
            'instance': '一个实例一条（挂到第一个 master CN）；'
                        '**A 类的实例级指标例外：逐组件各出一条**（见 3.5）',
            'table': '一张表一条（表名进指标名占位符）',
            'database': '一个库一条（库名进指标名占位符）',
            'port': '一个端口一条（端口号进指标名占位符）',
        }[gran]
        dims = {
            'instance': '无',
            'table': '`{#NAME}`',
            'database': '`{#DATNAME}`',
            'port': '`{#PORT}`',
        }[gran]
        lines.append('| %s | %d | %s | %s |' % (
            GRANULARITY_LABEL[gran], gran_count[gran], shape, dims))
    lines.append('')


def _spec_node_scoped(lines: List[str], usable: Sequence[Dict[str, Any]], views: Dict[str, Dict[str, Any]]) -> None:
    """第 3.2 节：node_scoped 归集口径。"""
    lines.append('### 3.2 `node_scoped`：节点级视图 vs 实例级视图')
    lines.append('')
    lines.append('命令中心的合并 SQL 会在**每个 master 节点各执行一次**，同一组视图因此返回多份结果。'
                 '归集口径由映射表的 `node_scoped` 字段决定（该字段是**实测判定**的结论）：')
    lines.append('')
    lines.append('> **本节只适用于命令中心（B 类）通道。**'
                 'A 类（监控指标接口）走的是「逐组件各出一条」（见 3.5），'
                 'C / D 类与节点无关（整条落到第一个 master CN）—— '
                 '它们都**不适用** master 维度的归集口径。')
    lines.append('')
    lines.append('| 合并 SQL 视图 | `node_scoped` | 含义 | 产出条数 | 底层视图 |')
    lines.append('|---|---|---|---|---|')
    scoped_views = OrderedDict()
    for rule in usable:
        sql_key = (rule.get('source') or {}).get('sql_key')
        if not sql_key:
            continue
        scoped_views.setdefault(sql_key, set()).add(bool(rule.get('node_scoped')))
    for sql_key in sorted(scoped_views):
        flags = scoped_views[sql_key]
        if len(flags) != 1:
            continue
        scoped = list(flags)[0]
        view = views.get(sql_key) or {}
        lines.append('| `%s` | `%s` | %s | %s | %s |' % (
            sql_key, 'true' if scoped else 'false',
            '节点级（各 master 值不同）' if scoped else '实例级（各 master 值相同）',
            '每个 master 一条' if scoped else '只取第一个 master 的一条',
            view.get('source_view', '-') or '-'))
    lines.append('')
    lines.append('- **节点级（`node_scoped = true`）**：各 master 的结果**各自成一份数据**，'
                 '**不跨 master 合并、不求和**。实测同一实例三个 master 的 `pg_stat_database` '
                 '首行 `xact_commit` 分别是 153291 / 153299 / 146452 —— 求和会得到没有业务含义的 3 倍累计值。')
    lines.append('- **实例级（`node_scoped = false`）**：各 master 返回相同的内容，**只取第一个 master CN 的一份**，'
                 '不重复产出三份。')
    lines.append('- **`master_view`**：实测出来的「各 master 是否一致」——`identical` 表示各 master 值相同'
                 '（实例级），`divergent` 表示各 master 值不同（节点级）。'
                 '本文档「归集口径」列已把它写进每一条指标。')
    lines.append('')


def _spec_multi_master(lines: List[str]) -> None:
    """第 3.3~3.6 节：多 master 的下发、去重与取值。"""
    lines.append('### 3.3 多 master 的下发与去重')
    lines.append('')
    lines.append('- 节点清单来自 `GET /rds/gaussdb/v3/instances/{id}/key-view-execute-node`，'
                 '**同一个 `node_id` 会在清单里出现多次**（每个 node_id 同时以 CN 和 DN 各出现一条，'
                 '实测 12 条清单条目只有 3 个唯一 `node_id`，其中 6 条 `role == "master"`）。'
                 '下发前**按 `node_id` 去重**，否则会重复下发。')
    lines.append('- 命令中心要求**只能在 master 节点执行**，所以下发的目标是**去重后的 master CN 列表**；'
                 '实测一次下发多个 master 节点是成功的。')
    lines.append('- 被选中的那个 master 在**各轮之间保持不变**（按节点清单顺序取第一个 master CN），'
                 '资源名不会因为「这轮 SQL 在哪个 master 上跑」而漂移。')
    lines.append('')
    lines.append('### 3.4 与节点无关的指标落在哪')
    lines.append('')
    lines.append('命令中心的实例级 / 表级 / 库级指标、诊断优化指标、实例接口指标都**与具体节点无关**，'
                 '统一挂到节点清单里的**第一个 master CN**（`role=master` 且 `type=CN`）上，'
                 '保证跨轮稳定。')
    lines.append('')
    lines.append('值与资源名**必须同源**：实例级指标的值取自承载资源名的那个 master。'
                 '若该 master 本轮没有返回结果，且该指标的 `master_view` 不是 `identical`'
                 '（值本来就可能因节点而异），**本轮不产出这条消息**，并记一条告警日志 —— '
                 '宁可少一条，也不用「A 节点的值配 B 节点的名字」。')
    lines.append('')
    lines.append('### 3.5 A 类（监控指标接口）的多 series 归集')
    lines.append('')
    lines.append('A 类的 11 个客户指标在客户库里都是**实例级一条**的消息，但平台按**组件**给出**多条 series**'
                 '（例如 `rds092_gaussv5_commit_counter` 对 3 个 CN 各给一条 series，'
                 '`datanode_switch_status` 对 9 个 DN 各给一条）。')
    lines.append('')
    lines.append('**现状实现**：**逐组件各出一条**消息 —— 每条 series 一条，'
                 '维度键 `COMPONENT_ID` 放进 `tags`，**不跨组件合成**；'
                 '资源名取该组件所在节点的 `{host_ip}_{node_id}`'
                 '（组件换算不到节点时回落到第一个 master CN）。')
    lines.append('')
    lines.append('下面是**用采集端代码回放 `data/metric_response/` 里的原生响应实测**的条数'
                 '（每个指标各一条消息 × 该指标的组件数）：')
    lines.append('')
    lines.append('| 客户指标 | 平台指标 | 一轮产出的消息条数 | 组件 |')
    lines.append('|---|---|---|---|')
    lines.append('| `gaussdb.Tps` | `rds400_gaussv5_total_counter` | 3 | 3 个 CN |')
    lines.append('| `gaussdb.QPS` | `rds402_gaussv5_ddl_dml_dcl_count` | 6 | 3 个 CN + 3 个 DN |')
    lines.append('| `GAUSSDB.Standby_status` | `datanode_switch_status` | 9 | 9 个 DN |')
    lines.append('| `GAUSSDB.Primary_status` | `datanode_switch_status` | 9 | 9 个 DN |')
    lines.append('| `gaussdb.Starttime` / `gaussdb.Uptimestr` | `rds133_cn_startup_time` | 各 3 | 3 个 CN |')
    lines.append('| `GAUSSDB.Number_of_ansactions_mmitted_r_cond` | `rds092_gaussv5_commit_counter` | 3 | 3 个 CN |')
    lines.append('| `GAUSSDB.Number_of_llback_ansactions_r_cond` | `rds093_gaussv5_rollback_counter` | 3 | 3 个 CN |')
    lines.append('| `GAUSSDB.Service_availability_testing` / `GAUSSDB.PG_Service_Status` / `gaussdb.Status` '
                 '| `gaussdb_instance_status` | 各 1 | 单值 |')
    lines.append('')
    lines.append('> **客户按「一个实例一条」估算 A 类消息量会算少**：'
                 '上表里最多的一个指标一轮就出 9 条。'
                 '合成口径（速率类求和 / 启动时刻取最早 / 主备状态按 DN 展开或归集为布尔）属业务口径，'
                 '**待客户确认**；确认后由映射表的 `value_rule` 承载，不改消息信封。')
    lines.append('')
    lines.append('### 3.6 累计计数的差分基线按节点隔离')
    lines.append('')
    lines.append('需要差分算速率的累计计数类指标（每秒 Fetch / Insert / Return / Update / Delete 记录数），'
                 '基线按**节点**隔离：基线键是二元组 `(指标名, 作用域:维度)`，'
                 '作用域一般是 `node_id`（必要时还带实例 ID）—— '
                 '效果等价于「指标名 + 维度 + 节点」三元组，'
                 '避免多个 master 共用一个存储时互相覆盖基线、算出跨节点的假速率。')
    lines.append('')


def _cycle_seconds(rule: Dict[str, Any]) -> int:
    """规则的声明周期（秒）—— 与 ``collectors.instance.collect_cycle_seconds`` 同口径。"""
    cycle = rule.get('collect_cycle') or {}
    unit = str(cycle.get('unit') or '')
    factor = {'秒': 1, '分': 60, '时': 3600, '天': 86400}.get(unit)
    if factor is None:
        return 0
    try:
        return int(cycle.get('value')) * factor
    except (TypeError, ValueError):
        return 0


def _spec_cycles(lines: List[str], rules: Sequence[Dict[str, Any]],
                 method_to_period: Dict[str, int],
                 by_code: Dict[str, Optional[int]]) -> None:
    """第 3.7 节：声明周期 vs 实际产出频率。"""
    lines.append('### 3.7 声明周期 ≠ 实际产出频率（**超服务**，请按实际频率配容量）')
    lines.append('')
    lines.append('这一节是**容量与带宽口径**，请客户重点看。')
    lines.append('')
    lines.append('| 口径 | 含义 | 来自哪里 |')
    lines.append('|---|---|---|')
    lines.append('| **声明周期** | 客户指标库「数据更新周期」列声明的**需求档** | '
                 '客户指标表 / 映射表的 `collect_cycle` |')
    lines.append('| **实际产出频率** | 这条指标**实际**多久产出一条 | '
                 '**所属通道**的调度周期（该通道内最高需求档 = 最短周期） |')
    lines.append('')
    lines.append('**当前实现里没有按指标 `collect_cycle` 的产出闸门** —— '
                 '采集器一轮会把该通道的**全部**规则都产出。'
                 '因此**同一通道里的所有指标都按该通道的频率产出**，'
                 '与各自的声明周期无关。')
    lines.append('')
    lines.append('| 取数通道 | 实际产出频率 | 覆盖指标数 | 通道内的声明周期分布 |')
    lines.append('|---|---|---|---|')
    method_of = source_method_map(rules)
    for method in sorted(method_to_period, key=lambda item: method_to_period[item]):
        items = [rule for rule in rules
                 if rule.get('enabled')
                 and method_of.get((rule.get('source') or {}).get('source')) == method]
        if not items:
            continue
        declared = sorted(set(describe_cycle(rule) for rule in items))
        lines.append('| %s | %s | %d | %s |' % (
            method, format_seconds(method_to_period[method]), len(items),
            '、'.join(declared)))
    lines.append('')
    overserved = []
    for rule in rules:
        if not rule.get('enabled'):
            continue
        period = by_code.get(rule.get('metric_code'))
        declared = _cycle_seconds(rule)
        if not period or not declared:
            continue
        if declared > period:
            overserved.append(rule.get('metric_code'))
    lines.append('**%d 个指标的声明周期慢于其通道的实际节奏**（即「超服务」）：'
                 '它们**按通道频率**产出，不是按声明周期。'
                 % len(overserved))
    lines.append('')
    lines.append('> 影响客户侧的量级：按「每条规则每轮一条」估算，'
                 '单个实例**每天至少 %s 条**指标消息'
                 '（A 类的多 series 展开、表级 / 库级 / 端口级指标的按行展开会让实际条数**更多**）。'
                 '客户侧请按这个量级准备存储与带宽，并**按「实际产出频率」列**配置保留策略与告警阈值；'
                 '若需要按声明周期节流，需要我们增加产出闸门（改采集端逻辑，不改消息格式）。'
                 % format_int(daily_message_volume(rules, by_code)))
    lines.append('')


def _spec_rule_types(lines: List[str], usable: Sequence[Dict[str, Any]]) -> None:
    """第 4 节：取值规则类型说明。"""
    lines.append('## 4. 取值规则类型说明')
    lines.append('')
    type_count = OrderedDict()
    for rule in usable:
        kind = (rule.get('value_rule') or {}).get('type')
        type_count[kind] = type_count.get(kind, 0) + 1
    lines.append('%d 个指标一共用了 %d 种取值规则（`value_rule.type`），逐条口径见第 5 节。' % (
        len(usable), len(type_count)))
    lines.append('')
    lines.append('| 规则类型 | 人话口径 | 指标数 |')
    lines.append('|---|---|---|')
    for kind in sorted(type_count, key=lambda item: (-type_count[item], str(item))):
        lines.append('| `%s` | %s | %d |' % (
            kind, describe_value_rule_generic(str(kind)), type_count[kind]))
    lines.append('')


def _spec_metric_sections(lines: List[str], by_channel: OrderedDict,
                          views: Dict[str, Dict[str, Any]],
                          by_code: Dict[str, Optional[int]]) -> None:
    """第 5 节：逐指标口径。"""
    lines.append('## 5. 逐指标口径')
    lines.append('')
    section_no = 0
    for method, items in by_channel.items():
        section_no += 1
        lines.append('### 5.%d %s（%d 个）' % (section_no, method, len(items)))
        lines.append('')
        lines.append('本表按取数通道分节，编号在节内连续。')
        lines.append('')
        lines.append(TABLE_MARKS['metrics'])
        lines.append('')
        lines.append('| # | 客户指标英文名 | 中文名 | 取数通道 | 来源 | 取值规则 | '
                     '声明周期 | 实际产出频率 | 资源粒度 | 归集口径 | 维度 | 备注 |')
        lines.append('|---|---|---|---|---|---|---|---|---|---|---|---|')
        for index, rule in enumerate(items, 1):
            code = rule.get('metric_code')
            name = rule.get('metric_name')
            dims = dimension_cell(rule)
            note = describe_note(rule)
            period = by_code.get(code)
            frequency = format_seconds(period) if period else '—'
            lines.append('| %d | `%s` | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |' % (
                index, code, name, method, describe_source(rule, views),
                describe_value_rule(rule.get('value_rule')),
                describe_cycle(rule), frequency,
                describe_granularity(rule),
                describe_collection(rule), dims, note or '—'))
        lines.append('')


def _spec_tail(lines: List[str]) -> None:
    """第 6、7 节：通用约定与待确认项。"""
    lines.append('## 6. 口径上的通用约定')
    lines.append('')
    lines.append('1. **缺值不产出假数据**：取不到值时该指标本轮不产出消息，'
                 '不填 0、不填上一轮的值。')
    lines.append('2. **先投成功再推进水位**：一轮的顺序是「拉取 → 转换 → 投递 → 成功后推进水位」，'
                 '投递失败不推进水位，重启后从水位续传（至少一次语义）。')
    lines.append('3. **值的线格式**按客户指标库「数据类型」列序列化：整数 / 浮点转数字，'
                 '文本转字符串；文本型明细指标按原样带出（列表）。'
                 '两份对接方案的文件字段表都写 `value` 是 String，这一项**待客户确认**。')
    lines.append('4. **慢 SQL 文本默认脱敏**：SQL 字面量替换为 `?` 并截断到 512 字符，'
                 '是否允许外发原始 `sql_text` **待客户书面确认**。')
    lines.append('')
    lines.append('## 7. 仍需客户确认的口径项')
    lines.append('')
    lines.append('以下几项已按默认值实现，确认后只改映射表即可，不改采集逻辑'
                 '（完整清单见 `tpops_kafka_collector/mapping/data/open_items.json`）：')
    lines.append('')
    lines.append('| 事项 | 当前默认口径 |')
    lines.append('|---|---|')
    lines.append('| A 类多 series 归集 | 逐组件各出一条消息，`COMPONENT_ID` 进 `tags`（见 3.5） |')
    lines.append('| 状态类指标的展示值 | 平台枚举值原样透传（实测 1=正常、0=异常） |')
    lines.append('| 采集频率 | **实际产出频率**取通道周期（监控指标 30 秒 / 命令中心 60 秒 / '
                 '诊断优化 120 秒 / 实例 3600 秒）；客户指标库的「数据更新周期」是**声明周期**，'
                 '当前**不作为产出闸门**，同通道指标一律按通道频率产出（见 3.7） |')
    lines.append('| `value` 的线格式 | 按客户指标库「数据类型」序列化 |')
    lines.append('| 慢 SQL 的 `sql_text` | 默认脱敏 + 截断 512 字符 |')
    lines.append('| `resourceName` 与台账对齐 | `{host_ip}_{node_id}`，待客户提供台账清单后核对 |')
    lines.append('')


def render_metric_spec(rules: Sequence[Dict[str, Any]], views: Dict[str, Dict[str, Any]],
                       counts: Mapping[str, Any] = None) -> str:
    """生成《交付-指标口径说明.md》。"""
    usable = enabled_rules(rules)
    by_channel = _group_by_channel(usable)
    by_code, channel_periods = channel_schedule(rules)
    method_of = source_method_map(rules)
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
    from tpops_kafka_collector.schedule import SOURCE_TO_CHANNEL
    method_to_period = {}
    for raw, method in method_of.items():
        channel = SOURCE_TO_CHANNEL.get(raw)
        if channel and channel in channel_periods:
            method_to_period[method] = channel_periods[channel]
    lines = []
    _spec_intro(lines, rules, usable, counts)
    _spec_channel_overview(lines, by_channel, channel_periods, method_to_period)
    _spec_granularity(lines, usable)
    _spec_node_scoped(lines, usable, views)
    _spec_multi_master(lines)
    _spec_cycles(lines, usable, method_to_period, by_code)
    _spec_rule_types(lines, usable)
    _spec_metric_sections(lines, by_channel, views, by_code)
    _spec_tail(lines)
    return '\n'.join(lines) + '\n'

# --------------------------------------------------------------------------
# 文档 2：不可提供指标清单
# --------------------------------------------------------------------------
#: 变通方案（人工判读；「为什么给不了」本身来自映射表的 unsupported_reason）
#: 结构：``metric_code`` → (清单表里的简短结论, 逐条说明里的详细方案)
UNSUPPORTED_WORKAROUND = {
    'gaussdb.Fastpath_function_call': (
        '可以做近似，但口径不等价（详见 3.1）',
        '**可以做近似，但口径不等价。**若客户要的是「当前有多少连接正在走快路径调用内置函数」，'
        '可在命令中心里对 `pg_stat_activity` 做一次附加统计（`state = \'active\'` 且 `query` '
        '命中内置函数名白名单）来近似。注意两点：一是这是**瞬时快照**的连接数，'
        '不是「累计快路径调用次数」；二是白名单需要客户给出他们关心的内置函数清单。'
        '若客户其实想要的是「快路径开关状态」，那是一条配置项（`pg_settings.gsi_fastpath_level`），'
        '可以单独作为一条状态类指标提供。'),
    'gaussdb.Disabled_conn': (
        '不能直接给出，可按容量口径换算（详见 3.2）',
        '**不能直接给出，但可以按容量口径换算。**客户指标库里「禁用的连接数」的常见业务含义是'
        '「因为连接策略 / 预留策略而无法被使用的连接数」。若客户确认是这个含义，'
        '可以换算为 `max_connections - numbackends - reserved_connections`'
        '（可建立总量 - 已建立 - 预留），这是一条**容量差值**，'
        '和「被禁用的连接」不是同一个东西，口径需要客户书面确认后才提供。'
        '若客户指的是「连接被拒绝的次数」，平台侧没有对应计数器，同样无法提供。'),
    'gaussdb.sendAgentName': (
        '无法从数据库侧提供，应由采集 Agent 侧承担（详见 3.3）',
        '**无法从数据库侧提供，应由采集 Agent 侧承担。**这条不是数据库的监控指标，'
        '而是「把设备名发给 Agent」这个 Agent 行为本身。可行的做法有两个：'
        '一是让部署侧的 Agent 在上报心跳时直接带上主机名；'
        '二是由采集端把节点清单里的节点名 / 主机名放进 `tags`（`tags.node_id`、`tags.instance_name` 已有），'
        '供客户侧自行关联 —— 但这属于「顺带带上」，不能算作一条指标。'
        '若客户必须要这条指标，需要在 Agent 侧新增一个采集点。'),
}


def _unsupported_intro(lines, items, counts):
    """开头：这份清单是什么、3 条指标怎么处置。"""
    client_total = counts.get('client_total')
    effective_total = counts.get('effective_total')
    lines.append('# 不可提供指标清单（客户交付版）')
    lines.append('')
    lines.append('客户指标库共 %s 个指标，其中 **%s 个可提供**（口径见《交付-指标口径说明.md》），'
                 '**%d 个无法提供**。本文档逐条说明：客户要什么、我们为什么给不了、能不能变通。' % (
                     client_total, effective_total, len(items)))
    lines.append('')
    lines.append('> - 数据来源：`tpops_kafka_collector/mapping/data/excluded_metrics.json`'
                 '（`reason = unsupported` 的 %d 条，每条自带排除理由）' % len(items))
    lines.append('> - 生成方式：`python tools/build_delivery_docs.py`（表驱动，重跑输出不变）')
    lines.append('')
    lines.append('## 1. 处置方式')
    lines.append('')
    lines.append('- 这 %d 个指标**不会出现在 Kafka 消息里**'
                 '（**已从生效口径移除**）。' % len(items))
    lines.append('- 处置方式是**构建期排除**：它们在生成生效映射表时就被排除清单剔除了，'
                 '**生效映射表里根本没有它们** —— 运行期没有「遍历到再丢弃」的分支，'
                 '轮汇总里的 `dropped_unsupported` 恒为 **0**。')
    lines.append('- 排除记录（指标码 + 理由）留在 '
                 '`tpops_kafka_collector/mapping/data/excluded_metrics.json`，'
                 '客户若要恢复其中某条，改数据文件即可，不用改代码。')
    lines.append('')


def _unsupported_table(lines, items):
    """第 2 节：清单表。"""
    lines.append('## 2. 清单')
    lines.append('')
    lines.append(TABLE_MARKS['unsupported'])
    lines.append('')
    lines.append('| # | 客户指标英文名 | 中文名 | 客户要什么 | 为什么给不了（技术原因） | 能不能变通 |')
    lines.append('|---|---|---|---|---|---|')
    for index, rule in enumerate(items, 1):
        code = rule.get('metric_code')
        name = rule.get('metric_name')
        category = rule.get('category') or '-'
        value_type = rule.get('value_type') or '-'
        reason = (rule.get('unsupported_reason') or rule.get('note') or '').replace('|', '/')
        summary, _detail = UNSUPPORTED_WORKAROUND.get(code, ('暂无可行变通方案。', ''))
        lines.append('| %d | `%s` | %s | %s（%s，%s，采集周期 %s） | %s | %s |' % (
            index, code, name, name, category, value_type, describe_cycle(rule),
            reason, summary))
    lines.append('')


def _unsupported_details(lines, items):
    """第 3 节：逐条说明（要什么 / 为什么给不了 / 能不能变通）。"""
    lines.append('## 3. 逐条说明')
    lines.append('')
    for index, rule in enumerate(items, 1):
        code = rule.get('metric_code')
        reason = (rule.get('unsupported_reason') or rule.get('note') or '')
        _summary, workaround = UNSUPPORTED_WORKAROUND.get(code, ('', '暂无可行变通方案。'))
        lines.append('### 3.%d %s（%s）' % (index, rule.get('metric_name'), code))
        lines.append('')
        lines.append('| 项 | 内容 |')
        lines.append('|---|---|')
        lines.append('| 客户指标英文名 | `%s` |' % code)
        lines.append('| 中文名 | %s |' % rule.get('metric_name'))
        lines.append('| 指标分类 | %s |' % (rule.get('category') or '-'))
        lines.append('| 数据类型 / 单位 | %s / %s |' % (
            rule.get('value_type') or '-', rule.get('unit') or '-'))
        lines.append('| 采集周期 | %s |' % describe_cycle(rule))
        lines.append('')
        lines.append('**客户要什么**：%s' % rule.get('metric_name'))
        lines.append('')
        lines.append('**为什么给不了**：%s' % reason)
        lines.append('')
        lines.append('**能不能变通**：%s' % workaround)
        lines.append('')


def _unsupported_tail(lines, items):
    """第 4 节：客户坚持要这 3 条时怎么办。"""
    lines.append('## 4. 如果客户坚持要这 3 条')
    lines.append('')
    lines.append('1. 请客户先书面确认每条指标的**业务定义**（尤其是「禁用连接」到底指什么）。')
    lines.append('2. 我们按上面的「变通」口径给出**替代指标**（新指标名 + 取数方式 + 口径说明），'
                 '由客户决定是否接受替代。')
    lines.append('3. 如果客户接受替代口径，我们会**新增指标**而不是改这 3 条的英文名 —— '
                 '客户指标库里的英文名以《交付-客户指标英文名核对清单.md》核对完的为准。')
    lines.append('')


def render_unsupported(rules, customer_rows):
    """生成《交付-不可提供指标清单.md》。

    条目来自**排除清单**（`reason = unsupported`）—— 被排除的指标根本不在
    生效映射表里，不能再从映射表里找「`enabled = false` 的规则」。
    """
    excluded_payload = load_excluded_metrics()
    items = unsupported_items(excluded_payload.get('excluded') or (), customer_rows)
    counts = excluded_payload.get('counts') or {}
    lines = []
    _unsupported_intro(lines, items, counts)
    _unsupported_table(lines, items)
    _unsupported_details(lines, items)
    _unsupported_tail(lines, items)
    return '\n'.join(lines) + '\n'


# --------------------------------------------------------------------------
# 文档 3：客户指标英文名核对清单（OCR 错字）
# --------------------------------------------------------------------------
#: 人工复核结果：``metric_code`` → [(疑似片段, 建议写法, 理由), ...]
#: 自动检测见 ``detect_ocr_findings``；两者的差异会写进文档的附录。
#:
#: **理由里的「少了什么」一律由 ``describe_char_diff`` 逐字符算出来**，
#: 不手写 —— 手写就会写出「少了一个 m 和一个 t」这种和实际不符的话。
OCR_REVIEW = OrderedDict([
    ('GAUSSDB.Number_of_ansactions_mmitted_r_cond', [
        ('ansactions', 'transactions', '中文名「每秒提交事务数」、单位 tps'),
        ('mmitted', 'committed', '同族指标写作 committed transactions'),
        ('r_cond', 'per_second', '中文名「每秒提交事务数」、单位 tps'),
    ]),
    ('GAUSSDB.Number_of_llback_ansactions_r_cond', [
        ('llback', 'rollback', '中文名「每秒回滚事务数」'),
        ('ansactions', 'transactions', '中文名「每秒回滚事务数」'),
        ('r_cond', 'per_second', '中文名「每秒回滚事务数」'),
    ]),
    ('gaussdb.{#NAME}.Number.lete.ples', [
        ('lete', 'delete', '同族中文名写作 number of delete tuples'),
        ('ples', 'tuples', '同族中文名写作 number of delete tuples'),
    ]),
    ('gaussdb.{#NAME}.Number.ad.ples', [
        ('ad', 'dead', '中文名「死亡行」；同族中文名写作 number of dead tuples'),
        ('ples', 'tuples', '同族中文名写作 number of dead tuples'),
    ]),
    ('gaussdb.{#NAME}.Number.t.date.ples', [
        ('t.date', 'update', '中文名「通过 HOT 更新的行数」；同族中文名写作 number of hot update tuples'),
        ('ples', 'tuples', '同族中文名写作 number of hot update tuples'),
    ]),
    ('gaussdb.{#NAME}.number.Of.quential.ples.ad', [
        ('quential', 'sequential', '同族英文名列写作 number.Of.sequential.scan'),
        ('ples', 'tuples', '同族中文名写作 number of index tuples read'),
        ('ad', 'read', '同族中文名写作 number of index tuples read'),
    ]),
    ('gaussdb.{#NAME}.number.Of.dex.ples.ad', [
        ('dex', 'index', '同族英文名列写作 number.Of.index.scan'),
        ('ples', 'tuples', '同族中文名写作 number of index tuples read'),
        ('ad', 'read', '同族中文名写作 number of index tuples read'),
    ]),
    ('GAUSSDB.{#NAME}.Number.ad.ples', [
        ('ad', 'dead', '同族中文名写作 number of dead tuples'),
        ('ples', 'tuples', '同族中文名写作 number of dead tuples'),
    ]),
    ('GAUSSDB.{#NAME}.Number.lete.ples', [
        ('lete', 'delete', '同族中文名写作 number of delete tuples'),
        ('ples', 'tuples', '同族中文名写作 number of delete tuples'),
    ]),
    ('GAUSSDB.{#NAME}.Number.t.date.ples', [
        ('t.date', 'update', '同族中文名写作 number of hot update tuples'),
        ('ples', 'tuples', '同族中文名写作 number of hot update tuples'),
    ]),
    ('GAUSSDB.{#NAME}.number.Of.dex.ples.ad', [
        ('dex', 'index', '同族英文名列写作 number.Of.index.scan'),
        ('ples', 'tuples', '同族中文名写作 number of index tuples read'),
        ('ad', 'read', '同族中文名写作 number of index tuples read'),
    ]),
    ('GAUSSDB.{#NAME}.number.Of.quential.ples.ad', [
        ('quential', 'sequential', '同族英文名列写作 number.Of.sequential.scan'),
        ('ples', 'tuples', '同族中文名写作 number of index tuples read'),
        ('ad', 'read', '同族中文名写作 number of index tuples read'),
    ]),
    ('GAUSSDB.Checkpoint_count_eckpoint_gments', [
        ('eckpoint', 'checkpoint', '本指标中文名写作 Checkpoint count (by checkpoint segments)'),
        ('gments', 'segments', '本指标中文名写作 Checkpoint count (by checkpoint segments)'),
    ]),
    ('GAUSSDB.Checkpoint_count_eckpoint_meout', [
        ('eckpoint', 'checkpoint', '本指标中文名写作 Checkpoint count (by checkpoint timeout)'),
        ('meout', 'timeout', '本指标中文名写作 Checkpoint count (by checkpoint timeout)'),
    ]),
])

#: 中文名列里的错字（``metric_code`` → [(错处, 建议写法, 理由)]）
#:
#: **这一列自动判据覆盖不到**（自动判据只扫英文名列的词元）——
#: 下面 3 条是**逐字符人工核对**出来的，来源：客户表 xlsx 的「中文名」列。
OCR_DESC_REVIEW = OrderedDict([
    ('GAUSSDB.[{#DATNAME}]_committed_transactions', [
        ('Commited', 'Committed', '同一行的英文名列写的是 committed（正确）'),
    ]),
    ('GAUSSDB.{#NAME}.number.Of.sequential.scan', [
        ('sequencial', 'sequential', '同一行的英文名列写的是 sequential（正确）'),
    ]),
    ('GAUSSDB.{#NAME}.number.Of.quential.ples.ad', [
        ('sequencial', 'sequential',
         '同族英文名列 gaussdb.{#NAME}.number.Of.sequential.scan 写的是 sequential（正确）'),
    ]),
])

#: 人工逐字符核对确认的错字片段（出现在客户表的哪一列都算）。
#: 守卫据此检查「客户表里凡是含这些片段的行，清单里都必须有对应条目」。
OCR_CONFIRMED_TOKENS = ('sequencial', 'Commited')

#: 自动规则命中、但人工复核判定为**正常**的项（写进附录，说明判据不是盲用）
OCR_ACCEPTED = {
    ('gaussdb.Idle_in_transaction', 'in', 'info'): 'in 是 in transaction 的正常写法',
    ('gaussdb.Idle_in_transaction_aborted', 'in', 'info'): 'in 是 in transaction 的正常写法',
    ('GAUSSDB.Idle_in_transaction_connections', 'in', 'info'): 'in 是 in transaction 的正常写法',
    ('gaussdb.Blk_write_time', 'write', 'written'): 'write 是「写」的正确写法，written 是其过去式',
}

CJK_RE = re.compile(r'[一-鿿]')
CODE_SEP_RE = re.compile(r'[._]')


def code_tokens(name: str) -> List[str]:
    """把指标英文名切成词（按 `.` / `_`，跳过占位符与纯数字）。"""
    tokens = []
    for token in CODE_SEP_RE.split(name or ''):
        if not token or re.search(r'[{#\[\]]', token):
            continue
        if not re.search(r'[A-Za-z]', token):
            continue
        tokens.append(token)
    return tokens


def desc_tokens(name: str) -> List[str]:
    """把英文描述切成词；描述是中文时返回空列表。"""
    if CJK_RE.search(name or ''):
        return []
    return [token.lower() for token in re.findall(r'[A-Za-z][A-Za-z0-9]*', name or '')]


def _is_subsequence(short: str, long: str) -> bool:
    """``short`` 是不是 ``long`` 的子序列。"""
    iterator = iter(long)
    return all(char in iterator for char in short)


def describe_char_diff(bad: str, good: str) -> str:
    """逐字符说明 ``bad`` → ``good`` 的差异（人话，给客户看）。

    **不算出来的说法不许写进文档** —— 手写「少了一个 m 和一个 t」这种话
    会和实际不符（``Commited`` → ``Committed`` 其实只少一个 ``t``）。
    """
    matcher = difflib.SequenceMatcher(None, bad, good)
    parts = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            continue
        if tag == 'insert':
            parts.append('正确写法的第 %d 位缺 `%s`' % (j1 + 1, good[j1:j2]))
        elif tag == 'delete':
            parts.append('第 %d 位多了 `%s`' % (i1 + 1, bad[i1:i2]))
        else:
            parts.append('第 %d 位 `%s` 应为 `%s`' % (i1 + 1, bad[i1:i2], good[j1:j2]))
    return '；'.join(parts)


def corpus_vocabulary(rules: Sequence[Dict[str, Any]]) -> set:
    """语料词表 —— 全部指标英文名 + 中文名里的词。"""
    words = set()
    for rule in rules:
        for token in code_tokens(rule.get('metric_code')):
            words.add(token.lower())
        for token in desc_tokens(rule.get('metric_name')):
            words.add(token)
    return words


def detect_substitution_findings(rules: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """**替换型**错字候选：与语料里的某个词**等长**且**只差 1 个字符**。

    这是 ``detect_ocr_findings``（删字型）的**盲区补齐** —— 删字型判据
    抓不到 ``sequencial`` → ``sequential`` 这种「换一个字母」的错误。

    只扫**中文名列**（英文名列已逐条人工核对，见文档第 8 节的说明）。
    """
    vocabulary = corpus_vocabulary(rules)
    findings = []
    for rule in rules:
        for token in desc_tokens(rule.get('metric_name')):
            candidates = []
            for word in vocabulary:
                if word == token or len(word) != len(token):
                    continue
                diff = [index for index, pair in enumerate(zip(token, word))
                        if pair[0] != pair[1]]
                if len(diff) == 1:
                    candidates.append(word)
            if not candidates:
                continue
            candidates.sort()
            findings.append({
                'metric_code': rule.get('metric_code'),
                'metric_name': rule.get('metric_name'),
                'token': token,
                'candidates': candidates,
            })
    return findings


#: 替换型候选的人工判定：``(metric_code, token, candidate)`` → (判定, 理由)
OCR_SUBSTITUTION_VERDICTS = {
    ('GAUSSDB.{#NAME}.number.Of.sequential.scan', 'sequencial', 'sequential'): (
        '错字',
        '中文名列里的 sequencial 应为 sequential —— 与同一行英文名列一致'),
    ('GAUSSDB.{#NAME}.number.Of.quential.ples.ad', 'sequencial', 'sequential'): (
        '错字',
        '中文名列里的 sequencial 应为 sequential —— 与同族英文名列一致'),
    ('GAUSSDB.{#NAME}.Number.ad.ples', 'dead', 'read'): (
        '正常',
        '中文名列「number of dead tuples」里的 dead 正确（死亡行）'),
    ('GAUSSDB.{#NAME}.Number.t.date.ples', 'hot', 'hit'): (
        '正常',
        '中文名列「number of hot update tuples」里的 hot 正确（HOT 更新）'),
    ('GAUSSDB.{#NAME}.number.Of.dex.ples.ad', 'read', 'dead'): (
        '正常',
        '中文名列「number of index tuples read」里的 read 正确'),
    ('GAUSSDB.{#NAME}.number.Of.quential.ples.ad', 'read', 'dead'): (
        '正常',
        '中文名列「… tuples read」里的 read 正确'),
    ('gaussdb.locks.accessexclusive[{#DBNAME}]', 'num', 'sum'): (
        '正常',
        '中文名列「Num of accessexclusive locks」是客户原文，不是错字'),
}


#: 英语屈折变化 —— 候选词是原词加这些后缀时不算错字
INFLECTIONS = ('s', 'es', 'ed', 'd', 'ing', 'n', 'en')


def detect_ocr_findings(rules: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """自动检测疑似 OCR 错字。

    判据（与文档里写的一致）：

    1. 把指标英文名按 ``.`` / ``_`` 切成词；
    2. 候选词表 = **全表所有指标名里的词** ∪ **本指标英文描述里的词**；
    3. 若某词是候选词删掉 1~2 个字符得到的子序列，且差异**不是**英语屈折变化
       （-s / -es / -ed / -d / -ing / -n / -en），则疑似 OCR 缺字；
    4. 候选词表里的词若本身又是别的词的截断，则不作候选（只保留「最完整」的那个）。
    """
    counter = {}
    for rule in rules:
        for token in code_tokens(rule.get('metric_code')):
            key = token.lower()
            counter[key] = counter.get(key, 0) + 1
    vocabulary = set(counter)

    def truncated(token: str, pool: Iterable[str]) -> bool:
        for word in pool:
            if word == token or len(word) - len(token) not in (1, 2):
                continue
            if not _is_subsequence(token, word):
                continue
            if word.startswith(token) and word[len(token):] in INFLECTIONS:
                continue
            return True
        return False

    maximal = set(word for word in vocabulary if not truncated(word, vocabulary))

    findings = []
    for rule in rules:
        extra = desc_tokens(rule.get('metric_name'))
        pool = maximal | set(extra)
        for token in code_tokens(rule.get('metric_code')):
            key = token.lower()
            candidates = []
            for word in pool:
                if word == key or len(word) - len(key) not in (1, 2):
                    continue
                if not _is_subsequence(key, word):
                    continue
                if word.startswith(key) and word[len(key):] in INFLECTIONS:
                    continue
                candidates.append(word)
            if not candidates:
                continue
            candidates.sort(key=lambda word: (
                len(word) - len(key), 0 if word in maximal else 1, word))
            findings.append({
                'metric_code': rule.get('metric_code'),
                'token': token,
                'candidates': candidates,
            })
    return findings


def apply_replacements(code: str, replacements: Sequence[Tuple[str, str]]) -> str:
    """把指标英文名里的疑似片段替换成建议写法，返回建议的完整名字。"""
    result = code
    for bad, good in replacements:
        pattern = r'(?<![A-Za-z0-9])' + r'[._]'.join(
            re.escape(part) for part in CODE_SEP_RE.split(bad)) + r'(?![A-Za-z0-9])'
        result = re.sub(pattern, good, result)
    return result


def corpus_word_counts(rules: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    """每个词出现在多少个指标的英文名里（用于「全表另有 N 处」的佐证）。"""
    counts = {}
    for rule in rules:
        words = set(token.lower() for token in code_tokens(rule.get('metric_code')))
        for word in words:
            counts[word] = counts.get(word, 0) + 1
    return counts


def _ocr_reason(bad: str, good: str, why: str, counts: Dict[str, int],
                own_words: Sequence[str]) -> str:
    """复核理由 + 自动佐证（全表别处还有几处写对了）。"""
    total = counts.get(good.lower(), 0)
    if not total or good.lower() in own_words:
        return why
    return '%s；全表另有 %d 处 `%s` 的正确写法' % (why, total, good)


def _ocr_intro(lines: List[str], rules: Sequence[Dict[str, Any]], customer_rows: Sequence[Dict[str, Any]]) -> None:
    """开头：这份清单是什么、现状怎么处理、判据是什么。"""
    lines.append('# 客户指标英文名核对清单（客户交付版）')
    lines.append('')
    lines.append('客户指标库的英文名来自**图片 OCR**，存在错字。本文稿列出疑似有问题的写法，'
                 '**请客户逐条核对**并给出最终拼写；确认后我们只改映射表（表驱动），采集逻辑不动。')
    lines.append('')
    lines.append('> - 数据来源：客户指标库 `高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx`'
                 '「指标库」sheet + `tpops_kafka_collector/mapping/data/metric_rules.json`')
    lines.append('> - 生成方式：`python tools/build_delivery_docs.py`（重跑输出不变）')
    lines.append('')
    lines.append('## 1. 现状与处理方式')
    lines.append('')
    lines.append('- 采集端**按客户指标表的原文投递**（`metricCode` 就是表里的英文名，一个字都没改）。'
                 '因此本文档「我们实际用的名字」一列与「客户表里的英文名」一列**目前完全相同**。')
    lines.append('- 客户确认后，我们把 `metric_rules.json` 里对应规则的 `metric_code`（以及有问题的'
                 '`metric_name`）改成建议写法，**代码不需要改**；届时「我们实际用的名字」才会与客户表产生差异。')
    lines.append('- **本次核对没有发现取值口径错误**：%d 个可提供指标的取数方式、维度、刷新周期'
                 '都按映射表复核过，问题集中在**名称拼写**上。'
                 '（单位口径另见第 8 节 —— 客户表里有几处「指标单位」与语义对不上，**待客户确认**。）')
    lines.append('')
    lines.append('## 2. 判据')
    lines.append('')
    lines.append('本文的清单由**两部分**组成，两部分**分开标注**（表里的「判据来源」列）：')
    lines.append('')
    lines.append('| 部分 | 判据 | 覆盖范围 | 章节 |')
    lines.append('|---|---|---|---|')
    lines.append('| **自动判据命中** | 删字型 + 替换型（见下） | **英文名列**（客户指标库的「英文名」列） | 第 3 节 |')
    lines.append('| **人工复核确认** | 逐字符人工比对 | **中文名列**（客户指标库的「中文名」列） | 第 4 节 |')
    lines.append('')
    lines.append('**自动判据一：删字型**（脚本 `tools/build_delivery_docs.py` 的 `detect_ocr_findings`）：')
    lines.append('')
    lines.append('1. 把指标英文名按 `.` / `_` 切成词；')
    lines.append('2. 候选词表 = 全表 %d 个指标名里的词 ∪ 本指标中文名里的词；' % len(rules))
    lines.append('3. 某词若是候选词**删掉 1~2 个字符**得到的子序列，且差异**不是**英语屈折变化'
                 '（-s / -es / -ed / -d / -ing / -n / -en），则判为疑似 OCR 缺字；')
    lines.append('4. 候选词本身若又是别的词的截断，则不作候选（只保留最完整的写法）。')
    lines.append('')
    lines.append('**自动判据二：替换型**（`detect_substitution_findings`）—— 某个词与语料里的另一个词'
                 '**等长**且**只差 1 个字符**（`sequencial` → `sequential` 这类「换一个字母」的错误，'
                 '删字型判据抓不到）。逐条人工复核结果见附录 B。')
    lines.append('')
    lines.append('**人工复核**：上表两种自动判据都只是**候选**。'
                 '本清单的每一条都**逐字符人工比对过**，'
                 '「疑似问题」一列里的「第几位缺什么 / 第几位应该是什么」'
                 '**由脚本逐字符算出来**，不是手写的。')
    lines.append('')
    lines.append('> **覆盖面说明（别过读）**：'
                 '自动判据只覆盖**英文名列**，且只覆盖「删字」与「换字母」两种形态；'
                 '**中文名列**全部由人工逐字符核对（见第 4 节），'
                 '英文名列也已逐条人工核对 —— 但自动判据**不能**证明「其余的都没问题」，'
                 '只能说明「按上述两种形态没发现」。')
    lines.append('')
    lines.append('「建议的正确拼写」一列是**建议**，最终以客户确认的为准。')
    lines.append('')



def _ocr_main_table(lines: List[str], rules: Sequence[Dict[str, Any]], counts: Dict[str, int]) -> None:
    """第 3 节：英文名列的疑似错字。"""
    lines.append('## 3. 疑似错字清单（英文名列 · 自动判据命中 + 人工复核确认）')
    lines.append('')
    lines.append('判据来源：**自动判据（删字型 / 替换型）命中，人工逐字符复核确认**。')
    lines.append('')
    lines.append(TABLE_MARKS['ocr'])
    lines.append('')
    lines.append('| # | 客户表里的英文名 | 我们实际用的名字 | 疑似问题（逐字符差异） | '
                 '建议的正确拼写 | 判据来源 | 影响面 |')
    lines.append('|---|---|---|---|---|---|---|')
    index = 0
    for code, items in OCR_REVIEW.items():
        suggestion = apply_replacements(code, [(bad, good) for bad, good, _ in items])
        own_words = [token.lower() for token in code_tokens(code)]
        problems = '；'.join(
            '`%s` → `%s`（%s%s）' % (
                bad, good, describe_char_diff(bad, good),
                ('；' + _ocr_reason(bad, good, why, counts, own_words)) if why else '')
            for bad, good, why in items)
        impact = ('仅 `metricCode` 的拼写（取值口径、维度、刷新周期均不变）；'
                  '客户确认后改映射表即可')
        index += 1
        lines.append('| %d | `%s` | `%s` | %s | `%s` | 自动判据 + 人工复核 | %s |' % (
            index, code, code, problems, suggestion, impact))
    lines.append('')
    lines.append('> 「我们实际用的名字」与「客户表里的英文名」相同，是因为采集端**按客户表原文投递**。'
                 '客户确认建议写法后，这一列会出现差异（新的 `metricCode`）。')
    lines.append('')


def _ocr_desc_table(lines: List[str], rules: Sequence[Dict[str, Any]]) -> None:
    """第 4 节：中文名列的疑似错字。"""
    lines.append('## 4. 疑似错字清单（中文名列 · 人工复核确认）')
    lines.append('')
    lines.append('中文名列同样会投递到 Kafka 的 `metricName` 字段，需要一并核对。')
    lines.append('')
    lines.append('**这一节的条目是人工逐字符核对出来的** —— 自动判据只扫英文名列，'
                 '覆盖不到中文名列（中文名里的英文词元不参与自动检测）。')
    lines.append('')
    lines.append(TABLE_MARKS['ocr_desc'])
    lines.append('')
    lines.append('| # | 客户表里的中文名 | 我们实际用的名字 | 疑似问题（逐字符差异） | '
                 '建议的正确写法 | 判据来源 | 影响面 |')
    lines.append('|---|---|---|---|---|---|---|')
    index = 0
    for code, items in OCR_DESC_REVIEW.items():
        name = ''
        for rule in rules:
            if rule.get('metric_code') == code:
                name = rule.get('metric_name') or ''
                break
        problems = '；'.join(
            '`%s` → `%s`（%s%s）' % (
                bad, good, describe_char_diff(bad, good),
                ('；' + why) if why else '')
            for bad, good, why in items)
        suggestion = name
        for bad, good, _ in items:
            suggestion = suggestion.replace(bad, good)
        index += 1
        lines.append('| %d | %s | %s | %s | %s | 人工复核 | 仅 `metricName` 的拼写，'
                     '`metricCode` 与取值口径均不变 |' % (
                         index, name, name, problems, suggestion))
    lines.append('')



def _ocr_style(lines: List[str]) -> None:
    """第 5 节：命名风格不一致。"""
    lines.append('## 5. 命名风格不一致（不影响取值，供一并确认）')
    lines.append('')
    lines.append('以下不是 OCR 错字，而是**同一族指标内部的写法不统一**，'
                 '会让 ITSCADA 台账按名字匹配时容易串。建议客户确认统一写法：')
    lines.append('')
    lines.append('| 现象 | 例子 | 建议 |')
    lines.append('|---|---|---|')
    lines.append('| 分隔符混用 | `gaussdb.{#NAME}.Number.lete.ples`（点号）'
                 '与 `GAUSSDB.Number_of_active_connections`（下划线） | 同一族统一用一种分隔符 |')
    lines.append('| 大小写不一致 | `gaussdb.{#NAME}.Number.lete.ples`（`Number`）'
                 '与 `gaussdb.{#NAME}.number.Of.index.scan`（`number`、`Of`） | '
                 '统一成一种大小写风格 |')
    lines.append('| 同名指标两套写法 | 表级指标同时存在 `gaussdb.{#NAME}.…`（中文名描述）'
                 '与 `GAUSSDB.{#NAME}.…`（英文名描述）两套 | '
                 '确认是两条独立指标，还是同一条的重复登记 |')
    lines.append('')


def _ocr_auto_table(lines: List[str], findings: List[Dict[str, Any]],
                      customer_rows: Sequence[Dict[str, Any]]) -> None:
    """第 6 节：自动规则命中但人工判定正常的项。"""
    lines.append('## 6. 附录 A：自动规则命中但经人工复核判定正常（英文名列）')
    lines.append('')
    lines.append('这些写法也满足「删 1~2 个字符得到另一个词」的形态，但它们是英语的正常写法，'
                 '**不是错字**，列出以说明判据经过复核而非盲用。')
    lines.append('')
    lines.append(TABLE_MARKS['ocr_auto'])
    lines.append('')
    lines.append('| 客户表里的英文名 | 自动规则给出的候选 | 人工判定 | 理由 |')
    lines.append('|---|---|---|---|')
    for finding in findings:
        key = (finding['metric_code'], finding['token'], finding['candidates'][0])
        reason = OCR_ACCEPTED.get(key)
        if not reason:
            continue
        lines.append('| `%s` | `%s` → `%s` | 正常 | %s |' % (
            finding['metric_code'], finding['token'], finding['candidates'][0], reason))
    lines.append('')


def _ocr_substitution_table(lines: List[str], rules: Sequence[Dict[str, Any]]) -> None:
    """第 7 节：替换型候选的逐条复核（附录 B）。"""
    lines.append('## 7. 附录 B：替换型候选逐条复核（中文名列）')
    lines.append('')
    lines.append('下表是**替换型判据**（等长、只差 1 个字符）在中文名列里命中的**全部候选**，'
                 '每一条都**人工复核过**并给出判定 —— '
                 '列出全部候选是为了说明「替换型这一类错字没有漏检」。')
    lines.append('')
    lines.append(TABLE_MARKS['ocr_sub'])
    lines.append('')
    lines.append('| 客户指标 | 中文名列里的词 | 自动判据给出的候选 | 人工判定 | 理由 |')
    lines.append('|---|---|---|---|---|')
    for finding in detect_substitution_findings(rules):
        for candidate in finding['candidates']:
            key = (finding['metric_code'], finding['token'], candidate)
            verdict = OCR_SUBSTITUTION_VERDICTS.get(key)
            if verdict is None:
                lines.append('| `%s` | `%s` | `%s` | **未复核** | '
                             '**新候选 —— 必须人工复核后补进判据表** |' % (
                                 finding['metric_code'], finding['token'], candidate))
                continue
            label, reason = verdict
            lines.append('| `%s` | `%s` | `%s` | %s | %s |' % (
                finding['metric_code'], finding['token'], candidate, label, reason))
    lines.append('')


def _ocr_conclusion(lines: List[str], customer_rows: Sequence[Dict[str, Any]],
                    rules: Sequence[Dict[str, Any]]) -> None:
    """第 8 节：结论。"""
    lines.append('## 8. 结论')
    lines.append('')
    total = len(customer_rows)
    fragments = sum(len(items) for items in OCR_REVIEW.values())
    fragments += sum(len(items) for items in OCR_DESC_REVIEW.values())
    codes = set(OCR_REVIEW.keys())
    codes.update(OCR_DESC_REVIEW.keys())
    lines.append('- 客户指标库共 %d 个指标；**%d 个指标**的英文名或中文名疑似存在 OCR 错字'
                 '（合计 %d 处片段），其中英文名列 %d 处、中文名列 %d 处。' % (
                     total, len(codes), fragments,
                     sum(len(items) for items in OCR_REVIEW.values()),
                     sum(len(items) for items in OCR_DESC_REVIEW.values())))
    lines.append('- 其余 **%d 个**指标**按上述判据**未发现疑似错字 —— '
                 '注意这是「按判据未发现」，**不等于**「已确认无误」：'
                 '删字型与替换型两种形态之外的问题（如拼错整个词、缩写不一致）判据抓不到。' % (
                     total - len(codes)))
    lines.append('- 疑似错字**全部是名称层面的**（`metricCode` / `metricName` 的拼写），'
                 '**不涉及取数口径、不涉及数据正确性**。')
    lines.append('- **单位口径另需客户确认**：客户表「指标单位」列有若干处与语义对不上'
                 '（例如「通过 HOT 更新的行数」标 `%`，而同族的「行数」类指标标 `tuples/s`；'
                 '「禁用的连接数」标 `个`、「等待资源的连接数」标 `%`），'
                 '这些**不是 OCR 错字**，本次核对未改动，**请客户一并确认**。')
    lines.append('- 请客户在验收前反馈确认后的最终拼写；我们收到后更新映射表并重新出这份清单。')
    lines.append('')


def render_ocr_checklist(rules, customer_rows):
    """生成《交付-客户指标英文名核对清单.md》。

    检测器跑在**客户指标库全量**（生效口径 + 被排除的）上 —— 核对清单的对象
    是客户的指标表，不是我们的生效口径。
    """
    scope = customer_scope_rules(rules, customer_rows)
    findings = detect_ocr_findings(scope)
    counts = corpus_word_counts(scope)
    lines = []
    _ocr_intro(lines, scope, customer_rows)
    _ocr_main_table(lines, scope, counts)
    _ocr_desc_table(lines, scope)
    _ocr_style(lines)
    _ocr_auto_table(lines, findings, customer_rows)
    _ocr_substitution_table(lines, scope)
    _ocr_conclusion(lines, customer_rows, scope)
    return '\n'.join(lines) + '\n'


# --------------------------------------------------------------------------
# markdown 表格解析（守卫测试用）
# --------------------------------------------------------------------------
def parse_markdown_table(text: str, mark: str) -> List[Dict[str, str]]:
    """按定位标记取出 markdown 表格，返回 [{列名: 单元格}, ...]。"""
    lines = text.splitlines()
    try:
        start = lines.index(mark)
    except ValueError:
        raise AssertionError('文档里找不到表格标记 %s' % mark)
    header = None
    rows = []
    for line in lines[start + 1:]:
        stripped = line.strip()
        if not stripped:
            if rows:
                break
            continue
        if not stripped.startswith('|'):
            break
        cells = [cell.strip() for cell in stripped.strip('|').split('|')]
        if header is None:
            header = cells
            continue
        if set(''.join(cells)) <= set('-: '):
            continue
        row = {}
        for index, name in enumerate(header):
            row[name] = cells[index] if index < len(cells) else ''
        rows.append(row)
    if header is None:
        raise AssertionError('表格标记 %s 后面没有表头' % mark)
    return rows


def parse_metric_spec_rows(text: str) -> List[Dict[str, str]]:
    """解析《交付-指标口径说明.md》里的全部指标行。"""
    rows = []
    for section in text.split('### 5.'):
        if TABLE_MARKS['metrics'] not in section:
            continue
        rows.extend(parse_markdown_table(section, TABLE_MARKS['metrics']))
    return rows


def parse_unsupported_rows(text: str) -> List[Dict[str, str]]:
    """解析《交付-不可提供指标清单.md》的清单表。"""
    return parse_markdown_table(text, TABLE_MARKS['unsupported'])


def parse_ocr_rows(text: str) -> List[Dict[str, str]]:
    """解析《交付-客户指标英文名核对清单.md》的主表。"""
    return parse_markdown_table(text, TABLE_MARKS['ocr'])


# --------------------------------------------------------------------------
# 入口
# --------------------------------------------------------------------------
def build_all() -> Dict[str, str]:
    """生成三份文档，返回 ``{路径: 内容}``。"""
    rules = load_metric_rules()['rules']
    views = load_merged_sql()
    customer_rows = load_customer_table()
    return {
        SPEC_DOC: render_metric_spec(
            rules, views, load_excluded_metrics().get('counts') or {}),
        UNSUPPORTED_DOC: render_unsupported(rules, customer_rows),
        OCR_DOC: render_ocr_checklist(rules, customer_rows),
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    """写文件。"""
    argv = list(argv or sys.argv[1:])
    check = '--check' in argv
    outputs = build_all()
    failed = False
    for path in sorted(outputs):
        content = outputs[path]
        relative = os.path.relpath(path, REPO_ROOT)
        if check:
            current = ''
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as handle:
                    current = handle.read()
            if current != content:
                print('不一致：%s' % relative)
                failed = True
            else:
                print('一致：%s' % relative)
            continue
        with open(path, 'w', encoding='utf-8') as handle:
            handle.write(content)
        print('已生成：%s（%d 行）' % (relative, content.count('\n')))
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
