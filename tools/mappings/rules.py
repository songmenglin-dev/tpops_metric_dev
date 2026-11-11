# -*- coding: utf-8 -*-
"""字段级转换规则：取值、占位符替换、资源名拼装。

规则本身在 ``data/mapping/`` 的 JSON 里，这里只负责**执行**规则。
字段级映射（原始字段 → ITSCADA 字段）见 ``data/mapping/itscada_field_map.json``。
"""
from __future__ import print_function

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

PLACEHOLDER_RE = re.compile(r'\{#(\w+)\}')
RESOURCE_FIELD_RE = re.compile(r'\{(\w+)\}')

# 取值结果：(维度键值, 值, 数据时间戳)。
# 维度为空表示该指标没有维度；时间戳是客户字段 clock 的来源
# （A 类取与 value 同一下标的 timestamps[i]，其余取采集完成时刻）。
MetricValue = Tuple[Dict[str, Any], Any, Optional[int]]


class RuleError(Exception):
    """规则无法执行。"""


@dataclass(frozen=True)
class RuleContext:
    """一次取数的输入。"""

    rows: Tuple[Mapping[str, Any], ...] = ()
    rows_by_key: Mapping[str, Sequence[Mapping[str, Any]]] = field(default_factory=dict)
    series: Mapping[str, Sequence[Optional[float]]] = field(default_factory=dict)
    timestamps: Tuple[int, ...] = ()
    collections: Mapping[str, Sequence[Mapping[str, Any]]] = field(default_factory=dict)
    instance: Mapping[str, Any] = field(default_factory=dict)
    now_ms: int = 0


class CounterDeltaStore:
    """累计计数器的差分基线。

    铁律（开发方案 §3.4）：首轮无基线不产出值；累计值回退判定为实例重启，
    丢弃本轮差分并重建基线；时间差用采集时刻差而不是标称周期。
    """

    def __init__(self) -> None:
        self._baselines = {}  # type: Dict[Tuple[str, str], Tuple[float, int]]

    def rate(
        self,
        metric_code: str,
        dimensions: Mapping[str, Any],
        counter_value: float,
        now_ms: int,
    ) -> Tuple[Optional[float], str]:
        """返回 (速率, 说明)。速率为 None 时说明原因。"""
        key = (metric_code, _dimension_key(dimensions))
        previous = self._baselines.get(key)
        if previous is None:
            self._baselines[key] = (counter_value, now_ms)
            return None, '首轮无基线，仅建立基线'
        last_value, last_ms = previous
        if counter_value < last_value:
            self._baselines[key] = (counter_value, now_ms)
            return None, '累计值回退，判定实例重启，丢弃本轮差分并重建基线'
        elapsed = (now_ms - last_ms) / 1000.0
        if elapsed <= 0:
            return None, '采集时刻未推进，丢弃本轮差分'
        self._baselines[key] = (counter_value, now_ms)
        return (counter_value - last_value) / elapsed, '按采集时刻差分'

    def forget(self, metric_code: str, dimensions: Mapping[str, Any]) -> None:
        self._baselines.pop((metric_code, _dimension_key(dimensions)), None)


def substitute_placeholders(template: str, dimensions: Mapping[str, Any]) -> str:
    """把 ``{#DATNAME}`` 替换成维度实际值（ITSCADA 文档 §7 的做法）。"""
    def _replace(match: 're.Match') -> str:
        name = match.group(1)
        if name not in dimensions:
            raise RuleError('占位符 {%s} 没有对应的维度值' % name)
        value = dimensions[name]
        return '' if value is None else str(value)

    return PLACEHOLDER_RE.sub(_replace, template or '')


def build_resource_name(
    template: str,
    context: Mapping[str, Any],
    fallback_template: Optional[str] = None,
) -> str:
    """按模板拼资源名。

    模板里的占位符取不到值时（None 或空串），改用回退模板；
    没有回退模板时留空串占位。
    """
    missing = [False]

    def _replace(match: 're.Match') -> str:
        value = context.get(match.group(1))
        if value is None or value == '':
            missing[0] = True
            return ''
        return str(value)

    rendered = RESOURCE_FIELD_RE.sub(_replace, template or '')
    if missing[0] and fallback_template:
        return build_resource_name(fallback_template, context)
    return rendered


def evaluate(
    metric_rule,
    ctx: RuleContext,
    deltas: Optional[CounterDeltaStore] = None,
) -> List[MetricValue]:
    """执行一个指标的取值规则，返回若干 (维度, 值, 时间戳)。"""
    rule = metric_rule.value_rule or {}
    rule_type = rule.get('type')
    handler = _HANDLERS.get(rule_type)
    if handler is None:
        raise RuleError('未知的取值规则类型：%s' % rule_type)
    return handler(metric_rule, rule, ctx, deltas)


# -- 各规则类型的实现 ---------------------------------------------------
def _eval_raw(metric_rule, rule, ctx, deltas):
    params = rule.get('params', {})
    column = params.get('column')
    policy = params.get('row_policy', 'first')
    rows = ctx.rows
    if policy == 'per_row':
        return [
            (_row_dimensions(metric_rule, row), row.get(column), ctx.now_ms)
            for row in rows
        ]
    if policy == 'sum':
        values = [_to_number(row.get(column)) for row in rows]
        return [({}, _sum(values), ctx.now_ms)]
    if not rows:
        return []
    return [({}, rows[0].get(column), ctx.now_ms)]


def _eval_raw_filtered(metric_rule, rule, ctx, deltas):
    params = rule.get('params', {})
    column = params.get('column')
    rows = [row for row in ctx.rows if _match_filter(row, params.get('filter', {}))]
    if not rows:
        return []
    value = rows[0].get(column)
    cast = params.get('cast')
    if cast == 'int':
        value = _to_int(value)
    return [({}, value, ctx.now_ms)]


def _eval_aggregate(metric_rule, rule, ctx, deltas):
    params = rule.get('params', {})
    rows = [row for row in ctx.rows if _match_filter(row, params.get('filter', {}))]
    group_by = params.get('group_by') or []
    value_column = params.get('value_column', 'cnt')
    if not group_by:
        total = _sum([_to_number(row.get(value_column)) for row in rows])
        return [({}, total, ctx.now_ms)]
    groups = {}  # type: Dict[Tuple[str, ...], List[Mapping[str, Any]]]
    for row in rows:
        key = tuple(str(row.get(column)) for column in group_by)
        groups.setdefault(key, []).append(row)
    results = []
    for _key, group_rows in groups.items():
        dims = {}
        for placeholder, column in metric_rule.dimension_bindings.items():
            dims[placeholder] = group_rows[0].get(column)
        total = _sum([_to_number(row.get(value_column)) for row in group_rows])
        results.append((dims, total, ctx.now_ms))
    return results


def _eval_counter_delta(metric_rule, rule, ctx, deltas):
    if deltas is None:
        raise RuleError('counter_delta 需要差分基线存储')
    params = rule.get('params', {})
    column = params.get('column')
    policy = params.get('row_policy', 'per_row')
    results = []
    if policy == 'per_row':
        for row in ctx.rows:
            dims = _row_dimensions(metric_rule, row)
            value, _reason = deltas.rate(
                metric_rule.metric_code, dims, _to_number(row.get(column)), ctx.now_ms
            )
            results.append((dims, value, ctx.now_ms))
        return results
    total = _sum([_to_number(row.get(column)) for row in ctx.rows])
    value, _reason = deltas.rate(metric_rule.metric_code, {}, total, ctx.now_ms)
    return [({}, value, ctx.now_ms)]


def _eval_ratio_across(metric_rule, rule, ctx, deltas):
    params = rule.get('params', {})
    numerator = _side_value(params.get('numerator', {}), ctx)
    denominator = _side_value(params.get('denominator', {}), ctx)
    if not denominator:
        return [({}, 0.0, ctx.now_ms)]
    return [({}, round(numerator / denominator, 2), ctx.now_ms)]


def _eval_series_point(metric_rule, rule, ctx, deltas):
    params = rule.get('params', {})
    position = params.get('position', 'last_non_null')
    results = []
    for key, values in ctx.series.items():
        value, index = _pick_point(values, position)
        if value is None:
            continue
        results.append(({'COMPONENT_ID': key}, value, _point_timestamp(ctx, index)))
    return results


def _eval_enum_map(metric_rule, rule, ctx, deltas):
    params = rule.get('params', {})
    mapping = params.get('mapping', {})
    default = params.get('default', 'passthrough')
    results = []
    for dims, value, timestamp in _eval_series_point(metric_rule, rule, ctx, deltas):
        key = _enum_key(value)
        if key in mapping:
            results.append((dims, mapping[key], timestamp))
        elif default == 'passthrough':
            results.append((dims, value, timestamp))
        else:
            results.append((dims, None, timestamp))
    return results


def _eval_gauge(metric_rule, rule, ctx, deltas):
    params = rule.get('params', {})
    unit = params.get('unit', '秒')
    if unit not in ('秒', 's'):
        raise RuleError('gauge 规则的时间单位不支持：%s' % unit)
    results = []
    now_seconds = ctx.now_ms / 1000.0
    for key, values in ctx.series.items():
        point, index = _pick_point(values, 'last_non_null')
        if point is None:
            continue
        elapsed = now_seconds - float(point)
        results.append(({'COMPONENT_ID': key}, round(elapsed, 2), _point_timestamp(ctx, index)))
    return results


def _eval_top_n(metric_rule, rule, ctx, deltas):
    params = rule.get('params', {})
    rows = list(ctx.rows)
    sort_by = params.get('sort_by')
    if sort_by is None:
        rows = list(ctx.collections.get(params.get('collection', ''), ()))
    reverse = params.get('order', 'desc') == 'desc'
    if sort_by:
        rows.sort(key=lambda row: _to_number(row.get(sort_by)), reverse=reverse)
    limit = params.get('limit', 10)
    selected = rows[:limit]
    columns = params.get('columns')
    if columns:
        selected = [{key: row.get(key) for key in columns} for row in selected]
    return [({}, selected, ctx.now_ms)]


def _eval_raw_rows(metric_rule, rule, ctx, deltas):
    return [({}, [dict(row) for row in ctx.rows], ctx.now_ms)]


def _eval_collection_count(metric_rule, rule, ctx, deltas):
    collection = ctx.collections.get(rule.get('params', {}).get('collection', ''), ())
    return [({}, len(collection), ctx.now_ms)]


def _eval_text_prefix_count(metric_rule, rule, ctx, deltas):
    params = rule.get('params', {})
    collection = ctx.collections.get(params.get('collection', ''), ())
    field_name = params.get('field', 'sql_text')
    prefixes = [p.lower() for p in params.get('prefixes', ())]
    count = 0
    for item in collection:
        text = (item.get(field_name) or '').strip().lower()
        if any(text.startswith(prefix) for prefix in prefixes):
            count += 1
    return [({}, count, ctx.now_ms)]


def _eval_instance_field(metric_rule, rule, ctx, deltas):
    field_name = rule.get('params', {}).get('field')
    return [({}, ctx.instance.get(field_name), ctx.now_ms)]


_HANDLERS = {
    'raw': _eval_raw,
    'raw_filtered': _eval_raw_filtered,
    'aggregate': _eval_aggregate,
    'counter_delta': _eval_counter_delta,
    'ratio_across': _eval_ratio_across,
    'series_point': _eval_series_point,
    'enum_map': _eval_enum_map,
    'gauge': _eval_gauge,
    'top_n': _eval_top_n,
    'raw_rows': _eval_raw_rows,
    'collection_count': _eval_collection_count,
    'text_prefix_count': _eval_text_prefix_count,
}


# -- 小工具 --------------------------------------------------------------
def _point_timestamp(ctx, index):
    """数据时间戳：A 类取与 value 同一下标的 timestamps[i]，其余取采集完成时刻。"""
    if index is not None and 0 <= index < len(ctx.timestamps):
        return ctx.timestamps[index]
    return ctx.now_ms


def _row_dimensions(metric_rule, row):
    dims = {}
    for placeholder, column in metric_rule.dimension_bindings.items():
        dims[placeholder] = row.get(column)
    return dims


def _match_filter(row, filters):
    for key, expected in filters.items():
        if key.endswith('_contains'):
            actual = row.get(key[:-len('_contains')])
            text = '' if actual is None else str(actual).lower()
            keywords = expected if isinstance(expected, (list, tuple)) else [expected]
            if not any(str(word).lower() in text for word in keywords):
                return False
        elif row.get(key) != expected:
            return False
    return True


def _side_value(spec, ctx):
    rows = ctx.rows_by_key.get(spec.get('sql_key'), ())
    column = spec.get('value_column')
    fn = spec.get('fn', 'sum')
    values = [_to_number(row.get(column)) for row in rows]
    if fn == 'sum':
        return _sum(values)
    if fn == 'max':
        return max(values) if values else 0.0
    if fn == 'count':
        return float(len(values))
    return _sum(values)


def _pick_point(values, position):
    """取时序上的一个点，返回 (值, 下标)。值为 null 的点跳过。"""
    for index in range(len(values) - 1, -1, -1):
        value = values[index]
        if value is not None:
            return value, index
    return None, None


def _enum_key(value):
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _to_number(value):
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value):
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _sum(values):
    total = 0.0
    for value in values:
        total += value
    if total.is_integer():
        return int(total)
    return total


def _dimension_key(dimensions):
    if not dimensions:
        return ''
    return '|'.join('%s=%s' % (key, dimensions[key]) for key in sorted(dimensions))
