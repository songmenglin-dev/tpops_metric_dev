"""时序类规则：``series_point`` / ``enum_map`` / ``gauge``。

A 类（监控指标接口）按**组件**给出多条 series —— 每个组件（CN / DN）一条，
维度键是 ``COMPONENT_ID``。规则在这一层取「每个组件的一个点」。
"""
from typing import Any, List, Mapping, Optional, Sequence, Tuple
from loguru import logger

from .rules_context import MetricValue, RuleContext, RuleError, point_timestamp

LOGGER = logger


def eval_series_point(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
                      deltas: Any) -> List[MetricValue]:
    """取时序上的一个点（每个组件一条）。

    ``position`` 支持 ``last_non_null``（默认）/ ``last`` / ``first_non_null`` /
    ``first``；值为 null 的点跳过 —— **不产出假数据**。
    """
    params = rule.get('params') or {}
    position = params.get('position', 'last_non_null')
    results = []  # type: List[MetricValue]
    for key, values in ctx.series.items():
        value, index = pick_point(values, position)
        if value is None:
            continue
        results.append(({'COMPONENT_ID': key}, value, point_timestamp(ctx, index)))
    return results


def eval_enum_map(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
                  deltas: Any) -> List[MetricValue]:
    """把时序点的值按枚举表翻译。

    枚举未命中时按 ``default`` 处理：``passthrough``（默认）原样透传**并记一条
    WARNING**（规格「枚举未命中」场景：原样透传 + 告警级日志）；其它取值给 None。
    """
    params = rule.get('params') or {}
    mapping = params.get('mapping') or {}
    default = params.get('default', 'passthrough')
    results = []  # type: List[MetricValue]
    for dims, value, timestamp in eval_series_point(metric_rule, rule, ctx, deltas):
        key = enum_key(value)
        if key in mapping:
            results.append((dims, mapping[key], timestamp))
        elif default == 'passthrough':
            LOGGER.warning(
                '枚举未命中，原样透传：指标 {} 的值 {!r} 不在枚举表里',
                metric_rule.metric_code, value,
            )
            results.append((dims, value, timestamp))
        else:
            results.append((dims, None, timestamp))
    return results


def eval_gauge(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
               deltas: Any) -> List[MetricValue]:
    """由「启动时刻」推算已运行时长（平台没有 uptime 指标）。"""
    params = rule.get('params') or {}
    unit = params.get('unit', '秒')
    if unit not in ('秒', 's'):
        raise RuleError(f'gauge 规则的时间单位不支持：{unit}')
    results = []  # type: List[MetricValue]
    now_seconds = ctx.now_ms / 1000.0
    for key, values in ctx.series.items():
        point, index = pick_point(values, 'last_non_null')
        if point is None:
            continue
        elapsed = now_seconds - float(point)
        results.append(({'COMPONENT_ID': key}, round(elapsed, 2), point_timestamp(ctx, index)))
    return results


def pick_point(
    values: Sequence[Optional[float]],
    position: str,
) -> Tuple[Optional[float], Optional[int]]:
    """取时序上的一个点，返回 ``(值, 下标)``。值为 null 的点跳过。"""
    if position in ('last_non_null', 'first_non_null'):
        indexes = range(len(values) - 1, -1, -1) if position == 'last_non_null' \
            else range(0, len(values))
        for index in indexes:
            value = values[index]
            if value is not None:
                return value, index
        return None, None
    if position == 'last':
        return (values[-1], len(values) - 1) if values else (None, None)
    if position == 'first':
        return (values[0], 0) if values else (None, None)
    raise RuleError(f'未知的时序取值位置：{position}')


def enum_key(value: Any) -> str:
    """枚举表的键：数值按整数形态查（``1.0`` 与 ``1`` 是同一个键）。"""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)
