"""规则分派：``value_rule.type`` → 执行函数。

映射表在用的**全部 13 种**规则类型都在 ``HANDLERS`` 里；
表里没有的类型不实现（规格「SHALL NOT 引入映射表未使用的规则类型」），
表里有的类型漏了就是启动期发现不了的缺陷 —— 所以这张表与
``data/mapping/metric_rules.json`` 是同一份契约的两面。
"""
from typing import Any, Callable, List, Mapping, Optional

from .rules_collection import (
    eval_collection_count,
    eval_instance_field,
    eval_text_prefix_count,
    eval_top_n,
)
from .rules_context import MetricValue, RuleContext, RuleError
from .rules_cross import eval_counter_delta, eval_ratio_across
from .rules_row import eval_aggregate, eval_raw, eval_raw_filtered, eval_raw_rows
from .rules_series import eval_enum_map, eval_gauge, eval_series_point

Handler = Callable[[Any, Mapping[str, Any], RuleContext, Any], List[MetricValue]]

HANDLERS = {
    'raw': eval_raw,
    'raw_filtered': eval_raw_filtered,
    'aggregate': eval_aggregate,
    'counter_delta': eval_counter_delta,
    'ratio_across': eval_ratio_across,
    'series_point': eval_series_point,
    'enum_map': eval_enum_map,
    'gauge': eval_gauge,
    'top_n': eval_top_n,
    'raw_rows': eval_raw_rows,
    'collection_count': eval_collection_count,
    'text_prefix_count': eval_text_prefix_count,
    'instance_field': eval_instance_field,
}

RULE_TYPES = tuple(sorted(HANDLERS))


def evaluate(
    metric_rule: Any,
    ctx: RuleContext,
    deltas: Optional[Any] = None,
) -> List[MetricValue]:
    """执行一个指标的取值规则，返回若干 ``(维度, 值, 时间戳)``。"""
    rule = metric_rule.value_rule or {}
    rule_type = rule.get('type')
    handler = HANDLERS.get(rule_type)
    if handler is None:
        raise RuleError(f'未知的取值规则类型：{rule_type}')
    return handler(metric_rule, rule, ctx, deltas)
