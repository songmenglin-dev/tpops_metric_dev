"""跨轮/跨视图规则：``counter_delta`` / ``ratio_across``。

- ``counter_delta``：累计计数 → 速率（差分基线在 ``deltas.py``）；
- ``ratio_across``：分子分母来自**不同视图**（``activity_agg`` / ``stat_database``）。
"""
import logging
from typing import Any, List, Mapping

from .rules_context import (
    MetricValue,
    RuleContext,
    RuleError,
    grouped_items,
    sum_values,
    to_number,
)

LOGGER = logging.getLogger(__name__)


def eval_counter_delta(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
                       deltas: Any) -> List[MetricValue]:
    """累计计数 → 速率。

    - 首轮无基线 → 只记基线，值为 None（不产出速率）；
    - 累计值回退（实例重启）→ 丢弃本轮差分、记 ERROR（见 ``deltas.py``）；
    - 时间差用**实际采集时刻之差**。
    """
    if deltas is None:
        raise RuleError('counter_delta 需要差分基线存储')
    params = rule.get('params') or {}
    column = params.get('column')
    policy = params.get('row_policy', 'per_row')
    if policy == 'per_row':
        results = []  # type: List[MetricValue]
        for dims, row in grouped_items(metric_rule, params, ctx.rows):
            value, _reason = deltas.rate(
                metric_rule.metric_code, dims, to_number(row.get(column)), ctx.now_ms
            )
            results.append((dims, value, ctx.now_ms))
        return results
    total = sum_values([to_number(row.get(column)) for row in ctx.rows])
    value, _reason = deltas.rate(metric_rule.metric_code, {}, total, ctx.now_ms)
    return [({}, value, ctx.now_ms)]


def eval_ratio_across(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
                      deltas: Any) -> List[MetricValue]:
    """跨视图比率。

    **分母取不到（为 0 或视图缺失）→ 不产出**（M-3）：比率在分母为 0 时没有定义，
    编一个 ``0.0`` 出去等于把「取不到」说成「真的是 0」—— 与本项目
    「不静默出 0」的口径相反（规格「缺值不产出假数据」）。

    分母可用、分子为 0 → 产出 ``0.0``：那是真的「没有活跃会话」。
    比率保留 2 位小数（投递规格：比率类按客户指标表的数据类型序列化）。
    """
    params = rule.get('params') or {}
    denominator = side_value(params.get('denominator') or {}, ctx)
    if not denominator:
        LOGGER.warning(
            'ratio_across 的分母不可用（%s），本轮不产出该指标的值',
            (params.get('denominator') or {}).get('sql_key'),
        )
        return []
    numerator = side_value(params.get('numerator') or {}, ctx)
    return [({}, round(numerator / denominator, 2), ctx.now_ms)]


def side_value(spec: Mapping[str, Any], ctx: RuleContext) -> float:
    """比率的一侧：按 ``fn`` 汇总该视图的行。

    ``fn`` 支持 ``sum``（默认）/ ``max`` / ``count``。
    """
    rows = ctx.rows_by_key.get(spec.get('sql_key')) or ()
    column = spec.get('value_column')
    fn = spec.get('fn', 'sum')
    if fn == 'sum':
        return float(sum_values([to_number(row.get(column)) for row in rows]))
    if fn == 'max':
        values = [to_number(row.get(column)) for row in rows]
        return max(values) if values else 0.0
    if fn == 'count':
        return float(len(rows))
    raise RuleError(f'未实现的 ratio fn：{fn}')
