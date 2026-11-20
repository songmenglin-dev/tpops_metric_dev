"""行级规则：``raw`` / ``raw_filtered`` / ``aggregate`` / ``raw_rows``。

取值口径（与 ``tools/expected_deriver.py`` 的独立推导一致）：

- ``raw``：``row_policy`` 三态 —— ``sum``（所有行求和）/ ``per_row``（一行一条，
  维度按 ``group_by`` 分组）/ ``first``（第一行）；
- ``raw_filtered``：按 ``filter`` 过滤后取**首个匹配行**（R-M1）；
- ``aggregate``：``filter`` 过滤 → ``group_by`` 分组 → ``fn: sum``；
- ``raw_rows``：整行列表。
"""
from typing import Any, List, Mapping, Tuple

from .rules_context import (
    MetricValue,
    RuleContext,
    RuleError,
    grouped_items,
    matches_filter,
    row_dimensions,
    sum_values,
    to_int,
    to_number,
)


def eval_raw(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
             deltas: Any) -> List[MetricValue]:
    """直接取列。

    - ``sum``：该列在所有行上求和；
    - ``per_row``：一行一条（维度按 ``group_by`` 分组，见 ``grouped_items``）；
    - ``first``：第一行（没有行就不产出）。
    """
    params = rule.get('params') or {}
    column = params.get('column')
    policy = params.get('row_policy', 'first')
    if policy == 'sum':
        values = [to_number(row.get(column)) for row in ctx.rows]
        return [({}, sum_values(values), ctx.now_ms)]
    if policy == 'per_row':
        return [
            (dims, row.get(column), ctx.now_ms)
            for dims, row in grouped_items(metric_rule, params, ctx.rows)
        ]
    if policy == 'first':
        if not ctx.rows:
            return []
        return [({}, ctx.rows[0].get(column), ctx.now_ms)]
    raise RuleError(f'未知的 row_policy：{policy}')


def eval_raw_filtered(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
                       deltas: Any) -> List[MetricValue]:
    """过滤后取**首个匹配行**的值。

    **多命中的口径（R-M1）**：取**首个**匹配行。留档上每个 filter 恰命中 1 行，
    「首行 vs 末行」不可区分，所以这条口径由合成样本守着
    （``tests/unit/test_rules.py::test_raw_filtered_takes_the_first_matching_row``）。

    一个匹配行都没有 → 不产出（不是 0）。
    """
    params = rule.get('params') or {}
    column = params.get('column')
    matched = [row for row in ctx.rows if matches_filter(row, params.get('filter') or {})]
    if not matched:
        return []
    row = matched[0]
    value = row.get(column)
    if params.get('cast') == 'int':
        value = to_int(value)
    # 维度要带上：指标名里的占位符（如 {#PORT}）要靠它替换，
    # 不填的话组装消息时 substitute_placeholders 会直接抛异常。
    dims = row_dimensions(metric_rule, row)
    return [(dims, value, ctx.now_ms)]


def eval_aggregate(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
                   deltas: Any) -> List[MetricValue]:
    """过滤 → 分组 → 求和。

    ``group_by`` 为空 → 整体求和出一条；不为空 → 每个分组一条，
    分组顺序按**首次出现**的顺序（与 SQL 的 ``GROUP BY`` 结果顺序一致）。
    """
    params = rule.get('params') or {}
    fn = params.get('fn', 'sum')
    if fn != 'sum':
        raise RuleError(f'未实现的 aggregate fn：{fn}')
    rows = [row for row in ctx.rows if matches_filter(row, params.get('filter') or {})]
    group_by = tuple(params.get('group_by') or ())
    value_column = params.get('value_column', 'cnt')
    if not group_by:
        total = sum_values([to_number(row.get(value_column)) for row in rows])
        return [({}, total, ctx.now_ms)]
    groups = _group_rows(rows, group_by)
    results = []  # type: List[MetricValue]
    for group_rows in groups:
        dims = row_dimensions(metric_rule, group_rows[0])
        total = sum_values([to_number(row.get(value_column)) for row in group_rows])
        results.append((dims, total, ctx.now_ms))
    return results


def eval_raw_rows(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
                  deltas: Any) -> List[MetricValue]:
    """整行列表（会话清单、锁视图、库级统计）。"""
    return [({}, [dict(row) for row in ctx.rows], ctx.now_ms)]


def _group_rows(
    rows: List[Mapping[str, Any]],
    group_by: Tuple[str, ...],
) -> List[List[Mapping[str, Any]]]:
    """按 ``group_by`` 分组，保持首次出现的分组顺序。"""
    groups = {}  # type: Dict[Tuple[Any, ...], List[Mapping[str, Any]]]
    for row in rows:
        key = tuple(str(row.get(column)) for column in group_by)
        groups.setdefault(key, []).append(row)
    return list(groups.values())
