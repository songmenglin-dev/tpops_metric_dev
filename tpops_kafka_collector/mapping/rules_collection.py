"""集合类规则：``top_n`` / ``raw_rows`` 之外的集合口径。

- ``top_n``：按排序列取前 N 行（排序口径见 ``sorting.py``）；
- ``collection_count``：集合条数；
- ``text_prefix_count``：按文本前缀分类计数；
- ``instance_field``：实例元数据里的一个字段。
"""
from typing import Any, List, Mapping

from .rules_context import MetricValue, RuleContext
from .rules_sorting import sort_rows


def eval_top_n(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
               deltas: Any) -> List[MetricValue]:
    """取前 N 行。

    数据来源：声明了 ``collection`` 就从该集合取（诊断优化接口的
    ``top_sql_infos`` / ``slow_sql_infos``），否则从当前视图的行取
    （命令中心的 ``activity_running`` / ``activity_top10``）。

    排序按 PostgreSQL 的 ``ORDER BY`` 口径（见 ``sorting.py``），
    再截断到 ``limit``，最后按 ``columns`` 投影。
    """
    params = rule.get('params') or {}
    collection = params.get('collection')
    if collection:
        rows = list(ctx.collections.get(collection, ()))
    else:
        rows = list(ctx.rows)
    ordered = sort_rows(rows, params.get('sort_by'), params.get('order', 'desc'))
    limit = params.get('limit', 10)
    selected = ordered[:limit]
    columns = params.get('columns')
    if columns:
        selected = [{key: row.get(key) for key in columns} for row in selected]
    return [({}, selected, ctx.now_ms)]


def eval_collection_count(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
                          deltas: Any) -> List[MetricValue]:
    """集合条数（慢 SQL 条数等）。"""
    collection = (rule.get('params') or {}).get('collection', '')
    return [({}, len(ctx.collections.get(collection, ())), ctx.now_ms)]


def eval_text_prefix_count(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
                           deltas: Any) -> List[MetricValue]:
    """按文本前缀给集合分类计数（``sql_text`` 以 ``select`` / ``insert`` 开头…）。

    前缀比较忽略大小写并去掉首尾空白。
    """
    params = rule.get('params') or {}
    collection = ctx.collections.get(params.get('collection', ''), ())
    field_name = params.get('field', 'sql_text')
    prefixes = [str(prefix).lower() for prefix in (params.get('prefixes') or ())]
    count = 0
    for item in collection:
        text = str(item.get(field_name) or '').strip().lower()
        if any(text.startswith(prefix) for prefix in prefixes):
            count += 1
    return [({}, count, ctx.now_ms)]


def eval_instance_field(metric_rule: Any, rule: Mapping[str, Any], ctx: RuleContext,
                         deltas: Any) -> List[MetricValue]:
    """实例元数据里的一个字段（``engine_version`` 等）。"""
    field_name = (rule.get('params') or {}).get('field')
    return [({}, ctx.instance.get(field_name), ctx.now_ms)]
