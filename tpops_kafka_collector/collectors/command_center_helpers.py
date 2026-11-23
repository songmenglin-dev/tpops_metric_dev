"""B 类采集器的**纯辅助函数与常量**（取数编排之外的都放这儿）。

拆出来的原因：``collectors/command_center.py`` 越过合同「单文件 < 400 行」，
而这些函数与采集器的编排逻辑无关（纯函数 + 常量），拆开两边都短。

这里只放**不碰 IO、不碰状态**的东西：查表、分页判定、指纹、体积估算。
采集器本身在 ``collectors/command_center.py``。
"""

import hashlib
from typing import Any, List, Mapping, Sequence, Tuple

from ..errors import MappingError
from ..mapping.nodes import select_master_node

__all__ = [
    'COMMAND_CENTER_SOURCE',
    'MIN_PAGE_SIZE',
    'OUTER_LIMIT',
    'OUTPUT_LIMIT_BYTES',
    'OUTPUT_WARN_RATIO',
    'approx_size_bytes',
    'command_center_rules',
    'has_more',
    'host_node_id',
    'is_output_limit_error',
    'merged_views',
    'page_signature',
    'same_as_previous',
    'tier_of',
]

#: 平台时序通道（映射层的 ``source.source`` 值）。
COMMAND_CENTER_SOURCE = 'command_center_sql'

#: 平台 Agent 对命令输出的硬上限（字节）—— **超了整条任务失败，不是截断**。
OUTPUT_LIMIT_BYTES = 102400

#: 接近上限到这个比例就记 WARNING。
OUTPUT_WARN_RATIO = 0.9

#: 缩小页容量的下限（再小就没有意义了）。
MIN_PAGE_SIZE = 10

#: 合并 SQL 的外层 limit（命令 ``general_single_sql_query`` 的模板是
#: ``select * from ({sql_text}) limit {limit}``，外层只取 1 行）。
OUTER_LIMIT = '1000'


def command_center_rules(mapping: Any) -> Tuple[Any, ...]:
    """映射表里属于 B 类（命令中心 SQL）的启用规则。"""
    rules: List[Any] = []
    for rule in getattr(mapping, 'metric_rules', None) or ():
        if not rule.enabled:
            continue
        if rule.source.get('source') == COMMAND_CENTER_SOURCE:
            rules.append(rule)
    return tuple(rules)


def merged_views(mapping: Any) -> Mapping[str, Any]:
    """映射表里的 ``merged_views`` 段（视图名 → json 列 / 分页标记）。"""
    payload = getattr(mapping, 'response_rules', None) or {}
    views = payload.get('merged_views') or {}
    if not views:
        raise MappingError('映射表里没有 merged_views 段 —— 合并形态无法解码')
    return views


def has_more(
    page_contexts: Mapping[str, Any],
    page_size: int,
    paged_views: Sequence[str],
) -> bool:
    """还有下一页吗？

    **只看可增长视图**（``paged = true``）—— 固定视图每页都重复返回全量数据，
    拿它判「还有下一页」会**死循环**。
    """
    for contexts in page_contexts.values():
        for name in paged_views:
            rows = contexts.rows_by_key.get(name) or ()
            if len(rows) >= page_size:
                return True
    return False


def approx_size_bytes(rows: Sequence[Any]) -> int:
    """估算一行批结果的**字节**体积（用于接近输出上限的告警）。

    **不用 ``json.dumps``** —— 产出消息的 ``json.dumps`` 在生产包里只有
    ``kafka/serializer.py`` 一处（AST 守卫全包扫），这里只做体积估算。
    """
    total = 0
    for row in rows:
        if isinstance(row, Mapping):
            for value in row.values():
                total += len(str(value).encode('utf-8'))
        else:
            total += len(str(row).encode('utf-8'))
    return total


def host_node_id(
    nodes: Sequence[Mapping[str, Any]], result_node_ids: Sequence[str]
) -> str:
    """承载资源名的 master —— 节点清单里的**第一个 master CN**。

    与节点无关的指标（``node_scoped = false``）挂在这个节点上，
    所以它们的**值**也必须取自这个节点 —— 值和资源名必须同源。
    """
    host = select_master_node(nodes)
    if host is not None:
        return str(host.get('node_id') or '')
    return result_node_ids[0] if result_node_ids else ''


def page_signature(rows: Sequence[Any]) -> str:
    """一页的指纹（用来认出「平台没生效 offset，返回了同一页」）。

    **不用 ``json.dumps``** —— 生产包里产出消息的 ``json.dumps`` 只有
    ``kafka/serializer.py`` 一处（有 AST 守卫全包扫），
    这里用 ``repr`` 的哈希做指纹就够。
    """
    return hashlib.sha256(repr(list(rows)).encode('utf-8')).hexdigest()


def same_as_previous(
    signatures: Mapping[str, Any], previous: Mapping[str, Any]
) -> bool:
    """这一页是不是和上一页一模一样。"""
    if not previous:
        return False
    return all(
        signatures.get(node_id) == signature
        for node_id, signature in previous.items()
    )


def is_output_limit_error(error: Any) -> bool:
    """失败是不是因为平台输出超了 102400 B。"""
    text = str(error or '')
    return 'exceeds the threshold' in text or str(OUTPUT_LIMIT_BYTES) in text


def tier_of(rules: Sequence[Any]) -> Tuple[int, str]:
    """本轮档位：取各指标里**最高**的档（同长度档位视为同档）。"""
    units = {'秒': 1, '分': 60, '时': 3600, '天': 86400}
    best = 0
    label = ''
    for rule in rules:
        cycle = getattr(rule, 'collect_cycle', None) or {}
        unit = str(cycle.get('unit') or '')
        factor = units.get(unit)
        if factor is None:
            continue
        seconds = int(cycle.get('value') or 0) * factor
        if seconds > best:
            best = seconds
            label = f'{cycle.get("value")} {cycle.get("unit")}'
    return best, label
