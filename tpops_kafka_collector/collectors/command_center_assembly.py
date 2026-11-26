"""B 类采集器的**归集与组装**（多 master 结果 → ITSCADA 指标消息）。

从 ``collectors/command_center.py`` 拆出来的：那一段只做「把各 master 的
取值归集成消息」，与「怎么取数（分页 / 重试 / 解码）」无关，
两边分开各自都短。

三条口径（都在这里实现）：

1. **``node_scoped`` 归集** —— ``true`` 的指标各 master 各自一条；
   ``false`` 的与节点无关，只落在**承载资源名**的那个 master 上。
2. **值与资源名必须同源** —— 与节点无关的指标的值取自承载资源名的 master；
   承载 master 本轮没回结果时，值来自别的 master 就**不产出**
   （值与名不同源 = 数据错）。
3. **差分基线按节点隔离** —— 每个 master 用自己的基线视图
   （基线键不含节点，不隔离会互相覆盖）。
"""

from typing import Any, Callable, Dict, List, Mapping, Sequence, Tuple
from loguru import logger

from ..mapping.message import build_metric_message
from ..mapping.rules import evaluate
from .command_center_helpers import host_node_id

LOGGER = logger.bind(channel="command_center")

__all__ = ['build_messages']


def build_messages(
    mapping: Any,
    rules: Sequence[Any],
    merged_by_node: Sequence[Tuple[Any, Any]],
    nodes: Sequence[Mapping[str, Any]],
    instance_id: str,
    instance_name: str,
    delta_store: Callable[[Any], Any],
) -> List[Dict[str, Any]]:
    """按 ``node_scoped`` 归集多 master 结果并组装消息。

    :param merged_by_node: ``(node_id, 合并后的上下文)`` 序列
    :param nodes: 节点清单（把 node_id 换算成 host_ip）
    :param delta_store: ``node_id → 差分基线视图`` 的工厂
        （基线**按节点隔离**）
    """
    values_by_node = _values_by_node(rules, merged_by_node, delta_store)
    host_id = host_node_id(nodes, list(values_by_node))
    shared = values_by_node.get(host_id) or {}
    host_available = bool(shared)
    if not shared and values_by_node:
        # 节点清单里的第一个 master 不在结果里（平台返回的 master 集合不同）
        # → 退回结果的第一个，但**要记 WARNING**，别静默错配。
        shared = list(values_by_node.values())[0]
        LOGGER.warning(
            '承载资源名的 master（{}）不在本轮结果里 —— '
            '与节点无关的指标退回用第一个结果节点的值',
            host_id,
        )
    messages = []  # type: List[Dict[str, Any]]
    for rule in rules:
        targets = (
            list(values_by_node.items())
            if rule.node_scoped
            else [(host_id, shared)]
        )
        for node_id, values in targets:
            messages.extend(_rule_messages(
                mapping, rule, node_id, values, instance_id, instance_name,
                nodes, fallback=not host_available,
            ))
    return messages


def _values_by_node(
    rules: Sequence[Any],
    merged_by_node: Sequence[Tuple[Any, Any]],
    delta_store: Callable[[Any], Any],
) -> Dict[str, Dict[str, Any]]:
    """每个 master 的各规则取值（**差分基线按节点隔离**）。"""
    values_by_node = {}  # type: Dict[str, Dict[str, Any]]
    for node_id, contexts in merged_by_node:
        values = {}  # type: Dict[str, Any]
        store = delta_store(node_id)
        for rule in rules:
            sql_key = rule.source.get('sql_key')
            ctx = contexts.by_sql_key.get(sql_key)
            if ctx is None:
                continue
            values[rule.metric_code] = evaluate(rule, ctx, store)
        values_by_node[node_id] = values
    return values_by_node


def _rule_messages(
    mapping: Any,
    rule: Any,
    node_id: Any,
    values: Mapping[str, Any],
    instance_id: str,
    instance_name: str,
    nodes: Sequence[Mapping[str, Any]],
    fallback: bool,
) -> List[Dict[str, Any]]:
    """一条规则在一个节点上的消息。"""
    if fallback and not rule.node_scoped and rule.master_view != 'identical':
        # 承载资源名的 master 本轮没回结果，而各 master 的值**不一致**
        # → 拿别的节点的值挂到承载节点上就是「值与资源名不同源」，
        #   本轮不产出（不静默错配）。
        LOGGER.warning(
            '与节点无关的指标 {} 的承载 master 本轮没回结果，'
            '且各 master 的值不一致（master_view={}）—— 本轮不产出该指标',
            rule.metric_code, rule.master_view,
        )
        return []
    messages = []  # type: List[Dict[str, Any]]
    for dims, value, timestamp in values.get(rule.metric_code) or ():
        if value is None:
            continue
        messages.append(build_metric_message(
            field_map=mapping.field_map,
            metric_rule=rule,
            dimensions=dims,
            value=value,
            resource_context=_resource_context(
                instance_id, instance_name, node_id, rule.node_scoped,
            ),
            timestamp=timestamp,
            nodes=nodes,
        ))
    return messages


def _resource_context(
    instance_id: str,
    instance_name: str,
    node_id: Any,
    node_scoped: bool,
) -> Dict[str, Any]:
    """资源上下文。

    ``node_scoped`` 的指标锚在**自己那个 master** 上；
    与节点无关的指标不写 ``node_id``，由 ``resolve_node_context``
    兜底到节点清单里的**第一个 master CN**（跨轮稳定）。
    """
    context = {
        'instance_id': instance_id,
        'instance_name': instance_name,
    }  # type: Dict[str, Any]
    if node_scoped:
        context['node_id'] = node_id
    return context
