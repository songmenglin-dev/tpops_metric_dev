# -*- coding: utf-8 -*-
"""节点身份：resourceName 的取值口径。

**resourceName 只能表示一个节点**（客户口径），所以统一用「节点 IP + 节点 ID」：
``172.16.103.126_3881c04403394ee1b36a2bf873e69007no14``。

与节点无关的指标（命令中心的实例级/表级/库级、诊断优化、实例接口）
挂到节点清单里的**第一个 master CN** 上 —— 固定一个节点，跨轮稳定，
不会因为「这轮 SQL 在哪个 master 上跑」而让资源名漂移。

节点清单来自 ``GET /rds/gaussdb/v3/instances/{id}/key-view-execute-node``，
每条形如::

    {"node_id": "...no14", "node_name": "gauss-6374_gaussdbv5_hcs2-0_0",
     "role": "master", "type": "CN", "component_id": "cn_5001",
     "node_ip": "172.16.103.126"}
"""
from __future__ import print_function

from typing import Any, Dict, Mapping, Optional, Sequence

MASTER_ROLE = 'master'
MASTER_TYPE = 'CN'


def select_master_node(nodes: Optional[Sequence[Mapping[str, Any]]]) -> Optional[Mapping[str, Any]]:
    """节点清单里的第一个 master CN。

    退化顺序：master CN → 任意 master → 清单里第一个节点 → None。
    清单是平台按固定顺序返回的，取第一条即可保证跨轮一致。
    """
    items = list(nodes or ())
    for predicate in (
        lambda node: node.get('role') == MASTER_ROLE and node.get('type') == MASTER_TYPE,
        lambda node: node.get('role') == MASTER_ROLE,
    ):
        for node in items:
            if predicate(node):
                return node
    return items[0] if items else None


def find_node_by_id(
    nodes: Optional[Sequence[Mapping[str, Any]]],
    node_id: Optional[str],
) -> Optional[Mapping[str, Any]]:
    """按 node_id 找节点（同一个 node_id 在清单里会出现多次：CN 一条 + 各 DN 一条）。"""
    if not node_id:
        return None
    for node in nodes or ():
        if node.get('node_id') == node_id:
            return node
    return None


def find_node_by_ip(
    nodes: Optional[Sequence[Mapping[str, Any]]],
    host_ip: Optional[str],
) -> Optional[Mapping[str, Any]]:
    """按节点 IP 找节点。"""
    if not host_ip:
        return None
    for node in nodes or ():
        if node.get('node_ip') == host_ip:
            return node
    return None


def resolve_node_context(
    context: Optional[Mapping[str, Any]],
    nodes: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    """补齐资源上下文里的节点身份。

    - 有 node_id → 用它，缺 IP 就从节点清单反查；
    - 只有 IP → 按 IP 反查 node_id，查不到就只用 IP（resourceName 退到 IP，
      不硬套别的节点 —— 宁可资源名对不上台账，也不能指错节点）；
    - 什么都没有（与节点无关的指标）→ 用第一个 master CN 兜底。

    返回新字典，不改传入的 context。
    """
    resolved = dict(context or {})
    node_id = resolved.get('node_id')
    if node_id:
        matched = find_node_by_id(nodes, node_id)
        if matched and not resolved.get('host_ip'):
            resolved['host_ip'] = matched.get('node_ip')
        return resolved
    host_ip = resolved.get('host_ip')
    if host_ip:
        matched = find_node_by_ip(nodes, host_ip)
        if matched:
            resolved['node_id'] = matched.get('node_id')
        return resolved
    master = select_master_node(nodes)
    if master is None:
        return resolved
    resolved['node_id'] = master.get('node_id')
    resolved['host_ip'] = master.get('node_ip')
    return resolved
