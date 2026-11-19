# -*- coding: utf-8 -*-
"""节点身份解析：resourceName 的取值口径。"""
import pytest

from tpops_kafka_collector.mapping.nodes import (
    find_node_by_id,
    find_node_by_ip,
    resolve_node_context,
    select_master_node,
)

# 节点清单样本（key-view-execute-node 的形状）：
# 3 个 CN master + 同一批物理节点上的 DN（node_id 复用）
NODES = [
    {'node_id': 'node-a', 'role': 'master', 'type': 'CN',
     'component_id': 'cn_5001', 'node_ip': '172.16.103.126'},
    {'node_id': 'node-b', 'role': 'master', 'type': 'CN',
     'component_id': 'cn_5002', 'node_ip': '172.16.103.127'},
    {'node_id': 'node-c', 'role': 'master', 'type': 'CN',
     'component_id': 'cn_5003', 'node_ip': '172.16.103.128'},
    {'node_id': 'node-a', 'role': 'master', 'type': 'DN',
     'component_id': 'dn_6001', 'node_ip': '172.16.103.126'},
]


def test_select_master_node_takes_the_first_master_cn():
    assert select_master_node(NODES)['node_id'] == 'node-a'


def test_select_master_node_falls_back_to_a_master_dn():
    """没有 CN 的实例（集中式版 mode=Ha，master 就是 DN）→ 退化到任意 master。"""
    nodes = [node for node in NODES if node['type'] == 'DN']
    assert select_master_node(nodes)['node_id'] == 'node-a'


def test_select_master_node_without_cn_falls_back_to_any_master():
    nodes = [{'node_id': 'node-d', 'role': 'master', 'type': 'DN'}]
    assert select_master_node(nodes)['node_id'] == 'node-d'


def test_select_master_node_on_empty_list():
    assert select_master_node([]) is None
    assert select_master_node(None) is None


def test_find_node_by_id_and_ip():
    assert find_node_by_id(NODES, 'node-b')['node_ip'] == '172.16.103.127'
    assert find_node_by_ip(NODES, '172.16.103.128')['node_id'] == 'node-c'
    assert find_node_by_id(NODES, 'nope') is None
    assert find_node_by_ip(NODES, '10.0.0.1') is None


def test_resolve_node_context_keeps_a_known_node():
    context = {'node_id': 'node-b', 'host_ip': '172.16.103.127'}
    resolved = resolve_node_context(context, NODES)
    assert resolved['node_id'] == 'node-b'
    assert resolved['host_ip'] == '172.16.103.127'


def test_resolve_node_context_fills_ip_by_node_id():
    resolved = resolve_node_context({'node_id': 'node-c'}, NODES)
    assert resolved['host_ip'] == '172.16.103.128'


def test_resolve_node_context_falls_back_to_master_node():
    """与节点无关的指标 → 第一个 master CN。"""
    resolved = resolve_node_context({'instance_name': 'gauss-6374'}, NODES)
    assert resolved['node_id'] == 'node-a'
    assert resolved['host_ip'] == '172.16.103.126'


def test_resolve_node_context_fills_node_id_by_ip():
    resolved = resolve_node_context({'host_ip': '172.16.103.128'}, NODES)
    assert resolved['node_id'] == 'node-c'


def test_resolve_node_context_keeps_unknown_ip_without_master():
    """只知道 IP、清单里查不到 → 就用 IP，不硬套别的节点。"""
    resolved = resolve_node_context({'host_ip': '10.0.0.9'}, NODES)
    assert resolved['host_ip'] == '10.0.0.9'
    assert 'node_id' not in resolved


def test_resolve_node_context_does_not_mutate_input():
    context = {'instance_name': 'gauss-6374'}
    resolve_node_context(context, NODES)
    assert context == {'instance_name': 'gauss-6374'}


def test_resolve_node_context_without_nodes():
    resolved = resolve_node_context({'node_id': 'node-a'}, [])
    assert resolved['node_id'] == 'node-a'
    assert 'host_ip' not in resolved
