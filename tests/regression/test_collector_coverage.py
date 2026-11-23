# -*- coding: utf-8 -*-
"""采集器层的「98 个可提供指标全部产出数据」回归（A / B 类 + 总闸，I-4 / T-16）。

**回归层原本只测映射层**（``decode_merged`` + ``evaluate``）——
**从没走过采集器**。合同完成标准「98 个可提供指标全部产出数据」因此
在采集器层**没有任何守卫**：wave-2b 的三条 Critical 全是采集器层的
（E 类采集器用生产映射对象必崩、B 类差分指标永不产出、差分基线跨 master 污染），
**一条都没被现有守卫拦住**。

本文件**走采集器**：用**实测留档回放**驱动真实的采集器，
断言该通道的**全部**可提供指标都产出了消息，且**值等于期望值表**。

通道与条数（合计 98 条可提供指标）：

===========  ======  ==========================================
通道          条数    取数路径
===========  ======  ==========================================
B 类           82    命令中心合并 SQL（``stat_database`` 等 11 个视图）
A 类           11    监控指标接口（``POST /rds/v3/monitor/metrics``）
C 类            4    诊断优化接口（``/rds/gaussdb/v3/.../slow-sql-list`` 等）
D 类            1    实例接口（``GET /rds/v3.2/instances``）
===========  ======  ==========================================

C / D 两条通道的用例在 ``test_collector_coverage_cd.py``；
公共件（假客户端 / 夹具 / 比对辅助）在 ``collector_support.py``。
"""

import json
import os

from integration.test_collector_command_center_deltas import (
    DELTA_AMOUNT_BY_NODE_NAME,
    DELTA_CODES,
    ELAPSED_SECONDS,
    ROUND1_NOW_MS,
    DeltaCommandCenter,
    _node_id_by_name,
    _rates_by_node,
)
from regression.collector_support import (
    COMMAND_CENTER_CHANNEL,
    FakeDiagnosticClient,
    FakeInstanceClient,
    FakeNodeRegistry,
    DIAGNOSTIC_CHANNEL,
    EXPECTED_NOW_MS,
    INSTANCE_CHANNEL,
    INSTANCE_ID,
    MONITOR_CHANNEL,
    canon,
    channel_metrics,
    collect_monitor,
    expected_non_b,
    expected_value,
    find,
    template_code,
    manifest,
    mapping,
    nodes,
    providable_codes,
)
from regression.conftest import MERGED_PROBE_PATH, METRIC_RESPONSE_DIR, load_json

from tpops_kafka_collector.collectors.command_center import CommandCenterCollector
from tpops_kafka_collector.collectors.diagnostic import DiagnosticCollector
from tpops_kafka_collector.collectors.instance import InstanceCollector
from tpops_kafka_collector.mapping.rules import CounterDeltaStore


# ---------------------------------------------------------------------------
# A 类（11 条）
# ---------------------------------------------------------------------------

def test_a_class_produces_every_providable_metric(
    mapping, nodes, manifest, expected_non_b, providable_codes
):
    """A 类 11 条指标**逐条**都产出消息，且值等于期望值表。

    **逐份留档回放** —— 一份留档只含一个平台指标，多个客户指标共用同一平台
    指标时（``gaussdb_instance_status`` 被 3 条规则用）各自用自己那份留档比对，
    值才与期望值表对得上。
    """
    produced = {}
    for item in channel_metrics(manifest, MONITOR_CHANNEL):
        messages = collect_monitor(mapping, nodes, item['file'])
        for message in messages:
            produced.setdefault(message['metricCode'], []).append(message)

    expected_codes = set(
        code for code, entry in expected_non_b.items()
        if entry['channel'] == 'monitor_metrics'
    )
    assert expected_codes, '期望值表里没有 A 类的指标'
    missing = sorted(expected_codes - set(produced))
    assert not missing, 'A 类没产出消息的指标：%s' % missing

    for code in sorted(expected_codes):
        entry = expected_non_b[code]
        for item in entry['expected']:
            matched = find(produced[code], code, item['dimensions'])
            assert matched, 'A 类 %s 没产出这条（维度 %s）' % (code, item['dimensions'])
            want = expected_value(mapping, code, item['value'])
            assert canon(matched['value']) == canon(want), (
                code, item['dimensions'], matched['value'], want,
            )


# ---------------------------------------------------------------------------
# B 类（82 条）
# ---------------------------------------------------------------------------

def test_b_class_produces_every_providable_metric(mapping, nodes, manifest):
    """B 类 82 条指标码**一条不落**（第一轮 + 第二轮）。

    第一轮 5 个 ``counter_delta`` 指标只建基线（没有值），第二轮才产出 ——
    所以这里跑**两轮**。
    """
    probe = load_json(MERGED_PROBE_PATH)['nodes']
    cc = DeltaCommandCenter(probe, nodes)
    state = {'round': 0}

    def _clock():
        return ROUND1_NOW_MS + state['round'] * ELAPSED_SECONDS * 1000

    collector = CommandCenterCollector(
        client=None, mapping=mapping, command_center=cc,
        deltas=CounterDeltaStore(), clock=_clock,
    )
    produced = set()
    for index in (0, 1):
        state['round'] = index
        cc.round_index = index
        result = collector.collect(instance_id=INSTANCE_ID)
        for message in result.messages:
            produced.add(template_code(message))

    rules = [
        rule for rule in mapping.metric_rules
        if rule.enabled and rule.source.get('source') == 'command_center_sql'
    ]
    assert len(rules) == 82
    missing = sorted(rule.metric_code for rule in rules
                     if rule.metric_code not in produced)
    # 留档里一条 ``AccessExclusiveLock`` 锁都没有 → 这个指标本来就没有值
    assert missing == ['gaussdb.locks.accessexclusive[{#DBNAME}]'], missing


def test_every_master_computes_its_own_rate_in_the_regression_layer(
    mapping, nodes, manifest
):
    """回归层也要能拦住「差分基线跨 master 污染」（wave-2b 遗留 Minor **M-N4**）。

    **为什么回归层要补这条**：wave-2b 的 C-3 就是这种形态 ——
    把差分作用域去掉后，``tests/regression`` 仍然全绿（B 类回归只断言
    「产出的指标码集合」，**不比对各 master 的速率**）。
    而 **wave 4 的交付文档验收只跑 ``tests/regression``** ——
    守卫缺口留着就等于交付物缺一道闸。

    **口径**与 ``tests/integration/test_collector_command_center_deltas.py``
    同源：三个 master 的累计计数增量被故意设成 6000 / 3000 / 9000，
    基线共享时三个速率**全错**（实测 100/50/150 会变成 50/None/200），
    基线按节点隔离后才各归各。
    """
    probe = load_json(MERGED_PROBE_PATH)['nodes']
    cc = DeltaCommandCenter(probe, nodes)
    state = {'round': 0}

    def _clock():
        return ROUND1_NOW_MS + state['round'] * ELAPSED_SECONDS * 1000

    collector = CommandCenterCollector(
        client=None, mapping=mapping, command_center=cc,
        deltas=CounterDeltaStore(), clock=_clock,
    )
    # 第一轮只建基线，第二轮才算得出速率。
    for index in (0, 1):
        state['round'] = index
        cc.round_index = index
        result = collector.collect(instance_id=INSTANCE_ID)

    rates = _rates_by_node(result.messages)
    node_id_by_name = _node_id_by_name(nodes)
    expected_by_node = {}
    for node_name, amount in DELTA_AMOUNT_BY_NODE_NAME.items():
        node_id = node_id_by_name.get(node_name)
        if node_id:
            expected_by_node[node_id] = amount // ELAPSED_SECONDS

    assert len(expected_by_node) == 3, expected_by_node
    for node_id, expected in expected_by_node.items():
        values = rates.get(node_id) or {}
        assert values, '节点 %s 一条差分指标都没产出' % node_id
        for code in DELTA_CODES:
            assert values.get(code) == expected, (
                node_id, code, values.get(code), expected,
            )
    # 三个 master 的速率必须**互不相同** —— 否则这条断言抓不到「基线被共享」。
    distinct = set(expected_by_node.values())
    assert len(distinct) == 3, expected_by_node




# ---------------------------------------------------------------------------
# 合同完成标准：98 个可提供指标全部产出数据
# ---------------------------------------------------------------------------

def test_all_ninety_eight_providable_metrics_are_produced_by_the_collectors(
    mapping, nodes, manifest, providable_codes
):
    """**98 个可提供指标**在**采集器层**全部产出消息（合同的完成标准）。

    这是把四个通道的产出合起来看：缺一条就是「某个指标在采集器层不产出」。
    """
    produced = set()

    for item in channel_metrics(manifest, MONITOR_CHANNEL):
        for message in collect_monitor(mapping, nodes, item['file']):
            produced.add(message['metricCode'])

    slow = None
    top = None
    for item in channel_metrics(manifest, DIAGNOSTIC_CHANNEL):
        body = load_json(os.path.join(METRIC_RESPONSE_DIR, item['file']))[0]
        if 'slow_sql_infos' in body.get('response_body', {}):
            slow = body['response_body']
        if 'top_sql_infos' in body.get('response_body', {}):
            top = body['response_body']
    diagnostic = DiagnosticCollector(
        client=FakeDiagnosticClient(slow, top),
        mapping=mapping,
        sql_sanitizer=lambda text: text,
        clock=lambda: EXPECTED_NOW_MS,
    )
    for message in diagnostic.collect(instance_id=INSTANCE_ID, nodes=nodes):
        produced.add(message['metricCode'])

    instance_body = load_json(
        os.path.join(METRIC_RESPONSE_DIR,
                     channel_metrics(manifest, INSTANCE_CHANNEL)[0]['file'])
    )[0]['response_body']
    instance = InstanceCollector(
        client=FakeInstanceClient(instance_body),
        mapping=mapping,
        node_registry=FakeNodeRegistry(nodes),
        clock=lambda: EXPECTED_NOW_MS,
    )
    for message in instance.collect(INSTANCE_ID).messages:
        produced.add(message['metricCode'])

    probe = load_json(MERGED_PROBE_PATH)['nodes']
    cc = DeltaCommandCenter(probe, nodes)
    state = {'round': 0}

    def _clock():
        return ROUND1_NOW_MS + state['round'] * ELAPSED_SECONDS * 1000

    command_center = CommandCenterCollector(
        client=None, mapping=mapping, command_center=cc,
        deltas=CounterDeltaStore(), clock=_clock,
    )
    for index in (0, 1):
        state['round'] = index
        cc.round_index = index
        for message in command_center.collect(instance_id=INSTANCE_ID).messages:
            produced.add(template_code(message))

    assert len(providable_codes) == 98, len(providable_codes)
    missing = sorted(providable_codes - produced)
    # 留档里**一条 AccessExclusiveLock 锁都没有** → 期望值表里这个指标的条目
    # 是空列表 → 本来就没有值可产出（「缺值不产出假数据」是正确行为）。
    assert missing == ['gaussdb.locks.accessexclusive[{#DBNAME}]'], missing
    assert len(produced) == 97, len(produced)
    # 反向：采集器不该产出「不可提供」的指标
    extra = sorted(produced - providable_codes)
    assert not extra, '产出了客户指标库里不是「可提供」的指标：%s' % extra
