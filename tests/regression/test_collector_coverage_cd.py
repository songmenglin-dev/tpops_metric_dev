# -*- coding: utf-8 -*-
"""采集器层回归的 **C / D 两条通道**（I-4 / T-16）。

C 类（诊断优化接口，4 条）：慢 SQL / Top SQL 两个集合；
D 类（实例接口，1 条）：实例元数据里的一个字段。

**走采集器**（不是只测映射层）：用**实测留档回放**驱动真实的采集器，
断言该通道的**全部**可提供指标都产出了消息，且**值等于期望值表**
（``tests/fixtures/expected_values_non_b.json``，由独立推导器从留档原始响应推出）。

A / B 两条通道与「98 条总闸」在 ``test_collector_coverage.py``；
公共件在 ``collector_support.py``。
"""

import os

from regression.collector_support import (
    DIAGNOSTIC_CHANNEL,
    FakeDiagnosticClient,
    FakeInstanceClient,
    FakeNodeRegistry,
    EXPECTED_NOW_MS,
    INSTANCE_CHANNEL,
    INSTANCE_ID,
    channel_metrics,
    expected_non_b,
    expected_value,
    find,
    manifest,
    mapping,
    nodes,
)
from regression.conftest import METRIC_RESPONSE_DIR, load_json

from tpops_kafka_collector.collectors.diagnostic import DiagnosticCollector
from tpops_kafka_collector.collectors.instance import InstanceCollector

def _diagnostic_bodies(manifest):
    """留档里的慢 SQL / Top SQL 响应体。"""
    slow = None
    top = None
    for item in channel_metrics(manifest, DIAGNOSTIC_CHANNEL):
        body = load_json(os.path.join(METRIC_RESPONSE_DIR, item['file']))[0]
        response = body.get('response_body') or {}
        if 'slow_sql_infos' in response:
            slow = response
        if 'top_sql_infos' in response:
            top = response
    return slow, top


def test_c_class_produces_every_providable_metric(
    mapping, nodes, manifest, expected_non_b
):
    """C 类 4 条指标都产出消息，值等于期望值表。"""
    slow, top = _diagnostic_bodies(manifest)
    assert slow is not None and top is not None, '留档里缺慢 SQL / Top SQL 响应'

    collector = DiagnosticCollector(
        client=FakeDiagnosticClient(slow, top),
        mapping=mapping,
        # 脱敏由 tests/unit/test_masking.py 守 —— 这里用恒等函数，
        # 才能与期望值表（**未脱敏**的留档原值）逐条相等。
        sql_sanitizer=lambda text: text,
        clock=lambda: EXPECTED_NOW_MS,
    )
    messages = collector.collect(instance_id=INSTANCE_ID, nodes=nodes)

    expected_codes = set(
        code for code, entry in expected_non_b.items()
        if entry['channel'] == 'diagnostic'
    )
    assert expected_codes, '期望值表里没有 C 类的指标'
    for code in sorted(expected_codes):
        entry = expected_non_b[code]
        for item in entry['expected']:
            matched = find(messages, code, item['dimensions'])
            assert matched, 'C 类 %s 没产出这条' % code
            expected = expected_value(mapping, code, item['value'])
            assert matched['value'] == expected, (
                code, matched['value'], expected,
            )


def test_d_class_produces_every_providable_metric(
    mapping, nodes, manifest, expected_non_b
):
    """D 类 1 条指标产出消息，值等于期望值表。"""
    item = channel_metrics(manifest, INSTANCE_CHANNEL)[0]
    body = load_json(os.path.join(METRIC_RESPONSE_DIR, item['file']))[0]['response_body']
    collector = InstanceCollector(
        client=FakeInstanceClient(body),
        mapping=mapping,
        node_registry=FakeNodeRegistry(nodes),
        clock=lambda: EXPECTED_NOW_MS,
    )
    result = collector.collect(INSTANCE_ID)

    expected_codes = set(
        code for code, entry in expected_non_b.items()
        if entry['channel'] == 'instance'
    )
    assert expected_codes, '期望值表里没有 D 类的指标'
    for code in sorted(expected_codes):
        entry = expected_non_b[code]
        for expected_item in entry['expected']:
            matched = find(result.messages, code, expected_item['dimensions'])
            assert matched, 'D 类 %s 没产出这条' % code
            expected = expected_value(mapping, code, expected_item['value'])
            assert matched['value'] == expected, (
                code, matched['value'], expected,
            )
