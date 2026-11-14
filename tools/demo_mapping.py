#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""演示：一条指标从 TPOPS 原始 JSON 到客户 Kafka JSON 的完整映射。

用仓库里的**实测留档**跑，逐字段标出「TPOPS 的哪个字段 → 客户的哪个字段」。

用法::

    python tools/demo_mapping.py
"""
from __future__ import print_function

import io
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, 'tools'))

from mappings import MappingTable, RuleContext, build_metric_message, evaluate  # noqa: E402

DATA_DIR = os.path.join(BASE_DIR, 'data')


def _read(path):
    with io.open(path, encoding='utf-8') as handle:
        return json.load(handle)


def _dump(obj, limit=700):
    text = json.dumps(obj, ensure_ascii=False, indent=2)
    return text if len(text) <= limit else text[:limit] + '\n  ...（已截断）'


def demo_a_class(table):
    print('=' * 78)
    print('示例一：A 类　监控指标接口')
    print('=' * 78)

    # 样本来自 data/metric_response/ 的逐指标原生响应留档
    calls = _read(os.path.join(
        DATA_DIR, 'metric_response',
        '001_GAUSSDB.Number_of_ansactions_mmitted_r_cond.json',
    ))
    info = calls[0]['response_body']['metric_infos'][0]
    metric = info['metrics'][0]
    actual = {
        'series': metric['series'],
        'timestamps': metric['timestamps'],
        'metric_name': metric['metric_name'],
        'collection_frequency': metric['collection_frequency'],
        'instance_id': info['instance_id'],
        'time_interval': info['time_interval'],
    }
    tpops_metric_id = metric['name']
    customer_metric = 'GAUSSDB.Number_of_ansactions_mmitted_r_cond'

    print('\n【第 1 步】TPOPS 返回的原始 JSON（实测留档，裁剪过）')
    raw = {
        'metric_infos': [{
            'metrics': [{
                'name': tpops_metric_id,
                'metric_name': actual['metric_name'],
                'series': {key: values[-3:] for key, values in actual['series'].items()},
                'timestamps': actual['timestamps'][-3:],
                'collection_frequency': actual['collection_frequency'],
            }],
            'instance_id': actual['instance_id'],
            'time_interval': actual['time_interval'],
        }],
    }
    print(_dump(raw))

    print('\n【第 2 步】按 TPOPS 指标 id 查映射表 → 拿到客户指标规则')
    print('  查表键：TPOPS 的 metrics[].name  ==  映射表的 source.tpops_metric')
    rule = table.metric_rule(customer_metric)
    print('  TPOPS 指标 id     : %s' % tpops_metric_id)
    print('  命中客户指标英文名 : %s' % rule.metric_code)
    print('  命中客户指标中文名 : %s' % rule.metric_name)
    print('  客户表单位        : %s' % rule.unit)
    print('  取值规则          : %s' % json.dumps(rule.value_rule, ensure_ascii=False))

    print('\n【第 3 步】按规则取值')
    ctx = RuleContext(
        series=actual['series'],
        timestamps=actual['timestamps'],
        now_ms=int(actual['timestamps'][-1]),
    )
    results = evaluate(rule, ctx)
    print('  取到 %d 条结果（每个 series 键一条）' % len(results))
    dims, value, clock = results[0]
    print('  第 1 条：维度=%s 值=%s 时间戳=%s' % (json.dumps(dims, ensure_ascii=False), value, clock))

    print('\n【第 4 步】组装成客户 Kafka 消息')
    message = build_metric_message(
        table.field_map,
        rule,
        dims,
        value,
        {'instance_name': 'gauss-6374', 'node_id': 'node-1'},
        clock,
    )
    print(_dump(message))

    print('\n【第 5 步】逐字段对照')
    pairs = [
        ('metricName', '映射表 metric_name（按 TPOPS 指标 id 查到的客户中文名）',
         rule.metric_name),
        ('metricCode', '映射表 metric_code（客户英文名）', rule.metric_code),
        ('resourceName', 'instances[].name + 节点清单 node_id', 'gauss-6374_node-1'),
        ('value', 'metrics[].series[<series 键>][最后一个非空点]', value),
        ('unit', '映射表 unit（客户指标表【指标单位】）', rule.unit),
        ('clock', 'metrics[].timestamps[同一下标]', clock),
        ('tags', 'instances[] / 节点清单 / series 键 组装', '见上'),
    ]
    for customer_field, origin, value_text in pairs:
        print('  %-13s ← %-52s = %s' % (
            customer_field, origin, json.dumps(value_text, ensure_ascii=False)))


def demo_b_class(table):
    print()
    print('=' * 78)
    print('示例二：B 类　命令中心 SQL')
    print('=' * 78)

    rows = [
        {'datname': 'postgres', 'xact_commit': '7265656', 'numbackends': '3'},
        {'datname': 'template1', 'xact_commit': '12', 'numbackends': '0'},
    ]
    print('\n【第 1 步】命令中心返回的原始 JSON（result 是 JSON 字符串，要二次解析）')
    raw = {
        'node_id': 'd5284a09d06c4b2798c8c713259fe705no14',
        'status': 'succeed',
        'result': json.dumps(rows, ensure_ascii=False),
    }
    print(_dump(raw))
    print('\n  ↑ result 是字符串，json.loads 两次之后才是上面这样的行数组；'
          '行里的值全是字符串')

    rule = table.metric_rule('GAUSSDB.[{#DATNAME}]_committed_transactions')
    print('\n【第 2 步】命中规则')
    print('  客户指标 : %s' % rule.metric_code)
    print('  取值规则 : %s' % json.dumps(rule.value_rule, ensure_ascii=False))
    print('  维度绑定 : %s' % json.dumps(rule.dimension_bindings, ensure_ascii=False))

    print('\n【第 3 步】按规则取值（逐行）')
    ctx = RuleContext(rows=rows, now_ms=1793921105094)
    results = evaluate(rule, ctx)
    for dims, value, clock in results:
        print('  维度=%s 值=%s' % (json.dumps(dims, ensure_ascii=False), value))

    print('\n【第 4 步】组装成客户 Kafka 消息（第 1 条）')
    dims, value, clock = results[0]
    message = build_metric_message(
        table.field_map, rule, dims, value,
        {'instance_name': 'gauss-75a2', 'node_id': 'd5284a09d06c4b2798c8c713259fe705no14'},
        clock,
    )
    print(_dump(message))
    print('\n  ↑ 注意 metricName / metricCode 里的 {#DATNAME} 已被换成实际值 postgres')


def main():
    table = MappingTable.load()
    demo_a_class(table)
    demo_b_class(table)
    print()
    print('=' * 78)
    print('查看全部映射：docs/映射表-ITSCADA字段对照.md')
    print('=' * 78)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
