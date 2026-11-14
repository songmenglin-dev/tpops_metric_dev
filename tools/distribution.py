# -*- coding: utf-8 -*-
"""实测：哪些指标的数据是按节点分布的。

判断依据全部来自 ``data/metric_response/`` 的实测留档，不靠推测：

- **命令中心（B 类）**：同一条合并 SQL 会在每个 master 上各跑一次。
  比对各 master 返回的结果：
  - 结果**相同** → 数据来自 DN 侧视图，各 master 看到的是同一份 → **不是节点级**
  - 结果**不同** → 数据来自 CN 自身会话视图，是节点视角 → **节点级**
- **监控指标（A 类）**：``series`` 按组件展开，组件即节点 → **节点级**；
  只有单值形态（series 只有一个 ``value`` 键）是实例级。
- **诊断优化（C 类）/ 实例接口（D 类）**：一次调用返回整实例的数据 → **实例级**

master 视角是**合并 SQL 的属性**（11 条 SQL），所以按 ``sql_key`` 归类，
实测一次到处复用。

注意：这与"业务对象粒度"（表/库/端口/实例，由指标名里的占位符决定）是
两个独立维度 —— 表级指标也可能是节点级，实例级指标也可能不是。
"""
from __future__ import print_function

import io
import json
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESPONSE_DIR = os.path.join(REPO_ROOT, 'data', 'metric_response')

IDENTICAL = 'identical'    # 各 master 结果相同 → 不是节点级
DIVERGENT = 'divergent'    # 各 master 结果不同 → 节点级

# A 类：单值形态（series 只有一个 'value' 键）是实例级，其余按组件展开 → 节点级
SINGLE_VALUE_KEY = 'value'


def _read_json(path):
    with io.open(path, encoding='utf-8') as fp:
        return json.load(fp)


def load_samples():
    """留档索引：客户指标 → {file, steps}。"""
    manifest = _read_json(os.path.join(RESPONSE_DIR, '000manifest.json'))
    samples = {}
    for item in manifest['metrics']:
        samples[item['customer_metric']] = {
            'file': item['file'],
            'steps': _read_json(os.path.join(RESPONSE_DIR, item['file'])),
        }
    return samples


def _detail_results(sample):
    """命令中心链路里各 master 的任务明细结果（归一化成可比字符串）。"""
    results = []
    for step in sample['steps']:
        api = step['api']
        if 'task-record' in api and api.rstrip('/').endswith('detail'):
            try:
                rows = json.loads(step['response_body'].get('result') or '[]')
            except ValueError:
                rows = []
            results.append(json.dumps(rows, sort_keys=True, ensure_ascii=False))
    return results


def master_view_by_sql_key(samples, sql_key_by_metric):
    """按 sql_key 归类：各 master 的结果是否相同。"""
    views = {}
    for code, sql_key in sql_key_by_metric.items():
        if not sql_key or sql_key in views:
            continue
        sample = samples.get(code)
        if not sample:
            continue
        results = _detail_results(sample)
        if len(results) < 2:
            continue
        views[sql_key] = IDENTICAL if len(set(results)) == 1 else DIVERGENT
    return views


def series_is_component_scoped(sample):
    """A 类的 series 是否按组件（节点）展开。None 表示不是 A 类。"""
    if not sample:
        return None
    for step in sample['steps']:
        if 'monitor/metrics' not in step['api']:
            continue
        for info in step['response_body'].get('metric_infos') or ():
            for metric in info.get('metrics') or ():
                keys = list((metric.get('series') or {}).keys())
                if not keys:
                    continue
                return not all(key == SINGLE_VALUE_KEY for key in keys)
    return None


def node_scoped_of(channel, master_view, series_scoped):
    """这个指标的数据是不是按节点分布的。"""
    if channel == 'command_center_sql':
        return master_view == DIVERGENT
    if channel == 'monitor_metrics':
        return bool(series_scoped)
    return False
