#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从**实测留档**生成包内的 ``observed_columns.json``（实际返回列的事实基准）。

**为什么要有这份文件**：规格要求「采集端 SHALL 从 ``mapping/data/*.json``
加载全部客户口径」且「运行时不需要仓库根目录」，而启动期校验的第一条
（来源列存在于实测留档的**实际返回列**）的事实基准是实测留档 ——
留档在仓库根 ``data/`` 下，有 194 KB 合并形态 + 98 份逐指标留档。

两份都不该塞进生产包（那是**测试数据**，不是运行时数据）。所以把留档里
**实际返回列**抽成一份**声明**放在包内：

    tpops_kafka_collector/mapping/data/observed_columns.json

采集端启动时拿它比对规则声明的来源列；本脚本负责从留档**现算**这份声明。
``tests/unit/test_mapping_loader.py`` 里有一条断言「包内声明 == 现算结果」，
防漂移。

**声明不丢信息**：抽的是列名集合本身 —— 校验只需要「某个列在不在」，
不需要行数据。

用法::

    python tools/build_observed_columns.py          # 生成并写盘
    python tools/build_observed_columns.py --check  # 只校验是否最新，不写
"""
from __future__ import print_function

import io
import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# 留档 → 实际返回列的实现只有一份：生产包 ``tpops_kafka_collector/mapping/columns.py``。
# 生成器与采集端用同一套逻辑，声明与事实不会各写各的。
from tpops_kafka_collector.mapping.columns import merged_archive_view_columns

MAPPING_DIR = os.path.join(REPO_ROOT, 'data', 'mapping')
RESPONSE_DIR = os.path.join(REPO_ROOT, 'data', 'metric_response')
MERGED_PROBE_PATH = os.path.join(
    REPO_ROOT, 'data', 'merged_sql', 'sql_merge_probe_result.json'
)
MANIFEST_PATH = os.path.join(RESPONSE_DIR, '000manifest.json')
OUTPUT_PATH = os.path.join(
    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'observed_columns.json'
)

class BuildError(Exception):
    """生成失败（留档结构与预期不符）。"""


def _read_json(path):
    with io.open(path, encoding='utf-8') as handle:
        return json.load(handle)


def _extract_series_metrics(steps, series_metrics, collections, instance_fields):
    """A 类：``metric_infos[].metrics[].name`` 是平台指标名。"""
    for step in steps:
        body = step.get('response_body')
        if not isinstance(body, dict):
            continue
        for info in body.get('metric_infos') or []:
            for metric in info.get('metrics') or []:
                name = metric.get('name')
                if name:
                    series_metrics.add(name)


def _extract_collection_columns(steps, series_metrics, collections, instance_fields):
    """C 类：响应体是一个 ``{集合名: [行, ...]}``，行的键就是列。"""
    for step in steps:
        body = step.get('response_body')
        if not isinstance(body, dict):
            continue
        for name, rows in body.items():
            if not isinstance(rows, list):
                continue
            bucket = collections.setdefault(name, set())
            for row in rows:
                if isinstance(row, dict):
                    bucket.update(row.keys())


def _extract_instance_fields(steps, series_metrics, collections, instance_fields):
    """D 类：``instances[]`` 的键就是实例元数据的字段名。"""
    for step in steps:
        body = step.get('response_body')
        if not isinstance(body, dict):
            continue
        for instance in body.get('instances') or []:
            if isinstance(instance, dict):
                instance_fields.update(instance.keys())


# 通道 → 留档里响应体的定位方式。
CHANNEL_EXTRACTORS = {
    '监控指标接口': _extract_series_metrics,
    '诊断优化接口': _extract_collection_columns,
    '实例接口': _extract_instance_fields,
}


def build():
    """现算 ``observed_columns.json`` 的内容。"""
    probe = _read_json(MERGED_PROBE_PATH)
    merged_views = merged_archive_view_columns(probe)
    manifest = _read_json(MANIFEST_PATH)
    series_metrics = set()  # type: set
    collections = {}  # type: dict
    instance_fields = set()  # type: set
    for item in manifest.get('metrics') or []:
        channel = item.get('channel')
        extractor = CHANNEL_EXTRACTORS.get(channel)
        if extractor is None:
            continue
        path = os.path.join(RESPONSE_DIR, item['file'])
        steps = _read_json(path)
        extractor(steps, series_metrics, collections, instance_fields)
    return {
        'version': '1.0',
        'generated_by': 'tools/build_observed_columns.py',
        'generated_from': {
            'merged_probe': 'data/merged_sql/sql_merge_probe_result.json',
            'metric_response': 'data/metric_response/',
        },
        'note': (
            '实测留档的**实际返回列**（事实基准）。启动期校验拿它比对映射表'
            '声明的来源列 —— 留档是事实，映射表是声明。'
            '由 tools/build_observed_columns.py 从留档生成，不许手改。'
        ),
        'merged_views': dict(
            (view, sorted(columns)) for view, columns in merged_views.items()
        ),
        'collections': dict(
            (name, sorted(columns)) for name, columns in collections.items()
        ),
        'instance_fields': sorted(instance_fields),
        'series_metrics': sorted(series_metrics),
    }


def render(payload):
    """序列化成文件内容（稳定格式，便于逐字节比对）。"""
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + '\n'


def main(argv):
    check_only = '--check' in argv
    text = render(build())
    if check_only:
        if not os.path.exists(OUTPUT_PATH):
            print('observed_columns.json 不存在')
            return 1
        with io.open(OUTPUT_PATH, encoding='utf-8') as handle:
            current = handle.read()
        if current != text:
            print('observed_columns.json 与生成器输出不一致，'
                  '请重新运行 tools/build_observed_columns.py')
            return 1
        print('observed_columns.json 是最新的')
        return 0
    directory = os.path.dirname(OUTPUT_PATH)
    if not os.path.isdir(directory):
        os.makedirs(directory)
    with io.open(OUTPUT_PATH, 'w', encoding='utf-8') as handle:
        handle.write(text)
    print('写出 %s' % os.path.relpath(OUTPUT_PATH, REPO_ROOT))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv[1:]))
    except BuildError as exc:
        print('生成失败：%s' % exc)
        sys.exit(2)
