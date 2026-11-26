#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""排除清单的读取（构建期共用）。

排除清单是**数据**，不是代码：

- 包内 ``tpops_kafka_collector/mapping/data/excluded_metrics.json`` 是**运行期真相源**；
- 仓库根 ``data/mapping/excluded_metrics.json`` 是**同源副本**（逐字节一致，
  由 ``tests/unit/test_mapping_data_sync.py`` 的防漂移闸守着）。

构建期的几个生成器（可提供性判定 / 取值规则 / ITSCADA 映射表）都从**同一份**
清单读排除口径 —— 客户口径只有一处，改清单就能改生效口径，不用改代码。
"""
from __future__ import print_function

import io
import json
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: 仓库根的同源副本（生成器的输入都在仓库根 ``data/`` 下，路径保持一致）。
EXCLUDED_PATH = os.path.join(BASE_DIR, 'data', 'mapping', 'excluded_metrics.json')

#: 排除原因的合法取值。
REASON_DUPLICATE = 'duplicate'
REASON_UNSUPPORTED = 'unsupported'


def load_excluded(path=None):
    """读排除清单，返回 ``{指标码: 条目}``。

    :param path: 清单路径；缺省是仓库根的同源副本。
    :return: ``{metric_code: entry}``。
    """
    target = path or EXCLUDED_PATH
    if not os.path.isfile(target):
        raise OSError(f'找不到排除清单：{target}')
    with io.open(target, encoding='utf-8') as handle:
        payload = json.load(handle)
    entries = {}
    for entry in payload.get('excluded') or ():
        code = entry.get('metric_code')
        if not code:
            raise ValueError(f'排除清单里有条目没有 metric_code：{entry!r}')
        entries[code] = entry
    return entries


def excluded_codes(path=None):
    """排除清单里的全部指标码。"""
    return set(load_excluded(path).keys())


def is_excluded(metric_code, path=None):
    """该指标码是否在排除清单里。"""
    return metric_code in excluded_codes(path)
