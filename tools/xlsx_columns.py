# -*- coding: utf-8 -*-
"""按表头名定位 Excel 的列，不要按位置猜。

客户指标表会被追加或插入列（例如新增「指标级别」列），
按固定列号读会把后面所有字段整体读串位 —— 一列的偏移就足以让
「指标获取方法」变成「指标单位」、「数据更新周期」变成「采集更新周期单位」，
而且不报错，只是悄悄生成错的规则。

这里统一按表头名定位：列挪了位置不影响，列名写错会立刻 KeyError。
"""
from __future__ import print_function

from typing import Any, Dict, List, Sequence


def column_map(headers: Sequence[Any]) -> Dict[str, int]:
    """表头行 → {表头名: 列号}。空表头忽略。"""
    mapping = {}
    for index, name in enumerate(headers):
        if name is None:
            continue
        key = str(name).strip()
        if key:
            mapping[key] = index
    return mapping


def require(mapping: Dict[str, int], name: str) -> int:
    """取列号；表头里没有这个名字就报错（别静默取错列）。"""
    if name not in mapping:
        raise KeyError('客户指标表里没有「%s」这一列，现有表头：%s'
                       % (name, '、'.join(sorted(mapping))))
    return mapping[name]


def row_value(row: Sequence[Any], mapping: Dict[str, int], name: str) -> Any:
    """按表头名取单元格的值。"""
    return row[require(mapping, name)]
