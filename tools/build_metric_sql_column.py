#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把命令中心 SQL 通道实际使用的 SQL 回填到客户指标明细表的第 2 个 sheet。

输入：
  - 高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx / 指标库-指标获取方法
      —— 101 个客户指标，第 10 列标注「指标获取方法」
  - data/mapping/metric_rules.json
      —— 逐指标的取数通道与命令中心 SQL 的 sql_key（由 tools/build_itscada_mapping.py 生成）
  - data/merged_sql/merged_sql.json
      —— 11 条合并 SQL 的正式定义（SQL 文本、来源视图、刷新档）

输出：
  - 同一个 xlsx 的「指标库-指标获取方法」sheet 末尾新增一列「使用的SQL」：
    走命令中心 SQL 的 82 个指标填入所属合并 SQL 的完整语句，其余指标留空。

说明：
  - 11 条合并 SQL 覆盖 82 个客户指标（见 data/merged_sql/README.md），
    同一 sql_key 的指标共用同一条 SQL，因此单元格内容会重复。
  - 平台把 sql_text 包在 `select * from ({sql_text}) limit {limit}` 模板里，
    11 条的 limit 均为 1000；本列填的是我们自己下发的 sql_text 原文。
  - 脚本幂等：重复执行只会重写同一列，不会追加重复列。

用法::

    python tools/build_metric_sql_column.py
"""
from __future__ import print_function

import copy
import json
import os
import shutil
import tempfile

import openpyxl

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
XLSX_PATH = os.path.join(
    BASE_DIR, '高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx')
RULES_PATH = os.path.join(BASE_DIR, 'data', 'mapping', 'metric_rules.json')
MERGED_SQL_PATH = os.path.join(BASE_DIR, 'data', 'merged_sql', 'merged_sql.json')

SHEET_NAME = '指标库-指标获取方法'
COL_CODE = '英文名'
COL_METHOD = '指标获取方法'
COL_SQL = '使用的SQL'
SQL_COLUMN_WIDTH = 60.0
METHOD_SQL = '命令中心SQL'


def load_json(path):
    with open(path, 'r') as fp:
        return json.load(fp)


def find_header_column(ws, header):
    """按表头文字找列号（1 基），找不到返回 None。"""
    for cell in ws[1]:
        if cell.value == header:
            return cell.column
    return None


def collect_sql_by_metric(rules, merged):
    """返回 {指标英文名: SQL 文本}，只含命令中心 SQL 通道的指标。"""
    mapping = {}
    for rule in rules:
        source = rule.get('source') or {}
        if source.get('method') != METHOD_SQL:
            continue
        sql_key = source.get('sql_key')
        batch = merged.get(sql_key)
        if batch is None:
            raise KeyError('metric_rules 引用了不存在的 sql_key: %s' % sql_key)
        mapping[rule['metric_code']] = batch['sql']
    return mapping


def style_header_cell(cell, template):
    cell.font = copy.copy(template.font)
    cell.fill = copy.copy(template.fill)
    cell.border = copy.copy(template.border)
    cell.alignment = copy.copy(template.alignment)


def style_value_cell(cell, template):
    cell.font = copy.copy(template.font)
    cell.border = copy.copy(template.border)
    alignment = copy.copy(template.alignment)
    alignment.vertical = 'top'
    alignment.wrap_text = True
    cell.alignment = alignment


def main():
    rules = load_json(RULES_PATH)['rules']
    merged = load_json(MERGED_SQL_PATH)['batches']
    merged_by_key = dict((b['sql_key'], b) for b in merged)
    sql_by_metric = collect_sql_by_metric(rules, merged_by_key)

    wb = openpyxl.load_workbook(XLSX_PATH)
    ws = wb[SHEET_NAME]

    col_code = find_header_column(ws, COL_CODE)
    col_method = find_header_column(ws, COL_METHOD)
    if col_code is None or col_method is None:
        raise KeyError('sheet %s 缺少表头 %s / %s' % (SHEET_NAME, COL_CODE, COL_METHOD))

    col_sql = find_header_column(ws, COL_SQL)
    if col_sql is None:
        col_sql = ws.max_column + 1
        header_cell = ws.cell(row=1, column=col_sql)
        header_cell.value = COL_SQL
        style_header_cell(header_cell, ws.cell(row=1, column=col_method))
        ws.column_dimensions[
            openpyxl.utils.get_column_letter(col_sql)].width = SQL_COLUMN_WIDTH

    filled = 0
    empty = 0
    missing = []
    for row in range(2, ws.max_row + 1):
        code = ws.cell(row=row, column=col_code).value
        method = ws.cell(row=row, column=col_method).value
        cell = ws.cell(row=row, column=col_sql)
        if method == METHOD_SQL and code in sql_by_metric:
            cell.value = sql_by_metric[code]
            filled += 1
        else:
            cell.value = None
            empty += 1
            if method == METHOD_SQL:
                missing.append(code)
        style_value_cell(cell, ws.cell(row=row, column=col_method))

    if missing:
        raise KeyError('以下命令中心指标在 metric_rules.json 里找不到 SQL: %s' % missing)

    # 自动筛选范围跟着扩一列
    last_col = openpyxl.utils.get_column_letter(ws.max_column)
    ws.auto_filter.ref = 'A1:%s%d' % (last_col, ws.max_row)

    # 先写临时文件再替换，避免中途失败把客户文档写坏
    tmp_dir = tempfile.mkdtemp()
    tmp_path = os.path.join(tmp_dir, os.path.basename(XLSX_PATH))
    try:
        wb.save(tmp_path)
        shutil.copyfile(tmp_path, XLSX_PATH)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    used_keys = sorted(set(
        (rule.get('source') or {}).get('sql_key')
        for rule in rules
        if (rule.get('source') or {}).get('method') == METHOD_SQL))
    print('已回填 %d 个命令中心 SQL 指标，其余 %d 个指标该列留空' % (filled, empty))
    print('涉及 %d 条合并 SQL: %s' % (len(used_keys), ', '.join(used_keys)))
    print('输出: %s' % XLSX_PATH)


if __name__ == '__main__':
    main()
