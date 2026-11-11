# -*- coding: utf-8 -*-
"""规则文件必须与生成器保持一致（改了生成器要重新生成并提交）。"""
import io
import json
import os
import sys

from conftest import DATA_DIR, REPO_ROOT

sys.path.insert(0, os.path.join(REPO_ROOT, 'tools'))

import build_itscada_mapping as builder  # noqa: E402
import build_metric_value_rules as value_builder  # noqa: E402


def test_metric_value_rules_file_is_up_to_date(tmpdir):
    regenerated = value_builder._build_metrics(value_builder._read_plan())
    stored = json.load(
        io.open(os.path.join(DATA_DIR, 'metric_value_rules.json'), encoding='utf-8')
    )
    assert stored['metrics'] == regenerated
    assert stored['counts']['total'] == 98


def test_metric_rules_file_is_up_to_date():
    regenerated = builder.build_metric_rules()
    stored = json.load(
        io.open(builder.METRIC_RULES_OUT, encoding='utf-8')
    )
    assert regenerated == stored


def test_alarm_rules_file_is_up_to_date():
    regenerated = builder.build_alarm_rules()
    stored = json.load(
        io.open(builder.ALARM_RULES_OUT, encoding='utf-8')
    )
    assert regenerated == stored


def test_value_rules_cover_all_provided_metrics():
    """开发方案 §4.3 的 98 条规则与可提供性判定一一对应。"""
    availability = json.load(
        io.open(os.path.join(DATA_DIR, 'customer_metric_mapping.json'), encoding='utf-8')
    )
    provided = set(
        item['customer_metric'] for item in availability['items']
        if item['status'] == '可提供'
    )
    value_rules = json.load(
        io.open(os.path.join(DATA_DIR, 'metric_value_rules.json'), encoding='utf-8')
    )
    assert set(value_rules['metrics']) == provided
