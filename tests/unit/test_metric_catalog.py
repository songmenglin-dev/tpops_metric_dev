# -*- coding: utf-8 -*-
"""生效口径的三道启动期校验（排除清单 / 唯一性 / 条数）。

守住的口径（``changes/tpops-mapping-and-logging/specs/mapping-and-rules/spec.md``）：

- **排除清单的指标码一个都不在生效映射表里** —— 排除是**构建期**做的；
- **生效规则的 ``metric_code`` 互不重复**；
- **生效规则数**与排除清单声明的生效条数一致（客户指标库 101 − 排除 14 = 87）。

每道校验都要有**正例**与**反例**：反例是人为把生效映射表改坏，
校验必须失败并把差异打印出来（不满足即启动失败）。
"""
from __future__ import absolute_import

import io
import json
import os
import shutil

import pytest

from conftest import REPO_ROOT

from tpops_kafka_collector.errors import MappingError  # noqa: E402
from tpops_kafka_collector.mapping import loader  # noqa: E402

PACKAGE_DATA_DIR = os.path.join(
    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data')


def _read(path):
    with io.open(path, encoding='utf-8') as handle:
        return handle.read()


def _write(path, payload):
    with io.open(path, 'w', encoding='utf-8') as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, indent=2) + '\n')


@pytest.fixture()
def sandbox(tmp_path):
    """包内映射数据的一份可改副本。"""
    target = tmp_path / 'mapping_data'
    shutil.copytree(PACKAGE_DATA_DIR, str(target))
    return str(target)


def _mutate_rules(sandbox, mutator):
    path = os.path.join(sandbox, 'metric_rules.json')
    payload = json.loads(_read(path))
    mutator(payload)
    _write(path, payload)
    return path


def _problems(sandbox):
    """跑校验并返回问题清单（不抛异常）。"""
    try:
        loader.load_mapping(data_dir=sandbox)
    except MappingError as exc:
        return str(exc)
    return ''


# -- 正例 -----------------------------------------------------------------
def test_the_real_catalog_passes_validation():
    bundle = loader.load_mapping()
    assert len(bundle.metric_rules) == 87
    assert len(bundle.excluded_metrics) == 14


def test_excluded_metrics_are_loaded_from_the_data_file():
    bundle = loader.load_mapping()
    codes = set(item.metric_code for item in bundle.excluded_metrics)
    assert 'GAUSSDB.{#NAME}.garbage.Ratio' in codes
    assert bundle.excluded_counts['effective_total'] == 87


# -- 反例 1：被排除的码出现在生效映射表里 ---------------------------------
def test_an_excluded_code_in_the_effective_mapping_fails_validation(sandbox):
    def _inject(payload):
        rule = dict(payload['rules'][0])
        rule['metric_code'] = 'GAUSSDB.{#NAME}.garbage.Ratio'
        rule['metric_name'] = '({#NAME})Garbage ratio'
        payload['rules'].append(rule)

    _mutate_rules(sandbox, _inject)
    message = _problems(sandbox)
    assert message, '塞进一条被排除的指标码，校验居然通过了'
    assert 'GAUSSDB.{#NAME}.garbage.Ratio' in message
    assert '排除清单' in message


# -- 反例 2：metric_code 重复 ---------------------------------------------
def test_a_duplicated_metric_code_fails_validation(sandbox):
    def _duplicate(payload):
        payload['rules'].append(dict(payload['rules'][0]))

    _mutate_rules(sandbox, _duplicate)
    message = _problems(sandbox)
    assert message, '复制一条规则，校验居然通过了'
    assert 'metric_code' in message
    assert '重复' in message


# -- 反例 3：生效条数不对 ---------------------------------------------------
def test_a_wrong_rule_count_fails_validation(sandbox):
    def _drop(payload):
        payload['rules'] = payload['rules'][:-1]

    _mutate_rules(sandbox, _drop)
    message = _problems(sandbox)
    assert message, '少一条规则，校验居然通过了'
    assert '生效指标规则条数不对' in message
    assert '87' in message and '86' in message


# -- 告警定义不受唯一性约束 -----------------------------------------------
def test_alarm_definitions_do_not_have_to_have_unique_metric_codes(sandbox):
    """告警定义靠 ``alarm_id`` 定位 —— 多条告警共用一个 ``metricCode`` 是允许的。

    事件类告警统一指标名之后，254 条告警的 ``metric_code`` 相同 ——
    唯一性校验**只对指标规则**要求，告警侧不得因此报错。
    """
    path = os.path.join(sandbox, 'alarm_rules.json')
    payload = json.loads(_read(path))
    definitions = payload.get('definitions') or {}
    assert len(definitions) >= 2
    shared = 'gaussdb.alarm'
    for entry in definitions.values():
        entry['metric_code'] = shared
    _write(path, payload)

    bundle = loader.load_mapping(data_dir=sandbox)
    codes = set(
        definition['metric_code'] for definition in definitions.values()
    )
    assert codes == {shared}
    assert len(bundle.metric_rules) == 87


# -- 表级规则一律按两列分组（任务 1.5）------------------------------------
def _table_rules():
    payload = json.loads(_read(
        os.path.join(PACKAGE_DATA_DIR, 'metric_rules.json')))
    return [
        rule for rule in payload['rules']
        if 'NAME' in (rule.get('placeholders') or [])
    ]


def test_table_rules_bind_the_name_to_schema_and_relname():
    table_rules = _table_rules()
    assert len(table_rules) == 11, '表级规则数不对：%d' % len(table_rules)
    for rule in table_rules:
        assert rule['dimension_bindings'] == {'NAME': 'schemaname.relname'}, rule['metric_code']


def test_table_rules_group_by_both_columns():
    for rule in _table_rules():
        params = (rule.get('value_rule') or {}).get('params') or {}
        assert params.get('group_by') == ['schemaname', 'relname'], rule['metric_code']


def test_no_rule_binds_a_dotted_column_as_a_single_column():
    """点号串只出现在维度绑定里，``value_rule.params.group_by`` 必须是拆开的列。"""
    payload = json.loads(_read(
        os.path.join(PACKAGE_DATA_DIR, 'metric_rules.json')))
    for rule in payload['rules']:
        params = (rule.get('value_rule') or {}).get('params') or {}
        for column in params.get('group_by') or ():
            assert '.' not in column, (rule['metric_code'], column)
