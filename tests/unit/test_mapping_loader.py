# -*- coding: utf-8 -*-
"""映射表加载与启动期四类校验。

守住三条线：

1. **运行时不需要仓库根目录** —— 加载过程中打开的文件全部在包内；
2. **包内声明 == 从实测留档现算的结果** —— 防漂移（``observed_columns.json``）；
3. **校验失败要 fail loud** —— 抛带上下文的 ``MappingError``，不静默跳过。

映射数据从仓库复制进包（设计决策 9），包内副本与仓库原文件逐字节一致。
"""
from __future__ import absolute_import

import builtins
import io
import json
import os
import shutil
import sys

import pytest

from conftest import REPO_ROOT

sys.path.insert(0, os.path.join(REPO_ROOT, 'tools'))
import build_observed_columns  # noqa: E402

from tpops_kafka_collector.errors import MappingError, ProtocolError  # noqa: E402
from tpops_kafka_collector.mapping import loader  # noqa: E402

PACKAGE_DIR = os.path.join(REPO_ROOT, 'tpops_kafka_collector')
DATA_DIR = os.path.join(PACKAGE_DIR, 'mapping', 'data')

# 包内副本 ← 仓库原文件。observed_columns.json 不在此列 ——
# 它是从留档生成的，不是仓库文件的副本。
COPIED_FILES = {
    'metric_rules.json': os.path.join('data', 'mapping', 'metric_rules.json'),
    'excluded_metrics.json': os.path.join('data', 'mapping', 'excluded_metrics.json'),
    'alarm_rules.json': os.path.join('data', 'mapping', 'alarm_rules.json'),
    'itscada_field_map.json': os.path.join('data', 'mapping', 'itscada_field_map.json'),
    'kafka_field_mapping.json': os.path.join('data', 'mapping', 'kafka_field_mapping.json'),
    'response_rules.json': os.path.join('data', 'mapping', 'response_rules.json'),
    'open_items.json': os.path.join('data', 'mapping', 'open_items.json'),
    'customer_metric_mapping.json': os.path.join('data', 'customer_metric_mapping.json'),
}


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
    shutil.copytree(DATA_DIR, str(target))
    return str(target)


def _mutate(sandbox, filename, mutator):
    path = os.path.join(sandbox, filename)
    payload = json.loads(_read(path))
    mutator(payload)
    _write(path, payload)
    return path


# -- 加载 -----------------------------------------------------------------
def test_load_mapping_reads_the_real_tables():
    bundle = loader.load_mapping()
    assert len(bundle.metric_rules) == 87
    assert len(bundle.customer_metrics) == 87
    assert len(bundle.alarm_rules['definitions']) == 257
    assert len(bundle.observed.merged_views) == 11


def test_load_mapping_never_touches_the_repo_root(monkeypatch, tmp_path):
    """运行时不需要仓库根目录 —— 打开的文件必须全在包内。"""
    opened = []
    real_open = builtins.open

    def _spy(path, *args, **kwargs):
        opened.append(os.path.abspath(str(path)))
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, 'open', _spy)
    monkeypatch.chdir(str(tmp_path))
    loader.load_mapping()

    assert opened, '一次文件都没打开，加载器没干活？'
    outside = [path for path in opened if not path.startswith(os.path.abspath(DATA_DIR))]
    assert not outside, '加载器读了包外的文件：%s' % outside


def test_load_mapping_reports_a_missing_file(sandbox):
    os.remove(os.path.join(sandbox, 'metric_rules.json'))
    with pytest.raises(MappingError) as excinfo:
        loader.load_mapping(data_dir=sandbox)
    assert '映射文件不存在' in str(excinfo.value)


def test_load_mapping_reports_broken_json(sandbox):
    with io.open(os.path.join(sandbox, 'metric_rules.json'), 'w', encoding='utf-8') as handle:
        handle.write('{ 这不是 JSON')
    with pytest.raises(MappingError) as excinfo:
        loader.load_mapping(data_dir=sandbox)
    assert '不是合法 JSON' in str(excinfo.value)


def test_load_mapping_reports_a_broken_table(sandbox):
    _mutate(sandbox, 'metric_rules.json', lambda payload: payload.pop('rules'))
    with pytest.raises(ProtocolError) as excinfo:
        loader.load_mapping(data_dir=sandbox)
    assert 'rules' in str(excinfo.value)


# -- 防漂移 ---------------------------------------------------------------
def test_package_copies_are_identical_to_the_repo_files():
    """包内副本与仓库原文件逐字节一致（设计决策 9）。"""
    for filename, relative in sorted(COPIED_FILES.items()):
        repo_path = os.path.join(REPO_ROOT, relative)
        package_path = os.path.join(DATA_DIR, filename)
        assert os.path.isfile(package_path), '包内缺少 %s' % filename
        assert _read(package_path) == _read(repo_path), (
            '%s 与仓库副本 %s 不一致 —— 重新复制或重跑生成器' % (filename, relative)
        )


def test_observed_columns_match_the_generator_output():
    """包内声明 == 从实测留档现算的结果（防漂移）。"""
    expected = build_observed_columns.render(build_observed_columns.build())
    actual = _read(os.path.join(DATA_DIR, 'observed_columns.json'))
    assert actual == expected, (
        'observed_columns.json 与生成器输出不一致，'
        '请重新运行 tools/build_observed_columns.py'
    )


def test_observed_columns_generator_is_in_sync():
    """生成器的 --check 模式必须通过（防漂移的第二道闸）。"""
    assert build_observed_columns.main(['--check']) == 0


# -- 四类校验 -------------------------------------------------------------
def test_all_checks_pass_on_the_real_tables():
    bundle = loader.load_mapping()
    assert loader.check_source_columns(bundle.metric_rules, bundle.observed) == []
    assert loader.check_providable_coverage(
        bundle.metric_rules, bundle.customer_metrics) == []
    assert loader.check_rule_entries(bundle.metric_rules, bundle.customer_metrics) == []
    assert loader.check_placeholder_bindings(bundle.metric_rules) == []


def test_missing_providable_metric_fails_loudly(sandbox):
    """可提供指标缺失 → 启动失败，报出指标名。"""
    def _drop(payload):
        payload['rules'] = [
            rule for rule in payload['rules']
            if rule['metric_code'] != 'GAUSSDB.Total_slow_queries'
        ]

    _mutate(sandbox, 'metric_rules.json', _drop)
    with pytest.raises(MappingError) as excinfo:
        loader.load_mapping(data_dir=sandbox)
    message = str(excinfo.value)
    assert 'GAUSSDB.Total_slow_queries' in message
    assert '没有启用的规则' in message


def test_entry_without_a_real_metric_fails_loudly(sandbox):
    """映射表里出现客户指标库里没有的条目 → 启动失败。"""

    def _add(payload):
        template = payload['rules'][0]
        bogus = dict(template)
        bogus['metric_code'] = 'GAUSSDB.Not_A_Real_Metric'
        payload['rules'].append(bogus)

    _mutate(sandbox, 'metric_rules.json', _add)
    with pytest.raises(MappingError) as excinfo:
        loader.load_mapping(data_dir=sandbox)
    message = str(excinfo.value)
    assert 'GAUSSDB.Not_A_Real_Metric' in message
    assert '在客户指标库里不存在' in message


def test_placeholder_binding_mismatch_fails_loudly(sandbox):
    """占位符与维度绑定不是一一对应 → 启动失败。"""

    def _break(payload):
        for rule in payload['rules']:
            if rule['metric_code'] == 'gaussdb.locks.get_metrics[{#DBNAME}]':
                rule['placeholders'] = []
                break
        else:
            raise AssertionError('没找到带占位符的规则')

    _mutate(sandbox, 'metric_rules.json', _break)
    with pytest.raises(MappingError) as excinfo:
        loader.load_mapping(data_dir=sandbox)
    message = str(excinfo.value)
    assert 'gaussdb.locks.get_metrics[{#DBNAME}]' in message
    assert '不是一一对应' in message


def test_unknown_source_column_fails_loudly(sandbox):
    """来源列不在实测留档的实际返回列里 → 启动失败，报出规则名与实际列。"""

    def _break(payload):
        for rule in payload['rules']:
            if rule.get('value_rule', {}).get('params', {}).get('column') == 'deadlocks':
                rule['value_rule']['params']['column'] = 'no_such_column'
                break
        else:
            raise AssertionError('没找到带 column 的命令中心规则')

    _mutate(sandbox, 'metric_rules.json', _break)
    with pytest.raises(MappingError) as excinfo:
        loader.load_mapping(data_dir=sandbox)
    message = str(excinfo.value)
    assert 'no_such_column' in message
    assert '不在实测留档视图' in message
    assert '实际列有' in message


def test_unknown_dimension_binding_column_fails_loudly(sandbox):
    """维度绑定引用的列不存在 → 启动失败。"""

    def _break(payload):
        for rule in payload['rules']:
            if rule['metric_code'] == 'gaussdb.{#NAME}.Number.lete.ples':
                rule['dimension_bindings'] = {'NAME': 'no_such_table_column'}
                break
        else:
            raise AssertionError('没找到带维度绑定的规则')

    _mutate(sandbox, 'metric_rules.json', _break)
    with pytest.raises(MappingError) as excinfo:
        loader.load_mapping(data_dir=sandbox)
    message = str(excinfo.value)
    assert 'no_such_table_column' in message
    assert '维度绑定' in message


def test_unknown_diagnostic_column_fails_loudly(sandbox):
    """诊断优化通道引用的集合列不存在 → 启动失败。"""

    def _break(payload):
        for rule in payload['rules']:
            if rule['metric_code'] == 'GAUSSDB.Select_slow_queries':
                rule['value_rule']['params']['field'] = 'no_such_sql_field'
                break
        else:
            raise AssertionError('没找到诊断优化规则')

    _mutate(sandbox, 'metric_rules.json', _break)
    with pytest.raises(MappingError) as excinfo:
        loader.load_mapping(data_dir=sandbox)
    message = str(excinfo.value)
    assert 'no_such_sql_field' in message
    assert '集合' in message
