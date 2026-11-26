# -*- coding: utf-8 -*-
"""排除清单（``excluded_metrics.json``）。

守住四件事：

1. **清单本身**：恰好 14 条（11 条 `duplicate` + 3 条 `unsupported`），
   必填字段齐全，`duplicate` 必带 ``kept``、`unsupported` 不带 ``kept``；
2. **两份副本一致**：包内（运行期真相源）与仓库根（同源副本）逐字节一致；
3. **清单与生效口径互斥**（任务 1.2 起）：被排除的指标码一个都不在生效映射表里，
   ``kept`` 指明的那一条**必须**在生效映射表里；
4. **生效口径的口径**（任务 1.2 起）：87 条规则、0 条 ``enabled=false``、
   ``metric_code`` 互不重复。
"""
from __future__ import absolute_import

import io
import json
import os

import pytest

from conftest import REPO_ROOT

PACKAGE_PATH = os.path.join(
    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'excluded_metrics.json')
REPO_COPY_PATH = os.path.join(
    REPO_ROOT, 'data', 'mapping', 'excluded_metrics.json')
METRIC_RULES_PATH = os.path.join(
    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'metric_rules.json')

#: 排除原因的合法取值。
REASONS = ('duplicate', 'unsupported')

#: 客户指标库里 11 组重复（英文名一条 vs 中文名一条）的「保留哪一条」。
#: 逐条写死 —— 这份清单是**人工判定**的结果，不能由代码推导。
EXPECTED_DUPLICATES = {
    'GAUSSDB.{#NAME}.Number.lete.ples': 'gaussdb.{#NAME}.Number.lete.ples',
    'GAUSSDB.{#NAME}.Number.ad.ples': 'gaussdb.{#NAME}.Number.ad.ples',
    'GAUSSDB.{#NAME}.Number.t.date.ples': 'gaussdb.{#NAME}.Number.t.date.ples',
    'GAUSSDB.{#NAME}.number.Of.index.scan': 'gaussdb.{#NAME}.number.Of.index.scan',
    'GAUSSDB.{#NAME}.number.Of.quential.ples.ad':
        'gaussdb.{#NAME}.number.Of.quential.ples.ad',
    'GAUSSDB.{#NAME}.number.Of.dex.ples.ad': 'gaussdb.{#NAME}.number.Of.dex.ples.ad',
    'GAUSSDB.{#NAME}.number.Of.insert.tuples': 'gaussdb.{#NAME}.number.Of.insert.tuples',
    'GAUSSDB.{#NAME}.number.Of.live.tuples': 'gaussdb.{#NAME}.number.Of.live.tuples',
    'GAUSSDB.{#NAME}.number.Of.update.tuples': 'gaussdb.{#NAME}.number.Of.update.tuples',
    'GAUSSDB.{#NAME}.number.Of.sequential.scan': 'gaussdb.{#NAME}.number.Of.sequential.scan',
    'GAUSSDB.{#NAME}.garbage.Ratio': 'gaussdb.{#NAME}.garbage.Ratio',
}

#: 3 条无法提供的指标（PG 侧没有对应的可查询来源）。
EXPECTED_UNSUPPORTED = {
    'gaussdb.Fastpath_function_call': '快路径函数调用的连接数',
    'gaussdb.Disabled_conn': '禁用的连接数',
    'gaussdb.sendAgentName': '发送设备名给Agent',
}


def _load(path):
    with io.open(path, encoding='utf-8') as handle:
        return json.load(handle)


@pytest.fixture(scope='module')
def excluded():
    return _load(PACKAGE_PATH)


@pytest.fixture(scope='module')
def entries(excluded):
    return excluded['excluded']


@pytest.fixture(scope='module')
def effective_codes():
    payload = _load(METRIC_RULES_PATH)
    return [rule['metric_code'] for rule in payload['rules']]


# -- 清单本身 -------------------------------------------------------------
def test_exclusion_list_has_exactly_fourteen_entries(entries):
    assert len(entries) == 14


def test_reason_distribution_is_eleven_duplicates_and_three_unsupported(entries):
    counts = {}
    for entry in entries:
        counts[entry['reason']] = counts.get(entry['reason'], 0) + 1
    assert counts == {'duplicate': 11, 'unsupported': 3}


def test_every_entry_has_the_required_fields(entries):
    for entry in entries:
        assert entry.get('metric_code'), entry
        assert entry.get('metric_name'), entry
        assert entry.get('reason') in REASONS, entry
        assert entry.get('detail'), entry


def test_duplicate_entries_carry_kept_and_unsupported_entries_do_not(entries):
    for entry in entries:
        if entry['reason'] == 'duplicate':
            assert entry.get('kept'), '重复项必须写明保留哪一条：%r' % entry
        else:
            assert 'kept' not in entry, '无法提供的指标没有「保留项」：%r' % entry


def test_metric_codes_are_unique(entries):
    codes = [entry['metric_code'] for entry in entries]
    assert len(codes) == len(set(codes))


def test_duplicate_entries_match_the_known_eleven_pairs(entries):
    actual = dict(
        (entry['metric_code'], entry['kept'])
        for entry in entries if entry['reason'] == 'duplicate'
    )
    assert actual == EXPECTED_DUPLICATES


def test_unsupported_entries_match_the_known_three(entries):
    actual = dict(
        (entry['metric_code'], entry['metric_name'])
        for entry in entries if entry['reason'] == 'unsupported'
    )
    assert actual == EXPECTED_UNSUPPORTED


def test_counts_section_matches_the_entries(excluded, entries):
    assert excluded['counts']['total'] == len(entries)
    assert excluded['counts']['duplicate'] == 11
    assert excluded['counts']['unsupported'] == 3


def test_counts_pin_the_effective_catalog_size(excluded):
    """排除条数与生效条数是**写死在一处**的口径：客户库 101 − 排除 14 = 生效 87。"""
    counts = excluded['counts']
    assert counts['client_total'] == 101
    assert counts['client_total'] - counts['total'] == counts['effective_total']
    assert counts['effective_total'] == 87


# -- 两份副本一致 ---------------------------------------------------------
def test_package_and_repo_copies_are_byte_identical():
    with open(PACKAGE_PATH, 'rb') as handle:
        package = handle.read()
    with open(REPO_COPY_PATH, 'rb') as handle:
        repo_copy = handle.read()
    assert package == repo_copy, '包内副本与仓库根同源副本不一致'


# -- 清单与生效口径互斥（任务 1.2）----------------------------------------
def test_no_excluded_code_is_in_the_effective_mapping(entries, effective_codes):
    excluded_codes = set(entry['metric_code'] for entry in entries)
    leaked = sorted(excluded_codes.intersection(effective_codes))
    assert not leaked, '被排除的指标码仍出现在生效映射表里：%s' % leaked


def test_kept_codes_are_in_the_effective_mapping(entries, effective_codes):
    for entry in entries:
        if entry['reason'] != 'duplicate':
            continue
        assert entry['kept'] in effective_codes, \
            '保留项 %r 不在生效映射表里' % entry['kept']


def test_kept_codes_are_not_excluded_themselves(entries):
    excluded_codes = set(entry['metric_code'] for entry in entries)
    for entry in entries:
        if entry['reason'] == 'duplicate':
            assert entry['kept'] not in excluded_codes, entry


# -- 生效口径（任务 1.2）--------------------------------------------------
def test_effective_mapping_has_exactly_eighty_seven_rules(effective_codes):
    assert len(effective_codes) == 87


def test_effective_mapping_has_no_disabled_rule():
    payload = _load(METRIC_RULES_PATH)
    disabled = [
        rule['metric_code'] for rule in payload['rules'] if not rule.get('enabled')
    ]
    assert not disabled, '生效口径里不该有 enabled=false 的规则：%s' % disabled


def test_effective_metric_codes_are_unique(effective_codes):
    duplicates = sorted(
        code for code in set(effective_codes)
        if effective_codes.count(code) > 1
    )
    assert not duplicates, '生效口径的 metric_code 有重复：%s' % duplicates
