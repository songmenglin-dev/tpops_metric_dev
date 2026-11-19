# -*- coding: utf-8 -*-
"""response_rules.json 的映射条件必须在实测 response_body 上真的成立。

这份测试是「映射条件」与「实测数据」之间唯一的连接点：
- 每个 enabled 规则都要能从留档里解出 RuleContext；
- 解出来的上下文交给规则引擎，要能产出至少一个非空值；
- 定位里写的列名，必须真的在实测响应体的列集合里。
"""
import io
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Mapping

import pytest

from conftest import DATA_DIR, REPO_ROOT

from tpops_kafka_collector.mapping.decode import DecodeError, decode
from tpops_kafka_collector.mapping.rules import CounterDeltaStore, evaluate

RESPONSE_DIR = os.path.join(DATA_DIR, 'metric_response')
RULES_PATH = os.path.join(DATA_DIR, 'mapping', 'response_rules.json')


def _load(path):
    with io.open(path, encoding='utf-8') as fp:
        return json.load(fp)


@pytest.fixture(scope='module')
def payload():
    return _load(RULES_PATH)


@pytest.fixture(scope='module')
def rules(payload):
    return payload['rules']


@dataclass(frozen=True)
class _RuleView:
    """规则引擎只认这几个字段，这里按 response_rules 的结构喂给它。"""

    metric_code: str
    value_rule: Mapping[str, Any]
    dimension_bindings: Mapping[str, str]


def _view(rule):
    business = rule['business']
    return _RuleView(
        metric_code=rule['metric_code'],
        value_rule=business['value_rule'],
        dimension_bindings=business['dimensions'],
    )


def _sample_body(rule):
    evidence = rule['evidence']
    steps = _load(os.path.join(RESPONSE_DIR, evidence['sample_file']))
    return steps[evidence['terminal_step']]['response_body']


def _resolve(rule, payload, now_ms=1760000000000):
    body = _sample_body(rule)
    shape = payload['shapes'][rule['shape']]
    return decode(body, shape, rule['locate'], now_ms=now_ms)


# -- 覆盖面 ---------------------------------------------------------------

def test_covers_all_enabled_metrics(rules):
    """98 个可提供指标各有一条规则；3 个无法提供的不建规则。"""
    metric_rules = _load(os.path.join(DATA_DIR, 'mapping', 'metric_rules.json'))['rules']
    enabled = set(r['metric_code'] for r in metric_rules if r.get('enabled'))
    assert len(rules) == 98
    assert set(r['metric_code'] for r in rules) == enabled


def test_every_shape_is_referenced(rules, payload):
    used = set(r['shape'] for r in rules)
    assert used == set(payload['shapes'].keys())


def test_every_rule_has_evidence(rules):
    for rule in rules:
        evidence = rule['evidence']
        assert os.path.exists(os.path.join(RESPONSE_DIR, evidence['sample_file']))
        assert isinstance(evidence['terminal_step'], int)


# -- 映射条件成立 ---------------------------------------------------------

def test_every_rule_decodes_from_real_response(rules, payload):
    """每条规则都要能从实测响应体解出上下文。"""
    failures = []
    for rule in rules:
        try:
            ctx = _resolve(rule, payload)
        except DecodeError as exc:
            failures.append('%s: %s' % (rule['metric_code'], exc))
            continue
        if rule['channel'] == 'command_center_sql':
            rule_type = rule['business']['value_rule']['type']
            assert ctx.rows or rule_type == 'top_n', rule['metric_code']
        elif rule['channel'] == 'monitor_metrics':
            assert ctx.series, rule['metric_code']
        elif rule['channel'] == 'diagnostic':
            assert ctx.collections, rule['metric_code']
        elif rule['channel'] == 'instance':
            assert ctx.instance, rule['metric_code']
    assert not failures, failures


def test_every_rule_produces_a_value(rules, payload):
    """解出来的上下文交给规则引擎，要产出至少一个非空值。

    counter_delta 首轮没有基线，按开发方案 §3.4 返回 null 是正确行为，
    这类规则改为校验"原始累计值取得到"。
    """
    failures = []
    unverified = []
    for rule in rules:
        ctx = _resolve(rule, payload)
        rule_type = rule['business']['value_rule'].get('type')
        deltas = CounterDeltaStore()
        results = evaluate(_view(rule), ctx, deltas)
        if rule.get('evidence', {}).get('verified_on_sample') is False:
            # 样本里就没有命中行（采集时刻不成立），这类规则必须被显式标记，
            # 而不是伪装成"验证通过"
            unverified.append(rule['metric_code'])
            continue
        if not results:
            failures.append('%s: 没有产出任何结果' % rule['metric_code'])
            continue
        values = [value for _dims, value, _ts in results]
        if rule_type == 'counter_delta':
            # 首轮无基线：值必为 null，但差分基线要建起来
            assert all(value is None for value in values), rule['metric_code']
            continue
        if all(value is None for value in values):
            failures.append('%s: 产出全是 null' % rule['metric_code'])
    assert not failures, failures
    # 未能在留档上验证的规则：过滤条件依赖采集时刻的会话状态/锁模式，
    # 采集时点上不成立。这批指标在生产里会"时有时无"，
    # 根因是客户端过滤了本就不存在的行（应在 SQL 里做条件聚合）。
    # 名单写死，新增一条就要有人来看一眼。
    assert set(unverified) == {
        'GAUSSDB.Idle_in_transaction_connections',
        'gaussdb.Idle_in_transaction',
        'gaussdb.Idle_in_transaction_aborted',
        'gaussdb.Lockwaiting',
        'gaussdb.Waiting_conn',
        'gaussdb.locks.accessexclusive[{#DBNAME}]',
    }, unverified


def test_unverified_rules_are_flagged_with_a_note(rules):
    """没在样本上验证过的规则，必须显式标记并写明原因。"""
    for rule in rules:
        evidence = rule['evidence']
        if evidence.get('verified_on_sample') is False:
            assert evidence.get('note'), rule['metric_code']
            assert rule['evidence']['observed']['matched_rows'] == 0, rule['metric_code']


def test_counter_delta_can_produce_rate_on_second_round(rules, payload):
    """第二轮有基线时，counter_delta 要能算出速率。"""
    counter_rules = [r for r in rules if r['business']['value_rule'].get('type') == 'counter_delta']
    assert counter_rules
    for rule in counter_rules:
        deltas = CounterDeltaStore()
        ctx = _resolve(rule, payload, now_ms=1760000000000)
        evaluate(_view(rule), ctx, deltas)
        ctx2 = _resolve(rule, payload, now_ms=1760000300000)
        results = evaluate(_view(rule), ctx2, deltas)
        assert any(value is not None for _dims, value, _ts in results), rule['metric_code']


def test_located_columns_exist_in_observed_response(rules):
    """定位里要取的列必须真实出现在实测响应的列集合里（§5.6.4）。"""
    problems = []
    for rule in rules:
        observed = rule['evidence'].get('observed', {})
        columns = observed.get('columns')
        if not columns:
            continue
        for key in ('column', 'value_column'):
            name = rule['locate'].get(key)
            if name and name not in columns:
                problems.append('%s: 列 %s 不在实测列 %s 里' % (rule['metric_code'], name, columns))
    assert not problems, problems


def test_ratio_across_sources_are_all_decoded(rules, payload):
    """跨 SQL 的比值指标，分子分母两个来源都要能解出来。"""
    ratio_rules = [r for r in rules if r['business']['value_rule'].get('type') == 'ratio_across']
    assert ratio_rules
    for rule in ratio_rules:
        params = rule['business']['value_rule']['params']
        for side in ('numerator', 'denominator'):
            shape_id = 'cmd_rows_%s' % params[side]['sql_key']
            assert shape_id in payload['shapes'], (rule['metric_code'], side, shape_id)


# -- 文件是最新的 ---------------------------------------------------------

def test_file_is_up_to_date():
    """文件必须与生成器输出一致（规则改了要重新生成）。"""
    sys.path.insert(0, os.path.join(REPO_ROOT, 'tools'))
    import build_response_rules

    current = _load(RULES_PATH)
    fresh = build_response_rules.build()
    assert current == fresh, 'response_rules.json 过期，请运行 tools/build_response_rules.py'
