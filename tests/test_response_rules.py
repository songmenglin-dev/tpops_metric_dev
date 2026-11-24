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
import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Mapping

import pytest

from conftest import MAPPING_DIR

from conftest import DATA_DIR, REPO_ROOT

sys.path.insert(0, os.path.join(REPO_ROOT, 'tools'))
import build_response_rules  # noqa: E402

from tpops_kafka_collector.mapping.columns import (
    merged_archive_view_columns,
    source_column_problems,
)
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
        if rule_type == 'ratio_across':
            # 跨视图比率：分子与分母来自**不同视图**（activity_agg / stat_database），
            # 单视图留档里根本没有另一侧 —— 分母取不到时按口径「不产出」
            # （M-3：不静默出 0），这两条规则的值由合并留档的期望值表守
            # （tests/test_expected_values.py 覆盖它们）。
            assert not ctx.rows_by_key, rule['metric_code']
            assert not results, rule['metric_code']
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
    current = _load(RULES_PATH)
    fresh = build_response_rules.build()
    assert current == fresh, 'response_rules.json 过期，请运行 tools/build_response_rules.py'


# -- 合并形态的列校验（实测留档为准）--------------------------------------
#
# **这个文件守的是「映射表与真实数据脱节」**：合并形态的视图名与来源列必须
# 能在实测留档（``data/merged_sql/sql_merge_probe_result.json``）里对上。
# 对不上时生成器必须**报错退出**，不能静默产出一份取不到列的映射表。

MERGED_PROBE_PATH = os.path.join(DATA_DIR, 'merged_sql', 'sql_merge_probe_result.json')

# 子进程里跑生成器：留档路径与输出路径指到临时目录，退出码才是真的退出码。
_GENERATOR_WRAPPER = '''\
import sys

sys.path.insert(0, %(tools_dir)r)
import build_response_rules as g

g.MERGED_PROBE_PATH = %(probe_path)r
g.OUTPUT_PATH = %(output_path)r

try:
    sys.exit(g.main(sys.argv[1:]))
except g.BuildError as exc:
    print('生成失败：' + str(exc))
    sys.exit(2)
'''


def _run_generator(tmp_path, probe_payload):
    """把留档换成给定的内容，跑一遍生成器，返回 (returncode, stdout+stderr)。

    用子进程跑（退出码才是真的退出码），并把留档路径与输出路径指到临时目录 ——
    不碰仓库里的文件。
    """
    probe_path = tmp_path / 'sql_merge_probe_result.json'
    with io.open(probe_path, 'w', encoding='utf-8') as fp:
        json.dump(probe_payload, fp, ensure_ascii=False)
    wrapper = tmp_path / 'run_generator.py'
    wrapper.write_text(_GENERATOR_WRAPPER % {
        'tools_dir': os.path.join(REPO_ROOT, 'tools'),
        'probe_path': str(probe_path),
        'output_path': str(tmp_path / 'out.json'),
    }, encoding='utf-8')
    process = subprocess.run(
        [sys.executable, str(wrapper)],
        capture_output=True,
        text=True,
    )
    return process.returncode, process.stdout + process.stderr


def test_generator_rejects_a_bogus_view_name(tmp_path):
    """留档里多出一个视图 → 生成器报错退出（不静默产出错的映射表）。"""
    payload = _load(MERGED_PROBE_PATH)
    node_name = sorted(payload['nodes'])[0]
    row = json.loads(payload['nodes'][node_name]['result'])[0]
    row['no_such_view'] = '[]'
    payload['nodes'][node_name]['result'] = json.dumps([row])

    returncode, output = _run_generator(tmp_path, payload)
    assert returncode != 0, output
    assert 'no_such_view' in output


def test_generator_rejects_a_bogus_source_column(tmp_path):
    """规则要取的列不在留档的实际列里 → 生成器报错退出。"""
    payload = _load(MERGED_PROBE_PATH)
    for node_name in payload['nodes']:
        node = payload['nodes'][node_name]
        row = json.loads(node['result'])[0]
        rows = json.loads(row['stat_database'])
        for item in rows:
            item.pop('numbackends', None)
        row['stat_database'] = json.dumps(rows)
        node['result'] = json.dumps([row])

    returncode, output = _run_generator(tmp_path, payload)
    assert returncode != 0, output
    assert 'numbackends' in output


def test_generator_tolerates_a_view_empty_on_one_node(tmp_path):
    """某个节点这一轮没有行 → 不算「列不存在」。

    列的判定取各节点的**并集**：一个视图在某个 master 上没有命中行，
    是正常现象（那台机器上没有锁、没有长事务），不代表这个列不存在。
    """
    payload = _load(MERGED_PROBE_PATH)
    node_name = sorted(payload['nodes'])[0]
    row = json.loads(payload['nodes'][node_name]['result'])[0]
    row['stat_database'] = '[]'
    payload['nodes'][node_name]['result'] = json.dumps([row])

    returncode, output = _run_generator(tmp_path, payload)
    assert returncode == 0, output


def test_generator_accepts_the_real_archive():
    """真实留档必须通过校验（否则上面两条测试就是「一律失败」的假阳性）。"""
    payload = _load(MERGED_PROBE_PATH)
    columns = merged_archive_view_columns(payload)
    rules = _load(os.path.join(DATA_DIR, 'mapping', 'metric_rules.json'))['rules']
    assert not source_column_problems(rules, columns)


def test_mapping_table_load_validates_source_columns():
    """加载映射表时按 §5.6.4 校验来源列（实测留档为准）。

    对不上就抛 ``MappingError``（不可重试 → 启动失败）。
    """
    from tpops_kafka_collector.errors import MappingError
    from tpops_kafka_collector.mapping import MappingTable

    # 真实留档 → 校验通过
    MappingTable.load(MAPPING_DIR)

    # 留档里少一列 → 加载即失败
    payload = _load(MERGED_PROBE_PATH)
    for node in payload['nodes'].values():
        row = json.loads(node['result'])[0]
        rows = json.loads(row['stat_database'])
        for item in rows:
            item.pop('numbackends', None)
        row['stat_database'] = json.dumps(rows)
        node['result'] = json.dumps([row])

    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'probe.json')
        with io.open(path, 'w', encoding='utf-8') as fp:
            json.dump(payload, fp, ensure_ascii=False)
        with pytest.raises(MappingError) as excinfo:
            MappingTable.load(MAPPING_DIR, probe_path=path)
    assert 'numbackends' in str(excinfo.value)


def test_no_production_code_references_the_deleted_archive():
    """``data/command_center/results/`` 已于 2026-11-13 删除 —— 代码里不得再引用。

    只看**字符串常量**（文档字符串里的历史说明不算）—— 一旦代码真去读那个
    目录，启动期的来源列校验就会因为「留档不在」而失效。
    """
    import ast

    package_dir = os.path.join(REPO_ROOT, 'tpops_kafka_collector')
    offenders = []
    for root, dirs, files in os.walk(package_dir):
        dirs[:] = [d for d in dirs if d != '__pycache__']
        for name in files:
            if not name.endswith('.py'):
                continue
            path = os.path.join(root, name)
            with io.open(path, encoding='utf-8') as fp:
                tree = ast.parse(fp.read(), filename=path)
            docstrings = set()
            for node in ast.walk(tree):
                if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef)):
                    doc = ast.get_docstring(node, clean=False)
                    if doc:
                        docstrings.add(doc)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                    continue
                if node.value in docstrings:
                    continue
                if 'command_center/results' in node.value:
                    offenders.append(
                        '%s:%s' % (os.path.relpath(path, REPO_ROOT), node.lineno)
                    )
    assert not offenders, offenders
