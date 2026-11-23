"""B 类（命令中心 SQL）82 个指标的**值相等**回归。

**这个文件守的是什么**：合并 SQL 返回形态下，取值口径错了**不报错**。
列名对、聚合口径错（``aggregate`` 的 ``fn`` 写错、``raw`` 的 ``row_policy``
写错、``counter_delta`` 的差分基线算错），产出的就是一个「看起来合法的 0」。
列存在性校验挡不住这类缺陷 —— 只有**值相等**能挡。

**为什么是回归而不是单元测试**：期望值是**从实测留档推出来的**，
判据是「留档回放 → 规则引擎 → 与期望值表逐条相等」。
把留档换成夹具（冻结样本）之后，这条链就是**可重复**的：
留档重新抓取不会让回归跟着变绿。

**期望值从哪来**：
- B 类 82 条 → ``tests/fixtures/expected_values.json``
  （wave-0 的那一张，逐字节等于 ``data/mapping/expected_values.json``），
  由 ``tools/expected_deriver.py`` 从留档原始行**独立推导**，不经过规则引擎；
- 本文件断言的**被测对象**是 ``tpops_kafka_collector/mapping/rules.py``。

两边是两套实现：引擎把 ``state_contains`` 理解错了，这里就会红。

**覆盖**：82 条 B 类指标**逐条**断言（一条不落），累计计数器另加第 2 轮差分断言。
"""
import json

import pytest

from tpops_kafka_collector.mapping import evaluate
from tpops_kafka_collector.mapping.decode import decode_merged
from tpops_kafka_collector.mapping.rules import CounterDeltaStore
from tpops_kafka_collector.mapping.table import MetricRule

#: B 类指标的条数（一条不落）。
EXPECTED_B_COUNT = 82

#: 累计计数器所在的视图（第 2 轮合成留档只改这个视图）。
COUNTER_VIEW = 'stat_database'


def _pairs(results):
    """引擎输出 → ``[(维度, 值)]``（时间戳不参与比较）。"""
    return [(dims, value) for dims, value, _timestamp in results]


def _expected_pairs(node_expected):
    return [(item['dimensions'], item['value']) for item in node_expected]


def _engine_rule(rule_payload):
    """规则引擎要的对象（用生产包里的规则声明，不自己拼）。"""
    return MetricRule.from_dict(rule_payload)


def _evaluate(rule_payload, ctx):
    """执行规则引擎（``counter_delta`` 必须给一个差分基线存储）。"""
    engine_rule = _engine_rule(rule_payload)
    if rule_payload['value_rule']['type'] == 'counter_delta':
        return evaluate(engine_rule, ctx, CounterDeltaStore())
    return evaluate(engine_rule, ctx)


def _b_rules():
    """82 条 B 类指标的声明（**包内**那份 —— 生产用的声明）。"""
    from regression.conftest import package_mapping

    payload = package_mapping('metric_rules.json')
    return [
        rule for rule in payload['rules']
        if (rule.get('source') or {}).get('source') == 'command_center_sql'
    ]


#: 82 条 B 类指标的编码（有序）—— 收集期就要定下来（``parametrize`` 不能用夹具）。
B_CODES = sorted(rule['metric_code'] for rule in _b_rules())

#: 累计计数器的 5 条。
COUNTER_CODES = sorted(
    rule['metric_code'] for rule in _b_rules()
    if rule['value_rule']['type'] == 'counter_delta'
)


# -- 覆盖完整性 ------------------------------------------------------------

def test_b_class_rule_count_is_82(b_class_rules):
    """B 类指标就是 82 条 —— 少一条就是少一条回归覆盖。"""
    assert len(b_class_rules) == EXPECTED_B_COUNT


def test_expected_table_covers_every_b_class_metric(expected_table):
    """期望值表**一条不落**地覆盖 82 条 B 类指标。"""
    assert sorted(expected_table['expected']) == B_CODES


# -- 值相等：82 条逐条断言 -----------------------------------------------

@pytest.mark.parametrize('metric_code', B_CODES)
def test_rule_engine_output_equals_expected_values(
    metric_code, expected_table, probe_nodes, merged_views, b_class_rules
):
    """规则引擎的输出必须**逐节点**等于期望值表。

    期望值由 ``tools/expected_deriver.py`` 从留档原始行独立推出，
    与被测的规则引擎是两套实现 —— 引擎把 ``state_contains`` 理解错了，
    这里就会红。
    """
    rule_payload = next(
        r for r in b_class_rules if r['metric_code'] == metric_code
    )
    entry = expected_table['expected'][metric_code]
    sql_key = entry['sql_key']
    now_ms = expected_table['counter_delta_plan']['round1_now_ms']

    for node, node_expected in sorted(entry['round1'].items()):
        merged = decode_merged(
            {'result': probe_nodes[node]['result']}, merged_views, now_ms=now_ms
        )
        ctx = merged.by_sql_key[sql_key]
        results = _evaluate(rule_payload, ctx)
        assert _pairs(results) == _expected_pairs(node_expected), (metric_code, node)


# -- 累计计数器：首轮不产出值、第 2 轮差分速率 ---------------------------

def test_counter_delta_rules_are_five():
    assert len(COUNTER_CODES) == 5


@pytest.mark.parametrize('metric_code', COUNTER_CODES)
def test_counter_delta_first_round_has_no_baseline(
    metric_code, expected_table, probe_nodes, merged_views, b_class_rules
):
    """首轮没有基线 → **不产出值**（不是 0，也不是假数据）。"""
    rule_payload = next(
        r for r in b_class_rules if r['metric_code'] == metric_code
    )
    entry = expected_table['expected'][metric_code]
    now_ms = expected_table['counter_delta_plan']['round1_now_ms']

    for node, node_expected in sorted(entry['round1'].items()):
        merged = decode_merged(
            {'result': probe_nodes[node]['result']}, merged_views, now_ms=now_ms
        )
        results = _evaluate(rule_payload, merged.by_sql_key[entry['sql_key']])
        pairs = _pairs(results)
        assert pairs, (metric_code, node)
        for _dims, value in pairs:
            assert value is None, (metric_code, node, value)
        assert pairs == _expected_pairs(node_expected), (metric_code, node)


@pytest.mark.parametrize('metric_code', COUNTER_CODES)
def test_counter_delta_second_round_rate_equals_expected(
    metric_code, expected_table, probe_nodes, merged_views, b_class_rules
):
    """第 2 轮（合成留档：累计计数加量）→ 差分速率 == 期望速率。"""
    rule_payload = next(
        r for r in b_class_rules if r['metric_code'] == metric_code
    )
    entry = expected_table['expected'][metric_code]
    sql_key = entry['sql_key']
    plan = expected_table['counter_delta_plan']
    deltas = plan['deltas']
    elapsed = plan['elapsed_seconds']
    column = rule_payload['value_rule']['params']['column']
    expected_rate = deltas[column] / elapsed

    for node, node_expected in sorted(entry['round2'].items()):
        store = CounterDeltaStore()
        first = decode_merged(
            {'result': probe_nodes[node]['result']},
            merged_views,
            now_ms=plan['round1_now_ms'],
        )
        # 首轮：只建基线
        evaluate(_engine_rule(rule_payload), first.by_sql_key[sql_key], store)
        # 第 2 轮：合成留档（计数按计划加量）
        second = decode_merged(
            _synthetic_round2(probe_nodes[node]['result'], deltas),
            merged_views,
            now_ms=plan['round2_now_ms'],
        )
        results = evaluate(_engine_rule(rule_payload), second.by_sql_key[sql_key], store)
        pairs = _pairs(results)
        assert pairs == _expected_pairs(node_expected), (metric_code, node)
        for _dims, value in pairs:
            assert value is not None, (metric_code, node)
            assert value == pytest.approx(expected_rate), (metric_code, node, value)


def _synthetic_round2(node_result, deltas):
    """构造第 2 轮留档：累计计数列按计划加量，其余原样。"""
    row = json.loads(node_result)[0]
    bumped = []
    for item in json.loads(row[COUNTER_VIEW]):
        new_item = dict(item)
        for column, delta in deltas.items():
            value = new_item.get(column)
            if value is not None:
                new_item[column] = value + delta
        bumped.append(new_item)
    new_row = dict(row)
    new_row[COUNTER_VIEW] = json.dumps(bumped)
    return {'result': json.dumps([new_row])}
