"""B 类（命令中心 SQL）82 个指标的**值相等**守卫。

**这个文件守的是什么**：合并 SQL 返回形态下，取值口径错了**不报错**。
列名对、聚合口径错（例如 ``aggregate`` 的 ``fn`` 写错、``raw`` 的 ``row_policy``
写错、``counter_delta`` 的差分基线算错），产出的就是一个「看起来合法的 0」。
列存在性校验挡不住这类缺陷 —— 只有**值相等**能挡。

**期望值从哪来**：``data/mapping/expected_values.json``（生成物），
由 ``tools/build_expected_values.py`` 从**实测留档的原始行**独立推出
（推导器在 ``tools/expected_deriver.py``，**不经过规则引擎**）。
所以两边是两套实现：一边是本文件断言的**被测对象**
（``tpops_kafka_collector/mapping/rules.py``），一边是独立推导的期望值。

覆盖：
- 82 条 B 类指标**逐条**断言「规则引擎输出 == 期望值表」（一条不落）；
- 期望值表与 82 条 B 类指标**完全对应**（不多不少）；
- 期望值表与生成器输出**一致**（改了规则要重新生成）；
- ``counter_delta`` 的 5 条：首轮无基线不产出值，第 2 轮差分速率 == 期望速率。
"""
import json
import os

import pytest

from conftest import DATA_DIR

from tpops_kafka_collector.mapping import evaluate
from tpops_kafka_collector.mapping.decode import decode_merged
from tpops_kafka_collector.mapping.rules import CounterDeltaStore, RuleContext
from tpops_kafka_collector.mapping.table import MetricRule

MAPPING_DIR = os.path.join(DATA_DIR, 'mapping')
METRIC_RULES_PATH = os.path.join(MAPPING_DIR, 'metric_rules.json')
RESPONSE_RULES_PATH = os.path.join(MAPPING_DIR, 'response_rules.json')
EXPECTED_PATH = os.path.join(MAPPING_DIR, 'expected_values.json')
PROBE_PATH = os.path.join(DATA_DIR, 'merged_sql', 'sql_merge_probe_result.json')

B_CLASS = 'command_center_sql'
EXPECTED_B_COUNT = 82
COUNTER_VIEW = 'stat_database'


def _load(path):
    with open(path, encoding='utf-8') as handle:
        return json.load(handle)


def _b_rules():
    """82 条 B 类指标（channel == command_center_sql）的声明。"""
    payload = _load(METRIC_RULES_PATH)
    return [
        rule for rule in payload['rules']
        if rule.get('source', {}).get('source') == B_CLASS
    ]


B_RULES = _b_rules()
B_CODES = sorted(rule['metric_code'] for rule in B_RULES)
COUNTER_CODES = sorted(
    rule['metric_code'] for rule in B_RULES
    if rule['value_rule']['type'] == 'counter_delta'
)


@pytest.fixture(scope='module')
def expected_table():
    return _load(EXPECTED_PATH)


@pytest.fixture(scope='module')
def probe_nodes():
    """实测留档里的每个 master 节点（一个节点 = 一次合并 SQL 的返回）。"""
    return _load(PROBE_PATH)['nodes']


@pytest.fixture(scope='module')
def merged_views():
    payload = _load(RESPONSE_RULES_PATH)
    views = payload.get('merged_views')
    assert views, 'response_rules.json 里没有 merged_views 段'
    return views


@pytest.fixture(scope='module')
def counter_plan(expected_table):
    return expected_table['counter_delta_plan']


def _engine_rule(rule_payload):
    """规则引擎要的对象（用生产包的规则声明，不自己拼）。"""
    return MetricRule.from_dict(rule_payload)


def _evaluate(engine_rule, ctx, rule_payload):
    """执行规则引擎。``counter_delta`` 必须给一个差分基线存储。"""
    if rule_payload['value_rule']['type'] == 'counter_delta':
        return evaluate(engine_rule, ctx, CounterDeltaStore())
    return evaluate(engine_rule, ctx)


def _pairs(results):
    """引擎输出 → ``[(维度, 值)]``（时间戳不参与比较）。"""
    return [(dims, value) for dims, value, _timestamp in results]


def _expected_pairs(node_expected):
    return [(item['dimensions'], item['value']) for item in node_expected]


# -- 期望值表本身的完整性 --------------------------------------------------

def test_b_class_metric_count_is_82():
    assert len(B_RULES) == EXPECTED_B_COUNT
    assert len(B_CODES) == EXPECTED_B_COUNT


def test_expected_table_covers_every_b_class_metric(expected_table):
    """期望值表必须**一条不落**地覆盖 82 条 B 类指标。"""
    expected = expected_table['expected']
    assert len(expected) == EXPECTED_B_COUNT
    assert sorted(expected) == B_CODES


def test_expected_table_has_no_extra_entries(expected_table):
    """表里不能有 B 类之外的指标（否则覆盖率是假的）。"""
    for code in expected_table['expected']:
        rule = next(r for r in B_RULES if r['metric_code'] == code)
        assert rule['source']['source'] == B_CLASS, code


def test_expected_table_records_the_rule_type(expected_table):
    """每条期望值都记着自己是怎么算出来的（便于排查差异）。"""
    for rule in B_RULES:
        entry = expected_table['expected'][rule['metric_code']]
        assert entry['value_rule_type'] == rule['value_rule']['type']
        assert entry['sql_key']


# -- 值相等：82 条逐条断言 ------------------------------------------------

@pytest.mark.parametrize('metric_code', B_CODES)
def test_rule_engine_output_equals_expected_values(
    metric_code, expected_table, probe_nodes, merged_views
):
    """规则引擎的输出必须**逐节点**等于期望值表。

    期望值由 ``tools/expected_deriver.py`` 从留档原始行独立推出，
    与被测的规则引擎是两套实现 —— 引擎把 ``state_contains`` 理解错了，
    这里就会红。
    """
    rule_payload = next(r for r in B_RULES if r['metric_code'] == metric_code)
    engine_rule = _engine_rule(rule_payload)
    entry = expected_table['expected'][metric_code]
    sql_key = entry['sql_key']
    now_ms = expected_table['counter_delta_plan']['round1_now_ms']

    for node, node_expected in sorted(entry['round1'].items()):
        merged = decode_merged(
            {'result': probe_nodes[node]['result']}, merged_views, now_ms=now_ms
        )
        ctx = merged.by_sql_key[sql_key]
        results = _evaluate(engine_rule, ctx, rule_payload)
        assert _pairs(results) == _expected_pairs(node_expected), (metric_code, node)


def test_counter_delta_rules_are_five():
    assert len(COUNTER_CODES) == 5


@pytest.mark.parametrize('metric_code', COUNTER_CODES)
def test_counter_delta_first_round_has_no_baseline(
    metric_code, expected_table, probe_nodes, merged_views
):
    """首轮没有基线 → **不产出值**（不是 0，也不是假数据）。"""
    rule_payload = next(r for r in B_RULES if r['metric_code'] == metric_code)
    engine_rule = _engine_rule(rule_payload)
    entry = expected_table['expected'][metric_code]
    now_ms = expected_table['counter_delta_plan']['round1_now_ms']

    for node, node_expected in sorted(entry['round1'].items()):
        merged = decode_merged(
            {'result': probe_nodes[node]['result']}, merged_views, now_ms=now_ms
        )
        results = _evaluate(engine_rule, merged.by_sql_key[entry['sql_key']], rule_payload)
        pairs = _pairs(results)
        assert pairs, (metric_code, node)
        for _dims, value in pairs:
            assert value is None, (metric_code, node, value)
        assert pairs == _expected_pairs(node_expected), (metric_code, node)


@pytest.mark.parametrize('metric_code', COUNTER_CODES)
def test_counter_delta_second_round_rate_equals_expected(
    metric_code, expected_table, probe_nodes, merged_views, counter_plan
):
    """第 2 轮（合成留档：累计计数加量）→ 差分速率 == 期望速率。"""
    rule_payload = next(r for r in B_RULES if r['metric_code'] == metric_code)
    engine_rule = _engine_rule(rule_payload)
    entry = expected_table['expected'][metric_code]
    sql_key = entry['sql_key']
    deltas = counter_plan['deltas']
    elapsed = counter_plan['elapsed_seconds']
    column = rule_payload['value_rule']['params']['column']
    expected_rate = deltas[column] / elapsed

    for node, node_expected in sorted(entry['round2'].items()):
        store = CounterDeltaStore()
        first = decode_merged(
            {'result': probe_nodes[node]['result']},
            merged_views,
            now_ms=counter_plan['round1_now_ms'],
        )
        # 首轮：只建基线
        evaluate(engine_rule, first.by_sql_key[sql_key], store)
        # 第 2 轮：合成留档（计数按计划加量）
        second = decode_merged(
            _synthetic_round2(probe_nodes[node]['result'], deltas),
            merged_views,
            now_ms=counter_plan['round2_now_ms'],
        )
        results = evaluate(engine_rule, second.by_sql_key[sql_key], store)
        pairs = _pairs(results)
        assert pairs == _expected_pairs(node_expected), (metric_code, node)
        for _dims, value in pairs:
            assert value is not None, (metric_code, node)
            assert value == pytest.approx(expected_rate), (metric_code, node, value)


# -- state_contains：留档里匹配不到任何行，语义要用合成样本补上 ---------
#
# **留档的盲区**：``activity_agg`` 视图实测只有 ``active`` / ``idle`` 两种 state，
# 而 ``gaussdb.Lockwaiting``（``state_contains: ["lock"]``）与
# ``gaussdb.Waiting_conn``（``state_contains: ["wait"]``）在这份留档上**一行都匹配不到**
# → 两条规则都算出 0，「子串包含」写错也看不出来。
# 所以这里用**合成行**补一条：声明说「state 含关键词就算命中」，
# 引擎就必须把含关键词的那行算进去（不看 state 的具体取值）。

CONTAINS_RULES = {
    'gaussdb.Lockwaiting': ('lock', 'waiting on lock'),
    'gaussdb.Waiting_conn': ('wait', 'waiting on lock'),
}

# 第三行故意用**大写** state：关键词是小写的，只有「忽略大小写」的子串包含
# 才会命中它。把引擎的 ``.lower()`` 去掉，这条用例会红。
CONTAINS_ROWS = (
    {'datname': 'db', 'state': 'active', 'cnt': 3},
    {'datname': 'db', 'state': 'waiting on lock', 'cnt': 5},
    {'datname': 'db', 'state': 'WAITING ON LOCK', 'cnt': 7},
    {'datname': 'db', 'state': 'idle', 'cnt': 11},
)

CONTAINS_MATCHED_COUNT = 12  # 5 + 7：两条含关键词的行


@pytest.mark.parametrize('metric_code', sorted(CONTAINS_RULES))
def test_state_contains_matches_rows_with_the_keyword(metric_code):
    """``state_contains`` 是**子串包含**（忽略大小写），不是等值。"""
    keyword, matching_state = CONTAINS_RULES[metric_code]
    rule_payload = next(r for r in B_RULES if r['metric_code'] == metric_code)
    declared = rule_payload['value_rule']['params']['filter']['state_contains']
    assert keyword in declared

    ctx = RuleContext(rows=CONTAINS_ROWS, now_ms=0)
    results = evaluate(_engine_rule(rule_payload), ctx)
    pairs = _pairs(results)
    assert len(pairs) == 1, (metric_code, pairs)
    # 只有含关键词的那两行（小写 5 + 大写 7）被算进去
    assert pairs[0][1] == CONTAINS_MATCHED_COUNT, (metric_code, pairs)
    assert matching_state in CONTAINS_ROWS[1]['state']


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


# -- 生成物与生成器一致 ----------------------------------------------------

def test_file_is_up_to_date():
    """期望值表必须与生成器输出一致（规则或留档变了要重新生成）。"""
    import build_expected_values

    current = _load(EXPECTED_PATH)
    fresh = build_expected_values.build()
    assert current == fresh, (
        'expected_values.json 过期，请运行 tools/build_expected_values.py'
    )
