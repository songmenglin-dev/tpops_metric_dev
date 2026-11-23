"""规则引擎各模块的边界与守卫（补测）。

守的是**口径**，不是代码形状：

- ``ratio_across``：分母取不到 / 为 0 → **不产出**；分子 0 分母正 → 产出 ``0.0``；
- ``counter_delta`` 的 ``row_policy: sum``：整视图求和（不是逐行出值）；
- 差分基线的**作用域隔离**与 ``forget``；
- 排序的边界：没有排序列、布尔列、时间戳列、空值位置；
- 时序取值位置 ``last`` / ``first`` 与枚举键 ``1.0 == 1``。

**冻结口径**（控制器裁决，见任务简报）：``ratio_across`` 分母不可用一律不产出，
本文件按此断言；规格 ``spec.md:156`` 写「分母为 0 时结果为 0」与该裁决相反，
属已登记偏差（T-6），本次不改规格。
"""

import pytest

from tpops_kafka_collector.mapping.rules import (
    CounterDeltaStore,
    RuleContext,
    RuleError,
    evaluate,
)
from tpops_kafka_collector.mapping.rules_context import dimension_key, to_int, to_number
from tpops_kafka_collector.mapping.rules_cross import side_value
from tpops_kafka_collector.mapping.rules_deltas import ScopedCounterDeltaStore
from tpops_kafka_collector.mapping.rules_series import pick_point
from tpops_kafka_collector.mapping.rules_sorting import sort_rows
from tpops_kafka_collector.mapping.table import MetricRule


def _rule(value_rule, dimension_bindings=None, metric_code='test.metric'):
    """按声明造一条规则（不依赖映射表，便于测边界口径）。"""
    payload = {
        'metric_code': metric_code,
        'metric_name': metric_code,
        'resource_type': 'GAUSSDB实例',
        'category': '事务信息',
        'value_type': '整数',
        'unit': None,
        'collect_cycle': {'value': 5, 'unit': '分'},
        'status': '可提供',
        'match_level': '直接对应',
        'enabled': True,
        'source': {
            'api': 'POST /rds/gaussdb/v3/customer-commands',
            'source': 'command_center_sql',
            'tpops_source': None,
            'value_source_expr': None,
            'sql_key': 'view',
            'method': '命令中心SQL',
        },
        'value_rule': value_rule,
        'placeholders': [],
        'resource_granularity': 'instance',
        'master_view': None,
        'node_scoped': True,
        'dimension_bindings': dimension_bindings or {},
        'note': '',
        'needs_confirmation': False,
    }
    return MetricRule.from_dict(payload)


# -- ratio_across：分母不可用一律不产出 ----------------------------------

def _ratio_rule(numerator_key='num', denominator_key='den'):
    return _rule({'type': 'ratio_across', 'params': {
        'numerator': {'sql_key': numerator_key, 'value_column': 'v', 'fn': 'sum'},
        'denominator': {'sql_key': denominator_key, 'value_column': 'v', 'fn': 'sum'},
    }})


def test_ratio_across_produces_nothing_when_the_denominator_view_is_missing():
    """分母视图缺失 → 不产出（不是 0，也不是除零异常）。"""
    ctx = RuleContext(rows_by_key={'num': ({'v': 5},)}, now_ms=1000)
    assert evaluate(_ratio_rule(), ctx) == []


def test_ratio_across_produces_nothing_when_the_denominator_is_zero():
    """分母为 0 → 比率无定义 → 不产出（冻结口径）。"""
    ctx = RuleContext(
        rows_by_key={'num': ({'v': 5},), 'den': ({'v': 0},)}, now_ms=1000,
    )
    assert evaluate(_ratio_rule(), ctx) == []


def test_ratio_across_produces_zero_when_the_numerator_is_zero():
    """分子 0、分母正 → 产出 ``0.0``（那是真的「没有活跃会话」）。"""
    ctx = RuleContext(
        rows_by_key={'num': ({'v': 0},), 'den': ({'v': 10},)}, now_ms=1000,
    )
    assert evaluate(_ratio_rule(), ctx) == [({}, 0.0, 1000)]


def test_ratio_across_rounds_to_two_decimals():
    ctx = RuleContext(
        rows_by_key={'num': ({'v': 1},), 'den': ({'v': 3},)}, now_ms=1000,
    )
    assert evaluate(_ratio_rule(), ctx) == [({}, 0.33, 1000)]


def test_side_value_max_takes_the_largest_row():
    ctx = RuleContext(rows_by_key={'den': ({'v': 1}, {'v': 7}, {'v': 3})})
    spec = {'sql_key': 'den', 'value_column': 'v', 'fn': 'max'}
    assert side_value(spec, ctx) == 7.0


def test_side_value_max_of_an_empty_view_is_zero():
    ctx = RuleContext(rows_by_key={'den': ()})
    spec = {'sql_key': 'den', 'value_column': 'v', 'fn': 'max'}
    assert side_value(spec, ctx) == 0.0


def test_side_value_count_counts_the_rows():
    ctx = RuleContext(rows_by_key={'den': ({'v': 1}, {'v': 2}, {'v': 3})})
    spec = {'sql_key': 'den', 'value_column': 'v', 'fn': 'count'}
    assert side_value(spec, ctx) == 3.0


def test_side_value_rejects_an_unimplemented_aggregation():
    ctx = RuleContext(rows_by_key={'den': ({'v': 1},)})
    with pytest.raises(RuleError) as excinfo:
        side_value({'sql_key': 'den', 'value_column': 'v', 'fn': 'median'}, ctx)
    assert '未实现的 ratio fn' in str(excinfo.value)


# -- counter_delta：整视图求和 -------------------------------------------

def test_counter_delta_sum_policy_diffs_the_whole_view_total():
    """``row_policy: sum`` → 整视图求和做差分，只出一条值。"""
    rule = _rule({'type': 'counter_delta',
                  'params': {'column': 'c', 'row_policy': 'sum'}})
    store = CounterDeltaStore()
    first = RuleContext(rows=({'c': 1}, {'c': 2}), now_ms=1000)
    assert evaluate(rule, first, store) == [({}, None, 1000)]
    second = RuleContext(rows=({'c': 2}, {'c': 4}), now_ms=2000)
    assert evaluate(rule, second, store) == [({}, 3.0, 2000)]


def test_counter_delta_sum_policy_ignores_the_row_dimensions():
    """整视图求和的差分基线不带维度（一条规则一条基线）。"""
    rule = _rule({'type': 'counter_delta',
                  'params': {'column': 'c', 'row_policy': 'sum'}})
    store = CounterDeltaStore()
    first = RuleContext(rows=({'c': 4}, {'c': 6}), now_ms=1000)
    assert evaluate(rule, first, store) == [({}, None, 1000)]
    # 下一轮的两行合计是 20 → 与上一轮的合计 10 相差 10，速率 10.0
    second = RuleContext(rows=({'c': 9}, {'c': 11}), now_ms=2000)
    assert evaluate(rule, second, store) == [({}, 10.0, 2000)]


# -- 差分基线：作用域隔离与 forget ----------------------------------------

def test_forget_drops_the_baseline_so_the_next_round_rebuilds_it():
    store = CounterDeltaStore()
    assert store.rate('m', {'a': '1'}, 10.0, 1000)[0] is None
    store.forget('m', {'a': '1'})
    value, reason = store.rate('m', {'a': '1'}, 20.0, 2000)
    assert value is None
    assert '首轮无基线' in reason


def test_forget_of_an_unknown_dimension_is_a_noop():
    store = CounterDeltaStore()
    store.rate('m', {'a': '1'}, 10.0, 1000)
    store.forget('m', {'b': '2'})
    value, _reason = store.rate('m', {'a': '1'}, 20.0, 2000)
    assert value == 10.0


def test_scoped_store_exposes_its_scope():
    store = CounterDeltaStore()
    assert ScopedCounterDeltaStore(store, 'node-a').scope == 'node-a'


def test_scoped_stores_keep_their_baselines_apart():
    """同一存储上的两个作用域互不覆盖（多个 master 共用存储时的老坑）。"""
    store = CounterDeltaStore()
    view_a = ScopedCounterDeltaStore(store, 'node-a')
    view_b = ScopedCounterDeltaStore(store, 'node-b')
    assert view_a.rate('m', {}, 10.0, 1000)[0] is None
    assert view_b.rate('m', {}, 99.0, 1000)[0] is None
    # A 的下一轮只跟 A 自己的基线比
    assert view_a.rate('m', {}, 12.0, 2000)[0] == 2.0


def test_scoped_store_forget_only_drops_its_own_scope():
    store = CounterDeltaStore()
    view_a = ScopedCounterDeltaStore(store, 'node-a')
    view_b = ScopedCounterDeltaStore(store, 'node-b')
    view_a.rate('m', {}, 10.0, 1000)
    view_b.rate('m', {}, 50.0, 1000)
    view_a.forget('m', {})
    assert view_a.rate('m', {}, 12.0, 2000)[0] is None
    assert view_b.rate('m', {}, 52.0, 2000)[0] == 2.0


def test_scoped_store_forget_is_a_noop_when_the_store_has_none():
    """底层存储没有 ``forget`` → 静默跳过，不抛异常。"""

    class _StoreWithoutForget:
        def rate(self, metric_code, dimensions, counter_value, now_ms, scope=''):
            return None, '首轮无基线，仅建立基线'

    view = ScopedCounterDeltaStore(_StoreWithoutForget(), 'node-a')
    view.forget('m', {})


# -- 数值工具 -------------------------------------------------------------

def test_to_number_of_none_is_zero():
    assert to_number(None) == 0.0


def test_to_number_of_a_boolean_is_zero_or_one():
    assert to_number(True) == 1.0
    assert to_number(False) == 0.0


def test_to_number_of_unparsable_text_is_zero():
    assert to_number('not a number') == 0.0


def test_to_int_of_none_is_none():
    """缺值不是 0 —— ``to_int`` 给 None。"""
    assert to_int(None) is None


def test_dimension_key_of_no_dimensions_is_empty():
    assert dimension_key({}) == ''


# -- 时序取值位置 ---------------------------------------------------------

def test_pick_point_last_returns_the_final_value():
    assert pick_point([1.0, 2.0, 3.0], 'last') == (3.0, 2)


def test_pick_point_last_on_an_empty_series_is_nothing():
    assert pick_point([], 'last') == (None, None)


def test_pick_point_first_returns_the_first_value():
    assert pick_point([1.0, 2.0, 3.0], 'first') == (1.0, 0)


def test_pick_point_first_on_an_empty_series_is_nothing():
    assert pick_point([], 'first') == (None, None)


def test_enum_map_treats_one_point_zero_as_the_integer_key_one():
    """``1.0`` 与 ``1`` 是同一个枚举键。"""
    rule = _rule({'type': 'enum_map', 'params': {
        'mapping': {'1': '运行中', '2': '空闲'},
    }})
    ctx = RuleContext(series={'cn_1': [1.0]}, now_ms=1000)
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_1'}, '运行中', 1000)]


def test_enum_map_uses_the_text_form_of_a_fractional_value():
    """非整数浮点的枚举键是它的字符串形态（``1.5`` → ``'1.5'``）。"""
    rule = _rule({'type': 'enum_map', 'params': {
        'mapping': {'1.5': '半负载'},
    }})
    ctx = RuleContext(series={'cn_1': [1.5]}, now_ms=1000)
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_1'}, '半负载', 1000)]


# -- 节点身份解析 ---------------------------------------------------------

def test_find_node_by_id_without_a_node_id_finds_nothing():
    """没有 node_id → 查不到（不按空串去匹配）。"""
    from tpops_kafka_collector.mapping.nodes import find_node_by_id
    nodes = [{'node_id': 'n1', 'node_ip': '1.2.3.4'}]
    assert find_node_by_id(nodes, None) is None
    assert find_node_by_id(nodes, '') is None


def test_find_node_by_ip_without_an_ip_finds_nothing():
    """没有 host_ip → 查不到（不按空串去匹配）。"""
    from tpops_kafka_collector.mapping.nodes import find_node_by_ip
    nodes = [{'node_id': 'n1', 'node_ip': '1.2.3.4'}]
    assert find_node_by_ip(nodes, None) is None
    assert find_node_by_ip(nodes, '') is None


# -- 排序 -----------------------------------------------------------------

def test_sort_rows_without_a_sort_column_keeps_the_original_order():
    """没有排序列 → 原样返回（不排序、不丢行）。"""
    rows = [{'v': 3}, {'v': 1}, {'v': 2}]
    assert sort_rows(rows, None) == rows
    assert sort_rows(rows, '') == rows


def test_sort_rows_compares_boolean_columns_as_text():
    """布尔列按文本比（不是按数值：False < True 但 str 出来是 'False'/'True'）。"""
    rows = [{'flag': True}, {'flag': False}]
    assert [row['flag'] for row in sort_rows(rows, 'flag', 'desc')] == [True, False]
    assert [row['flag'] for row in sort_rows(rows, 'flag', 'asc')] == [False, True]


def test_sort_rows_compares_timestamps_by_time_not_by_text():
    """两个不同时区偏移的时间戳 → 按**时间**比，不是按字典序。"""
    rows = [
        {'ts': '2026-11-18 16:27:07.000000+08:00'},   # UTC 08:27:07
        {'ts': '2026-11-18 09:00:00.000000+00:00'},   # UTC 09:00:00
    ]
    ordered = [row['ts'] for row in sort_rows(rows, 'ts', 'asc')]
    # 按字典序会是 '09:00...' 在前；按时间应是 +08:00 那条在前
    assert ordered == [
        '2026-11-18 16:27:07.000000+08:00',
        '2026-11-18 09:00:00.000000+00:00',
    ]


def test_sort_rows_falls_back_to_text_order_for_the_two_digit_offset_format():
    """留档里的 ``+08``（两位时区）在 Python 3.7 的 ``%z`` 下解析不了。

    当前行为：按**文本**比（同一偏移下与按时间一致）。这条测试把现状钉住，
    便于将来改时间戳格式时看到差异。
    """
    rows = [
        {'ts': '2026-11-18 16:27:07.557567+08'},
        {'ts': '2026-11-13 13:39:11.749342+08'},
    ]
    ordered = [row['ts'] for row in sort_rows(rows, 'ts', 'asc')]
    assert ordered == [
        '2026-11-13 13:39:11.749342+08',
        '2026-11-18 16:27:07.557567+08',
    ]
