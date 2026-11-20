"""取值规则引擎（13 种 ``value_rule.type``）的单元测试。

守的是**口径**，不是代码形状：

- 13 种规则类型各自的正常路径与边界路径（空行、缺列、类型不对、分母为 0、
  计数归零、枚举未命中…）；
- 差分用**实际采集时刻差**，首轮无基线只记基线，计数归零丢弃本轮并记 ERROR；
- **不静默出 0**：分母取不到就不产出，而不是编一个 0；
- ``top_n`` 按 **PostgreSQL ORDER BY** 口径排序（时间戳按时间比、字符串按字典序、
  空值升序在后降序在前）；
- ``raw_filtered`` 多命中取**首个匹配行**（R-M1）。

期望值口径的独立第二实现是 ``tools/expected_deriver.py``（B 类 82 条指标的
期望值表由它推出），本文件只测单条规则的行为。
"""

import logging

import pytest

from tpops_kafka_collector.mapping.rules import (
    CounterDeltaStore,
    RuleContext,
    RuleError,
    evaluate,
)
from tpops_kafka_collector.mapping.table import MetricRule


# -- 构造规则的夹具 -------------------------------------------------------

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


def _values(results):
    """引擎输出 → ``[(维度, 值)]``（时间戳不参与比较）。"""
    return [(dims, value) for dims, value, _clock in results]


# -- raw ------------------------------------------------------------------

def test_raw_sum_policy_sums_the_column():
    rule = _rule({'type': 'raw', 'params': {'column': 'cnt', 'row_policy': 'sum'}})
    ctx = RuleContext(rows=({'cnt': 3}, {'cnt': 4}))
    assert evaluate(rule, ctx) == [({}, 7, ctx.now_ms)]


def test_raw_sum_policy_on_empty_rows_is_zero():
    rule = _rule({'type': 'raw', 'params': {'column': 'cnt', 'row_policy': 'sum'}})
    assert evaluate(rule, RuleContext(rows=())) == [({}, 0, 0)]


def test_raw_first_policy_takes_the_first_row():
    rule = _rule({'type': 'raw', 'params': {'column': 'v', 'row_policy': 'first'}})
    ctx = RuleContext(rows=({'v': 'a'}, {'v': 'b'}))
    assert evaluate(rule, ctx) == [({}, 'a', ctx.now_ms)]


def test_raw_first_policy_on_empty_rows_produces_nothing():
    rule = _rule({'type': 'raw', 'params': {'column': 'v', 'row_policy': 'first'}})
    assert evaluate(rule, RuleContext(rows=())) == []


def test_raw_per_row_policy_emits_one_value_per_row():
    rule = _rule(
        {'type': 'raw', 'params': {'column': 'n', 'row_policy': 'per_row'}},
        {'NAME': 'relname'},
    )
    ctx = RuleContext(rows=({'relname': 't1', 'n': 10}, {'relname': 't2', 'n': 5}))
    assert evaluate(rule, ctx) == [
        ({'NAME': 't1'}, 10, ctx.now_ms),
        ({'NAME': 't2'}, 5, ctx.now_ms),
    ]


def test_raw_unknown_row_policy_raises():
    rule = _rule({'type': 'raw', 'params': {'column': 'n', 'row_policy': 'nonsense'}})
    with pytest.raises(RuleError):
        evaluate(rule, RuleContext(rows=({'n': 1},)))


def test_raw_missing_column_does_not_fabricate_zero():
    """行里没有该列 → 值是 None（缺值），**不是 0**。"""
    rule = _rule({'type': 'raw', 'params': {'column': 'n', 'row_policy': 'per_row'}})
    ctx = RuleContext(rows=({'other': 1},))
    assert evaluate(rule, ctx) == [({}, None, ctx.now_ms)]


# -- raw_filtered（R-M1：多命中取首个匹配行）------------------------------

FILTERED_ROWS = (
    {'name': 'port', 'setting': '5432'},
    {'name': 'max_connections', 'setting': '200'},
    {'name': 'port', 'setting': '6432'},
)


def test_raw_filtered_takes_the_first_matching_row():
    """**多命中取首个匹配行**（R-M1 定口径）。"""
    rule = _rule({'type': 'raw_filtered', 'params': {
        'filter': {'name': 'port'}, 'column': 'setting',
    }})
    ctx = RuleContext(rows=FILTERED_ROWS)
    assert evaluate(rule, ctx) == [({}, '5432', ctx.now_ms)]


def test_raw_filtered_single_match_is_unaffected():
    rule = _rule({'type': 'raw_filtered', 'params': {
        'filter': {'name': 'max_connections'}, 'column': 'setting', 'cast': 'int',
    }})
    ctx = RuleContext(rows=FILTERED_ROWS)
    assert evaluate(rule, ctx) == [({}, 200, ctx.now_ms)]


def test_raw_filtered_without_match_produces_nothing():
    rule = _rule({'type': 'raw_filtered', 'params': {
        'filter': {'name': 'nope'}, 'column': 'setting',
    }})
    assert evaluate(rule, RuleContext(rows=FILTERED_ROWS)) == []


def test_raw_filtered_cast_int_on_non_numeric_gives_none():
    rule = _rule({'type': 'raw_filtered', 'params': {
        'filter': {'name': 'port'}, 'column': 'setting', 'cast': 'int',
    }})
    ctx = RuleContext(rows=({'name': 'port', 'setting': 'not-a-number'},))
    assert evaluate(rule, ctx) == [({}, None, ctx.now_ms)]


def test_raw_filtered_carries_dimension_values():
    """维度要带上：指标名里的占位符靠它替换。"""
    rule = _rule(
        {'type': 'raw_filtered', 'params': {
            'filter': {'name': 'port'}, 'column': 'setting', 'cast': 'int',
        }},
        {'PORT': 'setting'},
    )
    ctx = RuleContext(rows=({'name': 'port', 'setting': '5432'},))
    # 维度值取该列的**原值**（与独立推导器一致），cast 只作用于指标值本身
    assert evaluate(rule, ctx) == [({'PORT': '5432'}, 5432, ctx.now_ms)]


# -- aggregate ------------------------------------------------------------

def test_aggregate_filters_and_sums():
    rule = _rule({'type': 'aggregate', 'params': {
        'filter': {'state': 'idle'}, 'group_by': [], 'fn': 'sum', 'value_column': 'cnt',
    }})
    ctx = RuleContext(rows=(
        {'state': 'idle', 'cnt': 4}, {'state': 'active', 'cnt': 2}, {'state': 'idle', 'cnt': 3},
    ))
    assert evaluate(rule, ctx) == [({}, 7, ctx.now_ms)]


def test_aggregate_groups_by_column():
    rule = _rule(
        {'type': 'aggregate', 'params': {
            'group_by': ['datname'], 'fn': 'sum', 'value_column': 'cnt',
        }},
        {'DBNAME': 'datname'},
    )
    ctx = RuleContext(rows=(
        {'datname': 'postgres', 'cnt': 2},
        {'datname': 'postgres', 'cnt': 1},
        {'datname': 'template1', 'cnt': 5},
    ))
    assert _values(evaluate(rule, ctx)) == [
        ({'DBNAME': 'postgres'}, 3), ({'DBNAME': 'template1'}, 5),
    ]


def test_aggregate_contains_filter_is_case_insensitive():
    rule = _rule({'type': 'aggregate', 'params': {
        'filter': {'state_contains': ['lock']}, 'group_by': [],
        'fn': 'sum', 'value_column': 'cnt',
    }})
    ctx = RuleContext(rows=(
        {'state': 'active', 'cnt': 1},
        {'state': 'WAITING ON LOCK', 'cnt': 4},
    ))
    assert evaluate(rule, ctx) == [({}, 4, ctx.now_ms)]


def test_aggregate_unsupported_fn_raises():
    rule = _rule({'type': 'aggregate', 'params': {
        'group_by': [], 'fn': 'max', 'value_column': 'cnt',
    }})
    with pytest.raises(RuleError):
        evaluate(rule, RuleContext(rows=({'cnt': 1},)))


def test_aggregate_on_empty_rows_is_zero():
    rule = _rule({'type': 'aggregate', 'params': {
        'filter': {'state': 'idle'}, 'group_by': [],
        'fn': 'sum', 'value_column': 'cnt',
    }})
    assert evaluate(rule, RuleContext(rows=())) == [({}, 0, 0)]


# -- counter_delta --------------------------------------------------------

def test_counter_delta_first_round_only_records_the_baseline():
    rule = _rule(
        {'type': 'counter_delta', 'params': {'column': 'c', 'row_policy': 'per_row'}},
        {'DATNAME': 'datname'},
    )
    store = CounterDeltaStore()
    ctx = RuleContext(rows=({'datname': 'db', 'c': 100},), now_ms=1000)
    assert evaluate(rule, ctx, store) == [({'DATNAME': 'db'}, None, 1000)]


def test_counter_delta_uses_the_real_sampling_time_difference():
    """差分用**实际采集时刻差**，不是标称周期。"""
    rule = _rule(
        {'type': 'counter_delta', 'params': {'column': 'c', 'row_policy': 'per_row'}},
        {'DATNAME': 'datname'},
    )
    store = CounterDeltaStore()
    evaluate(rule, RuleContext(rows=({'datname': 'db', 'c': 100},), now_ms=1000), store)
    result = evaluate(
        rule, RuleContext(rows=({'datname': 'db', 'c': 160},), now_ms=61000), store
    )
    assert result == [({'DATNAME': 'db'}, 1.0, 61000)]


def test_counter_delta_drops_the_round_when_the_counter_resets(caplog):
    """计数归零 → 丢弃本轮差分并记 ERROR（不产出负速率）。"""
    rule = _rule(
        {'type': 'counter_delta', 'params': {'column': 'c', 'row_policy': 'per_row'}},
        {'DATNAME': 'datname'},
    )
    store = CounterDeltaStore()
    evaluate(rule, RuleContext(rows=({'datname': 'db', 'c': 100},), now_ms=1000), store)
    with caplog.at_level(logging.ERROR, logger='tpops_kafka_collector.mapping.rules'):
        result = evaluate(
            rule, RuleContext(rows=({'datname': 'db', 'c': 0},), now_ms=61000), store
        )
    assert result == [({'DATNAME': 'db'}, None, 61000)]
    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert errors, '计数归零必须记 ERROR'
    assert '重启' in errors[0].getMessage()


def test_counter_delta_without_a_store_raises():
    rule = _rule({'type': 'counter_delta', 'params': {'column': 'c'}})
    with pytest.raises(RuleError):
        evaluate(rule, RuleContext(rows=({'c': 1},)))


def test_counter_delta_drops_the_round_when_the_clock_does_not_advance():
    rule = _rule(
        {'type': 'counter_delta', 'params': {'column': 'c', 'row_policy': 'per_row'}},
        {'DATNAME': 'datname'},
    )
    store = CounterDeltaStore()
    evaluate(rule, RuleContext(rows=({'datname': 'db', 'c': 100},), now_ms=1000), store)
    result = evaluate(
        rule, RuleContext(rows=({'datname': 'db', 'c': 200},), now_ms=1000), store
    )
    assert result == [({'DATNAME': 'db'}, None, 1000)]


# -- ratio_across（M-3：分母取不到 → 不产出，不静默出 0）------------------

def test_ratio_across_divides_the_two_sides():
    rule = _rule({'type': 'ratio_across', 'params': {
        'numerator': {'sql_key': 'a', 'fn': 'sum', 'value_column': 'cnt'},
        'denominator': {'sql_key': 'b', 'fn': 'sum', 'value_column': 'n'},
    }})
    ctx = RuleContext(rows_by_key={
        'a': ({'cnt': 5}, {'cnt': 5}),
        'b': ({'n': 20},),
    })
    assert evaluate(rule, ctx) == [({}, 0.5, ctx.now_ms)]


def test_ratio_across_with_zero_denominator_produces_nothing():
    """分母为 0 → **不产出**（不是 0.0）。"""
    rule = _rule({'type': 'ratio_across', 'params': {
        'numerator': {'sql_key': 'a', 'fn': 'sum', 'value_column': 'cnt'},
        'denominator': {'sql_key': 'b', 'fn': 'sum', 'value_column': 'n'},
    }})
    ctx = RuleContext(rows_by_key={'a': ({'cnt': 5},), 'b': ({'n': 0},)})
    assert evaluate(rule, ctx) == []


def test_ratio_across_with_missing_denominator_view_produces_nothing():
    rule = _rule({'type': 'ratio_across', 'params': {
        'numerator': {'sql_key': 'a', 'fn': 'sum', 'value_column': 'cnt'},
        'denominator': {'sql_key': 'b', 'fn': 'sum', 'value_column': 'n'},
    }})
    ctx = RuleContext(rows_by_key={'a': ({'cnt': 5},)})
    assert evaluate(rule, ctx) == []


def test_ratio_across_with_zero_numerator_produces_zero():
    """分母可用、分子为 0 → 比率真的是 0（没有活跃会话）。"""
    rule = _rule({'type': 'ratio_across', 'params': {
        'numerator': {'sql_key': 'a', 'fn': 'sum', 'value_column': 'cnt'},
        'denominator': {'sql_key': 'b', 'fn': 'sum', 'value_column': 'n'},
    }})
    ctx = RuleContext(rows_by_key={'a': (), 'b': ({'n': 20},)})
    assert evaluate(rule, ctx) == [({}, 0.0, ctx.now_ms)]


def test_ratio_across_keeps_two_decimals():
    rule = _rule({'type': 'ratio_across', 'params': {
        'numerator': {'sql_key': 'a', 'fn': 'sum', 'value_column': 'cnt'},
        'denominator': {'sql_key': 'b', 'fn': 'sum', 'value_column': 'n'},
    }})
    ctx = RuleContext(rows_by_key={'a': ({'cnt': 1},), 'b': ({'n': 3},)})
    assert evaluate(rule, ctx) == [({}, 0.33, ctx.now_ms)]


# -- series_point ---------------------------------------------------------

def test_series_point_takes_the_last_non_null_value():
    rule = _rule({'type': 'series_point', 'params': {'position': 'last_non_null'}})
    ctx = RuleContext(series={'cn_1': [1.0, 2.0, None, 4.0]})
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_1'}, 4.0, ctx.now_ms)]


def test_series_point_skips_a_series_without_any_value():
    rule = _rule({'type': 'series_point', 'params': {}})
    ctx = RuleContext(series={'cn_1': [None, None]})
    assert evaluate(rule, ctx) == []


def test_series_point_uses_the_timestamp_of_the_picked_index():
    rule = _rule({'type': 'series_point', 'params': {}})
    ctx = RuleContext(series={'cn_1': [1.0, None]}, timestamps=(111, 222))
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_1'}, 1.0, 111)]


def test_series_point_falls_back_to_now_when_the_timestamp_is_zero():
    rule = _rule({'type': 'series_point', 'params': {}})
    ctx = RuleContext(series={'cn_1': [1.0]}, timestamps=(0,), now_ms=999)
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_1'}, 1.0, 999)]


def test_series_point_unknown_position_raises():
    rule = _rule({'type': 'series_point', 'params': {'position': 'middle'}})
    with pytest.raises(RuleError):
        evaluate(rule, RuleContext(series={'cn_1': [1.0]}))


# -- enum_map -------------------------------------------------------------

ENUM_RULE = {'type': 'enum_map', 'params': {
    'mapping': {'1': '正常', '0': '异常'}, 'default': 'passthrough',
}}


def test_enum_map_translates_a_known_value():
    rule = _rule(ENUM_RULE)
    ctx = RuleContext(series={'cn_1': [1.0]})
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_1'}, '正常', ctx.now_ms)]


def test_enum_map_passes_an_unknown_value_through(caplog):
    rule = _rule(ENUM_RULE)
    ctx = RuleContext(series={'cn_1': [7.0]})
    with caplog.at_level(logging.WARNING, logger='tpops_kafka_collector.mapping.rules'):
        result = evaluate(rule, ctx)
    assert result == [({'COMPONENT_ID': 'cn_1'}, 7.0, ctx.now_ms)]
    assert any(record.levelno == logging.WARNING for record in caplog.records)


def test_enum_map_with_a_null_default_yields_none():
    rule = _rule({'type': 'enum_map', 'params': {
        'mapping': {'1': '正常'}, 'default': 'null',
    }})
    ctx = RuleContext(series={'cn_1': [7.0]})
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_1'}, None, ctx.now_ms)]


def test_enum_map_matches_the_float_and_the_integer_form():
    """枚举键是字符串：1.0 与 1 要命中同一条。"""
    rule = _rule(ENUM_RULE)
    ctx = RuleContext(series={'cn_1': [1.0]})
    assert evaluate(rule, ctx)[0][1] == '正常'


# -- gauge ----------------------------------------------------------------

def test_gauge_computes_the_elapsed_seconds():
    rule = _rule({'type': 'gauge', 'params': {'unit': '秒'}})
    ctx = RuleContext(series={'cn_1': [1000.0]}, now_ms=61000 * 1000)
    assert evaluate(rule, ctx) == [({'COMPONENT_ID': 'cn_1'}, 60000.0, ctx.now_ms)]


def test_gauge_rejects_an_unsupported_unit():
    rule = _rule({'type': 'gauge', 'params': {'unit': '分钟'}})
    with pytest.raises(RuleError):
        evaluate(rule, RuleContext(series={'cn_1': [1000.0]}))


def test_gauge_skips_a_series_without_any_value():
    rule = _rule({'type': 'gauge', 'params': {'unit': '秒'}})
    ctx = RuleContext(series={'cn_1': [None]}, now_ms=1000)
    assert evaluate(rule, ctx) == []


# -- top_n（R-M2：按 PostgreSQL ORDER BY 真正排序）-------------------------

def test_top_n_sorts_by_the_column_descending():
    rule = _rule({'type': 'top_n', 'params': {
        'sort_by': 'total_elapse_time', 'order': 'desc', 'limit': 2,
        'columns': ['total_elapse_time'],
    }})
    ctx = RuleContext(rows=(
        {'total_elapse_time': 10}, {'total_elapse_time': 30}, {'total_elapse_time': 20},
    ))
    rows = evaluate(rule, ctx)[0][1]
    assert [row['total_elapse_time'] for row in rows] == [30, 20]


def test_top_n_sorts_timestamps_by_time_not_by_text():
    """排序列是时间戳字符串 → 按**时间**比，不是按字典序，也不是全退化成 0。"""
    rule = _rule({'type': 'top_n', 'params': {
        'sort_by': 'query_start', 'order': 'desc', 'limit': 3,
        'columns': ['query_start'],
    }})
    ctx = RuleContext(rows=(
        {'query_start': '2026-11-13 13:39:11.749342+08'},
        {'query_start': '2026-11-18 16:27:07.557567+08'},
        {'query_start': '2026-11-13 13:39:11.855510+08'},
    ))
    rows = evaluate(rule, ctx)[0][1]
    assert [row['query_start'] for row in rows] == [
        '2026-11-18 16:27:07.557567+08',
        '2026-11-13 13:39:11.855510+08',
        '2026-11-13 13:39:11.749342+08',
    ]


def test_top_n_sorts_timestamps_ascending():
    rule = _rule({'type': 'top_n', 'params': {
        'sort_by': 'query_start', 'order': 'asc', 'limit': 2,
        'columns': ['query_start'],
    }})
    ctx = RuleContext(rows=(
        {'query_start': '2026-11-18 16:27:07.557567+08'},
        {'query_start': '2026-11-13 13:39:11.749342+08'},
    ))
    rows = evaluate(rule, ctx)[0][1]
    assert [row['query_start'] for row in rows] == [
        '2026-11-13 13:39:11.749342+08',
        '2026-11-18 16:27:07.557567+08',
    ]


def test_top_n_puts_nulls_last_when_ascending():
    """PostgreSQL 的 ORDER BY 默认：升序空值在后、降序空值在前。"""
    rule = _rule({'type': 'top_n', 'params': {
        'sort_by': 'query_start', 'order': 'asc', 'limit': 3,
        'columns': ['query_start'],
    }})
    ctx = RuleContext(rows=(
        {'query_start': None},
        {'query_start': '2026-11-13 13:39:11.749342+08'},
        {'query_start': None},
    ))
    rows = evaluate(rule, ctx)[0][1]
    assert rows[0]['query_start'] == '2026-11-13 13:39:11.749342+08'
    assert [row['query_start'] for row in rows[1:]] == [None, None]


def test_top_n_puts_nulls_first_when_descending():
    rule = _rule({'type': 'top_n', 'params': {
        'sort_by': 'query_start', 'order': 'desc', 'limit': 2,
        'columns': ['query_start'],
    }})
    ctx = RuleContext(rows=(
        {'query_start': '2026-11-13 13:39:11.749342+08'},
        {'query_start': None},
    ))
    rows = evaluate(rule, ctx)[0][1]
    assert rows[0]['query_start'] is None


def test_top_n_treats_an_empty_string_as_a_missing_value():
    """平台把 NULL 的 query_start 渲染成空串 —— 空串按**空值**处理（升序在后）。"""
    rule = _rule({'type': 'top_n', 'params': {
        'sort_by': 'query_start', 'order': 'asc', 'limit': 3,
        'columns': ['query_start'],
    }})
    ctx = RuleContext(rows=(
        {'query_start': ''},
        {'query_start': '2026-11-13 13:39:11.749342+08'},
        {'query_start': ''},
    ))
    rows = evaluate(rule, ctx)[0][1]
    assert rows[0]['query_start'] == '2026-11-13 13:39:11.749342+08'
    assert [row['query_start'] for row in rows[1:]] == ['', '']


def test_top_n_reads_the_declared_collection():
    """声明了 collection 就从集合里取（不是从 ctx.rows）。"""
    rule = _rule({'type': 'top_n', 'params': {
        'collection': 'top_sql_infos', 'sort_by': 'total_elapse_time',
        'order': 'desc', 'limit': 2,
    }})
    ctx = RuleContext(collections={'top_sql_infos': (
        {'total_elapse_time': 5}, {'total_elapse_time': 9},
    )})
    rows = evaluate(rule, ctx)[0][1]
    assert [row['total_elapse_time'] for row in rows] == [9, 5]


def test_top_n_on_empty_input_produces_an_empty_list():
    rule = _rule({'type': 'top_n', 'params': {'sort_by': 'v', 'limit': 5}})
    assert evaluate(rule, RuleContext(rows=())) == [({}, [], 0)]


# -- raw_rows -------------------------------------------------------------

def test_raw_rows_returns_every_row():
    rule = _rule({'type': 'raw_rows', 'params': {}})
    ctx = RuleContext(rows=({'a': 1}, {'a': 2}))
    assert evaluate(rule, ctx) == [({}, [{'a': 1}, {'a': 2}], ctx.now_ms)]


# -- collection_count -----------------------------------------------------

def test_collection_count_counts_the_collection():
    rule = _rule({'type': 'collection_count', 'params': {'collection': 'slow_sql_infos'}})
    ctx = RuleContext(collections={'slow_sql_infos': ({'sql_id': '1'}, {'sql_id': '2'})})
    assert evaluate(rule, ctx) == [({}, 2, ctx.now_ms)]


def test_collection_count_of_a_missing_collection_is_zero():
    rule = _rule({'type': 'collection_count', 'params': {'collection': 'nope'}})
    assert evaluate(rule, RuleContext(collections={})) == [({}, 0, 0)]


# -- text_prefix_count ----------------------------------------------------

def test_text_prefix_count_classifies_select():
    rule = _rule({'type': 'text_prefix_count', 'params': {
        'collection': 'slow_sql_infos', 'field': 'sql_text',
        'prefixes': ['select', 'with'],
    }})
    ctx = RuleContext(collections={'slow_sql_infos': (
        {'sql_text': 'SELECT * FROM t'},
        {'sql_text': 'with x as (select 1) select * from x'},
        {'sql_text': 'INSERT INTO t VALUES (1)'},
    )})
    assert evaluate(rule, ctx) == [({}, 2, ctx.now_ms)]


def test_text_prefix_count_of_a_missing_collection_is_zero():
    rule = _rule({'type': 'text_prefix_count', 'params': {
        'collection': 'nope', 'field': 'sql_text', 'prefixes': ['select'],
    }})
    assert evaluate(rule, RuleContext(collections={})) == [({}, 0, 0)]


def test_text_prefix_count_ignores_items_without_the_field():
    """集合里有的行没有该字段 → 当空文本，不计数也不报错。"""
    rule = _rule({'type': 'text_prefix_count', 'params': {
        'collection': 'slow_sql_infos', 'field': 'sql_text', 'prefixes': ['select'],
    }})
    ctx = RuleContext(collections={'slow_sql_infos': (
        {'sql_id': '1'}, {'sql_text': 'SELECT 1'}, {'sql_text': None},
    )})
    assert evaluate(rule, ctx) == [({}, 1, ctx.now_ms)]


# -- instance_field -------------------------------------------------------

def test_instance_field_reads_the_instance():
    rule = _rule({'type': 'instance_field', 'params': {'field': 'engine_version'}})
    ctx = RuleContext(instance={'engine_version': 'V2.0-10.0.0'})
    assert evaluate(rule, ctx) == [({}, 'V2.0-10.0.0', ctx.now_ms)]


def test_instance_field_missing_key_gives_none():
    rule = _rule({'type': 'instance_field', 'params': {'field': 'engine_version'}})
    assert evaluate(rule, RuleContext(instance={})) == [({}, None, 0)]


# -- group_by（R-M3：引擎不再忽略这个参数）--------------------------------

def test_group_by_provides_the_dimension_values():
    rule = _rule(
        {'type': 'raw', 'params': {
            'column': 'n', 'row_policy': 'per_row', 'group_by': ['relname'],
        }},
        {'NAME': 'relname'},
    )
    ctx = RuleContext(rows=({'relname': 't1', 'n': 3}, {'relname': 't2', 'n': 4}))
    assert _values(evaluate(rule, ctx)) == [({'NAME': 't1'}, 3), ({'NAME': 't2'}, 4)]


def test_group_by_reports_a_repeated_group_key(caplog):
    """分组键重复 → 维度值不唯一（会在幂等层撞键），必须显式报出来。"""
    rule = _rule(
        {'type': 'raw', 'params': {
            'column': 'n', 'row_policy': 'per_row', 'group_by': ['relname'],
        }},
        {'NAME': 'relname'},
    )
    ctx = RuleContext(rows=(
        {'relname': 't1', 'n': 3},
        {'relname': 't1', 'n': 4},
    ))
    with caplog.at_level(logging.WARNING, logger='tpops_kafka_collector.mapping.rules'):
        result = evaluate(rule, ctx)
    assert _values(result) == [({'NAME': 't1'}, 3), ({'NAME': 't1'}, 4)]
    assert any(record.levelno == logging.WARNING for record in caplog.records)


def test_group_by_inconsistent_with_the_dimension_bindings_raises():
    """group_by 与 dimension_bindings 不一致 → 声明自相矛盾，不能静默。"""
    rule = _rule(
        {'type': 'raw', 'params': {
            'column': 'n', 'row_policy': 'per_row', 'group_by': ['other'],
        }},
        {'NAME': 'relname'},
    )
    with pytest.raises(RuleError):
        evaluate(rule, RuleContext(rows=({'relname': 't1', 'n': 3},)))


# -- 分派 -----------------------------------------------------------------

def test_unknown_rule_type_raises():
    with pytest.raises(RuleError):
        evaluate(_rule({'type': '不存在的类型'}), RuleContext())


def test_missing_rule_type_raises():
    with pytest.raises(RuleError):
        evaluate(_rule({}), RuleContext())
