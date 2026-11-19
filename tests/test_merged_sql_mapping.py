# -*- coding: utf-8 -*-
"""合并 SQL（「一行 N 个 json_agg 列」）的解码与分页拼接。

**这个文件守的是本项目最危险的一种失败**：把合并形态的返回体当成行列表，
82 个 B 类指标会**静默产出 0 / None / 空列表**且不报错
（详见 ``changes/tpops-kafka-collector/specs/mapping-and-rules/spec.md``
的「合并 SQL 返回形态的解码」）。

覆盖：
- 真实留档 ``data/merged_sql/sql_merge_probe_result.json`` 能解出 11 个视图的行列表；
- 空视图（``''`` / ``null``）→ ``[]``，不抛异常；
- 坏 JSON 列值 → ``DecodeError``（不静默返回 ``[]`` 或 0）；
- ``result`` 不是恰好 1 行 → ``DecodeError``，并说明期望行数与实际行数；
- ``DecodeError`` 归属 ``MappingError`` → 不可重试；
- 既有 per-view 形态（``result`` 直接是行列表）仍按原样解。
"""
import io
import json
import os

import pytest

from conftest import DATA_DIR, REPO_ROOT

from tpops_kafka_collector.errors import MappingError, is_retryable
from tpops_kafka_collector.mapping import evaluate
from tpops_kafka_collector.mapping.decode import (
    DecodeError,
    decode,
    decode_merged,
    merge_pages,
)

MERGED_DIR = os.path.join(DATA_DIR, 'merged_sql')
PROBE_PATH = os.path.join(MERGED_DIR, 'sql_merge_probe_result.json')
MERGED_SQL_PATH = os.path.join(MERGED_DIR, 'merged_sql.json')
RULES_PATH = os.path.join(DATA_DIR, 'mapping', 'response_rules.json')
RESPONSE_DIR = os.path.join(DATA_DIR, 'metric_response')


def _load(path):
    with io.open(path, encoding='utf-8') as fp:
        return json.load(fp)


@pytest.fixture(scope='module')
def merged_views():
    """映射表里的 merged_views 段（由生成器产出）。"""
    payload = _load(RULES_PATH)
    views = payload.get('merged_views')
    assert views, 'response_rules.json 里没有 merged_views 段'
    return views


@pytest.fixture(scope='module')
def probe_body():
    """真实留档的合并形态返回体：``{"result": <平台返回的 JSON 字符串>}``。"""
    probe = _load(PROBE_PATH)
    node = probe['nodes'][sorted(probe['nodes'])[0]]
    return {'result': node['result']}


# -- 真实留档：解出每个视图的行列表 ---------------------------------------

def test_probe_result_decodes_into_one_row_list_per_view(probe_body, merged_views):
    merged = decode_merged(probe_body, merged_views)
    assert set(merged.rows_by_key) == set(merged_views)
    assert set(merged.by_sql_key) == set(merged_views)
    assert merged.rows_by_key['stat_database']
    assert merged.rows_by_key['stat_user_tables']


def test_decoded_rows_are_dicts_not_json_strings(probe_body, merged_views):
    """回归的核心：列值必须被解成行，不是「整段 JSON 字符串」。"""
    merged = decode_merged(probe_body, merged_views)
    for view, rows in merged.rows_by_key.items():
        for row in rows:
            assert isinstance(row, dict), view
            assert not isinstance(row, dict) or not all(
                isinstance(value, str) and value.startswith('[') for value in row.values()
            ), view
    row = merged.rows_by_key['stat_database'][0]
    assert 'datname' in row
    assert 'xact_commit' in row


def test_view_context_rows_match_rows_by_key(probe_body, merged_views):
    merged = decode_merged(probe_body, merged_views)
    for view, ctx in merged.by_sql_key.items():
        assert tuple(ctx.rows) == merged.rows_by_key[view], view


def test_context_carries_all_views_for_cross_view_rules(probe_body, merged_views):
    """跨视图比率（ratio_across）要能从上下文里取到别的视图的行。"""
    merged = decode_merged(probe_body, merged_views)
    ctx = merged.by_sql_key['activity_agg']
    assert ctx.rows_by_key.get('stat_database')
    assert ctx.rows_by_key.get('activity_agg')


# -- 空视图 ---------------------------------------------------------------

@pytest.mark.parametrize('empty_value', ['', None])
def test_empty_view_decodes_to_empty_list(empty_value):
    views = {'stat_database': {'json_column': 'stat_database', 'paged': False}}
    body = {'result': json.dumps([{'stat_database': empty_value}])}
    merged = decode_merged(body, views)
    assert merged.rows_by_key['stat_database'] == ()
    assert merged.by_sql_key['stat_database'].rows == ()


def test_empty_view_among_other_views_does_not_break_others():
    views = {
        'stat_database': {'json_column': 'stat_database', 'paged': False},
        'locks': {'json_column': 'locks', 'paged': False},
    }
    body = {'result': json.dumps([
        {'stat_database': '', 'locks': '[{"mode":"ExclusiveLock"}]'},
    ])}
    merged = decode_merged(body, views)
    assert merged.rows_by_key['stat_database'] == ()
    assert merged.rows_by_key['locks'][0]['mode'] == 'ExclusiveLock'


# -- 坏数据 ---------------------------------------------------------------

MALFORMED_VIEW_JSON = '[{"datname": '


def test_malformed_column_value_raises_decode_error():
    views = {'stat_database': {'json_column': 'stat_database', 'paged': False}}
    body = {'result': json.dumps([{'stat_database': MALFORMED_VIEW_JSON}])}
    with pytest.raises(DecodeError):
        decode_merged(body, views)


def test_malformed_column_value_does_not_silently_return_empty():
    views = {'stat_database': {'json_column': 'stat_database', 'paged': False}}
    body = {'result': json.dumps([{'stat_database': MALFORMED_VIEW_JSON}])}
    try:
        merged = decode_merged(body, views)
    except DecodeError:
        return
    pytest.fail('坏 JSON 静默返回了 %r' % (merged.rows_by_key.get('stat_database'),))


@pytest.mark.parametrize('result_payload,actual_rows', [
    ('[]', 0),
    ('[{}, {}]', 2),
])
def test_result_not_exactly_one_row_raises_decode_error(result_payload, actual_rows):
    views = {'stat_database': {'json_column': 'stat_database', 'paged': False}}
    body = {'result': result_payload}
    with pytest.raises(DecodeError) as excinfo:
        decode_merged(body, views)
    message = str(excinfo.value)
    assert '1' in message
    assert str(actual_rows) in message


def test_missing_view_column_raises_decode_error():
    views = {'stat_database': {'json_column': 'stat_database', 'paged': False}}
    body = {'result': json.dumps([{'locks': '[]'}])}
    with pytest.raises(DecodeError):
        decode_merged(body, views)


def test_decode_error_is_mapping_error_and_not_retryable():
    assert issubclass(DecodeError, MappingError)
    assert not is_retryable(DecodeError('坏了'))


# -- 分页拼接 -------------------------------------------------------------

def _page(view_rows):
    return {'result': json.dumps([view_rows])}


def test_merge_pages_concatenates_only_paged_views():
    views = {
        'stat_user_tables': {'json_column': 'stat_user_tables', 'paged': True},
        'stat_database': {'json_column': 'stat_database', 'paged': False},
    }
    page1 = _page({
        'stat_user_tables': '[{"relname":"t1"}]',
        'stat_database': '[{"datname":"lz"}]',
    })
    page2 = _page({
        'stat_user_tables': '[{"relname":"t2"}]',
        'stat_database': '[{"datname":"lz"}]',
    })
    merged = merge_pages([page1, page2], views)
    assert len(merged.rows_by_key['stat_user_tables']) == 2
    assert merged.rows_by_key['stat_user_tables'][1]['relname'] == 't2'
    # 固定视图只取第一页，不累加
    assert len(merged.rows_by_key['stat_database']) == 1


def test_merge_pages_single_page_equals_page():
    views = {
        'stat_user_tables': {'json_column': 'stat_user_tables', 'paged': True},
        'stat_database': {'json_column': 'stat_database', 'paged': False},
    }
    page1 = _page({
        'stat_user_tables': '[{"relname":"t1"}]',
        'stat_database': '[{"datname":"lz"}]',
    })
    merged = merge_pages([page1], views)
    assert len(merged.rows_by_key['stat_user_tables']) == 1
    assert len(merged.rows_by_key['stat_database']) == 1


def test_merge_pages_with_no_page_raises_decode_error():
    views = {'stat_database': {'json_column': 'stat_database', 'paged': False}}
    with pytest.raises(DecodeError):
        merge_pages([], views)


# -- 映射表里的 merged_views 段 -------------------------------------------

def test_merged_views_covers_every_view_of_merged_sql(merged_views):
    merged_sql = _load(MERGED_SQL_PATH)
    sql_keys = [batch['sql_key'] for batch in merged_sql['batches']]
    assert sorted(merged_views) == sorted(sql_keys)
    assert len(merged_views) == 11


def test_merged_views_entries_have_the_three_fields(merged_views):
    for name, spec in merged_views.items():
        assert spec['json_column'] == name
        assert isinstance(spec['paged'], bool)
        assert 'sql_merge.sql' in spec['sql_ref']['catalog']


def test_merged_views_paged_flags_come_from_merged_sql_paging(merged_views):
    merged_sql = _load(MERGED_SQL_PATH)
    paging_views = merged_sql.get('paging', {}).get('views', {})
    for name, spec in merged_views.items():
        expected = bool(paging_views.get(name, {}).get('paged', False))
        assert spec['paged'] == expected, name
    assert sorted(n for n, s in merged_views.items() if s['paged']) == [
        'dbstat_sum', 'stat_user_tables',
    ]


def test_existing_shapes_and_rules_are_untouched():
    """既有 11 个 cmd_rows_* 形状与 98 条规则一个字都不能变。"""
    payload = _load(RULES_PATH)
    cmd_shapes = [
        (name, shape) for name, shape in payload['shapes'].items()
        if name.startswith('cmd_rows_')
    ]
    assert len(cmd_shapes) == 11
    for name, shape in cmd_shapes:
        assert shape['kind'] == 'rows_from_result', name
        assert shape['payload_path'] == ['result'], name
    assert len(payload['rules']) == 98


# -- 既有 per-view 形态保持可解 -------------------------------------------

def test_old_per_view_form_still_decodes():
    """旧形态（``result`` 直接是行列表）必须原样解，不能被合并形态的改动带坏。"""
    payload = _load(RULES_PATH)
    rule = None
    for item in payload['rules']:
        if item['metric_code'] == 'gaussdb.Deadlocks':
            rule = item
            break
    assert rule is not None
    steps = _load(os.path.join(RESPONSE_DIR, rule['evidence']['sample_file']))
    body = steps[rule['evidence']['terminal_step']]['response_body']
    shape = payload['shapes'][rule['shape']]
    ctx = decode(body, shape, rule['locate'], now_ms=1760000000000)
    assert ctx.rows
    assert 'deadlocks' in ctx.rows[0]


def test_old_per_view_form_is_not_accepted_as_merged_form():
    """旧形态的 ``result`` 有多行 → 走合并路径必须报错，不能「猜」。"""
    views = {'stat_database': {'json_column': 'stat_database', 'paged': False}}
    body = {'result': '[{"a":1},{"a":2}]'}
    with pytest.raises(DecodeError):
        decode_merged(body, views)


def test_package_per_view_path_replays_every_command_center_sample():
    """包内的 per-view 解码路径逐条回放旧留档，行数必须与留档一致。

    这是「复制映射层没改坏」的守卫：合并形态的改造只应新增一条路径，
    既有路径的行为一个字都不能变。
    """
    payload = _load(RULES_PATH)
    checked = 0
    for rule in payload['rules']:
        if rule['channel'] != 'command_center_sql':
            continue
        evidence = rule['evidence']
        steps = _load(os.path.join(RESPONSE_DIR, evidence['sample_file']))
        body = steps[evidence['terminal_step']]['response_body']
        shape = payload['shapes'][rule['shape']]
        ctx = decode(body, shape, rule['locate'], now_ms=1760000000000)
        expected = evidence.get('observed', {}).get('row_count')
        if expected is None:
            continue
        assert len(ctx.rows) == expected, rule['metric_code']
        checked += 1
    assert checked >= 80, checked


# --- 重试判定（§5.7 重试表）------------------------------------------
#
# requests 的 Timeout / ConnectionError 在 requests 里是一组**子类**：
# ReadTimeout / ConnectTimeout 是 Timeout 的子类，ProxyError / SSLError 等
# 是 ConnectionError 的子类。requests 实际抛出的是这些子类，所以判定必须
# 走基类链，不能用类名精确相等 —— 否则最常见的瞬时故障会被判成不可重试，
# 直接中止本轮而不是退避重试。

import requests

from tpops_kafka_collector.errors import (
    AuthError,
    ProduceError,
    ProtocolError,
    TransientError,
)


@pytest.mark.parametrize('exc', [
    requests.exceptions.Timeout(),
    requests.exceptions.ReadTimeout(),
    requests.exceptions.ConnectTimeout(),
    requests.exceptions.ConnectionError(),
    requests.exceptions.ProxyError(),
])
def test_requests_timeout_and_connection_errors_are_retryable(exc):
    """requests 的超时/连接异常（含子类）都应判为可重试。"""
    assert is_retryable(exc) is True


@pytest.mark.parametrize('exc', [
    AuthError(),
    ProtocolError(),
    MappingError(),
])
def test_platform_errors_defined_as_non_retryable_stay_non_retryable(exc):
    """§5.7 里标为不可重试的异常不得被基类链判定改成可重试。"""
    assert is_retryable(exc) is False


def test_transient_and_produce_errors_are_retryable():
    assert is_retryable(TransientError()) is True
    assert is_retryable(ProduceError()) is True


def test_read_timeout_would_have_been_misclassified_by_exact_name_match():
    """回归锚点：类名精确相等会把 ReadTimeout 判成不可重试。

    这个断言固定住「必须走基类链」这个实现约束 —— 若有人把判定改回
    `type(exc).__name__ in (...)`，本用例立刻失败。
    """
    assert 'ReadTimeout' != 'Timeout'
    assert issubclass(requests.exceptions.ReadTimeout, requests.exceptions.Timeout)
    assert is_retryable(requests.exceptions.ReadTimeout()) is True


# -- 跨视图比率（ratio_across）--------------------------------------------
#
# **这个文件守的是「静默出 0」**：跨视图比率的分子分母来自**两个不同的视图**，
# 只解一个视图的话分母取不到 → 比率恒为 0，且**不报错**。
# 回归样本是真实留档 ``data/merged_sql/sql_merge_probe_result.json``。

RATIO_RULES = ('gaussdb.Total_pct', 'GAUSSDB.session_usage')

# 留档里的标准比值（分子/分母）—— 开发方案 §4.2.4 与执行合同里的验收数字。
CANONICAL_NUMERATOR = 23
CANONICAL_DENOMINATOR = 29


class _RatioRule(object):
    """规则引擎只认 ``value_rule``（跨视图比率不涉及维度绑定）。"""

    def __init__(self, metric_code, params):
        self.metric_code = metric_code
        self.value_rule = {'type': 'ratio_across', 'params': params}
        self.dimension_bindings = {}


def _ratio_params(metric_code):
    """从映射表里取这条指标的比值参数（分子/分母各来自哪个视图的哪一列）。"""
    for rule in _load(RULES_PATH)['rules']:
        if rule['metric_code'] == metric_code:
            return rule['business']['value_rule']['params']
    raise AssertionError('映射表里没有 %s' % metric_code)


def _archive_sides(node):
    """从留档的原始行里算出分子与分母（不借用实现里的算术）。"""
    row = json.loads(node['result'])[0]
    numerator = sum(item['cnt'] for item in json.loads(row['activity_agg']))
    denominator = sum(item['numbackends'] for item in json.loads(row['stat_database']))
    return numerator, denominator


@pytest.fixture(scope='module')
def probe_nodes():
    """留档里的每个 master 节点（一个节点 = 一次合并 SQL 的返回）。"""
    return _load(PROBE_PATH)['nodes']


@pytest.mark.parametrize('metric_code', RATIO_RULES)
def test_cross_view_ratio_equals_the_ratio_derived_from_the_archive(
    probe_nodes, merged_views, metric_code
):
    """跨视图比率必须等于留档原始行算出的比值，而不是 0。

    期望值从**留档自己的行**推出（分子=activity_agg 的 cnt 之和，
    分母=stat_database 的 numbackends 之和）—— 不复制实现的算术，
    这样实现的算错了这里也会跟着错。
    """
    params = _ratio_params(metric_code)
    numerator_spec = params['numerator']
    denominator_spec = params['denominator']
    # 分子分母确实来自两个不同的视图 —— 少解一个就恒为 0
    assert numerator_spec['sql_key'] != denominator_spec['sql_key']

    for node_name in sorted(probe_nodes):
        node = probe_nodes[node_name]
        merged = decode_merged({'result': node['result']}, merged_views)
        ctx = merged.by_sql_key[numerator_spec['sql_key']]

        results = evaluate(_RatioRule(metric_code, params), ctx)
        assert results, (metric_code, node_name)

        # 期望值：直接从留档原始行算
        numerator, denominator = _archive_sides(node)
        assert numerator > 0 and denominator > 0

        value = results[0][1]
        assert value == round(numerator / denominator, 2), (metric_code, node_name, value)
        assert value != 0, (metric_code, node_name)


def test_cross_view_ratio_on_the_measured_node_is_23_over_29(probe_nodes, merged_views):
    """留档里的标准比值 23/29 —— 两条比值指标都拿到 0.79，不是 0。"""
    params = _ratio_params('gaussdb.Total_pct')
    canonical_nodes = [
        name for name in sorted(probe_nodes)
        if _archive_sides(probe_nodes[name]) == (CANONICAL_NUMERATOR, CANONICAL_DENOMINATOR)
    ]
    assert canonical_nodes, '留档里没有 23/29 的节点了'

    for node_name in canonical_nodes:
        node = probe_nodes[node_name]
        merged = decode_merged({'result': node['result']}, merged_views)
        ctx = merged.by_sql_key[params['numerator']['sql_key']]
        for metric_code in RATIO_RULES:
            rule_params = _ratio_params(metric_code)
            value = evaluate(_RatioRule(metric_code, rule_params), ctx)[0][1]
            assert value == round(CANONICAL_NUMERATOR / CANONICAL_DENOMINATOR, 2)
            assert value == 0.79
            assert value != 0


# --- 告警投递单位：details 缺失时也必须存在（空数组，不是省略）-----------

def test_alarm_tags_always_carry_details_array():
    """规格「告警消息的投递单位」：`details` 缺失时 SHALL 输出空数组 `[]`，SHALL NOT 省略该字段。

    省略字段会让客户侧要写「有就取、没有就当空」的分支；
    统一输出 `[]` 才能让客户侧统一解析。
    """
    from tpops_kafka_collector.mapping.message import _alarm_tags

    tags_without = _alarm_tags({'alarm_id': '2007010001'}, [])
    assert 'details' in tags_without, 'details 为空时被省略了 —— 规格要求输出 []'
    assert tags_without['details'] == []

    tags_none = _alarm_tags({'alarm_id': '2007010001'}, None)
    assert tags_none['details'] == []

    tags_with = _alarm_tags({'alarm_id': '2007010001'}, [{'node_id': 'n1', 'host_ip': '1.2.3.4'}])
    assert tags_with['details'] == [{'node_id': 'n1', 'host_ip': '1.2.3.4'}]
