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
