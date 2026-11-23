"""``mapping/decode.py`` 的错误分支与边界（补测）。

守的是**解码失败要炸、不要静默出 0** 这条线：

- 合并形态（路径 M）的 ``result`` 结构不对 → ``DecodeError``，并说清
  「期望 1 行、实际几行」；
- 视图列是坏 JSON → ``DecodeError``（不是空列表）；
- 视图列为空串 / ``null`` / JSON ``null`` → 空列表（**不抛异常**，
  因为 ``json_agg`` 对零行就返回 NULL）；
- 旧路径（路径 P）的 ``result`` 为空 / 坏 JSON / 不是行列表 → 各自的行为；
- 跨页拼接：只拼接声明 ``paged = true`` 的视图，固定视图只取第一页。

每条测试都能被一个具体的小改动打红（见报告的「自检结论」）。
"""

import json

import pytest

from tpops_kafka_collector.errors import DecodeError
from tpops_kafka_collector.mapping.decode import decode, decode_merged, merge_pages


# -- 形状分派 -------------------------------------------------------------

def test_decode_rejects_a_collection_shape_without_a_collection_name():
    """``rows_from_collection`` 没声明 collection → 报错，不是空结果。"""
    with pytest.raises(DecodeError) as excinfo:
        decode({'rows': [{'a': 1}]}, {'kind': 'rows_from_collection'})
    assert 'collection' in str(excinfo.value)


def test_decode_rejects_an_unknown_shape():
    """未知形状必须报错 —— 静默返回空上下文等于把 82 个 B 类指标变成 0。"""
    with pytest.raises(DecodeError) as excinfo:
        decode({'result': '[]'}, {'kind': 'not_a_shape'})
    assert '未知的解码形状' in str(excinfo.value)


def test_decode_rejects_a_series_shape_without_a_metric_name():
    """``series_from_metric_infos`` 定位缺 metric → 报错。"""
    with pytest.raises(DecodeError) as excinfo:
        decode({'metric_infos': []}, {'kind': 'series_from_metric_infos'}, {})
    assert '缺 metric' in str(excinfo.value)


# -- 路径 P：``result`` 即行列表 ------------------------------------------

def test_decode_treats_a_null_result_as_no_rows():
    """``result`` 是 null → 0 行（不是报错，也不是一行 None）。"""
    ctx = decode({'result': None}, {'kind': 'rows_from_result'})
    assert ctx.rows == ()


def test_decode_treats_an_empty_result_string_as_no_rows():
    """``result`` 是空串 → 0 行。"""
    ctx = decode({'result': '   '}, {'kind': 'rows_from_result'})
    assert ctx.rows == ()


def test_decode_rejects_a_result_that_is_not_json():
    """``result`` 是坏 JSON → 报错，不静默当空列表。"""
    with pytest.raises(DecodeError) as excinfo:
        decode({'result': '[{坏'}, {'kind': 'rows_from_result'})
    assert '不是合法 JSON' in str(excinfo.value)


def test_decode_reports_a_missing_path_segment():
    """响应体里没有这条路径 → 报错并带出形状名。"""
    with pytest.raises(DecodeError) as excinfo:
        decode({}, {'kind': 'rows_from_result', 'payload_path': ['result']})
    assert '没有路径 result' in str(excinfo.value)


def test_decode_reports_a_list_index_out_of_range():
    """路径下标越界 → 报错（不是 IndexError 漏出去，也不是空结果）。"""
    with pytest.raises(DecodeError) as excinfo:
        decode(
            {'result': [{'a': 1}]},
            {'kind': 'rows_from_result', 'payload_path': ['result', '5']},
        )
    assert '下标越界' in str(excinfo.value)


def test_decode_reports_a_path_through_a_non_container():
    """路径要下钻一个标量 → 报错。"""
    with pytest.raises(DecodeError) as excinfo:
        decode(
            {'result': 5},
            {'kind': 'rows_from_result', 'payload_path': ['result', 'x']},
        )
    assert '不是可下钻的容器' in str(excinfo.value)


def test_decode_rejects_a_collection_that_is_not_a_row_list():
    """集合段不是行列表 → 报错（否则每条规则都取不到值且不报错）。"""
    with pytest.raises(DecodeError) as excinfo:
        decode({'items': {'a': 1}}, {'kind': 'rows_from_collection',
                                    'collection': 'items'})
    assert '期望行列表' in str(excinfo.value)


def test_decode_treats_a_null_collection_as_empty():
    """集合段是 null → 空列表，且 collections 里也是空的。"""
    ctx = decode({'items': None}, {'kind': 'rows_from_collection',
                                  'collection': 'items'})
    assert ctx.rows == ()
    assert ctx.collections == {'items': ()}


# -- 路径 M：合并形态 -----------------------------------------------------

def _merged_body(view_payloads):
    """造一个合并形态的响应体：``result`` 是「一行」的 JSON 字符串。"""
    return {'result': json.dumps([view_payloads], ensure_ascii=False)}


def test_decode_merged_rejects_a_body_that_is_not_an_object():
    with pytest.raises(DecodeError) as excinfo:
        decode_merged(['not', 'an', 'object'], {'v': {'json_column': 'v'}})
    assert '必须是对象' in str(excinfo.value)


def test_decode_merged_rejects_a_body_without_result():
    with pytest.raises(DecodeError) as excinfo:
        decode_merged({'data': []}, {'v': {'json_column': 'v'}})
    assert '没有 result' in str(excinfo.value)


def test_decode_merged_rejects_an_empty_result_string():
    with pytest.raises(DecodeError) as excinfo:
        decode_merged({'result': '  '}, {'v': {'json_column': 'v'}})
    assert '空字符串' in str(excinfo.value)


def test_decode_merged_rejects_a_result_that_is_not_json():
    with pytest.raises(DecodeError) as excinfo:
        decode_merged({'result': '[{坏'}, {'v': {'json_column': 'v'}})
    assert '不是合法 JSON' in str(excinfo.value)


def test_decode_merged_rejects_a_result_that_is_not_a_row_list():
    with pytest.raises(DecodeError) as excinfo:
        decode_merged({'result': '{"a": 1}'}, {'v': {'json_column': 'v'}})
    assert '期望行列表' in str(excinfo.value)


def test_decode_merged_reports_the_actual_row_count():
    """行数不对 → 报错并说清「期望 1 行、实际几行」。"""
    with pytest.raises(DecodeError) as excinfo:
        decode_merged({'result': '[]'}, {'v': {'json_column': 'v'}})
    assert '期望 1 行，实际 0 行' in str(excinfo.value)

    with pytest.raises(DecodeError) as excinfo:
        decode_merged({'result': '[{}, {}]'}, {'v': {'json_column': 'v'}})
    assert '期望 1 行，实际 2 行' in str(excinfo.value)


def test_decode_merged_rejects_a_row_that_is_not_an_object():
    with pytest.raises(DecodeError) as excinfo:
        decode_merged({'result': '[1]'}, {'v': {'json_column': 'v'}})
    assert '必须是对象' in str(excinfo.value)


def test_decode_merged_rejects_an_empty_view_set():
    """视图集为空 → 报错（静默返回空上下文 = 82 个指标全变 0 且不报错）。"""
    with pytest.raises(DecodeError) as excinfo:
        decode_merged(_merged_body({}), {})
    assert '视图集为空' in str(excinfo.value)


def test_decode_merged_rejects_a_missing_view_column():
    with pytest.raises(DecodeError) as excinfo:
        decode_merged(_merged_body({'other': '[]'}), {'v': {'json_column': 'v'}})
    assert '没有视图 v 的列 v' in str(excinfo.value)


def test_decode_merged_treats_a_null_view_column_as_empty_rows():
    """``json_agg`` 零行 → NULL → 平台渲染成 null → 空列表，不抛异常。"""
    merged = decode_merged(_merged_body({'v': None}), {'v': {'json_column': 'v'}})
    assert merged.rows_by_key['v'] == ()


def test_decode_merged_treats_an_empty_view_column_as_empty_rows():
    merged = decode_merged(_merged_body({'v': ''}), {'v': {'json_column': 'v'}})
    assert merged.rows_by_key['v'] == ()


def test_decode_merged_treats_a_json_null_view_column_as_empty_rows():
    """列值是 JSON 文本 ``null`` → 解出来是 None → 空列表。"""
    merged = decode_merged(_merged_body({'v': 'null'}), {'v': {'json_column': 'v'}})
    assert merged.rows_by_key['v'] == ()


def test_decode_merged_rejects_a_view_column_that_is_not_json():
    with pytest.raises(DecodeError) as excinfo:
        decode_merged(_merged_body({'v': '[{坏'}), {'v': {'json_column': 'v'}})
    assert '不是合法 JSON' in str(excinfo.value)


def test_decode_merged_rejects_a_view_column_that_is_not_a_row_list():
    with pytest.raises(DecodeError) as excinfo:
        decode_merged(_merged_body({'v': '"abc"'}), {'v': {'json_column': 'v'}})
    assert '期望行列表' in str(excinfo.value)


def test_decode_merged_rejects_a_view_column_with_a_non_object_row():
    with pytest.raises(DecodeError) as excinfo:
        decode_merged(_merged_body({'v': '[1]'}), {'v': {'json_column': 'v'}})
    assert '有非对象的行' in str(excinfo.value)


def test_decode_merged_reads_the_declared_json_column():
    """列名按声明的 ``json_column`` 取，不是按视图名。"""
    body = _merged_body({'jv': '[{"a": 1}]'})
    merged = decode_merged(body, {'v': {'json_column': 'jv'}})
    assert merged.rows_by_key['v'] == ({'a': 1},)


def test_decode_merged_falls_back_to_the_view_name_without_a_declared_column():
    """没声明 ``json_column`` → 列名回落到视图名。"""
    body = _merged_body({'v': '[{"a": 1}]'})
    merged = decode_merged(body, {'v': {}})
    assert merged.rows_by_key['v'] == ({'a': 1},)


def test_decode_merged_gives_every_view_the_rows_of_all_views():
    """每个视图的上下文里都带着**全部**视图的行（跨视图比率直接用）。"""
    body = _merged_body({'a': '[{"x": 1}]', 'b': '[{"y": 2}]'})
    merged = decode_merged(body, {'a': {}, 'b': {}})
    assert merged.by_sql_key['a'].rows_by_key['b'] == ({'y': 2},)
    assert merged.by_sql_key['b'].rows_by_key['a'] == ({'x': 1},)


# -- 跨页拼接 -------------------------------------------------------------

def test_merge_pages_rejects_an_empty_page_list():
    with pytest.raises(DecodeError) as excinfo:
        merge_pages([], {'v': {}})
    assert '没有可拼接的返回体' in str(excinfo.value)


def test_merge_pages_concatenates_only_the_paged_views():
    """可增长视图跨页拼接；固定视图只取第一页（累加会翻倍）。"""
    views = {'grow': {'paged': True}, 'fixed': {}}
    page1 = _merged_body({'grow': '[{"i": 1}]', 'fixed': '[{"i": 1}]'})
    page2 = _merged_body({'grow': '[{"i": 2}]', 'fixed': '[{"i": 2}]'})
    merged = merge_pages([page1, page2], views)
    assert merged.rows_by_key['grow'] == ({'i': 1}, {'i': 2})
    assert merged.rows_by_key['fixed'] == ({'i': 1},)


def test_merge_pages_of_a_single_page_is_that_page():
    views = {'grow': {'paged': True}}
    merged = merge_pages([_merged_body({'grow': '[{"i": 1}]'})], views)
    assert merged.rows_by_key['grow'] == ({'i': 1},)


def test_merge_pages_treats_a_non_object_view_spec_as_not_paged():
    """视图声明不是对象 → 按「不可增长」处理，只取第一页。"""
    page1 = _merged_body({'v': '[{"i": 1}]'})
    page2 = _merged_body({'v': '[{"i": 2}]'})
    merged = merge_pages([page1, page2], {'v': 'not-a-mapping'})
    assert merged.rows_by_key['v'] == ({'i': 1},)
