# -*- coding: utf-8 -*-
"""慢 SQL 脱敏与截断（wave-2b 任务 2.10）。

覆盖口径：
- 字符串字面量、数值字面量 → ``?``；
- ``NULL`` 等关键字**不替换**；
- 转义引号、字符串里的 ``--``、双引号标识符、美元引用等边界；
- 截断到 ``sql_text_max_length``（默认 512）并显式标记；
- ``SQL_MASKING_ENABLED`` 可关。
"""
from __future__ import absolute_import

import pytest

from tpops_kafka_collector.config import Config
from tpops_kafka_collector.mapping.masking import (
    DEFAULT_MAX_LENGTH,
    TRUNCATION_MARKER,
    SqlMaskingError,
    mask_sql_literals,
    sanitize_sql,
    sanitize_sql_with_config,
    truncate_sql,
)


# ---------------------------------------------------------------------------
# 字面量替换
# ---------------------------------------------------------------------------

def test_replaces_string_literal_with_placeholder():
    assert mask_sql_literals("select * from t where name = 'zhangsan'") == \
        'select * from t where name = ?'


def test_replaces_number_literal_with_placeholder():
    assert mask_sql_literals('select * from t where id = 1024') == \
        'select * from t where id = ?'


def test_replaces_decimal_and_exponent_literals():
    assert mask_sql_literals('select 1.5, 2e3, .25 from t') == 'select ?, ?, ? from t'


def test_replaces_hex_and_binary_literals():
    assert mask_sql_literals('select 0x1F, 0b101 from t') == 'select ?, ? from t'


def test_keeps_null_keyword():
    """NULL 是关键字不是字面量 —— 替换了语义就变了。"""
    assert mask_sql_literals('select * from t where a is null') == \
        'select * from t where a is null'


def test_keeps_boolean_and_type_keywords():
    assert mask_sql_literals('select true, false, int, text from t') == \
        'select true, false, int, text from t'


def test_keeps_number_inside_identifier():
    """``col1`` 里的 1 是标识符的一部分，不是数值字面量。"""
    assert mask_sql_literals('select col1, t2.a3 from t2') == 'select col1, t2.a3 from t2'


def test_replaces_escaped_quote_inside_literal():
    """标准 SQL 用两个单引号转义单引号。"""
    assert mask_sql_literals("select * from t where n = 'o''brien'") == \
        'select * from t where n = ?'


def test_replaces_backslash_escaped_literal():
    assert mask_sql_literals("select * from t where n = 'a\\'b'") == \
        'select * from t where n = ?'


def test_keeps_double_quoted_identifier():
    """双引号在 GaussDB/PG 里是标识符 quoting，不是字符串字面量。"""
    assert mask_sql_literals('select "my col" from t') == 'select "my col" from t'


def test_line_comment_inside_string_is_not_a_comment():
    """字符串里的 ``--`` 不是注释 —— 整条字符串应被替换成 ``?``。"""
    assert mask_sql_literals("select * from t where n = 'a -- b'") == \
        'select * from t where n = ?'


def test_comment_body_is_masked():
    """注释里可能带业务信息 —— 内容按脱敏处理，只留分隔符。"""
    assert mask_sql_literals('select 1 -- 客户 张三\nfrom t') == \
        'select ? -- ?\nfrom t'


def test_block_comment_body_is_masked():
    assert mask_sql_literals('select /* secret */ 1 from t') == 'select /* ? */ ? from t'


def test_dollar_quoted_string_is_masked():
    assert mask_sql_literals('select $tag$ abc $tag$ from t') == 'select ? from t'


def test_dollar_quoted_plain_string_is_masked():
    assert mask_sql_literals('select $$ abc $$ from t') == 'select ? from t'


def test_escape_string_prefix_is_masked():
    assert mask_sql_literals("select E'a\\nb' from t") == 'select ? from t'


def test_multiple_literals_all_masked():
    assert mask_sql_literals("select * from t where a = 'x' and b = 2 and c is null") == \
        'select * from t where a = ? and b = ? and c is null'


def test_empty_input():
    assert mask_sql_literals('') == ''


def test_keeps_dollar_placeholder_parameter():
    """``$1`` 是客户端参数占位符 —— 数字前有 ``$``，不是数值字面量。"""
    assert mask_sql_literals('select * from t where a = $1') == \
        'select * from t where a = $1'


def test_masks_multiline_string():
    text = "select 'multi\nline' from t"
    assert mask_sql_literals(text) == 'select ? from t'


def test_masks_insert_values():
    text = "insert into t(a, b) values ('x', 7)"
    assert mask_sql_literals(text) == 'insert into t(a, b) values (?, ?)'


# ---------------------------------------------------------------------------
# 截断
# ---------------------------------------------------------------------------

def test_short_text_is_not_truncated():
    text = 'select * from t'
    assert truncate_sql(text, DEFAULT_MAX_LENGTH) == text


def test_long_text_is_truncated_to_limit():
    text = 'select ' + ('a' * 1000) + ' from t'
    result = truncate_sql(text, 512)
    assert len(result) == 512
    assert result.endswith(TRUNCATION_MARKER)


def test_truncation_marker_is_inside_the_limit():
    """标记长度**算在**上限内 —— 输出总长不超过 max_length。"""
    text = 'x' * 100
    for limit in (1, 5, 13, 20, 50):
        result = truncate_sql(text, limit)
        assert len(result) <= limit
    assert truncate_sql(text, 20).endswith(TRUNCATION_MARKER)


def test_truncation_marker_is_visible():
    """截断后必须有明确标记，否则看起来像一条完整的 SQL。"""
    text = 'select ' + ('a' * 1000)
    result = truncate_sql(text, 512)
    assert TRUNCATION_MARKER in result
    assert result != text[:512]


def test_default_max_length_is_512():
    assert DEFAULT_MAX_LENGTH == 512


# ---------------------------------------------------------------------------
# 组合入口
# ---------------------------------------------------------------------------

def test_sanitize_masks_then_truncates():
    text = "select * from t where name = 'zhangsan' and id = 1024 " + ('pad ' * 300)
    result = sanitize_sql(text, masking_enabled=True, max_length=512)
    assert len(result) == 512
    assert 'zhangsan' not in result
    assert result.endswith(TRUNCATION_MARKER)


def test_sanitize_disabled_keeps_original_content():
    """脱敏关闭时**不做字面量替换**（截断仍生效 —— 那是长度上限，不是脱敏）。"""
    text = "select * from t where name = 'zhangsan'"
    result = sanitize_sql(text, masking_enabled=False, max_length=512)
    assert result == text


def test_sanitize_disabled_still_truncates():
    text = 'x' * 1000
    result = sanitize_sql(text, masking_enabled=False, max_length=100)
    assert len(result) == 100
    assert result.endswith(TRUNCATION_MARKER)


def test_sanitize_with_config_uses_config_values():
    config = Config(tpops_user='u', tpops_password='p',
                    sql_masking_enabled=False, sql_text_max_length=64)
    text = "select * from t where name = 'zhangsan'"
    result = sanitize_sql_with_config(text, config)
    assert result == text
    assert len(sanitize_sql_with_config('y' * 500, config)) == 64


def test_sanitize_with_config_masks_by_default():
    config = Config(tpops_user='u', tpops_password='p')
    assert config.sql_masking_enabled is True
    result = sanitize_sql_with_config("select * from t where n = 'a'", config)
    assert result == 'select * from t where n = ?'


# ---------------------------------------------------------------------------
# 入参校验
# ---------------------------------------------------------------------------

def test_none_input_raises():
    with pytest.raises(SqlMaskingError):
        mask_sql_literals(None)


def test_non_string_input_raises():
    with pytest.raises(SqlMaskingError):
        mask_sql_literals(123)


def test_non_positive_max_length_raises():
    with pytest.raises(SqlMaskingError):
        truncate_sql('select 1', 0)


def test_non_integer_max_length_raises():
    with pytest.raises(SqlMaskingError):
        truncate_sql('select 1', '512')


def test_boolean_max_length_raises():
    """``True`` 是 int 的子类 —— 但当长度用是错的，必须显式拒掉。"""
    with pytest.raises(SqlMaskingError):
        truncate_sql('select 1', True)
