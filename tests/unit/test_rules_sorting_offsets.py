# -*- coding: utf-8 -*-
"""``mapping/rules_sorting.py`` 的时区偏移解析（平台实测格式）。

**背景**：平台返回的 ``pg_stat_activity.query_start`` 形如
``2026-11-18 16:27:07.557567+08`` —— **两位时区**。
Python 3.7.9 的 ``_strptime`` 里 ``%z`` 的正则是
``(?P<z>[+-]\\d\\d:?[0-5]\\d(:?[0-5]\\d(\\.\\d{1,6})?)?|Z)`` ——
**必须匹配到时区**（且至少四位数字），``+08`` 匹配不上 → ``strptime`` 直接 ``ValueError``。

后果（修复前）：``parse_timestamp`` 对**所有真实数据**返回 ``None`` →
``_value_kind`` 判成 ``'text'`` → **按字典序比**。
同偏移下字典序 == 时间序，所以 ``top_n`` 的输出**碰巧是对的**；
但一旦同一列里混了不同偏移，整列会被判成 timestamp 档、解析不了的行被当成
**最小时间** → 在 ``desc`` 排序里**排到最前** → **Top N 取错行**（不报任何错）。

这些测试守住「**按时间比**」这条口径在**平台真实格式**上成立。
"""
from __future__ import absolute_import

import datetime

from tpops_kafka_collector.mapping.rules_sorting import (
    parse_timestamp,
    sort_rows,
)

#: 平台实测的两种偏移写法（``+08`` 与 ``+08:00`` 都要能解析）。
PLATFORM_OFFSET = '2026-11-18 16:27:07.557567+08'
PLATFORM_OFFSET_COLON = '2026-11-18 16:27:07.557567+08:00'


def test_parse_timestamp_accepts_the_two_digit_offset_the_platform_returns():
    """平台的 ``+08`` 必须能解析 —— 它是**真实数据**的格式。"""
    parsed = parse_timestamp(PLATFORM_OFFSET)
    assert parsed is not None, '平台的两位时区格式解析不了'
    assert parsed.utcoffset() == datetime.timedelta(hours=8)


def test_two_digit_and_colon_offset_parse_to_the_same_instant():
    """``+08`` 与 ``+08:00`` 是**同一个时刻** —— 混在一列里也要一致。"""
    short = parse_timestamp(PLATFORM_OFFSET)
    long = parse_timestamp(PLATFORM_OFFSET_COLON)
    assert short is not None and long is not None
    assert short == long


def test_sort_rows_orders_the_two_digit_offset_format_by_time():
    """留档格式（``+08``）的列必须**按时间**排，不是按字典序。"""
    rows = [
        {'ts': PLATFORM_OFFSET},
        {'ts': '2026-11-13 13:39:11.749342+08'},
    ]
    ordered = [row['ts'] for row in sort_rows(rows, 'ts', 'asc')]
    assert ordered == [
        '2026-11-13 13:39:11.749342+08',
        PLATFORM_OFFSET,
    ]


def test_sort_rows_orders_mixed_offsets_by_time_not_by_text():
    """**混偏移**时按时间比 —— 这正是修复前会取错 Top N 的场景。

    构造一个「字典序与时间序结论相反」的对，**而且第一条用平台的两位时区**
    （否则修复前整列判成 text、根本走不到按时间比的那条路，这个测试就恒绿了）：

    - ``+08`` 的 16:27 = UTC **08:27**（更早）；
    - ``+00:00`` 的 09:00 = UTC **09:00**（更晚）。

    字典序上 ``09:00`` 排在 ``16:27`` 前面 → 按时间比与按字典序**结论相反**。
    """
    rows = [
        {'ts': '2026-11-18 16:27:07.000000+08'},       # UTC 08:27:07
        {'ts': '2026-11-18 09:00:00.000000+00:00'},   # UTC 09:00:00
    ]
    ordered = [row['ts'] for row in sort_rows(rows, 'ts', 'asc')]
    assert ordered == [
        '2026-11-18 16:27:07.000000+08',
        '2026-11-18 09:00:00.000000+00:00',
    ], '混偏移时没有按时间比 —— Top N 会取错行'


def test_sort_rows_orders_the_two_digit_format_before_the_colon_format_by_time():
    """两条都是平台格式，但**第一条**是两位时区 —— 修复前整列会判成 text。

    ``+08`` 的 16:27（UTC 08:27）早于 ``+00:00`` 的 09:00（UTC 09:00），
    字典序却相反 → 这条测试在修复前**必须红**。
    """
    rows = [
        {'ts': '2026-11-18 16:27:07.000000+08'},
        {'ts': '2026-11-18 09:00:00.000000+00:00'},
    ]
    ordered = [row['ts'] for row in sort_rows(rows, 'ts', 'asc')]
    assert ordered[0] == '2026-11-18 16:27:07.000000+08', (
        '没有按时间比 —— 两位时区的那条被当成文本比了'
    )


def test_sort_rows_handles_a_column_that_mixes_two_digit_and_colon_offsets():
    """``+08`` 与 ``+08:00`` 混在同一列 —— 顺序必须与全同偏移时一致。"""
    rows = [
        {'ts': '2026-11-18 16:27:07.000000+08'},
        {'ts': '2026-11-13 13:39:11.000000+08:00'},
    ]
    ordered = [row['ts'] for row in sort_rows(rows, 'ts', 'asc')]
    assert ordered == [
        '2026-11-13 13:39:11.000000+08:00',
        '2026-11-18 16:27:07.000000+08',
    ]


def test_a_two_digit_offset_is_not_confused_with_a_trailing_date_fragment():
    """归一化**不能**把日期末尾的 ``-18`` 当成时区偏移。"""
    parsed = parse_timestamp('2026-11-18 16:27:07')
    assert parsed is None, '没有时区的时间戳不该被当成可解析的带时区时间'


def test_sort_rows_keeps_missing_values_last_in_ascending_order():
    """空串/None 仍按 PostgreSQL ``ORDER BY`` 的默认口径排在最后（升序）。"""
    rows = [
        {'ts': PLATFORM_OFFSET},
        {'ts': ''},
        {'ts': None},
    ]
    ordered = [row['ts'] for row in sort_rows(rows, 'ts', 'asc')]
    assert ordered[-2:] == ['', None]
    assert ordered[0] == PLATFORM_OFFSET
