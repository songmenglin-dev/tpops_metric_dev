"""PostgreSQL ``ORDER BY`` 口径的排序。

**为什么不能拿数值去比**：``top_n`` 的排序列有两类 —— 数值列
（``total_elapse_time``）与**时间戳文本**列（``pg_stat_activity.query_start``，
形如 ``2026-11-18 16:27:07.557567+08``）。后者按数值转换会全部退化成 0，
排序变成**空操作**（稳定排序 → 原顺序），「取 Top N」就变成了「取前 N 行」。

排序口径（与 PostgreSQL 一致）：

- 列里第一个非空值决定比较类型：数值 / 时间戳 / 文本；
- 数值按数值比；时间戳按**时间**比；文本按**字典序**比；
- **空值**：升序排在最后、降序排在最前（PostgreSQL 的 ``ORDER BY`` 默认）。

**空串按「空值」处理**：实测里平台把 NULL 的 ``query_start`` 渲染成空串
（``data/metric_response/`` 的逐指标样本是 ``''``，合并留档是 ``null`` ——
同一批行）。把空串当普通文本值会让「按时间取 Top 10」把没有时刻的行排在最前，
与 SQL 自己的 ``ORDER BY``（NULL 升序在后）正好相反。
"""
import datetime
import re
from typing import Any, Callable, List, Mapping, Optional, Sequence

from .rules_context import to_number

# GaussDB 实测的时间戳文本格式（``pg_stat_activity.query_start``）。
TIMESTAMP_FORMATS = (
    '%Y-%m-%d %H:%M:%S.%f%z',
    '%Y-%m-%d %H:%M:%S%z',
)

# 两位时区（平台实测的 ``+08``）→ 补成 ``+08:00``。
#
# Python 3.7.9 的 ``_strptime`` 里 ``%z`` 的正则是
# ``(?P<z>[+-]\d\d:?[0-5]\d(:?[0-5]\d(\.\d{1,6})?)?|Z)`` —— 必须匹配到时区，
# 且至少四位数字，``+08`` 匹配不上 → ``strptime`` 直接 ``ValueError``。
# 不归一化的话，平台的**真实数据**一条都解析不了，排序会退化成**字典序**：
# 同偏移下碰巧一致，**混偏移**时整列判成 text（或解析不了的行被当成最小时间）
# → **Top N 取错行，且不报任何错**。
#
# 锚在「日期 + 时间」之后，避免把日期末尾的 ``-18`` 误当成偏移。
_TWO_DIGIT_OFFSET = re.compile(
    r'^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)([+-])(\d{2})$'
)

# 最小时间（排序兜底）—— 带时区，才能和带时区的时间戳比较。
_MIN_TIMESTAMP = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)


def sort_rows(
    rows: Sequence[Mapping[str, Any]],
    sort_by: Optional[str],
    order: str = 'desc',
) -> List[Mapping[str, Any]]:
    """按 ``sort_by`` 排序（空值位置按 PostgreSQL 的 ``ORDER BY`` 默认口径）。"""
    if not sort_by:
        return list(rows)
    values = [row.get(sort_by) for row in rows]
    key_of = _sort_key(values)
    present = [(row, value) for row, value in zip(rows, values) if not is_missing(value)]
    missing = [row for row, value in zip(rows, values) if is_missing(value)]
    present.sort(key=lambda pair: key_of(pair[1]), reverse=(order == 'desc'))
    ordered = [row for row, _value in present]
    if order == 'desc':
        return missing + ordered
    return ordered + missing


def is_missing(value: Any) -> bool:
    """空值判定：``None`` 与**空串**都算空值。

    平台把 NULL 的时间戳渲染成空串（逐指标样本是 ``''``、合并留档是 ``null``），
    两者是同一批行 —— 排序时必须一起当空值处理。
    """
    if value is None:
        return True
    return isinstance(value, str) and not value.strip()


def _sort_key(values: Sequence[Any]) -> Callable[[Any], Any]:
    """按列里值的类型选一个可比较的键（数值 / 时间戳 / 字符串）。"""
    kind = _value_kind(values)
    if kind == 'number':
        return to_number
    if kind == 'timestamp':
        return _timestamp_key
    return lambda value: '' if value is None else str(value)


def _value_kind(values: Sequence[Any]) -> str:
    """列的比较类型 —— 由**第一个非空值**决定。"""
    for value in values:
        if is_missing(value):
            continue
        if isinstance(value, bool):
            return 'text'
        if isinstance(value, (int, float)):
            return 'number'
        text = str(value).strip()
        try:
            float(text)
            return 'number'
        except ValueError:
            pass
        return 'timestamp' if parse_timestamp(text) is not None else 'text'
    return 'text'


def _timestamp_key(value: Any) -> datetime.datetime:
    """时间戳文本 → 时间（解析不了按最小时间算，不抛异常）。"""
    parsed = parse_timestamp(str(value).strip())
    return parsed if parsed is not None else _MIN_TIMESTAMP


def parse_timestamp(text: str) -> Optional[datetime.datetime]:
    """解析 GaussDB 的时间戳文本；解析不了给 None。

    **先归一化两位时区**（``+08`` → ``+08:00``）——
    Python 3.7 的 ``%z`` 不认 ``+HH``，不归一化的话平台真实数据一条都解析不了。

    **只认带时区的文本**：``TIMESTAMP_FORMATS`` 里两个格式都带 ``%z`` →
    解析成功**一定**是 aware 的 datetime。**没有时区**的时间戳文本解析不了
    （返回 ``None``，那一列按文本比）—— 这里**不做**「按 UTC 兜底」：
    平台数据不带时区时按 UTC 猜，比按文本比更容易取错 Top N 行。
    """
    normalized = _normalize_offset(text.strip())
    for fmt in TIMESTAMP_FORMATS:
        try:
            parsed = datetime.datetime.strptime(normalized, fmt)
        except ValueError:
            continue
        return parsed
    return None


def _normalize_offset(text: str) -> str:
    """把结尾的**两位时区**补成 ``+HH:MM``（其余原样返回）。"""
    return _TWO_DIGIT_OFFSET.sub(r'\1\2\3:00', text)
