"""规则引擎的公共件：上下文、异常、数值/过滤/维度工具。

取值结果一律是三元组 ``(维度键值, 值, 数据时间戳)``：

- 维度为空表示该指标没有维度；
- 时间戳是客户字段 ``clock`` 的来源（A 类取与 value 同一下标的
  ``timestamps[i]``，其余取采集完成时刻）。
"""
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

LOGGER = logging.getLogger(__name__)

# 取值结果：(维度键值, 值, 数据时间戳)
MetricValue = Tuple[Dict[str, Any], Any, Optional[int]]


class RuleError(Exception):
    """规则无法执行（声明与数据/声明自身矛盾）。"""


@dataclass(frozen=True)
class RuleContext:
    """一次取数的输入。

    - ``rows``：该指标自己那个视图的行；
    - ``rows_by_key``：全部视图的行（跨视图比率用）；
    - ``series``：按组件展开的时序（A 类监控指标）；
    - ``collections``：按集合名展开的行（诊断优化接口）；
    - ``instance``：实例元数据（D 类实例接口）；
    - ``now_ms``：采集完成时刻（毫秒）—— 差分用的就是这个时刻。
    """

    rows: Tuple[Mapping[str, Any], ...] = ()
    rows_by_key: Mapping[str, Sequence[Mapping[str, Any]]] = field(default_factory=dict)
    series: Mapping[str, Sequence[Optional[float]]] = field(default_factory=dict)
    timestamps: Tuple[int, ...] = ()
    collections: Mapping[str, Sequence[Mapping[str, Any]]] = field(default_factory=dict)
    instance: Mapping[str, Any] = field(default_factory=dict)
    now_ms: int = 0


# -- 数值工具 -------------------------------------------------------------

def to_number(value: Any) -> float:
    """取不到数就当 0（**只在求和场景用** —— 单点取值不经过这里）。"""
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def to_int(value: Any) -> Optional[int]:
    """转整数；转不了给 None（**不给 0** —— 缺值不是 0）。"""
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def sum_values(values: Sequence[Any]) -> Any:
    """求和；结果是整数就返回整数（与客户指标表的「整数」类型对齐）。"""
    total = 0.0
    for value in values:
        total += to_number(value)
    if total.is_integer():
        return int(total)
    return total


# -- 过滤与维度 -----------------------------------------------------------

def matches_filter(row: Mapping[str, Any], filters: Mapping[str, Any]) -> bool:
    """``filter`` 匹配。

    - ``{"列": 值}`` → 等值；
    - ``{"列_contains": [词]}`` → **子串包含**（忽略大小写）。

    ``state_contains`` 是子串而不是等值：GaussDB 的会话状态是自由文本
    （``waiting on lock`` / ``WAITING ON LOCK`` …），等值匹配会一条都命中不了。
    """
    for key, expected in (filters or {}).items():
        if key.endswith('_contains'):
            actual = row.get(key[:-len('_contains')])
            text = '' if actual is None else str(actual).lower()
            keywords = expected if isinstance(expected, (list, tuple)) else [expected]
            if not any(str(word).lower() in text for word in keywords):
                return False
        elif row.get(key) != expected:
            return False
    return True


def row_dimensions(metric_rule: Any, row: Mapping[str, Any]) -> Dict[str, Any]:
    """按 ``dimension_bindings``（占位符 → 列名）取这一行的维度值。"""
    dims = {}
    for placeholder, column in (metric_rule.dimension_bindings or {}).items():
        dims[placeholder] = row.get(column)
    return dims


def dimension_key(dimensions: Mapping[str, Any]) -> str:
    """维度键（差分基线的键的一部分）—— 维度按键排序，与插入顺序无关。"""
    if not dimensions:
        return ''
    return '|'.join(f'{key}={dimensions[key]}' for key in sorted(dimensions))


def grouped_items(
    metric_rule: Any,
    params: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> List[Tuple[Dict[str, Any], Mapping[str, Any]]]:
    """行级规则的 ``group_by`` 执行入口 → ``[(维度, 行), ...]``。

    **``group_by`` 是行级规则（``raw`` / ``counter_delta`` 的 ``per_row``）的分组键
    声明**：它必须与 ``dimension_bindings`` 指向的列一致 —— 不一致说明声明自相矛盾
    （一边说按 ``relname`` 分组、一边把 ``schemaname`` 当维度），抛 ``RuleError``
    而不是静默取其中一个。分组键的值就是维度值。

    分组键重复（同名的两张表落在同一个键里）会记 WARNING：维度值不唯一，
    两条消息在幂等层会撞键 —— 显式报出来，不静默。
    """
    group_by = tuple((params or {}).get('group_by') or ())
    bindings = metric_rule.dimension_bindings or {}
    if not group_by:
        return [(row_dimensions(metric_rule, row), row) for row in rows]
    missing = sorted(set(bindings.values()) - set(group_by))
    if missing:
        raise RuleError(
            f'group_by {list(group_by)} 与维度绑定 {dict(bindings)} 不一致：分组键缺少列 {missing}'
        )
    groups = {}  # type: Dict[Tuple[Any, ...], List[Mapping[str, Any]]]
    for row in rows:
        key = tuple(row.get(column) for column in group_by)
        groups.setdefault(key, []).append(row)
    items = []  # type: List[Tuple[Dict[str, Any], Mapping[str, Any]]]
    for key, group_rows in groups.items():
        if len(group_rows) > 1:
            LOGGER.warning(
                '分组键重复：指标 %s 的 group_by=%s 命中 %d 行同名对象，维度值不唯一',
                metric_rule.metric_code, list(group_by), len(group_rows),
            )
        for row in group_rows:
            dims = dict(
                (placeholder, key[group_by.index(column)])
                for placeholder, column in bindings.items()
            )
            items.append((dims, row))
    return items


def point_timestamp(ctx: RuleContext, index: Optional[int]) -> int:
    """数据时间戳：A 类取与 value 同一下标的 ``timestamps[i]``，其余取采集完成时刻。

    实测里 timestamps 会出现 0（平台没给采集时刻），此时回退到采集时刻；
    客户字段 clock 是 Long，这里统一转成整数。
    """
    if index is not None and 0 <= index < len(ctx.timestamps):
        stamp = ctx.timestamps[index]
        if stamp:
            return int(stamp)
    return ctx.now_ms
