"""B 类（命令中心 SQL）指标的**独立期望值推导器**。

**为什么要有这份独立实现**：规则引擎（``tpops_kafka_collector/mapping/rules.py``）
是**被测对象**。期望值若从引擎的输出抄一份，测试就是恒真的 —— 引擎把
``state_contains`` 理解错了，期望值也会跟着错，什么缺陷都抓不到。
所以这里从**声明**出发，按 ``value_rule.params`` 的字面意思**重新实现一遍**求值：

- ``raw``：``row_policy: sum`` → 该列在所有行上求和；``per_row`` → 一行一条；
  ``first`` → 第一行
- ``raw_filtered``：按 ``filter`` 过滤后取第一行的 ``column``；``cast: int`` → 转整数
- ``raw_rows``：整行列表
- ``aggregate``：``filter`` 过滤 → ``fn: sum`` 求和；``group_by`` 按列分组（首次出现顺序）
- ``counter_delta``：跨轮差分；首轮无基线**不产出值**
- ``ratio_across``：分子 ÷ 分母；**分母取不到（为 0 或视图缺失）→ 不产出**；保留 2 位小数
- ``top_n``：按 ``sort_by`` 排序取前 ``limit`` 行，再投影 ``columns``

**本模块刻意不 import 任何生产包** —— 连解码都不借：合并留档自己解析，
期望值只依赖「留档原始行 + 规则声明」两样东西。

口径依据（不是抄引擎）：

- ``row_policy`` 三态见 ``data/mapping/response_rules.json`` 的
  ``business.scope.row.note``（per_row / sum / first）；
- 求和口径见 ``changes/tpops-kafka-collector/specs/mapping-and-rules/spec.md``
  「直接取值」场景（``row_policy: sum`` → 所有行求和）；
- ``counter_delta`` 首轮不产出值见同规格「累计计数的跨轮差分」；
- ``ratio_across`` 分子 ÷ 分母见同规格「跨视图比率」；**分母取不到（为 0 或视图缺失）
  → 不产出**（M-3）依据的是同规格「缺值不产出假数据」——SHALL NOT 用 0 代替缺失值；
  **保留 2 位小数**见 ``changes/tpops-kafka-collector/specs/delivery-and-state/spec.md``
  「值按客户指标表的数据类型序列化」（比率类 SHALL 保留 2 位小数）；
- ``top_n`` = 排序列 + 取前 N 行，见 ``docs/开发方案-GaussDB指标与告警采集.md`` §4.4。
"""

import datetime
import json
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

__all__ = ['ArchiveError', 'counter_baselines', 'derive_metric', 'parse_archive']


class ArchiveError(Exception):
    """留档结构或规则声明与预期不符。"""


# -- 留档解析（不借生产包的解码）----------------------------------------

def parse_archive(result_text: str) -> Dict[str, List[Dict[str, Any]]]:
    """把一个 master 节点的合并返回体解成「视图名 → 行列表」。

    合并形态是「一行 N 个 ``json_agg`` 列」：``result`` 解析后恰好一行，
    该行的每个键是一个视图名，值是该视图的 JSON 字符串。
    """
    payload = json.loads(result_text)
    if not isinstance(payload, list) or len(payload) != 1:
        raise ArchiveError(f'合并留档期望恰好 1 行，实际 {type(payload).__name__}')
    row = payload[0]
    if not isinstance(row, dict):
        raise ArchiveError('合并留档的那一行不是对象')
    views = {}
    for name, value in row.items():
        views[name] = _view_rows(name, value)
    return views


def _view_rows(name: str, value: Any) -> List[Dict[str, Any]]:
    """一个视图列的值 → 行列表。空串 / null → 空列表（json_agg 零行）。"""
    if value is None:
        return []
    if isinstance(value, str):
        if not value.strip():
            return []
        value = json.loads(value)
    if not isinstance(value, list):
        raise ArchiveError(f'视图 {name} 的列值不是行列表')
    return value


# -- 求值入口 -------------------------------------------------------------

def derive_metric(
    metric_code: str,
    rule: Mapping[str, Any],
    bindings: Optional[Mapping[str, str]],
    rows: Sequence[Mapping[str, Any]],
    rows_by_key: Mapping[str, Sequence[Mapping[str, Any]]],
    now_ms: int,
    baselines: Optional[Mapping[str, Tuple[float, int]]] = None,
) -> List[Tuple[Dict[str, Any], Any]]:
    """按声明推出一个指标的期望值。

    ``rows`` 是该指标**自己那个视图**的行；``rows_by_key`` 是全部视图的行
    （跨视图比率用）；``baselines`` 是累计计数器的差分基线（``None`` = 首轮）。
    返回 ``[(维度, 值), ...]``。
    """
    rule_type = (rule or {}).get('type')
    params = (rule or {}).get('params') or {}
    if rule_type == 'raw':
        return _derive_raw(params, bindings, rows)
    if rule_type == 'raw_filtered':
        return _derive_raw_filtered(params, bindings, rows)
    if rule_type == 'raw_rows':
        return [({}, [dict(row) for row in rows])]
    if rule_type == 'aggregate':
        return _derive_aggregate(params, bindings, rows)
    if rule_type == 'counter_delta':
        return _derive_counter_delta(params, bindings, rows, now_ms, baselines)
    if rule_type == 'ratio_across':
        return _derive_ratio(params, rows_by_key)
    if rule_type == 'top_n':
        return _derive_top_n(params, rows)
    raise ArchiveError(f'未支持的取值规则类型：{rule_type}（{metric_code}）')


def counter_baselines(
    rule: Mapping[str, Any],
    bindings: Optional[Mapping[str, str]],
    rows: Sequence[Mapping[str, Any]],
    now_ms: int,
) -> Dict[str, Tuple[float, int]]:
    """累计计数器的差分基线：``{维度键: (累计值, 采集时刻)}``。首轮拿它当基线。"""
    column = ((rule or {}).get('params') or {}).get('column')
    baselines = {}  # type: Dict[str, Tuple[float, int]]
    for row in rows:
        dims = _dimensions(bindings, row)
        baselines[_dimension_key(dims)] = (_num(row.get(column)), now_ms)
    return baselines


# -- 各规则类型的推导 -----------------------------------------------------

def _derive_raw(
    params: Mapping[str, Any],
    bindings: Optional[Mapping[str, str]],
    rows: Sequence[Mapping[str, Any]],
) -> List[Tuple[Dict[str, Any], Any]]:
    policy = params.get('row_policy', 'first')
    column = params.get('column')
    if policy == 'sum':
        return [({}, _sum(_num(row.get(column)) for row in rows))]
    if policy == 'per_row':
        return [(_dimensions(bindings, row), row.get(column)) for row in rows]
    if policy == 'first':
        if not rows:
            return []
        return [({}, rows[0].get(column))]
    raise ArchiveError(f'未知的 row_policy：{policy}')


def _derive_raw_filtered(
    params: Mapping[str, Any],
    bindings: Optional[Mapping[str, str]],
    rows: Sequence[Mapping[str, Any]],
) -> List[Tuple[Dict[str, Any], Any]]:
    matched = [row for row in rows if _matches(row, params.get('filter') or {})]
    if not matched:
        return []
    row = matched[0]
    value = row.get(params.get('column'))
    if params.get('cast') == 'int':
        value = _to_int(value)
    return [(_dimensions(bindings, row), value)]


def _derive_aggregate(
    params: Mapping[str, Any],
    bindings: Optional[Mapping[str, str]],
    rows: Sequence[Mapping[str, Any]],
) -> List[Tuple[Dict[str, Any], Any]]:
    matched = [row for row in rows if _matches(row, params.get('filter') or {})]
    group_by = params.get('group_by') or []
    value_column = params.get('value_column', 'cnt')
    fn = params.get('fn', 'sum')
    if fn != 'sum':
        raise ArchiveError(f'未实现的 aggregate fn：{fn}')
    if not group_by:
        return [({}, _sum(_num(row.get(value_column)) for row in matched))]
    groups = {}  # type: Dict[Tuple[str, ...], List[Mapping[str, Any]]]
    order = []  # type: List[Tuple[str, ...]]
    for row in matched:
        key = tuple(str(row.get(column)) for column in group_by)
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(row)
    results = []
    for key in order:
        group_rows = groups[key]
        results.append((
            _dimensions(bindings, group_rows[0]),
            _sum(_num(row.get(value_column)) for row in group_rows),
        ))
    return results


def _derive_counter_delta(
    params: Mapping[str, Any],
    bindings: Optional[Mapping[str, str]],
    rows: Sequence[Mapping[str, Any]],
    now_ms: int,
    baselines: Optional[Mapping[str, Tuple[float, int]]],
) -> List[Tuple[Dict[str, Any], Any]]:
    column = params.get('column')
    results = []
    for row in rows:
        dims = _dimensions(bindings, row)
        results.append((dims, _delta_value(dims, row.get(column), now_ms, baselines)))
    return results


def _delta_value(
    dims: Mapping[str, Any],
    counter: Any,
    now_ms: int,
    baselines: Optional[Mapping[str, Tuple[float, int]]],
) -> Optional[float]:
    """差分速率。首轮无基线 → 不产出值。"""
    if not baselines:
        return None
    previous = baselines.get(_dimension_key(dims))
    if previous is None:
        return None
    last_value, last_ms = previous
    current = _num(counter)
    if current < last_value:
        # 累计值回退 = 实例重启，丢弃本轮差分
        return None
    elapsed = (now_ms - last_ms) / 1000.0
    if elapsed <= 0:
        return None
    return (current - last_value) / elapsed


def _derive_ratio(
    params: Mapping[str, Any],
    rows_by_key: Mapping[str, Sequence[Mapping[str, Any]]],
) -> List[Tuple[Dict[str, Any], Any]]:
    """跨视图比率。

    **分母取不到（为 0 或视图缺失）→ 不产出**（不是 ``0.0``）：比率在分母为 0
    时没有定义，编一个 0 出去等于把「取不到」说成「真的是 0」。
    分母可用、分子为 0 → 比率就是 0（真的是没有活跃会话）。
    """
    denominator = _side_value(params.get('denominator') or {}, rows_by_key)
    if not denominator:
        return []
    numerator = _side_value(params.get('numerator') or {}, rows_by_key)
    # 比率类按投递规格保留 2 位小数
    return [({}, round(numerator / denominator, 2))]


def _side_value(
    spec: Mapping[str, Any],
    rows_by_key: Mapping[str, Sequence[Mapping[str, Any]]],
) -> float:
    rows = rows_by_key.get(spec.get('sql_key')) or []
    column = spec.get('value_column')
    fn = spec.get('fn', 'sum')
    if fn == 'sum':
        return _sum(_num(row.get(column)) for row in rows)
    if fn == 'max':
        values = [_num(row.get(column)) for row in rows]
        return max(values) if values else 0.0
    if fn == 'count':
        return float(len(rows))
    raise ArchiveError(f'未实现的 ratio fn：{fn}')


def _derive_top_n(
    params: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> List[Tuple[Dict[str, Any], Any]]:
    sort_by = params.get('sort_by')
    order = params.get('order', 'desc')
    limit = params.get('limit', 10)
    selected = _sorted_rows(rows, sort_by, order)[:limit]
    columns = params.get('columns')
    if columns:
        selected = [dict((key, row.get(key)) for key in columns) for row in selected]
    return [({}, selected)]


# -- 排序（PostgreSQL 的 ORDER BY 口径）-----------------------------------

# 最小时间（排序兜底）—— 带时区，才能和带时区的时间戳比较。
_MIN_TIMESTAMP = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

def _sorted_rows(
    rows: Sequence[Mapping[str, Any]],
    sort_by: Optional[str],
    order: str,
) -> List[Mapping[str, Any]]:
    """按 ``sort_by`` 排序。

    空值位置按 PostgreSQL 的 ``ORDER BY`` 默认口径：升序空值在后、降序空值在前
    （排序列来自 SQL 结果集，排序口径应与 SQL 自己的 ORDER BY 一致）。

    **空串按空值处理**：平台把 NULL 的 ``query_start`` 渲染成空串（逐指标样本
    是 ``''``、合并留档是 ``null`` —— 同一批行）。
    """
    if not sort_by:
        return list(rows)
    values = [row.get(sort_by) for row in rows]
    key_of = _sort_key_factory(values)
    present = [(row, value) for row, value in zip(rows, values) if not _is_empty(value)]
    missing = [row for row, value in zip(rows, values) if _is_empty(value)]
    present.sort(key=lambda pair: key_of(pair[1]), reverse=(order == 'desc'))
    ordered = [row for row, _value in present]
    if order == 'desc':
        return missing + ordered
    return ordered + missing


def _is_empty(value: Any) -> bool:
    """空值判定：``None`` 与**空串**都算空值（平台把 NULL 时间戳渲染成空串）。"""
    if value is None:
        return True
    return isinstance(value, str) and not value.strip()


def _sort_key_factory(values: Sequence[Any]):
    """按列里值的类型选一个可比较的键（数值 / 时间戳 / 字符串）。"""
    kind = _value_kind(values)
    if kind == 'number':
        return lambda value: _num(value)
    if kind == 'timestamp':
        return lambda value: _parse_timestamp(str(value).strip()) or _MIN_TIMESTAMP
    return lambda value: '' if value is None else str(value)


def _value_kind(values: Sequence[Any]) -> str:
    for value in values:
        if _is_empty(value):
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
        return 'timestamp' if _parse_timestamp(text) is not None else 'text'
    return 'text'


def _parse_timestamp(text: str) -> Optional[datetime.datetime]:
    for fmt in ('%Y-%m-%d %H:%M:%S.%f%z', '%Y-%m-%d %H:%M:%S%z'):
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


# -- 小工具 ---------------------------------------------------------------

def _matches(row: Mapping[str, Any], filters: Mapping[str, Any]) -> bool:
    """``filter`` 匹配：``{"列": 值}`` 等值；``{"列_contains": [词]}`` 忽略大小写子串。"""
    for key, expected in filters.items():
        if key.endswith('_contains'):
            column = key[:-len('_contains')]
            actual = row.get(column)
            text = '' if actual is None else str(actual).lower()
            words = expected if isinstance(expected, (list, tuple)) else [expected]
            if not any(str(word).lower() in text for word in words):
                return False
        elif row.get(key) != expected:
            return False
    return True


def _dimensions(
    bindings: Optional[Mapping[str, str]],
    row: Mapping[str, Any],
) -> Dict[str, Any]:
    dims = {}
    for placeholder, column in (bindings or {}).items():
        dims[placeholder] = row.get(column)
    return dims


def _dimension_key(dims: Mapping[str, Any]) -> str:
    if not dims:
        return ''
    return '|'.join(f'{key}={dims[key]}' for key in sorted(dims))


def _num(value: Any) -> float:
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


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _sum(values) -> Any:
    total = 0.0
    for value in values:
        total += value
    if float(total).is_integer():
        return int(total)
    return total
