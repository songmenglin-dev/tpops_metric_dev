"""把实测 response_body 解成规则引擎要的 RuleContext。

**为什么需要这一层**：映射规则里写的是"取哪个平台的哪个列"，
但采集端拿到的第一手数据是 HTTP 响应体 —— 中间隔着
「命令中心 result 是 JSON 字符串」「监控指标是按组件展开的 series」
「诊断接口返回的是列表」这些结构差异。缺了这一层，
每个采集器都要各写一遍解包，规则里的列名也就无从校验。

映射条件写在 ``data/mapping/response_rules.json``：
- **shape**（形状）决定"从响应体的哪一段取"；
- **locate**（定位）决定"取这一段里的哪部分"，是逐指标的条件。

本模块只认这两样，不含任何业务计算 —— 业务计算在 ``rules.py``。

**两条解码路径**（对应设计决策 3）：

- 路径 P（既有）：``decode(body, shape, locate)`` —— ``result`` 直接是行列表
  （旧的 11 条命令中心 SQL 形态、监控指标、诊断、实例接口）。
- 路径 M（新增，生产路径）：``decode_merged(body, views)`` / ``merge_pages(...)``
  —— ``result`` 是**一行**，该行的每个键是一个视图名，值是该视图的 JSON 字符串
  （「一条合并 SQL」的形态）。

两条路径**不共用形状**：一个 shape 只对应一种 body，不靠自动探测区分。
把合并形态当行列表用会**静默产出 0**（82 个 B 类指标全错且不报错），
详见 ``changes/tpops-kafka-collector/specs/mapping-and-rules/spec.md``。
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple  # noqa: F401  （Dict/Tuple 仅用于 # type: 注释）

from .rules import RuleContext
from ..errors import DecodeError

__all__ = ['DecodeError', 'MergedContexts', 'decode', 'decode_merged', 'merge_pages']

# ``DecodeError`` 的唯一实现在 ``tpops_kafka_collector.errors``（继承 ``MappingError``，
# 不可重试）；这里只是再导出，保持 ``from ...mapping.decode import DecodeError``
# 的旧引用可用 —— 同名不同类会让 ``except`` 抓不住。


def decode(
    body: Any,
    shape: Mapping[str, Any],
    locate: Optional[Mapping[str, Any]] = None,
    now_ms: int = 0,
) -> RuleContext:
    """按 shape 声明把响应体解成 RuleContext。

    :param body: HTTP 响应体（已 json.loads 的 dict）
    :param shape: 形状声明，含 ``kind``
    :param locate: 逐指标定位条件
    :param now_ms: 采集完成时刻（毫秒），缺值回退时用它
    """
    locate = locate or {}
    kind = shape.get('kind')
    if kind == 'rows_from_result':
        rows = _rows_from_result(body, shape)
        return RuleContext(rows=tuple(rows), now_ms=now_ms)
    if kind == 'rows_from_collection':
        collection = shape.get('collection') or locate.get('collection')
        if not collection:
            raise DecodeError('rows_from_collection 形状缺 collection')
        rows = _as_rows(_dig(body, [collection], shape))
        return RuleContext(
            rows=tuple(rows),
            collections={collection: tuple(rows)},
            now_ms=now_ms,
        )
    if kind == 'series_from_metric_infos':
        series, timestamps = _series_from_metric_infos(body, locate)
        return RuleContext(
            series=series,
            timestamps=timestamps,
            now_ms=now_ms,
        )
    if kind == 'instance_from_list':
        collection = shape.get('collection') or locate.get('collection')
        rows = _as_rows(_dig(body, [collection], shape))
        instance = dict(rows[0]) if rows else {}
        return RuleContext(
            instance=instance,
            rows=tuple(rows),
            collections={collection: tuple(rows)},
            now_ms=now_ms,
        )
    raise DecodeError(f'未知的解码形状：{kind}')


# -- 合并形态（路径 M）--------------------------------------------------

@dataclass(frozen=True)
class MergedContexts:
    """「一条合并 SQL」的解码结果。

    - ``by_sql_key``：视图名 → 该视图的 ``RuleContext``（``rows`` 是该视图的行）；
    - ``rows_by_key``：视图名 → 该视图的行元组（跨视图规则直接用这个）。

    ``by_sql_key`` 里每个上下文的 ``rows_by_key`` **也填好了全部视图**，
    这样 ``ratio_across`` 这类跨视图规则不必再单独拿一份数据。
    """

    by_sql_key: Mapping[str, RuleContext]
    rows_by_key: Mapping[str, Tuple[Mapping[str, Any], ...]]


def decode_merged(
    body: Any,
    views: Mapping[str, Any],
    now_ms: int = 0,
) -> MergedContexts:
    """把「一行 N 个 json_agg 列」的合并返回体解成每个视图的行列表。

    :param body: HTTP 响应体（``{"result": "<平台的 JSON 字符串>"}``）
    :param views: ``merged_views`` 段：视图名 → ``{json_column, paged, sql_ref}``
    :param now_ms: 采集完成时刻（毫秒）

    形态（实测）::

        result = '[{"stat_database":"[{\\"datname\\":\\"lz\\",...}]", ...}]'

    ``result`` 解析后是**恰好一行**，该行的每个键是一个视图名，值是该视图的
    JSON 字符串。**不能把这一行当行列表返回** —— 那样每个视图的列拿到的是
    一整段 JSON 字符串，取值时全部变成 0/None/空列表且不报错。

    - 视图的列值为 ``''`` 或 ``null``（``json_agg`` 零行 → NULL → 平台渲染成空串）
      → 该视图解成空列表 ``[]``，不抛异常；
    - 列值不是合法 JSON → 抛 ``DecodeError``（不静默返回空列表）；
    - ``result`` 不是恰好 1 行 → 抛 ``DecodeError``，并说明期望与实际行数；
    - ``views`` 为空 → 抛 ``DecodeError``（**不静默返回全零**）。

    **空视图集必须报错**：它通常意味着 ``merged_views`` 段没加载到（映射表缺失
    或版本不匹配）。静默返回空上下文会让 82 个 B 类指标**全部变成 0**且不报错。
    """
    if not views:
        raise DecodeError('视图集为空：merged_views 段没有声明任何视图，无法解码合并形态')
    row = _merged_row(body)
    rows_by_key = {}  # type: Dict[str, Tuple[Mapping[str, Any], ...]]
    for name in views:
        column = _json_column_of(views[name], name)
        rows_by_key[name] = _rows_of_view(row, name, column)
    return _build_merged_contexts(rows_by_key, now_ms)


def merge_pages(
    bodies: Sequence[Any],
    views: Mapping[str, Any],
    now_ms: int = 0,
) -> MergedContexts:
    """把多页返回体按视图拼成完整视图行列表。

    **只有声明 ``paged = true`` 的视图做跨页拼接**；固定视图只取第一页
    （每页都会重复返回同样的全量数据，累加会翻倍）。

    拼接放在映射层而不是采集端，是因为 ``row_policy: sum`` 的语义是
    「整视图求和」—— 逐页解码逐页求值会**按页出值**，表一多就静默出错。
    """
    if not bodies:
        raise DecodeError('没有可拼接的返回体')
    per_page = [decode_merged(body, views, now_ms) for body in bodies]
    rows_by_key = {}  # type: Dict[str, Tuple[Mapping[str, Any], ...]]
    for name in views:
        if _is_paged(views[name]):
            merged = []  # type: List[Mapping[str, Any]]
            for page in per_page:
                merged.extend(page.rows_by_key.get(name, ()))
            rows_by_key[name] = tuple(merged)
        else:
            rows_by_key[name] = per_page[0].rows_by_key.get(name, ())
    return _build_merged_contexts(rows_by_key, now_ms)


def _build_merged_contexts(
    rows_by_key: Mapping[str, Tuple[Mapping[str, Any], ...]],
    now_ms: int,
) -> MergedContexts:
    """给每个视图造一个上下文，并把**全部视图**的行填进 ``rows_by_key``。"""
    by_sql_key = {}  # type: Dict[str, RuleContext]
    for name, rows in rows_by_key.items():
        by_sql_key[name] = RuleContext(
            rows=rows,
            rows_by_key=rows_by_key,
            now_ms=now_ms,
        )
    return MergedContexts(by_sql_key=by_sql_key, rows_by_key=rows_by_key)


def _merged_row(body: Any) -> Mapping[str, Any]:
    """取出合并形态里的那一行（``result`` 解析后必须恰好一行）。"""
    if not isinstance(body, Mapping):
        raise DecodeError(f'合并形态的响应体必须是对象，实际是 {type(body).__name__}')
    if 'result' not in body:
        raise DecodeError('合并形态的响应体里没有 result')
    payload = body['result']
    if isinstance(payload, (bytes, str)):
        text = payload.decode('utf-8') if isinstance(payload, bytes) else payload
        if not text.strip():
            raise DecodeError('合并形态的 result 是空字符串，期望 1 行，实际 0 行')
        try:
            payload = json.loads(text)
        except ValueError as exc:
            raise DecodeError(f'result 不是合法 JSON：{exc}')
    if not isinstance(payload, list):
        raise DecodeError(f'合并形态的 result 期望行列表，实际是 {type(payload).__name__}')
    if len(payload) != 1:
        raise DecodeError(
            f'合并形态的 result 期望 1 行，实际 {len(payload)} 行'
        )
    row = payload[0]
    if not isinstance(row, Mapping):
        raise DecodeError(f'合并形态的那一行必须是对象，实际是 {type(row).__name__}')
    return row


def _rows_of_view(
    row: Mapping[str, Any],
    name: str,
    column: str,
) -> Tuple[Mapping[str, Any], ...]:
    """取一个视图的行列表。空视图 → 空列表；坏 JSON → 抛异常。"""
    if column not in row:
        raise DecodeError(f'合并形态的那一行里没有视图 {name} 的列 {column}')
    value = row[column]
    if value is None:
        return ()
    if isinstance(value, (bytes, str)):
        text = value.decode('utf-8') if isinstance(value, bytes) else value
        if not text.strip():
            # json_agg 对零行返回 NULL，平台渲染成空字符串
            return ()
        try:
            value = json.loads(text)
        except ValueError as exc:
            raise DecodeError(f'视图 {name} 的列 {column} 不是合法 JSON：{exc}')
    if value is None:
        return ()
    if not isinstance(value, list):
        raise DecodeError(
            f'视图 {name} 的列 {column} 期望行列表，实际是 {type(value).__name__}'
        )
    rows = []  # type: List[Mapping[str, Any]]
    for item in value:
        if not isinstance(item, Mapping):
            raise DecodeError(
                f'视图 {name} 的列 {column} 里有非对象的行：{type(item).__name__}'
            )
        rows.append(item)
    return tuple(rows)


def _json_column_of(spec: Any, name: str) -> str:
    """视图在合并那一行里对应的列名（默认与视图同名）。"""
    if isinstance(spec, Mapping):
        column = spec.get('json_column')
        if column:
            return str(column)
    return name


def _is_paged(spec: Any) -> bool:
    """视图是否可增长（声明 ``paged = true``）。缺省为 false。"""
    if isinstance(spec, Mapping):
        return bool(spec.get('paged', False))
    return False


# -- 各形状的实现 -------------------------------------------------------

def _rows_from_result(body: Mapping[str, Any], shape: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    """命令中心的任务明细：``result`` 是 JSON 字符串，二次解析才是行列表。"""
    payload = _dig(body, list(shape.get('payload_path') or ['result']), shape)
    if payload is None:
        return []
    if isinstance(payload, (bytes, str)):
        text = payload.decode('utf-8') if isinstance(payload, bytes) else payload
        if not text.strip():
            return []
        try:
            payload = json.loads(text)
        except ValueError as exc:
            raise DecodeError(f'result 不是合法 JSON：{exc}')
    return _as_rows(payload)


def _series_from_metric_infos(
    body: Mapping[str, Any],
    locate: Mapping[str, Any],
) -> Any:
    """监控指标接口：按平台指标 id 取出该指标的 series 与 timestamps。"""
    metric_id = locate.get('metric')
    if not metric_id:
        raise DecodeError('series_from_metric_infos 定位缺 metric')
    series = {}  # type: Dict[str, List[Optional[float]]]
    timestamps = ()  # type: Any
    for info in body.get('metric_infos') or ():
        for metric in info.get('metrics') or ():
            if metric.get('name') != metric_id:
                continue
            for key, values in (metric.get('series') or {}).items():
                series[key] = list(values)
            points = metric.get('timestamps') or ()
            if points:
                timestamps = tuple(points)
    if not series:
        raise DecodeError(f'响应体里没有指标 {metric_id} 的 series')
    return series, timestamps


# -- 小工具 ---------------------------------------------------------------

def _dig(payload: Any, path: Sequence[str], shape: Mapping[str, Any]) -> Any:
    """按路径取值，缺失时报错（带 shape 名，便于定位是哪条形状不匹配）。"""
    current = payload
    walked = []  # type: List[str]
    for segment in path:
        walked.append(str(segment))
        if isinstance(current, Mapping):
            if segment not in current:
                raise DecodeError(
                    f'响应体里没有路径 {"/".join(walked)}（形状 {shape.get("kind")}）'
                )
            current = current[segment]
        elif isinstance(current, list):
            try:
                current = current[int(segment)]
            except (IndexError, ValueError):
                raise DecodeError(
                    f'响应体路径 {"/".join(walked)} 下标越界（形状 {shape.get("kind")}）'
                )
        else:
            raise DecodeError(
                f'响应体路径 {"/".join(walked)} 不是可下钻的容器（形状 {shape.get("kind")}）'
            )
    return current


def _as_rows(payload: Any) -> List[Mapping[str, Any]]:
    if payload is None:
        return []
    if not isinstance(payload, list):
        raise DecodeError(f'期望行列表，实际是 {type(payload).__name__}')
    rows = []  # type: List[Mapping[str, Any]]
    for item in payload:
        if isinstance(item, Mapping):
            rows.append(item)
    return rows
