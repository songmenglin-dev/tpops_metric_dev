# -*- coding: utf-8 -*-
"""A/C/D 三条通道 16 个指标的**独立期望值推导器**。

**为什么要有这份独立实现**：规则引擎（``tpops_kafka_collector/mapping/rules.py``）
是**被测对象**。期望值若从引擎的输出抄一份，测试就是恒真的 —— 引擎把
``last_non_null`` 理解错了，期望值也会跟着错，什么缺陷都抓不到。
所以这里从**声明**出发，按 ``value_rule.params`` 的字面意思**重新实现一遍**求值。

**与 B 类的那份（``tools/expected_deriver.py``）的关系**：两份是**各自独立**的
推导实现，覆盖的通道不同 —— 那份管 B 类（命令中心 SQL，82 条），这份管
A/C/D（监控指标 11 条 + 诊断优化 4 条 + 实例接口 1 条）。两份都不 import
任何生产包：连解码都不借，留档自己解析。

口径依据（不是抄引擎）：

- ``series_point`` 的 ``position`` 取值语义见 ``response_rules.json`` 的
  ``business.scope.series`` 与 ``time_policy``；
- ``enum_map`` 是「把平台枚举值翻译成展示值」见
  ``changes/tpops-kafka-collector/specs/mapping-and-rules/spec.md``「枚举映射」；
- ``gauge`` 是「当前时刻 − 启动时刻」见同规格「由启动时刻推算运行时长」；
- ``collection_count`` / ``text_prefix_count`` / ``top_n`` 是集合口径，
  ``top_n`` = 排序列 + 取前 N 行（排序口径见开发方案 §4.4，
  **空值位置按 PostgreSQL 的 ORDER BY 默认**：升序在后、降序在前）。
"""

import datetime
import json
import os
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

__all__ = ['ArchiveError', 'derive_non_b_metrics', 'load_archive']

#: 推导用的「采集完成时刻」（毫秒）—— ``gauge`` 规则要用，必须固定下来。
#: 取**留档抓取之后**的时刻（留档抓于 2026-11-12），
#: 否则「运行时长 = 当前时刻 − 启动时刻」会算出负数。
NOW_MS = 1794500000000

#: A/C/D 三个通道。
NON_B_CHANNELS = ('monitor_metrics', 'diagnostic', 'instance')

#: 最小时间（排序兜底）—— 带时区，才能和带时区的时间戳比较。
_MIN_TIMESTAMP = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

#: GaussDB 实测的时间戳文本格式。
_TIMESTAMP_FORMATS = (
    '%Y-%m-%d %H:%M:%S.%f%z',
    '%Y-%m-%d %H:%M:%S%z',
)


class ArchiveError(Exception):
    """留档结构或规则声明与预期不符。"""


# -- 入口 -----------------------------------------------------------------

def derive_non_b_metrics(
    metric_rules: Mapping[str, Any],
    response_rules: Mapping[str, Any],
    archive_dir: str,
    now_ms: int = NOW_MS,
) -> Dict[str, Dict[str, Any]]:
    """推出 A/C/D 全部指标的期望值 → ``{指标编码: 条目}``。

    :param metric_rules: ``metric_rules.json`` 的内容
    :param response_rules: ``response_rules.json`` 的内容
    :param archive_dir: 逐指标留档目录
    :param now_ms: 采集完成时刻（毫秒）
    """
    shapes = response_rules.get('shapes') or {}
    responses = dict(
        (rule['metric_code'], rule) for rule in response_rules.get('rules') or ()
    )
    result = {}  # type: Dict[str, Dict[str, Any]]
    for rule in metric_rules.get('rules') or ():
        channel = (rule.get('source') or {}).get('source')
        if channel not in NON_B_CHANNELS:
            continue
        code = rule['metric_code']
        if code not in responses:
            raise ArchiveError('响应规则表里没有 %s 的定位条件' % code)
        response = responses[code]
        shape = shapes.get(response.get('shape'))
        if shape is None:
            raise ArchiveError('%s 的形状 %s 不存在' % (code, response.get('shape')))
        sample_file = _sample_file(response)
        archive = load_archive(os.path.join(archive_dir, sample_file))
        result[code] = {
            'value_rule_type': (rule.get('value_rule') or {}).get('type'),
            'channel': channel,
            'sample_file': sample_file,
            'expected': _expected_of(rule, response, shape, archive, now_ms),
        }
    return result


def load_archive(path: str) -> List[Mapping[str, Any]]:
    """读一份逐指标留档（调用步骤列表）。"""
    with open(path, encoding='utf-8') as handle:
        payload = json.load(handle)
    if not isinstance(payload, list) or not payload:
        raise ArchiveError('留档 %s 不是非空的步骤列表' % path)
    return payload


def _sample_file(response: Mapping[str, Any]) -> str:
    """留档文件名（响应规则的 evidence 段里带着）。"""
    evidence = response.get('evidence') or {}
    sample_file = evidence.get('sample_file')
    if not sample_file:
        raise ArchiveError('响应规则里没有 evidence.sample_file')
    return sample_file


# -- 分派 -----------------------------------------------------------------

def _expected_of(
    rule: Mapping[str, Any],
    response: Mapping[str, Any],
    shape: Mapping[str, Any],
    archive: Sequence[Mapping[str, Any]],
    now_ms: int,
) -> List[Dict[str, Any]]:
    """一个指标的期望值条目列表。"""
    channel = (rule.get('source') or {}).get('source')
    value_rule = rule.get('value_rule') or {}
    rule_type = value_rule.get('type')
    params = value_rule.get('params') or {}
    locate = response.get('locate') or {}

    if channel == 'monitor_metrics':
        return _monitor_expected(value_rule, params, locate, archive, now_ms)
    if channel == 'diagnostic':
        return _diagnostic_expected(value_rule, params, shape, locate, archive)
    if channel == 'instance':
        return _instance_expected(value_rule, params, shape, locate, archive)
    raise ArchiveError('未支持的通道：%s' % channel)


# -- A 类（监控指标接口）--------------------------------------------------

def _monitor_expected(
    value_rule: Mapping[str, Any],
    params: Mapping[str, Any],
    locate: Mapping[str, Any],
    archive: Sequence[Mapping[str, Any]],
    now_ms: int,
) -> List[Dict[str, Any]]:
    """A 类：按组件展开的时序 → 每个组件取一个点。"""
    body = _monitor_body(archive)
    series = _series_of(body, locate.get('metric'))
    position = params.get('position', 'last_non_null')
    rule_type = value_rule.get('type')
    results = []  # type: List[Dict[str, Any]]
    for key, values in series:
        point = _pick_point(values, position)
        if point is None:
            continue  # null 点不是 0 —— 跳过，不产出假数据
        results.append({
            'dimensions': {'COMPONENT_ID': key},
            'value': _monitor_value(rule_type, params, point, now_ms),
        })
    return results


def _monitor_value(
    rule_type: Optional[str],
    params: Mapping[str, Any],
    point: Any,
    now_ms: int,
) -> Any:
    """A 类三种规则类型的取值。"""
    if rule_type == 'series_point':
        return point
    if rule_type == 'enum_map':
        return _enum_value(params, point)
    if rule_type == 'gauge':
        return round(now_ms / 1000.0 - float(point), 2)
    raise ArchiveError('未支持的监控指标规则类型：%s' % rule_type)


def _enum_value(params: Mapping[str, Any], point: Any) -> Any:
    """枚举映射：命中就翻译；未命中按 ``default`` 处理。"""
    mapping = params.get('mapping') or {}
    key = _enum_key(point)
    if key in mapping:
        return mapping[key]
    if params.get('default', 'passthrough') == 'passthrough':
        return point
    return None


def _enum_key(value: Any) -> str:
    """枚举表的键：数值按整数形态查（``1.0`` 与 ``1`` 是同一个键）。"""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _monitor_body(archive: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    """取 ``is_latest_point_only=false`` 那一步的响应体（生产采集器固定用 false）。"""
    matched = []
    for step in archive:
        request_body = (step.get('request') or {}).get('body') or {}
        if str(request_body.get('is_latest_point_only')).lower() == 'false':
            matched.append(step)
    if len(matched) != 1:
        raise ArchiveError('留档里 is_latest_point_only=false 的调用不是 1 个')
    return matched[0].get('response_body') or {}


def _series_of(
    body: Mapping[str, Any],
    metric_id: Optional[str],
) -> List[Tuple[str, List[Any]]]:
    """从监控指标响应体里取出某个平台指标的 series（按响应里的顺序）。"""
    if not metric_id:
        raise ArchiveError('监控指标定位缺 metric')
    series = {}  # type: Dict[str, List[Any]]
    order = []  # type: List[str]
    for info in body.get('metric_infos') or ():
        for metric in info.get('metrics') or ():
            if metric.get('name') != metric_id:
                continue
            for key, values in (metric.get('series') or {}).items():
                if key not in series:
                    order.append(key)
                series[key] = list(values)
    if not series:
        raise ArchiveError('响应体里没有指标 %s 的 series' % metric_id)
    return [(key, series[key]) for key in order]


def _pick_point(values: Sequence[Any], position: str) -> Optional[Any]:
    """取时序上的一个点（值为 null 的点跳过）。"""
    if position in ('last_non_null', 'first_non_null'):
        indexes = range(len(values) - 1, -1, -1) if position == 'last_non_null' \
            else range(0, len(values))
        for index in indexes:
            value = values[index]
            if value is not None:
                return value
        return None
    if position == 'last':
        return values[-1] if values else None
    if position == 'first':
        return values[0] if values else None
    raise ArchiveError('未知的时序取值位置：%s' % position)


# -- C 类（诊断优化接口）--------------------------------------------------

def _diagnostic_expected(
    value_rule: Mapping[str, Any],
    params: Mapping[str, Any],
    shape: Mapping[str, Any],
    locate: Mapping[str, Any],
    archive: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """C 类：集合的条数 / 前缀分类计数 / Top N。"""
    collection = params.get('collection') or locate.get('collection') \
        or shape.get('collection')
    rows = _collection_rows(archive, collection)
    rule_type = value_rule.get('type')
    if rule_type == 'collection_count':
        value = len(rows)
    elif rule_type == 'text_prefix_count':
        value = _prefix_count(rows, params)
    elif rule_type == 'top_n':
        value = _top_n(rows, params)
    else:
        raise ArchiveError('未支持的诊断优化规则类型：%s' % rule_type)
    return [{'dimensions': {}, 'value': value}]


def _prefix_count(rows: Sequence[Mapping[str, Any]], params: Mapping[str, Any]) -> int:
    """按文本前缀给集合分类计数（忽略大小写、去掉首尾空白）。"""
    field = params.get('field', 'sql_text')
    prefixes = [str(prefix).lower() for prefix in (params.get('prefixes') or ())]
    count = 0
    for row in rows:
        text = str(row.get(field) or '').strip().lower()
        if any(text.startswith(prefix) for prefix in prefixes):
            count += 1
    return count


def _top_n(rows: Sequence[Mapping[str, Any]], params: Mapping[str, Any]) -> List[Any]:
    """按排序列取前 N 行（空值位置按 PostgreSQL 的 ORDER BY 默认）。"""
    ordered = _sort_rows(rows, params.get('sort_by'), params.get('order', 'desc'))
    selected = ordered[:params.get('limit', 10)]
    columns = params.get('columns')
    if columns:
        selected = [dict((key, row.get(key)) for key in columns) for row in selected]
    return [dict(row) for row in selected]


# -- D 类（实例接口）------------------------------------------------------

def _instance_expected(
    value_rule: Mapping[str, Any],
    params: Mapping[str, Any],
    shape: Mapping[str, Any],
    locate: Mapping[str, Any],
    archive: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """D 类：实例元数据里的一个字段。"""
    if value_rule.get('type') != 'instance_field':
        raise ArchiveError('未支持的实例接口规则类型：%s' % value_rule.get('type'))
    collection = locate.get('collection') or shape.get('collection') or 'instances'
    rows = _collection_rows(archive, collection)
    instance = dict(rows[0]) if rows else {}
    field = params.get('field')
    return [{'dimensions': {}, 'value': instance.get(field)}]


# -- 集合与排序 ------------------------------------------------------------

def _collection_rows(
    archive: Sequence[Mapping[str, Any]],
    collection: Optional[str],
) -> List[Mapping[str, Any]]:
    """从留档里取出某个集合的行。"""
    if not collection:
        raise ArchiveError('集合名缺失')
    for step in archive:
        body = step.get('response_body') or {}
        if collection in body:
            rows = body[collection] or []
            if not isinstance(rows, list):
                raise ArchiveError('集合 %s 不是行列表' % collection)
            return rows
    raise ArchiveError('留档里没有集合 %s' % collection)


def _sort_rows(
    rows: Sequence[Mapping[str, Any]],
    sort_by: Optional[str],
    order: str,
) -> List[Mapping[str, Any]]:
    """按排序列排序。

    空值位置按 PostgreSQL 的 ``ORDER BY`` 默认口径：升序空值在后、降序空值在前。
    空串按空值处理（平台把 NULL 的时间戳渲染成空串）。
    """
    if not sort_by:
        return list(rows)
    values = [row.get(sort_by) for row in rows]
    kind = _value_kind(values)
    present = [(row, value) for row, value in zip(rows, values) if not _is_missing(value)]
    missing = [row for row, value in zip(rows, values) if _is_missing(value)]
    present.sort(key=lambda pair: _sort_key(kind)(pair[1]), reverse=(order == 'desc'))
    ordered = [row for row, _value in present]
    if order == 'desc':
        return missing + ordered
    return ordered + missing


def _is_missing(value: Any) -> bool:
    """空值判定：``None`` 与**空串**都算空值。"""
    if value is None:
        return True
    return isinstance(value, str) and not value.strip()


def _value_kind(values: Sequence[Any]) -> str:
    """按列里第一个非空值决定比较类型。"""
    for value in values:
        if _is_missing(value):
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


def _sort_key(kind: str):
    """按比较类型选一个可比较的键。"""
    if kind == 'number':
        return lambda value: _to_float(value)
    if kind == 'timestamp':
        return lambda value: _parse_timestamp(str(value).strip()) or _MIN_TIMESTAMP
    return lambda value: '' if value is None else str(value)


def _parse_timestamp(text: str) -> Optional[datetime.datetime]:
    """解析 GaussDB 的时间戳文本（解析不了给 None）。"""
    for fmt in _TIMESTAMP_FORMATS:
        try:
            return datetime.datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _to_float(value: Any) -> float:
    """转数字（转不了当 0）。"""
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
