# -*- coding: utf-8 -*-
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
"""
from __future__ import print_function

import json
from typing import Any, List, Mapping, Optional, Sequence

from .rules import RuleContext

__all__ = ['DecodeError', 'decode']


class DecodeError(Exception):
    """响应体不符合规则声明的形状。"""


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
    raise DecodeError('未知的解码形状：%s' % kind)


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
            raise DecodeError('result 不是合法 JSON：%s' % exc)
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
        raise DecodeError('响应体里没有指标 %s 的 series' % metric_id)
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
                    '响应体里没有路径 %s（形状 %s）' % ('/'.join(walked), shape.get('kind'))
                )
            current = current[segment]
        elif isinstance(current, list):
            try:
                current = current[int(segment)]
            except (IndexError, ValueError):
                raise DecodeError(
                    '响应体路径 %s 下标越界（形状 %s）' % ('/'.join(walked), shape.get('kind'))
                )
        else:
            raise DecodeError(
                '响应体路径 %s 不是可下钻的容器（形状 %s）' % ('/'.join(walked), shape.get('kind'))
            )
    return current


def _as_rows(payload: Any) -> List[Mapping[str, Any]]:
    if payload is None:
        return []
    if not isinstance(payload, list):
        raise DecodeError('期望行列表，实际是 %s' % type(payload).__name__)
    rows = []  # type: List[Mapping[str, Any]]
    for item in payload:
        if isinstance(item, Mapping):
            rows.append(item)
    return rows
