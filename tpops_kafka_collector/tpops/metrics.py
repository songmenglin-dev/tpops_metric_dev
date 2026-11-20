"""A 类（监控指标接口）查询窗口计算与时间戳切片。

对应开发方案 §4.2.4、规格 ``specs/collection-channels/spec.md`` 的
「监控指标接口的窗口与时间戳」与「监控指标分组码」两条需求。

本模块把**实测踩过的坑**固化成代码（**不要凭直觉改**）：

1. **查询窗口 ≥ 1 小时** —— 5 分钟窗口覆盖 ``collection_frequency=3600``
   的指标数 = 0/21。窗口不足 1 小时 → **报错**，不静默截断。
2. **数据入库滞后约 5 分钟** —— ``end_time`` 必须回退
   ``METRIC_END_LAG_MINUTES``（默认 5），否则尾部大量 null（实测 null 率 77.5%）。
3. **``is_latest_point_only`` 必须是字符串 ``"false"``** —— 传布尔 true 会让
   约 74% 的指标 ``timestamps=[0.0]``（值对、时间丢）。
4. **12 个分组码必须硬编码** —— 平台**没有枚举接口**；分组名写错返回
   HTTP 200 + 空 series 且**不报错**，所以空分组必须记 WARNING 日志。
5. **时间单位统一毫秒** —— 平台的 ``timestamps`` 是 13 位毫秒（浮点），转 ``int``。

本模块**只做窗口计算与切片**，不发请求（发请求是采集器的事）；
采集器把结果交给映射层与投递层，不在这里拼 Kafka 消息。
"""
import collections.abc
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..errors import TpopsError
from .client import RDS_V3

LOGGER = logging.getLogger(__name__)

__all__ = [
    'GROUP_CATALOG_PATH', 'IS_LATEST_POINT_ONLY', 'METRIC_GROUPS', 'METRICS_PATH',
    'MIN_WINDOW_HOURS', 'MetricWindowError', 'MetricsError', 'QueryWindow',
    'SeriesPoint', 'build_query_body', 'fetch_metric_catalog', 'metric_tier_seconds',
    'parse_metric_points', 'parse_series_points', 'resolve_window',
    'resolve_window_from_config', 'slice_points', 'slice_points_by_tier', 'utc_now_ms',
]

#: **12 个监控指标分组码** —— 平台没有枚举接口，只能硬编码。
#:
#: 少一个分组就少一批指标，而且平台**不会报错**（写错的分组返回 200 + 空 series）。
#: 完整性由 ``tests/unit/test_metrics_window.py`` 的断言守着。
METRIC_GROUPS: Tuple[str, ...] = (
    'CPUMEMORY',
    'CONNECTION',
    'SYNCSTAT',
    'PROCESSRESOURCE',
    'NETWORK',
    'TRANSACTION',
    'IOSTORAGE',
    'LOCK',
    'INSTATUS',
    'FORDR',
    'AGENTSTATUS',
    'COREMETRICS',
)

#: 查询窗口下限（小时）。低于 1 小时会采不到 3600 秒档的指标。
MIN_WINDOW_HOURS = 1

#: ``is_latest_point_only`` 的取值 —— **必须是字符串**（实测坑）。
IS_LATEST_POINT_ONLY: str = 'false'

#: 监控指标时序查询接口（第一套命名空间 ``/rds/v3``）。
METRICS_PATH = RDS_V3 + '/monitor/metrics'
#: 监控指标分组目录接口。
GROUP_CATALOG_PATH = RDS_V3 + '/monitor/metrics/series'

MILLIS_PER_SECOND = 1000
MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND
MILLIS_PER_HOUR = 60 * MILLIS_PER_MINUTE

#: 采集频率缺失时的兜底档位（秒）。平台正常都会给 ``collection_frequency``。
DEFAULT_TIER_SECONDS = 60


class MetricsError(TpopsError):
    """A 类通道的配置 / 请求构造错误 —— 不可重试（改代码才能解决）。"""


class MetricWindowError(MetricsError):
    """查询窗口或请求参数不合法 —— **快速失败**，不静默截断。"""


@dataclass(frozen=True)
class QueryWindow:
    """一次监控指标查询的时间区间（毫秒）。

    :param start_time: 区间起点（毫秒，13 位）
    :param end_time: 区间终点（毫秒，13 位）；已按 ``METRIC_END_LAG_MINUTES`` 回退
    """

    start_time: int
    end_time: int

    @property
    def window_ms(self) -> int:
        """窗口长度（毫秒）。"""
        return self.end_time - self.start_time


@dataclass(frozen=True)
class SeriesPoint:
    """一个平台指标在一个组件 series 上的一个数据点。

    :param metric_name: 平台指标英文名（如 ``rds092_gaussv5_commit_counter``）
    :param metric_display_name: 平台指标中文名（``metrics[].metric_name``）
    :param series_key: 组件维度键（``cn_5001`` / ``172.16.1.1-dn_6001`` / ``value``）
    :param timestamp: 数据时间（毫秒）
    :param value: 指标值（**null 点已被跳过**，这里不会是 ``None``）
    :param collection_frequency: 采集周期（秒）—— 切片按它分档
    """

    metric_name: str
    metric_display_name: str
    series_key: str
    timestamp: int
    value: float
    collection_frequency: int


# ---------------------------------------------------------------------------
# 窗口计算
# ---------------------------------------------------------------------------

def utc_now_ms() -> int:
    """当前时刻的毫秒时间戳（13 位）。"""
    return int(time.time() * MILLIS_PER_SECOND)


def resolve_window(
    now_ms: int,
    window_hours: float,
    end_lag_minutes: float,
) -> QueryWindow:
    """按当前时刻计算查询区间。

    ``end_time`` = ``now_ms`` − 回退量；``start_time`` = ``end_time`` − 窗口长度。

    :param now_ms: 当前时刻（毫秒）
    :param window_hours: 窗口长度（小时），**不得小于** :data:`MIN_WINDOW_HOURS`
    :param end_lag_minutes: ``end_time`` 回退量（分钟），默认 5
    :raises MetricWindowError: 窗口不足 1 小时 / 回退量为负 / 参数不是数字
    """
    hours = _as_number(window_hours, 'window_hours')
    lag = _as_number(end_lag_minutes, 'end_lag_minutes')
    if hours < MIN_WINDOW_HOURS:
        raise MetricWindowError(
            f'监控指标查询窗口不足 {MIN_WINDOW_HOURS} 小时：{hours}'
            '（窗口过短会采不到 3600 秒档的指标）'
        )
    if lag < 0:
        raise MetricWindowError(f'监控指标 end_time 回退量不能为负：{lag}')

    end_time = int(now_ms) - int(lag * MILLIS_PER_MINUTE)
    start_time = end_time - int(hours * MILLIS_PER_HOUR)
    return QueryWindow(start_time=start_time, end_time=end_time)


def resolve_window_from_config(now_ms: int, config: Any) -> QueryWindow:
    """按 :class:`tpops_kafka_collector.config.Config` 计算查询区间。

    窗口下限取自 ``config.metric_window_hours``（本身不得小于 1），
    回退量取自 ``config.metric_end_lag_minutes``（默认 5）。

    :raises MetricWindowError: 配置的窗口不足 1 小时。
    """
    return resolve_window(
        now_ms,
        getattr(config, 'metric_window_hours'),
        getattr(config, 'metric_end_lag_minutes'),
    )


def _as_number(value: Any, name: str) -> float:
    """把入参转成数字；不是数字 → 快速失败。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MetricWindowError(f'{name} 必须是数字，实际是 {type(value).__name__}')
    return float(value)


# ---------------------------------------------------------------------------
# 请求体
# ---------------------------------------------------------------------------

def build_query_body(
    window: QueryWindow,
    instance_id: str,
    metric_names: Sequence[str],
    node_ids: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """构造监控指标查询请求体。

    ``is_latest_point_only`` 固定为**字符串** ``"false"``（实测坑）。

    :raises MetricWindowError: 实例 ID 为空或指标列表为空。
    """
    if not instance_id:
        raise MetricWindowError('监控指标查询缺少实例 ID')
    names = list(metric_names or ())
    if not names:
        raise MetricWindowError('监控指标查询缺少指标列表')

    instance: Dict[str, Any] = {'instance_id': instance_id}
    nodes = list(node_ids or ())
    if nodes:
        instance['node_ids'] = nodes

    return {
        'start_time': window.start_time,
        'end_time': window.end_time,
        'is_latest_point_only': IS_LATEST_POINT_ONLY,
        'instances': [instance],
        'metric_names': names,
    }


# ---------------------------------------------------------------------------
# 时间戳切片
# ---------------------------------------------------------------------------

def metric_tier_seconds(metric: Mapping[str, Any],
                        default: int = DEFAULT_TIER_SECONDS) -> int:
    """取一个平台指标的采集档位（秒）。

    优先用 ``collection_frequency``；缺失或非正数时退回 ``default``。
    """
    raw = metric.get('collection_frequency')
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return default
    value = int(raw)
    return value if value > 0 else default


def parse_series_points(metric: Mapping[str, Any]) -> Tuple[SeriesPoint, ...]:
    """把一个平台指标（``metrics[]`` 条目）拆成数据点。

    **null 点直接跳过** —— 平台未入库时值为 ``null``，null 不是 0
    （规格「缺值不产出假数据」）。

    :param metric: 平台的 ``metrics[]`` 条目
    :return: 数据点元组（顺序：先 series 键，再时间戳）
    """
    timestamps = metric.get('timestamps') or []
    series = metric.get('series') or {}
    if not isinstance(timestamps, collections.abc.Sequence) or isinstance(timestamps, (str, bytes)):
        timestamps = []
    name = str(metric.get('name') or '')
    display = str(metric.get('metric_name') or '')
    frequency = metric_tier_seconds(metric)

    points = []
    for series_key, values in series.items():
        if not isinstance(values, collections.abc.Sequence) or isinstance(values, (str, bytes)):
            continue
        for index, timestamp in enumerate(timestamps):
            if index >= len(values):
                break
            value = values[index]
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            points.append(SeriesPoint(
                metric_name=name,
                metric_display_name=display,
                series_key=str(series_key),
                timestamp=int(timestamp),
                value=float(value),
                collection_frequency=frequency,
            ))
    return tuple(points)


def parse_metric_points(payload: Mapping[str, Any]) -> Tuple[SeriesPoint, ...]:
    """把一次监控指标查询响应拆成全部数据点。

    平台的真实层级是 ``metric_infos[] → metrics[]``（**不是**拍平的写法）。
    """
    infos = payload.get('metric_infos') or []
    if not isinstance(infos, collections.abc.Sequence) or isinstance(infos, (str, bytes)):
        return ()
    points: List[SeriesPoint] = []
    for info in infos:
        if not isinstance(info, Mapping):
            continue
        metrics = info.get('metrics') or []
        if not isinstance(metrics, collections.abc.Sequence) or isinstance(metrics, (str, bytes)):
            continue
        for metric in metrics:
            if isinstance(metric, Mapping):
                points.extend(parse_series_points(metric))
    return tuple(points)


def slice_points(
    points: Sequence[SeriesPoint],
    tier_seconds: int,
    now_ms: int,
    tolerance_ms: int = 0,
) -> Tuple[SeriesPoint, ...]:
    """按档位切片：只保留落在该档位区间内的点。

    区间 = ``[now_ms − tier_seconds×1000 − 容差, now_ms + 容差]``。

    :param points: 数据点
    :param tier_seconds: 档位（秒）—— 一般取 ``collection_frequency``
    :param now_ms: 当前时刻（毫秒）
    :param tolerance_ms: 端点容差（毫秒），吸收采集边界的抖动
    :raises MetricWindowError: 档位不是正数。
    """
    if isinstance(tier_seconds, bool) or not isinstance(tier_seconds, int):
        raise MetricWindowError('档位必须是正整数秒')
    if tier_seconds <= 0:
        raise MetricWindowError(f'档位必须是正整数秒，实际是 {tier_seconds}')

    lower = now_ms - tier_seconds * MILLIS_PER_SECOND - tolerance_ms
    upper = now_ms + tolerance_ms
    return tuple(point for point in points if lower <= point.timestamp <= upper)


def slice_points_by_tier(
    points: Sequence[SeriesPoint],
    now_ms: int,
    tolerance_ms: int = 0,
) -> Tuple[SeriesPoint, ...]:
    """按**每个点自己的档位**切片。

    一次响应里不同指标的 ``collection_frequency`` 不同（10s/30s/60s/300s/3600s），
    各自只取自己档位区间内的点。
    """
    kept = []
    for point in points:
        tier = point.collection_frequency or DEFAULT_TIER_SECONDS
        if tier <= 0:
            tier = DEFAULT_TIER_SECONDS
        lower = now_ms - tier * MILLIS_PER_SECOND - tolerance_ms
        upper = now_ms + tolerance_ms
        if lower <= point.timestamp <= upper:
            kept.append(point)
    return tuple(kept)


# ---------------------------------------------------------------------------
# 分组目录
# ---------------------------------------------------------------------------

def fetch_metric_catalog(
    client: Any,
    instance_id: str,
) -> Dict[str, Tuple[Mapping[str, Any], ...]]:
    """按 12 个分组码拉取指标目录。

    **空分组必须记 WARNING** —— 分组码写错时平台返回 HTTP 200 + 空 series
    且**不报错**，不记日志就会静悄悄地少一批指标。

    :param client: 提供 ``get(path, params)`` 的 TPOPS 客户端
    :param instance_id: 实例 ID（带上它才能解析 ``alarm_threshold``）
    :return: 分组码 → 指标定义元组
    """
    catalog: Dict[str, Tuple[Mapping[str, Any], ...]] = {}
    for group in METRIC_GROUPS:
        payload = client.get(
            GROUP_CATALOG_PATH,
            params={'name': group, 'instance_id': instance_id},
        )
        metrics = _group_metrics(payload)
        if not metrics:
            LOGGER.warning(
                '监控指标空分组告警：分组码 %s 未返回任何指标'
                '（平台对错误分组码返回 200 + 空 series，不报错）',
                group,
            )
        catalog[group] = metrics
    return catalog


def _group_metrics(payload: Mapping[str, Any]) -> Tuple[Mapping[str, Any], ...]:
    """从目录响应里取出该分组的指标定义列表。"""
    if not isinstance(payload, Mapping):
        return ()
    series = payload.get('series') or []
    if not isinstance(series, collections.abc.Sequence) or isinstance(series, (str, bytes)):
        return ()
    metrics: List[Mapping[str, Any]] = []
    for group in series:
        if not isinstance(group, Mapping):
            continue
        items = group.get('metrics') or []
        if not isinstance(items, collections.abc.Sequence) or isinstance(items, (str, bytes)):
            continue
        for item in items:
            if isinstance(item, Mapping):
                metrics.append(item)
    return tuple(metrics)
