"""A 类采集器：监控指标接口（``POST /rds/v3/monitor/metrics``）。

对应规格 ``specs/collection-channels/spec.md`` 的「监控指标接口的窗口与时间戳」
「监控指标分组码」「监控指标多 series 的处理」三条需求。

**平台实测踩过的坑**（不要凭直觉改）：

1. **窗口 ≥ 1 小时**、**``end_time`` 回退 5 分钟**、**``is_latest_point_only``
   是字符串 ``"false"``** —— 这三条由 ``tpops/metrics.py`` 负责，本模块只**复用**，
   不重写一份。
2. **12 个分组码硬编码** —— 平台没有枚举接口；分组名写错时平台返回
   HTTP 200 + **空 series 且不报错**，只能靠「空分组」告警日志发现。
3. **A 类按组件给出多条 series**（客户指标是实例级）—— 现状口径是
   **逐组件各出一条消息**，维度键 ``COMPONENT_ID`` 归入 ``tags``。
   归集方式（求和 / 取最早 / 主备归并）属业务口径，**待客户确认**
   （``open_items.json`` / ``a-class-multi-series``），确认前不做自作主张的合成。
4. **null 点不是 0** —— 平台未入库时值为 ``null``，跳过而不是当 0。
5. **series 键是组件维度** —— ``component_id``（``cn_5002``）或
   ``ip-component_id``（``172.16.103.126-dn_6001``），经**节点清单**换算成
   ``{host_ip}_{node_id}``；换算不到落到节点清单里的**第一个 master CN**。

采集器**不推进水位**（那是 wave 3 调度器的职责），只返回结果。
"""

import logging
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from ..errors import DecodeError, TpopsError
from ..mapping.decode import decode
from ..mapping.message import build_metric_message
from ..mapping.nodes import find_node_by_ip
from ..mapping.rules import RuleContext, RuleError, evaluate
from ..tpops.metrics import (
    METRICS_PATH,
    QueryWindow,
    build_query_body,
    fetch_metric_catalog,
    metric_tier_seconds,
    resolve_window_from_config,
    utc_now_ms,
)

LOGGER = logging.getLogger(__name__)

__all__ = ['MonitorCollector']

#: 平台时序通道（映射层的 ``source.source`` 值）。
MONITOR_SOURCE = 'monitor_metrics'

#: 切片时的容差比例（相对档位）—— 半个档位，吸收平台入库与对齐抖动。
_TIER_TOLERANCE_RATIO = 0.5


class MonitorCollector:
    """A 类采集器：监控指标接口 → ITSCADA 指标消息。

    :param client: 提供 ``get(path, params)`` / ``post(path, payload)`` 的客户端
    :param mapping: :class:`tpops_kafka_collector.mapping.loader.LoadedMapping`
    :param instance_id: 目标实例 ID
    :param instance_name: 实例名（进 ``tags``）
    :param nodes: 节点清单（``key-view-execute-node`` 的返回形态）—— 用来把
        series 的组件键换算成节点身份
    :param config: 配置（窗口小时数与 ``end_time`` 回退量）
    :param clock: 返回当前毫秒时间戳的可注入时钟
    """

    def __init__(
        self,
        client: Any,
        mapping: Any,
        instance_id: str,
        instance_name: str = '',
        nodes: Sequence[Mapping[str, Any]] = (),
        config: Any = None,
        clock: Optional[Callable[[], int]] = None,
    ) -> None:
        if not instance_id:
            raise ValueError('监控指标采集器缺少实例 ID')
        self._client = client
        self._mapping = mapping
        self._instance_id = instance_id
        self._instance_name = instance_name
        self._nodes = tuple(nodes or ())
        self._config = config
        self._clock = clock if clock is not None else utc_now_ms
        self._rules = _monitor_rules(mapping)
        self._shapes = dict((getattr(mapping, 'response_rules', None) or {}).get('shapes') or {})
        self._responses = _monitor_response_rules(mapping)

    # ------------------------------------------------------------------
    # 对外入口
    # ------------------------------------------------------------------

    def collect(self) -> Tuple[Dict[str, Any], ...]:
        """采一轮 A 类指标，返回**已组装好**的 ITSCADA 指标消息元组。

        消息由 :func:`tpops_kafka_collector.mapping.message.build_metric_message`
        组装，序列化交给 ``kafka/serializer.py``（唯一 JSON 出口）。
        """
        now_ms = int(self._clock())
        window = self._window(now_ms)
        names = self._platform_metric_names()
        self._check_catalog(names)

        body = build_query_body(window, self._instance_id, names)
        payload = self._client.post(METRICS_PATH, body)

        messages = []  # type: List[Dict[str, Any]]
        for rule in self._rules:
            messages.extend(self._messages_of_rule(rule, payload, window, now_ms))
        LOGGER.info('A 类采集完成：%d 条消息（%d 个客户指标）', len(messages), len(self._rules))
        return tuple(messages)

    # ------------------------------------------------------------------
    # 内部步骤
    # ------------------------------------------------------------------

    def _window(self, now_ms: int) -> QueryWindow:
        """查询窗口 —— 复用 ``tpops/metrics.py``（≥ 1 小时、end_time 回退）。"""
        if self._config is None:
            raise ValueError('监控指标采集器缺少配置（窗口小时数与 end_time 回退量）')
        return resolve_window_from_config(now_ms, self._config)

    def _platform_metric_names(self) -> Tuple[str, ...]:
        """A 类客户指标用到的平台指标名（去重、排序）。"""
        names = set()
        for rule in self._rules:
            name = rule.source.get('tpops_source')
            if name:
                names.add(str(name))
        return tuple(sorted(names))

    def _check_catalog(self, names: Sequence[str]) -> None:
        """按 12 个分组码查指标目录，并**为每个空分组记告警日志**。

        平台对写错的分组码返回 HTTP 200 + 空 series 且**不报错** ——
        不记日志就会静悄悄地少一批指标。
        """
        try:
            catalog = fetch_metric_catalog(self._client, self._instance_id)
        except TpopsError as exc:
            LOGGER.error('监控指标分组目录查询失败（不影响取数，但分组码无法校验）：%s', exc)
            return

        empty = sorted(group for group, metrics in catalog.items() if not metrics)
        if empty:
            LOGGER.warning(
                '监控指标空分组告警：分组码 %s 未返回任何指标'
                '（平台对错误分组码返回 200 + 空 series，不报错）',
                ', '.join(empty),
            )
        available = _catalog_names(catalog)
        if not available:
            LOGGER.error(
                '监控指标分组目录全空：12 个分组码都没返回指标 —— 本轮 A 类无数据'
                '（检查分组码是否写错）'
            )
            return
        missing = sorted(set(names) - available)
        if missing:
            LOGGER.warning(
                '监控指标目录里没有这些平台指标：%s（对应客户指标本轮可能无数据）',
                ', '.join(missing),
            )

    def _messages_of_rule(
        self,
        rule: Any,
        payload: Mapping[str, Any],
        window: QueryWindow,
        now_ms: int,
    ) -> List[Dict[str, Any]]:
        """一个客户指标 → 若干条消息（**每个组件一条**）。"""
        response = self._responses.get(rule.metric_code)
        if response is None:
            LOGGER.error(
                '响应规则表里没有监控指标 %s 的定位条件（channel=%s）',
                rule.metric_code, MONITOR_SOURCE,
            )
            return []
        shape = self._shapes.get(response.get('shape'))
        if shape is None:
            LOGGER.error(
                '响应规则表里没有监控指标 %s 的形状 %s',
                rule.metric_code, response.get('shape'),
            )
            return []

        locate = response.get('locate') or {}
        try:
            context = decode(payload, shape, locate, now_ms)
        except DecodeError as exc:
            LOGGER.warning(
                '监控指标 %s 本轮没有数据（响应体里没有它的 series）：%s',
                rule.metric_code, exc,
            )
            return []

        context = _sliced_context(
            context, payload, locate.get('metric'), window, rule.metric_code
        )
        try:
            values = evaluate(rule, context, None)
        except RuleError as exc:
            LOGGER.error('监控指标 %s 的取值规则执行失败：%s', rule.metric_code, exc)
            return []

        messages = []  # type: List[Dict[str, Any]]
        for dimensions, value, timestamp in values:
            if value is None:
                # 缺值不产出假数据 —— 不拿 0 顶替
                continue
            messages.append(self._message(rule, dimensions, value, timestamp))
        return messages

    def _message(
        self,
        rule: Any,
        dimensions: Mapping[str, Any],
        value: Any,
        timestamp: Optional[int],
    ) -> Dict[str, Any]:
        """组装一条 ITSCADA 指标消息（维度键 ``COMPONENT_ID`` 归入 tags）。"""
        component_id = dimensions.get('COMPONENT_ID')
        context = self._resource_context(rule, component_id)
        return build_metric_message(
            self._mapping.field_map,
            rule,
            dimensions,
            value,
            context,
            int(timestamp or 0),
            None,
            self._nodes,
        )

    def _resource_context(self, rule: Any, component_id: Any) -> Dict[str, Any]:
        """资源上下文：实例 + 组件 + 节点身份。"""
        context = {
            'instance_id': self._instance_id,
            'instance_name': self._instance_name,
        }
        if component_id:
            context['component_id'] = str(component_id)
        node = _resolve_series_node(component_id, self._nodes)
        if node is None:
            if rule.node_scoped:
                LOGGER.warning(
                    '监控指标 %s 的组件 %s 在节点清单里找不到 —— 资源名回落到第一个 master CN',
                    rule.metric_code, component_id,
                )
            return context
        context['node_id'] = node.get('node_id')
        context['host_ip'] = node.get('node_ip')
        return context


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def _monitor_rules(mapping: Any) -> Tuple[Any, ...]:
    """映射表里属于 A 类（监控指标接口）的启用规则。"""
    rules = []  # type: List[Any]
    for rule in getattr(mapping, 'metric_rules', None) or ():
        if not rule.enabled:
            continue
        if rule.source.get('source') == MONITOR_SOURCE:
            rules.append(rule)
    return tuple(rules)


def _monitor_response_rules(mapping: Any) -> Dict[str, Mapping[str, Any]]:
    """响应规则表里属于 A 类的条目（客户指标 → 形状 + 定位条件）。"""
    payload = getattr(mapping, 'response_rules', None) or {}
    result = {}  # type: Dict[str, Mapping[str, Any]]
    for item in payload.get('rules') or ():
        if item.get('channel') != MONITOR_SOURCE:
            continue
        code = item.get('metric_code')
        if code:
            result[str(code)] = item
    return result


def _catalog_names(catalog: Mapping[str, Any]) -> set:
    """目录里出现过的平台指标名集合。"""
    names = set()
    for metrics in catalog.values():
        for item in metrics or ():
            name = item.get('name')
            if name:
                names.add(str(name))
    return names


def _metric_entries(payload: Mapping[str, Any]) -> Dict[str, Mapping[str, Any]]:
    """响应体里 ``平台指标名 → 指标条目``（取采集周期用）。"""
    entries = {}  # type: Dict[str, Mapping[str, Any]]
    for info in payload.get('metric_infos') or ():
        if not isinstance(info, Mapping):
            continue
        for metric in info.get('metrics') or ():
            if not isinstance(metric, Mapping):
                continue
            name = metric.get('name')
            if name and str(name) not in entries:
                entries[str(name)] = metric
    return entries


def _sliced_context(
    context: RuleContext,
    payload: Mapping[str, Any],
    metric_name: Any,
    window: QueryWindow,
    code: str,
) -> RuleContext:
    """按**采集档位**切片：只保留落在该档位区间内的数据点。

    区间以**窗口终点**为右端（``end_time`` 已回退过，平台数据到不了现在），
    左端留一个档位的容差，吸收入库抖动。

    平台时间戳的粒度可能**粗于** ``collection_frequency``（实测：
    ``gaussdb_instance_status`` 的采集周期是 30 秒，时间戳却是小时级），
    这种情况下切片会落空 —— 此时**回退到窗口内最新点**并记 WARNING，
    而不是把这一批指标整批丢掉。
    """
    entry = _metric_entries(payload).get(str(metric_name)) or {}
    tier = metric_tier_seconds(entry)
    tolerance = int(tier * 1000 * _TIER_TOLERANCE_RATIO)
    lower = window.end_time - tier * 1000 - tolerance
    upper = window.end_time + tolerance
    keep = [
        index for index, stamp in enumerate(context.timestamps)
        if stamp and lower <= stamp <= upper
    ]
    if not keep:
        LOGGER.warning(
            '监控指标 %s 的数据点都不在本档周期内（平台时间戳粒度可能粗于采集周期 %d 秒）'
            '—— 本轮回退到窗口内最新点',
            code, tier,
        )
        return context
    return _trim_context(context, keep)


def _trim_context(context: RuleContext, keep: Sequence[int]) -> RuleContext:
    """按保留的下标裁剪上下文（series 与 timestamps 同步裁剪）。"""
    timestamps = tuple(context.timestamps[index] for index in keep)
    series = {}  # type: Dict[str, Tuple[Any, ...]]
    for key, values in context.series.items():
        series[key] = tuple(
            values[index] for index in keep if index < len(values)
        )
    return RuleContext(series=series, timestamps=timestamps, now_ms=context.now_ms)


def _resolve_series_node(
    component_id: Any,
    nodes: Sequence[Mapping[str, Any]],
) -> Optional[Mapping[str, Any]]:
    """把 series 的组件键换算成节点清单里的一条节点。

    - ``component_id`` 精确匹配（``cn_5002``）；
    - ``ip-component_id`` 形态（``172.16.103.126-dn_6001``）→ 先按组件 ID 匹配，
      再按 IP 反查；
    - 都匹配不到 → ``None``（调用方回落到第一个 master CN）。
    """
    if not component_id or not nodes:
        return None
    key = str(component_id)
    for node in nodes:
        if node.get('component_id') == key:
            return node
    if '-' not in key:
        return None
    host_ip, _, component = key.rpartition('-')
    for node in nodes:
        if node.get('component_id') == component:
            return node
    return find_node_by_ip(nodes, host_ip)
