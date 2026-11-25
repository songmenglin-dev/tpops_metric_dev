"""D 类采集器：实例元数据（``GET /rds/v3.2/instances``）→ ITSCADA 指标消息。

对应规格 ``specs/collection-channels/spec.md`` 的「实例元数据通道」、
开发方案 §4.2.7。D 类只有一个客户指标（``gaussdb.server_version``）。

四条硬约束：

1. **接口路径是 ``/rds/v3.2/instances``** —— 不是 ``v3``（少一段前缀就 404）。
2. **只取本轮目标实例**的 ``engine_version`` —— 平台上有多个实例，
   每个实例各走一遍 D 类通道；找不到本轮这个实例就**不产出消息**（不猜、不取第一条）。
3. **D 类与节点无关** → 资源名落到节点清单里的**第一个 master CN**
   （``{host_ip}_{node_id}``），保证跨轮稳定。
4. **档位由映射表的 ``collect_cycle`` 推导**（表驱动，不写死）——
   改表就改档，不改代码。

**缺值不产出消息**（规格「缺值不产出假数据」）：实例列表为空 / 找不到目标实例 /
``engine_version`` 取不到，三种情况都**不产出消息**并记 ERROR 日志，
**不静默出 0**、不发 null 值。

采集器**不推进水位**（那是 wave 3 调度器的职责），只返回结果。
"""

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from ..errors import MappingError
from ..mapping.message import build_metric_message
from ..mapping.rules import RuleContext, evaluate
from ..tpops.instances import fetch_instances, select_instance
from ..tpops.metrics import utc_now_ms

LOGGER = logging.getLogger(__name__)

__all__ = [
    'CYCLE_UNIT_SECONDS',
    'InstanceCollectResult',
    'InstanceCollector',
    'collect_cycle_label',
    'collect_cycle_seconds',
]

#: 平台时序通道（映射层的 ``source.source`` 值）。
INSTANCE_SOURCE = 'instance'

#: 采集周期的单位换算（映射表 ``collect_cycle.unit`` 用中文单位）。
CYCLE_UNIT_SECONDS = {
    '秒': 1,
    '分': 60,
    '时': 3600,
    '天': 86400,
}


@dataclass(frozen=True)
class InstanceCollectResult:
    """一轮 D 类采集的结果。

    :param messages: 已组装好的 ITSCADA 指标消息元组（缺值时为空）。
    :param tier_seconds: 本轮档位（秒）—— 由映射表的 ``collect_cycle`` 推导。
    :param tier_label: 本轮档位的人话标签（如 ``1 时``）。
    """

    messages: Tuple[Dict[str, Any], ...] = ()
    tier_seconds: int = 0
    tier_label: str = ''


def collect_cycle_seconds(cycle: Optional[Mapping[str, Any]]) -> int:
    """把映射表的 ``collect_cycle``（``{'value': 1, 'unit': '时'}``）换算成秒。"""
    if not cycle:
        return 0
    unit = str(cycle.get('unit') or '')
    factor = CYCLE_UNIT_SECONDS.get(unit)
    if factor is None:
        known = sorted(CYCLE_UNIT_SECONDS)
        raise MappingError(f'未知的采集周期单位：{unit}（已知：{known}）')
    value = cycle.get('value')
    try:
        count = int(value)
    except (TypeError, ValueError):
        raise MappingError(f'采集周期的取值不是整数：{value!r}')
    return count * factor


def collect_cycle_label(cycle: Optional[Mapping[str, Any]]) -> str:
    """档位的人话标签（如 ``1 时``）。"""
    if not cycle:
        return ''
    return f'{cycle.get("value")} {cycle.get("unit")}'


class InstanceCollector:
    """D 类采集器：实例元数据 → ITSCADA 指标消息。

    :param client: 提供 ``get(path, params)`` 的 TPOPS 客户端
    :param mapping: :class:`tpops_kafka_collector.mapping.loader.LoadedMapping`
    :param node_registry: 提供 ``nodes(instance_id)`` 的节点清单来源
        （用来把资源名落到第一个 master CN）
    :param clock: 返回当前毫秒时间戳的可注入时钟
    """

    def __init__(
        self,
        client: Any,
        mapping: Any,
        node_registry: Any,
        clock: Optional[Callable[[], int]] = None,
    ) -> None:
        self._client = client
        self._mapping = mapping
        self._node_registry = node_registry
        self._clock = clock if clock is not None else utc_now_ms
        self._rules = _instance_rules(mapping)
        self._tier_seconds, self._tier_label = _tier_of(self._rules)

    # ------------------------------------------------------------------
    # 对外入口
    # ------------------------------------------------------------------

    def collect(self, instance_id: str) -> InstanceCollectResult:
        """采一轮 D 类指标，返回已组装好的消息与本轮档位。

        消息由 :func:`tpops_kafka_collector.mapping.message.build_metric_message`
        组装，序列化交给 ``kafka/serializer.py``（唯一 JSON 出口）。
        """
        empty = InstanceCollectResult(
            tier_seconds=self._tier_seconds, tier_label=self._tier_label,
        )
        instances = fetch_instances(self._client)
        if not instances:
            LOGGER.error(
                '实例列表为空（目标实例 %s）—— 本轮不产出 D 类消息', instance_id
            )
            return empty
        instance = select_instance(instances, instance_id)
        if instance is None:
            LOGGER.error(
                '找不到目标实例 %s —— 本轮不产出 D 类消息', instance_id
            )
            return empty

        nodes = self._node_registry.nodes(instance_id)
        now_ms = int(self._clock())
        messages = self._collect_messages(instance, nodes, now_ms)
        return InstanceCollectResult(
            messages=tuple(messages),
            tier_seconds=self._tier_seconds,
            tier_label=self._tier_label,
        )

    def _collect_messages(
        self,
        instance: Any,
        nodes: Sequence[Mapping[str, Any]],
        now_ms: int,
    ) -> List[Dict[str, Any]]:
        """按规则取值并组装消息（**缺值不产出**）。"""
        messages = []  # type: List[Dict[str, Any]]
        for rule in self._rules:
            value = self._rule_value(rule, instance, now_ms)
            if value is None:
                LOGGER.error(
                    'D 类指标的 %s 取不到（实例 %s）—— 不产出该条消息',
                    _field_name(rule), instance.instance_id,
                )
                continue
            messages.append(build_metric_message(
                field_map=self._mapping.field_map,
                metric_rule=rule,
                dimensions={},
                value=value,
                resource_context={
                    'instance_id': instance.instance_id,
                    'instance_name': instance.name,
                },
                timestamp=now_ms,
                nodes=nodes,
            ))
        return messages

    # ------------------------------------------------------------------
    # 取值
    # ------------------------------------------------------------------

    def _rule_value(self, rule: Any, instance: Any, now_ms: int) -> Any:
        """按映射表声明的取值规则取实例字段（``instance_field``）。"""
        ctx = RuleContext(instance=instance.fields, now_ms=now_ms)
        results = evaluate(rule, ctx)
        if not results:
            return None
        return results[0][1]


# ---------------------------------------------------------------------------
# 内部辅助
# ---------------------------------------------------------------------------

def _instance_rules(mapping: Any) -> Tuple[Any, ...]:
    """映射表里属于 D 类（实例接口）的启用规则。"""
    rules = []  # type: List[Any]
    for rule in getattr(mapping, 'metric_rules', None) or ():
        if not rule.enabled:
            continue
        if rule.source.get('source') == INSTANCE_SOURCE:
            rules.append(rule)
    return tuple(rules)


def _tier_of(rules: Tuple[Any, ...]) -> Tuple[int, str]:
    """本轮档位：取 D 类各指标里**最高**的档（同长度档位视为同档）。"""
    best = None  # type: Any
    for rule in rules:
        cycle = getattr(rule, 'collect_cycle', None) or {}
        if best is None or collect_cycle_seconds(cycle) > collect_cycle_seconds(best):
            best = cycle
    if best is None:
        return 0, ''
    return collect_cycle_seconds(best), collect_cycle_label(best)


def _field_name(rule: Any) -> str:
    """规则要取的实例字段名（用于日志说明是哪个字段取不到）。"""
    params = (getattr(rule, 'value_rule', None) or {}).get('params') or {}
    return str(params.get('field') or '值')
