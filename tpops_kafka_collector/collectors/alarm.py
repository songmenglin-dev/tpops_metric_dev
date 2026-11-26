"""E 类：告警采集器（拉取 → 归并 → 组装）。

对应规格 ``specs/collection-channels/spec.md`` 的「告警通道的分页与归并」、
「历史告警增量与超期检测」、「告警节点身份回填」，
以及 ``specs/mapping-and-rules/spec.md`` 的「告警消息的投递单位」。

口径（**不要凭直觉改**）：

1. **投递单位是收敛条目** —— 一条消息对应「实例 + alarm_id」，
   明细行归并进 ``details`` 数组（规格「告警消息的投递单位」）。
2. **活动与历史各取收敛与明细** —— 四种 ``is_active`` × ``is_convergence``
   组合各拉一次；``is_active`` / ``is_convergence`` 必传，``limit ≤ 100`` 分页取全。
3. **收敛形态不依赖平台侧过滤** —— 平台侧的 ``alarm_id`` / ``instance_id``
   过滤只在明细形态有效；收敛形态改由本地按 ``alarm_id + instance_id`` 归并。
4. **节点身份回填** —— 收敛条目没有节点身份，按 ``alarm_id + instance_id``
   关联明细行回填；**一条告警命中多个节点时回填不了** → 落节点清单里的
   **第一个 master CN**（固定节点，跨轮稳定；真实节点保留在 ``details`` 里）。
5. **水位增量** —— 只取比传入水位「新增或变化」的记录。
   变化用记录自己的事件时刻（``last_alarm_time`` / ``clear_alarm_time``）判定：
   **恢复后 ``first_alarm_time`` 不变**（实测坑 F6），只看它会把恢复事件丢掉。
   **采集器不推进水位**（那是调度器的职责）。
6. **30 天超期显式告警** —— 平台侧历史告警**仅保留 30 天**，停机超期那段
   数据已被平台清理，补不回来，必须记 WARNING，不能静默。

消息组装用 ``mapping/message.py`` 的 :func:`build_alarm_message`，
序列化由 ``kafka/serializer.py`` 独家负责 —— 本模块不拼 JSON 消息。
"""
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..ids import alarm_idempotency_key
from ..mapping.message import build_alarm_message
from ..mapping.nodes import select_master_node
from ..mapping.table import AlarmDefinition, build_event_alarm_definition
from ..tpops.alarms import DEFAULT_LIMIT, fetch_all_alarm_records

LOGGER = logging.getLogger(__name__)

__all__ = ['AlarmCollectionResult', 'AlarmMessage', 'collect_alarms']

#: 一天（毫秒）—— 保留窗口换算。
MILLIS_PER_DAY = 24 * 3600 * 1000

#: 记录的事件时刻字段（用于「新增或变化」判定）。
EVENT_TIME_FIELDS = ('first_alarm_time', 'last_alarm_time', 'clear_alarm_time')


@dataclass(frozen=True)
class AlarmMessage:
    """一条待投递的告警消息。

    :param message: ITSCADA 告警消息（13 个字段，由 ``build_alarm_message`` 组装）
    :param idempotency_key: 幂等键（``ids.alarm_idempotency_key``，
        ``status`` 用**平台原值**）
    :param alarm_id: 平台告警 ID
    """

    message: Mapping[str, Any]
    idempotency_key: str
    alarm_id: str


@dataclass(frozen=True)
class AlarmCollectionResult:
    """一轮告警采集的结果。

    :param messages: 待投递的告警消息（活动在前，历史在后）
    :param convergence_count: 拉到的收敛条目数
    :param detail_count: 拉到的明细行数
    """

    messages: Tuple[AlarmMessage, ...]
    convergence_count: int
    detail_count: int


def collect_alarms(
    client: Any,
    mapping: Any,
    nodes: Optional[Sequence[Mapping[str, Any]]] = None,
    instance_id: Optional[str] = None,
    watermark: Optional[int] = None,
    now_ms: Optional[int] = None,
    retention_days: int = 30,
    limit: int = DEFAULT_LIMIT,
) -> AlarmCollectionResult:
    """拉取一轮告警并归并成消息。

    :param client: 提供 ``post(path, payload)`` 的 TPOPS 客户端
    :param mapping: 映射表（提供告警定义与字段规范）
    :param nodes: 节点清单（回填不到节点时取第一个 master CN）
    :param instance_id: 只采集该实例（``None`` = 不限）
    :param watermark: 水位（毫秒）；只取比它「新增或变化」的记录
    :param now_ms: 当前时刻（毫秒），缺省取系统时间
    :param retention_days: 平台历史告警保留窗口（天），默认 30
    :param limit: 每页条数（平台上限 100）
    """
    now = int(now_ms) if now_ms is not None else int(time.time() * 1000)
    limit_ms = _as_millis(watermark)

    records = _fetch_four_shapes(client, limit, instance_id)
    active_convergence = records['active_convergence']
    active_detail = records['active_detail']
    history_convergence = records['history_convergence']
    history_detail = records['history_detail']

    _warn_history_gap(history_convergence, now, retention_days)
    _warn_downtime_gap(limit_ms, now, retention_days)

    active_index = _index_details(active_detail)
    history_index = _index_details(history_detail)
    messages = _build_messages(
        mapping, nodes, active_convergence, active_index, instance_id, limit_ms)
    messages += _build_messages(
        mapping, nodes, history_convergence, history_index, instance_id, limit_ms)
    LOGGER.info(
        '告警采集完成：收敛 %d 条 + 明细 %d 行 → 产出 %d 条消息',
        len(active_convergence) + len(history_convergence),
        len(active_detail) + len(history_detail),
        len(messages),
    )
    return AlarmCollectionResult(
        messages=tuple(messages),
        convergence_count=len(active_convergence) + len(history_convergence),
        detail_count=len(active_detail) + len(history_detail),
    )


def _fetch_four_shapes(
    client: Any,
    limit: int,
    instance_id: Optional[str],
) -> Dict[str, Tuple[Mapping[str, Any], ...]]:
    """取告警的四种形态（活动/历史 × 收敛/明细）。

    平台的告警接口用 ``is_active`` 与 ``is_convergence`` 两个布尔区分形态，
    四种组合要分别拉 —— 收敛列表给的是「一条告警一行」，明细给的是
    「一条告警多行明细」。

    **收敛形态不传 ``instance_id``** —— 合同口径「告警收敛形态下不依赖平台侧
    ``alarm_id`` / ``instance_id`` 过滤」；明细形态才按实例收窄。

    :param instance_id: 只采集该实例的明细（``None`` = 不限）。
    """
    def fetch(is_active: bool, is_convergence: bool,
              scoped: bool) -> Tuple[Mapping[str, Any], ...]:
        return fetch_all_alarm_records(
            client, is_active, is_convergence, limit,
            instance_id=instance_id if scoped else None)

    return {
        'active_convergence': fetch(True, True, False),
        'active_detail': fetch(True, False, True),
        'history_convergence': fetch(False, True, False),
        'history_detail': fetch(False, False, True),
    }


# ---------------------------------------------------------------------------
# 归并与组装
# ---------------------------------------------------------------------------

def _build_messages(
    mapping: Any,
    nodes: Optional[Sequence[Mapping[str, Any]]],
    convergence_rows: Sequence[Mapping[str, Any]],
    detail_index: Mapping[Tuple[str, str], Tuple[Mapping[str, Any], ...]],
    instance_id: Optional[str],
    limit_ms: Optional[int],
) -> List[AlarmMessage]:
    """把收敛条目组装成消息，明细按 ``alarm_id + instance_id`` 归并进去。"""
    messages = []
    for row in convergence_rows:
        if instance_id and row.get('instance_id') != instance_id:
            continue
        if limit_ms is not None and not _is_new_or_changed(row, limit_ms):
            continue
        alarm_id = str(row.get('alarm_id'))
        key = (alarm_id, str(row.get('instance_id')))
        details = detail_index.get(key, ())
        definition = _definition_for(mapping, row, alarm_id)
        message = build_alarm_message(
            field_map=mapping.field_map,
            record=row,
            definition=definition,
            resource_context=_node_context(details, nodes),
            severity_to_priority=mapping.alarm_rules.get('severity_to_priority', {}),
            status_to_itscada=mapping.alarm_rules.get('status_to_itscada', {}),
            metric_value=None,
            unit=_unit_for(mapping, definition),
            details=details,
            nodes=nodes,
        )
        messages.append(AlarmMessage(
            message=message,
            idempotency_key=alarm_idempotency_key(
                alarm_id, row.get('instance_id'),
                row.get('first_alarm_time'), row.get('status')),
            alarm_id=alarm_id,
        ))
    return messages


def _index_details(
    detail_rows: Sequence[Mapping[str, Any]],
) -> Dict[Tuple[str, str], Tuple[Mapping[str, Any], ...]]:
    """明细行按 ``alarm_id + instance_id`` 建索引（本地归并用）。"""
    grouped = {}  # type: Dict[Tuple[str, str], List[Mapping[str, Any]]]
    for row in detail_rows:
        key = (str(row.get('alarm_id')), str(row.get('instance_id')))
        grouped.setdefault(key, []).append(row)
    return {key: tuple(rows) for key, rows in grouped.items()}


def _node_context(
    details: Sequence[Mapping[str, Any]],
    nodes: Optional[Sequence[Mapping[str, Any]]],
) -> Dict[str, Any]:
    """收敛条目的节点身份：按明细行回填，回填不到落第一个 master CN。

    **一条告警命中多个节点时回填不了** —— 落到第一个 master CN（固定节点，
    跨轮稳定）；真实节点保留在 ``details`` 里。
    """
    identities = set()
    for row in details:
        node_id = row.get('node_id')
        host_ip = row.get('host_ip')
        if node_id or host_ip:
            identities.add((node_id, host_ip))
    if len(identities) == 1:
        node_id, host_ip = identities.pop()
        return {'node_id': node_id, 'host_ip': host_ip}
    if identities:
        LOGGER.info(
            '一条告警命中 %d 个节点，节点身份回填不了 → 落第一个 master CN'
            '（真实节点在 details 里）',
            len(identities),
        )
    master = select_master_node(nodes)
    if master is None:
        return {}
    return {'node_id': master.get('node_id'), 'host_ip': master.get('node_ip')}


def _definition_for(
    mapping: Any,
    row: Mapping[str, Any],
    alarm_id: str,
) -> AlarmDefinition:
    """取告警定义；定义缺失时按**统一指标名**兜底。

    统一指标名来自映射数据（``alarm_rules.json`` 的 ``event_metric`` 段）——
    与事件类告警的口径一致，代码里不写死任何客户口径。
    """
    definition = mapping.alarm_definition(alarm_id)
    if definition is not None:
        return definition
    event_metric = (mapping.alarm_rules or {}).get('event_metric') or {}
    LOGGER.warning(
        '告警定义缺失：alarm_id=%s（平台新告警？），按统一指标名 %s 兜底挂载',
        alarm_id, event_metric.get('metric_code'),
    )
    return build_event_alarm_definition(
        alarm_id, row.get('alarm_name') or '', event_metric)


def _unit_for(mapping: Any, definition: AlarmDefinition) -> Optional[str]:
    """挂到客户指标的告警取该指标单位；其余（自造指标名）为 ``None``。"""
    rule = mapping.metric_rule(definition.metric_code)
    if rule is None:
        return None
    return rule.unit


# ---------------------------------------------------------------------------
# 水位与超期检测
# ---------------------------------------------------------------------------

def _is_new_or_changed(record: Mapping[str, Any], limit_ms: int) -> bool:
    """记录是否比水位「新增或变化」。

    新增 = ``first_alarm_time`` 晚于水位；
    变化 = 记录自己的事件时刻（``last_alarm_time`` / ``clear_alarm_time``）
    晚于水位 —— **恢复后 ``first_alarm_time`` 不变**（实测坑 F6），
    只看它会把恢复事件整条丢掉（恢复事件必须发得出去）。
    """
    for name in EVENT_TIME_FIELDS:
        value = record.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        if int(value) > limit_ms:
            return True
    return False


def _as_millis(watermark: Any) -> Optional[int]:
    """水位统一成毫秒整数；无法解析时按「无水位」处理。"""
    if watermark is None:
        return None
    if isinstance(watermark, bool):
        return None
    if isinstance(watermark, (int, float)):
        return int(watermark)
    try:
        return int(str(watermark).strip())
    except (TypeError, ValueError):
        LOGGER.warning('告警水位无法解析为毫秒时间戳，按无水位处理：%r', watermark)
        return None


def _warn_history_gap(
    history_rows: Sequence[Mapping[str, Any]],
    now_ms: int,
    retention_days: int,
) -> None:
    """历史告警最新时间距今超过保留窗口 → 记 WARNING（该段补不回来）。

    平台侧历史告警**仅保留 30 天** —— 超期那段数据已被平台清理，
    采集端补不回来，必须显式告警，不能静默。
    """
    latest = _latest_history_time(history_rows)
    if latest is None:
        LOGGER.info(
            '历史告警为空（平台 %d 天保留窗口内没有历史告警），无法核对缺口',
            retention_days,
        )
        return
    age_days = (now_ms - latest) / float(MILLIS_PER_DAY)
    if age_days > retention_days:
        LOGGER.warning(
            '告警历史缺口告警：最新历史告警时间距今 %.1f 天，超过平台 %d 天保留窗口；'
            '该时间段平台侧已无更早数据，采集端无法补回',
            age_days, retention_days,
        )


def _warn_downtime_gap(
    limit_ms: Optional[int],
    now_ms: int,
    retention_days: int,
) -> None:
    """停机超过保留窗口 → 记 WARNING（平台已清理，缺口补不回来）。"""
    if limit_ms is None:
        return
    idle_days = (now_ms - limit_ms) / float(MILLIS_PER_DAY)
    if idle_days > retention_days:
        LOGGER.warning(
            '告警历史缺口告警：告警水位距今 %.1f 天，超过平台 %d 天保留窗口；'
            '停机期间（更早）的告警平台侧已无数据，采集端无法补回',
            idle_days, retention_days,
        )


def _latest_history_time(
    history_rows: Sequence[Mapping[str, Any]],
) -> Optional[int]:
    """历史告警里最新的事件时刻（毫秒）。"""
    latest = None
    for row in history_rows:
        for name in EVENT_TIME_FIELDS:
            value = row.get(name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            if value <= 0:
                continue
            latest = int(value) if latest is None else max(latest, int(value))
    return latest
