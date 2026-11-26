"""调度器的**不可变数据类型**与键的约定。

拆出来是因为 ``scheduler.py`` 越过合同「单文件 < 400 行」，
而这些类型与调度逻辑无关（纯数据 + 纯函数）。

**键的约定在这里定死**（多实例下不带实例的键会互相覆盖）：

============================  ==========================================
键                            约定
============================  ==========================================
指标水位 ``stream``            ``f'{instance_id}:{channel}'``
告警水位 ``stream``            ``f'{instance_id}:alarm'``
============================  ==========================================
"""
from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Sequence, Tuple
from loguru import logger

from .errors import TpopsError
from .ids import metric_idempotency_key

LOGGER = logger

__all__ = [
    'ChannelRoundStats',
    'DeliveryStats',
    'InstanceTarget',
    'SchedulerError',
    'TaskFailure',
    'TaskOutcome',
    'TickResult',
    'alarm_cursor',
    'alarm_stream',
    'metric_key',
    'metric_stream',
]

#: 失败原因的最大长度（异常消息可能很长，也可能混进敏感信息）。
MAX_REASON_LENGTH = 200


class SchedulerError(TpopsError):
    """调度器装配错误（通道没有执行器、执行器返回了别的东西等）。

    属于**不可重试**错误 —— 装配不对，跑起来只会一直错。
    """


@dataclass(frozen=True)
class InstanceTarget:
    """一个待采集的实例（不可变）。

    :param instance_id: 实例 ID（**进水位键与幂等键**）。
    :param instance_name: 实例名（进 ``tags.instance_name``）。
    :param nodes: 节点清单（``key-view-execute-node`` 的返回形态）。
    """

    instance_id: str
    instance_name: str = ''
    nodes: Tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True)
class TaskOutcome:
    """一个通道在一个实例上的一轮采集结果（不可变）。

    :param messages: 已组装好的**指标**消息元组（ISXCADA 7 字段信封）。
    :param alarms: 已组装好的**告警**消息（``collectors.alarm.AlarmMessage``）。
    :param end_time_ms: 本轮覆盖到的**结束时刻**（毫秒）—— 指标水位推进到哪。
        **不填则该组不推进指标水位** —— 宁可重复投递，也不丢数据。
    :param alarm_cursor: 告警水位游标（平台侧 ``first_alarm_time``）。
        不填则由调度器从告警消息里推。
    :param external_calls: 本组本轮的外部调用次数（默认 1）。
    :param fetched: 从平台取回的原始行数（§4.9.1 的 ``fetched``）。
    :param windows: 本轮执行的 SQL 条数（§4.9.1 的 ``windows``）。
    """

    messages: Tuple[Mapping[str, Any], ...] = ()
    alarms: Tuple[Any, ...] = ()
    end_time_ms: Optional[int] = None
    alarm_cursor: Optional[str] = None
    external_calls: int = 1
    fetched: int = 0
    windows: int = 0


@dataclass(frozen=True)
class TaskFailure:
    """一个 (通道, 实例) 任务的失败（不可变）。

    :param channel: 通道名。
    :param instance_id: 实例 ID。
    :param error_type: 异常类型名。
    :param reason: 失败原因（截断到 200 字）。
    """

    channel: str
    instance_id: str
    error_type: str
    reason: str


@dataclass(frozen=True)
class DeliveryStats:
    """一次投递的计数（不可变）。"""

    planned: int = 0
    delivered: int = 0
    failed: int = 0
    alarms: int = 0


@dataclass(frozen=True)
class ChannelRoundStats:
    """**一个通道**一轮的统计（不可变）。

    字段名与每通道汇总日志**逐字一致**（不自创）——
    客户侧日志平台按这些字段取值。

    :param channel: 通道名（``monitor`` / ``command_center`` / …）。
    :param tick: 轮次序号。
    :param instances: 本轮该通道参与的实例数。
    :param fetched: 该通道各实例 ``TaskOutcome.fetched`` 之和。
    :param mapped: 该通道各实例 ``DeliveryStats.planned`` 之和
        （**幂等过滤后**实际尝试投递的消息数）。
    :param produced: 该通道各实例 ``DeliveryStats.delivered`` 之和。
    :param failed: 该通道各实例 ``DeliveryStats.failed`` 之和。
    :param windows: 该通道各实例 ``TaskOutcome.windows`` 之和。
    :param calls: 该通道各实例 ``TaskOutcome.external_calls`` 之和。
    :param elapsed: 该通道本轮的墙钟耗时（秒）。
    :param delivery_rate: ``produced / mapped``（``mapped=0`` 时记 1.000）。
    """

    channel: str
    tick: int = 0
    instances: int = 0
    fetched: int = 0
    mapped: int = 0
    produced: int = 0
    failed: int = 0
    windows: int = 0
    calls: int = 0
    elapsed: float = 0.0
    delivery_rate: float = 1.0


@dataclass(frozen=True)
class TickResult:
    """一轮 tick 的结果（不可变）。

    字段名与开发方案 §4.9.1 的每轮汇总日志**逐字一致**（不自创）。

    :param tick_index: 轮次序号。
    :param executed: 本轮执行的通道名。
    :param skipped: 本轮未到期（没执行）的通道名。
    :param mapped: 转换后**实际尝试投递**的消息数（指标 + 告警）——
        **已滤掉幂等重复**（批内重复键与已投递过的键都不计入）。
    :param produced: 成功投递的消息数。
    :param failed: 投递失败的消息数。
    :param fetched: 从各通道取回的原始行数。
    :param windows: 本轮执行的 SQL 条数。
    :param alarms: 本轮待投递的告警消息条数。
    :param external_calls: 本轮外部调用次数。
    :param duration_seconds: 本轮墙钟耗时（秒）。
    :param tasks_total: 本轮执行的任务数（通道 × 实例）。
    :param tasks_failed: 采集阶段失败的任务数。
    :param failures: 失败明细（采集 + 投递）。
    """

    tick_index: int = 0
    executed: Tuple[str, ...] = ()
    skipped: Tuple[str, ...] = ()
    mapped: int = 0
    produced: int = 0
    failed: int = 0
    fetched: int = 0
    windows: int = 0
    alarms: int = 0
    external_calls: int = 0
    duration_seconds: float = 0.0
    tasks_total: int = 0
    tasks_failed: int = 0
    failures: Tuple[TaskFailure, ...] = ()


# ---------------------------------------------------------------------------
# 键的约定
# ---------------------------------------------------------------------------

def metric_stream(instance_id: str, channel: str) -> str:
    """指标水位的 ``stream`` 键 —— **必须含 instance_id**。

    多实例下不带实例的水位会互相覆盖（后一个实例覆盖前一个的进度）。
    """
    return f'{instance_id}:{channel}'


def alarm_stream(instance_id: str) -> str:
    """告警水位的 ``stream`` 键 —— **必须含 instance_id**。"""
    return f'{instance_id}:alarm'


def metric_key(instance_id: str, message: Mapping[str, Any]) -> str:
    """指标幂等键（``ids.metric_idempotency_key``）。

    ``dimensions`` 用「客户维度 + 节点身份」——
    只用 ``dimension_values`` 的话，A 类多组件（同一指标多个 series）
    会算出同一个键 → 后面的组件被当成重复丢弃 → **丢数据**。
    """
    tags = message.get('tags') or {}
    dimensions = dict(tags.get('dimension_values') or {})
    for name in ('node_id', 'component_id'):
        value = tags.get(name)
        if value:
            dimensions[name] = value
    return metric_idempotency_key(
        instance_id, message.get('metricName'), dimensions, message.get('clock'),
    )


def alarm_cursor(alarms: Sequence[Any]) -> Optional[str]:
    """从告警消息里推告警水位游标（最大的事件时刻，毫秒字符串）。"""
    latest = 0
    for item in alarms:
        message = getattr(item, 'message', None) or {}
        for name in ('alarmTime', 'recoverTime'):
            value = message.get(name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            latest = max(latest, int(value))
    return str(latest) if latest > 0 else None


def reason(exc: BaseException) -> str:
    """失败原因（截断）—— 异常消息可能很长，也可能混进敏感信息。"""
    text = str(exc)
    if len(text) > MAX_REASON_LENGTH:
        return text[:MAX_REASON_LENGTH] + '…'
    return text


def collect_failures(
    failures: Sequence[TaskFailure],
) -> List[TaskFailure]:
    """失败清单（拷贝成列表）—— 供调用方拼接。"""
    return list(failures)
