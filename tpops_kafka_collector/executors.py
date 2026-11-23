"""五个通道的**执行器**（装配层）。

执行器契约（任务 3.1 报告 §5.5）::

    (InstanceTarget) -> TaskOutcome

三条硬约束：

1. **停机检查** —— 每个执行器入口先看 ``shutdown``；已置位就**直接返回空的
   ``TaskOutcome``**，**不发起任何 HTTP 请求**。
   调度器（3.1）只保证「不再发起新任务」，**不保证**「不再发起新的 HTTP
   请求」—— 本轮已经排进线程池但还没开始跑的任务仍会启动 → 停机检查必须
   落在执行器这一层（规格「优雅退出」）。
2. **取数失败时 ``end_time_ms`` 给 ``None``** —— 给值会让水位冲过头 →
   **丢数据**。宁可重复投递，也不丢数据。
3. **告警消息进 ``outcome.alarms``**，不进 ``outcome.messages`` ——
   投递层按 ``outcome.alarms`` 走告警 Topic。

通道与周期：

============  ==========================================  ==================
通道          采集器                                      周期
============  ==========================================  ==================
monitor       ``collectors.monitor.MonitorCollector``      映射表实算
command_center ``collectors.command_center.…``            映射表实算
diagnostic    ``collectors.diagnostic.DiagnosticCollector`` 映射表实算（120 秒）
instance      ``collectors.instance.InstanceCollector``     映射表实算
alarm         ``collectors.alarm.collect_alarms``          30 秒
============  ==========================================  ==================
"""
import logging
import threading
from typing import Any, Callable, Dict, Optional

from .collectors.alarm import collect_alarms
from .collectors.command_center import CommandCenterCollector
from .collectors.diagnostic import DiagnosticCollector
from .collectors.instance import InstanceCollector
from .collectors.monitor import MonitorCollector
from .mapping.masking import sanitize_sql_with_config
from .schedule import (
    CHANNEL_ALARM,
    CHANNEL_COMMAND_CENTER,
    CHANNEL_DIAGNOSTIC,
    CHANNEL_INSTANCE,
    CHANNEL_MONITOR,
)
from .scheduler_types import InstanceTarget, TaskOutcome, alarm_stream
from .tpops.metrics import resolve_window_from_config, utc_now_ms

LOGGER = logging.getLogger(__name__)

__all__ = ['build_executors']

#: 各通道的外部调用次数（``external_calls``）—— 目录 + 查询 / 一次下发的页数等。
MONITOR_EXTERNAL_CALLS = 2
DIAGNOSTIC_EXTERNAL_CALLS = 1
INSTANCE_EXTERNAL_CALLS = 2
ALARM_EXTERNAL_CALLS = 4


def build_executors(
    client: Any,
    mapping: Any,
    config: Any,
    command_center: Any = None,
    node_registry: Any = None,
    deltas: Any = None,
    watermark: Any = None,
    shutdown: Optional[threading.Event] = None,
    clock: Optional[Callable[[], int]] = None,
) -> Dict[str, Callable[[InstanceTarget], TaskOutcome]]:
    """装配五个通道的执行器（**每个都套一层停机检查**）。

    :param deltas: 差分基线存储 —— **必须与 ``Scheduler(delta_store=…)``
        是同一个对象**（否则基线不落盘 → 重启后速率永远算不出来）。
    :param watermark: 水位存储（告警执行器要读告警水位游标）。
    :param shutdown: 停机事件；置位后执行器**不再发起任何 HTTP 请求**。
    """
    stop = shutdown if shutdown is not None else threading.Event()
    now = clock if clock is not None else utc_now_ms
    executors = {
        CHANNEL_MONITOR: _monitor_executor(client, mapping, config, now),
        CHANNEL_COMMAND_CENTER: _command_center_executor(
            client, mapping, command_center, deltas, now,
        ),
        CHANNEL_DIAGNOSTIC: _diagnostic_executor(client, mapping, config, now),
        CHANNEL_INSTANCE: _instance_executor(
            client, mapping, node_registry, now,
        ),
        CHANNEL_ALARM: _alarm_executor(
            client, mapping, config, watermark, now,
        ),
    }
    return dict(
        (channel, _guard(executor, stop))
        for channel, executor in executors.items()
    )


def _guard(
    executor: Callable[[InstanceTarget], TaskOutcome],
    stop: threading.Event,
) -> Callable[[InstanceTarget], TaskOutcome]:
    """套一层停机检查：已停机 → 空的 ``TaskOutcome``，**不发任何请求**。"""

    def run(instance: InstanceTarget) -> TaskOutcome:
        if stop.is_set():
            LOGGER.info(
                '已收到停机信号，跳过本组采集（不发任何 HTTP 请求）：实例 %s',
                instance.instance_id,
            )
            return TaskOutcome()
        return executor(instance)

    return run


# ---------------------------------------------------------------------------
# A 类：监控指标
# ---------------------------------------------------------------------------

def _monitor_executor(
    client: Any,
    mapping: Any,
    config: Any,
    clock: Callable[[], int],
) -> Callable[[InstanceTarget], TaskOutcome]:
    """A 类：监控指标接口 → 指标消息。"""

    def run(target: InstanceTarget) -> TaskOutcome:
        now_ms = int(clock())
        window = resolve_window_from_config(now_ms, config)
        collector = MonitorCollector(
            client, mapping, target.instance_id, target.instance_name,
            target.nodes, config, clock=lambda: now_ms,
        )
        messages = collector.collect()
        return TaskOutcome(
            messages=messages,
            # 取数成功（异常会向上抛 → 调度器记该组失败 → 不推进水位）。
            end_time_ms=window.end_time,
            external_calls=MONITOR_EXTERNAL_CALLS,
            fetched=len(messages),
        )

    return run


# ---------------------------------------------------------------------------
# B 类：命令中心
# ---------------------------------------------------------------------------

def _command_center_executor(
    client: Any,
    mapping: Any,
    command_center: Any,
    deltas: Any,
    clock: Callable[[], int],
) -> Callable[[InstanceTarget], TaskOutcome]:
    """B 类：命令中心合并 SQL → 指标消息。

    **采集器必须跨轮复用** —— 差分基线住在它持有的存储里，
    每轮新建一个采集器就等于每轮只有首轮（只建基线、不出速率）。
    """

    collector = CommandCenterCollector(
        client, mapping, command_center, deltas=deltas, clock=clock,
    )

    def run(target: InstanceTarget) -> TaskOutcome:
        result = collector.collect(target.instance_id, target.instance_name)
        # 取数失败时采集器返回空的 ``messages`` 且 **pages == 0**
        # （执行过至少一页才会 pages >= 1）—— 这时**不推进水位**。
        failed = result.pages <= 0
        if failed:
            LOGGER.warning(
                '命令中心本轮没有取到数据（实例 %s）—— 不推进水位，下轮重取',
                target.instance_id,
            )
        return TaskOutcome(
            messages=result.messages,
            end_time_ms=None if failed else int(clock()),
            external_calls=max(result.pages, 1),
            fetched=len(result.messages),
            windows=result.pages,
        )

    return run


# ---------------------------------------------------------------------------
# C 类：诊断优化
# ---------------------------------------------------------------------------

def _diagnostic_executor(
    client: Any,
    mapping: Any,
    config: Any,
    clock: Callable[[], int],
) -> Callable[[InstanceTarget], TaskOutcome]:
    """C 类：诊断优化接口 → 指标消息。"""

    def sanitize(text: str) -> str:
        return sanitize_sql_with_config(text, config)

    collector = DiagnosticCollector(client, mapping, sanitize, clock=clock)

    def run(target: InstanceTarget) -> TaskOutcome:
        messages = collector.collect(
            target.instance_id, target.nodes, target.instance_name,
        )
        return TaskOutcome(
            messages=messages,
            # 一条都没产出 → 取不到数据（没有落点或平台没给数）→ 不推进水位。
            end_time_ms=int(clock()) if messages else None,
            external_calls=DIAGNOSTIC_EXTERNAL_CALLS,
            fetched=len(messages),
        )

    return run


# ---------------------------------------------------------------------------
# D 类：实例元数据
# ---------------------------------------------------------------------------

def _instance_executor(
    client: Any,
    mapping: Any,
    node_registry: Any,
    clock: Callable[[], int],
) -> Callable[[InstanceTarget], TaskOutcome]:
    """D 类：实例元数据接口 → 指标消息。"""

    collector = InstanceCollector(client, mapping, node_registry, clock=clock)

    def run(target: InstanceTarget) -> TaskOutcome:
        result = collector.collect(target.instance_id)
        return TaskOutcome(
            messages=result.messages,
            # 一条都没产出 → 目标实例取不到或值取不到 → 不推进水位。
            end_time_ms=int(clock()) if result.messages else None,
            external_calls=INSTANCE_EXTERNAL_CALLS,
            fetched=len(result.messages),
        )

    return run


# ---------------------------------------------------------------------------
# E 类：告警
# ---------------------------------------------------------------------------

def _alarm_executor(
    client: Any,
    mapping: Any,
    config: Any,
    watermark: Any,
    clock: Callable[[], int],
) -> Callable[[InstanceTarget], TaskOutcome]:
    """E 类：告警 → **告警消息**（进 ``outcome.alarms``，不进 ``messages``）。"""

    def run(target: InstanceTarget) -> TaskOutcome:
        stream = alarm_stream(target.instance_id)
        cursor = watermark.alarm_watermark(stream) if watermark is not None else None
        result = collect_alarms(
            client, mapping, target.nodes, target.instance_id, cursor,
            int(clock()),
            int(getattr(config, 'alarm_retention_days', 30)),
            int(getattr(config, 'alarm_limit', 100)),
        )
        return TaskOutcome(
            alarms=result.messages,
            # 告警走自己的水位（``alarm_stream``），不推指标水位。
            end_time_ms=None,
            external_calls=ALARM_EXTERNAL_CALLS,
            fetched=result.convergence_count + result.detail_count,
        )

    return run
