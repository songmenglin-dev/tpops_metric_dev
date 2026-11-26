"""调度计划 —— **从映射表实算**每个通道的周期。

周期**不写死**：每个通道的周期由映射表 ``data/mapping/metric_rules.json``
的 ``collect_cycle`` 决定 —— 取该通道内**最高需求档**（= 最短周期）的那个值。

依据：开发方案 §4.6.3「一条合并 SQL 的刷新周期 = 它覆盖的指标中最高需求档
对应的周期」。这里的「最高需求」= **最短周期**（最急的那个指标决定刷新频率）。

告警通道（E 类）没有客户指标、没有 ``collect_cycle`` —— 它的周期取
开发方案 §4.6.2 的 30 秒。

**24 小时档（86400）无处安放**（开发方案 §2584 已记录）—— 本模块**不发明口径**：
命令中心通道的周期取其覆盖指标的最短档（60 秒），24 小时档的指标随之被
60 秒的刷新顺带覆盖。按 §4.6.3 的「新鲜度检查」口径，这意味着缓存结果的
采集时刻（60 秒）远小于该指标的档位周期（86400 秒）→ **不需要提前刷新**，
该档位自然落在 60 秒的刷新里。
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from loguru import logger

from .collectors.instance import collect_cycle_seconds

LOGGER = logger

__all__ = [
    'ALARM_INTERVAL_SECONDS',
    'DueTable',
    'CHANNEL_ALARM',
    'CHANNEL_COMMAND_CENTER',
    'CHANNEL_DIAGNOSTIC',
    'CHANNEL_INSTANCE',
    'CHANNEL_MONITOR',
    'CHANNEL_ORDER',
    'ChannelPlan',
    'SOURCE_TO_CHANNEL',
    'ScheduleEntry',
    'compute_channel_plans',
    'count_unsupported_metrics',
]

#: 告警通道的周期（秒）—— 开发方案 §4.6.2。
ALARM_INTERVAL_SECONDS = 30

#: 监控指标通道（A 类）。
CHANNEL_MONITOR = 'monitor'
#: 命令中心通道（B 类）。
CHANNEL_COMMAND_CENTER = 'command_center'
#: 诊断优化通道（C 类）。
CHANNEL_DIAGNOSTIC = 'diagnostic'
#: 实例元数据通道（D 类）。
CHANNEL_INSTANCE = 'instance'
#: 告警通道（E 类）。
CHANNEL_ALARM = 'alarm'

#: 映射表 ``source.source`` → 调度通道。
SOURCE_TO_CHANNEL = {
    'monitor_metrics': CHANNEL_MONITOR,
    'command_center_sql': CHANNEL_COMMAND_CENTER,
    'diagnostic': CHANNEL_DIAGNOSTIC,
    'instance': CHANNEL_INSTANCE,
}

#: 通道的固定顺序（调度表与日志按它排，保证同一份输入的输出可复现）。
CHANNEL_ORDER = (
    CHANNEL_MONITOR,
    CHANNEL_COMMAND_CENTER,
    CHANNEL_DIAGNOSTIC,
    CHANNEL_INSTANCE,
    CHANNEL_ALARM,
)


@dataclass(frozen=True)
class ChannelPlan:
    """一个通道的调度计划（不可变）。

    :param channel: 通道名（``monitor`` / ``command_center`` / …）。
    :param interval_seconds: 该通道的到期周期（秒）—— **实算**得来。
    :param tier_seconds: 该通道覆盖的最快档位（秒）—— 与周期同值。
    :param metric_count: 该通道覆盖的客户指标条数。
    :param tiers: 该通道覆盖的全部档位（秒，升序、去重）。
    """

    channel: str
    interval_seconds: int
    tier_seconds: int
    metric_count: int
    tiers: Tuple[int, ...] = ()


@dataclass(frozen=True)
class ScheduleEntry:
    """到期表里的一条（不可变）。

    :param task: 任务名（= 通道名）。
    :param interval_seconds: 该任务的到期周期（秒）。
    :param next_due_at: 下次到期的时间戳（**单调时钟**，秒）。
    """

    task: str
    interval_seconds: int
    next_due_at: float


def compute_channel_plans(
    mapping: Any,
    alarm_interval_seconds: int = ALARM_INTERVAL_SECONDS,
) -> Tuple[ChannelPlan, ...]:
    """从映射表实算每个通道的调度计划。

    :param mapping: 已加载的映射表（``mapping.loader.load_mapping()`` 的返回值）。
    :param alarm_interval_seconds: 告警通道的周期（秒）。
    :returns: 按 :data:`CHANNEL_ORDER` 排好的计划元组（告警通道恒在最后）。
    """
    buckets = _tiers_by_channel(mapping)
    plans = []  # type: List[ChannelPlan]
    for channel in CHANNEL_ORDER:
        if channel == CHANNEL_ALARM:
            continue
        tiers = buckets.get(channel) or []
        if not tiers:
            continue
        plans.append(ChannelPlan(
            channel=channel,
            interval_seconds=min(tiers),
            tier_seconds=min(tiers),
            metric_count=len(tiers),
            tiers=tuple(sorted(set(tiers))),
        ))
    plans.append(ChannelPlan(
        channel=CHANNEL_ALARM,
        interval_seconds=int(alarm_interval_seconds),
        tier_seconds=int(alarm_interval_seconds),
        metric_count=0,
        tiers=(),
    ))
    return tuple(plans)


def _tiers_by_channel(mapping: Any) -> Dict[str, List[int]]:
    """按通道把启用规则的 ``collect_cycle`` 折成秒并分桶。"""
    buckets = {}  # type: Dict[str, List[int]]
    for rule in getattr(mapping, 'metric_rules', None) or ():
        if not getattr(rule, 'enabled', False):
            continue
        source = getattr(rule, 'source', None) or {}
        raw_channel = source.get('source')
        channel = SOURCE_TO_CHANNEL.get(raw_channel)
        if channel is None:
            LOGGER.warning(
                '映射表里有认不出的取数通道 {!r}（指标 {}）—— 不参与调度',
                raw_channel, getattr(rule, 'metric_code', '?'),
            )
            continue
        tier = collect_cycle_seconds(getattr(rule, 'collect_cycle', None))
        if tier <= 0:
            LOGGER.warning(
                '指标 {} 没有可用的 collect_cycle —— 不参与调度',
                getattr(rule, 'metric_code', '?'),
            )
            continue
        buckets.setdefault(channel, []).append(tier)
    return buckets


def plan_by_channel(
    plans: Tuple[ChannelPlan, ...],
) -> Dict[str, ChannelPlan]:
    """计划元组 → 通道名索引的字典。"""
    return dict((plan.channel, plan) for plan in plans)


def optional_plan(
    plans: Tuple[ChannelPlan, ...], channel: str
) -> Optional[ChannelPlan]:
    """取某个通道的计划；没有则返回 ``None``。"""
    return plan_by_channel(plans).get(channel)


def count_unsupported_metrics(mapping: Any) -> int:
    """客户指标库里「无法提供」的指标条数（本项目实测 3 条）。

    这 3 条**丢弃**（FR-13）—— 采集端不产出它们的消息，但要**计数上报**
    （开发方案 §4.9.1 的 ``dropped_unsupported``）。

    数据来源：``LoadedMapping.customer_metrics``（每条带 ``status``）。
    """
    unsupported = 0
    for metric in getattr(mapping, 'customer_metrics', None) or ():
        status = str(getattr(metric, 'status', '') or '')
        if status and status != '可提供':
            unsupported += 1
    return unsupported


class DueTable:
    """到期表 —— 每个通道的**下次到期时刻**。

    到期驱动（而不是「每轮全跑」）的载体：每个任务有自己的到期时刻，
    未到期的**不执行**（规格「调度器的到期驱动与并发组」）。

    首次构造时**全部到期** —— 进程启动后第一轮就把数据拉起来，
    不用先空跑一个周期。
    """

    def __init__(self, plans: Sequence[ChannelPlan], now: float) -> None:
        self._entries = dict(
            (plan.channel, ScheduleEntry(
                task=plan.channel,
                interval_seconds=plan.interval_seconds,
                next_due_at=now,
            ))
            for plan in plans
        )

    def split_due(self, now: float) -> Tuple[List[str], List[str]]:
        """挑出本 tick 到期的通道，并把它们的下次到期时刻推后。

        :returns: ``(到期的通道, 未到期的通道)``。
        """
        due = []
        skipped = []
        for channel in sorted(self._entries):
            entry = self._entries[channel]
            if entry.next_due_at <= now:
                due.append(channel)
                self._entries[channel] = ScheduleEntry(
                    task=entry.task,
                    interval_seconds=entry.interval_seconds,
                    next_due_at=now + entry.interval_seconds,
                )
            else:
                skipped.append(channel)
        return due, skipped

    def entries(self) -> Tuple[ScheduleEntry, ...]:
        """当前到期表的不可变快照。"""
        return tuple(self._entries[channel] for channel in sorted(self._entries))
