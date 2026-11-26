"""每轮的**汇总日志**与**进程自愈**告警（开发方案 §4.9.1 / §4.7）。

拆出来是因为 ``scheduler.py`` 越过合同「单文件 < 400 行」，
而「怎么汇总、怎么告警」与「怎么调度」是两件事。

**字段名不自定义** —— 开发方案 §4.9.1 已经定死：

``tick`` ``elapsed`` ``fetched`` ``mapped`` ``produced`` ``failed``
``dropped_unsupported`` ``windows`` ``alarms``

规格 ``specs/delivery-and-state/spec.md`` 再点名两个：
``calls``（外部调用次数）与 ``delivery_rate``（投递成功率）。
"""
from typing import Any, Sequence

from .scheduler_types import ChannelRoundStats
from loguru import logger

LOGGER = logger

__all__ = [
    'CONSECUTIVE_FAILURE_ROUNDS',
    'EXTERNAL_CALL_BUDGET',
    'log_channel_summaries',
    'log_round_summary',
    'note_round_health',
]

#: 每通道汇总行的字段（顺序固定，名字不自定义）。
CHANNEL_SUMMARY_FORMAT = (
    'channel={} tick={} instances={} fetched={} mapped={} produced={} '
    'failed={} windows={} calls={} elapsed={:.1f}s delivery_rate={:.3f}'
)

#: 单轮外部调用数预算（规格「单轮外部调用数与投递成功率可观测」）。
EXTERNAL_CALL_BUDGET = 42

#: 连续多少轮全失败就打 ERROR 日警（开发方案 §4.7「进程自愈」——**不退出**）。
CONSECUTIVE_FAILURE_ROUNDS = 3


def log_channel_summaries(stats: Sequence[ChannelRoundStats]) -> None:
    """每通道一行汇总（决策 6）。

    同一个通道的行打**两次**：

    - 一次绑 ``channel=…`` → 进**该通道日志**（明细那一侧）；
    - 一次只绑 ``collector.summary`` → 进**总日志**（一眼看全各通道的量）。

    两行的**字段完全相同**（纯 ``k=v`` token，值里不含空格），
    这样 JSON 模式下能摊平成顶层字段。
    """
    for item in stats:
        args = (
            item.channel, item.tick, item.instances, item.fetched, item.mapped,
            item.produced, item.failed, item.windows, item.calls,
            item.elapsed, item.delivery_rate,
        )
        LOGGER.bind(channel=item.channel).info(CHANNEL_SUMMARY_FORMAT, *args)
        LOGGER.bind(logger_name='collector.summary').info(CHANNEL_SUMMARY_FORMAT, *args)


def log_round_summary(
    result: Any,
    dropped_unsupported: int,
    dropped_cumulative: int,
    channel_stats: Sequence[ChannelRoundStats] = (),
) -> int:
    """记每轮汇总日志；返回**更新后的**累计丢弃数。"""
    log_channel_summaries(channel_stats)
    planned = result.mapped
    rate = (result.produced / float(planned)) if planned else 1.0
    LOGGER.info(
        'tick={} elapsed={:.1f}s fetched={} mapped={} produced={} failed={} '
        'dropped_unsupported={} windows={} alarms={} calls={} '
        'delivery_rate={:.3f} groups={} skipped={}',
        result.tick_index,
        result.duration_seconds,
        result.fetched,
        result.mapped,
        result.produced,
        result.failed,
        dropped_unsupported,
        result.windows,
        result.alarms,
        result.external_calls,
        rate,
        ','.join(result.executed) or '-',
        ','.join(result.skipped) or '-',
    )
    cumulative = dropped_cumulative + dropped_unsupported
    LOGGER.info(
        '不可提供指标 {} 条（本轮不产出，累计 {}）',
        dropped_unsupported, cumulative,
    )
    if result.external_calls > EXTERNAL_CALL_BUDGET:
        LOGGER.warning(
            '单轮外部调用数 {} 超出预算 {} —— 检查是否有重复采集',
            result.external_calls, EXTERNAL_CALL_BUDGET,
        )
    return cumulative


def note_round_health(result: Any, consecutive_failures: int) -> int:
    """进程自愈：连续 N 轮全失败 → ERROR 日警（**进程不退出**）。

    :returns: 更新后的「连续全失败轮数」（成功一轮就归零）。
    """
    all_failed = (
        result.tasks_total > 0 and result.tasks_failed >= result.tasks_total
    )
    count = consecutive_failures + 1 if all_failed else 0
    if count < CONSECUTIVE_FAILURE_ROUNDS:
        return count
    if count % CONSECUTIVE_FAILURE_ROUNDS != 0:
        return count
    LOGGER.error(
        '连续 {} 轮采集全失败，采集链路可能已中断'
        '（进程不退出，交由 supervisor 重启）',
        count,
    )
    return count
