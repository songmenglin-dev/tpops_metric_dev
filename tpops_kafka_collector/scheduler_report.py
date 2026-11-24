"""每轮的**汇总日志**与**进程自愈**告警（开发方案 §4.9.1 / §4.7）。

拆出来是因为 ``scheduler.py`` 越过合同「单文件 < 400 行」，
而「怎么汇总、怎么告警」与「怎么调度」是两件事。

**字段名不自定义** —— 开发方案 §4.9.1 已经定死：

``tick`` ``elapsed`` ``fetched`` ``mapped`` ``produced`` ``failed``
``dropped_unsupported`` ``windows`` ``alarms``

规格 ``specs/delivery-and-state/spec.md`` 再点名两个：
``calls``（外部调用次数）与 ``delivery_rate``（投递成功率）。
"""
import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

__all__ = [
    'CONSECUTIVE_FAILURE_ROUNDS',
    'EXTERNAL_CALL_BUDGET',
    'log_round_summary',
    'note_round_health',
]

#: 单轮外部调用数预算（规格「单轮外部调用数与投递成功率可观测」）。
EXTERNAL_CALL_BUDGET = 42

#: 连续多少轮全失败就打 ERROR 日警（开发方案 §4.7「进程自愈」——**不退出**）。
CONSECUTIVE_FAILURE_ROUNDS = 3


def log_round_summary(
    result: Any,
    dropped_unsupported: int,
    dropped_cumulative: int,
) -> int:
    """记每轮汇总日志；返回**更新后的**累计丢弃数。"""
    planned = result.mapped
    rate = (result.produced / float(planned)) if planned else 1.0
    LOGGER.info(
        'tick=%d elapsed=%.1fs fetched=%d mapped=%d produced=%d failed=%d '
        'dropped_unsupported=%d windows=%d alarms=%d calls=%d '
        'delivery_rate=%.3f groups=%s skipped=%s',
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
        '不可提供指标 %d 条（本轮不产出，累计 %d）',
        dropped_unsupported, cumulative,
    )
    if result.external_calls > EXTERNAL_CALL_BUDGET:
        LOGGER.warning(
            '单轮外部调用数 %d 超出预算 %d —— 检查是否有重复采集',
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
        '连续 %d 轮采集全失败，采集链路可能已中断'
        '（进程不退出，交由 supervisor 重启）',
        count,
    )
    return count
