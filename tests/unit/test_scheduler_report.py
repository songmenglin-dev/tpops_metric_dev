# -*- coding: utf-8 -*-
"""每轮汇总日志的守卫（wave-3 修复轮 R-B）。

守的是两件事：

1. **单轮外部调用数超预算 → 记 WARNING** —— 规格
   ``specs/delivery-and-state/spec.md``「单轮外部调用数与投递成功率可观测」；
   修复前上报的是**编造的常数**（合计 20），42 次预算**永远不响**；
2. **「不可提供指标」的措辞** —— 那是**静态计数**（恒为 3），
   不能写成「本轮丢弃 3 个」，否则会被读成「这轮丢了 3 条消息」。
"""
import logging

from tpops_kafka_collector.scheduler_report import (
    EXTERNAL_CALL_BUDGET,
    log_round_summary,
)
from tpops_kafka_collector.scheduler_types import TickResult


def _result(external_calls=0, produced=10, mapped=10):
    return TickResult(
        tick_index=7,
        executed=('monitor',),
        skipped=(),
        mapped=mapped,
        produced=produced,
        failed=0,
        fetched=100,
        windows=2,
        alarms=1,
        external_calls=external_calls,
        duration_seconds=1.5,
        tasks_total=1,
        tasks_failed=0,
    )


# ---------------------------------------------------------------------------
# 预算告警
# ---------------------------------------------------------------------------

def test_warning_is_logged_when_calls_exceed_budget(caplog):
    """真实调用数超预算 → WARNING。"""
    with caplog.at_level(logging.WARNING):
        log_round_summary(_result(external_calls=EXTERNAL_CALL_BUDGET + 1), 3, 0)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, '超预算必须记 WARNING'
    assert str(EXTERNAL_CALL_BUDGET) in warnings[0].getMessage()


def test_no_warning_when_calls_are_within_budget(caplog):
    """刚好在预算内 → 不记 WARNING。"""
    with caplog.at_level(logging.WARNING):
        log_round_summary(_result(external_calls=EXTERNAL_CALL_BUDGET), 3, 0)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert not warnings, warnings


def test_round_summary_logs_the_real_call_count(caplog):
    """汇总日志里的 ``calls=`` 是**真实**的调用数。"""
    with caplog.at_level(logging.INFO):
        log_round_summary(_result(external_calls=37), 3, 0)

    summary = [
        r.getMessage() for r in caplog.records
        if 'tick=' in r.getMessage()
    ]
    assert summary, '每轮必须有汇总日志'
    assert 'calls=37' in summary[0]


# ---------------------------------------------------------------------------
# 措辞
# ---------------------------------------------------------------------------

def test_unsupported_metric_count_is_not_phrased_as_dropped_this_round(caplog):
    """不可提供指标是**静态计数** —— 措辞不能写成「本轮丢弃」。"""
    with caplog.at_level(logging.INFO):
        log_round_summary(_result(external_calls=1), 3, 6)

    messages = [r.getMessage() for r in caplog.records]
    joined = '\n'.join(messages)
    assert '本轮丢弃' not in joined
    assert '不可提供指标 3 条' in joined
    assert '累计 9' in joined


def test_cumulative_counter_is_updated_by_the_static_count(caplog):
    """累计数 = 上次累计 + 本次的静态条数。"""
    with caplog.at_level(logging.INFO):
        cumulative = log_round_summary(_result(external_calls=1), 3, 6)

    assert cumulative == 9
