# -*- coding: utf-8 -*-
"""单轮墙钟预算（合同完成标准：「60 秒档单轮墙钟 P95 ≤ 25 秒」）。

**测的是什么**：一轮里「通道 × 实例」的任务**并发**跑完的墙钟。
合同与开发方案 §4.8 给的实测延迟（正常负载）：

============  ==========  ==========
通道          单次耗时      串行合计
============  ==========  ==========
A 监控指标     2 s          2 s
B 命令中心    15 s         15 s
C 诊断优化     2 s          2 s
D 实例元数据   0 s          0 s
E 告警         2 s          2 s
============  ==========  ==========

串行 = 21 s（逼近 30 秒 tick 边界），并发 = max = **15 s**（2 倍余量）。
**这条用例守的是「并发」这个结构**：把线程池换成串行，墙钟会变成 21 s。

本文件用**缩小 10 倍**的延迟跑（0.2 / 1.5 / 0.2 / 0 / 0.2 秒）——
**比例关系**（并发 ≈ max，不是 sum）与**预算占比**是尺度无关的，
所以缩放不影响结论；而**真实规模**的数字由
``tests/performance/measure_round_latency.py`` 跑出来（一次约 2 分钟）。
"""
from __future__ import absolute_import

import time

from tpops_kafka_collector.schedule import (
    CHANNEL_ALARM,
    CHANNEL_COMMAND_CENTER,
    CHANNEL_DIAGNOSTIC,
    CHANNEL_INSTANCE,
    CHANNEL_MONITOR,
    ChannelPlan,
)
from tpops_kafka_collector.scheduler import Scheduler
from tpops_kafka_collector.scheduler_types import InstanceTarget, TaskOutcome
from tpops_kafka_collector.watermark import WatermarkStore

import os
import tempfile

#: 开发方案 §4.8 的实测延迟（秒）—— **缩放 10 倍**后用于快跑。
SCALED_LATENCY_SECONDS = {
    CHANNEL_MONITOR: 0.2,
    CHANNEL_COMMAND_CENTER: 1.5,
    CHANNEL_DIAGNOSTIC: 0.2,
    CHANNEL_INSTANCE: 0.0,
    CHANNEL_ALARM: 0.2,
}

#: 缩放前的真实规模延迟（秒）—— 供对照。
REAL_LATENCY_SECONDS = {
    CHANNEL_MONITOR: 2.0,
    CHANNEL_COMMAND_CENTER: 15.0,
    CHANNEL_DIAGNOSTIC: 2.0,
    CHANNEL_INSTANCE: 0.0,
    CHANNEL_ALARM: 2.0,
}

#: 合同完成标准：60 秒档单轮墙钟 P95 ≤ 25 秒。
BUDGET_SECONDS = 25.0

#: tick 周期（秒）—— ``config.poll_interval_seconds`` 的默认值。
TICK_INTERVAL_SECONDS = 30.0

#: 缩放因子（真实延迟 ÷ 缩放延迟）。
SCALE = 10.0


class _SleepingExecutor:
    """按给定时长「采集」的执行器（睡过去，模拟 I/O 等待）。"""

    def __init__(self, seconds):
        self.seconds = seconds

    def __call__(self, instance):
        if self.seconds:
            time.sleep(self.seconds)
        return TaskOutcome(end_time_ms=1)


class _NullProducer:
    def produce_metrics(self, records):
        return None

    def produce_alarms(self, records):
        return None

    def close(self):
        pass


def _plans():
    """全部通道的周期都设成 1 秒 —— 让每一轮都**全部到期**（测的是并发结构）。"""
    return tuple(
        ChannelPlan(
            channel=channel,
            interval_seconds=1,
            tier_seconds=1,
            metric_count=1,
        )
        for channel in (
            CHANNEL_MONITOR, CHANNEL_COMMAND_CENTER, CHANNEL_DIAGNOSTIC,
            CHANNEL_INSTANCE, CHANNEL_ALARM,
        )
    )


def _scheduler(latencies):
    state_dir = tempfile.mkdtemp(prefix='tpops-latency-')
    store = WatermarkStore(os.path.join(state_dir, 'watermark.json'))
    executors = dict(
        (channel, _SleepingExecutor(seconds))
        for channel, seconds in latencies.items()
    )
    return Scheduler(
        plans=_plans(),
        executors=executors,
        producer=_NullProducer(),
        watermark=store,
        instances=(InstanceTarget(instance_id='inst-1'),),
        tick_interval_seconds=1,
    )


def test_a_round_runs_the_channels_concurrently_not_serially():
    """一轮的墙钟 ≈ **最慢的那个通道**，不是各通道之和。"""
    scheduler = _scheduler(SCALED_LATENCY_SECONDS)

    started = time.monotonic()
    scheduler.tick()
    elapsed = time.monotonic() - started

    serial = sum(SCALED_LATENCY_SECONDS.values())
    slowest = max(SCALED_LATENCY_SECONDS.values())

    assert elapsed < serial * 0.75, (
        '一轮墙钟 %.2fs 接近串行合计 %.2fs —— 通道之间没有并发'
        % (elapsed, serial)
    )
    assert elapsed < slowest * 2.0, (
        '一轮墙钟 %.2fs 远超最慢通道 %.2fs —— 并发没生效' % (elapsed, slowest)
    )


def test_a_round_stays_well_inside_the_scaled_budget():
    """缩放后的墙钟 ≤ 缩放后的预算（真实规模的等价关系）。"""
    scheduler = _scheduler(SCALED_LATENCY_SECONDS)

    started = time.monotonic()
    scheduler.tick()
    elapsed = time.monotonic() - started

    scaled_budget = BUDGET_SECONDS / SCALE
    assert elapsed <= scaled_budget, (
        '一轮墙钟 %.2fs 超出缩放后的预算 %.2fs' % (elapsed, scaled_budget)
    )


def test_the_documented_latencies_concur_within_the_real_budget():
    """**按文档的真实延迟**推算：并发一轮 ≈ 15 s ≤ 25 s（合同完成标准）。

    这是**纸面推算**（不真等 15 秒），但它把「开发方案 §4.8 的延迟」
    与「合同的 25 秒预算」「30 秒 tick 周期」这两条**外部给定的数字**
    接在一起 —— 改了任一条，这条断言就会红。

    **「必须并发」的前提**：串行合计（21 s）会吃掉 30 秒 tick 周期的
    大半（余量不足 1/3），而且在 B 类命令中心那 8~15 秒的波动下
    随时可能冲过 25 秒的预算；并发后只剩最慢的那一路（15 s），
    对 30 秒 tick 有 **2 倍余量**（开发方案 §4.6.1 的原话）。
    """
    slowest = max(REAL_LATENCY_SECONDS.values())
    serial = sum(REAL_LATENCY_SECONDS.values())

    assert slowest <= BUDGET_SECONDS, (
        '最慢的通道 %.1fs 就已经超预算 %.1fs —— 并发也救不了'
        % (slowest, BUDGET_SECONDS)
    )
    assert serial > TICK_INTERVAL_SECONDS / 3.0 * 2, (
        '串行合计 %.1fs 并没有逼近 %.0fs 的 tick 边界 —— '
        '「必须并发」这个前提不成立，本用例失去意义'
        % (serial, TICK_INTERVAL_SECONDS)
    )
    # 并发之后离 tick 边界要留 **2 倍余量**（开发方案 §4.6.1）。
    assert slowest * 2 <= TICK_INTERVAL_SECONDS, (
        '并发一轮 %.1fs 对 %.0fs 的 tick 周期留不到 2 倍余量'
        % (slowest, TICK_INTERVAL_SECONDS)
    )
