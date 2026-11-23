# -*- coding: utf-8 -*-
"""**实测**单轮墙钟 —— 合同完成标准「60 秒档单轮墙钟 P95 ≤ 25 秒」。

这不是单元测试（一次要跑两分钟），是**可执行的测量脚本**：

.. code-block:: bash

    conda run -n tpops python tests/performance/measure_round_latency.py

**测的规模**：开发方案 §4.8 的实测延迟（正常负载）：

===========  ==========
通道          单次耗时
===========  ==========
A 监控指标     2 s
B 命令中心    15 s
C 诊断优化     2 s
D 实例元数据   0 s
E 告警         2 s
===========  ==========

**为什么用「睡过去」而不是连真平台**：真平台的延迟由**平台**决定，
不是采集端能控制的；这里要验的是**采集端的并发结构** ——
一轮的墙钟应该是「最慢的那一路」，而不是「各路之和」。
（真平台的延迟另由 `data/merged_sql/` 留档与开发方案 §4.8 记录。）

**输出**：每轮墙钟、P95、是否在预算内。退出码非 0 = 超预算。
"""
from __future__ import absolute_import

import logging
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from tpops_kafka_collector.schedule import (  # noqa: E402
    CHANNEL_ALARM,
    CHANNEL_COMMAND_CENTER,
    CHANNEL_DIAGNOSTIC,
    CHANNEL_INSTANCE,
    CHANNEL_MONITOR,
    ChannelPlan,
)
from tpops_kafka_collector.scheduler import Scheduler  # noqa: E402
from tpops_kafka_collector.scheduler_types import (  # noqa: E402
    InstanceTarget,
    TaskOutcome,
)
from tpops_kafka_collector.watermark import WatermarkStore  # noqa: E402

#: 开发方案 §4.8 的实测延迟（秒）。
LATENCY_SECONDS = {
    CHANNEL_MONITOR: 2.0,
    CHANNEL_COMMAND_CENTER: 15.0,
    CHANNEL_DIAGNOSTIC: 2.0,
    CHANNEL_INSTANCE: 0.0,
    CHANNEL_ALARM: 2.0,
}

#: 合同完成标准的预算（秒）。
BUDGET_SECONDS = 25.0

#: 跑几轮取 P95。
ROUNDS = 8


class _SleepingExecutor:
    """按给定时长「采集」的执行器。"""

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


def percentile(values, fraction):
    """最近秩法取分位数（样本少时比线性插值更稳）。"""
    ordered = sorted(values)
    if not ordered:
        return 0.0
    rank = int(-(-len(ordered) * fraction // 1))  # 向上取整
    index = min(max(rank, 1), len(ordered)) - 1
    return ordered[index]


def build_scheduler():
    """全部通道周期设为 1 秒 → 每轮**全部到期**（测的是并发结构）。"""
    plans = tuple(
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
    state_dir = tempfile.mkdtemp(prefix='tpops-latency-')
    store = WatermarkStore(os.path.join(state_dir, 'watermark.json'))
    executors = dict(
        (channel, _SleepingExecutor(seconds))
        for channel, seconds in LATENCY_SECONDS.items()
    )
    return Scheduler(
        plans=plans,
        executors=executors,
        producer=_NullProducer(),
        watermark=store,
        instances=(InstanceTarget(instance_id='inst-1'),),
        tick_interval_seconds=1,
    )


def main():
    # 假执行器不产出消息 → 投递层会记「空结果」WARNING —— 与本测量无关，压掉。
    logging.disable(logging.WARNING)
    scheduler = build_scheduler()
    slowest = max(LATENCY_SECONDS.values())
    serial = sum(LATENCY_SECONDS.values())

    print('通道延迟（开发方案 §4.8）：%s' % LATENCY_SECONDS)
    print('串行合计 = %.1fs ；并发下界 = %.1fs ；预算 = %.1fs'
          % (serial, slowest, BUDGET_SECONDS))
    print('轮次  单轮墙钟(s)')
    samples = []
    for index in range(ROUNDS):
        started = time.monotonic()
        scheduler.tick()
        elapsed = time.monotonic() - started
        samples.append(elapsed)
        print('%4d  %.3f' % (index + 1, elapsed))

    p95 = percentile(samples, 0.95)
    print('')
    print('最小 = %.3fs  最大 = %.3fs  **P95 = %.3fs**  预算 = %.1fs'
          % (min(samples), max(samples), p95, BUDGET_SECONDS))
    if p95 > BUDGET_SECONDS:
        print('❌ 超预算')
        return 1
    print('✅ 在预算内（余量 %.1fs）' % (BUDGET_SECONDS - p95))
    return 0


if __name__ == '__main__':
    sys.exit(main())
