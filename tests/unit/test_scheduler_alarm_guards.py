# -*- coding: utf-8 -*-
"""告警通道的**不变量守卫**（与指标侧对称）。

指标侧一直有两条守卫守着水位口径
（``tests/unit/test_scheduler.py``）：

- ``test_watermark_advances_only_after_successful_delivery``
- ``test_watermark_does_not_advance_when_delivery_fails``

**告警侧一条都没有** —— 变异 M11（在告警投递失败分支里也推进告警水位）
会让全套测试**照样全绿**（覆盖率也证明：``scheduler_delivery.py`` 的未覆盖行
正好是「告警去重 + 告警失败分支」）。

这两条不变量的破坏后果是**告警整条静默丢失**：

1. **投递失败却推进了告警水位** → 下一轮游标过滤把**从未投出**的告警
   判成「不新」→ 不再重发 → **静默丢失**；日志里只剩一条
   ``LOGGER.exception``，**没有任何计数**能暴露它；
2. **幂等去重（批内 ``seen`` + 跨轮 ``has_key``）被删** → 同一条告警
   **每 30 秒重发一次** —— 告警是持续事件流，重复量级远高于指标。

这些守卫**复用指标侧同一套夹具**（``unit.test_scheduler``），保证两侧口径一致；
夹具里的 ``FakeProducer`` 已支持 ``fail_alarm_batches``（可指定「第 N 批告警投递失败」）。
"""
from __future__ import absolute_import

from tpops_kafka_collector.schedule import CHANNEL_ALARM
from tpops_kafka_collector.scheduler_types import TaskOutcome, alarm_stream
from tpops_kafka_collector.watermark import WatermarkStore
from unit.test_scheduler import (
    INSTANCE_A,
    FakeClock,
    FakeProducer,
    alarm_message,
    make_plans,
    make_scheduler,
)

#: 告警通道的调度计划（周期 30 秒，与生产口径一致）。
ALARM_PLAN = ((CHANNEL_ALARM, 30),)


def _alarm_scheduler(store, producer, clock):
    """装配一个只跑告警通道的调度器（执行器每轮产出同一条告警）。"""
    return make_scheduler(
        {CHANNEL_ALARM: lambda instance: TaskOutcome(alarms=(alarm_message(),))},
        producer=producer,
        store=store,
        clock=clock,
        plans=make_plans(ALARM_PLAN),
    )


# ---------------------------------------------------------------------------
# 硬约束 1：告警投递失败**不推进告警水位**，且下轮重发
# ---------------------------------------------------------------------------

def test_alarm_watermark_does_not_advance_when_alarm_delivery_fails(tmp_path):
    """告警投递失败 → **告警水位不推进**。

    推进了的话，下一轮的游标过滤会把**从未投出**的告警判成「不新」→
    告警**整条静默丢失**（日志里只有一条 ``LOGGER.exception``，无计数可查）。
    """
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    producer = FakeProducer(fail_alarm_batches={0})
    clock = FakeClock()
    scheduler = _alarm_scheduler(store, producer, clock)

    result = scheduler.tick()

    assert result.failed == 1, '投递失败没有进失败清单'
    assert store.alarm_watermark(alarm_stream(INSTANCE_A)) is None, (
        '告警投递失败却推进了告警水位 —— 从未投出的告警会被下一轮判成「不新」'
    )


def test_failed_alarm_batch_is_retried_in_the_next_round(tmp_path):
    """投递失败的那批告警**下轮重发**（幂等键没被登记 → 不会被当成重复跳过）。"""
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    producer = FakeProducer(fail_alarm_batches={0})
    clock = FakeClock()
    scheduler = _alarm_scheduler(store, producer, clock)

    scheduler.tick()
    clock.advance(30)
    scheduler.tick()

    assert len(producer.alarm_batches) == 2, '失败的那批告警没有重发'
    assert len(producer.alarm_batches[1]) == 1
    assert store.alarm_watermark(alarm_stream(INSTANCE_A)) == '1000', (
        '重发成功之后应该推进告警水位'
    )


# ---------------------------------------------------------------------------
# 硬约束 2：告警的幂等去重（批内 + 跨轮）
# ---------------------------------------------------------------------------

def test_duplicate_alarm_keys_inside_one_batch_are_delivered_once():
    """同一批里两条**相同幂等键**的告警 → 只投一条。"""
    producer = FakeProducer()
    scheduler = make_scheduler(
        {
            CHANNEL_ALARM: lambda instance: TaskOutcome(
                alarms=(alarm_message(), alarm_message()),
            ),
        },
        producer=producer,
        plans=make_plans(ALARM_PLAN),
    )

    scheduler.tick()

    assert len(producer.alarm_batches[0]) == 1, (
        '同一批里相同幂等键的告警投了多条 —— 去重（批内 seen）没了'
    )


def test_already_delivered_alarms_are_skipped_in_the_next_round(tmp_path):
    """已投递过的告警键 → 下一轮**跳过**，不重复投递。

    告警是持续事件流（平台侧 257 条定义）—— 跨轮去重（``has_key``）被删的话，
    同一条告警**每 30 秒重发一次**。
    """
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    producer = FakeProducer()
    clock = FakeClock()
    scheduler = _alarm_scheduler(store, producer, clock)

    scheduler.tick()
    clock.advance(30)
    result = scheduler.tick()

    assert len(producer.alarm_batches) == 1, '重复投递了告警'
    assert result.produced == 0, '重复投递了告警'
