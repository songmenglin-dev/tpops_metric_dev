# -*- coding: utf-8 -*-
"""投递层「空结果告警」的通道区分。

**背景**：`scheduler_delivery.py::_deliver_metrics` 在「该组一条指标消息都没有」时
记一条 WARNING「本轮该组没有产出任何指标消息（空结果）」——
这是给**指标通道**（monitor / command_center / diagnostic / instance）准备的。

但**告警通道**（``alarm``）的消息走 ``TaskOutcome.alarms``，
``TaskOutcome.messages`` 本来就是空的 —— 不排除它的话，
**每 30 秒**就会多出一条这条无意义的 WARNING，把真告警
（「指标分组 X 返回空序列」「历史告警超出留存窗口」…）淹掉。

这些测试守住「告警通道不刷这条 WARNING、指标通道照刷」。
"""
from __future__ import absolute_import

import logging

from tpops_kafka_collector.scheduler_delivery import MessageDelivery
from tpops_kafka_collector.scheduler_types import TaskOutcome

EMPTY_RESULT_WARNING = '没有产出任何指标消息'


class _FakeProducer:
    """只记录调用、不真的连 Kafka 的生产者。"""

    def __init__(self):
        self.metric_batches = []
        self.alarm_batches = []

    def produce_metrics(self, records):
        self.metric_batches.append(tuple(records))
        return None

    def produce_alarms(self, records):
        self.alarm_batches.append(tuple(records))
        return None


class _FakeWatermark:
    """只实现投递层用到的那几个方法。"""

    def __init__(self):
        self.keys = set()
        self.metric_watermarks = {}
        self.alarm_watermarks = {}

    def has_key(self, key):
        return key in self.keys

    def remember_key(self, key):
        self.keys.add(key)
        return True

    def advance_metric(self, stream, end_time_ms):
        self.metric_watermarks[stream] = end_time_ms

    def advance_alarm(self, stream, last_event_time):
        self.alarm_watermarks[stream] = last_event_time


def _delivery():
    return MessageDelivery(_FakeProducer(), _FakeWatermark())


def test_alarm_channel_does_not_log_the_empty_metric_result_warning(caplog):
    """告警通道没有指标消息是**正常的** —— 不该刷这条 WARNING。"""
    delivery = _delivery()
    with caplog.at_level(logging.WARNING, logger='tpops_kafka_collector.scheduler_delivery'):
        delivery.deliver('alarm', 'inst-1', TaskOutcome())
    offenders = [r for r in caplog.records if EMPTY_RESULT_WARNING in r.getMessage()]
    assert not offenders, (
        '告警通道刷了「没有产出任何指标消息」的 WARNING —— 每 30 秒一条，会淹掉真告警：%s'
        % [r.getMessage() for r in offenders]
    )


def test_metric_channel_still_logs_the_empty_result_warning(caplog):
    """**指标通道**一条都没产出仍然要记 WARNING（不能一起被消掉）。"""
    delivery = _delivery()
    with caplog.at_level(logging.WARNING, logger='tpops_kafka_collector.scheduler_delivery'):
        delivery.deliver('monitor', 'inst-1', TaskOutcome())
    offenders = [r for r in caplog.records if EMPTY_RESULT_WARNING in r.getMessage()]
    assert offenders, '指标通道空结果必须记 WARNING —— 「空结果不能静默返回空列表」'


def test_alarm_channel_with_alarm_messages_logs_no_empty_result_warning(caplog):
    """告警通道**产出了告警**时，更不该刷「没有产出指标消息」。"""
    class _Alarm:
        def __init__(self):
            self.message = {'alarmId': 'a-1', 'tags': {'instance_id': 'inst-1'}}
            self.idempotency_key = 'k-1'

    delivery = _delivery()
    with caplog.at_level(logging.WARNING, logger='tpops_kafka_collector.scheduler_delivery'):
        delivery.deliver('alarm', 'inst-1', TaskOutcome(alarms=(_Alarm(),)))
    offenders = [r for r in caplog.records if EMPTY_RESULT_WARNING in r.getMessage()]
    assert not offenders, (
        '告警通道刷了「没有产出任何指标消息」的 WARNING：%s'
        % [r.getMessage() for r in offenders]
    )
