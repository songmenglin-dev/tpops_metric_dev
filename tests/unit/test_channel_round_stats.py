"""**每通道每轮**统计与汇总行的守卫（wave-2 任务 2.5 / 2.6）。

守的是：

1. 通道级计数**与整体行同口径** —— ``mapped`` / ``produced`` / ``failed``
   来自投递统计，``fetched`` / ``windows`` / ``calls`` 来自采集结果；
2. **各通道之和 == 整体行的同名字段**；
3. **没跑完的通道不出汇总行**（未到期 / 一个实例都没跑成）；
4. 汇总行是**纯 ``k=v`` token**（值里不含空格）—— JSON 模式才能摊平成顶层字段；
5. 同一行打两次：一次绑 ``channel=…``（进通道日志），
   一次绑 ``collector.summary``（进总日志）。
"""
import pytest
from loguru import logger

from tpops_kafka_collector.channel_stats import TaskTimer, aggregate_channel_stats
from tpops_kafka_collector.scheduler_report import log_channel_summaries
from tpops_kafka_collector.scheduler_types import (
    ChannelRoundStats,
    DeliveryStats,
    TaskOutcome,
)

CHANNELS = ('command_center', 'monitor')


@pytest.fixture
def captured():
    """收集 loguru 的记录（含 extra 里的 channel 绑定）。"""
    records = []

    def _sink(message):
        records.append(message.record)

    handler_id = logger.add(_sink, format='{message}', level=0, catch=False)
    yield records
    logger.remove(handler_id)


def _outcome(fetched=0, windows=0, calls=1):
    return TaskOutcome(
        messages=(), alarms=(), end_time_ms=1,
        external_calls=calls, fetched=fetched, windows=windows,
    )


def _stats():
    outcomes = {
        ('monitor', 'inst-1'): _outcome(fetched=100, windows=2, calls=3),
        ('command_center', 'inst-1'): _outcome(fetched=520, windows=11, calls=4),
        ('command_center', 'inst-2'): _outcome(fetched=8, windows=1, calls=2),
    }
    deliveries = {
        'monitor': DeliveryStats(planned=1041, delivered=1041, failed=0, alarms=0),
        'command_center': DeliveryStats(planned=498, delivered=496, failed=2, alarms=3),
    }
    spans = {
        ('monitor', 'inst-1'): (10.0, 12.5),
        ('command_center', 'inst-1'): (10.0, 13.2),
        ('command_center', 'inst-2'): (11.0, 12.0),
    }
    counts = {'monitor': 1, 'command_center': 2}
    return aggregate_channel_stats(12, outcomes, deliveries, spans, counts)


# ---------------------------------------------------------------------------
# 聚合
# ---------------------------------------------------------------------------

def test_one_row_per_completed_channel():
    rows = _stats()
    assert [row.channel for row in rows] == list(CHANNELS)


def _by_channel():
    return {row.channel: row for row in _stats()}


def test_counts_come_from_delivery_and_outcome_stats():
    rows = _by_channel()
    monitor, command_center = rows['monitor'], rows['command_center']
    assert monitor.mapped == 1041
    assert monitor.produced == 1041
    assert monitor.failed == 0
    assert monitor.fetched == 100
    assert monitor.windows == 2
    assert monitor.calls == 3
    assert command_center.mapped == 498
    assert command_center.produced == 496
    assert command_center.failed == 2
    assert command_center.fetched == 528
    assert command_center.windows == 12
    assert command_center.calls == 6


def test_instances_come_from_the_channel_instance_count():
    rows = _by_channel()
    monitor, command_center = rows['monitor'], rows['command_center']
    assert monitor.instances == 1
    assert command_center.instances == 2


def test_elapsed_is_the_channel_wall_clock():
    rows = _by_channel()
    monitor, command_center = rows['monitor'], rows['command_center']
    assert monitor.elapsed == pytest.approx(2.5)
    # 并发跑：最早开始 → 最晚结束，而不是相加
    assert command_center.elapsed == pytest.approx(3.2)


def test_delivery_rate_is_produced_over_mapped():
    rows = _by_channel()
    monitor, command_center = rows['monitor'], rows['command_center']
    assert monitor.delivery_rate == pytest.approx(1.0)
    assert command_center.delivery_rate == pytest.approx(496 / 498)


def test_delivery_rate_is_one_when_nothing_was_mapped():
    rows = aggregate_channel_stats(
        1, {('monitor', 'inst-1'): _outcome()},
        {'monitor': DeliveryStats()}, {}, {'monitor': 1},
    )
    assert rows[0].delivery_rate == 1.0
    assert rows[0].mapped == 0


def test_channels_without_any_result_get_no_row():
    """没跑成的通道不出行（它的失败由整体行体现）。"""
    rows = aggregate_channel_stats(
        1, {('monitor', 'inst-1'): _outcome(fetched=1)},
        {'monitor': DeliveryStats(planned=1, delivered=1)},
        {},
        {'monitor': 1, 'diagnostic': 1},
    )
    assert [row.channel for row in rows] == ['monitor']


def test_channel_sums_match_the_overall_row():
    """各通道之和 == 整体行的同名字段。"""
    rows = _stats()
    assert sum(row.fetched for row in rows) == 100 + 528
    assert sum(row.mapped for row in rows) == 1041 + 498
    assert sum(row.produced for row in rows) == 1041 + 496
    assert sum(row.failed for row in rows) == 0 + 2
    assert sum(row.windows for row in rows) == 2 + 12
    assert sum(row.calls for row in rows) == 3 + 6


def test_task_timer_records_the_span_of_each_task():
    ticks = iter([1.0, 4.0])
    timer = TaskTimer(lambda: next(ticks))
    wrapped = timer.wrap('monitor', lambda instance: _outcome())

    class _Instance:
        instance_id = 'inst-1'

    wrapped(_Instance())
    spans = timer.spans()
    assert spans[('monitor', 'inst-1')] == (1.0, 4.0)


# ---------------------------------------------------------------------------
# 汇总行
# ---------------------------------------------------------------------------

def test_each_channel_emits_a_channel_bound_row_and_a_summary_row(captured):
    log_channel_summaries(_stats())

    channel_rows = [r for r in captured if r['extra'].get('channel')]
    summary_rows = [r for r in captured
                    if r['extra'].get('logger_name') == 'collector.summary']
    assert len(channel_rows) == 2
    assert len(summary_rows) == 2
    assert sorted(r['extra']['channel'] for r in channel_rows) == list(CHANNELS)


def test_summary_row_carries_the_eleven_fixed_fields(captured):
    log_channel_summaries(_stats())

    row = [r for r in captured
           if r['extra'].get('logger_name') == 'collector.summary'
           and 'channel=monitor' in r['message']][0]
    message = row['message']
    fields = message.split()
    assert len(fields) == 11
    keys = [item.split('=')[0] for item in fields]
    assert keys == ['channel', 'tick', 'instances', 'fetched', 'mapped', 'produced',
                    'failed', 'windows', 'calls', 'elapsed', 'delivery_rate']
    assert 'channel=monitor' in fields


def test_summary_row_is_pure_key_value_pairs(captured):
    """纯 k=v token（值里不含空格）—— JSON 模式才能摊平成顶层字段。"""
    log_channel_summaries(_stats())

    row = [r for r in captured
           if r['extra'].get('logger_name') == 'collector.summary'][0]
    for token in row['message'].split():
        assert '=' in token
        assert ' ' not in token


def test_summary_row_values_are_the_aggregated_ones(captured):
    log_channel_summaries(_stats())

    rows = {r['message'].split()[0]: r['message']
            for r in captured if r['extra'].get('logger_name') == 'collector.summary'}
    assert rows['channel=monitor'] == (
        'channel=monitor tick=12 instances=1 fetched=100 mapped=1041 produced=1041 '
        'failed=0 windows=2 calls=3 elapsed=2.5s delivery_rate=1.000'
    )


def test_channel_rows_and_summary_rows_carry_the_same_message(captured):
    log_channel_summaries(_stats())

    channel_messages = sorted(r['message'] for r in captured
                              if r['extra'].get('channel'))
    summary_messages = sorted(r['message'] for r in captured
                              if r['extra'].get('logger_name') == 'collector.summary')
    assert channel_messages == summary_messages


def test_channel_round_stats_is_immutable():
    row = ChannelRoundStats(channel='monitor', tick=1)
    with pytest.raises(Exception):
        row.channel = 'other'
