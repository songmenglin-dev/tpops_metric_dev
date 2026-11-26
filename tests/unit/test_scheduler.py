"""调度器（任务 3.1）的**单元测试** —— 纯逻辑，不碰网络、不碰 Kafka。

守住四件最容易做错的事：

1. **到期驱动** —— 未到期的任务**不执行**；
2. **tick 内并发** —— 三组同时到期时，单 tick 墙钟 ≈ 三组耗时的**最大值**
   而不是之和（组 × 实例**两维**并发）；
3. **单组失败不拖垮其他组** —— 其余组的结果仍被投递，进程不退出；
4. **水位只在投递成功后推进** —— 任一消息失败则整批不推进。

外加：周期档位**表驱动**（从映射表的 ``collect_cycle`` 实算）、
幂等去重、水位键**含 instance_id**、差分基线**带作用域**地恢复、
每轮计数自洽、连续全失败只告警不退出。
"""

import io
import json
import os
import threading
import time

import pytest

from conftest import DATA_DIR

from tpops_kafka_collector.collectors.alarm import AlarmMessage

from tpops_kafka_collector.ids import alarm_idempotency_key
from tpops_kafka_collector.kafka.records import (
    BatchResult,
    FailedRecord,
    ProduceBatchError,
)
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.mapping.rules import CounterDeltaStore
from tpops_kafka_collector.schedule import (
    CHANNEL_ALARM,
    CHANNEL_COMMAND_CENTER,
    CHANNEL_DIAGNOSTIC,
    CHANNEL_INSTANCE,
    CHANNEL_MONITOR,
    ChannelPlan,
    ScheduleEntry,
    compute_channel_plans,
    count_unsupported_metrics,
)
from tpops_kafka_collector.scheduler import Scheduler
from tpops_kafka_collector.scheduler_types import (
    InstanceTarget,
    TaskOutcome,
    alarm_stream,
    metric_stream,
)
from tpops_kafka_collector.watermark import WatermarkStore

# ---------------------------------------------------------------------------
# 夹具与假依赖
# ---------------------------------------------------------------------------

INSTANCE_A = 'inst-a'
INSTANCE_B = 'inst-b'

#: 各通道的周期（秒）—— 单元测试里用小周期，省得等。
PLAN_INTERVALS = (
    (CHANNEL_MONITOR, 30),
    (CHANNEL_COMMAND_CENTER, 60),
    (CHANNEL_DIAGNOSTIC, 300),
    (CHANNEL_INSTANCE, 3600),
    (CHANNEL_ALARM, 30),
)


class FakeClock(object):
    """可控的单调时钟。"""

    def __init__(self, start=1000.0):
        self.now = float(start)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += float(seconds)


class FakeProducer(object):
    """假投递器 —— 记录批次；可以指定「第 N 批投递失败」。"""

    def __init__(self, fail_metric_batches=(), fail_alarm_batches=()):
        self.metric_batches = []
        self.alarm_batches = []
        self._fail_metric = set(fail_metric_batches)
        self._fail_alarm = set(fail_alarm_batches)

    def produce_metrics(self, records):
        index = len(self.metric_batches)
        items = tuple(records)
        self.metric_batches.append(items)
        if index in self._fail_metric:
            raise ProduceBatchError(
                '假投递失败',
                BatchResult(
                    topic='metrics',
                    failed=(FailedRecord(items[0], '假的失败'),),
                ),
            )
        return BatchResult(topic='metrics', succeeded=items)

    def produce_alarms(self, records):
        index = len(self.alarm_batches)
        items = tuple(records)
        self.alarm_batches.append(items)
        if index in self._fail_alarm:
            raise ProduceBatchError(
                '假投递失败',
                BatchResult(
                    topic='alarms',
                    failed=(FailedRecord(items[0], '假的失败'),),
                ),
            )
        return BatchResult(topic='alarms', succeeded=items)

    def metric_payloads(self):
        return [record.payload for batch in self.metric_batches for record in batch]


def metric_message(
    name='metric-1',
    instance_id=INSTANCE_A,
    clock_ms=1000,
    node_id='node-1',
    component_id=None,
    dimensions=None,
):
    """一条合法的 ITSCADA 指标消息（7 字段信封）。"""
    tags = {
        'instance_id': instance_id,
        'instance_name': 'inst',
        'node_id': node_id,
        'component_id': component_id,
        'host_ip': '10.0.0.1',
    }
    if dimensions:
        tags['dimension_values'] = dict(dimensions)
    return {
        'metricName': name,
        'metricCode': name,
        'resourceName': '10.0.0.1_{0}'.format(node_id),
        'value': 1,
        'unit': None,
        'clock': clock_ms,
        'tags': tags,
    }


def alarm_message(alarm_id='alarm-1', instance_id=INSTANCE_A, status='activated',
                  first_alarm_time=1000):
    """一条合法的 ITSCADA 告警消息（带幂等键）。"""
    message = {
        'alarmId': alarm_id,
        'alarmName': '告警名',
        'alarmContent': '内容',
        'priority': 2,
        'alarmTime': first_alarm_time,
        'status': 1,
        'recoverTime': None,
        'resourceName': '10.0.0.1_node-1',
        'metricName': 'metric-1',
        'metricCode': 'metric-1',
        'value': '1',
        'unit': None,
        'tags': {'instance_id': instance_id},
    }
    return AlarmMessage(
        message=message,
        idempotency_key=alarm_idempotency_key(
            alarm_id, instance_id, first_alarm_time, status,
        ),
        alarm_id=alarm_id,
    )


def make_plans(intervals=PLAN_INTERVALS):
    return tuple(
        ChannelPlan(
            channel=channel,
            interval_seconds=interval,
            tier_seconds=interval,
            metric_count=0,
        )
        for channel, interval in intervals
    )


def make_scheduler(executors, instances=None, producer=None, store=None, clock=None,
                   plans=None, delta_store=None, **kwargs):
    """装配一个调度器（测试用 —— 依赖全是假的）。"""
    instances = instances if instances is not None else [InstanceTarget(INSTANCE_A)]
    return Scheduler(
        plans=plans if plans is not None else make_plans(),
        executors=executors,
        producer=producer if producer is not None else FakeProducer(),
        watermark=store if store is not None else WatermarkStore(':memory:'),
        instances=instances,
        delta_store=delta_store,
        clock=clock if clock is not None else FakeClock(),
        **kwargs
    )


def recorder(channel, calls, messages=(), end_time_ms=1000):
    """记录被调用的执行器。"""
    def executor(instance):
        calls.append((channel, instance.instance_id))
        return TaskOutcome(
            messages=tuple(messages),
            end_time_ms=end_time_ms,
        )
    return executor


# ---------------------------------------------------------------------------
# 周期档位：表驱动，不写死
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def mapping():
    return load_mapping(validate=False)


def _raw_metric_rules():
    with io.open(
        os.path.join(DATA_DIR, 'mapping', 'metric_rules.json'), encoding='utf-8'
    ) as handle:
        return json.load(handle)


def test_channel_plans_are_derived_from_the_mapping_table(mapping):
    """周期**从映射表现算** —— 不是写死的常量。"""
    plans = compute_channel_plans(mapping)
    actual = dict((plan.channel, plan.interval_seconds) for plan in plans)

    assert actual == {
        CHANNEL_MONITOR: 30,
        CHANNEL_COMMAND_CENTER: 60,
        CHANNEL_DIAGNOSTIC: 120,
        CHANNEL_INSTANCE: 3600,
        CHANNEL_ALARM: 30,
    }


def test_channel_plans_match_an_independent_recomputation(mapping):
    """实算结果与「按 collect_cycle 独立重算」一致（防止实现与口径漂移）。"""
    units = {'秒': 1, '分': 60, '时': 3600, '天': 86400}
    source_to_channel = {
        'monitor_metrics': CHANNEL_MONITOR,
        'command_center_sql': CHANNEL_COMMAND_CENTER,
        'diagnostic': CHANNEL_DIAGNOSTIC,
        'instance': CHANNEL_INSTANCE,
    }
    expected = {}
    for rule in _raw_metric_rules()['rules']:
        if not rule.get('enabled'):
            continue
        source = rule.get('source') or {}
        channel = source_to_channel.get(source.get('source'))
        if channel is None:
            continue
        cycle = rule.get('collect_cycle') or {}
        seconds = int(cycle.get('value') or 0) * units[cycle.get('unit')]
        if seconds <= 0:
            continue
        expected[channel] = min(expected.get(channel, seconds), seconds)

    plans = compute_channel_plans(mapping)
    for plan in plans:
        if plan.channel == CHANNEL_ALARM:
            continue
        assert plan.interval_seconds == expected[plan.channel], (
            '通道 %s 的周期与独立重算不一致：%d != %d'
            % (plan.channel, plan.interval_seconds, expected[plan.channel])
        )


def test_command_center_sql_tiers_come_from_the_mapping_table(mapping):
    """命令中心各 ``sql_key`` 的刷新档位 = 它覆盖指标里的**最短**档。"""
    plans = compute_channel_plans(mapping)
    plan = dict((item.channel, item) for item in plans)[CHANNEL_COMMAND_CENTER]
    assert plan.tiers == (60, 120, 300, 86400)
    assert plan.metric_count == 71


def test_unsupported_metric_count_is_zero(mapping):
    """不可提供指标已在**构建期**排除 —— 计数上报恒为 0（``dropped_unsupported``）。

    运行期没有「遍历到再丢弃」的分支：被排除的指标码根本不在生效映射表里。
    """
    assert count_unsupported_metrics(mapping) == 0


def test_schedule_entry_is_immutable():
    """到期表的条目是 frozen dataclass（跨模块传递不改）。"""
    entry = ScheduleEntry(task='monitor', interval_seconds=30, next_due_at=1.0)
    with pytest.raises(Exception):
        entry.interval_seconds = 60


# ---------------------------------------------------------------------------
# 硬约束 1：到期驱动
# ---------------------------------------------------------------------------

def test_only_due_channels_are_executed():
    """未到期的通道**不执行**（到期驱动，不是每轮全跑）。"""
    calls = []
    clock = FakeClock()
    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: recorder(CHANNEL_MONITOR, calls),
            CHANNEL_COMMAND_CENTER: recorder(CHANNEL_COMMAND_CENTER, calls),
        },
        clock=clock,
        plans=make_plans(((CHANNEL_MONITOR, 30), (CHANNEL_COMMAND_CENTER, 60))),
    )

    scheduler.tick()
    assert sorted(name for name, _ in calls) == sorted(
        [CHANNEL_MONITOR, CHANNEL_COMMAND_CENTER]
    )

    # 同一时刻再跑一轮：两个通道都还没到期 → 谁都不执行
    scheduler.tick()
    assert len(calls) == 2, '未到期的通道被执行了'

    # 过了 30 秒：只有 monitor 到期
    clock.advance(30)
    scheduler.tick()
    assert [name for name, _ in calls].count(CHANNEL_MONITOR) == 2
    assert [name for name, _ in calls].count(CHANNEL_COMMAND_CENTER) == 1

    # 过了 60 秒（相对起点）：两个都到期
    clock.advance(30)
    scheduler.tick()
    assert [name for name, _ in calls].count(CHANNEL_MONITOR) == 3
    assert [name for name, _ in calls].count(CHANNEL_COMMAND_CENTER) == 2


def test_due_table_reports_the_next_due_time():
    """到期表对外可见（可观测 / 排障）。"""
    clock = FakeClock()
    scheduler = make_scheduler(
        {CHANNEL_MONITOR: recorder(CHANNEL_MONITOR, [])},
        clock=clock,
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    scheduler.tick()
    entries = dict(
        (entry.task, entry) for entry in scheduler.schedule()
    )
    assert entries[CHANNEL_MONITOR].next_due_at == clock.now + 30


# ---------------------------------------------------------------------------
# 硬约束 2：tick 内并发（组 × 实例两维）
# ---------------------------------------------------------------------------

def test_due_groups_run_concurrently_not_serially():
    """三组同时到期 → 墙钟 ≈ **最大**耗时，不是之和。

    用 ``threading.Barrier`` 做**确定性**判据：串行执行时后一个任务永远
    到不了屏障（会超时抛 BrokenBarrierError）→ 测试失败。
    """
    barrier = threading.Barrier(3, timeout=5)

    def executor(instance):
        barrier.wait()
        return TaskOutcome(messages=(), end_time_ms=1000)

    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: executor,
            CHANNEL_COMMAND_CENTER: executor,
            CHANNEL_DIAGNOSTIC: executor,
        },
        plans=make_plans((
            (CHANNEL_MONITOR, 30), (CHANNEL_COMMAND_CENTER, 30), (CHANNEL_DIAGNOSTIC, 30),
        )),
    )
    result = scheduler.tick()

    assert result.tasks_failed == 0, '并发没成立（有任务没跑到屏障）'
    assert result.tasks_total == 3


def test_two_dimensions_are_concurrent_groups_and_instances():
    """**组 × 实例两维**都并发 —— 2 个实例不该把墙钟翻倍。"""
    barrier = threading.Barrier(6, timeout=5)

    def executor(instance):
        barrier.wait()
        return TaskOutcome(messages=(), end_time_ms=1000)

    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: executor,
            CHANNEL_COMMAND_CENTER: executor,
            CHANNEL_DIAGNOSTIC: executor,
        },
        instances=[InstanceTarget(INSTANCE_A), InstanceTarget(INSTANCE_B)],
        plans=make_plans((
            (CHANNEL_MONITOR, 30), (CHANNEL_COMMAND_CENTER, 30), (CHANNEL_DIAGNOSTIC, 30),
        )),
    )
    result = scheduler.tick()

    assert result.tasks_failed == 0
    assert result.tasks_total == 6


def test_tick_wall_clock_is_close_to_the_max_not_the_sum():
    """墙钟证据：三组各 0.5 秒 → 单轮 ≈ 0.5 秒（串行会是 1.5 秒）。"""
    def slow(instance):
        time.sleep(0.5)
        return TaskOutcome(messages=(), end_time_ms=1000)

    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: slow,
            CHANNEL_COMMAND_CENTER: slow,
            CHANNEL_DIAGNOSTIC: slow,
        },
        plans=make_plans((
            (CHANNEL_MONITOR, 30), (CHANNEL_COMMAND_CENTER, 30), (CHANNEL_DIAGNOSTIC, 30),
        )),
    )
    result = scheduler.tick()

    assert result.duration_seconds < 1.0, (
        '单轮耗时 %.2fs —— 像是串行（应为 ≈0.5s 的最大值）' % result.duration_seconds
    )


# ---------------------------------------------------------------------------
# 硬约束 3：单组失败不拖垮其他组
# ---------------------------------------------------------------------------

def test_one_failing_group_does_not_stop_the_others():
    """某组全失败 → 其余组的结果**仍被投递**，进程不退出。"""
    calls = []
    producer = FakeProducer()

    def broken(instance):
        calls.append((CHANNEL_COMMAND_CENTER, instance.instance_id))
        raise RuntimeError('这组炸了')

    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: recorder(
                CHANNEL_MONITOR, calls,
                messages=(metric_message('metric-1'),),
            ),
            CHANNEL_COMMAND_CENTER: broken,
        },
        producer=producer,
        plans=make_plans(((CHANNEL_MONITOR, 30), (CHANNEL_COMMAND_CENTER, 30))),
    )

    result = scheduler.tick()

    assert result.tasks_failed == 1
    assert [f.channel for f in result.failures] == [CHANNEL_COMMAND_CENTER]
    assert producer.metric_batches, '失败的那组把别的组的投递也拖掉了'
    assert len(producer.metric_batches[0]) == 1


def test_a_failing_executor_does_not_kill_the_process():
    """执行器抛异常 → 记进失败清单，**不往上抛**。"""
    def broken(instance):
        raise RuntimeError('炸了')

    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: broken,
            CHANNEL_COMMAND_CENTER: recorder(CHANNEL_COMMAND_CENTER, []),
        },
        plans=make_plans(((CHANNEL_MONITOR, 30), (CHANNEL_COMMAND_CENTER, 30))),
    )
    result = scheduler.tick()   # 不抛异常

    assert result.tasks_failed == 1
    assert result.failures[0].error_type == 'RuntimeError'


def test_consecutive_all_failed_rounds_only_log_an_error(caplog):
    """连续 N 轮全失败 → ERROR 日警，**进程不退出**（§4.7 进程自愈）。"""
    def broken(instance):
        raise RuntimeError('炸了')

    clock = FakeClock()
    scheduler = make_scheduler(
        {CHANNEL_MONITOR: broken},
        clock=clock,
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    with caplog.at_level('ERROR'):
        for _ in range(3):
            scheduler.tick()
            clock.advance(30)

    assert '连续 3 轮采集全失败' in caplog.text


# ---------------------------------------------------------------------------
# 硬约束 4：水位只在投递成功后推进
# ---------------------------------------------------------------------------

def test_watermark_advances_only_after_successful_delivery(tmp_path):
    """任一消息投递失败 → 整批不推进水位；下一轮成功才推进。"""
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    producer = FakeProducer(fail_metric_batches={0})
    clock = FakeClock()
    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: recorder(
                CHANNEL_MONITOR, [],
                messages=(metric_message('metric-1'),),
                end_time_ms=1234,
            ),
        },
        producer=producer,
        store=store,
        clock=clock,
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )

    result = scheduler.tick()
    assert result.failed == 1
    assert store.metric_watermark(metric_stream(INSTANCE_A, CHANNEL_MONITOR)) is None, (
        '投递失败却推进了水位'
    )

    # 下一轮：投递成功 → 推进水位
    clock.advance(30)
    result = scheduler.tick()
    assert result.produced == 1
    assert store.metric_watermark(
        metric_stream(INSTANCE_A, CHANNEL_MONITOR)
    ) == 1234


def test_failed_batch_is_retried_in_the_next_round(tmp_path):
    """投递失败的那批**下轮重发**（幂等键没被登记 → 不会被当成重复跳过）。"""
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    producer = FakeProducer(fail_metric_batches={0})
    clock = FakeClock()
    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: recorder(
                CHANNEL_MONITOR, [],
                messages=(metric_message('metric-1'),),
                end_time_ms=1234,
            ),
        },
        producer=producer,
        store=store,
        clock=clock,
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )

    scheduler.tick()
    clock.advance(30)
    scheduler.tick()

    assert len(producer.metric_batches) == 2, '失败的那批没有重发'
    assert len(producer.metric_batches[1]) == 1


def test_metric_watermark_is_not_advanced_without_end_time(tmp_path):
    """执行器没报告结束时刻 → **不推进**水位（宁可重投也不丢数据）。"""
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: recorder(
                CHANNEL_MONITOR, [], messages=(metric_message('metric-1'),),
                end_time_ms=None,
            ),
        },
        store=store,
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    scheduler.tick()
    assert store.metric_watermark(metric_stream(INSTANCE_A, CHANNEL_MONITOR)) is None


# ---------------------------------------------------------------------------
# 幂等去重
# ---------------------------------------------------------------------------

def test_already_delivered_messages_are_skipped(tmp_path):
    """已投递过的幂等键 → 跳过，不重复投递。"""
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    clock = FakeClock()
    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: recorder(
                CHANNEL_MONITOR, [],
                messages=(metric_message('metric-1'),),
                end_time_ms=1234,
            ),
        },
        store=store,
        clock=clock,
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    scheduler.tick()
    clock.advance(30)
    result = scheduler.tick()

    assert result.produced == 0, '重复投递了'
    assert result.mapped == 0


def test_duplicate_keys_inside_one_batch_are_delivered_once():
    """同一批里两条消息幂等键相同 → 只投一条。"""
    producer = FakeProducer()
    message = metric_message('metric-1')
    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: recorder(
                CHANNEL_MONITOR, [], messages=(message, dict(message)),
                end_time_ms=1234,
            ),
        },
        producer=producer,
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    scheduler.tick()
    assert len(producer.metric_batches[0]) == 1


def test_metrics_of_different_components_have_different_keys():
    """A 类多组件 → 幂等键必须不同（否则后一个组件被当重复丢掉 → 丢数据）。"""
    from tpops_kafka_collector.scheduler_types import metric_key

    first = metric_message('metric-1', component_id='cn_5001')
    second = metric_message('metric-1', component_id='cn_5002')
    assert metric_key(INSTANCE_A, first) != metric_key(INSTANCE_A, second)


# ---------------------------------------------------------------------------
# 水位键的约定：必须含 instance_id
# ---------------------------------------------------------------------------

def test_watermark_stream_keys_contain_the_instance_id():
    """多实例下水位不能互相覆盖 → 键里必须有 instance_id。"""
    assert metric_stream(INSTANCE_A, CHANNEL_MONITOR) == 'inst-a:monitor'
    assert alarm_stream(INSTANCE_A) == 'inst-a:alarm'
    assert metric_stream(INSTANCE_A, CHANNEL_MONITOR) != metric_stream(
        INSTANCE_B, CHANNEL_MONITOR
    )


def test_each_instance_gets_its_own_watermark(tmp_path):
    """两个实例的水位各推各的。"""
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: recorder(
                CHANNEL_MONITOR, [],
                messages=(metric_message('metric-1'),),
                end_time_ms=1234,
            ),
        },
        instances=[InstanceTarget(INSTANCE_A), InstanceTarget(INSTANCE_B)],
        store=store,
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    scheduler.tick()

    assert store.metric_watermark(metric_stream(INSTANCE_A, CHANNEL_MONITOR)) == 1234
    assert store.metric_watermark(metric_stream(INSTANCE_B, CHANNEL_MONITOR)) == 1234


def test_alarm_watermark_advances_after_delivery(tmp_path):
    """告警水位推进（键含 instance_id）。"""
    store = WatermarkStore(str(tmp_path / 'wm.json'))
    scheduler = make_scheduler(
        {
            CHANNEL_ALARM: lambda instance: TaskOutcome(
                alarms=(alarm_message(),),
            ),
        },
        store=store,
        plans=make_plans(((CHANNEL_ALARM, 30),)),
    )
    scheduler.tick()
    assert store.alarm_watermark(alarm_stream(INSTANCE_A)) == '1000'


# ---------------------------------------------------------------------------
# 差分基线：带作用域地恢复
# ---------------------------------------------------------------------------

def test_delta_baselines_are_restored_with_their_scope(tmp_path):
    """重启后基线要**带作用域**回来（否则速率永远算不出来，且不报错）。"""
    path = str(tmp_path / 'wm.json')
    store = WatermarkStore(path)
    deltas = CounterDeltaStore()
    deltas.rate('metric-1', {'t': 'x'}, 100.0, 1000, scope='node-1')
    store.import_baselines_from(deltas)
    store.save()

    # 重启：新的水位存储 + 新的基线存储
    store2 = WatermarkStore(path)
    store2.load()
    deltas2 = CounterDeltaStore()
    scheduler = make_scheduler(
        {CHANNEL_MONITOR: recorder(CHANNEL_MONITOR, [])},
        store=store2,
        delta_store=deltas2,
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    scheduler.tick()

    baselines = store2.snapshot().delta_baselines
    assert baselines, '基线没恢复（重启后速率永远算不出来）'
    assert baselines[0].dimension_key.startswith('node-1:'), (
        '基线的作用域丢了：%r' % (baselines[0].dimension_key,)
    )


def test_put_baseline_style_keys_are_not_used(tmp_path):
    """``put_baseline`` 写的键没有作用域 —— 调度器不许用它恢复基线。"""
    path = str(tmp_path / 'wm.json')
    store = WatermarkStore(path)
    deltas = CounterDeltaStore()
    deltas.rate('metric-1', {'t': 'x'}, 100.0, 1000, scope='node-1')
    store.import_baselines_from(deltas)
    store.save()

    store2 = WatermarkStore(path)
    store2.load()
    deltas2 = CounterDeltaStore()
    scheduler = make_scheduler(
        {CHANNEL_MONITOR: recorder(CHANNEL_MONITOR, [])},
        store=store2,
        delta_store=deltas2,
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    scheduler.tick()

    dimension_keys = [item.dimension_key for item in store2.snapshot().delta_baselines]
    assert dimension_keys and all(':' in key for key in dimension_keys)


# ---------------------------------------------------------------------------
# 每轮计数自洽 + 可观测
# ---------------------------------------------------------------------------

def test_round_counters_are_self_consistent():
    """投递数 + 失败数 ≤ 转换数（规格：各计数之和自洽）。"""
    producer = FakeProducer(fail_metric_batches={1})
    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: recorder(
                CHANNEL_MONITOR, [],
                messages=(metric_message('metric-1'),),
                end_time_ms=1234,
            ),
            CHANNEL_COMMAND_CENTER: recorder(
                CHANNEL_COMMAND_CENTER, [],
                messages=(metric_message('metric-2'),),
                end_time_ms=1234,
            ),
        },
        producer=producer,
        plans=make_plans(((CHANNEL_MONITOR, 30), (CHANNEL_COMMAND_CENTER, 30))),
    )
    result = scheduler.tick()

    assert result.produced + result.failed <= result.mapped
    assert result.produced + result.failed == result.mapped
    assert result.mapped == 2


def test_summary_log_uses_the_agreed_field_names(caplog):
    """每轮汇总日志的字段名照开发方案 §4.9.1 —— 不自创。"""
    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: recorder(
                CHANNEL_MONITOR, [], messages=(metric_message('metric-1'),),
                end_time_ms=1234,
            ),
        },
        dropped_unsupported=3,
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    with caplog.at_level('INFO'):
        scheduler.tick()

    for field in ('tick=', 'elapsed=', 'fetched=', 'mapped=', 'produced=',
                  'failed=', 'dropped_unsupported=', 'windows=', 'alarms=',
                  'calls=', 'delivery_rate='):
        assert field in caplog.text, '汇总日志少了字段 %s' % field
    # 措辞归 ``scheduler_report``（R-B 的文件），这里只断言**字段的值**。
    assert 'dropped_unsupported=3' in caplog.text


def test_external_call_budget_overrun_is_logged(caplog):
    """单轮外部调用数超预算 → WARNING。"""
    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: lambda instance: TaskOutcome(
                messages=(), external_calls=50,
            ),
        },
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    with caplog.at_level('WARNING'):
        scheduler.tick()
    assert '超出预算' in caplog.text


# ---------------------------------------------------------------------------
# 装配校验
# ---------------------------------------------------------------------------

def test_missing_executor_is_a_configuration_error():
    """通道没有执行器 → 装配期就失败（快速失败）。"""
    from tpops_kafka_collector.scheduler_types import SchedulerError

    with pytest.raises(SchedulerError):
        make_scheduler(
            {CHANNEL_MONITOR: recorder(CHANNEL_MONITOR, [])},
            plans=make_plans(((CHANNEL_MONITOR, 30), (CHANNEL_COMMAND_CENTER, 30))),
        )


def test_no_instances_is_a_configuration_error():
    from tpops_kafka_collector.scheduler_types import SchedulerError

    with pytest.raises(SchedulerError):
        make_scheduler(
            {CHANNEL_MONITOR: recorder(CHANNEL_MONITOR, [])},
            instances=[],
            plans=make_plans(((CHANNEL_MONITOR, 30),)),
        )


def test_executor_returning_garbage_is_a_failure_not_a_crash():
    """执行器返回的不是 TaskOutcome → 记成失败，不崩。"""
    scheduler = make_scheduler(
        {CHANNEL_MONITOR: lambda instance: '不是 TaskOutcome'},
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    result = scheduler.tick()
    assert result.tasks_failed == 1


# ---------------------------------------------------------------------------
# 外部调用数：失败任务也要计数
# ---------------------------------------------------------------------------

class _CountingCalls(object):
    """最小的全局调用计数器（与 ``CallCounter`` 同形状）。"""

    def __init__(self):
        self._total = 0

    def record(self):
        self._total += 1

    def total(self):
        return self._total


def test_failed_tasks_calls_are_counted_in_the_round_summary():
    """**失败任务**的外部调用数也要出现在轮汇总里（失败也要计数）。

    失败任务的请求**确实发出去了** —— 平台调用与墙钟时间都已经消耗。
    只统计成功任务的话，轮汇总的调用数会偏小 → 42 次预算告警**漏报**。
    """
    counter = _CountingCalls()

    def failing(instance):
        counter.record()
        counter.record()
        raise RuntimeError('这组炸了')

    def ok(instance):
        counter.record()
        return TaskOutcome(messages=(), external_calls=1)

    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: ok,
            CHANNEL_COMMAND_CENTER: failing,
        },
        call_counter=counter,
        plans=make_plans(((CHANNEL_MONITOR, 30), (CHANNEL_COMMAND_CENTER, 30))),
    )

    result = scheduler.tick()

    assert result.tasks_failed == 1
    assert result.external_calls == 3, (
        '失败任务的 2 次调用没进轮汇总 —— 汇总偏小 → 预算告警漏报：%r'
        % (result.external_calls,)
    )


def test_round_call_count_is_not_smaller_than_the_reported_one():
    """没有计数器时，轮汇总至少等于各组自报的调用数之和。"""
    scheduler = make_scheduler(
        {
            CHANNEL_MONITOR: lambda instance: TaskOutcome(
                messages=(), external_calls=7,
            ),
        },
        plans=make_plans(((CHANNEL_MONITOR, 30),)),
    )
    result = scheduler.tick()
    assert result.external_calls == 7, result.external_calls
