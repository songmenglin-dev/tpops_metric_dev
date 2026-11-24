# -*- coding: utf-8 -*-
"""主循环（``lifecycle``）的单元测试 —— 节奏对齐 / 每轮落盘 / 保命分支。

守住三件最容易做错、且**坏了不会立刻报错**的事：

1. **节奏对齐到 tick 边界** —— 一轮跑完只睡「剩余时间」，
   实际采集周期 **= 档位**，不是「采集耗时 + 间隔」。
   客户指标库里 82/98 条走命令中心通道，节奏慢 29% 就是**静默欠服务**
   （日志一切正常、外部调用数正常，没有任何信号提示节奏偏了）。
2. **每轮结束就落盘** —— 崩溃（SIGKILL / OOM / 宿主机重启 / 容器驱逐）
   最多丢一轮，而不是丢掉**整个进程生命周期**的去重状态。
3. **一轮采集抛异常不带走进程** —— 主循环的最后一道防线，下轮重试。
4. **一轮全挂不算成功** —— ``/readyz`` 的 scheduler 探针**只**认
   ``TrackedScheduler`` 记下的「最近一次成功完成」时刻。这道闸**只有一处实现**
   （``lifecycle.TrackedScheduler.tick``），拆掉它 → 每轮都刷新时刻 →
   就绪探针恒报 200 → 采集端一条数据都投不出去时运维仍看到「就绪」。

本文件**不碰网络、不碰 Kafka**：平台与投递器都是假的，时钟也是假的。
"""
import os
import threading

import pytest

from tpops_kafka_collector.lifecycle import TrackedScheduler, run_loop
from tpops_kafka_collector.schedule import ChannelPlan
from tpops_kafka_collector.scheduler import Scheduler
from tpops_kafka_collector.scheduler_types import InstanceTarget, TaskOutcome
from tpops_kafka_collector.watermark import WatermarkStore

#: 被观察的通道名。
CHANNEL = 'monitor'
#: 该通道的档位（秒）—— 与客户指标库里的「30 秒档」一致。
TIER_SECONDS = 30
#: 模拟一轮采集的耗时（秒）—— 真平台上命令中心一轮要好几秒。
COLLECT_SECONDS = 8.0
#: 采集轮数（够算间隔就行）。
ROUNDS = 4


# ---------------------------------------------------------------------------
# 假依赖
# ---------------------------------------------------------------------------

class FakeClock(object):
    """可控的单调时钟（不真的走时间）。"""

    def __init__(self, start=1000.0):
        self.now = float(start)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += float(seconds)


class FakeConfig(object):
    """只带轮询间隔的假配置。"""

    def __init__(self, poll_interval_seconds):
        self.poll_interval_seconds = poll_interval_seconds


class FakeRuntime(object):
    """只带调度器 / 配置 / 水位存储的假运行时。"""

    def __init__(self, scheduler, config, watermark=None):
        self.scheduler = scheduler
        self.config = config
        self.watermark = watermark


class NullProducer(object):
    """不投任何东西的假投递器（本文件的消息都是空的）。"""

    def produce_metrics(self, records):
        return None

    def produce_alarms(self, records):
        return None

    def close(self):
        pass


def _store(tmp_path):
    """真的水位存储（落在磁盘上 —— 本文件要断言「磁盘上有没有文件」）。"""
    return WatermarkStore(str(tmp_path / 'watermark.json'))


def _channel_plan(interval_seconds=TIER_SECONDS):
    return (
        ChannelPlan(
            channel=CHANNEL,
            interval_seconds=interval_seconds,
            tier_seconds=interval_seconds,
            metric_count=1,
        ),
    )


# ---------------------------------------------------------------------------
# 硬约束 1：节奏对齐到 tick 边界
# ---------------------------------------------------------------------------

def test_channel_interval_is_aligned_to_the_tick_boundary(tmp_path):
    """相邻两次实际采集间隔 **≤ 档位 × 1.1**（对齐到 tick 边界）。

    用**真的调度器 + 真的到期表**（只有平台是假的）——
    到期表的「到期才跑」语义正是把节奏拉长的那一环。

    采集耗时 8 秒、档位 30 秒：睡满整间隔的话，实际间隔是 38 秒
    （= 采集耗时 + 间隔），比客户声明的档位长 27%。
    """
    clock = FakeClock()
    stop = threading.Event()
    calls = []

    def executor(instance):
        calls.append(clock.now)
        clock.advance(COLLECT_SECONDS)
        if len(calls) >= ROUNDS:
            stop.set()
        return TaskOutcome(messages=(), end_time_ms=1000)

    scheduler = Scheduler(
        plans=_channel_plan(),
        executors={CHANNEL: executor},
        producer=NullProducer(),
        watermark=_store(tmp_path),
        instances=[InstanceTarget('inst-a')],
        clock=clock,
    )
    runtime = FakeRuntime(scheduler, FakeConfig(TIER_SECONDS), _store(tmp_path))

    def wait(timeout):
        clock.advance(timeout)
        return stop.is_set()

    run_loop(runtime, stop, monotonic=clock, wait=wait)

    assert len(calls) >= 2, '采集轮数不够，算不出间隔：%r' % (calls,)
    intervals = [after - before for before, after in zip(calls, calls[1:])]
    for interval in intervals:
        assert interval <= TIER_SECONDS * 1.1, (
            '通道 %s 的实际采集间隔 %.1f 秒超过档位 %d 秒 —— 节奏没对齐到 tick 边界：%r'
            % (CHANNEL, interval, TIER_SECONDS, intervals)
        )


def test_a_slow_round_does_not_accumulate_drift(tmp_path):
    """一轮超时时**不堆积、不追赶**：下一轮从当前时刻重新锚定。

    连续三轮都超时（耗时 40 秒 > 档位 30 秒）→ 之后的间隔回到档位，
    而不是越欠越多（补偿风暴）。
    """
    clock = FakeClock()
    stop = threading.Event()
    calls = []

    def executor(instance):
        calls.append(clock.now)
        clock.advance(TIER_SECONDS * 2)   # 每轮都超时
        if len(calls) >= ROUNDS:
            stop.set()
        return TaskOutcome(messages=(), end_time_ms=1000)

    scheduler = Scheduler(
        plans=_channel_plan(),
        executors={CHANNEL: executor},
        producer=NullProducer(),
        watermark=_store(tmp_path),
        instances=[InstanceTarget('inst-a')],
        clock=clock,
    )
    runtime = FakeRuntime(scheduler, FakeConfig(TIER_SECONDS), _store(tmp_path))

    def wait(timeout):
        clock.advance(timeout)
        return stop.is_set()

    run_loop(runtime, stop, monotonic=clock, wait=wait)

    assert len(calls) >= 2, calls
    for before, after in zip(calls, calls[1:]):
        # 超时的那一轮不睡 → 间隔 = 采集耗时；不追赶到过去的边界。
        assert after - before <= TIER_SECONDS * 2 + 0.001, (
            '超时后还在追赶过去的边界 —— 会越欠越多：%r' % (calls,)
        )


# ---------------------------------------------------------------------------
# 硬约束 2：每轮结束就落盘
# ---------------------------------------------------------------------------

def test_watermark_is_written_to_disk_after_every_round(tmp_path):
    """一轮 tick 结束后，磁盘上**已经有**水位文件。

    只在退出时落盘的话，SIGKILL / OOM / 宿主机重启之后，
    整个进程生命周期的幂等键与水位全丢 → 那段数据被整段重投。
    """
    path = str(tmp_path / 'watermark.json')
    store = WatermarkStore(path)
    store.advance_metric('inst-a:monitor', 12345)
    stop = threading.Event()

    class _OneShotScheduler(object):
        def __init__(self):
            self.ticks = 0

        def tick(self):
            self.ticks += 1
            stop.set()

    scheduler = _OneShotScheduler()
    runtime = FakeRuntime(scheduler, FakeConfig(TIER_SECONDS), store)

    run_loop(runtime, stop, monotonic=FakeClock(), wait=lambda timeout: stop.wait(0))

    assert scheduler.ticks == 1, scheduler.ticks
    assert os.path.exists(path), (
        '一轮结束后磁盘上没有水位文件 —— 崩溃会丢掉整个进程生命周期的去重状态'
    )
    reloaded = WatermarkStore(path)
    reloaded.load()
    assert reloaded.metric_watermark('inst-a:monitor') == 12345


def test_every_round_rewrites_the_watermark_file(tmp_path):
    """**每一轮**都落盘（不是只写一次）。

    连跑三轮 → 水位存储被 ``save()`` 了 3 次。
    """
    class _CountingStore(WatermarkStore):
        """数一数落盘次数的水位存储。"""

        def __init__(self, path):
            WatermarkStore.__init__(self, path)
            self.saves = 0

        def save(self):
            self.saves += 1
            WatermarkStore.save(self)

    path = str(tmp_path / 'watermark.json')
    store = _CountingStore(path)
    stop = threading.Event()

    class _CountingScheduler(object):
        def __init__(self):
            self.ticks = 0

        def tick(self):
            self.ticks += 1
            if self.ticks >= 3:
                stop.set()

    runtime = FakeRuntime(_CountingScheduler(), FakeConfig(TIER_SECONDS), store)
    run_loop(runtime, stop, monotonic=FakeClock(), wait=lambda timeout: stop.wait(0))

    assert store.saves == 3, '每轮都该落盘一次，实际 %d 次' % store.saves
    assert os.path.exists(path)


# ---------------------------------------------------------------------------
# 硬约束 3：一轮采集抛异常不带走进程
# ---------------------------------------------------------------------------

def test_a_failing_round_does_not_stop_the_loop(caplog):
    """一轮采集抛异常 → 进程继续跑，下轮重试（主循环的最后一道防线）。"""
    stop = threading.Event()
    ticks = []

    class _FlakyScheduler(object):
        def __init__(self):
            self.count = 0

        def tick(self):
            self.count += 1
            ticks.append(self.count)
            if self.count >= 3:
                stop.set()
            if self.count == 1:
                raise RuntimeError('这轮炸了')

    runtime = FakeRuntime(_FlakyScheduler(), FakeConfig(TIER_SECONDS))
    with caplog.at_level('ERROR'):
        run_loop(
            runtime, stop, monotonic=FakeClock(),
            wait=lambda timeout: stop.wait(0),
        )

    assert ticks == [1, 2, 3], '一轮失败就把主循环带走了：%r' % (ticks,)
    assert '本轮调度异常' in caplog.text, caplog.text


# ---------------------------------------------------------------------------
# 硬约束 4：一轮全挂**不算成功**（就绪探针的唯一闸门）
# ---------------------------------------------------------------------------

def _tracked_scheduler(tmp_path, executor, wall_clock):
    """一个**真的** :class:`TrackedScheduler`（只把平台与时钟换成假的）。"""
    clock = FakeClock()
    scheduler = TrackedScheduler(
        plans=_channel_plan(),
        executors={CHANNEL: executor},
        producer=NullProducer(),
        watermark=_store(tmp_path),
        instances=[InstanceTarget('inst-a')],
        clock=clock,
        wall_clock=wall_clock,
    )
    return scheduler, clock


def _failing_executor(instance):
    """采集失败的执行器 —— 一轮下来 ``tasks_failed > 0``。"""
    raise RuntimeError('平台不可达')


def _succeeding_executor(instance):
    """采集成功的执行器 —— 一轮下来 ``tasks_failed == 0``。"""
    return TaskOutcome(messages=(), end_time_ms=1000)


def test_a_fully_failed_tick_does_not_count_as_a_success(tmp_path):
    """一轮**全挂** → ``last_tick_ok_at`` **保持** ``None``。

    这道闸是「就绪」的唯一依据（``main._scheduler_state`` 只读这个值）：
    拆掉它 → 每轮都刷新 → 就绪探针恒报 200 → 采集端一条数据都投不出去时
    运维仍然看到「就绪」，监控不告警、值班不介入。
    """
    scheduler, _clock = _tracked_scheduler(
        tmp_path, _failing_executor, lambda: 1700000000.0,
    )

    result = scheduler.tick()

    assert result.tasks_failed > 0, f'这轮本该失败：{result!r}'
    assert scheduler.last_tick_ok_at is None, (
        '一轮全挂却记了「最近一次成功完成」—— 就绪探针会报 200'
    )


def test_a_successful_tick_records_the_wall_clock(tmp_path):
    """一轮**全成** → ``last_tick_ok_at`` **被更新**（值 ≈ 墙钟）。"""
    wall = [1700000000.0]
    scheduler, _clock = _tracked_scheduler(
        tmp_path, _succeeding_executor, lambda: wall[0],
    )

    result = scheduler.tick()

    assert result.tasks_failed == 0, result
    assert scheduler.last_tick_ok_at == pytest.approx(wall[0]), (
        f'成功完成一轮却没有记录时刻：{scheduler.last_tick_ok_at!r}'
    )


def test_a_failed_round_after_a_success_does_not_refresh_the_timestamp(tmp_path):
    """成功过、之后又全挂 → 时间戳**不刷新**（旧值留着，由就绪探针按「过旧」判不就绪）。

    这条把「闸门是**逐轮**判定」钉死：实现若改成「上一轮成功就一直算成功」，
    或者改成「无论成败都刷新」，这条都会红。
    """
    wall = [1700000000.0]
    failing = {'now': False}

    def executor(instance):
        if failing['now']:
            raise RuntimeError('平台不可达')
        return TaskOutcome(messages=(), end_time_ms=1000)

    scheduler, clock = _tracked_scheduler(tmp_path, executor, lambda: wall[0])

    scheduler.tick()
    first = scheduler.last_tick_ok_at
    assert first == pytest.approx(wall[0]), first

    failing['now'] = True
    wall[0] += 600.0
    clock.advance(TIER_SECONDS)
    scheduler.tick()

    assert scheduler.last_tick_ok_at == first, (
        '全挂的一轮刷新了「最近一次成功完成」时刻 —— 挂掉的采集会被当成成功'
    )
