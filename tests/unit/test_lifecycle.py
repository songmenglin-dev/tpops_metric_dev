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

本文件**不碰网络、不碰 Kafka**：平台与投递器都是假的，时钟也是假的。
"""
import os
import threading

from tpops_kafka_collector.lifecycle import run_loop
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
