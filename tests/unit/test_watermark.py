# -*- coding: utf-8 -*-
"""水位与差分基线持久化的单元测试。

覆盖 specs/delivery-and-state/spec.md 的「水位与差分基线的持久化」与
「幂等去重」两条需求：

- 原子写：写文件途中失败，磁盘上仍是上一轮的完整内容；
- 重启续传：水位 / 差分基线 / 告警水位 / 幂等键四段都能恢复；
- 幂等键 LRU：容量可配、按插入顺序淘汰、重复键不重复入队。
"""
import json
import os

import pytest

from tpops_kafka_collector import watermark as watermark_module
from tpops_kafka_collector.ids import alarm_idempotency_key, metric_idempotency_key
from tpops_kafka_collector.watermark import (
    DEFAULT_IDEMPOTENCY_LRU_CAPACITY,
    IdempotencyKeyLRU,
    WatermarkError,
    WatermarkState,
    WatermarkStore,
)


class FakeCounterDeltaStore:
    """与 ``mapping.rules.CounterDeltaStore`` 同形状的最小替身。

    基线容器是 ``{(metric_code, dimension_key): (counter_value, sampled_at_ms)}``，
    与 ``tools/mappings/rules.py`` 的实现保持一致。
    """

    def __init__(self):
        self._baselines = {}

    def rate(self, metric_code, dimensions, counter_value, now_ms):
        key = (metric_code, _dimension_key(dimensions))
        self._baselines[key] = (counter_value, now_ms)
        return None, '仅建立基线'


def _dimension_key(dimensions):
    if not dimensions:
        return ''
    return '|'.join('%s=%s' % (key, dimensions[key]) for key in sorted(dimensions))


def _state_file(path):
    """目录里除状态文件以外不应有任何残留文件。"""
    directory = os.path.dirname(os.path.abspath(path))
    names = sorted(os.listdir(directory))
    return names


# --------------------------------------------------------------------------
# 四段内容的存取
# --------------------------------------------------------------------------

def test_save_then_load_restores_all_four_sections(tmp_path):
    path = str(tmp_path / 'watermark.json')
    store = WatermarkStore(path)
    store.advance_metric('instance-a', 1700000000000)
    store.advance_alarm('instance-a', '2026-11-20 10:00:00')
    store.put_baseline('xact_commit', 'datname=db1', 120.0, 1700000000000)
    store.remember_key('key-1')
    store.remember_key('key-2')
    store.save()

    restored = WatermarkStore(path)
    restored.load()

    assert restored.metric_watermark('instance-a') == 1700000000000
    assert restored.alarm_watermark('instance-a') == '2026-11-20 10:00:00'
    assert restored.get_baseline('xact_commit', 'datname=db1') == (120.0, 1700000000000)
    assert restored.has_key('key-1')
    assert restored.has_key('key-2')


def test_load_on_missing_file_yields_empty_state(tmp_path):
    store = WatermarkStore(str(tmp_path / 'absent.json'))
    store.load()

    assert store.metric_watermark('instance-a') is None
    assert store.alarm_watermark('instance-a') is None
    assert store.get_baseline('xact_commit', 'datname=db1') is None
    assert store.has_key('key-1') is False


def test_load_rejects_corrupted_state_file(tmp_path):
    path = tmp_path / 'watermark.json'
    path.write_text('{"version": 1, "metric_watermarks": [', encoding='utf-8')

    store = WatermarkStore(str(path))
    with pytest.raises(WatermarkError):
        store.load()


def test_load_rejects_unknown_state_version(tmp_path):
    path = tmp_path / 'watermark.json'
    path.write_text(json.dumps({'version': 999}), encoding='utf-8')

    store = WatermarkStore(str(path))
    with pytest.raises(WatermarkError):
        store.load()


def test_metric_watermark_never_moves_backwards(tmp_path):
    store = WatermarkStore(str(tmp_path / 'watermark.json'))
    store.advance_metric('instance-a', 2000)
    store.advance_metric('instance-a', 1000)

    assert store.metric_watermark('instance-a') == 2000


def test_alarm_watermark_advances_and_is_independent_per_stream(tmp_path):
    store = WatermarkStore(str(tmp_path / 'watermark.json'))
    store.advance_alarm('instance-a', 't-1')
    store.advance_alarm('instance-b', 't-0')

    assert store.alarm_watermark('instance-a') == 't-1'
    assert store.alarm_watermark('instance-b') == 't-0'
    assert store.alarm_watermark('instance-c') is None


# --------------------------------------------------------------------------
# 差分基线：键形状与 CounterDeltaStore 对齐
# --------------------------------------------------------------------------

def test_delta_baseline_roundtrip_uses_counter_delta_store_key_shape(tmp_path):
    store = WatermarkStore(str(tmp_path / 'watermark.json'))
    delta = FakeCounterDeltaStore()
    delta.rate('xact_commit', {'datname': 'db1'}, 100.0, 1000)

    store.import_baselines_from(delta)
    store.save()

    restored = WatermarkStore(str(tmp_path / 'watermark.json'))
    restored.load()
    target = FakeCounterDeltaStore()
    restored.export_baselines_into(target)

    assert target._baselines == {('xact_commit', 'datname=db1'): (100.0, 1000)}


def test_export_baselines_into_counter_delta_store_overwrites_keys(tmp_path):
    store = WatermarkStore(str(tmp_path / 'watermark.json'))
    store.put_baseline('xact_commit', 'datname=db1', 7.0, 10)
    store.put_baseline('xact_commit', 'datname=db2', 8.0, 10)

    delta = FakeCounterDeltaStore()
    store.export_baselines_into(delta)

    assert delta._baselines == {
        ('xact_commit', 'datname=db1'): (7.0, 10),
        ('xact_commit', 'datname=db2'): (8.0, 10),
    }


def test_import_baselines_from_replaces_previous_content(tmp_path):
    store = WatermarkStore(str(tmp_path / 'watermark.json'))
    store.put_baseline('stale_metric', 'datname=db9', 1.0, 1)

    delta = FakeCounterDeltaStore()
    delta.rate('xact_commit', {'datname': 'db1'}, 5.0, 20)
    store.import_baselines_from(delta)

    assert store.get_baseline('stale_metric', 'datname=db9') is None
    assert store.get_baseline('xact_commit', 'datname=db1') == (5.0, 20)


def test_forget_baseline_removes_entry(tmp_path):
    store = WatermarkStore(str(tmp_path / 'watermark.json'))
    store.put_baseline('xact_commit', 'datname=db1', 7.0, 10)
    store.forget_baseline('xact_commit', 'datname=db1')

    assert store.get_baseline('xact_commit', 'datname=db1') is None


# --------------------------------------------------------------------------
# 幂等键 LRU
# --------------------------------------------------------------------------

def test_idempotency_lru_evicts_oldest_inserted_when_over_capacity():
    lru = IdempotencyKeyLRU(capacity=2)
    lru.add('a')
    lru.add('b')
    lru.add('c')

    assert lru.contains('a') is False
    assert lru.contains('b') is True
    assert lru.contains('c') is True


def test_idempotency_lru_refreshes_recency_on_duplicate_add():
    """**真 LRU**：重复键要刷新它的新鲜度。

    规格（``specs/delivery-and-state/spec.md:170``）写的是「最近 N 条消息的幂等键
    集合（LRU，默认 10000 条）」。LRU 淘汰的是**最久未被使用**的键 ——
    一个键被再次遇到（``add`` 返回 ``False``，调用方据此跳过重复投递）就是一次「使用」，
    应当刷新它的新鲜度。

    插入序淘汰（FIFO）在这里是**错的**：一条正在持续重复投递的告警，它的键会被
    长期占用容量却仍被当成"最老"淘汰掉，真正久未出现的键反而留在集合里。
    """
    lru = IdempotencyKeyLRU(capacity=2)
    lru.add('a')
    lru.add('b')
    # 'a' 再次出现 —— 是一次「使用」，刷新新鲜度
    assert lru.add('a') is False
    lru.add('c')

    # LRU：'b' 最久未使用 → 被淘汰；'a' 刚用过 → 留下
    assert lru.contains('a') is True
    assert lru.contains('b') is False
    assert lru.contains('c') is True


def test_idempotency_lru_contains_does_not_refresh_recency():
    """``contains`` 是**查询**不是「使用」—— 不能刷新新鲜度。

    否则一条只被反复查询、实际不再投递的键会永久占着容量。
    """
    lru = IdempotencyKeyLRU(capacity=2)
    lru.add('a')
    lru.add('b')
    assert lru.contains('a') is True
    lru.add('c')

    assert lru.contains('a') is False
    assert lru.contains('b') is True
    assert lru.contains('c') is True


def test_idempotency_lru_capacity_is_configurable_and_has_default():
    assert DEFAULT_IDEMPOTENCY_LRU_CAPACITY == 10000
    assert IdempotencyKeyLRU()._capacity == DEFAULT_IDEMPOTENCY_LRU_CAPACITY


def test_idempotency_lru_rejects_non_positive_capacity():
    with pytest.raises(ValueError):
        IdempotencyKeyLRU(capacity=0)


def test_remember_key_reports_duplicate(tmp_path):
    store = WatermarkStore(str(tmp_path / 'watermark.json'))

    assert store.remember_key('key-1') is True
    assert store.remember_key('key-1') is False
    assert len(store.idempotency_keys()) == 1


def test_idempotency_keys_survive_restart_in_insertion_order(tmp_path):
    path = str(tmp_path / 'watermark.json')
    store = WatermarkStore(path)
    for key in ('a', 'b', 'c'):
        store.remember_key(key)
    store.save()

    restored = WatermarkStore(path)
    restored.load()

    assert restored.idempotency_keys() == ('a', 'b', 'c')


def test_store_evicts_oldest_key_when_capacity_exceeded(tmp_path):
    store = WatermarkStore(str(tmp_path / 'watermark.json'), idempotency_capacity=2)
    store.remember_key('a')
    store.remember_key('b')
    store.remember_key('c')

    assert store.has_key('a') is False
    assert store.has_key('c') is True


# --------------------------------------------------------------------------
# 原子性
# --------------------------------------------------------------------------

def test_failed_write_leaves_previous_state_file_intact(tmp_path, monkeypatch):
    path = tmp_path / 'watermark.json'
    store = WatermarkStore(str(path))
    store.advance_metric('instance-a', 1000)
    store.save()
    original = path.read_text(encoding='utf-8')

    def _half_written_dump(payload, handle):
        """写到一半就炸：模拟磁盘写满 / 进程被打断。"""
        handle.write('{"version": 1, "metric_watermarks": [')
        raise OSError('模拟写入中断')

    monkeypatch.setattr(watermark_module, '_dump_payload', _half_written_dump)

    store.advance_metric('instance-a', 2000)
    with pytest.raises(OSError):
        store.save()

    assert path.read_text(encoding='utf-8') == original
    assert json.loads(path.read_text(encoding='utf-8'))['metric_watermarks'][0]['end_time_ms'] == 1000


def test_failed_write_leaves_no_temporary_file_behind(tmp_path, monkeypatch):
    path = tmp_path / 'watermark.json'
    store = WatermarkStore(str(path))
    store.advance_metric('instance-a', 1000)
    store.save()

    def _boom(payload, handle):
        raise OSError('模拟写入中断')

    monkeypatch.setattr(watermark_module, '_dump_payload', _boom)

    with pytest.raises(OSError):
        store.save()

    assert _state_file(str(path)) == ['watermark.json']


def test_successful_write_leaves_only_the_state_file(tmp_path):
    path = str(tmp_path / 'watermark.json')
    store = WatermarkStore(path)
    store.advance_metric('instance-a', 1000)
    store.save()

    assert _state_file(path) == ['watermark.json']


def test_state_file_always_contains_complete_json(tmp_path):
    path = tmp_path / 'watermark.json'
    store = WatermarkStore(str(path))
    store.advance_metric('instance-a', 1000)
    store.put_baseline('xact_commit', 'datname=db1', 1.0, 2)
    store.save()

    payload = json.loads(path.read_text(encoding='utf-8'))

    assert payload['version'] == 1
    assert payload['metric_watermarks'] == [{'stream': 'instance-a', 'end_time_ms': 1000}]
    assert payload['delta_baselines'] == [{
        'metric_code': 'xact_commit',
        'dimension_key': 'datname=db1',
        'counter_value': 1.0,
        'sampled_at_ms': 2,
    }]


def test_stale_temporary_file_is_ignored_on_load(tmp_path):
    path = tmp_path / 'watermark.json'
    store = WatermarkStore(str(path))
    store.advance_metric('instance-a', 1000)
    store.save()
    (tmp_path / '.watermark-orphan.tmp').write_text('半截 JSON', encoding='utf-8')

    restored = WatermarkStore(str(path))
    restored.load()

    assert restored.metric_watermark('instance-a') == 1000


def test_snapshot_is_immutable(tmp_path):
    store = WatermarkStore(str(tmp_path / 'watermark.json'))
    store.advance_metric('instance-a', 1000)
    snapshot = store.snapshot()

    with pytest.raises(Exception):
        snapshot.metric_watermarks = ()


def test_state_payload_roundtrip_is_stable(tmp_path):
    path = tmp_path / 'watermark.json'
    store = WatermarkStore(str(path))
    store.advance_metric('instance-a', 1000)
    store.advance_alarm('instance-a', 't-1')
    store.put_baseline('xact_commit', 'datname=db1', 1.0, 2)
    store.remember_key('k1')
    store.save()

    state = WatermarkState.from_payload(
        json.loads(path.read_text(encoding='utf-8'))
    )

    assert state == store.snapshot()


def test_idempotency_keys_from_ids_module_dedupe_after_restart(tmp_path):
    """幂等键的真相源是 wave 0 的 ``ids.py``：键算出来 → 落盘 → 重启后仍能去重。"""
    path = str(tmp_path / 'watermark.json')
    activated = alarm_idempotency_key('a-1', 'i-1', 1700000000000, 'activated')
    recovered = alarm_idempotency_key('a-1', 'i-1', 1700000000000, 'recovered')

    store = WatermarkStore(path)
    assert store.remember_key(activated) is True
    assert store.remember_key(recovered) is True
    store.save()

    restored = WatermarkStore(path)
    restored.load()

    assert restored.has_key(activated)
    assert restored.has_key(recovered)
    assert restored.remember_key(activated) is False
    assert restored.remember_key(recovered) is False


def test_metric_idempotency_key_is_stable_across_dimension_order(tmp_path):
    """``ids.py`` 的指标键与维度插入顺序无关 —— LRU 里存的就是这个键。"""
    first = metric_idempotency_key('i-1', 'gaussdb.xact_commit', {'a': 1, 'b': 2}, 1700000000000)
    second = metric_idempotency_key('i-1', 'gaussdb.xact_commit', {'b': 2, 'a': 1}, 1700000000000)

    store = WatermarkStore(str(tmp_path / 'watermark.json'))
    assert store.remember_key(first) is True
    assert store.remember_key(second) is False


def test_real_counter_delta_store_baseline_keys_are_compatible(tmp_path):
    """包内规则引擎落地后，用真的 CounterDeltaStore 验一遍键形状。

    包内 ``mapping.rules`` 尚未落地时本用例跳过（``importorskip``）。
    """
    rules = pytest.importorskip('tpops_kafka_collector.mapping.rules')
    delta = rules.CounterDeltaStore()
    delta.rate('xact_commit', {'datname': 'db1'}, 100.0, 1000)

    store = WatermarkStore(str(tmp_path / 'watermark.json'))
    store.import_baselines_from(delta)
    store.save()

    restored = WatermarkStore(str(tmp_path / 'watermark.json'))
    restored.load()
    target = rules.CounterDeltaStore()
    restored.export_baselines_into(target)

    assert target._baselines == delta._baselines
