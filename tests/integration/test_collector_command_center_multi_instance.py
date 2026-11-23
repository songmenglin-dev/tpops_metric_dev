"""差分基线的**多实例作用域**守卫测试（任务 3.3 的第 ④ 项）。

wave-2b 的 **C-3** 缺陷（差分基线跨 master 共享）的**多实例版本**：
两个实例共用同一个 ``node_id`` 时，基线作用域必须能区分实例，
否则**不报错、但数据错** —— 各自的速率会算成对方的。
"""
import pytest

from tpops_kafka_collector.collectors import command_center as cc_module
from tpops_kafka_collector.collectors.command_center import CommandCenterCollector
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.mapping.rules_deltas import CounterDeltaStore
from tpops_kafka_collector.watermark import WatermarkStore


class FakeOutcome:
    """命令中心的假执行结果。"""

    ok = True
    tasks = ()
    error = None


class FakeCommandCenter:
    """假命令中心客户端。"""

    def __init__(self, nodes=()):
        self._nodes = tuple(nodes)
        self.calls = []

    def master_nodes(self, instance_id):
        return self._nodes

    def execute_sql(self, instance_id, sql_text, limit):
        self.calls.append(instance_id)
        return FakeOutcome()


def _collector(deltas, nodes=()):
    return CommandCenterCollector(
        client=None,
        mapping=load_mapping(),
        command_center=FakeCommandCenter(nodes),
        deltas=deltas,
    )


def test_delta_store_scope_contains_instance_id():
    """作用域必须是 ``f'{instance_id}:{node_id}'``。"""
    collector = _collector(CounterDeltaStore())
    view = collector._delta_store('inst-a', 'node-1')
    assert view.scope == 'inst-a:node-1'


def test_two_instances_sharing_node_id_keep_separate_baselines():
    """两个实例共用同一个 node_id → 差分基线**互不覆盖**。"""
    deltas = CounterDeltaStore()
    collector = _collector(deltas)
    view_a = collector._delta_store('inst-a', 'node-1')
    view_b = collector._delta_store('inst-b', 'node-1')
    assert view_a is not view_b

    # 首轮：各自只建基线
    assert view_a.rate('m.x', {'d': '1'}, 100.0, 1000)[0] is None
    assert view_b.rate('m.x', {'d': '1'}, 200.0, 1000)[0] is None

    # 第二轮：各自的速率只跟自己的基线比
    rate_a, _ = view_a.rate('m.x', {'d': '1'}, 110.0, 2000)
    rate_b, _ = view_b.rate('m.x', {'d': '1'}, 260.0, 2000)
    assert rate_a == pytest.approx(10.0)
    assert rate_b == pytest.approx(60.0)


def test_rates_survive_restore_from_watermark(tmp_path):
    """恢复（重启）后仍能各自算出速率。"""
    deltas = CounterDeltaStore()
    collector = _collector(deltas)
    view_a = collector._delta_store('inst-a', 'node-1')
    view_b = collector._delta_store('inst-b', 'node-1')
    view_a.rate('m.x', {'d': '1'}, 100.0, 1000)
    view_b.rate('m.x', {'d': '1'}, 200.0, 1000)

    store = WatermarkStore(str(tmp_path / 'watermark.json'))
    store.import_baselines_from(deltas)
    store.save()

    restored = CounterDeltaStore()
    reloaded = WatermarkStore(str(tmp_path / 'watermark.json'))
    reloaded.load()
    reloaded.export_baselines_into(restored)

    new_collector = _collector(restored)
    new_a = new_collector._delta_store('inst-a', 'node-1')
    new_b = new_collector._delta_store('inst-b', 'node-1')
    rate_a, _ = new_a.rate('m.x', {'d': '1'}, 110.0, 2000)
    rate_b, _ = new_b.rate('m.x', {'d': '1'}, 260.0, 2000)
    assert rate_a == pytest.approx(10.0)
    assert rate_b == pytest.approx(60.0)


def test_collect_passes_instance_scoped_delta_store(monkeypatch):
    """``collect()`` 传给装配层的 delta_store 视图也带实例作用域。"""
    captured = {}

    def _fake_build_messages(**kwargs):
        captured['delta_store'] = kwargs['delta_store']
        return []

    monkeypatch.setattr(cc_module, 'build_messages', _fake_build_messages)

    collector = _collector(
        CounterDeltaStore(),
        nodes=({'node_id': 'node-1', 'host_ip': '10.0.0.1'},),
    )
    collector.collect('inst-a', 'gauss-a')

    view = captured['delta_store']('node-1')
    assert view.scope == 'inst-a:node-1'
