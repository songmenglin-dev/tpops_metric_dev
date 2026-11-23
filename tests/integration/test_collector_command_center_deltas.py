# -*- coding: utf-8 -*-
"""B 类采集器的**差分基线**守卫（C-2 / C-3）。

两条 Critical 都是「不报错、但数据没有 / 数据错」的形态：

- **C-2**：``decode_merged`` 没收到采集时刻（``now_ms`` 恒为 0）→
  差分判定「采集时刻未推进」→ 5 个 ``*_records_per_second`` 指标
  **一条都不产出**（其余 97 个指标正常，从外面看不出异常）。
- **C-3**：三个 master **共用一个**差分基线存储，而基线键
  ``(metric_code, dimension_key)`` **不含节点** → 基线互相覆盖，
  2/3 的 master 恒为 ``None``，还可能算出「A 的当前值 − B 的上轮值」
  这种**跨节点速率**。

守的是：

1. **第一轮只建基线、不出值**（差分必须有上一轮才能算）；
2. **第二轮每个 master 都出自己的速率**；
3. **三个 master 的速率互不串**（用**不同的增量**造数据 ——
   基线共享时三个值全错，隔离后各归各）；
4. **同一轮内所有页共用同一个采集时刻**（基线时间轴必须一致）。
"""

import io
import json
import os

import pytest

from conftest import DATA_DIR

from tpops_kafka_collector.collectors.command_center import CommandCenterCollector
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.mapping.rules import CounterDeltaStore
from tpops_kafka_collector.tpops.command_center_result import NodeTask, SqlOutcome

MERGED_DIR = os.path.join(DATA_DIR, 'merged_sql')
PROBE_PATH = os.path.join(MERGED_DIR, 'sql_merge_probe_result.json')
NODE_PATH = os.path.join(
    DATA_DIR, 'metric_response', '003_GAUSSDB.NAME.Number.ad.ples.json'
)

INSTANCE_ID = '83cd68f2e4cb40b4b7e69240479ffa26in14'

#: 期望值表（B 类 82 条的逐维度期望值 + 差分计划）。
EXPECTED_PATH = os.path.join(DATA_DIR, 'mapping', 'expected_values.json')

#: 五个累计计数差分的客户指标码（模板形态）。
DELTA_CODES = (
    'GAUSSDB.[{#DATNAME}]_delete_records_per_second',
    'GAUSSDB.[{#DATNAME}]_fetch_records_per_second',
    'GAUSSDB.[{#DATNAME}]_insert_records_per_second',
    'GAUSSDB.[{#DATNAME}]_return_records_per_second',
    'GAUSSDB.[{#DATNAME}]_update_records_per_second',
)

#: 差分涉及的累计计数列（``pg_stat_database`` 的计数列）。
COUNTER_COLUMNS = (
    'tup_deleted', 'tup_fetched', 'tup_inserted', 'tup_returned', 'tup_updated',
)

#: 两轮之间的时间差（秒）—— 速率 = 增量 / 这个秒数。
ELAPSED_SECONDS = 60

#: 第一轮的采集时刻（毫秒）。
ROUND1_NOW_MS = 1760000000000

#: 三个 master **各自的**累计计数增量（秒）——
#: 故意取成互不相同：基线共享时三个速率会全错，隔离后各归各。
DELTA_AMOUNT_BY_NODE_NAME = {
    'gauss-6374_gaussdbv5_hcs2-0_0': 6000,
    'gauss-6374_gaussdbv5_hcs2-0_1': 3000,
    'gauss-6374_gaussdbv5_hcs2-0_2': 9000,
}


def _load(path):
    with io.open(path, encoding='utf-8') as handle:
        return json.load(handle)


@pytest.fixture(scope='module')
def probe_nodes():
    """实测留档的合并形态返回体。"""
    return _load(PROBE_PATH)['nodes']


@pytest.fixture(scope='module')
def node_rows():
    """节点清单（node_id ↔ node_name ↔ node_ip）。"""
    for step in _load(NODE_PATH):
        if 'key-view-execute-node' in step.get('api', ''):
            return tuple(step['response_body']['nodes'])
    raise AssertionError('留档里没有节点清单的响应')


@pytest.fixture(scope='module')
def mapping():
    return load_mapping()


class DeltaCommandCenter:
    """假命令中心：每轮把 ``stat_database`` 的累计计数加上各自的增量。"""

    def __init__(self, probe_nodes, node_rows, uniform_amount=None):
        self._probe = probe_nodes
        self._nodes = tuple(node_rows)
        #: 给定则三个 master 用**同一个**增量（复现期望值表的差分计划）
        self._uniform_amount = uniform_amount
        self._node_id_by_name = dict(
            (row['node_name'], row['node_id']) for row in self._nodes
        )
        self.calls = 0
        #: 第几轮（0 起）—— 累计计数的增量按轮次累加。
        self.round_index = 0

    def execute_sql(self, instance_id, sql_text, limit='1000'):
        self.calls += 1
        tasks = []
        for node_name, entry in self._probe.items():
            rows = _bump(json.loads(entry['result']), self._delta_of(node_name))
            tasks.append(NodeTask(
                node_id=self._node_id_by_name.get(node_name, node_name),
                task_id='task-%s-%d' % (node_name, self.calls),
                status='succeed',
                rows=tuple(rows),
            ))
        return SqlOutcome(sql_text=sql_text, ok=True, tasks=tuple(tasks), attempts=1)

    def master_nodes(self, instance_id):
        return self._nodes

    def _delta_of(self, node_name):
        """该 master 本轮的累计计数增量（每个计数列都加同样的量）。

        **增量是累加的** —— 第 N 轮的值 = 原始值 + N × 增量
        （累计计数器只会往上走）。
        """
        if self._uniform_amount is not None:
            amount = self._uniform_amount * (self.round_index + 1)
        else:
            amount = DELTA_AMOUNT_BY_NODE_NAME.get(node_name, 0) * (self.round_index + 1)
        return dict((column, amount) for column in COUNTER_COLUMNS)


def _bump(rows, delta):
    """把合并行里 ``stat_database`` 视图的累计计数加上增量（返回新行）。"""
    new_rows = []
    for row in rows:
        new_row = dict(row)
        payload = new_row.get('stat_database')
        if isinstance(payload, str) and payload:
            items = json.loads(payload)
            for item in items:
                for column, amount in delta.items():
                    value = item.get(column)
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        item[column] = value + amount
            new_row['stat_database'] = json.dumps(items)
        new_rows.append(new_row)
    return new_rows


def _collector(command_center, mapping, clock):
    """采集器（自带一个空的差分基线存储 —— 它必须**跨轮**存活）。"""
    return CommandCenterCollector(
        client=None,
        mapping=mapping,
        command_center=command_center,
        deltas=CounterDeltaStore(),
        clock=clock,
    )


def _delta_messages(messages):
    """挑出差分类指标的消息（按模板码归并 —— 消息里是替换后的码）。"""
    picked = []
    for message in messages:
        code = message['metricCode']
        if '_records_per_second' in code:
            picked.append(message)
    return picked


def _rates_by_node(messages):
    """``{node_id: {模板码: 速率}}`` —— 模板码按维度还原。"""
    result = {}
    for message in _delta_messages(messages):
        dims = message['tags'].get('dimension_values') or {}
        code = message['metricCode']
        for placeholder, value in dims.items():
            code = code.replace(str(value), '{#%s}' % placeholder)
        node_id = message['tags'].get('node_id')
        result.setdefault(node_id, {})[code] = message['value']
    return result


# ---------------------------------------------------------------------------
# 守卫
# ---------------------------------------------------------------------------

def test_first_round_only_establishes_the_baseline(mapping, probe_nodes, node_rows):
    """第一轮没有基线 → **只建基线，不出值**（不是 0，也不是假数据）。"""
    cc = DeltaCommandCenter(probe_nodes, node_rows)
    collector = _collector(cc, mapping, lambda: ROUND1_NOW_MS)

    result = collector.collect(instance_id=INSTANCE_ID)

    assert _delta_messages(result.messages) == []
    # 其余指标照常产出（差分类不产出不影响别的）
    assert result.messages


def test_counter_delta_produces_a_rate_on_the_second_round(
    mapping, probe_nodes, node_rows
):
    """第二轮：五个 ``*_records_per_second`` 指标都产出速率。

    **第一轮建基线、第二轮才有值** —— 这是差分的定义。
    """
    cc = DeltaCommandCenter(probe_nodes, node_rows)
    state = {'round': 0}

    def _clock():
        return ROUND1_NOW_MS + state['round'] * ELAPSED_SECONDS * 1000

    collector = _collector(cc, mapping, _clock)

    state['round'] = 0
    cc.round_index = 0
    collector.collect(instance_id=INSTANCE_ID)
    state['round'] = 1
    cc.round_index = 1
    second = collector.collect(instance_id=INSTANCE_ID)

    rates = _rates_by_node(second.messages)
    assert set(rates) == set(_node_id_by_name(node_rows).values()), rates
    for node_id, values in rates.items():
        assert set(values) == set(DELTA_CODES), (node_id, sorted(values))


def test_every_master_computes_its_own_rate(mapping, probe_nodes, node_rows):
    """三个 master **各算各的**速率，互不串（C-3）。

    三个 master 的累计计数增量被故意设成 6000 / 3000 / 9000 ——
    基线共享时三个速率**全错**（实测 100/50/150 会变成 50/None/200），
    基线按节点隔离后才各归各。
    """
    cc = DeltaCommandCenter(probe_nodes, node_rows)
    state = {'round': 0}

    def _clock():
        return ROUND1_NOW_MS + state['round'] * ELAPSED_SECONDS * 1000

    collector = _collector(cc, mapping, _clock)

    state['round'] = 0
    cc.round_index = 0
    collector.collect(instance_id=INSTANCE_ID)
    state['round'] = 1
    cc.round_index = 1
    second = collector.collect(instance_id=INSTANCE_ID)

    rates = _rates_by_node(second.messages)
    node_id_by_name = _node_id_by_name(node_rows)
    expected_by_node = {}
    for node_name, amount in DELTA_AMOUNT_BY_NODE_NAME.items():
        node_id = node_id_by_name.get(node_name)
        if node_id:
            expected_by_node[node_id] = amount // ELAPSED_SECONDS

    assert len(expected_by_node) == 3
    for node_id, expected in expected_by_node.items():
        values = rates.get(node_id) or {}
        assert values, '节点 %s 一条差分指标都没产出' % node_id
        for code in DELTA_CODES:
            assert values.get(code) == expected, (
                node_id, code, values.get(code), expected,
            )
    # 三个 master 的速率必须**互不相同**（用不同的增量造的数据）
    distinct = set(expected_by_node.values())
    assert len(distinct) == 3, expected_by_node


def test_counter_delta_rate_survives_a_restart_with_a_seeded_store(
    mapping, probe_nodes, node_rows
):
    """基线能跨轮延续（**同一个采集器实例**就是跨轮延续的载体）。

    采集器每轮新建一个 → 差分永远只有首轮，速率永远算不出来。
    """
    cc = DeltaCommandCenter(probe_nodes, node_rows)
    state = {'round': 0}

    def _clock():
        return ROUND1_NOW_MS + state['round'] * ELAPSED_SECONDS * 1000

    collector = _collector(cc, mapping, _clock)

    state['round'] = 0
    cc.round_index = 0
    collector.collect(instance_id=INSTANCE_ID)
    state['round'] = 1
    cc.round_index = 1
    second = collector.collect(instance_id=INSTANCE_ID)
    state['round'] = 2
    cc.round_index = 2
    third = collector.collect(instance_id=INSTANCE_ID)

    assert _rates_by_node(second.messages)
    assert _rates_by_node(third.messages)


def _node_id_by_name(node_rows):
    return dict((row['node_name'], row['node_id']) for row in node_rows)


def test_counter_delta_rates_match_the_expected_values_plan(
    mapping, probe_nodes, node_rows
):
    """第二轮的速率 == **期望值表的 ``counter_delta_plan``** 推出的速率。

    期望值表里写死了差分计划（``data/mapping/expected_values.json`` 的
    ``counter_delta_plan``）：每个累计计数列 +1000、时间差 60 秒
    → 速率 = 1000 / 60。采集器层算出来的必须与它一致
    （**三个 master 各自的**，不串）。
    """
    plan = _load(EXPECTED_PATH)['counter_delta_plan']
    elapsed = float(plan['elapsed_seconds'])
    columns = sorted(plan['deltas'])
    amount = int(plan['deltas'][columns[0]])
    expected = int(amount / elapsed)

    cc = DeltaCommandCenter(probe_nodes, node_rows, uniform_amount=amount)
    state = {'round': 0}

    def _clock():
        return ROUND1_NOW_MS + state['round'] * ELAPSED_SECONDS * 1000

    collector = _collector(cc, mapping, _clock)
    state['round'] = 0
    cc.round_index = 0
    collector.collect(instance_id=INSTANCE_ID)
    state['round'] = 1
    cc.round_index = 1
    second = collector.collect(instance_id=INSTANCE_ID)

    rates = _rates_by_node(second.messages)
    assert len(rates) == 3, rates
    for node_id, values in rates.items():
        assert set(values) == set(DELTA_CODES), (node_id, sorted(values))
        for code, value in values.items():
            assert value == expected, (node_id, code, value, expected)
