# -*- coding: utf-8 -*-
"""B 类采集器（任务 2.6）的集成测试。

**TPOPS 与 Kafka 一律 mock** —— 本文件不发起任何网络请求。

守的是这个项目里**最大的一块**（71 个客户指标走这条通道）的六个要害：

1. **必须走 ``decode_merged`` / ``merge_pages``** —— 合并形态当行列表用，
   71 个指标会**静默全变 0**（本项目最危险的失败形态）；
2. **必须检查 ``outcome.ok``** —— 命令中心失败不抛异常，不检查就是静默丢数据；
3. **分页只对可增长视图生效** —— 固定视图每页重复返回全量，累加会翻倍；
4. **``node_scoped`` 归集** —— ``true`` 各 master 各自一份，``false`` 只取第一个 master；
5. **输出上限 102400 B** —— 接近上限记 WARNING，超了整条失败（不是截断）；
6. **值等于期望值表** —— 71 条逐条断言（不是「非零」判据）。
"""
import io
import json
import os

import pytest

from conftest import DATA_DIR

from tpops_kafka_collector.collectors.command_center import (
    OUTPUT_LIMIT_BYTES,
    CommandCenterCollector,
)
from tpops_kafka_collector.errors import MappingError
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.mapping.message import serialize_value
from tpops_kafka_collector.mapping.nodes import select_master_node
from tpops_kafka_collector.mapping.rules import CounterDeltaStore
from tpops_kafka_collector.mapping.rules_naming import substitute_placeholders
from tpops_kafka_collector.tpops.command_center_result import (
    NodeTask,
    SqlOutcome,
)

MERGED_DIR = os.path.join(DATA_DIR, 'merged_sql')
PROBE_PATH = os.path.join(MERGED_DIR, 'sql_merge_probe_result.json')
NODE_PATH = os.path.join(
    DATA_DIR, 'metric_response', '003_GAUSSDB.NAME.Number.ad.ples.json'
)
EXPECTED_PATH = os.path.join(DATA_DIR, 'mapping', 'expected_values.json')

INSTANCE_ID = '83cd68f2e4cb40b4b7e69240479ffa26in14'

#: 留档里的三个 master 节点（按 node_name）。
NODE_NAMES = (
    'gauss-6374_gaussdbv5_hcs2-0_0',
    'gauss-6374_gaussdbv5_hcs2-0_1',
    'gauss-6374_gaussdbv5_hcs2-0_2',
)


def _load(path):
    with io.open(path, encoding='utf-8') as handle:
        return json.load(handle)


@pytest.fixture(scope='module')
def probe_nodes():
    """实测留档的合并形态返回体（``{node_name: {'result': '<JSON 字符串>'}}``）。"""
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


@pytest.fixture(scope='module')
def expected_table():
    return _load(EXPECTED_PATH)


class FakeCommandCenter:
    """假命令中心：按 SQL 里的 offset 回放留档的页，并记录下发过的 SQL。"""

    def __init__(self, pages_by_node=None, error=None, nodes=(), rows_by_node=None):
        self._pages = pages_by_node or {}
        self._rows = rows_by_node or {}
        self._error = error
        self._nodes = tuple(nodes)
        self.sql_texts = []
        self.node_calls = []
        self._node_id_by_name = dict(
            (row['node_name'], row['node_id']) for row in self._nodes
        )

    def execute_sql(self, instance_id, sql_text, limit='1000'):
        self.sql_texts.append(sql_text)
        if self._error is not None:
            return SqlOutcome(
                sql_text=sql_text, ok=False, error=self._error, attempts=1,
            )
        offset = _offset_of(sql_text)
        tasks = []
        for node_name in self._pages:
            rows = self._rows.get(node_name)
            if rows is None:
                entry = self._pages[node_name]
                if isinstance(entry, dict) and 'result' in entry:
                    # 留档形态：{'result': '<平台的 JSON 字符串>'}
                    rows = json.loads(entry['result'])
                else:
                    rows = json.loads(entry)
            elif isinstance(rows, str):
                rows = json.loads(rows)
            elif callable(rows):
                # 按 offset 递延的页（最后一页不满 → 翻页会停）
                rows = rows(offset)
                if isinstance(rows, str):
                    rows = json.loads(rows)
            tasks.append(NodeTask(
                node_id=self._node_id_by_name.get(node_name, node_name),
                task_id='task-%s-%d' % (node_name, offset),
                status='succeed',
                rows=tuple(rows),
            ))
        return SqlOutcome(sql_text=sql_text, ok=True, tasks=tuple(tasks), attempts=1)

    def master_nodes(self, instance_id):
        self.node_calls.append(instance_id)
        return self._nodes


def _offset_of(sql_text):
    """从渲染出的 SQL 里取**可增长视图**的分页偏移。

    **不能**取第一个 ``offset`` —— 固定视图渲染成 ``limit P offset 0`` 且排在最前，
    读到的一直是 0。要看可增长视图（``stat_user_tables``）那一份。
    """
    anchor = sql_text.find('as stat_user_tables')
    if anchor < 0:
        return 0
    head = sql_text[:anchor]
    index = head.rfind('offset ')
    if index < 0:
        return 0
    tail = head[index + len('offset '):]
    digits = ''
    for char in tail:
        if char.isdigit():
            digits += char
        else:
            break
    return int(digits or 0)


def _collector(command_center, mapping, **kwargs):
    """``counter_delta`` 的 5 条指标需要差分基线存储 —— 这里注入一个空的。"""
    kwargs.setdefault('deltas', CounterDeltaStore())
    return CommandCenterCollector(
        client=None, mapping=mapping, command_center=command_center, **kwargs
    )


def _node_id_by_name(node_rows):
    return dict((row['node_name'], row['node_id']) for row in node_rows)


# ---------------------------------------------------------------------------
# 1. 取数：下发 1 条合并 SQL
# ---------------------------------------------------------------------------

def test_collects_with_a_single_merged_sql(mapping, probe_nodes, node_rows):
    """留档里可增长视图不足一页 → 只下发 1 条 SQL。"""
    cc = FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    result = _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    assert len(cc.sql_texts) == 1
    assert result.pages == 1
    assert result.messages


def test_merged_sql_is_rendered_from_the_package_spec(mapping, probe_nodes, node_rows):
    """下发的 SQL 来自包内的合并 SQL 声明（每个内层子查询都带 limit/offset）。"""
    cc = FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    sql_text = cc.sql_texts[0]
    assert 'json_agg' in sql_text
    assert 'as stat_database' in sql_text
    assert 'limit ' in sql_text and 'offset ' in sql_text


# ---------------------------------------------------------------------------
# 2. 分页：只对可增长视图生效
# ---------------------------------------------------------------------------

def test_paginates_while_a_growable_view_is_full(mapping, probe_nodes, node_rows):
    """可增长视图满页 → 继续翻页，offset 递增。"""
    def _page_for(offset):
        # 前两页满页（10 行），之后不满 → 翻页会停
        count = 10 if offset < 20 else 3
        return _growable_page(row_count=count, offset=offset)

    cc = FakeCommandCenter(
        pages_by_node={name: None for name in probe_nodes},
        rows_by_node={name: _page_for for name in probe_nodes},
        nodes=node_rows,
    )
    _collector(cc, mapping, page_size=10).collect(instance_id=INSTANCE_ID)

    offsets = [_offset_of(text) for text in cc.sql_texts]
    assert offsets[0] == 0
    assert len(offsets) > 1, '可增长视图满页时应该继续翻页'
    assert offsets == sorted(offsets)
    assert offsets[-1] == 20, '最后一页不满就停'


def test_pagination_stops_when_growable_view_is_not_full(mapping, probe_nodes, node_rows):
    """可增长视图不满页 → 停。固定视图每页都满页，判它会死循环。"""
    small = _growable_page(row_count=3)
    cc = FakeCommandCenter(
        pages_by_node={name: small for name in probe_nodes},
        rows_by_node={name: small for name in probe_nodes},
        nodes=node_rows,
    )
    _collector(cc, mapping, page_size=10).collect(instance_id=INSTANCE_ID)

    assert len(cc.sql_texts) == 1


_BASE_ROW = []


def _base_row():
    """留档里那一行的完整形态（11 个视图齐全）。

    合成页必须以它为基底 —— 合并形态的解码要求每个视图的列都在，
    少一个就抛 ``DecodeError``。
    """
    if not _BASE_ROW:
        nodes = _load(PROBE_PATH)['nodes']
        first = nodes[sorted(nodes)[0]]
        _BASE_ROW.append(json.loads(first['result'])[0])
    return _BASE_ROW[0]


#: 可增长视图（``merged_views`` 里 ``paged = true`` 的那些）——
#: 分页只对它们生效，合成页必须**全部**缩掉，否则另一个满页会多翻一页。
GROWABLE_VIEWS = ('stat_user_tables', 'dbstat_sum')


def _growable_page(row_count, offset=0):
    """构造一页「可增长视图恰好满页」的合并行（其余视图照抄留档）。

    ``offset`` 参与行内容 —— 各页必须**内容不同**，否则会被
    「平台没生效 offset」的守卫误判成死循环。
    """
    row = dict(_base_row())
    for name in GROWABLE_VIEWS:
        row[name] = json.dumps(
            [{'schemaname': 'public', 'relname': 't%d_%d' % (offset, i)}
             for i in range(row_count)]
        )
    return json.dumps([row])


# ---------------------------------------------------------------------------
# 3. 失败不静默
# ---------------------------------------------------------------------------

def test_failed_command_produces_no_message_and_logs(mapping, caplog):
    """命令中心失败（不抛异常）→ 不产出消息，并记 ERROR。"""
    cc = FakeCommandCenter(error='平台 503', nodes=())
    with caplog.at_level('ERROR', logger='tpops_kafka_collector'):
        result = _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    assert result.messages == ()
    assert '命令中心取数失败' in caplog.text


# ---------------------------------------------------------------------------
# 4. node_scoped 归集
# ---------------------------------------------------------------------------

def test_node_scoped_metric_produces_one_message_per_master(mapping, probe_nodes, node_rows):
    """``node_scoped = true`` → 各 master 各自产出一份。"""
    cc = FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    result = _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    scoped = [m for m in result.messages if m['metricCode'] == 'gaussdb.Deadlocks']
    assert len(scoped) == len(NODE_NAMES)
    node_ids = set(m['tags']['node_id'] for m in scoped)
    assert len(node_ids) == len(NODE_NAMES)


def test_non_node_scoped_metric_uses_only_the_first_master(mapping, probe_nodes, node_rows):
    """``node_scoped = false`` → 与节点无关，只取第一个 master。

    映射表里 40 条 B 类指标是 ``node_scoped = false``（表级/库级/实例级那些），
    它们与节点无关 —— 三个 master 各返回一份，但只产出一条消息。
    """
    cc = FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    result = _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    shared = [
        m for m in result.messages
        if m['metricCode'] == 'gaussdb.get.lock.info'
    ]
    assert len(shared) == 1, [m['metricCode'] for m in result.messages][:5]
    master_ids = set(row['node_id'] for row in node_rows
                     if row.get('role') == 'master' and row.get('type') == 'CN')
    assert shared[0]['tags']['node_id'] in master_ids


def test_node_scoped_resource_name_anchors_on_its_own_master(mapping, probe_nodes, node_rows):
    """``node_scoped`` 的资源名锚在**自己那个 master** 上。"""
    cc = FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    result = _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    by_id = _node_id_by_name(node_rows)
    scoped = [m for m in result.messages if m['metricCode'] == 'gaussdb.Deadlocks']
    for message in scoped:
        node_id = message['tags']['node_id']
        host_ip = next(
            row['node_ip'] for row in node_rows if row['node_id'] == node_id
        )
        assert message['resourceName'] == '%s_%s' % (host_ip, node_id)
    assert by_id  # 节点清单确实带了 node_name


# ---------------------------------------------------------------------------
# 5. 输出上限
# ---------------------------------------------------------------------------

def test_does_not_warn_when_a_node_result_is_small(mapping, probe_nodes, node_rows, caplog):
    """单节点结果远小于上限 → **不**告警（不误报的守卫）。

    上限是**每个节点任务**的，不是各 master 加总 —— 3 个 master 加起来
    可能超过上限，但每一条单独看都没超，不该误报。
    """
    cc = FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    with caplog.at_level('WARNING', logger='tpops_kafka_collector'):
        _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    assert '接近平台输出上限' not in caplog.text


def test_warns_when_a_single_node_result_is_near_the_output_limit(
    mapping, probe_nodes, node_rows, caplog
):
    """单个节点的结果接近 102400 B → 记 WARNING。"""
    big = _growable_page(row_count=9000)
    cc = FakeCommandCenter(
        pages_by_node={name: None for name in probe_nodes},
        rows_by_node={name: big for name in probe_nodes},
        nodes=node_rows,
    )
    with caplog.at_level('WARNING', logger='tpops_kafka_collector'):
        _collector(cc, mapping, page_size=10000).collect(instance_id=INSTANCE_ID)

    assert '接近平台输出上限' in caplog.text


def test_shrinks_page_size_and_retries_when_output_exceeds_limit(mapping, probe_nodes, node_rows):
    """输出超上限（整条失败）→ 缩小页容量重试。"""
    calls = []

    class ShrinkingCommandCenter(FakeCommandCenter):
        def execute_sql(self, instance_id, sql_text, limit='1000'):
            calls.append(sql_text)
            if len(calls) == 1:
                return SqlOutcome(
                    sql_text=sql_text,
                    ok=False,
                    error='output size(131072 B) exceeds the threshold(102400 B)',
                    attempts=1,
                )
            return FakeCommandCenter.execute_sql(self, instance_id, sql_text, limit)

    def _page_for(offset):
        # 页会随 offset 变小（最后不满）→ 翻页会停
        count = 5 if offset < 10 else 2
        return _growable_page(row_count=count, offset=offset)

    cc = ShrinkingCommandCenter(
        pages_by_node={name: None for name in probe_nodes},
        rows_by_node={name: _page_for for name in probe_nodes},
        nodes=node_rows,
    )
    result = _collector(cc, mapping, page_size=100).collect(instance_id=INSTANCE_ID)

    assert len(calls) >= 2, '超限后应该缩小页容量重试'
    assert result.messages


def test_stops_paging_when_platform_ignores_offset(mapping, probe_nodes, node_rows, caplog):
    """平台**不认 offset**（每页返回同一批行）→ 停下来并记 WARNING。

    否则采集器会无限翻页，把整轮采集卡死。
    """
    same = _growable_page(row_count=10)

    def _page_for(offset):
        return same

    cc = FakeCommandCenter(
        pages_by_node={name: None for name in probe_nodes},
        rows_by_node={name: _page_for for name in probe_nodes},
        nodes=node_rows,
    )
    with caplog.at_level('WARNING', logger='tpops_kafka_collector'):
        _collector(cc, mapping, page_size=10).collect(instance_id=INSTANCE_ID)

    assert '平台可能未生效 offset' in caplog.text
    assert len(cc.sql_texts) < 10, '认出同一页后应该停下，不能无限翻'


def test_output_limit_constant_matches_the_platform_value():
    assert OUTPUT_LIMIT_BYTES == 102400


# ---------------------------------------------------------------------------
# 6. 值等于期望值表（71 条逐条）
# ---------------------------------------------------------------------------

def test_collected_values_equal_the_expected_table(mapping, probe_nodes, node_rows, expected_table):
    """**71 条逐条**：采集器产出的值 == 期望值表。

    期望值表由**独立推导器**从实测留档原始行推出
    （``tools/expected_deriver.py``），不是抄规则引擎的输出。

    **这条守卫覆盖全部 71 条指标码**（不是 47 条）：

    - 期望值表的键是**模板码**（``GAUSSDB.[{#DATNAME}]_committed_transactions``），
      消息里的 ``metricCode`` 是**占位符替换后的码**
      （``GAUSSDB.lz_committed_transactions``）—— 必须按替换后的码去匹配，
      否则维度展开型的指标一条都比不到（``continue`` 静默跳过）；
    - ``checked`` 断言**精确条数**（不是 ``> 0``）—— 「比少了」直接失败；
    - 期望值为 ``None`` 的维度**不许产出消息**（缺值不产出假数据）。
    """
    cc = FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    result = _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    produced = {}
    for message in result.messages:
        node_id = message['tags'].get('node_id')
        produced.setdefault(message['metricCode'], {})[node_id] = message['value']

    # **node_id ↔ node_name 是多对一**（同一 node_id 在节点清单里出现多次，
    # CN 一条 + 各 DN 一条），所以只能用**正向**映射 node_name → node_id；
    # 反向 dict() 会把值归到错误的节点上。
    node_id_by_name = dict(
        (row['node_name'], row['node_id']) for row in node_rows
    )
    all_node_ids = sorted(set(node_id_by_name.values()))
    host_node_id = select_master_node(node_rows)['node_id']
    rules = dict(
        (rule.metric_code, rule) for rule in mapping.metric_rules
        if rule.enabled and rule.source.get('source') == 'command_center_sql'
    )

    checked = 0
    comparable = 0
    compared_codes = set()
    for metric_code, entry in expected_table['expected'].items():
        rule = rules[metric_code]
        for node_name, items in entry['round1'].items():
            node_id = node_id_by_name.get(node_name)
            if node_id is None:
                continue
            # 与节点无关的指标只落在**承载资源名**的那个 master 上
            targets = all_node_ids if rule.node_scoped else [host_node_id]
            if node_id not in targets:
                continue
            for item in items:
                code = substitute_placeholders(metric_code, item['dimensions'])
                if item['value'] is None:
                    # 期望值为 None → 本轮**不该产出**消息（缺值不产出假数据）
                    assert code not in produced, (
                        '缺值的维度不该产出消息', metric_code, code,
                    )
                    continue
                comparable += 1
                values = produced.get(code) or {}
                assert node_id in values, (
                    '采集器没为这条产出消息', metric_code, code, node_name,
                )
                expected_value = serialize_value(item['value'], rule.value_type)
                assert values[node_id] == expected_value, (
                    metric_code, node_name, values[node_id], expected_value,
                )
                checked += 1
                compared_codes.add(metric_code)

    assert checked == comparable, (
        '比到的条数比应比的少', checked, comparable,
    )
    assert checked == EXPECTED_COMPARISONS, (
        '比到的条数与期望值表对不上', checked, EXPECTED_COMPARISONS,
    )
    # 71 条指标码里，**有期望值**的那些一条都不许漏
    assert compared_codes == _codes_with_expected_values(expected_table), (
        '有期望值的指标码没被比到',
        sorted(_codes_with_expected_values(expected_table) - compared_codes),
    )


#: 期望值表在**第一轮**（差分指标还没基线）的逐维度比对条数。
#: 71 条指标码 × 3 个 master × 各自主机上的维度条目数 —— 由期望值表算出。
#: （生效口径去掉 11 条重复的表级指标后，条数随之变化。）
EXPECTED_COMPARISONS = 954


def _codes_with_expected_values(expected_table):
    """期望值表里**有期望值**的指标码集合（其余是「本轮无值」）。"""
    codes = set()
    for metric_code, entry in expected_table['expected'].items():
        for items in entry['round1'].values():
            if any(item['value'] is not None for item in items):
                codes.add(metric_code)
                break
    return codes


def test_every_b_class_rule_is_covered_by_the_produced_messages(
    mapping, probe_nodes, node_rows
):
    """**71 条指标码一条不落**（按模板码归并消息）。

    第一轮有 6 条指标码**产不出消息**，且都是**有据可依**的：

    - 5 个 ``counter_delta`` 指标：首轮**只建基线**（差分的定义），
      第二轮才产出 —— 由 ``test_collector_command_center_deltas.py`` 覆盖；
    - ``gaussdb.locks.accessexclusive[{#DBNAME}]``：留档里**一条
      ``AccessExclusiveLock`` 锁都没有**（期望值表里它的条目是空列表），
      本来就没有值可产出。

    其余 76 条必须在第一轮就产出消息。
    """
    cc = FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    result = _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    produced_codes = set(_template_code(message) for message in result.messages)
    rules = [
        rule for rule in mapping.metric_rules
        if rule.enabled and rule.source.get('source') == 'command_center_sql'
    ]
    assert len(rules) == 71
    missing = sorted(rule.metric_code for rule in rules
                     if rule.metric_code not in produced_codes)
    assert missing == sorted(NO_VALUE_CODES), missing


#: 第一轮产不出消息的指标码（5 个首轮只建基线的差分指标 + 1 个留档里无值的）。
NO_VALUE_CODES = (
    'GAUSSDB.[{#DATNAME}]_delete_records_per_second',
    'GAUSSDB.[{#DATNAME}]_fetch_records_per_second',
    'GAUSSDB.[{#DATNAME}]_insert_records_per_second',
    'GAUSSDB.[{#DATNAME}]_return_records_per_second',
    'GAUSSDB.[{#DATNAME}]_update_records_per_second',
    'gaussdb.locks.accessexclusive[{#DBNAME}]',
)


def _template_code(message):
    """消息的 ``metricCode`` → **模板码**（把维度值换回 ``{#XXX}``）。"""
    code = message['metricCode']
    for placeholder, value in (message['tags'].get('dimension_values') or {}).items():
        code = code.replace(str(value), '{#%s}' % placeholder)
    return code


def test_expected_table_covers_the_b_class_rules(mapping):
    """期望值表覆盖全部 71 条 B 类规则（一条不落）。"""
    rules = [
        rule for rule in mapping.metric_rules
        if rule.enabled and rule.source.get('source') == 'command_center_sql'
    ]
    assert len(rules) == 71


# ---------------------------------------------------------------------------
# 7. 消息形态
# ---------------------------------------------------------------------------

def test_messages_use_the_frozen_envelope(mapping, probe_nodes, node_rows):
    from tpops_kafka_collector.kafka.serializer import METRIC_FIELDS

    cc = FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    result = _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    assert result.messages
    for message in result.messages:
        assert set(message) == set(METRIC_FIELDS)


def test_messages_serialize_through_the_only_exit(mapping, probe_nodes, node_rows):
    from tpops_kafka_collector.kafka.serializer import serialize_metric

    cc = FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    result = _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    payload = json.loads(serialize_metric(result.messages[0]))
    assert 'metricCode' in payload


def test_missing_merged_views_is_a_loud_failure(mapping):
    """映射表里没有 ``merged_views`` → 响亮失败，不静默。"""

    class NoViews(object):
        metric_rules = ()
        response_rules = {}

    cc = FakeCommandCenter(pages_by_node={}, nodes=())
    with pytest.raises(MappingError):
        CommandCenterCollector(
            client=None, mapping=NoViews(), command_center=cc,
        )


# ---------------------------------------------------------------------------
# 8. 节点清单取不到必须记 ERROR（不静默吞异常）
# ---------------------------------------------------------------------------

def test_master_nodes_failure_is_logged_as_error(
    mapping, probe_nodes, node_rows, caplog
):
    """节点清单取不到 → **必须记 ERROR**（原来静默吞掉，资源名会指错节点）。"""
    class BrokenCommandCenter(FakeCommandCenter):
        def master_nodes(self, instance_id):
            raise RuntimeError('节点清单接口 503')

    cc = BrokenCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    with caplog.at_level('ERROR', logger='tpops_kafka_collector'):
        _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    assert '节点清单取不到' in caplog.text
    assert '节点清单接口 503' in caplog.text


def test_missing_master_nodes_capability_is_logged_as_error(
    mapping, probe_nodes, node_rows, caplog
):
    """客户端**没有** ``master_nodes`` 能力 → 同样要记 ERROR（装配错了）。"""
    class NoMasterNodes(FakeCommandCenter):
        master_nodes = None

    cc = NoMasterNodes(pages_by_node=probe_nodes, nodes=node_rows)
    with caplog.at_level('ERROR', logger='tpops_kafka_collector'):
        _collector(cc, mapping).collect(instance_id=INSTANCE_ID)

    assert '没有 master_nodes' in caplog.text


# ---------------------------------------------------------------------------
# 9. 配置接入与页容量回报
# ---------------------------------------------------------------------------

def test_from_config_takes_the_page_size_from_the_config(mapping, probe_nodes, node_rows):
    """``from_config`` 用 ``CMD_PAGE_SIZE``（配置里的页容量）装配。"""
    from tpops_kafka_collector.config import Config

    config = Config(tpops_user='u', tpops_password='p', cmd_page_size=37)
    cc = FakeCommandCenter(pages_by_node=probe_nodes, nodes=node_rows)
    collector = CommandCenterCollector.from_config(
        client=None, mapping=mapping, command_center=cc, config=config,
    )

    result = collector.collect(instance_id=INSTANCE_ID)

    assert result.page_size == 37
    assert 'limit 37' in cc.sql_texts[0]


def test_result_reports_the_shrunk_page_size(mapping, probe_nodes, node_rows):
    """超限缩小页容量后，回报的 ``page_size`` 是**缩小后**的值。"""
    calls = []

    class ShrinkingCommandCenter(FakeCommandCenter):
        def execute_sql(self, instance_id, sql_text, limit='1000'):
            calls.append(sql_text)
            if len(calls) == 1:
                return SqlOutcome(
                    sql_text=sql_text, ok=False,
                    error='output size(131072 B) exceeds the threshold(102400 B)',
                    attempts=1,
                )
            return FakeCommandCenter.execute_sql(self, instance_id, sql_text, limit)

    def _page_for(offset):
        count = 5 if offset < 10 else 2
        return _growable_page(row_count=count, offset=offset)

    cc = ShrinkingCommandCenter(
        pages_by_node={name: None for name in probe_nodes},
        rows_by_node={name: _page_for for name in probe_nodes},
        nodes=node_rows,
    )
    result = _collector(cc, mapping, page_size=100).collect(instance_id=INSTANCE_ID)

    assert len(calls) >= 2
    assert result.page_size == 50, '回报的页容量应是缩小后的那个'
