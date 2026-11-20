"""E 类：告警拉取与归并（任务 2.9）。

**TPOPS 一律 mock** —— 用假客户端按平台语义分页回放；
``data/alarm/`` 下有真实抓取产物，用作回归样本回放。

守住七件事：

1. **分页取全** —— ``limit ≤ 100`` 必须翻页，否则静默丢数据；
2. **四种组合** —— 活动/历史 × 收敛/明细，各取各的；
3. **本地归并** —— 明细按 ``alarm_id + instance_id`` 并进收敛条目的 ``details``；
4. **收敛形态不依赖平台侧过滤** —— ``alarm_id`` / ``instance_id`` 只在明细形态有效；
5. **节点身份回填** —— 回填不到落到第一个 master CN；
6. **``details`` 恒为数组** —— 缺失时输出 ``[]``，不省略字段；
7. **30 天超期显式告警** —— 历史告警缺口必须记 WARNING 日志。
"""

import json
import logging
import os

import pytest

from tpops_kafka_collector.collectors.alarm import (
    collect_alarms,
)
from tpops_kafka_collector.ids import alarm_idempotency_key
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.mapping.table import MappingTable
from tpops_kafka_collector.tpops.alarms import (
    ALARM_RECORDS_PATH,
    MAX_LIMIT,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ALARM_SAMPLE_DIR = os.path.join(REPO_ROOT, 'data', 'alarm')

TARGET_INSTANCE = '83cd68f2e4cb40b4b7e69240479ffa26in14'
OTHER_INSTANCE = 'c35b8d078cae4a9eb05d616ed5cf032fin14'

MASTER_NODE = {
    'node_id': '3881c04403394ee1b36a2bf873e69007no14',
    'node_name': 'gauss-6374_gaussdbv5_hcs2-0_0',
    'role': 'master',
    'type': 'CN',
    'component_id': 'cn_5001',
    'node_ip': '172.16.103.126',
}
SECOND_MASTER_NODE = {
    'node_id': '40abc563810e462a97b065e718189a59no14',
    'node_name': 'gauss-6374_gaussdbv5_hcs2-0_1',
    'role': 'master',
    'type': 'CN',
    'component_id': 'cn_5002',
    'node_ip': '172.16.103.127',
}


# ---------------------------------------------------------------------------
# 假客户端：按平台语义分页
# ---------------------------------------------------------------------------

class FakeAlarmClient:
    """假 TPOPS 客户端：按 ``offset`` / ``limit`` 分页回放。

    ``pages`` 的键是 ``(is_active, is_convergence)``，值是该组合下的记录列表
    （平台返回什么就回什么，``total_count`` 取列表长度）。
    """

    def __init__(self, pages=None):
        self._pages = dict(pages or {})
        self.calls = []

    def post(self, path, payload=None):
        body = dict(payload or {})
        self.calls.append((path, body))
        assert path == ALARM_RECORDS_PATH, '告警记录接口路径不对：{}'.format(path)
        key = (bool(body.get('is_active')), bool(body.get('is_convergence')))
        records = list(self._pages.get(key, ()))
        offset = int(body.get('offset', 0))
        limit = int(body.get('limit', MAX_LIMIT))
        assert limit <= MAX_LIMIT, 'limit 不能超过平台上限'
        page = records[offset:offset + limit]
        return {'records': page, 'total_count': len(records)}

    def bodies(self):
        return [body for _, body in self.calls]


def convergence_row(alarm_id='2007010001', instance_id=TARGET_INSTANCE, **overrides):
    """一条收敛条目（平台形态：无节点身份）。"""
    row = {
        'severity': 'major',
        'status': 'activated',
        'cause': '',
        'engine': 'gaussdb',
        'alarm_id': alarm_id,
        'alarm_name': '节点NTP状态异常',
        'alarm_category': '节点状态',
        'instance_id': instance_id,
        'instance_name': 'gauss-6374',
        'alarm_items_sum': 1,
        'alarm_type': 'event',
        'first_alarm_time': 1793758083000,
        'last_alarm_time': 1793771156000,
        'alarm_duration': '4小时5分钟',
        'component_name': '实例管理',
    }
    row.update(overrides)
    return row


def detail_row(alarm_id='2007010001', instance_id=TARGET_INSTANCE, node=None, **overrides):
    """一条明细行（平台形态：带节点身份）。"""
    node = node or MASTER_NODE
    row = {
        'id': 'detail-{}'.format(node['node_id'][-6:]),
        'severity': 'major',
        'description': '{}节点NTP服务状态异常。'.format(node['node_ip']),
        'status': 'activated',
        'cause': '',
        'engine': 'gaussdb',
        'alarm_id': alarm_id,
        'alarm_name': '节点NTP状态异常',
        'resource_id': '{}-{}-nodeNtpStatusAbnormal'.format(instance_id, node['node_id']),
        'alarm_category': '节点状态',
        'instance_id': instance_id,
        'node_id': node['node_id'],
        'instance_name': 'gauss-6374',
        'host_ip': node['node_ip'],
        'alarm_detail_raise_count': 5,
        'alarm_type': 'event',
        'first_alarm_time': 1793758083000,
        'last_alarm_time': 1793771156000,
        'alarm_duration': '4小时5分钟',
        'component_name': '实例管理',
        'impaction': '节点NTP状态异常会导致实例无法正确进行时间同步。',
        'possible_causes': '1、NTP客户端和服务端的网络连接有问题。',
        'processing_steps': '1.登录告警对象节点。',
        'notify_status': 'disabled',
        'automatic_clear': True,
    }
    row.update(overrides)
    return row


@pytest.fixture(scope='module')
def mapping():
    bundle = load_mapping()
    return MappingTable(bundle.field_map, bundle.metric_rules, bundle.alarm_rules, {})


def _real_alarm_ids(count):
    """告警定义目录里的真实 alarm_id（避免兜底定义的 WARNING 噪声）。"""
    bundle = load_mapping()
    ids = sorted(bundle.alarm_rules.get('definitions', {}).keys())
    while len(ids) < count:
        ids.append(ids[0])
    return ids[:count]


def _collect(client, mapping, **kwargs):
    params = {
        'nodes': [MASTER_NODE, SECOND_MASTER_NODE],
        'instance_id': TARGET_INSTANCE,
    }
    params.update(kwargs)
    return collect_alarms(client, mapping, **params)


# ---------------------------------------------------------------------------
# 1. 分页取全
# ---------------------------------------------------------------------------

def test_pagination_fetches_every_page(mapping):
    """total_count=250 时必须翻页取全 250 条（只取第一页是静默丢数据）。"""
    rows = [convergence_row(alarm_id=alarm_id) for alarm_id in _real_alarm_ids(250)]
    client = FakeAlarmClient({(True, True): rows})

    result = _collect(client, mapping)

    assert len(result.messages) == 250
    offsets = [body.get('offset') for body in client.bodies()
               if body['is_convergence'] and body['is_active']]
    assert offsets == [0, 100, 200], '必须按 limit=100 翻三页'
    assert all(body['limit'] <= MAX_LIMIT for body in client.bodies())


def test_pagination_stops_when_total_reached(mapping):
    """少于一页时只发一次请求。"""
    client = FakeAlarmClient({(True, True): [convergence_row()]})

    result = _collect(client, mapping)

    assert len(result.messages) == 1
    pages = [b for b in client.bodies() if b['is_convergence'] and b['is_active']]
    assert len(pages) == 1


# ---------------------------------------------------------------------------
# 2. 四种组合
# ---------------------------------------------------------------------------

def test_all_four_combinations_are_queried(mapping):
    client = FakeAlarmClient({
        (True, True): [convergence_row()],
        (True, False): [detail_row()],
        (False, True): [convergence_row(status='recovered')],
        (False, False): [detail_row(status='recovered')],
    })

    result = _collect(client, mapping)

    combos = set((b['is_active'], b['is_convergence']) for b in client.bodies())
    assert combos == {(True, True), (True, False), (False, True), (False, False)}
    assert len(result.messages) == 2, '活动 1 条 + 历史 1 条'


def test_all_four_combinations_are_always_queried(mapping):
    """四种组合恒定各拉一次 —— 活动/历史 × 收敛/明细（规格要求）。"""
    client = FakeAlarmClient({})

    _collect(client, mapping)

    combos = [(b['is_active'], b['is_convergence']) for b in client.bodies()]
    assert combos.count((True, True)) == 1
    assert combos.count((True, False)) == 1
    assert combos.count((False, True)) == 1
    assert combos.count((False, False)) == 1


# ---------------------------------------------------------------------------
# 3. 本地归并
# ---------------------------------------------------------------------------

def test_details_are_merged_by_alarm_id_and_instance(mapping):
    """同一 alarm_id + instance_id 的 3 条明细 → 1 条消息，details 含 3 项。"""
    details = [
        detail_row(node=MASTER_NODE),
        detail_row(node=SECOND_MASTER_NODE),
        detail_row(node={'node_id': '5a3cd027ad5c4f9bbb67d728c6d279c0no14',
                         'node_ip': '172.16.103.128',
                         'component_id': 'cn_5003'}),
    ]
    client = FakeAlarmClient({
        (True, True): [convergence_row()],
        (True, False): details,
    })

    result = _collect(client, mapping)

    assert len(result.messages) == 1
    message = result.messages[0].message
    assert len(message['tags']['details']) == 3
    assert {d['node_id'] for d in message['tags']['details']} == {
        d['node_id'] for d in details
    }


def test_details_of_other_instance_are_not_merged(mapping):
    """归并键含 instance_id —— 另一个实例的同名告警不并进来。"""
    client = FakeAlarmClient({
        (True, True): [convergence_row(instance_id=TARGET_INSTANCE)],
        (True, False): [detail_row(instance_id=OTHER_INSTANCE)],
    })

    result = _collect(client, mapping)

    assert len(result.messages) == 1
    assert result.messages[0].message['tags']['details'] == []


# ---------------------------------------------------------------------------
# 4. 收敛形态不依赖平台侧过滤
# ---------------------------------------------------------------------------

def test_convergence_request_carries_no_platform_filters(mapping):
    """收敛形态下平台侧 alarm_id / instance_id 过滤无效 —— 请求里不传。"""
    client = FakeAlarmClient({(True, True): [convergence_row()]})

    _collect(client, mapping)

    for body in client.bodies():
        if body['is_convergence']:
            assert 'alarm_id' not in body
            assert 'instance_id' not in body


def test_convergence_rows_are_filtered_locally(mapping):
    """平台侧过滤在收敛形态下无效 —— 实例过滤必须本地做。"""
    client = FakeAlarmClient({
        (True, True): [
            convergence_row(instance_id=TARGET_INSTANCE),
            convergence_row(instance_id=OTHER_INSTANCE),
        ],
    })

    result = _collect(client, mapping)

    assert len(result.messages) == 1
    assert result.messages[0].message['tags']['instance_id'] == TARGET_INSTANCE


# ---------------------------------------------------------------------------
# 5. 节点身份回填
# ---------------------------------------------------------------------------

def test_node_is_backfilled_from_details(mapping):
    client = FakeAlarmClient({
        (True, True): [convergence_row()],
        (True, False): [detail_row(node=SECOND_MASTER_NODE)],
    })

    result = _collect(client, mapping)

    message = result.messages[0].message
    assert message['resourceName'] == '{}_{}'.format(
        SECOND_MASTER_NODE['node_ip'], SECOND_MASTER_NODE['node_id'])
    assert message['tags']['node_id'] == SECOND_MASTER_NODE['node_id']
    assert message['tags']['host_ip'] == SECOND_MASTER_NODE['node_ip']


def test_multi_node_alarm_falls_back_to_first_master_cn(mapping):
    """一条告警命中多个节点 → 回填不了 → 落第一个 master CN（固定节点）。"""
    client = FakeAlarmClient({
        (True, True): [convergence_row()],
        (True, False): [
            detail_row(node=MASTER_NODE),
            detail_row(node=SECOND_MASTER_NODE),
        ],
    })

    result = _collect(client, mapping)

    message = result.messages[0].message
    assert message['resourceName'] == '{}_{}'.format(
        MASTER_NODE['node_ip'], MASTER_NODE['node_id'])


def test_alarm_without_details_falls_back_to_first_master_cn(mapping):
    client = FakeAlarmClient({(True, True): [convergence_row()]})

    result = _collect(client, mapping)

    message = result.messages[0].message
    assert message['resourceName'] == '{}_{}'.format(
        MASTER_NODE['node_ip'], MASTER_NODE['node_id'])


# ---------------------------------------------------------------------------
# 6. details 恒为数组
# ---------------------------------------------------------------------------

def test_details_is_always_a_list_even_when_empty(mapping):
    client = FakeAlarmClient({(True, True): [convergence_row()]})

    result = _collect(client, mapping)

    message = result.messages[0].message
    assert 'details' in message['tags']
    assert message['tags']['details'] == []


def test_details_is_always_a_list_when_present(mapping):
    client = FakeAlarmClient({
        (True, True): [convergence_row()],
        (True, False): [detail_row()],
    })

    result = _collect(client, mapping)

    assert result.messages[0].message['tags']['details'] != []


# ---------------------------------------------------------------------------
# 7. 水位增量
# ---------------------------------------------------------------------------

def test_only_alarms_newer_than_watermark_are_returned(mapping):
    client = FakeAlarmClient({
        (True, True): [
            convergence_row(alarm_id='old', first_alarm_time=1000, last_alarm_time=1000),
            convergence_row(alarm_id='new', first_alarm_time=5000, last_alarm_time=5000),
        ],
    })

    result = _collect(client, mapping, watermark=2000)

    assert [message.alarm_id for message in result.messages] == ['new']


def test_watermark_none_returns_everything(mapping):
    client = FakeAlarmClient({(True, True): [convergence_row()]})

    result = _collect(client, mapping, watermark=None)

    assert len(result.messages) == 1


# ---------------------------------------------------------------------------
# 8. 30 天超期显式告警
# ---------------------------------------------------------------------------

def test_history_gap_beyond_retention_logs_warning(mapping, caplog):
    """历史告警最新时间距今超过保留窗口 → WARNING 日志（不静默）。"""
    now_ms = 1793771169000
    old = now_ms - 31 * 24 * 3600 * 1000
    client = FakeAlarmClient({
        (False, True): [convergence_row(status='recovered',
                                        first_alarm_time=old, last_alarm_time=old)],
    })

    with caplog.at_level(logging.WARNING):
        _collect(client, mapping, now_ms=now_ms, retention_days=30)

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any('30' in text and '缺口' in text for text in warnings), warnings


def test_recent_history_logs_no_retention_warning(mapping, caplog):
    now_ms = 1793771169000
    client = FakeAlarmClient({
        (False, True): [convergence_row(status='recovered',
                                        first_alarm_time=now_ms - 3600 * 1000,
                                        last_alarm_time=now_ms - 3600 * 1000)],
    })

    with caplog.at_level(logging.WARNING):
        _collect(client, mapping, now_ms=now_ms, retention_days=30)

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert not any('缺口' in text for text in warnings), warnings


# ---------------------------------------------------------------------------
# 9. 幂等键
# ---------------------------------------------------------------------------

def test_idempotency_key_uses_platform_raw_status(mapping):
    client = FakeAlarmClient({
        (True, True): [convergence_row(status='activated')],
        (False, True): [convergence_row(status='recovered')],
    })

    result = _collect(client, mapping)

    keys = sorted(message.idempotency_key for message in result.messages)
    assert keys == sorted([
        alarm_idempotency_key('2007010001', TARGET_INSTANCE, 1793758083000, 'activated'),
        alarm_idempotency_key('2007010001', TARGET_INSTANCE, 1793758083000, 'recovered'),
    ])


# ---------------------------------------------------------------------------
# 10. 留档回放
# ---------------------------------------------------------------------------

def _load_sample(name):
    with open(os.path.join(ALARM_SAMPLE_DIR, name), encoding='utf-8') as handle:
        return json.load(handle)


def test_replay_archived_alarm_samples(mapping):
    """用 data/alarm/ 的真实抓取产物回放。"""
    active = _load_sample('active.json')
    active_detail = _load_sample('active_detail.json')
    history = _load_sample('history.json')
    history_detail = _load_sample('history_detail.json')
    client = FakeAlarmClient({
        (True, True): active['records'],
        (True, False): active_detail['records'],
        (False, True): history['records'],
        (False, False): history_detail['records'],
    })

    result = _collect(client, mapping)

    assert len(result.messages) == 2, '目标实例：活动 1 条 + 历史 1 条'
    active_message = result.messages[0].message
    assert active_message['alarmId'] == '2007010001'
    assert active_message['resourceName'] == '{}_{}'.format(
        '172.16.103.128', '5a3cd027ad5c4f9bbb67d728c6d279c0no14')
    assert len(active_message['tags']['details']) == 1
    history_message = result.messages[1].message
    assert history_message['status'] == 0
    assert len(history_message['tags']['details']) == 14
