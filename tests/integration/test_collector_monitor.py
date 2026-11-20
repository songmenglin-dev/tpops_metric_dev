"""A 类采集器（监控指标接口）的集成测试。

TPOPS 一律 mock（平台不可达），但**用实测留档回放**：
``data/metric_response/001_*.json`` 就是平台原生响应体。

守的是几条「实测踩过的坑」与规格要求：

- **多 series 逐组件产出** —— 平台按组件给出多条 series，客户指标是实例级；
  现状口径（``open_items.json`` / ``a-class-multi-series``，**待客户确认**）是
  **逐组件各出一条消息**，SHALL NOT 自行求和。
- **维度键 ``COMPONENT_ID`` 归入 tags** —— 不是信封字段（信封只有 7 个字段）。
- **空分组必须记告警日志** —— 分组码写错平台返回 200 + 空 series 且不报错。
- **缺值不产出假数据** —— null 点跳过，不出 0；取不到值就不产出那条消息。
- **节点身份换算** —— series 键是 ``component_id``（``cn_5002``）或
  ``ip-component_id``（``172.16.103.126-dn_6001``），经节点清单换算成
  ``{host_ip}_{node_id}``；换算不到落到第一个 master CN。
- **查询窗口 ≥ 1 小时 / end_time 回退 5 分钟 / ``is_latest_point_only`` 是
  字符串 ``"false"``** —— 这三条由 ``tpops/metrics.py`` 负责，采集器只复用。
"""

import json
import logging
import os

import pytest

from tpops_kafka_collector.collectors.monitor import MonitorCollector
from tpops_kafka_collector.config import Config
from tpops_kafka_collector.errors import DecodeError
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.tpops.metrics import (
    METRIC_GROUPS,
    METRICS_PATH,
    MetricWindowError,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SAMPLE_DIR = os.path.join(REPO_ROOT, 'data', 'metric_response')

#: 目标实例（分布式版 gauss-6374，3 个 CN 均为 master）。
INSTANCE_ID = '83cd68f2e4cb40b4b7e69240479ffa26in14'

#: 采集时刻（毫秒）。
NOW_MS = 1794454239320
#: 窗口终点 = 采集时刻 − 5 分钟回退。
END_MS = NOW_MS - 5 * 60 * 1000

#: A 类 11 个客户指标用到的平台指标名（7 个）。
PLATFORM_METRICS = (
    'rds092_gaussv5_commit_counter',
    'rds093_gaussv5_rollback_counter',
    'rds400_gaussv5_total_counter',
    'rds402_gaussv5_ddl_dml_dcl_count',
    'rds133_cn_startup_time',
    'gaussdb_instance_status',
    'datanode_switch_status',
)

#: 节点清单（key-view-execute-node 的返回形态）。
NODES = (
    {'node_id': 'node-cn-1', 'node_name': 'gauss-6374_cn-1', 'role': 'master',
     'type': 'CN', 'component_id': 'cn_5001', 'node_ip': '172.16.103.126'},
    {'node_id': 'node-cn-2', 'node_name': 'gauss-6374_cn-2', 'role': 'master',
     'type': 'CN', 'component_id': 'cn_5002', 'node_ip': '172.16.103.127'},
    {'node_id': 'node-cn-3', 'node_name': 'gauss-6374_cn-3', 'role': 'master',
     'type': 'CN', 'component_id': 'cn_5003', 'node_ip': '172.16.103.128'},
    {'node_id': 'node-dn-1', 'node_name': 'gauss-6374_dn-1', 'role': 'master',
     'type': 'DN', 'component_id': 'dn_6001', 'node_ip': '172.16.103.126'},
    {'node_id': 'node-dn-2', 'node_name': 'gauss-6374_dn-2', 'role': 'master',
     'type': 'DN', 'component_id': 'dn_6002', 'node_ip': '172.16.103.127'},
)

#: 客户指标表【数据类型】为整数的 A 类指标（用于挑消息）。
COMMIT_METRIC = 'GAUSSDB.Number_of_ansactions_mmitted_r_cond'


def default_catalog():
    """12 个分组码的目录（每个分组都有指标，避免干扰其它用例）。"""
    catalog = dict((group, [{'name': f'dummy_{group}'}]) for group in METRIC_GROUPS)
    catalog['TRANSACTION'] = [
        {'name': 'rds092_gaussv5_commit_counter'},
        {'name': 'rds093_gaussv5_rollback_counter'},
    ]
    catalog['COREMETRICS'] = [
        {'name': 'rds400_gaussv5_total_counter'},
        {'name': 'rds402_gaussv5_ddl_dml_dcl_count'},
    ]
    catalog['INSTATUS'] = [
        {'name': 'rds133_cn_startup_time'},
        {'name': 'gaussdb_instance_status'},
    ]
    catalog['FORDR'] = [{'name': 'datanode_switch_status'}]
    return catalog


class FakeClient:
    """TPOPS 客户端替身：目录 GET + 时序 POST。"""

    def __init__(self, payload=None, catalog=None):
        self.payload = payload if payload is not None else {}
        self.catalog = default_catalog() if catalog is None else catalog
        self.gets = []
        self.posts = []

    def get(self, path, params=None):
        self.gets.append((path, dict(params or {})))
        group = (params or {}).get('name')
        return {'series': [{'name': group, 'metrics': list(self.catalog.get(group, []))}]}

    def post(self, path, payload=None):
        self.posts.append((path, dict(payload or {})))
        return self.payload


def monitor_payload(metrics):
    """拼一个监控指标响应体（``metric_infos → metrics``）。"""
    return {
        'metric_infos': [
            {'instance_id': INSTANCE_ID, 'time_interval': 60, 'metrics': list(metrics)},
        ],
    }


def commit_metric(series, timestamps=None, frequency=60):
    """``rds092``（每秒提交事务数）的平台指标条目。"""
    return {
        'name': 'rds092_gaussv5_commit_counter',
        'metric_name': '用户提交事务数/秒',
        'collection_frequency': frequency,
        'timestamps': list(timestamps if timestamps is not None else [END_MS]),
        'series': dict(series),
    }


@pytest.fixture(scope='module')
def mapping():
    """真实映射表（包内副本，不做启动期校验 —— 单测只关心取数路径）。"""
    return load_mapping(validate=False)


@pytest.fixture
def config():
    return Config(tpops_user='u', tpops_password='p',
                  metric_window_hours=2, metric_end_lag_minutes=5)


def build_collector(client, mapping, config, nodes=NODES, instance_name='gauss-6374'):
    return MonitorCollector(
        client=client,
        mapping=mapping,
        instance_id=INSTANCE_ID,
        instance_name=instance_name,
        nodes=nodes,
        config=config,
        clock=lambda: NOW_MS,
    )


def messages_of(messages, metric_code):
    return [m for m in messages if m['metricCode'] == metric_code]


# ---------------------------------------------------------------------------
# 多 series：逐组件各出一条消息
# ---------------------------------------------------------------------------

def test_one_message_per_component(mapping, config):
    """3 条组件 series → 3 条消息，各带自己的 component_id。"""
    client = FakeClient(monitor_payload([
        commit_metric({'cn_5001': [9.0], 'cn_5002': [11.0], 'cn_5003': [11.0]}),
    ]))

    messages = build_collector(client, mapping, config).collect()

    commits = messages_of(messages, COMMIT_METRIC)
    assert len(commits) == 3
    assert sorted(m['tags']['component_id'] for m in commits) == \
        ['cn_5001', 'cn_5002', 'cn_5003']
    assert sorted(m['value'] for m in commits) == [9, 11, 11]


def test_component_id_goes_into_tags_not_envelope(mapping, config):
    """维度键归入 tags；信封只有 7 个字段。"""
    client = FakeClient(monitor_payload([
        commit_metric({'cn_5001': [9.0]}),
    ]))

    message = messages_of(build_collector(client, mapping, config).collect(), COMMIT_METRIC)[0]

    assert set(message) == {
        'metricName', 'metricCode', 'resourceName', 'value', 'unit', 'clock', 'tags',
    }
    assert message['tags']['component_id'] == 'cn_5001'
    # COMPONENT_ID 是平台序列键 —— 不是客户维度，不进 dimension_values
    assert 'dimension_values' not in message['tags']
    assert 'COMPONENT_ID' not in message['tags']


def test_value_key_series_produces_instance_level_message(mapping, config):
    """``gaussdb_instance_status`` 的 series 键是 ``value``（实例级）。"""
    client = FakeClient(monitor_payload([
        {
            'name': 'gaussdb_instance_status',
            'metric_name': '实例状态',
            'collection_frequency': 30,
            'timestamps': [END_MS],
            'series': {'value': [1.0]},
        },
    ]))

    messages = build_collector(client, mapping, config).collect()

    status = messages_of(messages, 'GAUSSDB.Service_availability_testing')
    assert len(status) == 1
    assert status[0]['value'] == '正常'


# ---------------------------------------------------------------------------
# 缺值不产出假数据
# ---------------------------------------------------------------------------

def test_null_point_is_skipped_not_zero(mapping, config):
    """null 点跳过 —— 不产出 0。"""
    client = FakeClient(monitor_payload([
        commit_metric({'cn_5001': [None]}),
    ]))

    messages = build_collector(client, mapping, config).collect()

    assert messages_of(messages, COMMIT_METRIC) == []


def test_null_tail_keeps_previous_non_null_value(mapping, config):
    """尾部是 null 时取窗口内最后一个非空点，不取 0。"""
    client = FakeClient(monitor_payload([
        commit_metric({'cn_5001': [5.0, None]}, timestamps=[END_MS - 60000, END_MS]),
    ]))

    messages = build_collector(client, mapping, config).collect()

    commits = messages_of(messages, COMMIT_METRIC)
    assert len(commits) == 1
    assert commits[0]['value'] == 5


def test_component_without_value_is_not_produced(mapping, config):
    """一个组件没值 → 只产出有值的组件，不补 0。"""
    client = FakeClient(monitor_payload([
        commit_metric({'cn_5001': [9.0], 'cn_5002': [None]}),
    ]))

    messages = build_collector(client, mapping, config).collect()

    commits = messages_of(messages, COMMIT_METRIC)
    assert len(commits) == 1
    assert commits[0]['tags']['component_id'] == 'cn_5001'


def test_points_outside_the_tier_are_not_used(mapping, config):
    """按档位切片 —— 档位窗口里的点全是 null 就不产出（更早的点也不顶替）。"""
    client = FakeClient(monitor_payload([
        commit_metric(
            {'cn_5001': [100.0, None, None]},
            timestamps=[END_MS - 3600000, END_MS - 60000, END_MS],
        ),
    ]))

    messages = build_collector(client, mapping, config).collect()

    assert messages_of(messages, COMMIT_METRIC) == []


def test_coarse_platform_timestamps_fall_back_to_latest_point(mapping, config, caplog):
    """平台时间戳粒度粗于采集周期（实测如此）→ 回退到窗口内最新点并记告警。"""
    client = FakeClient(monitor_payload([
        commit_metric({'cn_5001': [42.0]}, timestamps=[END_MS - 3600000]),
    ]))

    with caplog.at_level(logging.WARNING, logger='tpops_kafka_collector.collectors.monitor'):
        messages = build_collector(client, mapping, config).collect()

    commits = messages_of(messages, COMMIT_METRIC)
    assert len(commits) == 1
    assert commits[0]['value'] == 42
    assert '采集周期' in caplog.text


# ---------------------------------------------------------------------------
# 空分组告警
# ---------------------------------------------------------------------------

def test_empty_group_logs_warning(mapping, config, caplog):
    """分组码写错 → 200 + 空 series 且不报错 → 必须记告警日志。"""
    client = FakeClient(monitor_payload([commit_metric({'cn_5001': [9.0]})]),
                        catalog={})

    with caplog.at_level(logging.WARNING, logger='tpops_kafka_collector.collectors.monitor'):
        build_collector(client, mapping, config).collect()

    assert 'CPUMEMORY' in caplog.text
    assert '空分组' in caplog.text


def test_empty_payload_logs_and_returns_nothing(mapping, config, caplog):
    """响应体里没有指标 → 记告警日志，不静默返回空列表。"""
    client = FakeClient(monitor_payload([]))

    with caplog.at_level(logging.WARNING, logger='tpops_kafka_collector.collectors.monitor'):
        messages = build_collector(client, mapping, config).collect()

    assert messages == ()
    assert 'GAUSSDB.Number_of_ansactions_mmitted_r_cond' in caplog.text


# ---------------------------------------------------------------------------
# 请求参数（窗口 / end_time 回退 / is_latest_point_only）
# ---------------------------------------------------------------------------

def test_query_body_uses_string_false_and_rolled_back_end(mapping, config):
    client = FakeClient(monitor_payload([commit_metric({'cn_5001': [9.0]})]))

    build_collector(client, mapping, config).collect()

    path, body = client.posts[0]
    assert path == METRICS_PATH
    assert body['is_latest_point_only'] == 'false'
    assert isinstance(body['is_latest_point_only'], str)
    assert body['end_time'] == END_MS
    assert body['start_time'] == END_MS - 2 * 3600 * 1000
    assert body['end_time'] - body['start_time'] >= 3600 * 1000
    assert body['instances'] == [{'instance_id': INSTANCE_ID}]
    assert 'rds092_gaussv5_commit_counter' in body['metric_names']


def test_window_below_one_hour_fails_fast(mapping):
    """窗口不足 1 小时 → 报错，不发请求（窗口过短采不到 3600 秒档的指标）。"""
    client = FakeClient(monitor_payload([commit_metric({'cn_5001': [9.0]})]))
    short = Config(tpops_user='u', tpops_password='p',
                   metric_window_hours=0, metric_end_lag_minutes=5)

    with pytest.raises(MetricWindowError):
        build_collector(client, mapping, short).collect()

    assert client.posts == []


def test_catalog_covers_all_twelve_group_codes(mapping, config):
    """12 个分组码必须硬编码并全部查询 —— 少一个就少一批指标且不报错。"""
    client = FakeClient(monitor_payload([commit_metric({'cn_5001': [9.0]})]))

    build_collector(client, mapping, config).collect()

    groups = [params['name'] for _, params in client.gets]
    assert groups == list(METRIC_GROUPS)
    assert len(groups) == 12


# ---------------------------------------------------------------------------
# 节点身份换算
# ---------------------------------------------------------------------------

def test_node_identity_from_component_id(mapping, config):
    """series 键 ``cn_5002`` → ``{host_ip}_{node_id}``。"""
    client = FakeClient(monitor_payload([
        commit_metric({'cn_5002': [11.0]}),
    ]))

    message = messages_of(build_collector(client, mapping, config).collect(), COMMIT_METRIC)[0]

    assert message['resourceName'] == '172.16.103.127_node-cn-2'
    assert message['tags']['node_id'] == 'node-cn-2'
    assert message['tags']['host_ip'] == '172.16.103.127'


def test_node_identity_from_ip_component_series_key(mapping, config):
    """series 键 ``ip-component_id``（DN）→ 同样换算成节点身份。"""
    client = FakeClient(monitor_payload([
        {
            'name': 'datanode_switch_status',
            'metric_name': 'DN主备状态',
            'collection_frequency': 30,
            'timestamps': [END_MS],
            'series': {'172.16.103.126-dn_6001': [1.0]},
        },
    ]))

    messages = build_collector(client, mapping, config).collect()

    standby = messages_of(messages, 'GAUSSDB.Standby_status')
    assert len(standby) == 1
    assert standby[0]['resourceName'] == '172.16.103.126_node-dn-1'
    assert standby[0]['tags']['component_id'] == '172.16.103.126-dn_6001'


def test_unresolvable_component_falls_back_to_master_cn(mapping, config):
    """换算不到节点 → 落到节点清单里的第一个 master CN（跨轮稳定）。"""
    client = FakeClient(monitor_payload([
        commit_metric({'cn_9999': [9.0]}),
    ]))

    message = messages_of(build_collector(client, mapping, config).collect(), COMMIT_METRIC)[0]

    assert message['resourceName'] == '172.16.103.126_node-cn-1'


def test_instance_level_metric_falls_back_to_master_cn(mapping, config):
    """实例级指标（``value`` 键）→ 第一个 master CN。"""
    client = FakeClient(monitor_payload([
        {
            'name': 'gaussdb_instance_status',
            'metric_name': '实例状态',
            'collection_frequency': 30,
            'timestamps': [END_MS],
            'series': {'value': [1.0]},
        },
    ]))

    messages = build_collector(client, mapping, config).collect()

    assert messages_of(messages, 'gaussdb.Status')[0]['resourceName'] == \
        '172.16.103.126_node-cn-1'


# ---------------------------------------------------------------------------
# 留档回放
# ---------------------------------------------------------------------------

def test_replay_archived_response(mapping, config):
    """用实测留档（原生响应体）回放 —— 3 条 CN series → 3 条消息。"""
    path = os.path.join(SAMPLE_DIR, f'001_{COMMIT_METRIC}.json')
    if not os.path.isfile(path):
        pytest.skip(f'实测留档不在工作区：{path}')
    with open(path, encoding='utf-8') as handle:
        records = json.load(handle)

    body = None
    for record in records:
        request = (record.get('request') or {}).get('body') or {}
        if request.get('is_latest_point_only') == 'false':
            body = record.get('response_body')
            break
    assert body is not None

    client = FakeClient(body)
    messages = build_collector(client, mapping, config).collect()

    commits = messages_of(messages, COMMIT_METRIC)
    assert len(commits) == 3
    assert sorted(m['tags']['component_id'] for m in commits) == \
        ['cn_5001', 'cn_5002', 'cn_5003']


# ---------------------------------------------------------------------------
# 规则异常不被静默吞掉
# ---------------------------------------------------------------------------

def test_missing_metric_in_response_does_not_break_the_cycle(mapping, config):
    """某个平台指标没返回 → 记告警并跳过，其余指标照常产出。"""
    client = FakeClient(monitor_payload([
        commit_metric({'cn_5001': [9.0]}),
    ]))

    messages = build_collector(client, mapping, config).collect()

    assert len(messages_of(messages, COMMIT_METRIC)) == 1
    assert messages_of(messages, 'gaussdb.Tps') == []


def test_collector_does_not_touch_watermark(mapping, config):
    """采集器不推进水位 —— 那是调度器的职责。"""
    client = FakeClient(monitor_payload([commit_metric({'cn_5001': [9.0]})]))

    collector = build_collector(client, mapping, config)
    assert not hasattr(collector, 'watermark')
    assert not hasattr(collector, 'advance')


def test_decode_error_is_not_raised_for_absent_metric(mapping, config):
    """响应体里没有该指标时抛的是 DecodeError，采集器要接住并记日志。"""
    with pytest.raises(DecodeError):
        # 直接调映射层确认异常类型（采集器内部已捕获）
        from tpops_kafka_collector.mapping.decode import decode
        decode({'metric_infos': []}, {'kind': 'series_from_metric_infos'},
               {'metric': 'rds092_gaussv5_commit_counter'}, NOW_MS)
