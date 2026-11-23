"""C 类（诊断优化）采集器的集成测试。

TPOPS 与 Kafka 一律 mock：本文件不发起任何网络请求。

覆盖：

- 两个接口的**路径与请求体**（命名空间必须是 ``/rds/gaussdb/v3/``）；
- 「集合条数」（``collection_count``）；
- 「按 ``sql_text`` 前缀分类计数」（``text_prefix_count``）；
- Top N（``top_n``）与 ``sql_text`` 脱敏衔接；
- C 类指标与节点无关 → 落到节点清单里的第一个 master CN；
- 空结果集记告警日志；
- 缺值不产出消息。
"""
import logging
import os

import pytest

from tpops_kafka_collector.collectors.diagnostic import DiagnosticCollector
from tpops_kafka_collector.errors import MappingError, ProtocolError
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.tpops.diagnostic import (
    SLOW_SQL_COLLECTION,
    SLOW_SQL_PATH,
    SLOW_SQL_THRESHOLD_UNIT,
    SLOW_SQL_WINDOW_MS,
    TOP_SQL_COLLECTION,
    TOP_SQL_PATH,
    TOP_SQL_WINDOW_MS,
    build_slow_sql_body,
    build_top_sql_body,
    parse_slow_sql_rows,
    parse_top_sql_rows,
)

INSTANCE_ID = '83cd68f2e4cb40b4b7e69240479ffa26in14'

#: 节点清单（实测留档 ``data/metric_response/034_…`` 的前三条 CN）。
NODES = [
    {
        'node_id': '3881c04403394ee1b36a2bf873e69007no14',
        'node_name': 'gauss-6374_gaussdbv5_hcs2-0_0',
        'role': 'master',
        'type': 'CN',
        'component_id': 'cn_5001',
        'node_ip': '172.16.103.126',
    },
    {
        'node_id': '40abc563810e462a97b065e718189a59no14',
        'node_name': 'gauss-6374_gaussdbv5_hcs2-0_1',
        'role': 'master',
        'type': 'CN',
        'component_id': 'cn_5002',
        'node_ip': '172.16.103.127',
    },
    {
        'node_id': '5a3cd027ad5c4f9bbb67d728c6d279c0no14',
        'node_name': 'gauss-6374_gaussdbv5_hcs2-0_2',
        'role': 'master',
        'type': 'CN',
        'component_id': 'cn_5003',
        'node_ip': '172.16.103.128',
    },
]

#: 期望的资源名 —— 第一个 master CN 的「节点 IP + 节点 ID」。
EXPECTED_RESOURCE_NAME = '172.16.103.126_3881c04403394ee1b36a2bf873e69007no14'


class FakeClient:
    """记录请求并回放留档响应的假客户端。"""

    def __init__(self, slow_body=None, top_body=None):
        self._slow_body = slow_body
        self._top_body = top_body
        self.calls = []

    def post(self, path, payload=None):
        self.calls.append((path, payload))
        if path.endswith('/slow-sql-list'):
            return self._slow_body if self._slow_body is not None else {}
        if path.endswith('/top-sql-list'):
            return self._top_body if self._top_body is not None else {}
        raise AssertionError(f'未预期的请求路径：{path}')

    def get(self, path, params=None):  # pragma: no cover - C 类不用 GET
        raise AssertionError(f'C 类不应发起 GET 请求：{path}')


def _slow_row(sql_text, sql_id='1', node_id=NODES[0]['node_id']):
    return {
        'db_name': 'postgres',
        'schema_name': 'public',
        'sql_id': sql_id,
        'user_name': 'rdsAdmin',
        'sql_text': sql_text,
        'query_plan': '',
        'calls': '3',
        'avg_exec_time': '1200.5',
        'node_id': node_id,
        'node_name': NODES[0]['node_name'],
        'component_name': 'cn_5001',
    }


def _top_row(sql_id, total_elapse_time, sql_text='select 1'):
    return {
        'sql_id': sql_id,
        'user_name': 'rdsAdmin',
        'sql_text': sql_text,
        'calls_percent': '1.00',
        'cpu_percent': '0.10',
        'io_percent': '0.00',
        'calls': '12',
        'returned_rows': '12',
        'tuple_read': '0',
        'avg_elapse_time': '0.0057',
        'total_elapse_time': total_elapse_time,
        'cpu_time': '0.0583',
        'io_time': '0.0000',
        'min_elapse_time': '0.0045',
        'max_elapse_time': '0.0071',
        'sql_hit_ratio': '1',
        'node_id': NODES[0]['node_id'],
        'node_name': NODES[0]['node_name'],
        'db_name': 'postgres',
    }


def _collector(client, sanitizer=None):
    mapping = load_mapping()
    return DiagnosticCollector(
        client=client,
        mapping=mapping,
        sql_sanitizer=sanitizer if sanitizer is not None else (lambda text: text),
    )


def _by_code(messages, code):
    return [m for m in messages if m['metricCode'] == code]


# ---------------------------------------------------------------------------
# 请求构造（平台层）
# ---------------------------------------------------------------------------

def test_slow_sql_path_uses_gaussdb_namespace():
    assert SLOW_SQL_PATH == (
        '/rds/gaussdb/v3/instances/{instance_id}/slow-sql-list'
    )
    assert TOP_SQL_PATH == '/rds/gaussdb/v3/instances/{instance_id}/top-sql-list'


def test_build_slow_sql_body_carries_threshold_and_nodes():
    body = build_slow_sql_body(
        instance_id=INSTANCE_ID,
        node_ids=['n1', 'n2'],
        start_time=1000,
        end_time=2000,
    )
    assert body['instance_id'] == INSTANCE_ID
    assert body['start_time'] == 1000
    assert body['end_time'] == 2000
    assert body['threshold'] == 1000
    assert body['threshold_unit'] == SLOW_SQL_THRESHOLD_UNIT
    assert body['node_ids'] == ['n1', 'n2']


def test_build_top_sql_body_is_pure_passthrough():
    """请求体构造器是纯传参 —— 窗口不在这里套。

    固定窗口（慢 SQL 1 天 / Top SQL 7 天）由采集器在发请求前算好
    （start_time = now - window），构造器只负责把给定的 start/end 放进去。
    这与它的兄弟 build_slow_sql_body 同构。
    """
    body = build_top_sql_body(
        instance_id=INSTANCE_ID,
        node_ids=['n1'],
        start_time=1000,
        end_time=2000,
    )
    assert body['instance_id'] == INSTANCE_ID
    assert body['node_ids'] == ['n1']
    assert body['start_time'] == 1000
    assert body['end_time'] == 2000


def test_collector_applies_fixed_window_per_collection():
    """采集器按集合套用固定窗口：慢 SQL 1 天、Top SQL 7 天。

    窗口的断言必须落在这一层 —— 构造器是纯传参，窗口是采集器加上去的。
    """
    client = FakeClient(
        slow_body={'slow_sql_infos': [], 'total': 0},
        top_body={'top_sql_infos': [], 'total': 0},
    )
    _collector(client).collect(instance_id=INSTANCE_ID, nodes=NODES)

    windows = {}
    for path, payload in client.calls:
        if path.endswith('/slow-sql-list'):
            windows['slow'] = payload['end_time'] - payload['start_time']
        elif path.endswith('/top-sql-list'):
            windows['top'] = payload['end_time'] - payload['start_time']
    assert windows.get('slow') == SLOW_SQL_WINDOW_MS
    assert windows.get('top') == TOP_SQL_WINDOW_MS


def test_slow_sql_window_is_one_day():
    assert SLOW_SQL_WINDOW_MS == 24 * 60 * 60 * 1000


# ---------------------------------------------------------------------------
# 响应解析（平台层）
# ---------------------------------------------------------------------------

def test_parse_slow_sql_rows_returns_rows():
    body = {'slow_sql_infos': [_slow_row('select 1')], 'total': 1}
    rows = parse_slow_sql_rows(body, 'slow-sql-list')
    assert len(rows) == 1
    assert rows[0]['sql_text'] == 'select 1'


def test_parse_top_sql_rows_returns_rows():
    body = {'top_sql_infos': [_top_row('1', '0.5')], 'total': 1}
    rows = parse_top_sql_rows(body, 'top-sql-list')
    assert len(rows) == 1


def test_parse_rows_rejects_non_list_collection():
    with pytest.raises(ProtocolError):
        parse_slow_sql_rows({'slow_sql_infos': {'a': 1}}, 'slow-sql-list')


def test_parse_rows_tolerates_missing_collection_as_empty():
    rows = parse_slow_sql_rows({}, 'slow-sql-list')
    assert rows == ()


# ---------------------------------------------------------------------------
# 采集器
# ---------------------------------------------------------------------------

def test_collector_posts_to_gaussdb_endpoints():
    client = FakeClient(
        slow_body={'slow_sql_infos': [], 'total': 0},
        top_body={'top_sql_infos': [], 'total': 0},
    )
    _collector(client).collect(instance_id=INSTANCE_ID, nodes=NODES)
    paths = [path for path, _payload in client.calls]
    assert paths == [
        SLOW_SQL_PATH.format(instance_id=INSTANCE_ID),
        TOP_SQL_PATH.format(instance_id=INSTANCE_ID),
    ]


def test_collection_count_metric_value_is_row_count():
    client = FakeClient(
        slow_body={
            'slow_sql_infos': [
                _slow_row('select 1', '1'),
                _slow_row('insert into t values(1)', '2'),
                _slow_row('update t set a=1', '3'),
            ],
            'total': 3,
        },
        top_body={'top_sql_infos': [], 'total': 0},
    )
    messages = _collector(client).collect(instance_id=INSTANCE_ID, nodes=NODES)
    counted = _by_code(messages, 'GAUSSDB.Total_slow_queries')
    assert len(counted) == 1
    assert counted[0]['value'] == 3
    assert counted[0]['metricName'] == '慢查询总数'


def test_prefix_counts_split_select_and_dml():
    client = FakeClient(
        slow_body={
            'slow_sql_infos': [
                _slow_row('select 1', '1'),
                _slow_row('  SELECT 2', '2'),
                _slow_row('with x as (select 1) select * from x', '3'),
                _slow_row('insert into t values(1)', '4'),
                _slow_row('UPDATE t set a=1', '5'),
                _slow_row('delete from t', '6'),
                _slow_row('create database lz;', '7'),
            ],
            'total': 7,
        },
        top_body={'top_sql_infos': [], 'total': 0},
    )
    messages = _collector(client).collect(instance_id=INSTANCE_ID, nodes=NODES)
    select = _by_code(messages, 'GAUSSDB.Select_slow_queries')
    dml = _by_code(messages, 'GAUSSDB.DML_slow_queries')
    assert len(select) == 1
    assert len(dml) == 1
    # select / with 计入 SELECT 类；insert / update / delete 计入 DML 类
    assert select[0]['value'] == 3
    assert dml[0]['value'] == 3


def test_prefix_lists_come_from_mapping_table():
    mapping = load_mapping()
    rules = {
        rule.metric_code: rule
        for rule in mapping.metric_rules
        if (rule.source or {}).get('source') == 'diagnostic'
    }
    select = rules['GAUSSDB.Select_slow_queries'].value_rule['params']['prefixes']
    dml = rules['GAUSSDB.DML_slow_queries'].value_rule['params']['prefixes']
    assert sorted(select) == ['select', 'with']
    assert sorted(dml) == ['delete', 'insert', 'update']


def test_top_n_metric_sorted_and_limited():
    rows = [_top_row(str(index), '%.4f' % (10 - index)) for index in range(15)]
    client = FakeClient(
        slow_body={'slow_sql_infos': [], 'total': 0},
        top_body={'top_sql_infos': rows, 'total': len(rows)},
    )
    messages = _collector(client).collect(instance_id=INSTANCE_ID, nodes=NODES)
    top = _by_code(messages, 'gaussdb.Top10sql')
    assert len(top) == 1
    value = top[0]['value']
    assert isinstance(value, list)
    assert len(value) == 10
    assert value[0]['sql_id'] == '0'
    assert value[-1]['sql_id'] == '9'


def test_sql_text_is_sanitized_before_entering_message():
    seen = []

    def sanitizer(text):
        seen.append(text)
        return 'MASKED'

    rows = [_top_row('1', '1.0', sql_text="select * from t where a='secret'")]
    client = FakeClient(
        slow_body={'slow_sql_infos': [], 'total': 0},
        top_body={'top_sql_infos': rows, 'total': 1},
    )
    messages = _collector(client, sanitizer=sanitizer).collect(
        instance_id=INSTANCE_ID, nodes=NODES
    )
    top = _by_code(messages, 'gaussdb.Top10sql')
    assert seen == ["select * from t where a='secret'"]
    assert top[0]['value'][0]['sql_text'] == 'MASKED'


def test_resource_name_falls_back_to_first_master_cn():
    client = FakeClient(
        slow_body={'slow_sql_infos': [_slow_row('select 1')], 'total': 1},
        top_body={'top_sql_infos': [], 'total': 0},
    )
    messages = _collector(client).collect(instance_id=INSTANCE_ID, nodes=NODES)
    assert messages
    for message in messages:
        assert message['resourceName'] == EXPECTED_RESOURCE_NAME
        assert message['tags']['node_id'] == NODES[0]['node_id']
        assert message['tags']['host_ip'] == NODES[0]['node_ip']


def test_empty_result_set_is_logged_as_warning(caplog):
    client = FakeClient(
        slow_body={'slow_sql_infos': [], 'total': 0},
        top_body={'top_sql_infos': [], 'total': 0},
    )
    with caplog.at_level(logging.WARNING, logger='tpops_kafka_collector'):
        messages = _collector(client).collect(instance_id=INSTANCE_ID, nodes=NODES)
    assert messages
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any(SLOW_SQL_COLLECTION in text for text in warnings)
    assert any(TOP_SQL_COLLECTION in text for text in warnings)


def test_empty_top_sql_collection_produces_no_top_n_message(caplog):
    client = FakeClient(
        slow_body={'slow_sql_infos': [], 'total': 0},
        top_body={'top_sql_infos': [], 'total': 0},
    )
    with caplog.at_level(logging.WARNING, logger='tpops_kafka_collector'):
        messages = _collector(client).collect(instance_id=INSTANCE_ID, nodes=NODES)
    assert _by_code(messages, 'gaussdb.Top10sql') == []


def test_missing_node_list_skips_messages_with_error_log(caplog):
    client = FakeClient(
        slow_body={'slow_sql_infos': [_slow_row('select 1')], 'total': 1},
        top_body={'top_sql_infos': [], 'total': 0},
    )
    with caplog.at_level(logging.ERROR, logger='tpops_kafka_collector'):
        messages = _collector(client).collect(instance_id=INSTANCE_ID, nodes=[])
    assert not messages
    assert any(
        r.levelno == logging.ERROR for r in caplog.records
    )


def test_collector_rejects_sanitizer_failure():
    def boom(text):
        raise MappingError('脱敏炸了')

    client = FakeClient(
        slow_body={'slow_sql_infos': [], 'total': 0},
        top_body={'top_sql_infos': [_top_row('1', '1.0')], 'total': 1},
    )
    with pytest.raises(MappingError):
        _collector(client, sanitizer=boom).collect(
            instance_id=INSTANCE_ID, nodes=NODES
        )


def test_message_fields_are_exactly_the_itscada_envelope():
    client = FakeClient(
        slow_body={'slow_sql_infos': [_slow_row('select 1')], 'total': 1},
        top_body={'top_sql_infos': [], 'total': 0},
    )
    messages = _collector(client).collect(instance_id=INSTANCE_ID, nodes=NODES)
    for message in messages:
        assert set(message) == {
            'metricName', 'metricCode', 'resourceName', 'value', 'unit', 'clock', 'tags',
        }


def test_messages_are_immutable_mapping():
    client = FakeClient(
        slow_body={'slow_sql_infos': [_slow_row('select 1')], 'total': 1},
        top_body={'top_sql_infos': [], 'total': 0},
    )
    messages = _collector(client).collect(instance_id=INSTANCE_ID, nodes=NODES)
    assert isinstance(messages, tuple)


def test_replay_of_archived_slow_sql_response():
    """用实测留档回放：``data/metric_response/019_GAUSSDB.Total_slow_queries.json``。"""
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        'data', 'metric_response', '019_GAUSSDB.Total_slow_queries.json',
    )
    if not os.path.isfile(path):  # pragma: no cover - 留档不在时跳过
        pytest.skip('实测留档不在本次运行的工作区里')
    import json
    with open(path, encoding='utf-8') as handle:
        payload = json.load(handle)
    body = payload[0]['response_body']
    client = FakeClient(slow_body=body, top_body={'top_sql_infos': [], 'total': 0})
    messages = _collector(client).collect(instance_id=INSTANCE_ID, nodes=NODES)
    counted = _by_code(messages, 'GAUSSDB.Total_slow_queries')
    assert len(counted) == 1
    assert counted[0]['value'] == len(body['slow_sql_infos'])
