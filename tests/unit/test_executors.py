# -*- coding: utf-8 -*-
"""执行器层的**外部调用次数**守卫（wave-3 修复轮 R-B）。

守的是三件事：

1. **上报的 ``external_calls`` == 真实发出的 HTTP 次数** —— 不是写死的常数
   （修复前是 ``MONITOR_EXTERNAL_CALLS = 2`` / ``ALARM_EXTERNAL_CALLS = 4``
   这类编造值：42 次预算告警因此永远不响）；
2. **指标分组目录有缓存** —— 同一轮不该对 12 个分组码各发一次 GET
   （修复前每轮 12 次，跨轮也不复用）；
3. **告警的调用数随页数变化** —— 四种形态各自分页，不是固定的 4。

假客户端**像真客户端一样**在「发出请求」那一刻计数（``calls``），
断言用的是它自己的计数 —— 测试里**不写死**任何调用次数。
"""
import json
import threading

from tpops_kafka_collector.config import Config
from tpops_kafka_collector.executors import build_executors
from tpops_kafka_collector.mapping.loader import load_mapping
from tpops_kafka_collector.schedule import (
    CHANNEL_ALARM,
    CHANNEL_COMMAND_CENTER,
    CHANNEL_DIAGNOSTIC,
    CHANNEL_INSTANCE,
    CHANNEL_MONITOR,
)
from tpops_kafka_collector.scheduler_types import InstanceTarget
from tpops_kafka_collector.tpops.call_tracking import CallCounter
from tpops_kafka_collector.tpops.command_center import CommandCenterClient
from tpops_kafka_collector.mapping.sqlgen import load_spec
from tpops_kafka_collector.tpops.metrics import GROUP_CATALOG_PATH, METRICS_PATH

#: 目标实例（与生产一致）。
INSTANCE_ID = 'inst-1'
#: 固定时刻（确定性）。
NOW_MS = 1794500000000


# ---------------------------------------------------------------------------
# 假客户端
# ---------------------------------------------------------------------------

class CountingFakeClient:
    """假 TPOPS 客户端：**像真客户端一样**在发出请求时计数。

    ``calls`` 是真正的 :class:`CallCounter` —— 与 ``TpopsClient`` 同一套接口，
    执行器因此能拿到**真实**的请求数（而不是自己编一个）。
    """

    def __init__(self, alarm_records=None):
        self.requests = []
        self._calls = CallCounter()
        self._alarm_records = list(alarm_records or ())

    @property
    def calls(self):
        return self._calls

    def get(self, path, params=None):
        self._calls.record()
        self.requests.append(('GET', path, params))
        return self._respond('GET', path, params)

    def post(self, path, payload=None):
        self._calls.record()
        self.requests.append(('POST', path, payload))
        return self._respond('POST', path, payload)

    def count_of(self, path):
        """某个路径被请求了多少次。"""
        return len([item for item in self.requests if item[1] == path])

    def _respond(self, method, path, payload):
        if path == GROUP_CATALOG_PATH:
            return {'series': [{'metrics': [{'name': 'rds092_gaussv5_commit_counter'}]}]}
        if path == METRICS_PATH:
            return {'metric_infos': []}
        if path.endswith('/key-view-execute-node'):
            return {'nodes': [{
                'node_id': 'n1', 'role': 'master', 'component_id': 'cn_5001',
                'node_ip': '10.0.0.1', 'type': 'CN',
            }]}
        if path.endswith('/instances'):
            return {
                'instances': [{
                    'id': INSTANCE_ID, 'name': 'gauss-drill', 'mode': 'Combined',
                    'engine_name': 'gaussdbv5', 'engine_version': 'V2.0-10.0.0',
                }],
                'total_count': 1,
            }
        if path.endswith('/alarm/records'):
            return self._alarm_page(payload or {})
        if 'customer-commands' in path:
            return {'job_id': 'job-1', 'node_tasks': [{'task_id': 't1', 'node_id': 'n1'}]}
        if 'task-records' in path:
            return {
                'job_status': 'running',
                'statistics': {'running': 0},
                'tasks': [{'task_id': 't1', 'status': 'succeed', 'error_msg': ''}],
            }
        if '/task-record/' in path:
            return {'status': 'succeed', 'result': json.dumps([_merged_row()])}
        return {}

    def _alarm_page(self, payload):
        """按 offset / limit 切页 —— 告警形态要能造出「多页」。"""
        limit = int(payload.get('limit') or 100)
        offset = int(payload.get('offset') or 0)
        records = self._alarm_records[offset:offset + limit]
        return {'records': records, 'total_count': len(self._alarm_records)}


def _alarm_record(index):
    """一条告警收敛条目。"""
    return {
        'alarm_id': 'alarm-%d' % index,
        'instance_id': INSTANCE_ID,
        'alarm_name': '压测告警',
        'alarm_type': 'event',
        'severity': 'major',
        'first_alarm_time': NOW_MS - 60000,
        'last_alarm_time': NOW_MS - 30000,
        'status': 'active',
    }


def _config():
    return Config(tpops_user='tester', tpops_password='secret')


def _target():
    return InstanceTarget(INSTANCE_ID, 'gauss-drill', (_node(),))


def _node():
    """一个 master CN 节点（平台原样的字段名）。"""
    return {
        'node_id': 'n1', 'role': 'master', 'component_id': 'cn_5001',
        'node_ip': '10.0.0.1', 'type': 'CN',
    }


def _merged_row():
    """合并形态的那一行（每个视图一个 ``json_agg`` 列）。"""
    spec = load_spec()
    return dict((view.sql_key, []) for view in spec.views)


class FakeNodeRegistry:
    """假节点清单（不发任何请求）。"""

    def nodes(self, instance_id):
        return (_node(),)


def _executors(client, command_center=None):
    return build_executors(
        client=client,
        mapping=load_mapping(),
        config=_config(),
        command_center=command_center,
        node_registry=FakeNodeRegistry(),
        deltas=None,
        watermark=None,
        shutdown=None,
        clock=lambda: NOW_MS,
    )


def _command_center(client):
    """真的命令中心客户端（走真的三跳），只把 HTTP 换成假客户端。"""
    return CommandCenterClient(
        client,
        cmd_id='cmd-1',
        concurrency=1,
        poll_timeout=5,
        poll_interval=0,
        sleep=lambda seconds: None,
        clock=lambda: 0.0,
    )


# ---------------------------------------------------------------------------
# 守卫 1：上报的调用数 == 真实请求数
# ---------------------------------------------------------------------------

def test_monitor_external_calls_equal_real_request_count():
    """A 类：上报的调用数 == 假客户端**实际收到**的请求数。"""
    client = CountingFakeClient()
    executors = _executors(client)

    outcome = executors[CHANNEL_MONITOR](_target())

    assert client.calls.total() > 0
    assert outcome.external_calls == client.calls.total()


def test_command_center_external_calls_equal_real_request_count():
    """B 类：一次 ``execute_sql`` 是**完整三跳** —— 调用数远大于页数。"""
    client = CountingFakeClient()
    executors = _executors(client, command_center=_command_center(client))

    outcome = executors[CHANNEL_COMMAND_CENTER](_target())

    assert outcome.external_calls == client.calls.total()
    # 三跳（下发 + 轮询 + 取明细）至少 3 次 —— 页数口径（1）明显偏低。
    assert outcome.external_calls > outcome.windows


def test_diagnostic_external_calls_equal_real_request_count():
    """C 类：慢 SQL 列表 + Top SQL 列表，各一次。"""
    client = CountingFakeClient()
    executors = _executors(client)

    outcome = executors[CHANNEL_DIAGNOSTIC](_target())

    assert outcome.external_calls == client.calls.total()
    assert outcome.external_calls >= 2


def test_instance_external_calls_equal_real_request_count():
    """D 类：实例列表 + 节点清单。"""
    client = CountingFakeClient()
    executors = _executors(client)

    outcome = executors[CHANNEL_INSTANCE](_target())

    assert outcome.external_calls == client.calls.total()


def test_alarm_external_calls_equal_real_request_count():
    """E 类：四种形态各一次（一页形态）。"""
    client = CountingFakeClient()
    executors = _executors(client)

    outcome = executors[CHANNEL_ALARM](_target())

    assert outcome.external_calls == client.calls.total()


def test_alarm_external_calls_scale_with_page_count():
    """E 类：告警记录超过一页 → 调用数**跟着页数涨**（不是固定的 4）。"""
    client = CountingFakeClient(alarm_records=[_alarm_record(i) for i in range(150)])
    executors = _executors(client)

    outcome = executors[CHANNEL_ALARM](_target())

    # 四种形态 × 各自的页数（150 条 / 每页 100 → 2 页）。
    assert outcome.external_calls == 8
    assert outcome.external_calls == client.calls.total()


def test_shutdown_skipped_task_reports_zero_calls():
    """停机跳过 → 一次请求都没发 → 调用数是 0（不是默认的 1）。"""
    stop = threading.Event()
    stop.set()
    client = CountingFakeClient()
    executors = build_executors(
        client=client,
        mapping=load_mapping(),
        config=_config(),
        command_center=None,
        node_registry=None,
        deltas=None,
        watermark=None,
        shutdown=stop,
        clock=lambda: NOW_MS,
    )

    outcome = executors[CHANNEL_MONITOR](_target())

    assert outcome.external_calls == 0
    assert client.calls.total() == 0


# ---------------------------------------------------------------------------
# 守卫 2：指标分组目录有缓存
# ---------------------------------------------------------------------------

def test_metric_catalog_is_cached_across_rounds():
    """第二轮不该再对 12 个分组码各发一次 GET。"""
    client = CountingFakeClient()
    executors = _executors(client)

    first = executors[CHANNEL_MONITOR](_target())
    catalog_gets_after_first = client.count_of(GROUP_CATALOG_PATH)
    second = executors[CHANNEL_MONITOR](_target())
    catalog_gets_after_second = client.count_of(GROUP_CATALOG_PATH)

    assert catalog_gets_after_first > 1, '首轮应当逐个分组码拉一次目录'
    assert catalog_gets_after_second == catalog_gets_after_first, (
        '目录命中缓存后不该再发请求'
    )
    # 第二轮只剩指标查询本身。
    assert second.external_calls < first.external_calls
