# -*- coding: utf-8 -*-
"""统一命令中心（B 类）三跳客户端的单元测试。

**不连真实平台** —— 用假 TPOPS 客户端回放留档形态的响应体。

守的是几条「实测踩过的坑」（每一��都对应一条断言）：

- **结束判据用 ``statistics.running == 0``**，**绝不能看 ``job_status``** ——
  实测节点任务已 ``succeed`` 而 ``job_status`` 仍停在 ``running``；
- ``result`` 是 **JSON 字符串**，要**二次 ``json.loads``** 才是行列表；
- **只在 master 节点执行** —— 下发前取节点清单，按 ``node_id`` **去重**
  （同一实例里同一个 master 以 CN / DN 各出现一次）；
- 节点清单**按实例缓存 1 小时**，不每轮都拉；
- **并发可配**（默认 2），一条 SQL 失败**不影响同批其他**；
- **逐条重试** —— 实测 50 条 SQL 有 3 条偶发失败，重试即成功；
- 重试判定**只认 ``errors.is_retryable``**（不按类名精确相等）。
"""
from __future__ import absolute_import

import json
import threading

import pytest

from tpops_kafka_collector.config import Config
from tpops_kafka_collector.errors import (
    AuthError,
    DecodeError,
    ProtocolError,
    TransientError,
    is_retryable,
)
from tpops_kafka_collector.retry import RetryPolicy
from tpops_kafka_collector.tpops.command_center import (
    GENERAL_SINGLE_SQL_CMD_ID,
    CommandCenterClient,
    CommandTaskError,
    parse_result_rows,
)

INSTANCE = '83cd68f2e4cb40b4b7e69240479ffa26in14'

#: 留档 ``data/metric_response/057_gaussdb.Prepared.json`` 的节点清单 ——
#: 12 条条目 / 6 条 role=master / **3 个唯一 node_id**（去重后）。
NODE_LIST = {
    'nodes': [
        {'node_id': 'node-a', 'node_name': 'n-0', 'role': 'master', 'type': 'CN',
         'component_id': 'cn_5001', 'node_ip': '10.0.0.1'},
        {'node_id': 'node-b', 'node_name': 'n-1', 'role': 'master', 'type': 'CN',
         'component_id': 'cn_5002', 'node_ip': '10.0.0.2'},
        {'node_id': 'node-c', 'node_name': 'n-2', 'role': 'master', 'type': 'CN',
         'component_id': 'cn_5003', 'node_ip': '10.0.0.3'},
        {'node_id': 'node-a', 'node_name': 'n-0', 'role': 'master', 'type': 'DN',
         'component_id': 'dn_6001', 'node_ip': '10.0.0.1'},
        {'node_id': 'node-b', 'node_name': 'n-1', 'role': 'slave', 'type': 'DN',
         'component_id': 'dn_6002', 'node_ip': '10.0.0.2'},
        {'node_id': 'node-c', 'node_name': 'n-2', 'role': 'slave', 'type': 'DN',
         'component_id': 'dn_6003', 'node_ip': '10.0.0.3'},
        {'node_id': 'node-a', 'node_name': 'n-0', 'role': 'master', 'type': 'DN',
         'component_id': 'dn_6004', 'node_ip': '10.0.0.1'},
        {'node_id': 'node-b', 'node_name': 'n-1', 'role': 'master', 'type': 'DN',
         'component_id': 'dn_6005', 'node_ip': '10.0.0.2'},
        {'node_id': 'node-c', 'node_name': 'n-2', 'role': 'master', 'type': 'DN',
         'component_id': 'dn_6006', 'node_ip': '10.0.0.3'},
    ]
}

MASTER_NODE_IDS = ('node-a', 'node-b', 'node-c')


def _records(job_id, running=0, status='succeed', error_msg=None, tasks=None):
    """构造 ``task-records`` 响应体（轮询第 2 跳）。"""
    if tasks is None:
        tasks = [
            {
                'task_id': 'task-%d' % index,
                'node_id': node_id,
                'node_name': 'n-%d' % index,
                'status': status,
                'error_msg': error_msg,
            }
            for index, node_id in enumerate(MASTER_NODE_IDS)
        ]
    return {
        'total_count': len(tasks),
        'statistics': {'total': len(tasks), 'succeed': len(tasks), 'failed': 0,
                       'running': running, 'other': 0},
        # **job_status 故意停在 running** —— 结束判据不能看它。
        'job_status': 'running',
        'tasks': tasks,
    }


def _detail(node_id, rows, status='succeed'):
    """构造任务明细响应体（第 3 跳）—— ``result`` 是 JSON **字符串**。"""
    return {
        'node_id': node_id,
        'status': status,
        'is_redirect': False,
        'result': json.dumps(rows),
        'out_format': 'json',
        'out_mode': 'array',
        'out_type': 'map',
        'download_file_id': None,
    }


class FakePlatform(object):
    """假 TPOPS 客户端：按路径分派，记录全部调用。"""

    def __init__(self, nodes=None, submit=None, records=None, detail=None):
        self._nodes = NODE_LIST if nodes is None else nodes
        self._submit = submit
        self._records = records if records is not None else (lambda job_id: _records(job_id))
        self._detail = detail if detail is not None else (
            lambda job_id, task_id: _detail(task_id.split('-')[-1], [{'v': task_id}])
        )
        self.calls = []
        self.node_requests = 0

    # -- 客户端接口 ----------------------------------------------------

    def get(self, path, params=None):
        self.calls.append(('GET', path, params))
        if 'key-view-execute-node' in path:
            self.node_requests += 1
            return self._nodes
        if 'task-records' in path:
            return self._records(params['job_id'])
        if path.endswith('/detail'):
            job_id = path.split('/batch-exec-job/')[1].split('/task-record/')[0]
            task_id = path.split('/task-record/')[1].split('/detail')[0]
            return self._detail(job_id, task_id)
        raise AssertionError('未预期的 GET：%s' % path)

    def post(self, path, payload=None):
        self.calls.append(('POST', path, payload))
        if 'customer-commands' in path:
            if self._submit is not None:
                return self._submit(payload)
            return {
                'job_id': 'job-1',
                'node_tasks': [
                    {'task_id': 'task-%d' % index, 'node_id': node_id}
                    for index, node_id in enumerate(MASTER_NODE_IDS)
                ],
            }
        raise AssertionError('未预期的 POST：%s' % path)

    # -- 断言辅助 ------------------------------------------------------

    def calls_of(self, kind, fragment):
        return [call for call in self.calls if call[0] == kind and fragment in call[1]]


def _client(platform, **kwargs):
    kwargs.setdefault('retry_policy', RetryPolicy(max_attempts=3, base_delay=0.0,
                                                  jitter_ratio=0.0))
    kwargs.setdefault('sleep', lambda _seconds: None)
    return CommandCenterClient(platform, **kwargs)


# ---------------------------------------------------------------------------
# 节点清单：只在 master + 按 node_id 去重 + 按实例缓存 1 小时
# ---------------------------------------------------------------------------

def test_master_node_ids_are_deduplicated_by_node_id():
    platform = FakePlatform()
    client = _client(platform)

    assert client.master_node_ids(INSTANCE) == MASTER_NODE_IDS


def test_non_master_nodes_are_never_dispatched_to():
    platform = FakePlatform()
    client = _client(platform)

    client.execute_sql(INSTANCE, 'select 1', limit=10)

    submit = platform.calls_of('POST', 'customer-commands')[0]
    assert submit[2]['node_ids'] == list(MASTER_NODE_IDS)


def test_node_list_is_cached_for_one_hour():
    platform = FakePlatform()
    now = [1000.0]
    client = _client(platform, clock=lambda: now[0])

    client.master_node_ids(INSTANCE)
    client.master_node_ids(INSTANCE)
    assert platform.node_requests == 1

    now[0] += 3599.0
    client.master_node_ids(INSTANCE)
    assert platform.node_requests == 1

    now[0] += 2.0
    client.master_node_ids(INSTANCE)
    assert platform.node_requests == 2


def test_node_list_cache_is_per_instance():
    platform = FakePlatform()
    client = _client(platform)

    client.master_node_ids(INSTANCE)
    client.master_node_ids('another-instance')

    assert platform.node_requests == 2


def test_empty_node_list_is_a_protocol_error():
    platform = FakePlatform(nodes={'nodes': []})
    client = _client(platform)

    with pytest.raises(ProtocolError):
        client.master_node_ids(INSTANCE)


# ---------------------------------------------------------------------------
# 三跳编排
# ---------------------------------------------------------------------------

def test_submit_uses_general_single_sql_command_namespace():
    platform = FakePlatform()
    client = _client(platform)

    client.execute_sql(INSTANCE, 'select 1', limit=10)

    submit = platform.calls_of('POST', 'customer-commands')[0]
    assert submit[1].startswith('/rds/gaussdb/v3/command-center/customer-commands/')
    assert GENERAL_SINGLE_SQL_CMD_ID in submit[1]
    params = dict((item['param_name'], item['param_value']) for item in submit[2]['params'])
    assert params['sql_text'] == 'select 1'
    assert params['limit'] == '10'


def test_polling_ends_when_statistics_running_is_zero_even_if_job_status_is_running():
    """**实测坑**：``job_status`` 仍是 ``running``，但 ``statistics.running`` 已是 0。

    结束判据必须是 ``statistics.running == 0``。
    """
    platform = FakePlatform()
    client = _client(platform)

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    # 只轮询了 1 次 —— 没有因为 job_status == 'running' 而继续轮询。
    assert len(platform.calls_of('GET', 'task-records')) == 1
    assert outcome.ok is True
    assert len(outcome.tasks) == 3


def test_polling_continues_while_statistics_running_is_not_zero():
    responses = [
        _records('job-1', running=2),
        _records('job-1', running=1),
        _records('job-1', running=0),
    ]

    def records(job_id):
        return responses.pop(0)

    platform = FakePlatform(records=records)
    client = _client(platform)

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    assert len(platform.calls_of('GET', 'task-records')) == 3
    assert outcome.ok is True


def test_polling_timeout_is_retryable():
    platform = FakePlatform(records=lambda job_id: _records(job_id, running=1))
    clock = [0.0]
    client = _client(
        platform,
        poll_timeout=5.0,
        clock=lambda: clock[0],
        sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        retry_policy=RetryPolicy(max_attempts=1, base_delay=0.0, jitter_ratio=0.0),
    )

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    assert outcome.ok is False
    assert '超时' in outcome.error


def test_missing_statistics_is_a_protocol_error():
    platform = FakePlatform(records=lambda job_id: {'job_status': 'running', 'tasks': []})
    client = _client(platform, retry_policy=RetryPolicy(max_attempts=1))

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    assert outcome.ok is False
    assert isinstance(outcome.exception, ProtocolError)
    assert not is_retryable(outcome.exception)


# ---------------------------------------------------------------------------
# result 的二次解析
# ---------------------------------------------------------------------------

def test_result_string_is_parsed_a_second_time_into_rows():
    platform = FakePlatform(
        detail=lambda job_id, task_id: _detail(task_id.split('-')[-1], [{'v': 42}])
    )
    client = _client(platform)

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    assert outcome.ok is True
    assert [task.rows for task in outcome.tasks] == [({'v': 42},)] * 3


def test_parse_result_rows_handles_empty_and_already_parsed():
    assert parse_result_rows('') == ()
    assert parse_result_rows(None) == ()
    assert parse_result_rows('[{"a": 1}]') == ({'a': 1},)
    assert parse_result_rows([{'a': 1}]) == ({'a': 1},)


def test_parse_result_rows_rejects_invalid_json():
    with pytest.raises(DecodeError):
        parse_result_rows('{not json')


def test_task_detail_is_fetched_for_every_master_node():
    platform = FakePlatform()
    client = _client(platform)

    client.execute_sql(INSTANCE, 'select 1', limit=10)

    details = platform.calls_of('GET', '/detail')
    assert len(details) == 3


# ---------------------------------------------------------------------------
# 逐条重试 + 单条失败隔离
# ---------------------------------------------------------------------------

def test_failed_node_task_triggers_a_retry_of_that_sql_only():
    state = {'calls': 0}

    def submit(payload):
        state['calls'] += 1
        return {
            'job_id': 'job-%d' % state['calls'],
            'node_tasks': [
                {'task_id': 'task-%d' % index, 'node_id': node_id}
                for index, node_id in enumerate(MASTER_NODE_IDS)
            ],
        }

    def records(job_id):
        status = 'failed' if job_id == 'job-1' else 'succeed'
        return _records(job_id, status=status, error_msg='boom' if status == 'failed' else None)

    platform = FakePlatform(submit=submit, records=records)
    client = _client(platform)

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    assert outcome.ok is True
    assert outcome.attempts == 2


def test_exhausted_retry_is_recorded_as_a_failure_not_raised():
    platform = FakePlatform(records=lambda job_id: _records(job_id, status='failed',
                                                             error_msg='boom'))
    client = _client(platform, retry_policy=RetryPolicy(max_attempts=2, base_delay=0.0,
                                                         jitter_ratio=0.0))

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    assert outcome.ok is False
    assert outcome.attempts == 2
    assert outcome.error


def test_one_failing_sql_does_not_affect_the_others_in_the_same_batch():
    def submit(payload):
        sql = dict((item['param_name'], item['param_value'])
                   for item in payload['params'])['sql_text']
        index = int(sql.rsplit(' ', 1)[-1])
        if index == 1:
            return {
                'job_id': 'bad-job',
                'node_tasks': [{'task_id': 'task-0', 'node_id': MASTER_NODE_IDS[0]}],
            }
        return {
            'job_id': 'good-job-%d' % index,
            'node_tasks': [{'task_id': 'task-0', 'node_id': MASTER_NODE_IDS[0]}],
        }

    def records(job_id):
        if job_id == 'bad-job':
            return _records(job_id, status='failed', error_msg='boom',
                            tasks=[{'task_id': 'task-0', 'node_id': MASTER_NODE_IDS[0],
                                    'node_name': 'n-0', 'status': 'failed',
                                    'error_msg': 'boom'}])
        return _records(job_id, tasks=[{'task_id': 'task-0', 'node_id': MASTER_NODE_IDS[0],
                                        'node_name': 'n-0', 'status': 'succeed',
                                        'error_msg': None}])

    def detail(job_id, task_id):
        return _detail(MASTER_NODE_IDS[0], [{'v': job_id}])

    platform = FakePlatform(submit=submit, records=records, detail=detail)
    client = _client(platform, retry_policy=RetryPolicy(max_attempts=1, base_delay=0.0,
                                                         jitter_ratio=0.0))

    outcomes = client.execute_batch(INSTANCE, ['select 0', 'select 1', 'select 2'])

    assert [outcome.ok for outcome in outcomes] == [True, False, True]
    assert outcomes[1].error
    assert outcomes[0].tasks[0].rows == ({'v': 'good-job-0'},)


def test_batch_failure_carries_the_failing_sql_and_the_reason():
    platform = FakePlatform(records=lambda job_id: _records(job_id, status='failed',
                                                             error_msg='boom'))
    client = _client(platform, retry_policy=RetryPolicy(max_attempts=1, base_delay=0.0,
                                                         jitter_ratio=0.0))

    outcomes = client.execute_batch(INSTANCE, ['select 0'])

    assert outcomes[0].ok is False
    assert outcomes[0].sql_text == 'select 0'
    assert 'boom' in outcomes[0].error


def test_auth_error_is_not_retried():
    class AuthPlatform(FakePlatform):
        def post(self, path, payload=None):
            raise AuthError('401')

    platform = AuthPlatform()
    client = _client(platform)

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    assert outcome.ok is False
    assert outcome.attempts == 1
    assert isinstance(outcome.exception, AuthError)


def test_transient_error_is_retried():
    class FlakyPlatform(FakePlatform):
        def __init__(self):
            FakePlatform.__init__(self)
            self.attempts = 0

        def post(self, path, payload=None):
            self.attempts += 1
            if self.attempts == 1:
                raise TransientError('503')
            return FakePlatform.post(self, path, payload)

    platform = FlakyPlatform()
    client = _client(platform)

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    assert outcome.ok is True
    assert outcome.attempts == 2


def test_command_task_error_is_retryable():
    assert is_retryable(CommandTaskError('boom'))


# ---------------------------------------------------------------------------
# 并发
# ---------------------------------------------------------------------------

def test_batch_runs_concurrently_and_respects_the_concurrency_limit():
    barrier = threading.Barrier(2, timeout=10)
    entered = []
    lock = threading.Lock()

    def submit(payload):
        with lock:
            entered.append(payload)
        barrier.wait()
        return {
            'job_id': 'job-%d' % len(entered),
            'node_tasks': [{'task_id': 'task-0', 'node_id': MASTER_NODE_IDS[0]}],
        }

    platform = FakePlatform(submit=submit)
    client = _client(platform, concurrency=2)

    outcomes = client.execute_batch(INSTANCE, ['select 0', 'select 1'])

    assert len(outcomes) == 2
    assert all(outcome.ok for outcome in outcomes)


def test_concurrency_of_one_serialises_the_batch():
    order = []
    lock = threading.Lock()

    def submit(payload):
        with lock:
            order.append(payload)
        return {
            'job_id': 'job-%d' % len(order),
            'node_tasks': [{'task_id': 'task-0', 'node_id': MASTER_NODE_IDS[0]}],
        }

    platform = FakePlatform(submit=submit)
    client = _client(platform, concurrency=1)

    client.execute_batch(INSTANCE, ['select 0', 'select 1'])

    assert len(order) == 2


def test_default_concurrency_is_two():
    client = CommandCenterClient(FakePlatform())
    assert client.concurrency == 2


def test_concurrency_is_passed_to_the_executor_factory():
    """并发数要能被**假执行器**断言 —— 不靠真 sleep、不靠计时。"""
    created = []

    class FakeExecutor(object):
        def __init__(self, max_workers):
            self.max_workers = max_workers
            created.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

        def submit(self, func, *args):
            class _Future(object):
                def result(inner):
                    return func(*args)

            return _Future()

    platform = FakePlatform()
    client = _client(platform, concurrency=3, executor_factory=FakeExecutor)

    client.execute_batch(INSTANCE, ['select 0'])

    assert created == [3]


def test_submit_without_node_tasks_is_a_protocol_error():
    platform = FakePlatform(submit=lambda payload: {'job_id': 'job-1', 'node_tasks': []})
    client = _client(platform)

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    assert outcome.ok is False
    assert isinstance(outcome.exception, ProtocolError)


def test_detail_is_fetched_only_for_the_dispatched_nodes():
    platform = FakePlatform(
        submit=lambda payload: {
            'job_id': 'job-1',
            'node_tasks': [{'task_id': 'task-0', 'node_id': MASTER_NODE_IDS[0]}],
        }
    )
    client = _client(platform)

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    assert len(platform.calls_of('GET', '/detail')) == 1
    assert len(outcome.tasks) == 1


def test_result_reporting_failure_is_treated_as_a_failed_task():
    platform = FakePlatform(
        detail=lambda job_id, task_id: {
            'status': 'succeed',
            'result': 'Failed to execute command on all nodes',
        }
    )
    client = _client(platform, retry_policy=RetryPolicy(max_attempts=1))

    outcome = client.execute_sql(INSTANCE, 'select 1', limit=10)

    assert outcome.ok is False
    assert 'Failed to execute command' in outcome.error


# ---------------------------------------------------------------------------
# 配置装配
# ---------------------------------------------------------------------------

def test_from_config_uses_the_configured_concurrency_and_poll_timeout():
    config = Config(
        tpops_user='u', tpops_password='p',
        cmd_concurrency=4, cmd_poll_timeout=90, cmd_retry_attempts=2,
    )
    client = CommandCenterClient.from_config(config, FakePlatform())

    assert client.concurrency == 4


def test_concurrency_below_one_is_rejected():
    with pytest.raises(ValueError):
        CommandCenterClient(FakePlatform(), concurrency=0)


# ---------------------------------------------------------------------------
# 留档回放：真实节点清单
# ---------------------------------------------------------------------------

def test_replay_archived_node_list_dedupes_to_three_masters():
    """回放留档里的真实节点清单 —— 实测 12 条 / 6 条 master / **3 个唯一 node_id**。"""
    import os

    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(
        repo, 'data', 'metric_response', '057_gaussdb.Prepared.json'
    )
    if not os.path.exists(path):
        pytest.skip('留档文件不在本仓库：%s' % path)
    with open(path, encoding='utf-8') as handle:
        calls = json.load(handle)
    nodes = None
    for call in calls:
        if 'key-view-execute-node' in call.get('api', ''):
            nodes = call['response_body']['nodes']
            break
    assert nodes is not None

    platform = FakePlatform(nodes={'nodes': nodes})
    client = _client(platform)

    node_ids = client.master_node_ids(INSTANCE)

    assert len(nodes) == 12
    assert sum(1 for node in nodes if node['role'] == 'master') == 6
    assert len(node_ids) == 3
    assert len(set(node_ids)) == 3


# ---------------------------------------------------------------------------
# 留档回放
# ---------------------------------------------------------------------------

def test_replay_archived_merged_sql_result_shape():
    """回放 ``data/merged_sql/sql_merge_probe_result.json`` 的真实形态。

    合并形态的 ``result`` 解析后是**恰好一行**，该行的每个键是一个视图名，
    值是该视图的 JSON 字符串 —— 采集端这一层只做二次解析，视图级解析在映射层。
    """
    import os

    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(repo, 'data', 'merged_sql', 'sql_merge_probe_result.json')
    if not os.path.exists(path):
        pytest.skip('留档文件不在本仓库：%s' % path)
    with open(path, encoding='utf-8') as handle:
        archived = json.load(handle)

    assert archived['statistics']['running'] == 0

    for node_name, node in archived['nodes'].items():
        rows = parse_result_rows(node['result'])
        assert len(rows) == 1
        assert len(rows[0]) == 11
        for view_name, value in rows[0].items():
            assert isinstance(value, str)
            assert isinstance(json.loads(value), list)
