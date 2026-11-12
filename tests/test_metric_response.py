# -*- coding: utf-8 -*-
"""data/metric_response/ 的原生响应留档必须与映射规则保持一致。"""
import io
import json
import os

import pytest

from conftest import DATA_DIR

RESP_DIR = os.path.join(DATA_DIR, 'metric_response')
MANIFEST_PATH = os.path.join(RESP_DIR, '000manifest.json')
RULES_PATH = os.path.join(DATA_DIR, 'mapping', 'metric_rules.json')
MERGED_SQL_PATH = os.path.join(DATA_DIR, 'merged_sql', 'merged_sql.json')

CMD_API_PATTERNS = [
    '/key-view-execute-node',
    '/customer-commands/',
    '/batch-exec-job/task-records',
    '/task-record/',
]


def _load(path):
    with io.open(path, encoding='utf-8') as fp:
        return json.load(fp)


@pytest.fixture(scope='module')
def manifest():
    return _load(MANIFEST_PATH)


@pytest.fixture(scope='module')
def rules():
    return dict((r['metric_code'], r) for r in _load(RULES_PATH)['rules'])


@pytest.fixture(scope='module')
def batches():
    return dict((b['sql_key'], b) for b in _load(MERGED_SQL_PATH)['batches'])


def test_manifest_covers_all_provided_metrics(manifest, rules):
    """98 个可提供指标各有一个文件；3 个无法提供的不建文件。"""
    enabled = [code for code, r in rules.items() if r.get('enabled')]
    assert manifest['metric_count'] == len(enabled) == 98
    assert set(x['customer_metric'] for x in manifest['metrics']) == set(enabled)


def test_every_manifest_entry_has_a_response_file(manifest):
    for item in manifest['metrics']:
        path = os.path.join(RESP_DIR, item['file'])
        assert os.path.exists(path), item['file']
        chain = _load(path)
        assert isinstance(chain, list) and chain
        assert len(chain) == item['api_count']
        for step in chain:
            assert step['api'].startswith(('GET ', 'POST '))
            assert 'response_body' in step


def test_response_files_match_manifest_entries(manifest):
    """目录里除 000manifest.json 外，不得有 manifest 之外的残留文件。"""
    on_disk = set(f for f in os.listdir(RESP_DIR) if f.endswith('.json'))
    on_disk.discard('000manifest.json')
    assert on_disk == set(x['file'] for x in manifest['metrics'])


def test_command_center_files_record_four_hops(manifest):
    """命令中心指标：取节点 → 下发 → 轮询 → 取结果（每个 master 一条）。"""
    cmd_items = [x for x in manifest['metrics'] if x['channel'] == '命令中心SQL']
    assert len(cmd_items) == 82
    for item in cmd_items:
        chain = _load(os.path.join(RESP_DIR, item['file']))
        apis = [s['api'] for s in chain]
        for pattern in CMD_API_PATTERNS:
            assert any(pattern in api for api in apis), (item['file'], pattern)
        # 前三跳顺序固定：取节点 → 下发 → 轮询
        assert apis[0].startswith('GET ') and '/key-view-execute-node' in apis[0]
        assert apis[1].startswith('POST ') and '/customer-commands/' in apis[1]
        assert apis[2].startswith('GET ') and 'task-records' in apis[2]
        # 最后一跳是每个 master 的任务结果
        assert len(apis) == 6      # 3 个 master
        assert all('/task-record/' in api for api in apis[3:])


def test_single_api_channels_have_one_response(manifest):
    """诊断优化 / 实例接口：各只有一次调用。"""
    single = [x for x in manifest['metrics']
              if x['channel'] in ('诊断优化接口', '实例接口')]
    assert len(single) == 5
    for item in single:
        chain = _load(os.path.join(RESP_DIR, item['file']))
        assert len(chain) == 1
        assert chain[0]['response_body']


def test_monitor_files_capture_latest_point_and_series(manifest):
    """监控指标：① 取最新点（is_latest_point_only=true）② 取带时间戳的时序。"""
    monitor = [x for x in manifest['metrics'] if x['channel'] == '监控指标接口']
    assert len(monitor) == 11
    for item in monitor:
        chain = _load(os.path.join(RESP_DIR, item['file']))
        assert len(chain) == 2
        latest, series = chain
        assert latest['request']['body']['is_latest_point_only'] == 'true'
        assert series['request']['body']['is_latest_point_only'] == 'false'
        # 只查一个实例 —— 与命令中心通道同一个分布式实例
        for step in chain:
            instances = step['request']['body']['instances']
            assert len(instances) == 1
            assert instances[0]['instance_id'] == manifest['instance_id']
            assert len(step['response_body']['metric_infos']) == 1
        # 最新点只返回一个点
        info = latest['response_body']['metric_infos'][0]
        assert len(info['metrics'][0]['timestamps']) == 1
        # 时序是完整序列，带真实时间戳
        info = series['response_body']['metric_infos'][0]
        assert len(info['metrics'][0]['timestamps']) > 1
        assert info['metrics'][0]['timestamps'][0] > 0


def test_every_metric_comes_from_the_same_instance(manifest):
    """所有通道都只查这一个实例 —— 采集端一条链路覆盖一个实例。"""
    instance_id = manifest['instance_id']
    for item in manifest['metrics']:
        chain = _load(os.path.join(RESP_DIR, item['file']))
        for step in chain:
            body = step['request'].get('body') or {}
            for key in ('instance_id',):
                if key in body:
                    assert body[key] == instance_id, (item['file'], key)
            for entry in body.get('instances') or []:
                assert entry['instance_id'] == instance_id, item['file']
            assert step['request']['path_params'].get('instance_id', instance_id) == instance_id


def test_every_step_records_the_full_request(manifest):
    """每次调用都要有完整请求对象；headers 只留接口必须的那几个。"""
    allowed_headers = {'X-Mini-Token', 'X-Language', 'Content-Type'}
    for item in manifest['metrics']:
        chain = _load(os.path.join(RESP_DIR, item['file']))
        for step in chain:
            request = step['request']
            for field in ('api', 'method', 'uri', 'path_params',
                          'query_params', 'body', 'headers'):
                assert field in request, (item['file'], field)
            assert request['method'] in ('GET', 'POST')
            assert request['api'].startswith(request['method'] + ' ')
            assert request['uri'] in request['api']
            assert set(request['headers']) <= allowed_headers
            # 真实 token 不落盘
            assert request['headers']['X-Mini-Token'] == '<token>'


def test_manifest_records_every_request_of_each_metric(manifest):
    """manifest 里每个指标要带齐它自己的全部请求对象。"""
    for item in manifest['metrics']:
        requests = item['requests']
        assert len(requests) == item['api_count']
        chain = _load(os.path.join(RESP_DIR, item['file']))
        assert requests == [s['request'] for s in chain]
        for request in requests:
            assert request['api'].startswith(('GET ', 'POST '))
            assert request['method'] in ('GET', 'POST')


def test_command_center_response_contains_metric_column(manifest, rules, batches):
    """每个命令中心指标的取数来源，必须真的出现在它自己的响应里。"""
    for item in manifest['metrics']:
        if item['channel'] != '命令中心SQL':
            continue
        rule = rules[item['customer_metric']]
        chain = _load(os.path.join(RESP_DIR, item['file']))
        rows = []
        for step in chain:
            result = step['response_body'].get('result')
            if isinstance(result, str):
                rows.extend(json.loads(result))
        assert rows, item['file']
        columns = set()
        for row in rows:
            columns.update(row.keys())
        names = set(str(row.get('name')) for row in rows)
        _assert_source_present(item, rule, rows, columns, names, batches)


def _assert_source_present(item, rule, rows, columns, names, batches):
    """取数来源要么是响应里的列，要么是合并 SQL 的来源视图。"""
    params = rule['value_rule'].get('params') or {}
    column = params.get('column')
    if column:
        assert column in columns or column in names, (item['file'], column)
        return
    expr = rule['source'].get('value_source_expr')
    if not expr or expr == '—':
        return                      # raw_rows 形态：指标消费整行，不绑定单列
    candidates = (expr, expr.split('.')[-1], expr.split()[0].split('.')[0])
    if any(c in columns or c in names for c in candidates):
        return
    # 视图级来源（如 pg_locks）：核对合并 SQL 的来源视图
    view = batches[rule['source']['sql_key']]['source_view']
    assert expr.split()[0].split('.')[0] in view, (item['file'], expr, view)
