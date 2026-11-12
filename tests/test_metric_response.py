# -*- coding: utf-8 -*-
"""data/metric_response/ 的原生响应留档必须与映射规则保持一致。"""
import io
import json
import os

import pytest

from conftest import DATA_DIR

RESP_DIR = os.path.join(DATA_DIR, 'metric_response')
MANIFEST_PATH = os.path.join(RESP_DIR, 'manifest.json')
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
    """目录里除 manifest.json 外，不得有 manifest 之外的残留文件。"""
    on_disk = set(f for f in os.listdir(RESP_DIR) if f.endswith('.json'))
    on_disk.discard('manifest.json')
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
    """监控指标 / 诊断优化 / 实例接口：各只有一次调用。"""
    single = [x for x in manifest['metrics'] if x['channel'] != '命令中心SQL']
    assert len(single) == 16
    for item in single:
        chain = _load(os.path.join(RESP_DIR, item['file']))
        assert len(chain) == 1
        assert chain[0]['response_body']


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
