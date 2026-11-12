#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""实测抓取每个客户指标的**原生 API 响应**，落盘到 data/metric_response/。

一个指标一个文件，内容是该指标取数链路上各接口的**完整请求对象 + 平台原生响应体**，
按调用顺序排列：

    [
      {"api": "POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}",
       "request": {
         "api": "POST /rds/gaussdb/v3/command-center/customer-commands/73722acc-...",
         "method": "POST",
         "uri": "/rds/gaussdb/v3/command-center/customer-commands/73722acc-...",
         "path_params": {"cmd_id": "73722acc-..."},
         "query_params": {},
         "body": {"instance_id": "...", "node_ids": ["..."], "params": [...]},
         "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn",
                     "Content-Type": "application/json"}
       },
       "response_body": { ...平台原样返回的 JSON... }}
    ]

headers 只留接口必须的那几个，**token 用占位符 <token>，不落盘真实 token**。

通道与文件数：

| 通道 | 文件数 | 每文件里的 API |
|---|---|---|
| 命令中心 SQL | 82 | ① 取可执行节点 ② 下发命令 ③ 轮询任务状态 ④ 取任务结果（每个 master 一条） |
| 监控指标接口 | 11 | ① 取最新点（is_latest_point_only=true）② 取时序（false + ≥1h 窗口，带真实时间戳） |
| 诊断优化接口 | 4 | ① slow-sql-list（3 个）/ top-sql-list（1 个） |
| 实例接口 | 1 | ① GET /rds/v3.2/instances |

命令中心 SQL 走的是 **11 条合并 SQL**（data/merged_sql/merged_sql.json），
与采集端实现一致：同一批合并 SQL 覆盖的指标共用同一次调用的响应，
所以同一批的多个文件内容相同（文件名/编号不同）。

用法::

    export TPOPS_USER=sml TPOPS_PASSWORD='...'
    python tools/capture_metric_response.py
"""
from __future__ import print_function

import argparse
import json
import os
import re
import sys
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RULES_PATH = os.path.join(ROOT, 'data', 'mapping', 'metric_rules.json')
MERGED_SQL_PATH = os.path.join(ROOT, 'data', 'merged_sql', 'merged_sql.json')
PLAN_PATH = os.path.join(ROOT, 'data', 'customer_metric_query_plan.json')
OUT_DIR = os.path.join(ROOT, 'data', 'metric_response')

BASE_URL = os.environ.get('TPOPS_BASE_URL', 'https://172.16.103.193:8002').rstrip('/')
TPOPS_USER = os.environ.get('TPOPS_USER', '')
TPOPS_PASSWORD = os.environ.get('TPOPS_PASSWORD', '')

CMD_ID = '73722acc-bff6-4d04-adc0-bc1e1fa1eb7d'   # 平台内置命令 general_single_sql_query
DEFAULT_INSTANCE = '83cd68f2e4cb40b4b7e69240479ffa26in14'   # gauss-6374，3 个 master
MONITOR_INSTANCES = [
    '83cd68f2e4cb40b4b7e69240479ffa26in14',
    'c35b8d078cae4a9eb05d616ed5cf032fin14',
]
MONITOR_WINDOW_MS = 2 * 3600 * 1000
MONITOR_END_LAG_MS = 5 * 60 * 1000
SLOW_SQL_WINDOW_MS = 24 * 3600 * 1000
TOP_SQL_WINDOW_MS = 7 * 24 * 3600 * 1000
MAX_ATTEMPTS = 3


def sanitize(name):
    """客户英文指标名 → 安全文件名（与 tools/verify_metric_sources.py 同一套规则）。"""
    out = name.strip()
    out = re.sub(r'[#{}()\[\]]', '', out)
    out = re.sub(r'[\\/:*?"<>|\r\n\t]', '_', out)
    out = re.sub(r'\s+', '_', out)
    out = re.sub(r'_{2,}', '_', out).strip('._')
    return out[:80] or 'metric'


class Tpops(object):
    """TPOPS HTTP 客户端（自签证书，verify=False）。"""

    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self._token = None

    def token(self):
        if self._token:
            return self._token
        if not TPOPS_USER or not TPOPS_PASSWORD:
            sys.exit('缺少 TPOPS_USER / TPOPS_PASSWORD 环境变量')
        resp = self.session.post(
            BASE_URL + '/rds/v3/auth/mini-token',
            json={'username': TPOPS_USER, 'password': TPOPS_PASSWORD}, timeout=30)
        tok = resp.headers.get('X-Mini-Token')
        if not tok:
            sys.exit('获取 X-Mini-Token 失败: HTTP %s %s' % (resp.status_code, resp.text[:200]))
        self._token = tok
        return tok

    def headers(self):
        return {'X-Mini-Token': self.token(), 'X-Language': 'zh-cn',
                'Content-Type': 'application/json'}

    def get(self, path, params=None):
        resp = self.session.get(BASE_URL + path, params=params,
                                 headers=self.headers(), timeout=120)
        resp.raise_for_status()
        return resp

    def post(self, path, body):
        resp = self.session.post(BASE_URL + path, json=body,
                                  headers=self.headers(), timeout=300)
        resp.raise_for_status()
        return resp


def build_request(method, uri, path_params=None, query_params=None, body=None):
    """构造请求对象。

    headers 只保留接口必须的几个（X-Mini-Token / X-Language，POST 再加
    Content-Type），token 用占位符 <token> —— **不落盘真实 token**。
    """
    headers = {'X-Mini-Token': '<token>', 'X-Language': 'zh-cn'}
    if body is not None:
        headers['Content-Type'] = 'application/json'
    api = '%s %s' % (method, uri)
    if query_params:
        api = '%s %s?%s' % (method, uri, '&'.join(
            '%s=%s' % (k, v) for k, v in query_params.items()))
    return {
        'api': api,
        'method': method,
        'uri': uri,
        'path_params': path_params or {},
        'query_params': query_params or {},
        'body': body,
        'headers': headers,
    }


def step(request, response):
    """一次调用 = 请求对象 + 平台原生响应体。"""
    return {'api': request['api'], 'request': request, 'response_body': response}


def capture_nodes(tp, instance_id):
    """取可执行节点（命令只允许在 master 节点执行）。"""
    uri = '/rds/gaussdb/v3/instances/%s/key-view-execute-node' % instance_id
    request = build_request('GET', uri, path_params={'instance_id': instance_id})
    resp = tp.get(uri)
    nodes = resp.json().get('nodes') or []
    masters = []
    seen = set()
    for node in nodes:
        if node.get('role') == 'master' and node.get('node_id') not in seen:
            seen.add(node.get('node_id'))
            masters.append(node)
    return step(request, resp.json()), masters


def capture_dispatch(tp, instance_id, node_ids, batch):
    """下发命令，返回 (job_id, 步骤)。"""
    uri = '/rds/gaussdb/v3/command-center/customer-commands/%s' % CMD_ID
    body = {'instance_id': instance_id, 'node_ids': list(node_ids),
            'params': [{'param_name': 'sql_text', 'param_value': batch['sql']},
                       {'param_name': 'limit', 'param_value': str(batch['limit'])}]}
    request = build_request('POST', uri, path_params={'cmd_id': CMD_ID}, body=body)
    resp = tp.post(uri, body)
    payload = resp.json()
    return payload.get('job_id'), step(request, payload)


def capture_task_records(tp, job_id):
    """轮询任务状态，结束判据用 statistics.running == 0（不看 job_status）。"""
    uri = '/rds/gaussdb/v3/command-center/command/%s/batch-exec-job/task-records' % CMD_ID
    request = build_request('GET', uri, path_params={'cmd_id': CMD_ID},
                            query_params={'job_id': job_id})
    payload = None
    for _ in range(120):
        resp = tp.get(uri, params={'job_id': job_id})
        payload = resp.json()
        if (payload.get('statistics') or {}).get('running', 1) == 0:
            break
        time.sleep(0.5)
    return payload, step(request, payload)


def capture_task_details(tp, job_id, records):
    """每个 master 节点任务各取一次结果。"""
    steps = []
    for task in records.get('tasks') or []:
        task_id = task.get('task_id')
        uri = ('/rds/gaussdb/v3/command-center/command/%s/batch-exec-job/%s'
               '/task-record/%s/detail' % (CMD_ID, job_id, task_id))
        request = build_request('GET', uri, path_params={
            'cmd_id': CMD_ID, 'job_id': job_id, 'task_id': task_id})
        resp = tp.get(uri)
        steps.append(step(request, resp.json()))
    return steps


def capture_command_center(tp, instance_id, node_ids, batch):
    """命令中心 SQL 通道：下发 → 轮询 → 取结果（失败自动重试）。"""
    last_err = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            job_id, dispatch = capture_dispatch(tp, instance_id, node_ids, batch)
            if not job_id:
                raise RuntimeError('下发未返回 job_id')
            records, records_step = capture_task_records(tp, job_id)
            details = capture_task_details(tp, job_id, records)
            failed = [t for t in (records.get('tasks') or []) if t.get('status') != 'succeed']
            if failed:
                raise RuntimeError('节点任务未成功: %s' % failed[0].get('error_msg'))
            return [dispatch, records_step] + details
        except Exception as exc:                      # noqa: BLE001 - 重试后原样抛出
            last_err = exc
            time.sleep(1)
    raise RuntimeError('命令中心 SQL 执行失败（重试 %d 次）: %s' % (MAX_ATTEMPTS, last_err))


def capture_monitor(tp, metric_name):
    """监控指标接口：取最新点 + 取带真实时间戳的时序。

    ① `is_latest_point_only=true` —— 平台只返回**最新点**（值对，但多数指标
       timestamp=0，实测 6 个指标里 5 个如此）
    ② `is_latest_point_only=false` + ≥1h 窗口 —— 完整时序，末尾即同一个最新点，
       但带真实时间戳

    两次都用同一个 end_time，所以两个响应里"最新点"的值一致；两个响应都是平台原生响应，
    不做任何裁剪。
    """
    uri = '/rds/v3/monitor/metrics'
    end = int(time.time() * 1000) - MONITOR_END_LAG_MS
    base = {'start_time': end - MONITOR_WINDOW_MS, 'end_time': end,
            'instances': [{'instance_id': i} for i in MONITOR_INSTANCES],
            'metric_names': [metric_name]}
    steps = []
    for latest_only in ('true', 'false'):
        body = dict(base, is_latest_point_only=latest_only)
        request = build_request('POST', uri, body=body)
        resp = tp.post(uri, body)
        steps.append(step(request, resp.json()))
    return steps


def capture_slow_sql(tp, instance_id, node_ids):
    """慢 SQL 集合接口。"""
    uri = '/rds/gaussdb/v3/instances/%s/slow-sql-list' % instance_id
    end = int(time.time() * 1000)
    body = {'start_time': end - SLOW_SQL_WINDOW_MS, 'end_time': end,
            'instance_id': instance_id, 'threshold': 1000, 'threshold_unit': 'ms',
            'node_ids': list(node_ids)}
    request = build_request('POST', uri, path_params={'instance_id': instance_id}, body=body)
    resp = tp.post(uri, body)
    return [step(request, resp.json())]


def capture_top_sql(tp, instance_id, node_ids):
    """Top SQL 列表接口。"""
    uri = '/rds/gaussdb/v3/instances/%s/top-sql-list' % instance_id
    end = int(time.time() * 1000)
    body = {'instance_id': instance_id, 'node_ids': list(node_ids),
            'start_time': end - TOP_SQL_WINDOW_MS, 'end_time': end}
    request = build_request('POST', uri, path_params={'instance_id': instance_id}, body=body)
    resp = tp.post(uri, body)
    return [step(request, resp.json())]


def capture_instances(tp):
    """实例列表接口。"""
    uri = '/rds/v3.2/instances'
    request = build_request('GET', uri, query_params={'limit': 100, 'offset': 0})
    resp = tp.get(uri, params={'limit': 100, 'offset': 0})
    return [step(request, resp.json())]


def load_json(path):
    with open(path, 'r') as fp:
        return json.load(fp)


def plan_metrics(plan):
    """按方案顺序取出 (序号, 指标) —— 序号沿用 data/metric_verification/ 的编号。"""
    items = []
    for category in plan['categories']:
        for metric in category['metrics']:
            items.append((category['category'], metric))
    return items


def build_captures(tp, instance_id, plan, rules_by_code, batches):
    """实跑一遍所有通道，返回 {指标英文名: [step, ...]}。"""
    node_step, masters = capture_nodes(tp, instance_id)
    node_ids = [m['node_id'] for m in masters]
    print('实例 %s —— %d 个 master: %s' % (
        instance_id, len(node_ids), ', '.join(m.get('node_name', '') for m in masters)))

    captures = {}
    batch_chains = {}
    for batch in batches:
        chain = capture_command_center(tp, instance_id, node_ids, batch)
        batch_chains[batch['sql_key']] = [node_step] + chain
        print('  合并 SQL %-18s %d 个 API 响应' % (batch['sql_key'], len(chain)))

    for _, metric in plan_metrics(plan):
        code = metric['customer_metric']
        rule = rules_by_code.get(code)
        if rule is None or not rule.get('enabled'):
            continue
        source = rule['source']
        method = source.get('method')
        if method == '命令中心SQL':
            captures[code] = batch_chains[source['sql_key']]
        elif method == '监控指标接口':
            captures[code] = capture_monitor(tp, source['tpops_source'])
        elif method == '诊断优化接口':
            if metric['query']['api'].endswith('top-sql-list'):
                captures[code] = capture_top_sql(tp, instance_id, node_ids)
            else:
                captures[code] = capture_slow_sql(tp, instance_id, node_ids)
        elif method == '实例接口':
            captures[code] = capture_instances(tp)
    return captures


def write_outputs(captures, plan, out_dir, meta):
    """落盘每个指标一个文件 + manifest 索引。"""
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    manifest = []
    seq = 0
    for category, metric in plan_metrics(plan):
        code = metric['customer_metric']
        chain = captures.get(code)
        if chain is None:
            continue
        seq += 1
        filename = '%03d_%s.json' % (seq, sanitize(code))
        path = os.path.join(out_dir, filename)
        with open(path, 'w') as fp:
            json.dump(chain, fp, ensure_ascii=False, indent=2)
        manifest.append({
            'seq': seq,
            'customer_metric': code,
            'customer_metric_name': metric.get('customer_metric_name'),
            'customer_category': category,
            'channel': metric.get('source'),
            'file': filename,
            'api_count': len(chain),
            'apis': [s['api'] for s in chain],
            'requests': [s['request'] for s in chain],
        })
    meta['metrics'] = manifest
    meta['metric_count'] = len(manifest)
    with open(os.path.join(out_dir, '000manifest.json'), 'w') as fp:
        json.dump(meta, fp, ensure_ascii=False, indent=2)
    return manifest


def main():
    parser = argparse.ArgumentParser(description='抓取每个客户指标的原生 API 响应')
    parser.add_argument('--instance-id', default=DEFAULT_INSTANCE, help='实测实例 ID')
    parser.add_argument('--out-dir', default=OUT_DIR, help='输出目录')
    args = parser.parse_args()

    rules = load_json(RULES_PATH)['rules']
    rules_by_code = dict((r['metric_code'], r) for r in rules)
    batches = load_json(MERGED_SQL_PATH)['batches']
    plan = load_json(PLAN_PATH)

    tp = Tpops()
    started = time.time()
    captures = build_captures(tp, args.instance_id, plan, rules_by_code, batches)

    meta = {
        'generated_at': time.strftime('%Y-%m-%d %H:%M:%S %z'),
        'instance_id': args.instance_id,
        'cmd_id': CMD_ID,
        'elapsed_seconds': round(time.time() - started, 1),
        'note': '命令中心 SQL 走 11 条合并 SQL，同一批指标共用同一次调用的响应',
    }
    manifest = write_outputs(captures, plan, args.out_dir, meta)
    print('落盘 %d 个指标文件 -> %s（耗时 %.1fs）' % (
        len(manifest), args.out_dir, meta['elapsed_seconds']))


if __name__ == '__main__':
    main()
