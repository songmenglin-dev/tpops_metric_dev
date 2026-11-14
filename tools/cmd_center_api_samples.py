#!/usr/bin/env python3
"""统一命令中心三跳的 API 描述样本（纯数据，不含逻辑）。

`tools/build_metric_query_plan.py` 要为每条 B 类指标写一份「这条指标怎么取数」的
查询方案，其中三跳接口的**请求/响应样例**来自这里的 `sample_apis()`。

样本来源是 `data/metric_response/` 下的逐指标原生响应留档（一个指标一个文件，
里面是完整的三跳请求与平台原生响应体）。同一文件里的四跳互相自洽 —— 下发返回的
`job_id` / `task_id` 与后续轮询、取结果的路径参数是对得上的。

> 原先这里用的是 `data/command_center/results/` 那套留档，已按"一次性探针产物"
> 的口径删除；本模块改从仍然保留的 `data/metric_response/` 取等价样本。

用法：
    from cmd_center_api_samples import sample_apis
    apis = sample_apis("select datname, xact_commit from pg_stat_database")
"""

import json
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESPONSE_DIR = os.path.join(BASE_DIR, 'data', 'metric_response')

CMD_SQL_ID = '73722acc-bff6-4d04-adc0-bc1e1fa1eb7d'
SAMPLE_INSTANCE = '83cd68f2e4cb40b4b7e69240479ffa26in14'
_HEADERS = {'X-Mini-Token': '<token>', 'X-Language': 'zh-cn'}

# 样本文件：体积最小的一条命令中心指标留档
SAMPLE_FILE = '057_gaussdb.Prepared.json'


def _load_sample():
    """读样本留档，按调用顺序返回 [(api, response_body), ...]。"""
    path = os.path.join(RESPONSE_DIR, SAMPLE_FILE)
    with open(path, encoding='utf-8') as handle:
        calls = json.load(handle)
    return [(call.get('api') or '', call.get('response_body') or {}) for call in calls]


def _pick(calls, predicate):
    for api, body in calls:
        if predicate(api):
            return api, body
    raise RuntimeError('样本留档里找不到匹配的三跳调用')


def _base_path(url):
    """把实测 URL 里的 base 地址去掉，只留路径与查询串。"""
    for prefix in ('https://172.16.103.193:8002',):
        if url.startswith(prefix):
            return url[len(prefix):]
    return url


def sample_apis(sql_text, limit='100'):
    """生成命令中心三跳 + 取节点共四个接口的描述（含真实响应体）。

    sql_text 会写进 api2（下发）的请求体 —— 那是每条指标各自不同的部分；
    其余字段全部取自真实调用。
    """
    calls = _load_sample()

    _, resp1 = _pick(calls, lambda a: 'key-view-execute-node' in a)
    _, resp2 = _pick(calls, lambda a: 'customer-commands' in a and 'POST' in a)
    _, resp3 = _pick(calls, lambda a: 'task-records' in a)
    _, resp4 = _pick(calls, lambda a: 'task-record/' in a and 'detail' in a)

    job_id = resp2['job_id']
    task_id = resp2['node_tasks'][0]['task_id']
    node_id = resp2['node_tasks'][0]['node_id']
    master = resp1['nodes'][0]

    return {
        'api1': {
            'name': '取可执行节点',
            'purpose': '命令只允许在 master 节点执行，下发前必须先拿到 master 的 node_id',
            'method': 'GET',
            'uri': '/rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node',
            'request': {
                'headers': dict(_HEADERS),
                'path_params': {'instance_id': SAMPLE_INSTANCE},
                'query_params': {},
                'body': None,
            },
            'response': resp1,
            'response_source': f'实测留档 data/metric_response/{SAMPLE_FILE}',
            'note': (
                f"取 role=master 的节点（上面返回里第 1 个 {master['node_name']} 即 master，"
                f"node_id={node_id}）。同一 node_id 会以 CN / DN 各出现一次"
                "（gauss-6374：12 条条目 / 3 个唯一 node_id / 6 条 role=master），"
                "下发前必须按 node_id 去重；去重后一次下发全部 master 实测成功。"),
        },
        'api2': {
            'name': '下发命令（传入自定义 SQL）',
            'purpose': '把自定义 SQL 交给平台内置命令 general_single_sql_query 执行',
            'method': 'POST',
            'uri': '/rds/gaussdb/v3/command-center/customer-commands/{cmd_id}',
            'request': {
                'headers': {'X-Mini-Token': '<token>', 'X-Language': 'zh-cn',
                            'Content-Type': 'application/json'},
                'path_params': {'cmd_id': CMD_SQL_ID},
                'query_params': {},
                'body': {
                    'instance_id': SAMPLE_INSTANCE,
                    'node_ids': [node_id],
                    'params': [
                        {'param_name': 'sql_text', 'param_value': sql_text},
                        {'param_name': 'limit', 'param_value': limit},
                    ],
                },
            },
            'response': resp2,
            'response_source': f'实测留档 data/metric_response/{SAMPLE_FILE}',
            'note': 'cmd_id 是平台内置命令 general_single_sql_query 的 id'
                    '（source=default，平台 admin 创建），模板为 '
                    'select * from ({sql_text}) limit {limit}; —— '
                    '平台提供执行器，SQL 由我们填。'
                    '本步响应只含 job_id 与 task_id，**不含数据**。'
                    '本指标的 sql_text 见 request.body.params[0]。',
        },
        'api3': {
            'name': '轮询任务状态',
            'purpose': '等命令执行完，并取节点任务状态与失败原因',
            'method': 'GET',
            'uri': '/rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records',
            'request': {
                'headers': dict(_HEADERS),
                'path_params': {'cmd_id': CMD_SQL_ID},
                'query_params': {'job_id': job_id, 'offset': 0, 'limit': 100},
                'body': None,
            },
            'response': resp3,
            'response_source': f'实测留档 data/metric_response/{SAMPLE_FILE}',
            'note': '结束判据用 statistics.running == 0，**不要看 job_status** —— '
                    '真实响应里 job_status 可能是 running，但 statistics.running 已是 0、'
                    '节点状态已是 succeed。单次往返 3~8 秒。'
                    '注意 tasks[].params 里的 sql_text 被掩码为 ******（执行不受影响）。',
        },
        'api4': {
            'name': '取任务结果',
            'purpose': '取回 SQL 的执行结果 —— 业务数据在这里',
            'method': 'GET',
            'uri': ('/rds/gaussdb/v3/command-center/command/{cmd_id}'
                    '/batch-exec-job/{job_id}/task-record/{task_id}/detail'),
            'request': {
                'headers': dict(_HEADERS),
                'path_params': {'cmd_id': CMD_SQL_ID, 'job_id': job_id,
                                'task_id': task_id},
                'query_params': {},
                'body': None,
            },
            'response': resp4,
            'response_source': f'实测留档 data/metric_response/{SAMPLE_FILE}',
            'note': 'result 是 **JSON 字符串**，必须二次 json.loads 解析才是行列表。'
                    '列名取自 SQL 的 as 别名，所以自定义 SQL 必须显式写别名。'
                    '本指标的实际结果见 data/metric_response/ 对应文件。',
        },
    }


def live_apis(sql_text, limit, execute_body, records_body, result_body,
              job_id, task_id, node_id):
    """用逐指标实测的真实响应生成四个 API 的描述。

    与 sample_apis 的区别：api2/3/4 的响应体是该指标本次执行的真实响应，
    而不是留档里的样本。api1（取节点）各指标相同，仍取样本。
    """
    apis = sample_apis(sql_text, limit)
    apis['api2']['request']['body'] = execute_body
    apis['api2']['response'] = {
        'job_id': job_id,
        'node_tasks': [{'task_id': task_id, 'node_id': node_id}],
    }
    apis['api2']['response_source'] = '本指标本次实测'
    apis['api3']['request']['query_params']['job_id'] = job_id
    apis['api3']['response'] = records_body
    apis['api3']['response_source'] = '本指标本次实测'
    apis['api4']['request']['path_params']['job_id'] = job_id
    apis['api4']['request']['path_params']['task_id'] = task_id
    apis['api4']['response'] = result_body
    apis['api4']['response_source'] = '本指标本次实测'
    apis['api4']['note'] = (
        'result 是 **JSON 字符串**，必须二次 json.loads 解析才是行列表。'
        '列名取自 SQL 的 as 别名，所以自定义 SQL 必须显式写别名。'
        '上面 response 是本指标本次实测的真实结果。')
    apis['api1']['note'] += ('本次实测所用节点即上面这个 master 节点'
                             f'（node_id={node_id}）。')
    return apis


if __name__ == '__main__':
    apis = sample_apis('select datname, xact_commit from pg_stat_database')
    for key in sorted(apis):
        print(f"{key}  {apis[key]['method']} {apis[key]['uri']}")
