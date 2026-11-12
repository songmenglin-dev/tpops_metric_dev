#!/usr/bin/env python3
"""命令中心取数链路的 API 描述（含实测真实响应）。

命令中心是异步任务模型，取一条自定义 SQL 的数据需要三跳调用：

    api1  GET  .../instances/{instance_id}/key-view-execute-node      取 master 节点
    api2  POST .../customer-commands/{cmd_id}                          下发 SQL → job_id
    api3  GET  .../batch-exec-job/task-records?job_id=                 轮询 → statistics.running
    api4  GET  .../batch-exec-job/{job_id}/task-record/{task_id}/detail 取数据 → result

本模块从 data/command_center/results/ 的实测留档里读出**真实响应**，
供方案文档与逐指标验证文件引用 —— 避免文档里出现编造的占位符。

实测留档来源：data/command_center/test_command_center_api.py（2026-11-06 真实调用）
"""

import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, "data", "command_center", "results")

CMD_SQL_ID = "73722acc-bff6-4d04-adc0-bc1e1fa1eb7d"   # general_single_sql_query

# 实测用的实例（分布式版，mode=Combined，3 个 CN 均为 master）与 master 节点，与留档中的响应一致
SAMPLE_INSTANCE = "83cd68f2e4cb40b4b7e69240479ffa26in14"

_CAPTURES = {
    "api1": "02_nodes_%s.json" % SAMPLE_INSTANCE,
    "api2": "05_execute_sql_6374.json",
    "api3": "06_task_records_sql_6374.json",
    "api4": "07_task_result_sql_6374.json",
}

_HEADERS = {"X-Mini-Token": "<token>", "X-Language": "zh-cn"}


def _load(name):
    """读一条实测留档，返回 (请求, 响应体)。"""
    path = os.path.join(RESULTS, _CAPTURES[name])
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    return raw.get("request") or {}, (raw.get("response") or {}).get("body")


def _url_to_path(url):
    """把实测 URL 里的 base 地址去掉，只留路径与查询串。"""
    for prefix in ("https://172.16.103.193:8002",):
        if url.startswith(prefix):
            return url[len(prefix):]
    return url


def sample_apis(sql_text, limit="100"):
    """用实测留档生成四个 API 的描述（含真实响应体）。

    sql_text 会写进 api2 的请求体 —— 它是每条指标各自不同的部分；
    其余字段全部取自真实调用。

    注意：api1/3/4 的响应体来自留档里的那一次执行（sql_text 为
    `select version() as v, now() as t`），各条指标共用同一份样本。
    逐指标的真实响应见 data/metric_verification/*.json。
    """
    req1, resp1 = _load("api1")
    req2, resp2 = _load("api2")
    req3, resp3 = _load("api3")
    req4, resp4 = _load("api4")

    job_id = resp2["job_id"]
    task_id = resp2["node_tasks"][0]["task_id"]
    node_id = resp2["node_tasks"][0]["node_id"]

    return {
        "api1": {
            "name": "取可执行节点",
            "purpose": "命令只允许在 master 节点执行，下发前必须先拿到 master 的 node_id",
            "method": "GET",
            "uri": "/rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node",
            "request": {
                "headers": dict(_HEADERS),
                "path_params": {"instance_id": SAMPLE_INSTANCE},
                "query_params": {},
                "body": None,
            },
            "response": resp1,
            "response_source": "实测留档 data/command_center/results/02_nodes_%s.json" % SAMPLE_INSTANCE,
            "note": "取 role=master 的节点（上面返回里第 1 个 %s 即 master，"
                    "node_id=%s）。同一 node_id 会以 CN / DN 各出现一次"
                    "（gauss-6374：12 条条目 / 3 个唯一 node_id / 6 条 role=master），"
                    "下发前必须按 node_id 去重；去重后一次下发全部 master 实测成功。"
                    % (resp1["nodes"][0]["node_name"], node_id),
        },
        "api2": {
            "name": "下发命令（传入自定义 SQL）",
            "purpose": "把自定义 SQL 交给平台内置命令 general_single_sql_query 执行",
            "method": "POST",
            "uri": "/rds/gaussdb/v3/command-center/customer-commands/{cmd_id}",
            "request": {
                "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn",
                            "Content-Type": "application/json"},
                "path_params": {"cmd_id": CMD_SQL_ID},
                "query_params": {},
                "body": {
                    "instance_id": SAMPLE_INSTANCE,
                    "node_ids": [node_id],
                    "params": [
                        {"param_name": "sql_text", "param_value": sql_text},
                        {"param_name": "limit", "param_value": limit},
                    ],
                },
            },
            "response": resp2,
            "response_source": "实测留档 data/command_center/results/05_execute_sql.json",
            "note": "cmd_id 是平台内置命令 general_single_sql_query 的 id"
                    "（source=default，平台 admin 创建），模板为 "
                    "select * from ({sql_text}) limit {limit}; —— "
                    "平台提供执行器，SQL 由我们填。"
                    "本步响应只含 job_id 与 task_id，**不含数据**。"
                    "上面 response 是样本（该次执行的 sql_text 为 "
                    "`select version() as v, now() as t`）；"
                    "本指标的 sql_text 见 request.body.params[0]。",
        },
        "api3": {
            "name": "轮询任务状态",
            "purpose": "等命令执行完，并取节点任务状态与失败原因",
            "method": "GET",
            "uri": "/rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records",
            "request": {
                "headers": dict(_HEADERS),
                "path_params": {"cmd_id": CMD_SQL_ID},
                "query_params": {"job_id": job_id, "offset": 0, "limit": 100},
                "body": None,
            },
            "response": resp3,
            "response_source": "实测留档 data/command_center/results/06_task_records_sql.json",
            "note": "结束判据用 statistics.running == 0，**不要看 job_status** —— "
                    "上面真实响应里 job_status 仍是 running，但 statistics.running 已是 0、"
                    "节点状态已是 succeed。单次往返 3~8 秒。"
                    "注意 tasks[].params 里的 sql_text 被掩码为 ******（执行不受影响）。",
        },
        "api4": {
            "name": "取任务结果",
            "purpose": "取回 SQL 的执行结果 —— 业务数据在这里",
            "method": "GET",
            "uri": ("/rds/gaussdb/v3/command-center/command/{cmd_id}"
                    "/batch-exec-job/{job_id}/task-record/{task_id}/detail"),
            "request": {
                "headers": dict(_HEADERS),
                "path_params": {"cmd_id": CMD_SQL_ID, "job_id": job_id,
                                "task_id": task_id},
                "query_params": {},
                "body": None,
            },
            "response": resp4,
            "response_source": "实测留档 data/command_center/results/07_task_result_sql.json",
            "note": "result 是 **JSON 字符串**，必须二次 json.loads 解析才是行列表。"
                    "列名取自 SQL 的 as 别名，所以自定义 SQL 必须显式写别名。"
                    "上面 response 的 result 是样本数据"
                    "（`select version() as v, now() as t` 的输出，"
                    "两条列 v/t）；本指标的实际结果见"
                    "data/metric_verification/ 对应文件的 verification.actual_data。",
        },
    }


def live_apis(sql_text, limit, execute_body, records_body, result_body,
              job_id, task_id, node_id):
    """用逐指标实测的真实响应生成四个 API 的描述。

    与 sample_apis 的区别：api2/3/4 的响应体是该指标本次执行的真实响应，
    而不是留档里的样本。api1（取节点）各指标相同，仍取样本。
    """
    apis = sample_apis(sql_text, limit)
    apis["api2"]["request"]["body"] = execute_body
    apis["api2"]["response"] = {
        "job_id": job_id,
        "node_tasks": [{"task_id": task_id, "node_id": node_id}],
    }
    apis["api2"]["response_source"] = "本指标本次实测"
    apis["api3"]["request"]["query_params"]["job_id"] = job_id
    apis["api3"]["response"] = records_body
    apis["api3"]["response_source"] = "本指标本次实测"
    apis["api4"]["request"]["path_params"]["job_id"] = job_id
    apis["api4"]["request"]["path_params"]["task_id"] = task_id
    apis["api4"]["response"] = result_body
    apis["api4"]["response_source"] = "本指标本次实测"
    apis["api4"]["note"] = (
        "result 是 **JSON 字符串**，必须二次 json.loads 解析才是行列表。"
        "列名取自 SQL 的 as 别名，所以自定义 SQL 必须显式写别名。"
        "上面 response 是本指标本次实测的真实结果。")
    apis["api1"]["note"] += ("本次实测所用节点即上面这个 master 节点"
                             "（node_id=%s）。" % node_id)
    return apis
