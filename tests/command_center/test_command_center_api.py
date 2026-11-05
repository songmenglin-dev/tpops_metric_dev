#!/usr/bin/env python3
"""统一命令中心（command-center）API 实测脚本。

对平台暴露的 5 个命令中心接口逐个发起真实请求，把完整响应原样落盘，
不做任何裁剪或改写 —— 目的是让每个接口的真实行为可被复核。

覆盖的接口：

  1. GET  /rds/gaussdb/v3/command-center/customer-commands
     查询统一命令列表（分页 / 过滤 / 排序）
  2. GET  /rds/gaussdb/v3/command-center/commands/{cmd_id}
     查询单条统一命令详情
  3. POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}
     执行单条统一命令
  4. GET  /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records
     查询统一命令实例任务状态及节点任务记录列表
  5. GET  /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}/task-record/{task_id}/detail
     查询统一命令节点任务结果详情

执行前置（不属于命令中心本身，但下发命令必须依赖）：
  GET /rds/v3.2/instances                                   实例列表
  GET /rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node
                                                          节点清单（node_id / role / type / component_id）

用法：
    export TPOPS_USER=sml TPOPS_PASSWORD='...'
    python tests/command_center/test_command_center_api.py

    # 只跑列表与详情（不执行任何命令，无需担心副作用）
    python tests/command_center/test_command_center_api.py --list-only

    # 指定实例与节点
    python tests/command_center/test_command_center_api.py \\
        --instance-id <iid> --node-id <nid>

输出（tests/command_center/results/）：
    00_auth.json                 认证响应（含 token 所在位置，不含 token 明文）
    01_instances.json            实例列表
    02_nodes_<instance_id>.json  节点清单
    03_command_list.json         全部 142 条命令（分页遍历后的合并结果）
    03_command_list_raw/         列表接口的逐页原始响应
    04_command_detail/<cmd_id>.json
                                每条命令的详情（142 个文件）
    05_execute_<cmd_id>.json     下发命令的响应
    06_task_records_<job_id>.json 任务状态列表
    07_task_result_<task_id>.json 任务结果详情
    _summary.json                本次实测的统计与结论
"""

import argparse
import json
import os
import sys
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RESULTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

DEFAULT_BASE_URL = "https://172.16.103.193:8002"

# 通用 SQL 查询命令：可执行任意 SQL，是采集 pg_stat 类指标的通道
CMD_SQL_ID = "73722acc-bff6-4d04-adc0-bc1e1fa1eb7d"

LIST_PAGE_SIZE = 100        # 接口规定 limit 取值范围 0~100
TASK_POLL_INTERVAL = 3      # 轮询间隔（秒）
TASK_POLL_TIMEOUT = 120     # 轮询总超时（秒）
HTTP_TIMEOUT = 60


class TpopsClient(object):
    """命令中心接口的最小客户端。"""

    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.session = requests.Session()
        self._token = None

    def token(self):
        if self._token is None:
            resp = self.session.post(
                self.base_url + "/rds/v3/auth/mini-token",
                json={"username": self.username, "password": self.password},
                timeout=HTTP_TIMEOUT,
                verify=False,
            )
            resp.raise_for_status()
            self._token = resp.headers.get("X-Mini-Token")
            if not self._token:
                raise RuntimeError("响应头里没有 X-Mini-Token")
        return self._token

    def _headers(self):
        return {"X-Mini-Token": self.token(), "X-Language": "zh-cn"}

    def _url(self, path):
        return self.base_url + "/rds" + path

    def get(self, path, params=None):
        return self.session.get(self._url(path), params=params,
                                headers=self._headers(), timeout=HTTP_TIMEOUT, verify=False)

    def post(self, path, body):
        return self.session.post(self._url(path), json=body,
                                 headers=self._headers(), timeout=HTTP_TIMEOUT, verify=False)


SECRET_KEYS = ("token", "password", "passwd", "secret")


def redact(payload):
    """把凭据类字段替换掉，避免把可用 token 落盘。

    认证接口的响应头与响应体里都带着 X-Mini-Token —— 那是可直接复用的凭据，
    落盘就等于把令牌提交进仓库。其余接口的响应体不含凭据，保持原样。
    """
    if isinstance(payload, dict):
        out = {}
        for key, value in payload.items():
            lowered = key.lower()
            if any(s in lowered for s in SECRET_KEYS):
                out[key] = "<REDACTED>"
            else:
                out[key] = redact(value)
        return out
    if isinstance(payload, list):
        return [redact(item) for item in payload]
    return payload


def write_json(path, payload):
    """写盘：完整保留响应体，仅对凭据类字段做脱敏。"""
    directory = os.path.dirname(path)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(redact(payload), fh, ensure_ascii=False, indent=2)
    size_kb = os.path.getsize(path) / 1024
    print("    wrote %s (%.0f KB)" % (os.path.relpath(path, ROOT), size_kb))


def record_response(path, resp, extra=None, request_body=None):
    """把 HTTP 响应完整记录下来：状态码 + 头 + 原始响应体。"""
    try:
        body = resp.json()
    except ValueError:
        body = resp.text
    payload = {
        "request": {
            "method": resp.request.method,
            "url": resp.request.url,
        },
        "response": {
            "status_code": resp.status_code,
            "headers": dict(resp.headers),
            "body": body,
        },
    }
    if request_body is not None:
        payload["request"]["body"] = request_body
    if extra:
        payload["extra"] = extra
    write_json(path, payload)
    return body


# --------------------------------------------------------------------------
# 接口 1：查询统一命令列表
# --------------------------------------------------------------------------
def test_command_list(client):
    """分页遍历全部命令，同时验证过滤与排序参数。"""
    print("\n[1] 查询统一命令列表 GET /gaussdb/v3/command-center/customer-commands")

    raw_dir = os.path.join(RESULTS, "03_command_list_raw")
    commands = []
    pages = []
    offset = 0
    while True:
        resp = client.get("/gaussdb/v3/command-center/customer-commands",
                          params={"offset": offset, "limit": LIST_PAGE_SIZE,
                                  "sort_key": "cmd_name", "sort_dir": "asc"})
        body = record_response(os.path.join(raw_dir, "page_%03d.json" % (offset // LIST_PAGE_SIZE)),
                                resp)
        pages.append(body)
        data = body.get("data", [])
        commands.extend(data)
        total = body.get("total_count", 0)
        print("    offset=%-4d limit=%-4d -> %d 条（累计 %d / %d）"
              % (offset, LIST_PAGE_SIZE, len(data), len(commands), total))
        offset += LIST_PAGE_SIZE
        if not data or len(commands) >= total:
            break

    # 过滤参数验证：按类型过滤，条数应与总数相符
    filters = {}
    for key, value in (("cmd_type", "SQL"), ("cmd_type", "SHELL"), ("cmd_type", "PYTHON"),
                       ("source", "default"), ("source", "custom"),
                       ("status", "enabled"), ("status", "disabled"),
                       ("call_type", "sync"), ("call_type", "async")):
        resp = client.get("/gaussdb/v3/command-center/customer-commands",
                          params={"cmd_type": value} if key == "cmd_type" else {key: value,
                                                                                "limit": LIST_PAGE_SIZE})
        body = record_response(os.path.join(RESULTS, "03_command_list_raw",
                                            "filter_%s_%s.json" % (key, value)), resp)
        filters["%s=%s" % (key, value)] = body.get("total_count")

    write_json(os.path.join(RESULTS, "03_command_list.json"), {
        "total_count": pages[0].get("total_count") if pages else 0,
        "fetched": len(commands),
        "filters_total_count": filters,
        "commands": commands,
    })
    return commands, filters


# --------------------------------------------------------------------------
# 接口 2：查询单条统一命令详情
# --------------------------------------------------------------------------
def test_command_details(client, commands):
    """逐条拉取命令详情。"""
    print("\n[2] 查询命令详情 GET /gaussdb/v3/command-center/commands/{cmd_id}")
    detail_dir = os.path.join(RESULTS, "04_command_detail")
    details = {}
    failures = []
    for index, cmd in enumerate(commands, start=1):
        cmd_id = cmd.get("cmd_id")
        resp = client.get("/gaussdb/v3/command-center/commands/" + cmd_id)
        if resp.status_code != 200:
            failures.append({"cmd_id": cmd_id, "cmd_name": cmd.get("cmd_name"),
                             "status_code": resp.status_code})
            record_response(os.path.join(detail_dir, cmd_id + ".json"), resp)
            continue
        body = record_response(os.path.join(detail_dir, cmd_id + ".json"), resp)
        details[cmd_id] = body
        if index % 20 == 0 or index == len(commands):
            print("    %d / %d" % (index, len(commands)))
    print("    成功 %d 条，失败 %d 条" % (len(details), len(failures)))
    return details, failures


# --------------------------------------------------------------------------
# 接口 3/4/5：执行命令 → 查任务状态 → 取任务结果
# --------------------------------------------------------------------------
def execute_and_wait(client, cmd_id, instance_id, node_ids, params, tag):
    """完整跑一遍下发→轮询→取结果的链路，三个接口的响应全部落盘。"""
    prefix = os.path.join(RESULTS, "05_execute_%s.json" % tag)

    resp = client.post("/gaussdb/v3/command-center/customer-commands/" + cmd_id, {
        "instance_id": instance_id,
        "node_ids": node_ids,
        "params": params,
    })
    body = record_response(prefix, resp, extra={"stage": "execute", "cmd_id": cmd_id},
                           request_body={"instance_id": instance_id,
                                         "node_ids": node_ids,
                                         "params": params})
    if resp.status_code != 200:
        return {"execute": body, "execute_status_code": resp.status_code,
                "records": None, "result": None, "ok": False}

    job_id = body.get("job_id")
    node_tasks = body.get("node_tasks", [])
    print("    下发成功 job_id=%s 节点任务 %d 个" % (job_id, len(node_tasks)))

    # 轮询任务状态
    started = time.time()
    records = None
    while time.time() - started < TASK_POLL_TIMEOUT:
        resp = client.get("/gaussdb/v3/command-center/command/" + cmd_id
                          + "/batch-exec-job/task-records",
                          params={"job_id": job_id, "offset": 0, "limit": 100})
        records = record_response(os.path.join(RESULTS, "06_task_records_%s.json" % tag),
                                  resp, extra={"stage": "task-records", "job_id": job_id})
        tasks = records.get("tasks", [])
        stat = records.get("statistics", {})
        print("    轮询 %2ds: job_status=%-8s 节点状态 %s"
              % (time.time() - started, records.get("job_status"),
                 [t.get("status") for t in tasks]))
        if stat.get("running", 0) == 0 and tasks:
            break
        time.sleep(TASK_POLL_INTERVAL)

    # 取任务结果
    result = None
    for task in (records or {}).get("tasks", []):
        task_id = task.get("task_id")
        resp = client.get("/gaussdb/v3/command-center/command/" + cmd_id
                          + "/batch-exec-job/" + job_id + "/task-record/" + task_id + "/detail")
        result = record_response(os.path.join(RESULTS, "07_task_result_%s.json" % tag),
                                 resp, extra={"stage": "task-result", "task_id": task_id})
        print("    结果 status=%s 输出 %d 字节"
              % (result.get("status"), len(str(result.get("result", "")))))
    return {"execute": body, "execute_status_code": 200,
            "records": records, "result": result, "ok": True}


def test_execution(client, instance_id, node_id, cmd_id, sql_text, tag):
    """对一条命令做完整的执行链路实测。"""
    print("\n[3/4/5] 执行链路 %s cmd_id=%s" % (tag, cmd_id))
    return execute_and_wait(client, cmd_id, instance_id, [node_id], [
        {"param_name": "sql_text", "param_value": sql_text},
        {"param_name": "limit", "param_value": "10"},
    ], tag)


# --------------------------------------------------------------------------
# 覆盖全部命令类型的执行矩阵
# --------------------------------------------------------------------------
def pick_commands_by_type(commands, details, instance_mode):
    """按 (cmd_type, call_type) 每组挑一条可执行的命令。

    挑选标准：status=enabled、cluster_mode 允许当前实例形态、
    limit_node_types 允许所选节点的角色。
    """
    node_role = "master"
    picked = {}
    for cmd in commands:
        key = (cmd["cmd_type"], cmd["call_type"])
        if key in picked:
            continue
        if cmd.get("status") != "enabled":
            continue
        detail = details.get(cmd["cmd_id"])
        if not detail:
            continue
        constraint = detail.get("constraint_config") or {}
        mode = (constraint.get("cluster_mode") or "All").lower()
        if mode not in ("all", instance_mode.lower()):
            continue
        allowed_roles = constraint.get("limit_node_types") or []
        if allowed_roles and node_role not in allowed_roles:
            continue
        picked[key] = cmd
    return picked


def build_params_for(cmd, detail):
    """按命令的参数定义构造入参。

    必填参数依次尝试：默认值 → 枚举首个值 → 按 data_type 给一个安全值。
    字符串型参数若带 pattern（如 ^[a-zA-Z0-9_-]{1,256}$），值必须匹配，
    这里用按参数名推断的探测值。
    """
    params = []
    param_config = detail.get("param_config") or {}
    for p in param_config.get("params") or []:
        if not p.get("required"):
            continue
        name = p.get("param_name")
        data_type = (p.get("data_type") or "").lower()
        default = p.get("default_value")
        if default not in (None, ""):
            value = default
        elif data_type == "enum":
            values = p.get("enum_values") or []
            value = values[0] if values else ""
        elif data_type == "integer":
            value = "10"
        elif data_type == "sql":
            value = "select 1 as probe"
        elif data_type == "boolean":
            value = "false"
        elif data_type == "string":
            value = PROBE_STRING_BY_PARAM.get(name, "probe")
        else:
            value = ""
        params.append({"param_name": name, "param_value": str(value)})
    return params


# 字符串型必填参数的探测值：按参数名给一个在测试实例上真实存在的值
PROBE_STRING_BY_PARAM = {
    "schema_name": "snapshot",
    "relname": "snap_global_os_threads",
    "database_name": "postgres",
    "database": "postgres",
    "table_name": "snap_global_os_threads",
    "table": "snap_global_os_threads",
    "user_name": "sml",
}


def test_execution_matrix(client, commands, details, instance_id, node_id, instance_mode):
    """对每种 (cmd_type, call_type) 组合实测一次执行链路。"""
    print("\n[执行矩阵] 覆盖全部命令类型")
    picked = pick_commands_by_type(commands, details, instance_mode)
    matrix_dir = os.path.join(RESULTS, "08_exec_matrix")
    results = []
    for (cmd_type, call_type), cmd in sorted(picked.items()):
        detail = details.get(cmd["cmd_id"])
        # cmd_type 大小写敏感（平台上有 "SQL" 与 "sql" 两种），文件名必须能区分；
        # 又因工作区位于 /mnt/c（大小写不敏感），不能只靠大小写区分，故用 l_/u_ 前缀
        tag = "%s%s_%s" % ("l_" if cmd_type.islower() else "u_", cmd_type.lower(), call_type)
        print("\n  --- %s / %s : %s" % (cmd_type, call_type, cmd["cmd_name"]))
        params = build_params_for(cmd, detail)
        outcome = execute_and_wait(client, cmd["cmd_id"], instance_id, [node_id],
                                   params, "matrix_" + tag)
        records = outcome.get("records") or {}
        result = outcome.get("result") or {}
        entry = {
            "cmd_id": cmd["cmd_id"],
            "cmd_name": cmd["cmd_name"],
            "cmd_type": cmd_type,
            "call_type": call_type,
            "params_sent": params,
            "execute_status_code": outcome.get("execute_status_code"),
            "job_status": records.get("job_status"),
            "node_status": [t.get("status") for t in records.get("tasks", [])],
            "node_error_msg": [t.get("error_msg") for t in records.get("tasks", [])],
            "result_status": result.get("status"),
            "out_format": result.get("out_format"),
            "out_mode": result.get("out_mode"),
            "out_type": result.get("out_type"),
            "is_redirect": result.get("is_redirect"),
            "result_preview": str(result.get("result", ""))[:400],
        }
        results.append(entry)
        write_json(os.path.join(matrix_dir, "matrix_%s.json" % tag), entry)
    return results


# --------------------------------------------------------------------------
# 异常路径：参数错误与约束冲突
# --------------------------------------------------------------------------
def test_negative_cases(client, instance_id, node_id, node_role):
    """验证接口对非法入参、非法 cmd_id、约束冲突的实际响应。"""
    print("\n[异常路径] 非法入参与约束冲突")
    negative_dir = os.path.join(RESULTS, "09_negative")
    cases = []

    def case(name, method, path, body=None, params=None, expect=None):
        if method == "GET":
            resp = client.get(path, params=params)
        else:
            resp = client.post(path, body or {})
        record_response(os.path.join(negative_dir, name + ".json"), resp,
                        extra={"case": name, "expectation": expect})
        entry = {"case": name, "method": method, "path": path,
                 "status_code": resp.status_code, "expectation": expect}
        try:
            body_json = resp.json()
            entry["error_code"] = body_json.get("error_code")
            entry["error_msg"] = body_json.get("error_msg")
        except ValueError:
            entry["body_text"] = resp.text[:200]
        cases.append(entry)
        print("    %-34s HTTP %-3s %s" % (name, resp.status_code,
                                          entry.get("error_msg") or entry.get("error_code") or ""))

    case("list_invalid_cmd_type", "GET",
         "/gaussdb/v3/command-center/customer-commands", params={"cmd_type": "BOGUS"},
         expect="非法枚举值应返回 400 参数错误")
    case("list_invalid_status", "GET",
         "/gaussdb/v3/command-center/customer-commands", params={"status": "BOGUS"},
         expect="非法枚举值应返回 400 参数错误")
    case("list_invalid_cluster_mode", "GET",
         "/gaussdb/v3/command-center/customer-commands", params={"cluster_mode": "Ha"},
         expect="cluster_mode 需 JSON 数组格式，字符串应返回 400")
    case("list_status_disabled", "GET",
         "/gaussdb/v3/command-center/customer-commands",
         params={"status": "disabled", "limit": 1},
         expect="平台无 disabled 命令时，过滤是否生效（实测返回全部 enabled）")
    case("detail_invalid_cmd_id", "GET",
         "/gaussdb/v3/command-center/commands/not-a-uuid",
         expect="非法 cmd_id 应返回 400")
    case("detail_nonexistent_cmd_id", "GET",
         "/gaussdb/v3/command-center/commands/00000000-0000-0000-0000-000000000000",
         expect="不存在的 cmd_id 应返回 404 或空结果")
    case("execute_invalid_cmd_id", "POST",
         "/gaussdb/v3/command-center/customer-commands/not-a-uuid",
         body={"instance_id": instance_id, "node_ids": [node_id], "params": []},
         expect="非法 cmd_id 应返回 400")
    case("execute_missing_instance", "POST",
         "/gaussdb/v3/command-center/customer-commands/" + CMD_SQL_ID,
         body={"node_ids": [node_id], "params": [
             {"param_name": "sql_text", "param_value": "select 1"},
             {"param_name": "limit", "param_value": "1"}]},
         expect="缺少 instance_id 应返回 400")
    case("execute_missing_node", "POST",
         "/gaussdb/v3/command-center/customer-commands/" + CMD_SQL_ID,
         body={"instance_id": instance_id, "params": [
             {"param_name": "sql_text", "param_value": "select 1"},
             {"param_name": "limit", "param_value": "1"}]},
         expect="缺少 node_ids 应返回 400")
    case("execute_bad_sql", "POST",
         "/gaussdb/v3/command-center/customer-commands/" + CMD_SQL_ID,
         body={"instance_id": instance_id, "node_ids": [node_id], "params": [
             {"param_name": "sql_text", "param_value": "drop table nonexistent_table_xyz"},
             {"param_name": "limit", "param_value": "1"}]},
         expect="非 select/show 开头的 SQL 应被参数校验拒绝")
    case("execute_limit_out_of_range", "POST",
         "/gaussdb/v3/command-center/customer-commands/" + CMD_SQL_ID,
         body={"instance_id": instance_id, "node_ids": [node_id], "params": [
             {"param_name": "sql_text", "param_value": "select 1"},
             {"param_name": "limit", "param_value": "99999"}]},
         expect="limit 超出 1~1000 应被拒绝")
    case("execute_unknown_node", "POST",
         "/gaussdb/v3/command-center/customer-commands/" + CMD_SQL_ID,
         body={"instance_id": instance_id, "node_ids": ["ffffffffffffffffffffffffffffffffno99"],
               "params": [{"param_name": "sql_text", "param_value": "select 1"},
                          {"param_name": "limit", "param_value": "1"}]},
         expect="不存在的 node_id 应被拒绝")
    case("task_records_no_job_id", "GET",
         "/gaussdb/v3/command-center/command/" + CMD_SQL_ID + "/batch-exec-job/task-records",
         expect="缺少 job_id 应返回 400")
    case("task_records_invalid_job_id", "GET",
         "/gaussdb/v3/command-center/command/" + CMD_SQL_ID + "/batch-exec-job/task-records",
         params={"job_id": "not-a-uuid"},
         expect="非法 job_id 应返回 400")
    case("task_detail_invalid_task_id", "GET",
         "/gaussdb/v3/command-center/command/" + CMD_SQL_ID
         + "/batch-exec-job/00000000-0000-0000-0000-000000000000/task-record/not-a-uuid/detail",
         expect="非法 task_id 应返回 400")

    return cases


def main():
    parser = argparse.ArgumentParser(description="统一命令中心 API 实测")
    parser.add_argument("--base-url", default=os.environ.get("TPOPS_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--user", default=os.environ.get("TPOPS_USER"))
    parser.add_argument("--password", default=os.environ.get("TPOPS_PASSWORD"))
    parser.add_argument("--instance-id", default=None, help="指定实例，不取则用第一个")
    parser.add_argument("--node-id", default=None, help="指定节点，不取则用第一个 master 节点")
    parser.add_argument("--cmd-id", default=CMD_SQL_ID, help="要执行的命令 id")
    parser.add_argument("--sql", default="select version() as v, now() as t",
                        help="通用 SQL 查询命令执行的 SQL")
    parser.add_argument("--list-only", action="store_true", help="只测列表与详情，不执行命令")
    parser.add_argument("--matrix-only", action="store_true",
                        help="只跑执行矩阵与异常路径，复用已有的列表/详情结果")
    args = parser.parse_args()

    if not args.user or not args.password:
        sys.exit("请设置 TPOPS_USER / TPOPS_PASSWORD 环境变量")

    if not os.path.isdir(RESULTS):
        os.makedirs(RESULTS)

    client = TpopsClient(args.base_url, args.user, args.password)
    summary = {
        "base_url": args.base_url,
        "tested_at": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "interfaces": {},
    }

    # --- 认证 ---
    print("\n[0] 认证 POST /rds/v3/auth/mini-token")
    resp = client.session.post(client.base_url + "/rds/v3/auth/mini-token",
                                json={"username": args.user, "password": args.password},
                                timeout=HTTP_TIMEOUT, verify=False)
    auth_body = record_response(os.path.join(RESULTS, "00_auth.json"), resp)
    summary["interfaces"]["mini-token"] = {"status_code": resp.status_code}
    client.token()
    print("    HTTP %d，取到 X-Mini-Token" % resp.status_code)

    # --- 前置：实例与节点 ---
    print("\n[前置] 实例列表 GET /rds/v3.2/instances")
    resp = client.get("/v3.2/instances", params={"limit": 100, "offset": 0})
    inst_body = record_response(os.path.join(RESULTS, "01_instances.json"), resp)
    instances = inst_body.get("instances", [])
    for inst in instances:
        print("    %s  %s  mode=%s status=%s"
              % (inst.get("id"), inst.get("name"), inst.get("mode"), inst.get("instance_status")))

    instance_id = args.instance_id or (instances[0]["id"] if instances else None)
    if not instance_id:
        sys.exit("没有可用实例")

    print("\n[前置] 节点清单 GET /gaussdb/v3/instances/{id}/key-view-execute-node")
    resp = client.get("/gaussdb/v3/instances/" + instance_id + "/key-view-execute-node")
    node_body = record_response(os.path.join(RESULTS, "02_nodes_%s.json" % instance_id), resp)
    nodes = node_body.get("nodes", [])
    for node in nodes:
        print("    %s  %-32s role=%-7s type=%-3s comp=%s"
              % (node.get("node_id"), node.get("node_name"), node.get("role"),
                 node.get("type"), node.get("component_id")))

    # 优先挑 master 节点 —— 约束配置要求 limit_node_types=[master]
    node_id = args.node_id
    if not node_id:
        masters = [n for n in nodes if n.get("role") == "master"]
        node_id = (masters or nodes)[0]["node_id"] if (masters or nodes) else None
    if not node_id:
        sys.exit("没有可用节点")

    summary["instance"] = {"instance_id": instance_id, "node_id": node_id,
                           "node_count": len(nodes)}

    # --- 接口 1、2 ---
    if args.matrix_only:
        # 复用已落盘的列表与详情结果，只重跑执行矩阵与异常路径
        print("\n[复用] 读取已有列表与详情结果")
        with open(os.path.join(RESULTS, "03_command_list.json"), encoding="utf-8") as fh:
            cached_list = json.load(fh)
        commands = cached_list["commands"]
        filters = cached_list.get("filters_total_count", {})
        details = {}
        detail_dir = os.path.join(RESULTS, "04_command_detail")
        for cmd in commands:
            path = os.path.join(detail_dir, cmd["cmd_id"] + ".json")
            if os.path.exists(path):
                with open(path, encoding="utf-8") as fh:
                    details[cmd["cmd_id"]] = json.load(fh)["response"]["body"]
        print("    列表 %d 条，详情 %d 条" % (len(commands), len(details)))
        summary["interfaces"]["commands/{cmd_id} (detail)"] = {
            "status_code": 200,
            "fetched": len(details),
            "failed": [],
            "note": "复用已有结果（--matrix-only）",
        }
    else:
        commands, filters = test_command_list(client)
        details, detail_failures = test_command_details(client, commands)
        summary["interfaces"]["commands/{cmd_id} (detail)"] = {
            "status_code": 200,
            "fetched": len(details),
            "failed": detail_failures,
        }

    summary["interfaces"]["customer-commands (list)"] = {
        "status_code": 200,
        "total_count": len(commands),
        "filters_total_count": filters,
    }

    if args.list_only:
        write_json(os.path.join(RESULTS, "_summary.json"), summary)
        print("\n完成（--list-only，未执行任何命令）")
        return

    # --- 接口 3、4、5 ---
    execution = test_execution(client, instance_id, node_id, args.cmd_id, args.sql, "sql")
    summary["interfaces"]["customer-commands/{cmd_id} (execute)"] = {
        "status_code": 200,
        "job_id": (execution.get("execute") or {}).get("job_id"),
        "ok": execution.get("ok"),
    }
    records = execution.get("records") or {}
    summary["interfaces"]["batch-exec-job/task-records"] = {
        "status_code": 200,
        "job_status": records.get("job_status"),
        "statistics": records.get("statistics"),
    }
    result = execution.get("result") or {}
    summary["interfaces"]["task-record/{task_id}/detail"] = {
        "status_code": 200,
        "status": result.get("status"),
        "out_format": result.get("out_format"),
        "out_mode": result.get("out_mode"),
        "out_type": result.get("out_type"),
    }

    # --- 执行矩阵：覆盖每种 (cmd_type, call_type) ---
    instance_mode = "Ha"
    for inst in instances:
        if inst.get("id") == instance_id:
            instance_mode = inst.get("mode") or "Ha"
    matrix = test_execution_matrix(client, commands, details, instance_id, node_id, instance_mode)
    summary["execution_matrix"] = matrix

    # --- 异常路径 ---
    negative = test_negative_cases(client, instance_id, node_id, None)
    summary["negative_cases"] = negative

    write_json(os.path.join(RESULTS, "_summary.json"), summary)
    print("\n完成，结果在 %s" % os.path.relpath(RESULTS, ROOT))


if __name__ == "__main__":
    main()
