# API 速查

本文件由 `build_api_quickref.py` 从 `results/` 的实测响应自动生成，内容全部来自真实调用。

完整的逐次留档见 `results/`（197 个文件），每个文件含请求方法/URL、响应状态码、响应头、响应体。

---

## 1. 认证 —— 生成 mini-token

**请求** `POST https://172.16.103.193:8002/rds/v3/auth/mini-token`

> 返回头里的 `X-Mini-Token` 就是后续所有接口的令牌；服务端 session_timeout 1440 分钟

**请求头** `X-Mini-Token: <token>`、`X-Language: zh-cn`（`Content-Type: application/json`）

**响应** HTTP 200

```
user_id: str  // 089437246ced42c1a2e416d98879b764
login_ip: str  // 172.16.100.90
login_failed_attempts: int  // 0
is_first_time_login: bool  // False
last_login_statistics:
  last_login_ip: str  // 172.16.100.90
  last_login_failed_ip: NoneType  // None
  last_login_time: str  // 2026-11-05T23:24:57+0000
  last_login_failed_time: str  // 
session_timeout: int  // 1440
password_expiration_days: str  // <REDACTED>
password_expired_remaining_days: str  // <REDACTED>
password_expired_reminder: str  // <REDACTED>
x_mini_token: str  // <REDACTED>
```

---

## 2. 实例列表

**请求** `GET https://172.16.103.193:8002/rds/v3.2/instances?limit=100&offset=0`

> `instances[].id` 用作后续接口的 `instance_id`

**查询参数**

- `limit` = `100`
- `offset` = `0`

**请求头** `X-Mini-Token: <token>`、`X-Language: zh-cn`（`Content-Type: application/json`）

**响应** HTTP 200

```
total_count: int  // 2
instances:
  [数组，共 2 项]
    id: str  // 83cd68f2e4cb40b4b7e69240479ffa26in14
    name: str  // gauss-6374
    mode: str  // Combined
    created_at: str  // 2026-11-03T07:52:54+0000
    instance_status: str  // normal
    engine_name: str  // gaussdbv5
    engine_version: str  // V2.0-10.0.0
    agent_info: dict
```

---

## 3. 节点清单（执行命令的前置）

**请求** `GET https://172.16.103.193:8002/rds/gaussdb/v3/instances/83cd68f2e4cb40b4b7e69240479ffa26in14/key-view-execute-node`

> `nodes[].node_id` 用作下发命令的 `node_ids`；下发必须选 `role=master` 的节点

**请求头** `X-Mini-Token: <token>`、`X-Language: zh-cn`（`Content-Type: application/json`）

**响应** HTTP 200

```
nodes:
  [数组，共 12 项]
    node_id: str  // 3881c04403394ee1b36a2bf873e69007no14
    node_name: str  // gauss-6374_gaussdbv5_hcs2-0_0
    role: str  // master
    type: str  // CN
    distributed_id: str  // 
    component_id: str  // cn_5001
    node_ip: str  // 172.16.103.126
    detail: str  // 
```

---

## 4. 查询统一命令列表

**请求** `GET https://172.16.103.193:8002/rds/gaussdb/v3/command-center/customer-commands?offset=0&limit=100&sort_key=cmd_name&sort_dir=asc`

> `limit` 上限 100 需分页；`cmd_type` / `status` / `source` 等为可选过滤条件

**查询参数**

- `offset` = `0`
- `limit` = `100`
- `sort_key` = `cmd_name`
- `sort_dir` = `asc`

**请求头** `X-Mini-Token: <token>`、`X-Language: zh-cn`（`Content-Type: application/json`）

**响应** HTTP 200

```
total_count: int  // 142
data:
  [数组，共 100 项]
    cmd_id: str  // a4dfe0b5-ab1c-429a-ba15-a3ac44c122c7
    cmd_name: str  // DBS.After505.Cent.ERROR.AccountLock
    cmd_type: str  // SHELL
    description: str  // 查看是否有账户锁定或账户失效发生
    call_type: str  // sync
    category: str  // error_scenes
    source: str  // default
    action: str  // query
    cluster_mode: str  // Ha
    is_redirect: bool  // False
    support_compare: bool  // False
    status: str  // enabled
    …共 19 个字段
```

---

## 5. 查询单条统一命令详情

**请求** `GET https://172.16.103.193:8002/rds/gaussdb/v3/command-center/commands/73722acc-bff6-4d04-adc0-bc1e1fa1eb7d`

> 详情里的 `constraint_config`（执行约束）与 `param_config`（参数定义）决定能否下发

**请求头** `X-Mini-Token: <token>`、`X-Language: zh-cn`（`Content-Type: application/json`）

**响应** HTTP 200

```
cmd_id: str  // 73722acc-bff6-4d04-adc0-bc1e1fa1eb7d
cmd_name: str  // general_single_sql_query
cmd_type: str  // SQL
description: str  // 执行通用查询语句
call_type: str  // sync
category: str  // general_sql
source: str  // default
action: str  // query
status: str  // enabled
limit_usage:
  [数组，共 1 项]
    str  // api
creator: str  // admin
operator: str  // admin
…共 21 个字段
```

---

## 6. 执行单条统一命令

**请求** `POST https://172.16.103.193:8002/rds/gaussdb/v3/command-center/customer-commands/73722acc-bff6-4d04-adc0-bc1e1fa1eb7d`

> 下发只返回 `job_id` 与节点任务 id，不含结果；结果要轮询 7、8 两个接口取

**请求体**

```json
{
  "instance_id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
  "node_ids": [
    "3881c04403394ee1b36a2bf873e69007no14",
    "40abc563810e462a97b065e718189a59no14",
    "5a3cd027ad5c4f9bbb67d728c6d279c0no14"
  ],
  "params": [
    {
      "param_name": "sql_text",
      "param_value": "select datname, numbackends, xact_commit, xact_rollback, deadlocks, conflicts, blks_read, blks_hit, blk_read_time, blk_write_time, tup_inserted, tup_updated, tup_deleted, tup_returned, tup_fetched, temp_files, temp_bytes, case when (blks_hit + blks_read) > 0 then round(blks_hit * 100.0 / (blks_hit + blks_read), 2) else 0 end as cache_hit_ratio from pg_stat_database order by datname"
    },
    {
      "param_name": "limit",
      "param_value": "1000"
    }
  ]
}
```

**请求头** `X-Mini-Token: <token>`、`X-Language: zh-cn`（`Content-Type: application/json`）

**响应** HTTP 200

```
job_id: str  // 25c9dadf-6771-4541-bb4d-ad0dd986535f
node_tasks:
  [数组，共 3 项]
    task_id: str  // 146eede4-d9ec-41cc-89bf-214303bed25c
    node_id: str  // 3881c04403394ee1b36a2bf873e69007no14
```

---

## 7. 查询任务状态及节点任务记录

**请求** `GET https://172.16.103.193:8002/rds/gaussdb/v3/command-center/command/73722acc-bff6-4d04-adc0-bc1e1fa1eb7d/batch-exec-job/task-records?job_id=25c9dadf-6771-4541-bb4d-ad0dd986535f`

> 判断任务是否结束用 `statistics.running == 0`，不要看 `job_status`（它常停在 running）

**查询参数**

- `job_id` = `25c9dadf-6771-4541-bb4d-ad0dd986535f`

**请求头** `X-Mini-Token: <token>`、`X-Language: zh-cn`（`Content-Type: application/json`）

**响应** HTTP 200

```
total_count: int  // 3
statistics:
  total: int  // 3
  succeed: int  // 3
  failed: int  // 0
  running: int  // 0
  other: int  // 0
job_status: str  // running
tasks:
  [数组，共 3 项]
    task_id: str  // 146eede4-d9ec-41cc-89bf-214303bed25c
    instance_id: str  // 83cd68f2e4cb40b4b7e69240479ffa26in14
    instance_name: str  // gauss-6374
    node_id: str  // 3881c04403394ee1b36a2bf873e69007no14
    node_name: str  // gauss-6374_gaussdbv5_hcs2-0_0
    agent_req_id: str  // ComOmCmd-146eede4-d9ec-41cc-89bf-214303bed25c
    status: str  // succeed
    params: list
    is_redirect: bool  // False
    download_file_id: NoneType  // None
    error_msg: NoneType  // None
    start_time: int  // 1794454662209
    …共 14 个字段
```

---

## 8. 查询节点任务结果详情

**请求** `GET https://172.16.103.193:8002/rds/gaussdb/v3/command-center/command/73722acc-bff6-4d04-adc0-bc1e1fa1eb7d/batch-exec-job/25c9dadf-6771-4541-bb4d-ad0dd986535f/task-record/146eede4-d9ec-41cc-89bf-214303bed25c/detail`

> `result` 字段是 JSON 字符串，需二次 `json.loads` 解析

**请求头** `X-Mini-Token: <token>`、`X-Language: zh-cn`（`Content-Type: application/json`）

**响应** HTTP 200

```
node_id: str  // 3881c04403394ee1b36a2bf873e69007no14
node_name: str  // gauss-6374_gaussdbv5_hcs2-0_0
status: str  // succeed
is_redirect: bool  // False
result: str  // [{"datname":"lz","numbackends":"0","xact_commit":"21420","xa…
out_format: str  // json
out_mode: str  // array
out_type: str  // map
download_file_id: NoneType  // None
```

---
