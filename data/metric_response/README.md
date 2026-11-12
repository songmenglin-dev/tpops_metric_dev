# metric_response —— 每个客户指标的原生 API 响应

> 目录约定：**一个指标一个文件**，文件内容是该指标取数链路上各接口的
> **完整请求对象 + 平台原生响应体**，按调用顺序排列。响应体不做任何加工/改写
> （`result` 字段仍是平台返回的 JSON 字符串）。

## 文件格式

顶层是一个数组，每个元素一次接口调用：

```json
[
  {
    "api": "POST /rds/gaussdb/v3/command-center/customer-commands/73722acc-bff6-4d04-adc0-bc1e1fa1eb7d",
    "request": {
      "api": "POST /rds/gaussdb/v3/command-center/customer-commands/73722acc-bff6-4d04-adc0-bc1e1fa1eb7d",
      "method": "POST",
      "uri": "/rds/gaussdb/v3/command-center/customer-commands/73722acc-bff6-4d04-adc0-bc1e1fa1eb7d",
      "path_params": {"cmd_id": "73722acc-bff6-4d04-adc0-bc1e1fa1eb7d"},
      "query_params": {},
      "body": {
        "instance_id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
        "node_ids": ["3881c044...", "40abc563...", "5a3cd027..."],
        "params": [
          {"param_name": "sql_text", "param_value": "select datname, numbackends, ... from pg_stat_database order by datname"},
          {"param_name": "limit", "param_value": "1000"}
        ]
      },
      "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn", "Content-Type": "application/json"}
    },
    "response_body": {
      "job_id": "81710013-4ac3-4887-8850-2e035a6f93aa",
      "node_tasks": [{"task_id": "56c99af5-...", "node_id": "3881c044..."}]
    }
  },
  {
    "api": "GET /rds/gaussdb/v3/command-center/command/73722acc-.../batch-exec-job/81710013.../task-record/56c99af5.../detail",
    "request": {
      "method": "GET",
      "uri": "/rds/gaussdb/v3/command-center/command/73722acc-.../batch-exec-job/81710013.../task-record/56c99af5.../detail",
      "path_params": {"cmd_id": "73722acc-...", "job_id": "81710013...", "task_id": "56c99af5..."},
      "query_params": {},
      "body": null,
      "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"}
    },
    "response_body": {
      "node_name": "gauss-6374_gaussdbv5_hcs2-0_0",
      "status": "succeed",
      "result": "[{\"datname\": \"postgres\", \"xact_commit\": \"20749\", ...}]"
    }
  }
]
```

请求对象各字段：

| 字段 | 含义 |
|---|---|
| `api` | `"<METHOD> <实际 URI>"`（URI 里的 job_id / task_id 是真实取值） |
| `method` | `GET` / `POST` |
| `uri` | 实际路径（不含 query string），路径参数已代入真实值 |
| `path_params` | 路径参数（`instance_id` / `cmd_id` / `job_id` / `task_id`） |
| `query_params` | 查询参数（`job_id` / `limit` / `offset`） |
| `body` | 请求体（GET 为 `null`）—— **含真实的 `sql_text`、时间窗口、metric_names** |
| `headers` | 只保留接口必须的几个：`X-Mini-Token` / `X-Language`（POST 再加 `Content-Type`） |

> `X-Mini-Token` 存的是占位符 `<token>`，**真实 token 不落盘**。

## 通道与文件构成

| 通道 | 文件数 | 每个文件里的 API（按顺序） |
|---|---|---|
| 命令中心 SQL | 82 | ① `GET …/key-view-execute-node` 取可执行节点<br>② `POST …/customer-commands/{cmd_id}` 下发命令<br>③ `GET …/batch-exec-job/task-records?job_id=…` 轮询任务状态<br>④ `GET …/batch-exec-job/{job_id}/task-record/{task_id}/detail` 取任务结果（**每个 master 一条**） |
| 监控指标接口 | 11 | ① `POST /rds/v3/monitor/metrics`（`is_latest_point_only=true`，**只要最新点**）<br>② `POST /rds/v3/monitor/metrics`（`is_latest_point_only=false` + ≥1h 窗口，**带真实时间戳的时序**） |
| 诊断优化接口 | 4 | ① `POST …/slow-sql-list`（3 个）/ `POST …/top-sql-list`（1 个） |
| 实例接口 | 1 | ① `GET /rds/v3.2/instances?limit=100&offset=0` |

3 个判定为**无法提供**的指标不建文件（原因见 `data/customer_metric_unsupported.csv`）。

### 为什么监控指标要发两次

实测（2026-11-12）：

| `is_latest_point_only` | 返回点数 | `timestamps` |
|---|---|---|
| `true` | 1（**就是最新点**） | 6 个指标里 **5 个是 0**，且 `node_id` 为 `null` |
| `false`（2h 窗口） | 115 | 全部是真实时间戳，`node_id` 是逗号连的 node_id 列表 |

所以只发 `true` 会丢掉时间戳和 node_id；只发 `false` 又拿不到"只要最新点"的形态。
两次用**同一个 `end_time`**，两个响应里的最新点是同一个值 —— 一个文件里既有最新值，
又有对应它的真实时间戳，两份都是平台原生响应。

采集端要用哪种取法，按 `data/mapping/metric_rules.json` 的 `value_rule`
（`series_point` = 取时序最后一个非空点）决定。

## 命令中心 SQL 用的是 11 条合并 SQL

与采集端实现一致（`data/merged_sql/merged_sql.json`）：

- 82 个指标共用 **11 条合并 SQL**（`stat_database` 26 个 / `stat_user_tables` 22 个 / `activity_agg` 13 个 …）
- **同一批合并 SQL 覆盖的指标共用同一次调用**，因此同一批的多个文件内容完全相同
  （文件名与序号不同），这是合并的必然结果，不是重复抓取
- 实测实例：`83cd68f2e4cb40b4b7e69240479ffa26in14`（gauss-6374，**3 个 master**）
  → 每个命令中心文件里 ④ 会出现 3 条（每个 master 一条结果）

## 文件命名

`序号_客户指标英文名.json`，序号沿用 `data/metric_verification/` 的编号，
占位符 `{#NAME}` 之类按同一套规则清洗（`tools/verify_metric_sources.py` 的 `sanitize()`），
便于与 `data/metric_verification/` 的逐指标实测留档一一对照。

`000manifest.json` 是索引：每个指标的序号、英文名/中文名、分类、通道、文件名、API 列表，
以及 **该指标全部的请求对象**（`requests` 数组，与指标文件里的 `request` 逐字一致）。

## 重新抓取

```bash
export TPOPS_USER=sml TPOPS_PASSWORD='...'
python tools/capture_metric_response.py
```

- 串行执行（平台并发 ≥ 4 会触发参数竞争，产生假的"语法错误"）
- 命令中心每条 SQL 最多重试 3 次
- 一次全量约 **95 秒**（98 个指标 / 11 条合并 SQL / 3 个 master / 监控指标发两次）

## 注意

- **认证 token 不落盘**：`POST /rds/v3/auth/mini-token` 是全局会话动作，不进任何指标文件，
  各请求的 `X-Mini-Token` 一律写占位符 `<token>`
- `response_body.result` 是**JSON 字符串**，要二次 `json.loads` 才是行列表
- 平台把响应里的 `sql_text` 掩码为 `******`（执行不受影响）；**我们自己发出的 SQL 原文
  在每个文件第 2 步的 `request.body.params[0].param_value` 里**
- 监控指标接口的时序响应用 2 小时窗口、`end_time` 回退 5 分钟；命令中心返回的是**当前快照**，
  累计计数类指标需自行差分
