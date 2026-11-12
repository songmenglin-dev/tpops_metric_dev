# metric_response —— 每个客户指标的原生 API 响应

> 目录约定：**一个指标一个文件**，文件内容是该指标取数链路上各接口的**平台原生响应体**，
> 按调用顺序排列。响应体不做任何加工/改写（`result` 字段仍是平台返回的 JSON 字符串）。

## 文件格式

顶层是一个数组，每个元素一个接口：

```json
[
  {
    "api": "POST /rds/gaussdb/v3/command-center/customer-commands/73722acc-bff6-4d04-adc0-bc1e1fa1eb7d",
    "response_body": {
      "job_id": "e227df7b-e9a8-4...",
      "node_tasks": [{"task_id": "5b1c...", "node_id": "..."}]
    }
  },
  {
    "api": "GET /rds/gaussdb/v3/command-center/command/73722acc-.../batch-exec-job/e227df7b.../task-record/5b1c.../detail",
    "response_body": {
      "node_name": "gauss-6374_gaussdbv5_hcs2-0_0",
      "status": "succeed",
      "result": "[{\"datname\": \"postgres\", \"xact_commit\": \"7265656\", ...}]"
    }
  }
]
```

- `api`：`"<METHOD> <实际 URI>"`（URI 里的 job_id / task_id 是真实取值）
- `response_body`：平台原样返回的 JSON（`resp.json()`），**未做字段裁剪或归一化**

## 通道与文件构成

| 通道 | 文件数 | 每个文件里的 API（按顺序） |
|---|---|---|
| 命令中心 SQL | 82 | ① `GET …/key-view-execute-node` 取可执行节点<br>② `POST …/customer-commands/{cmd_id}` 下发命令<br>③ `GET …/batch-exec-job/task-records?job_id=…` 轮询任务状态<br>④ `GET …/batch-exec-job/{job_id}/task-record/{task_id}/detail` 取任务结果（**每个 master 一条**） |
| 监控指标接口 | 11 | ① `POST /rds/v3/monitor/metrics` |
| 诊断优化接口 | 4 | ① `POST …/slow-sql-list`（3 个）/ `POST …/top-sql-list`（1 个） |
| 实例接口 | 1 | ① `GET /rds/v3.2/instances?limit=100&offset=0` |

3 个判定为**无法提供**的指标不建文件（原因见 `data/customer_metric_unsupported.csv`）。

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

`manifest.json` 是索引：每个指标的序号、英文名/中文名、分类、通道、文件名、API 列表。

## 重新抓取

```bash
export TPOPS_USER=sml TPOPS_PASSWORD='...'
python tools/capture_metric_response.py
```

- 串行执行（平台并发 ≥ 4 会触发参数竞争，产生假的"语法错误"）
- 命令中心每条 SQL 最多重试 3 次
- 一次全量约 **107 秒**（98 个指标 / 11 条合并 SQL / 3 个 master）

## 注意

- **认证 token 不落盘**：`POST /rds/v3/auth/mini-token` 是全局会话动作，不进任何指标文件
- `response_body.result` 是**JSON 字符串**，要二次 `json.loads` 才是行列表
- 响应里的 `sql_text` 被平台掩码为 `******`（执行不受影响），真正的 SQL 以
  `data/merged_sql/merged_sql.json` 为准
- 监控指标接口的响应是**时序**（本次用 2 小时窗口、`is_latest_point_only=false`、
  `end_time` 回退 5 分钟）；命令中心返回的是**当前快照**，累计计数类指标需自行差分
