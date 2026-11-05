# 统一命令中心 API 实测

对 TPOPS 平台**统一命令中心（command-center）**的 5 个接口做真实调用，把完整响应原样落盘，
用来确定接口的真实行为（文档与实际常有出入，这里以实测为准）。

测试脚本：[`test_command_center_api.py`](test_command_center_api.py)
实测数据：[`results/`](results/)

## 覆盖的接口

| # | 方法 | 路径 | 说明 |
|---|---|---|---|
| 1 | GET | `/rds/gaussdb/v3/command-center/customer-commands` | 查询统一命令列表（分页/过滤/排序） |
| 2 | GET | `/rds/gaussdb/v3/command-center/commands/{cmd_id}` | 查询单条统一命令详情 |
| 3 | POST | `/rds/gaussdb/v3/command-center/customer-commands/{cmd_id}` | 执行单条统一命令 |
| 4 | GET | `/rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records` | 查询任务状态及节点任务记录列表 |
| 5 | GET | `/rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}/task-record/{task_id}/detail` | 查询节点任务结果详情 |

执行命令还依赖两个前置接口（不属于命令中心，但下发必须先拿到）：

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/rds/v3.2/instances` | 实例列表 → `instance_id` |
| GET | `/rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node` | 节点清单 → `node_id` / `role` / `type` / `component_id` |

## 运行

```bash
export TPOPS_USER=sml TPOPS_PASSWORD='...'
python tests/command_center/test_command_center_api.py
```

常用参数：

| 参数 | 说明 |
|---|---|
| `--list-only` | 只测列表与详情，**不执行任何命令** |
| `--matrix-only` | 只跑执行矩阵与异常路径，复用已落盘的列表/详情结果 |
| `--instance-id` / `--node-id` | 指定实例与节点，不传则自动取第一个 master 节点 |
| `--cmd-id` / `--sql` | 指定要执行的命令与 SQL |

## 落盘结构

```
results/
├── 00_auth.json                 认证响应
├── 01_instances.json            实例列表
├── 02_nodes_<instance_id>.json  节点清单
├── 03_command_list.json         142 条命令的合并结果 + 各过滤条件的 total_count
├── 03_command_list_raw/         列表接口逐页原始响应 + 各过滤条件实测（11 个文件）
├── 04_command_detail/           每条命令的详情（142 个文件，按 cmd_id 命名）
├── 05_execute_*.json            下发命令的响应
├── 06_task_records_*.json       任务状态列表
├── 07_task_result_*.json        任务结果详情
├── 08_exec_matrix/              每种命令类型的执行结果汇总
├── 09_negative/                 异常路径实测（15 个文件）
└── _summary.json                总体统计
```

除 `03_command_list.json`、`08_exec_matrix/`、`_summary.json` 三个汇总文件外，
其余每个文件都是**完整响应记录**，含请求方法与 URL、响应状态码、响应头、响应体：

```json
{
  "request":  {"method": "GET", "url": "https://..."},
  "response": {"status_code": 200, "headers": {...}, "body": {...}}
}
```

**唯一的例外是凭据字段**：认证接口的响应头与响应体里都带 `X-Mini-Token`（可直接复用的凭据），
落盘就等于把令牌提交进仓库。脚本对键名含 `token` / `password` / `secret` 的字段统一替换为
`<REDACTED>`，其余内容一字不改。

## 实测结论

### 1. 平台共 142 条内置命令，全部可列出、全部可查详情

| 维度 | 分布 |
|---|---|
| `cmd_type` | SQL 112、SHELL 20、PYTHON 8、**sql 2**（小写，平台数据本身不统一） |
| `call_type` | sync 140、async 2 |
| `source` | default 142（无自定义命令） |
| `status` | enabled 142 |
| `action` | query 138、collect 2、his_query 2 |
| `cluster_mode` | All 61、Distributed 42、Ha 38、**all 1**（小写） |

列表接口 `limit` 上限 100，需分页（`offset`）。142 条命令的详情 **142 次调用全部成功，无一失败**。

### 2. 执行链路：下发 → 轮询 → 取结果

```
POST customer-commands/{cmd_id}      → {job_id, node_tasks:[{task_id, node_id}]}
GET  .../batch-exec-job/task-records → {job_status, statistics:{total,succeed,failed,running}, tasks:[...]}
GET  .../task-record/{task_id}/detail → {status, result, out_format, out_mode, out_type}
```

- **`job_status` 不可靠**：实测多次出现节点任务已 `succeed`，但 `job_status` 仍是 `running`。
  判断是否结束要用 `statistics.running == 0`，不能只看 `job_status`。
- **`result` 字段是 JSON 字符串**，需要二次 `json.loads` 解析。
- **`sql_text` 在任务记录里被掩码为 `******`**（执行本身不受影响）。
- 单次往返 **3~8 秒**（节点实际执行耗时实测 2.3s / 4.6s / 6.6s，加上下发与轮询开销）；
  异步命令（`auto_emergency_scenario_data_collect_*`）会跑满 60 秒后报
  `exec cmd timeout, exceed 60.0s`。
- 节点任务状态 `succeed` 不代表命令成功：SHELL 命令的 `result` 里仍是
  `Failed to execute command on all nodes`（命令在节点上执行失败，但调度层记为 succeed）。

### 3. 各命令类型的执行实测

| cmd_type | call_type | 命令 | 下发 | 节点状态 | 结果 |
|---|---|---|---|---|---|
| PYTHON | async | `auto_emergency_scenario_data_collect_ha` | 200 | failed | 超时 60s（`is_redirect=true`，输出转储到 SFTP） |
| PYTHON | sync | `DBS.check_archive` | 200 | succeed | `true` |
| SHELL | sync | `DBS.After505.Cent.ERROR.AccountLock` | 200 | succeed | 385 字节文本，内容为节点上执行失败 |
| SQL | sync | `DBS.Cent.SLOW.WaitLock` | 200 | succeed | `[]`（实例无等待锁会话） |
| sql | sync | `dbs.full.get_stat_all_tables` | **500** | — | `DBS.280005 服务器错误` |

`dbs.full.get_stat_all_tables` 的 500 已复现：在**两个实例、两种节点类型**（DN 与 CN）上都是 500，
加 `database` 参数也无效 —— 属平台侧问题，非入参问题。

### 4. 约束机制

命令详情的 `constraint_config` 给出执行约束，**不满足会直接失败**：

```json
{"cluster_mode": "All", "limit_node_types": ["master"], "limit_component_types": ["cn","dn"],
 "db_version": {"min_value": "3.100.0"}, "kernel_version": {"min_value": "503"}}
```

- `limit_node_types` 要求节点角色 —— 实测命令下发时**必须选 master 节点**
- `cluster_mode` 约束实例形态（Ha / Distributed / All），`cluster_mode: "all"`（小写）是数据本身的不统一
- `db_version` / `kernel_version` 是最低版本要求

参数约束（`param_config.params`）里 `required: true` 的参数必须传，缺失会报：

```
DBS.06030081 参数错误: ... error message: param schema_name is required, but is not set in input params.
```

`exec_config` 里还有 `switch_database` / `database_field` / `time_out` / `result_masks` 四个隐含参数
—— 带 `switch_database: true` 的命令需要 `database_name` 类参数指定库。

### 5. 通用 SQL 查询命令（采集主通道）

`general_single_sql_query`，cmd_id `73722acc-bff6-4d04-adc0-bc1e1fa1eb7d`：

- 参数：`sql_text`（必填，pattern 限制 `^(select|SELECT|show|SHOW)(.){1, 10000}$`）、`limit`（必填，1~1000）
- 约束：`cluster_mode=All`、`limit_node_types=[master]`、`limit_component_types=[cn,dn]`
- 模板：`select * from ({sql_text}) limit {limit}` —— **可执行任意 SQL**，是 82 个 pg_stat 类客户指标的主通道
- 实测有效 SQL：`select version() as v, now() as t` → 正常返回版本号与时间

### 6. 异常路径实测

| 场景 | 状态码 | 平台返回 |
|---|---|---|
| 列表 `cmd_type=BOGUS` | 400 | `DBS.06010013 参数错误: cmd_type` |
| 列表 `status=BOGUS` | 400 | `DBS.06010013 参数错误: status` |
| 列表 `cluster_mode=Ha`（字符串） | 400 | `DBS.06010013 参数错误: cluster_mode` |
| 列表 `status=disabled` | **200** | **返回全部 142 条 enabled 命令 —— 过滤未生效** |
| 详情 cmd_id 非 UUID | 400 | `参数错误: cmd_id` |
| 详情 cmd_id 不存在 | 404 | `统一命令不存在` |
| 执行缺少 instance_id | 400 | `参数错误: instance_id` |
| 执行缺少 node_ids | 400 | `参数错误. [node_ids]` |
| 执行非 select/show 的 SQL | 400 | `校验失败...invalid SQL statement with unsupported type. Only support single [select]` |
| 执行 limit 越界（99999） | 400 | `校验失败...parma.limit range invalid` |
| 执行不存在的 node_id | 400 | `参数错误: nodeIds[0].<list element>` |
| 任务状态缺 job_id | 400 | `参数不存在 [job_id]` |
| 任务状态 job_id 非 UUID | 400 | `参数错误: job_id` |
| 任务结果 task_id 非 UUID | 400 | `参数错误: task_id` |

**两个需要注意的平台行为**：

1. **`status=disabled` 过滤无效** —— 传 `disabled` 仍返回全部 142 条 enabled 命令；
   而传非法值 `BOGUS` 反而报 400。说明 `status` 只做了枚举校验，没做实际过滤。
   采集端不能依赖这个过滤条件判断命令是否可用，得自己看返回项的 `status` 字段。
2. **`cluster_mode` 必须是 JSON 数组** —— `cluster_mode=["All"]` 返回 61 条，
   而 `cluster_mode=Ha` 直接 400。文档里写了这点，但没写字符串形式会报错。

### 7. 其它实测细节

- 非法枚举值 / 非法 UUID 统一返回 `400 DBS.06010013 参数错误: <字段名>`
- 缺少必填参数返回 `400 DBS.280001 参数错误. [字段名]`（如缺 `node_ids`）、
  或 `400 DBS.280270 参数不存在 [字段名]`（如缺 `job_id`）
- 命令不存在用 `404 DBS.06030148 统一命令不存在`
- 参数校验失败用 `400 DBS.06030081 校验失败，可能参数非法或不满足场景要求`
- 平台侧故障用 `500 DBS.280005 服务器错误`
- 命令详情里的 `cmd_content` 是命令的原始 SQL / Shell 脚本，可直接看出它查什么
- `output_config` 给出输出形态：`format`（json/text）、`mode`（array/single）、`type`（map/bool/string）、
  `is_redirect`（是否转储到 SFTP）、`save_dir`

## 对采集端的直接影响

1. 走命令中心采集 pg_stat 类指标，**必须用 master 节点**，且 `instance_id` 与 `node_ids` 都要传。
2. 判断任务是否结束**用 `statistics.running == 0`**，不要用 `job_status`。
3. `result` 是 JSON 字符串，要二次解析；`sql_text` 在记录里被掩码，但不影响执行。
4. 同步命令单次往返 3~8 秒；82 个客户指标若各发一条 SQL，每轮 50 次往返 ≈ 3~7 分钟，
   采集周期撑不住 —— **应合并成少量几条 SQL**（`general_single_sql_query` 的模板支持）。
5. 累计计数类指标（`pg_stat_database.xact_commit` 等）需自行差分算速率，
   并处理实例重启导致的计数归零。
6. `auto_emergency_scenario_data_collect_*` 这类异步命令会超时 60 秒，
   采集端不应使用 `is_redirect=true` 的转储类命令。
