# 统一命令中心 —— 内置命令清单

> 平台内置命令 142 条的清单留档（response body 原样）+ 通用 SQL 命令的详情。
> 采集端启动时用它**定位 cmd_id 并校验该命令仍为 enabled**。

## 文件

| 文件 | 内容 | 体积 |
|---|---|---|
| `builtin_commands.json` | `GET /rds/gaussdb/v3/command-center/customer-commands` 的**分页原始 response body**（`pages`）+ 合并后的 142 条（`data`）+ 字段统计（`summary`） | 275 KB |
| `general_single_sql_query.json` | `GET .../commands/{cmd_id}` 的详情 —— `constraint_config`（执行约束）与 `param_config`（参数定义） | 3.3 KB |

`builtin_commands.json` 的结构：

```jsonc
{
  "generated_at": "2026-11-18 14:36:00 +0800",
  "api": "GET /rds/gaussdb/v3/command-center/customer-commands",
  "total_count": 142,          // 平台自报总数
  "fetched": 142,              // 实际取回条数（与 total_count 一致才算齐）
  "summary": { "cmd_type": {...}, "call_type": {...}, "source": {...}, "status": {...} },
  "pages": [ { "query_params": {...}, "response_body": {...} } ],  // 每页原始 body
  "data": [ { ...19 个字段... } ]                                  // 合并后的 142 条
}
```

单条命令的 19 个字段：
`cmd_id` `cmd_name` `cmd_type` `call_type` `category` `source` `action`
`cluster_mode` `status` `is_redirect` `limit_usage` `support_compare`
`cmd_content` `description` `remark` `creator` `operator` `create_time` `operate_time`

## 抓取方式

```bash
export TPOPS_USER=xxx TPOPS_PASSWORD=xxx
python tools/fetch_builtin_commands.py
```

分页遍历（`limit` 上限 100）直到取满 `total_count`；凭据只走环境变量，不入库。

## 2026-11-18 实测结论

| 维度 | 分布 |
|---|---|
| `cmd_type` | SQL 112、SHELL 20、PYTHON 8、**sql 2**（平台数据本身不统一，小写 `sql` 与大写 `SQL` 并存） |
| `call_type` | sync 140、async 2 |
| `source` | default 142（全部为平台内置，无自定义命令） |
| `status` | enabled 142 |

本项目只用到其中 **1 条**：`general_single_sql_query`
（`cmd_id = 73722acc-bff6-4d04-adc0-bc1e1fa1eb7d`，`status=enabled`，`cmd_type=SQL`），
模板为 `select * from ({sql_text}) limit {limit}` —— 82 个 `pg_stat` 类指标的主通道。

## 坑（实测，见开发方案 §命令目录）

- `status=disabled` **过滤无效**（仍返回全部 142 条 enabled），传非法值才 400
  → **判断命令可用性要逐条比对 `status` 字段**，不能用过滤条件。
- `cmd_type` 的取值不统一（`"SQL"` 112 条与 `"sql"` 2 条并存）→ 按 `cmd_type` 统计/过滤时
  两种大小写都要覆盖。
- `cluster_mode` 必须是 **JSON 数组**，字符串形式直接 400 —— 本脚本不传该参数。
- `limit` 上限 100，必须分页遍历，不能一次拉完。

## 自定义命令（`source=custom`）

2026-11-18 实测：平台上的命令清单为 **143 条 = 142 内置（source=default）+ 1 自定义（source=custom）**。

这条自定义命令由 **`sml` 账号在管控后台手工创建**（2026-11-18 15:04:30）：

| 字段 | 值 |
|---|---|
| `cmd_name` | `CMD.DBS.CRASH.CMQueryStatus_Custom` |
| `cmd_id` | `e96cffac-6c6a-4c4e-b623-c51c34cf64bf` |
| `cmd_type` / `call_type` | SHELL / **async**（内置那条是 sync） |
| `cmd_content` | `su - Ruby` → `source gauss_env_file` → `cm_ctl query -Cvpid` |
| `constraint_config` | `cluster_mode=All`、`limit_node_types=[master, slave, readreplica]`、`limit_component_types=[cn, dn:primary, dn:standby]`、db_version ≥ 8.218.0 / kernel ≥ 10.0.0 |
| `param_config` | 空（无参数） |
| `limit_usage` | `["api", "inspection"]` |

### 创建途径：**只能走管控后台，没有 API**

平台权限说明里命令中心**只有 5 个权限**，全是「查」和「执行」：

```
gaussdb:cmdCenter:listUniCommand          GET  /command-center/customer-commands
gaussdb:cmdCenter:showUniCommand          GET  /command-center/commands/{cmd_id}
gaussdb:cmdCenter:executeUniCommand       POST /command-center/customer-commands/{cmd_id}
gaussdb:cmdCenter:listUniCmdExecTask      GET  .../batch-exec-job/task-records
gaussdb:cmdCenter:showUniCmdExecTaskDetail GET  .../task-record/{task_id}/detail
```

控制台 JS 里确有管理类调用（`POST .../commands/create`、`PUT .../commands/{cmd_id}`、
`POST .../commands/switch-status`、`GET .../commands/import`），但走的是
**`openGauss/v3/...` 这个 miniConsole 产品前缀**，与本部署对外暴露的 `/rds/gaussdb/v3/...`
不是一套路由 → **对 API 调用方不可用**。

2026-11-18 实测（POST，body `{}`，只触发参数校验不会创建任何东西）：

| 候选路径 | 结果 |
|---|---|
| `/rds/gaussdb/v3/command-center/commands/create` | 404 |
| `/rds/gaussdb/v3/command-center/commands` | 404 |
| `/rds/gaussdb/v3/command-center/command/create` | 404 |
| `/rds/gaussdb/v3/command-center/commands/switch-status` | 404 |
| `/rds/v3/command-center/commands/create` | 404 |
| `/rds/openGauss/v3/command-center/commands/create` | 404 |
| `/rds/rest/gaussdb-iam5/v3/command-center/commands/create` | 404 |

⚠️ `GET /command-center/commands/import` 返回 **400「参数错误: cmd_id」**是**假象** ——
`GET .../commands/abc123` 也返回同样的 400，说明 `commands/{x}` 是 catch-all 路由，
`import` 只是被当成一个非法的 cmd_id。**不要据此以为 import 接口存在。**

### 管控后台创建/导入的约束（来自平台错误码）

| 错误码 | 约束 |
|---|---|
| `DBS.06030143` | UTF-8 编码的 **JSON** 文件；**文件名以 `DBS.` 开头**，≤256 字符，只能字母/数字/下划线 |
| `DBS.06030150` | **配置文件和签名文件必须同时上传** ← 硬门槛 |
| `DBS.06030145` / `06030146` | 自定义命令数量上限 / 文件大小上限 |
| `DBS.06030147` | 文件签名校验失败 |
| `DBS.06030148` / `06030152` | 统一命令不存在 / 已禁用 |
| `DBS.06030151` | **同一时刻只能有一个人做管理操作**（导入、编辑、禁用、激活、删除） |

### 对本项目的取舍

自定义命令**能**固化 SQL（省掉每次下发的 `sql_text` 请求体，平台还把它标为 `sensitive`），
PYTHON 型还能在服务端做**按节点差分**（解掉「计数类指标必须按 master 分基线」的硬伤）。

但：**cmd_id 由平台生成、跨环境不通用** → 采集端**不能硬编码**自定义命令的 cmd_id。
内置的 `general_single_sql_query`（`73722acc-bff6-4d04-adc0-bc1e1fa1eb7d`）全环境一致，
**必须作为主路径**；自定义命令只能作为可选增强（通过环境变量切换）。
