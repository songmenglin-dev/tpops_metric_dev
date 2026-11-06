# GaussDB 监控数据接入开发方案

> 范围：把 TPOPS（GaussDB 监控平台）的**指标**与**告警**数据，按客户的字段规范采集、转换，
> 通过 Kafka 投递到客户自有的监控平台。
> 形态：常驻进程，持续循环运行（至少一次语义，失败不推进水位）。

本文的事实基础全部来自本仓库已完成的实测留档，不含推测：

| 依据 | 位置 |
|---|---|
| 101 个客户指标的获取方法判定 | `data/customer_metric_mapping.json`、`data/customer_metric_query_plan.json` |
| 逐指标真实调用结果（98/98 通过） | `data/metric_verification/*.json`（99 个文件，含 `_summary.json`） |
| 平台接口契约与真实响应 | `data/command_center/results/`（26 组留档）、`data/tpops_api_catalog.json` |
| 告警侧原始数据与告警定义目录 | `data/alarm/`（9 个文件） |
| 接口文档原文（CHM 转换） | `docs/api/` |

---

## 0. 目标与验收口径

**目标**：常驻进程按周期拉取 TPOPS 的指标与告警 → 按客户字段规范映射 → 以 JSON 写入 Kafka，
供客户监控平台消费。

**验收口径**（与已有的实测结论对齐）：

1. 客户指标库 101 个指标中，**98 个可提供**、3 个确认无法提供（理由见 §1.2.6），
   可提供率 97.03%。
2. 98 个可提供指标**逐个真实调用通过**（`data/metric_verification/_summary.json`：
   `total=98, passed=98, failed=0`），其中 6 个当前实例上查询成功但无数据。
3. 指标与告警两条链路都能**持续运行**：进程常驻、按周期自动采集、失败自动退避重试、
   收到终止信号能优雅退出、成功投递后才推进水位。

---

## 1. 指标调研

### 1.1 客户指标总览

客户《高斯DB-指标明细》给出 **101 个指标**，分 **13 个类别**。字段规范包含数据类型、
数据更新周期、采集更新周期单位、指标单位（见 `data/metric_verification/*.json` 的
`customer_spec` 字段）。

| 维度 | 分布 |
|---|---|
| 状态 | 可提供 98 / 无法提供 3 |
| 匹配等级 | 直接对应 79、近似对应 10、需换算 6、可统计 3 |
| 数据类型 | 整数 77、字符 13、浮点 7、文本 1 |
| 数据更新周期 | 1 分 30 项、2 分 16 项、5 分 45 项、60 秒 3 项、30 秒 1 项、1 时 1 项、24 时 2 项 |

**关键结论：客户指标库里最快的一档是 30 秒。** 这决定了采集器的能力要求——
一轮采集必须在 30 秒内完成（§3.9 给出耗时预算）。

### 1.2 按获取方法分类（核心结论）

平台的监控面一共只有 **4 条取数通道**能覆盖客户指标（告警侧另算，见 §1.2.5）。**获取方法不同，数据形态不同，
采集逻辑完全不同**，这是整个方案的第一层切分：

| 获取方法 | 指标数 | 涉及接口 | 覆盖的客户指标类别 |
|---|---|---|---|
| **监控指标接口** | 11 | `POST /rds/v3/monitor/metrics` | 基本信息(6)、事务信息(2)、性能信息(2)、可用性(1) |
| **命令中心SQL** | 82 | `POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}` | 性能信息(18)、事务信息(16)、表信息(11)、数据库信息(9)、连接情况(8)、Bgwriter信息(8)、锁信息(5)、基本信息(3)、会话信息(2)、可用性(1)、数据库统计信息(1) |
| **诊断优化接口** | 4 | `POST /rds/gaussdb/v3/instances/{instance_id}/slow-sql-list`、`POST /rds/gaussdb/v3/instances/{instance_id}/top-sql-list` | 慢查询信息(3)、性能信息(1) |
| **无法提供** | 3 | — | 连接情况(2)、Agent(1) |
| **实例接口** | 1 | `GET /rds/v3.2/instances` | 基本信息(1) |

分类依据不是"指标长什么样"，而是"平台用什么接口能拿到它"，因为这直接决定了：

- 数据是**时序**（平台自己按采集周期攒好了）还是**快照**（只能查当前值，得自己轮询）；
- 采集是**一次拉一批**还是**一条指标一次调用**；
- 值要不要**自己做差分算速率**、**自己做聚合**。

### 1.2.1 A 类：监控指标接口（11 个指标，时序型）

**接口**：`POST /rds/v3/monitor/metrics`

平台原生监控指标，直接返回时间序列（`timestamps` + `series`，按主机 IP 展开）。
这 11 个指标是平台**唯一**能给出时序的通道。

| 客户指标 | 中文名 | TPOPS 指标 | 匹配 |
|---|---|---|---|
| `GAUSSDB.Number_of_ansactions_mmitted_r_cond` | 每秒提交事务数 | `rds092_gaussv5_commit_counter` | 直接对应 |
| `GAUSSDB.Number_of_llback_ansactions_r_cond` | 每秒回滚事务数 | `rds093_gaussv5_rollback_counter` | 直接对应 |
| `gaussdb.Tps` | TPS（每秒事务数） | `rds400_gaussv5_total_counter` | 直接对应 |
| `gaussdb.QPS` | QPS（每秒查询数） | `rds402_gaussv5_ddl_dml_dcl_count` | 直接对应 |
| `GAUSSDB.Service_availability_testing` | 服务可用性检测 | `gaussdb_instance_status` | 近似对应 |
| `GAUSSDB.PG_Service_Status` | GAUSSDB服务状态 | `gaussdb_instance_status` | 近似对应 |
| `gaussdb.Status` | 数据库状态 | `gaussdb_instance_status` | 近似对应 |
| `GAUSSDB.Standby_status` | Standby状态 | `datanode_switch_status` | 近似对应 |
| `GAUSSDB.Primary_status` | Primary状态 | `datanode_switch_status` | 近似对应 |
| `gaussdb.Starttime` | 启动时间 | `rds133_cn_startup_time` | 近似对应 |
| `gaussdb.Uptimestr` | 运行时长 | `rds133_cn_startup_time` | 需换算 |

**采集要点**（实测结论，勿凭直觉改）：

- 窗口 **≥ 1 小时**才能覆盖 `collection_frequency=3600` 的指标（5 分钟窗口覆盖 0/21）。
- `end_time` 必须**回退 5 分钟**（数据入库滞后约 5 分钟；5 分钟窗口实测 null 率 77.5%）。
- `is_latest_point_only` 用 **字符串 `"false"`**——用 `"true"` 会让约 74% 的指标 `timestamp=0`
  （值对、时间丢）。
- 一次请求可带多个 `metric_names` + 多个 `instances`，11 个指标**一次调用取完**，不是 11 次。
- 平台的采集周期档位（`collection_frequency`）：10s×29、30s×2、60s×152、300s×12、3600s×21。
- 指标的**中文名只在数据响应**的 `metric_name` 字段里，目录接口
  `GET /rds/v3/monitor/metrics/series` 不带中文名。

### 1.2.2 B 类：统一命令中心 SQL（82 个指标，快照型）

**接口**：`POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}`（三跳，见 §2.3）

这是**最大的一个通道（82/101）**。平台不提供 pg_stat 类监控指标，但内置命令
`general_single_sql_query`（`cmd_id = 73722acc-bff6-4d04-adc0-bc1e1fa1eb7d`）的模板是
`select * from ({sql_text}) limit {limit}` —— **平台提供执行器，我们提供 SQL**，
可以直接查 `pg_stat_database` / `pg_stat_user_tables` / `pg_stat_activity` / `pg_locks` /
`pg_stat_bgwriter` / `pg_settings` / `pg_database` / `pg_prepared_xacts`。

**这 82 个指标占了客户全部需求的 81%**，覆盖 11 个客户类别：

| 客户指标类别 | 指标数 | 主要视图 |
|---|---|---|
| 性能信息 | 18 | `pg_stat_database`、`pg_stat_activity` |
| 事务信息 | 16 | `pg_stat_database` |
| 表信息 | 11 | `pg_stat_user_tables` |
| 数据库信息 | 9 | `pg_stat_database`、`pg_database` |
| 连接情况 | 8 | `pg_stat_activity` |
| Bgwriter信息 | 8 | `pg_stat_bgwriter` |
| 锁信息 | 5 | `pg_locks` |
| 基本信息 | 3 | `pg_settings`、`pg_prepared_xacts` |
| 会话信息 | 2 | `pg_stat_activity` |
| 可用性 | 1 | `pg_settings` |
| 数据库统计信息 | 1 | `pg_stat_database` + `pg_stat_user_tables` |

**关键结论（决定了整个采集器的形态）**：

1. 命令中心返回的是**当前快照**，不是时序。平台不保存历史，所以这 82 个指标的
   "时序"必须由**采集端自己轮询攒出来**——这是本方案与 A 类最大的区别。
2. 累计计数类指标（`xact_commit`、`tup_inserted`、`blks_hit` 等）是**单调累计值**，
   客户要的是"每秒 X 条"，必须**自己差分**再除以时间差，并处理实例重启导致的归零。
3. 单次往返 **3~8 秒**。82 个指标若各发一条 SQL（现有方案就是 50 条），一轮要 **3~7 分钟**，
   撑不住客户 30 秒~5 分钟的采集周期 → **必须合并成少量几条 SQL**（§3.3）。
4. GaussDB 方言是 **PG 9.2 级**：不支持 `FILTER` 子句（用 `CASE WHEN`）；
   `state <> ''` 恒为假（排除后台进程要用 `state is not null`）。
5. 实测 50 条 SQL 里有 **3 条偶发失败，重试即成功** → 采集端**必须做重试**。

现有 50 条 SQL 按来源视图分布（详见 `data/customer_metric_query_plan.json`）：

| 数据来源视图 | 现有 SQL 条数 | 覆盖客户指标数 | 合并后 SQL 条数 |
|---|---|---|---|
| `pg_stat_database` | 18 | 27 | 1 |
| `pg_stat_user_tables` | 12 | 23 | 1 |
| `pg_stat_activity` | 4 | 15 | 2 |
| `pg_locks` | 2 | 3 | 1 |
| `pg_stat_bgwriter` | 8 | 8 | 1 |
| `pg_settings` | 4 | 4 | 1 |
| `pg_database` | 4 | 5 | 1 |
| `pg_prepared_xacts` | 1 | 1 | 1 |
| **合计** | **50** 条去重 SQL（列内合计 53，跨视图 SQL 重复计入） | **82** | **10** |

其中 1 条 SQL 同时查 `pg_stat_database` 与 `pg_stat_user_tables`（`gaussdb.get.dbstat.sum`
的"库级 + 表级统计联合结果"），单独成一条。

### 1.2.3 C 类：诊断优化接口（4 个指标，快照型）

**接口**：`POST /rds/gaussdb/v3/instances/{instance_id}/slow-sql-list`、
`POST /rds/gaussdb/v3/instances/{instance_id}/top-sql-list`

| 客户指标 | 中文名 | 接口 | 取值方式 |
|---|---|---|---|
| `GAUSSDB.Total_slow_queries` | 慢查询总数 | `slow-sql-list` | `len(slow_sql_infos)` |
| `GAUSSDB.Select_slow_queries` | Select慢查询数 | `slow-sql-list` | 按 `sql_text` 前缀统计 |
| `GAUSSDB.DML_slow_queries` | DML慢查询数 | `slow-sql-list` | 按 `sql_text` 前缀统计 |
| `gaussdb.Top10sql` | 当前运行时间最长的Top10 SQL | `top-sql-list` | `top_sql_infos` 按 `total_elapse_time` 排序 |

**采集要点**：

- `slow-sql-list` 的**时间窗口上限 24 小时**，超出（48h/7d/30d）一律返回 400「参数错误」。
  → 采集窗口必须钳在 24h 内。
- `threshold` 必传（ms 取 1~99000，s 取 1~99），`threshold_unit` 可选（ms / s）。
- `top-sql-list` 需传**全部 CN 节点 id**（不是只传 master）。
- 响应里的 `sql_text` 是业务 SQL 文本，投递到客户平台前需要考虑脱敏（§5 风险项）。

### 1.2.4 D 类：实例接口（1 个指标，静态元数据）

**接口**：`GET /rds/v3.2/instances`

| 客户指标 | 中文名 | 取值 |
|---|---|---|
| `gaussdb.server_version` | 版本 | `instances[].engine_version`（如 `V2.0-10.0.0`） |

元数据类指标，变化极少，采集器启动时取一次、之后按小时刷新即可。

### 1.2.5 告警（2 类接口 · 5 种取数组合）

告警侧与指标侧是**两套完全独立的接口**，形态也不同：

| 获取方法 | 接口 | 参数组合 | 实测条数 | 数据形态 |
|---|---|---|---|---|
| 活动告警（收敛列表） | `POST /rds/v3/alarm/records` | `is_active=true, is_convergence=true` | 2 | 每条 = 实例 + alarm_id 一行 |
| 活动告警（明细行） | 同上 | `is_active=true, is_convergence=false` | 3 | 每节点一行，含 `description`/`possible_causes`/`processing_steps` |
| 历史告警（收敛列表） | 同上 | `is_active=false, is_convergence=true` | 4 | 已恢复/已清理 |
| 历史告警（明细行） | 同上 | `is_active=false, is_convergence=false` | 32 | 逐节点明细 |
| 告警数量统计 | `GET /rds/v3/alarm/records/statistic` | — | — | 按 `severity` 计数，分系统侧/实例侧 |

告警定义的静态目录（**平台没有查询接口**，从 CHM 产品文档手工整理）：

| 文件 | 条数 | 内容 |
|---|---|---|
| `data/alarm/catalog_metric.json` | 85 | 指标类告警：告警ID、名称、归属、默认阈值与等级、震荡/恢复持续时间、关联指标、采集间隔 |
| `data/alarm/catalog_event.json` | 203 | 事件类告警：告警ID、名称、默认级别、告警类型、归属、是否内核告警、是否可自动清除 |

采集端用法：告警记录里的 `alarm_id` 与目录里的「告警ID」是同一套 ID，
**用 `alarm_id` 关联目录补全"这条告警是什么规则、默认阈值多少"**，不依赖名称匹配。

**告警侧的两条硬约束**：

- **历史告警仅保留 30 天**。停机超 30 天会丢告警 → 采集端日志必须显式告警
  （"某段时间的历史告警已超出平台留存窗口"）。
- `limit` 上限 **100**，超出需分页（`offset`）→ 采集端必须处理分页。

### 1.2.6 E 类：无法提供（3 个指标）

| 客户指标 | 中文名 | 原因 |
|---|---|---|
| `gaussdb.Fastpath_function_call` | 快路径函数调用的连接数 | PG 侧无 fastpath 计数器；`pg_settings` 仅有 `gsi_fastpath_level` 开关 |
| `gaussdb.Disabled_conn` | 禁用的连接数 | PG 无「禁用连接」概念；仅有 `reserved_connections` 等预留连接 |
| `gaussdb.sendAgentName` | 发送设备名给Agent | Agent 侧行为，不是数据库可查询的监控指标 |

处理方式：**丢弃并计数上报**（§3.8），在客户平台侧以"不可提供"清单交付，不静默吞掉。

### 1.3 类别 × 获取方法矩阵

| 客户指标类别 | 总数 | 命令中心SQL | 监控指标接口 | 诊断优化接口 | 实例接口 | 不可提供 | 小计 |
|---|---|---|---|---|---|---|---|
| 事务信息 | 18 | 16 | 2 | · | · | · | 18 |
| 慢查询信息 | 3 | · | · | 3 | · | · | 3 |
| 可用性 | 2 | 1 | 1 | · | · | · | 2 |
| 锁信息 | 5 | 5 | · | · | · | · | 5 |
| 性能信息 | 21 | 18 | 2 | 1 | · | · | 21 |
| 连接情况 | 10 | 8 | · | · | · | 2 | 8 |
| 基本信息 | 10 | 3 | 6 | · | 1 | · | 10 |
| 表信息 | 11 | 11 | · | · | · | · | 11 |
| 数据库统计信息 | 1 | 1 | · | · | · | · | 1 |
| Agent | 1 | · | · | · | · | 1 | 0 |
| Bgwriter信息 | 8 | 8 | · | · | · | · | 8 |
| 数据库信息 | 9 | 9 | · | · | · | · | 9 |
| 会话信息 | 2 | 2 | · | · | · | · | 2 |
| **合计** | **101** | **82** | **11** | **4** | **1** | **3** | **98** |

### 1.4 全量指标清单（101 个）

| # | 客户指标（英文） | 中文名 | 类别 | 获取方法 | TPOPS 来源 | 匹配 |
|---|---|---|---|---|---|---|
| 1 | `GAUSSDB.Number_of_ansactions_mmitted_r_cond` | 每秒提交事务数 | 事务信息 | A 监控指标接口 | `rds092_gaussv5_commit_counter` | 直接对应 |
| 2 | `GAUSSDB.Number_of_llback_ansactions_r_cond` | 每秒回滚事务数 | 事务信息 | A 监控指标接口 | `rds093_gaussv5_rollback_counter` | 直接对应 |
| 3 | `GAUSSDB.{#NAME}.Number.ad.ples` | ({#NAME}) number of dead tuples | 事务信息 | B 命令中心SQL | `pg_stat_user_tables.n_dead_tup` | 直接对应 |
| 4 | `GAUSSDB.{#NAME}.Number.lete.ples` | ({#NAME}) number of delete tuples | 事务信息 | B 命令中心SQL | `pg_stat_user_tables.n_tup_del` | 直接对应 |
| 5 | `GAUSSDB.{#NAME}.garbage.Ratio` | ({#NAME})Garbage ratio | 事务信息 | B 命令中心SQL | `pg_stat_user_tables.n_dead_tup/(n_live_tup+n_dead_tup)` | 直接对应 |
| 6 | `GAUSSDB.{#NAME}.Number.t.date.ples` | ({#NAME}) number of hot update tuples | 事务信息 | B 命令中心SQL | `pg_stat_user_tables.n_tup_hot_upd` | 直接对应 |
| 7 | `GAUSSDB.{#NAME}.number.Of.index.scan` | ({#NAME})number of index scan | 事务信息 | B 命令中心SQL | `pg_stat_user_tables.idx_scan` | 直接对应 |
| 8 | `GAUSSDB.{#NAME}.number.Of.dex.ples.ad` | ({#NAME})number of index tuples read | 事务信息 | B 命令中心SQL | `pg_stat_user_tables.idx_tup_fetch` | 直接对应 |
| 9 | `GAUSSDB.{#NAME}.number.Of.live.tuples` | ({#NAME})number of live tuples | 事务信息 | B 命令中心SQL | `pg_stat_user_tables.n_live_tup` | 直接对应 |
| 10 | `GAUSSDB.{#NAME}.number.Of.insert.tuples` | ({#NAME})number of insert tuples | 事务信息 | B 命令中心SQL | `pg_stat_user_tables.n_tup_ins` | 直接对应 |
| 11 | `GAUSSDB.{#NAME}.number.Of.sequential.scan` | ({#NAME})number of sequencial scan | 事务信息 | B 命令中心SQL | `pg_stat_user_tables.seq_scan` | 直接对应 |
| 12 | `GAUSSDB.{#NAME}.number.Of.quential.ples.ad` | ({#NAME})number of sequencial tuples read | 事务信息 | B 命令中心SQL | `pg_stat_user_tables.seq_tup_read` | 直接对应 |
| 13 | `GAUSSDB.{#NAME}.number.Of.update.tuples` | ({#NAME})number of update tuples | 事务信息 | B 命令中心SQL | `pg_stat_user_tables.n_tup_upd` | 直接对应 |
| 14 | `GAUSSDB.Idle_in_transaction_connections` | Idle in transaction connections | 事务信息 | B 命令中心SQL | `pg_stat_activity.state` | 直接对应 |
| 15 | `GAUSSDB.Idle_connections` | Idle connections | 事务信息 | B 命令中心SQL | `pg_stat_activity.state` | 直接对应 |
| 16 | `GAUSSDB.Current_number_of_connections` | 当前连接数 | 事务信息 | B 命令中心SQL | `pg_stat_database.numbackends` | 直接对应 |
| 17 | `GAUSSDB.maximum_connection` | 最大连接数 | 事务信息 | B 命令中心SQL | `pg_settings.max_connections` | 直接对应 |
| 18 | `GAUSSDB.Number_of_active_connections` | 活跃连接数 | 事务信息 | B 命令中心SQL | `pg_stat_activity.state` | 直接对应 |
| 19 | `GAUSSDB.Total_slow_queries` | 慢查询总数 | 慢查询信息 | C 诊断优化接口 | `slow-sql-list.sql_id` | 可统计 |
| 20 | `GAUSSDB.Select_slow_queries` | Select慢查询数 | 慢查询信息 | C 诊断优化接口 | `slow-sql-list.sql_text` | 可统计 |
| 21 | `GAUSSDB.DML_slow_queries` | DML慢查询数 | 慢查询信息 | C 诊断优化接口 | `slow-sql-list.sql_text` | 可统计 |
| 22 | `GAUSSDB.Service_availability_testing` | 服务可用性检测 | 可用性 | A 监控指标接口 | `gaussdb_instance_status` | 近似对应 |
| 23 | `gaussdb_{#PORT}port.status` | 服务端口{#PORT}检测 | 可用性 | B 命令中心SQL | `pg_settings.port` | 近似对应 |
| 24 | `gaussdb.Deadlocks` | 检测到的死锁次数 | 锁信息 | B 命令中心SQL | `pg_stat_database.deadlocks` | 直接对应 |
| 25 | `gaussdb.get.lock.info` | 获取数据库锁数据 | 锁信息 | B 命令中心SQL | `pg_locks` | 直接对应 |
| 26 | `gaussdb.Lockwaiting` | 查看当前锁等待数量 | 锁信息 | B 命令中心SQL | `pg_stat_activity.state` | 直接对应 |
| 27 | `gaussdb.locks.get_metrics[{#DBNAME}]` | DB [{#DBNAME}]: 获取锁信息 | 锁信息 | B 命令中心SQL | `pg_locks` | 近似对应 |
| 28 | `gaussdb.locks.accessexclusive[{#DBNAME}]` | DB [{#DBNAME}]: Num of accessexclusive locks | 锁信息 | B 命令中心SQL | `pg_locks.mode` | 直接对应 |
| 29 | `gaussdb.Xact_rollback` | 回滚的事务总数 | 性能信息 | B 命令中心SQL | `pg_stat_database.xact_rollback` | 直接对应 |
| 30 | `gaussdb.Tps` | TPS（每秒事务数） | 性能信息 | A 监控指标接口 | `rds400_gaussv5_total_counter` | 直接对应 |
| 31 | `gaussdb.QPS` | QPS（每秒查询数） | 性能信息 | A 监控指标接口 | `rds402_gaussv5_ddl_dml_dcl_count` | 直接对应 |
| 32 | `gaussdb.get.connections.sum` | 获取连接信息 | 性能信息 | B 命令中心SQL | `pg_stat_activity` | 直接对应 |
| 33 | `gaussdb.get.dbstat.sum` | 获取db性能数据 | 性能信息 | B 命令中心SQL | `pg_stat_database + pg_stat_user_tables` | 直接对应 |
| 34 | `gaussdb.running.sql.limit.20` | 正在运行的SQL | 性能信息 | B 命令中心SQL | `pg_stat_activity.query` | 直接对应 |
| 35 | `gaussdb.Top10sql` | 当前运行时间最长的Top10 SQL | 性能信息 | C 诊断优化接口 | `top-sql-list.total_elapse_time` | 直接对应 |
| 36 | `gaussdb.Blk_write_time` | 写块的时间（毫秒） | 性能信息 | B 命令中心SQL | `pg_stat_database.blk_write_time` | 直接对应 |
| 37 | `gaussdb.Numbackends` | 当前连接到数据库的后端进程数 | 性能信息 | B 命令中心SQL | `pg_stat_database.numbackends` | 直接对应 |
| 38 | `gaussdb.Temp_bytes` | 临时文件大小 | 性能信息 | B 命令中心SQL | `pg_stat_database.temp_bytes` | 直接对应 |
| 39 | `gaussdb.Blks_read` | 从磁盘读取的块数 | 性能信息 | B 命令中心SQL | `pg_stat_database.blks_read` | 直接对应 |
| 40 | `gaussdb.Blks_hit` | 从缓存命中读取的块数 | 性能信息 | B 命令中心SQL | `pg_stat_database.blks_hit` | 直接对应 |
| 41 | `gaussdb.Tup_deleted` | 删除的行数 | 性能信息 | B 命令中心SQL | `pg_stat_database.tup_deleted` | 直接对应 |
| 42 | `gaussdb.Xact_commit` | 提交的事务总数 | 性能信息 | B 命令中心SQL | `pg_stat_database.xact_commit` | 直接对应 |
| 43 | `gaussdb.Tup_inserted` | 插入的行数 | 性能信息 | B 命令中心SQL | `pg_stat_database.tup_inserted` | 直接对应 |
| 44 | `gaussdb.Conflicts` | 数据库操作冲突次数 | 性能信息 | B 命令中心SQL | `pg_stat_database.conflicts` | 直接对应 |
| 45 | `gaussdb.Tup_updated` | 更新的行数 | 性能信息 | B 命令中心SQL | `pg_stat_database.tup_updated` | 直接对应 |
| 46 | `gaussdb.Tup_returned` | 查询返回的行数 | 性能信息 | B 命令中心SQL | `pg_stat_database.tup_returned` | 直接对应 |
| 47 | `gaussdb.Temp_files` | 生成的临时文件数 | 性能信息 | B 命令中心SQL | `pg_stat_database.temp_files` | 直接对应 |
| 48 | `gaussdb.tup_fetched` | 通过索引获取的行数 | 性能信息 | B 命令中心SQL | `pg_stat_database.tup_fetched` | 直接对应 |
| 49 | `gaussdb.Blk_read_time` | 读块的时间（毫秒） | 性能信息 | B 命令中心SQL | `pg_stat_database.blk_read_time` | 直接对应 |
| 50 | `gaussdb.Idle_in_transaction_aborted` | 事务中已中止的空闲连接数 | 连接情况 | B 命令中心SQL | `pg_stat_activity.state` | 直接对应 |
| 51 | `gaussdb.Idle_in_transaction` | 事务中空闲的连接数 | 连接情况 | B 命令中心SQL | `pg_stat_activity.state` | 直接对应 |
| 52 | `gaussdb.Fastpath_function_call` | 快路径函数调用的连接数 | 连接情况 | E 不可提供 | `—` | — |
| 53 | `gaussdb.Total_conn` | 总连接数 | 连接情况 | B 命令中心SQL | `pg_stat_database.numbackends` | 直接对应 |
| 54 | `gaussdb.Active_conn` | 活动连接数 | 连接情况 | B 命令中心SQL | `pg_stat_activity.state` | 直接对应 |
| 55 | `gaussdb.Disabled_conn` | 禁用的连接数 | 连接情况 | E 不可提供 | `—` | — |
| 56 | `gaussdb.Idle_conn` | 空闲连接数 | 连接情况 | B 命令中心SQL | `pg_stat_activity.state` | 直接对应 |
| 57 | `gaussdb.Waiting_conn` | 等待资源的连接数 | 连接情况 | B 命令中心SQL | `pg_stat_activity.state` | 直接对应 |
| 58 | `gaussdb.Total_pct` | 连接使用率 | 连接情况 | B 命令中心SQL | `pg_stat_activity.state` | 近似对应 |
| 59 | `gaussdb.Prepared` | 预提交事务数 | 连接情况 | B 命令中心SQL | `pg_prepared_xacts` | 直接对应 |
| 60 | `GAUSSDB.PG_Service_Status` | GAUSSDB服务状态 | 基本信息 | A 监控指标接口 | `gaussdb_instance_status` | 近似对应 |
| 61 | `gaussdb.Starttime` | 启动时间 | 基本信息 | A 监控指标接口 | `rds133_cn_startup_time` | 近似对应 |
| 62 | `gaussdb.Uptimestr` | 运行时长 | 基本信息 | A 监控指标接口 | `rds133_cn_startup_time` | 需换算 |
| 63 | `gaussdb.server_version` | 版本 | 基本信息 | D 实例接口 | `instances.engine_version` | 直接对应 |
| 64 | `gaussdb.Serverencoding` | 字符集 | 基本信息 | B 命令中心SQL | `pg_database.encoding` | 直接对应 |
| 65 | `gaussdb.Status` | 数据库状态 | 基本信息 | A 监控指标接口 | `gaussdb_instance_status` | 近似对应 |
| 66 | `gaussdb.Timezone` | 时区 | 基本信息 | B 命令中心SQL | `pg_settings.TimeZone` | 直接对应 |
| 67 | `gaussdb.Pagesize` | 页大小 | 基本信息 | B 命令中心SQL | `pg_settings.block_size` | 直接对应 |
| 68 | `GAUSSDB.Standby_status` | Standby状态 | 基本信息 | A 监控指标接口 | `datanode_switch_status` | 近似对应 |
| 69 | `GAUSSDB.Primary_status` | Primary状态 | 基本信息 | A 监控指标接口 | `datanode_switch_status` | 近似对应 |
| 70 | `gaussdb.{#NAME}.Number.lete.ples` | ({#NAME})被删除的行总数（包括已回收行） | 表信息 | B 命令中心SQL | `pg_stat_user_tables.n_tup_del` | 直接对应 |
| 71 | `gaussdb.{#NAME}.Number.ad.ples` | ({#NAME})已删除但未回收的行数（死亡行） | 表信息 | B 命令中心SQL | `pg_stat_user_tables.n_dead_tup` | 直接对应 |
| 72 | `gaussdb.{#NAME}.Number.t.date.ples` | ({#NAME})通过 HOT更新的行数 | 表信息 | B 命令中心SQL | `pg_stat_user_tables.n_tup_hot_upd` | 直接对应 |
| 73 | `gaussdb.{#NAME}.number.Of.index.scan` | ({#NAME})索引扫描次数 | 表信息 | B 命令中心SQL | `pg_stat_user_tables.idx_scan` | 直接对应 |
| 74 | `gaussdb.{#NAME}.number.Of.quential.ples.ad` | ({#NAME})顺序扫描读取的行数 | 表信息 | B 命令中心SQL | `pg_stat_user_tables.seq_tup_read` | 直接对应 |
| 75 | `gaussdb.{#NAME}.number.Of.dex.ples.ad` | ({#NAME})通过索引扫描获取的行数 | 表信息 | B 命令中心SQL | `pg_stat_user_tables.idx_tup_fetch` | 直接对应 |
| 76 | `gaussdb.{#NAME}.number.Of.insert.tuples` | ({#NAME})插入的行数 | 表信息 | B 命令中心SQL | `pg_stat_user_tables.n_tup_ins` | 直接对应 |
| 77 | `gaussdb.{#NAME}.number.Of.live.tuples` | ({#NAME})当前活跃的行数 | 表信息 | B 命令中心SQL | `pg_stat_user_tables.n_live_tup` | 直接对应 |
| 78 | `gaussdb.{#NAME}.number.Of.update.tuples` | ({#NAME})更新的行总数 | 表信息 | B 命令中心SQL | `pg_stat_user_tables.n_tup_upd` | 直接对应 |
| 79 | `gaussdb.{#NAME}.number.Of.sequential.scan` | ({#NAME})顺序扫描次数 | 表信息 | B 命令中心SQL | `pg_stat_user_tables.seq_scan` | 直接对应 |
| 80 | `gaussdb.{#NAME}.garbage.Ratio` | ({#NAME})死亡行占比 | 表信息 | B 命令中心SQL | `pg_stat_user_tables.n_dead_tup/(n_live_tup+n_dead_tup)` | 直接对应 |
| 81 | `gaussdb.DBtsize[{#DATABASE_NAME}]` | [{#DATABASE_NAME}]库大小 | 数据库统计信息 | B 命令中心SQL | `pg_database_size(datname)` | 直接对应 |
| 82 | `gaussdb.sendAgentName` | 发送设备名给Agent | Agent | E 不可提供 | `—` | — |
| 83 | `GAUSSDB.Buffers_Alloc` | Buffers Alloc | Bgwriter信息 | B 命令中心SQL | `pg_stat_bgwriter.buffers_alloc` | 直接对应 |
| 84 | `GAUSSDB.Buffers_Backend` | Buffers Backend | Bgwriter信息 | B 命令中心SQL | `pg_stat_bgwriter.buffers_backend` | 直接对应 |
| 85 | `GAUSSDB.Buffers_Backend_Fsync` | Buffers Backend Fsync | Bgwriter信息 | B 命令中心SQL | `pg_stat_bgwriter.buffers_backend_fsync` | 直接对应 |
| 86 | `GAUSSDB.Buffers_Checkpoint` | Buffers Checkpoint | Bgwriter信息 | B 命令中心SQL | `pg_stat_bgwriter.buffers_checkpoint` | 直接对应 |
| 87 | `GAUSSDB.Buffers_Clean` | Buffers Clean | Bgwriter信息 | B 命令中心SQL | `pg_stat_bgwriter.buffers_clean` | 直接对应 |
| 88 | `GAUSSDB.Checkpoint_count_eckpoint_gments` | Checkpoint count (by checkpoint segments) | Bgwriter信息 | B 命令中心SQL | `pg_stat_bgwriter.checkpoints_req` | 直接对应 |
| 89 | `GAUSSDB.Checkpoint_count_eckpoint_meout` | Checkpoint count (by checkpoint timeout) | Bgwriter信息 | B 命令中心SQL | `pg_stat_bgwriter.checkpoints_timed` | 直接对应 |
| 90 | `GAUSSDB.Max_Written_Clean` | Max Written Clean | Bgwriter信息 | B 命令中心SQL | `pg_stat_bgwriter.maxwritten_clean` | 直接对应 |
| 91 | `GAUSSDB.[{#DATNAME}]_committed_transactions` | [{#DATNAME}] Commited transactions | 数据库信息 | B 命令中心SQL | `pg_stat_database.xact_commit` | 直接对应 |
| 92 | `GAUSSDB.[{#DATNAME}]_Active_connections` | [{#DATNAME}] Active connections | 数据库信息 | B 命令中心SQL | `pg_stat_activity` | 直接对应 |
| 93 | `GAUSSDB.[{#DATNAME}]_rolled_back_transactions` | [{#DATNAME}] Rolled back transactions | 数据库信息 | B 命令中心SQL | `pg_stat_database.xact_rollback` | 直接对应 |
| 94 | `GAUSSDB.[{#DATNAME}]_fetch_records_per_second` | [{#DATNAME}]每秒Fetch记录数 | 数据库信息 | B 命令中心SQL | `pg_stat_database.tup_fetched` | 需换算 |
| 95 | `GAUSSDB.[{#DATNAME}]_insert_records_per_second` | [{#DATNAME}]每秒Insert记录数 | 数据库信息 | B 命令中心SQL | `pg_stat_database.tup_inserted` | 需换算 |
| 96 | `GAUSSDB.[{#DATNAME}]_return_records_per_second` | [{#DATNAME}]每秒Return记录数 | 数据库信息 | B 命令中心SQL | `pg_stat_database.tup_returned` | 需换算 |
| 97 | `GAUSSDB.[{#DATNAME}]_update_records_per_second` | [{#DATNAME}]每秒Update记录数 | 数据库信息 | B 命令中心SQL | `pg_stat_database.tup_updated` | 需换算 |
| 98 | `GAUSSDB.[{#DATNAME}]_cache_hit_ratio` | [{#DATNAME}]缓存命中率 | 数据库信息 | B 命令中心SQL | `pg_stat_database.blks_hit/(blks_hit+blks_read)` | 直接对应 |
| 99 | `GAUSSDB.[{#DATNAME}]_delete_records_per_second` | [{#DATNAME}]每秒Delete记录数 | 数据库信息 | B 命令中心SQL | `pg_stat_database.tup_deleted` | 需换算 |
| 100 | `GAUSSDB.session_usage` | 连接数使用率 | 会话信息 | B 命令中心SQL | `pg_stat_activity.state` | 近似对应 |
| 101 | `GAUSSDB.sessions_active_top10` | 会话时间TOP10 | 会话信息 | B 命令中心SQL | `pg_stat_activity.query_start` | 直接对应 |

### 1.5 取值口径：三类转换

拿到原始值之后，到客户字段之间还有三类转换，必须显式建模（§4.5 的映射表结构）：

| 转换类型 | 出现场景 | 例子 |
|---|---|---|
| **直接取值** | 平台值与客户值同口径 | `n_dead_tup` → `{#NAME} number of dead tuples` |
| **口径换算** | 平台值与客户值同源但定义不同 | 平台给累计 `xact_commit`，客户要"每秒提交事务数" → 差分 ÷ 时间；平台给 `rds133_cn_startup_time`，客户要"运行时长" → `now - 启动时间` |
| **聚合/派生** | 客户要的是一个派生量 | `dead_tup_ratio` = `n_dead_tup/(n_live_tup+n_dead_tup)`；`cache_hit_ratio` = `blks_hit/(blks_hit+blks_read)`；"连接使用率" = `active/总数` |

**维度展开**：客户指标名里带占位符（`{#NAME}`、`{#DBNAME}`、`{#DATABASE_NAME}`、`{#PORT}`），
一条 SQL 返回 N 行就要展开成 **N 条 Kafka 消息**，占位符用行里的实际值回填。
例如 `gaussdb.get.dbstat.sum` 实测返回 85 行、`GAUSSDB.{#NAME}.Number.ad.ples` 返回 77 行。

### 1.6 指标侧调研结论小结

1. **4 条取数通道**覆盖 98/101 个指标，通道之间数据形态不同 → 采集器按通道分治。
2. **只有 A 类（11 个）是时序**，B/C/D 类（87 个）全是快照 → 必须靠轮询攒时序。
3. **B 类 82 个是主体**，必须把 50 条 SQL 合并成 10 条才能进 30 秒~5 分钟的采集周期。
4. **累计值差分算速率**是 B 类的核心加工步骤，重启归零要能识别。
5. **维度展开**（一行一消息）是消息条数膨胀的主因（一条 SQL → 最多 85 条消息）。

---

## 2. API 调研

平台共两个命名空间，**路径前缀错了就是 404**，不是"接口没实现"：

| 命名空间 | 覆盖 |
|---|---|
| `/rds/v3/…` | 认证、实例列表、监控指标、指标目录、告警记录、告警统计 |
| `/rds/gaussdb/v3/instances/{id}/…` | 整个**数据库诊断优化**接口集 + **统一命令中心** |

### 2.1 认证

| 项 | 内容 |
|---|---|
| 接口 | `POST /rds/v3/auth/mini-token` |
| 入参 | `{"username": ..., "password": ...}` |
| 出参 | Token 在**响应头** `X-Mini-Token`（不在 body） |
| 有效期 | 服务端 `session_timeout` **1440 分钟** |
| 风险 | **登录失败会累计 `login_failed_attempts` 并锁号** → 密码错误时禁止盲目重试 |

后续所有请求带 `X-Mini-Token` 头，`X-Language: zh-cn` 可选。

### 2.2 指标类接口

| 接口 | 方法 | 用途 | 关键约束 |
|---|---|---|---|
| `/rds/v3/monitor/metrics` | POST | 拉指标时间序列 | 窗口 ≥ 1h；`end_time` 回退 5min；`is_latest_point_only="false"`；`metric_names` 支持批量 |
| `/rds/v3/monitor/metrics/series` | GET | 指标目录（name/unit/alarm_id） | 分组码必须硬编码 12 个；**写错分组名返回 200 + 空 series 不报错**；不带中文名 |
| `/rds/v3.2/instances` | GET | 实例列表与元数据 | — |
| `/rds/v3/version-info` | GET | 服务版本 `DBS-GaussDB-Version` | — |

12 个指标分组码（平台没有枚举接口，写错返回空）：
`CPUMEMORY` `CONNECTION` `SYNCSTAT` `PROCESSRESOURCE` `NETWORK` `TRANSACTION`
`IOSTORAGE` `LOCK` `INSTATUS` `FORDR` `AGENTSTATUS` `COREMETRICS`

### 2.3 统一命令中心接口（三跳 + 2 个前置）

| # | 方法 | 路径 | 用途 |
|---|---|---|---|
| 前置 1 | GET | `/rds/v3.2/instances` | 取 `instance_id` |
| 前置 2 | GET | `/rds/gaussdb/v3/instances/{id}/key-view-execute-node` | 取节点清单 → **master 的 `node_id`** |
| 1 | POST | `/rds/gaussdb/v3/command-center/customer-commands/{cmd_id}` | 下发 → 返回 `job_id` + `task_id`（**不含数据**） |
| 2 | GET | `/rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records?job_id=` | 轮询 → `statistics.running` |
| 3 | GET | `/rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}/task-record/{task_id}/detail` | 取结果 → `result` |
| 旁路 | GET | `/rds/gaussdb/v3/command-center/customer-commands` | 命令目录（142 条内置命令） |
| 旁路 | GET | `/rds/gaussdb/v3/command-center/commands/{cmd_id}` | 命令详情（参数约束、版本约束） |

`general_single_sql_query` 的约束（来自命令详情 `constraint_config`）：

```json
{"cluster_mode": "All", "limit_node_types": ["master"],
 "limit_component_types": ["cn", "dn"], "db_version": {"min_value": "3.100.0"}}
```

参数约束：`sql_text` 必填（pattern 限制 `^(select|SELECT|show|SHOW)(.){1, 10000}$`）、
`limit` 必填（1~1000）。

### 2.4 告警类接口

| 接口 | 方法 | 用途 | 关键约束 |
|---|---|---|---|
| `/rds/v3/alarm/records` | POST | 告警记录列表 | `is_active`、`is_convergence` **必传**；`limit` 1~100；历史仅 30 天 |
| `/rds/v3/alarm/records/statistic` | GET | 按等级统计告警条数 | 无参 |

告警记录字段（收敛列表 / 明细行字段不同）：

`severity`(`notice`/`minor`/`major`/`critical`)、`status`(`activated`/`cleared`/`recovered`)、
`alarm_id`、`alarm_name`、`alarm_type`(`metric`/`event`/`system-metric`/`system-event`/`system`)、
`alarm_category`、`instance_id`、`instance_name`、`host_ip`、`node_id`、
`first_alarm_time`、`last_alarm_time`、`alarm_duration`、`clear_alarm_time`、
`alarm_detail_raise_count`、`possible_causes`、`processing_steps`、`impaction`、
`automatic_clear`、`notify_status`、`resource_id`、`alarm_items_sum`（收敛模式）。

### 2.5 API 约束汇总

| 约束 | 影响 |
|---|---|
| 指标窗口 ≥ 1h | 采集器不能用短窗口取数，只能取长窗口后按时间戳切分 |
| 入库滞后 5 分钟 | `end_time` 必须回退 |
| `is_latest_point_only=true` 丢时间戳 | 一律用 `"false"` |
| 告警 `limit` ≤ 100 | 必须分页 |
| 历史告警 30 天 | 长时间停机会丢告警，需告警日志 |
| 命令中心单次 3~8 秒 | 82 指标必须合并 SQL + 并发下发 |
| 命令只在 master 执行 | 必须先查节点清单 |
| 一次下发多节点失败 | 逐节点下发（本方案单节点） |
| 命令偶发失败（3/50） | 必须重试 |
| `result` 是 JSON 字符串 | 二次 `json.loads` |
| `job_status` 不可靠 | 用 `statistics.running == 0` |
| Token 1440 分钟 | 按需刷新 |
| 登录失败锁号 | 认证失败不重试 |
| 自签证书 | `verify_tls=False` |
| 分组名写错返回空 | "空分组告警"是唯一的发现手段 |

### 2.6 已实测确认的平台行为（采集端必须知道）

1. **`job_status` 不可靠** —— 节点任务已 `succeed`，`job_status` 仍是 `running`。
   结束判据用 `statistics.running == 0`。
2. **`result` 是 JSON 字符串** —— 需二次 `json.loads` 才是行列表。
3. **`sql_text` 在任务记录里被掩码为 `******`** —— 执行不受影响，但**不能靠任务记录回读 SQL**。
4. **列名取自 SQL 的 `as` 别名** —— 自定义 SQL 必须显式写别名，否则拿到的是 PG 原始列名。
5. **节点任务 `succeed` ≠ 命令成功** —— SHELL 命令 `result` 里仍是
   `Failed to execute command on all nodes` → 必须校验 `result` 内容，不能只看状态。
6. **`status` 过滤无效** —— 命令列表传 `status=disabled` 仍返回全部 142 条 enabled 命令，
   但传非法值 `BOGUS` 报 400。→ 不能依赖该过滤条件判断命令可用性。
7. **`cluster_mode` 必须是 JSON 数组** —— `cluster_mode=Ha`（字符串）直接 400。
8. **两个前缀都存在** —— 诊断优化接口在 `/rds/gaussdb/v3/…`，去掉 `gaussdb` 段全 404。
9. **12 个分组码写错返回 200 + 空 series** —— 不会报错，只能靠"空分组告警"发现。

---

## 3. 实现方法

### 3.1 总体数据流

```
                    ┌──────────────── 采集层（按通道分治）────────────────┐
Scheduler ────────► │ A 监控指标采集器  ─┐                                  │
（定时 + 水位）      │ B 命令中心采集器  ─┤  → Normalizer（内部统一 DTO）    │
                    │ C 诊断优化采集器  ─┤                                  │
                    │ D 实例元数据采集器 ─┤                                  │
                    │ E 告警采集器      ─┘                                  │
                    └──────────────────────┬───────────────────────────────┘
                                           ▼
                              FieldMapper（映射表驱动 + 取值规则）
                                           ▼
                              Serializer（唯一 JSON 出口）
                                           ▼
                              Kafka Producer（PLAINTEXT）
                                           ▼
                                    推进水位（仅投递成功后）
```

**不变式**：一次循环 = 拉取 → 转换 → 投递 → **成功后才推进水位**。
投递失败不推进水位，重启后从水位续传（至少一次语义）。

### 3.2 按获取方法分治：指标四条通道 + 告警一条

| 路径 | 采集器 | 调用形态 | 数据形态 | 水位粒度 |
|---|---|---|---|---|
| A | `MonitorCollector` | **1 次**调用取 11 个指标 | 时序（带时间戳） | 按指标 + 时间戳 |
| B | `CommandCenterCollector` | **10 条 SQL 并发下发**，三跳取回 | 快照 | 按指标 + 维度 + 采集时刻 |
| C | `DiagnosticCollector` | slow-sql-list + top-sql-list 各 1 次 | 快照 | 按指标 + 采集时刻 |
| D | `InstanceCollector` | 1 次，启动时 + 每小时 | 静态 | 按指标 |
| E | `AlarmCollector` | 4 次记录 + 1 次统计 | 事件 | 按告警幂等键 |

**为什么 A 类 1 次调用就够**：`metric_names` 是数组，11 个指标合并成一个请求，
返回 `metric_infos[].metrics[]`，每条自带 `timestamps` + `series`。

**为什么 B 类必须并发**：10 条 SQL 串行 = 10 × (下发 + 轮询 + 取结果) ≈ 50~75 秒，
超 30 秒档；并发下发后统一轮询，整轮 ≈ 8~15 秒（§3.9）。
命令中心本身是异步任务模型（POST 返回 `job_id`），**天然支持并发**。

### 3.3 命令中心 SQL 合并：50 条 → 10 条

现有方案是"一个指标一条 SQL"（50 条）。合并原则：**同一视图、同一粒度的字段合成一条宽 SQL**，
派生列用 `CASE WHEN` 算，聚合在 SQL 里做完。

| # | 合并后 SQL | 覆盖视图 | 覆盖指标数 |
|---|---|---|---|
| 1 | `pg_stat_database` 全字段 | `pg_stat_database` | 26 |
| 2 | `pg_stat_user_tables` 全字段 + `dead_tup_ratio` | `pg_stat_user_tables` | 22 |
| 3 | `pg_stat_activity` 按 `datname,state` 分组 | `pg_stat_activity` | 13 |
| 4 | `pg_stat_activity` 活跃会话 Top10（含 query 文本） | `pg_stat_activity` | 2 |
| 5 | `pg_locks` 按 `datname,locktype,mode` 聚合 | `pg_locks` | 3 |
| 6 | `pg_stat_bgwriter` 全字段 | `pg_stat_bgwriter` | 8 |
| 7 | `pg_settings` 4 个参数一次取 | `pg_settings` | 4 |
| 8 | `pg_database` 编码 + 库大小 | `pg_database` | 2 |
| 9 | `pg_prepared_xacts` 计数 | `pg_prepared_xacts` | 1 |
| 10 | `pg_stat_database` ⋈ `pg_stat_user_tables` 联表 | 两个视图 | 1 |
| **合计** | | | **82** |

（第 3、4 两条同属 `pg_stat_activity`，但一个按 `datname,state` 聚合、一个是活跃会话明细
Top10（带 query 文本、`limit 20`/`limit 10`），粒度与用途都不同，不能合成一条。）

示例（SQL#2，宽表 + 派生列 + 显式别名）：

```sql
select schemaname, relname,
       n_live_tup, n_dead_tup, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
       seq_scan, seq_tup_read, idx_scan, idx_tup_fetch,
       case when (n_live_tup + n_dead_tup) > 0
            then round(n_dead_tup * 100.0 / (n_live_tup + n_dead_tup), 2)
            else 0 end as dead_tup_ratio
from pg_stat_user_tables
order by dead_tup_ratio desc
```

约束（都是实测踩过的）：

- 只允许 `select` / `show` 开头，其他一律 400。
- **必须显式写 `as` 别名** —— 列名取自别名，不写就拿到 PG 原始列名。
- 派生列用 `CASE WHEN`（PG 9.2 无 `FILTER`）。
- `limit` 必填（1~1000）。宽表 + 77 张表 → `limit` 取 1000。
- 排除后台进程用 `state is not null`（`state <> ''` 恒为假）。

### 3.4 取值规则引擎

合并后 SQL 只解决"取到数据"，**"怎么变成客户的值"** 由规则引擎负责，表驱动、可配置：

| 规则类型 | 输入 | 输出 | 用途 |
|---|---|---|---|
| `raw` | 某一行的某一列 | 原值 | 直接对应（占绝大多数） |
| `aggregate` | 多行 | `sum`/`max`/`count`/`first` | 分组求和（如连接数按 state 汇总） |
| `ratio` | 两列 | `a/(a+b)`，分母为 0 时返回 0 | 死元组比例、缓存命中率 |
| `counter_delta` | 累计值 + 上次累计值 + 时间差 | `(now - last) / Δt` | **每秒提交事务数**这类速率指标 |
| `gauge` | 时间戳 | `now - 启动时间` | 运行时长 |
| `enum_map` | 平台枚举 | 客户枚举 | 实例状态、节点角色 |
| `top_n` | 多行 + 排序列 | 前 N 行 | Top10 SQL |

`counter_delta` 的三个细节：

1. **重启归零**：本次累计值 < 上次累计值 → 判定实例重启，**丢弃本次差分**（返回 null 并告警），
   下轮重新建立基线，否则会算出一个巨大的负数或假峰值。
2. **首次采集无基线**：首轮只记基线、不产出速率值。
3. **时间差用采集时刻差**，不是标称周期（采集本身有 3~8 秒抖动）。

### 3.5 告警采集与去重

```
每轮（默认 30s）:
  1. POST alarm/records  is_active=true  is_convergence=true    → 活动告警收敛列表
  2. POST alarm/records  is_active=true  is_convergence=false   → 活动告警明细行
  3. POST alarm/records  is_active=false is_convergence=true    → 历史告警收敛列表
  4. POST alarm/records  is_active=false is_convergence=false   → 历史告警明细行
  5. GET  alarm/records/statistic                               → 按等级计数
  6. 用 alarm_id 关联 data/alarm/catalog_*.json 补全告警定义
  7. 幂等键去重后投递
```

**幂等键**：`(alarm_id, instance_id, node_id, first_alarm_time)`。
`first_alarm_time` 保证"同一条告警的重复上报"被识别为同一条。

**历史告警的 30 天窗口**：每轮都全量拉历史会重复且昂贵 →
按 `first_alarm_time` 水位只取"上次水位之后新增/变化"的记录，
并且**每轮检查"最新历史告警时间距今 > 30 天"时打告警日志**（说明平台侧已丢弃数据）。

**告警条目与明细行的关系**：收敛列表给"哪些告警在烧"（一行 = 实例+告警ID），
明细行给"具体哪个节点、什么原因、怎么修"。建议**两条都投递**，
收敛列表作为主条目，明细行作为 `details` 数组挂在同一条消息里（字段规范待客户确认，§5）。

### 3.6 消息模型

客户 JSON 字段定义**尚未最终确认**（§5 待确认项 1）。因此：

- **内部统一 DTO** 先定死（`models.py`），与客户格式解耦；
- **映射表**（`mapping/data/*.json`）把内部 DTO 字段映射到客户字段名，
  客户确认后**只改表不改代码**；
- **表驱动**，映射不写死在代码里（CLAUDE.md 明确要求）。

内部 DTO 骨架：

```python
MetricPoint = frozen dataclass(
    customer_metric     # 客户指标英文名
    dimensions          # 维度值 dict，如 {"DATABASE_NAME": "postgres"}
    value               # 数值
    value_type          # 整数/浮点/字符/文本
    unit                # 指标单位
    timestamp           # 采集时刻（毫秒）
    source              # 获取方法：monitor_metrics / command_center_sql / ...
    instance_id
)

AlarmRecord = frozen dataclass(
    alarm_id, alarm_name, severity, status, alarm_type, alarm_category,
    instance_id, instance_name, host_ip, node_id,
    first_alarm_time, last_alarm_time, clear_alarm_time, alarm_duration,
    details             # 明细行列表（cause/impact/steps/...）
)
```

### 3.7 持续运行机制

**这是本方案的硬需求**：进程常驻，不能跑一次就退出。

| 机制 | 设计 |
|---|---|
| **调度** | 单进程主循环 + 周期表（30s / 1min / 2min / 5min / 1h / 24h 六档）。每档独立 due time，快的档多跑几次，指标按需调度而不是每轮全跑 |
| **水位** | 落盘水位文件（JSON），成功投递后才推进。指标水位 = 每指标的最后成功采集时刻；告警水位 = 最后一次处理到的 `first_alarm_time` |
| **重试** | 指数退避 + 抖动：1s → 2s → 4s → 8s（上限 5 次）。**认证失败不重试**（防锁号）；**命令中心单条 SQL 失败只重试该条**，不影响同批其他 SQL |
| **令牌** | 缓存令牌，过期前主动刷新；收到 401 强制刷新重试 1 次；认证接口本身失败 → 记录并退避，但**密码错误场景只报一次**不重复撞 |
| **优雅退出** | `SIGTERM`/`SIGINT` → 停止发起新请求 → 等待在途请求完成（超时 30s）→ 刷盘水位 → 退出。退出码 0 |
| **健康自检** | 每轮记录成功/失败/跳过/丢弃计数；连续 N 轮全失败 → 告警日志（不退出，交由进程管理器重启） |
| **日志** | `logging`，不用 `print`；凭据不入日志；每轮一行汇总（拉取数/转换数/投递数/失败数/耗时） |

**投递失败不推进水位** → 重启后从水位续传 → **至少一次语义**。
客户侧需按幂等键去重（我们已按 §3.5 的键去重，但 Kafka 侧仍可能重复）。

### 3.8 不可提供指标的处置

3 个无法提供的指标**丢弃并计数上报**：

- 每轮日志输出"本轮丢弃 N 个不可提供指标"，累计计数暴露给运维；
- 交付时附一份"无法提供清单"（`data/customer_metric_unsupported.csv` 已有）；
- **不静默吞掉** —— 客户问起来能立刻回答"哪几个、为什么"。

### 3.9 性能预算（每轮）

| 路径 | HTTP 调用数 | 预计耗时 | 并发 |
|---|---|---|---|
| A 监控指标 | 1 | 1~3 s | — |
| B 命令中心 | 10×3 = 30 | 8~15 s（并发） | 10 |
| C 诊断优化 | 2 | 2~4 s | 2 |
| D 实例元数据 | 0（缓存） | — | — |
| E 告警 | 5 | 2~5 s | 5 |
| **合计** | **约 35** | **约 15~25 s** | — |

最紧的一档是客户 30 秒周期的那个指标（`data_update_cycle=30秒`）——
B 类并发后 15 秒可完成，**留有约 2 倍余量**。
若实测发现 10 条并发把平台压垮，降并发数并把该指标并入 1 分钟档（需与客户确认）。

---

## 4. 代码设计

### 4.1 目录结构

沿用 CLAUDE.md 已定的骨架，按"通道分治"补齐采集器与规则引擎：

```
tpops_kafka/
├── main.py              # 入口：装配依赖、启动调度循环、信号处理
├── config.py            # 配置加载（环境变量 + 可选配置文件）
├── scheduler.py         # 轮询循环、分档调度、水位推进、重试退避、优雅退出
├── models.py            # 跨模块 DTO（frozen dataclass）
├── watermark.py         # 水位持久化（原子写 + 崩溃恢复）
├── tpops/
│   ├── client.py        # TPOPS HTTP 客户端（认证 + 会话 + 错误分类）
│   ├── metrics.py       # A 类：监控指标拉取、窗口与 end_time 计算
│   ├── command_center.py# B 类：命令中心三跳、批量下发、SQL 合并表
│   ├── diagnostic.py    # C 类：slow-sql-list / top-sql-list
│   ├── instances.py     # D 类：实例元数据
│   └── alarms.py        # E 类：告警拉取、分页、定义目录关联
├── mapping/
│   ├── table.py         # 映射表加载与查表
│   ├── rules.py         # 字段级转换规则（raw/aggregate/ratio/counter_delta/...）
│   └── data/
│       ├── metric_map.json      # 101 指标 → 取值规则（表驱动）
│       ├── sql_batches.json     # 10 条合并 SQL 与其覆盖指标
│       ├── alarm_catalog.json   # 告警定义目录（合并 metric + event）
│       └── unsupported.json     # 3 个不可提供指标及理由
├── collectors/          # 各通道采集器：拉取 → 归一化 → 交给 mapper
│   ├── monitor.py
│   ├── command_center.py
│   ├── diagnostic.py
│   ├── instance.py
│   └── alarm.py
└── kafka/
    ├── producer.py      # PLAINTEXT 生产者
    └── serializer.py    # 唯一 JSON 序列化出口
```

### 4.2 关键接口

```python
# tpops/command_center.py
class CommandCenterClient:
    def master_node(self, instance_id) -> NodeInfo: ...          # 带缓存
    def submit(self, instance_id, node_id, sql_text, limit) -> Job: ...   # 第 1 跳
    def poll(self, job_id) -> TaskRecords: ...                    # 第 2 跳
    def fetch_result(self, job_id, task_id) -> List[dict]: ...    # 第 3 跳（result 二次解析）
    def run_sql_batch(self, instance_id, sql_texts, limit=1000,
                      concurrency=9) -> Dict[str, List[dict]]: ... # 并发三跳 + 单条重试

# collectors/base.py
class Collector:
    def collect(self, ctx: CollectContext) -> List[DTO]: ...      # 纯函数式，便于测试

# mapping/rules.py
class RuleEngine:
    def apply(self, rule: MetricRule, rows: List[dict],
              prev: Optional[Snapshot]) -> List[Tuple[Dict[str, str], Any]]: ...
```

### 4.3 配置项

| 变量 | 默认 | 说明 |
|---|---|---|
| `TPOPS_USER` / `TPOPS_PASSWORD` / `TPOPS_BASE_URL` | — | 认证与地址（**必填，凭据只走环境变量**） |
| `KAFKA_BOOTSTRAP_SERVERS` | — | 逗号分隔 |
| `KAFKA_TOPIC_METRICS` / `KAFKA_TOPIC_ALARMS` | — | Topic 名（**待客户确认**） |
| `POLL_INTERVAL_SECONDS` | 30 | 主循环间隔 |
| `METRIC_WINDOW_HOURS` | 2 | 监控指标窗口，**不得小于 1** |
| `METRIC_END_LAG_MINUTES` | 5 | `end_time` 回退量 |
| `CMD_CONCURRENCY` | 9 | 命令中心并发下发数 |
| `CMD_LIMIT` | 1000 | SQL 返回行数上限 |
| `ALARM_INTERVAL_SECONDS` | 30 | 告警轮询间隔 |
| `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_SECONDS` | 5 / 1 | 重试次数与退避基数 |
| `WATERMARK_FILE` | `./.watermark.json` | 水位文件路径 |
| `LOG_LEVEL` | INFO | 日志级别 |

### 4.4 映射表格式（表驱动）

```json
{
  "customer_metric": "GAUSSDB.[{#DATNAME}]_committed_transactions",
  "customer_metric_name": "提交事务数",
  "category": "事务信息",
  "source": "command_center_sql",
  "sql_key": "stat_database",
  "dimension": "datname",
  "select": {"column": "xact_commit"},
  "transform": {"type": "counter_delta", "per_second": true},
  "value_type": "浮点",
  "unit": "tps"
}
```

客户字段名映射单独一层（`mapping/data/field_map.json`），
把内部 DTO 字段名映射成客户 JSON 字段名 —— 客户确认后只改这一层。

### 4.5 测试策略

| 类型 | 范围 | 方法 |
|---|---|---|
| 单元 | 规则引擎（`raw`/`ratio`/`counter_delta`/重启归零）、窗口计算、SQL 合并映射、告警幂等键 | 纯函数，**无网络** |
| 单元 | 三跳解析（`result` 二次 `json.loads`、`statistics.running` 判据） | 用 `data/command_center/results/` 留档当固定样本 |
| 集成 | 采集器 → 映射 → 序列化 | **TPOPS 与 Kafka 一律 mock** |
| 端到端 | 单轮采集 → 消息 JSON | 用真实留档回放，断言消息条数与值 |

- 目标覆盖率 **80%+**（CLAUDE.md 要求）。
- **已有 99 个逐指标实测留档可直接当回归样本** —— `data/metric_verification/*.json`
  里有每个指标的真实响应，规则引擎改完可用它回放校验。
- Python **3.7.9** 红线：不用海象、`list[str]`、`dict |`、`zoneinfo`、`str.removeprefix`；
  用 `typing.List/Dict/Optional`、frozen dataclass、NamedTuple。

### 4.6 里程碑

| 阶段 | 内容 | 产出 | 验收 |
|---|---|---|---|
| M1 | 骨架 + 配置 + Kafka 生产者 + 序列化 | 可启动、能收信号 | `python -m tpops_kafka.main` 起停正常 |
| M2 | A 类采集器（监控指标） | 11 个指标上 Kafka | 与 `data/metric_verification` 的实测值逐条比对一致 |
| M3 | B 类采集器（命令中心）+ 10 条合并 SQL + 规则引擎 | 82 个指标上 Kafka | 同上；一轮 ≤ 25s |
| M4 | C/D 类 + 告警采集器 + 告警目录关联 | 4 + 1 指标 + 告警上 Kafka | 幂等键去重生效 |
| M5 | 调度器 + 水位 + 重试退避 + 优雅退出 | 持续运行 | 杀进程重启后水位续传、无重复无丢失 |
| M6 | 客户字段映射落地（待客户确认） | 客户格式 JSON | 按客户字段规范逐条核对 |

**M3 是关键路径**：82/101 的指标在这里，且合并 SQL 的正确性决定全部。

---

## 5. 风险与待确认事项

### 5.1 待客户确认（阻塞 M6，不阻塞 M1~M5）

| # | 事项 | 现状 | 建议默认 |
|---|---|---|---|
| 1 | **客户 JSON 字段定义** | 未确认，以《高斯DB-指标明细》101 指标为准（其中英文名有 OCR 错字，如 `GAUSSDB.Number_of_ansactions_mmitted_r_cond`） | 内部 DTO + 映射表先行，字段名表驱动可改 |
| 2 | **Topic 命名与分区策略** | 未确认 | 指标/告警分两个 topic，分区键用 `instance_id`（保证同实例有序） |
| 3 | **指标与告警是否同 topic** | 未确认 | 分开（语义与消费频率都不同） |
| 4 | **客户指标库「采集更新周期」与 TPOPS `collection_frequency` 的对齐口径** | 未确认 | 按客户周期调度；平台原生周期仅用于解释数据点密度 |
| 5 | **告警字段规范** | 未确认 | 收敛列表作主条目，明细行挂 `details` |
| 6 | **不可提供 3 个指标的处理** | 已确认不可提供 | 丢弃并计数上报 + 交付清单 |
| 7 | **慢 SQL 的 `sql_text` 是否可外发** | 未确认（可能含业务 SQL 与参数） | **默认脱敏或截断**，需客户书面确认 |

### 5.2 技术风险

| 风险 | 影响 | 应对 |
|---|---|---|
| 命令中心并发压力未知 | 10 条并发可能触发平台限流或节点负载 | 并发数可配；实测压测；失败自动降级串行 |
| 累计值差分在实例重启后失真 | 速率指标出现假峰值/负数 | 检测归零 → 丢弃本轮 + 重建基线 + 告警日志 |
| 快照型指标无平台历史 | 采集端停机即数据空洞 | 水位续传只能补"当前值"，空洞不可回补 → 监控自身可用性 |
| 历史告警 30 天留存 | 长时间停机丢告警 | 显式告警日志 + 采集端自身可用性监控 |
| `result` 字段二次解析 | 上游格式变化会静默变成字符串 | 解析失败显式抛异常，不吞 |
| 节点任务 `succeed` 但命令实际失败 | 误判成功，数据为空 | 校验 `result` 内容；空结果单独标记 |
| 客户字段与 TPOPS 语义差异（近似对应 10 个、需换算 6 个） | 客户看到的值口径不符 | 每条映射规则的 `note` 写明口径，交付文档同步 |
| Python 3.7.9 停止维护 | 新版依赖装不上 | 锁版本；不用 3.8+ 语法 |

### 5.3 已在方案中消解的坑

命令中心三跳、`job_status` 不可靠、`result` 二次解析、掩码 `sql_text`、
方言差异（无 `FILTER`、`state <> ''` 恒假）、多节点下发失败、
指标窗口 ≥1h、入库滞后 5min、`is_latest_point_only` 丢时间戳、
分组名写错返回空、登录失败锁号、告警 30 天留存、告警 `limit` ≤100、
两个路径前缀 —— 全部来自实测留档，已在 §2.6 逐条记录，实现时按此编码。
