# B 类指标的合并 SQL

> 82 个 B 类指标（命令中心 SQL 通道）如何合并成少量宽 SQL —— 合并规则、实测结果、调度影响。

## 为什么要合并

客户 101 个指标里有 **82 个**走命令中心 SQL（`general_single_sql_query`）。
如果一个指标一条 SQL，一轮 82 次下发 + 82 次轮询 + 82 次取结果，**按实测 4~5 秒/条**算，
一轮要 **5~7 分钟** —— 远超客户最紧的 60 秒档。

合并的目标：**把下发次数从 82 降到 11**（60 秒档只需 7 条）。

## 合并规则

| # | 规则 | 说明 |
|---|---|---|
| **R1** | **同视图 + 同粒度 → 一条宽 SQL** | `pg_stat_database` 的 26 个指标都是"一行一个库" → 一条 SQL 全列取回 |
| **R2** | **派生列在 SQL 里算完** | `cache_hit_ratio`、`dead_tup_ratio` 用 `CASE WHEN`，客户端不再算 |
| **R3** | **列全部显式列出** | 不写 `select *` —— 平台的列名取自 `as` 别名，写错别名会拿到 PG 原名 |
| **R4** | **ORDER BY 只在必要时加，且方向必须与指标语义一致** | 见下方 R4 的坑 |
| **R5** | **一条 SQL 的刷新周期 = 它覆盖指标中最高需求档** | 见 `merged_sql.json` 的 `refresh_seconds` |
| **R6** | **不为了省一条 SQL 而牺牲可推导性** | 派生指标能由已有行推导就不单开 SQL（如 accessexclusive 锁） |

### R4 的坑（实测踩过）

`gaussdb.running.sql.limit.20` 与 `GAUSSDB.sessions_active_top10` 同源同粒度
（`pg_stat_activity` 的 active 会话），看起来能合并 —— **但两者要的排序方向相反**：

| 指标 | 语义 | 需要的排序 |
|---|---|---|
| `running.sql.limit.20` | 正在运行的 SQL | `query_start` **降序**（最新） |
| `sessions_active_top10` | 会话时间 TOP10 = 时长最长 | `query_start` **升序**（最早） |

一条 SQL 只能有一个 ORDER BY。两条都取"全量再客户端排序"的话，
`ORDER BY query_start ASC LIMIT 1000` 会截掉**最新**的那批（`running.sql` 需要的），
`DESC` 则会截掉**最早**的那批（top10 需要的）—— 两头都可能丢。

→ **拆成两条 SQL**，各自与实测留档逐字一致，客户端零推导。

## 合并结果：11 条覆盖 82 个指标

| # | sql_key | 来源视图 | 覆盖指标 | 刷新档 | 实测行数/节点 | 实测列数 |
|---|---|---|---|---|---|---|
| 1 | `stat_database` | `pg_stat_database` | **26** | 1 分 | 5 | 18 |
| 2 | `stat_user_tables` | `pg_stat_user_tables` | **22** | 2 分 | 68 | 13 |
| 3 | `activity_agg` | `pg_stat_activity`（按库+状态聚合） | **13** | 1 分 | 2 | 3 |
| 4 | `activity_running` | `pg_stat_activity`（active，`query<>''`，desc） | 1 | 1 分 | 5 | 5 |
| 5 | `activity_top10` | `pg_stat_activity`（active，asc） | 1 | 5 分 | 7 | 4 |
| 6 | `locks` | `pg_locks` ⋈ `pg_database` | 3 | 1 分 | 1 | 4 |
| 7 | `bgwriter` | `pg_stat_bgwriter` | 8 | 5 分 | 1 | 8 |
| 8 | `settings` | `pg_settings` | 4 | 1 分 | 4 | 3 |
| 9 | `database` | `pg_database` | 2 | 2 分 | 5 | 3 |
| 10 | `prepared_xacts` | `pg_prepared_xacts` | 1 | 1 分 | 1 | 1 |
| 11 | `dbstat_sum` | `pg_stat_database` ⋈ `pg_stat_user_tables` | 1 | 1 分 | 72 | 15 |
| | | | **82** | | | |

**分档后每轮实际要发的条数**：

| 档位 | 条数 | 说明 |
|---|---|---|
| **60 秒** | **7** | `stat_database` `activity_agg` `activity_running` `locks` `settings` `prepared_xacts` `dbstat_sum` |
| 120 秒 | +2 | `stat_user_tables` `database` |
| 300 秒 | +2 | `activity_top10` `bgwriter` |

→ **60 秒档只需 7 条 SQL**，不是 11 条。

## 多 master：一次下发全部 master

集中式实例 `gauss-6374` 的节点清单有 **12 条条目 / 3 个唯一 node_id**（每个 node_id
同时以 CN 与 DN 各出现一次），其中 **6 条 `role=master`**。

实测：**一次下发 3 个 master 成功** —— HTTP 200，返回 3 条 `node_tasks`，全部 `succeed`，
一次轮询就拿回 3 组行。

> ⚠️ 早期方案里的"一次下发多节点会失败"是**错的** —— 那条结论来自
> `data/command_center/results/07_task_result_matrix_u_shell_sync.json`，是 **SHELL 命令**
> 的 `result` 里出现 `Failed to execute command on all nodes.`，被错误推广到了 SQL 命令。
> 已实测证伪，见 `data/multi_master_probe/`。

**各 master 返回的计数值不同**（实测 `postgres.xact_commit` = 7265656 / 7218577 / 6315580），
所以：

- 采集端按 **(sql_key, master, 行)** 三层组织结果；
- 计数类指标的差分基线**必须按 master 分开**，汇总在基线之上做。

## 并发：平台的并发上限 ≈ 2~3，且并发**不省时间**

实测并发阶梯（同一实例、1 个 master、11 条 SQL）：

| 并发 | 失败数 | 墙钟(s) |
|---|---|---|
| 1 | 0 | 4.5 |
| 2 | 0 | 5.4 |
| 4 | **2** | 5.7 |
| 6 | **4** | 5.8 |
| 8 | **3** | 9.0 |
| 11 | **7** | 8.0 |

两个结论：

1. **并发 ≥ 4 开始出现随机失败**，错误信息是
   `"invalid SQL statement with syntax error. Please input correct and valid SQL statement"` ——
   **但 SQL 本身没问题**（同一条串行重跑就过；金丝雀测试 8/8 返回的列与提交的 SQL 完全对应，
   无参数串味）。这是平台侧的**参数竞争**，错误信息是误导性的。
2. **并发不省时间**：并发 1 是 4.5 秒，并发 11 是 8.0 秒（还带 7 个失败）。

→ **采集端用低并发（2）+ 逐条重试**，而不是方案里写的 `CMD_CONCURRENCY=9`。

**实测一轮耗时**（11 条串行 + 最多 3 次重试）：

| 实例 | 总墙钟 | 触发重试 | 最终失败 |
|---|---|---|---|
| HA（1 master） | 55.4 s | 1 | 0 |
| Combined（3 master） | 61.7 s | 0 | 0 |

→ 60 秒档只跑 **7 条** 时约 **35~42 秒**；并发 2 + 重试可压到 **~10 秒**。

## 关键产物

| 文件 | 内容 |
|---|---|
| `merged_sql.json` | 11 条合并 SQL 的正式定义：SQL 文本、来源视图、维度键、刷新档、覆盖指标数、**实测列集合**、备注 |
| `candidate_sqls.json` | 合并前的候选定义（无实测元数据） |
| `probe_result.json` | 真实调用的逐条留档：每条 SQL 每个 master 的 status / 行数 / 列名 / 耗时 |

实测脚本（可重跑）：

```bash
export TPOPS_USER=sml TPOPS_PASSWORD='...'
python tools/probe_merged_sql.py
```

脚本**故意用串行 + 重试**：实测并发 ≥ 4 会触发平台侧的参数竞争，产生假语法错误。
