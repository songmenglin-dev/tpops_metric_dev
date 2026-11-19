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

分布式实例 `gauss-6374`（mode=Combined，**3 个 CN 全是 master** + 9 个 DN）的节点清单有
**12 条条目 / 3 个唯一 node_id**（每个 node_id 同时以 CN 与 DN 各出现一次），其中 **6 条 `role=master`**。

实测：**一次下发 3 个 master 成功** —— HTTP 200，返回 3 条 `node_tasks`，全部 `succeed`，
一次轮询就拿回 3 组行。

> ⚠️ 早期方案里的"一次下发多节点会失败"是**错的** —— 那条结论来自
> 命令中心的 SHELL 命令实测留档，是 **SHELL 命令**
> 的 `result` 里出现 `Failed to execute command on all nodes.`，被错误推广到了 SQL 命令。
> 已实测证伪（多 master 探针已随一次性产物清理，结论见 `docs/开发方案-GaussDB指标与告警采集.md` §2.2.4）。

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

## 再合并：11 条 → **1 条** SQL

### 为什么还能再合

平台下发的**每一条** SQL 都要独立走一遍「建任务 → 分发到节点 → 收集 → 置完成」，
这段**固定开销约 5 秒**，与 SQL 复杂度无关（实测 `select 1 as x` 也要 5.06~5.61 秒，
18 列宽 SQL 5.28~5.67 秒，两者几乎一样）。

所以：**串行 N 条 = N × 5 秒**。11 条串行 ≈ 60 秒，合成 1 条 ≈ 8.3 秒（实测）。
合并不是为了少传数据，是为了**少付 N−1 次平台调度开销**。

### 怎么合

每个视图的 SQL 各自包成一个**标量子查询**（把「多行多列的表」压成「一个值」），
再用逗号并排放进同一行：

```sql
(select json_agg(t) from ( <原 SQL> ) t) as <sql_key>
```

- `from ( 原SQL ) t` —— 派生表：原 SQL 的结果变成一张有名字的表 `t`；
- `json_agg(t)` —— 聚合函数，把 `t` 的每行转成 JSON 对象（**key = 原列名**），
  首尾相接成 JSON 数组；没有 `GROUP BY`，整张表塌缩成**一个值**；
- 括号包住 → **标量子查询**（返回「一个值」而不是「一张表」）；
- 11 个标量子查询各占一列 → 结果是 **1 行 × 11 列**。

各子查询自带 `FROM/WHERE/GROUP BY/ORDER BY/LIMIT`，互不干扰 —— 所以
`activity_running`（`ORDER BY query_start DESC LIMIT 20`）与 `activity_top10`
（`ASC LIMIT 10`）排序方向相反也能共存，**R4 的限制在此不再成立**。

### 客户端解析

```
外层 json.loads  →  1 行 11 列的字典
按键拆开        →  每列是某视图的 JSON 数组字符串
每列再 json.loads →  该视图的行列表（列名与原视图一致）
```

⚠️ 平台有时把 JSON 列返回成**已解析的数组**、有时是**双重编码的字符串**，客户端两种都要兼容。

### 与单条查询的差异（映射层要归一化）

| | 单条路径 | 合并路径 |
|---|---|---|
| 数字 | 字符串 `"152661"` | JSON 数字 `152661` |
| NULL | 空字符串 `""` | JSON `null` |

### 实测（2026-11-18，落盘文件直接下发）

| 方案 | 耗时 | result 大小 |
|---|---|---|
| 合并 7 条（60 秒档）→ 1 条 | 6.39 ~ 6.99 s | 30.7 KB |
| **合并 11 条 → 1 条** | **8.25 ~ 8.40 s** | 53.2 KB |
| （对照）串行 11 条 | ≈ 60.5 s | — |

正确性：11 个视图的**列名与列数与单条完全一致**；`datname` 集合相同；
同一次对比中 9 个库里 8 个 `xact_commit` 完全相等（只有 `postgres` 差 264 —— 两次调用相隔 6 秒，
它是唯一在收事务的库）。

## 平台侧的三层限制（2026-11-18 实测）

| 层 | 上限 | 越界行为 |
|---|---|---|
| **① `limit` 参数** | **1000**（`param_config`: min=1, max=1000） | HTTP 400 `DBS.06030081`「校验失败，校验项 parma.limit」 |
| **② 结果集行数** | **1000 行**（平台模板 `select * from ({sql_text}) limit {limit}`） | **静默截断**（实测造 1500 行 → 返回 1000 行） |
| **③ 结果总体积** | **102400 B = 100 KB** | **整个任务失败**（不截断）：`agent run uni-op error, Error: output size(200022 B) exceeds the threshold(102400 B)` |
| ④ `sql_text` 长度 | 10000 字符（`param_config.pattern`） | HTTP 400 |

③ 是**真正的硬墙**，且是**每个节点各自算**（3 个 master 各出一份、各自 100 KB）。

### 合并 SQL 的体积余量（实测）

`sql_merge.sql` 当前 **53300 B，占上限 52%，余量 49100 B**：

| 视图 | 行数 | 字节 | 占比 |
|---|---|---|---|
| `dbstat_sum` | 76 | 22251 | **42%** |
| `stat_user_tables` | 68 | 17611 | **33%** |
| 其余 9 个视图 | — | 5348 | 12% |

⚠️ **单点故障风险**：两个视图占了 75%，而行数**会随实例表数量增长**
（现在 68 张表里 67 张是 `snapshot.*` 平台自己的元数据表）。业务库表一多，
`stat_user_tables` 涨到几百行 → 合并结果撞 100 KB → **整批 82 个指标全部取不到**
（一个视图撑爆，全批陪葬）。

→ 对策（按优先级）：
1. 给 `stat_user_tables` 加内层 `limit`（它现在是唯一没有内层 limit 的视图）；
2. 把 `stat_user_tables` + `dbstat_sum` 拆成**第二条**合并 SQL（易增长的单独一条），
   其余 9 个视图保持一条（5.3 KB，永远安全）→ 2 条 ≈ 12 秒，仍远优于 11 条的 60 秒；
3. 采集端对 result 长度做**自检**（> 90 KB 告警），提前发现而不是突然失败。

## 分页协议（每个内层子查询都带 limit / offset）

`merged_sql.json` 的 `paging` 段定义分页策略，生成脚本据此给**每个**内层子查询套分页窗口：

| 视图 | 分页 | 理由 |
|---|---|---|
| `stat_user_tables` | **分页** | 行数随实例表数量线性增长（实测 68~79 行 → 1 万张表即 2.6 MB） |
| `dbstat_sum` | **分页** | left join 结果随表数量线性增长；原 SQL 的 `limit 100` 只是护栏，分页时让位给外层窗口 |
| 其余 9 个视图 | 固定（每页全量） | 行数有界，合计仅 5.3 KB；`activity_running`（20 条）/ `activity_top10`（10 条）是**语义上限**，不是数据量限制 |

生成：

```bash
python tools/build_sql_merge.py                                  # 第 1 页 offset=0 limit=100
python tools/build_sql_merge.py --offset 100 --out /tmp/p2.sql    # 第 2 页
python tools/build_sql_merge.py --limit 200                       # 换页容量
```

**offset 只作用于可增长视图** —— 固定视图每页全量重复取。否则翻到第 2 页时
`stat_database` 之类会返回空行，客户端会误判「没有数据」。

### 实测（2026-11-18，gauss-6374，3 个 master）

| 页 | result 大小 | 耗时 | 视图行数 |
|---|---|---|---|
| 第 1 页 offset=0 | 53208 B（占上限 52%） | 8.26 s | 11 个视图全部有数据，最大 76 行 |
| 第 2 页 offset=100 | 8552 B（占 8%） | 8.24 s | 2 个可增长视图空、9 个固定视图照常全量 |

⚠️ **空页行为**：`json_agg` 对零行返回 NULL → 平台渲染成**空字符串 `''`**（不是 `[]`）。
客户端必须把 `''` / `null` 当成**空数组**处理，否则 `json.loads` 直接抛异常。

### 分页吞吐（1 万张表 = 100 页）

**并发分页实测**（2026-11-18，gauss-6374，3 个 master，每页一条合并 SQL）：

| 并发 | 页数 | 首轮墙钟 | 首轮成功 | 首轮失败 | 重试结果 |
|---|---|---|---|---|---|
| 3 | 3 | 10.88 s | 3/3 | 0 | — |
| 4 | 4 | 9.58 s | 3/4 | 1 | — |
| 8 | 8 | 13.27 s | 3/8 | **5** | 5 个**全部重试成功** |
| 16 | 16 | 24.79 s | 8/16 | **8** | 8 个**全部重试成功** |
| 32 | 32 | 15.43 s | 2/32 | **30** | 30 个重试成功 29（1 个再失败） |

失败信息**恒为这句假语法错误**：
`invalid SQL statement with syntax error. Please input correct and valid SQL statement`
—— SQL 本身没问题，是**平台侧参数竞争**。

**三个结论：**

1. **不能 100 次同时调。** 并发 32 时失败率 **94%**（30/32）。
2. **失败是随机的**（不是「前 N 个失败」——16 并发失败 8 个、32 并发失败 30 个，
   不呈单调），所以**必须逐条判 `status`，不能假设失败落在哪一批**。
3. **并发越高，总墙钟反而越长** —— 高并发省下的首轮时间被重试吃掉（重试是串行的，
   每页 ≈ 7 s）：

| 方案 | 总调用次数 | 估算总墙钟 | 有效吞吐 |
|---|---|---|---|
| 并发 3 | 100 | 10.88 + 0 ≈ **11 s / 3 页** | **0.28 页/s** |
| 并发 8 | 100 + ~62 次重试 | 13.27 + 62×7 ≈ **447 s** | 0.22 页/s |
| 并发 32 | 100 + ~940 次重试 | 15.43 + 940×7 ≈ **6.6 ks** | 0.15 页/s |

→ **最优是并发 3（保守用 2），且必须带重试。** 100 页 ≈ **6 分钟**一轮。

**这意味着表级指标在 1 万张表的实例上不可能进 60 秒档**，必须降频（≥ 300 秒档）
或按需拉取（只在客户台账新增表时翻页）。这是规模上限，**不是实现缺陷** —— 采集端要能
「一页一页追着走」并记录已取到哪个 offset。

## 关键产物

| 文件 | 内容 |
|---|---|
| `merged_sql.json` | 11 条合并 SQL 的正式定义：SQL 文本、来源视图、维度键、刷新档、覆盖指标数、**实测列集合**、备注 |
| **`sql_merge.sql`** | **11 条再合成 1 条的最终 SQL**（由 `tools/build_sql_merge.py` 从 `merged_sql.json` 机械生成，不手写） |
| `sql_merge_probe_result.json` | 合并 SQL 的实跑留档：job_id、三个 master 的原始 result、耗时 |
| `candidate_sqls.json` | 合并前的候选定义（无实测元数据） |
| `probe_result.json` | 真实调用的逐条留档：每条 SQL 每个 master 的 status / 行数 / 列名 / 耗时 |

实测脚本（可重跑）：

```bash
export TPOPS_USER=sml TPOPS_PASSWORD='...'
python tools/probe_merged_sql.py      # 11 条逐条跑
python tools/build_sql_merge.py       # 重新生成 sql_merge.sql（改视图定义后必须重跑）
python tools/build_sql_merge.py --tier 60 --out /tmp/sql_merge_60s.sql   # 只合 60 秒档的 7 条
```

脚本**故意用串行 + 重试**：实测并发 ≥ 4 会触发平台侧的参数竞争，产生假语法错误。
