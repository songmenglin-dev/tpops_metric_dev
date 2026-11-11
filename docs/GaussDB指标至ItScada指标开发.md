# GaussDB 指标至ItScada指标开发

## 1. 需求简介

| 项 | 内容 |
|---|---|
| **背景** | 客户不使用 TPOPS（GaussDB 监控平台）的管理界面，改为把多个数据库管理平台的监控/告警数据统一导入客户自有监控平台 |
| **本项目范围** | GaussDB 这一路：通过 TPOPS API 按时钟窗口拉取指标与告警 → 按客户字段规范映射 → 以约定格式写入 Kafka，由客户侧消费 |
| **形态** | **常驻进程**（调度 → 采集 → 转换 → 投递 → 成功后推进水Kafka） |
| **语言/运行时** | Python 3.7.9（语法红线严格）、Kafka PLAINTEXT、消息体 JSON |
| **交付节点** | 需求 2026-11-01 下达，**第一版 2026-12-15 交付** |
| **当前阶段** | 指标调研 |

**核心链路**

```
Scheduler（窗口 + 水位）
  → 采集器（A 监控指标 / B 命令中心 SQL / C 诊断优化 / D 实例 / E 告警）
  → TPOPS Client（mini-token 认证）
  → Normalizer → FieldMapper（映射表 + 取值规则） → Serializer（JSON）
  → Kafka Producer → 推进水位
```

---

## 2. 时间线

**锚点**：需求 11-01 下达 → 第一版 12-15 交付；投入窗口 **11-09 ~ 12-15 共 27 个工作日**。

| 阶段 | 起止 | 工作日 | 核心产出 |
|---|---|---|---|
| **P0** 需求与指标调研 | 11-01 ~ 11-08 | 5 | 指标调研实测数据留档 |
| **P1** 程序设计 | 11-09 ~ 11-13 | 5 | 程序设计方案 |
| **P2** 代码基础框架开发 | 11-16 ~ 11-20 | 5 | 可启动骨架 + TPOPS 客户端 |
| **P3** 采集映射推送开发 | 11-23 ~ 12-04 | 10 | A/B/C/D 类 + 告警全量上 Kafka |
| **P4** 调度资源与程序测试 | 12-07 ~ 12-11 | 5 | 常驻进程 + 映射表全量 + 交付文档 |
| **P5** 联调验收 | 12-14 ~ 12-15 | 2 | 验收报告、部署包 |

**投入核算**：合计约 **32 人天** 

---

## 3. API 调研

### 3.1 平台结构（结论性前提）

**本项目实际使用 12 个接口**（5 个指标接口 + 4 个前置接口 + 3 个告警/辅助接口），覆盖 98 个指标。

### 3.2 指标接口一览（各接口承担多少指标）

| # | 接口 | 方法 | 覆盖指标数 | 用途 |
|---|---|---|---|---|
| 1 | `POST /rds/v3/auth/mini-token` | POST | 0（前置） | 认证，取得 `X-Mini-Token` |
| 2 | `GET /rds/v3.2/instances` | GET | **1** | 实例列表与元数据（引擎版本） |
| 3 | `GET /rds/gaussdb/v3/instances/{id}/key-view-execute-node` | GET | 0（前置） | 取 master 节点 `node_id` |
| 4 | `POST /rds/v3/monitor/metrics` | POST | **11** | 监控指标时序（12 个分组码） |
| 5 | `GET /rds/v3/monitor/metrics/series?name=<GROUP>` | GET | 0（前置） | 指标目录（name/unit/alarm_id） |
| 6 | `POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}` | POST | **82** | 统一命令中心下发（通用 SQL） |
| 7 | `GET .../batch-exec-job/task-records?job_id=` | GET | 0（前置） | 第 2 跳：任务状态 |
| 8 | `GET .../batch-exec-job/{job_id}/task-record/{task_id}/detail` | GET | 0（前置） | 第 3 跳：业务结果 |
| 9 | `POST /rds/gaussdb/v3/instances/{id}/slow-sql-list` | POST | **3** | 慢 SQL 集合 |
| 10 | `POST /rds/gaussdb/v3/instances/{id}/top-sql-list` | POST | **1** | Top SQL 列表 |
| 11 | `POST /rds/v3/alarm/records` | POST | 告警线 | 告警记录（活动/历史 × 收敛/明细） |
| 12 | `GET /rds/v3/alarm/records/statistic` | GET | 告警线 | 告警数量统计 |

---

## 4. 指标取数情况

### 4.1 总量

| 结论 | 数值 |
|---|---|
| 客户指标库总数 | **101** |
| TPOPS **可提供** | **98**（97.03%），已逐指标真实调用 **98/98 通过** |
| TPOPS **无法提供** | **3** |

### 4.2 无法提供的 3 个指标（不产出消息，仅计数上报 + 书面清单）

| 客户指标 | 中文名 | 理由 |
|---|---|---|
| `gaussdb.Fastpath_function_call` | 快路径函数调用的连接数 | PG 侧无 fastpath 计数器，`pg_settings` 仅有 `gsi_fastpath_level` 开关 |
| `gaussdb.Disabled_conn` | 禁用的连接数 | PG 无「禁用连接」概念，仅有 `reserved_connections` 等预留连接 |
| `gaussdb.sendAgentName` | 发送设备名给 Agent | Agent 侧行为，非数据库可查询的监控指标 |

### 4.3 按取数通道分布（98 个可提供指标）

| 通道 | 接口 | 指标数 | 占比 | 性质 |
|---|---|---|---|---|
| **B 命令中心 SQL** | `POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}` | **82** | 81% | 快照型（`pg_stat_*` 累计量 → 需差分） |
| **A 监控指标接口** | `POST /rds/v3/monitor/metrics` | **11** | 11% | 真正的时序 |
| **C 诊断优化接口** | `slow-sql-list` / `top-sql-list` | **4** | 4% | 明细列表 |
| **D 实例接口** | `GET /rds/v3.2/instances` | **1** | 1% | 元数据 |
| **E 告警接口** | `POST /rds/v3/alarm/records` 等 | 无客户指标编号 | — | 独立消息线 |

### 4.4 按客户类别分布

| 类别 | 可提供 | 无法提供 | 类别 | 可提供 | 无法提供 |
|---|---|---|---|---|---|
| 事务信息 | 18 | 0 | 基本信息 | 10 | 0 |
| 性能信息 | 21 | 0 | 表信息 | 11 | 0 |
| 数据库信息 | 9 | 0 | Bgwriter 信息 | 8 | 0 |
| 连接情况 | 8 | **2** | 锁信息 | 5 | 0 |
| 慢查询信息 | 3 | 0 | 可用性 | 2 | 0 |
| 会话信息 | 2 | 0 | 数据库统计信息 | 1 | 0 |
| | | | Agent | 0 | **1** |

### 4.5 按匹配度分布（口径风险）

直接对应 **79** ｜ 近似对应 **10**（逐条带 `note`，交付前与客户确认） ｜ 需换算 **6** ｜ 可统计 **3**。

### 4.6 82 个 B 类指标的合并 SQL（11 条，已全部实测通过）

| # | sql_key | 来源视图 | 覆盖指标 | 刷新档 |
|---|---|---|---|---|
| 1 | `stat_database` | `pg_stat_database` | 26 | 1 分 |
| 2 | `stat_user_tables` | `pg_stat_user_tables` | 22 | 2 分 |
| 3 | `activity_agg` | `pg_stat_activity` | 13 | 1 分 |
| 4 | `activity_running` | `pg_stat_activity` | 1 | 1 分 |
| 5 | `activity_top10` | `pg_stat_activity` | 1 | 5 分 |
| 6 | `locks` | `pg_locks + pg_database` | 3 | 1 分 |
| 7 | `bgwriter` | `pg_stat_bgwriter` | 8 | 5 分 |
| 8 | `settings` | `pg_settings` | 4 | 1 分 |
| 9 | `database` | `pg_database` | 2 | 2 分 |
| 10 | `prepared_xacts` | `pg_prepared_xacts` | 1 | 1 分 |
| 11 | `dbstat_sum` | `pg_stat_database ⋈ pg_stat_user_tables` | 1 | 1 分 |
| | | | **82** | |

实测：Combined 实例 11/11 成功（70.7 s，含 1 次重试）、HA 实例 11/11 成功（55.0 s），
留档 `data/merged_sql/probe_result.json`。
