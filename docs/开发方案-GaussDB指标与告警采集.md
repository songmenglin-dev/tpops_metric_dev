# GaussDB 监控数据采集与投递 开发方案

| 项 | 内容 |
|---|---|
| 版本 | v2.1（v2.0 重写版 + 里程碑时间线，取代 v1.0） |
| 状态 | 需求已定稿；方案修订中（评审 BLOCKER/MAJOR 待关闭）；**目标 2026-12-15 交付第一版**，详见 §7.1.1 |
| 适用范围 | GaussDB（V2.0-10.0.0）实例的**指标**与**告警**数据 |
| 采集端形态 | 常驻进程，持续循环运行（至少一次语义） |
| 投递通道 | Kafka，PLAINTEXT，消息体 JSON |
| 目标读者 | 开发、测试、运维、客户对接人 |

## 0. 文档说明

### 0.1 本文档的定位

本文是**采集端的软件工程设计文档**，按「需求 → 调研 → 需求定稿 → 技术设计 → 代码设计 → 测试 → 计划 → 风险」
的方法论组织。每一章都能独立回答一个问题：

| 章节 | 回答的问题 | 读者主要用在哪 |
|---|---|---|
| §1 需求分析 | 为什么要做、做成什么样、怎么算做完 | 立项评审、验收 |
| §2 调研 | 平台与客户指标库究竟提供了什么 | 理解可行性与边界 |
| §3 需求定稿 | 最终冻结的需求（可开发、可验收） | 开发依据、合同附件 |
| §4 技术设计 | 用什么接口、怎么取数、怎么调度 | 开发、调试 |
| §5 代码设计 | 代码长什么样 | 开发 |
| §6 测试策略 | 怎么证明它是对的 | 测试 |
| §7 计划 | 分几步做完 | 项目管理 |
| §8 风险 | 哪里会出事、怎么办 | 运维、项目管理 |
| §9 附录 | 全量指标表、留档索引、术语 | 查表 |

### 0.2 事实基础（本文不含未经验证的推测）

本文的每一条接口约束、每一条指标结论，都来自本仓库已完成的真实调用留档：

| 依据 | 位置 | 规模 |
|---|---|---|
| 逐指标真实调用结果 | `data/metric_verification/*.json` | 98 个指标，98/98 通过 |
| 统一命令中心实测响应 | `data/command_center/results/` | 197 份请求/响应留档 |
| 客户指标库原始表 | `高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` | 101 指标 / 13 类别 |
| 指标→取数方法判定 | `data/customer_metric_mapping.json`、`data/customer_metric_query_plan.json` | 101 条 |
| 告警侧原始数据 | `data/alarm/` | 5 组抓取 + 288 条告警定义 |
| 平台接口契约 | `docs/api/`（CHM 原文转换）、`data/tpops_api_catalog.json` | 全部接口 URI/入参/出参 |

> **引用规范**：本文出现的"实测"字样，均可在上述留档中找到对应文件。留档里没有的，
> 一律写成"设计决定"或"待确认"，不混用。

### 0.3 v2.0 相对 v1.0 的变更

v1.0 是一份**从接口调研直接跳到实现的文档**，存在三个结构性问题：

1. **没有需求分析**——开篇即指标调研，读者无法判断"为什么 82 个指标要合并成 10 条 SQL"是需求还是实现偏好。
2. **待确认项过多且悬在正文**——Topic 命名、消息格式、告警字段这些本该在开发前定死的东西，
   在 v1.0 里以"待客户确认"散落各处，导致映射层无法落地、测试无法写出期望值。
3. **接口"详解"缺失**——v1.0 只有接口清单，没有逐接口的请求/响应/错误处理契约，
   开发时仍要回翻 CHM 和留档。

v2.0 的做法：**先需求分析 → 调研 → 需求定稿（冻结）→ 技术设计 → 代码设计**，
把能定的全部定死，只留「不影响开发启动」的少数商务性事项给客户确认（§3.9）。

---

## 1. 需求分析

### 1.1 问题陈述与业务背景

客户侧已有一套统一的数据库监控平台（广东电网 ITSCADA）。该平台对**多源数据库**
（达梦、Oracle、MySQL、SQLServer、ClickHouse、TiDB 等）采用**主机 Agent 采集**方式接入。

**GaussDB 不走 Agent 路线。** 客户的接入指引对其余数据库采用**主机 Agent 采集**，
而 GaussDB 这一路由本项目承担——通过 TPOPS 的开放接口取数，再以 Kafka 投递
（见 `docs/广东电网ITSCADA系统监控接入操作指引（数据库）V0.4.docx` 与本项目目标）。

GaussDB 实例由华为云 TPOPS（GaussDB 监控平台）托管，**我方能访问的数据源是 TPOPS 的开放接口**，
而不是数据库本身。

因此本项目要解决的是：

> **把分散在 TPOPS 里的 GaussDB 指标与告警，按客户平台能消费的格式，源源不断地送过去。**

这不是"调几个接口取个数"的问题，而是一个**数据管道（pipeline）产品**：

```
TPOPS（源） → 采集（Pull） → 归一化（Normalize） → 映射（Map） → 序列化 → Kafka → 客户平台（汇）
```

它的工程属性与一次性脚本完全不同：

- **常驻**：不是跑一次就退出，而是 7×24 循环。
- **有状态**：累计计数类指标要跨轮差分，必须有持久化的水位与基线。
- **有节奏**：101 个指标的刷新周期从 30 秒到 24 小时不等，必须分档调度。
- **有交付**：数据不对，客户看板就是错的——**错的数据比没有数据更危险**。

### 1.2 干系人与使用场景

| 干系人 | 诉求 | 对本方案的影响 |
|---|---|---|
| 客户监控平台（消费方） | 收到格式统一、字段齐全、时间戳可信的指标流与告警流 | 消息格式必须冻结（§3.2）；时间戳语义必须明确（§3.5） |
| 客户运维（告警接收方） | 告警及时、不重复、可定位到节点与原因 | 告警需幂等去重；明细行需挂载原因与处置步骤（§4.2.7） |
| 客户对接人 | 指标覆盖度可核对、口径可解释 | 不可提供指标要有书面清单与理由；口径换算要标注（§3.6） |
| 采集端运维（本方） | 进程稳定、可观测、出问题能定位 | 重试退避、优雅退出、每轮汇总日志（§4.7、§4.9） |
| 数据合规 | 慢 SQL 文本含业务信息 | 默认脱敏/截断，且可配置（§3.8） |

**典型使用场景**

- S1：客户大屏实时刷新 CPU/内存/连接数/事务率 → 依赖 30 秒~1 分钟档指标。
- S2：客户在 ITSCADA 上按实例+数据库+表查看行数、膨胀率 → 依赖维度展开（§3.5）。
- S3：客户值班人员收到告警后定位到具体节点 → 依赖告警明细行。
- S4：客户月度报表统计事务量 → 依赖累计计数指标的正确差分。
- S5：客户问"某指标你们为什么没给" → 依赖不可提供清单（§3.6）。

### 1.3 需求边界

**范围内（In Scope）**

| # | 范围 |
|---|---|
| I1 | 从 TPOPS 拉取客户指标库中 **101 个指标**的可提供部分（98 个） |
| I2 | 从 TPOPS 拉取**活动告警与历史告警**（收敛列表 + 明细行）及告警数量统计 |
| I3 | 按客户指标库的**字段规范**（数据类型 / 更新周期 / 单位）产出数据 |
| I4 | 以 **JSON** 格式写入 **Kafka（PLAINTEXT）** |
| I5 | 常驻运行：分档调度、水位续传、失败重试、优雅退出 |
| I6 | 3 个不可提供指标的**丢弃与计数上报**，附书面理由清单 |

**范围外（Out of Scope）**

| # | 不做 | 理由 |
|---|---|---|
| O1 | 客户监控平台本身的开发与部署 | 客户已有 ITSCADA |
| O2 | 达梦/Oracle/MySQL 等其他数据库的采集 | 走主机 Agent，非本项目路径 |
| O3 | TPOPS 平台自身的部署、升级、告警规则配置 | 我方是消费方，不是平台管理员 |
| O4 | 数据库侧的数据治理（慢 SQL 优化建议等） | 超出"数据接入"职责 |
| O5 | 数据的长期存储与历史回放 | Kafka 消费者侧职责；平台侧只存 30 天告警，采集端不补历史 |
| O6 | 认证/授权体系的改造 | 使用平台既有 mini-token 机制 |

### 1.4 功能需求（FR）

采用 EARS 风格的**可验证**表述——每条都给出验收方式，避免"系统应具有良好的性能"这类无法验收的句子。

| ID | 需求（EARS 表述） | 验收方式 |
|---|---|---|
| FR-01 | 采集端**应当**按客户指标库的「采集更新周期 + 采集更新周期单位」分档调度，档位集合为 {30 秒, 60 秒, 1 分, 2 分, 5 分, 1 时, 24 时} | 单测：调度器在给定周期下产生与档位表一致的到期序列 |
| FR-02 | 采集端**应当**产出客户指标库 101 个指标中**全部 98 个可提供指标**的数据 | 集成：98 个指标各至少产出一条消息 |
| FR-03 | 采集端**应当**为每个产出值标注**采集时刻**与**数据时间**（平台时序取平台时间戳，快照取采集时刻） | 集成：断言消息含两个时间字段且语义符合 §3.2 |
| FR-04 | 采集端**应当**按客户指标库的「数据类型」把值序列化为整数/浮点/字符/文本四种类型之一 | 单元：类型转换矩阵全覆盖 |
| FR-05 | 采集端**应当**按客户指标库的「指标单位」填充单位字段，缺省时填 `null` 而非空串 | 单元：对照 xlsx 逐条比对 |
| FR-06 | 采集端**应当**对含占位符（`{#NAME}`/`{#DBNAME}`/`{#DATABASE_NAME}`/`{#PORT}`）的指标**按维度展开**，一行数据产出一条消息 | 集成：`gaussdb.get.dbstat.sum` 展开后行数 = SQL 返回行数 |
| FR-07 | 采集端**应当**对累计计数类指标做**跨轮差分**换算为速率，且在实例重启导致计数归零时**丢弃该轮差分**并告警 | 单元：注入归零样本，断言不产生负数/假峰值 |
| FR-08 | 采集端**应当**在**全部投递成功**后才推进水位；投递失败**不得**推进 | 集成：注入投递失败，断言水位未变 |
| FR-09 | 采集端**应当**在重启后从水位续传，对未成功投递的数据**不丢失** | 集成：投递中断→重启→断言补齐 |
| FR-10 | 采集端**应当**对可重试的失败执行**指数退避重试**，且**认证失败不重试**（防账号锁定） | 单元：注入 401，断言不发起第二次认证 |
| FR-11 | 采集端**应当**对告警按幂等键 `(alarm_id, instance_id, node_id, first_alarm_time)` **去重** | 单元：重复投递同一告警，断言仅一条 |
| FR-12 | 采集端**应当**在收到 SIGTERM/SIGINT 时**停止发起新请求**、等待在途请求完成、刷盘水位后退出，退出码 0 | 集成：发信号，断言退出码与水位完整性 |
| FR-13 | 采集端**应当**对 3 个不可提供指标**不产出数据**，但**每轮计数上报**并在交付时附书面清单 | 单元：断言无消息产出 + 计数递增 |
| FR-14 | 采集端**应当**将慢 SQL 的 `sql_text` **默认脱敏或截断**，且该行为可通过配置关闭 | 单元：断言默认输出不含明文 SQL |
| FR-15 | 采集端**应当**在指标查询窗口内**回退 end_time** 以规避平台入库滞后，并在**使用 ≥1 小时窗口**以覆盖 3600s 采集周期的指标 | 单元：断言窗口参数计算符合 §4.2.4 |
| FR-16 | 采集端**应当**使用 `is_latest_point_only="false"` 以保留平台时间戳 | 单元：断言请求体该字段为字符串 `"false"` |
| FR-17 | 采集端**应当**对告警的 `limit`（≤100）做**分页**处理 | 集成：构造 >100 条，断言取全 |
| FR-18 | 采集端**应当**在平台历史告警留存窗口（30 天）之外存在数据缺口时**显式告警日志** | 单元：注入超期记录，断言产生告警级日志 |

### 1.5 非功能需求（NFR）

| ID | 需求 | 目标值 | 度量方式 |
|---|---|---|---|
| NFR-01 | **采集周期达成率** | 60 秒档在正常负载下 P95 采集耗时 ≤ 25 秒（余量 2.4×） | 每轮耗时日志统计 |
| NFR-02 | **单轮外部调用数** | ≤ 42 次（A 类 1 + 命令中心 30 + 诊断 2 + 告警 5 + 实例 1 + 前置 1 + 认证 1） | 调用计数埋点 |
| NFR-03 | **数据完整性** | 可提供指标投递成功率 ≥ 99%（按轮次统计） | 调度器计数 |
| NFR-04 | **幂等性** | 同一采集轮次重启后不产生重复消息（客户侧仍需按幂等键去重） | 水位测试 |
| NFR-05 | **可观测性** | 每轮输出一行结构化汇总（拉取/转换/投递/失败/耗时/丢弃） | 日志断言 |
| NFR-06 | **凭据安全** | 密码/token **不入日志、不入消息、不入库**；仅来自环境变量 | 日志扫描 + 代码审查 |
| NFR-07 | **资源占用** | 空闲时 CPU < 1%，常驻内存 < 200 MB（2 实例规模） | 压测观测 |
| NFR-08 | **可维护性** | 采集端单文件 < 400 行、函数 < 50 行；映射**表驱动**，不写死在代码里 | 代码评审 |
| NFR-09 | **可移植性** | 运行时 Python **3.7.9**；依赖锁定到兼容 3.7 的版本 | CI 矩阵 |
| NFR-10 | **测试覆盖** | 单元/集成覆盖率 ≥ 80%，**映射层与规则引擎 ≥ 95%** | CI 覆盖率门禁 |

### 1.6 约束条件

| 类别 | 约束 | 来源 |
|---|---|---|
| **技术** | 运行时 Python 3.7.9（3.7 已停止维护，新版依赖装不上） | 客户环境 |
| **技术** | Kafka PLAINTEXT（无 SASL/TLS），消息体 JSON | 客户平台 |
| **平台** | TPOPS 使用**自签证书** → 客户端必须 `verify_tls=False` | 实测 |
| **平台** | 平台有两套路径前缀（`/rds/v3/…` 与 `/rds/gaussdb/v3/…`），少一段即 404 | 实测 |
| **平台** | 登录失败累计 `login_failed_attempts` 并**锁号** → 认证失败禁止盲目重试 | 实测 |
| **平台** | 统一命令中心**只能在 master 节点执行**（命令级约束 `limit_node_types=["master"]`） | 实测（2026-11-06） |
| **平台** | 平台**不保存历史快照**，快照型指标的历史只能由采集端自己攒 | 实测 |
| **平台** | 历史告警**仅保留 30 天** | 实测 |
| **平台** | 指标查询窗口 **≥ 1 小时**才能覆盖 3600s 采集周期的指标 | 实测 |
| **平台** | 平台数据入库**滞后约 5 分钟** | 实测 |
| **客户** | 客户指标英文名存在 OCR 错字（如 `ansactions`、`mmitted`） | 客户表 |
| **客户** | 客户指标库**未定义 JSON 消息字段**（只定义了指标维度） | 客户表 |
| **合规** | 慢 SQL 文本可能含业务 SQL 与参数，外发需客户书面确认 | 内部判断 |

### 1.7 验收标准（Definition of Done）

| # | 验收项 | 判定 |
|---|---|---|
| D1 | 98 个可提供指标全部产出数据，字段与客户指标库 9 列规范逐条对齐 | 逐指标核对表签字 |
| D2 | 3 个不可提供指标不出数据、有书面理由、计数可查 | 清单核对 |
| D3 | 消息 JSON 符合 §3.2 冻结的格式 | schema 校验通过 |
| D4 | 连续运行 ≥ 24 小时，60 秒档达成率 ≥ 99%，无进程崩溃 | 运行日志 |
| D5 | 杀进程→重启，数据无缺口（从水位续传），无重复投递 | 中断演练 |
| D6 | 单元/集成覆盖率 ≥ 80% | CI 报告 |
| D7 | 认证失败场景验证：连续 3 次错误密码后账号**未**被锁定 | 演练记录 |
| D8 | 慢 SQL 文本默认脱敏 | 配置断言 |

---

## 2. 调研

### 2.1 客户指标库调研

**来源**：`高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` → `指标库` sheet（101 行）、`指标库-指标获取方法` sheet（101 行）。

#### 2.1.1 指标库给出了什么

| 列 | 含义 | 采集端如何使用 |
|---|---|---|
| 适用资源类型 | 全部为 `GAUSSDB实例` | 资源维度 |
| 指标分类 | 13 个类别 | 消息分组与客户看板对应 |
| 中文名 | 人读名称 | 消息 `metric_name_cn` |
| **英文名** | **客户侧唯一标识** | 消息 `metric_name`（**注意 OCR 错字**） |
| **数据类型** | 整数 79 / 字符 13 / 浮点 7 / 文本 2 | 消息 `value_type` + 序列化类型 |
| **数据更新周期** | 数字 | 与下一列合成周期 |
| **采集更新周期单位** | 秒 / 分 / 时 | 合成周期 |
| 生成性能基线 | 是/否 | 本项目不产出性能基线（客户未要求计算） |
| **指标单位** | 12 种取值 + 49 项缺省 | 消息 `unit` |

#### 2.1.2 类别 × 周期分布

| 客户类别 | 指标数 | 30秒 | 60秒 | 1分 | 2分 | 5分 | 1时 | 24时 |
|---|---|---|---|---|---|---|---|---|
| 事务信息 | 18 | | | | | 18 | | |
| 慢查询信息 | 3 | | | | | 3 | | |
| 可用性 | 2 | | | 2 | | | | |
| 锁信息 | 5 | | 1 | 2 | 2 | | | |
| 性能信息 | 21 | | | 17 | 2 | 2 | | |
| 连接情况 | 10 | | | 10 | | | | |
| 基本信息 | 10 | 1 | 2 | 1 | | 3 | 1 | 2 |
| 表信息 | 11 | | | | 11 | | | |
| 数据库统计信息 | 1 | | | | 1 | | | |
| Agent | 1 | | | | | 1 | | |
| Bgwriter信息 | 8 | | | | | 8 | | |
| 数据库信息 | 9 | | | | | 9 | | |
| 会话信息 | 2 | | | | | 2 | | |
| **合计** | **101** | **1** | **3** | **32** | **16** | **46** | **1** | **2** |

**关键发现 1：最快档只有 1 个指标（30 秒），且它走的是 A 类监控指标接口。**
这直接决定了调度难度——**没有任何一个命令中心（SQL）指标在 30 秒档**，
所以"命令中心 SQL 合并"的工作只需在 60 秒档内完成（§4.6）。

**关键发现 2：1/2/5 分钟三档占了 94 个指标（93%）。**
这三档的采集节奏基本一致，可以共用一次命令中心批量取数（§4.3）。

**关键发现 3：24 小时档 2 个指标 + 1 小时档 1 个指标**，全部是准静态元数据
（字符集、时区、版本），可用极低频率采集。

#### 2.1.3 客户指标的三个"坑"

| 坑 | 表现 | 应对 |
|---|---|---|
| **英文名 OCR 错字** | `Number_of_ansactions_mmitted_r_cond`（应为 `transactions_committed_per_second`）、`Number_of_llback_ansactions_r_cond` | **原样保留**，不擅自纠正——客户侧主键就是这个名字，改了就对不上；同时在交付文档中列出疑似错字清单请客户确认 |
| **占位符维度** | `{#NAME}`（表）、`{#DBNAME}`/`{#DATABASE_NAME}`（库）、`{#PORT}`（端口） | 一条 SQL 返回 N 行 → 展开为 N 条消息（§3.5） |
| **未定义 JSON 消息格式** | 客户表只定义了"指标"维度，**没定义消息信封** | 本项目冻结一个信封（§3.2），客户确认后只改映射表不改代码 |

### 2.2 TPOPS 平台调研

**来源**：`docs/api/`（CHM 原文转换）、`data/tpops_api_catalog.json`、`data/command_center/results/`（197 份实测留档）、`data/metric_verification/`（98 份实测留档）。

#### 2.2.1 平台结构

平台有两套 API 命名空间，**路径前缀错了就是 404**（不是"接口没实现"）：

| 命名空间 | 覆盖能力 | 本项目用到 |
|---|---|---|
| `/rds/v3/…` | 认证、实例列表、监控指标、指标目录、告警记录、告警统计 | ✅ |
| `/rds/gaussdb/v3/instances/{id}/…` | 数据库诊断优化接口集 + 统一命令中心 | ✅ |

> 86 个客户指标依赖 `/rds/gaussdb/v3/` 这一路。去掉 `gaussdb` 段等于丢掉最大的一块。

#### 2.2.2 认证机制

| 项 | 实测结论 |
|---|---|
| 接口 | `POST /rds/v3/auth/mini-token`，body `{"username","password"}` |
| Token 位置 | **响应头 `X-Mini-Token`**，不在 body 里 |
| 有效期 | 服务端 `session_timeout = 1440` 分钟（24 小时） |
| 锁号 | 登录失败累计 `login_failed_attempts`；达到阈值**锁号** |
| 后续请求 | 头 `X-Mini-Token: <token>`，`X-Language: zh-cn` 可选 |
| 证书 | **自签证书** → `verify_tls=False` |

#### 2.2.3 监控指标接口（A 类）

| 项 | 实测结论 |
|---|---|
| 接口 | `POST /rds/v3/monitor/metrics` |
| 请求 | `start_time`/`end_time`（**13 位毫秒**）、`is_latest_point_only`、`instances[]{instance_id}`、`metric_names[]`（**数组，支持批量**） |
| 响应 | `metric_infos[]`（**一项一个实例**）→ 含 `instance_id`、`time_interval`、`metrics[]`；每个 `metrics[]` 含 `name`、`metric_name`（**中文名只在这里**）、`collection_frequency`、`node_id`、`timestamps[]`、`series{}` |
| **series 的 key** | 是**组件 ID**（如 `cn_5001`/`cn_5002`/`cn_5003`、`dn_6001`），DN 侧是 `<主机IP>-<组件ID>`（如 `172.16.103.126-dn_6005`），**不是主机 IP**；key 里**不含 node_id**，需经节点清单接口的 `component_id` 反查，而 ITSCADA 的 `resourceName` 要 `{instance_name}_{node_id}` → **待客户确认口径**（见 §4.2.4） |
| **取数口径** | **取最新点**：① `is_latest_point_only=true`（只要最新点）② `is_latest_point_only=false` + ≥1h 窗口（补真实时间戳）；两次用同一个 `end_time`，最新点即时序末尾那点 |
| **实例范围** | 一次**只查一个实例**（与命令中心通道同一个分布式实例）；命令中心无法一次带多实例 → 采集端**按实例循环** |
| **窗口约束** | **≥ 1 小时**才能覆盖 `collection_frequency=3600` 的指标；5 分钟窗口覆盖 0/21 |
| **入库滞后** | 约 **5 分钟** → `end_time` 必须回退，否则尾部大量 null（5 分钟窗口实测 null 率 **77.5%**） |
| **致命坑** | `is_latest_point_only=true` 时约 **74%** 的指标 `timestamp=0` 且 `node_id=null`（值对、时间丢）→ 用 ② 补时间戳，或用采集时刻兜底 |
| 指标目录 | `GET /rds/v3/monitor/metrics/series?name=<GROUP>`；12 个分组码**必须硬编码**（平台无枚举接口），**写错分组名返回 200 + 空 series 不报错** |

平台采集周期档位（`collection_frequency`）：10s×29、30s×2、60s×152、300s×12、3600s×21。

#### 2.2.4 统一命令中心（B 类）

平台不提供 pg_stat 类监控指标，但内置命令 `general_single_sql_query`
（`cmd_id = 73722acc-bff6-4d04-adc0-bc1e1fa1eb7d`，`source=default`，`creator=admin`）
的模板是：

```
select * from ({sql_text}) limit {limit}
```

**平台提供执行器，我们提供 SQL** —— 这是 82 个客户指标的主通道。

| 项 | 实测结论 |
|---|---|
| 执行位置 | **只能在 master 节点执行**（`constraint_config.limit_node_types=["master"]`）—— **命令级约束，不是指标级**：82 个 B 类指标是顺带继承的 |
| 前置接口 | `GET /rds/gaussdb/v3/instances/{id}/key-view-execute-node` → 取 `role=master` 的 `node_id` |
| 多节点 | **一次下发多个 master 节点实测成功**（2026-11-06：HTTP 200，返回多条 `node_tasks` 全部 succeed）→ 可一次下发全部 master |
| 异步模型 | 下发返回 `job_id` + `node_tasks[]{task_id}`（**不含数据**） |
| 结束判据 | **`statistics.running == 0`**，**不能看 `job_status`**（实测任务已 `succeed` 而 `job_status` 仍停在 `running`） |
| 结果 | `result` 字段是 **JSON 字符串**，需**二次 `json.loads`** 才是行列表 |
| SQL 掩码 | `sql_text` 在任务记录里被掩码为 `******`（执行不受影响，**不能靠任务记录回读 SQL**） |
| 列名 | **取自 SQL 的 `as` 别名** —— 自定义 SQL 必须显式写别名，否则拿到 PG 原始列名 |
| 参数约束 | `sql_text` 必填（pattern `^(select\|SELECT\|show\|SHOW)(.){1,10000}$`）、`limit` 必填（1~1000） |
| 单次往返 | **3~8 秒**；实测 50 条 SQL 有 **3 条偶发失败，重试即成功** |
| 集群模式参数 | `cluster_mode` 必须是 **JSON 数组**，传字符串（`"Ha"`）直接 400 |

**方言约束（GaussDB 是 PG 9.2 级）**

| 约束 | 表现 | 应对 |
|---|---|---|
| 无 `FILTER` 子句 | 语法错误 | 条件聚合改用 `CASE WHEN` |
| `state <> ''` 恒为假 | 过滤失效 | 排除后台进程用 `state is not null` |
| 累计计数类 | 返回单调累计值 | 采集端自己差分（§4.5） |
| 无历史快照 | 每次只有当前值 | 采集端自己轮询攒时序 |

#### 2.2.5 诊断优化接口（C 类）

| 项 | 实测结论 |
|---|---|
| 慢 SQL 集合 | `POST /rds/gaussdb/v3/instances/{id}/slow-sql-list`；body 含 `start_time`/`end_time`/`instance_id`/`threshold`/`threshold_unit`/`node_ids` |
| 慢 SQL 窗口 | **上限 24 小时**（超出 48h/7d/30d 一律 400「参数错误」） |
| 慢 SQL 阈值 | `threshold` **必传**（ms 取 1~99000，s 取 1~99），`threshold_unit` 可选（ms/s） |
| Top SQL 列表 | `POST /rds/gaussdb/v3/instances/{id}/top-sql-list`；**需传全部 CN 节点 id**（不是只传 master），且**必须按 node_id 去重**（否则报 `DBS.06030136 节点数量超出限制`） |
| 实测结果 | 两个接口均 **HTTP 200**，但当前实例**无慢 SQL 数据**（`slow_sql_infos[]` / `top_sql_infos[]` 为空） |

> 说明：早前方案记录过 slow-sql-list 返回 400 的现象；2026-11-05 的实测留档显示
> 按 `tools/verify_metric_sources.py` 的请求体（窗口 24h、`threshold=1000ms`）调用**返回 200**。
> 以留档为准。

#### 2.2.6 实例接口（D 类）

`GET /rds/v3.2/instances?limit=100&offset=0` → `total_count` + `instances[]`
（`id`/`name`/`mode`/`created_at`/`instance_status`/`engine_name`/`engine_version`/`agent_info`）。

实测环境有 2 个实例：`gauss-6374`（Combined，**分布式版**，3 个 CN 均为 master）、`gauss-75a2`（Ha，集中式版，1 个 master DN），均为 `V2.0-10.0.0`。**本项目的目标实例是 `gauss-6374`**（命令中心、监控指标、诊断优化、逐指标实测留档全部用它）。

#### 2.2.7 告警接口（E 类）

| 项 | 实测结论 |
|---|---|
| 记录列表 | `POST /rds/v3/alarm/records`；`is_active`、`is_convergence` **必传**；`limit` **1~100**，超出需 `offset` 分页 |
| 数量统计 | `GET /rds/v3/alarm/records/statistic` → 系统侧/实例侧各 4 个等级（notice/minor/major/critical）计数 |
| 历史留存 | **仅 30 天** |
| 收敛列表 | 每行 = **实例 + alarm_id** 一行，含 `alarm_items_sum` |
| 明细行 | 每**节点**一行，含 `description`/`cause`/`impaction`/`possible_causes`/`processing_steps`/`host_ip`/`node_id`/`alarm_detail_raise_count` |

实测条数：活动收敛 2 / 活动明细 3 / 历史收敛 4 / 历史明细 32。

告警定义目录（平台**没有查询接口**，从 CHM 手工整理）：

| 文件 | 条数 | 说明 |
|---|---|---|
| `data/alarm/catalog_metric.json` | 85 | 指标类告警：告警ID、名称、归属、默认阈值与等级、震荡/恢复持续时间、关联指标、采集间隔 |
| `data/alarm/catalog_event.json` | 203 | 事件类告警：告警ID、名称、默认级别、告警类型、归属、是否内核告警、是否可自动清除 |

**关联方式**：告警记录里的 `alarm_id` 与目录里的「告警ID」是**同一套 ID** → 按 ID 关联，**不依赖名称匹配**。
注意 ID 空间不统一（既有 `200xxxxxxx` 也有 `50xxxxx` 两种形态）。

#### 2.2.8 健康检查接口 `GET /rds/health-status`

**实测留档**：`data/api_probe/health_status.json`（脚本 `tools/fetch_tpops_health_status.py`，可重跑）

| 探针 | 请求 | 实测结果 |
|---|---|---|
| `no_auth` | `GET /rds/health-status`，**不带任何头** | **HTTP 200，响应体为空**（`Content-Length: 0`），0.229 s |
| `with_token` | 同上，带 `X-Mini-Token` + `X-Language` | **HTTP 200，响应体为空**，与不带 token **完全一致** |
| `bare_prefix` | `GET /health-status`（少 `/rds` 前缀） | **HTTP 404**（Tomcat 默认错误页） |

三条结论：

1. **该接口不需要认证**（权限文档里它的权限项就是"无"）；
2. **响应体是空的** —— 只通过**状态码**表达信息，**不返回任何 JSON，不含实例/节点/端口/数据库名**；
3. **`/rds` 前缀必须**，少一段即 404，与"平台有两个命名空间"的结论一致。

> **修正**：开发方案早期版本与 `data/customer_metric_mapping.json` 曾写
> "可用性探测需 GET /rds/health-status" —— **被本次实测证伪**。
> 该接口回答的是"平台自己的 open-api 服务是否活着"，
> 回答不了"这个实例的端口通不通 / 数据库是否健康"，**不能作为任何客户指标的数据源**。
> `gaussdb_{#PORT}port.status` 与 `GAUSSDB.Service_availability_testing` 的可用性探测需另找来源
> （候选：`instances[].instance_status`、`pg_is_in_recovery()`、`gaussdb_instance_status`）。

#### 2.2.9 调研阶段踩过的坑（实现必须规避）

| # | 坑 | 现象 | 规避 |
|---|---|---|---|
| 1 | 路径前缀缺 `gaussdb` | 404 | 两条命名空间的接口清单分开维护 |
| 2 | Token 在 body 找不到 | 认证"成功"但后续全 401 | 从**响应头** `X-Mini-Token` 取 |
| 3 | 短窗口取指标 | 覆盖率 0/21 | 窗口 ≥ 1 小时 |
| 4 | 不回退 end_time | 尾部 null 率 77.5% | 回退 5 分钟 |
| 5 | `is_latest_point_only=true` | 74% 指标 timestamp=0 | 用字符串 `"false"` |
| 6 | 分组名写错 | 200 + 空 series，不报错 | 空分组告警是唯一发现手段 |
| 7 | 看 `job_status` 判完成 | 永远等不到 | 用 `statistics.running == 0` |
| 8 | `result` 当数组用 | 拿到字符串 | 二次 `json.loads` |
| 9 | SQL 不写 `as` 别名 | 列名变成 PG 原名 | 全部显式别名 |
| 10 | 用了 `FILTER` | 语法错误 | `CASE WHEN` |
| 11 | `state <> ''` | 恒假，过滤失效 | `state is not null` |
| 12 | 一次下发多节点 | ~~执行失败~~ **该结论已证伪**：2026-11-06 实测一次下发 3 个 master 全部 succeed。原结论源自 SHELL 命令的 `Failed to execute command on all nodes.`，被错误推广到 SQL 命令 | 见 §2.2.4 修正 |
| 13 | SQL 无重试 | 3/50 偶发失败 | 单条重试 |
| 14 | `cluster_mode` 传字符串 | 400 | 传 JSON 数组 |
| 15 | `sql_text` 掩码 | 无法从任务记录回读 SQL | SQL 留在本地配置里 |
| 16 | 命令成功但 result 是失败文案 | 误判成功 | **校验 result 内容**，不只看 status |
| 17 | 认证失败盲目重试 | **锁号** | 认证失败不重试 |

### 2.3 调研结论：四条取数通道

平台的监控面一共只有 **4 条通道**能覆盖客户指标（告警侧另算）。**通道不同，数据形态不同，
采集逻辑完全不同**——这是整个方案的第一层切分：

| 通道 | 指标数 | 接口 | 数据形态 | 采集方式 |
|---|---|---|---|---|
| **A 监控指标接口** | 11 | `POST /rds/v3/monitor/metrics` | **时序**（带平台时间戳） | 长窗口取回后按时间戳切片 |
| **B 命令中心 SQL** | **82** | `POST …/command-center/customer-commands/{cmd_id}` | **快照**（只有当前值） | 轮询攒时序 + 差分算速率 |
| **C 诊断优化接口** | 4 | `slow-sql-list` / `top-sql-list` | 快照（窗口内集合） | 轮询 |
| **D 实例接口** | 1 | `GET /rds/v3.2/instances` | 静态元数据 | 低频刷新 |
| **E 告警接口** | — | `POST /rds/v3/alarm/records` 等 | 事件 | 轮询 + 幂等去重 |
| **不可提供** | 3 | — | — | 丢弃并计数 |

| 通道 | 指标数 | 占比 | 客户类别覆盖 |
|---|---|---|---|
| A 监控指标接口 | 11 | 11% | 基本信息(6)、事务信息(2)、性能信息(2)、可用性(1) |
| B 命令中心 SQL | **82** | **81%** | 11 个类别全覆盖 |
| C 诊断优化接口 | 4 | 4% | 慢查询信息(3)、性能信息(1) |
| D 实例接口 | 1 | 1% | 基本信息(1) |
| E 不可提供 | 3 | 3% | 连接情况(2)、Agent(1) |

**这条结论决定了整个采集器的形态**：

1. 只有 A 类是时序，B/C/D 类全是快照 → **87/98 的指标历史只能由采集端自己攒**。
2. B 类占 81% → **必须合并 SQL**（50 条 → 10 条），否则一轮要 3~7 分钟，撑不住 60 秒档。
3. 累计计数类指标**必须自己差分**，且要处理实例重启归零。
4. 维度展开（一行一消息）是消息条数膨胀的主因（一条 SQL → 最多 85 条消息）。

### 2.4 已完成的可提供性验证

`tools/verify_metric_sources.py` 对 98 个可提供指标**逐个真实调用**并留档：

| 项 | 结果 |
|---|---|
| 总数 / 通过 / 失败 | **98 / 98 / 0**（`data/metric_verification/_summary.json`） |
| 实测时间 | 2026-11-05 |
| 实测实例 | `83cd68f2e4cb40b4b7e69240479ffa26in14`（gauss-6374，分布式版，master 节点 `gauss-6374_gaussdbv5_hcs2-0_0` 等 3 个） |
| **查询成功但无数据** | **6 个**：慢查询总数/Select慢查询数/DML慢查询数（无慢 SQL）、accessexclusive 锁（无该类锁）、正在运行的 SQL（无活跃会话）、Top10 SQL（无数据） |

这 6 个属于**环境性空数据**，不是接口不可用——采集端实现时必须区分"接口失败"与"查询成功但为空"，
两者在监控上不能混为一谈。

### 2.5 替代方案评估

在动手设计前，对三条备选路线做了评估：

| 方案 | 可行性 | 结论 |
|---|---|---|
| **A. 客户 Agent 直连 GaussDB**（走其余数据库的路径） | ✗ | 客户接入指引里的 Agent 路径覆盖的是达梦/Oracle/MySQL 等；GaussDB 由 TPOPS 托管，本项目的定位就是**从 TPOPS 取数再投递**（§1.1）。GaussDB 实例侧能否直连、能否部署 Agent **未在本项目验证范围内**。 |
| **B. Prometheus exporter 直连数据库** | ✗ | 同上，依赖数据库侧直连；且需长期维护额外组件，与"客户侧不引入新组件"的接入方式冲突。 |
| **C. 一次性脚本 + 客户侧定时拉取** | △ | 实现最简单，但**把"攒历史"的职责推给客户**，客户平台的采集口径我们不可控；且失败无重试、无水位。**不满足 FR-08/FR-09/FR-12**。 |
| **D. 常驻采集端 + Kafka**（本方案） | ✅ | 满足全部 FR/NFR；把历史攒在采集端，水位可控；失败可重试；进程可观测。 |

**选 D。**

---

## 3. 需求定稿（需求基线）

> 本章是**冻结的需求**。开发以此为准；客户后续提出的字段级调整，通过改映射表（§5.5）实现，
> 不改代码。

### 3.1 消息模型（JSON 信封）

#### 3.1.1 指标消息

```json
{
  "msg_type": "metric",
  "schema_version": "1.0",
  "instance_id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
  "instance_name": "gauss-6374",
  "metric_name": "GAUSSDB.[{#DATNAME}]_committed_transactions",
  "metric_name_cn": "[{#DATNAME}] Commited transactions",
  "metric_category": "数据库信息",
  "dimensions": { "DATNAME": "postgres" },
  "value": 12345,
  "value_type": "整数",
  "unit": "tps",
  "timestamp": 1793871051000,
  "collect_time": 1793871082000,
  "source": "command_center_sql"
}
```

| 字段 | 类型 | 必填 | 来源与语义 |
|---|---|---|---|
| `msg_type` | string | ✓ | 固定 `"metric"` |
| `schema_version` | string | ✓ | 固定 `"1.0"`，格式演进时递增 |
| `instance_id` | string | ✓ | TPOPS 实例 ID |
| `instance_name` | string | ✓ | TPOPS 实例名（如 `gauss-6374`） |
| `metric_name` | string | ✓ | **客户指标英文名，原样保留（含 OCR 错字）** |
| `metric_name_cn` | string | ✓ | 客户指标中文名 |
| `metric_category` | string | ✓ | 客户指标分类（13 类之一） |
| `dimensions` | object | ✓ | 维度键值；无维度时为 `{}` |
| `value` | number/string | ✓ | 按 `value_type` 序列化；**缺失时为 `null`，不填 0** |
| `value_type` | string | ✓ | 整数 / 浮点 / 字符 / 文本（**与客户表一致**） |
| `unit` | string/null | ✓ | 客户表「指标单位」；**缺省填 `null`，不填空串** |
| `timestamp` | number | ✓ | **数据时间**：A 类取平台时间戳；B/C/D 类取采集时刻 |
| `collect_time` | number | ✓ | 本条数据的采集完成时刻（毫秒） |
| `source` | string | ✓ | `monitor_metrics` / `command_center_sql` / `diagnostic` / `instance` |

#### 3.1.2 告警消息

```json
{
  "msg_type": "alarm",
  "schema_version": "1.0",
  "instance_id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
  "instance_name": "gauss-6374",
  "alarm_id": "2007010001",
  "alarm_name": "节点NTP状态异常",
  "alarm_type": "event",
  "alarm_category": "节点状态",
  "severity": "major",
  "status": "activated",
  "first_alarm_time": 1793758083000,
  "last_alarm_time": 1793771156000,
  "clear_alarm_time": null,
  "alarm_duration": "4小时5分钟",
  "alarm_items_sum": 1,
  "host_ip": "172.16.103.128",
  "node_id": "5a3cd027ad5c4f9bbb67d728c6d279c0no14",
  "description": "172.16.103.128节点NTP服务状态异常。",
  "impaction": "节点NTP状态异常会导致实例无法正确进行时间同步。",
  "possible_causes": "1、NTP客户端和服务端的网络连接有问题。…",
  "processing_steps": "1、登录告警对象节点；…",
  "alarm_detail_raise_count": 5,
  "notify_status": "disabled",
  "automatic_clear": true,
  "details": [
    { "node_id": "…", "host_ip": "…", "description": "…", "cause": "…" }
  ]
}
```

**告警消息的关键设计**：

- **收敛条目作主条目，明细行挂 `details` 数组**——一条消息对应"实例 + alarm_id"，
  `details` 覆盖该告警命中的各个节点。
- `details` 缺失时为空数组 `[]`，**不省略字段**（便于客户侧统一解析）。
- `alarm_duration` 保留平台原文（中文描述串），同时时间字段用毫秒时间戳供客户计算。

#### 3.1.3 统一约束

| 约束 | 规定 |
|---|---|
| 编码 | UTF-8，不转义非 ASCII（`ensure_ascii=False`） |
| 时间 | 一律 **13 位毫秒时间戳**（与 TPOPS 平台一致） |
| 缺失值 | 一律 `null`，**不填 0、不填空串**（0 与"取不到"是两件事） |
| 字段顺序 | 由序列化器固定（`sort_keys=False`，按定义顺序），保证可 diff |
| 数值 | 整数指标序列化为 `int`；浮点保留 2 位小数；比率类保留 2 位小数 |
| 空维度 | `dimensions` 传 `{}`，不传 `null` |
| 幂等键 | 指标消息：`instance_id + metric_name + dimensions + timestamp`；告警消息：§4.5.4 |

### 3.2 Topic 与分区策略（定稿）

| 项 | 决定 | 理由 |
|---|---|---|
| 指标 Topic | **`gaussdb.metrics`** | 语义独立，消费频率高（30 秒级） |
| 告警 Topic | **`gaussdb.alarms`** | 语义独立，量小但时效要求高，且消费方不同 |
| 指标与告警**是否同 Topic** | **否**——分开 | 语义、频率、消费方全不同；混流会让客户侧无法独立设保留策略与消费并发 |
| 分区键 | **`instance_id`** | 保证**同一实例的消息严格有序**；客户侧按实例做时序拼接才不会乱序 |
| 分区数 | 4（与实例数同量级，可配） | 实例数会增长，但同实例有序比跨实例均衡更重要 |
| 消息 Key | `instance_id`（指标）/ `alarm_id`（告警） | Kafka 分区键，保证同实例同告警落同一分区 |
| 压缩 | `lz4` | 消息以 JSON 为主，压缩比可观 |
| 投递语义 | `acks=all`，`enable_idempotence=False`（Kafka 幂等生产者需 3.6+ 客户端，kafka-python 2.0.2 不支持） | 重复由**采集端幂等键 + 客户侧去重**兜底 |

> Topic 名、分区数均**可通过环境变量覆盖**（§5.4），客户最终确认后改配置即可，不改代码。

### 3.3 采集周期分档（定稿）

客户指标库的周期共 7 档。采集端的调度档位与之对齐：

| 档位 | 客户指标数 | 涉及通道 | 该档执行的动作 |
|---|---|---|---|
| **30 秒** | 1 | A | A 类监控指标接口 1 次，切片出最后 30 秒的数据点 |
| **60 秒** | 3 | A + B | A 类 1 次 + 命令中心中覆盖 60 秒档指标的 SQL 子集 |
| **1 分钟** | 32 | B | 命令中心 SQL（覆盖 1 分钟档指标的集合） |
| **2 分钟** | 16 | B | 命令中心 SQL（覆盖 2 分钟档指标的集合） |
| **5 分钟** | 46 | B | 命令中心 SQL（覆盖 5 分钟档指标的集合） |
| **1 小时** | 1 | D | 实例接口 1 次 |
| **24 小时** | 2 | B | 命令中心 SQL（覆盖 24 小时档指标的集合） |

**核心调度原则：SQL 的调度频率由它所覆盖指标的最高需求档决定，而不是每个指标单独调度。**

这样做的收益是：一条合并 SQL 在一轮里取回的数据，**同时服务所有档位**——
例如 `stat_database` 这条 SQL 同时喂给 1 分钟档的 14 个指标和 5 分钟档的若干指标，
只需在 1 分钟档执行一次。

**每 tick（30 秒）执行的任务**：

| 任务组 | 频率 | 内容 |
|---|---|---|
| `monitor` | 每 tick | A 类监控指标接口 1 次 |
| `alarm` | 每 tick | 告警 4 次记录 + 1 次统计 |
| `command_center` | 按各 SQL 的到期档位 | 到期 SQL 并发下发 |
| `instance` | 每小时 | 实例接口 1 次 |

> **实测的周期达成性**：命令中心 10 条 SQL 并发实测一轮 8~15 秒（NFR-01 要求 ≤ 25 秒 @60 秒档）；
> A 类单次 1~3 秒；告警 5 次 2~5 秒。三组**并发**执行（HTTP I/O 密集，GIL 释放），
> 单 tick 墙钟 ≈ max(3s, 15s, 5s) ≈ **15 秒**，对 30 秒档有 2 倍余量。

### 3.4 取值口径（定稿）

拿到原始值之后，到客户字段之间只有 **5 类转换**，全部规则化、全部表驱动：

| 转换类型 | 输入 | 输出 | 出现场景 | 指标举例 |
|---|---|---|---|---|
| **直接取值** `raw` | 行的某一列 | 原值 | 占绝大多数（79 个直接对应） | `n_dead_tup` → `{#NAME} number of dead tuples` |
| **聚合** `aggregate` | 多行 | `sum`/`max`/`count`/`first` | 一个指标要跨行汇总 | `state` 分组计数 → 各状态连接数 |
| **比率** `ratio` | 两列 | `a/(a+b)`，分母 0 时返回 0 | 客户要的是百分比 | 死元组占比、缓存命中率 |
| **差分** `counter_delta` | 累计值 + 上次累计值 + 时间差 | `(now-last)/Δt` | 客户要速率，平台给累计 | `xact_commit` → 每秒提交事务数 |
| **派生** `gauge` | 时间戳 / 字段 | `now - 启动时间` | 客户要时长的派生量 | `rds133_cn_startup_time` → 运行时长 |
| **枚举映射** `enum_map` | 平台枚举 | 客户枚举 | 两边取值域不同 | 实例状态 `normal` → 客户枚举 |

**差分（`counter_delta`）的三条铁律**（对应 FR-07）：

1. **重启归零**：本次累计值 < 上次累计值 → 判定实例重启，**丢弃本次差分**（返回 `null` 并告警），
   下一轮重新建立基线。否则会算出巨大的负数或假峰值。
2. **首轮无基线**：第一轮只记基线、不产出速率值（`null`）。
3. **时间差用采集时刻差**，不是标称周期（命令中心单次 3~8 秒有抖动）。

**口径差异必须显式标注**：客户表中 10 个"近似对应" + 6 个"需换算"，
每条映射规则都带 `note` 字段说明口径，交付时随文档同步（避免客户拿到的值"看起来对但含义不同"）。

### 3.5 维度展开（定稿）

客户指标名含占位符时，**一条 SQL 返回 N 行 → 展开成 N 条消息**：

| 客户指标 | 占位符 | 维度键 | 展开来源 |
|---|---|---|---|
| `GAUSSDB.{#NAME}.Number.ad.ples` | `{#NAME}` | `NAME` | 行里的 `relname` |
| `gaussdb.locks.get_metrics[{#DBNAME}]` | `{#DBNAME}` | `DBNAME` | 行里的 `datname` |
| `GAUSSDB.[{#DATABASE_NAME}]_committed_transactions` | `{#DATABASE_NAME}` | `DATABASE_NAME` | 行里的 `datname` |
| `gaussdb_{#PORT}port.status` | `{#PORT}` | `PORT` | 行里的 `setting` |

**规则**：

- 占位符在 `metric_name` 中**原样保留**（客户侧用它做模板匹配），维度值另放在 `dimensions`。
- 无占位符的指标，`dimensions` 为 `{}`。
- 展开是**逐行**的；一个指标在不同维度上的值彼此独立，可部分缺值。

### 3.6 不可提供指标处置（定稿）

| 客户指标 | 中文名 | 类别 | 理由 |
|---|---|---|---|
| `gaussdb.Fastpath_function_call` | 快路径函数调用的连接数 | 连接情况 | PG 侧无 fastpath 计数器；`pg_settings` 仅有 `gsi_fastpath_level` 开关 |
| `gaussdb.Disabled_conn` | 禁用的连接数 | 连接情况 | PG 无「禁用连接」概念；仅有 `reserved_connections` 等预留连接 |
| `gaussdb.sendAgentName` | 发送设备名给Agent | Agent | Agent 侧行为，不是数据库可查询的监控指标 |

**处置方式**（FR-13）：

1. **不产出任何消息**。
2. 每轮日志输出丢弃计数，累计计数暴露给运维。
3. 交付时附书面清单 `data/customer_metric_unsupported.csv`。
4. **不静默吞掉**——客户问起来能立刻回答"哪几个、为什么"。

### 3.7 告警语义（定稿）

| 项 | 规定 |
|---|---|
| 采集范围 | 活动告警（收敛 + 明细）+ 历史告警（收敛 + 明细）+ 数量统计 |
| 主条目 | 收敛列表（实例 + alarm_id 为一条） |
| 明细挂载 | 明细行按 `alarm_id + instance_id` 归并进主条目的 `details` 数组 |
| 幂等键 | `(alarm_id, instance_id, node_id, first_alarm_time)` |
| 历史增量 | 按 `first_alarm_time` 水位只取新增/变化的记录，避免每轮全量 |
| 30 天窗口 | 每轮检查"最新历史告警时间距今 > 30 天"→ **显式告警日志**（FR-18） |
| 告警定义补全 | 用 `alarm_id` 关联 `data/alarm/catalog_*.json`，补全规则名、默认阈值、默认等级 |
| 状态变化 | 同一告警从 `activated` → `cleared`/`recovered` 时**作为新条目投递**（`last_alarm_time` 变化 → 幂等键变化） |

### 3.8 安全与脱敏（定稿）

| 对象 | 规则 |
|---|---|
| **凭据**（密码/token） | **只走环境变量**；不入日志、不入消息、不入水位文件、不入库 |
| **慢 SQL `sql_text`** | **默认脱敏**：保留 SQL 结构（表名/列名可保留），**字面量替换为 `?`**；并截断到 `SQL_TEXT_MAX_LENGTH`（默认 512 字符）。可通过 `SQL_MASKING_ENABLED=false` 关闭（**需客户书面确认后由客户自己开启**） |
| **会话 SQL 文本**（`pg_stat_activity.query`） | 同上策略处理 |
| **自签证书** | `verify_tls=False` 是平台事实；**凭据走 HTTPS 通道**，不落日志 |
| **错误日志** | 错误信息不包含密码/token；HTTP 响应体截断到 300 字符 |
| **锁号防护** | 认证失败**不重试**，只记录一次并退避（FR-10） |

### 3.9 仍需客户确认（**不阻塞开发启动**）

以下事项影响的是**交付形态**而非**开发**，全部通过配置或映射表吸收：

| # | 事项 | 我们的默认 | 吸收方式 |
|---|---|---|---|
| 1 | 客户 JSON 字段最终定义 | §3.1 的信封 | 改 `mapping/data/field_map.json`（**不改代码**） |
| 2 | Topic 名 | `gaussdb.metrics` / `gaussdb.alarms` | 环境变量 `KAFKA_TOPIC_*` |
| 3 | 分区数与保留策略 | 4 分区 | 环境变量 `KAFKA_TOPIC_PARTITIONS` |
| 4 | 指标/告警是否同 topic | 分开 | 需改代码（当前不支持合并），**已按分开设计** |
| 5 | 客户「采集更新周期」与平台 `collection_frequency` 的对齐口径 | **按客户周期调度**；平台周期仅用于解释数据点密度 | 映射表可加"降频/升频"标记 |
| 6 | 客户指标英文名的 OCR 错字是否更正 | **原样保留** | 映射表加 `alias` 字段，一行内两种写法同时输出 |
| 7 | 慢 SQL 文本能否外发 | **默认脱敏** | 环境变量 `SQL_MASKING_ENABLED` |
| 8 | 告警明细字段的最终取舍 | §3.1.2 全字段 | 映射表裁剪 |
| 9 | **A 类多 series 的归集口径**（一个客户指标对应多个组件 series，见 §4.2.4） | **暂按逐组件各出一条消息**（现状行为），维度键 `COMPONENT_ID` 归入 `tags` | 映射表 `value_rule` 增加聚合参数 + 改 `rules.py` 的 `series_point` 实现 |

> **结论：前 8 项均不阻塞编码。** 开发按默认值实现，映射表与配置留出调整位。
>
> **第 9 项需要单独说明**：它不改变消息信封与字段规范，但**直接影响 A 类 11 个指标的取值实现**
> ——客户指标是实例级一条，平台是每组件一条 series，合成方式（求和 / 取最早 / 按组件展开）
> 属业务口径，须客户确认。A 类通道的采集代码按「先按 series 产出、再按映射表聚合」的两段式写，
> 聚合环节默认关闭，确认后只改映射表即可启用。

---

## 4. 技术设计

### 4.1 总体架构

#### 4.1.1 分层与数据流

```
┌───────────────────────── 调度层 ─────────────────────────┐
│  Scheduler：due-time 调度表 · 分档到期 · 并发组编排         │
└───────────────┬──────────────────────────────────────────┘
                │ 触发 (tick 上下文)
   ┌────────────┼─────────────┬──────────────┐
   ▼            ▼             ▼              ▼
┌─────────┐ ┌─────────┐ ┌───────────┐ ┌──────────┐
│monitor  │ │command  │ │diagnostic │ │ instance │   ← 采集器（按通道分治）
│(A 类)   │ │(B 类)   │ │(C 类)     │ │ (D 类)   │
└────┬────┘ └────┬────┘ └─────┬─────┘ └────┬─────┘
     │           │            │            │
     └───────────┴─────┬──────┴────────────┘
                       ▼
              ┌──────────────────┐
              │ Normalizer       │  TPOPS 原始结构 → 内部统一 DTO
              │ （不可变 dataclass）│
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │ FieldMapper      │  映射表驱动 + 取值规则引擎
              │ （表驱动，不写死） │  + 差分基线 + 维度展开
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │ Serializer       │  唯一 JSON 序列化出口
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │ KafkaProducer    │  PLAINTEXT · key=instance_id
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │ WatermarkStore   │  仅在投递全部成功后推进
              └──────────────────┘
```

#### 4.1.2 三条不变式

| # | 不变式 | 违反后果 | 保障手段 |
|---|---|---|---|
| I-1 | 一次循环 = 拉取 → 转换 → 投递 → **成功后才推进水位** | 数据丢失 | `WatermarkStore` 只在 `produce()` 全部返回成功后调用 `advance()` |
| I-2 | **只有一个 JSON 序列化出口** | 格式漂移，客户端解析失败 | 任何模块不得自行 `json.dumps` |
| I-3 | **映射不写死在代码里** | 客户改字段要改代码，改不动 | 全部映射来自 `mapping/data/*.json` |

#### 4.1.3 依赖方向（单向，不允许反向）

```
main → scheduler → collectors → tpops → client
                    ↓
                 mapping → models
                    ↓
                 kafka
```

`models` 不依赖任何其他模块（纯 DTO 层）；`tpops` 不依赖 `mapping`（平台层不知道客户）；
`mapping` 不依赖 `kafka`。这条约束保证**单元测试可以完全 mock 掉 TPOPS 与 Kafka**。

### 4.2 API 详解

以下每个接口都给出：URL、方法、请求、响应、**实测约束**、错误处理策略。
标注「实测」的条目均可在 `data/` 下的留档中复核。

#### 4.2.1 认证 `POST /rds/v3/auth/mini-token`

**请求**

```http
POST /rds/v3/auth/mini-token
Content-Type: application/json

{"username": "<TPOPS_USER>", "password": "<TPOPS_PASSWORD>"}
```

**响应（HTTP 200）**

| 位置 | 字段 | 说明 |
|---|---|---|
| **响应头** | `X-Mini-Token` | **令牌在这里，body 里没有** |
| body | `user_id` / `login_ip` / `login_failed_attempts` | 登录审计 |
| body | `session_timeout` | 实测 **1440**（分钟） |
| body | `last_login_statistics` | 上次登录 IP / 时间 |

**约束**

| 约束 | 应对 |
|---|---|
| 失败会累计 `login_failed_attempts` 并**锁号** | **认证失败绝不重试**（FR-10）；只记录一次，退避到下一轮再试 |
| Token 有效期 1440 分钟 | 缓存 Token；到期前主动刷新；收到 401 强制刷新并重试**一次** |
| 平台用**自签证书** | `verify_tls=False`（配合 `urllib3.disable_warnings`） |

**错误处理**

| 情况 | 处理 |
|---|---|
| HTTP 401/403 | 抛 `AuthError`（**不可重试**），记录但**不打印密码** |
| HTTP 5xx / 连接超时 | 抛 `TransientError`（可重试），指数退避 |
| 200 但响应头无 `X-Mini-Token` | 抛 `ProtocolError`（不可重试），说明平台行为变了 |

#### 4.2.2 实例列表 `GET /rds/v3.2/instances`

**请求**

```http
GET /rds/v3.2/instances?limit=100&offset=0
X-Mini-Token: <token>
X-Language: zh-cn
```

**响应**

```json
{
  "total_count": 2,
  "instances": [
    {
      "id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
      "name": "gauss-6374",
      "mode": "Combined",
      "created_at": "2026-11-03T07:52:54+0000",
      "instance_status": "normal",
      "engine_name": "gaussdbv5",
      "engine_version": "V2.0-10.0.0",
      "agent_info": {
        "latest_version": "gaussdbv5_agent_26.861.0.1",
        "current_version": "gaussdbv5_agent_26.861.0.1",
        "last_version": null,
        "upgrade_status": "Completed"
      }
    }
  ]
}
```

**用途**：① 取 `instance_id`；② 取 `engine_version` → 客户指标 `gaussdb.server_version`（D 类，1 小时档）。

**约束**：`limit` 上限 100，超出需 `offset` 分页。当前实测 `total_count = 2`。

#### 4.2.3 节点清单 `GET /rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node`

**请求**

```http
GET /rds/gaussdb/v3/instances/83cd68f2e4cb40b4b7e69240479ffa26in14/key-view-execute-node
```

**响应**（实测样本，gauss-6374 分布式版共 **12 条**条目，此处节选）

```json
{
  "nodes": [
    { "node_id": "3881c04403394ee1b36a2bf873e69007no14", "node_name": "gauss-6374_gaussdbv5_hcs2-0_0",
      "role": "master", "type": "CN", "distributed_id": "", "component_id": "cn_5001",
      "node_ip": "172.16.103.126", "detail": "Normal" },
    { "node_id": "40abc563810e462a97b065e718189a59no14", "node_name": "gauss-6374_gaussdbv5_hcs2-0_1",
      "role": "master", "type": "CN", "distributed_id": "", "component_id": "cn_5002",
      "node_ip": "172.16.103.127", "detail": "Normal" },
    { "node_id": "5a3cd027ad5c4f9bbb67d728c6d279c0no14", "node_name": "gauss-6374_gaussdbv5_hcs2-0_2",
      "role": "master", "type": "CN", "distributed_id": "", "component_id": "cn_5003",
      "node_ip": "172.16.103.128", "detail": "Normal" },
    { "node_id": "3881c04403394ee1b36a2bf873e69007no14", "node_name": "gauss-6374_gaussdbv5_hcs2-0_0",
      "role": "slave",  "type": "DN", "distributed_id": "60011", "…": "…" }
  ]
}
```

> ⚠️ 同一个 `node_id` 会以 **CN** 和**各 distributed_id 的 DN** 重复出现（12 条条目只有 **3 个唯一 node_id**）。
> **下发命令前必须按 node_id 去重**，否则会重复下发。
> 完整留档：`data/command_center/results/02_nodes_83cd68f2e4cb40b4b7e69240479ffa26in14.json`

**用途**：命令中心下发前**必须**取 `role == "master"` 的 `node_id`。

**约束（2026-11-06 修正）**：
- master **不唯一**。分布式实例 `gauss-6374`（mode=Combined）的节点清单有 **12 条条目 / 3 个唯一 node_id**，其中 **6 条 `role=master`**（每个 node_id 同时以 CN 与 DN 各出现一次）；集中式实例 `gauss-75a2`（mode=Ha）才有恰好 1 个 master DN。
- **一次下发多个 master 成功**（原『多节点必失败』是 SHELL 命令的行为，被错误推广）。
- **各 master 返回的计数值不同**（实测 `postgres.xact_commit` = 7265656 / 7218577 / 6315580），选任一 master 只能得到**该节点视角**，不是整实例汇总。
→ 采集端**一次下发该实例的全部 master**，并按 §4.5.2 的**按节点分基线**规则处理计数类指标。

此接口结果**按实例缓存**（节点拓扑变化极少），但缓存 TTL 设为 1 小时以防拓扑变更。

#### 4.2.4 监控指标 `POST /rds/v3/monitor/metrics`

**请求**

```http
POST /rds/v3/monitor/metrics
X-Mini-Token: <token>
X-Language: zh-cn
Content-Type: application/json

{
  "start_time": 1793866451000,
  "end_time":   1793871351000,
  "is_latest_point_only": "false",
  "instances": [{"instance_id": "83cd68f2e4cb40b4b7e69240479ffa26in14"}],
  "metric_names": [
    "rds092_gaussv5_commit_counter",
    "rds133_cn_startup_time",
    "gaussdb_instance_status"
  ]
}
```

**响应**

```json
{
  "metric_infos": [
    {
      "instance_id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
      "time_interval": 60,
      "metrics": [
        {
          "name": "rds092_gaussv5_commit_counter",
          "metric_name": "用户提交事务数/秒",
          "collection_frequency": 60,
          "node_id": null,
          "tenant_id": null,
          "pdb_name": null,
          "timestamps": [1793869271000.0, 1793869331000.0, "…"],
          "series": {
            "cn_5001": [10.0, 9.0, "…"],
            "cn_5002": [11.0, 10.0, "…"],
            "cn_5003": [11.0, 11.0, "…"]
          }
        }
      ]
    }
  ]
}
```

> **结构来源**：`docs/api/API参考-监控大盘.md` 表5/表6 + 实测留档 `data/metric_response/`（98 份原生响应）。
>
> ⚠️ **不要按拍平的写法取值**。早期版本的本文把响应写成
> `metric_infos[].metric_name_cn / series / timestamps`，那是
> `tools/verify_metric_sources.py` 的**重塑输出**（脚本自己把平台的 `metric_name` 改名成
> `metric_name_cn`，见该脚本 `run_monitor()`）—— **平台并不返回 `metric_name_cn` 这个字段名**，
> 照拍平写法取值会 KeyError。真实层级是 `metric_infos[] → metrics[]`，
> `collection_frequency` 在 `metrics[]` 上，`time_interval` 在 `metric_infos[]` 上。

**`series` 的形态（实测）**

`series` 的 key 是**组件维度**，不是主机 IP：

| key 形态 | 例子 | 出现在 |
|---|---|---|
| CN 组件 ID | `cn_5001` `cn_5002` `cn_5003` | rds092 / rds093 / rds400 / rds402 / rds133 |
| DN 组件 ID | `dn_6001` `dn_6004` `dn_6007` | rds402（QPS） |
| `<IP>-<component_id>` | `172.16.103.126-dn_6005` | datanode_switch_status（9 个 DN） |
| 单值 | `value` | gaussdb_instance_status |

**一个平台指标对应多条 series**（每个组件一条）：

| 平台指标 | series 条数 | 组成 |
|---|---|---|
| `rds092_gaussv5_commit_counter` | 3 | 3 个 CN |
| `rds093_gaussv5_rollback_counter` | 3 | 3 个 CN |
| `rds400_gaussv5_total_counter` | 3 | 3 个 CN |
| `rds402_gaussv5_ddl_dml_dcl_count` | 6 | 3 CN + 3 DN |
| `rds133_cn_startup_time` | 3 | 3 个 CN |
| `gaussdb_instance_status` | 1 | 单值（key = `value`） |
| `datanode_switch_status` | 9 | 9 个 DN |

**关键约束（全部实测）**

| # | 约束 | 数值/表现 | 实现要求 |
|---|---|---|---|
| 1 | **窗口 ≥ 1 小时** | 5 分钟窗口覆盖 `collection_frequency=3600` 的指标数 = **0/21** | `METRIC_WINDOW_HOURS` 默认 **2**，**下限强制 1** |
| 2 | **入库滞后约 5 分钟** | 5 分钟窗口实测尾部 null 率 **77.5%** | `end_time = now - METRIC_END_LAG_MINUTES`（默认 5） |
| 3 | **`is_latest_point_only` 必须是字符串 `"false"`** | 用 `true` 时约 **74%** 指标 `timestamps=[0.0]`（值对、时间丢） | 硬编码 `"false"` 字符串，不传布尔 |
| 4 | 时间戳是 **13 位毫秒** | `timestamps` 为浮点数 | 转 `int` 使用 |
| 5 | **`metric_names` 是数组，支持批量** | 11 个 A 类指标**一次调用取完** | 按"该档到期的 A 类指标"分组批量取 |
| 6 | **中文名在 `metrics[].metric_name`** | 目录接口 `series` 不带中文名 | 从 `metric_infos[].metrics[].metric_name` 取（**不是** `metric_name_cn`） |
| 7 | `series` 按**组件**展开 | key 是组件 ID / `<IP>-<component_id>` / `value` | 需要按节点归集时用 `component_id` 对齐；主机 IP 在节点清单接口里 |

**数据切片规则（关键）**

平台返回整段窗口的时序，采集端需要**按各指标自己的档位切片**：

```
for 每个 metric_info:
    for 每个 m in metric_info.metrics:          # 一次调用可返回多个平台指标
        for 每个 (key, values) in m.series.items():   # 每个组件一条 series
            for i, ts in enumerate(m.timestamps):     # timestamps 与 values 等长同序
                if ts > now - 该指标的档位周期 - 端点容差:   # 只取本档需要的新点
                    value = values[i]
                    if value is None: 跳过（平台未入库）
                    产出 MetricPoint(timestamp=int(ts), value=value, series_key=key)
```

**⚠️ 未定义：多 series → 一个客户指标怎么合成**

客户这 11 个指标的适用资源类型都是 `GAUSSDB实例`（**实例级，一条消息**），
而平台按组件给出多条 series —— **两者的归集口径至今没有定义**（评审 B-01 未关闭）。

现状：`tools/mappings/rules.py` 的 `series_point` 规则**逐 series 产出一条消息**，
维度键 `COMPONENT_ID`；而 `tools/mappings/message.py` 又把 `COMPONENT_ID` 归进 `tags`
（不作为客户维度）—— 于是客户侧会收到**多条同名、同 resourceName、值不同的指标消息**。

候选口径（**待客户确认**，已登记为 `data/mapping/open_items.json` 的 `a-class-multi-series`）：

| 平台指标 | 客户指标语义 | 候选口径 |
|---|---|---|
| rds092 / rds093 / rds400 / rds402 | 实例级速率 | **求和**（各 CN / DN 分担流量） |
| rds133_cn_startup_time | 实例启动时刻 / 运行时长 | **取最早（min）** 的启动时刻 |
| gaussdb_instance_status | 实例状态 | 直接取（单 series） |
| datanode_switch_status | 实例主备状态 | **按 DN 展开**（资源名带组件）或归集为布尔 |

**确认前不要写死在代码里** —— 归集口径由 `data/mapping/metric_rules.json` 的 `value_rule`
承载，客户确认后改表 + 改规则实现即可。

> **为什么不能取"最后一个点"**：同一批次里不同指标的 `collection_frequency` 不同
> （10s/30s/60s/300s/3600s），最后一次采样时间也不同。按时间戳切片才能保证每个指标
> 都拿到**在它自己的档位内**的最新点。

**配套目录接口** `GET /rds/v3/monitor/metrics/series?name=<GROUP>`

| 项 | 说明 |
|---|---|
| 用途 | 取指标英文名 → 中文名 / 单位 / `alarm_id`；用于告警定义关联 |
| 分组码 | **12 个必须硬编码**（平台无枚举接口）：`CPUMEMORY` `CONNECTION` `SYNCSTAT` `PROCESSRESOURCE` `NETWORK` `TRANSACTION` `IOSTORAGE` `LOCK` `INSTATUS` `FORDR` `AGENTSTATUS` `COREMETRICS` |
| **坑** | **分组名写错返回 HTTP 200 + 空 series，不报错** → 实现里**必须**检测空分组并打 WARNING 日志 |
| 可选参数 | 带 `instance_id` 可解析每个指标的 `alarm_id`（不传则 `alarm_threshold` 为空） |

#### 4.2.5 统一命令中心（5 个接口）

##### (1) 命令目录 `GET /rds/gaussdb/v3/command-center/customer-commands`

```http
GET /rds/gaussdb/v3/command-center/customer-commands?offset=0&limit=100&sort_key=cmd_name&sort_dir=asc
```

响应 `total_count = 142`，`data[]` 每项含 `cmd_id`/`cmd_name`/`cmd_type`/`call_type`/
`category`/`source`/`action`/`cluster_mode`/`status` 等 19 个字段。

**用途**：定位 `general_single_sql_query` 的 `cmd_id`（本项目固定为
`73722acc-bff6-4d04-adc0-bc1e1fa1eb7d`），并**在启动时校验它仍然存在且 enabled**。

**坑（实测）**：`status=disabled` **过滤无效**（仍返回全部 142 条 enabled），
但传非法值 `BOGUS` 会 400 → **不能用该过滤条件判断命令可用性**，要逐条比对 `status` 字段。

##### (2) 命令详情 `GET /rds/gaussdb/v3/command-center/commands/{cmd_id}`

`general_single_sql_query` 的关键约束（实测）：

```json
{
  "cmd_id": "73722acc-bff6-4d04-adc0-bc1e1fa1eb7d",
  "cmd_name": "general_single_sql_query",
  "cmd_type": "SQL",
  "call_type": "sync",
  "category": "general_sql",
  "source": "default",
  "action": "query",
  "status": "enabled",
  "creator": "admin",
  "constraint_config": {
    "cluster_mode": "All",
    "limit_node_types": ["master"],
    "limit_component_types": ["cn", "dn"],
    "db_version": {"min_value": "3.110.0"}
  }
}
```

参数约束：`sql_text` 必填（pattern `^(select|SELECT|show|SHOW)(.){1,10000}$`，上限 10000 字符）、
`limit` 必填（1~1000）。

##### (3) 下发命令 `POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}`

**请求**

```http
POST /rds/gaussdb/v3/command-center/customer-commands/73722acc-bff6-4d04-adc0-bc1e1fa1eb7d
Content-Type: application/json

{
  "instance_id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
  "node_ids": ["d5284a09d06c4b2798c8c713259fe705no14"],
  "params": [
    {"param_name": "sql_text", "param_value": "select schemaname, relname, n_dead_tup from pg_stat_user_tables"},
    {"param_name": "limit",    "param_value": "1000"}
  ]
}
```

**响应**（**不含数据**）

```json
{
  "job_id": "7e5e1763-a56b-443b-b80a-ba3f21b6936a",
  "node_tasks": [
    {"task_id": "3216b95c-4e38-4b9a-bbb7-46badd99ed00",
     "node_id": "d5284a09d06c4b2798c8c713259fe705no14"}
  ]
}
```

**约束**

| 约束 | 应对 |
|---|---|
| **只能在 master 执行**（`limit_node_types:["master"]`） | 先查节点清单取 master |
| **可一次下发多个 master**（2026-11-06 实测成功，3 个 node_tasks 全部 succeed） | `node_ids` 放该实例的**全部 master**；注意各 master 返回值不同 |
| `cluster_mode` 必须是 **JSON 数组** | 传字符串（`"Ha"`）直接 400 |
| `sql_text` 只允许 `select`/`show` 开头 | 全部 SQL 以 `select` 开头 |
| `limit` 1~1000 | 宽表（77 张表）用 `limit=1000` |

##### (4) 轮询任务 `GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records`

```http
GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records?job_id={job_id}&offset=0&limit=100
```

**响应**

```json
{
  "total_count": 1,
  "statistics": { "total": 1, "succeed": 1, "failed": 0, "running": 0, "other": 0 },
  "job_status": "running",
  "tasks": [
    { "task_id": "…", "instance_id": "…", "node_id": "…", "node_name": "…",
      "agent_req_id": "ComOmCmd-…", "status": "succeed",
      "params": [], "is_redirect": false, "error_msg": null,
      "start_time": 1793921105094, "…": "…" }
  ]
}
```

> **关键（实测）**：上面这份响应里 `statistics.running = 0` 且任务 `status = "succeed"`，
> **但 `job_status` 仍然是 `"running"`**。
> → **结束判据必须是 `statistics.running == 0`，绝不能用 `job_status`。**

**轮询策略**：首次延迟 1 秒，之后按 0.5s / 1s / 2s / 3s 递增，总超时 **60 秒**。

##### (5) 取结果 `GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}/task-record/{task_id}/detail`

```http
GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}/task-record/{task_id}/detail
```

**响应**

```json
{
  "node_id": "d5284a09d06c4b2798c8c713259fe705no14",
  "node_name": "gauss-6374_gaussdbv5_hcs2-0_0",
  "status": "succeed",
  "is_redirect": false,
  "result": "[{\"v\":\"gaussdb (GaussDB Kernel 507.0.0 build d791c80a) compi…",
  "out_format": "json",
  "out_mode": "array",
  "out_type": "map",
  "download_file_id": null
}
```

**约束**

| 约束 | 应对 |
|---|---|
| **`result` 是 JSON 字符串** | **二次 `json.loads`** 才得到行列表 |
| **`sql_text` 被掩码为 `******`** | 不能靠任务记录回读 SQL；SQL 保存在本地配置 |
| **列名取自 SQL 的 `as` 别名** | 所有 SQL 显式写别名 |
| **`status=succeed` ≠ 命令真的成功** | SHELL 命令实测出现过 `result` 里是 `Failed to execute command on all nodes` → **必须校验 `result` 内容** |

##### (6) 三跳编排与并发

```python
# 单条 SQL 的完整链路
job   = submit(instance_id, master_node_id, sql_text, limit)   # 第 1 跳
wait  = poll_until_running_zero(job.job_id)                     # 第 2 跳
rows  = json.loads(wait.tasks[0].detail["result"])              # 第 3 跳
```

**并发模型**：命令中心是**异步任务模型**——第 1 跳立即返回 `job_id`，
因此**可以先并发下发 N 条 SQL，再统一轮询**。

| 方案 | 耗时估算 |
|---|---|
| 串行 10 条 | 10 × (下发 + 轮询 + 取结果) ≈ **50~75 秒** ❌ 撑不住 60 秒档 |
| **并发下发 9~10 条** | 下发 ≈ 1s + 轮询 ≈ 5s + 取结果 ≈ 1s ≈ **8~15 秒** ✅ |

**失败处理**：实测 50 条 SQL 里 **3 条偶发失败，重试即成功** →
**单条 SQL 独立重试**（3 次，指数退避），一条失败不影响同批其他 SQL。

#### 4.2.6 诊断优化接口（C 类）

##### 慢 SQL 集合 `POST /rds/gaussdb/v3/instances/{instance_id}/slow-sql-list`

```http
POST /rds/gaussdb/v3/instances/83cd68f2e4cb40b4b7e69240479ffa26in14/slow-sql-list

{
  "start_time": 1793784651000,
  "end_time":   1793871051000,
  "instance_id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
  "threshold": 1000,
  "threshold_unit": "ms",
  "node_ids": ["d5284a09d06c4b2798c8c713259fe705no14"]
}
```

**响应**：`slow_sql_infos[]`，每项含 `sql_id` / `sql_text` / `calls` / `db_name`。

**约束（实测）**

| 约束 | 应对 |
|---|---|
| **时间窗口上限 24 小时** | 超出（48h/7d/30d）返回 **400「参数错误」** → 窗口**钳在 24h 内** |
| `threshold` **必传**（ms 取 1~99000，s 取 1~99） | 固定 `threshold=1000, unit=ms` |
| 响应可能为空数组 | 「查询成功但无数据」≠ 失败（见 §2.4） |

##### Top SQL 列表 `POST /rds/gaussdb/v3/instances/{instance_id}/top-sql-list`

```http
POST /rds/gaussdb/v3/instances/{instance_id}/top-sql-list

{
  "instance_id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
  "node_ids": ["<cn1>", "<cn2>", "…"],
  "start_time": 1793784651000,
  "end_time":   1793871051000
}
```

**响应**：`top_sql_infos[]`，按 `total_elapse_time` 排序。

**约束**：`node_ids` **需传全部 CN 节点 id**（不是只传 master），且**必须按 node_id 去重** —— 节点清单里同一个 node_id 会以 CN 和各 distributed_id 的 DN 重复出现，不去重会重复下发，平台报 `DBS.06030136 节点数量超出限制`（实测：gauss-6374 去重前 6 个 id，去重后 3 个）。
本项目从节点清单里筛出所有 `type == "CN"` 的节点。

#### 4.2.7 告警接口（E 类）

##### 记录列表 `POST /rds/v3/alarm/records`

```http
POST /rds/v3/alarm/records
Content-Type: application/json

{"is_active": true, "is_convergence": true, "limit": 100, "offset": 0}
```

**参数语义**

| 参数 | 取值 | 含义 | 实测条数 |
|---|---|---|---|
| `is_active` | `true` | 活动告警 | — |
| | `false` | 历史告警（已清除/已恢复） | — |
| `is_convergence` | `true` | 收敛列表（每行 = 实例 + alarm_id） | 活动 2 / 历史 4 |
| | `false` | 明细行（每节点一行，含原因与处置步骤） | 活动 3 / 历史 32 |

**两个参数都必传**，否则行为不确定。

**约束**

| 约束 | 应对 |
|---|---|
| `limit` **1~100** | 超 100 条**必须分页**（`offset` 递增，直到返回条数 < limit） |
| **历史告警仅保留 30 天** | 每轮检查并**显式告警日志**（FR-18） |
| `alarm_id` / `instance_id` 过滤**仅在 `is_convergence=false` 时有效** | 见 `tools/tpops_client.py` 注释 |

**收敛列表字段**（实测）

```
severity, status, cause, engine, alarm_id, alarm_name, alarm_category,
instance_id, instance_name, alarm_items_sum, alarm_type,
first_alarm_time, last_alarm_time, alarm_duration, component_name
```

**明细行字段**（实测，多出这些）

```
id, description, paramFillList, alarm_remarks, resource_id, node_id, host_ip,
alarm_detail_raise_count, impaction, possible_causes, processing_steps,
notify_status, automatic_clear
```

##### 数量统计 `GET /rds/v3/alarm/records/statistic`

无参。响应含 `system_severity_statistics` 与 `instance_severity_statistics`，
各 4 项（`notice` / `minor` / `major` / `critical`）。

#### 4.2.8 API 调用矩阵（每轮实际发生什么）

| 通道 | 接口 | 方法 | 每 30s tick | 每 60s tick | 每 300s tick | 每 1h tick |
|---|---|---|---|---|---|---|
| 认证 | `/rds/v3/auth/mini-token` | POST | 仅在 Token 失效/401 时 | 同 | 同 | 同 |
| A 监控 | `/rds/v3/monitor/metrics` | POST | ✅ 1 | ✅ 1 | ✅ 1 | ✅ 1 |
| B 命令 | `customer-commands/{cmd_id}` | POST | — | ✅ 9 | ✅ 10 | ✅ 10 |
| B 命令 | `batch-exec-job/task-records` | GET | — | ✅ 9 | ✅ 10 | ✅ 10 |
| B 命令 | `…/task-record/{task_id}/detail` | GET | — | ✅ 9 | ✅ 10 | ✅ 10 |
| B 前置 | `key-view-execute-node` | GET | 缓存（1h TTL） | 缓存 | 缓存 | ✅ 1 刷新 |
| C 诊断 | `slow-sql-list` | POST | — | — | ✅ 1 | ✅ 1 |
| C 诊断 | `top-sql-list` | POST | — | — | ✅ 1 | ✅ 1 |
| D 实例 | `/rds/v3.2/instances` | GET | — | — | — | ✅ 1 |
| E 告警 | `/rds/v3/alarm/records` ×4 | POST | ✅ 4 | ✅ 4 | ✅ 4 | ✅ 4 |
| E 告警 | `/rds/v3/alarm/records/statistic` | GET | ✅ 1 | ✅ 1 | ✅ 1 | ✅ 1 |
| **合计（HTTP 调用数）** | | | **6** | **33** | **38** | **40** |

> 认证调用在 Token 有效期内**不发生**（服务端 `session_timeout = 1440` 分钟），上表未计入；
> 若发生一次则 +1。NFR-02 的上限取 **≤ 42**（含认证与节点清单刷新）。

> 说明：A 类每 tick 都调一次，是因为它一次调用就能拿到 11 个指标的长窗口时序，
> **按时间戳切片**后同时服务 30s/60s/1分/2分/5分各档（§4.6）。这样避免了按档位拆成多次调用。

#### 4.2.9 健康检查 `GET /rds/health-status`（**探针专用，不进采集链路**）

| 项 | 实测结论 |
|---|---|
| 认证 | **不需要**（不带 token 也能 200） |
| 响应 | **HTTP 200 + 空响应体**（`Content-Length: 0`） |
| 语义 | 仅"GaussDB open-api 服务是否存活"；**不含任何业务信息** |
| 前缀 | 必须带 `/rds`，否则 404 |

**设计决定：不进采集链路。** 它无法提供任何客户指标需要的数据（§2.2.8 已证伪）。
采集端只在**启动自检**时调用它一次，确认平台可达；结果只写日志，不产出消息。

因此 §4.2.8 的每轮调用矩阵**不包含**该接口。

### 4.3 指标 → 取数方法映射（可执行口径）

本节是**开发时逐条对照的表**。全量 101 指标见 §9.1。

#### 4.3.1 A 类：监控指标接口（11 个，时序型）

| 客户指标 | 中文名 | TPOPS 指标 | 匹配 | 取值方式 |
|---|---|---|---|---|
| `GAUSSDB.Number_of_ansactions_mmitted_r_cond` | 每秒提交事务数 | `rds092_gaussv5_commit_counter` | 直接对应 | `metrics[].series[组件ID][i]`，平台已是速率口径；**多 series 归集口径待确认，见 §4.2.4** |
| `GAUSSDB.Number_of_llback_ansactions_r_cond` | 每秒回滚事务数 | `rds093_gaussv5_rollback_counter` | 直接对应 | 同上 |
| `gaussdb.Tps` | TPS | `rds400_gaussv5_total_counter` | 直接对应 | 同上 |
| `gaussdb.QPS` | QPS | `rds402_gaussv5_ddl_dml_dcl_count` | 直接对应 | 同上 |
| `GAUSSDB.Service_availability_testing` | 服务可用性检测 | `gaussdb_instance_status` | 近似对应 | 状态值 → 客户枚举（`enum_map`） |
| `GAUSSDB.PG_Service_Status` | GAUSSDB服务状态 | `gaussdb_instance_status` | 近似对应 | 同上 |
| `gaussdb.Status` | 数据库状态 | `gaussdb_instance_status` | 近似对应 | 同上（**30 秒档的唯一指标**） |
| `GAUSSDB.Standby_status` | Standby状态 | `datanode_switch_status` | 近似对应 | 同上 |
| `GAUSSDB.Primary_status` | Primary状态 | `datanode_switch_status` | 近似对应 | 同上 |
| `gaussdb.Starttime` | 启动时间 | `rds133_cn_startup_time` | 近似对应 | 时间戳原值 |
| `gaussdb.Uptimestr` | 运行时长 | `rds133_cn_startup_time` | 需换算 | `now - 启动时间`（`gauge`） |

**注意**：多个客户指标**共用同一个 TPOPS 指标**（`gaussdb_instance_status` 被 5 个客户指标复用，
`datanode_switch_status` 被 2 个复用，`rds133_cn_startup_time` 被 2 个复用）。
→ 一次调用取回后，按映射表**扇出**成多条客户指标。

#### 4.3.2 B 类：命令中心 SQL（82 个，快照型）

按**合并 SQL** 组织，每条 SQL 一次下发取回多列，再按字段规则分发到各客户指标。

| SQL | 视图 | 覆盖指标数 | 覆盖指标（客户英文名 → 来源列 → 规则） |
|---|---|---|---|
| **SQL-1** | `pg_stat_database` | 26 | 见下方明细表 |
| **SQL-2** | `pg_stat_user_tables` | 22 | 见下方明细表 |
| **SQL-3** | `pg_stat_activity`（按库+状态聚合） | 12 | 见下方明细表 |
| **SQL-4** | `pg_stat_activity`（会话明细） | 3 | 见下方明细表 |
| **SQL-5** | `pg_locks` | 3 | 见下方明细表 |
| **SQL-6** | `pg_stat_bgwriter` | 8 | 见下方明细表 |
| **SQL-7** | `pg_settings` | 4 | 见下方明细表 |
| **SQL-8** | `pg_database` | 2 | 见下方明细表 |
| **SQL-9** | `pg_prepared_xacts` | 1 | 见下方明细表 |
| **SQL-10** | `pg_stat_database ⋈ pg_stat_user_tables` | 1 | 见下方明细表 |
| **合计** | | **82** | |

<details>
<summary><b>展开：82 个指标逐条取值规则</b>（点击展开）</summary>

| SQL | 客户指标（英文） | 中文名 | 来源列 | 规则 | 维度 | 口径说明 |
|---|---|---|---|---|---|---|
| SQL-1 | `GAUSSDB.Current_number_of_connections` | 当前连接数 | `numbackends` | `raw` | — |  |
| SQL-1 | `GAUSSDB.[{#DATNAME}]_cache_hit_ratio` | [{#DATNAME}]缓存命中率 | `blks_hit/(blks_hit+blks_read)` | `ratio` | NAME |  |
| SQL-1 | `GAUSSDB.[{#DATNAME}]_committed_transactions` | [{#DATNAME}] Commited transactions | `xact_commit` | `raw` | NAME | 按 datname 取累计值 |
| SQL-1 | `GAUSSDB.[{#DATNAME}]_delete_records_per_second` | [{#DATNAME}]每秒Delete记录数 | `tup_deleted` | `counter_delta` | NAME | 累计值，每秒速率需按周期差分 |
| SQL-1 | `GAUSSDB.[{#DATNAME}]_fetch_records_per_second` | [{#DATNAME}]每秒Fetch记录数 | `tup_fetched` | `counter_delta` | NAME | 累计值，每秒速率需按周期差分 |
| SQL-1 | `GAUSSDB.[{#DATNAME}]_insert_records_per_second` | [{#DATNAME}]每秒Insert记录数 | `tup_inserted` | `counter_delta` | NAME | 累计值，每秒速率需按周期差分 |
| SQL-1 | `GAUSSDB.[{#DATNAME}]_return_records_per_second` | [{#DATNAME}]每秒Return记录数 | `tup_returned` | `counter_delta` | NAME | 累计值，每秒速率需按周期差分 |
| SQL-1 | `GAUSSDB.[{#DATNAME}]_rolled_back_transactions` | [{#DATNAME}] Rolled back transactions | `xact_rollback` | `raw` | NAME | 按 datname 取累计值 |
| SQL-1 | `GAUSSDB.[{#DATNAME}]_update_records_per_second` | [{#DATNAME}]每秒Update记录数 | `tup_updated` | `counter_delta` | NAME | 累计值，每秒速率需按周期差分 |
| SQL-1 | `gaussdb.Blk_read_time` | 读块的时间（毫秒） | `blk_read_time` | `raw` | — | 累计读块耗时，口径与客户一致 |
| SQL-1 | `gaussdb.Blk_write_time` | 写块的时间（毫秒） | `blk_write_time` | `raw` | — | 累计写块耗时，口径与客户一致 |
| SQL-1 | `gaussdb.Blks_hit` | 从缓存命中读取的块数 | `blks_hit` | `raw` | — |  |
| SQL-1 | `gaussdb.Blks_read` | 从磁盘读取的块数 | `blks_read` | `raw` | — |  |
| SQL-1 | `gaussdb.Conflicts` | 数据库操作冲突次数 | `conflicts` | `raw` | — |  |
| SQL-1 | `gaussdb.Deadlocks` | 检测到的死锁次数 | `deadlocks` | `raw` | — | 平台另有 rds091_gaussv5_deadlocks |
| SQL-1 | `gaussdb.Numbackends` | 当前连接到数据库的后端进程数 | `numbackends` | `raw` | — |  |
| SQL-1 | `gaussdb.Temp_bytes` | 临时文件大小 | `temp_bytes` | `raw` | — |  |
| SQL-1 | `gaussdb.Temp_files` | 生成的临时文件数 | `temp_files` | `raw` | — |  |
| SQL-1 | `gaussdb.Total_conn` | 总连接数 | `numbackends` | `raw` | — |  |
| SQL-1 | `gaussdb.Tup_deleted` | 删除的行数 | `tup_deleted` | `raw` | — |  |
| SQL-1 | `gaussdb.Tup_inserted` | 插入的行数 | `tup_inserted` | `raw` | — |  |
| SQL-1 | `gaussdb.Tup_returned` | 查询返回的行数 | `tup_returned` | `raw` | — |  |
| SQL-1 | `gaussdb.Tup_updated` | 更新的行数 | `tup_updated` | `raw` | — |  |
| SQL-1 | `gaussdb.Xact_commit` | 提交的事务总数 | `xact_commit` | `raw` | — |  |
| SQL-1 | `gaussdb.Xact_rollback` | 回滚的事务总数 | `xact_rollback` | `raw` | — | 累计回滚事务数 |
| SQL-1 | `gaussdb.tup_fetched` | 通过索引获取的行数 | `tup_fetched` | `raw` | — |  |
| SQL-2 | `GAUSSDB.{#NAME}.Number.ad.ples` | ({#NAME}) number of dead tuples | `n_dead_tup` | `raw` | NAME |  |
| SQL-2 | `GAUSSDB.{#NAME}.Number.lete.ples` | ({#NAME}) number of delete tuples | `n_tup_del` | `raw` | NAME |  |
| SQL-2 | `GAUSSDB.{#NAME}.Number.t.date.ples` | ({#NAME}) number of hot update tuples | `n_tup_hot_upd` | `raw` | NAME |  |
| SQL-2 | `GAUSSDB.{#NAME}.garbage.Ratio` | ({#NAME})Garbage ratio | `n_dead_tup/(n_live_tup+n_dead_tup)` | `ratio` | NAME |  |
| SQL-2 | `GAUSSDB.{#NAME}.number.Of.dex.ples.ad` | ({#NAME})number of index tuples read | `idx_tup_fetch` | `raw` | NAME |  |
| SQL-2 | `GAUSSDB.{#NAME}.number.Of.index.scan` | ({#NAME})number of index scan | `idx_scan` | `raw` | NAME |  |
| SQL-2 | `GAUSSDB.{#NAME}.number.Of.insert.tuples` | ({#NAME})number of insert tuples | `n_tup_ins` | `raw` | NAME |  |
| SQL-2 | `GAUSSDB.{#NAME}.number.Of.live.tuples` | ({#NAME})number of live tuples | `n_live_tup` | `raw` | NAME |  |
| SQL-2 | `GAUSSDB.{#NAME}.number.Of.quential.ples.ad` | ({#NAME})number of sequencial tuples read | `seq_tup_read` | `raw` | NAME |  |
| SQL-2 | `GAUSSDB.{#NAME}.number.Of.sequential.scan` | ({#NAME})number of sequencial scan | `seq_scan` | `raw` | NAME |  |
| SQL-2 | `GAUSSDB.{#NAME}.number.Of.update.tuples` | ({#NAME})number of update tuples | `n_tup_upd` | `raw` | NAME |  |
| SQL-2 | `gaussdb.{#NAME}.Number.ad.ples` | ({#NAME})已删除但未回收的行数（死亡行） | `n_dead_tup` | `raw` | NAME |  |
| SQL-2 | `gaussdb.{#NAME}.Number.lete.ples` | ({#NAME})被删除的行总数（包括已回收行） | `n_tup_del` | `raw` | NAME | 累计删除行数 |
| SQL-2 | `gaussdb.{#NAME}.Number.t.date.ples` | ({#NAME})通过 HOT更新的行数 | `n_tup_hot_upd` | `raw` | NAME |  |
| SQL-2 | `gaussdb.{#NAME}.garbage.Ratio` | ({#NAME})死亡行占比 | `n_dead_tup/(n_live_tup+n_dead_tup)` | `ratio` | NAME |  |
| SQL-2 | `gaussdb.{#NAME}.number.Of.dex.ples.ad` | ({#NAME})通过索引扫描获取的行数 | `idx_tup_fetch` | `raw` | NAME |  |
| SQL-2 | `gaussdb.{#NAME}.number.Of.index.scan` | ({#NAME})索引扫描次数 | `idx_scan` | `raw` | NAME |  |
| SQL-2 | `gaussdb.{#NAME}.number.Of.insert.tuples` | ({#NAME})插入的行数 | `n_tup_ins` | `raw` | NAME |  |
| SQL-2 | `gaussdb.{#NAME}.number.Of.live.tuples` | ({#NAME})当前活跃的行数 | `n_live_tup` | `raw` | NAME |  |
| SQL-2 | `gaussdb.{#NAME}.number.Of.quential.ples.ad` | ({#NAME})顺序扫描读取的行数 | `seq_tup_read` | `raw` | NAME |  |
| SQL-2 | `gaussdb.{#NAME}.number.Of.sequential.scan` | ({#NAME})顺序扫描次数 | `seq_scan` | `raw` | NAME |  |
| SQL-2 | `gaussdb.{#NAME}.number.Of.update.tuples` | ({#NAME})更新的行总数 | `n_tup_upd` | `raw` | NAME |  |
| SQL-3 | `GAUSSDB.Idle_connections` | Idle connections | `state` | `aggregate` | — | state='idle' |
| SQL-3 | `GAUSSDB.Idle_in_transaction_connections` | Idle in transaction connections | `state` | `aggregate` | — | state='idle in transaction' |
| SQL-3 | `GAUSSDB.Number_of_active_connections` | 活跃连接数 | `state` | `aggregate` | — | state='active' |
| SQL-3 | `GAUSSDB.[{#DATNAME}]_Active_connections` | [{#DATNAME}] Active connections | `—` | `raw_rows` | NAME | 按 datname 统计活跃会话 |
| SQL-3 | `GAUSSDB.session_usage` | 连接数使用率 | `pg_stat_activity.state` | `approx` | — | 在线会话数 / 总连接数 |
| SQL-3 | `gaussdb.Active_conn` | 活动连接数 | `state` | `aggregate` | — | state='active'；平台另有 rds117 |
| SQL-3 | `gaussdb.Idle_conn` | 空闲连接数 | `state` | `aggregate` | — | state='idle' |
| SQL-3 | `gaussdb.Idle_in_transaction` | 事务中空闲的连接数 | `state` | `aggregate` | — | state='idle in transaction' |
| SQL-3 | `gaussdb.Idle_in_transaction_aborted` | 事务中已中止的空闲连接数 | `state` | `aggregate` | — | state='idle in transaction (aborted)' |
| SQL-3 | `gaussdb.Lockwaiting` | 查看当前锁等待数量 | `state` | `aggregate` | — | state 含 lock 相关会话 |
| SQL-3 | `gaussdb.Total_pct` | 连接使用率 | `pg_stat_activity.state` | `approx` | — | 活跃会话数 / 总连接数 |
| SQL-3 | `gaussdb.Waiting_conn` | 等待资源的连接数 | `state` | `aggregate` | — | state 含 wait 类会话 |
| SQL-4 | `GAUSSDB.sessions_active_top10` | 会话时间TOP10 | `query_start` | `top_n` | — | 按 query_start 排序取 Top N |
| SQL-4 | `gaussdb.get.connections.sum` | 获取连接信息 | `—` | `raw_rows` | — | 会话清单（27 个字段） |
| SQL-4 | `gaussdb.running.sql.limit.20` | 正在运行的SQL | `query` | `top_n` | — | 会话当前执行的 SQL 文本 |
| SQL-5 | `gaussdb.get.lock.info` | 获取数据库锁数据 | `—` | `raw_rows` | — | pg_locks 全量锁视图 |
| SQL-5 | `gaussdb.locks.accessexclusive[{#DBNAME}]` | DB [{#DBNAME}]: Num of accessexclusive locks | `mode` | `aggregate` | NAME | mode='AccessExclusiveLock' 按库聚合 |
| SQL-5 | `gaussdb.locks.get_metrics[{#DBNAME}]` | DB [{#DBNAME}]: 获取锁信息 | `pg_locks` | `approx` | NAME | 按 datname 聚合，平台无按 DBNAME 的锁指标 |
| SQL-6 | `GAUSSDB.Buffers_Alloc` | Buffers Alloc | `buffers_alloc` | `raw` | — |  |
| SQL-6 | `GAUSSDB.Buffers_Backend` | Buffers Backend | `buffers_backend` | `raw` | — |  |
| SQL-6 | `GAUSSDB.Buffers_Backend_Fsync` | Buffers Backend Fsync | `buffers_backend_fsync` | `raw` | — |  |
| SQL-6 | `GAUSSDB.Buffers_Checkpoint` | Buffers Checkpoint | `buffers_checkpoint` | `raw` | — |  |
| SQL-6 | `GAUSSDB.Buffers_Clean` | Buffers Clean | `buffers_clean` | `raw` | — |  |
| SQL-6 | `GAUSSDB.Checkpoint_count_eckpoint_gments` | Checkpoint count (by checkpoint segments) | `checkpoints_req` | `raw` | — | 按需触发（requested）的 checkpoint 次数 |
| SQL-6 | `GAUSSDB.Checkpoint_count_eckpoint_meout` | Checkpoint count (by checkpoint timeout) | `checkpoints_timed` | `raw` | — | 按超时触发的 checkpoint 次数 |
| SQL-6 | `GAUSSDB.Max_Written_Clean` | Max Written Clean | `maxwritten_clean` | `raw` | — |  |
| SQL-7 | `GAUSSDB.maximum_connection` | 最大连接数 | `setting` | `raw` | — | 实测 200 |
| SQL-7 | `gaussdb.Pagesize` | 页大小 | `setting` | `raw` | — | 实测 8192 |
| SQL-7 | `gaussdb.Timezone` | 时区 | `setting` | `raw` | — |  |
| SQL-7 | `gaussdb_{#PORT}port.status` | 服务端口{#PORT}检测 | `pg_settings.port` | `approx` | PORT | 实测 2026-11-06：`GET /rds/health-status` 返回 200 但**响应体为空**（Content-Length: 0），只反映 open-api 服务存活、不含端口/实例信息，**不能用于可用性探测**；本指标仅能取到端口号，可用性需另找来源 |
| SQL-8 | `gaussdb.DBtsize[{#DATABASE_NAME}]` | [{#DATABASE_NAME}]库大小 | `size_bytes` | `raw` | NAME | 平台另有 rds128 聚合值 |
| SQL-8 | `gaussdb.Serverencoding` | 字符集 | `encoding` | `raw` | — | pg_encoding_to_char() |
| SQL-9 | `gaussdb.Prepared` | 预提交事务数 | `prepared_xacts` | `raw` | — | count(*) 统计预备事务 |
| SQL-10 | `gaussdb.get.dbstat.sum` | 获取db性能数据 | `—` | `raw_rows` | — | 库级 + 表级统计视图 |

<!-- COUNTS: {'SQL-1': 26, 'SQL-2': 22, 'SQL-3': 12, 'SQL-4': 3, 'SQL-5': 3, 'SQL-6': 8, 'SQL-7': 4, 'SQL-8': 2, 'SQL-9': 1, 'SQL-10': 1} TOTAL 82 -->

</details>

#### 4.3.3 C 类：诊断优化接口（4 个）

| 客户指标 | 中文名 | 接口 | 取值方式 | 口径说明 |
|---|---|---|---|---|
| `GAUSSDB.Total_slow_queries` | 慢查询总数 | `slow-sql-list` | `len(slow_sql_infos)` | 集合条数 |
| `GAUSSDB.Select_slow_queries` | Select慢查询数 | `slow-sql-list` | 按 `sql_text` 前缀统计（`select`/`with`） | 文本判定 |
| `GAUSSDB.DML_slow_queries` | DML慢查询数 | `slow-sql-list` | 按 `sql_text` 前缀统计（`insert`/`update`/`delete`） | 文本判定 |
| `gaussdb.Top10sql` | 当前运行时间最长的Top10 SQL | `top-sql-list` | `top_sql_infos` 按 `total_elapse_time` 排序取前 10 | **含 SQL 文本 → 脱敏** |

#### 4.3.4 D 类：实例接口（1 个）

| 客户指标 | 中文名 | 取值方式 | 刷新周期 |
|---|---|---|---|
| `gaussdb.server_version` | 版本 | `instances[].engine_version`（如 `V2.0-10.0.0`） | 1 小时 |

#### 4.3.5 E 类：告警（无客户指标编号）

告警不走"指标"体系，走独立的 §3.1.2 消息模型。客户指标库中**没有告警指标条目**，
告警字段规范由 §3.7 定稿。

### 4.4 合并 SQL 目录（11 条 —— 已全部实测通过）
**合并规则**：① 同视图 + 同粒度 → 一条宽 SQL；② 派生列在 SQL 里用 `CASE WHEN` 算完；
③ 列全部显式写出（平台的列名取自 `as` 别名）；④ ORDER BY 只在必要时加，且**方向必须与指标语义一致**；
⑤ 一条 SQL 的刷新周期 = 它覆盖指标中最高需求档；⑥ 能由已有行推导的派生指标不单开 SQL。

> **这 11 条已于 2026-11-07 在两个实例上逐条真实执行通过**
> （留档 `data/merged_sql/probe_result.json`，脚本 `tools/probe_merged_sql.py`）：
> Combined 实例 11/11 成功（70.7 s，含 1 次重试）、HA 实例 11/11 成功（55.0 s）。
> 正式定义（含每条的**实测列集合**）见 `data/merged_sql/merged_sql.json`。

| # | sql_key | 来源视图 | 覆盖指标 | 刷新档 | 实测行数/节点 | 实测列数 |
|---|---|---|---|---|---|---|
| 1 | `stat_database` | `pg_stat_database` | **26** | 1 分 | 5/5/5 | 18 |
| 2 | `stat_user_tables` | `pg_stat_user_tables` | **22** | 2 分 | 68/68/68 | 13 |
| 3 | `activity_agg` | `pg_stat_activity` | **13** | 1 分 | 2/2/2 | 3 |
| 4 | `activity_running` | `pg_stat_activity` | **1** | 1 分 | 4/4/4 | 5 |
| 5 | `activity_top10` | `pg_stat_activity` | **1** | 5 分 | 7/6/6 | 4 |
| 6 | `locks` | `pg_locks + pg_database` | **3** | 1 分 | 1/1/1 | 4 |
| 7 | `bgwriter` | `pg_stat_bgwriter` | **8** | 5 分 | 1/1/1 | 8 |
| 8 | `settings` | `pg_settings` | **4** | 1 分 | 4/4/4 | 3 |
| 9 | `database` | `pg_database` | **2** | 2 分 | 5/5/5 | 3 |
| 10 | `prepared_xacts` | `pg_prepared_xacts` | **1** | 1 分 | 1/1/1 | 1 |
| 11 | `dbstat_sum` | `pg_stat_database ⋈ pg_stat_user_tables` | **1** | 1 分 | 72/72/72 | 15 |
| | | | **82** | | | |

**分档后每轮实际下发的条数**：

| 档位 | 条数 | SQL |
|---|---|---|
| **60 秒** | **7** | `stat_database` `activity_agg` `activity_running` `locks` `settings` `prepared_xacts` `dbstat_sum` |
| 120 秒 | +2 | `stat_user_tables` `database` |
| 300 秒 | +2 | `activity_top10` `bgwriter` |

> **为什么是 11 条而不是 10 条**：`gaussdb.running.sql.limit.20`（要 `query_start` **降序**）
> 与 `GAUSSDB.sessions_active_top10`（要**升序** —— 客户要的是"会话时间最长"= 最早开始）
> 同源同粒度但**排序方向相反**，一条 SQL 只能有一个 ORDER BY；
> 两条都"取全量再客户端排"的话，`ASC LIMIT 1000` 会截掉最新的那批、`DESC` 会截掉最早的那批。
> → 拆成两条，各自与实测留档逐字一致，**客户端零推导**。

### 4.4.1 SQL 文本

```sql
-- SQL-1  stat_database      26 指标 · 1 分 档
select datname, numbackends, xact_commit, xact_rollback, deadlocks, conflicts, blks_read, blks_hit, blk_read_time, blk_write_time, tup_inserted, tup_updated, tup_deleted, tup_returned, tup_fetched, temp_files, temp_bytes, case when (blks_hit + blks_read) > 0 then round(blks_hit * 100.0 / (blks_hit + blks_read), 2) else 0 end as cache_hit_ratio from pg_stat_database order by datname
```

```sql
-- SQL-2  stat_user_tables   22 指标 · 2 分 档
select schemaname, relname, n_live_tup, n_dead_tup, n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, case when (n_live_tup + n_dead_tup) > 0 then round(n_dead_tup * 100.0 / (n_live_tup + n_dead_tup), 2) else 0 end as dead_tup_ratio from pg_stat_user_tables order by dead_tup_ratio desc
```

```sql
-- SQL-3  activity_agg       13 指标 · 1 分 档
select datname, state, count(*) as cnt from pg_stat_activity where state is not null group by datname, state order by datname, state
```

```sql
-- SQL-4  activity_running    1 指标 · 1 分 档
select datname, usename, state, query_start, query from pg_stat_activity where state = 'active' and query <> '' order by query_start desc limit 20
```

```sql
-- SQL-5  activity_top10      1 指标 · 5 分 档
select datname, usename, query_start, query from pg_stat_activity where state = 'active' order by query_start limit 10
```

```sql
-- SQL-6  locks               3 指标 · 1 分 档
select d.datname, l.locktype, l.mode, count(*) as cnt from pg_locks l join pg_database d on l.database = d.oid group by d.datname, l.locktype, l.mode order by d.datname, l.locktype, l.mode
```

```sql
-- SQL-7  bgwriter            8 指标 · 5 分 档
select buffers_alloc, buffers_backend, buffers_backend_fsync, buffers_checkpoint, buffers_clean, checkpoints_req, checkpoints_timed, maxwritten_clean from pg_stat_bgwriter
```

```sql
-- SQL-8  settings            4 指标 · 1 分 档
select name, setting, unit from pg_settings where name in ('max_connections', 'port', 'TimeZone', 'block_size')
```

```sql
-- SQL-9  database            2 指标 · 2 分 档
select datname, pg_encoding_to_char(encoding) as encoding, pg_database_size(datname) as size_bytes from pg_database order by size_bytes desc
```

```sql
-- SQL-10 prepared_xacts      1 指标 · 1 分 档
select count(*) as prepared_xacts from pg_prepared_xacts
```

```sql
-- SQL-11 dbstat_sum          1 指标 · 1 分 档
select d.datname, d.numbackends, d.xact_commit, d.xact_rollback, d.blks_read, d.blks_hit, t.schemaname, t.relname, t.n_live_tup, t.n_dead_tup, t.seq_scan, t.idx_scan, t.n_tup_ins, t.n_tup_upd, t.n_tup_del from pg_stat_database d left join pg_stat_user_tables t on d.datname = current_database() order by t.n_dead_tup desc limit 100
```

### 4.4.2 SQL 编写红线（全部实测踩过的）

| # | 红线 | 原因 |
|---|---|---|
| 1 | 只能 `select` / `show` 开头 | pattern 限制，否则 400 |
| 2 | **派生列必须写 `as` 别名** | 平台的列名取自别名（原生列名不受控） |
| 3 | 不用 `FILTER`，改 `CASE WHEN` | PG 9.2 无 `FILTER` |
| 4 | 排除后台进程用 `state is not null` | `state <> ''` 恒为假 |
| 5 | `limit` 必填，1~1000 | 宽表用 1000 |
| 6 | `sql_text` ≤ 10000 字符 | pattern 限制 |
| 7 | 分母为 0 的比率用 `case when` 兜底为 0 | 防除零错误导致整条 SQL 失败 |

### 4.4.3 并发实测结论（2026-11-07，取代此前"8~15 秒"的估算）

| 并发作业数 | 失败数 | 墙钟(s) |
|---|---|---|
| 1 | 0 | 4.5 |
| 2 | 0 | 5.4 |
| 4 | **2** | 5.7 |
| 6 | **4** | 5.8 |
| 11 | **7** | 8.0 |

- **并发 ≥ 4 会随机失败**，错误信息是 `invalid SQL statement with syntax error` ——
  但 **SQL 本身没问题**（同一条串行重跑即过；金丝雀测试 8/8 返回的列与提交的 SQL 完全对应，
  无参数串味）。这是**平台侧的参数竞争**，错误信息是误导性的 → **必须重试**。
- **并发不省时间**：并发 1 是 4.5 s，并发 11 是 8.0 s（还带 7 个失败）。
- → **`CMD_CONCURRENCY` 取 2**（不是 9），配合逐条重试。

### 4.4.4 多 master：一次下发全部 master

分布式实例 `gauss-6374`（mode=Combined，**3 个 CN 全是 master** + 9 个 DN）的节点清单有
**12 条条目 / 3 个唯一 node_id**（每个 node_id 同时以 CN 与 DN 各出现一次），其中 **6 条 `role=master`**。

实测：**一次下发 3 个 master 成功**（HTTP 200，3 条 `node_tasks` 全部 `succeed`，
一次轮询拿回 3 组行）。早期方案里"一次下发多节点会失败"是**错的** —— 那条结论来自
`data/command_center/results/07_task_result_matrix_u_shell_sync.json`，是 **SHELL 命令**的
`Failed to execute command on all nodes.`，被错误推广到了 SQL 命令。

**各 master 返回的计数值不同**（实测 `postgres.xact_commit` = 7265656 / 7218577 / 6315580），
所以采集端按 **(sql_key, master, 行)** 三层组织结果，计数类指标的差分基线**按 master 分开**。

---

### 4.5 规则引擎

合并后 SQL 只解决"取到数据"，**"怎么变成客户的值"** 由规则引擎负责。

#### 4.5.1 规则类型

| 类型 | 输入 | 输出 | 实现要点 |
|---|---|---|---|
| `raw` | 行的某一列 | 原值 | 直接对应（占 79 个直接对应指标的大多数） |
| `aggregate` | 多行 | `sum`/`max`/`count`/`first` | 条件聚合（按 `state`/`mode` 过滤后求和） |
| `ratio` | 两列 | `a/(a+b)`，分母 0 → 0 | 已在 SQL 里算完的列直接 `raw` 取 |
| `counter_delta` | 累计值 + 上次累计值 + 时间差 | `(now-last)/Δt` | 见 §3.4 三条铁律 |
| `gauge` | 时间戳 | `now - 启动时间` | 单位换算到客户单位 |
| `enum_map` | 平台枚举 | 客户枚举 | 映射表驱动，未命中时**原样透传并告警** |
| `top_n` | 多行 + 排序列 | 前 N 行 | N 与排序列在映射表里配置 |

#### 4.5.2 差分（`counter_delta`）实现要点

```
baseline[(instance_id, metric_name, dimensions_key)] = (counter_value, collected_at)

本轮拿到 counter_value 与 now：
  1. 无基线            → 记基线，本轮 value = null（首轮无速率）
  2. counter_value < 基线值 → 判定重启：丢弃本轮差分（value = null）+ ERROR 日志
                              + 重建基线
  3. dt = now - 基线时刻（秒）
     rate = (counter_value - 基线值) / dt
     更新基线
```

**受影响指标**：6 个「需换算」指标 —— 每秒 Fetch / Insert / Return / Update / Delete 记录数
（`GAUSSDB.[{#DATNAME}]_*_records_per_second`）。

> 注：A 类的 `rds092_gaussv5_commit_counter` 等**平台本身已是速率口径**，不需要差分。
> 累计值的 `xact_commit` 对应的客户指标 `gaussdb.Xact_commit`（"提交的事务总数"）是**累计量**，
> 用 `raw` 取值，不差分。**两者不能混淆**——映射表里 `counter_delta` 与 `raw` 是显式区分的。

#### 4.5.3 维度展开

```
for row in sql_rows:
    dims = {}
    for placeholder, column in mapping.dimension_bindings.items():
        dims[placeholder] = row[column]        # 如 {"NAME": row["relname"]}
    for metric in mapping.metrics_of_this_sql:
        value = rule_engine.apply(metric.rule, row, baseline)
        if value is None: continue              # 缺值不产出假数据
        yield MetricPoint(customer_metric=metric.name, dimensions=dims, value=value, ...)
```

#### 4.5.4 幂等键

| 消息类型 | 幂等键 |
|---|---|
| 指标 | `instance_id + metric_name + 排序后的 dimensions + timestamp` |
| 告警 | `alarm_id + instance_id + node_id + first_alarm_time` |

采集端在内存里维护一个 **LRU 幂等键集合**（容量可配，默认 10000 条），
防止同一轮内的重复投递与相邻轮的重投。

### 4.6 调度器

#### 4.6.1 调度模型

**到期驱动（due-time）**，而不是"每轮全跑"：

```python
@dataclass(frozen=True)
class ScheduleEntry:
    task: str                 # "monitor" | "command_center" | "alarm" | "instance"
    interval_seconds: int     # 该任务的到期周期
    next_due_at: float        # 下次到期的时间戳（monotonic）
```

**每个 tick（默认 30 秒）**：

```
1. 扫描 due-time 表 → 找出本 tick 到期的任务
2. 到期任务并发执行（线程池，HTTP I/O 密集，GIL 释放）
3. 等待全部完成 → 汇总本轮统计 → 推进水位
4. sleep 到下一个 tick 边界
```

**为什么并发**：单 tick 内三组任务串行 = 3s + 15s + 5s = 23s，逼近 30 秒边界；
并发后 ≈ max(3s, 15s, 5s) = 15s，**留 2 倍余量**（NFR-01）。

#### 4.6.2 任务与周期

| 任务 | 周期 | 覆盖的指标档位 |
|---|---|---|
| `monitor` | 30 秒 | 30 秒 / 60 秒 / 1 分 / 2 分 / 5 分 的 A 类指标（长窗口 + 时间戳切片） |
| `alarm` | 30 秒 | 全部告警 |
| `command_center` | 60 秒（SQL-2 为 300 秒） | 60 秒 / 1 分 / 2 分 / 5 分 的 B 类指标 |
| `diagnostic` | 300 秒 | 5 分钟档的 C 类指标 |
| `instance` | 3600 秒 | 1 小时档的 D 类指标 |

#### 4.6.3 SQL 的到期判定

**规则：一条合并 SQL 的刷新周期 = 它覆盖的指标中最高需求档对应的周期。**

```
SQL-1 stat_database      覆盖最快 1 分档  → 60 秒
SQL-2 stat_user_tables   覆盖最快 5 分档  → 300 秒
SQL-3 activity_agg       覆盖最快 60 秒档 → 60 秒
SQL-4 activity_detail    覆盖最快 1 分档  → 60 秒
SQL-5 locks              覆盖最快 1 分档  → 60 秒
SQL-6 bgwriter           覆盖最快 1 分档  → 60 秒
SQL-7 settings           覆盖最快 1 分档  → 60 秒
SQL-8 database           覆盖最快 1 分档  → 60 秒
SQL-9 prepared_xacts     覆盖最快 1 分档  → 60 秒
SQL-10 dbstat_sum        覆盖最快 1 分档  → 60 秒
```

**取值的"新鲜度"检查**：指标在自己的档位到期时，从**最近一次 SQL 缓存结果**取值；
若缓存结果的采集时刻已超过该指标的档位周期，则**本轮触发该 SQL 提前刷新**。

#### 4.6.4 水位

| 水位 | 内容 | 用途 |
|---|---|---|
| 指标水位 | 每个 `(metric_name, dimensions_key)` 的**最后成功投递时刻** | 判断是否需要重发；重启续传 |
| 差分基线 | 每个 `(instance_id, metric_name, dimensions_key)` 的 `(累计值, 采集时刻)` | `counter_delta` 计算 |
| 告警水位 | 最后处理到的 `first_alarm_time` | 历史告警增量拉取 |
| 幂等键集合 | 最近 N 条消息的幂等键 | 去重 |

**持久化**：`mapping`/`state` 目录下的 JSON 文件，**原子写**（写临时文件 + `os.replace`），
避免进程被杀时写坏文件。

**推进时机**：**仅当该批消息全部成功投递后才推进**（I-1）。
部分成功时，只推进到"已成功投递的最大水位"——但为简单可靠，采用**批次原子推进**：
任一消息失败则整批不推进，下轮重发（客户侧按幂等键去重）。

#### 4.6.5 优雅退出

```
收到 SIGTERM / SIGINT
  → 设置 shutdown_event
  → 调度器停止发起新任务
  → 等待在途 HTTP 请求完成（超时 30 秒）
  → 刷盘水位（原子写）
  → 关闭 Kafka producer（flush）
  → exit(0)
```

### 4.7 可靠性设计

| 机制 | 设计 | 对应需求 |
|---|---|---|
| **重试** | 指数退避 + 抖动：`1s → 2s → 4s → 8s`，上限 5 次 | FR-10 |
| **认证失败不重试** | `AuthError` 直接抛出，只记录一次 | FR-10、§2.2.9 坑 17 |
| **单条 SQL 失败隔离** | 10 条 SQL 各自独立重试，一条失败不影响其他 | §4.2.5 |
| **令牌管理** | 缓存 + 到期前刷新（`session_timeout - 60s`）+ 401 强制刷新重试 1 次 | §4.2.1 |
| **投递失败不推进水位** | 批次原子推进 | FR-08、I-1 |
| **重启续传** | 水位文件持久化 | FR-09 |
| **进程自愈** | 连续 N 轮全失败 → 打 ERROR 日警（**不退出**，交由 systemd/supervisor 重启） | §4.9 |
| **崩溃保护** | 水位文件原子写；异常不导致水位损坏 | FR-09 |
| **平台限流** | 并发数可配（`CMD_CONCURRENCY`），失败自动降级串行 | §8 风险 |

### 4.8 性能预算

| 路径 | HTTP 调用数 | 单次耗时 | 并发 | 墙钟 |
|---|---|---|---|---|
| A 监控指标 | 1 | 0.6~3 s | — | ~2 s |
| B 命令中心（10 条 × 3 跳） | 30 | 3~8 s / 轮 | 10 | **8~15 s** |
| C 诊断优化 | 2 | 1.7~2.1 s | 2 | ~2 s |
| D 实例元数据 | 0（缓存） | — | — | 0 |
| E 告警 | 5 | 0.5~1 s | 5 | ~2 s |
| **单 tick（并发组）** | — | — | — | **≈ 15 s** |

**余量**：30 秒档要求单轮 ≤ 30 秒，实测预算 15 秒 → **2 倍余量**。

**若实测发现 10 条并发把平台压垮**（表现为轮询超时率升高）：
降 `CMD_CONCURRENCY` 到 4~5，并把受影响指标并入 1 分钟档（**需与客户确认周期变更**）。

### 4.9 可观测性

#### 4.9.1 每轮汇总日志（结构化，一行）

```log
2026-11-06 10:20:30 INFO  tick=1234 elapsed=14.8s fetched=1043 mapped=1041
  produced=1041 failed=0 dropped_unsupported=3 windows=9 alarms=2
```

| 字段 | 含义 |
|---|---|
| `tick` | 轮次序号 |
| `elapsed` | 本轮墙钟耗时 |
| `fetched` | 从各通道取回的原始行数 |
| `mapped` | 转换后待投递的指标消息数 |
| `produced` | 成功投递数 |
| `failed` | 投递失败数 |
| `dropped_unsupported` | 不可提供指标计数（FR-13） |
| `windows` | 本轮执行的 SQL 条数 |
| `alarms` | 本轮告警条数 |

#### 4.9.2 告警级日志（ERROR/WARNING）

| 场景 | 级别 | 内容 |
|---|---|---|
| 认证失败 | ERROR | 「认证失败，不重试（防锁号），本轮跳过」 |
| 实例重启导致计数归零 | ERROR | 「检测到计数器归零，丢弃本轮差分并重建基线」 |
| 历史告警超出 30 天留存 | WARNING | 「历史告警存在平台留存窗口外的缺口」 |
| 指标分组返回空 | WARNING | 「指标分组 X 返回空序列（分组码可能已变更）」 |
| 命令 `result` 是失败文案 | ERROR | 「命令执行失败：<result 前 200 字符>」 |
| 连续 N 轮全失败 | ERROR | 「连续 N 轮采集全失败，采集链路可能已中断」 |
| 不可提供指标 | INFO | 「本轮丢弃 N 个不可提供指标（累计 M）」 |

#### 4.9.3 健康检查

暴露一个本地 HTTP 端点（`/healthz`、`/readyz`），返回：

```json
{
  "status": "healthy",
  "last_tick_at": 1793871030000,
  "last_tick_elapsed_seconds": 14.8,
  "consecutive_failures": 0,
  "produced_total": 1834201,
  "dropped_unsupported_total": 3,
  "watermark": {"metrics": 98, "alarms": 12}
}
```

| 端点 | 语义 |
|---|---|
| `/healthz` | 进程活着 → 200 |
| `/readyz` | 最近 3 分钟内至少成功完成 1 轮 → 200，否则 503 |

---

## 5. 代码设计（初步）

### 5.1 目录结构

```
tpops_kafka/
├── main.py              # 入口：装配依赖、启动调度循环、信号处理、优雅退出
├── config.py            # 配置加载（环境变量 + 可选配置文件）
├── scheduler.py         # 到期调度、并发任务组编排、水位推进
├── models.py            # 跨模块 DTO（frozen dataclass，不可变）
├── errors.py            # 异常体系（含「可重试/不可重试」语义）
├── watermark.py         # 水位与差分基线持久化（原子写）
├── health.py            # /healthz、/readyz 本地健康端点
├── tpops/               # ── 平台层：只懂 TPOPS，不懂客户
│   ├── client.py        # HTTP 客户端：认证、会话、错误分类
│   ├── metrics.py       # A 类：监控指标拉取、窗口计算、时间戳切片
│   ├── command_center.py# B 类：三跳编排、并发下发、SQL 合并执行
│   ├── diagnostic.py    # C 类：slow-sql-list / top-sql-list
│   ├── instances.py     # D 类：实例元数据
│   └── alarms.py        # E 类：告警拉取、分页、定义目录关联
├── collectors/          # ── 采集层：通道分治，产出统一 DTO
│   ├── base.py          # Collector 抽象与 CollectContext
│   ├── monitor.py       # A 类采集器
│   ├── command_center.py# B 类采集器（缓存 SQL 结果 + 派生指标）
│   ├── diagnostic.py    # C 类采集器
│   ├── instance.py      # D 类采集器
│   └── alarm.py         # E 类采集器（归并明细 + 幂等去重）
├── mapping/             # ── 映射层：表驱动，不写死客户口径
│   ├── table.py         # 映射表加载、索引、校验
│   ├── rules.py         # 规则引擎：raw/aggregate/ratio/counter_delta/gauge/enum_map/top_n
│   └── data/
│       ├── metric_map.json      # 101 指标 → 取值规则（含占位符、维度绑定、口径 note）
│       ├── field_map.json       # 内部 DTO 字段 → 客户 JSON 字段名
│       ├── sql_batches.json     # 11 条合并 SQL + 覆盖指标 + 刷新周期
│       ├── alarm_catalog.json   # 告警定义目录（catalog_metric + catalog_event 合并）
│       └── unsupported.json     # 3 个不可提供指标及理由
├── kafka/               # ── 投递层
│   ├── producer.py      # PLAINTEXT 生产者、批量发送、错误处理
│   └── serializer.py    # 唯一 JSON 序列化出口（§3.1 信封）
└── state/               # 运行时状态目录（水位、基线、幂等键），gitignore

tests/
├── unit/                # 纯函数测试：规则、窗口、切片、序列化、SQL 映射
├── integration/         # 采集器 → 映射 → 序列化（TPOPS/Kafka 全 mock）
├── fixtures/            # 由 data/ 留档转成的固定样本
└── regression/          # 留档回放：98 指标逐条比对
```

**文件规模约束**（NFR-08）：单文件 < 400 行、函数 < 50 行。
`tools/` 下有几个历史离线脚本超过 400 行，属历史产物，采集端不沿用。

### 5.2 模块职责与依赖方向

```
main.py
  └── scheduler.py ──┬── collectors/* ── tpops/* ── tpops/client.py
                     ├── mapping/table.py + rules.py
                     ├── kafka/producer.py + serializer.py
                     └── watermark.py
models.py  ←  被所有模块引用，不引用任何模块
errors.py  ←  被所有模块引用，不引用任何模块
```

| 模块 | 职责 | 明确**不做**的事 |
|---|---|---|
| `tpops/client.py` | HTTP、认证、错误分类、超时 | 不认识"客户指标" |
| `tpops/command_center.py` | 三跳编排、并发、SQL 执行 | 不做指标映射 |
| `collectors/*` | 把平台原始结构翻译成 `MetricPoint` / `AlarmRecord` | 不做字段命名（那是 mapping 的事） |
| `mapping/rules.py` | 按规则把原始行变成客户值 | 不发 HTTP |
| `kafka/serializer.py` | 唯一的 JSON 出口 | 不做业务判断 |
| `scheduler.py` | 编排与生命周期 | 不认识具体指标 |

### 5.3 核心 DTO（`models.py`）

```python
# Python 3.7 兼容：只用 typing.List/Dict/Optional/Tuple/Union + frozen dataclass
from dataclasses import dataclass
from typing import Optional, Tuple, Union

ValueType = Union[int, float, str, None]


@dataclass(frozen=True)
class MetricPoint:
    """一个客户指标在一个维度上的一个数据点（不可变）。"""

    customer_metric: str            # 客户指标英文名（原样保留 OCR 错字）
    customer_metric_name: str       # 中文名
    customer_category: str          # 13 类之一
    dimensions: Tuple[Tuple[str, str], ...]   # 排序后的 (维度键, 维度值) 对，保证可哈希
    value: ValueType                # 缺失为 None（不填 0）
    value_type: str                 # 整数 / 浮点 / 字符 / 文本
    unit: Optional[str]             # 缺省为 None（不填空串）
    timestamp: int                  # 数据时间（毫秒）
    collect_time: int               # 采集完成时刻（毫秒）
    source: str                     # monitor_metrics / command_center_sql / ...
    instance_id: str
    instance_name: str

    @property
    def dimension_map(self) -> Dict[str, str]:
        return dict(self.dimensions)

    @property
    def idempotency_key(self) -> str:
        dims = "|".join("{}={}".format(k, v) for k, v in self.dimensions)
        return "{}|{}|{}|{}".format(
            self.instance_id, self.customer_metric, dims, self.timestamp)


@dataclass(frozen=True)
class AlarmDetail:
    node_id: str
    host_ip: str
    description: str
    cause: str
    impaction: str
    possible_causes: str
    processing_steps: str
    alarm_detail_raise_count: int


@dataclass(frozen=True)
class AlarmRecord:
    alarm_id: str
    alarm_name: str
    alarm_type: str
    alarm_category: str
    severity: str
    status: str
    instance_id: str
    instance_name: str
    first_alarm_time: int
    last_alarm_time: int
    clear_alarm_time: Optional[int]
    alarm_duration: str
    alarm_items_sum: int
    host_ip: Optional[str]
    node_id: Optional[str]
    description: Optional[str]
    impaction: Optional[str]
    possible_causes: Optional[str]
    processing_steps: Optional[str]
    alarm_detail_raise_count: int
    notify_status: Optional[str]
    automatic_clear: Optional[bool]
    details: Tuple[AlarmDetail, ...]

    @property
    def idempotency_key(self) -> str:
        return "{}|{}|{}|{}".format(
            self.alarm_id, self.instance_id, self.node_id or "", self.first_alarm_time)
```

> **为什么 `dimensions` 用元组而不是 dict**：frozen dataclass 生成 `__hash__`，
> dict 字段会导致哈希失败；元组保证真正不可变且可哈希（CLAUDE.md「不可变数据」要求）。
> 对外通过 `dimension_map` 属性拿到 dict。

### 5.4 核心接口签名

#### 5.4.1 平台客户端

```python
# tpops/client.py
class TpopsClient:
    def __init__(self, base_url: str, username: str, password: str,
                 verify_tls: bool = False, timeout: int = 60,
                 session: Optional[requests.Session] = None) -> None:
        ...

    def token(self, refresh: bool = False) -> str:
        """取 X-Mini-Token；失败抛 AuthError（不可重试，防锁号）。"""

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """错误分类：401/403→AuthError；5xx/超时→TransientError；结构异常→ProtocolError。"""

    def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...
```

#### 5.4.2 命令中心

```python
# tpops/command_center.py
@dataclass(frozen=True)
class NodeInfo:
    node_id: str
    node_name: str
    role: str          # master / slave
    node_type: str     # DN / CN
    node_ip: str


@dataclass(frozen=True)
class SqlBatchResult:
    sql_key: str
    rows: List[Dict[str, Any]]
    elapsed_seconds: float
    ok: bool
    error: Optional[str]


class CommandCenterClient:
    def master_node(self, instance_id: str) -> NodeInfo:
        """按实例缓存节点清单（TTL 1 小时）；取 role == 'master'。"""

    def cn_node_ids(self, instance_id: str) -> List[str]:
        """top-sql-list 需要全部 CN 节点 id。"""

    def submit(self, instance_id: str, node_id: str,
               sql_text: str, limit: int) -> Tuple[str, str]:
        """第 1 跳 → (job_id, task_id)。"""

    def poll(self, job_id: str, timeout: int = 60) -> Dict[str, Any]:
        """第 2 跳。结束判据：statistics['running'] == 0（绝不用 job_status）。"""

    def fetch_result(self, job_id: str, task_id: str) -> List[Dict[str, Any]]:
        """第 3 跳。result 是 JSON 字符串 → 二次 json.loads；校验失败文案。"""

    def run_sql(self, instance_id: str, sql_key: str, sql_text: str,
                limit: int) -> SqlBatchResult:
        """单条 SQL 的完整三跳（含单条重试）。"""

    def run_batch(self, instance_id: str,
                  items: List[Tuple[str, str, int]],
                  concurrency: int) -> Dict[str, List[SqlBatchResult]]:
        """并发下发 + 统一轮询；单条失败隔离。"""
```

#### 5.4.3 规则引擎

```python
# mapping/rules.py
@dataclass(frozen=True)
class CounterBaseline:
    value: float
    collected_at: int     # 毫秒


@dataclass(frozen=True)
class MetricRule:
    customer_metric: str
    source_column: str
    rule_type: str          # raw/aggregate/ratio/counter_delta/gauge/enum_map/top_n
    aggregate_fn: str       # sum/max/count/first（aggregate 用）
    condition: Dict[str, str]   # 条件聚合的匹配条件（如 state == 'active'）
    divisor: str            # ratio 的分母列
    enum_map: Dict[str, str]
    top_n: int
    top_order_by: str
    per_second: bool        # counter_delta 是否除以时间差


class RuleEngine:
    def apply(self, rule: MetricRule, row: Dict[str, Any],
              baseline: Optional[CounterBaseline]) -> ValueType:
        """纯函数：同样的输入永远得到同样的输出（便于单测与回放）。"""
```

#### 5.4.4 采集器

```python
# collectors/base.py
@dataclass(frozen=True)
class CollectContext:
    tick: int
    now_ms: int
    instances: Tuple[InstanceInfo, ...]
    previous_results: Dict[str, Any]     # 上一轮 SQL 结果缓存


class Collector:
    def collect(self, ctx: CollectContext) -> List[MetricPoint]:
        """纯函数式：输入上下文，输出 DTO 列表。不发 HTTP 之外的副作用。"""
```

#### 5.4.5 调度器

```python
# scheduler.py
class Scheduler:
    def run_forever(self) -> int:
        """主循环。收到 shutdown_event 后优雅退出，返回进程退出码。"""

    def _tick(self) -> TickResult:
        """一次 tick：挑出到期任务 → 并发执行 → 汇总 → 推进水位。"""
```

### 5.5 配置项（全部走环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `TPOPS_USER` / `TPOPS_PASSWORD` | — | **必填**；凭据只走环境变量 |
| `TPOPS_BASE_URL` | `https://172.16.103.193:8002` | 平台地址 |
| `TPOPS_VERIFY_TLS` | `false` | 平台自签证书，默认不校验 |
| `TPOPS_REQUEST_TIMEOUT` | `60` | 单次 HTTP 超时（秒）；指标接口另按需放大 |
| `KAFKA_BOOTSTRAP_SERVERS` | — | **必填**；逗号分隔 |
| `KAFKA_TOPIC_METRICS` | `gaussdb.metrics` | 指标 Topic |
| `KAFKA_TOPIC_ALARMS` | `gaussdb.alarms` | 告警 Topic |
| `KAFKA_TOPIC_PARTITIONS` | `4` | 分区数 |
| `KAFKA_COMPRESSION` | `lz4` | 压缩算法 |
| `POLL_INTERVAL_SECONDS` | `30` | 调度 tick 周期（秒） |
| `METRIC_WINDOW_HOURS` | `2` | 监控指标窗口（小时），**下限强制 1** |
| `METRIC_END_LAG_MINUTES` | `5` | `end_time` 回退量（分钟） |
| `CMD_CONCURRENCY` | `9` | 命令中心并发下发数（1~10） |
| `CMD_LIMIT` | `1000` | SQL `limit` 参数（1~1000） |
| `CMD_POLL_TIMEOUT` | `60` | 单个任务轮询超时（秒） |
| `CMD_RETRY_ATTEMPTS` | `3` | 单条 SQL 重试次数 |
| `ALARM_LIMIT` | `100` | 告警分页大小（平台上限 100） |
| `ALARM_RETENTION_DAYS` | `30` | 平台历史告警留存窗口（用于超期告警检测） |
| `SQL_MASKING_ENABLED` | `true` | 慢 SQL / 会话 SQL 文本脱敏开关 |
| `SQL_TEXT_MAX_LENGTH` | `512` | 脱敏后保留的最大长度 |
| `RETRY_MAX_ATTEMPTS` | `5` | 通用重试次数 |
| `RETRY_BASE_SECONDS` | `1` | 退避基数（秒），指数增长 + 抖动 |
| `STATE_DIR` | `./state` | 水位/基线/幂等键目录 |
| `HEALTH_HOST` / `HEALTH_PORT` | `127.0.0.1` / `8088` | 健康端点 |
| `LOG_LEVEL` | `INFO` | 日志级别 |
| `LOG_FORMAT` | `text` | `text` / `json` |

**约束**：所有配置在**启动时一次性加载并校验**，缺失必填项直接**快速失败**（不进入主循环）。

### 5.6 映射表格式（表驱动）

#### 5.6.1 `metric_map.json`

```json
{
  "version": "1.0",
  "metrics": [
    {
      "customer_metric": "GAUSSDB.[{#DATNAME}]_committed_transactions",
      "customer_metric_name": "[{#DATNAME}] Commited transactions",
      "customer_category": "数据库信息",
      "source": "command_center_sql",
      "sql_key": "stat_database",
      "value_type": "整数",
      "unit": null,
      "data_update_cycle": 1,
      "collect_cycle_unit": "分",
      "dimension_bindings": { "DATNAME": "datname" },
      "rule": { "type": "raw", "column": "xact_commit" },
      "match_level": "直接对应",
      "note": "累计事务数（未差分）；客户若需速率见 [{#DATNAME}]_fetch_records_per_second 同源口径"
    },
    {
      "customer_metric": "GAUSSDB.[{#DATNAME}]_fetch_records_per_second",
      "rule": { "type": "counter_delta", "column": "tup_fetched", "per_second": true },
      "note": "累计值差分算速率；实例重启归零时丢弃本轮"
    }
  ]
}
```

#### 5.6.2 `sql_batches.json`

```json
{
  "version": "1.0",
  "batches": [
    {
      "sql_key": "stat_database",
      "sql": "select datname, numbackends, ... from pg_stat_database order by datname",
      "limit": 1000,
      "refresh_seconds": 60,
      "covers": ["GAUSSDB.Current_number_of_connections", "gaussdb.Xact_commit", "…"]
    }
  ]
}
```

#### 5.6.3 `field_map.json`

把内部 DTO 字段名映射成客户 JSON 字段名，**客户确认后只改这一层**：

```json
{
  "version": "1.0",
  "metric_fields": {
    "customer_metric": "metric_name",
    "customer_metric_name": "metric_name_cn",
    "customer_category": "metric_category",
    "dimensions": "dimensions",
    "value": "value",
    "value_type": "value_type",
    "unit": "unit",
    "timestamp": "timestamp",
    "collect_time": "collect_time",
    "source": "source"
  },
  "alarm_fields": { "…": "…" },
  "fixed_fields": { "msg_type": "metric", "schema_version": "1.0" }
}
```

#### 5.6.4 映射表的**启动期校验**（防止映射表与真实数据脱节）

| 校验 | 失败处理 |
|---|---|
| 每条规则的 `source_column` 存在于对应 SQL 的**实际返回列**中 | 启动失败并打印差异（`sql_batches` 的列由 `data/command_center/results/` 留档推导） |
| 每个可提供指标都在 `metric_map` 中 | 启动失败 |
| 每个 `metric_map` 条目都对应一个真实可提供指标 | 启动失败（防拼写错误） |
| 占位符与 `dimension_bindings` 一一对应 | 启动失败 |

> 这条校验把"映射表写错"从**运行期静默产出错误数据**变成**启动期立即失败**——
> 这是本项目最重要的防御性设计之一。

### 5.7 异常体系

```python
# errors.py
class TpopsError(RuntimeError):
    """所有平台层错误的基类。"""


class AuthError(TpopsError):
    """认证失败 —— 不可重试（重试会累计 login_failed_attempts 并锁号）。"""


class TransientError(TpopsError):
    """网络/5xx/超时 —— 可重试。"""


class ProtocolError(TpopsError):
    """平台返回结构与预期不符 —— 不可重试（需要人工介入改代码）。"""


class MappingError(TpopsError):
    """映射表与实际数据不匹配 —— 不可重试。"""


class ProduceError(TpopsError):
    """Kafka 投递失败 —— 可重试，且不推进水位。"""
```

**重试判定表**（重试逻辑集中在 `retry.py`，不散落在各模块）

| 异常 | 可重试 | 退避 |
|---|---|---|
| `AuthError` | ✗ | — |
| `ProtocolError` | ✗ | — |
| `MappingError` | ✗ | — |
| `TransientError` | ✓ | 指数退避 + 抖动 |
| `ProduceError` | ✓ | 指数退避 + 抖动 |
| `requests.Timeout` / `ConnectionError` | ✓ | 指数退避 + 抖动 |

### 5.8 Python 3.7.9 红线

**语法/标准库禁用**（3.8+ 才有）：

- 海象 `:=`、`f"{x=}"`、字典合并 `d1 | d2`
- 内建泛型注解 `list[str]` / `dict[str, int]` → 一律 `typing.List/Dict/Optional/Set/Tuple`
- `typing.Literal` / `Protocol` / `TypedDict` / `Final` → 用 `NamedTuple` / frozen dataclass
- `functools.cached_property`、`importlib.metadata`、`math.prod`
- `zoneinfo` → 用 `pytz` / `dateutil`
- `asyncio.to_thread` → `loop.run_in_executor`
- `str.removeprefix` / `removesuffix`

**可用**：`dataclasses`、f-string、`typing.NamedTuple`、`asyncio.run`、`subprocess.run(capture_output=)`。

**依赖锁版本**（3.7 已停止维护，新版装不上）：

```
kafka-python==2.0.2    # 纯 Python；2.1+ 不再支持 3.7
requests>=2.25,<2.32   # 2.32 起要求 Python ≥3.8
pytest>=7,<8
pytest-cov<5
```

> 若改用 `confluent-kafka`，注意其 wheel 对 3.7 的支持范围，且需 librdkafka 运行时——
> 考虑到目标环境只有 Python 3.7.9，**优先选纯 Python 的 kafka-python**。

### 5.9 编码与提交约定

- **凭据只走环境变量**：禁止硬编码、禁止写入日志、禁止入库（`.env` 已 gitignore）。
- **不可变数据**：跨模块传递用 frozen dataclass，不原地修改。
- **错误显式处理**：捕获后包装成自定义异常并带上下文，**不静默吞异常**；用 `logging`，不用 `print`。
- **提交信息**：conventional commits（`feat:` / `fix:` / `docs:` / `refactor:` / `test:` / `chore:`），正文中文。

---

## 6. 测试策略

### 6.1 测试分层

| 层 | 范围 | 依赖 | 目标覆盖率 |
|---|---|---|---|
| **单元** | 规则引擎（7 种规则）、窗口计算、时间戳切片、差分与重启归零、序列化信封、SQL 合并映射、告警幂等键、掩码与截断 | **无网络、无 Kafka** | 规则与映射层 **≥95%**，整体 **≥80%** |
| **集成** | 采集器 → 映射 → 序列化全链路；调度器 due-time；水位推进；重试与退避 | **TPOPS 与 Kafka 全 mock** | ≥80% |
| **回归** | 98 个指标逐条回放真实留档，断言产出的值与留档一致 | 留档当固定样本 | 98/98 通过 |
| **端到端（手工）** | 真实环境跑一轮，人工核对客户看板 | 真实 TPOPS + 真实 Kafka | 交付前 1 次 |

**Mock 策略**：
- `TpopsClient` 用 `responses` 或自建 stub，按 `data/` 下的真实留档返回。
- `KafkaProducer` 用内存实现，记录发送的消息供断言。
- **不使用**真实网络（CI 里绝不允许）。

### 6.2 回归样本回放（这是本项目最大的测试资产）

| 留档 | 数量 | 用途 |
|---|---|---|
| `data/metric_verification/*.json` | 98 | 每个指标的真实响应 → 断言规则引擎产出的值 |
| `data/command_center/results/*.json` | 197 | 三跳解析（`result` 二次 `json.loads`、`statistics.running` 判据、掩码 SQL） |
| `data/alarm/*.json` | 5 | 告警收敛/明细/统计解析 |
| `data/instances.json` | 1 | 实例元数据 |

**回归断言清单**

| # | 断言 |
|---|---|
| R1 | 98 个指标各自能解析出至少一个值（无数据指标的期望值是 `null`） |
| R2 | `result` 是 JSON 字符串的场景，解析后为 `List[dict]` |
| R3 | `job_status == "running"` 但 `statistics.running == 0` 的样本，判定为**已完成** |
| R4 | `status == "succeed"` 但 `result` 含失败文案的样本，判定为**失败** |
| R5 | `sql_text` 被掩码为 `******` 的样本，不影响解析 |
| R6 | 6 个"查询成功但无数据"的指标，产出 `null` 而非异常 |

### 6.3 关键测试用例清单

| # | 用例 | 类型 | 对应需求 |
|---|---|---|---|
| T01 | `counter_delta` 首轮无基线 → 返回 `null` | 单元 | FR-07 |
| T02 | `counter_delta` 计数器归零 → 返回 `null` + 重建基线，**不产生负数** | 单元 | FR-07 |
| T03 | `counter_delta` 时间差用实际采集时刻差而非标称周期 | 单元 | FR-07 |
| T04 | `ratio` 分母为 0 → 返回 0 | 单元 | FR-05 |
| T05 | `aggregate` 按 `state == 'active'` 条件聚合 | 单元 | — |
| T06 | `enum_map` 未命中 → 原样透传 + 告警 | 单元 | — |
| T07 | 窗口 < 1 小时 → 抛错（强制下限） | 单元 | FR-15 |
| T08 | `end_time` = now − 5 分钟 | 单元 | FR-15 |
| T09 | `is_latest_point_only` 是**字符串** `"false"` | 单元 | FR-16 |
| T10 | 时间戳切片只取本档周期内、且非 null 的点 | 单元 | FR-01 |
| T11 | 维度展开：77 行 → 77 条消息，`dimensions` 含 `NAME` | 集成 | FR-06 |
| T12 | 维度展开：无占位符指标 `dimensions == {}` | 集成 | FR-06 |
| T13 | 投递失败 → 水位**未**推进 | 集成 | FR-08 |
| T14 | 投递全成功 → 水位推进 | 集成 | FR-08 |
| T15 | 重启后从水位续传，不重复不丢失 | 集成 | FR-09 |
| T16 | 认证失败 → **不发起第二次认证** | 单元 | FR-10 |
| T17 | `TransientError` → 指数退避重试 5 次 | 单元 | FR-10 |
| T18 | 告警幂等键去重：同键重复投递 → 仅一条 | 单元 | FR-11 |
| T19 | SIGTERM → 退出码 0，水位文件完整 | 集成 | FR-12 |
| T20 | 告警分页：>100 条时取全 | 集成 | FR-17 |
| T21 | 历史告警超 30 天 → 产生 WARNING 日志 | 单元 | FR-18 |
| T22 | 3 个不可提供指标 → 无消息 + 计数递增 | 集成 | FR-13 |
| T23 | 慢 SQL 文本默认脱敏（字面量替换为 `?`）+ 截断到 512 | 单元 | FR-14 |
| T24 | 映射表列名与 SQL 实际返回列不一致 → **启动失败** | 单元 | §5.6.4 |
| T25 | 序列化输出不含密码/token，且 `null` 不被写成 0 | 单元 | NFR-06 |
| T26 | 告警 >100 条时 `limit` 分页正确 | 集成 | FR-17 |

### 6.4 CI 与质量门禁

| 门禁 | 规则 |
|---|---|
| 语法 | 静态检查禁止 3.8+ 语法（可用 `vermin` 或自定义 AST 扫描） |
| 风格 | `black` + `isort` + `ruff` |
| 类型 | `mypy`（3.7 兼容模式） |
| 测试 | `pytest --cov=tpops_kafka --cov-report=term-missing --cov-fail-under=80` |
| 密钥扫描 | 提交前扫描硬编码凭据 |

---

## 7. 实施计划

### 7.1 里程碑

> 下表是**阶段顺序与退出准则**（与内容无关）；**对应的日历日期、投入核算与范围裁剪预案**
> 见 §7.1.1「里程碑时间线（2026-11-01 → 2026-12-15）」。

| 阶段 | 内容 | 产出 | 退出准则 |
|---|---|---|---|
| **M1** | 骨架 + 配置 + 异常体系 + DTO + Kafka 生产者 + 序列化 + 健康端点 | 可启动、能收信号、空跑不报错 | `python -m tpops_kafka.main` 起停正常；序列化快照测试通过 |
| **M2** | `tpops/client.py` + 认证 + 错误分类 + 重试框架 | 平台层可用 | 认证失败不重试（T16）；错误分类单测通过 |
| **M3** | **A 类采集器**（11 指标） | 11 个指标上 Kafka | 与 `data/metric_verification` 的 A 类留档**逐条比对一致** |
| **M4** | **命令中心客户端**（三跳 + 并发 + 重试）+ 11 条合并 SQL | B 类基础设施 | 三跳解析回归 R2~R5 通过；单条 SQL 3~8 秒内返回 |
| **M5** | **规则引擎 + B 类采集器**（82 指标） | 82 个指标上 Kafka | 回归 R1 全通过；一轮墙钟 ≤ 25 秒 |
| **M6** | C/D 类采集器 + 告警采集器 + 幂等去重 | 4+1 指标 + 告警上 Kafka | T18/T20/T21 通过；告警明细正确挂载 |
| **M7** | 调度器 + 水位 + 差分基线 + 优雅退出 + 健康端点 | 持续运行 | T13~T15/T19 通过；杀进程重启无重复无丢失 |
| **M8** | 映射表全量落地 + 脱敏 + 不可提供清单 + 交付文档 | 可交付 | 98 指标覆盖校验通过；D1~D8 验收 |

> **M4/M5 是关键路径**：82/101 的指标（81%）在这里，
> 且"合并 SQL 的正确性"决定全部 B 类指标的成败。

### 7.1.1 里程碑时间线（2026-11-01 → 2026-12-15）

**锚点**：客户需求于 **11-01（周日）** 下达；第一版于 **12-15（周二）** 交付，
区间共 **32 个工作日**（11-02~11-30 共 21 天 + 12-01~12-15 共 11 天），
扣除已完成的调研期，实际投入窗口为 **11-09~12-15 共 27 个工作日**。
（含 11-01 需求下达当天在内的 P0 阶段已占用 5 个工作日，故 11-01~11-08 记为 5 天。）

**一句话节奏**：调研已跑完 → 用一周把方案改对 → 四周把编码推到底 →
留两天联调验收（12-12 周六作缓冲）。**方案修订（P1）不可压缩**——评审已确认 12 条 BLOCKER，
带着它们写代码等于把错误复制进 82 条合并 SQL 和 98 条映射里。

#### 阶段总览

| 阶段 | 起止 | 工作日 | 核心产出 | 出口 |
|---|---|---|---|---|
| **P0** 需求与调研 | 11-01 ~ 11-08 | 5 | 实测留档、方案 v2.0、评审意见 | **已完成** |
| **P1** 方案修订 v2.1 | 11-09 ~ 11-13 | 5 | 方案 v2.1、映射表 v2 | BLOCKER/MAJOR 全部关闭 |
| **P2** 代码基础框架开发 | 11-16 ~ 11-20 | 5 | 可启动骨架 + TPOPS 客户端 | M1、M2 退出准则达成 |
| **P3** 采集映射推送开发（**核心**） | 11-23 ~ 12-04 | 10 | A/B/C/D 类 + 告警全量上 Kafka | M3~M6 退出准则达成 |
| **P4** 调度与测试 | 12-07 ~ 12-11 | 5 | 常驻进程 + 映射表全量 + 交付文档 | M7、M8 退出准则达成 |
| **P5** 联调验收 | 12-14 ~ 12-15 | 2 | 验收报告、部署包 | **第一版交付** |

#### P0 需求与调研（11-01 ~ 11-08，**已完成**）

11-01 客户需求下达，交付物为客户指标库（101 指标）与监控平台接入指引。
随后一周只做"把事实摸清"，不做编码：11-02~11-04 解析客户指标库并逐条判定
TPOPS 可提供性（结论 **98 可提供 / 3 无法提供**）；11-05~11-07 完成平台侧实测——
逐指标真实调用 **98/98 通过**（`data/metric_verification/`）、统一命令中心
**197 份请求响应留档**、告警侧 288 条告警定义、以及多 master 下发与
并发耗时两个探针（这两条结论直接推翻了 v1.0 的两个承重假设）。
11-08 输出方案 v2.0，并做 4 路独立评审（事实核对 / 设计自洽 / 工程完备 / 假设挑战），
**结论是 12 条 BLOCKER + 12 条 MAJOR 尚未修复**——这直接决定了 P1 的存在。

#### P1 方案修订 v2.1（11-09 ~ 11-13）

这一周不改代码，改设计。三件事必须做完：

1. **关闭 12 条 BLOCKER**。重点是三处结构性问题：A 类 `series` 键不是主机 IP 而是组件 ID
   且一个客户指标对应多条 series（多 series → 单指标的合成口径此前从未定义）；
   B 类合并 SQL 的行展开口径未定义（SQL-1 实测 9 行 × 26 指标会产生 9 个相同幂等键，
   去重后只留 1 条，丢 8/9）；两条合并 SQL 的排序方向相反，一条 SQL 服务不了两个指标。
2. **关闭 12 条 MAJOR**。其中影响面最大的是「累计计数类指标到底是累计值还是速率」
   （44 个指标标注"需要生成性能基线"，口径不定则 39~44 条映射的值形态不定）、
   告警幂等键缺 `last_alarm_time`（恢复事件会被去重永久丢弃）、
   以及「24 小时档在调度器里无处安放」。
3. **机械重生成 §4.3.2 的 82 行映射表**——评审明确要求"不要逐格手改"，
   改由 `tools/build_customer_metric_mapping.py` 从源数据重算，
   并让启动期校验（§5.6.4）能在 `source_column` 上真正通过。

**P1 的退出准则**：方案 v2.1 定稿、评审清单逐条标注"已修 / 已接受风险"、
映射表可重复生成且通过启动校验。**这一步不通过，不允许进入 P2**——
因为 M5 的 82 条映射全部依赖它。

> **客户侧死线（硬依赖）**：§3.9 的待确认项中，
> **Kafka 地址与 Topic 名、指标与告警是否同 topic、告警字段规范、慢 SQL 是否可外发**
> 四项必须在 **11-13 前**拿到客户书面结论。这四项不阻塞编码，但**阻塞联调**：
> Kafka 地址不到手，M1 的生产者与 M5 的端到端验证都只能空转；
> 慢 SQL 外发口径不到手，M8 的脱敏策略只能按最严口径实现（默认不外发），
> 事后放宽是改动、放严是客户投诉。

#### P2 代码基础框架开发（11-16 ~ 11-20）

并行两条线：主线做 **M1**（配置 + 异常体系 + DTO + Kafka 生产者 + JSON 序列化 +
健康端点 + 入口装配），副线做 **M2**（`tpops/client.py`：mini-token 认证、
错误分类、重试退避框架）。两条线都只依赖 `models.py`/`errors.py`，
可以同一人串行切换，也可以两人并行。

本周结束时的可验证状态：**`python -m tpops_kafka.main` 能起、能收信号、空跑不报错**，
且平台层已具备"认证失败不重试"的能力（登录失败会累计 `login_failed_attempts` 并锁号，
盲目重试会直接把账号锁死）。

#### P3 采集映射推送开发（核心）（11-23 ~ 12-04）

两周（10 个工作日）内跑完四条采集链。结构上按 WBS 的并行度拆：

- **第一周（11-23~11-27）**：**M3**（A 类 11 指标，走监控指标接口）与
  **M4**（命令中心三跳客户端 + 并发 + 重试 + 合并 SQL）并行。
  M4 是关键路径的起点——B 类 82 指标全部压在它上面，且合并 SQL 的正确性
  决定 81% 指标的成败。
- **第二周（11-30~12-04）**：**M5**（规则引擎 + B 类采集器 82 指标）与
  **M6**（C/D 类 5 指标 + 告警采集器）并行。M5 是整个项目最重的一块
  （估算 5 人天，占编码量的近四分之一），退出准则是回归 R1 全通过
  且单轮墙钟 ≤ 25 秒。

#### P4 调度与测试（12-07 ~ 12-11）

- **12-07~12-09**：**M7**——调度器（周期分档 + 并发组）、水位推进、差分基线、
  优雅退出、健康端点。退出准则是最硬的一条：**杀进程重启无重复无丢失**
  （水位只在投递成功后推进）。
- **12-10~12-11**：**M8**——98 条映射全量落地、脱敏、3 个不可提供指标清单、
  交付文档（逐条口径说明 + 疑似 OCR 错字清单 + 部署运行说明）。
- **12-09 构建完成后即挂机做 24 小时连续运行观察**（12-10 跑满 24 小时，
  12-11 核对日志），观察期内做一次杀进程重启演练。
  注意口径区分：**24 小时观察验证的是进程稳定性与水位续传**；
  M8 的映射与脱敏改动影响的是消息内容，由 12-14 的端到端联调验证。
- **12-12（周六）留作缓冲**：M7/M8 任一延期，就吃掉这一天，不顺延到验收日。

#### P5 联调验收（12-14 ~ 12-15）

12-14 在真实 TPOPS + 客户 Kafka 上跑完整一轮（**用 M8 的最终映射与脱敏配置**），
人工核对客户看板；再做一次杀进程重启演练，确认重启后水位正确续传。
12-15 走 D1~D8 验收、交付部署包与文档，**第一版发布**。

#### 投入核算与范围裁剪预案

| 阶段 | 估算（人天） | 说明 |
|---|---|---|
| P1 方案修订 | 5 | 24 条评审意见逐条关闭 + 映射表重生成 |
| P2 代码基础框架（M1+M2） | 6 | 3 + 3 |
| P3 采集映射推送开发（M3/M4/M5/M6） | 13 | 2 + 3 + 5 + 3，M3∥M4、M5∥M6 |
| P4 调度与测试（M7+M8） | 6 | 3 + 3 |
| P5 联调验收 | 2 | 含 24 小时运行的收尾观察 |
| **合计** | **约 32 人天** | **27 个工作日 → 平均 1.2 人，P3 峰值 2 人** |

**裁剪预案（按此顺序砍）**：第一版若被压缩到单人或期限提前，按下列顺序让出范围，
每砍一项都必须同步下调 §1.7 的验收口径，而不是偷偷少投数据：

1. **M6 的告警采集器**（约 2 人天）——A/B/C/D 类共 98 个指标是客户指标库的主体，
   告警是另一条线；砍掉后第一版仍满足"指标入库"的主诉求，
   告警作为 v1.1 交付（但 §3.9 的告警字段规范仍须 11-13 前问清）。
2. **M6 的 C/D 类诊断优化接口**（约 1 人天）——4+1 个指标占比最小。
3. **24 小时连续运行观察**（约 1 人天）——降级为"连续 2 小时 + 杀进程重启演练"，
   但 D4/D5 的验收口径须相应下调并书面说明。

**不可裁剪项**：P1 方案修订、M5 的 B 类采集器、M7 的水位与重启续传。
这三项任何一项缩水，交付的都是"能跑但数据错"的版本——
而错的数据比没有数据更危险（§1.1）。

### 7.2 任务分解（WBS）

| ID | 任务 | 依赖 | 负责模块 |
|---|---|---|---|
| W01 | 配置加载与启动期校验 | — | `config.py` |
| W02 | DTO 与异常体系 | — | `models.py` `errors.py` |
| W03 | 重试/退避框架 | W02 | `retry.py` |
| W04 | JSON 序列化器（信封） | W02 | `kafka/serializer.py` |
| W05 | Kafka 生产者 | W04 | `kafka/producer.py` |
| W06 | HTTP 客户端 + 认证 + 错误分类 | W02 W03 | `tpops/client.py` |
| W07 | 映射表加载 + 启动期校验 | W02 | `mapping/table.py` |
| W08 | 规则引擎（7 种规则） | W07 | `mapping/rules.py` |
| W09 | 水位与差分基线持久化 | W02 | `watermark.py` |
| W10 | A 类：窗口计算 + 时间戳切片 | W06 W08 | `tpops/metrics.py` |
| W11 | A 类采集器 | W10 W08 | `collectors/monitor.py` |
| W12 | 命令中心三跳客户端 | W06 W03 | `tpops/command_center.py` |
| W13 | 11 条合并 SQL 数据文件 | W07 | `mapping/data/sql_batches.json` |
| W14 | B 类采集器（含缓存与派生） | W12 W13 W08 | `collectors/command_center.py` |
| W15 | C 类（slow-sql / top-sql） | W06 W08 | `tpops/diagnostic.py` `collectors/diagnostic.py` |
| W16 | D 类（实例元数据） | W06 W08 | `tpops/instances.py` `collectors/instance.py` |
| W17 | 告警拉取 + 分页 + 明细归并 + 幂等 | W06 W09 | `tpops/alarms.py` `collectors/alarm.py` |
| W18 | SQL 文本脱敏与截断 | W02 | `mapping/rules.py` 或独立 `masking.py` |
| W19 | 调度器（due-time + 并发组） | W05 W09 W11 W14 W15 W16 W17 | `scheduler.py` |
| W20 | 健康端点 | W19 | `health.py` |
| W21 | 入口装配 + 信号处理 | W19 W20 | `main.py` |
| W22 | 回归测试夹具（留档 → fixture） | — | `tests/fixtures/` |
| W23 | 单元/集成/回归测试 | W01~W21 | `tests/` |
| W24 | 交付文档 + 不可提供清单 | W23 | `docs/` |

**并行度**：W06~W09 可并行；W10~W11、W12~W14、W15~W17 三条采集链可并行；
W19 依赖前三条链全部完成。

### 7.3 关键路径

```
W02 → W06 → W12 → W14 → W19 → W21 → W23 → W24
```

**估算**（按 1 人开发、熟悉 Python）：

| 阶段 | 估算 |
|---|---|
| M1~M2（基础设施） | 3 天 |
| M3（A 类） | 2 天 |
| M4（命令中心客户端） | 3 天 |
| M5（规则引擎 + B 类） | 5 天 |
| M6（C/D/告警） | 3 天 |
| M7（调度 + 水位） | 3 天 |
| M8（落地 + 交付） | 3 天 |
| **合计** | **约 22 人天** |

> 本表是**纯编码量**（1 人、不含方案修订）。§7.1.1 的时间线在此基础上另加了
> **P1 方案修订 5 人天**（关闭 12 条 BLOCKER + 12 条 MAJOR）与
> **P5 联调验收 2 人天**，合计约 **32 人天**，
> 落在 11-09~12-15 的 **27 个工作日**内 → 平均 1.2 人、P3 阶段峰值 2 人。
> **前提是 P1 评审意见在 11-13 前全部关闭**；否则编码阶段返工，32 人天会溢出。

---

## 8. 风险登记册

### 8.1 技术风险

| ID | 风险 | 概率 | 影响 | 等级 | 应对 | 触发信号 |
|---|---|---|---|---|---|---|
| R-01 | 10 条并发 SQL 压垮平台或触发限流 | 中 | 高 | **HIGH** | 并发数可配；失败自动降级串行；实测压测后再定档 | 轮询超时率 > 5% |
| R-02 | 累计值差分在实例重启后失真（假峰值/负数） | 高 | 中 | **HIGH** | 归零检测 → 丢弃本轮 + 重建基线 + ERROR 日志（T02） | 出现负速率 |
| R-03 | 快照型指标无平台历史，采集端停机即数据空洞 | 高 | 中 | **HIGH** | 水位续传只能补当前值；**空洞不可回补** → 监控采集端自身可用性（`/readyz`） | `/readyz` 连续 503 |
| R-04 | 历史告警仅 30 天留存，长时间停机丢告警 | 中 | 中 | MEDIUM | 显式 WARNING 日志（FR-18）+ 采集端可用性监控 | 告警日志出现超期提示 |
| R-05 | `result` 字段二次解析，平台格式变化会静默变成字符串 | 中 | 高 | **HIGH** | 解析失败显式抛 `ProtocolError`，不吞 | 解析异常日志 |
| R-06 | 节点任务 `succeed` 但命令实际失败 | 中 | 高 | **HIGH** | 校验 `result` 内容；失败文案单独识别（R4） | 指标值全为 0/空 |
| R-07 | 客户字段与 TPOPS 语义差异（10 近似 + 6 需换算） | 高 | 中 | **HIGH** | 每条映射带 `note` 说明口径；交付文档同步；交付前与客户逐条确认 | 客户反馈"值对不上" |
| R-08 | 平台接口变更（新增分组、改字段） | 中 | 中 | MEDIUM | 启动期映射校验 + 空分组告警 + 协议错误显式抛出 | 启动校验失败 / WARNING |
| R-09 | 认证失败导致账号被锁 | 低 | 高 | **HIGH** | 认证失败**不重试**；只记录一次（T16）；上线前演练 D7 | 认证异常日志 |
| R-10 | Kafka 不可用导致数据堆积 | 中 | 中 | MEDIUM | 投递失败不推进水位；批次重试；堆积由 Kafka 侧处理 | 投递失败率上升 |
| R-11 | Python 3.7.9 停止维护，新版依赖装不上 | 高 | 中 | **HIGH** | 锁版本；CI 加 3.7 语法扫描门禁；不使用 3.8+ 语法 | CI 依赖解析失败 |
| R-12 | 慢 SQL 文本含业务信息外发 | 中 | **高** | **HIGH** | **默认脱敏 + 截断**；外发需客户书面确认（FR-14） | 合规评审不通过 |
| R-13 | 30 秒档单轮耗时超 30 秒 | 低 | 中 | MEDIUM | 三组并发；实测预算 15 秒，余量 2 倍 | `elapsed` P95 > 25 秒 |
| R-14 | 维度展开导致消息条数暴涨（一条 SQL → 85 条） | 中 | 低 | LOW | 已实测最大 85 条；Kafka 侧无压力 | 消息量异常增长 |

### 8.2 项目与客户风险

| ID | 风险 | 概率 | 影响 | 等级 | 应对 |
|---|---|---|---|---|---|
| P-01 | 客户 JSON 字段定义与我们的信封不一致 | 中 | 中 | MEDIUM | 映射表隔离（§5.6.3）；客户确认后**只改表** |
| P-02 | 客户指标英文名的 OCR 错字被要求更正 | 中 | 低 | LOW | 映射表支持 `alias` 字段，一行内两种写法同时输出 |
| P-03 | 客户要求提升某指标周期（30 秒 → 1 分钟） | 中 | 低 | LOW | 周期在映射表里配置，改表不改码；需客户书面确认 |
| P-04 | 3 个不可提供指标客户不接受 | 中 | 中 | MEDIUM | 书面清单 + 理由；提供替代口径建议（如 `reserved_connections`） |
| P-05 | 客户要求提供性能基线（表里"生成性能基线=是"） | 中 | 中 | MEDIUM | 本方案未实现；需单独评估（基线计算与历史存储） |
| P-06 | 交付环境网络不通 Kafka 或 TPOPS | 中 | 高 | **HIGH** | 部署前连通性检查清单；`/healthz` 暴露依赖状态 |
| P-07 | **§3.9 待确认项（Kafka 地址/Topic 名、告警字段规范、慢 SQL 外发口径等）迟迟不回** | **高** | **高** | **HIGH** | **客户侧死线 11-13**（§7.1.1 P1）；逾期则 Kafka 地址走"占位 Topic + 联调环境"，慢 SQL 按最严口径（默认不外发）实现，告警字段延后到 v1.1 并书面告知客户范围变更 | 11-13 未收到客户书面结论 |
| P-08 | 评审的 12 条 BLOCKER 在 11-13 前未全部关闭就开始编码 | 中 | 高 | **HIGH** | P1 设为编码的准入门槛；映射表必须由 `tools/build_customer_metric_mapping.py` 机械重生成，禁止逐格手改 | 11-13 方案 v2.1 未定稿 |

### 8.3 已在设计中消解的坑（索引到 §2.2.9）

| # | 坑 | 消解位置 |
|---|---|---|
| 1 | 路径前缀缺 `gaussdb` | §4.2 接口清单分两套命名空间 |
| 2 | Token 在 body 找不到 | §4.2.1（从响应头取） |
| 3 | 短窗口取指标 | §4.2.4 窗口 ≥ 1h |
| 4 | 不回退 end_time | §4.2.4 回退 5 分钟 |
| 5 | `is_latest_point_only=true` 丢时间戳 | §4.2.4 硬编码 `"false"` |
| 6 | 分组名写错返回空 | §4.2.4 空分组 WARNING |
| 7 | 看 `job_status` 判完成 | §4.2.5(4) 用 `statistics.running` |
| 8 | `result` 当数组用 | §4.2.5(5) 二次 `json.loads` |
| 9 | SQL 不写别名 | §4.4 全部显式 `as` |
| 10 | 用了 `FILTER` | §4.4 用 `CASE WHEN` |
| 11 | `state <> ''` 恒假 | §4.4 用 `state is not null` |
| 12 | 一次下发多节点 | §4.2.5(3) 只下发 master |
| 13 | SQL 无重试 | §4.2.5(6) 单条独立重试 |
| 14 | `cluster_mode` 传字符串 | §4.2.5(3) 传 JSON 数组 |
| 15 | `sql_text` 掩码 | §4.2.5(5) SQL 存本地 |
| 16 | 命令成功但 result 是失败文案 | §4.2.5(5) 校验 result 内容 |
| 17 | 认证失败盲目重试锁号 | §4.2.1 / §5.7 不可重试 |
| 18 | 映射表与真实数据脱节 | §5.6.4 启动期校验 |
| 19 | 缺值被填成 0 | §3.1.3 一律 `null` |
| 20 | 慢 SQL 文本外发 | §3.8 默认脱敏 |

---

## 9. 附录

### 9.1 全量 101 指标对照表

> 数据源：`data/customer_metric_query_plan.json` × 客户指标库 `指标库` sheet。
> **通道**：A=监控指标接口，B=命令中心SQL，C=诊断优化接口，D=实例接口，E=不可提供。

| # | 客户指标（英文） | 中文名 | 类别 | 通道 | TPOPS 来源 | 匹配 | 数据类型 | 周期 | 单位 | 口径说明 |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `GAUSSDB.Number_of_ansactions_mmitted_r_cond` | 每秒提交事务数 | 事务信息 | A | `rds092_gaussv5_commit_counter` | 直接对应 | 整数 | 5分 | tps | 平台为速率口径；累计值见 pg_stat_database.xact_commit |
| 2 | `GAUSSDB.Number_of_llback_ansactions_r_cond` | 每秒回滚事务数 | 事务信息 | A | `rds093_gaussv5_rollback_counter` | 直接对应 | 整数 | 5分 | tps | 平台为速率口径 |
| 3 | `GAUSSDB.{#NAME}.Number.ad.ples` | ({#NAME}) number of dead tuples | 事务信息 | B | `pg_stat_user_tables.n_dead_tup` | 直接对应 | 整数 | 5分 | — |  |
| 4 | `GAUSSDB.{#NAME}.Number.lete.ples` | ({#NAME}) number of delete tuples | 事务信息 | B | `pg_stat_user_tables.n_tup_del` | 直接对应 | 整数 | 5分 | tuples/s |  |
| 5 | `GAUSSDB.{#NAME}.garbage.Ratio` | ({#NAME})Garbage ratio | 事务信息 | B | `pg_stat_user_tables.n_dead_tup/(n_live_tup+n_dead_tup)` | 直接对应 | 浮点 | 5分 | % |  |
| 6 | `GAUSSDB.{#NAME}.Number.t.date.ples` | ({#NAME}) number of hot update tuples | 事务信息 | B | `pg_stat_user_tables.n_tup_hot_upd` | 直接对应 | 整数 | 5分 | tuples/s |  |
| 7 | `GAUSSDB.{#NAME}.number.Of.index.scan` | ({#NAME})number of index scan | 事务信息 | B | `pg_stat_user_tables.idx_scan` | 直接对应 | 整数 | 5分 | scans/s |  |
| 8 | `GAUSSDB.{#NAME}.number.Of.dex.ples.ad` | ({#NAME})number of index tuples read | 事务信息 | B | `pg_stat_user_tables.idx_tup_fetch` | 直接对应 | 整数 | 5分 | tuples/s |  |
| 9 | `GAUSSDB.{#NAME}.number.Of.live.tuples` | ({#NAME})number of live tuples | 事务信息 | B | `pg_stat_user_tables.n_live_tup` | 直接对应 | 整数 | 5分 | — |  |
| 10 | `GAUSSDB.{#NAME}.number.Of.insert.tuples` | ({#NAME})number of insert tuples | 事务信息 | B | `pg_stat_user_tables.n_tup_ins` | 直接对应 | 整数 | 5分 | tuples/s |  |
| 11 | `GAUSSDB.{#NAME}.number.Of.sequential.scan` | ({#NAME})number of sequencial scan | 事务信息 | B | `pg_stat_user_tables.seq_scan` | 直接对应 | 整数 | 5分 | scans/s |  |
| 12 | `GAUSSDB.{#NAME}.number.Of.quential.ples.ad` | ({#NAME})number of sequencial tuples read | 事务信息 | B | `pg_stat_user_tables.seq_tup_read` | 直接对应 | 整数 | 5分 | tuples/s |  |
| 13 | `GAUSSDB.{#NAME}.number.Of.update.tuples` | ({#NAME})number of update tuples | 事务信息 | B | `pg_stat_user_tables.n_tup_upd` | 直接对应 | 整数 | 5分 | tuples/s |  |
| 14 | `GAUSSDB.Idle_in_transaction_connections` | Idle in transaction connections | 事务信息 | B | `pg_stat_activity.state` | 直接对应 | 整数 | 5分 | — | state='idle in transaction' |
| 15 | `GAUSSDB.Idle_connections` | Idle connections | 事务信息 | B | `pg_stat_activity.state` | 直接对应 | 整数 | 5分 | — | state='idle' |
| 16 | `GAUSSDB.Current_number_of_connections` | 当前连接数 | 事务信息 | B | `pg_stat_database.numbackends` | 直接对应 | 整数 | 5分 | — |  |
| 17 | `GAUSSDB.maximum_connection` | 最大连接数 | 事务信息 | B | `pg_settings.max_connections` | 直接对应 | 整数 | 5分 | — | 实测 200 |
| 18 | `GAUSSDB.Number_of_active_connections` | 活跃连接数 | 事务信息 | B | `pg_stat_activity.state` | 直接对应 | 整数 | 5分 | — | state='active' |
| 19 | `GAUSSDB.Total_slow_queries` | 慢查询总数 | 慢查询信息 | C | `slow-sql-list.sql_id` | 可统计 | 整数 | 5分 | — | 慢SQL集合条数统计 |
| 20 | `GAUSSDB.Select_slow_queries` | Select慢查询数 | 慢查询信息 | C | `slow-sql-list.sql_text` | 可统计 | 整数 | 5分 | — | 按 sql_text 统计 SELECT 类慢查询 |
| 21 | `GAUSSDB.DML_slow_queries` | DML慢查询数 | 慢查询信息 | C | `slow-sql-list.sql_text` | 可统计 | 整数 | 5分 | — | 按 sql_text 统计 DML 类慢查询 |
| 22 | `GAUSSDB.Service_availability_testing` | 服务可用性检测 | 可用性 | A | `gaussdb_instance_status` | 近似对应 | 字符 | 1分 | — | 实测 2026-11-06：`GET /rds/health-status` 返回 200 但**响应体为空**（Content-Length: 0），只反映 open-api 服务存活、不含实例信息，**不能作为本指标的数据源**；可用性改用 `instances[].instance_status` 或 `gaussdb_instance_status` |
| 23 | `gaussdb_{#PORT}port.status` | 服务端口{#PORT}检测 | 可用性 | B | `pg_settings.port` | 近似对应 | 整数 | 1分 | — | 实测 2026-11-06：`GET /rds/health-status` 返回 200 但**响应体为空**（Content-Length: 0），只反映 open-api 服务存活、不含端口/实例信息，**不能用于可用性探测**；本指标仅能取到端口号，可用性需另找来源 |
| 24 | `gaussdb.Deadlocks` | 检测到的死锁次数 | 锁信息 | B | `pg_stat_database.deadlocks` | 直接对应 | 整数 | 1分 | 个 | 平台另有 rds091_gaussv5_deadlocks |
| 25 | `gaussdb.get.lock.info` | 获取数据库锁数据 | 锁信息 | B | `pg_locks` | 直接对应 | 字符 | 1分 | — | pg_locks 全量锁视图 |
| 26 | `gaussdb.Lockwaiting` | 查看当前锁等待数量 | 锁信息 | B | `pg_stat_activity.state` | 直接对应 | 整数 | 60秒 | — | state 含 lock 相关会话 |
| 27 | `gaussdb.locks.get_metrics[{#DBNAME}]` | DB [{#DBNAME}]: 获取锁信息 | 锁信息 | B | `pg_locks` | 近似对应 | 字符 | 2分 | MB | 按 datname 聚合，平台无按 DBNAME 的锁指标 |
| 28 | `gaussdb.locks.accessexclusive[{#DBNAME}]` | DB [{#DBNAME}]: Num of accessexclusive locks | 锁信息 | B | `pg_locks.mode` | 直接对应 | 整数 | 2分 | — | mode='AccessExclusiveLock' 按库聚合 |
| 29 | `gaussdb.Xact_rollback` | 回滚的事务总数 | 性能信息 | B | `pg_stat_database.xact_rollback` | 直接对应 | 整数 | 1分 | 个 | 累计回滚事务数 |
| 30 | `gaussdb.Tps` | TPS（每秒事务数） | 性能信息 | A | `rds400_gaussv5_total_counter` | 直接对应 | 浮点 | 5分 | 个 | 平台为用户总事务数/秒 |
| 31 | `gaussdb.QPS` | QPS（每秒查询数） | 性能信息 | A | `rds402_gaussv5_ddl_dml_dcl_count` | 直接对应 | 浮点 | 5分 | 个 | 平台口径为 DDL+DML+DCL 平均SQL执行数/秒 |
| 32 | `gaussdb.get.connections.sum` | 获取连接信息 | 性能信息 | B | `pg_stat_activity` | 直接对应 | 字符 | 1分 | — | 会话清单（27 个字段） |
| 33 | `gaussdb.get.dbstat.sum` | 获取db性能数据 | 性能信息 | B | `pg_stat_database + pg_stat_user_tables` | 直接对应 | 字符 | 1分 | — | 库级 + 表级统计视图 |
| 34 | `gaussdb.running.sql.limit.20` | 正在运行的SQL | 性能信息 | B | `pg_stat_activity.query` | 直接对应 | 字符 | 2分 | — | 会话当前执行的 SQL 文本 |
| 35 | `gaussdb.Top10sql` | 当前运行时间最长的Top10 SQL | 性能信息 | C | `top-sql-list.total_elapse_time` | 直接对应 | 字符 | 2分 | — | 按 total_elapse_time 排序取 Top N |
| 36 | `gaussdb.Blk_write_time` | 写块的时间（毫秒） | 性能信息 | B | `pg_stat_database.blk_write_time` | 直接对应 | 整数 | 1分 | ms | 累计写块耗时，口径与客户一致 |
| 37 | `gaussdb.Numbackends` | 当前连接到数据库的后端进程数 | 性能信息 | B | `pg_stat_database.numbackends` | 直接对应 | 整数 | 1分 | 个 |  |
| 38 | `gaussdb.Temp_bytes` | 临时文件大小 | 性能信息 | B | `pg_stat_database.temp_bytes` | 直接对应 | 整数 | 1分 | — |  |
| 39 | `gaussdb.Blks_read` | 从磁盘读取的块数 | 性能信息 | B | `pg_stat_database.blks_read` | 直接对应 | 整数 | 1分 | 个 |  |
| 40 | `gaussdb.Blks_hit` | 从缓存命中读取的块数 | 性能信息 | B | `pg_stat_database.blks_hit` | 直接对应 | 整数 | 1分 | — |  |
| 41 | `gaussdb.Tup_deleted` | 删除的行数 | 性能信息 | B | `pg_stat_database.tup_deleted` | 直接对应 | 整数 | 1分 | — |  |
| 42 | `gaussdb.Xact_commit` | 提交的事务总数 | 性能信息 | B | `pg_stat_database.xact_commit` | 直接对应 | 整数 | 1分 | — |  |
| 43 | `gaussdb.Tup_inserted` | 插入的行数 | 性能信息 | B | `pg_stat_database.tup_inserted` | 直接对应 | 整数 | 1分 | — |  |
| 44 | `gaussdb.Conflicts` | 数据库操作冲突次数 | 性能信息 | B | `pg_stat_database.conflicts` | 直接对应 | 整数 | 1分 | ms |  |
| 45 | `gaussdb.Tup_updated` | 更新的行数 | 性能信息 | B | `pg_stat_database.tup_updated` | 直接对应 | 整数 | 1分 | 个 |  |
| 46 | `gaussdb.Tup_returned` | 查询返回的行数 | 性能信息 | B | `pg_stat_database.tup_returned` | 直接对应 | 整数 | 1分 | — |  |
| 47 | `gaussdb.Temp_files` | 生成的临时文件数 | 性能信息 | B | `pg_stat_database.temp_files` | 直接对应 | 整数 | 1分 | b |  |
| 48 | `gaussdb.tup_fetched` | 通过索引获取的行数 | 性能信息 | B | `pg_stat_database.tup_fetched` | 直接对应 | 整数 | 1分 | — |  |
| 49 | `gaussdb.Blk_read_time` | 读块的时间（毫秒） | 性能信息 | B | `pg_stat_database.blk_read_time` | 直接对应 | 整数 | 1分 | 个 | 累计读块耗时，口径与客户一致 |
| 50 | `gaussdb.Idle_in_transaction_aborted` | 事务中已中止的空闲连接数 | 连接情况 | B | `pg_stat_activity.state` | 直接对应 | 整数 | 1分 | 个 | state='idle in transaction (aborted)' |
| 51 | `gaussdb.Idle_in_transaction` | 事务中空闲的连接数 | 连接情况 | B | `pg_stat_activity.state` | 直接对应 | 整数 | 1分 | 个 | state='idle in transaction' |
| 52 | `gaussdb.Fastpath_function_call` | 快路径函数调用的连接数 | 连接情况 | E | `—` | — | 整数 | 1分 | 个 | PG 侧无 fastpath 计数器；pg_settings 仅有 gsi_fastpath_level 开关 |
| 53 | `gaussdb.Total_conn` | 总连接数 | 连接情况 | B | `pg_stat_database.numbackends` | 直接对应 | 整数 | 1分 | 个 |  |
| 54 | `gaussdb.Active_conn` | 活动连接数 | 连接情况 | B | `pg_stat_activity.state` | 直接对应 | 整数 | 1分 | 个 | state='active'；平台另有 rds117 |
| 55 | `gaussdb.Disabled_conn` | 禁用的连接数 | 连接情况 | E | `—` | — | 整数 | 1分 | 个 | PG 无「禁用连接」概念；pg_settings 仅有 reserved_connections 等预留连接 |
| 56 | `gaussdb.Idle_conn` | 空闲连接数 | 连接情况 | B | `pg_stat_activity.state` | 直接对应 | 整数 | 1分 | 个 | state='idle' |
| 57 | `gaussdb.Waiting_conn` | 等待资源的连接数 | 连接情况 | B | `pg_stat_activity.state` | 直接对应 | 整数 | 1分 | % | state 含 wait 类会话 |
| 58 | `gaussdb.Total_pct` | 连接使用率 | 连接情况 | B | `pg_stat_activity.state` | 近似对应 | 浮点 | 1分 | 个 | 活跃会话数 / 总连接数 |
| 59 | `gaussdb.Prepared` | 预提交事务数 | 连接情况 | B | `pg_prepared_xacts` | 直接对应 | 整数 | 1分 | 个 | count(*) 统计预备事务 |
| 60 | `GAUSSDB.PG_Service_Status` | GAUSSDB服务状态 | 基本信息 | A | `gaussdb_instance_status` | 近似对应 | 整数 | 5分 | — | 平台为实例状态，非 pg_is_in_recovery 语义 |
| 61 | `gaussdb.Starttime` | 启动时间 | 基本信息 | A | `rds133_cn_startup_time` | 近似对应 | 字符 | 60秒 | — | 平台为 CN 进程启动时刻(timestamp) |
| 62 | `gaussdb.Uptimestr` | 运行时长 | 基本信息 | A | `rds133_cn_startup_time` | 需换算 | 字符 | 60秒 | — | 平台无 uptime 指标，用启动时刻与当前时间推算 |
| 63 | `gaussdb.server_version` | 版本 | 基本信息 | D | `instances.engine_version` | 直接对应 | 字符 | 1时 | — | GET /rds/v3.2/instances；另有 GET /rds/v3/version-info |
| 64 | `gaussdb.Serverencoding` | 字符集 | 基本信息 | B | `pg_database.encoding` | 直接对应 | 字符 | 24时 | — | pg_encoding_to_char() |
| 65 | `gaussdb.Status` | 数据库状态 | 基本信息 | A | `gaussdb_instance_status` | 近似对应 | 整数 | 30秒 | — | 平台为实例状态 |
| 66 | `gaussdb.Timezone` | 时区 | 基本信息 | B | `pg_settings.TimeZone` | 直接对应 | 字符 | 24时 | 个 |  |
| 67 | `gaussdb.Pagesize` | 页大小 | 基本信息 | B | `pg_settings.block_size` | 直接对应 | 整数 | 1分 | — | 实测 8192 |
| 68 | `GAUSSDB.Standby_status` | Standby状态 | 基本信息 | A | `datanode_switch_status` | 近似对应 | 整数 | 5分 | — | 平台为 DN 主备状态 |
| 69 | `GAUSSDB.Primary_status` | Primary状态 | 基本信息 | A | `datanode_switch_status` | 近似对应 | 整数 | 5分 | — | 平台为 DN 主备状态 |
| 70 | `gaussdb.{#NAME}.Number.lete.ples` | ({#NAME})被删除的行总数（包括已回收行） | 表信息 | B | `pg_stat_user_tables.n_tup_del` | 直接对应 | 整数 | 2分 | — | 累计删除行数 |
| 71 | `gaussdb.{#NAME}.Number.ad.ples` | ({#NAME})已删除但未回收的行数（死亡行） | 表信息 | B | `pg_stat_user_tables.n_dead_tup` | 直接对应 | 整数 | 2分 | — |  |
| 72 | `gaussdb.{#NAME}.Number.t.date.ples` | ({#NAME})通过 HOT更新的行数 | 表信息 | B | `pg_stat_user_tables.n_tup_hot_upd` | 直接对应 | 整数 | 2分 | % |  |
| 73 | `gaussdb.{#NAME}.number.Of.index.scan` | ({#NAME})索引扫描次数 | 表信息 | B | `pg_stat_user_tables.idx_scan` | 直接对应 | 整数 | 2分 | — |  |
| 74 | `gaussdb.{#NAME}.number.Of.quential.ples.ad` | ({#NAME})顺序扫描读取的行数 | 表信息 | B | `pg_stat_user_tables.seq_tup_read` | 直接对应 | 整数 | 2分 | — |  |
| 75 | `gaussdb.{#NAME}.number.Of.dex.ples.ad` | ({#NAME})通过索引扫描获取的行数 | 表信息 | B | `pg_stat_user_tables.idx_tup_fetch` | 直接对应 | 整数 | 2分 | — |  |
| 76 | `gaussdb.{#NAME}.number.Of.insert.tuples` | ({#NAME})插入的行数 | 表信息 | B | `pg_stat_user_tables.n_tup_ins` | 直接对应 | 整数 | 2分 | — |  |
| 77 | `gaussdb.{#NAME}.number.Of.live.tuples` | ({#NAME})当前活跃的行数 | 表信息 | B | `pg_stat_user_tables.n_live_tup` | 直接对应 | 整数 | 2分 | — |  |
| 78 | `gaussdb.{#NAME}.number.Of.update.tuples` | ({#NAME})更新的行总数 | 表信息 | B | `pg_stat_user_tables.n_tup_upd` | 直接对应 | 整数 | 2分 | — |  |
| 79 | `gaussdb.{#NAME}.number.Of.sequential.scan` | ({#NAME})顺序扫描次数 | 表信息 | B | `pg_stat_user_tables.seq_scan` | 直接对应 | 整数 | 2分 | — |  |
| 80 | `gaussdb.{#NAME}.garbage.Ratio` | ({#NAME})死亡行占比 | 表信息 | B | `pg_stat_user_tables.n_dead_tup/(n_live_tup+n_dead_tup)` | 直接对应 | 浮点 | 2分 | — |  |
| 81 | `gaussdb.DBtsize[{#DATABASE_NAME}]` | [{#DATABASE_NAME}]库大小 | 数据库统计信息 | B | `pg_database_size(datname)` | 直接对应 | 字符 | 2分 | — | 平台另有 rds128 聚合值 |
| 82 | `gaussdb.sendAgentName` | 发送设备名给Agent | Agent | E | `—` | — | 文本 | 5分 | — | Agent 侧行为，不是数据库可查询的监控指标 |
| 83 | `GAUSSDB.Buffers_Alloc` | Buffers Alloc | Bgwriter信息 | B | `pg_stat_bgwriter.buffers_alloc` | 直接对应 | 整数 | 5分 | buffers/s |  |
| 84 | `GAUSSDB.Buffers_Backend` | Buffers Backend | Bgwriter信息 | B | `pg_stat_bgwriter.buffers_backend` | 直接对应 | 整数 | 5分 | buffers/s |  |
| 85 | `GAUSSDB.Buffers_Backend_Fsync` | Buffers Backend Fsync | Bgwriter信息 | B | `pg_stat_bgwriter.buffers_backend_fsync` | 直接对应 | 整数 | 5分 | buffers/s |  |
| 86 | `GAUSSDB.Buffers_Checkpoint` | Buffers Checkpoint | Bgwriter信息 | B | `pg_stat_bgwriter.buffers_checkpoint` | 直接对应 | 整数 | 5分 | buffers/s |  |
| 87 | `GAUSSDB.Buffers_Clean` | Buffers Clean | Bgwriter信息 | B | `pg_stat_bgwriter.buffers_clean` | 直接对应 | 整数 | 5分 | buffers/s |  |
| 88 | `GAUSSDB.Checkpoint_count_eckpoint_gments` | Checkpoint count (by checkpoint segments) | Bgwriter信息 | B | `pg_stat_bgwriter.checkpoints_req` | 直接对应 | 整数 | 5分 | cps | 按需触发（requested）的 checkpoint 次数 |
| 89 | `GAUSSDB.Checkpoint_count_eckpoint_meout` | Checkpoint count (by checkpoint timeout) | Bgwriter信息 | B | `pg_stat_bgwriter.checkpoints_timed` | 直接对应 | 整数 | 5分 | cps | 按超时触发的 checkpoint 次数 |
| 90 | `GAUSSDB.Max_Written_Clean` | Max Written Clean | Bgwriter信息 | B | `pg_stat_bgwriter.maxwritten_clean` | 直接对应 | 整数 | 5分 | buffers/s |  |
| 91 | `GAUSSDB.[{#DATNAME}]_committed_transactions` | [{#DATNAME}] Commited transactions | 数据库信息 | B | `pg_stat_database.xact_commit` | 直接对应 | 整数 | 5分 | tps | 按 datname 取累计值 |
| 92 | `GAUSSDB.[{#DATNAME}]_Active_connections` | [{#DATNAME}] Active connections | 数据库信息 | B | `pg_stat_activity` | 直接对应 | 整数 | 5分 | — | 按 datname 统计活跃会话 |
| 93 | `GAUSSDB.[{#DATNAME}]_rolled_back_transactions` | [{#DATNAME}] Rolled back transactions | 数据库信息 | B | `pg_stat_database.xact_rollback` | 直接对应 | 整数 | 5分 | tps | 按 datname 取累计值 |
| 94 | `GAUSSDB.[{#DATNAME}]_fetch_records_per_second` | [{#DATNAME}]每秒Fetch记录数 | 数据库信息 | B | `pg_stat_database.tup_fetched` | 需换算 | 整数 | 5分 | tup/s | 累计值，每秒速率需按周期差分 |
| 95 | `GAUSSDB.[{#DATNAME}]_insert_records_per_second` | [{#DATNAME}]每秒Insert记录数 | 数据库信息 | B | `pg_stat_database.tup_inserted` | 需换算 | 整数 | 5分 | tup/s | 累计值，每秒速率需按周期差分 |
| 96 | `GAUSSDB.[{#DATNAME}]_return_records_per_second` | [{#DATNAME}]每秒Return记录数 | 数据库信息 | B | `pg_stat_database.tup_returned` | 需换算 | 整数 | 5分 | tup/s | 累计值，每秒速率需按周期差分 |
| 97 | `GAUSSDB.[{#DATNAME}]_update_records_per_second` | [{#DATNAME}]每秒Update记录数 | 数据库信息 | B | `pg_stat_database.tup_updated` | 需换算 | 整数 | 5分 | tup/s | 累计值，每秒速率需按周期差分 |
| 98 | `GAUSSDB.[{#DATNAME}]_cache_hit_ratio` | [{#DATNAME}]缓存命中率 | 数据库信息 | B | `pg_stat_database.blks_hit/(blks_hit+blks_read)` | 直接对应 | 浮点 | 5分 | % |  |
| 99 | `GAUSSDB.[{#DATNAME}]_delete_records_per_second` | [{#DATNAME}]每秒Delete记录数 | 数据库信息 | B | `pg_stat_database.tup_deleted` | 需换算 | 整数 | 5分 | tup/s | 累计值，每秒速率需按周期差分 |
| 100 | `GAUSSDB.session_usage` | 连接数使用率 | 会话信息 | B | `pg_stat_activity.state` | 近似对应 | 浮点 | 5分 | % | 在线会话数 / 总连接数 |
| 101 | `GAUSSDB.sessions_active_top10` | 会话时间TOP10 | 会话信息 | B | `pg_stat_activity.query_start` | 直接对应 | 文本 | 5分 | — | 按 query_start 排序取 Top N |

### 9.2 实测留档索引

| 目录 | 内容 | 用途 |
|---|---|---|
| `data/metric_verification/` | 98 个指标逐个真实调用的请求/响应/结果（+ `_summary.json`） | **回归测试主样本** |
| `data/command_center/results/` | 197 份命令中心请求/响应留档（含 `03_command_list_raw/` 分页原始数据） | 三跳解析回归 |
| `data/command_center/API速查.md` | 由留档自动生成的接口速查 | 查阅 |
| `data/alarm/` | 告警抓取产物 + 288 条告警定义目录 | 告警解析回归 |
| `data/customer_metric_mapping.json` | 101 指标可提供性判定 | 映射基准 |
| `data/customer_metric_query_plan.json` | 101 指标逐条查询方案与真实响应 | 取数方案基准 |
| `data/customer_metric_unsupported.csv` | 3 个不可提供指标 | 交付清单 |
| `data/tpops_api_catalog.json` | TPOPS API 目录 | 接口清单 |
| `data/instances.json` | 实例元数据 | D 类回归 |
| `data/api_probe/` | 单接口探针留档（`GET /rds/health-status` 三次探针） | 健康检查接口契约 |
| `data/multi_master_probe/` | 多 master 探针（Combined 实例 3 个 master 的下发与逐节点快照对比） | master 选择与跨节点汇总 |
| `docs/api/` | CHM 转换的接口原文 | 契约参考 |

**复核命令**

```bash
# 逐指标回归（需要 TPOPS 环境变量）
python tools/verify_metric_sources.py

# 命令中心链路实测
python data/command_center/test_command_center_api.py

# 告警抓取
python tools/fetch_tpops_alarms.py
```

### 9.3 术语表

| 术语 | 含义 |
|---|---|
| **TPOPS** | 华为云 GaussDB 监控平台，本项目的数据源 |
| **mini-token** | TPOPS 的会话令牌，从 `POST /rds/v3/auth/mini-token` 的**响应头**取 |
| **收敛列表** | 告警记录的一种形态，每行 = 实例 + alarm_id |
| **明细行** | 告警记录的一种形态，每行 = 一个节点，含原因与处置步骤 |
| **统一命令中心** | TPOPS 的异步任务执行器，平台提供执行器、我方提供 SQL |
| **cmd_id** | 命令中心里某条内置命令的 ID；`general_single_sql_query` = `73722acc-bff6-4d04-adc0-bc1e1fa1eb7d` |
| **合并 SQL** | 把多个客户指标所需的字段合成一条宽 SQL，减少往返次数 |
| **差分基线** | 上一次的累计计数与采集时刻，用于算速率 |
| **水位（watermark）** | 每个指标/告警最后成功投递的时刻，用于续传 |
| **幂等键** | 消息的唯一标识，用于去重（指标与告警各一套） |
| **档位** | 采集周期档（30 秒/60 秒/1 分/2 分/5 分/1 时/24 时） |
| **占位符** | 客户指标名里的 `{#NAME}` / `{#DBNAME}` / `{#DATABASE_NAME}` / `{#PORT}` |
| **匹配等级** | 直接对应(79) / 近似对应(10) / 需换算(6) / 可统计(3) |
| **通道** | 取数方法分类：A 监控指标 / B 命令中心 / C 诊断优化 / D 实例 / E 告警 |

---
