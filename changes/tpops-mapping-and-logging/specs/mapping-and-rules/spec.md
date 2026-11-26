# 能力规格（增量）：映射层与取值规则引擎

本增量覆盖三条工作流里与映射口径相关的部分：
指标清理（生效口径 101 → 87）、告警统一指标名、表级指标的 `schema_name.table_name` 替换。

## ADDED Requirements

### Requirement: 排除清单

映射层 SHALL 用**独立的数据文件**
**`tpops_kafka_collector/mapping/data/excluded_metrics.json`**（运行期真相源；
仓库根 `data/mapping/excluded_metrics.json` 为同源副本）
记录被排除的客户指标，每条至少含：客户指标码、客户指标中文名、
排除原因（`duplicate` 重复 / `unsupported` 无法提供）、理由说明；
`reason` 为 `duplicate` 时**另需**给出「保留哪一条」。

排除 SHALL 发生在**构建期**（映射表生成器），因此运行期加载到的生效映射表里
**根本不存在**被排除的指标码 —— 运行期 SHALL NOT 存在「遍历到被排除指标再丢弃」的动作，
SHALL NOT 有对应的运行期丢弃日志。

采集端 SHALL NOT 在代码里硬编码任何被排除的指标码；
SHALL NOT 把被排除的指标产出为消息。

#### Scenario: 重复指标被排除

- **WHEN** 排除清单中某条 `reason` 为 `duplicate`
- **THEN** 该指标不产出消息，且生效映射表中只存在清单指明的「保留项」

#### Scenario: 无法提供的指标被排除

- **WHEN** 排除清单中某条 `reason` 为 `unsupported`
- **THEN** 该指标不产出消息，且其理由随交付文档一并给出

#### Scenario: 清单与生效映射表一致

- **WHEN** 启动期校验比对排除清单与生效映射表
- **THEN** 排除清单里的指标码**一个都不在**生效映射表中，校验通过

### Requirement: 生效指标口径为 87 条

生效映射表 `tpops_kafka_collector/mapping/data/metric_rules.json` SHALL 恰好包含 **87** 条规则，
每条 `enabled` 为 `true`，`metric_code` **互不重复**；
`tpops_kafka_collector/mapping/data/customer_metric_mapping.json` SHALL 与之一一对应（同样 87 条）。

采集端 SHALL NOT 存在「同一指标产出多条消息」的规则对
（同一 `resource_granularity` + 同一占位符集合 + 同一来源列的重复规则）。

#### Scenario: 11 条重复表级指标

- **WHEN** 客户指标库中同一表级指标有两条（中文名正常的一条 + 中文名是英文的一条）
- **THEN** 生效映射表只保留中文名正常的一条，英文那条进排除清单

#### Scenario: 3 条无法提供的指标

- **WHEN** 生效映射表被加载
- **THEN** `gaussdb.Fastpath_function_call`、`gaussdb.Disabled_conn`、
  `gaussdb.sendAgentName` 均不在其中

#### Scenario: 规则码唯一

- **WHEN** 启动期校验统计生效规则的 `metric_code`
- **THEN** 87 条规则对应 87 个互不相同的 `metric_code`

#### Scenario: 不可提供指标的丢弃计数

- **WHEN** 某轮采集正常结束
- **THEN** 轮汇总日志中的 `dropped_unsupported` 为 **0**
  （排除已在生效口径之外，运行期不再有「遍历到再丢弃」的动作）

### Requirement: 告警的统一指标名

平台告警定义中，**不挂靠客户指标**的事件类告警（`linked_customer_metric` 为空），
其 `metricCode` SHALL 全部等于**同一个固定值**，`metricName` SHALL 全部等于
**同一个固定中文名**；该固定值 SHALL 由映射数据给出（表驱动），SHALL NOT 写死在代码里。

挂靠了客户指标的告警 SHALL 保留该客户指标的 `metricCode` 与 `metricName`。

告警种类 SHALL 由 `alarmId` / `alarmName` / `alarmContent` 承载，
SHALL NOT 依赖 `metricCode` 区分。

#### Scenario: 事件类告警

- **WHEN** 一条告警的定义 `linked_customer_metric` 为空
- **THEN** 其 `metricCode` 等于统一固定值、`metricName` 等于统一固定中文名

#### Scenario: 挂靠客户指标的告警

- **WHEN** 一条告警的定义 `linked_customer_metric` 非空
- **THEN** 其 `metricCode` / `metricName` 仍为该客户指标的值，不被统一值覆盖

#### Scenario: 统一值可配置

- **WHEN** 映射数据里改写统一固定值
- **THEN** 产出的告警消息随之改变，采集端代码不变

## MODIFIED Requirements

### Requirement: 维度展开

含占位符的指标 SHALL 按维度展开为一行一条消息。
占位符共 **5 种**：`{#NAME}`、`{#DBNAME}`、`{#DATABASE_NAME}`、`{#DATNAME}`、`{#PORT}`。
占位符在 `metricName` / `metricCode` 中原样保留，维度值置于 `tags.dimension_values`；
无占位符时 `tags.dimension_values` 为空对象。

**表级指标的 `{#NAME}` SHALL 由 `schemaname` 与 `relname` 组合填充**，
组合形式为 `schema_name.table_name`（中间为 `.`）；
表级指标的分组 SHALL 同时按 `schemaname` 与 `relname` 进行，
SHALL NOT 只按 `relname` 分组。

#### Scenario: 表级指标展开

- **WHEN** 某表级指标对应的 SQL 返回 77 行且每行有 `schemaname` 与 `relname`
- **THEN** 产出 77 条消息，每条的 `tags.dimension_values` 含 `NAME` = 该行的
  `schemaname.relname`

#### Scenario: 不同 schema 的同名表

- **WHEN** 表级指标的来源行同时包含 `public.t1` 与 `other.t1`
- **THEN** 产出**两条**消息，`NAME` 分别为 `public.t1` 与 `other.t1`，
  且两条消息的 `metricName` 与 `metricCode` **不相同**

#### Scenario: 库级指标展开

- **WHEN** 某库级指标含占位符 `{#DATNAME}`，来源行的 `datname` 为 `postgres`
- **THEN** 该消息的 `tags.dimension_values` 含 `DATNAME` = `postgres`

#### Scenario: 无占位符指标

- **WHEN** 某指标不含占位符
- **THEN** 其 `tags.dimension_values` 为空对象

### Requirement: 不可提供指标的处置

采集端 SHALL 对被**排除清单**排除的客户指标不产出任何消息，
并 SHALL 随交付附书面清单与理由。

本变更加入的 3 条无法提供指标 SHALL 出现在书面清单中并标注「已从生效口径移除」；
由于排除发生在构建期，运行期 SHALL NOT 有「遍历到再丢弃」的动作，
轮汇总日志中的 `dropped_unsupported` SHALL 为 0。

#### Scenario: 被排除的指标不在生效口径

- **WHEN** 运行期加载生效映射表
- **THEN** 排除清单里的指标码一个都不在其中，采集端没有对应的丢弃分支

#### Scenario: 排除清单为空

- **WHEN** 生效映射表内没有可被排除的指标
- **THEN** 轮汇总日志中的 `dropped_unsupported` 为 0

### Requirement: 映射表驱动

采集端 SHALL 从 `tpops_kafka_collector/mapping/data/*.json` 加载全部客户口径（指标规则、字段映射、
SQL 视图清单、告警定义目录、**排除清单**），
SHALL NOT 在代码中硬编码任何客户指标名、列名、单位或维度键。

#### Scenario: 客户口径变更

- **WHEN** 只需修改某个指标的取值口径
- **THEN** 改动只发生在映射表数据文件，采集端代码不变

#### Scenario: 排除某个指标

- **WHEN** 某个指标需要从生效口径中移除
- **THEN** 改动只发生在排除清单数据文件，不改代码

### Requirement: 指标幂等键

指标消息的幂等键 SHALL 为 `instance_id + metric_name + 排序后的 dimensions + timestamp`。

`metric_name` SHALL 是**占位符替换后**的指标名；
表级指标的 `metric_name` 含 `schema_name.table_name`，
`dimensions` 中的 `NAME` 亦为 `schema_name.table_name`。
两条消息只要 `metric_name` 或 `dimensions` 任一不同，幂等键 SHALL 不同。

#### Scenario: 同一指标不同维度

- **WHEN** 同一指标在不同维度上各产出一个值
- **THEN** 两者的幂等键不同（维度参与拼接）

#### Scenario: 同名表不同 schema

- **WHEN** `public.t1` 与 `other.t1` 上同一表级指标各产出一个值
- **THEN** 两者的幂等键不同，两条消息都被投递

#### Scenario: 同一张表同一时刻

- **WHEN** 同一张表的同一指标在同一时刻被采集到两次
- **THEN** 两者幂等键相同，只投递一条
