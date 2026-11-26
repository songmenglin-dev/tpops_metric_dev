# 能力规格（增量）：投递与状态

本增量只覆盖一件事：表级指标维度改为 `schema_name.table_name` 之后，
幂等键与去重口径必须随之修正——否则不同 schema 的同名表会被去重**静默丢弃**。

## MODIFIED Requirements

### Requirement: 幂等去重

采集端 SHALL 在内存中维护最近 N 条消息的幂等键集合（LRU，默认 10000 条），
防止同一轮内的重复投递与相邻轮的重投。

幂等键 SHALL 由**维度值**参与拼接；表级指标的维度值 SHALL 是
`schema_name.table_name` 组合形式，
SHALL NOT 只用表名 —— 否则不同 schema 的同名表会得到相同的键而被去重丢弃。

#### Scenario: 同一轮重复

- **WHEN** 同一批内两条消息的幂等键相同
- **THEN** 只投递一条

#### Scenario: 相邻轮重投

- **WHEN** 上一轮已投递的幂等键在本轮再次出现
- **THEN** 该消息被跳过，不重复投递

#### Scenario: 不同 schema 的同名表

- **WHEN** 同一实例上存在 `public.t1` 与 `other.t1` 两个同名表，
  两者的表级指标在同一轮被采集到
- **THEN** 两条消息的幂等键**不相同**，两条都被投递

#### Scenario: 同一张表重复采集

- **WHEN** 同一张 `public.t1` 的同一指标在相邻两轮被采集到
- **THEN** 幂等键相同，后一条被跳过（原有去重语义不变）

### Requirement: 业务维度的落点

表名、库名、端口这类业务维度 SHALL NOT 进入 `resourceName`；
SHALL 放在 `tags.dimension_values` 里（键为占位符名，值为该行的实际值）。
`tags` 的其余键 SHALL 为 `instance_id`、`instance_name`、`node_id`、
`component_id`、`host_ip`。

表级指标的 `NAME` 维度值 SHALL 为 `schema_name.table_name` 组合形式。

#### Scenario: 表级指标

- **WHEN** 某指标含占位符 `{#NAME}`，其来源行的 `schemaname` 为 `public`、
  `relname` 为 `t1`
- **THEN** `tags.dimension_values` 为 `{"NAME": "public.t1"}`，`resourceName` 中不含 `t1`

#### Scenario: A 类多组件

- **WHEN** 某指标来自监控指标接口的多个组件 series
- **THEN** 每个组件一条消息，`tags.component_id` 为该组件的键（**小写下划线**）

#### Scenario: 无业务维度

- **WHEN** 某指标不含占位符
- **THEN** `tags.dimension_values` 为空对象
