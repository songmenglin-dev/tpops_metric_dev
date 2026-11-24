# delivery-and-state

## Purpose

The delivery-and-state capability documents the published behavior for users and maintainers.

## Requirements

### Requirement: 唯一 JSON 序列化出口

采集端 SHALL 只有一个序列化出口负责把内部 DTO 转成 JSON 消息体，
SHALL NOT 允许任何其他模块自行调用 `json.dumps` 产出消息。

**消息字段集以 `data/mapping/itscada_field_map.json` 的 `metric_message` / `alarm_message`
为准**（该文件直接取自客户两份对接方案 §6.2 / §3.2 的字段表）。
指标消息 **7 个字段**、告警消息 **13 个字段**：

| 消息 | 字段 |
|---|---|
| 指标 | `metricName` `metricCode` `resourceName` `value` `unit` `clock` `tags` |
| 告警 | `alarmId` `alarmName` `alarmContent` `priority` `alarmTime` `status` `recoverTime` `resourceName` `metricName` `metricCode` `value` `unit` `tags` |

> 开发方案 §3.1 的另一套信封（`msg_type` / `schema_version` / `instance_id` /
> `metric_name` … 共 14/25 字段）是映射层重构**之前**的设计，**已被客户字段表取代**。
> 本次以 `itscada_field_map.json` 为准，并同步修正开发方案 §3.1。

#### Scenario: 序列化结果

- **WHEN** 一个 `MetricPoint` 被序列化
- **THEN** 产出的 JSON 含 `metricName`、`metricCode`、`resourceName`、`value`、`unit`、`clock`、`tags` 七个字段，无多余字段、无缺失字段

#### Scenario: 告警序列化结果

- **WHEN** 一个 `AlarmRecord` 被序列化
- **THEN** 产出的 JSON 含上表列出的 13 个告警字段

#### Scenario: 编码与转义

- **WHEN** 序列化含中文的消息
- **THEN** 输出为 UTF-8 且不转义非 ASCII 字符

### Requirement: 业务维度的落点

表名、库名、端口这类业务维度 SHALL NOT 进入 `resourceName`；
SHALL 放在 `tags.dimension_values` 里（键为占位符名，值为该行的实际值）。
`tags` 的其余键 SHALL 为 `instance_id`、`instance_name`、`node_id`、
`component_id`、`host_ip`。

#### Scenario: 表级指标

- **WHEN** 某指标含占位符 `{#NAME}`，其来源行的 `relname` 为 `t1`
- **THEN** `tags.dimension_values` 为 `{"NAME": "t1"}`，`resourceName` 中不含 `t1`

#### Scenario: A 类多组件

- **WHEN** 某指标来自监控指标接口的多个组件 series
- **THEN** 每个组件一条消息，`tags.component_id` 为该组件的键（**小写下划线**）

#### Scenario: 无业务维度

- **WHEN** 某指标不含占位符
- **THEN** `tags.dimension_values` 为空对象

### Requirement: 值按客户指标表的数据类型序列化

采集端 SHALL 按客户指标表【数据类型】把值序列化为整数 / 浮点 / 字符 / 文本之一；
浮点 SHALL 保留 2 位小数，比率类 SHALL 保留 2 位小数。

#### Scenario: 整数型

- **WHEN** 某指标的数据类型为「整数」且原始值为字符串 `"12345"`
- **THEN** 输出的 `value` 是 JSON 数字 `12345`，不是字符串

#### Scenario: 文本型

- **WHEN** 某指标的数据类型为「文本」且原始值为 `"V2.0-10.0.0"`
- **THEN** 输出的 `value` 是 JSON 字符串

#### Scenario: 浮点型

- **WHEN** 某指标的数据类型为「浮点」且原始值为 `0.7941176470588235`
- **THEN** 输出的 `value` 保留 2 位小数

### Requirement: 消息字段的缺省约定

采集端 SHALL 对缺失值一律输出 `null`，SHALL NOT 输出 0 或空串；
`unit` 缺省时 SHALL 为 `null`，SHALL NOT 为 `""`；
所有时间 SHALL 为 13 位毫秒时间戳。

#### Scenario: 单位缺省

- **WHEN** 某指标在客户表中没有单位
- **THEN** `unit` 为 `null`，SHALL NOT 为 `""`

#### Scenario: 值缺失

- **WHEN** 某数据点取不到值
- **THEN** `value` 为 `null`，与真实的 0 可区分

#### Scenario: 字段顺序

- **WHEN** 同一消息被序列化两次
- **THEN** 两次输出的字段顺序一致（保证可 diff）

### Requirement: 指标与告警使用不同 Topic

采集端 SHALL 把指标消息投递到 `KAFKA_TOPIC_METRICS`（默认 `gaussdb.metrics`），
把告警消息投递到 `KAFKA_TOPIC_ALARMS`（默认 `gaussdb.alarms`），
SHALL NOT 混投。

#### Scenario: 指标消息

- **WHEN** 投递一条指标消息（`metricName` 存在）
- **THEN** 它出现在指标 Topic 上，消息 Key 为 `instance_id`

#### Scenario: 告警消息

- **WHEN** 投递一条告警消息（`alarmId` 存在）
- **THEN** 它出现在告警 Topic 上，消息 Key 为 `alarm_id`

#### Scenario: Topic 名可覆盖

- **WHEN** 环境变量设置了不同的 Topic 名
- **THEN** 消息投递到该 Topic，代码无需改动

### Requirement: 分区与压缩策略

采集端 SHALL 以 `instance_id` 作为指标消息的分区键、`alarm_id` 作为告警消息的分区键，
分区数由 `KAFKA_TOPIC_PARTITIONS`（默认 4）控制，
压缩算法由 `KAFKA_COMPRESSION`（默认 `lz4`）控制。

#### Scenario: 同实例有序

- **WHEN** 同一实例的多条消息被投递
- **THEN** 它们落到同一分区（同实例的消息严格有序）

#### Scenario: 分区数可覆盖

- **WHEN** 环境变量把分区数设为 8
- **THEN** 生产者按 8 个分区建立，代码无需改动

### Requirement: Kafka 投递参数

投递端 SHALL 使用 PLAINTEXT 连接，`acks=all`，
`enable_idempotence=False`（kafka-python 2.0.2 不支持幂等生产者），
重复投递由采集端幂等键与客户侧去重兜底。

#### Scenario: 投递成功

- **WHEN** broker 确认了全部消息
- **THEN** 生产者返回成功，调用方得以推进水位

### Requirement: 投递失败不推进水位

采集端 SHALL 仅在一批消息**全部投递成功**后才推进水位；
任一消息投递失败 SHALL 抛出 `ProduceError` 且水位保持不变。

#### Scenario: 部分失败

- **WHEN** 一批 100 条消息中有 1 条投递失败
- **THEN** 抛出 `ProduceError`，水位未推进，该批数据下轮重发

#### Scenario: 全部成功

- **WHEN** 一批消息全部投递成功
- **THEN** 水位推进到该批的结束位置

### Requirement: 幂等去重

采集端 SHALL 在内存中维护最近 N 条消息的幂等键集合（LRU，默认 10000 条），
防止同一轮内的重复投递与相邻轮的重投。

#### Scenario: 同一轮重复

- **WHEN** 同一批内两条消息的幂等键相同
- **THEN** 只投递一条

#### Scenario: 相邻轮重投

- **WHEN** 上一轮已投递的幂等键在本轮再次出现
- **THEN** 该消息被跳过，不重复投递

### Requirement: 水位与差分基线的持久化

采集端 SHALL 持久化：指标水位、差分基线、告警水位、幂等键集合，
SHALL 用「写临时文件 + 原子替换」的方式写，SHALL NOT 让进程被杀时写坏文件。

#### Scenario: 写文件中途被杀

- **WHEN** 进程在写水位文件过程中被强杀
- **THEN** 磁盘上的水位文件仍是上一轮的完整内容（不是半截 JSON）

#### Scenario: 重启续传

- **WHEN** 进程重启
- **THEN** 从持久化的水位继续，不重复投递已成功的数据

### Requirement: 调度器的到期驱动与并发组

调度器 SHALL 采用到期驱动（每个任务有自己的下次到期时刻），
SHALL 在一个 tick 内并发执行到期的任务组，
SHALL 在全部完成后汇总并推进水位。

#### Scenario: 到期判定

- **WHEN** 某任务的到期时刻已到而其他任务未到
- **THEN** 只有到期的任务被执行

#### Scenario: 并发执行

- **WHEN** 一个 tick 内有 monitor、alarm、command_center 三个组同时到期
- **THEN** 三组并发执行，单 tick 墙钟接近三组耗时的最大值而非之和

#### Scenario: 单组失败

- **WHEN** 某一组在本轮全部失败
- **THEN** 其余组的结果仍被投递，失败组的数据留待下轮；进程不退出

### Requirement: 单轮性能预算

60 秒档在正常负载下的单轮墙钟 SHALL 不超过 25 秒；
命令中心采用 1 条合并 SQL 时，单次往返 SHALL 在 15 秒内完成。

#### Scenario: 正常负载

- **WHEN** 目标实例为 3 个 master、68 张表的规模
- **THEN** 60 秒档的单轮耗时 P95 ≤ 25 秒，且日志中记录的耗时可核对

#### Scenario: 表量很大时降频

- **WHEN** 实例表数量达到 1 万张（100 页分页）
- **THEN** 表级指标的完整覆盖周期不短于 300 秒（该档周期被显式下调），
  且采集端逐页推进并记录已取到的 offset

### Requirement: 单轮外部调用数与投递成功率可观测

采集端 SHALL 在每轮汇总日志中记录本轮的**外部调用次数**与**投递成功率**
（成功投递数 ÷ 计划投递数）。单轮的外部调用数 SHALL NOT 超过 42 次
（A 类 1 + 命令中心三跳 3 + 诊断 2 + 告警 5 + 实例 1 + 认证 1 + 前置若干），
超出时 SHALL 记录 WARNING。

#### Scenario: 正常一轮

- **WHEN** 一个 tick 内各组任务均完成
- **THEN** 汇总日志含 `calls`（外部调用次数）与 `delivery_rate`（投递成功率）

#### Scenario: 调用数超预算

- **WHEN** 单轮外部调用次数超过 42
- **THEN** 记录 WARNING 日志，提示调用数超出预算

#### Scenario: 投递有失败

- **WHEN** 本轮有消息投递失败
- **THEN** 投递成功率小于 1，日志中 `failed` 计数大于 0

### Requirement: 平台不保存历史快照

快照型指标（命令中心、诊断、实例接口）的历史只能由采集端自己攒。
采集端 SHALL NOT 声称能为快照型指标补历史；SHALL 暴露自身可用性（`/readyz`），
使监控方能发现采集端停机造成的数据空洞。

#### Scenario: 采集端停机

- **WHEN** 采集端停止运行超过一个采集周期
- **THEN** `/readyz` 不再返回 200，监控方据此判断存在空洞
