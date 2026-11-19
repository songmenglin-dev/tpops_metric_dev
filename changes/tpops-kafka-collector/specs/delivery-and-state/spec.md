# 能力规格：序列化、Kafka 投递与状态

对应开发方案 §3.1（消息模型）、§3.2（Topic 与分区）、§3.5（维度展开）、
§4.6.4（水位）、§4.7（可靠性）、§4.8（性能预算）、§5.4.5（调度器）。
覆盖任务 W04、W05、W09、W19。

## ADDED Requirements

### Requirement: 唯一 JSON 序列化出口

采集端 SHALL 只有一个序列化出口负责把内部 DTO 转成 JSON 消息体，
SHALL NOT 允许任何其他模块自行调用 `json.dumps` 产出消息。

#### Scenario: 序列化结果

- **WHEN** 一个 `MetricPoint` 被序列化
- **THEN** 产出的 JSON 含 `msg_type`、`schema_version`、`instance_id`、`instance_name`、
  `metric_name`、`metric_name_cn`、`metric_category`、`dimensions`、`value`、`value_type`、
  `unit`、`timestamp`、`collect_time`、`source` 全部字段

#### Scenario: 编码与转义

- **WHEN** 序列化含中文的消息
- **THEN** 输出为 UTF-8 且不转义非 ASCII 字符

### Requirement: 消息字段的缺省约定

采集端 SHALL 对缺失值一律输出 `null`，SHALL NOT 输出 0 或空串；
`dimensions` SHALL 传空字典 `{}`，SHALL NOT 传 `null`；
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

- **WHEN** 投递一条 `msg_type = "metric"` 的消息
- **THEN** 它出现在指标 Topic 上，消息 Key 为 `instance_id`

#### Scenario: 告警消息

- **WHEN** 投递一条 `msg_type = "alarm"` 的消息
- **THEN** 它出现在告警 Topic 上，消息 Key 为 `alarm_id`

#### Scenario: Topic 名可覆盖

- **WHEN** 环境变量设置了不同的 Topic 名
- **THEN** 消息投递到该 Topic，代码无需改动

### Requirement: Kafka 投递参数

投递端 SHALL 使用 PLAINTEXT 连接，`acks=all`，
`enable_idempotence=False`，压缩算法由 `KAFKA_COMPRESSION`（默认 `lz4`）控制。

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

### Requirement: 平台不保存历史快照

快照型指标（命令中心、诊断、实例接口）的历史只能由采集端自己攒。
采集端 SHALL NOT 声称能为快照型指标补历史；SHALL 暴露自身可用性（`/readyz`），
使监控方能发现采集端停机造成的数据空洞。

#### Scenario: 采集端停机

- **WHEN** 采集端停止运行超过一个采集周期
- **THEN** `/readyz` 不再返回 200，监控方据此判断存在空洞
