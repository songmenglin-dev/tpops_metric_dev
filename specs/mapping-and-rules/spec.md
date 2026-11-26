# mapping-and-rules

## Purpose

The mapping-and-rules capability documents the published behavior for users and maintainers.

## Requirements

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

### Requirement: 启动期映射校验

采集端 SHALL 在启动时校验：每个规则的来源列存在于对应 SQL 的**实际返回列**中、
每个可提供指标都在映射表中、每个映射表条目都对应一个真实可提供指标、
占位符与维度绑定一一对应。
任一项不通过 SHALL 启动失败并打印差异。

**来源列的权威来源**：`data/metric_response/`（98 份逐指标留档，含旧 11 条形态的实际返回列）
与 `data/merged_sql/sql_merge_probe_result.json`（1 条合并形态的实际返回列）。
两者冲突时以**实测留档**为准（留档是事实，映射表是声明）。
开发方案 §5.6.4 原写的 `data/command_center/results/` 已于 2026-11-13 删除，不得再引用。

#### Scenario: 列名与实际不符

- **WHEN** 某条规则引用了实际返回中不存在的列
- **THEN** 启动失败，错误信息列出该规则名、期望的列名与实际可用列名

#### Scenario: 全部通过

- **WHEN** 全部校验项通过
- **THEN** 进程正常进入主循环

### Requirement: 映射数据在包内与仓库保持一致

包内的 `tpops_kafka_collector/mapping/data/*.json` SHALL 与仓库的 `data/mapping/`、`data/merged_sql/`
内容一致；采集端 SHALL 从包内读取映射数据。

#### Scenario: 仓库侧改动未同步到包内

- **WHEN** 仓库的 `data/mapping/` 某文件被改动而包内副本未更新
- **THEN** 一致性测试失败，指出不一致的文件

#### Scenario: 一致时

- **WHEN** 两侧内容一致
- **THEN** 采集端从包内读取到的映射表与仓库侧等价

### Requirement: 合并 SQL 返回形态的解码

映射层 SHALL 能解码「单条合并 SQL」的返回形态：
`result` 经 `json.loads` 后为**一行**，该行的每个键是一个视图名，
其值是该视图的 JSON 字符串。
每个视图的列 SHALL 由该列声明的 `json_column` 指定；
SHALL NOT 把整个单行对象当作行列表返回。

**为什么必须显式处理（实现者必读）**——把合并形态当行列表用会**静默产出 0**：

```
result = '[{"stat_database":"[{\"datname\":\"lz\",\"xact_commit\":153291,...}]",
            "locks":"[...]", ...}]'
  ↓ 旧的 _rows_from_result：json.loads 之后直接当行列表
ctx.rows = [ {"stat_database": "[{...}]", "locks": "[{...}]", ...} ]
           ↑ 1 行、11 个「整段 JSON 字符串」列 —— 不是视图的行
  ↓ 规则引擎 row.get("deadlocks") → None
sum / raw 规则 → _to_number(None) → 0
```

「列不存在」与「列的值是 0」在数值转换眼里是一回事 —— 82 个 B 类指标会**全部为 0 且不报错**，
这正是本需求要消除的失败模式。

#### Scenario: 解出视图行

- **WHEN** `result` 为 `[{"stat_database":"[{...}]","locks":"[{...}]"}]`
- **THEN** `stat_database` 视图解码为其中 JSON 字符串所表示的行列表，`locks` 同理

#### Scenario: 空视图

- **WHEN** 某个视图的列值为空字符串 `''` 或 `null`（`json_agg` 对零行返回 NULL，平台渲染成空串）
- **THEN** 该视图解码为空列表 `[]`，SHALL NOT 抛出 JSON 解析异常

#### Scenario: 列值不是合法 JSON

- **WHEN** 某个视图的列值是坏 JSON
- **THEN** 抛出 `DecodeError`，SHALL NOT 静默返回空列表或 0

#### Scenario: result 不是单行

- **WHEN** `result` 解码后不是长度恰为 1 的列表
- **THEN** 抛出 `DecodeError`，指明期望 1 行、实际几行

### Requirement: 解码错误的异常归属

`DecodeError` SHALL 归属到平台层异常体系（继承 `MappingError`），
SHALL 被判定为**不可重试**（响应结构与预期不符需要人工介入，不是网络抖动）。

#### Scenario: 解码失败被判定不可重试

- **WHEN** 某次采集抛出 `DecodeError`
- **THEN** 重试框架不发起重试，该错误计入本轮失败数并记 ERROR 日志

### Requirement: 既有 per-view 形态保持可解

映射层 SHALL 保留既有的「`result` 即行列表」解码路径，SHALL NOT 改变其行为。
既有 per-view 样本的解码结果 SHALL 与改造前一致。

#### Scenario: 旧样本回放

- **WHEN** 用 `data/metric_response/` 下的既有样本解码
- **THEN** 解码结果与改造前逐项相同，既有回归测试全部通过

### Requirement: 分页结果的拼接

映射层 SHALL 提供把多个分页返回体按视图拼接的接口：
仅对声明为可增长（`paged = true`）的视图做跨页行拼接，
固定视图 SHALL 只取第一页（不重复累加）。
`merged_views` 中未显式声明 `paged` 的视图 SHALL 视为 `paged = false`
（`merged_sql.json` 的 `paging.views` 只列了 4 个视图，其余 7 个按此缺省）。

#### Scenario: 两页拼接

- **WHEN** 传入两页返回体，其中可增长视图第一页有 68 行、第二页有 32 行
- **THEN** 该视图拼接后为 100 行；固定视图仍为第一页的行数

#### Scenario: 单页

- **WHEN** 只传入一页
- **THEN** 拼接结果等于该页各视图的行数

### Requirement: 取值规则的类型集

规则引擎 SHALL 支持映射表在用的全部 13 种 `value_rule.type`：
`raw`、`raw_filtered`、`aggregate`、`counter_delta`、`ratio_across`、`series_point`、
`enum_map`、`gauge`、`top_n`、`raw_rows`、`collection_count`、`text_prefix_count`、`instance_field`。
SHALL NOT 引入映射表未使用的规则类型；SHALL NOT 把 `ratio_across` 简化为 `ratio`
（跨视图比率的分子分母来自不同视图）。

#### Scenario: 直接取值

- **WHEN** 规则类型为 `raw` 且 `row_policy` 为 `sum`
- **THEN** 结果为该列在所有行上的求和

#### Scenario: 跨视图比率

- **WHEN** 规则类型为 `ratio_across`，分子取自 `activity_agg`、分母取自 `stat_database`，
  且上下文已提供两个视图的行
- **THEN** 结果为分子除以分母；分母为 0 时结果为 0，SHALL NOT 抛出除零异常

#### Scenario: 枚举未命中

- **WHEN** `enum_map` 中查不到平台枚举值
- **THEN** 原样透传该值，并记录一条告警级日志

### Requirement: 缺值不产出假数据

当某个数据点取不到值时，采集端 SHALL **不产出该条消息**（不进入投递批次），
SHALL NOT 用 0 代替缺失值。
`value = null` 只用于**消息内部字段**（如缺省的 `unit`），不用于「取不到值」。

#### Scenario: 该点为 null

- **WHEN** 平台返回的数据点值本身为 null
- **THEN** 该点不产出消息（不填 0）

#### Scenario: 整列缺失

- **WHEN** 行里没有该列
- **THEN** 该指标不产出消息，且与「取到 0」在输出上可区分（后者产出 value=0 的消息）

### Requirement: 累计计数的跨轮差分

采集端 SHALL 对累计计数类指标做跨轮差分换算为速率，
时间差 SHALL 用实际采集时刻之差（而非标称周期）。
第一轮无基线时 SHALL 只记基线、不产出速率值。

#### Scenario: 首轮无基线

- **WHEN** 某累计指标的差分基线不存在
- **THEN** 本轮只记录基线，产出值为 `null`，不产出速率

#### Scenario: 正常差分

- **WHEN** 基线存在且本次累计值大于基线值
- **THEN** 速率为 `(now - last) / Δt`，其中 Δt 为两次采集时刻之差

### Requirement: 实例重启归零的处理

当本次累计值小于上次累计值时，采集端 SHALL 判定为实例重启，
SHALL 丢弃本轮差分（值为 `null`）、SHALL 记录 ERROR 日志、SHALL 重建基线。

#### Scenario: 计数归零

- **WHEN** 本次累计值小于基线值
- **THEN** 本轮产出值为 `null`，日志为 ERROR 且包含重启判定信息，基线被重置为本次值
- **AND** SHALL NOT 产出负速率或假峰值

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

### Requirement: 多 master 结果的归集口径

命令中心的合并 SQL 会在**每个 master 节点各执行一次**，返回同一组视图的多份结果。
归集口径 SHALL 由映射表的 `node_scoped` 字段决定（该字段由实测判定：
各 master 结果不同 → `true`，相同 → `false`）：

| `node_scoped` | 归集口径 | 产出条数 |
|---|---|---|
| `true`（`stat_database`、`activity_agg`、`activity_running`、`activity_top10`、`dbstat_sum`） | **各 master 的结果各自成一份数据**，SHALL NOT 跨 master 合并 | 每 master 一份 |
| `false`（`stat_user_tables`、`database`、`locks`、`bgwriter`、`settings`、`prepared_xacts`） | **只取一个 master 的结果**（固定为第一个 master CN），SHALL NOT 重复产出 | 一份 |

SHALL NOT 把多个 master 的行合并后求和 —— 实测同一实例三个 master 的
`stat_database` 首行 `xact_commit` 分别为 153291 / 153299 / 146452（各不相同），
跨 master 求和会得到无业务含义的 3 倍累计值。

#### Scenario: 节点级视图

- **WHEN** 某指标来自 `stat_database`（`node_scoped = true`），三个 master 返回不同的行
- **THEN** 产出 3 组数据，每组对应一个 master；各组内部的行按 `row_policy` 求和，SHALL NOT 跨 master 相加

#### Scenario: 实例级视图

- **WHEN** 某指标来自 `stat_user_tables`（`node_scoped = false`），三个 master 返回相同的行
- **THEN** 只产出 1 份数据，取自第一个 master；SHALL NOT 产出 3 份重复

#### Scenario: 取 master 的顺序稳定

- **WHEN** 连续多轮采集同一实例
- **THEN** 被选中的那个 master 在各轮之间不变（按节点清单顺序取第一个 master CN）

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

### Requirement: 告警幂等键包含状态

告警幂等键 SHALL 为 `(alarm_id, instance_id, first_alarm_time, status)`。
SHALL NOT 使用 `node_id`（收敛条目的节点是回填得来，可能每轮不同），
SHALL NOT 使用 `last_alarm_time`（恢复后它不变，无法区分恢复事件）。

幂等键 SHALL 在**采集端的 DTO 上计算**（`AlarmRecord.idempotency_key`），
SHALL NOT 由生成器写死成字符串常量。
键的声明 SHALL 同时写入 `data/mapping/alarm_rules.json` 的 `idempotency` 段，
且开发方案 §3.7 与 §4.5.4 SHALL 同步为同一口径 —— 二者是同一契约的两个副本。

#### Scenario: 同一条告警的触发与恢复

- **WHEN** 同一告警先以 `status = activated` 被采集到，后以 `status = recovered` 被采集到
- **THEN** 两次的幂等键不同，恢复事件不会被去重丢弃

#### Scenario: 重复触发

- **WHEN** 同一条告警在多轮中被重复采集到且 `status` 不变
- **THEN** 幂等键相同，只产出一条消息

#### Scenario: 声明与实现一致

- **WHEN** 对比 `alarm_rules.json` 的 `idempotency` 段与 `AlarmRecord.idempotency_key` 的实现
- **THEN** 两者列出的字段相同，且与开发方案 §3.7 / §4.5.4 写的键相同

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

### Requirement: 告警消息的投递单位

告警消息 SHALL 以**收敛条目**为主条目（一条消息对应「实例 + alarm_id」），
明细行 SHALL 归并进 `details` 数组。
`details` 缺失时 SHALL 输出空数组 `[]`，SHALL NOT 省略该字段。

（基线：开发方案 §3.1.2；本条不改变口径，仅登记投递单位，
以便与「告警幂等键包含状态」一条对齐。）

#### Scenario: 一条告警命中多个节点

- **WHEN** 同一 alarm_id + instance_id 在 3 个节点上有明细行
- **THEN** 产出 1 条消息，其 `details` 数组含 3 项

#### Scenario: 无明细行

- **WHEN** 某收敛条目没有对应的明细行
- **THEN** 消息仍产出，`details` 为 `[]`

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

#### Scenario: 告警种类的唯一性

- **WHEN** 平台目录里有 254 条事件类告警
- **THEN** 它们的 `alarmId` **互不相同**；`alarmName` 逐条对得上平台目录
  （平台目录**不保证** `alarmName` 唯一 —— 有 13 行填的是等级词、另有同名不同 ID 的组，
  实测只有 241 个不同名）；告警种类 SHALL 由 `alarmId` / `alarmName` / `alarmContent`
  承载，SHALL NOT 依赖 `metricCode` 区分

#### Scenario: 统一值可配置

- **WHEN** 映射数据里改写统一固定值
- **THEN** 产出的告警消息随之改变，采集端代码不变
