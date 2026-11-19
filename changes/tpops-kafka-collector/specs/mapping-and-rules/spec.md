# 能力规格：映射层与取值规则引擎

对应开发方案 §3.4（取值口径）、§3.5（维度展开）、§3.6（不可提供指标）、
§4.5（规则引擎）、§5.6（映射表格式）、§5.6.4（启动期校验），
以及 `docs/设计-合并SQL映射基准改造.md`。
覆盖任务 W00、W07、W08、W13、W18。

## ADDED Requirements

### Requirement: 映射表驱动

采集端 SHALL 从 `mapping/data/*.json` 加载全部客户口径（指标规则、字段映射、
SQL 视图清单、告警定义目录、不可提供指标清单），
SHALL NOT 在代码中硬编码任何客户指标名、列名、单位或维度键。

#### Scenario: 客户口径变更

- **WHEN** 只需修改某个指标的取值口径
- **THEN** 改动只发生在映射表数据文件，采集端代码不变

### Requirement: 启动期映射校验

采集端 SHALL 在启动时校验：每个规则的来源列存在于对应 SQL 的**实际返回列**中、
每个可提供指标都在映射表中、每个映射表条目都对应一个真实可提供指标、
占位符与维度绑定一一对应。
任一项不通过 SHALL 启动失败并打印差异。

#### Scenario: 列名与实际不符

- **WHEN** 某条规则引用了实际返回中不存在的列
- **THEN** 启动失败，错误信息列出该规则名、期望的列名与实际可用列名

#### Scenario: 全部通过

- **WHEN** 全部校验项通过
- **THEN** 进程正常进入主循环

### Requirement: 合并 SQL 返回形态的解码

映射层 SHALL 能解码「单条合并 SQL」的返回形态：
`result` 经 `json.loads` 后为**一行**，该行的每个键是一个视图名，
其值是该视图的 JSON 字符串。
每个视图的列 SHALL 由该列声明的 `json_column` 指定；
SHALL NOT 把整个单行对象当作行列表返回。

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

#### Scenario: 两页拼接

- **WHEN** 传入两页返回体，其中可增长视图第一页有 68 行、第二页有 32 行
- **THEN** 该视图拼接后为 100 行；固定视图仍为第一页的行数

#### Scenario: 单页

- **WHEN** 只传入一页
- **THEN** 拼接结果等于该页各视图的行数

### Requirement: 取值规则的六类转换

规则引擎 SHALL 支持并区分：直接取值 `raw`、聚合 `aggregate`、比率 `ratio`、
差分 `counter_delta`、派生 `gauge`、枚举映射 `enum_map`（另含 `top_n` 用于会话 Top10）。
`raw` 与 `counter_delta` SHALL 显式区分，不得混用。

#### Scenario: 直接取值

- **WHEN** 规则类型为 `raw` 且 `row_policy` 为 `sum`
- **THEN** 结果为该列在所有行上的求和

#### Scenario: 比率

- **WHEN** 规则类型为 `ratio` 且分母为 0
- **THEN** 结果为 0，SHALL NOT 抛出除零异常

#### Scenario: 枚举未命中

- **WHEN** `enum_map` 中查不到平台枚举值
- **THEN** 原样透传该值，并记录一条告警级日志

### Requirement: 缺值不产出假数据

当某个数据点取不到值时，采集端 SHALL 产出 `value = null` 或**不产出该条消息**，
SHALL NOT 用 0 代替缺失值。

#### Scenario: 该点为 null

- **WHEN** 平台返回的数据点值本身为 null
- **THEN** 该点不产出消息（不填 0）

#### Scenario: 整列缺失

- **WHEN** 行里没有该列
- **THEN** 值为 `null`，且与「取到 0」在输出上可区分

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

含占位符（`{#NAME}` / `{#DBNAME}` / `{#DATABASE_NAME}` / `{#PORT}`）的指标
SHALL 按维度展开为一行一条消息，占位符在指标名中原样保留，
维度值另置于 `dimensions`；无占位符时 `dimensions` 为 `{}`。

#### Scenario: 表级指标展开

- **WHEN** 某表级指标对应的 SQL 返回 77 行且每行有 `relname`
- **THEN** 产出 77 条消息，每条的 `dimensions` 含 `NAME` = 该行的 `relname`

#### Scenario: 无占位符指标

- **WHEN** 某指标不含占位符
- **THEN** 其 `dimensions` 为空字典 `{}`，SHALL NOT 为 `null`

### Requirement: 不可提供指标的处置

采集端 SHALL 对 3 个不可提供指标不产出任何消息，
SHALL 每轮输出丢弃计数，并 SHALL 随交付附书面清单与理由。

#### Scenario: 不可提供指标出现在映射表中

- **WHEN** 采集端遍历到 `gaussdb.Fastpath_function_call` / `gaussdb.Disabled_conn` /
  `gaussdb.sendAgentName` 之一
- **THEN** 不产出消息，丢弃计数递增，日志记录被丢弃的指标名

### Requirement: 告警幂等键包含状态

告警幂等键 SHALL 为 `(alarm_id, instance_id, first_alarm_time, status)`。
SHALL NOT 使用 `node_id`（收敛条目的节点是回填得来，可能每轮不同），
SHALL NOT 使用 `last_alarm_time`（恢复后它不变，无法区分恢复事件）。

#### Scenario: 同一条告警的触发与恢复

- **WHEN** 同一告警先以 `status = activated` 被采集到，后以 `status = recovered` 被采集到
- **THEN** 两次的幂等键不同，恢复事件不会被去重丢弃

#### Scenario: 重复触发

- **WHEN** 同一条告警在多轮中被重复采集到且 `status` 不变
- **THEN** 幂等键相同，只产出一条消息

### Requirement: 指标幂等键

指标消息的幂等键 SHALL 为 `instance_id + metric_name + 排序后的 dimensions + timestamp`。

（基线：开发方案 §4.5.4 / §5.3 `MetricPoint.idempotency_key`；本条不改变口径，
在此登记以与告警侧并列。）

#### Scenario: 同一指标不同维度

- **WHEN** 同一指标在不同维度上各产出一个值
- **THEN** 两者的幂等键不同（维度参与拼接）

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

