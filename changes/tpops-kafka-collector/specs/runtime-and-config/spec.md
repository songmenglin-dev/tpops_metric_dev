# 能力规格：运行时、配置与可观测性

对应开发方案 §3.5 配置项、§4.6 调度器、§4.9 可观测性、§5.4.5、§5.5、§5.7、§5.9。
覆盖任务 W01、W20、W21。

## ADDED Requirements

### Requirement: 生产代码边界

`tpops_kafka_collector/` SHALL 是**唯一的生产代码**。
生产代码 SHALL NOT 导入仓库根的 `tools.*` 或 `data.*`；
运行时 SHALL NOT 读取仓库根目录下的任何文件。
仓库根的 `tools/` 与 `data/` 仅作参考与离线工具，SHALL NOT 作为生产依赖。

#### Scenario: 依赖方向检查

- **WHEN** 扫描 `tpops_kafka_collector/` 下全部模块的 import
- **THEN** 不存在任何指向 `tools` 或 `data` 的 import 语句

#### Scenario: 运行时读取路径

- **WHEN** 采集端在任意工作目录下启动并完成一轮采集
- **THEN** 它读取的映射表与 SQL 全部来自包内 `tpops_kafka_collector/mapping/data/`，
  不依赖仓库根目录的存在或位置

#### Scenario: 参考与生产不漂移

- **WHEN** 仓库根 `data/mapping/`、`data/merged_sql/` 与包内副本内容不一致
- **THEN** 一致性测试失败，指出不一致的文件

### Requirement: 启动期配置加载与快速失败

采集端 SHALL 在启动时一次性从环境变量加载全部配置，并对必填项做校验；
缺失或非法时 SHALL 在进入主循环之前失败退出，且不发起任何 HTTP 请求。

必填项：`TPOPS_USER`、`TPOPS_PASSWORD`、`KAFKA_BOOTSTRAP_SERVERS`。
其余项取值见开发方案 §5.5。

#### Scenario: 缺少必填配置

- **WHEN** 环境中缺少 `KAFKA_BOOTSTRAP_SERVERS`
- **THEN** 进程在启动阶段以非 0 退出码终止，日志中指明缺失的变量名，且未向 TPOPS 或 Kafka 发起任何请求

#### Scenario: 配置全部合法

- **WHEN** 环境变量齐全且合法
- **THEN** 进程进入调度主循环，所有配置以不可变对象持有，运行期间不再读取环境变量

#### Scenario: 数值配置越界

- **WHEN** `METRIC_WINDOW_HOURS` 小于 1，或 `CMD_CONCURRENCY` 不在 1~10 范围内
- **THEN** 进程在启动阶段失败退出，日志指明越界的变量名、实际值与允许范围

### Requirement: 凭据仅来自环境变量且不入日志

采集端 SHALL 只从环境变量读取 `TPOPS_PASSWORD` 与认证 token；
SHALL NOT 把密码、token 或含凭据的请求头写入日志、消息体或持久化状态。

#### Scenario: 日志输出

- **WHEN** 采集端记录任何一级别日志
- **THEN** 日志文本中不出现密码、token 字面值或 `X-Mini-Token` 的实际值

#### Scenario: 消息体

- **WHEN** 采集端产出指标消息或告警消息
- **THEN** 消息体的任一字段（含 `tags`）不包含凭据

#### Scenario: 持久化状态

- **WHEN** 采集端写出水位文件、差分基线文件或幂等键集合
- **THEN** 文件内容不包含凭据

### Requirement: 优雅退出

采集端 SHALL 在收到 SIGTERM 或 SIGINT 时停止发起新请求、等待在途 HTTP 请求完成
（超时 30 秒）、原子写出水位、flush Kafka 生产者，然后以退出码 0 终止。

#### Scenario: 收到终止信号

- **WHEN** 进程在主循环中收到 SIGTERM
- **THEN** 进程不再发起新的 HTTP 请求，等待在途请求结束，写出完整水位文件，Kafka producer 已 flush，退出码为 0

#### Scenario: 在途请求超时

- **WHEN** 收到 SIGTERM 后仍有 HTTP 请求未完成且超过 30 秒
- **THEN** 进程放弃等待该请求，写出水位并以退出码 0 终止，不挂死

### Requirement: 健康端点

采集端 SHALL 暴露本地健康端点 `/healthz`（存活）与 `/readyz`（就绪）。
`/readyz` SHALL 在 TPOPS 认证不可用时返回 503。

#### Scenario: 依赖正常

- **WHEN** TPOPS 认证成功且进程未收到关闭信号
- **THEN** `/readyz` 返回 200

#### Scenario: 依赖不可用

- **WHEN** 最近一次 TPOPS 认证失败
- **THEN** `/readyz` 返回 503，且不泄露错误细节中的凭据

### Requirement: 每轮结构化汇总日志

采集端 SHALL 每个调度 tick 输出一行结构化汇总日志，
包含 tick 号、耗时、拉取数、转换数、投递数、失败数、丢弃的不可提供指标数、窗口数、告警数。

#### Scenario: 正常一轮

- **WHEN** 一个 tick 内四组任务均完成
- **THEN** 输出一行含上述全部计数的日志，且各计数之和自洽（投递数 + 失败数 ≤ 转换数）

### Requirement: 结构化数据不可变

跨模块传递的数据 SHALL 使用 frozen dataclass 或不可变 NamedTuple，
SHALL NOT 原地修改已创建的 DTO。

#### Scenario: 维度字典的可哈希性

- **WHEN** 一个 `MetricPoint` 被放入集合或作为字典键
- **THEN** 该操作成功（其维度字段以排序后的元组存储，而非可变 dict）

### Requirement: 错误显式处理且不静默吞异常

采集端 SHALL 将捕获到的异常包装成带上下文的自定义异常并记录；
SHALL NOT 使用空的 `except` 块；SHALL NOT 在模块中调用 `print` 记录日志。

#### Scenario: 平台返回结构不符

- **WHEN** 平台的响应体缺少约定字段
- **THEN** 抛出 `ProtocolError`（或对应的更具体子类），异常消息包含出错的接口路径与字段名，且日志记录 ERROR

#### Scenario: 认证失败

- **WHEN** 平台返回 401/403
- **THEN** 抛出 `AuthError`，该错误被判定为不可重试，调用方不得再次发起认证
