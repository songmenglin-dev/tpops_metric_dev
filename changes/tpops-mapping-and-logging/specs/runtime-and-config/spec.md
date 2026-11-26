# 能力规格（增量）：运行时、配置与日志框架

本增量覆盖日志优化工作流：日志框架规范与格式、每通道每轮汇总、
日志落盘与总日志/通道日志分工、`collector.sh run` 后台运行。

## ADDED Requirements

### Requirement: 日志目录与文件布局

采集端 SHALL 把日志写入**可配置的日志目录**（环境变量 `LOG_DIR`），
目录下的文件布局 SHALL 为 **6 个基名、每个基名按天一个文件、文件名带日期后缀**：

```
<LOG_DIR>/collector-2026-11-26.log                  # 总日志
<LOG_DIR>/collector-monitor-2026-11-26.log          # A 类：监控指标通道
<LOG_DIR>/collector-command_center-2026-11-26.log   # B 类：命令中心通道
<LOG_DIR>/collector-diagnostic-2026-11-26.log       # C 类：诊断优化通道
<LOG_DIR>/collector-instance-2026-11-26.log         # D 类：实例元数据通道
<LOG_DIR>/collector-alarm-2026-11-26.log            # E 类：告警通道
```

命名规则 SHALL 为 `<基名>-<YYYY-MM-DD>.log`；同一天内超出单日上限时，
旧文件 SHALL 被改名为 `<基名>-<YYYY-MM-DD>.<日期>_<时分秒_微秒>.log`，
并新建同名文件继续写。

日志目录不存在时 SHALL 自动创建；不可写时 SHALL **启动失败**并给出明确错误
（SHALL NOT 静默降级为「只写 stderr」）。

`LOG_DIR` **未设置**时，采集端 SHALL 只写 stderr（不落盘）——这是开发态的既有行为；
`deploy/collector.sh run` SHALL **总是**显式设置 `LOG_DIR`（默认 `<安装目录>/logs`），
因此生产态一定落盘。`<安装目录>` = **`collector.sh` 所在目录**（脚本里的 `$HERE`）。

通道文件的**基名** SHALL 由通道名（`monitor` / `command_center` / `diagnostic` /
`instance` / `alarm`）确定，SHALL NOT 由指标名或实例名派生
（避免文件名数量随指标/实例膨胀）。

#### Scenario: 首次启动

- **WHEN** `LOG_DIR` 指向的目录不存在
- **THEN** 自动创建该目录，随后正常写入日志文件

#### Scenario: 目录不可写

- **WHEN** `LOG_DIR` 存在但当前用户无写权限
- **THEN** 进程启动失败，错误信息说明日志目录路径与不可写原因

#### Scenario: 文件名带日期

- **WHEN** 采集端在 2026-11-26 写日志
- **THEN** 产生 `collector-2026-11-26.log` 与 5 个
  `collector-<channel>-2026-11-26.log`

#### Scenario: 基名稳定

- **WHEN** 采集端在跑 87 个指标、多个实例
- **THEN** 日志目录下的**基名**仍然只有 6 个（总日志 + 5 个通道）

### Requirement: 总日志与通道日志的分工

采集端 SHALL 维护两类日志：

- **总日志**（`collector.log`）：生命周期（启动 / 停止 / 装配结果 / 致命错误）、
  每个 tick 的**整体**汇总、以及**每个通道一行**的简洁汇总
  （即总日志要能一眼看出各采集器这一轮的采集与推送情况）；
- **通道日志**（`collector-<channel>.log`）：该通道内部的采集与推送明细，
  以及该通道每轮结束时的**通道汇总行**。

通道日志 SHALL NOT 把其它通道的明细写入；总日志 SHALL NOT 出现通道内部明细。

logger 命名 SHALL 分层：`collector.lifecycle`（生命周期）、
`collector.summary`（轮汇总与通道汇总）、`collector.<channel>`（通道明细）。
通道 logger SHALL NOT 向总日志重复输出 INFO 级明细；
其 WARNING 及以上级别 SHALL 同时出现在总日志与该通道日志。

#### Scenario: 一轮正常结束

- **WHEN** 一个 tick 内 3 个通道到期并完成采集
- **THEN** 总日志里有 1 行整体汇总 + 3 行通道汇总；
  每个通道日志里有 1 行该通道的汇总

#### Scenario: 通道内部报错

- **WHEN** 某通道采集阶段抛异常并被记为 ERROR
- **THEN** 该 ERROR 同时出现在该通道日志与总日志中

#### Scenario: 通道日志不串味

- **WHEN** 通道 `monitor` 完成一轮
- **THEN** `collector-command_center.log` 中**没有**本轮 monitor 的记录

### Requirement: 每通道每轮汇总行

每类采集器**完成一轮**后 SHALL 输出一条属于该通道的汇总行，
至少包含下列字段（字段名固定，SHALL NOT 自创）：

| 字段 | 含义 |
|---|---|
| `channel` | 通道名（`monitor` / `command_center` / `diagnostic` / `instance` / `alarm`） |
| `tick` | 轮次序号 |
| `instances` | 本轮该通道参与的实例数 |
| `fetched` | 本轮该通道**获取的数据量**（从平台取回的原始行数 / 告警条数） |
| `mapped` | 转换后尝试投递的消息数（已滤掉幂等重复） |
| `produced` | **已推送数据量**（成功投递的消息数） |
| `failed` | 投递失败的消息数 |
| `windows` | 本轮该通道执行的 SQL 条数 |
| `calls` | 本轮该通道的外部调用次数 |
| `elapsed` | 本轮该通道的耗时（秒） |
| `delivery_rate` | 投递成功率（`produced / mapped`，无消息时为 1.000） |

未到期的通道 SHALL NOT 输出汇总行。

#### Scenario: 通道到期

- **WHEN** 通道 `command_center` 在 tick 12 到期并完成
- **THEN** 输出一行含上述全部字段、且 `channel=command_center` 的汇总行

#### Scenario: 通道未到期

- **WHEN** 通道 `diagnostic` 本轮未到期
- **THEN** 本轮**没有** `channel=diagnostic` 的汇总行

#### Scenario: 计数自洽

- **WHEN** 某通道本轮 `produced` 为 10、`failed` 为 2
- **THEN** `mapped` ≥ 12，且 `delivery_rate` = 10 / mapped

### Requirement: 日志框架为 loguru

采集端 SHALL 使用 **loguru** 作为日志框架（版本锁定 `loguru==0.8.0`），
SHALL NOT 自研日志 handler / 轮转 / 保留 / 格式化实现。

采集端 SHALL 全面使用 loguru 的 API（`from loguru import logger`）；
第三方库（`kafka` / `urllib3` / `requests`）的 stdlib 日志
SHALL 经 `InterceptHandler` 转发进 loguru。

loguru SHALL 满足本项目的部署红线：`requires_python >= 3.5`（Python 3.7.9 可用）、
发行包为纯 Python（`py3-none-any`，x86_64 与 aarch64 共用同一份部署包）、
在 Linux + Python 3.7 下**无运行期传递依赖**。

#### Scenario: 依赖可用

- **WHEN** 目标机只有 requests / kafka-python / urllib3 与 loguru 四个纯 Python 包
- **THEN** 采集端正常启动并写日志，不需要编译、不需要联网

#### Scenario: 第三方库日志

- **WHEN** `kafka` 客户端输出 WARNING 级日志
- **THEN** 该记录经 `InterceptHandler` 进入 loguru 的 sink，格式与其它日志一致

### Requirement: 日志格式

采集端 SHALL 支持 `text` 与 `json` 两种格式（由 `LOG_FORMAT` 选择）。

**text 模式 SHALL 直接使用 loguru 的原生格式**（含时间、级别、模块名:行号、消息），
SHALL NOT 自研 logback pattern 解析器；格式 SHALL 可用 `LOG_FORMAT` 覆盖
（loguru 原生字符串语法，如 `{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name} | {message}`）。

**json 模式**：每行一个 JSON 对象，含 `timestamp` / `level` / `logger` / `thread` /
`message` 字段；当整条消息是 `k=v` 形式时，那些键值 SHALL **额外成为顶层字段**
（客户侧日志平台要能直接按 `channel` / `fetched` / `produced` 等字段取值，
SHALL NOT 只能从一段 `message` 字符串里解析）。

异常路径 SHALL 带 `exception` 字段（堆栈不丢）；text 模式下堆栈 SHALL 追加在消息之后。

#### Scenario: text 格式

- **WHEN** `LOG_FORMAT=text` 且未配置 `LOG_FORMAT` 覆盖值
- **THEN** 每行含时间、级别、模块名与消息，格式为 loguru 原生形态

#### Scenario: 自定义格式

- **WHEN** 配置 `LOG_FORMAT={time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name} | {message}`
- **THEN** 输出按该 loguru 原生语法渲染

#### Scenario: json 格式

- **WHEN** `LOG_FORMAT=json`
- **THEN** 每行是一个可被 `json.loads` 解析的对象，
  且通道汇总行里的 `channel` / `fetched` / `produced` 是**顶层字段**

#### Scenario: 异常记录

- **WHEN** 记录日志时带有异常
- **THEN** json 输出含 `exception` 字段且含完整堆栈；text 输出在消息后追加堆栈

### Requirement: 日志切分与保留

采集端 SHALL 用 loguru 的 `rotation` / `retention` 实现切分与保留，
文件名 SHALL 带日期后缀。

- 按天切分：文件名用 loguru 的 `{time:YYYY-MM-DD}`，跨天即换新文件；
- 单日文件大小上限由 `LOG_MAX_BYTES` 控制（默认 **50 MB**）——
  同一天内超出时 SHALL 切分（旧文件被改名，新文件续写）；
- 保留天数由 `LOG_RETENTION_DAYS` 控制（默认 **3**）——
  保留**最近 3 个自然日（含当天）**，日期更早的文件 SHALL 删除；
- 被改名产生的同日分片 SHALL 同样纳入保留期清理；
- 切分与清理 SHALL 对总日志与全部通道日志**一致生效**。

#### Scenario: 跨天切分

- **WHEN** 进程跨过午夜继续写日志
- **THEN** 关闭当天的文件，打开 `<基名>-<新日期>.log`

#### Scenario: 单日超限

- **WHEN** 某个当天的日志文件写满 `LOG_MAX_BYTES`
- **THEN** 旧文件被改名为 `<基名>-<日期>.<日期>_<时分秒_微秒>.log`，
  并新建同名文件继续写

#### Scenario: 过期清理

- **WHEN** 日志目录里存在日期早于「今天 − (保留天数 − 1)」的文件
- **THEN** 这些文件被删除；最近 3 个自然日的文件（含被改名的同日分片）保留

#### Scenario: 默认值

- **WHEN** 未配置 `LOG_MAX_BYTES` / `LOG_RETENTION_DAYS`
- **THEN** 分别按 50 MB 与 3 天生效

### Requirement: 采集端后台运行

`deploy/collector.sh run` SHALL **后台启动**采集端：命令返回后进程仍在运行，
SHALL NOT 占用终端（SHALL NOT 进入日志跟随状态）。

后台启动 SHALL：

- 写 **PID 文件**（默认 `<安装目录>/run/collector.pid`）；
- 启动后确认进程存活，不存活则返回非零退出码并打印日志尾部；
- 已有实例在跑时 SHALL **拒绝重复启动**并给出提示；
- 停止后台进程 SHALL 走 SIGTERM（优雅退出，采集端据此推进/丢弃在途批次）。

`deploy/collector.sh` SHALL 另提供：

- `run --foreground`：**前台**运行（原行为），供 systemd 单元使用；
- `status`：报告进程是否在跑、PID、运行时长、日志目录；
- `stop`：按 PID 文件优雅停止。

systemd 单元的 `ExecStart` SHALL 改用 `collector.sh run --foreground`
（前台模式不写 PID 文件）。

#### Scenario: 后台启动

- **WHEN** 执行 `bash collector.sh run`
- **THEN** 命令在数秒内返回 0，PID 文件存在，进程仍在后台运行，日志文件持续增长

#### Scenario: 重复启动

- **WHEN** 已有实例在跑时再次执行 `collector.sh run`
- **THEN** 命令返回非零退出码并说明「已在运行」与既有 PID，不启动第二个进程

#### Scenario: 前台模式

- **WHEN** 执行 `bash collector.sh run --foreground`
- **THEN** 进程在前台运行，终端输出实时日志，不写 PID 文件

#### Scenario: 停止

- **WHEN** 执行 `bash collector.sh stop`
- **THEN** 进程收到 SIGTERM 后优雅退出，PID 文件被清理

#### Scenario: systemd 兼容

- **WHEN** systemd 用 `ExecStart=/opt/tpops-collector/collector.sh run --foreground` 启动
- **THEN** 进程保持在前台，systemd 能正常管理与重启它

### Requirement: 日志不落敏感信息

写落到**文件**的日志 SHALL 与写往 stderr 的日志遵守同一条红线：
SHALL NOT 出现 TPOPS 账号口令、mini-token、数据库业务 SQL 文本全文。

#### Scenario: 采集异常

- **WHEN** 命令中心任务失败，日志需要打印失败原因
- **THEN** 打印接口路径与错误分类，SHALL NOT 打印 `sql_text` 全文

## MODIFIED Requirements

### Requirement: 每轮结构化汇总日志

采集端 SHALL 每个调度 tick 输出一行结构化汇总日志到**总日志**，
包含 tick 号、耗时、拉取数、转换数、投递数、失败数、丢弃的不可提供指标数、窗口数、告警数。

该行 SHALL 为**整体**口径；各通道的分开口径由「每通道每轮汇总行」承担，
SHALL NOT 把通道明细塞进整体行。

#### Scenario: 正常一轮

- **WHEN** 一个 tick 内四组任务均完成
- **THEN** 总日志输出一行含上述全部计数的日志，且各计数之和自洽（投递数 + 失败数 ≤ 转换数）

#### Scenario: 汇总行落盘

- **WHEN** 进程以 `LOG_DIR` 配置运行一轮
- **THEN** **整体**汇总行只出现在总日志；**各通道**的汇总行只出现在对应通道日志，
  同时以简洁形式出现在总日志（SHALL NOT 出现通道内部明细）

### Requirement: 凭据仅来自环境变量且不入日志

TPOPS 用户名口令、mini-token SHALL 仅来自环境变量；
SHALL NOT 出现在任何日志输出中——包括**写往文件的日志**。

#### Scenario: 认证失败

- **WHEN** 认证接口返回失败
- **THEN** 日志记录失败原因与接口路径，不含口令或 token

#### Scenario: 落盘日志

- **WHEN** 日志写入 `<LOG_DIR>` 下的文件
- **THEN** 文件内容同样不含凭据与 token
