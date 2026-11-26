# 技术设计：指标清理、日志框架、字段映射修正

## 已知事实与约束

**现状事实（均已核实）**

1. 客户指标库 `高斯DB-指标明细…xlsx`「指标库」sheet 共 **101 行**：
   - **11 个表级指标各出现两次**：一次在「表信息」分类（中文名正常，如
     `({#NAME})插入的行数` / 码 `gaussdb.{#NAME}.number.Of.insert.tuples`），
     一次在「事务信息」分类（中文名是英文，如 `({#NAME})number of insert tuples` /
     码 `GAUSSDB.{#NAME}.number.Of.insert.tuples`）——两条**仅英文码大小写前缀不同**；
   - 另有 **14 条独立指标的中文名整体是英文**（`GAUSSDB.Buffers_Alloc`、
     `GAUSSDB.Idle_connections`、`gaussdb.locks.accessexclusive[{#DBNAME}]` 等）——
     它们**不重复**，本次**保留**（已与用户确认口径：只删 11 组重复 + 3 条无法提供）；
   - **3 条** `无法提供`：`gaussdb.Fastpath_function_call`、`gaussdb.Disabled_conn`、
     `gaussdb.sendAgentName`（当前在 `metric_rules.json` 里以 `enabled=false` 存在）。
2. `mapping/data/metric_rules.json` 现有 **101 条**规则，**11 条重复的表级指标两条都在**、
   都 `enabled=true` → 运行期对同一张表产出**两条不同的指标消息**
   （`metricCode` 与 `metricName` 都不同），ITSCADA 侧看到重复指标。
3. `tpops_kafka_collector/mapping/data/customer_metric_mapping.json` 由 `tools/build_itscada_mapping.py`
   从客户 xlsx + 可提供性判定生成；**xlsx 是客户文件**，本次不改它。
4. 告警定义 `mapping/data/alarm_rules.json` 共 **257 条**：3 条
   `linked_customer_metric` 指向 `gaussdb.Tps`，254 条事件类告警的
   `metric_code` 是 `gaussdb.alarm.<alarm_id>`、`metric_name` 是告警名。
   `build_alarm_message` 直接取 `definition.metric_name` / `definition.metric_code`。
5. 表级规则的 `dimension_bindings` 为 `{"NAME": "relname"}`，
   `value_rule.params.group_by` 为 `["relname"]`；
   合并视图 `stat_user_tables` 的**实测返回列已含 `schemaname`**（`observed_columns.json`）。
6. 幂等键（`tpops_kafka_collector/ids.py`）= `instance_id + metric_name + 排序后的 dimensions + timestamp`；
   `metric_name` 是占位符替换后的值。
7. 日志：`tpops_kafka_collector/logging_setup.py` 只装了一个
   `StreamHandler(sys.stderr)`；`deploy/collector.sh run` 用 `exec` **前台**启动；
   无日志目录、无 PID 文件、无轮转。
8. 每轮汇总：`scheduler_report.log_round_summary` 出一行 **tick 整体**口径
   （`tick/elapsed/fetched/mapped/produced/failed/dropped_unsupported/windows/alarms/calls/delivery_rate/groups/skipped`）。
   通道内数据散落在 `TaskOutcome`（`fetched` / `windows` / `external_calls`）
   与 `DeliveryStats`（`planned` / `delivered` / `failed` / `alarms`）里，**没有按通道聚合**。
9. 通道名固定五类（`schedule.py`）：`monitor` / `command_center` / `diagnostic` /
   `instance` / `alarm`；每类有**各自的采集周期**（1 / 2 / 5 / 60 分钟分档）。
10. **loguru 0.8.0 已在 Python 3.7.9 上实测**（2026-11-26）：
    `requires_python >=3.5`；wheel 为 `py3-none-any`；
    Linux + py3.7 下**运行期零依赖**（pip 实测未拉任何依赖）；
    `rotation="1 day"` / `retention="3 days"` / `filter=` / `{time:YYYY-MM-DD}`
    文件名 / `{level: <8}` / 异常堆栈全部原生可用；
    同名冲突时 loguru 自动把旧文件改名为 `<基名>.<日期>_<时分秒_微秒>.log`。

**不可破坏的约束**

- Python **3.7.9** 语法红线（禁海象、内建泛型、`zoneinfo`、`cached_property`、
  `str.removeprefix` 等）；依赖锁 `kafka-python==2.0.2` / `requests>=2.25,<3.2`。
- 目标机：**麒麟 V10，离线**，无 logrotate、无 C 扩展、Python 3.7~3.11；
  **x86_64 与 aarch64 共用同一份部署包**（只能用 `*-none-any.whl`）。
- 采集端单文件 **< 400 行**、函数 **< 50 行**；不可变 DTO；`logging` 不用 `print`；
  凭据只走环境变量、禁入日志。
- ITSCADA 侧按 **`metricName` + `resourceName`** 做资源/指标映射，
  指标名与资源名**必须稳定**——改名等于客户台账要跟着改。
- 对外消息字段集合以客户两份对接方案的字段表为准，**不得自创字段**。

## 目标与非目标

**目标**

1. 生效指标口径 = **87 条**，零重复、零「不可提供丢弃」。
2. 日志**落盘**；`collector.sh run` **后台运行**；总日志能一眼看出
   **各采集器这一轮的采集与推送情况**；每个采集器有自己的日志文件。
3. 表级指标的 `{#NAME}` = **`schema_name.table_name`**，
   不同 schema 的同名表**不碰撞**（消息、幂等键都不碰撞）。
4. 不涉及客户指标名的告警，`metricName` / `metricCode` **统一为一个固定名**。
5. 产出**日志框架规范与格式**（本文档 + 对应规格），供审核后实施。

**非目标**

- 不做日志外发/采集（shipper、fluent-bit、日志平台接入）。
- 不改客户 xlsx 原件；不改其余 14 条英文中文名指标；不删交付文档里的
  「不可提供指标」说明（改为标注「已从生效口径移除」）。
- 不重做调度模型（仍是到期驱动 + 两维并发）；不引入新依赖。
- 不做 P5 联调（等客户提供 Kafka 地址/Topic）。

---

## 决策（Decisions）

### 决策 1：日志落盘位置与文件布局

- **选择**：日志目录由环境变量 `LOG_DIR` 指定，**6 个基名**，每个基名**按天**一个文件，
  文件名带**日期后缀**：

  ```
  <LOG_DIR>/collector-2026-11-26.log                  # 总日志
  <LOG_DIR>/collector-monitor-2026-11-26.log          # A 类 监控指标
  <LOG_DIR>/collector-command_center-2026-11-26.log   # B 类 命令中心
  <LOG_DIR>/collector-diagnostic-2026-11-26.log       # C 类 诊断优化
  <LOG_DIR>/collector-instance-2026-11-26.log         # D 类 实例元数据
  <LOG_DIR>/collector-alarm-2026-11-26.log            # E 类 告警
  ```

  命名规则：`<基名>-<YYYY-MM-DD>.log`（loguru 的 `{time:YYYY-MM-DD}` 展开）；
  同一天内超出单日上限时，loguru 把旧文件**改名为**
  `<基名>-<YYYY-MM-DD>.<日期>_<时分秒_微秒>.log` 并新建同名文件继续写
  （实测行为；被改名文件同样在保留期清理范围内）。

  **`LOG_DIR` 未设置时**：进程只写 stderr（不落盘）——开发态的既有行为不变；
  `collector.sh run` 则**总是**显式设置 `LOG_DIR`（默认 `<安装目录>/logs`），
  所以生产态一定落盘。两种语义都由 `config.py` 显式判定，SHALL NOT 靠猜。

  `<安装目录>` = **`collector.sh` 所在目录**（脚本里的 `$HERE`；目标机为
  `/opt/tpops-collector`）。PID 文件 `<安装目录>/run/collector.pid`、
  `console.out` `<安装目录>/run/console.out` 同属该目录。

- **理由**：文件名按**通道基名**固定，不按指标/实例派生 → 无论 87 个指标还是 800 个指标，
  日志基名恒为 6 个，运维 `tail -f` 的对象稳定；通道名与 `schedule.py` 的五个常量
  一一对应，不引入第二套命名。**日期后缀**让「昨天的日志」一眼可辨、可单独拷贝，
  清理也只需按文件名里的日期判断，不需要额外的索引文件。
  `console.out` 放 `<安装目录>/run/` 而不是 `<LOG_DIR>/` → 清理策略只管
  `collector*.log` 一族。
- **替代方案**：
  - *按实例分文件*（`collector-<instance>.log`）：实例数会变，文件数不可控；
  - *单一大文件 + 采集器名过滤*：一条命令看不到「某采集器这一轮」，正是本次要解决的问题；
  - *日期放后缀（`collector.log.2026-11-26`）*：`TimedRotatingFileHandler` 的默认形态，
  但扩展名被挤到中间，不便按 `*.log` 收集与拷贝；
  - *交给 journald*：目标机部署形态不是 systemd，且历史留存口径不由我们控制。
- **后果**：新增 `LOG_DIR` 配置项；`config.py` 需加校验（目录不存在则创建，
  不可写则**启动失败**，不静默降级为只写 stderr）。

### 决策 2：sink 分层与「不重复、不串味」

- **选择**：loguru 的 **sink** 模型（每个 `logger.add(...)` 是一个 sink，
  用 `filter=` 决定哪些记录进哪个 sink）：

  | sink | 收什么 | 落点 |
  |---|---|---|
  | 总日志 sink | 生命周期（启动/停止/装配/致命错误）、tick 整体汇总、**每通道一行**的简洁汇总、WARNING 及以上 | `<LOG_DIR>/collector-<日期>.log` |
  | 通道 sink ×5 | 该通道内部的采集与推送明细 + 该通道每轮汇总行 | `<LOG_DIR>/collector-<channel>-<日期>.log` |
  | stderr sink | 与总日志同内容（前台运行/调试时可见） | 终端 |

  通道归属靠 `logger.bind(channel="…")` 打在记录上，
  通道 sink 的 `filter=` 读 `record["extra"]["channel"]`。
  5 个采集器模块各加一行 `LOGGER = logger.bind(channel="…")`；
  `scheduler_report.py` 发通道汇总时逐通道 `logger.bind(channel=…)` 后发出。
  未绑定 `channel` 的记录 → 只进总日志（不进任何通道 sink）。

  第三方库（`kafka` / `urllib3` / `requests`）的 stdlib 日志经官方
  `InterceptHandler` 转发进 loguru，并用 `logger.disable("kafka")` 等压噪。

- **理由**：总日志要的是**简洁的汇总**，不是明细；明细进分日志。
  用 sink + filter 表达「通道 INFO 不进总日志、通道 ERROR 进总日志」，
  比 stdlib 的 `propagate` + 多 handler 更直白，且不需要每个 logger 单独配置。
- **替代方案**：
  - *每个通道一个 loguru logger 实例（`logger.bind`）而不是 sink filter*：
    loguru 的 logger 是全局单例，bind 是**记录级**的，不是 logger 级的 →
    分流只能靠 sink filter；
  - *沿用 stdlib 的 `propagate` + 多 handler*：与决策 3 的「全面改 loguru API」冲突；
  - *按文件路径分流（写各自的 logger 对象）*：loguru 不支持「logger 绑文件」。
- **后果**：新增 `tpops_kafka_collector/logging_setup.py`（重写）负责装配 sink；
  现有 `setup_logging(level, log_format, stream)` 签名**保持向后兼容**
  （现有测试依赖它），新增 `log_dir` / `log_retention_days` / `log_max_bytes`
  为可选参数。

### 决策 3：日志框架选型 —— loguru（替代自研）

- **选择**：日志框架用 **loguru 0.8.0**，不自研 handler / 轮转 / 保留 / 格式化。
  项目内**全面改用 loguru API**（`from loguru import logger`），
  第三方库（`kafka` / `urllib3` / `requests`）的 stdlib 日志经官方
  `InterceptHandler` 转发进 loguru。

  **已实测核实（Python 3.7.9，2026-11-26）**：

  | 核实项 | 结论 |
  |---|---|
  | `requires_python` | `>=3.5` → 3.7.9 可用；实测 `import loguru` 成功 |
  | wheel | `loguru-0.8.0-py3-none-any.whl` → 纯 Python，x86_64 / aarch64 共用一份部署包 |
  | Linux + py3.7 运行期依赖 | **0 个**（`colorama` 仅 Windows、`aiocontextvars` 仅 py<3.7）；`pip download` 实测未拉任何依赖 |
  | 文件名带日期 | `logger.add("collector_{time:YYYY-MM-DD}.log", …)` → 实测产出 `collector_2026-11-26.log` |
  | 按天切分 | `rotation="1 day"` 原生 |
  | 保留 3 天 | `retention="3 days"` 原生（实测 `retention` 支持 str / int / timedelta / callable） |
  | 同日超限改名 | 同名冲突时 loguru **自动把旧文件改名**为 `<基名>.<日期>_<时分秒_微秒>.log` 并新建同名文件 |
  | 通道分流 | sink 的 `filter=` 读 `record["extra"]["channel"]` → 实测通道文件只含本通道记录 |
  | 级别对齐 | `{level: <8}` 原生 |
  | 异常堆栈 | 原生追加（实测输出完整 traceback） |

- **理由**：loguru 把「按天切分 / 文件名日期后缀 / 保留期 / 格式化 / 异常堆栈 /
  通道分流」全部变成**一行配置**，自研这些要写 ~4 个模块（pattern 解析器、
  按天切分 handler、保留清理、handler 装配）并各自测一遍 —— 换成 loguru 后
  这部分代码**净减少**，且 loguru 的行为是**上游维护、被广泛使用**的，
  不如自研轮转逻辑可靠。
  同时满足本项目的三条红线（py3.7.9 / 纯 Python wheel / 离线目标机零额外依赖）。

- **替代方案**：
  - *继续用 stdlib `logging` + 自研*：要多写 4 个模块、多测一轮，且轮转/保留逻辑
    长期要自己维护；
  - *stdlib `logging` + loguru 的 `InterceptHandler`（调用点零改动）*：
    改动最小，但项目里仍是 stdlib 风格，loguru 只当 sink 用，
    「用 loguru 简化代码」的收益打折；用户已明确选**全面改成 loguru API**；
  - *换 `structlog`*：py3.7 支持与纯 Python 均可，但按天切分/保留/通道分流
    都要自己再包一层，比 loguru 的现成能力差；
  - *引 `filebeat`/`fluent-bit` 收日志*：目标机离线、无额外二进制，与本次目标相反。
- **后果**：
  - **新增一个运行期依赖** `loguru==0.8.0`（纯 Python、零传递依赖）：
    `requirements-runtime.txt` 要加；`tools/build_deploy_bundle.py` 的离线 wheel
    清单要加；`CLAUDE.md` 里「运行时依赖只有 requests / kafka-python / urllib3
    三个纯 Python 包」这句话要改。**不破坏**「x86_64 与 aarch64 共用一份部署包」。
  - 项目编码约定要改：`CLAUDE.md` 的「用 `logging`，不用 `print`」改为
    「用 `loguru` 的 `logger`，不用 `print`」；
    `LOGGER = logging.getLogger(__name__)` 改为 `from loguru import logger`。
  - **约 30 个模块**的 logger 获取方式要改（调用点 `LOGGER.info(...)` 本身不变，
    只改 import 与 logger 变量的创建）。
  - 通道分流靠 `logger.bind(channel="…")`：5 个采集器模块各加一行
    `LOGGER = logger.bind(channel="…")`，`scheduler_report.py` 发通道汇总时
    用 `logger.bind(channel=…)` 逐通道发。
  - 第三方库降噪改用 loguru 的 `logger.disable("kafka")` 等。

### 决策 4：格式与 sink（loguru 原生，不强加 logback）

- **选择**：**直接用 loguru 的原生格式**，不自研 logback pattern 解析器。
  - **文本 sink**：用 loguru 默认格式；可用 `LOG_FORMAT`（loguru 原生字符串语法，
    如 `{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name} | {message}`）覆盖。
    loguru 默认格式已含 `{time}` / `{level}` / `{name}:{line}` / `{message}`，
    信息量与 logback 默认 pattern 相当。
  - **JSON sink**：用 loguru 的 `format=` **自定义函数**（不是字符串），
    产出既有 schema：`timestamp` / `level` / `logger` / `thread` / `message`，
    整条消息是 `k=v` 形式时**摊平成顶层字段**，异常带 `exception` 字段。
    （这一点 loguru 的 `serialize=True` 做不到 —— 它输出的是
    `{"text": …, "record": {…}}` 固定结构。）

- **理由**：用户明确「直接用 loguru 的日志格式即可，不用强制 logback」。
  loguru 的原生格式已经足够（时间/级别/模块:行号/消息），且客户可自行覆盖。
  自研 logback pattern 解析器（把 `%d{…}` 翻译成 `{time:…}`）要多写一个模块、
  多测一轮，换来的只是「能贴 logback 配置」这一点。

- **替代方案**：
  - *自研 logback pattern 解析器*：用户已否决；
  - *`serialize=True`*：schema 不符（`text` + `record` 嵌套），客户日志平台
    按 `channel` / `fetched` 取值要二次展开；
  - *只用文本、放弃 JSON*：客户侧日志平台按字段取值的诉求落空。
- **后果**：
  - 汇总行仍必须写成纯 `k=v` token（值不含空格），否则 JSON 摊平失效；
  - 文本 sink 的默认格式含 `{name}:{line}` —— 调用点行号对定位问题有用，
    但行号会随代码变动，**不应被当作稳定契约**。

### 决策 5：按天切分与保留策略（loguru 原生 + 一个 rotation 回调）

- **选择**：用 loguru 的 `rotation` + `retention`，两个参数：

  | 参数 | 环境变量 | 默认 | 落地方式 |
  |---|---|---|---|
  | 同日上限 | `LOG_MAX_BYTES` | **50 MB** | `rotation=` **自定义回调**：当日文件 `tell() + len(message)` 超限 → 立即切 |
  | 保留天数 | `LOG_RETENTION_DAYS` | **3** | `retention="3 days"`（保留最近 3 个自然日，更早的自动删） |
  | 跨天切分 | —— | 次日 00:00 | 文件名里的 `{time:YYYY-MM-DD}` + rotation 回调判定跨天 |

  文件名：`<基名>-<YYYY-MM-DD>.log`（loguru 的 `{time:YYYY-MM-DD}` 展开）。
  同日超限时，loguru 把旧文件**改名为** `<基名>-<YYYY-MM-DD>.<日期>_<时分秒_微秒>.log`
  并新建同名文件继续写（实测行为）；被改名的文件同样落在 `retention` 的清理范围内
  （glob 是 `<基名>-*.log`）。

- **理由**：`rotation` 原生只支持「时间」或「大小」二选一，本需求要
  「按天 + 同日上限」两个条件 → 用**一个 rotation 回调**同时表达
  （loguru 的 callable rotation 语义实测确认：返回真值即立即切）。
  `retention="3 days"` 正是用户要的「保留 3 天」，且**含**被改名产生的同日分片。

- **替代方案**：
  - *loguru 原生 `rotation="50 MB"`*：失去按天切分与日期文件名，不满足需求；
  - *loguru 原生 `rotation="1 day"`*：满足按天切分，但同日无上限；
  - *自研 handler*：与决策 3 冲突（又要把轮转逻辑写回自己手里）。
- **后果**：磁盘占用上限 ≈ `6 基名 × 3 天 × 50 MB`（最坏 ~900 MB，实际远低于此）；
  需在部署说明里写清保留口径与上限估算。

### 决策 6：每通道每轮汇总的统计口径

- **选择**：每轮（tick）内**到期并完成**的通道各输出一条汇总行，字段固定：

  ```
  channel=command_center tick=12 instances=1 fetched=520 mapped=498 produced=498
  failed=0 windows=11 calls=4 elapsed=3.2s delivery_rate=1.000
  ```

  | 字段 | 来源 | 定义 |
  |---|---|---|
  | `channel` | 通道名（`schedule.py` 的常量） | — |
  | `tick` | 轮次序号 | — |
  | `instances` | 本轮该通道参与的实例数 | — |
  | `fetched` | 该通道各任务 `TaskOutcome.fetched` 之和 | 从平台取回的原始行数 / 告警条数 |
  | `mapped` | 该通道各实例 `DeliveryStats.planned` 之和 | **幂等过滤后**实际尝试投递的消息数（与整体行的 `mapped` 同口径） |
  | `produced` | 该通道各实例 `DeliveryStats.delivered` 之和 | 成功投递的消息数 |
  | `failed` | 该通道各实例 `DeliveryStats.failed` 之和 | 投递失败的消息数 |
  | `windows` | 该通道各任务 `TaskOutcome.windows` 之和 | 本轮该通道执行的 SQL 条数 |
  | `calls` | 该通道各任务 `TaskOutcome.external_calls` 之和 | 本轮该通道的外部调用次数 |
  | `elapsed` | 该通道本轮耗时（秒） | 从该通道任务开始到全部完成的墙钟 |
  | `delivery_rate` | `produced / mapped` | `mapped=0` 时为 `1.000` |

  **未到期的通道不输出汇总行。** 该行用 `logger.bind(channel="<通道名>")` 打出
  （进该通道日志），同时在 `collector.summary` 上打一条**同字段、只留通道核心计数**的行
  （进总日志）。总日志因此能一眼看出各采集器这一轮的获取量/推送量/耗时。
  **通道行各字段之和 SHALL 等于整体行同名字段的值**（同一取数来源，只是分组更细）。

- **理由**：`fetched` / `windows` / `calls` 目前已随 `TaskOutcome` 逐任务带着走，
  `DeliveryStats` 已按 (通道, 实例) 给出投递计数 → 按通道聚合是**纯加法**，
  不需要改采集器内部逻辑。`elapsed` 用通道级墙钟（任务并发，取最晚完成者），
  与「这一轮该采集器跑了多久」这个运维问题的口径一致。
- **替代方案**：
  - *复用现有整体行的 `groups` 字段塞通道明细*：会把 JSON 摊平打乱，
    且明细量随通道数增长，不符合「总日志要简洁」；
  - *逐条消息记明细到通道日志（INFO 级）*：量太大（87 指标 × 多实例 × 周期），
    实测单轮几百到几千条，INFO 级不可接受。**明细留 DEBUG 级**，
    默认 `LOG_LEVEL=INFO` 不落。
- **后果**：`scheduler_report.py` 需从「只出整体行」扩成「出整体行 + 通道行」；
  `scheduler.py` 的 `_build_result` 需带上按通道聚合的结果（新增不可变 DTO
  `ChannelRoundStats`）。字段名沿用现有 `TickResult` 的命名（`fetched` / `mapped` /
  `produced` / `failed` / `windows` / `calls`），**不新造同义字段**。

### 决策 7：`collector.sh run` 后台化

- **选择**：

  | 子命令 | 行为 |
  |---|---|
  | `run` | **后台**启动：写 PID 文件（`<安装目录>/run/collector.pid`），命令返回后进程继续跑 |
  | `run --foreground` | **前台**启动（原行为），systemd 用 |
  | `status` | 报告在否 / PID / 运行时长 / 日志目录 / 日志尾部 |
  | `stop` | 按 PID 文件发 SIGTERM，优雅停止 |

  后台启动用 `setsid nohup … >> "<安装目录>/run/console.out" 2>&1 &`，
  启动后等待并确认进程存活；**进程没起来则返回非零并打印日志尾部**
  （`<LOG_DIR>/collector.log` 的最后 N 行 + `console.out` 的最后 N 行）；
  **已有实例在跑则拒绝启动**并返回非零。
  `<安装目录>/run/` 目录由脚本负责创建（不存在则创建，存在则复用）。

- **理由**：用户要求「启动时不是进入日志，而是在后台运行，日志文件要落盘」。
  `setsid` + `nohup` 让进程脱离终端会话，终端关闭或 SSH 断开都不会带走它；
  stdout/stderr 仍重定向到 `console.out`（Python 崩溃前的最后输出不会丢）。
  **保留 `--foreground` 是硬约束**：systemd 单元要求前台进程，
  直接把 `run` 改成后台会**打断现有 systemd 部署方式**——
  交付说明里的 `ExecStart` 必须同步改成 `run --foreground`。
- **替代方案**：
  - *只把 `run` 改后台、不留前台模式*：systemd 托管不了，运维失去 systemd 的
    重启与状态管理能力；
  - *用 `systemd-run`/supervisor 托管*：目标机不一定有 systemd（部署形态是脚本 + 手工）；
  - *PID 文件放状态目录*：状态目录（`STATE_DIR`）由 systemd `StateDirectory=` 建的，
    手工部署时可能不存在 → PID 文件跟安装目录走，与 `.install-mode`、`.python-bin` 同侧。
- **后果**：`deploy/collector.sh` 新增约 120 行（仍在 < 400 行约束内？
  当前 930 行 → **会超 400 行**）。因此把 `run` / `status` / `stop` 三段
  拆到**新文件** `deploy/collector_runtime.sh`，由 `collector.sh` 位置参数分发
  （保持「一个脚本」的对外形态不变）。部署说明与 `deploy/README.md` 同步更新。

### 决策 8：指标清理的落地方式（不改正典，改生效口径）

- **选择**：新增映射数据文件
  **`tpops_kafka_collector/mapping/data/excluded_metrics.json`**（包内为运行期真相源，
  仓库根 `data/mapping/excluded_metrics.json` 为同源副本）：

  ```json
  {
    "version": "1.0",
    "excluded": [
      {"metric_code": "GAUSSDB.{#NAME}.number.Of.insert.tuples",
       "metric_name": "({#NAME})number of insert tuples",
       "reason": "duplicate",
       "kept": "gaussdb.{#NAME}.number.Of.insert.tuples",
       "detail": "与「表信息」分类的同指标重复，仅英文码大小写前缀不同"},
      {"metric_code": "gaussdb.Fastpath_function_call",
       "metric_name": "快路径函数调用的连接数",
       "reason": "unsupported",
       "detail": "PG 侧无 fastpath 计数器；pg_settings 仅有 gsi_fastpath_level 开关"}
    ]
  }
  ```

  字段约定：`metric_code` / `metric_name` / `reason` / `detail` **必填**；
  `kept` **仅 `reason=duplicate` 时必填**（`unsupported` 无「保留哪一条」）。

  `tools/build_itscada_mapping.py` 生成 `metric_rules.json` 与
  `customer_metric_mapping.json` 时按此清单剔除；生效口径 = **101 − 11 − 3 = 87**。
  客户 xlsx **保持原样**。
  排除发生在**构建期**（生成器），运行期加载到的生效映射表里**根本不存在**这些指标码
  → 运行期没有「遍历到再丢弃」的动作，也就没有对应的丢弃日志。

- **理由**：xlsx 是**客户文件**，我们在仓库里改它会造成「客户原件与我们的理解不一致」，
  将来对账说不清。排除清单把「删了什么、为什么删、保留了哪条」变成**可审阅的数据**，
  正好也是交付文档「不可提供指标清单」的素材来源。
  运行时**不再有**「遍历到不可提供指标再丢弃」的动作 → `dropped_unsupported` 恒为 0。
- **替代方案**：
  - *直接改 xlsx*：污染客户原件，且下次客户给新版本会覆盖掉改动；
  - *在代码里写死 14 个码*：违反「映射层表驱动」红线；
  - *保留 3 条无法提供指标为 `enabled=false`*：用户明确要求「删除」，
    且留着会让「不可提供指标清单」永远显示 3 条，客户以为还要等我们补齐。
- **后果**：
  - `tpops_kafka_collector/mapping/data/metric_rules.json` 87 条、`enabled` 全为 `true`；
  - `tpops_kafka_collector/mapping/data/customer_metric_mapping.json` 87 条；
  - 启动期校验「映射表条目 ↔ 生效口径」改为与排除清单互斥校验
    （排除清单的指标码一个都不在生效映射表中）；
  - `docs/交付-指标口径说明.md` 重生成（87 条）、
    `docs/交付-不可提供指标清单.md` 重生成（3 条，标注「已从生效口径移除」+ 理由）；
  - `tools/build_delivery_docs.py` 与 `tools/build_itscada_mapping.py` 都要读排除清单。

### 决策 9：告警统一指标名（表驱动固定值）

- **选择**：在 `mapping/data/alarm_rules.json` 增加一段：

  ```json
  "event_metric": {"metric_code": "gaussdb.alarm", "metric_name": "数据库告警"}
  ```

  生成器在写 254 条事件类告警定义时，`metric_code` / `metric_name` 统一取该值；
  3 条 `linked_customer_metric` 非空的告警**保留**客户指标的值不变。
  该固定值同时登记进 `open_items.json`（待客户确认）。

- **理由**：告警种类由 `alarmId` / `alarmName` / `alarmContent` 承载，
  `metricCode` 逐告警自造（254 份）没有信息量，却让 ITSCADA 台账要维护
  254 个「指标」。统一后 ITSCADA 侧只有一个告警类指标。
  值放在映射数据里 → 客户改口径不用改代码（符合表驱动红线）。
- **替代方案**：
  - *保留 `gaussdb.alarm.<alarm_id>`、只统一 `metricName`*：用户已明确
    「metricName 与 metricCode 都统一」；
  - *统一成一条带告警类型的名（如「数据库告警-锁等待」）*：仍是 254 个，
    等于没统一。
- **后果**：
  - ITSCADA 侧需要**一个**告警指标台账项（不是 254 个）——这是**对外契约变化**，
    需在交付说明里写清楚；
  - 告警**幂等键不含** `metric_code`（键是 `alarm_id + instance_id +
    first_alarm_time + status`）→ 统一化不影响去重语义；
  - `alarm_rules.json` 的 `metric_code` 不再唯一 → 启动期校验的**唯一性约束要相应放宽**：
    **唯一性只对指标规则（`metric_rules.json`）要求**，对告警定义不要求
    （告警定义靠 `alarm_id` 唯一定位）。这条放宽落在任务 1.7。

### 决策 10：表级 `schema_name.table_name` 的组合位置

- **选择**：**在映射层组合**，不动合并 SQL：

  - `dimension_bindings`：`{"NAME": "relname"}` → `{"NAME": "schemaname.relname"}`
    （值里的 `.` 表示「按 `.` 拆成多列，用 `.` 连接各列的值」）；
  - `value_rule.params.group_by`：`["relname"]` → `["schemaname", "relname"]`
    （按两列分组，避免同名表被并成一组）。

  **生成器落点（已核实）**：`tools/build_itscada_mapping.py` 的
  `DIMENSION_COLUMN_BY_SQL` 里 `'stat_user_tables': {'NAME': 'relname'}` 这一行改为
  `'schemaname.relname'`；而 `group_by` 是由 `_row_policy` 从
  `dimension_bindings` 的值派生（`group_by = sorted(set(bindings.values()))`），
  因此**还要把派生处的「点号串」拆成多列**，否则 `group_by` 会变成一个
  叫 `schemaname.relname` 的单列（分组失效）。落点是生成器里的一处常量 + 一处派生逻辑。

  于是 `{#NAME}` 被替换成 `public.t1`；`metricName` = `(public.t1)插入的行数`；
  `metricCode` = `gaussdb.public.t1.number.Of.insert.tuples`；
  `tags.dimension_values.NAME` = `public.t1`；
  幂等键里的 `dimensions` 也随之带上 `public.t1` → **不同 schema 不再碰撞**。

- **理由**：合并视图 `stat_user_tables` 的**实测返回列已含 `schemaname`**
  （`observed_columns.json` 里 `stat_user_tables` 与 `dbstat_sum` 都有），
  所以数据是现成的，改动只在映射层。改合并 SQL 会波及 `observed_columns.json`
  校验、11 条视图的探针留档与 21 条 SQL 的口径，**收益远小于风险**。
  组合放在映射层 → 客户若要换成 `schema.table` 之外的格式，只改映射表。
- **替代方案**：
  - *在合并 SQL 里加一列 `schemaname || '.' || relname as fullname`*：
    能少写一点组合逻辑，但要动 1 条合并 SQL + 全部列名校验 + 留档；
  - *运行时在 `substitute_placeholders` 里特判表级*：把「表名怎么拼」写死在代码里，
    违反表驱动红线；
  - *用 `dimension_bindings` 直接绑两个键（`SCHEMA` / `TABLE`）*：
    客户指标码里只有一个占位符 `{#NAME}`，拆两个占位符会让
    `metricCode` 变成 `gaussdb.{#SCHEMA}.{#TABLE}…`，**与客户指标库对不上**。
- **后果**：
  - `dimension_bindings` 的取值语义扩展为「列名或点号连接的列名列表」→
    `rules_context.row_dimensions` 与 `grouped_items` 需支持组合与多列分组；
  - 表级指标的 `metricCode` **每个表一个**（`gaussdb.public.t1.…`）→
    ITSCADA 侧按 `metricName` 匹配台账，**表级指标的台账项要按实际表名录入**
    （交付说明里必须写清：表级指标的 `metricName` 含库表前缀）；
  - `{#NAME}` 里的 `.` 与指标码自身的 `.` 会连在一起（如
    `gaussdb.public.t1.number.Of.insert.tuples`）——占位符替换是纯文本替换，
    不会歧义，但**交付文档要给出示例**，避免客户误读。

---

## 风险与验证

| 风险 | 缓解 | 验证证据 |
|---|---|---|
| 删掉 11 条重复指标后，客户 ITSCADA 台账仍留着旧的 254/101 项，映射对不上 | 交付说明与 `docs/交付-客户指标英文名核对清单.md` 增列「已移除项 + 理由」；`open_items.json` 登记 | 交付文档重生成后含 87 条口径 + 14 条移除说明；`pytest` 中「生效口径 = 87」断言 |
| 表级 `metricCode` 变成「每表一个」，客户台账需要按表录入 | 交付说明写明表级指标的命名形式与示例；`metricName` 仍是 `({#NAME})…` 形式 | `docs/交付-指标口径说明.md` 中表级条目示例含 `public.t1` |
| 告警统一成一个指标后，客户侧看不到告警种类 | `alarmId` / `alarmName` / `alarmContent` 仍逐条携带；统一值登记进 `open_items.json` 等客户确认 | 告警消息样例测试：254 条事件类告警的 `metricCode` 相同、**`alarmId` 互不相同**、`alarmName` 逐条对得上平台目录（平台目录本身**不保证** `alarm_name` 唯一 —— 13 行填的是等级词、另有同名不同 ID 的组，实测 241 个不同名） |
| 日志量大导致磁盘写满 | 按天切分 + 单日 50 MB 上限（loguru rotation 回调）+ 保留 3 天自动删除；通道明细只在 DEBUG 级 | 切分单测：写满后旧文件被改名、新文件续写；清理单测：过期文件被删、近 3 天保留 |
| 新增 loguru 依赖破坏「纯 Python 三件套」与离线部署 | loguru 0.8.0 是 `py3-none-any`、Linux+py3.7 零传递依赖 → x86_64/aarch64 仍共用一份部署包 | 部署包构建测试断言 loguru wheel 在清单内且为纯 Python；目标机 `check-deps` 通过 |
| 全面改 loguru API 触及 ~30 个模块，易漏 | 改造按「只改 logger 获取方式、调用点不动」推进；`ruff` + 全量 pytest 兜底；未改造的模块仍走 stdlib → 由 `InterceptHandler` 兜住，不会丢日志 | 全量 pytest 全绿；新增测试断言生产包内不再出现 `logging.getLogger` |
| `collector.sh run` 改后台后 systemd 托管失效 | 保留 `run --foreground`；交付说明的 `ExecStart` 同步改 | `deploy/README.md` 与 `docs/交付-部署运行说明.md` 里的 unit 片段为 `run --foreground`；脚本测试覆盖 `run` / `status` / `stop` / 重复启动 |
| 后台进程起不来但脚本返回 0 | 启动后确认进程存活，不存活则返回非零并打印日志尾部 | 脚本测试：故意用坏解释器 → `run` 返回非零 |
| 日志文件泄露凭据 | 日志内容审查在采集器侧；新增「落盘文件不含凭据」的测试 | 落盘日志测试：跑一轮后 grep 日志文件无口令/token/`sql_text` |
| 通道级统计口径与现有整体口径打架（同一字段两套含义） | 通道行复用现有字段名与取数来源（`TaskOutcome` / `DeliveryStats`），不加新字段 | 通道汇总的 `fetched` 之和 == 整体行的 `fetched`（单测断言） |
