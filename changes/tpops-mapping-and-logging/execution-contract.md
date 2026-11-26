# 执行合同：指标清理、日志框架（loguru）、字段映射修正

> 语言：中文（`artifact_language=zh`）。标识符、字段名、路径、命令**逐字**保留。
> 规划工件：`proposal.md` / `specs/`（3 份增量）/ `design.md` / `tasks.md`（DP-2 已批准，修订版 2）。

## Intent Lock

- **变更名称**：`tpops-mapping-and-logging`
- **要解决的问题**（三个已观察到的事实）：
  1. **表级指标重复投递** —— 客户指标库 101 行里 11 个表级指标各出现两次
     （「表信息」分类中文名正常 vs「事务信息」分类中文名是英文，英文码仅大小写前缀不同），
     两行都被 `metric_rules.json` 收录且 `enabled=true` → 同一张表产出两条不同指标消息。
  2. **日志只到 stderr、无通道粒度** —— `logging_setup.py` 只装了一个
     `StreamHandler(sys.stderr)`，`collector.sh run` 以前台启动，日志不落盘；
     每轮汇总只有 tick 整体口径，看不到哪类采集器拉了多少、推了多少、耗时多久。
  3. **两个字段映射缺陷** —— 254 条事件类告警各造一个 `gaussdb.alarm.<alarm_id>`；
     表级 `{#NAME}` 绑到 `relname` → 不同 schema 的同名表得到**相同**的
     `metricName`/`metricCode`/幂等键 → 后一条被**静默丢弃**。
- **范围内**：
  - 生效指标口径 101 → **87**（删 11 条重复的英文变体 + 3 条无法提供）；
  - 日志框架换成 **loguru 0.8.0**，落盘（总日志 + 5 个通道日志，**文件名带日期后缀**）、
    **按天切分**、**保留 3 天**、每类采集器**每轮一行**汇总（获取量/已推送量/耗时等）；
  - `collector.sh run` 改**后台运行**（PID 文件），保留 `run --foreground`（systemd）、
    新增 `status` / `stop`；
  - 告警统一指标名（`metricName` + `metricCode` 都统一）；
  - 表级指标 `{#NAME}` = **`schema_name.table_name`**，不同 schema 不碰撞。
- **范围外**：
  - 客户 Kafka 地址 / Topic 的 P5 联调（等客户提供）；
  - **不改客户 xlsx 原件**（删除只落在排除清单）；
  - 14 条「中文名是英文」的**独立**指标（`GAUSSDB.Buffers_Alloc` 等）——**保留**；
  - 其余 8 项 `open_items.json` 待客户确认项；
  - 日志外发 / 日志平台接入 / systemd unit 的正式交付；
  - 采集端其它行为（调度模型、并发、水位、幂等语义本身）不变。

## Approved Behavior

19 条需求（3 份增量 spec 的全部 `### Requirement`），编号 `R1..R19`：

| # | 能力 | 需求 | 关键场景（WHEN → THEN 摘要） |
|---|---|---|---|
| R1 | mapping | 排除清单 | 清单在包内 `mapping/data/excluded_metrics.json`；`duplicate` 必带 `kept`；排除发生在**构建期**，运行期无丢弃分支 |
| R2 | mapping | 生效指标口径为 87 条 | 87 条规则、`enabled` 全 true、`metric_code` 唯一、`dropped_unsupported == 0` |
| R3 | mapping | 告警的统一指标名 | 未挂客户指标的事件类告警 `metricCode` 相同、`metricName` 相同；挂靠客户指标的不变；统一值表驱动 |
| R4 | mapping | 维度展开（改） | 表级 `{#NAME}` = `schemaname.relname`；按两列分组；`public.t1` 与 `other.t1` 的 `metricCode` 不同 |
| R5 | mapping | 不可提供指标的处置（改） | 被排除指标不在生效口径内；书面清单标注「已从生效口径移除」 |
| R6 | mapping | 映射表驱动（改） | 排除清单是数据文件；改口径不改代码 |
| R7 | mapping | 指标幂等键（改） | `metric_name`/`dimensions` 任一不同 → 键不同 |
| R8 | runtime | 日志目录与文件布局 | 6 基名 × 日期后缀；`LOG_DIR` 未设置 → 只写 stderr；不可写 → 启动失败 |
| R9 | runtime | 总日志与通道日志的分工 | 总日志有整体汇总 + 每通道一行；通道日志只有本通道；通道 ERROR 两边都有 |
| R10 | runtime | 每通道每轮汇总行 | 11 个固定字段；未到期通道不出行；通道行之和 == 整体行 |
| R11 | runtime | 日志格式 | text 用 loguru 原生格式（`LOG_FORMAT` 可覆盖）；json 含 `timestamp/level/logger/thread/message` + `k=v` 摊平 + `exception` |
| R12 | runtime | 日志切分与保留 | 按天切；同日超 50 MB 改名切分；保留 3 个自然日（含同日分片） |
| R13 | runtime | 采集端后台运行 | `run` 后台 + PID 文件 + 重复启动拒绝 + 启动失败打印日志尾部；`run --foreground` 前台；`status`/`stop` |
| R14 | runtime | 日志不落敏感信息 | 落盘文件无口令 / mini-token / `sql_text` 全文 |
| R15 | runtime | 日志框架为 loguru | 全面 loguru API；第三方库经 `InterceptHandler` 转发；py3.7.9 / 纯 Python / 零传递依赖 |
| R16 | runtime | 每轮结构化汇总日志（改） | 整体口径行只进总日志；通道口径由 R10 承担 |
| R17 | runtime | 凭据仅来自环境变量且不入日志（改） | 文件日志同样不含凭据 |
| R18 | delivery | 幂等去重（改） | 同名表不同 schema 的两条消息键不同、都被投递；同表重复采集仍去重 |
| R19 | delivery | 业务维度的落点（改） | 表级 `tags.dimension_values.NAME` = `public.t1`；`resourceName` 不含 `t1` |

- **验收检查**（全部可机械判定）：
  1. `tpops_kafka_collector/mapping/data/metric_rules.json` 恰好 87 条规则、0 条 `enabled=false`、`metric_code` 无重复；
  2. `excluded_metrics.json` 恰好 14 条（11 `duplicate` + 3 `unsupported`），每条有理由，重复项有 `kept`；
  3. 未挂客户指标的告警 `metricCode` 全部等于同一固定值；
  4. 表级 `tags.dimension_values.NAME` 形如 `public.t1`，`public.t1` 与 `other.t1` 的 `metricCode` 与幂等键不同；
  5. `<LOG_DIR>` 下出现 6 基名 × 日期后缀的日志文件；跨天产生新日期文件、超过 3 天的文件被删；
  6. 每个通道日志里能看到该通道每轮的 11 字段汇总行；总日志里能看到各通道的简洁汇总；
  7. `bash tests/scripts/test_collector_runtime.sh` 全绿；
  8. `conda run -n tpops python -m pytest -q` 全绿；覆盖率**全包 ≥ 80%、映射层 ≥ 90%**；
     `conda run -n tpops python -m pytest tests/test_py37_compat.py -q` 通过；
     `conda run -n tpops python -m ruff check tpops_kafka_collector tests` 无告警。

## Design Constraints

- **架构约束**
  - 采集端代码单文件 **< 400 行**、函数 **< 50 行**；按功能分模块，不堆大文件；
    `scheduler.py` 已 392 行 → 通道聚合逻辑放**新文件** `channel_stats.py`。
  - 跨模块传递的数据 SHALL 用 frozen dataclass / 不可变 NamedTuple，SHALL NOT 原地修改。
  - 错误显式处理：捕获后包装成自定义异常并带上下文；SHALL NOT 空 `except`；
    SHALL NOT 用 `print` 记录日志。
  - **映射层表驱动**：客户口径 SHALL 来自 `tpops_kafka_collector/mapping/data/*.json`
    （运行期真相源；仓库根 `data/mapping/` 是同源副本），SHALL NOT 硬编码指标名、
    列名、单位、维度键或排除清单。
  - 日志 sink 分层（loguru）：总日志 sink / 5 个通道 sink（`filter=` 读
    `record["extra"]["channel"]`）/ stderr sink；未绑定 channel 的记录只进总日志。
- **接口约束**
  - ITSCADA 消息字段集合 SHALL 以客户两份对接方案的字段表为准，SHALL NOT 自创字段；
    指标消息 7 字段、告警消息 13 字段。
  - ITSCADA 按 `metricName` + `resourceName` 匹配 → **指标名与资源名必须稳定**；
    表级指标的 `metricName` 变为含 `schema.table` 前缀（**对外契约变化**，
    交付说明必须写清台账录入方式）。
  - 告警 `metricName`/`metricCode` 统一 → ITSCADA 侧告警台账从 254 项变 1 项
    （**对外契约变化**，统一值登记进 `open_items.json` 待客户确认）。
  - 幂等键定义不变（`instance_id + metric_name + 排序后的 dimensions + timestamp`），
    只随维度值变化而变化。
- **依赖约束**
  - **Python 3.7.9 语法红线**：禁海象 `:=`、`f"{x=}"`、字典合并 `d1 | d2`、
    内建泛型注解 `list[str]`、`typing.Literal/Protocol/TypedDict/Final`、
    `functools.cached_property`、`importlib.metadata`、`math.prod`、`zoneinfo`、
    `asyncio.to_thread`、`str.removeprefix/removesuffix`。
  - 依赖锁：`kafka-python==2.0.2`、`requests>=2.25,<3.32`、`urllib3>=1.21.1,<3`、
    **`loguru==0.8.0`**（新增；纯 Python `py3-none-any`；Linux + py3.7 零传递依赖）。
  - 运行期依赖 SHALL 全部为**纯 Python**（`*-none-any`）→ x86_64 与 aarch64
    共用同一份部署包；SHALL NOT 需要编译或联网。
  - 目标机：麒麟 V10 离线，Python 3.7~3.11（**3.12+ 不行**）。
- **数据约束**
  - 客户 xlsx 原件 SHALL NOT 被修改；删除只落在排除清单数据文件。
  - 生效口径 = 101 − 11 − 3 = **87**；排除清单 14 条（11 + 3）。
  - `dimension_bindings` 的值 SHALL 支持「点号连接的多个列名」；
    `group_by` SHALL 支持多列分组。
  - 表级 `{#NAME}` = `schemaname` 与 `relname` 用 `.` 连接。
  - 日志：文件名 `<基名>-<YYYY-MM-DD>.log`；同日超限改名
    `<基名>-<YYYY-MM-DD>.<日期>_<时分秒_微秒>.log`；保留最近 3 个自然日（含当天）。
  - 日志 SHALL NOT 出现 TPOPS 口令、mini-token、业务 SQL 文本全文。
  - 汇总行 SHALL 写成纯 `k=v` token（值不含空格），否则 JSON 摊平失效。
- **TPOPS 接口红线**（本次不得违反）：两套命名空间
  （`/rds/v3/…` 与 `/rds/gaussdb/v3/…`）；mini-token 在响应头 `X-Mini-Token`，
  认证失败**不重试**（防锁号）；`verify_tls=False`；指标窗口 ≥ 1h；
  `end_time` 回退 5 分钟；`is_latest_point_only=false`；12 个分组码硬编码，
  空分组靠告警发现；告警 `limit ≤ 100` 需分页、历史仅存 30 天需显式告警；
  命令中心用 `statistics.running == 0` 判结束、`result` 二次 `json.loads`、
  只在 master 执行、GaussDB 方言无 `FILTER` 且 `state is not null`、
  `sql_text` 被掩码。

## Execution Plan

由 `ssf execution recommend` 列出可用方式并推荐；`ssf execution plan --confirm`
在 **DP-4** 由用户确认后落盘。本合同只定义**波次边界与验收**，不预设执行模式。

## Execution Waves

### Wave 1 · 映射口径（生效 87 条 + 两处字段修正）

- **Wave ID**：`wave-1-mapping`
- **任务**：1.1 排除清单数据文件 → 1.2 生成器按清单剔除 → 1.3 映射层加载与校验 →
  1.4 点号组合维度绑定 → 1.5 生成器改绑 `schemaname.relname` → 1.6 表级幂等不碰撞 →
  1.7 告警统一指标名（含放宽告警 `metric_code` 唯一性校验） → 1.8 交付文档重生成
- **依赖 wave**：无
- **策略**：`serial`（1.1→1.2→1.3 严格串行；1.4→1.5→1.6 严格串行；
  1.7、1.8 与前者无代码依赖，可并行）
- **目标**：生效映射表 87 条、零重复；表级维度 = `schema.table`；告警统一名落地
- **输入**：客户指标库 xlsx（只读）、`data/metric_response/`、`data/merged_sql/`
- **输出**：`包内/excluded_metrics.json`（新）、重生成后的 `metric_rules.json` /
  `customer_metric_mapping.json` / `alarm_rules.json`、
  `rules_context.py`（组合绑定）、`loader.py` + `validation.py`（清单校验）、
  `tools/build_itscada_mapping.py`、重生成的三份交付文档
- **完成标准**：
  - `pytest tests/unit/test_excluded_metrics.py tests/unit/test_dimension_binding.py tests/unit/test_alarm_metric_unification.py tests/unit/test_metric_catalog.py -q` 全绿；
  - 生效规则数 = 87、`enabled=false` 数 = 0、`metric_code` 无重复；
  - 排除清单 14 条（11/3），包内与仓库根两份一致；
  - `public.t1` 与 `other.t1` 的 `metricCode` 与幂等键不同。
- **Review gate**：review report 路径 `changes/tpops-mapping-and-logging/reviews/wave-1.md`，
  base/head SHA 由执行时记录，receipt 必须为 `pass`

### Wave 2 · 日志框架（loguru）

- **Wave ID**：`wave-2-logging`
- **任务**：2.1 loguru 依赖落地 → 2.2 `logging_setup.py` 重写为 loguru sink 装配 →
  2.3 全面改用 loguru API → 2.4 配置项与快速失败 → 2.5 通道级每轮统计 →
  2.6 通道级汇总行输出 → 2.7 敏感信息不进落盘日志
- **依赖 wave**：`wave-1-mapping`（仅排序先后，无代码依赖）
- **策略**：`serial`（2.1→2.2→2.3→2.4 严格串行；2.5→2.6 串行；2.7 依赖前两者）
- **目标**：日志落盘 + 按天切分 + 保留 3 天 + 通道分流 + 每通道每轮汇总；
  全项目走 loguru
- **输入**：`logging_setup.py`、`config.py`、各采集器模块、`scheduler_report.py`、
  `scheduler_types.py`、`requirements-runtime.txt`、`tools/build_deploy_bundle.py`
- **输出**：`logging_setup.py`（重写）、`channel_stats.py`（新）、
  `scheduler_types.py`（`ChannelRoundStats`）、`scheduler_report.py`、
  `config.py`、约 30 个模块的 logger 获取方式、`requirements-runtime.txt`、
  离线部署包 wheel 清单、`CLAUDE.md` 约定更新
- **完成标准**：
  - `pytest tests/unit/test_logging_files.py tests/unit/test_loguru_migration.py tests/unit/test_loguru_dependency.py tests/unit/test_channel_round_stats.py tests/integration/test_logs_have_no_secrets.py -q` 全绿；
  - 生产包内不再出现 `logging.getLogger`（`InterceptHandler` 除外）；
  - `<LOG_DIR>` 下 6 基名 × 日期后缀文件；跨天新文件；超 3 天文件被删；
  - 通道文件只含本通道记录；通道汇总行 11 个字段齐全。
- **Review gate**：review report 路径 `changes/tpops-mapping-and-logging/reviews/wave-2.md`，
  receipt 必须为 `pass`

### Wave 3 · 部署与文档

- **Wave ID**：`wave-3-deploy`
- **任务**：3.1 `collector.sh` 后台化（拆 `deploy/collector_runtime.sh`）→
  3.2 systemd 与部署说明同步 → 3.3 日志框架规范成文（`docs/日志框架规范.md`）
- **依赖 wave**：`wave-2-logging`
- **策略**：`serial`
- **目标**：`run` 后台化 + `status`/`stop`/`--foreground`；文档与脚本行为一致
- **输入**：`deploy/collector.sh`、`docs/交付-部署运行说明.md`、`deploy/README.md`、
  `design.md` 决策段
- **输出**：`deploy/collector_runtime.sh`（新）、`deploy/collector.sh`、
  `docs/交付-部署运行说明.md`、`deploy/README.md`、`docs/日志框架规范.md`（新）
- **完成标准**：
  - `bash tests/scripts/test_collector_runtime.sh` 全绿；
  - `pytest tests/unit/test_delivery_docs.py tests/unit/test_logging_doc.py -q` 全绿；
  - 交付说明里的 `ExecStart` 含 `run --foreground`；环境变量表含
    `LOG_DIR`/`LOG_FORMAT`/`LOG_MAX_BYTES`/`LOG_RETENTION_DAYS`；
  - `collector.sh` 与 `collector_runtime.sh` 均 < 400 行。
- **Review gate**：review report 路径 `changes/tpops-mapping-and-logging/reviews/wave-3.md`，
  receipt 必须为 `pass`

### Wave 4 · 全量回归与门禁（收口）

- **Wave ID**：`wave-4-verification`
- **任务**：3.4 全量回归与门禁（四条命令）
- **依赖 wave**：`wave-1-mapping`、`wave-2-logging`、`wave-3-deploy`
- **策略**：`serial`
- **目标**：全绿 + 覆盖率达标 + py37 兼容 + ruff 干净
- **完成标准**：四条命令全部通过（pytest / pytest --cov / pytest tests/test_py37_compat.py / ruff）
- **Review gate**：review report 路径 `changes/tpops-mapping-and-logging/reviews/wave-4.md`，
  receipt 必须为 `pass`

## Test Obligations

- **必须先从失败测试开始的行为（RED 先行）**：
  - 排除清单的加载与互斥校验（清单里的码一个都不在生效映射表中）；
  - 点号组合维度绑定（`schemaname.relname` → `public.t1`）；
  - 同名表不同 schema 的幂等键不碰撞；
  - 告警统一指标名（254 条同码、3 条挂靠的不变）；
  - 生效规则数 = 87 / `enabled=false` = 0 / `metric_code` 唯一；
  - loguru sink 装配：日期文件名、跨天切分、同日超限改名、保留 3 天清理、
    通道 filter 分流、总日志含各通道简洁汇总；
  - `collector.sh run` 后台启动、重复启动拒绝、`status`、`stop`、`--foreground`。
- **必需的边界情况**：
  - 排除清单里的指标码不出现在生效映射表；`kept` 缺失（`unsupported`）不算错；
  - 组合绑定取不到列值 → 抛 `RuleError`，SHALL NOT 静默留 `{#NAME}`；
  - `mapped == 0` 时 `delivery_rate` 为 1.000；
  - 未到期通道不输出汇总行；
  - `LOG_DIR` 未设置 → 只写 stderr；`LOG_DIR` 不可写 → 启动失败；
  - 非法 `LOG_MAX_BYTES` / `LOG_RETENTION_DAYS` → 启动失败退出码 2；
  - PID 文件存在且进程活着 → `run` 拒绝启动；
  - 启动后进程立刻退出 → `run` 返回非零并打印日志尾部；
  - 跨天 → 新日期文件出现；旧日期超过 3 天 → 被删除；同日分片同样被清理。
- **回归敏感区域**：
  - 既有 `setup_logging(level, log_format, stream)` 签名与幂等语义（现有测试依赖）；
  - 既有 json 格式的 `exception` 字段与 `k=v` 摊平；
  - `TickResult` 既有字段名（`fetched`/`mapped`/`produced`/`failed`/`windows`/`calls`）；
  - 幂等键定义与「同表重复采集仍去重」；
  - `dropped_unsupported` 字段仍在轮汇总里（值为 0）；
  - 交付文档与生效映射表条数一致（87 / 3）；
  - py37 语法兼容门禁（loguru 是外部包，不得引入 3.8+ 语法进生产代码）。

## Execution Mode

- **可用方式与推荐**：`ssf execution recommend changes/tpops-mapping-and-logging --wave wave-1-mapping:serial:1.1,1.2,1.3 --wave wave-2-logging:serial:2.1,2.2,2.3,2.4 --wave wave-3-deploy:serial:3.1,3.2,3.3 --wave wave-4-verification:serial:3.4`
- **用户确认的模式**：由用户在 DP-4 确认（`sdd` / `inline` / `batch-inline`）
- **执行计划命令**：`ssf execution plan changes/tpops-mapping-and-logging --mode <mode> --confirm --reason "<用户确认理由>" --wave ...`
- **允许的修订**：保留 / 升级为 `sdd`；**不允许降级**。

## Verification Dimensions

| 维度 | 状态 | 检查点 |
|---|---|---|
| Completeness | Pending | 19 条需求全部映射到 wave 且有测试义务（见下方映射表） |
| Correctness | Pending | 8 条验收检查全部机械可判定；四条门禁命令全绿 |
| Coherence | Pending | proposal/specs/design/tasks 的数量口径（101/87/14/11/3）与字段名一致 |
| Scope | Pending | 无新增未声明的依赖与行为；未触碰范围外项 |

**需求 → 波次映射（19/19 已映射，无遗漏）**

| 需求 | 波次 | 任务 |
|---|---|---|
| R1 排除清单 | wave-1 | 1.1, 1.2, 1.3 |
| R2 生效 87 条 | wave-1 | 1.2, 1.3 |
| R3 告警统一指标名 | wave-1 | 1.7 |
| R4 维度展开（schema.table） | wave-1 | 1.4, 1.5 |
| R5 不可提供指标处置 | wave-1 | 1.3, 1.8 |
| R6 映射表驱动 | wave-1 | 1.1, 1.2 |
| R7 指标幂等键 | wave-1 | 1.6 |
| R18 幂等去重 | wave-1 | 1.6 |
| R19 业务维度的落点 | wave-1 | 1.4, 1.5 |
| R8 日志目录与文件布局 | wave-2 | 2.2 |
| R9 总日志与通道日志分工 | wave-2 | 2.2, 2.6 |
| R10 每通道每轮汇总行 | wave-2 | 2.5, 2.6 |
| R11 日志格式 | wave-2 | 2.2 |
| R12 日志切分与保留 | wave-2 | 2.2 |
| R15 日志框架为 loguru | wave-2 | 2.1, 2.2, 2.3 |
| R16 每轮结构化汇总日志 | wave-2 | 2.5, 2.6 |
| R14 日志不落敏感信息 | wave-2 | 2.7 |
| R17 凭据不入日志 | wave-2 | 2.7 |
| R13 采集端后台运行 | wave-3 | 3.1, 3.2, 3.3 |

**跨波次依赖**：wave-1 与 wave-2 无代码依赖（仅排序）；wave-3 依赖 wave-2 的
日志目录与 sink 语义；wave-4 依赖全部。

## Review Gates

- **强制审查点**：每个 wave 完成后记录 `ssf execution review` 的 receipt
  （`--wave <id> --base <sha> --head <sha> --report <path> --verdict pass|fail`）。
- **阻塞类别**：依赖未通过、review receipt 为 `fail`、缺失或过期。
- **收口条件**：4 个 wave 全部有 `pass` receipt。
- **审查重点**：wave-2 额外查「生产包内是否残留 `logging.getLogger`」
  「loguru 是否为纯 Python 且 py3.7 可装」「日志文件是否泄露凭据」。

## Escalation Rules

- **何时回退到 `specifying`**：
  - 需求级口径需要再改（生效口径条数、告警统一名取值、表级命名形式、
    日志保留天数/格式）；
  - 发现「14 条英文中文名的独立指标」其实也该删/该改名；
  - 对外契约变化（表级 `metricName` 前缀、告警统一指标）被客户否决。
- **何时回退到 `bridging`**：
  - loguru 在目标机装不上、或与 py3.7 / 离线部署包冲突（依赖红线被破）；
  - `collector.sh` 后台化与 systemd 的关系需要重新定（必须同时保留
    `run --foreground`）。
- **何时不得继续实现**（任一命中即停）：
  - 生效规则数 ≠ 87 或排除清单 ≠ 14；
  - 表级 `{#NAME}` 仍只绑 `relname`（同名表仍会碰撞）；
  - 告警 `metricCode` 未统一（254 条仍是 `gaussdb.alarm.<id>`）；
  - 引入 3.8+ 语法或非纯 Python 依赖（py37 红线 / 部署包红线）；
  - 日志文件里出现口令、mini-token 或 `sql_text` 全文；
  - 生产包内仍有模块用 stdlib `logging.getLogger`（除 `InterceptHandler` 外）；
  - 覆盖率低于 全包 80% / 映射层 90%，或 py37 兼容门禁不通过；
  - 任一 wave 的 review receipt 为 `fail`。
