# 实现任务：指标清理、日志框架、字段映射修正

> **路径约定**：运行期映射数据真相源 = `tpops_kafka_collector/mapping/data/`；
> 仓库根 `data/mapping/` 是**同源副本**（由生成器同步，改一侧必须同步另一侧）。
> 下文写 `包内/…` 指前者、`仓库根/…` 指后者。

## 交付与证明

| 批次 | 交付结果 | 依赖 | 证明 |
|---|---|---|---|
| 1 | 生效指标口径收敛到 87 条，零重复、零丢弃；表级指标改 `schema_name.table_name`；告警统一指标名 | 无 | `pytest tests/unit/test_excluded_metrics.py tests/unit/test_dimension_binding.py tests/unit/test_alarm_metric_unification.py tests/unit/test_metric_catalog.py -q` |
| 2 | 日志框架换成 loguru：落盘（总日志 + 5 个通道日志，按天文件名）、按天切分 + 保留 3 天、通道级每轮汇总、全项目改 loguru API | 批次 1（仅排序在后，无代码依赖） | `pytest tests/unit/test_logging_files.py tests/unit/test_loguru_migration.py tests/unit/test_loguru_dependency.py tests/unit/test_channel_round_stats.py tests/integration/test_logs_have_no_secrets.py -q` |
| 3 | `collector.sh run` 后台化 + `status`/`stop`/`--foreground`；环境变量与交付文档同步 | 批次 2 | `bash tests/scripts/test_collector_runtime.sh` |
| 4 | 全量回归与门禁 | 批次 1~3 | 见任务 3.4 |

> **每个任务都自带它要新建的测试文件**（下文的「新增 …」即为该任务的交付物之一）——
> 证明命令引用的测试文件不存在，就是该任务没做完。

## 任务

### 批次 1 · 映射口径

- [ ] **1.1 建排除清单数据文件**：新增 `包内/excluded_metrics.json`，
  写入 14 条（11 条 `reason=duplicate`，各带 `kept` 指明保留项；3 条 `reason=unsupported`，
  各带 `detail` 理由）；`reason`/`metric_code`/`metric_name`/`detail` 必填，`kept` 仅重复项必填。
  同时新增 `仓库根/excluded_metrics.json` 同源副本。
  **新增 `tests/unit/test_excluded_metrics.py`**：断言清单 14 条、`reason` 分布 11/3、
  每条 `duplicate` 都有 `kept`、包内与仓库根两份内容一致。
  完成后：清单内容与客户指标库的 11 组重复 + 3 条无法提供**逐一对应**；
  证明：`pytest tests/unit/test_excluded_metrics.py -q`。

- [ ] **1.2 生成器按清单剔除**：改 `tools/build_itscada_mapping.py`，
  生成 `包内/metric_rules.json` 与 `包内/customer_metric_mapping.json` 时
  跳过排除清单里的指标码。**在 `tests/unit/test_excluded_metrics.py` 里补断言**：
  重生成后规则数 = 87、`enabled=false` 的规则数 = 0、`metric_code` 无重复。
  完成后：生效口径 = 87 条；证明：`pytest tests/unit/test_excluded_metrics.py -q`。

- [ ] **1.3 映射层加载与校验排除清单**：改 `tpops_kafka_collector/mapping/loader.py`
  （加载排除清单）与 `tpops_kafka_collector/mapping/validation.py`
  （启动期校验），校验三项：排除清单的指标码**一个都不在**生效映射表中、
  生效规则 `metric_code` 唯一、生效规则数 = 87。
  **新增 `tests/unit/test_metric_catalog.py`**：覆盖上述三项的正例与反例
  （人为塞一条被排除的指标码 → 校验必须失败并打印差异）。
  完成后：不满足即启动失败并打印差异；
  证明：`pytest tests/unit/test_metric_catalog.py -q`。

- [ ] **1.4 支持点号组合的维度绑定**：改 `tpops_kafka_collector/mapping/rules_context.py`
  的 `row_dimensions` 与 `grouped_items`——`dimension_bindings` 的值支持
  「点号连接的多个列名」（按 `.` 连接各列的值），`group_by` 支持多列分组。
  **新增 `tests/unit/test_dimension_binding.py`**：覆盖组合绑定（`schemaname.relname`
  → `public.t1`）、多列分组、单列绑定的既有行为不回归。
  完成后：`{"NAME": "schemaname.relname"}` 绑出 `public.t1`；
  证明：`pytest tests/unit/test_dimension_binding.py -q`。

- [ ] **1.5 生成器与规则改绑 schema+relname**：改 `tools/build_itscada_mapping.py` 的
  `DIMENSION_COLUMN_BY_SQL`（`'stat_user_tables': {'NAME': 'relname'}` →
  `'schemaname.relname'`）**并**改其 `group_by` 派生逻辑（点号串要拆成多列，
  否则 `group_by` 会变成一个叫 `schemaname.relname` 的单列）；
  重生成后 `包内/metric_rules.json` 里 11 条表级规则的
  `dimension_bindings` = `{"NAME": "schemaname.relname"}`、`group_by` = `["schemaname","relname"]`。
  **在 `tests/unit/test_metric_catalog.py` 里补断言**：表级规则一律按两列分组。
  完成后：表级规则的 `NAME` 绑定为 `schemaname.relname`、按两列分组；
  证明：`pytest tests/unit/test_metric_catalog.py -q`。

- [ ] **1.6 表级幂等不碰撞**：**在 `tests/unit/test_dimension_binding.py` 里补用例**，
  断言 `public.t1` 与 `other.t1` 的同一表级指标得到**不同**的 `metricName` /
  `metricCode` / 幂等键，而同一张表重复采集得到**相同**的幂等键。
  完成后：两条消息都会被投递；证明：`pytest tests/unit/test_dimension_binding.py -q`。

- [ ] **1.7 告警统一指标名**：在 `包内/alarm_rules.json` 增加
  `event_metric`（`metric_code` / `metric_name` 固定值），改
  `tools/build_itscada_mapping.py` 让**未挂客户指标**的事件类告警统一取该值，
  挂靠了客户指标的告警保持不变；并在 `包内/open_items.json` 登记该统一值待客户确认。
  同时**放宽启动期校验**：`metric_code` 唯一性**只对指标规则要求**，对告警定义不要求
  （告警定义靠 `alarm_id` 唯一定位）。
  **新增 `tests/unit/test_alarm_metric_unification.py`**：断言 254 条事件类告警的
  `metric_code` 相同、`alarm_name` 各不相同；3 条挂靠客户指标的告警的
  `metric_code` 仍等于该客户指标；告警定义不触发唯一性校验失败。
  完成后：254 条事件类告警 `metricCode` 相同、`alarmName` 各不相同；
  证明：`pytest tests/unit/test_alarm_metric_unification.py -q`。

- [ ] **1.8 交付文档重生成**：跑 `tools/build_delivery_docs.py` 重生成
  `docs/交付-指标口径说明.md`（87 条）、`docs/交付-不可提供指标清单.md`
  （3 条，标注「已从生效口径移除」+ 理由）、`docs/交付-客户指标英文名核对清单.md`；
  同步更新 `docs/映射表-ITSCADA字段对照.md` 与 `tpops_kafka_collector/mapping/README.md`。
  **新增 `tests/unit/test_delivery_docs.py`**：断言三份交付文档里的条数与
  生效映射表一致（87 / 3 / —），防止文档与代码漂移。
  完成后：文档条数与生效口径一致；证明：`pytest tests/unit/test_delivery_docs.py -q`。

### 批次 2 · 日志框架

- [ ] **2.1 loguru 依赖落地**：在 `requirements-runtime.txt` 加 `loguru==0.8.0`（锁版本）；
  改 `tools/build_deploy_bundle.py` 的离线 wheel 清单，把 loguru 纳入部署包；
  确认它是 `py3-none-any` 且在 Linux + py3.7 下无传递依赖。
  **新增 `tests/unit/test_loguru_dependency.py`**：断言部署包清单含 loguru、
  其 wheel 为纯 Python（`*-none-any`）、`requirements-runtime.txt` 锁了版本。
  完成后：目标机只装 4 个纯 Python 包即可跑；证明：`pytest tests/unit/test_loguru_dependency.py -q`。

- [ ] **2.2 `logging_setup.py` 重写为 loguru sink 装配**：改
  `tpops_kafka_collector/logging_setup.py`，装配：
  - 总日志 sink（生命周期 + 整体汇总 + 每通道简洁汇总 + WARNING 以上）；
  - 5 个通道 sink，`filter=` 读 `record["extra"]["channel"]`；
  - stderr sink（前台/调试可见）；
  - `rotation=` 自定义回调（跨天 or 当日文件超 `LOG_MAX_BYTES`）、
    `retention=` 保留 `LOG_RETENTION_DAYS` 天、文件名 `<基名>-{time:YYYY-MM-DD}.log`；
  - JSON sink 用 `format=` **自定义函数**产出既有 schema（`timestamp`/`level`/`logger`/
    `thread`/`message` + `k=v` 摊平 + `exception`）；
  - 官方 `InterceptHandler` 把第三方库 stdlib 日志转发进 loguru；
    `logger.disable("kafka")` 等降噪；
  - 保留现有 `setup_logging(level, log_format, stream)` 签名与幂等语义
    （py37 无 `basicConfig(force=…)`，自判 `root.handlers`）。
  **新增 `tests/unit/test_logging_files.py`**：覆盖文件名带日期、跨天切分、
  单日超限切分（旧文件改名 + 新文件续写）、过期文件被删而近 3 天保留、
  通道文件只含本通道记录、总日志含各通道简洁汇总、
  通道 INFO 不进总日志而 ERROR 两边都有、日志目录不可写时报错、
  既有 `setup_logging` 调用不回归、重复调用不叠加 sink、json 的 `exception` 字段保留。
  完成后：写日志时按日期命名、按天切分、过期自动清理、通道不串味；
  证明：`pytest tests/unit/test_logging_files.py -q` 与既有
  `pytest tests/unit/test_logging_setup.py -q`。

- [ ] **2.3 全面改用 loguru API**：把生产包内各模块的
  `LOGGER = logging.getLogger(__name__)` 改为 `from loguru import logger`
  （**调用点 `LOGGER.info(...)` 本身不动**）；
  5 个采集器模块各加一行 `LOGGER = logger.bind(channel="<通道名>")`
  （`monitor` / `command_center` / `diagnostic` / `instance` / `alarm`）；
  非采集器模块保持无 `channel` 绑定（其记录只进总日志）。
  **新增 `tests/unit/test_loguru_migration.py`**：断言生产包内不再出现
  `logging.getLogger`（第三方库转发用的 `InterceptHandler` 除外），
  且 5 个采集器模块的记录带上正确的 `extra["channel"]`。
  完成后：全项目日志走 loguru；证明：`pytest tests/unit/test_loguru_migration.py -q`。

- [ ] **2.4 配置项与快速失败**：改 `tpops_kafka_collector/config.py`，加 `LOG_DIR` /
  `LOG_FORMAT` / `LOG_MAX_BYTES`（默认 50 MB）/ `LOG_RETENTION_DAYS`（默认 3）；
  取值非法 → 启动失败退出码 2；日志目录不可写 → 启动失败并说明路径；
  `LOG_DIR` 未设置 → 只写 stderr（不落盘）；同步 `.env.example`。
  **新增 `tests/unit/test_config.py`**（若无同名文件则新建）：覆盖非法取值、
  未设置 `LOG_DIR` 的默认行为、合法取值的加载。
  完成后：非法取值与不可写目录都在启动期失败；
  证明：`pytest tests/unit/test_config.py -q`。

- [ ] **2.5 通道级每轮统计**：新增不可变 DTO `ChannelRoundStats`
  （`tpops_kafka_collector/scheduler_types.py`）与聚合逻辑
  **新文件 `tpops_kafka_collector/channel_stats.py`**（按通道聚合
  `fetched` / `mapped` / `produced` / `failed` / `windows` / `calls` / `elapsed` / `instances`）；
  `scheduler.py` 只负责在 tick 里调用，SHALL NOT 把聚合逻辑堆进 `scheduler.py`
  （该文件已 392 行）。
  **新增 `tests/unit/test_channel_round_stats.py`**：覆盖按通道聚合的正确性、
  各通道计数之和 == 整体行同名字段。
  完成后：通道统计与整体统计自洽；
  证明：`pytest tests/unit/test_channel_round_stats.py -q`。

- [ ] **2.6 通道级汇总行输出**：改 `tpops_kafka_collector/scheduler_report.py`，
  每轮为**到期且完成**的通道各输出一条汇总行（用 `logger.bind(channel=…)` 打出，
  进通道日志），同时在总日志输出一条同字段的核心计数；
  字段固定为 `channel/tick/instances/fetched/mapped/produced/failed/windows/calls/elapsed/delivery_rate`。
  **在 `tests/unit/test_channel_round_stats.py` 里补断言**：未到期通道不出行；
  json 模式下这些是**顶层字段**；通道行各字段之和 == 整体行。
  完成后：未到期通道不出行；json 模式下字段可直接被日志平台取；
  证明：`pytest tests/unit/test_channel_round_stats.py -q`。

- [ ] **2.7 敏感信息不进落盘日志**：跑一轮采集后扫描日志文件，确认无口令 /
  mini-token / `sql_text` 全文；必要时收紧采集器里的日志语句。
  **新增 `tests/integration/test_logs_have_no_secrets.py`**：跑一轮后
  grep `<LOG_DIR>` 下全部文件，断言不含配置的凭据值与 token 片段。
  完成后：落盘文件不含敏感串；
  证明：`pytest tests/integration/test_logs_have_no_secrets.py -q`。

### 批次 3 · 部署与文档

- [ ] **3.1 `collector.sh` 后台化**：把 `run` / `status` / `stop` 三段拆到新文件
  `deploy/collector_runtime.sh`（`collector.sh` 位置参数分发，对外仍是一个脚本）；
  `run` 走 `setsid nohup` 后台启动、写 `<安装目录>/run/collector.pid`、
  启动后确认存活（**没起来则返回非零并打印日志尾部**：`<LOG_DIR>/collector.log`
  与 `<安装目录>/run/console.out` 的最后 N 行）、已有实例在跑则拒绝启动；
  `run --foreground` 保持前台；`stop` 发 SIGTERM 并清理 PID 文件；
  `<安装目录>/run/` 目录由脚本创建。
  **新增 `tests/scripts/test_collector_runtime.sh`**：覆盖后台启动后进程仍在、
  重复启动返回非零、`status` 报告正确、`stop` 后进程退出且 PID 文件被清理、
  `--foreground` 前台模式不写 PID 文件。
  完成后：脚本单文件 < 400 行、重复启动返回非零；
  证明：`bash tests/scripts/test_collector_runtime.sh`。

- [ ] **3.2 systemd 与部署说明同步**：更新 `docs/交付-部署运行说明.md` 的 unit 片段为
  `ExecStart=/opt/tpops-collector/collector.sh run --foreground`，补 `LOG_DIR` 等新环境变量表、
  日志目录说明与磁盘占用上限（6 基名 × 3 天 × 50 MB）、`<安装目录>` 的定义；
  更新 `deploy/README.md` 的子命令表。
  **在 `tests/unit/test_delivery_docs.py` 里补断言**：交付说明里的 `ExecStart`
  含 `run --foreground`、环境变量表含 `LOG_DIR`/`LOG_MAX_BYTES`/`LOG_RETENTION_DAYS`/`LOG_PATTERN`。
  完成后：文档里的命令与脚本实际行为一致；
  证明：`pytest tests/unit/test_delivery_docs.py -q`。

- [ ] **3.3 日志框架规范成文**：把决策 1~6（布局与日期文件名、sink 分层、loguru 原生格式、
  按天切分与 3 天保留、汇总字段口径、后台运行）写成 `docs/日志框架规范.md`，
  与 `design.md` 的决策段一致；并同步更新 `CLAUDE.md` 的编码约定
  （「用 `logging`」→ 「用 `loguru`」）与「运行时依赖三个纯 Python 包」的表述。
  **新增 `tests/unit/test_logging_doc.py`**：校验文档里的字段名与
  `tpops_kafka_collector/scheduler_report.py` 的常量一致、文件布局与
  `tpops_kafka_collector/logging_files.py` 一致。
  完成后：文档覆盖目录布局、字段表、示例行；
  证明：`pytest tests/unit/test_logging_doc.py -q`。

- [ ] **3.4 全量回归与门禁**：跑

  ```bash
  conda run -n tpops python -m pytest -q
  conda run -n tpops python -m pytest --cov=tpops_kafka_collector --cov-report=term-missing -q
  conda run -n tpops python -m pytest tests/test_py37_compat.py -q
  conda run -n tpops python -m ruff check tpops_kafka_collector tests
  ```

  覆盖率门槛：**全包 ≥ 80%**、**映射层 ≥ 90%**（口径与 `proposal.md` 一致）。
  完成后：全绿、覆盖率达标、py37 兼容门禁通过、ruff 无告警；
  证明：上述四条命令的输出。

## 实施备注（仅在必要时）

- **表级指标的对外命名**：批次 1 完成后，表级指标的 `metricCode` 变成「每表一个」
  （如 `gaussdb.public.t1.number.Of.insert.tuples`），`metricName` 变成
  `(public.t1)插入的行数`。客户 ITSCADA 按 `metricName` 匹配台账，
  **表级指标的台账项要按实际库表名录入** —— 3.2 必须写清这一点。
- **告警统一值是待确认项**：`gaussdb.alarm` / `数据库告警` 是拟值，
  已登记进 `open_items.json`；客户若给不同的名字，改映射数据即可，不改代码。
- **不要改客户 xlsx**：排除清单是生效口径的唯一落点（决策 7）。
- **字段名不新造**：通道汇总行复用 `TickResult` 既有字段名
  （`fetched` / `mapped` / `produced` / `failed` / `windows` / `calls`），
  新增的只有 `channel` / `instances` / `elapsed` / `delivery_rate`。
- **`scheduler.py` 已 392 行**，加通道聚合会超 400 行 → 聚合逻辑放新文件
  `tpops_kafka_collector/channel_stats.py`，输出放 `scheduler_report.py`，
  `scheduler.py` 只留编排（任务 2.4）。
- **`ruff` 只扫 Python**：`deploy/` 下是 shell 脚本，不进 ruff 的检查范围。
