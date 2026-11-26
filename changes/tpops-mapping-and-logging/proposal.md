# 变更提案：指标清理、日志框架、字段映射修正

## 背景（Why）

在 `tpops-kafka-collector` 变更交付（2026-11-24）之后复核客户指标库与运行期行为，
发现三个**已观察到**的问题：

1. **表级指标重复投递**。客户指标库「指标库」sheet 共 101 行，其中 **11 个表级指标
   各出现两次**：一次在「表信息」分类（中文名正常），一次在「事务信息」分类
   （中文名是英文），两条英文名仅大小写前缀不同（`gaussdb.…` vs `GAUSSDB.…`）。
   两行都被 `metric_rules.json` 收录且 `enabled=true` → 运行期对**同一张表产出两条
   不同指标消息**，ITSCADA 侧看到重复指标。
2. **日志只到 stderr，且只有全局一轮汇总**。`logging_setup.py` 只装了一个
   `StreamHandler(sys.stderr)`，`collector.sh run` 以前台启动，**日志不落盘**；
   `scheduler_report.py` 每轮只给一行 tick 整体口径，**看不到通道内部**——
   哪类采集器拉了多少、推了多少、耗时多久，只能靠猜。目标机是麒麟 V10 离线环境，
   无 logrotate。
3. **两个字段映射缺陷**。告警侧：257 条平台告警里 254 条事件类告警的 `metricCode`
   是自造的 `gaussdb.alarm.<alarm_id>`，同一条规则复制了 254 份，而告警内容
   并不涉及客户指标名。表级指标侧：`{#NAME}` 绑到 `relname`、`group_by` 也是
   `["relname"]` → **不同 schema 的同名表**得到**完全相同**的 `metricName` /
   `metricCode` / 幂等键，后一条被幂等去重**静默丢弃**。

**为什么现在**：交付死线 **2026-12-15**。重复指标与表级碰撞会**直接进客户 ITSCADA
台账**（按 `metricName` + `resourceName` 匹配），交付后再改要客户重录台账；
日志须在 P5 联调**前**落盘，否则联调时无法核对「哪类采集器推了多少、漏了多少」。

## 变更内容（What Changes）

- **指标更新**：交付口径从 101 条收敛到 **87 条**——
  删除 11 条重复的表级指标「英文变体」（保留「表信息」分类的中文名版本），
  删除 3 条无法提供的指标（`gaussdb.Fastpath_function_call`、`gaussdb.Disabled_conn`、
  `gaussdb.sendAgentName`）。删除清单与理由落进**新的映射数据文件**
  `tpops_kafka_collector/mapping/data/excluded_metrics.json`（表驱动，不写死在代码里），
  仓库根 `data/mapping/` 放同源副本，客户指标库 xlsx 原件保持不改。
  交付文档（指标口径说明 / 不可提供指标清单）随之重生成。
  （注意区分两个「14」：**排除清单共 14 条** = 11 重复 + 3 无法提供；
  另有 **14 条独立指标的中文名整体是英文**（`GAUSSDB.Buffers_Alloc` 等）——它们**不重复**，
  本次**保留**。）
- **日志框架**：产出**日志框架规范与格式**（本文档 + `design.md` 的决策段），
  并按该规范实现——
  - 每个采集器通道（`monitor` / `command_center` / `diagnostic` / `instance` / `alarm`）
    在**每轮结束时**输出一条**该通道自己的**汇总行：获取数据量、已推送数据量、
    耗时、失败数、窗口数、外部调用数；
  - 日志**落盘**：`collector.sh run` 改为**后台运行**（写 PID 文件），
    日志目录可配置，**总日志**（生命周期 + 每轮整体汇总 + 错误）**总览各采集器
    的采集推送情况**，**各采集器另有自己的日志文件**（明细 + 该通道汇总）；
  - 日志**文件名带日期后缀**（`collector-2026-11-26.log`、
    `collector-monitor-2026-11-26.log`），**按天切分**，
    **过期 3 天的文件自动删除**；
  - 日志框架换成 **loguru 0.8.0**（纯 Python、Python 3.7.9 可用、
    Linux+py3.7 下零传递依赖），**直接用 loguru 原生格式**，
    不自研 handler / 轮转 / 保留 / 格式化；项目内全面改用 loguru API；
  - 保留 `collector.sh run --foreground` 前台模式（systemd 单元用这个）。
- **字段映射**：
  - 告警：**不涉及客户指标名**的事件类告警，`metricName` 与 `metricCode`
    **统一为一个固定名**（拟 `gaussdb.alarm` / `数据库告警`，标记为待客户确认）；
    告警种类靠 `alarmId` / `alarmName` / `alarmContent` 区分。
  - 表级指标：`{#NAME}` 改由 **`schemaname.relname`** 填充
    （即 `schema_name.table_name`），分组改为按 `schemaname + relname`，
    不同 schema 的同名表得到不同的 `metricName` / `metricCode` / 幂等键。

## 范围（Scope）

### 范围内（In Scope）

- 指标清理：14 条（11 重复 + 3 无法提供）的剔除与理由留档；映射数据重生成；
  交付文档重生成；运行期「不可提供指标丢弃计数」归零。
- 日志框架：日志目录/文件布局、格式（text + json）、轮转策略、总日志与分日志的分工；
  每通道每轮汇总行；`collector.sh run` 后台化 + PID 文件 + `status` / `stop`；
  `collector.sh run --foreground` 前台模式；日志相关环境变量。
- 字段映射：告警统一指标名；表级 `schema_name.table_name` 替换；
  幂等键随维度修正而不再跨 schema 碰撞。
- 相应的单元测试、集成测试与文档同步（`docs/交付-*.md`、
  `docs/映射表-ITSCADA字段对照.md`、`deploy/README.md`）。

### 范围外（Out of Scope）

- 客户 Kafka 地址 / Topic 的 P5 联调（等客户提供，仍在原变更之外）。
- 客户指标库 xlsx 原件的改动（客户文件，删除只落在本项目的生效口径与排除清单）。
- 14 条「中文名是英文」的**独立**指标（如 `GAUSSDB.Buffers_Alloc`、
  `GAUSSDB.Idle_connections`）——经确认本次**保留**（用户明确选择「只删 11 组重复 + 3」）。
- 其余 8 项 `open_items.json` 待客户确认项的结论。
- 日志采集与外发（shipper / Fluent Bit 等）、日志平台接入 —— 本次只做**本地落盘**。
- systemd 单元文件的正式交付（仍由客户按 `docs/交付-部署运行说明.md` 的参考模板自建，
  本次只把 `ExecStart` 改成 `run --foreground` 并更新说明）。

## 影响与验证

- **影响区域**
  - 代码：`tpops_kafka_collector/logging_setup.py`（重写为 loguru sink 装配）、
    **约 30 个模块的 logger 获取方式**（改 loguru API）、
    新增 `tpops_kafka_collector/channel_stats.py`（通道级统计）、
    `tpops_kafka_collector/scheduler_report.py`、`tpops_kafka_collector/scheduler.py`、
    `tpops_kafka_collector/scheduler_types.py`、`tpops_kafka_collector/scheduler_delivery.py`、
    `tpops_kafka_collector/config.py`、`tpops_kafka_collector/mapping/rules_context.py`、
    `tpops_kafka_collector/mapping/message.py`、`tpops_kafka_collector/mapping/table.py`、
    `tpops_kafka_collector/mapping/loader.py`；
  - 映射数据：`tpops_kafka_collector/mapping/data/metric_rules.json`、
    `tpops_kafka_collector/mapping/data/customer_metric_mapping.json`、
    `tpops_kafka_collector/mapping/data/alarm_rules.json`、
    **新增** `tpops_kafka_collector/mapping/data/excluded_metrics.json`，
    以及仓库根 `data/mapping/` 的同源副本（运行期只读包内那份）；
  - 部署：`deploy/collector.sh`（`run` 后台化、新增 `status` / `stop`）、
    `.env.example`（新增日志目录/轮转变量）；
  - 离线工具：`tools/build_itscada_mapping.py`（排除清单）、
    `tools/build_delivery_docs.py`（交付文档重生成）；
  - 文档：`docs/交付-*.md`、`docs/映射表-ITSCADA字段对照.md`、`deploy/README.md`。
- **完成证明**
  - `conda run -n tpops python -m pytest -q` 全绿；
    `conda run -n tpops python -m pytest --cov=tpops_kafka_collector --cov-report=term-missing -q`
    覆盖率**全包 ≥ 80%**、**映射层 ≥ 90%**；
  - `tpops_kafka_collector/mapping/data/metric_rules.json` 恰好 **87** 条规则、
    **0** 条 `enabled=false`、`metric_code` 无重复；
  - `tpops_kafka_collector/mapping/data/excluded_metrics.json` 恰好 **14** 条
    （11 条 `duplicate` + 3 条 `unsupported`），每条带理由；重复项另带「保留哪一条」；
  - 告警定义中，不挂客户指标的告警 `metricCode` 全部等于同一个固定值；
  - 表级指标的 `tags.dimension_values.NAME` 形如 `public.t1`，
    `public.t1` 与 `other.t1` 的 `metricCode` 与幂等键**不相同**；
  - `bash tests/scripts/test_collector_runtime.sh` 全绿（覆盖后台启动、重复启动拒绝、
    `status`、`stop`、前台模式）；
  - 在**已安装形态**的目标目录里执行 `bash collector.sh run` 后返回 0，
    PID 文件存在、进程仍在后台运行，`<LOG_DIR>` 下出现 1 个总日志 + 5 个通道日志文件，
    总日志里能看到各通道的推送情况；每个通道日志里能看到该通道每轮的
    `fetched/mapped/produced/failed/elapsed` 汇总行。
