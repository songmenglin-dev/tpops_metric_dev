# 归档与交付摘要 · tpops-mapping-and-logging

> 交付形态沿用上个变更（`tpops-kafka-collector`）的四件套：
> **97 个可提供指标 → 87 个**、不可提供清单、英文名核对清单、部署运行说明。
> 本次新增 **`docs/日志框架规范.md`**（日志框架的对外规范）。

## 1. 这次改了什么（面向客户/运维）

| 面向 | 变化 |
|---|---|
| **客户台账** | 指标清单从 **101 条收敛到 87 条**：删掉 11 条重复的表级指标（同一指标在客户指标库里有两行，只因英文码大小写前缀不同）+ 3 条平台侧拿不到的指标。**客户台账要按 87 条重录** |
| **客户台账（表级）** | 表级指标的指标名现在带库表前缀：`({#NAME})插入的行数` → `(public.t1)插入的行数`。**不同 schema 的同名表不再撞成同一个指标**（此前会有一条被幂等去重静默丢弃） |
| **客户台账（告警）** | 不涉及指标名的事件类告警，`metricName` / `metricCode` **统一成一个**（现值 `gaussdb.alarm` / `数据库告警`）。告警台账从 254 项变 1 项；告警种类靠 `alarmId` / `alarmName` / `alarmContent` 区分 |
| **运维** | 日志**落盘**了：`collector.sh run` 改后台运行，`logs/` 下 6 个基名 × 日期后缀（总日志 + 5 个采集器通道日志），按天切分、保留 3 天；每类采集器每轮一行汇总（获取量/已推送量/耗时…） |
| **运维** | 新增 `collector.sh status` / `stop`；systemd 的 `ExecStart` 改 `run --foreground` |

## 2. 交付物

| 文件 | 内容 |
|---|---|
| `docs/交付-指标口径说明.md` | **87** 条可提供指标逐条口径（表驱动重生成） |
| `docs/交付-不可提供指标清单.md` | **3** 条无法提供指标 + 理由 + 「已从生效口径移除」标注 |
| `docs/交付-客户指标英文名核对清单.md` | OCR 疑似错字清单（给客户核对） |
| `docs/交付-部署运行说明.md` | 部署四步 + systemd 单元 + 环境变量表 + 日志章节 + 排障表 |
| **`docs/日志框架规范.md`**（新） | 日志框架规范：目录布局、切分与保留、总日志/通道日志分工、格式、11 字段汇总表、启动方式、排障 |
| `docs/映射表-ITSCADA字段对照.md` | 两张字段级映射表（原始字段 → ITSCADA 客户字段） |

## 3. 对外契约变化（**需客户确认/重录**）

1. **指标清单 101 → 87**：11 条重复项 + 3 条无法提供项已移除。
   重复项的保留版本（中文名正常的那条）**保留在 87 条里**，客户台账要按保留版本录。
2. **表级指标的指标名含库表前缀**：`metricName` = `(schema.table)中文名`，
   `metricCode` = `gaussdb.schema.table.…`。客户按 `metricName` 匹配台账，
   **表级指标的台账项要按实际库表名录**。
3. **告警统一指标名**：`metricName` / `metricCode` 都统一为一个值。
   统一值是**表驱动**的（`alarm_rules.json` 的 `event_metric` 段），客户若要改
   只需改数据文件，不改代码。该值已登记进 `open_items.json` 待客户确认。

## 4. 验证结论

| 维度 | 结论 | 证据 |
|---|---|---|
| Completeness | **PASS** | 19 条需求（R1~R19）全部映射到 4 个波次并有测试；4/4 波次 receipt = pass；19 个任务全部勾选 |
| Correctness | **PASS** | 全量 pytest **1945 passed**；覆盖率 **TOTAL 95%**（映射层 98~100%）；py37 兼容门禁 **179 passed**；ruff 生产包 **0 违规**、全仓新违规类别 **0**；部署脚本 shell 测试 **18 项全绿** |
| Coherence | **PASS** | 数量口径（101/87/14/11/3/254/6/11 字段）在 proposal/specs/design/tasks/交付文档五处一致；主规格已合并（+452 行） |
| Scope | **PASS** | 未新增未声明的依赖（只加了 `loguru==0.8.0`，纯 Python、零传递依赖）；未改客户 xlsx；未碰 wave 之外的交付面 |

### 本次审查发现并修掉的问题（留档）

1. **wave-1 Important（已修）**：生成器 `_load_event_metric()` 在「数据文件存在但
   `event_metric` 段缺失/文件损坏」时**静默回落**硬编码默认值 → 会把运维改过的
   统一口径悄悄改回去。已改为**报错停下**，补 4 条测试（RED→GREEN 实测）。
2. **规格自纠**：原写「254 条告警 `alarm_name` 各不相同」**不成立**（平台目录 13 行填的是
   等级词、另有 2 组同名不同 ID，实测 241 个不同名）→ 唯一性由 `alarm_id`（254 个互不相同）
   承载。**改的是规格，不是让实现迁就错的规格**。
3. **wave-3 返工（已修）**：3.1 原本把 run/status/stop 拆到 `deploy/collector_runtime.sh`，
   违反项目守卫「deploy/ 下只能有一个 `.sh`」（客户侧只拿一个脚本）→ 撤回拆分。
4. **ruff 新类别（已清）**：`tools/excluded_metrics.py` 的 UP024、`tests/unit/test_logging_doc.py`
   的 F401、生产包 `validation.py` 的 2 条 UP031 → 生产包 ruff 现在 **0 违规**。

### 已知遗留（不阻塞交付）

- **仓库历史 lint 债 683 条**：属上个变更已登记的另案范围，本次未新增类别。
- **`deploy/collector.sh` 1194 行**：项目守卫要求只有一个 `.sh`，且「单文件 < 400 行」
  的约束针对**采集端代码**而非部署脚本 —— 两个约束一起看，单文件是唯一可行解。
- **stderr sink 与总日志同内容** → 开发态前台看不到通道 INFO 明细（明细在通道日志文件里）。
- **8 项 `open_items.json` 待客户确认**（含本次的告警统一指标名初值）。

## 5. 审查执行方式的披露

本次变更的**实现**由 subagent 完成 wave-1、wave-2；wave-3、wave-4 由主会话完成。
**审查**：独立 reviewer 子代理在本会话因 **API 配额/网络错误**（429 Token Plan 上限、
EAI_AGAIN）连续失败 4 次，**四份审查报告（wave-1 两轮 + wave-2 + wave-3 + wave-4）
全部由控制器（主会话）执行**。判级口径与证据要求不变（每条 finding 带 file:line、
RED→GREEN 实测证据、逐条拍板实现者提出的问题），但**独立性弱于独立代理审查**，
如实记录在此。

## 6. 归档

- 变更目录：`changes/tpops-mapping-and-logging/`
- 工件：proposal / 3 份 delta spec / design / tasks / execution-contract
- 执行：`sdd`，plan revision 2，4 个波次，4 份 pass receipt
- 审查报告：`reviews/wave-1.md`、`reviews/wave-1-rereview.md`、`reviews/wave-2.md`、
  `reviews/wave-3.md`、`reviews/wave-4.md`
- 主规格：已合并进仓库 `specs/`（3 个能力）
- 实现分支：`tpops_metric_dev`（worktree）
