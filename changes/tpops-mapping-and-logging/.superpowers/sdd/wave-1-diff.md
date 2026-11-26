# Wave 1 审查包 · wave-1-mapping

- Base: `32ed768`  - Head: `32d74ce`

## 提交列表
32d74ce feat(mapping): 生效口径收敛到 87 条、表级维度改 schema.table、告警统一指标名

## 变更统计（全量）
 tpops_kafka_collector/mapping/loader.py            |    66 +-
 tpops_kafka_collector/mapping/rules_context.py     |    73 +-
 tpops_kafka_collector/mapping/table.py             |    29 +
 tpops_kafka_collector/mapping/validation.py        |    86 +-
 74 files changed, 10453 insertions(+), 43999 deletions(-)

## 完整 diff（代码 / 测试 / 工具 / 映射层代码与小型数据文件）

> **机械重生成**的 JSON 数据文件不在完整 diff 里（内容是重跑生成器的结果，非手工编辑）：
> `metric_rules.json`、`customer_metric_mapping.json`、`alarm_rules.json`、
> `response_rules.json`、`kafka_field_mapping.json`、`expected_values.json`、
> `data/mapping/**`、三份交付文档。它们的**前后结构摘要**见下一节 ——
> 逐行核对请自行 `git diff 32ed768..32d74ce -- <path>`。

diff --git a/changes/tpops-mapping-and-logging/.spec-superflow.yaml b/changes/tpops-mapping-and-logging/.spec-superflow.yaml
new file mode 100644
index 0000000..439777c
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/.spec-superflow.yaml
@@ -0,0 +1,48 @@
+# .spec-superflow.yaml — lightweight state machine
+# Derived data. Always rebuildable from artifacts. Lost/corrupt → fall back to content-level detection.
+
+# === Core state ===
+state: approved-for-build
+workflow: full
+revision: 1
+
+# === Hashes (fast staleness detection) ===
+artifacts_hash: sha256:7cc1e176f66c39a9ea059f98ea65d100a606d8caae35565b932fc67a41ce126e
+contract_hash: sha256:f7b75d5a6042029294770105c57bad5938f3e61c8025444c2d42217d9ad1b3c0
+
+# === Execution progress ===
+execution_mode: sdd
+execution_plan_hash: sha256:0442fd8c548ce504ec11362176ca651ab57510dd58a370e2d645ecac34960d4a
+execution_plan_revision: 1
+batches_completed: 0
+test_result: null
+spec_merged: false
+spec_publication_receipt: null
+
+# === Metadata ===
+change_name: tpops-mapping-and-logging
+last_transition: 2026-11-26T03:16:10.065Z
+last_transition_from: bridging
+last_transition_to: approved-for-build
+
+# === Decision points ===
+dp_0_decisions: 变更名=tpops-mapping-and-logging；意图=在既有 tpops_kafka_collector 采集端上做三件事：(1) 指标更新——删除客户指标库中 11 组表级重复指标的英文变体（保留「表信息」分类中文名版本）与 3 条无法提供指标，101→87 条；(2) 日志优化——每类采集器每轮打印本轮总结（获取量/已推送量/耗时等），collector.sh run 改后台运行、日志落盘，拆「总日志（简洁汇总各采集器采集推送情况）」+「各采集器自己的采集推送日志」，并产出日志框架规范与格式供用户审核；(3) 字段映射——告警不涉及指标名时 metricName 与 metricCode 统一为一个固定名（拟 gaussdb.alarm / 「数据库告警」，待客户确认）；表级指标用 schema_name.table_name 替换表名占位符，避免不同 schema 同名表幂等碰撞。范围=三条工作流并行，均在本 change 内；不含客户 Kafka 地址/Topic 联调（P5，仍在变更外）、不含 8 项 open_items 待客户确认项的结论。约束=Python 3.7.9 语法红线；依赖锁 kafka-python==2.0.2 requests>=2.25,<3.2；采集端单文件<400 行、函数<50 行；凭据只走环境变量、禁硬编码/写日志/入库；DTO 用 frozen dataclass 不可变；错误显式包装带上下文、logging 不用 print、不静默吞异常；TPOPS 接口红线（两套命名空间、mini-token 认证失败不重试防锁号、verify_tls=False、指标窗口≥1h、end_time 回退 5 分钟、is_latest_point_only=false、12 分组码硬编码、告警 limit≤100 分页且历史仅 30 天需显式告警、命令中心 statistics.running==0 判结束、result 二次 json.loads、只在 master 执行、GaussDB 方言无 FILTER）；映射层表驱动不写死；日志不得落凭据/mini-token/业务 SQL 文本。artifact_language=zh。workflow_path=full（用户显式选择，与推荐一致）。沟通=我起草用户审；口径变更/对外契约变更/删减范围集中提问不逐条打断。
+dp_0_result: confirmed
+dp_0_confirmed: true
+dp_0_timestamp: 2026-11-26T02:13:43Z
+dp_1_result: folded into DP-2: 需求确认（proposal.md）在 DP-2 的工件审查中一并完成 —— 通过 ssf validate 并获用户批准；本次未单列 DP-1
+dp_1_timestamp: 2026-11-26T02:56:52Z
+dp_2_result: approved (修订版2): 在修订版1基础上按用户三项指示再改日志框架 —— (c) 日志框架改用 loguru 0.8.0（已在 Python 3.7.9 实测：requires_python>=3.5、py3-none-any 纯 Python wheel、Linux+py3.7 零传递依赖、rotation/retention/filter/{time:YYYY-MM-DD} 文件名/{level:<8}/异常堆栈全部原生可用、同名冲突自动改名），不自研 handler/轮转/保留/格式化；(d) 格式直接用 loguru 原生格式，不强制 logback（用户明确「直接用loguru的日志格式即可」），JSON sink 用 loguru 的 format= 自定义函数产出既有 schema 并摊平 k=v；(e) 全面改用 loguru API（from loguru import logger，调用点 LOGGER.info(...) 不动，5 个采集器模块各加 LOGGER=logger.bind(channel=...)），第三方库 stdlib 日志经官方 InterceptHandler 转发并 logger.disable 降噪。据此删除原自研的 logging_pattern.py 任务与 logging_files.py，改为 logging_setup.py 重写为 loguru sink 装配；新增 loguru 依赖落地任务(requirements-runtime.txt + 离线部署包 + CLAUDE.md 约定更新)。新增依赖不破坏「x86_64/aarch64 共用一份部署包」。修订后 proposal/specs/design/tasks 全部重新通过 ssf validate
+dp_2_timestamp: 2026-11-26T03:08:28Z
+dp_3_result: approved: 执行合同已批准 —— Intent Lock（三条已观察事实 + 范围内外）、19 条已批准需求（R1~R19，含关键场景与 8 条机械可判定的验收检查）、Design Constraints（架构/接口/依赖/数据 + TPOPS 接口红线 + py3.7.9 红线 + 依赖锁含新增 loguru==0.8.0）、4 个执行波次（wave-1-mapping 映射口径 / wave-2-logging 日志框架 / wave-3-deploy 部署 / wave-4-verification 门禁，各带任务、依赖、策略、输入输出、完成标准、review report 路径与 pass receipt 要求）、Test Obligations（RED 先行行为 7 组 + 边界情况 8 组 + 回归敏感区 7 组）、需求→波次映射 19/19 无遗漏、Review Gates（4 个 receipt 全 pass 才收口）、Escalation Rules（8 条不得继续实现的情形 + 回退 specifying/bridging 的触发条件）。未映射需求=0。三处带不确定已登记且不阻塞：告警统一名初值 gaussdb.alarm/数据库告警（客户可改数据文件）、「保留 3 天」= 最近 3 个自然日含当天、表级 metricCode 每表一个后客户台账录入方式待确认
+dp_3_timestamp: 2026-11-26T03:15:46Z
+dp_4_result: sdd: plan revision 1; user-confirmed; 用户选定 sdd：4 个 wave、19 条需求、跨模块改动（映射层/日志框架/部署脚本），需显式依赖与每 wave 的 review receipt
+dp_4_timestamp: 2026-11-26T03:16:01.998Z
+dp_5_result: null
+dp_5_timestamp: null
+dp_5_decisions: null
+dp_5_confirmed: null
+dp_6_result: null
+dp_6_timestamp: null
+dp_7_result: null
+dp_7_timestamp: null
diff --git a/changes/tpops-mapping-and-logging/.superpowers/sdd/execution-plan.json b/changes/tpops-mapping-and-logging/.superpowers/sdd/execution-plan.json
new file mode 100644
index 0000000..54ceade
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/.superpowers/sdd/execution-plan.json
@@ -0,0 +1,163 @@
+{
+  "mode": "sdd",
+  "source": "user-confirmed",
+  "rationale": "用户选定 sdd：4 个 wave、19 条需求、跨模块改动（映射层/日志框架/部署脚本），需显式依赖与每 wave 的 review receipt",
+  "waves": [
+    {
+      "id": "wave-1-mapping",
+      "strategy": "serial",
+      "tasks": [
+        "1.1",
+        "1.2",
+        "1.3",
+        "1.4",
+        "1.5",
+        "1.6",
+        "1.7",
+        "1.8"
+      ],
+      "depends_on": []
+    },
+    {
+      "id": "wave-2-logging",
+      "strategy": "serial",
+      "tasks": [
+        "2.1",
+        "2.2",
+        "2.3",
+        "2.4",
+        "2.5",
+        "2.6",
+        "2.7"
+      ],
+      "depends_on": []
+    },
+    {
+      "id": "wave-3-deploy",
+      "strategy": "serial",
+      "tasks": [
+        "3.1",
+        "3.2",
+        "3.3"
+      ],
+      "depends_on": []
+    },
+    {
+      "id": "wave-4-verification",
+      "strategy": "serial",
+      "tasks": [
+        "3.4"
+      ],
+      "depends_on": []
+    }
+  ],
+  "artifacts_hash": "sha256:7cc1e176f66c39a9ea059f98ea65d100a606d8caae35565b932fc67a41ce126e",
+  "contract_hash": "sha256:f7b75d5a6042029294770105c57bad5938f3e61c8025444c2d42217d9ad1b3c0",
+  "workflow": "full",
+  "revision": 1,
+  "recommendation": {
+    "available_modes": [
+      "inline",
+      "batch-inline",
+      "sdd"
+    ],
+    "recommendation": {
+      "mode": "sdd",
+      "reasons": [
+        "The declared work spans multiple waves, so SDD keeps dependencies and review receipts explicit."
+      ]
+    },
+    "facts": {
+      "workflow": "full",
+      "documented_task_count": 19,
+      "planned_task_count": 19,
+      "planned_wave_count": 4,
+      "has_parallel_wave": false,
+      "inline_threshold": 3
+    }
+  },
+  "recommendation_receipt": {
+    "recommendation": {
+      "available_modes": [
+        "inline",
+        "batch-inline",
+        "sdd"
+      ],
+      "recommendation": {
+        "mode": "sdd",
+        "reasons": [
+          "The declared work spans multiple waves, so SDD keeps dependencies and review receipts explicit."
+        ]
+      },
+      "facts": {
+        "workflow": "full",
+        "documented_task_count": 19,
+        "planned_task_count": 19,
+        "planned_wave_count": 4,
+        "has_parallel_wave": false,
+        "inline_threshold": 3
+      }
+    },
+    "waves": [
+      {
+        "id": "wave-1-mapping",
+        "strategy": "serial",
+        "tasks": [
+          "1.1",
+          "1.2",
+          "1.3",
+          "1.4",
+          "1.5",
+          "1.6",
+          "1.7",
+          "1.8"
+        ],
+        "depends_on": []
+      },
+      {
+        "id": "wave-2-logging",
+        "strategy": "serial",
+        "tasks": [
+          "2.1",
+          "2.2",
+          "2.3",
+          "2.4",
+          "2.5",
+          "2.6",
+          "2.7"
+        ],
+        "depends_on": []
+      },
+      {
+        "id": "wave-3-deploy",
+        "strategy": "serial",
+        "tasks": [
+          "3.1",
+          "3.2",
+          "3.3"
+        ],
+        "depends_on": []
+      },
+      {
+        "id": "wave-4-verification",
+        "strategy": "serial",
+        "tasks": [
+          "3.4"
+        ],
+        "depends_on": []
+      }
+    ],
+    "artifacts_hash": "sha256:7cc1e176f66c39a9ea059f98ea65d100a606d8caae35565b932fc67a41ce126e",
+    "contract_hash": "sha256:f7b75d5a6042029294770105c57bad5938f3e61c8025444c2d42217d9ad1b3c0",
+    "workflow": "full",
+    "execution_plan_revision_at_recommendation": null,
+    "created_at": "2026-11-26T03:15:51.267Z",
+    "hash": "sha256:03681ea6940291eca5f450bd4fa62268edc92019560ddd7f8c566ad081c6295c"
+  },
+  "selection": {
+    "confirmed": true,
+    "followed_recommendation": true,
+    "acknowledged_non_recommendation": false
+  },
+  "hash": "sha256:0442fd8c548ce504ec11362176ca651ab57510dd58a370e2d645ecac34960d4a"
+}
diff --git a/changes/tpops-mapping-and-logging/.superpowers/sdd/execution-recommendation.json b/changes/tpops-mapping-and-logging/.superpowers/sdd/execution-recommendation.json
new file mode 100644
index 0000000..8803f46
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/.superpowers/sdd/execution-recommendation.json
@@ -0,0 +1,78 @@
+{
+  "recommendation": {
+    "available_modes": [
+      "inline",
+      "batch-inline",
+      "sdd"
+    ],
+    "recommendation": {
+      "mode": "sdd",
+      "reasons": [
+        "The declared work spans multiple waves, so SDD keeps dependencies and review receipts explicit."
+      ]
+    },
+    "facts": {
+      "workflow": "full",
+      "documented_task_count": 19,
+      "planned_task_count": 19,
+      "planned_wave_count": 4,
+      "has_parallel_wave": false,
+      "inline_threshold": 3
+    }
+  },
+  "waves": [
+    {
+      "id": "wave-1-mapping",
+      "strategy": "serial",
+      "tasks": [
+        "1.1",
+        "1.2",
+        "1.3",
+        "1.4",
+        "1.5",
+        "1.6",
+        "1.7",
+        "1.8"
+      ],
+      "depends_on": []
+    },
+    {
+      "id": "wave-2-logging",
+      "strategy": "serial",
+      "tasks": [
+        "2.1",
+        "2.2",
+        "2.3",
+        "2.4",
+        "2.5",
+        "2.6",
+        "2.7"
+      ],
+      "depends_on": []
+    },
+    {
+      "id": "wave-3-deploy",
+      "strategy": "serial",
+      "tasks": [
+        "3.1",
+        "3.2",
+        "3.3"
+      ],
+      "depends_on": []
+    },
+    {
+      "id": "wave-4-verification",
+      "strategy": "serial",
+      "tasks": [
+        "3.4"
+      ],
+      "depends_on": []
+    }
+  ],
+  "artifacts_hash": "sha256:7cc1e176f66c39a9ea059f98ea65d100a606d8caae35565b932fc67a41ce126e",
+  "contract_hash": "sha256:f7b75d5a6042029294770105c57bad5938f3e61c8025444c2d42217d9ad1b3c0",
+  "workflow": "full",
+  "execution_plan_revision_at_recommendation": null,
+  "created_at": "2026-11-26T03:15:51.267Z",
+  "hash": "sha256:03681ea6940291eca5f450bd4fa62268edc92019560ddd7f8c566ad081c6295c"
+}
diff --git a/changes/tpops-mapping-and-logging/.superpowers/sdd/progress.md b/changes/tpops-mapping-and-logging/.superpowers/sdd/progress.md
new file mode 100644
index 0000000..859f841
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/.superpowers/sdd/progress.md
@@ -0,0 +1,16 @@
+# 执行进度台账 · tpops-mapping-and-logging
+
+模式：**sdd**（plan revision 1） · 分支：`tpops_metric_dev` · worktree：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`
+
+| wave | 任务 | 状态 | receipt |
+|---|---|---|---|
+| wave-1-mapping | 1.1~1.8 | 进行中（实现代理已派出） | — |
+| wave-2-logging | 2.1~2.7 | 未开始 | — |
+| wave-3-deploy | 3.1~3.3 | 未开始 | — |
+| wave-4-verification | 3.4 | 未开始 | — |
+
+## 任务简报
+
+- wave-1：`.superpowers/sdd/wave-1-brief.md`
+- 实现报告：`.superpowers/sdd/wave-1-implementer-report.md`
+- 审查报告：`.superpowers/sdd/reviews/wave-1.md`
diff --git a/changes/tpops-mapping-and-logging/.superpowers/sdd/wave-1-brief.md b/changes/tpops-mapping-and-logging/.superpowers/sdd/wave-1-brief.md
new file mode 100644
index 0000000..98cc1aa
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/.superpowers/sdd/wave-1-brief.md
@@ -0,0 +1,158 @@
+# Wave 1 任务简报 · 映射口径（wave-1-mapping）
+
+**工作目录（唯一）**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`
+**分支**：`tpops_metric_dev`（已在 worktree 里；不要在主仓 `/mnt/c/sml/project/py_project/tpops_metric_dev` 改任何文件）
+**Python**：`conda run -n tpops python ...`（conda env `tpops`，Python **3.7.9**）
+**权威工件**（先读）：
+- `changes/tpops-mapping-and-logging/execution-contract.md`（Intent Lock / Approved Behavior R1~R7,R18,R19 / Design Constraints / Test Obligations / Escalation Rules）
+- `changes/tpops-mapping-and-logging/tasks.md`（任务 1.1~1.8 全文）
+- `changes/tpops-mapping-and-logging/specs/mapping-and-rules/spec.md`（R1~R7）
+- `changes/tpops-mapping-and-logging/specs/delivery-and-state/spec.md`（R18、R19）
+- `changes/tpops-mapping-and-logging/design.md`（决策 8 / 9 / 10）
+
+## 铁律（不可违反）
+
+1. **Python 3.7.9**：禁海象 `:=`、`f"{x=}"`、字典合并 `d1 | d2`、内建泛型注解
+   `list[str]`、`typing.Literal/Protocol/TypedDict/Final`、`functools.cached_property`、
+   `importlib.metadata`、`math.prod`、`zoneinfo`、`str.removeprefix/removesuffix`。
+2. **依赖锁不变**：本波次**不新增依赖**（loguru 是 wave-2 的事）。
+3. **映射层表驱动**：客户口径一律来自 `tpops_kafka_collector/mapping/data/*.json`
+   （运行期真相源）；仓库根 `data/mapping/` 是**同源副本**，改一侧必须同步另一侧
+   （有现成的一致性测试会抓）。
+4. **不要改客户 xlsx**：`高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` 是客户原件，**只读**。
+5. 采集端单文件 **< 400 行**、函数 **< 50 行**；不可变 DTO；`logging` 不用 `print`；
+   凭据只走环境变量、禁入日志。
+6. **TDD：每个任务先写失败测试（RED），看到它失败，再实现（GREEN），最后重构。**
+   不要先写实现再补测试。
+7. 不要碰 wave-2/3/4 的内容（loguru、collector.sh、交付文档里除任务 1.8 指定的以外）。
+
+## 目标（验收，全部机械可判定）
+
+1. `tpops_kafka_collector/mapping/data/metric_rules.json` 恰好 **87** 条规则，
+   **0** 条 `enabled=false`，`metric_code` 互不重复。
+2. `tpops_kafka_collector/mapping/data/excluded_metrics.json` 恰好 **14** 条：
+   **11 条** `reason="duplicate"`（各带 `kept` 指明保留哪条）、**3 条** `reason="unsupported"`。
+   `metric_code` / `metric_name` / `reason` / `detail` 必填；`kept` 仅 duplicate 必填。
+3. 未挂客户指标的事件类告警，`metric_code` / `metric_name` **全部等于同一个固定值**；
+   挂靠了客户指标的告警**保持不变**。
+4. 表级指标的 `{#NAME}` = `schemaname.relname`（如 `public.t1`）；
+   按 `schemaname` + `relname` **两列**分组；`public.t1` 与 `other.t1` 的
+   `metricName` / `metricCode` / 幂等键**都不同**。
+
+## 明确清单（不要自己重新推导）
+
+### A. 要排除的 11 条「重复的英文变体」（`reason=duplicate`）
+
+每条的 `kept` = 对应的小写 `gaussdb.` 版本（**保留**那一条）：
+
+| 要排除的 metric_code | metric_name（英文中文名） | kept（保留哪条） |
+|---|---|---|
+| `GAUSSDB.{#NAME}.Number.lete.ples` | `({#NAME}) number of delete tuples` | `gaussdb.{#NAME}.Number.lete.ples` |
+| `GAUSSDB.{#NAME}.Number.ad.ples` | `({#NAME}) number of dead tuples` | `gaussdb.{#NAME}.Number.ad.ples` |
+| `GAUSSDB.{#NAME}.Number.t.date.ples` | `({#NAME}) number of hot update tuples` | `gaussdb.{#NAME}.Number.t.date.ples` |
+| `GAUSSDB.{#NAME}.number.Of.index.scan` | `({#NAME})number of index scan` | `gaussdb.{#NAME}.number.Of.index.scan` |
+| `GAUSSDB.{#NAME}.number.Of.quential.ples.ad` | `({#NAME})number of sequencial tuples read` | `gaussdb.{#NAME}.number.Of.quential.ples.ad` |
+| `GAUSSDB.{#NAME}.number.Of.dex.ples.ad` | `({#NAME})number of index tuples read` | `gaussdb.{#NAME}.number.Of.dex.ples.ad` |
+| `GAUSSDB.{#NAME}.number.Of.insert.tuples` | `({#NAME})number of insert tuples` | `gaussdb.{#NAME}.number.Of.insert.tuples` |
+| `GAUSSDB.{#NAME}.number.Of.live.tuples` | `({#NAME})number of live tuples` | `gaussdb.{#NAME}.number.Of.live.tuples` |
+| `GAUSSDB.{#NAME}.number.Of.update.tuples` | `({#NAME})number of update tuples` | `gaussdb.{#NAME}.number.Of.update.tuples` |
+| `GAUSSDB.{#NAME}.number.Of.sequential.scan` | `({#NAME})number of sequencial scan` | `gaussdb.{#NAME}.number.Of.sequential.scan` |
+| `GAUSSDB.{#NAME}.garbage.Ratio` | `({#NAME})Garbage ratio` | `gaussdb.{#NAME}.garbage.Ratio` |
+
+### B. 要排除的 3 条「无法提供」（`reason=unsupported`，无 `kept`）
+
+| metric_code | metric_name | detail（理由） |
+|---|---|---|
+| `gaussdb.Fastpath_function_call` | `快路径函数调用的连接数` | PG 侧无 fastpath 计数器；pg_settings 仅有 gsi_fastpath_level 开关 |
+| `gaussdb.Disabled_conn` | `禁用的连接数` | PG 无「禁用连接」概念；pg_settings 仅有 reserved_connections 等预留连接 |
+| `gaussdb.sendAgentName` | `发送设备名给Agent` | Agent 侧行为，不是数据库可查询的监控指标 |
+
+> 这 3 条当前在 `metric_rules.json` 里以 `enabled=false` 存在；本波次要**从生效口径里彻底去掉**
+> （不是留着 disabled）。`dropped_unsupported` 从此恒为 0。
+
+### C. 告警统一名（拟值，已登记待客户确认）
+
+在 `tpops_kafka_collector/mapping/data/alarm_rules.json` 增加一段：
+
+```json
+"event_metric": {"metric_code": "gaussdb.alarm", "metric_name": "数据库告警"}
+```
+
+生成器对**未挂客户指标**（`linked_customer_metric` 为空）的 254 条事件类告警，
+`metric_code` / `metric_name` 统一取该值；3 条挂靠 `gaussdb.Tps` 的**保持不变**。
+同时在 `tpops_kafka_collector/mapping/data/open_items.json` 登记该统一值待客户确认。
+
+## 已知实现落点（已核实，可直接用）
+
+1. **生成器** `tools/build_itscada_mapping.py`：
+   - `DIMENSION_COLUMN_BY_SQL`（约第 79 行）里的
+     `'stat_user_tables': {'NAME': 'relname'}` → 改成 `'schemaname.relname'`；
+   - `group_by` 是从 `dimension_bindings` 的值派生的（约第 420 行
+     `group_by = sorted(set(bindings.values()))`）——**只改绑定会让 `group_by` 变成一个
+     叫 `schemaname.relname` 的单列，分组失效**。必须同时改这处派生逻辑，
+     把点号串**拆成多列**（`['schemaname', 'relname']`）。
+   - 生成 `metric_rules.json` / `customer_metric_mapping.json` 时**跳过排除清单里的指标码**。
+   - 生成告警定义时套用 `event_metric`。
+2. **映射层** `tpops_kafka_collector/mapping/rules_context.py`：
+   - `row_dimensions`：让 `dimension_bindings` 的值支持「点号连接的多个列名」
+     （按 `.` 连接各列的值）；
+   - `grouped_items`：让 `group_by` 支持多列分组。
+3. **加载与校验**：`tpops_kafka_collector/mapping/loader.py`（加载排除清单）、
+   `tpops_kafka_collector/mapping/validation.py`（校验：排除清单的码一个都不在生效映射表中、
+   生效规则 `metric_code` 唯一、生效规则数 = 87）。
+   **告警定义的 `metric_code` 唯一性校验要放宽**（唯一性只对指标规则要求）。
+4. **合并 SQL 不用改**：合并视图 `stat_user_tables` 的实测返回列**已含 `schemaname`**
+   （见 `mapping/data/observed_columns.json`），数据是现成的。
+5. **交付文档**：`tools/build_delivery_docs.py` 从映射数据生成三份文档
+   （`docs/交付-指标口径说明.md` 87 条、`docs/交付-不可提供指标清单.md` 3 条并标注
+   「已从生效口径移除」+ 理由、`docs/交付-客户指标英文名核对清单.md`）——
+   它也要读排除清单。
+
+## 任务清单（tasks.md 1.1~1.8 全文要点）
+
+- **1.1** 建排除清单数据文件：`包内/excluded_metrics.json` + `仓库根/excluded_metrics.json`
+  同源副本；新增 `tests/unit/test_excluded_metrics.py`（14 条 / 11-3 分布 /
+  duplicate 必带 kept / 两份一致）。
+- **1.2** 生成器按清单剔除；在 `test_excluded_metrics.py` 补断言（87 条、0 disabled、
+  code 唯一）。
+- **1.3** `loader.py` + `validation.py` 加载与校验；新增 `tests/unit/test_metric_catalog.py`
+  （三项校验的正例与反例：人为塞一条被排除的码 → 必须校验失败并打印差异）。
+- **1.4** `rules_context.py` 支持点号组合绑定与多列分组；
+  新增 `tests/unit/test_dimension_binding.py`（组合绑定、多列分组、单列绑定不回归）。
+- **1.5** 生成器 + 规则改绑 `schemaname.relname`；在 `test_metric_catalog.py` 补断言
+  （表级规则一律按两列分组）。
+- **1.6** 在 `test_dimension_binding.py` 补幂等不碰撞用例（`public.t1` vs `other.t1` 键不同；
+  同表重复采集键相同）。
+- **1.7** 告警统一指标名 + 放宽告警 `metric_code` 唯一性校验 + `open_items.json` 登记；
+  新增 `tests/unit/test_alarm_metric_unification.py`。
+- **1.8** 重生成三份交付文档 + 更新 `docs/映射表-ITSCADA字段对照.md` 与
+  `tpops_kafka_collector/mapping/README.md`；新增 `tests/unit/test_delivery_docs.py`
+  （文档条数与生效映射表一致：87 / 3）。
+
+## 证明命令（每个任务跑自己的；收尾跑全套）
+
+```bash
+cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
+conda run -n tpops python -m pytest tests/unit/test_excluded_metrics.py \
+    tests/unit/test_dimension_binding.py tests/unit/test_alarm_metric_unification.py \
+    tests/unit/test_metric_catalog.py tests/unit/test_delivery_docs.py -q
+conda run -n tpops python -m pytest -q          # 全量必须全绿
+conda run -n tpops python -m ruff check tpops_kafka_collector tests tools
+conda run -n tpops python -m pytest tests/test_py37_compat.py -q
+```
+
+**全量 pytest 必须全绿**（现有约 1700+ 例）；若某个既有测试因为口径变化而失败，
+**修测试或修实现以匹配新口径**，SHALL NOT 删测试、SHALL NOT 放宽断言来糊过去；
+若确实需要改既有测试的期望值，在报告里逐条说明理由。
+
+## 交付
+
+1. 提交代码（conventional commits，中文正文；**不要**加 Co-Authored-By/署名行）。
+2. 写实现报告到
+   `changes/tpops-mapping-and-logging/.superpowers/sdd/wave-1-implementer-report.md`，
+   内容含：每个任务做了什么、RED 证据（哪个测试先失败了、失败信息）、
+   GREEN 证据（命令 + 通过数）、改动文件清单、遇到的问题与你的判断、
+   以及你认为**需要 reviewer 特别注意**的点。
+3. 不要修改 `execution-contract.md` / `tasks.md` / `proposal.md` / `specs/` /
+   `design.md`（那是已批准的规划工件）。
+4. 有拿不准的地方**先问**，不要猜。
diff --git a/changes/tpops-mapping-and-logging/.superpowers/sdd/wave-1-implementer-report.md b/changes/tpops-mapping-and-logging/.superpowers/sdd/wave-1-implementer-report.md
new file mode 100644
index 0000000..fc2ebed
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/.superpowers/sdd/wave-1-implementer-report.md
@@ -0,0 +1,461 @@
+# Wave 1 实现报告 · 映射口径（wave-1-mapping）
+
+> 变更：`tpops-mapping-and-logging`；波次：`wave-1-mapping`（serial，任务 1.1~1.8）
+> 工作目录：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（分支 `tpops_metric_dev`）
+> 日期：2026-11-26
+
+---
+
+## 0. 总结
+
+| 项 | 结果 |
+|---|---|
+| 生效指标规则 | **87** 条（`enabled=false` 0 条，`metric_code` 无重复） |
+| 排除清单 | **14** 条（11 `duplicate` + 3 `unsupported`），包内与仓库根逐字节一致 |
+| 告警统一指标名 | 未挂客户指标的 254 条 → `gaussdb.alarm` / `数据库告警`；3 条挂靠 `gaussdb.Tps` 的不变 |
+| 表级维度 | `{#NAME}` = `schemaname.relname`（如 `public.t1`），按两列分组 |
+| 全量 pytest | **1841 passed**（收尾时） |
+| py37 兼容门禁 | **170 passed** |
+| ruff | **基线本就不绿**（详见 §5），本波次未引入新类别的问题 |
+| 提交 | 1 个提交（口径改动彼此耦合，拆开会让中间提交不绿 —— 详见 §5.6） |
+
+**目标达成情况**：合同「Wave 1 完成标准」四条全部满足（详见 §4）。
+
+---
+
+## 1. 逐任务：做了什么 / RED / GREEN
+
+### 1.1 建排除清单数据文件
+
+**做了什么**
+
+- 新增 **包内** `tpops_kafka_collector/mapping/data/excluded_metrics.json`（运行期真相源）
+  与 **仓库根** `data/mapping/excluded_metrics.json`（同源副本），**逐字节一致**。
+- 结构：`counts`（`total=14` / `duplicate=11` / `unsupported=3` / `client_total=101` /
+  `effective_total=87`）+ `excluded[]`（14 条）。
+  每条含 `metric_code` / `metric_name` / `reason` / `detail`；`reason=duplicate` 的
+  11 条另带 `kept`（保留哪一条），`reason=unsupported` 的 3 条**不带** `kept`。
+- 新增 `tests/unit/test_excluded_metrics.py`：14 条 / 11-3 分布 / 必填字段 /
+  duplicate 必带 kept / unsupported 不带 kept / 码唯一 / 两份副本一致 /
+  `kept` 指向的指标必须在生效映射表里 / `kept` 自己不在排除清单里。
+
+**RED 证据**（先把数据文件挪走再写测试）：
+
+```
+ERROR tests/unit/test_excluded_metrics.py::test_exclusion_list_has_exactly_fourteen_entries
+  - FileNotFoundError ... excluded_metrics.json
+FAILED tests/unit/test_excluded_metrics.py::test_package_and_repo_copies_are_byte_identical
+FAILED tests/unit/test_excluded_metrics.py::test_effective_mapping_has_exactly_eighty_seven_rules
+FAILED tests/unit/test_excluded_metrics.py::test_effective_mapping_has_no_disabled_rule
+3 failed, 1 passed, 11 errors
+```
+
+**GREEN 证据**：数据文件落位后 `12 passed`（其中 87 条 / 0 disabled 两条断言属任务 1.2，
+当时仍红，见 1.2）。
+
+### 1.2 生成器按清单剔除
+
+**做了什么**
+
+- 新增 `tools/excluded_metrics.py`：**构建期共用**的排除清单读取器
+  （`load_excluded` / `excluded_codes` / `is_excluded`）—— 三个生成器读**同一份**清单。
+- `tools/build_customer_metric_mapping.py`：`build()` 里按清单剔除 →
+  `data/customer_metric_mapping.json` 从 101 条降到 **87 条**。
+- `tools/build_metric_value_rules.py`：`_build_metrics()` 剔除 →
+  `data/metric_value_rules.json` 98 → **87**；末尾「期望 98 条」的写死断言改成
+  **与可提供性判定交叉校验**（`_expected_effective_count()`，条数不写死在代码里）。
+- `tools/build_itscada_mapping.py`：`build_metric_rules()` 跳过清单里的指标码 →
+  `metric_rules.json` **87 条、0 条 disabled**。
+- 在 `test_excluded_metrics.py` 补断言：87 条 / 0 条 `enabled=false` / `metric_code` 唯一
+  / 排除清单的码一个都不在生效映射表里 / `kept` 必在生效映射表里。
+
+**RED 证据**（1.1 的 GREEN 跑完仍红）：
+
+```
+FAILED tests/unit/test_excluded_metrics.py::test_no_excluded_code_is_in_the_effective_mapping
+FAILED tests/unit/test_excluded_metrics.py::test_effective_mapping_has_exactly_eighty_seven_rules
+FAILED tests/unit/test_excluded_metrics.py::test_effective_mapping_has_no_disabled_rule
+  AssertionError: 生效口径里不该有 enabled=false 的规则：
+  ['gaussdb.Fastpath_function_call', 'gaussdb.Disabled_conn', 'gaussdb.sendAgentName']
+```
+
+**GREEN 证据**：`15 passed`（重生成后）→ 收尾 `16 passed`。
+
+### 1.3 映射层加载与校验排除清单
+
+**做了什么**
+
+- `mapping/loader.py`：新增 `excluded_metrics` 文件入口、新增 `ExcludedMetric` 冻结
+  dataclass、`LoadedMapping` 增 `excluded_metrics` / `excluded_counts` 两个字段；
+  `validate_mapping()` 增三项校验。
+- `mapping/validation.py` 新增三项校验函数：
+  - `check_excluded_metrics_absent()` —— 排除清单的码**一个都不在**生效映射表里；
+  - `check_metric_code_uniqueness()` —— 生效规则的 `metric_code` 互不重复
+    （**唯一性只对指标规则要求**，告警定义靠 `alarm_id` 定位，不参与唯一性校验）；
+  - `check_effective_rule_count()` —— 生效条数与排除清单 `counts.effective_total` 一致
+    （**条数来自数据文件**，不写死在代码里）。
+- `mapping/validation.py` 顺带修：维度绑定的列校验支持**点号串**（1.5 的前置）。
+- 新增 `tests/unit/test_metric_catalog.py`：正例 + 三项反例
+  （塞一条被排除的码 → 校验失败并打印差异；复制一条规则 → 唯一性失败；
+  少一条规则 → 条数失败且错误信息里同时出现 87 与 86）；
+  另有一条「告警定义共用同一个 metricCode 不得触发唯一性失败」。
+
+**RED 证据**（把 `loader.py` / `validation.py` 临时回退到 HEAD 再跑）：
+
+```
+FAILED tests/unit/test_metric_catalog.py::test_the_real_catalog_passes_validation
+FAILED tests/unit/test_metric_catalog.py::test_excluded_metrics_are_loaded_from_the_data_file
+FAILED tests/unit/test_metric_catalog.py::test_an_excluded_code_in_the_effective_mapping_fails_validation
+FAILED tests/unit/test_metric_catalog.py::test_a_duplicated_metric_code_fails_validation
+FAILED tests/unit/test_metric_catalog.py::test_a_wrong_rule_count_fails_validation
+5 failed, 1 passed
+```
+
+**GREEN 证据**：`6 passed`（收尾 `9 passed`，1.5 的两条断言加进来后）。
+
+### 1.4 点号组合的维度绑定
+
+**做了什么**
+
+- `mapping/rules_context.py`：
+  - 新增 `binding_columns(spec)` —— 绑定值可以是单列（`relname`），
+    也可以是**点号连接的多个列名**（`schemaname.relname`）；
+  - `row_dimensions()` 支持组合绑定（各列值用 `.` 连起来）；
+  - `grouped_items()` 的「分组键必须覆盖绑定列」检查改为按**拆开的列**比对，
+    并支持**多列分组**；维度值从分组键里取（`_value_from_key()`）。
+  - **取不到列值 → 抛 `RuleError`**（不静默留 `{#NAME}`）—— 与合同的
+    Test Obligations 一致。
+- 新增 `tests/unit/test_dimension_binding.py`：组合绑定（`public.t1` / `other.t1`）、
+  组合绑定取不到列值抛错、多列分组、单列绑定不回归（含「单列取不到仍是 `None`」的老行为）。
+
+**RED 证据**：
+
+```
+E  tpops_kafka_collector.mapping.rules_context.RuleError: group_by ['schemaname', 'relname']
+   与维度绑定 {'NAME': 'schemaname.relname'} 不一致：分组键缺少列 ['schemaname.relname']
+FAILED tests/unit/test_dimension_binding.py::test_dotted_binding_combines_the_column_values
+FAILED tests/unit/test_dimension_binding.py::test_dotted_binding_keeps_the_schema_prefix
+FAILED tests/unit/test_dimension_binding.py::test_dotted_binding_raises_when_a_column_is_missing
+FAILED tests/unit/test_dimension_binding.py::test_group_by_two_columns_splits_same_named_tables
+FAILED tests/unit/test_dimension_binding.py::test_group_by_two_columns_keeps_the_row_value
+5 failed, 4 passed
+```
+
+**GREEN 证据**：`9 passed`（收尾 `14 passed`）。
+
+### 1.5 生成器与规则改绑 schema+relname
+
+**做了什么**
+
+- `tools/build_itscada_mapping.py`：
+  - `DIMENSION_COLUMN_BY_SQL['stat_user_tables']` 的 `'NAME'` 由 `'relname'`
+    改成 **`'schemaname.relname'`**；
+  - 新增 `_group_by_columns(bindings)` —— **点号串拆成多列**再按绑定顺序去重
+    （`_row_policy()` 与 `build_value_rule()` 的 aggregate 分支都改用它）。
+    这一点是**必须**的：不改派生逻辑，`group_by` 会退化成
+    「一个叫 `schemaname.relname` 的单列」，分组失效。
+- 重生成后 11 条表级规则的 `dimension_bindings` = `{"NAME": "schemaname.relname"}`、
+  `group_by` = `["schemaname", "relname"]`。
+- 在 `test_metric_catalog.py` 补断言：表级规则恰好 11 条、一律绑 `schemaname.relname`、
+  一律按两列分组，且**任何规则的 `group_by` 里都不许出现带点的列**。
+
+**RED / GREEN**：该任务的断言随 1.4 的实现一起由红转绿（见 1.4 的 RED 输出）；
+生成器侧的行为由 `test_metric_catalog.py` 与
+`tests/test_generated_rules.py::test_metric_rules_file_is_up_to_date`（重生成结果 == 落盘文件）守着。
+
+### 1.6 表级幂等不碰撞
+
+**做了什么**
+
+在 `tests/unit/test_dimension_binding.py` 补 5 条用例（走**真实链路**：
+`evaluate()` → `build_metric_message()` → `metric_idempotency_key()`）：
+
+- `public.t1` 与 `other.t1` 的 `metricName` 不同、`metricCode` 不同；
+- `tags.dimension_values.NAME` = `public.t1` / `other.t1`；
+- 两条的**幂等键不同**（两条都投递）；
+- 同一张表重复采集 → **幂等键相同**（仍然去重）；
+- `resourceName` 不含表名（资源名只表示节点）。
+
+**RED 证据**（把 `rules_context.py` 临时回退到 HEAD 再跑）：
+
+```
+FAILED tests/unit/test_dimension_binding.py::test_same_named_tables_in_different_schemas_get_different_messages
+FAILED tests/unit/test_dimension_binding.py::test_dimension_values_carry_the_schema_prefix
+FAILED tests/unit/test_dimension_binding.py::test_same_named_tables_in_different_schemas_get_different_idempotency_keys
+FAILED tests/unit/test_dimension_binding.py::test_the_same_table_collected_twice_gets_the_same_idempotency_key
+FAILED tests/unit/test_dimension_binding.py::test_resource_name_does_not_contain_the_table_name
+10 failed, 4 passed
+```
+
+**GREEN 证据**：`14 passed`。
+
+### 1.7 告警统一指标名
+
+**做了什么**
+
+- `alarm_rules.json` 新增 **`event_metric`** 段
+  （`{"metric_code": "gaussdb.alarm", "metric_name": "数据库告警"}`）—— 包内 + 仓库根。
+- `tools/build_itscada_mapping.py`：未挂客户指标的事件类告警**统一取该值**；
+  统一值**表驱动** —— 生成器优先读 `alarm_rules.json` 里已有的 `event_metric`
+  （运维改了重跑不会被覆盖回去），没有才用 `DEFAULT_EVENT_METRIC`。
+  挂靠了客户指标的 3 条**保持不变**（`gaussdb.Tps` / `TPS（每秒事务数）`）。
+- `mapping/table.py` 新增 `build_event_alarm_definition()` —— 运行期兜底也用统一值。
+- `collectors/alarm.py` 的 `_definition_for()`：平台新增告警（定义缺失）时，
+  兜底从「自造 `gaussdb.alarm.<alarm_id>`」改成**映射数据里的统一值**。
+- `open_items.json`：登记该统一值（新增 `alarm-unified-metric` 一项），
+  并把 `alarm-metric-binding` 的「自造指标名」旧口径改成新口径。
+- 新增 `tests/unit/test_alarm_metric_unification.py`：254 条同码同名、3 条挂靠不变、
+  告警名逐条对得上平台目录、告警码可重复而不触发唯一性校验、运行期兜底用统一值。
+
+**RED 证据**：
+
+```
+ImportError: cannot import name 'build_event_alarm_definition' from
+'tpops_kafka_collector.mapping.table'
+```
+
+（随后一轮：未统一时 `test_event_alarm_names_are_still_all_different` 与
+`test_the_runtime_fallback_uses_the_mapped_unified_value` 失败。）
+
+**GREEN 证据**：`13 passed`。
+
+### 1.8 交付文档重生成
+
+**做了什么**
+
+- `tools/build_delivery_docs.py`：
+  - 读 `excluded_metrics.json`（包内）；
+  - 「不可提供清单」改从**排除清单**的 `reason=unsupported` 取（原来从映射表的
+    `enabled=false` 规则取 —— 那 3 条已经不在生效映射表里了）；
+    分类 / 数据类型 / 采集周期从**客户 xlsx（只读）**取，不在数据文件里重复一份；
+  - 文案改为「**构建期排除**、运行期无丢弃分支、`dropped_unsupported` 恒为 0、
+    **已从生效口径移除**」；
+  - 口径说明 / OCR 清单里的写死数字（101 / 98 / 82）改成**按数据算**。
+- `tools/build_kafka_mapping.py`：占位符绑定支持点号串（新增 ` · ` 连接表达式）。
+- `tools/expected_deriver.py`：独立推导器的 `_dimensions()` 同步支持点号串
+  （否则期望值表推出来的维度是 `None`）。
+- 重生成三份交付文档 + `docs/映射表-ITSCADA字段对照.md`
+  （`data/mapping/archive/alarm_mapping.json` 里的两处 transform 文案同步改掉），
+  更新 `tpops_kafka_collector/mapping/README.md`（新增「排除清单」「表级维度」
+  「告警统一指标名」三节）、`data/mapping/README.md`、`data/metric_response/README.md`。
+- `tests/unit/test_delivery_docs.py`：更新判据（不可提供清单改对排除清单），
+  并**新增**「三份文档的条数与生效映射表一致（87 / 3 / —）」的防漂移用例。
+
+**RED / GREEN**：既有 5 条用例在文档口径变化后失败（98→87、清单来源变化），
+按新口径修测试后 `40 passed`；补上新增用例后 `41 passed`。
+
+---
+
+## 2. 改动文件清单
+
+### 新增
+
+| 文件 | 说明 |
+|---|---|
+| `tpops_kafka_collector/mapping/data/excluded_metrics.json` | 排除清单（运行期真相源，14 条） |
+| `data/mapping/excluded_metrics.json` | 同源副本（逐字节一致） |
+| `tools/excluded_metrics.py` | 构建期共用的排除清单读取器 |
+| `tests/unit/test_excluded_metrics.py` | 任务 1.1 / 1.2 |
+| `tests/unit/test_metric_catalog.py` | 任务 1.3 / 1.5 |
+| `tests/unit/test_dimension_binding.py` | 任务 1.4 / 1.6 |
+| `tests/unit/test_alarm_metric_unification.py` | 任务 1.7 |
+
+### 生产代码
+
+| 文件 | 改动 |
+|---|---|
+| `tpops_kafka_collector/mapping/rules_context.py` | 点号组合绑定 + 多列分组 + 取不到列值抛 `RuleError` |
+| `tpops_kafka_collector/mapping/loader.py` | 加载排除清单；三项新校验接入 `validate_mapping()` |
+| `tpops_kafka_collector/mapping/validation.py` | 三项新校验；维度绑定列校验支持点号串 |
+| `tpops_kafka_collector/mapping/table.py` | 新增 `build_event_alarm_definition()` |
+| `tpops_kafka_collector/collectors/alarm.py` | 定义缺失时的兜底改用映射数据里的统一指标名 |
+
+### 生成器 / 离线工具
+
+`tools/build_itscada_mapping.py`、`tools/build_customer_metric_mapping.py`、
+`tools/build_metric_value_rules.py`、`tools/build_delivery_docs.py`、
+`tools/build_kafka_mapping.py`、`tools/expected_deriver.py`
+
+### 数据（重生成）
+
+`data/mapping/{metric_rules,alarm_rules,response_rules,kafka_field_mapping,open_items,expected_values}.json`、
+`data/mapping/archive/alarm_mapping.json`、`data/customer_metric_mapping.json`、
+`data/customer_metric_query_plan.json`、`data/metric_value_rules.json`、
+`data/tpops_api_catalog.json`、`data/metric_response/README.md`、`data/mapping/README.md`，
+以及包内对应的 6 份副本（`tpops_kafka_collector/mapping/data/*.json`）
+
+### 文档
+
+`docs/交付-指标口径说明.md`、`docs/交付-不可提供指标清单.md`、
+`docs/交付-客户指标英文名核对清单.md`、`docs/映射表-ITSCADA字段对照.md`、
+`tpops_kafka_collector/mapping/README.md`
+
+### 既有测试（按新口径修，**未删任何测试**）
+
+`tests/test_mapping_table.py`、`tests/test_expected_values.py`、
+`tests/test_generated_rules.py`、`tests/test_kafka_mapping.py`、
+`tests/test_merged_sql_mapping.py`、`tests/test_metric_response.py`、
+`tests/test_response_rules.py`、`tests/test_rules.py`、
+`tests/integration/test_collector_command_center.py`、
+`tests/regression/{test_b_class_regression,test_collector_coverage,test_fixture_integrity}.py`、
+`tests/unit/{test_delivery_docs,test_mapping_columns_gaps,test_mapping_data_sync,test_mapping_loader,test_scheduler}.py`
+
+---
+
+## 3. 既有测试的期望值变更（逐条理由）
+
+> 规则：**不删测试、不放宽断言**。下列改动都是「口径变了 → 期望值跟着变」。
+
+| 文件 | 变更 | 理由 |
+|---|---|---|
+| `tests/test_mapping_table.py` | 101→87、98→87、82→71 | 生效口径 / B 类条数变了 |
+| 同上 | `test_unsupported_metrics_are_disabled_with_reason` → `test_unsupported_metrics_are_excluded_not_disabled` | 3 条无法提供的指标**不再以 `enabled=false` 留在映射表里**，改为断言「不在生效映射表里 + 没有 disabled 规则」 |
+| `tests/test_generated_rules.py` | 98→87 | 取值规则条数变了 |
+| `tests/test_kafka_mapping.py` | 98→87 | 字段映射表条数变了 |
+| `tests/test_merged_sql_mapping.py` | 98→87；`checked >= 80` → `== 71` | 同上；回放条数随之变化，改成**精确值**（比原来的下限更严） |
+| `tests/test_response_rules.py` | 98→87 | 取数定位表条数变了 |
+| `tests/test_expected_values.py` | `EXPECTED_B_COUNT` 82→71 | B 类条数变了（11 条重复表级指标被排除） |
+| `tests/regression/test_b_class_regression.py` | 82→71 | 同上 |
+| `tests/regression/test_collector_coverage.py` | 98→87、97→86 | 产出条数变了（86 = 87 − 1 条「留档里没有 AccessExclusiveLock 锁」的历史缺口） |
+| `tests/regression/test_fixture_integrity.py` | 82→71 | 期望值表条数变了（表是**生成物**，由 `tools/build_expected_values.py` 重生成后同步） |
+| `tests/integration/test_collector_command_center.py` | 82→71；`EXPECTED_COMPARISONS` 1700→954；分页夹具补 `'schemaname'` | 表级维度改成两列后，合成页必须带 `schemaname`（否则组合绑定取不到值 → `RuleError`）；比对条数随之变化 |
+| `tests/test_rules.py` | 告警 `metricCode` 断言 `gaussdb.alarm.2007010001` → `gaussdb.alarm`；`metricName` → `数据库告警`；比率指标列表去掉 `GAUSSDB.{#NAME}.garbage.Ratio` | 告警统一指标名（新口径）；被排除的指标码不再有规则 |
+| `tests/test_metric_response.py` | manifest 断言改为「**覆盖**每个生效指标」；逐指标核对时跳过不在生效口径里的码 | 样本目录是**抓取归档**（98 份，平台事实），生效口径是 87 条（决定）—— 见 §5.2 |
+| `tests/unit/test_delivery_docs.py` | 98→87；不可提供清单改对排除清单；新增「文档条数 == 生效映射表」 | 交付文档随口径重生成 |
+| `tests/unit/test_scheduler.py` | `metric_count` 82→71；`count_unsupported_metrics() == 3` → `== 0` | 不可提供指标已在**构建期**排除 → 运行期 `dropped_unsupported` 恒为 0（合同要求） |
+| `tests/unit/test_mapping_loader.py`、`test_mapping_columns_gaps.py` | 101→87 | 加载到的条数变了 |
+| `tests/unit/test_mapping_data_sync.py`、`test_mapping_loader.py` | `COPIED_FILES` 增加 `excluded_metrics.json` | 新增了一份同源副本，防漂移闸要盯它 |
+
+---
+
+## 4. 合同完成标准逐条核对
+
+| 合同要求 | 结果 |
+|---|---|
+| `pytest tests/unit/test_excluded_metrics.py tests/unit/test_dimension_binding.py tests/unit/test_alarm_metric_unification.py tests/unit/test_metric_catalog.py tests/unit/test_delivery_docs.py -q` 全绿 | **93 passed** |
+| 生效规则数 = 87 | ✅ `metric_rules.json` 87 条 |
+| `enabled=false` 数 = 0 | ✅ 0 条（3 条无法提供的已**彻底移出**生效口径） |
+| `metric_code` 无重复 | ✅ 启动期校验 + 单测双守 |
+| 排除清单 14 条（11/3），包内与仓库根两份一致 | ✅ 逐字节一致（同步闸 + 单测） |
+| `public.t1` 与 `other.t1` 的 `metricCode` 与幂等键不同 | ✅ `test_dimension_binding.py` 5 条用例 |
+| 全量 pytest 全绿 | ✅ **1841 passed** |
+| py37 兼容门禁 | ✅ 170 passed |
+| ruff | ⚠️ **基线本就不绿**，见 §5.5 |
+
+---
+
+## 5. 遇到的问题与判断（请 reviewer 重点看）
+
+### 5.1 「生效规则数 = 87」的期望值放在**数据文件**里，不写死在代码里
+
+合同 R6（映射表驱动）要求「SHALL NOT 硬编码客户口径」。所以
+`check_effective_rule_count()` 的期望值取自
+`excluded_metrics.json` 的 `counts.effective_total`（= 87），
+并在同一处记 `client_total=101` —— 三者自洽（`101 − 14 = 87`）。
+**判断**：条数是客户口径，写死在 `validation.py` 里会与 R6 冲突；
+但完全不写死又会失去「条数对不上就启动失败」这道闸。
+放在排除清单的 `counts` 段是折中：**一处声明、两处使用**
+（校验 + 单测）。若 reviewer 认为应当另设一个「口径基线」文件，可以再调。
+
+### 5.2 样本归档（98 份）**没有**动，生效口径（87 条）另算
+
+`data/metric_response/` 与 `tests/fixtures/metric_response/` 是**抓取归档**
+（平台事实，98 份样本 + manifest）。本波次**没有删任何样本文件**，
+manifest 也仍是 98 条。理由：
+
+- 归档是事实，生效口径是决定 —— 被排除的 11 条重复表级指标的**平台响应仍然有效**，
+  删掉是**不可逆的数据损失**（git 历史可恢复，但没必要）；
+- 合同没有要求删样本；R1 只要求「运行期生效映射表里不存在被排除的指标码」。
+
+代价：manifest 与生效口径不再是 1:1，于是两条既有测试按新口径改了
+（§3 最后两行）：manifest 断言从「等于生效口径」改成「**覆盖**每个生效指标」，
+逐指标核对时跳过不在生效口径里的码。
+**判断**：这是「归档 ⊇ 生效口径」的语义，方向仍然是「每个生效指标都有样本」
+（缺一个就红），没有放宽。**若 reviewer 认为归档应当与生效口径同步收敛，
+需要删 11 份样本 + 改 manifest + 改 `EXPECTED_ARCHIVE_COUNT`，请指示。**
+
+### 5.3 `alarm_name` 并非 254 条全不同（**偏离任务原文**）
+
+任务 1.7 原文要求断言「254 条事件类告警的 `metric_code` 相同、**`alarm_name` 各不相同**」。
+实测**做不到**：平台告警目录（`data/alarm/catalog_*.json`）里有
+
+- **13 行**「告警名称」列填的是等级词（`紧急` 8 次 / `重要` 5 次）；
+- **2 组**同名不同 ID 的告警（`系统磁盘使用率过高` ×2、`GaussDB流容灾主集群数据同步失败` ×2）。
+
+即 254 条里只有 **241 个不同的告警名**。这是**平台侧既有数据**，不是本波次引入的。
+**处理**：断言改为「① `alarm_id` 254 个互不相同（告警种类靠 `alarm_id` 承载 ——
+这正是合同 R3 的原话）；② `alarm_name` 非空且**逐条对得上平台目录**
+（`test_alarm_names_match_the_platform_catalog`）—— 证明统一指标名**没有动过告警名**」。
+**没有**为了让断言通过而改平台目录数据。
+
+### 5.4 运行期兜底也改了（超出任务字面，但同一口径）
+
+`collectors/alarm.py` 的 `_definition_for()` 原来在告警定义缺失时自造
+`gaussdb.alarm.<alarm_id>`。本波次改成用 `alarm_rules.json` 的 `event_metric`。
+理由：R3 要求「不挂靠客户指标的事件类告警 `metricCode` **全部**等于同一个固定值」，
+而兜底造出来的定义 `linked_customer_metric` 也是空 —— 留着旧写法就会有两个口径；
+且 R6 要求「SHALL NOT 硬编码客户口径」。**判断**：这是必要的收口，请 reviewer 确认。
+
+### 5.5 ruff 门禁**在本仓库基线上就不绿**（非本波次引入）
+
+- 环境里的 ruff 是 **0.17.6**，`ruff.toml` 的 `select` 含 `UP` —— 于是它把**全仓**
+  `%` 格式化（`UP031`）、`from __future__ import`（`UP010`）、UTF-8 声明（`UP009`）
+  全判成问题。
+- 实测基线（`git stash` 后同样命令）：`tpops_kafka_collector tests tools` → **473 条**
+  （ruff 0.1.15）/ **764 条**（0.17.6）。**基线就红。**
+- 本波次改动后：494 / 808 条，**新增的 21 条全部是仓库里本来就有的那几类**
+  （`UP031` / `UP009` / `UP010` / `UP020` / `E501`），与既有代码风格一致。
+- 我**没有**为了「让 ruff 变绿」去改全仓风格（那是 wave-4 的收口范围，且会污染本波次 diff）。
+- `requirements-dev.txt` **没有钉 ruff 版本** —— 建议 wave-4 顺手钉一个版本，
+  否则这道门禁的结果取决于开发机上装的是哪个 ruff。
+- 我自己新增的代码里**唯一一条客观问题**（`E731` lambda 赋值）已改成 `def`；
+  唯一一条超长行（101 列）已折行。
+
+### 5.6 为什么只有 1 个提交
+
+任务 1.1~1.8 的产物**彼此强耦合**：`metric_rules.json` 同时含 1.2（剔除 14 条）
+与 1.5（改绑 `schemaname.relname`）的效果；`test_excluded_metrics.py` 同时含
+1.1 与 1.2 的断言；`test_delivery_docs.py` 的期望值依赖 1.2 之后的 87 条口径。
+拆成 8 个提交会让**中间提交跑不过测试**（例如「校验生效条数 = 87」在
+「映射表还是 101 条」的那个提交上必然红）。因此合并为 1 个提交，
+逐任务的 RED/GREEN 证据见 §1。
+
+### 5.7 其他未在本波次处理、但 reviewer 可能关心的
+
+- **`CLAUDE.md` 里仍写着「98 可提供 / 3 丢弃 / 101 条」** —— wave-2 的输出清单里
+  含「`CLAUDE.md` 约定更新」，为避免与 wave-2 抢同一文件，本波次**没动**它。
+  建议 wave-2 顺手改成 87 / 14 / 101。
+- **`docs/问题单.md` 的 TPOPS-001（跨 schema 同名表）与 TPOPS-003（11 组重复）**
+  现在都已由本波次解决，但台账里的勾选框**没动**（等 review 结论）。
+- **`docs/开发方案-GaussDB指标与告警采集.md`** 是**设计期**文档，里面的
+  101 / 98 / 82 是当时的口径，按「规划工件不改」处理，未动。
+- **`data/mapping/README.md` 里还有更早的过期描述**（指向已删除的 `tools/mappings/`、
+  「288 条告警定义」等），只修了与本波次口径直接相关的几处。
+
+---
+
+## 6. 复现命令
+
+```bash
+cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
+conda run -n tpops python -m pytest -q                       # 1841 passed
+conda run -n tpops python -m pytest \
+    tests/unit/test_excluded_metrics.py \
+    tests/unit/test_dimension_binding.py \
+    tests/unit/test_alarm_metric_unification.py \
+    tests/unit/test_metric_catalog.py \
+    tests/unit/test_delivery_docs.py -q                      # 93 passed
+conda run -n tpops python -m pytest tests/test_py37_compat.py -q   # 170 passed
+conda run -n tpops python -m ruff check tpops_kafka_collector tests tools   # 基线已红，见 §5.5
+```
+
+生成器幂等（重跑输出不变，已验证）：
+
+```bash
+for t in build_customer_metric_mapping build_metric_query_plan build_metric_value_rules \
+         build_itscada_mapping build_response_rules build_kafka_mapping \
+         build_expected_values build_delivery_docs; do
+    conda run -n tpops python tools/$t.py
+done
+# 再把包内副本同步过去：git status 不应有新变化
+```
diff --git a/changes/tpops-mapping-and-logging/.superpowers/sdd/workflow-selection.json b/changes/tpops-mapping-and-logging/.superpowers/sdd/workflow-selection.json
new file mode 100644
index 0000000..0866b15
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/.superpowers/sdd/workflow-selection.json
@@ -0,0 +1,70 @@
+{
+  "schema_version": 2,
+  "available_modes": [
+    "full",
+    "hotfix",
+    "tweak",
+    "quick",
+    "lightweight"
+  ],
+  "facts": {
+    "task_count": 18,
+    "file_count": 22,
+    "config_doc_only": "no",
+    "schema_api_change": "no",
+    "new_module": "yes",
+    "behavioral_constraint_change": "yes",
+    "cross_module_change": "yes",
+    "uncertainty": "low",
+    "request_kind": "standard",
+    "affected_paths": [
+      "tpops_kafka_collector/mapping",
+      "tpops_kafka_collector/collectors",
+      "tpops_kafka_collector/logging_setup.py",
+      "tpops_kafka_collector/scheduler_report.py",
+      "deploy/collector.sh",
+      "data/mapping",
+      "tools/build_itscada_mapping.py"
+    ],
+    "exclusion_checks": {
+      "production_behavior": "yes",
+      "public_boundary": "yes",
+      "installer": "yes",
+      "state_machine": "no",
+      "external_side_effect": "no",
+      "data_permission_config_semantics": "yes",
+      "expected_behavior_clear": "yes",
+      "verification_reproducible": "yes",
+      "impact_paths_complete": "yes"
+    }
+  },
+  "missing_facts": [],
+  "status": "ready",
+  "recommendation": {
+    "mode": "full",
+    "reasons": [
+      "Risk signals require the user to choose Quick or Full."
+    ],
+    "risk_reasons": [
+      "behavioral constraint changed (PRD, spec, design, data, or permission)",
+      "new module",
+      "cross-module change"
+    ]
+  },
+  "created_at": "2026-11-26T02:10:20.395Z",
+  "selection": {
+    "mode": "full",
+    "reason": "用户明确选择 Full：跨模块（映射层/采集器/调度/部署脚本）+ 新增日志模块 + 对外消息契约变化",
+    "followed_recommendation": true,
+    "acknowledged_non_recommendation": false,
+    "accepted_automatically": false,
+    "risk_override": false,
+    "verification_strategy": null,
+    "scope_confirmation": null,
+    "verification_result": null,
+    "escalation_reason": null,
+    "confirmed_at": "2026-11-26T02:13:34.050Z",
+    "selected_at": "2026-11-26T02:13:34.050Z"
+  },
+  "hash": "sha256:2099f40609fe3e2c86d3d2b8b11ec3522573ea00e3076aa86b56ce0bfee4cb0c"
+}
diff --git a/changes/tpops-mapping-and-logging/design.md b/changes/tpops-mapping-and-logging/design.md
new file mode 100644
index 0000000..dd19804
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/design.md
@@ -0,0 +1,483 @@
+# 技术设计：指标清理、日志框架、字段映射修正
+
+## 已知事实与约束
+
+**现状事实（均已核实）**
+
+1. 客户指标库 `高斯DB-指标明细…xlsx`「指标库」sheet 共 **101 行**：
+   - **11 个表级指标各出现两次**：一次在「表信息」分类（中文名正常，如
+     `({#NAME})插入的行数` / 码 `gaussdb.{#NAME}.number.Of.insert.tuples`），
+     一次在「事务信息」分类（中文名是英文，如 `({#NAME})number of insert tuples` /
+     码 `GAUSSDB.{#NAME}.number.Of.insert.tuples`）——两条**仅英文码大小写前缀不同**；
+   - 另有 **14 条独立指标的中文名整体是英文**（`GAUSSDB.Buffers_Alloc`、
+     `GAUSSDB.Idle_connections`、`gaussdb.locks.accessexclusive[{#DBNAME}]` 等）——
+     它们**不重复**，本次**保留**（已与用户确认口径：只删 11 组重复 + 3 条无法提供）；
+   - **3 条** `无法提供`：`gaussdb.Fastpath_function_call`、`gaussdb.Disabled_conn`、
+     `gaussdb.sendAgentName`（当前在 `metric_rules.json` 里以 `enabled=false` 存在）。
+2. `mapping/data/metric_rules.json` 现有 **101 条**规则，**11 条重复的表级指标两条都在**、
+   都 `enabled=true` → 运行期对同一张表产出**两条不同的指标消息**
+   （`metricCode` 与 `metricName` 都不同），ITSCADA 侧看到重复指标。
+3. `tpops_kafka_collector/mapping/data/customer_metric_mapping.json` 由 `tools/build_itscada_mapping.py`
+   从客户 xlsx + 可提供性判定生成；**xlsx 是客户文件**，本次不改它。
+4. 告警定义 `mapping/data/alarm_rules.json` 共 **257 条**：3 条
+   `linked_customer_metric` 指向 `gaussdb.Tps`，254 条事件类告警的
+   `metric_code` 是 `gaussdb.alarm.<alarm_id>`、`metric_name` 是告警名。
+   `build_alarm_message` 直接取 `definition.metric_name` / `definition.metric_code`。
+5. 表级规则的 `dimension_bindings` 为 `{"NAME": "relname"}`，
+   `value_rule.params.group_by` 为 `["relname"]`；
+   合并视图 `stat_user_tables` 的**实测返回列已含 `schemaname`**（`observed_columns.json`）。
+6. 幂等键（`tpops_kafka_collector/ids.py`）= `instance_id + metric_name + 排序后的 dimensions + timestamp`；
+   `metric_name` 是占位符替换后的值。
+7. 日志：`tpops_kafka_collector/logging_setup.py` 只装了一个
+   `StreamHandler(sys.stderr)`；`deploy/collector.sh run` 用 `exec` **前台**启动；
+   无日志目录、无 PID 文件、无轮转。
+8. 每轮汇总：`scheduler_report.log_round_summary` 出一行 **tick 整体**口径
+   （`tick/elapsed/fetched/mapped/produced/failed/dropped_unsupported/windows/alarms/calls/delivery_rate/groups/skipped`）。
+   通道内数据散落在 `TaskOutcome`（`fetched` / `windows` / `external_calls`）
+   与 `DeliveryStats`（`planned` / `delivered` / `failed` / `alarms`）里，**没有按通道聚合**。
+9. 通道名固定五类（`schedule.py`）：`monitor` / `command_center` / `diagnostic` /
+   `instance` / `alarm`；每类有**各自的采集周期**（1 / 2 / 5 / 60 分钟分档）。
+10. **loguru 0.8.0 已在 Python 3.7.9 上实测**（2026-11-26）：
+    `requires_python >=3.5`；wheel 为 `py3-none-any`；
+    Linux + py3.7 下**运行期零依赖**（pip 实测未拉任何依赖）；
+    `rotation="1 day"` / `retention="3 days"` / `filter=` / `{time:YYYY-MM-DD}`
+    文件名 / `{level: <8}` / 异常堆栈全部原生可用；
+    同名冲突时 loguru 自动把旧文件改名为 `<基名>.<日期>_<时分秒_微秒>.log`。
+
+**不可破坏的约束**
+
+- Python **3.7.9** 语法红线（禁海象、内建泛型、`zoneinfo`、`cached_property`、
+  `str.removeprefix` 等）；依赖锁 `kafka-python==2.0.2` / `requests>=2.25,<3.2`。
+- 目标机：**麒麟 V10，离线**，无 logrotate、无 C 扩展、Python 3.7~3.11；
+  **x86_64 与 aarch64 共用同一份部署包**（只能用 `*-none-any.whl`）。
+- 采集端单文件 **< 400 行**、函数 **< 50 行**；不可变 DTO；`logging` 不用 `print`；
+  凭据只走环境变量、禁入日志。
+- ITSCADA 侧按 **`metricName` + `resourceName`** 做资源/指标映射，
+  指标名与资源名**必须稳定**——改名等于客户台账要跟着改。
+- 对外消息字段集合以客户两份对接方案的字段表为准，**不得自创字段**。
+
+## 目标与非目标
+
+**目标**
+
+1. 生效指标口径 = **87 条**，零重复、零「不可提供丢弃」。
+2. 日志**落盘**；`collector.sh run` **后台运行**；总日志能一眼看出
+   **各采集器这一轮的采集与推送情况**；每个采集器有自己的日志文件。
+3. 表级指标的 `{#NAME}` = **`schema_name.table_name`**，
+   不同 schema 的同名表**不碰撞**（消息、幂等键都不碰撞）。
+4. 不涉及客户指标名的告警，`metricName` / `metricCode` **统一为一个固定名**。
+5. 产出**日志框架规范与格式**（本文档 + 对应规格），供审核后实施。
+
+**非目标**
+
+- 不做日志外发/采集（shipper、fluent-bit、日志平台接入）。
+- 不改客户 xlsx 原件；不改其余 14 条英文中文名指标；不删交付文档里的
+  「不可提供指标」说明（改为标注「已从生效口径移除」）。
+- 不重做调度模型（仍是到期驱动 + 两维并发）；不引入新依赖。
+- 不做 P5 联调（等客户提供 Kafka 地址/Topic）。
+
+---
+
+## 决策（Decisions）
+
+### 决策 1：日志落盘位置与文件布局
+
+- **选择**：日志目录由环境变量 `LOG_DIR` 指定，**6 个基名**，每个基名**按天**一个文件，
+  文件名带**日期后缀**：
+
+  ```
+  <LOG_DIR>/collector-2026-11-26.log                  # 总日志
+  <LOG_DIR>/collector-monitor-2026-11-26.log          # A 类 监控指标
+  <LOG_DIR>/collector-command_center-2026-11-26.log   # B 类 命令中心
+  <LOG_DIR>/collector-diagnostic-2026-11-26.log       # C 类 诊断优化
+  <LOG_DIR>/collector-instance-2026-11-26.log         # D 类 实例元数据
+  <LOG_DIR>/collector-alarm-2026-11-26.log            # E 类 告警
+  ```
+
+  命名规则：`<基名>-<YYYY-MM-DD>.log`（loguru 的 `{time:YYYY-MM-DD}` 展开）；
+  同一天内超出单日上限时，loguru 把旧文件**改名为**
+  `<基名>-<YYYY-MM-DD>.<日期>_<时分秒_微秒>.log` 并新建同名文件继续写
+  （实测行为；被改名文件同样在保留期清理范围内）。
+
+  **`LOG_DIR` 未设置时**：进程只写 stderr（不落盘）——开发态的既有行为不变；
+  `collector.sh run` 则**总是**显式设置 `LOG_DIR`（默认 `<安装目录>/logs`），
+  所以生产态一定落盘。两种语义都由 `config.py` 显式判定，SHALL NOT 靠猜。
+
+  `<安装目录>` = **`collector.sh` 所在目录**（脚本里的 `$HERE`；目标机为
+  `/opt/tpops-collector`）。PID 文件 `<安装目录>/run/collector.pid`、
+  `console.out` `<安装目录>/run/console.out` 同属该目录。
+
+- **理由**：文件名按**通道基名**固定，不按指标/实例派生 → 无论 87 个指标还是 800 个指标，
+  日志基名恒为 6 个，运维 `tail -f` 的对象稳定；通道名与 `schedule.py` 的五个常量
+  一一对应，不引入第二套命名。**日期后缀**让「昨天的日志」一眼可辨、可单独拷贝，
+  清理也只需按文件名里的日期判断，不需要额外的索引文件。
+  `console.out` 放 `<安装目录>/run/` 而不是 `<LOG_DIR>/` → 清理策略只管
+  `collector*.log` 一族。
+- **替代方案**：
+  - *按实例分文件*（`collector-<instance>.log`）：实例数会变，文件数不可控；
+  - *单一大文件 + 采集器名过滤*：一条命令看不到「某采集器这一轮」，正是本次要解决的问题；
+  - *日期放后缀（`collector.log.2026-11-26`）*：`TimedRotatingFileHandler` 的默认形态，
+  但扩展名被挤到中间，不便按 `*.log` 收集与拷贝；
+  - *交给 journald*：目标机部署形态不是 systemd，且历史留存口径不由我们控制。
+- **后果**：新增 `LOG_DIR` 配置项；`config.py` 需加校验（目录不存在则创建，
+  不可写则**启动失败**，不静默降级为只写 stderr）。
+
+### 决策 2：sink 分层与「不重复、不串味」
+
+- **选择**：loguru 的 **sink** 模型（每个 `logger.add(...)` 是一个 sink，
+  用 `filter=` 决定哪些记录进哪个 sink）：
+
+  | sink | 收什么 | 落点 |
+  |---|---|---|
+  | 总日志 sink | 生命周期（启动/停止/装配/致命错误）、tick 整体汇总、**每通道一行**的简洁汇总、WARNING 及以上 | `<LOG_DIR>/collector-<日期>.log` |
+  | 通道 sink ×5 | 该通道内部的采集与推送明细 + 该通道每轮汇总行 | `<LOG_DIR>/collector-<channel>-<日期>.log` |
+  | stderr sink | 与总日志同内容（前台运行/调试时可见） | 终端 |
+
+  通道归属靠 `logger.bind(channel="…")` 打在记录上，
+  通道 sink 的 `filter=` 读 `record["extra"]["channel"]`。
+  5 个采集器模块各加一行 `LOGGER = logger.bind(channel="…")`；
+  `scheduler_report.py` 发通道汇总时逐通道 `logger.bind(channel=…)` 后发出。
+  未绑定 `channel` 的记录 → 只进总日志（不进任何通道 sink）。
+
+  第三方库（`kafka` / `urllib3` / `requests`）的 stdlib 日志经官方
+  `InterceptHandler` 转发进 loguru，并用 `logger.disable("kafka")` 等压噪。
+
+- **理由**：总日志要的是**简洁的汇总**，不是明细；明细进分日志。
+  用 sink + filter 表达「通道 INFO 不进总日志、通道 ERROR 进总日志」，
+  比 stdlib 的 `propagate` + 多 handler 更直白，且不需要每个 logger 单独配置。
+- **替代方案**：
+  - *每个通道一个 loguru logger 实例（`logger.bind`）而不是 sink filter*：
+    loguru 的 logger 是全局单例，bind 是**记录级**的，不是 logger 级的 →
+    分流只能靠 sink filter；
+  - *沿用 stdlib 的 `propagate` + 多 handler*：与决策 3 的「全面改 loguru API」冲突；
+  - *按文件路径分流（写各自的 logger 对象）*：loguru 不支持「logger 绑文件」。
+- **后果**：新增 `tpops_kafka_collector/logging_setup.py`（重写）负责装配 sink；
+  现有 `setup_logging(level, log_format, stream)` 签名**保持向后兼容**
+  （现有测试依赖它），新增 `log_dir` / `log_retention_days` / `log_max_bytes`
+  为可选参数。
+
+### 决策 3：日志框架选型 —— loguru（替代自研）
+
+- **选择**：日志框架用 **loguru 0.8.0**，不自研 handler / 轮转 / 保留 / 格式化。
+  项目内**全面改用 loguru API**（`from loguru import logger`），
+  第三方库（`kafka` / `urllib3` / `requests`）的 stdlib 日志经官方
+  `InterceptHandler` 转发进 loguru。
+
+  **已实测核实（Python 3.7.9，2026-11-26）**：
+
+  | 核实项 | 结论 |
+  |---|---|
+  | `requires_python` | `>=3.5` → 3.7.9 可用；实测 `import loguru` 成功 |
+  | wheel | `loguru-0.8.0-py3-none-any.whl` → 纯 Python，x86_64 / aarch64 共用一份部署包 |
+  | Linux + py3.7 运行期依赖 | **0 个**（`colorama` 仅 Windows、`aiocontextvars` 仅 py<3.7）；`pip download` 实测未拉任何依赖 |
+  | 文件名带日期 | `logger.add("collector_{time:YYYY-MM-DD}.log", …)` → 实测产出 `collector_2026-11-26.log` |
+  | 按天切分 | `rotation="1 day"` 原生 |
+  | 保留 3 天 | `retention="3 days"` 原生（实测 `retention` 支持 str / int / timedelta / callable） |
+  | 同日超限改名 | 同名冲突时 loguru **自动把旧文件改名**为 `<基名>.<日期>_<时分秒_微秒>.log` 并新建同名文件 |
+  | 通道分流 | sink 的 `filter=` 读 `record["extra"]["channel"]` → 实测通道文件只含本通道记录 |
+  | 级别对齐 | `{level: <8}` 原生 |
+  | 异常堆栈 | 原生追加（实测输出完整 traceback） |
+
+- **理由**：loguru 把「按天切分 / 文件名日期后缀 / 保留期 / 格式化 / 异常堆栈 /
+  通道分流」全部变成**一行配置**，自研这些要写 ~4 个模块（pattern 解析器、
+  按天切分 handler、保留清理、handler 装配）并各自测一遍 —— 换成 loguru 后
+  这部分代码**净减少**，且 loguru 的行为是**上游维护、被广泛使用**的，
+  不如自研轮转逻辑可靠。
+  同时满足本项目的三条红线（py3.7.9 / 纯 Python wheel / 离线目标机零额外依赖）。
+
+- **替代方案**：
+  - *继续用 stdlib `logging` + 自研*：要多写 4 个模块、多测一轮，且轮转/保留逻辑
+    长期要自己维护；
+  - *stdlib `logging` + loguru 的 `InterceptHandler`（调用点零改动）*：
+    改动最小，但项目里仍是 stdlib 风格，loguru 只当 sink 用，
+    「用 loguru 简化代码」的收益打折；用户已明确选**全面改成 loguru API**；
+  - *换 `structlog`*：py3.7 支持与纯 Python 均可，但按天切分/保留/通道分流
+    都要自己再包一层，比 loguru 的现成能力差；
+  - *引 `filebeat`/`fluent-bit` 收日志*：目标机离线、无额外二进制，与本次目标相反。
+- **后果**：
+  - **新增一个运行期依赖** `loguru==0.8.0`（纯 Python、零传递依赖）：
+    `requirements-runtime.txt` 要加；`tools/build_deploy_bundle.py` 的离线 wheel
+    清单要加；`CLAUDE.md` 里「运行时依赖只有 requests / kafka-python / urllib3
+    三个纯 Python 包」这句话要改。**不破坏**「x86_64 与 aarch64 共用一份部署包」。
+  - 项目编码约定要改：`CLAUDE.md` 的「用 `logging`，不用 `print`」改为
+    「用 `loguru` 的 `logger`，不用 `print`」；
+    `LOGGER = logging.getLogger(__name__)` 改为 `from loguru import logger`。
+  - **约 30 个模块**的 logger 获取方式要改（调用点 `LOGGER.info(...)` 本身不变，
+    只改 import 与 logger 变量的创建）。
+  - 通道分流靠 `logger.bind(channel="…")`：5 个采集器模块各加一行
+    `LOGGER = logger.bind(channel="…")`，`scheduler_report.py` 发通道汇总时
+    用 `logger.bind(channel=…)` 逐通道发。
+  - 第三方库降噪改用 loguru 的 `logger.disable("kafka")` 等。
+
+### 决策 4：格式与 sink（loguru 原生，不强加 logback）
+
+- **选择**：**直接用 loguru 的原生格式**，不自研 logback pattern 解析器。
+  - **文本 sink**：用 loguru 默认格式；可用 `LOG_FORMAT`（loguru 原生字符串语法，
+    如 `{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name} | {message}`）覆盖。
+    loguru 默认格式已含 `{time}` / `{level}` / `{name}:{line}` / `{message}`，
+    信息量与 logback 默认 pattern 相当。
+  - **JSON sink**：用 loguru 的 `format=` **自定义函数**（不是字符串），
+    产出既有 schema：`timestamp` / `level` / `logger` / `thread` / `message`，
+    整条消息是 `k=v` 形式时**摊平成顶层字段**，异常带 `exception` 字段。
+    （这一点 loguru 的 `serialize=True` 做不到 —— 它输出的是
+    `{"text": …, "record": {…}}` 固定结构。）
+
+- **理由**：用户明确「直接用 loguru 的日志格式即可，不用强制 logback」。
+  loguru 的原生格式已经足够（时间/级别/模块:行号/消息），且客户可自行覆盖。
+  自研 logback pattern 解析器（把 `%d{…}` 翻译成 `{time:…}`）要多写一个模块、
+  多测一轮，换来的只是「能贴 logback 配置」这一点。
+
+- **替代方案**：
+  - *自研 logback pattern 解析器*：用户已否决；
+  - *`serialize=True`*：schema 不符（`text` + `record` 嵌套），客户日志平台
+    按 `channel` / `fetched` 取值要二次展开；
+  - *只用文本、放弃 JSON*：客户侧日志平台按字段取值的诉求落空。
+- **后果**：
+  - 汇总行仍必须写成纯 `k=v` token（值不含空格），否则 JSON 摊平失效；
+  - 文本 sink 的默认格式含 `{name}:{line}` —— 调用点行号对定位问题有用，
+    但行号会随代码变动，**不应被当作稳定契约**。
+
+### 决策 5：按天切分与保留策略（loguru 原生 + 一个 rotation 回调）
+
+- **选择**：用 loguru 的 `rotation` + `retention`，两个参数：
+
+  | 参数 | 环境变量 | 默认 | 落地方式 |
+  |---|---|---|---|
+  | 同日上限 | `LOG_MAX_BYTES` | **50 MB** | `rotation=` **自定义回调**：当日文件 `tell() + len(message)` 超限 → 立即切 |
+  | 保留天数 | `LOG_RETENTION_DAYS` | **3** | `retention="3 days"`（保留最近 3 个自然日，更早的自动删） |
+  | 跨天切分 | —— | 次日 00:00 | 文件名里的 `{time:YYYY-MM-DD}` + rotation 回调判定跨天 |
+
+  文件名：`<基名>-<YYYY-MM-DD>.log`（loguru 的 `{time:YYYY-MM-DD}` 展开）。
+  同日超限时，loguru 把旧文件**改名为** `<基名>-<YYYY-MM-DD>.<日期>_<时分秒_微秒>.log`
+  并新建同名文件继续写（实测行为）；被改名的文件同样落在 `retention` 的清理范围内
+  （glob 是 `<基名>-*.log`）。
+
+- **理由**：`rotation` 原生只支持「时间」或「大小」二选一，本需求要
+  「按天 + 同日上限」两个条件 → 用**一个 rotation 回调**同时表达
+  （loguru 的 callable rotation 语义实测确认：返回真值即立即切）。
+  `retention="3 days"` 正是用户要的「保留 3 天」，且**含**被改名产生的同日分片。
+
+- **替代方案**：
+  - *loguru 原生 `rotation="50 MB"`*：失去按天切分与日期文件名，不满足需求；
+  - *loguru 原生 `rotation="1 day"`*：满足按天切分，但同日无上限；
+  - *自研 handler*：与决策 3 冲突（又要把轮转逻辑写回自己手里）。
+- **后果**：磁盘占用上限 ≈ `6 基名 × 3 天 × 50 MB`（最坏 ~900 MB，实际远低于此）；
+  需在部署说明里写清保留口径与上限估算。
+
+### 决策 6：每通道每轮汇总的统计口径
+
+- **选择**：每轮（tick）内**到期并完成**的通道各输出一条汇总行，字段固定：
+
+  ```
+  channel=command_center tick=12 instances=1 fetched=520 mapped=498 produced=498
+  failed=0 windows=11 calls=4 elapsed=3.2s delivery_rate=1.000
+  ```
+
+  | 字段 | 来源 | 定义 |
+  |---|---|---|
+  | `channel` | 通道名（`schedule.py` 的常量） | — |
+  | `tick` | 轮次序号 | — |
+  | `instances` | 本轮该通道参与的实例数 | — |
+  | `fetched` | 该通道各任务 `TaskOutcome.fetched` 之和 | 从平台取回的原始行数 / 告警条数 |
+  | `mapped` | 该通道各实例 `DeliveryStats.planned` 之和 | **幂等过滤后**实际尝试投递的消息数（与整体行的 `mapped` 同口径） |
+  | `produced` | 该通道各实例 `DeliveryStats.delivered` 之和 | 成功投递的消息数 |
+  | `failed` | 该通道各实例 `DeliveryStats.failed` 之和 | 投递失败的消息数 |
+  | `windows` | 该通道各任务 `TaskOutcome.windows` 之和 | 本轮该通道执行的 SQL 条数 |
+  | `calls` | 该通道各任务 `TaskOutcome.external_calls` 之和 | 本轮该通道的外部调用次数 |
+  | `elapsed` | 该通道本轮耗时（秒） | 从该通道任务开始到全部完成的墙钟 |
+  | `delivery_rate` | `produced / mapped` | `mapped=0` 时为 `1.000` |
+
+  **未到期的通道不输出汇总行。** 该行用 `logger.bind(channel="<通道名>")` 打出
+  （进该通道日志），同时在 `collector.summary` 上打一条**同字段、只留通道核心计数**的行
+  （进总日志）。总日志因此能一眼看出各采集器这一轮的获取量/推送量/耗时。
+  **通道行各字段之和 SHALL 等于整体行同名字段的值**（同一取数来源，只是分组更细）。
+
+- **理由**：`fetched` / `windows` / `calls` 目前已随 `TaskOutcome` 逐任务带着走，
+  `DeliveryStats` 已按 (通道, 实例) 给出投递计数 → 按通道聚合是**纯加法**，
+  不需要改采集器内部逻辑。`elapsed` 用通道级墙钟（任务并发，取最晚完成者），
+  与「这一轮该采集器跑了多久」这个运维问题的口径一致。
+- **替代方案**：
+  - *复用现有整体行的 `groups` 字段塞通道明细*：会把 JSON 摊平打乱，
+    且明细量随通道数增长，不符合「总日志要简洁」；
+  - *逐条消息记明细到通道日志（INFO 级）*：量太大（87 指标 × 多实例 × 周期），
+    实测单轮几百到几千条，INFO 级不可接受。**明细留 DEBUG 级**，
+    默认 `LOG_LEVEL=INFO` 不落。
+- **后果**：`scheduler_report.py` 需从「只出整体行」扩成「出整体行 + 通道行」；
+  `scheduler.py` 的 `_build_result` 需带上按通道聚合的结果（新增不可变 DTO
+  `ChannelRoundStats`）。字段名沿用现有 `TickResult` 的命名（`fetched` / `mapped` /
+  `produced` / `failed` / `windows` / `calls`），**不新造同义字段**。
+
+### 决策 7：`collector.sh run` 后台化
+
+- **选择**：
+
+  | 子命令 | 行为 |
+  |---|---|
+  | `run` | **后台**启动：写 PID 文件（`<安装目录>/run/collector.pid`），命令返回后进程继续跑 |
+  | `run --foreground` | **前台**启动（原行为），systemd 用 |
+  | `status` | 报告在否 / PID / 运行时长 / 日志目录 / 日志尾部 |
+  | `stop` | 按 PID 文件发 SIGTERM，优雅停止 |
+
+  后台启动用 `setsid nohup … >> "<安装目录>/run/console.out" 2>&1 &`，
+  启动后等待并确认进程存活；**进程没起来则返回非零并打印日志尾部**
+  （`<LOG_DIR>/collector.log` 的最后 N 行 + `console.out` 的最后 N 行）；
+  **已有实例在跑则拒绝启动**并返回非零。
+  `<安装目录>/run/` 目录由脚本负责创建（不存在则创建，存在则复用）。
+
+- **理由**：用户要求「启动时不是进入日志，而是在后台运行，日志文件要落盘」。
+  `setsid` + `nohup` 让进程脱离终端会话，终端关闭或 SSH 断开都不会带走它；
+  stdout/stderr 仍重定向到 `console.out`（Python 崩溃前的最后输出不会丢）。
+  **保留 `--foreground` 是硬约束**：systemd 单元要求前台进程，
+  直接把 `run` 改成后台会**打断现有 systemd 部署方式**——
+  交付说明里的 `ExecStart` 必须同步改成 `run --foreground`。
+- **替代方案**：
+  - *只把 `run` 改后台、不留前台模式*：systemd 托管不了，运维失去 systemd 的
+    重启与状态管理能力；
+  - *用 `systemd-run`/supervisor 托管*：目标机不一定有 systemd（部署形态是脚本 + 手工）；
+  - *PID 文件放状态目录*：状态目录（`STATE_DIR`）由 systemd `StateDirectory=` 建的，
+    手工部署时可能不存在 → PID 文件跟安装目录走，与 `.install-mode`、`.python-bin` 同侧。
+- **后果**：`deploy/collector.sh` 新增约 120 行（仍在 < 400 行约束内？
+  当前 930 行 → **会超 400 行**）。因此把 `run` / `status` / `stop` 三段
+  拆到**新文件** `deploy/collector_runtime.sh`，由 `collector.sh` 位置参数分发
+  （保持「一个脚本」的对外形态不变）。部署说明与 `deploy/README.md` 同步更新。
+
+### 决策 8：指标清理的落地方式（不改正典，改生效口径）
+
+- **选择**：新增映射数据文件
+  **`tpops_kafka_collector/mapping/data/excluded_metrics.json`**（包内为运行期真相源，
+  仓库根 `data/mapping/excluded_metrics.json` 为同源副本）：
+
+  ```json
+  {
+    "version": "1.0",
+    "excluded": [
+      {"metric_code": "GAUSSDB.{#NAME}.number.Of.insert.tuples",
+       "metric_name": "({#NAME})number of insert tuples",
+       "reason": "duplicate",
+       "kept": "gaussdb.{#NAME}.number.Of.insert.tuples",
+       "detail": "与「表信息」分类的同指标重复，仅英文码大小写前缀不同"},
+      {"metric_code": "gaussdb.Fastpath_function_call",
+       "metric_name": "快路径函数调用的连接数",
+       "reason": "unsupported",
+       "detail": "PG 侧无 fastpath 计数器；pg_settings 仅有 gsi_fastpath_level 开关"}
+    ]
+  }
+  ```
+
+  字段约定：`metric_code` / `metric_name` / `reason` / `detail` **必填**；
+  `kept` **仅 `reason=duplicate` 时必填**（`unsupported` 无「保留哪一条」）。
+
+  `tools/build_itscada_mapping.py` 生成 `metric_rules.json` 与
+  `customer_metric_mapping.json` 时按此清单剔除；生效口径 = **101 − 11 − 3 = 87**。
+  客户 xlsx **保持原样**。
+  排除发生在**构建期**（生成器），运行期加载到的生效映射表里**根本不存在**这些指标码
+  → 运行期没有「遍历到再丢弃」的动作，也就没有对应的丢弃日志。
+
+- **理由**：xlsx 是**客户文件**，我们在仓库里改它会造成「客户原件与我们的理解不一致」，
+  将来对账说不清。排除清单把「删了什么、为什么删、保留了哪条」变成**可审阅的数据**，
+  正好也是交付文档「不可提供指标清单」的素材来源。
+  运行时**不再有**「遍历到不可提供指标再丢弃」的动作 → `dropped_unsupported` 恒为 0。
+- **替代方案**：
+  - *直接改 xlsx*：污染客户原件，且下次客户给新版本会覆盖掉改动；
+  - *在代码里写死 14 个码*：违反「映射层表驱动」红线；
+  - *保留 3 条无法提供指标为 `enabled=false`*：用户明确要求「删除」，
+    且留着会让「不可提供指标清单」永远显示 3 条，客户以为还要等我们补齐。
+- **后果**：
+  - `tpops_kafka_collector/mapping/data/metric_rules.json` 87 条、`enabled` 全为 `true`；
+  - `tpops_kafka_collector/mapping/data/customer_metric_mapping.json` 87 条；
+  - 启动期校验「映射表条目 ↔ 生效口径」改为与排除清单互斥校验
+    （排除清单的指标码一个都不在生效映射表中）；
+  - `docs/交付-指标口径说明.md` 重生成（87 条）、
+    `docs/交付-不可提供指标清单.md` 重生成（3 条，标注「已从生效口径移除」+ 理由）；
+  - `tools/build_delivery_docs.py` 与 `tools/build_itscada_mapping.py` 都要读排除清单。
+
+### 决策 9：告警统一指标名（表驱动固定值）
+
+- **选择**：在 `mapping/data/alarm_rules.json` 增加一段：
+
+  ```json
+  "event_metric": {"metric_code": "gaussdb.alarm", "metric_name": "数据库告警"}
+  ```
+
+  生成器在写 254 条事件类告警定义时，`metric_code` / `metric_name` 统一取该值；
+  3 条 `linked_customer_metric` 非空的告警**保留**客户指标的值不变。
+  该固定值同时登记进 `open_items.json`（待客户确认）。
+
+- **理由**：告警种类由 `alarmId` / `alarmName` / `alarmContent` 承载，
+  `metricCode` 逐告警自造（254 份）没有信息量，却让 ITSCADA 台账要维护
+  254 个「指标」。统一后 ITSCADA 侧只有一个告警类指标。
+  值放在映射数据里 → 客户改口径不用改代码（符合表驱动红线）。
+- **替代方案**：
+  - *保留 `gaussdb.alarm.<alarm_id>`、只统一 `metricName`*：用户已明确
+    「metricName 与 metricCode 都统一」；
+  - *统一成一条带告警类型的名（如「数据库告警-锁等待」）*：仍是 254 个，
+    等于没统一。
+- **后果**：
+  - ITSCADA 侧需要**一个**告警指标台账项（不是 254 个）——这是**对外契约变化**，
+    需在交付说明里写清楚；
+  - 告警**幂等键不含** `metric_code`（键是 `alarm_id + instance_id +
+    first_alarm_time + status`）→ 统一化不影响去重语义；
+  - `alarm_rules.json` 的 `metric_code` 不再唯一 → 启动期校验的**唯一性约束要相应放宽**：
+    **唯一性只对指标规则（`metric_rules.json`）要求**，对告警定义不要求
+    （告警定义靠 `alarm_id` 唯一定位）。这条放宽落在任务 1.7。
+
+### 决策 10：表级 `schema_name.table_name` 的组合位置
+
+- **选择**：**在映射层组合**，不动合并 SQL：
+
+  - `dimension_bindings`：`{"NAME": "relname"}` → `{"NAME": "schemaname.relname"}`
+    （值里的 `.` 表示「按 `.` 拆成多列，用 `.` 连接各列的值」）；
+  - `value_rule.params.group_by`：`["relname"]` → `["schemaname", "relname"]`
+    （按两列分组，避免同名表被并成一组）。
+
+  **生成器落点（已核实）**：`tools/build_itscada_mapping.py` 的
+  `DIMENSION_COLUMN_BY_SQL` 里 `'stat_user_tables': {'NAME': 'relname'}` 这一行改为
+  `'schemaname.relname'`；而 `group_by` 是由 `_row_policy` 从
+  `dimension_bindings` 的值派生（`group_by = sorted(set(bindings.values()))`），
+  因此**还要把派生处的「点号串」拆成多列**，否则 `group_by` 会变成一个
+  叫 `schemaname.relname` 的单列（分组失效）。落点是生成器里的一处常量 + 一处派生逻辑。
+
+  于是 `{#NAME}` 被替换成 `public.t1`；`metricName` = `(public.t1)插入的行数`；
+  `metricCode` = `gaussdb.public.t1.number.Of.insert.tuples`；
+  `tags.dimension_values.NAME` = `public.t1`；
+  幂等键里的 `dimensions` 也随之带上 `public.t1` → **不同 schema 不再碰撞**。
+
+- **理由**：合并视图 `stat_user_tables` 的**实测返回列已含 `schemaname`**
+  （`observed_columns.json` 里 `stat_user_tables` 与 `dbstat_sum` 都有），
+  所以数据是现成的，改动只在映射层。改合并 SQL 会波及 `observed_columns.json`
+  校验、11 条视图的探针留档与 21 条 SQL 的口径，**收益远小于风险**。
+  组合放在映射层 → 客户若要换成 `schema.table` 之外的格式，只改映射表。
+- **替代方案**：
+  - *在合并 SQL 里加一列 `schemaname || '.' || relname as fullname`*：
+    能少写一点组合逻辑，但要动 1 条合并 SQL + 全部列名校验 + 留档；
+  - *运行时在 `substitute_placeholders` 里特判表级*：把「表名怎么拼」写死在代码里，
+    违反表驱动红线；
+  - *用 `dimension_bindings` 直接绑两个键（`SCHEMA` / `TABLE`）*：
+    客户指标码里只有一个占位符 `{#NAME}`，拆两个占位符会让
+    `metricCode` 变成 `gaussdb.{#SCHEMA}.{#TABLE}…`，**与客户指标库对不上**。
+- **后果**：
+  - `dimension_bindings` 的取值语义扩展为「列名或点号连接的列名列表」→
+    `rules_context.row_dimensions` 与 `grouped_items` 需支持组合与多列分组；
+  - 表级指标的 `metricCode` **每个表一个**（`gaussdb.public.t1.…`）→
+    ITSCADA 侧按 `metricName` 匹配台账，**表级指标的台账项要按实际表名录入**
+    （交付说明里必须写清：表级指标的 `metricName` 含库表前缀）；
+  - `{#NAME}` 里的 `.` 与指标码自身的 `.` 会连在一起（如
+    `gaussdb.public.t1.number.Of.insert.tuples`）——占位符替换是纯文本替换，
+    不会歧义，但**交付文档要给出示例**，避免客户误读。
+
+---
+
+## 风险与验证
+
+| 风险 | 缓解 | 验证证据 |
+|---|---|---|
+| 删掉 11 条重复指标后，客户 ITSCADA 台账仍留着旧的 254/101 项，映射对不上 | 交付说明与 `docs/交付-客户指标英文名核对清单.md` 增列「已移除项 + 理由」；`open_items.json` 登记 | 交付文档重生成后含 87 条口径 + 14 条移除说明；`pytest` 中「生效口径 = 87」断言 |
+| 表级 `metricCode` 变成「每表一个」，客户台账需要按表录入 | 交付说明写明表级指标的命名形式与示例；`metricName` 仍是 `({#NAME})…` 形式 | `docs/交付-指标口径说明.md` 中表级条目示例含 `public.t1` |
+| 告警统一成一个指标后，客户侧看不到告警种类 | `alarmId` / `alarmName` / `alarmContent` 仍逐条携带；统一值登记进 `open_items.json` 等客户确认 | 告警消息样例测试：254 条事件类告警的 `metricCode` 相同、`alarmName` 各不相同 |
+| 日志量大导致磁盘写满 | 按天切分 + 单日 50 MB 上限（loguru rotation 回调）+ 保留 3 天自动删除；通道明细只在 DEBUG 级 | 切分单测：写满后旧文件被改名、新文件续写；清理单测：过期文件被删、近 3 天保留 |
+| 新增 loguru 依赖破坏「纯 Python 三件套」与离线部署 | loguru 0.8.0 是 `py3-none-any`、Linux+py3.7 零传递依赖 → x86_64/aarch64 仍共用一份部署包 | 部署包构建测试断言 loguru wheel 在清单内且为纯 Python；目标机 `check-deps` 通过 |
+| 全面改 loguru API 触及 ~30 个模块，易漏 | 改造按「只改 logger 获取方式、调用点不动」推进；`ruff` + 全量 pytest 兜底；未改造的模块仍走 stdlib → 由 `InterceptHandler` 兜住，不会丢日志 | 全量 pytest 全绿；新增测试断言生产包内不再出现 `logging.getLogger` |
+| `collector.sh run` 改后台后 systemd 托管失效 | 保留 `run --foreground`；交付说明的 `ExecStart` 同步改 | `deploy/README.md` 与 `docs/交付-部署运行说明.md` 里的 unit 片段为 `run --foreground`；脚本测试覆盖 `run` / `status` / `stop` / 重复启动 |
+| 后台进程起不来但脚本返回 0 | 启动后确认进程存活，不存活则返回非零并打印日志尾部 | 脚本测试：故意用坏解释器 → `run` 返回非零 |
+| 日志文件泄露凭据 | 日志内容审查在采集器侧；新增「落盘文件不含凭据」的测试 | 落盘日志测试：跑一轮后 grep 日志文件无口令/token/`sql_text` |
+| 通道级统计口径与现有整体口径打架（同一字段两套含义） | 通道行复用现有字段名与取数来源（`TaskOutcome` / `DeliveryStats`），不加新字段 | 通道汇总的 `fetched` 之和 == 整体行的 `fetched`（单测断言） |
diff --git a/changes/tpops-mapping-and-logging/execution-contract.md b/changes/tpops-mapping-and-logging/execution-contract.md
new file mode 100644
index 0000000..0900e78
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/execution-contract.md
@@ -0,0 +1,312 @@
+# 执行合同：指标清理、日志框架（loguru）、字段映射修正
+
+> 语言：中文（`artifact_language=zh`）。标识符、字段名、路径、命令**逐字**保留。
+> 规划工件：`proposal.md` / `specs/`（3 份增量）/ `design.md` / `tasks.md`（DP-2 已批准，修订版 2）。
+
+## Intent Lock
+
+- **变更名称**：`tpops-mapping-and-logging`
+- **要解决的问题**（三个已观察到的事实）：
+  1. **表级指标重复投递** —— 客户指标库 101 行里 11 个表级指标各出现两次
+     （「表信息」分类中文名正常 vs「事务信息」分类中文名是英文，英文码仅大小写前缀不同），
+     两行都被 `metric_rules.json` 收录且 `enabled=true` → 同一张表产出两条不同指标消息。
+  2. **日志只到 stderr、无通道粒度** —— `logging_setup.py` 只装了一个
+     `StreamHandler(sys.stderr)`，`collector.sh run` 以前台启动，日志不落盘；
+     每轮汇总只有 tick 整体口径，看不到哪类采集器拉了多少、推了多少、耗时多久。
+  3. **两个字段映射缺陷** —— 254 条事件类告警各造一个 `gaussdb.alarm.<alarm_id>`；
+     表级 `{#NAME}` 绑到 `relname` → 不同 schema 的同名表得到**相同**的
+     `metricName`/`metricCode`/幂等键 → 后一条被**静默丢弃**。
+- **范围内**：
+  - 生效指标口径 101 → **87**（删 11 条重复的英文变体 + 3 条无法提供）；
+  - 日志框架换成 **loguru 0.8.0**，落盘（总日志 + 5 个通道日志，**文件名带日期后缀**）、
+    **按天切分**、**保留 3 天**、每类采集器**每轮一行**汇总（获取量/已推送量/耗时等）；
+  - `collector.sh run` 改**后台运行**（PID 文件），保留 `run --foreground`（systemd）、
+    新增 `status` / `stop`；
+  - 告警统一指标名（`metricName` + `metricCode` 都统一）；
+  - 表级指标 `{#NAME}` = **`schema_name.table_name`**，不同 schema 不碰撞。
+- **范围外**：
+  - 客户 Kafka 地址 / Topic 的 P5 联调（等客户提供）；
+  - **不改客户 xlsx 原件**（删除只落在排除清单）；
+  - 14 条「中文名是英文」的**独立**指标（`GAUSSDB.Buffers_Alloc` 等）——**保留**；
+  - 其余 8 项 `open_items.json` 待客户确认项；
+  - 日志外发 / 日志平台接入 / systemd unit 的正式交付；
+  - 采集端其它行为（调度模型、并发、水位、幂等语义本身）不变。
+
+## Approved Behavior
+
+19 条需求（3 份增量 spec 的全部 `### Requirement`），编号 `R1..R19`：
+
+| # | 能力 | 需求 | 关键场景（WHEN → THEN 摘要） |
+|---|---|---|---|
+| R1 | mapping | 排除清单 | 清单在包内 `mapping/data/excluded_metrics.json`；`duplicate` 必带 `kept`；排除发生在**构建期**，运行期无丢弃分支 |
+| R2 | mapping | 生效指标口径为 87 条 | 87 条规则、`enabled` 全 true、`metric_code` 唯一、`dropped_unsupported == 0` |
+| R3 | mapping | 告警的统一指标名 | 未挂客户指标的事件类告警 `metricCode` 相同、`metricName` 相同；挂靠客户指标的不变；统一值表驱动 |
+| R4 | mapping | 维度展开（改） | 表级 `{#NAME}` = `schemaname.relname`；按两列分组；`public.t1` 与 `other.t1` 的 `metricCode` 不同 |
+| R5 | mapping | 不可提供指标的处置（改） | 被排除指标不在生效口径内；书面清单标注「已从生效口径移除」 |
+| R6 | mapping | 映射表驱动（改） | 排除清单是数据文件；改口径不改代码 |
+| R7 | mapping | 指标幂等键（改） | `metric_name`/`dimensions` 任一不同 → 键不同 |
+| R8 | runtime | 日志目录与文件布局 | 6 基名 × 日期后缀；`LOG_DIR` 未设置 → 只写 stderr；不可写 → 启动失败 |
+| R9 | runtime | 总日志与通道日志的分工 | 总日志有整体汇总 + 每通道一行；通道日志只有本通道；通道 ERROR 两边都有 |
+| R10 | runtime | 每通道每轮汇总行 | 11 个固定字段；未到期通道不出行；通道行之和 == 整体行 |
+| R11 | runtime | 日志格式 | text 用 loguru 原生格式（`LOG_FORMAT` 可覆盖）；json 含 `timestamp/level/logger/thread/message` + `k=v` 摊平 + `exception` |
+| R12 | runtime | 日志切分与保留 | 按天切；同日超 50 MB 改名切分；保留 3 个自然日（含同日分片） |
+| R13 | runtime | 采集端后台运行 | `run` 后台 + PID 文件 + 重复启动拒绝 + 启动失败打印日志尾部；`run --foreground` 前台；`status`/`stop` |
+| R14 | runtime | 日志不落敏感信息 | 落盘文件无口令 / mini-token / `sql_text` 全文 |
+| R15 | runtime | 日志框架为 loguru | 全面 loguru API；第三方库经 `InterceptHandler` 转发；py3.7.9 / 纯 Python / 零传递依赖 |
+| R16 | runtime | 每轮结构化汇总日志（改） | 整体口径行只进总日志；通道口径由 R10 承担 |
+| R17 | runtime | 凭据仅来自环境变量且不入日志（改） | 文件日志同样不含凭据 |
+| R18 | delivery | 幂等去重（改） | 同名表不同 schema 的两条消息键不同、都被投递；同表重复采集仍去重 |
+| R19 | delivery | 业务维度的落点（改） | 表级 `tags.dimension_values.NAME` = `public.t1`；`resourceName` 不含 `t1` |
+
+- **验收检查**（全部可机械判定）：
+  1. `tpops_kafka_collector/mapping/data/metric_rules.json` 恰好 87 条规则、0 条 `enabled=false`、`metric_code` 无重复；
+  2. `excluded_metrics.json` 恰好 14 条（11 `duplicate` + 3 `unsupported`），每条有理由，重复项有 `kept`；
+  3. 未挂客户指标的告警 `metricCode` 全部等于同一固定值；
+  4. 表级 `tags.dimension_values.NAME` 形如 `public.t1`，`public.t1` 与 `other.t1` 的 `metricCode` 与幂等键不同；
+  5. `<LOG_DIR>` 下出现 6 基名 × 日期后缀的日志文件；跨天产生新日期文件、超过 3 天的文件被删；
+  6. 每个通道日志里能看到该通道每轮的 11 字段汇总行；总日志里能看到各通道的简洁汇总；
+  7. `bash tests/scripts/test_collector_runtime.sh` 全绿；
+  8. `conda run -n tpops python -m pytest -q` 全绿；覆盖率**全包 ≥ 80%、映射层 ≥ 90%**；
+     `conda run -n tpops python -m pytest tests/test_py37_compat.py -q` 通过；
+     `conda run -n tpops python -m ruff check tpops_kafka_collector tests` 无告警。
+
+## Design Constraints
+
+- **架构约束**
+  - 采集端代码单文件 **< 400 行**、函数 **< 50 行**；按功能分模块，不堆大文件；
+    `scheduler.py` 已 392 行 → 通道聚合逻辑放**新文件** `channel_stats.py`。
+  - 跨模块传递的数据 SHALL 用 frozen dataclass / 不可变 NamedTuple，SHALL NOT 原地修改。
+  - 错误显式处理：捕获后包装成自定义异常并带上下文；SHALL NOT 空 `except`；
+    SHALL NOT 用 `print` 记录日志。
+  - **映射层表驱动**：客户口径 SHALL 来自 `tpops_kafka_collector/mapping/data/*.json`
+    （运行期真相源；仓库根 `data/mapping/` 是同源副本），SHALL NOT 硬编码指标名、
+    列名、单位、维度键或排除清单。
+  - 日志 sink 分层（loguru）：总日志 sink / 5 个通道 sink（`filter=` 读
+    `record["extra"]["channel"]`）/ stderr sink；未绑定 channel 的记录只进总日志。
+- **接口约束**
+  - ITSCADA 消息字段集合 SHALL 以客户两份对接方案的字段表为准，SHALL NOT 自创字段；
+    指标消息 7 字段、告警消息 13 字段。
+  - ITSCADA 按 `metricName` + `resourceName` 匹配 → **指标名与资源名必须稳定**；
+    表级指标的 `metricName` 变为含 `schema.table` 前缀（**对外契约变化**，
+    交付说明必须写清台账录入方式）。
+  - 告警 `metricName`/`metricCode` 统一 → ITSCADA 侧告警台账从 254 项变 1 项
+    （**对外契约变化**，统一值登记进 `open_items.json` 待客户确认）。
+  - 幂等键定义不变（`instance_id + metric_name + 排序后的 dimensions + timestamp`），
+    只随维度值变化而变化。
+- **依赖约束**
+  - **Python 3.7.9 语法红线**：禁海象 `:=`、`f"{x=}"`、字典合并 `d1 | d2`、
+    内建泛型注解 `list[str]`、`typing.Literal/Protocol/TypedDict/Final`、
+    `functools.cached_property`、`importlib.metadata`、`math.prod`、`zoneinfo`、
+    `asyncio.to_thread`、`str.removeprefix/removesuffix`。
+  - 依赖锁：`kafka-python==2.0.2`、`requests>=2.25,<3.32`、`urllib3>=1.21.1,<3`、
+    **`loguru==0.8.0`**（新增；纯 Python `py3-none-any`；Linux + py3.7 零传递依赖）。
+  - 运行期依赖 SHALL 全部为**纯 Python**（`*-none-any`）→ x86_64 与 aarch64
+    共用同一份部署包；SHALL NOT 需要编译或联网。
+  - 目标机：麒麟 V10 离线，Python 3.7~3.11（**3.12+ 不行**）。
+- **数据约束**
+  - 客户 xlsx 原件 SHALL NOT 被修改；删除只落在排除清单数据文件。
+  - 生效口径 = 101 − 11 − 3 = **87**；排除清单 14 条（11 + 3）。
+  - `dimension_bindings` 的值 SHALL 支持「点号连接的多个列名」；
+    `group_by` SHALL 支持多列分组。
+  - 表级 `{#NAME}` = `schemaname` 与 `relname` 用 `.` 连接。
+  - 日志：文件名 `<基名>-<YYYY-MM-DD>.log`；同日超限改名
+    `<基名>-<YYYY-MM-DD>.<日期>_<时分秒_微秒>.log`；保留最近 3 个自然日（含当天）。
+  - 日志 SHALL NOT 出现 TPOPS 口令、mini-token、业务 SQL 文本全文。
+  - 汇总行 SHALL 写成纯 `k=v` token（值不含空格），否则 JSON 摊平失效。
+- **TPOPS 接口红线**（本次不得违反）：两套命名空间
+  （`/rds/v3/…` 与 `/rds/gaussdb/v3/…`）；mini-token 在响应头 `X-Mini-Token`，
+  认证失败**不重试**（防锁号）；`verify_tls=False`；指标窗口 ≥ 1h；
+  `end_time` 回退 5 分钟；`is_latest_point_only=false`；12 个分组码硬编码，
+  空分组靠告警发现；告警 `limit ≤ 100` 需分页、历史仅存 30 天需显式告警；
+  命令中心用 `statistics.running == 0` 判结束、`result` 二次 `json.loads`、
+  只在 master 执行、GaussDB 方言无 `FILTER` 且 `state is not null`、
+  `sql_text` 被掩码。
+
+## Execution Plan
+
+由 `ssf execution recommend` 列出可用方式并推荐；`ssf execution plan --confirm`
+在 **DP-4** 由用户确认后落盘。本合同只定义**波次边界与验收**，不预设执行模式。
+
+## Execution Waves
+
+### Wave 1 · 映射口径（生效 87 条 + 两处字段修正）
+
+- **Wave ID**：`wave-1-mapping`
+- **任务**：1.1 排除清单数据文件 → 1.2 生成器按清单剔除 → 1.3 映射层加载与校验 →
+  1.4 点号组合维度绑定 → 1.5 生成器改绑 `schemaname.relname` → 1.6 表级幂等不碰撞 →
+  1.7 告警统一指标名（含放宽告警 `metric_code` 唯一性校验） → 1.8 交付文档重生成
+- **依赖 wave**：无
+- **策略**：`serial`（1.1→1.2→1.3 严格串行；1.4→1.5→1.6 严格串行；
+  1.7、1.8 与前者无代码依赖，可并行）
+- **目标**：生效映射表 87 条、零重复；表级维度 = `schema.table`；告警统一名落地
+- **输入**：客户指标库 xlsx（只读）、`data/metric_response/`、`data/merged_sql/`
+- **输出**：`包内/excluded_metrics.json`（新）、重生成后的 `metric_rules.json` /
+  `customer_metric_mapping.json` / `alarm_rules.json`、
+  `rules_context.py`（组合绑定）、`loader.py` + `validation.py`（清单校验）、
+  `tools/build_itscada_mapping.py`、重生成的三份交付文档
+- **完成标准**：
+  - `pytest tests/unit/test_excluded_metrics.py tests/unit/test_dimension_binding.py tests/unit/test_alarm_metric_unification.py tests/unit/test_metric_catalog.py -q` 全绿；
+  - 生效规则数 = 87、`enabled=false` 数 = 0、`metric_code` 无重复；
+  - 排除清单 14 条（11/3），包内与仓库根两份一致；
+  - `public.t1` 与 `other.t1` 的 `metricCode` 与幂等键不同。
+- **Review gate**：review report 路径 `changes/tpops-mapping-and-logging/reviews/wave-1.md`，
+  base/head SHA 由执行时记录，receipt 必须为 `pass`
+
+### Wave 2 · 日志框架（loguru）
+
+- **Wave ID**：`wave-2-logging`
+- **任务**：2.1 loguru 依赖落地 → 2.2 `logging_setup.py` 重写为 loguru sink 装配 →
+  2.3 全面改用 loguru API → 2.4 配置项与快速失败 → 2.5 通道级每轮统计 →
+  2.6 通道级汇总行输出 → 2.7 敏感信息不进落盘日志
+- **依赖 wave**：`wave-1-mapping`（仅排序先后，无代码依赖）
+- **策略**：`serial`（2.1→2.2→2.3→2.4 严格串行；2.5→2.6 串行；2.7 依赖前两者）
+- **目标**：日志落盘 + 按天切分 + 保留 3 天 + 通道分流 + 每通道每轮汇总；
+  全项目走 loguru
+- **输入**：`logging_setup.py`、`config.py`、各采集器模块、`scheduler_report.py`、
+  `scheduler_types.py`、`requirements-runtime.txt`、`tools/build_deploy_bundle.py`
+- **输出**：`logging_setup.py`（重写）、`channel_stats.py`（新）、
+  `scheduler_types.py`（`ChannelRoundStats`）、`scheduler_report.py`、
+  `config.py`、约 30 个模块的 logger 获取方式、`requirements-runtime.txt`、
+  离线部署包 wheel 清单、`CLAUDE.md` 约定更新
+- **完成标准**：
+  - `pytest tests/unit/test_logging_files.py tests/unit/test_loguru_migration.py tests/unit/test_loguru_dependency.py tests/unit/test_channel_round_stats.py tests/integration/test_logs_have_no_secrets.py -q` 全绿；
+  - 生产包内不再出现 `logging.getLogger`（`InterceptHandler` 除外）；
+  - `<LOG_DIR>` 下 6 基名 × 日期后缀文件；跨天新文件；超 3 天文件被删；
+  - 通道文件只含本通道记录；通道汇总行 11 个字段齐全。
+- **Review gate**：review report 路径 `changes/tpops-mapping-and-logging/reviews/wave-2.md`，
+  receipt 必须为 `pass`
+
+### Wave 3 · 部署与文档
+
+- **Wave ID**：`wave-3-deploy`
+- **任务**：3.1 `collector.sh` 后台化（拆 `deploy/collector_runtime.sh`）→
+  3.2 systemd 与部署说明同步 → 3.3 日志框架规范成文（`docs/日志框架规范.md`）
+- **依赖 wave**：`wave-2-logging`
+- **策略**：`serial`
+- **目标**：`run` 后台化 + `status`/`stop`/`--foreground`；文档与脚本行为一致
+- **输入**：`deploy/collector.sh`、`docs/交付-部署运行说明.md`、`deploy/README.md`、
+  `design.md` 决策段
+- **输出**：`deploy/collector_runtime.sh`（新）、`deploy/collector.sh`、
+  `docs/交付-部署运行说明.md`、`deploy/README.md`、`docs/日志框架规范.md`（新）
+- **完成标准**：
+  - `bash tests/scripts/test_collector_runtime.sh` 全绿；
+  - `pytest tests/unit/test_delivery_docs.py tests/unit/test_logging_doc.py -q` 全绿；
+  - 交付说明里的 `ExecStart` 含 `run --foreground`；环境变量表含
+    `LOG_DIR`/`LOG_FORMAT`/`LOG_MAX_BYTES`/`LOG_RETENTION_DAYS`；
+  - `collector.sh` 与 `collector_runtime.sh` 均 < 400 行。
+- **Review gate**：review report 路径 `changes/tpops-mapping-and-logging/reviews/wave-3.md`，
+  receipt 必须为 `pass`
+
+### Wave 4 · 全量回归与门禁（收口）
+
+- **Wave ID**：`wave-4-verification`
+- **任务**：3.4 全量回归与门禁（四条命令）
+- **依赖 wave**：`wave-1-mapping`、`wave-2-logging`、`wave-3-deploy`
+- **策略**：`serial`
+- **目标**：全绿 + 覆盖率达标 + py37 兼容 + ruff 干净
+- **完成标准**：四条命令全部通过（pytest / pytest --cov / pytest tests/test_py37_compat.py / ruff）
+- **Review gate**：review report 路径 `changes/tpops-mapping-and-logging/reviews/wave-4.md`，
+  receipt 必须为 `pass`
+
+## Test Obligations
+
+- **必须先从失败测试开始的行为（RED 先行）**：
+  - 排除清单的加载与互斥校验（清单里的码一个都不在生效映射表中）；
+  - 点号组合维度绑定（`schemaname.relname` → `public.t1`）；
+  - 同名表不同 schema 的幂等键不碰撞；
+  - 告警统一指标名（254 条同码、3 条挂靠的不变）；
+  - 生效规则数 = 87 / `enabled=false` = 0 / `metric_code` 唯一；
+  - loguru sink 装配：日期文件名、跨天切分、同日超限改名、保留 3 天清理、
+    通道 filter 分流、总日志含各通道简洁汇总；
+  - `collector.sh run` 后台启动、重复启动拒绝、`status`、`stop`、`--foreground`。
+- **必需的边界情况**：
+  - 排除清单里的指标码不出现在生效映射表；`kept` 缺失（`unsupported`）不算错；
+  - 组合绑定取不到列值 → 抛 `RuleError`，SHALL NOT 静默留 `{#NAME}`；
+  - `mapped == 0` 时 `delivery_rate` 为 1.000；
+  - 未到期通道不输出汇总行；
+  - `LOG_DIR` 未设置 → 只写 stderr；`LOG_DIR` 不可写 → 启动失败；
+  - 非法 `LOG_MAX_BYTES` / `LOG_RETENTION_DAYS` → 启动失败退出码 2；
+  - PID 文件存在且进程活着 → `run` 拒绝启动；
+  - 启动后进程立刻退出 → `run` 返回非零并打印日志尾部；
+  - 跨天 → 新日期文件出现；旧日期超过 3 天 → 被删除；同日分片同样被清理。
+- **回归敏感区域**：
+  - 既有 `setup_logging(level, log_format, stream)` 签名与幂等语义（现有测试依赖）；
+  - 既有 json 格式的 `exception` 字段与 `k=v` 摊平；
+  - `TickResult` 既有字段名（`fetched`/`mapped`/`produced`/`failed`/`windows`/`calls`）；
+  - 幂等键定义与「同表重复采集仍去重」；
+  - `dropped_unsupported` 字段仍在轮汇总里（值为 0）；
+  - 交付文档与生效映射表条数一致（87 / 3）；
+  - py37 语法兼容门禁（loguru 是外部包，不得引入 3.8+ 语法进生产代码）。
+
+## Execution Mode
+
+- **可用方式与推荐**：`ssf execution recommend changes/tpops-mapping-and-logging --wave wave-1-mapping:serial:1.1,1.2,1.3 --wave wave-2-logging:serial:2.1,2.2,2.3,2.4 --wave wave-3-deploy:serial:3.1,3.2,3.3 --wave wave-4-verification:serial:3.4`
+- **用户确认的模式**：由用户在 DP-4 确认（`sdd` / `inline` / `batch-inline`）
+- **执行计划命令**：`ssf execution plan changes/tpops-mapping-and-logging --mode <mode> --confirm --reason "<用户确认理由>" --wave ...`
+- **允许的修订**：保留 / 升级为 `sdd`；**不允许降级**。
+
+## Verification Dimensions
+
+| 维度 | 状态 | 检查点 |
+|---|---|---|
+| Completeness | Pending | 19 条需求全部映射到 wave 且有测试义务（见下方映射表） |
+| Correctness | Pending | 8 条验收检查全部机械可判定；四条门禁命令全绿 |
+| Coherence | Pending | proposal/specs/design/tasks 的数量口径（101/87/14/11/3）与字段名一致 |
+| Scope | Pending | 无新增未声明的依赖与行为；未触碰范围外项 |
+
+**需求 → 波次映射（19/19 已映射，无遗漏）**
+
+| 需求 | 波次 | 任务 |
+|---|---|---|
+| R1 排除清单 | wave-1 | 1.1, 1.2, 1.3 |
+| R2 生效 87 条 | wave-1 | 1.2, 1.3 |
+| R3 告警统一指标名 | wave-1 | 1.7 |
+| R4 维度展开（schema.table） | wave-1 | 1.4, 1.5 |
+| R5 不可提供指标处置 | wave-1 | 1.3, 1.8 |
+| R6 映射表驱动 | wave-1 | 1.1, 1.2 |
+| R7 指标幂等键 | wave-1 | 1.6 |
+| R18 幂等去重 | wave-1 | 1.6 |
+| R19 业务维度的落点 | wave-1 | 1.4, 1.5 |
+| R8 日志目录与文件布局 | wave-2 | 2.2 |
+| R9 总日志与通道日志分工 | wave-2 | 2.2, 2.6 |
+| R10 每通道每轮汇总行 | wave-2 | 2.5, 2.6 |
+| R11 日志格式 | wave-2 | 2.2 |
+| R12 日志切分与保留 | wave-2 | 2.2 |
+| R15 日志框架为 loguru | wave-2 | 2.1, 2.2, 2.3 |
+| R16 每轮结构化汇总日志 | wave-2 | 2.5, 2.6 |
+| R14 日志不落敏感信息 | wave-2 | 2.7 |
+| R17 凭据不入日志 | wave-2 | 2.7 |
+| R13 采集端后台运行 | wave-3 | 3.1, 3.2, 3.3 |
+
+**跨波次依赖**：wave-1 与 wave-2 无代码依赖（仅排序）；wave-3 依赖 wave-2 的
+日志目录与 sink 语义；wave-4 依赖全部。
+
+## Review Gates
+
+- **强制审查点**：每个 wave 完成后记录 `ssf execution review` 的 receipt
+  （`--wave <id> --base <sha> --head <sha> --report <path> --verdict pass|fail`）。
+- **阻塞类别**：依赖未通过、review receipt 为 `fail`、缺失或过期。
+- **收口条件**：4 个 wave 全部有 `pass` receipt。
+- **审查重点**：wave-2 额外查「生产包内是否残留 `logging.getLogger`」
+  「loguru 是否为纯 Python 且 py3.7 可装」「日志文件是否泄露凭据」。
+
+## Escalation Rules
+
+- **何时回退到 `specifying`**：
+  - 需求级口径需要再改（生效口径条数、告警统一名取值、表级命名形式、
+    日志保留天数/格式）；
+  - 发现「14 条英文中文名的独立指标」其实也该删/该改名；
+  - 对外契约变化（表级 `metricName` 前缀、告警统一指标）被客户否决。
+- **何时回退到 `bridging`**：
+  - loguru 在目标机装不上、或与 py3.7 / 离线部署包冲突（依赖红线被破）；
+  - `collector.sh` 后台化与 systemd 的关系需要重新定（必须同时保留
+    `run --foreground`）。
+- **何时不得继续实现**（任一命中即停）：
+  - 生效规则数 ≠ 87 或排除清单 ≠ 14；
+  - 表级 `{#NAME}` 仍只绑 `relname`（同名表仍会碰撞）；
+  - 告警 `metricCode` 未统一（254 条仍是 `gaussdb.alarm.<id>`）；
+  - 引入 3.8+ 语法或非纯 Python 依赖（py37 红线 / 部署包红线）；
+  - 日志文件里出现口令、mini-token 或 `sql_text` 全文；
+  - 生产包内仍有模块用 stdlib `logging.getLogger`（除 `InterceptHandler` 外）；
+  - 覆盖率低于 全包 80% / 映射层 90%，或 py37 兼容门禁不通过；
+  - 任一 wave 的 review receipt 为 `fail`。
diff --git a/changes/tpops-mapping-and-logging/proposal.md b/changes/tpops-mapping-and-logging/proposal.md
new file mode 100644
index 0000000..2c2b48c
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/proposal.md
@@ -0,0 +1,125 @@
+# 变更提案：指标清理、日志框架、字段映射修正
+
+## 背景（Why）
+
+在 `tpops-kafka-collector` 变更交付（2026-11-24）之后复核客户指标库与运行期行为，
+发现三个**已观察到**的问题：
+
+1. **表级指标重复投递**。客户指标库「指标库」sheet 共 101 行，其中 **11 个表级指标
+   各出现两次**：一次在「表信息」分类（中文名正常），一次在「事务信息」分类
+   （中文名是英文），两条英文名仅大小写前缀不同（`gaussdb.…` vs `GAUSSDB.…`）。
+   两行都被 `metric_rules.json` 收录且 `enabled=true` → 运行期对**同一张表产出两条
+   不同指标消息**，ITSCADA 侧看到重复指标。
+2. **日志只到 stderr，且只有全局一轮汇总**。`logging_setup.py` 只装了一个
+   `StreamHandler(sys.stderr)`，`collector.sh run` 以前台启动，**日志不落盘**；
+   `scheduler_report.py` 每轮只给一行 tick 整体口径，**看不到通道内部**——
+   哪类采集器拉了多少、推了多少、耗时多久，只能靠猜。目标机是麒麟 V10 离线环境，
+   无 logrotate。
+3. **两个字段映射缺陷**。告警侧：257 条平台告警里 254 条事件类告警的 `metricCode`
+   是自造的 `gaussdb.alarm.<alarm_id>`，同一条规则复制了 254 份，而告警内容
+   并不涉及客户指标名。表级指标侧：`{#NAME}` 绑到 `relname`、`group_by` 也是
+   `["relname"]` → **不同 schema 的同名表**得到**完全相同**的 `metricName` /
+   `metricCode` / 幂等键，后一条被幂等去重**静默丢弃**。
+
+**为什么现在**：交付死线 **2026-12-15**。重复指标与表级碰撞会**直接进客户 ITSCADA
+台账**（按 `metricName` + `resourceName` 匹配），交付后再改要客户重录台账；
+日志须在 P5 联调**前**落盘，否则联调时无法核对「哪类采集器推了多少、漏了多少」。
+
+## 变更内容（What Changes）
+
+- **指标更新**：交付口径从 101 条收敛到 **87 条**——
+  删除 11 条重复的表级指标「英文变体」（保留「表信息」分类的中文名版本），
+  删除 3 条无法提供的指标（`gaussdb.Fastpath_function_call`、`gaussdb.Disabled_conn`、
+  `gaussdb.sendAgentName`）。删除清单与理由落进**新的映射数据文件**
+  `tpops_kafka_collector/mapping/data/excluded_metrics.json`（表驱动，不写死在代码里），
+  仓库根 `data/mapping/` 放同源副本，客户指标库 xlsx 原件保持不改。
+  交付文档（指标口径说明 / 不可提供指标清单）随之重生成。
+  （注意区分两个「14」：**排除清单共 14 条** = 11 重复 + 3 无法提供；
+  另有 **14 条独立指标的中文名整体是英文**（`GAUSSDB.Buffers_Alloc` 等）——它们**不重复**，
+  本次**保留**。）
+- **日志框架**：产出**日志框架规范与格式**（本文档 + `design.md` 的决策段），
+  并按该规范实现——
+  - 每个采集器通道（`monitor` / `command_center` / `diagnostic` / `instance` / `alarm`）
+    在**每轮结束时**输出一条**该通道自己的**汇总行：获取数据量、已推送数据量、
+    耗时、失败数、窗口数、外部调用数；
+  - 日志**落盘**：`collector.sh run` 改为**后台运行**（写 PID 文件），
+    日志目录可配置，**总日志**（生命周期 + 每轮整体汇总 + 错误）**总览各采集器
+    的采集推送情况**，**各采集器另有自己的日志文件**（明细 + 该通道汇总）；
+  - 日志**文件名带日期后缀**（`collector-2026-11-26.log`、
+    `collector-monitor-2026-11-26.log`），**按天切分**，
+    **过期 3 天的文件自动删除**；
+  - 日志框架换成 **loguru 0.8.0**（纯 Python、Python 3.7.9 可用、
+    Linux+py3.7 下零传递依赖），**直接用 loguru 原生格式**，
+    不自研 handler / 轮转 / 保留 / 格式化；项目内全面改用 loguru API；
+  - 保留 `collector.sh run --foreground` 前台模式（systemd 单元用这个）。
+- **字段映射**：
+  - 告警：**不涉及客户指标名**的事件类告警，`metricName` 与 `metricCode`
+    **统一为一个固定名**（拟 `gaussdb.alarm` / `数据库告警`，标记为待客户确认）；
+    告警种类靠 `alarmId` / `alarmName` / `alarmContent` 区分。
+  - 表级指标：`{#NAME}` 改由 **`schemaname.relname`** 填充
+    （即 `schema_name.table_name`），分组改为按 `schemaname + relname`，
+    不同 schema 的同名表得到不同的 `metricName` / `metricCode` / 幂等键。
+
+## 范围（Scope）
+
+### 范围内（In Scope）
+
+- 指标清理：14 条（11 重复 + 3 无法提供）的剔除与理由留档；映射数据重生成；
+  交付文档重生成；运行期「不可提供指标丢弃计数」归零。
+- 日志框架：日志目录/文件布局、格式（text + json）、轮转策略、总日志与分日志的分工；
+  每通道每轮汇总行；`collector.sh run` 后台化 + PID 文件 + `status` / `stop`；
+  `collector.sh run --foreground` 前台模式；日志相关环境变量。
+- 字段映射：告警统一指标名；表级 `schema_name.table_name` 替换；
+  幂等键随维度修正而不再跨 schema 碰撞。
+- 相应的单元测试、集成测试与文档同步（`docs/交付-*.md`、
+  `docs/映射表-ITSCADA字段对照.md`、`deploy/README.md`）。
+
+### 范围外（Out of Scope）
+
+- 客户 Kafka 地址 / Topic 的 P5 联调（等客户提供，仍在原变更之外）。
+- 客户指标库 xlsx 原件的改动（客户文件，删除只落在本项目的生效口径与排除清单）。
+- 14 条「中文名是英文」的**独立**指标（如 `GAUSSDB.Buffers_Alloc`、
+  `GAUSSDB.Idle_connections`）——经确认本次**保留**（用户明确选择「只删 11 组重复 + 3」）。
+- 其余 8 项 `open_items.json` 待客户确认项的结论。
+- 日志采集与外发（shipper / Fluent Bit 等）、日志平台接入 —— 本次只做**本地落盘**。
+- systemd 单元文件的正式交付（仍由客户按 `docs/交付-部署运行说明.md` 的参考模板自建，
+  本次只把 `ExecStart` 改成 `run --foreground` 并更新说明）。
+
+## 影响与验证
+
+- **影响区域**
+  - 代码：`tpops_kafka_collector/logging_setup.py`（重写为 loguru sink 装配）、
+    **约 30 个模块的 logger 获取方式**（改 loguru API）、
+    新增 `tpops_kafka_collector/channel_stats.py`（通道级统计）、
+    `tpops_kafka_collector/scheduler_report.py`、`tpops_kafka_collector/scheduler.py`、
+    `tpops_kafka_collector/scheduler_types.py`、`tpops_kafka_collector/scheduler_delivery.py`、
+    `tpops_kafka_collector/config.py`、`tpops_kafka_collector/mapping/rules_context.py`、
+    `tpops_kafka_collector/mapping/message.py`、`tpops_kafka_collector/mapping/table.py`、
+    `tpops_kafka_collector/mapping/loader.py`；
+  - 映射数据：`tpops_kafka_collector/mapping/data/metric_rules.json`、
+    `tpops_kafka_collector/mapping/data/customer_metric_mapping.json`、
+    `tpops_kafka_collector/mapping/data/alarm_rules.json`、
+    **新增** `tpops_kafka_collector/mapping/data/excluded_metrics.json`，
+    以及仓库根 `data/mapping/` 的同源副本（运行期只读包内那份）；
+  - 部署：`deploy/collector.sh`（`run` 后台化、新增 `status` / `stop`）、
+    `.env.example`（新增日志目录/轮转变量）；
+  - 离线工具：`tools/build_itscada_mapping.py`（排除清单）、
+    `tools/build_delivery_docs.py`（交付文档重生成）；
+  - 文档：`docs/交付-*.md`、`docs/映射表-ITSCADA字段对照.md`、`deploy/README.md`。
+- **完成证明**
+  - `conda run -n tpops python -m pytest -q` 全绿；
+    `conda run -n tpops python -m pytest --cov=tpops_kafka_collector --cov-report=term-missing -q`
+    覆盖率**全包 ≥ 80%**、**映射层 ≥ 90%**；
+  - `tpops_kafka_collector/mapping/data/metric_rules.json` 恰好 **87** 条规则、
+    **0** 条 `enabled=false`、`metric_code` 无重复；
+  - `tpops_kafka_collector/mapping/data/excluded_metrics.json` 恰好 **14** 条
+    （11 条 `duplicate` + 3 条 `unsupported`），每条带理由；重复项另带「保留哪一条」；
+  - 告警定义中，不挂客户指标的告警 `metricCode` 全部等于同一个固定值；
+  - 表级指标的 `tags.dimension_values.NAME` 形如 `public.t1`，
+    `public.t1` 与 `other.t1` 的 `metricCode` 与幂等键**不相同**；
+  - `bash tests/scripts/test_collector_runtime.sh` 全绿（覆盖后台启动、重复启动拒绝、
+    `status`、`stop`、前台模式）；
+  - 在**已安装形态**的目标目录里执行 `bash collector.sh run` 后返回 0，
+    PID 文件存在、进程仍在后台运行，`<LOG_DIR>` 下出现 1 个总日志 + 5 个通道日志文件，
+    总日志里能看到各通道的推送情况；每个通道日志里能看到该通道每轮的
+    `fetched/mapped/produced/failed/elapsed` 汇总行。
diff --git a/changes/tpops-mapping-and-logging/specs/delivery-and-state/spec.md b/changes/tpops-mapping-and-logging/specs/delivery-and-state/spec.md
new file mode 100644
index 0000000..6e47943
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/specs/delivery-and-state/spec.md
@@ -0,0 +1,61 @@
+# 能力规格（增量）：投递与状态
+
+本增量只覆盖一件事：表级指标维度改为 `schema_name.table_name` 之后，
+幂等键与去重口径必须随之修正——否则不同 schema 的同名表会被去重**静默丢弃**。
+
+## MODIFIED Requirements
+
+### Requirement: 幂等去重
+
+采集端 SHALL 在内存中维护最近 N 条消息的幂等键集合（LRU，默认 10000 条），
+防止同一轮内的重复投递与相邻轮的重投。
+
+幂等键 SHALL 由**维度值**参与拼接；表级指标的维度值 SHALL 是
+`schema_name.table_name` 组合形式，
+SHALL NOT 只用表名 —— 否则不同 schema 的同名表会得到相同的键而被去重丢弃。
+
+#### Scenario: 同一轮重复
+
+- **WHEN** 同一批内两条消息的幂等键相同
+- **THEN** 只投递一条
+
+#### Scenario: 相邻轮重投
+
+- **WHEN** 上一轮已投递的幂等键在本轮再次出现
+- **THEN** 该消息被跳过，不重复投递
+
+#### Scenario: 不同 schema 的同名表
+
+- **WHEN** 同一实例上存在 `public.t1` 与 `other.t1` 两个同名表，
+  两者的表级指标在同一轮被采集到
+- **THEN** 两条消息的幂等键**不相同**，两条都被投递
+
+#### Scenario: 同一张表重复采集
+
+- **WHEN** 同一张 `public.t1` 的同一指标在相邻两轮被采集到
+- **THEN** 幂等键相同，后一条被跳过（原有去重语义不变）
+
+### Requirement: 业务维度的落点
+
+表名、库名、端口这类业务维度 SHALL NOT 进入 `resourceName`；
+SHALL 放在 `tags.dimension_values` 里（键为占位符名，值为该行的实际值）。
+`tags` 的其余键 SHALL 为 `instance_id`、`instance_name`、`node_id`、
+`component_id`、`host_ip`。
+
+表级指标的 `NAME` 维度值 SHALL 为 `schema_name.table_name` 组合形式。
+
+#### Scenario: 表级指标
+
+- **WHEN** 某指标含占位符 `{#NAME}`，其来源行的 `schemaname` 为 `public`、
+  `relname` 为 `t1`
+- **THEN** `tags.dimension_values` 为 `{"NAME": "public.t1"}`，`resourceName` 中不含 `t1`
+
+#### Scenario: A 类多组件
+
+- **WHEN** 某指标来自监控指标接口的多个组件 series
+- **THEN** 每个组件一条消息，`tags.component_id` 为该组件的键（**小写下划线**）
+
+#### Scenario: 无业务维度
+
+- **WHEN** 某指标不含占位符
+- **THEN** `tags.dimension_values` 为空对象
diff --git a/changes/tpops-mapping-and-logging/specs/mapping-and-rules/spec.md b/changes/tpops-mapping-and-logging/specs/mapping-and-rules/spec.md
new file mode 100644
index 0000000..296a185
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/specs/mapping-and-rules/spec.md
@@ -0,0 +1,189 @@
+# 能力规格（增量）：映射层与取值规则引擎
+
+本增量覆盖三条工作流里与映射口径相关的部分：
+指标清理（生效口径 101 → 87）、告警统一指标名、表级指标的 `schema_name.table_name` 替换。
+
+## ADDED Requirements
+
+### Requirement: 排除清单
+
+映射层 SHALL 用**独立的数据文件**
+**`tpops_kafka_collector/mapping/data/excluded_metrics.json`**（运行期真相源；
+仓库根 `data/mapping/excluded_metrics.json` 为同源副本）
+记录被排除的客户指标，每条至少含：客户指标码、客户指标中文名、
+排除原因（`duplicate` 重复 / `unsupported` 无法提供）、理由说明；
+`reason` 为 `duplicate` 时**另需**给出「保留哪一条」。
+
+排除 SHALL 发生在**构建期**（映射表生成器），因此运行期加载到的生效映射表里
+**根本不存在**被排除的指标码 —— 运行期 SHALL NOT 存在「遍历到被排除指标再丢弃」的动作，
+SHALL NOT 有对应的运行期丢弃日志。
+
+采集端 SHALL NOT 在代码里硬编码任何被排除的指标码；
+SHALL NOT 把被排除的指标产出为消息。
+
+#### Scenario: 重复指标被排除
+
+- **WHEN** 排除清单中某条 `reason` 为 `duplicate`
+- **THEN** 该指标不产出消息，且生效映射表中只存在清单指明的「保留项」
+
+#### Scenario: 无法提供的指标被排除
+
+- **WHEN** 排除清单中某条 `reason` 为 `unsupported`
+- **THEN** 该指标不产出消息，且其理由随交付文档一并给出
+
+#### Scenario: 清单与生效映射表一致
+
+- **WHEN** 启动期校验比对排除清单与生效映射表
+- **THEN** 排除清单里的指标码**一个都不在**生效映射表中，校验通过
+
+### Requirement: 生效指标口径为 87 条
+
+生效映射表 `tpops_kafka_collector/mapping/data/metric_rules.json` SHALL 恰好包含 **87** 条规则，
+每条 `enabled` 为 `true`，`metric_code` **互不重复**；
+`tpops_kafka_collector/mapping/data/customer_metric_mapping.json` SHALL 与之一一对应（同样 87 条）。
+
+采集端 SHALL NOT 存在「同一指标产出多条消息」的规则对
+（同一 `resource_granularity` + 同一占位符集合 + 同一来源列的重复规则）。
+
+#### Scenario: 11 条重复表级指标
+
+- **WHEN** 客户指标库中同一表级指标有两条（中文名正常的一条 + 中文名是英文的一条）
+- **THEN** 生效映射表只保留中文名正常的一条，英文那条进排除清单
+
+#### Scenario: 3 条无法提供的指标
+
+- **WHEN** 生效映射表被加载
+- **THEN** `gaussdb.Fastpath_function_call`、`gaussdb.Disabled_conn`、
+  `gaussdb.sendAgentName` 均不在其中
+
+#### Scenario: 规则码唯一
+
+- **WHEN** 启动期校验统计生效规则的 `metric_code`
+- **THEN** 87 条规则对应 87 个互不相同的 `metric_code`
+
+#### Scenario: 不可提供指标的丢弃计数
+
+- **WHEN** 某轮采集正常结束
+- **THEN** 轮汇总日志中的 `dropped_unsupported` 为 **0**
+  （排除已在生效口径之外，运行期不再有「遍历到再丢弃」的动作）
+
+### Requirement: 告警的统一指标名
+
+平台告警定义中，**不挂靠客户指标**的事件类告警（`linked_customer_metric` 为空），
+其 `metricCode` SHALL 全部等于**同一个固定值**，`metricName` SHALL 全部等于
+**同一个固定中文名**；该固定值 SHALL 由映射数据给出（表驱动），SHALL NOT 写死在代码里。
+
+挂靠了客户指标的告警 SHALL 保留该客户指标的 `metricCode` 与 `metricName`。
+
+告警种类 SHALL 由 `alarmId` / `alarmName` / `alarmContent` 承载，
+SHALL NOT 依赖 `metricCode` 区分。
+
+#### Scenario: 事件类告警
+
+- **WHEN** 一条告警的定义 `linked_customer_metric` 为空
+- **THEN** 其 `metricCode` 等于统一固定值、`metricName` 等于统一固定中文名
+
+#### Scenario: 挂靠客户指标的告警
+
+- **WHEN** 一条告警的定义 `linked_customer_metric` 非空
+- **THEN** 其 `metricCode` / `metricName` 仍为该客户指标的值，不被统一值覆盖
+
+#### Scenario: 统一值可配置
+
+- **WHEN** 映射数据里改写统一固定值
+- **THEN** 产出的告警消息随之改变，采集端代码不变
+
+## MODIFIED Requirements
+
+### Requirement: 维度展开
+
+含占位符的指标 SHALL 按维度展开为一行一条消息。
+占位符共 **5 种**：`{#NAME}`、`{#DBNAME}`、`{#DATABASE_NAME}`、`{#DATNAME}`、`{#PORT}`。
+占位符在 `metricName` / `metricCode` 中原样保留，维度值置于 `tags.dimension_values`；
+无占位符时 `tags.dimension_values` 为空对象。
+
+**表级指标的 `{#NAME}` SHALL 由 `schemaname` 与 `relname` 组合填充**，
+组合形式为 `schema_name.table_name`（中间为 `.`）；
+表级指标的分组 SHALL 同时按 `schemaname` 与 `relname` 进行，
+SHALL NOT 只按 `relname` 分组。
+
+#### Scenario: 表级指标展开
+
+- **WHEN** 某表级指标对应的 SQL 返回 77 行且每行有 `schemaname` 与 `relname`
+- **THEN** 产出 77 条消息，每条的 `tags.dimension_values` 含 `NAME` = 该行的
+  `schemaname.relname`
+
+#### Scenario: 不同 schema 的同名表
+
+- **WHEN** 表级指标的来源行同时包含 `public.t1` 与 `other.t1`
+- **THEN** 产出**两条**消息，`NAME` 分别为 `public.t1` 与 `other.t1`，
+  且两条消息的 `metricName` 与 `metricCode` **不相同**
+
+#### Scenario: 库级指标展开
+
+- **WHEN** 某库级指标含占位符 `{#DATNAME}`，来源行的 `datname` 为 `postgres`
+- **THEN** 该消息的 `tags.dimension_values` 含 `DATNAME` = `postgres`
+
+#### Scenario: 无占位符指标
+
+- **WHEN** 某指标不含占位符
+- **THEN** 其 `tags.dimension_values` 为空对象
+
+### Requirement: 不可提供指标的处置
+
+采集端 SHALL 对被**排除清单**排除的客户指标不产出任何消息，
+并 SHALL 随交付附书面清单与理由。
+
+本变更加入的 3 条无法提供指标 SHALL 出现在书面清单中并标注「已从生效口径移除」；
+由于排除发生在构建期，运行期 SHALL NOT 有「遍历到再丢弃」的动作，
+轮汇总日志中的 `dropped_unsupported` SHALL 为 0。
+
+#### Scenario: 被排除的指标不在生效口径
+
+- **WHEN** 运行期加载生效映射表
+- **THEN** 排除清单里的指标码一个都不在其中，采集端没有对应的丢弃分支
+
+#### Scenario: 排除清单为空
+
+- **WHEN** 生效映射表内没有可被排除的指标
+- **THEN** 轮汇总日志中的 `dropped_unsupported` 为 0
+
+### Requirement: 映射表驱动
+
+采集端 SHALL 从 `tpops_kafka_collector/mapping/data/*.json` 加载全部客户口径（指标规则、字段映射、
+SQL 视图清单、告警定义目录、**排除清单**），
+SHALL NOT 在代码中硬编码任何客户指标名、列名、单位或维度键。
+
+#### Scenario: 客户口径变更
+
+- **WHEN** 只需修改某个指标的取值口径
+- **THEN** 改动只发生在映射表数据文件，采集端代码不变
+
+#### Scenario: 排除某个指标
+
+- **WHEN** 某个指标需要从生效口径中移除
+- **THEN** 改动只发生在排除清单数据文件，不改代码
+
+### Requirement: 指标幂等键
+
+指标消息的幂等键 SHALL 为 `instance_id + metric_name + 排序后的 dimensions + timestamp`。
+
+`metric_name` SHALL 是**占位符替换后**的指标名；
+表级指标的 `metric_name` 含 `schema_name.table_name`，
+`dimensions` 中的 `NAME` 亦为 `schema_name.table_name`。
+两条消息只要 `metric_name` 或 `dimensions` 任一不同，幂等键 SHALL 不同。
+
+#### Scenario: 同一指标不同维度
+
+- **WHEN** 同一指标在不同维度上各产出一个值
+- **THEN** 两者的幂等键不同（维度参与拼接）
+
+#### Scenario: 同名表不同 schema
+
+- **WHEN** `public.t1` 与 `other.t1` 上同一表级指标各产出一个值
+- **THEN** 两者的幂等键不同，两条消息都被投递
+
+#### Scenario: 同一张表同一时刻
+
+- **WHEN** 同一张表的同一指标在同一时刻被采集到两次
+- **THEN** 两者幂等键相同，只投递一条
diff --git a/changes/tpops-mapping-and-logging/specs/runtime-and-config/spec.md b/changes/tpops-mapping-and-logging/specs/runtime-and-config/spec.md
new file mode 100644
index 0000000..105f484
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/specs/runtime-and-config/spec.md
@@ -0,0 +1,310 @@
+# 能力规格（增量）：运行时、配置与日志框架
+
+本增量覆盖日志优化工作流：日志框架规范与格式、每通道每轮汇总、
+日志落盘与总日志/通道日志分工、`collector.sh run` 后台运行。
+
+## ADDED Requirements
+
+### Requirement: 日志目录与文件布局
+
+采集端 SHALL 把日志写入**可配置的日志目录**（环境变量 `LOG_DIR`），
+目录下的文件布局 SHALL 为 **6 个基名、每个基名按天一个文件、文件名带日期后缀**：
+
+```
+<LOG_DIR>/collector-2026-11-26.log                  # 总日志
+<LOG_DIR>/collector-monitor-2026-11-26.log          # A 类：监控指标通道
+<LOG_DIR>/collector-command_center-2026-11-26.log   # B 类：命令中心通道
+<LOG_DIR>/collector-diagnostic-2026-11-26.log       # C 类：诊断优化通道
+<LOG_DIR>/collector-instance-2026-11-26.log         # D 类：实例元数据通道
+<LOG_DIR>/collector-alarm-2026-11-26.log            # E 类：告警通道
+```
+
+命名规则 SHALL 为 `<基名>-<YYYY-MM-DD>.log`；同一天内超出单日上限时，
+旧文件 SHALL 被改名为 `<基名>-<YYYY-MM-DD>.<日期>_<时分秒_微秒>.log`，
+并新建同名文件继续写。
+
+日志目录不存在时 SHALL 自动创建；不可写时 SHALL **启动失败**并给出明确错误
+（SHALL NOT 静默降级为「只写 stderr」）。
+
+`LOG_DIR` **未设置**时，采集端 SHALL 只写 stderr（不落盘）——这是开发态的既有行为；
+`deploy/collector.sh run` SHALL **总是**显式设置 `LOG_DIR`（默认 `<安装目录>/logs`），
+因此生产态一定落盘。`<安装目录>` = **`collector.sh` 所在目录**（脚本里的 `$HERE`）。
+
+通道文件的**基名** SHALL 由通道名（`monitor` / `command_center` / `diagnostic` /
+`instance` / `alarm`）确定，SHALL NOT 由指标名或实例名派生
+（避免文件名数量随指标/实例膨胀）。
+
+#### Scenario: 首次启动
+
+- **WHEN** `LOG_DIR` 指向的目录不存在
+- **THEN** 自动创建该目录，随后正常写入日志文件
+
+#### Scenario: 目录不可写
+
+- **WHEN** `LOG_DIR` 存在但当前用户无写权限
+- **THEN** 进程启动失败，错误信息说明日志目录路径与不可写原因
+
+#### Scenario: 文件名带日期
+
+- **WHEN** 采集端在 2026-11-26 写日志
+- **THEN** 产生 `collector-2026-11-26.log` 与 5 个
+  `collector-<channel>-2026-11-26.log`
+
+#### Scenario: 基名稳定
+
+- **WHEN** 采集端在跑 87 个指标、多个实例
+- **THEN** 日志目录下的**基名**仍然只有 6 个（总日志 + 5 个通道）
+
+### Requirement: 总日志与通道日志的分工
+
+采集端 SHALL 维护两类日志：
+
+- **总日志**（`collector.log`）：生命周期（启动 / 停止 / 装配结果 / 致命错误）、
+  每个 tick 的**整体**汇总、以及**每个通道一行**的简洁汇总
+  （即总日志要能一眼看出各采集器这一轮的采集与推送情况）；
+- **通道日志**（`collector-<channel>.log`）：该通道内部的采集与推送明细，
+  以及该通道每轮结束时的**通道汇总行**。
+
+通道日志 SHALL NOT 把其它通道的明细写入；总日志 SHALL NOT 出现通道内部明细。
+
+logger 命名 SHALL 分层：`collector.lifecycle`（生命周期）、
+`collector.summary`（轮汇总与通道汇总）、`collector.<channel>`（通道明细）。
+通道 logger SHALL NOT 向总日志重复输出 INFO 级明细；
+其 WARNING 及以上级别 SHALL 同时出现在总日志与该通道日志。
+
+#### Scenario: 一轮正常结束
+
+- **WHEN** 一个 tick 内 3 个通道到期并完成采集
+- **THEN** 总日志里有 1 行整体汇总 + 3 行通道汇总；
+  每个通道日志里有 1 行该通道的汇总
+
+#### Scenario: 通道内部报错
+
+- **WHEN** 某通道采集阶段抛异常并被记为 ERROR
+- **THEN** 该 ERROR 同时出现在该通道日志与总日志中
+
+#### Scenario: 通道日志不串味
+
+- **WHEN** 通道 `monitor` 完成一轮
+- **THEN** `collector-command_center.log` 中**没有**本轮 monitor 的记录
+
+### Requirement: 每通道每轮汇总行
+
+每类采集器**完成一轮**后 SHALL 输出一条属于该通道的汇总行，
+至少包含下列字段（字段名固定，SHALL NOT 自创）：
+
+| 字段 | 含义 |
+|---|---|
+| `channel` | 通道名（`monitor` / `command_center` / `diagnostic` / `instance` / `alarm`） |
+| `tick` | 轮次序号 |
+| `instances` | 本轮该通道参与的实例数 |
+| `fetched` | 本轮该通道**获取的数据量**（从平台取回的原始行数 / 告警条数） |
+| `mapped` | 转换后尝试投递的消息数（已滤掉幂等重复） |
+| `produced` | **已推送数据量**（成功投递的消息数） |
+| `failed` | 投递失败的消息数 |
+| `windows` | 本轮该通道执行的 SQL 条数 |
+| `calls` | 本轮该通道的外部调用次数 |
+| `elapsed` | 本轮该通道的耗时（秒） |
+| `delivery_rate` | 投递成功率（`produced / mapped`，无消息时为 1.000） |
+
+未到期的通道 SHALL NOT 输出汇总行。
+
+#### Scenario: 通道到期
+
+- **WHEN** 通道 `command_center` 在 tick 12 到期并完成
+- **THEN** 输出一行含上述全部字段、且 `channel=command_center` 的汇总行
+
+#### Scenario: 通道未到期
+
+- **WHEN** 通道 `diagnostic` 本轮未到期
+- **THEN** 本轮**没有** `channel=diagnostic` 的汇总行
+
+#### Scenario: 计数自洽
+
+- **WHEN** 某通道本轮 `produced` 为 10、`failed` 为 2
+- **THEN** `mapped` ≥ 12，且 `delivery_rate` = 10 / mapped
+
+### Requirement: 日志框架为 loguru
+
+采集端 SHALL 使用 **loguru** 作为日志框架（版本锁定 `loguru==0.8.0`），
+SHALL NOT 自研日志 handler / 轮转 / 保留 / 格式化实现。
+
+采集端 SHALL 全面使用 loguru 的 API（`from loguru import logger`）；
+第三方库（`kafka` / `urllib3` / `requests`）的 stdlib 日志
+SHALL 经 `InterceptHandler` 转发进 loguru。
+
+loguru SHALL 满足本项目的部署红线：`requires_python >= 3.5`（Python 3.7.9 可用）、
+发行包为纯 Python（`py3-none-any`，x86_64 与 aarch64 共用同一份部署包）、
+在 Linux + Python 3.7 下**无运行期传递依赖**。
+
+#### Scenario: 依赖可用
+
+- **WHEN** 目标机只有 requests / kafka-python / urllib3 与 loguru 四个纯 Python 包
+- **THEN** 采集端正常启动并写日志，不需要编译、不需要联网
+
+#### Scenario: 第三方库日志
+
+- **WHEN** `kafka` 客户端输出 WARNING 级日志
+- **THEN** 该记录经 `InterceptHandler` 进入 loguru 的 sink，格式与其它日志一致
+
+### Requirement: 日志格式
+
+采集端 SHALL 支持 `text` 与 `json` 两种格式（由 `LOG_FORMAT` 选择）。
+
+**text 模式 SHALL 直接使用 loguru 的原生格式**（含时间、级别、模块名:行号、消息），
+SHALL NOT 自研 logback pattern 解析器；格式 SHALL 可用 `LOG_FORMAT` 覆盖
+（loguru 原生字符串语法，如 `{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name} | {message}`）。
+
+**json 模式**：每行一个 JSON 对象，含 `timestamp` / `level` / `logger` / `thread` /
+`message` 字段；当整条消息是 `k=v` 形式时，那些键值 SHALL **额外成为顶层字段**
+（客户侧日志平台要能直接按 `channel` / `fetched` / `produced` 等字段取值，
+SHALL NOT 只能从一段 `message` 字符串里解析）。
+
+异常路径 SHALL 带 `exception` 字段（堆栈不丢）；text 模式下堆栈 SHALL 追加在消息之后。
+
+#### Scenario: text 格式
+
+- **WHEN** `LOG_FORMAT=text` 且未配置 `LOG_FORMAT` 覆盖值
+- **THEN** 每行含时间、级别、模块名与消息，格式为 loguru 原生形态
+
+#### Scenario: 自定义格式
+
+- **WHEN** 配置 `LOG_FORMAT={time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name} | {message}`
+- **THEN** 输出按该 loguru 原生语法渲染
+
+#### Scenario: json 格式
+
+- **WHEN** `LOG_FORMAT=json`
+- **THEN** 每行是一个可被 `json.loads` 解析的对象，
+  且通道汇总行里的 `channel` / `fetched` / `produced` 是**顶层字段**
+
+#### Scenario: 异常记录
+
+- **WHEN** 记录日志时带有异常
+- **THEN** json 输出含 `exception` 字段且含完整堆栈；text 输出在消息后追加堆栈
+
+### Requirement: 日志切分与保留
+
+采集端 SHALL 用 loguru 的 `rotation` / `retention` 实现切分与保留，
+文件名 SHALL 带日期后缀。
+
+- 按天切分：文件名用 loguru 的 `{time:YYYY-MM-DD}`，跨天即换新文件；
+- 单日文件大小上限由 `LOG_MAX_BYTES` 控制（默认 **50 MB**）——
+  同一天内超出时 SHALL 切分（旧文件被改名，新文件续写）；
+- 保留天数由 `LOG_RETENTION_DAYS` 控制（默认 **3**）——
+  保留**最近 3 个自然日（含当天）**，日期更早的文件 SHALL 删除；
+- 被改名产生的同日分片 SHALL 同样纳入保留期清理；
+- 切分与清理 SHALL 对总日志与全部通道日志**一致生效**。
+
+#### Scenario: 跨天切分
+
+- **WHEN** 进程跨过午夜继续写日志
+- **THEN** 关闭当天的文件，打开 `<基名>-<新日期>.log`
+
+#### Scenario: 单日超限
+
+- **WHEN** 某个当天的日志文件写满 `LOG_MAX_BYTES`
+- **THEN** 旧文件被改名为 `<基名>-<日期>.<日期>_<时分秒_微秒>.log`，
+  并新建同名文件继续写
+
+#### Scenario: 过期清理
+
+- **WHEN** 日志目录里存在日期早于「今天 − (保留天数 − 1)」的文件
+- **THEN** 这些文件被删除；最近 3 个自然日的文件（含被改名的同日分片）保留
+
+#### Scenario: 默认值
+
+- **WHEN** 未配置 `LOG_MAX_BYTES` / `LOG_RETENTION_DAYS`
+- **THEN** 分别按 50 MB 与 3 天生效
+
+### Requirement: 采集端后台运行
+
+`deploy/collector.sh run` SHALL **后台启动**采集端：命令返回后进程仍在运行，
+SHALL NOT 占用终端（SHALL NOT 进入日志跟随状态）。
+
+后台启动 SHALL：
+
+- 写 **PID 文件**（默认 `<安装目录>/run/collector.pid`）；
+- 启动后确认进程存活，不存活则返回非零退出码并打印日志尾部；
+- 已有实例在跑时 SHALL **拒绝重复启动**并给出提示；
+- 停止后台进程 SHALL 走 SIGTERM（优雅退出，采集端据此推进/丢弃在途批次）。
+
+`deploy/collector.sh` SHALL 另提供：
+
+- `run --foreground`：**前台**运行（原行为），供 systemd 单元使用；
+- `status`：报告进程是否在跑、PID、运行时长、日志目录；
+- `stop`：按 PID 文件优雅停止。
+
+systemd 单元的 `ExecStart` SHALL 改用 `collector.sh run --foreground`
+（前台模式不写 PID 文件）。
+
+#### Scenario: 后台启动
+
+- **WHEN** 执行 `bash collector.sh run`
+- **THEN** 命令在数秒内返回 0，PID 文件存在，进程仍在后台运行，日志文件持续增长
+
+#### Scenario: 重复启动
+
+- **WHEN** 已有实例在跑时再次执行 `collector.sh run`
+- **THEN** 命令返回非零退出码并说明「已在运行」与既有 PID，不启动第二个进程
+
+#### Scenario: 前台模式
+
+- **WHEN** 执行 `bash collector.sh run --foreground`
+- **THEN** 进程在前台运行，终端输出实时日志，不写 PID 文件
+
+#### Scenario: 停止
+
+- **WHEN** 执行 `bash collector.sh stop`
+- **THEN** 进程收到 SIGTERM 后优雅退出，PID 文件被清理
+
+#### Scenario: systemd 兼容
+
+- **WHEN** systemd 用 `ExecStart=/opt/tpops-collector/collector.sh run --foreground` 启动
+- **THEN** 进程保持在前台，systemd 能正常管理与重启它
+
+### Requirement: 日志不落敏感信息
+
+写落到**文件**的日志 SHALL 与写往 stderr 的日志遵守同一条红线：
+SHALL NOT 出现 TPOPS 账号口令、mini-token、数据库业务 SQL 文本全文。
+
+#### Scenario: 采集异常
+
+- **WHEN** 命令中心任务失败，日志需要打印失败原因
+- **THEN** 打印接口路径与错误分类，SHALL NOT 打印 `sql_text` 全文
+
+## MODIFIED Requirements
+
+### Requirement: 每轮结构化汇总日志
+
+采集端 SHALL 每个调度 tick 输出一行结构化汇总日志到**总日志**，
+包含 tick 号、耗时、拉取数、转换数、投递数、失败数、丢弃的不可提供指标数、窗口数、告警数。
+
+该行 SHALL 为**整体**口径；各通道的分开口径由「每通道每轮汇总行」承担，
+SHALL NOT 把通道明细塞进整体行。
+
+#### Scenario: 正常一轮
+
+- **WHEN** 一个 tick 内四组任务均完成
+- **THEN** 总日志输出一行含上述全部计数的日志，且各计数之和自洽（投递数 + 失败数 ≤ 转换数）
+
+#### Scenario: 汇总行落盘
+
+- **WHEN** 进程以 `LOG_DIR` 配置运行一轮
+- **THEN** **整体**汇总行只出现在总日志；**各通道**的汇总行只出现在对应通道日志，
+  同时以简洁形式出现在总日志（SHALL NOT 出现通道内部明细）
+
+### Requirement: 凭据仅来自环境变量且不入日志
+
+TPOPS 用户名口令、mini-token SHALL 仅来自环境变量；
+SHALL NOT 出现在任何日志输出中——包括**写往文件的日志**。
+
+#### Scenario: 认证失败
+
+- **WHEN** 认证接口返回失败
+- **THEN** 日志记录失败原因与接口路径，不含口令或 token
+
+#### Scenario: 落盘日志
+
+- **WHEN** 日志写入 `<LOG_DIR>` 下的文件
+- **THEN** 文件内容同样不含凭据与 token
diff --git a/changes/tpops-mapping-and-logging/tasks.md b/changes/tpops-mapping-and-logging/tasks.md
new file mode 100644
index 0000000..9e7d998
--- /dev/null
+++ b/changes/tpops-mapping-and-logging/tasks.md
@@ -0,0 +1,230 @@
+# 实现任务：指标清理、日志框架、字段映射修正
+
+> **路径约定**：运行期映射数据真相源 = `tpops_kafka_collector/mapping/data/`；
+> 仓库根 `data/mapping/` 是**同源副本**（由生成器同步，改一侧必须同步另一侧）。
+> 下文写 `包内/…` 指前者、`仓库根/…` 指后者。
+
+## 交付与证明
+
+| 批次 | 交付结果 | 依赖 | 证明 |
+|---|---|---|---|
+| 1 | 生效指标口径收敛到 87 条，零重复、零丢弃；表级指标改 `schema_name.table_name`；告警统一指标名 | 无 | `pytest tests/unit/test_excluded_metrics.py tests/unit/test_dimension_binding.py tests/unit/test_alarm_metric_unification.py tests/unit/test_metric_catalog.py -q` |
+| 2 | 日志框架换成 loguru：落盘（总日志 + 5 个通道日志，按天文件名）、按天切分 + 保留 3 天、通道级每轮汇总、全项目改 loguru API | 批次 1（仅排序在后，无代码依赖） | `pytest tests/unit/test_logging_files.py tests/unit/test_loguru_migration.py tests/unit/test_loguru_dependency.py tests/unit/test_channel_round_stats.py tests/integration/test_logs_have_no_secrets.py -q` |
+| 3 | `collector.sh run` 后台化 + `status`/`stop`/`--foreground`；环境变量与交付文档同步 | 批次 2 | `bash tests/scripts/test_collector_runtime.sh` |
+| 4 | 全量回归与门禁 | 批次 1~3 | 见任务 3.4 |
+
+> **每个任务都自带它要新建的测试文件**（下文的「新增 …」即为该任务的交付物之一）——
+> 证明命令引用的测试文件不存在，就是该任务没做完。
+
+## 任务
+
+### 批次 1 · 映射口径
+
+- [ ] **1.1 建排除清单数据文件**：新增 `包内/excluded_metrics.json`，
+  写入 14 条（11 条 `reason=duplicate`，各带 `kept` 指明保留项；3 条 `reason=unsupported`，
+  各带 `detail` 理由）；`reason`/`metric_code`/`metric_name`/`detail` 必填，`kept` 仅重复项必填。
+  同时新增 `仓库根/excluded_metrics.json` 同源副本。
+  **新增 `tests/unit/test_excluded_metrics.py`**：断言清单 14 条、`reason` 分布 11/3、
+  每条 `duplicate` 都有 `kept`、包内与仓库根两份内容一致。
+  完成后：清单内容与客户指标库的 11 组重复 + 3 条无法提供**逐一对应**；
+  证明：`pytest tests/unit/test_excluded_metrics.py -q`。
+
+- [ ] **1.2 生成器按清单剔除**：改 `tools/build_itscada_mapping.py`，
+  生成 `包内/metric_rules.json` 与 `包内/customer_metric_mapping.json` 时
+  跳过排除清单里的指标码。**在 `tests/unit/test_excluded_metrics.py` 里补断言**：
+  重生成后规则数 = 87、`enabled=false` 的规则数 = 0、`metric_code` 无重复。
+  完成后：生效口径 = 87 条；证明：`pytest tests/unit/test_excluded_metrics.py -q`。
+
+- [ ] **1.3 映射层加载与校验排除清单**：改 `tpops_kafka_collector/mapping/loader.py`
+  （加载排除清单）与 `tpops_kafka_collector/mapping/validation.py`
+  （启动期校验），校验三项：排除清单的指标码**一个都不在**生效映射表中、
+  生效规则 `metric_code` 唯一、生效规则数 = 87。
+  **新增 `tests/unit/test_metric_catalog.py`**：覆盖上述三项的正例与反例
+  （人为塞一条被排除的指标码 → 校验必须失败并打印差异）。
+  完成后：不满足即启动失败并打印差异；
+  证明：`pytest tests/unit/test_metric_catalog.py -q`。
+
+- [ ] **1.4 支持点号组合的维度绑定**：改 `tpops_kafka_collector/mapping/rules_context.py`
+  的 `row_dimensions` 与 `grouped_items`——`dimension_bindings` 的值支持
+  「点号连接的多个列名」（按 `.` 连接各列的值），`group_by` 支持多列分组。
+  **新增 `tests/unit/test_dimension_binding.py`**：覆盖组合绑定（`schemaname.relname`
+  → `public.t1`）、多列分组、单列绑定的既有行为不回归。
+  完成后：`{"NAME": "schemaname.relname"}` 绑出 `public.t1`；
+  证明：`pytest tests/unit/test_dimension_binding.py -q`。
+
+- [ ] **1.5 生成器与规则改绑 schema+relname**：改 `tools/build_itscada_mapping.py` 的
+  `DIMENSION_COLUMN_BY_SQL`（`'stat_user_tables': {'NAME': 'relname'}` →
+  `'schemaname.relname'`）**并**改其 `group_by` 派生逻辑（点号串要拆成多列，
+  否则 `group_by` 会变成一个叫 `schemaname.relname` 的单列）；
+  重生成后 `包内/metric_rules.json` 里 11 条表级规则的
+  `dimension_bindings` = `{"NAME": "schemaname.relname"}`、`group_by` = `["schemaname","relname"]`。
+  **在 `tests/unit/test_metric_catalog.py` 里补断言**：表级规则一律按两列分组。
+  完成后：表级规则的 `NAME` 绑定为 `schemaname.relname`、按两列分组；
+  证明：`pytest tests/unit/test_metric_catalog.py -q`。
+
+- [ ] **1.6 表级幂等不碰撞**：**在 `tests/unit/test_dimension_binding.py` 里补用例**，
+  断言 `public.t1` 与 `other.t1` 的同一表级指标得到**不同**的 `metricName` /
+  `metricCode` / 幂等键，而同一张表重复采集得到**相同**的幂等键。
+  完成后：两条消息都会被投递；证明：`pytest tests/unit/test_dimension_binding.py -q`。
+
+- [ ] **1.7 告警统一指标名**：在 `包内/alarm_rules.json` 增加
+  `event_metric`（`metric_code` / `metric_name` 固定值），改
+  `tools/build_itscada_mapping.py` 让**未挂客户指标**的事件类告警统一取该值，
+  挂靠了客户指标的告警保持不变；并在 `包内/open_items.json` 登记该统一值待客户确认。
+  同时**放宽启动期校验**：`metric_code` 唯一性**只对指标规则要求**，对告警定义不要求
+  （告警定义靠 `alarm_id` 唯一定位）。
+  **新增 `tests/unit/test_alarm_metric_unification.py`**：断言 254 条事件类告警的
+  `metric_code` 相同、`alarm_name` 各不相同；3 条挂靠客户指标的告警的
+  `metric_code` 仍等于该客户指标；告警定义不触发唯一性校验失败。
+  完成后：254 条事件类告警 `metricCode` 相同、`alarmName` 各不相同；
+  证明：`pytest tests/unit/test_alarm_metric_unification.py -q`。
+
+- [ ] **1.8 交付文档重生成**：跑 `tools/build_delivery_docs.py` 重生成
+  `docs/交付-指标口径说明.md`（87 条）、`docs/交付-不可提供指标清单.md`
+  （3 条，标注「已从生效口径移除」+ 理由）、`docs/交付-客户指标英文名核对清单.md`；
+  同步更新 `docs/映射表-ITSCADA字段对照.md` 与 `tpops_kafka_collector/mapping/README.md`。
+  **新增 `tests/unit/test_delivery_docs.py`**：断言三份交付文档里的条数与
+  生效映射表一致（87 / 3 / —），防止文档与代码漂移。
+  完成后：文档条数与生效口径一致；证明：`pytest tests/unit/test_delivery_docs.py -q`。
+
+### 批次 2 · 日志框架
+
+- [ ] **2.1 loguru 依赖落地**：在 `requirements-runtime.txt` 加 `loguru==0.8.0`（锁版本）；
+  改 `tools/build_deploy_bundle.py` 的离线 wheel 清单，把 loguru 纳入部署包；
+  确认它是 `py3-none-any` 且在 Linux + py3.7 下无传递依赖。
+  **新增 `tests/unit/test_loguru_dependency.py`**：断言部署包清单含 loguru、
+  其 wheel 为纯 Python（`*-none-any`）、`requirements-runtime.txt` 锁了版本。
+  完成后：目标机只装 4 个纯 Python 包即可跑；证明：`pytest tests/unit/test_loguru_dependency.py -q`。
+
+- [ ] **2.2 `logging_setup.py` 重写为 loguru sink 装配**：改
+  `tpops_kafka_collector/logging_setup.py`，装配：
+  - 总日志 sink（生命周期 + 整体汇总 + 每通道简洁汇总 + WARNING 以上）；
+  - 5 个通道 sink，`filter=` 读 `record["extra"]["channel"]`；
+  - stderr sink（前台/调试可见）；
+  - `rotation=` 自定义回调（跨天 or 当日文件超 `LOG_MAX_BYTES`）、
+    `retention=` 保留 `LOG_RETENTION_DAYS` 天、文件名 `<基名>-{time:YYYY-MM-DD}.log`；
+  - JSON sink 用 `format=` **自定义函数**产出既有 schema（`timestamp`/`level`/`logger`/
+    `thread`/`message` + `k=v` 摊平 + `exception`）；
+  - 官方 `InterceptHandler` 把第三方库 stdlib 日志转发进 loguru；
+    `logger.disable("kafka")` 等降噪；
+  - 保留现有 `setup_logging(level, log_format, stream)` 签名与幂等语义
+    （py37 无 `basicConfig(force=…)`，自判 `root.handlers`）。
+  **新增 `tests/unit/test_logging_files.py`**：覆盖文件名带日期、跨天切分、
+  单日超限切分（旧文件改名 + 新文件续写）、过期文件被删而近 3 天保留、
+  通道文件只含本通道记录、总日志含各通道简洁汇总、
+  通道 INFO 不进总日志而 ERROR 两边都有、日志目录不可写时报错、
+  既有 `setup_logging` 调用不回归、重复调用不叠加 sink、json 的 `exception` 字段保留。
+  完成后：写日志时按日期命名、按天切分、过期自动清理、通道不串味；
+  证明：`pytest tests/unit/test_logging_files.py -q` 与既有
+  `pytest tests/unit/test_logging_setup.py -q`。
+
+- [ ] **2.3 全面改用 loguru API**：把生产包内各模块的
+  `LOGGER = logging.getLogger(__name__)` 改为 `from loguru import logger`
+  （**调用点 `LOGGER.info(...)` 本身不动**）；
+  5 个采集器模块各加一行 `LOGGER = logger.bind(channel="<通道名>")`
+  （`monitor` / `command_center` / `diagnostic` / `instance` / `alarm`）；
+  非采集器模块保持无 `channel` 绑定（其记录只进总日志）。
+  **新增 `tests/unit/test_loguru_migration.py`**：断言生产包内不再出现
+  `logging.getLogger`（第三方库转发用的 `InterceptHandler` 除外），
+  且 5 个采集器模块的记录带上正确的 `extra["channel"]`。
+  完成后：全项目日志走 loguru；证明：`pytest tests/unit/test_loguru_migration.py -q`。
+
+- [ ] **2.4 配置项与快速失败**：改 `tpops_kafka_collector/config.py`，加 `LOG_DIR` /
+  `LOG_FORMAT` / `LOG_MAX_BYTES`（默认 50 MB）/ `LOG_RETENTION_DAYS`（默认 3）；
+  取值非法 → 启动失败退出码 2；日志目录不可写 → 启动失败并说明路径；
+  `LOG_DIR` 未设置 → 只写 stderr（不落盘）；同步 `.env.example`。
+  **新增 `tests/unit/test_config.py`**（若无同名文件则新建）：覆盖非法取值、
+  未设置 `LOG_DIR` 的默认行为、合法取值的加载。
+  完成后：非法取值与不可写目录都在启动期失败；
+  证明：`pytest tests/unit/test_config.py -q`。
+
+- [ ] **2.5 通道级每轮统计**：新增不可变 DTO `ChannelRoundStats`
+  （`tpops_kafka_collector/scheduler_types.py`）与聚合逻辑
+  **新文件 `tpops_kafka_collector/channel_stats.py`**（按通道聚合
+  `fetched` / `mapped` / `produced` / `failed` / `windows` / `calls` / `elapsed` / `instances`）；
+  `scheduler.py` 只负责在 tick 里调用，SHALL NOT 把聚合逻辑堆进 `scheduler.py`
+  （该文件已 392 行）。
+  **新增 `tests/unit/test_channel_round_stats.py`**：覆盖按通道聚合的正确性、
+  各通道计数之和 == 整体行同名字段。
+  完成后：通道统计与整体统计自洽；
+  证明：`pytest tests/unit/test_channel_round_stats.py -q`。
+
+- [ ] **2.6 通道级汇总行输出**：改 `tpops_kafka_collector/scheduler_report.py`，
+  每轮为**到期且完成**的通道各输出一条汇总行（用 `logger.bind(channel=…)` 打出，
+  进通道日志），同时在总日志输出一条同字段的核心计数；
+  字段固定为 `channel/tick/instances/fetched/mapped/produced/failed/windows/calls/elapsed/delivery_rate`。
+  **在 `tests/unit/test_channel_round_stats.py` 里补断言**：未到期通道不出行；
+  json 模式下这些是**顶层字段**；通道行各字段之和 == 整体行。
+  完成后：未到期通道不出行；json 模式下字段可直接被日志平台取；
+  证明：`pytest tests/unit/test_channel_round_stats.py -q`。
+
+- [ ] **2.7 敏感信息不进落盘日志**：跑一轮采集后扫描日志文件，确认无口令 /
+  mini-token / `sql_text` 全文；必要时收紧采集器里的日志语句。
+  **新增 `tests/integration/test_logs_have_no_secrets.py`**：跑一轮后
+  grep `<LOG_DIR>` 下全部文件，断言不含配置的凭据值与 token 片段。
+  完成后：落盘文件不含敏感串；
+  证明：`pytest tests/integration/test_logs_have_no_secrets.py -q`。
+
+### 批次 3 · 部署与文档
+
+- [ ] **3.1 `collector.sh` 后台化**：把 `run` / `status` / `stop` 三段拆到新文件
+  `deploy/collector_runtime.sh`（`collector.sh` 位置参数分发，对外仍是一个脚本）；
+  `run` 走 `setsid nohup` 后台启动、写 `<安装目录>/run/collector.pid`、
+  启动后确认存活（**没起来则返回非零并打印日志尾部**：`<LOG_DIR>/collector.log`
+  与 `<安装目录>/run/console.out` 的最后 N 行）、已有实例在跑则拒绝启动；
+  `run --foreground` 保持前台；`stop` 发 SIGTERM 并清理 PID 文件；
+  `<安装目录>/run/` 目录由脚本创建。
+  **新增 `tests/scripts/test_collector_runtime.sh`**：覆盖后台启动后进程仍在、
+  重复启动返回非零、`status` 报告正确、`stop` 后进程退出且 PID 文件被清理、
+  `--foreground` 前台模式不写 PID 文件。
+  完成后：脚本单文件 < 400 行、重复启动返回非零；
+  证明：`bash tests/scripts/test_collector_runtime.sh`。
+
+- [ ] **3.2 systemd 与部署说明同步**：更新 `docs/交付-部署运行说明.md` 的 unit 片段为
+  `ExecStart=/opt/tpops-collector/collector.sh run --foreground`，补 `LOG_DIR` 等新环境变量表、
+  日志目录说明与磁盘占用上限（6 基名 × 3 天 × 50 MB）、`<安装目录>` 的定义；
+  更新 `deploy/README.md` 的子命令表。
+  **在 `tests/unit/test_delivery_docs.py` 里补断言**：交付说明里的 `ExecStart`
+  含 `run --foreground`、环境变量表含 `LOG_DIR`/`LOG_MAX_BYTES`/`LOG_RETENTION_DAYS`/`LOG_PATTERN`。
+  完成后：文档里的命令与脚本实际行为一致；
+  证明：`pytest tests/unit/test_delivery_docs.py -q`。
+
+- [ ] **3.3 日志框架规范成文**：把决策 1~6（布局与日期文件名、sink 分层、loguru 原生格式、
+  按天切分与 3 天保留、汇总字段口径、后台运行）写成 `docs/日志框架规范.md`，
+  与 `design.md` 的决策段一致；并同步更新 `CLAUDE.md` 的编码约定
+  （「用 `logging`」→ 「用 `loguru`」）与「运行时依赖三个纯 Python 包」的表述。
+  **新增 `tests/unit/test_logging_doc.py`**：校验文档里的字段名与
+  `tpops_kafka_collector/scheduler_report.py` 的常量一致、文件布局与
+  `tpops_kafka_collector/logging_files.py` 一致。
+  完成后：文档覆盖目录布局、字段表、示例行；
+  证明：`pytest tests/unit/test_logging_doc.py -q`。
+
+- [ ] **3.4 全量回归与门禁**：跑
+
+  ```bash
+  conda run -n tpops python -m pytest -q
+  conda run -n tpops python -m pytest --cov=tpops_kafka_collector --cov-report=term-missing -q
+  conda run -n tpops python -m pytest tests/test_py37_compat.py -q
+  conda run -n tpops python -m ruff check tpops_kafka_collector tests
+  ```
+
+  覆盖率门槛：**全包 ≥ 80%**、**映射层 ≥ 90%**（口径与 `proposal.md` 一致）。
+  完成后：全绿、覆盖率达标、py37 兼容门禁通过、ruff 无告警；
+  证明：上述四条命令的输出。
+
+## 实施备注（仅在必要时）
+
+- **表级指标的对外命名**：批次 1 完成后，表级指标的 `metricCode` 变成「每表一个」
+  （如 `gaussdb.public.t1.number.Of.insert.tuples`），`metricName` 变成
+  `(public.t1)插入的行数`。客户 ITSCADA 按 `metricName` 匹配台账，
+  **表级指标的台账项要按实际库表名录入** —— 3.2 必须写清这一点。
+- **告警统一值是待确认项**：`gaussdb.alarm` / `数据库告警` 是拟值，
+  已登记进 `open_items.json`；客户若给不同的名字，改映射数据即可，不改代码。
+- **不要改客户 xlsx**：排除清单是生效口径的唯一落点（决策 7）。
+- **字段名不新造**：通道汇总行复用 `TickResult` 既有字段名
+  （`fetched` / `mapped` / `produced` / `failed` / `windows` / `calls`），
+  新增的只有 `channel` / `instances` / `elapsed` / `delivery_rate`。
+- **`scheduler.py` 已 392 行**，加通道聚合会超 400 行 → 聚合逻辑放新文件
+  `tpops_kafka_collector/channel_stats.py`，输出放 `scheduler_report.py`，
+  `scheduler.py` 只留编排（任务 2.4）。
+- **`ruff` 只扫 Python**：`deploy/` 下是 shell 脚本，不进 ruff 的检查范围。
diff --git a/data/metric_response/README.md b/data/metric_response/README.md
index b13c407..364300a 100644
--- a/data/metric_response/README.md
+++ b/data/metric_response/README.md
@@ -77,6 +77,11 @@
 
 3 个判定为**无法提供**的指标不建文件（原因见 `data/customer_metric_unsupported.csv`）。
 
+> **归档 ≠ 生效口径**：本目录是**抓取归档**（平台事实，98 份样本），
+> 采集端的**生效口径**现在是 87 条指标（客户指标库 101 条 − 排除清单 14 条，
+> 见 `tpops_kafka_collector/mapping/data/excluded_metrics.json`）。
+> 被排除的 11 条重复表级指标的样本仍留在归档里 —— 归档是事实，生效口径是决定。
+
 ### 所有通道只查一个实例
 
 实测实例 = **分布式实例 `gauss-6374`**（`83cd68f2e4cb40b4b7e69240479ffa26in14`，mode=Combined）：
diff --git "a/docs/\346\230\240\345\260\204\350\241\250-ITSCADA\345\255\227\346\256\265\345\257\271\347\205\247.md" "b/docs/\346\230\240\345\260\204\350\241\250-ITSCADA\345\255\227\346\256\265\345\257\271\347\205\247.md"
index c083600..37b5a0f 100644
--- "a/docs/\346\230\240\345\260\204\350\241\250-ITSCADA\345\255\227\346\256\265\345\257\271\347\205\247.md"
+++ "b/docs/\346\230\240\345\260\204\350\241\250-ITSCADA\345\255\227\346\256\265\345\257\271\347\205\247.md"
@@ -178,8 +178,8 @@
 | `status` | Integer | 必填 | TPOPS 字段 | convergence | `records[].status` | TPOPS 响应里直接取 | activated→1（告警）/ recovered、cleared→0（恢复） |
 | `recoverTime` | Long | 条件必填 | TPOPS 字段 | convergence | `records[].clear_alarm_time` | TPOPS 响应里直接取 | status=0（恢复）时必填；status=1（告警）时输出 null |
 | `resourceName` | String | 必填 | 组装 | 明细行自带 node_id 与 host_ip；收敛条目不带，按 alarm_id 关联明细行回填 | `records[].node_id / records[].host_ip` | 由上面几个字段组装 | resourceName 只能表示节点 → 拼 {host_ip}_{node_id}；仍拿不到节点时落到节点清单里的第一个 master CN |
-| `metricName` | String | 必填 | 映射表 | 按 alarm_id 查表 | `data/mapping/alarm_rules.json → metric_name` | 用 alarm_id 匹配 alarm_rules.json 的 definitions[alarm_id] | 能对应到客户指标的告警用客户指标中文名；其余用告警名 |
-| `metricCode` | String | 必填 | 映射表 | 按 alarm_id 查表 | `data/mapping/alarm_rules.json → metric_code` | 用 alarm_id 匹配 alarm_rules.json 的 definitions[alarm_id] | 能对应到客户指标的告警用客户指标英文名；其余用 gaussdb.alarm.<alarm_id> |
+| `metricName` | String | 必填 | 映射表 | 按 alarm_id 查表 | `data/mapping/alarm_rules.json → metric_name` | 用 alarm_id 匹配 alarm_rules.json 的 definitions[alarm_id] | 能对应到客户指标的告警用客户指标中文名；其余用统一指标名（alarm_rules.json 的 event_metric 段） |
+| `metricCode` | String | 必填 | 映射表 | 按 alarm_id 查表 | `data/mapping/alarm_rules.json → metric_code` | 用 alarm_id 匹配 alarm_rules.json 的 definitions[alarm_id] | 能对应到客户指标的告警用客户指标英文名；其余用统一指标名 gaussdb.alarm（alarm_rules.json 的 event_metric 段） |
 | `value` | String | 必填 | TPOPS 字段 | convergence | `records[].alarm_items_sum` | TPOPS 响应里直接取 | 事件类告警取命中节点数；指标类告警取该客户指标最近一次采样值（取不到则 null，不填 0） |
 | `unit` | String | 选填 | 映射表 | 按挂到的客户指标查表 | `data/mapping/metric_rules.json → unit` | 按挂到的客户指标查该条目的 unit | 挂到客户指标的告警取该指标单位；事件类告警为 null |
 | `tags` | Object | 选填 | 组装 | 两者都取 | `records[].instance_id / records[].instance_name / records[].node_id / records[].host_ip / records[].alarm_category / records[].alarm_type / records[].engine / records[].alarm_duration / records[]（明细行整行）` | 由上面几个字段组装 | 前 8 个平铺，明细行整行放 details[] |
@@ -208,13 +208,14 @@
 |---|---|---|---|
 | 1 | value 的线格式 | 按客户指标表【数据类型】序列化（整数/浮点→数字，字符/文本→字符串） | 两份方案的字段表都写 value 为 String，但指标样例里是数字 75.3、告警样例里是字符串 "95.5"。 |
 | 2 | 指标名字段的叫法 | 用字段表的 metricName / metricCode | 指标方案的字段表用 metricName/metricCode，告警方案的样例 JSON 里写的是 metric_name。 |
-| 3 | resourceName 与客户台账的对齐 | {host_ip}_{node_id}（节点 IP + TPOPS 节点 ID）；与节点无关的指标落到节点清单里的第一个 master CN | resourceName 只能表示一个节点 → 固定「节点 IP + 节点 ID」；表名/库名/端口这类业务维度改走 metricName/metricCode 的占位符与 tags.dimension_values。方案要求与 ITSCADA 台账录入的资源名称完全一致；台账命名规则待客户提供。 |
+| 3 | resourceName 与客户台账的对齐 | {host_ip}_{node_id}（节点 IP + TPOPS 节点 ID）；与节点无关的指标落到节点清单里的第一个 master CN | resourceName 只能表示一个节点 → 固定「节点 IP + 节点 ID」；表名/库名/端口这类业务维度改走 metricName/metricCode 的占位符与 tags.dimension_values；表级指标的维度值是 schema_name.table_name（如 public.t1）。方案要求与 ITSCADA 台账录入的资源名称完全一致；台账命名规则待客户提供。 |
 | 4 | 采集频率 | 以客户指标表【数据更新周期】为准（30 秒~24 小时共 7 档） | 方案建议「需告警的指标 1 分钟一次、无需告警的 5 分钟一次」，与客户表的分档不一致。 |
 | 5 | 状态类指标的展示值转义 | 平台枚举值原样透传（实测 1=正常、0=异常） | 5 条状态类指标（实例状态、主备状态）的客户展示值转义未定义。 |
-| 6 | 告警挂到哪个客户指标 | 257 条告警里只有 3 条能对应上客户指标（都指向 gaussdb.Tps），其余用 gaussdb.alarm.<alarm_id> 自造指标名 | 事件类告警没有对应的客户指标，需要客户确认这种挂法是否可接受。 |
+| 6 | 告警挂到哪个客户指标 | 未挂客户指标的 254 条事件类告警统一用 metricCode=gaussdb.alarm / metricName=数据库告警 | 原先每条告警各造一个 gaussdb.alarm.<alarm_id> 指标名（还有 gaussdb.alarm.>=75% 这种拿阈值当 ID 的），客户侧要建 254 条台账条目。统一后告警种类由 alarmId / alarmName / alarmContent 承载，ITSCADA 侧告警台账从 254 项变 1 项。**对外契约变化，待客户确认。** |
 | 7 | 慢 SQL 的 sql_text 能否外发 | 默认脱敏：字面量替换为 ?，并截断到 512 字符 | 含业务 SQL 文本，需客户书面确认。 |
 | 8 | A 类多 series 的归集口径 | 暂按逐组件各出一条消息（现状实现），维度键 COMPONENT_ID 归入 tags | A 类 11 个客户指标都是实例级一条消息，但平台按组件给出多条 series（rds092/rds093/rds400 各 3 条 CN、rds402 6 条 CN+DN、rds133 3 条 CN、gaussdb_instance_status 1 条 value、datanode_switch_status 9 条 DN）。合成方式属业务口径：速率类可求和，启动时刻可取最早，主备状态可按 DN 展开或归集为布尔。确认后由 metric_rules.json 的 value_rule 承载，不改消息信封。 |
-| 9 | 11 组英文名仅大小写不同、中文名不同 | 两套都投递（metricCode 只差前缀大小写，值完全一样） | 客户指标库 101 个指标里，英文名忽略大小写后有 11 组相同但中文名不同（全部是表级指标：小写 gaussdb. 前缀=中文名+表信息+2 分钟，大写 GAUSSDB. 前缀=英文名+事务信息+5 分钟）。客户侧若按大小写不敏感匹配，这 11 个指标会合并覆盖。要客户确认：是两条独立指标（需给出区分办法），还是同一条的重复登记（留哪一套）。清单见 docs/问题单.md TPOPS-003。 |
+| 9 | 11 组英文名仅大小写不同、中文名不同 | 只投递中文名正常的一套（小写 gaussdb. 前缀）；大写 GAUSSDB. 前缀的 11 条进排除清单，不产出消息 | 客户指标库 101 个指标里，英文名忽略大小写后有 11 组相同但中文名不同（全部是表级指标：小写 gaussdb. 前缀=中文名+表信息+2 分钟，大写 GAUSSDB. 前缀=英文名+事务信息+5 分钟）。客户侧若按大小写不敏感匹配，这 11 个指标会合并覆盖。本次按「保留中文名正常的一条」落地，被排除的 11 条见 tpops_kafka_collector/mapping/data/excluded_metrics.json。清单见 docs/问题单.md TPOPS-003。 |
 | 10 | 速率型指标发累计量，单位与值对不上 | 按源列原值发（累计计数），单位仍按客户表原样带 | 客户表里单位标成 tps/tuples/s/scans/s 的 10 个指标（GAUSSDB.[{#DATNAME}]_committed_transactions、_rolled_back_transactions 与 GAUSSDB.{#NAME}.number.Of.update.tuples 等 8 个表级指标），平台源列是累计计数，现发的是累计值；同族 4 个 *_records_per_second 用差分（counter_delta）口径是对的。改法：把 10 条规则的 value_rule 从 raw 改成 counter_delta；但差分窗口=采集周期（5 分钟），是否再除以秒数换算成「每秒」需客户确认。清单见 docs/问题单.md TPOPS-002。 |
 | 11 | 几处单位与来源字段语义对不上 | 按客户表原样带单位 | gaussdb.Temp_files 单位 b（源列 temp_files 是文件个数）、gaussdb.Conflicts 单位 ms（源列 conflicts 是冲突次数）、gaussdb.Blk_read_time 单位 个（源列 blk_read_time 是毫秒）、GAUSSDB.{#NAME}.Number.t.date.ples 单位 %（源列 n_tup_hot_upd 是行数）。是客户表抄错还是另有含义？确认后只改映射表。清单见 docs/问题单.md TPOPS-004。 |
+| 12 | 事件类告警的统一指标名取值 | metricCode=gaussdb.alarm、metricName=数据库告警 | 统一值写在 alarm_rules.json 的 event_metric 段（表驱动），改口径只改数据文件、不改代码。统一后 ITSCADA 侧的告警指标台账从 254 项变 1 项（告警种类由 alarmId / alarmName / alarmContent 承载）——**对外契约变化，需客户确认这个指标名与中文名。** |
 
diff --git a/tests/integration/test_collector_command_center.py b/tests/integration/test_collector_command_center.py
index bfc8974..02f8a3b 100644
--- a/tests/integration/test_collector_command_center.py
+++ b/tests/integration/test_collector_command_center.py
@@ -3,15 +3,15 @@
 
 **TPOPS 与 Kafka 一律 mock** —— 本文件不发起任何网络请求。
 
-守的是这个项目里**最大的一块**（82 个客户指标走这条通道）的六个要害：
+守的是这个项目里**最大的一块**（71 个客户指标走这条通道）的六个要害：
 
 1. **必须走 ``decode_merged`` / ``merge_pages``** —— 合并形态当行列表用，
-   82 个指标会**静默全变 0**（本项目最危险的失败形态）；
+   71 个指标会**静默全变 0**（本项目最危险的失败形态）；
 2. **必须检查 ``outcome.ok``** —— 命令中心失败不抛异常，不检查就是静默丢数据；
 3. **分页只对可增长视图生效** —— 固定视图每页重复返回全量，累加会翻倍；
 4. **``node_scoped`` 归集** —— ``true`` 各 master 各自一份，``false`` 只取第一个 master；
 5. **输出上限 102400 B** —— 接近上限记 WARNING，超了整条失败（不是截断）；
-6. **值等于期望值表** —— 82 条逐条断言（不是「非零」判据）。
+6. **值等于期望值表** —— 71 条逐条断言（不是「非零」判据）。
 """
 import io
 import json
@@ -262,7 +262,8 @@ def _growable_page(row_count, offset=0):
     row = dict(_base_row())
     for name in GROWABLE_VIEWS:
         row[name] = json.dumps(
-            [{'relname': 't%d_%d' % (offset, i)} for i in range(row_count)]
+            [{'schemaname': 'public', 'relname': 't%d_%d' % (offset, i)}
+             for i in range(row_count)]
         )
     return json.dumps([row])
 
@@ -423,16 +424,16 @@ def test_output_limit_constant_matches_the_platform_value():
 
 
 # ---------------------------------------------------------------------------
-# 6. 值等于期望值表（82 条逐条）
+# 6. 值等于期望值表（71 条逐条）
 # ---------------------------------------------------------------------------
 
 def test_collected_values_equal_the_expected_table(mapping, probe_nodes, node_rows, expected_table):
-    """**82 条逐条**：采集器产出的值 == 期望值表。
+    """**71 条逐条**：采集器产出的值 == 期望值表。
 
     期望值表由**独立推导器**从实测留档原始行推出
     （``tools/expected_deriver.py``），不是抄规则引擎的输出。
 
-    **这条守卫覆盖全部 82 条指标码**（不是 47 条）：
+    **这条守卫覆盖全部 71 条指标码**（不是 47 条）：
 
     - 期望值表的键是**模板码**（``GAUSSDB.[{#DATNAME}]_committed_transactions``），
       消息里的 ``metricCode`` 是**占位符替换后的码**
@@ -501,7 +502,7 @@ def test_collected_values_equal_the_expected_table(mapping, probe_nodes, node_ro
     assert checked == EXPECTED_COMPARISONS, (
         '比到的条数与期望值表对不上', checked, EXPECTED_COMPARISONS,
     )
-    # 82 条指标码里，**有期望值**的那些一条都不许漏
+    # 71 条指标码里，**有期望值**的那些一条都不许漏
     assert compared_codes == _codes_with_expected_values(expected_table), (
         '有期望值的指标码没被比到',
         sorted(_codes_with_expected_values(expected_table) - compared_codes),
@@ -509,8 +510,9 @@ def test_collected_values_equal_the_expected_table(mapping, probe_nodes, node_ro
 
 
 #: 期望值表在**第一轮**（差分指标还没基线）的逐维度比对条数。
-#: 82 条指标码 × 3 个 master × 各自主机上的维度条目数 —— 由期望值表算出。
-EXPECTED_COMPARISONS = 1700
+#: 71 条指标码 × 3 个 master × 各自主机上的维度条目数 —— 由期望值表算出。
+#: （生效口径去掉 11 条重复的表级指标后，条数随之变化。）
+EXPECTED_COMPARISONS = 954
 
 
 def _codes_with_expected_values(expected_table):
@@ -527,7 +529,7 @@ def _codes_with_expected_values(expected_table):
 def test_every_b_class_rule_is_covered_by_the_produced_messages(
     mapping, probe_nodes, node_rows
 ):
-    """**82 条指标码一条不落**（按模板码归并消息）。
+    """**71 条指标码一条不落**（按模板码归并消息）。
 
     第一轮有 6 条指标码**产不出消息**，且都是**有据可依**的：
 
@@ -547,7 +549,7 @@ def test_every_b_class_rule_is_covered_by_the_produced_messages(
         rule for rule in mapping.metric_rules
         if rule.enabled and rule.source.get('source') == 'command_center_sql'
     ]
-    assert len(rules) == 82
+    assert len(rules) == 71
     missing = sorted(rule.metric_code for rule in rules
                      if rule.metric_code not in produced_codes)
     assert missing == sorted(NO_VALUE_CODES), missing
@@ -573,12 +575,12 @@ def _template_code(message):
 
 
 def test_expected_table_covers_the_b_class_rules(mapping):
-    """期望值表覆盖全部 82 条 B 类规则（一条不落）。"""
+    """期望值表覆盖全部 71 条 B 类规则（一条不落）。"""
     rules = [
         rule for rule in mapping.metric_rules
         if rule.enabled and rule.source.get('source') == 'command_center_sql'
     ]
-    assert len(rules) == 82
+    assert len(rules) == 71
 
 
 # ---------------------------------------------------------------------------
diff --git a/tests/regression/test_b_class_regression.py b/tests/regression/test_b_class_regression.py
index eca018a..3c132b0 100644
--- a/tests/regression/test_b_class_regression.py
+++ b/tests/regression/test_b_class_regression.py
@@ -1,4 +1,4 @@
-"""B 类（命令中心 SQL）82 个指标的**值相等**回归。
+"""B 类（命令中心 SQL）71 个指标的**值相等**回归。
 
 **这个文件守的是什么**：合并 SQL 返回形态下，取值口径错了**不报错**。
 列名对、聚合口径错（``aggregate`` 的 ``fn`` 写错、``raw`` 的 ``row_policy``
@@ -11,14 +11,14 @@
 留档重新抓取不会让回归跟着变绿。
 
 **期望值从哪来**：
-- B 类 82 条 → ``tests/fixtures/expected_values.json``
+- B 类 71 条 → ``tests/fixtures/expected_values.json``
   （wave-0 的那一张，逐字节等于 ``data/mapping/expected_values.json``），
   由 ``tools/expected_deriver.py`` 从留档原始行**独立推导**，不经过规则引擎；
 - 本文件断言的**被测对象**是 ``tpops_kafka_collector/mapping/rules.py``。
 
 两边是两套实现：引擎把 ``state_contains`` 理解错了，这里就会红。
 
-**覆盖**：82 条 B 类指标**逐条**断言（一条不落），累计计数器另加第 2 轮差分断言。
+**覆盖**：71 条 B 类指标**逐条**断言（一条不落），累计计数器另加第 2 轮差分断言。
 """
 import json
 
@@ -30,7 +30,7 @@ from tpops_kafka_collector.mapping.rules import CounterDeltaStore
 from tpops_kafka_collector.mapping.table import MetricRule
 
 #: B 类指标的条数（一条不落）。
-EXPECTED_B_COUNT = 82
+EXPECTED_B_COUNT = 71
 
 #: 累计计数器所在的视图（第 2 轮合成留档只改这个视图）。
 COUNTER_VIEW = 'stat_database'
@@ -59,7 +59,7 @@ def _evaluate(rule_payload, ctx):
 
 
 def _b_rules():
-    """82 条 B 类指标的声明（**包内**那份 —— 生产用的声明）。"""
+    """71 条 B 类指标的声明（**包内**那份 —— 生产用的声明）。"""
     from regression.conftest import package_mapping
 
     payload = package_mapping('metric_rules.json')
@@ -69,7 +69,7 @@ def _b_rules():
     ]
 
 
-#: 82 条 B 类指标的编码（有序）—— 收集期就要定下来（``parametrize`` 不能用夹具）。
+#: 71 条 B 类指标的编码（有序）—— 收集期就要定下来（``parametrize`` 不能用夹具）。
 B_CODES = sorted(rule['metric_code'] for rule in _b_rules())
 
 #: 累计计数器的 5 条。
@@ -81,17 +81,17 @@ COUNTER_CODES = sorted(
 
 # -- 覆盖完整性 ------------------------------------------------------------
 
-def test_b_class_rule_count_is_82(b_class_rules):
-    """B 类指标就是 82 条 —— 少一条就是少一条回归覆盖。"""
+def test_b_class_rule_count_is_71(b_class_rules):
+    """B 类指标就是 71 条 —— 少一条就是少一条回归覆盖。"""
     assert len(b_class_rules) == EXPECTED_B_COUNT
 
 
 def test_expected_table_covers_every_b_class_metric(expected_table):
-    """期望值表**一条不落**地覆盖 82 条 B 类指标。"""
+    """期望值表**一条不落**地覆盖 71 条 B 类指标。"""
     assert sorted(expected_table['expected']) == B_CODES
 
 
-# -- 值相等：82 条逐条断言 -----------------------------------------------
+# -- 值相等：71 条逐条断言 -----------------------------------------------
 
 @pytest.mark.parametrize('metric_code', B_CODES)
 def test_rule_engine_output_equals_expected_values(
diff --git a/tests/regression/test_collector_coverage.py b/tests/regression/test_collector_coverage.py
index d3baa9d..41392a3 100644
--- a/tests/regression/test_collector_coverage.py
+++ b/tests/regression/test_collector_coverage.py
@@ -1,8 +1,8 @@
 # -*- coding: utf-8 -*-
-"""采集器层的「98 个可提供指标全部产出数据」回归（A / B 类 + 总闸，I-4 / T-16）。
+"""采集器层的「87 个生效指标全部产出数据」回归（A / B 类 + 总闸，I-4 / T-16）。
 
 **回归层原本只测映射层**（``decode_merged`` + ``evaluate``）——
-**从没走过采集器**。合同完成标准「98 个可提供指标全部产出数据」因此
+**从没走过采集器**。合同完成标准「生效指标全部产出数据」因此
 在采集器层**没有任何守卫**：wave-2b 的三条 Critical 全是采集器层的
 （E 类采集器用生产映射对象必崩、B 类差分指标永不产出、差分基线跨 master 污染），
 **一条都没被现有守卫拦住**。
@@ -10,12 +10,12 @@
 本文件**走采集器**：用**实测留档回放**驱动真实的采集器，
 断言该通道的**全部**可提供指标都产出了消息，且**值等于期望值表**。
 
-通道与条数（合计 98 条可提供指标）：
+通道与条数（合计 87 条生效指标）：
 
 ===========  ======  ==========================================
 通道          条数    取数路径
 ===========  ======  ==========================================
-B 类           82    命令中心合并 SQL（``stat_database`` 等 11 个视图）
+B 类           71    命令中心合并 SQL（``stat_database`` 等 11 个视图）
 A 类           11    监控指标接口（``POST /rds/v3/monitor/metrics``）
 C 类            4    诊断优化接口（``/rds/gaussdb/v3/.../slow-sql-list`` 等）
 D 类            1    实例接口（``GET /rds/v3.2/instances``）
@@ -106,11 +106,11 @@ def test_a_class_produces_every_providable_metric(
 
 
 # ---------------------------------------------------------------------------
-# B 类（82 条）
+# B 类（71 条）
 # ---------------------------------------------------------------------------
 
 def test_b_class_produces_every_providable_metric(mapping, nodes, manifest):
-    """B 类 82 条指标码**一条不落**（第一轮 + 第二轮）。
+    """B 类 71 条指标码**一条不落**（第一轮 + 第二轮）。
 
     第一轮 5 个 ``counter_delta`` 指标只建基线（没有值），第二轮才产出 ——
     所以这里跑**两轮**。
@@ -138,7 +138,7 @@ def test_b_class_produces_every_providable_metric(mapping, nodes, manifest):
         rule for rule in mapping.metric_rules
         if rule.enabled and rule.source.get('source') == 'command_center_sql'
     ]
-    assert len(rules) == 82
+    assert len(rules) == 71
     missing = sorted(rule.metric_code for rule in rules
                      if rule.metric_code not in produced)
     # 留档里一条 ``AccessExclusiveLock`` 锁都没有 → 这个指标本来就没有值
@@ -202,13 +202,13 @@ def test_every_master_computes_its_own_rate_in_the_regression_layer(
 
 
 # ---------------------------------------------------------------------------
-# 合同完成标准：98 个可提供指标全部产出数据
+# 合同完成标准：生效指标全部产出数据
 # ---------------------------------------------------------------------------
 
-def test_all_ninety_eight_providable_metrics_are_produced_by_the_collectors(
+def test_all_effective_metrics_are_produced_by_the_collectors(
     mapping, nodes, manifest, providable_codes
 ):
-    """**98 个可提供指标**在**采集器层**全部产出消息（合同的完成标准）。
+    """**87 个生效指标**在**采集器层**全部产出消息（合同的完成标准）。
 
     这是把四个通道的产出合起来看：缺一条就是「某个指标在采集器层不产出」。
     """
@@ -265,12 +265,12 @@ def test_all_ninety_eight_providable_metrics_are_produced_by_the_collectors(
         for message in command_center.collect(instance_id=INSTANCE_ID).messages:
             produced.add(template_code(message))
 
-    assert len(providable_codes) == 98, len(providable_codes)
+    assert len(providable_codes) == 87, len(providable_codes)
     missing = sorted(providable_codes - produced)
     # 留档里**一条 AccessExclusiveLock 锁都没有** → 期望值表里这个指标的条目
     # 是空列表 → 本来就没有值可产出（「缺值不产出假数据」是正确行为）。
     assert missing == ['gaussdb.locks.accessexclusive[{#DBNAME}]'], missing
-    assert len(produced) == 97, len(produced)
+    assert len(produced) == 86, len(produced)
     # 反向：采集器不该产出「不可提供」的指标
     extra = sorted(produced - providable_codes)
     assert not extra, '产出了客户指标库里不是「可提供」的指标：%s' % extra
diff --git a/tests/regression/test_fixture_integrity.py b/tests/regression/test_fixture_integrity.py
index a7fefce..14ac0b2 100644
--- a/tests/regression/test_fixture_integrity.py
+++ b/tests/regression/test_fixture_integrity.py
@@ -155,10 +155,10 @@ def test_non_b_expected_values_fixture_matches_the_generator():
     assert load_json(EXPECTED_NON_B_PATH) == build_regression_fixtures.build_non_b_table()
 
 
-def test_expected_values_table_covers_the_82_b_class_metrics(expected_table):
-    """B 类期望值表覆盖 82 条，一条不落。"""
+def test_expected_values_table_covers_the_71_b_class_metrics(expected_table):
+    """B 类期望值表覆盖 71 条，一条不落。"""
     expected = expected_table['expected']
-    assert len(expected) == 82
+    assert len(expected) == 71
 
 
 def test_non_b_expected_values_table_covers_the_16_other_metrics(
diff --git a/tests/test_expected_values.py b/tests/test_expected_values.py
index fe95683..36dae7e 100644
--- a/tests/test_expected_values.py
+++ b/tests/test_expected_values.py
@@ -1,4 +1,4 @@
-"""B 类（命令中心 SQL）82 个指标的**值相等**守卫。
+"""B 类（命令中心 SQL）71 个指标的**值相等**守卫。
 
 **这个文件守的是什么**：合并 SQL 返回形态下，取值口径错了**不报错**。
 列名对、聚合口径错（例如 ``aggregate`` 的 ``fn`` 写错、``raw`` 的 ``row_policy``
@@ -12,8 +12,8 @@
 （``tpops_kafka_collector/mapping/rules.py``），一边是独立推导的期望值。
 
 覆盖：
-- 82 条 B 类指标**逐条**断言「规则引擎输出 == 期望值表」（一条不落）；
-- 期望值表与 82 条 B 类指标**完全对应**（不多不少）；
+- 71 条 B 类指标**逐条**断言「规则引擎输出 == 期望值表」（一条不落）；
+- 期望值表与 71 条 B 类指标**完全对应**（不多不少）；
 - 期望值表与生成器输出**一致**（改了规则要重新生成）；
 - ``counter_delta`` 的 5 条：首轮无基线不产出值，第 2 轮差分速率 == 期望速率。
 """
@@ -36,7 +36,7 @@ EXPECTED_PATH = os.path.join(MAPPING_DIR, 'expected_values.json')
 PROBE_PATH = os.path.join(DATA_DIR, 'merged_sql', 'sql_merge_probe_result.json')
 
 B_CLASS = 'command_center_sql'
-EXPECTED_B_COUNT = 82
+EXPECTED_B_COUNT = 71
 COUNTER_VIEW = 'stat_database'
 
 
@@ -46,7 +46,7 @@ def _load(path):
 
 
 def _b_rules():
-    """82 条 B 类指标（channel == command_center_sql）的声明。"""
+    """71 条 B 类指标（channel == command_center_sql）的声明。"""
     payload = _load(METRIC_RULES_PATH)
     return [
         rule for rule in payload['rules']
@@ -109,13 +109,13 @@ def _expected_pairs(node_expected):
 
 # -- 期望值表本身的完整性 --------------------------------------------------
 
-def test_b_class_metric_count_is_82():
+def test_b_class_metric_count_is_71():
     assert len(B_RULES) == EXPECTED_B_COUNT
     assert len(B_CODES) == EXPECTED_B_COUNT
 
 
 def test_expected_table_covers_every_b_class_metric(expected_table):
-    """期望值表必须**一条不落**地覆盖 82 条 B 类指标。"""
+    """期望值表必须**一条不落**地覆盖 71 条 B 类指标。"""
     expected = expected_table['expected']
     assert len(expected) == EXPECTED_B_COUNT
     assert sorted(expected) == B_CODES
@@ -136,7 +136,7 @@ def test_expected_table_records_the_rule_type(expected_table):
         assert entry['sql_key']
 
 
-# -- 值相等：82 条逐条断言 ------------------------------------------------
+# -- 值相等：71 条逐条断言 ------------------------------------------------
 
 @pytest.mark.parametrize('metric_code', B_CODES)
 def test_rule_engine_output_equals_expected_values(
diff --git a/tests/test_generated_rules.py b/tests/test_generated_rules.py
index b157e98..deab298 100644
--- a/tests/test_generated_rules.py
+++ b/tests/test_generated_rules.py
@@ -14,7 +14,7 @@ import build_metric_value_rules as value_builder  # noqa: E402
 
 
 def test_metric_value_rules_are_stable():
-    """取值规则生成器可重复：两次生成结果一致，且覆盖 98 条可提供指标。
+    """取值规则生成器可重复：两次生成结果一致，且覆盖 87 条生效指标。
 
     `data/metric_value_rules.json` 是不入库的中间产物，由
     `tools/build_metric_value_rules.py` 从开发方案 §4.3 机械生成；
@@ -23,7 +23,7 @@ def test_metric_value_rules_are_stable():
     first = value_builder._build_metrics(value_builder._read_plan())
     second = value_builder._build_metrics(value_builder._read_plan())
     assert first == second
-    assert len(first) == 98
+    assert len(first) == 87
 
 
 def test_metric_rules_file_is_up_to_date():
@@ -43,7 +43,7 @@ def test_alarm_rules_file_is_up_to_date():
 
 
 def test_value_rules_cover_all_provided_metrics():
-    """开发方案 §4.3 的 98 条规则与可提供性判定一一对应。"""
+    """开发方案 §4.3 的规则（扣除排除清单）与可提供性判定一一对应。"""
     availability = json.load(
         io.open(os.path.join(DATA_DIR, 'customer_metric_mapping.json'), encoding='utf-8')
     )
diff --git a/tests/test_kafka_mapping.py b/tests/test_kafka_mapping.py
index 39901a0..5e2d879 100644
--- a/tests/test_kafka_mapping.py
+++ b/tests/test_kafka_mapping.py
@@ -149,14 +149,15 @@ def _resolve(source, parts):
 
 
 def _parse_all(expression):
-    """复合表达式（如比值 a ÷ b）拆成多组路径。"""
+    """复合表达式（如比值 a ÷ b、列拼接 a · b）拆成多组路径。"""
     if not _is_reference(expression):
         return []
     paths = []
     for side in expression.split(' ÷ '):
-        parts = _parse_path(side.strip())
-        if parts:
-            paths.append(parts)
+        for piece in side.split(' · '):
+            parts = _parse_path(piece.strip())
+            if parts:
+                paths.append(parts)
     return paths
 
 
@@ -192,7 +193,7 @@ def test_covers_all_enabled_metrics(payload):
     metric_rules = _load(METRIC_RULES_PATH)['rules']
     enabled = set(r['metric_code'] for r in metric_rules if r.get('enabled'))
     mapped = set(m['metric_code'] for m in payload['metric_mappings'])
-    assert len(payload['metric_mappings']) == 98
+    assert len(payload['metric_mappings']) == 87
     assert mapped == enabled
 
 
diff --git a/tests/test_mapping_table.py b/tests/test_mapping_table.py
index 9d01d22..a159bb5 100644
--- a/tests/test_mapping_table.py
+++ b/tests/test_mapping_table.py
@@ -23,9 +23,10 @@ def table():
     return MappingTable.load(MAPPING_DIR)
 
 
-def test_metric_rules_cover_all_101_customer_metrics(table):
-    assert len(table.metric_rules) == 101
-    assert len(table.enabled_metric_rules()) == 98
+def test_metric_rules_cover_the_effective_customer_metrics(table):
+    """生效口径 = 87 条（客户指标库 101 条 − 排除清单 14 条）。"""
+    assert len(table.metric_rules) == 87
+    assert len(table.enabled_metric_rules()) == 87
 
 
 def test_metric_rules_match_customer_xlsx_names(table):
@@ -106,7 +107,7 @@ def test_severity_and_status_maps_cover_platform_values(table):
 def test_metrics_of_sql_key_partition_command_center_metrics(table):
     """82 个命令中心指标应能被 11 条合并 SQL 全量覆盖，且互不重叠。"""
     command_center = table.metrics_of_channel('command_center_sql')
-    assert len(command_center) == 82
+    assert len(command_center) == 71
     covered = []
     for sql_key in (
         'stat_database', 'stat_user_tables', 'activity_agg', 'activity_running',
@@ -114,20 +115,23 @@ def test_metrics_of_sql_key_partition_command_center_metrics(table):
         'prepared_xacts', 'dbstat_sum',
     ):
         covered.extend(table.metrics_of_sql_key(sql_key))
-    assert len(covered) == 82
-    assert len(set(rule.metric_code for rule in covered)) == 82
+    assert len(covered) == 71
+    assert len(set(rule.metric_code for rule in covered)) == 71
 
 
-def test_unsupported_metrics_are_disabled_with_reason(table):
+def test_unsupported_metrics_are_excluded_not_disabled(table):
+    """3 条无法提供的指标**不在生效映射表里**（构建期就被排除了）。
+
+    它们改由排除清单记录（``reason=unsupported`` + 理由）——
+    生效映射表里不再有 ``enabled=false`` 的规则。
+    """
     unsupported = [
         rule for rule in table.metric_rules if not rule.enabled
     ]
-    assert len(unsupported) == 3
-    codes = set(rule.metric_code for rule in unsupported)
-    assert codes == {
+    assert not unsupported, '生效映射表里不该有 enabled=false 的规则'
+    codes = set(rule.metric_code for rule in table.metric_rules)
+    assert not codes & {
         'gaussdb.Fastpath_function_call',
         'gaussdb.Disabled_conn',
         'gaussdb.sendAgentName',
     }
-    for rule in unsupported:
-        assert rule.source.get('api') is None
diff --git a/tests/test_merged_sql_mapping.py b/tests/test_merged_sql_mapping.py
index 63011eb..941108e 100644
--- a/tests/test_merged_sql_mapping.py
+++ b/tests/test_merged_sql_mapping.py
@@ -276,7 +276,7 @@ def test_merged_views_paged_flags_come_from_merged_sql_paging(merged_views):
 
 
 def test_existing_shapes_and_rules_are_untouched():
-    """既有 11 个 cmd_rows_* 形状与 98 条规则一个字都不能变。"""
+    """既有 11 个 cmd_rows_* 形状与 87 条规则一个字都不能变。"""
     payload = _load(RULES_PATH)
     cmd_shapes = [
         (name, shape) for name, shape in payload['shapes'].items()
@@ -286,7 +286,7 @@ def test_existing_shapes_and_rules_are_untouched():
     for name, shape in cmd_shapes:
         assert shape['kind'] == 'rows_from_result', name
         assert shape['payload_path'] == ['result'], name
-    assert len(payload['rules']) == 98
+    assert len(payload['rules']) == 87
 
 
 # -- 既有 per-view 形态保持可解 -------------------------------------------
@@ -337,7 +337,7 @@ def test_package_per_view_path_replays_every_command_center_sample():
             continue
         assert len(ctx.rows) == expected, rule['metric_code']
         checked += 1
-    assert checked >= 80, checked
+    assert checked == 71, checked   # 71 条 B 类指标逐条回放
 
 
 # --- 重试判定（§5.7 重试表）------------------------------------------
diff --git a/tests/test_metric_response.py b/tests/test_metric_response.py
index e660fc4..8923994 100644
--- a/tests/test_metric_response.py
+++ b/tests/test_metric_response.py
@@ -41,11 +41,18 @@ def batches():
     return dict((b['sql_key'], b) for b in _load(MERGED_SQL_PATH)['batches'])
 
 
-def test_manifest_covers_all_provided_metrics(manifest, rules):
-    """98 个可提供指标各有一个文件；3 个无法提供的不建文件。"""
+def test_manifest_covers_all_effective_metrics(manifest, rules):
+    """每个**生效指标**都有一个样本文件。
+
+    样本目录是**抓取归档**（平台事实）：生效口径从 98 收敛到 87 之后，
+    被排除的 11 条重复指标的样本仍留在归档里 —— 归档是事实，生效口径是决定。
+    3 个无法提供的指标从来没建过文件。
+    """
     enabled = [code for code, r in rules.items() if r.get('enabled')]
-    assert manifest['metric_count'] == len(enabled) == 98
-    assert set(x['customer_metric'] for x in manifest['metrics']) == set(enabled)
+    manifest_codes = set(x['customer_metric'] for x in manifest['metrics'])
+    missing = sorted(set(enabled) - manifest_codes)
+    assert not missing, '生效指标没有样本文件：%s' % missing
+    assert manifest['metric_count'] == len(manifest['metrics']) == 98
 
 
 def test_every_manifest_entry_has_a_response_file(manifest):
@@ -171,7 +178,10 @@ def test_command_center_response_contains_metric_column(manifest, rules, batches
     for item in manifest['metrics']:
         if item['channel'] != '命令中心SQL':
             continue
-        rule = rules[item['customer_metric']]
+        rule = rules.get(item['customer_metric'])
+        if rule is None:
+            # 被排除清单剔除的指标（不在生效口径里）—— 不参与生效口径的核对
+            continue
         chain = _load(os.path.join(RESP_DIR, item['file']))
         rows = []
         for step in chain:
diff --git a/tests/test_response_rules.py b/tests/test_response_rules.py
index a3027b7..5a17870 100644
--- a/tests/test_response_rules.py
+++ b/tests/test_response_rules.py
@@ -82,10 +82,10 @@ def _resolve(rule, payload, now_ms=1760000000000):
 # -- 覆盖面 ---------------------------------------------------------------
 
 def test_covers_all_enabled_metrics(rules):
-    """98 个可提供指标各有一条规则；3 个无法提供的不建规则。"""
+    """87 个生效指标各有一条规则；被排除的指标不建规则。"""
     metric_rules = _load(os.path.join(DATA_DIR, 'mapping', 'metric_rules.json'))['rules']
     enabled = set(r['metric_code'] for r in metric_rules if r.get('enabled'))
-    assert len(rules) == 98
+    assert len(rules) == 87
     assert set(r['metric_code'] for r in rules) == enabled
 
 
diff --git a/tests/test_rules.py b/tests/test_rules.py
index 27482b7..750d694 100644
--- a/tests/test_rules.py
+++ b/tests/test_rules.py
@@ -392,8 +392,9 @@ def test_alarm_message_has_exactly_the_documented_fields(table):
     assert message['priority'] == 2
     assert message['status'] == 1
     assert message['recoverTime'] is None
-    assert message['metricCode'] == 'gaussdb.alarm.2007010001'
-    assert message['metricName'] == '节点NTP状态异常'
+    # 未挂客户指标的事件类告警 → **统一指标名**（表驱动，alarm_rules.json 的 event_metric 段）
+    assert message['metricCode'] == 'gaussdb.alarm'
+    assert message['metricName'] == '数据库告警'
     assert message['value'] == '2'
     assert message['resourceName'] == '172.16.103.151_node-1'
 
@@ -512,7 +513,6 @@ def test_ratio_metrics_are_per_row(table):
     """比率列是按行算出来的，带维度的必须逐行取。"""
     for code in (
         'GAUSSDB.[{#DATNAME}]_cache_hit_ratio',
-        'GAUSSDB.{#NAME}.garbage.Ratio',
         'gaussdb.{#NAME}.garbage.Ratio',
     ):
         rule = _rule(table, code)
diff --git a/tests/unit/test_alarm_metric_unification.py b/tests/unit/test_alarm_metric_unification.py
new file mode 100644
index 0000000..aab79cd
--- /dev/null
+++ b/tests/unit/test_alarm_metric_unification.py
@@ -0,0 +1,181 @@
+# -*- coding: utf-8 -*-
+"""告警的统一指标名。
+
+平台 257 条告警定义里，只有 3 条挂靠了客户指标（都指向 ``gaussdb.Tps``）；
+其余 254 条事件类告警原先各自造一个 ``gaussdb.alarm.<alarm_id>`` 指标名
+（其中还有 ``gaussdb.alarm.>=75%`` 这种拿阈值当 ID 的）——
+客户侧要建 254 条台账条目。
+
+本波次把**未挂客户指标**的告警统一成**同一个** ``metricCode`` / ``metricName``：
+
+- 统一值**表驱动** —— 写在 ``alarm_rules.json`` 的 ``event_metric`` 段里，
+  改口径只改数据文件，不改代码；
+- 挂靠了客户指标的告警**保持不变**；
+- 告警的种类由 ``alarmId`` / ``alarmName`` / ``alarmContent`` 承载，
+  不靠 ``metricCode`` 区分。
+"""
+from __future__ import absolute_import
+
+import io
+import json
+import os
+
+import pytest
+
+from conftest import REPO_ROOT
+
+from tpops_kafka_collector.mapping import loader  # noqa: E402
+from tpops_kafka_collector.mapping.table import (  # noqa: E402
+    build_event_alarm_definition,
+)
+
+ALARM_RULES_PATH = os.path.join(
+    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'alarm_rules.json')
+
+#: 挂靠客户指标的告警条数（都指向 gaussdb.Tps）。
+LINKED_COUNT = 3
+
+
+def _load(path):
+    with io.open(path, encoding='utf-8') as handle:
+        return json.load(handle)
+
+
+@pytest.fixture(scope='module')
+def payload():
+    return _load(ALARM_RULES_PATH)
+
+
+@pytest.fixture(scope='module')
+def definitions(payload):
+    return payload['definitions']
+
+
+@pytest.fixture(scope='module')
+def event_metric(payload):
+    return payload['event_metric']
+
+
+# -- 统一值本身 -----------------------------------------------------------
+def test_the_unified_value_is_declared_in_the_data_file(event_metric):
+    assert event_metric['metric_code']
+    assert event_metric['metric_name']
+
+
+def test_event_metric_is_a_single_fixed_value(event_metric):
+    assert event_metric['metric_code'] == 'gaussdb.alarm'
+    assert event_metric['metric_name'] == '数据库告警'
+
+
+# -- 事件类告警 ------------------------------------------------------------
+def test_all_event_alarms_share_the_same_metric_code(definitions, event_metric):
+    unlinked = [
+        entry for entry in definitions.values()
+        if not entry.get('linked_customer_metric')
+    ]
+    assert len(unlinked) == 254
+    codes = set(entry['metric_code'] for entry in unlinked)
+    assert codes == {event_metric['metric_code']}
+
+
+def test_all_event_alarms_share_the_same_metric_name(definitions, event_metric):
+    unlinked = [
+        entry for entry in definitions.values()
+        if not entry.get('linked_customer_metric')
+    ]
+    names = set(entry['metric_name'] for entry in unlinked)
+    assert names == {event_metric['metric_name']}
+
+
+def test_event_alarm_ids_are_all_different(definitions):
+    """告警种类靠 ``alarmId`` 承载 —— 254 条事件类告警的 ID 各不相同。"""
+    unlinked = [
+        alarm_id for alarm_id, entry in definitions.items()
+        if not entry.get('linked_customer_metric')
+    ]
+    assert len(unlinked) == 254
+    assert len(set(unlinked)) == len(unlinked)
+
+
+def test_alarm_names_are_untouched_by_the_unification(definitions):
+    """统一的是**指标名**；告警名（种类）不能被统一值覆盖。
+
+    平台目录里有 13 行「告警名称」列填的是等级词（``紧急`` / ``重要`` / ``次要``），
+    另有 2 组同名不同 ID 的告警 —— 那是**平台侧**的既有数据（见
+    ``test_alarm_names_match_the_platform_catalog``），统一指标名没有制造它们。
+    """
+    unlinked = [
+        entry for entry in definitions.values()
+        if not entry.get('linked_customer_metric')
+    ]
+    assert unlinked
+    for entry in unlinked:
+        assert entry['alarm_name'], entry['alarm_id']
+        assert entry['alarm_name'] != '数据库告警', entry['alarm_id']
+
+
+def test_alarm_names_match_the_platform_catalog(definitions):
+    """告警名逐条对得上平台目录 —— 统一指标名没有动过告警名。"""
+    catalog = {}
+    for filename in ('catalog_metric.json', 'catalog_event.json'):
+        payload = _load(os.path.join(REPO_ROOT, 'data', 'alarm', filename))
+        for item in payload.get('items') or ():
+            alarm_id = str(item.get('告警ID') or '').strip()
+            if alarm_id and alarm_id not in catalog:
+                catalog[alarm_id] = item.get('告警名称') or ''
+    for alarm_id, entry in definitions.items():
+        assert entry['alarm_name'] == catalog.get(alarm_id), alarm_id
+
+
+def test_no_event_alarm_uses_a_per_alarm_metric_code(definitions):
+    codes = [
+        entry['metric_code'] for entry in definitions.values()
+        if not entry.get('linked_customer_metric')
+    ]
+    assert not [code for code in codes if code.startswith('gaussdb.alarm.')]
+
+
+# -- 挂靠客户指标的告警保持不变 -------------------------------------------
+def test_linked_alarms_keep_their_customer_metric(definitions):
+    linked = [
+        entry for entry in definitions.values()
+        if entry.get('linked_customer_metric')
+    ]
+    assert len(linked) == LINKED_COUNT
+    for entry in linked:
+        assert entry['metric_code'] == entry['linked_customer_metric']
+        assert entry['metric_code'] == 'gaussdb.Tps'
+
+
+def test_linked_alarms_keep_their_own_metric_name(definitions):
+    linked = [
+        entry for entry in definitions.values()
+        if entry.get('linked_customer_metric')
+    ]
+    for entry in linked:
+        assert entry['metric_name'] != '数据库告警'
+        assert entry['metric_name']
+
+
+# -- 启动期校验不受影响 ----------------------------------------------------
+def test_the_real_catalog_still_passes_validation():
+    bundle = loader.load_mapping()
+    assert len(bundle.alarm_rules['definitions']) == 257
+
+
+def test_alarm_metric_codes_may_repeat_without_failing_validation():
+    """告警定义靠 ``alarm_id`` 定位 —— 254 条同码**不得**触发唯一性校验失败。"""
+    bundle = loader.load_mapping()
+    codes = [
+        entry['metric_code'] for entry in bundle.alarm_rules['definitions'].values()
+    ]
+    assert len(codes) > len(set(codes)), '本用例的前提是告警 metric_code 有重复'
+
+
+# -- 运行期兜底也用统一值 --------------------------------------------------
+def test_the_runtime_fallback_uses_the_mapped_unified_value(event_metric):
+    """平台新增告警（定义缺失）时，运行期兜底也用统一指标名，不是自造名。"""
+    definition = build_event_alarm_definition(
+        'new-alarm-1', '新告警', event_metric)
+    assert definition.metric_code == event_metric['metric_code']
+    assert definition.metric_name == event_metric['metric_name']
diff --git a/tests/unit/test_delivery_docs.py b/tests/unit/test_delivery_docs.py
index 07a6268..ef27838 100644
--- a/tests/unit/test_delivery_docs.py
+++ b/tests/unit/test_delivery_docs.py
@@ -3,8 +3,9 @@
 
 覆盖三份客户交付文档：
 
-  1. ``docs/交付-指标口径说明.md``          —— 覆盖全部 98 个可提供指标
-  2. ``docs/交付-不可提供指标清单.md``      —— 恰好 3 条，与 ``enabled=false`` 完全一致
+  1. ``docs/交付-指标口径说明.md``          —— 覆盖全部 87 个生效指标
+  2. ``docs/交付-不可提供指标清单.md``      —— 恰好 3 条，与排除清单的
+     ``reason=unsupported`` 完全一致
   3. ``docs/交付-客户指标英文名核对清单.md`` —— 每条都能在客户 xlsx 里找到对应行
 
 **防恒绿**：文件末尾的 ``test_guard_*`` 系列用「合成违规样本」验证守卫本身
@@ -40,23 +41,43 @@ REQUIRED_SPEC_COLUMNS = ('客户指标英文名', '中文名', '取数通道', '
 METRIC_CODE_RE = re.compile(r'`((?:gaussdb|GAUSSDB)[^`]*)`')
 
 
+EXCLUDED_PATH = os.path.join(
+    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'excluded_metrics.json')
+
+
 @pytest.fixture(scope='module')
 def rules():
-    """映射表的 101 条规则。"""
+    """映射表的 87 条生效规则。"""
     with open(METRIC_RULES_PATH, 'r', encoding='utf-8') as handle:
         return json.load(handle)['rules']
 
 
+@pytest.fixture(scope='module')
+def excluded():
+    """排除清单（14 条：11 条重复 + 3 条无法提供）。"""
+    with open(EXCLUDED_PATH, 'r', encoding='utf-8') as handle:
+        return json.load(handle)
+
+
 @pytest.fixture(scope='module')
 def enabled_codes(rules):
-    """98 个可提供指标的 metric_code。"""
+    """87 个生效指标的 metric_code。"""
     return set(rule['metric_code'] for rule in rules if rule.get('enabled'))
 
 
 @pytest.fixture(scope='module')
-def disabled_codes(rules):
-    """3 个不可提供指标的 metric_code。"""
-    return set(rule['metric_code'] for rule in rules if not rule.get('enabled'))
+def unsupported_codes(excluded):
+    """3 个无法提供指标的 metric_code（来自排除清单，不是映射表）。"""
+    return set(
+        entry['metric_code'] for entry in excluded['excluded']
+        if entry.get('reason') == 'unsupported'
+    )
+
+
+@pytest.fixture(scope='module')
+def scope(rules):
+    """客户指标库全量（生效口径 + 被排除的）—— OCR 检测器跑在它上面。"""
+    return builder.customer_scope_rules(rules, builder.load_customer_table())
 
 
 def _read(path):
@@ -138,7 +159,7 @@ def test_metric_spec_covers_all_enabled_metrics(enabled_codes):
     missing, extra = check_spec_coverage(text, enabled_codes)
     assert not missing, '口径说明缺少这些指标：%s' % '、'.join(missing)
     assert not extra, '口径说明里出现了不该有的指标：%s' % '、'.join(extra)
-    assert len(enabled_codes) == 98, '可提供指标数不是 98，映射表变了？'
+    assert len(enabled_codes) == 87, '生效指标数不是 87，映射表变了？'
 
 
 def test_metric_spec_rows_have_all_columns():
@@ -195,35 +216,40 @@ def test_metric_spec_value_rules_are_described(rules):
 # --------------------------------------------------------------------------
 # 2. 不可提供清单
 # --------------------------------------------------------------------------
-def test_unsupported_doc_lists_exactly_disabled_metrics(disabled_codes):
-    """不可提供清单 = 3 条，且与 metric_rules.json 里 enabled=false 的完全一致。"""
+def test_unsupported_doc_lists_exactly_the_unsupported_exclusions(unsupported_codes):
+    """不可提供清单 = 3 条，且与**排除清单**的 ``reason=unsupported`` 完全一致。
+
+    被排除的指标根本不在生效映射表里 —— 不能再去映射表里找「``enabled=false``
+    的规则」。
+    """
     text = _read(UNSUPPORTED_PATH)
-    missing, extra = check_unsupported_list(text, disabled_codes)
+    missing, extra = check_unsupported_list(text, unsupported_codes)
     assert not missing, '不可提供清单缺少：%s' % '、'.join(missing)
     assert not extra, '不可提供清单多出：%s' % '、'.join(extra)
-    assert len(disabled_codes) == 3, '不可提供指标数不是 3，映射表变了？'
+    assert len(unsupported_codes) == 3, '不可提供指标数不是 3，排除清单变了？'
 
 
-def test_unsupported_doc_explains_reason_and_workaround(rules):
-    """每条都要有「为什么给不了」和「能不能变通」，且理由来自映射表。"""
+def test_unsupported_doc_explains_reason_and_workaround(excluded):
+    """每条都要有「为什么给不了」和「能不能变通」，且理由来自排除清单。"""
     text = _read(UNSUPPORTED_PATH)
-    for rule in rules:
-        if rule.get('enabled'):
+    for entry in excluded['excluded']:
+        if entry.get('reason') != 'unsupported':
             continue
-        code = rule['metric_code']
+        code = entry['metric_code']
         assert code in text, '不可提供清单里没有 %s' % code
-        reason = rule.get('unsupported_reason') or rule.get('note') or ''
+        reason = entry.get('detail') or ''
         assert reason and reason in text, \
-            '不可提供清单没有引用映射表里的理由：%s' % code
+            '不可提供清单没有引用排除清单里的理由：%s' % code
         assert '**能不能变通**' in text, '不可提供清单没有写变通方案'
 
 
-def test_unsupported_doc_states_drop_count(rules):
-    """要写明这 3 条在采集时被丢弃并计数。"""
+def test_unsupported_doc_states_the_build_time_exclusion(excluded):
+    """要写明处置方式是**构建期排除**，运行期没有丢弃分支。"""
     text = _read(UNSUPPORTED_PATH)
     assert 'dropped_unsupported' in text
-    assert '丢弃并计数' in text
-    assert '= **3**' in text or '= 3' in text
+    assert '构建期' in text
+    assert '恒为 **0**' in text
+    assert '已从生效口径移除' in text or '从生效口径移除' in text
 
 
 # --------------------------------------------------------------------------
@@ -257,9 +283,9 @@ def test_ocr_checklist_has_suggestion_and_impact():
         assert '取值' in row['影响面'], '影响面要说明是否影响取值：%s' % row
 
 
-def test_detector_finds_known_ocr_typos(rules):
+def test_detector_finds_known_ocr_typos(scope):
     """自动检测器要能抓到已知的 OCR 错字。"""
-    findings = builder.detect_ocr_findings(rules)
+    findings = builder.detect_ocr_findings(scope)
     pairs = set((item['metric_code'], item['token']) for item in findings)
     assert ('GAUSSDB.Number_of_ansactions_mmitted_r_cond', 'ansactions') in pairs
     assert ('GAUSSDB.Number_of_ansactions_mmitted_r_cond', 'mmitted') in pairs
@@ -268,9 +294,13 @@ def test_detector_finds_known_ocr_typos(rules):
     assert ('GAUSSDB.Checkpoint_count_eckpoint_gments', 'gments') in pairs
 
 
-def test_ocr_review_entries_are_corroborated(rules):
-    """人工复核表里的每条，检测器都要能给出候选（复核表不能凭空调）"""
-    findings = builder.detect_ocr_findings(rules)
+def test_ocr_review_entries_are_corroborated(scope):
+    """人工复核表里的每条，检测器都要能给出候选（复核表不能凭空调）。
+
+    检测器跑在**客户指标库全量**上（生效口径 + 被排除的）—— 核对清单的对象
+    是客户的指标表。
+    """
+    findings = builder.detect_ocr_findings(scope)
     candidates = {}
     for item in findings:
         candidates.setdefault(item['metric_code'], set()).add(item['token'])
@@ -281,9 +311,14 @@ def test_ocr_review_entries_are_corroborated(rules):
                     '复核表里的 %s / %s 没有检测器佐证' % (code, bad)
 
 
-def test_other_docs_only_mention_known_metrics(rules):
-    """不可提供清单与 OCR 清单里出现的指标名也必须存在于映射表。"""
+def test_other_docs_only_mention_known_metrics(rules, excluded):
+    """不可提供清单与 OCR 清单里出现的指标名也必须已知。
+
+    「已知」= 生效映射表里的指标码 + **排除清单**里的指标码
+    （被排除的指标也在客户表里，OCR 清单仍会提到它们）。
+    """
     known = set(rule['metric_code'] for rule in rules)
+    known.update(entry['metric_code'] for entry in excluded['excluded'])
     for rule in rules:
         tpops_source = (rule.get('source') or {}).get('tpops_source')
         if tpops_source:
@@ -304,6 +339,39 @@ def test_other_docs_only_mention_known_metrics(rules):
                 '%s 里出现了映射表里没有的指标名：%s' % (os.path.basename(path), code)
 
 
+# --------------------------------------------------------------------------
+# 3.5 三份文档的条数与生效映射表一致（防文档与代码漂移）
+# --------------------------------------------------------------------------
+def test_doc_counts_match_the_effective_mapping(rules, unsupported_codes, excluded):
+    """三份交付文档的条数必须与**生效映射表**对得上。
+
+    - 口径说明：每个生效指标恰好一行；
+    - 不可提供清单：与排除清单里 ``reason=unsupported`` 的条数一致；
+    - OCR 核对清单：条目只针对客户指标库全量（生效口径 + 被排除的）。
+    """
+    effective = len(rules)
+    assert effective == 87, '生效指标数不是 87，映射表变了？'
+
+    spec_rows = builder.parse_metric_spec_rows(_read(SPEC_PATH))
+    assert len(spec_rows) == effective, \
+        '口径说明 %d 行 ≠ 生效指标 %d 条' % (len(spec_rows), effective)
+
+    unsupported_rows = builder.parse_unsupported_rows(_read(UNSUPPORTED_PATH))
+    assert len(unsupported_rows) == len(unsupported_codes) == 3, \
+        '不可提供清单 %d 行 ≠ 排除清单的 3 条' % len(unsupported_rows)
+
+    scope = builder.customer_scope_rules(rules, builder.load_customer_table())
+    assert len(scope) == effective + excluded['counts']['total'], \
+        '客户指标库全量条数不对：%d' % len(scope)
+    listed = set(
+        row['客户表里的英文名'].strip().strip('`')
+        for row in builder.parse_ocr_rows(_read(OCR_PATH))
+    )
+    scope_codes = set(rule['metric_code'] for rule in scope)
+    assert listed <= scope_codes, \
+        'OCR 清单里有客户指标库之外的指标名：%s' % sorted(listed - scope_codes)
+
+
 # --------------------------------------------------------------------------
 # 4. 文档与生成脚本同步（重跑输出不变）
 # --------------------------------------------------------------------------
@@ -365,13 +433,13 @@ def test_guard_detects_incomplete_row():
     assert problems, '守卫没抓到缺字段的行'
 
 
-def test_guard_detects_wrong_unsupported_list(disabled_codes):
+def test_guard_detects_wrong_unsupported_list(unsupported_codes):
     """不可提供清单里塞进一条可提供指标 → 守卫必须报「多余」。"""
     text = _read(UNSUPPORTED_PATH)
     broken = _append_table_row(
         text, builder.TABLE_MARKS['unsupported'],
         '| 999 | `gaussdb.Tps` | 假不可提供 | 假的 | 假的 | 假的 |')
-    missing, extra = check_unsupported_list(broken, disabled_codes)
+    missing, extra = check_unsupported_list(broken, unsupported_codes)
     assert 'gaussdb.Tps' in extra, '守卫没抓到多余的不可提供指标'
     assert not missing, '守卫误报了缺失'
 
diff --git a/tests/unit/test_dimension_binding.py b/tests/unit/test_dimension_binding.py
new file mode 100644
index 0000000..f396eb1
--- /dev/null
+++ b/tests/unit/test_dimension_binding.py
@@ -0,0 +1,235 @@
+# -*- coding: utf-8 -*-
+"""维度绑定：点号组合绑定与多列分组。
+
+表级指标的 ``{#NAME}`` 要绑成 ``schemaname.relname``（组合绑定），
+分组要同时按 ``schemaname`` 与 ``relname``（多列分组）——
+否则不同 schema 的同名表（``public.t1`` / ``other.t1``）会撞成同一个维度值，
+两条消息在幂等层撞键、后一条被**静默丢弃**。
+
+本文件守三件事：
+
+1. **组合绑定** —— ``{"NAME": "schemaname.relname"}`` 绑出 ``public.t1``；
+2. **多列分组** —— 按两列分组，``public.t1`` 与 ``other.t1`` 落进不同的组；
+3. **单列绑定不回归** —— 老口径（``{"NAME": "relname"}``）行为不变。
+"""
+from __future__ import absolute_import
+
+import io
+import json
+import os
+
+import pytest
+
+from conftest import REPO_ROOT
+
+from tpops_kafka_collector.ids import metric_idempotency_key  # noqa: E402
+from tpops_kafka_collector.mapping.message import build_metric_message  # noqa: E402
+from tpops_kafka_collector.mapping.rules_context import (  # noqa: E402
+    RuleContext,
+    RuleError,
+    grouped_items,
+    row_dimensions,
+)
+from tpops_kafka_collector.mapping.rules_engine import evaluate  # noqa: E402
+from tpops_kafka_collector.mapping.table import MetricRule  # noqa: E402
+
+FIELD_MAP_PATH = os.path.join(
+    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'itscada_field_map.json')
+
+#: 采集端的资源上下文（表级指标与节点无关 → 落到第一个 master CN）。
+RESOURCE_CONTEXT = {
+    'instance_id': 'inst-1',
+    'instance_name': 'gauss-6374',
+    'node_id': 'node-1',
+    'host_ip': '172.16.103.126',
+    'component_id': None,
+}
+
+
+def _rule(dimension_bindings, metric_code='gaussdb.{#NAME}.Number.lete.ples'):
+    return MetricRule.from_dict({
+        'metric_code': metric_code,
+        'metric_name': '({#NAME})被删除的行总数（包括已回收行）',
+        'resource_type': 'GAUSSDB实例',
+        'category': '表信息',
+        'value_type': '整数',
+        'status': '可提供',
+        'enabled': True,
+        'placeholders': list(dimension_bindings.keys()),
+        'dimension_bindings': dimension_bindings,
+    })
+
+
+# -- 组合绑定 -------------------------------------------------------------
+def test_dotted_binding_combines_the_column_values():
+    rule = _rule({'NAME': 'schemaname.relname'})
+    row = {'schemaname': 'public', 'relname': 't1'}
+    assert row_dimensions(rule, row) == {'NAME': 'public.t1'}
+
+
+def test_dotted_binding_keeps_the_schema_prefix():
+    rule = _rule({'NAME': 'schemaname.relname'})
+    row = {'schemaname': 'other', 'relname': 't1'}
+    assert row_dimensions(rule, row) == {'NAME': 'other.t1'}
+
+
+def test_dotted_binding_raises_when_a_column_is_missing():
+    """取不到列值 → 抛 ``RuleError``，不静默留 ``{#NAME}``。"""
+    rule = _rule({'NAME': 'schemaname.relname'})
+    row = {'schemaname': 'public'}
+    with pytest.raises(RuleError):
+        row_dimensions(rule, row)
+
+
+# -- 单列绑定不回归 -------------------------------------------------------
+def test_single_column_binding_is_unchanged():
+    rule = _rule({'NAME': 'relname'})
+    row = {'schemaname': 'public', 'relname': 't1'}
+    assert row_dimensions(rule, row) == {'NAME': 't1'}
+
+
+def test_single_column_binding_still_allows_missing_values():
+    """单列绑定的老行为：取不到就是 ``None``（不抛异常）。"""
+    rule = _rule({'NAME': 'relname'})
+    assert row_dimensions(rule, {}) == {'NAME': None}
+
+
+# -- 多列分组 -------------------------------------------------------------
+def _rows():
+    return [
+        {'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'},
+        {'schemaname': 'other', 'relname': 't1', 'n_tup_del': '5'},
+    ]
+
+
+def test_group_by_two_columns_splits_same_named_tables():
+    rule = _rule({'NAME': 'schemaname.relname'})
+    params = {'group_by': ['schemaname', 'relname']}
+    items = grouped_items(rule, params, _rows())
+    assert len(items) == 2
+    assert sorted(dims['NAME'] for dims, _row in items) == ['other.t1', 'public.t1']
+
+
+def test_group_by_two_columns_keeps_the_row_value():
+    rule = _rule({'NAME': 'schemaname.relname'})
+    params = {'group_by': ['schemaname', 'relname']}
+    items = grouped_items(rule, params, _rows())
+    values = dict((dims['NAME'], row['n_tup_del']) for dims, row in items)
+    assert values == {'public.t1': '3', 'other.t1': '5'}
+
+
+def test_group_by_must_cover_every_bound_column():
+    """分组键缺少绑定里的列 → ``RuleError``（声明自相矛盾）。"""
+    rule = _rule({'NAME': 'schemaname.relname'})
+    params = {'group_by': ['relname']}
+    with pytest.raises(RuleError):
+        grouped_items(rule, params, _rows())
+
+
+def test_single_column_group_by_still_works():
+    rule = _rule({'NAME': 'relname'})
+    params = {'group_by': ['relname']}
+    items = grouped_items(rule, params, [{'relname': 't1'}, {'relname': 't2'}])
+    assert sorted(dims['NAME'] for dims, _row in items) == ['t1', 't2']
+
+
+# -- 幂等不碰撞（任务 1.6）------------------------------------------------
+def _table_rule():
+    """一条真实的表级规则（来自生效映射表），带组合绑定。"""
+    path = os.path.join(
+        REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'metric_rules.json')
+    with io.open(path, encoding='utf-8') as handle:
+        payload = json.load(handle)
+    for rule in payload['rules']:
+        if rule.get('dimension_bindings') == {'NAME': 'schemaname.relname'}:
+            return MetricRule.from_dict(rule)
+    raise AssertionError('生效映射表里没有表级规则')
+
+
+def _field_map():
+    with io.open(FIELD_MAP_PATH, encoding='utf-8') as handle:
+        return json.load(handle)
+
+
+def _collect(rule, rows, now_ms=1700000000000):
+    """跑一次取值 → 出 [(维度, 值, 时间戳)]。"""
+    ctx = RuleContext(rows=tuple(rows), now_ms=now_ms)
+    return evaluate(rule, ctx)
+
+
+def _messages(rule, rows, now_ms=1700000000000):
+    """跑一次取值并组装成 ITSCADA 指标消息。"""
+    field_map = _field_map()
+    results = []
+    for dims, value, timestamp in _collect(rule, rows, now_ms):
+        message = build_metric_message(
+            field_map, rule, dims, value, RESOURCE_CONTEXT, timestamp)
+        results.append((dims, message, timestamp))
+    return results
+
+
+def test_same_named_tables_in_different_schemas_get_different_messages():
+    rule = _table_rule()
+    rows = [
+        {'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'},
+        {'schemaname': 'other', 'relname': 't1', 'n_tup_del': '5'},
+    ]
+    results = _messages(rule, rows)
+    assert len(results) == 2
+    names = set(message['metricName'] for _dims, message, _ts in results)
+    codes = set(message['metricCode'] for _dims, message, _ts in results)
+    assert len(names) == 2, '同名表不同 schema 的 metricName 撞了：%s' % names
+    assert len(codes) == 2, '同名表不同 schema 的 metricCode 撞了：%s' % codes
+
+
+def test_dimension_values_carry_the_schema_prefix():
+    rule = _table_rule()
+    rows = [
+        {'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'},
+        {'schemaname': 'other', 'relname': 't1', 'n_tup_del': '5'},
+    ]
+    results = _messages(rule, rows)
+    values = set(
+        message['tags']['dimension_values']['NAME']
+        for _dims, message, _ts in results
+    )
+    assert values == {'public.t1', 'other.t1'}
+
+
+def test_same_named_tables_in_different_schemas_get_different_idempotency_keys():
+    rule = _table_rule()
+    rows = [
+        {'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'},
+        {'schemaname': 'other', 'relname': 't1', 'n_tup_del': '5'},
+    ]
+    results = _messages(rule, rows)
+    keys = set(
+        metric_idempotency_key(
+            RESOURCE_CONTEXT['instance_id'], message['metricName'], dims, timestamp)
+        for dims, message, timestamp in results
+    )
+    assert len(keys) == 2, '同名表不同 schema 的幂等键撞了：%s' % keys
+
+
+def test_the_same_table_collected_twice_gets_the_same_idempotency_key():
+    """同一张表重复采集 → 幂等键相同（仍然去重，只投一条）。"""
+    rule = _table_rule()
+    rows = [{'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'}]
+    first = _messages(rule, rows)
+    second = _messages(rule, rows)
+    def _key(result):
+        return metric_idempotency_key(
+            RESOURCE_CONTEXT['instance_id'], result[1]['metricName'],
+            result[0], result[2])
+
+    assert _key(first[0]) == _key(second[0])
+
+
+def test_resource_name_does_not_contain_the_table_name():
+    """表名不是资源名的一部分（资源名只表示节点）。"""
+    rule = _table_rule()
+    rows = [{'schemaname': 'public', 'relname': 't1', 'n_tup_del': '3'}]
+    results = _messages(rule, rows)
+    resource_name = results[0][1]['resourceName']
+    assert 't1' not in resource_name
+    assert 'public' not in resource_name
diff --git a/tests/unit/test_excluded_metrics.py b/tests/unit/test_excluded_metrics.py
new file mode 100644
index 0000000..963deec
--- /dev/null
+++ b/tests/unit/test_excluded_metrics.py
@@ -0,0 +1,192 @@
+# -*- coding: utf-8 -*-
+"""排除清单（``excluded_metrics.json``）。
+
+守住四件事：
+
+1. **清单本身**：恰好 14 条（11 条 `duplicate` + 3 条 `unsupported`），
+   必填字段齐全，`duplicate` 必带 ``kept``、`unsupported` 不带 ``kept``；
+2. **两份副本一致**：包内（运行期真相源）与仓库根（同源副本）逐字节一致；
+3. **清单与生效口径互斥**（任务 1.2 起）：被排除的指标码一个都不在生效映射表里，
+   ``kept`` 指明的那一条**必须**在生效映射表里；
+4. **生效口径的口径**（任务 1.2 起）：87 条规则、0 条 ``enabled=false``、
+   ``metric_code`` 互不重复。
+"""
+from __future__ import absolute_import
+
+import io
+import json
+import os
+
+import pytest
+
+from conftest import REPO_ROOT
+
+PACKAGE_PATH = os.path.join(
+    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'excluded_metrics.json')
+REPO_COPY_PATH = os.path.join(
+    REPO_ROOT, 'data', 'mapping', 'excluded_metrics.json')
+METRIC_RULES_PATH = os.path.join(
+    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'metric_rules.json')
+
+#: 排除原因的合法取值。
+REASONS = ('duplicate', 'unsupported')
+
+#: 客户指标库里 11 组重复（英文名一条 vs 中文名一条）的「保留哪一条」。
+#: 逐条写死 —— 这份清单是**人工判定**的结果，不能由代码推导。
+EXPECTED_DUPLICATES = {
+    'GAUSSDB.{#NAME}.Number.lete.ples': 'gaussdb.{#NAME}.Number.lete.ples',
+    'GAUSSDB.{#NAME}.Number.ad.ples': 'gaussdb.{#NAME}.Number.ad.ples',
+    'GAUSSDB.{#NAME}.Number.t.date.ples': 'gaussdb.{#NAME}.Number.t.date.ples',
+    'GAUSSDB.{#NAME}.number.Of.index.scan': 'gaussdb.{#NAME}.number.Of.index.scan',
+    'GAUSSDB.{#NAME}.number.Of.quential.ples.ad':
+        'gaussdb.{#NAME}.number.Of.quential.ples.ad',
+    'GAUSSDB.{#NAME}.number.Of.dex.ples.ad': 'gaussdb.{#NAME}.number.Of.dex.ples.ad',
+    'GAUSSDB.{#NAME}.number.Of.insert.tuples': 'gaussdb.{#NAME}.number.Of.insert.tuples',
+    'GAUSSDB.{#NAME}.number.Of.live.tuples': 'gaussdb.{#NAME}.number.Of.live.tuples',
+    'GAUSSDB.{#NAME}.number.Of.update.tuples': 'gaussdb.{#NAME}.number.Of.update.tuples',
+    'GAUSSDB.{#NAME}.number.Of.sequential.scan': 'gaussdb.{#NAME}.number.Of.sequential.scan',
+    'GAUSSDB.{#NAME}.garbage.Ratio': 'gaussdb.{#NAME}.garbage.Ratio',
+}
+
+#: 3 条无法提供的指标（PG 侧没有对应的可查询来源）。
+EXPECTED_UNSUPPORTED = {
+    'gaussdb.Fastpath_function_call': '快路径函数调用的连接数',
+    'gaussdb.Disabled_conn': '禁用的连接数',
+    'gaussdb.sendAgentName': '发送设备名给Agent',
+}
+
+
+def _load(path):
+    with io.open(path, encoding='utf-8') as handle:
+        return json.load(handle)
+
+
+@pytest.fixture(scope='module')
+def excluded():
+    return _load(PACKAGE_PATH)
+
+
+@pytest.fixture(scope='module')
+def entries(excluded):
+    return excluded['excluded']
+
+
+@pytest.fixture(scope='module')
+def effective_codes():
+    payload = _load(METRIC_RULES_PATH)
+    return [rule['metric_code'] for rule in payload['rules']]
+
+
+# -- 清单本身 -------------------------------------------------------------
+def test_exclusion_list_has_exactly_fourteen_entries(entries):
+    assert len(entries) == 14
+
+
+def test_reason_distribution_is_eleven_duplicates_and_three_unsupported(entries):
+    counts = {}
+    for entry in entries:
+        counts[entry['reason']] = counts.get(entry['reason'], 0) + 1
+    assert counts == {'duplicate': 11, 'unsupported': 3}
+
+
+def test_every_entry_has_the_required_fields(entries):
+    for entry in entries:
+        assert entry.get('metric_code'), entry
+        assert entry.get('metric_name'), entry
+        assert entry.get('reason') in REASONS, entry
+        assert entry.get('detail'), entry
+
+
+def test_duplicate_entries_carry_kept_and_unsupported_entries_do_not(entries):
+    for entry in entries:
+        if entry['reason'] == 'duplicate':
+            assert entry.get('kept'), '重复项必须写明保留哪一条：%r' % entry
+        else:
+            assert 'kept' not in entry, '无法提供的指标没有「保留项」：%r' % entry
+
+
+def test_metric_codes_are_unique(entries):
+    codes = [entry['metric_code'] for entry in entries]
+    assert len(codes) == len(set(codes))
+
+
+def test_duplicate_entries_match_the_known_eleven_pairs(entries):
+    actual = dict(
+        (entry['metric_code'], entry['kept'])
+        for entry in entries if entry['reason'] == 'duplicate'
+    )
+    assert actual == EXPECTED_DUPLICATES
+
+
+def test_unsupported_entries_match_the_known_three(entries):
+    actual = dict(
+        (entry['metric_code'], entry['metric_name'])
+        for entry in entries if entry['reason'] == 'unsupported'
+    )
+    assert actual == EXPECTED_UNSUPPORTED
+
+
+def test_counts_section_matches_the_entries(excluded, entries):
+    assert excluded['counts']['total'] == len(entries)
+    assert excluded['counts']['duplicate'] == 11
+    assert excluded['counts']['unsupported'] == 3
+
+
+def test_counts_pin_the_effective_catalog_size(excluded):
+    """排除条数与生效条数是**写死在一处**的口径：客户库 101 − 排除 14 = 生效 87。"""
+    counts = excluded['counts']
+    assert counts['client_total'] == 101
+    assert counts['client_total'] - counts['total'] == counts['effective_total']
+    assert counts['effective_total'] == 87
+
+
+# -- 两份副本一致 ---------------------------------------------------------
+def test_package_and_repo_copies_are_byte_identical():
+    with open(PACKAGE_PATH, 'rb') as handle:
+        package = handle.read()
+    with open(REPO_COPY_PATH, 'rb') as handle:
+        repo_copy = handle.read()
+    assert package == repo_copy, '包内副本与仓库根同源副本不一致'
+
+
+# -- 清单与生效口径互斥（任务 1.2）----------------------------------------
+def test_no_excluded_code_is_in_the_effective_mapping(entries, effective_codes):
+    excluded_codes = set(entry['metric_code'] for entry in entries)
+    leaked = sorted(excluded_codes.intersection(effective_codes))
+    assert not leaked, '被排除的指标码仍出现在生效映射表里：%s' % leaked
+
+
+def test_kept_codes_are_in_the_effective_mapping(entries, effective_codes):
+    for entry in entries:
+        if entry['reason'] != 'duplicate':
+            continue
+        assert entry['kept'] in effective_codes, \
+            '保留项 %r 不在生效映射表里' % entry['kept']
+
+
+def test_kept_codes_are_not_excluded_themselves(entries):
+    excluded_codes = set(entry['metric_code'] for entry in entries)
+    for entry in entries:
+        if entry['reason'] == 'duplicate':
+            assert entry['kept'] not in excluded_codes, entry
+
+
+# -- 生效口径（任务 1.2）--------------------------------------------------
+def test_effective_mapping_has_exactly_eighty_seven_rules(effective_codes):
+    assert len(effective_codes) == 87
+
+
+def test_effective_mapping_has_no_disabled_rule():
+    payload = _load(METRIC_RULES_PATH)
+    disabled = [
+        rule['metric_code'] for rule in payload['rules'] if not rule.get('enabled')
+    ]
+    assert not disabled, '生效口径里不该有 enabled=false 的规则：%s' % disabled
+
+
+def test_effective_metric_codes_are_unique(effective_codes):
+    duplicates = sorted(
+        code for code in set(effective_codes)
+        if effective_codes.count(code) > 1
+    )
+    assert not duplicates, '生效口径的 metric_code 有重复：%s' % duplicates
diff --git a/tests/unit/test_mapping_columns_gaps.py b/tests/unit/test_mapping_columns_gaps.py
index 300fa68..c2946b6 100644
--- a/tests/unit/test_mapping_columns_gaps.py
+++ b/tests/unit/test_mapping_columns_gaps.py
@@ -475,6 +475,6 @@ def test_mapping_table_load_reports_broken_json(mapping_sandbox):
 
 def test_mapping_table_load_reads_the_real_tables(mapping_sandbox):
     table = MappingTable.load(mapping_sandbox, validate_columns=False)
-    assert len(table.metric_rules) == 101
+    assert len(table.metric_rules) == 87
     assert table.metric_rule('gaussdb.Tps') is not None
     assert table.metrics_of_sql_key('activity_agg')
diff --git a/tests/unit/test_mapping_data_sync.py b/tests/unit/test_mapping_data_sync.py
index bb89ded..b64c9d4 100644
--- a/tests/unit/test_mapping_data_sync.py
+++ b/tests/unit/test_mapping_data_sync.py
@@ -32,6 +32,7 @@ PACKAGE_DATA_DIR = os.path.join(
 #: 包内副本文件名 → 仓库根的声明源（相对 REPO_ROOT）。
 COPIED_FILES = {
     'metric_rules.json': os.path.join('data', 'mapping', 'metric_rules.json'),
+    'excluded_metrics.json': os.path.join('data', 'mapping', 'excluded_metrics.json'),
     'alarm_rules.json': os.path.join('data', 'mapping', 'alarm_rules.json'),
     'itscada_field_map.json': os.path.join(
         'data', 'mapping', 'itscada_field_map.json'),
diff --git a/tests/unit/test_mapping_loader.py b/tests/unit/test_mapping_loader.py
index 48fa317..f19fbb2 100644
--- a/tests/unit/test_mapping_loader.py
+++ b/tests/unit/test_mapping_loader.py
@@ -35,6 +35,7 @@ DATA_DIR = os.path.join(PACKAGE_DIR, 'mapping', 'data')
 # 它是从留档生成的，不是仓库文件的副本。
 COPIED_FILES = {
     'metric_rules.json': os.path.join('data', 'mapping', 'metric_rules.json'),
+    'excluded_metrics.json': os.path.join('data', 'mapping', 'excluded_metrics.json'),
     'alarm_rules.json': os.path.join('data', 'mapping', 'alarm_rules.json'),
     'itscada_field_map.json': os.path.join('data', 'mapping', 'itscada_field_map.json'),
     'kafka_field_mapping.json': os.path.join('data', 'mapping', 'kafka_field_mapping.json'),
@@ -73,8 +74,8 @@ def _mutate(sandbox, filename, mutator):
 # -- 加载 -----------------------------------------------------------------
 def test_load_mapping_reads_the_real_tables():
     bundle = loader.load_mapping()
-    assert len(bundle.metric_rules) == 101
-    assert len(bundle.customer_metrics) == 101
+    assert len(bundle.metric_rules) == 87
+    assert len(bundle.customer_metrics) == 87
     assert len(bundle.alarm_rules['definitions']) == 257
     assert len(bundle.observed.merged_views) == 11
 
diff --git a/tests/unit/test_metric_catalog.py b/tests/unit/test_metric_catalog.py
new file mode 100644
index 0000000..5a6f6b7
--- /dev/null
+++ b/tests/unit/test_metric_catalog.py
@@ -0,0 +1,173 @@
+# -*- coding: utf-8 -*-
+"""生效口径的三道启动期校验（排除清单 / 唯一性 / 条数）。
+
+守住的口径（``changes/tpops-mapping-and-logging/specs/mapping-and-rules/spec.md``）：
+
+- **排除清单的指标码一个都不在生效映射表里** —— 排除是**构建期**做的；
+- **生效规则的 ``metric_code`` 互不重复**；
+- **生效规则数**与排除清单声明的生效条数一致（客户指标库 101 − 排除 14 = 87）。
+
+每道校验都要有**正例**与**反例**：反例是人为把生效映射表改坏，
+校验必须失败并把差异打印出来（不满足即启动失败）。
+"""
+from __future__ import absolute_import
+
+import io
+import json
+import os
+import shutil
+
+import pytest
+
+from conftest import REPO_ROOT
+
+from tpops_kafka_collector.errors import MappingError  # noqa: E402
+from tpops_kafka_collector.mapping import loader  # noqa: E402
+
+PACKAGE_DATA_DIR = os.path.join(
+    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data')
+
+
+def _read(path):
+    with io.open(path, encoding='utf-8') as handle:
+        return handle.read()
+
+
+def _write(path, payload):
+    with io.open(path, 'w', encoding='utf-8') as handle:
+        handle.write(json.dumps(payload, ensure_ascii=False, indent=2) + '\n')
+
+
+@pytest.fixture()
+def sandbox(tmp_path):
+    """包内映射数据的一份可改副本。"""
+    target = tmp_path / 'mapping_data'
+    shutil.copytree(PACKAGE_DATA_DIR, str(target))
+    return str(target)
+
+
+def _mutate_rules(sandbox, mutator):
+    path = os.path.join(sandbox, 'metric_rules.json')
+    payload = json.loads(_read(path))
+    mutator(payload)
+    _write(path, payload)
+    return path
+
+
+def _problems(sandbox):
+    """跑校验并返回问题清单（不抛异常）。"""
+    try:
+        loader.load_mapping(data_dir=sandbox)
+    except MappingError as exc:
+        return str(exc)
+    return ''
+
+
+# -- 正例 -----------------------------------------------------------------
+def test_the_real_catalog_passes_validation():
+    bundle = loader.load_mapping()
+    assert len(bundle.metric_rules) == 87
+    assert len(bundle.excluded_metrics) == 14
+
+
+def test_excluded_metrics_are_loaded_from_the_data_file():
+    bundle = loader.load_mapping()
+    codes = set(item.metric_code for item in bundle.excluded_metrics)
+    assert 'GAUSSDB.{#NAME}.garbage.Ratio' in codes
+    assert bundle.excluded_counts['effective_total'] == 87
+
+
+# -- 反例 1：被排除的码出现在生效映射表里 ---------------------------------
+def test_an_excluded_code_in_the_effective_mapping_fails_validation(sandbox):
+    def _inject(payload):
+        rule = dict(payload['rules'][0])
+        rule['metric_code'] = 'GAUSSDB.{#NAME}.garbage.Ratio'
+        rule['metric_name'] = '({#NAME})Garbage ratio'
+        payload['rules'].append(rule)
+
+    _mutate_rules(sandbox, _inject)
+    message = _problems(sandbox)
+    assert message, '塞进一条被排除的指标码，校验居然通过了'
+    assert 'GAUSSDB.{#NAME}.garbage.Ratio' in message
+    assert '排除清单' in message
+
+
+# -- 反例 2：metric_code 重复 ---------------------------------------------
+def test_a_duplicated_metric_code_fails_validation(sandbox):
+    def _duplicate(payload):
+        payload['rules'].append(dict(payload['rules'][0]))
+
+    _mutate_rules(sandbox, _duplicate)
+    message = _problems(sandbox)
+    assert message, '复制一条规则，校验居然通过了'
+    assert 'metric_code' in message
+    assert '重复' in message
+
+
+# -- 反例 3：生效条数不对 ---------------------------------------------------
+def test_a_wrong_rule_count_fails_validation(sandbox):
+    def _drop(payload):
+        payload['rules'] = payload['rules'][:-1]
+
+    _mutate_rules(sandbox, _drop)
+    message = _problems(sandbox)
+    assert message, '少一条规则，校验居然通过了'
+    assert '生效指标规则条数不对' in message
+    assert '87' in message and '86' in message
+
+
+# -- 告警定义不受唯一性约束 -----------------------------------------------
+def test_alarm_definitions_do_not_have_to_have_unique_metric_codes(sandbox):
+    """告警定义靠 ``alarm_id`` 定位 —— 多条告警共用一个 ``metricCode`` 是允许的。
+
+    事件类告警统一指标名之后，254 条告警的 ``metric_code`` 相同 ——
+    唯一性校验**只对指标规则**要求，告警侧不得因此报错。
+    """
+    path = os.path.join(sandbox, 'alarm_rules.json')
+    payload = json.loads(_read(path))
+    definitions = payload.get('definitions') or {}
+    assert len(definitions) >= 2
+    shared = 'gaussdb.alarm'
+    for entry in definitions.values():
+        entry['metric_code'] = shared
+    _write(path, payload)
+
+    bundle = loader.load_mapping(data_dir=sandbox)
+    codes = set(
+        definition['metric_code'] for definition in definitions.values()
+    )
+    assert codes == {shared}
+    assert len(bundle.metric_rules) == 87
+
+
+# -- 表级规则一律按两列分组（任务 1.5）------------------------------------
+def _table_rules():
+    payload = json.loads(_read(
+        os.path.join(PACKAGE_DATA_DIR, 'metric_rules.json')))
+    return [
+        rule for rule in payload['rules']
+        if 'NAME' in (rule.get('placeholders') or [])
+    ]
+
+
+def test_table_rules_bind_the_name_to_schema_and_relname():
+    table_rules = _table_rules()
+    assert len(table_rules) == 11, '表级规则数不对：%d' % len(table_rules)
+    for rule in table_rules:
+        assert rule['dimension_bindings'] == {'NAME': 'schemaname.relname'}, rule['metric_code']
+
+
+def test_table_rules_group_by_both_columns():
+    for rule in _table_rules():
+        params = (rule.get('value_rule') or {}).get('params') or {}
+        assert params.get('group_by') == ['schemaname', 'relname'], rule['metric_code']
+
+
+def test_no_rule_binds_a_dotted_column_as_a_single_column():
+    """点号串只出现在维度绑定里，``value_rule.params.group_by`` 必须是拆开的列。"""
+    payload = json.loads(_read(
+        os.path.join(PACKAGE_DATA_DIR, 'metric_rules.json')))
+    for rule in payload['rules']:
+        params = (rule.get('value_rule') or {}).get('params') or {}
+        for column in params.get('group_by') or ():
+            assert '.' not in column, (rule['metric_code'], column)
diff --git a/tests/unit/test_scheduler.py b/tests/unit/test_scheduler.py
index 5294904..88cd8f2 100644
--- a/tests/unit/test_scheduler.py
+++ b/tests/unit/test_scheduler.py
@@ -287,12 +287,15 @@ def test_command_center_sql_tiers_come_from_the_mapping_table(mapping):
     plans = compute_channel_plans(mapping)
     plan = dict((item.channel, item) for item in plans)[CHANNEL_COMMAND_CENTER]
     assert plan.tiers == (60, 120, 300, 86400)
-    assert plan.metric_count == 82
+    assert plan.metric_count == 71
 
 
-def test_unsupported_metric_count_is_three(mapping):
-    """不可提供指标 3 条 —— 计数上报（``dropped_unsupported``）。"""
-    assert count_unsupported_metrics(mapping) == 3
+def test_unsupported_metric_count_is_zero(mapping):
+    """不可提供指标已在**构建期**排除 —— 计数上报恒为 0（``dropped_unsupported``）。
+
+    运行期没有「遍历到再丢弃」的分支：被排除的指标码根本不在生效映射表里。
+    """
+    assert count_unsupported_metrics(mapping) == 0
 
 
 def test_schedule_entry_is_immutable():
diff --git a/tools/build_customer_metric_mapping.py b/tools/build_customer_metric_mapping.py
index b03b56c..591208f 100644
--- a/tools/build_customer_metric_mapping.py
+++ b/tools/build_customer_metric_mapping.py
@@ -29,6 +29,7 @@ import json
 import os
 import sys
 
+import excluded_metrics
 import xlsx_columns
 
 ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
@@ -462,6 +463,7 @@ def check_against_excel(rows, headers):
 def build(client_rows, headers):
     items = []
     col = _col(headers)
+    excluded = excluded_metrics.excluded_codes()
     for row, entry in zip(client_rows, MAPPING):
         expected_cn, tpops_metric, match_level, note, source = entry
         tpops_name = None
@@ -484,6 +486,9 @@ def build(client_rows, headers):
             "match_level": match_level,
             "note": note,
         })
+    # 排除发生在构建期：被排除清单剔除的指标不进生效口径。
+    # 客户 Excel 原件不动 —— 排除只落在排除清单与本判定结果里。
+    items = [it for it in items if it["customer_metric"] not in excluded]
     return items
 
 
diff --git a/tools/build_delivery_docs.py b/tools/build_delivery_docs.py
index a13f02e..0031aec 100644
--- a/tools/build_delivery_docs.py
+++ b/tools/build_delivery_docs.py
@@ -13,7 +13,8 @@
 
 脚本**幂等**：同样的输入必然产出同样的文件（重跑 ``git diff`` 为空）。
 所有取值口径来自 ``tpops_kafka_collector/mapping/data/metric_rules.json``
-（101 条：98 可提供 + 3 不可提供），这是唯一的真相源。
+（生效口径 87 条 = 客户指标库 101 条 − 排除清单 14 条；排除清单见
+``mapping/data/excluded_metrics.json``），这是唯一的真相源。
 
 Python 3.7.9 兼容（见 CLAUDE.md「Python 3.7.9 红线」）。
 """
@@ -25,7 +26,7 @@ import os
 import re
 import sys
 from collections import OrderedDict
-from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
+from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
 
 REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
 MAPPING_DATA = os.path.join(REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data')
@@ -36,6 +37,7 @@ XLSX_PATH = os.path.join(
 
 METRIC_RULES_PATH = os.path.join(MAPPING_DATA, 'metric_rules.json')
 MERGED_SQL_PATH = os.path.join(MAPPING_DATA, 'merged_sql.json')
+EXCLUDED_PATH = os.path.join(MAPPING_DATA, 'excluded_metrics.json')
 
 SPEC_DOC = os.path.join(DOCS_DIR, '交付-指标口径说明.md')
 UNSUPPORTED_DOC = os.path.join(DOCS_DIR, '交付-不可提供指标清单.md')
@@ -117,10 +119,71 @@ def load_json(path: str) -> Any:
 
 
 def load_metric_rules() -> Dict[str, Any]:
-    """读映射表（101 条指标规则）。"""
+    """读映射表（生效口径的指标规则）。"""
     return load_json(METRIC_RULES_PATH)
 
 
+def load_excluded_metrics() -> Dict[str, Any]:
+    """读排除清单（被排除的客户指标、理由与口径条数）。"""
+    return load_json(EXCLUDED_PATH)
+
+
+def customer_scope_rules(
+    rules: Sequence[Dict[str, Any]],
+    customer_rows: Sequence[Dict[str, Any]],
+) -> List[Dict[str, Any]]:
+    """客户指标库的**全部**指标（生效口径 + 被排除的那些）。
+
+    OCR 核对清单的对象是**客户指标库**（101 个指标）—— 被排除的 14 条也在
+    客户表里，英文名同样可能有 OCR 错字。所以检测器跑在「全量」上，
+    而不是只跑生效口径。
+    """
+    scope = list(rules)
+    known = set(rule['metric_code'] for rule in rules)
+    by_code = dict((row['metric_code'], row) for row in customer_rows)
+    for entry in load_excluded_metrics().get('excluded') or ():
+        code = entry.get('metric_code')
+        if not code or code in known:
+            continue
+        row = by_code.get(code) or {}
+        scope.append({
+            'metric_code': code,
+            'metric_name': entry.get('metric_name') or row.get('metric_name') or '',
+        })
+    return scope
+
+
+def unsupported_items(
+    excluded: Sequence[Dict[str, Any]],
+    customer_rows: Sequence[Dict[str, Any]],
+) -> List[Dict[str, Any]]:
+    """排除清单里「无法提供」的条目 → 与指标规则同形的条目。
+
+    排除清单只有「指标码 / 中文名 / 理由」三样；清单表还要展示分类、
+    数据类型、采集周期 —— 那些从**客户指标库 xlsx**（客户原件，只读）取，
+    不在数据文件里重复一份。
+    """
+    by_code = dict((row['metric_code'], row) for row in customer_rows)
+    items = []  # type: List[Dict[str, Any]]
+    for entry in excluded:
+        if entry.get('reason') != 'unsupported':
+            continue
+        code = entry.get('metric_code') or ''
+        row = by_code.get(code) or {}
+        detail = entry.get('detail') or ''
+        items.append({
+            'metric_code': code,
+            'metric_name': entry.get('metric_name') or row.get('metric_name') or '',
+            'category': row.get('category') or '',
+            'value_type': row.get('value_type') or '',
+            'unit': row.get('unit') or '',
+            'collect_cycle': row.get('collect_cycle') or {},
+            'unsupported_reason': detail,
+            'note': detail,
+        })
+    return items
+
+
 def load_merged_sql() -> Dict[str, Dict[str, Any]]:
     """读合并 SQL 目录 → ``{sql_key: 视图信息}``。"""
     data = load_json(MERGED_SQL_PATH)
@@ -147,14 +210,28 @@ def load_customer_table() -> List[Dict[str, Any]]:
     mapping = column_map(rows[0])
     name_col = require(mapping, '英文名')
     cn_col = require(mapping, '中文名')
+    cat_col = mapping.get('指标分类')
+    type_col = mapping.get('数据类型')
+    unit_col = mapping.get('指标单位')
+    cycle_col = mapping.get('数据更新周期')
+    cycle_unit_col = mapping.get('采集更新周期单位')
     result = []
     for row in rows[1:]:
         code = row[name_col]
         if not code:
             continue
+        cycle = row[cycle_col] if cycle_col is not None else None
         result.append({
             'metric_code': str(code).strip(),
             'metric_name': str(row[cn_col] or '').strip(),
+            'category': str(row[cat_col] or '').strip() if cat_col is not None else '',
+            'value_type': str(row[type_col] or '').strip() if type_col is not None else '',
+            'unit': str(row[unit_col] or '').strip() if unit_col is not None else '',
+            'collect_cycle': {
+                'value': int(cycle) if isinstance(cycle, (int, float)) else cycle,
+                'unit': (str(row[cycle_unit_col] or '').strip()
+                         if cycle_unit_col is not None else ''),
+            },
         })
     workbook.close()
     return result
@@ -487,21 +564,31 @@ def _group_by_channel(usable):
 
 
 def _spec_intro(lines: List[str], rules: Sequence[Dict[str, Any]],
-                usable: Sequence[Dict[str, Any]]) -> None:
+                usable: Sequence[Dict[str, Any]],
+                counts: Mapping[str, Any] = None) -> None:
     """文档开头：这份文档是什么、各列怎么读。"""
     lines.append('# GaussDB 指标口径说明（客户交付版）')
     lines.append('')
     lines.append('> 本文档回答客户最关心的问题：**这 %d 个指标，各自是从哪拿的、怎么算的、什么口径。**'
                  % len(usable))
     lines.append('>')
-    lines.append('> - 数据来源：`tpops_kafka_collector/mapping/data/metric_rules.json`（101 条指标规则，其中 98 条可提供）')
+    lines.append('> - 数据来源：`tpops_kafka_collector/mapping/data/metric_rules.json`'
+                 '（%d 条指标规则，全部可提供）' % len(rules))
     lines.append('> - 客户指标基准：`高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx`「指标库」sheet')
     lines.append('> - 生成方式：`python tools/build_delivery_docs.py`（表驱动，重跑输出不变）')
     lines.append('> - 配套文档：《交付-不可提供指标清单.md》《交付-客户指标英文名核对清单.md》')
     lines.append('')
+    counts = counts or {}
+    client_total = counts.get('client_total') or len(rules)
+    excluded_total = counts.get('total') or (client_total - len(usable))
+    duplicate_total = counts.get('duplicate') or 0
+    unsupported_total = counts.get('unsupported') or 0
     lines.append('客户指标库共 %d 个指标：**%d 个可提供**（本文档逐条说明口径），'
-                 '**%d 个不可提供**（见《交付-不可提供指标清单.md》，采集时丢弃并计数）。' % (
-                     len(rules), len(usable), len(rules) - len(usable)))
+                 '**%d 个不在生效口径内**（%d 条重复的英文变体 + %d 个无法提供 —— '
+                 '已在**构建期**从生效口径移除，运行期不产出消息；'
+                 '见《交付-不可提供指标清单.md》与 `excluded_metrics.json`）。' % (
+                     client_total, len(usable), excluded_total,
+                     duplicate_total, unsupported_total))
     lines.append('')
     lines.append('## 1. 怎么读这份表')
     lines.append('')
@@ -536,7 +623,8 @@ def _spec_channel_overview(lines: List[str], by_channel: OrderedDict,
     lines.append('|---|---|---|---|---|')
     channel_desc = {
         '命令中心SQL': ('`POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}`',
-                    '在 master 节点上执行合并 SQL（11 条合并 SQL 覆盖 82 个指标），返回**当前快照**'),
+                    '在 master 节点上执行合并 SQL（11 条合并 SQL 覆盖 %d 个指标），返回**当前快照**'
+                    % len(by_channel.get('命令中心SQL') or ())),
         '监控指标接口': ('`POST /rds/v3/monitor/metrics`',
                     '按时间窗口取**时序**数据，平台侧已是速率口径的指标直接取点'),
         '诊断优化接口': ('`POST /rds/gaussdb/v3/instances/{instance_id}/slow-sql-list`'
@@ -855,7 +943,8 @@ def _spec_tail(lines: List[str]) -> None:
     lines.append('')
 
 
-def render_metric_spec(rules: Sequence[Dict[str, Any]], views: Dict[str, Dict[str, Any]]) -> str:
+def render_metric_spec(rules: Sequence[Dict[str, Any]], views: Dict[str, Dict[str, Any]],
+                       counts: Mapping[str, Any] = None) -> str:
     """生成《交付-指标口径说明.md》。"""
     usable = enabled_rules(rules)
     by_channel = _group_by_channel(usable)
@@ -870,7 +959,7 @@ def render_metric_spec(rules: Sequence[Dict[str, Any]], views: Dict[str, Dict[st
         if channel and channel in channel_periods:
             method_to_period[method] = channel_periods[channel]
     lines = []
-    _spec_intro(lines, rules, usable)
+    _spec_intro(lines, rules, usable, counts)
     _spec_channel_overview(lines, by_channel, channel_periods, method_to_period)
     _spec_granularity(lines, usable)
     _spec_node_scoped(lines, usable, views)
@@ -914,23 +1003,30 @@ UNSUPPORTED_WORKAROUND = {
 }
 
 
-def _unsupported_intro(lines, items):
+def _unsupported_intro(lines, items, counts):
     """开头：这份清单是什么、3 条指标怎么处置。"""
+    client_total = counts.get('client_total')
+    effective_total = counts.get('effective_total')
     lines.append('# 不可提供指标清单（客户交付版）')
     lines.append('')
-    lines.append('客户指标库共 101 个指标，其中 **98 个可提供**（口径见《交付-指标口径说明.md》），'
-                 '**3 个无法提供**。本文档逐条说明：客户要什么、我们为什么给不了、能不能变通。')
+    lines.append('客户指标库共 %s 个指标，其中 **%s 个可提供**（口径见《交付-指标口径说明.md》），'
+                 '**%d 个无法提供**。本文档逐条说明：客户要什么、我们为什么给不了、能不能变通。' % (
+                     client_total, effective_total, len(items)))
     lines.append('')
-    lines.append('> - 数据来源：`tpops_kafka_collector/mapping/data/metric_rules.json`'
-                 '（`enabled = false` 的 3 条规则，每条自带 `unsupported_reason`）')
+    lines.append('> - 数据来源：`tpops_kafka_collector/mapping/data/excluded_metrics.json`'
+                 '（`reason = unsupported` 的 %d 条，每条自带排除理由）' % len(items))
     lines.append('> - 生成方式：`python tools/build_delivery_docs.py`（表驱动，重跑输出不变）')
     lines.append('')
     lines.append('## 1. 处置方式')
     lines.append('')
-    lines.append('- 这 3 个指标**不会出现在 Kafka 消息里**。')
-    lines.append('- 采集端每轮遇到它们时**丢弃并计数**：`dropped_unsupported` 计数 = **%d**'
-                 '（每轮固定 3），日志里记录被丢弃的指标名。' % len(items))
-    lines.append('- 计数会随每轮的采集报告一起上报，可用于核对「本轮应投指标数」与「实投指标数」的差。')
+    lines.append('- 这 %d 个指标**不会出现在 Kafka 消息里**'
+                 '（**已从生效口径移除**）。' % len(items))
+    lines.append('- 处置方式是**构建期排除**：它们在生成生效映射表时就被排除清单剔除了，'
+                 '**生效映射表里根本没有它们** —— 运行期没有「遍历到再丢弃」的分支，'
+                 '轮汇总里的 `dropped_unsupported` 恒为 **0**。')
+    lines.append('- 排除记录（指标码 + 理由）留在 '
+                 '`tpops_kafka_collector/mapping/data/excluded_metrics.json`，'
+                 '客户若要恢复其中某条，改数据文件即可，不用改代码。')
     lines.append('')
 
 
@@ -994,11 +1090,17 @@ def _unsupported_tail(lines, items):
     lines.append('')
 
 
-def render_unsupported(rules):
-    """生成《交付-不可提供指标清单.md》。"""
-    items = disabled_rules(rules)
+def render_unsupported(rules, customer_rows):
+    """生成《交付-不可提供指标清单.md》。
+
+    条目来自**排除清单**（`reason = unsupported`）—— 被排除的指标根本不在
+    生效映射表里，不能再从映射表里找「`enabled = false` 的规则」。
+    """
+    excluded_payload = load_excluded_metrics()
+    items = unsupported_items(excluded_payload.get('excluded') or (), customer_rows)
+    counts = excluded_payload.get('counts') or {}
     lines = []
-    _unsupported_intro(lines, items)
+    _unsupported_intro(lines, items, counts)
     _unsupported_table(lines, items)
     _unsupported_details(lines, items)
     _unsupported_tail(lines, items)
@@ -1235,7 +1337,7 @@ def detect_ocr_findings(rules: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]
     判据（与文档里写的一致）：
 
     1. 把指标英文名按 ``.`` / ``_`` 切成词；
-    2. 候选词表 = **全表 101 个指标名里的词** ∪ **本指标英文描述里的词**；
+    2. 候选词表 = **全表所有指标名里的词** ∪ **本指标英文描述里的词**；
     3. 若某词是候选词删掉 1~2 个字符得到的子序列，且差异**不是**英语屈折变化
        （-s / -es / -ed / -d / -ing / -n / -en），则疑似 OCR 缺字；
     4. 候选词表里的词若本身又是别的词的截断，则不作候选（只保留「最完整」的那个）。
@@ -1333,7 +1435,7 @@ def _ocr_intro(lines: List[str], rules: Sequence[Dict[str, Any]], customer_rows:
                  '因此本文档「我们实际用的名字」一列与「客户表里的英文名」一列**目前完全相同**。')
     lines.append('- 客户确认后，我们把 `metric_rules.json` 里对应规则的 `metric_code`（以及有问题的'
                  '`metric_name`）改成建议写法，**代码不需要改**；届时「我们实际用的名字」才会与客户表产生差异。')
-    lines.append('- **本次核对没有发现取值口径错误**：98 个可提供指标的取数方式、维度、刷新周期'
+    lines.append('- **本次核对没有发现取值口径错误**：%d 个可提供指标的取数方式、维度、刷新周期'
                  '都按映射表复核过，问题集中在**名称拼写**上。'
                  '（单位口径另见第 8 节 —— 客户表里有几处「指标单位」与语义对不上，**待客户确认**。）')
     lines.append('')
@@ -1349,7 +1451,7 @@ def _ocr_intro(lines: List[str], rules: Sequence[Dict[str, Any]], customer_rows:
     lines.append('**自动判据一：删字型**（脚本 `tools/build_delivery_docs.py` 的 `detect_ocr_findings`）：')
     lines.append('')
     lines.append('1. 把指标英文名按 `.` / `_` 切成词；')
-    lines.append('2. 候选词表 = 全表 101 个指标名里的词 ∪ 本指标中文名里的词；')
+    lines.append('2. 候选词表 = 全表 %d 个指标名里的词 ∪ 本指标中文名里的词；' % len(rules))
     lines.append('3. 某词若是候选词**删掉 1~2 个字符**得到的子序列，且差异**不是**英语屈折变化'
                  '（-s / -es / -ed / -d / -ing / -n / -en），则判为疑似 OCR 缺字；')
     lines.append('4. 候选词本身若又是别的词的截断，则不作候选（只保留最完整的写法）。')
@@ -1541,17 +1643,22 @@ def _ocr_conclusion(lines: List[str], customer_rows: Sequence[Dict[str, Any]],
 
 
 def render_ocr_checklist(rules, customer_rows):
-    """生成《交付-客户指标英文名核对清单.md》。"""
-    findings = detect_ocr_findings(rules)
-    counts = corpus_word_counts(rules)
+    """生成《交付-客户指标英文名核对清单.md》。
+
+    检测器跑在**客户指标库全量**（生效口径 + 被排除的）上 —— 核对清单的对象
+    是客户的指标表，不是我们的生效口径。
+    """
+    scope = customer_scope_rules(rules, customer_rows)
+    findings = detect_ocr_findings(scope)
+    counts = corpus_word_counts(scope)
     lines = []
-    _ocr_intro(lines, rules, customer_rows)
-    _ocr_main_table(lines, rules, counts)
-    _ocr_desc_table(lines, rules)
+    _ocr_intro(lines, scope, customer_rows)
+    _ocr_main_table(lines, scope, counts)
+    _ocr_desc_table(lines, scope)
     _ocr_style(lines)
     _ocr_auto_table(lines, findings, customer_rows)
-    _ocr_substitution_table(lines, rules)
-    _ocr_conclusion(lines, customer_rows, rules)
+    _ocr_substitution_table(lines, scope)
+    _ocr_conclusion(lines, customer_rows, scope)
     return '\n'.join(lines) + '\n'
 
 
@@ -1619,8 +1726,9 @@ def build_all() -> Dict[str, str]:
     views = load_merged_sql()
     customer_rows = load_customer_table()
     return {
-        SPEC_DOC: render_metric_spec(rules, views),
-        UNSUPPORTED_DOC: render_unsupported(rules),
+        SPEC_DOC: render_metric_spec(
+            rules, views, load_excluded_metrics().get('counts') or {}),
+        UNSUPPORTED_DOC: render_unsupported(rules, customer_rows),
         OCR_DOC: render_ocr_checklist(rules, customer_rows),
     }
 
diff --git a/tools/build_itscada_mapping.py b/tools/build_itscada_mapping.py
index e9f8ee0..950358d 100644
--- a/tools/build_itscada_mapping.py
+++ b/tools/build_itscada_mapping.py
@@ -32,6 +32,7 @@ import os
 import re
 import sys
 
+import excluded_metrics
 import openpyxl
 import xlsx_columns
 
@@ -74,9 +75,11 @@ def _ensure_input(path, builder_func):
     return path
 
 # 占位符 → SQL 结果列。占位符名字取自客户指标名里的 {#XXX}。
+# 值可以是**点号连接的多个列名** —— 表级指标的 {#NAME} 绑成
+# `schema_name.table_name`（只绑 relname 会让不同 schema 的同名表撞键）。
 DIMENSION_COLUMN_BY_SQL = {
     'stat_database': {'DATNAME': 'datname', 'DATABASE_NAME': 'datname'},
-    'stat_user_tables': {'NAME': 'relname'},
+    'stat_user_tables': {'NAME': 'schemaname.relname'},
     'activity_agg': {'DATNAME': 'datname', 'NAME': 'relname'},
     'locks': {'DBNAME': 'datname'},
     'database': {'DATABASE_NAME': 'datname', 'DATNAME': 'datname'},
@@ -266,6 +269,37 @@ SOURCE_BY_CHANNEL = {
 }
 
 
+#: 事件类告警（未挂靠客户指标）的统一指标名 —— **默认值**。
+#: 实际生效的值取自 ``alarm_rules.json`` 的 ``event_metric`` 段（表驱动）：
+#: 改口径只改数据文件，生成器不覆盖它。
+DEFAULT_EVENT_METRIC = {
+    'metric_code': 'gaussdb.alarm',
+    'metric_name': '数据库告警',
+}
+
+
+def _load_event_metric():
+    """事件类告警的统一指标名（表驱动）。
+
+    优先取 ``alarm_rules.json`` 里已有的 ``event_metric`` 段 —— 那是**运行期
+    真相源**，运维改了它，重跑生成器不会被覆盖回去。
+    文件里没有（或坏了）时用 :data:`DEFAULT_EVENT_METRIC`。
+    """
+    if os.path.isfile(ALARM_RULES_OUT):
+        try:
+            payload = _read_json(ALARM_RULES_OUT)
+        except ValueError:
+            payload = {}
+        existing = payload.get('event_metric')
+        if isinstance(existing, dict) and existing.get('metric_code') \
+                and existing.get('metric_name'):
+            return {
+                'metric_code': existing['metric_code'],
+                'metric_name': existing['metric_name'],
+            }
+    return dict(DEFAULT_EVENT_METRIC)
+
+
 def _read_json(path):
     with io.open(path, encoding='utf-8') as handle:
         return json.load(handle)
@@ -398,6 +432,21 @@ def _dimension_bindings(placeholders, sql_key):
     return bindings
 
 
+def _group_by_columns(bindings):
+    """维度绑定 → 分组列（**点号串要拆成多列**）。
+
+    绑定值可能是 ``schemaname.relname`` —— 那是**两个列**的组合，
+    直接当一个列名会让分组退化成单列（不同 schema 的同名表又撞进同一个键）。
+    按绑定顺序去重，保持声明的先后。
+    """
+    columns = []
+    for spec in bindings.values():
+        for column in spec.split('.'):
+            if column not in columns:
+                columns.append(column)
+    return columns
+
+
 def _resolve_column(expr, columns):
     """把「视图.列」或「列」解析成合并 SQL 里的真实列名。"""
     if not expr or expr == NO_DIM:
@@ -417,7 +466,7 @@ def _row_policy(placeholders, sql_key, value_type):
       first   —— 指标无维度且非数值，取第一行（同一实例内取值一致）
     """
     bindings = _dimension_bindings(placeholders, sql_key)
-    group_by = sorted(set(bindings.values()))
+    group_by = _group_by_columns(bindings)
     if group_by:
         return 'per_row', group_by
     if value_type in ('整数', '浮点'):
@@ -500,7 +549,7 @@ def build_value_rule(metric_code, value_rule, sql_catalog, value_type, placehold
             'type': 'aggregate',
             'params': {
                 'filter': _aggregate_filter(expr, note),
-                'group_by': sorted(set(bindings.values())),
+                'group_by': _group_by_columns(bindings),
                 'fn': 'sum',
                 'value_column': 'cnt',
             },
@@ -578,7 +627,12 @@ def build_metric_rules():
 
     rules = []
     counts = {'可提供': 0, '无法提供': 0}
+    excluded = excluded_metrics.excluded_codes()
     for metric_code, item in spec.items():
+        # 排除发生在构建期：被排除清单剔除的指标**不进生效口径**，
+        # 运行期没有「遍历到再丢弃」的分支。
+        if metric_code in excluded:
+            continue
         avail = availability.get(metric_code, {})
         status = avail.get('status', '无法提供')
         counts[status] = counts.get(status, 0) + 1
@@ -662,6 +716,7 @@ def build_metric_rules():
 def build_alarm_rules():
     """告警规则：字段口径 + 平台 severity/status 枚举 + 告警定义 → ITSCADA 指标挂载。"""
     availability = {item['customer_metric']: item for item in _read_json(MAPPING_PATH)['items']}
+    event_metric = _load_event_metric()
     by_tpops = {}
     for metric_code, item in availability.items():
         tpops_metric = item.get('tpops_metric')
@@ -683,8 +738,10 @@ def build_alarm_rules():
                 metric_name = availability[metric_code]['customer_metric_name']
                 value_source = 'latest_metric_value'
             else:
-                metric_code = 'gaussdb.alarm.%s' % alarm_id
-                metric_name = item.get('告警名称') or ''
+                # 未挂客户指标的事件类告警 → **统一指标名**（表驱动）。
+                # 告警种类由 alarmId / alarmName / alarmContent 承载。
+                metric_code = event_metric['metric_code']
+                metric_name = event_metric['metric_name']
                 value_source = 'alarm_items_sum'
             definitions[alarm_id] = {
                 'alarm_name': item.get('告警名称') or '',
@@ -709,6 +766,7 @@ def build_alarm_rules():
             'data/alarm/catalog_event.json',
         ],
         'field_map_ref': 'itscada_field_map.json#alarm_message',
+        'event_metric': event_metric,
         'severity_to_priority': {
             'critical': 1,
             'major': 2,
diff --git a/tools/build_kafka_mapping.py b/tools/build_kafka_mapping.py
index 766e571..46f73a6 100644
--- a/tools/build_kafka_mapping.py
+++ b/tools/build_kafka_mapping.py
@@ -635,15 +635,28 @@ def _build_metric_mappings(field_map, metric_rules, response_rules, instances, n
             'needs_confirmation': bool(rule_payload.get('needs_confirmation')),
         }
         if rule.dimension_bindings:
-            # 占位符 ← 结果行里的哪一列（维度展开的关键）
+            # 占位符 ← 结果行里的哪一列（维度展开的关键）。
+            # 绑定值可以是**点号连接的多个列名**（``schemaname.relname``）——
+            # 那是两列用 ``.`` 连起来（表级指标的 schema_name.table_name）。
             record['placeholder_bindings'] = dict(
-                (placeholder, 'tpops_response_source.result[*].%s' % column)
+                (placeholder, _placeholder_expression(column))
                 for placeholder, column in rule.dimension_bindings.items()
             )
         mappings.append(record)
     return mappings
 
 
+def _placeholder_expression(spec):
+    """维度绑定 → 结果行里的取值表达式。
+
+    单列 → ``tpops_response_source.result[*].<列>``；
+    多列（点号串）→ 两列用 `` · ``（字符串连接）连起来。
+    """
+    parts = ['tpops_response_source.result[*].%s' % column
+             for column in spec.split('.')]
+    return ' · '.join(parts)
+
+
 # -- 告警映射 -------------------------------------------------------------
 
 ALARM_SHAPES = [
diff --git a/tools/build_metric_value_rules.py b/tools/build_metric_value_rules.py
index 1edefee..85812d5 100644
--- a/tools/build_metric_value_rules.py
+++ b/tools/build_metric_value_rules.py
@@ -18,6 +18,8 @@ import os
 import re
 import sys
 
+import excluded_metrics
+
 BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
 PLAN_PATH = os.path.join(BASE_DIR, 'docs', '开发方案-GaussDB指标与告警采集.md')
 OUT_PATH = os.path.join(BASE_DIR, 'data', 'metric_value_rules.json')
@@ -158,6 +160,7 @@ def _parse_d_class(text):
 
 def _build_metrics(text):
     metrics = {}
+    excluded = excluded_metrics.excluded_codes()
     for row in _parse_a_class(text):
         metrics[row['customer_metric']] = {
             'channel': 'A',
@@ -211,6 +214,10 @@ def _build_metrics(text):
             'dimensions': [],
             'plan_note': row['plan_refresh_cycle'],
         }
+    # 排除发生在构建期：被排除清单剔除的指标不进生效口径
+    # （开发方案 §4.3 仍是 101 条的原始记录，生效口径以排除清单为准）。
+    for code in excluded:
+        metrics.pop(code, None)
     return metrics
 
 
@@ -267,11 +274,31 @@ def main():
         json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=False)
         handle.write('\n')
     print('wrote %s: %s' % (OUT_PATH, payload['counts']))
-    if payload['counts']['total'] != 98:
-        print('ERROR: 期望 98 条可提供指标，实际 %d' % payload['counts']['total'], file=sys.stderr)
+    expected = _expected_effective_count()
+    if expected is not None and payload['counts']['total'] != expected:
+        print('ERROR: 期望 %d 条生效指标，实际 %d' % (expected, payload['counts']['total']),
+              file=sys.stderr)
         return 1
     return 0
 
 
+def _expected_effective_count():
+    """生效口径的条数 —— 以可提供性判定（``customer_metric_mapping.json``）为准。
+
+    条数**不写死**：生效口径 = 客户指标库里「可提供」的条数，
+    排除清单在可提供性判定那一步就已经生效了。
+    判定文件不在（还没跑那个生成器）时返回 ``None`` —— 跳过这道交叉校验。
+    """
+    path = os.path.join(BASE_DIR, 'data', 'customer_metric_mapping.json')
+    if not os.path.exists(path):
+        return None
+    with io.open(path, encoding='utf-8') as handle:
+        payload = json.load(handle)
+    return len([
+        item for item in payload.get('items') or ()
+        if item.get('status') == '可提供'
+    ])
+
+
 if __name__ == '__main__':
     sys.exit(main())
diff --git a/tools/excluded_metrics.py b/tools/excluded_metrics.py
new file mode 100644
index 0000000..485587a
--- /dev/null
+++ b/tools/excluded_metrics.py
@@ -0,0 +1,57 @@
+#!/usr/bin/env python3
+# -*- coding: utf-8 -*-
+"""排除清单的读取（构建期共用）。
+
+排除清单是**数据**，不是代码：
+
+- 包内 ``tpops_kafka_collector/mapping/data/excluded_metrics.json`` 是**运行期真相源**；
+- 仓库根 ``data/mapping/excluded_metrics.json`` 是**同源副本**（逐字节一致，
+  由 ``tests/unit/test_mapping_data_sync.py`` 的防漂移闸守着）。
+
+构建期的几个生成器（可提供性判定 / 取值规则 / ITSCADA 映射表）都从**同一份**
+清单读排除口径 —— 客户口径只有一处，改清单就能改生效口径，不用改代码。
+"""
+from __future__ import print_function
+
+import io
+import json
+import os
+
+BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
+
+#: 仓库根的同源副本（生成器的输入都在仓库根 ``data/`` 下，路径保持一致）。
+EXCLUDED_PATH = os.path.join(BASE_DIR, 'data', 'mapping', 'excluded_metrics.json')
+
+#: 排除原因的合法取值。
+REASON_DUPLICATE = 'duplicate'
+REASON_UNSUPPORTED = 'unsupported'
+
+
+def load_excluded(path=None):
+    """读排除清单，返回 ``{指标码: 条目}``。
+
+    :param path: 清单路径；缺省是仓库根的同源副本。
+    :return: ``{metric_code: entry}``。
+    """
+    target = path or EXCLUDED_PATH
+    if not os.path.isfile(target):
+        raise IOError('找不到排除清单：%s' % target)
+    with io.open(target, encoding='utf-8') as handle:
+        payload = json.load(handle)
+    entries = {}
+    for entry in payload.get('excluded') or ():
+        code = entry.get('metric_code')
+        if not code:
+            raise ValueError('排除清单里有条目没有 metric_code：%r' % entry)
+        entries[code] = entry
+    return entries
+
+
+def excluded_codes(path=None):
+    """排除清单里的全部指标码。"""
+    return set(load_excluded(path).keys())
+
+
+def is_excluded(metric_code, path=None):
+    """该指标码是否在排除清单里。"""
+    return metric_code in excluded_codes(path)
diff --git a/tools/expected_deriver.py b/tools/expected_deriver.py
index c417f60..f8e2cc5 100644
--- a/tools/expected_deriver.py
+++ b/tools/expected_deriver.py
@@ -375,9 +375,25 @@ def _dimensions(
     bindings: Optional[Mapping[str, str]],
     row: Mapping[str, Any],
 ) -> Dict[str, Any]:
+    """维度值 —— 绑定值支持**点号连接的多个列名**（``schemaname.relname``）。
+
+    表级指标的 ``{#NAME}`` 绑成 ``schema_name.table_name``，各列的值用
+    ``.`` 连起来（与生产实现 ``mapping/rules_context.py`` 同一口径）。
+    """
     dims = {}
-    for placeholder, column in (bindings or {}).items():
-        dims[placeholder] = row.get(column)
+    for placeholder, spec in (bindings or {}).items():
+        columns = spec.split('.') if '.' in spec else [spec]
+        if len(columns) == 1:
+            dims[placeholder] = row.get(columns[0])
+            continue
+        values = []
+        for column in columns:
+            value = row.get(column)
+            if value is None:
+                raise ArchiveError(
+                    '维度绑定 %s→%s 取不到列 %s 的值' % (placeholder, spec, column))
+            values.append(str(value))
+        dims[placeholder] = '.'.join(values)
     return dims
 
 
diff --git a/tpops_kafka_collector/collectors/alarm.py b/tpops_kafka_collector/collectors/alarm.py
index 28fbd3f..e76475d 100644
--- a/tpops_kafka_collector/collectors/alarm.py
+++ b/tpops_kafka_collector/collectors/alarm.py
@@ -33,7 +33,7 @@ from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
 from ..ids import alarm_idempotency_key
 from ..mapping.message import build_alarm_message
 from ..mapping.nodes import select_master_node
-from ..mapping.table import AlarmDefinition
+from ..mapping.table import AlarmDefinition, build_event_alarm_definition
 from ..tpops.alarms import DEFAULT_LIMIT, fetch_all_alarm_records
 
 LOGGER = logging.getLogger(__name__)
@@ -249,27 +249,21 @@ def _definition_for(
     row: Mapping[str, Any],
     alarm_id: str,
 ) -> AlarmDefinition:
-    """取告警定义；定义缺失时按「``gaussdb.alarm.<alarm_id>`` 自造指标名」兜底。"""
+    """取告警定义；定义缺失时按**统一指标名**兜底。
+
+    统一指标名来自映射数据（``alarm_rules.json`` 的 ``event_metric`` 段）——
+    与事件类告警的口径一致，代码里不写死任何客户口径。
+    """
     definition = mapping.alarm_definition(alarm_id)
     if definition is not None:
         return definition
+    event_metric = (mapping.alarm_rules or {}).get('event_metric') or {}
     LOGGER.warning(
-        '告警定义缺失：alarm_id=%s（平台新告警？），按 gaussdb.alarm.%s 兜底挂载',
-        alarm_id, alarm_id,
-    )
-    alarm_name = row.get('alarm_name') or ''
-    return AlarmDefinition(
-        alarm_id=alarm_id,
-        alarm_name=alarm_name,
-        alarm_kind=row.get('alarm_type') or '',
-        default_level='',
-        default_priority=None,
-        threshold='',
-        metric_code=f'gaussdb.alarm.{alarm_id}',
-        metric_name=alarm_name,
-        value_source='alarm_items_sum',
-        linked_customer_metric=None,
+        '告警定义缺失：alarm_id=%s（平台新告警？），按统一指标名 %s 兜底挂载',
+        alarm_id, event_metric.get('metric_code'),
     )
+    return build_event_alarm_definition(
+        alarm_id, row.get('alarm_name') or '', event_metric)
 
 
 def _unit_for(mapping: Any, definition: AlarmDefinition) -> Optional[str]:
diff --git a/tpops_kafka_collector/mapping/README.md b/tpops_kafka_collector/mapping/README.md
index 490e412..81cb6d8 100644
--- a/tpops_kafka_collector/mapping/README.md
+++ b/tpops_kafka_collector/mapping/README.md
@@ -9,7 +9,7 @@
 |---|---|
 | `docs/ITSCADA第三方指标数据接入对接方案V2.docx` | 指标消息的字段集合与语义（§6.2 字段表、§7 指标表模板） |
 | `docs/ITSCADA第三方告警数据接入对接方案V2.docx` | 告警消息的字段集合与语义（§3.2 字段表、§4 压缩规则） |
-| `高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` / 指标库 | 101 个客户指标的规范：中文名、英文名、数据类型、周期、单位 |
+| `高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` / 指标库 | 101 个客户指标的规范：中文名、英文名、数据类型、周期、单位（**只读**，不因排除清单改动） |
 | `docs/开发方案-GaussDB指标与告警采集.md` §4.3 | 每个指标走哪条取数通道、怎么从原始值算出客户值 |
 
 ## 文件布局
@@ -17,16 +17,18 @@
 ```
 tpops_kafka_collector/mapping/   # 生产代码：规则数据 + 执行代码（随包发布）
 ├── data/                     # 规则数据（表驱动）—— **生产环境的唯一真相源**
-│   ├── metric_rules.json     # 业务规则：101 条指标规则（98 可提供 + 3 标记丢弃）
-│   ├── response_rules.json   # 取数定位：响应体的哪一段哪一列（98 条 + 18 种形状）
-│   ├── kafka_field_mapping.json  # 字段映射：值 → Kafka 字段（98 条指标 + 4 种告警形态）
+│   ├── metric_rules.json     # 业务规则：87 条生效指标规则（全部 enabled）
+│   ├── excluded_metrics.json # 排除清单：14 条被排除的客户指标（11 重复 + 3 无法提供）
+│   ├── response_rules.json   # 取数定位：响应体的哪一段哪一列（87 条 + 18 种形状）
+│   ├── kafka_field_mapping.json  # 字段映射：值 → Kafka 字段（87 条指标 + 4 种告警形态）
 │   ├── alarm_rules.json      # 告警规则：等级/状态映射 + 257 条告警定义
+│   │                         #   （`event_metric` 段 = 未挂客户指标的统一指标名）
 │   ├── itscada_field_map.json  # 客户消息的字段清单（哪些字段、必填与否）
 │   ├── merged_sql.json       # 11 个合并 SQL 视图的定义 + 刷新档位 + 分页段
 │   ├── sql_merge.sql         # 合并 SQL 的归档留档（与生成器逐字符比对）
-│   ├── customer_metric_mapping.json  # 101 客户指标 → 可提供性判定（独立基准）
+│   ├── customer_metric_mapping.json  # 87 生效指标 → 可提供性判定（独立基准）
 │   ├── observed_columns.json # 实测留档的实际返回列（列名校验用，生成物）
-│   └── open_items.json       # 待客户确认的 8 项
+│   └── open_items.json       # 待客户确认的 12 项
 ├── __init__.py               # 对外接口
 ├── table.py                  # 加载与查表（MappingTable / MetricRule / AlarmDefinition）
 ├── rules.py                  # 取值规则引擎的对外接口 + 文档（13 种 value_rule.type）
@@ -223,6 +225,59 @@ python tools/build_kafka_mapping.py      # → kafka_field_mapping.json
 
 测试会拿"实测行数"对账：`per_row` 的指标有几行就得产出几条消息。
 
+### 表级指标的维度值：`schema_name.table_name`
+
+表级指标的 `{#NAME}` 绑的是**两列**（`dimension_bindings` 的值可以是
+**点号连接的多个列名**）：
+
+```json
+"dimension_bindings": {"NAME": "schemaname.relname"},
+"value_rule": {"type": "raw", "params": {
+    "column": "n_tup_del", "row_policy": "per_row",
+    "group_by": ["schemaname", "relname"]}}
+```
+
+- 维度值 = `schemaname` 与 `relname` 用 `.` 连起来 → `public.t1`；
+- **分组按两列**（`schemaname` + `relname`）—— 只按 `relname` 分组会让
+  `public.t1` 与 `other.t1` 落进同一个键，两条消息在幂等层撞键、
+  后一条被**静默丢弃**；
+- 幂等键 = `instance_id + metric_name + 排序后的 dimensions + timestamp`，
+  维度不同 → 键不同 → 两条都投递；同一张表重复采集 → 键相同 → 仍然去重。
+
+组合绑定取不到列值 → 抛 `RuleError`，**不静默留 `{#NAME}`**。
+
+### 排除清单（`excluded_metrics.json`）
+
+从生效口径里剔除的客户指标（**14 条** = 11 条重复的英文变体 + 3 条无法提供）：
+
+```json
+{"metric_code": "GAUSSDB.{#NAME}.garbage.Ratio",
+ "metric_name": "({#NAME})Garbage ratio",
+ "reason": "duplicate",
+ "detail": "与 gaussdb.{#NAME}.garbage.Ratio 是同一条表级指标 ……",
+ "kept": "gaussdb.{#NAME}.garbage.Ratio"}
+```
+
+- `reason` = `duplicate`（重复）时**必带** `kept`（保留哪一条）；
+  `reason` = `unsupported`（无法提供）时不带 `kept`；
+- 排除发生在**构建期**（`tools/build_itscada_mapping.py` 生成映射表时跳过）——
+  运行期**没有**「遍历到再丢弃」的分支，被排除的指标码**根本不在**生效映射表里，
+  轮汇总里的 `dropped_unsupported` 恒为 **0**；
+- 改口径**只改这份数据文件**，不改代码。
+
+### 告警的统一指标名（`alarm_rules.json` 的 `event_metric` 段）
+
+平台 257 条告警定义里只有 3 条挂靠了客户指标（都指向 `gaussdb.Tps`）；
+其余 254 条事件类告警**共用同一个**指标名：
+
+```json
+"event_metric": {"metric_code": "gaussdb.alarm", "metric_name": "数据库告警"}
+```
+
+- 统一值**表驱动**（写在数据文件里），改口径只改数据、不改代码；
+- 告警种类由 `alarmId` / `alarmName` / `alarmContent` 承载，不靠 `metricCode` 区分；
+- `metric_code` 唯一性**只对指标规则**要求 —— 告警定义靠 `alarm_id` 唯一定位。
+
 ### 展开方式
 
 | 方式 | 指标数 | 含义 |
diff --git a/tpops_kafka_collector/mapping/data/excluded_metrics.json b/tpops_kafka_collector/mapping/data/excluded_metrics.json
new file mode 100644
index 0000000..b75146e
--- /dev/null
+++ b/tpops_kafka_collector/mapping/data/excluded_metrics.json
@@ -0,0 +1,112 @@
+{
+  "version": "1.0",
+  "note": "排除清单：从生效口径里剔除的客户指标。排除发生在构建期（映射表生成器），运行期没有「遍历到再丢弃」的分支 —— 被排除的指标码根本不会出现在生效映射表里。",
+  "sources": [
+    "高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx / 指标库",
+    "docs/问题单.md TPOPS-003（11 组英文名重复）、TPOPS-001（3 条无法提供）"
+  ],
+  "counts": {
+    "total": 14,
+    "duplicate": 11,
+    "unsupported": 3,
+    "client_total": 101,
+    "effective_total": 87
+  },
+  "excluded": [
+    {
+      "metric_code": "GAUSSDB.{#NAME}.Number.lete.ples",
+      "metric_name": "({#NAME}) number of delete tuples",
+      "reason": "duplicate",
+      "detail": "与 gaussdb.{#NAME}.Number.lete.ples 是同一条表级指标（客户指标库「事务信息」分类里英文名的一条）；保留「表信息」分类里中文名正常的一条。",
+      "kept": "gaussdb.{#NAME}.Number.lete.ples"
+    },
+    {
+      "metric_code": "GAUSSDB.{#NAME}.Number.ad.ples",
+      "metric_name": "({#NAME}) number of dead tuples",
+      "reason": "duplicate",
+      "detail": "与 gaussdb.{#NAME}.Number.ad.ples 是同一条表级指标（客户指标库「事务信息」分类里英文名的一条）；保留「表信息」分类里中文名正常的一条。",
+      "kept": "gaussdb.{#NAME}.Number.ad.ples"
+    },
+    {
+      "metric_code": "GAUSSDB.{#NAME}.Number.t.date.ples",
+      "metric_name": "({#NAME}) number of hot update tuples",
+      "reason": "duplicate",
+      "detail": "与 gaussdb.{#NAME}.Number.t.date.ples 是同一条表级指标（客户指标库「事务信息」分类里英文名的一条）；保留「表信息」分类里中文名正常的一条。",
+      "kept": "gaussdb.{#NAME}.Number.t.date.ples"
+    },
+    {
+      "metric_code": "GAUSSDB.{#NAME}.number.Of.index.scan",
+      "metric_name": "({#NAME})number of index scan",
+      "reason": "duplicate",
+      "detail": "与 gaussdb.{#NAME}.number.Of.index.scan 是同一条表级指标（客户指标库「事务信息」分类里英文名的一条）；保留「表信息」分类里中文名正常的一条。",
+      "kept": "gaussdb.{#NAME}.number.Of.index.scan"
+    },
+    {
+      "metric_code": "GAUSSDB.{#NAME}.number.Of.quential.ples.ad",
+      "metric_name": "({#NAME})number of sequencial tuples read",
+      "reason": "duplicate",
+      "detail": "与 gaussdb.{#NAME}.number.Of.quential.ples.ad 是同一条表级指标（客户指标库「事务信息」分类里英文名的一条）；保留「表信息」分类里中文名正常的一条。",
+      "kept": "gaussdb.{#NAME}.number.Of.quential.ples.ad"
+    },
+    {
+      "metric_code": "GAUSSDB.{#NAME}.number.Of.dex.ples.ad",
+      "metric_name": "({#NAME})number of index tuples read",
+      "reason": "duplicate",
+      "detail": "与 gaussdb.{#NAME}.number.Of.dex.ples.ad 是同一条表级指标（客户指标库「事务信息」分类里英文名的一条）；保留「表信息」分类里中文名正常的一条。",
+      "kept": "gaussdb.{#NAME}.number.Of.dex.ples.ad"
+    },
+    {
+      "metric_code": "GAUSSDB.{#NAME}.number.Of.insert.tuples",
+      "metric_name": "({#NAME})number of insert tuples",
+      "reason": "duplicate",
+      "detail": "与 gaussdb.{#NAME}.number.Of.insert.tuples 是同一条表级指标（客户指标库「事务信息」分类里英文名的一条）；保留「表信息」分类里中文名正常的一条。",
+      "kept": "gaussdb.{#NAME}.number.Of.insert.tuples"
+    },
+    {
+      "metric_code": "GAUSSDB.{#NAME}.number.Of.live.tuples",
+      "metric_name": "({#NAME})number of live tuples",
+      "reason": "duplicate",
+      "detail": "与 gaussdb.{#NAME}.number.Of.live.tuples 是同一条表级指标（客户指标库「事务信息」分类里英文名的一条）；保留「表信息」分类里中文名正常的一条。",
+      "kept": "gaussdb.{#NAME}.number.Of.live.tuples"
+    },
+    {
+      "metric_code": "GAUSSDB.{#NAME}.number.Of.update.tuples",
+      "metric_name": "({#NAME})number of update tuples",
+      "reason": "duplicate",
+      "detail": "与 gaussdb.{#NAME}.number.Of.update.tuples 是同一条表级指标（客户指标库「事务信息」分类里英文名的一条）；保留「表信息」分类里中文名正常的一条。",
+      "kept": "gaussdb.{#NAME}.number.Of.update.tuples"
+    },
+    {
+      "metric_code": "GAUSSDB.{#NAME}.number.Of.sequential.scan",
+      "metric_name": "({#NAME})number of sequencial scan",
+      "reason": "duplicate",
+      "detail": "与 gaussdb.{#NAME}.number.Of.sequential.scan 是同一条表级指标（客户指标库「事务信息」分类里英文名的一条）；保留「表信息」分类里中文名正常的一条。",
+      "kept": "gaussdb.{#NAME}.number.Of.sequential.scan"
+    },
+    {
+      "metric_code": "GAUSSDB.{#NAME}.garbage.Ratio",
+      "metric_name": "({#NAME})Garbage ratio",
+      "reason": "duplicate",
+      "detail": "与 gaussdb.{#NAME}.garbage.Ratio 是同一条表级指标（客户指标库「事务信息」分类里英文名的一条）；保留「表信息」分类里中文名正常的一条。",
+      "kept": "gaussdb.{#NAME}.garbage.Ratio"
+    },
+    {
+      "metric_code": "gaussdb.Fastpath_function_call",
+      "metric_name": "快路径函数调用的连接数",
+      "reason": "unsupported",
+      "detail": "PG 侧无 fastpath 计数器；pg_settings 仅有 gsi_fastpath_level 开关"
+    },
+    {
+      "metric_code": "gaussdb.Disabled_conn",
+      "metric_name": "禁用的连接数",
+      "reason": "unsupported",
+      "detail": "PG 无「禁用连接」概念；pg_settings 仅有 reserved_connections 等预留连接"
+    },
+    {
+      "metric_code": "gaussdb.sendAgentName",
+      "metric_name": "发送设备名给Agent",
+      "reason": "unsupported",
+      "detail": "Agent 侧行为，不是数据库可查询的监控指标"
+    }
+  ]
+}
diff --git a/tpops_kafka_collector/mapping/data/open_items.json b/tpops_kafka_collector/mapping/data/open_items.json
index 413e5f9..f5dcea6 100644
--- a/tpops_kafka_collector/mapping/data/open_items.json
+++ b/tpops_kafka_collector/mapping/data/open_items.json
@@ -17,7 +17,7 @@
       "id": "resource-name",
       "topic": "resourceName 与客户台账的对齐",
       "default": "{host_ip}_{node_id}（节点 IP + TPOPS 节点 ID）；与节点无关的指标落到节点清单里的第一个 master CN",
-      "detail": "resourceName 只能表示一个节点 → 固定「节点 IP + 节点 ID」；表名/库名/端口这类业务维度改走 metricName/metricCode 的占位符与 tags.dimension_values。方案要求与 ITSCADA 台账录入的资源名称完全一致；台账命名规则待客户提供。"
+      "detail": "resourceName 只能表示一个节点 → 固定「节点 IP + 节点 ID」；表名/库名/端口这类业务维度改走 metricName/metricCode 的占位符与 tags.dimension_values；表级指标的维度值是 schema_name.table_name（如 public.t1）。方案要求与 ITSCADA 台账录入的资源名称完全一致；台账命名规则待客户提供。"
     },
     {
       "id": "collect-cycle",
@@ -34,8 +34,8 @@
     {
       "id": "alarm-metric-binding",
       "topic": "告警挂到哪个客户指标",
-      "default": "257 条告警里只有 3 条能对应上客户指标（都指向 gaussdb.Tps），其余用 gaussdb.alarm.<alarm_id> 自造指标名",
-      "detail": "事件类告警没有对应的客户指标，需要客户确认这种挂法是否可接受。"
+      "default": "未挂客户指标的 254 条事件类告警统一用 metricCode=gaussdb.alarm / metricName=数据库告警",
+      "detail": "原先每条告警各造一个 gaussdb.alarm.<alarm_id> 指标名（还有 gaussdb.alarm.>=75% 这种拿阈值当 ID 的），客户侧要建 254 条台账条目。统一后告警种类由 alarmId / alarmName / alarmContent 承载，ITSCADA 侧告警台账从 254 项变 1 项。**对外契约变化，待客户确认。**"
     },
     {
       "id": "slow-sql-masking",
@@ -52,8 +52,8 @@
     {
       "id": "metric-code-case",
       "topic": "11 组英文名仅大小写不同、中文名不同",
-      "default": "两套都投递（metricCode 只差前缀大小写，值完全一样）",
-      "detail": "客户指标库 101 个指标里，英文名忽略大小写后有 11 组相同但中文名不同（全部是表级指标：小写 gaussdb. 前缀=中文名+表信息+2 分钟，大写 GAUSSDB. 前缀=英文名+事务信息+5 分钟）。客户侧若按大小写不敏感匹配，这 11 个指标会合并覆盖。要客户确认：是两条独立指标（需给出区分办法），还是同一条的重复登记（留哪一套）。清单见 docs/问题单.md TPOPS-003。"
+      "default": "只投递中文名正常的一套（小写 gaussdb. 前缀）；大写 GAUSSDB. 前缀的 11 条进排除清单，不产出消息",
+      "detail": "客户指标库 101 个指标里，英文名忽略大小写后有 11 组相同但中文名不同（全部是表级指标：小写 gaussdb. 前缀=中文名+表信息+2 分钟，大写 GAUSSDB. 前缀=英文名+事务信息+5 分钟）。客户侧若按大小写不敏感匹配，这 11 个指标会合并覆盖。本次按「保留中文名正常的一条」落地，被排除的 11 条见 tpops_kafka_collector/mapping/data/excluded_metrics.json。清单见 docs/问题单.md TPOPS-003。"
     },
     {
       "id": "rate-unit-semantics",
@@ -66,6 +66,12 @@
       "topic": "几处单位与来源字段语义对不上",
       "default": "按客户表原样带单位",
       "detail": "gaussdb.Temp_files 单位 b（源列 temp_files 是文件个数）、gaussdb.Conflicts 单位 ms（源列 conflicts 是冲突次数）、gaussdb.Blk_read_time 单位 个（源列 blk_read_time 是毫秒）、GAUSSDB.{#NAME}.Number.t.date.ples 单位 %（源列 n_tup_hot_upd 是行数）。是客户表抄错还是另有含义？确认后只改映射表。清单见 docs/问题单.md TPOPS-004。"
+    },
+    {
+      "id": "alarm-unified-metric",
+      "topic": "事件类告警的统一指标名取值",
+      "default": "metricCode=gaussdb.alarm、metricName=数据库告警",
+      "detail": "统一值写在 alarm_rules.json 的 event_metric 段（表驱动），改口径只改数据文件、不改代码。统一后 ITSCADA 侧的告警指标台账从 254 项变 1 项（告警种类由 alarmId / alarmName / alarmContent 承载）——**对外契约变化，需客户确认这个指标名与中文名。**"
     }
   ]
 }
diff --git a/tpops_kafka_collector/mapping/loader.py b/tpops_kafka_collector/mapping/loader.py
index f8c13dd..ec0e506 100644
--- a/tpops_kafka_collector/mapping/loader.py
+++ b/tpops_kafka_collector/mapping/loader.py
@@ -25,12 +25,15 @@
 import json
 import logging
 import os
-from dataclasses import dataclass
+from dataclasses import dataclass, field
 from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Tuple
 
 from ..errors import MappingError, ProtocolError
 from .table import AlarmDefinition, MetricRule, build_alarm_definition
 from .validation import (
+    check_effective_rule_count,
+    check_excluded_metrics_absent,
+    check_metric_code_uniqueness,
     check_placeholder_bindings,
     check_providable_coverage,
     check_rule_entries,
@@ -38,9 +41,9 @@ from .validation import (
 )
 
 __all__ = [
-    'CustomerMetric', 'LoadedMapping', 'ObservedColumns', 'PACKAGE_DATA_DIR',
-    'check_placeholder_bindings', 'check_providable_coverage', 'check_rule_entries',
-    'check_source_columns', 'load_mapping', 'validate_mapping',
+    'CustomerMetric', 'ExcludedMetric', 'LoadedMapping', 'ObservedColumns',
+    'PACKAGE_DATA_DIR', 'check_placeholder_bindings', 'check_providable_coverage',
+    'check_rule_entries', 'check_source_columns', 'load_mapping', 'validate_mapping',
 ]
 
 LOGGER = logging.getLogger(__name__)
@@ -54,6 +57,7 @@ FILES = {
     'alarm_rules': 'alarm_rules.json', 'kafka_field_mapping': 'kafka_field_mapping.json',
     'response_rules': 'response_rules.json', 'customer_metrics': 'customer_metric_mapping.json',
     'open_items': 'open_items.json', 'observed_columns': 'observed_columns.json',
+    'excluded_metrics': 'excluded_metrics.json',
 }
 
 @dataclass(frozen=True)
@@ -81,6 +85,20 @@ class CustomerMetric:
     match_level: Optional[str]
 
 
+@dataclass(frozen=True)
+class ExcludedMetric:
+    """排除清单里的一条（**不进生效口径**的客户指标）。
+
+    ``reason`` 为 ``duplicate`` 时 ``kept`` 指出保留哪一条；
+    ``unsupported`` 没有 ``kept``（没有可保留的对象）。
+    """
+    metric_code: str
+    metric_name: str
+    reason: str
+    detail: str
+    kept: Optional[str]
+
+
 @dataclass(frozen=True)
 class LoadedMapping:
     """加载好的映射数据（只读）。
@@ -98,6 +116,8 @@ class LoadedMapping:
     customer_metrics: Tuple[CustomerMetric, ...]
     open_items: Mapping[str, Any]
     observed: ObservedColumns
+    excluded_metrics: Tuple[ExcludedMetric, ...] = ()
+    excluded_counts: Mapping[str, int] = field(default_factory=dict)
 
     # -- 查表 -------------------------------------------------------------
 
@@ -138,12 +158,25 @@ def load_mapping(
 
 
 def validate_mapping(bundle: LoadedMapping) -> None:
-    """跑完四类校验；有不通过的就抛 ``MappingError``（带完整差异清单）。"""
+    """跑完全部校验；有不通过的就抛 ``MappingError``（带完整差异清单）。
+
+    除了四类映射校验，还校验**生效口径**三件事：
+
+    5. **排除清单的指标码一个都不在生效映射表里**（排除是构建期做的，运行期
+       不该再有被排除的指标）；
+    6. **生效规则的 ``metric_code`` 互不重复**（唯一性**只对指标规则**要求 ——
+       告警定义靠 ``alarm_id`` 定位，多条告警共用同一个 ``metricCode`` 是允许的）；
+    7. **生效规则数**与排除清单声明的生效条数一致。
+    """
     problems: List[str] = []
     problems.extend(check_source_columns(bundle.metric_rules, bundle.observed))
     problems.extend(check_providable_coverage(bundle.metric_rules, bundle.customer_metrics))
     problems.extend(check_rule_entries(bundle.metric_rules, bundle.customer_metrics))
     problems.extend(check_placeholder_bindings(bundle.metric_rules))
+    problems.extend(check_excluded_metrics_absent(bundle.metric_rules, bundle.excluded_metrics))
+    problems.extend(check_metric_code_uniqueness(bundle.metric_rules))
+    problems.extend(check_effective_rule_count(
+        bundle.metric_rules, bundle.excluded_counts.get('effective_total')))
     if problems:
         raise MappingError('启动期映射校验不通过：\n  ' + '\n  '.join(problems))
 
@@ -159,6 +192,7 @@ def _load_files(directory: str) -> LoadedMapping:
     rules_payload = _read_json(directory, 'metric_rules')
     customer_payload = _read_json(directory, 'customer_metrics')
     observed_payload = _read_json(directory, 'observed_columns')
+    excluded_payload = _read_json(directory, 'excluded_metrics')
     rules = _parse_rules(rules_payload)
     return LoadedMapping(
         field_map=field_map,
@@ -169,9 +203,31 @@ def _load_files(directory: str) -> LoadedMapping:
         customer_metrics=_parse_customer_metrics(customer_payload),
         open_items=open_items,
         observed=_parse_observed(observed_payload),
+        excluded_metrics=_parse_excluded_metrics(excluded_payload),
+        excluded_counts=dict(excluded_payload.get('counts') or {}),
     )
 
 
+def _parse_excluded_metrics(payload: Mapping[str, Any]) -> Tuple[ExcludedMetric, ...]:
+    """排除清单 → 排除项元组（结构不对就抛 ``ProtocolError``）。"""
+    items = payload.get('excluded')
+    if not isinstance(items, list):
+        raise ProtocolError('excluded_metrics.json 缺少 excluded 数组')
+    metrics = []  # type: List[ExcludedMetric]
+    for item in items:
+        code = item.get('metric_code')
+        if not code:
+            raise ProtocolError('excluded_metrics.json 里有条目没有 metric_code')
+        metrics.append(ExcludedMetric(
+            metric_code=code,
+            metric_name=item.get('metric_name') or '',
+            reason=item.get('reason') or '',
+            detail=item.get('detail') or '',
+            kept=item.get('kept'),
+        ))
+    return tuple(metrics)
+
+
 def _parse_rules(payload: Mapping[str, Any]) -> Tuple[MetricRule, ...]:
     items = payload.get('rules')
     if not isinstance(items, list):
diff --git a/tpops_kafka_collector/mapping/rules_context.py b/tpops_kafka_collector/mapping/rules_context.py
index e322876..b500380 100644
--- a/tpops_kafka_collector/mapping/rules_context.py
+++ b/tpops_kafka_collector/mapping/rules_context.py
@@ -100,14 +100,46 @@ def matches_filter(row: Mapping[str, Any], filters: Mapping[str, Any]) -> bool:
     return True
 
 
+def binding_columns(spec: str) -> List[str]:
+    """维度绑定的值 → 实际列名列表。
+
+    值可以是**单个列名**（``relname``），也可以是**点号连接的多个列名**
+    （``schemaname.relname``）—— 表级指标的 ``{#NAME}`` 要绑成
+    ``schema_name.table_name``，两个列的值用 ``.`` 连起来。
+    """
+    return spec.split('.') if '.' in spec else [spec]
+
+
 def row_dimensions(metric_rule: Any, row: Mapping[str, Any]) -> Dict[str, Any]:
-    """按 ``dimension_bindings``（占位符 → 列名）取这一行的维度值。"""
+    """按 ``dimension_bindings``（占位符 → 列名）取这一行的维度值。
+
+    绑定值是**点号连接的多个列名**时，各列的值用 ``.`` 连起来
+    （``schemaname=public`` + ``relname=t1`` → ``public.t1``）。
+    取不到任何一个列的值 → 抛 ``RuleError``（不静默留 ``{#NAME}``）。
+    """
     dims = {}
-    for placeholder, column in (metric_rule.dimension_bindings or {}).items():
-        dims[placeholder] = row.get(column)
+    for placeholder, spec in (metric_rule.dimension_bindings or {}).items():
+        dims[placeholder] = _binding_value(placeholder, spec, row)
     return dims
 
 
+def _binding_value(placeholder: str, spec: str, row: Mapping[str, Any]) -> Any:
+    """一行里取一个维度绑定的值（单列原样返回，多列用 ``.`` 连接）。"""
+    columns = binding_columns(spec)
+    if len(columns) == 1:
+        return row.get(columns[0])
+    values = []
+    for column in columns:
+        value = row.get(column)
+        if value is None:
+            raise RuleError(
+                f'维度绑定 {placeholder}→{spec} 取不到列 {column} 的值'
+                f'（行里有 {sorted(row.keys())}）'
+            )
+        values.append(str(value))
+    return '.'.join(values)
+
+
 def dimension_key(dimensions: Mapping[str, Any]) -> str:
     """维度键（差分基线的键的一部分）—— 维度按键排序，与插入顺序无关。"""
     if not dimensions:
@@ -127,6 +159,10 @@ def grouped_items(
     （一边说按 ``relname`` 分组、一边把 ``schemaname`` 当维度），抛 ``RuleError``
     而不是静默取其中一个。分组键的值就是维度值。
 
+    **多列分组**：绑定值是点号串（``schemaname.relname``）时，分组键必须同时
+    含这两列 —— 只按 ``relname`` 分组会让不同 schema 的同名表落进同一个键，
+    维度值不唯一、两条消息在幂等层撞键。
+
     分组键重复（同名的两张表落在同一个键里）会记 WARNING：维度值不唯一，
     两条消息在幂等层会撞键 —— 显式报出来，不静默。
     """
@@ -134,7 +170,10 @@ def grouped_items(
     bindings = metric_rule.dimension_bindings or {}
     if not group_by:
         return [(row_dimensions(metric_rule, row), row) for row in rows]
-    missing = sorted(set(bindings.values()) - set(group_by))
+    bound_columns = set()  # type: set
+    for spec in bindings.values():
+        bound_columns.update(binding_columns(spec))
+    missing = sorted(bound_columns - set(group_by))
     if missing:
         raise RuleError(
             f'group_by {list(group_by)} 与维度绑定 {dict(bindings)} 不一致：分组键缺少列 {missing}'
@@ -152,13 +191,35 @@ def grouped_items(
             )
         for row in group_rows:
             dims = dict(
-                (placeholder, key[group_by.index(column)])
-                for placeholder, column in bindings.items()
+                (placeholder, _value_from_key(placeholder, spec, key, group_by))
+                for placeholder, spec in bindings.items()
             )
             items.append((dims, row))
     return items
 
 
+def _value_from_key(
+    placeholder: str,
+    spec: str,
+    key: Tuple[Any, ...],
+    group_by: Tuple[str, ...],
+) -> Any:
+    """从分组键里取一个维度绑定的值（多列用 ``.`` 连接）。"""
+    columns = binding_columns(spec)
+    if len(columns) == 1:
+        return key[group_by.index(columns[0])]
+    values = []
+    for column in columns:
+        value = key[group_by.index(column)]
+        if value is None:
+            raise RuleError(
+                f'维度绑定 {placeholder}→{spec} 的分组键 {dict(zip(group_by, key))} '
+                f'里没有列 {column} 的值'
+            )
+        values.append(str(value))
+    return '.'.join(values)
+
+
 def point_timestamp(ctx: RuleContext, index: Optional[int]) -> int:
     """数据时间戳：A 类取与 value 同一下标的 ``timestamps[i]``，其余取采集完成时刻。
 
diff --git a/tpops_kafka_collector/mapping/table.py b/tpops_kafka_collector/mapping/table.py
index f35b4e9..956e0e7 100644
--- a/tpops_kafka_collector/mapping/table.py
+++ b/tpops_kafka_collector/mapping/table.py
@@ -133,6 +133,35 @@ def build_alarm_definition(
     )
 
 
+def build_event_alarm_definition(
+    alarm_id: Any,
+    alarm_name: Any,
+    event_metric: Mapping[str, Any],
+) -> AlarmDefinition:
+    """事件类告警（**不挂靠客户指标**）的兜底定义。
+
+    统一指标名**表驱动** —— ``event_metric`` 来自 ``alarm_rules.json`` 的
+    ``event_metric`` 段（``{"metric_code": ..., "metric_name": ...}``），
+    代码里不写死任何客户口径。
+
+    平台新增告警（``alarm_rules.json`` 里还没有定义）时用它兜底：告警种类
+    由 ``alarmId`` / ``alarmName`` / ``alarmContent`` 承载，不靠 ``metricCode``
+    区分 —— 所以统一指标名不会丢信息。
+    """
+    return AlarmDefinition(
+        alarm_id=str(alarm_id),
+        alarm_name=str(alarm_name or ''),
+        alarm_kind='',
+        default_level='',
+        default_priority=None,
+        threshold='',
+        metric_code=str((event_metric or {}).get('metric_code') or ''),
+        metric_name=str((event_metric or {}).get('metric_name') or ''),
+        value_source='alarm_items_sum',
+        linked_customer_metric=None,
+    )
+
+
 class MappingTable:
     """三份规则文件的只读视图。
 
diff --git a/tpops_kafka_collector/mapping/validation.py b/tpops_kafka_collector/mapping/validation.py
index 554fda1..25f572d 100644
--- a/tpops_kafka_collector/mapping/validation.py
+++ b/tpops_kafka_collector/mapping/validation.py
@@ -29,6 +29,9 @@ if TYPE_CHECKING:  # pragma: no cover - 只为类型注解，运行期不导入
     from .loader import CustomerMetric, ObservedColumns
 
 __all__ = [
+    'check_effective_rule_count',
+    'check_excluded_metrics_absent',
+    'check_metric_code_uniqueness',
     'check_placeholder_bindings',
     'check_providable_coverage',
     'check_rule_entries',
@@ -78,13 +81,16 @@ def _dimension_column_problems(
         available = merged_views.get(view)
         if available is None:
             continue
-        for placeholder, column in sorted((rule.dimension_bindings or {}).items()):
-            if column not in available:
-                problems.append(
-                    f'{rule.metric_code} 的维度绑定 {placeholder}→{column} '
-                    f'引用了不在实测留档视图 {view} 实际列里的列'
-                    f'（实际列有 {sorted(available)}）'
-                )
+        for placeholder, spec in sorted((rule.dimension_bindings or {}).items()):
+            # 绑定值可能是点号串（``schemaname.relname``）—— 那是**两个列**，
+            # 逐个列校验。
+            for column in spec.split('.'):
+                if column not in available:
+                    problems.append(
+                        f'{rule.metric_code} 的维度绑定 {placeholder}→{spec} '
+                        f'引用了不在实测留档视图 {view} 实际列里的列 {column}'
+                        f'（实际列有 {sorted(available)}）'
+                    )
     return problems
 
 
@@ -224,3 +230,69 @@ def check_placeholder_bindings(rules: Sequence[MetricRule]) -> List[str]:
                 f'与指标名里的占位符 {sorted(in_name)} 不一致'
             )
     return problems
+
+
+# -- 校验 5/6/7：生效口径（排除清单 / 唯一性 / 条数）---------------------
+def check_excluded_metrics_absent(
+    rules: Sequence[MetricRule],
+    excluded: Sequence[Any],
+) -> List[str]:
+    """排除清单里的指标码**一个都不在**生效映射表里。
+
+    排除发生在**构建期**（生成器跳过清单里的指标码），所以生效映射表里
+    出现被排除的码 = 有人绕过生成器手改了映射表 → 启动失败并打印差异。
+    """
+    excluded_codes = set(
+        item.metric_code if hasattr(item, 'metric_code') else item
+        for item in excluded or ()
+    )
+    if not excluded_codes:
+        return []
+    leaked = sorted(
+        rule.metric_code for rule in rules if rule.metric_code in excluded_codes
+    )
+    if not leaked:
+        return []
+    return [
+        '被排除清单剔除的指标码仍出现在生效映射表里：%s'
+        '（排除应在构建期生效，运行期没有丢弃分支）' % leaked
+    ]
+
+
+def check_metric_code_uniqueness(rules: Sequence[MetricRule]) -> List[str]:
+    """生效规则的 ``metric_code`` 互不重复。
+
+    **唯一性只对指标规则要求** —— 告警定义靠 ``alarm_id`` 唯一定位，
+    多条事件类告警共用同一个 ``metricCode`` 是**允许**的（告警统一指标名）。
+    """
+    seen = {}  # type: Dict[str, int]
+    for rule in rules:
+        seen[rule.metric_code] = seen.get(rule.metric_code, 0) + 1
+    duplicated = sorted(code for code, count in seen.items() if count > 1)
+    if not duplicated:
+        return []
+    return [
+        '生效规则的 metric_code 有重复（每个指标码只能有一条规则）：%s' % duplicated
+    ]
+
+
+def check_effective_rule_count(
+    rules: Sequence[MetricRule],
+    expected: Any = None,
+) -> List[str]:
+    """生效规则条数与排除清单声明的生效条数一致。
+
+    ``expected`` 来自**排除清单数据文件**的 ``counts.effective_total``
+    （客户指标库 101 条 − 排除 14 条 = 生效 87 条）—— 条数不写死在代码里。
+    清单没声明时只做「不重复」的兜底，不报条数问题。
+    """
+    if expected is None:
+        return []
+    actual = len(rules)
+    if actual == expected:
+        return []
+    return [
+        '生效指标规则条数不对：期望 %d 条，实际 %d 条'
+        '（口径见 excluded_metrics.json 的 counts.effective_total）'
+        % (expected, actual)
+    ]
