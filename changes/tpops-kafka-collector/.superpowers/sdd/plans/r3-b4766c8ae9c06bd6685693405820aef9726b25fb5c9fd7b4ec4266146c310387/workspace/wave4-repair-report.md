# wave-4 修复轮报告 —— 交付文档与实现对齐

> 本轮由**控制器**在实施方被 429 配额中断后接手完成（实施方已完成生成器与三份文档的
> 主体改造，崩溃前未写报告、未写守卫）。
> 工作树：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`，分支 `tpops_metric_dev`。
> **未 commit**（由控制器统一提交）。

## 一、逐条修复

### C-1【Critical】A 类归集口径写错 + 文档自相矛盾

- **根因**：`tools/build_delivery_docs.py` 的 `describe_collection` 只按 `node_scoped` 分支，
  **不区分通道** → 给 A 类（`node_scoped` 也是 `true`）套上了 B 类的
  「节点级：每个 master 各出一条」。
- **实现事实（核实过）**：A 类由 `mapping/rules_series.eval_series_point` 对
  `ctx.series` 的**每个键各出一条**消息，维度键 `COMPONENT_ID` 归入 `tags`，
  **不跨组件合成**；`node_scoped` 对 A 类不参与归集。
  审查方实测：`Standby_status` → **9 条**（9 个 DN，不是 3 个 master）、
  `QPS` → **6 条**（3 CN + 3 DN）。
- **改法**：`describe_collection` 改为**按通道分支**（`monitor_metrics` → 组件级；
  `command_center_sql` → 按 `node_scoped`；`diagnostic` / `instance` → 实例级），
  三份文档重新生成，§3 与 §5.2 不再矛盾。

### I-1【Important】刷新周期「与客户指标库一致」不成立

- **实现事实（核实过）**：**没有按指标的 `collect_cycle` 做产出闸门**。
  `schedule.py:142-159` 的 `compute_channel_plans` 按通道取**最高需求档（最短周期）**。
  实测通道周期：`monitor 30s / command_center 60s / diagnostic 120s /
  instance 3600s / alarm 30s`。
  **98 条里 65 条超服务**；声明 24 小时档的指标实际每 60 秒出一条（1440 倍）。
- **改法**：口径说明把「刷新周期」拆成**「声明周期」**与**「实际产出频率」**两列；
  后者由生成器**直接调用生产调度代码** `compute_channel_plans()` 取 —— 与实现同源。
  另加一节说明「声明周期当前不作为产出闸门」，并写明体量（**152,664 条/天/实例**）。

### I-2【Important】`cp /opt/tpops-collector/.env.example` 必然失败

- `.env.example` 在**仓库根**，不在 `tpops_kafka_collector/` 产物内。
- **改法**：部署步骤改为只拷 `tpops_kafka_collector/` 与 `requirements.txt`，
  `.env` 由运维按 §3 的 30 个变量表**就地创建**（`vi /opt/tpops-collector/.env`）。

### I-3【Important】`Total_pct` 备注「活跃会话数」实为全部会话数

- **改法**：生成器用 `NOTE_OVERRIDE` 覆盖该条备注（**映射表本身不动** ——
  `data/mapping/*.json` 是生成物，不手改）。

### I-4【Important】排障表引用了代码里不存在的日志

- **改法**：排障表改为只引用**生产代码里真实存在**的日志文案；
  并新增守卫 `test_troubleshooting_table_only_quotes_existing_log_messages` 长期盯着。

### I-5【Important】空转变量未披露 + 承诺了不存在的报错

- **核对结果（控制器重算全部 30 个键）**：未被生产代码消费的共 **3 个** ——
  `CMD_LIMIT`、`RETRY_MAX_ATTEMPTS`、`KAFKA_TOPIC_PARTITIONS`。
- **改法**：三个在部署说明的变量表里如实写明「**尚未被消费**，改动不影响当前运行行为」；
  并删除「分区数不符」这类代码里不存在的报错承诺。
  新增守卫 `test_unconsumed_env_vars_are_disclosed_as_inert` 长期盯着。

### I-6【Important】`POLL_INTERVAL_SECONDS` 的「周期 = 轮询间隔」不成立

- **改法**：描述改为「调度 **tick 粒度**，**不是**各通道的采集周期」，
  并写明各通道实算周期、到期判定基于单调时钟、调大只会让主循环变慢
  （超过某通道周期时该通道被拉长到整数倍）、调小不增加平台请求量。

### I-7 / I-8 / I-9【Important】OCR 清单的事实性错误

- **改法**：措辞改为**由脚本逐字符计算**（「第几位缺什么 / 第几位应该是什么」），
  生成器里**不手写**这类描述（注释明确写：手写就会写出「少了一个 m 和一个 t」这种
  和实际不符的话）；R074 的 `quential` → `sequential` 已列入清单。

## 二、守卫补强（本轮的核心）

审查方 16 条变异里 **6 条改坏文档后守卫仍全绿** —— 根因是守卫只验
「文档 == 生成脚本(映射表)」，**从不验「文档 == 实现代码」**。

**新增 5 条真相源是生产代码的守卫**：

| 守卫 | 真相源 | 抓什么 |
|---|---|---|
| `test_a_class_collection_semantics_match_the_implementation` | 生产采集器行为 | A 类口径必须含「组件」、**不得**含「每个 master 各出一条」 |
| `test_b_class_node_scoped_says_per_master` | 生产采集器行为 | B 类 `node_scoped` 必须含「每个 master 各出一条」 |
| `test_actual_output_frequency_matches_the_production_schedule` | `compute_channel_plans()` | 「实际产出频率」必须等于生产通道周期 |
| `test_troubleshooting_table_only_quotes_existing_log_messages` | 生产源码字符串常量 | 排障表引用的日志必须在代码里存在 |
| `test_unconsumed_env_vars_are_disclosed_as_inert` | `config.py` vs 生产源码 | 空转变量必须披露「改了也不生效」 |

**变异验证（控制器自做）**：

| 变异 | 结果 |
|---|---|
| A 类口径改回「每个 master 各出一条」 | `test_a_class_collection_semantics_...` **红** ✅ |
| 排障表塞入代码里不存在的日志「重试后成功」 | `test_troubleshooting_table_only_quotes_...` **红** ✅ |
| 「实际产出频率」改成声明周期 | 鉴别力验证：原文档 0 行不通过 → 变异后 1 行不通过 ✅ |

**顺带修掉一个会让守卫在目标解释器上等于空转的 bug**：
守卫里用 `ast.Constant` 取字符串常量，但 **py3.7.9 的 AST 里字符串是 `ast.Str`**
（统一到 `Constant` 是 3.8 才做的）—— 第一次跑时「所有日志文案都不存在」就是这么来的。
改成两种都认。**这是「守卫自己也要被验」的又一例**。

## 三、实测

```
pytest -q tests/unit/test_delivery_docs.py    34 passed（29 原有 + 5 新增）
pytest -q                                      见收尾输出
ruff check tpops_kafka_collector/              All checks passed
pytest -q tests/test_py37_compat.py            见收尾输出
tools/build_delivery_docs.py --check           三份「一致」
```

## 四、发现但未修

- `tools/build_delivery_docs.py` 已超 1200 行（T-37，登记在账本，收尾后的独立小变更）
- 30 个配置键里 3 个是空转（T-35，账本已登记）
- `activity_running` 的刷新档在两个数据文件里声明不一致（T-36，账本已登记）

## 五、偏离说明

- 实施方在 429 中断前完成了生成器与三份文档的改造，**但没写报告、没写守卫**；
  本轮由控制器接手补齐守卫与报告，并对关键结论（A 类归集行为、通道周期、
  空转变量清单）**逐条自己核实**，未采信实施方或审查方的任何未经复核的数字。

---

## 六、第二轮复审的 7 条 Important（守卫层）—— 控制器接手修完

第二轮复审结论：**fail（Critical 0 / Important 7）**，但
**9 条文档发现已全部闭环**（复审方自己重放生产流水线验的：A 类 11 行
`Tps`=3 / `QPS`=6 / `Standby_status`=9，98 行「实际产出频率」**全量**核对 0 处不符）。
剩下的 7 条**全在守卫层** —— 文档是对的，但**守卫守不住它**。

### 守卫为什么守不住（根因）

首轮那 6 条恒绿变异里 5 条仍绿。两处新守卫各有一个洞：

| 洞 | 事实 |
|---|---|
| 排障日志守卫 | 只取**最长**中文片段，<6 字的直接跳过 → 「命令中心 …… 重试后成功」（最长 5 字）能过 |
| 空转变量守卫 | 按「名字出现过」判消费 → `producer.py:213` 的 `ensure_topics()` **没有生产调用方**，却被判成已消费 |
| A 类守卫 | 纯关键词判断 → 「组件级：**所有组件求和后出一条**」这种语义错误能过 |

**共同根因：判据停在「关键词」层，没有下沉到「真相源 / 语义」层。**

### 修法（新增 6 条守卫 + 修 2 条）

| 守卫 | 判据 | 挡住谁 |
|---|---|---|
| `test_a_class_collection_forbids_synthetic_aggregation` | 禁止出现「合成一条 / 求和后 / 合并成一条…」 | M10、语义错误 |
| `test_troubleshooting_log_quotes_check_every_fragment` | **每一个** ≥4 字的中文片段都必须在源码里存在 | 洞1 |
| `test_deploy_required_flags_match_the_config_source` | 「必填」列必须与 `config.py` 的 `_require`/`_servers` 判定一致 | M3 |
| `test_deploy_exit_codes_exist_in_the_production_code` | 提到的退出码必须是代码里真实的 `EXIT_*` | M4 |
| `test_deploy_required_sections_are_present` | 必备小节**必须以标题行存在**（正文提一嘴不算） | M5 |
| `test_value_rule_column_matches_the_mapping_table` | 「取值规则」列必须等于**从映射表重新推导**的文本（不是「文档 == 生成器输出」那种自证） | M2 |
| `test_unconsumed_env_vars_are_disclosed_as_inert`（修） | 消费判据改为**调用图**（键名只出现在**从未被调用**的函数里 → 未消费）+ 显式登记 `KNOWN_INERT_ENV_VARS`（读了但无行为效果的一类，自动判据看不出来） | 洞2 |

### 变异电池（控制器自做，8 条）

| 变异 | 结果 |
|---|---|
| 【基线】无变异 | **全绿** ✅ |
| M2 改真相源 value_rule | 红 ✅ |
| M3 必填 是→否 | 红 ✅ |
| M4 退出码写错 | 红 ✅ |
| M5 删掉「优雅退出」整节（含标题行） | 红 ✅ |
| M9 删掉 `CMD_LIMIT` 的「未消费」披露 | 红 ✅ |
| M10 生成器让 A 类写成「合成一条」 | 红 ✅ |
| 洞1 排障表塞入代码里不存在的日志 | 红 ✅ |
| 洞2 `KAFKA_TOPIC_PARTITIONS` 的披露被删 | 红 ✅ |

**8/8 全部变红，基线全绿。**

### 顺带记两条「守卫自己也要被验」的实例

1. **py3.7.9 的 AST 里字符串是 `ast.Str` 不是 `ast.Constant`**（统一到 Constant 是 3.8 才做的）
   —— 我第一版守卫用 `ast.Constant`，在目标解释器上**等于空转**。
2. **副本污染**：变异电池第一版把仓库根的 `高斯DB-指标明细….xlsx` 与 `.env.example`
   漏拷了（点文件 + 中文名），导致**每条变异都带三个固定的红** ——
   差点让我把「污染」当成「守卫抓到」。**先跑基线**是变异电池的必要一步。

### 实测（修复后）

```
pytest -q tests/unit/test_delivery_docs.py   40 passed
pytest -q                                     1734 passed
ruff check tpops_kafka_collector/             All checks passed
pytest -q tests/test_py37_compat.py           156 passed
tools/build_delivery_docs.py --check          三份「一致」
```
