# wave-4 实施报告 W4-A —— 交付文档主体

工作树：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`
分支：`tpops_metric_dev`（**未 commit / 未 add**，由控制器统一提交）

---

## 交付物

| 文件 | 行数 | 覆盖了什么 | 生成方式 |
|---|---|---|---|
| `docs/交付-指标口径说明.md` | 272 | **98 个可提供指标各一句口径**（4 个取数通道分节，每条 11 列）；含「资源粒度与多 master 归集口径」专章（`node_scoped` / `master_view` / node_id 去重 / 值与资源名同源 / A 类多 series / 差分基线按节点隔离）；13 种取值规则类型说明；通用约定；待客户确认项 | **脚本生成**：`tools/build_delivery_docs.py` |
| `docs/交付-不可提供指标清单.md` | 79 | **3 条**不可提供指标（`gaussdb.Fastpath_function_call` / `gaussdb.Disabled_conn` / `gaussdb.sendAgentName`），每条含「客户要什么 → 为什么给不了（技术原因，引映射表 `unsupported_reason`）→ 能不能变通」；写明采集时**丢弃并计数**（`dropped_unsupported = 3`） | **脚本生成** |
| `docs/交付-客户指标英文名核对清单.md` | 87 | **14 个指标 / 36 处 OCR 疑似错字**（英文名列）+ **2 处**中文名/英文描述列错字 + 命名风格不一致 3 类 + 自动规则误报附录 4 条 + 结论 | **脚本生成**（自动检测 + 人工复核表） |
| `tools/build_delivery_docs.py` | 1209 | 生成脚本（表驱动、幂等、带 `--check` 模式）；含 markdown 表格解析函数供守卫测试复用 | 新建 |
| `tests/unit/test_delivery_docs.py` | 595（其中我的部分 1~380 行 / 21 个用例） | 交付物与真相源一致的守卫 + 防恒绿自检 | 新建（**W4-B 后续把他们的第 6 节追加进同一文件**，见「偏离说明」） |

---

## 守卫测试（`tests/unit/test_delivery_docs.py`）

判据（`check_*` 函数）既用于真实文档，也用于**合成违规样本**，所以判据放松时一定变红。

### 用例清单（W4-A 部分，21 个）

| # | 用例 | 卡的是什么 |
|---|---|---|
| 1 | `test_metric_spec_covers_all_enabled_metrics` | 口径说明覆盖全部 **98** 个可提供指标，一个不漏一个不多 |
| 2 | `test_metric_spec_rows_have_all_columns` | 每行的取值规则 / 刷新周期 / 资源粒度 / 归集口径等必需列**不能空** |
| 3 | `test_metric_spec_only_mentions_known_metrics` | 文档里出现的指标名**确实存在于映射表**（含平台侧 `tpops_source`） |
| 4 | `test_metric_spec_documents_node_scoped_semantics` | `node_scoped` / `master_view` / `master CN` / 去重 / `node_id` 都要在文档里讲清 |
| 5 | `test_metric_spec_value_rules_are_described` | 取值规则列不能退化成 `None` 或「规则类型 X」（**生成脚本重构踩过的坑，加了专门守卫**） |
| 6 | `test_unsupported_doc_lists_exactly_disabled_metrics` | 不可提供清单**恰好 3 条**，且与 `enabled=false` 的**完全一致**（任务书点名） |
| 7 | `test_unsupported_doc_explains_reason_and_workaround` | 每条都要有「为什么给不了」+「能不能变通」，理由**引用映射表原文** |
| 8 | `test_unsupported_doc_states_drop_count` | 写明丢弃计数 `dropped_unsupported = 3` |
| 9 | `test_ocr_entries_exist_in_customer_xlsx` | OCR 清单每条都能在**客户 xlsx** 里找到对应行（防凭空编） |
| 10 | `test_ocr_checklist_matches_generator_review` | 清单与生成脚本的复核表逐条一致（不漏、不多写） |
| 11 | `test_ocr_checklist_has_suggestion_and_impact` | 每条都有建议拼写 + 影响面，影响面要说明是否影响取值 |
| 12 | `test_detector_finds_known_ocr_typos` | 自动检测器能抓到 `ansactions` / `mmitted` / `llback` / `dex` / `gments` |
| 13 | `test_ocr_review_entries_are_corroborated` | 人工复核表里每个片段，检测器都要能给出候选（复核表不能凭空调） |
| 14 | `test_other_docs_only_mention_known_metrics` | 另两份文档里也不能出现映射表没有的指标名 |
| 15 | `test_docs_match_generator_output` | 文档必须与生成脚本输出一致（手改后脱节会红） |
| 16 | `test_generator_is_idempotent` | 脚本连跑两次输出完全一样 |
| 17 | `test_guard_detects_missing_metric_row` | **自检**：删掉一行指标 → 守卫必须报缺失 |
| 18 | `test_guard_detects_extra_metric_row` | **自检**：塞一个映射表没有的指标 → 守卫必须报多余 |
| 19 | `test_guard_detects_incomplete_row` | **自检**：清空某行「取值规则」→ 守卫必须报缺字段 |
| 20 | `test_guard_detects_wrong_unsupported_list` | **自检**：不可提供清单塞一条可提供指标 → 守卫必须报多余 |
| 21 | `test_guard_detects_unknown_ocr_entry` | **自检**：OCR 清单编一个客户表没有的名字 → 守卫必须抓到 |

（W4-B 追加的第 6 节另有 8 个用例 —— 部署说明 / `.env.example` 与 `config.py` 的一致性守卫。）

### 变异验证记录（真实输出）

**(a) 合成违规样本 → 守卫全部变红**

```
$ conda run -n tpops python /tmp/mutation_check.py
=== 变异 1：从口径说明里删掉一行指标（总数少 1、名单少 1）===
  严格守卫（比名单）: RED —— 报缺失 ['GAUSSDB.Buffers_Alloc']
  放松守卫（只比数量）: RED（条数 97 vs 98）

=== 变异 2：把一行指标换成映射表里没有的指标（总数不变、名单变了）===
  严格守卫（比名单）: RED —— 报缺失/多余 ['GAUSSDB.Buffers_Backend'] / ['gaussdb.Not_A_Real_Metric']
  放松守卫（只比数量）: GREEN —— 抓不到被替换的 GAUSSDB.Buffers_Backend（条数 98 vs 98，数量相同 → 抓不到）

=== 变异 3：不可提供清单里塞一条可提供指标（总数变 4）===
  严格守卫（比名单）: RED —— 报多余 ['gaussdb.Tps']
  放松守卫（只比数量 3 条）: GREEN —— 抓不到多出来的 gaussdb.Tps（条数 3）

=== 变异 4：OCR 清单里编一个客户表里没有的指标名 ===
  严格守卫（查客户 xlsx）: RED —— 报 ['gaussdb.Fake_Metric']

=== 变异 5：把一行指标的「取值规则」清空 ===
  严格守卫（查字段齐全）: RED —— 报 ['`gaussdb.Deadlocks` / 取值规则']
```

> 变异 2 与变异 3 就是任务书举例的「只查数量不查名单」：**放松后守卫变绿（抓不到）**，说明名单级判据是必要的。

**(b) 变异生成脚本本身 → 守卫变红**（把 `_describe_row_rule` 的 `aggregate` 分支改成提前 `return None`）

```
--- 变异后（应红）---
FAILED tests/unit/test_delivery_docs.py::test_metric_spec_value_rules_are_described
FAILED tests/unit/test_delivery_docs.py::test_docs_match_generator_output - A...
2 failed, 27 passed in 0.70s
--- 恢复后（应绿）---
29 passed in 0.64s
```

---

## 口径说明的生成方式

- 脚本：`tools/build_delivery_docs.py`（`python tools/build_delivery_docs.py`，另有 `--check` 只比对不写）
- 数据源（**唯一真相源**）：`tpops_kafka_collector/mapping/data/metric_rules.json`（101 条：98 enabled + 3 disabled）、
  `tpops_kafka_collector/mapping/data/merged_sql.json`（11 条合并 SQL 视图的 `source_view` / 刷新档位）、
  `高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx`「指标库」sheet（**按表头名取列**，参考 `tools/xlsx_columns.py`，未按列序号）
- **表驱动**：文档内容全部由映射表字段推导，代码里没有硬编码任何指标清单

| 文档列 | 取自 |
|---|---|
| 客户指标英文名 | `metric_code` |
| 中文名 | `metric_name` |
| 取数通道 | `source.method`（命令中心SQL / 监控指标接口 / 诊断优化接口 / 实例接口） |
| 来源 | `source.api` + `source.sql_key`（→ `merged_sql.json` 的 `source_view`、刷新档位）或 `source.tpops_source` |
| 取值规则 | `value_rule.type` + `params`（13 种规则各有一段人话模板：`raw` / `raw_filtered` / `aggregate` / `counter_delta` / `ratio_across` / `series_point` / `enum_map` / `gauge` / `top_n` / `raw_rows` / `collection_count` / `text_prefix_count` / `instance_field`） |
| 刷新周期 | `collect_cycle.value` + `.unit` |
| 资源粒度 | `resource_granularity`（instance/table/database/port）+ `placeholders` |
| 归集口径 | `node_scoped` + `master_view`（+ `node_scoped=false` 时写明「挂第一个 master CN」） |
| 维度 | `placeholders`（渲染成 `{#NAME}` / `{#DATNAME}` / `{#PORT}`） |
| 备注 | `note` |

### 幂等验证（真实输出）

```
$ conda run -n tpops python tools/build_delivery_docs.py
已生成：docs/交付-不可提供指标清单.md（79 行）
已生成：docs/交付-客户指标英文名核对清单.md（87 行）
已生成：docs/交付-指标口径说明.md（272 行）

$ md5sum -c /tmp/before.md5
docs/交付-不可提供指标清单.md: OK
docs/交付-客户指标英文名核对清单.md: OK
docs/交付-指标口径说明.md: OK

$ conda run -n tpops python tools/build_delivery_docs.py --check
一致：docs/交付-不可提供指标清单.md
一致：docs/交付-客户指标英文名核对清单.md
一致：docs/交付-指标口径说明.md
```

> 说明：任务书给的验收命令是 `git diff --stat docs/` 应为空。**本工作树里三份文档是新增未跟踪文件**，
> `git diff --stat docs/` 只会显示 **W4-B 并发修改**的 `docs/开发方案-GaussDB指标与告警采集.md`
> （`1 file changed, 39 insertions(+), 21 deletions(-)`），与我的三份文档无关；
> 重跑生成脚本后我这三份文档 **md5 不变**（上面的 `md5sum -c` 全 OK）。

---

## OCR 错字清单

- **识别出 14 个指标 / 36 处片段**（英文名列）+ 2 处中文名/英文描述列错字；其余 87 个指标名未发现疑似错字。
- **判据**（写在文档第 2 节，也实现在 `detect_ocr_findings`）：
  1. 把指标英文名按 `.` / `_` 切成词；
  2. 候选词表 = 全表 101 个指标名里的词 ∪ 本指标英文描述里的词；
  3. 某词若是候选词**删掉 1~2 个字符**得到的子序列，且差异**不是**英语屈折变化
     （-s / -es / -ed / -d / -ing / -n / -en），判为疑似 OCR 缺字；
  4. 候选词本身若又是别的词的截断，则不作候选（只保留最完整的写法）。
  5. 自动命中后**逐条人工复核**，误报剔掉并写进文档附录。
- **例子（5 个）**

| 客户表里的英文名 | 疑似问题 | 建议 |
|---|---|---|
| `GAUSSDB.Number_of_ansactions_mmitted_r_cond` | `ansactions`→`transactions`（少 tr）、`mmitted`→`committed`（少 co）、`r_cond`→`per_second` | `GAUSSDB.Number_of_transactions_committed_per_second` |
| `GAUSSDB.Number_of_llback_ansactions_r_cond` | `llback`→`rollback`（少 ro）、`ansactions`→`transactions` | `GAUSSDB.Number_of_rollback_transactions_per_second` |
| `gaussdb.{#NAME}.number.Of.dex.ples.ad` | `dex`→`index`（少 in）、`ples`→`tuples`（少 tu）、`ad`→`read`（少 re） | `gaussdb.{#NAME}.number.Of.index.tuples.read` |
| `GAUSSDB.Checkpoint_count_eckpoint_gments` | `eckpoint`→`checkpoint`（少 ch）、`gments`→`segments`（少 se） | `GAUSSDB.Checkpoint_count_checkpoint_segments` |
| `GAUSSDB.Checkpoint_count_eckpoint_meout` | `eckpoint`→`checkpoint`、`meout`→`timeout`（少 ti） | `GAUSSDB.Checkpoint_count_checkpoint_timeout` |

- **措辞**：文档全程用「疑似 / 建议 / 请核对」，不写「你们错了」；并明确写清
  **「我们实际用的名字」一列与「客户表里的英文名」目前完全相同**（采集端按客户表原文投递），
  客户确认后只改 `metric_rules.json`（表驱动，代码不动），届时这一列才会出现差异。
- **影响面**：全部标注为「仅 `metricCode` / `metricName` 的拼写，取值口径、维度、刷新周期均不变」，
  并在结论里写明**本次核对没有发现取值口径错误**。

---

## 实测结果

```
$ cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev

$ conda run -n tpops python -m pytest -q tests/unit/test_delivery_docs.py
.............................                                            [100%]
29 passed in 0.63s

$ conda run -n tpops python -m pytest -q
...................................................................      [100%]
1723 passed in 85.12s (0:01:25)

$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest -q tests/test_py37_compat.py
............                                                             [100%]
156 passed in 1.55s

$ conda run -n tpops python tools/build_delivery_docs.py
已生成：docs/交付-不可提供指标清单.md（79 行）
已生成：docs/交付-客户指标英文名核对清单.md（87 行）
已生成：docs/交付-指标口径说明.md（272 行）

$ md5sum -c /tmp/before.md5
docs/交付-不可提供指标清单.md: OK
docs/交付-客户指标英文名核对清单.md: OK
docs/交付-指标口径说明.md: OK

$ conda run -n tpops python tools/build_delivery_docs.py --check
一致：docs/交付-不可提供指标清单.md
一致：docs/交付-客户指标英文名核对清单.md
一致：docs/交付-指标口径说明.md
```

- 全量 **1723 passed**（门禁要求 ≥1692；新增 29 个用例，无回归）。
- `tests/test_py37_compat.py` **156 passed** —— 新增的 `tools/build_delivery_docs.py` 与
  `tests/unit/test_delivery_docs.py` 都在该门禁的扫描范围内（3.7 语法、禁用 API、内建泛型注解全部通过）。
- 未新增任何依赖（只用标准库 + 已装的 `openpyxl`）。

---

## 发现但未修的问题

1. **`tests/unit/test_delivery_docs.py` 被 W4-B 同时写入** —— 双方都被指派创建这一个文件，
   W4-B 把他们的「部署说明 / `.env.example` 与 `config.py` 一致性」守卫作为第 6 节**追加**到了
   我这份文件的末尾（我的部分是第 1~5 节）。当前**两段共存、29 个用例全绿**，没有覆盖冲突，
   但提交时要注意这是一个文件里的两批内容。
2. **OCR 自动检测规则有已知边界**：会把 `in`（`Idle_in_transaction`）、`write`（`Blk_write_time`）
   判为可疑（`in`⊂`info`、`write`⊂`written`）。已人工复核为正常写法，并写进文档第 6 节附录，
   用来证明判据不是盲用 —— 但规则本身仍有误报，靠人工复核兜底。
3. **`gaussdb.{#NAME}.…` 与 `GAUSSDB.{#NAME}.…` 两套同义指标**（22 条表级指标，10 条带疑似 OCR 错字）：
   客户表里中文名描述一套、英文名描述一套，看起来是同一批指标的两次登记。
   已在文档第 5 节列为「命名风格不一致」请客户确认是两条独立指标还是重复登记 —— **未替客户下结论**。
4. **客户指标名里的占位符写法 `gaussdb_{#PORT}port.status`**（`{#PORT}` 后又跟了一个字面量 `port`），
   替换后形如 `gaussdb_5432port.status`，读起来别扭但**不是 OCR 错字**，未列入错字清单。
5. `docs/README.md`（文档总索引）里还没有这三份新文档 —— 该文件不在我的归属清单内，留给控制器 / W4-B。

---

## 偏离说明

1. **文件行数**：`tools/build_delivery_docs.py` 1209 行，超过「单文件 < 400 行」的约定。
   该约定针对**采集端生产代码**（本次**未改任何生产代码**）；仓库既有 `tools/` 脚本同样超行
   （`build_itscada_mapping.py` 776 行、`build_kafka_mapping.py` 891 行），
   脚本内已把 13 种取值规则描述、5 段章节渲染拆成独立函数（除 `describe_value_rule` 家族外均 < 50 行）。
   若控制器希望收敛到 400 行以内，可再拆成 `tools/delivery_docs/*.py` —— **未做**，因为超出我被授权的新建文件清单。
2. **共享文件**：见上「发现但未修的问题」第 1 条 —— `tests/unit/test_delivery_docs.py` 现在含 W4-A 与 W4-B 两部分。
3. **未 commit**：按任务书要求，全程只写工作树，**没有 `git add` / `git commit` / 改分支 / 动 HEAD**。
4. **未触碰他人文件**：`docs/交付-部署运行说明.md`、`.env.example`、`docs/开发方案-GaussDB指标与告警采集.md`、
   `tpops_kafka_collector/mapping/README.md`、`ids.py`、`models.py` 均**只读未改**
   （`git status` 里它们显示为 modified，那是 W4-B 的改动）。
5. **未改**：`execution-contract.md` / `tasks.md` / `specs/**` / `data/**` / `changes/**`（除本报告文件）。
