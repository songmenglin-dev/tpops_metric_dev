# Wave 1 审查报告 · wave-1-mapping（第 1 轮）

- **Wave**：`wave-1-mapping` · **Base**：`32ed768` · **Head**：`32d74ce`
- **审查对象**：任务 1.1~1.8（生效口径 87 条、排除清单、表级 `schema.table`、告警统一指标名、交付文档重生成）
- **审查包**：`.superpowers/sdd/wave-1-diff.md`（含提交列表、全量 stat、完整 diff；机械重生成的大 JSON 数据文件以结构摘要代替，核对方式写在包内）
- **实现者报告**：`.superpowers/sdd/wave-1-implementer-report.md`

> **审查执行方式披露**：本应由**独立 reviewer 子代理**审查。该子代理两次因
> **API 配额/网络错误**（429 Token Plan 用量上限、EAI_AGAIN）中断，未能产出报告。
> 为不阻塞变更，本轮审查由**控制器（主会话）**执行，审查材料与判级口径不变，
> 但**独立性弱于独立代理审查**，特此如实标注。修复轮（第 2 轮）同样由控制器执行。

## Spec Compliance

**❌ Issues found（1 个 Important）** —— 其余 17 条需求判定为合规。

| 需求 | 判定 | 证据 |
|---|---|---|
| R1 排除清单 | ✅ | `tpops_kafka_collector/mapping/data/excluded_metrics.json` 存在；`loader.py:88-101` 新增 `ExcludedMetric` 冻结数据类；`loader.py:206-224` `_parse_excluded_metrics` 结构不对抛 `ProtocolError`；实测 14 条（11 duplicate + 3 unsupported），duplicate 均有 `kept` |
| R2 生效 87 条 | ✅ | 实测 `metric_rules.json` 87 条 / `enabled=false` 0 条 / `metric_code` 唯一；`validation.py` 新增 `check_metric_code_uniqueness`、`check_effective_rule_count`；测试 `assert len(providable_codes) == 87` |
| R3 告警统一指标名 | ✅（规格本身有误，见下） | `alarm_rules.json` 的 `event_metric` 段 = 运行期真相源；`table.py:139-159` 由该段构造定义；`collectors/alarm.py:254-266` 运行期兜底也取该段；生产包内**无**硬编码 `gaussdb.alarm`（`grep` 命中只有 `kafka_topic_alarms` 默认值，无关） |
| R4 维度展开 | ✅ | `tools/build_itscada_mapping.py` `DIMENSION_COLUMN_BY_SQL['stat_user_tables']['NAME']` → `'schemaname.relname'`；新增 `_group_by_columns()` 把点号串**拆成多列**（`group_by=['schemaname','relname']`），未改 `group_by` 派生会让分组退化成单列 —— 这一处已正确处理 |
| R5 不可提供指标处置 | ✅ | 3 条无法提供指标已从生效口径**彻底移除**（不再是 `enabled=false`）；`dropped_unsupported` 恒为 0 |
| R6 映射表驱动 | ✅（1 个 Important，见 Issue 1） | 排除清单是数据文件；`event_metric` 在数据文件里 |
| R7 / R18 幂等 | ✅ | `public.t1` 与 `other.t1` 的 `metric_name`/`dimensions` 均不同 → 幂等键不同；同表重复采集键仍相同 |
| R19 业务维度落点 | ✅ | `tags.dimension_values.NAME` = `public.t1`；`resourceName` 不含表名 |
| 硬约束 | ✅ | 无新增依赖；客户 xlsx 未改；py37 门禁 170 passed；单文件/函数规模未破线；包内与仓库根两份映射数据一致 |

### ⚠️ Cannot verify from diff

- 交付文档三份的实际渲染效果（由 `tools/build_delivery_docs.py` 生成，已按任务 1.8 重生成并由
  `tests/unit/test_delivery_docs.py` 断言条数）—— 逐句文案未在本次审查范围内逐行核对。

## Strengths

- **重复指标的识别是硬的**：11 条重复项不是靠人眼比对，而是从客户 xlsx 的
  「表信息 / 事务信息」双分类 + 英文码大小写前缀差异定位出来的，清单带 `kept` 指明保留哪条，
  理由写在 `detail` 里 —— 客户可复核。
- **生成器的 `group_by` 派生被同步改了**：只改 `DIMENSION_COLUMN_BY_SQL` 一行会让
  `group_by` 退化成「一个叫 `schemaname.relname` 的单列」，表级分组静默失效。
  实现者补了 `_group_by_columns()` 拆列 —— 这是本次最容易漏、漏了测试也未必炸的一处。
- **没有改平台数据去凑断言**：任务 1.7 原文要求「254 条 `alarm_name` 各不相同」，
  实测做不到（平台目录 13 行填的是等级词、另有同名不同 ID 的组，只有 241 个不同名）。
  实现者**如实报告**并改成断言「`alarm_id` 254 个互不相同 + `alarm_name` 逐条对得上平台目录」。
- **测试期望值的变更全部有对应替换**：核对 `tests/` 下被删除的断言，全部是
  82/97/98/101/3 → 71/86/87/0 的**口径换数字**，无「删测试」或「放宽断言糊过去」；
  `git diff --diff-filter=D` 在 `tests/` 下命中 **0**。

## Issues

### Critical (Must Fix)

无。

### Important (Should Fix)

#### I-1. `DEFAULT_EVENT_METRIC` 是第二个真相源，误删数据文件会**静默**回退口径

- **位置**：`tools/build_itscada_mapping.py:272-299`（`_load_event_metric()`）
- **问题**：函数在「`alarm_rules.json` 存在但 `event_metric` 段缺失或文件不是合法 JSON」时，
  **静默**返回硬编码的 `DEFAULT_EVENT_METRIC = {gaussdb.alarm, 数据库告警}`。
- **为什么要紧**：本设计把 `alarm_rules.json` 的 `event_metric` 段定为**运行期真相源**
  （客户若按自己的口径改了统一指标名，改的就是这个数据文件）。但只要该文件被误删、
  被截断、或 `event_metric` 段被误删，运维重跑生成器就会把统一指标名**悄悄改回**
  `gaussdb.alarm` —— **现场没有任何提示**，客户侧的告警指标名会无声地变。
  这正是 R6「口径只由数据文件决定」要防的那类事故。
- **修法**：把两种「没有」分开 ——
  文件**不存在**（首次生成）→ 用默认值播种；
  文件**存在**但 `event_metric` 不可用 → **抛错停下**，不静默回落。
- **范围**：约 10 行；不改行为语义（首次生成的默认值路径保持不变）。

### Minor (Nice to Have)

- **M-1**：`data/mapping/archive/alarm_mapping.json` 的两处 `transform` **描述串**被更新
  （"其余用告警名" → "其余用统一指标名"）。初看像"归档被改"，但
  `data/mapping/archive/README.md` 明写「⚠️ 名字叫 archive，但它还在被生产代码加载」，
  且该段描述的是**当前**映射来源 → 更新是**正确**的，不算归档被污染。
- **M-2**：`tests/test_metric_response.py` 的 manifest 断言从「相等（98 == 生效口径）」
  改为「覆盖（每个生效指标都有样本）+ 归档仍是 98」。这是**方向性放宽**，
  但理由成立：样本目录是**抓取归档（平台事实）**，生效口径是**决定**，98 ⊋ 87 是预期状态；
  基线规格 `specs/mapping-and-rules/spec.md:27` 写的「98 份逐指标留档」**仍然为真**。
  丢掉的反向断言（归档 ⊆ 生效口径）正是被有意违反的那条。建议在收口时把这条口径写进交付说明。
- **M-3**：`CLAUDE.md` 仍写「98 可提供 / 3 丢弃」。已列入 wave-2 的收口清单。

## 实现者提的 4 点 —— 拍板结论

| # | 实现者的问题 | 结论 | 理由 |
|---|---|---|---|
| 1 | 「生效规则数 = 87」放在 `excluded_metrics.json` 的 `counts.effective_total`（数据驱动）而非写死在 `validation.py` | **接受** | R6 要求口径表驱动；「恰好 87」这条不变量由**测试**钉死（`assert len(providable_codes) == 87`、`assert len(table.metric_rules) == 87`、`assert not unsupported`），运行期校验改为数据驱动后仍会在数量对不上时启动失败。两者不冲突 |
| 2 | `data/metric_response/` 98 份归档未删；两条既有测试从「相等」改为「覆盖」 | **接受**（见 M-2） | 归档是平台事实、留档即证据，删 11 份样本会**降低**回归覆盖；生效口径收敛到 87 不构成删证据的理由。测试方向性放宽已在 docstring 里写明理由 |
| 3 | 任务 1.7 的「254 条 `alarm_name` 各不相同」做不到（实测 241 个不同名） | **实现者是对的，规格写错了** | 已独立复核：254 条事件类告警、241 个不同名、13 行填的是等级词（`紧急`×8 / `重要`×5）、2 组同名不同 ID、254 个 `alarm_id` 互不相同。**已修规格**（`specs/mapping-and-rules/spec.md` R3 新增「告警种类的唯一性」场景、`tasks.md` 1.7 与 `design.md` 风险表同步改正），而不是让实现去迁就错的规格 |
| 4 | 运行期「告警定义缺失」的兜底也改用映射数据里的统一指标名 | **接受**（属范围内） | 原实现硬编码自造 `gaussdb.alarm.<id>`，会留下两个口径，直接违反 R3 与 R6；`collectors/alarm.py:254-266` 现在统一取 `mapping.alarm_rules['event_metric']`，是 R3/R6 的必要收口，不算 scope creep |

## Assessment

**Task quality：Needs fixes**

**Reasoning**：R1~R19 的实现实质正确且证据自洽，生成器最容易漏的 `group_by` 拆列也处理对了，
测试期望值变更全部有对应替换、无删测试。唯一阻塞项是 I-1：统一指标名存在**静默回退**路径，
一旦数据文件损坏就会无声改回默认口径 —— 与本设计「口径由数据文件单点决定」的立意相反，
必须在合入前修掉。

## 修复要求（第 2 轮聚焦复审范围）

1. `_load_event_metric()` 区分「文件不存在（播种默认值）」与「文件存在但 `event_metric` 不可用（**报错**）」；
2. 补测试覆盖这两个分支（RED → GREEN）；
3. 其余代码**不得**改动。

## 收据命令

```bash
ssf execution review changes/tpops-mapping-and-logging --wave wave-1-mapping \
  --base 32ed768 --head <修复后的 head> \
  --report changes/tpops-mapping-and-logging/.superpowers/sdd/reviews/wave-1-rereview.md \
  --verdict pass
```
