# 进度账本 —— 变更 tpops-kafka-collector

计划：sdd，**revision 3**（6 个 wave，依赖链 wave-0 → wave-1 → wave-2a → wave-2b → wave-3 → wave-4）
工作树：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`，分支 `tpops_metric_dev`

> **revision 2 → 3 的原因**：实施方在上一会话中单方面改了已批准的合同与 tasks.md
> （commit `5725ba0` / `0c72251`：ruff 门禁从全仓 `ruff check` 收窄为
> `ruff check tpops_kafka_collector/`），导致 `contract_hash` 与 `artifacts_hash` 双双失配，
> **执行计划 stale，`ssf execution review` 拒绝签发任何 receipt**（连 `fail` 都签不了）。
> 用户已裁定：**追认这次收窄**（仓库 357 条历史 lint 债务与本变更无关），
> 并要求 wave 0 内把新生产包自己的 lint 清零，让门禁真正成立。
> 合同与 tasks.md 的文字**不再改动**，避免再次 stale。

## 📍 交接说明（新会话从这里进）

**`changes/tpops-kafka-collector/.superpowers/sdd/HANDOFF.md`** —— 交接说明的入口。

进度快照（2026-11-23）：
wave-0 / wave-1 / wave-2a / wave-2b 的 receipt **全部 pass**；
**wave-3 已解锁（eligible: true）**，32 个任务已完成 28 个，
剩余 **3.1 调度器 / 3.3 入口装配 / 3.5 全量门禁**，之后是 wave-4 交付文档。

门禁：1295 passed · ruff 0 · py37 门禁 120 passed。

## Wave 0 —— wave-0-mapping-baseline（serial）

- [x] 0.1 建包并复制映射层            （edd7666）
- [x] 0.2 在包内加 merged_views 段与合并形态解码  （f13f137）
- [x] 0.2' 定向修复：is_retryable 走基类链等     （9c8e6b8）
- [x] 0.3 在包内修告警幂等键            （c8538e8）
- [x] 0.4 在包内修跨视图比率            （c8538e8；rows_by_key 已填，
                                          Total_pct/session_usage → 0.79，测试从留档推期望值）
- [x] 0.5 合并形态的列校验与证据重推导   （c8538e8）
- [x] 0.6 同步开发方案的契约章节        （da1328f）
- [x] 0.7 把依赖切到包内并删除参考副本   （18f1815）
- [x] 0.5' 告警 details 恒为数组，不省略字段（5b4cba1）

### Wave 0 审查状态

- 第 1 轮 receipt（revision 2）：**fail**（Important：`errors.py` 的 `is_retryable` 用类名精确相等，
  把 `ReadTimeout` / `ConnectTimeout` 判成不可重试）+ 波次未完成。
  报告：`.superpowers/sdd/reviews/wave-0-mapping-baseline.md`
- 修复轮次 1：已修复（9c8e6b8 走 MRO 基类链）+ 补齐 0.3/0.4/0.5/0.6/0.7。
  全量测试 238 passed；生成器幂等（重跑 `build_response_rules.py` 后 `git diff` 为空）。
- 定向复审（轮次 1，revision 2）：**fail**。报告
  `.superpowers/sdd/reviews/wave-0-mapping-baseline-rereview.md`
  —— 先前 Important 已解决且无回归；0.3~0.7 全部合规；但有两条 **plan-mandated Important**：
  - **I-1**：合同 Wave 0 完成标准「82 个 B 类指标的值等于期望值表」既未实现（只有 2 条
    `ratio_across` 验了值相等，其余 80 条只验了列名存在），也未显式记录延后。
    → 用户裁定：**现在就补**（独立期望值推导器 + 生成物 + 82/82 断言）。
  - **I-2**：合同自设的 `ruff check tpops_kafka_collector/` 门禁不成立（**44 errors**）。
    → 用户裁定：**追认收窄 + 现在把新生产包 lint 清零**。
  - 治理附注：合同是被实施方自己改的，已由用户追认；此后**执行中不得再单方面改合同与 tasks.md**。
- 该复审的 receipt **未能签发**（计划已 stale）→ 走了 `ssf execution revise` → **revision 3**，
  revision 2 的 receipt 全部作废，wave-0 在 revision 3 下从零开始走 review 门禁。

### 修复轮次 2（revision 3，串行派发，同分支避免 git 索引竞争）

- [x] **A**：新生产包 lint 44 → 0（I-2）+ `CLAUDE.md`/`README.md` 指向已删 `tools/mappings/`（M-1）
  + `decode_merged` 传空 `views` 抛 `DecodeError`（M-8） —— **完成（`afbb66f`）**
  简报：`plans/r3-…/workspace/repair-r2-a-lint-and-docs-brief.md`
  验证：ruff 退出码 0 / pytest 241 passed（基线 238 + 新增 3）/ py37 41 passed /
  生成器幂等（重跑 `build_response_rules.py` 后 `response_rules.json` 无 diff）
  A-2 的取舍：原 `tools/mappings/README.md` 已随目录删除，**搬进包内**为
  `tpops_kafka_collector/mapping/README.md`（而不是指向 git 历史）——
  里面是「怎么改规则」的活文档，指向历史等于让读者自己去考古。
- [x] **B**：82 个 B 类指标的期望值表（I-1）—— 独立期望值推导器 + 生成物
  `data/mapping/expected_values.json` + `tests/test_expected_values.py` 断言 82/82 相等
  简报：`plans/r3-…/workspace/repair-r2-b-expected-values-brief.md` —— **完成（`ec27614`）**
  验证：类型分布 raw 54 / aggregate 12 / counter_delta 5 / raw_filtered 4 / raw_rows 3 /
  top_n 2 / ratio_across 2 = 82，实测 82/82（逐条 × 3 个 master 节点 = 246 组值相等）；
  生成器幂等（重跑 sha256 不变）；全量 344 passed；py37 44 passed；ruff 0。
  控制器补充（`3a04aa5`）：合成守卫补大写样本，兑现「忽略大小写」的断言 ——
  修复前把 `str(actual).lower()` 去掉测试照样绿。
- [x] wave-0 在 revision 3 下的 review：base = `357ece4`，head = `3a04aa5`，
  报告 → `.superpowers/sdd/reviews/wave-0-mapping-baseline.md` —— **verdict: pass**
  （Critical 0 / Important 0 / Minor 7）。receipt 已签发，`execution show` 显示
  wave-0 `receipt=pass`、**wave-1 已解锁**。

### 复审提出的 Minor 处置台账

| 编号 | 内容 | 处置 |
|---|---|---|
| M-1 | 根文档指向已删 `tools/mappings/` | 修复轮次 2 任务 A —— **已修** |
| M-2 | `mapping/rules.py` 425 行 > 合同 400 行（与「复制不重写」互斥） | **留给 wave 1 任务 1.8**（规则引擎本就重写该文件） |
| M-3 | `ratio_across` 分母取不到返回 `0.0` 而非「不产出」 | **留给 wave 1 任务 1.8** |
| M-4 | 开发方案仍有 5 处指向已删目录的引用 | **留给 wave 4**（交付文档） |
| M-5 | 进度账本过期 | 已修（见上） |
| M-6 | 视图名提取逻辑两处各写一份 | 记录，不修（改动生成器有损幂等性的风险） |
| M-7 | 启动期校验默认读仓库根 194 KB 留档 | **wave 2a 决策**：留档是否复制进包 |
| M-8 | `decode_merged` 传空视图集静默返回零值 | 修复轮次 2 任务 A —— **已修** |
| M-9 | `RuleContext`/`MergedContexts` 不可变是浅层的 | 记录（与既有实现一致，非回归） |

### revision 3 审查新增的 Minor（7 条，全部不阻塞）

| 编号 | 内容 | 处置 |
|---|---|---|
| R-M1 | `raw_filtered`「首行 vs 末行」在留档上不可区分（每个 filter 恰命中 1 行）；且**规格未定义**多命中语义 | **留给 wave 1 任务 1.8**：补一条合成多命中守卫 + 规格写明「多命中取首个匹配行」。当前无生产风险（`pg_settings.name` 唯一） |
| R-M2 | **`top_n` 的排序实为空操作**：引擎用 `_to_number(row.get(sort_by))` 排序，而排序列 `query_start` 是**时间戳字符串** → 全部退化成 `0.0`，输出顺序 = 留档顺序（恰好正确，因为 SQL 自带 `order by`） | **留给 wave 1 任务 1.8**：规则引擎重写时按 `ORDER BY` 口径真正排序。当前不产出错值，不阻塞 |
| R-M3 | `raw` + `per_row` 的 12 条表级指标声明里带 `group_by: ["relname"]`，**引擎完全忽略** `group_by`（按「一行一条」执行）。留档 68 行 68 个不同 relname，两种理解等价 | **留给 wave 1 任务 1.8**：要么执行 `group_by`，要么从映射表删掉这个不生效的参数（后者更好） |
| R-M4 | `.spec-superflow.yaml` 的 `contract_hash`/`artifacts_hash` 仍是 revision 2 旧值 | **已处理**：签 receipt 前跑 `ssf state rebuild` 刷新，`state check` 现为 consistent |
| R-M5 | `mapping/rules.py` 423 行 > 400（= M-2） | 已在台账显式延期到 1.8，在 1.8 简报里写入「拆文件」要求 |
| R-M6 | 映射层覆盖率 82%~95%（`decode.py` 82%、`columns.py` 88%、`rules.py` 92%），**未达合同「≥95%」** | 该门槛是 **wave 3** 完成标准；wave 3 规划时预留补测（尤其 `decode.py` 错误分支与 `columns.py` 多节点并集分支） |
| R-M7 | R-M2 / R-M3 未进本台账 | 已补（本表） |

### 控制器裁决（修复轮次 2 遗留）

- **`ratio_across` 舍入位置**（实施方请裁决）—— **不改，规则层保留 `round(…,2)`**。
  理由：① 投递规格 `delivery-and-state/spec.md:67`「比率类 SHALL 保留 2 位小数」是
  **投递层**要求，序列化层已实现（`mapping/message.py:123 _as_float` → `round(float(value), 2)`）；
  ② 两条 ratio 指标的 `value_type` 都是「浮点」→ 走 `_as_float`，不存在被 `_as_int`
  截断成 0 的风险；③ 规则层的舍入**幂等**（对已 2 位的值再 round 不变），且它让期望值表
  比对的是**交付值本身**。→ 规格与实现**无不一致**。
- **`pg_stat_activity.state` 的两条指标恒为 0** —— **已澄清：不是可提供性问题。**

  实测留档 `activity_agg` 的 `state` 取值**只有 `{active, idle}`**，
  所以 `gaussdb.Lockwaiting`（`state_contains:["lock"]`）与
  `gaussdb.Waiting_conn`（`state_contains:["wait"]`）在留档上匹配 0 行 → 值为 0。

  控制器一度按**原生 PostgreSQL 9.2** 的 `state` 取值集推断「结构性不可提供」
  —— **这个推断是错的**：GaussDB 是深度改造过的 openGauss 系，
  `pg_stat_activity.state` **有额外的等待态**（`waiting on lock` 之类）。
  **用户澄清**：恒为 0 是因为**测试实例本来就没有死锁、没有等待连接的负载**，
  不是取不到值。

  → 结论：这两条指标**可提供**，映射表的实现（子串包含）也是对的，
  当前恒为 0 是**真实的 0**。**不列入 `open_items.json`**，
  在交付文档里写一句「当前测试实例无锁等待/等待资源负载，该指标恒为 0 属正常」。

  顺带记一条**口径备注**（不是缺陷）：`Waiting_conn` 的 `state_contains: ["wait"]`
  是**宽口径** —— 会把 `waiting on lock` / `waiting on sync` / `waiting on clog`
  等**所有**等待类状态都算进去，与它「等待资源的连接数」这个名称相符，
  但交付文档里值得写明。

## Wave 1 —— wave-1-skeleton-platform（parallel）

- [ ] 1.1~1.9（配置 / DTO+异常 / 重试 / 序列化 / 生产者 / 客户端 / 映射表加载 / 规则引擎 / 水位）
- 简报已生成：`workspace/task-1.{1..9}-brief.md`
- 共享执行约定：`workspace/wave1-execution-brief.md`（硬约束 / TDD / 1.7-2.1 拆分裁决 / 1.8 的 Minor 清单）

### 任务级状态

| 任务 | 状态 | 提交 |
|---|---|---|
| 1.1 配置加载与启动校验 | ✅ 完成（47 用例） | `e5d3dbb` |
| 1.2 DTO 与异常体系 | ✅ 完成（32 用例） | `e5d3dbb` |
| 1.7 映射表加载与启动校验 | ✅ 完成（15 用例） | `e5d3dbb` |
| 1.9 水位与差分基线持久化 | ✅ 完成（26 用例） | `1eb0b66` |
| 1.3 重试与退避框架 | ✅ 完成（32 用例，`retry.py` 覆盖率 100%） | `2d3bc39` |
| 1.4 JSON 序列化器 | ✅ 完成（32 用例，AST 唯一出口守卫） | `bc29c53` |
| 1.5 Kafka 生产者 | ✅ 完成（26 例纯 mock） | `172b93c` |
| 1.6 TPOPS HTTP 客户端 | ✅ 完成（41 例，`client.py` 覆盖率 99%） | `b71d65c` |
| 1.8 规则引擎（13 种 value_rule.type） | ✅ 完成（60 例，拆 9 模块） | `9c44453` |
| 控制器补充：认证静默期外露为配置项 | ✅ 完成 | `4b95fb4` |

**wave-1 全部 9 个任务已落地**，全量 **715 passed**，`ruff check tpops_kafka_collector/` 为 0，
py37 门禁 77 passed。审查：base `3a04aa5` / head `4b95fb4`，
diff `workspace/review-wave1-r3.diff`（13197 行），
报告 → `.superpowers/sdd/reviews/wave-1-skeleton-platform.md`。

### 控制器在本波次做的裁决与修复

- **1.7 / 2.1 依赖倒置**（已获用户确认）：1.7 做「复制七张映射表进包 + loader + 四类启动期校验」，
  2.1 退化为「防漂移同步测试 + 补齐 `merged_sql.json`/`sql_merge.sql` 的复制」。
  合同与 `tasks.md` **均未改动**。
- **幂等键集合改成真 LRU**（`92e3223`）：规格 `delivery-and-state/spec.md:170` 写的是 LRU，
  实现却是插入序淘汰（FIFO）—— 语义不等价。`add()` 遇重复键刷新新鲜度，
  `contains()` 是查询不刷新。TDD：先加两条会失败的测试。
- **`DecodeError` 收敛到 `errors.py`**（1.2 做的）：原先定义在 `mapping/decode.py`，
  若在 `errors.py` 另定义同名类会有两个不同的类，`except` 抓不住、`isinstance` 分裂。

### M-3（`ratio_across` 分母取不到 → 不产出）的涟漪 —— **留给 wave-1 审查处置**

1.8 按 M-3 把 `ratio_across` 从「分母取不到返回 `0.0`」改成「**不产出**」后，
`gaussdb.Total_pct` 与 `GAUSSDB.session_usage` 在**逐指标留档**回放下
`message_count` 从 **1 变成 0**（`data/mapping/kafka_field_mapping.json` 已重生成，
`sample_message` 变 `null`）。

- 这是**修对了**：原来的 `value: 0.0` 正是「分母取不到却静默出 0」的缺陷形态。
  合并形态留档下这两个指标有真实值（23/29 ≈ 0.79），逐指标留档里没有跨视图的分母，
  所以「不产出」是正确行为。
- **但 wave-0 的一条测试是照着缺陷行为写的**：
  `tests/test_kafka_mapping.py::test_aggregate_expansion_produces_one_message`
  断言 `Total_pct` 的 `message_count == 1` → 现在红。
  该测试要更新为「跨视图比率在逐指标留档下不产出消息」，
  并保留合并形态下有值（0.79）的证据。**不是把实现改回出 0.0。**

### 1.4（序列化器）留下的尾巴 —— **留给 wave-1 审查处置**

| 编号 | 内容 | 处置建议 |
|---|---|---|
| S-1 | `SerializerError` 定义在 `kafka/serializer.py` 内，未进 `errors.py` 异常体系 | 与 1.2 把 `DecodeError` 收敛到 `errors.py` 的做法不一致；建议统一进 `errors.py`（`MappingError` 子类，不可重试） |
| S-2 | 序列化器**硬编码**了字段表（`METRIC_FIELDS`/`ALARM_FIELDS` 等），而 1.4 的测试比对的是**仓库根** `data/mapping/itscada_field_map.json` | 运行时用的是硬编码，**包内副本**（1.7 复制的）没有守卫。需一条断言：「包内副本的字段表 == 序列化器硬编码的字段表」 |
| S-3 | `dumps()` 用紧凑分隔符（无空格） | 影响消息体**逐字节**格式。语义无差别（消费方解析 JSON），但属对外契约面 → 交付文档里写明；若客户要可读 JSON 需改 |
| S-4 | `allow_nan=False` → NaN 抛 `SerializerError` | 合理。但 wave 2 的采集器要把 NaN 当 `null` 处理，否则会在投递层炸 |

### 1.3（重试框架）的口径 —— 控制器已接受

抖动取「**只增不减**」（实际等待 ∈ [基准退避, 基准×(1+jitter_ratio)]），不是常见的 full jitter。
合同只写「指数退避 + 抖动」未指定形式，两种都是合法设计。若后续要求 full jitter，
只需改 `compute_delay` 一处并同步测试。

### ⚠️ 规格自相矛盾：「跨视图比率」的分母为 0 —— **控制器裁决 + 规格改写延后**

`specs/mapping-and-rules/spec.md:156` 写「分母为 0 时**结果为 0**，SHALL NOT 抛出除零异常」，
而同文件「缺值不产出假数据」要求取不到值时**不产出该条消息**、SHALL NOT 用 0 代替缺失值。
这两条在 **x/0** 上直接冲突。

**裁决（控制器）**：

| 情形 | 行为 | 理由 |
|---|---|---|
| 分母**取不到**（视图缺失 / 无该列 / 无数据） | **不产出** | 「缺值不产出假数据」 |
| 分母取到但**值为 0**（x/0） | **不产出** | 比率在分母为 0 时**无定义**；输出 `0.0` 等于把「算不出来」说成「真的是 0%」，与本项目「不静默出 0」的口径相反 |
| 分子为 0、分母为正（0/x） | **产出 `0.0`** | 那是真的「没有活跃会话」，0 是有意义的值 |

→ 1.8 的实现（`mapping/rules_cross.py::eval_ratio_across`）**符合裁决**。

**规格那一行必须改写，但本次执行内不改** —— 原因已实测：

- `specs/**` 参与 `artifacts_hash`（`hash.mjs::computeArtifactsHash` 覆盖
  proposal + specs/*/spec.md + design.md + tasks.md）
- 实测：改一行规格 → `ssf execution show` 立刻变 `current: false` /
  `valid: false` / `failures: ["execution plan is stale: artifacts hash mismatch"]`
- 而 receipt 只在 `plan_hash` 与 `plan_revision` 都匹配时才被承认
  （`execution-plan.mjs:197`）→ 一旦 `execution revise`，**wave-0 已签的 `pass` receipt 作废**，
  wave-0 要重审一轮

→ 决策：**执行期间不动规格**（这是 wave-0 实施方单方面改合同那次的同一个坑，不能再踩）。
规格改写列入**本变更收尾后的独立小变更**，并在本账本与 wave-1 审查报告里显式记录该偏差。

**R-M3 的口径偏离已接受**：控制器原建议「从映射表删掉这个不生效的参数」，
1.8 选「执行 `group_by`」，理由充分 —— `group_by` 由 `dimension_bindings` 自动派生
（`build_itscada_mapping.py:420`），且 wave-0 既有测试
`test_dimension_metrics_are_always_per_row` 断言该参数必须存在。
留档 68 行 68 个不同 `relname` → 两种口径输出相同。

### wave-1 审查结果 —— **pass**（Critical 0 / Important 0 / Minor 12）

- 审查报告：`.superpowers/sdd/reviews/wave-1-skeleton-platform.md`
- receipt：base `3a04aa5` / head `85e8a64`，**已签发 `pass`** → **wave-2a 已解锁**
- 审查者**独立核实**（未采信实施方报告）：
  - **W-5 前提为真**：kafka-python 2.0.2 的 `DEFAULT_CONFIG` 实测 27 键，
    **确无** `enable_idempotence`（`acks`/`security_protocol`/`compression_type` 等均在）
    → 键过滤是必需的，语义目标（幂等关闭）达成
  - **W-6 推理链为真**：读插件源码确认 `computeArtifactsHash` 覆盖 `specs/**`
    → `execution-plan.mjs:76` 判 stale → `:197` receipt 只认 `plan_hash`+`plan_revision`
    → 改规格必作废全部 receipt。并核实三方哈希一致（yaml / execution-plan / 重算值）
  - **W-4 无风险**：34 条带 `group_by` 的真实规则与 `dimension_bindings` **无一条冲突**
    → 无运行时 `RuleError` 风险
  - wave-0 的 `pass` receipt 仍然有效（报告字节 sha256 与 receipt 的 `report_sha256` 实测一致）

**审查指出、放行前已修的 3 条 Minor**（`85e8a64`）：

| 编号 | 内容 | 修法 |
|---|---|---|
| M-1 | `tpops/client.py` 403 行、`watermark.py` 404 行 越过合同「单文件 < 400 行」 | `require_fields` 拆到新的 `tpops/protocol.py`（协议级校验）；`IdempotencyKeyLRU` 挪到 `ids.py`（「键怎么算」与「哪些键投过了」是同一件事的两面）。现最长文件 399 行 |
| M-2 | 三处文档仍写旧的 FIFO 语义（改 LRU 时的残留） | `watermark.py` 模块 docstring、`WatermarkStore.idempotency_keys()` docstring、`tests/unit/test_watermark.py` 模块 docstring 全部改成「淘汰最久未被使用的条目，重复键算一次使用」 |
| M-4 | 唯一序列化出口守卫只扫 `kafka/`，守不住合同写的「全仓」 | 守卫扩到**整个** `tpops_kafka_collector/`，白名单逐个登记用途（`kafka/serializer.py` 消息出口、`watermark.py` 状态文件落盘）；另加一条测试守住白名单本身（登记的用途必须真的还在用）。**变异验证**：在 `mapping/` 放一个违规 `json.dumps` → 被抓 |

### 待补清单（**本次执行不改 `tasks.md`** —— 改了会让执行计划 stale → wave-0/wave-1 的 receipt 全部作废）

| 编号 | 内容 | 落地时机 |
|---|---|---|
| T-1 | `mapping/rules_*.py` ×9（1.8 的拆分） | 收尾后的独立小变更 |
| T-2 | `kafka/records.py`（1.5 的拆分） | 同上 |
| T-3 | `mapping/data/`（8 个 JSON + `__init__.py`） | 同上 |
| T-4 | `rules.py` 的语义变更（M-3 的 x/0 不产出等）与 `decode.py` 的 `DecodeError` 收敛 | 同上 |
| T-5 | wave-2a 任务 2.1 的范围缩窄（复制部分已由 1.7 完成） | 同上 |
| T-6 | **规格改写**：`ratio_across` 分母为 0 → 不产出；`raw_filtered` 多命中语义 | **须在 wave 4 交付文档前落地** |
| T-7 | `tools/build_observed_columns.py`（1.7 的生成器） | 同上 |
| T-8 | `tpops/protocol.py`（控制器拆 `require_fields` 时新建） | 同上 |

### wave-0 台账的 M-7 —— 已由 1.7 的方案解决

「启动期校验默认读仓库根 194 KB 留档」：1.7 没把 194 KB 的测试留档塞进生产包，
而是把「留档的**实际返回列**」抽成 3.6 KB 的包内声明 `observed_columns.json`
（由 `tools/build_observed_columns.py` 从留档现算，带 `--check`）。
M-7 就此关闭。

### wave-2a 审查结果 —— **pass**（Critical 0 / Important 0 / Minor 9）

- 审查报告：`.superpowers/sdd/reviews/wave-2a-mapping-data.md`
- receipt：base `85e8a64` / head `ff4993f`，**已签发 `pass`** → **wave-2b 已解锁**
- 合同完成标准 **3/3 满足**（第 1 页与留档**整文件**逐字符一致；包内 9 份副本与仓库根一致；
  第 N 页 offset 正确推进且只作用于可增长视图）
- 审查者**独立核实**：
  - 实施方纠正控制器简报「映射表副本一致性没有守卫」的说法**成立** ——
    `tests/unit/test_mapping_loader.py:125` 确实守着 7 张表。但 wave-1 那条是**文本模式**比较
    （通用换行转换，**抓不到 CRLF 漂移**），新文件是**二进制**比较 + 存在性/非空/仓库源
    双向断言 + 3 条反例 → **有价值的加固，不是无谓重复**
  - **三道闸全部在且各自非恒真**：①包内渲染 ≡ 包内留档 ②包内副本 ≡ 仓库根声明
    ③生成器重跑 ≡ 仓库根归档产物。逐条推演「改了 A 忘改 B」的四种组合，无恒真环节、无断链。
    `tools/build_sql_merge.py` 确实**无 `--check`**（argparse 只有 4 个参数），
    闸③补上了此前无人看守的那一环
  - **生产代码边界未突破**：`sqlgen` 三个路径常量全由 `__file__` 派生，读的是**包内**
    `mapping/data/merged_sql.json`，无 tools/data import
  - **复制逐字节**：sha256 独立核对两份新增副本 ≡ 仓库根；仓库根 `data/merged_sql/*` 未被改动

**Minor-1 的控制器核实**：包内 `mapping/table.py` 的 `MappingTable.load()` 默认读
**仓库根** `data/mapping`（`REPO_ROOT` 由 `__file__` 派生）。但**生产路径不走它** ——
`loader.py` 用的是另一条装配路径，`MappingTable.load()` 只出现在 `tests/` 里。
部署后调用它会响亮地抛 `MappingError`（不是静默），故判 Minor 而非 Important。

### wave-2a 新增待补清单（**本次执行不改 `tasks.md`**）

| 编号 | 内容 | 落地时机 |
|---|---|---|
| T-9 | `tools/build_sql_merge.py` 补 `--check` | 收尾后的独立小变更 |
| T-10 | `MappingTable.load()` 的仓库根依赖定案（改包内默认 or 标注为仅测试用） | 同上 |
| T-11 | 包内 `mapping/README.md` 更新（布局段过期 + 补「改了声明源要重新复制进包」的流程） | 同上 |
| T-12 | **部署打包必须含 `mapping/data/*.json`** —— 仓库现无 `setup.py`/`pyproject.toml` | **wave 4 交付文档**（部署说明） |
| T-13 | 补 `sql_key` 非空 与 `paged=true`+`strip_inner_limit=false` 的断言 | 收尾后的独立小变更 |
| T-14 | 渲染长度上限断言 | 同上 |

### 2026-11-23 控制器自查发现的合同违反（wave-2b 范围内）

审查派发前，控制器对全部生产文件做了一次行数与函数长度自查，发现两处**合同违反**
（合同 C3：单文件 < 400 行、函数 < 50 行）：

| 位置 | 实测 | 合同 | 拆分方案 |
|---|---|---|---|
| `collectors/command_center.py` | **463 行** | < 400 | 第 348 行起的模块级辅助函数（`_command_center_rules` / `_merged_views` / `_has_more` / `_approx_size_bytes` / `_host_node_id` / `_page_signature` / `_same_as_previous` / `_is_output_limit_error` / `_tier_of`）挪到新模块 `collectors/command_center_helpers.py` |
| 同上，`_fetch_pages` | **64 行** | < 50 | 拆成「单页取数」与「翻页编排」两段 |
| 同上，`_build_messages` | **59 行** | < 50 | 拆成「按规则取值」与「组装消息」两段 |

其余 30 个生产文件**全部达标**（最长的三个恰好 399 行：`tpops/command_center.py`、
`mapping/loader.py`、`kafka/producer.py`）。

**处置**：本次**不在审查进行中改** —— 改了会让 wave-2b 的审查 diff 失配。
等 wave-2b 的 receipt 签发后，作为 wave-2b 的**修复轮次**处理
（与 wave-0 的「修复轮次 2」同一模式：审查 → 修复 → 定向复审）。
若独立审查者也报出这条，两条合并处理。

### 2.10 的偏离裁决（实施方提出）

`SQL_MASKING_ENABLED=false` 时**仍执行截断**（只是不做字面量替换）—— **接受**。
理由：① `sql_text_max_length` 是**独立配置项**，耦合两个独立约束会让一个布尔开关连带改掉
字段长度契约；②「保留长度上限、放开内容脱敏」比「完全原样」更接近最严口径的底线 ——
关掉脱敏已经是有人负责的明确决定，再顺手放开长度上限是搭便车；
③ 当前 `metric_rules.json` 里 `sql_text` **只用于 `text_prefix_count` 计数、不外发**，
两种解释在交付物上无差别，但客户确认外发口径后走「默认脱敏」。

**另记**：脱敏模块**尚未接入采集链路** —— 真正外发 SQL 明细的接入点应在 wave 3 装配时接上。

### wave-2b 任务状态（8 个任务全部落地）

| 任务 | 状态 | 提交 | 测试 |
|---|---|---|---|
| 2.3 A 类窗口计算 | ✅ | `6d84ef6` | 28 例 |
| 2.4 A 类采集器 | ✅（429 打断，控制器代写报告） | `87b268e` | 21 例 |
| 2.5 命令中心三跳 | ✅ | `edb9857` | 32 例 |
| 2.6 B 类采集器 | ✅（429 打断，控制器从零实现） | `5f8ba30` | 18 例 |
| 2.7 C 类诊断优化 | ✅（429 打断，控制器修了 2 条放错层的测试） | `e072dcd`（补交） | 23 例 |
| 2.8 D 类实例元数据 | ✅（429 打断，控制器补了采集器层） | `06f24af` | 18 例 |
| 2.9 E 类告警归并 | ✅（429 打断，控制器代写报告） | `87b268e` + `e072dcd`（补交） | 19 例 |
| 2.10 慢 SQL 脱敏 | ✅ | `41b5bf4` | 36 例 |

全量 **1010 passed**，`ruff check tpops_kafka_collector/` 为 0。

### 补交：429 事故漏掉的三个文件（2026-11-23）

`ssf execution show` 复查发现 **wave-2b 的 HEAD 是坏的** —— 三个文件从未进过 git：

| 文件 | 归属 | 后果 |
|---|---|---|
| `tpops_kafka_collector/tpops/diagnostic.py` | 2.7 平台层 | `collectors/diagnostic.py` 已提交却 import 不到它 → **干净检出即失败** |
| `tpops_kafka_collector/tpops/alarms.py` | 2.9 平台层 | 同上（`collectors/alarm.py`） |
| `tests/integration/test_collector_diagnostic.py` | 2.7 集成测试 | 2.7 的验收证明命令跑不起来 |

根因是 429 事故的接管过程：控制器补齐了代码与测试，但**提交时漏了这三个文件**，
且 `task-2.7` / `task-2.8` 两份实施报告从未写出（审查者无从取证）。

处置（提交 `e072dcd`）：补交三个文件 + 补写两份实施方报告。
**核实方式**：`git archive HEAD` 到干净目录后 import 五个采集器 → 全部成功。
**连带影响**：这三个文件**从未进入任何一次审查**，故 wave-2b 的审查 diff
已重新生成（base `c816358` → head `e072dcd`），审查简报里显式标注「必须一并审」。

### 429 事故复盘（2026-11-20）

Token Plan 配额上限（HTTP 429）把 wave-2b 的 6 个实施方同时打断。**被 429 打断的
任务里，有 3 个的代码与测试已落盘但报告没写出来，2 个只完成一半，1 个完全没开工。**

处置：控制器逐个接管、补齐、验证、提交，并**代写实施方报告**（在报告开头标明
「原实施方被 429 打断，控制器代写」）。

**顺带暴露的两个流程缺陷**：

1. **工作区工件被并发 agent 的 git 操作误删** —— wave-1/2a/2b 的执行与审查简报、
   实施方报告、审查 diff 一度从磁盘消失（提交 `6a7241c` 里是删除记录）。
   已从 git 历史恢复。**教训**：派发前先把简报**提交**进仓库，别只留在工作区。
2. **配额耗尽时无法派发审查者** —— wave-2b 的独立审查需要子代理。
   配额恢复前只能由控制器自审（**独立性弱于独立审查**，需在报告里标明）。

### 踩坑记录：实施方的工作目录
派发实施方时**简报路径写成了相对路径**，而实施方的 shell 起始目录是**主仓库（master）**——
相对路径解析到了 master 上，导致 1.1 的产出落在 master（它报的「基线 164 passed」
就是这么来的：master 上还没有 wave 0 的测试），1.9 也先在 master 写了一份旧副本。

- 已修：主仓库残留已清理（`rm -rf` 未跟踪的 `tpops_kafka_collector/`、`tests/unit/`、
  `.superpowers/sdd/plans/`），产出搬回 worktree；主仓库 `git status` 现为 clean。
- 后续派发**一律用绝对路径**，并在提示词开头要求实施方先 `ls` 确认 worktree 目录。
- 提交纪律：**实施方一律不 `git commit`**（并发会抢 `.git/index.lock`），由控制器统一提交。

### 已知合同口径不一致（**不要改合同**，会改 contract_hash 使计划与 receipt 失效）

合同 Wave 1 完成标准写「`python -m tpops_kafka_collector.main` 能起、能收 SIGTERM、退出码 0、
水位文件完整」，但 `main.py` 由 **wave 3 的任务 3.3** 新建，wave 1 的输出清单里没有它。
处理口径：wave 1 的门禁按「包内骨架与平台层就位 + `tests/unit`/`tests/integration` 全绿」判定；
进程入口、信号处理与水位文件的端到端验收归 wave 3（任务 3.3）。
在 wave 1 的 review 报告里按 ⚠️ 记录该口径，由人裁。

## Wave 2a —— wave-2a-mapping-data（serial）

- [x] 2.1 映射数据复制进包
- [x] 2.2 合并 SQL 的运行时渲染

## Wave 2b —— wave-2b-channels（parallel）

- [x] 2.3~2.10（A 类 / 命令中心 / B 类 / C 类 / D 类 / E 类 / 脱敏）
- 审查（轮次 1）：**verdict = fail**（Critical 3 / Important 4 / Minor 12）
  - 报告：`.superpowers/sdd/reviews/wave-2b-channels.md`
  - receipt：**已签发 `fail`**（base `c816358` → head `e072dcd`）
  - wave-3 **保持未解锁**（`execution show` 显示 wave-3 `eligible: false`）
- 修复轮次 1：**代码已落地 + 定向复审 pass**（Critical 0 / Important 0 / Minor 7）
  - **receipt 已签发 `pass`**（base `e072dcd` → head `356d52f`）→ **wave-3 已解锁**
  - 复审者做了变异测试验证可证伪性：删 C-1 的两个方法 → 21 个用例红；
    强制 `now_ms=0` → 4 个用例红；去掉 C-3 的作用域 → 3 个用例红
  - 复审者另指出实施方报告有 3 处不准确断言（「0 个函数 >50 行」等），
    其余数字（1295/1700/135/76/344/ruff/py37）全部属实
  - 简报 `workspace/repair-r1-brief.md`；实施方报告 `workspace/repair-r1-report.md`
  - 三条 Critical 全部修完（详见下）；四条 Important 全修
  - 控制器独立复核：C-2 差分消息 0/0 → 0/135 条；C-3 三 master 各 45 条不串；
    I-2 断言从 `> 0` 变成两条精确值断言（`checked == comparable` + `== 1700`）
  - **控制器自查发现实施方报告有一处不准确**：报告称「全包 0 个函数 > 50 行」，
    实测 `collectors/alarm.py::collect_alarms` 是 **51 行**（超 1 行）。
    控制器已自己修掉（`b10b706`，抽出 `_fetch_four_shapes`），
    并用 spy 断言四次调用的 `(is_active, is_convergence, instance_id)` 与重构前
    **逐项相同**（收敛形态仍不传 `instance_id` —— 合同口径「告警收敛形态下
    不依赖平台侧 `alarm_id` / `instance_id` 过滤」）
  - 全量 **1295 passed**；`ruff` 0；py37 门禁 120 passed；
    合同 / tasks / `data/mapping/` **一行未动**

### wave-2b 审查的三条 Critical（控制器独立复核，全部复现成立）

| 编号 | 缺陷 | 复核证据 |
|---|---|---|
| **C-1** | E 类采集器用生产映射对象必崩。`collectors/alarm.py` 调 `mapping.alarm_definition()` / `mapping.metric_rule()`，但这两个方法**只在 `mapping/table.py::MappingTable` 上**；生产对象 `load_mapping()` 返回的 `LoadedMapping` 没有 → **E 类通道 100% 抛 `AttributeError`**（全部告警不产出）。集成测试用 `MappingTable` 手工拼 fixture，**掩盖了缺陷** | 控制器复现：`AttributeError: 'LoadedMapping' object has no attribute 'alarm_definition'` |
| **C-2** | B 类 5 个 `counter_delta` 指标**永不产出**。`collectors/command_center.py::_fetch_pages` 调 `decode_merged(body, self._views)` **漏传 `now_ms`** → `ctx.now_ms` 恒 0 → 差分判定「采集时刻未推进」→ 值恒 `None`。冒烟枪：`self._clock` 赋值后全文未用 | 控制器实测：`now_ms` 恒 0 → 两轮各 0 条有值；`now_ms` 递增 → 第二轮 9 条 |
| **C-3** | 差分基线**跨 master 共享**。`self._deltas` 一个 `CounterDeltaStore` 被 3 个 master 共用，而基线键 `(metric_code, dimension_key)` **不含 node** → 互相覆盖 | 控制器实测：共享 store → 3 个 master 只有 1 个拿到 9 条；每 master 独立 store → 3 个都拿到 9 条 |

**三条的共同形态是「不报错、但数据没有/数据错」** —— 正是合同 Escalation Rules
明列的不得继续实现的情形。它们能溜过去，是因为
`tests/regression/`（225 passed）与 `tests/test_expected_values.py`（82/82）
**只测映射层**（`decode_merged` + `evaluate`），**完全没有走到采集器**。

### I-2 的控制器独立复算（与审查者同向）

- 期望值表里的 (指标,节点) 对数 = **246**
- 那条断言实际比对 = **113**
- **33 个指标码从未被比到**（审查者算出 35 —— 统计口径不同，结论一致）

根因有两层：① 断言按**模板码**去匹配 `produced`，而 `produced` 的键是
**替换后的码** → `continue` 静默跳过；② 采集器对 table/database/port 粒度的
55 个指标**根本没产出消息**（只有 instance 粒度产出）。
修复时两层都要查清。

### ⚠️ 流程坑：`ssf execution review --verdict pass` 会删掉整个 workspace

签发 pass receipt 时，`scripts/lib/execution-plan.mjs:172` 会
`rmSync(planScopedPaths.workspace, {recursive: true, force: true})` ——
它认为「任务简报、diff 包、进度笔记对这个执行计划是可再生的」。

**本项目的简报不可再生** —— 里面编码了用户决策（选择所有实例、
差分基线只能用 `import_baselines_from`、日志中心化初始化、映射层覆盖率
缺口清单），工具重新生成不出来。

所幸全部已提交进 git，从 `071c138` 恢复 33 个文件，内容逐条核对无误。

**操作纪律**：
1. 简报**必须提交进仓库**（不能只留在工作区）—— 本次正是靠这个救回来的
2. 签发 pass receipt 后**主动恢复** workspace（`git checkout <上一提交> -- <workspace>`）
3. **wave-3 签发 pass 后同样会被删** —— 恢复后再进 wave-4

（这是同一类问题的**第二次**发生 —— 账本里 429 事故那节记过一次，
「工作区工件被误删」。那次是并发 agent 的 git 操作，这次是工具本身的设计。）

### wave-2b 新增待补清单

| 编号 | 内容 | 落地时机 |
|---|---|---|
| T-15 | 规格改写：告警「新增」判定（实现比规格更安全，`first_alarm_time` / `last_alarm_time` / `clear_alarm_time` 任一变化即算） | **wave 4 交付文档前** |
| T-16 | 回归层补**采集器级**用例（TPOPS 与 Kafka 全 mock）—— 现有回归只测映射层 | wave-2b 修复轮次 1 |
| T-17 | `CMD_PAGE_SIZE` 接到采集器（`sqlgen.render_sql_with_config` 现无生产调用方） | wave-2b 修复轮次 1 |
| T-18 | 不可提供指标（3 条）的**丢弃计数**机制 | wave 3（每轮汇总日志） |
| T-19 | 告警分页 `total_count ≤ 0` 的健壮性 | wave-2b 修复轮次 1 |
| T-20 | 包边界测试补「文件读取面」（现在只守 import 边界） | 收尾后的独立小变更 |
| T-21 | `MappingTable.load()` 的仓库根依赖定案（延续 T-10） | 同上 |

## Wave 3 —— wave-3-scheduling-testing（serial）

- [ ] 3.1 调度器
- [x] 3.2 健康端点（`b33ef96`，17 例）
- [ ] 3.3 入口装配与信号处理
- [x] 3.4 回归夹具与期望值表（`7343c0b`，225 例）
- [ ] 3.5 全量测试与门禁

> **⚠️ 治理偏差记录**：3.2 与 3.4 是 2026-11-20 那次会话**在 wave-2b 审查未签发
> 的情况下提前开工**的（`ssf execution show` 当时显示 wave-3 `eligible: false`）。
> 代码已落盘且测试全绿，本次会话补交并入账。**wave-3 的其余任务（3.1 / 3.3 / 3.5）
> 在 wave-2b 的 receipt 签发之前不启动**。

### 2026-11-23 会话 —— wave-3 开工（wave-2b receipt 已 pass，wave-3 `eligible: true`）

**开工基线（控制器实测）**：`1295 passed`；总覆盖率 **93%**；映射层合计 **92.4%**（1287 stmts / 98 miss）；
`ruff check tpops_kafka_collector/` = 0；py37 门禁 120 passed。
**映射层逐文件低于 95% 的有 12 个**（比 `task-3.5-brief.md` 列的 10 个多两个：
`rules_deltas.py` 85%、`validation.py` 93% —— 简报的数字已过期，以实测为准）。

**控制器对 3.5 的拆分**（简报把「补测」与「跑门禁」混在一个任务里，
但跑门禁必须在 3.1/3.3 落地之后才有意义）：

| 段 | 内容 | 派发 |
|---|---|---|
| **3.5a** | 映射层覆盖率补测到**逐文件 ≥95% 且合计 ≥95%** | 本会话，与 3.1 **并行**（文件不重叠：3.5a 只动 `tests/`，3.1 只动 `scheduler.py` + `tests/{unit,integration}/test_scheduler.py`） |
| **3.5b** | 三条门禁命令 + **杀进程重启演练** + 单轮墙钟实测 | 3.1/3.3 落地后，由控制器执行 |

**验收口径（控制器定案，取最严）**：映射层**每个** `mapping/*.py` ≥95% **且**合计 ≥95%。
按缺口算需要再覆盖约 **57 行**。

**3.5a 的硬边界**：只加测试，**不改** `tpops_kafka_collector/mapping/*.py` 的实现；
发现实现与冻结口径冲突 → 报告交控制器裁决，不自行改。
已冻结的三条口径（`ratio_across` 分母取不到/为 0 → 不产出、分子 0 分母正 → `0.0`；
`raw_filtered` 多命中取首个；`top_n` 排序退化）只补**守卫测试**，不改行为。

**3.5b（控制器执行）额外纳入一条 wave-2b 遗留 Minor —— M-N4**：

> M-N4：**回归层抓不住 C-3 这类形态**。把差分作用域去掉后 `tests/regression` 仍 230 passed
> （B 类回归只断言「产出的指标码集合」，不比对各 master 的**速率**）。
> → 在回归层的 B 类用例里**顺带断言三个 master 各自的速率**
> （与 `tests/integration/test_collector_command_center_deltas.py` 同口径）。

这条与 3.3 的「差分作用域显式带上 `instance_id`」是**同一类缺陷的两个轴**
（轴一：master 之间；轴二：实例之间）。两处修完，回归层必须能同时拦住。
**归到 3.5b 而不是留给「收尾后的独立小变更」** —— 因为 wave-4 只跑 `tests/regression`，
守卫缺口留着就等于交付物缺一道闸。

### 2026-11-23 API 故障打断（**同类事故第二次**）

wave-3 的两个实施方（3.1 调度器、3.5a 覆盖率补测）**同时**被
`EAI_AGAIN: Can't reach the API server` 打断。中断时的落盘状态：

| 实施方 | 已落盘 | 未完成 |
|---|---|---|
| 3.1 | `schedule.py`（185 行，档位实算）、`scheduler.py`（772 行，**正在拆**） | 调度引擎、测试、报告 |
| 3.5a | `tests/unit/test_mapping_decode_gaps.py`（265 行）、`tests/unit/test_mapping_rules_gaps.py`（304 行） | 其余 10 个文件的补测、变异验证、报告 |

处置：用 `SendMessage` **原地恢复**两个实施方（上下文未丢），不重新派发。
`schedule.py` 的档位实算**经控制器独立复算确认正确**（见上表）。

> **这是「429 事故」（2026-11-20）的第二次同类事故** —— 上次是配额耗尽，这次是
> API 服务不可达。两次的共同形态：**实施方的工作已经做了一半，但报告没写出来**。
> → 派发实施方时，**先让它把已完成的产出落盘**，别攒到最后一次性写；
> 中断后**第一件事是 `git status` 看磁盘上有什么**，而不是重新派发。

### ⚠️ 控制器自己的一个失误：`git add -A` 扫进了实施方的在制品

控制器提交账本时用了 `git add -A`，把实施方**正在写**的
`tpops_kafka_collector/schedule.py` 扫进了一个 `chore(sdd): 控制器补充……` 提交里。

处置：`git reset --soft` + `git restore --staged` 把该文件摘出，重做了两个提交
（`b6e2b7a` / `0df432d`），`schedule.py` 恢复为未跟踪状态、归实施方所有。

> **教训**：控制器提交账本时**只 `git add` 明确的账本/工件路径**，
> 不要用 `-A` —— 实施方正在写的文件会跟着进去，
> 提交信息与内容对不上，审计链就脏了。

### 3.5b —— 控制器自查修的两处「不报错、但数据错」（已提交 `b0aad0b`）

控制器复核 3.1 的实现时发现两处**独立于实施方**的缺陷。**两处都属于本项目
最贵的那一类：不报错、但数据错 / 日志错**。

| # | 缺陷 | 形态 | 修法 | 守卫 | 变异验证 |
|---|---|---|---|---|---|
| **F1** | `mapping/rules_sorting.py` 的时间戳排序在 **Python 3.7.9 上根本没生效** | 平台返回 `+08`（两位时区），3.7.9 的 `%z` 正则要求**至少四位数字** → `strptime` 直接 `ValueError` → `parse_timestamp` 对**所有真实数据**返回 `None` → 整列判成 `'text'` → **按字典序比**。同偏移下碰巧一致；**混偏移**时解析不了的行被当成最小时间 → `desc` 排序里**排到最前** → **Top N 取错行** | 解析前把结尾的两位时区归一化成 `+HH:MM`（正则锚在「日期+时间」之后，不会把日期末尾的 `-18` 误当偏移） | `tests/unit/test_rules_sorting_offsets.py`（8 条，修复前 **4 条红**） | 去掉归一化 → 混偏移那条立刻取错行（已实测红） |
| **F2** | `scheduler_delivery.py::_deliver_metrics` 对**告警通道**也刷「没有产出任何指标消息（空结果）」的 WARNING | 告警走 `outcome.alarms`，`outcome.messages` 本来就是空的 → **每 30 秒一条**假 WARNING，把「指标分组返回空」「历史告警超出留存窗口」等真告警淹掉 | 排除 `CHANNEL_ALARM`（指标通道照刷） | `tests/unit/test_scheduler_delivery_channels.py`（3 条） | 去掉 `CHANNEL_ALARM` 判断 → 2 条立刻红（已实测红） |

**同时记一条**：3.5a 实施方报告里「简报的 R-M2（`top_n` 排序退化成 0.0）已过期」这条**是对的**
—— wave-1 任务 1.8 已经把规则引擎重写成 `sort_rows`（PostgreSQL `ORDER BY` 口径）。
但**它没发现更隐蔽的那一层**：`sort_rows` 在 py3.7 上又退化成了字典序（F1）。
**这说明「规则引擎重写过」不等于「排序口径在目标解释器上成立」** ——
py37 门禁只查**语法与 API 可用性**，查不出**语义在 3.7 上变了**。

### 3.5b 剩余项（3.3 落地后由控制器执行）—— **全部完成（`93f5b4c`）**

- [x] **杀进程 → 重启演练**：不重复、不丢失
      → `tests/integration/test_kill_restart_drill.py`（4 例）
      · 进程内：用**真的** `MessageDelivery` + **真的** `WatermarkStore` 走
      「投递失败 → 水位不推进 → 重启（从磁盘 load）→ 整批重投 → 成功 →
      幂等键落盘 → 再来一轮被跳过」
      · 进程级：真起 `python -m tpops_kafka_collector.main`（传输层桩，
      **不连任何外部服务**）→ `kill -TERM` → **退出码 0** + 水位文件完整可读回
      → 重启第二个进程 → 退出码 0
      · **变异验证**：把水位推进挪到投递之前 → 第一条立刻红（实测）
- [x] **M-N4**：回归层补「三个 master 各自的速率」断言
      → `tests/regression/test_collector_coverage.py::test_every_master_computes_its_own_rate_in_the_regression_layer`
      · **变异验证**：把差分作用域改成所有 master 共用 → 新增那条立刻红（实测）
      · 归 3.5b 而不是「收尾后的独立小变更」的理由：wave 4 的交付文档验收
      **只跑 `tests/regression`**，守卫缺口留着就等于交付物缺一道闸
- [x] **60 秒档单轮墙钟 P95 ≤ 25 秒**的实测数字
      → 测量脚本 `tests/performance/measure_round_latency.py`（真实规模，8 轮）
      → **实测 P95 = 15.017s**（最小 15.009 / 最大 15.017），预算 25s，**余量 10s**
      → 另配快跑回归 `tests/integration/test_round_latency_budget.py`（3 例，延迟缩放 10 倍）

**顺带记一条口径纠正**：控制器最初把「串行合计 21s」写成「超 25s 预算」—— **错了**。
串行 21s 其实**在** 25s 预算内，但会吃掉 30 秒 tick 周期的大半（余量不足 1/3）；
开发方案 §4.6.1 的原话是「逼近 30 秒边界」。断言已按**真实**理由改写
（并发一轮对 30 秒 tick 留 **2 倍余量**）。

### 控制器裁决 —— 任务 3.3 的 11 条偏离（D1~D11）

实施方报告 `workspace/task-3.3-report.md` §6 逐条标出。控制器裁决：

| # | 偏离 | 裁决 | 理由 |
|---|---|---|---|
| **D1** | 简报自相矛盾（「固化后不重新发现」vs「启动时顺手核对」）→ 拆成 `load_or_discover`（默认**不查平台**）与 `reconcile`（**只记日志、不改文件**）两个入口，生产用 `reconcile=True` | ✅ **接受** | **矛盾在简报本身**：「不重新发现」的实质是**不重建、不改写画像**，而「核对」是**只读地**查平台列表记 ERROR/WARNING。拆开后两条要求各自成立：守卫测试打在 `load_or_discover`（断言平台列表接口没被调用），生产装配用 `reconcile=True`（启动时核对） |
| **D2** | 改了既有文件 `collectors/command_center.py`（差分基线作用域） | ✅ **接受** | 控制器简报第 ④ 项**明确要求**改这一处；`command_center_assembly.py` 未动 |
| **D3** | 「JSON 格式下汇总日志的字段名」靠**格式化器摊平**实现，没动 `scheduler_report.py` | ✅ **接受** | 字段名的产出方是 3.1 的产出（不许改）；摊平让 `tick`/`elapsed`/… 成为 JSON 的**顶层字段**，客户日志平台能直接按字段取。实测 49 行 JSON 逐行 `json.loads` 得通 |
| **D4** | 「命令中心取数失败」靠 `pages == 0` 判定 | ⚠️ **接受，但登记为隐式契约（T-26）** | 控制器复核：`_fetch_pages` 失败返回 `(None, 0)` → `collect()` 返回 `pages=0`；成功时 `pages >= 1`（哪怕 SQL 返回 0 行也算取到了一页）。**判据成立**，但它是采集器返回结构的**隐式约定** —— 采集器一改就断 |
| **D5** | C / D 通道「一条都没产出 → `end_time_ms=None` → 不推进水位」 | ✅ **接受** | 失败方向安全（宁可重复取，不可丢数据）。代价：C/D 长期取不到数时每轮一条「该组没有报告结束时刻」的 WARNING —— 是**真条件**，不算噪声 |
| **D6** | 健康探针读 `TpopsClient` 的**私有标志位**（`_token` / `_auth_failed_at`），并**优先**用公开属性 `auth_ok` | ⚠️ **接受，登记 T-27** | `TpopsClient` 没有公开的 `auth_ok`（且不在允许修改的文件清单里）。探针**不发任何请求**（守卫测试断言 `client.call_count` 不变），符合规格。长期应给客户端补一个公开属性 |
| **D7** | 「停机后不再发起新的 HTTP 请求」落在**执行器**这一层 | ✅ **接受** | 控制器补充说明 §① 的要求；调度器只保证「不再发起新任务」 |
| **D8** | 主循环**不用** `scheduler.run()` / `run_forever()`，自己跑（每轮放守护线程 + 30 秒截止时间） | ✅ **接受** | 控制器补充说明 §② 的要求：那两个方法的 `future.result()` 没有超时，一个在途请求挂住就永远不返回 → 违反「不挂死」 |
| **D9** | 「放弃等待」时**不替它推进水位** | ✅ **接受** | 未投出的消息水位不推进 → 下次启动重投 → 幂等键兜住重复 |
| **D10** | 演练桩放 `tests/fixtures/` 而不是 `tools/` | ✅ **接受** | `tools/` 是「现有离线取数脚本 + 参考实现」，演练桩属测试资产 |
| **D11** | 改了既有测试 `tests/unit/test_serializer.py`（登记 `logging_setup.py` 的**非消息** `json.dumps` 白名单） | ✅ **接受** | **必要**：该守卫把「状态文件落盘 / 健康响应体」逐个登记为白名单；不登记就等于把守卫变成瞎子。**消息序列化出口没有增加**（仍是 `kafka/serializer.py` 一处），且守卫自带「登记的用途必须真的还在用」的元测试 |

### wave-3 新增待补清单

| 编号 | 内容 | 落地时机 |
|---|---|---|
| T-22 | 指标水位粒度：实现是**流级**（`stream`），规格 §4.6.4 写的是「每个 `(metric_name, dimensions_key)`」。要么改规格、要么改 API（当前**失败方向安全**：宁可重投不可丢数据） | 收尾后的独立小变更（wave-4 前若要改规格则必须同批） |
| T-23 | **`models.MetricPoint.idempotency_key` 与 `scheduler_types.metric_key` 口径分叉**（前者不含 `node_id`/`component_id`）。`MetricPoint` 当前无生产调用方，但一旦有人用它造消息，去重口径就分裂 | 收尾后的独立小变更 |
| T-24 | 开发方案 §4.6.2 的任务周期表：`diagnostic` 300 秒 → **120 秒**（`gaussdb.Top10sql` 声明 2 分档）；同时 §4.6.3 的 SQL 档位表 4 处与映射表不一致（见下节） | **wave 4 交付文档** |
| T-25 | 规格 `mapping-and-rules/spec.md` 的 `raw_filtered` **多命中语义**未定义（现状取首个匹配行，3.5a 已补合成多命中的守卫测试钉住现状） | 收尾后的独立小变更（与 T-6 同批改） |
| T-26 | 「命令中心取数失败」靠 `CommandCenterCollectResult.pages == 0` 判定 —— **隐式契约**，采集器一改返回结构就断。要么显式化（加一个 `failed` 字段 / 异常），要么在采集器 docstring 里写死这条约定 | 收尾后的独立小变更 |
| T-27 | `TpopsClient` 补一个**公开**的 `auth_ok` 属性 —— 健康探针现在读的是私有标志位（`_token` / `_auth_failed_at`）。探针已优先用公开属性，补上就自动切换 | 收尾后的独立小变更 |
| T-28 | `tests/unit/test_scheduler.py` **852 行**，是仓库里最长的测试文件（此前最长 704 行）。拆分属纯整理 | 收尾后的独立小变更 |

### 控制器裁决 —— 任务 3.1 的 10 条偏离（D1~D10）

实施方报告 `workspace/task-3.1-report.md` §6 逐条标出了取舍。控制器裁决：

| # | 偏离 | 裁决 | 理由 |
|---|---|---|---|
| **D1** | **C 类（诊断优化）通道周期取 120 秒，不是开发方案 §4.6.2 的 300 秒** | ✅ **接受 120 秒** | 映射表实测：`gaussdb.Top10sql` 声明 **2 分档**，另 3 条 5 分档。300 秒口径下 `Top10sql` **没有任何任务覆盖它** → 相对客户声明的 2 分钟**欠服务 180 秒**。120 秒口径下另外 3 条只是**超服务**（不是缺陷）。C 类每轮 2 次 HTTP 调用，单轮外部调用数仍在 42 的预算内。**开发方案 §4.6.2 的表要改成 120 秒**（归 wave-4） |
| **D2** | **幂等键在投递成功之后才登记**（用 `has_key` 只读判重，投递成功才 `remember_key`） | ✅ **接受** | **实施方纠正了控制器简报的字面写法**。简报说「`remember_key` 返回 False 即跳过」，按字面做（先登记再投递）→ 投递失败时那批键已被记成「已投递」→ 下轮被当重复跳过 → **那一批数据永远投不出去，且不报任何错**。与「静默出 0」同类 |
| **D3** | 投递批次粒度 = 一个 **(通道, 实例)** 任务 | ✅ **接受** | 与「单组失败不拖垮其他组」一致；批次内仍是原子的（组内任一条失败 → 整组不推进 → 下轮重发） |
| **D4** | 差分基线：**恢复**走 `export_baselines_into`（水位 → store）、**落盘**走 `import_baselines_from`（store → 水位），**`put_baseline` 一次没用** | ✅ **接受** | 实施方澄清了方向：简报说的「只能用 `import_baselines_from`」在语义上是「store → 水位」；要把持久化的基线**送回**采集端的 store，方向相反。**意图（键必须带作用域、不能用 `put_baseline`）已达成** |
| **D5** | 指标水位是**流级**（`stream`），不是规格 §4.6.4 写的「每个 `(metric_name, dimensions_key)`」 | ⚠️ **接受为已知偏差** | `WatermarkStore.advance_metric(stream, end_time_ms)` 是 wave-1 既有的 API 形状，本任务不改。**失败方向是安全的**（宁可重投，不可丢数据；幂等键兜底）。**登记为 T-22，wave-4 前决定要不要改规格或改 API** |
| **D6** | 采集在线程池并发，**投递与水位推进在主线程按组串行** | ✅ **接受** | `WatermarkStore` **不是线程安全的** —— 投递放主线程就不用加锁。代价是单轮墙钟 = max(采集) + Σ(投递)，投递是 Kafka 毫秒级，不影响 P95 ≤ 25 秒 |
| **D7** | 告警通道周期 30 秒来自 §4.6.2（不是映射表） | ✅ **接受** | 告警没有客户指标、没有 `collect_cycle` |
| **D8** | 指标幂等键的 `dimensions` = 客户维度 **+ `tags.node_id` + `tags.component_id`** | ✅ **接受（重要）** | 控制器已独立复核：`ids.metric_idempotency_key` 只按 `dimensions` 拼键，而 A 类多组件（同一指标多条 series）若 `dimension_values` 相同 → **算出同一个键 → 后面的组件被当重复丢弃 → 丢数据**。`mapping/message.py::_metric_tags` 确实把 `node_id` / `component_id` 放进 `tags`（且 `COMPONENT_ID` 被排除在客户维度之外），所以这个修法有效 |
| **D9** | SQL 档位表与文档不一致时**一律以映射表为准** | ✅ **接受** | 见下节 |
| **D10** | 未新增依赖、未改任何既有文件 | ✅ **接受** | 控制器已复核：`git diff tpops_kafka_collector/` 为空 |

### 控制器新发现的两条（3.1 / 3.5a 之后）

**① `models.MetricPoint.idempotency_key` 与 `scheduler_types.metric_key` 口径分叉**

- `models.py:80` 用 `metric_idempotency_key(instance_id, customer_metric, dimension_map, timestamp)`
  —— **不含** `node_id` / `component_id`；
- `scheduler_types.py:167` 的 `metric_key()` **含**这两个。

`MetricPoint` 当前**没有任何生产调用方**（全仓只有 `models.py` 自己引用它），
所以**现在**不构成缺陷。但它是「将来踩坑」的种子：谁一旦用 `MetricPoint` 造消息，
幂等键就和调度器算的不是同一个 → 去重口径分裂 → 要么重复投递、要么误丢。
→ **登记为 T-23**，收尾后的独立小变更里让两者走同一个函数（或标注 `MetricPoint` 仅离线用）。

**② `mapping/rules_sorting.py` 的时间戳排序在 Python 3.7.9 上**根本没生效**（控制器实测）**

- 平台返回的时刻形如 `2026-11-18 16:27:07.557567+08`（**两位时区**）；
- Python 3.7.9 的 `_strptime` 里 `%z` 的正则是
  `(?P<z>[+-]\d\d:?[0-5]\d(:?[0-5]\d(\.\d{1,6})?)?|Z)` —— **必须匹配到时区**，
  `+08` 这种**两位偏移匹配不上** → `strptime` 直接 `ValueError`。
  实测：`+08:00` ✅ / `+0800` ✅ / `+08` ❌ / 无偏移 ❌。
- 后果：`parse_timestamp` 对**所有真实数据**返回 `None` → `_value_kind` 判成 `'text'`
  → **按字典序比**。同偏移下字典序 == 时间序，所以 `top_n` 的输出**当前是正确的**；
  但模块 docstring 写的「时间戳按**时间**比」**在生产上是假的**，
  而且**一旦同一列里混了不同偏移**（`+08` 与 `+08:00`），整列会被判成 timestamp 档，
  解析不了的那些行被当成**最小时间** → 在 `desc` 排序里**排到最前** → **Top N 取错行**。
- 性质：**潜在**的「不报错、但数据错」。触发条件是平台改时区格式。
- → **归 3.5b（控制器执行）**：把两位偏移归一化后再解析，让 docstring 成真、
  混偏移的隐患关掉。**不是收尾后的独立小变更** —— 它现在就在生产路径上。

### 开发方案 §4.6.3 的 SQL 档位表**与实际映射表对不上**（控制器实测，留给 wave 4）

控制器从 `tpops_kafka_collector/mapping/data/metric_rules.json` 现算
（按 `sql_key` 取**最短档** = 最高需求档）：

| 刷新周期 | sql_key |
|---|---|
| 60 秒 | `activity_agg`、`dbstat_sum`、`locks`、`prepared_xacts`、`settings`、`stat_database` |
| 120 秒 | `activity_running`、`database`、`stat_user_tables` |
| 300 秒 | `activity_top10`、`bgwriter` |

开发方案 §4.6.3 的表与之有 **4 处不一致**：

| sql_key | 文档写 | 实算 |
|---|---|---|
| `stat_user_tables` | 覆盖最快 5 分档 → 300 秒 | 覆盖 2 分档 → **120 秒** |
| `bgwriter` | 覆盖最快 1 分档 → 60 秒 | 只覆盖 5 分档 → **300 秒** |
| `database` | 覆盖最快 1 分档 → 60 秒 | 覆盖 2 分档 → **120 秒** |
| `activity_detail` | SQL-4，覆盖 60 秒档 | **这个 sql_key 不存在**（实际是 `activity_top10`，5 分档） |

**处置**：**实现以实算为准**（开发方案自己也写了「以实算为准」，
且 `docs/` 不参与 `artifacts_hash`，改它不会让执行计划 stale）。
但**文档要改** —— 归到 **wave-4 交付文档**，否则客户拿到的开发方案
与采集端真实刷新频率对不上。**本次执行内不改 `docs/`**。

### 平台画像固化文件（用户决策，2026-11-23）

**实例 ID 不走配置** —— 采集端连上 TPOPS 后自己发现，固化到文件，以文件为准。

| 项 | 决定 |
|---|---|
| 发现方式 | `GET /rds/v3.2/instances` 取实例列表 → **全部实例都记** → 固化 |
| 固化文件 | `state_dir/platform_profile.json`（原子写，与 `watermark.py` 同一套） |
| **选择规则** | **「选择所有实例」** —— 不挑、不筛、不取第一个、不按 `mode` 筛选。一个平台上挂了几个实例就采几个（`CLAUDE.md` 原始设计：命令中心是「instance_id + node_ids 下发」，无法一次带多实例 → 采集端**按实例循环**）。实例列表为空 → 明确报错 |
| `mode` 字段 | **仍然记录**在画像里（便于排查），**但不参与选择** |
| **`master_count` 口径** | 按**唯一 `node_id`** 计数（**不是数条目** —— 实测分布式实例 `role=master` 有 6 条但只有 3 个唯一 node_id，数条目虚高一倍）。复用 `tpops/command_center_nodes.py::_select_masters` 的现成去重逻辑，不另写一份。**只用于记录与拓扑核对，不参与选择** |
| 复用时机 | 固化后**不再每轮重新发现**（否则平台列表顺序一变就静默增删采集目标，水位与差分基线全部错位）；启动时**核对**实例还在吗 / 有没有新增实例 / `engine_name` 变了吗 / `master_count` 变了吗 |
| 核对不一致时 | 画像里的 instance 不在平台列表 → **ERROR**；平台有画像没记的实例 → **WARNING**（提示「删画像文件可重新发现」，**但不自动加**）；`engine_name` 变 → **WARNING**（平台升级，平台指标码可能变）；`master_count` 变 → **WARNING**（拓扑变了，多 master 归集口径要复核） |
| 可编辑性 | 运维换实例 → 改这个文件即可，**不用改代码、不用改配置** |
| 落地位置 | wave-3 任务 **3.3**（入口装配）—— 简报已补该节 |

> **⚠️ 本节 2026-11-23 更正**：本节此前写的选择规则是
> 「多个实例 → 取 **master 节点数最多**的 → 相同则取第一个 + WARNING」，
> 该口径**已被用户推翻**（提交 `1da62eb`「3.1/3.3 简报改为『选择所有实例』」）。
> 现在的规则是**上表**的「选择所有实例」。`HANDOFF.md` §3 记的是新口径。
> 记录这个冲突是因为它**跨过了一个可执行的口径差异**：
> 「取一个」与「取全部」会导致差分基线作用域、水位 stream 键、投递 Key 的隔离方式完全不同。

**记一条口径**：平台的实例列表接口与节点清单接口**已经存在**
（`tpops/instances.py::fetch_instances` / `fetch_instance_nodes`），
本项不需要新建平台层能力，只是把 D 类采集器顺带取到的信息**独立成画像文件**。

## Wave 4 —— wave-4-delivery-docs（serial）

- [ ] 4.1 交付文档
