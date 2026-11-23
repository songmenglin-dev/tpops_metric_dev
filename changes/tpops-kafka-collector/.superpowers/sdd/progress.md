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

### 平台画像固化文件（用户决策，2026-11-23）

**实例 ID 不走配置** —— 采集端连上 TPOPS 后自己发现，固化到文件，以文件为准。

| 项 | 决定 |
|---|---|
| 发现方式 | `GET /rds/v3.2/instances` 取实例列表 → 选目标实例 → 固化 |
| 固化文件 | `state_dir/platform_profile.json`（原子写，与 `watermark.py` 同一套） |
| **选择规则** | **不按 `mode` 筛选**（用户明确决定）。1 个 → 直接用；多个 → 取 **master 节点数最多**的（多 master 归集逻辑按分布式拓扑写的）；master 数相同 → 取第一个 + **记 WARNING 列出全部候选**；0 个 → 明确报错 |
| 复用时机 | 固化后**不再每轮重新发现**（否则平台列表顺序一变就静默切换目标实例，水位与差分基线错位）；启动时**核对**实例还在吗 / `engine_name` 变了吗 / master 数变了吗 |
| 可编辑性 | 运维换实例 → 改这个文件即可，**不用改代码、不用改配置** |
| 落地位置 | wave-3 任务 **3.3**（入口装配）—— 简报已补该节 |

**记一条口径**：平台的实例列表接口与节点清单接口**已经存在**
（`tpops/instances.py::fetch_instances` / `fetch_instance_nodes`），
本项不需要新建平台层能力，只是把 D 类采集器顺带取到的信息**独立成画像文件**。

## Wave 4 —— wave-4-delivery-docs（serial）

- [ ] 4.1 交付文档
