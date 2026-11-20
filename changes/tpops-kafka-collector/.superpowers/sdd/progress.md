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
- **`pg_stat_activity.state` 平台侧盲区**（实施方提出）—— 已核实：实测留档 `activity_agg`
  的 `state` 取值**只有 `{active, idle}`**，列只有 `{cnt, datname, state}`。
  `gaussdb.Lockwaiting`（`state_contains:["lock"]`）与 `gaussdb.Waiting_conn`
  （`state_contains:["wait"]`）在留档上**匹配 0 行** → 恒为 0。PG 9.2 系的
  `pg_stat_activity.state` 本就没有 lock/wait 取值（那是 9.6+ 的 `wait_event` 体系），
  这两条指标**可能结构性不可提供**（98 条可提供里的 2 条）。
  → **待用户/客户决策**（是否列入 `data/mapping/open_items.json`）。

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

- [ ] 2.1 映射数据复制进包
- [ ] 2.2 合并 SQL 的运行时渲染

## Wave 2b —— wave-2b-channels（parallel）

- [ ] 2.3~2.10（A 类 / 命令中心 / B 类 / C 类 / D 类 / E 类 / 脱敏）

## Wave 3 —— wave-3-scheduling-testing（serial）

- [ ] 3.1~3.5（调度器 / 健康端点 / 入口 / 回归夹具 / 门禁）

## Wave 4 —— wave-4-delivery-docs（serial）

- [ ] 4.1 交付文档
