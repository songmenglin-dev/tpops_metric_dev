# 任务 1.8 报告：规则引擎重写（wave-1）

- 实施方：task-1.8 实施代理
- 工作目录：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（worktree，分支 `tpops_metric_dev`）
- 简报：`.../r2-009778c.../workspace/task-1.8-brief.md`；共享约定：`.../r3-b4766c8.../workspace/wave1-execution-brief.md`

---

## 1. 做了什么

### 1.1 规则引擎重写与拆文件（M-2 / R-M5）

`tpops_kafka_collector/mapping/rules.py` 由 423 行单文件重写为「**门面 + 9 个按 `value_rule.type` 拆分的模块**」：

| 文件 | 行数 | 内容 |
|---|---|---|
| `mapping/rules.py` | 52 | 对外接口 + 模块地图（`evaluate` / `CounterDeltaStore` / `RuleContext` / `RuleError` / `substitute_placeholders` / `build_resource_name`） |
| `mapping/rules_context.py` | 174 | `RuleContext`、`RuleError`、`MetricValue`、数值/过滤/维度工具、`group_by` 执行 |
| `mapping/rules_deltas.py` | 66 | `CounterDeltaStore`（差分基线） |
| `mapping/rules_naming.py` | 63 | 占位符替换、资源名拼装 |
| `mapping/rules_sorting.py` | 112 | PostgreSQL `ORDER BY` 口径的排序 |
| `mapping/rules_row.py` | 118 | `raw` / `raw_filtered` / `aggregate` / `raw_rows` |
| `mapping/rules_series.py` | 100 | `series_point` / `enum_map` / `gauge` |
| `mapping/rules_cross.py` | 85 | `counter_delta` / `ratio_across` |
| `mapping/rules_collection.py` | 69 | `top_n` / `collection_count` / `text_prefix_count` / `instance_field` |
| `mapping/rules_engine.py` | 53 | 分派表 `HANDLERS`（13 种）+ `evaluate` |

约束核验：全部单文件 **< 400 行**（最大 174）；最长函数 `grouped_items` **42 行**（< 50）；
`ruff check tpops_kafka_collector/mapping/` → **All checks passed**。

> **为什么是「门面 + 同级模块」而不是 `rules/` 子包**：先做成了子包，
> 但 wave-1 任务 1.7 落的 `tests/unit/test_package_boundary.py::
> test_package_mapping_layer_is_the_only_implementation` 断言
> `tpops_kafka_collector/mapping/rules.py` **必须是一个存在的文件**。
> 同名目录与同名文件并存会让 Python 优先取包、`.py` 变成死代码。
> 改成同级模块后，那条既有测试**不用改**就恢复绿。

### 1.2 13 种 `value_rule.type` 全部实现

| # | 类型 | 实现位置 | 正常路径 | 边界路径 |
|---|---|---|---|---|
| 1 | `raw` | `rules_row.eval_raw` | `sum` / `per_row` / `first` | 空行、`row_policy` 非法 → `RuleError`、缺列给 None（不编 0） |
| 2 | `raw_filtered` | `rules_row.eval_raw_filtered` | 过滤取首个匹配行 | 无匹配 → 不产出；`cast:int` 遇到非数值 → None；维度带出 |
| 3 | `aggregate` | `rules_row.eval_aggregate` | `filter` → `group_by` → `fn:sum` | `fn` 非 sum → `RuleError`；空行 → 0 |
| 4 | `counter_delta` | `rules_cross.eval_counter_delta` + `rules_deltas` | 差分速率 | 首轮无基线只记基线；**计数归零 → 丢弃本轮 + ERROR**；采集时刻未推进；无 store → `RuleError` |
| 5 | `ratio_across` | `rules_cross.eval_ratio_across` | 分子 ÷ 分母，保留 2 位小数 | **分母取不到 → 不产出**（M-3）；分母可用、分子为 0 → 0.0 |
| 6 | `series_point` | `rules_series.eval_series_point` | `last_non_null` | 全 null → 不产出；`position` 非法 → `RuleError`；时间戳回退 `now_ms` |
| 7 | `enum_map` | `rules_series.eval_enum_map` | 命中映射 | 未命中 → 透传 + **WARNING**；`default:null` → None；`1.0`/`1` 同键 |
| 8 | `gauge` | `rules_series.eval_gauge` | 已运行时长 | 单位不支持 → `RuleError`；无值 → 不产出 |
| 9 | `top_n` | `rules_collection.eval_top_n` | 排序取前 N | **PG `ORDER BY` 排序**（R-2）；空值升降序位置；`collection` 来源；空输入 |
| 10 | `raw_rows` | `rules_row.eval_raw_rows` | 整行列表 | — |
| 11 | `collection_count` | `rules_collection.eval_collection_count` | 集合条数 | 集合缺失 → 0 |
| 12 | `text_prefix_count` | `rules_collection.eval_text_prefix_count` | 前缀分类计数 | 集合缺失 → 0；行缺该字段 → 跳过 |
| 13 | `instance_field` | `rules_collection.eval_instance_field` | 实例字段 | 缺键 → None |

新增测试 `tests/unit/test_rules.py`：**60 条**，13 种类型各有正常路径 + 边界路径，
另含 `group_by` 执行与分派守卫。
（**未动** wave 0 既有的 `tests/test_rules.py`。）

### 1.3 顺带修掉的引擎缺陷

- `top_n` 声明了 `collection` 却仍从 `ctx.rows` 取数（`gaussdb.Top10sql` 取到空列表）→
  现在「声明了 collection 就从集合取」。
- `aggregate` 的 `fn` 参数被完全忽略（写错 `fn` 不会报错）→ 现在只支持 `sum`，其它显式 `RuleError`。
- `series_point` 的 `position` 参数被忽略 → 现在支持 `last_non_null` / `last` / `first_non_null` / `first`，
  其它显式 `RuleError`。
- `raw` 的 `row_policy` 写错会静默按 `first` 走 → 现在显式 `RuleError`。

---

## 2. 四个遗留 Minor 的处置

| 台账号 | 处置 | 理由 |
|---|---|---|
| **M-2 / R-M5**（`rules.py` 超 400 行） | **拆文件**（门面 + 9 个模块，见 §1.1） | 合同硬约束「单文件 < 400 行、函数 < 50 行」 |
| **M-3**（`ratio_across` 分母取不到返回 `0.0`） | **改成不产出**：分母取不到（为 0 或视图缺失）→ 返回空列表 + WARNING；分母可用、分子为 0 → 仍产出 `0.0` | 简报 §「1.8 必须一并处理」明写「与本项目『不静默出 0』的目标一致」；依据是规格 `specs/mapping-and-rules/spec.md` §「缺值不产出假数据」（SHALL NOT 用 0 代替缺失值） |
| **R-M2**（`top_n` 排序实为空操作） | **按 PostgreSQL `ORDER BY` 口径真正排序**（数值按数值、时间戳按时间、文本按字典序；空值升序在后、降序在前；**空串按空值处理**） | 见 §3.3 |
| **R-M3**（`raw`+`per_row` 的 `group_by` 被完全忽略） | **执行 `group_by`**（分组键提供维度值；与 `dimension_bindings` 不一致 → `RuleError`；分组键重复 → WARNING） | **偏离简报偏好的「删参数」路线**，理由见 §2.1 |

### 2.1 R-M3 为什么选「执行」而不是「从映射表删参数」

简报把「删参数」标为「后者更好」，但那条路线在本次执行里**会撞两处硬约束**：

1. **wave-0 既有测试锁住了这个参数**：
   `tests/test_rules.py::test_dimension_metrics_are_always_per_row` 断言
   「带维度的 `raw` / `counter_delta` 规则必须声明 `group_by`」。
   删掉参数 → 这条既有测试变红，而本任务**不得改动 wave 0 的测试文件**。
2. **`group_by` 是自动派生的**：`tools/build_itscada_mapping.py:420`
   `group_by = sorted(set(bindings.values()))` —— 它由 `dimension_bindings` 派生。
   删掉生成逻辑里的这一项，会让「分组键」与「维度绑定」这两份**同源声明**脱钩，
   以后改维度绑定就不会再同步分组键。

另外，从**语义**上看「按 `relname` 分组」本身就不成立：表的真实身份是
`(schemaname, relname)`，只按 `relname` 分组会把不同 schema 的同名表并成一组。
执行它（而不是执行「真正正确的分组」）会把一个**错误声明**变成运行期行为。

采用的口径（写在 `rules_context.grouped_items` 的 docstring 里）：

- `group_by` 是行级规则（`raw` / `counter_delta` 的 `per_row`）的**分组键声明**，
  引擎**真正按它分组**，分组键的值就是维度值；
- 分组键与 `dimension_bindings` 指向的列不一致 → 声明自相矛盾 → `RuleError`（不静默取其一）；
- 分组键重复（同名的两张表落进同一个键）→ **WARNING**：维度值不唯一，两条消息在幂等层
  （`ids.metric_idempotency_key`）会撞键 —— 显式报出来，不静默。

**留档证据**：三个 master 节点的 `stat_user_tables` 各 68 行、**68 个互不相同的 `relname`**
（`data/merged_sql/sql_merge_probe_result.json`），因此执行分组后**输出与现状逐条相同**，
期望值表不需要重生成。

### 2.2 R-M3 处置带来的连带改动

`data/mapping/kafka_field_mapping.json`（生成物）与包内副本已重新生成/同步 ——
变化的 3 条指标，全部是口径变化的**如实反映**：

- `gaussdb.Total_pct`、`GAUSSDB.session_usage`：`message_count` 1 → **0**、`sample_message` → `null`。
  原因：这两个是**跨视图比率**，生成器用的是**单视图**留档（`tools/build_kafka_mapping.py`
  走 `decode()` 单视图路径），分子/分母的另一半根本不在上下文里 → 分母取不到 →
  按 M-3 不产出。旧值 `0.0` 是**编出来的**（分母缺失被当成 0），新行为是诚实的。
  这两条指标的真实值由**合并留档**的期望值表守着（`tests/test_expected_values.py` 覆盖它们）。
- `GAUSSDB.sessions_active_top10`：`sample_message` 的行序变了 —— 这正是 R-2 修复生效的体现
  （旧排序是空操作，行序 = 留档原序）。

---

## 3. 改过的每一处口径判断及其**独立**依据

> 依据来源：规格行号 / 映射表 `note` / 实测留档形态 / **独立推导器** `tools/expected_deriver.py`。
> 引擎的行为**不是**照抄旧 `rules.py`。

### 3.1 `ratio_across` 分母取不到 → 不产出（M-3）

- **依据 1（规格）**：`specs/mapping-and-rules/spec.md` §「缺值不产出假数据」：
  「当某个数据点取不到值时，采集端 SHALL **不产出该条消息**，SHALL NOT 用 0 代替缺失值」。
- **依据 2（规格的冲突 ⚠️）**：同文件 §「跨视图比率」场景写的是「分母为 0 时结果为 0，
  SHALL NOT 抛出除零异常」。**这一句与上面的「缺值不产出」冲突**，
  本波次按控制器简报（wave-1 brief §「1.8 必须一并处理 wave 0 遗留的 4 个 Minor」）
  取「不产出」一侧 —— 见 §6 遗留问题（建议人工裁决时同步改规格这一行）。
- **依据 3（独立推导器）**：`tools/expected_deriver.py` 的 `_derive_ratio` 同步改成
  「分母取不到 → `return []`」。**先改推导器、再改引擎**，然后看
  `tests/test_expected_values.py` 是否红 —— **没有红**（见 §5），
  因为留档里分母（`stat_database.numbackends` 之和）恒 > 0，两边口径一致。

### 3.2 `raw_filtered` 多命中的口径（R-M1）

- **口径**：**多命中取首个匹配行**（`rows[0]` of the matched list）。
- **依据**：`tools/expected_deriver.py::_derive_raw_filtered` 独立实现的就是
  「过滤后取 `matched[0]`」；两边一致 → 期望值表守得住。
  留档上每个 filter 恰命中 1 行（「首行 vs 末行」不可区分），
  所以这条口径由**合成多命中守卫**守着：
  `tests/unit/test_rules.py::test_raw_filtered_takes_the_first_matching_row`
  （样本里 `name='port'` 有两行 `'5432'` / `'6432'`，断言取 `'5432'`）。
- 规格里目前**没有**这条定义 —— 见 §6（建议补进 `spec.md` 的 §「取值规则的类型集」）。

### 3.3 `top_n` 的排序口径（R-2）

- **依据 1（wave-0 评审）**：`.superpowers/sdd/reviews/wave-0-mapping-baseline.md` Minor-2
  明确「`top_n` 的排序实为空操作（`_to_number` 把时间戳字符串全退化成 `0.0`）」，
  要求「按 PostgreSQL `ORDER BY` 口径真正排序（字符串按字典序；时间戳字符串解析成时间比较；
  空值：升序在后、降序在前）」—— 本次实现与该描述逐条对应。
- **依据 2（独立推导器）**：`tools/expected_deriver.py::_sorted_rows` 已经实现了 PG 口径
  （数值/时间戳/文本三种键 + 空值位置）。引擎另起一份实现（`rules_sorting.py`），
  语义与它一致 → `tests/test_expected_values.py` 里 82 条 B 类指标**逐条相等**（含
  两条 `top_n`：`GAUSSDB.sessions_active_top10`、`gaussdb.running.sql.limit.20`）。
- **依据 3（留档形态）**：排序列 `query_start` 的实测形态是
  `'2026-11-18 16:27:07.557567+08'`（带时区、微秒）→ 解析格式取
  `'%Y-%m-%d %H:%M:%S.%f%z'` 与 `'%Y-%m-%d %H:%M:%S%z'`。

### 3.4 「空串」按「空值」处理（排序）——**本次新增的口径判断**

- **依据（留档形态）**：**同一批行**，逐指标留档（`data/metric_response/…`）把 NULL 的
  `query_start` 渲染成**空串** `''`，合并留档（`data/merged_sql/…`）渲染成 `null`。
  空串若当普通文本值参与排序，会排到所有时间戳之前（`'' < '2026-…'`），
  「按时间取 Top 10」会把**没有时刻**的行排在最前 —— 与 SQL 自己的
  `order by query_start`（NULL 升序在后）正好相反。
- **处置**：引擎（`rules_sorting.is_missing`）与独立推导器（`expected_deriver._is_empty`）
  **同时**把 `None` 与空串当空值。
- **对期望值表的影响**：合并留档里是 `null` 不是 `''`，两种口径结果相同 →
  `expected_values.json` **逐字节未变**。

### 3.5 `group_by` 的执行口径（R-3）

见 §2.1。

### 3.6 未改动的口径（复核过，确认保持）

| 口径 | 依据 |
|---|---|
| `counter_delta` 首轮只记基线、值给 None | 规格 §「累计计数的跨轮差分」；wave-0 既有测试 `test_counter_delta_first_round_has_no_value` |
| 计数归零 → 丢弃本轮 + **ERROR** | 规格 §「实例重启归零的处理」；本次把 ERROR 日志落到 `rules_deltas.py`（旧实现只返回一句说明，没人打日志） |
| 差分用**实际采集时刻差** | 规格同节；`deltas.rate` 用 `(now_ms - last_ms)/1000` |
| 差分基线键形状 `{(metric_code, dimension_key): (value, ms)}` | `tpops_kafka_collector/watermark.py:340-353` 的 `_baselines_container` 按同一形状存取 —— **未改动**（`tests/unit/test_watermark.py::test_real_counter_delta_store_baseline_keys_are_compatible` 绿） |
| `state_contains` = 子串包含、忽略大小写 | wave-0 合成守卫 `tests/test_expected_values.py::test_state_contains_matches_rows_with_the_keyword` —— **未动** |
| `raw_filtered` 的维度取该列**原值**、`cast` 只作用于指标值 | 期望值表里 `gaussdb_{#PORT}port.status` 的 `dimensions.PORT = "8000"`（字符串）、`value = 8000`（整数）—— 引擎与推导器一致 |
| 缺列 → 值 None（不编 0） | 规格 §「缺值不产出假数据」；**不产出消息**的决定在采集端（wave 3）—— wave-0 的 `test_metric_message_keeps_missing_value_as_null` 证明消息层保留 None。`raw` 的 `sum` 不会因缺列而出 0，因为**启动期列存在性校验**（`mapping/columns.py`）已经挡住不存在的列 |

---

## 4. 期望值表是否需要重新生成

**结论：需要重跑生成器，但产出的文件逐字节未变。**

- 推导器改了（§3.1 M-3、§3.4 空串口径）→ 按简报要求**先改推导器再改引擎**，
  然后跑 `python tools/build_expected_values.py` 重生成；
- 结果：`data/mapping/expected_values.json` **git diff 为空**（82 条 B 类指标的期望值一条没变）。
  原因：留档里 `ratio_across` 的分母恒 > 0（M-3 不触发）、空时间戳是 `null` 不是 `''`（§3.4 不触发）；
- `tests/test_expected_values.py::test_file_is_up_to_date`（期望值表 == 生成器输出）**绿**。

**重新生成并同步的另一个生成物**：`data/mapping/kafka_field_mapping.json`
（引擎输出变了 → 生成物变），包内副本 `tpops_kafka_collector/mapping/data/kafka_field_mapping.json`
已 `cp` 同步（`cmp` 逐字节一致）。变化内容见 §2.2（3 条指标）。

---

## 5. RED → GREEN

### 5.1 RED（新测试先写，跑出来的失败断言）

`pytest tests/unit/test_rules.py -q` → **15 failed, 42 passed**。失败断言（摘要）：

| 失败用例 | RED 时的失败断言 |
|---|---|
| `test_ratio_across_with_zero_denominator_produces_nothing` | 期望 `[]`，实得 `[({}, 0.0, 0)]`（分母为 0 编出 0.0） |
| `test_ratio_across_with_missing_denominator_view_produces_nothing` | 期望 `[]`，实得 `[({}, 0.0, 0)]`（视图缺失编出 0.0） |
| `test_top_n_sorts_timestamps_by_time_not_by_text` | 期望 `['2026-11-18 …', '2026-11-13 .855510', '2026-11-13 .749342']`，实得留档原序（排序是空操作） |
| `test_top_n_sorts_timestamps_ascending` | 期望升序，实得留档原序 |
| `test_top_n_puts_nulls_last_when_ascending` / `…_first_when_descending` | 空值位置与 PG 口径不符 |
| `test_top_n_reads_the_declared_collection` | `KeyError/空列表`（声明了 collection 却从 `ctx.rows` 取） |
| `test_raw_unknown_row_policy_raises` | `Failed: DID NOT RAISE`（`row_policy` 写错静默按 first 走） |
| `test_aggregate_unsupported_fn_raises` | `Failed: DID NOT RAISE`（`fn` 参数被忽略） |
| `test_series_point_unknown_position_raises` | `Failed: DID NOT RAISE`（`position` 参数被忽略） |
| `test_counter_delta_drops_the_round_when_the_counter_resets` | `assert errors` 失败 —— 计数归零**没有 ERROR 日志** |
| `test_enum_map_passes_an_unknown_value_through` | `assert any(...WARNING...)` 失败 —— 枚举未命中**没有告警级日志**（规格要求） |
| `test_group_by_reports_a_repeated_group_key` | 无 WARNING（`group_by` 被完全忽略） |
| `test_group_by_inconsistent_with_the_dimension_bindings_raises` | `Failed: DID NOT RAISE`（声明不一致不报错） |
| `test_raw_filtered_carries_dimension_values` | （**测试自身的笔误**：漏了行元组的逗号，迭代出的是 dict 的键 `'name'`；已修为期望维度取原值 `'5432'`） |

### 5.2 GREEN

```
$ conda run -n tpops python -m pytest tests/unit/test_rules.py -q
60 passed in 0.33s

$ conda run -n tpops python -m pytest -q
695 passed in 11.21s      # 任务开始时的基线是 548 passed（其间并发任务也在加测试）
```

`ruff check tpops_kafka_collector/mapping/ tests/unit/test_rules.py` → **All checks passed**。
（`ruff check tpops_kafka_collector/` 全量仍有 16 条 **UP031/UP004**，全部在
`tpops_kafka_collector/tpops/client.py` —— **并发任务 1.6 的文件**，不是本次改动。）

---

## 6. 偏离简报之处及原因

| 偏离 | 原因 |
|---|---|
| **R-M3 选「执行 `group_by`」而不是简报偏好的「删参数」** | 见 §2.1：wave-0 既有测试 `tests/test_rules.py::test_dimension_metrics_are_always_per_row` 断言该参数必须存在，而本任务不得改 wave 0 的测试文件；且 `group_by` 由 `dimension_bindings` 自动派生（`tools/build_itscada_mapping.py:420`），删掉会让两份同源声明脱钩。**没有改 `tools/build_itscada_mapping.py`，也没有重生成 `metric_rules.json`** |
| **规则引擎拆成「门面 + 9 个同级模块」而不是 `rules/` 子包** | wave-1 任务 1.7 的 `tests/unit/test_package_boundary.py` 断言 `mapping/rules.py` 是存在的**文件**；子包方案会让那条既有测试变红 |
| **新增 `tests/unit/__init__.py`** | `tests/test_rules.py`（wave-0）与 `tests/unit/test_rules.py`（本次）**同名**，pytest 的 rootdir 插入模式会撞模块名（`import file mismatch` → 收集直接失败）。加 `tests/unit/__init__.py` 让两者分别成为 `test_rules` 与 `unit.test_rules`。副作用：`tests/unit/` 下其余测试的模块名也变成 `unit.test_*`（全量测试仍全绿） |
| **改了 3 个 wave-0 文件**：`tests/test_response_rules.py`、`tests/test_kafka_mapping.py`、`tools/expected_deriver.py` | 前两个：M-3 让「单视图留档下的跨视图比率」不再产出值，这两条用例的断言（「每个规则都产出值」「聚合指标产出 1 条消息」）与新口径直接冲突 —— 加了**显式的例外分支 + 注释说明**（比率指标记 0 条、真实值由合并留档的期望值表守）。第三个：M-3 要求「先改独立推导器再改引擎」 |

---

## 7. 遗留问题 / 需要人工裁决

1. ⚠️ **规格内部冲突**：§「跨视图比率」写「分母为 0 时结果为 0」，§「缺值不产出假数据」写
   「SHALL NOT 用 0 代替缺失值」。本波次按控制器简报取「不产出」。
   **建议人工裁决时把 `specs/mapping-and-rules/spec.md` 那一行同步改掉**（本任务不改 `changes/` 下文件）。
2. ⚠️ **R-M1 缺规格定义**：规格里没有「`raw_filtered` 多命中取首个匹配行」的定义。
   本次把口径写进了代码 docstring + 合成守卫测试，但**规格里仍没有** —— 建议补进
   `spec.md` §「取值规则的类型集」。
3. ⚠️ **规格里也没有 `group_by` 在行级规则中的定义**（本次定的口径：分组键提供维度值、
   与维度绑定不一致即报错、分组键重复记 WARNING）。建议一并补进规格。
4. **遗留**：`data/mapping/kafka_field_mapping.json` 里两条跨视图比率指标的
   `message_count` 现在是 0（单视图留档算不出比率）。建议后续让
   `tools/build_kafka_mapping.py` 对 `ratio_across` 规则改用**合并留档**上下文回放，
   让声明里带上真实的样例消息。
5. **`tools/expected_deriver.py` 与引擎的空串口径是两份实现**：两边都把空串当空值
   （依据同为「平台把 NULL 时间戳渲染成空串」的留档形态）。若将来留档出现别的空值渲染形态，
   两边都要一起改 —— **不要只改一边**。
6. **未做**：`raw` 的 `sum` 遇到「所有行都缺该列」仍出 0。实践上不会发生（启动期
   `mapping/columns.py` 的列存在性校验会先失败），但如果将来放宽那条校验，这里要补。

---

## 8. 交付文件清单

**新增（生产代码）**
- `tpops_kafka_collector/mapping/rules_context.py`
- `tpops_kafka_collector/mapping/rules_deltas.py`
- `tpops_kafka_collector/mapping/rules_naming.py`
- `tpops_kafka_collector/mapping/rules_sorting.py`
- `tpops_kafka_collector/mapping/rules_row.py`
- `tpops_kafka_collector/mapping/rules_series.py`
- `tpops_kafka_collector/mapping/rules_cross.py`
- `tpops_kafka_collector/mapping/rules_collection.py`
- `tpops_kafka_collector/mapping/rules_engine.py`

**重写**
- `tpops_kafka_collector/mapping/rules.py`（门面 + 文档）
- `tools/expected_deriver.py`（M-3 + 空串口径；独立推导器）

**新增（测试）**
- `tests/unit/test_rules.py`（60 条）
- `tests/unit/__init__.py`（解同名模块冲突）

**修改**
- `tests/test_response_rules.py`（跨视图比率的显式例外）
- `tests/test_kafka_mapping.py`（跨视图比率的显式例外）
- `data/mapping/kafka_field_mapping.json`（生成物重生成）
- `tpops_kafka_collector/mapping/data/kafka_field_mapping.json`（包内副本同步）
- `tpops_kafka_collector/mapping/README.md`（文件布局补上拆分后的模块）

**未动**
- `data/mapping/metric_rules.json`、`data/mapping/expected_values.json`（逐字节未变）
- `tests/test_rules.py`（wave-0 既有测试）
- `tpops_kafka_collector/watermark.py`、`tpops_kafka_collector/ids.py`、`tpops_kafka_collector/errors.py`
- 并发任务的文件：`retry.py`、`kafka/*`、`tpops/client.py`

**未提交**（按简报要求不 `git commit`，由控制器统一提交）。
