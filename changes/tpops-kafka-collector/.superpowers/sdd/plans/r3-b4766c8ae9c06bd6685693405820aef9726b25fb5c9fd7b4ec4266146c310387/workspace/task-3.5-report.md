# 任务 3.5（前半段）报告 —— 映射层覆盖率补测

> 范围：**只补测试**。没有改 `tpops_kafka_collector/` 下任何文件，
> 没有改 `specs/` / `data/mapping/*.json` / `execution-contract.md` / `tasks.md`，
> **没有 `git commit`**（按提交纪律留给控制器）。
> 本报告的每个数字都是实测值（复现命令见 §6）。

---

## 0. 结论

| 项 | 补测前 | 补测后 | 门槛 |
|---|---|---|---|
| **映射层合计** | 1287 stmts / 98 miss = **92.4%** | 1287 stmts / **1 miss** = **99.9%** | ≥95% ✅ |
| **映射层最低单文件** | `rules_cross.py` **76.9%** | `rules_sorting.py` **98%**（唯一未覆盖行是死代码） | 每个文件 ≥95% ✅ |
| 全量测试 | 1295 passed | **1479 passed**（我的 130 条 + 并行实施方的 54 条） | 全绿 ✅ |
| `ruff check tpops_kafka_collector/` | 0 | **0** | 0 ✅ |
| `pytest tests/test_py37_compat.py -q` | 112 passed | **132 passed** | 通过 ✅ |

---

## 1. 映射层逐文件覆盖率对比（补测前 → 补测后）

补测前的基线由控制器给出（2026-11-23 实测），我本地复跑一致；补测后为本地实测。

| 文件 | 前 Stmts/Miss | 前覆盖 | 后 Stmts/Miss | 后覆盖 | 后未覆盖行 |
|---|---|---|---|---|---|
| `mapping/rules_cross.py` | 39 / 9 | 76.9% | 39 / **0** | **100%** | — |
| `mapping/decode.py` | 170 / 28 | 83.5% | 170 / **0** | **100%** | — |
| `mapping/rules_deltas.py` | 48 / 7 | 85.4% | 48 / **0** | **100%** | — |
| `mapping/rules_sorting.py` | 57 / 8 | 86.0% | 57 / **1** | **98.2%** | 110（死代码，见 §4.1） |
| `mapping/columns.py` | 100 / 12 | 88.0% | 100 / **0** | **100%** | — |
| `mapping/rules_context.py` | 85 / 6 | 92.9% | 85 / **0** | **100%** | — |
| `mapping/loader.py` | 106 / 7 | 93.4% | 106 / **0** | **100%** | — |
| `mapping/message.py` | 62 / 4 | 93.5% | 62 / **0** | **100%** | — |
| `mapping/sqlgen.py` | 89 / 6 | 93.3% | 89 / **0** | **100%** | — |
| `mapping/validation.py` | 90 / 6 | 93.3% | 90 / **0** | **100%** | — |
| `mapping/table.py` | 136 / 7 | 94.85% | 136 / **0** | **100%** | — |
| `mapping/rules_series.py` | 59 / 3 | 94.9% | 59 / **0** | **100%** | — |
| `mapping/nodes.py` | 44 / 2 | 95.0% | 44 / **0** | **100%** | — |
| `mapping/rules_row.py` / `rules_collection.py` / `rules_naming.py` / `rules_engine.py` / `rules.py` / `masking.py` / `__init__.py` | — | 100% | — | 100% | — |
| **映射层合计** | **1287 / 98** | **92.4%** | **1287 / 1** | **99.92%** | — |

**验收口径（取最严：每个文件 ≥95% 且合计 ≥95%）—— 已满足。**

> 注：全仓 `TOTAL` 由 93% 变成 95%，但剩下的缺口**不在映射层**：
> `collectors/*`、`kafka/*`、`tpops/*`、`health.py`、`watermark.py`，以及**并行实施方本轮新增**的
> `scheduler*.py`（`scheduler_signals.py` 42%、`scheduler.py` 86% 等）。那些不是本任务的范围。

---

## 2. 变异验证证据（6 条，实际输出）

每条都是：临时改坏实现里的**一个具体表达式** → 跑对应测试文件 → 确认变红 → 立刻还原。
还原后 `git diff --stat` 为空（`git status --short` 只剩新增的测试文件与并行实施方的新文件）。

### 变异 1 —— `mapping/decode.py`：合并形态的「行数必须为 1」守卫

* 变异点：`if len(payload) != 1:` → `if len(payload) < 0:`
* 预期：`test_decode_merged_reports_the_actual_row_count` 变红

```
mutated: len(payload) != 1  ->  len(payload) < 0
>       row = payload[0]
E       IndexError: list index out of range
tpops_kafka_collector/mapping/decode.py:209: IndexError
FAILED tests/unit/test_mapping_decode_gaps.py::test_decode_merged_reports_the_actual_row_count
1 failed, 32 passed in 0.38s
```

### 变异 2 —— `mapping/rules_cross.py`：`ratio_across` 的「分母不可用 → 不产出」守卫

* 变异点：`if not denominator:` → `if False:`
* 预期：`test_ratio_across_produces_nothing_when_the_denominator_view_is_missing` 与
  `..._when_the_denominator_is_zero` 变红（变成除零异常）

```
mutated: if not denominator -> if False
>       return [({}, round(numerator / denominator, 2), ctx.now_ms)]
E       ZeroDivisionError: float division by zero
tpops_kafka_collector/mapping/rules_cross.py:67: ZeroDivisionError
FAILED tests/unit/test_mapping_rules_gaps.py::test_ratio_across_produces_nothing_when_the_denominator_view_is_missing
FAILED tests/unit/test_mapping_rules_gaps.py::test_ratio_across_produces_nothing_when_the_denominator_is_zero
2 failed, 31 passed in 0.44s
```

### 变异 3 —— `mapping/columns.py`：来源列存在性校验

* 变异点：`if column not in available:` → `if False:`
* 预期：`test_source_column_problems_reports_a_column_that_the_view_does_not_have` 与
  `test_validate_source_columns_raises_on_a_missing_column` 变红（校验失效 → 启动不再失败）

```
mutated: if column not in available -> if False
>           validate_source_columns(rules, {'view': {'other'}})
E           Failed: DID NOT RAISE <class 'tpops_kafka_collector.errors.MappingError'>
FAILED tests/unit/test_mapping_columns_gaps.py::test_source_column_problems_reports_a_column_that_the_view_does_not_have
FAILED tests/unit/test_mapping_columns_gaps.py::test_validate_source_columns_raises_on_a_missing_column
2 failed, 42 passed in 1.00s
```

### 变异 4 —— `mapping/rules_sorting.py`：时间戳列的比较类型

* 变异点：`return 'timestamp' if parse_timestamp(text) is not None else 'text'` → `return 'text'`
* 预期：`test_sort_rows_compares_timestamps_by_time_not_by_text` 变红（排序退化成字典序）

```
mutated: timestamp kind detection -> always text
>       assert ordered == [
E       AssertionError: assert ['2026-11-18 ...000000+08:00'] == ['2026-11-18 ...000000+00:00']
E       At index 0 diff: '2026-11-18 09:00:00.000000+00:00' != '2026-11-18 16:27:07.000000+08:00'
FAILED tests/unit/test_mapping_rules_gaps.py::test_sort_rows_compares_timestamps_by_time_not_by_text
1 failed, 32 passed in 0.39s
```

### 变异 5 —— `mapping/message.py`：告警严重级别的默认优先级回落

* 变异点：`priority = definition.default_priority` → `priority = None`
* 预期：`test_alarm_priority_falls_back_to_the_definition_default` 变红

```
mutated: priority fallback -> None
>       assert message['priority'] == 3
E       assert None == 3
FAILED tests/unit/test_mapping_message_sqlgen_gaps.py::test_alarm_priority_falls_back_to_the_definition_default
1 failed, 19 passed in 0.37s
```

### 变异 6 —— `mapping/sqlgen.py`：合并 SQL 声明的视图结构校验

* 变异点：`if not sql_key or not sql:` → `if False:`
* 预期：`test_load_spec_rejects_a_view_without_a_sql_key` / `..._without_sql` 变红

```
mutated: view sql_key/sql guard -> if False
>           sql=' '.join(sql.split()),
E           AttributeError: 'NoneType' object has no attribute 'split'
tpops_kafka_collector/mapping/sqlgen.py:143: AttributeError
FAILED tests/unit/test_mapping_message_sqlgen_gaps.py::test_load_spec_rejects_a_view_without_a_sql_key
FAILED tests/unit/test_mapping_message_sqlgen_gaps.py::test_load_spec_rejects_a_view_without_sql
2 failed, 18 passed in 0.63s
```

---

## 3. 新增测试文件清单

| 文件 | 用例数 | 行数 | 覆盖的模块 |
|---|---|---|---|
| `tests/unit/test_mapping_decode_gaps.py` | **33** | 264 | `decode.py`（decode / decode_merged / merge_pages 的全部错误分支与边界） |
| `tests/unit/test_mapping_rules_gaps.py` | **33** | 332 | `rules_cross.py`、`rules_deltas.py`、`rules_sorting.py`、`rules_context.py`、`rules_series.py`、`nodes.py` |
| `tests/unit/test_mapping_columns_gaps.py` | **44** | 480 | `columns.py`、`validation.py`、`loader.py`、`table.py` |
| `tests/unit/test_mapping_message_sqlgen_gaps.py` | **20** | 277 | `message.py`、`sqlgen.py` |
| **合计** | **130** | 1353 | — |

**行数说明**：全部 4 个文件都在 264~480 行之间。
`test_mapping_columns_gaps.py` 480 行**超过 400 行**——按任务简报「测试文件按仓库既有惯例放宽」处理；
超出原因是它同时覆盖 4 个模块（来源列校验 / 启动期校验 / 映射表加载 / 查表接口），
其中「启动期校验」需要 10+ 条互不相同的规则构造。仓库既有测试文件
（`tests/unit/test_rules.py` 665 行、`tests/unit/test_client.py` 680 行）同样超出该阈值。
**没有新增 1000 行以上的文件**；若控制器要求拆分，可按
`test_mapping_columns_gaps.py` → `test_mapping_validation_gaps.py` + `test_mapping_table_gaps.py` 一刀切。

### 新增测试覆盖的口径（对照任务简报的优先方向）

1. **`decode.py`（84%→100%）**：`DecodeError` 的每条路径；`merge_pages` 的「只拼 `paged=true` 的视图、
   固定视图只取第一页」；视图列为 `''` / `null` / JSON `null` → 空列表（不抛异常）；
   视图列是坏 JSON / 不是行列表 / 行不是对象 → 报错（不静默返回空列表）。
2. **`rules_cross.py`（77%→100%）**：冻结口径的三种情形全部有守卫测试 ——
   **分母视图缺失 → 不产出**、**分母为 0 → 不产出**、**分子 0 分母正 → 产出 `0.0`**；
   外加 `side_value` 的 `sum` / `max` / `count` / 未实现的 `fn`。
3. **`columns.py`（88%→100%）**：**多节点列并集**（R-M6 点名的那条）、
   单节点缺 `result` 跳过、留档结构三类报错、视图缺失 / 列缺失的问题清单。
4. **`rules_sorting.py`（86%→98%）**：没有排序列、布尔列按文本比、时间戳列**按时间**比、
   留档 `+08` 两位时区格式的现状（见 §4.2）。
5. **`rules_deltas.py`（85%→100%）**：`forget`、作用域隔离（多 master 不互相覆盖）、
   底层存储没有 `forget` 时的静默跳过。
6. 其余（`rules_context` / `rules_series` / `loader` / `message` / `sqlgen` / `table` / `validation`）
   按未覆盖行号逐条补，每条都是行为断言。

---

## 4. 发现但**没有修**的问题（交由控制器裁决）

### 4.1 `mapping/rules_sorting.py:110` 是死代码（Python 3.7.9 下不可达）

```python
104    for fmt in TIMESTAMP_FORMATS:
105        try:
106            parsed = datetime.datetime.strptime(text, fmt)
107        except ValueError:
108            continue
109        if parsed.tzinfo is None:      # ← 覆盖到了
110            parsed = parsed.replace(tzinfo=datetime.timezone.utc)   # ← 唯一未覆盖行
111        return parsed
```

`TIMESTAMP_FORMATS` 的两个格式都带 `%z`，而 Python 3.7.9 的 `_strptime` 里 `'z'` 的正则是
`(?P<z>[+-]\d\d:?[0-5]\d(:?[0-5]\d(\.\d{1,6})?)?|Z)` —— **必须匹配到时区**（或 `Z`），
否则 `strptime` 直接 `ValueError`。所以「解析成功 ⇒ `tzinfo` 非 None」，
第 110 行的「把 naive 时间当 UTC」分支**永远走不到**。

实测（`conda run -n tpops python`）：

```
'2026-11-18 16:27:07.557567+08:00' 2026-11-18 16:27:07.557567+08:00
'2026-11-18 16:27:07.557567+0800' 2026-11-18 16:27:07.557567+08:00
'2026-11-18 16:27:07.557567Z'      2026-11-18 16:27:07.557567+00:00
'2026-11-18 16:27:07.557567+08'    None      ← 两位时区解析不了
'2026-11-18 16:27:07.557567'       None
```

**处置**：本轮只加测试，**没有改实现**。该文件因此停在 56/57 = **98.2%**，
仍高于 95% 门槛，不影响验收。若控制器认为该行该删或该改成 `%z` 可选，
请作为独立小变更处理（改动会让 `TIMESTAMP_FORMATS` 的语义变化，需要重新审视）。

### 4.2 留档里的 `+08` 两位时区格式在 Python 3.7.9 下**解析不了** → 时间戳列实际按**文本**比

`rules_sorting.py` 的 docstring 说「排序列是时间戳文本 → 按时间比」，
但实测留档里的时刻形如 `2026-11-18 16:27:07.557567+08`（**两位时区**），
在 3.7.9 的 `%z` 下 `parse_timestamp` 返回 `None` → `_value_kind` 判成 `'text'` → **按字典序比**。

- 后果：**同一偏移下字典序 == 时间序**，所以 `top_n` 的实际输出仍然正确
  （现有 `test_top_n_sorts_timestamps_by_time_not_by_text` 也照样绿）。
  只有当**同一列里混了不同偏移**时才会错。
- 我用 `test_sort_rows_compares_timestamps_by_time_not_by_text`（用 `+08:00` 与 `+00:00`，
  字典序与时间序**结论相反**）把「按时间比」这条分支真正守住；
  并用 `test_sort_rows_falls_back_to_text_order_for_the_two_digit_offset_format`
  把留档格式的**现状**钉住。
- **这意味着简报里「R-M2：`top_n` 排序退化成 `0.0`、输出顺序 = 留档顺序」的描述已过期** ——
  现状是 `eval_top_n` → `sort_rows`（PostgreSQL `ORDER BY` 口径），
  `tests/unit/test_rules.py` 里已有 6 条 `top_n` 排序测试。我没有改实现，只在报告里更正。

### 4.3 规格与冻结口径的不一致（**已登记偏差 T-6，本次未动规格**）

`changes/tpops-kafka-collector/specs/mapping-and-rules/spec.md:156` 写「分母为 0 时结果为 0」，
与控制器冻结的口径（分母为 0 → **不产出**）相反。
按任务简报要求：**不改 `specs/`**（改了会让执行计划 stale、前四个波次的 receipt 作废）。
本轮补的守卫测试按**冻结口径**断言（`test_ratio_across_produces_nothing_when_the_denominator_is_zero`）。

### 4.4 `raw_filtered` 多命中语义（规格未定义）

当前实现取**首个**匹配行。我补了一条合成多命中的守卫测试
（`test_raw_filtered_takes_the_first_matching_row` 已在 `tests/unit/test_rules.py` 中，
本次未重复添加），语义仍**未写进规格**。留档上每个 filter 恰命中 1 行，
「首行 vs 末行」在真实数据上不可区分 —— 留给收尾后的独立小变更补规格。

### 4.5 其它（不影响本次验收）

- `mapping/loader.py` 的 `_parse_rules` / `_parse_customer_metrics` / `_parse_observed` /
  `_read_json` 是**下划线私有**函数，我的测试直接调了它们。理由：这几条错误路径
  通过 `load_mapping(data_dir=...)` 也能覆盖，但需要在 tmp 目录里逐个改坏 8 个映射文件，
  断言会散在多处、看不出「哪一条校验坏了」。私有函数的测试在本仓库已有先例
  （`tests/unit/test_mapping_loader.py` 直接调 `loader._read_json` 等）。
- `mapping/table.py` 的 `MappingTable` 是**离线工具用**的旧查表入口（生产路径是
  `loader.LoadedMapping`）。我按简报要求把 `table.py` 的缺口也补齐了（94.85% → 100%），
  但如果控制器认为 `table.py` 属于历史遗留、可从验收口径里剔除，这部分测试可以撤掉。

---

## 5. 「能让它失败的小改动」自检结论

| 文件 | 自检结论（举例：把实现里的哪个表达式改坏 → 哪条测试会红） |
|---|---|
| `test_mapping_decode_gaps.py` | ① `decode.py:205` 的 `len(payload) != 1` → `test_decode_merged_reports_the_actual_row_count` 红（已实测）；② `decode.py:235` 的 `if value is None` 去掉 → JSON `null` 的视图列会变成「不是行列表」报错 → `test_decode_merged_treats_a_json_null_view_column_as_empty_rows` 红；③ `decode.py:163` 的 `if _is_paged(views[name])` 改成恒真 → 固定视图会跨页累加 → `test_merge_pages_concatenates_only_the_paged_views` 红；④ `decode.py:236` 的 `return ()` 改成 `return ([],)` → `test_decode_merged_treats_a_json_null_view_column_as_empty_rows` 红；⑤ `decode.py:44` 的 `kind == 'rows_from_result'` 分支删掉 → 未知形状的报错文案变了 → `test_decode_rejects_an_unknown_shape` 红。 |
| `test_mapping_rules_gaps.py` | ① `rules_cross.py:60` 的 `if not denominator` → `if False` → 两条分母守卫测试红（**已实测**）；② `rules_cross.py:78-85` 的 `fn` 分派把 `'max'` 写成 `'maxx'` → `test_side_value_max_takes_the_largest_row` 红；③ `rules_cross.py:42` 的 `sum_values([...])` 改成只取第一行 → `test_counter_delta_sum_policy_diffs_the_whole_view_total` 红（会得到 2.0 而不是 3.0）；④ `rules_deltas.py:97` 的 `pop(key, None)` 改成 `pop(key)` → `test_forget_of_an_unknown_dimension_is_a_noop` 红（KeyError）；⑤ `rules_deltas.py:66` 的 `scoped_dimension_key(scope, ...)` 去掉 scope → `test_scoped_stores_keep_their_baselines_apart` 红（A 会跟 B 的基线比，算出 87.0 而不是 2.0）；⑥ `rules_sorting.py:89` 的时间戳判定 → `test_sort_rows_compares_timestamps_by_time_not_by_text` 红（**已实测**）；⑦ `rules_sorting.py:79` 的 `isinstance(value, bool)` 分支删掉 → 布尔列会按数值比 → `test_sort_rows_compares_boolean_columns_as_text` 红；⑧ `rules_context.py:49/51/56-57/63/114` 任一改坏 → 对应的数值/维度键测试红；⑨ `rules_series.py:89/93/100` 任一改坏 → `last`/`first`/枚举键测试红。 |
| `test_mapping_columns_gaps.py` | ① `columns.py:109` 的 `if column not in available` → `if False` → 两条来源列测试红（**已实测**）；② `columns.py:59` 的 `bucket.update(item.keys())` 改成只留第一个节点的列 → `test_merged_archive_view_columns_unions_the_columns_of_every_node` 红；③ `columns.py:53` 的 `if not raw: continue` 改成不 continue → 空节点会让 `_single_row` 抛错 → `test_merged_archive_view_columns_skips_a_node_without_a_result` 红；④ `columns.py:168` 的 `if not view or not column: return` 删掉 → `required_source_columns` 会登记一个 `(code, None)` → `test_required_source_columns_skips_a_side_without_a_column` 红；⑤ `validation.py:104-107` 的集合缺失分支删掉 → `test_a_diagnostic_rule_with_an_unknown_collection_is_reported` 红；⑥ `validation.py:124/126` 的 `_contains` 去后缀 / `group_by` 登记删掉 → 两条诊断规则测试红（问题清单少一条）；⑦ `validation.py:164` 的 `name not in available` 改成恒假 → 平台指标名/实例字段的两条测试红；⑧ `loader.py:181-182/188/193/213-214/235` 任一守卫删掉 → 对应的 `ProtocolError` 测试红（异常类型或文案会变）；⑨ `table.py:198/203/207/255/266/270-271` 任一改坏 → 对应的查表/加载测试红。 |
| `test_mapping_message_sqlgen_gaps.py` | ① `message.py:66` 的 `priority = definition.default_priority` → `None` → 优先级回落测试红（**已实测**）；② `message.py:126-127` 的 `_as_float` 的 `except` 分支删掉 → 转不成数会抛 `ValueError` 而不是给 `None` → `test_serialize_value_of_an_unparsable_float_is_none` 红；③ `message.py:157` 的 `return record['cause']` 换成 `record['description']` → 正文回落测试红；④ `message.py:69-71` 的 `value_source == 'latest_metric_value'` 判定改掉 → 告警取值来源测试红；⑤ `sqlgen.py:108-111/119/139` 任一守卫删掉 → 对应的 `MappingError` 测试红（**已实测**：139 删掉 → `AttributeError` 而不是 `MappingError`）；⑥ `sqlgen.py:144` 的 `' '.join(sql.split())` 改成不折叠空白 → 渲染出的 SQL 与归档不一致（现有 `tests/unit/test_sqlgen.py` 会红）。 |

**结论：4 个文件里的每条测试都能被至少一个具体的实现表达式打红；没有「改了实现也照样绿」的凑数测试。**
（另有 6 条变异已实际执行并贴出输出，见 §2。）

---

## 6. 复现命令与实际输出

```bash
cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev

# 全量测试
$ conda run -n tpops python -m pytest -q
1479 passed in 25.78s

# 覆盖率（映射层逐文件）
$ conda run -n tpops python -m pytest -q --cov=tpops_kafka_collector --cov-report=term-missing
Name                                    Stmts   Miss  Cover   Missing
----------------------------------------------------------------------------------
tpops_kafka_collector/mapping/__init__.py    7      0   100%
tpops_kafka_collector/mapping/columns.py    100      0   100%
tpops_kafka_collector/mapping/decode.py     170      0   100%
tpops_kafka_collector/mapping/loader.py     106      0   100%
tpops_kafka_collector/mapping/masking.py      57      0   100%
tpops_kafka_collector/mapping/message.py      62      0   100%
tpops_kafka_collector/mapping/nodes.py        44      0   100%
tpops_kafka_collector/mapping/rules.py         5      0   100%
tpops_kafka_collector/mapping/rules_collection.py   33    0   100%
tpops_kafka_collector/mapping/rules_context.py     85    0   100%
tpops_kafka_collector/mapping/rules_cross.py       39    0   100%
tpops_kafka_collector/mapping/rules_deltas.py      48    0   100%
tpops_kafka_collector/mapping/rules_engine.py      16    0   100%
tpops_kafka_collector/mapping/rules_naming.py      30    0   100%
tpops_kafka_collector/mapping/rules_row.py         54    0   100%
tpops_kafka_collector/mapping/rules_series.py      59    0   100%
tpops_kafka_collector/mapping/rules_sorting.py     57    1    98%   110
tpops_kafka_collector/mapping/sqlgen.py        89      0   100%
tpops_kafka_collector/mapping/table.py       136      0   100%
tpops_kafka_collector/mapping/validation.py   90      0   100%
TOTAL                                       4190    224    95%
1479 passed in 34.59s

# lint 门禁
$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

# py37 门禁
$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
132 passed in 1.08s

# 新增测试文件单独跑（也顺带过了 ruff）
$ conda run -n tpops python -m ruff check tests/unit/test_mapping_*_gaps.py
All checks passed!
```

**映射层合计的算法**（便于控制器复核）：
`Stmts` 合计 1287，`Miss` 合计 **1**（只有 `rules_sorting.py:110`）→
`1286 / 1287 = 99.92%`；**最低单文件** `rules_sorting.py` = `56 / 57 = 98.2%`（打印为 98%）。

---

## 7. 边界确认

- 未改 `tpops_kafka_collector/` 下任何文件（变异验证的改动全部还原，`git diff --stat` 为空）。
- 未改 `changes/` 下已有文件（只新建了本报告）。
- 未改 `specs/`、`data/mapping/*.json`、`data/merged_sql/*`、`execution-contract.md`、`tasks.md`。
- 未 `git commit`（工作区里我的 4 个测试文件仍是 untracked，等控制器统一提交）。
- 未触碰并行实施方的 `tpops_kafka_collector/scheduler*.py`、`tests/unit/test_scheduler.py`、
  `tests/integration/test_scheduler.py`。
- 无新增依赖；Python 3.7.9 语法（无海象、无内建泛型、无 `dict |`）。
