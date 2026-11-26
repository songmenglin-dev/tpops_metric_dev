# Wave 1 实现报告 · 映射口径（wave-1-mapping）

> 变更：`tpops-mapping-and-logging`；波次：`wave-1-mapping`（serial，任务 1.1~1.8）
> 工作目录：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（分支 `tpops_metric_dev`）
> 日期：2026-11-26

---

## 0. 总结

| 项 | 结果 |
|---|---|
| 生效指标规则 | **87** 条（`enabled=false` 0 条，`metric_code` 无重复） |
| 排除清单 | **14** 条（11 `duplicate` + 3 `unsupported`），包内与仓库根逐字节一致 |
| 告警统一指标名 | 未挂客户指标的 254 条 → `gaussdb.alarm` / `数据库告警`；3 条挂靠 `gaussdb.Tps` 的不变 |
| 表级维度 | `{#NAME}` = `schemaname.relname`（如 `public.t1`），按两列分组 |
| 全量 pytest | **1841 passed**（收尾时） |
| py37 兼容门禁 | **170 passed** |
| ruff | **基线本就不绿**（详见 §5），本波次未引入新类别的问题 |
| 提交 | 1 个提交（口径改动彼此耦合，拆开会让中间提交不绿 —— 详见 §5.6） |

**目标达成情况**：合同「Wave 1 完成标准」四条全部满足（详见 §4）。

---

## 1. 逐任务：做了什么 / RED / GREEN

### 1.1 建排除清单数据文件

**做了什么**

- 新增 **包内** `tpops_kafka_collector/mapping/data/excluded_metrics.json`（运行期真相源）
  与 **仓库根** `data/mapping/excluded_metrics.json`（同源副本），**逐字节一致**。
- 结构：`counts`（`total=14` / `duplicate=11` / `unsupported=3` / `client_total=101` /
  `effective_total=87`）+ `excluded[]`（14 条）。
  每条含 `metric_code` / `metric_name` / `reason` / `detail`；`reason=duplicate` 的
  11 条另带 `kept`（保留哪一条），`reason=unsupported` 的 3 条**不带** `kept`。
- 新增 `tests/unit/test_excluded_metrics.py`：14 条 / 11-3 分布 / 必填字段 /
  duplicate 必带 kept / unsupported 不带 kept / 码唯一 / 两份副本一致 /
  `kept` 指向的指标必须在生效映射表里 / `kept` 自己不在排除清单里。

**RED 证据**（先把数据文件挪走再写测试）：

```
ERROR tests/unit/test_excluded_metrics.py::test_exclusion_list_has_exactly_fourteen_entries
  - FileNotFoundError ... excluded_metrics.json
FAILED tests/unit/test_excluded_metrics.py::test_package_and_repo_copies_are_byte_identical
FAILED tests/unit/test_excluded_metrics.py::test_effective_mapping_has_exactly_eighty_seven_rules
FAILED tests/unit/test_excluded_metrics.py::test_effective_mapping_has_no_disabled_rule
3 failed, 1 passed, 11 errors
```

**GREEN 证据**：数据文件落位后 `12 passed`（其中 87 条 / 0 disabled 两条断言属任务 1.2，
当时仍红，见 1.2）。

### 1.2 生成器按清单剔除

**做了什么**

- 新增 `tools/excluded_metrics.py`：**构建期共用**的排除清单读取器
  （`load_excluded` / `excluded_codes` / `is_excluded`）—— 三个生成器读**同一份**清单。
- `tools/build_customer_metric_mapping.py`：`build()` 里按清单剔除 →
  `data/customer_metric_mapping.json` 从 101 条降到 **87 条**。
- `tools/build_metric_value_rules.py`：`_build_metrics()` 剔除 →
  `data/metric_value_rules.json` 98 → **87**；末尾「期望 98 条」的写死断言改成
  **与可提供性判定交叉校验**（`_expected_effective_count()`，条数不写死在代码里）。
- `tools/build_itscada_mapping.py`：`build_metric_rules()` 跳过清单里的指标码 →
  `metric_rules.json` **87 条、0 条 disabled**。
- 在 `test_excluded_metrics.py` 补断言：87 条 / 0 条 `enabled=false` / `metric_code` 唯一
  / 排除清单的码一个都不在生效映射表里 / `kept` 必在生效映射表里。

**RED 证据**（1.1 的 GREEN 跑完仍红）：

```
FAILED tests/unit/test_excluded_metrics.py::test_no_excluded_code_is_in_the_effective_mapping
FAILED tests/unit/test_excluded_metrics.py::test_effective_mapping_has_exactly_eighty_seven_rules
FAILED tests/unit/test_excluded_metrics.py::test_effective_mapping_has_no_disabled_rule
  AssertionError: 生效口径里不该有 enabled=false 的规则：
  ['gaussdb.Fastpath_function_call', 'gaussdb.Disabled_conn', 'gaussdb.sendAgentName']
```

**GREEN 证据**：`15 passed`（重生成后）→ 收尾 `16 passed`。

### 1.3 映射层加载与校验排除清单

**做了什么**

- `mapping/loader.py`：新增 `excluded_metrics` 文件入口、新增 `ExcludedMetric` 冻结
  dataclass、`LoadedMapping` 增 `excluded_metrics` / `excluded_counts` 两个字段；
  `validate_mapping()` 增三项校验。
- `mapping/validation.py` 新增三项校验函数：
  - `check_excluded_metrics_absent()` —— 排除清单的码**一个都不在**生效映射表里；
  - `check_metric_code_uniqueness()` —— 生效规则的 `metric_code` 互不重复
    （**唯一性只对指标规则要求**，告警定义靠 `alarm_id` 定位，不参与唯一性校验）；
  - `check_effective_rule_count()` —— 生效条数与排除清单 `counts.effective_total` 一致
    （**条数来自数据文件**，不写死在代码里）。
- `mapping/validation.py` 顺带修：维度绑定的列校验支持**点号串**（1.5 的前置）。
- 新增 `tests/unit/test_metric_catalog.py`：正例 + 三项反例
  （塞一条被排除的码 → 校验失败并打印差异；复制一条规则 → 唯一性失败；
  少一条规则 → 条数失败且错误信息里同时出现 87 与 86）；
  另有一条「告警定义共用同一个 metricCode 不得触发唯一性失败」。

**RED 证据**（把 `loader.py` / `validation.py` 临时回退到 HEAD 再跑）：

```
FAILED tests/unit/test_metric_catalog.py::test_the_real_catalog_passes_validation
FAILED tests/unit/test_metric_catalog.py::test_excluded_metrics_are_loaded_from_the_data_file
FAILED tests/unit/test_metric_catalog.py::test_an_excluded_code_in_the_effective_mapping_fails_validation
FAILED tests/unit/test_metric_catalog.py::test_a_duplicated_metric_code_fails_validation
FAILED tests/unit/test_metric_catalog.py::test_a_wrong_rule_count_fails_validation
5 failed, 1 passed
```

**GREEN 证据**：`6 passed`（收尾 `9 passed`，1.5 的两条断言加进来后）。

### 1.4 点号组合的维度绑定

**做了什么**

- `mapping/rules_context.py`：
  - 新增 `binding_columns(spec)` —— 绑定值可以是单列（`relname`），
    也可以是**点号连接的多个列名**（`schemaname.relname`）；
  - `row_dimensions()` 支持组合绑定（各列值用 `.` 连起来）；
  - `grouped_items()` 的「分组键必须覆盖绑定列」检查改为按**拆开的列**比对，
    并支持**多列分组**；维度值从分组键里取（`_value_from_key()`）。
  - **取不到列值 → 抛 `RuleError`**（不静默留 `{#NAME}`）—— 与合同的
    Test Obligations 一致。
- 新增 `tests/unit/test_dimension_binding.py`：组合绑定（`public.t1` / `other.t1`）、
  组合绑定取不到列值抛错、多列分组、单列绑定不回归（含「单列取不到仍是 `None`」的老行为）。

**RED 证据**：

```
E  tpops_kafka_collector.mapping.rules_context.RuleError: group_by ['schemaname', 'relname']
   与维度绑定 {'NAME': 'schemaname.relname'} 不一致：分组键缺少列 ['schemaname.relname']
FAILED tests/unit/test_dimension_binding.py::test_dotted_binding_combines_the_column_values
FAILED tests/unit/test_dimension_binding.py::test_dotted_binding_keeps_the_schema_prefix
FAILED tests/unit/test_dimension_binding.py::test_dotted_binding_raises_when_a_column_is_missing
FAILED tests/unit/test_dimension_binding.py::test_group_by_two_columns_splits_same_named_tables
FAILED tests/unit/test_dimension_binding.py::test_group_by_two_columns_keeps_the_row_value
5 failed, 4 passed
```

**GREEN 证据**：`9 passed`（收尾 `14 passed`）。

### 1.5 生成器与规则改绑 schema+relname

**做了什么**

- `tools/build_itscada_mapping.py`：
  - `DIMENSION_COLUMN_BY_SQL['stat_user_tables']` 的 `'NAME'` 由 `'relname'`
    改成 **`'schemaname.relname'`**；
  - 新增 `_group_by_columns(bindings)` —— **点号串拆成多列**再按绑定顺序去重
    （`_row_policy()` 与 `build_value_rule()` 的 aggregate 分支都改用它）。
    这一点是**必须**的：不改派生逻辑，`group_by` 会退化成
    「一个叫 `schemaname.relname` 的单列」，分组失效。
- 重生成后 11 条表级规则的 `dimension_bindings` = `{"NAME": "schemaname.relname"}`、
  `group_by` = `["schemaname", "relname"]`。
- 在 `test_metric_catalog.py` 补断言：表级规则恰好 11 条、一律绑 `schemaname.relname`、
  一律按两列分组，且**任何规则的 `group_by` 里都不许出现带点的列**。

**RED / GREEN**：该任务的断言随 1.4 的实现一起由红转绿（见 1.4 的 RED 输出）；
生成器侧的行为由 `test_metric_catalog.py` 与
`tests/test_generated_rules.py::test_metric_rules_file_is_up_to_date`（重生成结果 == 落盘文件）守着。

### 1.6 表级幂等不碰撞

**做了什么**

在 `tests/unit/test_dimension_binding.py` 补 5 条用例（走**真实链路**：
`evaluate()` → `build_metric_message()` → `metric_idempotency_key()`）：

- `public.t1` 与 `other.t1` 的 `metricName` 不同、`metricCode` 不同；
- `tags.dimension_values.NAME` = `public.t1` / `other.t1`；
- 两条的**幂等键不同**（两条都投递）；
- 同一张表重复采集 → **幂等键相同**（仍然去重）；
- `resourceName` 不含表名（资源名只表示节点）。

**RED 证据**（把 `rules_context.py` 临时回退到 HEAD 再跑）：

```
FAILED tests/unit/test_dimension_binding.py::test_same_named_tables_in_different_schemas_get_different_messages
FAILED tests/unit/test_dimension_binding.py::test_dimension_values_carry_the_schema_prefix
FAILED tests/unit/test_dimension_binding.py::test_same_named_tables_in_different_schemas_get_different_idempotency_keys
FAILED tests/unit/test_dimension_binding.py::test_the_same_table_collected_twice_gets_the_same_idempotency_key
FAILED tests/unit/test_dimension_binding.py::test_resource_name_does_not_contain_the_table_name
10 failed, 4 passed
```

**GREEN 证据**：`14 passed`。

### 1.7 告警统一指标名

**做了什么**

- `alarm_rules.json` 新增 **`event_metric`** 段
  （`{"metric_code": "gaussdb.alarm", "metric_name": "数据库告警"}`）—— 包内 + 仓库根。
- `tools/build_itscada_mapping.py`：未挂客户指标的事件类告警**统一取该值**；
  统一值**表驱动** —— 生成器优先读 `alarm_rules.json` 里已有的 `event_metric`
  （运维改了重跑不会被覆盖回去），没有才用 `DEFAULT_EVENT_METRIC`。
  挂靠了客户指标的 3 条**保持不变**（`gaussdb.Tps` / `TPS（每秒事务数）`）。
- `mapping/table.py` 新增 `build_event_alarm_definition()` —— 运行期兜底也用统一值。
- `collectors/alarm.py` 的 `_definition_for()`：平台新增告警（定义缺失）时，
  兜底从「自造 `gaussdb.alarm.<alarm_id>`」改成**映射数据里的统一值**。
- `open_items.json`：登记该统一值（新增 `alarm-unified-metric` 一项），
  并把 `alarm-metric-binding` 的「自造指标名」旧口径改成新口径。
- 新增 `tests/unit/test_alarm_metric_unification.py`：254 条同码同名、3 条挂靠不变、
  告警名逐条对得上平台目录、告警码可重复而不触发唯一性校验、运行期兜底用统一值。

**RED 证据**：

```
ImportError: cannot import name 'build_event_alarm_definition' from
'tpops_kafka_collector.mapping.table'
```

（随后一轮：未统一时 `test_event_alarm_names_are_still_all_different` 与
`test_the_runtime_fallback_uses_the_mapped_unified_value` 失败。）

**GREEN 证据**：`13 passed`。

### 1.8 交付文档重生成

**做了什么**

- `tools/build_delivery_docs.py`：
  - 读 `excluded_metrics.json`（包内）；
  - 「不可提供清单」改从**排除清单**的 `reason=unsupported` 取（原来从映射表的
    `enabled=false` 规则取 —— 那 3 条已经不在生效映射表里了）；
    分类 / 数据类型 / 采集周期从**客户 xlsx（只读）**取，不在数据文件里重复一份；
  - 文案改为「**构建期排除**、运行期无丢弃分支、`dropped_unsupported` 恒为 0、
    **已从生效口径移除**」；
  - 口径说明 / OCR 清单里的写死数字（101 / 98 / 82）改成**按数据算**。
- `tools/build_kafka_mapping.py`：占位符绑定支持点号串（新增 ` · ` 连接表达式）。
- `tools/expected_deriver.py`：独立推导器的 `_dimensions()` 同步支持点号串
  （否则期望值表推出来的维度是 `None`）。
- 重生成三份交付文档 + `docs/映射表-ITSCADA字段对照.md`
  （`data/mapping/archive/alarm_mapping.json` 里的两处 transform 文案同步改掉），
  更新 `tpops_kafka_collector/mapping/README.md`（新增「排除清单」「表级维度」
  「告警统一指标名」三节）、`data/mapping/README.md`、`data/metric_response/README.md`。
- `tests/unit/test_delivery_docs.py`：更新判据（不可提供清单改对排除清单），
  并**新增**「三份文档的条数与生效映射表一致（87 / 3 / —）」的防漂移用例。

**RED / GREEN**：既有 5 条用例在文档口径变化后失败（98→87、清单来源变化），
按新口径修测试后 `40 passed`；补上新增用例后 `41 passed`。

---

## 2. 改动文件清单

### 新增

| 文件 | 说明 |
|---|---|
| `tpops_kafka_collector/mapping/data/excluded_metrics.json` | 排除清单（运行期真相源，14 条） |
| `data/mapping/excluded_metrics.json` | 同源副本（逐字节一致） |
| `tools/excluded_metrics.py` | 构建期共用的排除清单读取器 |
| `tests/unit/test_excluded_metrics.py` | 任务 1.1 / 1.2 |
| `tests/unit/test_metric_catalog.py` | 任务 1.3 / 1.5 |
| `tests/unit/test_dimension_binding.py` | 任务 1.4 / 1.6 |
| `tests/unit/test_alarm_metric_unification.py` | 任务 1.7 |

### 生产代码

| 文件 | 改动 |
|---|---|
| `tpops_kafka_collector/mapping/rules_context.py` | 点号组合绑定 + 多列分组 + 取不到列值抛 `RuleError` |
| `tpops_kafka_collector/mapping/loader.py` | 加载排除清单；三项新校验接入 `validate_mapping()` |
| `tpops_kafka_collector/mapping/validation.py` | 三项新校验；维度绑定列校验支持点号串 |
| `tpops_kafka_collector/mapping/table.py` | 新增 `build_event_alarm_definition()` |
| `tpops_kafka_collector/collectors/alarm.py` | 定义缺失时的兜底改用映射数据里的统一指标名 |

### 生成器 / 离线工具

`tools/build_itscada_mapping.py`、`tools/build_customer_metric_mapping.py`、
`tools/build_metric_value_rules.py`、`tools/build_delivery_docs.py`、
`tools/build_kafka_mapping.py`、`tools/expected_deriver.py`

### 数据（重生成）

`data/mapping/{metric_rules,alarm_rules,response_rules,kafka_field_mapping,open_items,expected_values}.json`、
`data/mapping/archive/alarm_mapping.json`、`data/customer_metric_mapping.json`、
`data/customer_metric_query_plan.json`、`data/metric_value_rules.json`、
`data/tpops_api_catalog.json`、`data/metric_response/README.md`、`data/mapping/README.md`，
以及包内对应的 6 份副本（`tpops_kafka_collector/mapping/data/*.json`）

### 文档

`docs/交付-指标口径说明.md`、`docs/交付-不可提供指标清单.md`、
`docs/交付-客户指标英文名核对清单.md`、`docs/映射表-ITSCADA字段对照.md`、
`tpops_kafka_collector/mapping/README.md`

### 既有测试（按新口径修，**未删任何测试**）

`tests/test_mapping_table.py`、`tests/test_expected_values.py`、
`tests/test_generated_rules.py`、`tests/test_kafka_mapping.py`、
`tests/test_merged_sql_mapping.py`、`tests/test_metric_response.py`、
`tests/test_response_rules.py`、`tests/test_rules.py`、
`tests/integration/test_collector_command_center.py`、
`tests/regression/{test_b_class_regression,test_collector_coverage,test_fixture_integrity}.py`、
`tests/unit/{test_delivery_docs,test_mapping_columns_gaps,test_mapping_data_sync,test_mapping_loader,test_scheduler}.py`

---

## 3. 既有测试的期望值变更（逐条理由）

> 规则：**不删测试、不放宽断言**。下列改动都是「口径变了 → 期望值跟着变」。

| 文件 | 变更 | 理由 |
|---|---|---|
| `tests/test_mapping_table.py` | 101→87、98→87、82→71 | 生效口径 / B 类条数变了 |
| 同上 | `test_unsupported_metrics_are_disabled_with_reason` → `test_unsupported_metrics_are_excluded_not_disabled` | 3 条无法提供的指标**不再以 `enabled=false` 留在映射表里**，改为断言「不在生效映射表里 + 没有 disabled 规则」 |
| `tests/test_generated_rules.py` | 98→87 | 取值规则条数变了 |
| `tests/test_kafka_mapping.py` | 98→87 | 字段映射表条数变了 |
| `tests/test_merged_sql_mapping.py` | 98→87；`checked >= 80` → `== 71` | 同上；回放条数随之变化，改成**精确值**（比原来的下限更严） |
| `tests/test_response_rules.py` | 98→87 | 取数定位表条数变了 |
| `tests/test_expected_values.py` | `EXPECTED_B_COUNT` 82→71 | B 类条数变了（11 条重复表级指标被排除） |
| `tests/regression/test_b_class_regression.py` | 82→71 | 同上 |
| `tests/regression/test_collector_coverage.py` | 98→87、97→86 | 产出条数变了（86 = 87 − 1 条「留档里没有 AccessExclusiveLock 锁」的历史缺口） |
| `tests/regression/test_fixture_integrity.py` | 82→71 | 期望值表条数变了（表是**生成物**，由 `tools/build_expected_values.py` 重生成后同步） |
| `tests/integration/test_collector_command_center.py` | 82→71；`EXPECTED_COMPARISONS` 1700→954；分页夹具补 `'schemaname'` | 表级维度改成两列后，合成页必须带 `schemaname`（否则组合绑定取不到值 → `RuleError`）；比对条数随之变化 |
| `tests/test_rules.py` | 告警 `metricCode` 断言 `gaussdb.alarm.2007010001` → `gaussdb.alarm`；`metricName` → `数据库告警`；比率指标列表去掉 `GAUSSDB.{#NAME}.garbage.Ratio` | 告警统一指标名（新口径）；被排除的指标码不再有规则 |
| `tests/test_metric_response.py` | manifest 断言改为「**覆盖**每个生效指标」；逐指标核对时跳过不在生效口径里的码 | 样本目录是**抓取归档**（98 份，平台事实），生效口径是 87 条（决定）—— 见 §5.2 |
| `tests/unit/test_delivery_docs.py` | 98→87；不可提供清单改对排除清单；新增「文档条数 == 生效映射表」 | 交付文档随口径重生成 |
| `tests/unit/test_scheduler.py` | `metric_count` 82→71；`count_unsupported_metrics() == 3` → `== 0` | 不可提供指标已在**构建期**排除 → 运行期 `dropped_unsupported` 恒为 0（合同要求） |
| `tests/unit/test_mapping_loader.py`、`test_mapping_columns_gaps.py` | 101→87 | 加载到的条数变了 |
| `tests/unit/test_mapping_data_sync.py`、`test_mapping_loader.py` | `COPIED_FILES` 增加 `excluded_metrics.json` | 新增了一份同源副本，防漂移闸要盯它 |

---

## 4. 合同完成标准逐条核对

| 合同要求 | 结果 |
|---|---|
| `pytest tests/unit/test_excluded_metrics.py tests/unit/test_dimension_binding.py tests/unit/test_alarm_metric_unification.py tests/unit/test_metric_catalog.py tests/unit/test_delivery_docs.py -q` 全绿 | **93 passed** |
| 生效规则数 = 87 | ✅ `metric_rules.json` 87 条 |
| `enabled=false` 数 = 0 | ✅ 0 条（3 条无法提供的已**彻底移出**生效口径） |
| `metric_code` 无重复 | ✅ 启动期校验 + 单测双守 |
| 排除清单 14 条（11/3），包内与仓库根两份一致 | ✅ 逐字节一致（同步闸 + 单测） |
| `public.t1` 与 `other.t1` 的 `metricCode` 与幂等键不同 | ✅ `test_dimension_binding.py` 5 条用例 |
| 全量 pytest 全绿 | ✅ **1841 passed** |
| py37 兼容门禁 | ✅ 170 passed |
| ruff | ⚠️ **基线本就不绿**，见 §5.5 |

---

## 5. 遇到的问题与判断（请 reviewer 重点看）

### 5.1 「生效规则数 = 87」的期望值放在**数据文件**里，不写死在代码里

合同 R6（映射表驱动）要求「SHALL NOT 硬编码客户口径」。所以
`check_effective_rule_count()` 的期望值取自
`excluded_metrics.json` 的 `counts.effective_total`（= 87），
并在同一处记 `client_total=101` —— 三者自洽（`101 − 14 = 87`）。
**判断**：条数是客户口径，写死在 `validation.py` 里会与 R6 冲突；
但完全不写死又会失去「条数对不上就启动失败」这道闸。
放在排除清单的 `counts` 段是折中：**一处声明、两处使用**
（校验 + 单测）。若 reviewer 认为应当另设一个「口径基线」文件，可以再调。

### 5.2 样本归档（98 份）**没有**动，生效口径（87 条）另算

`data/metric_response/` 与 `tests/fixtures/metric_response/` 是**抓取归档**
（平台事实，98 份样本 + manifest）。本波次**没有删任何样本文件**，
manifest 也仍是 98 条。理由：

- 归档是事实，生效口径是决定 —— 被排除的 11 条重复表级指标的**平台响应仍然有效**，
  删掉是**不可逆的数据损失**（git 历史可恢复，但没必要）；
- 合同没有要求删样本；R1 只要求「运行期生效映射表里不存在被排除的指标码」。

代价：manifest 与生效口径不再是 1:1，于是两条既有测试按新口径改了
（§3 最后两行）：manifest 断言从「等于生效口径」改成「**覆盖**每个生效指标」，
逐指标核对时跳过不在生效口径里的码。
**判断**：这是「归档 ⊇ 生效口径」的语义，方向仍然是「每个生效指标都有样本」
（缺一个就红），没有放宽。**若 reviewer 认为归档应当与生效口径同步收敛，
需要删 11 份样本 + 改 manifest + 改 `EXPECTED_ARCHIVE_COUNT`，请指示。**

### 5.3 `alarm_name` 并非 254 条全不同（**偏离任务原文**）

任务 1.7 原文要求断言「254 条事件类告警的 `metric_code` 相同、**`alarm_name` 各不相同**」。
实测**做不到**：平台告警目录（`data/alarm/catalog_*.json`）里有

- **13 行**「告警名称」列填的是等级词（`紧急` 8 次 / `重要` 5 次）；
- **2 组**同名不同 ID 的告警（`系统磁盘使用率过高` ×2、`GaussDB流容灾主集群数据同步失败` ×2）。

即 254 条里只有 **241 个不同的告警名**。这是**平台侧既有数据**，不是本波次引入的。
**处理**：断言改为「① `alarm_id` 254 个互不相同（告警种类靠 `alarm_id` 承载 ——
这正是合同 R3 的原话）；② `alarm_name` 非空且**逐条对得上平台目录**
（`test_alarm_names_match_the_platform_catalog`）—— 证明统一指标名**没有动过告警名**」。
**没有**为了让断言通过而改平台目录数据。

### 5.4 运行期兜底也改了（超出任务字面，但同一口径）

`collectors/alarm.py` 的 `_definition_for()` 原来在告警定义缺失时自造
`gaussdb.alarm.<alarm_id>`。本波次改成用 `alarm_rules.json` 的 `event_metric`。
理由：R3 要求「不挂靠客户指标的事件类告警 `metricCode` **全部**等于同一个固定值」，
而兜底造出来的定义 `linked_customer_metric` 也是空 —— 留着旧写法就会有两个口径；
且 R6 要求「SHALL NOT 硬编码客户口径」。**判断**：这是必要的收口，请 reviewer 确认。

### 5.5 ruff 门禁**在本仓库基线上就不绿**（非本波次引入）

- 环境里的 ruff 是 **0.17.6**，`ruff.toml` 的 `select` 含 `UP` —— 于是它把**全仓**
  `%` 格式化（`UP031`）、`from __future__ import`（`UP010`）、UTF-8 声明（`UP009`）
  全判成问题。
- 实测基线（`git stash` 后同样命令）：`tpops_kafka_collector tests tools` → **473 条**
  （ruff 0.1.15）/ **764 条**（0.17.6）。**基线就红。**
- 本波次改动后：494 / 808 条，**新增的 21 条全部是仓库里本来就有的那几类**
  （`UP031` / `UP009` / `UP010` / `UP020` / `E501`），与既有代码风格一致。
- 我**没有**为了「让 ruff 变绿」去改全仓风格（那是 wave-4 的收口范围，且会污染本波次 diff）。
- `requirements-dev.txt` **没有钉 ruff 版本** —— 建议 wave-4 顺手钉一个版本，
  否则这道门禁的结果取决于开发机上装的是哪个 ruff。
- 我自己新增的代码里**唯一一条客观问题**（`E731` lambda 赋值）已改成 `def`；
  唯一一条超长行（101 列）已折行。

### 5.6 为什么只有 1 个提交

任务 1.1~1.8 的产物**彼此强耦合**：`metric_rules.json` 同时含 1.2（剔除 14 条）
与 1.5（改绑 `schemaname.relname`）的效果；`test_excluded_metrics.py` 同时含
1.1 与 1.2 的断言；`test_delivery_docs.py` 的期望值依赖 1.2 之后的 87 条口径。
拆成 8 个提交会让**中间提交跑不过测试**（例如「校验生效条数 = 87」在
「映射表还是 101 条」的那个提交上必然红）。因此合并为 1 个提交，
逐任务的 RED/GREEN 证据见 §1。

### 5.7 其他未在本波次处理、但 reviewer 可能关心的

- **`CLAUDE.md` 里仍写着「98 可提供 / 3 丢弃 / 101 条」** —— wave-2 的输出清单里
  含「`CLAUDE.md` 约定更新」，为避免与 wave-2 抢同一文件，本波次**没动**它。
  建议 wave-2 顺手改成 87 / 14 / 101。
- **`docs/问题单.md` 的 TPOPS-001（跨 schema 同名表）与 TPOPS-003（11 组重复）**
  现在都已由本波次解决，但台账里的勾选框**没动**（等 review 结论）。
- **`docs/开发方案-GaussDB指标与告警采集.md`** 是**设计期**文档，里面的
  101 / 98 / 82 是当时的口径，按「规划工件不改」处理，未动。
- **`data/mapping/README.md` 里还有更早的过期描述**（指向已删除的 `tools/mappings/`、
  「288 条告警定义」等），只修了与本波次口径直接相关的几处。

---

## 6. 复现命令

```bash
cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
conda run -n tpops python -m pytest -q                       # 1841 passed
conda run -n tpops python -m pytest \
    tests/unit/test_excluded_metrics.py \
    tests/unit/test_dimension_binding.py \
    tests/unit/test_alarm_metric_unification.py \
    tests/unit/test_metric_catalog.py \
    tests/unit/test_delivery_docs.py -q                      # 93 passed
conda run -n tpops python -m pytest tests/test_py37_compat.py -q   # 170 passed
conda run -n tpops python -m ruff check tpops_kafka_collector tests tools   # 基线已红，见 §5.5
```

生成器幂等（重跑输出不变，已验证）：

```bash
for t in build_customer_metric_mapping build_metric_query_plan build_metric_value_rules \
         build_itscada_mapping build_response_rules build_kafka_mapping \
         build_expected_values build_delivery_docs; do
    conda run -n tpops python tools/$t.py
done
# 再把包内副本同步过去：git status 不应有新变化
```
