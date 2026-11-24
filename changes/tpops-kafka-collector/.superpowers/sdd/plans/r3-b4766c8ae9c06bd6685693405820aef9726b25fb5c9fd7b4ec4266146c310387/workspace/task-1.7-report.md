# 任务 1.7 报告 —— 映射表加载与启动校验

**实施方**：wave-1 任务 1.7
**工作目录**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（worktree，分支 `tpops_metric_dev`）
**日期**：2026-11-20

---

## 1. 做了什么

新增**包内**映射数据目录 `tpops_kafka_collector/mapping/data/`、加载器
`tpops_kafka_collector/mapping/loader.py`、留档列声明的生成器
`tools/build_observed_columns.py`，以及单元测试 `tests/unit/test_mapping_loader.py`。

加载器提供：

- `load_mapping(data_dir=None, validate=True) -> LoadedMapping`
  —— 从**包内** `mapping/data/*.json` 加载全部客户口径，默认做启动期四类校验；
- `validate_mapping(bundle)` —— 跑完四类校验，不通过抛 `MappingError`；
- 四个纯函数（可单测）：
  `check_source_columns` / `check_providable_coverage` / `check_rule_entries` /
  `check_placeholder_bindings`。

**四类校验的具体口径**：

| # | 校验 | 事实基准 |
|---|---|---|
| 1 | 来源列存在于实测留档的**实际返回列** | `observed_columns.json`（B 类 11 个合并视图 / C 类 2 个集合的列 / D 类实例字段 / A 类平台指标名） |
| 2 | 可提供指标齐全 | `customer_metric_mapping.json` 里 98 条「可提供」 ↔ 规则表里 98 条 `enabled=true`，双向比对 |
| 3 | 条目对应真实指标 | 101 条规则的 `metric_code` 必须都在客户指标库的 101 条里 |
| 4 | 占位符与维度绑定一一对应 | `placeholders` == `dimension_bindings` 的键 == 指标名里 `{#XXX}` 的集合 |

**失败一律 fail loud**：抛 `MappingError`（不可重试，映射与事实对不上必须人工改
数据）；映射文件本身结构不对（缺 `rules` 段、顶层不是对象）抛 `ProtocolError`；
文件缺失 / 不是合法 JSON 抛 `MappingError`。没有任何「打印告警了事」的分支。

**复用而非重写**：B 类的来源列校验直接调用 `mapping/columns.py` 的
`source_column_problems`（生成器与采集端同一份实现）。`MetricRule` 复用
`mapping/table.py` 的 frozen dataclass。

---

## 2. 新增 / 修改的文件

| 文件 | 性质 | 说明 |
|---|---|---|
| `tpops_kafka_collector/mapping/loader.py` | 新增（399 行） | 加载器 + 四类校验 |
| `tpops_kafka_collector/mapping/data/__init__.py` | 新增 | 该目录的说明（让 `data` 成为可打包的子包） |
| `tpops_kafka_collector/mapping/data/*.json`（7 张表 + 1 个生成物） | 新增 | 见下表 |
| `tools/build_observed_columns.py` | 新增 | 生成 `observed_columns.json`（照 `tools/build_response_rules.py` 的模式，带 `--check`） |
| `tests/unit/test_mapping_loader.py` | 新增 | 15 个用例 |

**未改动任何 wave 0 的既有文件**（`columns.py` / `table.py` / `rules.py` /
`decode.py` / `message.py` / `nodes.py` / `mapping/__init__.py` 一律未动）。

---

## 3. 复制了哪几张表、为什么

包内 `tpops_kafka_collector/mapping/data/` 共 **7 张仓库副本 + 1 个生成物**：

| 包内文件 | 来源 | 体积 | 为什么装进包 |
|---|---|---|---|
| `metric_rules.json` | `data/mapping/` | 118 KB | **四类校验的主表**（101 条指标规则：取哪个列、怎么算） |
| `alarm_rules.json` | `data/mapping/` | 108 KB | 规格要求加载「告警定义目录」——257 条平台告警定义 + 优先级/状态映射 |
| `itscada_field_map.json` | `data/mapping/` | 4 KB | 客户消息的字段规范（字段清单、必填与否、resource_name 模板） |
| `kafka_field_mapping.json` | `data/mapping/` | 456 KB | **字段级映射主表**（响应里的某个值 → Kafka 的某个字段），取代已归档的分类映射表 |
| `response_rules.json` | `data/mapping/` | 258 KB | 取数定位与响应形态（「SQL 视图清单」= 11 个合并视图 + 18 种形状） |
| `customer_metric_mapping.json` | `data/`（**不是** `data/mapping/`） | 43 KB | 客户指标库的可提供性判定（101 条：98 可提供 / 3 无法提供）——**校验 2 / 3 的独立基准** |
| `open_items.json` | `data/mapping/` | 3 KB | 待客户确认的 8 项（口径缺口随包走，别丢） |
| `observed_columns.json` | **生成物**（`tools/build_observed_columns.py`） | 3.6 KB | 实测留档的**实际返回列** —— 校验 1 的事实基准 |

**没有复制的**：

- `expected_values.json`（883 KB）—— 是回归测试的**期望值表**，测试数据不是运行时
  数据，按简报默认不装包；
- `data/mapping/archive/*`（5 张分类映射表 + README）—— 见下节「偏离」。

---

## 4. 第 2 点那个设计决策：实测留档的实际返回列

### 结论：走简报给的**优先方案** —— 抽成包内声明 `observed_columns.json`（3.6 KB）

**为什么不把 194 KB 留档塞进生产包**：留档是**测试数据**（`sql_merge_probe_result.json`
是某次探针的原始响应、`data/metric_response/` 是 98 份逐指标响应体），里面还有
token 占位、真实实例 ID、SQL 文本。生产包只需要的只是「某个列在不在」这一位信息，
把响应体整份发出去是**过度发布**。

**声明化会不会丢信息**：不会。校验只需要「列名 ∈ 集合」这个判定，
`merged_archive_view_columns()` 抽出来的就是**列名集合本身**，与行数据无关。
四个观测面全部覆盖：

- `merged_views`：11 个合并视图的行里出现过的列（18 / 13 / 15 / 8 / 5 / 4 / 4 / 3 / 3 / 3 / 1 列）；
- `collections`：`slow_sql_infos`（22 列）、`top_sql_infos`（19 列）—— 诊断优化通道；
- `instance_fields`：8 个实例元数据字段（含 `engine_version`）；
- `series_metrics`：7 个平台指标名（含 `rds133_cn_startup_time`）—— A 类。

**声明是生成物，不许手抄**：`tools/build_observed_columns.py` 从留档现算，
B 类部分直接调用生产包的 `mapping/columns.py`（声明与事实不会各写各的）。
生成器带 `--check` 模式。

**防漂移两道闸**（都在 `tests/unit/test_mapping_loader.py`）：

1. `test_observed_columns_match_the_generator_output` —— 包内声明 == 现算结果（逐字节）；
2. `test_observed_columns_generator_is_in_sync` —— 生成器 `--check` 返回 0。

**「运行时不需要仓库根目录」的证明**：

- `test_load_mapping_never_touches_the_repo_root` —— 劫持 `builtins.open`，
  断言加载过程中打开的**每一个文件**都在 `tpops_kafka_collector/mapping/data/` 里；
- 另做了一次部署模拟：把 `tpops_kafka_collector/` 整目录拷到 `/tmp/deploysim/`，
  在 `/` 下 `PYTHONPATH=/tmp/deploysim` 跑 `load_mapping()` → 正常返回
  （101 条规则 / 98 条启用 / 257 条告警定义 / 11 个视图）。

---

## 5. TDD：RED → GREEN

### RED（阶段 A：实现还不存在）

把 `loader.py` 移走跑测试：

```
ImportError while importing test module 'tests/unit/test_mapping_loader.py'.
tests/unit/test_mapping_loader.py:29: in <module>
    from tpops_kafka_collector.mapping import loader
E   ImportError: cannot import name 'loader' from 'tpops_kafka_collector.mapping'
1 error in 0.50s
```

### RED（阶段 B：把四个校验函数临时改成空转，看真实的断言失败）

```
FAILED tests/unit/test_mapping_loader.py::test_missing_providable_metric_fails_loudly
FAILED tests/unit/test_mapping_loader.py::test_entry_without_a_real_metric_fails_loudly
FAILED tests/unit/test_mapping_loader.py::test_placeholder_binding_mismatch_fails_loudly
FAILED tests/unit/test_mapping_loader.py::test_unknown_source_column_fails_loudly
FAILED tests/unit/test_mapping_loader.py::test_unknown_dimension_binding_column_fails_loudly
FAILED tests/unit/test_mapping_loader.py::test_unknown_diagnostic_column_fails_loudly
6 failed, 9 passed in 4.05s
```

失败断言（全部是同一条形态）：

```
E           Failed: DID NOT RAISE <class 'tpops_kafka_collector.errors.MappingError'>
```

即「校验没生效」正是这 6 条用例要卡的。

### GREEN

```
$ conda run -n tpops python -m pytest tests/unit/test_mapping_loader.py -q
15 passed in 1.39s
```

### 全量回归

```
$ conda run -n tpops python -m pytest -q
427 passed in 9.64s
```

（任务开始时基线 344 passed；本任务新增 15 条，其余增量来自并发跑的其他 wave-1 任务。）

### Lint

```
$ ruff check tpops_kafka_collector/
All checks passed!
```

（`ruff check tpops_kafka_collector/` 为 0；`tools/` 与 `tests/` 不在本仓库的 lint
口径内 —— 既有 `tools/build_response_rules.py` 同样有 31 条 UP 类提示，风格一致。）

---

## 6. 偏离简报之处及原因

| # | 偏离 | 原因 |
|---|---|---|
| D1 | **简报点的「六张表」在仓库里已经不存在**（`metric_mapping_monitor_metrics.json` / `metric_mapping_command_center_sql.json` / `metric_mapping_diagnostic.json` / `metric_mapping_instance.json` / `alarm_mapping.json` 现在都在 `data/mapping/archive/` 下，2026-11-13 归档；`itscada_field_map.json` 在 `data/mapping/` 根）。**没有把归档表装进生产包**，改装当前在用的表（见第 3 节）。 | 归档表的 README 明说已被 `kafka_field_mapping.json` 取代（「本表取代已归档的分类映射表」）。把归档声明装进生产包 =  ships 一份**已作废**的声明，正是「两份实现静默分叉」那类温床。 |
| D2 | 额外复制了 `kafka_field_mapping.json`（456 KB）、`response_rules.json`（258 KB）、`open_items.json` | 规格「映射表驱动」要求采集端从 `mapping/data/*.json` 加载**全部**客户口径（指标规则、字段映射、视图清单、告警定义目录、不可提供指标清单）。少装就等于运行时要回仓库根取，违背决策 9。包内合计 1.0 MB，可接受。 |
| D3 | 额外复制了 `data/customer_metric_mapping.json`（来源是 `data/` 根，不是 `data/mapping/`） | 校验 2（可提供指标齐全）与校验 3（条目对应真实指标）需要一份**独立于映射表**的基准，否则两条检查是自证的、恒真。 |
| D4 | 新建了 `tools/build_observed_columns.py`（不在「你独占的文件」清单里） | 简报明确要求「声明/生成物必须是生成物，由 `tools/` 下的生成器产出（照抄 `tools/build_response_rules.py` 的模式），不许手抄」。 |
| D5 | `tests/unit/test_mapping_loader.py` 里也放了「包内副本 == 仓库原文件」的逐字节断言 | 简报把 `test_mapping_data_sync.py` 留给 2.1（只管 `merged_sql` 两份）。我复制的 7 张表也得有人守漂移，就在自己的测试文件里守；2.1 的文件另起不冲突。 |
| D6 | 校验 1 覆盖了 **4 个通道**（B 类走 `columns.py`；C 类查集合列；D 类查实例字段；A 类查平台指标名），比简报的「来源列」字面更宽 | 规格把「来源列的权威来源」写成「`data/metric_response/`（98 份）**与** `sql_merge_probe_result.json`（1 条）」两份。`columns.py` 只管 B 类；C/D/A 三类的列引用（`params.field` / `params.sort_by` / `params.column`）此前**无人校验**，补上才算把第一条做实。 |
| D7 | B 类的「维度绑定列」（`dimension_bindings`）由 `loader.py` 单独校，没有改 `columns.py` | `columns.py` 覆盖的是 `value_rule.params` 里的列，`dimension_bindings`（占位符 → 列）是另一处列引用。wave 0 的文件按简报不许改，所以在 loader 里补了一个 20 行的补充检查，并在注释里写明原因。 |

---

## 7. 遗留问题 / 需要控制器知道

1. ⚠️ **`mapping/table.py` 仍在读仓库根** `data/mapping/`（`DATA_DIR` 指向 `REPO_ROOT`）。
   本任务交付的是**新的**包内加载器 `loader.py`，两条加载路径并存。采集端正式改用
   `loader.load_mapping()` 的时机建议放在 wave 2（2.1 之后），届时把 `table.py` 的
   `MappingTable.load` 改成薄封装或直接删掉 —— 否则又是一份「两份实现」。
   **本任务按简报没有动 wave 0 的 `table.py`。**
2. ⚠️ **`mapping/__init__.py` 的模块 docstring 现在过时**：它写着「映射**数据**仍在仓库根
   `data/mapping/`（wave 2a 任务 2.1 再搬进包内）」。数据已经搬进来了（loader 口径）。
   该文件是 wave 0 的、且 1.8 大概率要改它，**本任务没动**，请指派给 1.8 或 2.1 顺带改。
3. **包内数据无 `setup.py` / `package_data` 声明** —— 打包配置不在 wave 1 范围；
   部署形态（`pip install` 还是整目录拷贝）定下来后要确认 `mapping/data/*.json` 会进包。
   已加 `mapping/data/__init__.py` 让它成为可被子包发现的目录。
4. **2.1 的防漂移范围**：2.1 只做 `merged_sql.json` + `sql_merge.sql` 的逐字节一致；
   我复制的 7 张表的一致性由 `tests/unit/test_mapping_loader.py::test_package_copies_are_identical_to_the_repo_files`
   守着。如果 2.1 的 `test_mapping_data_sync.py` 也想统一守，可以把这条搬过去（重复断言无害）。
5. **`data/mapping/README.md` 未复制进包**（文档，不是运行时数据）。
6. **`observed_columns.json` 只覆盖了当前留档**：若将来换了实例或平台改了返回列，
   要重跑 `python tools/build_observed_columns.py`，否则校验 1 会以「列不存在」的形式
   fail loud（这是期望行为，不是 bug）。

---

## 8. 复现命令

```bash
cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
conda run -n tpops python -m pytest tests/unit/test_mapping_loader.py -q   # 15 passed
conda run -n tpops python -m pytest -q                                      # 427 passed
ruff check tpops_kafka_collector/                                           # All checks passed
conda run -n tpops python tools/build_observed_columns.py --check           # observed_columns.json 是最新的
```
