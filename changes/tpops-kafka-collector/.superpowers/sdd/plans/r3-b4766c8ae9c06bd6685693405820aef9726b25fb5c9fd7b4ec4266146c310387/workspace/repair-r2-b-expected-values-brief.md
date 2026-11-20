# 修复轮次 2 · 任务 B —— I-1：82 个 B 类指标的期望值表

波次：`wave-0-mapping-baseline`　计划：sdd revision 3
上一轮复审报告：`changes/tpops-kafka-collector/.superpowers/sdd/reviews/wave-0-mapping-baseline-rereview.md`
（结论 `fail`，Important **I-1** 就是本任务）

---

## B-1 背景：波次 0 的完成标准与当前缺口

合同 `execution-contract.md` 的 **Wave 0 完成标准**第 2 条写的是：

> 82 个 B 类指标在合并形态下的值**等于期望值表**

本波次的立项理由是：合并 SQL 返回形态下，82 个 B 类指标会**静默产出 0 / None / 空列表**
且不报错（见 `tests/test_merged_sql_mapping.py` 开头的中文说明）。

目前做到的是：
- **列存在性**被守住了 —— `tpops_kafka_collector/mapping/columns.py` 校验每个规则要取的列
  在实测留档的**实际返回列**里存在（生成期 + 启动期双重校验）。
- **值相等**只验了 **2 条** `ratio_across`
  （`tests/test_merged_sql_mapping.py:412` / `:444`，期望值从留档原始行自己算，23/29 ≈ 0.79）。
- 其余 **80 条**只受列存在性保护。

**列校验挡不住「列名对、但聚合口径错」** —— 例如 `raw` 的 `row_policy` 写错、
`aggregate` 的 `fn` 写错、`counter_delta` 的差分基线算错，都会产出「看起来合法的 0」。
那正是本波次要根治的缺陷形态。

用户已裁定：**现在就补**。

## B-2 目标

对 82 个 B 类（`channel == "command_center_sql"`）指标：

1. 写一个**独立的期望值推导器**——按每条规则的 `value_rule` 声明，从**实测留档的原始行**
   推出该指标**应该**是多少。
2. 产出一张「留档 → 期望值」表（**生成物，不手抄**）。
3. 加一条测试，断言**规则引擎的输出 == 期望值表**，82/82 全中。
4. `counter_delta` 的 5 条需要两轮快照才能差分，用**合成样本**（构造两轮留档）单独验。

## B-3 事实依据（自己核实，不要全信这里）

**82 条规则的声明分布**（`data/mapping/metric_rules.json` 的 `rules`，
筛 `source.source == "command_center_sql"`）：

| `value_rule.type` | 条数 | params 形态 |
|---|---|---|
| `raw` | 54 | `{column, row_policy, group_by}`；`row_policy` 形如 `sum` / `first` |
| `aggregate` | 12 | `{filter, group_by, fn, value_column}`；`filter` 形如 `{"state": "..."}` 或 `{"state_contains": [...]}` |
| `raw_filtered` | 4 | `{filter: {"name": ...}, column, cast}`；按 `name` 等值过滤取列 |
| `raw_rows` | 3 | `{}`；产出整行列表 |
| `counter_delta` | 5 | `{column, row_policy: "per_row", group_by: ["datname"]}` |
| `ratio_across` | 2 | `{numerator: {sql_key, fn, value_column}, denominator: {...}}` —— **已验** |
| `top_n` | 2 | `{sort_by, order, limit, columns}` |

**数据来源**：

- 实测留档（合并形态）：`data/merged_sql/sql_merge_probe_result.json`
  —— 一行 N 个 `json_agg` 列，按节点分。视图名与列定义见 `data/merged_sql/merged_sql.json`
  与 `data/mapping/response_rules.json` 的 `merged_views` 段。
- 每条规则取哪张视图的哪些行：`data/mapping/response_rules.json` 的 `rules`（`shape` 字段，
  如 `cmd_rows_stat_database` / `cmd_rows_activity_agg` / `cmd_rows_stat_user_tables` …）。
- 规则引擎（**被测对象**）：`tpops_kafka_collector/mapping/rules.py`。
- 合并形态解码：`tpops_kafka_collector/mapping/decode.py` 的 `decode_merged` /
  `merge_pages`。

## B-4 独立性要求（**这是本任务的核心，不能糊弄**）

期望值推导器**不得调用** `tpops_kafka_collector/mapping/rules.py` 的规则求值路径。
两套实现各自从声明出发，但必须**各自独立地**解释 `value_rule`：

- 期望值侧：一份**简单、直白**的求值代码，按 `params` 字面意思实现
  （`sum` 就是求和、`state_contains` 就是子串包含、`desc` 就是降序）。
- 被测侧：现有的规则引擎。

测试断言两者相等。这样「规则引擎把 `state_contains` 理解错了」这类缺陷会被抓住；
如果两边共用一份求值代码，这个测试就是恒真的、毫无价值。

> 如果某条规则的 `params` 语义有歧义（例如 `raw` 的 `row_policy` 到底有哪些取值、
> `aggregate` 的 `fn` 有哪些取值），**不要去读 `rules.py` 然后照抄它的行为**——
> 那会破坏独立性。去读：① `data/mapping/metric_rules.json` 里该规则的 `note`；
> ② `docs/开发方案-GaussDB指标与告警采集.md` §4.3 / §4.4 的口径；
> ③ 实测留档里的实际数据形态。实在判不出来，在报告里写明并给出你的取舍理由。

**同样的道理适用于期望值本身**：期望值必须从**留档原始行**推出来，
不能从规则引擎的输出抄一份当期望值（那是自证）。

## B-5 交付形态（沿用仓库既有模式）

仓库既有模式是「生成器 → 生成物 → 一致性测试」，照抄这个模式：

- 生成器：`tools/build_expected_values.py`（离线工具，允许放在 `tools/`；
  仓库根 `tools/` 与 `data/` 只作参考与离线工具，不作生产依赖）。
- 生成物：`data/mapping/expected_values.json`。
- 测试：`tests/test_expected_values.py`
  - 断言 82 条 B 类指标**逐条**「规则引擎输出 == 期望值表」，且**一条不落**（数出 82 条）；
  - 断言生成物与生成器**一致**（重跑生成器后 `git diff` 为空）——照抄
    `tests/test_response_rules.py::test_file_is_up_to_date` 的写法；
  - `counter_delta` 的 5 条：构造**两轮**留档（第 1 轮 = 实测留档，第 2 轮 = 计数增加的合成留档），
    断言差分速率 = 期望速率；并断言首轮无基线时值为 `None`（不产出假数据）。

> wave 3 的任务 3.4 会把这张表和留档搬进 `tests/fixtures/`。
> 本任务把表放在 `data/mapping/`，3.4 搬进 `tests/fixtures/` 时**复用同一张表**，
> 不要在两处各生成一份。

## B-6 硬约束

- **Python 3.7.9**：禁海象 `:=`、`f"{x=}"`、字典合并 `|`、内建泛型注解 `list[str]`、
  `typing.Literal/Protocol/TypedDict/Final`、`functools.cached_property`、`zoneinfo`、
  `str.removeprefix/removesuffix`。一律用 `typing.List/Dict/Optional/Set/Tuple`。
- **不新增依赖**（只用标准库 + 既有 `requests`/`pytest`）。
- **单文件 < 400 行、函数 < 50 行**（合同硬约束）。
- **`ruff check tpops_kafka_collector/` 必须为 0**（仓库 `ruff.toml`：`target-version = "py37"`，
  `line-length = 100`，`select = ["E","F","W","E501","UP","FA102"]`）。
  你新增的文件也必须过这条线。**注意**：`tools/` 与 `tests/` 目录不在这条门禁范围内，
  但你新增的文件仍应写干净。
- **凭据只走环境变量**，本任务不涉及凭据。
- **不静默吞异常**：错误显式包装并带上下文，用 `logging` 不用 `print`。
- **不手抄期望值**：表必须是生成物。
- **不改** `changes/tpops-kafka-collector/execution-contract.md` 与 `tasks.md`
  （改了会让执行计划 stale）。
- **不改** `tpops_kafka_collector/` 下**已存在**的文件 —— 那是并行任务 A 的领域
  （A 负责把那里的 44 条 lint 清零 + 改 `CLAUDE.md`/`README.md` + 给 `decode_merged` 加空视图集检查）。
  你只**新增**文件，以及修改 `tests/` 与 `tools/`、`data/mapping/` 下你新建的文件。
  如果你确实需要改包内既有模块，**先停下来**，在报告里说明需要改哪一处、为什么，
  由控制器裁决。

## B-7 完成判据

1. `pytest tests/test_expected_values.py -q` 通过，且断言覆盖**全部 82 条**（不是抽样）。
2. `python tools/build_expected_values.py` 重跑后 `git diff --exit-code data/mapping/expected_values.json` 为空。
3. `pytest tests/ -q` 保持全绿（当前基线 **238 passed**，你的新增用例会让总数上升）。
4. `pytest tests/test_py37_compat.py -q` 保持通过。
5. 报告里给出：82 条的类型分布与你实际验到的条数、任何一条判不出来或取舍过的规则及其理由、
   `counter_delta` 的合成样本构造方式。
