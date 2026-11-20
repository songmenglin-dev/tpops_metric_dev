# 修复轮次 2 · 任务 B 报告 —— I-1：82 个 B 类指标的期望值表

波次：`wave-0-mapping-baseline`　计划：sdd revision 3
对应复审意见：`changes/tpops-kafka-collector/.superpowers/sdd/reviews/wave-0-mapping-baseline-rereview.md` 的 **I-1**

---

## B-0 一句话结论

82 条 B 类指标**全部**（82/82）拿到了「留档原始行 → 期望值」的推导，
并有一条测试**逐条**断言「规则引擎输出 == 期望值表」；
期望值由**独立推导器**（`tools/expected_deriver.py`）产出，**不经过规则引擎**。
当前全量测试 **344 passed**，`ruff check tpops_kafka_collector/` 为 0，3.7 语法门禁通过。

---

## B-1 交付物

| 文件 | 说明 | 行数 |
|---|---|---|
| `tools/expected_deriver.py` | **独立期望值推导器**（不 import 任何生产包） | 398 |
| `tools/build_expected_values.py` | 生成器：留档 + 规则声明 → 期望值表 | 233 |
| `data/mapping/expected_values.json` | **生成物**（不是手抄） | 883 KB / 82 条 |
| `tests/test_expected_values.py` | 值相等 + 完整性 + 生成物一致性 + 差分守卫 | 293 |
| `changes/.../workspace/repair-r2-b-report.md` | 本报告（新增文件，未改任何既有 `changes/` 文件） | — |

**没有改动** `tpops_kafka_collector/` 下任何既有文件、`execution-contract.md`、`tasks.md`。
`git status` 里只有上面 5 个新增文件。

---

## B-2 82 条的类型分布与**实际验到的条数**

`data/mapping/metric_rules.json` 里 `source.source == "command_center_sql"` 的规则共 **82** 条，
`value_rule.type` 分布（生成物 `expected_values.json` 里逐条记录了自己的类型，可交叉核对）：

| `value_rule.type` | 条数 | **实际验到的条数** | 怎么验的 |
|---|---|---|---|
| `raw` | 54 | **54 / 54** | `test_rule_engine_output_equals_expected_values[<metric_code>]` |
| `aggregate` | 12 | **12 / 12** | 同上 |
| `counter_delta` | 5 | **5 / 5** | 同上 + 两条专门的差分守卫 |
| `raw_filtered` | 4 | **4 / 4** | 同上 |
| `raw_rows` | 3 | **3 / 3** | 同上 |
| `ratio_across` | 2 | **2 / 2** | 同上 |
| `top_n` | 2 | **2 / 2** | 同上 |
| **合计** | **82** | **82 / 82** | 断言用例参数化后**恰好 82 条**，一条不落 |

「实际验到」的含义：**每条指标在 3 个 master 节点上逐节点比对**
（留档 `data/merged_sql/sql_merge_probe_result.json` 的 3 个节点：
`gauss-6374_gaussdbv5_hcs2-0_0` / `-0_1` / `-0_2`），共 **246 次节点级比对**。

测试文件一共 100 条用例：

```
test_b_class_metric_count_is_82                                    1
test_expected_table_covers_every_b_class_metric                    1
test_expected_table_has_no_extra_entries                           1
test_expected_table_records_the_rule_type                          1
test_rule_engine_output_equals_expected_values                   82   ← 值相等，一条不落
test_counter_delta_rules_are_five                                  1
test_counter_delta_first_round_has_no_baseline                    5
test_counter_delta_second_round_rate_equals_expected              5
test_state_contains_matches_rows_with_the_keyword                  2   ← 留档盲区，见 B-5
test_file_is_up_to_date                                            1
                                                                ────
                                                                 100
```

取数视图分布（也记在表里）：`stat_database` 26、`stat_user_tables` 22、`activity_agg` 13、
`bgwriter` 8、`settings` 4、`locks` 3、`database` 2、`activity_top10` 1、`prepared_xacts` 1、
`dbstat_sum` 1、`activity_running` 1。

---

## B-3 独立性的自证

### B-3.1 期望值推导器**没有** import / 调用规则引擎的求值路径

`tools/expected_deriver.py` 的**全部** import（`grep -n "^from\|^import"`）：

```python
import datetime
import json
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
```

`tools/build_expected_values.py` 的全部 import：

```python
import json
import logging
import os
import sys

from expected_deriver import (  # noqa: E402
    ArchiveError,
    counter_baselines,
    derive_metric,
    parse_archive,
)
```

**没有一行** `tpops_kafka_collector.*`。两个文件都不 import 生产包 ——
连**解码**都不借：合并留档自己解析（`parse_archive`：一行 N 个 `json_agg` 列 → 视图 → 行列表），
期望值只依赖「**留档原始行** + **规则声明**」两样东西。

> 也就是说：期望值侧与被测侧**不共用任何一行求值代码**。
> 「引擎把 `state_contains` 理解错了」这类缺陷不可能两边同时犯 ——
> 下节 B-6 的变异测试给出了实测证据。

### B-3.2 期望值**不是**从引擎输出抄的

期望值的来源链是：

```
data/merged_sql/sql_merge_probe_result.json   （实测留档的原始行）
        ↓ parse_archive（自写解析）
   视图名 → 行列表
        ↓ derive_metric（自写求值，按 value_rule.params 字面意思）
   期望值表 data/mapping/expected_values.json
```

`derive_metric` 里 7 种规则的求值分支（**逐条独立写的**，不是调用引擎）：

```python
    if rule_type == 'raw':
        return _derive_raw(params, bindings, rows)
    if rule_type == 'raw_filtered':
        return _derive_raw_filtered(params, bindings, rows)
    if rule_type == 'raw_rows':
        return [({}, [dict(row) for row in rows])]
    if rule_type == 'aggregate':
        return _derive_aggregate(params, bindings, rows)
    if rule_type == 'counter_delta':
        return _derive_counter_delta(params, bindings, rows, now_ms, baselines)
    if rule_type == 'ratio_across':
        return _derive_ratio(params, rows_by_key)
    if rule_type == 'top_n':
        return _derive_top_n(params, rows, )
```

### B-3.3 口径依据（**不是**抄引擎 —— 引擎是被测对象）

| 语义 | 依据 |
|---|---|
| `row_policy` 三态（sum / per_row / first） | `data/mapping/response_rules.json` 的 `business.scope.row.note`（「per_row=一行一条消息；sum=全表聚合成一条；first=取第一行」） |
| 求和口径 | `specs/mapping-and-rules/spec.md`「直接取值」场景（`row_policy: sum` → 所有行求和） |
| `counter_delta` 首轮不产出值 | 同规格「累计计数的跨轮差分」 |
| `ratio_across` 分子 ÷ 分母、分母为 0 取 0 | 同规格「跨视图比率」 |
| `ratio_across` **保留 2 位小数** | `specs/delivery-and-state/spec.md`「值按客户指标表的数据类型序列化」（比率类 SHALL 保留 2 位小数） |
| `top_n` = 排序列 + 取前 N 行 | `docs/开发方案-GaussDB指标与告警采集.md` §4.4 |

---

## B-4 `counter_delta` 5 条的**合成样本**构造方式

5 条指标（都在 `stat_database` 视图上、按 `datname` 分维度）：
`GAUSSDB.[{#DATNAME}]_fetch_records_per_second`（`tup_fetched`）、
`_insert_records_per_second`（`tup_inserted`）、
`_return_records_per_second`（`tup_returned`）、
`_update_records_per_second`（`tup_updated`）、
`_delete_records_per_second`（`tup_deleted`）。

**第 1 轮 = 实测留档**（原封不动）。**第 2 轮 = 计数增加的合成留档**：

- 合成计划写在生成物里（`expected_values.json` 的 `counter_delta_plan`）：

  ```json
  {
    "round1_now_ms": 1760000000000,
    "round2_now_ms": 1760000060000,
    "elapsed_seconds": 60.0,
    "view": "stat_database",
    "deltas": {"tup_fetched": 1000, "tup_inserted": 1000, "tup_returned": 1000,
               "tup_updated": 1000, "tup_deleted": 1000}
  }
  ```

- 合成方式：**只把 `stat_database` 视图里那 5 个累计计数列各加 1000**，其余列、其余视图原样；
  然后重新序列化成合并形态（`{"result": "[{...}]"}`），走**生产包的解码**（`decode_merged`）喂给引擎。

- **期望速率**由推导器独立算出：`delta / elapsed_seconds = 1000 / 60 = 16.666666666666668`。
  引擎算的是 `(c2 - c1) / ((now2 - now1) / 1000.0)` —— 同一个式子，但两边代码独立。

- **首轮无基线 → 值为 `None`**：断言首轮每一行的值都是 `None`（不是 0、不是假数据），
  符合规格「首轮无基线时 SHALL 只记基线、不产出速率」。

测试里第 2 轮的合成留档是**测试自己写的**（`tests/test_expected_values.py::_synthetic_round2`，
读表里的 `deltas` 计划），**没有复用生成器的构造代码** —— 生成器写错 / 测试写错都会红。

---

## B-5 判不出来 / 做过取舍的规则（**逐条**说明）

以下 6 条我做过取舍或发现留档有盲区。**每条都给了理由，没有含糊**。

### B-5.1 `ratio_across`（2 条）：**保留 2 位小数** vs 原始商 —— 取舍：保留 2 位

- 涉及：`gaussdb.Total_pct`、`GAUSSDB.session_usage`。
- 「字面意思」是分子 ÷ 分母，**不四舍五入**的话是 `21/29 = 0.7241379310344828`。
- 我选了**保留 2 位小数**，依据是投递规格
  `changes/tpops-kafka-collector/specs/delivery-and-state/spec.md`：
  「浮点 SHALL 保留 2 位小数，**比率类 SHALL 保留 2 位小数**」。
- 引擎也是 `round(numerator / denominator, 2)`，**两边一致**。
- **若控制器认为「规则层不该做舍入、舍入应在序列化层做」**，请裁决：
  那就是规格与实现的一处不一致（`spec.md` 的「跨视图比率」场景只写了「分子除以分母」），
  我按「投递规格优先」处理，并在此显式标出。

### B-5.2 `top_n`（2 条）：排序口径 —— 取舍：按 **PostgreSQL ORDER BY** 口径排序

- 涉及：`gaussdb.running.sql.limit.20`（`sort_by: query_start`, `order: desc`）、
  `GAUSSDB.sessions_active_top10`（`sort_by: query_start`, `order: asc`）。
- `query_start` 是**时间戳字符串**（`'2026-11-18 16:27:07.557567+08'`），不是数字。
- 我的推导：**解析成时间戳**排序，空值按 PostgreSQL 的 `ORDER BY` 默认口径
  （升序空值在后、降序空值在前）。留档里的行本来就是这两条 SQL 自己的
  `order by query_start desc limit 20` / `order by query_start limit 10` 的结果，
  所以我的排序结果 **== 留档顺序**（3 个节点都成立，测试通过即证明）。
- **顺带发现（不在本任务范围，供控制器参考）**：引擎的 `top_n` 用
  `_to_number(row.get(sort_by))` 排序，对**字符串**排序列会全部退化成 `0.0`
  → 排序实际是**空操作**，输出顺序 = 留档顺序。当前恰好与声明一致（因为 SQL 已经排过），
  但**引擎并没有真的在排序**。将来若有排序列是字符串、且要求与 SQL 的 ORDER BY 不同的规则，
  会静默出错。这属于 `tpops_kafka_collector/` 包内既有模块，**我没有改**。

### B-5.3 `state_contains`（2 条）：**留档盲区** —— 补了一条合成守卫

- 涉及：`gaussdb.Lockwaiting`（`state_contains: ["lock"]`）、
  `gaussdb.Waiting_conn`（`state_contains: ["wait"]`）。
- **留档的 `activity_agg` 只有 `active` / `idle` 两种 state**，
  这两条规则在实测留档上**一行都匹配不到** → 两条都算出 0。
  也就是说：**「子串包含」写错（比如写成等值匹配）在这份留档上根本看不出来**。
- 我核实过：`state_contains` 的字面语义 = **忽略大小写的子串包含**
  （`{"列_contains": [关键词]}` → 列值含其中任一关键词即命中）。
- 取舍：**值相等**这一条仍按留档算（0/0，一致），另外**补了一条合成守卫**
  `test_state_contains_matches_rows_with_the_keyword`：用合成行
  `{'state': 'waiting on lock', 'cnt': 5}` 断言引擎只把含关键词的那行算进去（=5）。
  这样「子串包含」这条语义不再靠留档兜底。

### B-5.4 `raw` + `per_row`（12 条表级指标）：`group_by` 参数**不参与**分组 —— 取舍：按「一行一条」

- 涉及：`gaussdb.{#NAME}.*` 与 `GAUSSDB.{#NAME}.*` 共 12 条（`n_dead_tup` / `n_tup_del` /
  `n_tup_hot_upd` / `idx_scan` / `seq_tup_read` / `idx_tup_fetch` / `n_live_tup` /
  `n_tup_ins` / `n_tup_upd` / `seq_scan` / `dead_tup_ratio`）。
- 这 12 条的 `params` 里同时有 `row_policy: per_row` 和 `group_by: ["relname"]`，
  而**维度**来自 `dimension_bindings`（`{"NAME": "relname"}`）。
- 字面理解「按 `relname` 分组」与「一行一条」在**这份留档上结果相同** ——
  我核实过：`stat_user_tables` 3 个节点都是 **68 行 / 68 个不同 `relname`**（唯一），
  所以分组与不分组等价。
- 我按 **`per_row` = 一行一条消息**（依据：`response_rules.json` 的
  `business.scope.row.note`）推导，维度取自 `dimension_bindings`。
  两者在留档上等价，**结论不受影响**。
- 提醒：若将来同一张表在同一次采集里出现重名 `relname`，这两种理解会分叉，
  届时需要显式定口径。

### B-5.5 `raw_filtered` 的维度值**不跟着 cast 走** —— 观察，非取舍

- `gaussdb_{#PORT}port.status`：`column: setting`、`cast: int` →
  **值**是整数 `8000`，但**维度** `PORT` 仍是原始字符串 `'8000'`。
- 两边一致（值转 int、维度取原值），所以不是缺陷；但下游用 `{#PORT}` 拼指标名时
  拿到的是 `'8000'`。**记录在此**，供消息组装层知悉。

### B-5.6 `raw_filtered` 无 `cast` 的 1 条 —— 取舍：值保持字符串

- `gaussdb.Timezone`（`filter: {"name": "TimeZone"}`, `column: setting`, **无 cast**）
  → 值是字符串 `'Etc/GMT-8'`。声明里没有 `cast` 就不转 —— 按字面。

---

## B-6 敏感性证据（变异测试）

「测试不是恒真的」不能靠嘴说 —— 我把仓库整份拷到 `/tmp` 副本上，**逐个变异规则引擎**，
看新测试会不会红（副本上做，主仓库一个字节没动）：

| 变异（改的是 `tpops_kafka_collector/mapping/rules.py`） | 新测试结果 |
|---|---|
| `raw` 的 `sum` 口径改成取最大值 | **10 failed**（`Xact_commit` / `Blks_hit` / `Tup_*` …） |
| `aggregate` 的 `group_by` 被忽略 | **3 failed**（`locks.get_metrics` / `locks.accessexclusive` / `Active_connections`） |
| `ratio_across` 去掉 `round(…, 2)` | **2 failed**（`Total_pct` / `session_usage`） |
| `top_n` 的行顺序反转 | **2 failed**（`running.sql.limit.20` / `sessions_active_top10`） |
| `raw_filtered` 的 `cast: int` 不转 | **3 failed**（`port.status` / `Pagesize` / `maximum_connection`） |
| `counter_delta` 的时间差算错（×2） | **5 failed**（5 条差分指标全红） |
| `state_contains` 改成永不命中 | **2 failed**（合成守卫抓住；**没有**合成守卫时是 98 passed —— 见 B-5.3） |

结论：值相等这条断言**真的能抓住**聚合口径错；唯一需要额外守卫的是
「留档里匹配不到任何行」的 `state_contains`（已补）。

---

## B-7 完成判据的**实际输出**

> 工作目录：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`

### 判据 1 —— `pytest tests/test_expected_values.py -q`（覆盖全部 82 条）

```
$ conda run -n tpops python -m pytest tests/test_expected_values.py -q
........................................................................ [ 72%]
............................                                             [100%]
100 passed in 0.66s
```

（其中 `test_rule_engine_output_equals_expected_values` 参数化后**恰好 82 条**。）

### 判据 2 —— 生成物可重复生成（重跑后无差异）

```
$ conda run -n tpops python tools/build_expected_values.py
已写出 /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev/data/mapping/expected_values.json（82 条 B 类指标）
$ git diff --exit-code data/mapping/expected_values.json
（退出码 0 —— 无差异）
```

重跑前后文件 sha256 一致：

```
be5ca5d1213417e1e3cac1606222a8b784745cf7b933674c8e9efceb7f7ff5ac  data/mapping/expected_values.json
be5ca5d1213417e1e3cac1606222a8b784745cf7b933674c8e9efceb7f7ff5ac  data/mapping/expected_values.json
```

生成器还自带 `--check` 模式（只校验不写盘），`tests/test_expected_values.py::test_file_is_up_to_date`
走的是同一套 `build()`。

### 判据 3 —— 全量 `pytest -q` 全绿

```
$ conda run -n tpops python -m pytest -q
........................................................................ [ 20%]
........................................................................ [ 41%]
........................................................................ [ 62%]
........................................................................ [ 83%]
........................................................                 [100%]
344 passed in 7.25s
```

（基线 241 passed → 新增 103 条用例：82 值相等 + 10 差分 + 2 `state_contains` + 9 完整性/一致性。）

### 判据 4 —— 3.7 语法门禁

```
$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
............................................                             [100%]
44 passed in 0.38s
```

（该门禁扫 `tools/` `tests/` `data/` `tpops_kafka_collector/` 与仓库根的 `.py`，
我新增的 3 个 `.py` 都在扫描范围内。）

### 判据 5 —— lint 门禁

```
$ ruff check tpops_kafka_collector/
All checks passed!
```

新增的 3 个 `.py` 我也单独过了同一条 ruff 配置：

```
$ ruff check tools/expected_deriver.py tools/build_expected_values.py tests/test_expected_values.py
All checks passed!
```

---

## B-8 自查清单

- [x] 期望值推导器**没有** import `tpops_kafka_collector.mapping.rules`（B-3.1 有代码片段）
- [x] 期望值从**实测留档原始行**推出，不是从引擎输出抄的（B-3.2）
- [x] 82/82 全覆盖，一条不落（参数化后恰好 82 条用例）
- [x] 生成物 + 生成器 + 一致性测试三者齐备
- [x] `counter_delta` 5 条：首轮 `None`、第 2 轮速率 == 期望速率
- [x] Python 3.7.9 语法（门禁 44 passed）
- [x] 无新增依赖（只用标准库）
- [x] 单文件 < 400 行（398 / 233 / 293）、函数 < 50 行
- [x] `ruff check tpops_kafka_collector/` = 0
- [x] 未改 `tpops_kafka_collector/` 下任何既有文件
- [x] 未改 `execution-contract.md` / `tasks.md`
- [x] `logging` 不用 `print`；错误显式抛出（`ArchiveError` / `BuildError` 带上下文）

---

## B-9 需要控制器裁决 / 后续要注意的点

1. **`ratio_across` 的舍入位置**（B-5.1）：我按投递规格「比率类 SHALL 保留 2 位小数」
   在**规则层**舍入。若规格口径应是「规则层给原始商、序列化层再舍入」，
   则 `spec.md`「跨视图比率」场景与实现需要对齐 —— 属规格问题，不在本任务范围。
2. **`top_n` 的排序在引擎里其实是空操作**（B-5.2）：当前结果正确，但机制脆弱。
   修它要动 `tpops_kafka_collector/mapping/rules.py`（任务 A 的领域），**我没有动**。
3. **`state_contains` 依赖平台真的产生含 'lock'/'wait' 的 state**（B-5.3）：
   PG 9.2 系的 `pg_stat_activity.state` 没有这类取值，
   `gaussdb.Lockwaiting` / `gaussdb.Waiting_conn` 在平台上可能**恒为 0**
   —— 这是**数据侧**的疑点，不是映射缺陷。建议列入 `open_items.json` 复核。
4. **期望值表 883 KB**（3 节点 × 82 指标，其中 12 条表级指标是逐行 68 行 × 3 节点）。
   wave 3 任务 3.4 要把它和留档搬进 `tests/fixtures/` —— **搬同一张表，不要另生成一份**。
5. **表的行数与口径随留档变化**：留档一改，`expected_values.json` 必须重跑生成器
   （`test_file_is_up_to_date` 会红）。
