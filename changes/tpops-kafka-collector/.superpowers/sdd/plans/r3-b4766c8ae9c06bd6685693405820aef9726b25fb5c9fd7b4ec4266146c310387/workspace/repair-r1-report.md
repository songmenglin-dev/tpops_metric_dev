# wave-2b 修复轮次 1 报告 —— 三条 Critical + 四条 Important

- **波次**：`wave-2b-channels`（审查 verdict = **fail**）
- **工作树**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`
- **实施日期**：2026-11-23
- **本轮改动**：生产 7 改 4 增，测试 2 改 5 增
- **未提交**（按提交纪律，由控制器统一提交）

---

## 1. 结论摘要

| 编号 | 状态 | 修法（一句话） | 守卫测试 |
|---|---|---|---|
| **C-1** Critical | ✅ 修完 | `LoadedMapping` 上补 `alarm_definition()` / `metric_rule()`（与 `MappingTable` **共用同一份实现**） | `test_collector_works_with_the_production_mapping_object`、`test_production_mapping_object_finds_the_same_definitions` |
| **C-2** Critical | ✅ 修完 | 采集时刻 `now_ms` 从 `collect()` 一路传到 `decode_merged` / `merge_pages`（**同一轮所有页同一个值**） | `test_counter_delta_produces_a_rate_on_the_second_round` |
| **C-3** Critical | ✅ 修完 | 差分基线**按节点隔离**（`ScopedCounterDeltaStore`），基线键里带作用域 | `test_every_master_computes_its_own_rate`、`test_counter_delta_rate_survives_a_restart_with_a_seeded_store` |
| **I-1** Important | ✅ 修完 | 拆成 3 个文件：`command_center.py`(344) + `command_center_helpers.py`(161) + `command_center_assembly.py`(151)；两个超长函数各拆两段 | 全部既有 B 类用例仍绿 + `test_result_reports_the_shrunk_page_size` |
| **I-2** Important | ✅ 修完 | 值相等守卫按**替换后的码**匹配，`checked` 断言**精确值 1700**，并逐条列出 82 条指标码 | `test_collected_values_equal_the_expected_table`、`test_every_b_class_rule_is_covered_by_the_produced_messages` |
| **I-3** Important | ✅ 修完 | `_master_nodes` 异常与「没有该能力」都记 **ERROR**（带实例 ID 与异常文本） | `test_master_nodes_failure_is_logged_as_error`、`test_missing_master_nodes_capability_is_logged_as_error` |
| **I-4** Important | ✅ 修完 | 新增**采集器层**回归：四个通道都用实测留档回放驱动真实采集器，断言 98 个可提供指标全部产出（97 条有数据 + 1 条留档里本就无值） | `tests/regression/test_collector_coverage*.py`（6 个用例） |
| M-1 | ✅ 顺带修完 | `collectors/instance.py::collect` 52 行 → 抽 `_collect_messages`（现在 **0 个**函数 > 50 行） | 既有 D 类用例仍绿 |
| M-2 | ✅ 修完 | 告警分页：`total_count > 0` 才据此判结束 | `test_pagination_continues_when_total_count_is_zero` |
| M-3 | ✅ 修完 | 兜底路径：值与资源名不同源时**本轮不产出**（`master_view` 非 `identical`） | `command_center_assembly._rule_messages` |
| M-4 | ✅ 修完 | 采集器加 `from_config`（页容量取 `CMD_PAGE_SIZE`） | `test_from_config_takes_the_page_size_from_the_config` |
| M-5 | ✅ 修完 | 结果回报**实际用的**（缩小后的）页容量 | `test_result_reports_the_shrunk_page_size` |
| M-10 | ✅ 修完 | 「不推进水位」改成**可证伪**的行为断言（按**类型**查 + 水位文件逐字节不变 + 一整轮采集前后水位状态不变） | `tests/integration/test_collectors_do_not_advance_watermarks.py`（4 个用例） |

**验收**

```
$ conda run -n tpops python -m pytest -q
1295 passed in 23.05s          # 修复前基线 1264 passed（+31 个用例）

$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
120 passed in 0.93s

$ wc -l tpops_kafka_collector/collectors/command_center*.py
344 command_center.py / 161 command_center_helpers.py / 151 command_center_assembly.py

生产包内 > 50 行的函数：0 个（修复前 1 个：instance.py::collect = 52 行）
生产包内 >= 400 行的文件：0 个（修复前 2 个：command_center.py 463 / loader.py 399+23）
```

> `mapping/loader.py` 因为 C-1 的两个查表方法一度涨到 422 行 → 把**启动期校验**
> （四类 `check_*`）拆到新模块 `mapping/validation.py`（226 行），loader 回到 236 行；
> `loader.py` 仍然**原样导出**那四个函数名（`from .validation import ...`），
> 外部 import 路径不变。

---

## 2. C-1 【Critical】E 类采集器用生产映射对象必崩 —— 全部告警不产出

### 根因

`collectors/alarm.py:224/236` 调 `mapping.alarm_definition()` / `mapping.metric_rule()`，
这两个方法**只在 `mapping/table.py::MappingTable` 上**；生产装配走
`mapping/loader.py::load_mapping()` → `LoadedMapping`，**没有**这两个方法。

集成测试用 `MappingTable(bundle.field_map, bundle.metric_rules, bundle.alarm_rules, {})`
**手工拼 fixture**，比生产对象多两个方法 → 把缺陷掩盖掉了。

### 修法

1. `mapping/table.py` 抽出**唯一实现** `build_alarm_definition(alarm_rules, alarm_id)`
   （模块级函数），`MappingTable.alarm_definition` 改为调用它；
2. `mapping/loader.py` 的 `LoadedMapping` 上补两个查表方法：
   - `metric_rule(metric_code)` —— 在 `metric_rules` 元组里查；
   - `alarm_definition(alarm_id)` —— 调**同一份** `build_alarm_definition`；
3. **集成测试的 fixture 换成 `load_mapping()`**（这是「假绿」的根源，必须一起换）。

> 选「在生产对象上补方法」而不是改 `collectors/alarm.py`：查表能力本就该在
> **生产用的那个对象**上，否则每个调用点都要自己判断「我拿到的是哪种对象」。

### 守卫测试

- `tests/integration/test_collector_alarm.py::test_collector_works_with_the_production_mapping_object`
  —— 用 `load_mapping()` 驱动 E 类采集器，断言产出 1 条消息；并断言生产对象**有**这两个方法。
- `tests/integration/test_collector_alarm.py::test_production_mapping_object_finds_the_same_definitions`
  —— 生产对象与离线工具用的 `MappingTable` **查表结果逐条相等**（防两处分叉）。
- 整个 `test_collector_alarm.py` 的 fixture 改成 `load_mapping()` → **19 个既有用例**
  从「假绿」变成真守卫（现在它们也走生产对象）。

### RED → GREEN

RED（把 `mapping/loader.py` + `mapping/table.py` 回退到修复前）：

```
FAILED tests/integration/test_collector_alarm.py::test_collector_works_with_the_production_mapping_object
E       AssertionError: 生产映射对象没有告警定义查表
E       assert False
FAILED tests/integration/test_collector_alarm.py::test_production_mapping_object_finds_the_same_definitions
... 共 19 failed, 2 passed in 2.28s     ← E 类整档用例全红（AttributeError: 'LoadedMapping' object has no attribute 'alarm_definition'）
```

GREEN（修复后）：

```
$ conda run -n tpops python -m pytest tests/integration/test_collector_alarm.py -q
22 passed in 0.65s
```

---

## 3. C-2 【Critical】B 类 5 个 `counter_delta` 指标永不产出

### 根因

`collectors/command_center.py::_fetch_pages` 调 `decode_merged(body, self._views)`
**漏传 `now_ms`**（默认 0）→ `RuleContext.now_ms` 恒 0 → `eval_counter_delta` 里
`elapsed = (0 - 0) / 1000 = 0` → 恒走「采集时刻未推进，丢弃本轮差分」→ 值恒 `None`
→ 采集器 `if value is None: continue` → 一条消息都不产出。
`self._clock` 在 `__init__` 里赋值后**全文未用**（冒烟枪）。

> 注：真正决定 `ctx.now_ms` 的是 `_merge()` 里的 `merge_pages`（它会用 `per_page[0]`
> 重建上下文），所以**两处**都必须传 —— 修的时候两个都传了。

### 修法

- `collect()` 里 `now_ms = int(self._clock())` **取一次**，
- 往下传给 `_fetch_pages(instance_id, page_size, now_ms)` → `decode_merged(body, views, now_ms)`，
- 以及 `_merge(bodies, now_ms)` → `merge_pages(pages, views, now_ms)`；
- 缩小页容量的重试**沿用同一个 `now_ms`**（同一轮一个时刻），
- 模块 docstring 与类 docstring 写明「**同一轮内所有页必须用同一个 `now_ms`**」。

### 守卫测试

`tests/integration/test_collector_command_center_deltas.py`（新文件，4 个用例）：

| 用例 | 守什么 |
|---|---|
| `test_first_round_only_establishes_the_baseline` | 第一轮**只建基线、不出值**（差分的定义；不是 0，也不是假数据） |
| `test_counter_delta_produces_a_rate_on_the_second_round` | **第二轮**五个 `*_records_per_second` 都产出速率，三个 master 都产出 |
| `test_every_master_computes_its_own_rate` | 三个 master **各算各的**（用 6000/3000/9000 三种不同增量造数据） |
| `test_counter_delta_rate_survives_a_restart_with_a_seeded_store` | 基线**跨轮延续**（同一个采集器实例） |

### 修复前后对照数字

假命令中心：三个 master 各自的累计计数按 6000 / 3000 / 9000 递增，两轮相隔 60 秒
（真实速率应为 100 / 50 / 150）。差分类消息数（`_records_per_second`）：

| 场景 | 第 1 轮 | 第 2 轮 | 三个 master 的值 |
|---|---|---|---|
| **修复前**（`now_ms` 恒 0） | **0 条** | **0 条** | — |
| **修复后**（`now_ms` 递增） | 0 条（建基线） | **135 条**（3 × 45） | 100 / 50 / 150 ✔ |

（`now_ms` 恒 0 的对照是把 `decode_merged` 与 `merge_pages` 强制成 `now_ms=0` 实测出来的。）

### RED → GREEN

```
RED：FAILED test_counter_delta_produces_a_rate_on_the_second_round
     FAILED test_every_master_computes_its_own_rate
     FAILED test_counter_delta_rate_survives_a_restart_with_a_seeded_store
     E   AssertionError: 节点 3881c04403394ee1b36a2bf873e69007no14 一条差分指标都没产出
GREEN：4 passed in 0.68s
```

---

## 4. C-3 【Critical】差分基线跨 master 共享 → 互相污染

### 根因

`self._deltas` 是**一个** `CounterDeltaStore`，被 3 个 master 共用；
基线键是 `(metric_code, dimension_key)` —— **不含 node** → A 节点的基线被 B 节点覆盖。

### 生命周期结论（本轮必须查清的那一条）

**结论：`CounterDeltaStore` 是「采集器实例级」的运行时状态，不是每轮新建、也不是全局单例。**

依据与设计：

1. 它通过 `CommandCenterCollector.__init__(deltas=...)` **注入**，采集器把它存进
   `self._deltas` 并**持有到采集器销毁为止**；
2. 采集器实例本身按设计**跨轮存活**（`self._spec` / `self._rules` 只在 `__init__` 加载一次，
   `self._page_size` 跨轮被缩小后保留），`collect()` 才是「一轮」的入口；
3. 因此**基线跨轮延续的载体就是采集器实例**（＋可选的 `watermark.WatermarkStore`
   持久化那一层）。**每轮新建一个采集器 ⇒ 差分永远只有首轮（只建基线），
   次轮永远算不出速率。**
4. 修复后：**没注入 `deltas` 时采集器自建一个**（`self._deltas = deltas if deltas is not None
   else CounterDeltaStore()`）—— 采集器不再依赖调用方记得注入；
   自建的存储随采集器存活 ⇒ 跨轮延续成立。

> ⚠️ **交给 wave 3 的硬要求**（写在 `collect()` 的 docstring 与模块 docstring 里了）：
> **B 类采集器必须「启动时装配一次、每轮调 `collect()`」**，不能每轮 new 一个；
> 需要进程重启后仍能差分，就用 `watermark.WatermarkStore.import_baselines_from(deltas)` /
> `export_baselines_into(deltas)` 把基线落盘/恢复（`watermark.py` 的键形状**未变**，无需改）。

### 关于 `watermark.py` 的 `DeltaBaseline` 键

**简报里说「`watermark.py` 的 `DeltaBaseline` 键已经含 node」—— 实测不成立**：

```python
# watermark.py
DeltaBaseline(metric_code, dimension_key, counter_value, sampled_at_ms)
# WatermarkStore._baselines 的键：(metric_code, dimension_key)
```

`dimension_key` 来自 `rules_context.dimension_key(dimensions)`，而 `dimensions` 只含
**业务维度**（`DATNAME` / `NAME` / `DATABASE_NAME` / `DBNAME` / `PORT`）—— **不含 node**。
所以持久化那一侧同样会跨 master 冲突。

**据此的实现选择：把「作用域（node_id）」并进基线键的维度部分，
而键的形状仍是二元组 `(metric_code, dimension_key)`**：

- `rules_deltas.scoped_dimension_key(scope, key)` → `'{scope}:{key}'`
  （维度键本身是 `k=v` 用 `|` 连接的，首段不含 `=` ⇒ 不会与任何维度名撞上）；
- `CounterDeltaStore.rate(..., scope='')` / `forget(..., scope='')` 接受作用域；
- 新增 `ScopedCounterDeltaStore(store, scope)`：**同一存储**的作用域视图（视图无状态，
  基线仍住在底层存储里）→ 跨 master 隔离 + 跨轮延续 + 可整体落盘，**三个要求同时成立**；
- `watermark.py` **一行没改**（键形状与文件格式不变 ⇒ 已有的水位文件继续可用）。

### 修复前后对照数字

同样三个 master、同样 6000/3000/9000 的增量、同样相隔 60 秒：

| store 归属 | 第 1 轮 | 第 2 轮 | 各 master 的条数 | 各 master 的值 |
|---|---|---|---|---|
| **修复前**（共用一个、无作用域） | 0 条 | **45 条** | master_0: **0** / master_1: **0** / master_2: 45 | master_2 的值 = `[224, 225, 227, 250, 269, 272, 279, 318, 9947, 23767, 23861]` —— **跨节点的垃圾速率** |
| **修复后**（基线按节点隔离） | 0 条 | **135 条** | master_0: 45 / master_1: 45 / master_2: 45 | 100 / 50 / 150 ✔ 各自正确 |

（共享存储时另两个 master 命中「累计值回退，判定实例重启」→ 值 `None`；
幸存的那个算的是「自己的当前值 − 别的节点的上轮值」—— **数值看着合法、实则错误**，
正是本项目最贵的一类缺陷。）

### RED → GREEN

```
RED：FAILED test_every_master_computes_its_own_rate
     E   AssertionError: 节点 3881c044... 一条差分指标都没产出
     FAILED test_counter_delta_produces_a_rate_on_the_second_round
     FAILED test_counter_delta_rate_survives_a_restart_with_a_seeded_store
GREEN：4 passed
```

---

## 5. I-1 【Important】`collectors/command_center.py` 463 行 > 合同 400 行

### 修法（拆成 3 个文件）

| 文件 | 行数 | 内容 |
|---|---|---|
| `collectors/command_center.py` | **344**（原 463） | 采集器本体：装配、取数（分页 / 重试 / 解码）、结果对象 |
| `collectors/command_center_helpers.py`（新） | 161 | 常量 + **纯函数**：`command_center_rules` / `merged_views` / `has_more` / `approx_size_bytes` / `host_node_id` / `page_signature` / `same_as_previous` / `is_output_limit_error` / `tier_of` |
| `collectors/command_center_assembly.py`（新） | 151 | **归集与组装**：`build_messages` / `_values_by_node` / `_rule_messages` / `_resource_context` |

两个超长函数各拆两段：

- `_fetch_pages`（原 64 行）→ `_fetch_pages`（**47 行**）+ `_handle_failure`（**27 行**）；
- `_build_messages`（原 59 行）→ `command_center_assembly.build_messages`（**48 行**）
  + `_values_by_node` / `_rule_messages` / `_resource_context`（各自 < 30 行）。

**生产包内 > 50 行的函数：0 个**（顺带把 `collectors/instance.py::collect` 52 行也拆了 —— M-1）。

### 守卫

拆文件是纯重构，**既有 23 个 B 类用例 + 4 个差分用例 + 5 个回归用例全部仍绿**；
另加 `test_result_reports_the_shrunk_page_size`（拆出的 `_handle_failure` 分支）与
`test_from_config_takes_the_page_size_from_the_config`（新增的 `from_config`）。

---

## 6. I-2 【Important】值相等守卫只覆盖 47/82

### 修法

`tests/integration/test_collector_command_center.py::test_collected_values_equal_the_expected_table` 重写：

1. **按替换后的码匹配** —— 期望值表是**模板码**（`GAUSSDB.[{#DATNAME}]_committed_transactions`），
   消息里是**占位符替换后的码**（`GAUSSDB.lz_committed_transactions`）。
   原来直接 `produced.get(metric_code)` → 维度展开型指标**一条都比不到**、`continue` 静默跳过。
   现在按 `substitute_placeholders(模板码, 维度)` 反查。
2. **`node_scoped=false` 的指标只落在承载资源名的那一个 master 上** —— 比对时按
   `select_master_node()` 选出的承载 master 节点比，其余两个 master 不比（原来靠
   `if node_id not in values: continue` 隐式跳过）。
3. **`checked` 断言精确值**：`checked == comparable` **且** `checked == 1700`
   （原来是 `checked > 0` —— 「比少了」不会失败）。
4. **期望值为 `None` 的维度不许产出消息**（缺值不产出假数据）—— 从「不比」变成「反向断言」。
5. **逐条指标码清单断言**：`compared_codes == 有期望值的指标码集合`（76 条），
   外加 `test_every_b_class_rule_is_covered_by_the_produced_messages`
   （82 条模板码一条不落，差的 6 条有据可依，见下）。

### 修复前**没被比到的指标码清单**（35 条 = 全部带占位符的模板码）

**① 29 条：采集器**产出了**消息，但旧守卫的键不匹配 → 被 `continue` 静默跳过**

```
GAUSSDB.[{#DATNAME}]_Active_connections
GAUSSDB.[{#DATNAME}]_cache_hit_ratio
GAUSSDB.[{#DATNAME}]_committed_transactions
GAUSSDB.[{#DATNAME}]_rolled_back_transactions
GAUSSDB.{#NAME}.Number.ad.ples
GAUSSDB.{#NAME}.Number.lete.ples
GAUSSDB.{#NAME}.Number.t.date.ples
GAUSSDB.{#NAME}.garbage.Ratio
GAUSSDB.{#NAME}.number.Of.dex.ples.ad
GAUSSDB.{#NAME}.number.Of.index.scan
GAUSSDB.{#NAME}.number.Of.insert.tuples
GAUSSDB.{#NAME}.number.Of.live.tuples
GAUSSDB.{#NAME}.number.Of.quential.ples.ad
GAUSSDB.{#NAME}.number.Of.sequential.scan
GAUSSDB.{#NAME}.number.Of.update.tuples
gaussdb.DBtsize[{#DATABASE_NAME}]
gaussdb.locks.get_metrics[{#DBNAME}]
gaussdb.{#NAME}.Number.ad.ples
gaussdb.{#NAME}.Number.lete.ples
gaussdb.{#NAME}.Number.t.date.ples
gaussdb.{#NAME}.garbage.Ratio
gaussdb.{#NAME}.number.Of.dex.ples.ad
gaussdb.{#NAME}.number.Of.index.scan
gaussdb.{#NAME}.number.Of.insert.tuples
gaussdb.{#NAME}.number.Of.live.tuples
gaussdb.{#NAME}.number.Of.quential.ples.ad
gaussdb.{#NAME}.number.Of.sequential.scan
gaussdb.{#NAME}.number.Of.update.tuples
gaussdb_{#PORT}port.status
```

**② 6 条：采集器**根本没产出**消息（期望值表里本轮就没有期望值）**

```
GAUSSDB.[{#DATNAME}]_delete_records_per_second     ← C-2（首轮只建基线），第二轮产出
GAUSSDB.[{#DATNAME}]_fetch_records_per_second      ← C-2
GAUSSDB.[{#DATNAME}]_insert_records_per_second      ← C-2
GAUSSDB.[{#DATNAME}]_return_records_per_second      ← C-2
GAUSSDB.[{#DATNAME}]_update_records_per_second      ← C-2
gaussdb.locks.accessexclusive[{#DBNAME}]            ← 留档里一条 AccessExclusiveLock 锁都没有
```

> 审查报告说「35 条里没产出消息的是采集器的问题」—— 实测**两个原因叠加**：
> 29 条是**断言的键不匹配**（采集器其实产出了），6 条是**采集器确实没产出**
> （5 条是 C-2，第 6 条是留档里本就无值）。简报的判断「很可能两个都有」成立。

### 修复后的覆盖数

| | 修复前 | 修复后 |
|---|---|---|
| `checked`（实际逐条比对的组数） | **113** | **1700** |
| 被比到的指标码数 | **47** | **76**（第一轮有期望值的全部） |
| `continue` 静默跳过的指标码数 | **35** | **0**（键不匹配的那 29 条全部比上） |
| 比少的会不会失败 | ❌ 不会（`checked > 0`） | ✅ 会（`checked == comparable == 1700`） |
| 82 条指标码的覆盖 | 47/82 | **76 + 5（第二轮）= 81/82**；剩下的 1 条是留档里无值（已显式断言并注明原因） |

### RED → GREEN

```
RED（回退 command_center.py 后跑新守卫）：
  FAILED test_master_nodes_failure_is_logged_as_error
  FAILED test_missing_master_nodes_capability_is_logged_as_error
  ...
GREEN：
  $ conda run -n tpops python -m pytest tests/integration/test_collector_command_center.py -q
  23 passed in 3.49s
```

（说明：新守卫**抓不到 C-2** —— 期望值表里 `counter_delta` 的 `round1` 值本来就是 `null`
（首轮无基线），所以第一轮的比对条数不受影响；C-2 由 §3 的**第二轮**差分用例守住。
这正是审查报告 §5.2 指出的「真正能证伪的第 2 轮断言在采集器层完全没有」——
本轮补上了。）

---

## 7. I-3 【Important】`_master_nodes` 静默吞异常

### 修法

```python
try:
    return tuple(getter(instance_id))
except Exception as exc:
    LOGGER.error('命令中心节点清单取不到（实例 %s）：%s —— 节点身份换不出来，资源名会指错节点',
                 instance_id, exc)
    return ()
```

另外「客户端**没有** `master_nodes` 能力」（装配错了）也**记 ERROR**（原来静默返回 `()`）。

### 守卫测试

- `test_master_nodes_failure_is_logged_as_error` —— `master_nodes` 抛异常 → 日志含
  `节点清单取不到` 与异常文本；
- `test_missing_master_nodes_capability_is_logged_as_error` —— 没有该能力 → 日志含
  `没有 master_nodes`。

### RED → GREEN

```
RED：两条都 FAILED（回退后日志里什么都没有）
GREEN：23 passed
```

---

## 8. I-4 【Important】「98 个可提供指标全部产出数据」在采集器层不成立

### 根因

`tests/regression/` 与 `tests/test_expected_values.py` **只测映射层**
（`decode_merged` + `evaluate`），**从来没有一个测试走「采集器 → 消息」的全链路**。

### 修法

新增 `tests/regression/test_collector_coverage.py` + `test_collector_coverage_cd.py`
（6 个用例）+ 公共件 `collector_support.py` —— **每个通道都用实测留档回放驱动真实的采集器**：

| 用例 | 内容 |
|---|---|
| `test_a_class_produces_every_providable_metric` | A 类 11 条：逐份留档回放（一份留档只含一个平台指标，3 条规则共用 `gaussdb_instance_status` 时各自用自己那份），值等于期望值表 |
| `test_c_class_produces_every_providable_metric` | C 类 4 条：慢 SQL / Top SQL 留档回放，值等于期望值表 |
| `test_d_class_produces_every_providable_metric` | D 类 1 条：实例列表留档回放，值等于期望值表 |
| `test_b_class_produces_every_providable_metric` | B 类 82 条：跑**两轮**（第一轮 5 个差分指标只建基线），82 条模板码一条不落 |
| `test_all_ninety_eight_providable_metrics_are_produced_by_the_collectors` | **四个通道合起来**：98 个可提供指标全部产出（反向断言：不产出「不可提供」的指标） |

采集时刻统一用期望值表的推导时刻 `expected_values_non_b.json::now_ms = 1794500000000`
—— `gaussdb.Uptimestr` 这类「按采集时刻算」的指标才比得上。

### 结论（采集器层的真实覆盖）

- **98 个可提供指标中 97 个在采集器层产出了数据**；
- 剩下的 1 个是 `gaussdb.locks.accessexclusive[{#DBNAME}]` ——
  **留档里一条 `AccessExclusiveLock` 锁都没有**（期望值表里它的条目是空列表），
  「缺值不产出」是正确行为。已在测试里**显式写明**这个例外（不是静默放过）。

### RED → GREEN

```
RED（回退生产代码）：6 个用例全 FAILED
  FAILED test_a_class_produces_every_providable_metric
  FAILED test_c_class_produces_every_providable_metric
  FAILED test_d_class_produces_every_providable_metric
  FAILED test_b_class_produces_every_providable_metric
  FAILED test_all_ninety_eight_providable_metrics_are_produced_by_the_collectors
GREEN：6 passed
```

---

## 9. Minor（本轮一并处理的）

| 编号 | 修法 | 守卫 |
|---|---|---|
| **M-1** | `collectors/instance.py::collect` 52 行 → 抽出 `_collect_messages` | 既有 D 类用例仍绿（现在**全包 0 个**函数 > 50 行） |
| **M-2** | `tpops/alarms.py::fetch_all_alarm_records`：`if page.total_count > 0 and len(collected) >= page.total_count: break`；结束判据回到「不满一页」 | `test_pagination_continues_when_total_count_is_zero`（`total_count=0` + 150 条 → 翻两页取满） |
| **M-3** | 兜底路径（承载 master 本轮没回结果）：`master_view != 'identical'` 的规则**本轮不产出**（值与资源名不同源 = 数据错）；`identical` 的照常产出并记 WARNING | `command_center_assembly._rule_messages`（当前 40 条全是 `identical`，结构上闭合） |
| **M-4** | `CommandCenterCollector.from_config(client, mapping, command_center, config)` —— 页容量取 `config.cmd_page_size`（`CMD_PAGE_SIZE`） | `test_from_config_takes_the_page_size_from_the_config` |
| **M-5** | 结果回报 `page_size=self._page_size`（**缩小后**的值），不再是缩小前的 | `test_result_reports_the_shrunk_page_size`（100 → 50） |
| **M-10** | 「采集器不推进水位」改成**可证伪**的行为断言：① 按**类型**查（`isinstance(v, WatermarkStore)`，换属性名也绕不过）；② E 类收到水位后水位文件**逐字节不变**；③ 一整轮采集前后水位**状态**不变；④ B 类结果对象里没有水位字段 | `tests/integration/test_collectors_do_not_advance_watermarks.py`（4 个用例） |

---

## 10. 未修的条目及理由

| 编号 | 内容 | 理由 |
|---|---|---|
| **M-6** | 脱敏把双引号标识符里的数字也脱敏（`"col 1"` → `"col ?"`） | 不在本轮简报范围内；是**过脱敏**（安全方向），但会让客户看到的 SQL 失真 —— 建议 wave 4 前单独修 |
| **M-7** | 死代码（`tpops/instances.py::_as_dict`、`tpops/diagnostic.py::collection_window_ms`） | 不在本轮范围；改动会牵动别的模块，留给收尾 |
| **M-8** | C 类采集器二次取 `self._clock()`（与 `collect()` 的 `now_ms` 可能不一致）；C 类窗口写死在 `COLLECTION_WINDOWS` | 不在本轮范围（C 类的 `now_ms` 二次取值不影响值，只影响时间戳与窗口；且本轮不碰 `collectors/diagnostic.py`） |
| **M-9** | 「平台不认 offset」的守卫只看**相邻两页**（理论上存在交替页 A,B,A,B… 无限翻页） | 不在本轮范围；属防御性加固，可随 M-8 一并在收尾时改 |
| **M-11** | 5 个既有测试文件超 400 行 | 本轮**新增**文件而不是继续加长既有大文件（新增 3 个测试文件）；既有大文件未做拆分（拆分属纯整理，不阻塞） |
| **M-12** | 规格与实现口径不一致（历史告警增量的判定字段） | 审查已判定「实现更安全且必需」→ 应改**规格文字**（T-15），本轮不改 `changes/` 下任何文件 |
| **T-16** | 回归层补采集器级用例 | ✅ 本轮已做（`tests/regression/test_collector_coverage.py`） |
| **T-19** | 告警分页 `total_count` 的健壮性 | ✅ 本轮已做（M-2） |

**没有改动的文件**（按硬约束）：
`changes/tpops-kafka-collector/execution-contract.md`、`tasks.md`、
`data/mapping/*.json`、`data/merged_sql/*` —— **一行未动**。

---

## 11. 改动清单

### 生产代码

| 文件 | 改/增 | 内容 |
|---|---|---|
| `tpops_kafka_collector/mapping/loader.py` | 改 | `LoadedMapping` 补 `metric_rule()` / `alarm_definition()`；启动期校验拆出去（399 → 236 行） |
| `tpops_kafka_collector/mapping/table.py` | 改 | 抽出 `build_alarm_definition()`（唯一实现） |
| `tpops_kafka_collector/mapping/rules_deltas.py` | 改 | 基线支持**作用域**；新增 `ScopedCounterDeltaStore`、`scoped_dimension_key()` |
| `tpops_kafka_collector/mapping/validation.py` | **增** | 启动期四类校验（从 loader 拆出，226 行） |
| `tpops_kafka_collector/mapping/rules.py` / `mapping/__init__.py` | 改 | 导出 `ScopedCounterDeltaStore` |
| `tpops_kafka_collector/collectors/command_center.py` | 改 | `now_ms` 贯通、差分基线按节点隔离、`_master_nodes` 记 ERROR、`from_config`、回报实际页容量；拆到 344 行 |
| `tpops_kafka_collector/collectors/command_center_helpers.py` | **增** | 常量 + 纯函数（161 行） |
| `tpops_kafka_collector/collectors/command_center_assembly.py` | **增** | 归集与组装（151 行） |
| `tpops_kafka_collector/collectors/instance.py` | 改 | `collect` 拆出 `_collect_messages`（≤ 50 行） |
| `tpops_kafka_collector/tpops/alarms.py` | 改 | 分页 `total_count` 健壮性（M-2） |

### 测试

| 文件 | 改/增 | 内容 |
|---|---|---|
| `tests/integration/test_collector_alarm.py` | 改 | fixture 换成 `load_mapping()`（C-1）；+2 条生产映射对象守卫；+1 条 `total_count=0` 分页守卫 |
| `tests/integration/test_collector_command_center.py` | 改 | 值相等守卫重写（I-2）；+ 指标码覆盖守卫；+ 2 条 `_master_nodes` 日志守卫；+ `from_config` / 页容量回报守卫 |
| `tests/integration/test_collector_command_center_deltas.py` | **增** | C-2 / C-3 的 4 条守卫 |
| `tests/integration/test_collectors_do_not_advance_watermarks.py` | **增** | M-10 的 4 条可证伪断言 |
| `tests/regression/collector_support.py` | **增** | 采集器层回归的公共件（假客户端 / 夹具 / 比对辅助，256 行） |
| `tests/regression/test_collector_coverage.py` | **增** | I-4 的 A / B 类 + 「98 条总闸」（218 行） |
| `tests/regression/test_collector_coverage_cd.py` | **增** | I-4 的 C / D 类（112 行） |

---

## 12. 最终 RED → GREEN 汇总（一次跑完）

把 `mapping/loader.py`、`mapping/table.py`、`collectors/command_center.py`、
`tpops/alarms.py` 一起回退到修复前（其余保持修复态），跑本轮新增/改写的全部用例：

```
=== RED（生产代码回退）===
35 failed, 24 passed in 6.53s

FAILED tests/integration/test_collector_alarm.py::test_pagination_fetches_every_page
FAILED tests/integration/test_collector_alarm.py::test_collector_works_with_the_production_mapping_object   ← C-1
FAILED tests/integration/test_collector_alarm.py::test_production_mapping_object_finds_the_same_definitions ← C-1
FAILED tests/integration/test_collector_alarm.py::test_pagination_continues_when_total_count_is_zero       ← M-2
FAILED tests/integration/test_collector_command_center.py::test_master_nodes_failure_is_logged_as_error     ← I-3
FAILED tests/integration/test_collector_command_center.py::test_missing_master_nodes_capability_is_logged_as_error
FAILED tests/integration/test_collector_command_center.py::test_from_config_takes_the_page_size_from_the_config ← M-4
FAILED tests/integration/test_collector_command_center.py::test_result_reports_the_shrunk_page_size          ← M-5
FAILED tests/integration/test_collector_command_center_deltas.py::test_counter_delta_produces_a_rate_on_the_second_round ← C-2
FAILED tests/integration/test_collector_command_center_deltas.py::test_every_master_computes_its_own_rate   ← C-3
FAILED tests/integration/test_collector_command_center_deltas.py::test_counter_delta_rate_survives_a_restart_with_a_seeded_store
FAILED tests/integration/test_collector_command_center_deltas.py::test_counter_delta_rates_match_the_expected_values_plan
FAILED tests/integration/test_collectors_do_not_advance_watermarks.py::test_alarm_collector_reads_the_watermark_but_never_writes_it
FAILED tests/regression/test_collector_coverage.py::test_a_class_produces_every_providable_metric          ← I-4
FAILED tests/regression/test_collector_coverage.py::test_b_class_produces_every_providable_metric
FAILED tests/regression/test_collector_coverage.py::test_all_ninety_eight_providable_metrics_are_produced_by_the_collectors
FAILED tests/regression/test_collector_coverage_cd.py::test_c_class_produces_every_providable_metric
FAILED tests/regression/test_collector_coverage_cd.py::test_d_class_produces_every_providable_metric
（E 类整档 19 个既有用例也全红 —— fixture 换成生产映射对象后不再是假绿）← C-1

=== GREEN（修复恢复）===
1295 passed in 23.05s
```

---

## 13. 给 wave 3 的交接（**必读**）

1. **B 类采集器必须「启动时装配一次、每轮调 `collect()`」** ——
   差分基线的跨轮延续靠的是**采集器实例**（每轮 new 一个 ⇒ 速率永远算不出来）。
   要跨进程重启还能差分：用 `watermark.WatermarkStore.import_baselines_from(deltas)` /
   `export_baselines_into(deltas)`（**键形状没变**，水位文件不用迁移）。
2. **差分基线已按节点隔离** —— 装配时**不要**再自己按节点拆存储（会与
   `ScopedCounterDeltaStore` 打架）。
3. `CommandCenterCollector.from_config(...)` 已就绪（页容量取 `CMD_PAGE_SIZE`）。
4. **98 个可提供指标里 97 个在采集器层有数据**；`gaussdb.locks.accessexclusive[{#DBNAME}]`
   在留档里无值（真实环境有锁时会有）。
5. **3 个不可提供指标的「丢弃计数」仍未实现**（T-18，wave 3 每轮汇总日志）——
   本轮没做（不在简报范围）。
