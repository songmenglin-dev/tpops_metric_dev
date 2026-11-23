# wave-2b 修复轮次 1 —— 定向复审报告

- **波次**：`wave-2b-channels`（上一轮 verdict = **fail**：Critical 3 / Important 4 / Minor 12）
- **复审对象**：修复轮次 1 的提交 `e97c45d`（主要）+ `b10b706`（抽 `_fetch_four_shapes`）
- **工作树**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`
- **复审者**：独立复审者（**未采信实施方报告里的任何数字**，全部在本 worktree 上实测复现）
- **复审日期**：2026-11-23
- **复审方式**：
  1. 读 diff（`git diff 40309f5..b10b706 -- tpops_kafka_collector tests`）；
  2. **可证伪性实验**：用 pytest 插件在**运行期**把被修的行为改回修复前的形态
     （删掉 `LoadedMapping` 的两个方法 / 把 `now_ms` 强制成 0 / 把作用域去掉），
     看守卫测试会不会红；
  3. **独立重算**：期望值表的比对条数、差分速率、覆盖率、行数、函数长度全部自己算一遍。

---

## 1. 结论

> ## ✅ verdict = **pass**
>
> **Critical 0 / Important 0 / Minor 7**

| 级别 | 上一轮 | 本轮 | 摘要 |
|---|---|---|---|
| **Critical** | 3 | **0** | C-1 / C-2 / C-3 **全部真修好**，且每条都有**能让它失败**的守卫测试（已实测验证） |
| **Important** | 4 | **0** | I-1 / I-2 / I-3 / I-4 **全部真修好**，数值经独立重算核对无误 |
| **Minor** | 12 | **7** | 本轮新增 3 条（均为可维护性/交接风险），上一轮 12 条中 8 条已随本轮修复关闭，4 条仍未修（不在本轮范围） |

**为什么可以给 pass**：三条 Critical 全部是「**不报错、但数据没有/数据错**」的形态 ——
本轮不仅修了实现，而且**每一条都有一个能被证伪的守卫测试**（我逐条做了「把修复改回去」的实验，
守卫都真的变红）。这是上一轮报告 §11「没有守卫测试的修复不算修完」这条判据的正面回应。

**遗留的 Minor 7 条均不阻塞**：3 条新增 Minor 是可维护性/交接风险（无数据影响），
4 条是上一轮已明确划到 wave 3 / wave 4 的条目（见 §7.3）。

> ⚠️ **一处需要控制器知悉的口径**：合同完成标准第 2 条「**3 个不可提供指标无消息且丢弃计数递增**」
> 的**丢弃计数**部分**仍未实现**（全仓无任何丢弃计数机制），实施方已登记为 T-18 交 wave 3。
> 上一轮审查把它判为「⚠️ 部分满足」而**未**计入 Critical/Important，本轮维持同一口径。
> 若控制器认为「丢弃计数」必须在本波次闭环，请把本条的判定改为 Important 后重签。

---

## 2. 三条 Critical 的逐条核实

### C-1 【Critical】E 类采集器用生产映射对象必崩 —— 全部告警不产出

**判定：✅ 真修好。**

#### ① 生产对象上确实有这两个方法了吗？—— 有

`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev/tpops_kafka_collector/mapping/loader.py:104-117`

```python
def metric_rule(self, metric_code: str) -> Optional[MetricRule]:
    for rule in self.metric_rules:
        if rule.metric_code == metric_code:
            return rule
    return None

def alarm_definition(self, alarm_id: Any) -> Optional[AlarmDefinition]:
    return build_alarm_definition(self.alarm_rules, alarm_id)
```

`alarm_definition` 与 `MappingTable.alarm_definition` **共用同一份实现**
（`mapping/table.py:101` 新抽出的模块级函数 `build_alarm_definition`），
两处不再分叉 —— 符合「查表能力本就该在生产用的那个对象上」的修法意图。

#### ② E 类测试的 fixture 是不是真的换成了 `load_mapping()`？—— 是

`tests/integration/test_collector_alarm.py:145-152`

```python
@pytest.fixture(scope='module')
def mapping():
    """**生产装配用的映射对象**（``load_mapping()`` 的返回值）。..."""
    return load_mapping()
```

旧 fixture `MappingTable(bundle.field_map, bundle.metric_rules, bundle.alarm_rules, {})`
**已删除**（diff 里 `from ...mapping.table import MappingTable` 那一行被删掉）。
⇒ 整档 19 个既有用例从「假绿」变成真守卫。

#### ③ 有没有一个测试能让它失败？—— **有，实测能红**

我把 `LoadedMapping.alarm_definition` 与 `LoadedMapping.metric_rule` 在运行期删掉
（pytest 插件 `p_c1`，不改任何被审文件），再跑 E 类整档：

```
$ PYTHONPATH=/tmp/fakep conda run -n tpops python -m pytest -p p_c1 \
      tests/integration/test_collector_alarm.py -q
tpops_kafka_collector/collectors/alarm.py:253: AttributeError
FAILED tests/integration/test_collector_alarm.py::test_pagination_fetches_every_page
FAILED tests/integration/test_collector_alarm.py::test_pagination_stops_when_total_reached
FAILED tests/integration/test_collector_alarm.py::test_all_four_combinations_are_queried
FAILED tests/integration/test_collector_alarm.py::test_details_are_merged_by_alarm_id_and_instance
FAILED tests/integration/test_collector_alarm.py::test_details_of_other_instance_are_not_merged
FAILED tests/integration/test_collector_alarm.py::test_convergence_request_carries_no_platform_filters
FAILED tests/integration/test_collector_alarm.py::test_convergence_rows_are_filtered_locally
FAILED tests/integration/test_collector_alarm.py::test_node_is_backfilled_from_details
FAILED tests/integration/test_collector_alarm.py::test_multi_node_alarm_falls_back_to_first_master_cn
FAILED tests/integration/test_collector_alarm.py::test_alarm_without_details_falls_back_to_first_master_cn
FAILED tests/integration/test_collector_alarm.py::test_details_is_always_a_list_even_when_empty
FAILED tests/integration/test_collector_alarm.py::test_details_is_always_a_list_when_present
FAILED tests/integration/test_collector_alarm.py::test_only_alarms_newer_than_watermark_are_returned
FAILED tests/integration/test_collector_alarm.py::test_watermark_none_returns_everything
FAILED tests/integration/test_collector_alarm.py::test_history_gap_beyond_retention_logs_warning
FAILED tests/integration/test_collector_alarm.py::test_recent_history_logs_no_retention_warning
FAILED tests/integration/test_collector_alarm.py::test_idempotency_key_uses_platform_raw_status
FAILED tests/integration/test_collector_alarm.py::test_replay_archived_alarm_samples
FAILED tests/integration/test_collector_alarm.py::test_collector_works_with_the_production_mapping_object
FAILED tests/integration/test_collector_alarm.py::test_production_mapping_object_finds_the_same_definitions
FAILED tests/integration/test_collector_alarm.py::test_pagination_continues_when_total_count_is_zero
21 failed, 1 passed in 1.63s
```

**21 个用例红**（含新增的 2 条生产映射对象守卫）。
⇒ C-1 有真实的、覆盖整档 E 类行为的守卫，不是「断言形同虚设」。

**影响面**：E 类通道在生产装配下不再崩。✅

---

### C-2 【Critical】B 类 5 个 `counter_delta` 客户指标永不产出

**判定：✅ 真修好（实测第二轮产出 135 条）。**

#### ① `now_ms` 是否从 `collect()` 取一次并贯通？

`collectors/command_center.py:160-179`：

```python
targets = tuple(rules) if rules is not None else self._rules
tier_seconds, tier_label = tier_of(targets)
# **同一轮内所有页共用同一个采集时刻** —— 差分基线的时间轴必须一致
now_ms = int(self._clock())                      # ← 取一次
bodies, pages = self._fetch_pages(instance_id, self._page_size, now_ms)
...
merged_by_node = self._merge(bodies, now_ms)     # ← 贯通
```

- `_fetch_pages(..., now_ms)` → `decode_merged(body, self._views, now_ms)`（`:221-223`）；
- `_merge(bodies, now_ms)` → `merge_pages(pages, self._views, now_ms)`（`:300`）；
- 缩小页容量的重试**沿用同一个 `now_ms`**（`_handle_failure(..., now_ms)` → `self._fetch_pages(instance_id, smaller, now_ms)`，`:257-259`）。

`merge_pages` 用 `now_ms` 重建 `RuleContext`（`mapping/decode.py:170` `_build_merged_contexts(rows_by_key, now_ms)`），
`decode_merged` 也用同一个值 —— **两处都传，闭环成立**。

#### ② 同一轮的所有页是否用同一个 now_ms？—— 是

`now_ms` 是 `collect()` 的局部变量，`_fetch_pages` 的 `while True` 分页循环**不再取时钟**，
每一页都用同一个入参。**实测**：把 `decode_merged` 与 `merge_pages` 都强制成 `now_ms=0` 后，
日志刷屏 `采集时刻未推进，丢弃本轮差分：... now=0 last=0`（每轮每个维度一条），
证明基线时间轴确实由这一个值驱动。

#### ③ 实测第二轮是否真的产出 135 条？—— **是**

我**不用**测试文件里的断言，自己跑了一遍采集器（两轮、三个 master、增量 6000/3000/9000、
间隔 60 秒）：

```
$ conda run -n tpops python /tmp/verify_delta.py
R1 total=1700 delta=0
R2 total=1835 delta=135
  node 3881c04403394ee1b36a2bf873e69007no14 count 45 sample value 100 code GAUSSDB.[lz]_fetch_records_per_second dims {'DATNAME': 'lz'}
  node 40abc563810e462a97b065e718189a59no14 count 45 sample value  50 code GAUSSDB.[lz]_fetch_records_per_second dims {'DATNAME': 'lz'}
  node 5a3cd027ad5c4f9bbb67d728c6d279c0no14 count 45 sample value 150 code GAUSSDB.[lz]_fetch_records_per_second dims {'DATNAME': 'lz'}
```

- 第一轮 **0 条**差分消息（只建基线 —— 差分的定义，不是 0、不是假数据）；
- 第二轮 **135 条**（3 master × 5 指标模板 × 9 个 `DATNAME` 维度）；
- 三个 master 的速率 **100 / 50 / 150**（= 各自的增量 6000/3000/9000 ÷ 60 s）。

#### 守卫可证伪性 —— **实测能红**

```
$ PYTHONPATH=/tmp/fakep conda run -n tpops python -m pytest -p p_c2 \
      tests/integration/test_collector_command_center_deltas.py -q
FAILED ...::test_counter_delta_produces_a_rate_on_the_second_round
FAILED ...::test_every_master_computes_its_own_rate
FAILED ...::test_counter_delta_rate_survives_a_restart_with_a_seeded_store
FAILED ...::test_counter_delta_rates_match_the_expected_values_plan
4 failed, 1 passed in 0.69s
```

（插件把 `decode_merged` / `merge_pages` 的 `now_ms` 强制回 0。）

**影响面**：5 个 `*_records_per_second` 客户指标每轮产出 135 条，不再静默缺席。✅

---

### C-3 【Critical】差分基线跨 master 共享 → 互相污染

**判定：✅ 真修好。**

#### ① `ScopedCounterDeltaStore` 是否真的按节点隔离？—— 是

`mapping/rules_deltas.py:101-133`：视图**无状态**，作用域并进维度键：

```python
def scoped_dimension_key(scope: Any, key: str) -> str:
    text = str(scope or '')
    if not text:
        return key
    return f'{text}:{key}'
```

`CounterDeltaStore.rate(..., scope='')` 的键 =
`(metric_code, scoped_dimension_key(scope, dimension_key(dimensions)))`。

采集器侧（`collectors/command_center.py:304-316`）：

```python
def _delta_store(self, node_id: Any) -> ScopedCounterDeltaStore:
    key = str(node_id)
    store = self._scoped_deltas.get(key)
    if store is None:
        store = ScopedCounterDeltaStore(self._deltas, key)
        self._scoped_deltas[key] = store
    return store
```

**三个 master 三个视图，底层同一个存储** → 基线跨轮延续 ✓、按节点隔离 ✓、可整体落盘 ✓，三个要求同时成立。

#### ② 基线键的形状是否仍是二元组？—— **是，`watermark.py` 一行未改**

`git diff 40309f5..b10b706 --stat` 里**没有** `tpops_kafka_collector/watermark.py`。
`mapping/rules_deltas.py` 的键仍是 `Dict[Tuple[str, str], Tuple[float, int]]`，
作用域是并进**维度键字符串**的（`nodeid:DATNAME=lz`），不是加第三个元组元素。

**持久化 / 导入导出无影响**（聚焦核查：`watermark.py:178-192, 293-304`）：
`import_baselines_from` / `export_baselines_into` 直接读写底层 `_baselines` 容器，
键原样搬运 ⇒ 带作用域前缀的键**原样往返**，文件格式不变。
实测：`tests/unit/test_watermark.py` 的 4 条基线往返用例全绿（未改动）。

> ⚠️ **但有一个交接风险（已登记为 Minor M-N7）**：`WatermarkStore.put_baseline` /
> `get_baseline` / `forget_baseline` 用的是**不带作用域**的维度键。
> 全仓**只有测试**在用这三个方法（生产无调用方），但 wave 3 若拿它们去读写差分基线，
> 键会与 `ScopedCounterDeltaStore` 用的键**对不上**。wave 3 只能走
> `import_baselines_from` / `export_baselines_into`。

#### ③ 跨轮延续还成立吗？—— 是

`test_counter_delta_rate_survives_a_restart_with_a_seeded_store` 跑三轮，
第 2、3 轮都有速率。**实测**：我在自己的脚本里第 2 轮拿到 135 条、第 3 轮同样有值。
采集器持有 `self._deltas`（没注入时自建），采集器实例跨轮存活 → 基线跨轮延续。
模块 docstring（`command_center.py:22-29`）与 `collect()` docstring 写明了
「采集器实例必须跨轮复用」的硬要求。

#### ④ 三个 master 是不是各拿各的值、不串？—— **是**

实测见 C-2 的输出：**100 / 50 / 150**，与各自增量 ÷ 60 s 完全一致，互不串。

**守卫可证伪性 —— 实测能红**（插件把 `ScopedCounterDeltaStore.rate` 的 `scope` 强制成 `''`
＝ 退回修复前的「共用一个存储」）：

```
$ PYTHONPATH=/tmp/fakep conda run -n tpops python -m pytest -p p_c3 \
      tests/integration/test_collector_command_center_deltas.py -q
FAILED ...::test_counter_delta_produces_a_rate_on_the_second_round
FAILED ...::test_every_master_computes_its_own_rate
FAILED ...::test_counter_delta_rates_match_the_expected_values_plan
3 failed, 2 passed in 0.66s
```

（失败时的日志正是上一轮描述的形态：`scope= now=... last=...`「采集时刻未推进」。）

**影响面**：3 个 master 的差分基线互不覆盖，不再有 2/3 恒为 `None`、也不再算跨节点速率。✅

---

## 3. 四条 Important 的逐条核实

### I-1 【Important】`collectors/command_center.py` 越 400 行 / 函数越 50 行

**判定：✅ 真修好（且实施方自查此前不准确，现已修正）。**

**文件行数**（`wc -l`，全生产包）：

| 文件 | 行数 | 判定 |
|---|---|---|
| `collectors/monitor.py` | 382 | ✅ |
| `collectors/alarm.py` | 375 | ✅ |
| `collectors/command_center.py` | **344**（原 463） | ✅ |
| `tpops/command_center.py` | 399 | ✅（余量 1 行） |
| `tpops/metrics.py` | 396 | ✅ |
| `collectors/command_center_helpers.py`（新） | 161 | ✅ |
| `collectors/command_center_assembly.py`（新） | 151 | ✅ |
| `mapping/loader.py` | 236（原 399） | ✅ |
| `mapping/validation.py`（新） | 226 | ✅ |

**生产包内 ≥ 400 行的文件：0 个**（`wc -l` 全量扫描，逐个核对）。

**函数 > 50 行：0 个**（我用自己的 AST 脚本重测，Python 3.7 无 `end_lineno`，
按函数体内所有 `stmt` 的最大 `lineno` 计算）：

```
$ conda run -n tpops python /tmp/flen.py
49 tpops_kafka_collector/collectors/monitor.py _messages_of_rule 166
48 tpops_kafka_collector/collectors/command_center.py _fetch_pages 193
46 tpops_kafka_collector/tpops/alarms.py fetch_all_alarm_records 142
46 tpops_kafka_collector/mapping/decode.py decode 42
46 tpops_kafka_collector/collectors/alarm.py collect_alarms 79
45 tpops_kafka_collector/retry.py retry_call 136
...
```

**最长 49 行**（`monitor.py::_messages_of_rule`）。
`collect_alarms` 现在 **46 行**（控制器在 `b10b706` 之前查到的是 51 行，已被 `b10b706` 修掉）。
`instance.py::collect` 顺带拆成 `collect` + `_collect_messages`（M-1 关闭）。

**拆分没有丢守卫**（聚焦核查）：我把旧的 `command_center.py`（`40309f5` 版本）与新的
三个文件做了**逐函数文本比对**（归一化空白、忽略 `_` 前缀与类型注释写法）：

```
command_center_rules  DIFF   ← 仅 `rules = []  # type: List[Any]` → `rules: List[Any] = []`
merged_views          same
has_more              same
approx_size_bytes     same
host_node_id          same
page_signature        same
same_as_previous      same
is_output_limit_error same
tier_of               DIFF   ← 仅 `_tier_of` → `tier_of`（命名）+ 缩进归一化
```

⇒ **两处 DIFF 都只是改名/类型注释写法，没有任何逻辑差异**；
「`if not outcome.ok` → 记 ERROR 并放弃本轮」这道闸仍在 `command_center.py:212-215`。

---

### I-2 【Important】值相等守卫只覆盖 47/82

**判定：✅ 真修好（精确值 1700 经我独立重算，可复现）。**

#### ① 精确值是怎么算出来的？—— 我自己重算了一遍，**与实施方一致**

我没有采信实施方的 `EXPECTED_COMPARISONS = 1700`，而是按守卫的判定口径
（模板码 × 节点 × 维度条目；`node_scoped=false` 只算承载资源名的那个 master；
期望值为 `None` 的维度不计）**从期望值表独立重算**：

```
$ conda run -n tpops python /tmp/verify_i2.py
host master node: gauss-6374_gaussdbv5_hcs2-0_0 3881c04403394ee1b36a2bf873e69007no14
B-class rule count: 82
independent comparable = 1700
codes with expected values = 76
total template codes in table = 82
table codes not in B-class rules: []
```

**独立重算 = 1700**，与守卫里的 `EXPECTED_COMPARISONS` 完全一致
⇒ 这个数字是**从期望值表推导出来的**，不是照抄实施方的数，也不是随手拍的。

#### ② 有没有哪条指标仍然没被比到？—— **6 条，全部有据可依，且有别的守卫兜住**

| 指标码 | 原因 | 谁在守 |
|---|---|---|
| `GAUSSDB.[{#DATNAME}]_delete_records_per_second` | 首轮只建基线（差分的定义） | `test_collector_command_center_deltas.py` 第二轮用例 |
| `GAUSSDB.[{#DATNAME}]_fetch_records_per_second` | 同上 | 同上 |
| `GAUSSDB.[{#DATNAME}]_insert_records_per_second` | 同上 | 同上 |
| `GAUSSDB.[{#DATNAME}]_return_records_per_second` | 同上 | 同上 |
| `GAUSSDB.[{#DATNAME}]_update_records_per_second` | 同上 | 同上 |
| `gaussdb.locks.accessexclusive[{#DBNAME}]` | **留档里一条 `AccessExclusiveLock` 锁都没有** | 显式断言（见下） |

我**独立核实**了最后一条（不是采信报告）：直接读实测留档的 `locks` 视图：

```
$ conda run -n tpops python /tmp/verify_locks.py
gauss-6374_gaussdbv5_hcs2-0_2 locks rows 1 kinds ['relation']
gauss-6374_gaussdbv5_hcs2-0_1 locks rows 1 kinds ['relation']
gauss-6374_gaussdbv5_hcs2-0_0 locks rows 1 kinds ['relation']
```

⇒ 只有 `relation` 锁，**没有 `AccessExclusiveLock`** ⇒ 「缺值不产出假数据」是正确行为，
不是采集器漏产。

**关键改进**：上一轮那 35 条里，**29 条是「采集器其实产出了、但旧守卫键不匹配被 `continue` 跳过」**
—— 本轮改成按**替换后的码**（`substitute_placeholders(模板码, 维度)`）反查，
这 29 条**全部比上了**（被比到的指标码 47 → **76**）。
`continue` 静默跳过的指标码数：**35 → 0**。

**「键不匹配」是不是换了个姿势被跳过？—— 不是**：守卫现在还加了**反向断言**
「期望值为 `None` 的维度**不许**产出消息」（缺值不产出假数据），
以及 `compared_codes == _codes_with_expected_values(expected_table)`（76 条一条不漏）。

#### ③ 断言能不能被绕过？—— **不能（比少了会失败）**

- 旧断言 `checked > 0` → 新的 `checked == comparable` **且** `checked == 1700`。
- 「把 `checked` 少算几条」⇒ `checked` 掉到 1700 以下 ⇒ **`checked == 1700` 失败** ✅
- 「把循环范围缩小（少比几个指标码）」⇒ 同上失败 ✅
- 「`comparable` 也一起少算」⇒ `checked == comparable` 仍成立，但 `checked == 1700` 失败 ✅
  （两个断言是**独立**的，`comparable` 兜不住 `1700` 这条）
- 「把 `EXPECTED_COMPARISONS` 改成别的数」⇒ 那是**改守卫本身**，不是绕过；
  但因为 1700 可从期望值表独立重算（我已复现），这种改动会被 review 抓到。

#### 额外的一条守卫

`test_every_b_class_rule_is_covered_by_the_produced_messages`：82 条模板码**一条不落**，
差的那 6 条必须**恰好**等于写死的 `NO_VALUE_CODES`（多一条少一条都红）。

**判定**：I-2 已实质关闭。✅
（**遗留 Minor M-N5**：`EXPECTED_COMPARISONS = 1700` 是硬编码魔数，
可维护性上建议在测试里从期望值表**推导**出来，避免期望值表更新后忘了同步。）

---

### I-3 【Important】`_master_nodes` 静默吞异常

**判定：✅ 真修好（两个分支都记 ERROR）。**

`collectors/command_center.py:322-344`：

```python
getter = getattr(self._cc, 'master_nodes', None)
if getter is None:
    LOGGER.error('命令中心客户端没有 master_nodes（实例 %s）—— 节点身份换不出来，资源名会指错节点', instance_id)
    return ()
try:
    return tuple(getter(instance_id))
except Exception as exc:
    LOGGER.error('命令中心节点清单取不到（实例 %s）：%s —— 节点身份换不出来，资源名会指错节点', instance_id, exc)
    return ()
```

比上一轮要求的还多做了一步：**「客户端没有该能力」（装配错了）也记 ERROR**，不再静默返回 `()`。

**守卫**：`test_master_nodes_failure_is_logged_as_error`（异常路径，断言日志含
`节点清单取不到` **与异常文本**）与 `test_missing_master_nodes_capability_is_logged_as_error`
（能力缺失路径，断言含 `没有 master_nodes`）。两条都在 `caplog.at_level('ERROR')` 下断言
**具体文案**，不是「有日志就行」。

**判定**：I-3 关闭。✅

---

### I-4 【Important】「98 个可提供指标全部产出数据」在采集器层不成立

**判定：✅ 实质修好（97/98，缺的 1 条有据可依）。**

#### ① 它真的**走过采集器**吗？—— 是（不是只调映射层）

`tests/regression/test_collector_coverage.py` + `test_collector_coverage_cd.py`
用的是**真实的采集器类**：

- A 类：`MonitorCollector(...).collect()`（`collector_support.py:230-248`）
- B 类：`CommandCenterCollector(...).collect()`（**跑两轮**）
- C 类：`DiagnosticCollector(...).collect(...)`
- D 类：`InstanceCollector(...).collect(...)`

输入是**实测留档回放**（`data/metric_response/*.json` + `data/merged_sql/sql_merge_probe_result.json`），
假客户端只负责把留档响应体回放出来 —— 走的是真实解码 / 取值 / 组装路径。

#### ② 「98 个可提供指标全部产出」真的成立吗？—— **97 个成立，1 个有据可依**

```
assert len(providable_codes) == 98
missing = sorted(providable_codes - produced)
assert missing == ['gaussdb.locks.accessexclusive[{#DBNAME}]'], missing
assert len(produced) == 97, len(produced)
extra = sorted(produced - providable_codes)
assert not extra, '产出了客户指标库里不是「可提供」的指标：%s' % extra
```

- `providable_codes` 来自 `mapping.customer_metrics` 里 `status == '可提供'`（**独立于规则表**），
  实测 **98** 条（82 B + 11 A + 4 C + 1 D）。
- 缺的那 1 条就是上面核实过的 `accessexclusive`（留档里无 `AccessExclusiveLock` 锁），
  **「缺值不产出假数据」是正确行为**，且是**显式写死的期望**（不是静默放过）。
- `extra` 为空 ⇒ **反向断言也在**：3 个不可提供指标（`enabled=false`）确实**一条消息都没产出**。

**值的正确性**（不只是「有消息」）：
- A 类逐条比对期望值表的**值**（`canon(matched['value']) == canon(want)`）；
- C 类 4 条、D 类 1 条逐条比对**值**；
- B 类逐条比对**值**（在 `tests/integration/test_collector_command_center.py` 里，1700 条）。

我**独立核实**了 C/D 两档的期望表条目数（16 条全部**非空**，所以「每条指标码都必须产出至少一条消息」
这个断言不是恒真的）：

```
$ conda run -n tpops python /tmp/verify_cd.py
Counter({'monitor_metrics': 11, 'diagnostic': 4, 'instance': 1}) total 16
-- diagnostic 4
    GAUSSDB.DML_slow_queries expected items= 1
    GAUSSDB.Select_slow_queries expected items= 1
    GAUSSDB.Total_slow_queries expected items= 1
    gaussdb.Top10sql expected items= 1
-- instance 1
    gaussdb.server_version expected items= 1
```

#### ③ 它的断言会不会是恒真的？—— **不会，但有一个盲区（见 Minor M-N4）**

**不会恒真的证据**：把 C-2 的形态（`now_ms` 强制 0）打回去，回归层真的红：

```
$ PYTHONPATH=/tmp/fakep conda run -n tpops python -m pytest -p p_c2 tests/regression -q
FAILED tests/regression/test_collector_coverage.py::test_b_class_produces_every_providable_metric
FAILED tests/regression/test_collector_coverage.py::test_all_ninety_eight_providable_metrics_are_produced_by_the_collectors
2 failed, 228 passed in 3.13s
```

**盲区（M-N4）**：把 C-3 的形态（去掉作用域）打回去，**回归层全绿**：

```
$ PYTHONPATH=/tmp/fakep conda run -n tpops python -m pytest -p p_c3 tests/regression -q
230 passed in 2.91s
```

⇒ 回归层的 B 类用例只断言「**产出的指标码集合**」，**不比对各 master 的速率**，
所以跨 master 污染它抓不住。C-3 由 `tests/integration/test_collector_command_center_deltas.py` 守住
（已实测能红），**不是没有守卫**，但「回归层能守住全部 Critical」这个更强的性质不成立。
**建议**：回归层的 B 类用例顺带断言「三个 master 各自的速率」，
让 wave 4 的门禁只跑 `tests/regression` 时也能拦住 C-3 这类形态。

**判定**：I-4 关闭（97/98 + 有据可依的 1 条 + 反向断言 + 值的比对）。✅

---

## 4. 两个已知疑点的结论

### 疑点 1：`collectors/alarm.py::_fetch_four_shapes`（提交 `b10b706`）—— 重构有没有改语义？

**判定：✅ 没有改语义。合同口径「告警收敛形态下不依赖平台侧 `alarm_id` / `instance_id` 过滤」仍然成立。**

重构前后逐字对照（`git diff 40309f5..b10b706 -- collectors/alarm.py`）：

| | 修复前（内联） | 修复后（`_fetch_four_shapes`） |
|---|---|---|
| 活动·收敛 | `fetch_all_alarm_records(client, True, True, limit)` | `fetch(True, True, False)` → `instance_id=None` ✅ |
| 活动·明细 | `fetch_all_alarm_records(client, True, False, limit, instance_id=instance_id)` | `fetch(True, False, True)` → `instance_id=instance_id` ✅ |
| 历史·收敛 | `fetch_all_alarm_records(client, False, True, limit)` | `fetch(False, True, False)` → `instance_id=None` ✅ |
| 历史·明细 | `fetch_all_alarm_records(client, False, False, limit, instance_id=instance_id)` | `fetch(False, False, True)` → `instance_id=instance_id` ✅ |

`scoped` 标志的取值（`False/True/False/True`）与原来的**「有没有传 `instance_id`」完全一一对应**；
`instance_id` 本身为 `None` 时两种写法等价。**收敛形态仍然不传 `instance_id`**。

**旁证**：`test_convergence_request_carries_no_platform_filters`（断言收敛请求体里没有平台侧过滤）
在重构后仍绿；整档 22 个 E 类用例全绿。

**函数长度**：`collect_alarms` 由 51 行降到 **46 行** ✅（目的达成，语义未变）。

### 疑点 2：实施方自查「全包 0 个函数 > 50 行」是否准确？还有没有别的**不准确的断言**？

**判定：实施方报告里有 3 处不准确的断言（均已在本轮/提交 `b10b706` 中修正或只是统计口径问题）。**

| # | 实施方报告的断言 | 复审核实 | 结论 |
|---|---|---|---|
| 1 | 「生产包内 > 50 行的函数：**0 个**」 | 报告提交时（`e97c45d`）`collectors/alarm.py::collect_alarms` 是 **51 行**（控制器已独立查到）→ **报告的断言当时不成立** | **`b10b706` 已修正**；现在实测最长 **49 行**，断言成立 ✅ |
| 2 | 「本轮改动：生产 **7 改 4 增**」 | 实测 `git diff 40309f5..b10b706 --stat -- tpops_kafka_collector`：**9 改 3 增**（改：loader / table / rules / rules_deltas / __init__ / command_center / instance / alarms / collectors-alarm；增：command_center_assembly / command_center_helpers / mapping-validation） | **统计口径写错了**，不影响代码正确性 |
| 3 | 「修复前基线 **1264** passed（+31 个用例）」 | 上一轮审查在同一基线量到的是 **1262** passed；差 2 条来自分支上中间的提交 | 数字对不上，**但不影响结论**（本轮实测 1295 = 1295 ✅） |

**其余数字我逐条独立核对，全部属实**：

| 实施方报告的断言 | 我的实测 | 结论 |
|---|---|---|
| `pytest -q` → **1295 passed** | 1295 passed in 23.24s | ✅ |
| `ruff check tpops_kafka_collector/` → All checks passed | All checks passed! | ✅ |
| `pytest tests/test_py37_compat.py -q` → **120 passed** | 120 passed in 0.95s | ✅ |
| `command_center.py` 344 / helpers 161 / assembly 151 | 344 / 161 / 151 | ✅ |
| `loader.py` 236 / `validation.py` 226 | 236 / 226 | ✅ |
| C-2 第二轮 **135 条** | 135 条（3×45） | ✅ |
| C-3 各 master **100 / 50 / 150** | 100 / 50 / 150 | ✅ |
| I-2 `checked` **1700**、被比指标码 **76** | 独立重算 1700 / 76 | ✅ |
| 生产包内 **0 个**函数 > 50 行 | 最长 49 行 | ✅ |
| 生产包内 **0 个**文件 ≥ 400 行 | 全部 < 400 | ✅ |

---

## 5. 新引入问题的检查结果

### 5.1 拆出的三个模块：职责划分 / 循环依赖 / 守卫搬家

| 检查 | 结果 |
|---|---|
| **职责划分** | `command_center_helpers.py` = 常量 + **纯函数**（不碰 IO、不碰状态）；`command_center_assembly.py` = **归集与组装**（多 master 结果 → 消息）；`command_center.py` = 采集器本体（装配 / 取数 / 分页 / 重试 / 解码）。三者职责清晰、无重叠 ✅ |
| **循环依赖** | 实测导入全部 9 个相关模块**全部成功**（无 import 环）：`mapping.loader → mapping.validation → mapping.table`（单向）；`collectors.command_center → command_center_assembly → command_center_helpers`（单向） ✅ |
| **守卫搬家是否导致原守卫失效** | **逐函数文本比对**（§3 I-1）：9 个被搬走的辅助函数**逻辑零差异**；「`if not outcome.ok` → ERROR」这道闸仍在原处 ✅ |
| **新增依赖** | 无（`command_center_helpers.py` 只用标准库 `hashlib` + `typing`） ✅ |

### 5.2 `mapping/validation.py` 是新文件 —— 它做了什么？启动期校验的行为有没有变？

**做了什么**：把 `loader.py` 里的**四类启动期校验**（`check_source_columns` /
`check_providable_coverage` / `check_rule_entries` / `check_placeholder_bindings`）
整体搬到新模块；`loader.py` 只管「把 JSON 读进来」，并**原样导出**那四个函数名
（`from .validation import ...`，`__all__` 不变）⇒ **外部 import 路径不变**。

**行为有没有变？—— 没有。** 我把旧 `loader.py`（`40309f5`）里的四个函数与新
`validation.py` 里的同名函数做了**逐函数 diff**：

```
check_source_columns         DIFFERENT  ← 仅前向引用 'ObservedColumns' 字符串注解
check_providable_coverage    DIFFERENT  ← 仅前向引用 'CustomerMetric' 字符串注解
check_rule_entries           DIFFERENT  ← 同上
check_placeholder_bindings   DIFFERENT  ← 同上（其余正文逐字相同）
```

差异**只有类型注解的写法**（改成字符串前向引用以避免循环导入），
**校验逻辑逐字未变**。

**「缺值不产出假数据」「不静默吞异常」是否还成立？—— 成立**：

- 校验不通过仍**抛 `MappingError`**（不可重试），`validate_mapping` 的行为一字未改；
- `mapping/validation.py` 用 `TYPE_CHECKING` 守卫导入 `loader`（避免循环），
  **运行期不导入** ⇒ 循环依赖不存在，异常链不变；
- 四个 `check_*` 全部**返回问题清单**（不抛异常、不静默），由 `validate_mapping` 汇总后一次性抛出
  —— 与修复前完全一致。

> ⚠️ **新引入 Minor M-N1**：`mapping/loader.py` 拆完后**漏掉了 `List` 的 import**
> （旧文件第 29 行有 `List`，新文件的 `from typing import ...` 里没有），
> 但文件里还留着两处 `# type: List[str]` / `# type: List[CustomerMetric]` 类型注释。
> Python 3.7 运行期**不求值**类型注释 ⇒ **不影响运行、不影响测试**；
> 但 mypy / pyright 会报 `Name "List" is not defined`。

### 5.3 `rules_deltas.py` 的基线键加了 `scope` —— 对 `watermark.py` 的持久化 / 导入导出有没有影响？

**判定：✅ 没有影响（键的形状与文件格式都没变）。**

- `watermark.py` **一行未改**（`git diff --stat` 里没有这个文件）；
- 基线键仍是**二元组** `(metric_code, dimension_key)`，作用域是**并进维度键字符串**
  （`nodeid:DATNAME=lz`），不是加第三个元素；
- `import_baselines_from` / `export_baselines_into` 直接搬 `_baselines` 容器的键
  ⇒ 带前缀的键**原样往返**；`tests/unit/test_watermark.py` 的基线往返用例全绿。

> ⚠️ **交接风险（Minor M-N7）**：`WatermarkStore.put_baseline` / `get_baseline` /
> `forget_baseline` 用的是**不带作用域**的维度键；全仓**只有测试**在用它们。
> wave 3 若用这三个方法读写差分基线，键会与 `ScopedCounterDeltaStore` 的键**对不上**。
> wave 3 只能走 `import_baselines_from` / `export_baselines_into`。

### 5.4 有没有测试为了「凑覆盖率」而写得没有行为意义？

**判定：没有发现「凑覆盖率」的空测试。** 逐条看了本轮新增/改写的 7 个测试文件：

| 测试 | 行为意义 |
|---|---|
| `test_collector_command_center_deltas.py`（5 条） | 全部是**行为断言**：第 1 轮不出值、第 2 轮出值、三个 master 各 100/50/150、跨轮延续、与期望值表的差分计划一致。实测**都能让修复前的形态变红** ✅ |
| `test_collector_coverage*.py`（5 条） | 走真实采集器，比对**值**（不只是「有消息」），有**反向断言**（不多产出不可提供指标）。实测 C-2 形态能让它红 ✅ |
| `test_collectors_do_not_advance_watermarks.py`（4 条） | M-10 的**可证伪**化：按**类型**查水位存储（换属性名绕不过）、水位文件**逐字节**不变、整轮前后水位**状态**不变、结果对象字段集精确断言 ✅ |
| `test_collector_alarm.py` 新增 3 条 | 生产映射对象能跑 + 两份实现查表结果**逐条相等** + `total_count=0` 时靠「不满一页」停下 ✅ |
| `test_collector_command_center.py` 改写/新增 6 条 | 1700 条精确值相等 + 82 条模板码一条不落 + 两个 `_master_nodes` 日志守卫 + `from_config` + 页容量回报 ✅ |

**唯一偏「仪式性」的是** `test_command_center_result_carries_no_watermark_state` 里的
`assert len(NODE_NAMES) == 3` —— 这一行与该测试的主题无关，是**搭车断言**（Minor，登记为 M-N6）。

---

## 6. 合同完成标准逐条判定表

### 6.1 Wave 2b 完成标准（`execution-contract.md` §Execution Waves）

| # | 合同完成标准 | 判定 | 依据 |
|---|---|---|---|
| 1 | `pytest tests/regression -q` —— **98 个可提供指标全部产出数据**（D1） | ✅ **满足**（97/98 + 1 条有据可依） | `pytest tests/regression -q` → **230 passed**。新增的采集器层回归**真走过采集器**，实测 A 11 / B 82 / C 4 / D 1 = 98 条可提供指标中 **97 条产出消息且值等于期望值表**；缺的 1 条是 `gaussdb.locks.accessexclusive[{#DBNAME}]`（**留档里无 `AccessExclusiveLock` 锁**，我独立核实过）—— 「缺值不产出假数据」是正确行为。反向断言「不产出不可提供的指标」也在。上一轮的 C-2（5 个差分指标 0 条）已修 → **本条从「不成立」变为「成立」** |
| 2 | **3 个不可提供指标无消息**且**丢弃计数递增**（D2） | ⚠️ **部分满足**（维持上一轮口径） | 「无消息」✅：回归层 `extra` 为空的反向断言守住（3 条 `enabled=false`）。**「丢弃计数递增」❌ 仍未实现**（全仓无丢弃计数机制）→ 已登记为 **T-18**，wave 3 每轮汇总日志。**上一轮未把它计入 Critical/Important，本轮维持同一判定**（若控制器要求本波次闭环，见 §1 末尾的口径说明） |
| 3 | **多 master 归集**：节点级视图每 master 一份、实例级视图只一份、**无 3 倍累加** | ✅ **满足** | `node_scoped=true` 每 master 一份（`build_messages` 的 `targets`）、`false` 只落承载 master（`[(host_id, shared)]`）；`merge_pages` 只对 `paged=true` 的视图 `extend`；M-3 的兜底路径现在**本轮不产出**（`master_view != 'identical'` 时）—— 结构上闭合 |
| 4 | **分页**：两页拼接后行数正确；空页不抛异常 | ✅ **满足** | 未改动；`has_more` 只看可增长视图、`merge_pages` 只拼可增长视图；空列解成 `[]` 不抛 |
| 5 | **节点清单去重后只向唯一 `node_id` 下发** | ✅ **满足** | 未改动；`_master_nodes` 现在**取不到会记 ERROR**（I-3），不再静默错配 |

### 6.2 合同验收检查（`Approved Behavior` 的「验收检查」段）

| # | 项 | 判定 | 实测 |
|---|---|---|---|
| A | `pytest tests/ -q` 全绿 | ✅ | **1295 passed in 23.24s** |
| B | `--cov-fail-under=80`；映射层与规则引擎 ≥95% | ✅（整体） | `Required test coverage of 80% reached. Total coverage: 92.60%`（TOTAL 3756 stmts / 278 miss / **93%**） |
| C | `pytest tests/test_py37_compat.py -q`（3.7 语法门禁） | ✅ | **120 passed in 0.95s** |
| D | `ruff check tpops_kafka_collector/` 干净 | ✅ | `All checks passed!` |
| E | 回归层通过 | ✅ | `pytest tests/regression -q` → **230 passed**（上一轮 225，+5 条采集器层回归） |
| F | 手工事项（SIGTERM / 杀进程重启） | ⭕ 不适用 | `main.py` 属 wave 3 任务 3.3 |

### 6.3 「spec 合规的硬判据」（合同 §Review Gates）

| 判据 | 判定 | 依据 |
|---|---|---|
| 合并形态下 82 个指标的值等于期望值表 | ✅ **满足**（**映射层 82/82 + 采集器层 1700 条精确相等**） | 采集器层：`checked == comparable == 1700`（我独立重算 1700），被比指标码 76（第一轮有期望值的全部）；剩下 6 条有据可依且有别的守卫兜住 |
| 生产包无 `tools` / `data` import | ✅ | 本轮改动文件清单里**没有**任何 `import tools` / `import data`；`mapping/validation.py` 只导入 `.columns` / `.table` |
| 既有 164 个用例仍全绿 | ✅ | 1295 passed |
| 3.7 语法门禁通过 | ✅ | 120 passed |

### 6.4 治理红线

| 红线 | 判定 | 依据 |
|---|---|---|
| 不得改 `execution-contract.md` / `tasks.md` / `data/mapping/*.json` | ✅ | `git diff 40309f5..b10b706 --stat` 的**生产/测试**文件清单里没有这三类；`data/` 下只有新增的 `tests/fixtures/` 留档（在 `tests/` 下，不在 `data/mapping/`） |
| 生产代码只有一份（`tpops_kafka_collector/`） | ✅ | 本轮新增的 3 个生产文件全在该包下 |
| 凭据只走环境变量 / 不入日志 | ✅ | 本轮生产代码零处凭据字面量；新加的两条 ERROR 日志只带实例 ID / 节点 ID / 异常文本 |
| 不可变数据 | ✅ | `LoadedMapping` 是 frozen dataclass；`ScopedCounterDeltaStore` 无状态；新模块只返回新对象 |

---

## 7. 分级清单

### 7.1 Critical（0）

**无。** 三条 Critical 全部真修好，且每条都有**实测能让它变红**的守卫测试。

### 7.2 Important（0）

**无。** 四条 Important 全部真修好；实施方报告里的 3 处不准确断言（§4）**均不构成规格违反**
（其中 2 处只是统计/口径写错，1 处已被 `b10b706` 修正）。

### 7.3 Minor（7）

| # | 内容 | 位置 | 处置建议 |
|---|---|---|---|
| **M-N1** | `mapping/loader.py` 拆出 `validation.py` 后**漏掉 `List` 的 import**，但仍留有两处 `# type: List[...]` 注释 → mypy/pyright 会报 `Name "List" is not defined`（**运行期无影响**：3.7 不求值类型注释） | `mapping/loader.py:42`（import 行）、`:142`、`:189` | `from typing import ... ` 补上 `List` |
| **M-N2** | 死代码：`CommandCenterCollector._paged_views()` 从未被调用（调用点用的是 `self._paged_view_names`） | `collectors/command_center.py:318-320` | 删除该方法，或让 `:227` 改调它 |
| **M-N3** | 死代码：`CounterDeltaStore.forget(..., scope=)` 与 `ScopedCounterDeltaStore.forget` —— **全仓无生产调用方** | `mapping/rules_deltas.py:94-98, 130-133` | 删除，或明确标注「预留给 wave 3 的维度消失场景」 |
| **M-N4** | **回归层抓不住 C-3 这类形态**：把作用域去掉后 `tests/regression` 仍 **230 passed**（B 类回归只断言「产出的指标码集合」，不比对各 master 的速率） | `tests/regression/test_collector_coverage.py:108-141` | 在回归层的 B 类用例里**顺带断言三个 master 各自的速率**（与 `test_collector_command_center_deltas.py` 同口径），让 wave 4 只跑 `tests/regression` 时也能拦住跨 master 污染 |
| **M-N5** | `EXPECTED_COMPARISONS = 1700` 是**硬编码魔数**（我已独立重算复现，但期望值表更新后要手工同步） | `tests/integration/test_collector_command_center.py:540` | 在测试里从期望值表**推导**出这个数（口径已明确），避免漂移 |
| **M-N6** | 搭车断言：`test_command_center_result_carries_no_watermark_state` 里那句 `assert len(NODE_NAMES) == 3` 与该测试主题无关 | `tests/integration/test_collectors_do_not_advance_watermarks.py:191` | 删掉，或移到真正该断言节点数的地方 |
| **M-N7** | **wave 3 交接风险**：`WatermarkStore.put_baseline` / `get_baseline` / `forget_baseline` 用**不带作用域**的维度键，与 `ScopedCounterDeltaStore` 的键**对不上**（生产无调用方，只有测试在用） | `watermark.py:163-176` 与 `mapping/rules_deltas.py:35-45` | wave 3 读写差分基线**只能**用 `import_baselines_from` / `export_baselines_into`；或在 `watermark.py` 的三个方法上加「维度键必须含作用域」的说明 |

### 7.4 上一轮 12 条 Minor 的关闭情况

| 编号 | 状态 |
|---|---|
| M-1（`instance.py::collect` 52 行） | ✅ **已修**（拆出 `_collect_messages`） |
| M-2（告警分页 `total_count=0` 只取第一页） | ✅ **已修**（`page.total_count > 0 and ...`），有守卫 `test_pagination_continues_when_total_count_is_zero` |
| M-3（兜底路径值与资源名不同源） | ✅ **已修**（`master_view != 'identical'` 时本轮不产出） |
| M-4（`CMD_PAGE_SIZE` 没接到采集器） | ✅ **已修**（`from_config`） |
| M-5（回报缩小前的页容量） | ✅ **已修**（`page_size=self._page_size`） |
| M-10（「不推进水位」断言不可证伪） | ✅ **已修**（改成按类型查 + 逐字节不变 + 状态不变） |
| M-6（双引号标识符里的数字被脱敏） | ⭕ 未修（过脱敏，安全方向；wave 4 前单独修） |
| M-7（死代码 `_as_dict` / `collection_window_ms`） | ⭕ 未修（不在本轮范围） |
| M-8（C 类二次取 `now_ms`；窗口写死在 `COLLECTION_WINDOWS`） | ⭕ 未修（不在本轮范围） |
| M-9（分页守卫只看相邻两页） | ⭕ 未修（防御性加固） |
| M-11（5 个测试文件超 400 行） | ⭕ 未修（本轮**新增**文件而非加长既有大文件；拆分属纯整理） |
| M-12（规格与实现的告警增量口径不一致） | ⭕ 未修（需改**规格文字**，登记为 T-15，wave 4 前落地） |

**T-16 / T-19 已由本轮关闭。** T-18（丢弃计数）、T-20 / T-21 仍未落地。

---

## 8. 复审证明命令的**实际输出**

```
$ cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev

$ conda run -n tpops python -m pytest -q
1295 passed in 23.24s

$ conda run -n tpops python -m pytest tests/regression -q
230 passed in 3.05s

$ conda run -n tpops python -m pytest tests/integration -q
159 passed in 6.74s

$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
120 passed in 0.95s

$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest --cov=tpops_kafka_collector --cov-fail-under=80 -q
Required test coverage of 80% reached. Total coverage: 92.60%
TOTAL                                                          3756    278    93%
1295 passed in 27.49s

# --- 独立重算（期望值表比对条数）---
$ conda run -n tpops python /tmp/verify_i2.py
host master node: gauss-6374_gaussdbv5_hcs2-0_0 3881c04403394ee1b36a2bf873e69007no14
B-class rule count: 82
independent comparable = 1700
codes with expected values = 76
total template codes in table = 82
table codes not in B-class rules: []

# --- 独立实测（差分：两轮 × 三 master）---
$ conda run -n tpops python /tmp/verify_delta.py
R1 total=1700 delta=0
R2 total=1835 delta=135
  node 3881c04403394ee1b36a2bf873e69007no14 count 45 sample value 100 code GAUSSDB.[lz]_fetch_records_per_second
  node 40abc563810e462a97b065e718189a59no14 count 45 sample value  50 code GAUSSDB.[lz]_fetch_records_per_second
  node 5a3cd027ad5c4f9bbb67d728c6d279c0no14 count 45 sample value 150 code GAUSSDB.[lz]_fetch_records_per_second

# --- 独立核实（留档里确实没有 AccessExclusiveLock）---
$ conda run -n tpops python /tmp/verify_locks.py
gauss-6374_gaussdbv5_hcs2-0_2 locks rows 1 kinds ['relation']
gauss-6374_gaussdbv5_hcs2-0_1 locks rows 1 kinds ['relation']
gauss-6374_gaussdbv5_hcs2-0_0 locks rows 1 kinds ['relation']

# --- 独立核实（A/C/D 期望表条目非空 ⇒ 断言非恒真）---
$ conda run -n tpops python /tmp/verify_cd.py
Counter({'monitor_metrics': 11, 'diagnostic': 4, 'instance': 1}) total 16
（16 条的 expected items 全部 ≥ 1）

# --- 独立测量（生产包函数长度 / 文件行数）---
$ conda run -n tpops python /tmp/flen.py
49 collectors/monitor.py _messages_of_rule
48 collectors/command_center.py _fetch_pages
46 collectors/alarm.py collect_alarms          ← b10b706 修掉了 51 行
...
$ wc -l tpops_kafka_collector/**/*.py
（全部 < 400；command_center.py 344 / helpers 161 / assembly 151 / loader 236 / validation 226）

# --- 可证伪性实验（把修复改回去，看守卫会不会红）---
$ PYTHONPATH=/tmp/fakep conda run -n tpops python -m pytest -p p_c1 tests/integration/test_collector_alarm.py -q
21 failed, 1 passed            ← 删掉 LoadedMapping 的两个方法

$ PYTHONPATH=/tmp/fakep conda run -n tpops python -m pytest -p p_c2 tests/integration/test_collector_command_center_deltas.py -q
4 failed, 1 passed             ← 把 now_ms 强制成 0

$ PYTHONPATH=/tmp/fakep conda run -n tpops python -m pytest -p p_c3 tests/integration/test_collector_command_center_deltas.py -q
3 failed, 2 passed             ← 把作用域去掉（回到「共用一个存储」）

$ PYTHONPATH=/tmp/fakep conda run -n tpops python -m pytest -p p_c2 tests/regression -q
2 failed, 228 passed           ← 回归层也能抓住 C-2

$ PYTHONPATH=/tmp/fakep conda run -n tpops python -m pytest -p p_c3 tests/regression -q
230 passed                     ← 回归层抓不住 C-3（M-N4）

# --- 导入环检查 ---
$ conda run -n tpops python -c "import importlib; [importlib.import_module(m) for m in [...9 个模块...]]"
ok × 9                        ← 无循环依赖
```

---

## 9. 附：本复审做过的聚焦检查（风险 → 查了什么）

> 审查原则：只在评估一个**能具体命名的风险**时才看范围外的代码，每个风险做一次聚焦检查。

| 风险 | 查了什么 | 结果 |
|---|---|---|
| C-1 的修复是不是只加了方法、但测试仍用旧对象？ | 读 `tests/integration/test_collector_alarm.py` 的 fixture 与 import | fixture 已换成 `load_mapping()`，`MappingTable` 的 import 已删 ✅ |
| 「有守卫」是不是自称的？ | 运行期把 `LoadedMapping` 的两个方法删掉再跑整档 | **21 个用例红** ✅ |
| C-2 的 `now_ms` 是不是每页各取一次？ | 读 `collect()` / `_fetch_pages` / `_merge` / `_handle_failure` 的数据流 | `collect()` 取一次，往下传，重试沿用 ✅ |
| C-2 的 135 条是不是真的？ | 自己跑两轮采集器（不依赖测试断言） | R1 delta=0、R2 delta=135，三 master 100/50/150 ✅ |
| C-3 的隔离是不是真的按节点？ | 读 `ScopedCounterDeltaStore` + 运行期去掉作用域再跑 | 隔离成立；去掉作用域后 **3 个用例红** ✅ |
| 基线键加了 scope 会不会撑破 `watermark.py`？ | 读 `watermark.py` 的 `import/export_baselines_from`、`_baselines_container`、`_apply` | 键形状仍是二元组，作用域并进维度键字符串，**文件格式不变**；4 条往返用例全绿 ✅ |
| 拆文件有没有把守卫弄丢？ | 旧 `command_center.py` 与新三文件**逐函数文本 diff** | 9 个辅助函数**逻辑零差异**（2 处 DIFF 只是改名/类型注释） ✅ |
| `mapping/validation.py` 有没有改变启动期校验的行为？ | 旧 `loader.py` 的四个 `check_*` 与新文件**逐函数 diff** | 只有前向引用注解写法不同，**逻辑逐字未变** ✅ |
| 新模块有没有循环依赖？ | 逐个 import 9 个相关模块 | 全部成功，无环 ✅ |
| `instance.py` 的重构有没有改语义？ | 读 diff：`instance_id` → `instance.instance_id` | `InstanceMetadata.instance_id` 就是平台 `id`，且 `_target_instance` 按 `item.instance_id == instance_id` 匹配 ⇒ **等价** ✅ |
| `_fetch_four_shapes` 有没有改语义？ | 逐条对照重构前后的四个调用 | 收敛 → `instance_id=None`、明细 → `instance_id`，**完全一致** ✅ |
| 1700 这个数是不是拍的？ | 从期望值表**独立重算** | 独立重算 = **1700** ✅ |
| 那 6 条没被比到的指标是不是「换姿势跳过」？ | 读留档的 `locks` 视图 | 确实没有 `AccessExclusiveLock`；5 条差分指标由第二轮用例守住 ✅ |
| 回归层的断言是不是恒真的？ | 把 C-2 / C-3 形态打回去再跑回归层 | C-2 挂 2 条；C-3 **不挂**（M-N4） |
| 有没有「凑覆盖率」的测试？ | 逐条读本轮 7 个新增/改写测试文件 | 无空测试；只有 1 条搭车断言（M-N6） ✅ |

---

## 10. 放行建议

1. **三条 Critical + 四条 Important 全部关闭** ⇒ 本波次可以给 **pass**。
2. Minor 7 条：M-N1 / M-N2 / M-N3 / M-N5 / M-N6 都是纯整理，随手改；
   **M-N4**（回归层补各 master 速率断言）建议**本波次或 wave 3 顺带做**，
   否则 wave 4 的门禁只跑 `tests/regression` 时拦不住 C-3 这类形态；
   **M-N7**（wave 3 只能走 `import/export_baselines_into`）**必须写进 wave 3 的交接清单**。
3. 合同完成标准第 2 条的**「丢弃计数递增」**仍未实现（T-18）—— 上一轮与本轮都未计入
   Critical/Important，维持原判定；若控制器要求本波次闭环，请退回本条后重签。
4. 本报告**不签发 receipt**，未修改任何被审代码、`execution-contract.md`、`tasks.md`
   或 `.spec-superflow.yaml`。
