# Wave 2b 审查报告 —— wave-2b-channels（revision 3）

- **波次**：`wave-2b-channels`（8 个任务：2.3 / 2.4 / 2.5 / 2.6 / 2.7 / 2.8 / 2.9 / 2.10）
- **审查对象**：`workspace/review-wave2b-r3.diff`（25234 行，一次读完）
- **工作树**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`
- **审查者**：独立审查者（未采信实施方报告；所有结论均在本 worktree 上实测复现）
- **审查日期**：2026-11-23

---

## 1. 结论

> ## ❌ verdict = **fail**
>
> **Critical 3 / Important 4 / Minor 12**

| 级别 | 条数 | 摘要 |
|---|---|---|
| **Critical** | **3** | C-1 E 类采集器用生产映射对象必崩；C-2 B 类 5 个 `counter_delta` 客户指标永不产出；C-3 B 类差分基线跨 master 共享互相污染 |
| **Important** | **4** | I-1 `collectors/command_center.py` 463 行越过合同「单文件 < 400 行」；I-2 2.6 的「值等于期望值表」守卫只覆盖 47/82；I-3 `_master_nodes` 静默吞异常不记日志；I-4 合同完成标准「98 个可提供指标全部产出数据」不成立 |
| **Minor** | 12 | 见 §8.3 |

**为什么必须 fail**：三条 Critical 全部是「**不报错、但数据错/数据没有**」的形态 —— 正是本项目最贵的一类缺陷（合同 Escalation Rules 明列「发现『不报错但数据错』的路径而未被测试拦住」为不得继续实现的情形）。其中：

- C-1 使 **E 类通道在生产装配下 100% 抛 `AttributeError`**（全部告警不产出）——且集成测试用错了映射对象，把缺陷掩盖掉了；
- C-2 使 **5 个客户指标模板 × 9 个 DATNAME 维度 × 3 个 master = 135 条消息/轮永不投递**，其余 1700 条正常，**从外面看不出异常**；
- C-3 使 3 个 master 的差分基线互相覆盖，**2/3 的 master 恒为 `None`、其余 master 的速率可能算成别的节点的**。

三条都在**采集器层**，而 `tests/regression/`（225 passed）与 `tests/test_expected_values.py`（82/82）**只覆盖映射层**（`decode_merged` + `evaluate`），**完全没有走到采集器** —— 所以这三类缺陷一个都没被现有守卫拦住。

---

## 2. 合同完成标准逐条判定

### 2.1 Wave 2b 完成标准（`execution-contract.md` §Execution Waves）

| # | 合同完成标准 | 判定 | 依据 |
|---|---|---|---|
| 1 | `pytest tests/regression -q` —— **98 个可提供指标全部产出数据**（D1） | ❌ **不满足** | `pytest tests/regression -q` → **225 passed**（命令本身通过），但回归层只断言**映射层**的 `decode_merged` + `evaluate` 输出，**不经过任何采集器**。实测采集器层：1700 条消息中 **0 条** `*_records_per_second`（5 个 `counter_delta` 指标永不产出，见 C-2）；E 类采集器用生产映射对象直接崩溃（见 C-1）。「98 个指标全部产出数据」在采集器层**不成立** |
| 2 | **3 个不可提供指标无消息**且**丢弃计数递增**（D2） | ⚠️ **部分满足** | 「无消息」✅：`metric_rules.json` 里 3 条 `enabled=false`（`gaussdb.Fastpath_function_call` / `gaussdb.Disabled_conn` / `gaussdb.sendAgentName`），采集器按 `enabled` 过滤 → 不产出。「**丢弃计数递增**」❌：全仓无任何丢弃计数机制（`grep -rn "discard\|dropped" tpops_kafka_collector/` 只命中 kafka producer 的配置项忽略日志）。该机制属 wave 3 每轮汇总日志 → 登记为待补 T-18 |
| 3 | **多 master 归集**：节点级视图每 master 一份、实例级视图只一份、**无 3 倍累加** | ✅ **满足** | 独立实测：82 条 B 类规则 = 42 条 `node_scoped=true` + 40 条 `node_scoped=false`；`gaussdb.Deadlocks`（node_scoped）产出 3 份（3 个 master 各一），`gaussdb.get.lock.info`（非 node_scoped）产出 1 份。`merge_pages` 只对 `merged_views.paged=true` 的 2 个视图（`stat_user_tables` / `dbstat_sum`）跨页拼接，固定视图只取第 1 页 → 无累加翻倍（实测 `merged_views` 与 `merged_sql.json.paging.views` 两处 `paged` 集合**完全一致**） |
| 4 | **分页**：两页拼接后行数正确；空页不抛异常 | ✅ **满足** | `merge_pages` 对 `paged` 视图 extend、对固定视图取 `per_page[0]`；`decode_merged` 对 `''`/`null` 列 → `[]` 不抛异常。实测：可增长视图满页 → offset 递增（0/10/20），不满页即停；固定视图满页不判「还有下一页」 |
| 5 | **节点清单去重后只向唯一 `node_id` 下发** | ✅ **满足** | `MasterNodeRegistry._select_masters` 按 `role=='master'` 过滤 + 按 `node_id` 去重；留档回放断言 **12 条条目 / 6 条 master / 3 个唯一 node_id**，`submit['node_ids'] == ['node-a','node-b','node-c']` |

### 2.2 合同验收检查（`Approved Behavior` 的「验收检查」段）

| # | 项 | 判定 | 实测 |
|---|---|---|---|
| A | `pytest tests/ -q` 全绿 | ✅ | **1262 passed in 23.46s** |
| B | `--cov-fail-under=80`；映射层与规则引擎 ≥95% | ✅（整体） | 生产包整体 **92%**；映射层 `rules_*.py` 多数 ≥93%（`rules_cross.py` 77% 除外，属 wave 3 门槛） |
| C | `pytest tests/test_py37_compat.py -q`（3.7 语法门禁） | ✅ | **112 passed in 1.27s** |
| D | `ruff check tpops_kafka_collector/` 干净 | ✅ | `All checks passed!`（exit 0） |
| E | 回归层 98/98 通过 | ✅（映射层） | `pytest tests/regression -q` → **225 passed** |
| F | 手工事项（SIGTERM / 杀进程重启） | ⏭️ 不适用 | `main.py` 属 wave 3 任务 3.3，本波次不产出 |

### 2.3 「spec 合规的硬判据」（合同 §Review Gates）

| 判据 | 判定 | 依据 |
|---|---|---|
| 合并形态下 82 个指标的值等于期望值表 | ⚠️ **部分满足** | 映射层 **82/82**（`tests/test_expected_values.py` 与 `tests/regression/test_b_class_regression.py`，逐节点 246 组值相等）。**采集器层只覆盖 47/82**（见 §5，I-2） |
| 生产包无 `tools` / `data` import | ✅ | `tests/unit/test_package_boundary.py` 全包 AST 扫描通过；本审查另行核对：生产包唯一读仓库根 `data/` 的是 `mapping/table.py::MappingTable.load()`（wave-2a 已登记 T-10，**本波次无新增**），`sqlgen` / `loader` 均读包内 `mapping/data/` |
| 既有 164 个用例仍全绿 | ✅ | 1262 passed |
| 3.7 语法门禁通过 | ✅ | 112 passed；另做 AST/正则扫描：13 个新增生产文件 + 9 个新增测试文件，**0 处**命中禁用形态（`:=`、`list[`/`dict[`/`tuple[`/`set[`、`Literal`/`Protocol`/`TypedDict`/`Final`、`cached_property`、`zoneinfo`、`removeprefix`、f-string 自文档、`kw_only`、`asyncio.to_thread`、`importlib.metadata`）。`end_lineno` 缺失本身反证环境确为 **Python 3.7.9** |

---

## 3. 八个任务的证明命令 —— 实际运行结果

### 3.1 简报第 2 节指定的命令

```
$ conda run -n tpops python -m pytest \
    tests/unit/test_metrics_window.py \
    tests/unit/test_command_center.py \
    tests/unit/test_sqlgen.py \
    tests/unit/test_masking.py \
    tests/integration/test_collector_monitor.py \
    tests/integration/test_collector_command_center.py \
    tests/integration/test_collector_diagnostic.py \
    tests/integration/test_collector_instance.py \
    tests/integration/test_collector_alarm.py -q
```

```
........................................................................ [ 32%]
........................................................................ [ 65%]
........................................................................ [ 98%]
...                                                                      [100%]
219 passed in 6.38s
```

### 3.2 全量 + 静态检查 + 回归 + 门禁

```
$ conda run -n tpops python -m pytest -q
1262 passed in 23.46s

$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!
（exit 0）

$ conda run -n tpops python -m pytest tests/regression -q
225 passed in 2.87s

$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
112 passed in 1.27s

$ conda run -n tpops python -m pytest --cov=tpops_kafka_collector --cov-report=term-missing -q
1262 passed in 29.82s
TOTAL  3679 stmts  280 miss  92%
```

采集器层覆盖率：`collectors/alarm.py` 94%、`collectors/command_center.py` 94%、`collectors/diagnostic.py` 93%、`collectors/instance.py` 91%、`collectors/monitor.py` 90%。

### 3.3 逐任务核对

| 任务 | 证明文件 | 用例数 | 结果 |
|---|---|---|---|
| 2.3 A 类窗口与切片 | `tests/unit/test_metrics_window.py` | 397 行 | ✅ 窗口 ≥1h 快速失败、`end_time` 回退、`is_latest_point_only` 为**字符串** `'false'`、12 分组码齐全、空分组 WARNING、null 点跳过、档位切片 |
| 2.4 A 类采集器 | `tests/integration/test_collector_monitor.py` | 484 行 | ✅ 逐组件各出一条消息、`COMPONENT_ID` 进 tags、null 不出 0、空分组告警、节点身份换算（`cn_5002` / `ip-dn_6001`）、窗口不足不发请求 |
| 2.5 命令中心三跳 | `tests/unit/test_command_center.py` | 676 行 | ✅ `statistics.running==0` 判结束（`job_status` 故意停在 running）、`result` 二次解析、master + `node_id` 去重、清单缓存 1h、逐条重试、单条失败隔离、并发上限 |
| 2.6 B 类采集器 | `tests/integration/test_collector_command_center.py` | 516 行 | ⚠️ 走 `decode_merged`/`merge_pages` ✅、查 `outcome.ok` ✅、分页只对可增长视图 ✅、`node_scoped` 归集 ✅、102400 B 上限 ✅；**值相等守卫只覆盖 47/82**（I-2） |
| 2.7 C 类诊断优化 | `tests/integration/test_collector_diagnostic.py`（**补交 `e072dcd`，从未被审过**） | 452 行 | ✅ 路径在 `/rds/gaussdb/v3/`、请求体字段、窗口在采集器层断言、条数 / 前缀计数 / Top N、脱敏注入且失败即抛、集合空记 WARNING、缺值不产出 |
| 2.8 D 类实例元数据 | `tests/integration/test_collector_instance.py` | 344 行 | ✅ `/rds/v3.2/instances`、只取目标实例、`total_count` 翻页、落第一个 master CN、1 小时档表驱动、三种缺值都不产出、平台错误上抛 |
| 2.9 E 类告警归并 | `tests/integration/test_collector_alarm.py` + `tpops/alarms.py`（**补交 `e072dcd`，从未被审过**） | 485 行 | ✅ 分页取全（250 条 → offsets `[0,100,200]`）、四种组合、本地归并、收敛形态不依赖平台过滤、节点回填、`details` 恒为数组、30 天超期 WARNING、幂等键含 status；❌ **但用生产映射对象必崩**（C-1） |
| 2.10 慢 SQL 脱敏 | `tests/unit/test_masking.py` | 240 行 | ✅ 字符串 / 数值 / 十六进制 / 二进制 / 美元引用 / 转义引号 / `E''` 前缀 / 注释体 / `$1` 占位符 / `NULL`·关键字·标识符里的数字保留 / 双引号标识符保留 / 截断标记算在长度内 / `masking_enabled=False` 仍截断 |

---

## 4. 第 3 节四条坑 —— 守卫有效性判定

### 坑 1：合并形态当行列表用 → 82 个 B 类指标静默全变 0

**判定：守卫存在且非恒真，但采集器层的覆盖只有 47/82（I-2）。**

- 实现：`collectors/command_center.py::_fetch_pages` → `decode_merged(body, self._views)`；`_merge` → `merge_pages(pages, self._views)`。**没有**任何把 `task.rows` 直接当行列表的路径。
- 非恒真性：`decode_merged` 对「不是恰好 1 行」抛 `DecodeError`、对「视图集为空」抛 `DecodeError`（M-8 已在 wave 0 修过）。把采集器改成「直接用 `task.rows` 当视图行」→ 82 个指标的取值会变成 0/None，`test_collected_values_equal_the_expected_table` 的 47 条会红。
- **盲区**：该守卫在采集器层只比对 47 条（§5）。维度展开型的 35 条（`GAUSSDB.[{#DATNAME}]_*` / `GAUSSDB.{#NAME}.*` / `gaussdb.{#NAME}.*` / `gaussdb.DBtsize[{#DATABASE_NAME}]` / `gaussdb.locks.*`）走 `continue` 被跳过。映射层另有 82/82 的守卫（wave 0），但**采集器层的「占位符替换 → 维度展开 → 消息 metricCode」这一段没有任何值相等守卫**。

### 坑 2：命令中心失败不抛异常 → 不检查 `outcome.ok` 就静默丢数据

**判定：守卫存在、非恒真、有效。**

- 实现：`collectors/command_center.py::_fetch_pages` 首行即 `if not outcome.ok:` → 记 ERROR `'命令中心取数失败（实例 %s，第 %d 页）：%s'` → `return None, 0` → `collect()` 返回**空消息元组**（不产出任何消息、不推进水位）。
- 非恒真性：测试 `test_failed_command_produces_no_message_and_logs` 用 `FakeCommandCenter(error='平台 503')` 让 `execute_sql` 返回 `SqlOutcome(ok=False, ...)` → 断言 `result.messages == ()` 且日志含 `'命令中心取数失败'`。把 `if not outcome.ok:` 删掉 → 该用例立刻红。
- 旁证：超限缩小页容量的重试分支同样先查 `outcome.ok`。

### 坑 3：分页对固定视图也生效 → 累加翻倍

**判定：守卫存在、非恒真、有效（两道闸都在）。**

- 闸 ①（还要不要翻页）：`_has_more(page_contexts, page_size, self._paged_views())` **只遍历 `merged_views[paged=true]` 的视图**（`stat_user_tables` / `dbstat_sum`），固定视图不参与判「还有下一页」→ 不会死循环。
- 闸 ②（怎么拼页）：`merge_pages` 只对 `paged=true` 的视图 `extend`，固定视图取 `per_page[0]` → 不累加。
- 两处 `paged` 集合一致（实测 `response_rules.json.merged_views` 与 `merged_sql.json.paging.views` 都是 `{stat_user_tables, dbstat_sum}`）→ 渲染带 offset 的视图与判「还有下一页」的视图是同一批，不存在错位。
- 非恒真性：`test_pagination_stops_when_growable_view_is_not_full`（固定视图每页都满页 → 只发 1 次请求）；`test_paginates_while_a_growable_view_is_full`（offset 0→10→20）。

### 坑 4：`node_scoped=false` 时值与资源名落在不同 master

**判定：主路径结构性成立；兜底路径仍会错配（Minor M-3）。**

- 主路径：`_host_node_id(nodes, ...)` 取 `select_master_node(nodes)`（**第一个 master CN**）；`shared = values_by_node.get(host_node_id)`；`_resource_context` 对 `node_scoped=false` **不写 `node_id`** → `resolve_node_context` 用**同一个** `select_master_node(nodes)` 兜底 → 值与资源名**必然同源**。
- 兜底路径（承载 master 本轮没回结果）：代码记 WARNING 后 `shared = list(values_by_node.values())[0]` —— **值来自别的 master，资源名仍写第一个 master CN**。
- 独立复现（见 §4.1）：确实出现「值来自 `_0_1`、resourceName 写 `_0_0`」。
- 缓解事实：40 条 `node_scoped=false` 的 B 类指标**全部** `master_view: identical`（值在 master 间按设计相同）→ 当前**无数据影响**，故判 Minor 而非 Important。
- 测试可证伪性不足：现有断言只检查 `tags.node_id in master_ids`，**无法证伪「值与名不同源」** —— 因为非 node_scoped 的消息里没有「值来自哪个节点」的标记。

### 4.1 坑 4 兜底路径的独立复现（实测输出）

```
$ conda run -n tpops python /tmp/rev2b/host_node.py
承载资源名的 master（3881c04403394ee1b36a2bf873e69007no14）不在本轮结果里 —— 与节点无关的指标退回用第一个结果节点的值
节点清单里的第一个 master CN: 3881c04403394ee1b36a2bf873e69007no14 172.16.103.126

node_scoped=false 指标 gaussdb.get.lock.info 的消息：
   resourceName = 172.16.103.126_3881c04403394ee1b36a2bf873e69007no14 | tags.node_id = 3881c04403394ee1b36a2bf873e69007no14
   消息声明的节点 = 3881c04403394ee1b36a2bf873e69007no14 -> gauss-6374_gaussdbv5_hcs2-0_0
   值实际来自的 master = gauss-6374_gaussdbv5_hcs2-0_1   （第一个 master CN 没有结果）
```

### 4.2 期望值表的**独立重算**（不只信那张表）

按要求对期望值表做了**独立推导**：直接读 `data/merged_sql/sql_merge_probe_result.json` 的**原始行**，按映射表声明的取值口径自行重算，再与 `data/mapping/expected_values.json` 对比。共抽 **13 个指标 × 3 个 master = 39 组**，全部一致。

```
$ conda run -n tpops python /tmp/rev2b/recompute.py       # 6 个指标 × 3 节点 = 18 组
  gaussdb.Deadlocks       我算=0     表里=0     OK
  GAUSSDB.Buffers_Alloc   我算=0     表里=0     OK
  gaussdb.get.lock.info   我算=1 行  表里=1 项  OK
  gaussdb.Lockwaiting     我算=0     表里=0     OK
  gaussdb.Waiting_conn    我算=0     表里=0     OK
  GAUSSDB.session_usage   我算 21/25=0.84  表里=0.84  OK     ← node _0_0
  GAUSSDB.session_usage   我算 23/29=0.79  表里=0.79  OK     ← node _0_1 / _0_2
  activity_agg rows: [{"datname":"postgres","state":"active","cnt":7},{"datname":"postgres","state":"idle","cnt":14}]
  stat_database numbackends/deadlocks: [('lz',0,0),('ora',0,0),('postgres',25,0),('sml',0,0),('template0',0,0),
                                          ('template1',0,0),('templatea',0,0),('templatem',0,0),('xzl',0,0)]
  bgwriter: [{"buffers_alloc":0,"buffers_backend":6,...,"checkpoints_timed":7366,...}]

$ conda run -n tpops python /tmp/rev2b/recompute2.py      # 再抽 7 个（含非零值）× 3 节点 = 21 组
  gaussdb.Blks_hit                             我算=105175496  表里=105175496  OK
  gaussdb.Blks_read                            我算=19029      表里=19029      OK
  gaussdb.Numbackends                          我算=25         表里=25         OK
  gaussdb.Total_conn                           我算=25         表里=25         OK
  GAUSSDB.Current_number_of_connections        我算=25         表里=25         OK
  GAUSSDB.Checkpoint_count_eckpoint_meout      我算=7366       表里=7366       OK
  GAUSSDB.Checkpoint_count_eckpoint_gments     我算=2          表里=2          OK
  GAUSSDB.Buffers_Backend                      我算=6          表里=6          OK
  gaussdb.Idle_conn                            我算=14         表里=14         OK
  gaussdb.Active_conn                          我算=7          表里=7          OK
  gaussdb.Pagesize        （settings 里 name='block_size' → setting='8192'）表里=8192  OK
  gaussdb.Timezone        （settings 里 name='TimeZone'   → 'Etc/GMT-8'）  表里='Etc/GMT-8' OK
  GAUSSDB.maximum_connection（settings 里 name='max_connections' → '200'） 表里=200 OK
```

**结论**：期望值表**可信**，且**不是靠 0 值蒙混**（65 个指标的期望值非零，抽查的 `Blks_hit=105175496`、`checkpoints_timed=7366`、`session_usage=23/29=0.79` 等均与我的独立重算一致）。`23/29 = 0.79` 与合同「关键场景 4」的探针实测值吻合。

**但要注意**：`GAUSSDB.session_usage` 的分母 `numbackends` 在**三个 master 上不同**（25 / 29 / 29），而 `activity_agg.cnt` 也不同（21 / 23 / 23）→ **比率本身是逐 master 不同的**，这正是坑 4 里「值必须与节点同源」的根据（`session_usage` 是 `node_scoped=true`）。

---

## 5. 第 4 节「值等于期望值表」的覆盖度判定

### 5.1 判定：**不成立（采集器层只覆盖 47/82）**

`tests/integration/test_collector_command_center.py::test_collected_values_equal_the_expected_table` 的实测统计：

```
产出消息总数: 1700
checked 计数 = 113
被比到的 metric_code 数 = 47
未被比到的 metric_code 数 = 35
```

- `checked = 113`（不是 0，也不是个位数）→ 断言**不是**形同虚设。
- 但 **82 条 B 类指标里只有 47 条被比到**（57%）。
- **原因**（不是「没产出消息」）：期望值表的键是**模板码**（`GAUSSDB.[{#DATNAME}]_committed_transactions`），而消息里的 `metricCode` 是**占位符替换后的码**（`GAUSSDB.lz_committed_transactions`）→ `produced.get(metric_code)` 永远 miss → `continue` 跳过。**35 条维度展开型指标全部落在这个洞里。**
- 逐条比对时**走了同一套定型** ✅：`serialize_value(items[0]['value'], value_type_by_code.get(metric_code))` —— 期望值表存原始值、消息里是按客户表【数据类型】定型后的值，两边用同一个 `serialize_value` 对齐。**这一点是对的。**
- 已比对部分**全部相等**（不等的条目数 = 0）。

### 5.2 没被比到的 35 个指标码（完整清单）

```
GAUSSDB.[{#DATNAME}]_Active_connections              | aggregate    | activity_agg
GAUSSDB.[{#DATNAME}]_cache_hit_ratio                 | raw          | stat_database
GAUSSDB.[{#DATNAME}]_committed_transactions           | raw          | stat_database
GAUSSDB.[{#DATNAME}]_delete_records_per_second        | counter_delta| stat_database
GAUSSDB.[{#DATNAME}]_fetch_records_per_second         | counter_delta| stat_database
GAUSSDB.[{#DATNAME}]_insert_records_per_second        | counter_delta| stat_database
GAUSSDB.[{#DATNAME}]_return_records_per_second        | counter_delta| stat_database
GAUSSDB.[{#DATNAME}]_rolled_back_transactions         | raw          | stat_database
GAUSSDB.[{#DATNAME}]_update_records_per_second        | counter_delta| stat_database
GAUSSDB.{#NAME}.Number.ad.ples                        | raw          | stat_user_tables
GAUSSDB.{#NAME}.Number.lete.ples                      | raw          | stat_user_tables
GAUSSDB.{#NAME}.Number.t.date.ples                    | raw          | stat_user_tables
GAUSSDB.{#NAME}.garbage.Ratio                         | raw          | stat_user_tables
GAUSSDB.{#NAME}.number.Of.dex.ples.ad                 | raw          | stat_user_tables
GAUSSDB.{#NAME}.number.Of.index.scan                  | raw          | stat_user_tables
GAUSSDB.{#NAME}.number.Of.insert.tuples               | raw          | stat_user_tables
GAUSSDB.{#NAME}.number.Of.live.tuples                 | raw          | stat_user_tables
GAUSSDB.{#NAME}.number.Of.quential.ples.ad            | raw          | stat_user_tables
GAUSSDB.{#NAME}.number.Of.sequential.scan             | raw          | stat_user_tables
GAUSSDB.{#NAME}.number.Of.update.tuples               | raw          | stat_user_tables
gaussdb.DBtsize[{#DATABASE_NAME}]                     | raw          | database
gaussdb.locks.accessexclusive[{#DBNAME}]              | aggregate    | locks
gaussdb.locks.get_metrics[{#DBNAME}]                  | aggregate    | locks
gaussdb.{#NAME}.Number.ad.ples                        | raw          | stat_user_tables
gaussdb.{#NAME}.Number.lete.ples                      | raw          | stat_user_tables
gaussdb.{#NAME}.Number.t.date.ples                    | raw          | stat_user_tables
gaussdb.{#NAME}.garbage.Ratio                         | raw          | stat_user_tables
gaussdb.{#NAME}.number.Of.dex.ples.ad                 | raw          | stat_user_tables
gaussdb.{#NAME}.number.Of.index.scan                  | raw          | stat_user_tables
gaussdb.{#NAME}.number.Of.insert.tuples               | raw          | stat_user_tables
gaussdb.{#NAME}.number.Of.live.tuples                 | raw          | stat_user_tables
gaussdb.{#NAME}.number.Of.quential.ples.ad            | raw          | stat_user_tables
gaussdb.{#NAME}.number.Of.sequential.scan             | raw          | stat_user_tables
gaussdb.{#NAME}.number.Of.update.tuples               | raw          | stat_user_tables
gaussdb_{#PORT}port.status                            | raw_filtered | settings
```

**注意**：其中 **5 条是 `counter_delta`** —— 恰好就是 C-2 里**永不产出**的那 5 条。它们在期望值表里的 `round1` 值全是 `null`（首轮无基线），所以即使被比到也只比 `None == None`；**真正能证伪的「第 2 轮速率 == 期望速率」断言只在 wave 0 的映射层回归里有，采集器层完全没有**。

### 5.3 补救方向（不改代码也能先落地的）

映射层已有 82/82（`tests/test_expected_values.py` + `tests/regression/test_b_class_regression.py`）。采集器层要补的是**「按模板码聚合比对」**：把 `produced` 的键还原成模板码（用 `rule.placeholders` / `dimension_bindings` 反推），再与期望值表的**逐维度**条目比对（期望值表的 `round1[node][i]['dimensions']` 已带维度，天然可按 `(模板码, 维度 JSON)` 建索引）。

---

## 6. 第 5 节五个采集器的通则 —— 逐条判定

| 通则 | A 类（monitor） | B 类（command_center） | C 类（diagnostic） | D 类（instance） | E 类（alarm） |
|---|---|---|---|---|---|
| **缺值不产出消息** | ✅ `value is None → continue`（不填 0） | ✅ `value is None → continue` | ✅ `value is None` 与「空明细列表」都跳过并记 WARNING | ✅ `engine_version` 取不到 → 不产出 + ERROR | ✅ 记录级产出（值取 `alarm_items_sum`） |
| **空结果记日志** | ✅ 空分组 WARNING / 空 payload WARNING / 目录全空 ERROR | ✅ 失败 ERROR；**⚠️ 合并结果为空（无 task）时无日志** | ✅ 集合空 WARNING（含集合名） | ✅ 实例列表空 ERROR / 找不到目标实例 ERROR | ✅ 历史为空 INFO / 30 天超期 WARNING |
| **不静默吞异常** | ✅ `DecodeError`/`RuleError` 捕获后**记日志**并跳过（有意为之，测试有断言） | ❌ **`_master_nodes` 里 `except Exception: return ()`，无任何日志**（I-3） | ✅ 脱敏失败包装成 `MappingError` 上抛 | ✅ 平台异常原样上抛（`TransientError`） | ✅ 平台异常原样上抛 |
| **不推进水位** | ✅ 无水位依赖 | ✅ | ✅ | ✅ | ✅ 只读传入的 `watermark`，不写 |
| **不拼 Kafka 消息** | ✅ `mapping/message.build_metric_message` | ✅ 同左 | ✅ 同左 | ✅ 同左 | ✅ `mapping/message.build_alarm_message` |
| **序列化出口唯一** | ✅ 走 `kafka/serializer.serialize_metric`（AST 守卫全包扫） | ✅ | ✅ | ✅ | ✅ |

---

## 7. 测试质量（可证伪性）、越界与治理、行数核对

### 7.1 可证伪性逐条判定

| 关注点 | 判定 | 说明 |
|---|---|---|
| **2.6 的「平台不认 offset」守卫会不会漏掉真正的多页场景** | ⚠️ 基本可靠，有一条理论缺口 | 守卫只比较**相邻两页**的整页指纹（`sha256(repr(rows))`）。正常分页时相邻两页内容必不同 → 不会误停。缺口：若平台返回**交替页**（A,B,A,B…），相邻两页永远不同 → 无限翻页（M-9）。另一条：指纹覆盖**整行**（含固定视图的重复内容），只有可增长视图内容也相同才会命中 —— 语义正确 |
| **2.7 的窗口断言落点** | ✅ 正确 | `build_top_sql_body` / `build_slow_sql_body` 是**纯传参**；窗口断言落在**采集器层**（`test_collector_applies_fixed_window_per_collection`：读 `client.calls` 里的 `payload['end_time'] - payload['start_time']`），与 brief 的要求一致 |
| **2.10 的脱敏边界** | ⚠️ 覆盖充分，有一处过脱敏 | `NULL`/`TRUE`/`FALSE`/类型名 ✅ 保留；标识符里的数字 ✅ 保留；转义引号（`''` 与 `\'`）✅；美元引用（`$$…$$` 与 `$tag$…$tag$`）✅；`E''` 前缀 ✅；`$1` 占位符 ✅。**缺口**：双引号标识符**里带空格 + 数字**会被误脱敏（`"col 1"` → `"col ?"`，M-6）—— 过脱敏（安全方向），但会让客户看到的 SQL 失真 |
| **2.9 的分页取全** | ✅ **会翻页，不是只取第一页** | `fetch_all_alarm_records`：`offset` 按 `limit` 递增；`total_count=250` → 实测 offsets `[0, 100, 200]`、取满 250 条；测试 `test_pagination_fetches_every_page` 断言 `offsets == [0, 100, 200]`。**健壮性缺口**：`total_count` 为 0/缺失时 `len(collected) >= page.total_count` 会**在第一页就 break**（M-2） |
| **「采集器不推进水位」的断言** | ❌ **不可证伪** | `test_collector_does_not_touch_watermark` 断言 `not hasattr(collector, 'watermark')` —— 属性名可任意取，且 B/C/D/E 四个采集器**没有同类断言**（M-10） |
| **E 类采集器的集成测试用了什么映射对象** | ❌ **用错了对象，掩盖了 C-1** | fixture 是 `MappingTable(bundle.field_map, bundle.metric_rules, bundle.alarm_rules, {})`（手工拼的），而生产对象是 `load_mapping()` → `LoadedMapping`。两者 API 面不同（`LoadedMapping` 没有 `alarm_definition` / `metric_rule`） |
| **值相等守卫的可证伪性** | ⚠️ 部分 | 47 条可证伪；35 条被 `continue` 静默跳过（I-2）。断言 `checked > 0` 只保证「不是零条」，不保证「82 条都比上了」 |

### 7.2 越界与治理红线核对

| 红线 | 判定 | 依据 |
|---|---|---|
| 不得改 `execution-contract.md` | ✅ | diff 的文件清单里**没有** `execution-contract.md` |
| 不得改 `tasks.md` | ✅ | diff 的文件清单里**没有** `tasks.md` |
| 不得改 `data/mapping/*.json` 的内容 | ✅ | diff 里**没有**任何 `data/mapping/*` 的改动（生成物未被手改） |
| 生产代码只有一份（`tpops_kafka_collector/`） | ✅ | 本波次 13 个新增生产文件全部在 `tpops_kafka_collector/` 下；`tools/` 与 `data/` 无改动 |
| 生产代码不得 import `tools.*` / `data.*` | ✅ | `tests/unit/test_package_boundary.py` 全包 AST 扫描通过；本审查另行核对源码：**新增的 13 个生产文件 0 处** `import tools` / `import data`。`tests/unit/test_sqlgen.py` 里 `_generator_module()` 在 **sys.path 上临时加 `tools/`** 导入生成器 —— 这是**测试专用**，文档已注明「生产包不依赖 tools/」，不违反边界 |
| 生产代码不得读仓库根 `data/` 而不是包内 `mapping/data/` | ✅（本波次无新增） | 逐文件核对：本波次新增生产文件的路径常量全部由 `__file__` 派生指向**包内** `mapping/data/`（`sqlgen.PACKAGE_DATA_DIR` / `loader.PACKAGE_DATA_DIR`）。唯一读仓库根 `data/` 的是 `mapping/table.py::MappingTable.load()` —— **wave-2a 已登记为 T-10**，且生产路径不走它（`loader.load_mapping()` 走另一条装配路径）。**本波次无新增违例** |
| `tests/unit/test_package_boundary.py` 守得住吗 | ⚠️ 守 import 面，不守文件读取面 | 它是 **AST import 层面**的检查（docstring 自己写明了「不是文件读取层面的」）。它**守不住**「用 `open()` 读仓库根 `data/`」这类反向依赖。本波次无此类违例，但该测试的覆盖面应在 wave 4 补一条「生产包的路径常量必须落在包内」的断言 |
| 凭据只走环境变量 | ✅ | 本波次新增代码**零处**出现密码/token 字面量；`Config` 只从环境变量读；日志与异常消息里没有凭据 |
| 凭据不入日志 / 不入异常消息 | ✅ | 逐条核对新增的日志与异常消息：`_sql_label()` 只记 SQL 摘要；脱敏失败只记字段名不带 SQL 文本（`collectors/diagnostic.py::_sanitize` 明确「异常消息里不带 SQL 文本」）；`sanitize_sql` 只记长度不记内容 |
| 业务 SQL 不入日志 | ✅ | 脱敏模块只记长度；`_warn_if_near_limit` 只记字节数 |

### 7.3 行数核对（合同「单文件 < 400 行、函数 < 50 行」）

**生产文件（硬约束）**

| 文件 | 行数 | 判定 |
|---|---|---|
| `tpops_kafka_collector/collectors/command_center.py` | **463** | ❌ **超 63 行（I-1）** |
| `tpops_kafka_collector/tpops/command_center.py` | 399 | ✅（余量 1 行） |
| `tpops_kafka_collector/tpops/metrics.py` | 396 | ✅（余量 4 行） |
| `tpops_kafka_collector/collectors/monitor.py` | 382 | ✅ |
| `tpops_kafka_collector/collectors/alarm.py` | 346 | ✅ |
| `tpops_kafka_collector/collectors/diagnostic.py` | 268 | ✅ |
| `tpops_kafka_collector/mapping/sqlgen.py` | 264 | ✅ |
| `tpops_kafka_collector/tpops/diagnostic.py` | 252 | ✅ |
| `tpops_kafka_collector/collectors/instance.py` | 216 | ✅ |
| `tpops_kafka_collector/tpops/instances.py` | 203 | ✅ |
| `tpops_kafka_collector/mapping/masking.py` | 202 | ✅ |
| `tpops_kafka_collector/tpops/alarms.py` | 184 | ✅ |
| `tpops_kafka_collector/tpops/command_center_nodes.py` | 110 | ✅ |
| `tpops_kafka_collector/tpops/command_center_result.py` | 105 | ✅ |

**函数 > 50 行（硬约束）**：`collectors/instance.py::collect()` = **52 行**（M-1）；其余生产函数最长 `tpops/metrics.py::parse_series_points()` = 40 行。

**测试文件（按仓库既有惯例放宽，但需看清）**

| 文件 | 行数 |
|---|---|
| `tests/unit/test_command_center.py` | **676** |
| `tests/integration/test_collector_command_center.py` | **516** |
| `tests/integration/test_collector_alarm.py` | **485** |
| `tests/integration/test_collector_monitor.py` | **484** |
| `tests/integration/test_collector_diagnostic.py` | **452** |
| `tests/unit/test_metrics_window.py` | 397 |
| `tests/integration/test_collector_instance.py` | 344 |
| `tests/unit/test_sqlgen.py` | 245 |
| `tests/unit/test_masking.py` | 240 |

5 个测试文件超 400 行（M-11）。其中 `tests/unit/test_command_center.py`（676）与 `tests/integration/test_collector_command_center.py`（516）已明显过大，建议按「请求构造 / 三跳编排 / 重试与并发 / 归集与值相等」拆分。

---

## 8. 分级清单

### 8.1 Critical（3）

#### C-1 【Critical】E 类采集器用生产映射对象必崩 —— 全部告警不产出

- **位置**：`tpops_kafka_collector/collectors/alarm.py:224`（`mapping.alarm_definition(alarm_id)`）、`:236`（`mapping.metric_rule(...)`）
- **根因**：`collect_alarms` 需要的两个方法只存在于 `mapping/table.py::MappingTable`；**生产对象是 `load_mapping()` 返回的 `LoadedMapping`**（`mapping/loader.py:87`），它**没有** `alarm_definition`、也**没有** `metric_rule`。
- **独立复现**：

```
$ conda run -n tpops python /tmp/rev2b/alarm_wiring2.py
=== 生产对象 LoadedMapping ===
  !!! 调用失败: AttributeError 'LoadedMapping' object has no attribute 'alarm_definition'
```

（`hasattr(mapping,'alarm_definition')` → False；`hasattr(mapping,'metric_rule')` → False）

- **为什么测试没抓到**：`tests/integration/test_collector_alarm.py` 的 fixture 是
  `MappingTable(bundle.field_map, bundle.metric_rules, bundle.alarm_rules, {})`
  —— **手工拼的 `MappingTable`**，不是生产对象。**这是 2.9「从未被审过」的那批文件里的关键缺陷**。
- **影响**：wave 3 装配后 E 类通道 100% 抛 `AttributeError` → **全部告警一条都投不出去**，且是异常上抛（不是静默丢数），属于最好发现的一类；但当前**没有任何测试**用生产对象跑一遍 E 类。
- **修复指引**：
  1. 给 `LoadedMapping` 补两个只读访问器（与 `MappingTable` 同语义）：
     ```python
     # mapping/loader.py :: LoadedMapping
     def alarm_definition(self, alarm_id): ...
     def metric_rule(self, metric_code): ...
     ```
     （`alarm_definition` 的构造逻辑可从 `mapping/table.py:210` 原样搬过来；两处共用一个实现更好，避免再分叉。）
  2. **或者**把 `collectors/alarm.py` 改成直接读 `mapping.alarm_rules['definitions']`（`alarm_rules` 在 `LoadedMapping` 上是有的），不依赖 `MappingTable` 的方法。
  3. **必须补一条测试**：`tests/integration/test_collector_alarm.py` 里新增
     ```python
     def test_collector_works_with_the_production_mapping_object():
         mapping = load_mapping()          # 不是 MappingTable(...)
         result = collect_alarms(FakeClient({...}), mapping, nodes=[...], instance_id=...)
         assert result.messages
     ```
     并把现有 fixture 里的 `MappingTable(...)` 换成 `load_mapping()`（若 `MappingTable` 还需保留，则两条路径各测一次）。

#### C-2 【Critical】B 类 5 个 `counter_delta` 客户指标永远不产出消息

- **位置**：`tpops_kafka_collector/collectors/command_center.py::_fetch_pages` → `decode_merged(body, self._views)`（**缺 `now_ms`**）
- **根因**：`decode_merged(body, views, now_ms=0)` 的第三参默认 0 → 造出来的 `RuleContext.now_ms == 0` → `eval_counter_delta` 把 `ctx.now_ms` 传给 `deltas.rate(...)` → `elapsed = (0 - 0)/1000 = 0` → 恒走「采集时刻未推进，丢弃本轮差分」→ 值恒为 `None` → 采集器 `if value is None: continue` → **一条消息都不产出**。
- **冒烟枪**：`self._clock = clock if clock is not None else utc_now_ms` 在 `__init__` 里赋值后**全文再无使用**（`grep -n "_clock\|now_ms" collectors/command_center.py` 只有第 33、106 两行命中）。
- **独立复现（两轮采集，时钟推进 60 s，各 master 各自 +1000，真实速率 16.667/s）**：

```
$ conda run -n tpops python /tmp/rev2b/delta_cross2.py
R1 codes containing fetch_records: []
R1 total messages: 1700
R2 codes containing fetch_records: []
（其它 1700 条消息全部正常产出；5 个 *_records_per_second 指标两轮都是 0 条）
```

  日志侧证据（每轮刷屏）：`采集时刻未推进，丢弃本轮差分：metric=GAUSSDB.[{#DATNAME}]_fetch_records_per_second ... now=0 last=0`
- **影响**：5 个客户指标模板 × 9 个 `DATNAME` 维度 × 3 个 master ≈ **135 条消息/轮永不投递**；`GAUSSDB.[{#DATNAME}]_{fetch,insert,return,update,delete}_records_per_second` 全部为空。其余 97 个指标正常 → **从外面完全看不出异常**。
- **为什么测试没抓到**：期望值表里 `counter_delta` 的 `round1` 值就是 `null`（首轮无基线），比对 `None == None` 恒真；采集器层**没有第 2 轮**的断言（§5.2）。
- **修复指引**：
  1. 在 `_fetch_pages` 里把采集时刻传进去：`now_ms = int(self._clock())` → `decode_merged(body, self._views, now_ms)`；`_merge` 里的 `merge_pages(pages, self._views)` 同样要传 `now_ms`。
  2. `self._clock` 由此不再是无用属性（可加一条断言「`decode_merged` 收到的 `now_ms` 非 0」）。
  3. **补一条两轮回归**（这是本波次最缺的一类测试）：
     ```python
     def test_counter_delta_produces_a_rate_on_the_second_round(...):
         # 造一个时钟可控、每 master 各自 +1000 的假命令中心
         # 第 1 轮：不产出（无基线）；第 2 轮：每个 master 各产出一条，值 == 1000/60
     ```
     注意这条测试会连带暴露 C-3，两条必须一起修。

#### C-3 【Critical】B 类差分基线跨 master 共享 → 3 个 master 的 counter_delta 互相污染

- **位置**：`tpops_kafka_collector/collectors/command_center.py` —— `self._deltas`（单个 `CounterDeltaStore`）在 `_build_messages` 里被**所有 master 共用**：
  ```python
  for node_id, contexts in merged_by_node:
      for rule in rules:
          values[rule.metric_code] = evaluate(rule, ctx, self._deltas)   # ← 同一个 store
  ```
- **根因**：`CounterDeltaStore` 的基线键是 `(metric_code, dimension_key(dimensions))`（`mapping/rules_deltas.py:36`），**不含 node_id** → master A 的基线会被 master B 覆盖。
- **独立复现（在 `now_ms` 正确传递的前提下）**：

```
$ conda run -n tpops python /tmp/rev2b/delta_cross3.py
--- 共享一个 CounterDeltaStore（= 当前 2.6 的做法） ---
   轮次1 节点..._0_0  值=16.666666666666668
   轮次1 节点..._0_1  值=None
   轮次1 节点..._0_2  值=None
--- 每个 master 各自一个 store（应有行为） ---
   轮次1 节点..._0_0  值=16.666666666666668
   轮次1 节点..._0_1  值=16.666666666666668
   轮次1 节点..._0_2  值=16.666666666666668
真实速率 = 1000/60 = 16.6667/s
```

  日志：`采集时刻未推进，丢弃本轮差分：... now=1060000 last=1060000`（A 的基线时间戳被 B/C 顶掉）。
- **影响**：`node_scoped=true` 的 5 个 `counter_delta` 指标按定义**每个 master 一份**，但基线共享后 **2/3 的 master 恒为 `None`**；若节点顺序变化（`outcome.tasks` 的顺序由平台给），还可能算出「A 的当前值 − B 的上轮值」这种**跨节点速率**（数值看着合法、实则错误）。
- **注意**：留档里 `stat_database` 的累计计数**逐 master 不同**（如 `tup_fetched` 1394504 / 1392705 / …），所以这不是理论问题。
- **修复指引**：让差分基线**按节点隔离**。两种做法：
  1. **推荐**：把 `CounterDeltaStore` 的键扩展为 `(metric_code, dims, node_id)`（`rules_deltas.rate()` 多收一个 `scope` 参数），由采集器传 `node_id`；
  2. 或采集器为每个 master 维护**独立**的 `CounterDeltaStore`（`self._deltas` 改成 `Dict[node_id, CounterDeltaStore]`）。后者改动面小，但要注意基线存储的生命周期（要跨轮持久化，不能每轮新建 → 需要在 `CounterDeltaStore` 之外做一层持久化，wave 3 的水位/基线持久化要一起考虑）。
  - 无论哪种，都要**配套一条「3 个 master 各自算出自己的速率」的回归**（C-2 的那条测试已覆盖）。

### 8.2 Important（4）

#### I-1 【Important】`collectors/command_center.py` 463 行，越过合同「单文件 < 400 行」

- 合同 `Design Constraints → 依赖约束`：「单文件 < 400 行、函数 < 50 行」——这是**硬约束**（wave-1 的审查曾因 `client.py` 403 行、`watermark.py` 404 行开出 M-1 并已修）。
- 实测 **463 行**（超 63 行）。
- **修复指引**：把与「取数 / 分页」无关的部分拆出去。天然切口：
  - `_page_signature` / `_same_as_previous` / `_approx_size_bytes` / `_is_output_limit_error` / `_has_more` / `_tier_of` / `_command_center_rules` / `_merged_views` → 新文件 `collectors/command_center_paging.py`（约 −90 行）；
  - `CommandCenterCollectResult` 数据类 → `collectors/command_center_result.py`（与 `tpops/command_center_result.py` 同名不同层，注意命名区分）。
  - 拆完两个文件都应 < 300 行。

#### I-2 【Important】2.6 的「值等于期望值表」守卫只覆盖 47/82，35 个指标码被 `continue` 静默跳过

- 位置：`tests/integration/test_collector_command_center.py::test_collected_values_equal_the_expected_table`
- 实测：`checked = 113`，但**被比到的 `metric_code` 只有 47 个**；**35 个维度展开型指标码**（完整清单见 §5.2）因「模板码 vs 替换后码」的键不匹配被 `continue` 跳过。
- 后果：采集器层的「占位符替换 → 维度展开 → 消息 `metricCode`/`tags.dimension_values`」这一段**没有值相等守卫**；回归层（wave 0）只在映射层守。
- **修复指引**：
  1. 在测试里按**模板码**聚合 `produced`（用 `rule.placeholders` / `dimension_bindings` 把消息里的替换后码还原，或直接按 `rule.metric_code` + `rule.dimension_bindings` 建 `(模板码, 维度 JSON) → 值` 索引）；
  2. 断言从 `checked > 0` 收紧为 **`checked == 期望值表里 (指标, 节点, 维度) 的总条数`**（当前期望值表 82 指标 × 3 节点 × 维度条目数）；
  3. 再加一条**反向断言**：`set(被比到的指标码) == set(82 条 B 类规则)`。

#### I-3 【Important】`_master_nodes` 静默吞掉所有异常且不记日志

- 位置：`tpops_kafka_collector/collectors/command_center.py:337-345`
  ```python
  def _master_nodes(self, instance_id):
      getter = getattr(self._cc, 'master_nodes', None)
      if getter is None:
          return ()
      try:
          return tuple(getter(instance_id))
      except Exception:  # noqa: B902 —— 节点清单取不到不该毁掉整轮采集
          return ()
  ```
- 违反合同的「错误显式处理：…**不静默吞异常**」与「空结果要记日志」。
- 后果：节点清单取不到（平台报错 / 清单里没有 master → `MasterNodeRegistry` 抛 `ProtocolError`）时：
  - `node_scoped=true` 的指标：`resource_context` 有 `node_id` 但**没有 `host_ip`** → `resolve_node_context` 查不到节点 → `resourceName` 退化成 `_<node_id>`（**资源名指错**）；
  - `node_scoped=false` 的指标：`_host_node_id` 退回「结果的第一个节点」，而资源名走 `resolve_node_context(nodes=())` 的兜底 → 名字与值不同源；
  - **全程无一条日志** —— 这正是「不报错但数据错」的形态。
- **修复指引**：
  ```python
  try:
      return tuple(getter(instance_id))
  except Exception as exc:
      LOGGER.error('命令中心节点清单取不到（实例 %s）：%s', instance_id, exc)
      return ()
  ```
  并补一条测试：让 `master_nodes` 抛异常 → 断言日志里有 ERROR。

#### I-4 【Important】合同完成标准「98 个可提供指标全部产出数据」不成立

- 合同 Wave 2b 完成标准第 1 条。实测：
  - 映射层：98 条可提供指标（82 B + 11 A + 4 C + 1 D）的**值**都能算出来（回归层 225 passed）；
  - **采集器层**：B 类 5 个 `counter_delta` 指标产出 **0 条**（C-2）；E 类采集器用生产映射对象**直接崩**（C-1）；其余通道（A/C/D）正常。
- 另：`tests/regression/` 与 `tests/test_expected_values.py` **只测映射层**，没有任何一条测试走「采集器 → 消息」的全链路值相等 → 合同这条完成标准**目前没有任何守卫**。
- **修复指引**：
  1. 修完 C-1 / C-2 / C-3；
  2. 在 `tests/regression/` 增补一条**采集器级**的值相等回归（覆盖 82 条 B 类 + A/C/D 的 16 条），断言「98 个可提供指标全部产出消息」；
  3. 把「3 个不可提供指标无消息且丢弃计数递增」的**丢弃计数**落到 wave 3 的每轮汇总日志（见 T-18）。

### 8.3 Minor（12）

| # | 内容 | 位置 | 处置建议 |
|---|---|---|---|
| M-1 | `collect()` 52 行 > 合同「函数 < 50 行」 | `collectors/instance.py:121` | 抽 `_build_messages(rules, instance, now_ms)` 出去 |
| M-2 | 告警分页在 `total_count` 为 0/缺失时**只取第一页** | `tpops/alarms.py::fetch_all_alarm_records`（`if len(collected) >= page.total_count: break`） | 改为 `if page.total_count > 0 and len(collected) >= page.total_count: break`；并保留「不满一页即停」的兜底 |
| M-3 | 坑 4 的兜底路径：值来自别的 master、资源名仍写第一个 master CN | `collectors/command_center.py::_build_messages` 的 WARNING 分支 | 兜底时**同步改资源上下文**（让资源名跟着值走），或该轮不产出 `node_scoped=false` 的消息。当前 40 条该类指标均为 `master_view: identical` → **无数据影响**，但结构上未闭合 |
| M-4 | `CMD_PAGE_SIZE` **没有接到采集器**（`sqlgen.render_sql_with_config` 无生产调用方；采集器用的是 `merged_sql.json` 的 `default_page_size=100`） | `collectors/command_center.py::__init__` | wave 3 装配时传 `page_size=config.cmd_page_size`，或给采集器加 `from_config` |
| M-5 | 超限缩小页容量后，结果里报的 `page_size` 仍是**缩小前**的值 | `collectors/command_center.py::collect` | 用 `self._page_size`（已被更新）回报 |
| M-6 | 脱敏把**双引号标识符里的数字**也脱敏（`"col 1"` → `"col ?"`） | `mapping/masking.py::_TOKEN_RE` 的 `number` 分支 | 给 `number` 加一条「不在双引号标识符内」的约束，或先整体匹配双引号标识符 token |
| M-7 | 死代码 | `tpops/instances.py::_as_dict`、`tpops/diagnostic.py::collection_window_ms` | 删除（或改为被调用） |
| M-8 | C 类采集器二次取 `self._clock()`，与 `collect()` 的 `now_ms` 可能不一致；C 类窗口写死在 `tpops/diagnostic.py::COLLECTION_WINDOWS`（非表驱动） | `collectors/diagnostic.py::_fetch_collections` | 把 `now_ms` 传进 `_fetch_collections`；窗口改由映射表声明 |
| M-9 | 「平台不认 offset」守卫只看**相邻两页**；理论上存在交替页（A,B,A,B…）导致无限翻页 | `collectors/command_center.py::_same_as_previous` | 改成「与**已见过的所有页**指纹比对」或加最大翻页次数 |
| M-10 | 「采集器不推进水位」的断言**不可证伪**（`not hasattr(collector,'watermark')`），且只有 A 类有 | `tests/integration/test_collector_monitor.py` | 改成行为断言（如「采集器不持有任何可写的状态存储」或注入一个会记录写入的假水位存储并断言零写入），并给 B/C/D/E 补同类断言 |
| M-11 | 5 个测试文件超 400 行（676 / 516 / 485 / 484 / 452） | `tests/` | 按主题拆分（尤其 `test_command_center.py` 与 `test_collector_command_center.py`） |
| M-12 | 规格与实现口径不一致：规格「本轮只拉取 `first_alarm_time` 晚于 T 的记录」vs 实现（`first_alarm_time` / `last_alarm_time` / `clear_alarm_time` **任一** > T 即算新增或变化） | `specs/collection-channels/spec.md:166,177` vs `collectors/alarm.py::_is_new_or_changed` | **实现更安全且必需**（合同关键场景 3：恢复事件不能被去重丢弃；恢复后 `first_alarm_time` 不变）→ 改**规格文字**，登记为 T-15（wave 4 前落地） |

---

## 9. 待补清单（登记进合同，但本次不改 `tasks.md`）

> 与 wave-0 / wave-1 / wave-2a 的台账同格式。**改动 `tasks.md` 会让执行计划 stale → 已有 receipt 全部作废**，因此本次只登记。

| 编号 | 内容 | 落地时机 |
|---|---|---|
| T-15 | **规格改写**：历史告警增量的判定字段（`first_alarm_time` → 「`first_alarm_time` / `last_alarm_time` / `clear_alarm_time` 任一晚于水位」），以及「只取增量」场景的措辞 | **wave 4 交付文档前**（与 T-6 同批） |
| T-16 | **回归层补采集器级用例**：`tests/regression/` 需要一条走「采集器 → 消息」全链路、覆盖 82 条 B 类 + A/C/D 16 条的值相等回归；现有回归只覆盖映射层 | wave 3 任务 3.4 / 3.5 |
| T-17 | **`CMD_PAGE_SIZE` 接到采集器**：`sqlgen.render_sql_with_config` 目前无生产调用方，采集器用的是 `merged_sql.json` 的 `default_page_size` | wave 3 装配（任务 3.3） |
| T-18 | **3 个不可提供指标的「丢弃计数」**：当前无任何计数机制（`enabled=false` 只保证不产出消息） | wave 3 每轮汇总日志（任务 3.1） |
| T-19 | **告警分页 `total_count` 的健壮性**：`total_count ≤ 0` 时不据此判结束（当前会只取第一页） | 收尾后的独立小变更（或随 C-1 一并修） |
| T-20 | **包边界测试补「文件读取面」**：`tests/unit/test_package_boundary.py` 只查 import，查不住「生产代码读仓库根 `data/`」；应补一条「生产包的路径常量必须落在包内」的断言 | 收尾后的独立小变更 |
| T-21 | **`mapping/table.py::MappingTable.load()` 的仓库根依赖定案**（wave-2a 的 T-10 延续）：本波次核对确认**无新增**违例，但该入口仍在 | 同 T-10 |

---

## 10. 附：本审查做过的聚焦检查（风险 → 查了什么）

> 审查原则：只在评估一个**能具体命名的风险**时才看 diff 之外的代码，每个风险做一次聚焦检查。

| 风险 | 查了什么 | 结果 |
|---|---|---|
| 采集器有没有走「合并形态当行列表用」的路径 | 读 `mapping/decode.py::decode_merged` / `merge_pages` 全文 | 未走；两道闸（`_has_more` 只看 `paged` 视图、`merge_pages` 只拼 `paged` 视图）都在且非恒真 |
| `node_scoped=false` 的值与资源名是否同源 | 读 `mapping/nodes.py::resolve_node_context` / `select_master_node`；构造「承载 master 未回结果」的形态实跑 | 主路径同源；兜底路径错配（M-3） |
| 2.9 会不会只取第一页 | 读 `tpops/alarms.py::fetch_all_alarm_records`；用假客户端跑 `total_count=250/0/None` 三种 | 250 → 3 页取满；0/None → **只取第一页**（M-2） |
| 期望值表是否可信 | 读 `data/merged_sql/sql_merge_probe_result.json` 原始行 + `data/mapping/metric_rules.json` 声明，**自己重算** 13 个指标 × 3 节点 | 39/39 一致，含非零值 |
| 生产包有没有反向依赖 | 读 `tests/unit/test_package_boundary.py`；`grep` 生产包内 `REPO_ROOT` / `data` / `tools` | 无 import 反向依赖；无新增文件读取反向依赖 |
| E 类采集器能不能用生产映射装配 | 读 `mapping/loader.py::LoadedMapping` 与 `mapping/table.py::MappingTable` 的 API 面；实跑 | **不能**（C-1） |
| counter_delta 能不能产出值 | 读 `mapping/rules_deltas.py` / `rules_cross.py::eval_counter_delta`；两轮多 master 实跑 | **不能**（C-2）；基线跨 master 污染（C-3） |
| 3.7 语法是否越线 | `conda run -n tpops python -V` → 3.7.9；对 13 个生产文件 + 9 个测试文件做正则扫描 | 0 处命中 |
| 行数/函数长度 | `wc -l` + AST 逐函数测量（3.7 无 `end_lineno`，改用缩进边界测量） | 1 个生产文件超 400 行、1 个函数超 50 行 |
| 脱敏边界 | 对 16 条 SQL 形态逐条跑 `mask_sql_literals` | 1 处过脱敏（M-6），其余符合文档 |
| 合并 SQL 渲染与生成器同义 | 读 `mapping/sqlgen.py`（wave-2a 已审）+ `merged_sql.json.paging` 与 `response_rules.json.merged_views` 的 `paged` 集合 | 两处一致 |

---

## 11. 放行建议

1. **必须先修**：C-1、C-2、C-3（三条互相牵连，建议一次性改完并补「两轮 × 三 master」的回归）。
2. **必须先修**：I-1（拆文件，硬约束）、I-2（补齐 82/82 的采集器级值相等守卫）、I-3（记日志）、I-4（补采集器级回归）。
3. 修完请重跑 §3 的全部命令，并把 **C-2 / C-3 的两轮多 master 回归**与 **C-1 的生产映射对象回归**作为新增的证明材料附上。
4. Minor 12 条：M-1 / M-3 / M-4 建议随修复一并处理；其余可进待补清单。**M-12 必须改规格**（登记为 T-15，wave 4 前落地）。
5. 本波次的 review receipt 由**控制器**签发；本报告不签发 receipt，未修改任何被审代码、`execution-contract.md`、`tasks.md` 或 `.spec-superflow.yaml`。
