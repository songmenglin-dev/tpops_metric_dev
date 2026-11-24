# Wave 3 定向复审报告（维度：守卫与硬约束）—— wave-3-scheduling-testing

- **base** `3a71cb6` / **head** `2305cea`
- **审查方**：R2-B（定向复审，只核验守卫缺失与硬约束是否真闭环）
- **结论**：**verdict = fail**
- **计数**：**Critical 0 / Important 2 / Minor 4**

> **为什么是 fail**：B1、B2、C1 三条**真闭环**（变异后必红，实测已验证）。
> 但 B3 里的「跑了但全挂 → `/readyz` 503」这一条**只闭环了一半**：
> `_scheduler_state` 的**陈旧分支**有守卫（变异变红），但
> **`TrackedScheduler.tick` 里「本轮全失败就不算成功」的那道闸门仍然零守卫** ——
> 把它拆掉，**1683 passed，无一测试变红**，而 `/readyz` 会从 503 变成 200
> （采集端全挂却报「就绪」）。这正是本项目最贵的那一类缺陷形态
> 「不报错、但数据没有 / 数据错」。
> 另有一条 Important：`/readyz` 503 的端到端守卫**被 tpops 探针遮蔽**
> （FakeClient 从未认证 → tpops 探针本来就不就绪 → 无论 scheduler 探针怎么坏，
> 断言都成立）—— 这条守卫**恒绿**。

---

## 零、我自己的验证基线（不采信实施方报告）

沙箱 `/tmp/r2b-mut`（工作树副本 + `data/` `tools/` `docs/` + 客户指标 xlsx，
缺 xlsx/docs 会产生 6 个 collection error 与 2 个 failed —— 那是拷贝产物，不是缺陷）。

```
$ cd /tmp/r2b-mut && conda run -n tpops python -m pytest -q
1683 passed in 59.92s        # python 3.7.9-final-0
```

**首轮报告的基线是 1561 passed，本轮 1683 passed —— 净增 122 条新测试。**

```
$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
154 passed in 1.30s

$ conda run -n tpops python -V
Python 3.7.9
```

**变异纪律**：每个变异 = 改一处 → 跑**全量**（或该守卫所在文件的定向集）→
记录红了哪几条 → **还原** → 重跑全量确认恢复绿。收尾时
`diff -r` 确认沙箱与工作树**逐字节一致**（`SANDBOX_RESTORED`），工作树
`git status --porcelain` 只有控制器改的 `progress.md` 与另一名复审方的报告，
生产代码与测试目录 `git diff --stat` **为空**（零污染、零 commit）。

---

## 一、逐条核验

### B1. 告警通道「投递失败不推进告警水位」有守卫了 —— **已闭环** ✅

**代码位置**

- 生产：`tpops_kafka_collector/scheduler_delivery.py:133-152`
  （`_deliver_alarms` 的 `except` 分支只 `LOGGER.exception` 后 `return`，
  `self._advance_alarm(...)` 在 `:151` —— **成功之后**才执行）
- 守卫：`tests/unit/test_scheduler_alarm_guards.py:58`
  `test_alarm_watermark_does_not_advance_when_alarm_delivery_fails`
  与 `:77` `test_failed_alarm_batch_is_retried_in_the_next_round`

**① 守卫真能抓住（变异 M11）**

我把 `_advance_alarm` 挪进 `_deliver_alarms` 的 `except` 分支（即「投递失败也推进告警水位」）：

```python
# M11 注入（scheduler_delivery.py）
            self._advance_alarm(instance_id, outcome)   # <-- 挪进 except
            return (
                DeliveryStats(planned=len(records), failed=len(records)),
                TaskFailure(channel, instance_id, type(exc).__name__, reason(exc)),
            )
```

全量结果：

```
FAILED tests/unit/test_scheduler_alarm_guards.py::test_alarm_watermark_does_not_advance_when_alarm_delivery_fails
E       AssertionError: 告警投递失败却推进了告警水位 —— 从未投出的告警会被下一轮判成「不新」
E       assert '1000' is None
E        +  where '1000' = ...alarm_watermark(...)('inst-a:alarm')
1 failed, 1682 passed in 60.29s
```

**守得住。**

**② 没有别的测试把它「救活」**

M11 下**只有那一条守卫红**（1 failed / 1682 passed）—— 不存在「别的测试掩盖变红」的假绿。
且这条守卫与首轮的 M11（1561 passed 无一红）形成直接对照。

**③ 断言的是真实行为，不是 mock 调用次数**

```python
assert result.failed == 1, '投递失败没有进失败清单'
assert store.alarm_watermark(alarm_stream(INSTANCE_A)) is None, (...)
```
断言的是 **`WatermarkStore` 里真实的水位状态**（`store.alarm_watermark(...)`），
不是 `producer.produce_alarms.call_count`。
`store` 是真的 `WatermarkStore`（落磁盘），不是 mock。

**④ 改回去后全量恢复绿**

还原后：`1683 passed in 61.17s` ✅

**⑤ 第二条守卫（下一轮重发）也验了**

`test_failed_alarm_batch_is_retried_in_the_next_round` 断言
`len(producer.alarm_batches) == 2` 且 `alarm_watermark(...) == '1000'` ——
「失败的那批**重发了**」+「重发成功后才推进水位」，两条都在。

---

### B2. 告警幂等去重（批内 `seen` + 跨轮 `has_key`）有守卫了 —— **已闭环** ✅

**代码位置**

- 生产：`tpops_kafka_collector/scheduler_delivery.py:121-130`
  （`seen` 批内去重 + `self._watermark.has_key(key)` 跨轮去重）
- 守卫：`tests/unit/test_scheduler_alarm_guards.py:99`
  `test_duplicate_alarm_keys_inside_one_batch_are_delivered_once`（批内 `seen`）
  与 `:119` `test_already_delivered_alarms_are_skipped_in_the_next_round`（跨轮 `has_key`）

**① 注入 M14（删掉这两道去重）→ 两条守卫都红**

```python
# M14 注入：seen 与 has_key 两道一起去掉
        for item in outcome.alarms:
            key = item.idempotency_key
            records.append(alarm_record(item.message))
```

全量：

```
FAILED tests/unit/test_scheduler_alarm_guards.py::test_duplicate_alarm_keys_inside_one_batch_are_delivered_once
FAILED tests/unit/test_scheduler_alarm_guards.py::test_already_delivered_alarms_are_skipped_in_the_next_round
E       AssertionError: 重复投递了告警
E       assert 2 == 1
2 failed, 1681 passed in 61.09s
```

**两道去重都被钉住了**，与首轮的 M14（1561 passed 无一红）形成直接对照。

**② 分别单独拆掉一道 —— 各自都有专属守卫（不是一只守卫覆盖两只）**

只删 `seen`（保留 `has_key`）：

```
MUT M14a: only 'seen' removed
FAILED tests/unit/test_scheduler_alarm_guards.py::test_duplicate_alarm_keys_inside_one_batch_are_delivered_once
E       AssertionError: 同一批里相同幂等键的告警投了多条 —— 去重（批内 seen）没了
1 failed, 3 passed
```

只删 `has_key`（保留 `seen`）：

```
MUT M14b: only 'has_key' removed
FAILED tests/unit/test_scheduler_alarm_guards.py::test_already_delivered_alarms_are_skipped_in_the_next_round
E       AssertionError: 重复投递了告警
E       assert 2 == 1
1 failed, 3 passed
```

**两道去重各自独立可辨 —— 不是靠一条断言同时碰巧覆盖。**

**③ 改回去后全量恢复绿**：`1683 passed` ✅

---

### B3. 三条「保命」的异常分支有守卫了 —— **部分闭环** ⚠️

#### B3-① `lifecycle._tick_safely`（一轮采集抛异常 → 进程继续跑）—— **已闭环** ✅

- 生产：`tpops_kafka_collector/lifecycle.py:197-202`
- 守卫：`tests/unit/test_lifecycle.py:264` `test_a_failing_round_does_not_stop_the_loop`

**变异验证（去掉 try/except）**

```python
# M_B3_1 注入
def _tick_safely(scheduler):
    scheduler.tick()          # <-- try/except 整个去掉
```

全量：

```
FAILED tests/unit/test_lifecycle.py::test_a_failing_round_does_not_stop_the_loop
1 failed, 1682 passed in 69.06s
```

**特别确认「进程真的不会被一轮采集的异常带走」** —— 断言不是 mock 调用次数：

```python
assert ticks == [1, 2, 3], '一轮失败就把主循环带走了：%r' % (ticks,)
assert '本轮调度异常' in caplog.text, caplog.text
```

- `ticks` 是**假调度器真实被调用的次数**（不是 `mock.call_count`）—— 变异后
  实际输出里能看到 `Exception in thread scheduler-tick` 打到 stderr，
  说明异常确实逸出了；`ticks == [1,2,3]` 断言的是**主循环继续跑了三轮**这个行为。
- 变异后红的是**第二条**（日志断言）—— `ticks` 那条仍成立，
  因为「异常逸出 worker 线程」不等于「主循环停了」（worker 是守护线程）。
  两条断言合起来覆盖了「异常被兜住 + 主循环继续」。

再拆一层（**只删日志、保留 try/except**）：

```
MUT F: keep try/except, remove only the log
FAILED tests/unit/test_lifecycle.py::test_a_failing_round_does_not_stop_the_loop
1 failed, 4 passed
```

**兜住**与**记日志**两件事分别被钉住 ✅

#### B3-② `main.py` 装配失败 → 退出码 3 + 堆栈 —— **已闭环** ✅

- 生产：`tpops_kafka_collector/main.py:306-310`
- 守卫：`tests/integration/test_main.py:755`
  `test_startup_error_returns_exit_code_three_with_a_stack_trace`

**变异（退出码改 0）**

```
M_B3_2 injected: EXIT_STARTUP_ERROR -> EXIT_OK
FAILED tests/integration/test_main.py::test_startup_fails_when_no_instance_has_a_node_list
FAILED tests/integration/test_main.py::test_startup_error_returns_exit_code_three_with_a_stack_trace
2 failed, 1681 passed
```

**变异（退出码不变、只把 `LOGGER.exception` 换成 `LOGGER.error`，去掉堆栈）**

```
M_B3_3 injected: LOGGER.exception -> LOGGER.error (no stack trace)
FAILED tests/integration/test_main.py::test_startup_error_returns_exit_code_three_with_a_stack_trace
E       AssertionError: 装配失败的日志必须带堆栈
1 failed, 1682 passed
```

守卫断言的是 `record.exc_info is not None` —— **真的堆栈**，不是日志文案 ✅

#### B3-③ `main.py` 的 `_tpops_state` / `_scheduler_state` 分支 —— **部分闭环** ⚠️

**已闭环的部分：`_tpops_state` 的两条失败分支**

- 生产：`tpops_kafka_collector/main.py:271-274`
- 守卫：`tests/integration/test_main.py:774`（认证失败）、`:785`（尚未认证）

```
M_B3_4 injected: _tpops_state both failure branches -> True
FAILED tests/integration/test_main.py::test_tpops_probe_reports_an_authentication_failure
FAILED tests/integration/test_main.py::test_tpops_probe_reports_not_authenticated_yet
2 failed, 1681 passed
```

**已闭环的部分：`_scheduler_state` 的「陈旧」分支**

- 生产：`tpops_kafka_collector/main.py:283-284`
- 守卫：`tests/integration/test_main.py:796` `test_scheduler_probe_reports_a_stale_last_success`

```
M_B3_5 injected: _scheduler_state stale -> True
FAILED tests/integration/test_main.py::test_scheduler_probe_reports_a_stale_last_success
1 failed, 1682 passed
```

**未闭环的部分 —— 两条 Important：**

**【Important 1】`TrackedScheduler.tick` 的「本轮全失败就不算成功」这道闸门零守卫**

- 生产：`tpops_kafka_collector/lifecycle.py:87-91`

```python
def tick(self) -> TickResult:
    result = super().tick()
    if result.tasks_failed == 0:          # <-- 这道闸门没有任何守卫
        self._last_tick_ok_at = self._wall_clock()
    return result
```

它是「**跑了但全挂 → `/readyz` 503**」这条语义的**唯一实现点**：
`tasks_failed > 0` → `_last_tick_ok_at` 保持 `None`（或旧值）→
`_scheduler_state` 判不就绪 → 503。

**变异验证（把闸门拆掉，全失败也记成功）**

```python
# MUT E
        result = super().tick()
        self._last_tick_ok_at = self._wall_clock()   # <-- 闸门拆掉
        return result
```

**全量结果：**

```
1683 passed in 56.04s
```

**无一测试变红。**

我用独立探针确认了这条变异**真的改变了语义**（不是无害重排）：

```
# 干净树
tasks_failed = 1
last_tick_ok_at = None   <-- None means NOT ready (correct)

# MUT E 之后
tasks_failed = 1
last_tick_ok_at = 1795492813.159759   <-- 记成成功 → /readyz 报 200
```

**后果**：采集端**每一轮全挂**（平台挂了 / 认证挂了 / SQL 全失败），
`/readyz` 依然返回 **200「就绪」** —— 客户侧的编排/K8s 探针会认为采集端健康，
**一条数据都没有**且**没有任何信号**。这与首轮 A3/A4 是同一类
「不报错、但数据没有」。

首轮报告明确要求 B3 覆盖「**跑了但全挂**」这条语义，
修复方补了 `test_readiness_returns_503_when_the_last_round_failed`，
但那条守卫**没有覆盖到闸门本身**（见 Important 2）。

**【Important 2】`/readyz` 503 的端到端守卫被 tpops 探针遮蔽 —— 恒绿**

- 守卫：`tests/integration/test_main.py:807` `test_readiness_returns_503_when_the_last_round_failed`

```python
status, body = runtime.health.endpoints.readiness()
assert status == 503
assert b'"name": "scheduler"' in body or b'"name":"scheduler"' in body
```

它只断言**整体 503** + body 里出现 scheduler 的名字 ——
**不区分 503 是哪个探针贡献的**。

而该测试用的 `FakeClient`（`tests/integration/test_main.py:48`）**没有**
`auth_ok`、没有 `_token`、没有 `_auth_failed_at` → `_tpops_state`
（`main.py:268-275`）必然走「TPOPS 尚未完成认证」分支 → **tpops 探针本来就不就绪**。

**实测证据**：

```
# MUT B3_5 生效（_scheduler_state 陈旧分支 → True）时，这条守卫仍然 PASS
$ conda run -n tpops python -m pytest -q tests/integration/test_main.py
FAILED tests/integration/test_main.py::test_scheduler_probe_reports_a_stale_last_success
1 failed, 23 passed        <-- test_readiness_returns_503_when_the_last_round_failed 仍然 passed
```

再把 `FakeClient` 补上 `auth_ok`（让 tpops 探针就绪）、**scheduler 探针仍变异**：

```
MUT: FakeClient now authenticated (tpops probe ready); scheduler probe still mutated to True
E       AssertionError: assert 200 == 503
FAILED tests/integration/test_main.py::test_readiness_returns_503_when_the_last_round_failed
```

反过来，把 `FakeClient` 补上 `auth_ok`、**scheduler 探针保持正确**：

```
MUT M: FakeClient authenticated, scheduler probe INTACT
24 passed
```

**结论**：这条 503 端到端守卫的 503 **实际上来自 tpops 探针**，
scheduler 探针坏成什么样它都绿。它是**恒绿的**。

---

### C1. `platform_profile.reconcile` 拆到 < 50 行，且行为不变 —— **已闭环** ✅

**① 我自己用 AST 扫全包（不采信实施方数字）**

脚本按「函数体第一条语句 → 最后一条语句的行号」算 body span，
并同时统计「含签名与装饰器的 span」：

```
$ conda run -n tpops python /tmp/ast_scan.py
=== FUNCTIONS with body span > 50 ===
count: 0

=== TOP 15 by body span ===
body=42 span=49 tpops_kafka_collector/collectors/monitor.py:166-214 _messages_of_rule
body=39 span=48 tpops_kafka_collector/collectors/command_center.py:195-242 _fetch_pages
body=38 span=43 tpops_kafka_collector/tpops/client.py:189-231 _authenticate
body=38 span=41 tpops_kafka_collector/scheduler_delivery.py:71-111 _deliver_metrics
body=35 span=38 tpops_kafka_collector/scheduler_delivery.py:115-152 _deliver_alarms
...

=== FILES >= 400 lines ===
count: 0
```

**全包 0 个函数 body > 50 行、0 个文件 ≥ 400 行 —— 硬约束达标。**
（最大者是 `collectors/monitor.py:_messages_of_rule`，body 42 / span 49，两个口径都在 50 以内。）

**`reconcile` 拆分前后对比（同一脚本、同一口径）**

| | 行号 | body span | 含签名 span |
|---|---|---|---|
| base `3a71cb6` | `243-293` | **44** | **51** |
| head `2305cea` | `244-273` | **23** | **30** |

拆出的 4 个私有函数（head 实测 body span）：

```
_compare_instance       body=10  span 281-301
_compare_engine         body=10  span 305-315
_compare_master_count   body=12  span 318-330
_scan_added             body=12  span 333-349
```

> 注：实施方报告称「`_compare_instance` 22 / `_compare_engine` 11 /
> `_compare_master_count` 13 / `_scan_added` 17」—— 那是**含 docstring 与空行**的
> 口径（`281-301` 共 21 行、`305-315` 共 11、`318-330` 共 13、`333-349` 共 17）。
> 两种口径都不越线，**数字口径的差异不影响结论**。

**② 行为不变 + 拆出的私有函数被真覆盖**

覆盖率（`--cov-report=term-missing`）：

```
tpops_kafka_collector/platform_profile.py    155 stmts   10 miss   94%
Missing: 92, 95, 117, 148, 151, 156, 171-172, 359, 375
```

**未覆盖行全部落在 92~375 之间，但 `281-349`（4 个拆出的私有函数）一个未覆盖行都没有**
→ 4 个私有函数 **100% 被覆盖** ✅

**③ 变异：改坏每个拆出的私有函数 → 现有守卫必须变红**

| 变异 | 改了什么 | 红了哪条 | 结果 |
|---|---|---|---|
| `cmp_instance` | `return True, False, False` → `return False, False, False`（缺失实例不再报缺失） | `test_profile_instance_missing_on_platform_logs_error` | ✅ 红 |
| `cmp_engine` | `if current_engine == item.engine_name` → `if True`（引擎名变化不报） | `test_engine_name_change_logs_warning` | ✅ 红 |
| `cmp_master` | `if current_master == item.master_count` → `if True`（master 数变化不报） | `test_master_count_change_logs_warning` | ✅ 红 |
| `scan_added` | `added = []` → `return []`（新增实例不报） | `test_platform_instance_missing_from_profile_logs_warning` | ✅ 红 |

每条都是**恰好一条**守卫变红、其余 14 条通过 —— 说明拆分**没有引入未被测到的分支**，
每个私有函数的每条分支都有专属守卫。

**④ 顺带修的 Minor（M1）：`platform_profile.py` 的 docstring**

```python
# head 2305cea，platform_profile.py:193-199
        **固化文件存在 → 不重新发现、不改写**（画像以文件为准）：
        ...
        :param reconcile: 是否在读固化画像后**额外调一次**平台列表做核对
            （生产装配传 ``True``；**只记日志，不改画像文件**）。
            不传（``False``）时**平台列表接口一次都不调用**。
```

与实现**一致** ✅ —— 关键是把「一次都不调用」限定在 `reconcile=False` 上，
并显式说明生产装配（`main.py:246` 传 `reconcile=True`）会调一次。
配套守卫 `test_reconcile_calls_the_platform_list_but_does_not_rewrite_the_file`
（`tests/unit/test_platform_profile.py:172`）把这条口径钉住了。

---

## 二、新增测试的质量

### 恒绿守卫筛查

我对修复轮新增/修改的测试文件逐个做了变异，**结果如下**：

| 守卫文件 | 变异 | 结果 |
|---|---|---|
| `test_scheduler_alarm_guards.py` | M11 / M14 / M14a / M14b | ✅ 全部红 |
| `test_lifecycle.py` | 去掉 `_tick_safely` 的 try/except（M_B3_1） | ✅ 红 |
| `test_lifecycle.py` | 只删 `_tick_safely` 的日志（MUT F） | ✅ 红 |
| `test_lifecycle.py` | 节奏改回「睡满整间隔」（MUT N） | ✅ 2 条红 |
| `test_lifecycle.py` | 去掉每轮落盘（MUT O） | ✅ 2 条红 |
| `test_executors.py` | monitor 的 `external_calls` 改回硬编码常数 2 | ✅ 2 条红 |
| `test_executors.py` | `CallCounter.record()` 不再自增（MUT L） | ✅ 6 条红 |
| `test_scheduler_report.py` | 去掉预算 WARNING（MUT A） | ✅ 红（连带 `test_scheduler.py` 1 条） |
| `test_client.py` | 去掉 `client._calls.record()`（MUT B） | ✅ 5 条红 |
| `test_client.py` | `thread_total` 改成全局总数（串账，MUT D） | ✅ `test_call_count_is_per_thread` 红 |
| `test_metrics_window.py` | 目录缓存恒不命中（MUT C） | ✅ 2 条红 |
| `test_rules_sorting_offsets.py` | 加回 naive 格式 + UTC 兜底（MUT K） | ✅ 2 条红 |
| `test_package_boundary.py` | 生产代码引用仓库根 `data/` 路径 | ✅ 红 |
| `test_package_boundary.py` | 生产代码 `import tools`（MUT H） | ✅ 红 |
| `test_integration/test_main.py` | `os._exit` → `sys.exit`（MUT G，会被 atexit join 拖住） | ✅ 红（真进程挂死被捕获） |

**发现一条恒绿守卫**（Important 2，已在上面详述）：

- `tests/integration/test_main.py:807` `test_readiness_returns_503_when_the_last_round_failed`
  —— 503 实际来自 tpops 探针，scheduler 探针坏成什么样都绿。

**另发现一条「只差一层」的守卫**（Minor 4）：`test_package_boundary.py` 的
`data/` 检测器只认**单字符串字面量**（`PATH = 'data/mapping/x.json'`），
不认 `os.path.join('data', 'mapping', 'x.json')` 这种**分段拼接**形式 ——
见下面 Minor 3。

### 自证式测试（测试与生产犯同一个错，一起变错就一起绿）

**逐条排查结果**：

1. `test_executors.py` 的 `CountingFakeClient` 用的是**真的 `CallCounter`**，
   假客户端在 `get`/`post` 里 `self._calls.record()`。**风险**：
   测试断言 `outcome.external_calls == client.calls.total()` —— 两边都依赖
   `CallCounter`。
   **实证不成立**：把 `CallCounter.record()` 改成不自增（MUT L），
   `test_executors.py` **6 条全红**（monitor / command_center / diagnostic /
   instance / alarm / alarm 翻页）——
   因为 `outcome.external_calls` 与 `client.calls.total()` **来自不同的计数路径**
   （前者走 `_CallMeter` 的 `thread_total` 增量，后者走 `total()`），
   两者一起坏才会一起绿，而变异证明它们不会一起坏。**不是自证式** ✅

2. `test_client.py` 的 `test_call_count_does_not_lose_counts_under_concurrency`：
   断言 `client.calls.total() == 401` 且 `len(session.calls) == 401` ——
   **两个独立来源互证**（客户端自己的计数器 vs 假 Session 真实收到的请求数），
   裸 `+= 1` 的并发丢计数会被抓住 ✅

3. `test_call_count_is_per_thread` 用 `threading.Barrier` **强制四线程真交错**
   （每发一次就 `barrier.wait()` 一次），断言 `sorted(deltas) == [10,10,10,10]`。
   把 `thread_total` 改成全局总数（MUT D）→ **红** ✅
   —— 这条守卫**不是恒绿**，且能抓住「用全局计数当线程计数」的经典错误。

4. `test_package_boundary.py:171` `test_the_repository_root_data_detector_actually_detects`
   —— **守卫自检**：造一个含 `data/` 的假模块断言检测器能抓到，
   再造一个含 `tpops_kafka_collector/mapping/data` 的断言**不误报**。
   这是好设计（检测器自身被守住）✅

### `test_package_boundary.py` 现在守的是不是真实边界？

**是真实边界，不是自检式摆设。**

它现在守**两层**：

1. **import 层** —— 生产包每个模块都不得 `import tools.*` / `data.*`；
2. **文件读取层** —— 生产代码不得读**仓库根**的 `data/`。

第 2 层是本轮**新加严**的（原注释写着「映射数据仍从仓库根读取」，已过期）。
我注入 `'data/mapping/metric_rules.json'` 到 `mapping/loader.py`：

```
FAILED tests/unit/test_package_boundary.py::test_production_code_never_reads_the_repository_root_data_directory[tpops_kafka_collector/mapping/loader.py]
E       assert not ['data/mapping/metric_rules.json']
```

注入 `import tools` 到 `scheduler.py`：

```
FAILED tests/unit/test_package_boundary.py::test_production_package_never_imports_tools_or_data[tpops_kafka_collector/scheduler.py]
E       assert not ['tools']
```

两条边界**都是真守卫**（不是摆设），且**不误报**包内路径 ✅

**但**它有一处检测盲区（Minor 3，见下）。

### 覆盖率未覆盖行（`--cov-report=term-missing`）

```
$ conda run -n tpops python -m pytest -q --cov=tpops_kafka_collector --cov-report=term-missing
```

**重点核对：`scheduler_delivery.py` 的 `124/127/134-139`（首轮指出的零覆盖行）**

```
tpops_kafka_collector/scheduler_delivery.py     77 stmts    0 miss  100%
```

**首轮的 `124, 127, 134-139` 全部已被覆盖 —— 该文件现在 100%。**
`134-139` 正是告警投递失败的 `except` 分支（`134` 是 `produce_alarms`、
`135-139` 是 except 体），`124`/`127` 正是批内 `seen` 与跨轮 `has_key` 的
两处 `continue`。**首轮指出的零覆盖区已全部转为被覆盖** ✅

其他关键文件：

```
tpops_kafka_collector/lifecycle.py       99 stmts   15 miss   85%   88-91, 147, 189-194, 218, 222-225, 240, 247-248
tpops_kafka_collector/main.py           124 stmts    9 miss   93%   225, 232, 270, 275, 285, 311, 331-332, 337
tpops_kafka_collector/platform_profile.py 155 stmts  10 miss   94%   92, 95, 117, 148, 151, 156, 171-172, 359, 375
tpops_kafka_collector/executors.py       90 stmts    3 miss   97%   101, 235, 267
tpops_kafka_collector/tpops/call_tracking.py 93 stmts 10 miss  89%   115-118, 130-131, 195-198
tpops_kafka_collector/scheduler.py      151 stmts   22 miss   85%   152, 157, 196-200, 208-214, 255, 298-299, 379, 388-392
TOTAL                                   4927 stmts  256 miss   95%
```

**`lifecycle.py` 的 `88-91` 未覆盖 = 正是 `TrackedScheduler.tick` 那道闸门**
（Important 1 的覆盖率佐证，与变异结果互证）。
`main.py` 的 `270/275/285` 未覆盖 = `_tpops_state` 的 `auth_ok` 成功分支与
`_scheduler_state` 的「新鲜」成功分支（**成功路径未测，失败路径已测**）。

**映射层合计（逐文件 + 合计，合同完成标准 #1 要求 ≥95%）**

```
MAPPING LAYER: 1290 stmts, 0 miss, 100.00%
```

21 个映射文件**逐个 100%**（`rules_sorting.py` 首轮是 98%，本轮因
`parse_timestamp` 的 UTC 兜底分支删除而到 100%）。

---

## 三、实测汇总（全量 / ruff / py37 / AST 硬约束扫描 / 覆盖率）

```
$ conda run -n tpops python -m pytest -q
1683 passed in 59.92s

$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
154 passed in 1.30s

$ conda run -n tpops python -V
Python 3.7.9

$ conda run -n tpops python /tmp/ast_scan.py
=== FUNCTIONS with body span > 50 ===
count: 0
=== FILES >= 400 lines ===
count: 0

$ conda run -n tpops python -m pytest -q --cov=tpops_kafka_collector --cov-report=term-missing
TOTAL   4927 stmts   256 miss   95%
MAPPING LAYER: 1290 stmts, 0 miss, 100.00%
scheduler_delivery.py: 77 stmts, 0 miss, 100%
```

**变异验证清单（本轮共 21 条，每条改完 → 跑 → 还原 → 重跑）**

| # | 变异 | 预期红 | 实际 | 判定 |
|---|---|---|---|---|
| M11 | 告警投递失败也推进告警水位 | 红 | `test_alarm_watermark_does_not_advance_when_alarm_delivery_fails` | ✅ |
| M14 | 删告警 `seen` + `has_key` | 红 | 2 条（批内 + 跨轮） | ✅ |
| M14a | 只删 `seen` | 红 | 批内那条 | ✅ |
| M14b | 只删 `has_key` | 红 | 跨轮那条 | ✅ |
| M_B3_1 | 去 `_tick_safely` 的 try/except | 红 | `test_a_failing_round_does_not_stop_the_loop` | ✅ |
| MUT F | 只删 `_tick_safely` 的日志 | 红 | 同上 | ✅ |
| M_B3_2 | 退出码 3 → 0 | 红 | 2 条 | ✅ |
| M_B3_3 | `LOGGER.exception` → `LOGGER.error` | 红 | 堆栈那条 | ✅ |
| M_B3_4 | `_tpops_state` 两条失败分支 → True | 红 | 2 条 | ✅ |
| M_B3_5 | `_scheduler_state` 陈旧分支 → True | 红 | `test_scheduler_probe_reports_a_stale_last_success` | ✅ |
| **MUT E** | **去 `TrackedScheduler.tick` 的 `tasks_failed == 0` 闸门** | 红 | **1683 passed，无一红** | ❌ **守卫为假** |
| MUT M | `FakeClient` 认证态 + scheduler 探针仍变异 | 红 | `/readyz` 503 那条 | ✅（证明 503 来自 tpops） |
| MUT N | 节奏改回睡满间隔 | 红 | 2 条 | ✅ |
| MUT O | 去每轮落盘 | 红 | 2 条 | ✅ |
| MUT A | 去预算 WARNING | 红 | 2 条 | ✅ |
| MUT B | 去 `client._calls.record()` | 红 | 5 条 | ✅ |
| MUT C | 目录缓存恒不命中 | 红 | 2 条 | ✅ |
| MUT D | `thread_total` → 全局（串账） | 红 | `test_call_count_is_per_thread` | ✅ |
| MUT G | `os._exit` → `sys.exit` | 红 | 真进程挂死被捕获 | ✅ |
| MUT H | 生产 `import tools` | 红 | 边界那条 | ✅ |
| MUT I | `os.path.join('data', ...)` 分段拼接 | 红 | **136 passed，无一红** | ❌ 检测盲区 |
| MUT J/K | 恢复 UTC 兜底 / 加 naive 格式 | 红 | 加 naive 格式时 2 条红 | ✅ |
| MUT L | `CallCounter.record()` 不自增 | 红 | 6 条（证非自证式） | ✅ |
| cmp_instance/engine/master/scan_added | 改坏 4 个拆出的私有函数 | 红 | 各 1 条 | ✅ |

**每条变异后均已还原并重跑全量（1683 passed）。收尾 `diff -r` 确认沙箱与
工作树逐字节一致，工作树 `git diff --stat -- tpops_kafka_collector tests` 为空
—— 零污染、零 commit。**

---

## 四、发现

### Critical（0）

无。

### Important（2）

#### I1. `TrackedScheduler.tick` 的「全失败不算成功」闸门零守卫 —— 采集端全挂时 `/readyz` 仍报 200

- **File**：`tpops_kafka_collector/lifecycle.py:87-91`
- **现状**：
  ```python
  def tick(self) -> TickResult:
      result = super().tick()
      if result.tasks_failed == 0:      # <-- 无守卫
          self._last_tick_ok_at = self._wall_clock()
      return result
  ```
- **实证**：MUT E 拆掉这道闸门 → **1683 passed，无一测试变红**；
  独立探针确认语义真的变了（`tasks_failed = 1` 但 `last_tick_ok_at` 被记上）。
- **后果**：采集端**每一轮全挂**（平台不可达 / 认证反复失败 / SQL 全失败），
  `_last_tick_ok_at` 仍被刷新 → `_scheduler_state`（`main.py:278-285`）判就绪 →
  `/readyz` 返回 **200**。客户的编排系统 / K8s 探针认为采集端健康，
  **一条指标都没有投出去**，且**没有任何计数或日志能暴露它**
  （`failed=` 会在汇总日志里，但就绪端点是给编排看的）。
  这是本项目最贵的那一类缺陷形态。
- **修法**：补一条守卫，断言**真实行为**而非 mock 调用：
  1. 造一个「所有任务都失败」的调度器（执行器抛异常），跑一轮；
  2. 断言 `scheduler.last_tick_ok_at is None`（**没跑成功过 → 不许记成功**）；
  3. 再断言 `/readyz` 返回 503。
  **注意**：必须**独立于** `test_readiness_returns_503_when_the_last_round_failed`
  的 tpops 探针遮蔽（见 I2）—— 建议在守卫里用一个 `auth_ok=True` 的假客户端，
  让 503 **只能**由 scheduler 探针贡献。

#### I2. `/readyz` 503 的端到端守卫被 tpops 探针遮蔽 —— 恒绿

- **File**：`tests/integration/test_main.py:807`
  `test_readiness_returns_503_when_the_last_round_failed`（配套夹具 `:48` `FakeClient`）
- **实证**：
  - MUT B3_5（`_scheduler_state` 陈旧分支 → True）生效时，
    这条守卫**仍然 passed**（`1 failed, 23 passed`，红的只有单元那条）；
  - 把 `FakeClient` 补上 `auth_ok`、scheduler 探针**保持正确** → `24 passed`；
  - 把 `FakeClient` 补上 `auth_ok`、scheduler 探针**仍变异** → **红**（`200 != 503`）。
  → 证明这条 503 的 503 **实际来自 tpops 探针**（`FakeClient` 从未认证）。
- **后果**：这条守卫**恒绿** —— 它既没有钉住 scheduler 探针的「陈旧」分支
  （那条被 `test_scheduler_probe_reports_a_stale_last_success` 单独守住了），
  也没有钉住 I1 的「全挂不算成功」语义。
  修复轮声称「补了『跑了但全挂』的 503 守卫」—— **实际没补上**。
- **修法**：给这条守卫用的 `FakeClient` 加 `auth_ok = True`（或注入一个
  就绪的 tpops 探针），并在断言里**区分是哪个探针不就绪**
  （解析 body 的 `dependencies`，断言 `scheduler` 那项 `ready == false`），
  而不是只看整体 503。

### Minor（4）

#### M1. `test_package_boundary.py` 的 `data/` 检测器有分段拼接盲区

- **File**：`tests/unit/test_package_boundary.py:102-118`（`_repo_root_data_references`）
- **实证**：注入 `_LEAK2 = os.path.join('data', 'mapping', 'metric_rules.json')`
  → **136 passed，无一红**。检测器只扫**单个字符串字面量**里有没有
  `(?:^|[/\\])data[/\\]`，分段拼接逃逸。
- **后果**：将来有人用 `os.path.join(REPO_ROOT, 'data', ...)` 读仓库根数据，
  这条边界守卫**抓不到**。当前生产代码没有这种写法（已核），所以不是当下缺陷。
- **修法**：检测器补一条「`os.path.join` 的参数里出现独立 `'data'` 段」的判定，
  或在守卫里加一条针对 `os.path.join` 形态的断言。

#### M2. `test_package_boundary.py:127` `test_package_has_no_module` 是条无断言价值的摆设

```python
def test_package_has_no_module():
    """反证：包存在就不会走到这条。保留是为了让上面的失败信息可读。"""
    assert os.path.isdir(PACKAGE_DIR)
```
它与 `test_package_exists_and_is_not_empty` 重复，docstring 自承「保留是为了让
上面的失败信息可读」—— 但实际并没有被上面的失败信息引用。
建议删除，或改成真正有价值的断言（例如「包内**不得**残留
`__main__` 入口之外的顶层脚本」）。

#### M3. `main.py` 的 `_tpops_state` / `_scheduler_state` **成功路径**未覆盖

- **File**：`tpops_kafka_collector/main.py:270`（`auth_ok` 为真）、
  `:275`（已认证 → True）、`:285`（采集新鲜 → True）
- 覆盖率 `main.py` 未覆盖行含 `270, 275, 285`。
- 失败路径已测（见 B3-③），成功路径没有 —— 「一切正常 → `/readyz` 200」
  这条**最该绿**的路径反而没有守卫。
- **修法**：补 2 条断言（`auth_ok=True` → 探针就绪；新鲜 `last_tick_ok_at` → 探针就绪）。
  这两条同时也是修 I2 的前置（需要一个「就绪的 tpops 探针」夹具）。

#### M4. `test_executors.py:251` 的 `assert outcome.external_calls == 8` 是**写死**的数

- 该文件 docstring 明写「测试里**不写死**任何调用次数」，但 `:251` 写死了 8。
- 客观上它仍是真守卫（把 8 改成别的值会红，且与 `client.calls.total()` 互证），
  但**与自家声明的纪律相悖**；且若告警形态数从 4 变 5（平台加形态），
  这条会先红 —— 那是**好事**，但错误信息会指向「调用次数不对」而非「形态数变了」。
- **修法**：改成 `assert outcome.external_calls == client.calls.total()` 即可
  （下一行已有），把 8 换成 `4 * pages` 的推导式，或删掉 8 这行。

---

## 五、给控制器的话

- **B1、B2、C1 三条真闭环**（变异后必红，实测已验证），
  且我**没有发现任何别的测试掩盖变红**（每条变异下只有预期的守卫红）。
- **B3 剩 2 条 Important**，两条**指向同一个缺口**：
  「采集端全挂 → `/readyz` 503」这条语义**没有真正的守卫**。
  修复轮提交的 `test_readiness_returns_503_when_the_last_round_failed`
  看起来覆盖了它，实际被 tpops 探针遮蔽 —— **恒绿**。
- 建议**再走一轮小修**（只涉及 `lifecycle.py:87-91` 的守卫 +
  `tests/integration/test_main.py:807` 的夹具/断言），修完再签 pass receipt。
- **R2-A 维度（行为缺陷闭环）我没有重复核验**；本报告只覆盖守卫与硬约束维度。
  但据我实测，**A 维度声称闭环的 A1（节奏对齐）确实有真守卫**
  （MUT N 变红 2 条）、**A2（退出不挂死）确实有真守卫**
  （MUT G 变红）、**A5（每轮落盘）确实有真守卫**（MUT O 变红 2 条）——
  这三条我顺带变异验过，**不是恒绿摆设**，A 维度的结论我这边没有反证。

---

## 六、收尾

- 报告路径：
  `changes/tpops-kafka-collector/.superpowers/sdd/reviews/wave-3-scheduling-testing-rereview-b.md`
- **verdict = fail**
- 沙箱 `/tmp/r2b-mut` 已删除。
- 工作树**未 commit**（`git status --porcelain` 只有控制器改的 `progress.md`
  与 R2-A 的报告），生产代码与测试目录 `git diff --stat` **为空**。
- 所有变异均已在 `/tmp` 副本内完成并还原，**工作树零污染**。

签发 receipt 的命令：

```
ssf execution review changes/tpops-kafka-collector \
  --wave wave-3-scheduling-testing --base 3a71cb6 --head 2305cea \
  --report .superpowers/sdd/reviews/wave-3-scheduling-testing.md --verdict fail
```
