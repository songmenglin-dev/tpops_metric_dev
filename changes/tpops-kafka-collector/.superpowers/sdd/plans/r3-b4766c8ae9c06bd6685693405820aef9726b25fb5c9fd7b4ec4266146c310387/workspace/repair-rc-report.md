# 修复轮 R-C 报告 —— 告警守卫 / 结构 / 文档

工作树：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（分支 `tpops_metric_dev`）
执行方：**R-C**（不 commit，由控制器统一提交）

改动的文件（**仅限归属清单内**）：

| 类型 | 文件 |
|---|---|
| 生产 | `tpops_kafka_collector/scheduler_delivery.py`（注释）<br>`tpops_kafka_collector/platform_profile.py`（C1 拆分 + M1 文档）<br>`tpops_kafka_collector/scheduler_types.py`（M9 文档）<br>`tpops_kafka_collector/mapping/rules_sorting.py`（M3 死分支） |
| 测试 | 新建 `tests/unit/test_scheduler_alarm_guards.py`<br>改 `tests/unit/test_platform_profile.py`（M1）<br>改 `tests/unit/test_package_boundary.py`（M2）<br>改 `tests/unit/test_rules_sorting_offsets.py`（M3） |

**没有碰** `lifecycle.py` / `main.py` / `scheduler.py` / `executors.py` / `tpops/client.py` /
`tpops/metrics.py` / `scheduler_report.py` / `watermark.py`（R-A、R-B 的文件）。

---

## B1 告警失败不推进水位（守卫）

### 守卫测试

新建 `tests/unit/test_scheduler_alarm_guards.py`，两条守卫（与指标侧对称）：

- `test_alarm_watermark_does_not_advance_when_alarm_delivery_fails`
  —— 告警投递失败 → `store.alarm_watermark(alarm_stream(INSTANCE_A)) is None`；
- `test_failed_alarm_batch_is_retried_in_the_next_round`
  —— 失败的那批**下一轮重发**（幂等键没被登记 → 不会被当成重复跳过），
  重发成功后才推进告警水位。

夹具直接复用指标侧的（`from unit.test_scheduler import FakeClock, FakeProducer,
alarm_message, make_plans, make_scheduler`）—— `FakeProducer` 的
`fail_alarm_batches` 就是为这条准备的，保证两侧口径一致。

### 变异验证

> **说明（不假装经历 RED）**：B1/B2 的守卫写在**当前正确的实现**上，
> 因此一写出来就是绿的 —— 生产代码本来是对的，缺的是守卫。
> RED 的证明方式是**变异验证**：故意改坏实现 → 守卫红 → 改回 → 守卫绿。

**变异 M11**（把 `_advance_alarm` 挪进失败分支）：

```
$ conda run -n tpops python -m pytest -q tests/unit/test_scheduler_alarm_guards.py
        store = WatermarkStore(str(tmp_path / 'wm.json'))
        producer = FakeProducer(fail_alarm_batches={0})
        clock = FakeClock()
        scheduler = _alarm_scheduler(store, producer, clock)

        result = scheduler.tick()

        assert result.failed == 1, '投递失败没有进失败清单'
>       assert store.alarm_watermark(alarm_stream(INSTANCE_A)) is None, (
            '告警投递失败却推进了告警水位 —— 从未投出的告警会被下一轮判成「不新」'
        )
E       AssertionError: 告警投递失败却推进了告警水位 —— 从未投出的告警会被下一轮判成「不新」
E       assert '1000' is None
E        +  where '1000' = <bound method WatermarkStore.alarm_watermark of <...>('inst-a:alarm')
tests/unit/test_scheduler_alarm_guards.py:72: AssertionError
=========================== short test summary info ============================
FAILED tests/unit/test_scheduler_alarm_guards.py::test_alarm_watermark_does_not_advance_when_alarm_delivery_fails
1 failed, 3 passed in 0.75s
```

改回后：

```
$ conda run -n tpops python -m pytest -q tests/unit/test_scheduler_alarm_guards.py
....                                                                     [100%]
4 passed in 0.65s
```

### 生产代码注释改动

`tpops_kafka_collector/scheduler_delivery.py:135-141`（`_deliver_alarms` 的 except 分支）：

```python
        except Exception as exc:  # noqa: B902 —— 投递失败要隔离
            # **不变量**：告警水位**只能在投递成功之后**推进。
            # 在这里推进水位 → 下一轮的游标过滤会把**从未投出**的告警判成
            # 「不新」→ 告警整条**静默丢失**（日志里只剩这一条 exception，
            # 没有任何计数能暴露它）。守卫见
            # ``tests/unit/test_scheduler_alarm_guards.py``。
            LOGGER.exception(
                '告警投递失败 —— 整批不推进水位，下轮重投：channel=%s instance=%s',
                channel, instance_id,
            )
```

指标侧同一位置（`scheduler_delivery.py:99`）也补了一句对称的注释：

```python
            # **不变量**：指标水位**只能在投递成功之后**推进（与告警侧同一条）。
```

**行为未改** —— 只加注释。

---

## B2 告警幂等去重（守卫）

### 守卫测试（两条，都在 `tests/unit/test_scheduler_alarm_guards.py`）

- `test_duplicate_alarm_keys_inside_one_batch_are_delivered_once`
  —— 同一批里两条**相同幂等键**的告警只投一条（对照指标侧
  `test_duplicate_keys_inside_one_batch_are_delivered_once`）；
- `test_already_delivered_alarms_are_skipped_in_the_next_round`
  —— 已投递过的告警键在**下一轮**被跳过（对照指标侧
  `test_already_delivered_messages_are_skipped`）。

### 变异验证（变异 M14：删掉批内 `seen` + 跨轮 `has_key` 两道去重）

```
$ conda run -n tpops python -m pytest -q tests/unit/test_scheduler_alarm_guards.py
>       assert len(producer.alarm_batches) == 1, '重复投递了告警'
E       AssertionError: 重复投递了告警
E       assert 2 == 1
tests/unit/test_scheduler_alarm_guards.py:134: AssertionError
=========================== short test summary info ============================
FAILED tests/unit/test_scheduler_alarm_guards.py::test_duplicate_alarm_keys_inside_one_batch_are_delivered_once
FAILED tests/unit/test_scheduler_alarm_guards.py::test_already_delivered_alarms_are_skipped_in_the_next_round
2 failed, 2 passed in 0.77s
```

改回后：

```
$ conda run -n tpops python -m pytest -q tests/unit/test_scheduler_alarm_guards.py
....                                                                     [100%]
4 passed in 0.65s
```

**生产代码未改**（去重逻辑本来就是对的，缺的是守卫）。

---

## C1 reconcile 拆分

### 拆分后的函数清单与各自行数

`platform_profile.py` 里 `PlatformProfile.reconcile` 从 **55 行**拆成 1 个主函数 +
4 个模块级私有函数（`reconcile` 只负责汇总与组装 `ProfileReconciliation`）：

| 函数 | 行 | span | 职责 |
|---|---|---|---|
| `PlatformProfile.reconcile` | 244 | **30** | 取平台列表 → 逐实例比对 → 汇总组装 |
| `_compare_instance` | 280 | **22** | 比对画像里的**一个**实例与平台现状 |
| `_compare_engine` | 304 | **11** | 比对引擎名 |
| `_compare_master_count` | 317 | **13** | 比对 master 节点数（**没有节点清单就不比**） |
| `_scan_added` | 332 | **17** | 扫描平台新增实例（**只告警，不自动加**） |

全部 < 50 行。`platform_profile.py` 总行数 336 → **383**（仍 < 400）。

### 行为不变证明（现有守卫全绿）

```
$ conda run -n tpops python -m pytest -q tests/unit/test_platform_profile.py
...............                                                          [100%]
15 passed in 0.37s
```

（现有 14 条 + M1 新增的 1 条，见下节。）

**行为等价性说明**：原实现与新实现的差别只是把三段逻辑搬进私有函数，
**日志的级别、措辞、参数、发出顺序全部保持不变**
（缺失 → ERROR；引擎名变 → WARNING；master 数变 → WARNING；
新增实例 → WARNING，且新增实例的告警仍然在逐实例比对**之后**发出）。
`node_registry is None` 时**不比** master 数这条语义也原样保留
（`_compare_master_count` 里第一件事就是 `if node_registry is None: return False`）。

### 变异验证（改坏拆出去的私有函数 → 现有守卫必须变红）

**变异 M-A**：`_compare_master_count` 永远报「没变」

```
FAILED tests/unit/test_platform_profile.py::test_master_count_change_logs_warning
E       AssertionError: assert 'inst-1' in ()
1 failed, 13 passed in 0.41s
```

**变异 M-B**：`_scan_added` 不再收集新增实例

```
FAILED tests/unit/test_platform_profile.py::test_platform_instance_missing_from_profile_logs_warning
E       AssertionError: assert 'inst-new' in ()
1 failed, 13 passed in 0.41s
```

**变异 M-D**：`_compare_instance` 不再报「平台上已缺失」

```
FAILED tests/unit/test_platform_profile.py::test_profile_instance_missing_on_platform_logs_error
E       AssertionError: assert 'inst-gone' in ()
1 failed, 13 passed in 0.45s
```

三次变异都**只改私有函数**，全部被现有守卫抓到 → 拆分**没有引入未被覆盖的行为**，
不需要补测试。

---

## Minor 修正

### M1 —— `platform_profile.py:191-199` docstring 说了假话

**位置**：`platform_profile.py` `load_or_discover` 的 docstring。

**问题**：原文写「**固化文件存在 → 不重新发现**（平台列表接口一次都不调用）」——
但生产 `main.py:218` 传的是 `reconcile=True` → 每次启动**都会**调
`GET /rds/v3.2/instances` 做核对。

**改法**（`platform_profile.py:191-200`）：

```python
        """读固化画像；没有就发现一次并固化。

        **固化文件存在 → 不重新发现、不改写**（画像以文件为准）：
        平台列表的顺序会变、客户也会加实例，每轮重新发现等于**静默增删
        采集目标** —— 水位与差分基线全部错位，且不报错。

        :param reconcile: 是否在读固化画像后**额外调一次**平台列表做核对
            （生产装配传 ``True``；**只记日志，不改画像文件**）。
            不传（``False``）时**平台列表接口一次都不调用**。
        """
```

**配套测试**（`tests/unit/test_platform_profile.py`，新增
`test_reconcile_calls_the_platform_list_but_does_not_rewrite_the_file`）：
断言「平台列表**被调了**（`client.calls` 非空），但画像文件**一个字节都没被改**
（内容前后逐字相同），且返回的画像仍只有 `inst-1`（新增的 `inst-new` 不自动进画像）」。

另外把原有那条 `test_existing_profile_file_is_not_rediscovered` 的 docstring
补上「不核对时」三个字 —— 它断言的是 `reconcile=False` 的路径，仍然成立。

**变异验证**（把 `if reconcile:` 改成 `if False:`）：

```
FAILED tests/unit/test_platform_profile.py::test_reconcile_calls_the_platform_list_but_does_not_rewrite_the_file
E       AssertionError: reconcile=True 却没调平台列表 —— 核对没做
E       assert []
1 failed, 14 passed in 0.66s
```

### M2 —— `tests/unit/test_package_boundary.py` 的守卫文本过期

**位置**：该文件的模块 docstring。

**问题**：原文写「**这是 import 层面的边界检查**，不是文件读取层面的 ——
映射数据目前仍从仓库根 `data/mapping/` 读取，搬进包内是 wave-2a 任务 2.1 的交付物」。
**已过期** —— wave-2a（`c816358`）已把 10 份映射数据搬进
`tpops_kafka_collector/mapping/data/`，生产代码现在读的是包内
（`mapping/loader.py:49` `PACKAGE_DATA_DIR`）。

**改法**（**不削弱守卫，反而加严**）：

1. docstring 改成「**两条边界都要守**（两层都要查，缺一层就有缝）」：
   import 层面 + **文件读取层面**（生产代码不得读仓库根 `data/`）；
2. **新增**守卫 `test_production_code_never_reads_the_repository_root_data_directory`
   —— 用 AST 扫生产包每个模块的**字符串字面量**（**文档字符串除外** —— 文档字符串
   只是说明文字，不读文件），匹配「路径段级别的 `data/`」
   （`(?:^|[/\\])data[/\\]`，所以 `metadata/` 不误报、包内的
   `tpops_kafka_collector/mapping/data` 也不误报）；
3. **新增**守卫 `test_package_mapping_data_is_inside_the_package`
   —— 断言 `loader.PACKAGE_DATA_DIR` 解析后**在生产包目录内**、目录存在、且非空
   （不是仓库根的 `data/mapping`）；
4. **新增**守卫自检 `test_the_repository_root_data_detector_actually_detects`
   —— 用临时文件证明检测器**抓得到**违规（`'data/mapping/metric_rules.json'`）
   **且不误报**（`'tpops_kafka_collector/mapping/data'`）。
   没有这条，第 2 条守卫可能变成恒绿的空壳。

原来的 import 层守卫 `test_production_package_never_imports_tools_or_data` **原样保留**。

**变异验证**：守卫自检本身就是「阳性样本」——检测器在阳性样本上必须报错、
在阴性样本上必须不报，两条都在测试里断言。

```
$ conda run -n tpops python -m pytest -q tests/unit/test_package_boundary.py
........................................................................ [ 54%]
........................................................................ [100%]
132 passed in 1.24s
```

### M3 —— `mapping/rules_sorting.py` `parse_timestamp` 的「无时区按 UTC」是死分支

**位置**：`mapping/rules_sorting.py:114-131`。

**确认是死代码**（实测，不是推测）：

```
$ conda run -n tpops python _probe_ts.py
formats: ('%Y-%m-%d %H:%M:%S.%f%z', '%Y-%m-%d %H:%M:%S%z')
产出 naive datetime 的输入: []
parse_timestamp('2026-11-18 16:27:07.557567') -> None
parse_timestamp('2026-11-18 16:27:07') -> None
parse_timestamp('2026-11-18T16:27:07') -> None
```

`TIMESTAMP_FORMATS` 里两个格式**都带 `%z`** → `strptime` 产出的 datetime
**一定**是 aware 的 → `if parsed.tzinfo is None:` **永远不会执行**。
（`parse_timestamp` 的实际行为是：无时区文本 → 返回 `None` → 那一列按文本比。）

**改法**：按「确认是死代码且删掉不影响任何测试」这条路径 —— **删掉死分支**，
并把 docstring 改成与**实际行为**一致（原文声称「没有时区的按 UTC 处理」是**假的**）：

```python
    **只认带时区的文本**：``TIMESTAMP_FORMATS`` 里两个格式都带 ``%z`` →
    解析成功**一定**是 aware 的 datetime。**没有时区**的时间戳文本解析不了
    （返回 ``None``，那一列按文本比）—— 这里**不做**「按 UTC 兜底」：
    平台数据不带时区时按 UTC 猜，比按文本比更容易取错 Top N 行。
```

**行为零变化**（死分支不可达 ⇒ 删掉它对任何输入都等价）。
现有测试全绿，并补了两条守卫（`tests/unit/test_rules_sorting_offsets.py`）：

- `test_timestamps_without_a_timezone_are_not_silently_treated_as_utc`
  —— 无时区文本 → `None`（**不按 UTC 兜底**）；
- `test_parsed_timestamps_are_always_timezone_aware`
  —— 解析成功的结果**一定带时区**（不会与 naive 的兜底值混比报错）。

```
$ conda run -n tpops python -m pytest -q tests/unit/test_rules_sorting_offsets.py
..........                                                               [100%]
10 passed in 0.21s
```

### M9 —— `scheduler_types.py` `TickResult.mapped` 的文档口径

**位置**：`scheduler_types.py:121`。

**问题**：原文写「转换后待投递的**消息总数**」，实际值来自
`scheduler.py:341` `mapped=totals.planned`，而 `planned` 是
**滤掉幂等重复之后真正尝试投递的条数**（`_deliver_metrics` / `_deliver_alarms`
里 `seen` 与 `has_key` 过滤之后才 `append`）。

**改法**（**只改文档，不改行为**）：

```python
    :param mapped: 转换后**实际尝试投递**的消息数（指标 + 告警）——
        **已滤掉幂等重复**（批内重复键与已投递过的键都不计入）。
```

### M4 —— `put_baseline`「会丢作用域」的说法与实现不符（**只报告，未改**）

**位置**：`watermark.py:166-173`（`put_baseline`）与
`watermark.py:178-184`（`import_baselines_from`）。**这两个文件不归 R-C**，没动。

**事实**（读实现得出）：

```python
    def put_baseline(self, metric_code, dimension_key, counter_value, sampled_at_ms):
        self._baselines[(metric_code, dimension_key)] = (counter_value, sampled_at_ms)

    def import_baselines_from(self, delta_store):
        container = _baselines_container(delta_store)     # 同一个 {(metric_code, dimension_key): ...} 形状
        self._baselines = {(key[0], key[1]): (value[0], value[1]) for key, value in container.items()}
```

`put_baseline` 与 `import_baselines_from` 存的键**完全相同**
（`(metric_code, dimension_key)`），而**作用域本来就在 `dimension_key` 里**
（`scheduler.py` 的口径是 `scope:key`，见 `tests/unit/test_scheduler.py:723`
断言 `baselines[0].dimension_key.startswith('node-1:')`）。

→ **控制器口径 #2 的理由（"`put_baseline` 会丢作用域"）不成立**：
作用域不是 `put_baseline` 丢的，而是调用方传的 `dimension_key` 里有没有带。
`put_baseline` 本身**不丢作用域**，它只是不自己拼作用域。

**建议**（请控制器转 R-A 或留作待办）：要么改口径说明
（「不用 `put_baseline`」的理由改成「统一走整体搬运，避免两套写入路径」），
要么在 `put_baseline` 的 docstring 上写清「`dimension_key` 必须自带作用域」。

**另**：现有守卫 `test_put_baseline_style_keys_are_not_used`
（`tests/unit/test_scheduler.py:728`）断言的是「调度器不许用 `put_baseline` 恢复基线」
—— 这条**与作用域无关**，它是「只用整体搬运」的流程约束，仍然有效，未动。

---

## 实测结果

### 我负责的四个测试文件

```
$ conda run -n tpops python -m pytest -q tests/unit/test_scheduler_alarm_guards.py \
      tests/unit/test_platform_profile.py tests/unit/test_package_boundary.py \
      tests/unit/test_rules_sorting_offsets.py
........................................................................ [ 89%]
.................                                                        [100%]
161 passed in 1.72s
```

### 全量

```
$ conda run -n tpops python -m pytest -q
........................................................................ [ 95%]
........................................................................ [ 99%]
.........                                                                [100%]
1665 passed in 47.98s
```

（**≥ 1560 passed，无回归**。中途曾出现 1 条
`tests/unit/test_scheduler.py::test_summary_log_uses_the_agreed_field_names` 失败 ——
那是 **R-B** 正在改 `scheduler_report.py` 的日志文案（`'本轮丢弃 %d 个不可提供指标'`
→ `'不可提供指标 %d 条（本轮不产出，累计 %d）'`）而旧断言还在 `test_scheduler.py` 里；
不是 R-C 的改动造成的，R-B 修完后全量恢复绿。
另有一轮出现过 `tests/integration/test_main.py::test_real_process_receives_sigterm_and_exits_zero`
失败 —— 该用例**真起一个进程**再发 SIGTERM，属于**时序敏感**用例，
并发跑测试（本轮 R-A/R-B 同时在跑）时偶发；单独跑 `15 passed`，全量复跑即绿。
与 R-C 的改动无关 —— R-C 没碰 `main.py` / `lifecycle.py`。）

### ruff

```
$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m ruff check tpops_kafka_collector/scheduler_delivery.py \
      tpops_kafka_collector/platform_profile.py tpops_kafka_collector/scheduler_types.py \
      tpops_kafka_collector/mapping/rules_sorting.py
All checks passed!
```

> `tests/` 不在 ruff 门禁范围内（仓库现状：`ruff check tests/` 本来就有 80+ 条
> UP009/UP010/UP031/UP004 等历史告警）。R-C 新建的测试文件沿用**同目录既有风格**
> （`# -*- coding: utf-8 -*-` + `from __future__ import absolute_import`），
> 与 `test_scheduler_delivery_channels.py` 等保持一致。

### py3.7 兼容门禁

```
$ conda run -n tpops python -m pytest -q tests/test_py37_compat.py
........                                                                 [100%]
152 passed in 1.35s
```

### 超长函数扫描（合同「函数 < 50 行」）

```
$ python3 - <<'EOF'
import ast, pathlib
for p in sorted(pathlib.Path('tpops_kafka_collector').rglob('*.py')):
    t = ast.parse(p.read_text(encoding='utf-8'))
    for n in ast.walk(t):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            span = (n.end_lineno or n.lineno) - n.lineno + 1
            if span > 50: print('OVER-50', p, n.name, n.lineno, span)
EOF
OVER-50 tpops_kafka_collector/main.py build_runtime line=142 span=52
```

**`reconcile` 已不在列表里** ✅（本轮之前它是全包**唯一**越线函数）。
剩下的 `main.py::build_runtime`（52 行）属于 **R-A** 的在途改动，本轮不在 R-C 归属范围。

### 文件行数（合同「单文件 < 400 行」）

```
$ for f in $(find tpops_kafka_collector -name '*.py'); do n=$(wc -l < $f); \
    if [ "$n" -ge 400 ]; then echo "$n $f"; fi; done
401 tpops_kafka_collector/main.py
483 tpops_kafka_collector/scheduler.py
```

R-C 拥有的 4 个生产文件：`scheduler_delivery.py` 176 行、`platform_profile.py` **383** 行、
`scheduler_types.py` 211 行、`mapping/rules_sorting.py` 137 行 —— **均 < 400** ✅。
上表两条属于 R-A 的在途改动（见下节）。

### print 计数

```
$ grep -rn "print(" tpops_kafka_collector/ --include=*.py | grep -v "#" | wc -l
0
```

---

## 发现但未修的问题

1. **【R-B 的文件 —— 已由 R-B 自行解决】** `tpops_kafka_collector/scheduler_report.py`
   改了每轮汇总日志的文案（`'本轮丢弃 %d 个不可提供指标（累计 %d）'` →
   `'不可提供指标 %d 条（本轮不产出，累计 %d）'`），
   一度让旧断言 `tests/unit/test_scheduler.py:801` 变红。
   **该测试文件不在 R-C 归属范围，R-C 未动**；R-B 已把断言改成
   「只断言字段的值」（`assert 'dropped_unsupported=3' in caplog.text`），
   全量恢复绿。

2. **【R-A 的文件】** 合同「单文件 < 400 行」「函数 < 50 行」在 R-A 的在途改动上被突破：
   `tpops_kafka_collector/scheduler.py` **483 行**、`tpops_kafka_collector/main.py` **401 行**、
   `main.py::build_runtime` **52 行**。R-C 未动这三个文件，请控制器提醒 R-A。

3. **【文档过期，不在 R-C 归属范围】**
   - `tpops_kafka_collector/ids.py:15` 与 `tpops_kafka_collector/models.py:14`
     的 docstring 仍写「键的声明在 ``data/mapping/alarm_rules.json``」——
     那个文件现在在**包内** `tpops_kafka_collector/mapping/data/alarm_rules.json`。
     （M2 的新守卫刻意**不扫文档字符串**，就是为了不越界去改这两个文件。）
   - `tpops_kafka_collector/mapping/README.md:60-62` 仍写「映射数据还没搬进包内 ——
     那是 wave-2a 任务 2.1 的事」（wave-3 质量审查的 M-7，同一份报告）。

4. **【观察，非缺陷】** `platform_profile.py` 现在 382 行，离 400 行的合同上限只剩 18 行。
   再加功能前建议先拆文件（画像读写 / 核对 两块）。

---

## 偏离说明

1. **B1/B2 没有「RED」阶段**：这两条守卫写在**行为本来就正确**的生产代码上，
   写出来直接是绿的。任务书已明确「当前生产代码的行为是对的 —— 你要补的是守卫，
   不要改行为」。RED 的证明用**变异验证**（M11 / M14）代替，报告里贴的是真实输出。

2. **M3 选择了「删死分支」而不是「只改注释」**：任务书允许两者之一，
   前提是「确认是死代码且删掉不影响任何测试」。已实测确认不可达
   （两个格式都带 `%z`）、删掉后现有测试全绿、并补了两条守卫把**实际行为**
   （无时区 → `None`）钉住。**行为零变化**。

3. **M1 顺带改了 `test_existing_profile_file_is_not_rediscovered` 的 docstring**
   （加「不核对时」三个字）：原文「平台列表接口一次都没被调用」对
   `reconcile=False` 成立、对生产路径（`reconcile=True`）不成立，容易再被误读。
   **断言未动**，仍然有效。

4. **C1 的私有函数放在模块级**（`_compare_instance` / `_compare_engine` /
   `_compare_master_count` / `_scan_added`），不是 `PlatformProfile` 的
   `@staticmethod` —— 与同文件既有的辅助函数（`_master_count` / `_utc_now_iso`）
   风格一致，且便于单独做变异验证。

5. **给指标侧也加了一句不变量注释**（`scheduler_delivery.py:99`）：
   任务书只要求告警侧，但两侧是**同一条不变量**，写成对称的注释可减少
   「只改一边」的漂移。**纯注释，无行为变化**。

6. **未 commit / 未 `git add`**（按任务书要求由控制器统一提交）。
   未使用 `git checkout` / `git reset` / 未移动 HEAD / 未改分支。
   变异验证期间对生产文件的临时改动**全部已还原**（用 `cp` 备份/还原，未动 git 索引）。
