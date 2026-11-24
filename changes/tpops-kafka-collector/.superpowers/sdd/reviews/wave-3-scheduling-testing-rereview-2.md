# Wave 3 终审复审报告（修复轮 2）—— wave-3-scheduling-testing

- **base** `2305cea` / **head** `49bbd29`
- **复审范围**：仅审修复轮 2 新做的 7 件事（I1 / I2 / M1 / M2 / M3 / M4 / G1），不重审全量代码。
- **方法**：读代码 → 在 `/tmp/r2final-mut`（工作树副本）注入变异 → 确认守卫变红 → 还原 → 确认恢复绿。
  **未改工作树内任何 `tests/` 或 `tpops_kafka_collector/` 文件**；工作树 `git status` 与开工时一致。
- **结论**：**verdict = pass**
- **Critical / Important / Minor 计数**：**0 / 1 / 1**

> 说明：修复方报告 `repair-rd-report.md` 的数字本报告**一律未采信**，全部自行复跑。
> 该报告中「发现但未修的问题」第 1、2 条（`table.py` 仍指向仓库根 / 锚定式拼接扫不到）**属实**，本报告的 Important 项即为其一。

---

## 逐项核验

### I1 —— `TrackedScheduler.tick`「全挂不算成功」闸门补 3 条守卫

- **判定**：**通过**。
- **代码位置**：`tests/unit/test_lifecycle.py:298-388`（新增 3 条用例 + `_tracked_scheduler` / `_failing_executor` / `_succeeding_executor` 三个辅助）；
  被守的生产实现：`tpops_kafka_collector/lifecycle.py:87-91`（`if result.tasks_failed == 0:` → 才写 `_last_tick_ok_at`）。
- **生产实现未动**核验：

```
$ cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
$ git diff --stat 2305cea..49bbd29 -- tpops_kafka_collector/lifecycle.py tpops_kafka_collector/main.py tpops_kafka_collector/tpops/
(空 = 未改)
```

- **变异验证 1 —— 拆掉闸门**（`if result.tasks_failed == 0:` → `if True:`）：

```
$ conda run -n tpops python -m pytest -q tests/unit/test_lifecycle.py
FAILED tests/unit/test_lifecycle.py::test_a_fully_failed_tick_does_not_count_as_a_success
FAILED tests/unit/test_lifecycle.py::test_a_failed_round_after_a_success_does_not_refresh_the_timestamp
2 failed, 6 passed in 0.20s
```

- **变异验证 2 —— 反向（闸门永不满足）**（`if result.tasks_failed == 0:` → `if False:`）：

```
$ conda run -n tpops python -m pytest -q tests/unit/test_lifecycle.py
FAILED tests/unit/test_lifecycle.py::test_a_successful_tick_records_the_wall_clock
FAILED tests/unit/test_lifecycle.py::test_a_failed_round_after_a_success_does_not_refresh_the_timestamp
2 failed, 6 passed in 0.20s
```

  三个分支（成功记录 / 全挂不记录 / 成功后全挂不刷新）**双向**都有约束力，无恒真断言。
- **还原**：`cp /tmp/lifecycle.py.bak tpops_kafka_collector/lifecycle.py` → 全量 `1692 passed`。

---

### I2 —— `/readyz` 的 503 守卫归因修正

- **判定**：**通过**（本轮最关键的一条，见下节专项）。
- **代码位置**：`tests/integration/test_main.py:836-890`
  - `test_readiness_returns_503_when_the_last_round_failed`（改写）：`client.auth_ok = True` + 逐探针断言。
  - `test_readiness_returns_200_when_both_probes_are_ready`（新增正向对照）。
  - `_readiness_dependencies(body)`（新增辅助，把 `dependencies` 解析成 `{探针名: 条目}`）。
- **被守的生产实现**：`tpops_kafka_collector/main.py:262-285`（`_tpops_state` / `_scheduler_state`）、
  `tpops_kafka_collector/health.py:212-228`（`readiness()` 组装 `dependencies`）。
- **守卫自检结论**：503 用例的归因**唯一**，且**不是**碰巧红 —— 见下节。

---

### M1 —— `data/` 检测器补分段拼接写法

- **判定**：**部分通过 / 残留盲区（Important）**。修复方**主动披露**了盲区，本轮不构成隐瞒，但**未达到本轮验收标准所列的样本**。
- **代码位置**：`tests/unit/test_package_boundary.py`
  - 新增 `_dotted_name` / `_points_at_repo_root_data` / `_segmented_repo_root_data_references`（:112-170）；
  - `_repo_root_data_references` 现在两条查法并用（字面量 + 分段拼接）。
- **实测（对 5 种写法的探针，直接调检测器）**：

```
$ conda run -n tpops python probe.py
A_os_join_root_first     -> NOT DETECTED
B_path_div_root          -> NOT DETECTED
C_path_call_root         -> NOT DETECTED
D_os_join_literal        -> ["os.path.join('data', ...)"]
E_path_call_literal      -> ["Path('data', ...)"]
```

  （A = `os.path.join(REPO_ROOT, 'data', 'mapping')`；B = `Path(REPO_ROOT) / 'data' / 'mapping'`；
  C = `Path(REPO_ROOT, 'data', 'mapping')`；D = `os.path.join('data', 'mapping', 'x.json')`；
  E = `Path('data', 'mapping', 'x.json')`）

- **对**真实守卫**的变异验证**（把违规模块临时塞进包内）：

```
$ cat > tpops_kafka_collector/_violation_probe.py <<'EOF'
import os
from pathlib import Path

REPO_ROOT = '/repo'
A = os.path.join(REPO_ROOT, 'data', 'mapping')
B = Path(REPO_ROOT) / 'data' / 'mapping'
EOF
$ conda run -n tpops python -m pytest -q tests/unit/test_package_boundary.py
140 passed in 0.53s          # ← 守卫生效外，违规模块完全没被抓到（绿）
```

  对照组（同一守卫对另外两种写法**确实**会红）：

```
=== 字面量形式 'data/mapping/metric_rules.json' ===
FAILED tests/unit/test_package_boundary.py::test_production_code_never_reads_the_repository_root_data_directory[tpops_kafka_collector/_violation_probe.py]
1 failed, 139 passed in 0.52s

=== import tools 形式 ===
FAILED tests/unit/test_package_boundary.py::test_production_package_never_imports_tools_or_data[tpops_kafka_collector/_violation_probe.py]
1 failed, 139 passed in 0.51s
```

- **为什么重要**：检测器只认「**首段是 `data` 字面量**」，**锚定式拼接**（`REPO_ROOT` 打头）扫不到。
  而**这个形状今天就在生产包里**：`tpops_kafka_collector/mapping/table.py:15-16`

  ```python
  REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
  DATA_DIR = os.path.join(REPO_ROOT, 'data', 'mapping')
  ```

  即「边界守卫对包内现有的、形状完全相同的仓库根 `data/` 引用是绿的」→ 给出**部分虚假保证**。
- **不升级为 Critical 的理由**：(1) 修复方在 `repair-rd-report.md`「发现但未修的问题」第 1、2 条**明确披露**
  了这一点（"锚点变量不是字面量，静态判断不了"），不是隐瞒；(2) 该具体风险已被 **G1** 覆盖（见 G1：
  一旦有人构造 `MappingTable`，G1 就会红）；(3) 当前**没有**活跃违规 —— 包内除 `table.py` 外无锚定式仓库根 `data/` 引用。
- **建议（下一波必做）**：把 `table.py` 的 `DATA_DIR` 改成包内 `mapping/data`，或把 `MappingTable.load` 的
  `data_dir` 改成**必填**参数；检测器侧可增加「模块内出现 `REPO_ROOT` 且同段出现 `'data'` 字面量」的启发式。

---

### M2 —— 把摆设的边界断言换成「合成违规包」自检

- **判定**：**通过**。
- **代码位置**：`tests/unit/test_package_boundary.py:263-295`
  （`test_package_has_no_module` → `test_the_boundary_detectors_catch_a_synthetic_violation`，
  改名已在修复方报告「偏离说明」中披露，**测试未被删除**）。
- **变异验证 1 —— 拆掉分段拼接检测**：

```
$ conda run -n tpops python -m pytest -q tests/unit/test_package_boundary.py
FAILED tests/unit/test_package_boundary.py::test_the_boundary_detectors_catch_a_synthetic_violation
FAILED tests/unit/test_package_boundary.py::test_the_repository_root_data_detector_actually_detects
2 failed, 136 passed in 0.49s
```

- **变异验证 2 —— 拆掉禁用根 import 检测**（`_forbidden_root_imports` → `return []`）：

```
$ conda run -n tpops python -m pytest -q tests/unit/test_package_boundary.py
FAILED tests/unit/test_package_boundary.py::test_the_boundary_detectors_catch_a_synthetic_violation
1 failed, 137 passed in 0.44s
```

- **不误报核验**：合成包里的 `os.path.join('tpops_kafka_collector', 'mapping', 'data')` 必须**不**被报出 ——
  自检里已断言 `_repo_root_data_references(str(clean)) == []`，实测通过（140 passed 时该断言成立）。
- **守卫自检结论**：自检**有牙** —— 两个检测器任意一个退化，自检立刻变红。

---

### M3 —— `main.py` 健康探针的成功路径用例

- **判定**：**通过**。三条分支**各有**用例，断言落在**具体状态**上（`is True` / 具体 reason 文本），无恒真断言。
- **代码位置**：`tests/integration/test_main.py:807-834`
  - `test_scheduler_probe_reports_ready_after_a_successful_tick` → `assert state is True`
  - `test_scheduler_probe_reports_never_successful_yet` → `assert state is not True` + `'还没有成功完成过一轮' in state`
  - `test_tpops_probe_reports_ready_when_authenticated` → `assert state is True`
- **变异验证 —— scheduler 探针永不就绪**（`_scheduler_state` → 恒返回 `'调度器未就绪'`）：

```
$ conda run -n tpops python -m pytest tests/integration/test_main.py -k "probe or readiness" -v
PASSED tests/integration/test_main.py::test_health_probes_are_wired_and_lightweight
PASSED tests/integration/test_main.py::test_readiness_returns_503_before_first_tick
PASSED tests/integration/test_main.py::test_tpops_probe_reports_an_authentication_failure
PASSED tests/integration/test_main.py::test_tpops_probe_reports_not_authenticated_yet
FAILED tests/integration/test_main.py::test_scheduler_probe_reports_a_stale_last_success
FAILED tests/integration/test_main.py::test_scheduler_probe_reports_ready_after_a_successful_tick
FAILED tests/integration/test_main.py::test_scheduler_probe_reports_never_successful_yet
FAILED tests/integration/test_main.py::test_readiness_returns_503_when_the_last_round_failed
FAILED tests/integration/test_main.py::test_readiness_returns_200_when_both_probes_are_ready
```

  → **成功路径用例（`..._ready_after_a_successful_tick`）确实变红**，不是恒真。
- **变异验证 —— tpops 探针永不就绪**（`_tpops_state` → 恒返回 `'TPOPS 未认证'`）：

```
FAILED tests/integration/test_main.py::test_tpops_probe_reports_an_authentication_failure
FAILED tests/integration/test_main.py::test_tpops_probe_reports_ready_when_authenticated
FAILED tests/integration/test_main.py::test_readiness_returns_200_when_both_probes_are_ready
3 failed, 5 passed, 20 deselected
```

  → tpops 成功路径用例同样有约束力。

---

### M4 —— `test_executors.py:251` 写死的 `== 8` 改成推导值 + 关系型断言

- **判定**：**通过**。
- **代码位置**：`tests/unit/test_executors.py:136-165`（新增 `_alarm_request_shapes` / `_alarm_page_count`）、
  `:270-302`（`test_alarm_external_calls_scale_with_page_count` 改写）。
  期望值 = `len(shapes) × ceil(records / limit)`，两个因子都**从实际发生的事里数出来**
  （形态数看**请求体**里的 `is_active` × `is_convergence` 组合，页数看条数与每页容量）。
- **写死数字核验**：`grep -n "== 8" tests/unit/test_executors.py` → 无命中（期望值全部推导）。

```
$ grep -n "== 8\|external_calls ==" tests/unit/test_executors.py
224:    assert outcome.external_calls == client.calls.total()
234:    assert outcome.external_calls == client.calls.total()
246:    assert outcome.external_calls == client.calls.total()
257:    assert outcome.external_calls == client.calls.total()
267:    assert outcome.external_calls == client.calls.total()
288:    assert outcome.external_calls == expected, (
291:    assert outcome.external_calls == client.calls.total()
299:    assert single_outcome.external_calls == len(_alarm_request_shapes(single)) * (
323:    assert outcome.external_calls == 0
```

- **变异验证 —— 拆掉翻页**（`tpops_kafka_collector/tpops/alarms.py::fetch_all_alarm_records` 里
  `collected.extend(page.records)` 后 `break`）：

```
$ conda run -n tpops python -m pytest -q tests/unit/test_executors.py
>       assert outcome.external_calls == expected, (
E       AssertionError: 告警调用数没跟着页数涨：期望 8，实际 4
E       assert 4 == 8
FAILED tests/unit/test_executors.py::test_alarm_external_calls_scale_with_page_count
1 failed, 7 passed in 0.21s
```

  → 新断言**确实**跟着实际调用数走，调用数被改坏时变红。
- **守卫自检结论**：通过（写死值换成推导值后仍能抓住「页数没生效」）。

---

### G1 —— 生产代码不得构造/调用 `MappingTable`

- **判定**：**通过**。
- **代码位置**：
  - `tests/unit/test_package_boundary.py:196-253`：
    `_offline_mapping_table_constructions`（检测器）、
    `test_production_code_never_constructs_the_offline_mapping_table`（守卫）、
    `test_the_offline_mapping_table_detector_actually_detects`（自检）。
  - `tpops_kafka_collector/mapping/table.py:126-141`：`MappingTable` 类 docstring 写明「**离线工具专用，生产路径禁止使用**」，
    并指向 `mapping/loader.py` 与本守卫。
- **变异验证 —— 往包里塞违规模块**（同时写两种写法）：

```
$ cat > tpops_kafka_collector/_violation_probe.py <<'EOF'
from .mapping.table import MappingTable


def build():
    return MappingTable()


def build2():
    return MappingTable.load()
EOF
$ conda run -n tpops python -m pytest -q tests/unit/test_package_boundary.py
E       AssertionError: 生产代码构造了离线专用的 MappingTable（它读仓库根 data/mapping）：
          ['tpops_kafka_collector/_violation_probe.py:5 构造了 MappingTable',
           'tpops_kafka_collector/_violation_probe.py:9 构造了 MappingTable.load']
          —— 生产查表请用 tpops_kafka_collector.mapping.loader
FAILED tests/unit/test_package_boundary.py::test_production_code_never_constructs_the_offline_mapping_table
1 failed, 139 passed in 0.54s
```

  → `MappingTable()` 与 `MappingTable.load()` **两种写法都抓到**（属性调用没漏）。
- **守卫自检结论**：把检测器改成恒不命中 → 自检立刻变红：

```
$ conda run -n tpops python -m pytest -q tests/unit/test_package_boundary.py
E       AssertionError: 检测器没抓到两种写法 —— 守卫可能恒绿：[]
E       assert 0 == 2
FAILED tests/unit/test_package_boundary.py::test_the_offline_mapping_table_detector_actually_detects
1 failed, 137 passed in 0.56s
```

---

## I2 的归因唯一性（重点）

### 前提确认：上一轮那条 503 用例确实是**恒绿**的

用 `2305cea` 的旧测试文件 + 变异后的生产代码复跑（复现上一轮的盲区）：

```
$ git show 2305cea:tests/integration/test_main.py > /tmp/old_test_main.py
$ cp /tmp/old_test_main.py /tmp/r2final-mut/tests/integration/test_main.py
$ # 此时 tpops_kafka_collector/main.py 的 _scheduler_state 已被变异为恒返回 True
$ conda run -n tpops python -m pytest -q "tests/integration/test_main.py::test_readiness_returns_503_when_the_last_round_failed"
1 passed in 0.17s          # ← 旧用例在变异下仍然绿 = 恒绿盲区属实
```

→ 本轮修复**确实**关闭了这个盲区。

### 变异 1：`_scheduler_state` → 恒返回 `True`（503 用例必须变红）

```
$ conda run -n tpops python -m pytest -q tests/integration/test_main.py
>       assert status == 503
E       assert 200 == 503
tests/integration/test_main.py:861: AssertionError
FAILED tests/integration/test_main.py::test_health_probes_are_wired_and_lightweight
FAILED tests/integration/test_main.py::test_scheduler_probe_reports_a_stale_last_success
FAILED tests/integration/test_main.py::test_scheduler_probe_reports_never_successful_yet
FAILED tests/integration/test_main.py::test_readiness_returns_503_when_the_last_round_failed
4 failed, 24 passed in 29.57s
```

**503 用例变红 ✓**（旧版是绿的）。

### 变异 2：`_tpops_state` → 恒返回 `True`（正向用例）

```
$ conda run -n tpops python -m pytest -q "tests/integration/test_main.py::test_readiness_returns_503_when_the_last_round_failed" \
                                         "tests/integration/test_main.py::test_readiness_returns_200_when_both_probes_are_ready"
2 passed in 0.16s
```

**实测结果与本轮验收标准所列的期望不一致**：把 `_tpops_state` 变成「恒就绪」**不会**让 200 正向用例变红 ——
因为该用例断言的就是 `deps['tpops']['ready'] is True`，变异后的结果与断言方向一致。
这是**该断言的固有单向性**，不是回归：200 用例的判别力在「tpops **不就绪**」这一侧。

**互补变异 `_tpops_state` → 恒返回 `False`**（tpops 探针坏了）**两条用例同时变红**：

```
$ conda run -n tpops python -m pytest -q "tests/integration/test_main.py::test_readiness_returns_503_when_the_last_round_failed" \
                                         "tests/integration/test_main.py::test_readiness_returns_200_when_both_probes_are_ready"
>       assert status == 200
E       assert 503 == 200
tests/integration/test_main.py:885: AssertionError
FAILED tests/integration/test_main.py::test_readiness_returns_503_when_the_last_round_failed
FAILED tests/integration/test_main.py::test_readiness_returns_200_when_both_probes_are_ready
2 failed in 0.20s
```

「tpops **虚假就绪**」这一方向由两条直接单测覆盖（同样变异下变红）：

```
$ conda run -n tpops python -m pytest -q tests/integration/test_main.py -v
FAILED tests/integration/test_main.py::test_tpops_probe_reports_an_authentication_failure
FAILED tests/integration/test_main.py::test_tpops_probe_reports_not_authenticated_yet
```

→ **`_tpops_state` 的四个分支（已认证 / 认证失败 / 未认证 / 就绪）全部有约束力**，只是分布在
`test_tpops_probe_reports_*` 与 readiness 两条线上。**M3 的分支覆盖结论成立。**

### 变异 3：reason 文本断言是否钉住了状态（不是又一条恒真断言）

把 stale 分支的 reason 改成不含秒数的文本（`'调度器状态异常'`）：

```
$ conda run -n tpops python -m pytest -q "tests/integration/test_main.py::test_readiness_returns_503_when_the_last_round_failed" \
                                         "tests/integration/test_main.py::test_readiness_returns_200_when_both_probes_are_ready"
>       assert str(entry.SCHEDULER_STALE_SECONDS) in deps['scheduler']['reason'], deps
E       AssertionError: {'scheduler': {'name': 'scheduler', 'ready': False, 'reason': '调度器状态异常'},
                          'tpops': {'name': 'tpops', 'ready': True, 'reason': ''}}
E       assert '180' in '调度器状态异常'
FAILED tests/integration/test_main.py::test_readiness_returns_503_when_the_last_round_failed
1 failed, 1 passed in 0.19s
```

→ reason 文本断言**钉住了状态**，非恒真。

### 归因唯一性的代码级论证

`/readyz` 只注册两个探针（`tpops_kafka_collector/main.py:255-259`）：

```python
return ReadinessMonitor((
    DependencyProbe('tpops', lambda: _tpops_state(client)),
    DependencyProbe('scheduler', lambda: _scheduler_state(scheduler)),
))
```

`readiness()` 的 503 只可能来自这两个探针中**未就绪**的那个（`health.py:212-228`）。
503 用例现在**同时**断言：

1. `deps['tpops']['ready'] is True` → tpops 侧**不是** 503 的来源；
2. `deps['scheduler']['ready'] is False` → scheduler 侧**就是** 503 的来源；
3. `deps['scheduler']['reason']` 含 `SCHEDULER_STALE_SECONDS` → 归因到「跑过但**过旧**」这一具体分支。

配合变异 1（拆 scheduler → 红）与变异 3（改 reason → 红），
**503 的归因唯一，且不是碰巧红。**

---

## 实测

以下全部在**工作树** `/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（head = `49bbd29`）实跑。

```
$ conda run -n tpops python -m pytest -q
1692 passed in 80.46s (0:01:20)

$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest -q tests/test_py37_compat.py
154 passed in 1.20s

$ grep -rn "print(" tpops_kafka_collector/ | wc -l
0

$ python3 - <<'EOF'
import ast, pathlib
bad=[]
for p in pathlib.Path('tpops_kafka_collector').rglob('*.py'):
    t=ast.parse(p.read_text(encoding='utf-8'))
    for n in ast.walk(t):
        if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)):
            s=(n.end_lineno or n.lineno)-n.lineno+1
            if s>50: bad.append((str(p),n.name,s))
print('OVER-50:', bad if bad else '无')
EOF
OVER-50: 无
```

**`/tmp` 副本基线**（含 `docs/` 与客户指标 xlsx，否则 6 个 collection error —— 那是拷贝产物不是缺陷）：

```
$ cd /tmp/r2final-mut && conda run -n tpops python -m pytest -q
1692 passed in 60.34s (0:01:00)
```

**全部变异还原后复跑**：`1692 passed`（`grep -rn "MUTATION" tpops_kafka_collector/ tests/` 无命中）。

**工作树未被本轮复审污染**：

```
$ git status --short
 M changes/tpops-kafka-collector/.superpowers/sdd/plans/r3-.../repair-state/d2F2ZS0zLXNjaGVkdWxpbmctdGVzdGluZw.json
 M changes/tpops-kafka-collector/.superpowers/sdd/plans/r3-.../reviews/d2F2ZS0zLXNjaGVkdWxpbmctdGVzdGluZw.json
 M changes/tpops-kafka-collector/.superpowers/sdd/reviews/d2F2ZS0zLXNjaGVkdWxpbmctdGVzdGluZw.json
?? changes/tpops-kafka-collector/.superpowers/sdd/reviews/wave-3-scheduling-testing-rereview-b.md
```

（以上 4 项与开工时快照一致；本轮只新增了本报告一个文件。`/tmp/r2final-mut` 已删除。）

---

## 发现

### Critical

无。

### Important

**I-1（M1 残留盲区）—— 边界检测器抓不到「以仓库根为锚」的分段拼接，形状与包内现存违规完全相同**

- 位置：`tests/unit/test_package_boundary.py:112-170`（`_segmented_repo_root_data_references` /
  `_points_at_repo_root_data`）；`tpops_kafka_collector/mapping/table.py:15-16`。
- 事实：`_points_at_repo_root_data` 只认**首段是 `data` 字面量**的写法。
  `os.path.join(REPO_ROOT, 'data', 'mapping')`、`Path(REPO_ROOT) / 'data' / 'mapping'`、
  `Path(REPO_ROOT, 'data', 'mapping')` **均检测不到**（实测 A/B/C 三种 → `NOT DETECTED`）。
  把这样的模块塞进 `tpops_kafka_collector/` 后，`test_production_code_never_reads_the_repository_root_data_directory`
  **仍绿**（`140 passed`）。
- 影响：`tpops_kafka_collector/mapping/table.py:16` 的 `DATA_DIR = os.path.join(REPO_ROOT, 'data', 'mapping')`
  正是这个形状 → 边界守卫对**包内现存的**仓库根 `data/` 引用给绿，属于**部分虚假保证**。
- 修复方已在 `repair-rd-report.md`「发现但未修的问题」第 1、2 条**主动披露**并给出了方向
  （把 `table.py` 默认 `data_dir` 改成包内 `mapping/data`，或改成必填参数）——
  本报告确认其披露**属实**，不构成隐瞒，但**本轮验收标准所列的样本仍未通过**。
- 处置建议：下一波**必做**（不阻塞本次 pass）：把 `table.py` 的 `DATA_DIR` 指向包内 `mapping/data`
  或让 `data_dir` 必填；检测器侧补「模块内 `REPO_ROOT` 锚 + `'data'` 段」的启发式。

### Minor

**M-1（既存、非本轮范围）—— 同文件的兄弟用例仍保留旧的弱断言，scheduler 探针坏了它不会红**

- 位置：`tests/integration/test_main.py:545-553`（`test_readiness_returns_503_before_first_tick`）。
- 事实：这条用例**未**在本轮修改，仍是 `assert b'"name": "scheduler"' in body or ...`；
  且它的 `FakeClient` **没有** `auth_ok` → 503 实际来自 **tpops** 探针，scheduler 探针根本不是原因。
  实测（`_scheduler_state` 变异为恒返回 `True`）：

```
$ conda run -n tpops python -m pytest -q "tests/integration/test_main.py::test_readiness_returns_503_before_first_tick"
1 passed in 0.15s          # ← 恒绿盲区仍在（但属上一轮遗留，本轮 7 项未涉及）
```

- 建议：下一轮把这条也按 I2 的口径改写（`client.auth_ok = True` + 逐探针断言），
  或直接与 I2 合并成参数化用例。

---

## 收尾

签发 receipt 的命令：

    ssf execution review changes/tpops-kafka-collector \
      --wave wave-3-scheduling-testing --base 2305cea --head 49bbd29 \
      --report .superpowers/sdd/reviews/wave-3-scheduling-testing-rereview.md --verdict pass
