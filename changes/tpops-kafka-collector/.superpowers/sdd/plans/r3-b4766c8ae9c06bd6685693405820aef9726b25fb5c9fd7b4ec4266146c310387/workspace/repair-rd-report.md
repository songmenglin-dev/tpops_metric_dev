# 修复轮 2 报告 R-D —— 就绪判定的守卫

工作树：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（分支 `tpops_metric_dev`）
基线：1683 passed → 收尾 **1690 passed**（新增 7 条守卫测试，无回归）。

**生产代码一行未改**（`git diff --stat tpops_kafka_collector/` 为空）—— I1/I2 的生产实现本来就是对的，
本轮补的是**守卫**。唯一的生产文件改动是**临时变异**（用于验证守卫生效），验证后已逐个改回。

---

## I1 全挂不算成功（守卫）

- 守卫测试（新增 3 条，全在 `tests/unit/test_lifecycle.py`）：
  - `tests/unit/test_lifecycle.py::test_a_fully_failed_tick_does_not_count_as_a_success`
    —— 一轮 `tasks_failed > 0` → `last_tick_ok_at` **保持 `None`**。
  - `tests/unit/test_lifecycle.py::test_a_successful_tick_records_the_wall_clock`
    —— 一轮 `tasks_failed == 0` → `last_tick_ok_at` **被更新**（值 ≈ 注入的墙钟）。
  - `tests/unit/test_lifecycle.py::test_a_failed_round_after_a_success_does_not_refresh_the_timestamp`
    —— 成功过、之后又全挂 → 时间戳**不刷新**（旧值留着 → 就绪探针按「过旧」判不就绪）。
    这条把「闸门是**逐轮**判定」钉死。
- 支撑件：用**真的** `TrackedScheduler`（`lifecycle.py` 里那一处闸门的宿主）+ 真的到期表，
  只把平台执行器与时钟换成假的（`_tracked_scheduler` 辅助函数，`wall_clock` 注入假墙钟）。
  失败轮用「执行器抛异常」制造 → `TickResult.tasks_failed > 0`。
- 生产实现是否改动：**未改**（只补守卫）。

### 变异验证（真实输出）

改坏方式：把 `tpops_kafka_collector/lifecycle.py:87` 的闸门
`if result.tasks_failed == 0:` 改成 `if True:`。

```
########## I1 变异：if result.tasks_failed == 0  ->  if True ##########
=========================== short test summary info ============================
FAILED tests/unit/test_lifecycle.py::test_a_fully_failed_tick_does_not_count_as_a_success
FAILED tests/unit/test_lifecycle.py::test_a_failed_round_after_a_success_does_not_refresh_the_timestamp
2 failed, 6 passed in 0.79s
```

（失败详情：`AssertionError: 一轮全挂却记了「最近一次成功完成」—— 就绪探针会报 200`；
第二条为 `assert 1700000600.0 == 1700000000.0`。）

改回：

```
########## I1 改回 ##########
........                                                                 [100%]
8 passed in 0.64s
```

`git diff --stat tpops_kafka_collector/lifecycle.py` → 空（已完全还原）。

**注意（报告口径）**：本条**没有**「先写测试看到红」的过程 —— 生产实现本来就是对的，
RED 的证明方式就是上面的**变异验证**：把闸门拆掉 → 守卫变红；闸门装回去 → 守卫变绿。

---

## I2 `/readyz` 503 归因（守卫）

- 守卫测试（`tests/integration/test_main.py`）：
  - `test_readiness_returns_503_when_the_last_round_failed`（**改造**）
  - `test_readiness_returns_200_when_both_probes_are_ready`（**新增**正向对照）
  - 另加 M3 的 3 条探针分支用例（见下）。
- 改造内容（三处都改了）：
  1. **让 `FakeClient` 处于已认证状态**：该用例里 `client.auth_ok = True`。
     `_tpops_state` **优先**读公开属性 `auth_ok`（`main.py` 里 `getattr(client, 'auth_ok', None)`），
     返回 `bool(checker)` → 探针结果恒为 `True`，**不再**落到「TPOPS 尚未完成认证」那一支。
  2. **断言落到 scheduler 探针的具体状态文本**：解析响应体成
     `{探针名: {name, ready, reason}}`，断言
     `deps['tpops']['ready'] is True`（**证明 503 不来自 tpops**）、
     `deps['scheduler']['ready'] is False`、
     `str(entry.SCHEDULER_STALE_SECONDS) in deps['scheduler']['reason']`
     （reason 为「最近一次成功采集距今超过 180 秒」）。
     旧的断言 `b'"name": "scheduler"' in body` 已删除 —— 响应体里**每个探针的 name 都在**，那条恒真。
  3. **补正向用例**：tpops 与 scheduler 都就绪 → `/readyz` 返回 **200**，
     并断言 `payload['status'] == 'ok'` 与两个探针 `ready is True`。

### 为什么改完之后 503 的归因唯一

- 就绪响应体的 `dependencies` 数组**逐探针**给出 `name` / `ready` / `reason`（`health.py` 的
  `HealthEndpoints.readiness()`）。
- 现在这条用例把 **tpops 探针钉成 ready**（`auth_ok=True`）→ 它不可能贡献 503；
- 同时把 **scheduler 探针钉成 not-ready 且 reason 含「超过 180 秒」** → 503 只可能由它产生；
- 再用正向用例证明「两个探针都 ready → 200」，排除「503 是端点本身无条件返回」的可能。
  正反合起来：503 的**唯一**来源是 scheduler 探针。

### 变异验证（真实输出）

改坏方式：把 `tpops_kafka_collector/main.py` 的 `_scheduler_state` 整个变成恒返回 `True`。

```
########## I2 变异：_scheduler_state 恒返回 True ##########
=========================== short test summary info ============================
FAILED tests/integration/test_main.py::test_health_probes_are_wired_and_lightweight
FAILED tests/integration/test_main.py::test_scheduler_probe_reports_a_stale_last_success
FAILED tests/integration/test_main.py::test_scheduler_probe_reports_never_successful_yet
FAILED tests/integration/test_main.py::test_readiness_returns_503_when_the_last_round_failed
4 failed, 24 passed in 32.13s
```

（`test_readiness_returns_503_when_the_last_round_failed` 的失败点：
`assert 200 == 503` —— 原来那条恒绿断言被替换掉的正是这条。）

改回：

```
########## I2 改回 ##########
............................                                             [100%]
28 passed in 31.47s
```

`git diff --stat tpops_kafka_collector/main.py` → 空（已完全还原）。

> 补充说明：早先一次同样的变异里 `test_real_process_receives_sigterm_and_exits_zero` 也红了，
> 那条是**子进程演练**用例、与本次改动无关（同一变异复跑即绿）。

---

## Minor M1~M4

### M1 —— `data/` 检测器漏了分段拼接

- 位置：`tests/unit/test_package_boundary.py` 的 `_repo_root_data_references`。
- 改了什么：新增 `_segmented_repo_root_data_references(tree)` + `_dotted_name` +
  `_points_at_repo_root_data`；`_repo_root_data_references` 现在**两条查法并用**：
  1. 路径**字面量**（原逻辑）；
  2. **分段拼接** —— `os.path.join('data', ...)` / `posixpath.join` / `ntpath.join` /
     `join`（`from os.path import join`）/ `Path('data', ...)` / `pathlib.Path('data', ...)`，
     且**首段**是 `data`（`'data'` 或 `'data/...'`）才算命中。
- 依据：`os.path.join('data', 'mapping', 'x.json')` 的字面量是 `'data'` / `'mapping'` /
  `'x.json'` —— **没有一段含 `data/`** → 只扫字面量的检测器结构性漏掉这种写法。
  判「首段是 data」而不是「任意一段是 data」：否则
  `os.path.join('tpops_kafka_collector', 'mapping', 'data')`（包内路径）会被误报。
- 反例证据（把检测器改回修复前的盲区）：

```
########## M1 变异：去掉分段拼接检测（回到修复前的盲区） ##########
tests/unit/test_package_boundary.py:291: AssertionError
=========================== short test summary info ============================
FAILED tests/unit/test_package_boundary.py::test_the_boundary_detectors_catch_a_synthetic_violation
FAILED tests/unit/test_package_boundary.py::test_the_repository_root_data_detector_actually_detects
2 failed, 134 passed in 1.22s
########## M1 改回 ##########
136 passed in 1.13s
```

  同时在 `test_the_repository_root_data_detector_actually_detects` 里补了
  **分段拼接的违规样例**与**包内拼接的不误报样例**两条断言。

### M2 —— `test_package_has_no_module` 是摆设

- 位置：同上文件。
- 改了什么：**没有删测试**，而是把它从「断言包目录存在」（与边界守卫毫无关系的重复断言）
  换成**有约束力的守卫自检**，并按实际职责改名：
  `test_package_has_no_module` → **`test_the_boundary_detectors_catch_a_synthetic_violation`**。
- 现在它做什么：在 `tmp_path` 里造一个「假的违规包」，三个模块 ——
  `import tools.mappings`（违反 import 边界）、
  `os.path.join('data', 'mapping', 'metric_rules.json')`（违反文件读取边界）、
  `os.path.join('tpops_kafka_collector', 'mapping', 'data')`（**必须不报**）；
  断言前两个**必须被抓到**、第三个**必须不被误报**。
- 依据：边界守卫若恒绿，最先坏掉的是「检测器有没有能力」这一层；这条自检把那一层也钉住。
- 反例证据：见上面 M1 的变异输出 —— 检测器一旦退化，这条**立刻变红**
  （`FAILED ...::test_the_boundary_detectors_catch_a_synthetic_violation`）。

### M3 —— `main.py` 探针的成功路径未覆盖

- 位置：`tests/integration/test_main.py`（该仓库里没有 `tests/unit/test_main.py`，
  入口相关的测试都在这里 —— 按现状放置）。
- 改了什么：补 3 条用例，覆盖 `main.py` 里 `_tpops_state` / `_scheduler_state` 的**三个分支**：
  - `test_tpops_probe_reports_ready_when_authenticated` —— 认证正常 → `_tpops_state` 返回 `True`（成功路径）。
  - `test_scheduler_probe_reports_ready_after_a_successful_tick` —— 刚成功跑完一轮 → 返回 `True`（成功路径）。
  - `test_scheduler_probe_reports_never_successful_yet` —— 一轮都没跑过 → 返回
    「调度器还没有成功完成过一轮采集」（第三分支）。
- 依据：`main.py` 的探针有 4 个可观测分支（已认证 / 认证失败 / 未认证 / 就绪），
  原先只覆盖了「认证失败」「未认证」「过旧」三条，**成功路径一条没有** ——
  「恒返回 False」的变异（探针坏了恒不就绪）不会被任何用例抓到。
- 反例证据：M3 的三条在 I2 变异（`_scheduler_state` 恒 `True`）下，
  `test_scheduler_probe_reports_never_successful_yet` 与
  `test_scheduler_probe_reports_a_stale_last_success` **都变红**（见 I2 变异输出）。

### M4 —— `test_executors.py:251` 写死 `== 8`

- 位置：`tests/unit/test_executors.py::test_alarm_external_calls_scale_with_page_count`。
- 改了什么：把 `assert outcome.external_calls == 8` 换成**从实际发生的事算出来**的期望值：
  - 形态数 `_alarm_request_shapes(client)` —— 从**请求体**里的
    `is_active` / `is_convergence` 组合去重得到（并断言 4 种组合都拉到了）；
  - 每形态页数 `_alarm_page_count(len(records), ALARM_PAGE_LIMIT)` —— 由条数与每页容量算出
    （`ALARM_PAGE_LIMIT` 取生产常量 `tpops.alarms.DEFAULT_LIMIT`）；
  - 期望值 = 形态数 × 每形态页数；
  - 再补一条**关系型**断言：条数翻倍（150 条 vs 一页装得下的 100 条）→
    告警接口的请求数**必须涨**（`client.count_of(ALARM_RECORDS_PATH) > single.count_of(...)`），
    以及单页形态的调用数同样用算出来的期望值断言。
- 依据：自家纪律「测试里**不写死**任何调用次数」（该文件开头就是这么写的），
  写死 8 与之相悖 —— 平台改分页大小或形态数时，测试要么误报要么漏报。
- 反例证据（把生产代码的翻页改坏）：
  ```
  ########## M4 变异：告警接口不翻页 ##########
          assert len(shapes) == 4, (
  >       assert outcome.external_calls == expected, (
  E       assert 4 == 8
  FAILED tests/unit/test_executors.py::test_alarm_external_calls_scale_with_page_count
  1 failed, 7 passed in 1.04s
  ########## M4 改回 ##########
  8 passed in 0.88s
  ```
  （变异改的是 `tpops_kafka_collector/tpops/alarms.py` 的 `if len(page.records) < limit:`
  → `if True:`；验证后已改回，`git diff --stat tpops_kafka_collector/tpops/alarms.py` → 空。）
- 另一条证据（期望值确实是**算出来**的，不是换了个写死的数）：
  把夹具从 150 条改成 250 条（→ 3 页/形态 → 期望 12），测试**仍然绿**：
  `8 passed in 0.95s`（改回 150 条后同样 `8 passed in 0.95s`）。

---

## 实测结果（全部真实输出）

### 1. 全量测试

```
$ conda run -n tpops python -m pytest -q
........................................................................ [ 93%]
........................................................................ [ 97%]
..................................                                       [100%]
1690 passed in 80.78s (0:01:20)
```

基线 1683 passed → **1690 passed**（+7 = I1 三条 + I2 正向一条 + M3 三条；M1/M2 是改造既有用例，
条数不变）。**无回归**。

### 2. ruff（生产代码）

```
$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!
```

### 3. Python 3.7 兼容性

```
$ conda run -n tpops python -m pytest -q tests/test_py37_compat.py
..........                                                               [100%]
154 passed in 1.24s
```

### 4. print 计数

```
$ grep -rn "print(" tpops_kafka_collector/ | wc -l
0
```

### 5. 生产文件行数（合同：单文件 < 400）

```
$ find tpops_kafka_collector -name "*.py" -exec wc -l {} + | sort -rn | head -3
 12920 total
   399 tpops_kafka_collector/tpops/command_center.py
   399 tpops_kafka_collector/kafka/producer.py
```

### 6. AST 扫描（合同：函数 < 50 行）

```
$ python3 - <<'EOF'
...（脚本见验收清单）
EOF
OVER-50: 无
```

### 7. 改动范围（`git status --porcelain`）

```
 M tests/integration/test_main.py
 M tests/unit/test_executors.py
 M tests/unit/test_lifecycle.py
 M tests/unit/test_package_boundary.py
```

（`changes/**` 下那 3 个 json 与 1 个未跟踪 md 是**上一轮遗留**的控制器账本改动，
本轮未触碰。**生产代码零改动**：`git diff --stat tpops_kafka_collector/` 为空。）

### 8. 测试文件的 ruff 增量（同项目配置下 before/after 对比）

| 规则 | before | after | 说明 |
|---|---|---|---|
| UP031 | 18 | 18 | 我新增的消息文案一律用 f-string，零新增 |
| UP004 | 11 | 14 | 新增 3 个假类沿用同文件既有写法 `class X(object)`（与邻近假类一致） |
| 其余 | 不变 | 不变 | |

（测试目录本来就不在 ruff 的验收口径内 —— 验收只跑 `ruff check tpops_kafka_collector/`；
但我仍把新增的 UP031 全部清零，不给仓库加新的 lint 债。）

---

## 发现但未修的问题

1. **`tpops_kafka_collector/mapping/table.py:15-16` 仍然指向仓库根**：
   ```python
   REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
   DATA_DIR = os.path.join(REPO_ROOT, 'data', 'mapping')
   ```
   `MappingTable` 的默认参数 `data_dir: str = DATA_DIR`（`table.py:147`）就是**仓库根**的
   `data/mapping`。生产代码路径上目前没有调用方用这个默认值
   （`loader.load_mapping()` 默认走包内 `PACKAGE_DATA_DIR`；`mapping/loader.py` 只从
   `.table` 导入 `AlarmDefinition` / `MetricRule` / `build_alarm_definition`），
   所以是**潜伏**问题 —— 但只要有人调 `MappingTable()` 不传 `data_dir`，采集端就会去读仓库根
   `data/mapping`，部署产物不自洽（正是这条边界要防的事）。
   **本轮没修**：`table.py` 不在我的文件归属内；而且检测器**不该**用「任意一段是 data」来判
   （否则 `os.path.join(PACKAGE_DATA_DIR, 'data')` 这类包内路径会被误报）。
   建议下一轮：把 `table.py` 的默认 `data_dir` 改成包内 `mapping/data`，
   或让 `MappingTable` 的 `data_dir` 变成**必填**参数。
2. **仓库根 `data/` 的锚定式拼接检测不到**：当前检测器只认「首段是 `data`」的**相对**路径。
   `os.path.join(REPO_ROOT, 'data', 'mapping')` 这种**以仓库根为锚**的写法扫不到 ——
   锚点变量不是字面量，静态判断不了（要判断 `REPO_ROOT` 是不是仓库根）。
   上面第 1 条就是这个形状。彻底解决需要「模块路径 → 仓库根」的解析，属更大改动。
3. `tests/integration/test_main.py::test_real_process_receives_sigterm_and_exits_zero`
   在一次变异运行里红过一次、复跑即绿 —— **疑似子进程演练用例的时序敏感**（不是本轮引入的）。
   若控制器后续看到它偶发红，可单独看。

---

## 偏离说明

1. **改了测试名**：`test_package_has_no_module` → `test_the_boundary_detectors_catch_a_synthetic_violation`。
   理由：原名描述的是「包里没有模块」，而改造后的职责是「检测器抓得到违规」，
   名字不改会误导读代码的人。**测试没有被删掉**，约束力反而变强（见 M2）。
2. **M4 的期望值里仍有 `4`**：但它是**平台接口的结构**（`is_active` × `is_convergence`
   两个布尔 → 四种组合），而且是用「实际请求体里出现了几种组合」**数出来**的，
   不是写死的调用次数。写死的是「调用次数」这一层，纪律的落点没破。
3. **临时变异动过两个非归属的生产文件**（`tpops/alarms.py`、`main.py`、`lifecycle.py`），
   全部**只用来看守卫生效**，验证后立即还原；`git diff --stat tpops_kafka_collector/` 为空可证。
4. **没有新增依赖**：本轮只用了标准库（`ast` / `os` / `io` / `re` / `pytest`）与仓库内已有模块。
