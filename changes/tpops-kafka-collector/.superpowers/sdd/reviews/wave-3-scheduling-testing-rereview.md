# Wave 3 定向复审报告 —— wave-3-scheduling-testing

> 本文件是**两份定向复审分报告的合并总报告**。分报告同目录：
> `wave-3-scheduling-testing-rereview-a.md`（五条行为缺陷 A1~A5）、
> `wave-3-scheduling-testing-rereview-b.md`（守卫与硬约束 B1/B2/B3/C1）。
> 两者独立取证、互不通气；本总报告只做归并与定级。

- **波次**：`wave-3-scheduling-testing`
- **base**：`3a71cb6`（上一轮 fail receipt 的 head）
- **head**：`2305cea`（修复轮提交）
- **结论**：**verdict = fail**
- **计数**：**Critical 0 / Important 2 / Minor 6**

---

## 一、复审分工与结论

| 维度 | 复审方 | 结论 | Critical | Important | Minor |
|---|---|---|---|---|---|
| 五条行为缺陷（A1~A5） | R2-A | **pass** | 0 | 0 | 4 |
| 守卫与硬约束（B1/B2/B3/C1） | R2-B | **fail** | 0 | **2** | 4 |

**合并判定 = fail**（任一维度有 Important 即 fail）。

---

## 二、A 维度：五条行为缺陷 —— 全部闭环 ✅

R2-A **自己重新做了变异验证**（21 条变异逐条验，改完还原并与工作树逐字节比对），
不采信修复方的报告。

| 项 | 判定 | 复审方实测证据 |
|---|---|---|
| **A1** 节奏对齐 tick 边界 | ✅ 已闭环 | 自写时间线推演（真 `Scheduler` + 真 `DueTable`）：monitor/alarm **30.0s**、command_center **60.0s**、diagnostic **120.0s** —— 恰为档位 ×1.0。变异（`waiter(remaining)`→`waiter(interval)`）使其 **2 条断言同时变红**。超时场景（40s 轮次 vs 30s 间隔）保持严格算术（0/40/80/120…）：**无补偿风暴、无背靠背、无漂移** |
| **A2** 退出不挂死 | ✅ 已闭环 | 在途请求挂住 → **退出码 0**、在放弃窗口内退出、水位文件完整。变异 `os._exit`→`sys.exit` → 两条守卫变红（45.56s 超时 = atexit 挂死）。复审方**另行核了 `os._exit` 的副作用**：producer 已关闭、消息已投出、水位文件含 `metric_watermarks` **与** `idempotency_keys`、stdout+stderr 已 flush；进程内调 `main()` 返回 3 且进程存活 |
| **A3** 真实调用数 | ✅ 已闭环 | 计数点在 `client.py:326` 的 `_send()` 内 → 认证 / 401 刷新 / 重试**全计入**。用平台留档喂真执行器 + 自计数传输层复放：**第 1 轮报 26 = 实测 26；第 2 轮（目录已缓存）13 = 13**。变异回写死常数 → **6 条守卫变红**。并发自查：8 线程 × 5 次 → 各 delta=5、合计 40 |
| **A4** 节点清单 | ✅ 已闭环 | 真实 3 次退避重试（变异 `max_attempts=1` → 红）；失败实例被排除出采集（变异 → 红）；全失败 → **退出码 3 且未构造 Kafka producer**；`exc_info` 带堆栈 |
| **A5** 每轮落盘 | ✅ 已闭环 | `save()` 写入全部四段；SIGKILL 三阶段演练通过；变异 → **3 条红**（含真 SIGKILL 演练）。开销 **p95 = 12.0 ms / 115 KB 文件 = 30 秒轮的 0.04%** |

**新增文件核查**：无循环依赖（静态图 + 运行时 + 隔离导入三查）；ruff 全过；无 ≥400 行文件；最长函数 49 行（C1 闭环）；`print` 计数 0；py3.7.9 `compileall` 通过；无新依赖；禁改文件未动。

**跨组干扰核查**：R-A 把 `tests/unit/test_scheduler.py` 的一条断言改成与措辞无关的 `dropped_unsupported=3` ——
复审方把被记录的值变异成 4 → **立刻变红** → **不是恒绿回归**，措辞另有守卫。
`_install_counting_client` 写命令中心私有属性 `command_center._client` —— 无破坏性副作用，三跳经代理仍通。

---

## 三、B 维度：守卫与硬约束 —— 3 条闭环、**2 条 Important 未闭环** ❌

| 项 | 判定 | 复审方实测证据 |
|---|---|---|
| **B1** 告警失败不推进水位 | ✅ 已闭环 | M11 变异 → `test_alarm_watermark_does_not_advance_when_alarm_delivery_fails` 红，**且只有那一条红**（无掩盖）。断言的是 `WatermarkStore` 真实水位状态，非 mock 调用次数 |
| **B2** 告警去重 | ✅ 已闭环 | M14 变异 → 2 条红；再**分别单独拆** `seen` 与 `has_key` → 各自有专属守卫 |
| **C1** `reconcile` 拆分 | ✅ 已闭环 | R2-B **自己 AST 扫全包**：0 个函数 body > 50 行、0 个文件 ≥ 400 行（最大 42 行）；4 个拆出的私有函数覆盖率 **100%**，逐个变异各 1 条守卫红；docstring 已与 `reconcile=True` 实现一致 |
| **B3** 三条保命分支 | ❌ **未闭环** | 见下面 I1 / I2 —— **B3 的第三条（`/readyz`「跑了但全挂」）修成了恒绿守卫** |

首轮审查指出的 `scheduler_delivery.py` 零覆盖行 `124/127/134-139` **现已 100% 覆盖**；
映射层 21 个文件逐个 100%、合计 100%；TOTAL 95%；ruff 全过；py37 兼容 154 passed（Python 3.7.9-final-0）。

---

## 四、发现

### Critical（0）

无。

### Important（2）

#### I1. `TrackedScheduler.tick` 的「全挂不算成功」闸门零守卫 —— 采集端每轮全挂时 `/readyz` 仍报 200 就绪

- **File**：`tpops_kafka_collector/lifecycle.py:87-91`
  ```python
  def tick(self) -> TickResult:
      result = super().tick()
      if result.tasks_failed == 0:      # ← 这道闸
          self._last_tick_ok_at = self._wall_clock()
      return result
  ```
- **问题**：这道闸是「**一轮全挂就不算成功**」的唯一实现。拆掉它 → `_last_tick_ok_at` 每轮都更新 →
  `_scheduler_state`（`main.py:278-285`）永远认为「最近 3 分钟内成功完成过一轮」→ **就绪探针报 200**。
  **实测确认**：拆掉闸门后 **1683 passed 无一红**；独立探针确认语义真变了。
- **触发条件**：任何对 `tick()` 返回结构的改动、或一次「把 `_last_tick_ok_at` 改成无条件更新」的简化。
- **后果**：**采集端一条数据都投不出去，运维从 `/readyz` 看到的却是"就绪"** ——
  监控探针不会告警、值班不会介入、数据静默为空。这正是本项目最贵的那类缺陷形态。
- **佐证（控制器独立复核）**：`last_tick_ok_at` 在整个 `tests/` 目录里**只被赋值、从不被断言**
  （只有 `test_main.py:800` 的假 scheduler 与 `:813` 的直接赋值）；
  `tasks_failed` 有 8 处断言但**从不与 `last_tick_ok_at` 组合** → 这条不变量**零守卫**。
- **修法**：补守卫测试 ——
  ① 一轮 `tasks_failed > 0` → `last_tick_ok_at` **保持 `None`**；
  ② 一轮 `tasks_failed == 0` → `last_tick_ok_at` 被更新。
  **变异验证**必须做（把闸门拆掉 → 守卫变红）。

#### I2. `/readyz` 的 503 端到端守卫**被 tpops 探针遮蔽** —— 503 的归因是错的，这条测试恒绿

- **File**：`tests/integration/test_main.py:807-818`（`test_readiness_returns_503_when_the_last_round_failed`）
- **问题**：该测试用 `FakeClient` —— 它**从未完成认证** → `_tpops_state`（`main.py:262-275`）返回
  「TPOPS 尚未完成认证」→ **503 本来就来自 tpops 探针**，与 scheduler 探针无关。
  而断言只检查 `b'"name": "scheduler"' in body` —— 响应体里**每个探针的 name 都在**，所以这条断言**恒真**。
- **实测确认**：把 scheduler 探针变异成恒返回 `True` → 该测试**仍然绿**；
  给 `FakeClient` 补上 `auth_ok`（让 tpops 探针就绪）→ 立刻变红（`200 != 503`）→ **恒绿**。
- **后果**：`/readyz` 的「跑了但全挂」这条规格要求**实际上没有守卫**——
  探针实现改成永远就绪，测试也发现不了。
- **修法**：① 让 `FakeClient` 在该用例里**已认证**（`auth_ok` 为真），使 503 **只能**来自 scheduler 探针；
  ② 断言响应体里 scheduler 探针的**具体状态文本**（不只是 name）；
  ③ 再加一条「tpops 与 scheduler **都**就绪 → 200」的正向用例，两条合起来才锁住 503 的归因。

### Minor（6）

| # | 内容 | 位置 |
|---|---|---|
| M1 | `data/` 检测器漏 `os.path.join('data', ...)` 分段拼接 | `tests/unit/test_package_boundary.py` |
| M2 | `test_package_has_no_module` 是摆设 | 同上 |
| M3 | `main.py` 探针**成功路径**（270/275/285 行）未覆盖 | `main.py:262-285` |
| M4 | `test_executors.py:251` 写死 `== 8`，与自家「不写死调用次数」的纪律相悖 | `tests/unit/test_executors.py:251` |
| M5 | **规格里的「单轮外部调用数 ≤ 42」在真实规模下会超** —— 两个分页视图每表一行、每页 100 行 → 每多一页多 5 次请求：500 表=33、**1000 表=58（>42）**、2000 表=108。留档实测 13/26 是因为该实例表数少。**代码是对的**（真实计数 + 超预算如实告警）；**不要把数字改回假值**（那正是 A3 的病） | 已登记 T-33 |
| M6 | **`POLL_INTERVAL_SECONDS` 改成 60 会静默降级 30 秒档**（默认 30 恰为最紧档位），无日志、无告警、无守卫 | 已登记 T-34 |

---

## 五、交叉验证（一个维度反证另一个维度）

R2-B 顺带对 R2-A 声称闭环的 A1 / A2 / A5 做了变异验证 —— **三条都有真守卫，不是恒绿摆设**，
A 维度结论**没有反证**。两个维度的结论互不矛盾，可以合并。

---

## 六、修复轮 2 的范围（下一轮必须闭环的项）

| # | 内容 | 归属建议 |
|---|---|---|
| **I1** | 补 `TrackedScheduler.tick` 的两条守卫（全挂不更新 / 成功才更新）+ 变异验证 | 生命周期层 |
| **I2** | 修 `/readyz` 503 守卫的归因（FakeClient 已认证 + 断言具体状态文本 + 补正向 200 用例）+ 变异验证 | 健康端点/装配层 |
| M1~M4 | 四条 Minor 的测试质量修正（检测器覆盖分段拼接、去掉摆设断言、补探针成功路径、把写死的 `== 8` 改成不写死） | 同上 |

**修复轮 2 的硬约束**：同 wave-3（py3.7.9 语法红线、不新增依赖、单文件 < 400 行 / 函数 < 50 行、
不可变数据、凭据只走环境变量、`logging` 不用 `print`、不静默出 0、
不改 contract / tasks / specs / data）、实施方**一律不 commit**、每条修复配变异验证。

---

## 七、签发 receipt

```
ssf execution review changes/tpops-kafka-collector \
  --wave wave-3-scheduling-testing --base 3a71cb6 --head 2305cea \
  --report .superpowers/sdd/reviews/wave-3-scheduling-testing-rereview.md --verdict fail
```

> 修复轮 2 的 receipt `--base` 必须等于本轮 head `2305cea`。
