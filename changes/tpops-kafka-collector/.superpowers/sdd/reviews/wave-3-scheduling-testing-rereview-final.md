# Wave 3 定向复审总报告（终稿）—— wave-3-scheduling-testing

> 本文件是**三轮定向复审的最终归账**。分报告同目录：
> `wave-3-scheduling-testing-rereview-a.md`（A1~A5 行为缺陷）、
> `wave-3-scheduling-testing-rereview-b.md`（守卫与硬约束）、
> `wave-3-scheduling-testing-rereview-2.md`（终审，修复轮 2）。
> 三份分报告独立取证、互不通气；本文件只做归并、去重与定级。

- **波次**：`wave-3-scheduling-testing`
- **首轮审查范围**：`356d52f → 3a71cb6`（**fail**，9 Important）
- **修复轮 1**：`2305cea`
- **定向复审（轮 1）范围**：`3a71cb6 → 2305cea`（**fail**，2 Important）
- **修复轮 2 + 终审收口**：`49bbd29` → `15a076b`
- **终审范围**：`2305cea → 15a076b`
- **结论**：**verdict = pass**
- **计数**：**Critical 0 / Important 0 / Minor 0（未闭环项）**

---

## 一、三轮的账（每条发现 → 闭环方式 → 验证方式）

| 轮次 | 判定 | Important | 闭环情况 |
|---|---|---|---|
| 首轮独立审查 | fail | 9 | 全部闭环（修复轮 1） |
| 定向复审 轮 1 | fail | 2 | 全部闭环（修复轮 2） |
| 终审 轮 2 | pass | 1（判定不阻塞）+ 1 Minor | **控制器当场闭环**（`15a076b`） |

### 首轮 9 条 Important

**行为缺陷 5 条**

| # | 内容 | 闭环 | 验证方式 |
|---|---|---|---|
| A1 | 采集节奏 = 采集耗时 + 间隔，比客户档位长 ~29%（82/98 指标欠服务） | `lifecycle.run_loop` 对齐 tick 边界 | 复审方自写时间线推演：monitor/alarm **30.0s**、command_center **60.0s**、diagnostic **120.0s**（恰为 ×1.0）；变异 → 2 条断言红 |
| A2 | 「超时 30 秒放弃等待 → 不挂死」在真实路径不成立（py3.7 `atexit` join 线程池） | `main.exit_process` 走 `os._exit` + 线程池 `shutdown(wait=False)` | 变异 `os._exit`→`sys.exit` → 守卫红（「进程在放弃窗口内没有退出 —— 挂死了」）；**控制器独立复核过同一条** |
| A3 | 单轮外部调用数是写死常数 → 42 次预算告警永不触发 | `TpopsClient._send` 真实计数（全局锁 + 每线程 local）+ 目录快照缓存 + 删掉 4 个常量 | 复放平台留档：第 1 轮报 26 = 实测 26，第 2 轮 13 = 13；变异回写死常数 → 6 条守卫红 |
| A4 | 节点清单只在启动取一次，失败就**永久降级** `resourceName` | `node_targets.py`：退避重试 3 次 → 该实例不参与采集 → 全失败启动失败退 3 | 三个变异各自红；全失败时**未构造 Kafka producer** |
| A5 | 运行期从不落盘水位/幂等键 → SIGKILL 后丢整个生命周期去重状态 | 每轮 tick 后 `watermark.save()`；新增 SIGKILL 版演练 | 变异 → 3 条红（含真 SIGKILL 演练）；开销 p95 **12.0 ms / 115 KB** |

**守卫缺失 3 条**

| # | 内容 | 闭环 | 验证方式 |
|---|---|---|---|
| B1 | 告警「投递失败不推进水位」零守卫（变异后全量仍绿 → 告警静默丢失） | 新建 `tests/unit/test_scheduler_alarm_guards.py` | **控制器独立复核**：注入 M11 → 全量 1667 例里**只有这 1 例红** |
| B2 | 告警幂等去重（批内 `seen` + 跨轮 `has_key`）零守卫 → 告警重复投递 | 同上文件 | 变异 M14 → 2 条红；再**分别单独拆** `seen` / `has_key` → 各自有专属守卫 |
| B3 | 三条「保命」分支零覆盖 | 5 条守卫 | `_tick_safely` / 退 3 带堆栈 / 认证失败 / 未认证 / 「跑了但全挂」503 |

**硬约束 1 条**

| # | 内容 | 闭环 | 验证方式 |
|---|---|---|---|
| C1 | `platform_profile.reconcile` 55 行 > 50 行 | 拆成 5 个函数 | 复审方**自己 AST 扫全包**：0 个 >50 行函数、0 个 ≥400 行文件；4 个私有函数覆盖率 **100%** |

### 定向复审轮 1 的 2 条 Important（同指一个缺口：`/readyz` 就绪判定）

| # | 内容 | 闭环 | 验证方式 |
|---|---|---|---|
| I1 | `TrackedScheduler.tick` 的「全挂不算成功」闸门零守卫 → 采集端每轮全挂时 `/readyz` 仍报 200 就绪 | 3 条守卫（生产实现未动） | 变异 `if result.tasks_failed == 0:` → `if True:` → 2 条红；`if False:` → 2 条红。**控制器独立复核**：`last_tick_ok_at` 在 `tests/` 里原本只被赋值从不被断言 |
| I2 | `/readyz` 的 503 守卫**被 tpops 探针遮蔽**（FakeClient 从未认证 → 503 本来就来自 tpops 探针；断言只查 `"name": "scheduler"` 恒真）→ **恒绿** | `FakeClient` 补 `auth_ok=True` 使归因唯一；断言落到**逐探针的 ready + reason 文本**；补 200 正向对照 | 变异 `_scheduler_state` → 恒 `True` → 503 用例红（`assert 200 == 503`） |

### 终审轮 2 的残留（控制器当场闭环，`15a076b`）

| # | 内容 | 闭环 | 验证方式 |
|---|---|---|---|
| M1 残留 | 边界检测器只认「首段是 `data` 字面量」，抓不到 `os.path.join(REPO_ROOT,'data',...)` / `Path(REPO_ROOT)/'data'/...` —— **而这正是生产包原存写法**（`mapping/table.py:16`） | ① 生产侧去掉仓库根默认（`MappingTable.load()` 的 `data_dir` 改**必填无默认**，删除 `REPO_ROOT`/`DATA_DIR` 常量）；② 测试侧显式传路径（`conftest.MAPPING_DIR`）；③ 检测器补强（新增 `_mentions_repo_root` / `_mentions_this_file`，同时抓 join 与运算符链两种写法） | **控制器端到端验证**：把 `table.py:16` 的写法**原样**塞进生产包 → 边界守卫立刻红（点名文件行号）；守卫自检扩到**四种违规写法 + 两条不误报**（包内 `mapping/data`、基于 `__file__` 的包内数据目录） |
| Minor | `test_readiness_returns_503_before_first_tick` 弱断言（503 实来自 tpops 探针） | 同样补 `auth_ok=True` + 断言落到 scheduler 探针的**具体 reason 文本** | 变异 scheduler 探针 → 该用例立刻红，还原后绿 |

---

## 二、最终门禁（控制器自跑，非采信任何一方报告）

```
pytest -q                                  1692 passed
ruff check tpops_kafka_collector/          All checks passed!
pytest -q tests/test_py37_compat.py        154 passed（Python 3.7.9-final-0）
print( 计数                                0
AST 扫描 >50 行函数                        无
最大文件                                    399 行
```

**硬约束核对**：py3.7.9 语法红线（154 例门禁守着）· 无新依赖 · 单文件 < 400 行 · 函数 < 50 行 ·
不可变数据 · 凭据只走环境变量（不写日志 / 不进异常消息 / 不进健康响应）· 不用 `print` ·
生产代码唯一（不 import `tools.*`、不读仓库根 `data/`）· 消息只有一个序列化出口 ·
未改 contract / tasks / specs / `data/mapping/*.json`。

---

## 三、三轮复审沉淀下来的经验（写给下一个波次）

1. **完成标准通过 ≠ 波次通过。** 首轮那 5 条完成标准**全过**，但完成标准本身没覆盖
   「在途请求退出」「调用数真实性」「告警通道守卫」「非优雅退出续传」四类 ——
   判定范围比风险面窄。**完成标准是必要条件，不是充分条件。**
2. **实现对了 ≠ 修完了。** 本波 5 条 Important 里，有 3 条（B1/B2/B3/I1）**生产实现本来就是对的**，
   缺的是守卫。「守卫为假」比「实现错」更隐蔽 —— 它骗过了所有人两轮。
3. **恒绿的测试比没有测试更危险。** I2 那条 503 用例看着在测 scheduler 探针，
   实际测的是 tpops 探针 —— 它「一直绿」，绿得让人放心。
4. **守卫自己也要被验。** 每条新守卫都要问：**有没有一个能让它失败的小改动？**
   本波共做 **20+ 条变异注入**，控制器亲自验了 B1 与 A2 两条，其余由复审方独立验。
5. **「实测数字」要问它什么规模。** 「单轮 13 次调用远低于 42」这句话在留档规模下成立，
   复审方复算后指出真实规模下会超（500 表=33、1000 表=58、2000 表=108）——
   **数字对了，规模没问**。已登记 T-33。
6. **审查要分维度并行。** 三个互不通气的审查方 —— A 维度 pass、B 维度 fail ——
   分歧恰好暴露了「完成标准覆盖面」的问题。合并报告的价值不在于投票，
   而在于**把分歧摆到台面上**。

---

## 四、签发 receipt

```
ssf execution review changes/tpops-kafka-collector \
  --wave wave-3-scheduling-testing --base 2305cea --head 15a076b \
  --report .superpowers/sdd/reviews/wave-3-scheduling-testing-rereview.md --verdict pass
```

> 本轮之后 wave-4（交付文档）解锁。
> ⚠️ 签收可能清掉 `plans/<hash>/workspace/`（历史坑）；`plans/` 已被 git 跟踪，
> 若被清则用 `git checkout 15a076b -- "changes/tpops-kafka-collector/.superpowers/sdd/plans/"` 恢复。
