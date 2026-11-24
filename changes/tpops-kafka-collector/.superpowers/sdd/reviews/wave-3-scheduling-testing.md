# Wave 3 审查报告 —— wave-3-scheduling-testing

> 本文件是**三份分报告的合并总报告**（控制器签收用）。分报告同目录：
> `wave-3-scheduling-testing-contract.md`（合同与规格合规）、
> `wave-3-scheduling-testing-correctness.md`（数据正确性 + 变异验证）、
> `wave-3-scheduling-testing-quality.md`（代码质量、最小化与范围）。
> 三份分报告各自独立取证、互不通气；本总报告只做**归并、去重与定级**。

- **波次**：`wave-3-scheduling-testing`
- **base**：`356d52f`（wave-2b pass receipt 的 head）
- **head**：`3a71cb6`（当前 HEAD）
- **审查范围**：任务 3.1 调度器 / 3.2 健康端点 / 3.3 入口装配 / 3.4 回归夹具 / 3.5a 覆盖率补测 / 3.5b 全量门禁
- **结论**：**verdict = fail**
- **计数**：**Critical 0 / Important 9 / Minor 17**

> **为什么是 fail**：分报告结论不一致 —— 合同与规格维度判 **pass**（0 Important），
> 代码质量维度与数据正确性维度各判 **fail**（6 / 4 Important）。
> 控制器复核后**采信 fail**：
> 合同维度的 5 条完成标准确实全部通过，
> 但完成标准本身**没有覆盖**「在途请求被放弃时的退出」「外部调用数的真实性」
> 「告警通道的守卫」「非优雅退出后的续传」这四类 ——
> 也就是说 **pass 的判定范围比实际风险面窄**，不是「没问题」。
> 另有 2 条属于**对客户承诺的静默违约**（采集节奏慢 29% / 预算告警永不响），
> 按项目「不报错但数据错」的最高优先级判 Important。

---

## 一、合同完成标准逐条判定

| # | 标准 | 判定 | 证据 |
|---|---|---|---|
| 1 | `pytest --cov=tpops_kafka_collector --cov-fail-under=80 -q`；映射层与规则引擎 ≥95%（**逐文件**且**合计**） | **通过** | 1560 passed；TOTAL **95.12%**；映射层 21 个文件最低 `rules_sorting.py` 98%、其余 100%，合计 **99.92%** |
| 2 | `pytest tests/test_py37_compat.py -q` | **通过** | **147 passed**（整套测试就跑在 python 3.7.9-final-0 上） |
| 3 | `ruff check tpops_kafka_collector/` | **通过** | All checks passed |
| 4 | 杀进程 → 重启演练：不重复、不丢失 | **部分通过** | `tests/integration/test_kill_restart_drill.py` 4 例可执行；真进程 SIGTERM → 退出码 0、水位文件完整。**但只有 SIGTERM 版本，无 SIGKILL**；且运行期从不落盘（见 Important A5） |
| 5 | 60 秒档单轮墙钟 P95 ≤ 25 秒 | **通过** | 独立复跑 `measure_round_latency.py` → **P95 = 15.017s**（min 15.006 / max 15.017）。**注意**：为合成执行器推算，非真平台端到端（Minor 5） |

**合同完成标准整体：4 通过 / 1 部分通过。** 但见 Important A1~A3、A5 —— 完成标准没覆盖到的部分存在实质缺陷。

---

## 二、规格逐条判定

### `specs/delivery-and-state/spec.md`

| Requirement | 判定 | 证据 |
|---|---|---|
| 调度器的到期驱动与并发组 | **通过** | 到期判定正确；组×实例两维并发有 Barrier 确定性守卫（非计时断言）；单组失败不拖垮其他组 |
| 单轮性能预算 P95 ≤ 25 秒 | **通过** | 15.017s（合成执行器口径） |
| **单轮外部调用数与投递成功率可观测** | **不通过** | 上报值是**写死的常数**（monitor=2 / diagnostic=1 / instance=2 / alarm=4，B 类报页数），真实单轮已 >42 却报 20 → **42 次预算告警永不触发**。见 Important A3 |
| 平台不保存历史快照 | **通过** | 无历史快照落盘路径 |
| 水位与差分基线的持久化 | **部分通过** | 原子写、只在投递成功后推进、批次原子推进 —— 均通过；但**运行期不落盘**（Important A5） |
| 幂等去重 | **部分通过** | 指标通道有守卫；**告警通道零守卫**（Important B1/B2） |

### `specs/runtime-and-config/spec.md`

| Requirement | 判定 | 证据 |
|---|---|---|
| 优雅退出：SIGTERM → 停发新请求 → 等在途（**超时 30 秒放弃等待**）→ 原子写水位 → flush producer → **exit 0** | **不通过** | 「超时 30 秒放弃等待 → 不挂死」在**真实路径**上不成立：py3.7 的 `atexit.register(_python_exit)` 会 join 线程池工作线程。实测进程被拖 11 秒（脚本本身只 sleep 0.2s）；生产最多拖 `TPOPS_REQUEST_TIMEOUT=60` 秒。见 Important A2 |
| 健康端点 `/healthz` / `/readyz` | **通过** | 认证不可用 → 503；响应体不泄露凭据。但「跑了但全挂」分支零覆盖（Important B3） |
| 每轮结构化汇总日志 | **通过** | 11 个字段名逐字一致；各计数自洽；JSON 格式实测 54 行逐行 `json.loads` 通过 |
| 结构化数据不可变 | **通过** | frozen dataclass / NamedTuple，无原地改 |

### `specs/mapping-and-rules/spec.md`

映射层本轮只补测不改实现 —— **未被改坏**，逐文件覆盖率 98%~100%。**通过。**

---

## 三、控制器定死的 10 条口径逐条判定

| # | 口径 | 判定 | 证据 |
|---|---|---|---|
| 1 | 水位 `stream` 键 = `f'{instance_id}:{channel}'` | **通过** | 含 instance_id；变异 M2（去掉）→ 守卫红 |
| 2 | 差分基线恢复只用 `import_baselines_from` / `export_baselines_into`，不用 `put_baseline` | **口径与实现不符** | `put_baseline` 存的键与 `import_baselines_from` **完全相同**（作用域本就在 `dimension_key` 里）→ 简报必做变异 #4 **不可区分**（M4 全绿）。真正可测的 M4b（落盘剥作用域）→ 红。见 Minor 4 |
| 3 | 差分基线作用域 = `f'{instance_id}:{node_id}'` | **通过** | 变异 M3（去掉）→ 守卫红，**回归层也红**（M-N4 生效） |
| 4 | 组 × 实例两维并发 | **通过** | 变异 M5（改串行）→ 守卫红（Barrier 确定性断言） |
| 5 | 平台画像「选择所有实例」 | **通过** | 不挑、不筛、不取第一个、不按 mode 筛 |
| 6 | `master_count` 按唯一 `node_id` 计数 | **通过** | 复用 `_select_masters` 去重逻辑 |
| 7 | `dropped_unsupported` 计数 | **通过** | 实测 = 3 |
| 8 | 汇总日志字段名 | **通过** | `tick/elapsed/fetched/mapped/produced/failed/dropped_unsupported/windows/alarms` + `calls`/`delivery_rate` 逐字一致 |
| 9 | 日志中心化 | **通过** | 消费 `config.log_level` / `config.log_format`；输出 stderr；幂等（handler marker）；第三方降噪；凭据/token/SQL **不落日志**（已核） |
| 10 | 启动期配置缺失 → 非 0 退出码 + 日志指明变量名 + **未向 TPOPS/Kafka 发任何请求** | **通过** | 退出码 2；日志含变量名；**连 client / producer 都没构造** |

---

## 四、硬约束核对

| 约束 | 判定 | 证据 |
|---|---|---|
| Python 3.7.9 红线 | **通过** | 147 passed 的 py37 兼容门禁守住 |
| 不新增依赖 | **通过** | `requirements.txt` 本波无变动 |
| 单文件 < 400 行 | **通过** | 全包无 ≥400 行的文件 |
| **函数 < 50 行** | **不通过** | `platform_profile.py:243-298` `reconcile` **55 行**（AST 实测 span）。**全包唯一越线函数**。见 Important C1 |
| 不可变数据 | **通过** | frozen dataclass / NamedTuple |
| 凭据只走环境变量 | **通过** | 无硬编码；不写日志；不进异常消息；不进健康响应 |
| 不用 `print` | **通过** | 全包 `print(` 数量 = **0** |
| 生产代码唯一 | **通过** | 无 `tools.*` / 仓库根 `data.*` import；映射数据走包内 `mapping/data/` |
| 消息只有一个序列化出口 | **通过** | AST 守卫；变异注入 `json.dumps` → 守卫立刻红 |
| 不改 contract / tasks / specs / `data/mapping/*.json` / `data/merged_sql/*` | **通过** | `git diff --name-only 356d52f..3a71cb6` 核实 |
| 不静默出 0 | **部分通过** | 采集侧落实了；**告警通道的空结果语义**（全被幂等键过滤 vs 真的没数据）不可区分（Minor 3） |

---

## 五、发现

### Critical（0）

无。本轮未发现「生产实现当下就会导致数据错或数据丢」的实际缺陷。
下列 Important 中，**当前生产代码的行为是正确的**，但**缺少守卫**或**偏离已批准的口径** ——
它们属于「将来一次改动就会静默劣化」的那一类。

### Important（9）

#### A1. 采集节奏 = 采集耗时 + 轮询间隔，实际比客户声明的档位长约 29%（82/98 指标受影响）

- **File**：`tpops_kafka_collector/lifecycle.py:93-97`（`run_loop`）
- **问题**：`run_loop` 在**一轮跑完之后**才 `shutdown.wait(poll_interval_seconds)` → 实际周期 = `采集耗时 + 间隔`。
  叠加 `DueTable` 的「到期才跑」语义，每个通道的实际采集间隔都被拉长到 ~1.3 倍。
- **实测**（生产 `DueTable` 模拟时间线）：30 秒档实际平均 **38.7s**；60 秒档 `command_center` 实际 **77s**。
  客户指标库里 **82/98 条走 command_center 通道**，全部受影响。
- **偏离**：3.1 简报第 4 步明写「**sleep 到下一个 tick 边界**」。
- **性质**：**对客户刷新档位的静默欠服务** —— 日志里 `tick=` 递增正常、外部调用数正常，**没有任何信号提示节奏偏了**。
- **修法**：`run_loop` 记录本轮起点 `t0`，结束后睡 `max(0, poll_interval - (now - t0))`（对齐 tick 边界，不累积漂移）；
  并补守卫「某通道相邻两次实际采集间隔 ≤ 档位 × 1.1」。

#### A2. 「在途请求超过 30 秒 → 放弃等待 → 退出码 0、不挂死」在真实路径上不成立

- **File**：`tpops_kafka_collector/lifecycle.py:100-121` + `tpops_kafka_collector/scheduler.py:232`
- **问题**：三层叠加：
  1. `scheduler._collect_all` 用 `with ThreadPoolExecutor(...)` → 退出上下文时 `shutdown(wait=True)` → tick 线程卡在等挂死的 HTTP；
  2. py3.7 的 `concurrent/futures/thread.py` 注册 **`atexit.register(_python_exit)`**，`_python_exit` 给每个工作队列塞 `None` 并 **`t.join()`** 所有工作线程（工作线程虽是 daemon，但 atexit 钩子显式 join）；
  3. `lifecycle` 的设计意图（docstring「守护线程不拦解释器退出」）在 py3.7 上不成立。
- **实测**：脚本本身只 `sleep(0.2)`，进程实际耗时 **11 秒**（被 join 的工作线程睡了 8 秒）。
  conda env 实测确认：`python 3.7.9` 的 `concurrent.futures.thread` 确实 `atexit.register(_python_exit)` + `t.join()`。
- **生产影响**：默认 `TPOPS_REQUEST_TIMEOUT=60`。SIGTERM 后若有在途请求挂住，进程**最多再拖 60 秒**才退 ——
  超过简报的 30 秒放弃窗口，也超过 systemd 默认 `TimeoutStopSec`。被 SIGKILL 则**退出码不是 0**。
- **配套测试缺陷**：`tests/integration/test_main.py:294-333` 的守卫用 `ScriptedScheduler`（**没有线程池的假调度器**）→ 绕开了这条路径 → **恒绿**。
- **修法**：退出路径在「水位落盘 + producer flush」完成后显式 `os._exit(0)`（跳过 atexit 钩子）；
  并把守卫测试的假调度器换成**真 `Scheduler` + 真 `ThreadPoolExecutor`**。

#### A3. 「单轮外部调用数」是编造的常数，42 次预算告警形同虚设

- **File**：`tpops_kafka_collector/executors.py:55-59`（常量）、`:143/184/217/244/277`（各通道上报值）；
  `tpops_kafka_collector/scheduler_report.py:27,64-68`（预算与告警）
- **规格**：`specs/delivery-and-state/spec.md:235-240`「采集端 SHALL 在每轮汇总日志中记录本轮的**外部调用次数**…SHALL NOT 超过 42 次…超出时 SHALL 记录 WARNING」
- **问题**：5 个通道里 **4 个的 `external_calls` 是写死的常量**：
  - **A 类实际 13 次**：`_check_catalog` → `fetch_metric_catalog` 对 **12 个分组码各发一次 GET**（`tpops/metrics.py:362-367`，**每轮重拉、无缓存**）+ 1 次指标 POST → 却报 **2**；
  - **B 类实际是三跳的 3~4 倍**：`execute_sql` 一次就是完整三跳（下发 → 轮询 → 取明细），`pages` 只是翻页数，`external_calls = max(result.pages, 1)` **少算 3 倍**；
  - **告警实际是 4 × 页数**（四种形态各自分页，`limit ≤ 100`），常量 4 只覆盖「每形态一页」。
  - 合计：真实单轮已 **> 42**，上报值只有 `2+11+1+2+4 = 20` → **42 次预算告警永远不响**。
- **性质**：规格点名要求的**可观测量**现在是自造的数 → 「不报错、但运维看不见」。
- **修法**：`TpopsClient` 上挂**真实计数**（每次 `get`/`post` 递增，线程安全），各通道报真实 HTTP 次数；
  并把 A 类 12 次目录查询改成**按轮缓存**以压实际调用数。
- **⚠️ 口径待用户裁决**：真实计数落地后，若真实值仍 > 42，则按规格应当每轮 WARNING。
  规格里的 42 是按「命令中心三跳 = 3」估的，与真实的三跳 × 页数不是一个口径 ——
  **改规格会作废前四个波次的 receipt**，本变更内不能改。裁决项见账本与本轮控制器提问。

#### A4. 节点清单只在启动时取一次；取失败就**永久降级** `resourceName`，零测试、无堆栈

- **File**：`tpops_kafka_collector/main.py:222-243`（`_build_instances` / `_fetch_nodes`）
- **问题**：`nodes` 是**启动时取一次、之后永不再取**，而 `collectors/monitor.py` 用它把 series 的 `component_id`
  换算成节点身份，**`resourceName` 直接由它拼出来**。`_fetch_nodes` 捕获所有异常 → 只记一条**不带堆栈**的 ERROR → 返回空元组。
- **影响**：启动瞬间一次网络抖动 → 整个进程生命周期的 `resourceName` **永久退化成只有 node_id** →
  客户按台账查不到资源 → **数据看着有、但对不上台账**。该路径零覆盖（`main.py:238-243` 未覆盖）。
- **修法**：节点清单取不到 → **该实例本轮不采集**（不产出错的 `resourceName`），下轮重试；
  补 `exc_info=True`；补守卫「节点取不到 → 该实例不进本轮采集」。

#### A5. 运行期**从不落盘**水位 / 幂等键 —— 非优雅退出会丢掉整个进程生命周期的去重状态

- **File**：`tpops_kafka_collector/lifecycle.py:156-161`（`shutdown_runtime`）是全包**唯一**的 `watermark.save()` 调用点；
  `scheduler.py` 的 `tick()` 全程不落盘
- **问题**：水位与幂等键只在**进程退出时**写盘。规格「重启续传：进程重启后从持久化的水位继续，**不重复投递已成功的数据**」
  在**优雅退出（SIGTERM）**路径上成立，在**非优雅退出**上不成立。
- **触发条件**：SIGKILL / OOM Killer / 宿主机重启 / 容器被驱逐 —— 生产里最常见的「杀进程」。
- **后果**：重启后内存里的幂等键与水位**全部丢失** → 采集端自身的去重彻底失效 → 整个进程生命周期的数据被**整段重投**。
  A 类时序指标的键含平台数据点时间戳 → 客户侧能去重；**B 类快照指标（82/98 条）的 `clock` 是采集时刻 → 键每轮都变 →
  客户侧也无法去重**，等于把整段快照历史重发一遍。
- **佐证**：合同完成标准 #4 的杀进程演练**只有 SIGTERM 版本**，没有 SIGKILL 版本。
- **修法（推荐）**：每轮 tick 结束后调用一次 `watermark.save()`（原子写已实现好，成本只是一次小 JSON 落盘）→ 崩溃最多丢一轮；
  并补一个 **SIGKILL 版**杀进程演练，把行为钉死。

#### B1. 告警通道「投递失败不推进告警水位」**零守卫** —— 变异后全量测试仍全绿

- **File**：`tpops_kafka_collector/scheduler_delivery.py:134-139`（告警投递失败分支）
- **问题**：指标通道有 `test_watermark_advances_only_after_successful_delivery` /
  `test_watermark_does_not_advance_when_delivery_fails` 守着；**告警通道一条对应的守卫都没有**。
  变异 M11（在告警失败分支里也推进告警水位）→ **1561 passed，无一测试变红**。
- **覆盖率佐证**：`scheduler_delivery.py` 未覆盖行 = `124, 127, 134-139`，**正好是告警去重 + 告警失败分支**。
- **后果**：告警水位被推进 → 下一轮游标过滤把**从未投出的告警**判成「不新」→ **告警整条静默丢失**，
  日志里只有一条 `LOGGER.exception`，没有任何计数能暴露它。
- **实测**：干净树 `ALARM WATERMARK AFTER FAILED DELIVERY: None`；M11 变异后 `'1700000000000'`。
- **修法**：补一条与指标侧对称的守卫测试 + 在 except 分支加注释指明这条不变量。

#### B2. 告警幂等去重（批内 `seen` + 跨轮 `has_key`）**零守卫** —— 变异后全量测试仍全绿

- **File**：`tpops_kafka_collector/scheduler_delivery.py:121-129`
- **问题**：变异 M14（删掉这两道去重）→ **1561 passed，无一测试变红**。
  现有 `test_alarm_watermark_advances_after_delivery` 只跑**一轮**、只断言水位，不碰去重。
- **后果**：**告警重复投递** —— 同一条告警每 30 秒重发一次；告警是 257 条定义的持续事件流，重复量级远高于指标。
- **实测**：干净树同一条告警连投两轮 → `ALARM BATCHES DELIVERED = 1`；M14 → `= 2`。
- **修法**：补两条守卫（批内同键只投一条 / 已投递过的键下一轮被跳过），对照指标侧同名用例。

#### B3. 三个「保命」的异常分支没有测试（覆盖率数字掩盖了关键分支没测）

- **File**：
  - `tpops_kafka_collector/lifecycle.py:124-129`（`_tick_safely`：一轮采集抛异常 → 进程继续跑，下轮重试）——
    **整个主循环的最后一道防线，零覆盖**；
  - `tpops_kafka_collector/main.py:304-306`（`EXIT_STARTUP_ERROR`：装配失败 → 退出码 3 + 堆栈）—— 零覆盖；
  - `tpops_kafka_collector/main.py:266/268/280`（`_tpops_state` 的认证失败 / 未认证分支、
    `_scheduler_state` 的「最近一次成功采集超过 180 秒」分支）—— 零覆盖。
    **`/readyz` 的 503 分支只测了「还没跑过一轮」，没测「跑了但全挂」。**
- **影响**：这几条都是「出错时不要把错误放大」的路径。现在没测不是「会出错」，而是**一旦改坏没有守卫** ——
  与 wave-2b 的三条 Critical（回归层从没走过采集器）是同一类缺口。
- **修法**：各补 1 条守卫（`lifecycle._tick_safely` 列为必补）。

#### C1. `platform_profile.reconcile` **55 行**，越过合同「函数 < 50 行」硬约束

- **File**：`tpops_kafka_collector/platform_profile.py:243-298`（AST 实测 span = 55 行）
- **问题**：全包 AST 扫描结果 —— **只有 `reconcile` 一个函数越线**；全包无 ≥400 行的文件。
- **佐证**：实施方报告 `task-3.3-report.md:492` 声称「最长函数 **49 行**（`build_runtime`）」，与实测不符
  （`build_runtime` 47 行没错，但漏了同批次新增的 `platform_profile.py`）。
- **修法**：把「逐实例比对」与「新增实例扫描」拆成两个私有函数，主函数只负责汇总与组装。

### Minor（17，去重后）

| # | 内容 | 位置 |
|---|---|---|
| M1 | `load_or_discover` 的 docstring「平台列表接口一次都不调用」与生产 `reconcile=True` 不符（真实含义是「不重建、不改写画像」） | `platform_profile.py:193-195` vs `main.py:216-219` |
| M2 | `test_package_boundary.py` 的「映射数据仍从仓库根读」已过期 | `tests/` |
| M3 | `parse_timestamp` 的「无时区按 UTC」是死分支 | `mapping/rules_sorting.py` |
| M4 | `put_baseline` 与 `import_baselines_from` 键形状相同 → 控制器口径 #2 的理由与实现不符；`scheduler.py:293-298` docstring 与守卫测试名不副实 | `watermark.py:166-173` / `scheduler.py:293-298` |
| M5 | 存量约 23 处 `LOGGER.error` 无堆栈（新增代码已带 `exc_info=True`，存量未统一） | 全包 |
| M6 | P95 是合成执行器推算，非真平台端到端 | `tests/performance/` |
| M7 | 「全部消息命中幂等键」与「完全没数据」在日志上不可区分，`delivery_rate` 报 1.000 | `scheduler_delivery.py:87-95` / `scheduler_report.py:39-40` |
| M8 | 告警幂等键取自平台行的 `instance_id`，与调度器 `instance_id` 口径不一致（平台收敛行缺该字段 → 告警被整条过滤，无 WARNING） | `collectors/alarm.py:196-201` vs `scheduler_types.py:167-182` |
| M9 | `TickResult.mapped` 的文档口径（转换后总数）与实际值（滤掉幂等重复后）不一致 | `scheduler_types.py:121` |
| M10 | 「本轮丢弃 N 个不可提供指标」是**静态计数**（=3），易被理解为每轮丢 3 条 | `scheduler_report.py:59-63` |
| M11~M17 | 风格、注释措辞、冗余清理等（详见三份分报告的 Minor 段） | — |

---

## 六、变异验证记录

沙箱 `/tmp/w3-correct-mut`，每次「改坏 → 跑测试 → 还原 → 重跑全量」。**15/15 完成。**

| # | 变异内容 | 预期 | 实际 | 结果 |
|---|---|---|---|---|
| M1 | 水位推进挪到投递**之前** | 红 | 3 条守卫红（含 kill-restart 演练第一条） | ✅ 守得住 |
| M2 | `stream` 键去掉 `instance_id` | 红 | 红 | ✅ |
| M3 | 差分基线作用域去掉 | 红 | 红，**回归层也红**（M-N4 生效） | ✅ |
| M4 | `import_baselines_from` → `put_baseline` | 红 | **全绿 —— 不可区分** | ❌ **守卫为假**（见 Minor M4） |
| M4b | 落盘时剥掉作用域 | 红 | 红（`test_put_baseline_style_keys_are_not_used` 抓住） | ✅ |
| M5 | 两维并发改组内串行 | 红 | 红 | ✅ |
| M6 | 退出码改非 0 | 红 | 红 | ✅ |
| M7 | `log_format=json` 硬编码 text | 红 | 元测试红 | ✅ |
| **M11** | 告警投递失败**也**推进告警水位 | 红 | **1561 passed 无一红** | ❌ **守卫为假**（Important B1） |
| **M14** | 删掉告警 `seen` / `has_key` 两道去重 | 红 | **1561 passed 无一红** | ❌ **守卫为假**（Important B2） |
| 追加 | 注入 `json.dumps` 破坏唯一序列化出口 | 红 | 红 | ✅ |
| 追加 | A 类多组件同维度值 → 同幂等键 | 红 | 键含 `node_id`/`component_id`，不冲突 | ✅ |
| 追加 | 实例间隔离（共用 node_id） | 红 | 水位/基线/幂等键均含 instance_id | ✅ |
| 追加 | `state <> ''` 方言 | — | 生产用 `state is not null`，未踩坑 | ✅ |
| 追加 | 命令中心结束判据 / `result` 二次 loads | — | `statistics.running == 0` + 二次 `json.loads`，正确 | ✅ |

**每条变异后均已还原并重跑全量（1560 passed）。工作树零改动，沙箱已清理。**

---

## 七、实施方报告中的不准确断言

| # | 报告 | 声称 | 实测 |
|---|---|---|---|
| 1 | `task-3.3-report.md:492` | 最长函数 49 行（`build_runtime`） | `platform_profile.reconcile` **55 行** —— 全包唯一越线函数 |
| 2 | `task-3.1-report.md:22,540` | 最长函数 < 40 行 | 就 3.1 自己那批文件而言基本准确（最长 `_deliver_metrics` 正好 40 行），但**不适用于 3.3 的 `platform_profile.reconcile`**；两个报告的数字都不是全包真相 |
| 3 | `task-3.3-report.md:390` + `platform_profile.py:193-195` | 固化文件存在 → 平台列表接口一次都不调用 | 生产 `main.py:218` 传 `reconcile=True` → 每次启动都会调 `GET /rds/v3.2/instances` |
| 4 | `task-3.5-report.md` | 1287 stmts / 1 miss | 实测 1292 / 1（时序差，非错误） |
| 5 | 账本 T-22 | 引「规格 §4.6.4」 | 实为**开发方案** §4.6.4；`specs/**` 未规定水位粒度 → 本波未违反任何 `specs/**` 条款 |

其余数字（1560 全绿、ruff 0、py37 147 passed、映射层覆盖率、TOTAL 95%）**独立复核全部属实**。

---

## 八、做得好的地方（具体）

- **水位推进 / 幂等键登记的时机做对了** —— 成功后才登记、才推进；失败不推进。
- **`deltas` 是同一个对象**贯穿采集器与调度器 → 基线能落盘（wave-2b 的 Critical 之一已修）。
- **优雅退出绕开了 `future.result()` 无超时的挂死路径**（虽然还有 A2 的 atexit 缺口）。
- **`rules_sorting.py` 的两位时区归一化** —— 注释写出 py3.7 `_strptime` 的正则要求，测试**故意用平台真实格式做第一条**，确保修复前必红。
- **`logging_setup` 用 handler marker 实现 py37 幂等**（py3.8+ 才有 `basicConfig(force=)`）。
- **原子写复用同一套实现**（水位与画像文件）。
- **回归层真的走采集器**，并额外断言「三个 master 各自的速率必须互不相同」。
- **三份分报告对同一批数字的独立复跑结果一致**（1560 passed / 15.017s / 99.92%）—— 门禁数字可信。

---

## 九、修复轮要求（下一轮必须闭环的项）

> 修复轮按**文件归属**分三组并行，组间文件不重叠；实施方**一律不 commit**，由控制器统一提交。
> 修复后需要**定向复审**并签发替换 `pass` receipt。

| 组 | 覆盖 | 负责文件 |
|---|---|---|
| **R-A 运行时** | A1 节奏对齐、A2 退出不挂死、A4 节点清单失败处理、A5 运行期落盘、B3 保命分支守卫 | `lifecycle.py` `main.py` `scheduler.py` `tpops/client.py` + 对应测试 |
| **R-B 可观测性** | A3 真实外部调用计数 + 目录缓存 | `executors.py` `tpops/client.py` `tpops/metrics.py` `scheduler_report.py` + 对应测试 |
| **R-C 守卫与结构** | B1/B2 告警守卫、C1 `reconcile` 拆分、Minor 文档修正 | `platform_profile.py` `scheduler_delivery.py` + 新测试文件 |

**修复轮硬约束**（与 wave-3 相同）：py3.7.9 语法红线、不新增依赖、单文件 < 400 行 / 函数 < 50 行、
不可变数据、凭据只走环境变量、`logging` 不用 `print`、不静默出 0、
**不改** `execution-contract.md` / `tasks.md` / `specs/**` / `data/mapping/*.json` / `data/merged_sql/*`、
每条修复都要配**能抓住它的小改动**的守卫测试（问「有没有一个能让它失败的小改动」）。

---

## 十、签发 receipt

```
ssf execution review changes/tpops-kafka-collector \
  --wave wave-3-scheduling-testing --base 356d52f --head 3a71cb6 \
  --report .superpowers/sdd/reviews/wave-3-scheduling-testing.md --verdict fail
```

> 修复轮 receipt 的 `--base` 必须等于本轮 head `3a71cb6`。
> ⚠️ 签收可能清掉 `plans/<hash>/workspace/`（历史坑）；`plans/` 已被 git 跟踪，
> 若被清则用 `git checkout 3a71cb6 -- "changes/tpops-kafka-collector/.superpowers/sdd/plans/"` 恢复。
