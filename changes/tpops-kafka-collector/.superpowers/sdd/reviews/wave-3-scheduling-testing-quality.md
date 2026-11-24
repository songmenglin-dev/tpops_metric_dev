# Wave 3 审查报告（维度：代码质量与范围）—— wave-3-scheduling-testing

- **波次**：wave-3-scheduling-testing
- **base**：`356d52f`（wave-2b pass receipt 的 head）
- **head**：`3a71cb6`
- **审查范围**：`tpops_kafka_collector/` 本波新增/改动的 11 个生产模块（`scheduler.py` / `scheduler_types.py` / `scheduler_delivery.py` / `scheduler_report.py` / `scheduler_signals.py` / `schedule.py` / `lifecycle.py` / `executors.py` / `main.py` / `logging_setup.py` / `platform_profile.py`）+ 2 处既有生产文件的改动（`collectors/command_center.py`、`mapping/rules_sorting.py`）+ 本波全部测试与工件
- **审查维度**：代码质量与架构 / 错误处理 / 最小化与范围 / 测试质量 / 生产就绪
- **结论**：**verdict = fail**

- **Critical**：0
- **Important**：6
- **Minor**：11

> 判定说明：本波**没有**「报错但数据错」类的致命缺陷（幂等键登记顺序、水位推进时机、原子写、认证不重试这几处最贵的坑都做对了）。但有 **2 条 Important 直接使简报明写的硬性场景在真实代码路径上不成立**，且 **1 条 Important 让规格要求的可观测量变成常数**。这三条都属于「实现看起来做到了、实际没做到」的形态，按本项目的缺陷分类不能放行。

---

## 代码质量与架构

**模块边界总体清楚，无循环依赖。** 依赖方向单向：`main → lifecycle → scheduler → {schedule, scheduler_types, scheduler_delivery, scheduler_report, scheduler_signals}`，`platform_profile / logging_setup / executors` 为叶子。没有反向 import，没有 `scheduler → main` 之类的环。

**实测文件/函数长度（不采信实施方声称，自己数）：**

```
$ find tpops_kafka_collector -name "*.py" -exec wc -l {} + | sort -rn | head
  399  tpops_kafka_collector/tpops/command_center.py
  399  tpops_kafka_collector/kafka/producer.py
  396  tpops_kafka_collector/tpops/metrics.py
  393  tpops_kafka_collector/scheduler.py
  393  tpops_kafka_collector/health.py
  ...
  336  tpops_kafka_collector/platform_profile.py
  311  tpops_kafka_collector/main.py
  281  tpops_kafka_collector/executors.py
```

单文件全部 < 400 行 ✓。

**函数长度（AST 逐个函数数，含空行）** —— 全包**只有 1 个函数 > 50 行**：

| 行数 | 位置 | 函数 |
|---|---|---|
| **55** | `tpops_kafka_collector/platform_profile.py:244-298` | `PlatformProfile.reconcile` |

即实施方自称的「0 个函数 > 50 行」**不成立**，实测 1 个（`reconcile`）。这是合同硬约束「函数 < 50 行」的唯一一处越线（Minor，见 M-1）。

**共享可变状态**（本维度的重点，逐处查过）：

| 位置 | 状态 | 判断 |
|---|---|---|
| `executors.py:167` | `CommandCenterCollector` **单实例**被所有轮次、所有实例共用（这是对的 —— 差分基线要跨轮存活） | 有意为之，注释也写明了 |
| `collectors/command_center.py:264` | `self._page_size = smaller` —— 采集器对象在**线程池工作线程里**被就地改写，而该对象被多实例并发共用 | 见 I-4 |
| `collectors/command_center.py:314-320` | `self._scoped_deltas` 视图缓存字典，跨实例共用；本波已把作用域改成 `f'{instance_id}:{node_id}'`（正确） | 键不重叠 → CPython 下无实质竞态 |
| `mapping/rules_deltas.py:66-91` | `CounterDeltaStore._baselines` 跨实例并发读写 | 键含作用域 → 不同键 → GIL 下安全；但**代码里没有任何地方声明过它是线程安全的** |
| `scheduler.py:135` | `DueTable` 可变状态（`_entries`） | 只在主循环线程里 `tick()`，安全 |
| `logging_setup.py:147-157` | 改**全局 root logger** | 入口模块的职责，可接受 |

**命名与注释：** 绝大多数注释写清了「为什么」而不是「是什么」，这一点质量明显高于常见水平（例：`scheduler_delivery.py:8-11` 解释「幂等键必须在投递成功之后登记，否则下轮被当重复跳过 → 静默丢数据」；`lifecycle.py:8-12` 解释为什么不用 `scheduler.run()`）。但有 2 处注释与实现不一致（M-3、M-7）。

**过度抽象 vs 真重复的判断：** 调度器拆成 5 个模块（`scheduler.py` / `scheduler_types.py` / `scheduler_delivery.py` / `scheduler_report.py` / `scheduler_signals.py`）是被合同「单文件 < 400 行」逼出来的必要拆分，不算过度抽象 —— `scheduler.py` 若不拆会到 ~700 行。但拆分过程留下了**行为中性的死代码**（M-2）。

---

## 错误处理

**逐条查过本波 11 个新模块的全部 `except` 块（共 11 处），结论如下：**

- **静默吞异常：0 处。** 全包 `except:` 裸捕获 0 处；`except ...: pass` 仅 1 处（`mapping/rules_sorting.py:102`，是「这一列能不能转成数字」的探测式 `float(text)` 试探，属有意为之，不是吞异常）。
- **`print` 违规：0 处**（`grep -rn "print(" tpops_kafka_collector/ | wc -l` → 0）。本波新增的 4 个测试文件里 `print(` 也是 0。
- **异常路径带堆栈：大部分到位。** `scheduler.py:244`（采集组失败）、`scheduler_delivery.py:99/135`（投递失败）、`lifecycle.py:129`（一轮调度异常）、`lifecycle.py:169`（收尾动作失败）、`main.py:305`（装配失败）都用 `LOGGER.exception` → 带堆栈 ✓。`logging_setup.py:109-110` 的 `JsonLogFormatter` 也把 `exc_info` 摊平成 `exception` 字段 ✓。
- **捕获后有没有包装成自定义异常并带上下文**：平台层（`errors.py`）做得好（`AuthError` / `ProtocolError` / `TransientError` / `MappingError` / `ProduceError`），且 `reason(exc)` 截断到 200 字防敏感信息外泄（`scheduler_types.py:198-203`）。本波新代码里 `ProfileError(TpopsError)`（`platform_profile.py:61-65`）、`SchedulerError(TpopsError)`（`scheduler_types.py:41-45`）都带了上下文 ✓。
- **认证失败会重试吗：不会。** `tpops/client.py:170-183`「只发一次 —— 认证失败绝不重试」，`errors.py::is_retryable` 按 MRO 基类链把 `AuthError` 判成不可重试，`retry.py:167-168` 判定后原样上抛。这条红线守住了 ✓。

**但有 3 处「捕获后只记 ERROR、没有堆栈」，且都是异常路径**（M-4）：

| 位置 | 现状 | 为什么算问题 |
|---|---|---|
| `main.py:238-243` | `except Exception as exc: LOGGER.error('取节点清单失败（实例 %s）：%s …', instance_id, exc)` | 只 `%s` 打消息，**没有 `exc_info=True`** → 排查时看不到堆栈；简报 §③ 明写「异常路径应带堆栈」。而且这条路径**完全没有测试**（覆盖率报告 `main.py` 未覆盖行含 `238-243`）。 |
| `platform_profile.py:315-320` | 同上形态 | 同上；覆盖率未覆盖 `315-320`。 |
| `main.py:266/268` | `_tpops_state` 读私有标志位的两个分支 | 覆盖率未覆盖 `266/268/280`。 |

**凭据 / mini-token / 业务 SQL 文本有没有漏进日志**：本波新增日志点逐条看过，**没有发现泄漏**。`TpopsClient.__repr__` 明确不出现密码（`tpops/client.py:159-162`）；`health.py` 的响应体有 `secret_values_from_config` 脱敏兜底；`reason(exc)` 截断；`logging_setup` 只负责格式不负责内容。本波新增日志句子里没有插值凭据或 SQL 文本的地方 ✓。

---

## 最小化与范围（逐条引用任务要求 + diff 行号）

**结论：本波没有「合并阻塞级」的越界冗余。** 下面逐条核过：

| 检查项 | 任务要求 | diff 事实 | 判断 |
|---|---|---|---|
| 新增依赖 | 简报「不新增依赖」 | `git diff 356d52f..3a71cb6 -- requirements.txt setup.py pyproject.toml` → **0 行** | ✓ 无越界 |
| 新增配置项 | — | 本波**没有新增任何环境变量**：`HEALTH_HOST/PORT`、`LOG_LEVEL/LOG_FORMAT`、`STATE_DIR` 都是既有（`config.py` 在 base 里已有 `log_level`/`log_format`，本波只是第一次**消费**它们） | ✓ 无越界 |
| 改既有生产文件 | 简报「不改 `tpops_kafka_collector/` 下已有文件，除非任务简报明确说了要改」 | 只改了 2 处：`collectors/command_center.py:180/306-320`（差分作用域加 instance_id）、`mapping/rules_sorting.py`（两位时区归一化）。两处都在账本里有控制器裁决（3.3 的 D2、控制器 3.5b 项 ②） | ✓ 有据 |
| 模块拆分（11 个新模块） | 合同「单文件 < 400 行」 | `scheduler.py` 不拆会到 ~700 行 | ✓ 必要拆分，非过度抽象 |
| `executors.py` | 3.3 简报装配顺序列的是「五个采集器」 | 执行器适配层是必需的（采集器 API 是「按实例」，调度器契约是 `(InstanceTarget) -> TaskOutcome`） | ✓ 必要 |
| `logging_setup.py` / `platform_profile.py` | 3.3 简报明写要新建 | ✓ | ✓ |
| `schedule.py` | 3.1 简报「周期必须表驱动，不写死」 | ✓ | ✓ |

**唯一的多余是行为中性的死代码**（不阻塞，见 M-2）：`scheduler_types.py:206-210` 的 `collect_failures`、`schedule.py:174-178` 的 `optional_plan`（连带 `plan_by_channel` 只被它自己调用）、`scheduler.py:190-202` 的 `Scheduler.run_forever` 与 `178-188` 的 `Scheduler.run`（生产路径走 `lifecycle.run`，这两个方法**零调用方、零覆盖**）。其中 `Scheduler.run`/`run_forever` 还有副作用：它们是**第二份主循环实现**，与 `lifecycle.run_loop` 的语义不同（见 I-1），留着就等于给未来埋一个「用错的那个」。

**另外**：本波把 `changes/.../workspace/review-wave3-r3.diff`（9888 行）提交进了仓库。这是流程工件不是产物，仓库噪声（M-8）。

---

## 测试质量

**先说好的**：本波在测试质量上有实质进步，尤其是回归层。

- `tests/regression/test_collector_coverage.py` **真的走采集器**了 —— 用实测留档回放驱动 `MonitorCollector` / `CommandCenterCollector` / `DiagnosticCollector` / `InstanceCollector`，逐条断言「98 个可提供指标全部产出」且**值等于期望值表**（不是「非零」判据）。这正是 wave-2b 的教训（回归层只测映射层，从没走过采集器）。✓
- `test_every_master_computes_its_own_rate_in_the_regression_layer`（`tests/regression/test_collector_coverage.py:148-199`）在断言三个 master 的速率之外，**额外断言「三个速率必须互不相同」**（:197-199）—— 这一句让断言在「基线被共享」时必然变红，不是恒绿断言。✓ 这是本波测试设计最好的一处。
- `tests/unit/test_rules_sorting_offsets.py:60-95` 构造了「字典序与时间序结论相反」的对，且**故意让第一条用平台的两位时区格式**（:63-66 的注释解释了原因：否则修复前整列判成 text，测试根本走不到按时间比那条路 → 恒绿）。这是**变异敏感**的守卫，不是自证式。✓
- `tests/integration/test_main.py:264-287`（`test_no_new_http_request_after_shutdown`）断言的是 `client.call_count == 0`（**外部可观察的行为**），而不是「mock 被调用了几次」—— 断言的是真实语义。✓
- `tests/integration/test_kill_restart_drill.py` 第二段**真起进程**（`python -m tpops_kafka_collector.main` + `sitecustomize` 传输层桩）、真发 SIGTERM、真断言退出码 0 与水位文件可读回。✓
- `tests/unit/test_logging_setup.py` 是「元测试」：每条都**真的看到输出**（`json.loads` 逐行得通），而不是断言 mock 调用。✓

**问题**：

1. **「在途请求超过 30 秒 → 放弃等待 → 退出码 0」的守卫测试是自证式的**（I-2 的另一半）：`tests/integration/test_main.py:294-333` 用的是 `ScriptedScheduler`（:85-99，**一个没有线程池的假调度器**）。真实路径上放弃等待时，留下的是一个**卡在 `ThreadPoolExecutor` 里的工作线程**，而 py3.7 的 `concurrent.futures.thread` 注册了 `atexit.register(_python_exit)`，退出时会 `t.join()` 所有工作线程 → 进程**退不出来**。这条测试因为绕开了线程池而恒绿，**恰好掩盖了真实缺陷**。
2. **告警投递失败路径无守卫**（I-5）：`scheduler_delivery.py:133-139` 的告警投递失败分支**零覆盖**（覆盖率报告未覆盖行含 `134-139`），而 `tests/unit/test_scheduler.py:89-93` 的 `FakeProducer` **已经支持 `fail_alarm_batches`** 却**没有任何用例传过它**（`grep -rn "fail_alarm_batches" tests/` 只命中定义处）。作者写了工具没用 —— 说明这条守卫是漏掉而不是有意省略。
3. **`lifecycle._tick_safely` 的异常分支无守卫**（覆盖率未覆盖 `lifecycle.py:128-129`）：这是「一轮炸了不该带走进程」的最后一道防线（整个主循环就靠它），**恰恰没有测试**。
4. **`main.py:304-306`（`EXIT_STARTUP_ERROR` 路径）无守卫**（覆盖率未覆盖）：装配失败 → 退出码 3 这条路径没测。
5. **`main.py:238-243`（节点清单取不到 → 降级继续）无守卫**（覆盖率未覆盖）：这条路径的后果是 `resourceName` 永久退化（见 I-6），却没有一条测试。
6. **「P95 ≤ 25 秒」这条合同完成标准的证据是自证的**（M-6）：`tests/performance/measure_round_latency.py` 用 `time.sleep(15)` 模拟通道延迟，量的是「线程池并发跑 sleep 的墙钟」；账本里记的「**实测** P95 = 15.017s」就是这么来的。它证明的是「`ThreadPoolExecutor` 会并发」，不是「采集端一轮真的 ≤ 25 秒」。`tests/integration/test_round_latency_budget.py:162-190` 那条更是**纯纸面算术**（断言 15 ≤ 25、21 > 20、30 ≤ 30 —— 全是对自己定义的常量做断言，与生产代码零交互），docstring 里也自认「这是纸面推算」。**结论**：这条完成标准至今没有任何一条走过真实采集器路径的守卫。
7. **测试文件超长清单**（本波新增/改动的）：

| 文件 | 行数 | 备注 |
|---|---|---|
| `tests/unit/test_scheduler.py` | **852** | 账本已登记为 T-28（「仓库里最长的测试文件，此前最长 704 行」）—— 我复核确认 852 行属实 |
| `tests/integration/test_collector_command_center.py` | 704 | 既有，本波未动 |
| `tests/unit/test_mapping_columns_gaps.py` | 480 | 本波新增（3.5a 覆盖率补测） |
| `tests/integration/test_main.py` | 515 | 本波新增 |
| `tests/unit/test_platform_profile.py` | 340 | 本波新增 |
| `tests/unit/test_mapping_rules_gaps.py` | 335 | 本波新增 |
| `tests/unit/test_mapping_decode_gaps.py` | 264 | 本波新增 |
| `tests/unit/test_mapping_message_sqlgen_gaps.py` | 277 | 本波新增 |

「大量小文件」的规则只对生产代码明写（合同「单文件 < 400 行」指 `tpops_kafka_collector/`），测试文件超长目前只在 T-28 里登记了一条，我不额外开条目。

---

## 生产就绪

### 优雅退出

| 规格要求 | 实现 | 判断 |
|---|---|---|
| SIGTERM/SIGINT → 置位 shutdown | `scheduler_signals.py:18-30`，装不上只记 WARNING 照常跑 | ✓ |
| 停发新请求 | `executors.py:101-116` 的 `_guard` 把检查放在**执行器入口** —— 调度器层保证不了这件事，理由写在 `lifecycle.py:13-16` | ✓ **这是本波做得最好的一处设计** |
| 等在途请求，超时 30 秒放弃，不挂死 | `lifecycle.py:100-153`：每轮放**守护线程** + 截止时间 | ✗ **见 I-2**（守护线程保证不了进程能退出） |
| 原子写水位 | `watermark.py:316-341`（临时文件 → `fsync` → `os.replace` → 目录 `fsync`） | ✓ |
| flush producer | `lifecycle.py:159` → `producer.close()`；`kafka/producer.py:360-370` | ✓（`_produce` 已按 `send_timeout` 等 ack，所以 close 时基本无残留） |
| 退出码 0 | `lifecycle.py:189` | ✓（**但见 I-2 的超时场景**） |

### 启动期配置缺失

`main.py:294-299`：`load_config` 抛 `ConfigError` → 记 ERROR（含变量名）→ 返回 `EXIT_CONFIG_ERROR=2`，**在任何 `build_runtime` 之前** → 不向 TPOPS / Kafka 发任何请求。`tests/integration/test_main.py:174-218` 用 `_NeverClient` / `_NeverProducer`（其 `get`/`post` 直接 `raise AssertionError`）断言「一次都没被构造」—— **真行为断言** ✓。

### 健康端点

`/healthz` 恒 200（只看进程能否应答）、`/readyz` 依赖不可用返回 503；探针读标志位、**不发网络请求**（`test_health_probes_are_wired_and_lightweight` 断言 `client.call_count` 不变）；响应体有 `secret_values_from_config` 脱敏兜底 ✓。

### 日志中心化

- 消费 `config.log_level` / `config.log_format` ✓（`main.py:294` + `:300`）
- 输出 stderr ✓（`logging_setup.py:155`）
- 幂等 ✓（`logging_setup.py:121-126` 用自认领的 handler marker 判重，py37 没有 `basicConfig(force=)`）——`test_setup_logging_is_idempotent` / `test_setup_logging_keeps_single_owned_handler` 守住了
- 第三方降噪 ✓（`kafka` / `kafka.conn` / `kafka.cluster` / `urllib3` / `requests` → WARNING）
- JSON 格式下把每轮汇总的 `k=v` 摊平成顶层字段 ✓（客户日志平台能直接按字段取）

### 原子写

水位文件与画像文件都走 `watermark.py::_atomic_write_json`（fsync + `os.replace` + 目录 fsync）✓。`platform_profile.py:195-213` 有两条守卫测试（「中途失败不留半截」「旧内容原样保留」）✓。

### 文档

`tpops_kafka_collector/mapping/README.md` **与实现不一致**（M-7）：第 60-62 行仍写「映射数据（`data/mapping/*.json`）目前仍在仓库根，**还没搬进包内** —— 那是 wave 2a 任务 2.1 的事。包内只有执行代码。」但 `tpops_kafka_collector/mapping/data/` 已在 wave-2a（`c816358`）落地（10 份数据 + `observed_columns.json`）。另外第 39 行对 `rules_sorting.py` 的描述没有提本波的两位时区归一化。

---

## 实测记录（命令 + 实际输出）

```console
$ conda run -n tpops python -m pytest -q
1560 passed in 51.64s

$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest -q tests/test_py37_compat.py
147 passed in 1.23s

$ grep -rn "print(" tpops_kafka_collector/ | wc -l
0

$ grep -rn "except.*:\s*$" -A1 tpops_kafka_collector/ | grep -c "pass"
1        # 仅 mapping/rules_sorting.py:102（float() 探测式试探，非吞异常）

$ find tpops_kafka_collector -name "*.py" -exec wc -l {} + | sort -rn | head -10
  399 tpops/command_center.py   399 kafka/producer.py   396 tpops/metrics.py
  393 scheduler.py   393 health.py   382 tpops/client.py
  382 collectors/monitor.py   375 collectors/alarm.py   359 config.py   357 watermark.py

# 函数长度（AST 逐函数数，py37 无 end_lineno，用子节点最大行号近似）
# 全包 > 50 行的函数只有 1 个：
  55  platform_profile.py:244  PlatformProfile.reconcile

$ conda run -n tpops python -m pytest --cov=tpops_kafka_collector --cov-report=term-missing -q | tail -3
TOTAL   4670  228  95%
1560 passed in 57.76s

# 本波新模块的未覆盖行（缺口清单）
executors.py            99%  205
lifecycle.py            95%  128-129, 168-169        ← 一轮异常的兜底未测
logging_setup.py        96%  66, 70, 81
main.py                 92%  204, 238-243, 266, 268, 280, 304-306
platform_profile.py     91%  92, 95, 117, 148, 151, 156, 171-172, 312, 315-320, 328
schedule.py             91%  123, 151-155, 158-162, 171, 178
scheduler.py            87%  142, 147, 152, 184-188, 196-202, 255, 380, 389-393
scheduler_delivery.py   94%  124, 127, 134-139      ← 告警投递失败路径未测
scheduler_report.py     96%  84
scheduler_types.py      97%  202, 210

# 节奏实测（用真的 DueTable 模拟 lifecycle.run_loop 的时间线）
$ conda run -n tpops python /tmp/w3q/cadence2.py
monitor          档位= 30s  实际间隔 min=32 max=45 avg=38.7  → 超服务 29%
command_center   档位= 60s  实际间隔 min=77 max=77 avg=77.0  → 超服务 28%
alarm            档位= 30s  实际间隔 min=32 max=45 avg=38.7  → 超服务 29%

# atexit 阻塞实测（py3.7 ThreadPoolExecutor）
$ conda run -n tpops python /tmp/w3q/atexit_join.py
主线程放弃等待，准备退出
进程实际耗时 11 秒（脚本本身只 sleep 0.2s）
# → 证实：py3.7 的 atexit._python_exit 会 join 线程池工作线程
```

---

## 发现

### Critical

无。

### Important

#### I-1. 采集节奏 = 采集耗时 + 轮询间隔，实际比客户声明的档位长约 29%（82/98 的指标受影响）

- **File**：`tpops_kafka_collector/lifecycle.py:93-97`（`run_loop`）
- **问题**：`run_loop` 在**一轮跑完之后**才 `shutdown.wait(runtime.config.poll_interval_seconds)`，于是实际周期 = `本轮采集耗时 + 轮询间隔`。默认 `POLL_INTERVAL_SECONDS=30`、命令中心一轮 ~15s → 有效周期 45s。叠加 `DueTable.split_due` 的「到期才跑」语义（`schedule.py:217-235`），**每个通道的实际采集间隔都被拉长到 ~1.3 倍**。
- **实测**（用生产 `DueTable` 模拟时间线，见上）：30 秒档的 `monitor` / `alarm` 实际平均 **38.7 秒**（最坏 45 秒）；60 秒档的 `command_center` 实际 **77 秒**。客户指标库里 **82/98 条指标走 command_center 通道**，全部受影响。
- **偏离**：3.1 简报第 33 行明写「4. **sleep 到下一个 tick 边界**」—— 现在是「sleep 一整个间隔」，不是「对齐到边界」。
- **影响**：这正是本项目最贵的那类缺陷形态 —— **不报错、客户侧的 30 秒/60 秒刷新档位被静默欠服务 ~29%**。日志里 `tick=` 递增正常、外部调用数正常，没有任何信号提示节奏偏了。合同完成标准「按 SQL 覆盖指标的最高档定周期」在运行期不成立。
- **修法**：`run_loop` 里改成「对齐到 tick 边界」—— 记录本轮起点 `t0`，本轮结束后睡 `max(0, poll_interval - (now - t0))`；或者把 `DueTable` 的到期判断改成「上次到期时刻 + 间隔」（允许迟到但不多睡）。前者改动最小，且能同时把「命令中心那 15s 吃掉 tick 周期」的余量问题暴露出来。另：`scheduler.py:387-393` 的 `_wait_for_next_tick` 是同样的形状（虽然当前无调用方）。
- **建议同时补一条守卫**：断言「某通道的相邻两次实际采集间隔 ≤ 档位 × 1.1」——现在这条断言会红。

#### I-2. 「在途请求超过 30 秒 → 放弃等待 → 退出码 0，不挂死」在真实代码路径上不成立（py3.7 的 `atexit` 会 join 线程池工作线程）

- **File**：`tpops_kafka_collector/lifecycle.py:100-121`（`_run_one_tick`）+ `:110-113`（守护线程）+ `tpops_kafka_collector/scheduler.py:232`（`with ThreadPoolExecutor(...)`）
- **问题**：`lifecycle` 的设计意图（docstring `lifecycle.py:8-12`）是「本轮放在**守护线程**里跑 —— 放弃等待后进程仍能退出（守护线程不拦解释器退出）」。这个判断在 **py3.7 上不成立**：
  - `scheduler._collect_all` 用 `with ThreadPoolExecutor(...)` → 退出上下文管理器时 `shutdown(wait=True)` → tick 线程卡在等一个挂死的 HTTP；
  - py3.7 的 `concurrent/futures/thread.py` 注册了 **`atexit.register(_python_exit)`**，`_python_exit` 会给每个工作队列塞 `None` 并 **`t.join()`** 所有工作线程（工作线程本身虽是 daemon，但 atexit 钩子显式 join）。
  - 于是：主线程放弃等待 → 写水位 → 关 producer → `return 0` → **解释器退出阶段被 atexit 钩子挡住**，直到那个挂死的 HTTP 返回为止。
- **实测**（见上「atexit 阻塞实测」）：脚本本身只 `sleep(0.2)`，进程实际耗时 **11 秒**（被 join 的工作线程睡了 8 秒）。
- **生产影响**：默认 `TPOPS_REQUEST_TIMEOUT=60`。SIGTERM 后若有一个在途请求挂住，进程**最多再拖 60 秒**才退 —— 超过简报要求的 30 秒放弃窗口，也超过 systemd 默认的 `TimeoutStopSec=90/30`。若被 supervisor SIGKILL，**退出码就不是 0**，简报明写的「退出码 0」这条完成标准随之失守（水位文件在 SIGKILL 前已写完，所以不丢数据，但退出码与「优雅」二字都不成立）。
- **配套的测试缺陷**：`tests/integration/test_main.py:294-333` 的守卫用 `ScriptedScheduler`（:85-99，**没有线程池的假调度器**），因此**绕开了这条路径、恒绿**。
- **修法**（任选，建议 1+3）：1) 放弃等待时不要用 `with ThreadPoolExecutor(...)`（改成 `pool = ThreadPoolExecutor(...)` + `pool.shutdown(wait=False)`），让工作线程不被 atexit join —— 但 py3.7 的 `_python_exit` 仍会 join 它，所以更彻底的是 3)；2) 给 `TpopsClient` 的超时设上限（< 30s）；3) **推荐**：退出前显式 `os._exit(0)`（在完成水位落盘 + producer flush 之后），跳过 atexit 钩子 —— 需要先把 kafka 的 flush 做完，再 `_exit`。
- **守卫测试要改**：把 `test_in_flight_request_beyond_deadline_gives_up_and_exits_zero` 的假调度器换成**真的 `Scheduler` + 真的 `ThreadPoolExecutor`**（让某个执行器 sleep 住），否则这条守卫测的是假实现。

#### I-3. 「单轮外部调用数」是编造的常数，42 次预算告警形同虚设

- **File**：`tpops_kafka_collector/executors.py:55-59`（常量）、`:143` / `:184` / `:217` / `:244` / `:277`（各通道上报值）；`tpops_kafka_collector/scheduler_report.py:27`、`:64-68`（预算与告警）
- **规格**：`changes/tpops-kafka-collector/specs/delivery-and-state/spec.md:235-240`「采集端 SHALL 在每轮汇总日志中记录本轮的**外部调用次数**…单轮的外部调用数 SHALL NOT 超过 42 次…超出时 SHALL 记录 WARNING」。
- **问题**：5 个通道里 **4 个的 `external_calls` 是写死的常量**（monitor=2、diagnostic=1、instance=2、alarm=4），第 5 个（command_center）报的是**页数**而不是 HTTP 往返次数：
  - **A 类实际是 13 次**：`_check_catalog` → `fetch_metric_catalog` 对 **12 个分组码各发一次 GET**（`tpops/metrics.py:363-367`，**每轮都重新拉，无缓存**），再加 1 次指标 POST（`collectors/monitor.py:103-106`）→ 13 次，却报 **2**。
  - **B 类实际是 3 × 页数**：`execute_sql` 一次就是命令中心的**完整三跳**（下发 → 轮询 → 取明细，3+ 次 HTTP），`pages` 只是「翻了几页」，`external_calls=max(result.pages, 1)`（`executors.py:184`）**少算 3 倍**。
  - **告警实际是 4 × 页数**：四种形态各自分页（`limit ≤ 100`），常量 4 只覆盖了「每形态一页」。
  - 合计：单实例一轮的真实调用数已经 **> 42**（13 + 3×11 + 2 + 1 + 4 + 1 ≈ 54+），而上报值只有 `2+11+1+2+4 = 20` —— **永远不会触发 42 次预算告警**；多实例时真实值翻倍，上报值也跟着翻倍，比例不变 → 告警**永远不响**。
- **影响**：这是规格点名要求的**可观测量**，现在是一个自造的数 → 「不报错、但运维看不见」。真出现「重复采集 / 分页爆炸 / 分组码乱写」时，没有任何信号。
- **修法**：在 `TpopsClient` 上挂一个**真实计数**（每次 `get`/`post` 递增，线程安全用 `itertools.count` 或加锁），让采集器/执行器报**真实 HTTP 次数**而不是估算常数；或退一步，至少把 A 类的 12 次目录查询改成**按轮缓存**，让上报值与真实值数量级对齐。**注意**：这条改了之后，`EXTERNAL_CALL_BUDGET = 42` 这个数本身也要跟着重算（规格里那个 42 是按「命令中心三跳 3」估的，与真实的三跳×页数不是一个口径）。

#### I-4. 节点清单只在启动时取一次；取失败就**永久降级** `resourceName`，且这条路径无测试、无堆栈

- **File**：`tpops_kafka_collector/main.py:222-243`（`_build_instances` / `_fetch_nodes`）
- **问题**：`_fetch_nodes` 捕获**所有**异常 → 只记一条不带堆栈的 ERROR → 返回空元组 → `InstanceTarget.nodes = ()`。而 `nodes` 是**启动时取一次、之后永不再取**：`collectors/monitor.py:83/234/245` 用它把 series 的 `component_id` 换算成节点身份，**`resourceName` 直接由它拼出来**。
- **影响**：
  - 启动瞬间的一次网络抖动 / 认证未就绪 → 整个进程生命周期的 `resourceName` **永久退化成只有 node_id** → 客户按台账查不到资源 → **数据看着有、但对不上台账**（典型的「不报错、但数据错」）。
  - 错误只记一条 ERROR，**没有堆栈**（`main.py:238-243`），且这条路径**零测试**（覆盖率未覆盖 `238-243`）。
- **修法**：① 节点清单取失败时**不要**建实例（让那一轮失败、下轮重试），或改成**按需/每轮重取**（节点清单接口很轻）；② 至少补上 `exc_info=True`；③ 补一条守卫：节点取不到 → 该实例**不进本轮采集**（而不是带着空节点继续跑）。

#### I-5. 告警投递失败路径零覆盖 —— 支撑工具都写好了，就是没有用例

- **File**：`tpops_kafka_collector/scheduler_delivery.py:133-139`（告警投递失败分支）
- **问题**：`tests/unit/test_scheduler.py:89-93` 的 `FakeProducer` **已经支持 `fail_alarm_batches`**（:93 `self._fail_alarm = set(fail_alarm_batches)`），但 `grep -rn "fail_alarm_batches" tests/` **只命中定义处，没有任何用例传过它** → 覆盖率报告该分支未覆盖（`134-139`）。
- **影响**：规格「投递失败不推进水位」「批次原子推进」在**告警通道**上没有任何守卫。指标通道有守卫（`test_watermark_advances_only_after_successful_delivery`），告警通道没有 —— 而告警恰恰是本波 b0aad0b 刚修过的地方（不再刷假 WARNING），也是客户最关心的通道。**告警水位一旦被错误推进，历史告警就永久丢失**（平台侧历史告警只留 30 天）。
- **修法**：照 `test_watermark_advances_only_after_successful_delivery` 复制一条告警版：投递失败 → `store.alarm_watermark(stream) is None` 且下轮重发。

#### I-6. 三个「保命」的异常分支没有测试（覆盖率实测）

- **File / 行**：
  - `tpops_kafka_collector/lifecycle.py:124-129`（`_tick_safely`：一轮采集抛异常 → 进程继续跑，下轮重试）—— **整个主循环的最后一道防线，零覆盖**。
  - `tpops_kafka_collector/main.py:304-306`（`EXIT_STARTUP_ERROR`：装配失败 → 退出码 3 + 堆栈）—— 零覆盖。
  - `tpops_kafka_collector/main.py:266/268/280`（`_tpops_state` 的「认证失败 / 未认证」分支、`_scheduler_state` 的「最近一次成功采集超过 180 秒」分支）—— 零覆盖。**`/readyz` 的 503 分支只测了「还没跑过一轮」，没测「跑了但全挂」**。
- **影响**：这几条都是「出错时不要把错误放大」的路径。它们没测不是「现在会出错」，而是**一旦改坏了没有守卫** —— 与 wave-2b 的三条 Critical（回归层从没走过采集器）是同一类缺口。本波覆盖率 95% 这个数字**掩盖了关键分支没测**这件事（整段未覆盖行只有 13~21 行，容易被百分比掩盖）。
- **修法**：各补 1 条守卫（异常路径各一条，共 3~4 条），并把 `lifecycle._tick_safely` 的那条列为**必补**。

### Minor

- **M-1. 唯一一处函数 > 50 行**：`platform_profile.py:244-298` `PlatformProfile.reconcile` **55 行**，越了合同「函数 < 50 行」。拆成「比对实例存在性」「比对 engine/master_count」「比对新增实例」三段即可。
- **M-2. 死代码（行为中性，不阻塞）**：`scheduler_types.py:206-210` `collect_failures`（零调用方，且名字与 `scheduler.py:167` 的局部变量撞名，容易误读）；`schedule.py:174-178` `optional_plan` 及其唯一调用者 `plan_by_channel:167-171`；`scheduler.py:190-202` `run_forever` 与 `178-188` `run`（**第二份主循环实现**，与 `lifecycle.run_loop` 语义不同，见 I-1 —— 留着它，未来有人用 `scheduler.run()` 就会拿到偏慢的节奏）。建议删 `run`/`run_forever`（或明确标注 deprecated 并加测试）。
- **M-3. 注释与实现不一致（平台画像）**：`platform_profile.py:193-194` 的 docstring 写「**固化文件存在 → 不重新发现**（平台列表接口一次都不调用）」，但 `main.py:216-219` 的生产装配传的是 `reconcile=True` → **每次启动都会调用平台列表接口**。账本 D1 已裁定「拆成 `load_or_discover`（默认不查平台）与 `reconcile`（只记日志）两个入口，生产用 `reconcile=True`」—— 行为是对的，**是 docstring 没跟着改**。
- **M-4. 三处「捕获异常后只记 ERROR、不带堆栈」**：`main.py:238-243`、`platform_profile.py:315-320`、`main.py:297-299`。简报 §③ 明写「异常路径应带堆栈（`LOGGER.exception(...)` 或 `exc_info=True`）」。建议统一 `LOGGER.exception`。
- **M-5. 「放弃等待」路径上的水位落盘竞态**：`watermark.py:204-217` 的 `snapshot()` 用 `sorted(self._metric_watermarks.items())`；若被放弃的那轮 tick 线程此时正在 `advance_metric`（`watermark.py:150-154`，**会插入新 key**），`sorted()` 会抛 `RuntimeError: dictionary changed size during iteration` → 被 `lifecycle.py:158` 的 `_safe` 兜住 → **水位文件这一轮没写成**。后果不严重（重启后重投，幂等键兜住，不丢数据），但「退出前一定要写出完整水位」这条就打折了。修法：`snapshot()` 里对字典取 `list(...)` 副本再排序。
- **M-6. 「P95 ≤ 25 秒」的证据是自证的**：见「测试质量」第 6 条。建议把账本里的措辞从「**实测** P95 = 15.017s」改成「并发结构实测（sleep 模拟）」，并另找机会对真实平台跑一轮真数据来补这个数字。
- **M-7. 包内 README 与实现不一致**：`tpops_kafka_collector/mapping/README.md:60-62` 仍写「映射数据还没搬进包内 —— 那是 wave 2a 任务 2.1 的事」，但 `tpops_kafka_collector/mapping/data/` 已在 wave-2a（`c816358`）落地；第 39 行对 `rules_sorting.py` 的描述也没提本波的两位时区归一化。
- **M-8. 仓库里提交了 9888 行的 diff 工件**：`changes/tpops-kafka-collector/.superpowers/sdd/plans/.../workspace/review-wave3-r3.diff`。是流程产物（账本里有同类先例），但它让 `git diff --stat` 完全失真，后续审查者要自己过滤。
- **M-9. `collectors/command_center.py:264` 的 `self._page_size = smaller`**：命令中心采集器对象被多实例并发共用，而这个字段在**工作线程里**被就地改写（输出超限时缩小页容量）。当前无实质竞态（只是个 int 赋值），但它是**跨实例共享的可变状态**，且 `CommandCenterCollector` 的 docstring 没声明线程安全性。建议在类 docstring 里写明「本对象被多线程共享，字段写入须谨慎」，或把 `page_size` 改为按实例持有。
- **M-10. `logging_setup.py:149-153` 的「输出到 stderr」可能被静默改道**：只有当调用方**显式传了** `stream` 时才会换 handler；先 `setup_logging(stream=X)` 再 `setup_logging()`（不传 stream）会**继续往 X 写**，而不是回到 stderr。生产路径（`main.py:294` → `:300`）恰好没踩到，但这个 API 语义有点反直觉。建议改成「不传 stream 时若已有 handler 则重置为 stderr」。
- **M-11. `logging_setup.py:108` 的字段摊平可能覆盖保留键**：`payload.update(_flatten_pairs(message))` 会让一条形如 `message=x level=y` 的日志把 `message`/`level` 顶层字段覆盖掉。当前汇总日志的字段名不冲突，但建议排除 `timestamp`/`level`/`logger`/`message` 四个保留键。

---

## 做得好的地方（具体到文件）

1. **`scheduler_delivery.py:8-11, 107-108, 143-144` —— 幂等键「投递成功之后才登记」**。这是本项目最贵的坑（「先登记后投递 → 投递失败的那批键已被记成已投递 → 下轮被当重复跳过 → **静默丢数据**」），实现不但做对了，还在注释里把失败形态写清楚了。`tests/integration/test_kill_restart_drill.py:118-141` 用「重启后不再投一次」把它钉住了。
2. **`executors.py:101-116` + `lifecycle.py:13-16` —— 停机检查放在执行器层**。调度器层只能保证「不再发起新任务」，保证不了「不再发起新的 HTTP 请求」（已排进线程池但还没开始跑的任务仍会启动）—— 这个区分很准，`tests/integration/test_main.py:264-287` 用 `client.call_count == 0` 从外部可观察地守住了它。
3. **`scheduler.py:216-249` —— 单组失败隔离**：每组任务独立捕获、记 `TaskFailure`、用 `LOGGER.exception` 带堆栈，其余组照常投递；`tests/unit/test_scheduler.py:443-470` 断言的是「失败组没把别的组的投递拖掉」，不是「调用了几次」。
4. **`tests/unit/test_scheduler.py:363-413` 的并发守卫用 `threading.Barrier`**：串行执行时后一个任务永远到不了屏障 → 抛 `BrokenBarrierError` → 测试**必然失败**。这是确定性判据，不是靠 sleep 猜时间。
5. **`mapping/rules_sorting.py:31-45` 的注释把 py3.7 `_strptime` 的 `%z` 正则原样写出来**，并说明「不归一化的话平台真实数据一条都解析不了，排序退化成字典序 → **Top N 取错行，且不报任何错**」；配套测试（`tests/unit/test_rules_sorting_offsets.py:60-95`）**故意用平台的两位时区格式做第一条**，确保修复前必红。**这是本波质量最高的一处修复**。
6. **`tests/regression/test_collector_coverage.py:148-199` 的跨 master 速率断言**：除逐节点比对外，额外断言「三个速率必须互不相同」—— 这一句让断言在「基线被共享」时必然变红（账本里 wave-2b 的 C-3 就是这种形态）。
7. **`platform_profile.py:305-329` 的 `master_count` 按唯一 `node_id` 计数**：docstring 里写清了实测依据（分布式实例 `role=master` 有 6 条但只有 3 个唯一 node_id，数条目虚高一倍），并有 `tests/unit/test_platform_profile.py:143-151` 钉住。
8. **`logging_setup.py` 的幂等实现**：py3.7 没有 `basicConfig(force=)`，用「自己装上去的 handler 打标记」来认领（`:121-126`、`:154-157`），并且**换输出流时是替换而不是叠加**。`tests/unit/test_logging_setup.py:89-107` 两条守卫把「不叠加」「永远只有一个」分别钉住。
9. **`main.py:294-299` 的启动期校验位置**：在 `build_runtime` **之前**返回，天然满足「配置缺失 → 未向任何外部服务发请求」；测试用「构造即抛 AssertionError」的 `_NeverClient`/`_NeverProducer` 从外部可观察地断言了这一点。
10. **`platform_profile.py:176-178` 复用 `watermark.py` 的原子写**而不是另写一份 —— 避免了「两套原子写实现」的分叉风险（正是 CLAUDE.md 里点名的「双副本分叉」风险）。
11. **工程门禁全绿**：`1560 passed` / `ruff check` 0 / `print` 0 / `py37` 兼容 147 passed / 覆盖率 95%（映射层 99.9%）/ 未新增依赖。

---

## 收尾

**必修（不修不建议签 pass）**：I-1（采集节奏）、I-2（退出挂死 + 守卫测试换成真线程池）。
**强烈建议同批修**：I-3（外部调用计数）、I-4（节点清单降级）、I-5（告警投递失败守卫）、I-6（三条保命分支守卫）。
**可在收尾后的独立小变更里做**：全部 Minor（M-1~M-11），其中 M-2 建议与账本里已登记的 T-28（`test_scheduler.py` 852 行）合并处理。

签发 receipt 的命令：

```
ssf execution review changes/tpops-kafka-collector \
  --wave wave-3-scheduling-testing --base 356d52f --head 3a71cb6 \
  --report .superpowers/sdd/reviews/wave-3-scheduling-testing-quality.md --verdict fail
```
