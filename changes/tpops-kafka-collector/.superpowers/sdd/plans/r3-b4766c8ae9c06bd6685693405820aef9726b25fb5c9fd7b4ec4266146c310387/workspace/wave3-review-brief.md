# Wave 3 独立审查简报（wave-3-scheduling-testing）

> 这是**审查者**的工作说明。审查者**独立取证**，不采信实施方报告里的任何数字。
> 审查报告写到 `.superpowers/sdd/reviews/wave-3-scheduling-testing.md`（**非空**、写成常规文件）。

## 审查范围

- **base** = wave-3 起始 SHA（wave-2b 的 pass receipt 的 head：`356d52f`）
- **head** = wave-3 结束 SHA
- 任务：**3.1 调度器 / 3.2 健康端点（已入账） / 3.3 入口装配 / 3.4 回归夹具（已入账）/ 3.5 全量门禁**
- 简报：`workspace/task-3.{1,3,5}-brief.md`、`workspace/controller-note-3.3.md`、`workspace/wave3-execution-brief.md`
- 规格：`changes/tpops-kafka-collector/specs/{delivery-and-state,runtime-and-config,mapping-and-rules}/spec.md`
- 合同：`changes/tpops-kafka-collector/execution-contract.md` 的 Wave 3 段

## ⚠️ 上一轮的教训（wave-2b）

- wave-2b 首轮审查出 **3 条 Critical + 4 条 Important**，全部是「**不报错、但数据没有/数据错**」的形态。
- 它们能溜过去，是因为**回归层只测映射层、从来没走过采集器**。
- **实施方的自查数字可能不准**（wave-2b 实施方声称「全包 0 个函数 >50 行」，实测有 51 行的）。
  → **控制器/审查者要自己核**，不要照抄报告里的数字。
- **变异测试是唯一能证明「测试真的有守卫力」的手段** → 审查时要自己挑几条关键守卫做变异验证。

## 一、合同完成标准逐条判定（每条给结论 + 证据）

1. `pytest --cov=tpops_kafka_collector --cov-report=term-missing --cov-fail-under=80 -q` 通过；
   **映射层与规则引擎 ≥95%**（**逐文件**且**合计**都要核 —— 控制器定案取最严口径）
2. `pytest tests/test_py37_compat.py -q` 通过
3. `ruff check tpops_kafka_collector/` 为 0
4. **杀进程 → 重启演练：不重复、不丢失**（要有**可执行的**测试或脚本，不是报告里的一段话）
5. **60 秒档单轮墙钟 P95 ≤ 25 秒**（实测规模）

## 二、规格逐条判定

### `specs/delivery-and-state/spec.md`

| Requirement | 判定要点 |
|---|---|
| 调度器的到期驱动与并发组 | 到期判定（只有到期的执行）/ 并发执行（墙钟 ≈ 最大值而非之和）/ 单组失败（其余组仍投递、进程不退出） |
| 单轮性能预算 | P95 ≤ 25 秒；命令中心单次往返 ≤ 15 秒 |
| 单轮外部调用数与投递成功率可观测 | `calls`、`delivery_rate`、`failed`；**单轮调用数 > 42 记 WARNING** |
| 平台不保存历史快照 | 不得落历史快照 |
| 水位与差分基线的持久化 | 原子写；**只在投递成功后推进**（批次原子推进） |
| 幂等去重 | 幂等键跨轮持久化 |

### `specs/runtime-and-config/spec.md`

| Requirement | 判定要点 |
|---|---|
| 优雅退出 | SIGTERM → 停发新请求 → 等在途（**超时 30 秒放弃等待**）→ 原子写水位 → flush producer → **exit 0** |
| 健康端点 | `/healthz` 存活、`/readyz` 就绪；TPOPS 认证不可用 → 503；**不泄露凭据** |
| 每轮结构化汇总日志 | tick/耗时/拉取/转换/投递/失败/丢弃不可提供/窗口/告警；**各计数之和自洽** |
| 结构化数据不可变 | frozen dataclass / NamedTuple，不原地改 |

### `specs/mapping-and-rules/spec.md`

映射层本轮**只补测不改实现** —— 核对**没有**被改坏（3.5a 的边界）。

## 三、控制器定死的口径（逐条核）

| # | 口径 | 判定 |
|---|---|---|
| 1 | **水位 `stream` 键 = `f'{instance_id}:{channel}'`** | 指标与告警都是；**不许**用 `'instance-a'` 这类随意取值 |
| 2 | **差分基线恢复只能用 `import_baselines_from` / `export_baselines_into`** | **不许**用 `put_baseline`（键无作用域 → 重启后速率永远算不出来，且不报错） |
| 3 | **差分基线作用域 = `f'{instance_id}:{node_id}'`** | 两个实例共用 node_id 时基线互不覆盖 |
| 4 | **组 × 实例两维并发** | 单 tick 墙钟 ≈ max(组耗时, 实例耗时)，**不是乘** |
| 5 | **平台画像「选择所有实例」** | 不挑、不筛、不取第一个、不按 `mode` 筛 |
| 6 | **`master_count` 按唯一 `node_id` 计数** | 不是数条目（实测条目 6 / 唯一 3） |
| 7 | **`dropped_unsupported` 计数** | 3 条不可提供指标，每轮汇总日志里报出来 |
| 8 | **汇总日志字段名** | `tick/elapsed/fetched/mapped/produced/failed/dropped_unsupported/windows/alarms` + `calls`/`delivery_rate` |
| 9 | **日志中心化** | 消费 `config.log_level` / `config.log_format`；输出 stderr；**幂等**（重复调用不叠 handler）；第三方降噪；异常带堆栈；**凭据/token/SQL 不落日志**；**元测试** |
| 10 | **启动期配置缺失** | 非 0 退出码 + 日志指明**变量名** + **未向 TPOPS/Kafka 发任何请求**（mock 断言 send 从未被调用） |

## 四、硬约束（违反即 Important）

- Python 3.7.9 红线（海象 / 内建泛型 / PEP 604 / `typing.Literal|Protocol|TypedDict|Final` / `cached_property` / `zoneinfo` / `removeprefix` / `to_thread`）
- **不新增依赖**
- **单文件 < 400 行、函数 < 50 行**（**生产文件**；测试文件按仓库既有惯例放宽，但要在报告里说明哪些超了、为什么）
- **不可变数据**：跨模块 frozen dataclass / NamedTuple
- **凭据只走环境变量**：禁硬编码、禁写日志、禁进异常消息、**禁进健康检查响应**
- **错误显式处理**：不静默吞异常；用 `logging`，**不用 `print`**（全包 `print` 数量应为 0）
- **生产代码唯一**：`tpops_kafka_collector/`；不得 import `tools.*` / `data.*`；不得读仓库根 `data/`
- **消息只有一个序列化出口**：`kafka/serializer.py`（AST 守卫测试守着）
- **不改** `execution-contract.md` / `tasks.md` / `specs/**` / `data/mapping/*.json` / `data/merged_sql/*`
- **不静默出 0**：取不到值就不产出那条消息

## 五、专项深挖（这几处最容易藏「不报错但数据错」）

1. **水位推进的时序** —— 投递失败时水位**确实**没推进？「批次原子推进」是真的
   （不是「部分成功也推进」）？重启后续传会不会**丢**？
2. **幂等键** —— 跨轮持久化了？重启后仍能去重？键里**确实含 instance_id**？
3. **差分基线** —— 作用域带 instance_id 吗？跨 master 不互相覆盖吗？
   **重启后**能算出速率吗（`import_baselines_from` 那条路走通了吗）？
4. **并发** —— 「组 × 实例两维」是真的两维吗？线程池用对了吗？
   有没有共享可变状态（采集器实例、`_deltas`、`_scoped_deltas`）导致串扰？
5. **优雅退出** —— 「超时 30 秒放弃等待」这条**真的实现了吗**（不是靠测试里的假超时蒙混）？
   退出码 0 吗？水位文件完整吗？
6. **平台画像** —— 「固化文件已存在 → 不重新发现」（断言平台列表接口**没被调用**）？
   核对不一致时记 ERROR / WARNING 了吗？
7. **日志** —— `log_level=DEBUG` 真的能输出 INFO？`log_format=json` 真的逐行 `json.loads` 得通？
   **凭据 / mini-token / 业务 SQL 有没有漏进日志**？
8. **空结果 / 空分组** —— 是不是静默返回空列表？

## 六、必做的变异验证（挑关键的，自己改坏再改回）

至少覆盖这几条：
- 把水位推进挪到投递**之前** → 守卫测试应红
- 把 `stream` 键去掉 `instance_id` → 守卫测试应红
- 把差分基线作用域去掉 → 守卫测试应红（**回归层**也要红 —— M-N4）
- 把 `import_baselines_from` 换成 `put_baseline` → 守卫测试应红
- 把两维并发改成组内串行 → 守卫测试应红
- 把退出码改成非 0 → 守卫测试应红
- 把 `log_format=json` 硬编码成 text → 元测试应红

**每次变异后必须把实现改回去并重跑全量**。

## 七、报告格式

```
# Wave 3 审查报告 —— wave-3-scheduling-testing

- **波次** / **base** / **head** / **审查范围**
- **结论**：verdict = pass | fail
- **Critical / Important / Minor 计数**
- 合同完成标准逐条判定表
- 规格逐条判定表
- 控制器定死口径逐条判定表
- 硬约束核对表
- 专项深挖的发现
- **变异验证记录**（改了什么 → 哪个测试红了 → 实际输出 → 改回去了没有）
- 实施方报告的**不准确断言**清单（若有）
- 签发 receipt 的命令：
    ssf execution review changes/tpops-kafka-collector \
      --wave wave-3-scheduling-testing --base <base> --head <head> \
      --report .superpowers/sdd/reviews/wave-3-scheduling-testing.md --verdict <pass|fail>
```
