# 任务 1.3 —— 重试与退避框架 报告

> 变更：`tpops-kafka-collector` / wave-1-skeleton-platform / 任务 1.3（W03）
> 工作目录：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（worktree）

## 1. 做了什么

新建重试与退避框架，把「哪些异常能重试」与「重试时等多久」两件事分开：

| 文件 | 状态 | 行数 | 说明 |
|---|---|---|---|
| `tpops_kafka_collector/retry.py` | 新建 | 213 | 指数退避 + 抖动；重试判定调用 `errors.is_retryable` |
| `tests/unit/test_retry.py` | 新建 | 353 | 32 个用例 |

未改动任何其它文件（`errors.py` / `config.py` / `mapping/` 一律未动）。

对外 API：

- `RetryPolicy` —— frozen dataclass：`max_attempts` / `base_delay` / `factor` / `jitter_ratio` / `max_delay`
- `DEFAULT_POLICY` —— 1s→2s→4s→8s，5 次尝试（首次 + 4 次重试）
- `nominal_delays(policy)` —— 基准退避序列，默认 `(1.0, 2.0, 4.0, 8.0)`
- `compute_delay(policy, attempt, random_source)` —— 实际等待
- `retry_call(func, *args, policy=..., sleep=..., random_source=..., **kwargs)`
- `retry(...)` —— 与 `retry_call` 同语义的装饰器

### 关键设计决定

1. **判定不重写**：模块内 `from .errors import is_retryable`，判定只认这一个入口。
   没有另写一份「按类名精确相等」的判定表 —— `requests` 实际抛的是
   `ReadTimeout` / `ConnectTimeout`（`Timeout` / `ConnectionError` 的子类），
   精确相等会把最常见的瞬时故障判成不可重试。`errors.is_retryable` 已按 MRO 基类链实现，本模块直接复用。
2. **不可重试立即上抛**：`AuthError` / `ProtocolError` / `MappingError`（含子类 `DecodeError`）
   一次都不重试，原样上抛（认证失败重试会累计 `login_failed_attempts` 锁号）。
3. **可重试退避重试**：`TransientError` / `ProduceError` / 超时类按 1s→2s→4s→8s 重试。
4. **抖动可测**：`jitter_ratio` 默认 0.1，口径是**只增不减** ——
   实际等待落在 `[基准退避, 基准退避 × (1 + jitter_ratio)]`，保证「至少等满基准退避」恒成立；
   `jitter_ratio = 0` 时退避序列完全确定。随机源可注入（默认 `random` 模块，只需有 `uniform(a, b)`）。
5. **不真等**：`sleep` 可注入（默认 `time.sleep`），测试注入记录器。
6. **耗尽后原样上抛**：重试用尽时抛回**最后一次的原始异常**（不换类型），
   调用方仍能按 `TransientError` / `ProduceError` 分支处理；同时 ERROR 日志记录放弃原因。
7. Python 3.7.9 合规：无海象、无 `list[str]`、用 `typing` 泛型；frozen dataclass 不可变；
   `logging` 不用 `print`；单文件 213 行 < 400，函数均 < 50 行。

## 2. TDD 记录

### RED（先写测试，看到它失败）

第一次运行：

```
$ conda run -n tpops python -m pytest tests/unit/test_retry.py -q
ERROR collecting tests/unit/test_retry.py
tests/unit/test_retry.py:22: in <module>
    from tpops_kafka_collector.retry import (
E   ModuleNotFoundError: No module named 'tpops_kafka_collector.retry'
Interrupted: 1 error during collection
```

**RED 时失败的断言 / 失败点**：测试模块在 `import tpops_kafka_collector.retry`
处直接 `ModuleNotFoundError` 中断收集 —— 「重试框架尚不存在」这一事实让全部 32 条断言
（不可重试立即上抛、退避序列 1/2/4/8、抖动确定性、注入点、策略校验）无从执行。

实现过程中还真实触发过一次**断言级** RED：

```
FAILED tests/unit/test_retry.py::test_nominal_delays_follow_the_configured_factor
E   assert (0.5, 1.5, 4.5, 13.5, 30.0) == (0.5, 1.5, 4.5, 13.5, 40.5)
E     At index 4 diff: 30.0 != 40.5
```

这是**测试期期望值写错**（忘了 `max_delay` 默认封顶 30s），不是实现缺陷 ——
修的是测试（把该用例的 `max_delay` 提到 100 以隔离 factor），实现未动。

### GREEN

```
$ conda run -n tpops python -m pytest tests/unit/test_retry.py -q
................................                                         [100%]
32 passed in 0.62s
```

覆盖率（`--cov=tpops_kafka_collector.retry`）：

```
tpops_kafka_collector/retry.py      67      0   100%
```

全量回归：

```
$ conda run -n tpops python -m pytest -q
548 passed in 13.29s
```

（基线 476 passed → 548 passed，增量来自本任务的 32 条 + 并发中的 1.4/1.7 任务。
⚠️ 见「遗留问题」中关于 1.4 序列化器的并发瞬时红灯。）

### 静态检查

```
$ ruff check tpops_kafka_collector/retry.py tests/unit/test_retry.py
All checks passed!
```

## 3. 测试覆盖了什么（32 条）

| 组 | 用例 |
|---|---|
| 基本行为 | 成功即返回不等待；参数透传 |
| 不可重试立即上抛 | `AuthError` / `ProtocolError` / `MappingError` / `DecodeError` 参数化：只调用 1 次、`sleep` 0 次 |
| 可重试退避 | `TransientError`、`ProduceError`、超时子类（`ReadTimeout`）、`TransientError` 自定义子类、真实 `requests.exceptions.ReadTimeout`（`importorskip`） |
| 耗尽 | 重试用尽后原样上抛最后一次异常；`max_attempts=1` 不重试 |
| 退避序列 | 默认 `(1.0, 2.0, 4.0, 8.0)`；自定义 factor；`max_delay` 封顶 |
| 抖动 | 固定随机源下结果确定（`1.25`）；只增不减；`jitter_ratio=0` 关闭；与 `random` 模块兼容；随机源收到单位区间 |
| 策略校验 | 7 组非法参数 → `ValueError`；负数 attempt → `ValueError` |
| 装饰器 / 日志 | `@retry(...)` 同语义；重试时 WARNING 日志含「重试」 |

## 4. 偏离简报之处

1. **额外提供 `retry` 装饰器与 `RetryPolicy` / `nominal_delays` / `compute_delay`**
   —— 简报只说「新建 `retry.py`，指数退避 + 抖动」。把策略抽成 frozen dataclass、
   把退避计算拆成纯函数，是为了让「抖动可测 / 退避序列可断言」这条要求落地
   （不注入就无法在测试里断言序列）。**未超出任务范围**（未新建任何其它文件）。
2. **`sleep` 与 `random_source` 默认值取自标准库**（`time.sleep` / `random`），
   简报要求「注入一个可替换的随机源（默认 `random`）」「重试等待要能注入/关闭」—— 已满足。
3. **重试用尽后原样上抛原始异常**，没有包装成新的 `RetryExhausted` 异常。
   理由：换类型会破坏调用方按 `TransientError` / `ProduceError` 的分支处理
   （1.6 TPOPS 客户端、1.5 Kafka 生产者都要用）。放弃信息走 ERROR 日志。
4. **抖动口径选了「只增不减」**（实际等待 ∈ [基准, 基准×(1+ratio)]），而不是
   常见的「full jitter」（在 [0, 基准] 内随机）。原因：简报要求退避序列可断言，
   且「至少等满基准退避」更符合平台限流保护的意图。若控制器要求改成
   full jitter，只需改 `compute_delay` 一处并同步调整测试。
5. **模块内不重复实现可重试判定** —— 严格按简报要求复用 `errors.is_retryable`，
   未另写按类名精确相等的判定。

## 5. 遗留问题 / 需要控制器知道

1. ⚠️ **并发瞬时红灯（非本任务引起）**：全量回归的某一次运行出现
   `FAILED tests/unit/test_serializer.py::test_kafka_package_has_only_one_json_dumps`
   （断言 `not ['__init__.py']`）—— 属并发中的任务 1.4（序列化器）的在制品，
   我的文件不涉及 `kafka/` 包。请在该任务收敛后重跑全量。
   同时 `ruff check tpops_kafka_collector/` 当时对 `tpops_kafka_collector/kafka/*.py`
   报 7 处（`UP009` / `UP031`），同样属 1.4 的在制品；**我的两个文件 ruff 为 0**。
2. **`retry_call` 的关键字专属参数**（`policy` / `sleep` / `random_source` / `description`）
   不透传给被调函数；若下游函数有同名形参会冲突（已在 docstring 写明，
   并给了 `@retry` 装饰器作为替代路径）。后续任务（1.5/1.6）接入时注意。
3. **`max_attempts` 的语义**（总尝试次数，含首次）本任务自定，合同未规定；
   默认 5 = 首次 + 4 次重试 = 退避序列 1/2/4/8。若控制器希望改成「重试次数」语义，
   需要同步改 `nominal_delays` 与 `RetryPolicy` 的校验。
4. **未提交**（按简报要求不 `git commit`，由控制器统一提交）。
