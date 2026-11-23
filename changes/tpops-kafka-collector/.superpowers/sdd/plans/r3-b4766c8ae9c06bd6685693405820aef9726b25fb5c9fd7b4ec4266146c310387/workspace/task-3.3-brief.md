# 任务 3.3 简报 —— 入口装配与信号处理

> **前置条件**：3.1（调度器）与 3.2（健康端点）已落地。

## 目标

新建 `tpops_kafka_collector/main.py`：装配全部依赖 → 启动调度循环 →
捕获 SIGTERM/SIGINT → 优雅退出 → 每轮结构化汇总日志。

**这是合同里「`python -m tpops_kafka_collector.main` 能起、能收 SIGTERM、
退出码 0、水位文件完整」这条完成标准的落地点** —— 合同的 Wave 1 完成标准写了
这条，但 `main.py` 在 Wave 1 的输出清单里没有（那个口径不一致已在账本里记录，
明确归到 wave 3 的任务 3.3）。

## 必读

- 合同 `changes/tpops-kafka-collector/execution-contract.md` 的 Wave 3 段
- 规格 `changes/tpops-kafka-collector/specs/runtime-and-config/spec.md`
  的「优雅退出」与「配置校验」
- 开发方案 `docs/开发方案-GaussDB指标与告警采集.md` §4.6.5
- 共享约定：`workspace/wave3-execution-brief.md`

## 优雅退出（开发方案 §4.6.5 + 规格「优雅退出」）

```
收到 SIGTERM / SIGINT
  → 设置 shutdown_event
  → 调度器停止发起新任务
  → 等待在途 HTTP 请求完成（超时 30 秒）
  → 刷盘水位（原子写）
  → 关闭 Kafka producer（flush）
  → exit(0)
```

**两条硬性场景**（都有测试守卫）：

| 场景 | 期望 |
|---|---|
| 主循环中收到 SIGTERM | 不再发起新的 HTTP 请求 → 等在途请求结束 → 写出**完整**水位文件 → producer 已 flush → **退出码 0** |
| 收到 SIGTERM 后仍有 HTTP 请求未完成且**超过 30 秒** | **放弃等待**该请求，写出水位并以**退出码 0** 终止，**不挂死** |

注意第二条 —— 「超时后放弃等待」是**必须**的行为，不是可选优化。

## 启动期校验（规格「配置校验」）

- 启动阶段配置缺失 → 以**非 0 退出码**终止
- 日志中**指明缺失的变量名**
- 且**未向 TPOPS 或 Kafka 发起任何请求**（这条要有测试守卫 —— 用 mock 断言
  「HTTP 客户端的 send 从未被调用」）

## 每轮结构化汇总日志

每轮结束记录：本轮执行了哪些组、各组耗时、消息条数、投递成功率、外部调用次数、
水位推进到哪。**用 `logging`，不用 `print`**。

## 装配顺序（注意依赖）

```
config.load_config()        → 启动期校验（缺变量即退出，不发任何请求）
WatermarkStore.load()       → 从磁盘恢复水位 / 差分基线 / 幂等键
TPOPSClient                 → mini-token 认证（惰性，首次请求时才认证）
MessageProducer             → PLAINTEXT，指标/告警分 Topic
五个采集器                   → monitor / command_center / diagnostic / instance / alarm
HealthServer                → 3.2 已完成
Scheduler                   → 3.1 已完成
```

## 硬约束

- Python 3.7.9 红线（禁海象 / 内建泛型 / `zoneinfo` / `cached_property` …）
- 不新增依赖（信号用标准库 `signal`）
- 单文件 < 400 行、函数 < 50 行
- **凭据只走环境变量**：禁硬编码、禁写日志、**禁进异常消息**
- 错误显式处理：包装成自定义异常并带上下文，不静默吞异常
- **不改** `execution-contract.md` 与 `tasks.md`

## 提交纪律

**不要 `git commit`**（由控制器统一提交）。只写文件 + 跑测试。

## 测试放哪

```
tests/integration/test_main.py
```

测试要点：
- 配置缺失 → 非 0 退出码 + 日志指明变量名 + **未发任何 HTTP 请求**
- 收到 SIGTERM → 退出码 0 + 水位文件完整 + producer 已 flush
- 在途请求超过 30 秒 → 放弃等待，仍以退出码 0 退出（**用可控的假时钟，
  不要真等 30 秒**）

## 报告

写到 `.../workspace/task-3.3-report.md`（**绝对路径**），含 RED→GREEN 证据、
三条场景各自的守卫测试名、以及**手工起进程发 SIGTERM 看退出码与水位文件**的实际结果。

## 验收判据

- `pytest tests/integration/test_main.py -q` 通过
- `ruff check tpops_kafka_collector/` 为 0
- 手工演练：`python -m tpops_kafka_collector.main` 能起 → `kill -TERM` → 退出码 0 + 水位文件完整
