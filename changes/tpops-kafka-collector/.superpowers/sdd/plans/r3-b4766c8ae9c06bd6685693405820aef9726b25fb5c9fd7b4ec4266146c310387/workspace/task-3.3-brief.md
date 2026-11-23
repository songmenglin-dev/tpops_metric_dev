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
PlatformProfile.load_or_discover()  → 见下节：实例信息从平台发现并固化
MessageProducer             → PLAINTEXT，指标/告警分 Topic
五个采集器                   → monitor / command_center / diagnostic / instance / alarm
HealthServer                → 3.2 已完成
Scheduler                   → 3.1 已完成
```

---

# 平台画像固化文件（`platform_profile.json`）

## 目标

**实例 ID 不走配置** —— 采集端连上 TPOPS 后**自己发现**，然后**固化到文件**，
之后以文件为准。这样换平台、换实例都不用改配置或改代码。

## 文件位置与格式

放在 `state_dir` 下（`STATE_DIR` 配置项，默认 `./state`）：

```json
{
  "version": 1,
  "instances": [
    {
      "instance_id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
      "instance_name": "gauss-6374",
      "mode": "Combined",
      "engine_name": "gaussdbv5",
      "engine_version": "V2.0-10.0.0",
      "master_count": 3
    },
    {
      "instance_id": "c35b8d078cae4a9eb05d616ed5cf032fin14",
      "instance_name": "gauss-75a2",
      "mode": "Ha",
      "engine_name": "gaussdbv5",
      "engine_version": "V2.0-10.0.0",
      "master_count": 1
    }
  ],
  "discovered_at": "2026-11-23T04:00:00Z"
}
```

**原子写**：与 `watermark.py` 同一套 —— 同目录临时文件 → `fsync` → `os.replace`。
可以复用 `watermark.py` 里已有的原子写辅助函数。

## 发现规则：**选择所有实例**

```
1. 取平台实例列表（GET /rds/v3.2/instances）
2. **全部实例都要采集** —— 不挑、不筛、不取第一个
3. 平台没有实例 → 明确报错，指明「实例列表为空」
```

**这是用户明确的决定**：采集端面向「连什么平台都能跑」，
一个平台上挂了几个实例就采几个，**不假设只有一个**。
这与 `CLAUDE.md` 的原始设计一致：「命令中心是「instance_id + node_ids 下发」，
无法一次带多实例 → 采集端**按实例循环**」。

**不按 `mode` 筛选** —— `mode` 字段仍然**记录**在画像里（便于排查），但不参与选择。

### ⚠️ 「master 节点数」的口径 —— 按 `node_id` 去重后计数，不是数条目数

实测留档（分布式实例 `gauss-6374`）的节点清单：

```
节点总数 12   role: master 6 / slave 6   type: DN 9 / CN 3
master 的 6 条里，_0_0 / _0_1 / _0_2 各出现两次（一次 CN、一次 DN）
→ 唯一的 master node_id 只有 **3 个**
```

数条目会得 6（虚高一倍），**必须按唯一 `node_id` 计数**。
`tpops/command_center_nodes.py::_select_masters` 已经有现成的「挑 master + 按
node_id 去重」逻辑 —— **复用它，不要另写一份**。

`master_count` 只是**记录**与**拓扑核对**用，不参与选择。

## 多实例的键隔离（**必查**）

采集多个实例时，下面这些键**必须能区分实例**，否则会互相覆盖：

| 键 | 现状 | 结论 |
|---|---|---|
| 告警幂等键 | `alarm_idempotency_key(alarm_id, instance_id, first_alarm_time, status)` | ✅ **已含 instance_id** |
| 指标幂等键 | `metric_idempotency_key(instance_id, metric_name, dimensions, timestamp)` | ✅ **已含 instance_id** |
| `resourceName` | `{host_ip}_{node_id}`，node_id 来自**该实例**的节点清单 | ✅ **天然区分** |
| 差分基线作用域 | `ScopedCounterDeltaStore(self._deltas, node_id)` | ⚠️ 同一平台 node_id 跨实例唯一，**但作用域应显式带上 instance_id** 更稳（`f'{instance_id}:{node_id}'`）—— 同一采集器对象循环多个实例时作用域缓存是共享的 |
| **指标水位 `stream`** | 生产代码**尚未调用** `advance_metric`（水位推进归 wave 3 任务 3.1 的调度器） | ⚠️ **`stream` 键的约定还没定 —— 本次就要定成含 instance_id**，例如 `f'{instance_id}:{channel}'` |
| **告警水位 `stream`** | 同上 | ⚠️ 同上，按实例分开 |

> 这两条水位键**现在是空白**（测试里用的是 `'instance-a'` / `'b-class'` 这类随意取值），
> 正好趁 3.1 还没写把约定定下来。

## 固化后不再自动重新发现

```
启动时：
  固化文件存在  → 读它（**以文件为准**），但顺手核对：
                     · 画像里的每个 instance_id 在平台实例列表里还在吗？
                     · 平台列表里有没有**新增**的实例？
                     · engine_name 变了吗（平台升级 → 指标码可能变）？
                   核对不过 → 明确记 ERROR / 告警，**不要静默继续**
  固化文件不存在 → 发现 → 记录全部实例 → 原子写入固化文件
```

**为什么不能每轮重新发现**：平台实例列表的顺序可能变，也可能客户后来加了实例。
每轮重新发现会导致**静默增删采集目标** —— 水位与差分基线全部错位，
而且是「不报错但数据错」的形态。

> **新增实例怎么办**：平台多了一个实例，画像里没有 —— 记 **WARNING**
> 并提示「删除画像文件可重新发现」，**但不自动加**（加了就等于每轮重新发现）。
> 这与「新增实例要人工确认」是一回事。

**文件可手工编辑**：运维要增删实例，改这个文件即可（下一轮启动生效），
**不用改代码、不用改配置**。这就是「固化」的价值。

## 核对内容

| 核对项 | 不一致时 |
|---|---|
| 画像里的某个 `instance_id` 不在平台实例列表里 | ERROR：该实例已不存在（被删/重建？），需人工确认 |
| 平台列表里有**画像没记录**的实例 | WARNING：平台新增了实例，画像需人工更新（删画像文件可重新发现） |
| `engine_name` 与画像记录的不同 | WARNING：平台已升级，平台指标码可能已变，映射表可能需要重新生成 |
| `master_count` 与画像记录的不同 | WARNING：拓扑变了（扩缩容），多 master 归集口径要复核 |

## 要有守卫测试

- 「实例列表只有一个」→ 画像里记它
- 「实例列表有多个」→ **画像里全部都记**（**断言条数与顺序都对**）
- 「实例列表为空」→ 抛明确异常
- 「固化文件已存在」→ **不重新发现**（断言平台列表接口**没被调用**）
- 「画像里的 instance_id 不在平台列表里」→ 记 ERROR
- 「平台列表有画像没记录的实例」→ 记 WARNING
- 原子写：中途失败不会留下半截 JSON

## 硬约束

- 画像文件**不含凭据**（凭据只走环境变量）
- 画像文件里的 `discovered_at` 用 **UTC ISO 格式**
- 不可变数据：内部用 frozen dataclass 承载，读出来即不可变

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
