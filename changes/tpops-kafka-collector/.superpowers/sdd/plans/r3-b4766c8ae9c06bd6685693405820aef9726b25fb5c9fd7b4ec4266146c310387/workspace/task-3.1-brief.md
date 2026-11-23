# 任务 3.1 简报 —— 调度器（到期驱动 + 并发组）

> **前置条件**：wave-2b 的 receipt 已签发 `pass`。未签发前**不开工**。

## 目标

新建 `tpops_kafka_collector/scheduler.py`：**到期驱动**（不是每轮全跑），
tick 内并发执行到期的任务组，全部完成后汇总并推进水位。

## 必读

- 合同 `changes/tpops-kafka-collector/execution-contract.md` 的 Wave 3 段
- 规格 `changes/tpops-kafka-collector/specs/delivery-and-state/spec.md`
  的「调度器的到期驱动与并发组」「单轮性能预算」「单轮外部调用数与投递成功率可观测」
- 开发方案 `docs/开发方案-GaussDB指标与告警采集.md` §4.6（4.6.1~4.6.4）
- 共享约定：`workspace/wave3-execution-brief.md`

## 调度模型（开发方案 §4.6.1）

```python
@dataclass(frozen=True)
class ScheduleEntry:
    task: str                 # "monitor" | "command_center" | "diagnostic" | "instance" | "alarm"
    interval_seconds: int     # 该任务的到期周期
    next_due_at: float        # 下次到期的时间戳（monotonic）
```

每个 tick（默认 30 秒，`POLL_INTERVAL_SECONDS`）：

1. 扫描 due-time 表 → 找出本 tick 到期的任务
2. 到期任务**并发执行**（线程池 —— HTTP I/O 密集，GIL 会释放）
3. 等待全部完成 → 汇总本轮统计 → **推进水位**
4. sleep 到下一个 tick 边界

**为什么并发**：单 tick 内三组任务串行 = 3s + 15s + 5s = 23s，逼近 30 秒边界；
并发后 ≈ max(3s, 15s, 5s) = 15s，**留 2 倍余量**（NFR-01）。

## 五个任务与周期（开发方案 §4.6.2）

| 任务 | 周期 | 覆盖的指标档位 |
|---|---|---|
| `monitor` | 30 秒 | 30 秒 / 60 秒 / 1 分 / 2 分 / 5 分 的 A 类指标（长窗口 + 时间戳切片） |
| `alarm` | 30 秒 | 全部告警 |
| `command_center` | 60 秒（SQL-2 为 300 秒） | 60 秒 / 1 分 / 2 分 / 5 分 的 B 类指标 |
| `diagnostic` | 300 秒 | 5 分钟档的 C 类指标 |
| `instance` | 3600 秒 | 1 小时档的 D 类指标 |

## ⚠️ 每个任务组内部要**按实例循环**

采集端面向「连什么平台都能跑」—— 一个平台上挂了几个实例就采几个
（实例清单来自 wave-3 任务 3.3 的**平台画像固化文件** `platform_profile.json`）。

```
一个 tick 内：
  1. 找出本 tick 到期的任务组
  2. 每个到期的组，对画像里的**每个实例**执行一次
     （命令中心无法一次带多实例 —— 「instance_id + node_ids 下发」）
  3. 全部完成 → 汇总 → 推进水位
```

**「按实例循环」在哪个层次做，有讲究**：

| 做法 | 单 tick 墙钟 | 评价 |
|---|---|---|
| 组并发 → 组内实例串行 | max(组耗时) × 实例数 | 2 个实例就翻倍，逼近 25 秒预算 |
| **组 × 实例两维并发** | max(组耗时, 实例耗时) | ✅ 推荐 |

60 秒档单轮墙钟 **P95 ≤ 25 秒** 是合同完成标准 —— 实例变多时只有两维并发才守得住。

### 水位与基线的键**必须带 instance_id**

多实例下这些键若不带实例就会互相覆盖：

| 键 | 约定 |
|---|---|
| 指标水位 `stream` | `f'{instance_id}:{channel}'`（如 `83cd68f2…in14:command_center`） |
| 告警水位 `stream` | `f'{instance_id}:alarm'` |
| 差分基线作用域 | `f'{instance_id}:{node_id}'`（`ScopedCounterDeltaStore` 的 scope） |
| 幂等键 | ✅ 已含 instance_id（`metric_idempotency_key` / `alarm_idempotency_key`） |

> **`stream` 键的约定现在还是空白** —— 生产代码尚未调用 `advance_metric`
> （水位推进就是本任务的职责），测试里用的是 `'instance-a'` / `'b-class'`
> 这类随意取值。**本次就要把约定定死**，否则多实例下水位会互相覆盖。

**周期必须表驱动**（`data/mapping/metric_rules.json` 的 `collect_cycle`），
不写死。从映射表现算出的实际档位分布（实现时以实算为准）：

| 取数方法 | 指标条数 | 覆盖周期（秒） | 该组的周期 |
|---|---|---|---|
| 监控指标接口 | 11 | 30 / 60 / 300 | 30 |
| 诊断优化接口 | 4 | 120 / 300 | 300 |
| 命令中心 SQL | 82 | 60 / 120 / 300 / 86400 | 60 |
| 实例接口 | 1 | 3600 | 3600 |

**档位取值规则**：开发方案 §4.6.3 说「一条合并 SQL 的刷新周期 = 它覆盖的指标中
**最高需求档**对应的周期」—— **「最高需求」= 最短周期**（最急的那个指标决定刷新频率）。
按 `sql_key` 实算的结果：

| sql_key | 覆盖周期 | 刷新周期 |
|---|---|---|
| `stat_database` / `stat_user_tables` / `activity_agg` / `activity_running` / `activity_detail` / `locks` / `bgwriter` / `settings` / `database` / `prepared_xacts` | 见映射表 | 取最短 |
| `dbstat_sum` | 60 | 60 |

**24 小时档（86400）无处安放** —— 开发方案 §2584 已记录这条：
「以及『24 小时档在调度器里无处安放』」。实现时**不要**自己发明口径：
按 §4.6.3 的「新鲜度检查」处理 —— 指标在自己的档位到期时，若缓存结果的采集时刻
已超过该档位周期，则本轮触发该 SQL 提前刷新。

## 四条硬约束（都有测试守卫）

1. **到期驱动** —— 只有到期的任务被执行，未到期的**不执行**
2. **tick 内并发** —— 三组同时到期时，单 tick 墙钟接近**三组耗时的最大值**而非之和
3. **单组失败不拖垮其他组** —— 某组全失败时，其余组的结果**仍被投递**，
   失败组的数据留待下轮；**进程不退出**
4. **水位只在投递成功后推进** —— 批次原子推进：任一消息失败则整批不推进，下轮重发

## 水位推进（开发方案 §4.6.4）

| 水位 | 内容 |
|---|---|
| 指标水位 | 每个 `(metric_name, dimensions_key)` 的最后成功投递时刻 |
| 差分基线 | 每个 `(instance_id, metric_name, dimensions_key)` 的 `(累计值, 采集时刻)` |
| 告警水位 | 最后处理到的 `first_alarm_time` |
| 幂等键集合 | 最近 N 条消息的幂等键 |

**采集器不推进水位** —— 那是本任务的职责。已有接口在
`tpops_kafka_collector/watermark.py`：`advance_metric` / `advance_alarm` /
`put_baseline` / `remember_key` / `snapshot` / `save`。

## 可观测（规格「单轮外部调用数与投递成功率可观测」）

每轮记录：**外部调用次数**（单轮 SHALL NOT 超过 42 次）、**投递成功率**
（成功投递数 ÷ 计划投递数）、**单轮墙钟耗时**（P95 ≤ 25 秒 @60 秒档）。
用 `logging`，不用 `print`。

## 硬约束

- Python 3.7.9 红线（禁海象 / 内建泛型 / `zoneinfo` / `cached_property` …）
- 不新增依赖（并发用标准库 `concurrent.futures`）
- 单文件 < 400 行、函数 < 50 行
- 不可变数据：`ScheduleEntry` 用 frozen dataclass
- 错误显式处理：用 `logging`，不静默吞异常
- **不改** `execution-contract.md` 与 `tasks.md`（改了会让执行计划 stale → 前四个波次的 receipt 全部作废）

## 提交纪律

**不要 `git commit`**（由控制器统一提交，避免 git index 锁竞争）。只写文件 + 跑测试。

## 测试放哪

```
tests/unit/test_scheduler.py          # 纯单元：到期判定、周期档位、并发组
tests/integration/test_scheduler.py    # 采集器与 Kafka 全部 mock
```

## 报告

写到 `.../workspace/task-3.1-report.md`（**绝对路径**），
含：RED→GREEN 证据、周期档位的实算表、并发耗时证据、
四条硬约束各自的守卫测试名。

## 验收判据

- `pytest tests/unit/test_scheduler.py tests/integration/test_scheduler.py -q` 通过
- `ruff check tpops_kafka_collector/` 为 0
- 单文件 < 400 行、函数 < 50 行
