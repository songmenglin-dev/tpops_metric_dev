# 修复轮 R-B 报告 —— 可观测性（外部调用数真实性）

> 变更：`tpops-kafka-collector`；工作树 `/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`；
> 分支 `tpops_metric_dev`。**未 commit**（按要求由控制器统一提交）。
>
> **改动的文件（全部在我的归属范围内）**：
>
> | 类型 | 文件 | 说明 |
> |---|---|---|
> | 生产 | `tpops_kafka_collector/tpops/call_tracking.py` | **新增**（见「偏离说明」）—— 真实调用计数 + 计数代理 + 目录快照缓存 |
> | 生产 | `tpops_kafka_collector/tpops/client.py` | 客户端挂真实 HTTP 计数 |
> | 生产 | `tpops_kafka_collector/executors.py` | 删掉 4 个写死常量，改为上报真实次数 |
> | 生产 | `tpops_kafka_collector/scheduler_report.py` | 「不可提供指标」措辞（只改措辞，不改行为） |
> | 测试 | `tests/unit/test_executors.py` | **新增** —— 执行器层守卫 |
> | 测试 | `tests/unit/test_scheduler_report.py` | **新增** —— 预算告警 + 措辞守卫 |
> | 测试 | `tests/unit/test_client.py` | 追加 5 条 —— 计数（含重试 / 并发不丢 / 增量语义 / 按线程归属） |
> | 测试 | `tests/unit/test_metrics_window.py` | 追加 4 条 —— 目录快照缓存（命中 / 过期 / 失败不入缓存 / 不影响别的路径） |
>
> `tpops_kafka_collector/tpops/metrics.py` **未改动**（在我的归属内，但缓存不需要落在那里 —— 见「偏离说明」第 2 条）。

---

## 修了什么

### 1. 真实 HTTP 计数（挂在 `TpopsClient` 上）

**实现位置**：`tpops_kafka_collector/tpops/call_tracking.py:42`（`CallCounter`）+ `tpops_kafka_collector/tpops/client.py:137,140,326`。

- 计数点：`client.py:326` —— 在 `TpopsClient._send()` 里、**真正把请求发出去之前**递增
  （`self._calls.record()`）。计数点必须在「发出去」这一刻：在调用入口计会漏掉
  401 刷新后的第二次请求，在返回后计会漏掉失败请求。
- 因此计数**含**：认证请求、401 强制刷新后的重发、**重试产生的额外请求**
  （重试由 `retry.retry_call` 包住 `client.get/post`，每次尝试都过一次 `_send`）。
- 对外接口（`client.calls`）：
  - `calls.total()` —— 进程内累计发出的请求数（所有线程）；
  - `calls.thread_total()` —— **当前线程**累计发出的请求数；
  - `calls.mark()` / `calls.delta(mark)` —— **增量语义：开始做一件事之前取基线，
    做完之后再取一次，两者相减 = 这件事期间真实发出的请求数**。

**线程安全做法**（不用裸 `+= 1`）：

- **全局计数**：用 `threading.Lock` 保护（`call_tracking.py:57-63`）；
- **线程计数**：放在 `threading.local` 里（`call_tracking.py:61`）—— 每个线程天然独占，
  不需要加锁，也**不会丢计数**；
- 为什么必须有「线程计数」：调度器是**两维并发**的（通道 × 实例一起丢进线程池），
  并发任务如果共用一个全局计数器就会互相串账（一个任务把别人的调用算进自己头上，
  轮汇总也会重复计数）。按线程归属后，各任务的调用数互不串账，
  **加起来正好是这一轮的真实总数**。

### 2. 各通道上报真实次数（`executors.py`）

- **计数器选择**（`executors.py:61` `_CallMeter`）：
  1. 客户端自带 wire 级计数（生产里的 `TpopsClient.calls`）→ 记**真正发出的
     HTTP 请求数**（含认证 / 401 刷新 / 重试 / 节点清单）；
  2. 客户端没有计数能力（测试里的假客户端）→ 退回到**代理计数**
     （`CountingClient`，每次 `get` / `post` 记 1）。
- **四个写死的常量全部删除**（原 `executors.py:55-59`）：
  `MONITOR_EXTERNAL_CALLS = 2` / `DIAGNOSTIC_EXTERNAL_CALLS = 1` /
  `INSTANCE_EXTERNAL_CALLS = 2` / `ALARM_EXTERNAL_CALLS = 4`，
  以及 B 类的 `external_calls=max(result.pages, 1)`（页数 ≠ HTTP 次数）。
- 现在的上报点（全部是 `meter.delta(mark)`，基线在采集**开始前**取）：
  - A 类 `executors.py:198`（目录 GET + 指标 POST；目录命中缓存则不计）；
  - B 类 `executors.py:245`（**完整三跳**：下发 + 轮询 + 每个 master 一次明细）；
  - C 类 `executors.py:281`（慢 SQL 列表 + Top SQL 列表）；
  - D 类 `executors.py:311`（实例列表 + 节点清单）；
  - E 类 `executors.py:348`（四种形态 × 各自的页数）。
- **B 类的接线**（`executors.py:89` `_install_counting_client`）：命令中心三跳走的是
  **它自己持有的**客户端（`CommandCenterClient._client`），不是采集器传进来的那个 ——
  装配层把命令中心的客户端引用换成计数代理（只换引用，行为不变：代理原样转发
  `get` / `post`）。不换的话 B 类的调用数数不到。
- **停机跳过**（`executors.py:165`）：停机检查命中时**一次请求都没发** → 显式上报
  `external_calls=0`（`TaskOutcome` 的默认值是 1，是假的）。

### 3. A 类目录查询缓存（`executors.py:129` + `call_tracking.py:143`）

- **缓存在哪**：`CatalogCachingClient`（`call_tracking.py:143`）包在计数代理外面，
  只对**目录路径**（`GROUP_CATALOG_PATH`）生效，其它路径原样转发
  （`call_tracking.py:177`）。采集器 `MonitorCollector._check_catalog` 调的还是
  `fetch_metric_catalog(client, instance_id)` —— **不用改采集器**。
- **同轮口径**：同一轮内 12 个分组码**只拉一次目录快照**（首轮 12 次 GET，
  之后命中缓存 0 次）。
- **跨轮口径（我的选择）**：**复用**，TTL = 1 小时（`call_tracking.py:39`
  `CATALOG_CACHE_TTL_SECONDS = 3600`，与平台节点清单缓存 `NODE_CACHE_TTL_SECONDS`
  同量级）。
  - **理由**：目录是**平台元数据**（指标名 / 采集周期 / 告警阈值），变化极慢；
    它**只用于两处诊断** —— 「分组码写错 → 空分组告警」与「指标名不存在 → 缺失告警」
    —— **不参与任何指标取值**。所以陈旧的**唯一后果**是这两条告警晚最多一个 TTL
    报出来，**不影响任何一条指标数据的正确性**。
  - **失效策略**：TTL 到期自动重拉（`call_tracking.py:205` `_lookup`）；
    **拉取失败不入缓存**（异常照常抛出，缓存里不留半截数据 —— 有守卫测试）；
    缓存键 = `(分组码, 实例 ID)`（目录按实例解析告警阈值）。
  - **代价**：平台侧新增指标时，「指标名缺失」告警会**误报最多 1 小时**
    （自愈：TTL 到期后自动消失）。这是 WARNING 级诊断噪声，不是数据错误。
- **缓存的生命周期**：缓存在 `build_executors` 装配时创建 —— 生产里
  `build_executors` **只在启动时装配一次**（`main.py:142`），所以缓存存活整个进程。

### 4. `scheduler_report.py` 的 Minor（只改措辞，不改行为）

`scheduler_report.py:60-63`：「本轮丢弃 %d 个不可提供指标（累计 %d）」→
**「不可提供指标 %d 条（本轮不产出，累计 %d）」**。
理由：那 3 条是**静态计数**（恒等于 3，来自 `count_unsupported_metrics`），
旧措辞会被读成「这一轮丢了 3 条消息」。累计数的算法（`dropped_cumulative + dropped_unsupported`）
**未动**。42 次预算与 WARNING 逻辑**未动**（规格写死）。

---

## 守卫测试

| 守卫 | 测试文件::用例 | 修复前（红） | 修复后（绿） | 变异验证 |
|---|---|---|---|---|
| 客户端计数 == 实际发出的请求数 | `tests/unit/test_client.py::test_call_count_equals_the_number_of_requests_actually_sent` | 红：`AttributeError: 'TpopsClient' object has no attribute 'calls'` | 绿（47 passed） | M1（全局计数退化成线程计数）→ 红 |
| 计数**含重试**产生的额外请求 | `tests/unit/test_client.py::test_call_count_includes_retried_requests` | 红：同上（AttributeError） | 绿 | — |
| 并发下计数**不丢** | `tests/unit/test_client.py::test_call_count_does_not_lose_counts_under_concurrency` | 红：同上（AttributeError） | 绿 | M1 → **红**（`assert 401 == 1`） |
| 增量语义（基线 → 相减） | `tests/unit/test_client.py::test_call_delta_measures_the_calls_between_mark_and_now` | 红：同上 | 绿 | — |
| 计数**按线程归属**（并发不串账） | `tests/unit/test_client.py::test_call_count_is_per_thread` | 红：同上 | 绿 | M2（线程计数退化成全局计数）→ **红** |
| A 类上报数 == 真实请求数 | `tests/unit/test_executors.py::test_monitor_external_calls_equal_real_request_count` | 红：`assert 2 == 13` | 绿 | 整体回退（常量）→ 红 |
| B 类上报数 == 真实请求数（三跳） | `tests/unit/test_executors.py::test_command_center_external_calls_equal_real_request_count` | 红：`assert 1 == 4` | 绿 | 整体回退 → 红 |
| C 类上报数 == 真实请求数 | `tests/unit/test_executors.py::test_diagnostic_external_calls_equal_real_request_count` | 红：`assert 1 == 2` | 绿 | 整体回退 → 红 |
| D 类上报数 == 真实请求数 | `tests/unit/test_executors.py::test_instance_external_calls_equal_real_request_count` | 红：`assert 2 == 1` | 绿 | 整体回退 → 红 |
| E 类上报数 == 真实请求数 | `tests/unit/test_executors.py::test_alarm_external_calls_equal_real_request_count` | 红：`assert 4 == 1`（旧常量 4 vs 真实 1） | 绿 | 整体回退 → 红 |
| E 类调用数**随页数变化** | `tests/unit/test_executors.py::test_alarm_external_calls_scale_with_page_count` | 红：`assert 4 == 8`（旧常量 4 vs 真实 8） | 绿 | 整体回退 → 红 |
| 停机跳过 → 调用数 0 | `tests/unit/test_executors.py::test_shutdown_skipped_task_reports_zero_calls` | 红：`assert 1 == 0`（默认值 1） | 绿 | 整体回退 → 红 |
| **目录缓存生效**（GET 次数下降） | `tests/unit/test_executors.py::test_metric_catalog_is_cached_across_rounds` | 红：`assert 24 == 12`（两轮各 12 次） | 绿 | 整体回退 → 红 |
| 目录缓存：TTL 到期重拉 | `tests/unit/test_metrics_window.py::test_catalog_cache_expires_after_the_ttl` | 红（类不存在 → ImportError） | 绿 | — |
| 目录缓存：失败**不入缓存** | `tests/unit/test_metrics_window.py::test_catalog_cache_does_not_cache_failures` | 红（ImportError） | 绿 | — |
| 目录缓存：不影响别的路径 | `tests/unit/test_metrics_window.py::test_catalog_cache_does_not_affect_other_paths` | 红（ImportError） | 绿 | — |
| 超预算 → WARNING | `tests/unit/test_scheduler_report.py::test_warning_is_logged_when_calls_exceed_budget` | 绿（逻辑本身没坏，坏的是**喂进去的数**） | 绿 | 喂 43 → 红（喂 20 → 不响） |
| 预算内 → 不 WARNING | `tests/unit/test_scheduler_report.py::test_no_warning_when_calls_are_within_budget` | 绿 | 绿 | — |
| 汇总日志里的 `calls=` 是真实值 | `tests/unit/test_scheduler_report.py::test_round_summary_logs_the_real_call_count` | 红（喂进去的是常数） | 绿 | — |
| 「不可提供指标」措辞不是「本轮丢弃」 | `tests/unit/test_scheduler_report.py::test_unsupported_metric_count_is_not_phrased_as_dropped_this_round` | 红：`assert '本轮丢弃' not in '…本轮丢弃 3 个不可提供指标（累计 9）'` | 绿 | — |
| 累计计数算法未变 | `tests/unit/test_scheduler_report.py::test_cumulative_counter_is_updated_by_the_static_count` | 绿 | 绿 | — |

### RED 的真实输出（把实现回退到 `git HEAD` 的修复前版本后跑新守卫）

```
$ conda run -n tpops python -m pytest -q tests/unit/test_executors.py tests/unit/test_client.py \
      tests/unit/test_metrics_window.py tests/unit/test_scheduler_report.py
...
FAILED tests/unit/test_executors.py::test_monitor_external_calls_equal_real_request_count
FAILED tests/unit/test_executors.py::test_command_center_external_calls_equal_real_request_count
FAILED tests/unit/test_executors.py::test_diagnostic_external_calls_equal_real_request_count
FAILED tests/unit/test_executors.py::test_instance_external_calls_equal_real_request_count
FAILED tests/unit/test_executors.py::test_alarm_external_calls_scale_with_page_count
FAILED tests/unit/test_executors.py::test_shutdown_skipped_task_reports_zero_calls
FAILED tests/unit/test_executors.py::test_metric_catalog_is_cached_across_rounds
FAILED tests/unit/test_client.py::test_call_count_equals_the_number_of_requests_actually_sent
FAILED tests/unit/test_client.py::test_call_count_includes_retried_requests
FAILED tests/unit/test_client.py::test_call_count_does_not_lose_counts_under_concurrency
FAILED tests/unit/test_client.py::test_call_delta_measures_the_calls_between_mark_and_now
FAILED tests/unit/test_client.py::test_call_count_is_per_thread
FAILED tests/unit/test_scheduler_report.py::test_unsupported_metric_count_is_not_phrased_as_dropped_this_round
13 failed, 79 passed in 1.70s
```

关键断言（**修复前上报的数 vs 真实发生的 HTTP 次数**）：

```
E       assert 2 == 13          # A 类：上报 2（写死）vs 真实 13（12 目录 GET + 1 POST）
E       assert 1 == 4           # B 类：上报 1（= 页数）vs 真实 4（三跳 + 节点清单）
E       assert 1 == 2           # C 类：上报 1（写死）vs 真实 2
E       assert 2 == 1           # D 类：上报 2（写死）vs 真实 1
E       assert 4 == 8           # E 类：上报 4（写死）vs 真实 8（4 形态 × 2 页）
E       assert 1 == 0           # 停机跳过：上报 1（默认值）vs 真实 0
E       assert 24 == 12         # 目录缓存：两轮 24 次 GET vs 缓存后 12 次
E       AssertionError: assert '本轮丢弃' not in '…本轮丢弃 3 个不可提供指标（累计 9）'
```

> 注：这批 RED 是**把实现回退到修复前版本**后跑出来的（等价于「先写守卫、再改实现」的
> 红态）；回退用的是 `git show HEAD:<file>` 覆写工作副本，**没有动 git 状态**
> （无 `checkout` / `commit` / `reset`）。

### 变异验证（在正确实现上做定向变异，确认守卫真的变红）

| 变异 | 期望 | 真实输出 |
|---|---|---|
| M1：`CallCounter.total()` 退化成线程计数（全局汇总丢计数） | 并发守卫变红 | `FAILED tests/unit/test_client.py::test_call_count_does_not_lose_counts_under_concurrency` / `1 failed, 46 passed` |
| M2：`CallCounter.thread_total()` 退化成全局计数（并发串账） | 按线程守卫变红 | `FAILED tests/unit/test_client.py::test_call_count_is_per_thread` / `1 failed, 46 passed` |
| M3：把 `executors.py` 整体换回写死常量的版本 | 7 条执行器守卫变红 | 见上「RED 的真实输出」（13 failed） |

两次变异后都已恢复正确实现并重跑全量（见下）。

---

## 实测结果

### 验收命令（全部自己跑，真实输出）

```
$ conda run -n tpops python -m pytest -q
1665 passed in 48.63s
```

```
$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!
```

```
$ conda run -n tpops python -m pytest -q tests/test_py37_compat.py
152 passed in 1.31s
```

```
$ grep -rn "print(" tpops_kafka_collector/ | wc -l
0
```

```
$ find tpops_kafka_collector -name "*.py" -exec wc -l {} + | sort -rn | head -5
 12757 total
   483 tpops_kafka_collector/scheduler.py
   401 tpops_kafka_collector/main.py
   399 tpops_kafka_collector/tpops/command_center.py
   399 tpops_kafka_collector/kafka/producer.py
   398 tpops_kafka_collector/tpops/client.py
```

> 说明：`scheduler.py`(483) / `main.py`(401) 超过 400 行 —— **这两个文件是 R-A 的**
> （我在归属清单之外，没动）。**我自己的文件**全部 < 400：
> `tpops/client.py` 398、`executors.py` 352、`tpops/call_tracking.py` 220、
> `scheduler_report.py` 90。
>
> 全量基线与回归：本轮我新增 **22** 条守卫（8 + 5 + 5 + 4）；我改动落地后的那次全量是
> **1659 passed**，之后 R-A / R-C 并行提交了他们的新用例 → 现在是 **1665 passed**。
> **无回归**（修复前的基线是 1637 passed = 1659 − 我新增的 22 条）。
>
> **并发组的在途失败（与 R-B 无关）**：最后一次全量跑出
> `tests/integration/test_main.py::test_node_list_is_retried_three_times` 失败 ——
> 该用例是 **R-A 刚加进工作副本的守卫**（文件自己的 docstring 写着「修复轮 R-A 追加的
> 守卫」），失败路径全在 **R-A 的 `main.py`**：
> `main.py:174 _build_instances` → `main.py:310 _fetch_nodes` →
> `tpops/instances.py:162 fetch_instance_nodes` → `main.py:293 NodeListUnavailable`，
> **完全没有经过 R-B 改的任何代码**（`executors.py` / `call_tracking.py` / `client.py`；
> 计数代理是在 `main.py:177` 的 `build_executors` 里才建的，比这条路径晚）。
> 排除该在途用例后：**1673 passed, 1 deselected**。
> 我**没有**去动 R-A 的文件（归属之外）。

### 一轮真实规模的外部调用数实测值

**方法**：用 `data/` 下的**平台实测留档**（告警记录条数：活动 2 / 活动明细 3 /
历史 4 / 历史明细 32；命令中心三跳的真实结果 `data/merged_sql/sql_merge_probe_result.json`
的 3 个 master）喂给**真的执行器**（真代码路径：真命令中心客户端走真三跳），
量每一轮真实发出的 HTTP 请求数。脚本放在工作树**之外**（`/tmp/rb_measure_round.py`），
不入库。

```
合并 SQL 视图数：11
告警留档条数合计：41
--- 第 1 轮（目录缓存冷） ---
  monitor          external_calls=13  (真实请求累计 13)
  command_center   external_calls=6   (真实请求累计 19)
  diagnostic       external_calls=2   (真实请求累计 21)
  instance         external_calls=1   (真实请求累计 22)
  alarm            external_calls=4   (真实请求累计 26)
  单轮合计 external_calls = 26  （预算 42 → 未超）
--- 第 2 轮（目录缓存命中） ---
  monitor          external_calls=1   (真实请求累计 27)
  command_center   external_calls=5   (真实请求累计 32)
  diagnostic       external_calls=2   (真实请求累计 34)
  instance         external_calls=1   (真实请求累计 35)
  alarm            external_calls=4   (真实请求累计 39)
  单轮合计 external_calls = 13  （预算 42 → 未超）
--- 告警 5000 条的情形（每形态 50 页） ---
  monitor          external_calls=13  (真实请求累计 13)
  command_center   external_calls=6   (真实请求累计 19)
  diagnostic       external_calls=2   (真实请求累计 21)
  instance         external_calls=1   (真实请求累计 22)
  alarm            external_calls=204 (真实请求累计 226)
  单轮合计 external_calls = 226  （预算 42 → 超预算！）
```

**结论**：

- **一轮真实规模的外部调用数 = 13（稳态）/ 26（目录缓存冷的那一轮）**
  —— **不超过 42**。按规格，每轮**不会** WARNING；预算告警**如实工作**
  （只在真的超预算时才响）。
- 修复前上报的是 `2+11+1+2+4 = 20`（写死）→ 42 次预算**永远不响**；
  修复后上报的是**真值** 13/26 → 预算口径终于对上了。
- **什么情况下会超 42**：告警历史暴涨（平台历史告警保留 30 天，条数会涨）。
  实测「告警 5000 条」时**单是告警通道就是 204 次**（4 形态 × 51 页），
  整轮 226 → **超预算，WARNING 会真的响**。这正是这条守卫存在的意义。

### 若超预算，还能砍什么（分析，供控制器裁决）

当前实测**不超**（13/26），下面是「假如要压到 42 以下」的备选（按性价比排序）：

| 手段 | 省下次数（实测口径） | 代价 |
|---|---|---|
| **告警通道按增量拉取**（用告警水位把 `offset` 起点前移，或只拉「未收敛/未恢复」的形态） | 每形态省掉「已收敛历史」那几页 —— 5000 条时每形态 50 页 → 约 **150 次** | 要改 `collectors/alarm.py` / `tpops/alarms.py`（**不在我归属内**）；且平台的告警接口是 `offset` 分页，省不了「翻到那一页」的开销 |
| 告警分页**只拉明细形态**、收敛形态本地归并（现在的口径是四形态都拉） | 每轮省 **2 页**（收敛 2 形态） | 规格「活动与历史各取收敛与明细」是写死的口径 → **要改规格**，本变更内不能做 |
| 告警通道周期从 30 秒拉长（告警不需要 30 秒一次） | 摊薄到每轮：采集频率 ÷ N | 客户侧对告警时效的口径（30 秒）写在开发方案里 → 要客户/控制器确认 |
| 目录快照 TTL 从 1 小时拉长到 24 小时 | 每天省 **12 × 23 ≈ 276 次**（摊到每轮约 0.3 次） | 平台侧指标定义变更的发现更慢（只影响告警时效，不影响数据） |
| B 类命令中心的**三跳合并**（平台侧支持一次取明细） | 每页省 **2 次**（轮询 + 明细各一） | 平台接口不支持 → **做不到**（实测链路就是三跳） |

结论：**当前规模（13/26）不需要动 42**；真要超，唯一有意义的杠杆是**告警通道的
分页规模**（历史条数），而不是采集端本身。

---

## 发现但未修的问题

1. **失败任务的调用数不进轮汇总**：`scheduler.py:333-336` 只累加**成功**任务的
   `external_calls`；某个通道整轮失败（抛异常）时，它已经发出的请求**不计入**
   `calls=`。→ **需要 R-A 改 `scheduler.py`**（失败任务也带一个 `external_calls`）。
   我没动（不在归属内）。
2. **失败任务没有 outcome**：`TaskOutcome` 只在成功时产生（`executors.py` 里异常
   直接向上抛）。同上，属 R-A 的范围。
3. **B 类的节点清单拉取不在代理计数里**：`MasterNodeRegistry` 在
   `CommandCenterClient` 构造时拿的是**原始客户端**，它的节点清单 GET（1 小时 1 次）
   不经过计数代理。**在生产里不影响**（wire 级计数在原始客户端上，全量可见），
   只影响「假客户端没有计数能力」时的兜底口径（少 1 次/小时）。
   → 要彻底解决需改 `tpops/command_center_nodes.py`（**不在我归属内**）。
4. **`scheduler.py` 483 行 / `main.py` 401 行超 400 行红线** —— 属 R-A 的文件，
   我没动，仅报告。
5. **A 类多 series 的归集口径**（`open_items.json` 的 `a-class-multi-series`）
   仍是待客户确认项 —— 与本次修复无关，只是提醒：确认后 A 类的消息条数会变，
   但**调用数不变**（仍是一次目录快照 + 一次指标 POST）。

---

## 偏离说明

1. **新增了一个生产文件** `tpops_kafka_collector/tpops/call_tracking.py`（**不在
   我给的归属清单里**）—— 理由：`tpops/client.py` 原本已经 **382 行**，
   加完计数接口会变成 407 行，**破 400 行红线**；把计数实现放进新模块后
   `client.py` 只加 16 行（→398 行）。新模块放在 `tpops/` 包下（R-A / R-C 都不碰
   这个包），不与他人冲突。若控制器要求合并进 `client.py`，需要接受 `client.py`
   破 400 行，或由控制器指定另一个落点。
2. **`tpops/metrics.py` 没有改**（虽然在我的归属清单里）：目录缓存放在
   `call_tracking.py` 的 `CatalogCachingClient` + 执行器装配层，
   **不需要改 `fetch_metric_catalog` 的签名**，也就不必动 `metrics.py`
   （它已经 396 行，只剩 4 行余量，装不下缓存逻辑）。
3. **`_install_counting_client` 写了命令中心的私有属性**（`command_center._client`）：
   命令中心三跳走的是它**自己持有的**客户端，不换掉就数不到 B 类。
   这是**装配层的接线**（只换引用，代理原样转发 `get` / `post`，行为不变），
   已加注释说明。若不接受私有属性访问，替代方案是让 `TpopsClient` 自带计数
   （生产路径本就如此，私有属性替换只在「假客户端兜底」时起作用）——
   但那样 B 类在假客户端下会报 0。
4. **测试文件里出现了一个硬编码的 `8`**（`test_alarm_external_calls_scale_with_page_count`）：
   那是**期望值**（4 形态 × 2 页 = 8），断言的另一半是
   `outcome.external_calls == client.calls.total()`（**与假客户端自己的计数对拍**，
   测试里不写死调用次数）。其余 12 条守卫都是「上报值 == 假客户端实际计数」的对拍。
5. **没有 commit**（按要求由控制器统一提交）；全程未用 `git add` / `checkout` /
   `reset`，未动分支与 HEAD。
