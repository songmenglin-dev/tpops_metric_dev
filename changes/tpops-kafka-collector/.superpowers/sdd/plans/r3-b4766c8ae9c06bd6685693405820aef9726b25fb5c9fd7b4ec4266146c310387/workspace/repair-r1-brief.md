# wave-2b 修复轮次 1 简报 —— 三条 Critical + 四条 Important

> **审查报告**：`.superpowers/sdd/reviews/wave-2b-channels.md`（verdict = **fail**）
> **receipt**：已签发 `fail`，wave-3 保持未解锁。
> 本轮修完后需**定向复审**并重新签发 receipt。

## 必读

- 审查报告全文（592 行）：`changes/tpops-kafka-collector/.superpowers/sdd/reviews/wave-2b-channels.md`
- 共享约定：`workspace/wave3-execution-brief.md`（硬约束同样适用）
- 开发方案 `docs/开发方案-GaussDB指标与告警采集.md`

## 工作目录

```
cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev && <命令>
```

## 三条 Critical —— 必须修

### C-1 E 类采集器用生产映射对象必崩（全部告警不产出）

`collectors/alarm.py:224/236` 调 `mapping.alarm_definition()` /
`mapping.metric_rule()`，但这两个方法**只在 `mapping/table.py::MappingTable` 上**，
生产对象 `load_mapping()` 返回的 `LoadedMapping` **没有这两个方法**。

```python
# 实测复现
AttributeError: 'LoadedMapping' object has no attribute 'alarm_definition'
```

**为什么测试没抓住**：集成测试用 `MappingTable(...)` **手工拼 fixture**，
而生产装配走 `load_mapping()` → `LoadedMapping`。

**修法（二选一，推荐前者）**：

1. **在 `LoadedMapping` 上补这两个方法** —— 让生产对象具备同样的查表能力。
   `LoadedMapping` 已经有 `metric_rules` 元组与 `alarm_rules` 字典，
   补两个查表方法即可（`metric_rule(code)` 返回元组里那条或 `None`；
   `alarm_definition(alarm_id)` 从 `alarm_rules` 里找）。
   **为什么推荐这个**：查表能力本就该在**生产用的那个对象**上 ——
   否则每个调用点都要自己判断「我拿到的是哪种对象」。
2. 改 `collectors/alarm.py` 改成用 `mapping.metric_rules` 元组自己找。

**必须补的测试**：用 `load_mapping()`（**不是** `MappingTable`）构造采集器，
断言 E 类采集器能正常产出消息。**现有的 E 类测试的 fixture 要一并换掉** ——
它正是掩盖缺陷的原因。

### C-2 B 类 5 个 `counter_delta` 指标永不产出

`collectors/command_center.py::_fetch_pages` 调 `decode_merged(body, self._views)`
**漏传 `now_ms`** → `ctx.now_ms` 恒为 `0` → 差分判定「采集时刻未推进」→ 值恒 `None`。

**冒烟枪**：`self._clock` 在 `__init__` 里赋值后**全文未用**。

**实测对照**（控制器已独立复核）：

| now_ms | 第一轮有值 | 第二轮有值 |
|---|---|---|
| 恒为 0（现状） | 0 条 | **0 条** |
| 递增（应有行为） | 0 条（首轮建基线） | **9 条** |

**修法**：`decode_merged(body, self._views, now_ms=self._now_ms())`。
注意：**同一轮内所有页必须用同一个 now_ms**，否则跨页的基线时间戳会不一致。

### C-3 差分基线跨 master 共享，互相污染

`self._deltas` 是**一个** `CounterDeltaStore`，被 3 个 master 共用；
而基线键是 `(metric_code, dimension_key)` —— **不含 node**。

**实测对照**（控制器已独立复核）：

| store 归属 | master_0 | master_1 | master_2 |
|---|---|---|---|
| 共享一个（现状） | 9 条 | **0 条** | **0 条** |
| 每 master 独立 | 9 条 | 9 条 | 9 条 |

**修法**：每个 master 用**自己的** `CounterDeltaStore`。
注意：`CounterDeltaStore` 是采集器内的运行时状态（不是持久化的差分基线 ——
持久化那部分在 `watermark.py` 的 `DeltaBaseline`，键里**已经含 node**）。
所以修法是：在 `_build_messages` 里按 `node_id` 分组取 store，
或者在 `_fetch_pages` 里按节点建 store。

**要确认**：基线**跨轮**怎么延续？如果每个 master 每轮新建 store，
差分就永远只有首轮建基线、次轮算不出速率。
→ **必须查清 `CounterDeltaStore` 的生命周期**：它是每轮新建（采集器对象每轮新建）
还是每进程一个。若是前者，需要把基线**持久化到 `watermark.py`** 或让采集器持有状态。

### 修复后必须自问的一句话

**这三条的共同形态是「不报错、但数据没有/数据错」。**
每修一条，都要问：**有没有一个测试能让它失败？**
没有守卫测试的修复不算修完。

## 四条 Important

### I-1 `collectors/command_center.py` 463 行 > 合同 400 行

拆法（审查报告给了切点）：
- 第 348 行起的**模块级辅助函数**挪到新模块 `collectors/command_center_helpers.py`：
  `_command_center_rules` / `_merged_views` / `_has_more` / `_approx_size_bytes` /
  `_host_node_id` / `_page_signature` / `_same_as_previous` / `_is_output_limit_error` /
  `_tier_of`
- 顺带修两个超长函数：`_fetch_pages`（64 行）、`_build_messages`（59 行）→ 各拆成两段

> 控制器在派发审查前**独立自查**发现了同一处（账本已记），
> 拆法一致 —— 审查报告与控制器自查结论相互印证。

### I-2 值相等守卫只覆盖 47/82

`tests/integration/test_collector_command_center.py::test_collected_values_equal_the_expected_table`
里，`continue` 分支**静默跳过**了 35 个维度展开型指标码
（模板码 vs 替换后码键不匹配）。

**控制器的独立复算**（与审查者同向）：
- 期望值表里的 (指标,节点) 对数 = **246**
- 断言实际比对 = **113**
- **33 个指标码从未被比到**（审查者算出 35 —— 差异来自统计口径，结论一致）

**修法**：
1. 断言要按**替换后的 metricCode** 去匹配 `produced`（模板码 → 替换码）
2. `checked` 的断言从 `> 0` 改成**精确期望值**（如 `== 246`），
   让「比少了」直接失败
3. **列出并逐条处理**那 35 个指标 —— 它们不是「比不到」，
   而是**采集器根本没为它们产出消息**（见下方 I-4）

### I-3 `_master_nodes` 静默吞异常

`collectors/command_center.py::_master_nodes` 的 `except Exception: return ()`
—— 静默吞掉异常且**不记日志**。违反「不静默吞异常」。
**修法**：记 ERROR 日志（带上下文），再返回空。

### I-4 合同完成标准「98 个可提供指标全部产出数据」不成立

回归层（225 passed）与 `tests/test_expected_values.py`（82/82）
**只测映射层**（`decode_merged` + `evaluate`），**完全没有走到采集器**。

**实测**：采集器层 1700 条消息中 `*_records_per_second` **0 条**；
55 个 table/database/port 粒度的指标**一条都没产出**（只有 instance 粒度产出）。

**修法**：补**采集器级**的回归用例（T-16）——
每个通道都要有一条「采集器产出的消息里，98 个可提供指标全部有值」的断言。

> ⚠️ 审查者报告的 §5 说「35 个指标码被 continue 跳过」，
> 而控制器的独立排查发现的是**两个叠加的问题**：
> ① 断言的键不匹配（跳过）② 采集器对 table/database/port 粒度**根本不产出**。
> 修 I-2 时必须查清是哪个原因 —— **很可能两个都有**。

## 建议一并处理的 Minor（不阻塞，但都在本波次范围内）

| 编号 | 内容 | 处置 |
|---|---|---|
| M-2 | 告警分页 `total_count` 为 0/缺失时只取第一页 | 改成 `if page.total_count > 0 and len(collected) >= page.total_count: break`，保留「不满一页即停」兜底 |
| M-3 | `node_scoped=false` 的兜底路径：值来自别的 master、资源名仍写第一个 master | 兜底时同步改资源上下文，或该轮不产出该类消息 |
| M-4 | `CMD_PAGE_SIZE` 没接到采集器 | 采集器加 `from_config`，wave 3 装配时传 |
| M-5 | 超限缩小页容量后回报的 `page_size` 仍是缩小前的值 | 用更新后的值回报 |
| M-10 | 「采集器不推进水位」的断言不可证伪 | 改成行为断言，并给 B/C/D/E 补同类断言 |

## 硬约束（不变）

- Python 3.7.9 红线
- 不新增依赖
- 单文件 < 400 行、函数 < 50 行（**本轮修 I-1 时尤其注意别拆出新的超长文件**）
- 不可变数据
- 凭据只走环境变量
- 错误显式处理：用 `logging`，不静默吞异常
- **不改** `execution-contract.md` 与 `tasks.md`（改了会让执行计划 stale → 前四个波次的 receipt 全部作废）
- **不改** `data/mapping/*.json` 的内容

## 提交纪律

**不要 `git commit`**（由控制器统一提交，避免 git index 锁竞争）。只写文件 + 跑测试。

## 报告

写到 `.../workspace/repair-r1-report.md`（**绝对路径**），含：
- 每条 Critical/Important 的**修法**与**守卫测试名**
- RED→GREEN 证据（先让新测试红，再修绿）
- 三条 Critical 的**修复前后对照数字**（如 C-2 的两轮有值条数）
- 未修的条���及理由

## 验收判据

- `pytest -q` 全绿
- `ruff check tpops_kafka_collector/` 为 0
- 三条 Critical 各有**能让它失败**的守卫测试
- `collectors/command_center.py` < 400 行，所有函数 < 50 行
- 值相等守卫覆盖**全部** 82 条指标（`checked` 断言精确值）
