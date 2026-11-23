# wave-2b 任务 2.7 · C 类（诊断优化）—— 实施方报告（控制器代写）

> **原实施方被 Token Plan 配额上限（429）打断**，代码落盘但报告没写出来。
> 由控制器接管：补写报告、修正 2 条放错层的测试、补交遗漏文件。

## 交付

| 文件 | 说明 | 行数 |
|---|---|---|
| `tpops_kafka_collector/tpops/diagnostic.py` | C 类平台层（两个接口的路径、请求体、解析） | 252 |
| `tpops_kafka_collector/collectors/diagnostic.py` | C 类采集器（4 个客户指标） | 264 |
| `tests/integration/test_collector_diagnostic.py` | 23 例 | — |

## 4 个客户指标

| 指标 | 取值口径 | 规则类型 |
|---|---|---|
| `GAUSSDB.Total_slow_queries` | 慢 SQL 集合条数 | `collection_count` |
| `GAUSSDB.Select_slow_queries` | 按 `sql_text` 前缀分类计数 | `collection_count` |
| `GAUSSDB.DML_slow_queries` | 按 `sql_text` 前缀分类计数 | `collection_count` |
| `gaussdb.Top10sql` | 按 `total_elapse_time` 取前 10 条 | `top_n` |

**口径全部来自映射表**（`data/mapping/metric_rules.json`）—— 前缀清单
（`select` / `with` / `insert` / `update` / `delete`）、集合名、排序列、条数上限
都不写死在代码里。

## 硬约束（都有测试守卫）

1. **第二套命名空间** —— 两个接口都在 `/rds/gaussdb/v3/` 下，路径前缀写进常量
   （`SLOW_SQL_PATH` / `TOP_SQL_PATH`），少写一段 `gaussdb` 平台返回 404。
2. **返回的是当前快照** —— 没有自己的时间戳，客户字段 `clock` 取采集完成时刻
   （采集器传给规则引擎的 `now_ms`）。
3. **脱敏在采集器层接入** —— 2.10 的脱敏模块此前**没有接入链路**，
   本任务把 `_sanitize_rows` 接上：业务 SQL 文本逐行脱敏后再进规则引擎。
4. **异常消息里不带 SQL 文本** —— 脱敏失败包装成 `MappingError`，
   消息只带**字段名**，不带业务 SQL。

## 控制器修正的两条「放错层的测试」

原实施方把两条断言写在了集成测试里，但它们约束的是**平台层的请求体形状**，
不属于「采集器 + 假 HTTP」的测试范围：

- `slow-sql-list` 请求体必须含 `threshold` 与 `threshold_unit`
- `top-sql-list` 请求体必须含 `start_time` / `end_time`

改为在平台层直接断言请求体（不发 HTTP），集成测试只保留采集器语义。

## 验证

- `pytest tests/integration/test_collector_diagnostic.py -q` → **23 passed**
- `ruff check tpops_kafka_collector/` → 0
- 全量（补交后）→ **1262 passed**
