# wave-3 任务 3.4 · 回归夹具与期望值表 —— 实施方报告（控制器代写）

> **原实施方被 Token Plan 配额上限（429）打断**，代码落盘但报告没写出来。
> 由控制器接管：补写报告、修复夹具与生成器的不一致、修新增文件的 lint。

## 交付

| 文件 | 说明 | 用例数 |
|---|---|---|
| `tools/expected_deriver_non_b.py` | 独立期望值推导器（A/C/D 16 条） | — |
| `tools/build_regression_fixtures.py` | 夹具生成器（带 `--check` 幂等守卫） | — |
| `tests/fixtures/metric_response/` | 99 份逐指标原生留档冻结样本 | — |
| `tests/fixtures/merged_sql/` | 合并形态探针结果 | — |
| `tests/fixtures/expected_values.json` | B 类 82 条期望值表 | — |
| `tests/fixtures/expected_values_non_b.json` | A/C/D 16 条期望值表 | — |
| `tests/regression/test_b_class_regression.py` | B 类回放 | 95 |
| `tests/regression/test_non_b_regression.py` | A/C/D 回放 | 18 |
| `tests/regression/test_fixture_integrity.py` | 夹具完整性 | 112 |

## 核心变化：判据从「非零」变成「等于期望值」

此前回归只能判「值不是 0」，无法发现「**算出了值但算错了**」这类缺陷 ——
比如单位换算错、差分基线用错、sum 与 avg 搞反。现在期望值由**独立推导器**
从实测留档的**原始行**现算（**不经规则引擎** —— 引擎是被测对象），逐条断言相等。

- B 类 82 条：期望值来自 wave-0 的生成器 `tools/build_expected_values.py`，
  与 `data/mapping/expected_values.json` 逐字节相同
- A/C/D 16 条：新增 `tools/expected_deriver_non_b.py` 独立推导

## 控制器修的两处

### 1. 夹具与生成器不一致（测试红）

`tests/regression/test_fixture_integrity.py::test_non_b_expected_values_fixture_matches_the_generator`
失败：夹具里的 `now_ms` 是 `1760000000000`（2025-10-09），生成器用的是
`1794500000000`（2026-11-12，与留档抓取日一致）。

查清后判定：**生成器是对的，夹具是旧的** —— `now_ms` 会影响 `gauge` 类规则的
期望值（运行时长 = 采集完成时刻 − 启动时刻），必须用留档抓取之后的时刻，
否则算出的运行时长是负数。按生成器重建夹具后两者自洽，
重跑生成器 `git diff` 为空（幂等）。

### 2. 新增文件的 lint

新增的 `tests/regression/` 5 个文件有 12 条 ruff 错（UP009 编码声明 / UP031
printf 格式）。本变更**已提交**的新测试文件一律不用编码声明、不用 %-format，
照此统一后 `ruff check tests/regression/` 为 0。

## 夹具完整性守卫（112 例）

不只是「文件在不在」，而是**逐字节**：

- 99 份留档与 `data/metric_response/` 逐字节相同（改了样本即被抓）
- manifest 索引每一份留档、无多出无缺少
- 合并形态探针逐字节相同
- B 类期望值表与 `data/mapping/` 那张分叉即被抓
- **夹具里出现凭据即被抓**（`FORBIDDEN_PATTERNS` 扫全文）
- 每份留档里的 `X-Mini-Token` 必须是占位符
- 夹具里的规则声明与生产用的那一份是同一份（防止改了生产规则、忘了改夹具）

## 验证

- `pytest tests/regression -q` → **225 passed**
- `python tools/build_regression_fixtures.py --check` → 「夹具是最新的」
- 全量 → **1262 passed**；`ruff check tpops_kafka_collector/` → 0
