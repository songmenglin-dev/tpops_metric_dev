# 任务 2.3 报告 —— A 类：窗口计算与时间戳切片

- **任务**：wave-2b / 2.3（`changes/tpops-kafka-collector/tasks.md`）
- **实施方**：spec-superflow 变更 `tpops-kafka-collector` 的 wave-2b 实施分支
- **工作目录**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（worktree）
- **未提交**：按简报要求**没有 `git commit`**，只落文件 + 跑测试。

---

## 1. 新增文件清单

| 文件 | 行数 | 说明 |
|---|---|---|
| `/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev/tpops_kafka_collector/tpops/metrics.py` | 396 | A 类通道的窗口计算、请求体构造、时间戳切片、分组目录 |
| `/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev/tests/unit/test_metrics_window.py` | 396 | 单元测试 28 条 |

**没有改动任何既有文件**（`tpops_kafka_collector/` 下已有文件、`data/`、`changes/` 均未动）。

模块公开 API：

```
METRIC_GROUPS            # 12 个分组码（元组）
MIN_WINDOW_HOURS = 1     # 窗口下限（小时）
IS_LATEST_POINT_ONLY = 'false'   # 字符串常量
METRICS_PATH / GROUP_CATALOG_PATH

MetricsError(TpopsError) / MetricWindowError(MetricsError)
QueryWindow(start_time, end_time)      # frozen dataclass，毫秒
SeriesPoint(metric_name, metric_display_name, series_key,
            timestamp, value, collection_frequency)   # frozen dataclass

resolve_window(now_ms, window_hours, end_lag_minutes) -> QueryWindow
resolve_window_from_config(now_ms, config)            -> QueryWindow
build_query_body(window, instance_id, metric_names, node_ids=None) -> dict
metric_tier_seconds(metric, default=60) -> int
parse_series_points(metric)      -> Tuple[SeriesPoint, ...]
parse_metric_points(payload)     -> Tuple[SeriesPoint, ...]
slice_points(points, tier_seconds, now_ms, tolerance_ms=0) -> Tuple[...]
slice_points_by_tier(points, now_ms, tolerance_ms=0)      -> Tuple[...]
fetch_metric_catalog(client, instance_id) -> Dict[str, Tuple[dict, ...]]
utc_now_ms() -> int
```

---

## 2. 12 个分组码：在哪定义、怎么保证不漏

**定义位置**：模块级常量 `METRIC_GROUPS`
（`tpops_kafka_collector/tpops/metrics.py:57` 附近），全模块**唯一**来源：

```python
METRIC_GROUPS: Tuple[str, ...] = (
    'CPUMEMORY', 'CONNECTION', 'SYNCSTAT', 'PROCESSRESOURCE',
    'NETWORK', 'TRANSACTION', 'IOSTORAGE', 'LOCK',
    'INSTATUS', 'FORDR', 'AGENTSTATUS', 'COREMETRICS',
)
```

**不漏的保证（两层）**：

1. **测试断言集合相等**（`test_metric_groups_contains_the_twelve_expected_codes`）：
   `set(METRIC_GROUPS) == set(EXPECTED_GROUPS)` 且 `len(METRIC_GROUPS) == 12`
   —— 少一个、多一个、拼错一个都会红。
2. **测试断言不重复**（`test_metric_groups_has_no_duplicate`）：
   `len(set(METRIC_GROUPS)) == len(METRIC_GROUPS)`。
3. **测试断言实际请求覆盖全部分组码**
   （`test_fetch_metric_catalog_requests_every_group`）：
   断言 `fetch_metric_catalog` 发出的 12 次 `GET .../monitor/metrics/series`
   的 `name` 参数序列**恰好等于** 12 个期望分组码、顺序一致 —— 防止「常量写对了但
   拉取时漏发/写错」。
4. **运行期兜底**：分组码写错时平台返回 **HTTP 200 + 空 series 且不报错**，
   所以 `fetch_metric_catalog` 对空分组记 **WARNING** 日志
   （`监控指标空分组告警：分组码 %s 未返回任何指标（平台对错误分组码返回 200 + 空 series，不报错）`），
   由 `test_fetch_metric_catalog_warns_on_empty_group` 断言
   （断言日志同时包含分组码与「空分组」字样）。

---

## 3. `is_latest_point_only` 的类型断言怎么写的

**常量层面**（模块级）：

```python
IS_LATEST_POINT_ONLY: str = 'false'
```

`build_query_body` 把它原样放进请求体（不经过 `str()`/`bool()` 转换）。

**测试层面**（`tests/unit/test_metrics_window.py`）：

```python
def test_is_latest_point_only_is_the_string_false():
    assert IS_LATEST_POINT_ONLY == 'false'
    assert isinstance(IS_LATEST_POINT_ONLY, str)
    assert not isinstance(IS_LATEST_POINT_ONLY, bool)   # bool 是 str 的子类，必须单独排除

def test_build_query_body_uses_string_false_not_boolean():
    body = build_query_body(window, 'inst-1', ['rds092_gaussv5_commit_counter'])
    flag = body['is_latest_point_only']
    assert isinstance(flag, str), 'is_latest_point_only 必须是字符串，传布尔会丢时间戳'
    assert not isinstance(flag, bool)
    assert flag == 'false'
```

关键点：Python 里 `isinstance(True, int)` 为真、`bool` 是 `int` 的子类，
所以「`isinstance(x, str)`」还不足以排除布尔 —— 这里**显式再断言一次
`not isinstance(flag, bool)`**，确保传出去的是字符串 `'false'` 而不是布尔 `False`。

---

## 4. 窗口下限与 `end_time` 回退量怎么处理的

**下限**：`MIN_WINDOW_HOURS = 1`（模块级常量）。

- `resolve_window(now_ms, window_hours, end_lag_minutes)`：
  `window_hours < 1` → **抛 `MetricWindowError`**（不静默截断）。
  同时拒绝负回退量、非数字入参。
- `resolve_window_from_config(now_ms, config)`：直接取
  `config.metric_window_hours` 与 `config.metric_end_lag_minutes`
  （即「窗口下限用 `config.metric_window_hours`，不得小于 1」），
  同样走 `resolve_window` 的校验。
- 异常继承链：`MetricWindowError` → `MetricsError` → `TpopsError`。
  `errors.is_retryable` 对 `TpopsError` 基类**不判可重试**（不在
  `NON_RETRYABLE`/`RETRYABLE` 里，MRO 里也没有 `Timeout`/`ConnectionError`），
  所以配置类错误**不会被重试框架反复重试**。
- 双保险：`config.py` 的 `METRIC_WINDOW_HOURS` 已有 `minimum=1`；
  本模块再挡一层，防止 `Config` 被程序化构造（不走 env 校验）时绕过。

**回退量**：`end_time = now_ms − end_lag_minutes × 60_000`；
`start_time = end_time − window_hours × 3_600_000`（即
「`end_time` = 当前时刻 − 5 分钟，`start_time` = `end_time` − 窗口长度」，
与规格 Scenario 完全一致）。默认 5 分钟由 `config.metric_end_lag_minutes` 提供
（`Config` 默认值即 5）。

**时间单位**：入参出参统一**毫秒**（13 位）；平台返回的浮点时间戳在
`parse_series_points` 里统一 `int(timestamp)`。

---

## 5. TDD：RED 的失败断言

**第一轮 RED**（先写测试，未实现模块）：

```
$ conda run -n tpops python -m pytest tests/unit/test_metrics_window.py -q
tests/unit/test_metrics_window.py:25: in <module>
    from tpops_kafka_collector.tpops.metrics import (
E   ModuleNotFoundError: No module named 'tpops_kafka_collector.tpops.metrics'
!!! Interrupted: 1 error during collection (0.59s)
```

**第二轮 RED**（模块骨架就位后跑测试，暴露一个真实的**断言写错**）：

```
FAILED tests/unit/test_metrics_window.py::test_fetch_metric_catalog_returns_metrics_per_group
E       AssertionError: assert 'sys010_mem_usage' in ({'alarm_id': None, 'alarm_threshold': None,
E                                                        'name': 'sys010_mem_usage', 'unit': '%'},)
```

—— 这是**测试自身的写法问题**（`catalog[group]` 是「指标定义元组」，
不是「名字集合」）。按 TDD 的规矩**改测试不改实现**（实现行为是对的）：
断言改为 `[item['name'] for item in catalog['CPUMEMORY']] == ['sys010_mem_usage']`。

---

## 6. GREEN 的测试输出摘要

```
$ conda run -n tpops python -m pytest tests/unit/test_metrics_window.py -q
............................                                             [100%]
28 passed in 0.35s
```

全量（排除**别人正在写**的 `tests/unit/test_masking.py`，其模块尚未落地导致收集失败）：

```
$ conda run -n tpops python -m pytest -q --ignore=tests/unit/test_masking.py
810 passed in 12.28s
```

3.7 语法门禁：

```
$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
84 passed in 0.93s
```

Lint（**我的两个文件**）：

```
$ ruff check tpops_kafka_collector/tpops/metrics.py tests/unit/test_metrics_window.py
All checks passed!
```

约束自查：`metrics.py` 396 行（< 400）；所有函数体 ≤ 40 行（< 50）；
Python 3.7.9 语法（无海象 / 无内建泛型注解 / 无 `__future__` 冗余导入）；
不可变数据（`QueryWindow` / `SeriesPoint` 均为 frozen dataclass）；
`logging` 不用 `print`；无新增依赖；无凭据处理。

---

## 7. 偏离简报之处及原因

1. **额外提供了 `fetch_metric_catalog`（分组目录查询 + 空分组 WARNING）**。
   简报只要求「12 个分组码写成模块级常量」，但常量若无人使用就只是一串死字符串；
   而「空分组必须记告警日志」是硬约束（写错分组码平台不报错）。把目录拉取放在
   `metrics.py`（平台接口访问层）而不是采集器里，符合分层：**采集器不直接调平台**。
   任务 2.4（`collectors/monitor.py`）可直接复用本函数，也可以自行实现 —— 两者不冲突。
2. **`resolve_window` 接受 `float` 小时数**（如 0.5）而不是只接受 `int`：
   便于测试「窗口不足 1 小时」这条边界；`config` 侧仍是 `int`（`minimum=1`）。
3. **切片上界取 `now_ms`**：平台不应返回未来时间戳，若出现（时钟漂移）会被丢弃，
   避免把未来点当成「最新点」投给客户。可用 `tolerance_ms` 参数放宽。
4. **本模块不发请求**：`build_query_body` 只构造请求体，实际 `POST` 由采集器
   （任务 2.4）经 `TpopsClient` 发出 —— 符合「采集器只负责取数→解码→规则→组装→交投递」
   的分层与依赖注入要求（时钟与实例 ID 均为入参）。
5. **异常放在 `metrics.py` 内**（`MetricsError` / `MetricWindowError`）而没有加到
   `tpops_kafka_collector/errors.py`：简报明确「不改既有文件，除非简报说了」；
   新异常继承 `TpopsError`，因此仍被 `errors.is_retryable` 正确判定为**不可重试**。

---

## 8. 遗留问题

1. **`ruff check tpops_kafka_collector/` 目前仍非 0**，但**全部不在我的文件里**：
   `tpops_kafka_collector/mapping/sqlgen.py`（任务 2.2）与
   `tpops_kafka_collector/mapping/masking.py`（任务 2.10）在并行开发中，
   尚未收敛。同理 `tests/unit/test_masking.py` 存在但被测模块未落地，
   导致全量 pytest 收集失败 —— 这两项由对应任务的实施方收尾。
2. **多 series → 一个客户指标的归集口径仍未定**（`open_items.json` 的
   `a-class-multi-series`）：本模块**只做逐组件切片**，不做求和 / 取最早 /
   按 DN 展开 —— 口径确认前不写死在代码里（与开发方案 §4.2.4 一致）。
3. **端点容差 `tolerance_ms` 目前默认 0**：实测的采集边界抖动量级尚未采到样本，
   先按 0 实现并留出参数；接入真实数据后可按实际抖动调参。
4. **档位取自 `collection_frequency`**（秒）；平台正常都会给，缺失时兜底 60 秒。
   若客户指标库的档位口径与平台 `collection_frequency` 有出入（例如平台给的是
   采样间隔而客户要的是采集周期），需在任务 2.4 / 3.1 对齐 —— 本模块已把档位
   作为参数暴露，改口径不用动切片逻辑。
5. **未与真实平台联调**：本任务范围只有窗口计算与切片，真实响应结构以
   `data/metric_response/` 的 98 份留档与开发方案 §4.2.4 为准；
   回归回放属任务 3.4。
