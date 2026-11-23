# 任务 1.9 报告 —— 水位与差分基线持久化（W09）

> 工作目录：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（worktree）
> 波次：`wave-1-skeleton-platform`（revision 3）
> 未执行 `git commit`（按共享约定由控制器统一提交）。

## 1. 交付内容

### 新增文件

| 文件 | 行数 | 说明 |
|---|---|---|
| `tpops_kafka_collector/watermark.py` | 396 | 水位 / 差分基线 / 告警水位 / 幂等键 LRU 的内存态 + 原子落盘 |
| `tests/unit/test_watermark.py` | 393 | 26 个单元测试（四段内容往返 / 原子性 / LRU / 损坏与版本拒绝） |

### 公共 API

```python
DEFAULT_IDEMPOTENCY_LRU_CAPACITY = 10000   # LRU 容量（可配常量，非魔法数字）
WATERMARK_STATE_VERSION = 1                 # 水位文件格式版本

class WatermarkError(TpopsError)            # 不可重试（errors.is_retryable → False）

@dataclass(frozen=True) MetricWatermark      # stream + end_time_ms
@dataclass(frozen=True) AlarmWatermark       # stream + last_event_time（平台侧游标）
@dataclass(frozen=True) DeltaBaseline        # metric_code + dimension_key + counter_value + sampled_at_ms
@dataclass(frozen=True) WatermarkState       # 四段的不可变快照；to_payload() / from_payload()

class IdempotencyKeyLRU                      # 容量可配；add() 返回 False 表示重复
class WatermarkStore                         # load() / save() / snapshot() + 各类读写
```

`WatermarkStore` 主要方法：`metric_watermark` / `advance_metric`（只前进不回退）、
`alarm_watermark` / `advance_alarm`、`get_baseline` / `put_baseline` / `forget_baseline` /
`import_baselines_from(delta_store)` / `export_baselines_into(delta_store)`、
`remember_key` / `has_key` / `idempotency_keys`、`load()` / `save()` / `snapshot()`。

## 2. 关键设计

### 原子写（任务核心）

`_atomic_write_json(path, payload)`：

1. `tempfile.NamedTemporaryFile(dir=<目标目录>, delete=False, prefix='.watermark-', suffix='.tmp')`
   —— **与目标文件同目录**，否则 `os.replace` 跨设备会 `EXDEV`；
2. 写 JSON → `flush()` → `os.fsync(fd)`；
3. `os.replace(temp_path, path)` —— 原子替换；
4. `_fsync_directory(directory)` —— 目录项刷盘，保证崩溃后替换结果可见；
5. `finally` 兜底：任何异常路径都 `os.unlink(temp_path)`，**不留残留临时文件**。

落盘内容来自不可变快照 `WatermarkState`（frozen dataclass + 元组），写文件期间内存态不变。

### 差分基线（只做持久化，不重实现差分算法）

键形状与 wave 0 的 `tpops_kafka_collector/mapping/rules.py::CounterDeltaStore` 完全一致：
`{(metric_code, dimension_key): (counter_value, sampled_at_ms)}`。
`import_baselines_from` / `export_baselines_into` 采用**整体替换**语义（避免两份真相源漂移），
容器按 `_baselines` → `baselines` 两个属性名依次探测（1.8 重写规则引擎时若改属性名也不会炸，
改了就抛 `WatermarkError`，不会静默丢基线）。

### 幂等键 LRU

- 键本身**不由本模块计算** —— 真相源是 wave 0 的 `tpops_kafka_collector/ids.py`
  （`alarm_idempotency_key` / `metric_idempotency_key`）；本模块只维护「已投递键」的 LRU 集合。
- 容量上限是模块常量 `DEFAULT_IDEMPOTENCY_LRU_CAPACITY = 10000`，构造时可覆盖。
- 按**插入顺序**淘汰（命中已有键不刷新顺序、不重复入队）。

## 3. TDD 证据

### RED（实现为脚手架，公开 API 抛 `NotImplementedError`）

```
$ conda run -n tpops python -m pytest tests/unit/test_watermark.py -q
...
FAILED tests/unit/test_watermark.py::test_failed_write_leaves_previous_state_file_intact
FAILED tests/unit/test_watermark.py::test_failed_write_leaves_no_temporary_file_behind
FAILED tests/unit/test_watermark.py::test_successful_write_leaves_only_the_state_file
FAILED tests/unit/test_watermark.py::test_state_file_always_contains_complete_json
FAILED tests/unit/test_watermark.py::test_stale_temporary_file_is_ignored_on_load
FAILED tests/unit/test_watermark.py::test_snapshot_is_immutable - NotImplemen...
FAILED tests/unit/test_watermark.py::test_state_payload_roundtrip_is_stable
FAILED tests/unit/test_watermark.py::test_idempotency_keys_from_ids_module_dedupe_after_restart
FAILED tests/unit/test_watermark.py::test_metric_idempotency_key_is_stable_across_dimension_order
FAILED tests/unit/test_watermark.py::test_real_counter_delta_store_baseline_keys_are_compatible
26 failed in 0.68s
```

RED 时的失败断言（全部为 `NotImplementedError`，即「能力尚未实现」）：

- `test_save_then_load_restores_all_four_sections` — `WatermarkStore(path)` 构造即
  `raise NotImplementedError`（断言 `restored.metric_watermark('instance-a') == 1700000000000` 未执行到）；
- `test_failed_write_leaves_previous_state_file_intact` — 断言
  `path.read_text() == original` 与 `json.loads(...)['metric_watermarks'][0]['end_time_ms'] == 1000`；
- `test_failed_write_leaves_no_temporary_file_behind` — 断言
  `sorted(os.listdir(dir)) == ['watermark.json']`（无临时文件残留）；
- `test_idempotency_lru_evicts_oldest_inserted_when_over_capacity` — 断言
  `lru.contains('a') is False` / `lru.contains('b') is True`；
- `test_real_counter_delta_store_baseline_keys_are_compatible` — 断言
  `target._baselines == delta._baselines`（真实 `CounterDeltaStore` 的键形状）。

### GREEN

```
$ conda run -n tpops python -m pytest tests/unit/test_watermark.py -q
..........................                                               [100%]
26 passed in 0.32s

$ ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest -q --ignore=tests/unit/test_serializer.py
480 passed in 11.42s
```

（worktree 全量基线 453 passed → 加本任务 26 个用例 = 479/480；
`tests/unit/test_serializer.py` 是并发的 1.4 任务在写中的文件，
其 `tpops_kafka_collector/kafka/serializer.py` 尚未落地导致 collect 报错 —— 与本任务无关。）

### 原子性测试是真验证，不是走过场

`test_failed_write_leaves_previous_state_file_intact` 通过 monkeypatch 注入
`_dump_payload` 失败（先写入半截 JSON 再抛 `OSError`）：

- 目标文件内容与写前**逐字节相同**；
- 目标文件仍能 `json.loads` 出上一轮的 `end_time_ms == 1000`；
- `test_failed_write_leaves_no_temporary_file_behind` 断言目录里只剩
  `watermark.json`（临时文件已清理）；
- `test_stale_temporary_file_is_ignored_on_load` 断言遗留的 `.watermark-*.tmp`
  不会影响 `load()`（只读目标文件）。

## 4. 偏离简报之处及原因

| 偏离 | 原因 |
|---|---|
| 简报提到 `tpops_kafka_collector/ids.py` 是幂等键真相源 —— 已按此实现（键不由本模块计算） | 无偏离；但**键的计算**留在 `ids.py`，本模块只存「已投递键」的 LRU 集合 |
| 简报说 `mapping/rules.py` 的 `CounterDeltaStore` 是内存态差分基线 —— 已按其键形状落盘 | 无偏离；本模块只做存盘/恢复，差分算法仍归 1.8 的规则引擎 |
| 简报未指定水位文件路径来源 | `WatermarkStore` 构造参数 `path` 由调用方给；本任务不新增环境变量（§5.5 配置表未列该变量），路径口径留给 3.1/3.3 的装配任务裁决 |
| 幂等键 LRU 的淘汰语义 | spec 写「LRU」，简报写「按插入顺序淘汰」；本实现按**简报**执行（命中不刷新顺序 → 淘汰顺序 = 首次出现顺序） |
| `WatermarkError` 继承 `errors.TpopsError` | wave 0 已建异常体系；继承后 `is_retryable()` 返回 `False`（水位文件损坏不可重试），符合 §5.7 判定表 |

## 5. 遗留问题 / 需要控制器知道

1. ⚠️ **1.8 重写 `mapping/rules.py` 时不要改基线容器形状**：本模块按
   `_baselines` → `baselines` 探测字典容器。若 1.8 把 `CounterDeltaStore` 拆到
   `mapping/rules/` 子包或改了属性名，`import_baselines_from` /
   `export_baselines_into` 会抛 `WatermarkError`（**不会静默丢基线**，但会报错）。
   请 1.8 要么保留 `{(metric_code, dimension_key): (counter_value, sampled_at_ms)}`
   形状，要么在 `CounterDeltaStore` 上补一个公开的 `baselines` 属性。
   回归用例 `test_real_counter_delta_store_baseline_keys_are_compatible` 会守住这条。
2. **水位文件路径未定义**：调度器（3.1）/ 入口（3.3）需要决定路径与「写盘时机」
   （每轮成功投递后写一次即可）。本模块不写日志里的凭据、不做目录创建
   （目录不存在直接抛 `WatermarkError`，不做隐式 `mkdir`）。
3. **告警水位值是平台侧游标原样保存**（字符串）。若 2.9 的告警采集器改用
   毫秒时间戳，改 `AlarmWatermark.last_event_time` 的类型即可，不影响文件结构。
4. **幂等键 LRU 落盘体积**：默认 10000 条键会显著增大水位文件（每条几十字节量级）。
   当前实现照 spec 的 10000 默认值落盘；若运维反馈文件过大，可在 3.1 调小构造参数
   （容量是构造参数，不是写死的常数）。
5. 单实例假设：同一时刻只有一个进程写该文件（开发方案 §5.6.4）。
