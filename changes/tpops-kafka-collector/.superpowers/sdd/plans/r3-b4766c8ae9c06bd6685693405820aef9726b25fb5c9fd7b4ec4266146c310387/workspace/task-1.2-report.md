# 任务 1.2 报告 —— DTO 与异常体系

- **变更**：tpops-kafka-collector
- **波次**：wave-1（wave-1-skeleton-platform，revision 3）
- **工作目录**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（worktree，分支 `tpops_metric_dev`）
- **未提交**（按波次约定由控制器统一提交）

## 1. 交付物

| 文件 | 动作 | 行数 |
|---|---|---|
| `tpops_kafka_collector/models.py` | **新建** —— `MetricPoint` / `AlarmDetail` / `AlarmRecord` | 176 |
| `tpops_kafka_collector/errors.py` | **扩展**（保留 wave 0 的全部内容，只加 `DecodeError` 与判定表补行） | 95 |
| `tpops_kafka_collector/mapping/decode.py` | **小改**（3 行）：`DecodeError` 改为从 `..errors` import，不再本地定义 | — |
| `tests/unit/test_models.py` | **新建** —— 32 个用例 | 256 |

### models.py 要点

- 三个 DTO 全部是 **frozen dataclass**（`dataclasses.FrozenInstanceError` 可测）。
- `MetricPoint.dimensions` 在 `__post_init__` 里归一化成**按键排序的元组**
  `((键, 值), ...)`（接受 dict 或 `(键, 值)` 序列输入；值为 `None` 折成空串）。
  → `MetricPoint` 可哈希、能进集合、能当字典键（规格「维度字典的可哈希性」）。
  对外用 `dimension_map` 属性取 dict 视图（每次新建，改它不影响 DTO）。
- `AlarmRecord.details` 归一化成 `Tuple[AlarmDetail, ...]`。
- **幂等键不在 models 里另写一份常量**：`MetricPoint.idempotency_key` →
  `ids.metric_idempotency_key(instance_id, customer_metric, dimension_map, timestamp)`；
  `AlarmRecord.idempotency_key` → `ids.alarm_idempotency_key(alarm_id, instance_id,
  first_alarm_time, status)`（`status` 传**平台原值**）。真相源仍是 wave 0 的 `ids.py`。
  测试断言的是「DTO 上的键 == `ids` 模块算出来的键」，而不是把拼法抄第二遍。
- Python 3.7.9 兼容：只用 `typing.Dict/Optional/Tuple/Union/Iterable/Mapping/Any`，
  不用 `list[str]`、不用海象；`frozen=True` + `object.__setattr__` 做归一化。

### errors.py 要点（**扩展，非重建**）

- wave 0 已有的 `TpopsError` 与 `AuthError` / `TransientError` / `ProtocolError` /
  `MappingError` / `ProduceError` **原样保留**。
- 新增 `DecodeError(MappingError)`：异常体系里 `DecodeError` 的**唯一实现**。
- **没有动 `is_retryable` 的判定逻辑** —— 上一轮复审的 Important 修复（MRO 基类链判定）
  原样保留：`requests.exceptions.ReadTimeout` / `ConnectTimeout` 仍被判为**可重试**。
  只在判定表的注释里补了一行 `DecodeError`（继承 `MappingError` → 不可重试）。
- `mapping/decode.py` 里原本本地定义的 `class DecodeError(MappingError)` 改为
  `from ..errors import DecodeError` 并再导出 —— **同名不同类会让 `except` 抓不住**，
  所以必须收敛到一处。既有引用（`tests/test_response_rules.py`、
  `tests/test_merged_sql_mapping.py`、`mapping/__init__.py`）不受影响。

## 2. TDD

### RED（两轮）

**RED #1 —— `DecodeError` 尚未存在**：

```
ImportError: cannot import name 'DecodeError' from 'tpops_kafka_collector.errors'
→ ERROR tests/unit/test_models.py  (1 error in 0.60s)
```

**RED #2 —— 先放一份「naive 桩实现」（可变 dataclass、维度存 dict、无幂等键属性），
让模块能被收集，从而看到真正的断言失败**：

```
18 failed, 14 passed in 1.11s
```

失败的断言（节选）：

| 用例 | 失败断言 |
|---|---|
| `test_metric_point_is_frozen` | `Failed: DID NOT RAISE <class 'dataclasses.FrozenInstanceError'>` |
| `test_metric_point_dimensions_normalized_to_sorted_tuple` | `assert point.dimensions == (('A','1'),('B','2'))` 不成立（存的是 dict） |
| `test_metric_point_is_hashable_and_usable_as_dict_key` | `TypeError: unhashable type: 'dict'` |
| `test_metric_point_idempotency_key_matches_ids_module` | `AttributeError: 'MetricPoint' object has no attribute 'idempotency_key'` |
| `test_alarm_detail_is_hashable` / `test_alarm_record_is_hashable` | `TypeError: unhashable type: 'dict'` |
| `test_alarm_record_idempotency_key_differs_between_activated_and_recovered` | `AttributeError: 'AlarmRecord' object has no attribute 'idempotency_key'` |
| `test_alarm_record_idempotency_key_ignores_node_id` | 同上（`AttributeError`） |

### GREEN

```
$ conda run -n tpops python -m pytest tests/unit/test_models.py -q
32 passed in 0.23s

$ conda run -n tpops python -m pytest -q
400 passed in 11.49s
```

（基线 344 passed + 本任务 32 个用例 + 并发中的任务 1.7 带来的用例；无回归。）

```
$ conda run -n tpops ruff check tpops_kafka_collector/ tests/unit/test_models.py
All checks passed!
```

## 3. 偏离简报之处及原因

1. **改了 `tpops_kafka_collector/mapping/decode.py`（3 行）** ——
   共享约定说「不改 `mapping/` 下 wave 0 已建的文件，除非任务简报明确说了要改」。
   简报要求「`DecodeError` 归入 `MappingError`」且错误体系集中在 `errors.py`；
   而 wave 0 把 `DecodeError` 定义在 `decode.py` 里。若在 `errors.py` 再定义一个同名类，
   就会出现**两个不同的 `DecodeError`**（`except` 抓不住其中一个、
   `isinstance` 判定分裂）。因此把 `decode.py` 的本地类改为从 `..errors` import 并再导出，
   保持既有引用路径可用。这是本任务唯一动到 wave 0 文件的地方，改动面 3 行。

2. **`MetricPoint` 比开发方案 §5.3 多三个字段**：`node_id` / `host_ip` / `component_id`
   （均为 `Optional[str] = None`，追加在末尾，不影响 §5.3 里的构造方式）。
   原因：客户消息的 `resourceName` 与 `tags` 要靠节点身份（节点 IP + 节点 ID），
   §5.3 的 DTO 只有 `instance_id` / `instance_name`，序列化层（任务 1.4）就得
   重新解析一遍节点。追加这三个字段比让下游重新解析更省事，也不破坏原字段集。

3. **`MetricPoint` / `AlarmRecord` 的字段给了默认值**（§5.3 里全是必填）。
   原因：平台返回体里不少字段可能缺失（如 `node_id`），必填会让归一化层
   到处写 `or ''`；给了默认值后 DTO 可以直接由平台 dict 构造。
   关键字段（`alarm_id` / `instance_id` / `first_alarm_time` / `status` /
   `customer_metric` 等）仍是必填。

4. **`MetricPoint.idempotency_key` 用 `customer_metric` 而不是 `metric_name`** ——
   与开发方案 §5.3 给的写法一致（§5.3 的实现用 `self.customer_metric`），
   也与 `ids.METRIC_IDEMPOTENCY_FIELDS` 里的 `'metric_name'` 指的是同一个东西
   （客户指标库里的英文名）。DTO 上**没有**另设 `metric_name` 字段，避免同一含义两个名字。

## 4. 遗留 / 需要控制器知道

- **⚠️ 合同口径不一致**（沿用 wave 1 共享简报的统一口径）：合同的 Wave 1 完成标准写
  `python -m tpops_kafka_collector.main` 能起，但 `main.py` 属 wave 3 任务 3.3，
  wave 1 无法证明。本任务只提供 DTO 与异常体系，不涉及进程入口。
- `mapping/decode.py` 的 `DecodeError` 现在是**再导出**（`from ..errors import DecodeError`）。
  任务 1.8 重写规则引擎时若动了 `decode.py`，注意别把 `DecodeError` 又挪回去。
- 幂等键的**声明侧**（`data/mapping/alarm_rules.json` 的 `idempotency` 段）本任务未改 ——
  它由 `tools/build_itscada_mapping.py` 从 `ids.py` 生成，声明与实现同源；
  测试断言的是「DTO 的键 == `ids` 的键」，声明侧的一致性由 wave 0 的
  `tests/test_rules.py -k idempotency` 守。
- 未执行 `git commit`（按波次约定由控制器统一提交）。
