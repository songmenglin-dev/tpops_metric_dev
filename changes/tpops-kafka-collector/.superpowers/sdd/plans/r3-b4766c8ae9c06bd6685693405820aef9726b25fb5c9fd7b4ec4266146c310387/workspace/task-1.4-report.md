# 任务 1.4 报告：JSON 序列化器（唯一序列化出口）

- 实施方：wave-1 任务 1.4
- 工作目录：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（worktree，分支 `tpops_metric_dev`）
- 未提交 git（按 wave-1 共享约定，提交由控制器统一做）

---

## 1. 做了什么

### 1.1 新增文件

| 文件 | 行数 | 内容 |
|---|---|---|
| `tpops_kafka_collector/kafka/serializer.py` | 270 | 唯一 JSON 序列化出口 |
| `tpops_kafka_collector/kafka/__init__.py` | 31 | 投递层包导出 |
| `tests/unit/test_serializer.py` | 350 | 32 个单元测试 |

### 1.2 `serializer.py` 的职责边界

**分工**（与 wave 0 的 `mapping/message.py` 划清，避免两处各写一套）：

- `mapping/message.py` —— **组装**信封（每个字段取什么值）。
- `kafka/serializer.py` —— **定型与落地**（字段顺序、缺省、类型、JSON 编码）。

公开 API：

| 符号 | 说明 |
|---|---|
| `serialize_metric(message, value_type=None) -> str` | 指标消息 → JSON 文本 |
| `serialize_alarm(message) -> str` | 告警消息 → JSON 文本 |
| `apply_value_type(value, value_type=None) -> Any` | 按客户表【数据类型】定型取值 |
| `to_epoch_millis(value) -> Optional[int]` | 时间统一成 13 位毫秒 |
| `dumps(payload) -> str` | 唯一调 `json.dumps` 的地方 |
| `SerializerError` | 序列化失败（不可重试） |
| `METRIC_FIELDS` / `ALARM_FIELDS` | 指标 7 字段 / 告警 13 字段（顺序即输出顺序） |
| `METRIC_TAG_FIELDS` / `ALARM_TAG_FIELDS` | 指标 6 键 / 告警 9 键标签 |

### 1.3 逐条对齐简报要求

| 简报要求 | 落地方式 |
|---|---|
| 指标 7 字段 / 告警 13 字段 | `METRIC_FIELDS` / `ALARM_FIELDS`，与 `itscada_field_map.json` **逐字段逐顺序**由测试比对 |
| `ensure_ascii=False` | `dumps()` 里显式传 `ensure_ascii=False`（中文不转义） |
| 字段顺序固定 | `sort_keys=False` + 按信封顺序建 dict；两次序列化输出逐字节相同（已测） |
| 缺失值一律 `null` | 缺失键 → `None`；空串 → `None`（`unit` 尤其）；不填 0 |
| 时间 13 位毫秒 | `to_epoch_millis()`：秒级自动 ×1000；`0`（平台「无采集时刻」）→ `null`；浮点截断为 int |
| 业务维度落 `tags.dimension_values` | `tags` 按 `METRIC_TAG_FIELDS` 归一，维度单独进 `dimension_values`，无维度 → `{}` |
| 值按【数据类型】序列化 | `apply_value_type()`：整数 `int(float(v))` / 浮点 `round(float(v), 2)` / 字符·文本 `str(v)`；取不到 → `null` |
| 全仓只有这一处 `json.dumps` 产出消息 | 见 §4 |

### 1.4 与 wave 0 的口径一致性（关键）

**没有另起一套值转换逻辑。** `mapping/message.py` 的 `serialize_value` 已经实现了
「按客户表【数据类型】序列化」和「浮点保留 2 位小数」；本模块的 `apply_value_type`
与之**逐值等价**，由 `test_value_serialization_matches_mapping_layer` 对 10 组
（类型 × 值）样本断言 `payload['value'] == serialize_value(value, value_type)`
—— 尤其守住 `_as_float` 的 `round(float(value), 2)`。

命名刻意不同（`apply_value_type` vs 映射层的 `serialize_value`）：两边都吃
`value_type`，但映射层那个必须给类型，本函数允许 `value_type=None` 表示
「已定型，原样落地」（映射层组装好的消息走这条路，不重复定型）。

### 1.5 「唯一 json.dumps」的落实与守卫

- 生产代码 `tpops_kafka_collector/` 内，`json.dumps` 只出现在
  `tpops_kafka_collector/kafka/serializer.py:186` 一处。
- 其余两处 json 编码**不是产出消息**，未动：
  - `tpops_kafka_collector/watermark.py:360` —— 状态文件（任务 1.9 的文件，不是消息）。
  - `tpops_kafka_collector/tpops/client.py` —— TPOPS HTTP 请求/响应体（任务 1.6 的文件）。
- 守卫测试 `test_kafka_package_has_only_one_json_dumps`：**用 AST 扫 `tpops_kafka_collector/kafka/`
  全包**（不是字符串 grep —— 文档字符串里提到 `json.dumps` 不算），除 `serializer.py`
  外任何 `json.dumps` / `json.dump` 调用都判失败。
  守卫范围刻意限定在**投递层**：HTTP 请求体与状态文件用 json 编码是正当的，
  全包一刀切会误伤。全包一刀切也不可行 —— `tpops/` 与 `watermark.py` 都要用 json。

---

## 2. RED → GREEN

### 2.1 RED（实现前）

```
$ conda run -n tpops python -m pytest tests/unit/test_serializer.py -q
ERROR collecting tests/unit/test_serializer.py
ImportError while importing test module '.../tests/unit/test_serializer.py'.
tests/unit/test_serializer.py:18: in <module>
    from tpops_kafka_collector.kafka.serializer import (
E   ModuleNotFoundError: No module named 'tpops_kafka_collector.kafka'
=========================== short test summary info ============================
ERROR tests/unit/test_serializer.py
1 error in 0.23s
```

**RED 时失败的断言**：模块不存在（`ModuleNotFoundError`）—— 32 条断言一条都还没跑起来。
这是「新建模块」型任务的正常 RED：先有测试文件（引用尚不存在的
`tpops_kafka_collector.kafka.serializer`），再落地实现。

### 2.2 GREEN

```
$ conda run -n tpops python -m pytest tests/unit/test_serializer.py -q
................................                                         [100%]
32 passed in 0.26s
```

### 2.3 中途真实失败（已修）

第一次实现后有 2 条失败，记录在此以备 review 追溯：

1. `test_serialized_mapping_message_has_the_full_envelope` — `NameError: name 'FIELD_MAP' is not defined`
   → 测试里用了未定义的模块级 `FIELD_MAP`（fixture 名是 `field_map`）。改为用 fixture 参数。
2. `test_kafka_package_has_only_one_json_dumps` —
   `AssertionError: kafka 包里除 serializer.py 外还有 json.dumps：['__init__.py']`
   → 字符串 grep 命中了 `__init__.py` **文档字符串**里的 `json.dumps` 字样。
   → 改为 AST 扫 `ast.Attribute` 调用（`json.dumps` / `json.dump`），文档字符串不再误报。

---

## 3. 测试覆盖的内容

32 条，分五组：

**信封（7 条）** —— 指标恰好 7 字段、告警恰好 13 字段、字段顺序固定、
两次序列化输出一致、**与 `itscada_field_map.json` 逐字段逐顺序比对**
（指标字段 / 告警字段 / 指标标签键 / 告警标签键四项全比）。

**缺省约定（7 条）** —— 缺字段 → `null`；缺失值 → `null`（≠ 0）；空 `unit` → `null`；
信封外字段丢弃；`tags` 外键丢弃；无维度 → `dimension_values == {}`；
无明细 → `details == []`。

**编码与转义（2 条）** —— 中文不转义（输出里无 `\u`）；UTF-8 往返无损。

**值类型（5 条）** —— 整数 `"12345"` → JSON 数字 `12345`（`isinstance int`）；
浮点 `0.7941176470588235` → `0.79`；文本 → JSON 字符串；字符 `7` → `"7"`；
**与映射层 `serialize_value` 逐值等价（10 组样本）**；告警 value → 字符串。

**时间戳（5 条）** —— `clock` 是 13 位毫秒；秒级自动放大；浮点截断为 int；
缺失 → `null`；告警 `alarmTime` / `recoverTime` 同规则。

**错误处理（2 条）** —— 非映射输入 → `SerializerError`；`NaN` → `SerializerError`
（`allow_nan=False`，NaN 不是合法 JSON，宁可报错也不发出去）。

**唯一出口（1 条）** —— AST 扫 `kafka/` 全包，只有 `serializer.py` 能调 `json.dumps`。

**衔接（2 条）** —— 映射层 `build_metric_message` 组装的消息经序列化后仍是 7 字段、
维度仍在 `tags.dimension_values`；`value_type=None` 时值原样落地。

---

## 4. 门禁

| 检查 | 结果 |
|---|---|
| `ruff check tpops_kafka_collector/kafka/` | **All checks passed!** |
| `ruff check tests/unit/test_serializer.py` | **All checks passed!** |
| `pytest tests/unit/test_serializer.py -q` | **32 passed** |
| `pytest tests/test_py37_compat.py -q` | **72 passed**（py3.7 门禁含新文件） |
| 单文件行数 | `serializer.py` 270 / `__init__.py` 31 / 测试 350 —— 均 < 400 |
| 函数长度 | 全部 < 50 行（最长 `apply_value_type` 16 行、`_normalize_tags` 20 行） |
| `print` | 无（用 `logging`） |
| Python 3.7.9 | 无海象 / 无内建泛型注解 / 无 3.8+ API；`typing.List/Dict/Optional/Tuple` |

### 全仓测试

基线（我开工时）：**429 passed**（brief 里写的 344 已过时）。

我完工时的实测：

```
$ conda run -n tpops python -m pytest -q --ignore=tests/unit/test_rules.py --ignore=tests/unit/test_client.py
2 failed, 574 passed in 9.74s
FAILED tests/test_kafka_mapping.py::test_file_is_up_to_date
FAILED tests/test_response_rules.py::test_every_rule_produces_a_value
```

**这 2 条失败不是我的**（任务 1.8 正在重写规则引擎，改 `mapping/rules.py` →
`mapping/rules/`，日志里能看到 1.8 的新代码 `mapping.rules_cross` 在跑）。
**已实证与我的改动无关**：把我的两个文件移出仓库再跑，同样这 2 条失败
（`2 failed, 536 passed`）—— 见 §5 复现步骤。同理 `tests/unit/test_client.py`
（任务 1.6）与 `tests/unit/test_rules.py`（1.8，与 `tests/test_rules.py` 同名导致
pytest 收集冲突）都是并发任务的在飞状态，我未碰。

**控制器需要知道**：等 1.8 / 1.6 落地后跑一次完整 `pytest -q`，确认这 2 条转绿。

---

## 5. 复现步骤（供控制器核对）

```bash
cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
conda run -n tpops python -m pytest tests/unit/test_serializer.py -q   # 32 passed
ruff check tpops_kafka_collector/kafka/                                # All checks passed
```

「失败与我的改动无关」的实证：

```bash
mv tpops_kafka_collector/kafka /tmp/kafka_bak
mv tests/unit/test_serializer.py /tmp/test_serializer_bak
conda run -n tpops python -m pytest -q --ignore=tests/unit/test_rules.py --ignore=tests/unit/test_client.py
# 2 failed, 536 passed —— 同样那 2 条失败
mv /tmp/kafka_bak tpops_kafka_collector/kafka
mv /tmp/test_serializer_bak tests/unit/test_serializer.py
```

---

## 6. 偏离简报之处及原因

### 6.1 未创建 `models.py`，序列化器用普通 dict

**原因**：任务 1.2（`models.py`）与本任务并发产出，1.2 的 DTO 形态此刻未定，
创建会抢 1.2 的文件。按简报指示「按 wave 0 `mapping/message.py` 的形态用普通 dict」。

**依赖说明**：本模块**不依赖** `models.py`，只用 `typing.Mapping`。
调用方（wave 2/3 的采集器）拿到 `MetricPoint` 后，用 `dataclasses.asdict` 或
直接传映射进来都能用。**若 1.2 的 DTO 最终形态与普通 dict 差异较大，
需要在 1.2 落地后复核一次 `serialize_metric` 的入参类型注解**（不影响实现逻辑）。

### 6.2 序列化器自带一份值定型逻辑，而不是直接 import 映射层的 `serialize_value`

**原因**：两个理由。
1. **依赖方向**：`design.md` 决策 1 定的是 `mapping → models`，
   `kafka` **不反向依赖任何层**。`kafka/serializer.py` import `mapping/message.py`
   会让投递层反向依赖映射层 —— 违反决策 1。
2. **职责划分**：`mapping/message.py` 管「字段取什么值」，
   `kafka/serializer.py` 管「定型与落地」。两边各自完整，序列化器可以脱离
   映射层单测（32 条测试里 30 条不碰映射层）。

**漂移风险的对冲**：不是靠自觉，是靠
`test_value_serialization_matches_mapping_layer` —— 10 组（类型 × 值）样本断言
本模块的 `apply_value_type` 与映射层的 `serialize_value` **逐值相等**。
任一边改了口径而另一边没改，测试立刻红。

### 6.3 秒级时间戳自动 ×1000

**简报只说「时间 13 位毫秒」，没说输入单位。** 实测留档里 A 类
`timestamps` 是 `1794447039000.0`（已是毫秒），而采集完成时刻 `now_ms`
也是毫秒 —— 但 `1e11` 以下的整数（如 `1794447039`）显然是秒。
`to_epoch_millis` 按 `1e11` 分界（`1e11` ms = 1973 年，`1e11` s = 5138 年，
中间不可能是真实采集时刻），低于分界按秒 ×1000。
`0` 按平台「无采集时刻」处理 → `null`。

### 6.4 告警 value 一律按「字符」定型

`mapping/message.py` 的 `build_alarm_message` 就是这么做的
（`serialize_value(value, '字符')`）。本模块沿用同一口径，
两边一致（`test_alarm_value_is_serialized_as_string` 守住）。

### 6.5 信封外字段 / `tags` 外键：**丢弃 + WARNING**，不抛异常

客户字段表是**封闭集合**（7 / 13 字段），多出来的字段客户侧不认。
但这属于「上游多给了信息」，不是「消息无法表达」——
抛异常会让整轮投递因一个字段名失败，代价过大。
因此：**丢弃 + `logging.WARNING`**（不静默吞），日志里能看到具体是哪些键。

### 6.6 `SerializerError` 定义在 `kafka/serializer.py` 内，未进 `errors.py`

**原因**：`errors.py` 是任务 1.2 的产物（`TpopsError` 体系在那儿），
1.2 与本任务并发，不宜抢改它的文件；且 `SerializerError` 是投递层的本地错误，
不进平台的异常体系也合理（它不该被 `is_retryable` 当成 TPOPS 平台错误处理）。
**待办**：1.2 落地后，若控制器认为它该进 `errors.py`（例如接入统一重试判定），
需要协调一次 —— 但注意它**不可重试**，重试同样的输入只会再失败一次。

---

## 7. 遗留问题与注意事项

1. **⚠️ 完整测试需在 1.8 / 1.6 落地后重跑**。当前
   `tests/test_kafka_mapping.py::test_file_is_up_to_date` 与
   `tests/test_response_rules.py::test_every_rule_produces_a_value`
   两条失败来自 1.8 的规则引擎重写，与本任务无关（§4、§5 有实证）。

2. **⚠️ `tests/unit/test_rules.py` 与 `tests/test_rules.py` 同名**，
   pytest 因 `tests/unit/` 无 `__init__.py` 而收集冲突（报
   `import file mismatch`）。这是 1.8 新建的文件带来的，**不是我建的**
   （我的 `tests/unit/test_serializer.py` 名字唯一，无冲突）。
   建议 1.8 或控制器补 `tests/unit/__init__.py` 或改文件名。

3. **映射数据搬进包内后（任务 1.7 / 2.1），
   `itscada_field_map.json` 的包内副本要与仓库根一致** ——
   我的测试读的是**仓库根** `data/mapping/itscada_field_map.json`
   （`conftest.DATA_DIR`）。若 1.7 把六张表复制进 `tpops_kafka_collector/mapping/data/`，
   建议把本测试的字段表来源改为**包内副本**（生产代码运行时不读仓库根），
   或加一条「两份一致」的断言。这条我**没做** —— 1.7 的文件还没落地，
   猜路径会写错。**请控制器在 1.7 落地后指派这一条**。

4. **`serialize_metric` 的 `value_type` 参数**：
   映射层组装的消息里 `value` 已按规则定型，走默认的 `value_type=None` 即可；
   只有在**绕过映射层**、直接拿原始值来序列化时才需显式传类型。
   调用方（wave 2 采集器）请按此选用，避免重复定型。

5. **`dumps()` 用了 `separators=(',', ':')`（紧凑）**。
   客户侧若按可读性人工核对消息体，会看到没有空格的紧凑 JSON。
   如需带空格（`json.dumps` 默认），改 `dumps()` 一行即可 —— 但改了就是
   消息体逐字节变化，属对外契约，建议先与客户确认。

6. **`allow_nan=False`** → `NaN` / `Infinity` 抛 `SerializerError` 而不是发出
   非法 JSON。若上游真的产出 `NaN`，本轮该条消息失败并进重试；
   由于是同一个坏数据反复重试，建议在调用侧把 `NaN` 视作 `null`
   （`apply_value_type` 的 `float('nan')` 会走到 `_as_float` → `round(nan,2)` = `nan`，
   最终在 `dumps` 被拦下）。**这条建议列入 wave 2 采集器的口径检查。**
