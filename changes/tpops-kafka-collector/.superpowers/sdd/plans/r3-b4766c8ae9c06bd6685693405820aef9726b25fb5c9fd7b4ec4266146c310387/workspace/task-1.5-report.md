# 任务 1.5 报告：Kafka 生产者

> 实施方：wave-1 任务 1.5 ｜ 工作目录：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`
> （全程在 worktree 内操作，未 `git commit`）

## 1. 做了什么

新建 Kafka 生产者：**PLAINTEXT**、**`acks=all`**、**`enable_idempotence=False`**、
压缩与分区数可配、指标与告警**分 Topic**、Key 分别是 `instance_id` 与 `alarm_id`；
任一条没投出去抛 `ProduceError`（子类 `ProduceBatchError`），调用方据此**不推进水位**。

### 文件清单

| 文件 | 状态 | 行数 | 说明 |
|---|---|---|---|
| `tpops_kafka_collector/kafka/producer.py` | 新建 | 399 | 生产者本体：`MessageProducer` + 生产者配置 |
| `tpops_kafka_collector/kafka/records.py` | **新建（额外文件，见 §4 偏离 1）** | 127 | 投递记录 / 结果 DTO + Key 规则 |
| `tpops_kafka_collector/kafka/__init__.py` | 修改（**仅追加**导出，未删改 1.4 的内容） | 61 | 追加 producer/records 的公开名 |
| `tests/integration/test_producer.py` | 新建 | 478 | 26 条测试，**全部 mock，不连真实 Kafka** |
| `tests/integration/__init__.py` | 新建（空文件） | 0 | 与既有 `tests/unit/__init__.py` 约定一致 |

### 公开 API

```python
from tpops_kafka_collector.kafka.producer import (
    MessageProducer, ProduceRecord, BatchResult, FailedRecord, ProduceBatchError,
    metric_record, alarm_record, build_producer_config, supported_kwargs, create_producer,
)

producer = MessageProducer.from_config(config)          # Topic/地址/压缩/分区数全走配置
result  = producer.produce_metrics([metric_record(msg)]) # 全成功 → BatchResult
producer.produce_alarms([alarm_record(msg)])             # 任一条失败 → ProduceBatchError
producer.ensure_topics()                                 # 按配置的分区数建 Topic（可注入 admin）
producer.close()
```

### 冻结口径的落点

| 口径 | 落点 |
|---|---|
| PLAINTEXT | `build_producer_config()` 的 `security_protocol='PLAINTEXT'`；**没有任何 `sasl_*` / `ssl_*` 项**（有测试守住） |
| `acks=all` | `build_producer_config()` 的 `acks='all'`（不是默认的 `'1'`） |
| `enable_idempotence=False` | `build_producer_config()` **显式**写 `False`（常量 `ENABLE_IDEMPOTENCE`） |
| 分 Topic | `produce_metrics()` → 指标 Topic；`produce_alarms()` → 告警 Topic；**永不混投** |
| Key | `metric_record()` 取 `tags.instance_id`；`alarm_record()` 取 `alarmId`；取不到**显式报错**，不静默丢消息 |
| 压缩 / 分区数 | `KAFKA_COMPRESSION` → `compression_type`；`KAFKA_TOPIC_PARTITIONS` → `ensure_topics()` 的 `num_partitions` |
| 失败语义 | 任一条失败 → `ProduceBatchError`（`ProduceError` 子类，`is_retryable` 为 `True`），携带 `result`/`failed` |

### 关键实现说明

1. **`ProduceBatchError` 是 `ProduceError` 的子类**（不是「另一个异常类型」）：
   `except ProduceError` 照样抓住，`errors.is_retryable` 照样判为可重试；
   额外的 `result` / `failed` 只是告诉调用方**哪几条没投出去**（供下轮重投）。
   全部成功时 `produce_*` 返回 `BatchResult`（含 `succeeded` / `failed` 两个集合）——
   「批量投递的返回值要能知道哪些成功、哪些失败」由返回值 + 异常两条路径共同满足。
2. **`enable_idempotence` 的处理**：`kafka-python==2.0.2` 的
   `KafkaProducer.DEFAULT_CONFIG` **没有** `enable_idempotence` 这个键（实测确认），
   直接传会抛 `KafkaConfigurationError`。所以逻辑配置里**显式**写 `False`，
   再由 `supported_kwargs()` 按客户端实际支持的键过滤（并记 INFO）。
   语义上幂等是**关着的**：2.0.2 根本无法开启幂等；将来换成支持幂等的客户端，
   该键会被原样传下去（值恒为 `False`）。有测试守住「值恒为 False」。
3. **消息体自己编 UTF-8 字节**：`kafka-python` 2.0.2 在不配 `value_serializer` 时
   **不做**编码，传 `str` 会被它自己的断言拦下，所以 `_to_bytes()` 统一编码。
4. **`max_in_flight_requests_per_connection = 1`**：幂等已关闭，重试时在途请求可能被
   后来的消息超越；取 1 才保证「同实例消息严格有序」（规格场景）。
5. **启动期故障分类**：broker 不可达 / 建 Topic 失败 → 包装成 `TransientError`
   （可重试），不把客户端原始异常直接抛给调度层。
6. **`ensure_topics()` 逐个建 Topic**：客户端一次建多个时，只要有一个已存在就整批报错，
   另一个 Topic 就再也建不上。

## 2. TDD：RED 的失败断言

### RED-1（首次）：模块还不存在

```
$ conda run -n tpops python -m pytest tests/integration/test_producer.py -q
E   ModuleNotFoundError: No module named 'tpops_kafka_collector.kafka.producer'
ERROR tests/integration/test_producer.py
```

### RED-2（断言级）：拿「想当然的朴素实现」跑规格测试

为了给出**断言级**的 RED 证据，在 `/tmp/red15`（**仓库外**，跑完已删）复制了一份包，
把 `kafka/producer.py` 换成朴素实现（`acks='1'`、不声明 `enable_idempotence`、
Key 写死 `'constant'`、失败只记 WARNING 吞掉、不区分 Topic）：

```
$ PYTHONPATH=/tmp/red15 conda run -n tpops python -m pytest tests/integration/test_producer.py -q
20 failed, 6 passed
```

失败的关键断言（原文）：

```
E  AssertionError: assert '1' == 'all'                        # acks 必须是 all
E  KeyError: 'enable_idempotence'                              # 必须显式声明关闭幂等
E  AssertionError: assert 'constant' == 'inst-42'             # 指标 Key 必须是 instance_id
E  AssertionError: assert 'constant' == 'alarm-77'            # 告警 Key 必须是 alarm_id
E  Failed: DID NOT RAISE <class 'ValueError'>                 # 取不到 Key 必须显式报错
E  AssertionError: assert 'constant' == b'inst-1'             # 投递时 Key 必须是 instance_id
E  AssertionError: assert ['constant', 'constant'] == [b'inst-1', b'inst-1']   # 同 Key 同分区
E  AssertionError: assert () == (<ProduceRecord ...>)         # 成功也要返回成功清单
E  Failed: DID NOT RAISE <class 'ProduceError'>               # 失败必须抛 ProduceError
E  Failed: DID NOT RAISE <class 'ProduceError'>               # 发送阶段失败同样要抛
```

（另有 `test_producer_config_has_no_sasl_or_ssl_options`、
`test_supported_kwargs_only_contains_options_known_to_kafka_python`、
`test_ensure_topics_*` 等一并变红。）

### GREEN

```
$ conda run -n tpops python -m pytest tests/integration/test_producer.py -q
..........................                                          [100%]
26 passed in 0.21s
```

全量回归（同一时刻）：

```
$ conda run -n tpops python -m pytest -q
711 passed in 10.65s
```

门禁：

```
$ ruff check tpops_kafka_collector/
All checks passed!            # 退出码 0
$ ruff check tests/integration/
All checks passed!
$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
77 passed                     # 3.7 语法/标准库门禁（含新增的 producer.py / records.py）
```

> 注：`tests/test_kafka_mapping.py` 那条已知失败在本轮已不复现（全量 711 全绿），
> 应是并发的任务 1.8 已修完。**我的判据**（`tests/integration/test_producer.py` 全绿 +
> `ruff check tpops_kafka_collector/` 为 0）均满足。

## 3. 依赖与合规自查

| 约束 | 落实 |
|---|---|
| 依赖锁版本 | 只用 `kafka-python==2.0.2`；**没有**换成 `confluent-kafka`（不引入 librdkafka 运行时） |
| 不新增依赖 | 无新依赖 |
| PLAINTEXT | 配置里只有 `security_protocol='PLAINTEXT'`，无 SASL/SSL 项 |
| Python 3.7.9 | 无海象、无内建泛型注解、无 3.8+ 标准库 API（`tests/test_py37_compat.py` 门禁通过） |
| 不可变数据 | `ProduceRecord` / `FailedRecord` / `BatchResult` 全是 `@dataclass(frozen=True)` |
| 显式错误处理 | 发送/确认/flush/关闭的异常都有显式处理；失败不吞、不改类型 |
| `logging` 不用 `print` | 生产代码零 `print` |
| 单文件 < 400 行 | `producer.py` 399 行、`records.py` 127 行 |
| 函数 < 50 行 | 最长 `__init__` 约 40 行 |
| 无硬编码凭据 | 生产者不涉及凭据；broker 地址/Topic 全走配置 |
| Kafka 一律 mock | 测试用假 producer / 假 admin 注入；**没有**任何测试连真实 broker |

## 4. 偏离简报之处及原因

**偏离 1（文件）：多了一个 `tpops_kafka_collector/kafka/records.py`。**
原因：`producer.py` 写完后 **527 行**，超出合同的「单文件 < 400 行」硬约束。
按全局规范（MANY SMALL FILES）与 wave-0 先例（任务 1.8 拆 `mapping/rules.py`），
把「投递记录 / 投递结果 / Key 规则」拆到 `records.py`（127 行），
`producer.py` 收敛到 399 行。**公开 API 未变** —— `producer.py` 与
`kafka/__init__.py` 都重新导出了 `ProduceRecord` / `BatchResult` /
`FailedRecord` / `ProduceBatchError` / `metric_record` / `alarm_record`，
调用方（含 wave 3 的调度层）照旧 `from ...kafka.producer import ...` 即可。
⚠️ 合同 `execution-contract.md` 的 wave-1 输出清单写的是 `kafka/{serializer,producer}.py`，
多出的 `records.py` **未写进合同**（按波次约定不改合同），提请控制器裁决是否登记。

**偏离 2（`kafka/__init__.py`）：追加了 producer/records 的导出。**
简报明确允许「追加导出，但不要删改 1.4 的内容」—— 本次只**追加**，
1.4 的序列化器导出与 `__all__` 原有条目一个没动。

**偏离 3（新增 `tests/integration/__init__.py`）：** 空文件，与既有 `tests/unit/__init__.py` 对齐。

**偏离 4（异常是 `ProduceError` 的子类）：** 见 §1「关键实现说明」第 1 条。
`ProduceBatchError(ProduceError)` —— **不是**另一个异常类型：
`except ProduceError` 能抓住、`is_retryable` 判为可重试、`pytest.raises(ProduceError)` 成立。
之所以带 `result`/`failed`，是因为简报要求「返回值要能知道哪些成功、哪些失败」，
而失败路径必须抛异常 —— 两者的信息用子类承载。

**偏离 5（新增 `TransientError` 的使用）：** 启动期 broker 不可达 / 建 Topic 失败
包装成 `TransientError`（errors.py 已有、可重试），而不是把 kafka 客户端的原始异常
抛给调度层。`ProduceError` 的语义是「这条没投出去」，与「启动时连不上 broker」不是一回事，
所以没有滥用 `ProduceError`。

**偏离 6（`max_in_flight_requests_per_connection = 1`）：** 简报未提。
为满足规格场景「同实例的消息严格有序」而显式设 1（幂等已关闭，顺序得自己扛）。
若客户侧吞吐不足，这一项可以调大 —— 改一个常量即可，不影响其它口径。

## 5. 遗留问题 / 提请控制器注意

1. **`kafka/records.py` 未写进合同的 wave-1 输出清单**（见偏离 1）—— 请裁决是否登记。
2. **`enable_idempotence` 在 kafka-python 2.0.2 上无处可传**（客户端不支持该键）。
   本实现是「逻辑配置显式声明 False + 构造时按客户端支持的键过滤」。
   如果审核要求字面上必须把该键传给 `KafkaProducer`，那与锁版本
   `kafka-python==2.0.2` 冲突（会抛 `KafkaConfigurationError`）——需要裁决。
3. **`ensure_topics()` 未被调用方使用**：wave 3 的调度层/启动流程需要决定
   何时建 Topic（启动时一次性？连接不上时跳过？）。当前实现是「显式调用才建」。
4. **`replication_factor` 默认 1**：客户环境的 broker 数未知，副本数没走配置项
   （`config.py` 里没有这一项，我也没改配置模块 —— 那属于任务 1.1 的地盘）。
   建议在配置里补一个 `KAFKA_TOPIC_REPLICATION_FACTOR`（默认 1）。
5. **未做真实 broker 联调**：仓库没有可用 broker，本任务全部测试是纯 mock。
   `kafka-python` 的真实往返（acks=all 的确认时延、压缩是否被 broker 接受）留到 P5 联调。
6. **`produce_*` 不做幂等去重**：规格里的「LRU 幂等键集合」在
   `tpops_kafka_collector/watermark.py`（`IdempotencyKeyLRU`，任务 1.9 已落盘），
   由调用方在投递前使用；生产者本身不持有幂等状态 —— 避免两处各去重一次。
7. **本任务不推进水位**：水位的推进在 wave 3 的调度层；本模块只负责
   「投递失败就抛 `ProduceError`」这一半契约。
