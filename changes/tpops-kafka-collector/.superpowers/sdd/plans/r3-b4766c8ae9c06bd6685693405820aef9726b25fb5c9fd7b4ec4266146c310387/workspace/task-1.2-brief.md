- [ ] **1.2 DTO 与异常体系**（W02）：新建 `tpops_kafka_collector/models.py`（`MetricPoint` /
  `AlarmDetail` / `AlarmRecord`，frozen dataclass，维度用排序后的元组保证可哈希；
  `AlarmRecord.idempotency_key` 按波次 0 定的新键计算）与 `tpops_kafka_collector/errors.py`
  （`TpopsError` 及 `AuthError` / `TransientError` / `ProtocolError` / `MappingError` /
  `ProduceError` 五个子类 + 可重试判定表；`DecodeError` 归入 `MappingError`）。
  证明：`pytest tests/unit/test_models.py -q`
