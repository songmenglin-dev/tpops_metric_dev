- [ ] **1.5 Kafka 生产者**（W05）：新建 `tpops_kafka_collector/kafka/producer.py`，PLAINTEXT、
  `acks=all`、`enable_idempotence=False`、压缩与分区数可配、指标与告警分 Topic、
  Key 分别为 `instance_id` 与 `alarm_id`。完成后失败抛 `ProduceError`，
  调用方得以据此不推进水位。
  证明：`pytest tests/integration/test_producer.py -q`
