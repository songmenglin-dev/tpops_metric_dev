- [ ] **1.4 JSON 序列化器**（W04）：新建 `tpops_kafka_collector/kafka/serializer.py`，实现
  `itscada_field_map.json` 的信封（指标 7 字段 / 告警 13 字段），`ensure_ascii=False`、
  字段顺序固定、缺失值一律 `null`、时间 13 位毫秒、业务维度落 `tags.dimension_values`、
  值按客户表【数据类型】序列化（整数/浮点/字符/文本）。
  完成后全仓只有这一处 `json.dumps` 产出消息。
  证明：`pytest tests/unit/test_serializer.py -q`
