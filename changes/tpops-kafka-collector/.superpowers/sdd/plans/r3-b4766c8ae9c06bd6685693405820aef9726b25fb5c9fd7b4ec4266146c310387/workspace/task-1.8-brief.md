- [ ] **1.8 规则引擎**（W08）：在 `tpops_kafka_collector/mapping/rules.py` 实现映射表在用的全部
  13 种 `value_rule.type`（`raw` / `raw_filtered` / `aggregate` / `counter_delta` /
  `ratio_across` / `series_point` / `enum_map` / `gauge` / `top_n` / `raw_rows` /
  `collection_count` / `text_prefix_count` / `instance_field`），差分用实际采集时刻差，
  首轮无基线只记基线，计数归零丢弃本轮并记 ERROR。
  证明：`pytest tests/unit/test_rules.py -q`
