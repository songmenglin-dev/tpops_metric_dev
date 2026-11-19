- [ ] **0.2 在包内加 `merged_views` 段与合并形态解码**：在 `tpops_kafka_collector/mapping/decode.py`
  新增 `decode_merged` / `merge_pages`；改 `tools/build_response_rules.py` 生成
  `merged_views` 段（11 个视图，各带 `json_column` / `paged` / `sql_ref`；
  未声明 `paged` 的按 `false`），并校验它与 `data/merged_sql/merged_sql.json`
  的视图名一一对应。
  完成后「一行 11 个 json_agg 列」的返回体能解出每个视图的行列表，空页得到 `[]`，
  坏 JSON 抛 `DecodeError`（归入 `MappingError`，不可重试）。
  证明：`pytest tests/test_merged_sql_mapping.py -q`
