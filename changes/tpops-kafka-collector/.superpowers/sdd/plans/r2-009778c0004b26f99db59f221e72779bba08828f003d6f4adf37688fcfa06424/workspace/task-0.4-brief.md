- [ ] **0.4 在包内修跨视图比率**：让包内的 `decode_merged` 同时填 `ctx.rows_by_key`。
  完成后 `gaussdb.Total_pct` 与 `GAUSSDB.session_usage` 拿到真实比率而非 0。
  证明：`pytest tests/test_merged_sql_mapping.py -q -k cross_view`
