- [ ] **0.5 合并形态的列校验与证据重推导**：在 `tools/build_response_rules.py` 增加一条针对
  合并形态的列名校验，数据源为 `data/merged_sql/sql_merge_probe_result.json` 与
  `data/merged_sql/merged_sql.json`（实测留档为准，映射表为声明）。
  完成后映射表与真实返回的列名不一致时生成器直接报错；且重跑生成器后
  `git diff` 为空（映射表可重复生成）。
  证明：`pytest tests/test_response_rules.py tests/test_generated_rules.py -q`
  与 `python tools/build_response_rules.py && git diff --exit-code data/mapping/response_rules.json`
