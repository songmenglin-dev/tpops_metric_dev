- [ ] **0.1 建包并复制映射层**：新建 `tpops_kafka_collector/` 包骨架（`__init__.py`、
  `mapping/__init__.py`、`errors.py`、`models.py`），
  把 `tools/mappings/{table,rules,decode,message,nodes}.py` **复制**进 `tpops_kafka_collector/mapping/`
  （本步不改逻辑，只搬家）。
  完成后包内有一份与 `tools/mappings/` 等价的实现。
  证明：`pytest tests/ -q` 全绿（此时两边都还在，测试指向不变）
