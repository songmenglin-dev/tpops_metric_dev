- [ ] **1.7 映射表加载与启动校验**（W07）：新建 `tpops_kafka_collector/mapping/loader.py`
  加载**包内** `tpops_kafka_collector/mapping/data/*.json` 并做启动期四类校验（来源列存在于实测留档的
  实际返回列、可提供指标齐全、条目对应真实指标、占位符与维度绑定一一对应）。
  完成后校验全部在包内完成，运行时不需要仓库根目录。
  证明：`pytest tests/unit/test_mapping_loader.py -q`
