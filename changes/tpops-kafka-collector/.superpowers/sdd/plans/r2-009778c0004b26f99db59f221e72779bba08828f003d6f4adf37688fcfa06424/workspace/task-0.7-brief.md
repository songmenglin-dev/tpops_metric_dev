- [ ] **0.7 把依赖切到包内并删除参考副本**：把 `tools/` 下 10+ 个脚本与 `tests/` 下 8 个
  测试文件的 `import tools.mappings` 全部改为 `import tpops_kafka_collector.mapping`；
  `tools/` 脚本的 `sys.path` 需同时加入项目根；**删除 `tools/mappings/`**（git 历史保留）；
  加一条测试断言 `tpops_kafka_collector/` 下无任何指向 `tools` / `data` 的 import。
  完成后仓库里只剩一份映射层实现，且生产包自洽。
  证明：`pytest tests/ -q` 全绿 + `pytest tests/unit/test_package_boundary.py -q`

### 波次 1 —— 骨架与平台层（W01~W09）

