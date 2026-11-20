- [ ] **1.1 配置加载与启动校验**（W01）：新建 `tpops_kafka_collector/config.py`，从环境变量加载
  §5.5 全部配置项，校验必填项与数值范围（窗口 ≥1、并发 1~10、页容量 ≤1000），
  缺失即快速失败且不发任何请求。完成后 `CMD_CONCURRENCY` 默认 2、`CMD_PAGE_SIZE` 默认 100。
  证明：`pytest tests/unit/test_config.py -q`
