- [ ] **1.6 TPOPS HTTP 客户端**（W06）：新建 `tpops_kafka_collector/tpops/client.py`，mini-token 认证
  （token 从响应头取）、401 强制刷新重试一次、`verify_tls=False` 默认、错误四分类。
  完成后认证失败不发起第二次认证。
  证明：`pytest tests/unit/test_client.py -q`
