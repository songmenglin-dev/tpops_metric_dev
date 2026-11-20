- [ ] **1.3 重试与退避框架**（W03）：新建 `tpops_kafka_collector/retry.py`，指数退避 + 抖动，
  重试判定集中于此。完成后 `AuthError`/`ProtocolError`/`MappingError`（含 `DecodeError`）
  立即上抛，`TransientError`/`ProduceError`/超时按 1s→2s→4s→8s 重试。
  证明：`pytest tests/unit/test_retry.py -q`
