- [ ] **0.3 在包内修告警幂等键**：在包内定义 `AlarmRecord.idempotency_key`（键为
  `(alarm_id, instance_id, first_alarm_time, status)`）；
  改 `tools/build_itscada_mapping.py` 的 `build_alarm_rules()`，在 `alarm_rules.json`
  增加 `idempotency` 段；删掉 `tools/build_response_rules.py` 里从未被写出的死代码
  `IDEMPOTENCY['alarm']`。完成后同一告警的触发与恢复是两个不同的键。
  证明：`pytest tests/test_rules.py -q -k idempotency`
