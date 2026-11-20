- [ ] **1.9 水位与差分基线持久化**（W09）：新建 `tpops_kafka_collector/watermark.py`，
  写临时文件 + `os.replace` 的原子写，保存指标水位、差分基线、告警水位、幂等键 LRU。
  完成后进程在写文件途中被杀不会留下半截 JSON。
  证明：`pytest tests/unit/test_watermark.py -q`

### 波次 2a —— 映射数据与 SQL 渲染（W13）

### 波次 2b —— 五条采集通道（W10~W18）

