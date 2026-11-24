# wave-2b 任务 2.9 · E 类告警拉取与归并 —— 实施方报告（控制器代写）

> **原实施方在收尾时被 Token Plan 配额上限（429）打断**，代码与测试已落盘且
> 自测通过（19 例），但**没来得及写这份报告**。由控制器代写。

## 交付

| 文件 | 说明 |
|---|---|
| `tpops_kafka_collector/tpops/alarms.py` | 平台层（告警接口 + 分页 + 解析） |
| `tpops_kafka_collector/collectors/alarm.py` | 采集器（归并 + 节点回填 + 水位过滤） |
| `tests/integration/test_collector_alarm.py` | 19 例 |

## 实现要点（对照任务 2.9 的完成标准）

- `limit ≤ 100` **分页取全**
- **活动与历史各取收敛与明细**（`is_active` × `is_convergence` 四种组合）
- **明细按 `alarm_id + instance_id` 归并进 `details`**
- **收敛形态下不依赖平台侧 `alarm_id`/`instance_id` 过滤** —— 平台侧的收敛列表
  不带这两个字段可用，改**本地归并**
- `first_alarm_time` **水位增量**（水位由调度器推进，采集器只读）
- **30 天超期显式告警** —— 平台的历史告警仅保留 30 天，停机超期会丢告警
- **节点身份回填** —— 明细行自带 `node_id`/`host_ip`；收敛条目按 `alarm_id`
  关联明细行回填，回填不到落**第一个 master CN**
- **幂等键**复用 `ids.py` 的真相源（含 `status` 的平台原值）
- **`details` 恒为数组** —— 缺失时输出 `[]`，不省略该字段

## 验证

- `pytest tests/integration/test_collector_alarm.py -q` → **19 passed**
- `ruff check tpops_kafka_collector/` → 0
