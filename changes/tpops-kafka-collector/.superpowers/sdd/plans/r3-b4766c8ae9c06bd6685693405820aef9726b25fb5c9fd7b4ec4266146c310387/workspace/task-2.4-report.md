# wave-2b 任务 2.4 · A 类采集器 —— 实施方报告（控制器代写）

> **原实施方在收尾时被 Token Plan 配额上限（429）打断**，代码与测试已落盘且
> 自测通过（21 例），但**没来得及写这份报告**。由控制器根据落盘的代码与测试代写。

## 交付

| 文件 | 说明 |
|---|---|
| `tpops_kafka_collector/collectors/monitor.py` | A 类采集器 |
| `tests/integration/test_collector_monitor.py` | 21 例 |

## 实现要点（对照任务 2.4 的完成标准）

- **多 series 按组件逐条产出** —— 客户指标是实例级，平台按组件给多条 series，
  现状口径是**逐组件各出一条消息**（归集方式属业务口径，待客户确认，
  见 `open_items.json` / `a-class-multi-series`）
- **维度键 `component_id` 归入 `tags`** —— 不是信封字段（信封只有 7 个字段）
- **空分组记告警日志** —— 分组码写错时平台返回 200 + 空 series **且不报错**，
  只能靠这条日志发现
- **null 点不是 0** —— 平台未入库时值为 null，跳过而不是当 0
- **窗口与分组码复用 `tpops/metrics.py`**（任务 2.3），不在本模块重写一份

## 验证

- `pytest tests/integration/test_collector_monitor.py -q` → **21 passed**
- `ruff check tpops_kafka_collector/` → 0
