# wave-2b 任务 2.6 · B 类采集器 —— 实施方报告（控制器代写）

> **原实施方在开工初期就被 Token Plan 配额上限（429）打断**，连测试都没写。
> 由控制器从零实现（代码 + 测试）。

## 交付

| 文件 | 说明 |
|---|---|
| `tpops_kafka_collector/collectors/command_center.py` | B 类采集器（82 个指标） |
| `tests/integration/test_collector_command_center.py` | 18 例 |

## 五个硬约束（都有测试守卫）

1. **走 `decode_merged` / `merge_pages`** —— 合并形态当行列表用，
   82 个指标会**静默全变 0** 且不报错
2. **检查 `outcome.ok`** —— 命令中心失败**不抛异常**而是返回 `SqlOutcome`
3. **分页只对可增长视图生效** —— 固定视图每页重复返回全量，累加会翻倍
4. **`node_scoped` 归集** —— `true` 各 master 各自一份，`false` 只取一个 master
5. **输出上限 102400 B** —— 90% 记 WARNING，超了缩小页容量重试

## 实现中发现并改掉的三个真问题

### 1. 值与资源名错配（Important）

`node_scoped = false` 的指标原本「**值**取自 outcome 里的第一个 master，
**资源名**却落到节点清单里的第一个 master CN」—— 两者不是同一个节点时，
值属于 A 节点、资源名写着 B 节点。改为**值和资源名同源**（都取承载资源名的那个 master）；
承载节点不在结果里时记 WARNING 而不是静默错配。

### 2. 平台不认 offset 会死循环（Important）

平台若忽略 `offset`（每页返回同一批行），采集器会**无限翻页**把整轮卡死。
加了**同页指纹守卫**（hashlib 指纹，不用 `json.dumps`），认出后停下并记 WARNING。

### 3. `json.dumps` 越界（Important）

最初用 `json.dumps` 算页指纹和结果体积 —— 违反了「生产包里产出消息的
`json.dumps` 只有 `kafka/serializer.py` 一处」的 AST 守卫。改为 hashlib 指纹 +
近似体积估算，**不放宽守卫**。

## 验收判据

**值等于期望值表**（不是「非零」判据）：82 条逐条断言，期望值来自
`data/mapping/expected_values.json`（**独立推导器** `tools/expected_deriver.py`
从实测留档原始行推出）。比较时按客户表【数据类型】定型 —— 期望值表存的是原始值，
消息里是定型后的值。

## 验证

- `pytest tests/integration/test_collector_command_center.py -q` → **18 passed**
- 全量 → **1010 passed**；`ruff check tpops_kafka_collector/` → 0
