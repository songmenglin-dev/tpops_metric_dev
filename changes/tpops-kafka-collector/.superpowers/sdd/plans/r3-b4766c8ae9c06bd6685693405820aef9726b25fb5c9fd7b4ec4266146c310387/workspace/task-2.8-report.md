# wave-2b 任务 2.8 · D 类（实例元数据）—— 实施方报告（控制器代写）

> **原实施方被 Token Plan 配额上限（429）打断**，平台层落盘、采集器层没写完。
> 由控制器接管：补齐采集器层、补写报告。

## 交付

| 文件 | 说明 | 行数 |
|---|---|---|
| `tpops_kafka_collector/tpops/instances.py` | D 类平台层（实例列表拉取与解析） | 203 |
| `tpops_kafka_collector/collectors/instance.py` | D 类采集器（1 个客户指标） | 216 |
| `tests/integration/test_collector_instance.py` | 18 例 | 344 |

## 1 个客户指标

`gaussdb.server_version` —— 取值是实例的 `engine_version`（如 `V2.0-10.0.0`）。

## 四条硬约束（都有测试守卫）

1. **接口路径是 `/rds/v3.2/instances`** —— 不是 `v3`（少一段前缀就 404）。
2. **只取目标实例** —— 平台上有两个实例（`83cd68f2…in14` 分布式版 /
   `c35b8d07…in14` 集中式版），本项目只覆盖前者；找不到目标实例就**不产出消息**
   （不猜、不取第一条）。
3. **D 类与节点无关** → 资源名落到节点清单里的**第一个 master CN**
   （`{host_ip}_{node_id}`），保证跨轮稳定。
4. **档位由映射表的 `collect_cycle` 推导**（表驱动，不写死）—— 改表就改档，不改代码。

## 缺值不产出消息（规格「缺值不产出假数据」）

三种情况都**不产出消息**并记 **ERROR** 日志，绝不静默出 `0`、不发 null 值：

- 实例列表为空
- 找不到目标实例
- `engine_version` 取不到（字段缺失或为空串）

## 控制器补的部分

原实施方只完成了平台层（`tpops/instances.py`），采集器层（`collectors/instance.py`）
与集成测试是控制器补的：补齐 `collect()` 主流程、三种缺值分支、
资源名落到第一个 master CN 的解析、以及档位推导（`collect_cycle_seconds` /
`collect_cycle_label` 两个纯函数，便于单测）。

## 验证

- `pytest tests/integration/test_collector_instance.py -q` → **18 passed**
- `ruff check tpops_kafka_collector/` → 0
- 全量（补交后）→ **1262 passed**
