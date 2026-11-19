# data/ —— 实测留档与映射规则数据

本目录装两类东西：

1. **实测留档**（`alarm/`、`command_center/`、`merged_sql/`、`metric_response/`、`instances.json`）——
   平台真实调用的响应体留档，是「实测」二字唯一可追溯的依据。
2. **映射规则数据**（`mapping/`）—— **表驱动**：字段口径写在数据文件里，代码不写死。

> 目录 `data/metric_verification/`、`data/command_center/results/`、`data/api_probe/`、
> `data/multi_master_probe/` 已于 2026-11-13 按「一次性探针产物」口径删除（git 历史可恢复）；
> 等价内容在 `metric_response/` 与 `command_center/`。

## 本目录的文件

| 文件 | 作用 | 生成者 / 使用者 | 能不能删 |
|---|---|---|---|
| `customer_metric_mapping.json` | **101 个客户指标 → TPOPS 可提供性判定**（98 可提供 / 3 无法提供），含每个指标的口径说明（直接对应/近似对应/需换算） | 生成：`tools/build_customer_metric_mapping.py`；用：`tools/build_metric_query_plan.py` | 不能 |
| `customer_metric_unsupported.csv` | **3 个无法提供的指标清单 + 理由**（客户书面清单用） | 生成：`tools/build_customer_metric_mapping.py` | 不能 |
| `customer_metric_query_plan.json` | 101 条逐指标查询方案（走哪条通道、哪个接口、哪条 SQL） | 生成：`tools/build_metric_query_plan.py`。**中间产物，已 gitignore**，缺失时自动重建 | 能（可重建） |
| `metric_value_rules.json` | 从开发方案 §4.3 逐指标表抽出的取值规则 | 生成：`tools/build_metric_value_rules.py`。**中间产物，已 gitignore**，缺失时自动重建 | 能（可重建） |
| `instances.json` | 目标实例的元数据（实例 ID、名称、节点清单） | 用：`tools/build_kafka_mapping.py`、`tools/tpops_client.py` | 不能 |
| `tpops_api_catalog.json` | TPOPS API 目录（URI、入参、出参、指标目录） | 用：`tools/tpops_api_defs.py` | 不能 |

## 子目录

| 目录 | 内容 | 说明 |
|---|---|---|
| [`mapping/`](mapping/) | **映射规则数据** —— 采集端落地时要搬进 `tpops_kafka/` 的那一层 | 见 [`mapping/README.md`](mapping/README.md) |
| [`alarm/`](alarm/) | 告警侧留档（活动/历史 × 收敛/明细、告警定义目录、181 条规则配置） | 见 [`alarm/README.md`](alarm/README.md) |
| [`command_center/`](command_center/) | 统一命令中心的内置命令清单（142 条）+ 通用 SQL 命令详情 | 见 [`command_center/README.md`](command_center/README.md) |
| [`merged_sql/`](merged_sql/) | 11 个视图的合并 SQL、分页协议、实跑留档 | 见 [`merged_sql/README.md`](merged_sql/README.md) |
| [`metric_response/`](metric_response/) | **逐指标原生 API 响应体留档（98 个文件）—— 本项目最大的测试资产** | 见 [`metric_response/README.md`](metric_response/README.md) |

## 约定

- **留档文件不改**。它们是证据，改了就失去证据价值；平台变了就重新抓一份新的。
- **中间产物不入库**。`customer_metric_query_plan.json` 与 `metric_value_rules.json` 由生成器重建。
- **映射表由生成器产出**，禁止逐格手改（方案 §7.1.1 的硬要求）。
