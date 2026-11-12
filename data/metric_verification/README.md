# 逐指标实测留档

98 个客户指标逐条真实调用 TPOPS 平台、验证取值通道可用性后落盘的记录。
**回归样本**，不是运行时输入 —— 采集端不读这个目录。

## 每个文件

| 段 | 内容 |
|---|---|
| `customer_metric` / `customer_metric_name` / `customer_category` | 客户指标标识与中文名 |
| `tpops_metric` / `tpops_metric_name` | 对应的平台指标 |
| `status` / `match_level` / `source` / `note` | 可提供性判定与匹配程度 |
| `customer_spec` | 客户指标表里的【数据类型】【数据更新周期】【单位】 |
| `query` | 该指标的查询方法：接口、请求体、预期结果；命令中心类还带 `flow` 与 `apis`（四跳） |
| `verification` | 实测结果：`tested_at` / `state` / `row_count` / `error` / `actual_data` |

## 重抓

```bash
export TPOPS_USER=... TPOPS_PASSWORD=...
python tools/verify_metric_sources.py            # 默认落盘到本目录
```

## 三条必须知道的注意事项

### 1. `query` 里的 `sql_text` 是**合并前**的形态，不要照抄

82 个命令中心指标各自带一条独立 SQL（去重后 **50 条**），
而采集端实际下发的是 **`data/merged_sql/merged_sql.json` 的 11 条合并 SQL**
（把下发次数从 82 降到 11，否则一轮要 3~7 分钟）。
**SQL 的唯一来源是 `data/merged_sql/merged_sql.json`**，不是本目录。

### 2. `verification.actual_data` 是**脚本重塑后的中间结构**，不是平台原生响应

监控指标通道尤其要注意：`tools/verify_metric_sources.py` 的 `run_monitor()` 把平台响应
拍平，并把平台的 `metric_name` 字段改名成了 `metric_name_cn` —— **`metric_name_cn` 是脚本自造的字段名，
平台并不返回它**。平台原生响应在 `data/metric_response/`（含完整 request 对象）。

真实层级是 `metric_infos[] → metrics[]`，`collection_frequency` 在 `metrics[]` 上。

### 3. 留档里的 `note` 含**已被证伪的旧结论**

部分记录里写着「一次下发多节点会失败，须逐节点下发」—— 这条结论**已被 2026-11-06 的实测证伪**：
一次下发全部 master 节点是成功的（HTTP 200，多条 node_tasks 全部 succeed）。
正确结论与证据见 [`../merged_sql/README.md`](../merged_sql/README.md) 的「多 master：一次下发全部 master」。
