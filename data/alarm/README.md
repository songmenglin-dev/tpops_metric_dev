# 告警数据

TPOPS 平台的告警侧数据，与指标侧（`data/` 根目录）分开存放。

## 抓取产物（`tools/fetch_tpops_alarms.py`）

```bash
python tools/fetch_tpops_alarms.py
```

| 文件 | 来源 | 内容 |
|---|---|---|
| `manifest.json` | — | 出处记录：哪个 API 产生了哪个文件，含确切请求参数 |
| `statistic.json` | `GET /rds/v3/alarm/records/statistic` | 按等级（severity）统计的告警条数，分系统侧与实例侧 |
| `active.json` | `POST /rds/v3/alarm/records` | **活动告警**，收敛列表：每条 = 实例 + alarm_id 一行 |
| `active_detail.json` | 同上 | **活动告警**，明细行：每节点一行，含 `description` / `possible_causes` / `processing_steps` |
| `history.json` | 同上 | **历史告警**（已清除/已恢复），收敛列表 |
| `history_detail.json` | 同上 | **历史告警**，明细行 |

`POST /rds/v3/alarm/records` 的两个必传参数决定取哪种数据：

| 参数 | 取值 | 含义 |
|---|---|---|
| `is_active` | `true` / `false` | 活动告警 / 已清除或已恢复的历史告警 |
| `is_convergence` | `true` / `false` | 收敛列表（实例 + alarm_id 聚合）/ 明细行（逐节点，含原因与处置步骤） |

`limit` 上限 100，超出需分页。**历史告警仅保留 30 天**，停机超 30 天会丢告警，采集端日志要显式告警。

## 告警定义目录（手工整理，无抓取接口）

以下两个文件是平台内置的告警规则定义，从 CHM 产品文档整理而来，平台没有对应的查询接口：

| 文件 | 条数 | 内容 |
|---|---|---|
| `catalog_metric.json` | 85 | 指标类告警：告警ID、名称、归属、默认阈值与等级、震荡/恢复持续时间、关联指标、采集间隔 |
| `catalog_event.json` | 203 | 事件类告警：告警ID、名称、默认级别、告警类型、归属、是否内核告警、是否可自动清除 |

`catalog_metric.json` 每条另带三个匹配字段，用于把告警关联到平台指标：

| 字段 | 取值 | 含义 |
|---|---|---|
| `metric_id` | 指标英文 id | 匹配到的平台指标 |
| `match` | `exact` / `candidate` / `none` | `exact`=精确匹配，`candidate`=按名称推断的候选，`none`=未匹配 |
| `metric_id_candidates` | 指标英文 id 数组 | 推断出的候选，供人工确认 |

当前匹配情况：exact 21 条、candidate 8 条、none 56 条。**56 条未匹配**意味着这些告警无法直接对应到平台指标——做告警→指标关联时不能默认按 `metric_id` 取值，需人工确认。

## 与指标侧的关系

- 指标侧数据在 `data/` 根目录（`manifest.json`、`instances.json`、`customer_metric_mapping.json` 等）
- **按 `alarm_id` 关联**（平台自身给的，最可靠）：`GET /rds/v3/monitor/metrics/series` 返回的
  `metrics[].alarm_id` 与 `catalog_metric.json` 的「告警ID」是同一套 ID —— 实测 28 个平台指标带
  `alarm_id`，其中 23 个能在告警目录中找到同 ID 的告警定义。这条路径不依赖名称推断。
  注意 ID 空间不统一，既有 `200xxxxxxx` 也有 `50xxxxx` 两种形态。
- **按 `metric_id` 关联**（`catalog_metric.json` 里的匹配字段）：这是按告警名称推断出来的，
  `exact` 才可信，`candidate` 需人工确认。
- 告警目录 85 行对应 54 个唯一告警ID（同一告警可按不同指标配置多条规则）。
- 客户侧的告警字段规范**尚未确认**（见 CLAUDE.md 待定事项），当前这些文件是接口返回的
  原始结构，尚未做客户字段映射。
