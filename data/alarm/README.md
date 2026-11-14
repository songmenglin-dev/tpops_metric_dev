# 告警数据

TPOPS 平台的告警侧数据，与指标侧（`data/` 根目录）分开存放。

## 抓取产物（`tools/fetch_tpops_alarms.py`）

```bash
python tools/fetch_tpops_alarms.py
```

| 文件 | 来源 | 内容 |
|---|---|---|
| `000manifest.json` | — | 出处记录：哪个 API 产生了哪个文件，含确切请求参数 |
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

- 指标侧数据在 `data/` 根目录（`instances.json`、`customer_metric_mapping.json`、`customer_metric_query_plan.json` 等）
- **按 `alarm_id` 关联**（平台自身给的，最可靠）：`GET /rds/v3/monitor/metrics/series` 返回的
  `metrics[].alarm_id` 与 `catalog_metric.json` 的「告警ID」是同一套 ID —— 实测 28 个平台指标带
  `alarm_id`，其中 23 个能在告警目录中找到同 ID 的告警定义。这条路径不依赖名称推断。
  注意 ID 空间不统一，既有 `200xxxxxxx` 也有 `50xxxxx` 两种形态。
- **按 `metric_id` 关联**（`catalog_metric.json` 里的匹配字段）：这是按告警名称推断出来的，
  `exact` 才可信，`candidate` 需人工确认。
- 告警目录 85 行对应 54 个唯一告警ID（同一告警可按不同指标配置多条规则）。
- 客户侧的告警字段规范**已到**（`docs/ITSCADA第三方告警数据接入对接方案V2.docx`），
  字段级映射落地在 `data/mapping/alarm_rules.json`（等级/状态映射 + 257 条告警定义
  → 客户指标的挂载）；这些文件仍是接口返回的**原始结构**，供规则回放与回归用。

## 某条告警能否查到它对应的指标名

**结论：能 —— 用 `GET /gaussdb/v3/alarm/configs`（告警规则配置目录）。**

**注意：这个接口在 CHM 产品文档里没有记载**，只在 Web 控制台的前端代码里用。
它提供**平台权威的告警→指标映射**：每条规则带 `metric_code`（平台指标 id）、
阈值、观测策略、生效时段和中文告警名。

（`POST /rds/v3/alarm/records` 的响应里**确实没有任何指标字段** ——
`AlarmRecord` 只有 `alarm_id` / `alarm_name` / `alarm_type` / `alarm_category`。
但那只是"告警记录"接口，规则配置在另一个命名空间。）

### 接口与鉴权（踩坑记录）

```
GET https://{Endpoint}/gaussdb/v3/alarm/configs?limit=100&offset=0
```

**命名空间是 `/gaussdb/v3/`**（**不是** `/rds/v3/`，也不是 `/rds/gaussdb/v3/`）。

**四个必需 header，缺一个就报错（注意 404 与 403 的含义不同）：**

| header | 值 | 缺失时 |
|---|---|---|
| `service-tag` | `luban` | **404**（路由不到该路径） |
| `x-mini-token` | `gaussdbtpops`（**字面量**，不是 JWT！） | 403 |
| `cookie` | `session_timeout=30; X-Mini-Token=<控制台JWT>; isLogin=logged; userName=<user>; ts=<毫秒>` | 403 |
| `x-mini-csrf` | 登录时下发的 CSRF | 403 |

> ⚠️ `x-mini-token` 头里放 **JWT 会 404** —— 它是路由用的服务标识字面量。
> JWT 要放在 **cookie** 里。

**登录：`POST /gaussdb/v3/login`**（body 同 mini-token：`username` / `password`），
响应**头**里给两个东西：

- `X-Mini-Token` —— 控制台 JWT，claims 里 `login_type=CONSOLE`（对比
  `POST /rds/v3/auth/mini-token` 给的是 `login_type=OPENAPI`，**不能**用于本接口）
- `Csrf` —— CSRF 令牌，之后原样放进 `x-mini-csrf`

`x-language: zh-cn` 可选 —— **不加返回英文**（`alarm_name` = "Low Buffer Pool Hit Ratio"），
加了才是中文（"缓冲池命中率过低"）。`x-language: en-us` 同英文。

已封装为 `tools/tpops_client.py` 的 `console_login()` / `alarm_rule_configs()` /
`alarm_rule_catalog()`：

```python
from tpops_client import TpopsClient
rules, total = TpopsClient().alarm_rule_catalog()   # 自动登录 + 翻页
```

### 返回内容（181 条，2026-11-13 抓取留档在 `alarm_configs.json`）

`{total_count: 181, alarm_rule_config: [...]}`，`alarm_type` 分布 **event 148 / metric 33**，
**33 条 metric 类全部带 `metric_code`**（`metric_alarm_type` 全是 `single`，无 `multi_metric_config`）。

每条规则除 `metric_code` 外还有：`threshold_configs.default_threshold`
（critical/major/minor/notice 四级阈值）、`observation_policy`（持续时长 + 聚合函数）、
`effective_policy`（生效时段/星期）、`alarm_wait_time` / `recover_wait_time`、
`ignore_policy`（重复抑制）、`description` / `impaction` / `possible_causes` /
`processing_steps`（中英文随 `x-language` 切换）。

### 这份目录解决了什么

**它是告警→指标的权威映射，比反查表和离线目录都准：**

- 20 条与离线 `catalog_metric.json` 的 `metric_id` **完全一致**，**0 条冲突**
- 6 条离线标 `none`/`candidate` 的，平台给出了明确 `metric_code`
  （`5101162 → rds085_gaussv5_wait_ratio`、`2001010001 → sys065_disk_usage` 等）
- 补上了反查表查不到的 `2001060008` / `2001060009`（TPS 幅度告警，
  与 `2001050018` 共用 `rds400_gaussv5_total_counter`）

**但能挂到客户指标的仍然只有 3 条** —— 33 个 `metric_code` 里只有
`rds400_gaussv5_total_counter` 对应客户指标 `gaussdb.Tps`（对应
`2001050018` / `2001060008` / `2001060009` 三条告警）。
其余 30 条指标类告警 + 148 条事件类告警，都只能落到 `gaussdb.alarm.<alarm_id>` 兜底。
**这个结论与 `open_items.json` 的 `alarm-metric-binding` 一致，仍待客户确认。**

### 平台告警接口全集（2026-11-13 实测）

| # | 方法 | 路径 | 命名空间 | 实测 |
|---|---|---|---|---|
| 1 | POST | `/v3/alarm/notifications/interconnection/check` | /rds | 500 |
| 2 | POST | `/v3/alarm/notifications` | /rds | 400 `instanceIds cannot be empty` |
| 3 | PUT | `/v3/alarm/notifications` | /rds | 400 `instanceIds cannot be empty` |
| 4 | GET | `/v3/instances/{id}/alarm/notifications` | /rds | 200 |
| 5 | POST | `/v3/alarm/records` | /rds | 200 |
| 6 | GET | `/v3/alarm/records/statistic` | /rds | 200 |
| 7 | POST | `/v3/alarm/update/configs` | /rds | 400 `source_id is empty` |
| 8 | **GET** | **`/v3/alarm/configs`** | **/gaussdb** | **200（181 条）** |

第 8 个是 CHM 里**没有记载**的 —— CHM 全文 360 个 `/v3**` 路径中含 `alarm` 的只有 6 个。
**教训：CHM 路径提取 ≠ 平台接口全集**，结论必须以实际调用为准
（同 `data/tpops_api_catalog.json` 里 `api_survey.lesson` 记的那条）。

`POST /v3/alarm/update/configs`（"实例告警批量配置"）是**写操作**：
把模板实例的告警策略批量复制到其它实例（`source_id` + `target_instance_ids`，上限 1000），
**不返回任何配置内容**，采集端不该调用。

### 关联要走「反向查表」，两条路径

| 路径 | 做法 | 可信度 |
|---|---|---|
| **按 `alarm_id` + 规则配置目录（最佳）** | `GET /gaussdb/v3/alarm/configs` → 每条规则自带 `metric_code` | **最高** —— 平台权威，含阈值 |
| 按 `alarm_id` 反查指标目录 | 告警的 `alarm_id` → `GET /rds/v3/monitor/metrics/series` 的 `metrics[].alarm_id` → 指标英文 id + 单位 | 高 —— 两边都是平台给的同一套 ID |
| 按 `metric_id` | `catalog_metric.json` 的 `metric_id` 字段 | 中 —— 离线按名称推断，可能过时 |

反向表怎么建：`GET /rds/v3/monitor/metrics/series?name=<GROUP>&instance_id=<id>`，
12 个分组各取一遍，攒成 `alarm_id → (metric英文id, unit, group)`。
**注意 `instance_id` 必须传** —— 不传时平台把 `alarm_threshold` 留空、`alarm_id` 不给。

可运行示例：[`tools/demo_alarm_metric_mapping.py`](../../tools/demo_alarm_metric_mapping.py)

```bash
export TPOPS_USER=sml TPOPS_PASSWORD='...'
python tools/demo_alarm_metric_mapping.py            # 拉真实告警演示
python tools/demo_alarm_metric_mapping.py 2001050018 # 演示指定 alarm_id
```

### 实时反查的覆盖（2026-11-13 实测，gauss-6374）

指标目录共 **243** 个 metric，其中 **28** 个带 `alarm_id`，去重后 **23** 个 `alarm_id` 可反查。
**一个 metric 最多带 1 个 `alarm_id`**（实测无一对多），所以反查表是 `alarm_id → 单个 metric`。

23 条可反查的告警：

| alarm_id | 分组 | 平台指标 | 离线 match | 挂到的客户指标 |
|---|---|---|---|---|
| 2001010001 | IOSTORAGE | sys065_disk_usage | none | gaussdb.alarm.2001010001 |
| 2001010002 | CPUMEMORY / COREMETRICS | mem_usage | candidate | gaussdb.alarm.2001010002 |
| 2001010005 | CPUMEMORY / COREMETRICS | cpu_percent | candidate | gaussdb.alarm.2001010005 |
| 2001010009 | CPUMEMORY | cpu_wait_usage / io_wait_percent | candidate | gaussdb.alarm.2001010009 |
| 2001010012 | IOSTORAGE | sys081_svctm | exact | gaussdb.alarm.2001010012 |
| 2001010016 | IOSTORAGE | nas_usage | none | gaussdb.alarm.2001010016 |
| 2001050001 | PROCESSRESOURCE | rds090_gaussv5_buffer_hit_ratio | exact | gaussdb.alarm.2001050001 |
| 2001050010 | TRANSACTION / COREMETRICS | rds104_gaussv5_P95 | exact | gaussdb.alarm.2001050010 |
| 2001050012 | CONNECTION | rds086_gaussv5_active_ratio | exact | gaussdb.alarm.2001050012 |
| 2001050016 | CONNECTION | rds144_online_ratio | exact | gaussdb.alarm.2001050016 |
| **2001050018** | TRANSACTION | rds400_gaussv5_total_counter | exact | **gaussdb.Tps** |
| 2001050019 | TRANSACTION | rds096_gaussv5_resp_avg | exact | gaussdb.alarm.2001050019 |
| 2001050023 | SYNCSTAT | rds123_xlog_lsn | exact | gaussdb.alarm.2001050023 |
| 2001050024 | PROCESSRESOURCE / COREMETRICS | rds108_gaussdbv5_thread_pool | exact | gaussdb.alarm.2001050024 |
| 2001050026 | IOSTORAGE | sys041_sys_disk_usage | exact | gaussdb.alarm.2001050026 |
| 2001050027 | IOSTORAGE | sys023_file_handler_num | exact | gaussdb.alarm.2001050027 |
| 2001060003 | TRANSACTION | rds097_gaussv5_rollback_ratio | exact | gaussdb.alarm.2001060003 |
| 2001060010 | PROCESSRESOURCE | rds114_dynamic_used_memory_usage | exact | gaussdb.alarm.2001060010 |
| 2001060011 | NETWORK | database_rx_dropped | exact | gaussdb.alarm.2001060011 |
| 2001060012 | NETWORK | database_tx_dropped | exact | gaussdb.alarm.2001060012 |
| 5025234 | PROCESSRESOURCE | rds240_gaussdbv5_thread_pool_session_waiting | exact | gaussdb.alarm.5025234 |
| 5025235 | PROCESSRESOURCE | rds242_gaussdbv5_stream_max_connection | exact | gaussdb.alarm.5025235 |
| 5101162 | CONNECTION | rds085_gaussv5_wait_ratio | none | gaussdb.alarm.5101162 |

**反向查表比离线目录更准** —— 6 条离线标 `none`/`candidate` 的告警，实时能查到确切指标
（`5101162 → rds085_gaussv5_wait_ratio`、`2001010001 → sys065_disk_usage` 等），
说明 `catalog_metric.json` 的 `match` 是按名称推断的，**实时反查更可信，应优先用它**。

反过来，离线标 `exact` 但实时查不到的 4 条，是因为**一个 metric 只挂 1 个 alarm_id**：
`rds400_gaussv5_total_counter` 只挂 `2001050018`，所以 TPS 的另外两条
（`2001060008` 下降幅度过大、`2001060009` 上升幅度过大）反查不到 ——
**它们是挂在同一指标上的不同告警规则，反查表给不出，要靠 `metric_id` 那一列**。
`sys063_disk_used_size` 则完全不带 alarm_id（`2002010012` 同理查不到）。

### 但能挂到客户指标的极少

23 条可反查里，**只有 1 条**（`2001050018`）能挂到真实客户指标 `gaussdb.Tps`；
其余 22 条只能落到 `gaussdb.alarm.<alarm_id>` 兜底。

`alarm_rules.json` 的 257 条定义里：

| 分类 | 条数 | 说明 |
|---|---|---|
| `alarm_kind=metric` | 54 | 指标类告警 |
| `alarm_kind=event` | 203 | **事件类告警，本来就没有对应指标** |
| `linked_customer_metric` 非空 | **3** | 全部指向 `gaussdb.Tps`（2001050018 / 2001060008 / 2001060009） |
| 用 `gaussdb.alarm.<alarm_id>` 自造指标名 | **254** | 无真实指标可挂时的兜底 |

那 3 条里，`2001060008` / `2001060009` 靠 `metric_id`（同为 `rds400_gaussv5_total_counter`）
才挂得上 —— **反查表 + metric_id 两条路合起来才覆盖全部 3 条**。

### 采集端该怎么取 metricName / metricCode

`data/mapping/alarm_mapping.json` 已经定了口径（不是接口直接给的）：

- `metricName` ← `alarm_rules.json` —— 能对应客户指标的用客户指标中文名，其余用告警名
- `metricCode` ← `alarm_rules.json` —— 能对应的用客户指标英文名，其余用 `gaussdb.alarm.<alarm_id>`
- `value` ← 事件类取 `alarm_items_sum`；指标类取该客户指标最近一次采样值（取不到则 null，**不填 0**）

**这个"257 条里只有 3 条能挂到客户指标"的挂法本身仍是待客户确认项**
（`data/mapping/open_items.json` 的 `alarm-metric-binding`）：
事件类告警没有对应客户指标，用自造指标名挂载是否可接受，需要客户书面确认。

### 采集端实现建议

告警→指标的关联**要在启动时建一次表**，不要每条告警现查：

1. 启动时：12 个分组各调一次 `metrics/series`（带 `instance_id`），攒
   `alarm_id → (metric_id, unit)`，共 23 条；再叠上 `alarm_rules.json` 的
   `linked_customer_metric`（3 条）和 `catalog_metric.json` 的 `metric_id` 兜底。
2. 运行时：告警到达 → 查表 → 有客户指标就用客户的 `metricCode`/`metricName`，
   没有就用 `gaussdb.alarm.<alarm_id>` + 告警名。
3. **指标目录接口的 `metrics[]` 不带中文名** —— `metric_name` 只在
   `POST /rds/v3/monitor/metrics` 的数据响应里。中文名要走
   `catalog_metric.json` 的「指标」列，或用 `metric_rules.json` 的 `metric_name`。
