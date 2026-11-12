# TPOPS tools

Fetch monitoring data for every GaussDB instance visible on the TPOPS platform.

> **API 参考文档：[`docs/api/`](../docs/api/)** — 从 CHM 产品文档转换而来的接口定义（原始内容，未做增补）。索引见 [`docs/README.md`](../docs/README.md)。

## Setup

```bash
cp .env.example .env
# edit .env: set TPOPS_PASSWORD
set -a && source .env && set +a
```

The platform uses a self-signed certificate, so TLS verification is off by default
(`TpopsClient(verify_tls=False)`).

## Usage

```bash
python tools/fetch_tpops_metrics.py --hours 24
```

## API endpoints used

All paths are under `/rds/` — the `/gaussdb/` prefix in the docs applies to the SPA
web console, not to the API.

| Purpose | Method | Path |
|---|---|---|
| Get token | POST | `/rds/v3/auth/mini-token` |
| List instances | GET | `/rds/v3.2/instances` |
| Metric catalogue | GET | `/rds/v3/monitor/metrics/series?name=<GROUP>` |
| Metric data | POST | `/rds/v3/monitor/metrics` |
| Alarm list | POST | `/rds/v3/alarm/records` |
| Alarm severity counts | GET | `/rds/v3/alarm/records/statistic` |

The token comes back in the `X-Mini-Token` **response header** and must be sent as
the `X-Mini-Token` request header on every other call. Server-side `session_timeout`
is 1440 minutes; the client fetches a fresh token per run.

## Alarms

`python tools/fetch_tpops_alarms.py` — fetches alarm data into `data/alarm/`.
See `data/alarm/README.md` for what each file holds.

| API | Purpose |
|---|---|
| `POST /rds/v3/alarm/records` | 告警结果列表 — the alarm data query API |
| `GET /rds/v3/alarm/records/statistic` | 告警数量查询 — counts per severity |

`POST /rds/v3/alarm/records` body:

| Field | Required | Meaning |
|---|---|---|
| `is_active` | yes | `true` = active alarms, `false` = recovered/cleared history |
| `is_convergence` | yes | `true` = grouped list (one row per instance+alarm_id), `false` = detail rows (one per node, with `description`, `possible_causes`, `processing_steps`) |
| `limit` | no | 1–100, default 100 |
| `offset` | no | default 0 |
| `alarm_id` | no | only when `is_convergence=false`; combine with `instance_id` |
| `instance_id` | no | only when `is_convergence=false`; combine with `alarm_id` |

Response: `records[]` plus `total_count`. Key fields — `severity`
(`notice`/`minor`/`major`/`critical`), `status` (`activated`/`cleared`/`recovered`),
`alarm_id`, `alarm_name`, `alarm_type` (`metric`/`event`/`system-metric`/`system-event`/`system`),
`instance_id`, `instance_name`, `first_alarm_time`, `last_alarm_time`, `alarm_duration`.

Historical alarms are retained for **30 days**.

The CHM product guide also documents alarm *integration* APIs — `POST`/`PUT
/rds/v3/alarm/notifications`, `GET /rds/v3/instances/{id}/alarm/notifications`,
`POST /rds/v3/alarm/notifications/interconnection/check` and
`POST /rds/v3/alarm/update/configs`. These configure how alarms are pushed out
(SNMP/Syslog/HTTP(S)); they are **not** used for reading alarm data.

## Metric groups

`CPUMEMORY`, `CONNECTION`, `SYNCSTAT`, `PROCESSRESOURCE`, `NETWORK`, `TRANSACTION`,
`IOSTORAGE`, `LOCK`, `INSTATUS`, `FORDR`, `AGENTSTATUS`, `COREMETRICS`

## Customer metric mapping & verification

Three scripts produce the customer-facing deliverables in `data/`:

| Script | Output | Purpose |
|---|---|---|
| `build_customer_metric_mapping.py` | `data/customer_metric_mapping.json` | 客户 101 个指标 → TPOPS 可提供性判定与映射关系 |
| `build_metric_query_plan.py` | `data/customer_metric_query_plan.json`、`data/tpops_api_catalog.json` | 逐指标的查询方案与 API 目录 |
| `verify_metric_sources.py` | `data/metric_verification/*.json` | **逐指标真实调用**，通过后才落盘 |
| `build_metric_value_rules.py` | `data/metric_value_rules.json` | 开发方案 §4.3 的逐指标取值规则 → 机器可读 |
| `build_itscada_mapping.py` | `data/mapping/*.json` | ITSCADA 映射规则（字段映射 / 指标规则 / 告警规则） |
| `capture_metric_response.py` | `data/metric_response/*.json` | **逐指标抓原生 API 响应体**（一个指标一个文件，命令中心走 11 条合并 SQL） |
| `build_metric_sql_column.py` | 客户指标明细表 xlsx | 回填「指标库-指标获取方法」sheet 的「使用的SQL」列 |

```bash
python tools/build_customer_metric_mapping.py   # 需要先建 tpops_all_metric.json
python tools/build_metric_query_plan.py
python tools/verify_metric_sources.py           # 需 TPOPS_USER / TPOPS_PASSWORD
python tools/capture_metric_response.py         # 需 TPOPS_USER / TPOPS_PASSWORD
```

### ITSCADA 映射层

`tools/mappings/` 是规则执行代码（加载查表、取值规则引擎、消息组装），
规则数据在 `data/mapping/`。用法见 [`tools/mappings/README.md`](mappings/README.md)。

```python
import sys
sys.path.insert(0, 'tools')
from mappings import MappingTable

table = MappingTable.load()   # 默认读 data/mapping/
```

`build_metric_query_plan.py` imports the API catalogue from `tools/tpops_api_defs.py`
(data only — no logic), keeping both files under the size limit.

### The command center is an async three-hop

82 of the 101 customer metrics are pg_stat-style values that the platform does **not**
expose as monitoring metrics. They are fetched by running SQL through the platform's
built-in command `general_single_sql_query` (`cmd_id`
`73722acc-bff6-4d04-adc0-bc1e1fa1eb7d`, `source=default`, created by platform admin).
Its template is `select * from ({sql_text}) limit {limit};` — **the platform supplies the
executor, we supply the SQL**.

The platform is an async job model, so one metric needs three calls:

| # | API | Purpose |
|---|---|---|
| 1 | `POST .../customer-commands/{cmd_id}` | submit the SQL → returns `job_id` + `task_id` (no data) |
| 2 | `GET .../batch-exec-job/task-records?job_id=` | poll until `statistics.running == 0` |
| 3 | `GET .../batch-exec-job/{job_id}/task-record/{task_id}/detail` | fetch the data |

Gotchas confirmed by live measurement:

- **`job_status` is unreliable** — node tasks are already `succeed` while `job_status`
  still reads `running`. Use `statistics.running == 0`.
- **`result` is a JSON string** — it needs a second `json.loads`.
- Column names come from the SQL `as` aliases, so custom SQL must alias explicitly.
- Commands run on **master nodes only**; submitting to multiple nodes at once fails.
- One round trip takes 3–8 s. 82 metrics need only 50 distinct SQL statements.
- GaussDB's dialect is PG 9.2 level: no `FILTER` (use `CASE WHEN`), and
  `state <> ''` is always false (use `state is not null` to exclude background
  processes).

See `data/metric_verification/README.md` for the shape of the per-metric records,
including the persisted `job_id`/`task_id` (which can be used to re-fetch the original
result from the platform).

## Output (`data/`)

All outputs are JSON — each file is the raw JSON body returned by one API call.

| File | Source API | Contents |
|---|---|---|
| `manifest.json` | — | provenance: which API produced which file, with the exact request parameters |
| `instances.json` | `GET /rds/v3.2/instances` | instance list; `instances[].id` feeds the metrics calls |
| `metric_catalog.json` | `GET /rds/v3/monitor/metrics/series` | metric catalogue per group (12 groups, one request); `metrics[].name` feeds `metric_names` |
| `metrics_latest.json` | `POST /rds/v3/monitor/metrics` | current snapshot (`is_latest_point_only: "true"`) |
| `metrics_series_<N>m.json` | `POST /rds/v3/monitor/metrics` | current values over a short window (`is_latest_point_only: "false"`) — **use this for "current value"** |
| `metrics_series_<N>h.json` | `POST /rds/v3/monitor/metrics` | long time series (`is_latest_point_only: "false"`) — default `--hours 2` |

The default run produces three data files:

| File | Window | Size | Covers |
|---|---|---|---|
| `metrics_series_5m.json` | 5 min | ~520 KB | fast metrics only (375 of 423) |
| `metrics_series_2h.json` | 2 h | ~4.6 MB | **all 423 metrics**, timestamps intact |
| `metrics_latest.json` | snapshot | ~215 KB | all values, but 74% have `timestamp=0` |

Pass `--hours 24` for a full-day series (11 MB) when you need trends.

These exports are all **reproducible from the platform** and are therefore gitignored:
`metrics_series_*.json`, `metrics_latest.json`, `metric_catalog.json`, `manifest.json`
(the provenance record for the fetch itself), plus the two files derived from them
(`tpops_all_metric.json`, `metric_names.csv`, see below).
`instances.json` is kept as a committed baseline.

### Rebuilding the metric dictionary

`data/tpops_all_metric.json` (234 platform metrics: english id / chinese name / unit /
group / collection frequency) and `data/metric_names.csv` (the same data as a table) are
built from the raw exports above:

```bash
python tools/fetch_tpops_metrics.py --hours 2
python tools/build_tpops_metric_dict.py
```

`build_customer_metric_mapping.py` needs `tpops_all_metric.json` to resolve the
platform-side metric names, so run the dictionary build first if it is missing.

### Which file to use for "current value"

Use `metrics_series_<N>m.json` (default 5 min), not `metrics_latest.json`.

`is_latest_point_only: "true"` returns `timestamp=0` for ~74% of metrics, so the values
are there but you cannot tell when they were collected. A short window with
`is_latest_point_only: "false"` returns real timestamps for every metric.

The API picks the resolution from the window (`time_interval` in the response):
300 s for a 24 h window, 10 s for a 5 min window. Point count per metric follows
`collection_frequency` — 10 s → 30 points, 30 s → 10, 60 s → 5, 300 s → 1.

### Window size vs. collection interval

A metric only produces points if the window is at least as long as its
`collection_frequency`. Metrics that collect hourly are therefore missing from short
windows. Measured against the 21 hourly metrics (`collection_frequency=3600`):

| Window | `time_interval` | Hourly metrics covered |
|---|---|---|
| 5 min | 10 s | 0 / 21 |
| 30 min | 10 s | 15 / 21 (partial) |
| **60 min** | 10 s | **21 / 21** |
| 120 min | 60 s | 21 / 21 |
| 240 min | 60 s | 21 / 21 |

So the minimum window that covers every metric is **1 hour**; the default 2 h window
leaves margin over that edge and still covers all 423 metrics.

`time_interval` (the resolution the API picks for the window) is capped independently
of `collection_frequency`: 10 s for windows up to 1 h, 60 s from 2 h, 300 s at 24 h.
So a 24 h window downsamples the 10 s metrics to 5-minute resolution — use a shorter
window when you need to see 10-second-level variation.

### Ingestion lag — why some points are null

The platform does not publish data in real time: **the newest ~5 minutes have not been
ingested yet**. The response grid's last slot also sits one `time_interval` before
`end_time`. Combined, the newest data available is:

```
frontier = min(end_time - time_interval, wall_clock - ~5 min)
```

Querying up to "now" therefore leaves an empty tail. Measured null rates:

| Window | `end_time` | Null rate |
|---|---|---|
| 5 min | now | **77.5 %** |
| 5 min | now − 5 min | 1.6 % |
| 2 h | now | 5.2 % |
| 2 h | now − 5 min | 1.5 % |

The fetch script compensates automatically: `end_time` is shifted back by `--lag`
minutes (default 5). Pass `--lag 0` to query the raw, partly-empty tail.

Note the interaction with window length — a 5-minute window is *shorter than the
ingestion lag*, so without the shift almost the entire response is null. That is why
the short file is nearly useless without compensation.
