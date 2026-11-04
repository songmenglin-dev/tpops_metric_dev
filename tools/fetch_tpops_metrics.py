#!/usr/bin/env python3
"""Fetch all TPOPS instance metric data and write it to data/ as JSON.

Every output file is the (raw, unmodified) JSON body returned by one TPOPS API
call. `data/manifest.json` records which API produced which file and with what
request parameters.

Usage:
    export TPOPS_USER=sml TPOPS_PASSWORD='...'
    python tools/fetch_tpops_metrics.py [--hours 24]

Outputs (under data/):
    manifest.json          provenance: file -> API call + request parameters
    instances.json         raw return of GET  /rds/v3.2/instances
    metric_catalog.json    merged return of GET /rds/v3/monitor/metrics/series
    metrics_latest.json    raw return of POST /rds/v3/monitor/metrics (latest point)
    metrics_series_<N>h.json  raw return of POST /rds/v3/monitor/metrics (time series)
"""

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tpops_client import METRIC_GROUPS, TpopsClient  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")


def write_json(name, payload):
    path = os.path.join(DATA_DIR, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    size_kb = os.path.getsize(path) / 1024
    print(f"  wrote data/{name} ({size_kb:.0f} KB)")
    return path


def main():
    ap = argparse.ArgumentParser(description="Fetch TPOPS metric data for all instances")
    ap.add_argument("--hours", type=int, default=2,
                    help="time-series window in hours (1h is the minimum that covers every metric)")
    ap.add_argument("--minutes", type=int, default=5,
                    help="short time-series window in minutes (current values, with real timestamps)")
    ap.add_argument("--lag", type=int, default=5,
                    help="minutes to subtract from end_time to compensate for the platform's "
                         "ingestion lag (the newest ~5 min of data has not landed yet)")
    args = ap.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)
    client = TpopsClient()
    manifest = {}

    # --- GET /rds/v3.2/instances -----------------------------------------
    print("fetching instances ...")
    inst_body = client.instances()
    instances = inst_body.get("instances", [])
    instance_ids = [i["id"] for i in instances]
    print(f"  {inst_body.get('total_count')} instance(s): "
          + ", ".join(f"{i['name']} ({i.get('mode')}, {i.get('instance_status')})" for i in instances))
    write_json("instances.json", inst_body)
    manifest["instances.json"] = {
        "api": "查询数据库实例列表/查询实例详情（v3.2）",
        "method": "GET",
        "url": f"{client.base_url}/rds/v3.2/instances",
        "query": {"limit": 100, "offset": 0},
        "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
        "response": "instances[].id -> used as instance_id for the metrics calls",
    }

    # --- GET /rds/v3/monitor/metrics/series (one call per group) ---------
    print("fetching metric catalogue ...")
    catalog = client.metric_catalog()
    all_names = sorted({m["name"] for ms in catalog.values() for m in ms})
    print(f"  {sum(len(v) for v in catalog.values())} entries, {len(all_names)} unique metrics")
    write_json("metric_catalog.json", catalog)
    manifest["metric_catalog.json"] = {
        "api": "查询监控指标分组",
        "method": "GET",
        "url": f"{client.base_url}/rds/v3/monitor/metrics/series",
        "query": {"name": METRIC_GROUPS},
        "note": "`name` is repeatable -- all 12 groups are fetched in a single request",
        "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
        "response": "series[].metrics[].name -> used as metric_names for the metrics calls",
    }

    # The platform has an ingestion lag: data collected in the last ~5 minutes has
    # not landed yet. Querying up to "now" therefore returns an empty tail, and on a
    # short window most of the response is null. Shift end_time back by --lag so the
    # window covers fully-ingested data.
    now_ms = int(time.time() * 1000) - args.lag * 60 * 1000
    window_ms = args.hours * 60 * 60 * 1000
    start_ms, end_ms = now_ms - window_ms, now_ms

    metrics_body = {
        "start_time": start_ms,
        "end_time": end_ms,
        "is_latest_point_only": "<varies>",
        "instances": [{"instance_id": i} for i in instance_ids],
        "metric_names": f"<all {len(all_names)} metric names from metric_catalog.json>",
    }

    # --- POST /rds/v3/monitor/metrics (latest point only) ----------------
    print("fetching current snapshot ...")
    latest = client.metrics(instance_ids, all_names, start_ms, end_ms, latest_only=True)
    write_json("metrics_latest.json", latest)
    manifest["metrics_latest.json"] = {
        "api": "查询监控指标",
        "method": "POST",
        "url": f"{client.base_url}/rds/v3/monitor/metrics",
        "body": {**metrics_body, "is_latest_point_only": "true"},
        "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
    }

    # --- POST /rds/v3/monitor/metrics (time series) ----------------------
    print(f"fetching {args.hours}h series ...")
    series = client.metrics(instance_ids, all_names, start_ms, end_ms, latest_only=False)
    write_json(f"metrics_series_{args.hours}h.json", series)
    manifest[f"metrics_series_{args.hours}h.json"] = {
        "api": "查询监控指标",
        "method": "POST",
        "url": f"{client.base_url}/rds/v3/monitor/metrics",
        "body": {**metrics_body, "is_latest_point_only": "false"},
        "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
    }

    # --- POST /rds/v3/monitor/metrics (short window, real timestamps) -----
    # A short window with is_latest_point_only=false gives the current values
    # *with* usable timestamps -- unlike is_latest_point_only=true, which returns
    # timestamp=0 for most metrics.
    short_ms = args.minutes * 60 * 1000
    short_start_ms, short_end_ms = now_ms - short_ms, now_ms
    print(f"fetching {args.minutes}m series ...")
    short = client.metrics(instance_ids, all_names, short_start_ms, short_end_ms, latest_only=False)
    short_name = f"metrics_series_{args.minutes}m.json"
    write_json(short_name, short)
    manifest[short_name] = {
        "api": "查询监控指标",
        "method": "POST",
        "url": f"{client.base_url}/rds/v3/monitor/metrics",
        "body": {
            **metrics_body,
            "start_time": short_start_ms,
            "end_time": short_end_ms,
            "is_latest_point_only": "false",
        },
        "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
        "note": "current values over a short window; timestamps are populated "
                "(unlike metrics_latest.json, where 74% come back as 0)",
    }

    # --- provenance ------------------------------------------------------
    manifest["_meta"] = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "base_url": client.base_url,
        "ingestion_lag_minutes": args.lag,
        "ingestion_lag_note": "end_time is shifted back by this many minutes; data newer "
                              "than that has not been ingested by the platform yet",
        "time_window": {
            "start_time_ms": start_ms,
            "end_time_ms": end_ms,
            "hours": args.hours,
            "start_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_ms / 1000)),
            "end_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_ms / 1000)),
        },
        "short_time_window": {
            "start_time_ms": short_start_ms,
            "end_time_ms": short_end_ms,
            "minutes": args.minutes,
            "start_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(short_start_ms / 1000)),
            "end_iso": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(short_end_ms / 1000)),
        },
        "instances": {i["id"]: i["name"] for i in instances},
        "metric_count": len(all_names),
    }
    write_json("manifest.json", manifest)


if __name__ == "__main__":
    main()
