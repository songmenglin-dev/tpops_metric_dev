#!/usr/bin/env python3
"""Fetch alarm data from TPOPS and write it to data/ as JSON.

Usage:
    export TPOPS_USER=sml TPOPS_PASSWORD='...'
    python tools/fetch_tpops_alarms.py

Outputs (under data/):
    alarm_statistic.json          severity counts (system + instance)
    alarm_active.json             active alarms, grouped per instance+alarm_id
    alarm_active_detail.json      active alarms, one row per node (with causes/steps)
    alarm_history.json            recovered/cleared alarms, grouped
    alarm_history_detail.json     recovered/cleared alarms, one row per node
    alarm_manifest.json           provenance: which API produced which file
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tpops_client import TpopsClient  # noqa: E402

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
    os.makedirs(DATA_DIR, exist_ok=True)
    client = TpopsClient()
    manifest = {}
    url = f"{client.base_url}/rds/v3/alarm/records"

    print("fetching alarm severity statistics ...")
    stat = client.alarm_statistics()
    write_json("alarm_statistic.json", stat)
    manifest["alarm_statistic.json"] = {
        "api": "告警数量查询",
        "method": "GET",
        "url": f"{client.base_url}/rds/v3/alarm/records/statistic",
        "query": {},
        "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
    }

    variants = [
        ("alarm_active.json", True, True, "active alarms, grouped per instance+alarm_id"),
        ("alarm_active_detail.json", True, False, "active alarms, one row per node"),
        ("alarm_history.json", False, True, "recovered/cleared alarms, grouped"),
        ("alarm_history_detail.json", False, False, "recovered/cleared alarms, one row per node"),
    ]

    for name, is_active, is_convergence, desc in variants:
        print(f"fetching {desc} ...")
        body = client.alarm_records(is_active=is_active, is_convergence=is_convergence,
                                    limit=100, offset=0)
        write_json(name, body)
        manifest[name] = {
            "api": "告警结果列表",
            "method": "POST",
            "url": url,
            "body": {
                "is_active": is_active,
                "is_convergence": is_convergence,
                "limit": 100,
                "offset": 0,
            },
            "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
            "note": desc,
            "total_count": body.get("total_count"),
        }

    manifest["_meta"] = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "base_url": client.base_url,
        "note": "历史告警默认保留 30 天",
    }
    write_json("alarm_manifest.json", manifest)


if __name__ == "__main__":
    main()
