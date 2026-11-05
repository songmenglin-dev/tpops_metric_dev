#!/usr/bin/env python3
"""Fetch alarm data from TPOPS and write it to data/alarm/ as JSON.

Usage:
    export TPOPS_USER=sml TPOPS_PASSWORD='...'
    python tools/fetch_tpops_alarms.py

Outputs (under data/alarm/):
    statistic.json         severity counts (system + instance)
    active.json            active alarms, grouped per instance+alarm_id
    active_detail.json     active alarms, one row per node (with causes/steps)
    history.json           recovered/cleared alarms, grouped
    history_detail.json    recovered/cleared alarms, one row per node
    manifest.json          provenance: which API produced which file

告警目录（告警定义表）不在此脚本产出，见 data/alarm/README.md。
"""

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tpops_client import TpopsClient  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ALARM_DIR = os.path.join(ROOT, "data", "alarm")


def write_json(name, payload):
    path = os.path.join(ALARM_DIR, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    size_kb = os.path.getsize(path) / 1024
    print(f"  wrote data/alarm/{name} ({size_kb:.0f} KB)")
    return path


def main():
    os.makedirs(ALARM_DIR, exist_ok=True)
    client = TpopsClient()
    manifest = {}
    url = f"{client.base_url}/rds/v3/alarm/records"

    print("fetching alarm severity statistics ...")
    stat = client.alarm_statistics()
    write_json("statistic.json", stat)
    manifest["statistic.json"] = {
        "api": "告警数量查询",
        "method": "GET",
        "url": f"{client.base_url}/rds/v3/alarm/records/statistic",
        "query": {},
        "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
    }

    variants = [
        ("active.json", True, True, "active alarms, grouped per instance+alarm_id"),
        ("active_detail.json", True, False, "active alarms, one row per node"),
        ("history.json", False, True, "recovered/cleared alarms, grouped"),
        ("history_detail.json", False, False, "recovered/cleared alarms, one row per node"),
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
    write_json("manifest.json", manifest)


if __name__ == "__main__":
    main()
