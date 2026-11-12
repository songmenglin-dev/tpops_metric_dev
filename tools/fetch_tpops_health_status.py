#!/usr/bin/env python3
"""Probe the TPOPS platform health endpoint and record the real responses.

Usage:
    export TPOPS_USER=sml TPOPS_PASSWORD='...'
    python tools/fetch_tpops_health_status.py

Outputs (under data/api_probe/):
    health_status.json   the raw request/response records for every probe
    000manifest.json        provenance: which URL produced which record

Why this exists
---------------
`GET /rds/health-status` is referenced by the dev plan as the only way to probe
service availability for the customer metrics `gaussdb_{#PORT}port.status` and
`GAUSSDB.Service_availability_testing` (both are "近似对应" -- the SQL channel
can only return the port number). The plan quoted the CHM contract but had never
called it, so the actual response shape, the auth requirement and the path
prefix behaviour were all unverified.

The probe records three variants so the plan can cite facts instead of guesses:
    1. /rds/health-status   without auth   -- does it need a token at all?
    2. /rds/health-status   with auth      -- does the token change the response?
    3. /health-status       (bare prefix)  -- confirms the /rds namespace rule

Credentials come from the environment -- never hardcoded.
"""

import json
import os
import sys
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tpops_client import TpopsClient  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT, "data", "api_probe")

# Headers worth keeping in the record -- the rest of the response headers are
# noise from a load balancer / gateway and would bloat the file.
INTERESTING_HEADERS = ("content-type", "content-length", "server", "date",
                       "x-request-id", "x-trace-id")


def probe(session, base_url, path, headers, label):
    """Call one URL and return a留档-shaped record."""
    url = base_url + path
    started = time.time()
    try:
        resp = session.get(url, headers=headers, timeout=30)
        elapsed = round(time.time() - started, 3)
        body_text = resp.text
        try:
            body = resp.json()
            body_is_json = True
        except ValueError:
            body = body_text
            body_is_json = False
        record = {
            "label": label,
            "request": {"method": "GET", "url": url,
                        "headers_sent": sorted(headers.keys())},
            "response": {
                "status_code": resp.status_code,
                "headers": {k: v for k, v in resp.headers.items()
                            if k.lower() in INTERESTING_HEADERS},
                "body_is_json": body_is_json,
                "body": body,
                "body_text": None if body_is_json else body_text[:2000],
            },
            "elapsed_seconds": elapsed,
        }
        print(f"  {label:28} -> HTTP {resp.status_code}  {elapsed:6.3f}s  "
              f"body={body_text[:120]!r}")
        return record
    except requests.exceptions.RequestException as exc:
        elapsed = round(time.time() - started, 3)
        print(f"  {label:28} -> FAILED after {elapsed:6.3f}s: {exc}")
        return {
            "label": label,
            "request": {"method": "GET", "url": url,
                        "headers_sent": sorted(headers.keys())},
            "response": None,
            "error": "{}: {}".format(type(exc).__name__, exc),
            "elapsed_seconds": elapsed,
        }


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    client = TpopsClient()
    session = client.session

    print("probing TPOPS health endpoint at {} ...".format(client.base_url))

    # 1. no auth at all -- the permission doc says this endpoint needs no auth item
    r1 = probe(session, client.base_url, "/rds/health-status",
               {}, "no_auth")

    # 2. with a mini-token -- does the platform behave differently?
    try:
        token = client.token()
        token_headers = {"X-Mini-Token": token, "X-Language": "zh-cn"}
    except Exception as exc:  # noqa: BLE001 -- recorded, not swallowed
        print("  could not obtain a mini-token: {}".format(exc))
        token_headers = {}
    r2 = probe(session, client.base_url, "/rds/health-status",
               token_headers, "with_token")

    # 3. bare prefix -- documents the two-namespace rule from the dev plan
    r3 = probe(session, client.base_url, "/health-status",
               token_headers, "bare_prefix")

    records = [r for r in (r1, r2, r3) if r is not None]

    payload = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "base_url": client.base_url,
        "probes": records,
        "findings": summarise(records),
    }
    path = os.path.join(OUT_DIR, "health_status.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    print("  wrote data/api_probe/health_status.json")

    manifest = {
        "health_status.json": {
            "api": "健康检查",
            "method": "GET",
            "url": client.base_url + "/rds/health-status",
            "query": {},
            "headers": "none required (probed both with and without X-Mini-Token)",
            "probes": [r["label"] for r in records],
        },
        "_meta": {
            "generated_at": payload["generated_at"],
            "base_url": client.base_url,
            "note": "探针留档：该接口在开发方案中被引用为可用性探测手段，"
                    "但此前从未被真实调用过。",
        },
    }
    path = os.path.join(OUT_DIR, "000manifest.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    print("  wrote data/api_probe/000manifest.json")

    for line in payload["findings"]:
        print("  finding: {}".format(line))
    return 0


def summarise(records):
    """Turn the raw records into short factual statements."""
    out = []
    by_label = {r["label"]: r for r in records}

    no_auth = by_label.get("no_auth")
    if no_auth and no_auth.get("response"):
        code = no_auth["response"]["status_code"]
        out.append("GET /rds/health-status 无需认证即返回 HTTP {}".format(code))
        if code == 200 and not no_auth["response"]["body_is_json"]:
            body = (no_auth["response"]["body_text"] or "").strip()
            out.append("响应体为空字符串（长度 {}），非 JSON".format(len(body)))
        elif code == 200:
            out.append("响应体为 JSON: {}".format(
                json.dumps(no_auth["response"]["body"], ensure_ascii=False)[:200]))

    with_token = by_label.get("with_token")
    if no_auth and with_token and no_auth.get("response") and with_token.get("response"):
        a = no_auth["response"]
        b = with_token["response"]
        same = (a["status_code"] == b["status_code"]
                and a["body"] == b["body"]
                and a["body_text"] == b["body_text"])
        out.append("带 / 不带 X-Mini-Token 响应{}".format(
            "完全一致（该接口不受 token 影响）" if same else "不同（token 影响结果）"))

    bare = by_label.get("bare_prefix")
    if bare and bare.get("response"):
        out.append("GET /health-status（少 /rds 前缀）返回 HTTP {}".format(
            bare["response"]["status_code"]))
    return out


if __name__ == "__main__":
    sys.exit(main())
