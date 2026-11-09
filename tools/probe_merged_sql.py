#!/usr/bin/env python3
"""Execute the merged SQL set against TPOPS and record what actually comes back.

Usage:
    export TPOPS_USER=sml TPOPS_PASSWORD='...'
    python tools/probe_merged_sql.py

Reads  data/merged_sql/merged_sql.json
Writes data/merged_sql/probe_result.json

Sequential + retry by design: measured 2026-11-07, submitting >=4 jobs concurrently
to one instance makes the platform fail a random subset with a bogus
"invalid SQL statement with syntax error" (the SQL is fine; it is a parameter
race on the platform side), while concurrency buys no wall-clock time.
"""
import json, os, sys, time

sys.path.insert(0, os.path.join(os.getcwd(), "tools"))
from tpops_client import TpopsClient  # noqa: E402

B = "https://172.16.103.193:8002"
CMD = "73722acc-bff6-4d04-adc0-bc1e1fa1eb7d"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "data", "merged_sql", "merged_sql.json")
OUT = os.path.join(ROOT, "data", "merged_sql", "probe_result.json")
INSTANCES = [("83cd68f2e4cb40b4b7e69240479ffa26in14", "gauss-6374", "Combined"),
             ("c35b8d078cae4a9eb05d616ed5cf032fin14", "gauss-75a2", "Ha")]


def masters_of(inst):
    p = os.path.join(ROOT, "data", "command_center", "results",
                     "02_nodes_%s.json" % inst)
    ns = json.load(open(p, encoding="utf-8"))["response"]["body"]["nodes"]
    out, seen = [], set()
    for n in ns:
        if n["role"] == "master" and n["node_id"] not in seen:
            seen.add(n["node_id"])
            out.append(n)
    return out


def run_sql(cl, h, inst, ids, batch):
    body = {"instance_id": inst, "node_ids": ids,
            "params": [{"param_name": "sql_text", "param_value": batch["sql"]},
                       {"param_name": "limit", "param_value": batch["limit"]}]}
    r = cl.session.post(
        f"{B}/rds/gaussdb/v3/command-center/customer-commands/{CMD}",
        json=body, headers=h, timeout=60)
    if r.status_code != 200:
        return None, "HTTP %s %s" % (r.status_code, r.text[:200])
    j = r.json()
    job = j["job_id"]
    for _ in range(120):
        rr = cl.session.get(
            f"{B}/rds/gaussdb/v3/command-center/command/{CMD}"
            f"/batch-exec-job/task-records?job_id={job}", headers=h, timeout=60).json()
        if rr.get("statistics", {}).get("running", 1) == 0:
            break
        time.sleep(0.3)
    nodes = []
    for t in rr.get("tasks", []):
        d = cl.session.get(
            f"{B}/rds/gaussdb/v3/command-center/command/{CMD}"
            f"/batch-exec-job/{job}/task-record/{t['task_id']}/detail",
            headers=h, timeout=60).json()
        res = d.get("result") or ""
        try:
            parsed = json.loads(res) if res else []
            ok = isinstance(parsed, list)
        except ValueError:
            parsed, ok = None, False
        nodes.append({
            "node_name": d.get("node_name"), "status": d.get("status"),
            "row_count": len(parsed) if ok else None,
            "columns": sorted(parsed[0].keys()) if ok and parsed else [],
            "parse_ok": ok,
            "error_msg": None if ok else (t.get("error_msg") or res[:120]),
        })
    if any(n["status"] != "succeed" for n in nodes):
        return None, "; ".join(str(n["error_msg"]) for n in nodes if n["status"] != "succeed")
    return nodes, None


def main():
    spec = json.load(open(SRC, encoding="utf-8"))
    cl = TpopsClient()
    h = {"X-Mini-Token": cl.token(), "X-Language": "zh-cn",
         "Content-Type": "application/json"}
    results = {}
    for inst, name, mode in INSTANCES:
        ms = masters_of(inst)
        ids = [m["node_id"] for m in ms]
        print("\n=== %s (%s) —— %d 个 master ===" % (name, mode, len(ids)))
        rows, retries, failed = [], 0, []
        t_round = time.time()
        for b in spec["batches"]:
            for attempt in range(3):
                nodes, err = run_sql(cl, h, inst, ids, b)
                if nodes is not None:
                    break
                retries += 1
                time.sleep(1)
            else:
                failed.append(b["sql_key"])
                nodes = [{"node_name": "?", "status": "failed", "row_count": None,
                          "columns": [], "parse_ok": False, "error_msg": err}]
            rows.append({"sql_key": b["sql_key"], "attempts": attempt + 1,
                         "nodes": nodes})
            print("  %-18s attempts=%d rows=%-10s cols=%d" % (
                b["sql_key"], attempt + 1,
                [n["row_count"] for n in nodes], len(nodes[0]["columns"])))
        print("  --- 轮次总耗时 %.1fs | 重试 %d 次 | 最终失败 %d 条 %s" % (
            time.time() - t_round, retries, len(failed), failed))
        results[inst] = {"instance_name": name, "instance_mode": mode,
                          "master_count": len(ids),
                          "master_nodes": [m["node_name"] for m in ms],
                          "round_seconds": round(time.time() - t_round, 2),
                          "retries": retries, "failed": failed, "batches": rows}
    out = {"generated_at": time.strftime("%Y-%m-%d %H:%M:%S %z"),
           "command": {"cmd_id": CMD, "cmd_name": "general_single_sql_query",
                       "constraint_config": {"limit_node_types": ["master"]}},
           "execution_mode": "sequential + retry(3)",
           "instances": results}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print("\nwrote data/merged_sql/probe_result.json")


if __name__ == "__main__":
    main()
