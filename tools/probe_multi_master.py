import json, os, sys, time
sys.path.insert(0, os.path.join(os.getcwd(), 'tools'))
from tpops_client import TpopsClient

SQL = ("select datname, numbackends, xact_commit, tup_inserted, deadlocks "
       "from pg_stat_database order by datname")
CMD = "73722acc-bff6-4d04-adc0-bc1e1fa1eb7d"
B = "https://172.16.103.193:8002"

def run(cl, inst, node):
    h = {"X-Mini-Token": cl.token(), "X-Language": "zh-cn", "Content-Type": "application/json"}
    body = {"instance_id": inst, "node_ids": [node],
            "params": [{"param_name": "sql_text", "param_value": SQL},
                       {"param_name": "limit", "param_value": "1000"}]}
    r = cl.session.post(f"{B}/rds/gaussdb/v3/command-center/customer-commands/{CMD}",
                         json=body, headers=h, timeout=60)
    j = r.json(); job = j.get("job_id"); task = j["node_tasks"][0]["task_id"]
    for _ in range(30):
        rr = cl.session.get(f"{B}/rds/gaussdb/v3/command-center/command/{CMD}"
                            f"/batch-exec-job/task-records?job_id={job}",
                            headers=h, timeout=60).json()
        if rr.get("statistics", {}).get("running", 1) == 0:
            break
        time.sleep(1)
    d = cl.session.get(f"{B}/rds/gaussdb/v3/command-center/command/{CMD}"
                       f"/batch-exec-job/{job}/task-record/{task}/detail",
                       headers=h, timeout=60).json()
    rows = json.loads(d["result"]) if d.get("result") else []
    return d.get("node_name"), rows

cl = TpopsClient()
INST = "83cd68f2e4cb40b4b7e69240479ffa26in14"   # gauss-6374, Combined
nodes = json.load(open("data/command_center/results/02_nodes_%s.json" % INST, encoding="utf-8"))
nodes = nodes["response"]["body"]["nodes"]
masters = []
for n in nodes:
    if n["role"] == "master" and n["node_id"] not in [m["node_id"] for m in masters]:
        masters.append(n)

print("Combined 实例 %s —— role=master 的唯一节点: %d 个" % (INST, len(masters)))
results = []
for n in masters:
    try:
        nn, rows = run(cl, INST, n["node_id"])
        results.append((n, nn, rows))
        print("  node %s (%s) -> 返回 %d 行" % (n["node_name"], n["type"], len(rows)))
    except Exception as e:
        print("  node %s FAILED: %s" % (n["node_name"], e))

print("\n=== 各 master 的 pg_stat_database 快照对比 ===")
for n, nn, rows in results:
    print("-- %s (%s) node_name=%s" % (n["node_name"], n["type"], nn))
    for r in rows:
        print("     ", json.dumps(r, ensure_ascii=False))

if len(results) >= 2:
    def norm(rs): return {r.get("datname"): (r.get("numbackends"), r.get("xact_commit"), r.get("tup_inserted")) for r in rs}
    base = norm(results[0][2])
    print("\n=== 是否各 master 相同 ===")
    for n, nn, rows in results[1:]:
        same = norm(rows) == base
        print("  %s vs %s : %s" % (results[0][0]["node_name"], n["node_name"], "完全相同" if same else "不同"))
        if not same:
            for db in sorted(set(base) | set(norm(rows))):
                if base.get(db) != norm(rows).get(db):
                    print("     差异 %s: A=%s  B=%s" % (db, base.get(db), norm(rows).get(db)))
