#!/usr/bin/env python3
"""逐指标实测指标值获取方法，通过后才落盘 JSON（目录与字段名均用英文）。

流程：
  1. 读 data/customer_metric_query_plan.json（每个指标的查询方法）
  2. 对每个「可提供」的指标，按方案里的 API + 入参真实调用一次
  3. 判定：查询是否成功（HTTP 200 / 任务 succeed / 返回体含预期字段）
  4. 通过的指标才写入 data/指标获取方法实测/<序号>_<客户中文名>.json，
     文件含客户指标名、TPOPS 对应指标名、获取方法、实测结果

用法：
    python tools/verify_metric_sources.py [--out-dir data/指标获取方法实测]
"""

import argparse
import csv
import glob
import json
import os
import re
import sys
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
PLAN = os.path.join(DATA_DIR, "customer_metric_query_plan.json")
MAPPING = os.path.join(DATA_DIR, "customer_metric_mapping.json")

BASE_URL = os.environ.get("TPOPS_BASE_URL", "https://172.16.103.193:8002").rstrip("/")
TPOPS_USER = os.environ.get("TPOPS_USER", "")
TPOPS_PASSWORD = os.environ.get("TPOPS_PASSWORD", "")

# 实测用的实例（HA 形态，含真实业务数据）
TEST_INSTANCE = "c35b8d078cae4a9eb05d616ed5cf032fin14"
BOTH_INSTANCES = ["83cd68f2e4cb40b4b7e69240479ffa26in14", "c35b8d078cae4a9eb05d616ed5cf032fin14"]
MAX_RETRY = 3

STATUS_PASS = "通过"
STATUS_PASS_EMPTY = "通过（查询成功，当前无数据）"
STATUS_FAIL = "不通过"


class Tpops(object):
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self._token = None

    def token(self):
        if self._token:
            return self._token
        if not TPOPS_USER or not TPOPS_PASSWORD:
            sys.exit("缺少 TPOPS_USER / TPOPS_PASSWORD 环境变量")
        resp = self.session.post(
            BASE_URL + "/rds/v3/auth/mini-token",
            json={"username": TPOPS_USER, "password": TPOPS_PASSWORD}, timeout=30)
        tok = resp.headers.get("X-Mini-Token")
        if not tok:
            sys.exit("获取 X-Mini-Token 失败: HTTP %s %s" % (resp.status_code, resp.text[:200]))
        self._token = tok
        return tok

    def headers(self):
        return {"X-Mini-Token": self.token(), "X-Language": "zh-cn",
                "Content-Type": "application/json"}

    def master_nodes(self, instance_id):
        """取实例上可执行命令的 master 节点。"""
        resp = self.session.get(
            BASE_URL + "/rds/gaussdb/v3/instances/%s/key-view-execute-node" % instance_id,
            headers=self.headers(), timeout=30)
        if resp.status_code != 200:
            raise RuntimeError("key-view-execute-node HTTP %s" % resp.status_code)
        nodes = [n for n in resp.json().get("nodes", []) if n.get("role") == "master"]
        if not nodes:
            raise RuntimeError("实例 %s 无 master 节点" % instance_id)
        return nodes


def sanitize(name):
    """把客户英文指标名转成安全文件名（保留可读性）。"""
    out = name.strip()
    out = re.sub(r"[#{}()\[\]]", "", out)          # 去掉占位符花括号
    out = re.sub(r'[\\/:*?"<>|\r\n\t]', "_", out)  # 文件系统非法字符
    out = re.sub(r"\s+", "_", out)
    out = re.sub(r"_{2,}", "_", out).strip("._")
    return out[:80] or "metric"


def run_sql(tpops, node_id, sql, limit=100):
    """命令中心执行 SQL，返回 (状态, 结果, 错误信息, 耗时秒)。"""
    cmd_id = "73722acc-bff6-4d04-adc0-bc1e1fa1eb7d"   # general_single_sql_query
    last_err = None
    started = time.time()
    for attempt in range(MAX_RETRY):
        body = {"instance_id": TEST_INSTANCE, "node_ids": [node_id],
                "params": [{"param_name": "sql_text", "param_value": sql},
                           {"param_name": "limit", "param_value": str(limit)}]}
        resp = tpops.session.post(
            BASE_URL + "/rds/gaussdb/v3/command-center/customer-commands/%s" % cmd_id,
            json=body, headers=tpops.headers(), timeout=120)
        if resp.status_code != 200:
            last_err = "下发 HTTP %s %s" % (resp.status_code, resp.text[:200])
            time.sleep(2)
            continue
        job = resp.json().get("job_id")
        task = (resp.json().get("node_tasks") or [{}])[0].get("task_id")
        if not job:
            last_err = "下发未返回 job_id: %s" % resp.text[:200]
            time.sleep(2)
            continue
        done = None
        for _ in range(20):
            time.sleep(2.5)
            rr = tpops.session.get(
                BASE_URL + "/rds/gaussdb/v3/command-center/command/%s/batch-exec-job/task-records" % cmd_id,
                params={"job_id": job}, headers=tpops.headers(), timeout=60)
            tasks = (rr.json() or {}).get("tasks") or []
            if tasks and all(t.get("status") not in ("running", "pending") for t in tasks):
                done = tasks[0]
                break
        if done is None:
            last_err = "轮询超时"
            continue
        if done.get("status") != "succeed":
            last_err = (done.get("error_msg") or "")[:300]
            time.sleep(2)
            continue
        detail = tpops.session.get(
            BASE_URL + ("/rds/gaussdb/v3/command-center/command/%s/batch-exec-job/%s"
                        "/task-record/%s/detail" % (cmd_id, job, task)),
            headers=tpops.headers(), timeout=60).json()
        raw = detail.get("result")
        try:
            rows = json.loads(raw) if raw else []
        except ValueError:
            rows = raw
        return STATUS_PASS, rows, None, round(time.time() - started, 1)
    return STATUS_FAIL, None, last_err, round(time.time() - started, 1)


def run_monitor(tpops, metric_name):
    """监控指标接口取时间序列。"""
    now = int(time.time() * 1000) - 5 * 60 * 1000
    body = {"start_time": now - 2 * 3600 * 1000, "end_time": now,
            "is_latest_point_only": "false",
            "instances": [{"instance_id": i} for i in BOTH_INSTANCES],
            "metric_names": [metric_name]}
    started = time.time()
    resp = tpops.session.post(BASE_URL + "/rds/v3/monitor/metrics",
                               json=body, headers=tpops.headers(), timeout=300)
    elapsed = round(time.time() - started, 1)
    if resp.status_code != 200:
        return STATUS_FAIL, None, "HTTP %s %s" % (resp.status_code, resp.text[:200]), elapsed
    found = []
    for info in resp.json().get("metric_infos", []):
        for m in info.get("metrics", []):
            if m.get("name") == metric_name:
                found.append({"instance_id": info.get("instance_id"),
                              "metric_name_cn": m.get("metric_name"),
                              "collection_frequency": m.get("collection_frequency"),
                              "timestamps": len(m.get("timestamps") or []),
                              "hosts": len(m.get("series") or {}),
                              "sample": {k: (v[:3] if isinstance(v, list) else v)
                                         for k, v in list((m.get("series") or {}).items())[:2]}})
    if not found:
        return STATUS_FAIL, None, "响应中没有指标 %s" % metric_name, elapsed
    return (STATUS_PASS if found else STATUS_PASS_EMPTY), found, None, elapsed


def run_slow_sql(tpops, node_id):
    """慢 SQL 集合接口。"""
    now = int(time.time() * 1000)
    body = {"start_time": now - 86400 * 1000, "end_time": now,   # 实测窗口上限 24 小时
            "instance_id": TEST_INSTANCE, "threshold": 1000, "threshold_unit": "ms",
            "node_ids": [node_id]}
    started = time.time()
    resp = tpops.session.post(
        BASE_URL + "/rds/gaussdb/v3/instances/%s/slow-sql-list" % TEST_INSTANCE,
        json=body, headers=tpops.headers(), timeout=120)
    elapsed = round(time.time() - started, 1)
    if resp.status_code != 200:
        return STATUS_FAIL, None, "HTTP %s %s" % (resp.status_code, resp.text[:200]), elapsed
    data = resp.json()
    infos = data.get("slow_sql_infos")
    if infos is None:
        return STATUS_FAIL, None, "响应中无 slow_sql_infos 字段: %s" % resp.text[:200], elapsed
    return (STATUS_PASS if infos else STATUS_PASS_EMPTY), infos, None, elapsed


def run_top_sql(tpops, node_ids):
    """Top SQL 列表接口。"""
    now = int(time.time() * 1000)
    body = {"instance_id": TEST_INSTANCE, "node_ids": node_ids,
            "start_time": now - 7 * 86400 * 1000, "end_time": now}
    started = time.time()
    resp = tpops.session.post(
        BASE_URL + "/rds/gaussdb/v3/instances/%s/top-sql-list" % TEST_INSTANCE,
        json=body, headers=tpops.headers(), timeout=120)
    elapsed = round(time.time() - started, 1)
    if resp.status_code != 200:
        return STATUS_FAIL, None, "HTTP %s %s" % (resp.status_code, resp.text[:200]), elapsed
    data = resp.json()
    infos = data.get("top_sql_infos")
    if infos is None:
        return STATUS_FAIL, None, "响应中无 top_sql_infos 字段: %s" % resp.text[:200], elapsed
    return (STATUS_PASS if infos else STATUS_PASS_EMPTY), infos, None, elapsed


def test_metric(tpops, entry, node_id, node_ids):
    """按方案执行一次，返回 (状态, 结果, 错误, 耗时, 实际调用说明)。"""
    query = entry.get("query")
    if not query:
        return STATUS_FAIL, None, "无查询方案", 0, ""
    api = query["api"]
    body = query["request"].get("body") or {}
    if api.endswith("command-center/customer-commands/{cmd_id}"):
        sql = None
        for p in body.get("params", []):
            if p.get("param_name") == "sql_text":
                sql = p.get("param_value")
        if not sql:
            return STATUS_FAIL, None, "方案中无 sql_text", 0, ""
        st, rows, err, el = run_sql(tpops, node_id, sql)
        if st == STATUS_PASS and isinstance(rows, list) and not rows:
            st = STATUS_PASS_EMPTY
        return st, rows, err, el, "命令中心执行 SQL"
    if api == "POST /rds/v3/monitor/metrics":
        st, rows, err, el = run_monitor(tpops, entry["tpops_metric"])
        return st, rows, err, el, "监控指标接口"
    if api.endswith("/slow-sql-list"):
        st, rows, err, el = run_slow_sql(tpops, node_id)
        return st, rows, err, el, "慢SQL集合接口"
    if api == "GET /rds/v3.2/instances":
        started = time.time()
        resp = tpops.session.get(BASE_URL + "/rds/v3.2/instances",
                                 params={"limit": 100, "offset": 0},
                                 headers=tpops.headers(), timeout=60)
        elapsed = round(time.time() - started, 1)
        if resp.status_code != 200:
            return STATUS_FAIL, None, "HTTP %s %s" % (resp.status_code, resp.text[:200]), elapsed, ""
        rows = [{"id": i.get("id"), "name": i.get("name"), "mode": i.get("mode"),
                 "engine_name": i.get("engine_name"), "engine_version": i.get("engine_version"),
                 "instance_status": i.get("instance_status")} for i in resp.json().get("instances", [])]
        if not rows or not rows[0].get("engine_version"):
            return STATUS_FAIL, None, "响应中无 engine_version", elapsed, ""
        return STATUS_PASS, rows, None, elapsed, "实例列表接口"
    if api.endswith("/top-sql-list"):
        st, rows, err, el = run_top_sql(tpops, node_ids)
        return st, rows, err, el, "Top SQL 列表接口"
    return STATUS_FAIL, None, "未支持的 API: %s" % api, 0, ""


def build_record(entry, category, status, result, err, elapsed, way, tested_at,
                 spec, node_name):
    """构造单个指标的实测记录，字段名与 customer_metric_query_plan.json 对齐。"""
    query = entry.get("query") or {}
    sample = result[:2] if isinstance(result, list) else result
    return {
        "customer_metric": entry["customer_metric"],
        "customer_metric_name": entry["customer_metric_name"],
        "customer_category": category,
        "tpops_metric": entry["tpops_metric"],
        "tpops_metric_name": entry["tpops_metric_name"],
        "status": entry["status"],
        "match_level": entry["match_level"],
        "source": entry["source"],
        "note": entry.get("note"),
        "customer_spec": {
            "data_type": spec.get("数据类型"),
            "data_update_cycle": spec.get("数据更新周期"),
            "collect_cycle_unit": spec.get("采集更新周期单位"),
            "metric_unit": spec.get("指标单位"),
        },
        "query": {
            "api": query.get("api"),
            "method": query.get("method"),
            "uri": query.get("uri"),
            "request": query.get("request"),
            "expected_result": query.get("expected_result"),
            "related_apis": query.get("related_apis"),
        },
        "verification": {
            "tested_at": tested_at,
            "method": way,
            "instance_id": TEST_INSTANCE,
            "node_name": node_name,
            "state": status,
            "elapsed_seconds": elapsed,
            "row_count": (len(result) if isinstance(result, list) else None),
            "error": err,
            "sample": sample,
        },
    }


def main():
    ap = argparse.ArgumentParser(description="逐指标实测获取方法，通过后落盘 JSON")
    ap.add_argument("--out-dir", default=os.path.join(DATA_DIR, "metric_verification"))
    args = ap.parse_args()

    if not os.path.exists(PLAN):
        sys.exit("缺少 %s，请先运行 tools/build_metric_query_plan.py" % PLAN)
    with open(PLAN, encoding="utf-8") as fh:
        plan = json.load(fh)
    with open(MAPPING, encoding="utf-8") as fh:
        mapping = json.load(fh)
    mapping_by_name = dict((m["customer_metric_name"], m) for m in mapping["items"])

    # 客户 Excel 的补充字段（数据类型/周期/单位）
    xlsx_fields = {}
    for path in glob.glob(os.path.join(ROOT, "*.xlsx")):
        if "指标明细" in os.path.basename(path) and not os.path.basename(path).startswith("~$"):
            import openpyxl
            ws = openpyxl.load_workbook(path, data_only=True)["指标库"]
            rows = list(ws.iter_rows(values_only=True))
            for r in rows[1:]:
                xlsx_fields[r[3]] = {"数据类型": r[4], "数据更新周期": r[5],
                                     "采集更新周期单位": r[6], "指标单位": r[8]}
            break

    os.makedirs(args.out_dir, exist_ok=True)
    tpops = Tpops()
    master = tpops.master_nodes(TEST_INSTANCE)
    node_id = master[0]["node_id"]
    node_ids = [n["node_id"] for n in master]
    node_name = master[0].get("node_name")

    targets = []
    for cat in plan["categories"]:
        for entry in cat["metrics"]:
            if entry["status"] == "可提供":
                targets.append((cat["category"], entry))
    print("待实测指标：%d 个" % len(targets), flush=True)

    ok = 0
    failed = []
    for idx, (cat, entry) in enumerate(targets, start=1):
        tested_at = time.strftime("%Y-%m-%d %H:%M:%S")
        status, result, err, elapsed, way = test_metric(tpops, entry, node_id, node_ids)
        if status in (STATUS_PASS, STATUS_PASS_EMPTY):
            ok += 1
            rec = build_record(entry, cat, status, result, err, elapsed, way, tested_at,
                               xlsx_fields.get(entry["customer_metric"], {}), node_name)
            fname = "%03d_%s.json" % (idx, sanitize(entry["customer_metric"]))
            with open(os.path.join(args.out_dir, fname), "w", encoding="utf-8") as fh:
                json.dump(rec, fh, ensure_ascii=False, indent=2)
            print("[%s] %-3d %s" % (status, idx, entry["customer_metric_name"]), flush=True)
        else:
            failed.append((entry["customer_metric_name"], err))
            print("[%s] %-3d %s -- %s" % (status, idx, entry["customer_metric_name"], err), flush=True)

    print("\n实测通过 %d / %d" % (ok, len(targets)))
    if failed:
        print("未通过（不生成 JSON 文件）：")
        for name, err in failed:
            print("   - %s: %s" % (name, err))
    with open(os.path.join(args.out_dir, "_summary.json"), "w", encoding="utf-8") as fh:
        json.dump({"tested_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                   "total": len(targets), "passed": ok, "failed": len(failed),
                   "failed_items": [{"customer_metric_name": n, "error": e} for n, e in failed],
                   "instance_id": TEST_INSTANCE},
                  fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
