#!/usr/bin/env python3
"""重建 TPOPS 平台指标字典。

输入（均由 tools/fetch_tpops_metrics.py 抓取，属可再生产物）：
  - data/metric_catalog.json        GET  /rds/v3/monitor/metrics/series
                                    给出 metric(英文 id) / unit / 所属分组
  - data/metrics_series_*.json      POST /rds/v3/monitor/metrics
                                    给出 metric_name(中文名) / collection_frequency

输出：
  - data/tpops_all_metric.json      234 条平台指标全集（JSON）
  - data/metric_names.csv           同一份数据的表格版，供人工核对

字段口径：
  - group          指标所属分组；同一指标出现在多个分组时取第一个为主分组
  - also_in_group  指标同时出现的其它分组（COREMETRICS 是多数指标的重复组）
  - has_data       该指标是否在时序响应中出现过（未出现则无中文名、无采集周期）

用法：
    python tools/fetch_tpops_metrics.py --hours 2   # 先抓原始数据
    python tools/build_tpops_metric_dict.py
"""

import csv
import glob
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
CATALOG = os.path.join(DATA_DIR, "metric_catalog.json")
OUT_JSON = os.path.join(DATA_DIR, "tpops_all_metric.json")
OUT_CSV = os.path.join(DATA_DIR, "metric_names.csv")

CSV_COLUMNS = ["metric", "metric_name", "unit", "group",
               "also_in_group", "collection_frequency", "has_data"]


def load_catalog():
    if not os.path.exists(CATALOG):
        sys.exit("找不到 %s\n先运行：python tools/fetch_tpops_metrics.py" % CATALOG)
    with open(CATALOG, encoding="utf-8") as fh:
        return json.load(fh)


def load_series():
    """合并所有时序文件里的 metric -> (中文名, 采集周期)。

    时序文件按窗口长度命名（metrics_series_5m.json / _2h.json）。
    窗口越长覆盖的指标越全——采集周期 3600s 的指标在 5 分钟窗口里根本不会出现，
    所以这里按窗口从长到短取，先出现的（即最长窗口里的）优先。
    """
    seen = {}
    files = sorted(glob.glob(os.path.join(DATA_DIR, "metrics_series_*.json")),
                   key=_window_minutes, reverse=True)
    if not files:
        sys.exit("找不到 data/metrics_series_*.json\n"
                 "先运行：python tools/fetch_tpops_metrics.py --hours 2")
    for path in files:
        with open(path, encoding="utf-8") as fh:
            body = json.load(fh)
        for info in body.get("metric_infos", []):
            for m in info.get("metrics", []):
                name = m.get("name")
                if name and name not in seen:
                    seen[name] = (m.get("metric_name"), m.get("collection_frequency"))
    return seen, files


def _window_minutes(path):
    """从文件名解析窗口长度（分钟），如 metrics_series_2h.json -> 120。

    解析不出���按 0 处理（排最后）。
    """
    name = os.path.basename(path)
    token = name.rsplit("_", 1)[-1].split(".")[0]     # "2h" / "5m"
    if token.endswith("h"):
        token = token[:-1]
        scale = 60
    elif token.endswith("m"):
        token = token[:-1]
        scale = 1
    else:
        return 0
    try:
        return int(token) * scale
    except ValueError:
        return 0


def build_rows(catalog, series):
    """把目录与时序合并成一份按 metric 排序的指标列表。"""
    merged = {}
    order = []
    # 按目录接口返回的分组顺序遍历，不排序：主分组取第一个出现的，
    # COREMETRICS 在目录里排在最后，因此重复指标的主分组是它所属的业务组。
    for group in catalog.keys():
        for entry in catalog[group]:
            name = entry.get("name")
            if not name:
                continue
            if name not in merged:
                merged[name] = {"unit": entry.get("unit"), "groups": []}
                order.append(name)
            if group not in merged[name]["groups"]:
                merged[name]["groups"].append(group)

    rows = []
    for name in order:
        groups = merged[name]["groups"]
        metric_name, frequency = series.get(name, (None, None))
        rows.append({
            "metric": name,
            "metric_name": metric_name,
            "unit": merged[name]["unit"],
            "group": groups[0],
            "also_in_group": groups[1:],
            "collection_frequency": frequency,
            "has_data": name in series,
        })
    rows.sort(key=lambda r: r["metric"])
    return rows


def write_json(rows):
    payload = {
        "source": {
            "metric": "data/metric_catalog.json (GET /rds/v3/monitor/metrics/series, "
                       "metrics[].name, 12 分组并集)",
            "metric_name": "data/metrics_series_2h.json (POST /rds/v3/monitor/metrics, "
                           "metrics[].metric_name)",
            "unit": "data/metric_catalog.json (metrics[].unit)",
            "group": "data/metric_catalog.json (所在分组，COREMETRICS 为重复组另记于 also_in_group)",
            "collection_frequency": "data/metrics_series_2h.json (metrics[].collection_frequency)",
        },
        "join_key": "metric",
        "total": len(rows),
        "with_chinese_name": sum(1 for r in rows if r["metric_name"]),
        "metrics": rows,
    }
    with open(OUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    return payload


def write_csv(rows):
    with open(OUT_CSV, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, lineterminator="\r\n")
        writer.writeheader()
        for row in rows:
            out = dict(row)
            out["also_in_group"] = ",".join(row["also_in_group"])
            out["collection_frequency"] = "" if row["collection_frequency"] is None \
                else row["collection_frequency"]
            out["has_data"] = "Y" if row["has_data"] else "N"
            writer.writerow(out)


def main():
    catalog = load_catalog()
    series, files = load_series()
    rows = build_rows(catalog, series)
    payload = write_json(rows)
    write_csv(rows)
    print("平台指标 %d 条（其中 %d 条带中文名）" % (payload["total"], payload["with_chinese_name"]))
    print("数据来源：%s" % ", ".join(os.path.basename(f) for f in files))

    # has_data 取决于窗口长度：采集周期 3600s 的指标在 5 分钟窗口里不会返回，
    # 会被误判为「平台无此指标」。窗口覆盖不全时要提醒，否则字典不可信。
    missing = [r for r in rows if not r["has_data"]]
    if missing:
        longest = max(_window_minutes(f) for f in files)
        if longest < 60:
            print("WARNING: 最长窗口只有 %d 分钟，采集周期 3600s 的指标不会出现在响应里，"
                  "%d 条指标会被误判为 has_data=False。\n"
                  "         请改用：python tools/fetch_tpops_metrics.py --hours 2"
                  % (longest, len(missing)), file=sys.stderr)
    print("写出 %s" % os.path.relpath(OUT_JSON, ROOT))
    print("写出 %s" % os.path.relpath(OUT_CSV, ROOT))


if __name__ == "__main__":
    main()
