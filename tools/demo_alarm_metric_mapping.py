#!/usr/bin/env python3
"""示例：把一条告警映射到它的指标。

    export TPOPS_USER=sml TPOPS_PASSWORD='...'
    python tools/demo_alarm_metric_mapping.py

背景：`POST /rds/v3/alarm/records` 的响应里**没有任何指标字段** —— 只有
`alarm_id` / `alarm_name` / `alarm_type` / `alarm_category`。平台也没有
"按告警查指标"的接口。

所以关联靠**反向查表**：

    告警的 alarm_id  ==  指标目录 metrics[].alarm_id   （平台给的同一套 ID）

本脚本演示三步：

1. 建反向表 `alarm_id -> 指标`（来自 GET /rds/v3/monitor/metrics/series）
2. 拉一条真实告警
3. 用它的 alarm_id 去查表，输出能拿到的指标名，以及为什么拿不到

**注意 `instance_id` 必须传**：不带它时平台把 `alarm_id` / `alarm_threshold`
留空，反向表会全是空键。

用法：
    python tools/demo_alarm_metric_mapping.py                # 演示第一条告警
    python tools/demo_alarm_metric_mapping.py 2001050001     # 演示指定 alarm_id
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tpops_client import TpopsClient  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_METRIC = os.path.join(ROOT, "data", "alarm", "catalog_metric.json")
CATALOG_EVENT = os.path.join(ROOT, "data", "alarm", "catalog_event.json")
ALARM_RULES = os.path.join(ROOT, "data", "mapping", "alarm_rules.json")


def load_json(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def build_alarm_to_metric(client, instance_id):
    """反向表：alarm_id -> 指标定义。来源是指标目录接口的 metrics[].alarm_id。"""
    catalog = client.metric_catalog(instance_id=instance_id)
    table = {}
    for group, metrics in catalog.items():
        for m in metrics:
            alarm_id = m.get("alarm_id")
            if not alarm_id:
                continue
            table[str(alarm_id)] = {
                "group": group,
                "metric_id": m.get("name"),
                "metric_name": m.get("metric_name"),  # 目录接口一般不带中文名
                "unit": m.get("unit"),
            }
    return table


def resolve(alarm_id, table, catalog_metric, catalog_event, rules):
    """alarm_id -> 指标名。返回 (结果说明, 指标或 None)。"""
    result = {"alarm_id": alarm_id}

    # 1) 平台指标目录的反向表（可信：两边都是平台给的同一套 ID）
    hit = table.get(str(alarm_id))
    result["from_metrics_series"] = hit

    # 2) 离线告警定义目录（手工整理的告警规则，含中文指标名与阈值）
    row = next((r for r in catalog_metric["items"]
                if str(r.get("告警ID")) == str(alarm_id)), None)
    result["from_catalog_metric"] = row

    # 2b) 事件类目录 —— 与指标类目录零交集，命中说明这是事件类告警
    event = next((r for r in catalog_event["items"]
                  if str(r.get("告警ID")) == str(alarm_id)), None)
    result["from_catalog_event"] = event

    # 3) 客户侧口径：能否挂到客户指标
    definition = rules["definitions"].get(str(alarm_id))
    result["from_alarm_rules"] = definition
    return result


def explain(result):
    aid = result["alarm_id"]
    print("=" * 72)
    print(f"alarm_id = {aid}")
    print("=" * 72)

    series = result.get("from_metrics_series")
    row = result.get("from_catalog_metric")
    event = result.get("from_catalog_event")
    definition = result.get("from_alarm_rules")

    if series:
        metric_name = series.get("metric_name") or "(目录接口不带中文名)"
        print("\n[1] 指标目录反向表命中（可信）")
        print(f"    分组     : {series['group']}")
        print(f"    指标 id  : {series['metric_id']}")
        print(f"    指标名   : {metric_name}")
        print(f"    单位     : {series.get('unit')}")
    else:
        print("\n[1] 指标目录反向表：未命中")
        print("    -> 该 alarm_id 没有对应的平台监控指标")

    if row:
        metric_id = row.get("metric_id") or "(未匹配)"
        trigger = row.get("告警阈值（默认）")
        clear = row.get("清除告警阈值（默认）")
        print("\n[2] 指标类告警目录（离线整理）")
        print(f"    告警名   : {row.get('告警名称')}")
        print(f"    指标     : {row.get('指标')}")
        print(f"    匹配级别 : {row.get('match')}")
        print(f"    指标 id  : {metric_id}")
        if row.get("metric_id_candidates"):
            print(f"    候选     : {row['metric_id_candidates']}")
        print(f"    阈值     : 触发 {trigger} / 清除 {clear}")
    elif event:
        print("\n[2] 事件类告警目录（离线整理）—— 事件类本就没有对应指标")
        print(f"    告警名   : {event.get('告警名称')}")
        print(f"    告警类型 : {event.get('告警类型')}")
        print(f"    默认级别 : {event.get('告警级别（默认）')}")
        print(f"    归属     : {event.get('告警归属')}")
    else:
        print("\n[2] 两个告警目录都没命中（告警ID不在离线整理的范围内）")

    if definition:
        linked = (definition.get("linked_customer_metric")
                  or "(无 -> 用 gaussdb.alarm.<alarm_id> 兜底)")
        print("\n[3] 客户侧挂载口径（alarm_rules.json）")
        print(f"    类型     : {definition.get('alarm_kind')}")
        print(f"    metricCode: {definition.get('metric_code')}")
        print(f"    metricName: {definition.get('metric_name')}")
        print(f"    挂客户指标: {linked}")
        print(f"    取值来源 : {definition.get('value_source')}")
    else:
        print("\n[3] 客户侧：无对应定义")

    print()


def main():
    client = TpopsClient()

    instances = client.instances()
    rows = instances.get("instances") or instances.get("records") or []
    if not rows:
        print("no instances returned", file=sys.stderr)
        return 1
    instance_id = rows[0]["id"]
    print(f"instance_id = {instance_id} ({rows[0].get('name')})\n")

    print("building alarm_id -> metric reverse table ...")
    table = build_alarm_to_metric(client, instance_id)
    print(f"  {len(table)} 个 alarm_id 可反查到平台指标\n")

    catalog_metric = load_json(CATALOG_METRIC)
    catalog_event = load_json(CATALOG_EVENT)
    rules = load_json(ALARM_RULES)

    # 取一条真实告警
    records = client.alarm_records(is_active=True, is_convergence=True, limit=10)
    alarms = records.get("records") or []
    if not alarms:
        print("no active alarms right now; falling back to the offline catalogue",
              file=sys.stderr)

    if len(sys.argv) > 1:
        wanted = sys.argv[1]
        result = resolve(wanted, table, catalog_metric, catalog_event, rules)
        explain(result)
        return 0

    for alarm in alarms[:2]:
        result = resolve(alarm["alarm_id"], table, catalog_metric, catalog_event, rules)
        print("原始告警记录：")
        print(json.dumps(alarm, ensure_ascii=False, indent=2))
        explain(result)

    # 对照：一个能反查到指标、且能挂到客户指标的告警（TPS）
    print("对照：能挂到客户指标的告警长这样 ——")
    result = resolve("2001050018", table, catalog_metric, catalog_event, rules)
    explain(result)

    return 0


if __name__ == "__main__":
    sys.exit(main())
