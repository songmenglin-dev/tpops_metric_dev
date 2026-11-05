#!/usr/bin/env python3
"""判定客户要求的指标中，哪些 TPOPS 能提供、哪些不能。

输入：
  - 高斯DB-指标明细*.xlsx    客户要求的 101 个指标（含中文名/英文名/字段规范）
  - data/tpops_all_metric.json  TPOPS 平台指标字典（metric / metric_name / unit ...）

输出：
  - data/customer_metric_mapping.json    逐条判定结论 + 统计
  - data/customer_metric_unsupported.csv  TPOPS 无法提供的指标，字段与客户 Excel 完全一致

映射表 MAPPING 是人工判定的结果，按客户 Excel 的行序排列；每条都带客户中文名，
脚本启动时会与 Excel 逐行核对，对不上直接报错（防止 Excel 改动后静默错位）。

用法：
    python tools/build_customer_metric_mapping.py
"""

import csv
import glob
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
TPOPS_DICT = os.path.join(DATA_DIR, "tpops_all_metric.json")
OUT_JSON = os.path.join(DATA_DIR, "customer_metric_mapping.json")
OUT_CSV = os.path.join(DATA_DIR, "customer_metric_unsupported.csv")

# 客户 Excel 的列顺序，CSV 输出沿用
CLIENT_COLUMNS = [
    "适用资源类型", "指标分类", "中文名", "英文名", "数据类型",
    "数据更新周期", "采集更新周期单位", "生成性能基线", "指标单位",
]

PROVIDABLE = "可提供"
UNPROVIDABLE = "无法提供"

# 匹配程度
DIRECT = "直接对应"        # 口径一致，仅字段名不同
APPROX = "近似对应"        # 平台有同类指标但口径/维度不同
CONVERT = "需换算"         # 同一底层计数，但客户要累计值、平台给速率

# (客户中文名, TPOPS 指标英文名 或 None, 匹配程度, 备注)
MAPPING = [
    ("每秒提交事务数", "rds092_gaussv5_commit_counter", DIRECT,
     "平台另有 _SYS/_USER 拆分（系统用户/业务用户）"),
    ("慢查询总数",
     "rds146_gaussdbv5_slowquery_sys_SYS + rds146_gaussdbv5_slowquery_sys_USER"
     " + rds147_gaussdbv5_slowquery_user_SYS + rds147_gaussdbv5_slowquery_user_USER",
     CONVERT, "平台按系统库/业务库 × 系统用户/业务用户四条序列，需求和得总数"),
    ("Select慢查询数", None, "",
     "平台慢 SQL 仅按系统库/业务库拆分，无 Select/DML 维度"),
    ("DML慢查询数", None, "", "同上，平台无慢 SQL 的 DML 拆分"),
    ("每秒回滚事务数", "rds093_gaussv5_rollback_counter", DIRECT,
     "平台另有 _SYS/_USER 拆分"),
    ("服务可用性检测", "gaussdb_instance_status", APPROX,
     "平台无端口/服务探测指标，实例状态为最接近的可用量"),
    ("检测到的死锁次数", "rds091_gaussv5_deadlocks", DIRECT,
     "平台另有 _SYS_db/_USER_db 按库拆分"),
    ("回滚的事务总数", "rds093_gaussv5_rollback_counter", CONVERT,
     "平台为速率（次/秒），累计总数需按采样周期累加"),
    ("事务中已中止的空闲连接数", None, "", "平台无 idle in transaction (aborted) 口径"),
    ("TPS（每秒事务数）", "rds400_gaussv5_total_counter", DIRECT,
     "平台口径为用户总事务数/秒"),
    ("QPS（每秒查询数）", "rds402_gaussv5_ddl_dml_dcl_count", DIRECT,
     "平台口径为 DDL+DML+DCL 平均 SQL 执行数/秒"),
    ("服务端口{#PORT}检测", None, "", "平台无端口探测类指标"),
    ("GAUSSDB服务状态", "gaussdb_instance_status", APPROX,
     "平台为实例状态，非 pg_is_in_recovery 语义"),
    ("获取连接信息", None, "", "该项为 Agent 侧采集接口，非监控指标"),
    ("获取db性能数据", None, "", "该项为 Agent 侧采集接口，非监控指标"),
    ("获取数据库锁数据", "rds407_gaussv5_total_locks_count", APPROX,
     "平台为实例级锁总数（另有表级/行级/其他锁拆分），无按 DBNAME 维度"),
    ("({#NAME})被删除的行总数（包括已回收行）", None, "", "表级指标，平台无表维度"),
    ("启动时间", "rds133_cn_startup_time", APPROX,
     "平台为 CN 进程启动时刻(timestamp)，另含 DN/GTM/CMS/ETCD/CMA 进程"),
    ("查看当前锁等待数量", "rds195_gaussv5_locks_session", DIRECT,
     "平台口径为等锁会话数量；比率口径见 rds196_gaussv5_locks_ratio"),
    ("运行时长", "rds133_cn_startup_time", CONVERT,
     "平台无 uptime 指标，需用启动时刻与当前时间推算"),
    ("版本", None, "", "平台无实例版本指标"),
    ("正在运行的SQL", None, "",
     "平台仅提供 statement 数量（rds145_gaussdbv5_statement），无 SQL 明细"),
    ("({#NAME})已删除但未回收的行数（死亡行）", None, "", "表级指标，平台无表维度"),
    ("({#NAME})通过 HOT更新的行数", None, "", "表级指标，平台无表维度"),
    ("({#NAME})索引扫描次数", None, "", "表级指标，平台无表维度"),
    ("({#NAME})顺序扫描读取的行数", None, "", "表级指标，平台无表维度"),
    ("({#NAME})通过索引扫描获取的行数", None, "", "表级指标，平台无表维度"),
    ("({#NAME})插入的行数", None, "", "表级指标，平台无表维度"),
    ("({#NAME})当前活跃的行数", None, "", "表级指标，平台无表维度"),
    ("({#NAME})更新的行总数", None, "", "表级指标，平台无表维度"),
    ("({#NAME})顺序扫描次数", None, "", "表级指标，平台无表维度"),
    ("DB [{#DBNAME}]: 获取锁信息", "rds407_gaussv5_total_locks_count", APPROX,
     "平台按系统库/业务库统计，非按 DBNAME"),
    ("DB [{#DBNAME}]: Num of accessexclusive locks", None, "",
     "平台只有锁类型大类（表级/行级/其他），无 accessexclusive 细分"),
    ("[{#DATABASE_NAME}]库大小", "rds128_user_database_size", APPROX,
     "平台为用户数据库总大小（聚合），非按 DBNAME；系统库见 rds127_sys_database_size"),
    ("({#NAME})死亡行占比", None, "", "表级指标，平台无表维度"),
    ("字符集", None, "", "平台无字符集指标"),
    ("当前运行时间最长的Top10 SQL", None, "", "平台无 SQL 明细/排行接口"),
    ("数据库状态", "gaussdb_instance_status", APPROX, "平台为实例状态"),
    ("时区", None, "", "平台无时区指标（仅有时钟偏差 sys084_clock_synchronization_diff）"),
    ("页大小", None, "", "平台无页大小指标"),
    ("事务中空闲的连接数", None, "", "平台无 idle in transaction 口径"),
    ("写块的时间（毫秒）", "sys075_avg_disk_ms_per_write", APPROX,
     "平台为数据磁盘单次写入平均耗时，非数据库 blk_write_time 口径"),
    ("当前连接到数据库的后端进程数", "rds089_gaussv5_inuse_counter", DIRECT,
     "平台口径为 CN 连接数；实例 c35b8d07 上该指标无数据"),
    ("快路径函数调用的连接数", None, "", "平台无快路径调用口径"),
    ("总连接数", "rds089_gaussv5_inuse_counter", APPROX,
     "可用 rds143_gaussv5_online_session（在线会话数量）作为替代口径"),
    ("临时文件大小", None, "", "平台无临时文件指标"),
    ("从磁盘读取的块数", "rds411_gaussv5_blks_read_counter", CONVERT,
     "平台为速率（block/秒），累计块数需累加"),
    ("从缓存命中读取的块数", "rds412_gaussv5_blks_hit_counter", CONVERT,
     "平台为速率（block/秒），累计块数需累加"),
    ("删除的行数", None, "",
     "平台仅有 DELETE 语句数量/秒（rds405_gaussv5_delete_sql_count），无行数口径"),
    ("提交的事务总数", "rds092_gaussv5_commit_counter", CONVERT,
     "平台为速率（次/秒），累计总数需累加"),
    ("插入的行数", None, "",
     "平台仅有 INSERT 语句数量/秒（rds403_gaussv5_insert_sql_count），无行数口径"),
    ("数据库操作冲突次数", None, "", "平台无冲突次数指标"),
    ("更新的行数", None, "",
     "平台仅有 UPDATE 语句数量/秒（rds406_gaussv5_update_sql_count），无行数口径"),
    ("查询返回的行数", None, "", "平台无返回行数指标"),
    ("生成的临时文件数", None, "", "平台无临时文件指标"),
    ("通过索引获取的行数", None, "", "平台无索引取行数指标"),
    ("活动连接数", "rds117_gaussv5_active_session", DIRECT,
     "平台另有 _SYS/_USER 拆分"),
    ("禁用的连接数", None, "", "平台无禁用连接数口径"),
    ("空闲连接数", None, "", "平台无 idle 连接口径"),
    ("等待资源的连接数", "rds118_gaussv5_wait_session", DIRECT,
     "平台口径为等待会话数量；比率口径见 rds085_gaussv5_wait_ratio"),
    ("读块的时间（毫秒）", "sys076_avg_disk_ms_per_read", APPROX,
     "平台为数据磁盘单次读取平均耗时，非数据库 blk_read_time 口径"),
    ("连接使用率", "rds086_gaussv5_active_ratio", APPROX,
     "平台口径为活跃会话率；在线会话率见 rds144_online_ratio"),
    ("预提交事务数", None, "", "平台无预备事务（prepared transaction）指标"),
    ("发送设备名给Agent", None, "", "Agent 侧行为，不是监控指标"),
    ("({#NAME}) number of dead tuples", None, "", "表级指标，平台无表维度"),
    ("({#NAME}) number of delete tuples", None, "", "表级指标，平台无表维度"),
    ("({#NAME})Garbage ratio", None, "", "表级指标，平台无表维度"),
    ("({#NAME}) number of hot update tuples", None, "", "表级指标，平台无表维度"),
    ("({#NAME})number of index scan", None, "", "表级指标，平台无表维度"),
    ("({#NAME})number of index tuples read", None, "", "表级指标，平台无表维度"),
    ("({#NAME})number of live tuples", None, "", "表级指标，平台无表维度"),
    ("({#NAME})number of insert tuples", None, "", "表级指标，平台无表维度"),
    ("({#NAME})number of sequencial scan", None, "", "表级指标，平台无表维度"),
    ("({#NAME})number of sequencial tuples read", None, "", "表级指标，平台无表维度"),
    ("({#NAME})number of update tuples", None, "", "表级指标，平台无表维度"),
    ("Buffers Alloc", None, "", "平台无 bgwriter 类指标"),
    ("Buffers Backend", None, "", "平台无 bgwriter 类指标"),
    ("Buffers Backend Fsync", None, "", "平台无 bgwriter 类指标"),
    ("Buffers Checkpoint", None, "", "平台无 bgwriter 类指标"),
    ("Buffers Clean", None, "", "平台无 bgwriter 类指标"),
    ("Checkpoint count (by checkpoint segments)", None, "",
     "平台仅有待落盘数据量/脏页数量（rds105/rds125），无 checkpoint 次数"),
    ("Checkpoint count (by checkpoint timeout)", None, "", "同上"),
    ("Idle in transaction connections", None, "", "平台无 idle in transaction 口径"),
    ("Idle connections", None, "", "平台无 idle 连接口径"),
    ("Max Written Clean", None, "", "平台无 bgwriter 类指标"),
    ("Standby状态", "datanode_switch_status", APPROX,
     "平台为 DN 主备状态；备机 RTO/redo 进度见 rds080/rds083"),
    ("[{#DATNAME}] Commited transactions", None, "", "平台无按 DBNAME 维度的事务计数"),
    ("[{#DATNAME}] Active connections", None, "", "平台无按 DBNAME 维度的连接数"),
    ("[{#DATNAME}] Rolled back transactions", None, "", "平台无按 DBNAME 维度的事务计数"),
    ("当前连接数", "rds143_gaussv5_online_session", APPROX,
     "平台口径为在线会话数量；另有 rds089_gaussv5_inuse_counter（CN连接数）"),
    ("最大连接数", None, "",
     "平台 rds416_gaussv5_max_active_session 是历史最高活跃会话数，不是配置上限"),
    ("活跃连接数", "rds117_gaussv5_active_session", DIRECT, "同 57 号客户指标"),
    ("[{#DATNAME}]每秒Fetch记录数", None, "", "平台无按 DBNAME 维度的行数指标"),
    ("[{#DATNAME}]每秒Insert记录数", None, "", "平台无按 DBNAME 维度的行数指标"),
    ("[{#DATNAME}]每秒Return记录数", None, "", "平台无按 DBNAME 维度的行数指标"),
    ("[{#DATNAME}]每秒Update记录数", None, "", "平台无按 DBNAME 维度的行数指标"),
    ("[{#DATNAME}]缓存命中率", "rds090_gaussv5_buffer_hit_ratio", APPROX,
     "平台为实例级命中率，非按 DBNAME"),
    ("连接数使用率", "rds144_online_ratio", APPROX, "平台口径为在线会话率"),
    ("会话时间TOP10", None, "", "平台无会话明细/排行接口"),
    ("Primary状态", "datanode_switch_status", APPROX, "平台为 DN 主备状态"),
    ("[{#DATNAME}]每秒Delete记录数", None, "", "平台无按 DBNAME 维度的行数指标"),
]


def read_client_rows():
    """读客户 Excel，返回 (表头行, 数据行 list)。找不到文件直接退出。"""
    import openpyxl

    candidates = [p for p in glob.glob(os.path.join(ROOT, "*.xlsx"))
                  if "指标明细" in os.path.basename(p)]
    if not candidates:
        sys.exit("找不到客户指标 Excel（根目录 *指标明细*.xlsx）")
    wb = openpyxl.load_workbook(candidates[0], data_only=True)
    ws = wb["指标库"]
    rows = [list(r) for r in ws.iter_rows(values_only=True)]
    return rows[0], rows[1:]


def load_tpops_dict():
    with open(TPOPS_DICT, encoding="utf-8") as fh:
        return json.load(fh)


def check_against_excel(rows):
    """核对 MAPPING 与 Excel 行序一致，且引用的 TPOPS 指标真实存在。"""
    if len(MAPPING) != len(rows):
        sys.exit("MAPPING 条数 %d 与客户 Excel 行数 %d 不一致" % (len(MAPPING), len(rows)))
    for idx, (row, entry) in enumerate(zip(rows, MAPPING), start=1):
        expected_cn, tpops_metric = entry[0], entry[1]
        actual_cn = row[2]
        if actual_cn != expected_cn:
            sys.exit("第 %d 行客户中文名不一致: Excel=%r MAPPING=%r" % (idx, actual_cn, expected_cn))
        if tpops_metric is None:
            continue
        for one in tpops_metric.split(" + "):
            if one not in TP_OPS_BY_ID:
                sys.exit("第 %d 行引用了不存在的 TPOPS 指标: %s" % (idx, one))
            if not TP_OPS_BY_ID[one]["has_data"]:
                sys.exit("第 %d 行引用的 TPOPS 指标无数据: %s" % (idx, one))


def build(client_rows):
    items = []
    for row, entry in zip(client_rows, MAPPING):
        expected_cn, tpops_metric, match_level, note = entry
        tpops_name = None
        if tpops_metric is not None:
            parts = [TP_OPS_BY_ID[one]["metric_name"] for one in tpops_metric.split(" + ")]
            tpops_name = " + ".join(parts)
        items.append({
            "customer_metric": row[3],
            "customer_metric_name": row[2],
            "customer_category": row[1],
            "tpops_metric": tpops_metric,
            "tpops_metric_name": tpops_name,
            "status": PROVIDABLE if tpops_metric else UNPROVIDABLE,
            "match_level": match_level,
            "note": note,
        })
    return items


def summarize(items):
    by_status = {PROVIDABLE: 0, UNPROVIDABLE: 0}
    by_level = {}
    by_category = {}
    for it in items:
        by_status[it["status"]] += 1
        if it["match_level"]:
            by_level[it["match_level"]] = by_level.get(it["match_level"], 0) + 1
        cat = by_category.setdefault(it["customer_category"], {PROVIDABLE: 0, UNPROVIDABLE: 0})
        cat[it["status"]] += 1
    return by_status, by_level, by_category


def write_json(items, by_status, by_level, by_category):
    payload = {
        "inputs": {
            "client_metrics": "高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx / 指标库",
            "tpops_metrics": "data/tpops_all_metric.json",
        },
        "generated_by": "tools/build_customer_metric_mapping.py",
        "total": len(items),
        "statistics": {
            "total": len(items),
            "provided_by_tpops": by_status[PROVIDABLE],
            "not_provided_by_tpops": by_status[UNPROVIDABLE],
            "provided_ratio": round(by_status[PROVIDABLE] / float(len(items)), 4),
            "by_match_level": by_level,
            "by_customer_category": by_category,
        },
        "items": items,
    }
    with open(OUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    return payload


def write_unsupported_csv(client_rows):
    unsupported_idx = [i for i, e in enumerate(MAPPING) if e[1] is None]
    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.writer(fh)
        writer.writerow(CLIENT_COLUMNS)
        for i in unsupported_idx:
            writer.writerow(["" if v is None else v for v in client_rows[i][:len(CLIENT_COLUMNS)]])
    return unsupported_idx


def main():
    global TP_OPS_BY_ID
    header, client_rows = read_client_rows()
    if [h for h in header] != CLIENT_COLUMNS:
        sys.exit("客户 Excel 表头与预期不一致: %r" % (header,))
    tpops = load_tpops_dict()
    TP_OPS_BY_ID = dict((m["metric"], m) for m in tpops["metrics"])

    check_against_excel(client_rows)
    items = build(client_rows)
    by_status, by_level, by_category = summarize(items)
    payload = write_json(items, by_status, by_level, by_category)
    unsupported_idx = write_unsupported_csv(client_rows)

    print("客户指标 %d 条" % len(items))
    print("  TPOPS 可提供: %d" % by_status[PROVIDABLE])
    print("  TPOPS 无法提供: %d" % by_status[UNPROVIDABLE])
    print("  按匹配程度: %s" % by_level)
    print("写出 %s" % os.path.relpath(OUT_JSON, ROOT))
    print("写出 %s（%d 行）" % (os.path.relpath(OUT_CSV, ROOT), len(unsupported_idx)))


if __name__ == "__main__":
    main()
