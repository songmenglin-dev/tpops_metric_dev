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

# 诊断优化接口返回字段的中文名（TPOPS 指标字典里没有这些字段）
FIELD_CN = {
    "slow-sql-list.sql_text": "慢SQL集合的 SQL 文本",
    "slow-sql-list.calls": "慢SQL调用次数",
    "slow-sql-list.avg_exec_time": "慢SQL平均执行时间",
    "slow-sql-list.buffer_hit_ratio": "慢SQL buffer 命中率",
    "real-time-session.sessions": "实时会话列表",
    "real-time-session.state": "会话状态",
    "real-time-session.query": "会话当前执行的 SQL",
    "real-time-session.query_runtime": "会话运行时长",
    "real-time-session.trans_num": "会话事务数",
    "real-time-session.rollback_num": "会话回滚次数",
    "real-time-session.sql_num": "会话SQL数",
    "real-time-session.database_name": "会话所属数据库",
    "real-time-session.user_name": "会话用户",
    "top-sql-list.sql_text": "Top SQL 的 SQL 文本",
    "top-sql-list.total_elapse_time": "SQL 总执行时间",
    "top-sql-list.avg_elapse_time": "SQL 平均执行时间",
    "top-sql-list.calls": "SQL 调用次数",
    "top-sql-list.returned_rows": "SQL 返回行数",
    "top-sql-list.tuple_read": "SQL 扫描元组数",
    "top-sql-list.sql_hit_ratio": "SQL 命中率",
    "top-sql-list.cpu_percent": "SQL 占用 CPU 比例",
    "database-table-analysis": "表级分析信息",
    "database-table-analysis.tuple_insert": "表级插入行数",
    "database-table-analysis.tuple_update": "表级更新行数",
    "database-table-analysis.tuple_delete": "表级删除行数",
    "database-table-analysis.tuple_hot_update": "表级 HOT 更新行数",
    "database-table-analysis.seq_scan": "表级顺序扫描次数",
    "database-table-analysis.seq_tup_read": "表级顺序扫描读取行数",
    "database-table-analysis.index_scan": "表级索引扫描次数",
    "database-table-analysis.index_tup_fetch": "表级索引扫描获取行数",
    "database-index-analysis": "索引级分析信息",
    "database-index-analysis.index_scan": "索引扫描次数",
    "database-index-analysis.index_tup_read": "索引项数",
    "database-index-analysis.index_tup_fetch": "索引扫描获取行数",
    "instances.engine_version": "引擎版本",
    "instances.instance_status": "实例状态",
    "version-info": "服务版本信息",
}

FIELD_CN.update({
    "pg_stat_user_tables.n_live_tup": "表活跃行数",
    "pg_stat_user_tables.n_dead_tup": "表死亡行数",
    "pg_stat_user_tables.n_tup_ins": "表累计插入行数",
    "pg_stat_user_tables.n_tup_upd": "表累计更新行数",
    "pg_stat_user_tables.n_tup_del": "表累计删除行数",
    "pg_stat_user_tables.n_tup_hot_upd": "表累计 HOT 更新行数",
    "pg_stat_user_tables.seq_scan": "表顺序扫描次数",
    "pg_stat_user_tables.seq_tup_read": "表顺序扫描读取行数",
    "pg_stat_user_tables.idx_scan": "表索引扫描次数",
    "pg_stat_user_tables.idx_tup_fetch": "表索引扫描获取行数",
    "pg_stat_user_tables.n_dead_tup/(n_live_tup+n_dead_tup)": "表死亡行占比",
    "pg_stat_all_tables.n_live_tup": "表活跃行数",
    "pg_stat_all_tables.n_dead_tup": "表死亡行数",
    "pg_stat_activity": "实时会话视图",
    "pg_stat_activity.state": "会话状态",
    "pg_stat_activity.query": "会话当前 SQL",
    "pg_stat_activity.query_start": "会话查询开始时刻",
    "pg_stat_database": "库级统计视图",
    "pg_stat_database.numbackends": "当前连接的后端进程数",
    "pg_stat_database.xact_commit": "累计提交事务数",
    "pg_stat_database.xact_rollback": "累计回滚事务数",
    "pg_stat_database.blks_read": "从磁盘读取的块数",
    "pg_stat_database.blks_hit": "从缓存命中读取的块数",
    "pg_stat_database.tup_returned": "查询返回的行数",
    "pg_stat_database.tup_fetched": "通过索引获取的行数",
    "pg_stat_database.tup_inserted": "插入的行数",
    "pg_stat_database.tup_updated": "更新的行数",
    "pg_stat_database.tup_deleted": "删除的行数",
    "pg_stat_database.conflicts": "数据库操作冲突次数",
    "pg_stat_database.temp_files": "生成的临时文件数",
    "pg_stat_database.temp_bytes": "临时文件大小",
    "pg_stat_database.deadlocks": "死锁次数",
    "pg_stat_database.blk_read_time": "读块的时间",
    "pg_stat_database.blk_write_time": "写块的时间",
    "pg_stat_database.blks_hit/(blks_hit+blks_read)": "缓存命中率",
    "pg_stat_bgwriter": "后台写入进程统计视图",
    "pg_stat_bgwriter.buffers_alloc": "Buffers Alloc",
    "pg_stat_bgwriter.buffers_backend": "Buffers Backend",
    "pg_stat_bgwriter.buffers_backend_fsync": "Buffers Backend Fsync",
    "pg_stat_bgwriter.buffers_checkpoint": "Buffers Checkpoint",
    "pg_stat_bgwriter.buffers_clean": "Buffers Clean",
    "pg_stat_bgwriter.maxwritten_clean": "Max Written Clean",
    "pg_stat_bgwriter.checkpoints_req": "按需触发的 checkpoint 次数",
    "pg_stat_bgwriter.checkpoints_timed": "按超时触发的 checkpoint 次数",
    "pg_settings.TimeZone": "时区",
    "pg_settings.max_connections": "最大连接数",
    "pg_settings.block_size": "页大小",
    "pg_settings.port": "服务端口",
    "pg_database.encoding": "字符集",
    "pg_locks": "锁信息视图",
    "pg_locks.mode": "锁模式",
    "pg_prepared_xacts": "预备事务视图",
    "pg_database_size(datname)": "数据库大小",
    "slow-sql-list.sql_id": "慢SQL唯一标识",
})

# 平台已实现的诊断优化接口（2026-11-05 实测，路径前缀 /rds/gaussdb/v3/instances/{id}）
KNOWN_SOURCES = {
    "pg_stat_user_tables", "pg_stat_all_tables", "pg_stat_activity", "pg_stat_database",
    "pg_stat_bgwriter", "pg_settings", "pg_database", "pg_locks", "pg_prepared_xacts",
    "pg_database_size", "pg_encoding_to_char",
}

KNOWN_APIS = {
    "key-view-execute-node", "real-time-session", "kill-session", "kill-free-session",
    "database-table-analysis", "database-index-analysis", "top-sql-list",
    "slow-sql-list", "slow-sql-detail", "slow-sql-execute-node",
    "wdr-snapshot", "wdr-snapshots", "sql-patch", "handle-sql-patch",
    "databases", "tables", "schemas",
}

# (客户中文名, TPOPS 指标英文名/接口字段 或 None, 匹配程度, 备注, 来源)
MAPPING = [
    ("每秒提交事务数", "rds092_gaussv5_commit_counter", "直接对应",
     "平台为速率口径；累计值见 pg_stat_database.xact_commit", "监控指标接口"),
    ("慢查询总数", "slow-sql-list.sql_id", "可统计",
     "慢SQL集合条数统计", "诊断优化接口"),
    ("Select慢查询数", "slow-sql-list.sql_text", "可统计",
     "按 sql_text 统计 SELECT 类慢查询", "诊断优化接口"),
    ("DML慢查询数", "slow-sql-list.sql_text", "可统计",
     "按 sql_text 统计 DML 类慢查询", "诊断优化接口"),
    ("每秒回滚事务数", "rds093_gaussv5_rollback_counter", "直接对应",
     "平台为速率口径", "监控指标接口"),
    ("服务可用性检测", "gaussdb_instance_status", "近似对应",
     "另可用 GET /rds/health-status 探测服务健康", "监控指标接口"),
    ("检测到的死锁次数", "pg_stat_database.deadlocks", "直接对应",
     "平台另有 rds091_gaussv5_deadlocks", "命令中心SQL"),
    ("回滚的事务总数", "pg_stat_database.xact_rollback", "直接对应",
     "累计回滚事务数", "命令中心SQL"),
    ("事务中已中止的空闲连接数", "pg_stat_activity.state", "直接对应",
     "state='idle in transaction (aborted)'", "命令中心SQL"),
    ("TPS（每秒事务数）", "rds400_gaussv5_total_counter", "直接对应",
     "平台为用户总事务数/秒", "监控指标接口"),
    ("QPS（每秒查询数）", "rds402_gaussv5_ddl_dml_dcl_count", "直接对应",
     "平台口径为 DDL+DML+DCL 平均SQL执行数/秒", "监控指标接口"),
    ("服务端口{#PORT}检测", "pg_settings.port", "近似对应",
     "仅能取到端口号，可用性探测需 GET /rds/health-status", "命令中心SQL"),
    ("GAUSSDB服务状态", "gaussdb_instance_status", "近似对应",
     "平台为实例状态，非 pg_is_in_recovery 语义", "监控指标接口"),
    ("获取连接信息", "pg_stat_activity", "直接对应",
     "会话清单（27 个字段）", "命令中心SQL"),
    ("获取db性能数据", "pg_stat_database + pg_stat_user_tables", "直接对应",
     "库级 + 表级统计视图", "命令中心SQL"),
    ("获取数据库锁数据", "pg_locks", "直接对应",
     "pg_locks 全量锁视图", "命令中心SQL"),
    ("({#NAME})被删除的行总数（包括已回收行）", "pg_stat_user_tables.n_tup_del", "直接对应",
     "累计删除行数", "命令中心SQL"),
    ("启动时间", "rds133_cn_startup_time", "近似对应",
     "平台为 CN 进程启动时刻(timestamp)", "监控指标接口"),
    ("查看当前锁等待数量", "pg_stat_activity.state", "直接对应",
     "state 含 lock 相关会话", "命令中心SQL"),
    ("运行时长", "rds133_cn_startup_time", "需换算",
     "平台无 uptime 指标，用启动时刻与当前时间推算", "监控指标接口"),
    ("版本", "instances.engine_version", "直接对应",
     "GET /rds/v3.2/instances；另有 GET /rds/v3/version-info", "实例接口"),
    ("正在运行的SQL", "pg_stat_activity.query", "直接对应",
     "会话当前执行的 SQL 文本", "命令中心SQL"),
    ("({#NAME})已删除但未回收的行数（死亡行）", "pg_stat_user_tables.n_dead_tup", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})通过 HOT更新的行数", "pg_stat_user_tables.n_tup_hot_upd", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})索引扫描次数", "pg_stat_user_tables.idx_scan", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})顺序扫描读取的行数", "pg_stat_user_tables.seq_tup_read", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})通过索引扫描获取的行数", "pg_stat_user_tables.idx_tup_fetch", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})插入的行数", "pg_stat_user_tables.n_tup_ins", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})当前活跃的行数", "pg_stat_user_tables.n_live_tup", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})更新的行总数", "pg_stat_user_tables.n_tup_upd", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})顺序扫描次数", "pg_stat_user_tables.seq_scan", "直接对应",
     "", "命令中心SQL"),
    ("DB [{#DBNAME}]: 获取锁信息", "pg_locks", "近似对应",
     "按 datname 聚合，平台无按 DBNAME 的锁指标", "命令中心SQL"),
    ("DB [{#DBNAME}]: Num of accessexclusive locks", "pg_locks.mode", "直接对应",
     "mode='AccessExclusiveLock' 按库聚合", "命令中心SQL"),
    ("[{#DATABASE_NAME}]库大小", "pg_database_size(datname)", "直接对应",
     "平台另有 rds128 聚合值", "命令中心SQL"),
    ("({#NAME})死亡行占比", "pg_stat_user_tables.n_dead_tup/(n_live_tup+n_dead_tup)", "直接对应",
     "", "命令中心SQL"),
    ("字符集", "pg_database.encoding", "直接对应",
     "pg_encoding_to_char()", "命令中心SQL"),
    ("当前运行时间最长的Top10 SQL", "top-sql-list.total_elapse_time", "直接对应",
     "按 total_elapse_time 排序取 Top N", "诊断优化接口"),
    ("数据库状态", "gaussdb_instance_status", "近似对应",
     "平台为实例状态", "监控指标接口"),
    ("时区", "pg_settings.TimeZone", "直接对应",
     "", "命令中心SQL"),
    ("页大小", "pg_settings.block_size", "直接对应",
     "实测 8192", "命令中心SQL"),
    ("事务中空闲的连接数", "pg_stat_activity.state", "直接对应",
     "state='idle in transaction'", "命令中心SQL"),
    ("写块的时间（毫秒）", "pg_stat_database.blk_write_time", "直接对应",
     "累计写块耗时，口径与客户一致", "命令中心SQL"),
    ("当前连接到数据库的后端进程数", "pg_stat_database.numbackends", "直接对应",
     "", "命令中心SQL"),
    ("快路径函数调用的连接数", None, "",
     "PG 侧无 fastpath 计数器；pg_settings 仅有 gsi_fastpath_level 开关", ""),
    ("总连接数", "pg_stat_database.numbackends", "直接对应",
     "", "命令中心SQL"),
    ("临时文件大小", "pg_stat_database.temp_bytes", "直接对应",
     "", "命令中心SQL"),
    ("从磁盘读取的块数", "pg_stat_database.blks_read", "直接对应",
     "", "命令中心SQL"),
    ("从缓存命中读取的块数", "pg_stat_database.blks_hit", "直接对应",
     "", "命令中心SQL"),
    ("删除的行数", "pg_stat_database.tup_deleted", "直接对应",
     "", "命令中心SQL"),
    ("提交的事务总数", "pg_stat_database.xact_commit", "直接对应",
     "", "命令中心SQL"),
    ("插入的行数", "pg_stat_database.tup_inserted", "直接对应",
     "", "命令中心SQL"),
    ("数据库操作冲突次数", "pg_stat_database.conflicts", "直接对应",
     "", "命令中心SQL"),
    ("更新的行数", "pg_stat_database.tup_updated", "直接对应",
     "", "命令中心SQL"),
    ("查询返回的行数", "pg_stat_database.tup_returned", "直接对应",
     "", "命令中心SQL"),
    ("生成的临时文件数", "pg_stat_database.temp_files", "直接对应",
     "", "命令中心SQL"),
    ("通过索引获取的行数", "pg_stat_database.tup_fetched", "直接对应",
     "", "命令中心SQL"),
    ("活动连接数", "pg_stat_activity.state", "直接对应",
     "state='active'；平台另有 rds117", "命令中心SQL"),
    ("禁用的连接数", None, "",
     "PG 无「禁用连接」概念；pg_settings 仅有 reserved_connections 等预留连接", ""),
    ("空闲连接数", "pg_stat_activity.state", "直接对应",
     "state='idle'", "命令中心SQL"),
    ("等待资源的连接数", "pg_stat_activity.state", "直接对应",
     "state 含 wait 类会话", "命令中心SQL"),
    ("读块的时间（毫秒）", "pg_stat_database.blk_read_time", "直接对应",
     "累计读块耗时，口径与客户一致", "命令中心SQL"),
    ("连接使用率", "pg_stat_activity.state", "近似对应",
     "活跃会话数 / 总连接数", "命令中心SQL"),
    ("预提交事务数", "pg_prepared_xacts", "直接对应",
     "count(*) 统计预备事务", "命令中心SQL"),
    ("发送设备名给Agent", None, "",
     "Agent 侧行为，不是数据库可查询的监控指标", ""),
    ("({#NAME}) number of dead tuples", "pg_stat_user_tables.n_dead_tup", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME}) number of delete tuples", "pg_stat_user_tables.n_tup_del", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})Garbage ratio", "pg_stat_user_tables.n_dead_tup/(n_live_tup+n_dead_tup)", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME}) number of hot update tuples", "pg_stat_user_tables.n_tup_hot_upd", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})number of index scan", "pg_stat_user_tables.idx_scan", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})number of index tuples read", "pg_stat_user_tables.idx_tup_fetch", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})number of live tuples", "pg_stat_user_tables.n_live_tup", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})number of insert tuples", "pg_stat_user_tables.n_tup_ins", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})number of sequencial scan", "pg_stat_user_tables.seq_scan", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})number of sequencial tuples read", "pg_stat_user_tables.seq_tup_read", "直接对应",
     "", "命令中心SQL"),
    ("({#NAME})number of update tuples", "pg_stat_user_tables.n_tup_upd", "直接对应",
     "", "命令中心SQL"),
    ("Buffers Alloc", "pg_stat_bgwriter.buffers_alloc", "直接对应",
     "", "命令中心SQL"),
    ("Buffers Backend", "pg_stat_bgwriter.buffers_backend", "直接对应",
     "", "命令中心SQL"),
    ("Buffers Backend Fsync", "pg_stat_bgwriter.buffers_backend_fsync", "直接对应",
     "", "命令中心SQL"),
    ("Buffers Checkpoint", "pg_stat_bgwriter.buffers_checkpoint", "直接对应",
     "", "命令中心SQL"),
    ("Buffers Clean", "pg_stat_bgwriter.buffers_clean", "直接对应",
     "", "命令中心SQL"),
    ("Checkpoint count (by checkpoint segments)", "pg_stat_bgwriter.checkpoints_req", "直接对应",
     "按需触发（requested）的 checkpoint 次数", "命令中心SQL"),
    ("Checkpoint count (by checkpoint timeout)", "pg_stat_bgwriter.checkpoints_timed", "直接对应",
     "按超时触发的 checkpoint 次数", "命令中心SQL"),
    ("Idle in transaction connections", "pg_stat_activity.state", "直接对应",
     "state='idle in transaction'", "命令中心SQL"),
    ("Idle connections", "pg_stat_activity.state", "直接对应",
     "state='idle'", "命令中心SQL"),
    ("Max Written Clean", "pg_stat_bgwriter.maxwritten_clean", "直接对应",
     "", "命令中心SQL"),
    ("Standby状态", "datanode_switch_status", "近似对应",
     "平台为 DN 主备状态", "监控指标接口"),
    ("[{#DATNAME}] Commited transactions", "pg_stat_database.xact_commit", "直接对应",
     "按 datname 取累计值", "命令中心SQL"),
    ("[{#DATNAME}] Active connections", "pg_stat_activity", "直接对应",
     "按 datname 统计活跃会话", "命令中心SQL"),
    ("[{#DATNAME}] Rolled back transactions", "pg_stat_database.xact_rollback", "直接对应",
     "按 datname 取累计值", "命令中心SQL"),
    ("当前连接数", "pg_stat_database.numbackends", "直接对应",
     "", "命令中心SQL"),
    ("最大连接数", "pg_settings.max_connections", "直接对应",
     "实测 200", "命令中心SQL"),
    ("活跃连接数", "pg_stat_activity.state", "直接对应",
     "state='active'", "命令中心SQL"),
    ("[{#DATNAME}]每秒Fetch记录数", "pg_stat_database.tup_fetched", "需换算",
     "累计值，每秒速率需按周期差分", "命令中心SQL"),
    ("[{#DATNAME}]每秒Insert记录数", "pg_stat_database.tup_inserted", "需换算",
     "累计值，每秒速率需按周期差分", "命令中心SQL"),
    ("[{#DATNAME}]每秒Return记录数", "pg_stat_database.tup_returned", "需换算",
     "累计值，每秒速率需按周期差分", "命令中心SQL"),
    ("[{#DATNAME}]每秒Update记录数", "pg_stat_database.tup_updated", "需换算",
     "累计值，每秒速率需按周期差分", "命令中心SQL"),
    ("[{#DATNAME}]缓存命中率", "pg_stat_database.blks_hit/(blks_hit+blks_read)", "直接对应",
     "", "命令中心SQL"),
    ("连接数使用率", "pg_stat_activity.state", "近似对应",
     "在线会话数 / 总连接数", "命令中心SQL"),
    ("会话时间TOP10", "pg_stat_activity.query_start", "直接对应",
     "按 query_start 排序取 Top N", "命令中心SQL"),
    ("Primary状态", "datanode_switch_status", "近似对应",
     "平台为 DN 主备状态", "监控指标接口"),
    ("[{#DATNAME}]每秒Delete记录数", "pg_stat_database.tup_deleted", "需换算",
     "累计值，每秒速率需按周期差分", "命令中心SQL"),
]


def read_client_rows():
    """读客户 Excel，返回 (表头行, 数据行 list)。找不到文件直接退出。"""
    import openpyxl

    candidates = [p for p in glob.glob(os.path.join(ROOT, "*.xlsx"))
                  if "指标明细" in os.path.basename(p)
                  and not os.path.basename(p).startswith("~$")]
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
    """核对 MAPPING 与 Excel 行序一致，且引用的 TPOPS 指标/接口真实存在。"""
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
            if one in TP_OPS_BY_ID:
                if not TP_OPS_BY_ID[one]["has_data"]:
                    sys.exit("第 %d 行引用的 TPOPS 指标无数据: %s" % (idx, one))
                continue
            if one in TP_OPS_BY_ID:
                continue
            head = one.split(".")[0].split("(")[0]
            if head not in KNOWN_APIS and head not in KNOWN_SOURCES \
                    and head not in ("instances", "version-info"):
                sys.exit("第 %d 行引用了未知来源: %s" % (idx, one))


def build(client_rows):
    items = []
    for row, entry in zip(client_rows, MAPPING):
        expected_cn, tpops_metric, match_level, note, source = entry
        tpops_name = None
        if tpops_metric is not None:
            names = []
            for one in tpops_metric.split(" + "):
                if one in TP_OPS_BY_ID:
                    names.append(TP_OPS_BY_ID[one]["metric_name"])
                else:
                    names.append(FIELD_CN.get(one, one))
            tpops_name = " + ".join(names)
        items.append({
            "customer_metric": row[3],
            "customer_metric_name": row[2],
            "customer_category": row[1],
            "tpops_metric": tpops_metric,
            "tpops_metric_name": tpops_name,
            "source": source,
            "status": PROVIDABLE if tpops_metric else UNPROVIDABLE,
            "match_level": match_level,
            "note": note,
        })
    return items


def summarize(items):
    by_status = {PROVIDABLE: 0, UNPROVIDABLE: 0}
    by_level = {}
    by_source = {}
    by_category = {}
    for it in items:
        by_status[it["status"]] += 1
        if it["match_level"]:
            by_level[it["match_level"]] = by_level.get(it["match_level"], 0) + 1
        if it["source"]:
            by_source[it["source"]] = by_source.get(it["source"], 0) + 1
        cat = by_category.setdefault(it["customer_category"], {PROVIDABLE: 0, UNPROVIDABLE: 0})
        cat[it["status"]] += 1
    return by_status, by_level, by_source, by_category


def write_json(items, by_status, by_level, by_source, by_category):
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
            "by_source": by_source,
            "by_customer_category": by_category,
        },
        "items": items,
    }
    with open(OUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    return payload


def write_unsupported_csv(client_rows):
    unsupported_idx = [i for i, e in enumerate(MAPPING) if e[1] is None]
    tmp = OUT_CSV + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.writer(fh)
        writer.writerow(CLIENT_COLUMNS)
        for i in unsupported_idx:
            writer.writerow(["" if v is None else v for v in client_rows[i][:len(CLIENT_COLUMNS)]])
    try:
        os.replace(tmp, OUT_CSV)
    except OSError as exc:
        sys.exit("无法写入 %s（可能正被 Excel/编辑器占用，请关闭后重跑）: %s" % (OUT_CSV, exc))
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
    by_status, by_level, by_source, by_category = summarize(items)
    payload = write_json(items, by_status, by_level, by_source, by_category)
    unsupported_idx = write_unsupported_csv(client_rows)

    print("客户指标 %d 条" % len(items))
    print("  TPOPS 可提供: %d" % by_status[PROVIDABLE])
    print("  TPOPS 无法提供: %d" % by_status[UNPROVIDABLE])
    print("  按匹配程度: %s" % by_level)
    print("  按来源: %s" % by_source)
    print("写出 %s" % os.path.relpath(OUT_JSON, ROOT))
    print("写出 %s（%d 行）" % (os.path.relpath(OUT_CSV, ROOT), len(unsupported_idx)))


if __name__ == "__main__":
    main()
