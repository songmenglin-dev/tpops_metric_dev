#!/usr/bin/env python3
"""生成两份交付文档：

1. data/customer_metric_query_plan.json
   按客户指标分类，逐条给出：客户指标英文名/中文名 → TPOPS 对应指标英文名/中文名，
   以及该指标的查询方法（用到的 API、入参、预期结果）。

2. data/tpops_api_catalog.json
   本方案用到的全部 API 集合：URI、方法、入参、出参、覆盖的指标数。
   另含 platform_facts 段：平台侧只测一次的结论（API 全量清点、内置命令清单、
   pg_stat 视图字段、合并采集范式），这些结论无法从运行时重新推导，
   集中放在这里，避免散落在一次性报告里。

输入：data/customer_metric_mapping.json（101 条可提供性判定）
用法：python tools/build_metric_query_plan.py
"""

import json
import os
import sys
from collections import OrderedDict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
MAPPING = os.path.join(DATA_DIR, "customer_metric_mapping.json")
OUT_PLAN = os.path.join(DATA_DIR, "customer_metric_query_plan.json")
OUT_APIS = os.path.join(DATA_DIR, "tpops_api_catalog.json")

CMD_SQL_ID = "73722acc-bff6-4d04-adc0-bc1e1fa1eb7d"   # general_single_sql_query

# --------------------------------------------------------------------------
# API 定义
# --------------------------------------------------------------------------
APIS = OrderedDict()

APIS["POST /rds/v3/auth/mini-token"] = {
    "uri": "/rds/v3/auth/mini-token",
    "method": "POST",
    "purpose": "认证，取得 X-Mini-Token（后续所有接口的前置）",
    "headers": {"Content-Type": "application/json"},
    "path_params": {},
    "query_params": {},
    "body": {
        "username": "TPOPS 用户名（环境变量 TPOPS_USER）",
        "password": "TPOPS 密码（环境变量 TPOPS_PASSWORD）",
    },
    "response": {
        "headers": {"X-Mini-Token": "令牌，后续请求放 X-Mini-Token 头"},
        "body": "JSON，含用户信息与 login_failed_attempts 等",
    },
    "notes": "服务端 session_timeout 1440 分钟；登录失败会累计 login_failed_attempts 并锁号",
    "covers_metrics": 0,
    "is_prerequisite": True,
}

APIS["GET /rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node"] = {
    "uri": "/rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node",
    "method": "GET",
    "purpose": "取实例节点清单（node_id / component_id / role / type），是命令下发与 SQL 采集的前置",
    "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
    "path_params": {"instance_id": "实例 ID"},
    "query_params": {},
    "body": None,
    "response": {
        "nodes": [
            {"node_id": "节点 ID", "node_name": "节点名", "role": "master/slave",
             "type": "CN/DN", "component_id": "组件 ID", "node_ip": "节点 IP"}
        ]
    },
    "notes": "命令只允许在 master 节点执行；集中式实例 3 个 CN 均为 master，HA 实例只有 1 个 master DN",
    "covers_metrics": 0,
    "is_prerequisite": True,
}

APIS["POST /rds/v3/monitor/metrics"] = {
    "uri": "/rds/v3/monitor/metrics",
    "method": "POST",
    "purpose": "拉取监控指标时间序列（平台原生指标，共 234 个，16 个指标分组）",
    "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn", "Content-Type": "application/json"},
    "path_params": {},
    "query_params": {},
    "body": {
        "start_time": "起始时间（13 位毫秒时间戳），窗口 ≥ 1 小时",
        "end_time": "结束时间（13 位毫秒时间戳），需回退 5 分钟以避开入库延迟",
        "is_latest_point_only": "字符串 \"false\"；用 \"true\" 会让约 74% 的指标时间戳变成 0",
        "instances": "[{\"instance_id\": \"<实例ID>\"}]",
        "metric_names": "指标名数组，来自 GET /rds/v3/monitor/metrics/series",
    },
    "response": {
        "metric_infos": [
            {
                "instance_id": "实例 ID",
                "time_interval": "响应时间粒度（秒），2h 窗口为 60",
                "metrics": [
                    {"name": "指标英文名", "metric_name": "指标中文名",
                     "collection_frequency": "采集周期（秒）",
                     "timestamps": "时间戳数组", "series": "按主机 IP 展开的值数组"}
                ],
            }
        ]
    },
    "notes": "中文名在数据响应的 metric_name 字段里，目录接口 GET /rds/v3/monitor/metrics/series 不带中文名",
    "covers_metrics": 0,
    "is_prerequisite": False,
}

APIS["POST /rds/gaussdb/v3/instances/{instance_id}/slow-sql-list"] = {
    "uri": "/rds/gaussdb/v3/instances/{instance_id}/slow-sql-list",
    "method": "POST",
    "purpose": "查询慢 SQL 集合（慢查询总数、Select/DML 慢查询拆分）",
    "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn", "Content-Type": "application/json"},
    "path_params": {"instance_id": "实例 ID（须与 body 内一致）"},
    "query_params": {},
    "body": {
        "start_time": "起始时间（13 位毫秒时间戳）",
        "end_time": "结束时间（13 位毫秒时间戳）",
        "instance_id": "实例 ID",
        "threshold": "慢 SQL 阈值（ms 单位取 1~99000，s 单位取 1~99）",
        "threshold_unit": "可选，ms 或 s",
        "node_ids": "节点 ID 数组，来自 key-view-execute-node",
    },
    "response": {
        "slow_sql_infos": [
            {"schema_name": "模式名", "db_name": "库名", "sql_id": "SQL 唯一标识",
             "user_name": "用户", "node_id": "节点 ID", "node_name": "节点名",
             "sql_text": "SQL 文本", "query_plan": "执行计划",
             "calls": "调用次数", "avg_exec_time": "平均执行时间",
             "avg_cpu_time": "平均 CPU 时间", "avg_io_time": "平均 IO 时间",
             "avg_returned_rows": "平均返回行数", "avg_fetched_rows": "平均抓取行数",
             "buffer_hit_ratio": "buffer 命中率"}
        ]
    },
    "notes": "慢查询总数 = slow_sql_infos 长度；Select/DML 拆分按 sql_text 前缀统计。"
             "实测约束：查询时间窗口上限 24 小时，超出（48h/7d/30d）一律返回 400「参数错误」；"
             "2026-11-05 实测 24h 窗口返回 200，该实例当前无超过阈值的慢 SQL",
    "covers_metrics": 0,
    "is_prerequisite": False,
}

APIS["POST /rds/gaussdb/v3/instances/{instance_id}/top-sql-list"] = {
    "uri": "/rds/gaussdb/v3/instances/{instance_id}/top-sql-list",
    "method": "POST",
    "purpose": "查询 Top SQL 列表（当前运行时间最长的 Top10 SQL）",
    "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn", "Content-Type": "application/json"},
    "path_params": {"instance_id": "实例 ID"},
    "query_params": {},
    "body": {
        "instance_id": "实例 ID",
        "node_ids": "节点 ID 数组",
        "start_time": "起始时间（13 位毫秒时间戳）",
        "end_time": "结束时间（13 位毫秒时间戳）",
        "start_time_utc": "可选，起始时间字符串",
        "end_time_utc": "可选，结束时间字符串",
    },
    "response": {
        "top_sql_infos": [
            {"sql_id": "SQL 标识", "user_name": "用户", "sql_text": "SQL 文本",
             "calls": "调用次数", "calls_percent": "调用占比",
             "cpu_percent": "CPU 占比", "io_percent": "IO 占比",
             "returned_rows": "返回行数", "tuple_read": "扫描元组数",
             "avg_elapse_time": "平均耗时", "total_elapse_time": "总耗时",
             "min_elapse_time": "最小耗时", "max_elapse_time": "最大耗时",
             "sql_hit_ratio": "命中率", "node_id": "节点 ID",
             "node_name": "节点名", "db_name": "库名"}
        ],
        "total": "记录数",
    },
    "notes": "2026-11-05 实测可用（需传全部 CN 节点 id）",
    "covers_metrics": 0,
    "is_prerequisite": False,
}

APIS["POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}"] = {
    "uri": "/rds/gaussdb/v3/command-center/customer-commands/{cmd_id}",
    "method": "POST",
    "purpose": "统一命令中心：执行单条命令。general_single_sql_query 可执行任意 SQL，"
               "是本方案 82 个 pg_stat 类指标的主通道",
    "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn", "Content-Type": "application/json"},
    "path_params": {"cmd_id": "命令 ID；通用 SQL 用 " + CMD_SQL_ID + "（general_single_sql_query）"},
    "query_params": {},
    "body": {
        "instance_id": "实例 ID",
        "node_ids": "节点 ID 数组（只能 master 节点；实测一次下发多节点会失败，须逐节点下发）",
        "params": [
            {"param_name": "sql_text", "param_value": "要执行的 SQL；模板为 select * from ({sql_text}) limit {limit}"},
            {"param_name": "limit", "param_value": "返回行数上限"},
        ],
    },
    "response": {"job_id": "任务 ID", "node_tasks": [{"task_id": "节点任务 ID", "node_id": "节点 ID"}]},
    "notes": "GaussDB 的 SQL 方言为 PG 9.2 级：不支持 FILTER 子句，请用 CASE WHEN；"
             "实测方言坑：条件 state <> 空串 恒为假（会把全部会话过滤掉），排除后台进程请用 state is not null；"
             "2026-11-05 实测 50 条 SQL 中有 3 条偶发失败（重试即成功），采集端必须做重试；"
             "参数 sql_text 属敏感字段，任务记录里显示为 ******",
    "covers_metrics": 0,
    "is_prerequisite": False,
}

APIS["GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records"] = {
    "uri": "/rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records",
    "method": "GET",
    "purpose": "轮询命令执行任务状态与错误信息",
    "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
    "path_params": {"cmd_id": "命令 ID"},
    "query_params": {"job_id": "任务 ID（来自下发接口返回）"},
    "body": None,
    "response": {
        "total_count": "任务数",
        "job_status": "running/succeed/failed",
        "tasks": [{"task_id": "节点任务 ID", "node_id": "节点 ID", "node_name": "节点名",
                   "status": "running/succeed/failed", "params": "下发参数（sql_text 被掩码）",
                   "error_msg": "失败原因", "start_time": "开始时间", "finish_time": "结束时间"}],
    },
    "notes": "实测单次往返约 10~20 秒",
    "covers_metrics": 0,
    "is_prerequisite": True,
}

APIS["GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}/task-record/{task_id}/detail"] = {
    "uri": "/rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}/task-record/{task_id}/detail",
    "method": "GET",
    "purpose": "取命令执行结果",
    "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
    "path_params": {"cmd_id": "命令 ID", "job_id": "任务 ID", "task_id": "节点任务 ID"},
    "query_params": {},
    "body": None,
    "response": {
        "node_id": "节点 ID", "node_name": "节点名", "status": "succeed/failed",
        "result": "JSON 数组字符串（业务数据）", "out_format": "json",
        "out_mode": "array", "out_type": "map", "download_file_id": "转储文件 ID（大结果集）",
    },
    "notes": "result 需二次 json.loads 解析",
    "covers_metrics": 0,
    "is_prerequisite": True,
}

APIS["GET /rds/v3.2/instances"] = {
    "uri": "/rds/v3.2/instances",
    "method": "GET",
    "purpose": "实例列表与元数据（含引擎版本）",
    "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
    "path_params": {},
    "query_params": {"limit": "每页条数，默认 100", "offset": "偏移量"},
    "body": None,
    "response": {
        "total_count": "实例数",
        "instances": [{"id": "实例 ID", "name": "实例名", "mode": "Combined/Ha/Distributed",
                       "created_at": "创建时间", "instance_status": "normal",
                       "engine_name": "引擎名", "engine_version": "引擎版本，如 V2.0-10.0.0"}],
    },
    "notes": "另有 GET /rds/v3/version-info 返回服务版本 DBS-GaussDB-Version",
    "covers_metrics": 0,
    "is_prerequisite": False,
}

# --------------------------------------------------------------------------
# 平台侧只测一次的结论
# --------------------------------------------------------------------------
# 以下内容来自对平台 API 的全量探测，运行时无法重新推导（探测本身要逐个接口
# 发请求，且有 30 天历史留存等限制），集中记录在此，作为实现采集端的依据。
PLATFORM_FACTS = {
    "api_survey": {
        "method": "从 CHM 产品文档（14655 个 HTML 页面，gb18030 编码）提取全部 /v3/** "
                  "接口路径，共 147 个去重 API，再按两种前缀候选逐个实测",
        "prefix_candidates": {
            "/rds/gaussdb/v3/**": "102 个非 404（400=缺参数 45、200=可用 35、201=1、"
                                 "500=服务端错误 6、403=无权限 15）",
            "/rds/v3/**": "仅认证、实例列表、监控指标、告警这几个",
        },
        "conclusion": "两种前缀都真实存在：/rds/v3 是基础平台接口（监控、告警、认证），"
                      "/rds/gaussdb/v3 是数据库管理接口（命令中心、诊断优化）。"
                      "只查其中一种会漏掉大半指标。",
        "lesson": "「平台没有某指标」这种结论，必须以「该平台全部 API 都探测过」为前提，"
                  "否则就是把「没查到」当成「没有」。",
    },
    "builtin_commands": {
        "api": "GET /rds/gaussdb/v3/command-center/customer-commands",
        "count": 142,
        "note": "内置命令含 SQL / SHELL / PYTHON 三类，可直接用任意 SQL 的通用命令",
        "relevant": {
            "general_single_sql_query": "通用 SQL 查询，模板 select * from ({sql_text}) limit {limit};"
                                        "可执行任意 SQL，是 82 个 pg_stat 类指标的主通道",
            "dbs.full.get_stat_all_tables": "top 20 死元组占用（n_dead_tup / n_live_tup / dead_tup_ratio）",
            "query_slow_table_statistics": "用户表 tuple 统计（n_tup_ins/upd/del、n_live_tup、n_dead_tup、last_vacuum…）",
            "query_table_statistics_info": "指定表的统计信息",
            "query_database_info": "字符集（pg_encoding_to_char(encoding)）",
            "DBS.FULL.get_database_size": "各库大小",
            "query_table_locks_ha / query_table_locks_distribute": "表上锁（pg_locks）",
            "query_slow_sql_ha / query_slow_sql_distribute / query_history_slow_sql_records": "慢 SQL",
        },
    },
    "pg_stat_views": {
        "note": "字段已与实库 information_schema.columns 核对；GaussDB 的 pg_stat_activity "
                "无 wait_event_type/wait_event，等待类指标按 state 判定",
        "pg_stat_user_tables / pg_stat_all_tables": {
            "field_count": 22,
            "fields": "seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, "
                      "n_tup_del, n_tup_hot_upd, n_live_tup, n_dead_tup, last_vacuum, vacuum_count …",
        },
        "pg_stat_activity": {
            "field_count": 27,
            "fields": "datname, pid, state, query, query_start, waiting, unique_sql_id …",
        },
        "pg_stat_database": {
            "field_count": 19,
            "fields": "numbackends, xact_commit, xact_rollback, blks_read, blks_hit, "
                      "tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted, "
                      "conflicts, temp_files, temp_bytes, deadlocks, blk_read_time, blk_write_time",
        },
        "pg_stat_bgwriter": {
            "field_count": 11,
            "fields": "checkpoints_timed, checkpoints_req, buffers_checkpoint, buffers_clean, "
                      "maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc",
        },
    },
    "merged_collection": {
        "note": "命令中心单次往返 10~20 秒，82 个指标若各发一条 SQL 则一轮要 50 次往返，"
                "采集周期撑不住；应合并成少量几条 SQL 每轮下发一次",
        "verified_example": (
            "select (select row_to_json(b) from pg_stat_bgwriter b) as bgwriter, "
            "(select string_agg(name||'='||setting,' | ' order by name) from pg_settings "
            "where name in ('max_connections','TimeZone','block_size','port','shared_buffers')) as settings, "
            "(select row_to_json(d) from pg_stat_database d where datname='postgres') as dbstat, "
            "(select count(*) from pg_locks where mode='AccessExclusiveLock') as accessexclusive_locks, "
            "(select count(*) from pg_prepared_xacts) as prepared_xacts, "
            "(select string_agg(datname||':'||pg_encoding_to_char(encoding),',') from pg_database) as encodings"
        ),
        "verified_result_keys": ["bgwriter", "settings", "dbstat",
                                 "accessexclusive_locks", "prepared_xcts", "encodings"],
    },
    "execution_constraints": {
        "note": "命令详情里的 constraint_config 约束：执行节点角色/组件类型须满足"
                "（如 general_single_sql_query 要求 limit_node_types=[master]、"
                "limit_component_types=[cn,dn]），集群形态也须匹配",
        "risk": "执行自定义命令（来源非系统默认）属于风险操作，命令详情明确要求确认"
                "不会产生慢SQL、输出大小可控（建议每轮一条合并 SQL、limit 适中）",
        "masking": "任务记录里 sql_text 属敏感参数，会被掩码显示为 ******（执行本身不受影响）",
    },
    "cumulative_counters": {
        "note": "pg_stat_* 是累计量（自 stats_reset 起算），而 POST /rds/v3/monitor/metrics "
                "是真正的时序。Kafka 侧要统一成客户的「每秒/每分钟」口径，得在采集侧统一做差分，"
                "并处理实例重启导致的计数归零",
        "rate_metrics": [
            "GAUSSDB.[{#DATNAME}]_fetch_records_per_second",
            "GAUSSDB.[{#DATNAME}]_insert_records_per_second",
            "GAUSSDB.[{#DATNAME}]_return_records_per_second",
            "GAUSSDB.[{#DATNAME}]_update_records_per_second",
            "GAUSSDB.[{#DATNAME}]_delete_records_per_second",
        ],
    },
    "data_caveat": {
        "note": "当前两个实例创建于 2026-11-03，是空库：pg_stat_user_tables 无业务表、"
                "real-time-session 空、databases 空（仅系统库）。"
                "表级 22 项指标虽有字段，但当前实例上没有真实数据可验，"
                "需在有业务数据的实例上复验数值合理性",
    },
}

# --------------------------------------------------------------------------
# 每个来源对应的查询方法
# --------------------------------------------------------------------------
MONITOR_API = "POST /rds/v3/monitor/metrics"
SLOW_API = "POST /rds/gaussdb/v3/instances/{instance_id}/slow-sql-list"
TOP_API = "POST /rds/gaussdb/v3/instances/{instance_id}/top-sql-list"
CMD_API = "POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}"
INST_API = "GET /rds/v3.2/instances"

# 监控指标接口：每个 metric id 对应的预期结果说明
MONITOR_EXPECT = {
    "rds092_gaussv5_commit_counter": "series 按主机 IP 给出「用户提交事务数/秒」时序值",
    "rds093_gaussv5_rollback_counter": "series 按主机 IP 给出「用户回滚事务数/秒」时序值",
    "rds400_gaussv5_total_counter": "series 按主机 IP 给出「用户总事务数/秒（TPS）」时序值",
    "rds402_gaussv5_ddl_dml_dcl_count": "series 按主机 IP 给出「平均SQL执行数（QPS）」时序值",
    "gaussdb_instance_status": "series 给出「实例状态」取值（0=正常，非 0 为异常态）",
    "datanode_switch_status": "series 给出「DN主备状态」取值",
    "rds133_cn_startup_time": "series 给出 CN 进程启动时刻（timestamp，毫秒）",
    "rds117_gaussv5_active_session": "series 给出「活跃会话数量」时序值",
    "rds118_gaussv5_wait_session": "series 给出「等待会话数量」时序值",
    "rds195_gaussv5_locks_session": "series 给出「等锁会话数量」时序值",
    "rds407_gaussv5_total_locks_count": "series 给出「锁总数」时序值",
}


def sql_for(field):
    """按 TPOPS 指标字段生成 SQL 与预期结果描述。"""
    if field == "pg_database.encoding":
        return ("select datname, pg_encoding_to_char(encoding) as encoding from pg_database",
                "每库一行：datname + encoding（UTF8/SQL_ASCII…）")
    if field == "pg_database_size(datname)":
        return ("select datname, pg_database_size(datname) as size_bytes from pg_database "
                "order by size_bytes desc",
                "每库一行：datname + size_bytes")
    if field == "pg_locks":
        return ("select d.datname, l.locktype, l.mode, count(*) as cnt "
                "from pg_locks l join pg_database d on l.database = d.oid "
                "group by d.datname, l.locktype, l.mode",
                "按库/锁类型/模式聚合的锁数量（GaussDB 的 pg_locks 无 datname 列，须 join pg_database）")
    if field == "pg_locks.mode":
        return ("select d.datname, count(*) as cnt "
                "from pg_locks l join pg_database d on l.database = d.oid "
                "where l.mode = 'AccessExclusiveLock' group by d.datname",
                "按库统计 AccessExclusiveLock 锁数量（同上需 join pg_database）")
    if field == "pg_prepared_xacts":
        return ("select count(*) as prepared_xacts from pg_prepared_xacts",
                "预备事务总数")
    if field.startswith("pg_settings."):
        name = field.split(".", 1)[1]
        return ("select name, setting, unit from pg_settings where name = '%s'" % name,
                "该参数名与取值，如 %s = ..." % name)
    if field == "pg_stat_bgwriter":
        return ("select * from pg_stat_bgwriter", "后台写入进程统计单行")
    if field.startswith("pg_stat_bgwriter."):
        col = field.split(".", 1)[1]
        return ("select %s from pg_stat_bgwriter" % col,
                "单行单列：%s" % col)
    if field == "pg_stat_activity":
        return ("select datname, state, count(*) as cnt from pg_stat_activity "
                "where state is not null group by datname, state",
                "按库/会话状态聚合的会话数（state 为 NULL 的后台进程已被排除）")
    if field == "pg_stat_activity.state":
        return ("select state, count(*) as cnt from pg_stat_activity "
                "where state is not null group by state",
                "按会话状态聚合：idle / active / idle in transaction / idle in transaction (aborted)")
    if field == "pg_stat_activity.query":
        return ("select datname, usename, state, query_start, query from pg_stat_activity "
                "where state = 'active' and query <> '' order by query_start desc limit 20",
                "正在执行的会话与当前 SQL 文本")
    if field == "pg_stat_activity.query_start":
        return ("select datname, usename, query_start, query from pg_stat_activity "
                "where state = 'active' order by query_start limit 10",
                "会话列表，按 query_start 升序取前 10（时长最长的 10 个会话）")
    if field == "pg_stat_database + pg_stat_user_tables":
        return ("select d.datname, d.numbackends, d.xact_commit, d.xact_rollback, "
                "d.blks_read, d.blks_hit, t.schemaname, t.relname, t.n_live_tup, t.n_dead_tup, "
                "t.seq_scan, t.idx_scan, t.n_tup_ins, t.n_tup_upd, t.n_tup_del "
                "from pg_stat_database d left join pg_stat_user_tables t "
                "on d.datname = current_database() order by t.n_dead_tup desc limit 100",
                "库级统计 + 表级统计联合结果")
    if field == "pg_stat_database.blks_hit/(blks_hit+blks_read)":
        return ("select datname, blks_hit, blks_read, "
                "case when (blks_hit + blks_read) > 0 "
                "then round(blks_hit * 100.0 / (blks_hit + blks_read), 2) else 0 end "
                "as cache_hit_ratio from pg_stat_database",
                "每库的缓存命中率百分比")
    if field.startswith("pg_stat_user_tables."):
        col = field.split(".", 1)[1]
        if "/" in col:
            return ("select schemaname, relname, n_live_tup, n_dead_tup, "
                    "case when (n_live_tup + n_dead_tup) > 0 "
                    "then round(n_dead_tup * 100.0 / (n_live_tup + n_dead_tup), 2) else 0 end "
                    "as dead_tup_ratio from pg_stat_user_tables order by dead_tup_ratio desc",
                    "每表一行：模式名、表名、活跃/死亡行数与死亡占比")
        return ("select schemaname, relname, %s from pg_stat_user_tables" % col,
                "每表一行：模式名、表名、%s" % col)
    if field.startswith("pg_stat_database."):
        col = field.split(".", 1)[1]
        return ("select datname, %s from pg_stat_database" % col,
                "每库一行：datname + %s" % col)
    raise KeyError(field)


def build_query(item):
    """生成单条指标的查询方法。"""
    field = item["tpops_metric"]
    src = item["source"]
    if src == "监控指标接口":
        return {
            "api": MONITOR_API,
            "method": "POST",
            "uri": "/rds/v3/monitor/metrics",
            "request": {
                "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn",
                            "Content-Type": "application/json"},
                "body": {
                    "start_time": "<13 位毫秒时间戳，窗口 ≥ 1 小时>",
                    "end_time": "<13 位毫秒时间戳，回退 5 分钟>",
                    "is_latest_point_only": "false",
                    "instances": [{"instance_id": "<instance_id>"}],
                    "metric_names": [field],
                },
            },
            "expected_result": MONITOR_EXPECT.get(field, "series 给出该指标的时间序列值"),
            "related_apis": [MONITOR_API],
        }
    if src == "实例接口":
        return {
            "api": INST_API,
            "method": "GET",
            "uri": "/rds/v3.2/instances",
            "request": {
                "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
                "query_params": {"limit": 100, "offset": 0},
            },
            "expected_result": "instances[].engine_version 为引擎版本（如 V2.0-10.0.0）",
            "related_apis": [INST_API],
        }
    if src == "诊断优化接口":
        if field.startswith("slow-sql-list."):
            return {
                "api": SLOW_API, "method": "POST",
                "uri": "/rds/gaussdb/v3/instances/{instance_id}/slow-sql-list",
                "request": {
                    "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn",
                                "Content-Type": "application/json"},
                    "path_params": {"instance_id": "<instance_id>"},
                    "body": {
                        "start_time": "<13 位毫秒时间戳>",
                        "end_time": "<13 位毫秒时间戳>",
                        "instance_id": "<instance_id>",
                        "threshold": 1000, "threshold_unit": "ms",
                        "node_ids": ["<node_id>"],
                    },
                },
                "expected_result": "slow_sql_infos[] 每项含 sql_id / sql_text / calls / db_name；"
                                   "总数取数组长度，Select/DML 拆分按 sql_text 前缀统计",
                "related_apis": [SLOW_API,
                                 "GET /rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node"],
                "known_issue": "2026-11-05 实测传齐文档参数仍返回 400「参数错误」，落地前需调试",
            }
        return {
            "api": TOP_API, "method": "POST",
            "uri": "/rds/gaussdb/v3/instances/{instance_id}/top-sql-list",
            "request": {
                "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn",
                            "Content-Type": "application/json"},
                "path_params": {"instance_id": "<instance_id>"},
                "body": {
                    "instance_id": "<instance_id>",
                    "node_ids": ["<node_id>", "<node_id2>"],
                    "start_time": "<13 位毫秒时间戳>",
                    "end_time": "<13 位毫秒时间戳>",
                },
            },
            "expected_result": "top_sql_infos[] 每项含 sql_text / total_elapse_time / calls / db_name；"
                               "按 total_elapse_time 降序取前 10",
            "related_apis": [TOP_API,
                             "GET /rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node"],
        }
    # 命令中心 SQL
    sql, expect = sql_for(field)
    return {
        "api": CMD_API, "method": "POST",
        "uri": "/rds/gaussdb/v3/command-center/customer-commands/{cmd_id}",
        "request": {
            "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn",
                        "Content-Type": "application/json"},
            "path_params": {"cmd_id": CMD_SQL_ID},
            "body": {
                "instance_id": "<instance_id>",
                "node_ids": ["<node_id>"],
                "params": [
                    {"param_name": "sql_text", "param_value": sql},
                    {"param_name": "limit", "param_value": "100"},
                ],
            },
        },
        "expected_result": expect,
        "related_apis": [CMD_API,
                         "GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records",
                         "GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}/task-record/{task_id}/detail",
                         "GET /rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node"],
    }


def main():
    if not os.path.exists(MAPPING):
        sys.exit("缺少 %s，请先运行 tools/build_customer_metric_mapping.py" % MAPPING)
    with open(MAPPING, encoding="utf-8") as fh:
        mapping = json.load(fh)
    items = mapping["items"]

    # ---- 覆盖统计 ----
    api_cover = {}
    for it in items:
        if it["status"] != "可提供":
            continue
        api = build_query(it)["api"]
        api_cover[api] = api_cover.get(api, 0) + 1
    for api, n in api_cover.items():
        APIS[api]["covers_metrics"] = n

    # ---- 文档 1：逐指标查询方案（按客户指标分类）----
    cats = OrderedDict()
    for it in items:
        cat = it["customer_category"]
        cats.setdefault(cat, []).append(it)

    plan = {
        "generated_by": "tools/build_metric_query_plan.py",
        "inputs": {
            "customer_metrics": "高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx / 指标库",
            "availability_mapping": "data/customer_metric_mapping.json",
        },
        "statistics": {
            "total": len(items),
            "provided": mapping["statistics"]["provided_by_tpops"],
            "not_provided": mapping["statistics"]["not_provided_by_tpops"],
            "categories": len(cats),
        },
        "categories": [],
    }
    for cat, rows in cats.items():
        entries = []
        for it in rows:
            q = build_query(it) if it["status"] == "可提供" else None
            entries.append({
                "customer_metric": it["customer_metric"],
                "customer_metric_name": it["customer_metric_name"],
                "tpops_metric": it["tpops_metric"],
                "tpops_metric_name": it["tpops_metric_name"],
                "status": it["status"],
                "match_level": it["match_level"],
                "source": it["source"],
                "query": q,
                "not_provided_reason": it["note"] if q is None else None,
                "note": it["note"],
            })
        plan["categories"].append({
            "category": cat,
            "total": len(rows),
            "provided": sum(1 for e in entries if e["status"] == "可提供"),
            "not_provided": sum(1 for e in entries if e["status"] != "可提供"),
            "metrics": entries,
        })

    with open(OUT_PLAN, "w", encoding="utf-8") as fh:
        json.dump(plan, fh, ensure_ascii=False, indent=2)

    # ---- 文档 2：API 集合 ----
    catalog = {
        "generated_by": "tools/build_metric_query_plan.py",
        "base_url": "https://172.16.103.193:8002",
        "statistics": {
            "api_count": len(APIS),
            "metric_apis": sum(1 for a in APIS.values() if not a["is_prerequisite"]),
            "prerequisite_apis": sum(1 for a in APIS.values() if a["is_prerequisite"]),
            "covered_metrics": sum(a["covers_metrics"] for a in APIS.values()),
        },
        "platform_facts": PLATFORM_FACTS,
        "apis": [],
    }
    for key, meta in APIS.items():
        item = OrderedDict()
        item["id"] = key
        item["uri"] = meta["uri"]
        item["method"] = meta["method"]
        item["purpose"] = meta["purpose"]
        item["covers_metrics"] = meta["covers_metrics"]
        item["is_prerequisite"] = meta["is_prerequisite"]
        item["request"] = {
            "headers": meta["headers"],
            "path_params": meta["path_params"],
            "query_params": meta["query_params"],
            "body": meta["body"],
        }
        item["response"] = meta["response"]
        if meta.get("notes"):
            item["notes"] = meta["notes"]
        if meta.get("known_issue"):
            item["known_issue"] = meta["known_issue"]
        catalog["apis"].append(item)

    with open(OUT_APIS, "w", encoding="utf-8") as fh:
        json.dump(catalog, fh, ensure_ascii=False, indent=2)

    # ---- 自检 ----
    assert plan["statistics"]["total"] == len(items)
    assert sum(a["covers_metrics"] for a in APIS.values()) == mapping["statistics"]["provided_by_tpops"]
    used = set()
    for c in plan["categories"]:
        for e in c["metrics"]:
            if e["status"] == "可提供":
                used.add(e["query"]["api"])
    assert used <= set(APIS), used - set(APIS)
    print("写出 %s（%d 类 / %d 条）" % (os.path.relpath(OUT_PLAN, ROOT),
                                        len(cats), len(items)))
    print("写出 %s（%d 个 API，覆盖 %d 个指标）" % (os.path.relpath(OUT_APIS, ROOT), len(APIS),
                                                   catalog["statistics"]["covered_metrics"]))
    for key, a in APIS.items():
        print("   %-4s %-78s 覆盖 %d" % (a["method"], a["uri"], a["covers_metrics"]))


if __name__ == "__main__":
    main()
