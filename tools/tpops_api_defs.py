#!/usr/bin/env python3
"""TPOPS API 目录定义（数据部分）。

本模块只放数据：方案里用到的每个 API 的 URI、方法、入参、出参、注意事项，
以及平台侧只测一次的结论（platform_facts）。

这些内容来自对平台 API 的全量探测，运行时无法重新推导（探测本身要逐个接口
发请求，且有 30 天历史留存等限制），集中记录在此，作为实现采集端的依据。

由 build_metric_query_plan.py 导入使用。
"""

from collections import OrderedDict

CMD_SQL_ID = "73722acc-bff6-4d04-adc0-bc1e1fa1eb7d"   # general_single_sql_query

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
    "notes": "命令只允许在 master 节点执行；分布式实例（mode=Combined）3 个 CN 均为 master，集中式实例（mode=Ha）只有 1 个 master DN。"
             "一次下发全部 master 节点实测成功（2026-11-06，HTTP 200，3 条 node_tasks 全部 succeed，见 "
             "data/command_center/results/05_execute_sql_6374.json 与 data/multi_master_probe/）；"
             "节点清单里同一 node_id 会以 CN / DN 各出现一次（gauss-6374：12 条条目 / 3 个唯一 node_id / 6 条 role=master），"
             "下发前必须按 node_id 去重，否则会报「节点数量超出限制」",
    "covers_metrics": 0,
    "is_prerequisite": True,
    "flow_step": 1,
    "flow_of": "POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}",
}

APIS["POST /rds/v3/monitor/metrics"] = {
    "uri": "/rds/v3/monitor/metrics",
    "method": "POST",
    "purpose": "拉取监控指标时间序列（平台原生监控指标，12 个指标分组；分组码见 docs/api/API参考-监控大盘.md）",
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
        "node_ids": "节点 ID 数组（只能 master 节点；一次下发全部 master 实测成功，"
                    "但必须先按 node_id 去重 —— 节点清单里同一 node_id 会以 CN / DN 各出现一次）",
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
    "purpose": "轮询命令执行任务状态与错误信息（命令中心三跳的第 2 跳）",
    "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
    "path_params": {"cmd_id": "命令 ID"},
    "query_params": {"job_id": "任务 ID（来自下发接口返回）", "offset": 0, "limit": 100},
    "body": None,
    "response": {
        "total_count": "任务数",
        "statistics": {"total": "任务总数", "succeed": "成功数", "failed": "失败数",
                       "running": "运行中数", "other": "其它状态数"},
        "job_status": "running/succeed/failed",
        "tasks": [{"task_id": "节点任务 ID", "node_id": "节点 ID", "node_name": "节点名",
                   "status": "running/succeed/failed", "params": "下发参数（sql_text 被掩码）",
                   "error_msg": "失败原因", "start_time": "开始时间", "finish_time": "结束时间"}],
    },
    "notes": "结束判据用 statistics.running == 0，不要看 job_status —— 实测节点任务已 succeed "
             "而 job_status 仍停在 running；单次往返 3~8 秒（节点执行 2.3~6.6 秒 + 下发与轮询开销）",
    "covers_metrics": 0,
    "is_prerequisite": True,
    "flow_step": 2,
    "flow_of": "POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}",
}

APIS["GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}/task-record/{task_id}/detail"] = {
    "uri": "/rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}/task-record/{task_id}/detail",
    "method": "GET",
    "purpose": "取命令执行结果（命令中心三跳的第 3 跳，业务数据在此返回）",
    "headers": {"X-Mini-Token": "<token>", "X-Language": "zh-cn"},
    "path_params": {"cmd_id": "命令 ID", "job_id": "任务 ID", "task_id": "节点任务 ID"},
    "query_params": {},
    "body": None,
    "response": {
        "node_id": "节点 ID", "node_name": "节点名", "status": "succeed/failed",
        "result": "JSON 数组字符串（业务数据）", "out_format": "json",
        "out_mode": "array", "out_type": "map", "download_file_id": "转储文件 ID（大结果集）",
    },
    "notes": "result 是 JSON 字符串，需二次 json.loads 解析；列名取自 SQL 的 as 别名，"
             "自定义 SQL 需显式写别名",
    "covers_metrics": 0,
    "is_prerequisite": True,
    "flow_step": 3,
    "flow_of": "POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}",
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
