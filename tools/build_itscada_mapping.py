#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成 ITSCADA 映射规则文件。

输入：
  - 高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx / 指标库
      —— 客户指标规范（中文名、英文名、数据类型、周期、单位）
  - data/customer_metric_mapping.json
      —— 101 个客户指标的可提供性判定与 TPOPS 来源
  - data/metric_value_rules.json
      —— 开发方案 §4.3 的逐指标取值规则（由 tools/build_metric_value_rules.py 生成）
  - data/merged_sql/merged_sql.json
      —— 11 条合并 SQL 的真实列集合与刷新档
  - data/alarm/catalog_metric.json、catalog_event.json
      —— 平台内置告警定义目录

输出：
  - data/mapping/metric_rules.json
  - data/mapping/alarm_rules.json

用法::

    python tools/build_itscada_mapping.py
"""
from __future__ import print_function

import distribution

import io
import json
import os
import re
import sys

import excluded_metrics
import openpyxl
import xlsx_columns

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# 幂等键的字段清单只有一份：生产包 ``tpops_kafka_collector/ids.py`` 是实现，
# 本生成器把它写进 ``alarm_rules.json`` 的 ``idempotency`` 段做**声明** ——
# 声明与实现同源，改一处不会漏另一处（键的**格式**不在这里写死）。
from tpops_kafka_collector.ids import ALARM_IDEMPOTENCY_FIELDS

XLSX_PATH = os.path.join(BASE_DIR, '高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx')
MAPPING_PATH = os.path.join(BASE_DIR, 'data', 'customer_metric_mapping.json')
QUERY_PLAN_PATH = os.path.join(BASE_DIR, 'data', 'customer_metric_query_plan.json')
VALUE_RULES_PATH = os.path.join(BASE_DIR, 'data', 'metric_value_rules.json')
MERGED_SQL_PATH = os.path.join(BASE_DIR, 'data', 'merged_sql', 'merged_sql.json')
ALARM_METRIC_CATALOG = os.path.join(BASE_DIR, 'data', 'alarm', 'catalog_metric.json')
ALARM_EVENT_CATALOG = os.path.join(BASE_DIR, 'data', 'alarm', 'catalog_event.json')
OUT_DIR = os.path.join(BASE_DIR, 'data', 'mapping')
METRIC_RULES_OUT = os.path.join(OUT_DIR, 'metric_rules.json')
ALARM_RULES_OUT = os.path.join(OUT_DIR, 'alarm_rules.json')

PLACEHOLDER_RE = re.compile(r'\{#(\w+)\}')
NO_DIM = '—'


def _ensure_input(path, builder_func):
    """输入文件是生成物，缺失时先跑它的生成器。

    `data/customer_metric_query_plan.json` 与 `data/metric_value_rules.json` 都是
    中间产物，不入库；缺失时自动重建，保证映射表可重复生成。
    """
    if os.path.isfile(path):
        return path
    print(f'  [重建] {os.path.basename(path)} 缺失，正在生成 ...')
    builder_func()
    if not os.path.isfile(path):
        raise RuntimeError(f'生成失败：{path}')
    return path

# 占位符 → SQL 结果列。占位符名字取自客户指标名里的 {#XXX}。
# 值可以是**点号连接的多个列名** —— 表级指标的 {#NAME} 绑成
# `schema_name.table_name`（只绑 relname 会让不同 schema 的同名表撞键）。
DIMENSION_COLUMN_BY_SQL = {
    'stat_database': {'DATNAME': 'datname', 'DATABASE_NAME': 'datname'},
    'stat_user_tables': {'NAME': 'schemaname.relname'},
    'activity_agg': {'DATNAME': 'datname', 'NAME': 'relname'},
    'locks': {'DBNAME': 'datname'},
    'database': {'DATABASE_NAME': 'datname', 'DATNAME': 'datname'},
    'settings': {'PORT': 'setting'},
}

# 合并 SQL 里已经算好的比率列（§4.4.1 的 as 别名）。
RATIO_ALIAS_BY_SQL = {
    'stat_database': 'cache_hit_ratio',
    'stat_user_tables': 'dead_tup_ratio',
}

# 平台枚举 → 客户枚举。平台侧语义未在接口文档中明示，先按实测值给建议映射，
# confirmed=false 表示需客户确认；未命中的值原样透传并告警。

# 需要逐个判定的指标：开发方案的规则标签不足以直接执行时，在这里显式落规则。
SPECIAL_VALUE_RULES = {
    'GAUSSDB.[{#DATNAME}]_Active_connections': {
        'type': 'aggregate',
        'params': {
            'filter': {'state': 'active'},
            'group_by': ['datname'],
            'fn': 'sum',
            'value_column': 'cnt',
        },
        'note': '开发方案标注为 raw_rows，实际语义是「按库统计活跃会话」，按聚合实现',
    },
    'GAUSSDB.session_usage': {
        'type': 'ratio_across',
        'params': {
            'numerator': {'sql_key': 'activity_agg', 'fn': 'sum', 'value_column': 'cnt'},
            'denominator': {'sql_key': 'stat_database', 'fn': 'sum', 'value_column': 'numbackends'},
        },
        'note': '在线会话数 / 总连接数：分子=activity_agg 全部会话数，分母=stat_database 各库 numbackends 之和',
    },
    'gaussdb.Total_pct': {
        'type': 'ratio_across',
        'params': {
            'numerator': {'sql_key': 'activity_agg', 'fn': 'sum', 'value_column': 'cnt'},
            'denominator': {'sql_key': 'stat_database', 'fn': 'sum', 'value_column': 'numbackends'},
        },
        'note': '连接使用率：分子=activity_agg 全部会话数，分母=stat_database 各库 numbackends 之和',
    },
    'gaussdb.locks.get_metrics[{#DBNAME}]': {
        'type': 'aggregate',
        'params': {'group_by': ['datname'], 'fn': 'sum', 'value_column': 'cnt'},
        'note': '按库聚合锁数量；客户表单位标 MB，实际是锁个数，口径待客户确认',
    },
    'GAUSSDB.maximum_connection': {
        'type': 'raw_filtered',
        'params': {'filter': {'name': 'max_connections'}, 'column': 'setting', 'cast': 'int'},
        'note': '',
    },
    'gaussdb.Pagesize': {
        'type': 'raw_filtered',
        'params': {'filter': {'name': 'block_size'}, 'column': 'setting', 'cast': 'int'},
        'note': '实测 8192',
    },
    'gaussdb.Timezone': {
        'type': 'raw_filtered',
        'params': {'filter': {'name': 'TimeZone'}, 'column': 'setting'},
        'note': '客户表单位标「个」，与时区语义不符，待客户确认',
    },
    'gaussdb_{#PORT}port.status': {
        'type': 'raw_filtered',
        'params': {'filter': {'name': 'port'}, 'column': 'setting', 'cast': 'int'},
        'note': '只能取到端口号；健康检查接口 GET /rds/health-status 响应体为空，不能做可用性探测',
    },
    'gaussdb.DBtsize[{#DATABASE_NAME}]': {
        'type': 'raw',
        'params': {'column': 'size_bytes', 'row_policy': 'per_row', 'group_by': ['datname']},
        'note': '',
    },
    'gaussdb.Prepared': {
        'type': 'raw',
        'params': {'column': 'prepared_xacts', 'row_policy': 'first', 'group_by': []},
        'note': 'count(*) 统计预备事务',
    },
    'GAUSSDB.sessions_active_top10': {
        'type': 'top_n',
        'params': {
            'sort_by': 'query_start',
            'order': 'asc',
            'limit': 10,
            'columns': ['datname', 'usename', 'query_start'],
        },
        'note': '会话时长最长 = 最早开始，SQL 已按 query_start 升序',
    },
    'gaussdb.running.sql.limit.20': {
        'type': 'top_n',
        'params': {
            'sort_by': 'query_start',
            'order': 'desc',
            'limit': 20,
            'columns': ['datname', 'usename', 'state', 'query_start', 'query'],
        },
        'note': '正在运行的 SQL 文本，需按 §3.8 脱敏',
    },
    'gaussdb.get.connections.sum': {
        'type': 'raw_rows',
        'params': {},
        'note': '开发方案描述为 27 字段会话清单，实际合并 SQL 返回的是按库+状态聚合的三列，需另加 SQL 才能取到完整清单',
    },
    'gaussdb.get.dbstat.sum': {
        'type': 'raw_rows',
        'params': {},
        'note': '',
    },
    'gaussdb.get.lock.info': {
        'type': 'raw_rows',
        'params': {},
        'note': '',
    },
}

# A 类（监控指标接口）逐指标取值方式。
A_CLASS_RULES = {
    'GAUSSDB.Number_of_ansactions_mmitted_r_cond': {'type': 'series_point', 'params': {}},
    'GAUSSDB.Number_of_llback_ansactions_r_cond': {'type': 'series_point', 'params': {}},
    'gaussdb.Tps': {'type': 'series_point', 'params': {}},
    'gaussdb.QPS': {'type': 'series_point', 'params': {}},
    'gaussdb.Starttime': {'type': 'series_point', 'params': {}},
    'gaussdb.Uptimestr': {
        'type': 'gauge',
        'params': {'column': 'rds133_cn_startup_time', 'unit': '秒'},
        'note': '运行时长 = 当前时刻 - CN 进程启动时刻',
    },
}

# 平台侧是枚举值、客户侧要展示成可读文本的指标。
ENUM_METRICS = {
    'GAUSSDB.Service_availability_testing',
    'GAUSSDB.PG_Service_Status',
    'gaussdb.Status',
    'GAUSSDB.Standby_status',
    'GAUSSDB.Primary_status',
}

# C 类（诊断优化接口）逐指标取值方式。
C_CLASS_RULES = {
    'GAUSSDB.Total_slow_queries': {
        'type': 'collection_count',
        'params': {'collection': 'slow_sql_infos'},
    },
    'GAUSSDB.Select_slow_queries': {
        'type': 'text_prefix_count',
        'params': {'collection': 'slow_sql_infos', 'field': 'sql_text', 'prefixes': ['select', 'with']},
    },
    'GAUSSDB.DML_slow_queries': {
        'type': 'text_prefix_count',
        'params': {
            'collection': 'slow_sql_infos',
            'field': 'sql_text',
            'prefixes': ['insert', 'update', 'delete'],
        },
    },
    'gaussdb.Top10sql': {
        'type': 'top_n',
        'params': {'collection': 'top_sql_infos', 'sort_by': 'total_elapse_time', 'order': 'desc', 'limit': 10},
        'note': '含 SQL 文本，需按 §3.8 脱敏',
    },
}

# D 类（实例接口）。
# 用 instance_field：引擎里 raw 读的是 params.column（行列表语义），
# 而 D 类的取值在 instances[0] 这个"实例对象"上，必须走 instance_field。
D_CLASS_RULES = {
    'gaussdb.server_version': {
        'type': 'instance_field',
        'params': {'field': 'engine_version'},
        'note': 'GET /rds/v3.2/instances[].engine_version',
    },
}

API_BY_CHANNEL = {
    'A': 'POST /rds/v3/monitor/metrics',
    'B': 'POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}',
    'C': 'GET /rds/gaussdb/v3/optimization/slow-sql-list（slow-sql-list）',
    'D': 'GET /rds/v3.2/instances',
}

SOURCE_BY_CHANNEL = {
    'A': 'monitor_metrics',
    'B': 'command_center_sql',
    'C': 'diagnostic',
    'D': 'instance',
}


#: 事件类告警（未挂靠客户指标）的统一指标名 —— **默认值**。
#: 实际生效的值取自 ``alarm_rules.json`` 的 ``event_metric`` 段（表驱动）：
#: 改口径只改数据文件，生成器不覆盖它。
DEFAULT_EVENT_METRIC = {
    'metric_code': 'gaussdb.alarm',
    'metric_name': '数据库告警',
}


def _load_event_metric():
    """事件类告警的统一指标名（表驱动）。

    优先取 ``alarm_rules.json`` 里已有的 ``event_metric`` 段 —— 那是**运行期
    真相源**，运维改了它，重跑生成器不会被覆盖回去。
    文件里没有（或坏了）时用 :data:`DEFAULT_EVENT_METRIC`。
    """
    if os.path.isfile(ALARM_RULES_OUT):
        try:
            payload = _read_json(ALARM_RULES_OUT)
        except ValueError:
            payload = {}
        existing = payload.get('event_metric')
        if isinstance(existing, dict) and existing.get('metric_code') \
                and existing.get('metric_name'):
            return {
                'metric_code': existing['metric_code'],
                'metric_name': existing['metric_name'],
            }
    return dict(DEFAULT_EVENT_METRIC)


def _read_json(path):
    with io.open(path, encoding='utf-8') as handle:
        return json.load(handle)


def _write_json(path, payload):
    with io.open(path, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=False)
        handle.write('\n')


def _load_api_by_metric():
    """查询方案里逐指标的真实接口路径。"""
    import build_metric_query_plan
    _ensure_input(QUERY_PLAN_PATH, build_metric_query_plan.main)
    plan = _read_json(QUERY_PLAN_PATH)
    apis = {}
    for category in plan['categories']:
        for metric in category['metrics']:
            query = metric.get('query') or {}
            api = query.get('api')
            if api:
                apis[metric['customer_metric']] = api
    return apis


def load_spec():
    """客户指标库「指标库」sheet → 按英文名索引的规范。

    列按**表头名**定位，不按位置 —— 客户表新增/插入列时不会整体读串位。
    """
    workbook = openpyxl.load_workbook(XLSX_PATH, data_only=True)
    sheet = workbook['指标库']
    rows = list(sheet.iter_rows(values_only=True))
    col = xlsx_columns.column_map(rows[0])
    spec = {}
    for row in rows[1:]:
        code = xlsx_columns.row_value(row, col, '英文名')
        if not code:
            continue
        spec[code] = {
            'resource_type': xlsx_columns.row_value(row, col, '适用资源类型'),
            'category': xlsx_columns.row_value(row, col, '指标分类'),
            'metric_name': xlsx_columns.row_value(row, col, '中文名'),
            'metric_code': code,
            'value_type': xlsx_columns.row_value(row, col, '数据类型'),
            'collect_cycle': {
                'value': int(xlsx_columns.row_value(row, col, '数据更新周期')),
                'unit': xlsx_columns.row_value(row, col, '采集更新周期单位'),
            },
            'generate_baseline': xlsx_columns.row_value(row, col, '生成性能基线'),
            'unit': xlsx_columns.row_value(row, col, '指标单位'),
        }
    return spec


def load_method():
    """客户指标库「指标库-指标获取方法」sheet → 英文名 → 获取方法。"""
    workbook = openpyxl.load_workbook(XLSX_PATH, data_only=True)
    sheet = workbook['指标库-指标获取方法']
    rows = list(sheet.iter_rows(values_only=True))
    col = xlsx_columns.column_map(rows[0])
    method = {}
    for row in rows[1:]:
        code = xlsx_columns.row_value(row, col, '英文名')
        if not code:
            continue
        method[code] = xlsx_columns.row_value(row, col, '指标获取方法')
    return method


def load_merged_sql():
    """合并 SQL 目录 → sql_key → 列集合 / 刷新档。"""
    payload = _read_json(MERGED_SQL_PATH)
    catalog = {}
    for batch in payload['batches']:
        columns = []
        for verified in batch.get('verified', {}).values():
            columns = verified.get('columns', [])
            break
        catalog[batch['sql_key']] = {
            'sql': batch['sql'],
            'limit': batch.get('limit'),
            'source_view': batch.get('source_view'),
            'dimension_keys': batch.get('dimension_keys'),
            'refresh_seconds': batch.get('refresh_seconds'),
            'refresh_tier': batch.get('refresh_tier'),
            'columns': columns,
        }
    return catalog


def _placeholders(metric_code, metric_name):
    names = []
    for text in (metric_code or '', metric_name or ''):
        for name in PLACEHOLDER_RE.findall(text):
            if name not in names:
                names.append(name)
    return names


def _resource_granularity(placeholders):
    """资源粒度由占位符决定。

    客户指标表里的 resource_type（GAUSSDB实例）说的是"这个指标属于 GaussDB 这个产品"，
    不是资源粒度。真正的粒度看指标名里的占位符：
      {#NAME}                       → 表级
      {#DATNAME}/{#DBNAME}/…        → 库级
      {#PORT}                       → 端口级
      没有占位符                     → 实例级
    """
    names = set(placeholders or ())
    if 'NAME' in names:
        return 'table'
    if names & {'DATNAME', 'DBNAME', 'DATABASE_NAME'}:
        return 'database'
    if 'PORT' in names:
        return 'port'
    return 'instance'


def _dimension_bindings(placeholders, sql_key):
    """占位符 → SQL 结果列的绑定。"""
    table = DIMENSION_COLUMN_BY_SQL.get(sql_key, {})
    bindings = {}
    for name in placeholders:
        column = table.get(name)
        if column:
            bindings[name] = column
    return bindings


def _group_by_columns(bindings):
    """维度绑定 → 分组列（**点号串要拆成多列**）。

    绑定值可能是 ``schemaname.relname`` —— 那是**两个列**的组合，
    直接当一个列名会让分组退化成单列（不同 schema 的同名表又撞进同一个键）。
    按绑定顺序去重，保持声明的先后。
    """
    columns = []
    for spec in bindings.values():
        for column in spec.split('.'):
            if column not in columns:
                columns.append(column)
    return columns


def _resolve_column(expr, columns):
    """把「视图.列」或「列」解析成合并 SQL 里的真实列名。"""
    if not expr or expr == NO_DIM:
        return None
    candidate = expr.split('.')[-1].strip()
    if candidate in columns:
        return candidate
    return None


def _row_policy(placeholders, sql_key, value_type):
    """决定一条 SQL 结果行怎么变成一个指标值。

    返回 (策略, 分组列)：
      per_row —— 指标带维度，每行产出一条消息
      sum     —— 指标无维度且是数值，跨行求和（实例级总量口径）
      first   —— 指标无维度且非数值，取第一行（同一实例内取值一致）
    """
    bindings = _dimension_bindings(placeholders, sql_key)
    group_by = _group_by_columns(bindings)
    if group_by:
        return 'per_row', group_by
    if value_type in ('整数', '浮点'):
        return 'sum', []
    return 'first', []


def _aggregate_filter(source_expr, note):
    """从开发方案的说明文字里解析聚合过滤条件。"""
    match = re.search(r"(\w+)='([^']+)'", note or '')
    if match:
        return {match.group(1): match.group(2)}
    contains = re.search(r"(\w+)\s*含\s*([^；;]+)", note or '')
    if contains:
        text = contains.group(2)
        for suffix in ('相关会话', '类会话', '的会话'):
            text = text.replace(suffix, '')
        keywords = [k.strip() for k in re.split(r'[、,，\s]+', text) if k.strip()]
        return {'%s_contains' % contains.group(1): keywords}
    if source_expr == 'state':
        return {'state': 'active'}
    return {}


def build_value_rule(metric_code, value_rule, sql_catalog, value_type, placeholders):
    """开发方案取值规则 → 可执行规则。"""
    channel = value_rule['channel']
    if metric_code in SPECIAL_VALUE_RULES:
        return dict(SPECIAL_VALUE_RULES[metric_code])
    if channel == 'A':
        rule = dict(A_CLASS_RULES.get(metric_code, {'type': 'series_point', 'params': {}}))
        if metric_code in ENUM_METRICS:
            rule = _build_enum_rule(value_type)
        if rule['type'] == 'series_point' and not rule['params']:
            rule['params'] = {'position': 'last_non_null'}
        return rule
    if channel == 'C':
        return dict(C_CLASS_RULES.get(metric_code, {'type': 'collection_count', 'params': {}}))
    if channel == 'D':
        return dict(D_CLASS_RULES.get(metric_code, {'type': 'raw', 'params': {}}))

    plan_rule = value_rule['plan_rule']
    sql_key = value_rule['sql_key']
    columns = sql_catalog.get(sql_key, {}).get('columns', [])
    expr = value_rule['source_expr']
    note = value_rule['plan_note']

    if plan_rule == 'raw':
        column = _resolve_column(expr, columns)
        if column is None:
            return {
                'type': 'raw',
                'params': {'column': expr},
                'needs_confirmation': True,
                'note': '来源列 %s 不在 %s 的实测列集合中，需核对' % (expr, sql_key),
            }
        policy = _row_policy(placeholders, sql_key, value_type)
        return {
            'type': 'raw',
            'params': {'column': column, 'row_policy': policy[0], 'group_by': policy[1]},
        }
    if plan_rule == 'ratio':
        alias = RATIO_ALIAS_BY_SQL.get(sql_key)
        if alias:
            policy = _row_policy(placeholders, sql_key, value_type)
            return {
                'type': 'raw',
                'params': {'column': alias, 'row_policy': policy[0], 'group_by': policy[1]},
                'note': '比率已在合并 SQL 内用 CASE WHEN 算好（%s）' % alias,
            }
        return {
            'type': 'ratio',
            'params': {'expression': expr},
            'needs_confirmation': True,
            'note': '合并 SQL 未预置该比率列，需在客户端计算',
        }
    if plan_rule == 'aggregate':
        bindings = _dimension_bindings(placeholders, sql_key)
        return {
            'type': 'aggregate',
            'params': {
                'filter': _aggregate_filter(expr, note),
                'group_by': _group_by_columns(bindings),
                'fn': 'sum',
                'value_column': 'cnt',
            },
            'note': note,
        }
    if plan_rule == 'counter_delta':
        column = _resolve_column(expr, columns) or expr
        policy = _row_policy(placeholders, sql_key, value_type)
        return {
            'type': 'counter_delta',
            'params': {'column': column, 'row_policy': policy[0], 'group_by': policy[1]},
            'note': '累计值差分为速率；首轮无基线、实例重启归零时产出 null',
        }
    if plan_rule == 'approx':
        return {
            'type': 'approx',
            'params': {'source_expr': expr},
            'needs_confirmation': True,
            'note': note or '近似口径，规则待细化',
        }
    if plan_rule == 'top_n':
        return {
            'type': 'top_n',
            'params': {'sort_by': expr, 'order': 'desc', 'limit': 10},
            'note': note,
        }
    if plan_rule == 'raw_rows':
        return {'type': 'raw_rows', 'params': {}, 'note': note}
    return {
        'type': 'raw',
        'params': {'column': expr},
        'needs_confirmation': True,
        'note': '未识别的开发方案规则标签：%s' % plan_rule,
    }


def _build_enum_rule(value_type):
    """枚举型指标：客户表是字符型的做展示值转义，整数型保留平台原值。"""
    if value_type == '字符':
        return {
            'type': 'enum_map',
            'params': {
                'mapping': {'1': '正常', '0': '异常'},
                'default': 'passthrough',
                'confirmed': False,
            },
            'note': '平台实例状态实测 1=正常；展示值转义为建议值，需客户确认',
            'needs_confirmation': True,
        }
    return {
        'type': 'series_point',
        'params': {'position': 'last_non_null'},
        'note': '平台枚举值原样透传（实测 1=正常/0=异常），客户展示值转义待确认',
        'needs_confirmation': True,
    }


def build_metric_rules():
    spec = load_spec()
    method = load_method()
    availability = {item['customer_metric']: item for item in _read_json(MAPPING_PATH)['items']}
    import build_metric_value_rules
    _ensure_input(VALUE_RULES_PATH, build_metric_value_rules.main)
    value_rules = _read_json(VALUE_RULES_PATH)['metrics']
    sql_catalog = load_merged_sql()
    apis = _load_api_by_metric()

    # 实测：各 master 返回的结果是否相同 → 哪些指标的数据是按节点分布的
    samples = distribution.load_samples()
    sql_key_by_metric = dict(
        (code, (value_rules.get(code) or {}).get('sql_key'))
        for code in spec
    )
    master_views = distribution.master_view_by_sql_key(samples, sql_key_by_metric)

    rules = []
    counts = {'可提供': 0, '无法提供': 0}
    excluded = excluded_metrics.excluded_codes()
    for metric_code, item in spec.items():
        # 排除发生在构建期：被排除清单剔除的指标**不进生效口径**，
        # 运行期没有「遍历到再丢弃」的分支。
        if metric_code in excluded:
            continue
        avail = availability.get(metric_code, {})
        status = avail.get('status', '无法提供')
        counts[status] = counts.get(status, 0) + 1
        placeholders = _placeholders(metric_code, item['metric_name'])
        value_rule = value_rules.get(metric_code)
        source = {
            'api': apis.get(metric_code) or (API_BY_CHANNEL.get(value_rule['channel']) if value_rule else None),
            'source': SOURCE_BY_CHANNEL.get(value_rule['channel']) if value_rule else None,
            'tpops_source': avail.get('tpops_metric'),
            'value_source_expr': value_rule['source_expr'] if value_rule else None,
            'sql_key': value_rule['sql_key'] if value_rule else None,
            'method': method.get(metric_code),
        }
        entry = {
            'metric_code': metric_code,
            'metric_name': item['metric_name'],
            'resource_type': item['resource_type'],
            'category': item['category'],
            'value_type': item['value_type'],
            'unit': item['unit'],
            'collect_cycle': item['collect_cycle'],
            'status': status,
            'match_level': avail.get('match_level'),
            'enabled': status == '可提供',
            'source': source,
            'value_rule': None,
            'placeholders': placeholders,
            'resource_granularity': _resource_granularity(placeholders),
            'master_view': None,
            'node_scoped': False,
            'dimension_bindings': {},
            'note': avail.get('note') or '',
            'needs_confirmation': False,
        }
        # 数据分布（实测）：命令中心看各 master 的结果是否相同，
        # 监控指标看 series 是否按组件展开，诊断与实例接口一次返回整实例数据。
        channel_key = SOURCE_BY_CHANNEL.get(value_rule['channel']) if value_rule else None
        sql_key = value_rule['sql_key'] if value_rule else None
        master_view = master_views.get(sql_key) if sql_key else None
        series_scoped = distribution.series_is_component_scoped(samples.get(metric_code))
        node_scoped = distribution.node_scoped_of(channel_key, master_view, series_scoped)
        entry['master_view'] = master_view
        entry['node_scoped'] = node_scoped
        # resourceName 的模板是全局的（只表示节点：节点 IP + 节点 ID），
        # 唯一出处是 itscada_field_map.json 的 resource_name 段，不逐指标存。

        if value_rule:
            rule = build_value_rule(
                metric_code, value_rule, sql_catalog, item['value_type'], placeholders
            )

            entry['value_rule'] = rule
            entry['needs_confirmation'] = bool(rule.get('needs_confirmation'))
            entry['dimension_bindings'] = _dimension_bindings(placeholders, value_rule['sql_key'])
        else:
            entry['note'] = avail.get('note') or entry['note']
            entry['unsupported_reason'] = avail.get('note') or 'TPOPS 侧无可提供来源'
        rules.append(entry)

    payload = {
        'version': '1.0',
        'generated_by': 'tools/build_itscada_mapping.py',
        'sources': [
            '高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx / 指标库',
            'data/customer_metric_mapping.json',
            'data/metric_value_rules.json',
            'data/merged_sql/merged_sql.json',
        ],
        'field_map_ref': 'itscada_field_map.json',
        'counts': {
            'total': len(rules),
            'enabled': len([r for r in rules if r['enabled']]),
            'disabled': len([r for r in rules if not r['enabled']]),
            'needs_confirmation': len([r for r in rules if r['needs_confirmation']]),
        },
        'rules': rules,
    }
    return payload


def build_alarm_rules():
    """告警规则：字段口径 + 平台 severity/status 枚举 + 告警定义 → ITSCADA 指标挂载。"""
    availability = {item['customer_metric']: item for item in _read_json(MAPPING_PATH)['items']}
    event_metric = _load_event_metric()
    by_tpops = {}
    for metric_code, item in availability.items():
        tpops_metric = item.get('tpops_metric')
        if tpops_metric:
            by_tpops.setdefault(tpops_metric, []).append(metric_code)

    definitions = {}
    for path, kind in ((ALARM_METRIC_CATALOG, 'metric'), (ALARM_EVENT_CATALOG, 'event')):
        for item in _read_json(path)['items']:
            alarm_id = str(item.get('告警ID') or '').strip()
            if not alarm_id:
                continue
            if alarm_id in definitions:
                continue
            level = item.get('告警等级（默认）') or item.get('告警级别（默认）') or ''
            matched = [code for code in by_tpops.get(item.get('metric_id') or '', [])]
            if matched:
                metric_code = matched[0]
                metric_name = availability[metric_code]['customer_metric_name']
                value_source = 'latest_metric_value'
            else:
                # 未挂客户指标的事件类告警 → **统一指标名**（表驱动）。
                # 告警种类由 alarmId / alarmName / alarmContent 承载。
                metric_code = event_metric['metric_code']
                metric_name = event_metric['metric_name']
                value_source = 'alarm_items_sum'
            definitions[alarm_id] = {
                'alarm_name': item.get('告警名称') or '',
                'alarm_kind': kind,
                'default_level': level,
                'default_priority': _LEVEL_TO_PRIORITY.get(level),
                'threshold': item.get('告警阈值（默认）') or '',
                'clear_threshold': item.get('清除告警阈值（默认）') or '',
                'metric_code': metric_code,
                'metric_name': metric_name,
                'value_source': value_source,
                'match': item.get('match') or ('exact' if matched else 'none'),
                'linked_customer_metric': matched[0] if matched else None,
            }

    return {
        'version': '1.0',
        'generated_by': 'tools/build_itscada_mapping.py',
        'sources': [
            'docs/ITSCADA第三方告警数据接入对接方案V2.docx',
            'data/alarm/catalog_metric.json',
            'data/alarm/catalog_event.json',
        ],
        'field_map_ref': 'itscada_field_map.json#alarm_message',
        'event_metric': event_metric,
        'severity_to_priority': {
            'critical': 1,
            'major': 2,
            'minor': 3,
            'notice': 4,
        },
        'catalog_level_to_priority': {
            '紧急': 1,
            '重要': 2,
            '次要': 3,
            '提示': 4,
        },
        'status_to_itscada': {
            'activated': 1,
            'recovered': 0,
            'cleared': 0,
            'cleared_by_user': 0,
        },
        'value_sources': {
            'latest_metric_value': '取告警时刻该客户指标的最近一次采样值',
            'alarm_items_sum': '取收敛条目的 alarm_items_sum（命中节点数）',
        },
        'idempotency': {
            'alarm': {
                'fields': list(ALARM_IDEMPOTENCY_FIELDS),
                'status_domain': 'platform_raw',
                'note': (
                    '键含 status（平台原值 activated/recovered/cleared/cleared_by_user）——'
                    'ITSCADA 整数枚举会把 recovered 与 cleared 压成同一个键。'
                    '不含 node_id（收敛条目的节点靠明细行回填，逐轮可能不同）与 '
                    'last_alarm_time（恢复后它不变）。'
                    '计算见 tpops_kafka_collector/ids.py。'
                ),
            },
        },
        'counts': {
            'total_definitions': len(definitions),
            'linked_to_customer_metric': len([
                d for d in definitions.values() if d['linked_customer_metric']
            ]),
            'event_fallback': len([
                d for d in definitions.values() if not d['linked_customer_metric']
            ]),
        },
        'definitions': definitions,
    }


_LEVEL_TO_PRIORITY = {'紧急': 1, '重要': 2, '次要': 3, '提示': 4}


def main():
    if not os.path.isdir(OUT_DIR):
        os.makedirs(OUT_DIR)
    metric_payload = build_metric_rules()
    alarm_payload = build_alarm_rules()
    _write_json(METRIC_RULES_OUT, metric_payload)
    _write_json(ALARM_RULES_OUT, alarm_payload)
    print('wrote %s: %s' % (METRIC_RULES_OUT, metric_payload['counts']))
    print('wrote %s: %s' % (ALARM_RULES_OUT, alarm_payload['counts']))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
