#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""生成 ``data/mapping/kafka_field_mapping.json`` —— 「响应值 → Kafka 字段」对应表。

**这份表回答一个问题**：客户指标的某条消息，ITSCADA 那 7 个（或告警 13 个）字段，
每一个的值到底是 TPOPS 响应里的哪个值、怎么转出来的。

每条记录的形状固定为三段：

```json
{
  "tpops_response_source": { ... },   // 响应里取什么、怎么取
  "kafka_target":        { ... },     // Kafka 消息每个字段 ← 引用 source 里的哪个成员
  "remark":              "..."        // 人话说明（尤其是维度展开口径）
}
```

``kafka_target`` 的每个值都是一条 ``tpops_response_source.<成员名>`` 的引用，
所以"哪个值 → 哪个字段"一眼可见；``tpops_response_source`` 里写明这个成员
**从响应体的哪一段、哪一列取、怎么转**。

维度展开（占位符指标）是这张表的重点：
一条 SQL 查出 6000 张表 ⇒ 6000 行 ⇒ **每行产出一条 Kafka 消息**，
规则写在 ``tpops_response_source.value.row_expansion`` 与 ``dimension_columns`` 里。

生成时会把整条链路真跑一遍：
    实测 response_body → 解包 → 取值规则 → 组装消息
所以 ``sample_message`` 不是手写的，是跑出来的。

用法::

    python tools/build_kafka_mapping.py          # 生成并写盘
    python tools/build_kafka_mapping.py --check  # 只校验是否最新
"""
from __future__ import print_function

import io
import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAPPING_DIR = os.path.join(REPO_ROOT, 'data', 'mapping')
RESPONSE_DIR = os.path.join(REPO_ROOT, 'data', 'metric_response')
ALARM_DIR = os.path.join(REPO_ROOT, 'data', 'alarm')

sys.path.insert(0, os.path.join(REPO_ROOT, 'tools'))
sys.path.insert(0, REPO_ROOT)  # 生产包 tpops_kafka_collector 在项目根

from tpops_kafka_collector.mapping.decode import decode  # noqa: E402
from tpops_kafka_collector.mapping.message import build_alarm_message, build_metric_message  # noqa: E402
from tpops_kafka_collector.mapping.nodes import resolve_node_context  # noqa: E402
from tpops_kafka_collector.mapping.rules import CounterDeltaStore, evaluate  # noqa: E402
from tpops_kafka_collector.mapping.table import AlarmDefinition, MetricRule  # noqa: E402

FIELD_MAP_PATH = os.path.join(MAPPING_DIR, 'itscada_field_map.json')
METRIC_RULES_PATH = os.path.join(MAPPING_DIR, 'metric_rules.json')
RESPONSE_RULES_PATH = os.path.join(MAPPING_DIR, 'response_rules.json')
ALARM_RULES_PATH = os.path.join(MAPPING_DIR, 'alarm_rules.json')
INSTANCES_PATH = os.path.join(REPO_ROOT, 'data', 'instances.json')
OUTPUT_PATH = os.path.join(MAPPING_DIR, 'kafka_field_mapping.json')

NOW_MS = 1760000000000

# 留档对应的目标实例（采集端由 TPOPS_INSTANCE_ID 配置）。
# 诊断优化与实例接口的响应体里没有 instance_id，要靠它兜底。
DEFAULT_INSTANCE_ID = '83cd68f2e4cb40b4b7e69240479ffa26in14'

# 展开方式：一批结果行怎么变成消息条数
EXPANSION_PER_ROW = 'per_row'        # 一行一条消息（带占位符的指标）
EXPANSION_PER_GROUP = 'per_group'    # 每个分组一条消息（按维度分组聚合）
EXPANSION_AGGREGATE = 'aggregate'    # 整批聚合成一条
EXPANSION_PER_COMPONENT = 'per_component'  # 每个组件一条（A 类时序）
EXPANSION_SINGLE = 'single'          # 一条

# 维度占位符 → 展开条数的说法（讲人话）
DIMENSION_NOUN = {
    'NAME': '张表',
    'DATNAME': '个库',
    'DBNAME': '个库',
    'DATABASE_NAME': '个库',
    'PORT': '个端口',
}

# 资源作用域：决定 resourceName 指向哪个节点
SCOPE_INSTANCE = 'instance'   # 数据与节点无关 → 资源 = 第一个 master CN
SCOPE_NODE = 'node'           # 数据按节点分布 → 资源 = 数据来源的那个节点

GRANULARITY_NOTE = {
    'instance': '实例级（无占位符）',
    'database': '库级（指标名带库占位符）',
    'table': '表级（指标名带表占位符）',
    'port': '端口级（指标名带端口占位符）',
}

EXPANSION_NOTE = {
    EXPANSION_PER_ROW: '一行一条消息',
    EXPANSION_PER_GROUP: '每个分组一条消息',
    EXPANSION_AGGREGATE: '整批聚合成一条消息',
    EXPANSION_PER_COMPONENT: '每个组件一条消息',
    EXPANSION_SINGLE: '一条消息',
}


class BuildError(Exception):
    """生成期校验不通过。"""


def _read_json(path):
    with io.open(path, encoding='utf-8') as fp:
        return json.load(fp)


# -- 资源上下文（拼 resourceName、写 tags 要用） ----------------------------

def _instance_of(instances, instance_id):
    for item in instances.get('instances', []):
        if item.get('id') == instance_id:
            return item
    return None


def _nodes_of(steps):
    """从 key-view-execute-node 步骤取节点清单。"""
    for step in steps:
        if 'key-view' in step['api']:
            return step['response_body'].get('nodes', [])
    return []


def _instance_nodes():
    """节点清单是实例级的，A 类样本里没有这一步 —— 从任一带清单的样本取。

    同一实例的节点清单在各留档里一致（同一实例、同一次采集）。
    """
    manifest = _read_json(os.path.join(RESPONSE_DIR, '000manifest.json'))
    for item in manifest['metrics']:
        steps = _read_json(os.path.join(RESPONSE_DIR, item['file']))
        nodes = _nodes_of(steps)
        if nodes:
            return nodes
    return []


def _component_of_series_key(key):
    """series 键可能是 component_id，也可能是 "<IP>-<component_id>"。"""
    if '-' in key and key.split('-', 1)[0].count('.') == 3:
        return key.split('-', 1)[1]
    return key


def _resource_context(steps, body, instances, channel, dims, fallback_nodes=(),
                      scope=SCOPE_NODE):
    """拼出 resourceName 与 tags 需要的资源属性。

    resourceName 只能表示一个节点 → 一律「节点 IP + 节点 ID」：

    - 数据按节点分布（SCOPE_NODE）→ 资源 = 数据来源的那个节点；
    - 数据与节点无关（SCOPE_INSTANCE）→ 资源 = 节点清单里的第一个 master CN，
      固定一个节点，避免「这轮 SQL 在哪个 master 上跑」导致资源名漂移。
    """
    nodes = _nodes_of(steps) or list(fallback_nodes)
    # 同一个 node_id 在节点清单里会出现多次（CN 一条 + 各 distributed_id 的 DN 一条），
    # 命令中心只在 master(CN) 上执行，取 CN 那条。
    by_node_id = {}
    for node in nodes:
        current = by_node_id.get(node['node_id'])
        if current is None or (current.get('type') != 'CN' and node.get('type') == 'CN'):
            by_node_id[node['node_id']] = node
    by_component = dict((n['component_id'], n) for n in nodes)

    instance_id = None
    node_id = None
    component_id = None
    host_ip = None

    if channel == 'command_center_sql':
        node_id = body.get('node_id')
        node = by_node_id.get(node_id)
        if node:
            component_id = node.get('component_id')
            host_ip = node.get('node_ip')
    elif channel == 'monitor_metrics':
        component_id = _component_of_series_key(dims.get('COMPONENT_ID') or '')
        node = by_component.get(component_id)
        if node:
            node_id = node.get('node_id')
            host_ip = node.get('node_ip')
    elif channel == 'diagnostic':
        for key in ('slow_sql_infos', 'top_sql_infos'):
            for item in body.get(key) or ():
                node_id = item.get('node_id')
                host_ip = item.get('host_ip')
                break
            if node_id:
                break

    instance_id = instance_id or DEFAULT_INSTANCE_ID
    instance = _instance_of(instances, instance_id) or {}
    if scope == SCOPE_INSTANCE:
        # 与节点无关：节点不是数据来源，统一落到第一个 master CN
        component_id = None
        node_id = None
        host_ip = None
    context = {
        'instance_id': instance_id,
        'instance_name': instance.get('name'),
        'node_id': node_id,
        'component_id': component_id,
        'host_ip': host_ip,
    }
    return resolve_node_context(context, nodes)


# -- 源：真实响应片段 --------------------------------------------------------

# 裁剪长度：source 里放真实数据，但长数组只留开头几项（remark 里写明真实长度）
KEEP_ROWS = 3
KEEP_POINTS = 2
KEEP_ITEMS = 2


def _trim_series(series):
    """时序数组只留前 KEEP_POINTS 个点，字段名与真实值不变。"""
    trimmed = {}
    for key, values in series.items():
        trimmed[key] = list(values[:KEEP_POINTS])
    return trimmed


def _master_view_of(sql_key, samples_by_metric, rules_by_code):
    """实测各 master 返回的结果是否相同。

    相同 → 数据来自 DN 侧视图，各 master 一样，取任一个即可；
    不同 → 数据来自 CN 自身会话视图，是节点视角，要跨 master 归集才有实例级语义。
    """
    for code, rule in rules_by_code.items():
        if rule.get('source', {}).get('sql_key') != sql_key:
            continue
        sample = samples_by_metric.get(code)
        if not sample:
            continue
        results = []
        for step in sample['steps']:
            if 'task-record' in step['api'] and step['api'].rstrip('/').endswith('detail'):
                try:
                    results.append(json.dumps(json.loads(step['response_body']['result'] or '[]'),
                                              sort_keys=True, ensure_ascii=False))
                except ValueError:
                    results.append('')
        if len(results) < 2:
            continue
        return 'identical' if len(set(results)) == 1 else 'divergent'
    return None


def _sample_body_for_sql_key(sql_key, samples_by_metric, rules_by_code):
    """找一条用同一条 SQL 的样本，取它的真实响应片段。"""
    for code, rule in rules_by_code.items():
        if rule.get('source', {}).get('sql_key') != sql_key:
            continue
        sample = samples_by_metric.get(code)
        if not sample:
            continue
        for step in sample['steps']:
            if 'task-record' in step['api'] and step['api'].rstrip('/').endswith('detail'):
                return step['response_body']
    return None


def _real_source(channel, body, locate):
    """从实测响应体里取出该指标对应的**原样片段**（真实字段名 + 真实数据）。"""
    if channel == 'command_center_sql':
        source = {}
        for key, value in body.items():
            if key == 'result':
                # 原始是 JSON 字符串，这里放解析后的真实行（字段名与值不变）
                try:
                    rows = json.loads(value) if value else []
                except ValueError:
                    rows = []
                source['result'] = rows[:KEEP_ROWS]
            else:
                source[key] = value
        return source
    if channel == 'monitor_metrics':
        metric_id = locate.get('metric')
        for info in body.get('metric_infos') or ():
            for metric in info.get('metrics') or ():
                if metric.get('name') != metric_id:
                    continue
                return {
                    'metric_infos': [{
                        'instance_id': info.get('instance_id'),
                        'time_interval': info.get('time_interval'),
                        'metrics': [{
                            'name': metric.get('name'),
                            'metric_name': metric.get('metric_name'),
                            'collection_frequency': metric.get('collection_frequency'),
                            'node_id': metric.get('node_id'),
                            'series': _trim_series(metric.get('series') or {}),
                            'timestamps': list((metric.get('timestamps') or [])[:KEEP_POINTS]),
                        }],
                    }],
                }
        return body
    if channel == 'diagnostic':
        source = {}
        for key, value in body.items():
            if isinstance(value, list):
                source[key] = value[:KEEP_ITEMS]
            else:
                source[key] = value
        return source
    if channel == 'instance':
        source = {}
        for key, value in body.items():
            if isinstance(value, list):
                source[key] = value[:1]
            else:
                source[key] = value
        return source
    return body


# -- 源 → 目标 ------------------------------------------------------------

def _expansion_of(rule_type, params):
    """一批结果行怎么变成消息条数。"""
    if rule_type == 'aggregate':
        return EXPANSION_PER_GROUP if params.get('group_by') else EXPANSION_AGGREGATE
    if rule_type in ('raw', 'counter_delta'):
        policy = params.get('row_policy')
        if policy == 'per_row':
            return EXPANSION_PER_ROW
        if policy == 'sum':
            return EXPANSION_AGGREGATE
        return EXPANSION_SINGLE
    if rule_type == 'ratio_across':
        return EXPANSION_SINGLE
    return None


def _value_member(channel, locate, params, value_rule, row_policy):
    """``tpops_response_source.value``：值从响应体的哪一段、哪一列取、怎么转。"""
    rule_type = value_rule.get('type')
    if channel == 'command_center_sql':
        member = {
            'from': 'result',
            'path': 'detail.result',
            'json': True,
            'rows': 'rows[]',
            'row_filter': params.get('filter', {}) or {},
            'value_transform': rule_type,
        }
        if rule_type == 'ratio_across':
            # 跨两条 SQL 相除，两个来源都要写清楚
            member['sources'] = [
                {'sql_key': params[side]['sql_key'],
                 'value_column': params[side]['value_column'],
                 'fn': params[side].get('fn', 'sum')}
                for side in ('numerator', 'denominator')
            ]
            member['row_expansion'] = EXPANSION_SINGLE
        else:
            column = params.get('column') or params.get('value_column')
            if column:
                member['value_column'] = column
            member['row_expansion'] = (
                _expansion_of(rule_type, params) or EXPANSION_AGGREGATE
            )
            if member['row_expansion'] == EXPANSION_AGGREGATE:
                member['aggregate_fn'] = 'sum'
            if member['row_expansion'] == EXPANSION_PER_GROUP:
                member['group_by'] = params.get('group_by')
        return member

    if channel == 'monitor_metrics':
        return {
            'from': 'series',
            'path': 'metric_infos[].metrics[].series',
            'match_metric': locate.get('metric'),
            'position': params.get('position', 'last_non_null'),
            'value_transform': rule_type,
            'row_expansion': EXPANSION_PER_COMPONENT,
        }

    if channel == 'diagnostic':
        collection = params.get('collection')
        member = {
            'from': 'collection',
            'path': collection,
            'value_transform': rule_type,
            'row_expansion': EXPANSION_SINGLE,
        }
        if params.get('field'):
            member['field'] = params['field']
        if params.get('prefixes'):
            member['prefixes'] = params['prefixes']
        if params.get('sort_by'):
            member['sort_by'] = params['sort_by']
            member['limit'] = params.get('limit')
        return member

    if channel == 'instance':
        return {
            'from': 'instance',
            'path': 'instances[0]',
            'field': params.get('field'),
            'value_transform': rule_type,
            'row_expansion': EXPANSION_SINGLE,
        }
    raise BuildError('未知通道：%s' % channel)


def _clock_member(channel, locate):
    if channel == 'monitor_metrics':
        return {
            'from': 'series_timestamp',
            'path': 'metric_infos[].metrics[].timestamps[i]',
            'fallback': 'collect_time',
            'note': 'i 与 value 取到的点是同一下标；timestamps 为 0 或缺失时回退到采集时刻',
        }
    return {
        'from': 'collect_time',
        'note': '本轮采集完成时刻；命令中心返回的是当前快照，没有自带时间戳',
    }


def _value_expression(channel, locate, params, value_rule, expansion,
                      prefix='tpops_response_source'):
    """value 字段的取值表达式 —— 指向真实响应里的位置。"""
    rule_type = value_rule.get('type')
    if channel == 'command_center_sql':
        column = params.get('column') or params.get('value_column')
        if rule_type == 'ratio_across':
            num = params['numerator']
            den = params['denominator']
            return ('%s ÷ %s' % (
                'sum(%s.%s.result[*].%s)' % (prefix, num['sql_key'], num['value_column']),
                'sum(%s.%s.result[*].%s)' % (prefix, den['sql_key'], den['value_column']),
            ))
        if rule_type == 'raw_rows':
            return 'tpops_response_source.result[*]'
        if rule_type == 'top_n':
            return 'top(tpops_response_source.result[*], %s, %s)' % (
                params.get('sort_by'), params.get('limit', 10))
        if expansion == EXPANSION_PER_ROW:
            return 'tpops_response_source.result[*].%s' % column
        if expansion == EXPANSION_PER_GROUP:
            group = (params.get('group_by') or [''])[0]
            return 'sum(tpops_response_source.result[?%s=*].%s)' % (group, column)
        if expansion == EXPANSION_AGGREGATE:
            return 'sum(tpops_response_source.result[*].%s)' % column
        return 'tpops_response_source.result[0].%s' % column
    if channel == 'monitor_metrics':
        return ('tpops_response_source.metric_infos[].metrics[name=%s].series[*][-1]'
                % locate.get('metric'))
    if channel == 'diagnostic':
        collection = params.get('collection')
        if rule_type == 'collection_count':
            return 'count(tpops_response_source.%s)' % collection
        if rule_type == 'text_prefix_count':
            return 'count(tpops_response_source.%s[?sql_text starts with %s])' % (
                collection, '|'.join(params.get('prefixes') or ()))
        if rule_type == 'top_n':
            return 'top(tpops_response_source.%s, %s, %s)' % (
                collection, params.get('sort_by'), params.get('limit', 10))
        return 'count(tpops_response_source.%s)' % collection
    if channel == 'instance':
        return 'tpops_response_source.instances[0].%s' % params.get('field')
    raise BuildError('未知通道：%s' % channel)


def _source_and_target(field_map, channel, rule, locate, value_rule, params, source_data,
                       scope=SCOPE_NODE):
    """拼出 tpops_response_source（真实响应片段）与 kafka_target（取值表达式）。"""
    resource_name = field_map['resource_name']
    expansion = _expansion_of(value_rule.get('type'), params)
    if channel == 'monitor_metrics':
        expansion = EXPANSION_PER_COMPONENT
    elif channel in ('diagnostic', 'instance'):
        expansion = EXPANSION_SINGLE
    elif expansion is None:
        expansion = EXPANSION_SINGLE

    target = {
        'metricName': rule.metric_name,
        'metricCode': rule.metric_code,
        'resourceName': field_map['resource_name']['template'],
        'value': _value_expression(channel, locate, params, value_rule, expansion),
        'unit': rule.unit,
        'clock': 'collect_time',
        'tags': 'instance + node + dimensions',
    }
    if channel == 'monitor_metrics':
        target['clock'] = (
            'tpops_response_source.metric_infos[].metrics[name=%s].timestamps[-1]'
            % locate.get('metric')
        )
    return source_data, target, expansion


def _remark(rule, expansion, message_count, observed, channel, params, source_data,
            scope=SCOPE_NODE, master_view=None):
    """人话说明：展开口径是这张表最需要讲清楚的部分。"""
    parts = []
    dimensions = rule.dimension_bindings or {}
    if expansion == EXPANSION_PER_ROW:
        placeholders = '、'.join('{%s}' % p for p in sorted(dimensions)) or '维度'
        rows = observed.get('row_count')
        noun = DIMENSION_NOUN.get(sorted(dimensions)[0] if dimensions else '', '行')
        parts.append(
            '按 %s 展开：一行一条消息，result 的每一行产出一条'
            '（实测 %s 行 → %s 条；6000 %s就是 6000 条）'
            % (placeholders, rows, message_count, noun)
        )
    elif expansion == EXPANSION_PER_GROUP:
        parts.append('按 %s 分组，每组一条消息' % '、'.join(params.get('group_by') or []))
    elif expansion == EXPANSION_AGGREGATE:
        parts.append('整批聚合成一条消息（row_policy=sum）')
    elif expansion == EXPANSION_PER_COMPONENT:
        parts.append('每个组件一条消息；归集口径待客户确认'
                     '（open_items.json / a-class-multi-series）')
    else:
        parts.append('一条消息')

    rule_type = (rule.value_rule or {}).get('type')
    if rule_type == 'counter_delta':
        parts.append('累计值跨轮差分为速率；首轮无基线返回 null，累计值回退判定重启并丢弃')
    if channel == 'monitor_metrics':
        parts.append('平台中文名「%s」与客户指标表中文名不同，按客户指标表发'
                     % (source_data.get('metric_infos', [{}])[0]
                        .get('metrics', [{}])[0].get('metric_name') or ''))
    granularity = getattr(rule, 'resource_granularity', 'instance')
    parts.append('资源粒度：%s' % GRANULARITY_NOTE.get(granularity, granularity))
    if scope == SCOPE_INSTANCE:
        parts.append('resourceName 指向节点清单里的第一个 master CN（数据与节点无关，固定一个节点）')
    else:
        parts.append('resourceName 指向数据来源的那个节点（数据按节点分布）')
    if master_view == 'identical':
        parts.append('各 master 返回的结果完全相同（DN 侧视图），取任一个 master 即可')
    elif master_view == 'divergent':
        parts.append('各 master 返回的结果不同（CN 侧会话视图，节点视角），'
                     '实例级语义需要跨 master 归集（口径待确认）')
    if rule.note:
        parts.append(rule.note)
    return '；'.join(parts)


# -- 指标映射 -------------------------------------------------------------

def _build_metric_mappings(field_map, metric_rules, response_rules, instances, nodes,
                            samples_by_metric):
    mappings = []
    shapes = _read_json(RESPONSE_RULES_PATH)['shapes']
    rules_by_code = dict((r['metric_code'], r) for r in metric_rules)
    for rule_payload in metric_rules:
        if not rule_payload.get('enabled'):
            continue
        code = rule_payload['metric_code']
        rule = MetricRule.from_dict(rule_payload)
        entry = response_rules.get(code)
        if entry is None:
            raise BuildError('%s 在 response_rules.json 里没有条目' % code)

        sample_file = entry['evidence']['sample_file']
        steps = _read_json(os.path.join(RESPONSE_DIR, sample_file))
        step_index = entry['evidence']['terminal_step']
        body = steps[step_index]['response_body']

        shape = shapes[entry['shape']]
        deltas = CounterDeltaStore()
        ctx = decode(body, shape, entry['locate'], now_ms=NOW_MS)
        results = evaluate(rule, ctx, deltas)
        if (rule.value_rule or {}).get('type') == 'counter_delta':
            # 首轮只建基线不产出值（§3.4 铁律），样例要跑第二轮才有速率
            ctx = decode(body, shape, entry['locate'], now_ms=NOW_MS + 300000)
            results = evaluate(rule, ctx, deltas)

        params = (rule.value_rule or {}).get('params', {}) or {}
        source_data = _real_source(entry['channel'], body, entry['locate'])
        master_view = rule.master_view

        # 资源作用域看「数据是否按节点分布」——实测结论落在 rule.node_scoped 上：
        # 命令中心比各 master 的返回是否相同，监控指标看 series 是否按组件展开，
        # 诊断与实例接口一次返回整实例数据。
        scope = SCOPE_NODE if rule.node_scoped else SCOPE_INSTANCE
        resource = _resource_context(steps, body, instances, entry['channel'], {}, nodes,
                                     scope=scope)
        messages = []
        for dims, value, timestamp in results:
            if entry['channel'] == 'monitor_metrics' and dims.get('COMPONENT_ID'):
                resource = _resource_context(
                    steps, body, instances, entry['channel'], dims, nodes, scope=scope)
            messages.append(build_metric_message(
                field_map, rule, dims, value, resource, timestamp or NOW_MS,
                nodes=nodes,
            ))

        if (rule.value_rule or {}).get('type') == 'ratio_across':
            # 比值的分子分母来自两条不同的 SQL，source 里放两条的真实响应
            merged = {}
            for side in ('numerator', 'denominator'):
                sql_key = params[side]['sql_key']
                if sql_key in merged:
                    continue
                if sql_key == rule.source.get('sql_key'):
                    merged[sql_key] = _real_source(entry['channel'], body, entry['locate'])
                    continue
                other = _sample_body_for_sql_key(
                    sql_key, samples_by_metric, rules_by_code)
                merged[sql_key] = _real_source(entry['channel'], other, entry['locate']) \
                    if other else {}
            source_data = merged
        source, target, expansion = _source_and_target(
            field_map, entry['channel'], rule, entry['locate'], rule.value_rule,
            params, source_data, scope=scope)
        observed = entry['evidence'].get('observed', {})

        record = {
            'metric_code': code,
            'metric_name': rule.metric_name,
            'message_type': 'metric',
            'tpops_response_source': source,
            'kafka_target': target,
            'resource_granularity': getattr(rule, 'resource_granularity', 'instance'),
            'master_view': master_view,
            'node_scoped': bool(rule.node_scoped),
            'remark': _remark(rule, expansion, len(messages), observed,
                              entry['channel'], params, source, scope, master_view),
            'message_count': len(messages),
            'sample_message': messages[0] if messages else None,
            'evidence': entry['evidence'],
            'needs_confirmation': bool(rule_payload.get('needs_confirmation')),
        }
        if rule.dimension_bindings:
            # 占位符 ← 结果行里的哪一列（维度展开的关键）。
            # 绑定值可以是**点号连接的多个列名**（``schemaname.relname``）——
            # 那是两列用 ``.`` 连起来（表级指标的 schema_name.table_name）。
            record['placeholder_bindings'] = dict(
                (placeholder, _placeholder_expression(column))
                for placeholder, column in rule.dimension_bindings.items()
            )
        mappings.append(record)
    return mappings


def _placeholder_expression(spec):
    """维度绑定 → 结果行里的取值表达式。

    单列 → ``tpops_response_source.result[*].<列>``；
    多列（点号串）→ 两列用 `` · ``（字符串连接）连起来。
    """
    parts = ['tpops_response_source.result[*].%s' % column
             for column in spec.split('.')]
    return ' · '.join(parts)


# -- 告警映射 -------------------------------------------------------------

ALARM_SHAPES = [
    ('active_convergence', 'active.json', '活动告警 · 收敛列表',
     'is_active=true & is_convergence=true', False),
    ('active_detail', 'active_detail.json', '活动告警 · 明细行',
     'is_active=true & is_convergence=false', True),
    ('history_convergence', 'history.json', '历史告警 · 收敛列表',
     'is_active=false & is_convergence=true', False),
    ('history_detail', 'history_detail.json', '历史告警 · 明细行',
     'is_active=false & is_convergence=false', True),
]


def _alarm_source_and_target(field_map, records, has_details, is_history,
                             convergence_records=None):
    """告警：source 是真实响应里的 records，target 指向其中的字段。"""
    source = {
        'records': records[:KEEP_ITEMS],
    }
    if has_details:
        # 明细行没有 alarm_items_sum，value 要按 alarm_id 从收敛条目取
        source['convergence_records'] = (convergence_records or [])[:KEEP_ITEMS]
    target = {
        'alarmId': 'tpops_response_source.records[0].alarm_id',
        'alarmName': 'tpops_response_source.records[0].alarm_name',
        # 明细行才有 description；收敛条目没有，退到 cause / alarm_name
        'alarmContent': ('tpops_response_source.records[0].description' if has_details
                         else 'tpops_response_source.records[0].cause'),
        'priority': 'severity_to_priority[tpops_response_source.records[0].severity]',
        'alarmTime': 'tpops_response_source.records[0].first_alarm_time',
        'status': 'status_to_itscada[tpops_response_source.records[0].status]',
        # 活动告警还没恢复，没有恢复时间
        'recoverTime': ('tpops_response_source.records[0].clear_alarm_time'
                        if is_history else None),
        'resourceName': field_map['resource_name']['template'],
        'metricName': '告警定义里的指标名（alarm_rules.json）',
        'metricCode': '告警定义里的指标编码（alarm_rules.json）',
        'value': ('tpops_response_source.convergence_records[0].alarm_items_sum'
                  if has_details else 'tpops_response_source.records[0].alarm_items_sum'),
        'unit': '告警定义里的单位（alarm_rules.json）',
        'tags': 'tpops_response_source.records[0]',
    }
    return source, target


def _node_by_alarm_id():
    """alarm_id → 节点（node_id, host_ip）。

    收敛条目不带节点身份，明细行带。只在明细行指向**同一个**节点时才回填，
    涉及多个节点（一条告警落在多个节点上）就不指定 —— 由 resourceName
    回落到第一个 master CN。
    """
    mapping = {}
    for filename in ('active_detail.json', 'history_detail.json'):
        payload = _read_json(os.path.join(ALARM_DIR, filename))
        for item in payload.get('records', []):
            key = item.get('alarm_id')
            node = (item.get('node_id'), item.get('host_ip'))
            if key not in mapping:
                mapping[key] = node
            elif mapping[key] != node:
                mapping[key] = None
    return mapping


def _build_alarm_mappings(field_map, alarm_rules, nodes=()):
    definitions = alarm_rules.get('definitions', {})
    severity_to_priority = alarm_rules.get('severity_to_priority', {})
    status_to_itscada = alarm_rules.get('status_to_itscada', {})
    nodes_by_alarm = _node_by_alarm_id()
    mappings = []
    for shape_id, filename, label, request, has_details in ALARM_SHAPES:
        payload = _read_json(os.path.join(ALARM_DIR, filename))
        records = payload.get('records', [])
        if not records:
            continue
        record = records[0]
        if has_details:
            # 明细行没有 alarm_items_sum，值要按 alarm_id 从收敛条目关联取
            convergence_file = 'history.json' if shape_id.startswith('history') else 'active.json'
            convergence = _read_json(os.path.join(ALARM_DIR, convergence_file))
            for item in convergence.get('records', []):
                if item.get('alarm_id') == record.get('alarm_id'):
                    record = dict(item, **record)
                    break
        raw_definition = definitions.get(str(record.get('alarm_id')))
        if not record.get('node_id'):
            # 收敛条目没有节点身份 → 用明细行的节点（同一节点时才回填）
            node = nodes_by_alarm.get(record.get('alarm_id'))
            if node:
                record = dict(record, node_id=node[0], host_ip=node[1])
        definition = None
        if raw_definition:
            definition = AlarmDefinition(
                alarm_id=str(record.get('alarm_id')),
                alarm_name=raw_definition.get('alarm_name', ''),
                alarm_kind=raw_definition.get('alarm_kind', ''),
                default_level=raw_definition.get('default_level', ''),
                default_priority=raw_definition.get('default_priority'),
                threshold=raw_definition.get('threshold', ''),
                metric_code=raw_definition.get('metric_code', ''),
                metric_name=raw_definition.get('metric_name', ''),
                value_source=raw_definition.get('value_source', 'alarm_items_sum'),
                linked_customer_metric=raw_definition.get('linked_customer_metric'),
            )
        details = [record] if has_details else []
        message = build_alarm_message(
            field_map,
            record,
            definition,
            {
                'instance_id': record.get('instance_id'),
                'instance_name': record.get('instance_name'),
                'node_id': record.get('node_id'),
                'host_ip': record.get('host_ip'),
            },
            severity_to_priority,
            status_to_itscada,
            metric_value=None,
            unit=None,
            details=details,
            nodes=nodes,
        )
        convergence_records = []
        if has_details:
            convergence_file = 'history.json' if shape_id.startswith('history') else 'active.json'
            convergence_records = _read_json(
                os.path.join(ALARM_DIR, convergence_file)).get('records', [])
        source, target = _alarm_source_and_target(
            field_map, payload.get('records', []), has_details,
            shape_id.startswith('history'), convergence_records)
        target['metricName'] = definition.metric_name if definition else ''
        target['metricCode'] = definition.metric_code if definition else ''
        mappings.append({
            'shape': shape_id,
            'label': label,
            'message_type': 'alarm',
            'tpops_response_source': source,
            'kafka_target': target,
            'remark': (
                '收敛条目是主条目（一条 = 实例 + alarm_id）；明细行挂进 details 数组。'
                '明细行本身没有 alarm_items_sum，value 要按 alarm_id 从收敛条目取。'
                '每条告警定义（257 条）的指标挂载见 data/mapping/alarm_rules.json'
            ),
            'sample_message': message,
        })
    return mappings


# -- 组装 -----------------------------------------------------------------

def build():
    field_map = _read_json(FIELD_MAP_PATH)
    metric_rules = _read_json(METRIC_RULES_PATH)['rules']
    response_payload = _read_json(RESPONSE_RULES_PATH)
    response_rules = dict((r['metric_code'], r) for r in response_payload['rules'])
    alarm_rules = _read_json(ALARM_RULES_PATH)
    instances = _read_json(INSTANCES_PATH)

    manifest = _read_json(os.path.join(RESPONSE_DIR, '000manifest.json'))
    samples_by_metric = {}
    for item in manifest['metrics']:
        samples_by_metric[item['customer_metric']] = {
            'file': item['file'],
            'steps': _read_json(os.path.join(RESPONSE_DIR, item['file'])),
        }
    instance_nodes = _instance_nodes()
    metric_mappings = _build_metric_mappings(
        field_map, metric_rules, response_rules, instances, instance_nodes,
        samples_by_metric)
    alarm_mappings = _build_alarm_mappings(field_map, alarm_rules, instance_nodes)

    return {
        'version': '2.0',
        'generated_by': 'tools/build_kafka_mapping.py',
        'generated_from': {
            'field_map': 'data/mapping/itscada_field_map.json',
            'metric_rules': 'data/mapping/metric_rules.json',
            'response_rules': 'data/mapping/response_rules.json',
            'alarm_rules': 'data/mapping/alarm_rules.json',
            'response_samples': 'data/metric_response/',
            'alarm_samples': 'data/alarm/',
        },
        'note': (
            '「响应里的某个值 → Kafka 消息的某个字段」的对应表。'
            'tpops_response_source 写明从响应体的哪一段、哪一列取、怎么转；'
            'kafka_target 的每个值都是一条 tpops_response_source.<成员名> 引用；'
            'remark 讲清展开口径（有占位符的指标一行一条消息）。'
            'sample_message 是用实测留档真实跑出来的一条消息（不是手写的）。'
            '本表取代已归档的分类映射表（data/mapping/archive/）。'
        ),
        'layers': {
            'metric_rules.json': '业务规则：取哪个列、怎么算（值形态、差分、聚合）',
            'response_rules.json': '取数定位：响应体的哪一段、哪一列',
            'kafka_field_mapping.json': '字段映射：这个值 → Kafka 的哪个字段（本表）',
        },
        'message_types': {
            'metric': {
                'topic_env': 'KAFKA_TOPIC_METRICS',
                'fields': field_map['metric_message']['fields'],
                'required_fields': field_map['metric_message']['required_fields'],
            },
            'alarm': {
                'topic_env': 'KAFKA_TOPIC_ALARMS',
                'fields': field_map['alarm_message']['fields'],
                'required_fields': field_map['alarm_message']['required_fields'],
            },
        },
        'metric_mappings': metric_mappings,
        'alarm_mappings': alarm_mappings,
    }


def main(argv):
    check_only = '--check' in argv
    payload = build()
    text = json.dumps(payload, ensure_ascii=False, indent=2) + '\n'
    if check_only:
        if not os.path.exists(OUTPUT_PATH):
            print('kafka_field_mapping.json 不存在')
            return 1
        with io.open(OUTPUT_PATH, encoding='utf-8') as fp:
            if fp.read() != text:
                print('kafka_field_mapping.json 与生成器输出不一致，'
                      '请重新运行 tools/build_kafka_mapping.py')
                return 1
        print('kafka_field_mapping.json 是最新的')
        return 0
    with io.open(OUTPUT_PATH, 'w', encoding='utf-8') as fp:
        fp.write(text)
    print('写出 %s：指标 %d 条 / 告警形态 %d 种' % (
        os.path.relpath(OUTPUT_PATH, REPO_ROOT),
        len(payload['metric_mappings']),
        len(payload['alarm_mappings']),
    ))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv[1:]))
    except BuildError as exc:
        print('生成失败：%s' % exc)
        sys.exit(2)
