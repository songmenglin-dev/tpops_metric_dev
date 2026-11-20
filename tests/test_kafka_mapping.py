# -*- coding: utf-8 -*-
"""kafka_field_mapping.json：「真实响应 → Kafka 字段」的对应表必须成立。

每条记录的形状是三段：
- ``tpops_response_source`` —— **实测响应体的原样片段**（真实字段名 + 真实数据）
- ``kafka_target`` —— Kafka 每个字段的取值表达式（引用 source 里的路径，或是常量）
- ``remark`` —— 人话说明（尤其是维度展开口径）

测试守住：
1. source 里的字段名**真实存在于实测响应体**（不许自定义字段）；
2. target 的每个字段都有取值表达式，引用的路径能在 source 里解析到；
3. target 的字段集合 = 客户两份对接方案规定的字段集合；
4. 维度展开口径与实测数据一致（有多少行就有多少条消息）。
"""
import io
import json
import os
import re
import sys

import pytest

from conftest import DATA_DIR, REPO_ROOT

sys.path.insert(0, os.path.join(REPO_ROOT, 'tools'))

MAPPING_DIR = os.path.join(DATA_DIR, 'mapping')
RESPONSE_DIR = os.path.join(DATA_DIR, 'metric_response')
MAPPING_PATH = os.path.join(MAPPING_DIR, 'kafka_field_mapping.json')
FIELD_MAP_PATH = os.path.join(MAPPING_DIR, 'itscada_field_map.json')
METRIC_RULES_PATH = os.path.join(MAPPING_DIR, 'metric_rules.json')

SOURCE_PREFIX = 'tpops_response_source.'


def _load(path):
    with io.open(path, encoding='utf-8') as fp:
        return json.load(fp)


@pytest.fixture(scope='module')
def payload():
    return _load(MAPPING_PATH)


@pytest.fixture(scope='module')
def field_map():
    return _load(FIELD_MAP_PATH)


def _metric_fields(field_map):
    return [f['name'] for f in field_map['metric_message']['fields']]


def _alarm_fields(field_map):
    return [f['name'] for f in field_map['alarm_message']['fields']]


# -- 路径解析：把 target 里的表达式落到 source 上 -------------------------

SEGMENT_RE = re.compile(
    r'([A-Za-z_][A-Za-z0-9_]*)'
    r'|\[(\*|-?\d+)?\]'
    r'|\[name=([^\]]+)\]'
    r'|\[\?([A-Za-z_][A-Za-z0-9_]*)\s*(starts with|=)\s*([^\]]+)\]'
)


def _parse_path(expression):
    """把 'tpops_response_source.result[*].col' 拆成路径片段。"""
    path = expression
    for func in ('sum', 'count', 'top'):
        if path.startswith(func + '('):
            path = path[len(func) + 1:].split(',')[0].rstrip(')')
            break
    index = path.find(SOURCE_PREFIX)
    if index < 0:
        return None
    path = path[index + len(SOURCE_PREFIX):]
    parts = []
    for match in SEGMENT_RE.finditer(path):
        key, index, name = match.group(1), match.group(2), match.group(3)
        filter_field, filter_op = match.group(4), match.group(5)
        filter_values = match.group(6)
        if key:
            parts.append(('key', key))
        elif filter_field:
            parts.append(('filter', (filter_field, filter_op, filter_values)))
        elif match.group(0) == '[]' or index == '*':
            parts.append(('all', None))
        elif index is not None:
            parts.append(('index', int(index)))
        elif name:
            parts.append(('name', name))
    return parts


def _resolve(source, parts):
    """按路径片段在 source 里取值；解析不到返回 None。"""
    current = source
    for kind, value in parts:
        if kind == 'key':
            if not isinstance(current, dict) or value not in current:
                return None
            current = current[value]
        elif kind == 'index':
            if not isinstance(current, list):
                return None
            if value >= len(current) or value < -len(current):
                return None
            current = current[value]
        elif kind == 'all':
            if isinstance(current, dict):
                current = list(current.values())
            if not isinstance(current, list) or not current:
                return None
            current = current[0]
        elif kind == 'filter':
            field, operator, values = value
            if not isinstance(current, list):
                return None
            if not any(field in item for item in current if isinstance(item, dict)):
                return None          # 字段不存在 → 表达式写错了
            if operator == '=' and values.strip() == '*':
                # 按该列的每个取值分组：表达式合法即可，取第一行做代表
                return current[0] if current else []
            wanted = [v.strip().lower() for v in values.split('|')]
            hit = None
            for item in current:
                text = str((item or {}).get(field) or '').strip().lower()
                if any(text.startswith(w) for w in wanted):
                    hit = item
                    break
            if hit is None:
                return []            # 字段存在但样本里没有匹配行 → 选不出数据
            current = hit
        elif kind == 'name':
            if not isinstance(current, list):
                return None
            hit = None
            for item in current:
                if isinstance(item, dict) and item.get('name') == value:
                    hit = item
                    break
            if hit is None:
                return None
            current = hit
    return current


def _parse_all(expression):
    """复合表达式（如比值 a ÷ b）拆成多组路径。"""
    if not _is_reference(expression):
        return []
    paths = []
    for side in expression.split(' ÷ '):
        parts = _parse_path(side.strip())
        if parts:
            paths.append(parts)
    return paths


def _is_reference(expression):
    return isinstance(expression, str) and SOURCE_PREFIX in expression


def _real_body(filename):
    """取留档里含业务数据的那一步的响应体。"""
    steps = _load(os.path.join(RESPONSE_DIR, filename))
    for step in steps:
        if 'task-record' in step['api'] and step['api'].rstrip('/').endswith('detail'):
            return step['response_body']
        if step['api'] == 'POST /rds/v3/monitor/metrics':
            return step['response_body']
        if 'slow-sql-list' in step['api'] or 'top-sql-list' in step['api']:
            return step['response_body']
        if step['api'].startswith('GET /rds/v3.2/instances'):
            return step['response_body']
    raise AssertionError('没有找到终止步骤：%s' % filename)


def _first_metric(body):
    for info in body.get('metric_infos') or ():
        for metric in info.get('metrics') or ():
            return metric
    raise AssertionError('没有 metric_infos')


# -- 覆盖面 ---------------------------------------------------------------

def test_covers_all_enabled_metrics(payload):
    metric_rules = _load(METRIC_RULES_PATH)['rules']
    enabled = set(r['metric_code'] for r in metric_rules if r.get('enabled'))
    mapped = set(m['metric_code'] for m in payload['metric_mappings'])
    assert len(payload['metric_mappings']) == 98
    assert mapped == enabled


def test_four_alarm_record_shapes(payload):
    shapes = set(m['shape'] for m in payload['alarm_mappings'])
    assert shapes == {
        'active_convergence', 'active_detail',
        'history_convergence', 'history_detail',
    }


# -- 结构 -----------------------------------------------------------------

def test_every_record_has_the_three_sections(payload):
    for mapping in payload['metric_mappings'] + payload['alarm_mappings']:
        name = mapping.get('metric_code') or mapping.get('shape')
        assert mapping.get('tpops_response_source'), name
        assert mapping.get('kafka_target'), name
        assert mapping.get('remark'), name


def test_target_fields_match_customer_document(payload, field_map):
    """kafka_target 的字段集合 = 客户指标方案的字段表。"""
    expected = _metric_fields(field_map)
    for mapping in payload['metric_mappings']:
        assert list(mapping['kafka_target'].keys()) == expected, mapping['metric_code']


def test_alarm_target_fields_match_customer_document(payload, field_map):
    expected = _alarm_fields(field_map)
    for mapping in payload['alarm_mappings']:
        assert list(mapping['kafka_target'].keys()) == expected, mapping['shape']


# -- source 是真实响应 ----------------------------------------------------

def test_source_fields_exist_in_the_real_response(payload):
    """source 的字段名必须真实存在于实测响应体里（不许自定义字段）。"""
    manifest = _load(os.path.join(RESPONSE_DIR, '000manifest.json'))
    files = dict((m['customer_metric'], m['file']) for m in manifest['metrics'])
    for mapping in payload['metric_mappings']:
        code = mapping['metric_code']
        source = mapping['tpops_response_source']
        real = _real_body(files[code])
        if 'metric_infos' in source:
            for key in source['metric_infos'][0]:
                assert key in real['metric_infos'][0], (code, key)
            real_metric = _first_metric(real)
            for key in source['metric_infos'][0]['metrics'][0]:
                assert key in real_metric, (code, key)
            continue
        if 'result' in source:
            for key in source:
                assert key in real, (code, key)
            continue
        if all(isinstance(value, dict) for value in source.values()):
            # 跨 SQL 的比值：source 按 sql_key 分组，每组仍是原样的响应片段
            for group, fragment in source.items():
                for key in fragment:
                    assert key in real, (code, group, key)
            continue
        for key in source:
            assert key in real, (code, key)


def test_source_carries_real_data(payload):
    """source 里要有真实数据，不是空壳。"""
    for mapping in payload['metric_mappings']:
        code = mapping['metric_code']
        source = mapping['tpops_response_source']
        if 'result' in source:
            assert source['result'], code
            assert isinstance(source['result'][0], dict), code
            assert any(value not in ('', None) for value in source['result'][0].values()), code
        elif 'metric_infos' in source:
            assert source['metric_infos'][0]['metrics'][0]['series'], code
        else:
            assert any(value for value in source.values()), code


# -- target 的取值表达式 --------------------------------------------------

def test_every_target_field_has_an_expression(payload):
    for mapping in payload['metric_mappings'] + payload['alarm_mappings']:
        name = mapping.get('metric_code') or mapping.get('shape')
        for field, expression in mapping['kafka_target'].items():
            if expression is None:
                continue  # unit 可以合法为 null
            assert expression, (name, field)


def test_every_reference_resolves_in_the_source(payload):
    """target 里引用 source 的路径，必须真的能在 source 里解析到。"""
    for mapping in payload['metric_mappings'] + payload['alarm_mappings']:
        name = mapping.get('metric_code') or mapping.get('shape')
        source = mapping['tpops_response_source']
        for field, expression in mapping['kafka_target'].items():
            if not _is_reference(expression):
                continue
            paths = _parse_all(expression)
            assert paths, (name, field, expression)
            for parts in paths:
                assert _resolve(source, parts) is not None, (name, field, expression)


def test_placeholder_bindings_resolve(payload):
    """占位符绑定的列，必须能在 source 的结果行里找到。"""
    for mapping in payload['metric_mappings']:
        bindings = mapping.get('placeholder_bindings')
        if not bindings:
            continue
        source = mapping['tpops_response_source']
        for placeholder, expression in bindings.items():
            paths = _parse_all(expression)
            assert paths, (mapping['metric_code'], placeholder)
            for parts in paths:
                assert _resolve(source, parts) is not None, \
                    (mapping['metric_code'], placeholder, expression)


def test_value_expression_points_at_a_real_column(payload):
    """value 的表达式必须指向 source 里真实存在的列。"""
    for mapping in payload['metric_mappings']:
        expression = mapping['kafka_target']['value']
        paths = _parse_all(expression)
        assert paths, mapping['metric_code']
        for parts in paths:
            assert _resolve(mapping['tpops_response_source'], parts) is not None, \
                mapping['metric_code']


# -- 样例消息 -------------------------------------------------------------

def test_sample_messages_have_exactly_the_declared_fields(payload, field_map):
    for mapping in payload['metric_mappings']:
        message = mapping.get('sample_message')
        if message is None:
            continue
        assert list(message.keys()) == _metric_fields(field_map), mapping['metric_code']
    for mapping in payload['alarm_mappings']:
        assert list(mapping['sample_message'].keys()) == _alarm_fields(field_map), \
            mapping['shape']


def test_required_metric_fields_are_populated(payload, field_map):
    required = set(field_map['metric_message']['required_fields'])
    empty = []
    for mapping in payload['metric_mappings']:
        message = mapping.get('sample_message')
        if message is None:
            continue
        for name in required:
            value = message.get(name)
            if value is None or value == '':
                empty.append('%s.%s' % (mapping['metric_code'], name))
    assert not empty, empty


def test_required_alarm_fields_are_populated(payload, field_map):
    required = set(field_map['alarm_message']['required_fields'])
    empty = []
    for mapping in payload['alarm_mappings']:
        message = mapping['sample_message']
        for name in required:
            value = message.get(name)
            if value is None or value == '':
                empty.append('%s.%s' % (mapping['shape'], name))
    assert not empty, empty


def test_placeholders_are_substituted_in_sample_messages(payload):
    for mapping in payload['metric_mappings']:
        message = mapping.get('sample_message')
        if message is None:
            continue
        for field in ('metricName', 'metricCode'):
            assert not re.search(r'\{#\w+\}', message.get(field) or ''), \
                (mapping['metric_code'], field)


# -- 维度展开口径 ---------------------------------------------------------

def _expansion_of(mapping):
    """从取值表达式判断展开方式。"""
    expression = mapping['kafka_target']['value']
    if expression.startswith('top('):
        return 'single'
    if 'result[?' in expression:
        return 'per_group'
    if expression.startswith('sum('):
        return 'aggregate'
    if expression.endswith('result[*]'):      # raw_rows：整批行装一条消息
        return 'single'
    if 'result[*]' in expression:
        return 'per_row'
    if 'series[*]' in expression:
        return 'per_component'
    return 'single'


def test_per_row_expansion_matches_observed_row_count(payload):
    """按行展开的指标：实测几行就该产出几条消息。"""
    for mapping in payload['metric_mappings']:
        if _expansion_of(mapping) != 'per_row':
            continue
        observed = mapping['evidence'].get('observed', {})
        rows = observed.get('row_count')
        if rows is None:
            continue
        assert mapping['message_count'] == rows, mapping['metric_code']


def test_aggregate_expansion_produces_one_message(payload):
    """整批聚合的指标只产出一条消息。

    **跨视图比率例外**（取值表达式里有 ``÷``）：分子与分母来自不同视图，
    单视图留档里根本没有另一侧 → 分母取不到时引擎按口径「不产出」（M-3，
    不静默出 0），声明里如实记 0 条。这两条指标的真实值由**合并留档**的期望值表
    守（``tests/test_expected_values.py`` 覆盖它们）。
    """
    for mapping in payload['metric_mappings']:
        if _expansion_of(mapping) != 'aggregate':
            continue
        if '÷' in mapping['kafka_target']['value']:
            assert mapping['message_count'] == 0, mapping['metric_code']
            continue
        assert mapping['message_count'] == 1, mapping['metric_code']


def test_remark_explains_the_expansion(payload):
    """remark 要把展开口径讲清楚。"""
    for mapping in payload['metric_mappings']:
        expansion = _expansion_of(mapping)
        remark = mapping['remark']
        if expansion == 'per_row':
            assert '一行一条消息' in remark, mapping['metric_code']
        elif expansion == 'per_component':
            assert '每个组件一条消息' in remark, mapping['metric_code']


# -- 资源粒度与数据分布 ----------------------------------------------------

def test_every_metric_declares_its_resource_granularity(payload):
    """业务对象粒度（表/库/端口/实例）由占位符决定。"""
    for mapping in payload['metric_mappings']:
        assert mapping.get('resource_granularity') in (
            'instance', 'database', 'table', 'port'), mapping['metric_code']


def test_granularity_matches_placeholders(payload):
    metric_rules = dict(
        (r['metric_code'], r) for r in _load(METRIC_RULES_PATH)['rules']
    )
    for mapping in payload['metric_mappings']:
        rule = metric_rules[mapping['metric_code']]
        placeholders = set(rule.get('placeholders') or ())
        if 'NAME' in placeholders:
            expected = 'table'
        elif placeholders & {'DATNAME', 'DBNAME', 'DATABASE_NAME'}:
            expected = 'database'
        elif 'PORT' in placeholders:
            expected = 'port'
        else:
            expected = 'instance'
        assert mapping['resource_granularity'] == expected, mapping['metric_code']


def test_node_scoped_is_recorded(payload):
    """数据是不是按节点分布 —— 这是独立于粒度的另一个维度。"""
    for mapping in payload['metric_mappings']:
        assert isinstance(mapping.get('node_scoped'), bool), mapping['metric_code']


def test_resource_name_is_node_ip_and_node_id(payload):
    """resourceName 只表示节点 → 固定「节点 IP + 节点 ID」。"""
    for mapping in payload['metric_mappings']:
        message = mapping.get('sample_message')
        if message is None:
            continue
        assert mapping['kafka_target']['resourceName'] == '{host_ip}_{node_id}', \
            mapping['metric_code']

        resource = message['resourceName']
        host_ip = message['tags'].get('host_ip')
        node_id = message['tags'].get('node_id')
        assert host_ip, mapping['metric_code']
        assert resource.startswith(host_ip + '_'), mapping['metric_code']
        assert node_id and resource == '%s_%s' % (host_ip, node_id), \
            mapping['metric_code']


def test_resource_name_does_not_carry_business_dimensions(payload):
    """表名/库名/端口这类业务维度不进资源名（只放 tags.dimension_values）。"""
    for mapping in payload['metric_mappings']:
        message = mapping.get('sample_message')
        if message is None:
            continue
        resource = message['resourceName']
        dimension = message['tags'].get('dimension_values') or {}
        for name, value in dimension.items():
            assert str(value) not in resource, mapping['metric_code']


def test_instance_scoped_metrics_use_the_first_master_node(payload):
    """与节点无关的指标 → 落到节点清单里的第一个 master CN（跨轮稳定）。"""
    master = '172.16.103.126_3881c04403394ee1b36a2bf873e69007no14'
    for mapping in payload['metric_mappings']:
        message = mapping.get('sample_message')
        if message is None or mapping['node_scoped']:
            continue
        assert message['resourceName'] == master, mapping['metric_code']


def test_table_level_metrics_are_not_node_scoped(payload):
    """表级指标来自 DN 侧视图，各 master 数据相同 —— 加 node_id 只会把资源拆多份。"""
    for mapping in payload['metric_mappings']:
        if mapping['resource_granularity'] != 'table':
            continue
        assert mapping['node_scoped'] is False, mapping['metric_code']


def test_master_view_is_recorded_from_measurements(payload):
    """命令中心的指标要标明各 master 的数据是否相同（实测得出）。"""
    for mapping in payload['metric_mappings']:
        if mapping['tpops_response_source'].get('sql_key') is None:
            continue
        assert mapping.get('master_view') in ('identical', 'divergent'), \
            mapping['metric_code']


# -- 文件是最新的 ---------------------------------------------------------

def test_file_is_up_to_date():
    """文件必须与生成器输出一致（映射条件改了要重新生成）。"""
    sys.path.insert(0, os.path.join(REPO_ROOT, 'tools'))
    import build_kafka_mapping

    assert _load(MAPPING_PATH) == build_kafka_mapping.build(), \
        'kafka_field_mapping.json 过期，请运行 tools/build_kafka_mapping.py'
