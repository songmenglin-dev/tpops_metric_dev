"""唯一 JSON 序列化出口（任务 1.4）。

守住三件事：
1. **信封**：指标 7 字段 / 告警 13 字段，字段集合与顺序与
   ``data/mapping/itscada_field_map.json`` 逐字段一致；
2. **缺省**：缺失值一律 ``null``（不填 0、不填空串），时间是 13 位毫秒；
3. **值类型**：整数 / 浮点 / 字符 / 文本按客户指标表【数据类型】序列化，
   浮点保留 2 位小数 —— 口径与映射层 ``serialize_value`` 一致（不另起一套）。
"""
import ast
import json
import os

import pytest

from conftest import DATA_DIR
from tpops_kafka_collector.kafka.serializer import (
    ALARM_FIELDS,
    ALARM_TAG_FIELDS,
    METRIC_FIELDS,
    METRIC_TAG_FIELDS,
    SerializerError,
    apply_value_type,
    serialize_alarm,
    serialize_metric,
    to_epoch_millis,
)
from tpops_kafka_collector.mapping.message import build_metric_message, serialize_value

FIELD_MAP_PATH = os.path.join(DATA_DIR, 'mapping', 'itscada_field_map.json')

# 客户指标表里的一条规则（缓存命中率，表级维度）
_RULE = type('Rule', (), {
    'metric_name': 'GAUSSDB.[{#DATNAME}]_cache_hit_ratio',
    'metric_code': 'GAUSSDB.[{#DATNAME}]_cache_hit_ratio',
    'value_type': '浮点',
    'unit': '%',
})()


@pytest.fixture(scope='module')
def field_map():
    with open(FIELD_MAP_PATH, encoding='utf-8') as handle:
        return json.load(handle)


def _metric_message(**overrides):
    message = {
        'metricName': 'GAUSSDB.[{#DATNAME}]_cache_hit_ratio',
        'metricCode': 'GAUSSDB.[{#DATNAME}]_cache_hit_ratio',
        'resourceName': '172.16.103.126_3881c04403394ee1b36a2bf873e69007no14',
        'value': 99.84,
        'unit': '%',
        'clock': 1794447039320,
        'tags': {
            'instance_id': '83cd68f2e4cb40b4b7e69240479ffa26in14',
            'instance_name': 'gauss-6374',
            'node_id': '3881c04403394ee1b36a2bf873e69007no14',
            'component_id': 'cn_5001',
            'host_ip': '172.16.103.126',
            'dimension_values': {'DATNAME': 'postgres'},
        },
    }
    message.update(overrides)
    return message


def _alarm_message(**overrides):
    message = {
        'alarmId': '2007010001',
        'alarmName': '节点NTP状态异常',
        'alarmContent': '节点 NTP 时间偏移过大',
        'priority': 2,
        'alarmTime': 1793758083000,
        'status': 1,
        'recoverTime': None,
        'resourceName': '172.16.103.126_3881c04403394ee1b36a2bf873e69007no14',
        'metricName': 'gaussdb.Tps',
        'metricCode': 'gaussdb.Tps',
        'value': '1',
        'unit': None,
        'tags': {
            'instance_id': '83cd68f2e4cb40b4b7e69240479ffa26in14',
            'instance_name': 'gauss-6374',
            'node_id': '3881c04403394ee1b36a2bf873e69007no14',
            'host_ip': '172.16.103.126',
            'alarm_category': '节点状态',
            'alarm_type': 'event',
            'engine': 'gaussdb',
            'alarm_duration': '4小时5分钟',
            'details': [],
        },
    }
    message.update(overrides)
    return message


def _decoded(text):
    return json.loads(text)


def _key_order(text):
    return list(json.loads(text, object_pairs_hook=lambda pairs: [k for k, _ in pairs]))


# -- 信封：字段集合与顺序 ------------------------------------------------
def test_metric_envelope_has_exactly_seven_fields():
    payload = _decoded(serialize_metric(_metric_message()))
    assert len(payload) == 7
    assert set(payload) == set(METRIC_FIELDS)


def test_alarm_envelope_has_exactly_thirteen_fields():
    payload = _decoded(serialize_alarm(_alarm_message()))
    assert len(payload) == 13
    assert set(payload) == set(ALARM_FIELDS)


def test_metric_field_order_is_fixed():
    assert _key_order(serialize_metric(_metric_message())) == list(METRIC_FIELDS)


def test_alarm_field_order_is_fixed():
    assert _key_order(serialize_alarm(_alarm_message())) == list(ALARM_FIELDS)


def test_two_serializations_have_the_same_field_order():
    first = serialize_metric(_metric_message())
    second = serialize_metric(_metric_message())
    assert first == second


def test_envelope_matches_customer_field_map(field_map):
    """字段集合与顺序必须与客户字段表逐字段一致。"""
    assert list(METRIC_FIELDS) == [
        item['name'] for item in field_map['metric_message']['fields']
    ]
    assert list(ALARM_FIELDS) == [
        item['name'] for item in field_map['alarm_message']['fields']
    ]
    assert list(METRIC_TAG_FIELDS) == list(field_map['tags']['metric'])
    assert list(ALARM_TAG_FIELDS) == list(field_map['tags']['alarm'])


# -- 缺省约定 ------------------------------------------------------------
def test_missing_fields_become_null():
    message = _metric_message()
    del message['unit']
    payload = _decoded(serialize_metric(message))
    assert payload['unit'] is None


def test_missing_value_becomes_null_not_zero():
    payload = _decoded(serialize_metric(_metric_message(value=None)))
    assert payload['value'] is None
    assert payload['value'] != 0


def test_empty_unit_becomes_null():
    payload = _decoded(serialize_metric(_metric_message(unit='')))
    assert payload['unit'] is None


def test_extra_fields_are_dropped():
    payload = _decoded(serialize_metric(_metric_message(secret='x')))
    assert 'secret' not in payload


def test_extra_tag_keys_are_dropped():
    message = _metric_message()
    message['tags']['unexpected'] = 'x'
    payload = _decoded(serialize_metric(message))
    assert set(payload['tags']) == set(METRIC_TAG_FIELDS)


def test_dimension_values_default_to_empty_object():
    message = _metric_message()
    del message['tags']['dimension_values']
    payload = _decoded(serialize_metric(message))
    assert payload['tags']['dimension_values'] == {}


def test_alarm_details_default_to_empty_array():
    message = _alarm_message()
    del message['tags']['details']
    payload = _decoded(serialize_alarm(message))
    assert payload['tags']['details'] == []


# -- 编码与转义 ----------------------------------------------------------
def test_chinese_is_not_escaped():
    text = serialize_metric(_metric_message(metricName='每秒提交事务数'))
    assert '每秒提交事务数' in text
    assert '\\u' not in text


def test_output_is_valid_utf8_json():
    text = serialize_alarm(_alarm_message(alarmContent='节点 NTP 异常'))
    assert json.loads(text.encode('utf-8').decode('utf-8'))['alarmContent'] == '节点 NTP 异常'


# -- 值类型 --------------------------------------------------------------
def test_integer_value_is_json_number():
    payload = _decoded(serialize_metric(_metric_message(value='12345'), value_type='整数'))
    assert payload['value'] == 12345
    assert isinstance(payload['value'], int)


def test_float_value_keeps_two_decimals():
    payload = _decoded(
        serialize_metric(_metric_message(value=0.7941176470588235), value_type='浮点')
    )
    assert payload['value'] == 0.79


def test_text_value_is_json_string():
    payload = _decoded(
        serialize_metric(_metric_message(value='V2.0-10.0.0'), value_type='文本')
    )
    assert payload['value'] == 'V2.0-10.0.0'
    assert isinstance(payload['value'], str)


def test_char_value_is_json_string():
    payload = _decoded(serialize_metric(_metric_message(value=7), value_type='字符'))
    assert payload['value'] == '7'


def test_value_serialization_matches_mapping_layer():
    """口径必须与映射层一致 —— 不另起一套值转换逻辑。"""
    cases = [
        ('整数', '12345'),
        ('整数', 12345),
        ('整数', None),
        ('浮点', 0.7941176470588235),
        ('浮点', '1.005'),
        ('浮点', None),
        ('字符', 7),
        ('字符', 'abc'),
        ('文本', 'V2.0-10.0.0'),
        ('文本', None),
    ]
    for value_type, value in cases:
        payload = _decoded(serialize_metric(_metric_message(value=value), value_type=value_type))
        assert payload['value'] == serialize_value(value, value_type), (value_type, value)
        assert payload['value'] == apply_value_type(value, value_type), (value_type, value)


def test_apply_value_type_without_type_keeps_value():
    """不传类型 → 原样落地（映射层已定型过时走这条）。"""
    assert apply_value_type(12345, None) == 12345
    assert apply_value_type(None, None) is None


def test_alarm_value_is_serialized_as_string():
    payload = _decoded(serialize_alarm(_alarm_message(value=1)))
    assert payload['value'] == '1'


# -- 时间戳 --------------------------------------------------------------
def test_clock_is_13_digit_milliseconds():
    payload = _decoded(serialize_metric(_metric_message(clock=1794447039320)))
    assert payload['clock'] == 1794447039320
    assert len(str(payload['clock'])) == 13


def test_seconds_are_scaled_to_milliseconds():
    assert to_epoch_millis(1794447039) == 1794447039000


def test_float_milliseconds_are_truncated_to_int():
    assert to_epoch_millis(1794447039320.0) == 1794447039320


def test_missing_timestamp_becomes_null():
    payload = _decoded(serialize_metric(_metric_message(clock=None)))
    assert payload['clock'] is None


def test_alarm_times_are_milliseconds():
    payload = _decoded(serialize_alarm(_alarm_message(alarmTime=1793758083000)))
    assert payload['alarmTime'] == 1793758083000
    payload = _decoded(serialize_alarm(_alarm_message(recoverTime=1793771156000)))
    assert payload['recoverTime'] == 1793771156000


# -- 与映射层的衔接 ------------------------------------------------------
def test_serialized_mapping_message_has_the_full_envelope(field_map):
    """映射层组装的消息经序列化后仍是 7 字段 —— 信封不被序列化改写。"""
    message = build_metric_message(
        field_map,
        _RULE,
        {'DATNAME': 'postgres'},
        12.5,
        {'instance_id': 'i-1', 'instance_name': 'gauss-6374',
         'node_id': 'n-1', 'component_id': 'cn_5001', 'host_ip': '172.16.103.126'},
        1794447039320,
    )
    payload = _decoded(serialize_metric(message))
    assert list(payload) == list(METRIC_FIELDS)
    assert payload['tags']['dimension_values'] == {'DATNAME': 'postgres'}


def test_value_type_none_keeps_value_untouched():
    """不传数据类型时值原样落地（映射层已定型过）。"""
    payload = _decoded(serialize_metric(_metric_message(value='12345')))
    assert payload['value'] == '12345'


# -- 唯一序列化出口 ------------------------------------------------------
# -- 唯一序列化出口 ------------------------------------------------------
#: 允许调用 json.dumps / json.dump 的地方，以及为什么允许。
#: 合同要求「全仓只有这一处 json.dumps 产出消息」—— 精确的形态是：
#: 只有 kafka/serializer.py 能产出消息；其余 json 编码是别的用途
#: （状态文件落盘），必须逐个登记用途，不允许「扫不到就当没有」。
_ALLOWED_JSON_WRITERS = {
    'kafka/serializer.py': '唯一的消息序列化出口',
    'watermark.py': '水位状态文件落盘（不是消息）',
    'health.py': '健康端点响应体（不是消息，不进 Kafka）',
}


def _package_dir():
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        'tpops_kafka_collector',
    )


def _json_writer_calls(path):
    """返回该文件里所有 json.dumps / json.dump 调用的行号。"""
    with open(path, encoding='utf-8') as handle:
        tree = ast.parse(handle.read(), filename=path)
    hits = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr in ('dumps', 'dump')
            and isinstance(node.value, ast.Name)
            and node.value.id == 'json'
        ):
            hits.append(node.lineno)
    return hits


def test_package_has_only_one_json_dumps_for_messages():
    """整个生产包里，产出消息的 json.dumps 只有一处。

    合同：「完成后全仓只有这一处 json.dumps 产出消息」。
    这里扫**整个** tpops_kafka_collector/（不是只扫投递层），
    白名单逐个登记用途 —— 扫不到就当没有，正是这个守卫要防的失败形态。
    """
    package_dir = _package_dir()
    offenders = []
    for root, _dirs, files in os.walk(package_dir):
        for name in sorted(files):
            if not name.endswith('.py'):
                continue
            path = os.path.join(root, name)
            relative = os.path.relpath(path, package_dir).replace(os.sep, '/')
            if relative in _ALLOWED_JSON_WRITERS:
                continue
            for lineno in _json_writer_calls(path):
                offenders.append(relative + ':' + str(lineno))
    assert not offenders, (
        '生产包里除已登记的 '
        + str(sorted(_ALLOWED_JSON_WRITERS))
        + ' 外还有 json.dumps：'
        + str(offenders)
        + ' —— 消息只能有一个序列化出口'
    )


def test_allowed_json_writers_are_still_actually_used():
    """白名单不能变成免死金牌 —— 登记的用途必须真的还在用。

    否则有人删掉 serializer.py 的 json.dumps 之后，白名单会变成一个
    永远生效的豁免，守卫就瞎了。
    """
    package_dir = _package_dir()
    for relative in sorted(_ALLOWED_JSON_WRITERS):
        path = os.path.join(package_dir, relative)
        assert _json_writer_calls(path), (
            relative + ' 登记为允许 json.dumps，但实际没有调用 —— 白名单该清理了'
        )




# -- 错误处理 ------------------------------------------------------------
def test_non_mapping_message_raises():
    with pytest.raises(SerializerError):
        serialize_metric(['not', 'a', 'mapping'])


def test_nan_value_is_rejected():
    with pytest.raises(SerializerError):
        serialize_metric(_metric_message(value=float('nan')), value_type='浮点')
