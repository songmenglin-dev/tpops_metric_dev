# -*- coding: utf-8 -*-
"""按 ITSCADA 字段规范组装消息。

两份对接方案（指标 / 告警）规定的字段集合就是这里输出的字段集合，
不多不少；字段含义见 ``data/itscada_field_map.json``。
"""
from __future__ import print_function

from typing import Any, Dict, Mapping, Optional, Sequence

from .rules import build_resource_name, substitute_placeholders


def build_metric_message(
    field_map: Mapping[str, Any],
    metric_rule,
    dimensions: Mapping[str, Any],
    value: Any,
    resource_context: Mapping[str, Any],
    timestamp: int,
    tags: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """组装一条 ITSCADA 指标消息。"""
    metric_name = substitute_placeholders(metric_rule.metric_name, dimensions)
    metric_code = substitute_placeholders(metric_rule.metric_code, dimensions)
    template, fallback = _resource_name_templates(field_map, metric_rule)
    resource_name = build_resource_name(
        template,
        dict(resource_context, **dimensions),
        fallback,
    )
    return {
        'metricName': metric_name,
        'metricCode': metric_code,
        'resourceName': resource_name,
        'value': serialize_value(value, metric_rule.value_type),
        'unit': metric_rule.unit,
        'clock': timestamp,
        'tags': _metric_tags(resource_context, dimensions, tags),
    }


def build_alarm_message(
    field_map: Mapping[str, Any],
    record: Mapping[str, Any],
    definition,
    resource_context: Mapping[str, Any],
    severity_to_priority: Mapping[str, int],
    status_to_itscada: Mapping[str, int],
    metric_value: Any = None,
    unit: Optional[str] = None,
    details: Optional[Sequence[Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    """组装一条 ITSCADA 告警消息。"""
    status = status_to_itscada.get(record.get('status'))
    priority = severity_to_priority.get(record.get('severity'))
    if priority is None:
        priority = definition.default_priority
    content = _alarm_content(record, details)
    value = metric_value if definition.value_source == 'latest_metric_value' else record.get('alarm_items_sum')
    return {
        'alarmId': str(record.get('alarm_id')),
        'alarmName': record.get('alarm_name') or definition.alarm_name,
        'alarmContent': content,
        'priority': priority,
        'alarmTime': record.get('first_alarm_time'),
        'status': status,
        'recoverTime': record.get('clear_alarm_time') if status == 0 else None,
        'resourceName': build_resource_name(
            field_map['resource_name']['template'],
            resource_context,
            field_map['resource_name'].get('fallback_template'),
        ),
        'metricName': definition.metric_name,
        'metricCode': definition.metric_code,
        'value': serialize_value(value, '字符'),
        'unit': unit,
        'tags': _alarm_tags(record, details),
    }


def resource_name_template(field_map, placeholders, node_scoped=False):
    """resourceName 模板：实例名 + [节点] + 每个占位符各占一段。

    占位符（表名/库名/端口号）取自结果行，拼进资源名，
    客户才能按 ITSCADA 台账对上。
    节点这一段只在数据按节点分布时才加（实测结论，见 tools/distribution.py）。
    """
    config = field_map['resource_name']
    template = config.get('metric_template', '{instance_name}')
    if node_scoped:
        template += '_{node_id}'
    for name in placeholders or ():
        template += '_{%s}' % name
    return template


def _resource_name_templates(field_map, metric_rule):
    """取 resourceName 模板与回退模板。

    优先用规则里写好的模板（映射表与代码共用同一份，避免各说各话）。
    """
    config = field_map['resource_name']
    template = getattr(metric_rule, 'resource_name_template', None)
    if not template:
        template = resource_name_template(
            field_map,
            getattr(metric_rule, 'placeholders', ()),
            bool(getattr(metric_rule, 'node_scoped', False)),
        )
    return template, config.get('fallback_template', '{instance_name}')


def serialize_value(value: Any, value_type: str) -> Any:
    """按客户指标表的数据类型序列化取值。

    文档字段表写 value 是 String，样例里指标是数字、告警是字符串；
    这里统一按客户表【数据类型】来，缺失值一律 null（不填 0）。
    """
    if value is None:
        return None
    if value_type == '整数':
        return _as_int(value)
    if value_type == '浮点':
        return _as_float(value)
    if isinstance(value, (list, tuple)):
        return list(value)
    return str(value) if not isinstance(value, str) else value


def _as_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _as_float(value):
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _metric_tags(resource_context, dimensions, extra):
    """指标消息的标签：资源属性 + 维度值。"""
    tags = {
        'instance_id': resource_context.get('instance_id'),
        'instance_name': resource_context.get('instance_name'),
        'node_id': resource_context.get('node_id'),
        'component_id': resource_context.get('component_id'),
        'host_ip': resource_context.get('host_ip'),
    }
    if dimensions:
        # COMPONENT_ID 是平台序列键，属于资源属性而不是客户维度
        customer_dims = {
            key: value for key, value in dimensions.items() if key != 'COMPONENT_ID'
        }
        if customer_dims:
            tags['dimension_values'] = customer_dims
    tags.update(extra or {})
    return tags


def _alarm_content(record, details):
    for detail in details or ():
        if detail.get('description'):
            return detail['description']
    if record.get('description'):
        return record['description']
    if record.get('cause'):
        return record['cause']
    return record.get('alarm_name') or ''


def _alarm_tags(record, details):
    tags = {
        'instance_id': record.get('instance_id'),
        'instance_name': record.get('instance_name'),
        'node_id': record.get('node_id'),
        'host_ip': record.get('host_ip'),
        'alarm_category': record.get('alarm_category'),
        'alarm_type': record.get('alarm_type'),
        'engine': record.get('engine'),
        'alarm_duration': record.get('alarm_duration'),
    }
    if details:
        tags['details'] = [dict(detail) for detail in details]
    return tags
