# -*- coding: utf-8 -*-
"""映射表加载与查表。

映射规则是**数据**（JSON），这里是加载入口与查表接口。
规则内容改了就改 ``data/mapping/`` 下的 JSON，不用改代码。
"""
from __future__ import print_function

import io
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(REPO_ROOT, 'data', 'mapping')
FIELD_MAP_FILE = 'itscada_field_map.json'
METRIC_RULES_FILE = 'metric_rules.json'
ALARM_RULES_FILE = 'alarm_rules.json'
CATEGORY_MAPPING_FILES = {
    'monitor_metrics': 'metric_mapping_monitor_metrics.json',
    'command_center_sql': 'metric_mapping_command_center_sql.json',
    'diagnostic': 'metric_mapping_diagnostic.json',
    'instance': 'metric_mapping_instance.json',
    'alarm': 'alarm_mapping.json',
}


class MappingError(Exception):
    """映射表缺失或结构不合法。"""


@dataclass(frozen=True)
class MetricRule:
    """一个客户指标的取值规则。"""

    metric_code: str
    metric_name: str
    resource_type: str
    category: str
    value_type: str
    unit: Optional[str]
    collect_cycle: Mapping[str, Any]
    status: str
    match_level: Optional[str]
    enabled: bool
    source: Mapping[str, Any]
    value_rule: Mapping[str, Any]
    placeholders: Tuple[str, ...]
    dimension_bindings: Mapping[str, str]
    note: str
    needs_confirmation: bool

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> 'MetricRule':
        return cls(
            metric_code=payload['metric_code'],
            metric_name=payload['metric_name'],
            resource_type=payload['resource_type'],
            category=payload['category'],
            value_type=payload['value_type'],
            unit=payload.get('unit'),
            collect_cycle=payload.get('collect_cycle', {}),
            status=payload['status'],
            match_level=payload.get('match_level'),
            enabled=bool(payload.get('enabled')),
            source=payload.get('source', {}),
            value_rule=payload.get('value_rule') or {},
            placeholders=tuple(payload.get('placeholders', ())),
            dimension_bindings=payload.get('dimension_bindings', {}),
            note=payload.get('note', ''),
            needs_confirmation=bool(payload.get('needs_confirmation')),
        )


@dataclass(frozen=True)
class AlarmDefinition:
    """一条平台告警定义 → ITSCADA 告警指标的挂载关系。"""

    alarm_id: str
    alarm_name: str
    alarm_kind: str
    default_level: str
    default_priority: Optional[int]
    threshold: str
    metric_code: str
    metric_name: str
    value_source: str
    linked_customer_metric: Optional[str]


class MappingTable:
    """三份规则文件的只读视图。"""

    def __init__(
        self,
        field_map: Mapping[str, Any],
        metric_rules: Tuple[MetricRule, ...],
        alarm_rules: Mapping[str, Any],
        category_mappings: Mapping[str, Mapping[str, Any]],
    ) -> None:
        self._field_map = field_map
        self._metric_rules = metric_rules
        self._alarm_rules = alarm_rules
        self._category_mappings = category_mappings
        self._by_code = {rule.metric_code: rule for rule in metric_rules}

    @classmethod
    def load(cls, data_dir: str = DATA_DIR) -> 'MappingTable':
        field_map = _read_json(os.path.join(data_dir, FIELD_MAP_FILE))
        metric_payload = _read_json(os.path.join(data_dir, METRIC_RULES_FILE))
        alarm_payload = _read_json(os.path.join(data_dir, ALARM_RULES_FILE))
        category_mappings = {}
        for key, filename in CATEGORY_MAPPING_FILES.items():
            category_mappings[key] = _read_json(os.path.join(data_dir, filename))
        rules = tuple(MetricRule.from_dict(item) for item in metric_payload['rules'])
        return cls(field_map, rules, alarm_payload, category_mappings)

    # -- 字段映射 ---------------------------------------------------------
    @property
    def field_map(self) -> Mapping[str, Any]:
        return self._field_map

    def message_fields(self, message_type: str) -> Tuple[Mapping[str, Any], ...]:
        """'metric' / 'alarm' → 该消息的字段定义。"""
        section = self._field_map.get('%s_message' % message_type)
        if not section:
            raise MappingError('字段映射表里没有 %s_message' % message_type)
        return tuple(section['fields'])

    def required_fields(self, message_type: str) -> Tuple[str, ...]:
        section = self._field_map.get('%s_message' % message_type)
        if not section:
            raise MappingError('字段映射表里没有 %s_message' % message_type)
        return tuple(section.get('required_fields', ()))

    @property
    def resource_name_template(self) -> str:
        return self._field_map['resource_name']['template']

    @property
    def resource_name_fallback(self) -> str:
        return self._field_map['resource_name']['fallback_template']

    # -- 指标规则 ---------------------------------------------------------
    @property
    def metric_rules(self) -> Tuple[MetricRule, ...]:
        return self._metric_rules

    def metric_rule(self, metric_code: str) -> Optional[MetricRule]:
        return self._by_code.get(metric_code)

    def enabled_metric_rules(self) -> Tuple[MetricRule, ...]:
        return tuple(rule for rule in self._metric_rules if rule.enabled)

    def metrics_of_sql_key(self, sql_key: str) -> Tuple[MetricRule, ...]:
        return tuple(
            rule for rule in self._metric_rules
            if rule.enabled and rule.source.get('sql_key') == sql_key
        )

    def metrics_of_channel(self, channel: str) -> Tuple[MetricRule, ...]:
        return tuple(
            rule for rule in self._metric_rules
            if rule.enabled and rule.source.get('source') == channel
        )

    # -- 告警规则 ---------------------------------------------------------
    @property
    def alarm_rules(self) -> Mapping[str, Any]:
        return self._alarm_rules

    def alarm_definition(self, alarm_id: str) -> Optional[AlarmDefinition]:
        raw = self._alarm_rules.get('definitions', {}).get(str(alarm_id))
        if not raw:
            return None
        return AlarmDefinition(
            alarm_id=str(alarm_id),
            alarm_name=raw.get('alarm_name', ''),
            alarm_kind=raw.get('alarm_kind', ''),
            default_level=raw.get('default_level', ''),
            default_priority=raw.get('default_priority'),
            threshold=raw.get('threshold', ''),
            metric_code=raw.get('metric_code', ''),
            metric_name=raw.get('metric_name', ''),
            value_source=raw.get('value_source', 'alarm_items_sum'),
            linked_customer_metric=raw.get('linked_customer_metric'),
        )

    def severity_to_priority(self, severity: str) -> Optional[int]:
        return self._alarm_rules.get('severity_to_priority', {}).get(severity)

    def status_to_itscada(self, status: str) -> Optional[int]:
        return self._alarm_rules.get('status_to_itscada', {}).get(status)

    # -- 分类字段映射 -----------------------------------------------------
    def category_mapping(self, category: str) -> Mapping[str, Any]:
        """某个采集分类的字段映射表（monitor_metrics / … / alarm）。"""
        if category not in self._category_mappings:
            raise MappingError('没有这个分类的字段映射：%s' % category)
        return self._category_mappings[category]

    @property
    def category_mappings(self) -> Mapping[str, Mapping[str, Any]]:
        return self._category_mappings


def _read_json(path: str) -> Dict[str, Any]:
    if not os.path.isfile(path):
        raise MappingError('映射文件不存在：%s' % path)
    try:
        with io.open(path, encoding='utf-8') as handle:
            return json.load(handle)
    except ValueError as exc:
        raise MappingError('映射文件不是合法 JSON：%s（%s）' % (path, exc))
