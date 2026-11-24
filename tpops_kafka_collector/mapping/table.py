"""映射表加载与查表。

映射规则是**数据**（JSON），这里是加载入口与查表接口。
规则内容改了就改 ``data/mapping/`` 下的 JSON，不用改代码。
"""

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

from ..errors import MappingError
from .columns import merged_archive_view_columns, validate_source_columns

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(REPO_ROOT, 'data', 'mapping')
FIELD_MAP_FILE = 'itscada_field_map.json'
METRIC_RULES_FILE = 'metric_rules.json'
ALARM_RULES_FILE = 'alarm_rules.json'
# 合并形态的实测留档 —— **来源列的权威来源**（开发方案 §5.6.4）。
# 旧的 ``data/command_center/results/`` 已于 2026-11-13 删除，不得再引用。
PROBE_FILE = 'sql_merge_probe_result.json'
# 旧的分类字段映射表已归档（2026-11-13）—— 现在的主映射表是
# kafka_field_mapping.json，逐指标列出「响应的某个值 → Kafka 的某个字段」。
# 这几个文件留作历史对照与字段集合校验的基准。
ARCHIVE_DIR = 'archive'
CATEGORY_MAPPING_FILES = {
    'monitor_metrics': ARCHIVE_DIR + '/metric_mapping_monitor_metrics.json',
    'command_center_sql': ARCHIVE_DIR + '/metric_mapping_command_center_sql.json',
    'diagnostic': ARCHIVE_DIR + '/metric_mapping_diagnostic.json',
    'instance': ARCHIVE_DIR + '/metric_mapping_instance.json',
    'alarm': ARCHIVE_DIR + '/alarm_mapping.json',
}


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
    resource_granularity: str
    master_view: Optional[str]
    node_scoped: bool
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
            resource_granularity=payload.get('resource_granularity', 'instance'),
            master_view=payload.get('master_view'),
            node_scoped=bool(payload.get('node_scoped')),
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


def build_alarm_definition(
    alarm_rules: Mapping[str, Any],
    alarm_id: Any,
) -> Optional[AlarmDefinition]:
    """按 ``alarm_id`` 从告警规则表里取一条定义。

    **唯一的告警定义查表实现** —— ``MappingTable``（离线工具用）与
    ``mapping.loader.LoadedMapping``（**生产装配用的那个**）共用这一份，
    避免两处分叉。查不到返回 ``None``（调用方按「自造指标名」兜底）。
    """
    raw = (alarm_rules or {}).get('definitions', {}).get(str(alarm_id))
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


class MappingTable:
    """三份规则文件的只读视图。

    .. warning::
       **离线工具专用，生产路径禁止使用。**

       :meth:`load` 的默认 ``data_dir`` 指向**仓库根** ``data/mapping``
       （要读 ``archive/`` 与 ``sql_merge_probe_result.json`` —— 这两个文件
       不在包内 ``mapping/data/``）。用它就等于又读上了仓库根，
       采集端不再是**独立可部署**的产物。

       生产查表走 :mod:`tpops_kafka_collector.mapping.loader`（读包内数据）。
       ``tests/unit/test_package_boundary.py`` 有守卫盯着生产代码不碰它。
    """

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
    def load(
        cls,
        data_dir: str = DATA_DIR,
        validate_columns: bool = True,
        probe_path: Optional[str] = None,
    ) -> 'MappingTable':
        """加载映射表。

        :param validate_columns: 启动期校验来源列（开发方案 §5.6.4）——
            每条规则要取的列必须真实存在于**实测留档**的实际返回列里。
            对不上就抛 ``MappingError``（不可重试）→ 启动失败并打印差异。
        :param probe_path: 合并形态的实测留档路径；缺省按 ``data_dir`` 的
            同级 ``merged_sql/`` 推导（映射数据搬进包内时路径跟着搬）。
        """
        field_map = _read_json(os.path.join(data_dir, FIELD_MAP_FILE))
        metric_payload = _read_json(os.path.join(data_dir, METRIC_RULES_FILE))
        alarm_payload = _read_json(os.path.join(data_dir, ALARM_RULES_FILE))
        category_mappings = {}
        for key, filename in CATEGORY_MAPPING_FILES.items():
            category_mappings[key] = _read_json(os.path.join(data_dir, filename))
        rules = tuple(MetricRule.from_dict(item) for item in metric_payload['rules'])
        table = cls(field_map, rules, alarm_payload, category_mappings)
        if validate_columns:
            if probe_path is None:
                probe_path = _default_probe_path(data_dir)
            table.validate_source_columns(probe_path)
        return table

    def validate_source_columns(self, probe_path: str) -> None:
        """校验每条规则的来源列都存在于实测留档的实际返回列中。

        留档是**事实**，映射表是**声明** —— 两者对不上就启动失败
        （``MappingError`` 不可重试），否则采集端会取到不存在的列、静默出 0。
        """
        payload = _read_json(probe_path)
        columns = merged_archive_view_columns(payload)
        validate_source_columns(self._metric_rules, columns)

    # -- 字段映射 ---------------------------------------------------------
    @property
    def field_map(self) -> Mapping[str, Any]:
        return self._field_map

    def message_fields(self, message_type: str) -> Tuple[Mapping[str, Any], ...]:
        """'metric' / 'alarm' → 该消息的字段定义。"""
        section = self._field_map.get(f'{message_type}_message')
        if not section:
            raise MappingError(f'字段映射表里没有 {message_type}_message')
        return tuple(section['fields'])

    def required_fields(self, message_type: str) -> Tuple[str, ...]:
        section = self._field_map.get(f'{message_type}_message')
        if not section:
            raise MappingError(f'字段映射表里没有 {message_type}_message')
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
        return build_alarm_definition(self._alarm_rules, alarm_id)

    def severity_to_priority(self, severity: str) -> Optional[int]:
        return self._alarm_rules.get('severity_to_priority', {}).get(severity)

    def status_to_itscada(self, status: str) -> Optional[int]:
        return self._alarm_rules.get('status_to_itscada', {}).get(status)

    # -- 分类字段映射 -----------------------------------------------------
    def category_mapping(self, category: str) -> Mapping[str, Any]:
        """某个采集分类的字段映射表（monitor_metrics / … / alarm）。"""
        if category not in self._category_mappings:
            raise MappingError(f'没有这个分类的字段映射：{category}')
        return self._category_mappings[category]

    @property
    def category_mappings(self) -> Mapping[str, Mapping[str, Any]]:
        return self._category_mappings


def _default_probe_path(data_dir: str) -> str:
    """合并形态实测留档的路径（映射数据同级目录下的 ``merged_sql/``）。"""
    parent = os.path.dirname(os.path.abspath(data_dir))
    return os.path.join(parent, 'merged_sql', PROBE_FILE)


def _read_json(path: str) -> Dict[str, Any]:
    if not os.path.isfile(path):
        raise MappingError(f'映射文件不存在：{path}')
    try:
        with open(path, encoding='utf-8') as handle:
            return json.load(handle)
    except ValueError as exc:
        raise MappingError(f'映射文件不是合法 JSON：{path}（{exc}）')
