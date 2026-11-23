"""映射表加载与**启动期校验**。

数据在**包内** ``tpops_kafka_collector/mapping/data/*.json`` —— 采集端是独立
部署的包，运行时不读仓库根目录（设计决策 9）。

启动时校验四件事（**实现见 ``mapping/validation.py``**；规格
``specs/mapping-and-rules/spec.md``「启动期映射校验」），任一不通过**启动失败**
并打印差异：

1. **来源列存在** —— 规则要取的列存在于实测留档的**实际返回列**中；
2. **可提供指标齐全** —— 客户指标库里每个「可提供」的指标都有启用的规则；
3. **条目对应真实指标** —— 每条规则都对应客户指标库里真实存在的指标；
4. **占位符与维度绑定一一对应** —— ``placeholders`` / ``dimension_bindings``
   / 指标名里的 ``{#XXX}`` 三者一致。

**事实与声明的分工**：实测留档是事实，映射表是声明。留档（194 KB 合并形态 +
98 份逐指标留档）是**测试数据**，不随包发布；留档里的**实际返回列**抽成包内
声明 ``observed_columns.json``（由 ``tools/build_observed_columns.py`` 生成），
启动期拿它比对。

不通过时抛 :class:`~tpops_kafka_collector.errors.MappingError`（不可重试）——
映射与实际数据对不上必须人工介入改数据，重试只会把坏数据再拉一遍。
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Tuple

from ..errors import MappingError, ProtocolError
from .table import AlarmDefinition, MetricRule, build_alarm_definition
from .validation import (
    check_placeholder_bindings,
    check_providable_coverage,
    check_rule_entries,
    check_source_columns,
)

__all__ = [
    'CustomerMetric', 'LoadedMapping', 'ObservedColumns', 'PACKAGE_DATA_DIR',
    'check_placeholder_bindings', 'check_providable_coverage', 'check_rule_entries',
    'check_source_columns', 'load_mapping', 'validate_mapping',
]

LOGGER = logging.getLogger(__name__)

#: 包内映射数据目录（**不依赖仓库根**）。
PACKAGE_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

#: 逻辑名 → 包内文件名。
FILES = {
    'field_map': 'itscada_field_map.json', 'metric_rules': 'metric_rules.json',
    'alarm_rules': 'alarm_rules.json', 'kafka_field_mapping': 'kafka_field_mapping.json',
    'response_rules': 'response_rules.json', 'customer_metrics': 'customer_metric_mapping.json',
    'open_items': 'open_items.json', 'observed_columns': 'observed_columns.json',
}

@dataclass(frozen=True)
class ObservedColumns:
    """实测留档的**实际返回列**（事实基准）。

    ``merged_views`` 是合并 SQL 各视图的列，``collections`` 是诊断优化接口各
    集合的列，``instance_fields`` 是实例元数据的字段名，
    ``series_metrics`` 是监控指标接口返回的平台指标名。
    """
    merged_views: Mapping[str, FrozenSet[str]]
    collections: Mapping[str, FrozenSet[str]]
    instance_fields: FrozenSet[str]
    series_metrics: FrozenSet[str]


@dataclass(frozen=True)
class CustomerMetric:
    """客户指标库里的一条指标（可提供性判定的独立基准）。"""
    metric_code: str
    metric_name: str
    category: str
    tpops_metric: Optional[str]
    status: str
    match_level: Optional[str]


@dataclass(frozen=True)
class LoadedMapping:
    """加载好的映射数据（只读）。

    **查表能力在这一份上**（``metric_rule`` / ``alarm_definition``）——
    采集端拿到的是**这个对象**（``load_mapping()`` 的返回值），
    不是 ``mapping.table.MappingTable``（那是离线工具用的）。
    查表方法只在这里实现一次，``MappingTable`` 复用同一份实现。
    """
    field_map: Mapping[str, Any]
    metric_rules: Tuple[MetricRule, ...]
    alarm_rules: Mapping[str, Any]
    kafka_field_mapping: Mapping[str, Any]
    response_rules: Mapping[str, Any]
    customer_metrics: Tuple[CustomerMetric, ...]
    open_items: Mapping[str, Any]
    observed: ObservedColumns

    # -- 查表 -------------------------------------------------------------

    def metric_rule(self, metric_code: str) -> Optional[MetricRule]:
        """按客户指标码查规则；查不到返回 ``None``。"""
        for rule in self.metric_rules:
            if rule.metric_code == metric_code:
                return rule
        return None

    def alarm_definition(self, alarm_id: Any) -> Optional[AlarmDefinition]:
        """按 ``alarm_id`` 查告警定义；查不到返回 ``None``。

        与 ``MappingTable.alarm_definition`` **同一份实现**
        （``mapping.table.build_alarm_definition``）。
        """
        return build_alarm_definition(self.alarm_rules, alarm_id)


def load_mapping(
    data_dir: Optional[str] = None,
    validate: bool = True,
) -> LoadedMapping:
    """加载包内映射数据，默认做启动期四类校验。

    :param data_dir: 映射数据目录；缺省是包内 ``mapping/data/``。
    :param validate: 是否做启动期校验（缺省做）。
    :raises MappingError: 映射文件缺失 / 不是合法 JSON / 校验不通过。
    :raises ProtocolError: 映射文件结构与预期不符（缺关键段、类型不对）。
    """
    bundle = _load_files(data_dir or PACKAGE_DATA_DIR)
    if validate:
        validate_mapping(bundle)
    LOGGER.info('映射表加载完成：%d 条指标规则 / %d 条客户指标 / %d 条告警定义',
                len(bundle.metric_rules), len(bundle.customer_metrics),
                len(bundle.alarm_rules.get('definitions') or {}))
    return bundle


def validate_mapping(bundle: LoadedMapping) -> None:
    """跑完四类校验；有不通过的就抛 ``MappingError``（带完整差异清单）。"""
    problems: List[str] = []
    problems.extend(check_source_columns(bundle.metric_rules, bundle.observed))
    problems.extend(check_providable_coverage(bundle.metric_rules, bundle.customer_metrics))
    problems.extend(check_rule_entries(bundle.metric_rules, bundle.customer_metrics))
    problems.extend(check_placeholder_bindings(bundle.metric_rules))
    if problems:
        raise MappingError('启动期映射校验不通过：\n  ' + '\n  '.join(problems))


# -- 加载 ---------------------------------------------------------------
def _load_files(directory: str) -> LoadedMapping:
    """读包内的映射文件；结构不对就抛 ``ProtocolError``。"""
    field_map = _read_json(directory, 'field_map')
    alarm_rules = _read_json(directory, 'alarm_rules')
    kafka_field_mapping = _read_json(directory, 'kafka_field_mapping')
    response_rules = _read_json(directory, 'response_rules')
    open_items = _read_json(directory, 'open_items')
    rules_payload = _read_json(directory, 'metric_rules')
    customer_payload = _read_json(directory, 'customer_metrics')
    observed_payload = _read_json(directory, 'observed_columns')
    rules = _parse_rules(rules_payload)
    return LoadedMapping(
        field_map=field_map,
        metric_rules=rules,
        alarm_rules=alarm_rules,
        kafka_field_mapping=kafka_field_mapping,
        response_rules=response_rules,
        customer_metrics=_parse_customer_metrics(customer_payload),
        open_items=open_items,
        observed=_parse_observed(observed_payload),
    )


def _parse_rules(payload: Mapping[str, Any]) -> Tuple[MetricRule, ...]:
    items = payload.get('rules')
    if not isinstance(items, list):
        raise ProtocolError('metric_rules.json 缺少 rules 数组')
    try:
        return tuple(MetricRule.from_dict(item) for item in items)
    except (KeyError, TypeError) as exc:
        raise ProtocolError(f'metric_rules.json 的规则结构不对：{exc}')


def _parse_customer_metrics(payload: Mapping[str, Any]) -> Tuple[CustomerMetric, ...]:
    items = payload.get('items')
    if not isinstance(items, list):
        raise ProtocolError('customer_metric_mapping.json 缺少 items 数组')
    metrics: List[CustomerMetric] = []
    for item in items:
        code = item.get('customer_metric')
        if not code:
            raise ProtocolError('customer_metric_mapping.json 里有条目没有 customer_metric')
        metrics.append(CustomerMetric(
            metric_code=code,
            metric_name=item.get('customer_metric_name') or '',
            category=item.get('customer_category') or '',
            tpops_metric=item.get('tpops_metric'),
            status=item.get('status') or '',
            match_level=item.get('match_level'),
        ))
    return tuple(metrics)


def _parse_observed(payload: Mapping[str, Any]) -> ObservedColumns:
    try:
        return ObservedColumns(
            merged_views=_string_sets(payload.get('merged_views') or {}),
            collections=_string_sets(payload.get('collections') or {}),
            instance_fields=frozenset(payload.get('instance_fields') or ()),
            series_metrics=frozenset(payload.get('series_metrics') or ()),
        )
    except (AttributeError, TypeError) as exc:
        raise ProtocolError(f'observed_columns.json 的结构不对：{exc}')


def _string_sets(payload: Mapping[str, Any]) -> Dict[str, FrozenSet[str]]:
    result = {}  # type: Dict[str, FrozenSet[str]]
    for key, values in payload.items():
        result[key] = frozenset(values)
    return result


def _read_json(directory: str, name: str) -> Mapping[str, Any]:
    """读一个映射文件；缺失或不是合法 JSON 就抛 ``MappingError``。"""
    path = os.path.join(directory, FILES[name])
    if not os.path.isfile(path):
        raise MappingError(f'映射文件不存在：{path}')
    try:
        with open(path, encoding='utf-8') as handle:
            payload = json.load(handle)
    except ValueError as exc:
        raise MappingError(f'映射文件不是合法 JSON：{path}（{exc}）')
    if not isinstance(payload, dict):
        raise ProtocolError(f'映射文件顶层不是对象：{path}')
    return payload
