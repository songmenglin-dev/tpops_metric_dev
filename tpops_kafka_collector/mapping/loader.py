"""映射表加载与**启动期校验**。

数据在**包内** ``tpops_kafka_collector/mapping/data/*.json`` —— 采集端是独立
部署的包，运行时不读仓库根目录（设计决策 9）。

启动时校验四件事（规格 ``specs/mapping-and-rules/spec.md``「启动期映射校验」），
任一不通过**启动失败**并打印差异：

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
import re
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

from ..errors import MappingError, ProtocolError
from .columns import source_column_problems
from .table import MetricRule

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

#: 客户指标库里「可提供」的状态字面量。
PROVIDED_STATUS = '可提供'

#: 指标名里的占位符，形如 ``{#DATNAME}``。
PLACEHOLDER_RE = re.compile(r'\{#(\w+)\}')


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
    """加载好的映射数据（只读）。"""
    field_map: Mapping[str, Any]
    metric_rules: Tuple[MetricRule, ...]
    alarm_rules: Mapping[str, Any]
    kafka_field_mapping: Mapping[str, Any]
    response_rules: Mapping[str, Any]
    customer_metrics: Tuple[CustomerMetric, ...]
    open_items: Mapping[str, Any]
    observed: ObservedColumns


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
    problems = []  # type: List[str]
    problems.extend(check_source_columns(bundle.metric_rules, bundle.observed))
    problems.extend(check_providable_coverage(bundle.metric_rules, bundle.customer_metrics))
    problems.extend(check_rule_entries(bundle.metric_rules, bundle.customer_metrics))
    problems.extend(check_placeholder_bindings(bundle.metric_rules))
    if problems:
        raise MappingError('启动期映射校验不通过：\n  ' + '\n  '.join(problems))


# -- 校验 1：来源列存在于实测留档的实际返回列 ----------------------------
def check_source_columns(
    rules: Sequence[MetricRule],
    observed: ObservedColumns,
) -> List[str]:
    """规则要取的列必须在实测留档的实际返回列里。

    命令中心（B 类）走 ``mapping/columns.py`` 的列校验（同一份实现，不重写）；
    其余三个通道各查各的观测面：诊断优化查集合的列、实例接口查实例字段、
    监控指标查平台指标名。
    """
    problems = list(source_column_problems(rules, _as_sets(observed.merged_views)))
    problems.extend(_dimension_column_problems(rules, observed.merged_views))
    problems.extend(_collection_column_problems(rules, observed.collections))
    problems.extend(_instance_field_problems(rules, observed.instance_fields))
    problems.extend(_series_metric_problems(rules, observed.series_metrics))
    return problems


def _dimension_column_problems(
    rules: Sequence[MetricRule],
    merged_views: Mapping[str, FrozenSet[str]],
) -> List[str]:
    """维度绑定引用的列也要真实存在。

    ``mapping/columns.py`` 覆盖的是 ``value_rule.params`` 里的列；
    ``dimension_bindings``（占位符 → 列）是另一处列引用，单独校。
    """
    problems = []  # type: List[str]
    for rule in rules:
        view = rule.source.get('sql_key')
        if not view:
            continue
        available = merged_views.get(view)
        if available is None:
            continue
        for placeholder, column in sorted((rule.dimension_bindings or {}).items()):
            if column not in available:
                problems.append(
                    f'{rule.metric_code} 的维度绑定 {placeholder}→{column} '
                    f'引用了不在实测留档视图 {view} 实际列里的列'
                    f'（实际列有 {sorted(available)}）'
                )
    return problems


def _collection_column_problems(
    rules: Sequence[MetricRule],
    collections: Mapping[str, FrozenSet[str]],
) -> List[str]:
    """诊断优化（C 类）规则引用的集合列必须存在。"""
    problems = []  # type: List[str]
    for rule in rules:
        if rule.source.get('source') != 'diagnostic':
            continue
        params = (rule.value_rule or {}).get('params') or {}
        name = params.get('collection')
        available = collections.get(name)
        if available is None:
            problems.append(
                f'{rule.metric_code} 引用的集合 {name} 不在实测留档里'
            )
            continue
        for column in _params_columns(params):
            if column not in available:
                problems.append(
                    f'{rule.metric_code} 要取的列 {column} '
                    f'不在实测留档集合 {name} 的实际列里（实际列有 {sorted(available)}）'
                )
    return problems


def _params_columns(params: Mapping[str, Any]) -> List[str]:
    """取值参数里引用到的列名。"""
    names = []  # type: List[str]
    for key in ('column', 'value_column', 'field', 'sort_by'):
        if params.get(key):
            names.append(params[key])
    for key in (params.get('filter') or {}):
        names.append(key[:-len('_contains')] if key.endswith('_contains') else key)
    for key in (params.get('group_by') or []):
        names.append(key)
    return names


def _instance_field_problems(
    rules: Sequence[MetricRule],
    instance_fields: FrozenSet[str],
) -> List[str]:
    """实例接口（D 类）规则引用的字段必须存在。"""
    return _named_lookup_problems(rules, 'instance', 'field', instance_fields, '实例字段')


def _series_metric_problems(
    rules: Sequence[MetricRule],
    series_metrics: FrozenSet[str],
) -> List[str]:
    """监控指标（A 类）规则引用的平台指标名必须存在。"""
    return _named_lookup_problems(rules, 'monitor_metrics', 'column', series_metrics, '监控指标')


def _named_lookup_problems(
    rules: Sequence[MetricRule],
    channel: str,
    key: str,
    available: FrozenSet[str],
    label: str,
) -> List[str]:
    """按通道查「规则引用的名字」是否真的在实测留档里。

    实例接口引的是**字段名**（``params.field``），监控指标接口引的是
    **平台指标名**（``params.column``）—— 都是「按名字查」，共用这一份。
    """
    problems = []  # type: List[str]
    for rule in rules:
        if rule.source.get('source') != channel:
            continue
        name = ((rule.value_rule or {}).get('params') or {}).get(key)
        if name and name not in available:
            problems.append(
                f'{rule.metric_code} 要取的{label} {name} 不在实测留档里'
                f'（实际{label}有 {sorted(available)}）'
            )
    return problems


def _as_sets(columns: Mapping[str, FrozenSet[str]]) -> Dict[str, set]:
    """``columns.py`` 的列校验接口要的是 ``Set``（不是 ``FrozenSet``）。"""
    return dict((view, set(columns)) for view, columns in columns.items())


# -- 校验 2 / 3：可提供指标齐全 / 条目对应真实指标 ----------------------
def check_providable_coverage(
    rules: Sequence[MetricRule],
    customer_metrics: Sequence[CustomerMetric],
) -> List[str]:
    """客户指标库里每个「可提供」的指标都要有启用的规则；反之亦然。"""
    provided = set(
        metric.metric_code for metric in customer_metrics
        if metric.status == PROVIDED_STATUS
    )
    enabled = set(rule.metric_code for rule in rules if rule.enabled)
    problems = []  # type: List[str]
    for code in sorted(provided - enabled):
        problems.append(f'可提供指标 {code} 在映射表里没有启用的规则')
    for code in sorted(enabled - provided):
        problems.append(f'映射表里 {code} 是启用的，但客户指标库判定它不是可提供')
    return problems


def check_rule_entries(
    rules: Sequence[MetricRule],
    customer_metrics: Sequence[CustomerMetric],
) -> List[str]:
    """每条规则都要对应客户指标库里真实存在的指标。"""
    known = set(metric.metric_code for metric in customer_metrics)
    return [
        f'映射表里的条目 {rule.metric_code} 在客户指标库里不存在'
        for rule in rules
        if rule.metric_code not in known
    ]


# -- 校验 4：占位符与维度绑定一一对应 -----------------------------------
def check_placeholder_bindings(rules: Sequence[MetricRule]) -> List[str]:
    """``placeholders`` / ``dimension_bindings`` / 指标名里的 ``{#XXX}`` 三者一致。"""
    problems = []  # type: List[str]
    for rule in rules:
        declared = set(rule.placeholders or ())
        bound = set((rule.dimension_bindings or {}).keys())
        in_name = set(PLACEHOLDER_RE.findall(rule.metric_code or ''))
        if declared != bound:
            problems.append(
                f'{rule.metric_code} 的占位符 {sorted(declared)} '
                f'与维度绑定 {sorted(bound)} 不是一一对应'
            )
        if declared != in_name:
            problems.append(
                f'{rule.metric_code} 的占位符 {sorted(declared)} '
                f'与指标名里的占位符 {sorted(in_name)} 不一致'
            )
    return problems


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
    metrics = []  # type: List[CustomerMetric]
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
