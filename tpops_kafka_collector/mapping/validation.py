"""映射表的**启动期校验**（四类）。

从 ``mapping/loader.py`` 拆出来的：加载与校验是两件事 ——
「把 JSON 读进来」与「读进来的东西对不对」分开，loader 只管前者。

校验四件事（规格 ``specs/mapping-and-rules/spec.md``「启动期映射校验」），
任一不通过**启动失败**并打印差异：

1. **来源列存在** —— 规则要取的列存在于实测留档的**实际返回列**中；
2. **可提供指标齐全** —— 客户指标库里每个「可提供」的指标都有启用的规则；
3. **条目对应真实指标** —— 每条规则都对应客户指标库里真实存在的指标；
4. **占位符与维度绑定一一对应** —— ``placeholders`` / ``dimension_bindings``
   / 指标名里的 ``{#XXX}`` 三者一致。

**事实与声明的分工**：实测留档是事实，映射表是声明。留档里的**实际返回列**
抽成包内声明 ``observed_columns.json``，启动期拿它比对。

不通过时抛 :class:`~tpops_kafka_collector.errors.MappingError`（不可重试）——
映射与实际数据对不上必须人工介入改数据，重试只会把坏数据再拉一遍。
"""

import re
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Sequence

from .columns import source_column_problems
from .table import MetricRule

if TYPE_CHECKING:  # pragma: no cover - 只为类型注解，运行期不导入（避免循环）
    from .loader import CustomerMetric, ObservedColumns

__all__ = [
    'check_placeholder_bindings',
    'check_providable_coverage',
    'check_rule_entries',
    'check_source_columns',
]

#: 客户指标库里「可提供」的状态字面量。
PROVIDED_STATUS = '可提供'

#: 指标名里的占位符，形如 ``{#DATNAME}``。
PLACEHOLDER_RE = re.compile(r'\{#(\w+)\}')


# -- 校验 1：来源列存在于实测留档的实际返回列 ----------------------------
def check_source_columns(
    rules: Sequence[MetricRule],
    observed: 'ObservedColumns',
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
    merged_views: Mapping[str, Any],
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
    collections: Mapping[str, Any],
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
    instance_fields: Any,
) -> List[str]:
    """实例接口（D 类）规则引用的字段必须存在。"""
    return _named_lookup_problems(rules, 'instance', 'field', instance_fields, '实例字段')


def _series_metric_problems(
    rules: Sequence[MetricRule],
    series_metrics: Any,
) -> List[str]:
    """监控指标（A 类）规则引用的平台指标名必须存在。"""
    return _named_lookup_problems(rules, 'monitor_metrics', 'column', series_metrics, '平台指标名')


def _named_lookup_problems(
    rules: Sequence[MetricRule],
    channel: str,
    key: str,
    available: Any,
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


def _as_sets(columns: Mapping[str, Any]) -> Dict[str, set]:
    """``columns.py`` 的列校验接口要的是 ``Set``（不是 ``FrozenSet``）。"""
    return dict((view, set(columns)) for view, columns in columns.items())


# -- 校验 2 / 3：可提供指标齐全 / 条目对应真实指标 ----------------------
def check_providable_coverage(
    rules: Sequence[MetricRule],
    customer_metrics: Sequence['CustomerMetric'],
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
    customer_metrics: Sequence['CustomerMetric'],
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
