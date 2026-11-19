"""来源列校验：规则要取的列必须真实存在于**实测留档**的实际返回列里。

对应开发方案 §5.6.4「启动期校验」的第一条（每条规则的 ``source_column``
存在于对应 SQL 的实际返回列中），也是规格
``changes/tpops-kafka-collector/specs/mapping-and-rules/spec.md``
「启动期映射校验」的实现。

**权威来源是实测留档**（留档是事实，映射表是声明）：

- ``data/merged_sql/sql_merge_probe_result.json`` —— 合并形态（生产路径）的
  实际返回列；每个 ``nodes[*].result`` 解析后是**一行**，该行的键是视图名，
  值是该视图的 JSON 字符串。
- ``data/metric_response/`` —— 98 份逐指标留档，旧 per-view 形态的实测返回。

开发方案 §5.6.4 原写的 ``data/command_center/results/`` 已于 2026-11-13
删除，**不得再引用**。

本模块是**纯函数**（只比较规则与列集合，不读文件、不连平台）——
留档由调用方（生成器 / 启动加载器）读进来。
"""
import json
from typing import Any, Dict, Iterable, List, Mapping, Set, Tuple

from ..errors import MappingError

__all__ = [
    'merged_archive_view_columns',
    'required_source_columns',
    'source_column_problems',
    'validate_source_columns',
]

# 规则参数里可能指向列名的键。
_COLUMN_PARAMS = ('column', 'value_column')


def merged_archive_view_columns(
    payload: Mapping[str, Any],
) -> Dict[str, Set[str]]:
    """从合并形态的实测留档里取出每个视图的**实际返回列**。

    :param payload: ``data/merged_sql/sql_merge_probe_result.json`` 的内容；
        ``nodes[*].result`` 是平台返回的 JSON 字符串（解析后恰好一行，
        该行的键是视图名，值是该视图的 JSON 字符串）。

    多个节点的列取**并集** —— 某个节点这一轮没返回的行不代表列不存在。
    """
    columns = {}  # type: Dict[str, Set[str]]
    nodes = payload.get('nodes') or {}
    for name in sorted(nodes):
        node = nodes[name] or {}
        raw = node.get('result')
        if not raw:
            continue
        row = _single_row(raw, name)
        for view, body in row.items():
            bucket = columns.setdefault(view, set())
            for item in _rows_of_view(body, view, name):
                bucket.update(item.keys())
    return columns


def required_source_columns(
    rules: Iterable[Any],
) -> Dict[str, Set[Tuple[str, str]]]:
    """每条规则要取的列，按视图分组。

    :param rules: 规则，可以是 ``data/mapping/metric_rules.json`` 的字典，
        也可以是包内 ``MetricRule`` —— 两者都带 ``metric_code`` /
        ``source`` / ``value_rule``。

    :return: ``{视图名: {(指标名, 列名), ...}}``；只收命令中心（B 类）规则的列，
        其余通道的列来自各自的接口形态，不在这份合并留档里。
    """
    needed = {}  # type: Dict[str, Set[Tuple[str, str]]]
    for rule in rules:
        code, source, value_rule = _rule_parts(rule)
        if source.get('source') != 'command_center_sql':
            continue
        params = (value_rule.get('params') or {}) if value_rule else {}
        if value_rule and value_rule.get('type') == 'ratio_across':
            # 跨视图比率：分子分母各来自一个视图
            for side in ('numerator', 'denominator'):
                spec = params.get(side) or {}
                _add(needed, spec.get('sql_key'), code, spec.get('value_column'))
            continue
        view = source.get('sql_key')
        for name in _columns_of_params(params):
            _add(needed, view, code, name)
    return needed


def source_column_problems(
    rules: Iterable[Any],
    columns_by_view: Mapping[str, Set[str]],
) -> List[str]:
    """比较规则要的列与实测留档的实际列，返回**问题清单**（空 = 全部通过）。

    问题文本带规则名、期望的列名与实际可用列名，便于直接定位。
    """
    problems = []  # type: List[str]
    needed = required_source_columns(rules)
    for view in sorted(needed):
        available = columns_by_view.get(view)
        if available is None:
            problems.append(f'视图 {view} 不在实测留档里')
            continue
        for code, column in sorted(needed[view]):
            if column not in available:
                problems.append(
                    f'{code} 要取的列 {column} 不在实测留档视图 {view} 的实际列里'
                    f'（实际列有 {sorted(available)}）'
                )
    return problems


def validate_source_columns(
    rules: Iterable[Any],
    columns_by_view: Mapping[str, Set[str]],
) -> None:
    """来源列校验不通过 → 抛不可重试的 ``MappingError``（启动失败）。"""
    problems = source_column_problems(rules, columns_by_view)
    if problems:
        raise MappingError('来源列校验不通过：\n  ' + '\n  '.join(problems))


def _rule_parts(rule: Any) -> Tuple[str, Mapping[str, Any], Mapping[str, Any]]:
    """取出一条规则的 ``(指标名, source, value_rule)``。

    规则可以是字典（``data/mapping/metric_rules.json``），
    也可以是包内 ``MetricRule``（同名属性）。
    """
    if isinstance(rule, Mapping):
        code = rule.get('metric_code') or ''
        source = rule.get('source') or {}
        value_rule = rule.get('value_rule') or {}
    else:
        code = getattr(rule, 'metric_code', '') or ''
        source = getattr(rule, 'source', None) or {}
        value_rule = getattr(rule, 'value_rule', None) or {}
    return code, source, value_rule


def _columns_of_params(params: Mapping[str, Any]) -> List[str]:
    """一条规则的取值参数里引用到的所有列名。"""
    names = []  # type: List[str]
    for key in _COLUMN_PARAMS:
        if params.get(key):
            names.append(params[key])
    for key in (params.get('filter') or {}):
        names.append(key[:-len('_contains')] if key.endswith('_contains') else key)
    for key in (params.get('group_by') or []):
        names.append(key)
    if params.get('sort_by'):
        names.append(params['sort_by'])
    for key in (params.get('columns') or []):
        names.append(key)
    return names


def _add(
    needed: Dict[str, Set[Tuple[str, str]]],
    view: Any,
    code: str,
    column: Any,
) -> None:
    if not view or not column:
        return
    needed.setdefault(view, set()).add((code, column))


def _single_row(raw: Any, node_name: str) -> Mapping[str, Any]:
    """合并形态的 ``result`` 解析后必须恰好一行（键是视图名）。"""
    text = raw.decode('utf-8') if isinstance(raw, bytes) else raw
    try:
        payload = json.loads(text)
    except ValueError as exc:
        raise MappingError(f'留档节点 {node_name} 的 result 不是合法 JSON：{exc}')
    if not isinstance(payload, list) or len(payload) != 1:
        raise MappingError(
            f'留档节点 {node_name} 的 result 期望 1 行，实际 {len(payload or [])} 行'
        )
    return payload[0]


def _rows_of_view(body: Any, view: str, node_name: str) -> List[Mapping[str, Any]]:
    """一个视图的列值是 JSON 字符串（行列表）。"""
    if body is None:
        return []
    text = body.decode('utf-8') if isinstance(body, bytes) else body
    if not str(text).strip():
        return []
    try:
        rows = json.loads(text)
    except ValueError as exc:
        raise MappingError(
            f'留档节点 {node_name} 的视图 {view} 不是合法 JSON：{exc}'
        )
    if not isinstance(rows, list):
        raise MappingError(
            f'留档节点 {node_name} 的视图 {view} 期望行列表，'
            f'实际是 {type(rows).__name__}'
        )
    return [row for row in rows if isinstance(row, Mapping)]
