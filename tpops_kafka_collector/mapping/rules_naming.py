"""占位符替换与资源名拼装。

- 占位符 ``{#NAME}`` 出现在客户指标的 ``metricName`` / ``metricCode`` 里，
  替换成维度实际值（ITSCADA 文档 §7 的做法）；
- ``resourceName`` 只能表示**一个节点**，模板是 ``{host_ip}_{node_id}``，
  取不到值时按回退模板逐个试。
"""
import re
from typing import Any, Mapping

from .rules_context import RuleError

PLACEHOLDER_RE = re.compile(r'\{#(\w+)\}')
RESOURCE_FIELD_RE = re.compile(r'\{(\w+)\}')


def substitute_placeholders(template: str, dimensions: Mapping[str, Any]) -> str:
    """把 ``{#DATNAME}`` 替换成维度实际值。

    维度里没有这个占位符就抛 ``RuleError`` —— 拼不出客户要的指标名，
    不能悄悄留个 ``{#DATNAME}`` 出去。
    """

    def _replace(match: 're.Match') -> str:
        name = match.group(1)
        if name not in dimensions:
            raise RuleError(f'占位符 {{{name}}} 没有对应的维度值')
        value = dimensions[name]
        return '' if value is None else str(value)

    return PLACEHOLDER_RE.sub(_replace, template or '')


def build_resource_name(
    template: str,
    context: Mapping[str, Any],
    fallback_template: Any = None,
) -> str:
    """按模板拼资源名。

    模板里的占位符取不到值时（None 或空串），改用回退模板；
    回退模板可以给一串，按顺序取第一个渲染出非空结果的；
    没有回退模板时留空串占位。
    """
    missing = [False]

    def _replace(match: 're.Match') -> str:
        value = context.get(match.group(1))
        if value is None or value == '':
            missing[0] = True
            return ''
        return str(value)

    rendered = RESOURCE_FIELD_RE.sub(_replace, template or '')
    if missing[0] and fallback_template:
        fallbacks = (fallback_template,) if isinstance(fallback_template, str) \
            else tuple(fallback_template)
        for candidate in fallbacks:
            attempt = build_resource_name(candidate, context)
            if attempt:
                return attempt
        return ''
    return rendered
