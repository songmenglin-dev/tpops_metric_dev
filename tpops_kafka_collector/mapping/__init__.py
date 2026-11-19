# -*- coding: utf-8 -*-
"""映射层：客户指标规则 → ITSCADA 字段。

与 ``tools/mappings/`` 同源。**生产包里的这一份才是运行时用的**；
``tools/mappings/`` 在切依赖后删除（``changes/tpops-kafka-collector/design.md`` 决策 2）。
"""

from .table import AlarmDefinition, MappingError, MappingTable, MetricRule
from .rules import CounterDeltaStore, RuleContext, RuleError, evaluate
from .nodes import resolve_node_context, select_master_node
from .message import build_alarm_message, build_metric_message

__all__ = [
    'AlarmDefinition',
    'CounterDeltaStore',
    'MappingError',
    'MappingTable',
    'MetricRule',
    'RuleContext',
    'RuleError',
    'build_alarm_message',
    'build_metric_message',
    'evaluate',
    'resolve_node_context',
    'select_master_node',
]
