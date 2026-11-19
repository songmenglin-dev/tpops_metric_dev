# -*- coding: utf-8 -*-
"""映射层：客户指标规则 → ITSCADA 字段。

与 ``tools/mappings/`` 同源。**生产包里的这一份才是运行时用的**；
``tools/mappings/`` 在切依赖后删除（``changes/tpops-kafka-collector/design.md`` 决策 2）。

两条解码路径（设计决策 3）：
- ``decode``：既有 per-view 形态（``result`` 直接是行列表）；
- ``decode_merged`` / ``merge_pages``：合并 SQL 形态（一行 N 个 ``json_agg`` 列）。
"""

from .table import AlarmDefinition, MappingTable, MetricRule
from .rules import CounterDeltaStore, RuleContext, RuleError, evaluate
from .nodes import resolve_node_context, select_master_node
from .message import build_alarm_message, build_metric_message
from .decode import (
    DecodeError,
    MergedContexts,
    decode_merged,
    merge_pages,
)
from ..errors import MappingError

__all__ = [
    'AlarmDefinition',
    'CounterDeltaStore',
    'DecodeError',
    'MappingError',
    'MappingTable',
    'MergedContexts',
    'MetricRule',
    'RuleContext',
    'RuleError',
    'build_alarm_message',
    'build_metric_message',
    'decode_merged',
    'evaluate',
    'merge_pages',
    'resolve_node_context',
    'select_master_node',
]
