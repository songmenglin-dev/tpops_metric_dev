"""映射层：客户指标规则 → ITSCADA 字段。

本目录是**唯一的生产实现**（原 ``tools/mappings/`` 已随任务 0.7 删除，
git 历史保留；见 ``changes/tpops-kafka-collector/design.md`` 决策 2）。

两条解码路径（设计决策 3）：
- ``decode``：既有 per-view 形态（``result`` 直接是行列表）；
- ``decode_merged`` / ``merge_pages``：合并 SQL 形态（一行 N 个 ``json_agg`` 列）。

映射**数据**仍在仓库根 ``data/mapping/``（wave 2a 任务 2.1 再搬进包内）。
"""

from .table import AlarmDefinition, MappingTable, MetricRule
from .rules import (
    CounterDeltaStore,
    RuleContext,
    RuleError,
    ScopedCounterDeltaStore,
    evaluate,
)
from .nodes import resolve_node_context, select_master_node
from .message import build_alarm_message, build_metric_message
from .decode import (
    DecodeError,
    MergedContexts,
    decode,
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
    'ScopedCounterDeltaStore',
    'build_alarm_message',
    'build_metric_message',
    'decode',
    'decode_merged',
    'evaluate',
    'merge_pages',
    'resolve_node_context',
    'select_master_node',
]
