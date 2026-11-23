"""字段级转换规则：取值、占位符替换、资源名拼装。

规则本身在 ``data/mapping/`` 的 JSON 里，这里只负责**执行**规则。
字段级映射（原始字段 → ITSCADA 字段）见 ``data/mapping/itscada_field_map.json``。

**模块划分**（按 ``value_rule.type``，单文件都短小好读）：

======================  ==========================================
模块                     内容
======================  ==========================================
``rules_context``        上下文、异常、数值/过滤/维度工具
``rules_deltas``         累计计数器的差分基线
``rules_naming``         占位符替换、资源名拼装
``rules_sorting``        PostgreSQL ``ORDER BY`` 口径的排序
``rules_row``            ``raw`` / ``raw_filtered`` / ``aggregate`` / ``raw_rows``
``rules_series``         ``series_point`` / ``enum_map`` / ``gauge``
``rules_cross``          ``counter_delta`` / ``ratio_across``
``rules_collection``     ``top_n`` / ``collection_count`` / ``text_prefix_count`` /
                        ``instance_field``
``rules_engine``         分派表（``value_rule.type`` → 执行函数）
======================  ==========================================

**差分基线键的形状是跨模块契约**：
``{(metric_code, dimension_key): (counter_value, sampled_at_ms)}`` ——
``tpops_kafka_collector/watermark.py`` 按同一形状存取，改了就存不上。
"""

from .rules_context import MetricValue, RuleContext, RuleError
from .rules_deltas import CounterDeltaStore, ScopedCounterDeltaStore
from .rules_engine import HANDLERS, RULE_TYPES, evaluate
from .rules_naming import build_resource_name, substitute_placeholders

__all__ = [
    'CounterDeltaStore',
    'ScopedCounterDeltaStore',
    'HANDLERS',
    'MetricValue',
    'RULE_TYPES',
    'RuleContext',
    'RuleError',
    'build_resource_name',
    'evaluate',
    'substitute_placeholders',
]
