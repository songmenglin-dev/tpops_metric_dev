"""幂等键的计算（采集端唯一实现）。

设计决策 5（``changes/tpops-kafka-collector/design.md``）：

- 告警键 = ``(alarm_id, instance_id, first_alarm_time, status)``；
  ``status`` 用**平台原值**（``activated`` / ``recovered`` / ``cleared`` /
  ``cleared_by_user``），**不是** ITSCADA 整数枚举 —— 枚举会把
  ``recovered`` 与 ``cleared`` 压成同一个键。
- **不含** ``node_id``：收敛条目的节点靠关联明细行回填，回填目标可能逐轮不同
  → 进键会让同一条告警因回填漂移而重复投递。
- **不含** ``last_alarm_time``：恢复后它**不变**（实测 last ≠ clear），
  加它救不了「恢复事件被去重丢弃」的问题。
- 指标键 = ``instance_id + metric_name + 排序后的 dimensions + timestamp``。

键的**声明**（字段清单）在 ``data/mapping/alarm_rules.json`` 的 ``idempotency``
段里，由 ``tools/build_itscada_mapping.py`` 从本模块的常量生成 ——
声明与实现是同一份契约的两个副本，改一处必须改另一处。
"""
from typing import Any, Mapping, Optional

__all__ = [
    'ALARM_IDEMPOTENCY_FIELDS',
    'METRIC_IDEMPOTENCY_FIELDS',
    'KEY_SEPARATOR',
    'alarm_idempotency_key',
    'metric_idempotency_key',
]

# 告警幂等键的字段（顺序即拼接顺序）。声明与实现共用这一份清单。
ALARM_IDEMPOTENCY_FIELDS = ('alarm_id', 'instance_id', 'first_alarm_time', 'status')

# 指标幂等键的字段（顺序即拼接顺序）。
METRIC_IDEMPOTENCY_FIELDS = ('instance_id', 'metric_name', 'dimensions', 'timestamp')

# 拼接分隔符。
KEY_SEPARATOR = '|'


def alarm_idempotency_key(
    alarm_id: Any,
    instance_id: Any,
    first_alarm_time: Any,
    status: Any,
) -> str:
    """告警幂等键。

    :param alarm_id: 平台告警 ID
    :param instance_id: 实例 ID
    :param first_alarm_time: 首次告警时间（毫秒）
    :param status: **平台原值**状态（``activated`` / ``recovered`` /
        ``cleared`` / ``cleared_by_user``）—— 不要传 ITSCADA 整数枚举，
        枚举会把 ``recovered`` 与 ``cleared`` 压成同一个键。

    同一条告警的触发与恢复得到**不同的**键，恢复事件不会被去重丢弃；
    状态不变时重复采集得到**相同的**键。
    """
    parts = (alarm_id, instance_id, first_alarm_time, status)
    return KEY_SEPARATOR.join(_part(value) for value in parts)


def metric_idempotency_key(
    instance_id: Any,
    metric_name: Any,
    dimensions: Optional[Mapping[str, Any]] = None,
    timestamp: Any = None,
) -> str:
    """指标幂等键：``instance_id + metric_name + 排序后的 dimensions + timestamp``。

    ``dimensions`` 按键排序后拼接，保证同一组维度无论插入顺序如何都得到同一个键。
    """
    dim_text = ','.join(
        f'{key}={dimensions[key]}'
        for key in sorted(dimensions or {})
    )
    parts = (instance_id, metric_name, dim_text, timestamp)
    return KEY_SEPARATOR.join(_part(value) for value in parts)


def _part(value: Any) -> str:
    """键的单个片段：``None`` 折成空串，其余转字符串。"""
    if value is None:
        return ''
    return str(value)
