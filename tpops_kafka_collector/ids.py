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

键的**声明**（字段清单）在**包内**
``tpops_kafka_collector/mapping/data/alarm_rules.json`` 的 ``idempotency``
段里，由仓库根的 ``tools/build_itscada_mapping.py`` 从本模块的常量生成 ——
声明与实现是同一份契约的两个副本，改一处必须改另一处。

本模块同时提供**已投递键的有界集合** :class:`IdempotencyKeyLRU` ——
「键怎么算」与「哪些键已经投过了」是同一件事的两面，放在一起。
"""
from collections import OrderedDict
from typing import Any, Mapping, Optional, Tuple

__all__ = [
    'ALARM_IDEMPOTENCY_FIELDS',
    'METRIC_IDEMPOTENCY_FIELDS',
    'KEY_SEPARATOR',
    'DEFAULT_IDEMPOTENCY_LRU_CAPACITY',
    'IdempotencyKeyLRU',
    'alarm_idempotency_key',
    'metric_idempotency_key',
]

# 告警幂等键的字段（顺序即拼接顺序）。声明与实现共用这一份清单。
ALARM_IDEMPOTENCY_FIELDS = ('alarm_id', 'instance_id', 'first_alarm_time', 'status')

# 指标幂等键的字段（顺序即拼接顺序）。
METRIC_IDEMPOTENCY_FIELDS = ('instance_id', 'metric_name', 'dimensions', 'timestamp')

# 拼接分隔符。
KEY_SEPARATOR = '|'

#: 已投递幂等键集合的默认容量（specs/delivery-and-state/spec.md「幂等去重」）。
DEFAULT_IDEMPOTENCY_LRU_CAPACITY = 10000


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


class IdempotencyKeyLRU:
    """幂等键集合：容量上限可配，超限淘汰**最久未被使用**的条目。

    规格（``specs/delivery-and-state/spec.md:170``）要求的是 **LRU**：
    ``add`` 遇到已存在的键（返回 ``False``，调用方据此跳过重复投递）算一次
    「使用」，会刷新该键的新鲜度；``contains`` 是**查询**，不刷新。
    """

    def __init__(self, capacity: int = DEFAULT_IDEMPOTENCY_LRU_CAPACITY) -> None:
        if capacity <= 0:
            raise ValueError(f'幂等键 LRU 容量必须为正整数，实际 {capacity!r}')
        self._capacity = capacity
        self._keys = OrderedDict()  # type: OrderedDict

    @property
    def capacity(self) -> int:
        return self._capacity

    def contains(self, key: str) -> bool:
        """查询键是否存在。**不刷新**新鲜度 —— 查询不是「使用」。"""
        return key in self._keys

    def add(self, key: str) -> bool:
        """加入幂等键；已存在返回 ``False``（调用方据此跳过投递）。

        重复键是一次「使用」—— 刷新它的新鲜度，这样持续重复投递的告警不会
        因为「首次出现得早」而被当成最老条目淘汰掉。
        """
        if key in self._keys:
            self._keys.move_to_end(key)
            return False
        self._keys[key] = None
        while len(self._keys) > self._capacity:
            self._keys.popitem(last=False)
        return True

    def keys(self) -> Tuple[str, ...]:
        """按新鲜度返回全部键（最久未使用 → 最新使用）。"""
        return tuple(self._keys.keys())

    def __len__(self) -> int:
        return len(self._keys)
