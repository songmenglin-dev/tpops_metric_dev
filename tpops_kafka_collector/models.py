"""跨模块 DTO（不可变）。

采集端在模块之间只传这三种对象：**指标点**、**告警明细行**、**告警条目**。

两条设计约束（CLAUDE.md「不可变数据」+ 规格「结构化数据不可变」）：

- **frozen dataclass** —— 创建后不可改，跨模块传递不会被人就地改坏。
- **维度用排序后的元组存**，不用 dict —— frozen dataclass 会生成 ``__hash__``，
  里面若有 dict 字段则哈希失败，``MetricPoint`` 就进不了集合、不能当字典键。
  对外用 ``dimension_map`` 属性取 dict 视图。

**幂等键不在这里另写一份实现** —— 它由 ``tpops_kafka_collector.ids`` 计算，
本模块的 DTO 只做转发。键的**声明**（字段清单）在**包内**
``tpops_kafka_collector/mapping/data/alarm_rules.json`` 的 ``idempotency`` 段，
由仓库根的 ``tools/build_itscada_mapping.py`` 从 ``ids.py`` 的常量生成 ——
声明与实现同源，改一处必须改另一处。
"""
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Union

from .ids import alarm_idempotency_key, metric_idempotency_key

__all__ = ['AlarmDetail', 'AlarmRecord', 'MetricPoint', 'ValueType']

# 客户指标表【数据类型】对应的取值类型：整数 / 浮点 / 字符 / 文本 / 缺失(None)。
ValueType = Union[int, float, str, None]


@dataclass(frozen=True)
class MetricPoint:
    """一个客户指标在一个维度上的一个数据点。

    字段与开发方案 §5.3 一致；``node_id`` / ``host_ip`` / ``component_id``
    是**追加**的 —— 资源身份（resourceName / tags）要靠它们，
    少一个就得在下游重新解析一遍节点。

    :param customer_metric: 客户指标英文名（原样保留 OCR 错字）
    :param customer_metric_name: 中文名
    :param customer_category: 13 类之一
    :param dimensions: 业务维度；可以是 dict 或 ``(键, 值)`` 序列，
        存下来时归一化成**按键排序的元组**（保证可哈希且与插入顺序无关）
    :param value: 指标值；缺失为 ``None``（**不填 0** —— 缺值不是 0）
    :param value_type: 客户指标表【数据类型】：整数 / 浮点 / 字符 / 文本
    :param unit: 单位；缺省 ``None``（不填空串）
    :param timestamp: 数据时间（毫秒）
    :param collect_time: 采集完成时刻（毫秒）
    :param source: 数据来源通道（monitor_metrics / command_center_sql / …）
    """

    customer_metric: str
    customer_metric_name: str
    customer_category: str
    dimensions: Tuple[Tuple[str, str], ...] = ()
    value: ValueType = None
    value_type: str = ''
    unit: Optional[str] = None
    timestamp: int = 0
    collect_time: int = 0
    source: str = ''
    instance_id: str = ''
    instance_name: str = ''
    node_id: Optional[str] = None
    host_ip: Optional[str] = None
    component_id: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, 'dimensions', _normalize_dimensions(self.dimensions))

    @property
    def dimension_map(self) -> Dict[str, str]:
        """维度键值对的 dict 视图（每次新建，改它不影响 DTO）。"""
        return dict(self.dimensions)

    @property
    def idempotency_key(self) -> str:
        """幂等键 = ``instance_id + metric_name + 排序后的 dimensions + timestamp``。

        由 ``ids.metric_idempotency_key`` 计算 —— **不在这里另写一份拼法**。
        """
        return metric_idempotency_key(
            self.instance_id,
            self.customer_metric,
            self.dimension_map,
            self.timestamp,
        )


@dataclass(frozen=True)
class AlarmDetail:
    """告警明细行（逐节点一条）。

    收敛条目（``AlarmRecord``）的节点是靠关联明细行回填的，
    因此明细行**不参与**告警幂等键。
    """

    node_id: str = ''
    host_ip: str = ''
    description: str = ''
    cause: str = ''
    impaction: str = ''
    possible_causes: str = ''
    processing_steps: str = ''
    alarm_detail_raise_count: int = 0


@dataclass(frozen=True)
class AlarmRecord:
    """告警条目（收敛条目 = 实例 + alarm_id 一条）。

    幂等键 = ``(alarm_id, instance_id, first_alarm_time, status)``：
    ``status`` 用**平台原值**（``activated`` / ``recovered`` / ``cleared`` /
    ``cleared_by_user``），**不含** ``node_id`` 与 ``last_alarm_time``
    （理由见 ``tpops_kafka_collector.ids`` 的模块文档）。

    :param details: 明细行；恒为元组（可为空元组，不省略字段）
    """

    alarm_id: str
    alarm_name: str = ''
    alarm_type: str = ''
    alarm_category: str = ''
    severity: str = ''
    status: str = ''
    instance_id: str = ''
    instance_name: str = ''
    first_alarm_time: int = 0
    last_alarm_time: int = 0
    clear_alarm_time: Optional[int] = None
    alarm_duration: str = ''
    alarm_items_sum: int = 0
    host_ip: Optional[str] = None
    node_id: Optional[str] = None
    description: Optional[str] = None
    impaction: Optional[str] = None
    possible_causes: Optional[str] = None
    processing_steps: Optional[str] = None
    alarm_detail_raise_count: int = 0
    notify_status: Optional[str] = None
    automatic_clear: Optional[bool] = None
    details: Tuple[AlarmDetail, ...] = ()
    engine: Optional[str] = None
    component_name: Optional[str] = None
    cause: Optional[str] = None
    alarm_remarks: Optional[str] = None
    resource_id: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, 'details', tuple(self.details or ()))

    @property
    def idempotency_key(self) -> str:
        """告警幂等键 —— 由 ``ids.alarm_idempotency_key`` 计算。"""
        return alarm_idempotency_key(
            self.alarm_id,
            self.instance_id,
            self.first_alarm_time,
            self.status,
        )


def _normalize_dimensions(
    dimensions: Optional[Union[Mapping[str, Any], Iterable[Any]]],
) -> Tuple[Tuple[str, str], ...]:
    """把维度归一化成**按键排序**的 ``((键, 值), ...)`` 元组。

    接受 dict 或 ``(键, 值)`` 序列；值统一转字符串（维度是业务标识，
    不是数值）。``None`` → 空元组。
    """
    if dimensions is None:
        return ()
    if isinstance(dimensions, Mapping):
        items = dimensions.items()
    else:
        items = dimensions
    pairs = [(str(key), '' if value is None else str(value)) for key, value in items]
    return tuple(sorted(pairs))
