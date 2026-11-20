"""唯一 JSON 序列化出口。

**全仓只有这一处 ``json.dumps`` 产出 ITSCADA 消息**（生产代码内）。

口径来源（冻结，不可改）：
- 信封字段集与顺序 → ``data/mapping/itscada_field_map.json``
  （指标 7 字段 / 告警 13 字段；``tests/unit/test_serializer.py`` 逐字段比对，
  映射数据搬进包内后仍以那份 JSON 为准）；
- 值类型 → 客户指标表【数据类型】（整数 / 浮点 / 字符 / 文本）；
- 缺省 → 缺失值一律 ``null``，不填 0、不填空串；时间是 13 位毫秒时间戳。

与映射层的关系：``mapping/message.py`` 负责**组装**信封（字段取什么值），
本模块负责**定型与落地**（字段顺序、缺省、类型、JSON 编码）。
值转换的口径两边必须一致 —— ``mapping.message.serialize_value`` 与本模块的
``serialize_value`` 对同一组输入给出同一结果，由
``tests/unit/test_serializer.py::test_value_serialization_matches_mapping_layer`` 守住。
"""

import json
import logging
import math
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

LOGGER = logging.getLogger(__name__)

# -- 信封（顺序即输出顺序）----------------------------------------------
METRIC_FIELDS: Tuple[str, ...] = (
    'metricName',
    'metricCode',
    'resourceName',
    'value',
    'unit',
    'clock',
    'tags',
)

ALARM_FIELDS: Tuple[str, ...] = (
    'alarmId',
    'alarmName',
    'alarmContent',
    'priority',
    'alarmTime',
    'status',
    'recoverTime',
    'resourceName',
    'metricName',
    'metricCode',
    'value',
    'unit',
    'tags',
)

METRIC_TAG_FIELDS: Tuple[str, ...] = (
    'instance_id',
    'instance_name',
    'node_id',
    'component_id',
    'host_ip',
    'dimension_values',
)

ALARM_TAG_FIELDS: Tuple[str, ...] = (
    'instance_id',
    'instance_name',
    'node_id',
    'host_ip',
    'alarm_category',
    'alarm_type',
    'engine',
    'alarm_duration',
    'details',
)

# -- 客户指标表【数据类型】----------------------------------------------
INTEGER_TYPE = '整数'
FLOAT_TYPE = '浮点'
CHAR_TYPE = '字符'
TEXT_TYPE = '文本'

# 告警的 value 一律按「字符」落地（与 mapping/message.py 一致）
ALARM_VALUE_TYPE = CHAR_TYPE

# 秒与毫秒的分界：小于它的整数时间戳按秒处理（乘 1000）。
# 现在是毫秒 ~1.8e12、秒 ~1.8e9；1e11 ms 是 1973 年，1e11 s 是 5138 年 ——
# 落在两者之间的整数不会是真实采集时刻。
_SECONDS_THRESHOLD = 10 ** 11

# 时间字段（毫秒）
_TIME_FIELDS = frozenset(('clock', 'alarmTime', 'recoverTime'))


class SerializerError(ValueError):
    """消息无法按客户字段规范序列化。

    不可重试 —— 同样的输入再来一次还是同样的结果，要人工介入改代码。
    """


def serialize_metric(
    message: Mapping[str, Any],
    value_type: Optional[str] = None,
) -> str:
    """把一条指标消息序列化成 JSON 文本（唯一序列化出口）。

    :param message: 指标消息（字段与 ``itscada_field_map.json`` 的
        ``metric_message`` 一致），由映射层组装。
    :param value_type: 客户指标表【数据类型】。传了就按它定型 ``value``；
        不传则 ``value`` 原样落地（映射层已按规则定型过时用这个）。
    """
    payload = _ordered_payload(message, METRIC_FIELDS, 'metric')
    payload['value'] = apply_value_type(payload['value'], value_type)
    return dumps(payload)


def serialize_alarm(message: Mapping[str, Any]) -> str:
    """把一条告警消息序列化成 JSON 文本（唯一序列化出口）。"""
    payload = _ordered_payload(message, ALARM_FIELDS, 'alarm')
    payload['value'] = apply_value_type(payload['value'], ALARM_VALUE_TYPE)
    return dumps(payload)


def apply_value_type(value: Any, value_type: Optional[str] = None) -> Any:
    """按客户指标表【数据类型】序列化取值。

    与 ``mapping.message.serialize_value`` **口径一致**（同一组输入同一结果）：

    命名刻意与映射层的 ``serialize_value`` 不同：本函数允许
    ``value_type=None`` 表示「已定型，原样落地」（映射层那个必须给类型）。

    - 缺失（``None``）→ ``None``（输出成 ``null``），不填 0；
    - 「整数」→ ``int(float(value))``，取不到 → ``None``；
    - 「浮点」→ ``round(float(value), 2)``，取不到 → ``None``；
    - 列表/元组 → 列表；
    - 其余（「字符」/「文本」）→ 字符串。
    """
    if value_type is None:
        return value
    if value is None:
        return None
    if value_type == INTEGER_TYPE:
        return _as_int(value)
    if value_type == FLOAT_TYPE:
        return _as_float(value)
    if isinstance(value, (list, tuple)):
        return list(value)
    return value if isinstance(value, str) else str(value)


def to_epoch_millis(value: Any) -> Optional[int]:
    """时间统一成 13 位毫秒时间戳。

    平台把「没有采集时刻」记成 ``0``，那不是真实时刻 → ``None``（输出 ``null``）。
    秒级时间戳按 1000 倍换算成毫秒（客户侧只要毫秒）。
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            value = float(value)
        except ValueError:
            LOGGER.warning('时间戳不是数字，按缺失处理：%r', value)
            return None
    if not isinstance(value, (int, float)):
        LOGGER.warning('时间戳类型不认识，按缺失处理：%r', value)
        return None
    if isinstance(value, float) and not math.isfinite(value):
        LOGGER.warning('时间戳不是有限数，按缺失处理：%r', value)
        return None
    if value <= 0:
        return None
    if value < _SECONDS_THRESHOLD:
        return int(round(value * 1000))
    return int(value)


def dumps(payload: Mapping[str, Any]) -> str:
    """整个生产代码里**唯一**产出消息的 ``json.dumps``。

    ``ensure_ascii=False`` —— 中文直接落地，不转成 ``\\uXXXX``；
    ``sort_keys=False`` —— 字段顺序由信封定义决定，不按字典序重排；
    ``allow_nan=False`` —— NaN / Infinity 不是合法 JSON，宁可报错也不发出去。
    """
    try:
        return json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=False,
            allow_nan=False,
            separators=(',', ':'),
        )
    except (TypeError, ValueError) as exc:
        raise SerializerError(f'消息无法序列化为 JSON：{exc}')


# -- 内部实现 ------------------------------------------------------------
def _ordered_payload(
    message: Mapping[str, Any],
    fields: Sequence[str],
    kind: str,
) -> Dict[str, Any]:
    """按信封顺序取出字段；缺失 → ``None``，多余字段丢弃并记 WARNING。"""
    if not isinstance(message, Mapping):
        raise SerializerError(
            f'{kind} 消息必须是映射，实际是 {type(message).__name__}'
        )
    payload: Dict[str, Any] = {}
    for name in fields:
        value = message.get(name)
        if name in _TIME_FIELDS:
            value = to_epoch_millis(value)
        elif name == 'tags':
            value = _normalize_tags(value, kind)
        elif isinstance(value, str) and not value:
            value = None
        payload[name] = value
    extra = sorted(set(message) - set(fields))
    if extra:
        LOGGER.warning('%s 消息含信封外字段，已丢弃：%s', kind, extra)
    return payload


def _normalize_tags(tags: Any, kind: str) -> Dict[str, Any]:
    """标签按信封的键与顺序落地；缺失 → ``None``，多余键丢弃并记 WARNING。"""
    if tags is None:
        tags = {}
    if not isinstance(tags, Mapping):
        raise SerializerError(
            f'{kind} 消息的 tags 必须是映射，实际是 {type(tags).__name__}'
        )
    fields = METRIC_TAG_FIELDS if kind == 'metric' else ALARM_TAG_FIELDS
    result: Dict[str, Any] = {}
    for name in fields:
        value = tags.get(name)
        if name == 'dimension_values':
            value = _dimension_values(value, kind)
        elif name == 'details':
            value = [] if value is None else list(value)
        result[name] = value
    extra = sorted(set(tags) - set(fields))
    if extra:
        LOGGER.warning('%s 消息的 tags 含信封外键，已丢弃：%s', kind, extra)
    return result


def _dimension_values(value: Any, kind: str) -> Dict[str, Any]:
    """业务维度落 ``tags.dimension_values``；无维度 → 空对象。"""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise SerializerError(
            f'{kind} 消息的 dimension_values 必须是映射，实际是 {type(value).__name__}'
        )
    return dict(value)


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> Optional[float]:
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None
