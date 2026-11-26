"""C 类（诊断优化接口）的平台层。

对应开发方案 §4.2.6、规格 ``specs/collection-channels/spec.md``
「诊断优化通道」。

**两个接口都在第二套命名空间 ``/rds/gaussdb/v3/`` 下** —— 少写一段
``gaussdb`` 平台就返回 404（不是「接口没实现」），86 个客户指标全丢。
本模块把路径前缀写死在常量里，采集器只管调用。

实测（``data/metric_response/019_…`` / ``035_…`` 留档）：

===================================  ==========================================
接口                                 请求 / 响应
===================================  ==========================================
``POST …/slow-sql-list``             请求 ``{start_time, end_time, instance_id,
                                     threshold, threshold_unit, node_ids}``；
                                     响应 ``{slow_sql_infos: [...], total}``
``POST …/top-sql-list``              请求 ``{instance_id, node_ids, start_time,
                                     end_time}``；
                                     响应 ``{top_sql_infos: [...], total}``
===================================  ==========================================

两个接口返回的都是**当前快照**，没有自己的时间戳 —— 客户字段 ``clock``
取采集完成时刻（由采集器传给规则引擎的 ``now_ms`` 决定）。

本模块**只懂 TPOPS**：不发消息、不认识客户字段、不做业务计算。
"""

from typing import Any, Dict, Mapping, Optional, Sequence, Tuple
from loguru import logger

from ..errors import MappingError, ProtocolError
from .client import GAUSSDB_V3

LOGGER = logger

__all__ = [
    'COLLECTION_PATHS',
    'DEFAULT_SLOW_SQL_THRESHOLD_MS',
    'SLOW_SQL_COLLECTION',
    'SLOW_SQL_PATH',
    'SLOW_SQL_THRESHOLD_UNIT',
    'SLOW_SQL_WINDOW_MS',
    'TOP_SQL_COLLECTION',
    'TOP_SQL_PATH',
    'TOP_SQL_WINDOW_MS',
    'build_slow_sql_body',
    'build_top_sql_body',
    'fetch_diagnostic_collection',
    'fetch_slow_sql',
    'fetch_top_sql',
    'parse_slow_sql_rows',
    'parse_top_sql_rows',
]

#: 慢 SQL 清单接口（第二套命名空间 —— 少了 ``gaussdb`` 就是 404）。
SLOW_SQL_PATH = GAUSSDB_V3 + '/instances/{instance_id}/slow-sql-list'
#: Top SQL 清单接口（同上）。
TOP_SQL_PATH = GAUSSDB_V3 + '/instances/{instance_id}/top-sql-list'

#: 慢 SQL 清单在响应体里的集合名。
SLOW_SQL_COLLECTION = 'slow_sql_infos'
#: Top SQL 清单在响应体里的集合名。
TOP_SQL_COLLECTION = 'top_sql_infos'

#: 集合名 → 接口路径模板（采集器按映射表声明的集合名取数，不写死接口）。
COLLECTION_PATHS = {
    SLOW_SQL_COLLECTION: SLOW_SQL_PATH,
    TOP_SQL_COLLECTION: TOP_SQL_PATH,
}

#: 慢 SQL 的判定阈值（毫秒）—— 实测留档用的是 1000ms。
DEFAULT_SLOW_SQL_THRESHOLD_MS = 1000
#: 阈值单位（与平台一致）。
SLOW_SQL_THRESHOLD_UNIT = 'ms'

#: 慢 SQL 清单的查询窗口（毫秒）—— 实测留档用的是 **1 天**。
SLOW_SQL_WINDOW_MS = 24 * 60 * 60 * 1000
#: Top SQL 清单的查询窗口（毫秒）—— 实测留档用的是 **7 天**。
TOP_SQL_WINDOW_MS = 7 * 24 * 60 * 60 * 1000

#: 集合名 → 查询窗口（毫秒）。
COLLECTION_WINDOWS = {
    SLOW_SQL_COLLECTION: SLOW_SQL_WINDOW_MS,
    TOP_SQL_COLLECTION: TOP_SQL_WINDOW_MS,
}


def build_slow_sql_body(
    instance_id: str,
    node_ids: Sequence[str],
    start_time: int,
    end_time: int,
    threshold_ms: int = DEFAULT_SLOW_SQL_THRESHOLD_MS,
    threshold_unit: str = SLOW_SQL_THRESHOLD_UNIT,
) -> Dict[str, Any]:
    """构造慢 SQL 清单的请求体（字段与实测留档逐字一致）。"""
    if not instance_id:
        raise MappingError('慢 SQL 清单请求缺少实例 ID')
    return {
        'start_time': int(start_time),
        'end_time': int(end_time),
        'instance_id': instance_id,
        'threshold': int(threshold_ms),
        'threshold_unit': threshold_unit,
        'node_ids': list(node_ids or ()),
    }


def build_top_sql_body(
    instance_id: str,
    node_ids: Sequence[str],
    start_time: int,
    end_time: int,
) -> Dict[str, Any]:
    """构造 Top SQL 清单的请求体（字段与实测留档逐字一致）。"""
    if not instance_id:
        raise MappingError('Top SQL 清单请求缺少实例 ID')
    return {
        'instance_id': instance_id,
        'node_ids': list(node_ids or ()),
        'start_time': int(start_time),
        'end_time': int(end_time),
    }


def parse_slow_sql_rows(
    body: Mapping[str, Any],
    endpoint: str = 'slow-sql-list',
) -> Tuple[Mapping[str, Any], ...]:
    """从慢 SQL 清单响应里取出 ``slow_sql_infos`` 集合。

    - 集合字段缺失 → **当作空集合**并记 WARNING（平台某次没给这个字段，
      不该让整轮采集炸掉）；
    - 集合字段类型不对 → 抛 :class:`ProtocolError`（结构与预期不符，
      重试只会再拿一份一样的坏数据）。
    """
    return _parse_collection(body, SLOW_SQL_COLLECTION, endpoint)


def parse_top_sql_rows(
    body: Mapping[str, Any],
    endpoint: str = 'top-sql-list',
) -> Tuple[Mapping[str, Any], ...]:
    """从 Top SQL 清单响应里取出 ``top_sql_infos`` 集合。"""
    return _parse_collection(body, TOP_SQL_COLLECTION, endpoint)


def fetch_slow_sql(
    client: Any,
    instance_id: str,
    node_ids: Sequence[str],
    start_time: int,
    end_time: int,
    threshold_ms: int = DEFAULT_SLOW_SQL_THRESHOLD_MS,
) -> Tuple[Mapping[str, Any], ...]:
    """拉慢 SQL 清单并解析成行列表。"""
    path = SLOW_SQL_PATH.format(instance_id=instance_id)
    body = client.post(path, build_slow_sql_body(
        instance_id=instance_id,
        node_ids=node_ids,
        start_time=start_time,
        end_time=end_time,
        threshold_ms=threshold_ms,
    ))
    return parse_slow_sql_rows(body, path)


def fetch_top_sql(
    client: Any,
    instance_id: str,
    node_ids: Sequence[str],
    start_time: int,
    end_time: int,
) -> Tuple[Mapping[str, Any], ...]:
    """拉 Top SQL 清单并解析成行列表。"""
    path = TOP_SQL_PATH.format(instance_id=instance_id)
    body = client.post(path, build_top_sql_body(
        instance_id=instance_id,
        node_ids=node_ids,
        start_time=start_time,
        end_time=end_time,
    ))
    return parse_top_sql_rows(body, path)


def fetch_diagnostic_collection(
    client: Any,
    collection: str,
    instance_id: str,
    node_ids: Sequence[str],
    start_time: int,
    end_time: int,
) -> Tuple[Mapping[str, Any], ...]:
    """按**集合名**取诊断优化的数据（采集器按映射表声明的集合名调用）。

    :raises MappingError: 集合名不在诊断优化接口的集合清单里
        （说明映射表声明了平台没有的集合 —— 不可重试，要人工改表）。
    """
    path = COLLECTION_PATHS.get(collection)
    if path is None:
        raise MappingError(
            f'诊断优化接口没有这个集合：{collection}（已知集合：{sorted(COLLECTION_PATHS)}）'
        )
    if collection == SLOW_SQL_COLLECTION:
        return fetch_slow_sql(client, instance_id, node_ids, start_time, end_time)
    return fetch_top_sql(client, instance_id, node_ids, start_time, end_time)


def collection_window_ms(collection: str) -> Optional[int]:
    """集合的查询窗口（毫秒）；未知集合给 ``None``。"""
    return COLLECTION_WINDOWS.get(collection)


# -- 内部实现 ------------------------------------------------------------
def _parse_collection(
    body: Mapping[str, Any],
    collection: str,
    endpoint: str,
) -> Tuple[Mapping[str, Any], ...]:
    """取一个集合并校验它是行列表。"""
    if not isinstance(body, Mapping):
        raise ProtocolError(
            f'TPOPS 响应结构异常（{endpoint}）：期望对象，实际 {type(body).__name__}'
        )
    if collection not in body:
        LOGGER.warning(
            '诊断优化接口返回体里没有集合 {}（{}）—— 本轮按空集合处理',
            collection, endpoint,
        )
        return ()
    rows = body.get(collection)
    if rows is None:
        LOGGER.warning(
            '诊断优化接口的集合 {} 是 null（{}）—— 本轮按空集合处理',
            collection, endpoint,
        )
        return ()
    if not isinstance(rows, list):
        raise ProtocolError(
            f'TPOPS 响应结构异常（{endpoint}）：集合 {collection} 期望行列表，'
            f'实际 {type(rows).__name__}'
        )
    result = []  # type: list
    for row in rows:
        if not isinstance(row, Mapping):
            raise ProtocolError(
                f'TPOPS 响应结构异常（{endpoint}）：集合 {collection} 里有非对象的行：'
                f'{type(row).__name__}'
            )
        result.append(row)
    return tuple(result)
