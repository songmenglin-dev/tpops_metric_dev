"""E 类：告警记录接口（平台层）。

对应规格 ``specs/collection-channels/spec.md`` 的「告警通道的分页与归并」，
开发方案 §4.2.7。

平台实测的坑（**不要凭直觉改**）：

1. ``is_active`` 与 ``is_convergence`` **必传** —— 少一个平台按默认值查，
   拿到的是错的形态（收敛列表 vs 明细行）。
2. ``limit`` 上限 **100** —— 超出平台报错；``total_count`` 会大于 100，
   **只取第一页就是静默丢数据**（本模块按 ``offset`` 翻页取全）。
3. 平台侧的 ``alarm_id`` / ``instance_id`` 过滤**只在明细形态
   （``is_convergence = false``）下有效**；收敛形态下平台会忽略这两个条件。
   所以收敛形态的请求体里**不传**这两个字段，实例与告警的归并改在本地做
   （见 ``collectors/alarm.py``）。
4. **历史告警仅保留 30 天** —— 停机超 30 天会丢告警。

本模块只做「发请求 + 翻页 + 结构校验」，不组装消息（那是映射层与采集器的职责）。
"""
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple
from loguru import logger

from ..errors import TpopsError
from .client import RDS_V3
from .protocol import require_fields

LOGGER = logger

__all__ = [
    'ALARM_RECORDS_PATH',
    'ALARM_STATISTIC_PATH',
    'DEFAULT_LIMIT',
    'MAX_LIMIT',
    'MAX_PAGES',
    'AlarmPage',
    'AlarmQueryError',
    'build_query_body',
    'fetch_alarm_page',
    'fetch_all_alarm_records',
]

#: 告警结果列表接口（第一套命名空间 ``/rds/v3``）。
ALARM_RECORDS_PATH = RDS_V3 + '/alarm/records'
#: 告警数量统计接口（统计口径，采集端不用）。
ALARM_STATISTIC_PATH = RDS_V3 + '/alarm/records/statistic'

#: 平台 ``limit`` 上限（实测：最大 100）。
MAX_LIMIT = 100
#: 默认页容量 —— 取上限，少发请求。
DEFAULT_LIMIT = 100
#: 翻页上限（防御性上限，避免平台 ``total_count`` 异常时死循环）。
MAX_PAGES = 1000


class AlarmQueryError(TpopsError):
    """告警查询参数不合法 —— 不可重试（改代码才能解决）。"""


@dataclass(frozen=True)
class AlarmPage:
    """告警结果列表的一页。

    :param records: 本页的记录（收敛条目或明细行）
    :param total_count: 平台给出的总记录数（**可能大于一页**）
    """

    records: Tuple[Mapping[str, Any], ...]
    total_count: int


def build_query_body(
    is_active: bool,
    is_convergence: bool,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    alarm_id: Optional[str] = None,
    instance_id: Optional[str] = None,
) -> Dict[str, Any]:
    """构造 ``POST /rds/v3/alarm/records`` 的请求体。

    ``is_active`` / ``is_convergence`` 恒传（平台要求必传）。
    ``alarm_id`` / ``instance_id`` **只在明细形态下**才带上 ——
    收敛形态下平台忽略这两个过滤条件，带上等于假装过滤生效。

    :raises AlarmQueryError: ``limit`` 越界 / ``offset`` 为负。
    """
    if limit < 1 or limit > MAX_LIMIT:
        raise AlarmQueryError(
            f'告警查询 limit 必须在 1~{MAX_LIMIT} 之间，实际 {limit}'
        )
    if offset < 0:
        raise AlarmQueryError(f'告警查询 offset 不能为负，实际 {offset}')
    body = {
        'is_active': bool(is_active),
        'is_convergence': bool(is_convergence),
        'limit': int(limit),
        'offset': int(offset),
    }
    if is_convergence:
        for name, value in (('alarm_id', alarm_id), ('instance_id', instance_id)):
            if value:
                LOGGER.warning(
                    '收敛形态下平台侧 {} 过滤无效，已忽略（改由本地归并）：{}',
                    name, value,
                )
    else:
        if alarm_id:
            body['alarm_id'] = alarm_id
        if instance_id:
            body['instance_id'] = instance_id
    return body


def fetch_alarm_page(
    client: Any,
    is_active: bool,
    is_convergence: bool,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    alarm_id: Optional[str] = None,
    instance_id: Optional[str] = None,
) -> AlarmPage:
    """取告警结果列表的一页。

    :param client: 提供 ``post(path, payload)`` 的 TPOPS 客户端
    :raises ProtocolError: 响应体缺 ``records`` / ``total_count``。
    """
    body = build_query_body(
        is_active, is_convergence, limit, offset, alarm_id, instance_id)
    payload = client.post(ALARM_RECORDS_PATH, body)
    require_fields(payload, ('records', 'total_count'), ALARM_RECORDS_PATH)
    records = payload.get('records') or []
    total = payload.get('total_count')
    try:
        total_count = int(total)
    except (TypeError, ValueError):
        total_count = len(records)
    return AlarmPage(records=tuple(records), total_count=total_count)


def fetch_all_alarm_records(
    client: Any,
    is_active: bool,
    is_convergence: bool,
    limit: int = DEFAULT_LIMIT,
    alarm_id: Optional[str] = None,
    instance_id: Optional[str] = None,
    max_pages: int = MAX_PAGES,
) -> Tuple[Mapping[str, Any], ...]:
    """分页取全某个形态下的全部告警记录。

    ``limit`` 上限 100（平台硬限），``total_count`` 更大时**必须翻页** ——
    只取第一页会静默丢掉其余记录。

    :param max_pages: 翻页上限（防御性）
    """
    collected = []
    offset = 0
    pages = 0
    while True:
        page = fetch_alarm_page(
            client, is_active, is_convergence, limit, offset, alarm_id, instance_id)
        pages += 1
        collected.extend(page.records)
        if not page.records:
            break
        # ``total_count`` 为 0 / 缺失时**不能**据此判结束 ——
        # 平台偶尔不返回总数，据此停下就只取第一页（静默丢数据）。
        # 真正的结束判据是「不满一页」。
        if page.total_count > 0 and len(collected) >= page.total_count:
            break
        if len(page.records) < limit:
            # 平台返回了不满一页 —— 后面没有了
            break
        offset += limit
        if pages >= max_pages:
            LOGGER.warning(
                '告警记录翻页达到上限 {} 页（已取 {} 条 / total_count={}），后续未取',
                max_pages, len(collected), page.total_count,
            )
            break
    LOGGER.debug(
        '告警记录拉取完成：is_active={} is_convergence={} 共 {} 条（{} 页）',
        is_active, is_convergence, len(collected), pages,
    )
    return tuple(collected)
