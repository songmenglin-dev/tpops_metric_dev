"""D 类（实例元数据）平台接口层。

对应规格 ``specs/collection-channels/spec.md`` 的「实例元数据通道」，
任务 2.8。平台接口只有一条：

    GET /rds/v3.2/instances

**路径是 ``v3.2`` 而不是 ``v3``** —— 实例列表挂在 ``/rds/v3.2`` 这一段
（少一段前缀平台返回 404，不是「接口没实现」）。这一段与
``/rds/gaussdb/v3``（诊断优化 + 统一命令中心）是**两套命名空间，不能混**。

**响应结构**（实测留档 ``data/metric_response/061_gaussdb.server_version.json``）::

    {
      "total_count": 2,
      "instances": [
        {"id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
         "name": "gauss-6374",
         "mode": "Combined",
         "created_at": "2026-11-03T07:52:54+0000",
         "instance_status": "normal",
         "engine_name": "gaussdbv5",
         "engine_version": "V2.0-10.0.0",
         "agent_info": {...}}
      ]
    }

**平台上不止一个实例**（实测 2~3 个）—— 采集端**按平台返回的全部实例循环采集**
（交付时客户的实例名/ID 与探测环境不同，不写死任何一个）；
需要单个实例的元数据时**由调用方按实例 ID 挑选**（``select_instance``）；采集器不猜。

本模块**只做取数与解析**，不做映射、不拼 Kafka 消息。
"""

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple
from loguru import logger

from ..errors import ProtocolError
from .client import GAUSSDB_V3, RDS_V3_2, require_fields

LOGGER = logger

__all__ = [
    'INSTANCE_FIELDS',
    'INSTANCES_PATH',
    'NODE_LIST_PATH',
    'InstanceMetadata',
    'fetch_instance_nodes',
    'fetch_instances',
    'select_instance',
]

#: 实例列表接口（**注意是 v3.2**）。
INSTANCES_PATH = RDS_V3_2 + '/instances'

#: 节点清单接口（第二套命名空间）—— D 类指标与节点无关，但资源名要落到
#: 一个具体节点上（第一个 master CN），所以要取节点清单。
NODE_LIST_PATH = GAUSSDB_V3 + '/instances/{instance_id}/key-view-execute-node'

#: 实例列表的默认分页大小（平台上限 1000，留档用的是 100）。
DEFAULT_PAGE_SIZE = 100

#: 实例对象的字段名（实测留档的**实际返回字段**，不是我们想当然的）。
INSTANCE_FIELDS = (
    'id',
    'name',
    'mode',
    'created_at',
    'instance_status',
    'engine_name',
    'engine_version',
)


@dataclass(frozen=True)
class InstanceMetadata:
    """一个实例的元数据（不可变）。

    :param instance_id: 实例 ID（平台字段 ``id``）。
    :param name: 实例名（如 ``gauss-6374``）。
    :param mode: 部署形态（``Combined`` / ``Ha``）。
    :param engine_version: 引擎版本（如 ``V2.0-10.0.0``）—— **D 类唯一被取的值**。
    :param instance_status: 实例状态（``normal`` 等）。
    :param engine_name: 引擎名（如 ``gaussdbv5``）。
    :param created_at: 创建时刻。
    :param fields: **平台原始行**（规则引擎按平台字段名取值用）。
    """

    instance_id: str
    name: str = ''
    mode: str = ''
    engine_version: Optional[str] = None
    instance_status: Optional[str] = None
    engine_name: Optional[str] = None
    created_at: Optional[str] = None
    fields: Mapping[str, Any] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        object.__setattr__(self, 'fields', dict(self.fields or {}))


def fetch_instances(
    client: Any,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Tuple[InstanceMetadata, ...]:
    """拉取实例列表（按 ``total_count`` 翻页取全）。

    :param client: 提供 ``get(path, params)`` 的 TPOPS 客户端。
    :param page_size: 单页条数。
    :raises ProtocolError: 响应体缺 ``instances`` 字段（平台行为变了）。
    """
    if page_size <= 0:
        raise ProtocolError(f'实例列表分页大小必须为正整数：{page_size}')

    collected = []  # type: list
    seen = set()  # type: set
    offset = 0
    while True:
        payload = client.get(
            INSTANCES_PATH,
            params={'limit': page_size, 'offset': offset},
        )
        rows = _instance_rows(payload)
        total = _total_count(payload)
        for row in rows:
            item = _to_metadata(row)
            if item.instance_id in seen:
                # 平台忽略 offset 时会重复返回同一页 —— 停在这里，不要死循环。
                continue
            seen.add(item.instance_id)
            collected.append(item)
        if len(rows) < page_size or len(collected) >= total:
            break
        offset += page_size
    return tuple(collected)


def select_instance(
    instances: Sequence[InstanceMetadata],
    instance_id: str,
) -> Optional[InstanceMetadata]:
    """按实例 ID 挑选目标实例；找不到返回 ``None``（**不猜、不取第一条**）。"""
    for item in instances or ():
        if item.instance_id == instance_id:
            return item
    return None


def fetch_instance_nodes(
    client: Any,
    instance_id: str,
) -> Tuple[Mapping[str, Any], ...]:
    """取实例的节点清单（**全量**，不做 master 过滤）。

    过滤与去重是**调用方**的事 —— D 类要的是「第一个 master CN」
    （``mapping.nodes.select_master_node``），过滤掉别的节点会让
    「第一个」失去意义。

    :raises ProtocolError: 响应体缺 ``nodes`` 字段。
    """
    path = NODE_LIST_PATH.format(instance_id=instance_id)
    payload = client.get(path)
    body = require_fields(payload, ('nodes',), path)
    nodes = body.get('nodes') or []
    return tuple(dict(node) for node in nodes if isinstance(node, Mapping))


def _instance_rows(payload: Mapping[str, Any]) -> Tuple[Mapping[str, Any], ...]:
    """取出实例行列表；缺 ``instances`` 字段 → 协议错误。"""
    body = require_fields(payload, ('instances',), INSTANCES_PATH)
    rows = body.get('instances') or []
    if not isinstance(rows, (list, tuple)):
        raise ProtocolError(
            f'实例列表响应里的 instances 期望行列表，实际是 {type(rows).__name__}'
        )
    return tuple(dict(row) for row in rows if isinstance(row, Mapping))


def _total_count(payload: Mapping[str, Any]) -> int:
    """响应里的总条数；缺失或非法按已取条数处理（不猜）。"""
    total = payload.get('total_count')
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        return 0
    return total


def _to_metadata(row: Mapping[str, Any]) -> InstanceMetadata:
    """平台实例行 → :class:`InstanceMetadata`。"""
    return InstanceMetadata(
        instance_id=str(row.get('id') or ''),
        name=str(row.get('name') or ''),
        mode=str(row.get('mode') or ''),
        engine_version=row.get('engine_version'),
        instance_status=row.get('instance_status'),
        engine_name=row.get('engine_name'),
        created_at=row.get('created_at'),
        fields=dict(row),
    )


def _as_dict(payload: Any) -> Dict[str, Any]:
    """保底：响应体不是对象时给空字典。"""
    return dict(payload) if isinstance(payload, Mapping) else {}
