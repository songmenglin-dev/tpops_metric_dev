"""命令中心的 master 节点清单：只留 master + 按 ``node_id`` 去重 + 按实例缓存。

与 :mod:`tpops_kafka_collector.tpops.command_center` 配套（任务 2.5）——
单独成文件是为了守住「单文件 < 400 行」的红线。

**三条实测踩过的坑在这里固化**：

1. **命令只能在 master 节点执行** —— 命令级约束 ``limit_node_types=["master"]``。
   平台清单里既有 master 也有 slave；只留 ``role == "master"``。
2. **同一个 master 会以 CN / DN 各出现一次** —— 实测 12 条条目 / 6 条 master /
   **3 个唯一 node_id**。下发前**必须按 ``node_id`` 去重**，
   否则同一个节点会被下发两次。
3. **节点清单按实例缓存 1 小时** —— 不每轮都拉平台。
"""
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple  # noqa: F401  （Dict/List 仅用于 # type: 注释）
from loguru import logger

from ..errors import ProtocolError
from .client import GAUSSDB_V3, require_fields

LOGGER = logger

__all__ = ['EXECUTE_NODE_PATH', 'NODE_CACHE_TTL_SECONDS', 'MasterNodeRegistry']

#: 取可执行节点清单（命令**只能**在 master 节点执行，下发前必须先拿）。
EXECUTE_NODE_PATH = GAUSSDB_V3 + '/instances/{instance_id}/key-view-execute-node'

#: 节点清单的缓存时长（秒）—— **按实例**缓存，不每轮都拉。
NODE_CACHE_TTL_SECONDS = 3600.0

#: master 节点在平台清单里的角色标记。
MASTER_ROLE = 'master'


class MasterNodeRegistry:
    """master 节点清单的取数与缓存。

    :param client: TPOPS HTTP 客户端（需有 ``get``）。
    :param ttl: 缓存时长（秒）。
    :param clock: 单调时钟（测试注入假实现）。
    """

    def __init__(
        self,
        client: Any,
        ttl: float = NODE_CACHE_TTL_SECONDS,
        clock: Callable[[], float] = None,
    ) -> None:
        self._client = client
        self._ttl = ttl
        self._clock = clock if clock is not None else _default_clock
        # 实例 → (取回时刻, 去重后的 master 节点元组)
        self._cache = {}  # type: Dict[str, Tuple[float, Tuple[Mapping[str, Any], ...]]]

    def nodes(self, instance_id: str) -> Tuple[Mapping[str, Any], ...]:
        """实例的 master 节点清单（按 ``node_id`` 去重，顺序稳定）。"""
        cached = self._cached(instance_id)
        if cached is not None:
            return cached
        path = EXECUTE_NODE_PATH.format(instance_id=instance_id)
        body = self._client.get(path)
        raw = require_fields(body, ('nodes',), path).get('nodes') or []
        masters = _select_masters(raw)
        if not masters:
            raise ProtocolError(
                f'实例 {instance_id} 的节点清单里没有可执行的 master 节点'
            )
        self._cache[instance_id] = (self._clock(), masters)
        return masters

    def node_ids(self, instance_id: str) -> Tuple[str, ...]:
        """master 节点 ID 列表（已去重，顺序稳定）。"""
        return tuple(str(node['node_id']) for node in self.nodes(instance_id))

    def _cached(
        self, instance_id: str
    ) -> Optional[Tuple[Mapping[str, Any], ...]]:
        """取缓存的节点清单；过期或没有返回 ``None``。"""
        entry = self._cache.get(instance_id)
        if entry is None:
            return None
        fetched_at, nodes = entry
        if self._clock() - fetched_at >= self._ttl:
            return None
        return nodes


def _select_masters(
    raw: Any,
) -> Tuple[Mapping[str, Any], ...]:
    """从平台清单里挑出 master 并**按 ``node_id`` 去重**。"""
    masters = []  # type: List[Mapping[str, Any]]
    seen = set()  # type: set
    for node in raw or ():
        if not isinstance(node, Mapping):
            continue
        if node.get('role') != MASTER_ROLE:
            continue
        node_id = node.get('node_id')
        if not node_id or node_id in seen:
            continue
        seen.add(node_id)
        masters.append(dict(node))
    return tuple(masters)


def _default_clock() -> float:
    import time

    return time.monotonic()
