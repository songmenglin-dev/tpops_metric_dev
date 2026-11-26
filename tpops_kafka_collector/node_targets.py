"""**采集目标**的装配 —— 平台画像里的每个实例 → 一个带节点清单的采集目标。

从 :mod:`tpops_kafka_collector.main` 里拆出来 —— 「入口怎么跑」与
「采集目标怎么来」是两件事。

三条口径（修复轮 R-A 的 A4）：

1. **节点清单决定资源名** —— ``resourceName`` = ``{host_ip}_{node_id}``；
   节点清单取不到就退化成「只有 node_id」→ 客户按台账查不到资源 →
   **数据看着有、但对不上台账**；
2. **退避重试 3 次** —— 一次网络抖动不该让整个进程生命周期的资源名永久退化；
3. **取不到就不采** —— 重试用尽后该实例**不参与采集**；
   **所有**实例都取不到 → **启动失败**（那等于平台不可达）。
"""
import time
from typing import Any, Callable, Optional, Tuple
from loguru import logger

from .retry import RetryPolicy, retry_call
from .scheduler_types import InstanceTarget
from .tpops.instances import fetch_instance_nodes

LOGGER = logger

__all__ = [
    'NODE_FETCH_POLICY',
    'NodeListUnavailable',
    'build_instances',
    'fetch_nodes',
]

#: 取节点清单的重试策略 —— **3 次尝试**（首次 + 2 次退避重试）。
#:
#: 重试的判定仍由 :func:`tpops_kafka_collector.errors.is_retryable` 决定
#: （认证失败**绝不重试** —— 平台登录失败会累计 ``login_failed_attempts``
#: 并锁号）。
NODE_FETCH_POLICY = RetryPolicy(
    max_attempts=3, base_delay=1.0, factor=2.0, jitter_ratio=0.1,
)


class NodeListUnavailable(RuntimeError):
    """**所有**实例的节点清单都取不到 —— 平台不可达，**启动失败**。

    与其投出对不上台账的数据，不如**启动失败**（非 0 退出码 + 明确日志），
    让运维立刻看见。
    """


def build_instances(
    client: Any,
    profile: Any,
    policy: Optional[RetryPolicy] = None,
    sleep: Optional[Callable[[float], None]] = None,
) -> Tuple[InstanceTarget, ...]:
    """画像里的每个实例 → 一个采集目标（带节点清单）。

    **节点清单取不到 → 该实例不参与采集**（不产出错的 ``resourceName``）。
    **所有实例都取不到 → 启动失败**（抛 :class:`NodeListUnavailable`）。

    :param policy: 取节点清单的重试策略（默认 :data:`NODE_FETCH_POLICY`）。
    :param sleep: 退避等待函数（测试可注入「不真等」）。
    """
    effective = policy if policy is not None else NODE_FETCH_POLICY
    targets = []
    failed = []
    for item in profile.instances:
        try:
            nodes = fetch_nodes(client, item.instance_id, effective, sleep)
        except Exception:  # noqa: B902 —— 该实例降级，不拖垮其它实例
            LOGGER.exception(
                '取节点清单失败（实例 {}）—— 该实例不参与采集'
                '（resourceName 会退化成只有 node_id，宁可不采）',
                item.instance_id,
            )
            failed.append(item.instance_id)
            continue
        targets.append(InstanceTarget(
            instance_id=item.instance_id,
            instance_name=item.instance_name,
            nodes=nodes,
        ))
    if profile.instances and not targets:
        raise NodeListUnavailable(
            f'所有实例的节点清单都取不到（{len(failed)} 个实例：'
            f'{", ".join(failed)}）—— 平台不可达，进程退出'
        )
    return tuple(targets)


def fetch_nodes(
    client: Any,
    instance_id: str,
    policy: RetryPolicy = NODE_FETCH_POLICY,
    sleep: Optional[Callable[[float], None]] = None,
) -> Tuple[Any, ...]:
    """取实例的节点清单（**退避重试**）；取不到就抛出（由调用方处置）。"""
    return tuple(retry_call(
        fetch_instance_nodes, client, instance_id,
        policy=policy,
        sleep=sleep if sleep is not None else time.sleep,
        description=f'取节点清单（实例 {instance_id}）',
    ))
