"""采集端的**入口装配与信号处理**（任务 3.3）。

装配顺序（依赖关系决定的）::

    load_config()            → 启动期校验（缺变量即退出，**不发任何请求**）
    WatermarkStore.load()    → 从磁盘恢复水位 / 差分基线 / 幂等键
    TpopsClient              → mini-token 认证（**惰性**，首次请求才认证）
    PlatformProfile          → 平台画像（发现一次 → 固化 → 之后以文件为准）
    MessageProducer          → PLAINTEXT，指标 / 告警分 Topic
    五个采集器 + 执行器       → 每个执行器都套一层「停机检查」
    HealthServer             → /healthz（存活）+ /readyz（就绪）
    Scheduler                → 到期驱动 + 组 × 实例两维并发

优雅退出（规格「优雅退出」+ 开发方案 §4.6.5）::

    收到 SIGTERM / SIGINT
      → 置位 shutdown（执行器不再发起新的 HTTP 请求）
      → 停发新任务；在途请求最多等 30 秒（**超时就放弃，不挂死**）
      → 原子写水位
      → flush / 关闭 Kafka producer
      → 退出码 0

**不用 ``scheduler.run()`` / ``run_forever()``** —— 它们内部
``future.result()`` 没有超时，且线程池的上下文管理器会
``shutdown(wait=True)``：只要有一个在途 HTTP 调用挂住，进程就永远不返回，
违反规格「放弃等待该请求 …… 不挂死」。本模块自己跑主循环，每轮放在
**守护线程**里跑并带截止时间。
"""
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from .config import ConfigError, load_config
from .executors import build_executors
from .health import DependencyProbe, HealthServer, ReadinessMonitor
from .kafka.producer import MessageProducer
from .logging_setup import setup_logging, setup_logging_from_config
from .mapping.loader import load_mapping
from .mapping.rules_deltas import CounterDeltaStore
from .platform_profile import PROFILE_FILENAME, PlatformProfile
from .schedule import compute_channel_plans, count_unsupported_metrics
from .lifecycle import (
    SCHEDULER_STALE_SECONDS,
    SHUTDOWN_DEADLINE_SECONDS,
    TrackedScheduler,
    run,
    run_loop,
)
from .scheduler_types import InstanceTarget
from .tpops.client import TpopsClient
from .tpops.command_center import CommandCenterClient
from .tpops.command_center_nodes import MasterNodeRegistry
from .tpops.instances import fetch_instance_nodes
from .watermark import WatermarkStore

LOGGER = logging.getLogger(__name__)

__all__ = [
    'EXIT_CONFIG_ERROR',
    'EXIT_OK',
    'EXIT_STARTUP_ERROR',
    'Runtime',
    'SHUTDOWN_DEADLINE_SECONDS',
    'TrackedScheduler',
    'build_runtime',
    'main',
    'run',
    'run_loop',
]

#: 正常退出码。
EXIT_OK = 0
#: 启动期配置缺失 / 非法。
EXIT_CONFIG_ERROR = 2
#: 装配失败（依赖装不起来）。
EXIT_STARTUP_ERROR = 3

#: 水位文件名（放在 ``state_dir`` 下）。
WATERMARK_FILENAME = 'watermark.json'


@dataclass(frozen=True)
class Runtime:
    """一次装配的产物（不可变）。

    ``main()`` 只负责「装 → 起 → 收信号 → 优雅退 → 退出」；
    测试与演练脚本直接用 :func:`build_runtime` 换掉外部依赖。
    """

    config: Any
    mapping: Any
    client: Any
    profile: Any
    producer: Any
    watermark: Any
    scheduler: Any
    health: Any
    instances: Tuple[InstanceTarget, ...]
    deltas: Any
    shutdown: Any


# ---------------------------------------------------------------------------
# 装配
# ---------------------------------------------------------------------------

def build_runtime(
    config: Any,
    client: Any = None,
    producer: Any = None,
    mapping: Any = None,
    watermark: Any = None,
    node_registry: Any = None,
    command_center: Any = None,
    profile: Any = None,
    health_server: Any = None,
    shutdown: Optional[threading.Event] = None,
    start_health: bool = False,
) -> Runtime:
    """装配全部依赖。

    **外部依赖可注入**（``client`` / ``producer`` / …）—— Kafka 的 broker
    地址待客户确认，端到端演练不可能连真 Kafka，演练脚本靠注入假依赖跑。
    """
    stop = shutdown if shutdown is not None else threading.Event()
    bundle = mapping if mapping is not None else load_mapping()
    store = _watermark_store(config, watermark)
    tpops = client if client is not None else TpopsClient.from_config(config)
    registry = node_registry if node_registry is not None else MasterNodeRegistry(tpops)
    center = command_center if command_center is not None else CommandCenterClient.from_config(
        config, tpops,
    )
    platform = profile if profile is not None else _load_profile(config, tpops, registry)
    sink = producer if producer is not None else MessageProducer.from_config(config)
    deltas = CounterDeltaStore()
    instances = _build_instances(tpops, platform)
    executors = build_executors(
        client=tpops, mapping=bundle, config=config, command_center=center,
        node_registry=registry, deltas=deltas, watermark=store, shutdown=stop,
    )
    scheduler = _build_scheduler(
        config, bundle, executors, sink, store, instances, deltas,
    )
    health = _build_health(config, tpops, scheduler, health_server, start_health)
    LOGGER.info(
        '装配完成：%d 个实例 / %d 个通道 / 不可提供指标 %d 条',
        len(instances), len(executors), count_unsupported_metrics(bundle),
    )
    return Runtime(
        config=config, mapping=bundle, client=tpops, profile=platform,
        producer=sink, watermark=store, scheduler=scheduler, health=health,
        instances=instances, deltas=deltas, shutdown=stop,
    )


def _build_scheduler(
    config: Any,
    bundle: Any,
    executors: Any,
    sink: Any,
    store: Any,
    instances: Any,
    deltas: Any,
) -> TrackedScheduler:
    """调度器 —— **差分基线存储与采集器共用同一个对象**。

    给了不同的对象，基线就不落盘 → 重启后速率永远算不出来（且不报错）。
    """
    return TrackedScheduler(
        plans=compute_channel_plans(bundle),
        executors=executors,
        producer=sink,
        watermark=store,
        instances=instances,
        delta_store=deltas,
        dropped_unsupported=count_unsupported_metrics(bundle),
        tick_interval_seconds=config.poll_interval_seconds,
    )


def _build_health(
    config: Any,
    client: Any,
    scheduler: Any,
    health_server: Any,
    start_health: bool,
) -> Any:
    """健康端点（两个轻量探针）+ 按需启动。"""
    health = health_server if health_server is not None else HealthServer.from_config(
        config, monitor=_build_monitor(client, scheduler),
    )
    if start_health:
        health.start()
    return health


def _watermark_store(config: Any, injected: Any) -> Any:
    """水位存储（自己建的就从磁盘恢复；注入的不动）。"""
    if injected is not None:
        return injected
    state_dir = str(getattr(config, 'state_dir', './state'))
    os.makedirs(state_dir, exist_ok=True)
    store = WatermarkStore(os.path.join(state_dir, WATERMARK_FILENAME))
    store.load()
    return store


def _load_profile(config: Any, client: Any, registry: Any) -> PlatformProfile:
    """平台画像（读固化文件；没有就发现一次并固化）+ 顺手核对。"""
    state_dir = str(getattr(config, 'state_dir', './state'))
    os.makedirs(state_dir, exist_ok=True)
    return PlatformProfile.load_or_discover(
        os.path.join(state_dir, PROFILE_FILENAME), client,
        node_registry=registry, reconcile=True,
    )


def _build_instances(client: Any, profile: Any) -> Tuple[InstanceTarget, ...]:
    """画像里的每个实例 → 一个采集目标（带节点清单）。"""
    targets = []
    for item in profile.instances:
        targets.append(InstanceTarget(
            instance_id=item.instance_id,
            instance_name=item.instance_name,
            nodes=_fetch_nodes(client, item.instance_id),
        ))
    return tuple(targets)


def _fetch_nodes(client: Any, instance_id: str) -> Tuple[Any, ...]:
    """取实例的节点清单；取不到只记 ERROR（不毁掉整轮采集）。"""
    try:
        return tuple(fetch_instance_nodes(client, instance_id))
    except Exception as exc:  # noqa: B902 —— 节点清单缺失只影响资源名
        LOGGER.error(
            '取节点清单失败（实例 %s）：%s —— resourceName 会退化成只有 node_id',
            instance_id, exc,
        )
        return ()


# ---------------------------------------------------------------------------
# 健康端点接线
# ---------------------------------------------------------------------------

def _build_monitor(client: Any, scheduler: Any) -> ReadinessMonitor:
    """两个**轻量**依赖探针（读标志位，不发网络请求）。"""
    return ReadinessMonitor((
        DependencyProbe('tpops', lambda: _tpops_state(client)),
        DependencyProbe('scheduler', lambda: _scheduler_state(scheduler)),
    ))


def _tpops_state(client: Any) -> Any:
    """TPOPS 认证状态：最近一次认证成功 → 就绪。

    读客户端**已经维护好的标志位**，不在探针里发认证请求
    （探针本身不能变成故障源）。
    """
    checker = getattr(client, 'auth_ok', None)
    if checker is not None:
        return bool(checker)
    if getattr(client, '_auth_failed_at', None) is not None:
        return 'TPOPS 最近一次认证失败'
    if not getattr(client, '_token', None):
        return 'TPOPS 尚未完成认证'
    return True


def _scheduler_state(scheduler: Any) -> Any:
    """调度器状态：最近 3 分钟内成功完成过一轮 → 就绪。"""
    last = getattr(scheduler, 'last_tick_ok_at', None)
    if last is None:
        return '调度器还没有成功完成过一轮采集'
    if time.time() - float(last) > SCHEDULER_STALE_SECONDS:
        return f'最近一次成功采集距今超过 {SCHEDULER_STALE_SECONDS} 秒'
    return True


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def main(env: Any = None) -> int:
    """进程入口：装 → 起 → 收信号 → 优雅退 → 返回退出码。

    :param env: 环境变量映射（默认 ``os.environ``）；显式传入便于测试。
    :returns: 退出码（0 = 正常退出）。
    """
    setup_logging()
    try:
        config = load_config(env)
    except ConfigError as exc:
        LOGGER.error('配置校验失败，进程退出：%s', exc)
        return EXIT_CONFIG_ERROR
    setup_logging_from_config(config)
    LOGGER.info('采集端启动：状态目录 %s', config.state_dir)
    try:
        runtime = build_runtime(config, start_health=True)
    except Exception:  # noqa: B902 —— 装配失败要说清楚，不要裸崩
        LOGGER.exception('装配失败，进程退出')
        return EXIT_STARTUP_ERROR
    return run(runtime)


if __name__ == '__main__':
    sys.exit(main())
