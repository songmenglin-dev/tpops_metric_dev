"""平台画像的**发现、固化与核对**（任务 3.3）。

**实例 ID 不走配置** —— 采集端连上 TPOPS 后自己发现，然后固化到文件，
之后以文件为准。换平台、换实例都不用改配置、也不用改代码。

发现规则：**选择所有实例** ——

1. 取平台实例列表（``GET /rds/v3.2/instances``）；
2. **全部实例都要采集** —— 不挑、不筛、不取第一个、**不按 ``mode`` 筛**
   （``mode`` 只记录在画像里，便于排查，不参与选择）；
3. 平台没有实例 → **明确报错**（不静默跑一个空采集）。

**为什么固化后不再每轮重新发现**：平台实例列表的顺序可能变，客户后来也可能
加实例。每轮重新发现会导致**静默增删采集目标** —— 水位与差分基线全部错位，
而且是「不报错但数据错」的形态。

**核对（启动时顺手做，只记日志、不改文件）**：

=========================================  ==========================
不一致项                                    处置
=========================================  ==========================
画像里的 instance_id 不在平台实例列表里     **ERROR**（该实例可能已被删/重建）
平台列表里有画像没记录的实例                 **WARNING**（提示删画像文件可重新发现）
``engine_name`` 变了                        **WARNING**（平台升级 → 指标码可能变）
``master_count`` 变了                       **WARNING**（扩缩容 → 多 master 口径要复核）
=========================================  ==========================

**新增实例不自动加** —— 加了就等于每轮重新发现。新增实例要人工确认：
改画像文件（可手工编辑）后下一轮启动生效。
"""
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, List, Mapping, Optional, Sequence, Set, Tuple

from .errors import TpopsError
from .tpops.instances import fetch_instances
from .watermark import _atomic_write_json

LOGGER = logging.getLogger(__name__)

__all__ = [
    'PROFILE_FILENAME',
    'PROFILE_VERSION',
    'PlatformProfile',
    'ProfileError',
    'ProfileInstance',
    'ProfileReconciliation',
]

#: 画像文件名（放在 ``state_dir`` 下）。
PROFILE_FILENAME = 'platform_profile.json'

#: 画像文件的格式版本。
PROFILE_VERSION = 1


class ProfileError(TpopsError):
    """平台画像不可用（实例列表为空 / 文件损坏 / 版本不兼容）。

    属于**不可重试**错误 —— 带着空画像继续跑等于什么都不采。
    """


@dataclass(frozen=True)
class ProfileInstance:
    """画像里的一个实例（不可变）。"""

    instance_id: str
    instance_name: str = ''
    mode: str = ''
    engine_name: str = ''
    engine_version: str = ''
    master_count: int = 0

    def to_payload(self) -> Mapping[str, Any]:
        return {
            'instance_id': self.instance_id,
            'instance_name': self.instance_name,
            'mode': self.mode,
            'engine_name': self.engine_name,
            'engine_version': self.engine_version,
            'master_count': self.master_count,
        }

    @classmethod
    def from_payload(cls, payload: Any) -> 'ProfileInstance':
        if not isinstance(payload, Mapping):
            raise ProfileError('平台画像条目不是对象')
        instance_id = str(payload.get('instance_id') or '')
        if not instance_id:
            raise ProfileError('平台画像条目缺 instance_id')
        return cls(
            instance_id=instance_id,
            instance_name=str(payload.get('instance_name') or ''),
            mode=str(payload.get('mode') or ''),
            engine_name=str(payload.get('engine_name') or ''),
            engine_version=str(payload.get('engine_version') or ''),
            master_count=int(payload.get('master_count') or 0),
        )


@dataclass(frozen=True)
class ProfileReconciliation:
    """一次核对的结果（不可变）—— **只用于记账，不改画像文件**。"""

    missing_on_platform: Tuple[str, ...] = ()
    added_on_platform: Tuple[str, ...] = ()
    engine_changed: Tuple[str, ...] = ()
    master_count_changed: Tuple[str, ...] = ()

    @property
    def is_consistent(self) -> bool:
        return not (
            self.missing_on_platform or self.added_on_platform
            or self.engine_changed or self.master_count_changed
        )


@dataclass(frozen=True)
class PlatformProfile:
    """平台画像（不可变）。

    :param version: 文件格式版本。
    :param instances: 实例清单（**全部**实例，顺序与平台列表一致）。
    :param discovered_at: 发现时刻（UTC ISO 格式）。
    """

    version: int = PROFILE_VERSION
    instances: Tuple[ProfileInstance, ...] = ()
    discovered_at: str = ''

    # -- 序列化 -----------------------------------------------------------

    def to_payload(self) -> Mapping[str, Any]:
        return {
            'version': self.version,
            'instances': [item.to_payload() for item in self.instances],
            'discovered_at': self.discovered_at,
        }

    @classmethod
    def from_payload(cls, payload: Any) -> 'PlatformProfile':
        if not isinstance(payload, Mapping):
            raise ProfileError('平台画像文件顶层不是对象')
        version = payload.get('version')
        if version != PROFILE_VERSION:
            raise ProfileError(
                f'平台画像版本不兼容：期望 {PROFILE_VERSION}，实际 {version!r}'
            )
        raw = payload.get('instances') or []
        if not isinstance(raw, (list, tuple)):
            raise ProfileError('平台画像里的 instances 不是数组')
        return cls(
            version=version,
            instances=tuple(ProfileInstance.from_payload(item) for item in raw),
            discovered_at=str(payload.get('discovered_at') or ''),
        )

    # -- 读写 -------------------------------------------------------------

    @classmethod
    def load(cls, path: str) -> 'PlatformProfile':
        """从固化文件读画像。"""
        try:
            with open(path, encoding='utf-8') as handle:
                payload = json.load(handle)
        except (OSError, ValueError) as exc:
            raise ProfileError(f'平台画像文件读取失败：{path}（{exc}）')
        return cls.from_payload(payload)

    @classmethod
    def save(cls, path: str, profile: 'PlatformProfile') -> None:
        """原子写画像（复用水位模块的原子写实现，不另写一份）。"""
        _atomic_write_json(path, profile.to_payload())

    # -- 发现 -------------------------------------------------------------

    @classmethod
    def load_or_discover(
        cls,
        path: str,
        client: Any,
        node_registry: Any = None,
        clock: Optional[Callable[[], float]] = None,
        reconcile: bool = False,
    ) -> 'PlatformProfile':
        """读固化画像；没有就发现一次并固化。

        **固化文件存在 → 不重新发现、不改写**（画像以文件为准）：
        平台列表的顺序会变、客户也会加实例，每轮重新发现等于**静默增删
        采集目标** —— 水位与差分基线全部错位，且不报错。

        :param reconcile: 是否在读固化画像后**额外调一次**平台列表做核对
            （生产装配传 ``True``；**只记日志，不改画像文件**）。
            不传（``False``）时**平台列表接口一次都不调用**。
        """
        if os.path.exists(path):
            profile = cls.load(path)
            LOGGER.info(
                '平台画像已从固化文件读取（不重新发现）：%s（%d 个实例）',
                path, len(profile.instances),
            )
            if reconcile:
                cls.reconcile(profile, client, node_registry=node_registry)
            return profile
        profile = cls.discover(client, node_registry=node_registry, clock=clock)
        cls.save(path, profile)
        LOGGER.info(
            '平台画像已固化：%s（%d 个实例，发现时刻 %s）',
            path, len(profile.instances), profile.discovered_at,
        )
        return profile

    @classmethod
    def discover(
        cls,
        client: Any,
        node_registry: Any = None,
        clock: Optional[Callable[[], float]] = None,
    ) -> 'PlatformProfile':
        """从平台发现**全部**实例并记成画像。"""
        instances = fetch_instances(client)
        if not instances:
            raise ProfileError('平台实例列表为空 —— 没有可采集的实例')
        entries = []
        for meta in instances:
            entries.append(ProfileInstance(
                instance_id=meta.instance_id,
                instance_name=meta.name,
                mode=meta.mode,
                engine_name=str(meta.engine_name or ''),
                engine_version=str(meta.engine_version or ''),
                master_count=_master_count(node_registry, meta.instance_id),
            ))
        LOGGER.info('平台画像发现完成：%d 个实例', len(entries))
        return cls(PROFILE_VERSION, tuple(entries), _utc_now_iso(clock))

    # -- 核对 -------------------------------------------------------------

    @classmethod
    def reconcile(
        cls,
        profile: 'PlatformProfile',
        client: Any,
        node_registry: Any = None,
    ) -> ProfileReconciliation:
        """拿画像与平台现状对一遍 —— **只记日志，不改画像文件**。"""
        platform = fetch_instances(client)
        by_id = dict((item.instance_id, item) for item in platform)
        recorded = set(item.instance_id for item in profile.instances)
        missing = []
        engine_changed = []
        master_changed = []
        for item in profile.instances:
            is_missing, engine_diff, master_diff = _compare_instance(
                item, by_id.get(item.instance_id), node_registry,
            )
            if is_missing:
                missing.append(item.instance_id)
                continue
            if engine_diff:
                engine_changed.append(item.instance_id)
            if master_diff:
                master_changed.append(item.instance_id)
        return ProfileReconciliation(
            missing_on_platform=tuple(missing),
            added_on_platform=tuple(_scan_added(platform, recorded)),
            engine_changed=tuple(engine_changed),
            master_count_changed=tuple(master_changed),
        )


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def _compare_instance(
    item: ProfileInstance,
    meta: Any,
    node_registry: Any,
) -> Tuple[bool, bool, bool]:
    """比对画像里的一个实例与平台现状 —— **只记日志，不改画像**。

    :param item: 画像里的实例。
    :param meta: 平台上的同实例元数据（``None`` 表示平台列表里没有它）。
    :param node_registry: master 节点清单（``None`` 表示**不比** master 数）。
    :returns: ``(平台上已缺失, 引擎名变了, master 数变了)`` 三个布尔。
    """
    if meta is None:
        LOGGER.error(
            '平台画像里的实例 %s（%s）在平台实例列表里已不存在 —— '
            '需人工确认（被删除或重建？），本轮不采集它',
            item.instance_id, item.instance_name or '-',
        )
        return True, False, False
    return False, _compare_engine(item, meta), _compare_master_count(
        item, node_registry,
    )


def _compare_engine(item: ProfileInstance, meta: Any) -> bool:
    """比对引擎名 —— 变了返回 True（平台升级 → 指标码可能变）。"""
    current_engine = str(meta.engine_name or '')
    if current_engine == item.engine_name:
        return False
    LOGGER.warning(
        '实例 %s 的引擎名与画像不一致（画像 %s → 平台 %s）—— '
        '平台升级后指标码可能已变，映射表可能需要重新生成',
        item.instance_id, item.engine_name or '-', current_engine or '-',
    )
    return True


def _compare_master_count(item: ProfileInstance, node_registry: Any) -> bool:
    """比对 master 节点数 —— 变了返回 True（**没有节点清单就不比**）。"""
    if node_registry is None:
        return False
    current_master = _master_count(node_registry, item.instance_id)
    if current_master == item.master_count:
        return False
    LOGGER.warning(
        '实例 %s 的 master 节点数与画像不一致（画像 %d → 当前 %d）—— '
        '拓扑变了（扩缩容），多 master 归集口径要复核',
        item.instance_id, item.master_count, current_master,
    )
    return True


def _scan_added(platform: Sequence[Any], recorded: Set[str]) -> List[str]:
    """平台上有、但画像里没记录的实例 —— **只告警，不自动加**。

    新增实例要人工确认（改画像文件后下一轮启动生效）—— 自动加就等于
    每轮重新发现，水位与差分基线会全部错位。
    """
    added = []
    for meta in platform:
        if meta.instance_id in recorded:
            continue
        added.append(meta.instance_id)
        LOGGER.warning(
            '平台新增了实例 %s（%s）—— 平台画像里没有记录，需人工更新'
            '（删除画像文件可重新发现）',
            meta.instance_id, meta.name or '-',
        )
    return added


def _master_count(node_registry: Any, instance_id: str) -> int:
    """该实例的 master 节点数 —— **按唯一 node_id 计数**，不数条目。

    实测：分布式实例 ``role=master`` 有 6 条，但只有 3 个唯一 ``node_id``
    （一次 CN、一次 DN）—— 数条目会虚高一倍。
    """
    if node_registry is None:
        return 0
    try:
        nodes = node_registry.nodes(instance_id)
    except Exception as exc:  # noqa: B902 —— 取不到清单只影响这一个计数
        LOGGER.error(
            '取 master 节点清单失败（实例 %s）：%s —— master_count 记 0',
            instance_id, exc,
        )
        return 0
    unique = set()
    without_id = 0
    for node in nodes or ():
        node_id = node.get('node_id') if isinstance(node, Mapping) else None
        if node_id:
            unique.add(str(node_id))
        else:
            without_id += 1
    return len(unique) + without_id


def _utc_now_iso(clock: Optional[Callable[[], float]] = None) -> str:
    """当前时刻的 UTC ISO 格式（如 ``2026-11-23T04:00:00Z``）。"""
    now = clock() if clock is not None else time.time()
    moment = datetime.fromtimestamp(float(now), tz=timezone.utc)
    return moment.strftime('%Y-%m-%dT%H:%M:%SZ')
