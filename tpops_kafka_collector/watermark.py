"""水位、差分基线与幂等键的持久化。

对应 ``specs/delivery-and-state/spec.md`` 的「水位与差分基线的持久化」与
「幂等去重」两条需求。

设计要点：

- **原子写**：写**同目录**下的临时文件 → ``fsync`` → ``os.replace`` 原子替换。
  进程在写文件途中被杀，磁盘上留下的仍是上一轮的完整 JSON（不是半截）。
  临时文件必须在同目录 —— ``os.replace`` 跨设备会失败（``EXDEV``）。
  落盘前先组装不可变快照 :class:`WatermarkState`，写文件期间内存态不变。
- **差分基线**：键形状与 ``mapping.rules.CounterDeltaStore`` 一致 ——
  ``{(metric_code, dimension_key): (counter_value, sampled_at_ms)}``；
  本模块只负责存盘与恢复，不实现差分算法本身。
- **幂等键**：键由 ``ids.py`` 计算（唯一真相源），本模块只维护 LRU 集合
  （容量可配，默认 10000；淘汰**最久未被使用**的条目 —— 重复键算一次「使用」）。

单实例假设（开发方案 §5.6.4）：同一时刻只有一个进程写这个文件。
"""
import json
import os
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple
from loguru import logger

from .errors import TpopsError
from .ids import DEFAULT_IDEMPOTENCY_LRU_CAPACITY, IdempotencyKeyLRU

LOGGER = logger

#: 水位文件的格式版本。结构不兼容变更时 +1，旧文件按「版本不兼容」拒绝加载。
WATERMARK_STATE_VERSION = 1

#: 临时文件名前缀 / 后缀 —— 与最终状态文件区分，便于人工排查残留。
TEMP_FILE_PREFIX = '.watermark-'
TEMP_FILE_SUFFIX = '.tmp'


class WatermarkError(TpopsError):
    """水位文件不可读、已损坏或版本不兼容。

    属于**不可重试**错误（``errors.is_retryable`` 对 ``TpopsError`` 的普通子类
    返回 ``False``）：带着半截状态继续跑比直接失败更危险。
    """


@dataclass(frozen=True)
class MetricWatermark:
    """某条采集流（按实例 / 通道区分）的指标水位。"""

    stream: str
    end_time_ms: int


@dataclass(frozen=True)
class AlarmWatermark:
    """某条采集流的告警水位（平台侧的增量游标，原样保存）。"""

    stream: str
    last_event_time: str


@dataclass(frozen=True)
class DeltaBaseline:
    """累计计数器的差分基线，与 CounterDeltaStore 的键一一对应。"""

    metric_code: str
    dimension_key: str
    counter_value: float
    sampled_at_ms: int


@dataclass(frozen=True)
class WatermarkState:
    """水位文件的完整内容（不可变）。"""

    metric_watermarks: Tuple[MetricWatermark, ...] = ()
    alarm_watermarks: Tuple[AlarmWatermark, ...] = ()
    delta_baselines: Tuple[DeltaBaseline, ...] = ()
    idempotency_keys: Tuple[str, ...] = ()

    def to_payload(self) -> Dict[str, Any]:
        """转成可 JSON 序列化的字典。"""
        return {
            'version': WATERMARK_STATE_VERSION,
            'metric_watermarks': [
                {'stream': item.stream, 'end_time_ms': item.end_time_ms}
                for item in self.metric_watermarks
            ],
            'alarm_watermarks': [
                {'stream': item.stream, 'last_event_time': item.last_event_time}
                for item in self.alarm_watermarks
            ],
            'delta_baselines': [
                {
                    'metric_code': item.metric_code,
                    'dimension_key': item.dimension_key,
                    'counter_value': item.counter_value,
                    'sampled_at_ms': item.sampled_at_ms,
                }
                for item in self.delta_baselines
            ],
            'idempotency_keys': list(self.idempotency_keys),
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> 'WatermarkState':
        """从 JSON 字典还原；结构不合法时抛 :class:`WatermarkError`。"""
        if not isinstance(payload, Mapping):
            raise WatermarkError('水位文件顶层不是对象')
        version = payload.get('version')
        if version != WATERMARK_STATE_VERSION:
            raise WatermarkError(
                f'水位文件版本不兼容：期望 {WATERMARK_STATE_VERSION}，实际 {version!r}'
            )
        return cls(
            metric_watermarks=_parse_section(
                payload.get('metric_watermarks'), 'metric_watermarks', _metric_entry),
            alarm_watermarks=_parse_section(
                payload.get('alarm_watermarks'), 'alarm_watermarks', _alarm_entry),
            delta_baselines=_parse_section(
                payload.get('delta_baselines'), 'delta_baselines', _baseline_entry),
            idempotency_keys=tuple(
                str(key) for key in (payload.get('idempotency_keys') or ())
            ),
        )


class WatermarkStore:
    """水位 / 差分基线 / 幂等键的内存态 + 原子落盘。

    用法：``load()`` → 每轮更新内存态 → ``save()``（原子写）。
    """

    def __init__(
        self,
        path: str,
        idempotency_capacity: int = DEFAULT_IDEMPOTENCY_LRU_CAPACITY,
    ) -> None:
        self._path = path
        self._metric_watermarks = {}  # type: Dict[str, int]
        self._alarm_watermarks = {}  # type: Dict[str, str]
        self._baselines = {}  # type: Dict[Tuple[str, str], Tuple[float, int]]
        self._keys = IdempotencyKeyLRU(capacity=idempotency_capacity)

    def metric_watermark(self, stream: str) -> Optional[int]:
        return self._metric_watermarks.get(stream)

    def advance_metric(self, stream: str, end_time_ms: int) -> None:
        """推进指标水位；只前进不回退（回退的数据会在下轮重发）。"""
        current = self._metric_watermarks.get(stream)
        if current is None or end_time_ms > current:
            self._metric_watermarks[stream] = end_time_ms

    def alarm_watermark(self, stream: str) -> Optional[str]:
        return self._alarm_watermarks.get(stream)

    def advance_alarm(self, stream: str, last_event_time: str) -> None:
        """推进告警水位（平台侧游标，原样保存）。"""
        self._alarm_watermarks[stream] = last_event_time

    def get_baseline(self, metric_code: str, dimension_key: str) -> Optional[Tuple[float, int]]:
        return self._baselines.get((metric_code, dimension_key))

    def put_baseline(
        self,
        metric_code: str,
        dimension_key: str,
        counter_value: float,
        sampled_at_ms: int,
    ) -> None:
        self._baselines[(metric_code, dimension_key)] = (counter_value, sampled_at_ms)

    def forget_baseline(self, metric_code: str, dimension_key: str) -> None:
        self._baselines.pop((metric_code, dimension_key), None)

    def import_baselines_from(self, delta_store: Any) -> None:
        """从差分基线存储（CounterDeltaStore 同形状）导入，**整体替换**。"""
        container = _baselines_container(delta_store)
        self._baselines = {
            (key[0], key[1]): (value[0], value[1])
            for key, value in container.items()
        }

    def export_baselines_into(self, delta_store: Any) -> None:
        """把本模块的基线**整体替换**进差分基线存储。"""
        container = _baselines_container(delta_store)
        container.clear()
        for key, value in self._baselines.items():
            container[key] = value

    def remember_key(self, key: str) -> bool:
        """登记幂等键；返回 ``False`` 表示该键此前已投递过，应跳过。"""
        return self._keys.add(key)

    def has_key(self, key: str) -> bool:
        return self._keys.contains(key)

    def idempotency_keys(self) -> Tuple[str, ...]:
        """按新鲜度返回已登记的幂等键（最久未使用 → 最新使用）。"""
        return self._keys.keys()

    def snapshot(self) -> WatermarkState:
        """当前状态的不可变快照。"""
        return WatermarkState(
            metric_watermarks=tuple(
                MetricWatermark(stream, end_time_ms)
                for stream, end_time_ms in sorted(self._metric_watermarks.items())
            ),
            alarm_watermarks=tuple(
                AlarmWatermark(stream, last_event_time)
                for stream, last_event_time in sorted(self._alarm_watermarks.items())
            ),
            delta_baselines=tuple(
                DeltaBaseline(key[0], key[1], value[0], value[1])
                for key, value in sorted(self._baselines.items())
            ),
            idempotency_keys=self._keys.keys(),
        )

    def load(self) -> None:
        """从磁盘恢复状态；文件不存在视为空状态。"""
        if not os.path.exists(self._path):
            self._apply(WatermarkState())
            return
        try:
            with open(self._path, encoding='utf-8') as handle:
                payload = json.load(handle)
        except (OSError, ValueError) as exc:
            raise WatermarkError(f'水位文件读取失败：{self._path}（{exc}）')
        state = WatermarkState.from_payload(payload)
        self._apply(state)

    def save(self) -> None:
        """把当前状态原子写到磁盘。"""
        payload = self.snapshot().to_payload()
        _atomic_write_json(self._path, payload)
        LOGGER.debug('水位已落盘：{}', self._path)

    def _apply(self, state: WatermarkState) -> None:
        self._metric_watermarks = {
            item.stream: item.end_time_ms for item in state.metric_watermarks
        }
        self._alarm_watermarks = {
            item.stream: item.last_event_time for item in state.alarm_watermarks
        }
        self._baselines = {
            (item.metric_code, item.dimension_key): (item.counter_value, item.sampled_at_ms)
            for item in state.delta_baselines
        }
        self._keys = IdempotencyKeyLRU(capacity=self._keys.capacity)
        for key in state.idempotency_keys:
            self._keys.add(key)


# --- 解析辅助 -------------------------------------------------------------

def _metric_entry(item: Mapping[str, Any]) -> MetricWatermark:
    return MetricWatermark(str(item['stream']), int(item['end_time_ms']))


def _alarm_entry(item: Mapping[str, Any]) -> AlarmWatermark:
    return AlarmWatermark(str(item['stream']), str(item['last_event_time']))


def _baseline_entry(item: Mapping[str, Any]) -> DeltaBaseline:
    return DeltaBaseline(
        str(item['metric_code']),
        str(item['dimension_key']),
        float(item['counter_value']),
        int(item['sampled_at_ms']),
    )


def _parse_section(raw: Any, name: str, builder: Any) -> Tuple[Any, ...]:
    """把一段 JSON 数组解析成不可变条目元组。"""
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise WatermarkError(f'{name} 不是数组')
    result = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise WatermarkError(f'{name} 条目不是对象')
        try:
            result.append(builder(item))
        except (KeyError, TypeError, ValueError) as exc:
            raise WatermarkError(f'{name} 条目字段非法：{item!r}（{exc}）')
    return tuple(result)


def _baselines_container(delta_store: Any) -> Dict[Tuple[str, str], Tuple[float, int]]:
    """取出差分基线存储里的基线字典。

    与 ``mapping.rules.CounterDeltaStore`` 的 ``_baselines`` 容器同形状：
    ``{(metric_code, dimension_key): (counter_value, sampled_at_ms)}``。
    规则引擎重写后若改了属性名，``baselines`` 作为备选。
    """
    for attr in ('_baselines', 'baselines'):
        container = getattr(delta_store, attr, None)
        if isinstance(container, dict):
            return container
    raise WatermarkError(
        f'差分基线存储缺少基线字典：{type(delta_store).__name__!r}'
    )


# --- 原子写 ---------------------------------------------------------------

def _dump_payload(payload: Mapping[str, Any], handle: Any) -> None:
    """把状态写成 JSON（单独成函数，便于测试注入写入失败）。"""
    json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)


def _atomic_write_json(path: str, payload: Mapping[str, Any]) -> None:
    """原子写：临时文件 → fsync → ``os.replace``。

    进程在写文件途中被杀时，磁盘上的目标文件仍是上一轮的完整内容；
    异常路径下临时文件被清理，不留残留。
    """
    directory = os.path.dirname(os.path.abspath(path)) or '.'
    if not os.path.isdir(directory):
        raise WatermarkError(f'水位文件目录不存在：{directory}')
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w',
            encoding='utf-8',
            dir=directory,
            prefix=TEMP_FILE_PREFIX,
            suffix=TEMP_FILE_SUFFIX,
            delete=False,
        ) as temp_file:
            temp_path = temp_file.name
            _dump_payload(payload, temp_file)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, path)
        temp_path = None
        _fsync_directory(directory)
    finally:
        if temp_path is not None and os.path.exists(temp_path):
            os.unlink(temp_path)


def _fsync_directory(directory: str) -> None:
    """把目录项刷盘，保证 ``os.replace`` 的结果在崩溃后仍然可见。"""
    try:
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError as exc:
        # 部分平台（如 Windows）不支持目录 fsync；替换本身已是原子的。
        LOGGER.debug('目录 fsync 失败，不影响原子性：{}（{}）', directory, exc)
