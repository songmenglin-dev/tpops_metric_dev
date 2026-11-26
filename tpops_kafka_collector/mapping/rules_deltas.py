"""累计计数器的差分基线。

铁律（开发方案 §3.4 / 规格「累计计数的跨轮差分」）：

- **首轮无基线只记基线**，不产出值（不是 0，也不是假数据）；
- 时间差用**实际采集时刻之差**，不是标称采集周期；
- 累计值回退判定为**实例重启**：丢弃本轮差分（值为 None）、记 **ERROR**、重建基线。

**基线键的形状是跨模块契约**：
``{(metric_code, dimension_key): (counter_value, sampled_at_ms)}`` ——
``tpops_kafka_collector/watermark.py`` 的水位文件按同一形状存取基线，
改了这里就存不上（``WatermarkError``）。

**基线键不含节点**（``dimension_key`` 只有业务维度）—— 同一轮的多个 master
共用一个存储时，A 节点的基线会被 B 节点覆盖（实测：3 个 master 里 2 个恒为
``None``，还可能算出「A 的当前值 − B 的上轮值」这种跨节点速率）。
因此基线键支持一个**作用域**（``scope``，一般是 ``node_id``）：
作用域并进维度键（``scope:key``），**键的形状仍然是二元组**，
水位文件那一侧不用改。
"""
from typing import Any, Dict, Mapping, Optional, Tuple
from loguru import logger

from .rules_context import dimension_key

LOGGER = logger

__all__ = [
    'CounterDeltaStore',
    'ScopedCounterDeltaStore',
    'scoped_dimension_key',
]


def scoped_dimension_key(scope: Any, key: str) -> str:
    """把作用域并进维度键。

    维度键本身是 ``k=v`` 用 ``|`` 连接的一串；首段不含 ``=``，
    所以 ``scope:key`` 不会与任何维度名撞上。
    """
    text = str(scope or '')
    if not text:
        return key
    return f'{text}:{key}'


class CounterDeltaStore:
    """累计计数器的差分基线。"""

    def __init__(self) -> None:
        self._baselines: Dict[Tuple[str, str], Tuple[float, int]] = {}

    def rate(
        self,
        metric_code: str,
        dimensions: Mapping[str, Any],
        counter_value: float,
        now_ms: int,
        scope: str = '',
    ) -> Tuple[Optional[float], str]:
        """返回 ``(速率, 说明)``。速率为 None 时说明原因。

        :param scope: 作用域（一般是 ``node_id``）—— 同一存储被多个 master
            共用时用它把基线**按节点隔离**；留空则与旧行为一致。
        """
        key = (metric_code, scoped_dimension_key(scope, dimension_key(dimensions)))
        previous = self._baselines.get(key)
        if previous is None:
            self._baselines[key] = (counter_value, now_ms)
            LOGGER.debug(
                '首轮无基线，仅建立基线：metric={} dims={} scope={} value={}',
                metric_code, dimensions, scope, counter_value,
            )
            return None, '首轮无基线，仅建立基线'
        last_value, last_ms = previous
        if counter_value < last_value:
            self._baselines[key] = (counter_value, now_ms)
            LOGGER.error(
                '累计计数回退，判定实例重启，丢弃本轮差分并重建基线：'
                'metric={} dims={} scope={} 上次={} 本次={}',
                metric_code, dimensions, scope, last_value, counter_value,
            )
            return None, '累计值回退，判定实例重启，丢弃本轮差分并重建基线'
        elapsed = (now_ms - last_ms) / 1000.0
        if elapsed <= 0:
            LOGGER.warning(
                '采集时刻未推进，丢弃本轮差分：metric={} dims={} scope={} now={} last={}',
                metric_code, dimensions, scope, now_ms, last_ms,
            )
            return None, '采集时刻未推进，丢弃本轮差分'
        self._baselines[key] = (counter_value, now_ms)
        return (counter_value - last_value) / elapsed, '按采集时刻差分'

    def forget(self, metric_code: str, dimensions: Mapping[str, Any],
               scope: str = '') -> None:
        """丢掉某个维度组合的基线（对象消失时用）。"""
        key = (metric_code, scoped_dimension_key(scope, dimension_key(dimensions)))
        self._baselines.pop(key, None)


class ScopedCounterDeltaStore:
    """差分基线存储的**作用域视图**。

    底层还是**同一个**存储（基线因此能跨轮延续、能整体落盘），
    但键里带上了作用域 —— 多个 master 各用一个视图，基线互不覆盖。

    视图本身是**无状态**的，可以每轮现建；基线住在底层存储里。
    """

    def __init__(self, store: Any, scope: Any) -> None:
        self._store = store
        self._scope = str(scope or '')

    @property
    def scope(self) -> str:
        return self._scope

    def rate(
        self,
        metric_code: str,
        dimensions: Mapping[str, Any],
        counter_value: float,
        now_ms: int,
    ) -> Tuple[Optional[float], str]:
        return self._store.rate(
            metric_code, dimensions, counter_value, now_ms, scope=self._scope,
        )

    def forget(self, metric_code: str, dimensions: Mapping[str, Any]) -> None:
        forget = getattr(self._store, 'forget', None)
        if forget is None:
            return
        forget(metric_code, dimensions, scope=self._scope)
