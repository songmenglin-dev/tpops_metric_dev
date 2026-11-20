"""累计计数器的差分基线。

铁律（开发方案 §3.4 / 规格「累计计数的跨轮差分」）：

- **首轮无基线只记基线**，不产出值（不是 0，也不是假数据）；
- 时间差用**实际采集时刻之差**，不是标称采集周期；
- 累计值回退判定为**实例重启**：丢弃本轮差分（值为 None）、记 **ERROR**、重建基线。

**基线键的形状是跨模块契约**：
``{(metric_code, dimension_key): (counter_value, sampled_at_ms)}`` ——
``tpops_kafka_collector/watermark.py`` 的水位文件按同一形状存取基线，
改了这里就存不上（``WatermarkError``）。
"""
import logging
from typing import Any, Mapping, Optional, Tuple

from .rules_context import dimension_key

LOGGER = logging.getLogger(__name__)


class CounterDeltaStore:
    """累计计数器的差分基线。"""

    def __init__(self) -> None:
        self._baselines = {}  # type: Dict[Tuple[str, str], Tuple[float, int]]

    def rate(
        self,
        metric_code: str,
        dimensions: Mapping[str, Any],
        counter_value: float,
        now_ms: int,
    ) -> Tuple[Optional[float], str]:
        """返回 ``(速率, 说明)``。速率为 None 时说明原因。"""
        key = (metric_code, dimension_key(dimensions))
        previous = self._baselines.get(key)
        if previous is None:
            self._baselines[key] = (counter_value, now_ms)
            LOGGER.debug(
                '首轮无基线，仅建立基线：metric=%s dims=%s value=%s',
                metric_code, dimensions, counter_value,
            )
            return None, '首轮无基线，仅建立基线'
        last_value, last_ms = previous
        if counter_value < last_value:
            self._baselines[key] = (counter_value, now_ms)
            LOGGER.error(
                '累计计数回退，判定实例重启，丢弃本轮差分并重建基线：'
                'metric=%s dims=%s 上次=%s 本次=%s',
                metric_code, dimensions, last_value, counter_value,
            )
            return None, '累计值回退，判定实例重启，丢弃本轮差分并重建基线'
        elapsed = (now_ms - last_ms) / 1000.0
        if elapsed <= 0:
            LOGGER.warning(
                '采集时刻未推进，丢弃本轮差分：metric=%s dims=%s now=%s last=%s',
                metric_code, dimensions, now_ms, last_ms,
            )
            return None, '采集时刻未推进，丢弃本轮差分'
        self._baselines[key] = (counter_value, now_ms)
        return (counter_value - last_value) / elapsed, '按采集时刻差分'

    def forget(self, metric_code: str, dimensions: Mapping[str, Any]) -> None:
        """丢掉某个维度组合的基线（对象消失时用）。"""
        self._baselines.pop((metric_code, dimension_key(dimensions)), None)
