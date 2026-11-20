"""采集器层：五条采集通道。

每个采集器只做「取数 → 解码 → 调规则引擎 → 调 message 组装 → 交投递」，
**不自己拼 Kafka 消息**（那是 ``kafka/serializer.py`` 的唯一职责），
**也不推进水位**（水位推进是调度器的职责）。

- A 类监控指标 → ``monitor.py``
- B 类命令中心 → ``command_center.py``
- C 类诊断优化 → ``diagnostic.py``
- D 类实例元数据 → ``instance.py``
- E 类告警 → ``alarm.py``
"""

from .monitor import MonitorCollector

__all__ = ['MonitorCollector']
