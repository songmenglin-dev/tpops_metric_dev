"""GaussDB 监控/告警数据采集端（生产包）。

**这是本项目唯一的生产代码包** —— ``tools/`` 与 ``data/`` 只作参考与回归样本，
运行时不从那里 import（见 ``changes/tpops-kafka-collector/design.md`` 决策 2）。

分层（依赖单向）::

    main → scheduler → collectors → tpops → client
                        ↘ mapping → models

- ``tpops_kafka_collector.mapping``：映射层 —— 客户口径的规则执行与响应解码；
- ``tpops_kafka_collector.ids``：幂等键的计算（告警 / 指标）；
- ``tpops_kafka_collector.errors``：异常体系与「可重试 / 不可重试」判定。
"""

__all__ = ['__version__']

__version__ = '0.1.0'
