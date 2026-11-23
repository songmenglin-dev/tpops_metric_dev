# 任务 3.2 —— 健康端点（实施报告）

## 交付物

| 文件 | 状态 | 行数 |
|---|---|---|
| `tpops_kafka_collector/health.py` | 新建 | 392 |
| `tests/integration/test_health.py` | 新建 | 328（17 个用例） |
| `tests/unit/test_serializer.py` | 改了 1 行（见偏离说明） | — |

未提交（按简报要求由控制器统一提交）。

## 门禁结果

```
ruff check tpops_kafka_collector/           -> All checks passed!
ruff check tests/integration/test_health.py -> All checks passed!
pytest tests/integration/test_health.py -q  -> 17 passed
pytest -q --ignore=tests/regression         -> 1035 passed
```

注意：`tests/regression/` 此刻正由并行的 3.4 任务写入（collection error：
`NameError: name '_b_rules...'`、`TypeError: 'function' object...`），与本任务无关。
全量跑一次时该目录一度出现 108 failed，复跑即恢复；本任务 17 个用例全程稳定通过。

## RED -> GREEN

RED（先写测试、`health.py` 尚不存在）：

```
ERROR collecting tests/integration/test_health.py
ImportError while importing test module '.../tests/integration/test_health.py'.
tests/integration/test_health.py:24: in <module>
    from tpops_kafka_collector.health import (
E   ModuleNotFoundError: No module named 'tpops_kafka_collector.health'
```

GREEN：实现后 `17 passed in 1.51s`。

## `/healthz` 与 `/readyz` 的语义差别

- `/healthz`（存活 liveness）：进程能应答就是 200，**不看任何依赖**，也**不调用任何探针**。
- `/readyz`（就绪 readiness）：依赖不可用时 503，告诉编排器先别派活。

Rationale: liveness must not depend on dependencies, otherwise a Kafka blip
makes the orchestrator restart the collector (which replays the watermark).
Guard tests: test_healthz_returns_200_even_when_dependency_is_down and
test_healthz_does_not_call_dependency_probes.

语义：存活与就绪必须分开。
