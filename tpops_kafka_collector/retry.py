"""重试与退避框架（开发方案 §5.7）。

职责边界 —— **判定与等待分开**：

- **判定**在 :func:`tpops_kafka_collector.errors.is_retryable`（唯一真相源）。
  本模块**不再**按类名重复判定一遍：``requests`` 实际抛出的是
  ``ReadTimeout`` / ``ConnectTimeout``，它们是 ``Timeout`` /
  ``ConnectionError`` 的**子类**，只有按基类链（MRO）判定才能把它们
  判成可重试；按类名精确相等会把最常见的瞬时故障判反。
- **等待**在本模块：指数退避 **1s → 2s → 4s → 8s** + 抖动。

两条铁律（开发方案 §2.2.9 坑 17 / §5.7）：

- **认证失败绝不重试** —— 平台登录失败会累计 ``login_failed_attempts``
  并锁号，盲目重试等于自己把账号锁死；
- **映射 / 协议类错误不重试** —— 响应结构与预期不符要人工介入改代码，
  重试只会把同一个坏数据重复拉一遍。

两个注入点（测试用，避免 flaky 与真等）：

- ``sleep``：默认 :func:`time.sleep`；测试注入记录器，不真的等；
- ``random_source``：默认 :mod:`random` 模块（需提供 ``uniform(a, b)``）；
  测试注入固定值 → 退避序列可断言。

抖动口径：**只增不减** —— 实际等待 = 基准退避 × (1 + ``jitter_ratio`` × u)，
u ∈ [0, 1] 来自随机源。这样「至少等满基准退避」恒成立，且在
``jitter_ratio = 0`` 时退避序列完全确定。
"""
import functools
import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Tuple

from .errors import is_retryable

LOGGER = logging.getLogger(__name__)

__all__ = [
    'DEFAULT_BASE_DELAY',
    'DEFAULT_FACTOR',
    'DEFAULT_JITTER_RATIO',
    'DEFAULT_MAX_ATTEMPTS',
    'DEFAULT_MAX_DELAY',
    'DEFAULT_POLICY',
    'RetryPolicy',
    'compute_delay',
    'nominal_delays',
    'retry',
    'retry_call',
]

#: 首次重试等待的基准秒数。
DEFAULT_BASE_DELAY = 1.0
#: 退避倍数 —— 1s → 2s → 4s → 8s。
DEFAULT_FACTOR = 2.0
#: 总尝试次数（含首次调用）。5 = 首次 + 4 次重试。
DEFAULT_MAX_ATTEMPTS = 5
#: 抖动幅度占基准退避的比例（0 ~ 1）。0.1 = 最多多等 10%。
DEFAULT_JITTER_RATIO = 0.1
#: 单次等待的上限（秒）。
DEFAULT_MAX_DELAY = 30.0


@dataclass(frozen=True)
class RetryPolicy:
    """重试策略（不可变）。

    :param max_attempts: 总尝试次数，含首次调用；``1`` 表示不重试。
    :param base_delay: 首次重试的基准等待秒数。
    :param factor: 指数退避倍数。
    :param jitter_ratio: 抖动幅度（占基准退避的比例），``0`` 关闭抖动。
    :param max_delay: 单次等待上限（秒）。
    """

    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    base_delay: float = DEFAULT_BASE_DELAY
    factor: float = DEFAULT_FACTOR
    jitter_ratio: float = DEFAULT_JITTER_RATIO
    max_delay: float = DEFAULT_MAX_DELAY

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError('max_attempts 至少为 1（1 表示不重试）')
        if self.base_delay < 0:
            raise ValueError('base_delay 不能为负')
        if self.factor < 1:
            raise ValueError('factor 不能小于 1')
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError('jitter_ratio 必须落在 [0, 1] 区间')
        if self.max_delay <= 0:
            raise ValueError('max_delay 必须为正')

    def nominal_delay(self, attempt: int) -> float:
        """第 ``attempt`` 次重试（0 起）的**基准**等待秒数（不含抖动）。

        指数退避并以 ``max_delay`` 封顶。
        """
        if attempt < 0:
            raise ValueError('attempt 不能为负')
        return min(self.base_delay * (self.factor ** attempt), self.max_delay)


#: 默认策略 —— 1s → 2s → 4s → 8s。
DEFAULT_POLICY = RetryPolicy()


def nominal_delays(policy: RetryPolicy = DEFAULT_POLICY) -> Tuple[float, ...]:
    """该策略下全部重试的基准等待秒数（不含抖动）。

    默认策略返回 ``(1.0, 2.0, 4.0, 8.0)``。
    """
    return tuple(policy.nominal_delay(index) for index in range(policy.max_attempts - 1))


def compute_delay(
    policy: RetryPolicy,
    attempt: int,
    random_source: Any = random,
) -> float:
    """第 ``attempt`` 次重试的**实际**等待秒数（基准退避 + 抖动）。

    抖动只增不减：实际等待 ∈ [基准退避, 基准退避 × (1 + jitter_ratio)]。

    :param random_source: 提供 ``uniform(low, high)`` 的随机源，
        默认 :mod:`random` 模块；测试注入固定值即可断言退避序列。
    """
    nominal = policy.nominal_delay(attempt)
    if policy.jitter_ratio == 0:
        return nominal
    ratio = random_source.uniform(0.0, 1.0)
    return nominal + nominal * policy.jitter_ratio * ratio


def retry_call(
    func: Callable[..., Any],
    *args: Any,
    policy: RetryPolicy = DEFAULT_POLICY,
    sleep: Callable[[float], None] = time.sleep,
    random_source: Any = random,
    description: str = '',
    **kwargs: Any
) -> Any:
    """调用 ``func``，可重试异常按退避策略重试。

    行为：

    - 成功 → 直接返回结果；
    - 异常**不可重试**（AuthError / ProtocolError / MappingError 等）→
      **立即上抛**，一次都不重试；
    - 异常可重试 → 按 ``policy`` 的退避序列等待后重试；
    - 重试次数用尽 → **原样上抛最后一次的异常**（不换类型，
      调用方仍能按类型分支处理）。

    ``policy`` / ``sleep`` / ``random_source`` / ``description`` 是**关键字
    专属**参数，不透传给 ``func``；``func`` 自己若有同名形参，
    请改用 :func:`retry` 装饰器。
    """
    label = description or getattr(func, '__name__', repr(func))
    attempts = 0
    while True:
        attempts += 1
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # noqa: B902 —— 判定后原样上抛
            if not is_retryable(exc):
                raise
            if attempts >= policy.max_attempts:
                LOGGER.error(
                    '重试次数用尽，放弃：%s（%s：%s）',
                    label, type(exc).__name__, exc,
                )
                raise
            delay = compute_delay(policy, attempts - 1, random_source)
            LOGGER.warning(
                '第 %s/%s 次重试 %s，等待 %.3f 秒：%s：%s',
                attempts, policy.max_attempts, label, delay, type(exc).__name__, exc,
            )
            sleep(delay)


def retry(
    policy: RetryPolicy = DEFAULT_POLICY,
    sleep: Callable[[float], None] = time.sleep,
    random_source: Any = random,
    description: str = '',
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """把重试策略包成装饰器 —— 与 :func:`retry_call` 同一套语义。

    用法::

        @retry(policy=RetryPolicy(max_attempts=3))
        def fetch(url):
            ...
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return retry_call(
                func,
                *args,
                policy=policy,
                sleep=sleep,
                random_source=random_source,
                description=description or getattr(func, '__name__', ''),
                **kwargs
            )

        return wrapper

    return decorator
