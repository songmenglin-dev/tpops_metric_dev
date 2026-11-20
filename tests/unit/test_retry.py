"""重试与退避框架的单元测试。

覆盖开发方案 §5.7 的重试判定表与 1s→2s→4s→8s 的退避序列：

- **不可重试**（AuthError / ProtocolError / MappingError / DecodeError）立即上抛，
  一次都不重试（认证失败重试会锁号）；
- **可重试**（TransientError / ProduceError / 超时类）按退避序列重试；
- **抖动可注入** —— 注入固定随机源后退避序列可断言，测试不 flaky；
- **等待可注入** —— 测试不真等。
"""
import pytest

from tpops_kafka_collector.errors import (
    AuthError,
    DecodeError,
    MappingError,
    ProduceError,
    ProtocolError,
    TransientError,
)
from tpops_kafka_collector.retry import (
    DEFAULT_MAX_ATTEMPTS,
    RetryPolicy,
    compute_delay,
    nominal_delays,
    retry,
    retry_call,
)


# --- 测试替身 -------------------------------------------------------------

class Timeout(Exception):
    """与 ``requests.exceptions.Timeout`` **同名**的替身。

    ``errors.is_retryable`` 按基类链（MRO）里的类名判定超时，
    所以替身必须同名，否则测不到 MRO 判定这条路径。
    """


class ReadTimeout(Timeout):
    """``requests`` 实际抛出的读超时 —— ``Timeout`` 的子类。"""


class FixedRandom:
    """固定随机源：``uniform`` 永远返回同一个值。"""

    def __init__(self, value=0.5):
        self.value = value
        self.calls = []

    def uniform(self, low, high):
        self.calls.append((low, high))
        return self.value


class RecordingSleep:
    """记录等待时长，不真的睡。"""

    def __init__(self):
        self.delays = []

    def __call__(self, delay):
        self.delays.append(delay)


NON_RETRYABLE_CASES = [
    (AuthError, '认证失败'),
    (ProtocolError, '响应结构与预期不符'),
    (MappingError, '映射表与实际数据不匹配'),
    (DecodeError, '合并 SQL 的 result 不是合法 JSON'),
]


# --- 基本行为 -------------------------------------------------------------

def test_successful_call_returns_result_without_waiting():
    sleeper = RecordingSleep()
    calls = []

    def ok():
        calls.append(1)
        return 'value'

    assert retry_call(ok, sleep=sleeper) == 'value'
    assert len(calls) == 1
    assert sleeper.delays == []


def test_arguments_are_forwarded_to_the_callable():
    sleeper = RecordingSleep()

    def add(first, second=0):
        return first + second

    assert retry_call(add, 1, second=2, sleep=sleeper) == 3


# --- 不可重试：立即上抛 ----------------------------------------------------

@pytest.mark.parametrize('error_type, message', NON_RETRYABLE_CASES)
def test_non_retryable_errors_are_raised_immediately(error_type, message):
    sleeper = RecordingSleep()
    calls = []

    def fail():
        calls.append(1)
        raise error_type(message)

    with pytest.raises(error_type):
        retry_call(fail, sleep=sleeper)

    assert len(calls) == 1
    assert sleeper.delays == []


def test_decode_error_is_not_retried_even_though_it_is_a_mapping_error():
    sleeper = RecordingSleep()
    calls = []

    def fail():
        calls.append(1)
        raise DecodeError('解不出来')

    with pytest.raises(DecodeError):
        retry_call(fail, sleep=sleeper)

    assert len(calls) == 1
    assert sleeper.delays == []


# --- 可重试：退避重试 ------------------------------------------------------

def test_transient_error_is_retried_until_success():
    sleeper = RecordingSleep()
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise TransientError('平台 503')
        return 'ok'

    result = retry_call(flaky, sleep=sleeper, random_source=FixedRandom(0.0))

    assert result == 'ok'
    assert len(attempts) == 3
    assert sleeper.delays == [1.0, 2.0]


def test_produce_error_is_retried():
    sleeper = RecordingSleep()
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 2:
            raise ProduceError('Kafka 投递失败')
        return 'ok'

    assert retry_call(flaky, sleep=sleeper, random_source=FixedRandom(0.0)) == 'ok'
    assert len(attempts) == 2
    assert sleeper.delays == [1.0]


def test_timeout_subclass_is_retried():
    """requests 抛的是 ``ReadTimeout``（``Timeout`` 的子类）—— 必须走 MRO 判定。"""
    sleeper = RecordingSleep()
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 2:
            raise ReadTimeout('读超时')
        return 'ok'

    assert retry_call(flaky, sleep=sleeper, random_source=FixedRandom(0.0)) == 'ok'
    assert len(attempts) == 2


def test_requests_read_timeout_is_retried_when_requests_is_available():
    requests = pytest.importorskip('requests')
    sleeper = RecordingSleep()
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 2:
            raise requests.exceptions.ReadTimeout('读超时')
        return 'ok'

    assert retry_call(flaky, sleep=sleeper, random_source=FixedRandom(0.0)) == 'ok'
    assert len(attempts) == 2


def test_subclass_of_transient_error_is_retried():
    class PlatformBusy(TransientError):
        """平台繁忙 —— 自定义的可重试子类。"""

    sleeper = RecordingSleep()
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 2:
            raise PlatformBusy('平台繁忙')
        return 'ok'

    assert retry_call(flaky, sleep=sleeper, random_source=FixedRandom(0.0)) == 'ok'
    assert len(attempts) == 2


def test_exhausted_retries_reraise_the_last_error():
    sleeper = RecordingSleep()
    calls = []

    def always_fail():
        calls.append(1)
        raise TransientError('平台 503')

    policy = RetryPolicy(max_attempts=3)
    with pytest.raises(TransientError):
        retry_call(always_fail, policy=policy, sleep=sleeper, random_source=FixedRandom(0.0))

    assert len(calls) == 3
    assert sleeper.delays == [1.0, 2.0]


def test_max_attempts_of_one_means_no_retry():
    sleeper = RecordingSleep()
    calls = []

    def always_fail():
        calls.append(1)
        raise TransientError('平台 503')

    policy = RetryPolicy(max_attempts=1)
    with pytest.raises(TransientError):
        retry_call(always_fail, policy=policy, sleep=sleeper, random_source=FixedRandom(0.0))

    assert len(calls) == 1
    assert sleeper.delays == []


# --- 退避序列 -------------------------------------------------------------

def test_default_backoff_sequence_is_1_2_4_8_seconds():
    policy = RetryPolicy()
    assert nominal_delays(policy) == (1.0, 2.0, 4.0, 8.0)
    assert policy.max_attempts == DEFAULT_MAX_ATTEMPTS == 5


def test_nominal_delays_follow_the_configured_factor():
    policy = RetryPolicy(max_attempts=6, base_delay=0.5, factor=3.0, max_delay=100.0)
    assert nominal_delays(policy) == (0.5, 1.5, 4.5, 13.5, 40.5)


def test_delay_is_capped_by_max_delay():
    policy = RetryPolicy(max_attempts=6, base_delay=1.0, factor=10.0, max_delay=30.0)
    assert policy.nominal_delay(0) == 1.0
    assert policy.nominal_delay(1) == 10.0
    assert policy.nominal_delay(3) == 30.0
    assert nominal_delays(policy) == (1.0, 10.0, 30.0, 30.0, 30.0)


# --- 抖动 -----------------------------------------------------------------

def test_jitter_is_deterministic_with_an_injected_random_source():
    policy = RetryPolicy(jitter_ratio=0.5)
    delay = compute_delay(policy, 0, FixedRandom(0.5))
    assert delay == pytest.approx(1.25)


def test_jitter_never_shortens_the_nominal_delay():
    policy = RetryPolicy(jitter_ratio=0.2)
    for value in (0.0, 0.25, 1.0):
        delay = compute_delay(policy, 2, FixedRandom(value))
        assert delay >= policy.nominal_delay(2)
        assert delay <= policy.nominal_delay(2) * 1.2


def test_jitter_ratio_of_zero_disables_jitter_entirely():
    policy = RetryPolicy(jitter_ratio=0.0)
    assert compute_delay(policy, 3, FixedRandom(1.0)) == 8.0


def test_default_random_source_is_compatible_with_the_random_module():
    import random

    policy = RetryPolicy(jitter_ratio=0.5)
    delay = compute_delay(policy, 0, random)
    assert 1.0 <= delay <= 1.5


def test_random_source_receives_a_unit_interval():
    source = FixedRandom(0.5)
    compute_delay(RetryPolicy(jitter_ratio=0.5), 0, source)
    assert source.calls == [(0.0, 1.0)]


def test_negative_attempt_index_is_rejected():
    with pytest.raises(ValueError):
        RetryPolicy().nominal_delay(-1)
    with pytest.raises(ValueError):
        compute_delay(RetryPolicy(), -1, FixedRandom(0.0))


# --- 策略校验 -------------------------------------------------------------

@pytest.mark.parametrize('kwargs', [
    {'max_attempts': 0},
    {'max_attempts': -1},
    {'base_delay': -1.0},
    {'factor': 0.5},
    {'jitter_ratio': -0.1},
    {'jitter_ratio': 1.5},
    {'max_delay': 0.0},
])
def test_invalid_policy_is_rejected(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


# --- 装饰器与日志 ---------------------------------------------------------

def test_decorator_applies_the_same_retry_policy():
    sleeper = RecordingSleep()
    attempts = []

    @retry(policy=RetryPolicy(max_attempts=3), sleep=sleeper, random_source=FixedRandom(0.0))
    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise TransientError('平台 503')
        return 'ok'

    assert flaky() == 'ok'
    assert len(attempts) == 3
    assert sleeper.delays == [1.0, 2.0]


def test_retry_is_logged_with_the_waiting_time(caplog):
    sleeper = RecordingSleep()

    def always_fail():
        raise TransientError('平台 503')

    policy = RetryPolicy(max_attempts=2)
    with caplog.at_level('WARNING'):
        with pytest.raises(TransientError):
            retry_call(always_fail, policy=policy, sleep=sleeper, random_source=FixedRandom(0.0))

    assert any('重试' in record.getMessage() for record in caplog.records)
