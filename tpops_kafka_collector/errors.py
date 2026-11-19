# -*- coding: utf-8 -*-
"""采集端的异常体系（含「可重试 / 不可重试」语义）。

对应开发方案 §5.7。**重试判定集中在这里**，不散落到各模块：
``is_retryable(exc)`` 是唯一的判定入口，重试框架（``retry.py``）只认它。

铁律两条：

- **认证失败绝不重试** —— 平台登录失败会累计 ``login_failed_attempts`` 并锁号，
  盲目重试等于自己把账号锁死（开发方案 §2.2.9 坑 17）。
- **映射 / 协议类错误不重试** —— 响应结构与预期不符要人工介入改代码，
  重试只会把同一个坏数据重复拉一遍。
"""
from __future__ import print_function

from typing import Tuple

__all__ = [
    'AuthError',
    'MappingError',
    'ProduceError',
    'ProtocolError',
    'TransientError',
    'TpopsError',
    'is_retryable',
]


class TpopsError(RuntimeError):
    """所有平台层错误的基类。"""


class AuthError(TpopsError):
    """认证失败 —— 不可重试（重试会累计 login_failed_attempts 并锁号）。"""


class TransientError(TpopsError):
    """网络/5xx/超时 —— 可重试。"""


class ProtocolError(TpopsError):
    """平台返回结构与预期不符 —— 不可重试（需要人工介入改代码）。"""


class MappingError(TpopsError):
    """映射表与实际数据不匹配 —— 不可重试。"""


class ProduceError(TpopsError):
    """Kafka 投递失败 —— 可重试，且不推进水位。"""


# 不可重试的类型：先判它们（明确排除），再判可重试的。
NON_RETRYABLE = (AuthError, ProtocolError, MappingError)  # type: Tuple[type, ...]
RETRYABLE = (TransientError, ProduceError)  # type: Tuple[type, ...]


def is_retryable(exc: BaseException) -> bool:
    """这个异常该不该重试。

    判定表（开发方案 §5.7）::

        AuthError                        ✗
        ProtocolError                    ✗
        MappingError                     ✗
        TransientError                   ✓  指数退避 + 抖动
        ProduceError                     ✓  指数退避 + 抖动
        requests.Timeout / ConnectionError ✓  指数退避 + 抖动

    ``requests`` 的异常用类名判定，避免为了判一个异常类型就把
    ``requests`` 硬绑到这层（采集端其它模块也不一定需要它）。
    """
    if isinstance(exc, NON_RETRYABLE):
        return False
    if isinstance(exc, RETRYABLE):
        return True
    return type(exc).__name__ in ('Timeout', 'ConnectionError')
