"""SIGTERM / SIGINT 的安装与还原（开发方案 §4.6.5「优雅退出」）。

拆出来是因为 ``scheduler.py`` 越过合同「单文件 < 400 行」。

**只在主线程里能装** —— ``signal.signal`` 在子线程里抛 ``ValueError``；
装不上就记 WARNING 并照常跑（不因为装不上信号处理就不干活）。
"""
import logging
import signal
import threading
from typing import Any, Dict

LOGGER = logging.getLogger(__name__)

__all__ = ['install_signal_handlers', 'restore_signal_handlers']


def install_signal_handlers(shutdown: threading.Event) -> Dict[int, Any]:
    """装 SIGTERM / SIGINT 处理（置位 ``shutdown`` → 主循环优雅退出）。

    :returns: 原先的信号处理（用来还原）。
    """
    previous = {}
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, lambda *_: shutdown.set())
        except (ValueError, OSError) as exc:
            LOGGER.warning('装信号处理失败（不在主线程？）：%s', exc)
    return previous


def restore_signal_handlers(previous: Dict[int, Any]) -> None:
    """还原原来的信号处理。"""
    for signum, handler in previous.items():
        try:
            signal.signal(signum, handler)
        except (ValueError, OSError):  # pragma: no cover —— 退出路径
            LOGGER.debug('还原信号处理失败：%s', signum)
