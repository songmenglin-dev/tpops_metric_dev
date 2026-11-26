"""日志装配（loguru）—— 采集端**唯一**的日志出口。

设计要点（wave-2 / 决策 2~6）：

1. **sink 分层**（决策 2）

   | sink | 收什么 | 落点 |
   |---|---|---|
   | 总日志 | 生命周期、tick 整体汇总、每通道一行简洁汇总、WARNING 以上 | ``collector-<日期>.log`` |
   | 通道 sink ×5 | 该通道明细 + 该通道每轮汇总行 | ``collector-<通道>-<日期>.log`` |
   | stderr | 与总日志同内容（前台可见） | 终端 |

   通道归属用 ``logger.bind(channel="…")`` 打在记录上，通道 sink 的 ``filter=``
   读 ``record["extra"]["channel"]``；**未绑定 channel 的记录只进总日志**。
2. **格式**（决策 4）：text 用 loguru 原生格式（可用 ``log_pattern`` 覆盖）；
   json 用 ``format=`` **自定义函数**，产出 ``timestamp`` / ``level`` / ``logger`` /
   ``thread`` / ``message``，整条消息是 ``k=v`` 形式时摊平成顶层字段，异常带
   ``exception`` 字段。
3. **切分与保留**（决策 5）：``rotation=`` 自定义回调（跨天**或**当日超
   ``LOG_MAX_BYTES`` → 立即切）；``retention=`` 保留最近 N 个自然日（含当天）。

第三方库（``kafka`` / ``urllib3`` / ``requests``）的 stdlib 日志经
:class:`InterceptHandler` 转发进 loguru；降噪放在 stdlib 侧（压到 WARNING），
**不**用 loguru 的 ``disable()`` —— 那会把它们的 WARNING 一起丢掉。

Python 3.7 语法红线：不用海象 / 内建泛型注解 / dict 合并。
"""
import inspect
import logging
import os
import sys
from typing import Any, Callable, Dict, List, Optional, TextIO

from loguru import logger

from . import log_config
from .logging_formats import (
    LOGGER_NAME_LIFECYCLE,
    TOTAL_LOG_BASE,
    flatten_pairs,
    json_format,
    logical_logger_name,
    make_retention,
    make_rotation,
)

__all__ = [
    'CHANNEL_NAMES',
    'LOGGER_NAME_LIFECYCLE',
    'LOGGER_NAME_SUMMARY',
    'DEFAULT_LOG_MAX_BYTES',
    'DEFAULT_LOG_RETENTION_DAYS',
    'HANDLER_MARKER',
    'LogSetupError',
    'NOISY_LOGGERS',
    'TOTAL_LOG_BASE',
    'flatten_pairs',
    'json_format',
    'logical_logger_name',
    'make_retention',
    'make_rotation',
    'reset_logging',
    'setup_logging',
    'setup_logging_from_config',
]

#: 5 个采集通道（与 schedule.py 的通道常量一一对应）。
CHANNEL_NAMES = ('monitor', 'command_center', 'diagnostic', 'instance', 'alarm')

#: 单个日志文件的大小上限（默认 50 MB）。
DEFAULT_LOG_MAX_BYTES = 50 * 1024 * 1024

#: 保留天数（默认 3 个自然日，含当天）。
DEFAULT_LOG_RETENTION_DAYS = 3

#: 标记「本模块装上去的 handler」—— 幂等靠它认领自己的 handler。
HANDLER_MARKER = '_tpops_collector_handler'

#: 标记「这条 stdlib 记录是 loguru 转过去的」—— 防 loguru → stdlib → loguru 回环。
BRIDGE_MARKER = '_tpops_from_loguru'

#: logger 命名分层（规格要求）。
LOGGER_NAME_SUMMARY = 'collector.summary'

#: 需要降噪的第三方库 → 级别（压到 WARNING，WARNING 仍要能进 loguru）。
NOISY_LOGGERS = {
    'kafka': logging.WARNING,
    'kafka.conn': logging.WARNING,
    'kafka.cluster': logging.WARNING,
    'urllib3': logging.WARNING,
    'requests': logging.WARNING,
}

#: 本模块装上的 loguru sink id（重复装配时先拆掉旧的）。
_STATE = {'handler_ids': []}  # type: Dict[str, List[int]]


class LogSetupError(RuntimeError):
    """日志装配失败（日志目录不可用等）。"""


# --------------------------------------------------------------------------
# sink 分流
# --------------------------------------------------------------------------
def _total_filter(record: Dict[str, Any]) -> bool:
    """总日志 / stderr 的过滤器。

    未绑定 channel 的记录（生命周期、汇总）**只进总日志**；
    绑定 channel 的记录只有 WARNING 及以上才进总日志（明细不重复）。
    """
    extra = record.get('extra') or {}
    if extra.get('channel') is None:
        return True
    return record['level'].no >= logging.WARNING


def _channel_filter(channel: str) -> Callable[[Dict[str, Any]], bool]:
    """通道 sink 的过滤器：只看绑定到本通道的记录。"""

    def _filter(record: Dict[str, Any]) -> bool:
        extra = record.get('extra') or {}
        return extra.get('channel') == channel

    return _filter


def _file_path(directory: str, base_name: str, channel: Optional[str] = None) -> str:
    """日志文件路径模板（日期由 loguru 的 ``{time:YYYY-MM-DD}`` 展开）。"""
    name = base_name if channel is None else f'{base_name}-{channel}'
    return os.path.join(directory, f'{name}-{{time:YYYY-MM-DD}}.log')


# --------------------------------------------------------------------------
# stdlib → loguru
# --------------------------------------------------------------------------
class InterceptHandler(logging.Handler):
    """把 stdlib 日志转发进 loguru（loguru 官方文档里的写法）。

    第三方库（``kafka`` / ``urllib3`` / ``requests``）的日志走这条路，
    之后与自家日志同格式、同 sink。
    """

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(record, BRIDGE_MARKER, False):
            return  # loguru → stdlib → loguru 的回环，直接掐掉
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        # 找到**真正发日志的那一帧**（跳过 logging 模块内部的帧），
        # 这样 loguru 记录里的模块名:行号 指向调用方，而不是本模块。
        frame, depth = inspect.currentframe(), 1
        frame = frame.f_back
        while frame is not None and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.bind(logger_name=record.name).opt(
            depth=depth, exception=record.exc_info).log(level, record.getMessage())


def _install_root_interceptor(numeric_level: int) -> None:
    """把 stdlib 根 logger 接到 loguru 上（**不叠加**）。"""
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, HANDLER_MARKER, False):
            root.removeHandler(handler)
    handler = InterceptHandler()
    setattr(handler, HANDLER_MARKER, True)
    root.addHandler(handler)
    root.setLevel(numeric_level)


def _quiet_noisy_loggers() -> None:
    """把第三方库的日志级别压下去（防刷屏）。"""
    for name, level in NOISY_LOGGERS.items():
        logging.getLogger(name).setLevel(level)


# --------------------------------------------------------------------------
# 装配
# --------------------------------------------------------------------------
def _level_value(level: Any) -> int:
    """把级别名转成 ``logging`` 的数值；认不出就用 INFO。"""
    if isinstance(level, int):
        return level
    value = getattr(logging, str(level or '').upper(), None)
    if isinstance(value, int):
        return value
    return logging.INFO


def _make_format(log_format: str, log_pattern: Optional[str]) -> Any:
    """选格式：json 用自定义函数，text 用 loguru 原生格式（可被覆盖）。"""
    if str(log_format or '').lower() == 'json':
        return json_format
    if log_pattern:
        return log_pattern
    return None


def _prepare_log_dir(log_dir: str) -> str:
    """准备日志目录：不存在就建，不可用就**报错**（不静默降级）。

    校验逻辑与配置层共用 :func:`tpops_kafka_collector.log_config.ensure_log_dir`
    —— 两处各写一份的话，改了一处另一处就会不一致。
    """
    try:
        return log_config.ensure_log_dir(log_dir)
    except log_config.LogConfigError as exc:
        raise LogSetupError(str(exc))


def _remove_default_handler() -> None:
    """拆掉 loguru 自带的默认 stderr handler（否则每条日志会重复一份）。"""
    try:
        logger.remove(0)
    except ValueError:
        pass


def reset_logging() -> None:
    """拆掉本模块装上的 sink 与根 handler（重装 / 测试用）。"""
    for handler_id in _STATE['handler_ids']:
        try:
            logger.remove(handler_id)
        except ValueError:
            pass
    _STATE['handler_ids'] = []
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, HANDLER_MARKER, False):
            root.removeHandler(handler)


def setup_logging(
    level: Any = 'INFO',
    log_format: str = 'text',
    stream: Optional[TextIO] = None,
    log_dir: Optional[str] = None,
    log_max_bytes: int = DEFAULT_LOG_MAX_BYTES,
    log_retention_days: int = DEFAULT_LOG_RETENTION_DAYS,
    log_pattern: Optional[str] = None,
) -> List[int]:
    """装配 loguru 的 sink（**幂等** —— 重复调用不叠加）。

    :param level: 日志级别（``DEBUG`` / ``INFO`` / …）。
    :param log_format: ``text`` 或 ``json``。
    :param stream: stderr sink 的输出流；缺省是 ``sys.stderr``。
    :param log_dir: 日志目录；缺省（或 ``None``）= **只写 stderr，不落盘**。
    :param log_max_bytes: 单个日志文件的大小上限。
    :param log_retention_days: 保留天数（自然日，含当天）。
    :param log_pattern: 覆盖 text 模式的格式（loguru 原生字符串语法）。
    :returns: 装上的 sink id。
    """
    reset_logging()
    _remove_default_handler()
    numeric = _level_value(level)
    _install_root_interceptor(numeric)
    _quiet_noisy_loggers()

    sink_format = _make_format(log_format, log_pattern)
    format_kwargs = {} if sink_format is None else {'format': sink_format}
    handler_ids = []  # type: List[int]

    handler_ids.append(logger.add(
        stream if stream is not None else sys.stderr,
        level=level, filter=_total_filter, colorize=False, **format_kwargs))

    if log_dir:
        directory = _prepare_log_dir(log_dir)
        rotation = make_rotation(log_max_bytes)
        handler_ids.append(logger.add(
            _file_path(directory, TOTAL_LOG_BASE),
            level=level, filter=_total_filter, rotation=rotation,
            retention=make_retention(TOTAL_LOG_BASE, log_retention_days),
            colorize=False, encoding='utf-8', **format_kwargs))
        for channel in CHANNEL_NAMES:
            handler_ids.append(logger.add(
                _file_path(directory, TOTAL_LOG_BASE, channel),
                level=level, filter=_channel_filter(channel), rotation=rotation,
                retention=make_retention(
                    f'{TOTAL_LOG_BASE}-{channel}', log_retention_days),
                colorize=False, encoding='utf-8', **format_kwargs))

    _STATE['handler_ids'] = handler_ids
    return handler_ids


def setup_logging_from_config(
    config: Any, stream: Optional[TextIO] = None
) -> List[int]:
    """按 :class:`tpops_kafka_collector.config.Config` 初始化日志。

    鸭子类型取属性 —— 配置层与日志层不必互相 import。
    """
    return setup_logging(
        getattr(config, 'log_level', 'INFO'),
        getattr(config, 'log_format', 'text'),
        stream=stream,
        log_dir=getattr(config, 'log_dir', None),
        log_max_bytes=getattr(config, 'log_max_bytes', DEFAULT_LOG_MAX_BYTES),
        log_retention_days=getattr(config, 'log_retention_days', DEFAULT_LOG_RETENTION_DAYS),
        log_pattern=getattr(config, 'log_pattern', None),
    )
