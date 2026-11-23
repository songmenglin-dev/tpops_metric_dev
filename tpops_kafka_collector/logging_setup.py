"""日志的**中心化初始化**（任务 3.3）。

``config.py`` 里早就有 ``log_level`` 与 ``log_format``（支持 ``text`` / ``json``），
但在本模块之前**没有任何代码消费它们** —— 全包没有一处 ``basicConfig`` /
``Formatter`` / ``Handler``。后果是 Python 的 ``lastResort`` 兜底：

- ``DEBUG`` / ``INFO`` 日志**全丢**；
- ``WARNING`` / ``ERROR`` 输出了，但是**裸格式** —— 没有时间戳、没有级别、
  没有模块名，事后排查没法用。

本模块把那两项配置**真的用起来**，并守住三条硬要求：

1. **幂等** —— ``basicConfig(force=…)`` 是 3.8+ 才有的，py37 自己判
   ``root.handlers``：重复调用**不得叠加 handler**；
2. **输出到 stderr** —— 常驻进程的标准做法（日志轮转是运维的事，
   本模块不自己写文件）；
3. **敏感信息不落日志** —— 凭据 / mini-token / 业务 SQL 文本一律不进日志
   （本模块只负责格式，不负责内容；内容侧的审查在采集器里做）。

另有两件顺带的事：

- **第三方库降噪** —— ``kafka`` / ``urllib3`` / ``requests`` 压到 WARNING；
- **JSON 格式下把 ``key=value`` 汇总行摊平成顶层字段** —— 每轮汇总日志的
  字段名（``tick`` / ``elapsed`` / ``fetched`` / …）由
  :mod:`tpops_kafka_collector.scheduler_report` 定死，客户侧日志平台要能
  直接按字段取，不能只塞在一段 ``message`` 字符串里。
"""
import json
import logging
import re
import sys
from typing import Any, Dict, Optional, TextIO

__all__ = [
    'JsonLogFormatter',
    'NOISY_LOGGERS',
    'TEXT_FORMAT',
    'setup_logging',
    'setup_logging_from_config',
]

LOGGER = logging.getLogger(__name__)

#: 标记「本模块装上去的 handler」—— 幂等靠它认领自己的 handler。
HANDLER_MARKER = '_tpops_collector_handler'

#: text 格式：时间 | 级别 | 模块 | 消息。
TEXT_FORMAT = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'

#: 需要降噪的第三方库 → 级别。
NOISY_LOGGERS = {
    'kafka': logging.WARNING,
    'kafka.conn': logging.WARNING,
    'kafka.cluster': logging.WARNING,
    'urllib3': logging.WARNING,
    'requests': logging.WARNING,
}

#: ``key=value`` 形式的 token（只认不带空白的值）。
_PAIR_PATTERN = re.compile(r'^([A-Za-z_][A-Za-z0-9_]*)=(\S+)$')


def _level_value(level: Any) -> int:
    """把级别名转成 ``logging`` 的数值；认不出就用 INFO。"""
    if isinstance(level, int):
        return level
    value = getattr(logging, str(level or '').upper(), None)
    if isinstance(value, int):
        return value
    return logging.INFO


def _flatten_pairs(message: str) -> Dict[str, str]:
    """把 ``k=v k=v`` 形式的整条消息摊平成字典。

    **只对「整条消息的每个 token 都是 ``k=v``」的情况生效** ——
    普通的自然语言消息（含空格）原样不动，避免误伤。
    """
    tokens = message.split()
    if not tokens:
        return {}
    pairs = {}  # type: Dict[str, str]
    for token in tokens:
        matched = _PAIR_PATTERN.match(token)
        if matched is None:
            return {}
        pairs[matched.group(1)] = matched.group(2)
    return pairs


class JsonLogFormatter(logging.Formatter):
    """每行一个 JSON 对象的格式化器。

    字段：``timestamp`` / ``level`` / ``logger`` / ``message``；
    消息整体是 ``k=v`` 形式时，那些键值**额外**成为顶层字段
    （每轮汇总日志的 ``tick`` / ``elapsed`` / … 就是这样被摊平的）。
    异常路径带 ``exception`` 字段（**堆栈不丢**）。
    """

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        payload = {
            'timestamp': self.formatTime(record, '%Y-%m-%dT%H:%M:%S%z'),
            'level': record.levelname,
            'logger': record.name,
            'message': message,
        }  # type: Dict[str, Any]
        payload.update(_flatten_pairs(message))
        if record.exc_info:
            payload['exception'] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def _make_formatter(log_format: str) -> logging.Formatter:
    """按配置选格式化器。"""
    if str(log_format or '').lower() == 'json':
        return JsonLogFormatter()
    return logging.Formatter(TEXT_FORMAT)


def _owned_handler(root: logging.Logger) -> Optional[logging.Handler]:
    """找本模块之前装上去的 handler。"""
    for handler in root.handlers:
        if getattr(handler, HANDLER_MARKER, False):
            return handler
    return None


def _quiet_noisy_loggers() -> None:
    """把第三方库的日志级别压下去（防刷屏）。"""
    for name, level in NOISY_LOGGERS.items():
        logging.getLogger(name).setLevel(level)


def setup_logging(
    level: Any = 'INFO',
    log_format: str = 'text',
    stream: Optional[TextIO] = None,
) -> logging.Handler:
    """初始化根 logger（**幂等**）。

    :param level: 日志级别（``DEBUG`` / ``INFO`` / …）。
    :param log_format: ``text`` 或 ``json``。
    :param stream: 输出流；缺省是 ``sys.stderr``。
    :returns: 装上的 handler（调用方一般不需要用）。
    """
    root = logging.getLogger()
    handler = _owned_handler(root)
    if handler is not None and stream is not None and handler.stream is not stream:
        # 调用方换了输出流 → 换掉自己的 handler（**不叠加**）。
        root.removeHandler(handler)
        handler.close()
        handler = None
    if handler is None:
        handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
        setattr(handler, HANDLER_MARKER, True)
        root.addHandler(handler)
    numeric = _level_value(level)
    handler.setLevel(numeric)
    handler.setFormatter(_make_formatter(log_format))
    root.setLevel(numeric)
    _quiet_noisy_loggers()
    return handler


def setup_logging_from_config(
    config: Any, stream: Optional[TextIO] = None
) -> logging.Handler:
    """按 :class:`tpops_kafka_collector.config.Config` 初始化日志。

    鸭子类型取属性 —— 配置层与日志层不必互相 import。
    """
    return setup_logging(
        getattr(config, 'log_level', 'INFO'),
        getattr(config, 'log_format', 'text'),
        stream=stream,
    )
