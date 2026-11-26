"""日志的**格式与切分**部件（wave-2 决策 4 / 5）。

从 :mod:`tpops_kafka_collector.logging_setup` 里拆出来，好让那个模块
只管「装配 sink」，本模块只管「一条记录长什么样」「什么时候切文件」。

- :func:`json_format` —— loguru 的 **JSON 格式函数**（自定义 ``format=``）；
- :func:`make_rotation` —— loguru 的 ``rotation=`` 回调（跨天**或**当日超限即切）；
- :func:`make_retention` —— loguru 的 ``retention=`` 回调（保留最近 N 个自然日）。

Python 3.7 语法红线：不用海象 / 内建泛型注解 / dict 合并。
"""
import json
import os
import re
import traceback
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List

__all__ = [
    'flatten_pairs',
    'json_format',
    'logical_logger_name',
    'make_retention',
    'make_rotation',
]

#: 总日志的基名（通道日志是「基名 + 通道名」）。
TOTAL_LOG_BASE = 'collector'

#: logger 命名分层（规格要求）。
LOGGER_NAME_LIFECYCLE = 'collector.lifecycle'

#: ``key=value`` 形式的 token（只认不带空白的值）。
_PAIR_PATTERN = re.compile(r'^([A-Za-z_][A-Za-z0-9_]*)=(\S+)$')

#: 文件名里的日期（``collector-2026-11-26.log`` / 被改名的分片）。
_DATE_IN_NAME = re.compile(r'-(\d{4}-\d{2}-\d{2})(?:\.|$)')


def flatten_pairs(message: str) -> Dict[str, str]:
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


def logical_logger_name(record: Dict[str, Any]) -> str:
    """记录对应的**分层 logger 名**。

    优先级：显式绑定的 ``logger_name``（第三方库转发时带的是 stdlib 的 logger 名）
    → 通道名 → ``collector.lifecycle``。
    """
    extra = record.get('extra') or {}
    name = extra.get('logger_name')
    if name:
        return str(name)
    channel = extra.get('channel')
    if channel:
        return f'{TOTAL_LOG_BASE}.{channel}'
    return LOGGER_NAME_LIFECYCLE


def json_format(record: Dict[str, Any]) -> str:
    """loguru 的 **JSON 格式函数**（决策 4）。

    每行一个 JSON 对象：``timestamp`` / ``level`` / ``logger`` / ``thread`` /
    ``message``；整条消息是 ``k=v`` 形式时那些键值**额外**成为顶层字段
    （客户侧日志平台可直接按 ``channel`` / ``fetched`` / ``produced`` 取值）。
    异常路径带 ``exception`` 字段（堆栈不丢）。
    """
    message = record['message']
    payload = {
        'timestamp': record['time'].isoformat(),
        'level': record['level'].name,
        'logger': logical_logger_name(record),
        'thread': record['thread'].name,
        'message': message,
    }  # type: Dict[str, Any]
    payload.update(flatten_pairs(message))
    exception = record.get('exception')
    if exception is not None:
        payload['exception'] = ''.join(traceback.format_exception(*exception)).strip()
    # loguru 把自定义 format 函数的返回值**再当格式串解析一遍**：
    #   1. JSON 里的花括号要转义（否则被当成 {record_key} 占位符）；
    #   2. 行尾要自己带换行（loguru 不会替动态格式补）。
    text = json.dumps(payload, ensure_ascii=False)
    return text.replace('{', '{{').replace('}', '}}') + '\n'


# --------------------------------------------------------------------------
# 切分与保留
# --------------------------------------------------------------------------
def make_rotation(max_bytes: int) -> Callable[[str, Any], bool]:
    """构造 loguru 的 ``rotation=`` 回调（决策 5）。

    loguru 的回调返回真值 = **立即切**（不是「到点再切」），因此
    「跨天」与「当日超 ``max_bytes``」两个条件都要在回调里判断。

    :param max_bytes: 单个日志文件的大小上限（字节）。
    """
    limit = int(max_bytes)

    def _rotation(message: str, file: Any) -> bool:
        if _crossed_day(file):
            return True
        try:
            size = file.tell()
        except (OSError, ValueError):
            size = 0
        return size + len(message) > limit

    return _rotation


def _crossed_day(file: Any) -> bool:
    """当前文件是不是「昨天（或更早）的那一份」。"""
    name = os.path.basename(str(getattr(file, 'name', '') or ''))
    matched = _DATE_IN_NAME.search(name)
    if matched is None:
        return False
    return matched.group(1) != date.today().strftime('%Y-%m-%d')


def make_retention(base_name: str, days: int) -> Callable[[List[str]], None]:
    """构造 loguru 的 ``retention=`` 回调：保留最近 N 个**自然日**（含当天）。

    只清理**本基名**的文件（总日志的清理不碰通道日志，反之亦然）；
    被改名产生的同日分片（``<基名>-<日期>.<日期>_<时分秒_微秒>.log``）一并清理。
    """
    keep_days = max(1, int(days))
    pattern = re.compile(rf'^{re.escape(base_name)}-(\d{{4}}-\d{{2}}-\d{{2}})(?:\..*)?$')

    def _retention(logs: List[str]) -> None:
        cutoff = date.today() - timedelta(days=keep_days - 1)
        for path in logs:
            matched = pattern.match(os.path.basename(path))
            if matched is None:
                continue
            try:
                stamp = datetime.strptime(matched.group(1), '%Y-%m-%d').date()
            except ValueError:
                continue
            if stamp < cutoff:
                try:
                    os.remove(path)
                except OSError:
                    pass

    return _retention
