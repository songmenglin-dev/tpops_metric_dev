"""日志相关的**配置解析**（wave-2 任务 2.4）。

从 :mod:`tpops_kafka_collector.config` 里拆出来 —— 那个模块已经贴着
「单文件 < 400 行」的红线。

这里只做三件事：

1. ``LOG_FORMAT`` —— ``text`` / ``json``，或者**直接给一个 loguru 原生格式串**
   （含 ``{`` 即按格式串处理）；
2. ``LOG_PATTERN`` —— 显式覆盖 text 模式的格式（与上面等价，优先）；
3. ``LOG_DIR`` —— 日志目录：不存在就建，**不可用/不可写就报错**
   （绝不静默降级成「只写 stderr」）。

Python 3.7 语法红线：不用海象 / 内建泛型注解 / dict 合并。
"""
import os
from typing import Any, Dict, Mapping, Optional, Tuple

__all__ = [
    'LogConfigError',
    'ensure_log_dir',
    'log_fields',
    'parse_log_format',
    'prepare_log_dir',
    'read_pattern',
]


class LogConfigError(Exception):
    """日志配置不可用（目录不可写、格式不认识等）。

    由 :mod:`tpops_kafka_collector.config` 翻译成 :class:`ConfigError`
    （退出码 2）—— 本模块不依赖配置层，避免循环 import。
    """


def _raw(env: Mapping[str, str], name: str) -> Optional[str]:
    """取原始字符串；未设置或只有空白返回 ``None``。"""
    value = env.get(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def parse_log_format(
    env: Mapping[str, str],
    name: str,
    default: str,
    allowed: Tuple[str, ...],
) -> Tuple[str, Optional[str]]:
    """取日志格式 → ``(模式, 格式串)``。

    - ``text`` / ``json`` → 模式就是它，格式串为空（用内置格式）；
    - 其它含 ``{`` 的值 → **当作 loguru 原生格式串**（模式退回 ``text``）；
    - 其它值 → 拒绝（多半是拼错了）。
    """
    raw = _raw(env, name)
    if raw is None:
        return default, None
    lowered = raw.lower()
    for candidate in allowed:
        if candidate.lower() == lowered:
            return candidate, None
    if '{' in raw:
        return 'text', raw
    raise LogConfigError(
        '环境变量 {name} 的值 {value!r} 不在允许取值内'
        '（允许 {allowed}，或直接给一个 loguru 原生格式串）'.format(
            name=name, value=raw, allowed=', '.join(allowed))
    )


def read_pattern(env: Mapping[str, str], name: str) -> Optional[str]:
    """取显式的格式串覆盖（``LOG_PATTERN``）。未设置返回 ``None``。"""
    return _raw(env, name)


def ensure_log_dir(path: str) -> str:
    """确认日志目录**能用**（不存在就建），返回绝对路径。

    不可用 / 不可写 → :class:`LogConfigError`（绝不静默降级）。
    """
    resolved = os.path.abspath(os.path.expanduser(str(path)))
    try:
        os.makedirs(resolved, exist_ok=True)
    except OSError as exc:
        raise LogConfigError(f'日志目录不可用：{resolved}（{exc}）')
    if not os.path.isdir(resolved):
        raise LogConfigError(f'日志目录不是目录：{resolved}')
    if not os.access(resolved, os.W_OK):
        raise LogConfigError(f'日志目录不可写：{resolved}')
    return resolved


def prepare_log_dir(env: Mapping[str, str], name: str) -> Optional[str]:
    """取日志目录，并**当场确认它能用**（不存在就建，不可用就快速失败）。"""
    raw = _raw(env, name)
    if raw is None:
        return None
    try:
        return ensure_log_dir(raw)
    except LogConfigError as exc:
        raise LogConfigError(f'环境变量 {name} 的目录有问题：{exc}')


def log_fields(env: Mapping[str, str], allowed: Tuple[str, ...]) -> Dict[str, Any]:
    """日志相关的三项配置 → ``{'log_format', 'log_pattern', 'log_dir'}``。

    :param allowed: ``LOG_FORMAT`` 的内置取值（``text`` / ``json``）。
    """
    mode, pattern = parse_log_format(env, 'LOG_FORMAT', 'text', allowed)
    explicit = read_pattern(env, 'LOG_PATTERN')
    directory = prepare_log_dir(env, 'LOG_DIR')
    return {
        'log_format': mode,
        'log_pattern': explicit if explicit is not None else pattern,
        'log_dir': directory,
    }
