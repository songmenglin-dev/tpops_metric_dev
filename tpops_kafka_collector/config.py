"""启动期配置加载与校验。

全部配置项来自环境变量（开发方案 §5.5）。本模块**只读环境变量**：
不发起任何网络请求、不读取仓库根目录下的任何文件。

设计约束：
- 加载一次，得到不可变（frozen dataclass）的 :class:`Config`；
  运行期间不再回头读环境变量。
- 必填项缺失或数值越界 → 抛 :class:`ConfigError`，调用方据此在进入主循环前快速失败。
- 凭据（``TPOPS_PASSWORD``）不进日志、不进异常消息、不进 ``repr``。
"""
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple

LOGGER = logging.getLogger(__name__)

#: 允许在异常消息中回显取值的变量。凭据类一律不在其中 ——
#: 缺失/非法时只报变量名，绝不报值。
_SECRET_VARS = frozenset({"TPOPS_PASSWORD"})

_TRUE_VALUES = frozenset({"true", "1", "yes", "on"})
_FALSE_VALUES = frozenset({"false", "0", "no", "off"})

_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
_LOG_FORMATS = ("text", "json")
_KAFKA_COMPRESSIONS = ("none", "gzip", "snappy", "lz4", "zstd")

#: 平台 `limit` 参数上限（命令中心 SQL 与告警分页共用同一上限体系）
PLATFORM_LIMIT_MAX = 1000


class ConfigError(Exception):
    """配置缺失或非法。

    消息中只包含变量名、非法取值与允许范围；凭据变量不含取值。
    """


@dataclass(frozen=True)
class Config:
    """一次性加载的运行期配置（不可变）。"""

    # --- TPOPS 平台 ---
    tpops_user: str
    tpops_password: str = field(repr=False)
    tpops_base_url: str = 'https://172.16.103.193:8002'
    tpops_verify_tls: bool = False
    tpops_request_timeout: int = 60
    #: 认证失败后的静默期（秒）。期内不再发起认证请求 ——
    #: 平台的认证失败会累计 ``login_failed_attempts`` 并**锁号**，
    #: 没有静默期的话每轮轮询都会再试一次，等于慢性锁号。
    tpops_auth_failure_cooldown: int = 60

    # --- Kafka ---
    kafka_bootstrap_servers: Tuple[str, ...] = ()
    kafka_topic_metrics: str = 'gaussdb.metrics'
    kafka_topic_alarms: str = 'gaussdb.alarms'
    kafka_topic_partitions: int = 4
    kafka_compression: str = 'lz4'

    # --- 调度与指标窗口 ---
    poll_interval_seconds: int = 30
    metric_window_hours: int = 2
    metric_end_lag_minutes: int = 5

    # --- 命令中心 ---
    cmd_concurrency: int = 2
    cmd_limit: int = PLATFORM_LIMIT_MAX
    cmd_page_size: int = 100
    cmd_poll_timeout: int = 60
    cmd_retry_attempts: int = 3

    # --- 告警 ---
    alarm_limit: int = 100
    alarm_retention_days: int = 30

    # --- 慢 SQL 脱敏 ---
    sql_masking_enabled: bool = True
    sql_text_max_length: int = 512

    # --- 通用重试 ---
    retry_max_attempts: int = 5
    retry_base_seconds: float = 1.0

    # --- 运行时 ---
    state_dir: str = './state'
    health_host: str = '127.0.0.1'
    health_port: int = 8088
    log_level: str = 'INFO'
    log_format: str = 'text'


# ---------------------------------------------------------------------------
# 取值辅助
# ---------------------------------------------------------------------------

def _raw(env: Mapping[str, str], name: str) -> Optional[str]:
    """取原始字符串；未设置或只有空白返回 ``None``。"""
    value = env.get(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def _require(env: Mapping[str, str], name: str, strip: bool = True) -> str:
    """取必填字符串；缺失即快速失败（消息只含变量名）。

    ``strip=False`` 用于凭据：密码前后空格属于密码本身，不能被裁掉。
    """
    value = env.get(name)
    if value is None or not value.strip():
        raise ConfigError(f"缺少必填环境变量：{name}")
    return value.strip() if strip else value


def _text(env: Mapping[str, str], name: str, default: str) -> str:
    """取字符串配置，缺失时用默认值。"""
    value = _raw(env, name)
    return default if value is None else value


def _int(
    env: Mapping[str, str],
    name: str,
    default: int,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    """取整数配置并校验范围。"""
    raw = _raw(env, name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"环境变量 {name} 的值 {_display(name, raw)} 不是整数")
    if _out_of_range(value, minimum, maximum):
        raise ConfigError(
            f"环境变量 {name} 的值 {_display(name, raw)} 超出允许范围 "
            f"{_range_text(minimum, maximum)}"
        )
    return value


def _float(
    env: Mapping[str, str],
    name: str,
    default: float,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> float:
    """取浮点配置并校验范围。"""
    raw = _raw(env, name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ConfigError(f"环境变量 {name} 的值 {_display(name, raw)} 不是数字")
    if _out_of_range(value, minimum, maximum):
        raise ConfigError(
            f"环境变量 {name} 的值 {_display(name, raw)} 超出允许范围 "
            f"{_range_text(minimum, maximum)}"
        )
    return value


def _bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    """取布尔配置，兼容 true/1/yes/on 与 false/0/no/off。"""
    raw = _raw(env, name)
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered in _TRUE_VALUES:
        return True
    if lowered in _FALSE_VALUES:
        return False
    raise ConfigError(
        f"环境变量 {name} 的值 {_display(name, raw)} 不是布尔值"
        "（允许 true/false/1/0/yes/no/on/off）"
    )


def _enum(env: Mapping[str, str], name: str, default: str, allowed: Tuple[str, ...]) -> str:
    """取枚举配置，值必须落在允许集合内（大小写不敏感）。"""
    raw = _raw(env, name)
    if raw is None:
        return default
    lowered = raw.lower()
    for candidate in allowed:
        if candidate.lower() == lowered:
            return candidate
    raise ConfigError(
        f"环境变量 {name} 的值 {_display(name, raw)} 不在允许取值内"
        f"（允许 {', '.join(allowed)}）"
    )


def _display(name: str, raw: str) -> str:
    """凭据变量不回显取值。"""
    if name in _SECRET_VARS:
        return '<redacted>'
    return raw


def _out_of_range(
    value: float, minimum: Optional[float], maximum: Optional[float]
) -> bool:
    """判断数值是否越界。"""
    if minimum is not None and value < minimum:
        return True
    return maximum is not None and value > maximum


def _range_text(minimum: Optional[float], maximum: Optional[float]) -> str:
    """把范围渲染成可读文本。"""
    if minimum is not None and maximum is not None:
        return f"[{minimum}, {maximum}]"
    if minimum is not None:
        return f">= {minimum}"
    if maximum is not None:
        return f"<= {maximum}"
    return '无限制'


def _servers(env: Mapping[str, str], name: str) -> Tuple[str, ...]:
    """取 Kafka 地址列表（逗号分隔）。"""
    raw = env.get(name)
    if raw is None or not raw.strip():
        raise ConfigError(f"缺少必填环境变量：{name}")
    servers = tuple(part.strip() for part in raw.split(',') if part.strip())
    if not servers:
        raise ConfigError(f"环境变量 {name} 的值不能为空")
    return servers


# ---------------------------------------------------------------------------
# 分组加载
# ---------------------------------------------------------------------------

def _tpops_fields(env: Mapping[str, str]) -> Dict[str, Any]:
    """TPOPS 平台相关配置。"""
    return {
        'tpops_user': _require(env, 'TPOPS_USER'),
        'tpops_password': _require(env, 'TPOPS_PASSWORD', strip=False),
        'tpops_base_url': _text(env, 'TPOPS_BASE_URL', 'https://172.16.103.193:8002'),
        'tpops_verify_tls': _bool(env, 'TPOPS_VERIFY_TLS', False),
        'tpops_request_timeout': _int(env, 'TPOPS_REQUEST_TIMEOUT', 60, minimum=1),
        'tpops_auth_failure_cooldown': _int(
            env, 'TPOPS_AUTH_FAILURE_COOLDOWN_SECONDS', 60, minimum=0
        ),
    }


def _kafka_fields(env: Mapping[str, str]) -> Dict[str, Any]:
    """Kafka 相关配置。"""
    return {
        'kafka_bootstrap_servers': _servers(env, 'KAFKA_BOOTSTRAP_SERVERS'),
        'kafka_topic_metrics': _text(env, 'KAFKA_TOPIC_METRICS', 'gaussdb.metrics'),
        'kafka_topic_alarms': _text(env, 'KAFKA_TOPIC_ALARMS', 'gaussdb.alarms'),
        'kafka_topic_partitions': _int(env, 'KAFKA_TOPIC_PARTITIONS', 4, minimum=1),
        'kafka_compression': _enum(env, 'KAFKA_COMPRESSION', 'lz4', _KAFKA_COMPRESSIONS),
    }


def _schedule_fields(env: Mapping[str, str]) -> Dict[str, Any]:
    """调度与指标窗口配置。"""
    return {
        'poll_interval_seconds': _int(env, 'POLL_INTERVAL_SECONDS', 30, minimum=1),
        'metric_window_hours': _int(env, 'METRIC_WINDOW_HOURS', 2, minimum=1),
        'metric_end_lag_minutes': _int(env, 'METRIC_END_LAG_MINUTES', 5, minimum=0),
    }


def _command_fields(env: Mapping[str, str]) -> Dict[str, Any]:
    """命令中心相关配置。"""
    return {
        'cmd_concurrency': _int(env, 'CMD_CONCURRENCY', 2, minimum=1, maximum=10),
        'cmd_limit': _int(env, 'CMD_LIMIT', PLATFORM_LIMIT_MAX, minimum=1,
                          maximum=PLATFORM_LIMIT_MAX),
        'cmd_page_size': _int(env, 'CMD_PAGE_SIZE', 100, minimum=1,
                              maximum=PLATFORM_LIMIT_MAX),
        'cmd_poll_timeout': _int(env, 'CMD_POLL_TIMEOUT', 60, minimum=1),
        'cmd_retry_attempts': _int(env, 'CMD_RETRY_ATTEMPTS', 3, minimum=0),
    }


def _alarm_fields(env: Mapping[str, str]) -> Dict[str, Any]:
    """告警相关配置。"""
    return {
        'alarm_limit': _int(env, 'ALARM_LIMIT', 100, minimum=1, maximum=100),
        'alarm_retention_days': _int(env, 'ALARM_RETENTION_DAYS', 30, minimum=1),
    }


def _sql_fields(env: Mapping[str, str]) -> Dict[str, Any]:
    """慢 SQL 脱敏配置。"""
    return {
        'sql_masking_enabled': _bool(env, 'SQL_MASKING_ENABLED', True),
        'sql_text_max_length': _int(env, 'SQL_TEXT_MAX_LENGTH', 512, minimum=1),
    }


def _retry_fields(env: Mapping[str, str]) -> Dict[str, Any]:
    """通用重试与退避配置。"""
    return {
        'retry_max_attempts': _int(env, 'RETRY_MAX_ATTEMPTS', 5, minimum=0),
        'retry_base_seconds': _float(env, 'RETRY_BASE_SECONDS', 1.0, minimum=0.0),
    }


def _runtime_fields(env: Mapping[str, str]) -> Dict[str, Any]:
    """状态目录、健康端点与日志配置。"""
    return {
        'state_dir': _text(env, 'STATE_DIR', './state'),
        'health_host': _text(env, 'HEALTH_HOST', '127.0.0.1'),
        'health_port': _int(env, 'HEALTH_PORT', 8088, minimum=1, maximum=65535),
        'log_level': _enum(env, 'LOG_LEVEL', 'INFO', _LOG_LEVELS),
        'log_format': _enum(env, 'LOG_FORMAT', 'text', _LOG_FORMATS),
    }


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def load_config(env: Optional[Mapping[str, str]] = None) -> Config:
    """从环境变量加载并校验全部配置。

    :param env: 环境变量映射；默认读 ``os.environ``。显式传入便于测试。
    :raises ConfigError: 必填项缺失或取值非法。
    :return: 不可变的 :class:`Config`。
    """
    source: Mapping[str, str] = os.environ if env is None else env

    fields: Dict[str, Any] = {}
    try:
        for loader in (
            _tpops_fields,
            _kafka_fields,
            _schedule_fields,
            _command_fields,
            _alarm_fields,
            _sql_fields,
            _retry_fields,
            _runtime_fields,
        ):
            fields.update(loader(source))
        return Config(**fields)
    except ConfigError as exc:
        # 消息里只含变量名/非法值/允许范围，凭据值不会出现在这里。
        LOGGER.error('配置校验失败：%s', exc)
        raise
    except TypeError as exc:
        LOGGER.error('配置装配失败：%s', exc)
        raise ConfigError(f'配置装配失败：{exc}')
