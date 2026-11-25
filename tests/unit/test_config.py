"""配置加载与启动校验的单元测试。

覆盖 specs/runtime-and-config/spec.md 的「启动期配置加载与快速失败」与
「凭据仅来自环境变量且不入日志」两条需求。
"""
import dataclasses
import socket

import pytest

from tpops_kafka_collector.config import Config, ConfigError, load_config

# 一套合法的环境变量（缺什么由各用例自己覆盖/删除）
VALID_ENV = {
    'TPOPS_USER': 'sml',
    'TPOPS_PASSWORD': 'sup3r-s3cret',
    'TPOPS_BASE_URL': 'https://172.16.103.193:8002',
    'KAFKA_BOOTSTRAP_SERVERS': '10.0.0.1:9092,10.0.0.2:9092',
}

REQUIRED_VARS = ('TPOPS_USER', 'TPOPS_PASSWORD', 'KAFKA_BOOTSTRAP_SERVERS')


def _env(**overrides):
    """以 VALID_ENV 为底，删除值为 None 的键，其余覆盖。"""
    env = dict(VALID_ENV)
    for key, value in overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return env


# --------------------------------------------------------------------------
# 正常路径
# --------------------------------------------------------------------------

def test_load_config_returns_all_defaults_for_optional_items():
    cfg = load_config(_env())

    assert cfg.tpops_base_url == 'https://172.16.103.193:8002'
    assert cfg.tpops_verify_tls is False
    assert cfg.tpops_request_timeout == 60
    assert cfg.kafka_topic_metrics == 'gaussdb.metrics'
    assert cfg.kafka_topic_alarms == 'gaussdb.alarms'
    assert cfg.kafka_topic_partitions == 4
    assert cfg.kafka_compression == 'gzip'
    assert cfg.poll_interval_seconds == 30
    assert cfg.metric_window_hours == 2
    assert cfg.metric_end_lag_minutes == 5
    assert cfg.cmd_concurrency == 2
    assert cfg.cmd_limit == 1000
    assert cfg.cmd_page_size == 100
    assert cfg.cmd_poll_timeout == 60
    assert cfg.cmd_retry_attempts == 3
    assert cfg.alarm_limit == 100
    assert cfg.alarm_retention_days == 30
    assert cfg.sql_masking_enabled is True
    assert cfg.sql_text_max_length == 512
    assert cfg.retry_max_attempts == 5
    assert cfg.retry_base_seconds == 1
    assert cfg.state_dir == './state'
    assert cfg.health_host == '127.0.0.1'
    assert cfg.health_port == 8088
    assert cfg.log_level == 'INFO'
    assert cfg.log_format == 'text'


def test_load_config_reads_every_value_from_environment():
    env = _env(
        TPOPS_USER='u1',
        TPOPS_PASSWORD='p1',
        TPOPS_BASE_URL='https://tpops.example:8002',
        TPOPS_VERIFY_TLS='true',
        TPOPS_REQUEST_TIMEOUT='90',
        TPOPS_AUTH_FAILURE_COOLDOWN_SECONDS='120',
        KAFKA_BOOTSTRAP_SERVERS='kafka1:9092,kafka2:9092',
        KAFKA_TOPIC_METRICS='m.topic',
        KAFKA_TOPIC_ALARMS='a.topic',
        KAFKA_TOPIC_PARTITIONS='8',
        KAFKA_COMPRESSION='gzip',
        POLL_INTERVAL_SECONDS='45',
        METRIC_WINDOW_HOURS='6',
        METRIC_END_LAG_MINUTES='10',
        CMD_CONCURRENCY='4',
        CMD_LIMIT='500',
        CMD_PAGE_SIZE='250',
        CMD_POLL_TIMEOUT='120',
        CMD_RETRY_ATTEMPTS='5',
        ALARM_LIMIT='50',
        ALARM_RETENTION_DAYS='7',
        SQL_MASKING_ENABLED='false',
        SQL_TEXT_MAX_LENGTH='256',
        RETRY_MAX_ATTEMPTS='7',
        RETRY_BASE_SECONDS='2.5',
        STATE_DIR='/var/lib/tpops',
        HEALTH_HOST='0.0.0.0',
        HEALTH_PORT='9090',
        LOG_LEVEL='DEBUG',
        LOG_FORMAT='json',
    )

    cfg = load_config(env)

    assert cfg.tpops_user == 'u1'
    assert cfg.tpops_password == 'p1'
    assert cfg.tpops_base_url == 'https://tpops.example:8002'
    assert cfg.tpops_verify_tls is True
    assert cfg.tpops_request_timeout == 90
    assert cfg.tpops_auth_failure_cooldown == 120
    assert cfg.kafka_bootstrap_servers == ('kafka1:9092', 'kafka2:9092')
    assert cfg.kafka_topic_metrics == 'm.topic'
    assert cfg.kafka_topic_alarms == 'a.topic'
    assert cfg.kafka_topic_partitions == 8
    assert cfg.kafka_compression == 'gzip'
    assert cfg.poll_interval_seconds == 45
    assert cfg.metric_window_hours == 6
    assert cfg.metric_end_lag_minutes == 10
    assert cfg.cmd_concurrency == 4
    assert cfg.cmd_limit == 500
    assert cfg.cmd_page_size == 250
    assert cfg.cmd_poll_timeout == 120
    assert cfg.cmd_retry_attempts == 5
    assert cfg.alarm_limit == 50
    assert cfg.alarm_retention_days == 7
    assert cfg.sql_masking_enabled is False
    assert cfg.sql_text_max_length == 256
    assert cfg.retry_max_attempts == 7
    assert cfg.retry_base_seconds == 2.5
    assert cfg.state_dir == '/var/lib/tpops'
    assert cfg.health_host == '0.0.0.0'
    assert cfg.health_port == 9090
    assert cfg.log_level == 'DEBUG'
    assert cfg.log_format == 'json'


def test_bootstrap_servers_are_split_and_trimmed():
    cfg = load_config(_env(KAFKA_BOOTSTRAP_SERVERS=' a:9092 , b:9092 ,'))

    assert cfg.kafka_bootstrap_servers == ('a:9092', 'b:9092')


def test_config_is_immutable():
    cfg = load_config(_env())

    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.metric_window_hours = 99


# --------------------------------------------------------------------------
# 必填项缺失 —— 快速失败
# --------------------------------------------------------------------------

@pytest.mark.parametrize('missing', REQUIRED_VARS)
def test_missing_required_variable_fails_fast(missing):
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(**{missing: None}))

    assert missing in str(excinfo.value)


@pytest.mark.parametrize('missing', REQUIRED_VARS)
def test_missing_required_variable_message_has_no_credential_value(missing):
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(**{missing: None}))

    message = str(excinfo.value)
    assert 'sup3r-s3cret' not in message
    assert 'sml' not in message


def test_blank_required_variable_is_treated_as_missing():
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(KAFKA_BOOTSTRAP_SERVERS='   '))

    assert 'KAFKA_BOOTSTRAP_SERVERS' in str(excinfo.value)


def test_empty_bootstrap_server_list_is_rejected():
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(KAFKA_BOOTSTRAP_SERVERS=' , '))

    assert 'KAFKA_BOOTSTRAP_SERVERS' in str(excinfo.value)


# --------------------------------------------------------------------------
# 数值范围校验
# --------------------------------------------------------------------------

@pytest.mark.parametrize('window', ['0', '-1'])
def test_metric_window_hours_below_one_is_rejected(window):
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(METRIC_WINDOW_HOURS=window))

    message = str(excinfo.value)
    assert 'METRIC_WINDOW_HOURS' in message
    assert window in message
    assert '1' in message


@pytest.mark.parametrize('concurrency', ['0', '11', '-3'])
def test_cmd_concurrency_out_of_range_is_rejected(concurrency):
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(CMD_CONCURRENCY=concurrency))

    message = str(excinfo.value)
    assert 'CMD_CONCURRENCY' in message
    assert concurrency in message
    assert '10' in message


@pytest.mark.parametrize('page_size', ['0', '1001', '5000'])
def test_cmd_page_size_above_platform_limit_is_rejected(page_size):
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(CMD_PAGE_SIZE=page_size))

    message = str(excinfo.value)
    assert 'CMD_PAGE_SIZE' in message
    assert '1000' in message


def test_alarm_limit_above_platform_limit_is_rejected():
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(ALARM_LIMIT='101'))

    message = str(excinfo.value)
    assert 'ALARM_LIMIT' in message
    assert '100' in message


def test_password_is_preserved_verbatim():
    cfg = load_config(_env(TPOPS_PASSWORD='  pad  '))

    assert cfg.tpops_password == '  pad  '


def test_non_numeric_value_is_rejected():
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(METRIC_WINDOW_HOURS='two'))

    message = str(excinfo.value)
    assert 'METRIC_WINDOW_HOURS' in message
    assert 'two' in message


def test_non_numeric_float_value_is_rejected():
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(RETRY_BASE_SECONDS='soon'))

    assert 'RETRY_BASE_SECONDS' in str(excinfo.value)


def test_non_numeric_port_is_rejected():
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(HEALTH_PORT='http'))

    assert 'HEALTH_PORT' in str(excinfo.value)


@pytest.mark.parametrize('port', ['0', '65536'])
def test_health_port_out_of_range_is_rejected(port):
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(HEALTH_PORT=port))

    assert 'HEALTH_PORT' in str(excinfo.value)


# --------------------------------------------------------------------------
# 枚举与布尔解析
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    'raw,expected',
    [
        ('true', True),
        ('TRUE', True),
        ('True', True),
        ('1', True),
        ('yes', True),
        ('on', True),
        ('false', False),
        ('FALSE', False),
        ('0', False),
        ('no', False),
        ('off', False),
    ],
)
def test_boolean_parsing(raw, expected):
    cfg = load_config(_env(TPOPS_VERIFY_TLS=raw, SQL_MASKING_ENABLED=raw))

    assert cfg.tpops_verify_tls is expected
    assert cfg.sql_masking_enabled is expected


@pytest.mark.parametrize('raw', ['maybe', '2', 'yes please'])
def test_invalid_boolean_is_rejected(raw):
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(TPOPS_VERIFY_TLS=raw))

    assert 'TPOPS_VERIFY_TLS' in str(excinfo.value)


def test_invalid_log_level_is_rejected():
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(LOG_LEVEL='VERBOSE'))

    assert 'LOG_LEVEL' in str(excinfo.value)


def test_invalid_log_format_is_rejected():
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(LOG_FORMAT='xml'))

    assert 'LOG_FORMAT' in str(excinfo.value)


def test_kafka_compression_rejects_codecs_needing_extra_packages():
    """lz4 / snappy / zstd 需要额外装带 C 扩展的 Python 包 ——
    目标机通常没有，装了也架构不通用。配置层直接拒掉，别等到启动时
    kafka-python 抛 "Libraries for lz4 compression codec not found"。"""
    for codec in ('lz4', 'snappy', 'zstd'):
        with pytest.raises(Exception) as excinfo:
            load_config(_env(KAFKA_COMPRESSION=codec))
        assert 'KAFKA_COMPRESSION' in str(excinfo.value)


def test_invalid_kafka_compression_is_rejected():
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(KAFKA_COMPRESSION='brotli'))

    assert 'KAFKA_COMPRESSION' in str(excinfo.value)


# --------------------------------------------------------------------------
# 凭据不入日志 / 不进异常消息
# --------------------------------------------------------------------------

def test_password_is_absent_from_config_repr():
    cfg = load_config(_env())

    assert 'sup3r-s3cret' not in repr(cfg)
    assert 'sup3r-s3cret' not in str(cfg)


def test_password_value_never_appears_in_error_messages():
    secret = 'p@ssw0rd-with-symbols'
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(TPOPS_PASSWORD=secret, METRIC_WINDOW_HOURS='0'))

    assert secret not in str(excinfo.value)


# --------------------------------------------------------------------------
# 启动期不发任何请求
# --------------------------------------------------------------------------

def test_load_config_opens_no_socket(monkeypatch):
    def _fail(*args, **kwargs):
        raise AssertionError('配置加载阶段不允许发起任何网络请求')

    monkeypatch.setattr(socket, 'socket', _fail)
    monkeypatch.setattr(socket, 'create_connection', _fail)

    cfg = load_config(_env())

    assert isinstance(cfg, Config)


# --------------------------------------------------------------------------
# 认证失败静默期
# --------------------------------------------------------------------------

def test_auth_failure_cooldown_defaults_to_60_seconds():
    """默认 60 秒。

    平台的认证失败会累计 ``login_failed_attempts`` 并**锁号**，
    没有静默期的话每轮轮询都会再试一次认证，等于慢性锁号。
    """
    cfg = load_config(_env())
    assert cfg.tpops_auth_failure_cooldown == 60


def test_auth_failure_cooldown_can_be_disabled_with_zero():
    """0 = 关闭静默期（允许运维明确选择「立刻重试」）。"""
    cfg = load_config(_env(TPOPS_AUTH_FAILURE_COOLDOWN_SECONDS='0'))
    assert cfg.tpops_auth_failure_cooldown == 0


def test_auth_failure_cooldown_rejects_negative_value():
    with pytest.raises(ConfigError) as excinfo:
        load_config(_env(TPOPS_AUTH_FAILURE_COOLDOWN_SECONDS='-1'))
    assert 'TPOPS_AUTH_FAILURE_COOLDOWN_SECONDS' in str(excinfo.value)
