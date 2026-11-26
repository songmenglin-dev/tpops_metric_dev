"""loguru sink 装配的守卫（wave-2 任务 2.2）。

守护的是**文件层面的行为**（不是 API 形状）：

- 6 个基名 × 日期后缀的文件布局（总日志 + 5 个通道日志）；
- 通道分流：通道的 INFO 明细只在通道日志里，总日志不出现通道内部明细；
  通道的 ERROR 两边都有；
- 切分：跨天切、单日超限改名；
- 清理：保留最近 N 个自然日（含当天），被改名的同日分片一并清理；
- 目录不可写 → 启动失败（不静默降级为「只写 stderr」）；
- 既有 ``setup_logging(level, log_format, stream)`` 签名与幂等语义不回归；
- JSON 模式的 schema、``k=v`` 摊平与 ``exception`` 字段。
"""

import io
import json
import logging
import os
import re
from datetime import date, timedelta

import pytest
from loguru import logger

from tpops_kafka_collector import logging_setup

TODAY = date.today().strftime('%Y-%m-%d')
SHARD_PATTERN = re.compile(
    r'^collector(-\w+)?-\d{4}-\d{2}-\d{2}\.\d{4}-\d{2}-\d{2}'
    r'_\d{2}-\d{2}-\d{2}_\d{6}\.log$')


@pytest.fixture(autouse=True)
def _reset_sinks():
    """每个用例结束后拆掉 sink（否则文件句柄与 sink 会跨用例堆积）。"""
    yield
    logging_setup.reset_logging()


def _read(path):
    with open(path, encoding='utf-8') as handle:
        return handle.read()


def _setup(tmp_path, level='INFO', log_format='text', **kwargs):
    stream = io.StringIO()
    logging_setup.setup_logging(level, log_format, stream, log_dir=str(tmp_path), **kwargs)
    return stream


def test_creates_six_daily_files(tmp_path):
    """6 个基名 × 日期后缀。"""
    _setup(tmp_path)
    assert sorted(os.listdir(str(tmp_path))) == sorted([
        f'collector-{TODAY}.log',
        f'collector-monitor-{TODAY}.log',
        f'collector-command_center-{TODAY}.log',
        f'collector-diagnostic-{TODAY}.log',
        f'collector-instance-{TODAY}.log',
        f'collector-alarm-{TODAY}.log',
    ])


def test_channel_detail_only_reaches_its_own_channel_file(tmp_path):
    """通道的 INFO 明细只在通道日志里，总日志不出现通道内部明细。"""
    _setup(tmp_path)
    logger.bind(channel='monitor').info('monitor 明细')
    logger.info('生命周期消息')

    channel_log = _read(str(tmp_path / f'collector-monitor-{TODAY}.log'))
    total_log = _read(str(tmp_path / f'collector-{TODAY}.log'))
    assert 'monitor 明细' in channel_log
    assert 'monitor 明细' not in total_log
    assert '生命周期消息' in total_log


def test_channel_records_do_not_leak_into_other_channel_files(tmp_path):
    """通道日志不串味。"""
    _setup(tmp_path)
    logger.bind(channel='monitor').info('monitor 的活')
    other = _read(str(tmp_path / f'collector-command_center-{TODAY}.log'))
    assert 'monitor 的活' not in other


def test_channel_error_appears_in_both_channel_and_total_log(tmp_path):
    """通道的 WARNING 及以上级别同时进通道日志与总日志。"""
    _setup(tmp_path)
    logger.bind(channel='alarm').error('告警通道炸了')

    channel_log = _read(str(tmp_path / f'collector-alarm-{TODAY}.log'))
    total_log = _read(str(tmp_path / f'collector-{TODAY}.log'))
    assert '告警通道炸了' in channel_log
    assert '告警通道炸了' in total_log


def test_total_log_carries_channel_summary_rows(tmp_path):
    """总日志里有每个通道一行的简洁汇总（用 collector.summary 命名）。"""
    _setup(tmp_path)
    logger.bind(logger_name='collector.summary').info('channel=monitor fetched=3 produced=3')
    logger.bind(channel='monitor', logger_name='collector.summary').info(
        'channel=monitor fetched=3 produced=3')

    total_log = _read(str(tmp_path / f'collector-{TODAY}.log'))
    channel_log = _read(str(tmp_path / f'collector-monitor-{TODAY}.log'))
    assert 'channel=monitor' in total_log
    assert 'channel=monitor' in channel_log


def test_rotation_renames_file_when_single_day_limit_exceeded(tmp_path):
    """单日超限 → 旧文件被改名（<基名>-<日期>.<日期>_<时分秒_微秒>.log）。

    同名的新文件继续写。
    """
    _setup(tmp_path, log_max_bytes=200)
    for _ in range(50):
        logger.info('x' * 30)

    names = os.listdir(str(tmp_path))
    assert f'collector-{TODAY}.log' in names
    shards = [n for n in names if SHARD_PATTERN.match(n)]
    assert shards, f'超限后没产生被改名的分片：{names}'


def test_rotation_callback_cuts_when_day_changed(tmp_path):
    """跨天条件在回调里判断（loguru 的回调返回真值 = 立即切）。"""
    rotation = logging_setup.make_rotation(1024 * 1024)
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    old_file = tmp_path / f'collector-{yesterday}.log'
    old_file.write_text('x', encoding='utf-8')
    with open(str(old_file), encoding='utf-8') as handle:
        assert rotation('hello', handle) is True


def test_rotation_callback_keeps_file_within_limit(tmp_path):
    """当天且未超限 → 不切。"""
    rotation = logging_setup.make_rotation(1024 * 1024)
    current = tmp_path / f'collector-{TODAY}.log'
    current.write_text('x', encoding='utf-8')
    with open(str(current), encoding='utf-8') as handle:
        assert rotation('hello', handle) is False


def test_rotation_callback_cuts_when_size_exceeded(tmp_path):
    """当日文件超限 → 切。"""
    rotation = logging_setup.make_rotation(10)
    current = tmp_path / f'collector-{TODAY}.log'
    current.write_text('x' * 100, encoding='utf-8')
    with open(str(current), 'r+', encoding='utf-8') as handle:
        handle.seek(0, 2)
        assert rotation('hello', handle) is True


def test_retention_removes_files_older_than_window(tmp_path):
    """保留最近 3 个自然日（含当天）；更早的删除；同名分片一并清理。"""
    old = (date.today() - timedelta(days=5)).strftime('%Y-%m-%d')
    expired = tmp_path / f'collector-{old}.log'
    shard = tmp_path / f'collector-{old}.{old}_00-00-00_000000.log'
    kept = tmp_path / f'collector-{TODAY}.log'
    other_channel = tmp_path / f'collector-monitor-{old}.log'
    for path in (expired, shard, kept, other_channel):
        path.write_text('x', encoding='utf-8')

    logging_setup.make_retention('collector', 3)([
        str(expired), str(shard), str(kept), str(other_channel)])

    assert not expired.exists()
    assert not shard.exists()
    assert kept.exists()
    assert other_channel.exists(), '总日志的清理不该动别的基名'


def test_retention_runs_when_file_rotates(tmp_path):
    """切分时顺带做保留期清理。"""
    old = (date.today() - timedelta(days=5)).strftime('%Y-%m-%d')
    expired = tmp_path / f'collector-{old}.log'
    expired.write_text('x', encoding='utf-8')

    _setup(tmp_path, log_max_bytes=200)
    for _ in range(50):
        logger.info('y' * 30)

    assert not expired.exists()


def test_unusable_log_dir_raises(tmp_path):
    """日志目录不可用 → 报错并说明路径（不静默降级）。"""
    blocker = tmp_path / 'not-a-dir'
    blocker.write_text('x', encoding='utf-8')
    with pytest.raises(logging_setup.LogSetupError) as excinfo:
        logging_setup.setup_logging('INFO', 'text', io.StringIO(),
                                    log_dir=str(blocker / 'logs'))
    assert 'logs' in str(excinfo.value)


def test_legacy_signature_still_accepted():
    """既有签名 setup_logging(level, log_format, stream) 不回归。"""
    stream = io.StringIO()
    logging_setup.setup_logging('INFO', 'text', stream)
    logger.info('老签名也能用')
    assert '老签名也能用' in stream.getvalue()


def test_repeated_setup_does_not_stack_sinks(tmp_path):
    """重复调用不叠加 sink（同一条日志只出现一次）。"""
    stream = io.StringIO()
    logging_setup.setup_logging('INFO', 'text', stream, log_dir=str(tmp_path))
    logging_setup.setup_logging('INFO', 'text', stream, log_dir=str(tmp_path))
    logger.info('只出现一次')
    assert stream.getvalue().count('只出现一次') == 1

    owned = [h for h in logging.getLogger().handlers
             if getattr(h, logging_setup.HANDLER_MARKER, False)]
    assert len(owned) == 1


def test_text_format_is_loguru_native():
    """text 模式用 loguru 原生格式（时间 / 级别 / 模块名 / 消息）。"""
    stream = io.StringIO()
    logging_setup.setup_logging('INFO', 'text', stream)
    logger.info('原生格式')
    line = stream.getvalue().strip()
    assert '原生格式' in line
    assert 'INFO' in line
    assert 'test_logging_files' in line


def test_custom_pattern_override():
    """LOG_PATTERN 覆盖成 loguru 原生语法。"""
    stream = io.StringIO()
    logging_setup.setup_logging('INFO', 'text', stream,
                                log_pattern='{level} | {message}')
    logger.info('自定义')
    assert stream.getvalue().strip() == 'INFO | 自定义'


def test_json_format_has_schema_and_flattens_pairs(tmp_path):
    """json 模式：既有 schema + k=v 摊平成顶层字段。"""
    _setup(tmp_path, log_format='json')
    logger.bind(channel='monitor').info('channel=monitor fetched=3 produced=3')
    content = _read(str(tmp_path / f'collector-monitor-{TODAY}.log'))
    payload = json.loads(content.strip())
    assert payload['level'] == 'INFO'
    assert payload['logger'] == 'collector.monitor'
    assert payload['thread']
    assert payload['message'] == 'channel=monitor fetched=3 produced=3'
    assert payload['channel'] == 'monitor'
    assert payload['fetched'] == '3'
    assert payload['produced'] == '3'


def test_json_format_keeps_exception():
    """异常路径带 exception 字段（堆栈不丢）。"""
    stream = io.StringIO()
    logging_setup.setup_logging('INFO', 'json', stream)
    try:
        raise ValueError('boom')
    except ValueError:
        logger.exception('处理失败了')
    payload = json.loads(stream.getvalue().strip())
    assert 'ValueError: boom' in payload['exception']


def test_third_party_stdlib_log_is_forwarded_into_loguru():
    """第三方库的 stdlib 日志经 InterceptHandler 进 loguru 的 sink。"""
    stream = io.StringIO()
    logging_setup.setup_logging('INFO', 'text', stream)
    logging.getLogger('kafka').warning('kafka 客户端告警')
    assert 'kafka 客户端告警' in stream.getvalue()
