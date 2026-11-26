"""日志框架规范文档（``docs/日志框架规范.md``）与代码的一致性守卫。

文档是给运维看的，**代码是真相**。文档里的字段名 / 基名 / 环境变量名一旦与代码
脱节，运维照着文档做的排障就是白做 —— 所以这里逐项钉死。

RED 先行：先写这些断言（文档还不存在 / 名字对不上）→ 再写文档 → 通过。
"""
from __future__ import absolute_import

import io
import os
import re

import pytest

from conftest import REPO_ROOT

from tpops_kafka_collector.logging_formats import TOTAL_LOG_BASE  # noqa: E402
from tpops_kafka_collector.logging_setup import CHANNEL_NAMES  # noqa: E402
from tpops_kafka_collector.scheduler_report import (  # noqa: E402
    CHANNEL_SUMMARY_FORMAT,
)

DOC_PATH = os.path.join(REPO_ROOT, 'docs', '日志框架规范.md')

#: 汇总行的 11 个字段（与规格「每通道每轮汇总行」一致）。
SUMMARY_FIELDS = (
    'channel', 'tick', 'instances', 'fetched', 'mapped', 'produced',
    'failed', 'windows', 'calls', 'elapsed', 'delivery_rate',
)

#: 文档必须写清的环境变量。
LOG_ENV_VARS = (
    'LOG_DIR', 'LOG_FORMAT', 'LOG_PATTERN', 'LOG_MAX_BYTES',
    'LOG_RETENTION_DAYS',
)


def _doc():
    assert os.path.isfile(DOC_PATH), '缺少日志框架规范文档：%s' % DOC_PATH
    with io.open(DOC_PATH, encoding='utf-8') as handle:
        return handle.read()


def test_the_summary_format_has_exactly_the_eleven_fields():
    """代码里的汇总格式就是 11 个字段 —— 文档要照着它写。"""
    found = re.findall(r'(\w+)=\{', CHANNEL_SUMMARY_FORMAT)
    assert tuple(found) == SUMMARY_FIELDS


def test_the_document_lists_every_summary_field():
    """文档里 11 个字段名一个都不能少（逐个断言，别用模糊匹配）。"""
    text = _doc()
    missing = [name for name in SUMMARY_FIELDS if name not in text]
    assert not missing, '日志框架规范缺这些字段：%s' % missing


def test_the_document_shows_the_summary_field_table():
    """字段要有**表格行**（`| `channel` | ...`），不是只在样例里出现一次。"""
    text = _doc()
    for name in SUMMARY_FIELDS:
        assert re.search(r'\|\s*`?%s`?\s*\|' % re.escape(name), text), \
            '日志框架规范里 %s 没有字段表行' % name


def test_the_document_lists_every_log_base_name():
    """6 个基名（总日志 + 5 通道）都要出现在文档里。"""
    text = _doc()
    bases = [TOTAL_LOG_BASE] + [
        '%s-%s' % (TOTAL_LOG_BASE, channel) for channel in CHANNEL_NAMES
    ]
    missing = [name for name in bases if name not in text]
    assert not missing, '日志框架规范缺这些日志基名：%s' % missing


def test_the_document_lists_every_log_env_var():
    """日志相关的环境变量名要与配置层一致。"""
    text = _doc()
    missing = [name for name in LOG_ENV_VARS if name not in text]
    assert not missing, '日志框架规范缺这些环境变量：%s' % missing


def test_the_document_explains_the_background_run_commands():
    """后台运行的三条命令与 systemd 的写法都要写清。"""
    text = _doc()
    for needle in ('collector.sh run', 'run --foreground',
                   'collector.sh status', 'collector.sh stop',
                   'collector.pid'):
        assert needle in text, '日志框架规范没写 %s' % needle


def test_the_document_states_the_retention_and_size_limits():
    """保留天数与单日上限的默认值要写对（与配置层默认值一致）。"""
    from tpops_kafka_collector import config

    text = _doc()
    assert str(config.DEFAULT_LOG_RETENTION_DAYS) in text, \
        '日志框架规范没写默认保留天数'
    assert str(config.DEFAULT_LOG_MAX_BYTES) in text, \
        '日志框架规范没写默认单日上限'


def test_the_document_warns_about_the_sensitive_data_red_line():
    """敏感信息红线必须在文档里（口令 / mini-token / SQL 文本）。"""
    text = _doc()
    for needle in ('口令', 'mini-token', 'SQL'):
        assert needle in text, '日志框架规范没提 %s 这条红线' % needle
