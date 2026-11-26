# -*- coding: utf-8 -*-
"""pytest 公共夹具。"""
import logging
import os
import sys

import pytest
from loguru import logger

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS_DIR = os.path.join(REPO_ROOT, 'tools')
for _path in (REPO_ROOT, TOOLS_DIR):
    if _path not in sys.path:
        sys.path.insert(0, _path)

DATA_DIR = os.path.join(REPO_ROOT, 'data')
#: 仓库根的映射表目录 —— **仅供离线工具与测试**使用
#: （``MappingTable`` 是离线工具专用类，要读 ``archive/`` 与实测留档，
#: 它们不在包内 ``mapping/data/``）。生产代码不得引用这里。
MAPPING_DIR = os.path.join(DATA_DIR, 'mapping')
XLSX_PATH = os.path.join(
    REPO_ROOT, '高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx'
)


def _forward_to_stdlib(message):
    """把一条 loguru 记录转成 stdlib 记录（给 caplog 看）。"""
    record = message.record
    name = record['name']
    level = record['level'].no
    stdlib_logger = logging.getLogger(name)
    if not stdlib_logger.isEnabledFor(level):
        return
    stdlib_record = stdlib_logger.makeRecord(
        name, level, record['file'].path, record['line'], record['message'], (),
        record.get('exception'))
    # 标记「这条记录是 loguru 转过来的」——
    # logging_setup 的 InterceptHandler 见到它就跳过，不会 loguru → stdlib → loguru 回环。
    setattr(stdlib_record, '_tpops_from_loguru', True)
    stdlib_logger.handle(stdlib_record)


@pytest.fixture(autouse=True)
def _bridge_loguru_to_stdlib():
    """让 ``caplog`` 仍然能看到采集端的日志。

    wave-2 起生产代码全用 loguru，而 pytest 的 ``caplog`` 只认 stdlib logging ——
    这里加一层桥接：loguru 的记录按原模块名、级别、消息转成 stdlib 记录，
    ``caplog.at_level`` / ``caplog.records`` / ``caplog.text`` 的语义不变。
    """
    handler_id = logger.add(_forward_to_stdlib, format='{message}', level=0, catch=False)
    yield
    logger.remove(handler_id)
