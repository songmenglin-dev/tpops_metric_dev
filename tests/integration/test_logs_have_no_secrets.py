"""**落盘日志里不能有敏感信息**（wave-2 任务 2.7）。

日志文件是运维与客户侧**都能看到**的东西 —— 凭据或业务 SQL 原文进了文件
就等于泄露（工单、截图、归档备份都会把它带出去）。

这里跑一轮真实的采集流程（TPOPS 与 Kafka **全 mock**，不发起任何网络请求），
把日志落到临时目录，然后扫全部日志文件，确认里面没有：

- 平台**口令**；
- **mini-token**；
- 业务 **SQL 全文**（``select ...`` / ``json_agg`` 之类）。

扫的是**文件内容**（不是内存里的记录）—— 这才是真正会泄露出去的载体。
"""
import io
import os

import pytest

from tpops_kafka_collector.logging_setup import reset_logging, setup_logging
from tpops_kafka_collector.tpops.client import AUTH_PATH, TOKEN_HEADER, TpopsClient

PASSWORD = 'sup3r-s3cret-do-not-log'
TOKEN = 'mini-token-abcdef123456'


class FakeResponse:
    """最小化的 ``requests.Response`` 替身。"""

    def __init__(self, status_code=200, headers=None, json_body=None):
        self.status_code = status_code
        self.headers = headers if headers is not None else {}
        self._json_body = json_body
        self.text = ''
        self.content = b''

    def json(self):
        return self._json_body


class FakeSession:
    """假 Session：认证请求返回带 mini-token 的响应头。"""

    def __init__(self):
        self.calls = []

    def request(self, method, url, params=None, json=None, headers=None, timeout=None):
        self.calls.append((method, url, json))
        if url.endswith(AUTH_PATH):
            return FakeResponse(
                status_code=200,
                headers={TOKEN_HEADER: TOKEN},
                json_body={'user_id': 7, 'session_timeout': 1440},
            )
        return FakeResponse(status_code=200, json_body={'data': []})


@pytest.fixture
def log_dir(tmp_path):
    """把日志落到临时目录（用完拆掉 sink）。"""
    directory = tmp_path / 'logs'
    setup_logging('DEBUG', 'text', io.StringIO(), log_dir=str(directory))
    yield str(directory)
    reset_logging()


def _log_files(directory):
    return [
        os.path.join(directory, name) for name in sorted(os.listdir(directory))
        if name.endswith('.log')
    ]


def _all_content(directory):
    chunks = []
    for path in _log_files(directory):
        with open(path, encoding='utf-8') as handle:
            chunks.append(handle.read())
    return '\n'.join(chunks)


def test_authentication_does_not_leak_password_or_token(log_dir):
    """认证流程走一遍 —— 口令与 mini-token 都不能进日志文件。"""
    session = FakeSession()
    client = TpopsClient(
        base_url='https://tpops.example.com:8002',
        username='collector',
        password=PASSWORD,
        session=session,
    )
    client.token()  # 触发认证

    content = _all_content(log_dir)
    assert PASSWORD not in content, '口令进了日志文件'
    assert TOKEN not in content, 'mini-token 进了日志文件'
    assert '认证请求' in content, '认证流程没有留下日志 —— 用例失去意义'


def test_command_center_round_does_not_leak_sql(log_dir):
    """命令中心下发 SQL —— SQL 全文不能进日志文件。"""
    from tpops_kafka_collector.collectors.command_center import (
        CommandCenterCollector,
    )
    from tpops_kafka_collector.mapping.loader import load_mapping
    from tpops_kafka_collector.tpops.command_center_result import SqlOutcome

    class FakeCommandCenter:
        """假命令中心：把下发过的 SQL 记下来（用来比对「日志里没有它」）。"""

        def __init__(self):
            self.sql_texts = []

        def master_nodes(self, instance_id):
            return ()

        def execute_sql(self, instance_id, sql_text, limit='1000'):
            self.sql_texts.append(sql_text)
            return SqlOutcome(
                sql_text=sql_text, ok=False, tasks=(), attempts=1,
                error='平台侧失败（假）',
            )

    command_center = FakeCommandCenter()
    collector = CommandCenterCollector(
        client=object(),
        mapping=load_mapping(),
        command_center=command_center,
    )
    collector.collect(instance_id='inst-1')

    content = _all_content(log_dir)
    assert command_center.sql_texts, '这一轮没有下发 SQL —— 用例失去意义'
    for sql_text in command_center.sql_texts:
        assert sql_text not in content, 'SQL 原文进了日志文件'
    assert '命令中心取数失败' in content, '取数失败没有留下日志 —— 用例失去意义'


def test_logs_were_actually_written(log_dir):
    """日志文件真的落盘了（否则上面的「没泄露」是空断言）。"""
    client = TpopsClient(
        base_url='https://tpops.example.com:8002',
        username='collector',
        password=PASSWORD,
        session=FakeSession(),
    )
    client.token()

    files = _log_files(log_dir)
    assert files, '日志目录里没有日志文件'
    assert _all_content(log_dir).strip(), '日志文件是空的'
