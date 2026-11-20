# -*- coding: utf-8 -*-
"""TPOPS HTTP 客户端的单元测试。

全部用假 Session，**不连真实平台**。

守的是几条「实测踩过的坑」：

- mini-token 在**响应头** ``X-Mini-Token``，不在 body 里；
- 认证失败**不发起第二次认证**（平台会累计 ``login_failed_attempts`` 并锁号）；
- 业务请求收到 401 → 强制刷新 token 并**只重试一次**；
- 平台有两套路径命名空间，少一段前缀就是 404 → 不可重试的协议错误；
- 自签证书 → 默认 ``verify_tls=False``；
- 错误四分类：401/403→AuthError、5xx/超时→TransientError、结构异常→ProtocolError；
- 凭据不进日志、不进异常消息。
"""
from __future__ import absolute_import

import collections
import json
import logging

import pytest
import requests

from tpops_kafka_collector.errors import (
    AuthError,
    ProtocolError,
    TransientError,
    is_retryable,
)
from tpops_kafka_collector.tpops.client import (
    AUTH_PATH,
    GAUSSDB_V3,
    RDS_V3,
    RDS_V3_2,
    TOKEN_HEADER,
    TpopsClient,
    require_fields,
)

BASE_URL = 'https://tpops.example.com:8002'
USER = 'collector'
PASSWORD = 'super-secret-password'


Call = collections.namedtuple(
    'Call', 'method url params json headers timeout'
)


class FakeResponse(object):
    """最小化的 ``requests.Response`` 替身。"""

    def __init__(self, status_code=200, headers=None, json_body=None, text=None):
        self.status_code = status_code
        self.headers = headers if headers is not None else {}
        self._json_body = json_body
        if text is None:
            text = '' if json_body is None else json.dumps(json_body)
        self.text = text

    def json(self):
        if self._json_body is None:
            raise ValueError('响应体不是合法 JSON')
        return self._json_body


class FakeSession(object):
    """按队列返回预置响应，并记录全部请求。"""

    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.calls = []
        self.verify = True

    def request(self, method, url, params=None, json=None, headers=None, timeout=None):
        self.calls.append(
            Call(method, url, params, json, headers, timeout)
        )
        if not self.responses:
            raise AssertionError('FakeSession 没有预置响应，但收到了请求：%s %s' % (method, url))
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    # -- 断言辅助 --

    def urls(self):
        return [call.url for call in self.calls]

    def auth_calls(self):
        return [call for call in self.calls if call.url.endswith(AUTH_PATH)]

    def business_calls(self):
        return [call for call in self.calls if not call.url.endswith(AUTH_PATH)]


def make_client(session, **kwargs):
    params = {
        'base_url': BASE_URL,
        'username': USER,
        'password': PASSWORD,
        'session': session,
    }
    params.update(kwargs)
    return TpopsClient(**params)


def auth_ok(token='token-1', session_timeout=1440):
    """一次成功的认证响应（token 在**响应头**里）。"""
    return FakeResponse(
        status_code=200,
        headers={TOKEN_HEADER: token},
        json_body={'user_id': 7, 'session_timeout': session_timeout},
    )


# ---------------------------------------------------------------------------
# mini-token 认证
# ---------------------------------------------------------------------------

def test_token_is_read_from_response_header_not_body():
    """token 在响应头；body 里即使有同名字段也不取。"""
    session = FakeSession([
        FakeResponse(
            status_code=200,
            headers={TOKEN_HEADER: 'header-token'},
            json_body={'token': 'body-token', 'session_timeout': 1440},
        ),
    ])
    client = make_client(session)

    assert client.token() == 'header-token'
    assert session.auth_calls()[0].url == BASE_URL + AUTH_PATH
    assert session.auth_calls()[0].json == {'username': USER, 'password': PASSWORD}


def test_authentication_body_carries_credentials_and_no_token_in_body():
    """认证请求体是 username/password；客户端不把 token 写回任何 body。"""
    session = FakeSession([auth_ok()])
    client = make_client(session)
    client.token()

    call = session.auth_calls()[0]
    assert call.json['username'] == USER
    assert call.json['password'] == PASSWORD
    assert TOKEN_HEADER not in (call.json or {})


def test_token_is_cached_between_calls():
    """token 有效期内不重复认证。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=200, json_body={'total_count': 0}),
        FakeResponse(status_code=200, json_body={'total_count': 0}),
    ])
    client = make_client(session)

    client.get(RDS_V3_2 + '/instances')
    client.get(RDS_V3_2 + '/instances')

    assert len(session.auth_calls()) == 1
    assert len(session.business_calls()) == 2


def test_token_is_refreshed_before_expiry():
    """到期前主动刷新（有效期 = session_timeout 分钟 - 60 秒）。"""
    clock = {'now': 1000.0}
    session = FakeSession([
        auth_ok(token='token-1', session_timeout=1440),
        auth_ok(token='token-2', session_timeout=1440),
        FakeResponse(status_code=200, json_body={}),
    ])
    client = make_client(session, clock=lambda: clock['now'])

    assert client.token() == 'token-1'
    # 1440 分钟 - 60 秒 = 86340 秒
    clock['now'] += 86340
    assert client.token() == 'token-2'
    assert len(session.auth_calls()) == 2


def test_authentication_success_without_token_header_is_protocol_error():
    """200 但没有 X-Mini-Token 响应头 → ProtocolError（平台行为变了）。"""
    session = FakeSession([FakeResponse(status_code=200, json_body={'user_id': 7})])
    client = make_client(session)

    with pytest.raises(ProtocolError):
        client.token()
    assert not is_retryable(ProtocolError())


# ---------------------------------------------------------------------------
# 认证失败不重试（防锁号）
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('status', [400, 401, 403, 500])
def test_authentication_failure_raises_auth_error_and_never_retries(status):
    """认证接口非 2xx → AuthError，且**只发一次**认证请求。"""
    session = FakeSession([FakeResponse(status_code=status, json_body={})])
    client = make_client(session)

    with pytest.raises(AuthError):
        client.get(RDS_V3_2 + '/instances')

    assert len(session.auth_calls()) == 1, '认证失败后又发了一次认证请求（会累计 login_failed_attempts）'
    assert session.business_calls() == [], '认证失败后不应再发业务请求'


def test_authentication_failure_does_not_issue_second_authentication_attempt():
    """显式契约断言：认证失败后不会再来一次认证。"""
    session = FakeSession([FakeResponse(status_code=401, json_body={})])
    client = make_client(session)

    for _ in range(3):
        with pytest.raises(AuthError):
            client.token()

    assert len(session.auth_calls()) == 1


def test_authentication_failure_message_contains_no_password():
    """异常消息里没有密码。"""
    session = FakeSession([FakeResponse(status_code=401, json_body={})])
    client = make_client(session)

    with pytest.raises(AuthError) as excinfo:
        client.token()

    assert PASSWORD not in str(excinfo.value)


def test_authentication_failure_is_not_retryable():
    """AuthError 不可重试 —— 锁号风险。"""
    assert is_retryable(AuthError()) is False


# ---------------------------------------------------------------------------
# 401 强制刷新并重试一次
# ---------------------------------------------------------------------------

def test_401_triggers_token_refresh_and_retries_once():
    """业务请求 401 → 刷新 token → 重试一次并成功。"""
    session = FakeSession([
        auth_ok(token='token-1'),
        FakeResponse(status_code=401, json_body={}),
        auth_ok(token='token-2'),
        FakeResponse(status_code=200, json_body={'total_count': 3}),
    ])
    client = make_client(session)

    payload = client.get(RDS_V3_2 + '/instances')

    assert payload == {'total_count': 3}
    assert len(session.auth_calls()) == 2
    assert len(session.business_calls()) == 2
    first_token = session.business_calls()[0].headers[TOKEN_HEADER]
    second_token = session.business_calls()[1].headers[TOKEN_HEADER]
    assert first_token == 'token-1'
    assert second_token == 'token-2'


def test_repeated_401_raises_auth_error_after_exactly_one_retry():
    """一直 401 → 重试一次后抛 AuthError，不再重试。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=401, json_body={}),
        auth_ok(token='token-2'),
        FakeResponse(status_code=401, json_body={}),
    ])
    client = make_client(session)

    with pytest.raises(AuthError):
        client.get(RDS_V3_2 + '/instances')

    assert len(session.business_calls()) == 2
    assert len(session.auth_calls()) == 2


def test_403_does_not_trigger_a_refresh():
    """403 是授权失败，不刷新 token，直接 AuthError。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=403, json_body={}),
    ])
    client = make_client(session)

    with pytest.raises(AuthError):
        client.get(RDS_V3_2 + '/instances')

    assert len(session.auth_calls()) == 1
    assert len(session.business_calls()) == 1


# ---------------------------------------------------------------------------
# 路径命名空间
# ---------------------------------------------------------------------------

def test_both_namespaces_are_preserved_in_url():
    """两套前缀各自成路径，少一段前缀不会被客户端悄悄补上。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=200, json_body={}),
        FakeResponse(status_code=200, json_body={}),
    ])
    client = make_client(session)

    client.get(RDS_V3 + '/monitor/metrics/series')
    client.get(GAUSSDB_V3 + '/instances/abc/key-view-execute-node')

    assert session.business_calls()[0].url == BASE_URL + '/rds/v3/monitor/metrics/series'
    assert session.business_calls()[1].url == (
        BASE_URL + '/rds/gaussdb/v3/instances/abc/key-view-execute-node'
    )


def test_404_is_protocol_error_not_retryable():
    """漏掉 /gaussdb 段 → 404 → 不可重试的协议错误（不是「接口无数据」）。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=404, json_body={}),
    ])
    client = make_client(session)

    with pytest.raises(ProtocolError):
        client.get(GAUSSDB_V3 + '/instances/abc/xxx')
    assert is_retryable(ProtocolError()) is False


# ---------------------------------------------------------------------------
# 错误四分类
# ---------------------------------------------------------------------------

def test_5xx_raises_transient_error():
    """5xx → TransientError，可重试。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=503, json_body={}),
    ])
    client = make_client(session)

    with pytest.raises(TransientError):
        client.get(RDS_V3_2 + '/instances')
    assert is_retryable(TransientError()) is True


def test_read_timeout_raises_transient_error():
    """超时（ReadTimeout 是 Timeout 的子类）→ TransientError。"""
    session = FakeSession([
        auth_ok(),
        requests.exceptions.ReadTimeout('read timed out'),
    ])
    client = make_client(session)

    with pytest.raises(TransientError):
        client.get(RDS_V3_2 + '/instances')


def test_connection_error_raises_transient_error():
    """连接错误 → TransientError。"""
    session = FakeSession([
        auth_ok(),
        requests.exceptions.ConnectionError('connection refused'),
    ])
    client = make_client(session)

    with pytest.raises(TransientError):
        client.get(RDS_V3_2 + '/instances')


def test_malformed_json_body_raises_protocol_error():
    """200 但响应体不是 JSON → ProtocolError。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=200, text='<html>oops</html>'),
    ])
    client = make_client(session)

    with pytest.raises(ProtocolError):
        client.get(RDS_V3_2 + '/instances')


def test_missing_field_raises_protocol_error_naming_endpoint_and_field():
    """缺字段 → ProtocolError，消息里带接口与缺失字段名。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=200, json_body={'total_count': 1}),
    ])
    client = make_client(session)

    payload = client.get(RDS_V3_2 + '/instances')

    with pytest.raises(ProtocolError) as excinfo:
        require_fields(payload, ['instances'], RDS_V3_2 + '/instances')

    message = str(excinfo.value)
    assert 'instances' in message
    assert RDS_V3_2 + '/instances' in message


def test_empty_body_is_treated_as_empty_object():
    """空 body（HTTP 200，响应体为空）→ {}。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=200, text=''),
    ])
    client = make_client(session)

    assert client.get(RDS_V3_2 + '/instances') == {}


# ---------------------------------------------------------------------------
# TLS / 配置
# ---------------------------------------------------------------------------

def test_verify_tls_defaults_to_false_for_self_signed_platform():
    """平台用自签证书 → 默认不校验。"""
    session = FakeSession([])
    client = make_client(session)
    assert session.verify is False


def test_verify_tls_can_be_enabled():
    """verify_tls=True 时透传给 Session。"""
    session = FakeSession([])
    client = make_client(session, verify_tls=True)
    assert session.verify is True


def test_client_requires_username_and_password():
    """缺凭据直接快速失败，消息里不回显取值。"""
    session = FakeSession([])
    with pytest.raises(ValueError):
        make_client(session, username='')
    with pytest.raises(ValueError):
        make_client(session, password='')


def test_client_from_config_uses_configured_values():
    """从 Config 装配：地址、账号、密码、TLS、超时全部取自配置。"""
    from tpops_kafka_collector.config import Config

    session = FakeSession([auth_ok()])
    config = Config(
        tpops_user=USER,
        tpops_password=PASSWORD,
        tpops_base_url=BASE_URL,
        tpops_verify_tls=False,
        tpops_request_timeout=42,
    )

    client = TpopsClient.from_config(config, session=session)
    client.token()

    assert session.auth_calls()[0].timeout == 42


# ---------------------------------------------------------------------------
# 凭据不外泄
# ---------------------------------------------------------------------------

def test_password_never_appears_in_logs(caplog):
    """日志里没有密码，也没有 token。"""
    session = FakeSession([
        auth_ok(token='super-secret-token'),
        FakeResponse(status_code=401, json_body={}),
        auth_ok(token='super-secret-token-2'),
        FakeResponse(status_code=200, json_body={'ok': True}),
    ])
    client = make_client(session)

    with caplog.at_level(logging.DEBUG):
        client.get(RDS_V3_2 + '/instances')

    assert caplog.text, '客户端应当记录请求日志'
    assert PASSWORD not in caplog.text
    assert 'super-secret-token' not in caplog.text


def test_password_never_appears_in_exception_messages():
    """业务失败抛出的异常里没有密码。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=500, json_body={}),
    ])
    client = make_client(session)

    with pytest.raises(TransientError) as excinfo:
        client.get(RDS_V3_2 + '/instances')

    assert PASSWORD not in str(excinfo.value)


def test_repr_of_client_does_not_leak_password():
    """repr 也不泄露密码。"""
    session = FakeSession([])
    client = make_client(session)
    assert PASSWORD not in repr(client)


# ---------------------------------------------------------------------------
# 认证失败静默期（防锁号）与其它分支
# ---------------------------------------------------------------------------

def test_auth_transport_failure_is_auth_error_and_not_retried():
    """认证请求本身的传输层失败也**不可重试**（平台可能已计一次失败登录）。"""
    session = FakeSession([requests.exceptions.ConnectionError('connection refused')])
    client = make_client(session)

    with pytest.raises(AuthError):
        client.token()

    assert len(session.auth_calls()) == 1
    assert is_retryable(AuthError()) is False


def test_auth_failure_enters_cooldown_and_sends_no_second_request():
    """认证失败进入静默期：期内再要 token 直接复用失败结论，**不发请求**。"""
    clock = {'now': 500.0}
    session = FakeSession([FakeResponse(status_code=401, json_body={})])
    client = make_client(session, clock=lambda: clock['now'])

    with pytest.raises(AuthError):
        client.token()

    clock['now'] += 10.0
    with pytest.raises(AuthError):
        client.token()

    assert len(session.auth_calls()) == 1


def test_auth_failure_cooldown_expires_and_allows_one_more_attempt():
    """静默期过了才允许下一次认证（配合调度器的轮询退避）。"""
    clock = {'now': 500.0}
    session = FakeSession([
        FakeResponse(status_code=401, json_body={}),
        auth_ok(token='token-after-cooldown'),
    ])
    client = make_client(session, clock=lambda: clock['now'])

    with pytest.raises(AuthError):
        client.token()

    clock['now'] += 61.0
    assert client.token() == 'token-after-cooldown'
    assert len(session.auth_calls()) == 2


def test_post_sends_json_body_and_returns_payload():
    """POST 带 JSON 请求体。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=200, json_body={'job_id': 'job-1'}),
    ])
    client = make_client(session)

    payload = client.post(
        GAUSSDB_V3 + '/instances/abc/customer-commands/cmd-1',
        {'sql_text': 'select 1', 'limit': 100},
    )

    assert payload == {'job_id': 'job-1'}
    call = session.business_calls()[0]
    assert call.method == 'POST'
    assert call.json == {'sql_text': 'select 1', 'limit': 100}
    assert call.headers[TOKEN_HEADER] == 'token-1'


def test_path_without_leading_slash_is_normalized():
    """路径少个前导斜杠也能拼对（不改变命名空间段）。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=200, json_body={}),
    ])
    client = make_client(session)

    client.get('rds/v3.2/instances')

    assert session.business_calls()[0].url == BASE_URL + '/rds/v3.2/instances'


def test_non_object_response_body_raises_protocol_error():
    """200 但 body 是数组 → ProtocolError。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=200, json_body=[1, 2, 3]),
    ])
    client = make_client(session)

    with pytest.raises(ProtocolError):
        client.get(RDS_V3_2 + '/instances')


def test_require_fields_rejects_non_mapping_payload():
    """响应体不是对象 → ProtocolError。"""
    with pytest.raises(ProtocolError):
        require_fields([1, 2], ['a'], RDS_V3_2 + '/instances')


def test_non_retryable_transport_error_is_protocol_error():
    """不可重试的请求异常 → ProtocolError（不当成瞬时故障）。"""
    session = FakeSession([
        auth_ok(),
        requests.exceptions.InvalidURL('bad url'),
    ])
    client = make_client(session)

    with pytest.raises(ProtocolError):
        client.get(RDS_V3_2 + '/instances')


def test_other_4xx_is_protocol_error():
    """既不是 401/403 也不是 404 的 4xx → ProtocolError（不可重试）。"""
    session = FakeSession([
        auth_ok(),
        FakeResponse(status_code=422, json_body={}),
    ])
    client = make_client(session)

    with pytest.raises(ProtocolError):
        client.get(RDS_V3_2 + '/instances')


def test_token_ttl_falls_back_to_default_when_body_has_no_session_timeout():
    """body 里没有 session_timeout → 用默认 1440 分钟。"""
    session = FakeSession([
        FakeResponse(status_code=200, headers={TOKEN_HEADER: 'tok'}, json_body={}),
    ])
    client = make_client(session)

    assert client.token() == 'tok'
    # 默认 1440 分钟 - 60 秒余量 = 86340 秒
    assert client._token_expires_at - client._clock() == pytest.approx(86340.0)


def test_token_ttl_falls_back_to_default_when_body_is_not_json():
    """body 不是 JSON → 用默认有效期。"""
    session = FakeSession([
        FakeResponse(status_code=200, headers={TOKEN_HEADER: 'tok'}, text='not json'),
    ])
    client = make_client(session)

    assert client.token() == 'tok'
    assert client._token_expires_at - client._clock() == pytest.approx(86340.0)
