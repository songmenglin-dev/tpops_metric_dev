"""TPOPS 平台 HTTP 客户端（mini-token 认证 + 错误四分类）。

对应开发方案 §4.2.1（认证）、§4.2（接口前缀）、§5.4.1（客户端签名）、
§5.7（重试口径），规格 ``specs/platform-client/spec.md``。

三条实测踩过的坑在这里固化（**不要凭直觉改**）：

1. **token 在响应头** ``X-Mini-Token``，**不在 body 里**。
   从 body 取 token 的后果是「认证成功但后续全 401」（开发方案 §2.2.9 坑 2）。
2. **认证失败绝不重试** —— 平台登录失败会累计 ``login_failed_attempts`` 并**锁号**。
   本模块在任何情况下**只发起一次**认证请求；认证接口非 2xx 一律抛
   :class:`AuthError`（不可重试）。
3. **两套路径命名空间不可混**：``/rds/v3/…``（认证、实例列表、监控指标、告警）与
   ``/rds/gaussdb/v3/…``（诊断优化、统一命令中心）。少一段前缀平台返回 404 ——
   这里把 404 归为**不可重试的协议错误**并显式报错，而不是当作「接口无数据」。

错误四分类：``AuthError``（401/403，不可重试）、``TransientError``
（5xx/超时/连接错误，可重试）、``ProtocolError``（404/结构不符，不可重试）、
``ProduceError``（Kafka 投递失败，由 kafka 层抛）。

**可重试性判定不在本模块另写一份** —— 统一走 ``errors.is_retryable``
（按基类链判定，子类如 ``requests.exceptions.ReadTimeout`` 也会被判为可重试）。

凭据只从参数/环境变量进来，**不进日志、不进异常消息、不进 ``repr``**。
"""
import logging
import time
from typing import Any, Callable, Dict, Mapping, Optional

import requests
import urllib3

from ..errors import (
    AuthError,
    ProtocolError,
    TransientError,
    TpopsError,
    is_retryable,
)
from .call_tracking import CallCounter
from .protocol import require_fields

LOGGER = logging.getLogger(__name__)

__all__ = [
    'AUTH_PATH',
    'DEFAULT_LANGUAGE',
    'GAUSSDB_V3',
    'LANGUAGE_HEADER',
    'RDS_V3',
    'RDS_V3_2',
    'TOKEN_HEADER',
    'TpopsClient',
    'require_fields',
]


#: 平台第一套命名空间：认证、实例列表、监控指标、告警记录。
RDS_V3 = '/rds/v3'
#: 实例列表用的是 ``v3.2``（注意不是 ``v3``）。
RDS_V3_2 = '/rds/v3.2'
#: 平台第二套命名空间：诊断优化、统一命令中心（少了它 86 个客户指标全丢）。
GAUSSDB_V3 = '/rds/gaussdb/v3'

#: 认证接口（属于第一套命名空间）。
AUTH_PATH = '/rds/v3/auth/mini-token'
#: **token 在响应头里** —— 不是响应体。
TOKEN_HEADER = 'X-Mini-Token'
LANGUAGE_HEADER = 'X-Language'
DEFAULT_LANGUAGE = 'zh-cn'

#: 平台会话超时（分钟）。实测值 1440。
DEFAULT_SESSION_TIMEOUT_MINUTES = 1440
#: 到期前多久刷新（秒）。实测 token 有效期 = ``session_timeout`` 分钟。
TOKEN_REFRESH_MARGIN_SECONDS = 60
#: 认证失败后的**静默期**（秒）：期内不再发起认证请求。
#:
#: 平台登录失败会累计 ``login_failed_attempts`` 并**锁号**，所以认证失败后
#: 绝不能马上再试。本客户端在静默期内直接复用上一次的失败结论（不再发请求），
#: 静默期过了才允许下一次认证（配合调度器的轮询退避 —— 开发方案 §4.2.1）。
AUTH_FAILURE_COOLDOWN_SECONDS = 60.0


class TpopsClient:
    """TPOPS 平台的 HTTP 客户端。

    职责：认证（mini-token）、发请求、按平台语义把失败翻译成
    :mod:`tpops_kafka_collector.errors` 里的四类异常。
    **不做重试编排** —— 重试由 ``retry`` 模块按 ``is_retryable`` 统一决定。
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        verify_tls: bool = False,
        timeout: int = 60,
        session: Optional[requests.Session] = None,
        clock: Optional[Callable[[], float]] = None,
        auth_failure_cooldown: float = AUTH_FAILURE_COOLDOWN_SECONDS,
    ) -> None:
        """装配客户端。

        :param base_url: 平台根地址（如 ``https://172.16.103.193:8002``）。
        :param username: TPOPS 账号（来自环境变量 ``TPOPS_USER``）。
        :param password: TPOPS 密码（来自环境变量 ``TPOPS_PASSWORD``）。
        :param verify_tls: 是否校验 TLS 证书。平台用**自签证书**，默认 ``False``。
        :param timeout: 单次请求超时（秒）。
        :param session: 可注入的 ``requests.Session``（测试用）。
        :param clock: 可注入的单调时钟（测试 token 过期用）。
        :param auth_failure_cooldown: 认证失败后的静默期（秒）；期内不再发起认证。
        :raises ValueError: 账号或密码缺失（**不回显取值**）。
        """
        if not username:
            raise ValueError('缺少 TPOPS 用户名')
        if not password:
            raise ValueError('缺少 TPOPS 密码')

        self._base_url = base_url.rstrip('/')
        self._username = username
        self._password = password
        self._timeout = timeout
        self._session = session if session is not None else requests.Session()
        self._session.verify = verify_tls
        if not verify_tls:
            # 平台用自签证书，握手告警会刷屏 —— 只在确实不校验时静音。
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self._clock = clock if clock is not None else time.monotonic
        self._auth_failure_cooldown = auth_failure_cooldown

        self._token: Optional[str] = None
        self._token_expires_at = 0.0
        self._auth_failed_at: Optional[float] = None
        self._auth_failure: Optional[BaseException] = None
        #: **真实外部调用计数**（线程安全）—— 每次真正发出的请求记 1。
        self._calls = CallCounter()

    @property
    def calls(self) -> CallCounter:
        """真实外部调用计数（全局 + 按线程）。

        ``calls.total()`` / ``calls.thread_total()`` 取当前值；
        ``calls.mark()`` / ``calls.delta(mark)`` 取基线与增量
        （**开始前取基线、结束后相减**）。计数**含重试**产生的额外请求。
        """
        return self._calls

    @classmethod
    def from_config(
        cls,
        config: Any,
        session: Optional[requests.Session] = None,
        clock: Optional[Any] = None,
    ) -> 'TpopsClient':
        """从 :class:`tpops_kafka_collector.config.Config` 装配客户端。"""
        return cls(
            base_url=config.tpops_base_url,
            username=config.tpops_user,
            password=config.tpops_password,
            verify_tls=config.tpops_verify_tls,
            timeout=config.tpops_request_timeout,
            auth_failure_cooldown=config.tpops_auth_failure_cooldown,
            session=session,
            clock=clock,
        )

    def __repr__(self) -> str:
        """repr 里**不出现密码**（配置快照/日志会打印 repr）。"""
        return f'TpopsClient(base_url={self._base_url!r}, username={self._username!r})'

    # ------------------------------------------------------------------
    # 认证
    # ------------------------------------------------------------------

    def token(self, refresh: bool = False) -> str:
        """取 mini-token；没有或过期时**认证一次**。

        :param refresh: 强制重新认证（401 之后的强制刷新用）。
        :raises AuthError: 认证接口非 2xx —— **不可重试**（防锁号）。
        :raises ProtocolError: 认证成功但响应头没有 ``X-Mini-Token``。
        """
        if not refresh and self._token is not None:
            if self._clock() < self._token_expires_at:
                return self._token
        self._authenticate()
        return self._token  # type: ignore[return-value]

    def _authenticate(self) -> None:
        """向平台认证一次。

        **只发一次** —— 认证失败绝不重试（平台会累计 ``login_failed_attempts``
        并锁号）。消息里不含密码、不含 token。
        """
        self._raise_if_in_cooldown()
        LOGGER.debug('TPOPS 认证请求：POST %s', AUTH_PATH)
        try:
            response = self._send(
                'POST',
                AUTH_PATH,
                '',
                None,
                {'username': self._username, 'password': self._password},
            )
        except TpopsError as exc:
            # 认证请求本身的失败（含传输层）**一律不可重试**：
            # 平台可能已经收到并计数过一次失败登录，再来一次就是锁号。
            error = AuthError(f'TPOPS 认证失败：{exc} —— 不重试（{AUTH_PATH}）')
            self._remember_auth_failure(error)
            raise error from exc
        status = response.status_code
        if not 200 <= status < 300:
            # 非 2xx 一律 AuthError：这里**不做**任何重试。
            error = AuthError(
                f'TPOPS 认证失败（HTTP {status}）：{AUTH_PATH} —— 不重试'
                '（平台会累计 login_failed_attempts 并锁号）'
            )
            self._remember_auth_failure(error)
            raise error
        token = response.headers.get(TOKEN_HEADER)
        if not token:
            error = ProtocolError(
                f'TPOPS 认证响应头缺少 {TOKEN_HEADER}（{AUTH_PATH}）—— 平台行为变了'
            )
            self._remember_auth_failure(error)
            raise error
        self._token = token
        self._token_expires_at = self._clock() + self._token_ttl(response)
        self._auth_failed_at = None
        self._auth_failure = None
        LOGGER.debug('TPOPS 认证成功，token 有效期 %s 秒', self._token_expires_at)

    def _remember_auth_failure(self, error: BaseException) -> None:
        """记下认证失败，进入静默期（期内不再发起认证请求）。"""
        self._auth_failed_at = self._clock()
        self._auth_failure = error
        LOGGER.error('TPOPS 认证失败：%s', error)

    def _raise_if_in_cooldown(self) -> None:
        """静默期内直接复用上次的失败结论，**不发请求**。"""
        if self._auth_failed_at is None or self._auth_failure is None:
            return
        elapsed = self._clock() - self._auth_failed_at
        if elapsed < self._auth_failure_cooldown:
            raise self._auth_failure
        # 静默期已过 —— 允许再试一次（调度器的轮询退避在此之上）。
        self._auth_failed_at = None
        self._auth_failure = None

    def _token_ttl(self, response: Any) -> float:
        """token 有效期（秒）：``session_timeout`` 分钟 - 提前刷新的余量。"""
        minutes = DEFAULT_SESSION_TIMEOUT_MINUTES
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, Mapping):
            raw = body.get('session_timeout')
            if isinstance(raw, (int, float)) and raw > 0:
                minutes = raw
        return minutes * 60 - TOKEN_REFRESH_MARGIN_SECONDS

    # ------------------------------------------------------------------
    # 业务请求
    # ------------------------------------------------------------------

    def get(
        self, path: str, params: Optional[Mapping[str, Any]] = None
    ) -> Dict[str, Any]:
        """GET 一个平台接口并返回解析后的 JSON。

        :param path: 完整路径（含 ``/rds/v3/…`` 或 ``/rds/gaussdb/v3/…``）。
        :param params: 查询参数。
        :raises AuthError: 401/403（不可重试）。
        :raises TransientError: 5xx / 超时 / 连接错误（可重试）。
        :raises ProtocolError: 404 / 响应体不是 JSON / 缺约定字段。
        """
        return self._request('GET', path, params=params)

    def post(
        self, path: str, payload: Optional[Mapping[str, Any]] = None
    ) -> Dict[str, Any]:
        """POST 一个平台接口并返回解析后的 JSON。

        :param path: 完整路径（含命名空间前缀）。
        :param payload: JSON 请求体。
        """
        return self._request('POST', path, payload=payload)

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Mapping[str, Any]] = None,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """发一次请求；401 时强制刷新 token 并**只重试一次**。"""
        token = self.token()
        response = self._send(method, path, token, params, payload)
        if response.status_code == 401:
            LOGGER.warning(
                'TPOPS 业务请求返回 401，强制刷新 token 后重试一次：%s %s', method, path
            )
            token = self.token(refresh=True)
            response = self._send(method, path, token, params, payload)
            if response.status_code == 401:
                raise AuthError(f'TPOPS 刷新 token 后仍返回 401：{method} {path}')
        self._raise_for_status(response, method, path)
        return self._parse_json(response, method, path)

    def _send(
        self,
        method: str,
        path: str,
        token: str,
        params: Optional[Mapping[str, Any]],
        payload: Optional[Mapping[str, Any]],
    ) -> Any:
        """真正发请求；只处理传输层异常，状态码由调用方判定。"""
        headers = {LANGUAGE_HEADER: DEFAULT_LANGUAGE}
        if token:
            headers[TOKEN_HEADER] = token
        LOGGER.debug('TPOPS 请求：%s %s', method, path)
        # **真正发出的请求**才计数 —— 计数点必须在「发出去」这一刻，
        # 不能在调用入口（否则重试 / 401 刷新会被漏掉或重复计）。
        self._calls.record()
        try:
            response = self._session.request(
                method,
                self._url(path),
                params=params,
                json=payload,
                headers=headers,
                timeout=self._timeout,
            )
        except requests.exceptions.RequestException as exc:
            raise self._transport_error(exc, method, path) from exc
        LOGGER.debug('TPOPS 响应：%s %s -> HTTP %s', method, path, response.status_code)
        return response

    def _transport_error(
        self, exc: BaseException, method: str, path: str
    ) -> BaseException:
        """把传输层异常翻译成平台异常。

        可重试性**不在这里另写判定** —— 走 ``errors.is_retryable``
        （基类链判定，``ReadTimeout``/``ConnectTimeout`` 都会被判为可重试）。
        """
        if is_retryable(exc):
            return TransientError(
                f'TPOPS 请求失败（{type(exc).__name__}）：{method} {path}'
            )
        # 不可重试的请求构造类错误（例如 URL 写错）—— 当协议错误处理，立即暴露。
        return ProtocolError(
            f'TPOPS 请求无法完成（{type(exc).__name__}）：{method} {path}'
        )

    def _raise_for_status(self, response: Any, method: str, path: str) -> None:
        """按状态码分类平台错误。"""
        status = response.status_code
        if 200 <= status < 300:
            return
        if status in (401, 403):
            raise AuthError(f'TPOPS 认证/授权失败（HTTP {status}）：{method} {path}')
        if status == 404:
            # 漏掉命名空间前缀（/rds/v3 vs /rds/gaussdb/v3）就是 404 ——
            # 显式报错，不当作「接口无数据」。
            raise ProtocolError(
                f'TPOPS 接口不存在（HTTP 404）：{method} {path} —— 检查平台路径命名空间'
                '（/rds/v3 与 /rds/gaussdb/v3 不可混）'
            )
        if status >= 500:
            raise TransientError(
                f'TPOPS 服务端错误（HTTP {status}）：{method} {path}'
            )
        raise ProtocolError(f'TPOPS 请求被拒绝（HTTP {status}）：{method} {path}')

    def _parse_json(self, response: Any, method: str, path: str) -> Dict[str, Any]:
        """解析响应体；空体当作空对象，坏 JSON 抛协议错误。"""
        text = getattr(response, 'text', '') or ''
        if not text.strip():
            return {}
        try:
            payload = response.json()
        except ValueError as exc:
            raise ProtocolError(f'TPOPS 响应体不是合法 JSON：{method} {path}') from exc
        if not isinstance(payload, Mapping):
            raise ProtocolError(
                f'TPOPS 响应结构异常：{method} {path} —— 期望对象，实际 '
                f'{type(payload).__name__}'
            )
        return dict(payload)

    def _url(self, path: str) -> str:
        """拼接完整 URL（路径自带命名空间前缀，这里只做拼接）。"""
        if not path.startswith('/'):
            path = '/' + path
        return self._base_url + path
