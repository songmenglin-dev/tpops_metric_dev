"""健康端点（存活 / 就绪）—— 任务 3.2。

两个端点的语义**必须分开**：

- ``/healthz``（**存活**）—— 进程能应答就是 200。它**不看任何依赖**：
  依赖（TPOPS 平台、Kafka）挂了不代表进程该被重启。
- ``/readyz``（**就绪**）—— 依赖不可用时返回 **503**，告诉编排器「先别派活给我」。

两者混为一谈会有真实代价：编排器的 liveness 探针若挂在依赖上，
一次 Kafka 抖动就会把采集端反复重启（重启又让水位重放，问题放大）。

**依赖探针可注入**：采集端自己维护依赖状态（认证是否成功、Kafka 是否可用、
映射表是否加载、水位目录是否可写），健康端点只**读**这些状态 ——
不在探针里 ping 平台、不连 Kafka（那会让探针本身变成故障源）。

**不泄露凭据**：响应体只有固定结构的字段 + 探针自报的简短原因；
探针抛异常时**只记异常类型**、不回显异常消息（异常消息里常有口令）。
另有 ``secrets`` 兜底脱敏：即便某个原因文本混进了口令或令牌，
序列化前也会被替换成 ``***``。

用法（3.3 装配时）::

    monitor = ReadinessMonitor((
        DependencyProbe('tpops_auth', lambda: client.auth_ok),
        DependencyProbe('kafka', lambda: producer.is_connected),
    ))
    server = HealthServer.from_config(config, monitor=monitor)
    server.start()
"""
import errno
import json
import logging
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Iterable, Optional, Tuple
from urllib.parse import urlsplit

from .config import Config

LOGGER = logging.getLogger(__name__)

__all__ = [
    'HEALTH_PATH',
    'READY_PATH',
    'DependencyProbe',
    'DependencyStatus',
    'HealthEndpoints',
    'HealthServer',
    'HealthServerError',
    'ReadinessMonitor',
    'ReadinessSnapshot',
    'secret_values_from_config',
]

#: 存活探针路径。
HEALTH_PATH = '/healthz'
#: 就绪探针路径。
READY_PATH = '/readyz'

#: 探针自报原因的最大长度（超出截断）—— 健康响应不承载长文本。
MAX_REASON_LENGTH = 200

#: 脱敏后凭据的占位符。
REDACTED = '***'


class HealthServerError(RuntimeError):
    """健康端点起不来（端口占用、地址非法等）。"""


# ---------------------------------------------------------------------------
# 探针接口
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DependencyStatus:
    """单个依赖的就绪状态。

    :param name: 依赖名（如 ``tpops_auth`` / ``kafka``）—— 只暴露名字，
        不暴露地址。
    :param is_ready: 是否就绪。
    :param reason: 不就绪的简短原因；**不得**含凭据（脱敏兜底仍会再过一遍）。
    """

    name: str
    is_ready: bool
    reason: str = ''


@dataclass(frozen=True)
class ReadinessSnapshot:
    """一次就绪检查的结果（不可变）。"""

    dependencies: Tuple[DependencyStatus, ...] = ()

    @property
    def is_ready(self) -> bool:
        """全部依赖就绪才算就绪（没有依赖时视为就绪）。"""
        return all(item.is_ready for item in self.dependencies)

    @property
    def failed(self) -> Tuple[DependencyStatus, ...]:
        """不就绪的依赖。"""
        return tuple(item for item in self.dependencies if not item.is_ready)


@dataclass(frozen=True)
class DependencyProbe:
    """一个依赖探针。

    :param name: 依赖名，进响应体的 ``dependencies[].name``。
    :param check: 检查函数，**必须轻量**（读一个已维护的状态），
        返回值语义：

        - ``bool`` —— True 就绪、False 不就绪；
        - ``str`` —— 不就绪，并把该字符串作为简短原因；
        - ``DependencyStatus`` —— 完全自定义（名字与原因）；
        - ``None`` —— 视为不就绪。
    """

    name: str
    check: Callable[[], Any]


class ReadinessMonitor:
    """依赖探针集合 —— 3.3 在这里接上真实依赖。

    探针是**按需调用**的：每次 ``/readyz`` 才跑一遍。
    因此探针函数要轻量（读标志位），不要在里面发网络请求。
    """

    def __init__(self, probes: Iterable[DependencyProbe] = ()) -> None:
        self._probes = tuple(probes)

    @property
    def probes(self) -> Tuple[DependencyProbe, ...]:
        return self._probes

    def snapshot(self) -> ReadinessSnapshot:
        """跑一遍全部探针，返回不可变快照。

        单个探针抛异常**不会**向上抛 —— 探针坏了等于「这个依赖不就绪」，
        健康端点不能因为探针坏掉而自己挂掉。
        """
        return ReadinessSnapshot(tuple(_run_probe(probe) for probe in self._probes))


def _run_probe(probe: DependencyProbe) -> DependencyStatus:
    """跑一个探针并把结果规整成 :class:`DependencyStatus`。"""
    try:
        result = probe.check()
    except Exception as exc:  # noqa: BLE001 —— 探针异常不得拖垮端点
        # 只记异常类型，不记异常消息：消息里可能带口令/令牌。
        LOGGER.warning('依赖探针 %s 执行异常：%s', probe.name, type(exc).__name__)
        return DependencyStatus(name=probe.name, is_ready=False, reason='探针执行异常')
    return _to_status(probe.name, result)


def _to_status(name: str, result: Any) -> DependencyStatus:
    """把探针返回值规整成 :class:`DependencyStatus`。"""
    if isinstance(result, DependencyStatus):
        return DependencyStatus(
            name=result.name or name,
            is_ready=result.is_ready,
            reason=_short_reason(result.reason),
        )
    if isinstance(result, str):
        return DependencyStatus(name=name, is_ready=False, reason=_short_reason(result))
    return DependencyStatus(name=name, is_ready=bool(result), reason='')


def _short_reason(reason: Any) -> str:
    """原因截断到 :data:`MAX_REASON_LENGTH` 个字符。"""
    text = '' if reason is None else str(reason)
    return text[:MAX_REASON_LENGTH]


# ---------------------------------------------------------------------------
# 端点逻辑（与 HTTP 无关，可单独用）
# ---------------------------------------------------------------------------

class HealthEndpoints:
    """健康端点的响应构造。

    :param monitor: 依赖探针集合；为空则「没有可检查的依赖」。
    :param secrets: 需要从响应里抹掉的值（口令、令牌、连接串里的用户信息）。
        这是**兜底**措施 —— 正常路径下响应体根本不带这些值。
    """

    def __init__(
        self,
        monitor: Optional[ReadinessMonitor] = None,
        secrets: Iterable[str] = (),
    ) -> None:
        self._monitor = monitor if monitor is not None else ReadinessMonitor()
        self._secrets = tuple(value for value in secrets if value)

    @property
    def monitor(self) -> ReadinessMonitor:
        return self._monitor

    def liveness(self) -> Tuple[int, bytes]:
        """存活响应：进程能应答就是 200，**不查依赖**。"""
        payload = {
            'status': 'ok',
            'check': 'liveness',
            'timestamp': _now(),
        }
        return 200, self._encode(payload)

    def readiness(self) -> Tuple[int, bytes]:
        """就绪响应：依赖全好 200，否则 503。"""
        snapshot = self._monitor.snapshot()
        payload = {
            'status': 'ok' if snapshot.is_ready else 'unavailable',
            'check': 'readiness',
            'timestamp': _now(),
            'dependencies': [
                {
                    'name': item.name,
                    'ready': item.is_ready,
                    'reason': item.reason,
                }
                for item in snapshot.dependencies
            ],
        }
        status = 200 if snapshot.is_ready else 503
        return status, self._encode(payload)

    def _encode(self, payload: Dict[str, Any]) -> bytes:
        """序列化成 JSON 字节串，并做兜底脱敏。"""
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return self._redact(text).encode('utf-8')

    def _redact(self, text: str) -> str:
        """把登记过的凭据替换成占位符。"""
        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, REDACTED)
        return text


def _now() -> int:
    """当前时刻（epoch 秒）。"""
    return int(time.time())


def secret_values_from_config(config: Config) -> Tuple[str, ...]:
    """从配置里挑出**不该出现在响应里**的值。

    - ``TPOPS_PASSWORD``；
    - Kafka 连接串里的用户信息（``user:pass@host`` → ``user:pass``）。

    只放**值**，不放变量名 —— 变量名本身不敏感。
    """
    values = [config.tpops_password]
    for server in config.kafka_bootstrap_servers:
        if '@' in server:
            userinfo = server.rsplit('@', 1)[0]
            if userinfo:
                values.append(userinfo)
    return tuple(value for value in values if value)


# ---------------------------------------------------------------------------
# HTTP 服务
# ---------------------------------------------------------------------------

class _HealthRequestHandler(BaseHTTPRequestHandler):
    """``/healthz`` 与 ``/readyz`` 的请求处理。

    请求处理只做「分发 + 回包」，判定逻辑都在 :class:`HealthEndpoints` 里。
    """

    server_version = 'tpops-collector-health'
    protocol_version = 'HTTP/1.1'

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        endpoints = getattr(self.server, 'endpoints', None)
        if endpoints is None:
            status, body = 500, b'{"status": "error"}'
        elif path == HEALTH_PATH:
            status, body = endpoints.liveness()
        elif path == READY_PATH:
            status, body = endpoints.readiness()
        else:
            status, body = 404, b'{"status": "not_found"}'
        self._respond(status, body)

    def _respond(self, status: int, body: bytes) -> None:
        """回一个 JSON 响应。"""
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        """请求日志走 ``logging``，不用 ``print``。"""
        LOGGER.debug('健康端点 %s', fmt % args)


class HealthServer:
    """健康端点的 HTTP 服务（标准库 ``ThreadingHTTPServer``）。

    不引入任何 web 框架 —— 端点只有两个、标准库足够。
    """

    def __init__(
        self,
        host: str = '127.0.0.1',
        port: int = 8088,
        monitor: Optional[ReadinessMonitor] = None,
        secrets: Iterable[str] = (),
    ) -> None:
        self._host = host
        self._port = port
        self._endpoints = HealthEndpoints(monitor=monitor, secrets=secrets)
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    @classmethod
    def from_config(
        cls,
        config: Config,
        monitor: Optional[ReadinessMonitor] = None,
    ) -> 'HealthServer':
        """按配置装配（监听地址、端口取自 ``health_host`` / ``health_port``）。"""
        return cls(
            host=config.health_host,
            port=config.health_port,
            monitor=monitor,
            secrets=secret_values_from_config(config),
        )

    @property
    def endpoints(self) -> HealthEndpoints:
        return self._endpoints

    @property
    def address(self) -> Tuple[str, int]:
        """实际监听地址（端口为 0 时是系统分配的临时端口）。"""
        if self._server is None:
            raise HealthServerError('健康端点尚未启动，取不到监听地址')
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    def start(self) -> 'HealthServer':
        """后台线程起服务。"""
        if self._server is not None:
            raise HealthServerError('健康端点已经启动，不能重复启动')
        try:
            server = ThreadingHTTPServer((self._host, self._port), _HealthRequestHandler)
        except OSError as exc:
            hint = ''
            if getattr(exc, 'errno', None) == errno.EADDRINUSE:
                # 端口被占用最常见的两种原因，都跟「上一个实例没退干净」有关
                hint = ('（端口已被占用 —— 多半是上一次启动的进程还在：'
                        '① 优雅退出最多要等 30 秒，退出前就重启会撞上；'
                        '② Ctrl-Z 挂起（SIGTSTP）的进程也还占着端口。'
                        f'用 ss -tlunp | grep {self._port} 找到 pid，确认后 kill 掉再启动）'
                        )
            raise HealthServerError(
                f'健康端点监听 {self._host}:{self._port} 失败：{exc}{hint}'
            ) from exc
        server.endpoints = self._endpoints  # type: ignore[attr-defined]
        self._server = server
        thread = threading.Thread(
            target=server.serve_forever,
            kwargs={'poll_interval': 0.1},
            name='health-server',
        )
        thread.daemon = True
        thread.start()
        self._thread = thread
        LOGGER.info('健康端点已启动：%s:%s', self._host, self._port)
        return self

    def stop(self) -> None:
        """停服务（可重复调用）。"""
        server, thread = self._server, self._thread
        self._server = None
        self._thread = None
        if server is None:
            return
        try:
            server.shutdown()
        finally:
            server.server_close()
            if thread is not None:
                thread.join(timeout=5)
            LOGGER.info('健康端点已停止')

    def __enter__(self) -> 'HealthServer':
        return self.start()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()
