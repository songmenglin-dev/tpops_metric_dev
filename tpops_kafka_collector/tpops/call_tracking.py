"""外部调用**真实计数**与**目录快照缓存**（可观测性 + 省调用）。

对应规格 ``specs/delivery-and-state/spec.md``「单轮外部调用数与投递成功率可观测」。

三样东西，各管一件事：

1. :class:`CallCounter` —— **真实 HTTP 计数**（线程安全）。
   挂在 :class:`~tpops_kafka_collector.tpops.client.TpopsClient` 上：
   **每次真正发出的请求**记 1（含认证、401 刷新、重试）。
   **全局计数**用锁保护；**线程计数**放在 ``threading.local`` 里
   （每个线程天然独占，不需要锁）。
2. :class:`CountingClient` —— 给客户端套一层「按线程计数」的代理。
   调度器是**并发**的（通道 × 实例两维并发），所以计数必须**按线程**归属，
   否则并发任务互相串账（一个任务把别人的调用算进自己头上）。
3. :class:`CatalogCachingClient` —— 指标分组目录的 **TTL 快照缓存**。
   目录是**平台元数据**（指标名 / 采集周期 / 告警阈值），变化极慢；
   采集端每轮对 12 个分组码各发一次 GET —— 12 次/轮纯属浪费。

**为什么计数按线程归属**：规格要的是「**单轮**的外部调用次数」。
一轮里的多个任务并发跑，各任务自己的调用数必须**互不串账**，
加起来才是这一轮的真实总数。
"""
import threading
import time
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

__all__ = [
    'CATALOG_CACHE_TTL_SECONDS',
    'CallCounter',
    'CatalogCachingClient',
    'CountingClient',
]

#: 目录快照缓存时长（秒）—— 与节点清单缓存同量级（1 小时）。
#:
#: 目录只用于「校验分组码非空 + 校验指标名存在」两处**诊断**，
#: **不参与任何指标取值** → 陈旧最多让告警晚一个 TTL 报出来，
#: 不影响数据正确性。
CATALOG_CACHE_TTL_SECONDS = 3600.0


class CallCounter:
    """真实外部调用计数（线程安全）。

    - **全局计数**：进程内累计发出的请求数（所有线程之和）；
    - **线程计数**：当前线程累计发出的请求数 —— 并发任务按线程归属。

    线程安全做法：**全局计数用锁保护**（不是裸 ``+= 1``），
    线程计数放在 ``threading.local`` 里（线程独占，无需加锁）。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._total = 0
        self._local = threading.local()

    def record(self) -> int:
        """记一次**真正发出**的请求；返回新的全局计数。"""
        with self._lock:
            self._total += 1
            total = self._total
        self._local.count = int(getattr(self._local, 'count', 0)) + 1
        return total

    def total(self) -> int:
        """进程内累计发出的请求数（所有线程）。"""
        with self._lock:
            return self._total

    def thread_total(self) -> int:
        """当前线程累计发出的请求数。"""
        return int(getattr(self._local, 'count', 0))

    def mark(self) -> int:
        """取一个**基线**（当前线程的累计值）。

        基线语义：**开始做一件事之前**取一次，**做完之后**再取一次，
        两者相减 = 这件事期间真实发出的请求数。
        """
        return self.thread_total()

    def delta(self, mark: int) -> int:
        """增量 = 当前线程累计 − 基线（基线由 :meth:`mark` 取得）。"""
        return max(self.thread_total() - int(mark), 0)


class CountingClient:
    """给客户端套一层「按线程计数」的代理。

    计数口径（优先级从高到低）：

    1. 底层客户端自带 **wire 级**计数（``TpopsClient.calls``）→
       记**真正发出的 HTTP 请求数**（含认证、401 刷新、重试）；
    2. 底层客户端没有计数能力（测试里的假客户端）→ 每次 ``get`` /
       ``post`` 记 1（**接口调用**口径）。

    代理是**共享**的（一个进程一个），计数按**线程**归属 ——
    并发任务互不串账。
    """

    def __init__(self, client: Any) -> None:
        self._client = client
        self._local = threading.local()

    def get(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Any:
        """转发一次 ``GET``（并计数）。"""
        return self._call(self._client.get, path, params=params)

    def post(self, path: str, payload: Optional[Mapping[str, Any]] = None) -> Any:
        """转发一次 ``POST``（并计数）。"""
        return self._call(self._client.post, path, payload=payload)

    def __getattr__(self, name: str) -> Any:
        """其余属性转发给底层客户端（``token()`` / ``calls`` 等）。"""
        client = self.__dict__.get('_client')
        if client is None:
            raise AttributeError(name)
        return getattr(client, name)

    def thread_count(self) -> int:
        """当前线程经由本代理发出的外部调用次数。"""
        return int(getattr(self._local, 'count', 0))

    def _call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """转发一次调用并记账。"""
        counter = getattr(self._client, 'calls', None)
        getter = getattr(counter, 'thread_total', None) if counter is not None else None
        if not callable(getter):
            # 底层客户端没有计数能力 → 按接口调用计（每次记 1）。
            self._bump(1)
            return func(*args, **kwargs)
        before = getter()
        try:
            return func(*args, **kwargs)
        finally:
            # 失败也算数 —— 请求已经发出去了。
            self._bump(getter() - before)

    def _bump(self, amount: int) -> None:
        self._local.count = int(getattr(self._local, 'count', 0)) + int(amount)


class CatalogCachingClient:
    """指标分组目录的 **TTL 快照缓存**。

    目录（``GET /rds/v3/monitor/metrics/series?name=<分组码>``）是
    **平台元数据**（指标名 / 采集周期 / 告警阈值），变化极慢；
    采集端每轮对 12 个分组码各发一次 GET —— 12 次/轮纯属浪费。

    **缓存口径**：

    - **键** = ``(分组码, 实例 ID)``（目录按实例解析告警阈值）；
    - **TTL** = :data:`CATALOG_CACHE_TTL_SECONDS`（1 小时），到期自动重拉；
    - **失败不入缓存** —— 拉取抛异常时照常抛出，缓存里不留半截数据；
    - 缓存内容**按只读使用**（调用方只读不写）。

    **陈旧的风险边界**：目录只用于两处**诊断**
    （「分组码写错 → 空分组告警」与「指标名不存在 → 缺失告警」），
    **不参与任何指标取值** → 陈旧只会让这两条告警**晚一个 TTL** 报出来，
    不影响任何一条指标数据的正确性。
    """

    def __init__(
        self,
        client: Any,
        catalog_path: str,
        ttl_seconds: float = CATALOG_CACHE_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._catalog_path = catalog_path
        self._ttl = float(ttl_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        self._cache: Dict[Tuple[Any, Any], Tuple[float, Any]] = {}

    def get(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Any:
        """目录路径走缓存，其余路径原样转发。"""
        if path != self._catalog_path:
            return self._client.get(path, params=params)
        key = self._key(params)
        cached = self._lookup(key)
        if cached is not None:
            return cached
        payload = self._client.get(path, params=params)
        self._store(key, payload)
        return payload

    def post(self, path: str, payload: Optional[Mapping[str, Any]] = None) -> Any:
        """``POST`` 原样转发（目录是只读接口，没有 POST）。"""
        return self._client.post(path, payload=payload)

    def __getattr__(self, name: str) -> Any:
        """其余属性转发给底层客户端。"""
        client = self.__dict__.get('_client')
        if client is None:
            raise AttributeError(name)
        return getattr(client, name)

    def _key(self, params: Optional[Mapping[str, Any]]) -> Tuple[Any, Any]:
        """缓存键 = （分组码, 实例 ID）。"""
        query = dict(params or {})
        return (query.get('name'), query.get('instance_id'))

    def _lookup(self, key: Tuple[Any, Any]) -> Any:
        """命中且未过期 → 返回快照；否则 ``None``（缓存内容按只读使用）。"""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            expires_at, payload = entry
            if self._clock() >= expires_at:
                self._cache.pop(key, None)
                return None
            return payload

    def _store(self, key: Tuple[Any, Any], payload: Any) -> None:
        """存快照（TTL 从现在起算）。"""
        with self._lock:
            self._cache[key] = (self._clock() + self._ttl, payload)
