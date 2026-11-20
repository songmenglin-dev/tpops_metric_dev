"""统一命令中心（B 类）**异步三跳**客户端。

对应开发方案 §4.2.5（统一命令中心）、规格 ``specs/collection-channels/spec.md``
「命令中心通道的执行」，任务 2.5。

链路（三跳）::

    POST …/command-center/customer-commands/{cmd_id}
        → {job_id, node_tasks:[{task_id, node_id}]}          （不含数据）
    GET  …/command-center/command/{cmd_id}/batch-exec-job/task-records?job_id=…
        → {job_status, statistics:{running,…}, tasks:[…]}    （轮询到结束）
    GET  …/command-center/command/{cmd_id}/batch-exec-job/{job_id}
           /task-record/{task_id}/detail
        → {status, result, …}                               （业务数据在这里）

**六条实测踩过的坑在这里固化（不要凭直觉改）**：

1. **结束判据是 ``statistics.running == 0``，绝不能看 ``job_status``** ——
   实测节点任务已 ``succeed`` 而 ``job_status`` 仍停在 ``running``。
2. **``result`` 是 JSON 字符串**，要**二次 ``json.loads``** 才是行列表。
3. **只能在 master 节点执行**（命令级约束 ``limit_node_types=["master"]``）——
   下发前先取节点清单，只留 ``role == "master"``。
4. **节点清单按 ``node_id`` 去重** —— 同一个 master 在清单里会以 CN / DN
   各出现一次（实测 12 条条目 / 6 条 master / **3 个唯一 node_id**）。
5. **节点清单按实例缓存 1 小时** —— 不每轮都拉。
6. **单次往返 3~8 秒**，实测 50 条 SQL 有 **3 条偶发失败（重试即成功）**
   → **逐条重试**，且**一条失败不影响同批其他**。

重试判定**不在本模块另写一份** —— 统一走 ``errors.is_retryable``；
重试编排走 ``retry.retry_call``。
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple  # noqa: F401  （Dict 仅用于 # type: 注释）

from ..errors import ProtocolError, TransientError
from ..retry import RetryPolicy, retry_call
from .client import GAUSSDB_V3, require_fields
from .command_center_nodes import NODE_CACHE_TTL_SECONDS, MasterNodeRegistry
from .command_center_result import (
    CommandTaskError,
    NodeTask,
    SqlOutcome,
    parse_result_rows,
)

LOGGER = logging.getLogger(__name__)

__all__ = [
    'CommandCenterClient',
    'CommandTaskError',
    'GENERAL_SINGLE_SQL_CMD_ID',
    'NodeTask',
    'SqlOutcome',
    'parse_result_rows',
]
# 结果类型与 result 二次解析在同包的 ``command_center_result`` 里
# （守住「单文件 < 400 行」的红线），这里原样再导出，调用方只 import 本模块即可。

#: 平台内置命令 ``general_single_sql_query`` 的 ``cmd_id``
#: （``source=default``，平台 admin 创建）。模板是
#: ``select * from ({sql_text}) limit {limit}`` —— **平台提供执行器，我们提供 SQL**。
GENERAL_SINGLE_SQL_CMD_ID = '73722acc-bff6-4d04-adc0-bc1e1fa1eb7d'

#: 下发命令（第 1 跳）。
SUBMIT_PATH = GAUSSDB_V3 + '/command-center/customer-commands/{cmd_id}'
#: 轮询任务状态（第 2 跳）。
TASK_RECORDS_PATH = GAUSSDB_V3 + '/command-center/command/{cmd_id}/batch-exec-job/task-records'
#: 取任务结果（第 3 跳）。
TASK_DETAIL_PATH = (
    GAUSSDB_V3 + '/command-center/command/{cmd_id}/batch-exec-job/{job_id}'
    + '/task-record/{task_id}/detail'
)
#: 默认并发数。
DEFAULT_CONCURRENCY = 2
#: 轮询首次等待（秒），之后按 1.5 倍递增，上限 3 秒。
DEFAULT_POLL_INTERVAL = 1.0
MAX_POLL_INTERVAL = 3.0
#: 轮询总超时（秒）。
DEFAULT_POLL_TIMEOUT = 60.0
#: ``task-records`` 的分页大小。
TASK_RECORDS_LIMIT = 100
#: 平台对「命令真的执行成功」的自述失败串（``status=succeed`` 不代表成功）。
FAILED_RESULT_MARKER = 'Failed to execute command'


class CommandCenterClient:
    """统一命令中心的三跳客户端。

    职责：取 master 节点清单 → 下发 SQL → 轮询到结束 → 取回并二次解析结果。
    **不做映射、不拼消息** —— 那是映射层与 ``kafka/serializer.py`` 的事。

    :param client: TPOPS HTTP 客户端（需有 ``get`` / ``post``）。
    :param cmd_id: 平台内置命令 ID，默认 ``general_single_sql_query``。
    :param concurrency: 批内并发数，默认 2。
    :param poll_timeout: 单条 SQL 的轮询总超时（秒）。
    :param poll_interval: 轮询首次等待（秒）。
    :param node_cache_ttl: 节点清单缓存时长（秒）。
    :param retry_policy: 单条 SQL 的重试策略（判定只认 ``errors.is_retryable``）。
    :param sleep: 等待函数（测试注入假实现，不真等）。
    :param clock: 单调时钟（测试注入假实现）。
    :param executor_factory: 线程池工厂（默认 :class:`ThreadPoolExecutor`）。
    """

    def __init__(
        self,
        client: Any,
        cmd_id: str = GENERAL_SINGLE_SQL_CMD_ID,
        concurrency: int = DEFAULT_CONCURRENCY,
        poll_timeout: float = DEFAULT_POLL_TIMEOUT,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        node_cache_ttl: float = NODE_CACHE_TTL_SECONDS,
        retry_policy: Optional[RetryPolicy] = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        executor_factory: Callable[..., Any] = ThreadPoolExecutor,
    ) -> None:
        if concurrency < 1:
            raise ValueError('concurrency 至少为 1')
        self._client = client
        self._cmd_id = cmd_id
        self._concurrency = concurrency
        self._poll_timeout = poll_timeout
        self._poll_interval = poll_interval
        self._node_cache_ttl = node_cache_ttl
        self._retry_policy = retry_policy if retry_policy is not None else RetryPolicy()
        self._sleep = sleep
        self._clock = clock
        self._executor_factory = executor_factory
        self._nodes = MasterNodeRegistry(
            client, ttl=node_cache_ttl, clock=clock
        )

    @classmethod
    def from_config(
        cls,
        config: Any,
        client: Any,
        **kwargs: Any
    ) -> 'CommandCenterClient':
        """从 :class:`tpops_kafka_collector.config.Config` 装配。"""
        kwargs.setdefault('concurrency', config.cmd_concurrency)
        kwargs.setdefault('poll_timeout', float(config.cmd_poll_timeout))
        kwargs.setdefault(
            'retry_policy',
            RetryPolicy(
                max_attempts=max(1, config.cmd_retry_attempts),
                base_delay=config.retry_base_seconds,
            ),
        )
        return cls(client, **kwargs)

    @property
    def concurrency(self) -> int:
        """批内并发数。"""
        return self._concurrency

    # ------------------------------------------------------------------
    # 节点清单
    # ------------------------------------------------------------------

    def master_nodes(self, instance_id: str) -> Tuple[Mapping[str, Any], ...]:
        """实例的 master 节点清单（按 ``node_id`` 去重，顺序稳定）。"""
        return self._nodes.nodes(instance_id)

    def master_node_ids(self, instance_id: str) -> Tuple[str, ...]:
        """master 节点 ID 列表（已去重，顺序稳定）。"""
        return self._nodes.node_ids(instance_id)

    # ------------------------------------------------------------------
    # 单条 SQL
    # ------------------------------------------------------------------

    def execute_sql(
        self,
        instance_id: str,
        sql_text: str,
        limit: Any = '1000',
    ) -> SqlOutcome:
        """执行一条 SQL，失败时**返回**失败结果（不抛异常）。

        失败不抛异常是为了**单条失败隔离** —— 一条 SQL 挂了不影响同批其他。
        原始异常挂在 ``outcome.exception`` 上，失败原因在 ``outcome.error``。
        """
        attempts = [0]  # type: List[int]

        def _run() -> SqlOutcome:
            attempts[0] += 1
            return self._execute_once(instance_id, sql_text, limit)

        try:
            outcome = retry_call(
                _run,
                policy=self._retry_policy,
                sleep=self._sleep,
                description=f'命令中心 SQL：{_sql_label(sql_text)}',
            )
            return replace(outcome, attempts=attempts[0])
        except Exception as exc:  # noqa: B902 —— 判定后原样记录，不静默吞
            LOGGER.error(
                '命令中心 SQL 执行失败（%s）：%s', _sql_label(sql_text), exc
            )
            return SqlOutcome(
                sql_text=sql_text,
                ok=False,
                error=str(exc),
                attempts=attempts[0],
                exception=exc,
            )

    def execute_batch(
        self,
        instance_id: str,
        sql_texts: Sequence[str],
        limit: Any = '1000',
    ) -> Tuple[SqlOutcome, ...]:
        """并发执行一批 SQL，**逐条**成功/失败互不影响。

        返回顺序与入参顺序一致。并发数由 ``concurrency`` 控制。
        """
        if not sql_texts:
            return ()
        # 先把节点清单取出来（顺带填好缓存），避免并发线程重复拉平台。
        self.master_nodes(instance_id)
        with self._executor_factory(max_workers=self._concurrency) as pool:
            futures = [
                pool.submit(self.execute_sql, instance_id, sql_text, limit)
                for sql_text in sql_texts
            ]
            return tuple(future.result() for future in futures)

    def _execute_once(
        self, instance_id: str, sql_text: str, limit: Any
    ) -> SqlOutcome:
        """跑完一次三跳（不重试）。失败时抛异常。"""
        node_ids = self.master_node_ids(instance_id)
        job_id, node_tasks = self._submit(instance_id, node_ids, sql_text, limit)
        records = self._poll(job_id)
        node_tasks = self._collect_tasks(job_id, node_tasks, records)
        # 失败判据：**状态不是 succeed**，或**带错误原因**（自述失败串也算失败）。
        failed = [
            task for task in node_tasks
            if task.status != 'succeed' or task.error
        ]
        if failed:
            reasons = '；'.join(
                f'{task.node_id}: {task.error or task.status}' for task in failed
            )
            raise CommandTaskError(
                f'命令中心任务失败（节点 {job_id}）：{reasons}'
            )
        return SqlOutcome(
            sql_text=sql_text,
            ok=True,
            tasks=tuple(node_tasks),
            attempts=1,
            job_id=job_id,
        )

    def _submit(
        self, instance_id: str, node_ids: Sequence[str], sql_text: str, limit: Any
    ) -> Tuple[str, List[Mapping[str, Any]]]:
        """第 1 跳：下发命令，返回 ``(job_id, node_tasks)``。"""
        path = SUBMIT_PATH.format(cmd_id=self._cmd_id)
        payload = {
            'instance_id': instance_id,
            'node_ids': list(node_ids),
            'params': [
                {'param_name': 'sql_text', 'param_value': sql_text},
                {'param_name': 'limit', 'param_value': str(limit)},
            ],
        }
        body = self._client.post(path, payload)
        job_id = require_fields(body, ('job_id',), path).get('job_id')
        node_tasks = body.get('node_tasks') or []
        if not job_id:
            raise ProtocolError('命令中心下发响应里没有 job_id')
        if not node_tasks:
            raise ProtocolError('命令中心下发响应里没有 node_tasks（没有可取结果的任务）')
        return str(job_id), list(node_tasks)

    def _poll(self, job_id: str) -> Mapping[str, Any]:
        """第 2 跳：轮询到 ``statistics.running == 0`` 为止。

        **结束判据不看 ``job_status``** —— 实测节点任务已 ``succeed`` 而
        ``job_status`` 仍停在 ``running``。
        """
        path = TASK_RECORDS_PATH.format(cmd_id=self._cmd_id)
        deadline = self._clock() + self._poll_timeout
        delay = self._poll_interval
        while True:
            body = self._client.get(
                path,
                params={'job_id': job_id, 'offset': 0, 'limit': TASK_RECORDS_LIMIT},
            )
            statistics = require_fields(body, ('statistics', 'tasks'), path).get('statistics')
            running = statistics.get('running')
            if running is None:
                raise ProtocolError('命令中心 task-records 的 statistics 里没有 running')
            if running == 0:
                return body
            if self._clock() >= deadline:
                raise TransientError(
                    f'命令中心任务轮询超时（job_id={job_id}，timeout={self._poll_timeout}s）'
                )
            self._sleep(delay)
            delay = min(delay * 1.5, MAX_POLL_INTERVAL)

    def _collect_tasks(
        self,
        job_id: str,
        node_tasks: Sequence[Mapping[str, Any]],
        records: Mapping[str, Any],
    ) -> List[NodeTask]:
        """第 3 跳：按任务取明细，**二次解析** ``result``。

        以**下发时拿到的 ``node_tasks``** 为准（只对下发的节点取结果），
        再用轮询响应的 ``tasks`` 补上每个任务的状态与失败原因。
        """
        by_task_id = _index_tasks(records)
        return [
            self._collect_one(job_id, node_task, by_task_id)
            for node_task in node_tasks
        ]

    def _collect_one(
        self,
        job_id: str,
        node_task: Mapping[str, Any],
        by_task_id: Mapping[str, Mapping[str, Any]],
    ) -> NodeTask:
        """取单个任务的明细并二次解析 ``result``。"""
        task_id = str(node_task.get('task_id') or '')
        node_id = str(node_task.get('node_id') or '')
        task = by_task_id.get(task_id, {})
        status = str(task.get('status') or '')
        error = task.get('error_msg')
        if status != 'succeed':
            return NodeTask(
                node_id=node_id,
                task_id=task_id,
                status=status,
                error=str(error) if error else status,
            )
        detail = self._fetch_detail(job_id, task_id)
        # **先看自述失败串再解析** —— 失败串不是合法 JSON，
        # 顺序反了会把「命令失败」误报成「result 不是合法 JSON」。
        if _result_indicates_failure(detail.get('result')):
            return NodeTask(
                node_id=node_id,
                task_id=task_id,
                status=status,
                error=FAILED_RESULT_MARKER,
            )
        return NodeTask(
            node_id=node_id,
            task_id=task_id,
            status=status,
            rows=parse_result_rows(detail.get('result')),
        )

    def _fetch_detail(self, job_id: str, task_id: str) -> Mapping[str, Any]:
        """取单个任务的明细。"""
        path = TASK_DETAIL_PATH.format(cmd_id=self._cmd_id, job_id=job_id, task_id=task_id)
        body = self._client.get(path)
        return require_fields(body, ('status',), path)


def _index_tasks(
    records: Mapping[str, Any],
) -> Mapping[str, Mapping[str, Any]]:
    """把轮询响应里的 ``tasks`` 按 ``task_id`` 建索引。"""
    by_task_id = {}  # type: Dict[str, Mapping[str, Any]]
    for record in records.get('tasks') or ():
        if isinstance(record, Mapping) and record.get('task_id'):
            by_task_id[str(record['task_id'])] = record
    return by_task_id


def _result_indicates_failure(result: Any) -> bool:
    """``status == "succeed"`` ≠ 命令真的成功。

    平台在 ``result`` 里塞自述失败串（实测 ``Failed to execute command on all
    nodes``）—— 这种情况要当失败处理。
    """
    if not isinstance(result, (bytes, str)):
        return False
    text = result.decode('utf-8') if isinstance(result, bytes) else result
    return FAILED_RESULT_MARKER in text


def _sql_label(sql_text: str, size: int = 60) -> str:
    """日志里的 SQL 摘要（截断，避免刷屏）。"""
    flat = ' '.join(sql_text.split())
    if len(flat) <= size:
        return flat
    return flat[:size] + '…'
