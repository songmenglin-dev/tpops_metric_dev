"""命令中心的结果数据类型与 ``result`` 二次解析。

与 :mod:`tpops_kafka_collector.tpops.command_center` 配套（任务 2.5）——
单独成文件是为了守住「单文件 < 400 行」的红线。

**核心坑**：平台任务明细里的 ``result`` 是 **JSON 字符串**，
要**再 ``json.loads`` 一次**才是行列表。**不要假设它已经是对象。**
"""
import json
from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Tuple  # noqa: F401  （List 仅用于 # type: 注释）

from ..errors import DecodeError, TransientError

__all__ = [
    'CommandTaskError',
    'NodeTask',
    'SqlOutcome',
    'parse_result_rows',
]


class CommandTaskError(TransientError):
    """节点任务执行失败 —— **可重试**（实测偶发失败，重试即成功）。

    继承 :class:`TransientError` → ``errors.is_retryable`` 判为可重试，
    调用方**不**另写一份按类名精确相等的判定。
    """


@dataclass(frozen=True)
class NodeTask:
    """单个 master 节点上的任务结果（不可变）。

    :param node_id: 节点 ID（TPOPS 的 ``node_id``）。
    :param task_id: 任务 ID。
    :param status: 平台任务状态（``succeed`` / ``failed`` / …）。
    :param rows: **二次解析**后的行列表。
    :param error: 失败原因（成功时为 ``None``）。
    """

    node_id: str
    task_id: str
    status: str
    rows: Tuple[Mapping[str, Any], ...] = ()
    error: Optional[str] = None


@dataclass(frozen=True)
class SqlOutcome:
    """一条 SQL 的执行结果（成功与失败都**显式**表达，不静默吞异常）。

    :param sql_text: 下发的 SQL 文本。
    :param ok: 是否成功。
    :param tasks: 各节点的任务结果（成功时非空）。
    :param error: 失败原因（成功时为 ``None``）。
    :param attempts: 实际尝试次数（含首次）。
    :param job_id: 平台任务作业 ID。
    :param exception: 失败时的原始异常（``is_retryable`` 判它是否可重试）。
    """

    sql_text: str
    ok: bool
    tasks: Tuple[NodeTask, ...] = ()
    error: Optional[str] = None
    attempts: int = 0
    job_id: Optional[str] = None
    exception: Optional[BaseException] = None


def parse_result_rows(result: Any) -> Tuple[Mapping[str, Any], ...]:
    """把任务明细的 ``result`` **二次解析**成行列表。

    - 空串 / ``None`` → 空行列表（不是失败）；
    - 已经是列表 → 直接用（防御平台改格式）；
    - 不是合法 JSON → 抛 :class:`DecodeError`（不可重试，要人工介入）。

    合并形态（一条合并 SQL）解析后是**恰好一行**，该行的每个键是一个视图名，
    值是该视图的 JSON 字符串 —— 视图级解析在映射层（``mapping.decode``），
    本层只做二次解析。
    """
    if result is None:
        return ()
    if isinstance(result, (bytes, str)):
        text = result.decode('utf-8') if isinstance(result, bytes) else result
        if not text.strip():
            return ()
        try:
            payload = json.loads(text)
        except ValueError as exc:
            raise DecodeError(f'命令中心 result 不是合法 JSON：{exc}')
    else:
        payload = result
    if not isinstance(payload, list):
        raise DecodeError(
            f'命令中心 result 期望行列表，实际是 {type(payload).__name__}'
        )
    rows = []  # type: List[Mapping[str, Any]]
    for item in payload:
        if not isinstance(item, Mapping):
            raise DecodeError(
                f'命令中心 result 里有非对象的行：{type(item).__name__}'
            )
        rows.append(dict(item))
    return tuple(rows)
