"""B 类采集器：命令中心合并 SQL → ITSCADA 指标消息。

对应规格 ``specs/collection-channels/spec.md`` 的「命令中心 SQL 通道」、
开发方案 §4.3。**82 个客户指标**走这一条通道（项目里最大的一块）。

五条硬约束（前人实测踩过的）：

1. **必须走 ``decode_merged`` / ``merge_pages``** —— 合并 SQL 返回的是
   「一行 N 个 ``json_agg`` 列」，把它当行列表用，82 个指标**全部静默产出 0**
   且不报错。这是本项目最危险的失败形态。
2. **必须检查 ``outcome.ok``** —— 命令中心**失败不抛异常**，而是返回
   ``SqlOutcome``（失败原因挂在 ``error`` / ``exception`` 上）。不检查就是静默丢数据。
3. **分页只对可增长视图生效** —— 固定视图每页都会重复返回全量数据，累加会翻倍。
4. **``node_scoped`` 归集** —— ``true`` 的指标各 master 各自产出一份；
   ``false`` 的与节点无关，只取**第一个 master**。
5. **输出上限 102400 B** —— 超了是**整条失败**（不是截断）。
   接近上限（90%）记 WARNING；真超了就**缩小页容量重试**。

采集器**不推进水位**（那是 wave 3 调度器的职责），只返回结果。
"""

import hashlib
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from ..errors import MappingError
from ..mapping.decode import decode_merged, merge_pages
from ..mapping.message import build_metric_message
from ..mapping.nodes import select_master_node
from ..mapping.rules import evaluate
from ..mapping.sqlgen import MergedSqlSpec, load_spec, render_sql
from ..tpops.metrics import utc_now_ms

LOGGER = logging.getLogger(__name__)

__all__ = [
    'CommandCenterCollector',
    'CommandCenterCollectResult',
    'OUTPUT_LIMIT_BYTES',
]

#: 平台时序通道（映射层的 ``source.source`` 值）。
COMMAND_CENTER_SOURCE = 'command_center_sql'

#: 平台 Agent 对命令输出的硬上限（字节）—— **超了整条任务失败，不是截断**。
OUTPUT_LIMIT_BYTES = 102400

#: 接近上限到这个比例就记 WARNING。
OUTPUT_WARN_RATIO = 0.9

#: 缩小页容量的下限（再小就没有意义了）。
MIN_PAGE_SIZE = 10

#: 合并 SQL 的外层 limit（命令 ``general_single_sql_query`` 的模板是
#: ``select * from ({sql_text}) limit {limit}``，外层只取 1 行）。
OUTER_LIMIT = '1000'


@dataclass(frozen=True)
class CommandCenterCollectResult:
    """一轮 B 类采集的结果。

    :param messages: 已组装好的 ITSCADA 指标消息元组。
    :param tier_seconds: 本轮档位（秒）。
    :param tier_label: 本轮档位的人话标签。
    :param pages: 本轮实际翻了几页。
    :param page_size: 本轮实际用的页容量（超限时会被缩小）。
    """

    messages: Tuple[Dict[str, Any], ...] = ()
    tier_seconds: int = 0
    tier_label: str = ''
    pages: int = 0
    page_size: int = 0


class CommandCenterCollector:
    """B 类采集器：命令中心合并 SQL → ITSCADA 指标消息。

    :param client: 提供 ``get(path, params)`` 的 TPOPS 客户端（供命令中心用）
    :param mapping: :class:`tpops_kafka_collector.mapping.loader.LoadedMapping`
    :param command_center: :class:`tpops_kafka_collector.tpops.command_center.CommandCenterClient`
    :param spec: 合并 SQL 规格；``None`` 时读包内 ``merged_sql.json``
    :param page_size: 每页行数上限（可增长视图）
    :param deltas: 差分基线存储（``counter_delta`` 规则要用）
    :param clock: 返回当前毫秒时间戳的可注入时钟
    """

    def __init__(
        self,
        client: Any,
        mapping: Any,
        command_center: Any,
        spec: Optional[MergedSqlSpec] = None,
        page_size: Optional[int] = None,
        deltas: Any = None,
        clock: Optional[Callable[[], int]] = None,
    ) -> None:
        self._client = client
        self._mapping = mapping
        self._cc = command_center
        self._spec = spec if spec is not None else load_spec()
        self._page_size = page_size or self._spec.default_page_size
        self._deltas = deltas
        self._clock = clock if clock is not None else utc_now_ms
        self._rules = _command_center_rules(mapping)
        self._views = _merged_views(mapping)
        self._paged_view_names = tuple(
            name for name, spec in self._views.items() if spec.get('paged')
        )

    # ------------------------------------------------------------------
    # 对外入口
    # ------------------------------------------------------------------

    def collect(
        self,
        instance_id: str,
        instance_name: str = '',
        rules: Optional[Sequence[Any]] = None,
    ) -> CommandCenterCollectResult:
        """采一轮 B 类指标。

        :param instance_id: 目标实例 ID。
        :param instance_name: 实例名（进 ``tags``）。
        :param rules: 本轮要产出的规则；``None`` 表示全部 B 类规则
            （调度器按档位传子集）。
        """
        targets = tuple(rules) if rules is not None else self._rules
        tier_seconds, tier_label = _tier_of(targets)
        page_size = self._page_size
        bodies, pages = self._fetch_pages(instance_id, page_size)
        if bodies is None:
            return CommandCenterCollectResult(
                tier_seconds=tier_seconds, tier_label=tier_label,
                page_size=page_size,
            )
        merged_by_node = self._merge(bodies)
        messages = self._build_messages(
            instance_id, instance_name, targets, merged_by_node,
        )
        return CommandCenterCollectResult(
            messages=tuple(messages),
            tier_seconds=tier_seconds,
            tier_label=tier_label,
            pages=pages,
            page_size=page_size,
        )

    # ------------------------------------------------------------------
    # 取数：分页下发 1 条合并 SQL
    # ------------------------------------------------------------------

    def _fetch_pages(
        self, instance_id: str, page_size: int
    ) -> Tuple[Optional[Dict[str, List[Any]]], int]:
        """按 ``paging`` 推进分页，把每个 master 的各页响应体收起来。

        返回 ``(每个 master 的页列表, 页数)``；取数失败时返回 ``(None, 0)``。

        **输出超限时缩小页容量重试** —— 平台对命令输出有 102400 B 硬上限，
        超了整条任务失败（不是截断），只能靠更小的页容量把它拿回来。
        """
        bodies = {}  # type: Dict[str, List[Any]]
        offset = 0
        pages = 0
        previous = {}  # type: Dict[str, Any]
        while True:
            sql_text = render_sql(
                self._spec, page_size=page_size, offset=offset,
            )
            outcome = self._cc.execute_sql(instance_id, sql_text, OUTER_LIMIT)
            if not outcome.ok:
                if _is_output_limit_error(outcome.error) and page_size > MIN_PAGE_SIZE:
                    smaller = max(MIN_PAGE_SIZE, page_size // 2)
                    LOGGER.warning(
                        '命令中心输出超过平台上限（%d B），页容量 %d → %d 重试',
                        OUTPUT_LIMIT_BYTES, page_size, smaller,
                    )
                    retry_bodies, retry_pages = self._fetch_pages(
                        instance_id, smaller,
                    )
                    if retry_bodies is None:
                        return None, 0
                    self._page_size = smaller
                    return retry_bodies, retry_pages
                LOGGER.error(
                    '命令中心取数失败（实例 %s，第 %d 页）：%s',
                    instance_id, offset, outcome.error,
                )
                return None, 0

            page_contexts = {}  # type: Dict[str, Any]
            signatures = {}  # type: Dict[str, Any]
            for task in outcome.tasks:
                body = {'result': list(task.rows)}
                bodies.setdefault(task.node_id, []).append(body)
                page_contexts[task.node_id] = decode_merged(
                    body, self._views,
                )
                signatures[task.node_id] = _page_signature(task.rows)
            pages += 1
            self._warn_if_near_limit(outcome, instance_id)
            if not _has_more(page_contexts, page_size, self._paged_views()):
                break
            # 平台若**不认 offset**（每页返回同一批行），再翻也是同一页 ——
            # 认出来并停下，否则采集器会**无限翻页**把整轮卡死。
            if _same_as_previous(signatures, previous):
                LOGGER.warning(
                    '命令中心返回了与上一页相同的内容（实例 %s，offset %d）—— '
                    '平台可能未生效 offset，本轮停止翻页',
                    instance_id, offset,
                )
                break
            previous = signatures
            offset += page_size
        return bodies, pages

    def _warn_if_near_limit(self, outcome: Any, instance_id: str) -> None:
        """结果接近平台输出上限时记 WARNING（超了会整条失败）。

        上限是**每个节点任务**的（一个 master 一条命令输出），
        所以按**单任务**算，不是把各 master 加总。
        """
        threshold = int(OUTPUT_LIMIT_BYTES * OUTPUT_WARN_RATIO)
        for task in outcome.tasks:
            size = _approx_size_bytes(task.rows)
            if size >= threshold:
                LOGGER.warning(
                    '命令中心单个节点的结果接近平台输出上限（%d / %d B，'
                    '节点 %s，实例 %s）—— 建议缩小页容量',
                    size, OUTPUT_LIMIT_BYTES, task.node_id, instance_id,
                )

    # ------------------------------------------------------------------
    # 解码与归集
    # ------------------------------------------------------------------

    def _merge(self, bodies: Dict[str, List[Any]]) -> Tuple[Tuple[Any, ...], ...]:
        """每个 master 的各页拼成完整视图（``merge_pages``）。

        **必须用 ``merge_pages``** —— 它只对可增长视图做跨页拼接，
        固定视图只取第一页（每页都重复返回全量，累加会翻倍）。
        """
        merged = []
        for node_id, pages in bodies.items():
            contexts = merge_pages(pages, self._views)
            merged.append((node_id, contexts))
        return tuple(merged)

    def _build_messages(
        self,
        instance_id: str,
        instance_name: str,
        rules: Sequence[Any],
        merged_by_node: Tuple[Tuple[Any, Any], ...],
    ) -> List[Dict[str, Any]]:
        """按 ``node_scoped`` 归集多 master 结果并组装消息。"""
        nodes = self._master_nodes(instance_id)
        values_by_node = {}
        for node_id, contexts in merged_by_node:
            values = {}
            for rule in rules:
                sql_key = rule.source.get('sql_key')
                ctx = contexts.by_sql_key.get(sql_key)
                if ctx is None:
                    continue
                values[rule.metric_code] = evaluate(rule, ctx, self._deltas)
            values_by_node[node_id] = values

        # 与节点无关的指标，值和资源名必须来自**同一个** master ——
        # 否则会出现「值取自 A 节点、资源名却写着 B 节点」的错配。
        host_node_id = _host_node_id(nodes, list(values_by_node))
        shared = values_by_node.get(host_node_id) or {}
        if not shared:
            # 节点清单里的第一个 master 不在结果里（平台返回的 master 集合不同）
            # → 退回结果的第一个，但**要记 WARNING**，别静默错配。
            if values_by_node:
                shared = list(values_by_node.values())[0]
                LOGGER.warning(
                    '承载资源名的 master（%s）不在本轮结果里 —— '
                    '与节点无关的指标退回用第一个结果节点的值',
                    host_node_id,
                )

        messages = []  # type: List[Dict[str, Any]]
        for rule in rules:
            # node_scoped=False 的指标与节点无关 → 取**承载资源名**的那个 master
            targets = (
                list(values_by_node.items())
                if rule.node_scoped
                else [(host_node_id, shared)]
            )
            for node_id, values in targets:
                for dims, value, timestamp in values.get(rule.metric_code) or ():
                    if value is None:
                        continue
                    messages.append(build_metric_message(
                        field_map=self._mapping.field_map,
                        metric_rule=rule,
                        dimensions=dims,
                        value=value,
                        resource_context=self._resource_context(
                            instance_id, instance_name, node_id, rule.node_scoped,
                        ),
                        timestamp=timestamp,
                        nodes=nodes,
                    ))
        return messages

    def _resource_context(
        self,
        instance_id: str,
        instance_name: str,
        node_id: Any,
        node_scoped: bool,
    ) -> Dict[str, Any]:
        """资源上下文。

        ``node_scoped`` 的指标锚在**自己那个 master** 上；
        与节点无关的指标不写 ``node_id``，由 ``resolve_node_context``
        兜底到节点清单里的**第一个 master CN**（跨轮稳定）。
        """
        context = {
            'instance_id': instance_id,
            'instance_name': instance_name,
        }  # type: Dict[str, Any]
        if node_scoped:
            context['node_id'] = node_id
        return context

    def _paged_views(self) -> Tuple[str, ...]:
        """可增长视图的名字（分页只对它们生效）。"""
        return self._paged_view_names

    def _master_nodes(self, instance_id: str) -> Tuple[Mapping[str, Any], ...]:
        """master 节点清单（用来把 node_id 换算成 host_ip）。"""
        getter = getattr(self._cc, 'master_nodes', None)
        if getter is None:
            return ()
        try:
            return tuple(getter(instance_id))
        except Exception:  # noqa: B902 —— 节点清单取不到不该毁掉整轮采集
            return ()


# ---------------------------------------------------------------------------
# 内部辅助
# ---------------------------------------------------------------------------

def _command_center_rules(mapping: Any) -> Tuple[Any, ...]:
    """映射表里属于 B 类（命令中心 SQL）的启用规则。"""
    rules = []  # type: List[Any]
    for rule in getattr(mapping, 'metric_rules', None) or ():
        if not rule.enabled:
            continue
        if rule.source.get('source') == COMMAND_CENTER_SOURCE:
            rules.append(rule)
    return tuple(rules)


def _merged_views(mapping: Any) -> Mapping[str, Any]:
    """映射表里的 ``merged_views`` 段（视图名 → json 列 / 分页标记）。"""
    payload = getattr(mapping, 'response_rules', None) or {}
    views = payload.get('merged_views') or {}
    if not views:
        raise MappingError('映射表里没有 merged_views 段 —— 合并形态无法解码')
    return views


def _has_more(
    page_contexts: Mapping[str, Any],
    page_size: int,
    paged_views: Sequence[str],
) -> bool:
    """还有下一页吗？

    **只看可增长视图**（``paged = true``）—— 固定视图每页都重复返回全量数据，
    拿它判「还有下一页」会**死循环**。
    """
    for contexts in page_contexts.values():
        for name in paged_views:
            rows = contexts.rows_by_key.get(name) or ()
            if len(rows) >= page_size:
                return True
    return False


def _approx_size_bytes(rows: Sequence[Any]) -> int:
    """估算一行批结果的**字节**体积（用于接近输出上限的告警）。

    **不用 ``json.dumps``** —— 产出消息的 ``json.dumps`` 在生产包里只有
    ``kafka/serializer.py`` 一处（AST 守卫全包扫），这里只做体积估算。
    """
    total = 0
    for row in rows:
        if isinstance(row, Mapping):
            for value in row.values():
                total += len(str(value).encode('utf-8'))
        else:
            total += len(str(row).encode('utf-8'))
    return total


def _host_node_id(
    nodes: Sequence[Mapping[str, Any]], result_node_ids: Sequence[str]
) -> str:
    """承载资源名的 master —— 节点清单里的**第一个 master CN**。

    与节点无关的指标（``node_scoped = false``）挂在这个节点上，
    所以它们的**值**也必须取自这个节点 —— 值和资源名必须同源。
    """
    host = select_master_node(nodes)
    if host is not None:
        return str(host.get('node_id') or '')
    return result_node_ids[0] if result_node_ids else ''


def _page_signature(rows: Sequence[Any]) -> str:
    """一页的指纹（用来认出「平台没生效 offset，返回了同一页」）。

    **不用 ``json.dumps``** —— 生产包里产出消息的 ``json.dumps`` 只有
    ``kafka/serializer.py`` 一处（有 AST 守卫全包扫），
    这里用 ``repr`` 的哈希做指纹就够。
    """
    return hashlib.sha256(repr(list(rows)).encode('utf-8')).hexdigest()


def _same_as_previous(
    signatures: Mapping[str, Any], previous: Mapping[str, Any]
) -> bool:
    """这一页是不是和上一页一模一样。"""
    if not previous:
        return False
    return all(
        signatures.get(node_id) == signature
        for node_id, signature in previous.items()
    )


def _is_output_limit_error(error: Any) -> bool:
    """失败是不是因为平台输出超了 102400 B。"""
    text = str(error or '')
    return 'exceeds the threshold' in text or str(OUTPUT_LIMIT_BYTES) in text


def _tier_of(rules: Sequence[Any]) -> Tuple[int, str]:
    """本轮档位：取各指标里**最高**的档（同长度档位视为同档）。"""
    units = {'秒': 1, '分': 60, '时': 3600, '天': 86400}
    best = 0
    label = ''
    for rule in rules:
        cycle = getattr(rule, 'collect_cycle', None) or {}
        unit = str(cycle.get('unit') or '')
        factor = units.get(unit)
        if factor is None:
            continue
        seconds = int(cycle.get('value') or 0) * factor
        if seconds > best:
            best = seconds
            label = f'{cycle.get("value")} {cycle.get("unit")}'
    return best, label
