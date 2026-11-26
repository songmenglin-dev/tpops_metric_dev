"""B 类采集器：命令中心合并 SQL → ITSCADA 指标消息。

对应规格 ``specs/collection-channels/spec.md`` 的「命令中心 SQL 通道」、
开发方案 §4.3。**82 个客户指标**走这一条通道（项目里最大的一块）。

六条硬约束（前人实测踩过的）：

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
6. **采集时刻必须传下去** —— ``decode_merged`` 的 ``now_ms`` 决定差分基线的
   时间轴；不传（恒为 0）则 ``counter_delta`` 永远算不出速率（实测 5 个
   ``*_records_per_second`` 指标一条都不产出）。

**差分基线的生命周期**（``counter_delta`` 规则要用）：

- 基线住在 :class:`~tpops_kafka_collector.mapping.rules_deltas.CounterDeltaStore`
  里，**由采集器持有**（没注入时采集器自己建一个）；
- **采集器实例必须跨轮复用** —— 差分要靠「上一轮的基线」才算得出速率，
  每轮新建一个采集器就等于每轮都只有首轮（只建基线、不出值）；
- 同一轮的多个 master **共用一个存储**，但基线**按节点隔离**
  （``ScopedCounterDeltaStore``）—— 基线键不含节点，不隔离就会互相覆盖；
  作用域是 ``f'{instance_id}:{node_id}'`` —— **实例 ID 也要带上**，
  采集器对象在多实例间复用时视图缓存是共享的。

采集器**不推进水位**（那是 wave 3 调度器的职责），只返回结果。
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple
from loguru import logger

from ..mapping.decode import decode_merged, merge_pages
from ..mapping.rules_deltas import CounterDeltaStore, ScopedCounterDeltaStore
from ..mapping.sqlgen import MergedSqlSpec, load_spec, render_sql
from ..tpops.metrics import utc_now_ms
from .command_center_assembly import build_messages
from .command_center_helpers import (
    COMMAND_CENTER_SOURCE,
    MIN_PAGE_SIZE,
    OUTER_LIMIT,
    OUTPUT_LIMIT_BYTES,
    OUTPUT_WARN_RATIO,
    approx_size_bytes,
    command_center_rules,
    has_more,
    is_output_limit_error,
    merged_views,
    page_signature,
    same_as_previous,
    tier_of,
)

LOGGER = logger.bind(channel="command_center")

__all__ = [
    'COMMAND_CENTER_SOURCE',
    'CommandCenterCollector',
    'CommandCenterCollectResult',
    'MIN_PAGE_SIZE',
    'OUTER_LIMIT',
    'OUTPUT_LIMIT_BYTES',
    'OUTPUT_WARN_RATIO',
]


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
    :param deltas: 差分基线存储（``counter_delta`` 规则要用）；
        ``None`` 时采集器自建一个 —— **它必须跨轮存活**（见模块说明）
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
        self._deltas = deltas if deltas is not None else CounterDeltaStore()
        self._clock = clock if clock is not None else utc_now_ms
        self._rules = command_center_rules(mapping)
        self._views = merged_views(mapping)
        self._paged_view_names = tuple(
            name for name, spec in self._views.items() if spec.get('paged')
        )
        #: 每个 master 的差分基线视图（**视图无状态**，基线在 ``_deltas`` 里）
        self._scoped_deltas = {}  # type: Dict[str, ScopedCounterDeltaStore]

    # ------------------------------------------------------------------
    # 对外入口
    # ------------------------------------------------------------------

    @classmethod
    def from_config(
        cls,
        client: Any,
        mapping: Any,
        command_center: Any,
        config: Any,
        **kwargs: Any,
    ) -> 'CommandCenterCollector':
        """按运行期配置装配（页容量取 ``CMD_PAGE_SIZE``）。"""
        kwargs.setdefault('page_size', getattr(config, 'cmd_page_size', None))
        return cls(
            client=client, mapping=mapping, command_center=command_center, **kwargs
        )

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
        tier_seconds, tier_label = tier_of(targets)
        # **同一轮内所有页共用同一个采集时刻** —— 差分基线的时间轴必须一致
        now_ms = int(self._clock())
        bodies, pages = self._fetch_pages(instance_id, self._page_size, now_ms)
        if bodies is None:
            return CommandCenterCollectResult(
                tier_seconds=tier_seconds, tier_label=tier_label,
                page_size=self._page_size,
            )
        merged_by_node = self._merge(bodies, now_ms)
        messages = build_messages(
            mapping=self._mapping,
            rules=targets,
            merged_by_node=merged_by_node,
            nodes=self._master_nodes(instance_id),
            instance_id=instance_id,
            instance_name=instance_name,
            delta_store=lambda node_id: self._delta_store(instance_id, node_id),
        )
        return CommandCenterCollectResult(
            messages=tuple(messages),
            tier_seconds=tier_seconds,
            tier_label=tier_label,
            pages=pages,
            # 回报**实际用的**页容量（超限时被缩小过）
            page_size=self._page_size,
        )

    # ------------------------------------------------------------------
    # 取数：分页下发 1 条合并 SQL
    # ------------------------------------------------------------------

    def _fetch_pages(
        self, instance_id: str, page_size: int, now_ms: int
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
                return self._handle_failure(
                    instance_id, offset, outcome, page_size, now_ms,
                )
            page_contexts = {}  # type: Dict[str, Any]
            signatures = {}  # type: Dict[str, Any]
            for task in outcome.tasks:
                body = {'result': list(task.rows)}
                bodies.setdefault(task.node_id, []).append(body)
                page_contexts[task.node_id] = decode_merged(
                    body, self._views, now_ms,
                )
                signatures[task.node_id] = page_signature(task.rows)
            pages += 1
            self._warn_if_near_limit(outcome, instance_id)
            if not has_more(page_contexts, page_size, self._paged_view_names):
                break
            # 平台若**不认 offset**（每页返回同一批行），再翻也是同一页 ——
            # 认出来并停下，否则采集器会**无限翻页**把整轮卡死。
            if same_as_previous(signatures, previous):
                LOGGER.warning(
                    '命令中心返回了与上一页相同的内容（实例 {}，offset {}）—— '
                    '平台可能未生效 offset，本轮停止翻页',
                    instance_id, offset,
                )
                break
            previous = signatures
            offset += page_size
        return bodies, pages

    def _handle_failure(
        self,
        instance_id: str,
        offset: int,
        outcome: Any,
        page_size: int,
        now_ms: int,
    ) -> Tuple[Optional[Dict[str, List[Any]]], int]:
        """取数失败的处理：输出超限就缩小页容量重试，否则记 ERROR 并放弃本轮。"""
        if is_output_limit_error(outcome.error) and page_size > MIN_PAGE_SIZE:
            smaller = max(MIN_PAGE_SIZE, page_size // 2)
            LOGGER.warning(
                '命令中心输出超过平台上限（{} B），页容量 {} → {} 重试',
                OUTPUT_LIMIT_BYTES, page_size, smaller,
            )
            retry_bodies, retry_pages = self._fetch_pages(
                instance_id, smaller, now_ms,
            )
            if retry_bodies is None:
                return None, 0
            self._page_size = smaller
            return retry_bodies, retry_pages
        LOGGER.error(
            '命令中心取数失败（实例 {}，第 {} 页）：{}',
            instance_id, offset, outcome.error,
        )
        return None, 0

    def _warn_if_near_limit(self, outcome: Any, instance_id: str) -> None:
        """结果接近平台输出上限时记 WARNING（超了会整条失败）。

        上限是**每个节点任务**的（一个 master 一条命令输出），
        所以按**单任务**算，不是把各 master 加总。
        """
        threshold = int(OUTPUT_LIMIT_BYTES * OUTPUT_WARN_RATIO)
        for task in outcome.tasks:
            size = approx_size_bytes(task.rows)
            if size >= threshold:
                LOGGER.warning(
                    '命令中心单个节点的结果接近平台输出上限（{} / {} B，'
                    '节点 {}，实例 {}）—— 建议缩小页容量',
                    size, OUTPUT_LIMIT_BYTES, task.node_id, instance_id,
                )

    # ------------------------------------------------------------------
    # 解码与归集
    # ------------------------------------------------------------------

    def _merge(
        self, bodies: Dict[str, List[Any]], now_ms: int
    ) -> Tuple[Tuple[Any, ...], ...]:
        """每个 master 的各页拼成完整视图（``merge_pages``）。

        **必须用 ``merge_pages``** —— 它只对可增长视图做跨页拼接，
        固定视图只取第一页（每页都重复返回全量，累加会翻倍）。
        """
        merged = []
        for node_id, pages in bodies.items():
            contexts = merge_pages(pages, self._views, now_ms)
            merged.append((node_id, contexts))
        return tuple(merged)

    def _delta_store(self, instance_id: str, node_id: Any) -> ScopedCounterDeltaStore:
        """该 master 的差分基线视图。

        **按节点隔离** —— 基线键 ``(metric_code, dimension_key)`` 不含节点，
        三个 master 共用一个存储时基线会互相覆盖（实测 2/3 的 master 恒为
        ``None``，还可能算出跨节点的速率）。

        **作用域还要带上实例 ID** —— 采集器对象在多实例间复用时，
        视图缓存是共享的；只用 ``node_id`` 做键的话，两个实例的同名节点
        会共用一个视图 → 基线互相覆盖 → **不报错、但速率是错的**。
        """
        key = f'{instance_id}:{node_id}'
        store = self._scoped_deltas.get(key)
        if store is None:
            store = ScopedCounterDeltaStore(self._deltas, key)
            self._scoped_deltas[key] = store
        return store

    def _paged_views(self) -> Tuple[str, ...]:
        """可增长视图的名字（分页只对它们生效）。"""
        return self._paged_view_names

    def _master_nodes(self, instance_id: str) -> Tuple[Mapping[str, Any], ...]:
        """master 节点清单（用来把 node_id 换算成 host_ip）。

        **取不到必须记 ERROR** —— 节点清单缺失会让 ``resourceName`` 退化成
        只有 node_id、资源名指错节点，而且全程没有日志（「不报错但数据错」）。
        """
        getter = getattr(self._cc, 'master_nodes', None)
        if getter is None:
            LOGGER.error(
                '命令中心客户端没有 master_nodes（实例 {}）—— '
                '节点身份换不出来，资源名会指错节点',
                instance_id,
            )
            return ()
        try:
            return tuple(getter(instance_id))
        except Exception as exc:  # noqa: B902 —— 取不到清单不该毁掉整轮采集
            LOGGER.error(
                '命令中心节点清单取不到（实例 {}）：{} —— '
                '节点身份换不出来，资源名会指错节点',
                instance_id, exc,
            )
            return ()
