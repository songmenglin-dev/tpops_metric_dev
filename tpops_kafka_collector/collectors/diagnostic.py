# -*- coding: utf-8 -*-
"""C 类（诊断优化接口）采集器。

对应规格 ``specs/collection-channels/spec.md``「诊断优化通道」、
开发方案 §4.2.6。产出 4 个客户指标：

=================================  ======================================
指标                                 取值口径
=================================  ======================================
``GAUSSDB.Total_slow_queries``       慢 SQL 集合条数（``collection_count``）
``GAUSSDB.Select_slow_queries``      按 ``sql_text`` 前缀分类计数（SELECT 类）
``GAUSSDB.DML_slow_queries``         按 ``sql_text`` 前缀分类计数（DML 类）
``gaussdb.Top10sql``                 按 ``total_elapse_time`` 取前 10 条
=================================  ======================================

**口径全部来自映射表**（``data/mapping/metric_rules.json``）——
前缀清单（``select`` / ``with`` / ``insert`` / ``update`` / ``delete``）、
要取的集合名、排序列与条数上限都不写死在代码里。采集器只做
「取数 → 解码 → 调规则引擎 → 调 message 组装 → 交投递」。

三条硬约束：

1. **接口在 ``/rds/gaussdb/v3/`` 命名空间下** —— 少一段前缀就是 404
   （见 :mod:`tpops_kafka_collector.tpops.diagnostic`）。
2. **C 类指标与节点无关** —— 统一挂到节点清单里的**第一个 master CN**
   （``mapping/nodes.py`` 的 ``select_master_node`` /
   ``resolve_node_context``），跨轮稳定。
3. **``sql_text`` 外发前必须脱敏** —— 脱敏函数由装配层**注入**
   （``sql_sanitizer``）：本模块**不自己实现**脱敏，也不依赖某个具体
   脱敏模块；没有注入就**快速失败**，绝不把未脱敏的业务 SQL 放出去。

本模块**不推进水位**（那是调度器的职责），也**不拼 Kafka 消息**
（消息组装走 ``mapping/message.py``，序列化走 ``kafka/serializer.py``）。
"""

import logging
import time
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from ..errors import MappingError
from ..mapping.loader import LoadedMapping
from ..mapping.message import build_metric_message
from ..mapping.nodes import select_master_node
from ..mapping.rules import RuleContext, evaluate
from ..tpops.diagnostic import (
    COLLECTION_PATHS,
    COLLECTION_WINDOWS,
    fetch_diagnostic_collection,
)

LOGGER = logging.getLogger(__name__)

__all__ = ['DiagnosticCollector', 'SQL_TEXT_FIELDS']

#: 行里承载**业务 SQL 文本**的字段（外发前逐个脱敏）。
SQL_TEXT_FIELDS: Tuple[str, ...] = ('sql_text', 'sql', 'query_plan_optimize')

#: 采集通道名（映射表 ``source.source`` 的取值）。
CHANNEL = 'diagnostic'


class DiagnosticCollector(object):
    """C 类（诊断优化）采集器。

    :param client: 提供 ``post(path, payload)`` 的 TPOPS 客户端。
    :param mapping: 已加载的映射表（``mapping.loader.load_mapping()``）。
    :param sql_sanitizer: ``str -> str`` 的脱敏函数（**必填**）——
        装配层注入（默认实现见 :mod:`tpops_kafka_collector.mapping.masking`）。
        没有它就等于把客户的业务 SQL 原样外发，因此**快速失败**。
    :param clock: 返回毫秒时间戳的可注入时钟（测试用）。
    """

    def __init__(
        self,
        client: Any,
        mapping: LoadedMapping,
        sql_sanitizer: Callable[[str], str],
        clock: Optional[Callable[[], int]] = None,
    ) -> None:
        if not callable(sql_sanitizer):
            raise ValueError(
                '诊断优化采集器必须注入 sql_sanitizer（慢 SQL 文本外发前必须脱敏）'
            )
        self._client = client
        self._mapping = mapping
        self._sanitizer = sql_sanitizer
        self._clock = clock if clock is not None else _now_ms

    # -- 规则 ------------------------------------------------------------
    @property
    def rules(self) -> Tuple[Any, ...]:
        """本采集器负责的规则（映射表里通道为 ``diagnostic`` 的启用规则）。"""
        return tuple(
            rule for rule in self._mapping.metric_rules
            if rule.enabled and (rule.source or {}).get('source') == CHANNEL
        )

    # -- 主流程 ----------------------------------------------------------
    def collect(
        self,
        instance_id: str,
        nodes: Optional[Sequence[Mapping[str, Any]]] = None,
        instance_name: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], ...]:
        """采集一轮 C 类指标，返回**已组装好的 ITSCADA 指标消息**元组。

        :param instance_id: 实例 ID。
        :param nodes: 节点清单（``key-view-execute-node`` 的结果）——
            C 类指标与节点无关，用它挑出第一个 master CN 作为资源落点。
        :param instance_name: 实例名（可空；取不到就填 ``null``）。
        :return: 指标消息元组（每条已是 ITSCADA 的 7 字段信封）。
        """
        master = select_master_node(nodes)
        if master is None:
            LOGGER.error(
                'C 类指标无处落点：节点清单里没有 master CN —— 本轮不产出 C 类消息'
            )
            return ()
        resource_context = {
            'instance_id': instance_id,
            'instance_name': instance_name,
            'component_id': master.get('component_id'),
        }
        now_ms = int(self._clock())
        contexts = self._fetch_collections(instance_id, nodes or ())
        messages = []  # type: list
        for rule in self.rules:
            messages.extend(self._rule_messages(
                rule, contexts, resource_context, nodes or (), now_ms,
            ))
        return tuple(messages)

    # -- 取数 ------------------------------------------------------------
    def _fetch_collections(
        self,
        instance_id: str,
        nodes: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, RuleContext]:
        """按映射表声明的集合名取数并解码成规则上下文。"""
        node_ids = _query_node_ids(nodes)
        now_ms = int(self._clock())
        collections = {}  # type: Dict[str, Tuple[Mapping[str, Any], ...]]
        for collection in self._required_collections():
            window = COLLECTION_WINDOWS.get(collection) or 0
            rows = fetch_diagnostic_collection(
                client=self._client,
                collection=collection,
                instance_id=instance_id,
                node_ids=node_ids,
                start_time=now_ms - window,
                end_time=now_ms,
            )
            if not rows:
                LOGGER.warning(
                    '诊断优化集合 %s 本轮为空（%s）—— 计数值按 0 记，明细类指标不产出',
                    collection, instance_id,
                )
            collections[collection] = self._sanitize_rows(rows)
        return {
            name: RuleContext(collections={name: rows}, now_ms=now_ms)
            for name, rows in collections.items()
        }

    def _required_collections(self) -> Tuple[str, ...]:
        """本轮要取的集合（按映射表声明，不写死）。"""
        names = []  # type: list
        for rule in self.rules:
            params = (rule.value_rule or {}).get('params') or {}
            collection = params.get('collection')
            if collection and collection not in names:
                names.append(collection)
        return tuple(names)

    # -- 取值与组装 ------------------------------------------------------
    def _rule_messages(
        self,
        rule: Any,
        contexts: Mapping[str, RuleContext],
        resource_context: Mapping[str, Any],
        nodes: Sequence[Mapping[str, Any]],
        now_ms: int,
    ) -> Tuple[Dict[str, Any], ...]:
        """执行一条规则的取值规则并组装消息。"""
        params = (rule.value_rule or {}).get('params') or {}
        collection = params.get('collection')
        context = contexts.get(collection or '')
        if context is None:
            LOGGER.error(
                'C 类指标 %s 声明的集合 %s 本轮没有取到 —— 不产出该指标',
                rule.metric_code, collection,
            )
            return ()
        messages = []  # type: list
        for dimensions, value, timestamp in evaluate(rule, context):
            if value is None:
                LOGGER.warning(
                    'C 类指标 %s 本轮取不到值 —— 不产出该条消息（缺值不填 0）',
                    rule.metric_code,
                )
                continue
            if isinstance(value, (list, tuple)) and not value:
                LOGGER.warning(
                    'C 类指标 %s 本轮没有可产出的明细 —— 不产出该条消息',
                    rule.metric_code,
                )
                continue
            messages.append(build_metric_message(
                field_map=self._mapping.field_map,
                metric_rule=rule,
                dimensions=dimensions,
                value=value,
                resource_context=resource_context,
                timestamp=int(timestamp or now_ms),
                nodes=nodes,
            ))
        return tuple(messages)

    # -- 脱敏 ------------------------------------------------------------
    def _sanitize_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
    ) -> Tuple[Mapping[str, Any], ...]:
        """把行里的业务 SQL 文本逐个脱敏（返回新行，不改原对象）。"""
        sanitized = []  # type: list
        for row in rows:
            new_row = dict(row)
            for field in SQL_TEXT_FIELDS:
                text = new_row.get(field)
                if isinstance(text, str) and text:
                    new_row[field] = self._sanitize(text, field)
            sanitized.append(new_row)
        return tuple(sanitized)

    def _sanitize(self, text: str, field: str) -> str:
        """调用注入的脱敏函数；失败时包装成带上下文的异常。

        **异常消息里不带 SQL 文本** —— 业务 SQL 不进日志、不进异常消息。
        """
        try:
            return self._sanitizer(text)
        except MappingError:
            raise
        except Exception as exc:
            raise MappingError(
                '慢 SQL 文本脱敏失败（字段 {0}）：{1}'.format(field, exc)
            ) from exc


# -- 内部工具 ------------------------------------------------------------
def _now_ms() -> int:
    """当前时刻的毫秒时间戳（13 位）。"""
    return int(time.time() * 1000)


def _query_node_ids(nodes: Sequence[Mapping[str, Any]]) -> Tuple[str, ...]:
    """请求里要带的 ``node_ids``（master CN 优先，按 ``node_id`` 去重）。

    实测留档带的就是 3 个 master CN 的 ``node_id``；同一个 ``node_id``
    在节点清单里会出现多次（CN 一条 + 各 DN 一条）—— 必须去重。
    """
    candidates = [
        node for node in nodes
        if node.get('role') == 'master' and node.get('type') == 'CN'
    ] or list(nodes)
    node_ids = []  # type: list
    for node in candidates:
        node_id = node.get('node_id')
        if node_id and node_id not in node_ids:
            node_ids.append(node_id)
    return tuple(node_ids)
