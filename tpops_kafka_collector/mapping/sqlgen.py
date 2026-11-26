"""合并 SQL 的**运行时渲染**。

把 ``tools/build_sql_merge.py`` 的渲染逻辑**复制**进生产包 —— 生产包不得
import ``tools/``（设计决策 2），但渲染必须与生成器**同源同义**，否则
「跑起来的 SQL」与「留档的 SQL」会分叉。

数据源是**包内** ``mapping/data/merged_sql.json``（设计决策 9）：采集端是独立
部署的包，运行时不读仓库根目录。路径基于 ``__file__`` 解析 —— 采集端是
**常驻进程**，可能从任意工作目录启动，不能假设 cwd。

三个入口：

- :func:`render_sql` —— 显式传入 ``limit`` / ``offset``（采集器按 offset 翻页）；
- :func:`render_sql_with_config` —— 页容量取配置默认值（``CMD_PAGE_SIZE``）；
- :func:`render_sql_document` —— 复现归档产物（注释头 + SQL），
  用于与包内 ``sql_merge.sql`` 逐字符比对。

分页语义（与生成器一致）：

- **可增长视图**（``paged = true``）：剥掉原 SQL 的 ``limit`` 护栏，
  外层套 ``limit P offset O``；
- **固定视图**（``paged = false``，含 ``paging.views`` 未声明的）：保留语义上限
  （如 ``activity_running`` 的 ``limit 20``）—— 再套一个 limit 会变成
  ``limit 20 limit 100`` 的语法错误；offset 恒为 0。
"""

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Tuple
from loguru import logger

from ..config import Config
from ..errors import MappingError

__all__ = [
    'MergedSqlSpec', 'MergedView', 'PagingRule',
    'PACKAGE_DATA_DIR', 'PACKAGE_MERGED_SQL_PATH', 'PACKAGE_SQL_MERGE_PATH',
    'load_spec', 'render_sql', 'render_sql_document', 'render_sql_with_config',
    'select_views',
]

LOGGER = logger

#: 包内映射数据目录（**不依赖仓库根、不依赖 cwd**）。
PACKAGE_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

#: 包内合并 SQL 声明（11 个视图定义 + 刷新档位 + paging 段）。
PACKAGE_MERGED_SQL_PATH = os.path.join(PACKAGE_DATA_DIR, 'merged_sql.json')

#: 包内归档的合并 SQL（第 1 页留档）。
PACKAGE_SQL_MERGE_PATH = os.path.join(PACKAGE_DATA_DIR, 'sql_merge.sql')

#: SQL 末尾的 limit N（分页时要把护栏让位给外层窗口）。
_TRAILING_LIMIT = re.compile(r"\s+limit\s+\d+\s*$", re.IGNORECASE)

#: 归档产物的注释头（**与生成器逐字符一致**，否则无法与留档做逐字符比对）。
#: 第一行是**归档产物的出处声明**（该文件确由生成器产出），不是运行时依赖 ——
#: 生产包不 import ``tools/``。
_HEADER_CREDIT = '-- 由 tools/build_sql_merge.py 从 data/merged_sql/merged_sql.json 生成'
_HEADER_SIZE_LIMIT = (
    '-- 每个内层子查询都带 limit/offset：平台 Agent 对输出有 102400 B 硬上限，'
    '超了整条任务失败。'
)
_HEADER_NO_MANUAL_EDIT = '-- 不要手改本文件；改视图定义后重跑生成脚本。'


@dataclass(frozen=True)
class PagingRule:
    """单个视图的分页策略（来自 ``paging.views``）。"""

    paged: bool
    strip_inner_limit: bool


@dataclass(frozen=True)
class MergedView:
    """合并 SQL 的一个视图定义。"""

    sql_key: str
    #: 压成单行的 SQL 文本（与生成器一致：空白折叠为一个空格）。
    sql: str
    #: 刷新档位（秒）。
    refresh_seconds: int
    paging: PagingRule


@dataclass(frozen=True)
class MergedSqlSpec:
    """``merged_sql.json`` 的运行时视图。"""

    views: Tuple[MergedView, ...]
    default_page_size: int


def load_spec(path: Optional[str] = None) -> MergedSqlSpec:
    """读合并 SQL 声明。

    :param path: 声明文件路径；``None`` 时用包内副本。
    :return: 不可变的规格对象。
    :raises MappingError: 文件缺失、不是合法 JSON、结构不符。
    """
    target = path or PACKAGE_MERGED_SQL_PATH
    try:
        with open(target, encoding='utf-8') as handle:
            payload = json.load(handle)
    except OSError as exc:
        raise MappingError(f'读取合并 SQL 声明失败：{target}（{exc}）')
    except ValueError as exc:
        raise MappingError(f'合并 SQL 声明不是合法 JSON：{target}（{exc}）')
    return _build_spec(payload, target)


def _build_spec(payload: Mapping[str, Any], source: str) -> MergedSqlSpec:
    """把 JSON 载荷转成不可变的规格对象。"""
    batches = payload.get('batches')
    if not isinstance(batches, list) or not batches:
        raise MappingError(f'合并 SQL 声明缺 batches 段：{source}')
    paging = payload.get('paging') or {}
    rules = paging.get('views') or {}
    views = [_build_view(batch, rules, source) for batch in batches]
    default_page_size = paging.get('default_page_size') or 100
    return MergedSqlSpec(
        views=tuple(views),
        default_page_size=int(default_page_size),
    )


def _build_view(
    batch: Mapping[str, Any],
    rules: Mapping[str, Any],
    source: str,
) -> MergedView:
    """把一个视图定义转成不可变的视图对象。"""
    sql_key = batch.get('sql_key')
    sql = batch.get('sql')
    if not sql_key or not sql:
        raise MappingError(f'合并 SQL 声明的视图缺 sql_key/sql：{source}')
    rule = rules.get(sql_key) or {}
    return MergedView(
        sql_key=sql_key,
        sql=' '.join(sql.split()),
        refresh_seconds=int(batch.get('refresh_seconds') or 0),
        paging=PagingRule(
            paged=bool(rule.get('paged')),
            strip_inner_limit=bool(rule.get('strip_inner_limit')),
        ),
    )


def select_views(
    spec: MergedSqlSpec,
    tier_seconds: Optional[int] = None,
) -> Tuple[MergedView, ...]:
    """按刷新档位挑视图。

    档位**从声明的 ``refresh_seconds`` 现算**（不硬编码视图清单），
    与生成器 ``--tier`` 的挑法等价。

    :param spec: 合并 SQL 规格。
    :param tier_seconds: 刷新档（秒）；``None`` 表示全量。
    :return: 选中的视图（保持声明顺序）。
    :raises MappingError: 档位不存在。
    """
    if tier_seconds is None:
        return spec.views
    picked = tuple(view for view in spec.views
                   if view.refresh_seconds == tier_seconds)
    if not picked:
        raise MappingError(f'合并 SQL 声明里没有 {tier_seconds} 秒档的视图')
    return picked


def render_sql(
    spec: MergedSqlSpec,
    page_size: Optional[int] = None,
    offset: int = 0,
    tier_seconds: Optional[int] = None,
) -> str:
    """渲染一条合并 SQL（每个内层子查询都带 limit/offset）。

    :param spec: 合并 SQL 规格。
    :param page_size: 每页行数上限；``None`` 时用声明的
        ``paging.default_page_size``。
    :param offset: 分页偏移（只作用于可增长视图）。
    :param tier_seconds: 刷新档（秒）；``None`` 表示全量。
    :return: SQL 文本（单行，无注释头）。
    :raises MappingError: 页容量或偏移非法、档位不存在。
    """
    limit = spec.default_page_size if page_size is None else page_size
    if limit < 1:
        raise MappingError(f'分页页容量必须 ≥ 1，收到 {limit}')
    if offset < 0:
        raise MappingError(f'分页偏移必须 ≥ 0，收到 {offset}')
    views = select_views(spec, tier_seconds=tier_seconds)
    parts = [_render_part(view, limit, offset) for view in views]
    return 'select ' + ', '.join(parts)


def _render_part(view: MergedView, page_size: int, offset: int) -> str:
    """渲染一个视图的 ``(select json_agg(t) from ( ... ) t) as <key>`` 片段。"""
    inner = view.sql
    if view.paging.paged:
        if view.paging.strip_inner_limit:
            inner = _TRAILING_LIMIT.sub('', inner)
        window = f' limit {page_size} offset {offset}'
    elif _TRAILING_LIMIT.search(inner):
        # 固定视图自带语义上限（如 activity_running 的 limit 20）—— 原样保留
        window = ''
    else:
        window = f' limit {page_size} offset 0'
    return f'(select json_agg(t) from ( {inner}{window} ) t) as {view.sql_key}'


def render_sql_with_config(
    spec: MergedSqlSpec,
    config: Config,
    offset: int = 0,
    tier_seconds: Optional[int] = None,
) -> str:
    """用配置里的页容量渲染（``CMD_PAGE_SIZE`` → ``config.cmd_page_size``）。

    :param spec: 合并 SQL 规格。
    :param config: 运行期配置。
    :param offset: 分页偏移（只作用于可增长视图）。
    :param tier_seconds: 刷新档（秒）；``None`` 表示全量。
    :return: SQL 文本。
    """
    return render_sql(spec, page_size=config.cmd_page_size, offset=offset,
                      tier_seconds=tier_seconds)


def render_sql_document(
    spec: MergedSqlSpec,
    page_size: Optional[int] = None,
    offset: int = 0,
    tier_seconds: Optional[int] = None,
) -> str:
    """复现归档产物：注释头 + SQL + 尾随换行。

    注释头**与生成器逐字符一致** —— 这样归档产物与运行时渲染可以直接做
    逐字符比对（``tests/unit/test_sqlgen.py``），视图定义一旦与留档分叉
    立刻暴露。

    :param spec: 合并 SQL 规格。
    :param page_size: 每页行数上限；``None`` 时用声明的默认值。
    :param offset: 分页偏移（只作用于可增长视图）。
    :param tier_seconds: 刷新档（秒）；``None`` 表示全量。
    :return: 完整文档文本（含尾随换行）。
    """
    limit = spec.default_page_size if page_size is None else page_size
    sql = render_sql(spec, page_size=limit, offset=offset,
                     tier_seconds=tier_seconds)
    views = select_views(spec, tier_seconds=tier_seconds)
    keys = '、'.join(view.sql_key for view in views)
    header = [
        _HEADER_CREDIT,
        f'-- 覆盖 {len(views)} 个视图：{keys}',
        f'-- 分页窗口：limit {limit} offset {offset}（offset 只作用于可增长视图）',
        _HEADER_SIZE_LIMIT,
        _HEADER_NO_MANUAL_EDIT,
    ]
    return '\n'.join(header) + '\n' + sql + '\n'
