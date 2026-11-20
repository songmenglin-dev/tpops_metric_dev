"""慢 SQL 脱敏与截断。

**为什么默认脱敏**：慢 SQL 的 ``sql_text`` 是客户的**业务 SQL 文本**，
里面可能有库名、表名、列名、常量值、甚至注释里的人员信息。
客户是否允许外发**尚未书面确认**（``data/mapping/open_items.json``
的 ``slow-sql-masking`` 项）→ 客户确认之前一律走脱敏路径。

**默认口径**（与 ``open_items.json`` 的 ``default`` 一致）：

- 字面量（字符串、数值）替换为 ``?``；
- 截断到 ``sql_text_max_length``（默认 512）并**显式标记**。

**关键字不动**：`NULL` / `TRUE` / `FALSE` / 类型名是 SQL 语义的一部分，
替换成 ``?`` 会让客户侧看到的东西**不再是那条 SQL 的语义** —— 与脱敏
（隐藏数据）的目的相反。

**实现口径**：只用标准库 ``re``，不引入 ``sqlparse`` 之类的新依赖
（Python 3.7.9 上可选的 SQL 词法分析库很少，且都是新依赖）。
做法是**一条按优先级排序的 token 正则** + 一次 ``re.sub``：
注释 → 美元引用 → 字符串 → 数值。顺序即优先级，字符串 token 会把
内部的 ``--``、``/*`` 一起吃掉，所以**字符串里的注释符不会被误判成注释**。

边界处理清单：

===========================  ==========================================
情形                          处理
===========================  ==========================================
``'abc'``                    → ``?``（字符串字面量）
``'o''brien'``（双写转义）    → ``?``（整体一个 token）
``'a\'b'``（反斜杠转义）      → ``?``
``E'a\\nb'``（转义字符串前缀） → ``?``（前缀一并吃掉）
``$tag$ ... $tag$``          → ``?``（美元引用）
``123`` / ``1.5`` / ``2e3``   → ``?``（数值字面量）
``.25``                      → ``?``
``0x1F`` / ``0b101``          → ``?``（十六进制 / 二进制字面量）
``NULL`` / ``TRUE``           → **不替换**（关键字）
``col1`` / ``t2.a3``          → **不替换**（标识符里的数字）
``"my col"``                 → **不替换**（双引号是标识符 quoting）
``-- 注释``                  → ``-- ?``（注释体脱敏）
``/* 注释 */``               → ``/* ? */``（注释体脱敏）
===========================  ==========================================

**已知限制**（不在本轮解决，见任务 2.10 报告）：

- 不做真正的词法分析，**未闭合**的字符串（``'abc``）之后的内容会被当成
  普通文本处理 —— 平台返回的是完整 SQL，未闭合只在人工构造时出现。
- ``'...'::regclass`` 之类的类型转换后缀保留（``::regclass`` 不是字面量）。
- ``$1`` 这样的占位符（客户端参数）保持原样 —— ``1`` 前有 ``$``，
  数值正则的前视条件把它排除在外。
"""

import logging
import re
from typing import Any, Tuple

from ..errors import MappingError

LOGGER = logging.getLogger(__name__)

#: 慢 SQL 文本的默认上限（与 ``Config.sql_text_max_length`` 的默认值一致）
DEFAULT_MAX_LENGTH = 512

#: 截断标记。**长度算在上限内** —— 输出的总长不超过 max_length。
TRUNCATION_MARKER = '...[TRUNCATED]'


class SqlMaskingError(MappingError):
    """脱敏 / 截断的入参非法。

    继承 :class:`MappingError` → 不可重试（入参错了重试也没用）。
    """


#: token 优先级：注释 → 美元引用 → 字符串 → 数值。
#:
#: 字符串在注释之后 —— 但由于 ``re.sub`` 从左到右扫描，扫描到字符串的
#: 开引号时整个字符串（含内部的 ``--`` / ``/*``）会被一次性吃掉，
#: 所以注释分支永远看不到字符串内部的符号。
_TOKEN_RE = re.compile(
    r"""
      (?P<line_comment>--[^\n]*)
    | (?P<block_comment>/\*.*?\*/)
    | (?P<dollar_plain>\$\$.*?\$\$)
    | (?P<dollar_tag>\$(?P<dtag>[A-Za-z_][A-Za-z_0-9]*)\$.*?\$(?P=dtag)\$)
    | (?P<string>(?:[eE]|[uU]&|[bBxX])?'(?:''|\\.|[^'\\])*')
    | (?P<hex>0[xX][0-9a-fA-F]+)
    | (?P<binary>0[bB][01]+)
    | (?P<number>(?<![\w$])(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?(?![\w$]))
    """,
    re.VERBOSE | re.DOTALL,
)

_LINE_COMMENT_REPLACEMENT = '-- ?'
_BLOCK_COMMENT_REPLACEMENT = '/* ? */'
_LITERAL_REPLACEMENT = '?'


def _require_text(sql_text: Any, func_name: str) -> str:
    """校验入参是字符串 —— 传错类型要**显式报错**，不静默吞掉。"""
    if not isinstance(sql_text, str):
        raise SqlMaskingError(
            f'{func_name} 的 sql_text 必须是字符串，实际收到 {type(sql_text).__name__}'
        )
    return sql_text


def _replace_token(match: Any) -> str:
    """把一个 token 换成它的脱敏形态。"""
    if match.group('line_comment') is not None:
        return _LINE_COMMENT_REPLACEMENT
    if match.group('block_comment') is not None:
        return _BLOCK_COMMENT_REPLACEMENT
    return _LITERAL_REPLACEMENT


def mask_sql_literals(sql_text: str) -> str:
    """把字面量替换为 ``?``（关键字与标识符不动）。

    返回**新字符串**，不改传入的对象。
    """
    text = _require_text(sql_text, 'mask_sql_literals')
    masked, _ = mask_sql_literals_with_count(text)
    return masked


def mask_sql_literals_with_count(sql_text: str) -> Tuple[str, int]:
    """同 :func:`mask_sql_literals`，额外返回被替换的 token 数量。

    数量只用于日志与统计 —— **不外发**、也不入消息。
    """
    text = _require_text(sql_text, 'mask_sql_literals_with_count')
    counter = [0]

    def _counting_replace(match: Any) -> str:
        counter[0] += 1
        return _replace_token(match)

    masked = _TOKEN_RE.sub(_counting_replace, text)
    return masked, counter[0]


def truncate_sql(sql_text: str, max_length: int = DEFAULT_MAX_LENGTH) -> str:
    """截断到 ``max_length`` 字符，并**显式标记**。

    - 标记形式：``TRUNCATION_MARKER``（``...[TRUNCATED]``）；
    - **标记长度算在上限内** → 输出的总长 **≤** ``max_length``；
    - 未超长时原样返回（不追加标记）。

    为什么一定要标记：截断后的 SQL 看起来像一条**完整**的 SQL，
    客户侧无从判断它被截过 —— 那比截断本身更危险。
    """
    text = _require_text(sql_text, 'truncate_sql')
    if not isinstance(max_length, int) or isinstance(max_length, bool):
        raise SqlMaskingError(
            f'truncate_sql 的 max_length 必须是整数，实际收到 {type(max_length).__name__}'
        )
    if max_length <= 0:
        raise SqlMaskingError(
            f'truncate_sql 的 max_length 必须 ≥ 1，实际收到 {max_length}')
    if len(text) <= max_length:
        return text
    keep = max_length - len(TRUNCATION_MARKER)
    if keep <= 0:
        # 上限比标记还短 —— 只保留标记的前缀，保证总长不超过上限。
        return TRUNCATION_MARKER[:max_length]
    return text[:keep] + TRUNCATION_MARKER


def sanitize_sql(
    sql_text: str,
    masking_enabled: bool = True,
    max_length: int = DEFAULT_MAX_LENGTH,
) -> str:
    """慢 SQL 文本的对外出口：先脱敏，再截断。

    - ``masking_enabled=False`` → **不做字面量替换**（原样保留内容）；
      截断**仍然生效** —— 长度上限是消息字段的硬约束，与脱敏开关无关。
    - 顺序是「先脱敏后截断」：截断发生在脱敏之后，切口落在 ``?`` 上，
      不会因为切在字面量中间而漏出半个业务值。
    """
    text = _require_text(sql_text, 'sanitize_sql')
    masked = mask_sql_literals(text) if masking_enabled else text
    result = truncate_sql(masked, max_length)
    if len(result) < len(text):
        # 只记长度，不记内容 —— 日志里不能出现业务 SQL。
        LOGGER.warning(
            '慢 SQL 文本已截断：原长 %d 字符 → 保留 %d 字符（上限 %d，脱敏 %s）',
            len(text), len(result), max_length,
            '开' if masking_enabled else '关',
        )
    return result


def sanitize_sql_with_config(sql_text: str, config: Any) -> str:
    """按 :class:`tpops_kafka_collector.config.Config` 的脱敏配置处理。

    鸭子类型取属性（不 import ``config`` 模块）→ 避免映射层反向依赖配置层，
    测试里也能直接塞一个带同样属性的对象进来。
    """
    masking_enabled = bool(getattr(config, 'sql_masking_enabled', True))
    max_length = int(getattr(config, 'sql_text_max_length', DEFAULT_MAX_LENGTH))
    return sanitize_sql(sql_text, masking_enabled=masking_enabled, max_length=max_length)
