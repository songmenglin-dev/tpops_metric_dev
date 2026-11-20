"""平台响应的**协议级**校验（与具体接口无关的部分）。

``require_fields`` 校验响应体含有约定字段、缺则抛 :class:`ProtocolError`。
它不涉及连接、认证或重试 —— 那些在 ``client.py``；这里只管
「这个响应长得对不对」。
"""
from typing import Any, Mapping

from ..errors import ProtocolError

__all__ = ['require_fields']


def require_fields(
    payload: Mapping[str, Any], fields: Any, endpoint: str
) -> Mapping[str, Any]:
    """校验响应体含有约定字段，缺则抛 :class:`ProtocolError`。

    :param payload: 平台响应体（解析后的对象）。
    :param fields: 必须存在的字段名（可迭代）。
    :param endpoint: 出错的接口标识（写进异常消息，便于定位）。
    :raises ProtocolError: 缺字段 / 响应体不是对象。
    """
    if not isinstance(payload, Mapping):
        raise ProtocolError(
            f'TPOPS 响应结构异常（{endpoint}）：期望对象，实际 {type(payload).__name__}'
        )
    missing = [name for name in fields if name not in payload]
    if missing:
        raise ProtocolError(f'TPOPS 响应缺少字段（{endpoint}）：{"、".join(sorted(missing))}')
    return payload
