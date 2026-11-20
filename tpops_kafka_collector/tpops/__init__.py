"""TPOPS 平台客户端层（wave 1 只有 HTTP 客户端）。

采集端与 GaussDB 监控平台（TPOPS）对话的一层：认证、发请求、把平台错误
翻译成 ``tpops_kafka_collector.errors`` 里的四类异常。

**依赖方向**：``collectors → tpops → client``（本包不反向依赖任何上层）。
"""

from .client import (
    AUTH_PATH,
    GAUSSDB_V3,
    RDS_V3,
    RDS_V3_2,
    TOKEN_HEADER,
    TpopsClient,
    require_fields,
)

__all__ = [
    'AUTH_PATH',
    'GAUSSDB_V3',
    'RDS_V3',
    'RDS_V3_2',
    'TOKEN_HEADER',
    'TpopsClient',
    'require_fields',
]
