#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""拉取统一命令中心的内置命令清单并持久化（response body 原样留档）。

平台内置命令 142 条（SQL / SHELL / PYTHON 三类），本项目只用其中一条
`general_single_sql_query`（通用 SQL 通道），但清单本身是**定位 cmd_id 与
校验该命令仍为 enabled** 的依据，开发方案 §命令目录 一节要求启动时做这件事。

接口（实测）：
    GET /rds/gaussdb/v3/command-center/customer-commands
        ?offset=0&limit=100&sort_key=cmd_name&sort_dir=asc
    → {total_count: 142, data: [ {...19 个字段...} ]}

坑（实测，见开发方案 §命令目录）：
    - `limit` 上限 100，必须分页遍历；
    - `status=disabled` 过滤无效（仍返回全部 142 条 enabled），传非法值才 400，
      所以**判断可用性要逐条比对 status 字段**，不能用过滤条件；
    - `cluster_mode` 必须是 JSON 数组，字符串形式直接 400 —— 本脚本不传该参数。

留档：
    data/command_center/builtin_commands.json          清单（分页原始 body + 合并后的 data）
    data/command_center/general_single_sql_query.json  该命令的详情（约束与参数定义）

用法：
    export TPOPS_USER=xxx TPOPS_PASSWORD=xxx
    python tools/fetch_builtin_commands.py
"""

from __future__ import print_function

import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))

from tpops_client import TpopsClient  # noqa: E402

LOG = logging.getLogger("fetch_builtin_commands")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT, "data", "command_center")
OUT_LIST = os.path.join(OUT_DIR, "builtin_commands.json")
OUT_DETAIL = os.path.join(OUT_DIR, "general_single_sql_query.json")

LIST_URI = "/rds/gaussdb/v3/command-center/customer-commands"
DETAIL_URI = "/rds/gaussdb/v3/command-center/commands/{cmd_id}"
PAGE_LIMIT = 100
MAX_PAGES = 50
CMD_SQL_ID = "73722acc-bff6-4d04-adc0-bc1e1fa1eb7d"   # general_single_sql_query


def fetch_page(client, offset, limit):
    """取一页命令清单，返回 (query_params, response_body)。"""
    params = {"offset": offset, "limit": limit,
              "sort_key": "cmd_name", "sort_dir": "asc"}
    resp = client.session.get(client.base_url + LIST_URI, params=params,
                              headers={"X-Mini-Token": client.token(),
                                       "X-Language": "zh-cn"},
                              timeout=client.timeout)
    client._raise_for_status(resp, "customer-commands list")
    return params, resp.json()


def fetch_all(client):
    """分页遍历直到取满 total_count，返回 (pages, merged_data, total_count)。"""
    pages = []
    merged = []
    total_count = None
    offset = 0
    for _ in range(MAX_PAGES):
        params, body = fetch_page(client, offset, PAGE_LIMIT)
        rows = body.get("data") or []
        if total_count is None:
            total_count = body.get("total_count")
        pages.append({"query_params": params, "response_body": body})
        merged.extend(rows)
        LOG.info("offset=%d 取回 %d 条，累计 %d 条", offset, len(rows), len(merged))
        if not rows:
            break
        offset += PAGE_LIMIT
        if total_count is not None and len(merged) >= total_count:
            break
        time.sleep(0.2)
    return pages, merged, total_count


def fetch_detail(client, cmd_id):
    """取单条命令详情（constraint_config / param_config）。"""
    uri = DETAIL_URI.format(cmd_id=cmd_id)
    resp = client.session.get(client.base_url + uri,
                              headers={"X-Mini-Token": client.token(),
                                       "X-Language": "zh-cn"},
                              timeout=client.timeout)
    client._raise_for_status(resp, "command detail")
    return {"api": "GET " + uri, "response_body": resp.json()}


def summarize(data):
    """按 cmd_type / call_type / source / status 统计，便于快速核对。"""
    out = {}
    for field in ("cmd_type", "call_type", "source", "status"):
        counter = {}
        for item in data:
            key = str(item.get(field))
            counter[key] = counter.get(key, 0) + 1
        out[field] = counter
    return out


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    client = TpopsClient()

    pages, data, total_count = fetch_all(client)
    if not data:
        raise SystemExit("命令清单为空，接口可能改了路径")

    sql_cmd = None
    for item in data:
        if item.get("cmd_id") == CMD_SQL_ID:
            sql_cmd = item
            break
    if sql_cmd is None:
        LOG.warning("清单里没有 general_single_sql_query(%s)，请人工核对 cmd_id", CMD_SQL_ID)
    else:
        LOG.info("general_single_sql_query: status=%s cmd_type=%s",
                 sql_cmd.get("status"), sql_cmd.get("cmd_type"))

    if not os.path.isdir(OUT_DIR):
        os.makedirs(OUT_DIR)

    payload = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "api": "GET " + LIST_URI,
        "base_url": client.base_url,
        "note": ("分页遍历的原始 response body（pages）与合并后的 data（142 条）。"
                 "命令清单不含凭据。"),
        "total_count": total_count,
        "fetched": len(data),
        "summary": summarize(data),
        "pages": pages,
        "data": data,
    }
    with open(OUT_LIST, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    LOG.info("写入 %s（total_count=%s, 取回 %d 条）", OUT_LIST, total_count, len(data))

    detail = fetch_detail(client, CMD_SQL_ID)
    detail["generated_at"] = payload["generated_at"]
    with open(OUT_DETAIL, "w", encoding="utf-8") as handle:
        json.dump(detail, handle, ensure_ascii=False, indent=2)
    LOG.info("写入 %s", OUT_DETAIL)


if __name__ == "__main__":
    main()
