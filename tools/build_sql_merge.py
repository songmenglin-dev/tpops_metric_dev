#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 merged_sql.json 里的 N 条合并 SQL 再合成 **1 条** SQL。

## 为什么要有这一步

平台下发的**每一条** SQL 都要独立走一遍「建任务 → 分发到节点 → 收集 → 置完成」，
这段**固定开销约 5 秒**（实测 `select 1 as x` 也要 5.06~5.61 秒，与 SQL 复杂度无关）。
所以 11 条串行 = 11 × 5 秒 ≈ 60 秒；而 11 个结果集塞进 1 条 SQL = 1 × 5 秒 + SQL 执行
= 实测 8.3 秒。

## 怎么合

每个视图的 SQL 各自包成一个**标量子查询**（把「多行多列的表」压成「一个值」），
再用逗号并排放进同一行：

    (select json_agg(t) from ( <原 SQL> ) t) as <sql_key>

`json_agg` 让每行的列名成为 JSON 对象的 key，所以客户端拆开后拿到的仍是原始列名；
各子查询自带 FROM/WHERE/GROUP BY/ORDER BY/LIMIT，彼此不干扰
（`activity_running` 与 `activity_top10` 的 ORDER BY 方向相反也能共存）。

## 用法

    python tools/build_sql_merge.py                 # 11 条 → 1 条（全量）
    python tools/build_sql_merge.py --tier 60       # 只合 60 秒档的 7 条
    python tools/build_sql_merge.py --out /tmp/x.sql

输出默认写到 `data/merged_sql/sql_merge.sql`。**SQL 由 merged_sql.json 机械生成，
不要手写** —— 改视图定义后重跑本脚本即可。
"""

from __future__ import print_function

import argparse
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "data", "merged_sql", "merged_sql.json")
DEFAULT_OUT = os.path.join(ROOT, "data", "merged_sql", "sql_merge.sql")

# 刷新档（秒）→ 该档必须包含的 sql_key（与 merged_sql.json 的 refresh_seconds 一致）
TIER_KEYS = {
    60: ["stat_database", "activity_agg", "activity_running", "locks",
         "settings", "prepared_xacts", "dbstat_sum"],
    120: ["stat_user_tables", "database"],
    300: ["activity_top10", "bgwriter"],
}

# SQL 末尾的 limit N（分页时要把护栏让位给外层窗口）
_TRAILING_LIMIT = re.compile(r"\s+limit\s+\d+\s*$", re.IGNORECASE)


def load_spec(path):
    """读 merged_sql.json。"""
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def load_batches(spec):
    """返回 [(sql_key, sql_text), ...]（sql 压成单行）。"""
    return [(b["sql_key"], " ".join(b["sql"].split())) for b in spec["batches"]]


def pick(batches, tier):
    """按刷新档挑 sql_key；tier=0 表示全量。"""
    if not tier:
        return list(batches)
    keys = list(TIER_KEYS.get(tier) or [])
    if not keys:
        raise SystemExit("未知档位 %s，可选：%s / all" % (tier, sorted(TIER_KEYS)))
    known = dict(batches)
    missing = [k for k in keys if k not in known]
    if missing:
        raise SystemExit("merged_sql.json 里没有这些 sql_key：%s" % "、".join(missing))
    return [(k, known[k]) for k in keys]


def paging_of(spec, sql_key):
    """取该视图的分页策略：{'paged': bool, 'strip_inner_limit': bool}。"""
    views = (spec.get("paging") or {}).get("views") or {}
    rule = views.get(sql_key) or {}
    return {"paged": bool(rule.get("paged")),
            "strip_inner_limit": bool(rule.get("strip_inner_limit"))}


def combine(batches, spec, page_size, offset):
    """生成合并 SQL：每个内层子查询都带 limit/offset。

    - 可增长视图（paged=true）：剥掉原 SQL 的 limit 护栏，外层套 `limit P offset O`；
    - 固定视图（paged=false）：原样保留（含语义上限），offset 恒为 0 —— 否则翻页时
      这些视图会返回空行，客户端会误判「没有数据」。
    """
    parts = []
    for key, sql in batches:
        rule = paging_of(spec, key)
        inner = sql
        if rule["paged"]:
            # 可增长视图：剥掉原 SQL 的 limit 护栏，外层套分页窗口
            if rule["strip_inner_limit"]:
                inner = _TRAILING_LIMIT.sub("", inner)
            window = " limit %d offset %d" % (page_size, offset)
        elif _TRAILING_LIMIT.search(inner):
            # 固定视图自带语义上限（如 activity_running 的 limit 20）—— 原样保留，
            # 再加一个 limit 会变成 "limit 20 limit 100" 的语法错误
            window = ""
        else:
            window = " limit %d offset 0" % page_size
        parts.append("(select json_agg(t) from ( %s%s ) t) as %s"
                     % (inner, window, key))
    return "select " + ", ".join(parts)


def main():
    parser = argparse.ArgumentParser(description="把多条合并 SQL 再合成 1 条")
    parser.add_argument("--tier", type=int, default=0,
                        help="刷新档（60/120/300），默认 0 = 全量")
    parser.add_argument("--limit", type=int, default=0,
                        help="每页行数上限，0 = 用 merged_sql.json 的 paging.default_page_size")
    parser.add_argument("--offset", type=int, default=0, help="分页偏移（只作用于可增长视图）")
    parser.add_argument("--out", default=DEFAULT_OUT, help="输出文件路径")
    args = parser.parse_args()

    spec = load_spec(SRC)
    page_size = args.limit or int((spec.get("paging") or {}).get("default_page_size") or 100)
    batches = pick(load_batches(spec), args.tier)
    sql = combine(batches, spec, page_size, args.offset)

    out_dir = os.path.dirname(os.path.abspath(args.out))
    if out_dir and not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    with open(args.out, "w", encoding="utf-8") as handle:
        handle.write("-- 由 tools/build_sql_merge.py 从 data/merged_sql/merged_sql.json 生成\n")
        handle.write("-- 覆盖 %d 个视图：%s\n" % (len(batches), "、".join(k for k, _ in batches)))
        handle.write("-- 分页窗口：limit %d offset %d（offset 只作用于可增长视图）\n"
                     % (page_size, args.offset))
        handle.write("-- 每个内层子查询都带 limit/offset：平台 Agent 对输出有 102400 B 硬上限，超了整条任务失败。\n")
        handle.write("-- 不要手改本文件；改视图定义后重跑生成脚本。\n")
        handle.write(sql)
        handle.write("\n")

    print("合并 %d 条 → %s（limit=%d offset=%d）" % (len(batches), args.out, page_size, args.offset))
    print("  SQL 长度：%d 字符（平台 sql_text 上限 10000）" % len(sql))
    for key, _ in batches:
        rule = paging_of(spec, key)
        print("  - %-20s %s" % (key, "分页" if rule["paged"] else "固定（每页全量）"))


if __name__ == "__main__":
    main()
