#!/usr/bin/env python3
"""从实测结果生成 API 速查表。

results/ 下按"每次调用一个文件"留档，共 197 个文件，便于逐条复核；
但查阅某个接口的入参与响应结构时不该翻 197 个文件。

本脚本从实测数据里提取每个接口的：
  - 请求方法与路径（含实际用过的查询参数）
  - 请求体示例
  - 响应状态码与响应体结构（字段名 + 类型 + 示例值）

输出 API速查.md —— 10 个接口一页看完。

用法：
    python data/command_center/build_api_quickref.py
"""

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
RESULTS = os.path.join(HERE, "results")
OUT = os.path.join(HERE, "API速查.md")

# 要收录的接口：(小节标题, 结果文件, 请求说明)
SECTIONS = [
    ("1. 认证 —— 生成 mini-token", "00_auth.json",
     "返回头里的 `X-Mini-Token` 就是后续所有接口的令牌；服务端 session_timeout 1440 分钟"),
    ("2. 实例列表", "01_instances.json",
     "`instances[].id` 用作后续接口的 `instance_id`"),
    ("3. 节点清单（执行命令的前置）", "02_nodes_83cd68f2e4cb40b4b7e69240479ffa26in14.json",
     "`nodes[].node_id` 用作下发命令的 `node_ids`；下发必须选 `role=master` 的节点"),
    ("4. 查询统一命令列表", "03_command_list_raw/page_000.json",
     "`limit` 上限 100 需分页；`cmd_type` / `status` / `source` 等为可选过滤条件"),
    ("5. 查询单条统一命令详情", "04_command_detail/73722acc-bff6-4d04-adc0-bc1e1fa1eb7d.json",
     "详情里的 `constraint_config`（执行约束）与 `param_config`（参数定义）决定能否下发"),
    ("6. 执行单条统一命令", "05_execute_sql_6374.json",
     "下发只返回 `job_id` 与节点任务 id，不含结果；结果要轮询 7、8 两个接口取"),
    ("7. 查询任务状态及节点任务记录", "06_task_records_sql_6374.json",
     "判断任务是否结束用 `statistics.running == 0`，不要看 `job_status`（它常停在 running）"),
    ("8. 查询节点任务结果详情", "07_task_result_sql_6374.json",
     "`result` 字段是 JSON 字符串，需二次 `json.loads` 解析"),
]


def load(rel_path):
    path = os.path.join(RESULTS, rel_path)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def describe(value, indent=0, max_depth=2):
    """描述一个值的结构：类型、字段名、示例值。嵌套展开到 max_depth 层。"""
    pad = "  " * indent
    if isinstance(value, dict):
        if not value:
            return pad + "{}  (空对象)"
        lines = []
        for key, item in list(value.items())[:12]:
            if isinstance(item, (dict, list)) and max_depth > 0:
                lines.append("%s%s:" % (pad, key))
                lines.append(describe(item, indent + 1, max_depth - 1))
            elif isinstance(item, (dict, list)):
                lines.append("%s%s: %s" % (pad, key, type(item).__name__))
            else:
                text = str(item)
                if len(text) > 60:
                    text = text[:60] + "…"
                lines.append("%s%s: %s  // %s" % (pad, key, type(item).__name__, text))
        if len(value) > 12:
            lines.append("%s…共 %d 个字段" % (pad, len(value)))
        return "\n".join(lines)
    if isinstance(value, list):
        if not value:
            return pad + "[]  (空数组)"
        head = describe(value[0], indent + 1, max_depth - 1)
        return "%s[数组，共 %d 项]\n%s" % (pad, len(value), head)
    return "%s%s  // %s" % (pad, type(value).__name__, value)


def section_body(title, rel_path, note):
    data = load(rel_path)
    if data is None:
        return "## %s\n\n> 缺少结果文件 `%s`\n" % (title, rel_path)

    request = data.get("request", {})
    response = data.get("response", {})
    body = response.get("body")

    out = ["## %s" % title, ""]
    out.append("**请求** `%s %s`" % (request.get("method"), request.get("url")))
    out.append("")
    if note:
        out.append("> %s" % note)
        out.append("")

    # 查询参数
    url = request.get("url", "")
    if "?" in url:
        query = url.split("?", 1)[1]
        out.append("**查询参数**")
        out.append("")
        for pair in query.split("&"):
            if pair:
                key, _, val = pair.partition("=")
                out.append("- `%s` = `%s`" % (key, val))
        out.append("")

    # 请求体
    request_body = request.get("body")
    if request_body is not None:
        out.append("**请求体**")
        out.append("")
        out.append("```json")
        out.append(json.dumps(request_body, ensure_ascii=False, indent=2))
        out.append("```")
        out.append("")

    out.append("**请求头** `X-Mini-Token: <token>`、`X-Language: zh-cn`"
               "（`Content-Type: application/json`）")
    out.append("")

    out.append("**响应** HTTP %s" % response.get("status_code"))
    out.append("")
    out.append("```")
    out.append(describe(body))
    out.append("```")
    out.append("")
    return "\n".join(out)


def main():
    parts = ["# API 速查",
              "",
              "本文件由 `build_api_quickref.py` 从 `results/` 的实测响应自动生成，"
              "内容全部来自真实调用。",
              "",
              "完整的逐次留档见 `results/`（197 个文件），"
              "每个文件含请求方法/URL、响应状态码、响应头、响应体。",
              "",
              "---",
              ""]
    for title, rel_path, note in SECTIONS:
        parts.append(section_body(title, rel_path, note))
        parts.append("---")
        parts.append("")

    with open(OUT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(parts).rstrip() + "\n")
    print("写出 %s" % os.path.relpath(OUT, ROOT))


if __name__ == "__main__":
    main()
