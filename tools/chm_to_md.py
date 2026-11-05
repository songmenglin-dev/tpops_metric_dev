#!/usr/bin/env python3
"""Convert the GaussDB CHM product guide into Markdown.

The CHM is a Windows HTMLHelp file; its pages are GB2312-encoded. This extracts
the API reference pages and writes them out as Markdown, preserving the original
content (no additions).

Usage:
    7z x "docs/06_GaussDB V2.0-26.861.1 产品文档(for 轻量化部署形态) 01.chm" -o/tmp/chm
    python tools/chm_to_md.py /tmp/chm docs/api
"""

import html
import os
import re
import sys

TOC_STRUCTURE = [
    # (output file, section title, [(topic title, source html), ...])
    ("API参考-调用说明.md", "调用说明", [
        ("使用前必读", "zh-cn_topic_0000002694702096.html"),
        ("API接口概览", "zh-cn_topic_0000002724181935.html"),
        ("构造请求", "zh-cn_topic_0000002724181899.html"),
        ("认证鉴权", "zh-cn_topic_0000002694542432.html"),
        ("返回结果", "zh-cn_topic_0000002724181875.html"),
    ]),
    ("API参考-登录.md", "平台用户管理 › 登录", [
        ("生成Token", "zh-cn_topic_0000002694702160.html"),
    ]),
    ("API参考-实例管理.md", "实例管理", [
        ("查询数据库实例列表/查询实例详情（v3.2）", "zh-cn_topic_0000002694702356.html"),
    ]),
    ("API参考-监控大盘.md", "监控大盘", [
        ("查询监控指标", "zh-cn_topic_0000002724181647.html"),
        ("查询监控指标分组", "zh-cn_topic_0000002694542582.html"),
        ("健康检查", "zh-cn_topic_0000002724181995.html"),
    ]),
    ("API参考-告警管理.md", "告警管理", [
        ("告警对接测试", "zh-cn_topic_0000002724341633.html"),
        ("新增告警对接配置", "zh-cn_topic_0000002694542178.html"),
        ("修改告警对接配置", "zh-cn_topic_0000002724181549.html"),
        ("查看告警对接配置", "zh-cn_topic_0000002694542268.html"),
        ("告警结果列表", "zh-cn_topic_0000002724181907.html"),
        ("告警数量查询", "zh-cn_topic_0000002694542146.html"),
        ("实例告警批量配置", "zh-cn_topic_0000002724181567.html"),
    ]),
    ("API参考-权限说明.md", "权限说明", [
        ("权限说明", "zh-cn_topic_0000002694542348.html"),
    ]),
    ("API参考-附录.md", "附录", [
        ("异常请求结果", "zh-cn_topic_0000002724341565.html"),
        ("状态码", "zh-cn_topic_0000002694702030.html"),
        ("错误码", "zh-cn_topic_0000002724181857.html"),
        ("监控指标项", "zh-cn_topic_0000002694542276.html"),
        ("修改接口限流配置", "zh-cn_topic_0000002694542156.html"),
    ]),
]


def clean(text):
    text = re.sub(r"[\s\xa0]+", " ", text)
    return text.strip()


def inline(text):
    """Flatten a cell/paragraph fragment to inline Markdown."""
    text = re.sub(r"(?is)<(script|style).*?</\1>", "", text)
    text = re.sub(r"(?i)<br\s*/?>", " ", text)
    text = re.sub(r"(?i)</p>", " ", text)
    text = re.sub(r"<[^>]+>", "", text)
    return clean(html.unescape(text))


def cell_text(td):
    """Cell text: paragraphs joined, list items kept as separate clauses."""
    t = td
    t = re.sub(r"(?i)</p>", " ", t)
    t = re.sub(r"(?i)<br\s*/?>", " ", t)
    t = re.sub(r"(?i)<li[^>]*>", "；", t)
    t = re.sub(r"(?i)</li>", " ", t)
    t = re.sub(r"<[^>]+>", "", t)
    t = clean(html.unescape(t))
    t = re.sub(r"；\s*；+", "；", t)
    return t.strip("；").strip()


def render_table(tb):
    lines = []
    caption = ""
    cap = re.search(r"(?is)<caption[^>]*>(.*?)</caption>", tb)
    if cap:
        caption = inline(cap.group(1))
    rows = []
    for tr in re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", tb):
        cells = re.findall(r"(?is)<t[dh][^>]*>(.*?)</t[dh]>", tr)
        if cells:
            rows.append([cell_text(c) for c in cells])
    if not rows:
        return []
    ncol = max(len(r) for r in rows)
    rows = [r + [""] * (ncol - len(r)) for r in rows]
    if caption:
        lines.append(f"**{caption}**")
        lines.append("")
    lines.append("| " + " | ".join(rows[0]) + " |")
    lines.append("| " + " | ".join(["---"] * ncol) + " |")
    for r in rows[1:]:
        lines.append("| " + " | ".join(c.replace("|", "\\|") for c in r) + " |")
    lines.append("")
    return lines


def render_page(path):
    raw = open(path, "rb").read().decode("gb18030", "replace")
    body = re.search(r"(?is)<body[^>]*>(.*?)</body>", raw)
    body = body.group(1) if body else raw
    # drop nav chrome
    body = re.sub(r"(?is)<a[^>]*name=[^>]*>\s*</a>", "", body)
    body = re.sub(r'''(?is)<div[^>]*id="[^"]*(?:footer|copyright|nav)[^"]*"[^>]*>.*?</div>''', "", body)

    out = []
    for m in re.finditer(
        r"(?is)<h1[^>]*>(.*?)</h1>"
        r"|<h[2-6][^>]*>(.*?)</h[2-6]>"
        r"|<table[^>]*>.*?</table>"
        r"|<pre[^>]*>(.*?)</pre>"
        r"|<p[^>]*>(.*?)</p>",
        body,
    ):
        seg = m.group(0)
        if seg.lower().startswith("<h1"):
            continue  # page title is emitted by the caller as a section heading
        elif seg.lower().startswith("<h"):
            lvl = min(6, int(re.match(r"(?i)<h(\d)", seg).group(1)))
            out.append("")
            out.append("#" * min(lvl, 3) + f" {inline(m.group(2))}")
            out.append("")
        elif seg.lower().startswith("<table"):
            out.extend(render_table(seg))
        elif seg.lower().startswith("<pre"):
            out.append("```")
            out.append(html.unescape(re.sub(r"<[^>]+>", "", m.group(3))).strip())
            out.append("```")
            out.append("")
        else:
            t = inline(m.group(4))
            if t:
                out.append(t)
                out.append("")

    # trim trailing blanks
    while out and not out[-1]:
        out.pop()
    return "\n".join(out)


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else "/tmp/chm"
    dst = sys.argv[2] if len(sys.argv) > 2 else "docs/api"
    os.makedirs(dst, exist_ok=True)
    for fname, section, pages in TOC_STRUCTURE:
        chunks = [f"# {section}\n"]
        for title, html_file in pages:
            path = os.path.join(src, html_file)
            if not os.path.exists(path):
                print(f"  !! missing {html_file}")
                continue
            md = render_page(path)
            chunks.append(f"\n---\n\n## {title}\n")
            chunks.append(md)
            chunks.append("")
        out = os.path.join(dst, fname)
        with open(out, "w", encoding="utf-8") as fh:
            fh.write("\n".join(chunks).rstrip() + "\n")
        print(f"  wrote {out} ({len(pages)} pages, {os.path.getsize(out)//1024} KB)")


if __name__ == "__main__":
    main()
