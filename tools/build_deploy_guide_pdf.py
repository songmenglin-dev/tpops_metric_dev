#!/usr/bin/env python3
"""把《部署运行说明》渲染成 **PDF 版的软件部署指南**。

为什么要 PDF：交付给客户的部署指南要给**运维**看 —— 现场多半没有
Markdown 阅读器，也不好打印归档。PDF 是通用格式。

渲染实现见 :mod:`deploy_bundle.pdf`（本脚本只是命令行薄壳）。

用法::

    python tools/build_deploy_guide_pdf.py                       # 默认输出
    python tools/build_deploy_guide_pdf.py --src X.md --out X.pdf

Python 3.7 语法红线：不用海象 / 内建泛型注解 / dict 合并。
"""

import argparse
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS_DIR = os.path.join(REPO_ROOT, 'tools')
if TOOLS_DIR not in sys.path:
    sys.path.insert(0, TOOLS_DIR)

from deploy_bundle.pdf import build_pdf  # noqa: E402

DEFAULT_SRC = os.path.join(REPO_ROOT, 'docs', '交付-部署运行说明.md')
DEFAULT_OUT = os.path.join(REPO_ROOT, 'dist', '软件部署指南.pdf')


def main(argv=None):
    parser = argparse.ArgumentParser(description='渲染 PDF 版软件部署指南')
    parser.add_argument('--src', default=DEFAULT_SRC, help='源 Markdown')
    parser.add_argument('--out', default=DEFAULT_OUT, help='输出 PDF')
    args = parser.parse_args(argv)

    try:
        path = build_pdf(args.src, args.out)
    except (OSError, RuntimeError) as exc:
        print(f'[ERROR] {exc}', file=sys.stderr)
        return 1
    print(f'[ok] PDF 部署指南：{path}（{os.path.getsize(path) / 1024.0:.1f} KB）')
    return 0


if __name__ == '__main__':
    sys.exit(main())
