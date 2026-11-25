"""部署指南的 PDF 渲染（Markdown → pandoc → xelatex → PDF）。"""

import os
import re
import subprocess
import tempfile

#: 最后一列的宽度倍数（长说明通常在最后一列）
LAST_COLUMN_BOOST = 3

#: 渲染参数（字体/纸张）—— DejaVu 覆盖 ``>=`` 这类符号，
#: 换成纯 Latin Modern 的话 ``>=`` 在 PDF 里会变成空白
LATEX_VARS = [
    '-V', 'papersize=a4',
    '-V', 'geometry:margin=1.8cm',
    '-V', 'fontsize=10pt',
    '-V', 'mainfont=DejaVu Serif',
    '-V', 'monofont=DejaVu Sans Mono',
    '-V', 'CJKmainfont=Noto Serif CJK SC',
    '-V', 'CJKmonofont=Noto Sans Mono CJK SC',
]

#: 表格分隔行：只由 | - : 组成
SEPARATOR_RE = re.compile(r'^\s*\|[\s:\-|]+\|\s*$')


def widen_last_column(text):
    """给每张表的**最后一列**加权 —— 只影响渲染，不改内容。

    pandoc 按 markdown 表格分隔行的短横线比例分配列宽，默认等宽，
    最后一列（通常是长说明）会被挤成一窄条。
    """
    out = []
    for line in text.splitlines():
        stripped = line.strip()
        if SEPARATOR_RE.match(stripped) and stripped.count('-') >= 3:
            parts = stripped.split('|')
            # 形如 ['', '---', '---', ..., ''] —— 倒数第二段是最后一列
            if len(parts) >= 4 and parts[-2]:
                parts[-2] = '-' * (len(parts[-2]) * LAST_COLUMN_BOOST)
                line = '|'.join(parts)
        out.append(line)
    return '\n'.join(out) + '\n'


def pandoc_available():
    """这台机器上有没有 pandoc。"""
    try:
        result = subprocess.run(['pandoc', '--version'], capture_output=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def build_pdf(src_path, out_path, timeout=600):
    """渲染 PDF，返回输出路径。"""
    if not os.path.isfile(src_path):
        raise OSError(f'找不到源文件：{src_path}')
    if not pandoc_available():
        raise RuntimeError(
            '找不到 pandoc —— 渲染 PDF 需要 pandoc + xelatex + CJK 字体（Noto Serif CJK SC）')

    parent = os.path.dirname(out_path)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent)

    with open(src_path, encoding='utf-8') as handle:
        raw = handle.read()

    tmp = tempfile.NamedTemporaryFile('w', suffix='.md', delete=False, encoding='utf-8')
    try:
        tmp.write(widen_last_column(raw))
        tmp.close()
        command = ['pandoc', tmp.name, '-o', out_path, '--pdf-engine=xelatex']
        command += LATEX_VARS
        result = subprocess.run(command, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=timeout)
    finally:
        os.unlink(tmp.name)

    if result.returncode != 0:
        output = result.stdout.decode('utf-8', 'replace') if result.stdout else ''
        raise RuntimeError(f'pandoc 渲染失败：\n{output[-2000:]}')
    if not os.path.isfile(out_path):
        raise RuntimeError(f'pandoc 没产出 PDF：{out_path}')
    return out_path
