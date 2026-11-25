"""部署包的文件处理：拷贝、sha256、清单。

Python 3.7 语法红线：不用海象 / 内建泛型注解 / dict 合并。
"""

import hashlib
import os
import shutil

#: 打包时要跳过的目录（缓存、IDE 配置）
SKIP_DIRS = frozenset([
    '__pycache__', '.git', '.pytest_cache', '.ruff_cache', '.mypy_cache', '.idea',
])

#: 打包时要跳过的后缀（字节码）
SKIP_SUFFIXES = ('.pyc', '.pyo')


def iter_files(root):
    """列出 root 下所有要打包的文件（相对路径），已过滤缓存与字节码。"""
    collected = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
        for name in sorted(filenames):
            if name.endswith(SKIP_SUFFIXES):
                continue
            abs_path = os.path.join(dirpath, name)
            collected.append(os.path.relpath(abs_path, root))
    return collected


def copy_tree(src, dst):
    """复制目录（只拷文件，跳过缓存目录与字节码）。返回文件数。"""
    count = 0
    for rel in iter_files(src):
        target = os.path.join(dst, rel)
        parent = os.path.dirname(target)
        if parent and not os.path.isdir(parent):
            os.makedirs(parent)
        shutil.copy2(os.path.join(src, rel), target)
        count += 1
    return count


def copy_wheels(src_dir, dst_dir):
    """把 src_dir 下的 wheel 拷到 dst_dir。返回拷贝数量。"""
    count = 0
    for name in sorted(os.listdir(src_dir)):
        if not name.endswith('.whl'):
            continue
        shutil.copy2(os.path.join(src_dir, name), os.path.join(dst_dir, name))
        count += 1
    return count


def sha256(path):
    """文件的 sha256 十六进制串。"""
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def write_manifest(bundle_dir, version):
    """生成 MANIFEST.txt：每个文件的 sha256 + 字节数。

    清单在包**内部**，因此只能发现传输损坏，不能发现「包被人改过」——
    防篡改靠外置的 ``.tar.gz.sha256``（见 :func:`deploy_bundle.bundle.make_tarball`）。
    """
    manifest_path = os.path.join(bundle_dir, 'MANIFEST.txt')
    entries = []
    for rel in iter_files(bundle_dir):
        if rel == 'MANIFEST.txt':
            continue
        abs_path = os.path.join(bundle_dir, rel)
        entries.append((sha256(abs_path), os.path.getsize(abs_path), rel))

    header = [
        '# TPOPS 采集端 —— 离线部署包清单',
        '#',
        '# 格式：<sha256>  <字节数>  <相对路径>',
        '#',
        f'# 版本    : {version}',
        '# Python  : >=3.7,<3.12（麒麟 V10 SP2+ 自带 3.7.9）',
        '# 架构    : 通用 —— 全部依赖均为纯 Python wheel（*-none-any.whl），',
        '#           x86_64 与 aarch64 共用这一份包',
        '#',
    ]
    with open(manifest_path, 'w', encoding='utf-8') as handle:
        for line in header:
            handle.write(line + '\n')
        for digest, size, rel in entries:
            handle.write(f'{digest}  {size}  {rel}\n')
    return manifest_path
