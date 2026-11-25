#!/usr/bin/env python3
"""把交付物按**版本号**归到 ``release/`` 目录。

产出结构::

    release/
    ├── README.md                     # 这个目录是什么、怎么重建
    └── <版本号>/
        ├── RELEASE-NOTES.md          # 本版本的清单与校验和
        ├── tpops-collector-<版本>.tar.gz        # 部署包：装到客户机器上跑
        ├── tpops-collector-<版本>.tar.gz.sha256
        ├── tpops-metric-dev-<版本>-source.tar.gz # 完整工程源码（留档/审阅）
        └── tpops-metric-dev-<版本>-source.tar.gz.sha256

两个包的区别：

* **部署包** —— 客户机上真正装的东西：最终部署文档说明 + 部署脚本
  （``check_env`` / ``install`` / ``verify_env`` / ``run``）+ 采集端生产包
  ``tpops_kafka_collector/``（含映射数据）。零联网安装，x86_64 与 aarch64 通用。
* **源码包** —— 整个工程（按 git 跟踪的文件打包），给客户留档或审阅代码用。

用法::

    python tools/build_release.py               # 构建并归档到 release/<版本>/
    python tools/build_release.py --no-source   # 只出部署包
    python tools/build_release.py --version 0.2.0

Python 3.7 语法红线：不用海象 / 内建泛型注解 / dict 合并。
"""

import argparse
import os
import shutil
import subprocess
import sys
import tarfile
from typing import List, Optional

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS_DIR = os.path.join(REPO_ROOT, 'tools')
if TOOLS_DIR not in sys.path:
    sys.path.insert(0, TOOLS_DIR)

from deploy_bundle import DeployBundleError, build_bundle, make_tarball, read_version  # noqa: E402
from deploy_bundle.bundle import SOFTWARE_NAME, SOURCE_NAME  # noqa: E402
from deploy_bundle.files import sha256  # noqa: E402

RELEASE_ROOT = os.path.join(REPO_ROOT, 'release')
DIST_ROOT = os.path.join(REPO_ROOT, 'dist')

#: 源码包里**排除**的路径片段（生成物 / 大文件 / 客户无关）
SOURCE_EXCLUDE = (
    'release/',
    'dist/',
    '.git/',
    '__pycache__/',
    '.pytest_cache/',
    '.ruff_cache/',
    '.idea/',
    '.claude/',
    'changes/',
)

#: 源码包里**额外排除**的可重建产物（CLAUDE.md 写明「不入库，缺失时由 tools/ 重建」）
SOURCE_EXCLUDE_GENERATED = (
    'data/customer_metric_query_plan.json',   # tools/build_metric_query_plan.py 重建
    'data/metric_value_rules.json',           # tools/build_metric_value_rules.py 重建
)


def _git_tracked_files(repo_root: str) -> Optional[List[str]]:
    """git 跟踪的文件列表；没有 git 或不在仓库里就返回 None。"""
    try:
        result = subprocess.run(
            ['git', '-C', repo_root, 'ls-files'],
            capture_output=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    files = result.stdout.decode('utf-8', 'replace').splitlines()
    return [f for f in files if f.strip()]


def _walk_source_files(repo_root: str) -> List[str]:
    """没有 git 时的兜底：遍历目录，按排除规则过滤。"""
    files = []
    for dirpath, dirnames, filenames in os.walk(repo_root):
        rel_dir = os.path.relpath(dirpath, repo_root)
        dirnames[:] = [d for d in dirnames
                       if not any(os.path.join(rel_dir, d).startswith(ex)
                                  for ex in SOURCE_EXCLUDE)]
        for name in filenames:
            rel = os.path.relpath(os.path.join(dirpath, name), repo_root)
            if any(rel.startswith(ex) for ex in SOURCE_EXCLUDE):
                continue
            if name.endswith(('.pyc', '.pyo')):
                continue
            files.append(rel)
    return files


def source_file_list(repo_root: str) -> List[str]:
    """源码包要收录的文件清单（相对路径）。

    优先用 git 跟踪的文件（= 工程快照）；不是 git 仓库就遍历目录。
    两种来源都会排掉：``SOURCE_EXCLUDE`` 里的路径、以及可重建的中间产物。
    """
    files = _git_tracked_files(repo_root)
    if files is None:
        files = _walk_source_files(repo_root)
        print('[warn] 不是 git 仓库（或 git 不可用），改为按目录遍历打包')
    else:
        files = [f for f in files if not any(f.startswith(ex) for ex in SOURCE_EXCLUDE)]

    skipped = [f for f in SOURCE_EXCLUDE_GENERATED if f in files]
    files = [f for f in files if f not in SOURCE_EXCLUDE_GENERATED]
    if skipped:
        print('[..] 源码包跳过可重建产物：{}'.format('、'.join(skipped)))
    return sorted(files)


def build_source_tarball(repo_root: str, dest_path: str, version: str) -> str:
    """把整个工程打成源码包（按 git 跟踪的文件；没有 git 就遍历目录）。"""
    files = source_file_list(repo_root)

    if os.path.exists(dest_path):
        os.remove(dest_path)
    arc_root = f'{SOURCE_NAME}_{version}'
    with tarfile.open(dest_path, 'w:gz') as tar:
        for rel in files:
            abs_path = os.path.join(repo_root, rel)
            if not os.path.isfile(abs_path):
                continue
            tar.add(abs_path, arcname=os.path.join(arc_root, rel))
    return dest_path


def _write_sha256(path: str) -> str:
    checksum_path = path + '.sha256'
    with open(checksum_path, 'w', encoding='utf-8') as handle:
        handle.write(f'{sha256(path)}  {os.path.basename(path)}\n')
    return checksum_path


def _write_release_notes(version: str, release_dir: str, artifacts: List[str]) -> str:
    """写本版本的 RELEASE-NOTES.md —— 交付物清单 + 校验和。"""
    path = os.path.join(release_dir, 'RELEASE-NOTES.md')
    lines = [
        f'# TPOPS 采集端 —— 发布说明（版本 {version}）',
        '',
        '生成时间：{}'.format(subprocess.check_output(
            ['date', '+%Y-%m-%d %H:%M:%S']).decode('utf-8').strip()),
        '',
        '## 交付物',
        '',
        '| 文件 | 是什么 | 校验 |',
        '|---|---|---|',
    ]
    lines += _artifact_rows(release_dir, artifacts)
    lines += _usage_section(version)
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write('\n'.join(lines) + '\n')
    return path


def _artifact_rows(release_dir: str, artifacts: List[str]) -> List[str]:
    """交付物表格的每一行（文件名 / 说明 / sha256）。"""
    descriptions = {
        'tpops_metric_collector_': '**软件发行包** —— 装到客户机器上跑：'
                           '**软件部署指南.pdf**（+ 同内容的 部署运行说明.md）+ '
                           '部署脚本 `collector.sh`（一个脚本，位置参数选功能：'
                           'check-env / check-deps / install / verify / run）+ '
                           '**离线依赖包 wheels/** + '
                           '采集端软件包 `tpops_kafka_collector/`（含映射数据）',
        'tpops_metric_dev_': '**工程源码包** —— 整个项目（按 git 跟踪的文件），'
                             '给客户留档或审阅代码（默认不产出）',
    }
    rows = []
    for name in artifacts:
        desc = '交付物'
        for prefix, text in descriptions.items():
            if name.startswith(prefix):
                desc = text
                break
        if name.endswith('.pdf'):
            desc = '**软件部署指南**（PDF）—— 同一份内容的 PDF 版'
        elif name.endswith('.md'):
            desc = '部署运行说明（Markdown 版）'
        elif name.endswith('.sh'):
            desc = '部署与运维脚本（一个脚本，位置参数选功能）'
        checksum = ''
        checksum_path = os.path.join(release_dir, name + '.sha256')
        if os.path.isfile(checksum_path):
            checksum = _read(checksum_path).split()[0]
        rows.append(f'| `{name}` | {desc} | `{checksum}` |')
    return rows


def _usage_section(version: str) -> List[str]:
    """客户侧上手步骤。"""
    return [
        '',
        '## 客户侧怎么用',
        '',
        '```bash',
        '# 1) 验校验和（.sha256 与压缩包分开传，防篡改）',
        f'sha256sum -c {SOFTWARE_NAME}_{version}.tar.gz.sha256',
        f'tar xzf {SOFTWARE_NAME}_{version}.tar.gz',
        f'cd {SOFTWARE_NAME}_{version}',
        '',
        '# 2) 检查环境（只读，不装任何东西）',
        'bash collector.sh check-env',
        '',
        '# 3) 离线依赖预检（这份依赖包在这台机器上装不装得上）',
        'bash collector.sh check-deps',
        '',
        '# 4) 安装环境需要的包 + 部署采集端包（离线）',
        'sudo bash collector.sh install',
        '',
        '# 5) 配环境变量 → 部署后自检 → 启动',
        'sudo vi /opt/tpops-collector/.env',
        'sudo bash /opt/tpops-collector/collector.sh verify',
        'bash /opt/tpops-collector/collector.sh run',
        '```',
        '',
        '完整说明见部署包内的《部署运行说明.md》。',
        '',
    ]


def _read(path: str) -> str:
    with open(path, encoding='utf-8') as handle:
        return handle.read()


def build_release(release_root: str = RELEASE_ROOT,
                  dist_root: str = DIST_ROOT,
                  version: Optional[str] = None,
                  with_source: bool = False,
                  source_repo_root: Optional[str] = None,
                  with_pdf: bool = True,
                  wheels_source: Optional[str] = None) -> str:
    """构建发布物并归档到 ``release/<版本>/``，返回该目录。

    ``release/<版本>/`` 里放的是**交付物**：

    * **软件发行包** ``tpops_metric_collector_<版本>.tar.gz``（+ .sha256）
    * **部署相关的所有东西**平铺一份：部署指南 PDF、部署说明 Markdown、部署脚本

    工程源码包**默认不产出**（``with_source=False``）—— 客户交付的是能跑的
    软件发行包，不是源码；要源码时显式传 ``with_source=True``。

    ``source_repo_root`` —— 源码包要打包的工程根目录（默认本仓库）；
    给测试用小目录，免得每次都打整个仓库。

    ``wheels_source`` —— 离线依赖包的来源目录；给了就不联网下载
    （测试用，避免依赖网络）。
    """
    version = version or read_version(REPO_ROOT)
    release_dir = os.path.join(release_root, version)
    if not os.path.isdir(release_dir):
        os.makedirs(release_dir)

    # 1) 部署包（文档 + 脚本 + 生产包）
    bundle_dir = build_bundle(repo_root=REPO_ROOT, dist_root=dist_root, version=version,
                              with_pdf=with_pdf, wheels_source=wheels_source)
    tar_path, _ = make_tarball(bundle_dir)
    deploy_name = os.path.basename(tar_path)
    shutil.copy2(tar_path, os.path.join(release_dir, deploy_name))
    _write_sha256(os.path.join(release_dir, deploy_name))
    print(f'[ok] 部署包：{deploy_name}')

    artifacts = [deploy_name]

    # 2) 部署相关的文档与脚本平铺一份 —— 现场不用解压就能看指南、跑预检
    for name in ('软件部署指南.pdf', '部署运行说明.md', 'collector.sh'):
        src = os.path.join(bundle_dir, name)
        if os.path.isfile(src):
            shutil.copy2(src, os.path.join(release_dir, name))
            print(f'[ok] 部署件：{name}')

    # 3) 工程源码包（默认不产出）
    if with_source:
        source_name = f'{SOURCE_NAME}_{version}-source.tar.gz'
        source_path = os.path.join(release_dir, source_name)
        build_source_tarball(source_repo_root or REPO_ROOT, source_path, version)
        _write_sha256(source_path)
        artifacts.append(source_name)
        print(f'[ok] 源码包：{source_name}')

    # 4) 发布说明
    _write_release_notes(version, release_dir, artifacts)
    print('[ok] RELEASE-NOTES.md')

    print()
    print(f'发布目录：{release_dir}')
    for name in sorted(os.listdir(release_dir)):
        size = os.path.getsize(os.path.join(release_dir, name))
        print(f'  {name:<48} {size / 1024.0:>8.1f} KB')
    return release_dir


def main(argv=None):
    parser = argparse.ArgumentParser(description='把交付物按版本号归档到 release/')
    parser.add_argument('--release-dir', default=RELEASE_ROOT, help='发布根目录（默认 release/）')
    parser.add_argument('--dist', default=DIST_ROOT, help='构建工作目录（默认 dist/）')
    parser.add_argument('--version', default=None, help='覆盖版本号')
    parser.add_argument('--with-source', action='store_true',
                        help='额外打包工程源码（默认不产出 —— 交付的是软件发行包）')
    parser.add_argument('--no-pdf', action='store_true', help='不渲染 PDF 部署指南')
    args = parser.parse_args(argv)

    try:
        build_release(release_root=args.release_dir, dist_root=args.dist,
                      version=args.version, with_source=args.with_source,
                      with_pdf=not args.no_pdf)
    except DeployBundleError as exc:
        print(f'[ERROR] {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
