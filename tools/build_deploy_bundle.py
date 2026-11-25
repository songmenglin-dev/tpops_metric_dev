#!/usr/bin/env python3
"""采集端**离线部署包**的构建入口（命令行）。

为什么有这个东西
----------------
采集端的运行时依赖只有 ``requests`` / ``kafka-python`` / ``urllib3`` 三个
**纯 Python** 包（无 C 扩展），因此：

* 同一份 wheel 可以同时跑在 **x86_64** 与 **aarch64** 上 ——
  麒麟两种架构**不需要分别打包**；
* 目标机只要有 Python 3.7 ~ 3.11（麒麟 V10 SP2+ 自带 3.7.9）即可，
  **不需要装 conda、不需要编译、不需要联网**。

本脚本在**有网机器**上跑一次，产出：

    dist/tpops-collector-<版本>/            目录
    dist/tpops-collector-<版本>.tar.gz      压缩包
    dist/tpops-collector-<版本>.tar.gz.sha256   外置校验（与压缩包分开传）

用法::

    python tools/build_deploy_bundle.py                  # 下载 wheel + 打包
    python tools/build_deploy_bundle.py --wheels-dir DIR # 复用已有 wheel（离线机器）
    python tools/build_deploy_bundle.py --no-tarball

Python 3.7 语法红线：不用海象 / 内建泛型注解 / dict 合并。
"""

import argparse
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS_DIR = os.path.join(REPO_ROOT, 'tools')
if TOOLS_DIR not in sys.path:
    sys.path.insert(0, TOOLS_DIR)

from deploy_bundle import DeployBundleError, build_bundle, make_tarball  # noqa: E402

DEFAULT_DIST = os.path.join(REPO_ROOT, 'dist')


def main(argv=None):
    parser = argparse.ArgumentParser(description='构建采集端离线部署包')
    parser.add_argument('--dist', default=DEFAULT_DIST, help='输出目录（默认 dist/）')
    parser.add_argument('--wheels-dir', default=None,
                        help='复用已有 wheel 目录（离线机器用；不再联网下载）')
    parser.add_argument('--no-download', action='store_true',
                        help='禁止下载 wheel（必须配合 --wheels-dir）')
    parser.add_argument('--no-tarball', action='store_true', help='不打包 tar.gz')
    parser.add_argument('--version', default=None, help='覆盖版本号')
    args = parser.parse_args(argv)

    try:
        bundle_dir = build_bundle(
            repo_root=REPO_ROOT,
            dist_root=args.dist,
            wheels_source=args.wheels_dir,
            allow_download=not args.no_download,
            version=args.version,
        )
    except DeployBundleError as exc:
        print(f'[ERROR] {exc}', file=sys.stderr)
        return 1

    print()
    print(f'部署包：{bundle_dir}')
    if not args.no_tarball:
        tar_path, checksum_path = make_tarball(bundle_dir)
        print(f'压缩包：{tar_path}')
        print(f'校验文件：{checksum_path}（与压缩包分开传，接收端用 sha256sum -c 验）')
    print()
    print('传到目标机后：')
    print('  sha256sum -c tpops-collector-*.tar.gz.sha256')
    print('  tar xzf tpops-collector-*.tar.gz')
    print('  cd tpops-collector-*')
    print('  sudo bash install.sh          # 离线安装')
    print('  sudo bash verify_env.sh       # 环境自检')
    return 0


if __name__ == '__main__':
    sys.exit(main())
