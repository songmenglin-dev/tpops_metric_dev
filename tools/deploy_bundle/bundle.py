"""装配整个离线部署包。

Python 3.7 语法红线：不用海象 / 内建泛型注解 / dict 合并。
"""

import os
import re
import shutil
import tarfile

from .files import copy_tree, copy_wheels, sha256, write_manifest
from .wheels import (BOOTSTRAP_PACKAGES, DeployBundleError, assert_arch_independent,
                     resolve_wheels, write_lock_file)

#: 部署包根目录里要带上的东西（install.sh / verify_env.sh / run.sh 单独拷）
PAYLOAD_ITEMS = (
    'tpops_kafka_collector',
    'wheels',
    'requirements-runtime.txt',
    'run.sh',
    'verify_env.sh',
    'install.sh',
    '.env.example',
    'MANIFEST.txt',
)

#: 仓库里要带进部署包的脚本
DEPLOY_SCRIPTS = ('install.sh', 'check_env.sh', 'verify_env.sh', 'run.sh')

#: .env.example 里要去掉开发环境痕迹的键 —— 交付给客户时不能带开发账号
SANITIZED_ENV_KEYS = ('TPOPS_USER', 'TPOPS_BASE_URL')
SANITIZED_PLACEHOLDER = 'CHANGE_ME'


def read_version(repo_root):
    """从生产包的 __init__.py 里读 __version__（不 import，避免依赖问题）。"""
    init_path = os.path.join(repo_root, 'tpops_kafka_collector', '__init__.py')
    with open(init_path, encoding='utf-8') as handle:
        content = handle.read()
    match = re.search(r"^__version__\s*=\s*['\"]([^'\"]+)['\"]", content, re.M)
    if not match:
        raise DeployBundleError('读不到 tpops_kafka_collector 的 __version__')
    return match.group(1)


def sanitize_env_example(src_path, dest_path):
    """把开发环境的 .env.example 洗成客户可用的版本。

    开发环境里有真实的平台地址与账号（``TPOPS_USER=sml``）——
    交付给客户时应当是占位符，由客户填自己的。
    """
    with open(src_path, encoding='utf-8') as handle:
        lines = handle.read().splitlines()

    out = []
    for line in lines:
        stripped = line.strip()
        replaced = False
        for key in SANITIZED_ENV_KEYS:
            if stripped.startswith(key + '='):
                out.append(f'{key}={SANITIZED_PLACEHOLDER}')
                replaced = True
                break
        if not replaced:
            out.append(line)

    with open(dest_path, 'w', encoding='utf-8') as handle:
        handle.write('\n'.join(out) + '\n')
    return dest_path


def _prepare_payload(repo_root, bundle_dir):
    """把「非 wheel」的部分放进部署包：生产包、依赖清单、脚本、样例、文档。"""
    # 1) 生产包
    src_package = os.path.join(repo_root, 'tpops_kafka_collector')
    if not os.path.isdir(src_package):
        raise DeployBundleError('找不到生产包 tpops_kafka_collector')
    file_count = copy_tree(src_package, os.path.join(bundle_dir, 'tpops_kafka_collector'))
    print(f'[ok] 生产包：{file_count} 个文件')

    # 2) 运行时依赖
    req_src = os.path.join(repo_root, 'requirements-runtime.txt')
    if not os.path.isfile(req_src):
        raise DeployBundleError('找不到 requirements-runtime.txt')
    shutil.copy2(req_src, os.path.join(bundle_dir, 'requirements-runtime.txt'))
    print('[ok] 运行时依赖清单')

    # 3) 部署脚本
    for name in DEPLOY_SCRIPTS:
        src = os.path.join(repo_root, 'deploy', name)
        if not os.path.isfile(src):
            raise DeployBundleError(f'找不到 {src}')
        shutil.copy2(src, os.path.join(bundle_dir, name))
        os.chmod(os.path.join(bundle_dir, name), 0o755)
    print('[ok] 部署脚本（check / install / verify / run）')

    # 4) 环境变量样例 —— 注意它在仓库根，不在生产包内，必须显式带；
    #    且要去掉开发环境的账号与平台地址
    env_src = os.path.join(repo_root, '.env.example')
    if os.path.isfile(env_src):
        sanitize_env_example(env_src, os.path.join(bundle_dir, '.env.example'))
        print('[ok] .env.example（已去开发环境痕迹）')

    # 5) 部署文档
    doc_src = os.path.join(repo_root, 'docs', '交付-部署运行说明.md')
    if os.path.isfile(doc_src):
        shutil.copy2(doc_src, os.path.join(bundle_dir, '部署运行说明.md'))
        print('[ok] 部署运行说明')

    return req_src


def _prepare_wheels(repo_root, bundle_dir, wheels_source, allow_download, req_src):
    """准备 wheel（运行时 + 引导），并强制校验「架构无关」。"""
    wheels_dir = os.path.join(bundle_dir, 'wheels')
    bootstrap_dir = os.path.join(wheels_dir, 'bootstrap')
    for directory in (wheels_dir, bootstrap_dir):
        if not os.path.isdir(directory):
            os.makedirs(directory)

    if wheels_source:
        if not os.path.isdir(wheels_source):
            raise DeployBundleError(f'wheels 源目录不存在：{wheels_source}')
        copy_wheels(wheels_source, wheels_dir)
        src_bootstrap = os.path.join(wheels_source, 'bootstrap')
        if os.path.isdir(src_bootstrap):
            copy_wheels(src_bootstrap, bootstrap_dir)
        print(f'[ok] 复用 wheel：{wheels_source}')
    else:
        if not allow_download:
            raise DeployBundleError('既没有 --wheels-dir，也不允许下载 —— 无法准备 wheel')
        print('[..] 解析运行时 wheel（纯 Python，x86_64 / aarch64 通用）...')
        resolve_wheels(wheels_dir, requirements_path=req_src)
        print('[..] 解析引导 wheel（pip / setuptools / wheel）...')
        resolve_wheels(bootstrap_dir,
                       packages=[f'{name}=={ver}' for name, ver in BOOTSTRAP_PACKAGES])
        print('[ok] wheel 解析完成')

    wheels = assert_arch_independent(wheels_dir)
    write_lock_file(wheels_dir)
    print(f'[ok] 架构无关性校验通过：{len(wheels)} 个运行时 wheel 全为 *-none-any.whl')

    bootstrap_wheels = assert_arch_independent(bootstrap_dir)
    write_lock_file(bootstrap_dir)
    print(f'[ok] 引导 wheel：{len(bootstrap_wheels)} 个（目标机无 pip 时用它离线补 pip）')


def build_bundle(repo_root, dist_root, wheels_source=None, allow_download=True,
                 version=None):
    """构建部署包，返回部署包目录路径。"""
    version = version or read_version(repo_root)
    bundle_name = f'tpops-collector-{version}'
    bundle_dir = os.path.join(dist_root, bundle_name)

    if os.path.isdir(bundle_dir):
        shutil.rmtree(bundle_dir)
    os.makedirs(bundle_dir)

    req_src = _prepare_payload(repo_root, bundle_dir)
    _prepare_wheels(repo_root, bundle_dir, wheels_source, allow_download, req_src)

    write_manifest(bundle_dir, version)
    print('[ok] MANIFEST.txt')

    return bundle_dir


def make_tarball(bundle_dir):
    """把部署包打成 .tar.gz，并生成外置的 sha256 校验文件。

    外置校验文件是**防篡改**的那一半 —— MANIFEST.txt 在包内部，
    改包的人可以顺手把清单也改掉；校验文件要跟压缩包**分开**传。
    """
    tar_path = bundle_dir + '.tar.gz'
    if os.path.exists(tar_path):
        os.remove(tar_path)
    with tarfile.open(tar_path, 'w:gz') as tar:
        tar.add(bundle_dir, arcname=os.path.basename(bundle_dir))

    checksum_path = tar_path + '.sha256'
    with open(checksum_path, 'w', encoding='utf-8') as handle:
        handle.write(f'{sha256(tar_path)}  {os.path.basename(tar_path)}\n')
    return tar_path, checksum_path
