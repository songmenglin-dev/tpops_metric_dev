"""部署包的 wheel 处理：解析、架构无关性校验、版本锁。

Python 3.7 语法红线：不用海象 / 内建泛型注解 / dict 合并。
"""

import os
import subprocess

#: 目标机 Python 版本（解析 wheel 时按它挑）
TARGET_PY_VERSION = '3.7'

#: 引导用的 wheel —— 目标机建 venv 时若没有 pip，用它们离线补上。
#: 版本都是**最后支持 Python 3.7** 的那一版，且全是纯 Python wheel。
#:
#: 注意：**不用 get-pip.py** —— 它自己要联网去 PyPI 找 pip 包，
#: 加 ``--no-index`` 反而装不上。真正的离线路径是
#: 「把 pip wheel 直接解压进 site-packages」（见 deploy/install.sh）。
BOOTSTRAP_PACKAGES = (
    ('pip', '23.3.2'),
    ('setuptools', '67.8.0'),
    ('wheel', '0.42.0'),
)


class DeployBundleError(Exception):
    """部署包构建失败。"""


def resolve_wheels(dest_dir, requirements_path=None, packages=None,
                   python_executable=None, timeout=600):
    """离线解析 wheel 到 dest_dir。

    用 ``--platform any --python-version 3.7 --implementation py --abi none``
    强制 pip **只接受纯 Python wheel** —— 若某个依赖没有纯 Python 版本，
    这里会直接失败（而不是等到目标机上编译失败）。
    """
    python_executable = python_executable or os.sys.executable
    if not os.path.isdir(dest_dir):
        os.makedirs(dest_dir)

    command = [
        python_executable, '-m', 'pip', 'download',
        '--only-binary=:all:',
        '--platform', 'any',
        '--python-version', TARGET_PY_VERSION,
        '--implementation', 'py',
        '--abi', 'none',
        '--no-cache-dir',
        '--disable-pip-version-check',
    ]
    if requirements_path:
        command += ['-r', requirements_path]
    elif packages:
        command += list(packages)
    else:
        raise DeployBundleError('resolve_wheels 需要 requirements_path 或 packages')
    command += ['-d', dest_dir]

    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=timeout)
    except subprocess.TimeoutExpired:
        raise DeployBundleError(f'pip download 超时（{timeout} 秒）')
    except OSError as exc:
        raise DeployBundleError(f'无法执行 pip download：{exc}')

    if result.returncode != 0:
        output = result.stdout.decode('utf-8', 'replace') if result.stdout else ''
        raise DeployBundleError(
            f'pip download 失败（退出码 {result.returncode}）：\n{output}')

    return sorted(os.path.join(dest_dir, n) for n in os.listdir(dest_dir)
                  if n.endswith('.whl'))


def assert_arch_independent(wheels_dir):
    """断言所有 wheel 都是纯 Python（``*-none-any.whl``）。

    这是「一份包通吃 x86_64 / aarch64」的**硬保证** —— 出现平台相关 wheel
    时必须在这里失败，而不是发到目标机上才发现装不上。
    """
    wheels = sorted(n for n in os.listdir(wheels_dir) if n.endswith('.whl'))
    if not wheels:
        raise DeployBundleError('wheels/ 里没有 wheel —— 离线安装无从谈起')
    platform_specific = [w for w in wheels if not w.endswith('-none-any.whl')]
    if platform_specific:
        raise DeployBundleError(
            '出现了平台相关 wheel，x86_64 与 aarch64 就不能共用一份部署包了：\n  '
            + '\n  '.join(platform_specific))
    return wheels


def write_lock_file(wheels_dir):
    """把本次解析出的精确版本写成 requirements.lock.txt。"""
    lines = []
    for name in sorted(os.listdir(wheels_dir)):
        if not name.endswith('.whl'):
            continue
        parts = name[:-len('.whl')].split('-')
        if len(parts) < 3:
            continue
        # wheel 文件名用下划线（kafka_python），PyPI 规范名用连字符（kafka-python）
        lines.append('{}=={}'.format(parts[0].replace('_', '-'), parts[1]))
    path = os.path.join(wheels_dir, 'requirements.lock.txt')
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write('# 本次部署实际安装的精确版本（由 tools/build_deploy_bundle.py 生成）\n')
        handle.write('# 全部为纯 Python wheel，x86_64 / aarch64 通用\n')
        for line in lines:
            handle.write(line + '\n')
    return path
