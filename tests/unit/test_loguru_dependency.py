"""loguru 依赖的落地守卫（wave-2 任务 2.1）。

守护三件事：

1. **运行时依赖清单明确钉住** ``loguru==0.8.0`` —— 部署包只带运行时那一份，
   目标机上不能出现「装到别的版本」的情况；
2. 装进环境的 loguru 是**纯 Python wheel**（``py3-none-any``）——
   x86_64 与 aarch64（麒麟两种架构）共用同一份部署包；
3. Linux + Python 3.7 上 loguru **零传递依赖**（``colorama`` 只在 Windows、
   ``aiocontextvars`` 只在 py<3.7）—— 目标机离线安装不会缺件。

另有一组「离线部署面」的守卫：部署包里带的是同一份运行时清单，
``collector.sh`` 的依赖自检也得认 loguru。
"""

import importlib.util
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RUNTIME_REQUIREMENTS = os.path.join(REPO_ROOT, 'requirements-runtime.txt')
COLLECTOR_SH = os.path.join(REPO_ROOT, 'deploy', 'collector.sh')

#: loguru 在 Linux + Python 3.7 上**不生效**的可选依赖
_RUNTIME_MARKERS = ("sys_platform=='win32'", "python_version<'3.7'")


def _read(path):
    with open(path, encoding='utf-8') as handle:
        return handle.read()


def _requirement_lines(path):
    lines = []
    for raw in _read(path).splitlines():
        line = raw.split('#')[0].strip()
        if line and not line.startswith('-'):
            lines.append(line)
    return lines


def _dist_info_dir():
    """返回已安装 loguru 的 ``*.dist-info`` 目录路径。"""
    spec = importlib.util.find_spec('loguru')
    assert spec is not None, 'loguru 没装进当前环境'
    site_packages = os.path.dirname(spec.submodule_search_locations[0])
    names = [n for n in os.listdir(site_packages)
             if n.startswith('loguru-') and n.endswith('.dist-info')]
    assert len(names) == 1, f'loguru 的 dist-info 目录不唯一：{names}'
    return os.path.join(site_packages, names[0])


def test_runtime_requirements_pin_loguru():
    """运行时依赖清单里必须钉住 loguru==0.8.0。"""
    pins = [line for line in _requirement_lines(RUNTIME_REQUIREMENTS)
            if line.lower().startswith('loguru')]
    assert pins == ['loguru==0.8.0'], f'loguru 的版本钉法不对：{pins}'


def test_loguru_is_importable():
    """当前开发环境能 import loguru（否则生产代码 import 就炸）。"""
    import loguru  # noqa: F401


def test_installed_loguru_is_pure_python_wheel():
    """装进来的 loguru 必须是纯 Python wheel（一份包通吃两种架构）。"""
    wheel_meta = _read(os.path.join(_dist_info_dir(), 'WHEEL'))
    assert 'Root-Is-Purelib: true' in wheel_meta, wheel_meta
    assert 'Tag: py3-none-any' in wheel_meta, wheel_meta


def test_loguru_has_no_unconditional_dependency():
    """METADATA 里不得有无条件的 Requires-Dist（那会在目标机上变成缺件）。"""
    metadata = _read(os.path.join(_dist_info_dir(), 'METADATA'))
    requires = [line.strip() for line in metadata.splitlines()
                if line.startswith('Requires-Dist:')]
    assert requires, '没读到 loguru 的 Requires-Dist，METADATA 解析可能失效'
    unconditional = [line for line in requires if ';' not in line]
    assert not unconditional, f'loguru 出现了无条件依赖：{unconditional}'


def test_loguru_runtime_dependencies_do_not_apply_to_linux_py37():
    """非 extra 的依赖在 Linux + Python 3.7 上全部不生效。"""
    metadata = _read(os.path.join(_dist_info_dir(), 'METADATA'))
    runtime_requires = [line.strip() for line in metadata.splitlines()
                        if line.startswith('Requires-Dist:') and 'extra ==' not in line]
    for line in runtime_requires:
        assert any(marker in line for marker in _RUNTIME_MARKERS), \
            f'loguru 的运行期依赖在 Linux+py3.7 上会生效：{line}'


def test_python_version_supports_py37():
    """loguru 声明的 requires_python 必须覆盖 3.7。"""
    metadata = _read(os.path.join(_dist_info_dir(), 'METADATA'))
    requires_python = [line.strip() for line in metadata.splitlines()
                       if line.startswith('Requires-Python:')]
    assert requires_python == ['Requires-Python: >=3.5'], requires_python
    assert sys.version_info[:2] >= (3, 5)


def test_deploy_bundle_ships_the_same_runtime_requirements():
    """部署包带的是同一份运行时清单（因此 loguru 一定在离线 wheel 里）。"""
    sys.path.insert(0, os.path.join(REPO_ROOT, 'tools'))
    from deploy_bundle import bundle as bundle_mod  # noqa: E402

    assert 'requirements-runtime.txt' in bundle_mod.PAYLOAD_ITEMS
    assert os.path.exists(RUNTIME_REQUIREMENTS)
    assert 'loguru==0.8.0' in _read(RUNTIME_REQUIREMENTS)


def test_collector_sh_dependency_probe_covers_loguru():
    """目标机自检脚本的依赖清单要认 loguru。"""
    script = _read(COLLECTOR_SH)
    assert 'for mod in requests kafka urllib3 loguru' in script, \
        'collector.sh 的运行时依赖自检没覆盖 loguru'
