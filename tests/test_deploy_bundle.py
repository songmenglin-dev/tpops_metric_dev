"""离线部署包的构建守卫。

守护三条不变量：

1. **运行时依赖与开发依赖分开** —— 部署包里只带运行时那一份；
2. **一份部署包通吃 x86_64 与 aarch64** —— 全部 wheel 必须是纯 Python；
3. **目标机安装零联网** —— 安装脚本只能用本地 wheel，且不得打印口令。

另有一组**行为性**守卫：真的把 install.sh 跑起来（用假 python3），
验证「位置参数真的生效」「安装模式标记真的写下来」——
纯静态断言抓不到这两类问题。
"""

import os
import subprocess
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, 'tools'))

import build_deploy_bundle as cli  # noqa: E402
import deploy_bundle as bdb  # noqa: E402
from deploy_bundle import wheels as wheels_mod  # noqa: E402
from deploy_bundle.files import copy_tree, sha256  # noqa: E402

RUNTIME_REQUIREMENTS = os.path.join(REPO_ROOT, 'requirements-runtime.txt')
DEV_REQUIREMENTS = os.path.join(REPO_ROOT, 'requirements-dev.txt')
DEPLOY_DIR = os.path.join(REPO_ROOT, 'deploy')


def _read(path):
    with open(path, encoding='utf-8') as handle:
        return handle.read()


def _requirement_names(path):
    """解析 requirements 文件里的包名（忽略注释与 -r 引用）。"""
    names = []
    for line in _read(path).splitlines():
        line = line.split('#')[0].strip()
        if not line or line.startswith('-'):
            continue
        for sep in ('>=', '<=', '==', '~=', '>', '<', '!='):
            if sep in line:
                line = line.split(sep)[0]
                break
        names.append(line.strip().lower())
    return names


def _fake_wheels(directory):
    """造几个纯 Python wheel（空文件即可，构建器只看名字与哈希）。"""
    os.makedirs(directory, exist_ok=True)
    for name in ('requests-2.31.0-py3-none-any.whl',
                 'urllib3-2.0.7-py3-none-any.whl',
                 'kafka_python-2.0.2-py2.py3-none-any.whl',
                 'certifi-2024.2.2-py3-none-any.whl'):
        with open(os.path.join(directory, name), 'wb') as handle:
            handle.write(b'')
    bootstrap = os.path.join(directory, 'bootstrap')
    os.makedirs(bootstrap, exist_ok=True)
    for name in ('pip-23.3.2-py3-none-any.whl',
                 'setuptools-67.8.0-py3-none-any.whl',
                 'wheel-0.42.0-py3-none-any.whl'):
        with open(os.path.join(bootstrap, name), 'wb') as handle:
            handle.write(b'')
    return directory


@pytest.fixture
def bundle(tmp_path):
    """构建一个离线部署包（用假 wheel，不联网）。"""
    wheels = _fake_wheels(str(tmp_path / 'src_wheels'))
    dist = str(tmp_path / 'dist')
    bundle_dir = bdb.build_bundle(repo_root=REPO_ROOT, dist_root=dist,
                                  wheels_source=wheels)
    return bundle_dir


# --------------------------------------------------------------------------
# 依赖拆分
# --------------------------------------------------------------------------
def test_runtime_requirements_declares_exactly_the_runtime_deps():
    """运行时依赖 = 生产代码真正 import 的三个包。"""
    assert _requirement_names(RUNTIME_REQUIREMENTS) == ['requests', 'kafka-python', 'urllib3']


def test_runtime_requirements_excludes_test_and_tool_only_packages():
    """测试 / 离线工具的依赖不能进部署包。"""
    names = _requirement_names(RUNTIME_REQUIREMENTS)
    for forbidden in ('pytest', 'pytest-cov', 'openpyxl'):
        assert forbidden not in names


def test_dev_requirements_include_runtime_requirements():
    """开发依赖必须包含运行时依赖（一条 -r 引用即可）。"""
    assert '-r requirements-runtime.txt' in _read(DEV_REQUIREMENTS)
    names = _requirement_names(DEV_REQUIREMENTS)
    for expected in ('pytest', 'pytest-cov', 'openpyxl'):
        assert expected in names


def test_runtime_requirements_keep_python37_compatible_pins():
    """版本红线：requests 2.32+ 与 kafka-python 2.1+ 都不支持 Python 3.7。"""
    content = _read(RUNTIME_REQUIREMENTS)
    assert 'requests>=2.25,<2.32' in content
    assert 'kafka-python==2.0.2' in content


# --------------------------------------------------------------------------
# 部署包结构
# --------------------------------------------------------------------------
def test_bundle_contains_runtime_payload(bundle):
    """部署包必须自带：生产包、依赖清单、wheel、三个脚本、环境变量样例。"""
    for name in ('tpops_kafka_collector', 'requirements-runtime.txt', 'wheels',
                 'install.sh', 'verify_env.sh', 'run.sh', '.env.example', 'MANIFEST.txt'):
        assert os.path.exists(os.path.join(bundle, name)), '缺少 ' + name


def test_bundle_carries_mapping_data(bundle):
    """映射数据必须随包走 —— 运行时不去读仓库根的 data/。"""
    data_dir = os.path.join(bundle, 'tpops_kafka_collector', 'mapping', 'data')
    assert os.path.isdir(data_dir)
    rules = os.path.join(data_dir, 'metric_rules.json')
    assert os.path.isfile(rules)
    assert os.path.getsize(rules) > 0


def test_bundle_env_example_has_no_dev_account(bundle):
    """交付给客户的 .env.example 不能带开发环境的账号与平台地址。"""
    content = _read(os.path.join(bundle, '.env.example'))
    assert 'TPOPS_USER=CHANGE_ME' in content
    assert 'TPOPS_USER=sml' not in content
    assert 'TPOPS_BASE_URL=CHANGE_ME' in content


def test_bundle_excludes_bytecode_and_caches(bundle):
    """部署包里不该有 __pycache__ / .pyc。"""
    offenders = []
    for dirpath, dirnames, filenames in os.walk(bundle):
        if '__pycache__' in dirnames:
            offenders.append(dirpath)
        offenders.extend(os.path.join(dirpath, f) for f in filenames if f.endswith('.pyc'))
    assert offenders == []


def test_manifest_lists_every_file_with_hash(bundle):
    """MANIFEST 要覆盖除自身外的每个文件，且哈希对得上。"""
    manifest = _read(os.path.join(bundle, 'MANIFEST.txt'))
    entries = {}
    for line in manifest.splitlines():
        if not line or line.startswith('#'):
            continue
        digest, size, rel = line.split('  ', 2)
        entries[rel] = (digest, int(size))
    assert entries

    on_disk = []
    for dirpath, dirnames, filenames in os.walk(bundle):
        dirnames[:] = [d for d in dirnames if d != '__pycache__']
        for name in filenames:
            rel = os.path.relpath(os.path.join(dirpath, name), bundle)
            if rel != 'MANIFEST.txt':
                on_disk.append(rel)
    assert set(entries) == set(on_disk)

    for rel, (digest, size) in entries.items():
        path = os.path.join(bundle, rel)
        assert os.path.getsize(path) == size, rel
        assert sha256(path) == digest, rel


def test_bundle_records_locked_wheel_versions(bundle):
    """wheels/ 里要有一份精确版本锁（便于事后追溯装了什么）。"""
    lock = _read(os.path.join(bundle, 'wheels', 'requirements.lock.txt'))
    assert 'requests==2.31.0' in lock
    assert 'kafka-python==2.0.2' in lock


def test_bundle_carries_bootstrap_wheels(bundle):
    """引导 wheel 必须随包 —— 目标机没有 pip 时要靠它离线补上。"""
    bootstrap = os.path.join(bundle, 'wheels', 'bootstrap')
    names = os.listdir(bootstrap)
    assert any(n.startswith('pip-') for n in names), '缺 pip wheel'
    assert any(n.startswith('setuptools-') for n in names), '缺 setuptools wheel'
    for name in names:
        if name.endswith('.whl'):
            assert name.endswith('-none-any.whl'), name


# --------------------------------------------------------------------------
# 架构无关性
# --------------------------------------------------------------------------
def test_build_bundle_rejects_platform_specific_wheel(tmp_path):
    """出现平台相关 wheel 必须**构建失败** —— 否则 ARM 机器上装不上。"""
    wheels = _fake_wheels(str(tmp_path / 'src_wheels'))
    with open(os.path.join(wheels, 'charset_normalizer-3.0.1-cp37-cp37m-manylinux1_x86_64.whl'),
              'wb') as handle:
        handle.write(b'')
    with pytest.raises(bdb.DeployBundleError) as excinfo:
        bdb.build_bundle(repo_root=REPO_ROOT, dist_root=str(tmp_path / 'dist'),
                         wheels_source=wheels)
    assert '平台相关' in str(excinfo.value)


def test_assert_arch_independent_accepts_pure_wheels(tmp_path):
    wheels = _fake_wheels(str(tmp_path / 'w'))
    assert len(bdb.assert_arch_independent(wheels)) == 4


# --------------------------------------------------------------------------
# 构建器的失败路径与 CLI
# --------------------------------------------------------------------------
def test_resolve_wheels_raises_on_pip_failure(tmp_path, monkeypatch):
    """pip download 失败必须抛 DeployBundleError —— 不能静默产出一个空包。"""
    class _Failed:
        returncode = 1
        stdout = b'ERROR: no matching distribution'

    monkeypatch.setattr(wheels_mod.subprocess, 'run', lambda *a, **k: _Failed())
    with pytest.raises(bdb.DeployBundleError) as excinfo:
        bdb.resolve_wheels(str(tmp_path / 'w'), requirements_path=RUNTIME_REQUIREMENTS)
    assert 'pip download 失败' in str(excinfo.value)


def test_resolve_wheels_requires_a_source(tmp_path):
    """既没有 requirements 也没有包清单 —— 明确报错，不猜。"""
    with pytest.raises(bdb.DeployBundleError):
        bdb.resolve_wheels(str(tmp_path / 'w'))


def test_make_tarball_contains_bundle_and_detached_checksum(tmp_path):
    """压缩包解开后就是部署包目录；校验文件在包**外**，防篡改靠它。"""
    import tarfile
    wheels = _fake_wheels(str(tmp_path / 'src_wheels'))
    bundle_dir = bdb.build_bundle(repo_root=REPO_ROOT, dist_root=str(tmp_path / 'dist'),
                                  wheels_source=wheels)
    tar_path, checksum_path = bdb.make_tarball(bundle_dir)
    assert os.path.isfile(tar_path)
    assert os.path.isfile(checksum_path)
    with tarfile.open(tar_path) as tar:
        names = tar.getnames()
    assert os.path.basename(bundle_dir) in names
    assert any(n.endswith('tpops_kafka_collector/main.py') for n in names)

    content = _read(checksum_path).split()
    assert content[0] == sha256(tar_path)
    assert content[1] == os.path.basename(tar_path)


def test_cli_builds_bundle_from_wheels_dir(tmp_path, capsys):
    """命令行入口：复用本地 wheel 构建，不联网。"""
    wheels = _fake_wheels(str(tmp_path / 'src_wheels'))
    code = cli.main([
        '--dist', str(tmp_path / 'dist'),
        '--wheels-dir', wheels,
        '--no-tarball',
    ])
    assert code == 0
    bundle_dir = os.path.join(str(tmp_path / 'dist'),
                              'tpops-collector-' + bdb.read_version(REPO_ROOT))
    assert os.path.isfile(os.path.join(bundle_dir, 'MANIFEST.txt'))
    assert '部署包' in capsys.readouterr().out


def test_cli_reports_failure_without_wheels(tmp_path):
    """既不给 wheel 目录又禁止下载 —— 退出码 1，不产出半成品。"""
    code = cli.main([
        '--dist', str(tmp_path / 'dist'),
        '--no-download',
        '--no-tarball',
    ])
    assert code == 1


def test_read_version_matches_package():
    """版本号取自生产包的 __version__，别处不要再硬写一份。"""
    init_path = os.path.join(REPO_ROOT, 'tpops_kafka_collector', '__init__.py')
    assert bdb.read_version(REPO_ROOT) in _read(init_path)


def test_copy_tree_skips_bytecode(tmp_path):
    """拷贝生产包时要跳过 __pycache__ 与 .pyc。"""
    src = tmp_path / 'pkg'
    (src / '__pycache__').mkdir(parents=True)
    (src / '__pycache__' / 'x.pyc').write_bytes(b'x')
    (src / 'mod.py').write_text('# code\n', encoding='utf-8')
    dst = tmp_path / 'out'
    count = copy_tree(str(src), str(dst))
    assert count == 1
    assert os.path.isfile(str(dst / 'mod.py'))
    assert not os.path.exists(str(dst / '__pycache__'))


# --------------------------------------------------------------------------
# 部署脚本 —— 静态断言
# --------------------------------------------------------------------------
def test_install_script_is_strict_and_offline():
    """安装脚本：set -euo pipefail + 只用本地 wheel + 不碰系统包管理器。"""
    content = _read(os.path.join(DEPLOY_DIR, 'install.sh'))
    assert 'set -euo pipefail' in content
    assert '--no-index' in content, '必须禁用 PyPI，否则离线机器会卡在联网超时'
    assert '--find-links' in content, '必须指向本地 wheel 目录'
    assert '--disable-pip-version-check' in content, 'pip 自检会发起联网请求'
    for forbidden in ('pip3 install', 'apt-get', 'yum ', 'dnf '):
        assert forbidden not in content, '安装脚本不应调用系统包管理器：' + forbidden


def test_install_script_creates_venv_and_does_not_touch_system_python():
    """安装必须落在安装目录内，不污染系统的 site-packages。"""
    content = _read(os.path.join(DEPLOY_DIR, 'install.sh'))
    assert '-m venv' in content
    for forbidden in ('/usr/lib/python', '/usr/lib64/python', '/usr/local/lib/python'):
        assert forbidden not in content, '安装脚本不应写入系统 Python 目录：' + forbidden


def test_install_script_bootstraps_pip_offline():
    """目标机没有 pip 时要能**离线**补上 —— 直接解压 wheel，不联网。"""
    content = _read(os.path.join(DEPLOY_DIR, 'install.sh'))
    assert 'bootstrap/pip-' in content, '应有从 bootstrap 目录取 pip wheel 的逻辑'
    assert 'extractall' in content, '应把 pip wheel 解压进 site-packages'


def test_install_script_does_not_execute_downloaded_get_pip():
    """get-pip.py 自己要联网找 pip 包 —— 不能作为离线安装路径，更不能 root 执行。"""
    content = _read(os.path.join(DEPLOY_DIR, 'install.sh'))
    code_lines = [ln for ln in content.splitlines()
                  if ln.strip() and not ln.strip().startswith('#')]
    for line in code_lines:
        assert 'get-pip.py' not in line, 'get-pip.py 不是离线路径，且会带来 root 执行风险：' + line


def test_verify_script_never_prints_secret_values():
    """自检脚本只看环境变量**是否存在**，绝不打印取值（口令不能进日志）。"""
    content = _read(os.path.join(DEPLOY_DIR, 'verify_env.sh'))
    assert '$TPOPS_PASSWORD' not in content
    assert '${TPOPS_PASSWORD' not in content
    assert '${!var:-}' in content


def test_verify_script_does_not_create_the_state_dir():
    """自检**不能创建**状态目录 —— 以 root 建出来的目录，服务用户写不进去，
    水位会静默丢失。"""
    content = _read(os.path.join(DEPLOY_DIR, 'verify_env.sh'))
    assert 'mkdir -p "$STATE_DIR"' not in content
    assert 'mkdir' not in content, '自检脚本不应创建任何东西'


def test_verify_script_reads_env_file_without_executing_it():
    """.env 要**解析**而不是 source —— source 等于让 root 执行客户可编辑的文件。"""
    content = _read(os.path.join(DEPLOY_DIR, 'verify_env.sh'))
    assert 'load_env_file' in content
    assert 'set -a' not in content, '不应 source .env'


def test_run_script_prefers_the_install_mode_marker():
    """run.sh 要以 .install-mode 为准 —— 装了一半的 venv 会盖掉装好的 vendor。"""
    content = _read(os.path.join(DEPLOY_DIR, 'run.sh'))
    assert '.install-mode' in content
    assert 'venv/bin/python' in content
    assert 'vendor' in content
    assert 'PYTHONPATH' in content


@pytest.mark.parametrize('name', ['install.sh', 'check_env.sh', 'verify_env.sh', 'run.sh'])
def test_shell_scripts_pass_syntax_check(name):
    """四个脚本都要通过 bash -n。"""
    path = os.path.join(DEPLOY_DIR, name)
    result = subprocess.run(['bash', '-n', path], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    assert result.returncode == 0, result.stdout.decode('utf-8', 'replace')


@pytest.mark.parametrize('name', ['install.sh', 'check_env.sh', 'verify_env.sh', 'run.sh'])
def test_shell_scripts_use_bash_and_are_executable(name):
    path = os.path.join(DEPLOY_DIR, name)
    content = _read(path)
    assert content.startswith('#!/usr/bin/env bash')
    assert os.access(path, os.X_OK), name + ' 需要可执行位'


# --------------------------------------------------------------------------
# 环境预检（check_env.sh）
# --------------------------------------------------------------------------
def test_check_script_is_read_only():
    """预检**只读** —— 不建目录、不装包、不改权限。

    以 root 跑完预检再让服务以普通用户跑，很容易留下
    「root 建的目录、服务用户写不进去」的坑（水位静默丢失）。

    判据看的是**命令位置**：提示语里出现「先 mkdir -p」不算，
    真去执行 mkdir 才算。
    """
    import re
    content = _read(os.path.join(DEPLOY_DIR, 'check_env.sh'))
    code = [ln for ln in content.splitlines()
            if ln.strip() and not ln.strip().startswith('#')]
    for line in code:
        assert not re.match(r'\s*(mkdir|cp|chmod|chown|rm|mv|tee)\b', line), \
            '预检脚本不应改动系统：' + line.strip()
        assert 'pip install' not in line, '预检脚本不应装包：' + line.strip()


def test_check_script_covers_the_key_questions():
    """预检要回答：架构、Python 版本区间、venv/ensurepip/pip 能力、目录、磁盘、端口。"""
    content = _read(os.path.join(DEPLOY_DIR, 'check_env.sh'))
    for probe in ('import venv', 'import ensurepip', '-m pip --version',
                  'uname -m', 'df -P', 'HEALTH_PORT', '3.12'):
        assert probe in content, '预检缺少检查项：' + probe


def test_bundle_contains_the_check_script(bundle):
    """部署包必须带上预检脚本 —— 它是部署流程的第一步。"""
    assert os.path.isfile(os.path.join(bundle, 'check_env.sh'))


def test_check_script_passes_on_this_machine(tmp_path):
    """在这台机器上真跑一遍预检：应当通过，且**不留下任何文件**。"""
    stub_dir = _stub_python(tmp_path)
    env = dict(os.environ)
    env['PATH'] = stub_dir + os.pathsep + env.get('PATH', '')
    prefix = tmp_path / 'opt'
    result = subprocess.run(
        ['bash', os.path.join(DEPLOY_DIR, 'check_env.sh'), str(prefix)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    output = result.stdout.decode('utf-8', 'replace')
    assert result.returncode == 0, output
    assert '预检通过' in output
    assert not prefix.exists(), '预检不该创建安装目录'


def test_check_script_fails_when_no_usable_python(tmp_path):
    """没有可用 Python 时预检必须**失败**（退出码非 0），别让人白装一遍。"""
    stub_dir = tmp_path / 'stub_bin'
    stub_dir.mkdir()
    # 假解释器报 3.13（太新）→ 预检应判 FAIL
    stub = stub_dir / 'python3'
    stub.write_text(
        '#!/bin/sh\n'
        'cmd=""; prev=""\n'
        'for a in "$@"; do\n'
        '  if [ "$prev" = "-c" ]; then cmd="$a"; fi\n'
        '  prev="$a"\n'
        'done\n'
        'case "$cmd" in\n'
        '  *SystemExit*) exit 1 ;;\n'
        '  *version_info*) echo "3.13.0"; exit 0 ;;\n'
        'esac\n'
        'exit 0\n', encoding='utf-8')
    stub.chmod(0o755)

    env = dict(os.environ)
    # 只保留假解释器 + 基本工具（真实解释器都会被判为「太新」而忽略）
    env['PATH'] = str(stub_dir) + os.pathsep + '/usr/bin:/bin'
    result = subprocess.run(
        ['bash', os.path.join(DEPLOY_DIR, 'check_env.sh'), str(tmp_path / 'opt')],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    output = result.stdout.decode('utf-8', 'replace')
    assert result.returncode == 1, output
    assert '预检未通过' in output


# --------------------------------------------------------------------------
# 部署脚本 —— 行为性冒烟（用假 python3 真跑一遍 install.sh）
# --------------------------------------------------------------------------

#: 假的 python3：只实现 install.sh 用到的调用。版本报 3.7.0 ——
#: 故意低于机器上任何真实解释器，保证它被选为第一个候选。
_STUB_PYTHON = r'''#!/bin/sh
cmd=""
prev=""
for a in "$@"; do
  if [ "$prev" = "-c" ]; then cmd="$a"; fi
  prev="$a"
done
case "$cmd" in
  *SystemExit*) exit 0 ;;
  *version_info*) echo "3.7.0"; exit 0 ;;
  *sysconfig*) echo "/tmp/fake-site-packages"; exit 0 ;;
esac
if [ "$1" = "-m" ] && [ "$2" = "venv" ]; then
  mkdir -p "$3/bin" && cp "$0" "$3/bin/python" && chmod +x "$3/bin/python"
  exit 0
fi
# -m pip ... 一律假装成功（pip 装没装成功不影响本测试要验的东西）
if [ "$1" = "-m" ] && [ "$2" = "pip" ]; then exit 0; fi
exit 0
'''


def _stub_python(tmp_path):
    """造一个假 python3 并返回它所在的目录（要放在 PATH 最前面）。"""
    stub_dir = tmp_path / 'stub_bin'
    stub_dir.mkdir()
    stub = stub_dir / 'python3'
    stub.write_text(_STUB_PYTHON, encoding='utf-8')
    stub.chmod(0o755)
    return str(stub_dir)


def _run_install(bundle, prefix, stub_dir, extra_args=None):
    env = dict(os.environ)
    env['PATH'] = stub_dir + os.pathsep + env.get('PATH', '')
    args = ['bash', os.path.join(bundle, 'install.sh')] + (extra_args or [])
    args.append(str(prefix))
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)


def test_install_script_honors_the_positional_prefix(tmp_path, bundle):
    """**位置参数必须真的生效** —— 曾被静默忽略，装到 /opt 而不是用户指定的地方。"""
    prefix = tmp_path / 'custom' / 'tpops'
    result = _run_install(bundle, prefix, _stub_python(tmp_path))
    assert result.returncode == 0, result.stdout.decode('utf-8', 'replace')
    assert (prefix / 'tpops_kafka_collector').is_dir(), '装错目录了'
    assert (prefix / 'wheels').is_dir()
    assert (prefix / 'requirements-runtime.txt').is_file()


def test_install_script_writes_the_mode_marker(tmp_path, bundle):
    """安装模式要显式落盘 —— run.sh 靠它选解释器，不靠目录猜。"""
    prefix = tmp_path / 'opt'
    result = _run_install(bundle, prefix, _stub_python(tmp_path))
    assert result.returncode == 0, result.stdout.decode('utf-8', 'replace')
    marker = prefix / '.install-mode'
    assert marker.is_file(), '缺 .install-mode'
    assert marker.read_text(encoding='utf-8').strip() in ('venv', 'vendor')
    assert (prefix / '.python-bin').is_file()


def test_install_script_is_idempotent(tmp_path, bundle):
    """重复安装不能报错（运维常常重跑一遍）。"""
    stub_dir = _stub_python(tmp_path)
    prefix = tmp_path / 'opt'
    first = _run_install(bundle, prefix, stub_dir)
    assert first.returncode == 0, first.stdout.decode('utf-8', 'replace')
    second = _run_install(bundle, prefix, stub_dir)
    assert second.returncode == 0, second.stdout.decode('utf-8', 'replace')


def test_install_script_accepts_prefix_equal_to_bundle_dir(tmp_path, bundle):
    """把部署包直接解压到安装目录再跑 install.sh —— 不能因为「cp 自己」而失败。"""
    stub_dir = _stub_python(tmp_path)
    result = _run_install(bundle, bundle, stub_dir)
    assert result.returncode == 0, result.stdout.decode('utf-8', 'replace')
    assert os.path.isfile(os.path.join(bundle, '.install-mode'))
