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
import shutil
import subprocess
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, 'tools'))

import build_deploy_bundle as cli  # noqa: E402
import build_release as release_cli  # noqa: E402
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
                 'collector.sh', '.env.example', 'MANIFEST.txt'):
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
    from deploy_bundle.bundle import SOFTWARE_NAME
    bundle_dir = os.path.join(
        str(tmp_path / 'dist'),
        f'{SOFTWARE_NAME}_{bdb.read_version(REPO_ROOT)}')
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
# --------------------------------------------------------------------------
# 部署脚本（collector.sh）—— 静态断言
# --------------------------------------------------------------------------
SCRIPT = os.path.join(DEPLOY_DIR, 'collector.sh')


def _script():
    return _read(SCRIPT)


def test_only_one_deploy_script_exists():
    """部署资产只有**一个**脚本 —— 功能用位置参数选，不是一堆散脚本。"""
    scripts = sorted(n for n in os.listdir(DEPLOY_DIR) if n.endswith('.sh'))
    assert scripts == ['collector.sh'], '部署脚本应当合并成一个：' + str(scripts)


def test_script_dispatches_by_positional_argument():
    """位置参数选功能：check-env / check-deps / install / verify / run。"""
    content = _script()
    for command in ('check-env', 'check-deps', 'install', 'verify', 'run'):
        assert f'{command}(' in content or f'{command})' in content \
            or f'{command} ' in content, '缺子命令：' + command
    assert 'main "$@"' in content


def test_script_is_strict_and_offline():
    """安装路径必须零联网：只用本地 wheel、禁用 pip 版本自检。"""
    content = _script()
    assert '--no-index' in content, '必须禁用 PyPI，否则离线机器会卡在联网超时'
    assert '--find-links' in content, '必须指向本地 wheel 目录'
    assert '--disable-pip-version-check' in content, 'pip 自检会发起联网请求'
    for forbidden in ('pip3 install', 'apt-get', 'yum ', 'dnf '):
        assert forbidden not in content, '不应调用系统包管理器：' + forbidden


def test_script_does_not_touch_system_python():
    """安装只落在安装目录内，不污染系统的 site-packages。"""
    content = _script()
    assert '-m venv' in content
    for forbidden in ('/usr/lib/python', '/usr/lib64/python', '/usr/local/lib/python'):
        assert forbidden not in content, '不应写入系统 Python 目录：' + forbidden


def test_script_bootstraps_pip_offline():
    """目标机没有 pip 时要能**离线**补上 —— 直接解压 wheel，不联网。"""
    content = _script()
    assert 'bootstrap/pip-' in content, '应有从 bootstrap 目录取 pip wheel 的逻辑'
    assert 'extractall' in content, '应把 pip wheel 解压进 site-packages'


def test_script_does_not_execute_downloaded_get_pip():
    """get-pip.py 自己要联网找 pip 包 —— 不能作为离线安装路径，更不能 root 执行。"""
    content = _script()
    code_lines = [ln for ln in content.splitlines()
                  if ln.strip() and not ln.strip().startswith('#')]
    for line in code_lines:
        assert 'get-pip.py' not in line, 'get-pip.py 不是离线路径，且会带来 root 执行风险：' + line


def test_script_never_prints_secret_values():
    """只看环境变量**是否存在**，绝不打印取值（口令不能进日志）。"""
    content = _script()
    assert '$TPOPS_PASSWORD' not in content
    assert '${TPOPS_PASSWORD' not in content
    assert '${!var:-}' in content


def test_script_reads_env_file_without_executing_it():
    """.env 要**解析**而不是 source —— source 等于让 root 执行客户可编辑的文件。"""
    content = _script()
    assert 'load_env_file' in content
    assert 'set -a' not in content, '不应 source .env'


def test_run_prefers_the_install_mode_marker():
    """run 要以 .install-mode 为准 —— 装了一半的 venv 会盖掉装好的 vendor。"""
    content = _script()
    assert '.install-mode' in content
    assert 'venv/bin/python' in content
    assert 'vendor' in content
    assert 'PYTHONPATH' in content


def test_run_loads_the_env_file(tmp_path):
    """`collector.sh run` 必须把 .env 读进进程环境。

    采集端只从进程环境读配置（config.py 不做 dotenv 解析）。若 run 不读 .env，
    就会出现「verify 说配置齐了、run 却报『缺少必填环境变量：TPOPS_USER』」
    这种自相矛盾 —— 现场就是这么踩的。
    """
    install = tmp_path / 'opt'
    (install / 'venv' / 'bin').mkdir(parents=True)
    # 假解释器：把 TPOPS_USER 打出来，验证它确实进了进程环境
    fake_python = install / 'venv' / 'bin' / 'python'
    fake_python.write_text(
        '#!/bin/sh\n'
        'echo "TPOPS_USER=$TPOPS_USER"\n'
        'echo "KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS"\n', encoding='utf-8')
    fake_python.chmod(0o755)
    (install / '.install-mode').write_text('venv\n', encoding='utf-8')
    (install / '.env').write_text(
        'TPOPS_USER=from-env-file\nKAFKA_BOOTSTRAP_SERVERS=broker:9092\n', encoding='utf-8')
    shutil.copy2(SCRIPT, str(install / 'collector.sh'))

    result = subprocess.run(['bash', str(install / 'collector.sh'), 'run'],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = result.stdout.decode('utf-8', 'replace')
    assert 'TPOPS_USER=from-env-file' in output, output
    assert 'KAFKA_BOOTSTRAP_SERVERS=broker:9092' in output, output


def test_process_env_wins_over_the_env_file(tmp_path):
    """进程环境里已有的值优先于 .env（与 systemd EnvironmentFile= 一致）。"""
    install = tmp_path / 'opt'
    (install / 'venv' / 'bin').mkdir(parents=True)
    fake_python = install / 'venv' / 'bin' / 'python'
    fake_python.write_text('#!/bin/sh\necho "TPOPS_USER=$TPOPS_USER"\n', encoding='utf-8')
    fake_python.chmod(0o755)
    (install / '.install-mode').write_text('venv\n', encoding='utf-8')
    (install / '.env').write_text('TPOPS_USER=from-env-file\n', encoding='utf-8')
    shutil.copy2(SCRIPT, str(install / 'collector.sh'))

    env = dict(os.environ)
    env['TPOPS_USER'] = 'from-process-env'
    result = subprocess.run(['bash', str(install / 'collector.sh'), 'run'],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    output = result.stdout.decode('utf-8', 'replace')
    assert 'TPOPS_USER=from-process-env' in output, output


def test_script_works_when_invoked_with_sh():
    """`sh collector.sh ...` 也能跑 —— 现场手误用 sh 调用是常事。

    sh 在麒麟 / RHEL 上通常是 dash，不支持进程替换，会报
    "syntax error near unexpected token `<'"。脚本顶部的 guard 会改用 bash 重跑。
    """
    result = subprocess.run(['sh', SCRIPT, 'help'], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    output = result.stdout.decode('utf-8', 'replace')
    assert result.returncode == 0, output
    assert 'check-env' in output


def test_script_passes_syntax_check():
    """脚本要通过 bash -n。"""
    result = subprocess.run(['bash', '-n', SCRIPT], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    assert result.returncode == 0, result.stdout.decode('utf-8', 'replace')


def test_script_uses_bash_and_is_executable():
    content = _script()
    assert content.startswith('#!/usr/bin/env bash')
    assert os.access(SCRIPT, os.X_OK), 'collector.sh 需要可执行位'


# --------------------------------------------------------------------------
# 环境预检（collector.sh check-env）—— 行为性
# --------------------------------------------------------------------------
def test_check_env_creates_nothing(tmp_path):
    """预检**只读** —— 以 root 跑完再让服务以普通用户跑，很容易留下
    「root 建的目录、服务用户写不进去」的坑（水位静默丢失）。"""
    stub_dir = _stub_python(tmp_path)
    env = dict(os.environ)
    env['PATH'] = stub_dir + os.pathsep + env.get('PATH', '')
    prefix = tmp_path / 'opt'
    result = subprocess.run(
        ['bash', SCRIPT, 'check-env', str(prefix)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    output = result.stdout.decode('utf-8', 'replace')
    assert result.returncode == 0, output
    assert '预检通过' in output
    assert not prefix.exists(), '预检不该创建安装目录'


def test_check_env_covers_the_key_questions():
    """预检要回答：架构、Python 版本区间、venv/ensurepip/pip 能力、目录、磁盘、端口。"""
    content = _script()
    for probe in ('import venv', 'import ensurepip', '-m pip --version',
                  'uname -m', 'df -P', 'HEALTH_PORT', '3.12'):
        assert probe in content, '预检缺少检查项：' + probe


def test_check_env_fails_when_no_usable_python(tmp_path):
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
        ['bash', SCRIPT, 'check-env', str(tmp_path / 'opt')],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    output = result.stdout.decode('utf-8', 'replace')
    assert result.returncode == 1, output
    assert '预检未通过' in output


# --------------------------------------------------------------------------
# 离线依赖预检（collector.sh check-deps）—— 行为性
# --------------------------------------------------------------------------
def test_check_deps_finds_wheel_with_underscore_name(tmp_path):
    """依赖名用连字符、wheel 文件名用下划线（kafka-python / kafka_python）——
    匹配时两种写法都要试，否则会误报「离线装不上」。"""
    content = _script()
    assert '${name//-/_}' in content
    assert '${name//_/-}' in content


def _make_real_wheel(directory, name='demo', version='1.0'):
    """造一个**合法**的纯 Python wheel（能真的被 pip 装上）。

    预检的最后一步是「真装一遍到临时目录」，空文件 wheel 装不上，
    正例必须用真 wheel 才测得出东西。
    """
    import zipfile
    os.makedirs(directory, exist_ok=True)
    wheel_path = os.path.join(directory, f'{name}-{version}-py3-none-any.whl')
    dist_info = f'{name}-{version}.dist-info'
    with zipfile.ZipFile(wheel_path, 'w') as zf:
        zf.writestr(f'{name}/__init__.py', 'VALUE = 1\n')
        zf.writestr(f'{dist_info}/METADATA',
                    f'Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n')
        zf.writestr(f'{dist_info}/WHEEL',
                    'Wheel-Version: 1.0\nGenerator: test\n'
                    'Root-Is-Purelib: true\nTag: py3-none-any\n')
        zf.writestr(f'{dist_info}/RECORD', '')
    return wheel_path


def _make_pkg_dir(tmp_path, wheels=None):
    """造一个「部署包目录」：collector.sh + requirements-runtime.txt + wheels/。"""
    pkg = tmp_path / 'pkg'
    pkg.mkdir()
    shutil.copy2(SCRIPT, str(pkg / 'collector.sh'))
    wheels_dir = pkg / 'wheels'
    wheels_dir.mkdir()
    for wheel in (wheels or []):
        shutil.copy2(wheel, str(wheels_dir / os.path.basename(wheel)))
    # 引导 wheel（目标机没 pip 时用）—— 预检会检查它在不在
    bootstrap = wheels_dir / 'bootstrap'
    bootstrap.mkdir()
    bootstrap.joinpath('pip-23.3.2-py3-none-any.whl').write_bytes(b'')
    return pkg


def _usable_python_dir():
    """当前解释器所在目录（版本得在 3.7~3.11 才算可用）。"""
    result = subprocess.run(
        [sys.executable, '-c', 'import sys; print(sys.version_info[0], sys.version_info[1])'],
        capture_output=True)
    if result.returncode != 0:
        return None
    major, minor = (int(x) for x in result.stdout.decode().split())
    if not ((3, 7) <= (major, minor) < (4, 0)) or (major, minor) >= (3, 12):
        return None
    return os.path.dirname(os.path.abspath(sys.executable))


def test_check_deps_passes_with_a_real_wheel(tmp_path):
    """依赖齐全时预检通过 —— 包括最后那步「真装一遍到临时目录」。"""
    python_dir = _usable_python_dir()
    if not python_dir:
        pytest.skip('当前解释器不在 3.7~3.11 区间 —— 跳过')
    wheel = _make_real_wheel(str(tmp_path / 'wheels_src'))
    pkg = _make_pkg_dir(tmp_path, wheels=[wheel])
    pkg.joinpath('requirements-runtime.txt').write_text('demo==1.0\n', encoding='utf-8')
    env = dict(os.environ)
    env['PATH'] = python_dir + os.pathsep + env.get('PATH', '')
    result = subprocess.run(
        ['bash', str(pkg / 'collector.sh'), 'check-deps', str(pkg)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    output = result.stdout.decode('utf-8', 'replace')
    assert result.returncode == 0, output
    assert '依赖预检通过' in output
    assert '离线安装演练通过' in output


def test_check_deps_reports_missing_wheel(tmp_path):
    """缺 wheel 时预检必须报错（退出码非 0）—— 别装到一半才发现。"""
    pkg = _make_pkg_dir(tmp_path)
    pkg.joinpath('requirements-runtime.txt').write_text('demo==1.0\n', encoding='utf-8')
    result = subprocess.run(
        ['bash', str(pkg / 'collector.sh'), 'check-deps', str(pkg)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = result.stdout.decode('utf-8', 'replace')
    assert result.returncode == 1, output
    assert '预检未通过' in output


# --------------------------------------------------------------------------
# 部署脚本 —— 行为性冒烟（用假 python3 真跑一遍 collector.sh install）
# --------------------------------------------------------------------------

#: 假的 python3：只实现脚本用到的调用。版本报 3.7.0 ——
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


def _run_install(bundle, prefix, stub_dir):
    env = dict(os.environ)
    env['PATH'] = stub_dir + os.pathsep + env.get('PATH', '')
    return subprocess.run(
        ['bash', os.path.join(bundle, 'collector.sh'), 'install', str(prefix)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)


def test_install_honors_the_positional_prefix(tmp_path, bundle):
    """**位置参数必须真的生效** —— 曾被静默忽略，装到 /opt 而不是用户指定的地方。"""
    prefix = tmp_path / 'custom' / 'tpops'
    result = _run_install(bundle, prefix, _stub_python(tmp_path))
    assert result.returncode == 0, result.stdout.decode('utf-8', 'replace')
    assert (prefix / 'tpops_kafka_collector').is_dir(), '装错目录了'
    assert (prefix / 'wheels').is_dir()
    assert (prefix / 'requirements-runtime.txt').is_file()


def test_install_writes_the_mode_marker(tmp_path, bundle):
    """安装模式要显式落盘 —— run 靠它选解释器，不靠目录猜。"""
    prefix = tmp_path / 'opt'
    result = _run_install(bundle, prefix, _stub_python(tmp_path))
    assert result.returncode == 0, result.stdout.decode('utf-8', 'replace')
    marker = prefix / '.install-mode'
    assert marker.is_file(), '缺 .install-mode'
    assert marker.read_text(encoding='utf-8').strip() in ('venv', 'vendor')
    assert (prefix / '.python-bin').is_file()


def test_install_is_idempotent(tmp_path, bundle):
    """重复安装不能报错（运维常常重跑一遍）。"""
    stub_dir = _stub_python(tmp_path)
    prefix = tmp_path / 'opt'
    first = _run_install(bundle, prefix, stub_dir)
    assert first.returncode == 0, first.stdout.decode('utf-8', 'replace')
    second = _run_install(bundle, prefix, stub_dir)
    assert second.returncode == 0, second.stdout.decode('utf-8', 'replace')


def test_install_accepts_prefix_equal_to_bundle_dir(tmp_path, bundle):
    """把部署包直接解压到安装目录再装 —— 不能因为「cp 自己」而失败。"""
    stub_dir = _stub_python(tmp_path)
    result = _run_install(bundle, bundle, stub_dir)
    assert result.returncode == 0, result.stdout.decode('utf-8', 'replace')
    assert os.path.isfile(os.path.join(bundle, '.install-mode'))


# release/ —— 按版本号归档
# --------------------------------------------------------------------------
def _fake_source_repo(root):
    """造一个小「工程」当源码包的输入 —— 别在测试里打整个真仓库。"""
    os.makedirs(os.path.join(str(root), 'deploy'), exist_ok=True)
    os.makedirs(os.path.join(str(root), 'data'), exist_ok=True)
    with open(os.path.join(str(root), 'CLAUDE.md'), 'w', encoding='utf-8') as handle:
        handle.write('# fake\n')
    with open(os.path.join(str(root), 'deploy', 'install.sh'), 'w', encoding='utf-8') as handle:
        handle.write('#!/usr/bin/env bash\n')
    with open(os.path.join(str(root), 'data', 'customer_metric_query_plan.json'), 'w',
              encoding='utf-8') as handle:
        handle.write('{}\n')
    with open(os.path.join(str(root), 'data', 'keep_me.json'), 'w', encoding='utf-8') as handle:
        handle.write('{}\n')
    with open(os.path.join(str(root), 'big.chm'), 'w', encoding='utf-8') as handle:
        handle.write('x')
    return str(root)


@pytest.fixture(scope='module')
def release_dir(tmp_path_factory):
    """构建一次发布目录，供本组测试共用（每次重打包太慢）。"""
    root = tmp_path_factory.mktemp('release')
    wheels = _fake_wheels(str(root / 'src_wheels'))
    return release_cli.build_release(
        release_root=str(root / 'release'), dist_root=str(root / 'dist'),
        version='9.9.9', with_pdf=False,
        source_repo_root=_fake_source_repo(root / 'fake_repo'),
        wheels_source=wheels)   # 用假 wheel，测试不依赖网络


def test_release_dir_is_version_named(release_dir):
    """发布物按**版本号**归档：release/<版本>/。"""
    assert os.path.basename(release_dir) == '9.9.9'
    assert os.path.isdir(release_dir)


def test_release_contains_deploy_bundle_with_doc_scripts_and_package(release_dir):
    """部署包里要有三样：最终部署文档说明、脚本、采集端软件包。"""
    import tarfile
    tar_path = os.path.join(release_dir, 'tpops_metric_collector_9.9.9.tar.gz')
    assert os.path.isfile(tar_path)
    with tarfile.open(tar_path) as tar:
        names = tar.getnames()
    assert 'tpops_metric_collector_9.9.9/部署运行说明.md' in names, '缺最终部署文档说明'
    assert 'tpops_metric_collector_9.9.9/collector.sh' in names, '缺部署脚本'
    assert 'tpops_metric_collector_9.9.9/tpops_kafka_collector/main.py' in names, '缺软件包'
    assert ('tpops_metric_collector_9.9.9/tpops_kafka_collector/mapping/data/'
            'metric_rules.json') in names
    assert 'tpops_metric_collector_9.9.9/wheels/requests-2.31.0-py3-none-any.whl' in names
    assert 'tpops_metric_collector_9.9.9/.env.example' in names


def test_release_dir_holds_the_release_package_and_deployment_assets(release_dir):
    """release/<版本>/ = **软件发行包 + 部署相关的所有东西**，**不含源码包**。"""
    names = set(os.listdir(release_dir))
    assert 'tpops_metric_collector_9.9.9.tar.gz' in names, '缺软件发行包'
    assert 'tpops_metric_collector_9.9.9.tar.gz.sha256' in names, '缺校验文件'
    assert 'RELEASE-NOTES.md' in names
    # 部署件平铺一份 —— 现场不用解压就能看指南、跑预检
    # （PDF 由 test_release_package_contains_pdf_guide 单独断言：那个构建才渲 PDF）
    assert '部署运行说明.md' in names, '缺部署说明'
    assert 'collector.sh' in names, '缺部署脚本'
    # 源码包不该在交付目录里
    assert not any('source' in n for n in names), '源码包不该出现在 release/ 里'


def test_release_ships_checksums_and_notes(release_dir):
    """发行包要有 sha256 校验文件，另有 RELEASE-NOTES.md。"""
    notes = os.path.join(release_dir, 'RELEASE-NOTES.md')
    assert os.path.isfile(notes)
    content = _read(notes)
    assert 'tpops_metric_collector_9.9.9.tar.gz' in content

    name = 'tpops_metric_collector_9.9.9.tar.gz'
    checksum = _read(os.path.join(release_dir, name + '.sha256')).split()
    assert checksum[0] == sha256(os.path.join(release_dir, name))
    assert checksum[1] == name


def test_source_package_is_opt_in(tmp_path):
    """工程源码包默认**不产出** —— 要源码得显式开。"""
    from deploy_bundle.bundle import SOFTWARE_NAME, SOURCE_NAME
    wheels = _fake_wheels(str(tmp_path / 'src_wheels'))
    release_dir = release_cli.build_release(
        release_root=str(tmp_path / 'release'), dist_root=str(tmp_path / 'dist'),
        version='9.9.9', with_source=True, with_pdf=False,
        source_repo_root=_fake_source_repo(tmp_path / 'fake_repo'),
        wheels_source=wheels)
    names = os.listdir(release_dir)
    assert f'{SOURCE_NAME}_9.9.9-source.tar.gz' in names
    assert f'{SOFTWARE_NAME}_9.9.9.tar.gz' in names


def test_source_package_skips_regenerable_artifacts(tmp_path):
    """源码包（若产出）不含可重建的中间产物。"""
    import tarfile
    wheels = _fake_wheels(str(tmp_path / 'src_wheels'))
    release_dir = release_cli.build_release(
        release_root=str(tmp_path / 'release'), dist_root=str(tmp_path / 'dist'),
        version='9.9.9', with_source=True, with_pdf=False,
        source_repo_root=_fake_source_repo(tmp_path / 'fake_repo'),
        wheels_source=wheels)
    tar_path = os.path.join(release_dir, 'tpops_metric_dev_9.9.9-source.tar.gz')
    with tarfile.open(tar_path) as tar:
        names = tar.getnames()
    joined = '\n'.join(names)
    assert 'tpops_metric_dev_9.9.9/CLAUDE.md' in joined
    assert 'tpops_metric_dev_9.9.9/deploy/install.sh' in joined
    assert 'tpops_metric_dev_9.9.9/data/keep_me.json' in joined
    assert 'data/customer_metric_query_plan.json' not in joined, '可重建产物不该收'


def test_source_package_excludes_release_itself():
    """源码包不能把 release/ 自己再收进去（会越滚越大）。"""
    excluded = release_cli.SOURCE_EXCLUDE
    assert 'release/' in excluded
    assert 'dist/' in excluded


def test_real_repo_source_files_exclude_generated():
    """真仓库上算出来的源码文件清单里，不该有那两个可重建产物。"""
    files = release_cli.source_file_list(REPO_ROOT)
    for generated in release_cli.SOURCE_EXCLUDE_GENERATED:
        assert generated not in files, '可重建产物不该进源码包：' + generated
    assert 'CLAUDE.md' in files
    assert 'deploy/collector.sh' in files
    assert not any(f.startswith('release/') for f in files), '源码包不该收 release/ 自己'


def test_release_package_contains_dependency_precheck_script(release_dir):
    """部署包里要有「离线依赖预检」脚本 —— 装之前先验依赖包装不装得上。"""
    import tarfile
    tar_path = os.path.join(release_dir, 'tpops_metric_collector_9.9.9.tar.gz')
    with tarfile.open(tar_path) as tar:
        names = tar.getnames()
    assert 'tpops_metric_collector_9.9.9/collector.sh' in names
    assert 'tpops_metric_collector_9.9.9/wheels/requirements.lock.txt' in names


def test_release_package_contains_pdf_guide(tmp_path):
    """部署包里要有 **PDF 版的软件部署指南**（现场运维多半不看 Markdown）。"""
    import tarfile
    from deploy_bundle.pdf import pandoc_available
    if not pandoc_available():
        pytest.skip('这台机器没有 pandoc —— 渲染不了 PDF')
    wheels = _fake_wheels(str(tmp_path / 'src_wheels'))
    release_dir = release_cli.build_release(
        release_root=str(tmp_path / 'release'), dist_root=str(tmp_path / 'dist'),
        version='9.9.9', with_source=False, with_pdf=True, wheels_source=wheels)
    tar_path = os.path.join(release_dir, 'tpops_metric_collector_9.9.9.tar.gz')
    pdf_name = 'tpops_metric_collector_9.9.9/软件部署指南.pdf'
    with tarfile.open(tar_path) as tar:
        names = tar.getnames()
        assert pdf_name in names, '缺 PDF 部署指南'
        member = tar.getmember(pdf_name)
    assert member.size > 10000, 'PDF 太小，八成没渲染出来'


def test_widen_last_column_only_touches_table_separators():
    """列宽加权只许动表格分隔行 —— 不能改到正文。"""
    from deploy_bundle.pdf import widen_last_column
    original = (
        '正文里有个 | 竖线 | 不是表格\n'
        '\n'
        '|---|---|---|\n'
        '\n'
        '| a | b | c |\n')
    widened = widen_last_column(original)
    lines = widened.splitlines()
    assert lines[0] == '正文里有个 | 竖线 | 不是表格'
    assert lines[2].startswith('|---')
    assert lines[2].endswith('|')
    # 最后一列被加权了
    assert len(lines[2].split('|')[-2]) > 3


def test_check_deps_script_finds_wheel_with_underscore_name():
    """依赖名用连字符、wheel 文件名用下划线（kafka-python / kafka_python）——
    匹配时两种写法都要试，否则会误报「离线装不上」。"""
    content = _script()
    assert '${name//-/_}' in content
    assert '${name//_/-}' in content
