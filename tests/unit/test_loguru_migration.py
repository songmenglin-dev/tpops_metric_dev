"""生产包**全面改用 loguru** 的守卫（wave-2 任务 2.3）。

守护四件事：

1. 生产包里不再出现 ``logging.getLogger`` —— 唯一例外是
   :mod:`tpops_kafka_collector.logging_setup`，它要装 ``InterceptHandler``
   把第三方库的 stdlib 日志转发进 loguru；
2. 用到 logger 的模块都 ``from loguru import logger``，且 logger 的创建行是
   ``LOGGER = logger``（5 个采集器模块多一个 ``.bind(channel=…)``）；
3. **只有 5 个采集器模块**绑 channel —— 别的模块绑了就会把记录写进通道日志；
4. **日志调用点里不再有 printf 风格的占位符** —— loguru 用的是
   ``str.format`` 语义（``{}``），``%s`` 会被**静默丢弃**（不报错、值全丢）。
"""

import ast
import os
import re

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PACKAGE_ROOT = os.path.join(REPO_ROOT, 'tpops_kafka_collector')

#: 唯一允许出现 stdlib logging 的模块（要装 InterceptHandler）。
ALLOWED_STDLIB_MODULES = ('logging_setup.py',)

#: 采集器模块 → 通道名。
CHANNEL_BINDINGS = {
    'collectors/monitor.py': 'monitor',
    'collectors/command_center.py': 'command_center',
    'collectors/command_center_assembly.py': 'command_center',
    'collectors/diagnostic.py': 'diagnostic',
    'collectors/instance.py': 'instance',
    'collectors/alarm.py': 'alarm',
}

LEVELS = ('debug', 'info', 'warning', 'error', 'exception', 'critical', 'log', 'success')
PRINTF_SPEC = re.compile(r'%[-+ #0-9.*]*[a-zA-Z%]')


def _module_paths():
    for dirpath, _, names in os.walk(PACKAGE_ROOT):
        for name in sorted(names):
            if name.endswith('.py'):
                yield os.path.join(dirpath, name)


def _read(path):
    with open(path, encoding='utf-8') as handle:
        return handle.read()


def _relative(path):
    return os.path.relpath(path, PACKAGE_ROOT)


def test_production_package_does_not_use_stdlib_get_logger():
    """生产包里不再有 logging.getLogger（InterceptHandler 模块除外）。"""
    offenders = [rel for rel in (_relative(p) for p in _module_paths())
                 if rel not in ALLOWED_STDLIB_MODULES and 'logging.getLogger' in _read(
                     os.path.join(PACKAGE_ROOT, rel))]
    assert not offenders, f'还在用 stdlib logging.getLogger：{offenders}'


def test_modules_using_logger_import_loguru():
    """用到 logger 的模块都 from loguru import logger，且创建行是 LOGGER = logger。"""
    missing = []
    for path in _module_paths():
        rel = _relative(path)
        if rel in ALLOWED_STDLIB_MODULES:
            continue
        text = _read(path)
        if 'LOGGER.' not in text:
            continue
        if 'from loguru import logger' not in text:
            missing.append(rel + '（没 import loguru）')
        elif not re.search(r'^LOGGER = logger', text, re.M):
            missing.append(rel + '（logger 创建行不对）')
    assert not missing, missing


def test_collectors_bind_their_channel():
    """5 个采集器模块各自绑自己的通道。"""
    for rel, channel in sorted(CHANNEL_BINDINGS.items()):
        text = _read(os.path.join(PACKAGE_ROOT, rel))
        assert f'LOGGER = logger.bind(channel="{channel}")' in text, rel


def test_non_collector_modules_do_not_bind_a_channel():
    """非采集器模块不得绑 channel（否则记录会落进通道日志）。"""
    offenders = []
    for path in _module_paths():
        rel = _relative(path)
        if rel in CHANNEL_BINDINGS:
            continue
        if re.search(r'^LOGGER = logger\.bind\(channel=', _read(path), re.M):
            offenders.append(rel)
    assert not offenders, offenders


def test_log_calls_use_brace_placeholders():
    """日志调用点里不再有 printf 风格占位符（loguru 是 str.format 语义）。"""
    offenders = []
    for path in _module_paths():
        tree = ast.parse(_read(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute) or func.attr not in LEVELS:
                continue
            if getattr(func.value, 'id', None) not in ('LOGGER', 'logger'):
                continue
            if not node.args:
                continue
            first = node.args[0]
            if isinstance(first, ast.Str) and PRINTF_SPEC.search(first.s):
                offenders.append(f'{_relative(path)}:{node.lineno} {first.s[:50]}')
    assert not offenders, f'printf 风格的占位符会被 loguru 静默丢弃：{offenders}'


def test_log_calls_placeholder_count_matches_arguments():
    """占位符个数必须与参数个数一致（否则 loguru 抛异常 / 丢值）。"""
    import string
    formatter = string.Formatter()
    problems = []
    for path in _module_paths():
        tree = ast.parse(_read(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute) or func.attr not in LEVELS:
                continue
            if getattr(func.value, 'id', None) not in ('LOGGER', 'logger'):
                continue
            if not node.args or not isinstance(node.args[0], ast.Str):
                continue
            text = node.args[0].s
            placeholders = sum(1 for _, field, _, _ in formatter.parse(text)
                               if field is not None)
            supplied = len(node.args) - 1 + len(node.keywords)
            if placeholders != supplied:
                problems.append(
                    f'{_relative(path)}:{node.lineno} 占位符 {placeholders} '
                    f'!= 参数 {supplied}')
    assert not problems, problems
