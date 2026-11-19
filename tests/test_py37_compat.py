"""Python 3.7.9 兼容性门禁。

本仓库的运行目标是 **Python 3.7.9**（见 CLAUDE.md「Python 3.7.9 红线」），
但开发机 / 测试机可能是 3.13 —— **本地 pytest 全绿不代表目标运行时能跑**。
这里用 ast 做一次语法级 + 标准库级的 3.7 校验：

  1. ``ast.parse(src, feature_version=(3, 7))`` —— 拦截 3.8+ 才有的语法
     （海象 ``:=``、``f"{x=}"``、字典合并 ``d1 | d2`` 等）。
  2. 扫描 CLAUDE.md 点名的**禁用 API**（3.8+ 才有的标准库成员）。
  3. 拦截**内建泛型注解**（``list[str]``、``dict[str, int]``）——
     3.7 运行期直接 ``TypeError: 'type' object is not subscriptable``。
  4. 拦截 **PEP 604 联合注解**（``int | str``）——
     3.7 运行期对类型做 ``|`` 会直接 ``TypeError``。

只用标准库 ast，**在 3.13 上也能查出 3.7 的问题**，所以这条门禁在任何机器上都有意义。
"""
import ast
import os

import pytest

from conftest import REPO_ROOT

# 要检查的目录（相对仓库根）。tpops_kafka_collector/ 尚未建立，出现了就一起检查。
SCAN_DIRS = ('tools', 'tests', 'data')
SCAN_ROOT_FILES = True

# CLAUDE.md「Python 3.7.9 红线」点名的禁用 API → 原因
BANNED_ATTRIBUTES = {
    'cached_property': 'functools.cached_property 是 3.8+',
    'removeprefix': 'str.removeprefix 是 3.9+',
    'removesuffix': 'str.removesuffix 是 3.9+',
    'to_thread': 'asyncio.to_thread 是 3.9+',
}

BANNED_MODULES = {
    'zoneinfo': 'zoneinfo 是 3.9+，用 pytz / dateutil',
    'importlib.metadata': 'importlib.metadata 是 3.8+',
}

BANNED_FROM_IMPORTS = {
    ('math', 'prod'): 'math.prod 是 3.8+',
    ('functools', 'cached_property'): 'functools.cached_property 是 3.8+',
}

BANNED_TYPING_NAMES = {
    'Literal': 'typing.Literal 是 3.8+',
    'Protocol': 'typing.Protocol 是 3.8+',
    'TypedDict': 'typing.TypedDict 是 3.8+',
    'Final': 'typing.Final 是 3.8+',
}

# 内建泛型注解的宿主（3.7 运行期不可下标）
BUILTIN_GENERIC_HOSTS = {'list', 'dict', 'set', 'tuple', 'frozenset', 'type'}


def _iter_python_files():
    """仓库内所有需要检查的 .py 文件。"""
    paths = []
    if SCAN_ROOT_FILES:
        for name in sorted(os.listdir(REPO_ROOT)):
            if name.endswith('.py'):
                paths.append(os.path.join(REPO_ROOT, name))
    for sub in SCAN_DIRS:
        base = os.path.join(REPO_ROOT, sub)
        if not os.path.isdir(base):
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in ('__pycache__', '.git')]
            for name in sorted(filenames):
                if name.endswith('.py'):
                    paths.append(os.path.join(dirpath, name))
    return paths


def _rel(path):
    return os.path.relpath(path, REPO_ROOT)


def _read(path):
    with open(path, encoding='utf-8') as handle:
        return handle.read()


def _annotation_nodes(tree):
    """所有注解位置的 AST 节点（用于 PEP 604 检测）。"""
    nodes = []
    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign):
            nodes.append(node.annotation)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.returns is not None:
                nodes.append(node.returns)
            for arg in list(node.args.args) + list(node.args.kwonlyargs):
                if arg.annotation is not None:
                    nodes.append(arg.annotation)
    return nodes


def _check_file(path):
    """返回该文件的问题清单（每条是 (行号, 说明)）。"""
    problems = []
    source = _read(path)

    # 1. 语法级：3.7 能否解析
    # feature_version 是 3.8+ 的参数；在 3.7 解释器上跑这道门禁时没有它，
    # 但 3.7 自带的解析器本身就拒绝 3.8+ 语法，语法级检查天然成立。
    try:
        tree = ast.parse(source, filename=path, feature_version=(3, 7))
    except TypeError:
        # 当前解释器就是 3.7（或更早）：用它自己的解析器
        try:
            tree = ast.parse(source, filename=path)
        except SyntaxError as exc:
            problems.append((exc.lineno or 0, f'3.7 无法解析：{exc.msg}'))
            return problems
    except SyntaxError as exc:
        problems.append((exc.lineno or 0, f'3.7 无法解析：{exc.msg}'))
        # 语法已经不过 3.7，但用当前版本再解析一遍，把 API 层的问题也一并报出来
        try:
            tree = ast.parse(source, filename=path)
        except SyntaxError:
            return problems

    for node in ast.walk(tree):
        # 2. 禁用属性
        if isinstance(node, ast.Attribute) and node.attr in BANNED_ATTRIBUTES:
            problems.append((node.lineno, BANNED_ATTRIBUTES[node.attr]))

        # 3. 禁用模块
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in BANNED_MODULES:
                    problems.append((node.lineno, BANNED_MODULES[alias.name]))
        elif isinstance(node, ast.ImportFrom):
            if node.module in BANNED_MODULES:
                problems.append((node.lineno, BANNED_MODULES[node.module]))
            for alias in node.names:
                reason = BANNED_FROM_IMPORTS.get((node.module, alias.name))
                if reason:
                    problems.append((node.lineno, reason))
                if node.module == 'typing' and alias.name in BANNED_TYPING_NAMES:
                    problems.append((node.lineno, BANNED_TYPING_NAMES[alias.name]))

        # 4. 内建泛型注解
        elif isinstance(node, ast.Subscript):
            host = node.value
            if isinstance(host, ast.Name) and host.id in BUILTIN_GENERIC_HOSTS:
                problems.append((
                    node.lineno,
                    f'内建泛型注解 {host.id}[...] 在 3.7 运行期 TypeError，'
                    '用 typing.List/Dict/Optional/Tuple/Set',
                ))

    # 5. PEP 604 联合注解
    for annotation in _annotation_nodes(tree):
        for sub in ast.walk(annotation):
            if isinstance(sub, ast.BinOp) and isinstance(sub.op, ast.BitOr):
                problems.append((
                    sub.lineno,
                    'PEP 604 联合注解 X | Y 在 3.7 运行期 TypeError，用 typing.Union[X, Y]',
                ))
                break

    return problems


@pytest.mark.parametrize('path', _iter_python_files(), ids=_rel)
def test_file_is_python37_compatible(path):
    """每个 .py 文件都必须能在 Python 3.7 上解析并只用 3.7 有的 API。"""
    problems = _check_file(path)
    assert not problems, '\n'.join(
        f'{_rel(path)}:{lineno} {reason}' for lineno, reason in problems
    )
