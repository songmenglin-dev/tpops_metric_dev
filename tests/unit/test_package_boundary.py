# -*- coding: utf-8 -*-
"""生产代码边界：`tpops_kafka_collector/` 是唯一的生产代码。

设计决策 2：`tools/` 与 `data/` 只作参考与离线工具，生产代码不得依赖它们。
若生产包 import 了 `tools.*` 或 `data.*`，部署产物就不自洽（要带着仓库根的
离线脚本目录一起装，而那里面有会连真实 TPOPS 平台的脚本）。

**两条边界都要守**（两层都要查，缺一层就有缝）：

1. **import 层面** —— 生产包不得 import ``tools.*`` / ``data.*``；
2. **文件读取层面** —— 生产代码不得读**仓库根**的 ``data/``。
   映射数据已经在 wave-2a（任务 2.1）搬进包内
   （``tpops_kafka_collector/mapping/data/``），采集端是**独立可部署**的产物 ——
   还读仓库根 ``data/`` 就等于部署时要带着整个仓库走。

> 第 2 条以前没有守卫（当时的注释写着「映射数据仍从仓库根读取」）——
> 那条注释已经过期，守卫按**现状**补上。
"""
from __future__ import absolute_import

import ast
import io
import os
import re

import pytest

from conftest import REPO_ROOT

PACKAGE_DIR = os.path.join(REPO_ROOT, 'tpops_kafka_collector')
FORBIDDEN_ROOTS = ('tools', 'data')

#: 仓库根 ``data/`` 的路径片段（**路径段级别**匹配 —— ``metadata/`` 不算）。
REPO_ROOT_DATA = re.compile(r'(?:^|[/\\])data[/\\]')


def _package_modules():
    """包内全部 .py 模块（相对仓库根的路径）。"""
    found = []
    for root, dirs, files in os.walk(PACKAGE_DIR):
        dirs[:] = [d for d in dirs if d != '__pycache__']
        for name in files:
            if name.endswith('.py'):
                found.append(os.path.relpath(os.path.join(root, name), REPO_ROOT))
    return sorted(found)


def _imported_roots(path):
    """该模块 import 的顶层包名（相对导入记为 '.'，表示包内）。"""
    with io.open(path, encoding='utf-8') as handle:
        tree = ast.parse(handle.read(), filename=path)
    roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                roots.add(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                continue  # 相对导入 = 包内
            if node.module:
                roots.add(node.module.split('.')[0])
    return roots


def _is_string_node(node):
    """该 AST 节点是不是字符串常量。

    3.7 的 AST 是 ``ast.Str``、3.8+ 是 ``ast.Constant`` —— 这里按**类名**判断，
    两个 Python 版本都能跑（3.13 已经把 ``ast.Str`` 删了）。
    """
    name = type(node).__name__
    if name == 'Str':
        return isinstance(node.s, str)
    if name == 'Constant':
        return isinstance(node.value, str)
    return False


def _string_value(node):
    """字符串常量的值（3.7 的 ``.s`` / 3.8+ 的 ``.value``）。"""
    if type(node).__name__ == 'Str':
        return node.s
    return node.value


def _docstring_nodes(tree):
    """各模块/类/函数的**文档字符串**节点（按身份去重）。"""
    found = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef,
                                 ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = getattr(node, 'body', None)
        if not body:
            continue
        first = body[0]
        if isinstance(first, ast.Expr) and _is_string_node(first.value):
            found.add(id(first.value))
    return found


def _repo_root_data_references(path):
    """该模块里指向**仓库根** ``data/`` 的路径字面量（文档字符串不算）。

    文档字符串只是说明文字（不读文件），不算「读仓库根」；
    但**路径字面量**若指向仓库根的 ``data/``，就说明生产代码还在读仓库根。
    """
    with io.open(path, encoding='utf-8') as handle:
        tree = ast.parse(handle.read(), filename=path)
    docstrings = _docstring_nodes(tree)
    hits = []
    for node in ast.walk(tree):
        if not _is_string_node(node) or id(node) in docstrings:
            continue
        value = _string_value(node)
        if REPO_ROOT_DATA.search(value):
            hits.append(value)
    return hits


def test_package_exists_and_is_not_empty():
    """生产包必须存在且非空 —— 边界检查的前提。"""
    assert os.path.isdir(PACKAGE_DIR)
    assert _package_modules(), '生产包里没有任何 .py 模块'


def test_package_has_no_module():
    """反证：包存在就不会走到这条。保留是为了让上面的失败信息可读。"""
    assert os.path.isdir(PACKAGE_DIR)


@pytest.mark.parametrize('relative_path', _package_modules())
def test_production_package_never_imports_tools_or_data(relative_path):
    """生产包的每个模块都不得 import `tools.*` 或 `data.*`。"""
    offending = sorted(_imported_roots(relative_path) & set(FORBIDDEN_ROOTS))
    assert not offending, (
        '%s 导入了 %s —— 生产代码不得依赖仓库根的 tools/ 与 data/ '
        '（设计决策 2）' % (relative_path, offending)
    )


@pytest.mark.parametrize('relative_path', _package_modules())
def test_production_code_never_reads_the_repository_root_data_directory(relative_path):
    """生产代码**不读仓库根**的 ``data/``（映射数据已搬进包内）。

    读仓库根 ``data/`` 等于部署时要带着整个仓库走 —— 采集端本该是
    **独立可部署**的产物。
    """
    offending = _repo_root_data_references(relative_path)
    assert not offending, (
        '%s 引用了仓库根的 data/ 路径 %s —— 生产代码只准读包内 '
        'tpops_kafka_collector/mapping/data/' % (relative_path, offending)
    )


def test_package_mapping_data_is_inside_the_package():
    """映射数据在**包内** ``mapping/data/`` —— 不是仓库根的 ``data/mapping``。"""
    from tpops_kafka_collector.mapping import loader

    data_dir = os.path.abspath(loader.PACKAGE_DATA_DIR)
    package_dir = os.path.abspath(PACKAGE_DIR)
    assert data_dir.startswith(package_dir + os.sep), (
        '映射数据目录不在生产包内：%s' % data_dir
    )
    assert os.path.isdir(data_dir), data_dir
    assert [name for name in os.listdir(data_dir) if name.endswith('.json')], (
        '包内映射数据目录是空的：%s' % data_dir
    )


def test_the_repository_root_data_detector_actually_detects(tmp_path):
    """守卫自检 —— 检测器本身**抓得到违规**（否则上面那条守卫可能恒绿）。

    同时守住「不误报」：**包内**的 ``.../mapping/data`` 不算仓库根 ``data/``。
    """
    offender = tmp_path / 'offender.py'
    offender.write_text("PATH = 'data/mapping/metric_rules.json'\n", encoding='utf-8')
    assert _repo_root_data_references(str(offender)), (
        '检测器抓不到仓库根 data/ 路径 —— 那条守卫是恒绿的'
    )

    innocent = tmp_path / 'innocent.py'
    innocent.write_text(
        "PATH = 'tpops_kafka_collector/mapping/data'\n", encoding='utf-8')
    assert _repo_root_data_references(str(innocent)) == [], '误报了包内路径'


def test_reference_copy_of_mapping_layer_is_deleted():
    """`tools/mappings/` 是被复制进包内的参考副本，切换依赖后应已删除。

    留着它就等于留着两份映射层实现 —— 改一份忘另一份，测试全绿也不代表
    生产代码对（这正是「静默出 0」那类缺陷的温床）。
    """
    leftover = os.path.join(REPO_ROOT, 'tools', 'mappings')
    remaining = []
    if os.path.isdir(leftover):
        remaining = [n for n in os.listdir(leftover) if n.endswith('.py')]
    assert not remaining, (
        'tools/mappings/ 仍留有 %s：生产代码在 tpops_kafka_collector/mapping/，'
        '留两份实现会静默分叉' % remaining
    )


def test_package_mapping_layer_is_the_only_implementation():
    """包内的映射层必须齐备（决策 2 要求的 5 个模块 + loader/sqlgen/masking 由后续任务补）。"""
    mapping_dir = os.path.join(PACKAGE_DIR, 'mapping')
    assert os.path.isdir(mapping_dir)
    for name in ('table.py', 'rules.py', 'decode.py', 'message.py', 'nodes.py'):
        assert os.path.exists(os.path.join(mapping_dir, name)), (
            'tpops_kafka_collector/mapping/%s 缺失' % name
        )
