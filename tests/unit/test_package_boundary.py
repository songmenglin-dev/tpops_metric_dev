# -*- coding: utf-8 -*-
"""生产代码边界：`tpops_kafka_collector/` 是唯一的生产代码。

设计决策 2：`tools/` 与 `data/` 只作参考与离线工具，生产代码不得依赖它们。
若生产包 import 了 `tools.*` 或 `data.*`，部署产物就不自洽（要带着仓库根的
离线脚本目录一起装，而那里面有会连真实 TPOPS 平台的脚本）。

**这是 import 层面的边界检查**，不是文件读取层面的 —— 映射数据目前仍从
仓库根 `data/mapping/` 读取，搬进包内是 wave-2a 任务 2.1 的交付物。
"""
from __future__ import absolute_import

import ast
import io
import os

import pytest

from conftest import REPO_ROOT

PACKAGE_DIR = os.path.join(REPO_ROOT, 'tpops_kafka_collector')
FORBIDDEN_ROOTS = ('tools', 'data')


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
