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

#: 路径**分段拼接**的函数名（点分）—— ``os.path.join('data', ...)`` 这类写法
#: 的字面量是 ``'data'`` / ``'mapping'`` / ``'x.json'``，**没有一段**含
#: ``data/`` → 只扫字符串字面量的检测器抓不到（盲区）。
JOIN_FUNCTIONS = ('os.path.join', 'posixpath.join', 'ntpath.join', 'join')


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


def _forbidden_root_imports(path):
    """该模块 import 的**禁用**顶层包名（``tools`` / ``data``）。"""
    return sorted(_imported_roots(path) & set(FORBIDDEN_ROOTS))


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


def _dotted_name(node):
    """节点对应的点分名字（``os.path.join`` / ``Path``）；不是名字就返回 ``''``。"""
    parts = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if not isinstance(current, ast.Name):
        return ''
    parts.append(current.id)
    return '.'.join(reversed(parts))


def _points_at_repo_root_data(value):
    """该字符串是否以 ``data`` 这一**段**开头（相对当前目录 → 仓库根 ``data/``）。

    只认**首段**是 ``data`` 的写法 —— ``'data/mapping/x.json'`` / ``'data'``。
    ``'tpops_kafka_collector/mapping/data'`` 的首段不是 ``data`` → 不算。
    """
    if not isinstance(value, str):
        return False
    text = value.replace('\\', '/')
    return text == 'data' or text.startswith('data/')


def _mentions_repo_root(node):
    """表达式里是否引用了仓库根（``REPO_ROOT`` 名字 / 等于 REPO_ROOT 的字符串）。"""
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name) and sub.id == 'REPO_ROOT':
            return True
        if _is_string_node(sub) and _string_value(sub) == REPO_ROOT:
            return True
    return False


def _mentions_this_file(node):
    """表达式里是否引用了**本文件**（``__file__``）—— 即包内路径。

    ``os.path.join(os.path.dirname(__file__), 'data')`` 指向**包内**
    ``mapping/data/`` —— 那是**正确**用法，不能算「读仓库根」。
    """
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name) and sub.id == '__file__':
            return True
    return False


def _is_data_segment(value):
    """该字符串是否**恰好**是 ``data`` 这一段（或以 ``data/`` 开头）。"""
    if not isinstance(value, str):
        return False
    text = value.replace('\\', '/')
    return text == 'data' or text.startswith('data/')


def _segmented_repo_root_data_references(tree):
    """分段拼接的仓库根 ``data/`` 引用。

    路径**字面量**扫描抓不到分段拼接：``os.path.join('data', 'mapping', 'x.json')``
    的字面量分别是 ``'data'`` / ``'mapping'`` / ``'x.json'``，没有一段含 ``data/``。

    **四种写法**都要抓得到：

    1. ``os.path.join('data', 'mapping', 'x.json')``      —— 首段是 ``data``；
    2. ``os.path.join(REPO_ROOT, 'data', 'mapping')``      —— 仓库根 + ``data`` 段
       （首段**不是**字符串，是 ``REPO_ROOT`` 名字 —— 只看 ``args[0]`` 会漏）；
    3. ``Path(REPO_ROOT) / 'data' / 'mapping'``           —— ``/`` 运算符链，
       **根本不是 Call**；
    4. ``os.path.join(REPO_ROOT, 'data')`` 等变体。

    第 2、3 种曾经就是生产包里的写法（``mapping/table.py`` 的 ``DATA_DIR``）。
    """
    hits = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            dotted = _dotted_name(node.func)
            is_join = dotted in JOIN_FUNCTIONS
            is_path = dotted == 'Path' or dotted.endswith('.Path')
            if not (is_join or is_path):
                continue
            label = dotted
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            label = 'Path(...) / ...'
        else:
            continue

        segments = []
        for sub in ast.walk(node):
            if _is_string_node(sub):
                segments.append(_string_value(sub))
        if not segments:
            continue

        mentions_root = _mentions_repo_root(node)
        mentions_file = _mentions_this_file(node)
        if not any(_is_data_segment(s) for s in segments):
            continue
        # 命中条件：引用了仓库根，或首段就是 data（相对路径 → 仓库根 data/）
        # —— 但**基于 ``__file__`` 的包内路径**（``mapping/data/``）是正确用法，
        #    不能误报。
        first_is_data = _is_data_segment(segments[0])
        if mentions_root or (first_is_data and not mentions_file):
            hits.append('%s(%s)' % (label, ', '.join(repr(s) for s in segments)))
    return hits


def _repo_root_data_references(path):
    """该模块里指向**仓库根** ``data/`` 的路径（文档字符串不算）。

    文档字符串只是说明文字（不读文件），不算「读仓库根」；
    但**路径**若指向仓库根的 ``data/``，就说明生产代码还在读仓库根。

    两条查法都要有：

    1. **路径字面量** —— ``'data/mapping/x.json'``；
    2. **分段拼接** —— ``os.path.join('data', 'mapping', 'x.json')``
       （字面量里没有 ``data/``，只扫字面量会漏）。
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
    hits.extend(_segmented_repo_root_data_references(tree))
    return hits


def _offline_mapping_table_constructions(path):
    """该模块里构造 ``MappingTable`` 的地方（离线工具专用类）。

    ``mapping/table.py`` 里的 ``MappingTable`` 是**离线工具**用的查表类，
    它的默认 ``DATA_DIR`` 指向**仓库根** ``data/mapping``（要读 ``archive/``
    与 ``sql_merge_probe_result.json``，这两个文件不在包内 ``mapping/data/``）。

    生产查表走的是 ``mapping/loader.py``（包内数据）。
    所以：**生产代码一旦构造 ``MappingTable``，就等于又读上了仓库根。**
    """
    with io.open(path, encoding='utf-8') as handle:
        tree = ast.parse(handle.read(), filename=path)
    hits = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _dotted_name(node.func)
        if not name:
            continue
        # ``MappingTable(...)`` 与 ``MappingTable.load(...)`` 都要抓 ——
        # 后者是属性调用，末段名是 ``load``，只比末段会漏。
        if 'MappingTable' in name.split('.'):
            hits.append((name, node.lineno))
    return hits


def test_production_code_never_constructs_the_offline_mapping_table(tmp_path):
    """生产代码**不得构造** ``MappingTable``（离线工具类，读仓库根 data/）。

    它是潜伏的边界违规：默认参数指向仓库根 ``data/mapping``，
    而生产查表走 ``mapping/loader.py``（包内数据）。
    一旦有人在生产路径用它 → 又读上了仓库根 → 采集端不再是独立可部署产物。
    """
    offending = []
    for relative_path in _package_modules():
        for name, lineno in _offline_mapping_table_constructions(
                os.path.join(REPO_ROOT, relative_path)):
            offending.append('%s:%s 构造了 %s' % (relative_path, lineno, name))
    assert not offending, (
        '生产代码构造了离线专用的 MappingTable（它读仓库根 data/mapping）：%s '
        '—— 生产查表请用 tpops_kafka_collector.mapping.loader' % offending
    )


def test_the_offline_mapping_table_detector_actually_detects(tmp_path):
    """守卫自检 —— 检测器本身**抓得到违规**（否则上面那条守卫可能恒绿）。

    两种写法都要抓得到：``MappingTable()`` 与 ``MappingTable.load()``
    （后者是属性调用，默认参数指向仓库根 ``data/mapping``）。
    """
    violation = tmp_path / 'violation.py'
    violation.write_text(
        'from tpops_kafka_collector.mapping import MappingTable\n'
        '\n'
        'def build():\n'
        '    return MappingTable()\n'
        '\n'
        'def build2():\n'
        '    return MappingTable.load()\n',
        encoding='utf-8',
    )
    hits = _offline_mapping_table_constructions(str(violation))
    assert len(hits) == 2, '检测器没抓到两种写法 —— 守卫可能恒绿：%s' % (hits,)
    assert any(name.endswith('MappingTable') for name, _ in hits), hits
    assert any(name.endswith('MappingTable.load') for name, _ in hits), hits


def test_package_exists_and_is_not_empty():
    """生产包必须存在且非空 —— 边界检查的前提。"""
    assert os.path.isdir(PACKAGE_DIR)
    assert _package_modules(), '生产包里没有任何 .py 模块'


def test_the_boundary_detectors_catch_a_synthetic_violation(tmp_path):
    """守卫自检 —— 两条边界守卫**抓得到违规**（否则它们可能恒绿）。

    造一个「假的违规包」：一个模块 import 了 ``tools.*``，另一个用
    ``os.path.join('data', ...)`` 读仓库根 ``data/`` —— 两条守卫都必须报出来。

    （原来这里是一条摆设断言（``test_package_has_no_module``：只断言
    「包目录存在」，与两条守卫毫无关系）；现在它守的是**检测器本身有没有能力**。）
    """
    package = tmp_path / 'tpops_kafka_collector'
    package.mkdir()

    importer = package / 'importer.py'
    importer.write_text('import tools.mappings\n', encoding='utf-8')
    reader = package / 'reader.py'
    reader.write_text(
        "import os\nPATH = os.path.join('data', 'mapping', 'metric_rules.json')\n",
        encoding='utf-8',
    )
    clean = package / 'clean.py'
    clean.write_text(
        "import os\nPATH = os.path.join('tpops_kafka_collector', 'mapping', 'data')\n",
        encoding='utf-8',
    )

    assert _forbidden_root_imports(str(importer)) == ['tools'], (
        '检测器抓不到 tools/data 的 import —— 那条守卫是恒绿的'
    )
    assert _repo_root_data_references(str(reader)), (
        '检测器抓不到分段拼接的仓库根 data/ 路径 —— 那条守卫是恒绿的'
    )
    assert _repo_root_data_references(str(clean)) == [], '误报了包内路径'


@pytest.mark.parametrize('relative_path', _package_modules())
def test_production_package_never_imports_tools_or_data(relative_path):
    """生产包的每个模块都不得 import `tools.*` 或 `data.*`。"""
    offending = _forbidden_root_imports(relative_path)
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

    违规的**四种写法**都要抓得到：

    1. 路径字面量 —— ``'data/mapping/metric_rules.json'``；
    2. **分段拼接** —— ``os.path.join('data', 'mapping', 'x.json')``
       （字面量里根本没有 ``data/`` —— 只扫字面量的检测器会漏）；
    3. **仓库根 + data 段** —— ``os.path.join(REPO_ROOT, 'data', 'mapping')``
       （首段不是字符串，只看 ``args[0]`` 的检测器会漏 —— **这曾经就是
       生产包里的写法**）；
    4. **运算符链** —— ``Path(REPO_ROOT) / 'data' / 'mapping'``
       （根本不是 Call，只扫 Call 的检测器会漏）。
    """
    offender = tmp_path / 'offender.py'
    offender.write_text("PATH = 'data/mapping/metric_rules.json'\n", encoding='utf-8')
    assert _repo_root_data_references(str(offender)), (
        '检测器抓不到仓库根 data/ 路径 —— 那条守卫是恒绿的'
    )

    segmented = tmp_path / 'segmented.py'
    segmented.write_text(
        "import os\nPATH = os.path.join('data', 'mapping', 'metric_rules.json')\n",
        encoding='utf-8',
    )
    assert _repo_root_data_references(str(segmented)), (
        '检测器抓不到分段拼接的仓库根 data/ 路径 —— 那条守卫是恒绿的'
    )

    rooted = tmp_path / 'rooted.py'
    rooted.write_text(
        "import os\n"
        "REPO_ROOT = '/repo'\n"
        "PATH = os.path.join(REPO_ROOT, 'data', 'mapping')\n",
        encoding='utf-8',
    )
    assert _repo_root_data_references(str(rooted)), (
        '检测器抓不到「仓库根 + data 段」的写法 —— 那条守卫是恒绿的'
    )

    chained = tmp_path / 'chained.py'
    chained.write_text(
        "from pathlib import Path\n"
        "REPO_ROOT = Path('/repo')\n"
        "PATH = REPO_ROOT / 'data' / 'mapping'\n",
        encoding='utf-8',
    )
    assert _repo_root_data_references(str(chained)), (
        '检测器抓不到「运算符链」的写法 —— 那条守卫是恒绿的'
    )

    innocent = tmp_path / 'innocent.py'
    innocent.write_text(
        "PATH = 'tpops_kafka_collector/mapping/data'\n", encoding='utf-8')
    assert _repo_root_data_references(str(innocent)) == [], '误报了包内路径'

    innocent_join = tmp_path / 'innocent_join.py'
    innocent_join.write_text(
        "import os\nPATH = os.path.join('tpops_kafka_collector', 'mapping', 'data')\n",
        encoding='utf-8',
    )
    assert _repo_root_data_references(str(innocent_join)) == [], '误报了包内路径'

    # 基于 ``__file__`` 的**包内**数据目录是正确用法 —— 不能误报
    package_local = tmp_path / 'package_local.py'
    package_local.write_text(
        "import os\n"
        "PACKAGE_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')\n",
        encoding='utf-8',
    )
    assert _repo_root_data_references(str(package_local)) == [], (
        '误报了包内的 __file__ 数据目录'
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
