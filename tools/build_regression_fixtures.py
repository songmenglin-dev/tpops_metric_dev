#!/usr/bin/env python3
"""生成 ``tests/fixtures/`` 下的回归夹具。

**夹具是什么**：把实测留档**逐字节复制**成**冻结的样本**，让回归判据
（「值等于期望值」）可重复：

===========================================  ====================================
夹具                                        来源
===========================================  ====================================
``tests/fixtures/metric_response/*.json``    ``data/metric_response/`` 的 99 份 JSON
``tests/fixtures/merged_sql/…json``          ``data/merged_sql/sql_merge_probe_result.json``
``tests/fixtures/expected_values.json``      ``data/mapping/expected_values.json``（wave-0 的那一张）
``tests/fixtures/expected_values_non_b.json`` 独立推导器现算（A/C/D 16 条）
===========================================  ====================================

**为什么逐字节复制而不是重新序列化**：重新序列化会改缩进、键序、浮点表示 ——
改完就不是「同一份样本」了，回归就失去了意义。留档里的原始响应体**一字不改**。

**期望值表只有一份真相源**：B 类那 82 条**不是重新推导的**，而是 wave-0 的
独立推导器（``tools/expected_deriver.py``）现算的结果 —— 与
``data/mapping/expected_values.json`` 逐字节相同。两处并存由
``tests/regression/test_fixture_integrity.py`` 的逐字节相等断言把住。

用法::

    python tools/build_regression_fixtures.py          # 生成并写盘
    python tools/build_regression_fixtures.py --check  # 只校验是否最新，不写
"""
import json
import logging
import os
import shutil
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS_DIR = os.path.join(REPO_ROOT, 'tools')
for _path in (TOOLS_DIR, REPO_ROOT):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from expected_deriver_non_b import (  # noqa: E402  （路径要在 sys.path 调整之后）
    NOW_MS,
    ArchiveError,
    derive_non_b_metrics,
)

LOGGER = logging.getLogger('build_regression_fixtures')

#: 夹具根目录。
FIXTURES_DIR = os.path.join(REPO_ROOT, 'tests', 'fixtures')

#: 逐指标留档夹具目录。
ARCHIVE_FIXTURE_DIR = os.path.join(FIXTURES_DIR, 'metric_response')

#: 合并形态留档夹具目录。
MERGED_FIXTURE_DIR = os.path.join(FIXTURES_DIR, 'merged_sql')

#: B 类期望值表夹具。
EXPECTED_VALUES_PATH = os.path.join(FIXTURES_DIR, 'expected_values.json')

#: A/C/D 期望值表夹具。
EXPECTED_NON_B_PATH = os.path.join(FIXTURES_DIR, 'expected_values_non_b.json')

#: 留档源目录（活的留档）。
SOURCE_ARCHIVE_DIR = os.path.join(REPO_ROOT, 'data', 'metric_response')

#: 合并形态留档源。
SOURCE_MERGED_PROBE = os.path.join(
    REPO_ROOT, 'data', 'merged_sql', 'sql_merge_probe_result.json')

#: 映射声明源（仓库根那份）。
SOURCE_MAPPING_DIR = os.path.join(REPO_ROOT, 'data', 'mapping')

#: 留档里要复制进夹具的文件（只复制 JSON —— README 是文档，不是样本）。
ARCHIVE_SUFFIX = '.json'


class BuildError(Exception):
    """生成失败。"""


# -- 期望值表 -------------------------------------------------------------

def build_non_b_table():
    """A/C/D 16 条指标的期望值表（由独立推导器现算）。

    这是 ``tests/fixtures/expected_values_non_b.json`` 的内容 ——
    **不经规则引擎**（引擎是被测对象）。
    """
    metric_rules = _read_json(os.path.join(SOURCE_MAPPING_DIR, 'metric_rules.json'))
    response_rules = _read_json(
        os.path.join(SOURCE_MAPPING_DIR, 'response_rules.json'))
    expected = derive_non_b_metrics(
        metric_rules, response_rules, SOURCE_ARCHIVE_DIR, NOW_MS)
    return {
        'version': 1,
        'generated_by': 'tools/build_regression_fixtures.py',
        'deriver': 'tools/expected_deriver_non_b.py',
        'now_ms': NOW_MS,
        'metric_count': len(expected),
        'expected': expected,
    }


def build_b_table():
    """B 类 82 条指标的期望值表。

    **不是重新推导** —— 直接用 wave-0 的生成器（``tools/build_expected_values.py``），
    与 ``data/mapping/expected_values.json`` 逐字节相同。
    """
    import build_expected_values

    return build_expected_values.build()


# -- 夹具内容 -------------------------------------------------------------

def _archive_fixtures():
    """逐指标留档夹具：``{文件名: 绝对路径}``。"""
    names = sorted(
        name for name in os.listdir(SOURCE_ARCHIVE_DIR)
        if name.endswith(ARCHIVE_SUFFIX)
    )
    if not names:
        raise BuildError('留档目录里没有 JSON 样本')
    return dict((name, os.path.join(ARCHIVE_FIXTURE_DIR, name)) for name in names)


def _dump(payload):
    """可 JSON 序列化的结构 → 字节（与仓库根生成物同一套格式）。"""
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    return (text + '\n').encode('utf-8')


def _read_bytes(path):
    with open(path, 'rb') as handle:
        return handle.read()


def _read_json(path):
    with open(path, encoding='utf-8') as handle:
        return json.load(handle)


# -- 生成 / 校验 ----------------------------------------------------------

def write_fixtures():
    """把全部夹具写盘。"""
    _write_archives()
    _write_merged_probe()
    _write_bytes(EXPECTED_VALUES_PATH, _dump(build_b_table()))
    _write_bytes(EXPECTED_NON_B_PATH, _dump(build_non_b_table()))
    LOGGER.info('已写出夹具：%s', FIXTURES_DIR)


def check_fixtures():
    """校验夹具是否与来源一致（不写盘）。返回问题清单。"""
    problems = []
    problems.extend(_check_archives())
    problems.extend(_check_merged_probe())
    problems.extend(_check_table(EXPECTED_VALUES_PATH, build_b_table()))
    problems.extend(_check_table(EXPECTED_NON_B_PATH, build_non_b_table()))
    return problems


def _write_archives():
    """逐字节复制逐指标留档。"""
    os.makedirs(ARCHIVE_FIXTURE_DIR, exist_ok=True)
    for name, target in sorted(_archive_fixtures().items()):
        with open(os.path.join(SOURCE_ARCHIVE_DIR, name), 'rb') as source:
            payload = source.read()
        _write_bytes(target, payload)


def _write_merged_probe():
    """逐字节复制合并形态留档。"""
    os.makedirs(MERGED_FIXTURE_DIR, exist_ok=True)
    target = os.path.join(MERGED_FIXTURE_DIR, os.path.basename(SOURCE_MERGED_PROBE))
    with open(SOURCE_MERGED_PROBE, 'rb') as source:
        _write_bytes(target, source.read())


def _write_bytes(path, payload):
    """原子写一个文件。"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temp = path + '.tmp'
    with open(temp, 'wb') as handle:
        handle.write(payload)
    os.replace(temp, path)


def _check_archives():
    """逐字节校验留档夹具。"""
    problems = []
    expected = _archive_fixtures()
    if not os.path.isdir(ARCHIVE_FIXTURE_DIR):
        return ['夹具目录不存在：%s' % ARCHIVE_FIXTURE_DIR]
    actual = set(
        name for name in os.listdir(ARCHIVE_FIXTURE_DIR)
        if name.endswith(ARCHIVE_SUFFIX)
    )
    for name in sorted(set(expected) - actual):
        problems.append('夹具缺少留档 %s' % name)
    for name in sorted(actual - set(expected)):
        problems.append('夹具多出留档 %s' % name)
    for name in sorted(set(expected) & actual):
        source = os.path.join(SOURCE_ARCHIVE_DIR, name)
        target = expected[name]
        if _read_bytes(source) != _read_bytes(target):
            problems.append('夹具与留档不一致：%s' % name)
    return problems


def _check_merged_probe():
    """逐字节校验合并形态留档夹具。"""
    target = os.path.join(MERGED_FIXTURE_DIR, os.path.basename(SOURCE_MERGED_PROBE))
    if not os.path.isfile(target):
        return ['夹具缺少合并形态留档：%s' % target]
    if _read_bytes(SOURCE_MERGED_PROBE) != _read_bytes(target):
        return ['夹具与合并形态留档不一致：%s' % target]
    return []


def _check_table(path, payload):
    """校验期望值表夹具是否与生成器现算的一致。"""
    if not os.path.isfile(path):
        return ['夹具缺少期望值表：%s' % path]
    if _read_bytes(path) != _dump(payload):
        return ['期望值表与生成器输出不一致：%s' % path]
    return []


def main(argv):
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    check_only = '--check' in argv
    try:
        if check_only:
            problems = check_fixtures()
            if problems:
                for item in problems:
                    LOGGER.error('%s', item)
                return 1
            LOGGER.info('夹具是最新的：%s', FIXTURES_DIR)
            return 0
        write_fixtures()
        return 0
    except (ArchiveError, BuildError) as exc:
        LOGGER.error('生成失败：%s', exc)
        return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
