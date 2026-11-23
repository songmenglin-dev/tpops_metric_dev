"""回归夹具的路径、加载器与公共夹具。

**夹具是什么**：把实测留档**逐字节复制**到 ``tests/fixtures/`` 下作为固定样本：

- ``tests/fixtures/metric_response/`` —— 98 份逐指标原生响应 + 索引 manifest；
- ``tests/fixtures/merged_sql/sql_merge_probe_result.json`` —— 合并形态留档；
- ``tests/fixtures/expected_values.json`` —— B 类 82 条指标的期望值表
  （**wave-0 那一张的副本**，不是新生成的一份）；
- ``tests/fixtures/expected_values_non_b.json`` —— A/C/D 16 条指标的期望值表。

**为什么要复制而不是直接读 ``data/``**：``data/`` 是**活的留档**（会被重新抓取、
被生成器改写），回归样本必须是**冻结的** —— 留档变了回归就该红，而不是跟着变绿。
「冻结的样本」与「活的留档」之间的差异由 ``test_fixture_integrity.py`` 显式报出来，
而不是悄悄生效。
"""
import json
import os

import pytest

from conftest import REPO_ROOT

#: 夹具根目录。
FIXTURES_DIR = os.path.join(REPO_ROOT, 'tests', 'fixtures')

#: 逐指标留档夹具目录（98 份原生响应 + manifest）。
METRIC_RESPONSE_DIR = os.path.join(FIXTURES_DIR, 'metric_response')

#: 合并形态留档夹具目录。
MERGED_SQL_DIR = os.path.join(FIXTURES_DIR, 'merged_sql')

#: 合并形态留档夹具文件。
MERGED_PROBE_PATH = os.path.join(
    MERGED_SQL_DIR, 'sql_merge_probe_result.json')

#: B 类期望值表夹具（wave-0 那一张的副本）。
EXPECTED_VALUES_PATH = os.path.join(FIXTURES_DIR, 'expected_values.json')

#: A/C/D 期望值表夹具。
EXPECTED_NON_B_PATH = os.path.join(FIXTURES_DIR, 'expected_values_non_b.json')

#: 包内映射数据目录（**生产用的那份声明** —— 回归要验的是跑起来的那份）。
PACKAGE_DATA_DIR = os.path.join(
    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data')

#: 仓库根的留档目录（「活的留档」，夹具是它的冻结副本）。
LIVE_METRIC_RESPONSE_DIR = os.path.join(REPO_ROOT, 'data', 'metric_response')

#: 仓库根的合并形态留档。
LIVE_MERGED_PROBE_PATH = os.path.join(
    REPO_ROOT, 'data', 'merged_sql', 'sql_merge_probe_result.json')

#: 仓库根的期望值表（wave-0 生成物的所在地）。
LIVE_EXPECTED_VALUES_PATH = os.path.join(
    REPO_ROOT, 'data', 'mapping', 'expected_values.json')

#: B 类通道名。
B_CLASS = 'command_center_sql'

#: 非 B 类通道（A 监控指标 / C 诊断优化 / D 实例接口）。
NON_B_CHANNELS = ('monitor_metrics', 'diagnostic', 'instance')


def load_json(path):
    """读一个 JSON 文件（UTF-8）。"""
    with open(path, encoding='utf-8') as handle:
        return json.load(handle)


def read_bytes(path):
    """按二进制读整个文件（逐字节比较，不做换行转换）。"""
    with open(path, 'rb') as handle:
        return handle.read()


def package_mapping(name):
    """读包内映射数据的一份（生产用的声明）。"""
    return load_json(os.path.join(PACKAGE_DATA_DIR, name))


def sample_files(directory):
    """目录下的 ``.json`` 文件名清单（有序）。"""
    return sorted(
        name for name in os.listdir(directory) if name.endswith('.json')
    )


# -- pytest 夹具 ----------------------------------------------------------

def metric_rules_payload():
    """包内指标规则声明（生产用的那份）。"""
    return package_mapping('metric_rules.json')


def response_rules_payload():
    """包内响应规则声明（形状 + 逐指标定位条件）。"""
    return package_mapping('response_rules.json')


def b_class_rules_payload():
    """82 条 B 类指标的声明。"""
    return [
        rule for rule in metric_rules_payload()['rules']
        if (rule.get('source') or {}).get('source') == B_CLASS
    ]


def non_b_rules_payload():
    """16 条 A/C/D 指标的声明。"""
    return [
        rule for rule in metric_rules_payload()['rules']
        if (rule.get('source') or {}).get('source') in NON_B_CHANNELS
    ]


@pytest.fixture(scope='module')
def expected_table():
    """B 类期望值表（夹具副本）。"""
    return load_json(EXPECTED_VALUES_PATH)


@pytest.fixture(scope='module')
def expected_non_b_table():
    """A/C/D 期望值表（夹具）。"""
    return load_json(EXPECTED_NON_B_PATH)


@pytest.fixture(scope='module')
def probe_nodes():
    """合并形态留档里的每个 master 节点。"""
    return load_json(MERGED_PROBE_PATH)['nodes']


@pytest.fixture(scope='module')
def metric_rules():
    """包内指标规则声明（生产用的那份）。"""
    return metric_rules_payload()


@pytest.fixture(scope='module')
def response_rules():
    """包内响应规则声明（形状 + 逐指标定位条件）。"""
    return response_rules_payload()


@pytest.fixture(scope='module')
def merged_views():
    """合并形态的视图集声明。"""
    views = response_rules_payload().get('merged_views')
    assert views, 'response_rules.json 里没有 merged_views 段'
    return views


@pytest.fixture(scope='module')
def b_class_rules():
    """82 条 B 类指标的声明。"""
    return b_class_rules_payload()


@pytest.fixture(scope='module')
def non_b_rules():
    """16 条 A/C/D 指标的声明。"""
    return non_b_rules_payload()
