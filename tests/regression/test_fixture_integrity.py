"""回归夹具的**完整性**守卫。

**这个文件守的是什么**：夹具是「冻结的样本」。它一旦与留档、与生成器脱节，
回归测试就会变成**恒真** —— 用一份过期或被手改过的样本去比期望值，绿了也说明不了什么。

所以这里守四件事：

1. **夹具 = 留档的逐字节副本** —— 98 份逐指标响应 + 合并形态留档，
   内容必须与 ``data/`` 下的「活的留档」逐字节相同（不做重新序列化，
   改了就不是同一份样本了）；
2. **夹具 = 生成器的输出** —— 期望值表必须与生成器现算的一致
   （手改过、或生成器变了没重跑，都会被抓出来）；
3. **期望值表只有一份真相源** —— ``tests/fixtures/expected_values.json`` 与
   ``data/mapping/expected_values.json`` **逐字节相同**。
   wave-0 的那张表被搬进夹具，但它同时还是仓库里的生成物，
   两处并存就有「分叉」的风险 —— 这里用逐字节相等把住（分叉即红）。
4. **夹具里没有凭据** —— 认证 token 一律是占位符 ``<token>``。
"""
import os
import re

import pytest

from regression.conftest import (
    EXPECTED_NON_B_PATH,
    EXPECTED_VALUES_PATH,
    FIXTURES_DIR,
    LIVE_EXPECTED_VALUES_PATH,
    LIVE_MERGED_PROBE_PATH,
    LIVE_METRIC_RESPONSE_DIR,
    MERGED_PROBE_PATH,
    METRIC_RESPONSE_DIR,
    load_json,
    package_mapping,
    read_bytes,
    sample_files,
)

#: 逐指标留档的份数（98 个可提供指标）。
EXPECTED_ARCHIVE_COUNT = 98

#: 索引 manifest 的文件名。
MANIFEST_NAME = '000manifest.json'

#: 认证 token 的占位符（留档里真实 token 不落盘）。
TOKEN_PLACEHOLDER = '<token>'

#: 夹具里**不许**出现的凭据形态（真 token、已知口令字面量、私钥、密码赋值）。
FORBIDDEN_PATTERNS = (
    (re.compile(r'X-Mini-Token":\s*"(?!<token>)[^"]*"'), '出现非占位符的 X-Mini-Token'),
    (re.compile(r'Gauss@'), '出现口令字面量'),
    (re.compile(r'-----BEGIN [A-Z ]*PRIVATE KEY-----'), '出现私钥'),
    (re.compile(r'TPOPS_PASSWORD\s*=\s*\S'), '出现密码赋值'),
    (re.compile(r'"?(sasl\.jaas\.config|truststore_password|keystore_password)"?\s*[:=]'),
     '出现中间件密码字段'),
)


def _fixture_files():
    """夹具目录下的全部文件（相对路径 → 绝对路径）。"""
    found = {}
    for root, _dirs, names in os.walk(FIXTURES_DIR):
        for name in names:
            path = os.path.join(root, name)
            found[os.path.relpath(path, FIXTURES_DIR)] = path
    return found


# -- 夹具结构 -------------------------------------------------------------

def test_fixture_layout_is_complete():
    """四类夹具一个都不能少（少一样就有一类指标没有回归覆盖）。"""
    assert os.path.isdir(METRIC_RESPONSE_DIR)
    assert os.path.isfile(MERGED_PROBE_PATH)
    assert os.path.isfile(EXPECTED_VALUES_PATH)
    assert os.path.isfile(EXPECTED_NON_B_PATH)


def test_archive_count_is_98_plus_manifest():
    """98 份逐指标留档 + 1 份索引 manifest。"""
    names = sample_files(METRIC_RESPONSE_DIR)
    assert len(names) == EXPECTED_ARCHIVE_COUNT + 1
    assert MANIFEST_NAME in names
    archives = [name for name in names if name != MANIFEST_NAME]
    assert len(archives) == EXPECTED_ARCHIVE_COUNT


def test_manifest_indexes_every_archive():
    """manifest 里的每个文件都真实存在（索引与样本对得上）。"""
    manifest = load_json(os.path.join(METRIC_RESPONSE_DIR, MANIFEST_NAME))
    metrics = manifest['metrics']
    assert len(metrics) == EXPECTED_ARCHIVE_COUNT
    for item in metrics:
        path = os.path.join(METRIC_RESPONSE_DIR, item['file'])
        assert os.path.isfile(path), item['file']


# -- 夹具 = 留档的逐字节副本 ----------------------------------------------

@pytest.mark.parametrize('name', sorted(sample_files(LIVE_METRIC_RESPONSE_DIR)))
def test_archive_fixture_is_byte_identical_to_the_capture(name):
    """每份留档夹具与「活的留档」**逐字节相同**。

    逐字节（而不是「解析后相等」）—— 重新序列化会改缩进、键序、浮点表示，
    改完就不是同一份样本了。
    """
    live = os.path.join(LIVE_METRIC_RESPONSE_DIR, name)
    frozen = os.path.join(METRIC_RESPONSE_DIR, name)
    assert os.path.isfile(frozen), f'夹具缺少 {name}'
    assert read_bytes(frozen) == read_bytes(live), (
        f'{name} 与留档不一致 —— 重新生成夹具（tools/build_regression_fixtures.py）'
    )


def test_no_extra_files_in_the_archive_fixture():
    """夹具里不能有留档之外的文件（多出来的样本没有出处）。"""
    live = set(sample_files(LIVE_METRIC_RESPONSE_DIR))
    frozen = set(sample_files(METRIC_RESPONSE_DIR))
    assert frozen == live, (
        f'多出：{sorted(frozen - live)} / 缺少：{sorted(live - frozen)}'
    )


def test_merged_probe_fixture_is_byte_identical_to_the_capture():
    """合并形态留档夹具逐字节相同。"""
    assert read_bytes(MERGED_PROBE_PATH) == read_bytes(LIVE_MERGED_PROBE_PATH)


# -- 期望值表：只有一份真相源 --------------------------------------------

def test_expected_values_fixture_is_byte_identical_to_the_mapping_copy():
    """夹具里的期望值表与 ``data/mapping/`` 那张**逐字节相同**。

    wave-0 的那张表被搬进夹具，但它同时还是仓库里的生成物 ——
    两处并存就要用逐字节相等把住，否则就是两份会分叉的真相源。
    """
    assert read_bytes(EXPECTED_VALUES_PATH) == read_bytes(
        LIVE_EXPECTED_VALUES_PATH
    ), '夹具里的期望值表与 data/mapping/ 那张分叉了'


def test_expected_values_fixture_matches_the_generator():
    """期望值表必须是生成器现算的结果（不是手抄、不是手改的）。"""
    import build_expected_values

    assert load_json(EXPECTED_VALUES_PATH) == build_expected_values.build(), (
        '期望值表与生成器输出不一致 —— 跑 tools/build_expected_values.py 再重建夹具'
    )


def test_non_b_expected_values_fixture_matches_the_generator():
    """A/C/D 期望值表也必须是生成器现算的结果。"""
    import build_regression_fixtures

    assert load_json(EXPECTED_NON_B_PATH) == build_regression_fixtures.build_non_b_table()


def test_expected_values_table_covers_the_82_b_class_metrics(expected_table):
    """B 类期望值表覆盖 82 条，一条不落。"""
    expected = expected_table['expected']
    assert len(expected) == 82


def test_non_b_expected_values_table_covers_the_16_other_metrics(
    expected_non_b_table
):
    """A/C/D 期望值表覆盖 16 条（11 监控 + 4 诊断 + 1 实例）。"""
    expected = expected_non_b_table['expected']
    assert len(expected) == 16
    channels = {}
    for entry in expected.values():
        channels[entry['channel']] = channels.get(entry['channel'], 0) + 1
    assert channels == {'monitor_metrics': 11, 'diagnostic': 4, 'instance': 1}


# -- 夹具里没有凭据 -------------------------------------------------------

def test_no_credentials_in_the_fixtures():
    """夹具里不许出现真 token / 口令 / 私钥。

    留档本身是干净的（认证 token 一律写占位符 ``<token>``），
    这一条守住「别把凭据带进夹具」。
    """
    problems = []
    for relative, path in sorted(_fixture_files().items()):
        if not relative.endswith('.json'):
            continue
        text = read_bytes(path).decode('utf-8', 'replace')
        for pattern, reason in FORBIDDEN_PATTERNS:
            if pattern.search(text):
                problems.append(f'{relative}：{reason}')
    assert not problems, f'夹具里出现凭据：{problems}'


def test_mini_token_is_the_placeholder_in_every_archive():
    """每份留档里的 ``X-Mini-Token`` 都是占位符。"""
    pattern = re.compile(r'X-Mini-Token":\s*"([^"]*)"')
    seen = set()
    for name in sample_files(METRIC_RESPONSE_DIR):
        text = read_bytes(os.path.join(METRIC_RESPONSE_DIR, name)).decode('utf-8')
        for value in pattern.findall(text):
            seen.add(value)
    assert seen <= {TOKEN_PLACEHOLDER}, f'留档里的 token 不是占位符：{sorted(seen)}'


# -- 夹具里的规则声明与生产用的那一份是同一份 ----------------------------

def test_fixture_expected_table_uses_the_package_rules(expected_table):
    """期望值表里的规则类型与**包内**（生产用的）规则声明对得上。

    期望值表由仓库根的规则声明推导，生产跑的是包内副本 ——
    两者必须一致（``tests/unit/test_mapping_data_sync.py`` 守副本一致，
    这里守「期望值表的规则类型与包内声明一致」）。
    """
    rules = package_mapping('metric_rules.json')
    by_code = {}
    for rule in rules['rules']:
        by_code[rule['metric_code']] = rule
    for code, entry in expected_table['expected'].items():
        assert code in by_code, code
        assert entry['value_rule_type'] == by_code[code]['value_rule']['type'], code
