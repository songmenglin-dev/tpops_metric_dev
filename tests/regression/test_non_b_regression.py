"""A/C/D 三条通道 16 个指标的**值相等**回归。

- **A 类（监控指标接口，11 条）**：按组件展开的时序 → 每个组件取一个点；
- **C 类（诊断优化接口，4 条）**：慢 SQL / Top SQL 集合的条数、前缀分类计数、Top N；
- **D 类（实例接口，1 条）**：实例元数据里的一个字段。

**判据**：留档回放 → 解码（``mapping/decode.py``）→ 规则引擎
（``mapping/rules.py``）→ 与期望值表**逐条值相等**。

**期望值从哪来**：``tests/fixtures/expected_values_non_b.json``，
由 ``tools/expected_deriver_non_b.py`` 从**留档原始响应**独立推出
（**不经过规则引擎** —— 引擎是被测对象，借它的输出当期望值就是自证）。
B 类那 82 条的期望值表是 wave-0 的独立推导器 ``tools/expected_deriver.py``
产出的，与本文件是**两套独立的推导实现**。
"""
import json
import os

import pytest

from regression.conftest import METRIC_RESPONSE_DIR, load_json

from tpops_kafka_collector.mapping import evaluate
from tpops_kafka_collector.mapping.decode import decode
from tpops_kafka_collector.mapping.table import MetricRule


def _canon(value):
    """值的规范化形态（比较用）。

    - 浮点的整数值归一成整数（``10.0`` 与 ``10`` 是同一个值）；
    - 浮点保留 6 位小数（避免尾数噪声）；
    - 列表 / 字典递归规范化。
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else round(value, 6)
    if isinstance(value, int):
        return value
    if isinstance(value, list):
        return [_canon(item) for item in value]
    if isinstance(value, dict):
        return dict((key, _canon(item)) for key, item in value.items())
    return value


def _normalized(pairs):
    """``[(维度, 值)]`` → 可比较的有序列表（与取值顺序无关）。"""
    return sorted(
        json.dumps([_canon(dims), _canon(value)], ensure_ascii=False, sort_keys=True)
        for dims, value in pairs
    )


def _pairs(results):
    """引擎输出 → ``[(维度, 值)]``（时间戳不参与比较）。"""
    return [(dims, value) for dims, value, _timestamp in results]


def _archive_steps(sample_file):
    """留档夹具的调用步骤列表。"""
    path = os.path.join(METRIC_RESPONSE_DIR, sample_file)
    return load_json(path)


def _monitor_body(steps):
    """A 类：取 ``is_latest_point_only=false`` 那一步的响应体（生产用的就是它）。

    留档里 A 类有两步：``true``（只要最新点，1 个点）与 ``false``（带时间戳的时序）。
    生产采集器固定用 ``false``（``tpops/metrics.py``），所以回放也用 ``false``。
    """
    matched = []
    for step in steps:
        body = (step.get('request') or {}).get('body') or {}
        flag = str(body.get('is_latest_point_only')).lower()
        if flag == 'false':
            matched.append(step)
    assert len(matched) == 1, '留档里 is_latest_point_only=false 的调用不是 1 个'
    return matched[0]['response_body']


def _collection_body(steps, collection):
    """C/D 类：取响应体里带该集合的那一步。"""
    for step in steps:
        if collection in (step.get('response_body') or {}):
            return step['response_body']
    raise AssertionError(f'留档里没有集合 {collection} 的响应')


def _non_b_rules():
    """16 条 A/C/D 指标的声明（**包内**那份 —— 生产用的声明）。"""
    from regression.conftest import NON_B_CHANNELS, package_mapping

    payload = package_mapping('metric_rules.json')
    return [
        rule for rule in payload['rules']
        if (rule.get('source') or {}).get('source') in NON_B_CHANNELS
    ]


#: 16 条 A/C/D 指标的编码（有序）—— 收集期就要定下来（``parametrize`` 不能用夹具）。
NON_B_CODES = sorted(rule['metric_code'] for rule in _non_b_rules())


@pytest.fixture(scope='module')
def response_by_code(response_rules):
    """指标编码 → 响应规则（形状 + 定位条件）。"""
    return dict((rule['metric_code'], rule) for rule in response_rules['rules'])


# -- 覆盖完整性 ------------------------------------------------------------

def test_non_b_rule_count_is_16(non_b_rules):
    """A/C/D 就是 16 条（11 监控 + 4 诊断 + 1 实例）。"""
    assert len(non_b_rules) == 16


def test_expected_table_covers_every_non_b_metric(expected_non_b_table):
    """期望值表**一条不落**地覆盖 16 条。"""
    assert sorted(expected_non_b_table['expected']) == NON_B_CODES


# -- 值相等：16 条逐条断言 -----------------------------------------------

@pytest.mark.parametrize('metric_code', NON_B_CODES)
def test_value_equals_expected(
    metric_code, expected_non_b_table, response_by_code, non_b_rules,
    response_rules
):
    """留档回放 → 规则引擎 → 与期望值表**逐条值相等**。"""
    entry = expected_non_b_table['expected'][metric_code]
    rule_payload = next(
        r for r in non_b_rules if r['metric_code'] == metric_code
    )
    response = response_by_code[metric_code]
    shape = response_rules['shapes'][response['shape']]
    locate = response.get('locate') or {}
    now_ms = expected_non_b_table['now_ms']

    steps = _archive_steps(entry['sample_file'])
    body = _body_of(steps, entry['channel'], locate, shape)

    ctx = decode(body, shape, locate, now_ms)
    results = evaluate(MetricRule.from_dict(rule_payload), ctx)
    assert _normalized(_pairs(results)) == _normalized(
        (item['dimensions'], item['value']) for item in entry['expected']
    ), metric_code


def _body_of(steps, channel, locate, shape):
    """按通道取出该回放的响应体。"""
    if channel == 'monitor_metrics':
        return _monitor_body(steps)
    collection = (locate or {}).get('collection') or (shape or {}).get('collection')
    return _collection_body(steps, collection)
