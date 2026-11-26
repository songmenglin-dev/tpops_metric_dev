# -*- coding: utf-8 -*-
"""告警的统一指标名。

平台 257 条告警定义里，只有 3 条挂靠了客户指标（都指向 ``gaussdb.Tps``）；
其余 254 条事件类告警原先各自造一个 ``gaussdb.alarm.<alarm_id>`` 指标名
（其中还有 ``gaussdb.alarm.>=75%`` 这种拿阈值当 ID 的）——
客户侧要建 254 条台账条目。

本波次把**未挂客户指标**的告警统一成**同一个** ``metricCode`` / ``metricName``：

- 统一值**表驱动** —— 写在 ``alarm_rules.json`` 的 ``event_metric`` 段里，
  改口径只改数据文件，不改代码；
- 挂靠了客户指标的告警**保持不变**；
- 告警的种类由 ``alarmId`` / ``alarmName`` / ``alarmContent`` 承载，
  不靠 ``metricCode`` 区分。
"""
from __future__ import absolute_import

import io
import json
import os

import pytest

from conftest import REPO_ROOT

from tpops_kafka_collector.mapping import loader  # noqa: E402
from tpops_kafka_collector.mapping.table import (  # noqa: E402
    build_event_alarm_definition,
)

ALARM_RULES_PATH = os.path.join(
    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data', 'alarm_rules.json')

#: 挂靠客户指标的告警条数（都指向 gaussdb.Tps）。
LINKED_COUNT = 3


def _load(path):
    with io.open(path, encoding='utf-8') as handle:
        return json.load(handle)


@pytest.fixture(scope='module')
def payload():
    return _load(ALARM_RULES_PATH)


@pytest.fixture(scope='module')
def definitions(payload):
    return payload['definitions']


@pytest.fixture(scope='module')
def event_metric(payload):
    return payload['event_metric']


# -- 统一值本身 -----------------------------------------------------------
def test_the_unified_value_is_declared_in_the_data_file(event_metric):
    assert event_metric['metric_code']
    assert event_metric['metric_name']


def test_event_metric_is_a_single_fixed_value(event_metric):
    assert event_metric['metric_code'] == 'gaussdb.alarm'
    assert event_metric['metric_name'] == '数据库告警'


# -- 事件类告警 ------------------------------------------------------------
def test_all_event_alarms_share_the_same_metric_code(definitions, event_metric):
    unlinked = [
        entry for entry in definitions.values()
        if not entry.get('linked_customer_metric')
    ]
    assert len(unlinked) == 254
    codes = set(entry['metric_code'] for entry in unlinked)
    assert codes == {event_metric['metric_code']}


def test_all_event_alarms_share_the_same_metric_name(definitions, event_metric):
    unlinked = [
        entry for entry in definitions.values()
        if not entry.get('linked_customer_metric')
    ]
    names = set(entry['metric_name'] for entry in unlinked)
    assert names == {event_metric['metric_name']}


def test_event_alarm_ids_are_all_different(definitions):
    """告警种类靠 ``alarmId`` 承载 —— 254 条事件类告警的 ID 各不相同。"""
    unlinked = [
        alarm_id for alarm_id, entry in definitions.items()
        if not entry.get('linked_customer_metric')
    ]
    assert len(unlinked) == 254
    assert len(set(unlinked)) == len(unlinked)


def test_alarm_names_are_untouched_by_the_unification(definitions):
    """统一的是**指标名**；告警名（种类）不能被统一值覆盖。

    平台目录里有 13 行「告警名称」列填的是等级词（``紧急`` / ``重要`` / ``次要``），
    另有 2 组同名不同 ID 的告警 —— 那是**平台侧**的既有数据（见
    ``test_alarm_names_match_the_platform_catalog``），统一指标名没有制造它们。
    """
    unlinked = [
        entry for entry in definitions.values()
        if not entry.get('linked_customer_metric')
    ]
    assert unlinked
    for entry in unlinked:
        assert entry['alarm_name'], entry['alarm_id']
        assert entry['alarm_name'] != '数据库告警', entry['alarm_id']


def test_alarm_names_match_the_platform_catalog(definitions):
    """告警名逐条对得上平台目录 —— 统一指标名没有动过告警名。"""
    catalog = {}
    for filename in ('catalog_metric.json', 'catalog_event.json'):
        payload = _load(os.path.join(REPO_ROOT, 'data', 'alarm', filename))
        for item in payload.get('items') or ():
            alarm_id = str(item.get('告警ID') or '').strip()
            if alarm_id and alarm_id not in catalog:
                catalog[alarm_id] = item.get('告警名称') or ''
    for alarm_id, entry in definitions.items():
        assert entry['alarm_name'] == catalog.get(alarm_id), alarm_id


def test_no_event_alarm_uses_a_per_alarm_metric_code(definitions):
    codes = [
        entry['metric_code'] for entry in definitions.values()
        if not entry.get('linked_customer_metric')
    ]
    assert not [code for code in codes if code.startswith('gaussdb.alarm.')]


# -- 挂靠客户指标的告警保持不变 -------------------------------------------
def test_linked_alarms_keep_their_customer_metric(definitions):
    linked = [
        entry for entry in definitions.values()
        if entry.get('linked_customer_metric')
    ]
    assert len(linked) == LINKED_COUNT
    for entry in linked:
        assert entry['metric_code'] == entry['linked_customer_metric']
        assert entry['metric_code'] == 'gaussdb.Tps'


def test_linked_alarms_keep_their_own_metric_name(definitions):
    linked = [
        entry for entry in definitions.values()
        if entry.get('linked_customer_metric')
    ]
    for entry in linked:
        assert entry['metric_name'] != '数据库告警'
        assert entry['metric_name']


# -- 启动期校验不受影响 ----------------------------------------------------
def test_the_real_catalog_still_passes_validation():
    bundle = loader.load_mapping()
    assert len(bundle.alarm_rules['definitions']) == 257


def test_alarm_metric_codes_may_repeat_without_failing_validation():
    """告警定义靠 ``alarm_id`` 定位 —— 254 条同码**不得**触发唯一性校验失败。"""
    bundle = loader.load_mapping()
    codes = [
        entry['metric_code'] for entry in bundle.alarm_rules['definitions'].values()
    ]
    assert len(codes) > len(set(codes)), '本用例的前提是告警 metric_code 有重复'


# -- 运行期兜底也用统一值 --------------------------------------------------
def test_the_runtime_fallback_uses_the_mapped_unified_value(event_metric):
    """平台新增告警（定义缺失）时，运行期兜底也用统一指标名，不是自造名。"""
    definition = build_event_alarm_definition(
        'new-alarm-1', '新告警', event_metric)
    assert definition.metric_code == event_metric['metric_code']
    assert definition.metric_name == event_metric['metric_name']


def _generator(tmp_path, monkeypatch):
    """把生成器的输出路径指向临时目录（不碰真实映射数据）。"""
    import build_itscada_mapping as gen  # noqa: WPS433（测试内延迟导入）

    target = tmp_path / 'alarm_rules.json'
    monkeypatch.setattr(gen, 'ALARM_RULES_OUT', str(target))
    return gen, target


def test_generator_seeds_the_default_when_the_file_is_absent(tmp_path, monkeypatch):
    """首次生成（文件不存在）→ 用默认值播种，不报错。"""
    gen, target = _generator(tmp_path, monkeypatch)
    assert not target.exists()
    assert gen._load_event_metric() == gen.DEFAULT_EVENT_METRIC


def test_generator_keeps_the_value_from_the_data_file(tmp_path, monkeypatch):
    """数据文件里改过 → 生成器原样读回，不被默认值覆盖。"""
    gen, target = _generator(tmp_path, monkeypatch)
    target.write_text(
        json.dumps({'event_metric': {'metric_code': 'my.alarm',
                                     'metric_name': '我的告警'}}),
        encoding='utf-8',
    )
    assert gen._load_event_metric() == {'metric_code': 'my.alarm',
                                        'metric_name': '我的告警'}


def test_generator_refuses_to_silently_fall_back_when_the_segment_is_gone(
        tmp_path, monkeypatch):
    """文件存在但 event_metric 段没了 → **报错**，不静默回落默认值。

    静默回落会把运维改过的口径悄悄改回去，现场看不到任何提示。
    """
    gen, target = _generator(tmp_path, monkeypatch)
    target.write_text(json.dumps({'definitions': {}}), encoding='utf-8')
    with pytest.raises(RuntimeError) as caught:
        gen._load_event_metric()
    assert 'event_metric' in str(caught.value)


def test_generator_refuses_to_silently_fall_back_on_a_broken_file(
        tmp_path, monkeypatch):
    """文件存在但不是合法 JSON → **报错**，不静默回落默认值。"""
    gen, target = _generator(tmp_path, monkeypatch)
    target.write_text('{ 这不是 JSON', encoding='utf-8')
    with pytest.raises(RuntimeError) as caught:
        gen._load_event_metric()
    assert 'JSON' in str(caught.value)
