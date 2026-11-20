"""DTO 与异常体系的单元测试（任务 1.2）。

三组关注点：

1. **DTO 不可变 + 可哈希** —— 维度以**排序后的元组**存储，
   所以 ``MetricPoint`` 能进集合、能当字典键（规格「结构化数据不可变」）。
2. **幂等键与 ``tpops_kafka_collector.ids`` 同源** —— 这里断言的是
   「DTO 上的键 == ``ids`` 模块算出来的键」，而不是把键的拼法抄一遍：
   抄一遍就等于有了第二份真相源，改一处会漏另一处。
3. **异常体系与可重试判定表** —— 含一条**锁住 MRO 判定**的守卫：
   ``requests`` 实际抛出的是 ``ReadTimeout`` / ``ConnectTimeout``
   （``Timeout`` / ``ConnectionError`` 的子类），按类名精确相等会判反。
"""
import dataclasses

import pytest
import requests

from tpops_kafka_collector.errors import (
    AuthError,
    DecodeError,
    MappingError,
    ProduceError,
    ProtocolError,
    TpopsError,
    TransientError,
    is_retryable,
)
from tpops_kafka_collector.ids import (
    alarm_idempotency_key,
    metric_idempotency_key,
)
from tpops_kafka_collector.models import AlarmDetail, AlarmRecord, MetricPoint


def make_metric_point(**overrides):
    """造一个字段齐全的指标点，测试里只覆盖关心的字段。"""
    kwargs = dict(
        customer_metric='gaussdb.Total_pct',
        customer_metric_name='事务总数',
        customer_category='数据库性能',
        dimensions=(('DATNAME', 'db1'),),
        value=12.5,
        value_type='浮点',
        unit='%',
        timestamp=1793758083000,
        collect_time=1793758090000,
        source='monitor_metrics',
        instance_id='inst-1',
        instance_name='gauss-6374',
    )
    kwargs.update(overrides)
    return MetricPoint(**kwargs)


def make_alarm_detail(**overrides):
    kwargs = dict(
        node_id='node-1',
        host_ip='172.16.103.128',
        description='节点 NTP 服务状态异常。',
        cause='',
        impaction='无法正确进行时间同步。',
        possible_causes='NTP 客户端与服务端的网络连接有问题。',
        processing_steps='登录告警对象节点排查。',
        alarm_detail_raise_count=5,
    )
    kwargs.update(overrides)
    return AlarmDetail(**kwargs)


def make_alarm_record(**overrides):
    kwargs = dict(
        alarm_id='2007010001',
        alarm_name='节点NTP状态异常',
        alarm_type='event',
        alarm_category='节点状态',
        severity='major',
        status='activated',
        instance_id='inst-1',
        instance_name='gauss-6374',
        first_alarm_time=1793758083000,
        last_alarm_time=1793771156000,
        alarm_duration='4小时5分钟',
        alarm_items_sum=1,
        alarm_detail_raise_count=5,
        details=(make_alarm_detail(),),
    )
    kwargs.update(overrides)
    return AlarmRecord(**kwargs)


# --------------------------------------------------------------------------
# MetricPoint
# --------------------------------------------------------------------------

def test_metric_point_is_frozen():
    point = make_metric_point()
    with pytest.raises(dataclasses.FrozenInstanceError):
        point.value = 1.0


def test_metric_point_dimensions_normalized_to_sorted_tuple():
    point = make_metric_point(dimensions={'B': '2', 'A': '1'})
    assert point.dimensions == (('A', '1'), ('B', '2'))
    assert isinstance(point.dimensions, tuple)


def test_metric_point_dimensions_accept_pair_sequence():
    point = make_metric_point(dimensions=[('B', '2'), ('A', '1')])
    assert point.dimensions == (('A', '1'), ('B', '2'))


def test_metric_point_is_hashable_and_usable_as_dict_key():
    point = make_metric_point()
    assert hash(point) == hash(make_metric_point())
    assert {point: 'x'}[make_metric_point()] == 'x'
    assert len({point, make_metric_point()}) == 1


def test_metric_point_dimension_map_property():
    point = make_metric_point(dimensions={'A': '1', 'B': '2'})
    assert point.dimension_map == {'A': '1', 'B': '2'}
    # dimension_map 是每次新建的 dict，改它不影响 DTO 本身
    point.dimension_map['A'] = 'changed'
    assert point.dimensions == (('A', '1'), ('B', '2'))


def test_metric_point_idempotency_key_matches_ids_module():
    point = make_metric_point(dimensions={'DATNAME': 'db1'})
    assert point.idempotency_key == metric_idempotency_key(
        'inst-1', 'gaussdb.Total_pct', {'DATNAME': 'db1'}, 1793758083000)


def test_metric_point_idempotency_key_ignores_dimension_insertion_order():
    first = make_metric_point(dimensions={'A': '1', 'B': '2'})
    second = make_metric_point(dimensions={'B': '2', 'A': '1'})
    assert first.idempotency_key == second.idempotency_key


def test_metric_point_idempotency_key_differs_by_dimension():
    first = make_metric_point(dimensions={'DATNAME': 'db1'})
    second = make_metric_point(dimensions={'DATNAME': 'db2'})
    assert first.idempotency_key != second.idempotency_key


def test_metric_point_idempotency_key_differs_by_timestamp():
    first = make_metric_point()
    second = make_metric_point(timestamp=1793758083001)
    assert first.idempotency_key != second.idempotency_key


# --------------------------------------------------------------------------
# AlarmDetail / AlarmRecord
# --------------------------------------------------------------------------

def test_alarm_detail_is_frozen():
    detail = make_alarm_detail()
    with pytest.raises(dataclasses.FrozenInstanceError):
        detail.description = '改了'


def test_alarm_detail_is_hashable():
    assert hash(make_alarm_detail()) == hash(make_alarm_detail())


def test_alarm_record_is_frozen():
    record = make_alarm_record()
    with pytest.raises(dataclasses.FrozenInstanceError):
        record.status = 'recovered'


def test_alarm_record_details_is_tuple_of_alarm_details():
    record = make_alarm_record(details=[make_alarm_detail()])
    assert isinstance(record.details, tuple)
    assert all(isinstance(detail, AlarmDetail) for detail in record.details)


def test_alarm_record_is_hashable():
    assert hash(make_alarm_record()) == hash(make_alarm_record())


def test_alarm_record_idempotency_key_matches_ids_module():
    record = make_alarm_record()
    assert record.idempotency_key == alarm_idempotency_key(
        '2007010001', 'inst-1', 1793758083000, 'activated')


def test_alarm_record_idempotency_key_differs_between_activated_and_recovered():
    activated = make_alarm_record(status='activated')
    recovered = make_alarm_record(status='recovered')
    assert activated.idempotency_key != recovered.idempotency_key


def test_alarm_record_idempotency_key_stable_for_repeated_collection():
    first = make_alarm_record()
    second = make_alarm_record(last_alarm_time=1793779999000)
    assert first.idempotency_key == second.idempotency_key


def test_alarm_record_idempotency_key_ignores_node_id():
    without_node = make_alarm_record(node_id=None)
    with_node = make_alarm_record(node_id='node-9')
    assert without_node.idempotency_key == with_node.idempotency_key


# --------------------------------------------------------------------------
# 异常体系
# --------------------------------------------------------------------------

@pytest.mark.parametrize('exc_cls', [
    AuthError, TransientError, ProtocolError, MappingError, ProduceError,
])
def test_error_subclasses_derive_from_tpops_error(exc_cls):
    assert issubclass(exc_cls, TpopsError)
    assert issubclass(exc_cls, RuntimeError)


def test_decode_error_belongs_to_mapping_error():
    assert issubclass(DecodeError, MappingError)
    assert not is_retryable(DecodeError('坏 JSON'))


@pytest.mark.parametrize('exc', [
    AuthError('401'),
    ProtocolError('缺字段'),
    MappingError('对不上'),
    DecodeError('坏 JSON'),
])
def test_non_retryable_errors(exc):
    assert not is_retryable(exc)


@pytest.mark.parametrize('exc', [
    TransientError('500'),
    ProduceError('kafka 挂了'),
])
def test_retryable_errors(exc):
    assert is_retryable(exc)


def test_requests_timeout_subclasses_are_retryable():
    """锁住「走 MRO 基类链」的判定 —— 不能退回按类名精确相等。

    ``requests`` 实际抛出的是 ``ReadTimeout`` / ``ConnectTimeout``，
    它们是 ``Timeout`` / ``ConnectionError`` 的**子类**；
    精确相等会把这两个最常见的瞬时故障判成不可重试（直接中止本轮）。
    """
    assert is_retryable(requests.exceptions.ReadTimeout('读超时'))
    assert is_retryable(requests.exceptions.ConnectTimeout('连超时'))
    assert is_retryable(requests.exceptions.Timeout('超时'))
    assert is_retryable(requests.exceptions.ConnectionError('连不上'))


def test_unrelated_exception_is_not_retryable():
    assert not is_retryable(ValueError('坏了'))
    assert not is_retryable(requests.exceptions.InvalidURL('URL 不对'))
