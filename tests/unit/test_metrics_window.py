"""A 类（监控指标接口）窗口计算与时间戳切片的单元测试。

全部离线，**不连真实平台**（TPOPS 一律 mock）。

守的是几条「实测踩过的坑」（见 wave2b 执行简报 §3）：

- 查询窗口 **≥ 1 小时** —— 5 分钟窗口覆盖 0/21 个指标（采集周期 3600s 的采不到）；
  窗口不足要**报错**，不静默截断。
- ``end_time`` **回退 5 分钟** —— 数据入库滞后约 5 分钟，否则尾部大量 null。
- ``is_latest_point_only`` 必须传**字符串** ``"false"`` —— 传布尔 true 会让
  约 74% 的指标 ``timestamp=0``（值对、时间丢）。
- **12 个分组码硬编码** —— 平台没有枚举接口；写错分组名返回 200 + 空 series
  且**不报错**，少一个分组就少一批指标且没有任何报错。
- 切片**按档位**（``collection_frequency``）并**跳过 null 点**（null ≠ 0）。
- 时间单位统一**毫秒**。
"""
import logging

import pytest

from tpops_kafka_collector.config import Config
from tpops_kafka_collector.tpops.call_tracking import CatalogCachingClient
from tpops_kafka_collector.tpops.metrics import (
    GROUP_CATALOG_PATH,
    IS_LATEST_POINT_ONLY,
    METRIC_GROUPS,
    MetricsError,
    MetricWindowError,
    QueryWindow,
    build_query_body,
    fetch_metric_catalog,
    parse_metric_points,
    parse_series_points,
    resolve_window,
    resolve_window_from_config,
    slice_points,
    slice_points_by_tier,
)

# ---------------------------------------------------------------------------
# 夹具
# ---------------------------------------------------------------------------

NOW_MS = 1793871351000

#: 平台实际返回的分组码全集（写错任何一个 → 200 + 空 series 且不报错）
EXPECTED_GROUPS = (
    'CPUMEMORY',
    'CONNECTION',
    'SYNCSTAT',
    'PROCESSRESOURCE',
    'NETWORK',
    'TRANSACTION',
    'IOSTORAGE',
    'LOCK',
    'INSTATUS',
    'FORDR',
    'AGENTSTATUS',
    'COREMETRICS',
)


def _metric(name='rds092_gaussv5_commit_counter', frequency=60, timestamps=None,
            series=None, display_name='用户提交事务数/秒'):
    """造一个平台 ``metrics[]`` 条目（结构见开发方案 §4.2.4）。"""
    return {
        'name': name,
        'metric_name': display_name,
        'collection_frequency': frequency,
        'timestamps': list(timestamps or []),
        'series': dict(series or {}),
    }


def _payload(metrics, instance_id='83cd68f2e4cb40b4b7e69240479ffa26in14'):
    return {
        'metric_infos': [
            {
                'instance_id': instance_id,
                'time_interval': 60,
                'metrics': list(metrics),
            }
        ]
    }


class _FakeClient:
    """只实现 ``get`` 的假客户端（记录调用参数）。"""

    def __init__(self, responses=None):
        self._responses = responses or {}
        self.calls = []

    def get(self, path, params=None):
        self.calls.append((path, dict(params or {})))
        name = (params or {}).get('name')
        return self._responses.get(name, {'series': []})


# ---------------------------------------------------------------------------
# 分组码
# ---------------------------------------------------------------------------

def test_metric_groups_contains_the_twelve_expected_codes():
    """12 个分组码齐全 —— 少一个就少一批指标，且平台**不会报错**。"""
    assert set(METRIC_GROUPS) == set(EXPECTED_GROUPS)
    assert len(METRIC_GROUPS) == 12


def test_metric_groups_has_no_duplicate():
    assert len(set(METRIC_GROUPS)) == len(METRIC_GROUPS)


def test_fetch_metric_catalog_requests_every_group():
    """目录查询必须覆盖全部分组码 —— 靠测试保证「不漏」。"""
    client = _FakeClient()
    fetch_metric_catalog(client, 'inst-1')

    assert [params['name'] for _, params in client.calls] == list(EXPECTED_GROUPS)
    assert all(path.endswith('/monitor/metrics/series') for path, _ in client.calls)


def test_fetch_metric_catalog_returns_metrics_per_group():
    client = _FakeClient({
        'CPUMEMORY': {
            'series': [{
                'name': 'CPUMEMORY',
                'metrics': [{'name': 'sys010_mem_usage', 'unit': '%',
                             'alarm_id': None, 'alarm_threshold': None}],
            }]
        },
    })
    catalog = fetch_metric_catalog(client, 'inst-1')

    assert [item['name'] for item in catalog['CPUMEMORY']] == ['sys010_mem_usage']
    assert catalog['CPUMEMORY'][0]['unit'] == '%'


def test_fetch_metric_catalog_warns_on_empty_group(caplog):
    """分组名写错 → 平台返回 200 + 空 series，**必须**记告警日志，不能静默。"""
    client = _FakeClient()

    with caplog.at_level(logging.WARNING, logger='tpops_kafka_collector.tpops.metrics'):
        catalog = fetch_metric_catalog(client, 'inst-1')

    assert 'CPUMEMORY' in caplog.text
    assert '空分组' in caplog.text
    assert catalog['CPUMEMORY'] == ()


# ---------------------------------------------------------------------------
# 窗口计算
# ---------------------------------------------------------------------------

def test_resolve_window_rejects_less_than_one_hour():
    """窗口不足 1 小时 → 报错（不静默截断）。"""
    with pytest.raises(MetricWindowError):
        resolve_window(NOW_MS, 0, 5)

    with pytest.raises(MetricWindowError):
        resolve_window(NOW_MS, 0.5, 5)


def test_resolve_window_accepts_exactly_one_hour():
    window = resolve_window(NOW_MS, 1, 5)

    assert isinstance(window, QueryWindow)
    assert window.end_time - window.start_time == 3600 * 1000


def test_resolve_window_lags_end_time_by_configured_minutes():
    """end_time 回退 METRIC_END_LAG_MINUTES（默认 5 分钟）—— 入库滞后约 5 分钟。"""
    window = resolve_window(NOW_MS, 2, 5)

    assert window.end_time == NOW_MS - 5 * 60 * 1000
    assert window.start_time == window.end_time - 2 * 3600 * 1000


def test_resolve_window_rejects_negative_lag():
    with pytest.raises(MetricWindowError):
        resolve_window(NOW_MS, 2, -1)


def test_resolve_window_from_config_uses_configured_window_and_lag():
    """窗口下限取自 config.metric_window_hours，回退量取自 metric_end_lag_minutes。"""
    config = Config(tpops_user='u', tpops_password='p',
                    metric_window_hours=3, metric_end_lag_minutes=7)

    window = resolve_window_from_config(NOW_MS, config)

    assert window.end_time == NOW_MS - 7 * 60 * 1000
    assert window.start_time == window.end_time - 3 * 3600 * 1000


def test_resolve_window_from_config_rejects_window_below_one_hour():
    config = Config(tpops_user='u', tpops_password='p', metric_window_hours=0)

    with pytest.raises(MetricWindowError):
        resolve_window_from_config(NOW_MS, config)


# ---------------------------------------------------------------------------
# 请求体
# ---------------------------------------------------------------------------

def test_is_latest_point_only_is_the_string_false():
    """必须是**字符串** ``"false"`` —— 传布尔会让约 74% 的指标 timestamp=0。"""
    assert IS_LATEST_POINT_ONLY == 'false'
    assert isinstance(IS_LATEST_POINT_ONLY, str)
    assert not isinstance(IS_LATEST_POINT_ONLY, bool)


def test_build_query_body_uses_string_false_not_boolean():
    window = resolve_window(NOW_MS, 2, 5)

    body = build_query_body(window, 'inst-1', ['rds092_gaussv5_commit_counter'])

    flag = body['is_latest_point_only']
    assert isinstance(flag, str), 'is_latest_point_only 必须是字符串，传布尔会丢时间戳'
    assert not isinstance(flag, bool)
    assert flag == 'false'


def test_build_query_body_carries_window_instance_and_metric_names():
    window = resolve_window(NOW_MS, 2, 5)

    body = build_query_body(window, 'inst-1', ['m1', 'm2'])

    assert body['start_time'] == window.start_time
    assert body['end_time'] == window.end_time
    assert body['instances'] == [{'instance_id': 'inst-1'}]
    assert body['metric_names'] == ['m1', 'm2']


def test_build_query_body_includes_node_ids_only_when_given():
    window = resolve_window(NOW_MS, 2, 5)

    body = build_query_body(window, 'inst-1', ['m1'], node_ids=['n1', 'n2'])

    assert body['instances'] == [{'instance_id': 'inst-1', 'node_ids': ['n1', 'n2']}]


def test_build_query_body_rejects_empty_metric_names():
    window = resolve_window(NOW_MS, 2, 5)

    with pytest.raises(MetricWindowError):
        build_query_body(window, 'inst-1', [])


# ---------------------------------------------------------------------------
# 时间戳切片
# ---------------------------------------------------------------------------

def test_parse_series_points_skips_null_values():
    """null 点必须跳过 —— 缺值不是 0（平台未入库时值为 null）。"""
    metric = _metric(
        timestamps=[1000, 2000, 3000],
        series={'cn_5001': [1.0, None, 3.0]},
    )

    points = parse_series_points(metric)

    assert [p.timestamp for p in points] == [1000, 3000]
    assert [p.value for p in points] == [1.0, 3.0]
    assert points[0].series_key == 'cn_5001'


def test_parse_series_points_keeps_metric_identity():
    metric = _metric(
        name='rds092_gaussv5_commit_counter',
        frequency=60,
        timestamps=[1000],
        series={'cn_5001': [7.0]},
        display_name='用户提交事务数/秒',
    )

    point = parse_series_points(metric)[0]

    assert point.metric_name == 'rds092_gaussv5_commit_counter'
    assert point.metric_display_name == '用户提交事务数/秒'
    assert point.collection_frequency == 60
    assert point.timestamp == 1000


def test_parse_series_points_tolerates_shorter_values_array():
    """timestamps 比 values 长时不越界（平台偶发不齐）。"""
    metric = _metric(timestamps=[1000, 2000], series={'cn_5001': [1.0]})

    points = parse_series_points(metric)

    assert len(points) == 1


def test_parse_series_points_converts_float_timestamps_to_int():
    metric = _metric(timestamps=[1793869271000.0], series={'cn_5001': [1.0]})

    point = parse_series_points(metric)[0]

    assert isinstance(point.timestamp, int)
    assert point.timestamp == 1793869271000


def test_slice_points_keeps_only_points_inside_the_tier_window():
    """按档位切片：60 秒档只取最近 60 秒内的点。"""
    now = NOW_MS
    points = parse_series_points(_metric(
        timestamps=[now - 120_000, now - 30_000, now - 5_000],
        series={'cn_5001': [1.0, 2.0, 3.0]},
    ))

    sliced = slice_points(points, tier_seconds=60, now_ms=now)

    assert [p.timestamp for p in sliced] == [now - 30_000, now - 5_000]


def test_slice_points_for_hourly_tier_keeps_a_full_hour():
    """3600 秒档的取最近 1 小时 —— 短窗口会完全采不到这类指标。"""
    now = NOW_MS
    points = parse_series_points(_metric(
        timestamps=[now - 2 * 3600_000, now - 1800_000],
        series={'cn_5001': [1.0, 2.0]},
    ))

    sliced = slice_points(points, tier_seconds=3600, now_ms=now)

    assert [p.timestamp for p in sliced] == [now - 1800_000]


def test_slice_points_drops_points_in_the_future():
    now = NOW_MS
    points = parse_series_points(_metric(
        timestamps=[now + 60_000],
        series={'cn_5001': [1.0]},
    ))

    assert slice_points(points, tier_seconds=3600, now_ms=now) == ()


def test_slice_points_tolerates_endpoint_skew():
    """端点容差：采集周期边界上晚到几个毫秒的点不该被丢掉。"""
    now = NOW_MS
    points = parse_series_points(_metric(
        timestamps=[now - 60_000 - 500],
        series={'cn_5001': [1.0]},
    ))

    assert slice_points(points, tier_seconds=60, now_ms=now) == ()
    assert len(slice_points(points, tier_seconds=60, now_ms=now, tolerance_ms=1000)) == 1


def test_slice_points_by_tier_uses_each_point_own_frequency():
    """一次响应里不同指标的采集频率不同 → 各按自己的档位切片。"""
    now = NOW_MS
    payload = _payload([
        _metric(name='fast', frequency=60,
                timestamps=[now - 30_000],
                series={'cn_5001': [1.0]}),
        _metric(name='slow', frequency=3600,
                timestamps=[now - 1800_000],
                series={'cn_5001': [2.0]}),
    ])

    sliced = slice_points_by_tier(parse_metric_points(payload), now_ms=now)

    assert [(p.metric_name, p.value) for p in sliced] == [
        ('fast', 1.0),
        ('slow', 2.0),
    ]


def test_slice_points_by_tier_drops_point_outside_its_own_tier():
    """60 秒档的点不在 60 秒窗口内就该被丢掉（旧的不能再投一次）。"""
    now = NOW_MS
    payload = _payload([
        _metric(name='fast', frequency=60,
                timestamps=[now - 300_000],
                series={'cn_5001': [1.0]}),
    ])

    assert slice_points_by_tier(parse_metric_points(payload), now_ms=now) == ()


def test_parse_metric_points_reads_the_nested_platform_structure():
    """平台层级是 metric_infos[] → metrics[]（不是拍平的写法）。"""
    payload = _payload([
        _metric(name='m1', timestamps=[1000], series={'cn_5001': [1.0]}),
        _metric(name='m2', timestamps=[2000], series={'value': [2.0]}),
    ])

    points = parse_metric_points(payload)

    assert [(p.metric_name, p.series_key, p.value) for p in points] == [
        ('m1', 'cn_5001', 1.0),
        ('m2', 'value', 2.0),
    ]


def test_parse_metric_points_returns_empty_tuple_for_empty_response():
    assert parse_metric_points({'metric_infos': []}) == ()
    assert parse_metric_points({}) == ()


# ---------------------------------------------------------------------------
# 目录快照缓存（wave-3 修复轮 R-B）
# ---------------------------------------------------------------------------

def test_catalog_cache_serves_the_second_call_without_a_request():
    """目录是**平台元数据** —— 同一轮里 12 个分组码只拉一次快照。"""
    client = _FakeClient()
    cached = CatalogCachingClient(client, GROUP_CATALOG_PATH, ttl_seconds=3600)

    fetch_metric_catalog(cached, 'inst-1')
    first_calls = len(client.calls)
    fetch_metric_catalog(cached, 'inst-1')

    assert first_calls == len(EXPECTED_GROUPS)
    assert len(client.calls) == first_calls, '命中缓存后不该再发请求'


def test_catalog_cache_expires_after_the_ttl():
    """TTL 到期 → 重新拉（陈旧只影响告警时效，不影响数据）。"""
    clock = {'now': 1000.0}
    client = _FakeClient()
    cached = CatalogCachingClient(
        client, GROUP_CATALOG_PATH, ttl_seconds=60, clock=lambda: clock['now'],
    )

    fetch_metric_catalog(cached, 'inst-1')
    calls_after_first = len(client.calls)
    clock['now'] += 61
    fetch_metric_catalog(cached, 'inst-1')

    assert len(client.calls) == calls_after_first * 2


def test_catalog_cache_does_not_cache_failures():
    """拉取失败**不入缓存** —— 不能留半截数据。"""
    class _Boom(object):
        def __init__(self):
            self.calls = 0

        def get(self, path, params=None):
            self.calls += 1
            raise MetricsError('平台抽风')

    client = _Boom()
    cached = CatalogCachingClient(client, GROUP_CATALOG_PATH, ttl_seconds=3600)

    with pytest.raises(MetricsError):
        fetch_metric_catalog(cached, 'inst-1')
    with pytest.raises(MetricsError):
        fetch_metric_catalog(cached, 'inst-1')

    assert client.calls == 2, '失败不能被缓存住'


def test_catalog_cache_does_not_affect_other_paths():
    """缓存只作用于目录路径 —— 别的接口照常发请求。"""
    client = _FakeClient()
    cached = CatalogCachingClient(client, GROUP_CATALOG_PATH, ttl_seconds=3600)

    cached.get('/rds/v3/monitor/metrics', params={'a': 1})
    cached.get('/rds/v3/monitor/metrics', params={'a': 1})

    assert len(client.calls) == 2
