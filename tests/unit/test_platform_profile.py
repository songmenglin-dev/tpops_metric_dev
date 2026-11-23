"""平台画像的**发现、固化与核对**守卫测试（任务 3.3）。

守三件事：

1. **发现规则 = 选择所有实例** —— 平台上有几个就记几个（不挑、不筛、
   不取第一个、不按 ``mode`` 筛）；
2. **固化后不再自动重新发现** —— 固化文件在，就以文件为准；
3. **核对不一致要出声** —— 画像里的实例没了 → ERROR；平台新增了 → WARNING
   （**但不自动加**）。
"""
import json
import os

import pytest

from tpops_kafka_collector import watermark as watermark_module
from tpops_kafka_collector.platform_profile import (
    PROFILE_FILENAME,
    PlatformProfile,
    ProfileError,
)


class FakeClient:
    """只实现 ``get`` 的假 TPOPS 客户端。"""

    def __init__(self, instances=(), nodes=None):
        self.instances = list(instances)
        self.nodes = dict(nodes or {})
        self.calls = []

    def get(self, path, params=None):
        self.calls.append(path)
        if 'key-view-execute-node' in path:
            instance_id = path.split('/instances/')[-1].split('/')[0]
            return {'nodes': list(self.nodes.get(instance_id, ()))}
        if path.endswith('/instances'):
            return {
                'instances': list(self.instances),
                'total_count': len(self.instances),
            }
        return {}


class FakeNodeRegistry:
    """只实现 ``nodes`` 的假 master 节点清单（**故意不去重**）。"""

    def __init__(self, nodes):
        self._nodes = dict(nodes)
        self.calls = []

    def nodes(self, instance_id):
        self.calls.append(instance_id)
        return tuple(self._nodes.get(instance_id, ()))


def _instance(instance_id, name='gauss', mode='Combined', engine='gaussdbv5'):
    return {
        'id': instance_id,
        'name': name,
        'mode': mode,
        'engine_name': engine,
        'engine_version': 'V2.0-10.0.0',
    }


def _master_nodes(count, prefix='node'):
    """构造 count 条 master 节点（**含重复 node_id**）。"""
    nodes = []
    for index in range(count):
        nodes.append({
            'node_id': '%s-%d' % (prefix, index),
            'role': 'master',
            'component_id': 'cn-%d' % index,
        })
    # 实测：分布式实例 role=master 有 6 条但只有 3 个唯一 node_id
    for index in range(count):
        nodes.append({
            'node_id': '%s-%d' % (prefix, index),
            'role': 'master',
            'component_id': 'dn-%d' % index,
            'type': 'DN',
        })
    return nodes


def _profile_path(tmp_path):
    return os.path.join(str(tmp_path), PROFILE_FILENAME)


# -- 发现 ------------------------------------------------------------------

def test_single_instance_is_recorded(tmp_path):
    """实例列表只有一个 → 画像里记它。"""
    client = FakeClient([_instance('inst-1', 'gauss-6374')])
    registry = FakeNodeRegistry({'inst-1': _master_nodes(3)})

    profile = PlatformProfile.load_or_discover(
        _profile_path(tmp_path), client, node_registry=registry,
    )

    assert len(profile.instances) == 1
    assert profile.instances[0].instance_id == 'inst-1'
    assert profile.instances[0].instance_name == 'gauss-6374'
    assert profile.instances[0].master_count == 3


def test_all_instances_are_recorded_in_platform_order(tmp_path):
    """实例列表有多个 → **全部都记**（条数与顺序都对），不按 mode 筛。"""
    client = FakeClient([
        _instance('inst-1', 'gauss-6374', mode='Combined'),
        _instance('inst-2', 'gauss-75a2', mode='Ha'),
        _instance('inst-3', 'gauss-9', mode='Ha'),
    ])
    registry = FakeNodeRegistry({
        'inst-1': _master_nodes(3),
        'inst-2': _master_nodes(1),
        'inst-3': _master_nodes(1),
    })

    profile = PlatformProfile.load_or_discover(
        _profile_path(tmp_path), client, node_registry=registry,
    )

    assert len(profile.instances) == 3
    assert [item.instance_id for item in profile.instances] == [
        'inst-1', 'inst-2', 'inst-3',
    ]
    assert [item.master_count for item in profile.instances] == [3, 1, 1]


def test_empty_instance_list_raises_clear_error(tmp_path):
    """实例列表为空 → 抛明确异常。"""
    client = FakeClient([])
    with pytest.raises(ProfileError) as excinfo:
        PlatformProfile.load_or_discover(
            _profile_path(tmp_path), client, node_registry=FakeNodeRegistry({}),
        )
    assert '实例列表为空' in str(excinfo.value)
    assert not os.path.exists(_profile_path(tmp_path))


def test_master_count_counts_unique_node_ids(tmp_path):
    """master 节点数按**唯一 node_id** 计数，不数条目。"""
    client = FakeClient([_instance('inst-1')])
    registry = FakeNodeRegistry({'inst-1': _master_nodes(3)})  # 6 条 / 3 个唯一

    profile = PlatformProfile.load_or_discover(
        _profile_path(tmp_path), client, node_registry=registry,
    )

    assert profile.instances[0].master_count == 3


# -- 固化 ------------------------------------------------------------------

def test_existing_profile_file_is_not_rediscovered(tmp_path):
    """固化文件已存在 → **不重新发现**（平台列表接口一次都没被调用）。"""
    path = _profile_path(tmp_path)
    profile = PlatformProfile(
        instances=(), discovered_at='2026-11-23T04:00:00Z',
    )
    PlatformProfile.save(path, profile)

    client = FakeClient([_instance('inst-1')])
    loaded = PlatformProfile.load_or_discover(path, client)

    assert client.calls == [], client.calls
    assert loaded.discovered_at == '2026-11-23T04:00:00Z'


def test_profile_file_contains_no_credentials(tmp_path):
    """画像文件里没有凭据。"""
    client = FakeClient([_instance('inst-1')])
    path = _profile_path(tmp_path)
    PlatformProfile.load_or_discover(path, client, node_registry=FakeNodeRegistry({}))

    with open(path, encoding='utf-8') as handle:
        raw = handle.read()
    for secret in ('TPOPS_PASSWORD', 'password', 'token', 'X-Mini-Token'):
        assert secret not in raw


def test_discovered_at_is_utc_iso(tmp_path):
    """discovered_at 用 UTC ISO 格式。"""
    client = FakeClient([_instance('inst-1')])
    profile = PlatformProfile.load_or_discover(
        _profile_path(tmp_path), client, node_registry=FakeNodeRegistry({}),
    )
    assert profile.discovered_at.endswith('Z')
    assert 'T' in profile.discovered_at
    assert len(profile.discovered_at) == len('2026-11-23T04:00:00Z')


def test_atomic_write_failure_leaves_no_partial_json(tmp_path, monkeypatch):
    """原子写中途失败不留半截 JSON。"""
    path = _profile_path(tmp_path)
    original = watermark_module._dump_payload

    def _boom(payload, handle):
        handle.write('{"version": 1, "instances": [')
        raise RuntimeError('磁盘满了')

    monkeypatch.setattr(watermark_module, '_dump_payload', _boom)
    with pytest.raises(Exception):
        PlatformProfile.save(path, PlatformProfile())

    assert not os.path.exists(path), '失败时不该留下半截文件'
    leftovers = [
        name for name in os.listdir(str(tmp_path))
        if name.startswith('.watermark-')
    ]
    assert leftovers == [], leftovers


def test_failed_atomic_write_keeps_previous_content(tmp_path, monkeypatch):
    """原子写中途失败 → 旧内容原样保留（不是半截）。"""
    path = _profile_path(tmp_path)
    client = FakeClient([_instance('inst-1')])
    PlatformProfile.load_or_discover(path, client, node_registry=FakeNodeRegistry({}))
    before = open(path, encoding='utf-8').read()

    def _boom(payload, handle):
        handle.write('{"version": 1, "inst')
        raise RuntimeError('磁盘满了')

    monkeypatch.setattr(watermark_module, '_dump_payload', _boom)
    with pytest.raises(Exception):
        PlatformProfile.save(path, PlatformProfile())

    assert open(path, encoding='utf-8').read() == before
    json.loads(open(path, encoding='utf-8').read())


# -- 核对 ------------------------------------------------------------------

def _saved_profile(tmp_path, instances, discovered_at='2026-11-23T04:00:00Z'):
    path = _profile_path(tmp_path)
    PlatformProfile.save(path, PlatformProfile(
        instances=tuple(instances), discovered_at=discovered_at,
    ))
    return path


def test_profile_instance_missing_on_platform_logs_error(tmp_path, caplog):
    """画像里的 instance 不在平台列表 → 记 ERROR。"""
    path = _saved_profile(tmp_path, [_entry('inst-gone')])
    client = FakeClient([_instance('inst-1')])

    with caplog.at_level('ERROR'):
        report = PlatformProfile.reconcile(
            PlatformProfile.load(path), client, node_registry=FakeNodeRegistry({}),
        )

    assert 'inst-gone' in report.missing_on_platform
    assert any(
        record.levelname == 'ERROR' and 'inst-gone' in record.getMessage()
        for record in caplog.records
    ), [record.getMessage() for record in caplog.records]


def test_platform_instance_missing_from_profile_logs_warning(tmp_path, caplog):
    """平台有画像没记录的实例 → 记 WARNING（提示删画像文件可重新发现）。"""
    path = _saved_profile(tmp_path, [_entry('inst-1')])
    client = FakeClient([_instance('inst-1'), _instance('inst-new', 'gauss-new')])

    with caplog.at_level('WARNING'):
        report = PlatformProfile.reconcile(
            PlatformProfile.load(path), client, node_registry=FakeNodeRegistry({}),
        )

    assert 'inst-new' in report.added_on_platform
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        record.levelname == 'WARNING' and 'inst-new' in message
        for record, message in zip(caplog.records, messages)
    ), messages
    assert any('删除画像文件可重新发现' in message for message in messages), messages


def test_reconcile_does_not_add_new_instance_to_profile(tmp_path):
    """平台新增的实例**不自动加**进画像（文件内容不变）。"""
    path = _saved_profile(tmp_path, [_entry('inst-1')])
    client = FakeClient([_instance('inst-1'), _instance('inst-new')])

    PlatformProfile.reconcile(
        PlatformProfile.load(path), client, node_registry=FakeNodeRegistry({}),
    )

    payload = json.loads(open(path, encoding='utf-8').read())
    assert [item['instance_id'] for item in payload['instances']] == ['inst-1']


def test_engine_name_change_logs_warning(tmp_path, caplog):
    """engine_name 变 → 记 WARNING。"""
    path = _saved_profile(tmp_path, [_entry('inst-1', engine='gaussdbv5')])
    client = FakeClient([_instance('inst-1', engine='gaussdbv6')])

    with caplog.at_level('WARNING'):
        report = PlatformProfile.reconcile(
            PlatformProfile.load(path), client, node_registry=FakeNodeRegistry({}),
        )

    assert 'inst-1' in report.engine_changed
    assert any(
        record.levelname == 'WARNING' and '引擎名' in record.getMessage()
        for record in caplog.records
    ), [record.getMessage() for record in caplog.records]


def test_master_count_change_logs_warning(tmp_path, caplog):
    """master_count 变 → 记 WARNING。"""
    path = _saved_profile(tmp_path, [_entry('inst-1', master_count=3)])
    client = FakeClient([_instance('inst-1')])
    registry = FakeNodeRegistry({'inst-1': _master_nodes(2)})

    with caplog.at_level('WARNING'):
        report = PlatformProfile.reconcile(
            PlatformProfile.load(path), client, node_registry=registry,
        )

    assert 'inst-1' in report.master_count_changed
    assert any(
        record.levelname == 'WARNING' and 'master' in record.getMessage()
        for record in caplog.records
    ), [record.getMessage() for record in caplog.records]


# -- 辅助 ------------------------------------------------------------------

def _entry(instance_id, name='gauss', engine='gaussdbv5', master_count=0):
    from tpops_kafka_collector.platform_profile import ProfileInstance
    return ProfileInstance(
        instance_id=instance_id,
        instance_name=name,
        mode='Combined',
        engine_name=engine,
        engine_version='V2.0-10.0.0',
        master_count=master_count,
    )
