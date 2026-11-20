# -*- coding: utf-8 -*-
"""映射数据副本的**防漂移闸**（设计决策 9）。

``data/mapping/``、``data/merged_sql/``、``data/customer_metric_mapping.json``
是**参考与发布源**；``tpops_kafka_collector/mapping/data/`` 是**生产用的副本**
（采集端独立部署，运行时不读仓库根目录）。两份必须**逐字节一致**，否则
「发布的声明」和「跑起来的声明」会分叉。

本文件守三件事：

1. **副本存在** —— 包内 9 个副本一个都不能少；
2. **副本内容与仓库根声明逐字节一致** —— 含缩进、键序、尾随换行；
3. **闸本身有效** —— 故意删一份 / 改一份，检查器必须报出来
   （否则「两边相等」的空断言在副本根本没复制过来时也会绿）。

``observed_columns.json`` 不在此列 —— 它是从实测留档**现算**的声明，
不是仓库文件的副本，由 ``test_mapping_loader.py`` 的生成器闸守住。
"""
from __future__ import absolute_import

import json
import os
import shutil

import pytest

from conftest import REPO_ROOT

PACKAGE_DATA_DIR = os.path.join(
    REPO_ROOT, 'tpops_kafka_collector', 'mapping', 'data')

#: 包内副本文件名 → 仓库根的声明源（相对 REPO_ROOT）。
COPIED_FILES = {
    'metric_rules.json': os.path.join('data', 'mapping', 'metric_rules.json'),
    'alarm_rules.json': os.path.join('data', 'mapping', 'alarm_rules.json'),
    'itscada_field_map.json': os.path.join(
        'data', 'mapping', 'itscada_field_map.json'),
    'kafka_field_mapping.json': os.path.join(
        'data', 'mapping', 'kafka_field_mapping.json'),
    'response_rules.json': os.path.join(
        'data', 'mapping', 'response_rules.json'),
    'open_items.json': os.path.join('data', 'mapping', 'open_items.json'),
    'customer_metric_mapping.json': os.path.join(
        'data', 'customer_metric_mapping.json'),
    'merged_sql.json': os.path.join('data', 'merged_sql', 'merged_sql.json'),
    'sql_merge.sql': os.path.join('data', 'merged_sql', 'sql_merge.sql'),
}

#: 副本文件名清单（有序，便于报告逐个列出）。
COPY_NAMES = tuple(sorted(COPIED_FILES))


def read_bytes(path):
    """按二进制读整个文件（逐字节比较，不做换行转换）。"""
    with open(path, 'rb') as handle:
        return handle.read()


def find_copy_problems(package_dir, repo_root=REPO_ROOT):
    """列出包内副本与仓库声明源之间的**全部**差异。

    返回人类可读的差异描述列表；列表为空表示双向都对得上。

    :param package_dir: 被检查的包内数据目录（沙箱测试会传临时目录）。
    :param repo_root: 仓库根目录。
    :return: 差异描述列表。
    """
    problems = []
    for name in COPY_NAMES:
        package_path = os.path.join(package_dir, name)
        repo_path = os.path.join(repo_root, COPIED_FILES[name])
        if not os.path.isfile(repo_path):
            problems.append('%s: 仓库声明源不存在（%s）' % (name, COPIED_FILES[name]))
            continue
        if not os.path.isfile(package_path):
            problems.append('%s: 包内副本不存在' % name)
            continue
        if read_bytes(package_path) != read_bytes(repo_path):
            problems.append('%s: 包内副本与仓库声明源 %s 不一致'
                            % (name, COPIED_FILES[name]))
    return problems


@pytest.fixture()
def sandbox(tmp_path):
    """包内映射数据的一份可改副本。"""
    target = str(tmp_path / 'mapping_data')
    shutil.copytree(PACKAGE_DATA_DIR, target)
    return target


# -- 副本存在 -------------------------------------------------------------
@pytest.mark.parametrize('name', COPY_NAMES)
def test_package_copy_exists(name):
    """包内副本必须存在（少一个文件就是漏复制，不能靠「两边相等」蒙混）。"""
    path = os.path.join(PACKAGE_DATA_DIR, name)
    assert os.path.isfile(path), '包内缺少副本 %s' % name
    assert os.path.getsize(path) > 0, '包内副本 %s 是空文件' % name


@pytest.mark.parametrize('name', COPY_NAMES)
def test_repo_source_exists(name):
    """仓库声明源必须存在（副本要有可对账的对象）。"""
    path = os.path.join(REPO_ROOT, COPIED_FILES[name])
    assert os.path.isfile(path), '仓库缺少声明源 %s' % COPIED_FILES[name]


# -- 逐字节一致 -----------------------------------------------------------
@pytest.mark.parametrize('name', COPY_NAMES)
def test_package_copy_is_byte_identical_to_the_repo_source(name):
    """包内副本与仓库声明源逐字节一致（缩进/键序/尾随换行都算）。"""
    package_path = os.path.join(PACKAGE_DATA_DIR, name)
    repo_path = os.path.join(REPO_ROOT, COPIED_FILES[name])
    assert read_bytes(package_path) == read_bytes(repo_path), (
        '%s 与仓库声明源 %s 不一致 —— 重新复制或重跑生成器'
        % (name, COPIED_FILES[name]))


def test_no_copy_problems_at_all():
    """整体扫一遍：任何差异都不许有。"""
    assert find_copy_problems(PACKAGE_DATA_DIR) == []


# -- 闸本身有效（反例） ---------------------------------------------------
def test_a_missing_copy_is_reported(sandbox):
    """故意删掉一份副本 → 检查器必须报出来。"""
    os.remove(os.path.join(sandbox, 'merged_sql.json'))
    problems = find_copy_problems(sandbox)
    assert problems, '删掉副本后检查器仍然报「无差异」—— 闸是假的'
    assert any('merged_sql.json' in item and '不存在' in item
               for item in problems), problems


def test_a_drifted_copy_is_reported(sandbox):
    """故意改一份副本的内容 → 检查器必须报出来。"""
    path = os.path.join(sandbox, 'merged_sql.json')
    with open(path, 'rb') as handle:
        payload = handle.read()
    with open(path, 'wb') as handle:
        handle.write(payload + b'\n')
    problems = find_copy_problems(sandbox)
    assert problems, '改坏副本后检查器仍然报「无差异」—— 闸是假的'
    assert any('merged_sql.json' in item and '不一致' in item
               for item in problems), problems


def test_a_drifted_text_copy_is_reported(sandbox):
    """故意改一份文本副本（SQL）的内容 → 检查器必须报出来。"""
    path = os.path.join(sandbox, 'sql_merge.sql')
    with open(path, 'rb') as handle:
        payload = handle.read()
    with open(path, 'wb') as handle:
        handle.write(payload.replace(b'limit 100', b'limit 999'))
    problems = find_copy_problems(sandbox)
    assert problems, '改坏 SQL 副本后检查器仍然报「无差异」—— 闸是假的'
    assert any('sql_merge.sql' in item and '不一致' in item
               for item in problems), problems


# -- 副本的结构完整性（不只是「文件在」） --------------------------------
def test_merged_sql_copy_keeps_views_tiers_and_paging():
    """merged_sql.json 副本保留 11 个视图定义、刷新档位与 paging 段。

    2.2 的 sqlgen 要靠视图定义与 paging 段渲染分页 SQL；
    副本一旦丢段，运行时才发现就晚了。
    """
    path = os.path.join(PACKAGE_DATA_DIR, 'merged_sql.json')
    with open(path, 'rb') as handle:
        payload = json.loads(handle.read().decode('utf-8'))

    assert 'paging' in payload, 'merged_sql.json 副本缺 paging 段'
    assert payload['paging']['default_page_size'] > 0
    assert payload['paging']['views'], 'paging.views 为空'

    batches = payload['batches']
    assert len(batches) == 11, '视图定义数不对：%d' % len(batches)
    for batch in batches:
        assert batch.get('sql'), '视图 %s 缺 sql' % batch.get('sql_key')
        assert batch.get('refresh_seconds'), '视图 %s 缺刷新档位' % batch.get('sql_key')
        assert batch.get('refresh_tier'), '视图 %s 缺刷新档位名' % batch.get('sql_key')


def test_sql_merge_copy_is_not_empty_text():
    """sql_merge.sql 副本是有效 SQL 文本（不是空文件/二进制垃圾）。"""
    path = os.path.join(PACKAGE_DATA_DIR, 'sql_merge.sql')
    with open(path, 'rb') as handle:
        raw = handle.read()
    text = raw.decode('utf-8')
    assert text.strip(), 'sql_merge.sql 副本是空的'
    assert 'select' in text.lower()
    assert 'json_agg' in text.lower()
