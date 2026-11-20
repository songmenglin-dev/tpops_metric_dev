"""合并 SQL 的运行时渲染（``tpops_kafka_collector.mapping.sqlgen``）。

守住三条线：

1. **渲染与留档逐字符一致** —— 渲染出的第 1 页（limit 100 / offset 0）与包内
   ``mapping/data/sql_merge.sql`` **逐字符相同**（含空白、缩进、尾随换行）。
   这条断言同时把 ``merged_sql.json → sql_merge.sql`` 的推导链在**包内**闭合：
   生成器 ``tools/build_sql_merge.py`` 的输出与运行时的渲染器同源同义，
   任何一边改了视图定义而另一边没跟上，测试立刻红。
2. **不依赖 cwd** —— 采集端是常驻进程，可能从任意目录启动；
   数据文件路径基于 ``__file__`` 解析。
3. **分页语义与生成器一致** —— 可增长视图（``paged = true``）带
   ``limit N offset M``；固定视图保留语义上限（``limit 20`` / ``limit 10``），
   否则再套一个 limit 会变成 ``limit 20 limit 100`` 的语法错误。
"""
import ast
import os
import sys

import pytest

from conftest import REPO_ROOT
from tpops_kafka_collector.config import load_config
from tpops_kafka_collector.errors import MappingError
from tpops_kafka_collector.mapping import sqlgen


# -- 第 1 页与留档逐字符一致 ---------------------------------------------
def test_first_page_is_byte_identical_to_the_archived_sql():
    """渲染出的第 1 页与包内 sql_merge.sql 逐字符一致。"""
    spec = sqlgen.load_spec()
    rendered = sqlgen.render_sql_document(spec, page_size=100, offset=0)
    with open(sqlgen.PACKAGE_SQL_MERGE_PATH, encoding='utf-8') as handle:
        expected = handle.read()
    assert rendered == expected, (
        f'渲染出的第 1 页与 {sqlgen.PACKAGE_SQL_MERGE_PATH} 不一致 '
        '—— 视图定义与留档已分叉')


def test_rendered_statement_is_the_tail_of_the_archived_file():
    """纯 SQL 语句 = 留档文件去掉注释头后的内容（含尾随换行）。"""
    spec = sqlgen.load_spec()
    sql = sqlgen.render_sql(spec, page_size=100, offset=0)
    with open(sqlgen.PACKAGE_SQL_MERGE_PATH, encoding='utf-8') as handle:
        lines = handle.read().splitlines(True)
    body = ''.join(line for line in lines if not line.startswith('--'))
    assert body == sql + '\n'


def test_rendered_statement_covers_all_eleven_views():
    """全量渲染出 11 个视图的别名。"""
    spec = sqlgen.load_spec()
    sql = sqlgen.render_sql(spec, page_size=100, offset=0)
    for view in spec.views:
        assert f') as {view.sql_key}' in sql, f'缺少视图 {view.sql_key}'
    assert len(spec.views) == 11


# -- 视图定义与分页规则 ----------------------------------------------------
def test_spec_default_page_size_is_hundred():
    spec = sqlgen.load_spec()
    assert spec.default_page_size == 100


def test_paging_rules_match_the_declaration():
    """paging 段里没声明的视图按 paged=false 处理。"""
    spec = sqlgen.load_spec()
    rules = dict((view.sql_key, view.paging) for view in spec.views)
    assert rules['stat_user_tables'].paged is True
    assert rules['stat_user_tables'].strip_inner_limit is True
    assert rules['dbstat_sum'].paged is True
    assert rules['activity_running'].paged is False
    assert rules['activity_top10'].paged is False
    # 未声明 → 固定视图
    assert rules['stat_database'].paged is False


# -- 分页：offset 只作用于可增长视图 --------------------------------------
def test_offset_only_shifts_growable_views():
    """offset 只作用于 paged=true 的视图。"""
    spec = sqlgen.load_spec()
    sql = sqlgen.render_sql(spec, page_size=100, offset=200)
    assert 'limit 100 offset 200' in sql
    # 固定视图保留语义上限，不受 offset 影响
    assert 'limit 20 ) t) as activity_running' in sql
    assert 'limit 10 ) t) as activity_top10' in sql


def test_growable_views_strip_the_inner_limit_guard():
    """可增长视图剥掉原 SQL 的 limit 护栏，再套外层窗口。"""
    spec = sqlgen.load_spec()
    sql = sqlgen.render_sql(spec, page_size=100, offset=0)
    # dbstat_sum 的原 SQL 以 limit 100 收尾 → 剥掉后只留一个窗口
    assert 'limit 100 limit' not in sql
    assert 'order by t.n_dead_tup desc limit 100 offset 0' in sql


def test_page_size_is_honoured():
    spec = sqlgen.load_spec()
    sql = sqlgen.render_sql(spec, page_size=25, offset=0)
    assert 'limit 25 offset 0' in sql


# -- 配置入口（页容量取自 CMD_PAGE_SIZE） ---------------------------------
def test_page_size_comes_from_config():
    """用配置默认值的入口：页容量取 config.cmd_page_size。"""
    spec = sqlgen.load_spec()
    config = load_config({
        'TPOPS_USER': 'u', 'TPOPS_PASSWORD': 'p',
        'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
        'CMD_PAGE_SIZE': '37',
    })
    sql = sqlgen.render_sql_with_config(spec, config, offset=0)
    assert 'limit 37 offset 0' in sql


def test_config_default_page_size_is_used_when_env_absent():
    spec = sqlgen.load_spec()
    config = load_config({
        'TPOPS_USER': 'u', 'TPOPS_PASSWORD': 'p',
        'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
    })
    sql = sqlgen.render_sql_with_config(spec, config, offset=0)
    assert f'limit {config.cmd_page_size} offset 0' in sql


# -- 刷新档位 --------------------------------------------------------------
def test_tier_selection_by_refresh_seconds():
    """按刷新秒数挑视图（60 秒档 7 条 / 120 秒档 2 条 / 300 秒档 2 条）。"""
    spec = sqlgen.load_spec()
    keys60 = [view.sql_key for view in sqlgen.select_views(spec, tier_seconds=60)]
    keys120 = [view.sql_key for view in sqlgen.select_views(spec, tier_seconds=120)]
    keys300 = [view.sql_key for view in sqlgen.select_views(spec, tier_seconds=300)]
    assert keys60 == ['stat_database', 'activity_agg', 'activity_running',
                       'locks', 'settings', 'prepared_xacts', 'dbstat_sum']
    assert keys120 == ['stat_user_tables', 'database']
    assert keys300 == ['activity_top10', 'bgwriter']
    assert len(sqlgen.select_views(spec, tier_seconds=None)) == 11


def test_unknown_tier_raises():
    spec = sqlgen.load_spec()
    with pytest.raises(MappingError):
        sqlgen.render_sql(spec, page_size=100, offset=0, tier_seconds=999)


# -- 与生成器语义等价（「复制」不是「重写」） --------------------------------
@pytest.mark.parametrize('tier,page_size,offset', [
    (0, 100, 0),
    (0, 100, 200),
    (60, 100, 0),
    (60, 50, 100),
    (120, 100, 300),
    (300, 100, 0),
])
def test_render_matches_the_generator_tool(tier, page_size, offset):
    """与 tools/build_sql_merge.py 的渲染结果逐字符相同。

    生产包**不 import** tools/（设计决策 2），但两份实现必须同义 ——
    这条测试把「复制」的语义钉死：改了包内渲染逻辑而没同步生成器（或反之），
    测试立刻红。
    """
    generator = _generator_module()
    raw_spec = generator.load_spec(sqlgen.PACKAGE_MERGED_SQL_PATH)
    batches = generator.pick(generator.load_batches(raw_spec), tier)
    expected = generator.combine(batches, raw_spec, page_size, offset)
    rendered = sqlgen.render_sql(
        sqlgen.load_spec(), page_size=page_size, offset=offset,
        tier_seconds=tier or None)
    assert rendered == expected


def _generator_module():
    """import 生成器（**仅测试用**，生产包不依赖 tools/）。"""
    tools_dir = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), 'tools')
    if tools_dir not in sys.path:
        sys.path.insert(0, tools_dir)
    import build_sql_merge

    return build_sql_merge


def test_generator_reproduces_the_archived_artifact(tmp_path, monkeypatch):
    """生成器重跑一遍 → 与仓库根归档产物逐字节一致。

    ``tools/build_sql_merge.py`` **没有 --check 模式**，仓库根那条
    「merged_sql.json → sql_merge.sql」的推导链原本没有闸。这条测试补上：
    改了视图定义却没重跑生成器（或生成器本身被改坏），归档产物就会与
    生成器输出分叉，测试立刻红。
    """
    generator = _generator_module()
    out = str(tmp_path / 'sql_merge.sql')
    monkeypatch.setattr(sys, 'argv', ['build_sql_merge.py', '--out', out])
    generator.main()
    with open(out, 'rb') as handle:
        produced = handle.read()
    archived = os.path.join(REPO_ROOT, 'data', 'merged_sql', 'sql_merge.sql')
    with open(archived, 'rb') as handle:
        expected = handle.read()
    assert produced == expected, (
        '生成器输出与仓库根归档产物 data/merged_sql/sql_merge.sql 不一致 '
        '—— 改了视图定义要重跑 tools/build_sql_merge.py')


# -- 参数校验 --------------------------------------------------------------
@pytest.mark.parametrize('page_size', [0, -1])
def test_invalid_page_size_raises(page_size):
    spec = sqlgen.load_spec()
    with pytest.raises(MappingError):
        sqlgen.render_sql(spec, page_size=page_size, offset=0)


def test_negative_offset_raises():
    spec = sqlgen.load_spec()
    with pytest.raises(MappingError):
        sqlgen.render_sql(spec, page_size=100, offset=-1)


# -- 路径解析：不依赖 cwd --------------------------------------------------
def test_data_paths_do_not_depend_on_cwd(tmp_path):
    """换到别的目录再加载，数据仍从包内读。"""
    here = os.getcwd()
    os.chdir(str(tmp_path))
    try:
        spec = sqlgen.load_spec()
        assert len(spec.views) == 11
        assert os.path.isabs(sqlgen.PACKAGE_MERGED_SQL_PATH)
        assert os.path.isabs(sqlgen.PACKAGE_SQL_MERGE_PATH)
    finally:
        os.chdir(here)


def test_module_does_not_import_tools_or_repo_root_data():
    """生产包不得 import tools/ 或仓库根的 data/（设计决策 2）。"""
    with open(sqlgen.__file__, encoding='utf-8') as handle:
        tree = ast.parse(handle.read())
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    assert not [name for name in imported
                if name.split('.')[0] in ('tools', 'data')], imported
