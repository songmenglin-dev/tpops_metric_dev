# 任务 2.2 报告：合并 SQL 的运行时渲染

- 波次：wave-2a（serial，2.1 → 2.2）
- 实施范围：`tpops_kafka_collector/mapping/sqlgen.py`（新建）、`tests/unit/test_sqlgen.py`（新建）
- 工作目录：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（worktree）
- 未提交（按简报要求，提交由控制器统一执行）

## 1. 新增文件清单

| 文件 | 行数 | 说明 |
|---|---|---|
| `/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev/tpops_kafka_collector/mapping/sqlgen.py` | 264 | 合并 SQL 的运行时渲染（从 `tools/build_sql_merge.py` 复制） |
| `/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev/tests/unit/test_sqlgen.py` | 245 | 24 条单元测试 |

未改动任何既有文件（wave 0/1 的文件、`changes/` 下的文件、`data/` 与 `data/merged_sql/` 下的声明源）。

## 2. 「第 1 页逐字符一致」这条断言怎么写的

**断言本体**（`tests/unit/test_sqlgen.py`）：

```python
def test_first_page_is_byte_identical_to_the_archived_sql():
    """渲染出的第 1 页与包内 sql_merge.sql 逐字符一致。"""
    spec = sqlgen.load_spec()
    rendered = sqlgen.render_sql_document(spec, page_size=100, offset=0)
    with open(sqlgen.PACKAGE_SQL_MERGE_PATH, encoding='utf-8') as handle:
        expected = handle.read()
    assert rendered == expected, (
        f'渲染出的第 1 页与 {sqlgen.PACKAGE_SQL_MERGE_PATH} 不一致 '
        '—— 视图定义与留档已分叉')
```

**为什么比的是「文档」而不是「SQL 语句」**：`sql_merge.sql` 的实际形态是
**5 行注释头 + 1 行 SQL + 尾随换行**（无模板占位符）。为了让「逐字符一致」覆盖
整个文件而不是只覆盖其中一段，渲染器提供了两个函数：

- `render_sql(spec, page_size, offset, tier_seconds)` —— **纯 SQL 语句**（单行，
  无注释头）→ 这是要发给命令中心的 `sql_text`；
- `render_sql_document(...)` —— **注释头 + SQL + 尾随换行**，与归档产物同构。

注释头**逐字符复制**自生成器（`tools/build_sql_merge.py` 的 `main()` 里那 5 行
`handle.write(...)`），包括第一行的出处声明
`-- 由 tools/build_sql_merge.py 从 data/merged_sql/merged_sql.json 生成`
—— 那是**归档产物的出处声明**（该文件确由生成器产出），不是运行时依赖；
生产包不 import `tools/`（有专门测试守着，见 §7）。

另外两条配套断言：

- `test_rendered_statement_is_the_tail_of_the_archived_file` —— 纯 SQL 语句
  == 留档文件去掉注释头后的内容（含尾随换行），防止「文档对了但 SQL 主体错了」
  这类半吊子一致；
- `test_render_matches_the_generator_tool`（6 组参数：tier ∈ {全量,60,120,300} ×
  不同 page_size/offset）—— 把包内渲染与 `tools/build_sql_merge.py` 的
  `pick()` + `combine()` 输出逐字符对比，钉死「复制不改语义」。

## 3. 实测是否真的逐字符相同

**是。** 实测输出：

```
document identical: True 3167 3167
bytes identical: True
sql len: 2818
```

即 `render_sql_document(spec, page_size=100, offset=0)` 与包内
`mapping/data/sql_merge.sql` **逐字节相同**（3167 字符，含尾随换行）。
`tests/unit/test_sqlgen.py::test_first_page_is_byte_identical_to_the_archived_sql`
绿灯。

## 4. 路径解析怎么做的（为什么不依赖 cwd）

```python
PACKAGE_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
PACKAGE_MERGED_SQL_PATH = os.path.join(PACKAGE_DATA_DIR, 'merged_sql.json')
PACKAGE_SQL_MERGE_PATH = os.path.join(PACKAGE_DATA_DIR, 'sql_merge.sql')
```

- 基准是**模块文件自身的绝对路径**（`os.path.abspath(__file__)`），不是 cwd，
  也不是 `sys.path` 推导出来的包路径（后者在 zip/egg 部署下会失效）。
- 数据源是**包内** `mapping/data/merged_sql.json`（设计决策 9：采集端独立部署，
  运行时不读仓库根），**不读**仓库根的 `data/merged_sql/`。
- `load_spec(path=None)` 允许显式传入路径（测试与离线排查用），生产默认走包内。
- 有测试守着：`test_data_paths_do_not_depend_on_cwd` —— `os.chdir(tmp_path)`
  之后仍能加载出 11 个视图，且两个路径常量都是绝对路径。

## 5. limit / offset 的入口设计

| 入口 | 页容量来源 | offset | 用途 |
|---|---|---|---|
| `render_sql(spec, page_size=None, offset=0, tier_seconds=None)` | 显式传入；`None` → 声明里的 `paging.default_page_size`（100） | 显式传入 | 采集器按 offset 翻页 |
| `render_sql_with_config(spec, config, offset=0, tier_seconds=None)` | **`config.cmd_page_size`**（环境变量 `CMD_PAGE_SIZE`，默认 100） | 显式传入 | 「用配置默认值」的入口 |
| `render_sql_document(spec, page_size=None, offset=0, tier_seconds=None)` | 同 `render_sql` | 显式传入 | 复现归档产物，与留档逐字符比对 |

- `page_size` / `offset` **都能显式传**，采集器按 offset 翻页走
  `render_sql(spec, page_size=cfg.cmd_page_size, offset=n * page_size)`。
- `page_size < 1` / `offset < 0` → 抛 `MappingError`（不静默）。
- 刷新档位 `tier_seconds`（60/120/300）**从声明的 `refresh_seconds` 现算**
  （不硬编码视图清单），与生成器 `--tier` 的挑法等价；未知档 → `MappingError`。

**分页语义（与生成器一致）**：

- 可增长视图（`paged = true`，本声明里是 `stat_user_tables` 与 `dbstat_sum`）：
  剥掉原 SQL 末尾的 `limit` 护栏（`strip_inner_limit`），外层套
  `limit P offset O`；
- 固定视图（`paged = false`，含 `paging.views` **未声明**的 7 个视图）：
  - 自带语义上限的（`activity_running` 的 `limit 20`、`activity_top10` 的
    `limit 10`）→ **原样保留**，offset 恒为 0（再套一个 limit 会变成
    `limit 20 limit 100` 的语法错误）；
  - 不带上限的 → `limit P offset 0`。

实测第 2 页（offset=100）：`... order by t.n_dead_tup desc limit 100 offset 100 ) t) as dbstat_sum`。

## 6. 那条推导链现在由什么守着

**先回答简报里的问题**：`tools/build_sql_merge.py` **没有 `--check` 模式**
（它的参数只有 `--tier` / `--limit` / `--offset` / `--out`）。对比：
`tools/build_observed_columns.py` 是**有** `--check` 的。所以仓库根那条
「`merged_sql.json` → `sql_merge.sql`」的推导链**此前没有任何闸**。

现在由**三道闸串起来**闭合：

1. **包内渲染 ≡ 包内留档**（本次新增，`test_sqlgen.py`）：
   `render_sql_document(spec, 100, 0)` ≡ `mapping/data/sql_merge.sql`（逐字节）。
   → 视图定义（`merged_sql.json`）与留档产物分叉，立刻红。
2. **包内副本 ≡ 仓库根声明**（wave-2a 任务 2.1，`test_mapping_data_sync.py`）：
   `mapping/data/merged_sql.json` ≡ `data/merged_sql/merged_sql.json`，
   `mapping/data/sql_merge.sql` ≡ `data/merged_sql/sql_merge.sql`（逐字节）。
3. **生成器重跑 ≡ 仓库根归档产物**（本次新增，
   `test_generator_reproduces_the_archived_artifact`）：以 `--out <tmp>` 重跑
   `tools/build_sql_merge.py`，输出与仓库根 `data/merged_sql/sql_merge.sql`
   逐字节一致。→ 改了视图定义却没重跑生成器，红。

（1）+（2）已经把「声明 → 留档」在包内闭合；（3）补上「声明 → 生成器输出 →
归档产物」这一段，把此前无人看守的那一环接上。

**残余缺口（遗留项）**：三道闸都只在**测试**里；`tools/build_sql_merge.py`
本身仍无 `--check` 模式，无法在 CI 里当「生成物闸」用。若要补齐，应给该脚本
加 `--check`（对比现产物，不一致则非零退出）—— **该文件不在本任务的独占范围内，
未改动**，留给控制器裁决。

## 7. RED 的失败断言

TDD 顺序：先写 `tests/unit/test_sqlgen.py`，再放一个**只会读 JSON、不会渲染**
的桩 `sqlgen.py`（`render_sql` 直接把 11 条原 SQL 用 `, ` 拼起来），跑测试看红：

```
16 failed, 1 passed in 0.89s
```

核心那条的失败断言（RED 原文）：

```
E       AssertionError: 渲染出的第 1 页与 /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev/tpops_kafka_collector/mapping/data/sql_merge.sql 不一致 —— 视图定义与留档已分叉
E       assert 'select selec...esc limit 100' == '-- 由 tools/b... dbstat_sum\n'
E         + select select datname, numbackends, xact_commit, ... order by datname, select schemaname, relname, ...
```

即：渲染出的是「11 条裸 SQL 直接拼接」，而留档是「`json_agg` 包起来的一条
合并 SQL + 注释头」—— 断言精确地把差异打了出来。

（第一版测试连模块都 import 不到，RED 表现为
`ImportError: cannot import name 'sqlgen' from 'tpops_kafka_collector.mapping'`；
换成桩实现后得到上面这条更有意义的**断言失败**。）

## 8. GREEN 的测试输出摘要

```
$ conda run -n tpops python -m pytest tests/unit/test_sqlgen.py -q
24 passed in 0.38s

$ conda run -n tpops python -m pytest -q --ignore=tests/unit/test_command_center.py
848 passed in 12.64s

$ ruff check tpops_kafka_collector/mapping/sqlgen.py tests/unit/test_sqlgen.py
All checks passed!
```

- 本任务基线：简报给的基线是 752 passed；**2.1 落地后**仓库基线已变成
  809（2.1 的防漂移闸是参数化的，条数多），本次新增 24 条 → 809 + 24 = **833**；
  实测 848 是因为**并发的 wave-2b 实施方**同时在 worktree 里新增了
  `tests/unit/test_masking.py`（19 条）等文件 —— 与本任务无关。
- 全量套件在我最后一次全绿运行时是 **843 passed**；随后 wave-2b 的
  `tests/unit/test_command_center.py` 落盘（它 import 的
  `tpops_kafka_collector.tpops.command_center` 尚未存在）→ 全量收集报
  `ModuleNotFoundError`，**与本任务无关**（我的 24 条在排除该文件后全绿）。
- `ruff check tpops_kafka_collector/` 现有 5 条告警，**全部在 wave-2b 的
  `tpops_kafka_collector/mapping/masking.py`**（UP009/UP010/UP031×3），
  我的 `sqlgen.py` 干净为 0。提请控制器注意（属他人文件，我未改）。
- Python 3.7.9 兼容闸：`tests/test_py37_compat.py` **85 passed**（含本次新增的
  两个文件；无海象、无内建泛型注解、无 3.8+ 标准库 API）。
- 单文件行数：`sqlgen.py` 264 行、测试 245 行，均 < 400；最长函数
  `render_sql_document` 约 30 行，< 50。

## 9. 偏离简报之处及原因

| 偏离 | 原因 |
|---|---|
| 渲染分**两个**函数（`render_sql` 纯 SQL / `render_sql_document` 注释头+SQL），而不是一个 | `sql_merge.sql` 里含 5 行注释头。若只渲染纯 SQL，「逐字符一致」就只能比文件的一部分；拆开后既能让**整文件**逐字符相同，又能让发给平台的 `sql_text` 不带历史注释 |
| 注释头逐字符复制了生成器的 5 行（含第一行的 `tools/build_sql_merge.py` 出处声明） | 逐字符比对的前提。注释头是**归档产物的出处声明**，不是运行时依赖；生产包不 import `tools/`（`test_module_does_not_import_tools_or_repo_root_data` 用 ast 守着） |
| 刷新档位从声明的 `refresh_seconds` 现算，而不是复制生成器里硬编码的 `TIER_KEYS` | 去掉一份重复的视图清单（DRY）。**语义等价由测试证明**：`test_render_matches_the_generator_tool` 6 组参数（含 3 个档位）逐字符比对生成器输出 |
| 额外加了 `test_generator_reproduces_the_archived_artifact`（重跑生成器与仓库根归档产物比对） | 简报只要求「在报告里说明这条链现在由什么守着」。生成器**没有 `--check`**，光靠包内两条闸关不掉「改了视图定义没重跑生成器」这一段；顺手补上（只写 tmp 目录，不碰仓库文件） |
| 额外加了 `test_render_matches_the_generator_tool`（测试侧 import `tools/`） | 把「复制不改语义」钉死，防止后续只改包内渲染器。生产包不依赖 `tools/`，只有测试依赖 |

## 10. 遗留问题

1. **`tools/build_sql_merge.py` 仍无 `--check`** —— 建议后续给生成器加
   `--check`（与 `tools/build_observed_columns.py` 对齐），把「生成物闸」从测试
   提升到 CI 闸。**本任务未改该文件**（不在独占范围）。
2. **`offset` 的语义边界**：当前 offset 对**固定视图**恒为 0（与生成器一致）。
   采集器按 offset 翻页时，固定视图每页都会重复取一遍全量（合计约 5.3 KB，
   见 `merged_sql.json` 的 `paging.note`）；拼接侧由
   `tpops_kafka_collector.mapping.decode.merge_pages` 负责只取第一页
   （规格「分页结果的拼接」）。本任务只管渲染，拼接未动。
3. **平台侧的 `sql_text` 长度上限（10000 字符）**：全量合并 SQL 是 2818 字符，
   有余量；但若将来视图数增加，需要复核。
4. **并发工作树的噪声**：本任务执行期间，wave-2b 的实施方在同一 worktree 里
   落盘了 `mapping/masking.py`、`tests/unit/test_masking.py`、
   `tests/unit/test_command_center.py`。后者在 `tpops_kafka_collector/tpops/command_center.py`
   落地前会让**全量** pytest 收集失败（与本任务无关）。控制器统一提交时请确认
   提交范围。
5. **未提交**：按简报要求未执行 `git commit`。
