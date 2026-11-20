# Wave 2a 审查报告（wave-2a-mapping-data，revision 3）

- **变更**：`tpops-kafka-collector`
- **波次**：`wave-2a-mapping-data`（serial：2.1 → 2.2）
- **Base**：`85e8a64`（wave-1 审查通过的 head）
- **Head**：`ff4993f`
- **本波次审的任务**：`c816358`（2.1 映射数据防漂移）、`ff4993f`（2.2 合并 SQL 运行时渲染）
- **不审**：`6d84ef6`（2.3 A 类窗口计算，属 wave-2b）
- **审查者**：独立审查者（未参与实施）

---

## 1. 结论

**Verdict：`pass`**

- Critical：**0**
- Important：**0**
- Minor：**9**（逐条见 §10，均不阻塞本波次）

三道闸经独立核实**全部存在且各自非恒真**；两个任务的证明命令在 worktree 里**实跑通过**；
生产代码边界经核实**未被突破**（`sqlgen` 只读包内 `mapping/data/`）；
diff **未触碰** `execution-contract.md` / `tasks.md` / 仓库根 `data/mapping/*.json` /
`data/merged_sql/*` 的内容。

已获用户裁决的 W-1 偏离（1.7/2.1 依赖倒置拆分）判定为**可接受**，理由见 §7。

> 遗留的风险（仓库根生成物无 `--check`、`MappingTable.load()` 仍指向仓库根、
> 部署打包未固化 `mapping/data/*.json`）均登记在 §11 待补清单，本波次不阻塞，
> 但**必须在 wave 4 交付前闭环**。

---

## 2. 审查范围与方法

- 只读 diff 文件一次（1548 行）；diff 的上下文行即被改文件内容。
- diff 的**变更统计**列出 7 个文件，但**正文只含 5 个**：
  `tpops_kafka_collector/mapping/data/merged_sql.json`（939 行）与
  `mapping/data/sql_merge.sql`（6 行）被「排除 data/ 下的大生成物」规则连带排除在正文外。
  这两份**新增的包内副本**因此无法从 diff 读到内容 → 改用 **sha256 逐一核对**核实（§5.1）。
- **未跑任何 git 命令**；提交列表与 diff 全部取自 diff 文件头部。
- 为评估**可具体命名的风险**，做了 4 次 diff 之外的聚焦检查，逐条记录于 §12。
- 全程**未修改任何文件**（只跑 pytest / ruff / sha256 / ls / grep / sed 只读命令）。

---

## 3. 合同 Wave 2a 完成标准逐条判定

来源：`changes/tpops-kafka-collector/execution-contract.md` §Wave 2a（完成标准三条）。

| # | 合同完成标准 | 判定 | 依据 |
|---|---|---|---|
| 1 | 渲染出的第 1 页 SQL 与 `data/merged_sql/sql_merge.sql` **逐字符一致** | **满足** | `tests/unit/test_sqlgen.py::test_first_page_is_byte_identical_to_the_archived_sql` 断言 `render_sql_document(spec, page_size=100, offset=0)` **逐字符**等于 `mapping/data/sql_merge.sql`。该断言直接比的是**包内副本**而非合同点名的仓库根路径；两者等价性由 `test_mapping_data_sync.py` 的逐字节闸保证，且我独立核对了 sha256 完全相同（§5.1）→ **传递成立**。另有一条 `test_rendered_statement_is_the_tail_of_the_archived_file` 把「纯 SQL 语句 == 留档去掉注释头」也钉住，防止「文档对了、SQL 主体错了」的半吊子一致。 |
| 2 | 包内映射数据与仓库根 `data/` 一致（一致性测试通过） | **满足** | `tests/unit/test_mapping_data_sync.py` 覆盖**全部 9 份**副本（7 张既有表 + 本波新增 2 个），逐**字节**比较；`test_no_copy_problems_at_all` 整体扫一遍。我另行用 sha256 核对 `merged_sql.json` / `sql_merge.sql` 两份新增副本 ≡ 仓库根（§5.1），并实跑全量 `pytest -q` → **885 passed**。 |
| 3 | 渲染出的第 N 页 SQL 的 `offset` 正确推进，且只作用于可增长视图 | **满足** | `test_offset_only_shifts_growable_views`（offset=200）断言 `limit 100 offset 200` 出现，且固定视图 `activity_running`（`limit 20`）/`activity_top10`（`limit 10`）保留语义上限、offset 恒为 0；`test_render_matches_the_generator_tool` 用 6 组参数（含 offset=200/100/300、page_size=50/100、tier=全量/60/120/300）与生成器 `pick()+combine()` 输出**逐字符**比对。 |

**判定：3/3 满足。**

---

## 4. 两个任务的证明命令（实跑）

| 命令 | 结果 |
|---|---|
| `conda run -n tpops python -m pytest tests/unit/test_mapping_data_sync.py -q` | **通过**（与 test_sqlgen.py 合并跑：`57 passed in 0.97s`；2.1 报告称单文件 33 条、2.2 报告称单文件 24 条，33+24=57 **条数吻合**） |
| `conda run -n tpops python -m pytest tests/unit/test_sqlgen.py -q` | **通过**（同上，合并跑的 57 条里含全部 24 条） |
| `conda run -n tpops python -m pytest -q`（全量，含并发落地的 wave-2b 文件） | **885 passed in 13.38s** |
| `conda run -n tpops ruff check tpops_kafka_collector/` | **All checks passed!**（exit 0；2.2 报告提到 masking.py 有 5 条告警，现已不存在/已被修，说明 wave-2b 后续补齐） |

两条证明命令**独立复跑成立**，不是引述报告。

---

## 5. 任务 2.1 逐条判定（映射数据防漂移）

### 5.1 复制是否逐字节 —— **是**

diff 未含两份新增副本的正文，改用 sha256 独立核对（worktree 现状）：

```
f0acc8191a79603693be4d76179810b94f825c17edf200fe36ad83a38f03c4ba  tpops_kafka_collector/mapping/data/merged_sql.json
f0acc8191a79603693be4d76179810b94f825c17edf200fe36ad83a38f03c4ba  data/merged_sql/merged_sql.json
306ab72197e99a64297233efa5df62bdef08572f1036ae64ff54cb57e6712ba9  tpops_kafka_collector/mapping/data/sql_merge.sql
306ab72197e99a64297233efa5df62bdef08572f1036ae64ff54cb57e6712ba9  data/merged_sql/sql_merge.sql
```

两侧 hash **完全相同**；`sql_merge.sql` 两侧均 3339 B（含尾随换行）。→ **逐字节复制成立**。
且 diff 的变更统计显示仓库根 `data/merged_sql/*` **未被改动**（不在 7 个变更文件里），
说明这是「复制」而不是「改了仓库根再复制」。

### 5.2 一致性测试是否覆盖包内全部副本 —— **是，9/9**

`COPIED_FILES` 显式列出 9 条：7 张既有表（`metric_rules` / `alarm_rules` /
`itscada_field_map` / `kafka_field_mapping` / `response_rules` / `open_items` /
`customer_metric_mapping`）+ 本波新增 2 个（`merged_sql.json` / `sql_merge.sql`）。
与包内实际文件清单（`ls tpops_kafka_collector/mapping/data/`）逐一对上，
唯一未纳入的是 `observed_columns.json` —— 理由成立：它不是仓库文件的副本，
而是由 `tools/build_observed_columns.py` 现算的声明，
由 `test_mapping_loader.py::test_observed_columns_match_the_generator_output`
与 `::test_observed_columns_generator_is_in_sync`（后者跑生成器的 `--check`）双重守住。

### 5.3 存在性是否被独立断言 —— **是**

`test_package_copy_exists[name]`（`isfile` + `getsize() > 0`）与
`test_repo_source_exists[name]`（双向对账：副本有可对账的对象）**独立于**内容比较断言。
若只写「两边相等」，副本没复制过来时会因 `FileNotFoundError` 报错而不是「绿」——
但那不会告诉你「少了哪个」；显式断言把失败信息指向具体文件。**符合简报要求。**

### 5.4 反例是否有效 —— **是，且非空壳**

三条反例都在 `shutil.copytree` 出来的**沙箱副本**上做（不改仓库文件）：

- `test_a_missing_copy_is_reported`：删 `merged_sql.json` → 断言 problems 非空**且**包含
  `'merged_sql.json' in item and '不存在' in item`（不只看「非空」，还校验了**哪一条**差异）；
- `test_a_drifted_copy_is_reported`：给 JSON 副本尾部追加 `\n` → 断言含 `'不一致'`；
- `test_a_drifted_text_copy_is_reported`：把 SQL 副本里的 `limit 100` 改成 `limit 999`
  → 断言含 `'不一致'`。

这三条若检查器恒返回 `[]` 必红。**闸有效。**

### 5.5 实施方对「1.7 已有守卫」的说法 —— **核实为真；控制器简报是错的**

`tests/unit/test_mapping_loader.py:36-44` 定义了**同样 7 张表**的 `COPIED_FILES`，
`:125-131` 的 `test_package_copies_are_identical_to_the_repo_files` 逐个断言
「包内副本 isfile」+「内容 == 仓库副本」。

→ **实施方说法成立，控制器简报「没有守卫」的说法不成立。**

### 5.6 新文件是有价值的加固，还是无谓重复？ —— **有价值的加固，不是重复**

不重复的理由有两条，其中第二条是简报未点出的**实质差异**：

1. **覆盖面不同**：wave-1 那条只守 7 张表；新文件守 9 份（多 `merged_sql.json` /
   `sql_merge.sql`），且新增了「仓库源存在性」「副本非空」两条独立断言，
   还有 3 条**闸自身有效性**的反例 —— wave-1 那条只有「等不等」一种失败模式。
2. **比较语义不同（关键）**：wave-1 用 `_read()`（`io.open(..., encoding='utf-8')`
   **文本模式**）读取 —— 文本模式做**通用换行转换**（CRLF → LF），
   因此**包内副本若被改成 CRLF，wave-1 的断言照样绿**；
   新文件用 `open(path, 'rb')` **二进制读 + bytes 相等**，CRLF 漂移会被抓住。
   在 WSL/Windows 混合开发的本仓库里，这不是理论差异。

**判定：保留双份是对的**（实施方也自陈「若审查认为重复，可把 wave-1 那条收敛为引用
本文件的检查器」）。唯一瑕疵是两份 `COPIED_FILES` 映射表**各自维护**，
新增副本要改两处 → 见 Minor-3。

---

## 6. 任务 2.2 逐条判定（合并 SQL 的运行时渲染）

### 6.1 「复制不改语义」有断言钉住吗 —— **有，而且是最强的一条**

`test_render_matches_the_generator_tool`（6 组参数：`(0,100,0)`、`(0,100,200)`、
`(60,100,0)`、`(60,50,100)`、`(120,100,300)`、`(300,100,0)`）把
`sqlgen.render_sql(...)` 与 `tools/build_sql_merge.py` 的
`load_spec() + pick() + combine()` 输出**逐字符**比对。

这条还顺带证明了 2.2 报告 §9 声称的 **DRY 偏离**（包内按 `refresh_seconds` 现算档位，
生成器按硬编码 `TIER_KEYS` 挑）**语义等价**：tier=60/120/300 三档都在参数矩阵里，
且 `test_tier_selection_by_refresh_seconds` 断言三档的 `sql_key` 清单与生成器的
`TIER_KEYS` **完全一致**（60 档 7 条 / 120 档 2 条 / 300 档 2 条 / 全量 11 条）。

### 6.2 路径解析是否依赖 cwd —— **不依赖**

```python
PACKAGE_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
```

基准是**模块自身绝对路径**，不是 cwd、也不是 `sys.path` 推导（在 zip/egg 部署下会失效）。
`test_data_paths_do_not_depend_on_cwd` 做 `os.chdir(tmp_path)` 后仍能加载 11 个视图，
并断言两个路径常量 `os.path.isabs`。**符合「常驻进程、任意目录启动」的要求。**

### 6.3 「第 1 页逐字符一致」是比整个文件还是比片段 —— **比整个文件**

比的是**整个文件**（含 5 行注释头 + SQL + 尾随换行），不是片段。
为此渲染器拆成两个入口：`render_sql`（纯 SQL 语句，发给平台的 `sql_text`）
与 `render_sql_document`（注释头 + SQL + 尾随换行，复现归档产物）。

**实测是否真的 identical：真的。** 2.2 报告给出 `document identical: True 3167 3167`
（3167 是**字符**数；文件 3339 **字节**，差值来自注释头里的中文 UTF-8 多字节 ——
`wc -c` 实测两侧均 3339 B，sha256 相同，无矛盾）。
测试实跑通过（见 §4）。

### 6.4 三道闸 —— **核实：都在，且各自非恒真**

| 闸 | 位置 | 守什么 | 失效场景（可证伪） |
|---|---|---|---|
| ① 包内渲染 ≡ 包内留档 | `test_first_page_is_byte_identical_to_the_archived_sql` | 视图定义（`merged_sql.json`）与留档（`sql_merge.sql`）分叉 | 改 `merged_sql.json` 的任一 `sql` 而不重生成留档 → 渲染结果变、留档不变 → 红 |
| ② 包内副本 ≡ 仓库根声明 | `test_mapping_data_sync.py`（9 份逐字节） | 「发布的声明」与「跑起来的声明」分叉 | 改仓库根任一副本而不复制进包 → 红；反之亦然 |
| ③ 生成器重跑 ≡ 仓库根归档产物 | `test_generator_reproduces_the_archived_artifact` | `merged_sql.json → sql_merge.sql` 的推导链（仓库根侧此前**无闸**） | 改仓库根 `merged_sql.json` 而不重跑生成器 → 重跑输出 ≠ 归档产物 → 红 |

**串联闭合性核对**（我独立推演）：

- 改**生成器**（`combine()`）而不重跑 → 闸③ 红（生成器输出 ≠ 归档产物）。
- 重跑了生成器（仓库根归档更新）而没改包内渲染器 → 闸② 红（包内副本 ≠ 仓库根）。
- 重跑了生成器且改了包内渲染器但没复制进包 → 闸② 红。
- 只改包内渲染器 → 闸① 红。
- 只改包内 `merged_sql.json` → 闸① 红 + 闸② 红。

**三道闸没有恒真的环节，也没有断链。** 实施方关于「`tools/build_sql_merge.py` 没有
`--check`」的说法**核实为真**：该脚本的 `argparse` 只有 `--tier` / `--limit` /
`--offset` / `--out` 四个参数，无 `--check`；对照 `tools/build_observed_columns.py`
确有 `--check`（`test_mapping_loader.py::test_observed_columns_generator_is_in_sync` 调它）。
→ 闸③ 是**补上此前无人看守的那一环**，属于净增益。

### 6.5 生产代码边界 —— **未被突破**

- `sqlgen` 的三个路径常量全部由 `__file__` 派生，指向**包内** `mapping/data/`；
  **不读**仓库根 `data/merged_sql/`（`load_spec(path=None)` 缺省走包内；
  显式传参仅供测试/离线排查使用，生产入口 `render_sql*` 不传）。
- `test_module_does_not_import_tools_or_repo_root_data` 用 AST 断言 `sqlgen` 无
  `tools`/`data` 顶层 import。**核实：成立**（`sqlgen` 只 import `json/logging/os/re/
  dataclasses/typing` + 包内 `..config` / `..errors`）。
- 仓库根 `tests/unit/test_package_boundary.py`（wave-0 建立）守的是 **import 层面**的边界，
  其 docstring 明写「映射数据目前仍从仓库根 `data/mapping/` 读取，搬进包内是
  wave-2a 任务 2.1 的交付物」→ 2.1 已把**运行时加载路径**（`loader.py` 的
  `PACKAGE_DATA_DIR`）切到包内。
  → **sqlgen 读的是包内 `mapping/data/merged_sql.json`，不是仓库根。符合要求。**
  （残留的 `table.py::MappingTable.load()` 默认值仍指向仓库根，见 Minor-1 / 待补 T-2。）

---

## 7. W-1 偏离判定（1.7 / 2.1 依赖倒置拆分）

**判定：偏离可接受。**

- 背景：wave-1 的 1.7 必须先有包内数据才能写 loader（否则无数据可加载）；
  2.1 的原范围（「复制六张表 + merged_sql.json + sql_merge.sql 进包 + 一致性测试」）
  因此被**依赖倒置**吃掉。这是波次编排缺陷，不是实施方擅自缩范围。
- 合同与 `tasks.md` **均未改动**（见 §9 越界核对），用户已裁决。
- 拆分后的实际交付**覆盖了 2.1 的全部实质内容**：两份新文件已复制进包（sha256 核实），
  一致性守卫覆盖 9/9 份副本 + 存在性 + 反例。
- **「七张表 vs 简报说的六张」的理由 —— 判定：结论可接受，但陈述的事实有一处不准。**
  - 可接受的部分：仓库根 `data/mapping/` 现存 6 张 JSON + `archive/`（5 张）+ `expected_values.json`；
    「六张表」的口径本身已随 2026-11-13 的归档动作失效。生产包纳入
    `metric_rules` / `alarm_rules` / `itscada_field_map` / `kafka_field_mapping` /
    `response_rules` / `open_items` + `data/customer_metric_mapping.json` 共 7 份，
    正是**当前运行时真正加载的文件**（`loader.py::FILES` 7 项 + `customer_metrics`）。
  - 不准的部分：`data/mapping/archive/README.md` 首行写着
    **「⚠️ 名字叫 archive，但它还在被生产代码加载」**，并说明这 5 张表
    「留作历史对照与**字段集合校验的基准**」；
    而包内 `tpops_kafka_collector/mapping/table.py:27-32` 至今仍声明
    `CATEGORY_MAPPING_FILES = {archive/…}`，`MappingTable.load()` 会去读它们。
    所以「装进包等于 ships 一份**已作废**的声明」这个说法**不准确**。
  - 但**结论仍然成立**：运行时加载路径（`loader.py`）**完全不读**归档表，
    归档表只被**遗留的** `MappingTable.load()`（生产包 `__init__` 仍导出，
    但生产代码路径不调用，只有仓库级测试 `tests/test_field_mapping.py` 等调用）依赖。
    → 不把它们装进生产包是**合理**的（避免把历史对照基准当成运行时声明），
    但必须把 `MappingTable.load()` 的仓库根依赖一并处理 → 登记为 Minor-1 / 待补 T-2。
- **偏离未被掩盖**：2.1 报告 §7 明确自陈这条偏离并说明理由，未静默缩范围。

---

## 8. 测试质量（可证伪性）逐条判断

判据：「存在一个小改动能让它失败吗？」

### 8.1 `tests/unit/test_mapping_data_sync.py`（33 条）

| 测试 | 可证伪性 | 备注 |
|---|---|---|
| `test_package_copy_exists[name]` | ✅ | 删文件 → 红；置空 → 红（`getsize>0`） |
| `test_repo_source_exists[name]` | ✅ | 移动仓库根声明源 → 红 |
| `test_package_copy_is_byte_identical_to_the_repo_source[name]` | ✅ | **二进制**比较，追加一个 `\n` 即红（wave-1 的文本模式比较抓不到 CRLF 漂移，见 §5.6） |
| `test_no_copy_problems_at_all` | ✅ | 整体扫描，非恒真（有 3 条反例佐证） |
| `test_a_missing_copy_is_reported` | ✅ | 反例，且断言了**具体**差异文案 |
| `test_a_drifted_copy_is_reported` | ✅ | 反例 |
| `test_a_drifted_text_copy_is_reported` | ✅ | 反例（文本副本） |
| `test_merged_sql_copy_keeps_views_tiers_and_paging` | ✅ | 断言 `batches` 恰 11 条、每条有 `sql`/`refresh_seconds`/`refresh_tier`、`paging.default_page_size>0`、`paging.views` 非空。**未**断言 `sql_key` 存在（见 Minor-5） |
| `test_sql_merge_copy_is_not_empty_text` | ✅ | 断言非空且含 `select` / `json_agg`（偏弱，但与上面的结构断言互补） |

**无恒真断言。**

### 8.2 `tests/unit/test_sqlgen.py`（24 条）

| 测试 | 可证伪性 | 备注 |
|---|---|---|
| `test_first_page_is_byte_identical_to_the_archived_sql` | ✅ | 整文件逐字符比较；渲染逻辑错 → 红 |
| `test_rendered_statement_is_the_tail_of_the_archived_file` | ✅ | 防「文档对、SQL 主体错」 |
| `test_rendered_statement_covers_all_eleven_views` | ✅ | 逐个视图别名 + 计数 11 |
| `test_spec_default_page_size_is_hundred` | ✅ | 断言声明里的默认页容量 |
| `test_paging_rules_match_the_declaration` | ✅ | 逐视图断言 `paged`/`strip_inner_limit`，含「未声明 → 固定视图」 |
| `test_offset_only_shifts_growable_views` | ✅ | offset=200；固定视图保留 `limit 20`/`limit 10` |
| `test_growable_views_strip_the_inner_limit_guard` | ✅ | `'limit 100 limit' not in sql` —— 抓 `limit 20 limit 100` 类语法错误 |
| `test_page_size_is_honoured` / `test_page_size_comes_from_config` / `test_config_default_page_size_is_used_when_env_absent` | ✅ | 参数化入口 |
| `test_tier_selection_by_refresh_seconds` | ✅ | 三档 `sql_key` 清单**逐条**钉死（与生成器 `TIER_KEYS` 等价的证据） |
| `test_unknown_tier_raises` | ✅ | 未知档 → `MappingError` |
| `test_render_matches_the_generator_tool`（6 组） | ✅ | **最强的一条**：与生成器逐字符比对，覆盖 tier/page_size/offset 三个维度 |
| `test_generator_reproduces_the_archived_artifact` | ✅ | 闸③：以 `--out <tmp>` 重跑，不碰仓库文件 |
| `test_invalid_page_size_raises` / `test_negative_offset_raises` | ✅ | 参数化 0/-1 |
| `test_data_paths_do_not_depend_on_cwd` | ✅ | `os.chdir` 后仍能加载 |
| `test_module_does_not_import_tools_or_repo_root_data` | ⚠️ 部分 | 只查 **import**，不查**路径字面量**；`sqlgen` 实际无仓库根路径字面量（我已人工核对），故当前无漏网，但该守卫**弱于**它想守的东西（见 Minor-6） |

**无恒真断言。** 唯一需要提醒的是「逐字符一致」这条**不是恒真**：
渲染器的注释头常量（`_HEADER_CREDIT` 等）是生成器注释头的**手工副本**，
若两边同时被改到一致，「文档==文档」仍然成立 —— 但那种情况下
闸③（生成器重跑 vs 仓库根归档）会因仓库根归档未同步而红。
**链上不存在可整体绕过且保持绿的组合。**

---

## 9. 越界与治理红线核对

| 核对项 | 结果 | 依据 |
|---|---|---|
| 是否改 `execution-contract.md` | **否** | diff 变更统计 7 个文件，无该文件 |
| 是否改 `tasks.md` | **否** | 同上 |
| 是否改仓库根 `data/mapping/*.json` | **否** | 同上；§5.1 sha256 显示 `data/merged_sql/*` 未变 |
| 是否改 `data/merged_sql/*`（生成物） | **否** | 同上（闸③ 因此仍成立） |
| 是否改 wave-0/wave-1 的既有文件 | **否** | 变更统计里除 2.3 的 `tpops/metrics.py` + `test_metrics_window.py`（属 wave-2b，不审）外，全是新增文件 |
| 是否 `git commit`（越权提交） | 否 | 提交由控制器统一执行（提交列表里的 3 个代码 commit 由控制器提交） |
| 是否签发 receipt / 改 `.spec-superflow.yaml` | 否 | 本审查不签发 |
| diff 混入别的波次 | **是**：`6d84ef6`（2.3，wave-2b）夹在中间 | 已按简报剔除，未计入本波次评价 |

**结论：无越界改动。** 新增的包内副本是**机械复制**（sha256 已证），
不是「手改生成物再反向同步仓库根」。

---

## 10. 分级清单

### Critical（0）
无。

### Important（0）
无。

### Minor（9）

| # | 级别 | 问题 | 位置 | 处置建议 |
|---|---|---|---|---|
| 1 | Minor | **遗留 API `MappingTable.load()` 的默认 `data_dir` 仍指向仓库根 `data/mapping/`**，且依赖 5 张 `archive/*.json` 与仓库根 `data/merged_sql/sql_merge_probe_result.json`。独立部署时该 API 会 `MappingError`（**响亮失败，不是静默出 0**）；生产运行路径（`loader.py`）不受影响。 | `tpops_kafka_collector/mapping/table.py:16,27-32,120,248` | 与待补 T-2 一并处理：改为包内默认，或从生产包 `__init__` 的公开 API 中移除（仅测试用则移出生产包） |
| 2 | Minor | **仓库根 `sql_merge.sql` 的生成物闸只在 pytest 里**，CI/`tools` 侧无 `--check`。三道闸中的闸③依赖 `tests/unit/test_sqlgen.py` 存在才会跑。 | `tools/build_sql_merge.py` | 待补 T-1：加 `--check`（对齐 `tools/build_observed_columns.py`） |
| 3 | Minor | **两份 `COPIED_FILES` 映射表各自维护**（`test_mapping_loader.py` 7 条 / `test_mapping_data_sync.py` 9 条），新增副本要改两处，易漂移。 | 两个测试文件 | 把 wave-1 那条收敛为引用新文件的检查器（实施方已自陈可做），或抽一个共享常量模块 |
| 4 | Minor | **包内 `mapping/README.md` 的「文件布局」段已过期**：仍以 `data/mapping/` 为主目录列出 5 张归档表为在用文件，且**未说明「运行时读包内副本，改仓库根声明源必须重新复制」**这一关键更新流程。 | `tpops_kafka_collector/mapping/README.md:15-30` | 待补 T-3：更新布局段 + 写明副本更新流程 |
| 5 | Minor | `test_merged_sql_copy_keeps_views_tiers_and_paging` 断言每条 batch 有 `sql`/`refresh_seconds`/`refresh_tier`，**未断言 `sql_key` 非空**（而 `sqlgen._build_view` 恰恰以 `sql_key` 为必需字段）。 | `tests/unit/test_mapping_data_sync.py` | 补一条 `assert batch.get('sql_key')` |
| 6 | Minor | `test_module_does_not_import_tools_or_repo_root_data` 只做 **import 层** AST 检查，**不查路径字面量**（例如 `os.path.join(REPO_ROOT, 'data', ...)` 这类绕过 import 的仓库根依赖不会被抓）。 | `tests/unit/test_sqlgen.py` | 可加一条「模块内不得出现 `REPO_ROOT` / `tools` / `data` 路径字面量」的断言，或在 `test_package_boundary.py` 里统一加一条全包路径字面量扫描 |
| 7 | Minor | **声明若出现 `paged=true` 但 `strip_inner_limit=false` 的视图，渲染会产生 `limit 20 limit 100` 语法错误**。当前声明安全（两个 paged 视图都是 `strip_inner_limit=true`），生成器行为相同，但**没有断言守住这个不变量**。 | `tpops_kafka_collector/mapping/sqlgen.py:_render_part` | 加断言：`all(v.paging.strip_inner_limit for v in views if v.paging.paged)` |
| 8 | Minor | **没有测试守住渲染结果的平台长度上限**（`sql_text` 10000 字符；平台 Agent 输出 102400 B 硬上限）。当前全量渲染 2818 字符，余量充足，但视图增加后无自动告警。 | `tests/unit/test_sqlgen.py` | 加断言 `len(rendered) < 10000`，或纳入 wave-2b 的命令中心通道检查 |
| 9 | Minor | **合同里的 review report 路径与实际交付路径不一致**：合同写 `.superpowers/reviews/wave-2a.md`，简报与仓库既有惯例（`wave-0-mapping-baseline.md` / `wave-1-skeleton-platform.md`）是 `.superpowers/sdd/reviews/wave-2a-mapping-data.md`。 | `execution-contract.md:163` vs 本文件 | 治理层面对齐一次（改合同或改惯例，二选一），本次按简报路径交付 |

---

## 11. 待补清单（登记进合同，本次不改 `tasks.md`）

| ID | 事项 | 理由 | 建议波次 |
|---|---|---|---|
| **T-1** | 给 `tools/build_sql_merge.py` 加 `--check` 模式（比对现有产物，不一致则非零退出），把闸③从 pytest 提升为 CI 级生成物闸 | 生成器确实无 `--check`；闸③目前只活在测试里 | wave-3（门禁）或 wave-4 前 |
| **T-2** | 处理 `MappingTable.load()` 的仓库根依赖：改为包内默认（含 5 张归档表与 probe 留档的取舍），**或**将其从生产包公开 API 中移除/降级为测试专用 | 2.1 的「包自洽」目标目前只对 `loader.py` 路径成立；该 API 仍导出在 `tpops_kafka_collector.mapping.__init__` | wave-3（入口装配前必须定） |
| **T-3** | 更新 `tpops_kafka_collector/mapping/README.md`：文件布局段按包内 `mapping/data/` 重写；写明「改了仓库根声明源必须重新复制进包」的流程（2.1 报告 §8.2 自陈的遗留） | 文档与实际结构已不符，且副本更新流程无处可查 | wave-4（交付文档） |
| **T-4** | 部署打包配置**必须包含 `tpops_kafka_collector/mapping/data/*.json` 与 `sql_merge.sql`**（仓库当前无 `setup.py` / `pyproject.toml` / `MANIFEST.in`，打包方式尚未固化） | 缺文件 → 启动期 `MappingError`，部署即失败 | wave-3（入口装配）或 wave-4 |
| **T-5** | 加两条小断言：①`batch['sql_key']` 非空；②所有 `paged=true` 的视图 `strip_inner_limit=true` | 堵住 Minor-5 / Minor-7 | 随下个波次顺带 |
| **T-6** | 加渲染长度上限断言（`< 10000` 字符） | Minor-8 | wave-2b 命令中心通道 |

---

## 12. 附：diff 之外的聚焦检查（风险 → 查了什么）

| # | 我能具体命名的风险 | 查了什么 | 结果 |
|---|---|---|---|
| R-1 | 实施方声称「1.7 的 `test_package_copies_are_identical_to_the_repo_files` 已经在守那 7 张表」——控制器简报说**相反** | 读 `tests/unit/test_mapping_loader.py:36-44,125-131` | **实施方为真**；且发现该断言是**文本模式**比较（抓不到 CRLF 漂移）→ 判定新文件非重复 |
| R-2 | 合同要求的「逐字符一致」可能只比了片段，或实际不 identical | 读 `sqlgen.render_sql_document` 实现 + `wc -c`/`sha256` 实测两份 `sql_merge.sql`（均 3339 B、hash 相同）+ 跑测试 | 比的是**整个文件**；**确实 identical** |
| R-3 | 「三道闸」可能有一道恒真，或断链 | 读 `tools/build_sql_merge.py` 的 `argparse`/`combine()`/`main()`，逐条推演四种「改了 A 忘了改 B」的组合 | 三道闸**都在**且**无恒真环节**、**无断链**；生成器确认**无 `--check`** |
| R-4 | 生产代码可能依赖仓库根 `tools/` 或 `data/`（尤其 `sqlgen` 读的是不是包内 `merged_sql.json`） | 读 `sqlgen.py` 三个路径常量 + `loader.py:44,110` 的 `PACKAGE_DATA_DIR` + `table.py:16` 的 `REPO_ROOT` + `tests/unit/test_package_boundary.py` 全文 | `sqlgen` **只读包内**；但发现 `table.py::MappingTable.load()` 仍指向仓库根 → Minor-1 / T-2 |
| R-5 | 「七张表 vs 五张归档表」的理由是否成立（归档表是否真的已作废） | 读 `data/mapping/archive/README.md` 首行与正文、`tpops_kafka_collector/mapping/table.py:27-32`、grep `MappingTable` 的调用方（仅仓库级测试调用，生产路径不调用） | 运行时不需要归档表 → **不装进包的结论成立**；但「已作废声明」的**说法不准** → §7 判定 |

---

## 13. 审查产物

- 本报告：`changes/tpops-kafka-collector/.superpowers/sdd/reviews/wave-2a-mapping-data.md`
- **未**签发 receipt，**未**改 `.spec-superflow.yaml` / `execution-contract.md` / `tasks.md`，
  **未**改任何被审查的代码。receipt 由控制器签发。
