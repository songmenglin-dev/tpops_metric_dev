# 任务 2.1 报告 —— 映射数据防漂移（wave-2a）

> 变更：`tpops-kafka-collector` ｜ 波次：wave-2a（serial）｜ 任务：2.1
> 工作目录（worktree）：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`
> 分支状态：master（未提交，按约定由控制器统一提交）

---

## 1. 结论摘要

- 任务范围（按控制器裁决缩窄后）：**只剩「防漂移」** —— 复制 `merged_sql.json` /
  `sql_merge.sql` 进包 + 加一份覆盖全部 9 份副本的一致性守卫。
- **未发现既有漂移**：包内已有的 7 张映射表副本与仓库根声明源**全部逐字节一致**
  （sha256 逐一核对，见 §4）。wave-1 的 1.8 重生成 `kafka_field_mapping.json` 后，
  包内副本**已同步**，无需修复。
- 新增 3 个文件，未改动任何既有文件（`git status --short` 只有 3 条 `??`）。
- 全量测试 **752 passed**（其中本任务新增 33 条；既有 719 条全绿）。
- `ruff check tpops_kafka_collector/` → **All checks passed**（exit 0）。

---

## 2. 新增文件清单

| 文件 | 说明 |
|---|---|
| `tpops_kafka_collector/mapping/data/merged_sql.json` | 从 `data/merged_sql/merged_sql.json` **逐字节**复制（24 975 B）：11 个视图定义 + `refresh_seconds`/`refresh_tier` 刷新档位 + `paging` 段（`default_page_size=100` + 4 个视图的 `paged` 标注） |
| `tpops_kafka_collector/mapping/data/sql_merge.sql` | 从 `data/merged_sql/sql_merge.sql` **逐字节**复制（3 339 B，UTF-8 文本，含尾随换行） |
| `tests/unit/test_mapping_data_sync.py` | 新建，33 条测试：副本存在 / 副本非空 / 仓库源存在 / 逐字节一致 / 整体无差异 / 反例（删副本、改 JSON 副本、改 SQL 副本都必须被检查器报出）/ 结构完整性（11 视图 + 刷新档位 + paging 段） |

复制方式（`shutil.copyfile`，**未**走 `json.load` + `json.dump` 往返，避免改缩进/键序/尾随换行）：

```python
shutil.copyfile('data/merged_sql/merged_sql.json',
                'tpops_kafka_collector/mapping/data/merged_sql.json')
shutil.copyfile('data/merged_sql/sql_merge.sql',
                'tpops_kafka_collector/mapping/data/sql_merge.sql')
```

---

## 3. TDD 记录

### RED（先写测试，尚未复制文件时）

命令：`conda run -n tpops python -m pytest tests/unit/test_mapping_data_sync.py -q`

结果：**10 failed, 23 passed**

失败的断言（关键几条）：

| 失败用例 | 失败断言 |
|---|---|
| `test_package_copy_exists[merged_sql.json]` | `assert os.path.isfile(path), '包内缺少副本 %s'` → 包内副本不存在 |
| `test_package_copy_exists[sql_merge.sql]` | 同上 |
| `test_package_copy_is_byte_identical_to_the_repo_source[merged_sql.json]` | `read_bytes(package_path) == read_bytes(repo_path)` → 副本不存在 |
| `test_package_copy_is_byte_identical_to_the_repo_source[sql_merge.sql]` | 同上 |
| `test_no_copy_problems_at_all` | `find_copy_problems(PACKAGE_DATA_DIR) == []` → 实际返回含 `merged_sql.json: 包内副本不存在` / `sql_merge.sql: 包内副本不存在` |
| `test_a_missing_copy_is_reported` | `assert problems, '删掉副本后检查器仍然报「无差异」—— 闸是假的'` |
| `test_a_drifted_copy_is_reported` | `assert problems, '改坏副本后检查器仍然报「无差异」—— 闸是假的'` |
| `test_a_drifted_text_copy_is_reported` | 同上（SQL 文本副本） |
| `test_merged_sql_copy_keeps_views_tiers_and_paging` | `FileNotFoundError: .../mapping/data/merged_sql.json` |
| `test_sql_merge_copy_is_not_empty_text` | `FileNotFoundError: .../mapping/data/sql_merge.sql` |

> 23 passed 的部分即**包内已有的 7 张映射表**（存在性 + 逐字节一致）—— 说明这 7 份
> 本来就没漂移，RED 精确命中了新复制的那 2 个文件。

### GREEN（补齐复制后）

- 单文件：`33 passed in 0.75s`
- 全量：`752 passed in 11.76s`
  （既有 719 条：`pytest -q --ignore=tests/unit/test_mapping_data_sync.py` → `719 passed`
  —— 简报里写的基线 718 已过期 1 条，属 wave-1 收尾新增，非本任务所致）

---

## 4. 包内 9 份副本 vs 仓库根声明源（sha256 逐一核对）

**全部 IDENTICAL —— 逐个列出：**

| # | 包内副本 | 仓库根声明源 | sha256（前 16 位） | 字节数 | 结果 |
|---|---|---|---|---|---|
| 1 | `mapping/data/metric_rules.json` | `data/mapping/metric_rules.json` | `7b194341125e7ee3` | 121 530 | **IDENTICAL** |
| 2 | `mapping/data/alarm_rules.json` | `data/mapping/alarm_rules.json` | `dd1df0c5519f4d35` | 111 077 | **IDENTICAL** |
| 3 | `mapping/data/itscada_field_map.json` | `data/mapping/itscada_field_map.json` | `2e12aca9bcbb3b99` | 4 020 | **IDENTICAL** |
| 4 | `mapping/data/kafka_field_mapping.json` | `data/mapping/kafka_field_mapping.json` | `b55ee6aff7090ed8` | 465 929 | **IDENTICAL** |
| 5 | `mapping/data/response_rules.json` | `data/mapping/response_rules.json` | `56b06bad3a2f7144` | 263 546 | **IDENTICAL** |
| 6 | `mapping/data/open_items.json` | `data/mapping/open_items.json` | `f0fb3b41f5a705e6` | 3 196 | **IDENTICAL** |
| 7 | `mapping/data/customer_metric_mapping.json` | `data/customer_metric_mapping.json` | `c02a5dc526d32c0f` | 43 756 | **IDENTICAL** |
| 8 | `mapping/data/merged_sql.json`（本任务新增） | `data/merged_sql/merged_sql.json` | `f0acc8191a796036` | 24 975 | **IDENTICAL** |
| 9 | `mapping/data/sql_merge.sql`（本任务新增） | `data/merged_sql/sql_merge.sql` | `306ab72197e99a64` | 3 339 | **IDENTICAL** |

关于「1.8 重生成 `kafka_field_mapping.json` 后是否漂了」的专项核实：
**没有漂**。第 4 行 sha256 双方一致 —— 1.8 改仓库根时包内副本也同步更新了
（或生成器输出本就未变）。**无需修复。**

> 附注：`customer_metric_mapping.json` 的声明源在 `data/` 下**不在** `data/mapping/` 下
> —— 与 `test_mapping_loader.py` 里既有的 `COPIED_FILES` 映射一致，本文件沿用同一约定。

**不属于「副本」的包内文件**：`mapping/data/observed_columns.json` —— 它是从实测留档
**现算**的声明（`tools/build_observed_columns.py` 生成），不是仓库文件的副本，
由 `test_mapping_loader.py::test_observed_columns_match_the_generator_output`
与生成器 `--check` 双重守住。本任务不重复覆盖，也不把它列入副本表。

---

## 5. 反例如何验证有效

守卫的核心是 `find_copy_problems(package_dir, repo_root)` —— 返回差异描述列表，
空列表才表示对得上。**有效性靠三层证据**：

### (a) 单元级反例（沙箱内自证闸不是空壳）
`test_a_missing_copy_is_reported` / `test_a_drifted_copy_is_reported` /
`test_a_drifted_text_copy_is_reported` 用 `shutil.copytree` 把包内数据目录复制到
`tmp_path`，然后分别**删文件**、**在 JSON 副本尾部追加一个 `\n`**、
**把 SQL 副本里的 `limit 100` 改成 `limit 999`**，断言检查器必须报出差异。
若检查器写成恒返回 `[]`，这三条立即红。

### (b) 端到端反例（真实包内文件被临时改坏）
直接在真实副本上做了一次「改坏 → 跑测试 → 恢复」：

```
# 改坏 metric_rules.json（追加一个换行）
FAILED test_package_copy_is_byte_identical_to_the_repo_source[metric_rules.json]
FAILED test_no_copy_problems_at_all
2 failed, 31 passed

# 恢复后改坏 sql_merge.sql（limit 100 → limit 999）
FAILED test_package_copy_is_byte_identical_to_the_repo_source[sql_merge.sql]
FAILED test_no_copy_problems_at_all
2 failed, 31 passed

# 恢复
33 passed
```

改坏任一副本 → 测试立刻红；恢复 → 立刻绿。**闸确实在守。**
（恢复后已用 sha256 复核，两个文件回到 §4 表中的原值。）

### (c) 存在性断言（防「压根没复制」）
`test_package_copy_exists`（`isfile` + `getsize() > 0`）与 `test_repo_source_exists`
分别守「副本在」和「副本有可对账对象」。这正是简报强调的那一条：
若只写「两边相等」的断言，副本没复制过来时会因 `FileNotFoundError` 而**报错**，
而不是「绿」—— 但显式的存在性断言让失败信息直接指向「包内缺少副本 X」，
排查成本更低，且在副本为空文件时也能红。

---

## 6. 测试设计要点（供 2.2 参考）

- `COPIED_FILES` 是**包内文件名 → 仓库根相对路径**的显式映射，新增副本只需加一行。
- 比较用**二进制读**（`open(path, 'rb')` + `bytes` 相等），不做换行转换，
  缩进/键序/尾随换行/编码全部纳入「逐字节」语义。
- 额外的结构断言 `test_merged_sql_copy_keeps_views_tiers_and_paging` 断言
  `paging` 段存在且 `default_page_size > 0`、`paging.views` 非空、
  `batches` 恰好 11 条且每条都有 `sql` / `refresh_seconds` / `refresh_tier`。
  **2.2 的 `sqlgen` 直接依赖这些字段**，副本一旦丢段，运行期才发现就晚了。

---

## 7. 偏离简报之处及原因

1. **简报称「映射表本身的副本一致性目前没有守卫」，与事实不符。**
   `tests/unit/test_mapping_loader.py:118` 已有
   `test_package_copies_are_identical_to_the_repo_files`，
   用同一份 `COPIED_FILES` 映射守着 7 张表的逐字节一致。
   **未删改该用例**（属 wave-1 文件，本任务不碰）。
   本任务新建的 `test_mapping_data_sync.py` 与它**有重叠但不是简单复制**：
   新文件额外覆盖了 ① 新复制的 2 个 merged_sql 文件；② 存在性与非空断言；
   ③ **仓库源存在性**（双向对账）；④ 基于 `find_copy_problems` 的
   「闸自身有效性」反例（删除 / 改 JSON / 改 SQL 三种）。
   保留两处冗余是刻意的：wave-1 的用例只做「等不等」，本文件做「等不等 **且** 缺了要红 **且** 闸本身不是空壳」。
   若后续审查认为重复，可把 wave-1 那条收敛为引用本文件的检查器（未擅自改动）。

2. **复制动作用一次性 `shutil.copyfile` 命令行执行，没有落成脚本。**
   理由：`data/merged_sql/merged_sql.json` 与 `sql_merge.sql` 本身是
   `tools/build_sql_merge.py` 的生成物，「复制」是纯机械搬运；
   固化一个复制脚本会引入新的生成链路与漂移面。防漂移由**一致性测试**承担
   （这正是设计决策 9 的分工）。若后续需要在 CI 里自动刷新副本，
   建议在 `tools/build_sql_merge.py` 上加 `--copy-to-package` 开关（**本任务未做**）。

3. **`ruff` 对新测试文件报 13 条 UP009/UP010/UP031，未修。**
   理由：`ruff check tpops_kafka_collector/`（合同硬约束）**为 0**，已通过；
   `tests/` 不在合同约束范围内，且仓库既有测试文件（如
   `test_mapping_loader.py`、`test_watermark.py`）用的是同一套
   `# -*- coding: utf-8 -*-` + `from __future__ import absolute_import` + `%` 格式化风格。
   为与既有测试风格保持一致而保留。

---

## 8. 遗留问题

1. **`sql_merge.sql` 的生成物没有「生成器 --check」闸。**
   `merged_sql.json` → `sql_merge.sql` 的推导关系目前**只靠**本任务加的
   「包内副本 == 仓库根留档」保证；**仓库根那份** `sql_merge.sql` 是否与
   `merged_sql.json` 同步，靠 `tools/build_sql_merge.py`（未核其是否有 `--check`）。
   2.2 会加「渲染出的第 1 页 == `data/merged_sql/sql_merge.sql` 逐字符一致」的断言，
   那时这条链就闭合了。**建议 2.2 顺带确认 `build_sql_merge.py` 是否有 `--check` 模式。**

2. **包内副本的更新流程未文档化。**
   仓库根声明源变了 → 必须手工重新复制到包内 → 否则一致性测试红。
   本任务**未**改 `tools/mappings/README.md` 或包内 README（不在本任务文件所有权内）。
   **建议在 2.2 或 wave-2b 收尾时，把「改了声明源要重新复制」的步骤写进
   `tools/mappings/README.md`。**

3. **`data/mapping/expected_values.json`（883 KB）没有进包。**
   它是**测试**期望值（`tests/test_expected_values.py` 从留档现算校验），
   不是运行时依赖，不属「生产用副本」，本任务有意不纳入。

4. **`data/mapping/archive/` 下的 5 张历史映射表**（`metric_mapping_*.json`、
   `alarm_mapping.json`）是旧版留档，包内**没有**对应副本，
   也**不在**新映射表的消费链路上（新版由 `metric_rules.json` /
   `alarm_rules.json` 承载）。有意不纳入，如后续确认仍需运行时加载再补。

---

## 9. 验收清单

| 项 | 结果 |
|---|---|
| `merged_sql.json` 逐字节复制进包（含 paging 段） | ✅ sha256 一致 |
| `sql_merge.sql` 逐字节复制进包（UTF-8 + 尾随换行） | ✅ sha256 一致 |
| 新增 `tests/unit/test_mapping_data_sync.py` | ✅ 33 条 |
| 覆盖**全部 9 份**副本（7 张既有表 + 2 个新文件） | ✅ |
| 断言「副本存在」+「内容与声明一致」 | ✅ 存在性 / 非空 / 仓库源存在 / 逐字节相等 四条独立断言 |
| 反例有效（改副本 → 测试红） | ✅ 单元级 3 条 + 端到端实测 2 次改坏/恢复 |
| 未改仓库根 `data/mapping/`、`data/merged_sql/` | ✅ `git status` 仅 3 条新增 |
| 未改包内既有 7 张副本 | ✅ |
| 未改 wave-0/1 已有文件 | ✅ |
| 未 `git commit` | ✅ |
| Python 3.7.9 兼容 | ✅ 无 3.8+ 语法（`typing.List` 等；测试文件用 `open()` 二进制模式） |
| `ruff check tpops_kafka_collector/` | ✅ exit 0，All checks passed |
| 全量测试 | ✅ 752 passed |
