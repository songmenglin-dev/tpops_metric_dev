# Wave 0 任务级审查 —— 任务 0.1 + 0.2（base 357ece4 / head f054d08）

| 维度 | 结论 |
|---|---|
| 规格符合性 | **PASS** |
| 代码质量 | **FAIL**（1 个 Important） |
| 波次结论 | **fail** |

## 规格符合性 —— PASS

- **「合并 SQL 返回形态的解码」**：PASS。用真实留档 `data/merged_sql/sql_merge_probe_result.json` 验证：
  `stat_database` → 9 行真实 `datname`/`xact_commit`，`stat_user_tables` → 68 行，`xact_commit` 求和 = **10575876**
  （旧路径静默出 0 的缺陷确实修好了）。空视图：`None` 与 `''`/空白都返回 `()`，
  且 `text.strip()` 检查在 `json.loads` **之前**——永远不会走到 `json.loads('')`。
  另处理了 `'null'` 字符串形态（`json.loads('null') → None → ()`）。
  坏 JSON → `DecodeError`；行数不等于 1 → `DecodeError` 并说明期望 1 / 实际 N。
- **「解码错误的异常归属」**：PASS。`DecodeError(MappingError)`；包内 `MappingError` **只有一个定义**
  （`errors.py:45`），`decode.py` 与 `mapping/table.py` 都从 `..errors` 导入，**无重复/遮蔽定义**。
  `is_retryable(DecodeError(...))` → `False`。
- **「既有 per-view 形态保持可解」**：PASS。AST 级比对：`tools/mappings/decode.py` 与包内版本的
  **每一个共有函数逐字节相同**（`decode` / `_rows_from_result` / `_series_from_metric_infos` /
  `_dig` / `_as_rows`），只有新增的合并路径函数是差异。
  `tools/mappings/*` 未被 diff 触及。`data/mapping/response_rules.json` 只有**一个 hunk**（追加
  `merged_views` 段，`]` → `],`），11 个 `cmd_rows_*` shape 与 98 条规则未被触及。
  `--check` 报「是最新的」（幂等）。
- **「分页结果的拼接」**：PASS。`merge_pages` 只对 `paged=true` 的视图拼接；固定视图只取 `per_page[0]`。
  `_is_paged` 缺失时默认 `False`。生成的 `merged_views` 11 条，`paged=true` 恰为
  `stat_user_tables` 与 `dbstat_sum`，与 `merged_sql.json` 的 `paging.views` 吻合。
- **生成器交叉校验 fail-loudly**：PASS。`_build_merged_views` 用括号配平扫描器（非正则）解析
  `sql_merge.sql` 的列别名，与 `merged_sql.json` 的 `batches[].sql_key` 比对，不一致 → `BuildError`
  → `sys.exit(2)`。审查者注入假 `sql_key` 与删除真实视图**两种情况都被拒绝**，退出码非 0。

## 代码质量 —— FAIL

### Important

**1. `tpops_kafka_collector/errors.py:77` —— `is_retryable` 把 requests 的超时/连接异常的子类判成不可重试。**

```python
return type(exc).__name__ in ('Timeout', 'ConnectionError')
```

类名**精确相等**只匹配两个基类，而 requests 实际抛出的是子类：

| 异常 | `is_retryable` | 应为 |
|---|---|---|
| `requests.exceptions.Timeout` | True | True |
| `requests.exceptions.ConnectionError` | True | True |
| `requests.exceptions.ReadTimeout` | **False** | True |
| `requests.exceptions.ConnectTimeout` | **False** | True |

`ReadTimeout` / `ConnectTimeout` 正是 requests 在读超时 / 连接超时时抛的两个异常，两者分别继承
`Timeout` / `ConnectionError`。按现写法，生产上一次瞬时读超时会被判为不可重试，直接中止本轮而不是退避
——与 §5.7 重试表正好相反。无测试覆盖（只测了 `DecodeError`）。

**修法（一行，不新增依赖）**：
`return any(base.__name__ in ('Timeout', 'ConnectionError') for base in type(exc).__mro__)`
（走 MRO 的类名，既能覆盖子类，又不把 requests 硬绑进异常层。）

### Minor

2. `tpops_kafka_collector/mapping/__init__.py:30` —— per-view 的 `decode` **没有导出**，
   而 `decode_merged` / `merge_pages` 导出了。旧路径是既有 shape 的生产路径，导出不对称会诱发从错的地方导入。
3. `models.py` 未创建（属任务 1.2）——可以接受（空壳无价值），但 **0.1 与 1.2 都声称拥有 `errors.py`**，
   归属重叠需在 1.2 里明确（1.2 扩展而非重建）。
4. `ruff`（仓库 `ruff.toml`，选了 `F`）在新包报 2 个新 **F401**：`errors.py:16` 的 `typing.Tuple`、
   `decode.py:32` 的 `typing.Dict`（都只在 `# type:` 注释里用到，ruff 不计）。`tools/mappings` 基线是 0 个 F401。
5. `decode_merged(body, views={})` 返回空的 `MergedContexts` 而不抛错——传入空/过期视图集时
   调用方拿到静默零值而不是错误。
6. `sql_ref.catalog` 硬编码为仓库相对路径 `'data/merged_sql/sql_merge.sql'`（`build_response_rules.py:553`）。
   映射数据搬进包（wave-2a 的 2.1/2.2）后该路径会失效，需重新生成——登记，勿忘。
7. `RuleContext` / `MergedContexts` 是 `frozen=True` 但持有可变 dict，且所有合并上下文共享同一个
   `rows_by_key` dict 对象——不可变性是浅层的（与既有 `tools/mappings` 一致，非回归）。
8. `tests/test_merged_sql_mapping.py:382` 的 `test_existing_shapes_and_rules_are_untouched`
   只数了 98 条规则、查了两个字段，抓不到规则体被改。该需求的真正证据是 git diff（纯新增 hunk）
   加 `test_package_per_view_path_replays_every_command_center_sample`（≥80 条规则、行数与留档比对）——
   后者才是强证据，且成立。
9. 生成器只对 `sql_merge.sql` + `merged_sql.json` 校验，**未**对实测留档
   （`sql_merge_probe_result.json`）校验。审查者确认二者今天一致，且「启动期映射校验」规格把留档定为权威——
   但那正是任务 0.5 的交付物，属**已登记的缺口**，非 0.2 的缺陷。
10. 新模块里的 `from __future__ import print_function` 是死代码（沿袭 `tools/` 的风格）。

## 测试质量（可证伪性）

23 个新测试总体上以可观察行为立论，期望值独立，审查者能对每个重要测试指出一个「会让它失败的小改动」：
把整个单行当行列表返回（破 `test_decoded_rows_are_dicts_not_json_strings`）、
对 `''` 调用 `json.loads`（破 `test_empty_view_decodes_to_empty_list`）、
把合并行当行列表拼接（破 `test_merge_pages_concatenates_only_paged_views`）、
去掉 `sql_merge.sql`↔`sql_key` 交叉校验（破 `test_merged_views_covers_every_view_of_merged_sql`）。
per-view 回放测试对照的是留档里的 `observed.row_count`，不是从实现反推——正确的 oracle 形态。

两处弱项：`test_existing_shapes_and_rules_are_untouched`（见 Minor 8）、
`test_decoded_rows_are_dicts_not_json_strings`（第 215 行）的第二个断言
`not isinstance(row, dict) or not all(...)` 在第一个分支上是恒真的，实际只检查了后半句。

审查者未重跑全量（实现者的运行已覆盖）。只跑了两个聚焦子集以回答具体疑问：
`tests/test_py37_compat.py` + `tests/test_merged_sql_mapping.py`（67 passed，疑问：3.7 门禁是否真的覆盖新包、
`ast.parse(feature_version=(3,7))` 是否通过）与
`tests/test_response_rules.py test_generated_rules.py test_rules.py`（56 passed，疑问：重新生成的
`response_rules.json` 是否破坏既有生成器/规则测试）。另对新包跑了 `ruff` 与 3.7 语法解析（无 3.8+ 语法、
无超 50 行函数、无超 400 行文件）。

## 波次级说明（影响 receipt）

本次 diff 只覆盖任务 0.1 与 0.2。wave-0 的完成标准**未达成**：0.3（告警幂等键）、
0.5（合并形态对留档的列校验）、0.6（开发方案契约同步）、0.7（切依赖 + `tests/unit/test_package_boundary.py`，
该文件尚不存在）仍在进行中。在此 head 上签发波次级 `pass` 等于断言一个不存在的波次状态。
任务 0.4 的实质（跨视图填充 `ctx.rows_by_key`）已随本次改动落地。

**结论：`fail`** —— 由 Important 级 `is_retryable` 缺陷驱动（errors.py:77），
其次是波次尚未完成。任务 0.1/0.2 本身满足各自简报与四条绑定规格。
