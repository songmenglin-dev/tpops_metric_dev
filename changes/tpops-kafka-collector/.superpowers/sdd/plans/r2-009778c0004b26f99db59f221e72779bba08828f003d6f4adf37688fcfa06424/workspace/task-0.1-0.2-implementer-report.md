# 实现者报告 —— 任务 0.1 + 0.2

状态：DONE_WITH_CONCERNS

## 实现内容

**任务 0.1 —— 建包与复制映射层**（commit `edd7666`）
- `tpops_kafka_collector/__init__.py`：包 docstring，说明这是生产包。
- `tpops_kafka_collector/errors.py`：按开发方案 §5.7 的异常体系（`TpopsError(RuntimeError)` 基类；
  `AuthError`/`TransientError`/`ProtocolError`/`MappingError`/`ProduceError`）+ `is_retryable(exc)`
  编码 §5.7 的重试判定表（requests 的 `Timeout`/`ConnectionError` 按类名匹配，避免把 requests
  硬绑进异常层）。
- `tpops_kafka_collector/mapping/`：逐字复制 `tools/mappings/{table,rules,decode,message,nodes}.py`。
  唯一的包内导入修正：`table.py` 里本地定义的 `class MappingError(Exception)` 改为
  `from ..errors import MappingError`，使包内只有一个 `MappingError`，`DecodeError` 才能继承它。
- `tools/mappings/` 未动（`git diff HEAD~2 -- tools/mappings/` 为空）。未建 `models.py`（属 1.2）。

**任务 0.2 —— merged_views 段与合并形态解码**（commit `f13f137`）
- `tpops_kafka_collector/mapping/decode.py`：新增 `decode_merged(body, views)` → `MergedContexts(by_sql_key, rows_by_key)`；
  新增 `merge_pages(bodies, views)`（只对 `paged=true` 的视图做跨页拼接；固定视图只取第 1 页）。
  `DecodeError` 改为继承 `MappingError` → 不可重试。
- `tools/build_response_rules.py`：新增 `_build_merged_views()` 生成 `merged_views` 段
  （11 个视图，带 `json_column`/`paged`/`sql_ref`）。`paged` 取自 `merged_sql.json` 的
  `paging.views`（缺失 → `false`）。视图名与 `sql_merge.sql` 里实际存在的列别名
  （用括号配平扫描器，不是正则）及 `batches[].sql_key` 交叉校验，不一致 → `BuildError`（退出码 2）。
  已用注入假 `sql_key` 验证过 fail-loud。
- `data/mapping/response_rules.json` 已重新生成并提交；既有 11 个 `cmd_rows_*` shape 与 98 条规则
  保持逐字不变（diff 为纯新增：`91 insertions(+), 1 deletion(-)`，那 1 处删除只是 `]` → `],`）。
- `tests/test_merged_sql_mapping.py` —— 23 个测试。

## RED → GREEN 证据

**RED**（实现回退、测试保留时）：
```
13 failed, 2 passed, 7 errors in 0.37s
FAILED test_empty_view_decodes_to_empty_list[]           - AttributeError: module ... has no attribute 'decode_merged'
FAILED test_malformed_column_value_raises_decode_error   - AttributeError: ... 'decode_merged'
FAILED test_result_not_exactly_one_row_raises_decode_error[[]-0]
FAILED test_decode_error_is_mapping_error_and_not_retryable
FAILED test_merge_pages_concatenates_only_paged_views
ERROR  test_merged_views_covers_every_view_of_merged_sql  - response_rules.json 里没有 merged_views 段
```

**GREEN**：`tests/test_merged_sql_mapping.py -q` → `23 passed in 0.37s`

**全量**：`188 passed in 4.80s`（164 既有 + 23 新 + 1 额外）；`tests/test_py37_compat.py` → 36 passed；
wave-0 子集（merged_sql_mapping + rules + response_rules）→ 75 passed。
生成器幂等：重跑 `tools/build_response_rules.py` 后 `git diff` 为空；`--check` 报「是最新的」。

## 缺陷修复证明（wave 0 的意义所在）

把真实 `sql_merge_probe_result.json` 灌进新路径：
- `gaussdb.Xact_commit` → `10575876`（旧路径下是 `0`）
- `gaussdb.Total_pct` / `GAUSSDB.session_usage` → `0.84`（旧路径下是 `0.0`）
- `gaussdb.Deadlocks` → `0`（样本里确实没有死锁，不是解码失败）

## 提交

- `edd7666` `feat(mapping): 建 tpops_kafka_collector 生产包并复制映射层`
- `f13f137` `feat(mapping): 加 merged_views 段与合并形态解码`

## 顾虑

1. `tests/test_py37_compat.py` 尚未扫描 `tpops_kafka_collector/`（`SCAN_DIRS` 为 `('tools','tests','data')`）。
   实现者被要求不碰该文件，改为手工跑 `_check_file()` 遍历新包：**0 问题**。
   → **控制者已在审查前修复**：把 `tpops_kafka_collector` 加入 `SCAN_DIRS`（commit `f054d08`），
   `tests/test_py37_compat.py` → 44 passed。
2. 包目前运行时仍读仓库根的 `data/mapping/`（`table.py` 的 `REPO_ROOT` 与 `tools/` 那份解析一致）。
   这违反最终的「生产包不读仓库根」边界，但 wave-2a 的任务 2.1（映射数据进包）是指定修法，
   现在移动会超出本任务范围。
