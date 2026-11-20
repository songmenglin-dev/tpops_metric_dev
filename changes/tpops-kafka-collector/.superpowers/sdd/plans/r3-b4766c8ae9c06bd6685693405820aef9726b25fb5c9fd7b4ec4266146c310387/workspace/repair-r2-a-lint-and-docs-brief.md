# 修复轮次 2 · 任务 A —— wave-0 收尾：lint 归零 + 文档指向修正 + 空视图集不再静默

波次：`wave-0-mapping-baseline`　计划：sdd revision 3
上一轮复审报告：`changes/tpops-kafka-collector/.superpowers/sdd/reviews/wave-0-mapping-baseline-rereview.md`

本任务关闭复审报告里的 **I-2**（Important）与两个直接相关的 Minor。

---

## A-1（Important I-2）新生产包的 ruff 门禁归零

合同「验收检查」与「何时不得继续实现」已把门禁收窄为
`ruff check tpops_kafka_collector/` 必须干净。**用户已明确追认这次收窄**，
并要求现在把新生产包自己的 lint 清零，让门禁真正成立。

现状：`ruff check tpops_kafka_collector/` → **44 errors**，分布：

| 规则 | 数量 | 说明 |
|---|---|---|
| `UP031` | 27 | `'%s' % x` → 改用 f-string 或 `.format` |
| `UP009` | 8 | 多余的 `# -*- coding: utf-8 -*-` 声明 |
| `UP010` | 7 | 多余的 `from __future__ import print_function` |
| `UP020` | 1 | `io.open(...)` → `open(...)` |
| `E501` | 1 | `tpops_kafka_collector/mapping/message.py:70` 行长 111 > 100 |

配置文件 `ruff.toml`：`target-version = "py37"`，`line-length = 100`，
`select = ["E", "F", "W", "E501", "UP", "FA102"]`。

要求：

1. `ruff check tpops_kafka_collector/` 归零（退出码 0）。
2. **行为零变更**：所有改写必须语义等价。`%` 格式化改 f-string 时注意 `%s` 与 `%d`/`%r` 的区别，
   `%(name)s` 字典式格式化要展开成具名 f-string。
3. **Python 3.7.9 红线不得违反**：不得引入 3.8+ 语法。
   f-string 里的 `=` 说明符（`f"{x=}"`）是 3.8+，禁止。
4. `pytest tests/ -q` 保持全绿（当前基线 **238 passed**）。
5. `pytest tests/test_py37_compat.py -q` 保持通过（该测试扫全包的 3.7 语法）。
6. 44 条里大部分落在从 `tools/mappings/` 逐字复制过来的文件
   （`mapping/message.py`、`mapping/table.py`、`mapping/rules.py`、`mapping/nodes.py`、
   `mapping/decode.py`、`mapping/__init__.py`）。合同同时要求「映射层**复制**进包（不重写）」——
   这里的「不重写」指的是**不改映射语义**，格式与编码声明的机械改写不违反该约束；
   但你**不得**借机重构这些文件的逻辑或改变规则语义。

## A-2（Minor M-1）根目录文档指向已删除的 `tools/mappings/`

`tools/mappings/` 已随任务 0.7 删除（git 历史保留），但下列位置仍指向它：

- `CLAUDE.md:70`（目录结构树里的 `tools/mappings/  # 映射层执行代码`）
- `CLAUDE.md:257`（`tools/mappings/nodes.py`）
- `CLAUDE.md:284`（相关资料表里的 `tools/mappings/README.md`）
- `README.md:48`（架构图里的 `data/mapping/ + tools/mappings/`）
- `README.md:54`（`tools/mappings/`）

改成指向新的生产位置 `tpops_kafka_collector/mapping/`，并注明
**映射数据目前仍在仓库根 `data/mapping/`**（要等 wave 2a 的任务 2.1 才复制进包内），
避免读者误以为数据也在包内。

`tools/mappings/README.md` 这个文件本身已随目录删除——`CLAUDE.md:284` 那行需要指向新的对应文档；
若包内没有等价 README，就改成指向 `tools/mappings/README.md` 在 git 历史里的位置并注明，
或直接改写为描述包内映射层的新说明文字（你判断哪个更合适，在报告里说明理由）。

## A-3（Minor M-8）`decode_merged` 传入空视图集时不再静默返回零值

`tpops_kafka_collector/mapping/decode.py` 的 `decode_merged(body, views, now_ms=0)`：
`views` 是必填位置参数，但调用方一旦传入**空或过期**的视图集，当前会返回空的
`MergedContexts`（静默零值），而不是报错——这与本波次「不静默出 0」的目标直接冲突。

要求：传入空 `views` 时抛 `DecodeError`（`DecodeError` 是 `MappingError` 的子类，不可重试），
异常消息要说明是「视图集为空」而不是「响应体坏」。
新增一条测试断言这个行为，并确认现有 14 处 `decode_merged` 调用点都不受影响
（`tests/test_merged_sql_mapping.py` 里已有调用，另有 `merge_pages` 内部的自调用）。

---

## 边界

- **不要**碰 `tpops_kafka_collector/mapping/rules.py` 的规则语义、
  `data/mapping/*.json` 的内容、生成器 `tools/build_response_rules.py` 与
  `tools/build_itscada_mapping.py` 的逻辑。
- **不要**改 `changes/tpops-kafka-collector/execution-contract.md` 与 `tasks.md`
  （改了会让执行计划再次 stale）。
- **不要**改 `tpops_kafka_collector/mapping/columns.py`、`ids.py` 以外的新模块——
  那是并行任务 B 的领域（B 会新增文件；你不要动 B 的文件）。
- **不要**改仓库其余部分的 lint（357 条历史债务不在本变更范围）。
- 本任务只允许触及：`tpops_kafka_collector/` 下**已存在**的 8 个文件、
  `CLAUDE.md`、`README.md`，以及 `tests/` 下你新增/修改的测试文件。
