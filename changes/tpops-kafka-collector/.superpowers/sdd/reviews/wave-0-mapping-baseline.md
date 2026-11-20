# Wave 0 审查报告（revision 3）

- 变更：`changes/tpops-kafka-collector`，波次 `wave-0-mapping-baseline`
- 计划：**revision 3**（`execution_plan_hash = sha256:b4766c8ae9c06bd6685693405820aef9726b25fb5c9fd7b4ec4266146c310387`）
- Base：`357ece4`（Wave 0 起始 SHA） / Head：`3a04aa5`
- 审查日期：2026-11-20
- 审查者：独立审查者（第二遍，revision 3）
- diff：`…/plans/r3-b4766c8ae9c06bd6685693405820aef9726b25fb5c9fd7b4ec4266146c310387/workspace/review-wave0-r3.diff`（16105 行 / 78 文件）

## 结论

**verdict：pass**

Critical 0 · Important 0 · Minor 7（均不阻塞，含 2 项需带入后续波次）

理由：上一轮复审的两条 plan-mandated Important **均已关闭，且由审查者独立取证**（未采信实施方报告）；
合同 Wave 0 的 5 条完成标准**逐条对照通过**；本次 diff **未出现新的合同 / tasks.md 改动**；
范围越界检查通过（`data/mapping/*.json` 全部可由生成器逐字节复现）。

审查者自跑的验证（在 worktree 内执行，未修改任何被审查文件）：

| 命令 | 结果 |
|---|---|
| `ruff check tpops_kafka_collector/` | **All checks passed**（退出码 0） |
| `ruff check .`（对照） | 305 errors（历史技术债，按合同不在本波次门禁内） |
| `pytest tests/ -q` | **344 passed** |
| `pytest tests/test_merged_sql_mapping.py tests/test_rules.py tests/test_response_rules.py -q` | **109 passed** |
| `pytest tests/unit/test_package_boundary.py -q` | **14 passed** |
| `pytest tests/test_expected_values.py -q` | **100 passed** |
| `pytest tests/test_py37_compat.py -q` | **44 passed** |
| `pytest tests/ --cov=tpops_kafka_collector` | 344 passed，TOTAL **90%**（映射层 82%~95%） |
| 生成器幂等（`/tmp` 副本上跑 4 个生成器后 `diff -rq data/mapping`） | **零差异** |
| 变异测试（在 `/tmp` 副本上改引擎，7 个变异体） | **6 个被抓住**，1 个逃逸（见 §4a） |

---

## 1. 上一轮复审 Important 的关闭判定

### I-1 ✅ **已关闭**（合同完成标准第 2 条「82 个 B 类指标在合并形态下的值等于期望值表」）

独立证据（不依赖实施方报告）：

1. **期望值推导器与规则引擎确系两套实现。**
   `tools/expected_deriver.py`（新增 398 行）的 import 只有 `datetime` / `json` / `typing` ——
   `grep -n "tpops_kafka_collector" tools/expected_deriver.py` 仅命中 docstring 里的**说明文字**，
   **无任何生产包 import**；连合并留档的解码都自己重写（`parse_archive` / `_view_rows`），
   不借 `mapping/decode.py`。求值按 `value_rule.params` 的**字面意思**重新实现，
   且在 docstring 里逐条注明口径依据（规格行号 / `response_rules.json` 的 note），**不是抄引擎**。
2. **期望值来自实测留档的原始行。**
   `tools/build_expected_values.py` 读 `data/merged_sql/sql_merge_probe_result.json`（3 个 master 节点），
   逐视图解出原始行后交给 deriver 求值；表头 `sources` 段记录了三个输入文件的相对路径。
3. **82 条逐条断言，覆盖率真的是 82。**
   `tests/test_expected_values.py` 用 `@pytest.mark.parametrize('metric_code', B_CODES)` 对 82 条
   **逐条**断言，且**逐节点**比对（3 个节点 → 246 组值相等断言）。
   另有 4 条完整性用例：`test_b_class_metric_count_is_82`、`test_expected_table_covers_every_b_class_metric`
   （`sorted(expected) == B_CODES`）、`test_expected_table_has_no_extra_entries`（表里不得有 B 类之外的指标）、
   `test_expected_table_records_the_rule_type`。
   实测：`data/mapping/metric_rules.json` 里 `source == command_center_sql` 的规则数为
   `raw 54 + aggregate 12 + counter_delta 5 + raw_filtered 4 + raw_rows 3 + top_n 2 + ratio_across 2 = 82` ✓，
   `expected_values.json` 的 `metric_count = 82` ✓。
4. **守卫不是恒真的 —— 审查者自跑变异测试。**
   在 `/tmp/w0`（仓库副本）上逐个改 `tpops_kafka_collector/mapping/rules.py` 后跑
   `pytest tests/test_expected_values.py`：

   | 变异 | 结果 |
   |---|---|
   | `aggregate` 求和 → 取最大 | **2 failed** / 98 passed |
   | `counter_delta` 首轮无基线 → 产出 `0.0`（假数据） | **10 failed** / 90 passed |
   | `match_filter` 去掉 `.lower()`（大小写） | **2 failed** / 98 passed |
   | `raw_filtered` 完全忽略 filter | **3 failed** / 97 passed |
   | `ratio_across` 保留 2 位 → 6 位小数 | 被抓住（failed） |
   | `raw` 的 `sum` 口径 → 取首行 | 被抓住（failed） |
   | `raw_filtered` 取首行 → 取末行 | **100 passed（未抓住）** ← 见 §4a |

   即：**这类「列名对、聚合口径错」的缺陷（正是本波次的立项缺陷）现在被守住了**。
5. **`counter_delta` 的 5 条按合同边界处理**：
   - 首轮无基线 → **不产出假数据**：`test_counter_delta_first_round_has_no_baseline`
     断言每条产出对的**值都是 `None`**（不是 0），且与期望值表一致；
   - 第 2 轮差分用**合成留档**验：`test_counter_delta_second_round_rate_equals_expected`
     用 `COUNTER_DELTA = 1000` 的加量计划合成第 2 轮返回体，断言速率 == `deltas[column] / elapsed_seconds`；
     期望速率由**测试内独立算出**（没有复用生成器的构造代码）；
   - 计数归零（实例重启）另有 `tests/test_rules.py:211` 覆盖（差分丢弃、不产生负速率）。
6. **期望值表是生成物且可复现**：`tools/build_expected_values.py --check` → 「expected_values.json 是最新的（82 条）」；
   `tests/test_expected_values.py::test_file_is_up_to_date` 直接比对「当前文件 == 生成器输出」。
7. **`state_contains` 的留档盲区已被补上合成守卫**（控制器点名的 (b)）：
   `test_state_contains_matches_rows_with_the_keyword` 用 4 条合成行，其中故意含
   `'WAITING ON LOCK'`（大写）—— 变异 `str(actual).lower()` → `str(actual)` 现在 **2 failed**（审查者实测）。
   即 (b) **已关闭**。

### I-2 ✅ **已关闭**（合同自设的 `ruff check tpops_kafka_collector/` 门禁）

- 审查者实测：`ruff check tpops_kafka_collector/` → **All checks passed**（退出码 0）。
- 上一轮的 44 条（UP031×27 / UP009×8 / UP010×7 / UP020×1 / E501×1）已全部清零。
- 改动性质：**纯等价改写**（逐字核对了 diff 中的每处 hunk）：
  `%` 格式化 → f-string；删除多余的 `# -*- coding: utf-8 -*-` 与 `from __future__ import print_function`；
  `io.open` → `open`；折行。**无行为变更**（例如
  `'%s=%s' % (key, dimensions[key])` → `f'{key}={dimensions[key]!s}'` 语义一致）。
- **未把仓库其余部分的 lint 债务带进本波次**：`ruff check .` 仍为 305 errors，历史债未被顺手改动
  （diff 中不含对 `tools/` 旧脚本的大面积 lint 重写）。

---

## 2. 合同 Wave 0 完成标准逐条判定

| # | 完成标准（`execution-contract.md` §Wave 0） | 判定 | 依据 |
|---|---|---|---|
| 1 | `pytest tests/test_merged_sql_mapping.py tests/test_rules.py tests/test_response_rules.py -q` 全绿 | ✅ 满足 | 审查者实测 **109 passed** |
| 2 | **82 个 B 类指标在合并形态下的值等于期望值表** | ✅ 满足 | §1 的 I-1：82 条**逐条**、逐节点值相等；期望值由**独立推导器**从**实测留档原始行**推出；变异测试证明守卫会红（唯一逃逸见 §4a，定性为 Minor） |
| 3 | `pytest tests/ -q` 全绿（含既有 164 个用例，import 已切到包内） | ✅ 满足 | 审查者实测 **344 passed**（238 基线 + 3 + 103，与 `progress.md` 的计数链一致；既有 164 用例均在，无删除） |
| 4 | `pytest tests/unit/test_package_boundary.py -q` 通过（无 tools / data import） | ✅ 满足 | **14 passed**；AST 级逐模块断言 + `tools/mappings/` 已删除 |
| 5 | 开发方案 §3.1 / §3.7 / §4.5.4 与包内实现口径一致 | ✅ 满足 | 上一轮复审已逐节核对通过（§3.1 指标 7 字段 / 告警 13 字段、§3.7 与 §4.5.4 幂等键含 status、§4.4 合并 SQL 与 102400 B、§5.5 配置默认、§5.6.4 留档为权威来源、§6.2 回归样本）；本波次未再改动该文档的这几节 |

> 结论：**5/5 满足**。合同 Wave 0 的完成标准全部达成。

### 附带：合同「验收检查」里与 wave 0 相关的条目

| 条目 | 判定 | 说明 |
|---|---|---|
| `pytest tests/test_py37_compat.py -q` | ✅ | 44 passed（生产包已纳入扫描目录） |
| `ruff check tpops_kafka_collector/` 干净 | ✅ | 0 error |
| `pytest --cov=tpops_kafka_collector --cov-fail-under=80 -q` | ✅（整体） | TOTAL 90%；但**映射层与规则引擎 ≥95% 尚未达成**（rules.py 92%、decode.py 82%、columns.py 88%）—— 该条是 **wave 3** 的完成标准，此处仅作**前置提醒**（见 Minor-6） |
| 回归层 98/98 通过 | ⏭ 不适用 | 属 wave 2b（任务 3.4/2b 回归层） |
| 手工 SIGTERM / 杀进程重启演练 | ⏭ 不适用 | 属 wave 3（3.3） |

---

## 3. Minor 台账逐条核对（声明 vs 实际改动）

台账位于 `.superpowers/sdd/progress.md` 的「复审提出的 Minor 处置台账」。核对结果：

| 编号 | 台账声明的处置 | 核对结论 | 说明 |
|---|---|---|---|
| M-1 | 根文档指向已删 `tools/mappings/` → 修复轮次 2 任务 A | ✅ **与实际一致** | `CLAUDE.md:70/263/290`、`README.md:48/55` 已全部改指 `tpops_kafka_collector/mapping/`；旧路径在两文件中零命中 |
| M-2 | `mapping/rules.py` 425 行 > 400 → **留给 wave 1 任务 1.8** | ✅ **一致，延期合理且显式** | 实测 **423 行**（lint 清理后 -2），仍 > 400；该文件是 `tools/mappings/rules.py` 的逐字复制件，与合同「映射层复制进包（不重写）」直接冲突，两条约束互斥 → 交给 1.8「规则引擎重写」是唯一合理去处。**未偷偷少做**：台账明确写了去向 |
| M-3 | `ratio_across` 分母取不到返回 `0.0` → **留给 wave 1 任务 1.8** | ✅ **一致，延期合理** | `rules.py:_eval_ratio_across` 仍为 `if not denominator: return 0.0`；与规格「缺值不产出假数据」的张力存在，但规则引擎属 1.8。**须带入 1.8 的简报**（另见下方注意事项） |
| M-4 | 开发方案 5 处指向已删目录的引用 → **留给 wave 4** | ✅ **一致，延期合理** | 文档中仍有 **10 行**命中 `command_center/results` / `api_probe` / `metric_verification` / `multi_master_probe`；这些属「交付文档」口径，wave 4 正是交付文档波次。**不是偷偷少做**：台账逐条列了 |
| M-5 | 进度账本过期 | ✅ **已修** | `progress.md` 现含完整波次 0 勾选（含 0.5′）、审查状态（两轮 fail 的来龙去脉）、修复轮次 2 的 A/B 任务与验证数字、Minor 台账、Wave 1 已知口径不一致的显式记录 |
| M-6 | 视图名提取逻辑两处各写一份 → 记录，不修 | ✅ **一致** | `tools/build_response_rules.py` 仍自带一份提取逻辑；理由（改动生成器有损幂等性）成立，且幂等性是本波次的硬证据，**不修是对的** |
| M-7 | 启动期校验默认读仓库根 194 KB 留档 → **wave 2a 决策** | ✅ **一致** | `mapping/table.py` 的默认留档路径逻辑未动；台账明确指向 2a 的「留档是否复制进包」决策 |
| M-8 | `decode_merged` 传空视图集静默返回零值 → 修复轮次 2 任务 A | ✅ **已修** | `mapping/decode.py`：`if not views: raise DecodeError('视图集为空：…')`；`tests/test_merged_sql_mapping.py` 新增 3 个用例（`test_empty_views_mapping_raises_decode_error` / `…_does_not_silently_return_zero_values` / `test_merge_pages_with_empty_views_raises_decode_error`） |
| M-9 | `RuleContext`/`MergedContexts` 不可变是浅层的 → 记录 | ✅ **一致** | 未改动；与既有实现一致、非回归；属 1.2/1.8 的 DTO 口径 |

**台账整体判定：诚实。** 9 条中 4 条已修、5 条延期，**每条都有明确去向且被显式记录**，无「声明已修但实际没动」的情况
（M-1/M-5/M-8 均已由 diff 与测试证实）。

⚠️ **台账未收录的两条新发现**（实施方报告里披露了但没进台账）：
见 §6 的 Minor-2（`top_n` 排序实为空操作）与 Minor-3（`raw` 的 `group_by` 参数被引擎忽略）。
**建议把这两条补进台账并指向 wave 1 任务 1.8** —— 否则它们只存在于一份实施方报告里，容易在波次交接时丢失。

---

## 4. 控制器指出的两处残留盲区 —— 独立复核与定性

### (a) `raw_filtered` 的「取第一个匹配行 vs 最后一个匹配行」

**复核过程**：审查者在 `/tmp/w0` 副本上做变异测试 ——
`tpops_kafka_collector/mapping/rules.py` 的 `_eval_raw_filtered` 中 `row = rows[0]` → `rows[-1]`，
结果 `pytest tests/test_expected_values.py` **100 passed（未抓住）**；而「完全忽略 filter」的变异被抓住（3 failed）。
与控制器结论一致。

**根因（已量化）**：

- 4 条 `raw_filtered` 规则全部查 `pg_settings` 视图，filter 都是 `{"name": <唯一键>}`：
  `gaussdb_{#PORT}port.status`（`name=port`）、`gaussdb.Timezone`（`name=TimeZone`）、
  `gaussdb.Pagesize`（`name=block_size`）、`GAUSSDB.maximum_connection`（`name=max_connections`）。
- 留档的 `settings` 视图只有 4 行，**每个 filter 恰好命中 1 行** → `rows[0] == rows[-1]`，
  语义在**这份留档上不可区分**。
- 生产侧：`pg_settings.name` 是主键，filter 命中 1 行是**该数据源的结构性事实**，
  不存在「同一条 filter 命中多行」的现实场景。

**定性：Minor —— 守卫盲区（测试强度），不是产品缺陷，也不是合同违反。**

理由：

1. **不是产品缺陷**：引擎当前行为 `rows[0]`（首个匹配行）与独立推导器 `matched[0]` 一致，
   且**产出值正确**（与期望值表相等），无「不报错但数据错」的路径。
2. **不是合同违反**：合同完成标准第 2 条的字面要求（「值等于期望值表」）**已满足**；
   7 个变异体中 6 个被抓住，说明该标准**所要防的那一类缺陷（聚合/过滤口径错）已被覆盖**。
   无法被区分的那一条恰好在**该数据源上不存在**。
3. **但确实是残余的守卫盲区**：若将来有人新增一条「一行可能命中多条」的 `raw_filtered` 规则，
   该规则会**不受值相等守卫保护**。规格 `specs/mapping-and-rules/spec.md` 也**没有**定义
   `raw_filtered` 的多命中语义（既没说取首行，也没说取末行）—— 这是一个**规格空白**。

**处置建议（可派发，优先级低于 wave 1）**：

- **推荐做法（与 `state_contains` 同一手法）**：在 `tests/test_expected_values.py` 补一条**合成多命中**守卫 ——
  构造一个 filter 命中 ≥2 行的 `RuleContext`，断言引擎取**第一行**（并把「取首行」写进断言与注释），
  同时在 `specs/mapping-and-rules/spec.md` 补一句「`raw_filtered` 多命中时取**首个匹配行**」，
  消除规格空白。**代价**：约 15 行测试 + 1 行规格；**收益**：把语义钉死，且与 M-3 的口径修订可一并做。
- **最低限度（若本波次不想再动）**：把本条按 Minor 记入 `progress.md` 台账，指向 wave 1 任务 1.8
  （规则引擎重写时一并定口径），并**显式写明「`pg_settings.name` 唯一，故当前无生产风险」**，
  避免下一波把这条当成「已覆盖」而丢失。
- **不建议**的做法：为此改合同或改 `raw_filtered` 的实现语义（会同时让期望值表与引擎耦合，且与 1.8 重写冲突）。

### (b) `state_contains` 的大小写

**定性：已关闭 ✅。** 提交 `3a04aa5` 在合成夹具里加入了 `'WAITING ON LOCK'`（大写 state，关键词小写），
审查者实测变异 `str(actual).lower()` → `str(actual)` 现在 **2 failed**（此前无该夹具时为 98 passed）。
即「忽略大小写的子串包含」这条语义已被守卫钉住，**不留档盲区**。

---

## 5. 变更范围与治理红线核对

### 5.1 越界改动检查

| 检查项 | 结论 | 证据 |
|---|---|---|
| 改 `execution-contract.md` / `tasks.md` | ✅ **无新增改动** | 本 diff 中这两份文件只有 **一处** 改动：`execution-contract.md` +10 行 / `tasks.md` +5 行 —— 逐行核对确认**全部**来自 `5725ba0` / `0c72251`（ruff 门禁收窄），**已在上一轮由用户追认**。revision 3 建立之后（`8229f69` / `afbb66f` / `1c7d266` / `ec27614` / `3a04aa5`）**没有任何 commit 再碰过这两份文件**。独立佐证：`execution-plan.json`（revision 3）记录的 `contract_hash = sha256:59817bef…` 与当前 `execution-contract.md` 实测 sha256 **完全一致**；`tasks.md` 实测 sha256 = `82689acad3bf…`，本波次无改动 |
| 改 `data/mapping/*.json` 的既有内容 | ✅ **无手改** | 本波次触及 4 份：`alarm_rules.json`（+12 行 `idempotency.alarm` 段，任务 0.3 产物）、`kafka_field_mapping.json`（2 条告警样本加 `"details": []`，任务 0.5′ 产物）、`response_rules.json`（+92 行 `merged_views` 段，任务 0.2 产物）、`expected_values.json`（**新增生成物**）。审查者在 `/tmp` 副本上重跑 `build_response_rules.py` / `build_itscada_mapping.py` / `build_kafka_mapping.py` / `build_expected_values.py --check` 后 `diff -rq data/mapping` → **零差异**，证明**全部内容可由生成器逐字节复现**，没有手写夹带 |
| 把仓库其余部分的 lint 债务带进本波次 | ✅ **未发生** | `ruff check .` 仍为 **305 errors**（历史债未动）；diff 中不含对 `tools/` 旧脚本的大面积 lint 重写；本波次的 lint 改动**只在** `tpops_kafka_collector/` 内 |
| 越出 Wave 0 范围的功能改动 | ✅ **未发现** | 生产包新增文件只有 `__init__.py` / `errors.py` / `ids.py` / `mapping/{__init__,columns,decode,message,nodes,rules,table,README}.py`；wave 1/2/3/4 的模块（config、models、scheduler、kafka、tpops、collectors、main）**均未出现** |
| 生产代码边界 | ✅ | `tests/unit/test_package_boundary.py` 14 passed（AST 级断言生产包无 `tools`/`data` import）；`tools/mappings/` 整目录已删 |

### 5.2 治理红线

- 历史问题（`5725ba0` / `0c72251` 实施方单方面收窄合同门禁）**已被用户追认**（`dp_4_result` 明确记录）。
- **本次 diff 未再出现任何新的合同 / tasks.md 改动**（见 5.1 第一行）—— 红线遵守。
- 审查者**未签发 receipt、未改 `.spec-superflow.yaml`、未改合同 / tasks.md、未改任何被审查代码**；
  所有验证均在 `/tmp` 副本或只读命令上完成。

### 5.3 ⚠️ 需要控制器在签发 receipt 前处理的一项（**不是本波次实现缺陷**）

`.spec-superflow.yaml` 里记录的两个哈希**仍是 revision 2 的旧值**，与 revision 3 的执行计划不一致：

| 位置 | contract_hash | artifacts_hash |
|---|---|---|
| `.spec-superflow.yaml`（现状） | `sha256:46a5301c…` | `sha256:eee95993…` |
| `execution-plan.json`（revision 3） | `sha256:59817bef…` | `sha256:b425f6d3…` |
| `execution-contract.md` 实测 | **`sha256:59817bef…`**（与计划一致 ✓） | — |

即：yaml 的 `contract_hash` 指向**收窄前**的合同内容，而**当前合同的实际哈希与 revision 3 计划记录的一致**
（说明合同自 revision 3 建立后未再被改动，**这是好的一面**）。
风险是：若 `ssf execution review` 用 yaml 里的哈希做 stale 判定，receipt 可能被判 stale 而签不出。
**处置**：签发前请让 CLI/控制器把 `.spec-superflow.yaml` 的两个哈希刷新到与 `execution-plan.json` 一致
（**不要手工改合同或 tasks.md**，那会真的让计划 stale）。本项属控制面维护，不计入本波次 verdict。

---

## 6. 分级清单

### Critical（必须修）
无。

### Important（应该修）
无。上一轮的两条 plan-mandated Important（I-1 / I-2）**均已关闭并经审查者独立取证**。

### Minor（可选改进 / 记录）

| 编号 | 内容 | 位置 | 处置建议 |
|---|---|---|---|
| Minor-1 | `raw_filtered`「首行 vs 末行」在留档上不可区分（每 filter 恰命中 1 行），变异 `rows[0]`→`rows[-1]` 逃逸；且**规格未定义** `raw_filtered` 的多命中语义 | `tpops_kafka_collector/mapping/rules.py:151-167`；`specs/mapping-and-rules/spec.md`（无对应条款） | 补一条**合成多命中**守卫（照 `state_contains` 的手法）＋ 在规格里写明「多命中取首个匹配行」；或至少记入 `progress.md` 台账指向 wave 1 任务 1.8，并写明「`pg_settings.name` 唯一 → 当前无生产风险」。**不需改合同、不需改实现语义** |
| Minor-2 | **`top_n` 的排序实为空操作**：引擎用 `_to_number(row.get(sort_by))` 排序，而排序列 `query_start` 是**时间戳字符串** → 全部退化为 `0.0`，输出顺序 = 留档顺序（恰好正确，因为 SQL 自带 `order by`）。**实施方已主动披露，未改**（超出本任务范围） | `tpops_kafka_collector/mapping/rules.py`（`_eval_top_n`） | **带入 wave 1 任务 1.8**（规则引擎重写时按 `ORDER BY` 口径真正排序）。当前不产出错值，故不阻塞本波次；但**不要让它只留在实施方报告里** —— 请补进 `progress.md` 台账 |
| Minor-3 | `raw` + `per_row` 的 12 条表级指标声明里带 `group_by: ["relname"]`，**引擎完全忽略 `group_by`**（按「一行一条」执行）。留档里 68 行 68 个不同 `relname`，两种理解等价；将来若出现重名 `relname` 会分叉。**实施方已披露** | `data/mapping/metric_rules.json`（12 条）+ `rules.py` `_eval_raw` | 同 Minor-2：带入 1.8，在规则引擎里定口径（要么执行 `group_by`，要么在映射表里删掉这个不生效的参数 —— 后者更好：**声明里不该留一个被忽略的参数**） |
| Minor-4 | `.spec-superflow.yaml` 的 `contract_hash` / `artifacts_hash` 仍为 revision 2 旧值，与 `execution-plan.json`（revision 3）不一致 | `changes/tpops-kafka-collector/.spec-superflow.yaml` | **控制器处理**：签 receipt 前刷新 yaml 哈希（见 §5.3）。不改合同 / tasks.md |
| Minor-5 | `mapping/rules.py` **423 行** > 合同「单文件 < 400 行」（M-2 的延期项） | `tpops_kafka_collector/mapping/rules.py` | 已在台账显式延期到 1.8，**合理**；在 1.8 简报里显式写入「拆文件」要求，避免再次顺延 |
| Minor-6 | 映射层覆盖率 82%~95%（`decode.py` 82%、`columns.py` 88%、`rules.py` 92%），**未达合同「映射层与规则引擎 ≥95%」** | `tpops_kafka_collector/mapping/*` | 该门槛是 **wave 3** 的完成标准，wave 0 无需满足；但距 95% 尚有距离，**wave 3 规划时需预留补测**（尤其 `decode.py` 的错误分支与 `columns.py` 的多节点并集分支） |
| Minor-7 | 修复轮次 2 的两条新发现（Minor-2 / Minor-3）**未进 `progress.md` 的 Minor 台账** | `progress.md` | 补进台账并指向 wave 1 任务 1.8（控制面完整性） |

---

## 7. 做得好的地方（供后续波次沿用）

- **期望值推导器真独立**：不 import 生产包、连解码都自己写、按 `value_rule.params` 的字面意思**重新实现**一遍，
  并在 docstring 里给出每个口径的**规格依据**（规格行号 / 映射表 note 段）—— 这是「期望值表不手抄」的正解。
- **守卫用变异测试自证**：实施方在 `/tmp` 副本上跑了 7 个变异体并公开结果表；
  审查者独立复跑 7 个变异体，结论一致（唯一逃逸项也已定位到「留档不可区分」的根因）。
- **留档盲区主动披露并补合成守卫**：`state_contains` 的「留档里一行都匹配不到」被如实写进报告 B-5.3，
  并补了带**大写样本**的合成用例 —— 这是「不吹」的正确姿势。
- **口径冲突被显式上抛**：`ratio_across` 的「规则层要不要舍入」被写进报告 B-5.1 并请控制器裁决，
  而不是自行拍板（裁决结果：按投递规格「保留 2 位小数」）。
- **生成器幂等性是硬证据**：`--check` 模式 + `test_file_is_up_to_date` 双向锁定，
  审查者重跑 4 个生成器后 `diff` 为空 —— 「`data/mapping/*.json` 没被手改」由此可证。
- **lint 清理是纯等价改写**，逐 hunk 核对无行为变更，且**没有顺手去修仓库的历史债**（守住范围）。

---

## 8. 结论与放行建议

**verdict：pass**（Critical 0 · Important 0 · Minor 7）

- 上一轮复审的两条 plan-mandated Important（I-1 期望值表 / I-2 ruff 门禁）**均已关闭**，且由审查者独立取证；
- 合同 Wave 0 完成标准 **5/5 满足**；
- 无越界改动，**治理红线未被再次触碰**（合同 / tasks.md 自 revision 3 起未再被修改，哈希与计划记录一致）。

**放行前控制器需做两件事（都不是本波次实现的问题）**：

1. 刷新 `.spec-superflow.yaml` 的 `contract_hash` / `artifacts_hash`（§5.3 / Minor-4），避免 receipt 被判 stale；
2. 把 Minor-2 / Minor-3 补进 `progress.md` 的 Minor 台账并指向 wave 1 任务 1.8（Minor-7）。

**Wave 1 开始前建议一并处理（可并行，不阻塞 wave 1 启动）**：Minor-1 的合成多命中守卫 + 规格补一句；
以及在 1.8 的简报里显式写入三件事 —— `rules.py` 拆到 < 400 行（Minor-5）、`top_n` 真正排序（Minor-2）、
`raw` 的 `group_by` 参数要么执行要么从映射表删除（Minor-3）。

> receipt 由控制器签发；本报告不修改 `.spec-superflow.yaml`、合同、tasks.md 或任何被审查代码。
