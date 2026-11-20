# Wave 0 定向复审（修复轮次 1）

- 变更：`changes/tpops-kafka-collector`，波次 `wave-0-mapping-baseline`，修复轮次 1
- 复审日期：2026-11-19

## 复审范围与证据

- scoped diff：base = `f054d087129efa7a26077092930a9271867e568a`，head = `5b4cba1`
- diff 文件：
  `changes/tpops-kafka-collector/.superpowers/sdd/plans/r2-009778c0004b26f99db59f221e72779bba08828f003d6f4adf37688fcfa06424/workspace/review-f054d08..5b4cba1.diff`（6569 行，含 8 个 commit）
  - `9c8e6b8` 定向修复（`is_retryable` 走基类链 + 导出 per-view `decode`）
  - `5725ba0` / `0c72251` ruff 门禁收窄（合同与任务书）
  - `da1328f` 任务 0.6 开发方案契约章节同步
  - `18f1815` 任务 0.7 切依赖 + 删除 `tools/mappings/`
  - `e16eb72` 进度账本
  - `c8538e8` 任务 0.3 / 0.4 / 0.5（告警幂等键含 status、跨视图比率、合并形态列校验对实测留档）
  - `5b4cba1` 告警 `details` 恒为数组
- 先前报告：`changes/tpops-kafka-collector/.superpowers/sdd/reviews/wave-0-mapping-baseline.md`
  （结论 **fail**：1 个 Important = `errors.py:77` 的 `is_retryable` 类名精确相等；并判定 wave-0 完成标准未达成）
- 简报：`workspace/task-0.{3,4,5,6,7}-brief.md`；任务原文：`changes/tpops-kafka-collector/tasks.md` 波次 0 段
- 合同：`changes/tpops-kafka-collector/execution-contract.md`（本轮被实施方修改过，见 Issues I-2）
- 复审者自跑的聚焦验证（未跑全量套件）：
  - `pytest tests/test_merged_sql_mapping.py tests/test_rules.py tests/test_response_rules.py tests/unit/test_package_boundary.py -q`
    → **120 passed in 3.41s**，输出干净（无 warning / 无噪音）
  - `ruff check tpops_kafka_collector/` → **44 errors**（详见 I-2）
  - `ast.parse(feature_version=(3,7))` 扫全包 → 0 语法错误
  - 文件/函数体量：唯一越界为 `tpops_kafka_collector/mapping/rules.py` = 425 行（见 M-2）

## 先前 Important 发现的复审

**结论：已解决，且未引入回归。**

- 修法与先前建议一致：`tpops_kafka_collector/errors.py:82`

  ```python
  return any(base.__name__ in ('Timeout', 'ConnectionError')
             for base in type(exc).__mro__)
  ```

- 行为核对（`type(exc).__mro__`）：
  - `requests.exceptions.Timeout` → MRO 含 `Timeout` → True ✓
  - `requests.exceptions.ReadTimeout`（→ `Timeout`）→ True ✓（修前是 False，即先前报告点名的缺陷）
  - `requests.exceptions.ConnectTimeout`（→ `ConnectionError`, `Timeout`）→ True ✓
  - `requests.exceptions.ConnectionError` / `ProxyError`（→ `ConnectionError`）→ True ✓
  - `AuthError` / `ProtocolError` / `MappingError`（含 `DecodeError`）→ **False** ✓
    —— 关键：`is_retryable` 先走 `isinstance(exc, NON_RETRYABLE)` 早退（`errors.py:78-80`），
    MRO 判定只对**平台异常体系之外**的异常生效，因此不存在「把不该重试的误判成可重试」的回归。
  - `TransientError` / `ProduceError` → True ✓
- 回归证据在测试里：`tests/test_merged_sql_mapping.py:336`（requests 五个异常含子类全 True）、
  `:346`（三个不可重试的平台异常全 False）、`:356`（`test_read_timeout_would_have_been_misclassified_by_exact_name_match`
  —— 明确断言 `issubclass(ReadTimeout, Timeout)` 且判定为 True，把「必须走基类链」钉成实现约束）。
  我跑的聚焦套件里这些用例全绿。
- 唯一的语义副作用（可接受，已在 docstring 里写明）：任何**自定义**的、类名恰为
  `Timeout` / `ConnectionError` 的异常也会被判为可重试。当前代码库没有这种自定义异常，
  且这正是先前报告建议的修法本身，不作为发现。
- 先前 Minor 2（per-view `decode` 未导出）也已修：`tpops_kafka_collector/mapping/__init__.py:14,33`。

## 波次完成度复审（0.3 / 0.4 / 0.5 / 0.6 / 0.7）

### 任务 0.3 —— 包内修告警幂等键：✅ 合规

- 实现：`tpops_kafka_collector/ids.py:30`（`ALARM_IDEMPOTENCY_FIELDS = ('alarm_id','instance_id','first_alarm_time','status')`）、
  `:39`（`alarm_idempotency_key`，用**平台原值** status）。
- 声明：`data/mapping/alarm_rules.json` 新增 `idempotency.alarm` 段（fields / `status_domain: platform_raw` / note），
  由 `tools/build_itscada_mapping.py:727-748` 从 `ids.py` 的常量生成 → **声明与实现同源**，不会各写各的。
- 死代码清理：`tools/build_response_rules.py:128` 的 `IDEMPOTENCY` 现在只剩 `metric`，`alarm` 段已删 ✓（简报点名要求）。
- 测试：`tests/test_rules.py:554-660` —— 触发/恢复键不同、状态不变键稳定、`recovered`≠`cleared`、
  不含 `node_id`/`last_alarm_time`、声明段与实现一致、**声明里的每个字段都真的参与键计算**。
- 偏离（可接受）：简报写的是「在包内定义 `AlarmRecord.idempotency_key`」，实现落在模块级函数
  `ids.alarm_idempotency_key`。`AlarmRecord` DTO 属任务 1.2，而 1.2 的任务书明确写
  「`AlarmRecord.idempotency_key` 按**波次 0 定的新键**计算」—— 波次 0 提供键的算法、1.2 提供 DTO 载体，
  两者一致，不构成缺陷。

### 任务 0.4 —— 包内修跨视图比率：✅ 合规

- 机制：`tpops_kafka_collector/mapping/decode.py:188-198`（`_build_merged_contexts` 给**每个**视图的
  `RuleContext.rows_by_key` 填**全部**视图的行）+ `rules.py:362-373`（`_side_value` 从
  `ctx.rows_by_key[sql_key]` 取分子/分母）。这段实现在本轮 base 之前已随 0.2 落地，本轮补的是**证据**。
- 证据（本轮新增）：`tests/test_merged_sql_mapping.py:412` —— 期望值**从留档原始行自己算**
  （分子 = `activity_agg` 的 `cnt` 之和，分母 = `stat_database` 的 `numbackends` 之和），
  不复制实现的算术；`:444` —— 标准节点实测 23/29 = 0.79，且断言 `value != 0`。
- 契约一致：规则类型真名 `ratio_across`（`data/mapping/metric_rules.json` 与
  `response_rules.json` 里都是 `ratio_across`，无 `ratio` 简写；13 种类型齐全）。
- 残留（Minor，M-3）：`rules.py:214-220` 分母取不到时返回 `0.0` 而不是「不产出」，
  与规格「缺值不产出假数据」（`specs/mapping-and-rules/spec.md:163`）有张力；但规则引擎整体归任务 1.8。

### 任务 0.5 —— 合并形态的列校验与证据重推导：✅ 合规

- 实现：`tpops_kafka_collector/mapping/columns.py`（纯函数，204 行）
  - `merged_archive_view_columns()`：从实测留档 `data/merged_sql/sql_merge_probe_result.json` 取每个视图的**实际返回列**，多节点取**并集**；
  - `required_source_columns()`：按 `source.source == 'command_center_sql'` 收 82 条 B 类规则的列，
    `ratio_across` 单独处理分子/分母两个视图；
  - `source_column_problems()` / `validate_source_columns()`：对不上 → `MappingError`（不可重试 → 启动失败）。
- 生成器 fail-loudly：`tools/build_response_rules.py:513` `_assert_archive_matches_views()` —— 视图名对不上、
  来源列对不上都抛 `BuildError` → 退出码 2。**权威来源是实测留档，映射表是声明**，与简报一致。
- 启动期校验：`tpops_kafka_collector/mapping/table.py:121-157`（`MappingTable.load(validate_columns=True)`，
  留档路径缺省按 `data_dir` 同级 `merged_sql/` 推导，`:248` `_default_probe_path`）。
- 可重复生成：**`data/mapping/response_rules.json` 不在本轮 diff 里** → 重跑生成器输出不变；
  且 `tests/test_response_rules.py` 的 `test_file_is_up_to_date`（比较当前文件与 `build_response_rules.build()`）
  在我跑的聚焦套件里通过 —— 这比「git diff 为空」更硬的幂等证据。
- 测试可证伪：`tests/test_response_rules.py` 新增 6 个用例 —— 伪造视图名被拒（退出码 ≠0 且输出含视图名）、
  伪造缺列被拒（输出含 `numbackends`）、某节点空视图**不**误判为列不存在、真实留档必须通过、
  `MappingTable.load()` 对不上即抛 `MappingError`、生产包不得再引用已删除的 `data/command_center/results/`。
  这些都用**子进程跑生成器**拿真实退出码，且把留档/输出路径指到临时目录，不碰仓库文件 —— 证据形态正确。

### 任务 0.6 —— 同步开发方案的契约章节：✅ 合规（含 Minor M-4）

逐条核对简报点名的章节：

| 简报要求 | 结果 |
|---|---|
| §3.1 消息信封改为 7/13 字段 | ✓ §3.1.1「指标消息（7 个字段）」字段表 = `metricName/metricCode/resourceName/value/unit/clock/tags`，与 `data/mapping/itscada_field_map.json` 的 `metric_message.fields` 逐项一致；§3.1.2「告警消息（13 个字段）」与 `alarm_message.fields` 一致 |
| §3.7 / §4.5.4 告警幂等键加 status、去 node_id | ✓ 两处都改成 `alarm_id + instance_id + first_alarm_time + status`，并写明「恢复时 first/last 都不变（实测 last≠clear），所以键里必须有 status，否则恢复事件会被去重永久丢弃」 |
| §4.4 / §4.4.2 1 条合并 SQL + 102400 B + 分页 | ✓ §4.4 改成「11 个视图 → **1 条合并 SQL**」，写明平台三层限制（limit ≤1000 / 行数 ≤1000 静默截断 / 体积 ≤102400 B 整条失败）与可增长视图分页 |
| §5.5 `CMD_PAGE_SIZE`、`CMD_CONCURRENCY` 默认 2 | ✓ 两项都在配置表里，且给了改并发的实测理由（并发 ≥4 触发平台参数竞争） |
| §5.6.4 校验来源改为实测留档、删已删目录引用 | ✓ 明确「列的权威来源是实测留档」，并把 `data/command_center/results/` 划掉注明已删除 |
| §6.2 回归样本补合并形态 | ✓ 新增 `data/merged_sql/sql_merge_probe_result.json` 一行，明确「82 个 B 类指标的值等于期望值表」 |
| §5.1 目录结构注明 tools/、data/ 只作参考 | ✓ 加了「生产代码边界」说明段 |
| **§1.7 验收口径下调（D4 / D7）** | ✓ D4 改为「本变更不覆盖 → 降级为『连续 2 小时 + 杀进程重启演练』」，D7 注明「本变更只交付其单元形态」，两处都写明依据 §7.1.1 的显式下调口径 —— **是显式记录而非隐去**，符合 §7.1.1 的要求 |

残留（M-4，Minor）：文档里仍有若干处指向**已删除目录**的引用 —— §4.3.2（行 402，`data/api_probe/`）、
行 938 / 1698（`data/command_center/results/…`）、§7.1（行 2566，`data/metric_verification/`）、
§9.2 实测留档索引（行 2904-2918，列了 `metric_verification` / `command_center/results` / `api_probe` /
`multi_master_probe`）。这些不在简报点名清单里（简报只点了 §5.6.4），但与文档自己的
「留档清理说明」和 §0.2 / §6.2 的新口径互相矛盾。

### 任务 0.7 —— 把依赖切到包内并删除参考副本：✅ 合规

- `tools/mappings/` 已在 diff 中**整目录删除**（`__init__.py` / `decode.py` / `message.py` / `nodes.py` /
  `rules.py` / `table.py` / `README.md`）。
- 依赖切换：`tools/build_kafka_mapping.py`、`tools/demo_mapping.py`、`tools/build_response_rules.py`、
  `tools/build_itscada_mapping.py` 全部改为 `import tpops_kafka_collector.…` 并把项目根加进 `sys.path`；
  测试侧 `tests/test_field_mapping.py` / `test_mapping_table.py` / `test_nodes.py` / `test_response_rules.py` /
  `test_rules.py` 同步切换。
- 全仓复查（我做的）：`grep -rn "^\s*(from|import)\s.*mappings" --include=*.py .`（排除生产包）→ **零命中**；
  `tools/mappings` 的路径引用只剩 Markdown 文档（M-1）。
- 边界测试：`tests/unit/test_package_boundary.py` 现在存在（97 行），三条实质断言：
  生产包无任何 `tools`/`data` 顶层 import（AST 级，逐模块参数化）、`tools/mappings/` 已删除、
  包内 5 个映射模块齐备。跑通 ✓。
- 附带收获：`tests/test_response_rules.py` 新增「生产包不得再引用已删除的 `data/command_center/results/`」
  （AST 级、只查字符串常量且排除 docstring）—— 这是对「删目录后启动期校验失效」的正面防御。

## Spec 合规

### ❌ Issues found

**I-1（Important，plan-mandated）—— 波次 0 完成标准「82 个 B 类指标在合并形态下的值等于期望值表」未达成，且未显式记录延后。**

- 合同 `execution-contract.md` Wave 0 完成标准第 2 条写的是「82 个 B 类指标在合并形态下的值**等于期望值表**」
  （不是「非零」判据）。
- 现状：仓库里**不存在**任何「82 条 B 类指标的期望值表」；`grep` 全库，只有
  `tests/test_merged_sql_mapping.py:412/444` 对 **2 条** `ratio_across` 指标做了「值 == 留档推出的期望值」断言；
  其余 80 条 B 类指标在本轮只受**列存在性**校验（`columns.py`，证明「列名对得上」，
  不证明「算出来的值对得上」）。
- 为什么重要：本波次的立项理由就是「82 个 B 类指标在合并形态下**静默产出 0 / None / 空列表**且不报错」。
  列校验能挡住「列名写错」，挡不住「列名对但**聚合口径**错」（例如 `raw` 的 `row_policy`、
  `aggregate` 的 `fn`、`counter_delta` 的差分基线算错）—— 而那正是「静默出 0」最可能的残余形态。
  现在给 wave 0 签 `pass`，等于替一个人类还没验过的结论背书。
- 期望值表的生成器是任务 3.4（wave 3，`tests/fixtures/`），合同内部本身就有这个错位。两条出路，二选一：
  1. **现在就补**：把 2 条 ratio 的期望值推导 generalize 到全部 82 条 —— 每条规则按其
     `value_rule.type` 从留档原始行推出期望值（`raw`→求和/取值、`aggregate`→按 filter+fn、
     `ratio_across`→跨视图比值……），产出「留档 → 期望值」表并断言相等。这与 3.4 的期望值表同源，可提前落地；
  2. **显式延后**：像 §1.7 的 D4/D7 那样，在合同/进度账本里写明「该完成标准延后到 wave 3 的 3.4，
     wave 0 的验收以『列校验 + 2 条 ratio 值相等』为准」，并写清延后理由与依据条款。
     现状是**两头都没有** —— 既没做，也没记。
- 顺带：`progress.md` 的波次 0 账本也已过期（0.3 / 0.4 / 0.5 仍标 `[ ]`，0.7 标 `[ ]` 但已由 `18f1815` 完成），
  见 M-5。

**I-2（Important，plan-mandated）—— 本轮新写进合同的 ruff 门禁，新生产包自己就不满足。**

- 本轮 commit `5725ba0` / `0c72251` 把验收检查从 `ruff check` 收窄为 `ruff check tpops_kafka_collector/`，
  并在「何时不得继续实现」里新增一条：「新生产包的 `ruff check tpops_kafka_collector/` 不干净（仓库其余部分的历史
  lint 债务不在此门禁内）」。
- 实测（复审者跑）：`ruff check tpops_kafka_collector/` → **44 errors**
  - `UP031` × 27（`'%s' % x` 格式）
  - `UP009` × 8（UTF-8 编码声明多余）、`UP010` × 7（`from __future__ import print_function` 多余）
  - `UP020` × 1（`io.open` → `open`）
  - **`E501` × 1**：`tpops_kafka_collector/mapping/message.py:70`，行长 111 > 100
- 归属：44 条全部落在**从 `tools/mappings/` 逐字复制**过来的文件里（base `f054d08` 即已如此），
  本轮新增的 `ids.py` / `columns.py` 是干净的。所以不是本轮引入的回归，但**本轮把这条门禁写进合同后，
  就再没有「不属于本变更范围」的免责空间了** —— 门禁的适用范围恰恰是新生产包，而新生产包就是那批复制代码。
  （顺带更正先前报告 Minor 4 的说法：仓库 `ruff.toml` 的 `select` 是 `E/F/W/E501/UP/FA102`，不止 `F`；
  先前报告「只测了 F」的判断不准确，本轮把范围算全了。）
- 为什么重要：合同的「不得继续实现」条件之一就是这条；带着 44 条不通过就签 wave 0 的 `pass`，
  等于让执行流程在自设门禁不成立的状态下进入下一波。修复成本很低（`ruff check tpops_kafka_collector/ --fix`
  自动修 16 条，余下 28 条是机械替换 + 一处折行），但**必须显式做**，否则门禁就是一句空话。
- 另一层问题（治理面）：**执行合同是被实施方自己改的**。收窄门禁本身有正当理由（仓库有 357 条历史 lint 债务），
  但「批准后的合同在执行途中被放宽，且放宽后的新标准自己没达到」需要人来裁，不该由执行方自批。
  建议控制器把这一条转人工确认。

### ✅ 合规的部分

- **生产代码边界**：`tpops_kafka_collector/` 无 `tools.*` / `data.*` import（AST 级测试 + 我全仓 grep 复查）；
  `tools/mappings/` 已删除，仓库只剩一份映射层实现。
- **消息信封**：`itscada_field_map.json` 为准 —— 指标 7 字段 / 告警 13 字段；
  业务维度落 `tags.dimension_values`；`component_id` 小写下划线（`mapping/message.py:141-152`）；
  文档 §3.1 与实现逐字段一致。
- **告警幂等键**：`(alarm_id, instance_id, first_alarm_time, status)`，平台原值 status，实现与声明同源。
- **规则类型**：映射表在用的 13 种齐全，跨视图比率真名 `ratio_across`。
- **Python 3.7.9**：全包 `ast.parse(feature_version=(3,7))` 通过；无海象、无 `f"{x=}"`、无字典合并 `|`、
  无内建泛型注解、无 `typing.Literal/Protocol/TypedDict/Final`、无 `zoneinfo`、无 `removeprefix`。
  （`ids.py` / `columns.py` 用 `typing.List/Dict/Optional/Set/Tuple` 与 `# type:` 注释，符合红线。）
- **不新增依赖**：新增代码只用标准库 + 既有 `requests`（测试里用于构造异常）。
- **凭据**：本轮无凭据相关代码；无硬编码密钥、无 `print`（生产包用 `logging`/异常）。
- **不可变 DTO / 显式错误**：`MappingError` 包装带上下文（`table.py:249-261`、`columns.py:139-197`），无静默吞异常。
- **TPOPS 红线**：本轮未触碰认证/命令中心路径，既有约束未被违反。
- **`details` 恒为数组**（`5b4cba1`）：`mapping/message.py:179` 输出空数组而非省略字段，
  与 `specs/mapping-and-rules/spec.md:311`「`details` 缺失时 SHALL 输出空数组 `[]`，SHALL NOT 省略该字段」
  一致，并有 `tests/test_merged_sql_mapping.py:467` 直接断言。

### ⚠️ 无法从 diff 判定（控制器应查）

- 「既有 164 个用例仍全绿」：我只跑了与本轮 diff 直接相关的 4 个文件（120 passed）。
  `tests/test_field_mapping.py` / `test_generated_rules.py` / `test_kafka_mapping.py` /
  `test_mapping_table.py` / `test_metric_response.py` / `test_nodes.py` / `test_py37_compat.py`
  未重跑（按复审约束不跑全量套件）—— 控制器应确认 `pytest tests/ -q` 的整体结果与用例总数。
- 覆盖率门禁（`--cov-fail-under=80`，映射层与规则引擎 ≥95%）：本轮未测。
- 合同要求的手工项（SIGTERM 优雅退出、杀进程重启演练）属 wave 3，不在本波次范围。

## 做得好的地方

- **修法选得准**：`is_retryable` 走 `type(exc).__mro__` 而不是把 `requests` 硬绑进异常层，
  既覆盖子类又不新增依赖，还保留了「平台异常体系优先早退」的次序 —— 我逐个异常推演过 MRO，没有回归。
- **测试钉的是实现约束，不是实现细节**：`test_read_timeout_would_have_been_misclassified_by_exact_name_match`
  把「必须走基类链」写成断言，改回精确相等就红；`test_alarm_idempotency_every_declared_field_changes_the_key`
  保证 JSON 里声明的每个字段都真的参与键计算，而不是写进文件当摆设。
- **声明与实现同源**：`alarm_rules.json` 的 `idempotency` 段由 `tools/build_itscada_mapping.py` 从
  `tpops_kafka_collector/ids.py` 的常量生成，`response_rules.json` 的 `idempotency.metric` 同理 ——
  杜绝了「两份契约各写各的、悄悄漂移」这一类最难查的缺陷。
- **权威来源选对了**：把「实测留档」定为事实、「映射表」定为声明，生成器与启动期校验用**同一套**代码
  （`mapping/columns.py`）跑 —— 生成器与采集端不会各判各的。
- **列校验的失败形态是 fail-loudly**：视图名对不上、列对不上都抛 `BuildError` → 退出码 2，
  且测试用**子进程**拿真实退出码、把留档与输出指到临时目录 —— 不碰仓库文件就能证伪。
- **并集语义的边界考虑到了**：某个 master 这一轮没有命中行 ≠ 列不存在，列判定取多节点并集，
  并有专门用例固定这个语义（「tolerates_a_view_empty_on_one_node」）。
- **docs 变更把「裁剪」显式记下来了**：D4/D7 在 §1.7 里写成降级并注明依据 §7.1.1，
  而不是悄悄少投数据 —— 这正是 §7.1.1 要求的做法。
- **`details` 恒为数组**是主动对齐规格（「缺了字段客户侧要写分支」），而不是被动补测试。

## Issues

### Critical (Must Fix)

无。

### Important (Should Fix)

**I-1（plan-mandated）波次 0 完成标准「82 个 B 类指标的值等于期望值表」既未实现、也未显式延后**

- 位置：合同 `execution-contract.md` Wave 0 完成标准第 2 条；
  现状证据见 `tests/test_merged_sql_mapping.py:412`（仅 2 条 ratio）、`:444`（仅 2 条 ratio）。
- 错在哪：只有「列存在性」被守住，80 条 B 类指标的**取值正确性**无人验证；
  而本波次的立项缺陷就是「静默出 0 / None」。
- 为什么重要：wave 0 的 `pass` receipt 会被人当作「82 个 B 类指标在合并形态下是对的」的结论。
- 怎么修（两条路，二选一，不要两头空）：
  1. 把 ratio 用例的期望值推导 generalize 到全部 82 条 —— 按 `value_rule.type` 从留档原始行算期望值，
     产出「留档 → 期望值表」并断言相等（与任务 3.4 的表同源，提前落地即可）；
  2. 或像 §1.7 的 D4/D7 那样，在合同与 `progress.md` 里**显式**写明该完成标准延后到 wave 3 的 3.4、
     延后理由与本波的替代验收口径。

**I-2（plan-mandated）本轮写入合同的 `ruff check tpops_kafka_collector/` 门禁不成立（44 errors）**

- 位置：合同 `execution-contract.md`「验收检查」第 4 条 +「何时不得继续实现」新增条；
  实测：`ruff check tpops_kafka_collector/` → 44 errors
  （`UP031`×27、`UP009`×8、`UP010`×7、`UP020`×1、**`E501`×1 @ `tpops_kafka_collector/mapping/message.py:70`**）。
- 错在哪：门禁写的是「新生产包自身干净」，而新生产包正是那批复制代码；本轮既没修，也没把它排除出门禁。
- 为什么重要：它是合同自设的「不得继续实现」条件，不成立就不能签 wave 0 的 `pass`。
- 怎么修：`ruff check tpops_kafka_collector/ --fix`（自动修 16 条）→ 手工把剩余 28 条
  （`%` 格式化 → f-string / `.format`、去掉多余的 `# -*- coding: utf-8 -*-` 与 `from __future__ import print_function`、
  `io.open` → `open`、折 `message.py:70`）改完 → `ruff check tpops_kafka_collector/` 归零。
  行为零变更（全是等价改写），但必须是**显式做完**，否则门禁无意义。
- 治理附注：合同是实施方自己收窄的（`5725ba0`/`0c72251`）。收窄理由成立（仓库 357 条历史债务），
  但「批准后的合同在执行途中被放宽、且放宽后的标准自己没达到」应由人裁，请控制器转人工确认。

### Minor (Nice to Have)

**M-1 根目录文档仍指向已删除的 `tools/mappings/`**
`CLAUDE.md:70`（目录结构）、`CLAUDE.md:257`、`CLAUDE.md:284`（工具表）、`README.md:48,54`。
这些是给未来 agent/人看的入口文档，指向一个已经不存在的目录。建议改成 `tpops_kafka_collector/mapping/`
（并注明「数据仍在 `data/mapping/`，wave 2a 才搬进包内」）。

**M-2 `tpops_kafka_collector/mapping/rules.py` = 425 行 > 合同的「单文件 < 400 行」**
文件是从 `tools/mappings/rules.py`（425 行）逐字复制来的，与合同「映射层复制进包（不重写）」直接冲突 ——
两条约束本身互斥。功能/函数体量都合格（无 >50 行的函数）。建议在任务 1.8（规则引擎）里顺手拆分，
或把合同该条对「复制件」显式豁免。

**M-3 `ratio_across` 分母取不到时产出 `0.0` 而不是「不产出」**
`tpops_kafka_collector/mapping/rules.py:214-220`。与规格「缺值不产出假数据、且与『取到 0』可区分」
（`specs/mapping-and-rules/spec.md:163`）有张力（分母为 0 或视图为空时）。规则引擎整体属任务 1.8，
在那儿一并处理即可；此处的测试只覆盖了「分母 > 0」的正常路径。

**M-4 开发方案仍有指向已删除目录的引用**
行 402（`data/api_probe/health_status.json`，且脚本 `tools/fetch_tpops_health_status.py` 已不存在）、
行 938 / 1698（`data/command_center/results/…`）、行 2566（`data/metric_verification/`）、
§9.2 留档索引行 2904-2918（`metric_verification` / `command_center/results` / `api_probe` / `multi_master_probe`）。
与文档自己的「留档清理说明」及 §0.2 / §6.2 的新口径矛盾。简报只点了 §5.6.4，故不算漏做，
但建议在 wave 4 交付文档前顺手改掉。

**M-5 进度账本过期**
`.superpowers/sdd/progress.md` 波次 0 段仍把 0.3 / 0.4 / 0.5 标 `[ ]`、0.7 标 `[ ]`，
实际都已由 `c8538e8` / `18f1815` 完成（0.4 标注「待补测试」也已补）。账本是控制面的依据，
建议与本轮结果同步。

**M-6 视图名提取逻辑在两处各写一份**
`tools/build_response_rules.py:539`（`_archive_view_names`）与
`tpops_kafka_collector/mapping/columns.py:63`（`_single_row`）解析的是同一种留档结构，
逻辑重复。建议生成器直接复用 `columns.merged_archive_view_columns()` 的视图名集合，
少一份会漂移的解析代码。

**M-7 映射表加载默认读仓库根的实测留档（wave 2a 的前置风险）**
`tpops_kafka_collector/mapping/table.py:121-157`：启动期校验默认读
`data/merged_sql/sql_merge_probe_result.json`（194 KB）。这与合同「运行时不读仓库根」冲突，
也正是任务 2.1/2.2（映射数据复制进包）要解决的；但**这份实测留档是否也复制进包**尚未决策 ——
它是 194 KB 的实测产物，不是「映射数据」。建议在 wave 2a 明确：复制进包，还是改成
「按 `merged_sql.json` 的列声明 + 留档指纹」校验。

**M-8 `decode_merged(body, views={})` 仍返回空的 `MergedContexts` 而不报错**
（承接先前报告 Minor 5，未修）。传入空/过期视图集时调用方拿到静默零值。
建议传空 `views` 时抛 `DecodeError`。

**M-9 `RuleContext` / `MergedContexts` 的不可变是浅层的**
`frozen=True` 但持有可变 dict，且所有合并上下文共享同一个 `rows_by_key` 对象
（承接先前报告 Minor 7，与既有 `tools/mappings` 一致，非回归）。

## 结论

**波次判定：fail**

**理由**：先前唯一的 Important（`is_retryable` 类名精确相等）已正确修复且无回归，
0.3 / 0.4 / 0.5 / 0.6 / 0.7 五个任务均已按简报交付、证据形态合格（聚焦套件 120 passed）；
但波次仍有两条**计划要求的**完成标准未达成 —— 一是合同自设的
`ruff check tpops_kafka_collector/` 门禁在 44 条 error 上不成立（I-2），
二是波次 0 完成标准「82 个 B 类指标的值等于期望值表」既未实现也未显式延后（I-1）。
两条都可低成本关闭（前者是机械改写，后者是 generalize 2 条 ratio 的期望值推导或补一句显式延后记录），
建议控制器转人工裁定 I-2 中「实施方自行收窄已批准合同」这一治理面问题后，进入下一轮修复。
