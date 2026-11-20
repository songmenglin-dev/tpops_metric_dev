# Wave 1 审查报告 —— wave-1-skeleton-platform（revision 3）

| 项 | 值 |
|---|---|
| 波次 | `wave-1-skeleton-platform` |
| Base / Head | `3a04aa5` / `4b95fb4` |
| 执行计划 | revision 3，`plan_hash = sha256:b4766c8a…`，`plan_revision = 3` |
| 审查范围 | `workspace/review-wave1-r3.diff`（13197 行，已排除 3 类大生成物）+ 9 份任务报告 + 账本 + 合同 + `tasks.md` |
| 审查方式 | 只读 diff 一次；证明命令全部实跑；W-5 / W-6 的裁决前提独立核实 |
| **结论** | **`pass`** |

---

## 0. 结论

**verdict = `pass`。Critical 0 / Important 0 / Minor 12。**

判据：Critical 与 Important 均为 0，达成 `pass` 条件。12 条 Minor 全部逐条给出处置建议，
其中 **3 条要求在 wave 2 开工前修掉**（M-1 文件行数、M-2 文档与 LRU 新语义矛盾、M-4 唯一出口守卫覆盖面），
其余可并入 wave 2 / wave 3 的既有补测窗口。另有 **6 项待补登记**（第 9 节）——按简报要求**本次不改
`tasks.md` / 合同**，只登记。

已实跑并全部成立的证据：

| 门禁 | 命令 | 结果 |
|---|---|---|
| 全量测试 | `pytest tests/ -q` | **715 passed** |
| Wave 1 完成标准 | `pytest tests/unit tests/integration -q` | **352 passed** |
| 3.7 语法门禁 | `pytest tests/test_py37_compat.py -q` | **77 passed** |
| 新生产包 lint | `ruff check tpops_kafka_collector/` | **All checks passed!（退出码 0）** |
| 覆盖率 | `pytest tests/ --cov=tpops_kafka_collector` | **TOTAL 92%**（≥80% 达标） |
| 生产代码边界 | `pytest tests/unit/test_package_boundary.py -q` | **35 passed** |
| 生成器幂等 | `python tools/build_kafka_mapping.py --check` | **exit 0（是最新的）** |
| 生成器幂等 | `python tools/build_observed_columns.py --check` | **exit 0（是最新的）** |

---

## 1. 合同 Wave 1 完成标准逐条判定

合同 `execution-contract.md` 的 Wave 1 完成标准共 4 条：

| # | 完成标准原文 | 判定 | 依据 |
|---|---|---|---|
| 1 | `pytest tests/unit tests/integration -q` 全绿 | **满足** | 实跑 **352 passed**；新增 `tests/unit/`（9 文件）+ `tests/integration/`（1 文件）全部在列 |
| 2 | `python -m tpops_kafka_collector.main` 能起、能收 SIGTERM、退出码 0、水位文件完整 | **⚠️ 无法判定（已知合同口径不一致，已获用户裁决）** | `main.py` 由 wave 3 任务 3.3 新建，wave-1 输出清单里没有它；wave-1 的 diff 中确无 `main.py`。**按简报第 1 节的口径**：wave 1 的门禁按「包内骨架与平台层就位 + unit/integration 全绿」判定，进程入口/信号处理/水位文件端到端验收归 wave 3（3.3）。**本报告不重复要求改合同**，仅按 ⚠️ 记录，由人裁 |
| 3 | 序列化器输出与 `itscada_field_map.json` 的字段集逐字段一致 | **满足** | `tests/unit/test_serializer.py::test_envelope_matches_customer_field_map` 逐字段比对指标 7 字段 / 告警 13 字段 + 两组 tag 字段（**含顺序**）；另有 4 条字段顺序断言。**包内副本**的传递性守卫生效：`test_mapping_loader.py::test_package_copies_are_identical_to_the_repo_files` 断言包内 `itscada_field_map.json` 与仓库根 `data/mapping/itscada_field_map.json` **逐字节一致** → 「序列化器硬编码 == 仓库根 == 包内副本」链闭合（详见第 5 节 S-2） |
| 4 | 13 种 `value_rule.type` 全部有测试覆盖 | **满足** | 实测 `HANDLERS` 的键与映射表在用类型**完全相同**（各 13 项，无多无缺）：`raw / raw_filtered / aggregate / counter_delta / ratio_across / series_point / enum_map / gauge / top_n / raw_rows / collection_count / text_prefix_count / instance_field`。`tests/unit/test_rules.py` 60 例逐类型覆盖正常路径 + 边界路径（空行、缺列、类型不对、分母为 0、计数归零、枚举未命中、未知类型/未知 position 抛 `RuleError`）。表内另有 3 条 `type=null`（即 3 个不可提供指标，本就不产出） |

**其他合同条款**（非 Wave 1 完成标准，但属 Design Constraints / Test Obligations）：Python 3.7.9 语法红线 ✅（py37 门禁 77 passed）；不新增依赖 ✅（只用了已在锁内的 `kafka-python` / `requests`）；单文件 < 400 行 ⚠️（2 个文件越线，见 M-1）；函数 < 50 行 ✅（AST 全量扫描 `tpops_kafka_collector/`，**无一个函数 > 50 行**）；不可变数据 ✅（DTO 全 frozen dataclass，跨模块值对象均为 frozen）；凭据只走环境变量 ✅（见第 7 节）；错误显式处理 ✅（生产代码零 `print`、零静默吞异常）。

---

## 2. 九个任务的证明命令（全部实跑）

| 任务 | 证明命令 | 实际结果 | 判定 |
|---|---|---|---|
| 1.1 配置加载与启动校验 | `pytest tests/unit/test_config.py -q` | **50 passed** | ✅ |
| 1.2 DTO 与异常体系 | `pytest tests/unit/test_models.py -q` | **32 passed** | ✅ |
| 1.3 重试与退避框架 | `pytest tests/unit/test_retry.py -q` | **32 passed**（`retry.py` 覆盖率 100%） | ✅ |
| 1.4 JSON 序列化器 | `pytest tests/unit/test_serializer.py -q` | **32 passed** | ✅ |
| 1.5 Kafka 生产者 | `pytest tests/integration/test_producer.py -q` | **26 passed**（纯 mock，见第 6 节） | ✅ |
| 1.6 TPOPS HTTP 客户端 | `pytest tests/unit/test_client.py -q` | **42 passed**（`client.py` 覆盖率 99%） | ✅ |
| 1.7 映射表加载与启动校验 | `pytest tests/unit/test_mapping_loader.py -q` | **15 passed** | ✅ |
| 1.8 规则引擎（13 种 type） | `pytest tests/unit/test_rules.py -q` | **60 passed** | ✅ |
| 1.9 水位与差分基线持久化 | `pytest tests/unit/test_watermark.py -q` | **28 passed** | ✅ |

（账本记的是 47/32/15/26/32/26/41/60/26，差额来自控制器后续两个提交补的用例：`92e3223` 的 LRU 两条、
`4b95fb4` 的静默期相关两条、`e5d3dbb` 起的配置项一条 —— 账本数字已过期但方向一致，不构成问题。）

**逐任务的实质判定**（不只看测试数）：

- **1.1**：必填项缺失 / 空白 / 越界全部 `ConfigError` 快速失败，**不发任何请求**（模块无网络代码）；范围校验覆盖窗口 ≥1、并发 1~10、页容量 ≤1000、告警 limit ≤100、端口 1~65535；`TPOPS_PASSWORD` 不 strip（空格属密码）、错误消息与 `repr` 都不含凭据（`field(repr=False)` + `_SECRET_VARS` 脱敏）。✅
- **1.2**：`AlarmRecord.idempotency_key` **转发 `ids.alarm_idempotency_key`**，测试断言的是「DTO 上的键 == ids 模块算出来的键」而不是抄一遍拼法 —— 真相源唯一，符合合同「幂等键真相源同步」。✅
- **1.3**：判定集中在 `errors.is_retryable`（**按 MRO 基类链**，实测 `ReadTimeout`/`ConnectTimeout` 都判为可重试），`retry.py` 不重复判定；退避 1s→2s→4s→8s，抖动「只增不减」。✅
- **1.4**：信封 7/13 字段固定顺序、缺省一律 `null`、13 位毫秒、`ensure_ascii=False`、值按客户表【数据类型】定型且与映射层 `serialize_value` 口径一致（`test_value_serialization_matches_mapping_layer` 10 组对拍）。✅
- **1.5**：PLAINTEXT / `acks=all` / 幂等显式关闭 / 分 Topic / Key = `instance_id` 与 `alarm_id`；失败抛 `ProduceBatchError`（`ProduceError` 子类，`is_retryable` 为 True）→ 调用方可据此不推进水位。✅
- **1.6**：token 从**响应头** `X-Mini-Token` 取；认证失败**只发一次**认证请求（3 次连续 `token()` 调用后 `auth_calls()==1`）；401 强制刷新并**只重试一次**；403 不刷新；404 → `ProtocolError` 并提示命名空间；两套前缀不被客户端悄悄补全；凭据不进日志/异常/`repr`。✅
- **1.7**：包内 `mapping/data/` 复制 7 张表 + 生成的 `observed_columns.json`；启动期四类校验（来源列 / 可提供齐全 / 条目对应真实指标 / 占位符-维度绑定一一对应）全部 fail-loud 并带差异清单；`PACKAGE_DATA_DIR` 由 `__file__` 推导，**运行时不需要仓库根目录**。✅
- **1.8**：13 种 type 全覆盖；差分用实际采集时刻差、首轮只记基线、归零记 ERROR 并丢弃本轮；`ratio_across` 分母不可用不产出；`top_n` 按 PG `ORDER BY` 口径真正排序。✅
- **1.9**：临时文件 → `fsync` → `os.replace` 原子写（同目录，避免 EXDEV）+ 目录 fsync；写失败时目标文件仍是上一轮完整内容且**不留临时文件残留**；水位 / 差分基线 / 告警水位 / 幂等键四段可恢复。✅

---

## 3. 六条已获裁决的偏离 —— 「偏离是否可接受」判定

| 编号 | 偏离 | 判定 | 依据与独立核实 |
|---|---|---|---|
| **W-1** | 1.7 / 2.1 依赖倒置 | **可接受** | 1.7 的完成标准「运行时不需要仓库根目录」在 wave-1 结束时**真的成立**（实测通过），2.1 退化为防漂移同步测试 + 补齐 `merged_sql.json`/`sql_merge.sql` 的复制，其证明命令 `pytest tests/unit/test_mapping_data_sync.py -q` 不变。合同与 `tasks.md` **均未改动**（diff 文件清单里没有这两个文件），执行计划未 stale。**遗留**：wave-2a 的 2.1 范围已缩窄，须登记（见待补清单 T-5） |
| **W-2** | 1.8 把 `mapping/rules.py` 拆成 9 个同级模块（门面 43 行） | **可接受（事实层面）**，但**产物未进合同输出清单** → 登记待补 | 拆分是合同「单文件 < 400 行」的硬要求（wave-0 先例：422 行的 `rules.py` 已被 wave-0 独立审查立项）。实测 9 个新模块全部 < 200 行（最大 `rules_context.py` 172 行），门面 `rules.py` 43 行。**但合同 Wave 1 输出清单只写了 `mapping/{loader,rules}.py`** → 见待补清单 T-1 |
| **W-3** | 1.5 多一个 `kafka/records.py`（`producer.py` 527 → 399 行） | **可接受（事实层面）**，登记待补 | `producer.py` 实测 **399 行**（< 400）；`records.py` 127 行。拆分后「Key 规则 / 记录形状」与「怎么投」分离，`kafka/__init__.py` 的公开接口不变 → 见待补清单 T-2 |
| **W-4** | 1.8 的 R-M3 选「执行 `group_by`」而非「删参数」 | **可接受** | **独立核实**：`rules_context.grouped_items` 真的读了 `group_by` —— ① 用 `group_by` 的列构造分组键，维度值**取自分组键**而非 `row.get()`；② 强制 `group_by ⊇ dimension_bindings.values()`，不一致直接抛 `RuleError`（不静默取其中一个）；③ 分组键重复（维度值不唯一）记 WARNING（幂等层会撞键）。**并实测了真实映射表**：34 条带 `group_by` 的规则，**无一条**与 `dimension_bindings` 冲突 → 不存在「运行时抛 RuleError」的风险。留档 68 行 68 个不同 `relname` → 两种口径输出相同，裁决理由成立 |
| **W-5** | 1.5 的 `enable_idempotence` 适配（kafka-python 2.0.2 无此键） | **可接受 —— 前提已独立核实为真** | **独立核实**：`KafkaProducer.DEFAULT_CONFIG`（kafka-python **2.0.2**，实测）共 27 个键，**不含 `enable_idempotence`**（`acks`/`security_protocol`/`compression_type`/`client_id`/`retries`/`linger_ms`/`max_in_flight_requests_per_connection` **都在**）。直传会抛 `KafkaConfigurationError` 的前提成立。逻辑配置里显式写 `False`（语义目标「幂等关闭」达成），`supported_kwargs()` 按客户端已知键过滤并记 INFO。`test_supported_kwargs_only_contains_options_known_to_kafka_python` + `test_idempotence_is_never_enabled_even_if_client_supports_it` 守住了「换客户端也不会误开幂等」。**结论：偏离可接受，无需改** |
| **W-6** | 跨视图比率：规格写「分母为 0 → 结果为 0」，实现为「不产出」 | **可接受 —— 推理链已独立核实为真，且已显式记录** | **推理链核实（读 spec-superflow 1.0.0 源码）**：① `hash.mjs::computeArtifactsHash` 覆盖 `proposal.md` + `specs/*/spec.md` + `design.md` + `tasks.md` → 改 `specs/**` 必然改 `artifacts_hash`；② `execution-plan.mjs:76` 判定 `plan.artifacts_hash !== computeArtifactsHash(changeDir)` 即 `execution plan is stale`；③ `execution-plan.mjs:197` receipt 只在 `plan_hash` 与 `plan_revision` **都匹配**时才被承认 → 一旦 `execution revise`，revision 2/3 的全部 receipt 作废，wave-0 与 wave-1 都要重审。**链条成立**，延后改规格是刻意的、正确的决策。**同时核实了当前状态是干净的**：用插件源码重算 `artifacts_hash = b425f6d3…`、`contract_hash = 59817bef…`，与 `execution-plan.json` 里存的、`.spec-superflow.yaml` 里存的**三者完全一致** → wave-1 里那次 `.spec-superflow.yaml` 的 hash 刷新是合法的 `state rebuild`（把 revision 2 的旧值补齐），不是绕过门禁。**wave-0 的 `pass` receipt 仍然有效**：`report_sha256 = a824a7a3…` 与当前 `wave-0-mapping-baseline.md` 的**文件字节 sha256 实测一致**，root 与 plan-scoped 两份 receipt 内容相同。**偏离已被显式记录**（账本「⚠️ 规格自相矛盾」一节 + `wave1-execution-brief.md`），**实现与裁决一致**（`eval_ratio_across`：分母取不到 → 不产出；分母为 0 → 不产出；分子 0、分母正 → 产出 `0.0`）。**收口建议**：规格改写列为**独立小变更**是唯一可行的收口方式，但必须**在 wave 4 交付文档之前**落地（交付文档要写口径，届时若规格仍与实现冲突，文档与规格互相矛盾） |

---

## 4. 账本里指向 1.8 的 5 个遗留项 —— 是否真的关闭

| 台账号 | 内容 | 1.8 应做的 | 判定 |
|---|---|---|---|
| **M-2 / R-M5** | `mapping/rules.py` 超 400 行 | 拆文件 | **✅ 已关闭**。`rules.py` 现在是 **43 行的门面**（只 re-export + 文档），实现拆到 9 个模块（最大 172 行）。无一个生产文件 > 404 行（见 M-1） |
| **M-3** | `ratio_across` 分母取不到返回 `0.0` 而非「不产出」 | 改成不产出 | **✅ 已关闭**。`rules_cross.eval_ratio_across` 分母 falsy → `return []` 并记 WARNING。**涟漪已正确处理**：`tests/test_kafka_mapping.py` 与 `tests/test_response_rules.py` 改成断言「跨视图比率在逐指标留档下不产出」，**不是**把实现改回出 `0.0`；`data/mapping/kafka_field_mapping.json` 由**生成器**重生成（`gaussdb.Total_pct` / `GAUSSDB.session_usage` 现为 `message_count=0`、`sample_message=null`），且 `python tools/build_kafka_mapping.py --check` **exit 0**（可复现） |
| **R-M1** | `raw_filtered` 多命中语义规格没定义 | 定口径 + 补合成多命中守卫 | **✅ 实现侧已关闭**：口径定为「多命中取**首个**匹配行」，并有合成多命中守卫 `test_raw_filtered_takes_the_first_matching_row`（3 行同名 `port` 的样本 → 取 `5432` 而非 `6432`）。**⚠️ 规格侧未关闭** ——「在规格里写明」这一半因 W-6 同理延后 → 登记待补 T-6 |
| **R-M2** | `top_n` 排序实为空操作（时间戳字符串全退化成 `0.0`） | 按 PG `ORDER BY` 口径真正排序 | **✅ 已关闭**。新增 `rules_sorting.py`（112 行）：比较类型由**列里第一个非空值**决定（数值 / 时间戳 / 文本）；时间戳按**时间**比（`%Y-%m-%d %H:%M:%S.%f%z`）；**空值升序在后、降序在前**（PG 默认）；**空串按空值处理**（平台把 NULL 的 `query_start` 渲染成 `''`）。6 条测试覆盖：数值降序、时间戳降/升序、空值升序在后、空值降序在前、空串当空值、从集合取行。**并同步改了独立推导器** `tools/expected_deriver.py` 的 `_is_empty`（见 M-8） |
| **R-M3** | `raw` + `per_row` 的 12 条表级指标带 `group_by`，引擎完全忽略 | 见 W-4 | **✅ 已关闭**（W-4 判定成立） |

---

## 5. 1.4 留下的 4 条尾巴 —— 处置是否可接受

| 编号 | 内容 | 判定 | 处置建议 |
|---|---|---|---|
| **S-1** | `SerializerError` 定义在 `kafka/serializer.py` 内，未进 `errors.py` 异常体系 | **可接受（现状无害），但不建议长期留着** | `SerializerError`（`ValueError` 子类）与 `ConfigError`（`Exception` 子类）都在 `errors.py` 体系外。行为上安全（`is_retryable` 对二者都返回 False → 不可重试，与意图一致），但**是「碰巧对」而非「声明对」**。**建议**：wave 2/3 把二者收进 `errors.py`（`SerializerError` → `MappingError` 子类；`ConfigError` → 独立于 `TpopsError` 的启动期错误），与 1.2 把 `DecodeError` 收敛的做法保持一致。见 M-3 |
| **S-2** | 序列化器硬编码字段表，测试比对的是**仓库根** `data/mapping/itscada_field_map.json`；包内副本没有直接守卫 | **✅ 实际已闭合（传递性）**，但缺一条直接断言 | 链条：`test_serializer::test_envelope_matches_customer_field_map` 断言「序列化器硬编码 == 仓库根字段表」× `test_mapping_loader::test_package_copies_are_identical_to_the_repo_files` 断言「包内副本 == 仓库根字段表」（**逐字节**）→ 三者相等。**建议**：仍补一条显式断言「包内副本的字段表 == 序列化器硬编码的字段表」，让这个不变量不必靠两条测试的传递性来支撑 |
| **S-3** | `dumps()` 用紧凑分隔符 → 消息体**逐字节**格式（对外契约面） | **可接受** | 语义与 `json.loads` 消费方无差别；`ensure_ascii=False` + 固定字段顺序 + 紧凑分隔符是稳定的对外字节格式。**必须写进 wave 4 的交付文档**（客户若要求可读 JSON 需要改） |
| **S-4** | `allow_nan=False` → NaN 抛 `SerializerError`；wave 2 的采集器要把 NaN 当 `null` | **可接受（wave 1 范围内无 NaN 来源），但必须进 wave-2 验收条目** | **核实了具体失效路径**：`apply_value_type(value, '浮点')` 对 NaN 得 `round(nan,2)=nan`（**不抛**），NaN 一路到 `dumps(allow_nan=False)` 才抛 `SerializerError`；且该异常在 `metric_record()` 里抛出（**在生产者之外、在「不推进水位」路径之外**）→ 会被当作未分类异常打断整轮。`value_type=None` 的路径同样漏。**建议**：wave 2 的 brief 明确「NaN/Inf 一律归一化为 `None` 再进序列化」，并加一条 NaN 的回归测试。见 M-7 |

---

## 6. 控制器在 wave-1 期间自己做的两处修复 —— 判定是否正确

### 6.1 幂等键集合改成真 LRU（`92e3223`）—— **判定：语义正确，实现正确；但文档没跟上（M-2）**

**独立核实**：

- 规格原文（`specs/delivery-and-state/spec.md:170`）：「采集端 SHALL 在内存中维护**最近 N 条消息**的幂等键集合（LRU，默认 10000 条），防止同一轮内的重复投递与相邻轮的重投。」
- 实现（`watermark.IdempotencyKeyLRU`）：`add()` 遇重复键 → `move_to_end` 刷新新鲜度并返回 `False`（调用方据此跳过投递）；`contains()` 是**纯查询**，不刷新；超容量淘汰最久未使用。
- **「重复键算不算一次使用」**：**算**。理由：规格的措辞是「最近 N 条**消息**的幂等键集合」—— 一个重复键对应的是**一条新到的消息**（只是被判定为重复而**未投递**），它属于「最近 N 条消息」，理应占据集合里的一个位置并刷新新鲜度。若把重复键当「不使用」，则一条持续重复出现的告警会因为「首次出现得早」而被当成最老条目淘汰掉，真正久未出现的键反而留在集合里 —— 这正是 FIFO 的缺陷。**另一种读法**（把集合读成「最近 N 个**已投递**的不同键」）也能自洽，但它与规格原文「消息」一词不符，且导致「最近 N 条消息」这一保护窗口在重复投递密集时失效。**本实现取「重复键=一次使用」是更贴规格原文的读法，判定为正确。**
- **`contains()` 不刷新**：**正确**。查询不是「一条消息」，只是读；若查询刷新新鲜度，一条只被反复查询、实际不再投递的键会永久占着容量。
- 两条专门的测试守住了这两个语义（`test_idempotency_lru_refreshes_recency_on_duplicate_add` / `test_idempotency_lru_contains_does_not_refresh_recency`），且都是**可证伪**的（把 `move_to_end` 去掉 / 把 `contains` 改成刷新，都会红）。

**但**：控制器改完语义后，**三处文档仍写着旧的 FIFO 口径**（M-2）。

### 6.2 `DecodeError` 收敛到 `errors.py`（1.2 做的）—— **判定：完全正确**

**独立核实**（读 `tpops_kafka_collector/errors.py` 与 `mapping/decode.py`）：

- `errors.py` 里 `class DecodeError(MappingError)` 是**唯一实现**，`__all__` 已收录。
- `mapping/decode.py` 改为 `from ..errors import DecodeError` 并原样再导出（`__all__` 仍含 `DecodeError`），**不再另起一份同名类** → 全仓只有一个 `DecodeError` 类对象，`except DecodeError` 处处抓得住、`isinstance` 不会分裂。
- `NON_RETRYABLE = (AuthError, ProtocolError, MappingError)` → `DecodeError` 作为 `MappingError` 子类**天然不可重试**（`test_decode_error_is_not_retried_even_though_it_is_a_mapping_error` 守住）。
- 这正是 wave-0 复审要求的修法。

**两处修复均判定为正确，无需返工。**

---

## 7. 测试质量（可证伪性）

| 关注点 | 判定 | 说明 |
|---|---|---|
| **1.4 的 AST 唯一出口守卫** | **能守住常见写法，但覆盖面有洞（M-4）** | 守卫扫 `tpops_kafka_collector/kafka/*.py`（排除 `serializer.py`）里的 `ast.Attribute(attr in ('dumps','dump'), value=Name('json'))` → 在 `kafka/` 里写 `json.dumps(...)` 会被抓住（**能证伪**）。**漏的写法**：① `from json import dumps` 后直接 `dumps(...)`；② `import json as j` 后 `j.dumps(...)`；③ **`kafka/` 之外**的任何模块（`mapping/`、`tpops/`、`watermark.py`…）——即合同任务 1.4 写的「**全仓**只有这一处 `json.dumps` 产出消息」并不在守卫范围内；④ `serializer.py` 本身被整个排除，它内部可以有多个 `json.dumps` 而不被计数。**实测现状是合规的**：生产包内 `json.dumps`/`json.dump` 只有 `serializer.py`（消息）与 `watermark.py`（状态文件，非消息）两处。**建议**见 M-4 |
| **1.8 的 13 种规则类型** | **正常路径与大部分边界路径都测到；边界有缺口（M-5）** | 13 种类型逐个有断言；边界覆盖到：空行不产出、缺列不编 0、`row_policy` 未知抛错、`aggregate` 未实现 `fn` 抛错、`counter_delta` 无基线只记基线 / 归零丢轮 + ERROR / 采集时刻未推进丢轮、`ratio_across` 分母 0 / 视图缺失 / 分子 0 / 保留 2 位、`series_point` 未知 position 抛错、枚举未命中透传 + WARNING、未知枚举 `default='null'`、`gauge` 单位不支持抛错、`top_n` 的 PG 空值位置 + 空串 + 集合来源 + 空输入、`collection_count` 缺集合为 0、`text_prefix_count` 缺字段不计、`instance_field` 缺键给 `None`、`group_by` 与维度绑定不一致抛错 + 重复键 WARNING、未知/缺失 rule type 抛错。**缺口**：`side_value` 的 `fn=max` / `fn=count`、未知 `fn` 抛错、`counter_delta` 的非 `per_row` 路径、`sort_rows` 的纯文本比较分支 —— 这些正是 `rules_cross.py` 覆盖率 **77%**、`rules_sorting.py` **86%** 的来源（M-5） |
| **1.5 的生产者测试** | **✅ 纯 mock，不连 broker** | `FakeProducer` / `FakeFuture` / `FakeAdmin` 全部注入；唯一触及真实类的是 `test_broker_failure_at_startup_is_retryable_transient_error`，它把 `producer_module.KafkaProducer` 猴补丁成 `Boom`（构造即抛 `NoBrokersAvailable`），**不建立任何连接**。`ensure_topics` 的 admin 也全部是假对象 |
| **1.7 的「运行时不需要仓库根目录」** | **✅ 证明够硬（有一处理论缺口，M-10）** | 结构性保证：`PACKAGE_DATA_DIR` 由 `__file__` 推导，**根本不存在**指向仓库根的路径常量。行为性保证：测试猴补丁 `builtins.open` 做间谍 + `chdir` 到 `tmp_path`，断言**打开的每一个文件**都在包内 `mapping/data/` 下 —— 这个断言不是「路径看起来对」，而是「一个包外文件都没被打开」。**理论缺口**：① 用 `startswith(DATA_DIR)` 前缀比较，`<data_dir>_evil` 也会通过；② `os.path.isfile` 用的是 `stat` 而非 `open`，不在间谍范围内（但它只用于**判存在**、随后仍走 `open`） |
| **1.3 的退避序列断言** | **✅ 不会 flaky** | `sleep` 与 `random_source` 都是注入点（`RecordingSleep` / `FixedRandom`），**没有任何真实等待、没有墙钟依赖**；退避序列断言是精确值（`[1.0, 2.0]`、`(1.0,2.0,4.0,8.0)`）。唯一用真 `random` 的用例断言的是**有界区间**（`1.0 ≤ delay ≤ 1.5`，由 `uniform(0,1)` 保证），不可能 flaky |
| **caplog 用例的 logger 名** | **⚠️ 指向已迁走的模块（M-6）** | `test_rules.py` 里两处 `caplog.at_level(..., logger='tpops_kafka_collector.mapping.rules')` —— 拆文件后日志实际由 `rules_deltas` / `rules_series` 发出。断言目前**仍然成立**（`at_level` 只用来设级别，记录经 root handler 被捕获），但指向的是一个不再打日志的模块，脆弱：一旦有人给 `rules_deltas` 显式设了 WARNING 以上级别，测试会静默失效 |

---

## 8. 越界与治理红线核对

| 红线 | 核对结果 |
|---|---|
| **不得改 `execution-contract.md` 与 `tasks.md`** | ✅ **未触碰**。diff 文件清单（83 个文件）里没有这两个文件 → 执行计划未 stale，receipt 可正常签发 |
| **不得改 `data/mapping/*.json` 的内容（除生成物重生成，且必须由生成器产出 + 可复现）** | ✅ 仅 `data/mapping/kafka_field_mapping.json` 变化（36 行，M-3 的涟漪），**由生成器产出**且**可复现**：实跑 `python tools/build_kafka_mapping.py --check` → **exit 0**。其余 `data/mapping/*.json` 未变。包内副本 `tpops_kafka_collector/mapping/data/*.json` 与仓库根**逐字节一致**（`test_package_copies_are_identical_to_the_repo_files`），`observed_columns.json` 亦为生成物且 `--check` exit 0 |
| **不得改 `specs/**`** | ✅ 未触碰。W-6 的延后因此保住了执行计划的有效性 |
| **凭据只走环境变量、不入日志/异常/快照** | ✅ `Config.tpops_password` 用 `field(repr=False)`；`ConfigError` 消息只含变量名/非法值，`TPOPS_PASSWORD` 一律 `<redacted>`；`TpopsClient.__repr__` 不含密码；`client.py` 的日志只打 method/path，不打请求体与响应头。测试里有 `test_password_never_appears_in_logs` / `..._in_exception_messages` / `test_repr_of_client_does_not_leak_password` 三道守卫。生产包内**无任何硬编码凭据**（全包扫描只有 `self._password = password` / `password=config.tpops_password` 两处赋值，无字面量） |
| **不得改 `changes/` 下其他既有文件**（共享执行约定） | ⚠️ 本波次改了 `progress.md`（账本，控制器职责）、`.spec-superflow.yaml`（控制器 `state rebuild`）、`reviews/wave-0-mapping-baseline*.md`（wave-0 的报告留档）。**这三处都在控制器的职责面内，且 `.spec-superflow.yaml` 的 hash 刷新经重算与计划一致**（`b425f6d3…` / `59817bef…`），**wave-0 的 `pass` receipt 仍然有效**（`report_sha256` 与报告文件字节 sha256 实测一致，root 与 scoped 两份 receipt 相同）。**不判越界** |
| **`ruff check tpops_kafka_collector/` 必须为 0** | ✅ **All checks passed!**（退出码 0） |
| **`tests/test_py37_compat.py` 必须通过** | ✅ **77 passed** |
| **不新增依赖** | ✅ 只用了锁内的 `kafka-python==2.0.2` / `requests`；未引入新第三方库 |
| **生产代码不用 `print`** | ✅ 全包扫描零命中 |
| **单文件 < 400 行** | ⚠️ **2 个文件越线**：`tpops/client.py` **403 行**、`tpops_kafka_collector/watermark.py` **404 行**（`loader.py` 399、`producer.py` 399 贴线但未越）。见 M-1 |
| **函数 < 50 行** | ✅ AST 全量扫描生产包，**无一个函数 > 50 行** |
| **3.7 语法红线** | ✅ py37 门禁 77 passed；人工抽查无海象/内建泛型/`zoneinfo` 等 |

---

## 9. 分级清单

### Critical（0）

无。未发现安全漏洞、数据丢失或会导致生产事故的缺陷。

### Important（0）

无。未发现逻辑缺陷、规格违反、合同完成标准未满足、缺失必需测试或越界改动。

> 说明：M-1（两个文件 403/404 行 > 合同 400 行）曾是 Important 的候选。判为 Minor 的理由是
> **先例一致**（wave-0 独立审查把 425 行的 `mapping/rules.py` 定为 Minor 并入台账，而非 Important），
> 且越线幅度 3~4 行、零功能影响。**但**：控制器在 W-2 的裁决里把「单文件 < 400 行」称为「硬约束」，
> 若按该口径执行，这一条应升级为 Important 并在签发 wave-1 receipt **之前**修掉（修法极轻，见 M-1）。
> **本报告把它列为第一条 Minor，并明确标注这一分歧，交由人裁。**

### Minor（12）

| 编号 | 内容 | 处置建议 |
|---|---|---|
| **M-1**（首） | `tpops/client.py` **403 行**、`tpops_kafka_collector/watermark.py` **404 行**，越过合同「单文件 < 400 行」 | **建议在 wave 2 开工前修掉**（各减 4~5 行即可）：把 `client.py` 的 `require_fields` 与 `AUTH_FAILURE_COOLDOWN_SECONDS` 拆到 `tpops/client_errors.py`；把 `watermark.py` 的 `_parse_section` / `_baselines_container` 拆到 `watermark_codec.py`。若按「硬约束」口径执行 → 升级为 Important，须在签 receipt 前修 |
| **M-2**（次） | **三处文档与 LRU 新语义矛盾**：`watermark.py` 模块 docstring 仍写「按插入顺序淘汰，重复键不入队」；`WatermarkStore.idempotency_keys()` docstring 仍写「按插入顺序返回」；`tests/unit/test_watermark.py` 模块 docstring 同样写着「按插入顺序淘汰、重复键不重复入队」 | **建议在 wave 2 开工前修掉**（纯文案）。这三句是控制器 `92e3223` 改语义后的残留，读者按文档建模会得到错误结论（重复键到底算不算一次使用） |
| **M-3** | `SerializerError`（`kafka/serializer.py`）与 `ConfigError`（`config.py`）都在 `errors.py` 异常体系外（S-1 的扩展） | wave 2/3 收进 `errors.py`：`SerializerError` → `MappingError` 子类（不可重试）；`ConfigError` → 启动期专用错误。现状行为正确，只是「碰巧对」 |
| **M-4** | 1.4 的 AST 唯一出口守卫覆盖面不足：只扫 `kafka/` 目录、排除 `serializer.py` 本身、只认 `json.dumps` 属性调用（漏 `from json import dumps` / `import json as j`），合同任务写的「**全仓**只有一处」不在守卫内 | 守卫扩到整个 `tpops_kafka_collector/`，用**白名单**（`serializer.py` 的唯一一处 + `watermark.py` 的状态文件一处）替代「排除 serializer.py」；同时识别 `from json import dumps` 形态 |
| **M-5** | `rules_cross.py` 覆盖率 **77%**、`rules_sorting.py` **86%** —— wave-1 新写的模块低于 wave-0 基线（`rules.py` 92%）；未覆盖 `side_value` 的 `fn=max`/`fn=count`、未知 `fn` 抛错、`counter_delta` 非 `per_row` 路径、纯文本排序分支 | 记入 wave 3 补测清单（合同 wave-3 门槛是「映射层与规则引擎 ≥95%」）。**wave 3 规划时必须覆盖这两个模块**，否则门槛过不了 |
| **M-6** | `test_rules.py` 两处 `caplog.at_level(..., logger='tpops_kafka_collector.mapping.rules')` 指向的模块拆文件后已不再产生日志（实际在 `rules_deltas` / `rules_series`） | 把 logger 名改成实际发日志的模块名。当前断言仍成立，但一旦相关 logger 被显式设级就会静默失效 |
| **M-7** | S-4 的 NaN 前向风险：**`apply_value_type` 的浮点路径对 NaN 不抛**（`round(nan,2)=nan`），NaN 一路到 `dumps(allow_nan=False)` 才炸；且异常抛在 `metric_record()` 里 —— **在生产者之外，不走「投递失败→不推进水位」路径**，会打断整轮 | **写进 wave 2 的验收条目**：「NaN/Inf 一律归一化为 `None` 再进序列化」，并加一条 NaN 的回归测试。wave 1 范围内无 NaN 来源，故不判 Important |
| **M-8** | 独立期望值推导器 `tools/expected_deriver.py` 与被审实现**同波次同改**（`ratio_across` 分母语义 + 空串当空值） | 这是口径裁决的必然结果（不改推导器则期望值表必然分叉）。但「独立第二实现」的价值正在于此处被削弱 → wave 3 重算期望值表时，留意「两边一起错」的风险；对这两处口径补一条**人工独立核对**的证据（而非只靠 `test_expected_values.py` 自洽） |
| **M-9** | wave-2a 的任务 2.1 范围已被 W-1 实质缩窄（七张表 + `observed_columns.json` 已在 1.7 复制进包；2.1 只剩 `merged_sql.json` / `sql_merge.sql` + 同步测试） | 登记进待补清单 T-5，并在 wave-2a 派发前明确 2.1 的剩余范围，避免实施方重复劳动或误以为 2.1 已无内容 |
| **M-10** | `test_load_mapping_never_touches_the_repo_root` 的前缀断言用 `startswith(DATA_DIR)`（`<data_dir>_x` 也会通过）；`os.path.isfile` 的 `stat` 不在 `open` 间谍范围内 | 断言改成 `path == DATA_DIR or path.startswith(DATA_DIR + os.sep)`。理论缺口，实际风险极低 |
| **M-11** | 账本 `progress.md` 的任务级用例数已过期（47/32/15/26/32/26/41/60/26 → 实测 50/32/15/26/32/26/42/60/28） | 更新账本数字（纯记账）。**不要改 `tasks.md` / 合同**（改了会让执行计划 stale） |
| **M-12** | 合同完成标准第 2 条（`main.py` 能起 / 收 SIGTERM / 退出码 0 / 水位文件完整）在 wave 1 **无法判定**（`main.py` 属 wave 3 任务 3.3） | 已获用户裁决归 wave 3。按简报要求仅 ⚠️ 记录，**不要求现在改合同**；但 wave 3 派发时必须把这条重新提为硬门禁 |

---

## 10. 待补清单（要登记进合同输出清单 / 台账，本次**不改** `tasks.md` 与合同）

> 理由与简报一致：改 `tasks.md` / 合同 → `artifacts_hash` / `contract_hash` 变 → 执行计划 stale →
> revision 3 的 receipt 全部作废、wave-0 与 wave-1 都要重审。故本次只登记。

| 编号 | 待登记内容 | 归属 |
|---|---|---|
| **T-1** | `tpops_kafka_collector/mapping/rules_context.py`、`rules_deltas.py`、`rules_naming.py`、`rules_sorting.py`、`rules_row.py`、`rules_series.py`、`rules_cross.py`、`rules_collection.py`、`rules_engine.py`（**9 个**，1.8 拆分的产物） | 合同的 Wave 1 输出清单（现只写了 `mapping/{loader,rules}.py`） |
| **T-2** | `tpops_kafka_collector/kafka/records.py`（1.5 从 `producer.py` 拆出） | 同上（现只写了 `kafka/{serializer,producer}.py`） |
| **T-3** | `tpops_kafka_collector/mapping/data/__init__.py` + `mapping/data/` 下的**8 个 JSON**（7 张表副本 + `observed_columns.json`） | Wave 1 输出清单（`mapping/data/` 现挂在 wave-2a 的 2.1 名下） |
| **T-4** | `tpops_kafka_collector/mapping/rules.py` 的**语义变更**（从「实现」变成「门面」：43 行 re-export）；以及 `mapping/decode.py` 的 `DecodeError` 收敛改动 | Wave 1 输出清单的注解 |
| **T-5** | **wave-2a 任务 2.1 的范围缩窄**：七张表 + `observed_columns.json` 的复制已在 1.7 完成；2.1 只剩 `merged_sql.json` / `sql_merge.sql` 的复制 + `tests/unit/test_mapping_data_sync.py` | wave-2a 派发前明确 |
| **T-6** | **规格改写（独立小变更，收尾后落地）**：① `specs/mapping-and-rules/spec.md:156`「分母为 0 时结果为 0」→ 改为「分母不可用（取不到 / 为 0）→ **不产出**；分子为 0、分母为正 → 产出 0」；② 补写 `raw_filtered` 的**多命中取首个匹配行**语义（R-M1 规格侧欠账） | **必须在 wave 4 交付文档之前**落地（交付文档要写口径） |
| **T-7** | 新增生成器 `tools/build_observed_columns.py`（不在合同的输出清单里，但属 wave 1 产出） | 登记 |

---

## 11. 审查方法与未覆盖项声明

- **只读 diff 一次**；diff 的上下文行即被改文件内容。唯一例外：为判定下列**具体命名的风险**，在 worktree 内做了聚焦检查（均只读）：
  1. *W-5 的前提*（kafka-python 2.0.2 的 `DEFAULT_CONFIG` 是否含 `enable_idempotence`）→ 读 `KafkaProducer.DEFAULT_CONFIG`（27 键，**不含**）；
  2. *W-6 的推理链*（改 `specs/**` 是否真会 stale 并作废 receipt）→ 读 spec-superflow 1.0.0 的 `hash.mjs` 与 `execution-plan.mjs`（`computeArtifactsHash` 的覆盖范围、`execution-plan.mjs:76` 的 stale 判定、`:197` 的 receipt 承认条件、`validateReviewReportEvidence` 的 sha 口径）；
  3. *当前状态是否干净*（`.spec-superflow.yaml` 在 wave-1 里被改过 hash）→ 用插件源码**重算** `artifacts_hash` / `contract_hash`，与 `execution-plan.json`、yaml 三方比对（**三者一致**）；
  4. *wave-0 的 pass receipt 是否仍有效* → 实算 `wave-0-mapping-baseline.md` 的字节 sha256，与 receipt 的 `report_sha256` 比对（**一致**），并比对 root / plan-scoped 两份 receipt（**相同**）；
  5. *`group_by` 是否真被执行 / 会不会运行时抛 `RuleError`* → 解析真实 `metric_rules.json`，比对 34 条带 `group_by` 的规则与各自的 `dimension_bindings`（**无一条冲突**）；
  6. *序列化器唯一出口的实际状况* → 全包扫描 `json.dumps` / `json.dump`（只有 `serializer.py` 与 `watermark.py` 两处）；
  7. *`data/mapping/kafka_field_mapping.json` 的改动是否可复现* → 跑生成器的 `--check`（**exit 0**，只读不写）；
  8. *单文件行数 / 函数长度 / `print` / 硬编码凭据* → AST + 全文扫描；
  9. *13 种 rule type 与 `HANDLERS` 是否一一对应* → 解析真实映射表比对（**完全一致**）。
- **未跑 git 命令**（遵守简报约定）；未修改任何被审查的文件；未签发 receipt；未改 `.spec-superflow.yaml` / 合同 / `tasks.md`。
- **一处无法在本次完全核实的项**：`data/mapping/kafka_field_mapping.json` 的 36 行改动内容**不在 diff 正文里**（被「3 类大生成物」排除规则排除）。我以生成器 `--check`（exit 0）间接证明它可由生成器复现，并以 `message_count=0` / `sample_message=null` 核对了两个比率指标的内容与新口径一致。**逐字节 diff 比对留给控制器**（`git diff` 后重跑 `python tools/build_kafka_mapping.py` 看 `git diff` 是否为空）。

---

## 12. 放行建议

1. **可以签发 wave-1 的 `pass` receipt**（Critical 0 / Important 0；合同 Wave 1 门禁按已裁决口径判定成立）。
2. **签发前建议先修 M-1 与 M-2**（各 5 分钟内的改动：两个文件各减 4~5 行；三句文案改写）。若不修，须在本报告与账本里显式留档这两条 Minor，并把 M-1 的升级判定权交给人。
3. **wave 2 派发前必须带上**：M-7（NaN → `null`）、M-3（异常收敛）、M-4（唯一出口守卫扩面）、M-6（caplog logger 名）、M-5 的一半（`rules_cross` / `rules_sorting` 补测）。
4. **wave 4 交付文档前必须落地 T-6 的规格改写**（否则交付文档与规格互相矛盾）。
5. **wave 3 门槛提醒**：`rules_cross.py` 77% / `rules_sorting.py` 86% 是当前离「映射层与规则引擎 ≥95%」最近的两块缺口，wave 3 规划时要留出补测窗口。

---

*审查者：独立审查者（spec-superflow）。本报告由控制器读取以签发 receipt；报告本身不构成 receipt。*
