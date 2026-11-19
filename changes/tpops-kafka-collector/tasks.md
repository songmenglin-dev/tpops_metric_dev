# 实现任务：TPOPS → ITSCADA Kafka 采集端

编号沿用开发方案 §7.2 的 WBS（W01~W24），另加 W00（映射层基准改造，前置）。
每个任务给出：受影响的路径或边界、可观察的完成结果、证明命令、依赖。

**测试目录约定**：既有 10 个测试文件（164 个用例）**留在 `tests/` 根不搬迁**；
本次新增的测试按 `tests/unit/` `tests/integration/` `tests/regression/` `tests/fixtures/`
分目录（`pytest.ini` 的 `testpaths = tests` 会递归收集）。
`tests/test_merged_sql_mapping.py` 建在 `tests/` 根，因为它是波次 0 的证明、且要在
夹具目录建立之前就能跑。

## 交付与证明

| 波次 | 交付结果 | 依赖 | 证明 |
|---|---|---|---|
| 0 | 映射层能解合并 SQL 形态，82 个 B 类指标产出真值；告警幂等键含 `status`；跨视图比率不再为 0；开发方案的契约章节同步 | 无 | `pytest tests/test_merged_sql_mapping.py tests/test_rules.py tests/test_response_rules.py -q` |
| 1 | `python -m tpops_kafka_collector.main` 能起、能收信号、空跑不报错；平台层具备「认证失败不重试」 | 波次 0 | `pytest tests/unit tests/integration -q` + 手工起进程收 SIGTERM 看退出码 |
| 2 | A/B/C/D/E 五条通道全部产出数据 | 波次 1 | `pytest tests/regression -q`（98/98） |
| 3 | 常驻进程 + 水位推进 + 优雅退出 + 全量测试与门禁 | 波次 2 | `pytest --cov=tpops_kafka_collector --cov-fail-under=80 -q` + 杀进程重启演练 |
| 4 | 交付文档齐备 | 波次 3 | 人工核对交付清单 |

## 任务

### 波次 0 —— 生产代码落位、映射层基准改造与契约同步（前置，W00）

> **顺序约束**：先复制进包，再在包内改造。生产代码全部在 `tpops_kafka_collector/`；
> `tools/` 与 `data/` 只作参考。

- [ ] **0.1 建包并复制映射层**：新建 `tpops_kafka_collector/` 包骨架（`__init__.py`、
  `mapping/__init__.py`、`errors.py`、`models.py`），
  把 `tools/mappings/{table,rules,decode,message,nodes}.py` **复制**进 `tpops_kafka_collector/mapping/`
  （本步不改逻辑，只搬家）。
  完成后包内有一份与 `tools/mappings/` 等价的实现。
  证明：`pytest tests/ -q` 全绿（此时两边都还在，测试指向不变）
- [ ] **0.2 在包内加 `merged_views` 段与合并形态解码**：在 `tpops_kafka_collector/mapping/decode.py`
  新增 `decode_merged` / `merge_pages`；改 `tools/build_response_rules.py` 生成
  `merged_views` 段（11 个视图，各带 `json_column` / `paged` / `sql_ref`；
  未声明 `paged` 的按 `false`），并校验它与 `data/merged_sql/merged_sql.json`
  的视图名一一对应。
  完成后「一行 11 个 json_agg 列」的返回体能解出每个视图的行列表，空页得到 `[]`，
  坏 JSON 抛 `DecodeError`（归入 `MappingError`，不可重试）。
  证明：`pytest tests/test_merged_sql_mapping.py -q`
- [ ] **0.3 在包内修告警幂等键**：在包内定义 `AlarmRecord.idempotency_key`（键为
  `(alarm_id, instance_id, first_alarm_time, status)`）；
  改 `tools/build_itscada_mapping.py` 的 `build_alarm_rules()`，在 `alarm_rules.json`
  增加 `idempotency` 段；删掉 `tools/build_response_rules.py` 里从未被写出的死代码
  `IDEMPOTENCY['alarm']`。完成后同一告警的触发与恢复是两个不同的键。
  证明：`pytest tests/test_rules.py -q -k idempotency`
- [ ] **0.4 在包内修跨视图比率**：让包内的 `decode_merged` 同时填 `ctx.rows_by_key`。
  完成后 `gaussdb.Total_pct` 与 `GAUSSDB.session_usage` 拿到真实比率而非 0。
  证明：`pytest tests/test_merged_sql_mapping.py -q -k cross_view`
- [ ] **0.5 合并形态的列校验与证据重推导**：在 `tools/build_response_rules.py` 增加一条针对
  合并形态的列名校验，数据源为 `data/merged_sql/sql_merge_probe_result.json` 与
  `data/merged_sql/merged_sql.json`（实测留档为准，映射表为声明）。
  完成后映射表与真实返回的列名不一致时生成器直接报错；且重跑生成器后
  `git diff` 为空（映射表可重复生成）。
  证明：`pytest tests/test_response_rules.py tests/test_generated_rules.py -q`
  与 `python tools/build_response_rules.py && git diff --exit-code data/mapping/response_rules.json`
- [ ] **0.6 同步开发方案的契约章节**：改
  `docs/开发方案-GaussDB指标与告警采集.md` 的 §3.1（消息信封改为客户字段表的 7/13 字段）、
  §3.7 与 §4.5.4（告警幂等键加 `status`、去 `node_id`）、§4.4 与 §4.4.2
  （1 条合并 SQL + 102400 B 上限 + 分页）、§5.5（新增 `CMD_PAGE_SIZE`、
  `CMD_CONCURRENCY` 默认值改 2）、§5.6.4（校验来源改为实测留档，删掉已删目录的引用）、
  §6.2（回归样本补合并形态）、§5.1（目录结构注明 `tools/`、`data/` 只作参考）、
  **§1.7（验收口径下调）**：D4「连续运行 ≥24 小时」不在本变更内（属 P5 联调），
  按 §7.1.1「每砍一项都必须同步下调 §1.7 的验收口径，而不是偷偷少投数据」，
  在 §1.7 注明降级为「连续 2 小时 + 杀进程重启演练」；
  D7 的**真实平台演练**（连续 3 次错误密码后账号未被锁定）同属 P5，
  本变更只交付其单元形态（注入 401 断言不发起第二次认证）。
  完成后文档与实现、映射表三处口径一致，且验收口径的裁剪是显式记录而非隐去。
  证明：`grep -n "11 条合并 SQL" docs/开发方案-GaussDB指标与告警采集.md` 的结果中，
  与本次改动相关的章节不再出现；`pytest tests/test_generated_rules.py -q`
- [ ] **0.7 把依赖切到包内并删除参考副本**：把 `tools/` 下 10+ 个脚本与 `tests/` 下 8 个
  测试文件的 `import tools.mappings` 全部改为 `import tpops_kafka_collector.mapping`；
  `tools/` 脚本的 `sys.path` 需同时加入项目根；**删除 `tools/mappings/`**（git 历史保留）；
  加一条测试断言 `tpops_kafka_collector/` 下无任何指向 `tools` / `data` 的 import。
  完成后仓库里只剩一份映射层实现，且生产包自洽。
  证明：`pytest tests/ -q` 全绿 + `pytest tests/unit/test_package_boundary.py -q`

### 波次 1 —— 骨架与平台层（W01~W09）

- [ ] **1.1 配置加载与启动校验**（W01）：新建 `tpops_kafka_collector/config.py`，从环境变量加载
  §5.5 全部配置项，校验必填项与数值范围（窗口 ≥1、并发 1~10、页容量 ≤1000），
  缺失即快速失败且不发任何请求。完成后 `CMD_CONCURRENCY` 默认 2、`CMD_PAGE_SIZE` 默认 100。
  证明：`pytest tests/unit/test_config.py -q`
- [ ] **1.2 DTO 与异常体系**（W02）：新建 `tpops_kafka_collector/models.py`（`MetricPoint` /
  `AlarmDetail` / `AlarmRecord`，frozen dataclass，维度用排序后的元组保证可哈希；
  `AlarmRecord.idempotency_key` 按波次 0 定的新键计算）与 `tpops_kafka_collector/errors.py`
  （`TpopsError` 及 `AuthError` / `TransientError` / `ProtocolError` / `MappingError` /
  `ProduceError` 五个子类 + 可重试判定表；`DecodeError` 归入 `MappingError`）。
  证明：`pytest tests/unit/test_models.py -q`
- [ ] **1.3 重试与退避框架**（W03）：新建 `tpops_kafka_collector/retry.py`，指数退避 + 抖动，
  重试判定集中于此。完成后 `AuthError`/`ProtocolError`/`MappingError`（含 `DecodeError`）
  立即上抛，`TransientError`/`ProduceError`/超时按 1s→2s→4s→8s 重试。
  证明：`pytest tests/unit/test_retry.py -q`
- [ ] **1.4 JSON 序列化器**（W04）：新建 `tpops_kafka_collector/kafka/serializer.py`，实现
  `itscada_field_map.json` 的信封（指标 7 字段 / 告警 13 字段），`ensure_ascii=False`、
  字段顺序固定、缺失值一律 `null`、时间 13 位毫秒、业务维度落 `tags.dimension_values`、
  值按客户表【数据类型】序列化（整数/浮点/字符/文本）。
  完成后全仓只有这一处 `json.dumps` 产出消息。
  证明：`pytest tests/unit/test_serializer.py -q`
- [ ] **1.5 Kafka 生产者**（W05）：新建 `tpops_kafka_collector/kafka/producer.py`，PLAINTEXT、
  `acks=all`、`enable_idempotence=False`、压缩与分区数可配、指标与告警分 Topic、
  Key 分别为 `instance_id` 与 `alarm_id`。完成后失败抛 `ProduceError`，
  调用方得以据此不推进水位。
  证明：`pytest tests/integration/test_producer.py -q`
- [ ] **1.6 TPOPS HTTP 客户端**（W06）：新建 `tpops_kafka_collector/tpops/client.py`，mini-token 认证
  （token 从响应头取）、401 强制刷新重试一次、`verify_tls=False` 默认、错误四分类。
  完成后认证失败不发起第二次认证。
  证明：`pytest tests/unit/test_client.py -q`
- [ ] **1.7 映射表加载与启动校验**（W07）：新建 `tpops_kafka_collector/mapping/loader.py`
  加载**包内** `tpops_kafka_collector/mapping/data/*.json` 并做启动期四类校验（来源列存在于实测留档的
  实际返回列、可提供指标齐全、条目对应真实指标、占位符与维度绑定一一对应）。
  完成后校验全部在包内完成，运行时不需要仓库根目录。
  证明：`pytest tests/unit/test_mapping_loader.py -q`
- [ ] **1.8 规则引擎**（W08）：在 `tpops_kafka_collector/mapping/rules.py` 实现映射表在用的全部
  13 种 `value_rule.type`（`raw` / `raw_filtered` / `aggregate` / `counter_delta` /
  `ratio_across` / `series_point` / `enum_map` / `gauge` / `top_n` / `raw_rows` /
  `collection_count` / `text_prefix_count` / `instance_field`），差分用实际采集时刻差，
  首轮无基线只记基线，计数归零丢弃本轮并记 ERROR。
  证明：`pytest tests/unit/test_rules.py -q`
- [ ] **1.9 水位与差分基线持久化**（W09）：新建 `tpops_kafka_collector/watermark.py`，
  写临时文件 + `os.replace` 的原子写，保存指标水位、差分基线、告警水位、幂等键 LRU。
  完成后进程在写文件途中被杀不会留下半截 JSON。
  证明：`pytest tests/unit/test_watermark.py -q`

### 波次 2 —— 五条采集通道（W10~W18）

- [ ] **2.1 映射数据复制进包**（W13-搬）：把 `data/mapping/` 的六张表、
  `data/merged_sql/merged_sql.json`（11 个视图定义 + 刷新档位 + paging 段）与
  `data/merged_sql/sql_merge.sql` **复制**进 `tpops_kafka_collector/mapping/data/`；
  加一条测试断言两份副本内容一致。完成后采集端包自洽，不依赖仓库根目录。
  证明：`pytest tests/unit/test_mapping_data_sync.py -q`
- [ ] **2.2 合并 SQL 的运行时渲染**（W13-渲染）：把 `tools/build_sql_merge.py` 的渲染逻辑
  **复制**进 `tpops_kafka_collector/mapping/sqlgen.py`，按 `merged_sql.json` 的视图定义与 `paging` 段
  渲染出带指定 `limit`/`offset` 的 SQL 文本（页容量取自 `CMD_PAGE_SIZE`）；
  加一条测试断言渲染出的第 1 页与 `data/merged_sql/sql_merge.sql` 逐字符一致。
  完成后采集端能按 offset 翻页。
  证明：`pytest tests/unit/test_sqlgen.py -q`
- [ ] **2.3 A 类：窗口计算与时间戳切片**（W10）：新建 `tpops_kafka_collector/tpops/metrics.py`，
  窗口 ≥1 小时、`end_time` 回退 5 分钟、`is_latest_point_only` 为字符串 `"false"`、
  12 个分组码硬编码、按档位切片并跳过 null 点。
  证明：`pytest tests/unit/test_metrics_window.py -q`
- [ ] **2.4 A 类采集器**（W11）：新建 `tpops_kafka_collector/collectors/monitor.py`，
  多 series 按组件逐条产出、维度键 `component_id` 归入 tags、空分组记告警日志。
  证明：`pytest tests/integration/test_collector_monitor.py -q`
- [ ] **2.5 命令中心三跳客户端**（W12）：新建 `tpops_kafka_collector/tpops/command_center.py`，
  `statistics.running == 0` 判结束、`result` 二次 `json.loads`、只在 master 下发、
  **节点清单按 `node_id` 去重**、节点清单按实例缓存 1 小时、并发可配（默认 2）、
  单条失败隔离、逐条重试。
  证明：`pytest tests/unit/test_command_center.py -q`
- [ ] **2.6 B 类采集器**（W14）：新建 `tpops_kafka_collector/collectors/command_center.py`，
  下发 1 条合并 SQL、按 `paging` 推进分页 offset、调用 `merge_pages` 拼整视图、
  按 `node_scoped` 归集多 master 结果（`true` 各 master 各自一份、
  `false` 只取第一个 master）、结果长度 >90% 上限记 WARNING、超限时缩小容量重试。
  证明：`pytest tests/integration/test_collector_command_center.py -q`
- [ ] **2.7 C 类：诊断优化**（W15）：新建 `tpops_kafka_collector/tpops/diagnostic.py` 与
  `collectors/diagnostic.py`，slow-sql-list / top-sql-list，
  集合条数与按 `sql_text` 前缀分类计数。
  证明：`pytest tests/integration/test_collector_diagnostic.py -q`
- [ ] **2.8 D 类：实例元数据**（W16）：新建 `tpops_kafka_collector/tpops/instances.py` 与
  `collectors/instance.py`，`GET /rds/v3.2/instances` 取 `engine_version`，1 小时档。
  证明：`pytest tests/integration/test_collector_instance.py -q`
- [ ] **2.9 E 类：告警拉取与归并**（W17）：新建 `tpops_kafka_collector/tpops/alarms.py` 与
  `collectors/alarm.py`，`limit ≤ 100` 分页取全、活动与历史各取收敛与明细、
  明细按 `alarm_id + instance_id` 归并进 `details`、
  收敛形态下不依赖平台侧 `alarm_id`/`instance_id` 过滤（改本地归并）、
  `first_alarm_time` 水位增量、30 天超期显式告警、节点身份回填
  （回填不到落第一个 master CN）。
  证明：`pytest tests/integration/test_collector_alarm.py -q`
- [ ] **2.10 慢 SQL 脱敏与截断**（W18）：新建 `tpops_kafka_collector/mapping/masking.py`，
  字面量替换为 `?` 并截断到 512 字符，`SQL_MASKING_ENABLED` 可关。
  证明：`pytest tests/unit/test_masking.py -q`

### 波次 3 —— 调度、装配与测试（W19~W23）

- [ ] **3.1 调度器**（W19）：新建 `tpops_kafka_collector/scheduler.py`，到期驱动表、
  按 SQL 覆盖指标的最高档定周期（同长度档位视为同档）、tick 内并发执行、
  汇总、水位推进、单组失败不拖垮其他组。完成后 60 秒档单轮墙钟 ≤25 秒。
  证明：`pytest tests/integration/test_scheduler.py -q`
- [ ] **3.2 健康端点**（W20）：新建 `tpops_kafka_collector/health.py`，`/healthz` 与 `/readyz`，
  依赖不可用时 `/readyz` 返回 503 且不泄露凭据。
  证明：`pytest tests/integration/test_health.py -q`
- [ ] **3.3 入口装配与信号处理**（W21）：新建 `tpops_kafka_collector/main.py`，装配依赖、
  启动调度循环、捕获 SIGTERM/SIGINT、优雅退出（等在途请求 ≤30 秒、刷盘水位、
  flush producer、退出码 0）、每轮结构化汇总日志。
  证明：`pytest tests/integration/test_main.py -q` + 手工起进程发 SIGTERM 看退出码与水位文件
- [ ] **3.4 回归夹具与期望值表**（W22）：把 `data/metric_response/` 98 份留档与
  `data/merged_sql/sql_merge_probe_result.json` 转成 `tests/fixtures/` 下的固定样本，
  并生成逐指标期望值表（由留档自动生成，不手抄）。
  完成后「值等于期望值」成为回归判据（不是「非零」判据）。
  证明：`pytest tests/regression -q`
- [ ] **3.5 全量测试与门禁**（W23）：补齐单元/集成/回归三层，跑通覆盖率门禁
  （整体 ≥80%、映射层与规则引擎 ≥95%）与 3.7 语法门禁。
  证明：`pytest --cov=tpops_kafka_collector --cov-report=term-missing --cov-fail-under=80 -q`
  与 `pytest tests/test_py37_compat.py -q`

### 波次 4 —— 交付文档（W24）

- [ ] **4.1 交付文档**（W24）：产出逐条口径说明（98 个指标各一句，
  含 `node_scoped` 归集口径的说明）、客户指标英文名疑似 OCR 错字清单、
  3 个不可提供指标的书面清单与理由、部署运行说明（含环境变量表与 `.env.example`）。
  证明：`pytest tests/unit/test_delivery_docs.py -q`（断言清单文件与
  `data/mapping/` 中的不可提供指标一致）

## 实施备注

- **接口先定**：波次 1 的 `models.py`（DTO 字段与幂等键）与 `kafka/serializer.py`
  （信封字段顺序）是波次 2、3 的共同依赖，须在 1.2 / 1.4 完成并冻结后再并行推进其余任务。
- **任务级依赖**（除波次级外）：
  0.5 依赖 0.1~0.4；1.2 依赖 0.2（幂等键）；1.7 依赖 0.1、0.3（解码路径）；
  1.8 依赖 1.7；2.2 依赖 2.1；2.6 依赖 2.2、2.5；2.9 依赖 0.2；
  3.1 依赖 1.5、1.9、2.4、2.6、2.7、2.8、2.9；3.3 依赖 3.1、3.2。
  2.6 依赖 1.8（规则引擎）；3.4 依赖 2.1、2.2（夹具）。
- **生产代码只有一份**：决策 2 定下 `tpops_kafka_collector/` 是唯一生产代码。
  波次 0 的 0.7 会把 `tools/` 与 `tests/` 的 import 全部切到包内并删除 `tools/mappings/`，
  从此仓库里不再有「映射层的第二份实现」—— 双副本分叉是静默走样的主要来源，
  与本次要修的「静默出 0」同属一类风险。
- **映射数据仍是两份**：`data/mapping/`、`data/merged_sql/` 是参考与发布源，
  `tpops_kafka_collector/mapping/data/` 是生产用的副本（决策 9），由 2.1 的一致性测试守住防漂移。
  数据文件本身是**声明**（不像代码那样会分叉），保留两份可接受。
- **既有 164 个测试不搬迁**：留在 `tests/` 根，本次新增测试按四层分目录。
- **裁剪预案**（若工期吃紧，按开发方案 §7.1.1 顺序让出范围，且必须同步下调验收口径）：
  先砍 2.9（告警采集器）→ 再砍 2.7/2.8（C/D 类）→ 再砍 24 小时连续运行观察。
  不可裁剪：波次 0、2.6（B 类采集器）、3.1（水位与重启续传）。
- **客户未决项**：慢 SQL 外发口径按最严实现（默认脱敏）；告警字段规范若在波次 3 前仍未回，
  按客户字段表的 13 个字段全量输出并书面标注范围变更；
  Kafka 地址与 Topic 名若仍未回，本变更交付到「可在真实环境跑一轮」为止。
- **RED/GREEN 证据、review 回执与调度细节**属 execution contract / task brief，不在本文件重复。
