# 实现任务：TPOPS → ITSCADA Kafka 采集端

编号沿用开发方案 §7.2 的 WBS（W01~W24），另加 W00（映射层基准改造，前置）。
每个任务给出：受影响的路径或边界、可观察的完成结果、证明命令、依赖。

## 交付与证明

| 波次 | 交付结果 | 依赖 | 证明 |
|---|---|---|---|
| 0 | 映射层能解合并 SQL 形态，82 个 B 类指标产出真值；告警幂等键含 `status`；跨视图比率不再为 0 | 无 | `pytest tests/test_merged_sql_mapping.py tests/test_rules.py -q` |
| 1 | `python -m tpops_kafka.main` 能起、能收信号、空跑不报错；平台层具备「认证失败不重试」 | 波次 0 | `pytest tests/unit tests/integration -q` + 手工起进程收 SIGTERM 看退出码 |
| 2 | A/B/C/D/E 五条通道全部产出数据 | 波次 1 | 回归层 98/98 通过（`pytest tests/regression -q`） |
| 3 | 常驻进程 + 水位推进 + 优雅退出 + 全量测试 | 波次 2 | `pytest --cov=tpops_kafka --cov-fail-under=80 -q` + 杀进程重启演练 |
| 4 | 交付文档齐备 | 波次 3 | 人工核对交付清单 |

## 任务

### 波次 0 —— 映射层基准改造（前置，W00）

- [ ] **0.1 加 `merged_views` 段与合并形态解码**（W00a）：改 `tools/mappings/decode.py` 新增 `decode_merged` / `merge_pages`，改 `tools/build_response_rules.py` 生成 `merged_views` 段并校验它与 `data/merged_sql/merged_sql.json` 的视图名一一对应。完成后「一行 11 个 json_agg 列」的返回体能解出每个视图的行列表，空页得到 `[]`，坏 JSON 抛 `DecodeError`。证明：`pytest tests/test_merged_sql_mapping.py -q`
- [ ] **0.2 修告警幂等键**（W00b）：改 `tools/build_itscada_mapping.py` 的 `build_alarm_rules()`，在 `alarm_rules.json` 增加 `idempotency` 段，键为 `(alarm_id, instance_id, first_alarm_time, status)`；删掉 `tools/build_response_rules.py` 里从未被写出的死代码 `IDEMPOTENCY['alarm']`。完成后同一告警的触发与恢复是两个不同的键。证明：`pytest tests/test_rules.py -q -k idempotency`
- [ ] **0.3 修跨视图比率**（W00c）：改 `tools/mappings/decode.py` 让 `decode_merged` 同时填 `ctx.rows_by_key`。完成后 `gaussdb.Total_pct` 与 `GAUSSDB.session_usage` 拿到真实比率而非 0。证明：`pytest tests/test_merged_sql_mapping.py -q -k cross_view`
- [ ] **0.4 合并形态的列校验与证据重推导**（W00d）：在 `tools/build_response_rules.py` 增加一条针对合并形态的列名校验（数据源 `data/merged_sql/sql_merge_probe_result.json` 与 `merged_sql.json`）。完成后映射表与真实返回的列名不一致时生成器直接报错。证明：`pytest tests/test_response_rules.py tests/test_generated_rules.py -q`

### 波次 1 —— 骨架与平台层（W01~W09）

- [ ] **1.1 配置加载与启动校验**（W01）：新建 `tpops_kafka/config.py`，从环境变量加载 §5.5 全部配置项，校验必填项与数值范围（窗口 ≥1、并发 1~10），缺失即快速失败且不发任何请求。证明：`pytest tests/unit/test_config.py -q`
- [ ] **1.2 DTO 与异常体系**（W02）：新建 `tpops_kafka/models.py`（`MetricPoint` / `AlarmDetail` / `AlarmRecord`，frozen dataclass，维度用排序后的元组保证可哈希）与 `tpops_kafka/errors.py`（`TpopsError` 及五个子类 + 可重试判定表）。证明：`pytest tests/unit/test_models.py -q`
- [ ] **1.3 重试与退避框架**（W03）：新建 `tpops_kafka/retry.py`，指数退避 + 抖动，重试判定集中于此，不散落到各模块。完成后 `AuthError`/`ProtocolError`/`MappingError` 立即上抛，`TransientError`/`ProduceError`/超时按 1s→2s→4s→8s 重试。证明：`pytest tests/unit/test_retry.py -q`
- [ ] **1.4 JSON 序列化器**（W04）：新建 `tpops_kafka/kafka/serializer.py`，实现 §3.1 信封（指标 14 字段 / 告警 21 字段），`ensure_ascii=False`、字段顺序固定、缺失值一律 `null`、时间 13 位毫秒、`dimensions` 传 `{}`。完成后全仓只有这一处 `json.dumps` 产出消息。证明：`pytest tests/unit/test_serializer.py -q`
- [ ] **1.5 Kafka 生产者**（W05）：新建 `tpops_kafka/kafka/producer.py`，PLAINTEXT、`acks=all`、`enable_idempotence=False`、压缩可配、指标与告警分 Topic、Key 分别为 `instance_id` 与 `alarm_id`。完成后失败抛 `ProduceError` 且调用方得以据此不推进水位。证明：`pytest tests/integration/test_producer.py -q`
- [ ] **1.6 TPOPS HTTP 客户端**（W06）：新建 `tpops_kafka/tpops/client.py`，mini-token 认证（token 从响应头取）、401 强制刷新重试一次、`verify_tls=False` 默认、错误四分类。完成后认证失败不发起第二次认证。证明：`pytest tests/unit/test_client.py -q`
- [ ] **1.7 映射层搬入与表加载**（W07）：把 `tools/mappings/{table,rules,decode,message,nodes}.py` 搬进 `tpops_kafka/mapping/`，新建 `tpops_kafka/mapping/loader.py` 加载 `tpops_kafka/mapping/data/*.json` 并做启动期四类校验（来源列存在、可提供指标齐全、条目对应真实指标、占位符与维度绑定一一对应）。证明：`pytest tests/unit/test_mapping_loader.py -q`
- [ ] **1.8 规则引擎**（W08）：在 `tpops_kafka/mapping/rules.py` 实现 7 类规则（raw/aggregate/ratio/counter_delta/gauge/enum_map/top_n），差分用实际采集时刻差，首轮无基线只记基线，计数归零丢弃本轮并记 ERROR。证明：`pytest tests/unit/test_rules.py -q`
- [ ] **1.9 水位与差分基线持久化**（W09）：新建 `tpops_kafka/watermark.py`，写临时文件 + `os.replace` 的原子写，保存指标水位、差分基线、告警水位、幂等键 LRU。完成后进程在写文件途中被杀不会留下半截 JSON。证明：`pytest tests/unit/test_watermark.py -q`

### 波次 2 —— 五条采集通道（W10~W18）

- [ ] **2.1 映射数据文件随包发布**（W13-搬）：把 `data/mapping/` 的六张表与 `data/merged_sql/sql_merge.sql` 复制进 `tpops_kafka/mapping/data/`，并加一条测试断言两份副本内容一致。完成后采集端包自洽，不依赖仓库根目录。证明：`pytest tests/unit/test_mapping_data_sync.py -q`
- [ ] **2.2 A 类：窗口计算与时间戳切片**（W10）：新建 `tpops_kafka/tpops/metrics.py`，窗口 ≥1 小时、`end_time` 回退 5 分钟、`is_latest_point_only` 为字符串 `"false"`、12 个分组码硬编码、按档位切片并跳过 null 点。证明：`pytest tests/unit/test_metrics_window.py -q`
- [ ] **2.3 A 类采集器**（W11）：新建 `tpops_kafka/collectors/monitor.py`，多 series 按组件逐条产出、维度键 `COMPONENT_ID` 归入 tags、空分组记告警日志。证明：`pytest tests/integration/test_collector_monitor.py -q`
- [ ] **2.4 命令中心三跳客户端**（W12）：新建 `tpops_kafka/tpops/command_center.py`，`statistics.running == 0` 判结束、`result` 二次 `json.loads`、只在 master 下发、节点清单按实例缓存 1 小时、并发可配、单条失败隔离、逐条重试。证明：`pytest tests/unit/test_command_center.py -q`
- [ ] **2.5 B 类采集器**（W14）：新建 `tpops_kafka/collectors/command_center.py`，下发 1 条合并 SQL、按 `paging` 推进分页 offset、调用 `merge_pages` 拼整视图、结果长度 >90% 上限记 WARNING、超限时缩小容量重试。证明：`pytest tests/integration/test_collector_command_center.py -q`
- [ ] **2.6 C 类：诊断优化**（W15）：新建 `tpops_kafka/tpops/diagnostic.py` 与 `collectors/diagnostic.py`，slow-sql-list / top-sql-list，集合条数与按 `sql_text` 前缀分类计数。证明：`pytest tests/integration/test_collector_diagnostic.py -q`
- [ ] **2.7 D 类：实例元数据**（W16）：新建 `tpops_kafka/tpops/instances.py` 与 `collectors/instance.py`，`GET /rds/v3.2/instances` 取 `engine_version`，1 小时档。证明：`pytest tests/integration/test_collector_instance.py -q`
- [ ] **2.8 E 类：告警拉取与归并**（W17）：新建 `tpops_kafka/tpops/alarms.py` 与 `collectors/alarm.py`，`limit ≤ 100` 分页取全、活动与历史各取收敛与明细、明细按 `alarm_id + instance_id` 归并进 `details`、`first_alarm_time` 水位增量、30 天超期显式告警、节点身份回填（回填不到落第一个 master CN）。证明：`pytest tests/integration/test_collector_alarm.py -q`
- [ ] **2.9 慢 SQL 脱敏与截断**（W18）：新建 `tpops_kafka/mapping/masking.py`，字面量替换为 `?` 并截断到 512 字符，`SQL_MASKING_ENABLED` 可关。证明：`pytest tests/unit/test_masking.py -q`

### 波次 3 —— 调度、装配与测试（W19~W23）

- [ ] **3.1 调度器**（W19）：新建 `tpops_kafka/scheduler.py`，到期驱动表、按 SQL 覆盖指标的最高档定周期、tick 内并发执行、汇总、水位推进、单组失败不拖垮其他组。完成后 60 秒档单轮墙钟 ≤25 秒。证明：`pytest tests/integration/test_scheduler.py -q`
- [ ] **3.2 健康端点**（W20）：新建 `tpops_kafka/health.py`，`/healthz` 与 `/readyz`，依赖不可用时 `/readyz` 返回 503 且不泄露凭据。证明：`pytest tests/integration/test_health.py -q`
- [ ] **3.3 入口装配与信号处理**（W21）：新建 `tpops_kafka/main.py`，装配依赖、启动调度循环、捕获 SIGTERM/SIGINT、优雅退出（等在途请求 ≤30 秒、刷盘水位、flush producer、退出码 0）、每轮结构化汇总日志。证明：`pytest tests/integration/test_main.py -q` + 手工起进程发 SIGTERM 看退出码与水位文件
- [ ] **3.4 回归夹具**（W22）：把 `data/metric_response/` 98 份留档与 `data/merged_sql/sql_merge_probe_result.json` 转成 `tests/fixtures/` 下的固定样本，并生成逐指标期望值表（由留档自动生成，不手抄）。证明：`pytest tests/regression -q`
- [ ] **3.5 全量测试与门禁**（W23）：补齐单元/集成/回归三层，跑通覆盖率门禁（整体 ≥80%、映射层与规则引擎 ≥95%）与 3.7 语法门禁。证明：`pytest --cov=tpops_kafka --cov-report=term-missing --cov-fail-under=80 -q` 与 `pytest tests/test_py37_compat.py -q`

### 波次 4 —— 交付文档（W24）

- [ ] **4.1 交付文档**（W24）：产出逐条口径说明（98 个指标各一句）、客户指标英文名疑似 OCR 错字清单、3 个不可提供指标的书面清单与理由、部署运行说明（含环境变量表与 `.env.example`）。证明：`pytest tests/unit/test_delivery_docs.py -q`（断言清单文件与 `data/mapping/` 中的不可提供指标一致）

## 实施备注

- **接口先定**：波次 1 的 `models.py`（DTO 字段与幂等键）与 `kafka/serializer.py`（信封字段顺序）
  是波次 2、3 的共同依赖，须在 1.2 / 1.4 完成并冻结后再并行推进其余任务。
- **映射层双副本**：决策 2 决定 `tools/mappings/` 与 `tpops_kafka/mapping/` 存在同源副本，
  以 `tpops_kafka/mapping/` 为准；`tools/` 侧继续服务离线回放与文档生成。
  后续可让离线脚本改为从 `tpops_kafka` 导入以消除副本 —— 超出本变更范围。
- **映射数据双副本**：决策 9 要求 `tpops_kafka/mapping/data/` 与仓库根 `data/` 保持一致，
  由 2.1 的测试守住，防漂移。
- **裁剪预案**（若工期吃紧，按开发方案 §7.1.1 顺序让出范围，且必须同步下调验收口径）：
  先砍 2.8（告警采集器）→ 再砍 2.6/2.7（C/D 类）→ 再砍 24 小时连续运行观察。
  不可裁剪：波次 0、2.5（B 类采集器）、3.1（水位与重启续传）。
- **客户未决项**：慢 SQL 外发口径按最严实现（默认脱敏）；告警字段规范若在波次 3 前仍未回，
  按 §3.1.2 全字段输出并书面标注范围变更。
- **RED/GREEN 证据、review 回执与调度细节**属 execution contract / task brief，不在本文件重复。
