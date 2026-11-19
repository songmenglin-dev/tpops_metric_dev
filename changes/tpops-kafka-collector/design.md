# 技术设计：TPOPS → ITSCADA Kafka 采集端

本设计的依据全部在仓库里：开发方案 v2.1（§4 技术设计、§5 代码设计、§6 测试策略）、
`docs/设计-合并SQL映射基准改造.md`（映射层改造）、`data/mapping/`（映射表）、
`data/metric_response/`（98 份留档）、`data/merged_sql/`（合并 SQL 与留档）。

## 已知事实与约束

### 现状事实

| # | 事实 | 出处 |
|---|---|---|
| F1 | `tpops_kafka_collector/` 尚不存在；仓库只有映射层与离线脚本 | 目录结构 |
| F2 | 映射层能解「`result` 即行列表」（旧的 11 条形态），164 个测试基于此 | `tools/mappings/` |
| F3 | 合并 SQL 的返回形态是「一行 11 个 `json_agg` 列，列值是 JSON 字符串」，当前映射层解它会得到全 0 | 实测复现（`sql_merge_probe_result.json`） |
| F4 | 合并 SQL 全量 1 条实测 8.39 秒；100 页翻页在 1 万张表时约 6 分钟一轮 | `data/merged_sql/README.md` |
| F5 | 平台对命令输出有 102400 B 硬上限，超了整条任务失败（不截断）；`json_agg` 零行渲染成空串 | 同上 |
| F6 | 告警恢复后 `last_alarm_time` 不变（≠ `clear_alarm_time`），`first_alarm_time` 也不变 | `data/alarm/` 实测 |
| F7 | 收敛告警条目没有 `node_id`，其节点身份靠关联明细行回填，回填目标可能逐轮变化 | 同上 |
| F8 | 跨视图比率规则读的 `ctx.rows_by_key` 从未被生产路径填充过，两个指标恒为 0.0 | `tools/mappings/rules.py:363` |
| F9 | 12 个监控指标分组码无枚举接口，写错返回 200 + 空 series | 开发方案 §2.2.4 |
| F10 | 认证失败会累计 `login_failed_attempts` 并锁号 | 开发方案 §2.2.2 |
| F11 | 客户指标表英文名有 OCR 错字；客户未定义 JSON 字段（只定义维度） | 客户表 |

### 不可破坏的约束

| # | 约束 |
|---|---|
| C1 | 运行时 Python **3.7.9**；禁用海象、内建泛型注解、`zoneinfo`、`functools.cached_property` 等 3.8+ 语法 |
| C2 | 依赖锁版本：`kafka-python==2.0.2`、`requests>=2.25,<2.32`、`openpyxl>=3.0,<3.12`、`pytest>=7,<8`；不新增依赖 |
| C3 | 单文件 < 400 行、函数 < 50 行；映射**表驱动**，不写死在代码里 |
| C4 | 凭据只走环境变量；不入日志、不入消息、不入库 |
| C5 | 跨模块 DTO 不可变（frozen dataclass） |
| C6 | 客户侧 Kafka 信封、Topic 名、分区策略、7 档周期、5 类取值转换、维度展开规则 —— **已冻结，不得改动** |
| C7 | 目标交付 2026-12-15；剩余 18 个工作日 |
| C8 | 客户两项死线（告警字段规范、慢 SQL 外发口径）未回 → 慢 SQL 按最严口径（默认脱敏）实现 |

## 目标与非目标

### 目标

- G1：`tpops_kafka_collector/` 常驻进程，7×24 循环，把 98 个可提供指标与告警持续投递到 Kafka。
- G2：映射层能解合并 SQL 形态，82 个 B 类指标产出真值；既有 per-view 路径不受影响。
- G3：告警恢复事件不再被去重丢弃；跨视图比率指标不再恒为 0。
- G4：单元/集成覆盖率 ≥ 80%，映射层与规则引擎 ≥ 95%；Python 3.7 门禁常设。
- G5：交付文档齐备（逐条口径、OCR 错字清单、部署说明、不可提供清单）。

### 非目标

- 联调验收（需客户 Kafka 地址/Topic）——不在本变更。
- 客户侧看板开发、其他数据库采集、TPOPS 平台运维、历史数据回补。
- 改变已冻结的客户侧契约（信封 / Topic / 周期 / 口径）。
- 顺手做的清理（`archive/` 改名、评审核对表、开发方案漂移同步）——另开变更。

## 决策（Decisions）

### 决策 1：采集端按「平台层 / 采集层 / 映射层 / 投递层 / 调度层」五层单向依赖

- **选择**：`tpops_kafka_collector/` 下分 `tpops/`（只懂 TPOPS）、`collectors/`（通道分治）、
  `mapping/`（表驱动）、`kafka/`（唯一序列化出口）、顶层 `scheduler.py`/`main.py`/`models.py`/
  `errors.py`/`config.py`/`watermark.py`/`health.py`。
  依赖方向严格单向：`main → scheduler → collectors → tpops → client`，
  `mapping → models`，`kafka` 不反向依赖任何层。
- **理由**：开发方案 §4.1.3 已定此约束；它保证单元测试可以完全 mock 掉 TPOPS 与 Kafka
  （NFR-10 的前提），也保证平台层永远不需要知道客户字段。
- **替代方案**：把 `tools/mappings/` 直接当生产包用（路径在 `tools/` 下，语义上是离线工具）；
  或扁平单包。两者都让「离线工具」与「生产代码」边界模糊，采集端上线时要连同一堆
  抓取脚本一起装。
- **后果**：多一层搬运工作（把 `tools/mappings/*.py` 搬进 `tpops_kafka_collector/mapping/`），
  但生产包自洽，不依赖 `tools/`。

### 决策 2：生产代码全部在 `tpops_kafka_collector/`，`tools/` 与 `data/` 只作参考

- **选择**：`tpops_kafka_collector/` 是**唯一的生产代码**。`tools/` 与 `data/` 是参考与离线工具，
  生产代码**不得**依赖它们：
  - `tools/mappings/{table,rules,decode,message,nodes}.py` **复制**进 `tpops_kafka_collector/mapping/`，
    全部改造（`decode_merged` / `merge_pages`、告警幂等键、跨视图比率）**只在包内做**。
  - 复制完成后，把 `tools/` 下 10+ 个脚本与 `tests/` 下 8 个测试文件的
    `import tools.mappings` 全部改为 `import tpops_kafka_collector.mapping`，然后**删除 `tools/mappings/`**
    （git 历史保留）。
  - 映射数据在包内 `tpops_kafka_collector/mapping/data/` 持有一份生产副本；
    仓库根 `data/mapping/`、`data/merged_sql/` 保留为参考与发布源，
    由一致性测试保证两侧不漂移。
- **理由**：
  1. **生产包必须自洽**。部署产物是 `tpops_kafka_collector/`，运行时不能去读仓库根的 `tools/` 与
     `data/`（`tools/` 里有会连真实平台的脚本，边界不清）。
  2. **双副本会静默分叉**。若 `tools/mappings/` 与 `tpops_kafka_collector/mapping/` 并存，
     而 164 个现有测试测的是前者，那么**测试全绿并不代表生产代码正确** ——
     这与本次要修的「静默出 0」是同一类风险。切依赖 + 删副本是唯一能消除它的做法。
  3. 那 5 个模块已有 164 个通过的测试覆盖，逻辑正确且已被回归样本验证；
     **复制**（而非重写）把这批验证一并带进生产包。
- **替代方案**：(a) 生产代码 `import tools.mappings` —— 生产包依赖离线工具目录，部署不完整；
  (b) 重写 —— 把已验证的代码再赌一次；(c) 保留 `tools/mappings/` 双副本 ——
  测试指向哪一份就绿哪一份，另一份无人验证。
- **后果**：
  - 改造顺序必须调整：**先复制、再在包内改**，不能「先改 tools 再复制」（否则改两遍）。
  - 切依赖是一次跨 18+ 个文件的机械改动（import 重定向），由全量测试把关。
  - `tools/` 下脚本运行时需要把项目根加入 `sys.path`（它们目前只加了 `tools/` 目录）。
  - 仓库里从此不再有「映射层的第二份实现」，开发方案 §5.1 的目录结构与实际一致。

### 决策 3：合并 SQL 的解码用新增 shape 段，不动既有 shape

- **选择**：`response_rules.json` 新增 `merged_views` 段（11 个视图，各带
  `json_column` / `paged` / `sql_ref`），新增 `decode_merged(body, views)` 与
  `merge_pages(bodies, views)`；既有 11 个 `cmd_rows_*` shape 与
  `_rows_from_result` **一字不改**。规则一行不改（每条已带 `locate.sql_key`）。
- **理由**：既有 11 个 shape 会被**两种不兼容的 body** 共用（旧的 per-view 与新的合并形态）。
  若给它们加 `json_column` 并靠自动探测区分，三种行为各坏一处：抛错 → 82 条规则的解码测试全挂；
  返回 `[]` → 值测试挂；行列表原样返回 → 静默产出错误数据（正是要消灭的东西）。
  分开两段让每个 shape 只有一种 body，不存在猜测。
- **替代方案**：(a) 单 shape 双形态 + 自动探测 —— 见上，不成立；
  (b) 把 `ctx.rows` 改成 `ctx.rows_by_view` —— 82 条规则全部要改、全部要回归；
  (c) 改映射表整体结构（规则内联视图名与列名）—— 推翻表驱动，与 §5.6 冲突。
- **后果**：映射表多一个段（`merged_views`），生成器要同时校验它与
  `merged_sql.json` 的视图名一一对应；两套形态并存意味着解码有两条路径要各自测试。

### 决策 4：分页拼接放在映射层

- **选择**：`merge_pages(bodies, views)` 由映射层提供，按 `merged_views[].paged`
  把多页按视图拼成**完整视图行列表**；采集端只负责把 N 页的 body 拿回来。
- **理由**：`row_policy: sum` 的语义是「整视图求和」，`counter_delta` 的基线是
  「整视图的累计值」。若采集端逐页解码逐页求值，表一多（超过 100 张）就会
  **按页出值**——那 22 个表级指标会静默出错，而且错的正好是客户最看重的表级数据。
  把拼接放在规则所在的同一层，语义边界不出层。
- **替代方案**：采集端拼接 —— 采集端需要知道哪些视图可增长（把 `paging` 语义复制一份到采集端），
  且行拼接规则散落在两层，容易走样。
- **后果**：映射层对外多一个「多 body 入」的接口；采集端的分页循环只负责翻页与推进 offset。

### 决策 5：告警幂等键加 `status`、去 `node_id`

- **选择**：键 = `(alarm_id, instance_id, first_alarm_time, status)`，
  `status` 用平台原值（`activated`/`recovered`/`cleared`/`cleared_by_user`）。
- **理由**：
  - 恢复事件：`first_alarm_time` 不变 → 原键会把恢复消息永久去重丢弃；
    而客户字段表里 `recoverTime` 是「status=0 时必填」，恢复消息必须发得出去。
  - `last_alarm_time` 在恢复后**不变**（实测 last ≠ clear）→ 加它救不了；
    这正是开发方案 §3.7「`last_alarm_time` 变化 → 幂等键变化」写错的地方。
  - `node_id`：收敛条目没有节点，其节点靠关联明细行回填，回填目标可能逐轮不同 →
    进键会导致同一告警因回填漂移而重复投递。
  - 平台原值而非 ITSCADA 枚举：枚举会把 `recovered` 与 `cleared` 压成同一个键。
- **替代方案**：(a) 加 `last_alarm_time` —— 实测不成立；
  (b) 用 `clear_alarm_time` 替代 —— 语义绕，且 `cleared` 状态下可能为 None；
  (c) 按明细行建键 —— 投递单位是收敛条目，与投递单位不一致。
- **后果**：键定义变更，已发布的 FR-11 需要按「修改」处理；客户侧若按旧键去重，
  恢复事件会重复到达一次（客户侧按 `alarmId + status` 去重即可，属可接受变更）。

### 决策 6：`kafka-python` 而非 `confluent-kafka`

- **选择**：`kafka-python==2.0.2`（纯 Python）。
- **理由**：目标环境只有 Python 3.7.9；`confluent-kafka` 的 wheel 对 3.7 的支持范围更窄，
  且需 librdkafka 运行时（部署环境多一个 C 库依赖）。开发方案 §5.8 已给出同样结论。
- **替代方案**：`confluent-kafka` —— 性能更好，但引入运行时依赖与 3.7 兼容风险。
- **后果**：`enable_idempotence` 不可用（需 3.6+ 客户端），重复投递由采集端幂等键
  + 客户侧去重兜底（已写入 §3.2 定稿）。

### 决策 7：并发用线程池，不用 asyncio

- **选择**：`concurrent.futures.ThreadPoolExecutor`（Python 3.7 自带）。
- **理由**：工作是 HTTP I/O 密集，GIL 在等待时释放；开发方案 §4.6.1 已给出
  「三组并发 ≈ max(耗时) 而非 sum」的实测依据。asyncio 需要重写整个平台层
  （requests 是同步的），收益不抵成本。
- **替代方案**：asyncio + aiohttp —— 需新增依赖（违反 C2），且要重写已验证的调用逻辑。
- **后果**：并发上限由线程池大小控制；CPU 密集部分（JSON 序列化）仍在 GIL 内，
  但实测占比极低。

### 决策 8：状态持久化用 JSON 文件 + 原子替换

- **选择**：`state/` 目录下的 JSON 文件（`watermark.json`、`baselines.json`、
  `alarm_watermark.json`、`idempotency.json`），写临时文件后 `os.replace`。
- **理由**：开发方案 §4.6.4 已定；不引入数据库（部署环境没有、也无运维能力）；
  数据量小（幂等键 LRU 上限 10000 条）。
- **替代方案**：SQLite —— 更适合大量状态，但本项目状态量级小，且 JSON 可直接人工排障。
- **后果**：单实例假设（多副本部署会有水位竞争）。方案未要求多副本；
  若客户要求多副本，需另做分布式水位。

### 决策 9：映射数据文件随包搬运，仓库保留一份

- **选择**：`data/mapping/` 与 `data/merged_sql/` 的映射表与 SQL 复制到
  `tpops_kafka_collector/mapping/data/`（随包发布），仓库原文件保留（离线工具与文档生成仍用它们）。
- **理由**：采集端要能独立部署成一个包，不能在运行时去读仓库根目录。
- **替代方案**：运行时读仓库根 —— 部署产物不完整。
- **后果**：两份副本需要保持一致 → 用「仓库是源、`tpops_kafka_collector/mapping/data/` 是发布副本」
  的约定，并在测试里断言两者内容一致（防止漂移）。

### 决策 10：测试分四层，回归层用「期望值表」而非「非零」判据

- **选择**：`tests/unit`（纯函数）、`tests/integration`（全链路 mock）、
  `tests/regression`（回放 98 份留档 + 合并 SQL 留档）、`tests/fixtures`（留档转固定样本）。
  回归断言用**逐指标期望值表**（由留档生成后固化进测试）。
- **理由**：实测分布是 58 个非零 / 23 个合法为 0 或 null / 1 个过滤无匹配行。
  若写「不得为 0」的断言，`gaussdb.Deadlocks`（该实例确实没有死锁）就会失败，
  而调宽断言就把回归测试变成了摆设。期望值表既能卡住真值，也能表达「这里合法为 0」。
- **替代方案**：只断言「能解析出值」——无法发现口径错（本次 F3 的缺陷正是这类）。
- **后果**：期望值表需随平台数据变化时更新；由留档自动生成，避免手抄。

### 决策 11：多 master 结果按 `node_scoped` 归集，不跨 master 合并

- **选择**：归集口径由映射表的 `node_scoped` 字段决定 ——
  `true` 的视图（`stat_database` / `activity_agg` / `activity_running` /
  `activity_top10` / `dbstat_sum`）各 master 的结果**各自成一份数据**；
  `false` 的视图（`stat_user_tables` / `database` / `locks` / `bgwriter` /
  `settings` / `prepared_xacts`）**只取第一个 master 的结果**。
  节点清单 SHALL **按 `node_id` 去重**后才下发。
- **理由**：`node_scoped` 是**实测判定**的结论（`tools/distribution.py`：
  各 master 结果不同 → 节点级，相同 → 实例级），已经固化在 `metric_rules.json` 里。
  实测同一实例三个 master 的 `stat_database` 首行 `xact_commit`
  分别为 153291 / 153299 / 146452 —— 确实不同，因此**跨 master 求和会得到无业务含义的
  3 倍累计值**。反过来 `stat_user_tables` 等视图各 master 结果相同，
  取三份就是三份重复。
  节点清单对同一实例返回 12 条条目但只有 3 个唯一 `node_id`
  （每个 node_id 同时以 CN 与 DN 各出现一次，其中 6 条 `role == "master"`），
  不去重就会重复下发。
- **替代方案**：(a) 一律取第一个 master —— 会丢掉节点级指标的节点区分，
  与实测结论矛盾；(b) 一律各 master 一份 —— 实例级指标重复 3 倍；
  (c) 跨 master 求和 —— 得到 3 倍假累计值，是最坏的一种。
- **后果**：`scope.node.policy` 的语义被固化下来（`per_master` 对节点级视图、
  取首个 master 对实例级视图）。**这是业务口径**：节点级视图的实例级指标
  （如 `gaussdb.Xact_commit`）会在 3 个节点资源上各出现一次、值略有差异
  （GaussDB 分布式下各 CN 的 `pg_stat_database` 是各自视角）。
  该口径需在交付文档中逐条说明，并列入待客户确认项。

### 决策 12：消息信封以客户字段表为准，开发方案 §3.1 视为过时

- **选择**：最终 Kafka 消息的字段集以 `data/mapping/itscada_field_map.json`
  的 `metric_message`（7 字段）/ `alarm_message`（13 字段）为准 ——
  该文件直接取自客户两份对接方案 §6.2 / §3.2 的字段表，
  且 `tools/mappings/message.py` 已按它实现并被测试覆盖。
  开发方案 §3.1 的另一套信封（`msg_type` / `schema_version` / `instance_id` /
  `metric_name` … 共 14/25 字段）是**映射层重构之前**的设计，已被取代，
  本次 SHALL 同步修正开发方案 §3.1。
- **理由**：客户对接方案是**外部契约**，其字段表是唯一权威；
  开发方案是内部设计文档。两者冲突时以客户方案为准。
  `itscada_field_map.json` 正是从客户方案逐字段落下来的，
  且现有 164 个测试与 `build_kafka_mapping.py` 都已围绕它工作。
- **替代方案**：按开发方案 §3.1 实现 —— 会推翻已被验证的 `message.py`，
  且产出的字段集不是客户文档里的那份。
- **后果**：本变更必须包含**开发方案 §3.1 的同步修改**（这不属于「附带漂移」，
  而是本次直接改动的契约的真相源）。业务维度的落点同步确定为
  `tags.dimension_values`（不是 `dimensions`），`COMPONENT_ID` 的大小写也统一为
  `component_id`。

### 决策 13：合并 SQL 的 SQL 文本在运行时按分页窗口生成

- **选择**：包内携带 `merged_sql.json`（11 个视图定义 + `refresh_seconds` /
  `refresh_tier` + `paging.views[].paged`），
  由**运行时**渲染出带指定 `limit`/`offset` 的 SQL 文本；
  SHALL NOT 打包一个固定的成品 `.sql` 文件。
  分页容量由 `CMD_PAGE_SIZE`（默认 100）配置。
- **理由**：`data/merged_sql/sql_merge.sql` 只是**第 1 页（offset=0）的成品**；
  第 2 页起由 `tools/build_sql_merge.py --offset N` 现场生成。
  若只打包成品文件，采集端根本没有翻页能力，
  而表级视图在表数超过 100 时必须翻页才能取全。
  把渲染逻辑放进包，采集端才能推进 offset。
- **替代方案**：(a) 打包固定 SQL —— 无法翻页，B5 阻塞；
  (b) 打包 N 个预生成的页 —— 页数取决于表数量，运行前无法确定；
  (c) 用正则替换成品 SQL 里的 `limit N offset M` —— 脆弱，视图的 limit 各不相同
  （`activity_running` 语义上限 20、`activity_top10` 上限 10），正则会误改。
- **后果**：需要把 `build_sql_merge.py` 的渲染逻辑搬进包（与决策 2 的「搬而不重写」一致），
  并加一条测试断言「包内渲染出的第 1 页 SQL 与 `data/merged_sql/sql_merge.sql` 一致」。

## 风险与验证

| 风险 | 缓解 | 验证证据 |
|---|---|---|
| **合并 SQL 形态解错，82 个指标静默出 0**（F3 已复现） | 决策 3 的独立 `merged_views` 段 + 解码失败即抛错 | 回归层用 `sql_merge_probe_result.json` 回放，逐指标比对期望值表 |
| **分页后按页求值，表级指标静默出错** | 决策 4：拼接在映射层 | `test_merge_pages_concatenates_paged_views` + 表级指标的期望值表按「整视图」断言 |
| **告警恢复事件被去重丢弃** | 决策 5：键加 `status` | 单元：同一告警 activated → recovered 产出两条消息 |
| **认证失败重试导致账号锁死** | `AuthError` 不可重试，只记录一次 | 单元：注入 401，断言不发起第二次认证；演练 D7 |
| **表量大时撞 102400 B 硬上限，整批 82 指标取不到** | 单页容量可配 + 结果长度 >90% 告警 + 失败后缩小容量重试 | 单元：注入超限响应，断言缩小容量重试；单元：90% 触发 WARNING |
| **客户慢 SQL 外发口径未定** | 默认按最严口径（脱敏 + 截断 512）；开关关闭时才明文 | 单元：默认输出不含明文 SQL；配置断言 D8 |
| **客户 Kafka 地址/Topic 未定** | Topic 与地址全走环境变量，代码不写死 | 单元：不同环境变量下投递到不同 Topic |
| **交付期只剩 18 个工作日** | 四个波次按 §7.1.1 的可裁剪顺序（先砍告警采集器 → C/D 类 → 24 小时观察） | 每个波次结束时的退出准则即证明 |
| **快照型指标停机即空洞且不可回补** | `/readyz` 暴露自身可用性；文档明确不承诺补历史 | 集成：`/readyz` 在依赖不可用时 503 |
