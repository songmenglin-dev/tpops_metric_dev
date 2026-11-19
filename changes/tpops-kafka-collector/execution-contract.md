# 执行合同

> 本文件是 `proposal.md` / `specs/` / `design.md` / `tasks.md` 的执行握手。
> 详细口径以那四份为准；本文件只固定「做什么、不做什么、按什么批次做、什么算做完、什么时候必须停」。

## Intent Lock

- **变更名称**：`tpops-kafka-collector`
- **要解决的问题**：
  1. 仓库里没有一行采集端代码 —— 需求、映射规则、实测样本都齐了，但 `tpops_kafka/` 这个包尚不存在，
     客户平台至今拿不到本项目产出的数据。
  2. 映射层不认识「单条合并 SQL」的返回形态（`result` = 一行 11 个 `json_agg` 列），
     82 个 B 类指标在该形态下**静默产出 0 / None / 空列表**且不报错，
     而现有 164 个测试全绿也拦不住（回归样本还是旧的 11 条形态）。
- **范围内**：
  - 新建 `tpops_kafka/` 采集端（28 个模块），实现 W01~W24 全部任务，分 5 个波次。
  - 映射层基准改造：新增 `merged_views` 段与 `decode_merged` / `merge_pages`；
    告警幂等键加 `status`；修跨视图比率恒为 0；把依赖切到包内并删除 `tools/mappings/`。
  - 同步开发方案中**与本次改动直接相关**的契约章节（§3.1 / §3.7 / §4.5.4 / §4.4 / §5.1 / §5.5 / §5.6.4 / §6.2）。
  - 交付文档：逐条口径说明、OCR 错字清单、不可提供指标清单、部署运行说明。
- **范围外**：
  - 联调验收 P5（2026-12-14~15）—— 需客户真实 Kafka 地址与 Topic 名；**验收项 D4（连续运行 ≥24 小时）
    不在本变更的完成证明内**（按 §7.1.1 的裁剪口径显式下调为「连续 2 小时 + 杀进程重启演练」，
    并同步写入开发方案 §1.7）；**D7 的真实平台演练**（连续 3 次错误密码后账号未锁定）同属 P5，
    本变更只交付其单元形态。
  - `data/mapping/archive/` 改名 `classification/`。
  - 24 条评审意见现状核对表。
  - 开发方案中**与本次改动无关**的「11 条合并 SQL」残留。
  - 客户 ITSCADA 平台本身的开发与部署；其他数据库的采集（走 Agent）；
    TPOPS 平台运维；数据的长期存储与历史回放。

## Approved Behavior

- **已批准需求摘要**（58 条，逐条 SHALL + WHEN/THEN，126 个场景，全文见 `specs/`）：

| 能力 | 需求数 | 覆盖的关键行为 |
|---|---|---|
| runtime-and-config | 8 | 生产代码边界、启动期配置快速失败、凭据不入日志、优雅退出、健康端点、每轮汇总日志、不可变 DTO、错误不静默 |
| platform-client | 9 | mini-token 认证、路径命名空间、自签证书、错误四分类、指数退避、命令中心三跳、master + node_id 去重、并发上限与失败隔离、慢 SQL 脱敏 |
| mapping-and-rules | 17 | 表驱动、启动期校验、包内数据一致、合并 SQL 解码、DecodeError 归属、per-view 兼容、分页拼接、13 种规则类型、缺值不产出假数据、跨轮差分、重启归零、维度展开、多 master 归集、不可提供指标、告警幂等键、指标幂等键、告警投递单位 |
| collection-channels | 10 | 周期分档、监控窗口与时间戳、12 分组码、多 series、命令中心执行、诊断优化、实例元数据、告警分页归并、历史增量与超期、节点身份回填 |
| delivery-and-state | 14 | 唯一序列化出口、业务维度落点、值类型序列化、缺省约定、Topic 分离、分区压缩、投递参数、失败不推进水位、幂等去重、水位持久化、调度器、单轮预算、调用数与成功率可观测、快照无历史 |

- **关键场景**（必须由测试直接断言，不得靠人工目视）：
  1. 合并形态的返回体解出 11 个视图的行列表；空页 → `[]`；坏 JSON → `DecodeError`。
  2. 82 个 B 类指标在合并形态下的值**等于期望值表**（不是「非零」判据）。
  3. 同一告警 `activated` → `recovered` 的幂等键不同，恢复消息不被去重丢弃。
  4. `gaussdb.Total_pct` 与 `GAUSSDB.session_usage` 拿到跨视图比率（探针实测 23/29 = 0.79）。
  5. `node_scoped = false` 的视图只产出 1 份；`true` 的视图每个 master 各一份；跨 master 不求和。
  6. 节点清单 12 条条目只向 3 个唯一 `node_id` 下发。
  7. 投递失败 → `ProduceError` → 水位未推进。
  8. 计数归零 → 值为 `null` + ERROR 日志 + 重建基线，不产生负速率。
  9. 认证失败 → 只发起一次认证请求。
  10. `tpops_kafka/` 无任何指向 `tools` / `data` 的 import。
- **验收检查**（全部为命令，逐条见 `tasks.md` 的证明列）：
  - `pytest tests/ -q` 全绿（含既有 164 个用例与本次新增）
  - `pytest --cov=tpops_kafka --cov-fail-under=80 -q`；映射层与规则引擎 ≥95%
  - `pytest tests/test_py37_compat.py -q`；`ruff check` 通过（`target-version = "py37"`）
  - 回归层 98/98 通过
  - 手工：起进程 → 发 SIGTERM → 退出码 0 且水位文件完整
  - 手工：杀进程 → 重启 → 不重复、不丢失

## Design Constraints

- **架构约束**（`design.md` 决策 1、2、7、8）：
  - 依赖单向：`main → scheduler → collectors → tpops → client`；`mapping → models`；`kafka` 不反向依赖。
  - `tpops_kafka/` 是**唯一生产代码**；不得 import `tools.*` / `data.*`，运行时不读仓库根。
  - 映射层**复制**进包（不重写），改造只在包内做；`tools/mappings/` 在切依赖后删除。
  - 并发用 `ThreadPoolExecutor`（HTTP I/O 密集，GIL 在等待时释放），不用 asyncio（3.7 兼容 + 无新依赖）。
  - 状态用 JSON 文件 + 「临时文件 + `os.replace`」原子写；单实例假设。
- **接口约束**：
  - 消息信封以 `data/mapping/itscada_field_map.json` 为准：**指标 7 字段 / 告警 13 字段**；
    业务维度落 `tags.dimension_values`；`component_id` 小写。
  - 平台路径两套前缀不可混（`/rds/v3/…` 与 `/rds/gaussdb/v3/instances/{id}/…`）。
  - mini-token 在**响应头** `X-Mini-Token`；`verify_tls=False`；认证失败**不重试**（防锁号）。
  - 命令中心：`statistics.running == 0` 判结束（不看 `job_status`）；`result` 二次 `json.loads`；
    只在 master 下发且按 `node_id` 去重；输出硬上限 **102400 B**（超了整条失败，不是截断）。
  - 合并 SQL：**单条全量**（11 视图），可增长视图分页；SQL 文本**运行时按 offset 渲染**，
    不打包固定成品。
- **依赖约束**：Python **3.7.9**；`kafka-python==2.0.2`、`requests>=2.25,<2.32`、
  `openpyxl>=3.0,<3.12`、`pytest>=7,<8`；**不新增依赖**；单文件 < 400 行、函数 < 50 行。
- **数据约束**：
  - 映射数据在包内 `tpops_kafka/mapping/data/` 持生产副本；仓库根 `data/` 为参考与发布源；
    两侧一致性由测试守住。
  - 规则类型以映射表在用的 **13 种**为准；跨视图比率真名 `ratio_across`，**不得**简化为 `ratio`。
  - 告警幂等键 `(alarm_id, instance_id, first_alarm_time, status)`，用平台原值 status。

## Execution Plan

full 路径：先运行 `ssf execution recommend`，按任务量与 wave 策略列出可用方式并推荐一种，
同时保存匹配当前 wave 的 recommendation receipt。Agent 展示候选项与理由；
`plan` / `revise` 只接受仍匹配 artifact、contract 与 wave 的凭据；
用户以 `--confirm` 明确确认；选择非推荐方式时还必须记录 `--acknowledge-recommendation`。
Batch Inline 是串行模式，不得描述为并行。批准后 `ssf execution plan` 把执行计划保存到
`<change>/.superpowers/sdd/execution-plan.json`（该 JSON 是计划的持久化控制面，不属于本执行合同）。

**DP-4（执行模式选择）在 build-executor 阶段进行**，本文件只固定 wave 的划分与完成标准。

## Execution Waves

每个 wave 有唯一 ID；只有依赖 wave 的 review receipt 为 `pass` 后，后续 wave 才可开始。
`parallel` 只表示允许在宿主支持并发派发时同时执行；不支持并发时必须明确报告该能力不可用，
不能把 `parallel` 计划悄然改写成串行执行。

### Wave 0

- **Wave ID**：`wave-0-mapping-baseline`
- **任务**：0.1 建包并复制映射层 → 0.2 加 `merged_views` 与合并形态解码 → 0.3 告警幂等键 →
  0.4 跨视图比率 → 0.5 合并形态列校验 → 0.6 开发方案契约章节同步 → 0.7 切依赖并删 `tools/mappings/`
- **依赖 wave**：无
- **策略**：`serial`（0.1 是其余全部的前置；0.3/0.4 在 0.2 之后可并行，0.6 与 0.5 可并行）
- **目标**：映射层能解合并形态且不静默出 0；生产代码边界成立；契约文档与实现一致
- **输入**：`tools/mappings/`、`data/mapping/`、`data/merged_sql/`、`docs/开发方案-*.md`
- **输出**：`tpops_kafka/mapping/`（含 `decode_merged` / `merge_pages`）、
  `response_rules.json` 的 `merged_views` 段、`alarm_rules.json` 的 `idempotency` 段、
  `tests/test_merged_sql_mapping.py`、同步后的开发方案；`tools/mappings/` 已删除
- **完成标准**：
  - `pytest tests/test_merged_sql_mapping.py tests/test_rules.py tests/test_response_rules.py -q` 全绿
  - 82 个 B 类指标在合并形态下的值**等于期望值表**
  - `pytest tests/ -q` 全绿（含既有 164 个用例，import 已切到包内）
  - `pytest tests/unit/test_package_boundary.py -q` 通过（无 `tools` / `data` import）
  - 开发方案 §3.1 / §3.7 / §4.5.4 与包内实现口径一致
- **Review gate**：review report 路径 `changes/tpops-kafka-collector/.superpowers/reviews/wave-0.md`；
  base = Wave 0 起始 SHA，head = Wave 0 结束 SHA；review receipt 必须为 `pass`

### Wave 1

- **Wave ID**：`wave-1-skeleton-platform`
- **任务**：1.1 配置 → 1.2 DTO 与异常 → 1.3 重试 → 1.4 序列化器 → 1.5 Kafka 生产者 →
  1.6 TPOPS 客户端 → 1.7 映射表加载与启动校验 → 1.8 规则引擎 → 1.9 水位持久化
- **依赖 wave**：`wave-0-mapping-baseline`
- **策略**：`parallel`（1.1/1.2/1.3 可并行；1.4 依赖 1.2；1.6 依赖 1.2+1.3；
  1.7 依赖 1.2；1.8 依赖 1.7；1.9 依赖 1.2）
- **目标**：进程可启动、可优雅退出；平台层具备「认证失败不重试」；映射层与规则引擎就位
- **输入**：Wave 0 的包；`data/mapping/`；客户字段表
- **输出**：`tpops_kafka/{config,models,errors,retry,watermark}.py`、
  `tpops_kafka/kafka/{serializer,producer}.py`、`tpops_kafka/tpops/client.py`、
  `tpops_kafka/mapping/{loader,rules}.py`
- **完成标准**：
  - `pytest tests/unit tests/integration -q` 全绿
  - `python -m tpops_kafka.main` 能起、能收 SIGTERM、退出码 0、水位文件完整
  - 序列化器输出与 `itscada_field_map.json` 的字段集逐字段一致
  - 13 种 `value_rule.type` 全部有测试覆盖
- **Review gate**：review report 路径 `.superpowers/reviews/wave-1.md`；
  base = Wave 1 起始 SHA，head = Wave 1 结束 SHA；review receipt 必须为 `pass`

### Wave 2

- **Wave ID**：`wave-2-collection-channels`
- **任务**：2.1 映射数据复制 → 2.2 合并 SQL 运行时渲染 → 2.3 A 类窗口与切片 →
  2.4 A 类采集器 → 2.5 命令中心三跳客户端 → 2.6 B 类采集器 → 2.7 C 类 → 2.8 D 类 →
  2.9 E 类告警 → 2.10 慢 SQL 脱敏
- **依赖 wave**：`wave-1-skeleton-platform`
- **策略**：`parallel`（三条采集链可并行：A=2.3+2.4、B=2.5+2.6、C/D/E=2.7+2.8+2.9；
  2.1→2.2 是所有通道的前置）
- **目标**：五条通道全部产出数据；多 master 归集与分页拼接口径落地
- **输入**：Wave 1 的包；`data/merged_sql/merged_sql.json`；`data/alarm/`；`data/metric_response/`
- **输出**：`tpops_kafka/tpops/{metrics,command_center,diagnostic,instances,alarms}.py`、
  `tpops_kafka/collectors/{monitor,command_center,diagnostic,instance,alarm}.py`、
  `tpops_kafka/mapping/{sqlgen,masking}.py`、`tpops_kafka/mapping/data/`
- **完成标准**：
  - `pytest tests/regression -q` —— 98 个可提供指标全部产出数据（对应 D1）
  - 3 个不可提供指标无消息且丢弃计数递增（对应 D2）
  - 多 master 归集：节点级视图每 master 一份、实例级视图只一份、无 3 倍累加
  - 分页：两页拼接后行数正确；空页不抛异常；渲染出的第 1 页与
    `data/merged_sql/sql_merge.sql` 逐字符一致
  - 节点清单去重后只向唯一 `node_id` 下发
- **Review gate**：review report 路径 `.superpowers/reviews/wave-2.md`；
  base = Wave 2 起始 SHA，head = Wave 2 结束 SHA；review receipt 必须为 `pass`

### Wave 3

- **Wave ID**：`wave-3-scheduling-testing`
- **任务**：3.1 调度器 → 3.2 健康端点 → 3.3 入口装配与信号处理 → 3.4 回归夹具与期望值表 →
  3.5 全量测试与门禁
- **依赖 wave**：`wave-2-collection-channels`
- **策略**：`serial`（3.1 依赖 1.5+1.9+Wave 2 全部采集器；3.3 依赖 3.1+3.2；3.5 在最后）
- **目标**：常驻进程 + 水位推进 + 优雅退出 + 全量测试与门禁通过
- **输入**：Wave 2 的包；98 份留档；合并 SQL 留档
- **输出**：`tpops_kafka/{scheduler,health,main}.py`、`tests/{unit,integration,regression,fixtures}/`
- **完成标准**：
  - `pytest --cov=tpops_kafka --cov-report=term-missing --cov-fail-under=80 -q` 通过；
    映射层与规则引擎 ≥95%
  - `pytest tests/test_py37_compat.py -q` 与 `ruff check` 通过
  - 杀进程 → 重启演练：不重复、不丢失（水位只在投递成功后推进）
  - 60 秒档单轮墙钟 P95 ≤ 25 秒（实测规模）
- **Review gate**：review report 路径 `.superpowers/reviews/wave-3.md`；
  base = Wave 3 起始 SHA，head = Wave 3 结束 SHA；review receipt 必须为 `pass`

### Wave 4

- **Wave ID**：`wave-4-delivery-docs`
- **任务**：4.1 交付文档（逐条口径说明、OCR 错字清单、不可提供指标清单与理由、部署运行说明）
- **依赖 wave**：`wave-3-scheduling-testing`
- **策略**：`serial`
- **目标**：交付物齐备，客户可据文档核对覆盖度与口径
- **输入**：Wave 3 的映射表与测试结果；客户指标库原件
- **输出**：`docs/` 下的交付文档；`data/customer_metric_unsupported.csv` 的对应说明
- **完成标准**：
  - `pytest tests/unit/test_delivery_docs.py -q` 通过（清单与映射表一致）
  - 98 个指标各有口径说明；3 个不可提供指标各有书面理由
  - 环境变量表与 `tpops_kafka/config.py` 实际读取的变量一致
- **Review gate**：review report 路径 `.superpowers/reviews/wave-4.md`；
  base = Wave 4 起始 SHA，head = Wave 4 结束 SHA；review receipt 必须为 `pass`

## Test Obligations

- **必须先从失败测试开始的行为**（RED 先行，不得先写实现）：
  1. 合并形态解码（0.2）—— `tests/test_merged_sql_mapping.py` 先红
  2. 告警幂等键含 `status`（0.3）—— 恢复事件不被去重丢弃，先红
  3. 跨视图比率（0.4）—— `Total_pct` / `session_usage` 不为 0，先红
  4. 生产代码边界（0.7）—— 无 `tools` / `data` import，先红
  5. 13 种规则类型（1.8）与 7/13 字段信封（1.4）—— 先红
- **必需的边界情况**：
  - 空页（`''` / `null`）→ `[]`，不抛异常
  - 坏 JSON 列值 → `DecodeError`；`result` 不是 1 行 → `DecodeError`
  - 计数归零 → `null` + ERROR + 重建基线，无负速率
  - 差分首轮无基线 → 只记基线，值为 `null`
  - 枚举未命中 → 原样透传 + 告警日志
  - 投递部分失败 → `ProduceError`，水位未推进
  - 认证失败 → 只发起一次认证
  - 结果体 >90% 上限 → WARNING；超 102400 B → 缩小容量重试
  - 告警收敛形态下不依赖平台侧 `alarm_id` / `instance_id` 过滤
- **回归敏感区域**：
  - `data/metric_response/` 98 份留档（旧的 per-view 形态 —— 必须**仍能解**）
  - `data/merged_sql/sql_merge_probe_result.json`（新的合并形态 —— 生产路径）
  - `data/alarm/` 收敛/明细/统计（告警归并与幂等）
  - `data/mapping/` 六张表（生成物与生成器一致性；重跑生成器后 `git diff` 必须为空）
  - **期望值表**由留档自动生成，不手抄 —— 手抄的期望值会跟着实现一起错

## Execution Mode

- **可用方式与推荐**：`ssf execution recommend <change-dir> [--wave <id>:<parallel|serial>:<task,...>[:<depends-on,...>]]`
- **用户确认的模式**：`sdd` | `inline` | `batch-inline`（DP-4 决定）
- **推荐理由 / 项目事实**：32 个任务 / 5 个 wave；波次 1、2 内部有真实并行度，
  波次 0、3、4 存在强串行前置（接口先定、夹具最后建）。
- **非推荐选择的风险确认**：`--acknowledge-recommendation`（若适用）
- **执行计划命令**：`ssf execution plan <change-dir> --mode <mode> --confirm --reason <text> --wave <id>:<parallel|serial>:<task,...>[:<depends-on,...>] [--acknowledge-recommendation]`
- **允许的修订**：将已有计划保留/升级为 `sdd`；先重新 recommend，并以 `--confirm` 生成新 revision
  和清除旧 receipt；不允许降级：`ssf execution revise <change-dir> --mode sdd --confirm --reason <text> --wave <id>:<parallel|serial>:<task,...>[:<depends-on,...>] [--acknowledge-recommendation]`
- **计划 revision / artifact hash**：DP-4 执行 `ssf execution plan` 后由 CLI 写入
  `.superpowers/sdd/execution-plan.json`，本文件不复制该内容。

## Verification Dimensions

| 维度 | 状态 | 发现 |
|------|------|------|
| Completeness | Pending | 58 条需求已全部映射到 wave 与任务（见下表），无未映射项 |
| Correctness | Pending | 待 build-executor 与 code-reviewer 阶段填充 |
| Coherence | Pending | proposal / specs / design / tasks / 本合同四处口径一致（DP-2 已核对） |

**需求 → 波次映射**（58 条全部有归属）

| 波次 | 覆盖的需求 |
|---|---|
| Wave 0 | 合并 SQL 返回形态的解码、分页结果的拼接、解码错误的异常归属、告警幂等键包含状态、启动期映射校验、生产代码边界、取值规则的类型集（`ratio_across`）、指标幂等键 |
| Wave 1 | 映射表驱动、映射数据在包内与仓库保持一致、唯一 JSON 序列化出口、值按客户指标表的数据类型序列化、消息字段的缺省约定、指标与告警使用不同 Topic、分区与压缩策略、Kafka 投递参数、投递失败不推进水位、幂等去重、水位与差分基线的持久化、启动期配置加载与快速失败、凭据仅来自环境变量且不入日志、结构化数据不可变、错误显式处理且不静默吞异常、指数退避重试、mini-token 认证、平台路径命名空间、自签证书、错误分类、缺值不产出假数据、累计计数的跨轮差分、实例重启归零的处理、维度展开 |
| Wave 2 | 周期分档调度、监控指标接口的窗口与时间戳、监控指标分组码、监控指标多 series 的处理、命令中心通道的执行、诊断优化通道、实例元数据通道、告警通道的分页与归并、历史告警增量与超期检测、告警节点身份回填、告警消息的投递单位、多 master 结果的归集口径、命令中心三跳编排、命令中心仅在 master 节点执行，且节点清单按 node_id 去重、命令中心并发上限与单条失败隔离、慢 SQL 文本默认脱敏、业务维度的落点、不可提供指标的处置 |
| Wave 3 | 调度器的到期驱动与并发组、单轮性能预算、单轮外部调用数与投递成功率可观测、健康端点、优雅退出、每轮结构化汇总日志、平台不保存历史快照、既有 per-view 形态保持可解 |
| Wave 4 | 交付文档（含不可提供指标的书面清单） |

**总体结论**：Pending

## Review Gates

- **强制审查点**：每个 Execution Wave 完成后记录 `ssf execution review` 的 review receipt；
  reviewer 须同时做 spec 合规与代码质量两维检查，报告写入
  `changes/tpops-kafka-collector/.superpowers/reviews/wave-<n>.md`
- **阻塞类别**：依赖 wave 的 receipt 非 `pass`；当前 wave 的 receipt 为 `fail`；
  receipt 缺失或已过期（artifacts / contract / plan 变更后失效）
- **收口条件**：所有 wave 都有 `pass` review receipt，且 `test_result: pass`
- **spec 合规的硬判据**（reviewer 必须逐条核对）：合并形态下 82 个指标的值等于期望值表；
  生产包无 `tools` / `data` import；既有 164 个用例仍全绿；3.7 语法门禁通过

## Escalation Rules

- **何时回退到 `specifying`**：
  - 规格与实现的冲突只能靠改规格解决时（例如客户字段表与 `itscada_field_map.json` 不一致）
  - 需要新增能力或扩大范围（超出本合同的 Scope Fence）
  - 盲读级别的复查发现规格本身判不了真假
- **何时回退到 `bridging`**：
  - 契约与工件不再匹配（scope / 需求 / 约束 / 波次发生实质变化）
  - 波次划分需要重排（如发现 1.2 与 1.4 无法并行）
  - 客户侧结论（告警字段规范、慢 SQL 外发口径）到达，导致已批准行为需要调整
- **何时不得继续实现**：
  - 任何 wave 的 review receipt 为 `fail` 或缺失
  - `ssf execution plan` 不存在或与当前 wave 不匹配
  - 覆盖率门禁未过（整体 <80% 或映射层与规则引擎 <95%）
  - 3.7 语法门禁未过
  - 发现「不报错但数据错」的路径（静默出 0、跨 master 3 倍累加、分页按页求值）而未被测试拦住
  - 客户两项死线未回时，**慢 SQL 不得以明文外发**（只能按最严口径脱敏）
