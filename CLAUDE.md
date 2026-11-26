# CLAUDE.md

## 项目目标

客户不使用 TPOPS（GaussDB 监控平台）的管理界面，改为把**多个数据库管理平台的监控/告警数据统一导入客户自有的监控平台**。

本项目负责其中 GaussDB 这一路：通过 TPOPS 的 API 按时钟窗口拉取指标与告警数据 → 按客户的字段规范做映射 → 以客户约定的格式写入 Kafka，由客户侧消费。

- 形态：**常驻进程**，持续循环运行，不是一次性脚本
- 语言：**Python 3.7.9**（见下方红线，语法必须兼容）
- 投递：Kafka，**PLAINTEXT**，消息体为 **JSON**
- **时间线**：需求 2026-11-01 下达，**第一版 2026-12-15 交付**；
  阶段划分与各阶段退出准则见开发方案 §7.1.1（含投入核算与范围裁剪预案）。
  **当前处于 P1（11-09~11-13）方案修订**：先关闭评审的 12 条 BLOCKER + 12 条 MAJOR、
  机械重生成映射表，再进入编码。

## 架构 / 数据流

```
Scheduler(窗口 + 水位)
   → 采集器（按获取方法分五路：A 监控指标 / B 命令中心 SQL / C 诊断优化 / D 实例 / E 告警）
   → TPOPS Client  (mini-token 认证)
   → Normalizer    (TPOPS 原始结构 → 内部统一 DTO)
   → FieldMapper   (按映射表 + 取值规则转成客户字段)
   → Serializer    (唯一序列化出口，JSON)
   → Kafka Producer(PLAINTEXT)
   → 推进水位
```

一次循环：拉取 → 转换 → 投递 → **成功后才推进水位**。投递失败不推进水位，重启后从水位续传（至少一次语义）。

五路采集器的划分与依据见 [`docs/开发方案-GaussDB指标与告警采集.md`](docs/开发方案-GaussDB指标与告警采集.md) §2.3（调研结论）与 §4.3（逐指标取数方法）。

## 目录结构

`tpops_kafka_collector/` 是**采集端的生产代码包** —— 本仓库同时还有调研产物、映射规则、
离线工具与部署资产：

```
tpops_kafka_collector/              # 采集端（唯一的生产代码包）
├── main.py          # 入口：装配依赖、启动调度循环、信号处理
├── config.py        # 配置加载（环境变量 + 可选配置文件）
├── scheduler.py     # 轮询循环、水位推进、重试退避、优雅退出
├── models.py        # 跨模块 DTO（frozen dataclass / NamedTuple）
├── tpops/
│   ├── client.py        # TPOPS HTTP 客户端（从 tools/tpops_client.py 提炼）
│   ├── metrics.py       # A 类：监控指标拉取、窗口与 end_time 计算
│   ├── command_center.py# B 类：命令中心三跳、批量下发、SQL 合并
│   ├── diagnostic.py    # C 类：slow-sql-list / top-sql-list
│   ├── instances.py     # D 类：实例元数据
│   └── alarms.py        # E 类：告警拉取（active/history × 收敛/明细）
└── kafka/
    ├── producer.py  # PLAINTEXT 生产者
    └── serializer.py# JSON 序列化
```

**映射层分两处**（执行代码已搬进生产包，映射数据暂留仓库根）：

```
data/mapping/             # 映射规则数据（**表驱动，不写死在代码里**）
├── metric_mapping_monitor_metrics.json   # A 类 监控指标接口的字段映射
├── metric_mapping_command_center_sql.json # B 类 命令中心 SQL 的字段映射
├── metric_mapping_diagnostic.json         # C 类 诊断优化接口的字段映射
├── metric_mapping_instance.json           # D 类 实例接口的字段映射
├── alarm_mapping.json                     # 告警的字段映射
├── itscada_field_map.json   # 客户消息的字段清单（哪些字段、必填与否）
├── metric_rules.json        # 87 条生效指标规则（101 条里 87 可提供，14 条不在生效口径内）
├── alarm_rules.json         # 告警规则 + 257 条告警定义
└── open_items.json          # 待客户确认的 7 项

tpops_kafka_collector/mapping/   # 映射层执行代码（**生产代码**）
├── table.py     # 映射表加载与查表
├── rules.py     # 字段级转换规则（类型、单位、缺省值）
├── nodes.py     # 节点身份解析（resourceName：节点 IP + 节点 ID；选 master CN）
├── message.py   # 按 ITSCADA 字段规范组装消息
├── decode.py    # 响应体解码（含合并 SQL 的合并形态）
├── columns.py   # 列名校验（列名与实测留档对齐）
└── ids.py       # 标识与幂等键
```

> **映射数据仍在仓库根 `data/mapping/`**（要等 wave 2a 任务 2.1 才复制进包内）。
> `tools/` 与 `data/` 只作参考与离线工具，**不是生产依赖**。

```
tests/     # 映射层单元测试（pytest）；TPOPS 与 Kafka 一律 mock
deploy/    # 部署资产（进部署包）：collector.sh（一个脚本，位置参数选功能）
tools/     # 现有离线取数脚本 + 参考实现，保留
           #   build_deploy_bundle.py —— 构建离线部署包（纯 Python wheel，架构通用）
data/      # 离线样本与调研产物
│  ├── alarm/               # 告警侧数据（抓取产物 + 告警定义目录 + 181 条规则配置），详见该目录 README
│  ├── command_center/      # 内置命令清单（142 条）+ 通用 SQL 命令详情
│  ├── metric_response/     # 逐指标**原生 API 响应体**（100 个文件，命令中心走 11 条合并 SQL）—— 回归样本
│  ├── merged_sql/          # 11 条合并 SQL 及其探针结果
│  ├── customer_metric_mapping.json    # 101 客户指标 → TPOPS 可提供性判定
│  ├── tpops_api_catalog.json          # TPOPS API 目录
│  └── customer_metric_unsupported.csv # 无法提供清单
docs/      # 文档
│  ├── api/            # TPOPS API 参考（由 CHM 转换，未增补）
│  ├── *.chm           # GaussDB 产品文档原始包（312 MB，未入库）
│  └── *.docx          # 客户监控平台接入指引（走 Agent，非本项目路径）
```

**已清理的产物**（2026-11-13，按"一次性探针产物"口径删除，均可从 git 历史恢复）：

- `data/metric_verification/`、`data/command_center/results/`、`data/api_probe/`、
  `data/multi_master_probe/` —— 逐指标实测留档、命令中心全量留档、探针结果。
  **等价内容在 `data/metric_response/`**（逐指标原生响应，一个指标一文件），
  回归回放改用它；**命令清单已重新拉取并保留在 `data/command_center/`**。
- `data/customer_metric_query_plan.json`、`data/metric_value_rules.json` —— 映射表的中间产物，
  **不入库**，缺失时由 `tools/build_itscada_mapping.py` 自动重建（见该文件的 `_ensure_input`）。
- `data/tpops_all_metric.json`（TPOPS 原始指标字典）—— 可提供性结论已固化在
  `customer_metric_mapping.json` 里，不重建源字典。

## 运行

**开发环境**（conda，Python 3.7.9 —— 不用 venv）：

```bash
conda create -n tpops python=3.7.9 -c conda-forge --override-channels -y
conda install -n tpops -c conda-forge --override-channels -y pip
conda run -n tpops python -m pip install -r requirements-dev.txt
conda activate tpops          # 或每次用 conda run -n tpops <命令>
conda run -n tpops python -m pytest -q
```

> Anaconda 默认源（repo.anaconda.com）需要接受商业 TOS，**不要用它**；一律加
> `-c conda-forge --override-channels`。

启动采集端：

```bash
cp .env.example .env        # 填 TPOPS_PASSWORD、KAFKA_* 等
set -a && source .env && set +a
python -m tpops_kafka_collector.main
```

配置项（全部走环境变量，`.env` 已 gitignore）：

| 变量 | 说明 |
|---|---|
| `TPOPS_USER` / `TPOPS_PASSWORD` / `TPOPS_BASE_URL` | TPOPS 认证与地址 |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka 地址，逗号分隔（**待客户确认**） |
| `KAFKA_TOPIC_METRICS` / `KAFKA_TOPIC_ALARMS` | Topic 名（**待客户确认**） |
| `POLL_INTERVAL_SECONDS` | 轮询间隔 |
| `METRIC_WINDOW_HOURS` | 指标查询窗口，**不得小于 1** |
| `METRIC_END_LAG_MINUTES` | end_time 回退量，默认 5 |

`.env.example` 已包含 `TPOPS_*`、`KAFKA_*`（地址与两个 Topic 名）与调度参数
（`POLL_INTERVAL_SECONDS` / `METRIC_WINDOW_HOURS` / `METRIC_END_LAG_MINUTES`）；
`KAFKA_BOOTSTRAP_SERVERS` 仍待客户提供，Topic 名用的是默认值。

## Python 3.7.9 红线

**语法/标准库禁用**（3.8+ 才有，用了就跑不起来）：

- 海象 `:=`、`f"{x=}"`、字典合并 `d1 | d2`
- 内建泛型注解 `list[str]` / `dict[str, int]` → 一律用 `typing.List/Dict/Optional/Set/Tuple`
- `typing.Literal` / `Protocol` / `TypedDict` / `Final` → 用 `NamedTuple`、frozen dataclass 或普通类
- `functools.cached_property`、`importlib.metadata`、`math.prod`
- `zoneinfo` → 用 `pytz` 或 `dateutil`；`asyncio.to_thread` → 用 `loop.run_in_executor`
- `str.removeprefix` / `removesuffix`

**可用**：`dataclasses`、`f-string`、`typing.NamedTuple`、`asyncio.run`、`subprocess.run(capture_output=)`。

**依赖锁版本**（3.7 已停止维护，新版会直接装不上）：

```
kafka-python==2.0.2    # 纯 Python；2.1+ 不再支持 3.7
requests>=2.25,<2.32   # 2.32 起要求 Python ≥3.8（2.32.5 要 ≥3.9）
urllib3>=1.21.1,<3     # 生产代码直接 import 它（此前只靠 requests 隐式带入）
loguru==0.8.0          # 日志框架；0.8.0 是最后一个支持 Python 3.7 的版本
pytest>=7,<8
pytest-cov<5
openpyxl>=3.0,<3.12    # 仅离线工具与测试用
```

分两个文件：**`requirements-runtime.txt`**（运行时，进部署包）与
**`requirements-dev.txt`**（开发/测试，不进部署包）。

如改用 `confluent-kafka`，注意其 wheel 对 3.7 的支持范围，且需 librdkafka 运行时。

## 部署形态（目标机）

**生产部署不需要 conda** —— 采集端的运行时依赖只有 `requests` / `kafka-python` /
`urllib3` / `loguru` 四个**纯 Python** 包（无 C 扩展）：

- 目标机只要有 **Python 3.7 ~ 3.11**（麒麟 V10 SP2+ 自带 3.7.9）即可，
  **不装 conda、不编译、不联网**；
- 全部依赖都是 `*-none-any.whl` → **x86_64 与 aarch64 共用同一份部署包**；
- Python **3.12+ 不行**：`kafka-python 2.0.2` 依赖的 `six.moves` 机制在 3.12
  被移除的 `find_module` API 打断，`import kafka` 直接报
  `No module named 'kafka.vendor.six.moves'`。

部署包由 `tools/build_deploy_bundle.py` 构建（见 `docs/交付-部署运行说明.md`）：

```
tools/build_deploy_bundle.py   # 有网机器上构建 → dist/tpops_metric_collector_<版本>/
tools/build_release.py         # 构建并归档到 release/<版本>/（部署包 + 工程源码包）
tools/deploy_bundle/           # 构建器的实现（files / wheels / bundle 三个模块）
deploy/collector.sh             # 目标机：**一个脚本**，位置参数选功能 ——
                                #   check-env  装之前的环境预检（只读）
                                #   check-deps 离线依赖预检
                                #   install    零联网安装（阶段一装包、阶段二部署包）
                                #   verify     装之后的自检（不打印口令、不创建任何东西）
                                #   run        启动采集端（systemd 调它）
```

目标机上的部署顺序是**四步**，全用同一个脚本 `collector.sh`：
**检查环境**（`check-env`，只读）→ **查离线依赖**（`check-deps`）→
**安装环境需要的包 + 部署采集端包**（`install`，内部两个阶段）→
**配置与验收**（`verify` + `run`）。

产出的部署包含外置的 `.tar.gz.sha256`（**与压缩包分开传**，防篡改）；
安装脚本把安装模式写进 `.install-mode`、解释器写进 `.python-bin`，
`collector.sh run` 据此选解释器 —— 不靠「目录在不在」猜。

## TPOPS 接口要点（均已实测，勿凭直觉改）

0. **实例按平台返回的全部实例采集**（交付时客户的实例名/ID 与我们探测时不同，
   不能写死任何一个 ID）。两种形态都要能处理：**分布式版** mode=Combined（3 个 CN 均为 master）
   与**集中式版** mode=Ha（1 个 master DN、无 CN）。
   命令中心是「instance_id + node_ids 下发」，无法一次带多实例 → 采集端**按实例循环**。
   （开发期做逐指标实测留档时用的是探测环境的实例，但那只是**探测口径**，
   不构成交付时的采集范围。）
1. **平台有两个命名空间，不能混**：
   - `/rds/v3/…` —— 认证、实例列表、监控指标、告警记录
   - `/rds/gaussdb/v3/…` —— **数据库诊断优化接口集 + 统一命令中心**
   少一段前缀就 404（不是"接口没实现"）。**86 个客户指标靠 `/rds/gaussdb/v3/` 这一路**，
   去掉 `gaussdb` 段等于把最大的那块指标全丢掉。实例列表：`GET /rds/v3.2/instances`。
2. 认证：`POST /rds/v3/auth/mini-token`，Token 在**响应头** `X-Mini-Token`；服务端 session_timeout 1440 分钟，需按需刷新。**登录失败会累计 `login_failed_attempts` 并锁号**（认证失败不要盲目重试）。
3. 平台用**自签证书** → `verify_tls=False`。
4. 指标查询窗口 **≥ 1 小时**才能覆盖 3600s 采集周期的指标（5 分钟窗口覆盖 0/21）。
5. **数据入库滞后约 5 分钟** → `end_time` 必须回退，否则尾部大量 null（5 分钟窗口实测 null 率 77.5%）。
6. `is_latest_point_only=true` 会让约 74% 的指标 `timestamp=0`（值对、时间丢）→ **用 `false` + ≥1h 窗口**。
7. **12 个指标分组码必须硬编码**（平台没有枚举接口）：`CPUMEMORY CONNECTION SYNCSTAT PROCESSRESOURCE NETWORK TRANSACTION IOSTORAGE LOCK INSTATUS FORDR AGENTSTATUS COREMETRICS`。分组名写错返回 **200 + 空 series**，不报错，只能靠"空分组告警"发现。
8. 告警 `POST /rds/v3/alarm/records`：`is_active`（活动/历史）、`is_convergence`（收敛列表/明细行）必传，`limit` ≤ 100 需分页；**历史告警仅保留 30 天**，停机超 30 天会丢告警，日志要显式告警。
9. 指标目录（name/unit/alarm_id）：`GET /rds/v3/monitor/metrics/series?name=<GROUP>`。
10. **指标中文名只在数据响应**的 `metric_name` 字段里，目录接口 `series` 不带中文名。

### 统一命令中心（82/101 客户指标的主通道）

`general_single_sql_query`（`cmd_id = 73722acc-bff6-4d04-adc0-bc1e1fa1eb7d`，平台内置命令）
的模板是 `select * from ({sql_text}) limit {limit}` —— **平台提供执行器，我们提供 SQL**，
可直接查 `pg_stat_database` / `pg_stat_user_tables` / `pg_stat_activity` / `pg_locks` /
`pg_stat_bgwriter` / `pg_settings` / `pg_database` / `pg_prepared_xacts`。

**三跳链路**（异步任务模型）：

```
POST …/customer-commands/{cmd_id}              → {job_id, node_tasks:[{task_id, node_id}]}（不含数据）
GET  …/batch-exec-job/task-records?job_id=     → {job_status, statistics:{running,…}, tasks:[…]}
GET  …/batch-exec-job/{job_id}/task-record/{task_id}/detail → {status, result, …}
```

硬约束（都是实测踩过的）：

- **结束判据用 `statistics.running == 0`，不能看 `job_status`** —— 实测节点任务已 `succeed`
  而 `job_status` 仍停在 `running`。
- **`result` 是 JSON 字符串**，要二次 `json.loads` 才是行列表。
- **只能在 master 节点执行**（`constraint_config.limit_node_types=["master"]`）；**一次下发多个 master 节点实测成功**（2026-11-06：HTTP 200，多条 node_tasks 全部 succeed）；下发前先
  `GET /rds/gaussdb/v3/instances/{id}/key-view-execute-node` 取 master 的 `node_id`。
- 单次往返 **3~8 秒**；实测 50 条 SQL 有 **3 条偶发失败**（重试即成功）→ **必须做重试**。
- **GaussDB 方言是 PG 9.2 级**：**无 `FILTER` 子句**（用 `CASE WHEN`）；
  **`state <> ''` 恒为假**（排除后台进程要用 `state is not null`）。
- 命令返回的是**当前快照，不是时序** —— 累计计数类指标（`xact_commit` 等）要**自己差分算速率**，
  并处理实例重启导致的计数归零。
- `sql_text` 在任务记录里被掩码为 `******`（执行不受影响，但不能靠任务记录回读 SQL）。
- 一次下发多指标要**合并 SQL**（现有 50 条 → 合并为 **11 条**），否则一轮要 3~7 分钟，撑不住采集周期。

## 编码约定

- **凭据只走环境变量**，禁止硬编码、禁止写入日志、禁止入库（`.env` 已 gitignore）。
- **不可变数据**：跨模块传递用 frozen dataclass / NamedTuple，不原地修改。
- 错误显式处理：捕获后包装成自定义异常并带上下文，**不静默吞异常**；用 `loguru` 的 `logger`，不用 `print`。
- 采集端代码单文件 < 400 行、函数 < 50 行；按功能分模块，不堆大文件。
  （`tools/` 下有几个离线脚本超 400 行，是历史产物；采集端按上面的新结构写，不沿用。）
- 测试用 pytest：映射、窗口计算、序列化写单元测试；TPOPS 与 Kafka 一律 mock。目标覆盖率 80%。
  `data/metric_response/` 里的逐指标原生响应可直接当回归样本回放。
- 提交信息用 conventional commits（`feat:` / `fix:` / `docs:` / `refactor:` / `test:` / `chore:`），正文中文。

## 待定事项（客户未最终确认，先按可配置实现）

> **交付前的问题单台账在 [`docs/问题单.md`](docs/问题单.md)**（12 条：2 条数据正确性高危、
> 4 条待客户确认、6 条交付/运维）—— 那里是**全部**问题的唯一台账（编号 `TPOPS-00x`、
> 状态、证据、方案、验收、勾选待办）。下面这份清单是其中**需要客户拍板口径**的部分。
>
> 开发方案 v2.1 §3.9 已为下列各项**冻结默认值**（消息信封、Topic 名、周期分档、告警字段、
> 脱敏策略），并说明各自通过「映射表 / 环境变量」吸收客户后续调整 —— **均不阻塞编码启动**。
> 本清单与该节保持同步。

1. **客户 JSON 字段定义** —— **已到客户对接方案**（`docs/ITSCADA第三方指标数据接入对接方案V2.docx`
   与 `docs/ITSCADA第三方告警数据接入对接方案V2.docx`），字段集合以两份方案的字段表为准，
   指标名以客户指标库 `高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` 的「指标库」sheet
   （101 个指标）为准。该表英文名存在 OCR 错字（如 `GAUSSDB.Number_of_ansactions_mmitted_r_cond`），
   需与客户核对正确拼写。可提供性**已判定：87 可提供**（wave-1 已把 11 条仅大小写不同的
   英文变体合并掉；另有 3 个无法提供 —— 合计 14 条不在生效口径内），逐指标真实调用 **98/98 通过**。
   落地：`data/mapping/itscada_field_map.json`（字段级映射）+ `data/mapping/metric_rules.json`（逐指标规则）。
2. **客户指标 → 取数方法的映射表已建好** —— `data/customer_metric_query_plan.json`
   （101 条逐指标查询方案，**生成物不入库**，由 `tools/build_metric_query_plan.py` 从
   `data/customer_metric_mapping.json` 重建），另加 sheet「指标库-指标获取方法」标注每个指标走哪条通道
   （命令中心SQL 82 / 监控指标接口 11 / 诊断优化接口 4 / 实例接口 1 / 无法提供 3）。
   采集端落地为 `data/mapping/` 下的**表驱动数据文件，不要写死在代码中**；
   3 个无法提供的指标丢弃并计数上报。
3. **Topic 命名与分区策略**。
4. **指标与告警是否同 topic**。
5. 客户指标库里的「数据更新周期 / 采集更新周期单位」与 TPOPS `collection_frequency` 的对齐口径。
6. **告警字段规范** —— 已按 ITSCADA 告警对接方案 §3.2 落地（`alarm_rules.json`）；
   仍待客户确认的是**告警→指标的挂载口径**：257 条平台告警里只有 3 条能对应到客户指标
   （都指向 `gaussdb.Tps`），其余 254 条事件类告警用 `gaussdb.alarm.<alarm_id>` 自造指标名。
7. **慢 SQL 的 `sql_text` 是否可外发** —— 含业务 SQL 文本，默认脱敏或截断，需客户书面确认。
8. **`resourceName` 与客户台账的对齐** —— **口径已定**：resourceName 只能表示一个节点 →
   固定 `{host_ip}_{node_id}`（节点 IP + TPOPS 节点 ID），指标与告警同规则；
   与节点无关的指标（命令中心实例级/表级/库级、诊断优化、实例接口）落到节点清单里的
   **第一个 master CN**（`tpops_kafka_collector/mapping/nodes.py`）；表名/库名/端口等业务维度改走
   `metricName`/`metricCode` 的占位符与 `tags.dimension_values`。
   剩下的只是与 ITSCADA 台账录入的资源名称核对，模板在 `itscada_field_map.json` 的
   `resource_name` 段，改表即可、不改代码。
9. **`value` 的线格式** —— 两份方案的字段表写 String，指标样例是数字、告警样例是字符串；
   当前按客户指标表【数据类型】序列化，需客户确认。
10. **A 类多 series 的归集口径** —— A 类 11 个客户指标都是**实例级一条消息**，
    平台却按组件给出**多条 series**（rds092/rds093/rds400 各 3 条 CN、rds402 6 条 CN+DN、
    rds133 3 条 CN、`gaussdb_instance_status` 1 条 `value`、`datanode_switch_status` 9 条 DN）。
    合成方式（速率类求和 / 启动时刻取最早 / 主备状态按 DN 展开）属业务口径，
    现状实现是**逐组件各出一条消息**（维度键 `COMPONENT_ID` 归入 `tags`）。
    确认后由 `metric_rules.json` 的 `value_rule` 承载 —— 详见开发方案 §4.2.4 与
    `data/mapping/open_items.json` 的 `a-class-multi-series`。

## 相关资料

| 文件 | 内容 |
|---|---|
| `docs/GaussDB指标至ItScada指标开发.md` | **项目概要**：需求简介 / 时间线 / API 调研（12 个接口、每个接口承担多少指标）/ 指标取数情况（87 可提供 + 14 条不在生效口径内） |
| `docs/开发方案-GaussDB指标与告警采集.md` | **开发方案 v2.1**（v2.0 重写版 + §7.1.1 里程碑时间线）：需求分析 → 调研 → 需求定稿（消息模型/Topic/周期/口径/脱敏已冻结）→ 技术设计（API 详解 + 逐指标取数 + 11 条合并 SQL）→ 代码设计 → 测试策略 → 里程碑与时间线 → 风险登记册 |
| `docs/README.md` | 文档总索引 |
| `docs/api/API参考-监控大盘.md` | 指标查询接口定义 |
| `docs/api/API参考-告警管理.md` | 告警接口定义 |
| `docs/api/API参考-登录.md` | mini-token 认证流程 |
| `data/customer_metric_query_plan.json` | 逐指标查询方案（生成物，采集端的实现依据） |
| `docs/ITSCADA第三方指标数据接入对接方案V2.docx` | **客户侧指标接入规范**：字段表、占位符规则、指标表模板 |
| `docs/ITSCADA第三方告警数据接入对接方案V2.docx` | **客户侧告警接入规范**：字段表、告警压缩规则 |
| `tpops_kafka_collector/mapping/README.md` | 映射层说明：字段含义、取值规则类型、怎么改 |
| `docs/映射表-ITSCADA字段对照.md` | **开发依据**：两张字段级映射表（指标 / 告警），原始字段 → ITSCADA 客户字段 |
| `data/metric_response/README.md` | 逐指标**原生 API 响应体**留档（98 个文件）：格式、通道构成、重抓命令 |
| `data/alarm/README.md` | 告警侧数据结构与抓取产物说明 |
| `tools/README.md` | 离线取数脚本用法（`tools/tpops_client.py` 是可用参考实现） |
| `高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` | 客户指标库（101 个指标，映射的基准；含「指标库-指标获取方法」sheet） |
| `docs/广东电网ITSCADA系统监控接入操作指引（数据库）V0.4.docx` | 客户监控平台接入指引（其余数据库走 Agent 方式，非 Kafka） |
| `docs/06_GaussDB V2.0-26.861.1 产品文档(for 轻量化部署形态) 01.chm` | GaussDB 产品文档原始包（312 MB，未入库） |
