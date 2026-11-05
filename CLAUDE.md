# CLAUDE.md

## 项目目标

客户不使用 TPOPS（GaussDB 监控平台）的管理界面，改为把**多个数据库管理平台的监控/告警数据统一导入客户自有的监控平台**。

本项目负责其中 GaussDB 这一路：通过 TPOPS 的 API 按时钟窗口拉取指标与告警数据 → 按客户的字段规范做映射 → 以客户约定的格式写入 Kafka，由客户侧消费。

- 形态：**常驻进程**，持续循环运行，不是一次性脚本
- 语言：**Python 3.7.9**（见下方红线，语法必须兼容）
- 投递：Kafka，**PLAINTEXT**，消息体为 **JSON**

## 架构 / 数据流

```
Scheduler(窗口 + 水位)
   → TPOPS Client  (mini-token 认证)
   → Normalizer    (TPOPS 原始结构 → 内部统一 DTO)
   → FieldMapper   (按映射表转成客户字段)
   → Serializer    (唯一序列化出口，JSON)
   → Kafka Producer(PLAINTEXT)
   → 推进水位
```

一次循环：拉取 → 转换 → 投递 → **成功后才推进水位**。投递失败不推进水位，重启后从水位续传（至少一次语义）。

## 目录结构

```
tpops_kafka/
├── main.py          # 入口：装配依赖、启动调度循环、信号处理
├── config.py        # 配置加载（环境变量 + 可选配置文件）
├── scheduler.py     # 轮询循环、水位推进、重试退避、优雅退出
├── models.py        # 跨模块 DTO（frozen dataclass / NamedTuple）
├── tpops/
│   ├── client.py    # TPOPS HTTP 客户端（从 tools/tpops_client.py 提炼）
│   ├── metrics.py   # 指标拉取、窗口与 end_time 计算
│   └── alarms.py    # 告警拉取（active/history × 收敛/明细）
├── mapping/
│   ├── table.py     # 映射表加载与查表（表驱动，映射不写死在代码里）
│   └── rules.py     # 字段级转换规则（类型、单位、缺省值）
└── kafka/
    ├── producer.py  # PLAINTEXT 生产者
    └── serializer.py# JSON 序列化

tools/     # 现有离线取数脚本 + 参考实现，保留
data/      # 离线样本（指标侧：目录、映射、查询方案、实测结果）
│  └── alarm/   # 告警侧数据（抓取产物 + 告警定义目录），详见该目录 README
docs/      # TPOPS API 参考（由 CHM 转换，未增补）
```

## 运行

```bash
cp .env.example .env        # 填 TPOPS_PASSWORD、KAFKA_* 等
set -a && source .env && set +a
python -m tpops_kafka.main
```

配置项（全部走环境变量，`.env` 已 gitignore）：

| 变量 | 说明 |
|---|---|
| `TPOPS_USER` / `TPOPS_PASSWORD` / `TPOPS_BASE_URL` | TPOPS 认证与地址 |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka 地址，逗号分隔 |
| `KAFKA_TOPIC_METRICS` / `KAFKA_TOPIC_ALARMS` | Topic 名（待客户确认） |
| `POLL_INTERVAL_SECONDS` | 轮询间隔 |
| `METRIC_WINDOW_HOURS` | 指标查询窗口，**不得小于 1** |
| `METRIC_END_LAG_MINUTES` | end_time 回退量，默认 5 |

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
requests>=2.25,<3
pytest>=7,<8
pytest-cov<5
```

如改用 `confluent-kafka`，注意其 wheel 对 3.7 的支持范围，且需 librdkafka 运行时。

## TPOPS 接口要点（均已实测，勿凭直觉改）

1. **路径前缀是 `/rds/`**（根路径），不是文档里的 `/gaussdb/`——后者只适用于 SPA 控制台。实例列表：`GET /rds/v3.2/instances`。
2. 认证：`POST /rds/v3/auth/mini-token`，Token 在**响应头** `X-Mini-Token`；服务端 session_timeout 1440 分钟，需按需刷新。**登录失败会累计 `login_failed_attempts` 并锁号**。
3. 平台用**自签证书** → `verify_tls=False`。
4. 指标查询窗口 **≥ 1 小时**才能覆盖 3600s 采集周期的指标（5 分钟窗口覆盖 0/21）。
5. **数据入库滞后约 5 分钟** → `end_time` 必须回退，否则尾部大量 null（5 分钟窗口实测 null 率 77.5%）。
6. `is_latest_point_only=true` 会让约 74% 的指标 `timestamp=0`（值对、时间丢）→ **用 `false` + ≥1h 窗口**。
7. **12 个指标分组码必须硬编码**（平台没有枚举接口）：`CPUMEMORY CONNECTION SYNCSTAT PROCESSRESOURCE NETWORK TRANSACTION IOSTORAGE LOCK INSTATUS FORDR AGENTSTATUS COREMETRICS`。分组名写错返回 **200 + 空 series**，不报错，只能靠"空分组告警"发现。
8. 告警 `POST /rds/v3/alarm/records`：`is_active`（活动/历史）、`is_convergence`（收敛列表/明细行）必传，`limit` ≤ 100 需分页；**历史告警仅保留 30 天**，停机超 30 天会丢告警，日志要显式告警。
9. 指标目录（name/unit/alarm_id）：`GET /rds/v3/monitor/metrics/series?name=<GROUP>`。

## 编码约定

- **凭据只走环境变量**，禁止硬编码、禁止写入日志、禁止入库（`.env` 已 gitignore）。
- **不可变数据**：跨模块传递用 frozen dataclass / NamedTuple，不原地修改。
- 错误显式处理：捕获后包装成自定义异常并带上下文，**不静默吞异常**；用 `logging`，不用 `print`。
- 单文件 < 400 行、函数 < 50 行；按功能分模块，不堆大文件。
- 测试用 pytest：映射、窗口计算、序列化写单元测试；TPOPS 与 Kafka 一律 mock。目标覆盖率 80%。
- 提交信息用 conventional commits（`feat:` / `fix:` / `docs:` / `refactor:` / `test:` / `chore:`），正文中文。

## 待定事项（客户未最终确认，先按可配置实现）

1. **客户 JSON 字段定义** —— 字段名以客户指标库《高斯DB-指标明细.xlsx》（101 个指标）为准。该表英文名存在 OCR 错字（如 `GAUSSDB.Number_of_ansactions_mmitted_r_cond`），需与客户核对正确拼写。
2. **TPOPS 指标 → 客户指标映射表** —— 待补充。落在 `tpops_kafka/mapping/` 的数据文件里，**表驱动，不要写死在代码中**；未匹配的 TPOPS 指标丢弃并计数上报。
3. **Topic 命名与分区策略**。
4. **指标与告警是否同 topic**。
5. 客户指标库里的「数据更新周期 / 采集更新周期单位」与 TPOPS `collection_frequency` 的对齐口径。

## 相关资料

| 文件 | 内容 |
|---|---|
| `docs/README.md` | TPOPS API 参考索引（CHM 原文转换） |
| `docs/API参考-监控大盘.md` | 指标查询接口定义 |
| `docs/API参考-告警管理.md` | 告警接口定义 |
| `docs/API参考-登录.md` | mini-token 认证流程 |
| `tools/README.md` | 离线取数脚本用法（`tools/tpops_client.py` 是可用参考实现） |
| `高斯DB-指标明细.xlsx` | 客户指标库（101 个指标，映射的基准） |
| `广东电网ITSCADA系统监控接入操作指引（数据库）V0.4.docx` | 客户监控平台接入指引（其余数据库走 Agent 方式，非 Kafka） |
