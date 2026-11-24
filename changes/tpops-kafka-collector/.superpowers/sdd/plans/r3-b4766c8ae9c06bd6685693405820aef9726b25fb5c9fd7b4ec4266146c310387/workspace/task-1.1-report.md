# 任务 1.1 报告 —— 配置加载与启动校验

变更：`tpops-kafka-collector` ｜ 波次：wave-1（骨架与平台层）｜ 任务：1.1

## 1. 输入偏差（重要）

任务书指定的两个简报文件**在本仓库中不存在**：

- `changes/tpops-kafka-collector/.superpowers/sdd/plans/r3-…/workspace/wave1-execution-brief.md` → 不存在
- `changes/tpops-kafka-collector/.superpowers/sdd/plans/r2-…/workspace/task-1.1-brief.md` → 不存在

`.superpowers/` 下只有 `sdd/execution-plan.json`、`sdd/execution-recommendation.json`、`sdd/workflow-selection.json`。

因此本任务改为以下列既有工件为准（均已核对）：

| 来源 | 用途 |
|---|---|
| `docs/开发方案-GaussDB指标与告警采集.md` §5.5 | 配置项清单与默认值/范围（权威） |
| `changes/tpops-kafka-collector/specs/runtime-and-config/spec.md` | 必填项、越界行为、凭据不入日志 |
| `changes/tpops-kafka-collector/specs/collection-channels/spec.md` §分页 | `CMD_PAGE_SIZE`（默认 100，≤1000） |
| `changes/tpops-kafka-collector/specs/platform-client/spec.md` | `CMD_CONCURRENCY` 范围 1~10 |
| `changes/tpops-kafka-collector/tasks.md` L83-86（1.1）、L63-64（0.6 改 §5.5） | 任务验收口径、`CMD_CONCURRENCY` 默认 2、`CMD_PAGE_SIZE` 默认 100 |
| `changes/tpops-kafka-collector/execution-contract.md`（Wave 1 / Test Obligations） | 波次完成标准与门禁 |

## 2. 文件清单

新增：

| 文件 | 说明 |
|---|---|
| `tpops_kafka_collector/config.py` | 配置加载与校验（352 行） |
| `tpops_kafka_collector/__init__.py` | 包入口（**超出任务书独占清单**，见 §6） |
| `tests/unit/test_config.py` | 单元测试（355 行，47 个用例） |

未修改任何其他既有文件；**未执行 git commit**。

## 3. 设计要点

- API：`load_config(env: Optional[Mapping[str, str]] = None) -> Config`。
  默认读 `os.environ`；显式传入映射便于测试（也便于将来从文件加载）。
- `Config` 是 `@dataclass(frozen=True)`，29 个字段全部对应 §5.5 的一个环境变量，
  字段名 = 环境变量名小写下划线化（`CMD_PAGE_SIZE` → `cmd_page_size`）。
  加载完成后运行期不再读环境变量（满足 spec「运行期间不再读取环境变量」）。
- `ConfigError` 定义在 `config.py` 内（**未依赖 wave-1 任务 1.2 的 `errors.py`**，
  避免与并发任务产生 import 耦合）。后续若 1.2 落地 `TpopsError`，可在 1.2/本任务之间
  统一继承关系。
- **凭据处理**：`TPOPS_PASSWORD` 用 `field(repr=False)`，不出现在 `repr`/日志；
  异常消息里凭据取值一律替换为 `<redacted>`（`_display()`），只报变量名。
  密码**不做 strip**（前后空格属于密码本身），但空白视为未设置。
- **快速失败**：必填缺失（`TPOPS_USER` / `TPOPS_PASSWORD` / `KAFKA_BOOTSTRAP_SERVERS`）
  或取值非法 → 抛 `ConfigError`，并 `LOGGER.error('配置校验失败：%s', exc)`。
  本模块**不 import 任何网络库**，加载路径上不产生任何请求。
- **取值与范围**（全部来自 §5.5）：

| 变量 | 默认 | 校验 |
|---|---|---|
| `TPOPS_USER` / `TPOPS_PASSWORD` | — | **必填** |
| `TPOPS_BASE_URL` | `https://172.16.103.193:8002` | 非空 |
| `TPOPS_VERIFY_TLS` | `false` | 布尔（true/false/1/0/yes/no/on/off） |
| `TPOPS_REQUEST_TIMEOUT` | `60` | ≥1 |
| `KAFKA_BOOTSTRAP_SERVERS` | — | **必填**，逗号分隔且非空 |
| `KAFKA_TOPIC_METRICS` / `KAFKA_TOPIC_ALARMS` | `gaussdb.metrics` / `gaussdb.alarms` | 非空 |
| `KAFKA_TOPIC_PARTITIONS` | `4` | ≥1 |
| `KAFKA_COMPRESSION` | `lz4` | none/gzip/snappy/lz4/zstd |
| `POLL_INTERVAL_SECONDS` | `30` | ≥1 |
| `METRIC_WINDOW_HOURS` | `2` | **≥1（强制下限）** |
| `METRIC_END_LAG_MINUTES` | `5` | ≥0 |
| `CMD_CONCURRENCY` | **2** | **1~10** |
| `CMD_LIMIT` | `1000` | 1~1000 |
| `CMD_PAGE_SIZE` | **100** | **1~1000** |
| `CMD_POLL_TIMEOUT` | `60` | ≥1 |
| `CMD_RETRY_ATTEMPTS` | `3` | ≥0 |
| `ALARM_LIMIT` | `100` | 1~100（平台上限） |
| `ALARM_RETENTION_DAYS` | `30` | ≥1 |
| `SQL_MASKING_ENABLED` | `true` | 布尔 |
| `SQL_TEXT_MAX_LENGTH` | `512` | ≥1 |
| `RETRY_MAX_ATTEMPTS` | `5` | ≥0 |
| `RETRY_BASE_SECONDS` | `1` | ≥0（浮点） |
| `STATE_DIR` | `./state` | 非空 |
| `HEALTH_HOST` / `HEALTH_PORT` | `127.0.0.1` / `8088` | 端口 1~65535 |
| `LOG_LEVEL` | `INFO` | DEBUG/INFO/WARNING/ERROR/CRITICAL |
| `LOG_FORMAT` | `text` | text/json |

## 4. TDD 记录

### RED

先只写 `tests/unit/test_config.py`，实现尚未存在：

```
$ conda run -n tpops python -m pytest tests/unit/test_config.py -q
ERROR collecting tests/unit/test_config.py
ImportError while importing test module '.../tests/unit/test_config.py'.
tests/unit/test_config.py:12: in <module>
    from tpops_kafka_collector.config import Config, ConfigError, load_config
E   ModuleNotFoundError: No module named 'tpops_kafka_collector'
1 error in 0.25s
```

**RED 失败断言**：`ModuleNotFoundError: No module named 'tpops_kafka_collector'`
（采集阶段即失败，47 个用例一个都没跑到）。

### GREEN

```
$ conda run -n tpops python -m pytest tests/unit/test_config.py -q
..............................................                           [100%]
47 passed in 0.44s

$ conda run -n tpops python -m pytest -q
212 passed in 10.40s

$ ruff check tpops_kafka_collector/ tests/unit/test_config.py
All checks passed!
```

用例覆盖：默认值、逐项读取、必填缺失（参数化 3 个）、空白必填、地址列表拆分、
不可变性、窗口 <1、并发越界、页容量 >1000、告警分页 >100、非数值、非数值浮点、
端口非数值/越界、布尔 11 种取值 + 非法、三个枚举非法、密码不入 repr、
密码不进异常消息、加载期不建 socket。

## 5. 基线说明

任务书给出的当前基线是 **344 passed**，实测仓库基线是 **164 passed**
（`pytest -q` 在本任务开工前）。本任务新增 47 个用例，结束时 **212 passed**
（差额 1 个用例来自并发任务 `impl-taskB` 新增的文件，非本任务产生）。

## 6. 偏离与需要决策的地方

1. **新建了 `tpops_kafka_collector/__init__.py`**（不在任务书独占清单内）。
   原因：包目录此前**根本不存在**（wave-0 尚未落地），没有 `__init__.py` 则
   `tpops_kafka_collector.config` 无法被 import，RED 都跑不起来。
   内容仅 3 行 docstring，无逻辑；若 wave-0 后续落地同名文件，按其版本覆盖即可。
2. **保留 `CMD_LIMIT`（默认 1000）并新增 `CMD_PAGE_SIZE`（默认 100）**。
   开发方案 §5.5（当前磁盘版本）只有 `CMD_LIMIT`；tasks.md L63-64 的 0.6 任务要求
   §5.5「新增 `CMD_PAGE_SIZE`、`CMD_CONCURRENCY` 默认值改 2」，即**新增而非替换**。
   两者的分工：`CMD_LIMIT` = 单批 SQL 的 limit 上限，`CMD_PAGE_SIZE` = 分页容量。
   **依赖**：0.6 改完 §5.5 后，两者才能与 `config.py` 完全一致（wave-4 的
   「环境变量表与 config.py 读取的变量一致」门禁依赖 0.6 先落地）。
3. **`CMD_CONCURRENCY` 默认值取 2**（§5.5 磁盘版本写的是 9）——按 tasks.md 0.6 的
   明确要求取 2；§5.5 待 0.6 同步。
4. **错误类型放在 `config.py` 而非 `errors.py`**：避免与并发任务 1.2 抢文件 /
   产生 import 依赖。若后续统一到 `TpopsError` 体系，需要在 1.2 落地后做一次对齐。
5. **`KAFKA_COMPRESSION` 允许 `zstd`**：§5.5 只写默认值 `lz4`，未限定取值集合；
   这里按 kafka-python 2.0.2 支持的集合收敛（none/gzip/snappy/lz4/zstd）。
   `LOG_LEVEL` / `LOG_FORMAT` 同理按 §5.5 的「`text` / `json`」描述收敛。
6. **`METRIC_WINDOW_HOURS` 允许 1 以上任意整数**（§5.5 只规定下限 1），未加人为上限；
   确需上限时由客户口径补。

## 7. 遗留问题

- §5.5 与 `config.py` 的变量表要等 **任务 0.6** 同步后才能完全对齐
  （`CMD_PAGE_SIZE`、`CMD_CONCURRENCY` 默认值）；`.env.example` 目前也缺
  `CMD_*` / `KAFKA_TOPIC_PARTITIONS` / `KAFKA_COMPRESSION` / `LOG_*` 等项，
  是否补齐 `.env.example` 不在本任务范围（wave-4 有交付文档口径）。
- `tests/test_py37_compat.py` 的 `SCAN_DIRS` 目前只扫 `tools`/`tests`/`data`，
  **尚未包含 `tpops_kafka_collector`**（注释里写「尚未建立，出现了就一起检查」）。
  本文件已在 Python 3.7.9 解释器下实跑通过，但建议 wave-3 门禁把该目录纳入扫描。
- `ConfigError` 与 `errors.py`（任务 1.2）的继承关系待对齐；建议 1.2 落地后
  让 `ConfigError` 继承 `TpopsError`，以便上层统一捕获。
