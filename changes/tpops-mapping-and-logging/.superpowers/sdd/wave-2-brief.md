# Wave 2 任务简报 · 日志框架换成 loguru（wave-2-logging）

**工作目录（唯一）**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`
**分支**：`tpops_metric_dev`（worktree；主仓 `/mnt/c/sml/project/py_project/tpops_metric_dev` 不要动）
**Python**：`conda run -n tpops python ...`（conda env `tpops`，Python **3.7.9**）
**权威工件**（先读）：
- `changes/tpops-mapping-and-logging/execution-contract.md`（R8~R17、R19 / Design Constraints / Test Obligations / Escalation Rules）
- `changes/tpops-mapping-and-logging/specs/runtime-and-config/spec.md`（R8~R17）
- `changes/tpops-mapping-and-logging/design.md`（**决策 2 / 3 / 4 / 5 / 6**）
- `changes/tpops-mapping-and-logging/tasks.md`（任务 2.1~2.7）

> wave-2 依赖 wave-1-mapping 的 review receipt 为 `pass` 才开工（编排上只排先后，**无代码依赖**）。

## 已实测核实的事实（2026-11-26 在 Python 3.7.9 上实测过，不要重新论证）

| 核实项 | 结论 |
|---|---|
| `loguru==0.8.0` 的 `requires_python` | `>=3.5` → **3.7.9 可用**，实测 `import loguru` 成功 |
| wheel | `loguru-0.8.0-py3-none-any.whl` → **纯 Python**，x86_64 与 aarch64 共用同一份部署包 |
| Linux + py3.7 运行期依赖 | **0 个**（`colorama` 仅 Windows、`aiocontextvars` 仅 py<3.7）；`pip download loguru` 实测未拉任何依赖 |
| 文件名带日期 | `logger.add("collector_{time:YYYY-MM-DD}.log", …)` → 实测产出 `collector_2026-11-26.log` |
| 按天轮转 | `rotation="1 day"` 原生 |
| 保留 3 天 | `retention="3 days"` 原生（`retention` 支持 str / int / timedelta / callable） |
| 同名冲突改名 | loguru 自动把旧文件改名为 `<基名>.<日期>_<时分秒_微秒>.log` 并新建同名文件续写（**不是** `.1` 序号） |
| 通道分流 | sink 的 `filter=` 读 `record["extra"]["channel"]` → 实测通道文件只含本通道记录 |
| 级别对齐 | `{level: <8}` 原生 |
| 异常堆栈 | 原生追加（实测输出完整 traceback） |
| callable rotation 语义 | `rotation(message, file)` 返回真值即**立即**切；返回 datetime **不**表示「到点再切」 |

## 铁律

1. **Python 3.7.9**：禁海象 `:=`、`f"{x=}"`、字典合并 `d1 | d2`、内建泛型注解 `list[str]`、
   `typing.Literal/Protocol/TypedDict/Final`、`functools.cached_property`、`importlib.metadata`、
   `math.prod`、`zoneinfo`、`str.removeprefix/removesuffix`。
2. **只允许新增 `loguru==0.8.0`**（纯 Python、零传递依赖）。其它依赖**一律不加**。
   运行期依赖仍须全部是 `*-none-any` → 目标机不编译、不联网。
3. 采集端单文件 **< 400 行**、函数 **< 50 行**；不可变 DTO；不用 `print` 记日志；
   凭据只走环境变量、**禁入日志**（含日志文件）。
4. **TDD：先写失败测试（RED）→ 看到失败 → 再实现（GREEN）→ 重构。**
5. 不改 wave-1 的口径成果（生效 87 条、排除清单、表级 `schema.table`、告警统一名）。
6. 不碰 wave-3 的 `collector.sh` 后台化（任务 3.1~3.3 归 wave-3），但**依赖它**的
   `LOG_DIR` 默认值语义要与之对齐（见下）。

## 设计要点（决策 2/3/4/5，实现时按此落地）

### 布局（6 个基名 × 日期后缀）

```
<LOG_DIR>/collector-<YYYY-MM-DD>.log                  # 总日志
<LOG_DIR>/collector-monitor-<YYYY-MM-DD>.log          # A 类
<LOG_DIR>/collector-command_center-<YYYY-MM-DD>.log   # B 类
<LOG_DIR>/collector-diagnostic-<YYYY-MM-DD>.log       # C 类
<LOG_DIR>/collector-instance-<YYYY-MM-DD>.log         # D 类
<LOG_DIR>/collector-alarm-<YYYY-MM-DD>.log            # E 类
```

- `LOG_DIR` **未设置** → 只写 stderr（不落盘），这是开发态既有行为；
- `LOG_DIR` 指向的目录不存在 → 自动创建；不可写 → **启动失败**（不静默降级）；
- `LOG_DIR` 由 `collector.sh run` 显式设置（默认 `<安装目录>/logs`，wave-3 落地）；
  `<安装目录>` = `collector.sh` 所在目录。

### sink 分层（决策 2）

| sink | 收什么 | 落点 |
|---|---|---|
| 总日志 | 生命周期、tick 整体汇总、**每通道一行**简洁汇总、WARNING 及以上 | `collector-<日期>.log` |
| 通道 sink ×5 | 该通道明细 + 该通道每轮汇总行 | `collector-<channel>-<日期>.log` |
| stderr | 与总日志同内容（前台/调试可见） | 终端 |

通道归属用 `logger.bind(channel="…")` 打在记录上，通道 sink 的 `filter=` 读
`record["extra"]["channel"]`。**未绑定 channel 的记录只进总日志**。
5 个采集器模块各加一行 `LOGGER = logger.bind(channel="…")`：
`collectors/monitor.py`→`monitor`、`collectors/command_center.py`→
`command_center`（注意 `command_center_assembly.py` 也是 B 类，一并绑 `command_center`）、
`collectors/diagnostic.py`→`diagnostic`、`collectors/instance.py`→`instance`、
`collectors/alarm.py`→`alarm`。

第三方库（`kafka` / `urllib3` / `requests`）的 stdlib 日志经官方 `InterceptHandler`
转发进 loguru，并用 `logger.disable("kafka")` 等压噪。

### 格式（决策 4）

- **text sink**：**直接用 loguru 原生格式**，不自研 logback pattern 解析器。
  可用 `LOG_FORMAT` 覆盖（loguru 原生语法，如
  `{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name} | {message}`）。
- **json sink**：用 loguru 的 `format=` **自定义函数**（不是字符串），产出既有 schema：
  `timestamp` / `level` / `logger` / `thread` / `message`；
  整条消息是 `k=v` 形式时**摊平成顶层字段**；异常带 `exception` 字段。
  （`serialize=True` 做不到 —— 它输出 `{"text":…, "record":{…}}` 固定结构。）

### 切分与保留（决策 5）

- `rotation=` **自定义回调**：跨天（次日 00:00）**或** 当日文件
  `file.tell() + len(message) > LOG_MAX_BYTES` → 返回真值立即切；
- `retention=` 保留 `LOG_RETENTION_DAYS` 天（默认 3）；被改名产生的同日分片同样被清理；
- 文件名用 loguru 的 `{time:YYYY-MM-DD}`。

### 每通道每轮汇总（决策 6，字段固定 11 个）

```
channel=command_center tick=12 instances=1 fetched=520 mapped=498 produced=498 \
failed=0 windows=11 calls=4 elapsed=3.2s delivery_rate=1.000
```

- `mapped` = 该通道各实例 `DeliveryStats.planned` 之和（**幂等过滤后**尝试投递数，与整体行同口径）；
- `produced` = `DeliveryStats.delivered` 之和；`failed` = `DeliveryStats.failed` 之和；
- `fetched` / `windows` / `calls` = 该通道各任务 `TaskOutcome` 的对应字段之和；
- `elapsed` = 该通道本轮墙钟；`delivery_rate` = `produced/mapped`（`mapped=0` 时 1.000）；
- **未到期的通道不输出汇总行**；**各通道之和 == 整体行同名字段**；
- 汇总行必须写成**纯 `k=v` token（值不含空格）**，否则 JSON 摊平失效。

## 任务清单（tasks.md 2.1~2.7）

- **2.1 loguru 依赖落地**：`requirements-runtime.txt` 加 `loguru==0.8.0`；
  `tools/build_deploy_bundle.py` 的离线 wheel 清单纳入 loguru；
  确认纯 Python 且 py3.7 无传递依赖。
  新增 `tests/unit/test_loguru_dependency.py`。
- **2.2 `logging_setup.py` 重写为 loguru sink 装配**（见上面「设计要点」全部）；
  保留现有 `setup_logging(level, log_format, stream)` 签名与幂等语义
  （py37 无 `basicConfig(force=…)`，自判 `root.handlers`）。
  新增 `tests/unit/test_logging_files.py`（覆盖日期文件名、跨天切分、单日超限改名、
  过期清理、通道 filter 分流、总日志含各通道简洁汇总、通道 INFO 不进总日志而 ERROR 两边都有、
  目录不可写报错、既有签名不回归、重复调用不叠加 sink、json 的 `exception` 字段保留）。
- **2.3 全面改用 loguru API**：生产包内 **40 个**用 `logging.getLogger` 的模块改成
  `from loguru import logger`（**调用点 `LOGGER.info(...)` 本身不动**）；
  5 个采集器模块各加 `LOGGER = logger.bind(channel="…")`。
  新增 `tests/unit/test_loguru_migration.py`（生产包内不再出现 `logging.getLogger`，
  除外：转发第三方库用的 `InterceptHandler`）。
- **2.4 配置项与快速失败**：`config.py` 加 `LOG_DIR` / `LOG_FORMAT` / `LOG_MAX_BYTES`
  （默认 50 MB）/ `LOG_RETENTION_DAYS`（默认 3）；非法取值 → 启动失败退出码 2；
  目录不可写 → 启动失败并说明路径；`LOG_DIR` 未设置 → 只写 stderr；同步 `.env.example`。
  新增 `tests/unit/test_config.py`。
- **2.5 通道级每轮统计**：新增不可变 DTO `ChannelRoundStats`（`scheduler_types.py`）
  与聚合逻辑**新文件 `channel_stats.py`**（`scheduler.py` 已 392 行，
  **不得**把聚合逻辑堆进去）。
  新增 `tests/unit/test_channel_round_stats.py`。
- **2.6 通道级汇总行输出**：`scheduler_report.py` 每轮为到期且完成的通道各输出一条
  （`logger.bind(channel=…)` 打出）；总日志另出一条同字段核心计数。
- **2.7 敏感信息不进落盘日志**：跑一轮后扫日志文件，确认无口令 / mini-token /
  `sql_text` 全文。新增 `tests/integration/test_logs_have_no_secrets.py`。

### 本波次还要顺带收口的三件（wave-1 遗留，不是新范围）

1. **`CLAUDE.md`**：把「用 `logging`，不用 `print`」改成「用 `loguru` 的 `logger`，不用 `print`」；
   「运行时依赖只有 requests / kafka-python / urllib3 三个纯 Python 包」这句要加上 loguru；
   **「98 可提供 / 3 丢弃」要改成「87 可提供」**（wave-1 已把口径收敛到 87）。
2. **`docs/问题单.md`**：TPOPS-001 / TPOPS-003 已由 wave-1 解决 → 勾选并注明解决方式。
3. **`requirements-dev.txt`**：钉 ruff 版本（现装 0.17.6 与仓库 `ruff.toml` 的 `select=UP` 冲突，
   基线 473~764 条历史违规不是本次引入）。

## 证明命令

```bash
cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
conda run -n tpops python -m pytest tests/unit/test_loguru_dependency.py \
    tests/unit/test_logging_files.py tests/unit/test_loguru_migration.py \
    tests/unit/test_config.py tests/unit/test_channel_round_stats.py \
    tests/integration/test_logs_have_no_secrets.py -q
conda run -n tpops python -m pytest -q                 # 全量必须全绿
conda run -n tpops python -m pytest tests/test_py37_compat.py -q
conda run -n tpops python -m ruff check tpops_kafka_collector tests
```

**ruff 说明**：仓库基线本就不绿（历史 lint 债另案）。本波次的门槛是
「**本次改动文件不引入新的 ruff 违规类别**」，不要为了刷绿去改历史代码。

## 交付

1. 提交代码（conventional commits，中文正文，**不要**加署名行）。
2. 实现报告写到
   `changes/tpops-mapping-and-logging/.superpowers/sdd/wave-2-implementer-report.md`：
   每个任务做了什么、RED 证据、GREEN 证据、改动文件清单、
   40 个模块迁移的**机械改法说明**（是否真的只改了 import 与 logger 创建行）、
   需要 reviewer 特别注意的点。
3. 不要修改 `execution-contract.md` / `tasks.md` / `proposal.md` / `specs/` / `design.md`。
4. 拿不准**先问**，不要猜。
