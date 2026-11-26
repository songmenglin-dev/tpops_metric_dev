# Wave 2 实现报告 · 日志框架换成 loguru（wave-2-logging）

- **执行顺序**：严格按 `wave-2-brief.md` 的 2.1 → 2.7，每个任务 **RED → GREEN → 重构**
- **工作目录**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（worktree，分支 `tpops_metric_dev`）；主仓未动
- **Python**：`conda run -n tpops python ...`（3.7.9）
- **新增依赖**：只有 `loguru==0.8.0`（纯 Python、Linux+py3.7 零传递依赖）
- **状态**：**DONE_WITH_CONCERNS**（功能与规格对齐，但有 3 处需要 reviewer 拍板的判断，见 §5）

---

## 1. 逐任务：做了什么 / RED 证据 / GREEN 证据

### 2.1 loguru 依赖落地

**做了什么**

- `requirements-runtime.txt`：加 `loguru==0.8.0`，并把「只有这 3 个包」改成「4 个包」，注明版本红线
  （0.8.0 是最后一个支持 py3.7 的版本）与「Linux+py3.7 零传递依赖」。
- `deploy/collector.sh`：目标机自检的运行时依赖清单 `for mod in requests kafka urllib3` → 加上 `loguru`。
- `tools/build_deploy_bundle.py`：离线 wheel 清单本来就是从 `requirements-runtime.txt` 解析的
  （`deploy_bundle/bundle.py:92,151`），因此 loguru 自动进部署包；只把模块 docstring 里的
  「三个纯 Python 包」改成四个。
- `tests/test_deploy_bundle.py`：运行时依赖清单断言 3 → 4 个；假 wheel 列表加 `loguru-0.8.0-py3-none-any.whl`；
  纯 Python wheel 数量断言 4 → 5。

**RED 证据**（`tests/unit/test_loguru_dependency.py`，新增）

```
FAILED tests/unit/test_loguru_dependency.py::test_runtime_requirements_pin_loguru
FAILED tests/unit/test_loguru_dependency.py::test_deploy_bundle_ships_the_same_runtime_requirements
FAILED tests/unit/test_loguru_dependency.py::test_collector_sh_dependency_probe_covers_loguru
3 failed, 5 passed
```

**GREEN 证据**：`65 passed`（`test_loguru_dependency.py` + `test_deploy_bundle.py`）。

### 2.2 `logging_setup.py` 重写为 loguru sink 装配

**做了什么**

- 新增 `tpops_kafka_collector/logging_formats.py`（160 行）：`json_format`（JSON 的
  `format=` 自定义函数）、`make_rotation`（切分回调）、`make_retention`（保留期回调）、
  `flatten_pairs`（`k=v` 摊平）、`logical_logger_name`（分层 logger 名）。
- 重写 `tpops_kafka_collector/logging_setup.py`（304 行）：7 个 sink
  （stderr 1 + 总日志 1 + 通道 5），日期后缀文件名、自定义 rotation、保留期、
  `InterceptHandler`（第三方库 stdlib 日志 → loguru）、`reset_logging()`。
- **签名与幂等语义不变**：`setup_logging(level, log_format, stream)` 三个位置参数照旧，
  新增 `log_dir` / `log_max_bytes` / `log_retention_days` / `log_pattern` 为**可选关键字参数**；
  重复调用不叠加 sink（自管 sink id + `root.handlers`）。

**RED 证据**（`tests/unit/test_logging_files.py`，新增，19 个用例）

```
18 failed, 1 passed, 19 errors      # 全部 AttributeError（模块里还没有这些函数）
```

**GREEN 证据**：`19 passed`；连同既有的 `tests/unit/test_logging_setup.py` 共 `28 passed`。

### 2.3 全面改用 loguru API

**做了什么**：生产包内 **39 个**用 `logging.getLogger` 的模块全部改成 `from loguru import logger`
（简报说的「40 个」里，第 40 个是 `logging_setup.py` —— 它**必须**保留 stdlib logging，
因为要装 `InterceptHandler` 把第三方库的日志转发进 loguru），

`LOGGER = logging.getLogger(__name__)` → `LOGGER = logger`；5 个采集器模块（含
`command_center_assembly.py`）改成 `LOGGER = logger.bind(channel="…")`。
**外加 111 处调用点的占位符转换**（printf → `str.format`）—— 详见 §3。

**RED 证据**（`tests/unit/test_loguru_migration.py`，新增，6 个用例）

> 说明：这个任务的守卫测试是在机械迁移**之后**补写的，RED 状态用「把迁移整体回退」
> 的方式复现过：迁移前生产包里有 39 处 `logging.getLogger`、111 处 printf 占位符，
> 6 个守卫用例全部变红（其中 4 个直接因 `logging.getLogger` 命中、2 个因占位符命中）。

**GREEN 证据**：`6 passed`。

### 2.4 配置项与快速失败

**做了什么**

- `tpops_kafka_collector/log_config.py`（新增）：`LOG_FORMAT`（`text`/`json`
  **或**一个 loguru 原生格式串）、`LOG_PATTERN`（显式覆盖，优先）、`LOG_DIR`
  （不存在就建、不可用/不可写就报错）。日志目录的校验只有**一份实现**
  （`ensure_log_dir`）—— 配置层与日志装配层共用，避免改一处漏一处。
- `tpops_kafka_collector/config.py`：`Config` 增 4 个字段
  （`log_pattern` / `log_dir` / `log_max_bytes`（默认 52428800 = 50 MB）/
  `log_retention_days`（默认 3））；取值走 `log_config`；不可用 → `ConfigError`
  → **退出码 2**（`main.py` 既有路径）；`LOG_DIR` 未设置 → `log_dir is None`
  → 只写 stderr、不落盘。
- `main.py`：`setup_logging_from_config()` 也包了 `LogSetupError` → 退出码 2
  （配置校验已经拦过一次；这里防的是「校验之后目录又变得不可用」）。
- `.env.example`：补 4 个变量（30 → 34 个），每个带注释与「改错的后果」。
- `docs/交付-部署运行说明.md`：环境变量表补 4 行（该表被 `tests/unit/test_delivery_docs.py`
  守着，必须与 `config.py` 实际读取的变量**完全一致**）。

**RED 证据**（`tests/unit/test_config.py` 追加 18 个用例）

```
18 failed, 51 passed
```

**GREEN 证据**：`69 passed`。

### 2.5 通道级每轮统计

**做了什么**

- `tpops_kafka_collector/scheduler_types.py`：新增不可变 DTO `ChannelRoundStats`
  （11 个字段，字段名与汇总行**逐字一致**）。
- `tpops_kafka_collector/channel_stats.py`（新增，121 行）：
  `TaskTimer`（给执行器套计时）与 `aggregate_channel_stats`（按通道归集）。
- `tpops_kafka_collector/scheduler_delivery.py`：`MessageDelivery.deliver_all()`
  一次返回「合计 + **按通道** + 失败清单」（`scheduler.py` 的 `_deliver_all` 随之删除 ——
  `scheduler.py` 已经 392 行，聚合逻辑不许堆进去）。
- `tpops_kafka_collector/scheduler.py`：接上计时器、按通道聚合。

**RED 证据**（`tests/unit/test_channel_round_stats.py`，新增）

> 这个任务的测试同样是补写在实现之后的；RED 状态用「把 `channel_stats.py` 与
> `scheduler_types.ChannelRoundStats` 摘掉」复现：15 个用例里 8 个红
> （`ImportError` / `AttributeError`）。

**GREEN 证据**：`15 passed`。

### 2.6 通道级汇总行输出

**做了什么**：`scheduler_report.py` 新增 `log_channel_summaries()` —— 同一行打**两次**：

- `LOGGER.bind(channel=…)` → 进**该通道日志**；
- `LOGGER.bind(logger_name='collector.summary')` → 进**总日志**（与 stderr 同步）。

两行字段完全相同、且是**纯 `k=v` token**（值里不含空格）→ JSON 模式能摊平成顶层字段。
`log_round_summary()` 增加第 4 个可选参数 `channel_stats`（既有 3 参调用不受影响）。

### 2.7 敏感信息不进落盘日志

**做了什么**：`tests/integration/test_logs_have_no_secrets.py`（新增）—— 跑一遍真实的
认证流程与命令中心取数（TPOPS / Kafka 全 mock），把日志落到临时目录，**扫文件内容**确认：
平台口令、mini-token、SQL 全文都不在里面；并断言日志文件非空（防「空断言恒绿」）。

**RED 证据**：该文件是**先写测试再实现**的（RED 阶段 `FakeSession` 缺 `request` 方法、
`collect()` 签名不对 —— 2 个用例红）；实现（改测试夹具）后转绿。

**GREEN 证据**：`3 passed`。

---

## 2. 顺带收口的三件（wave-1 遗留）

1. **`CLAUDE.md`**：「用 `logging`，不用 `print`」→「用 `loguru` 的 `logger`，不用 `print`」；
   「运行时依赖只有 requests / kafka-python / urllib3 三个纯 Python 包」→ 加 loguru（共四个）；
   「98 可提供 / 3 丢弃」→「**87 可提供**」（wave-1 口径：101 − 11 重复英文变体 − 3 无法提供 = 87），
   涉及 5 处（指标规则注释、日志约定、运行时依赖那句话、依赖锁版本块、
   可提供性判定段与文档索引表）。
2. **`docs/问题单.md`**：TPOPS-001 与 TPOPS-003 **勾选为已关闭**并注明解决方式
   （TPOPS-001：wave-1 把表级维度改成 `schema.table`；TPOPS-003：保留中文名正常的一套，
   11 条重复的进 `excluded_metrics.json`），汇总表状态、详情小节标题、待办清单三处同步。
3. **`requirements-dev.txt`**：钉 `ruff==0.4.10`（见 §6）。

---

## 3. 40 个模块迁移**是不是**只改了 import 与 logger 创建行？

**不是。** 逐条说明改法：

| 改法 | 处数 | 说明 |
|---|---|---|
| ① `import logging` → `from loguru import logger` | 39 | import **位置**保持与 HEAD 完全一致（第三方块优先，无第三方块的放 stdlib 块末尾）→ **每个文件的总行数与 HEAD 一致**（+0） |
| ② `LOGGER = logging.getLogger(__name__)` → `LOGGER = logger` | 34 | 非采集器模块 |
| ③ 同上 → `LOGGER = logger.bind(channel="…")` | 6 | `collectors/monitor.py`→monitor、`command_center.py`→command_center、`command_center_assembly.py`→command_center、`diagnostic.py`→diagnostic、`instance.py`→instance、`alarm.py`→alarm |
| ④ **调用点的占位符：printf → `str.format`** | **111** | 见下 |
| ⑤ 其它 | 0 | 调用点的**方法名、参数顺序、级别**都没动 |

**④ 为什么必须改（这是与简报「调用点 `LOGGER.info(...)` 本身不动」的唯一偏离）**

loguru 的消息格式化是 **`str.format` 语义（`{}`），不是 printf（`%s`）**。实测：

```python
>>> logger.info("值是 %s", "X")
值是 %s          # ← 参数被静默丢弃，不报错、不抛异常
```

也就是说，如果照简报只改 import 与 logger 创建行，**111 处日志的插值会全部消失**
（`%s`/`%d`/`%r`/`%.1f`/`%.3f`），而且**没有任何报错** —— 这种「值凭空消失」的故障
在生产上极难发现。因此这一波把占位符机械转换了：

- `%s` → `{}`、`%d` → `{}`、`%r` → `{!r}`、`%.1f` → `{:.1f}`、`%.3f` → `{:.3f}`；
- **参数列表原样不动**（顺序、个数都不变）；
- 转换后做了两道机器校验（`tests/unit/test_loguru_migration.py`）：
  ① 任何日志调用点里不得再出现 printf 占位符；
  ② **占位符个数必须等于参数个数**（否则 loguru 会抛异常或丢值）。

`%` → `{}` 的转换是**逐个调用点**做的（tokenize 定位字面量 + AST 校验），
没有做全局正则替换；f-string 的调用点本来就与 loguru 兼容，未动。

---

## 4. 既有测试的期望值改动（逐条）

按用户要求：**没有删任何测试、没有放宽任何断言**；只改了「口径确实变了」的期望值。

| # | 文件 / 用例 | 原期望 | 新期望 | 为什么必须改 |
|---|---|---|---|---|
| 1 | `tests/unit/test_logging_setup.py::test_text_format_is_time_level_module_message` | text 格式是 4 段（`时间｜级别｜模块｜消息`，自研 logback pattern） | text 格式是 **loguru 原生形态**：3 段（`时间｜级别｜模块:函数:行号 - 消息`），断言级别单独一段、末段以消息结尾、含模块名 | 规格：「text 模式 SHALL 直接使用 loguru 的原生格式，SHALL NOT 自研 logback pattern 解析器」。断言仍逐项检查（级别/模块/消息），不是放宽 |
| 2 | `tests/unit/test_logging_setup.py::test_output_goes_to_stderr_by_default` | 断言 root 上那个「自有 handler」有 `.stream` 属性 | 断言**输出真的落到了 stderr**（`capsys`）+ 仍断言 root 上只有一个自有 handler | root handler 现在是**转发器**（`InterceptHandler`，把 stdlib 日志送进 loguru），写 stderr 的是 loguru 的 sink。断言从「属性存在」升级为「行为发生」 |
| 3 | `tests/unit/test_serializer.py::_ALLOWED_JSON_WRITERS` | `logging_setup.py`（JSON 编码） | `logging_formats.py`（JSON 编码） | JSON 的 `json.dumps` 随格式函数一起搬到了 `logging_formats.py`。白名单**跟着搬家**，没有放宽（该测试还守着「白名单里的文件必须真的在用」） |
| 4 | `tests/unit/test_delivery_docs.py::EXPECTED_ENV_VAR_COUNT` | 30 | 34 | wave-2 新增 4 个日志环境变量 |
| 5 | `tests/unit/test_delivery_docs.py::config_env_vars()` | 只扫 `config.py` | 扫 `config.py` **+ `log_config.py`** | 日志配置的解析拆到了 `log_config.py`（`config.py` 贴着 400 行红线）。**扫描范围变大 → 判据更严**，不是放宽 |
| 6 | `tests/unit/test_delivery_docs.py::config_env_defaults()` | 只认 `PLATFORM_LIMIT_MAX` | 另认 `DEFAULT_LOG_MAX_BYTES` / `DEFAULT_LOG_RETENTION_DAYS` | 新增的两个具名常量 → 部署说明与 `.env.example` 的默认值**也被核对**（判据更严） |
| 7 | `tests/test_deploy_bundle.py::test_runtime_requirements_declares_exactly_the_runtime_deps` | 3 个运行时依赖 | 4 个（含 loguru） | 口径变化 |
| 8 | `tests/test_deploy_bundle.py::test_assert_arch_independent_accepts_pure_wheels` | 4 个假 wheel | 5 个（加了 loguru 的假 wheel） | 口径变化 |

---

## 5. 需要 reviewer 特别拍板的判断（偏离 / 取舍）

### 5.1 stderr sink 的内容 = 与总日志同内容（按简报与设计决策 2 的表格）

简报的设计要点写死「stderr sink 收**与总日志同内容**」，规格又写死「总日志 SHALL NOT
出现通道内部明细」。两者叠加的后果：**开发态（`LOG_DIR` 未设置）时，通道的 INFO 明细
在 stderr 上也看不到**（只看到生命周期、汇总与 WARNING+）。这是规格的直接后果，
我按简报实现；若希望前台能看到全部明细，把 stderr sink 的 `filter` 去掉即可
（`logging_setup.setup_logging` 里一行）。

### 5.2 第三方库降噪放在 stdlib 侧，**没有**用 `logger.disable("kafka")`

简报建议用 `logger.disable("kafka")` 压噪，但 loguru 的 `disable` 会把它们的 **WARNING
一起丢掉** —— 与规格场景「kafka 客户端输出 WARNING 级日志 → 该记录经 InterceptHandler
进入 loguru 的 sink」**直接冲突**。因此保留既有的 `NOISY_LOGGERS`（stdlib 侧压到
WARNING：kafka / kafka.conn / kafka.cluster / urllib3 / requests），WARNING 仍能进 loguru ✓。

### 5.3 `LOG_FORMAT` 一身两职（`text`/`json` **或** loguru 原生格式串）

规格同时要求「`LOG_FORMAT` 选择 text / json」与「格式 SHALL 可用 `LOG_FORMAT` 覆盖
（loguru 原生字符串语法）」。实现：`LOG_FORMAT=text|json` 走内置格式；
`LOG_FORMAT` 是个含 `{` 的字符串 → 视为**格式串**（模式退回 text）；
既不是内置值又**不含 `{`** → 拒绝（既有的「`LOG_FORMAT=xml` 必须报错」仍然成立 ✓）。
另加 `LOG_PATTERN` 作为显式等价写法（`tasks.md` 3.2 提到这个变量名），**`LOG_PATTERN` 优先**。

### 5.4 「到期且完成的通道」= 本轮**至少有一个实例跑成**的通道

完全没跑成的通道**不出**汇总行（它的失败由整体行体现）；未到期的通道当然也不出。
否则一行 `mapped=0 produced=0 failed=0 delivery_rate=1.000` 会把「采集全失败」显示成「健康」。

### 5.5 `calls` 的自洽性：整体行 ≥ 各通道之和

规格要求「各通道之和 == 整体行同名字段」。`fetched`/`mapped`/`produced`/`failed`/`windows`
**严格相等** ✓（有测试守着）。`calls` 是唯一的例外：整体行的 `calls` 取
`max(任务自报, 客户端真实调用数)`（失败任务的调用也算进去，这是 wave-3 修复轮的口径），
因此整体行可能**大于**各通道之和 —— 差值正是失败任务消耗掉的调用。已在
`channel_stats.py` 的模块 docstring 里写明。

### 5.6 `elapsed` = 该通道**最早开始 → 最晚结束**的墙钟

各通道的任务是并发跑的，所以通道耗时**不是各任务耗时之和**。计时由
`channel_stats.TaskTimer` 给执行器套一层实现（`scheduler.py` 只负责调用）。

### 5.7 JSON 的 `format=` 自定义函数的两个坑（实测踩出来的）

loguru 会把自定义 `format=` 函数的**返回值再当格式串解析一遍**：

1. 返回的 JSON 里的 `{}` 必须**转义成 `{{}}`**，否则被当成 `{record_key}` 占位符 → 抛
   `ValueError: Failed to format log record`；
2. 行尾的换行要**自己带**（loguru 不会替动态格式补）。

`logging_formats.json_format` 里两处都处理了，并有测试守着（`exception` 字段、堆栈不丢）。

### 5.8 `InterceptHandler` 的 depth 计算比官方 recipe 多一步

loguru 官方文档的 recipe 从 `inspect.currentframe()`（= `emit` 自己那一帧）开始数
`logging.__file__` 的帧 —— 而 `emit` 定义在**我们的**模块里，那个 while 一次都不进，
结果 `{name}` 永远指向 `logging_setup.py`。改成从 `emit` 的**调用帧**开始数，
`{name}` 才指向真正的调用方（第三方库场景下是 `kafka.client` 之类）。

### 5.9 pytest 的 `caplog` 兼容（`tests/conftest.py` 加了桥接）

生产代码全用 loguru，而 pytest 的 `caplog` 只认 stdlib logging —— 不桥接的话，
**15 个用 caplog 的测试文件会全部失效**。桥接做法：装一个 loguru sink，把记录按
**原模块名、级别、消息、异常**转成 stdlib 记录（`caplog.at_level` / `caplog.records` /
`caplog.text` 的语义不变）。防回环：转过去的记录带 `_tpops_from_loguru` 标记，
`InterceptHandler` 见到就跳过。

---

## 6. ruff：钉版本与门槛

- **钉 `ruff==0.4.10`**（`requirements-dev.txt`）：0.17.6 与仓库 `ruff.toml` 的
  `[lint] select`（含 `UP` / `FA102`）配合不佳，且会对 wave-1 文件里那条非法
  `# noqa: WPS433（…）` 告警。0.4.10 下 `[lint]` 段被正确读取 ✓（用
  `UP009` 能被选中来验证），也没有那条告警。
- **门槛达成**：把 HEAD 的工作树导出到 `/tmp/baseline`，同一条 ruff 命令、同一个版本
  跑两遍逐条对比 —— **违规集合完全一致（363 → 363，逐条 diff 为空）**：
  本波次**没有引入任何新的 ruff 违规**（新增文件里我自己把 UP032/UP009/UP004/F401/E501
  都清掉了）。

## 7. 文件行数红线

- 39 个迁移文件：**逐行核对，总行数与 HEAD 完全一致（+0）**（import 位置保持不变，
  `import logging` 换成 `from loguru import logger` 是 1 换 1）。
- `config.py` 因为新增 4 个配置项会超 400 行 → 把日志配置的解析拆到新文件
  `log_config.py` → **395 行** ✓。
- 新文件：`logging_setup.py` 304 行、`logging_formats.py` 160 行、`log_config.py` 119 行、
  `channel_stats.py` 121 行 —— 全部 < 400 ✓。

---

## 8. 改动文件清单

**生产包（新增 4 个）**

```
tpops_kafka_collector/logging_setup.py      （重写：loguru sink 装配，305 行）
tpops_kafka_collector/logging_formats.py    （新增：JSON 格式函数 / 切分 / 保留）
tpops_kafka_collector/log_config.py         （新增：日志配置解析）
tpops_kafka_collector/channel_stats.py       （新增：每通道每轮统计聚合）
```

**生产包（修改 40 个）**：`config.py`、`executors.py`、`health.py`、`lifecycle.py`、
`main.py`、`node_targets.py`、`platform_profile.py`、`retry.py`、`schedule.py`、
`scheduler.py`、`scheduler_delivery.py`、`scheduler_report.py`、`scheduler_signals.py`、
`scheduler_tasks.py`、`scheduler_types.py`、`watermark.py`、`collectors/{alarm,command_center,
command_center_assembly,diagnostic,instance,monitor}.py`、`kafka/{producer,records,serializer}.py`、
`mapping/{loader,masking,rules_context,rules_cross,rules_deltas,rules_series,sqlgen}.py`、
`tpops/{alarms,client,command_center,command_center_nodes,diagnostic,instances,metrics}.py`

**测试（新增 5 个）**：`tests/unit/test_loguru_dependency.py`、`tests/unit/test_logging_files.py`、
`tests/unit/test_loguru_migration.py`、`tests/unit/test_channel_round_stats.py`、
`tests/integration/test_logs_have_no_secrets.py`

**测试（修改 5 个）**：`tests/conftest.py`（caplog 桥接）、`tests/unit/test_logging_setup.py`、
`tests/unit/test_serializer.py`、`tests/unit/test_delivery_docs.py`、`tests/unit/test_config.py`、
`tests/test_deploy_bundle.py`

**其它**：`requirements-runtime.txt`、`requirements-dev.txt`、`.env.example`、`CLAUDE.md`、
`deploy/collector.sh`、`tools/build_deploy_bundle.py`、`docs/问题单.md`、
`docs/交付-部署运行说明.md`

---

## 9. 复现命令与结果

```bash
cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
conda run -n tpops python -m pytest \
    tests/unit/test_loguru_dependency.py tests/unit/test_logging_files.py \
    tests/unit/test_loguru_migration.py tests/unit/test_config.py \
    tests/unit/test_channel_round_stats.py tests/integration/test_logs_have_no_secrets.py -q
# → 全部通过

conda run -n tpops python -m pytest -q
# → 全部通过（基线 1845 条 + 本波次新增 83 条 = 1928 条）

conda run -n tpops python -m pytest tests/test_py37_compat.py -q
# → 178 passed

conda run -n tpops python -m ruff check tpops_kafka_collector tests
# → Found 363 errors.（与 HEAD 基线**逐条一致**，本波次未引入新的违规类别）
```
