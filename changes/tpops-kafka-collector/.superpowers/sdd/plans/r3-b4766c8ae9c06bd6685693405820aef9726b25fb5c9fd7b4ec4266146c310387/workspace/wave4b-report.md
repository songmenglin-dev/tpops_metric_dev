# wave-4 实施报告 W4-B —— 部署说明与环境变量

> 变更：`tpops-kafka-collector` · 波次：wave-4 · 实施方：**W4-B**
> 工作树：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev` · 分支 `tpops_metric_dev`
> **未 commit / 未 add**（由控制器统一提交）

---

## 交付物

| 文件 | 性质 | 规模 | 说明 |
|---|---|---|---|
| `docs/交付-部署运行说明.md` | **新建** | 323 行 / 9 节 | 客户运维照着就能部署运行 |
| `.env.example` | 改写 | 26 → **148 行**，11 → **30 个变量** | 每个变量一段中文注释（用途 / 默认值 / 改错的后果） |
| `docs/开发方案-GaussDB指标与告警采集.md` | 改 | +39 / −21 行 | §4.6.2 周期表 + §4.6.3 档位表 + 6 处「10 条 SQL」漂移 |
| `tpops_kafka_collector/ids.py` | 改（**只 docstring**） | +1/−1 | 第 15 行：映射数据路径指向包内 |
| `tpops_kafka_collector/models.py` | 改（**只 docstring**） | +2/−2 | 第 14 行：同上 |
| `tpops_kafka_collector/mapping/README.md` | 改 | +38/−25 行 | 「还没搬进包内」→ 现状；文件布局、用法示例、路径引用同步 |
| `tests/unit/test_delivery_docs.py` | **追加** | +~215 行（文件末尾） | 8 条守卫用例（与 W4-A 共用同一文件） |

`docs/交付-部署运行说明.md` 的 9 节结构：

```
1. 前置条件      —— Python 3.7.9 / 依赖 / Kafka(PLAINTEXT) / TPOPS 账号权限 / 自签证书 / 磁盘 / 出站网络
2. 部署步骤      —— 只拷 tpops_kafka_collector/ + requirements.txt；conda 3.7.9 环境；.env；systemd 单元
3. 环境变量      —— 30 个变量，按 8 组分表：变量名 | 默认值 | 是否必填 | 说明 | 改错的后果
4. 启动与优雅退出 —— 启动顺序、退出码 0/2/3、SIGTERM→SIGINT 优雅退出五步链
5. 运行期文件    —— STATE_DIR 下的 watermark.json / platform_profile.json；换实例的两种做法；画像核对四类不一致
6. 健康检查      —— /healthz（存活）vs /readyz（就绪）；/readyz 503 的两种情况
7. 日志          —— LOG_LEVEL / LOG_FORMAT、stderr、第三方库降噪
8. 排障          —— 17 条「症状 → 可能原因 → 处理」
9. 附：与其它交付文档的关系
```

---

## 环境变量对齐

`config.py` 实际读取 **30** 个变量；`.env.example` 与部署说明表**各 30** 个，**完全一致、无缺无余**。

```
config.py 读取: 30 | .env.example: 30 | 部署说明表: 30
缺: [] []
```

逐项对照（config.py 默认值 / `.env.example` 取值 / 部署说明表「默认值」列）：

| 变量 | config.py 默认 | .env.example | 部署说明表 | 必填 |
|---|---|---|---|---|
| `TPOPS_USER` | （必填，无默认） | `sml` | —（示例文件里是 `sml`） | **是** |
| `TPOPS_PASSWORD` | （必填，无默认） | **（留空）** | — | **是** |
| `TPOPS_BASE_URL` | `https://172.16.103.193:8002` | 同 | 同 | 否 |
| `TPOPS_VERIFY_TLS` | `False` | `false` | `false` | 否 |
| `TPOPS_REQUEST_TIMEOUT` | `60` | `60` | `60` | 否 |
| `TPOPS_AUTH_FAILURE_COOLDOWN_SECONDS` | `60` | `60` | `60` | 否 |
| `KAFKA_BOOTSTRAP_SERVERS` | （必填，无默认） | **（留空）** | — | **是** |
| `KAFKA_TOPIC_METRICS` | `gaussdb.metrics` | 同 | 同 | 否 |
| `KAFKA_TOPIC_ALARMS` | `gaussdb.alarms` | 同 | 同 | 否 |
| `KAFKA_TOPIC_PARTITIONS` | `4` | `4` | `4` | 否 |
| `KAFKA_COMPRESSION` | `lz4` | `lz4` | `lz4` | 否 |
| `POLL_INTERVAL_SECONDS` | `30` | `30` | `30` | 否 |
| `METRIC_WINDOW_HOURS` | `2` | `2` | `2` | 否 |
| `METRIC_END_LAG_MINUTES` | `5` | `5` | `5` | 否 |
| `CMD_CONCURRENCY` | `2` | `2` | `2` | 否 |
| `CMD_LIMIT` | `1000`（`PLATFORM_LIMIT_MAX`） | `1000` | `1000` | 否 |
| `CMD_PAGE_SIZE` | `100` | `100` | `100` | 否 |
| `CMD_POLL_TIMEOUT` | `60` | `60` | `60` | 否 |
| `CMD_RETRY_ATTEMPTS` | `3` | `3` | `3` | 否 |
| `ALARM_LIMIT` | `100` | `100` | `100` | 否 |
| `ALARM_RETENTION_DAYS` | `30` | `30` | `30` | 否 |
| `SQL_MASKING_ENABLED` | `True` | `true` | `true` | 否 |
| `SQL_TEXT_MAX_LENGTH` | `512` | `512` | `512` | 否 |
| `RETRY_MAX_ATTEMPTS` | `5` | `5` | `5` | 否 |
| `RETRY_BASE_SECONDS` | `1.0` | `1.0` | `1.0` | 否 |
| `STATE_DIR` | `./state` | `./state` | `./state` | 否 |
| `HEALTH_HOST` | `127.0.0.1` | `127.0.0.1` | `127.0.0.1` | 否 |
| `HEALTH_PORT` | `8088` | `8088` | `8088` | 否 |
| `LOG_LEVEL` | `INFO` | `INFO` | `INFO` | 否 |
| `LOG_FORMAT` | `text` | `text` | `text` | 否 |

**缺漏清单：无。** 布尔与数值的书写差异（`False`/`true`、`1.0`）是「同一语义的两种写法」，
不是取值不一致 —— 守卫用统一的归一化函数（`_normalize_default`）比较，`false`≡`False`、
`1.0`≡`1.0`。

**凭据处理**：`TPOPS_PASSWORD=` 与 `KAFKA_BOOTSTRAP_SERVERS=` **留空**；
`TPOPS_USER=sml` 按要求保留；`TPOPS_BASE_URL` 保留了 `config.py` 的既有默认值
（`https://172.16.103.193:8002`，仓库原 `.env.example` 已有，非新增的真实地址）。

---

## SQL 档位表现算

**现算脚本**（从包内 `tpops_kafka_collector/mapping/data/metric_rules.json` 按 `sql_key`
取**最短** `collect_cycle` = 最高需求档，只取 `enabled=true` 且 `status=可提供` 的指标），
并与包内 `merged_sql.json` 的 `refresh_seconds` 对照：

```
sql_key                映射表最短档       实算秒数         merged_sql 覆盖指标数
activity_agg           1 分          60           60       13
activity_running       2 分          120          60        1
activity_top10         5 分          300          300       1
bgwriter               5 分          300          300       8
database               2 分          120          120       2
dbstat_sum             1 分          60           60        1
locks                  1 分          60           60        3
prepared_xacts         1 分          60           60        1
settings               1 分          60           60        4
stat_database          1 分          60           60       26
stat_user_tables       2 分          120          120      22

sql_key 总数: 11
与 merged_sql.json 的 refresh_seconds 不一致的: {'activity_running': (120, 60)}
```

### 与控制器给的表的差异（**已独立验证**）

| sql_key | 控制器给的值 | **我现算的值** | 结论 |
|---|---|---|---|
| `stat_user_tables` | 120 秒 | **120 秒** | 一致 ✓ |
| `bgwriter` | 300 秒 | **300 秒** | 一致 ✓ |
| `database` | 120 秒 | **120 秒** | 一致 ✓ |
| `activity_detail` | 「改成 `activity_top10` / 300 秒」 | **该 sql_key 不存在**；现实是 **11** 个 sql_key，`activity_running`(120s) 与 `activity_top10`(300s) **两个都在** | 控制器给的是「改名」口径；实际是**表少了一条**（原表 10 条，现实 11 条） |

**我按现实写了 11 行**（把 `activity_detail` 一行拆成 `activity_running` + `activity_top10` 两行），
而不是简单改名 —— 否则 `activity_running` 会在文档里凭空消失。

### 通道档位（`compute_channel_plans` 实跑）

```
command_center min= 60 n= 82 tiers= [60, 120, 300, 86400]
diagnostic     min=120 n=  4 tiers= [120, 300]
instance       min=3600 n= 1 tiers= [3600]
monitor        min= 30 n= 11 tiers= [30, 60, 300]

monitor 30 30 11 (30, 60, 300)
command_center 60 60 82 (60, 120, 300, 86400)
diagnostic 120 120 4 (120, 300)
instance 3600 3600 1 (3600,)
alarm 30 30 0 ()
```

`diagnostic` 的 4 条指标：`gaussdb.Top10sql`=**2 分**、其余 3 条=5 分 →
通道周期 **120 秒**（300 秒口径下 Top10sql 欠服务 180 秒），与控制器给的判断一致，已核实。

### 改了开发方案的什么

| 位置 | 原值 | 新值 |
|---|---|---|
| §4.6.2 `monitor` 档位列 | `30 秒 / 60 秒 / 1 分 / 2 分 / 5 分` | `30 秒 / 1 分 / 5 分`（实算 tiers = 30/60/300，**没有 2 分档**） |
| §4.6.2 `command_center` 周期 | `60 秒（SQL-2 为 300 秒）` | `60 秒（stat_user_tables / database 为 120 秒，activity_top10 / bgwriter 为 300 秒）` |
| §4.6.2 `command_center` 档位列 | `60 秒 / 1 分 / 2 分 / 5 分` | `1 分 / 2 分 / 5 分 / 24 小时`（实算含 86400） |
| §4.6.2 `diagnostic` | `300 秒` / `5 分钟档` | **`120 秒`** / `2 分 / 5 分档`（并加了一段说明为什么是 120 秒） |
| §4.6.3 档位表 | 10 行，`activity_detail` 60 秒 | **11 行**（`stat_user_tables`→120、`activity_running`→120、`activity_top10`→300、`bgwriter`→300、`database`→120，其余不变）+「现算来源」说明 |
| §3.3 每 tick 任务组表 | 无 `diagnostic` 行 | 补 `diagnostic` 行（按档位 120 秒） |

**顺带核对的「10 条 SQL」漂移**（现状是 **11 个视图合并成 1 条 SQL**，见 §4.4）：

| 位置 | 原值 | 新值 |
|---|---|---|
| §2 需求分析结论 2 | `必须合并 SQL（50 条 → 10 条）` | `（50 条 → 11 个视图合并成 1 条）` |
| §3.3 周期达成性 | `命令中心 10 条 SQL 并发实测一轮 8~15 秒` | `命令中心 1 条合并 SQL（11 个视图）实测一轮 8~15 秒` |
| §4.2.5 三跳并发模型 | `串行 10 条` / `并发下发 9~10 条` | `串行 11 条` / `并发下发 11 条`（与 §4.4 自己的「11 条串行 = 11 × 5 秒」对齐） |
| §4.7 可靠性设计 | `单条 SQL 失败隔离 \| 10 条 SQL` | `11 条 SQL` |
| §4.8 性能预算 | `B 命令中心（10 条 × 3 跳） \| 30 \| 并发 10` | `B 命令中心（1 条合并 SQL × 3 跳） \| 3 \| 并发 1` |
| §4.8 余量段 | `若实测发现 10 条并发把平台压垮` | `若实测发现并发下发把平台压垮` |
| §8.1 风险 R-01 | `10 条并发 SQL 压垮平台或触发限流` | `并发下发压垮平台或触发限流` |

**没改的一处**：`§0.3 v2.0 相对 v1.0 的变更` 里的
「为什么 82 个指标要合并成 **10 条 SQL**」—— 那是**对 v1.0 文档的历史描述**
（v1.0 当时确实写的是 10 条），改它等于篡改变更记录。见「发现但未修的问题」。

---

## 守卫测试

### 与 W4-A 的冲突规避

`tests/unit/test_delivery_docs.py` 由 **W4-A 先建**。W4-B 的做法：

1. **先 `ls` 确认**文件已出现（W4-A 的 `tools/build_delivery_docs.py` 与三份
   `docs/交付-*.md` 也已落地），再动手；
2. **只在文件末尾追加**（`cat >>`），**未改写 W4-A 的任何一行**——
   W4-A 原文件 341 行 → 现 595 行，中间 341 行逐字未动；
3. 追加块自带独立的常量（`DEPLOY_DOC_PATH` / `ENV_EXAMPLE_PATH` / `CONFIG_PATH` /
   `SECRET_ENV_VARS`）与判据函数，**不改动 W4-A 的任何常量与函数**；
4. 命名带域前缀（`check_deploy_env_table` / `check_env_example_covers` /
   `check_env_example_secrets`），与 W4-A 的 `check_*` 不冲突。

### 追加的 8 条用例

| 用例 | 判据 |
|---|---|
| `test_deploy_doc_env_table_matches_config` | 部署说明的变量表与 `config.py` 读取的变量**完全一致（不多不少）**；并断言变量数 = 30 |
| `test_env_example_covers_every_config_var` | `.env.example` **覆盖** `config.py` 全部变量（可多不可少） |
| `test_env_example_has_no_hardcoded_password` | `.env.example` 里 `TPOPS_PASSWORD=` 后面**必须为空** |
| `test_deploy_doc_defaults_match_config` | 部署说明表「默认值」列 == `config.py` 默认值（并断言默认值解析出 ≥ 25 个，防止判据恒绿） |
| `test_env_example_values_match_config_defaults` | `.env.example` 每个取值 == `config.py` 默认值 |
| `test_guard_detects_env_var_missing_from_deploy_doc` | **防恒绿**：合成「文档漏一个变量」的样本 → 守卫必须报缺失 |
| `test_guard_detects_env_var_missing_from_env_example` | **防恒绿**：合成「.env.example 漏一个变量」→ 守卫必须报缺失 |
| `test_guard_detects_hardcoded_password_in_env_example` | **防恒绿**：合成「.env.example 写死口令」→ 守卫必须抓到 |

判据函数（既用于真实文件，也用于合成样本 —— **判据被放松时必须变红**）：
`config_env_vars()` / `config_env_defaults()` / `parse_deploy_env_table()` /
`check_deploy_env_table()` / `env_example_vars()` / `check_env_example_covers()` /
`check_env_example_secrets()` / `_normalize_default()`。

### 变异验证记录

**变异 1 —— 把判据改松**（`check_deploy_env_table` 不再报「缺失」）：

```
$ python3 - <<'PY'   # 把 return sorted(expected - listed), ... 改成 return [], ...
$ conda run -n tpops python -m pytest -q tests/unit/test_delivery_docs.py
........................F..                                              [100%]
FAILED tests/unit/test_delivery_docs.py::test_guard_detects_env_var_missing_from_deploy_doc
E       AssertionError: 守卫没抓到部署说明里漏写的变量 ALARM_LIMIT
E       assert 'ALARM_LIMIT' in []
1 failed, 26 passed in 0.67s
```
→ **变红**，防恒绿自检有效。（改完后已还原）

**变异 2 —— 真的删掉部署说明里的一行**（删 `SQL_TEXT_MAX_LENGTH` 那一行）：

```
$ conda run -n tpops python -m pytest -q tests/unit/test_delivery_docs.py
FAILED tests/unit/test_delivery_docs.py::test_deploy_doc_env_table_matches_config
FAILED tests/unit/test_delivery_docs.py::test_deploy_doc_defaults_match_config
E       AssertionError: SQL_TEXT_MAX_LENGTH（表里没有这一行或列不全）
2 failed, 25 passed in 0.66s
```
→ **变红**。（已还原文档，`git diff` 复核过）

---

## 实测结果

```
$ cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
$ conda run -n tpops python -m pytest -q tests/unit/test_delivery_docs.py
............................                                             [100%]
28 passed in 0.55s

$ conda run -n tpops python -m pytest -q
........................................................................ [ 91%]
........................................................................ [ 96%]
...................................................................      [100%]
1723 passed in 85.44s (0:01:25)

$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest -q tests/test_py37_compat.py
........................................................................ [ 92%]
............                                                             [100%]
156 passed in 1.37s

$ grep -c "^[A-Z_]*=" .env.example
30
```

变量一致性自查：

```
$ python3 - <<'EOF'
import re
cfg = open('tpops_kafka_collector/config.py', encoding='utf-8').read()
envs = set(re.findall(r"env,\s*'([A-Z][A-Z0-9_]+)'", cfg))
ex = set(re.findall(r"^([A-Z][A-Z0-9_]+)=", open('.env.example', encoding='utf-8').read(), re.M))
print('config.py 读取:', len(envs), '| .env.example:', len(ex), '| 缺:', sorted(envs - ex))
EOF
config.py 读取: 30 | .env.example: 30 | 缺: []
```

> **全量测试的瞬时红灯说明**：一次全量跑里出现
> `test_docs_match_generator_output` 失败（W4-A 的用例 —— 它会**重跑生成器**再比对
> 磁盘上的文档）。这是 W4-A 正在并发改文档/生成器时的**竞态**，不是本波次的回归：
> 该用例**单独跑通过**（`1 passed`），紧接着的全量跑也是 `1723 passed`。

---

## 发现但未修的问题

1. **`activity_running` 的档位在两处不一致（数据文件层面，需 wave-5 或 W4-A 之外的裁决）**
   `metric_rules.json` 里 `gaussdb.running.sql.limit.20` 声明 **2 分档**，
   而 `merged_sql.json` 的 `refresh_seconds` 记 **60 秒**。
   运行时以 `merged_sql.json` 为准（更频，不影响正确性）；
   **§4.6.3 按任务要求以 `metric_rules.json` 现算为准写 120 秒，并在表下加了 ⚠ 显式标注**。
   这两处数据文件都不在我的文件归属内，未改。

2. **`CMD_LIMIT` 与 `RETRY_MAX_ATTEMPTS` 是「读了但没人用」的配置**
   `grep -rn "cmd_limit\|retry_max_attempts" tpops_kafka_collector/ --include=*.py`
   只命中 `config.py` 自己 —— 运行时的重试次数是各自写死的
   （命令中心用 `CMD_RETRY_ATTEMPTS`，节点清单用 `RetryPolicy(max_attempts=3)`）。
   已在 `.env.example` 与部署说明表里**如实标注**（「尚未被消费，改动不影响当前运行行为」），
   未改代码。

3. **§0.3「v2.0 相对 v1.0 的变更」里的「合并成 10 条 SQL」是 v1.0 的历史描述**
   现状是 11 个视图合并成 1 条。改它等于篡改变更记录，**未改**；
   如需统一，建议写成「（v1.0 当时写的是 10 条）」而不是直接改数字。

4. **部署说明里 `TPOPS_BASE_URL` 保留了真实内网地址**
   它就是 `config.py` 的默认值，仓库原 `.env.example` 已有；任务要求「默认值与
   config.py 一致」且「不要在示例文件里写真实地址」指的是凭据类
   （`TPOPS_PASSWORD` / `KAFKA_BOOTSTRAP_SERVERS`，已留空）。若客户要求示例文件
   完全不含内网地址，需同步改 `config.py` 的默认值（**超出本波次归属**）。

5. **`docs/交付-部署运行说明.md` 的 §9 引用了三份 W4-A 的交付文档**
   若 W4-A 最终改了这三份文档的**文件名**，§9 的引用需要同步（W4-A 侧收尾时核对）。

6. **`tests/unit/test_delivery_docs.py` 的 ruff 现状**
   W4-A 的原文件已有 40 条 ruff 告警（`UP031` printf 格式化等，与仓库既有测试风格一致）；
   W4-B 追加后 41 条（W4-B 只贡献 1 条同风格的 `UP031`）。
   验收口径的 ruff 只跑 `tpops_kafka_collector/`（`All checks passed!`），未动测试风格。

---

## 偏离说明

| 项 | 计划 | 实际 | 理由 |
|---|---|---|---|
| §4.6.3 档位表 | 控制器给的是「`activity_detail` → `activity_top10` / 300 秒」 | 改成 **11 行**（`activity_running` + `activity_top10` 两行） | 现算证明 `activity_detail` 不存在，现实有 11 个 sql_key；改名会丢掉 `activity_running` |
| `activity_running` 档位 | 控制器未提 | 写 **120 秒**（metric_rules 现算）+ ⚠ 标注与 `merged_sql.json` 的 60 秒不一致 | 任务明确要求「以 metric_rules.json 现算为准」；不一致必须显式暴露而不是抹平 |
| §4.6.2 `monitor` 档位 | 未提 | 一并改成 `30 秒 / 1 分 / 5 分` | 原值里的「2 分档」在实算 tiers 里不存在，属同一处漂移 |
| 顺带核对范围 | 「11 条合并 SQL 之类与现状不符的说法」 | 改了 7 处「10 条 SQL」+ 补 1 行 `diagnostic` | 都是**数量与现状不符**的同一类漂移；未做措辞层面的润色 |
| §0.3 的 10 条 | — | **未改** | 属 v1.0 历史描述，不是现状陈述 |
| 报告落点 | `changes/.../workspace/wave4b-report.md` | 同 | 铁律里的「不改 changes/**」指的是规划工件（contract / tasks / specs），本报告是任务明确指定的唯一产出物 |
| commit | — | **未 commit / 未 add** | 按要求由控制器统一提交 |

**未触碰的文件**：`execution-contract.md`、`tasks.md`、`specs/**`、`data/**`、
`changes/**`（除本报告）、W4-A 的三份 `docs/交付-*.md` 与 `tools/build_delivery_docs.py`、
以及 `tpops_kafka_collector/` 下的任何**行为代码**（只改了两个 docstring）。

```
$ git status --short
 M .env.example
 M docs/开发方案-GaussDB指标与告警采集.md
 M tpops_kafka_collector/ids.py
 M tpops_kafka_collector/mapping/README.md
 M tpops_kafka_collector/models.py
?? docs/交付-指标口径说明.md              ← W4-A
?? docs/交付-客户指标英文名核对清单.md      ← W4-A
?? docs/交付-不可提供指标清单.md            ← W4-A
?? docs/交付-部署运行说明.md               ← W4-B（本次新建）
?? tests/unit/test_delivery_docs.py       ← W4-A 建、W4-B 末尾追加
?? tools/build_delivery_docs.py           ← W4-A
```
