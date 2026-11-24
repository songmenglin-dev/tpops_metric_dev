# Wave 4 审查报告 —— wave-4-delivery-docs

- **base** `15a076b` / **head** `2650399`
- **结论**：verdict = **fail**
- **计数**：**Critical 1 / Important 8 / Minor 9**

> 审查方式：**不采信实施方报告的任何数字与结论**。全部结论由本审查方自己跑命令、
> 自己读实现代码得出；本报告内每条结论都附真实命令输出片段。
> 变异验证在 `/tmp/w4rev-mut` 副本里做（工作树只写入本报告一个文件），收尾已删除副本。

---

## 合同完成标准

### 标准 1：`pytest tests/unit/test_delivery_docs.py -q` 通过 —— ✅ 满足

```
$ cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
$ conda run -n tpops python -m pytest tests/unit/test_delivery_docs.py -q
.............................                                            [100%]
29 passed in 0.57s
```

全量回归（`/tmp/w4rev-mut` 副本，含 `.env.example` 与客户 xlsx）：

```
$ conda run -n tpops python -m pytest -q
1723 passed in 61.84s (0:01:01)
```

> 注：按任务书给的拷贝清单只拷 `tpops_kafka_collector tests tools docs pytest.ini data` 时，
> 会因缺 `.env.example` 与客户 xlsx 而出现 **11 failed**（6 个 collection error + 5 个断言失败）——
> 那是拷贝产物不是缺陷。补上这两个文件后全绿。

其他门禁：

```
$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!                     # 合同门禁：ruff check tpops_kafka_collector/ → 0

$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
156 passed in 1.34s

$ conda run -n tpops python tools/build_delivery_docs.py --check
一致：docs/交付-不可提供指标清单.md
一致：docs/交付-客户指标英文名核对清单.md
一致：docs/交付-指标口径说明.md
```

py37 语法红线（`ast.parse(feature_version=(3,7))` 全量扫 `tpops_kafka_collector/` + 本波两个新文件）：
**0 失败**。

> `ruff check .`（整仓）= 714 errors、`ruff check tools/build_delivery_docs.py tests/unit/test_delivery_docs.py`
> = 146 errors —— 合同第 59 行明示「仓库现状 `ruff check .` 有历史 lint 错误」，门禁只针对
> `tpops_kafka_collector/`，**该门禁通过**。但本波新增的 `tools/build_delivery_docs.py`
> 自带 **2 条 F841（未使用变量）**，见 Minor M-1。

### 标准 2：98 个指标各有口径说明；3 个不可提供指标各有书面理由 —— ✅ 满足（结构层面）

```
$ conda run -n tpops python -c "import json;d=json.load(open('tpops_kafka_collector/mapping/data/metric_rules.json'));print(d['counts'])"
{'total': 101, 'enabled': 98, 'disabled': 3, 'needs_confirmation': 5}
```

口径说明文档表格行数 = 98（逐条核对 `collect_cycle` / `value_rule` / `placeholders` /
`dimension_bindings`，**98/98 与真相源一致，零例外**）；不可提供清单 = 3 条，
每条都有「为什么给不了」（引用映射表的 `unsupported_reason`）+「能不能变通」。

> ⚠️ 但「98 个指标**各有**口径说明」在**内容正确性**上不成立 —— 见 Critical C-1。
> 覆盖度达标 ≠ 口径正确。

### 标准 3：环境变量表与 `config.py` 实际读取的变量一致 —— ✅ 满足

```
$ grep -oE "env, '[A-Z_]+'" tpops_kafka_collector/config.py | grep -oE "'[A-Z_]+'" | tr -d "'" | sort -u  > /tmp/cfg_vars.txt
$ grep -E '^\| `[A-Z][A-Z0-9_]+` \|' docs/交付-部署运行说明.md | grep -oE '^\| `[A-Z][A-Z0-9_]+`' | tr -d '|` ' | sort -u > /tmp/doc_vars.txt
$ diff /tmp/cfg_vars.txt /tmp/doc_vars.txt
(no diff)                    # 30 == 30，集合完全一致
```

30 个变量的**默认值 30/30 全部一致**（以「只给 3 项必填、其余留空」实跑 `load_config()` 比对）；
必填项恰好 3 项（`_require`/`_servers`：`TPOPS_USER` / `TPOPS_PASSWORD` / `KAFKA_BOOTSTRAP_SERVERS`），
与文档「必填」列逐项一致。

---

## 口径说明的事实准确性抽验

抽验 16 条，覆盖 4 个通道 + 全部 13 种取值规则类型。**「文档说的」一栏直接摘自语料文档，
「实现做的」一栏全部由本审查方读实现代码 + 实跑得出。**

| # | 指标 | 文档说的 | 实现做的 | 一致？ |
|---|---|---|---|---|
| 1 | `gaussdb.Xact_commit` | 取列 `xact_commit`，对该视图所有行求和（**累计值**）；备注空 | `value_rule={type:raw, params:{column:xact_commit, row_policy:sum}}` → `rules_row.py` 按 `sum` 聚合 `stat_database` 视图全部行。**不差分**，与文档一致 | ✅ |
| 2 | `GAUSSDB.[{#DATNAME}]_fetch_records_per_second` | 「累计值 `tup_fetched` **跨轮差分算速率**：本次值减上轮值再除以实际采集间隔秒数；首轮只建基线不产出，实例重启导致计数回退时本轮丢弃并记错误日志」 | `value_rule={type:counter_delta, column:tup_fetched, row_policy:per_row, group_by:[datname]}` → `rules_cross.py:21 eval_counter_delta` → `rules_deltas.py:53 rate()`：**首轮返回 (None,'首轮无基线，仅建立基线')**；回退走 `counter_value < last_value` → `LOGGER.error` + 重建基线 + 返回 None；间隔用 `(now_ms - last_ms)/1000.0`（**实际采集时刻之差**，非标称周期）。实跑验证：<br>`cn1: (None, '首轮无基线，仅建立基线')` / `cn2: (None, ...)` / 2 轮后 `cn1: (1.0,'按采集时刻差分')` `cn2: (16.667,...)`（**按节点隔离，互不覆盖**）/ 回退时 `cn1: (None, '累计值回退，判定实例重启，丢弃本轮差分并重建基线')` | ✅ |
| 3 | `GAUSSDB.[{#DATNAME}]_fetch_records_per_second` 的**基线按节点隔离**（§3.6） | 「基线键是『指标名 + 维度 + 节点』三元组」 | `rules_deltas.py:35 scoped_dimension_key(scope, key)` → 键仍是**二元组** `(metric_code, 'node:dimkey')`，作用域并进维度键。**语义等价**（按节点隔离已实现），但文档说「三元组」与实现的「二元组 + scope 前缀」字面不符 —— 文档第 95 行还引用了 `rules_deltas.py` 的 docstring「基线键不含节点」这一**已被推翻的旧描述** | ⚠️ 措辞不准（Minor M-4） |
| 4 | `gaussdb.Tps`（A 类） | 取值规则「取时序上最后一个非空点（平台按组件给多条 series，**每个组件各出一条**）」；但**归集口径列写「节点级：每个 master 各出一条」** | `rules_series.py:14 eval_series_point` 对 `ctx.series` **每个 key 出一条**；`monitor.py` `_resource_context` 按 `COMPONENT_ID` 换算节点身份。实测（跑 `data/metric_response/030_gaussdb.Tps.json`）：`series=3 → 消息条数=3`，维度键是 `COMPONENT_ID`（`cn_5001/cn_5002/cn_5003`），**不是 master 维度** | ❌ **归集口径列与实现不符**（见 C-1） |
| 5 | `GAUSSDB.Standby_status`（A 类） | 归集口径「节点级：**每个 master 各出一条**」 | 实测（`066_GAUSSDB.Standby_status.json`）：`series=9 → 消息条数=9`，维度键是 9 个 **DN**（`172.16.103.126-dn_6005` 等）。**既不是 3 个 master，也不是每个 master 一条** | ❌ **严重不符**（见 C-1） |
| 6 | `gaussdb.QPS`（A 类） | 归集口径「节点级：每个 master 各出一条」 | 实测（`031_gaussdb.QPS.json`）：`series=6 → 消息条数=6`（3 个 CN + 3 个 DN）。**6 条 ≠ 每个 master 一条** | ❌ 不符（C-1） |
| 7 | `GAUSSDB.Service_availability_testing` | 「时序点的值按枚举表翻译（0→异常、1→正常）；未命中时原样透传并记告警日志」 | `value_rule={type:enum_map, mapping:{1:正常,0:异常}, default:passthrough}` → `rules_series.py:32 eval_enum_map`：命中→翻译；未命中且 `passthrough` → `LOGGER.warning('枚举未命中，原样透传：…')` + 原样透传。**逐字一致** | ✅ |
| 8 | `gaussdb.Uptimestr` | 「由平台给的启动时刻推算已运行时长（当前时刻减启动时刻，单位 秒）」 | `value_rule={type:gauge, params:{column:rds133_cn_startup_time, unit:秒}}` → `rules_series.py:57 eval_gauge`：`elapsed = now_seconds - float(point)`，`round(elapsed, 2)`；单位非「秒/s」直接 `RuleError` | ✅ |
| 9 | `gaussdb.Total_pct` | 「activity_agg 视图的 `cnt` ÷ stat_database 视图的 `numbackends`，分母取不到时本轮不产出」；备注「活跃会话数 / 总连接数」 | `value_rule={type:ratio_across, numerator:{sql_key:activity_agg, fn:sum, value_column:cnt}, denominator:{sql_key:stat_database, fn:sum, value_column:numbackends}}` → `rules_cross.py:47`：分母 falsy → `LOGGER.warning` + 返回 `[]`（**不产出假数据**）；分子 0 → 产出 `0.0`；`round(x,2)`。**但备注文案不准**：分子是 activity_agg 的**全部会话数**（`pg_stat_activity` 按 `datname,state` 分组的 cnt 之和，**含 idle**），不是「活跃会话数」—— 映射表自己的 `value_rule.note` 写的是「分子=activity_agg 全部会话数」，两条互相打架 | ⚠️ 备注错（Important I-3） |
| 10 | `gaussdb.Lockwaiting` | 「按 `{"state_contains": ["lock"]}` 过滤后对 `cnt` 求和，整体一条」；刷新周期「60 秒」 | `value_rule={type:aggregate, params:{filter:{state_contains:[lock]}, group_by:[], fn:sum, value_column:cnt}}` → `rules_context.grouped_items` + `sum_values`。与文档一致 | ✅ |
| 11 | `gaussdb.running.sql.limit.20` | 「按 `query_start` desc 排序取前 20 条，只保留 `datname`/`usename`/`state`/`query_start`/`query`」 | `value_rule={type:top_n, params:{sort_by:query_start, order:desc, limit:20, columns:[datname,usename,state,query_start,query]}}` → `rules_sorting.py`。一致 | ✅ |
| 12 | `gaussdb.Top10sql`（C 类） | 「按 `total_elapse_time` desc 排序取前 10 条，数据来自 `top_sql_infos` 集合」；刷新周期「2 分钟」 | `value_rule={type:top_n, params:{collection:top_sql_infos, sort_by:total_elapse_time, order:desc, limit:10}}`；`collect_cycle={value:2, unit:分}` = 120 秒 | ✅ |
| 13 | `GAUSSDB.Total_slow_queries` | 「统计 `slow_sql_infos` 集合的条数」 | `value_rule={type:collection_count, params:{collection:slow_sql_infos}}`；采集走 `POST /rds/gaussdb/v3/instances/{id}/slow-sql-list` | ✅ |
| 14 | `gaussdb.server_version`（D 类） | 「取实例元数据里的 `engine_version` 字段」；刷新周期「1 小时」 | `value_rule={type:instance_field, params:{field:engine_version}}`；`collect_cycle={value:1, unit:时}` = 3600 秒 | ✅ |
| 15 | `gaussdb.{#NAME}.Number.lete.ples` | 「取列 `n_tup_del`，一行一条，按 `relname` 分维度展开」；维度 `{#NAME}` | `value_rule={type:raw, params:{column:n_tup_del, row_policy:per_row, group_by:[relname]}}`；`dimension_bindings={NAME: relname}`。占位符一致性全量核：98 条规则的 `placeholders` / `dimension_bindings` / `metric_code` 里的 `{#X}` / `metric_name` 里的 `{#X}` **四者完全一致，0 处不符** | ✅ |
| 16 | `gaussdb.get.connections.sum` / `gaussdb.get.dbstat.sum` / `gaussdb.get.lock.info`（明细型） | 「把该视图的整行列表原样带出（明细型指标）」 | `value_rule.type=raw_rows`（3 条）；`message.py:serialize_value` 对 list 直接 `list(value)` 原样带出 | ✅ |

### 抽验小结

- **口径正确的 13 条**：速率类差分 ✅、A 类 gauge/enum_map ✅、B 类 raw/aggregate/top_n/ratio_across ✅、
  C 类 top_n/collection_count ✅、D 类 instance_field ✅、明细型 ✅。
- **口径错误 3 条**（全在 A 类 `node_scoped=true` 的 8 条指标上）：**C-1**。
- **备注文案错误 1 条**：`gaussdb.Total_pct`（I-3）。
- **措辞不准 1 条**：基线键「三元组」（M-4）。

### 刷新周期 vs 实际调度节奏 —— ⚠️ Important I-1（全局）

**实算（`schedule.py` 真相源，本审查方自己跑）：**

```
$ conda run -n tpops python /tmp/pollcheck.py
monitor interval= 30 tiers= (30, 60, 300) metric_count= 11
command_center interval= 60 tiers= (60, 120, 300, 86400) metric_count= 82
diagnostic interval= 120 tiers= (120, 300) metric_count= 4
instance interval= 3600 tiers= (3600,) metric_count= 1
alarm interval= 30 tiers= () metric_count= 0
```

口径说明第 21 行写「刷新周期 | 该指标的采集周期，**与客户指标库「数据更新周期」一致**」，
第 268 行写「采集频率 | 以客户指标库「数据更新周期」为准（30 秒 ~ 24 小时共 7 档）」。

**但实现里没有任何按指标 `collect_cycle` 的产出闸门**：

```
$ grep -rn "collect_cycle" tpops_kafka_collector/ --include=*.py
  schedule.py:156   tier = collect_cycle_seconds(...)      # 只用于算「通道」周期
  collectors/instance.py:214-219                          # 只用于算「通道」周期
  mapping/table.py:77                                     # 只做数据装载
  （无任何一处按 rule.collect_cycle 做「这条指标这轮该不该产出」的判定）

$ grep -rn "rules=" tpops_kafka_collector/executors.py
  （四个采集器调用点都不传 rules 子集 → 每轮产出全部规则）
```

`collectors/command_center.py:161` 的 docstring 写「`None` 表示全部 B 类规则（**调度器按档位传子集**）」
—— **调度器并没有传子集**，docstring 与实现不符。

**后果**：65/98 条指标的**声明周期慢于通道实际节奏**，消息量比客户预期高 2.5~1440 倍：

| 声明周期 | 通道实际节奏 | 条数 | 倍数 |
|---|---|---|---|
| 24 小时（2 条，如 `gaussdb.Serverencoding`） | 每 60 秒 | 2 | **1440×** |
| 5 分钟（B 类 35 条 + A 类 7 条 + C 类 3 条） | 每 60 秒 / 30 秒 / 120 秒 | 45 | 2~5× |
| 2 分钟（B 类 15 条） | 每 60 秒 | 15 | 2× |
| 1 分钟（A 类 3 条） | 每 30 秒 | 3 | 2× |

客户按「刷新周期」列配置保留策略 / 告警阈值，会得到比预期高一个数量级的消息流。
**文档没有一处说明这个区别**（`grep -n "通道\|实际\|档位\|超服务\|提频" docs/交付-指标口径说明.md` 只命中
第 3、18、21、29、31 行的表头文字）。

---

## 交付承诺的兑现核实

### 不可提供清单（「能不能变通」是否真的做不到）

| 指标 | 文档「为什么给不了」 | 实现上真的做不到？ |
|---|---|---|
| `gaussdb.Fastpath_function_call` | 「PG 侧无 fastpath 计数器；pg_settings 仅有 gsi_fastpath_level 开关」 | ✅ 属实。合并 SQL 的 `settings` 视图只取 `('max_connections','port','TimeZone','block_size')` 四项（实测 `data/merged_sql/sql_merge.sql`），无 fastpath 计数器；`gsi_fastpath_level` 是配置开关不是计数。**不能直接提供**成立 |
| `gaussdb.Disabled_conn` | 「PG 无『禁用连接』概念；pg_settings 仅有 reserved_connections 等预留连接」 | ✅ 属实。PG 无「禁用连接」计数；`reserved_connections` 是配置项。文档的变通方案 `max_connections - numbackends - reserved_connections` 需要 `reserved_connections` 落到 SQL（当前 `settings` 视图**没取**这一项）—— 文档说「口径需要客户书面确认后才提供」，**表述正确** |
| `gaussdb.sendAgentName` | 「Agent 侧行为，不是数据库可查询的监控指标」 | ✅ 属实。数据库侧无此概念；文档给的变通（节点名进 `tags`）在 `message.py:130 _metric_tags` 里确实有 `node_id`/`instance_name` |

**结论：3 条理由均属实，「能不能变通」的方案与实际能力边界吻合，未发现夸大。** ✅

### 部署运行说明逐条核实

| 文档承诺 | 核实结果 |
|---|---|
| 优雅退出：置位 shutdown → 在途最多等 30 秒 → 原子写水位 → flush/关闭 producer → 退出码 0 | ✅ `lifecycle.py:63 SHUTDOWN_DEADLINE_SECONDS = 30`；`executors.py:152 _guard()` 停机后不发请求；`watermark.py:316-344` 临时文件 + fsync + `os.replace` 原子写；`main.py:333 os._exit(int(code))` |
| 退出走 `os._exit`，跳过 atexit | ✅ `main.py:314-333`；全包无 `sys.exit` 调用 |
| 水位每轮落盘（崩溃最多丢一轮） | ✅ `lifecycle.py:160-170 _persist_watermark` 每轮调 |
| 退出码 0 / 2 / 3 | ✅ `lifecycle.py:72 EXIT_OK=0`、`main.py:90 EXIT_CONFIG_ERROR=2`、`main.py:92 EXIT_STARTUP_ERROR=3`；`health.py:355-360` 端口占用 `OSError` → `HealthServerError` → main 捕获 → 返回 3 |
| `/healthz` = 200 + `{"status":"ok","check":"liveness","timestamp":…}`，不看任何依赖 | ✅ `health.py:203-210` 逐字一致（仅 `sort_keys=True` 导致键序为 check/status/timestamp，无害） |
| `/readyz` = 200/503 + `{"status","check","dependencies":[{name,ready,reason}]}` | ✅ `health.py:212-229` 逐字一致 |
| 两种 503 情形与 reason 文案 | ✅ `lifecycle.py:66 SCHEDULER_STALE_SECONDS = 180`（全包仅此一处 180）；`main.py:272/274/282/284` 四条 reason 逐字一致 |
| 探针只读标志位、不 ping 平台、不连 Kafka；单探针异常不拖垮端点；响应体不含凭据 | ✅ `health.py:149-157 _run_probe` 异常兜底；`health.py:249-263 secret_values_from_config` 兜底脱敏 |
| 状态目录有 `watermark.json`（含水位 + 差分基线 + 幂等键）与 `platform_profile.json` | ✅ `watermark.py:83-105 to_payload` 含 `metric_watermarks / alarm_watermarks / delta_baselines / idempotency_keys`；`platform_profile.py:79-87 to_payload` 含 `instance_id/instance_name/mode/engine_name/engine_version/master_count` |
| 平台画像可手工编辑，改完下一轮启动生效；平台新增实例不会自动加入 | ✅ `platform_profile.py:201-215`（文件存在则以其为准，否则 discover + 固化）；`platform_profile.py:333-349 _scan_added` 只告警不加入；日志文案**逐字**含「平台新增了实例 %s（%s）—— 平台画像里没有记录，需人工更新（删除画像文件可重新发现）」 |
| 启动时画像与平台对账，**只记日志不改文件**，4 种不一致项级别正确 | ✅ `platform_profile.py:293`（实例不存在→ERROR）、`344`（平台新增→WARNING）、`310`（引擎名变→WARNING）、`325`（master 数变→WARNING）；`reconcile()` 全程不写文件 |
| 排障：认证失败会累计 `login_failed_attempts` 并锁号，不要盲目重试；已内置 60 秒静默期 | ✅ `client.py:239-248 _raise_if_in_cooldown()` 静默期内 `raise` 复用旧失败结论、**不发请求**；`client.py:195` 在 `_send` 之前先检查；`client.py:216` 日志文案含「平台会累计 login_failed_attempts 并锁号」 |
| 排障：分组码写错返回 200 + 空 series，靠「空分组告警」发现 | ✅ `tpops/metrics.py:369-375` 文案逐字含「监控指标空分组告警：分组码 %s 未返回任何指标」 |
| 排障：「12 个分组码都没返回指标 —— 本轮 A 类无数据」 | ✅ `collectors/monitor.py:153-158` 逐字一致；`tpops/metrics.py:44-57 METRIC_GROUPS` **恰好 12 项** |
| 排障：历史告警仅保留 30 天，缺口显式告警 | ✅ `collectors/alarm.py:337-342` 文案逐字含「告警历史缺口告警：最新历史告警时间距今 %.1f 天，超过平台 %d 天保留窗口」；保留期来自 `config.py:295 ALARM_RETENTION_DAYS` → `executors.py:339` → `alarm.py:337` |
| 排障：命令中心偶发失败会重试 | ⚠️ **重试确实实现**（`tpops/command_center.py:149 max_attempts=max(1, config.cmd_retry_attempts)`、`retry.py` 指数退避 1→2→4），但文档 §8 引号里的日志「命令中心 …… **重试后成功**」**在代码里不存在**：<br>`$ grep -rn "重试后成功" tpops_kafka_collector/` → 只有 `command_center.py:26` 与 `command_center_result.py:24` 两条**注释**命中，实际日志只有 `retry.py:176`「第 %s/%s 次重试 …」与 `retry.py:170`「重试次数用尽，放弃：…」。**运维 grep 不到**（Important I-4） |
| 排障：命令中心 offset 未生效则停止翻页 | ✅ `collectors/command_center.py:231-239` 文案逐字一致 + `break` |
| 日志：stderr / text 竖线格式 / json 摊平 / 第三方降噪 / 幂等 | ✅ `logging_setup.py:155 StreamHandler(sys.stderr)`、`48 TEXT_FORMAT='%(asctime)s \| %(levelname)s \| %(name)s \| %(message)s'`、`100-111` 摊平、`51-57 NOISY_LOGGERS`、幂等 handler |

**兑现度：部署说明 §4/§5/§6/§7 的承诺基本全部兑现（实测 21 条里 18 条完全兑现）**，
问题集中在 §2 部署步骤与 §3 的「改错的后果」文案（I-2 / I-5 / I-6）。

---

## OCR 清单可信度

客户原件：`高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx`「指标库」sheet
（表头 9 列：`适用资源类型 | 指标分类 | 中文名 | 英文名 | 数据类型 | 数据更新周期 | 采集更新周期单位 | 生成性能基线 | 指标单位`；102 行 = 1 表头 + 101 指标）。

**抽验 5 条「疑似错字」**（字节级核对，全 14 条 + 2 条描述名一并核）：

```
$ conda run -n tpops python /tmp/exact.py
doc TABLE:ocr rows parsed: 14
  #1  xlsx D2   EXACT-MATCH  'GAUSSDB.Number_of_ansactions_mmitted_r_cond'
  #2  xlsx D6   EXACT-MATCH  'GAUSSDB.Number_of_llback_ansactions_r_cond'
  #3  xlsx D18  EXACT-MATCH  'gaussdb.{#NAME}.Number.lete.ples'
  #5  xlsx D25  EXACT-MATCH  'gaussdb.{#NAME}.Number.t.date.ples'
  #8  xlsx D66  EXACT-MATCH  'GAUSSDB.{#NAME}.Number.ad.ples'
  #13 xlsx D82  EXACT-MATCH  'GAUSSDB.Checkpoint_count_eckpoint_gments'
doc TABLE:ocr-desc rows parsed: 2
  #1  xlsx C88  EXACT-MATCH  '[{#DATNAME}] Commited transactions'
  #2  xlsx C75  EXACT-MATCH  '({#NAME})number of sequencial tuples read'
```

**14/14 + 2/2 全部字节级命中 xlsx**（大小写、下划线、点号、`{#NAME}` 花括号完全一致）。
「建议的正确拼写」**没有臆造** —— 组成词全部在 101 个指标名里真实存在，
且「全表另有 N 处」的计数逐个吻合：

```
transactions -> 2   committed -> 1   per -> 5   second -> 5   rollback -> 1
delete -> 1         tuples -> 6      update -> 3  sequential -> 2  read -> 2  index -> 2
```

14 条建议名**本身都不是表里已有的名字**（是**合成建议**），但文档 §2 第 4 段**明确标注**
「『建议的正确拼写』一列是**建议**，最终以客户确认的为准」—— **诚实标注，未伪装成表里原有的**。

「影响面」核实：14 条的 `collect_cycle` / `value_rule` / `placeholders` / `dimension_bindings`
**都是按指标独立存储、不从 `metric_code` 派生**的字段 → 改名字确实不改变取值 ✅。
`{#NAME}` 占位符在英文名与中文名两处都存在（12 条占位符指标逐条核对 ✅）。

§7 结论数字核对：TABLE:ocr **14 行**、片段 **36** 处、**87 = 101 − 14** ✅。

### OCR 清单的问题

| 级别 | 问题 |
|---|---|
| **Important I-7** | **§4 第 1 条理由是错的**：文档写「`Commited` → `Committed`（英文描述里**少了一个 m 和一个 t**）」。实测 `Commited`(8 字符) → `Committed`(9 字符) **只少一个 `t`，不少 `m`**（`difflib.ndiff` 只显示 `+ t`）。客户一比就发现 |
| **Important I-8** | **§4 第 2 条理由是错的**：文档写「`sequencial` → `sequential`（英文描述里**少了一个 e**）」。实测两者**等长（均 10 字符）**，第 7 位是 **`c` → `t` 的换字母**（`ndiff` 显示 `- c` / `+ t`），**不是少一个 e** |
| **Important I-9** | **§4 漏检 1 条同族错字**：xlsx **R074** 的中文名 `({#NAME})number of sequencial scan` 含与 R075 完全相同的 `sequencial` 错字，§4 只列了 R075。后果：客户改完 R075 后，**R074 的 `metricName` 仍以错字投递到 Kafka**（`metric_rules.json` 证实：`GAUSSDB.{#NAME}.number.Of.sequential.scan` 的 `metric_name` = `({#NAME})number of sequencial scan`） |
| Minor M-5 | **杜撰的列名「英文描述」**：xlsx **没有「英文描述」列**（只有「中文名」列），但文档 §2 判据 2、§3 理由（"本指标英文描述里写的是 checkpoint segments"）、§4 理由反复把「中文名」列称作「英文描述」，而 §4 表头又写「客户表里的中文名」—— 同一列两种叫法 |
| Minor M-6 | §3 理由里的「同族英文名写作 dead tuples / delete tuples / hot update tuples / tuples read」实际来自 xlsx **「中文名」列里混写的英文**，不是「英文名」列 |
| Minor M-7 | §7「其余 87 个未发现疑似错字」只覆盖自动判据（删 1~2 字符 + 子序列 + 正确词须在语料中），**抓不到换字母型错误**（I-8 正是此类），也完全没覆盖中文名列。措辞本身诚实（§2 有写判据），但读者容易过读成「87 个已确认无误」 |
| Minor M-8 | §1「本次核对没有发现取值口径错误」未覆盖**单位口径**类问题（如 xlsx R025 单位是 `%`、同族 R069 是 `tuples/s`） |

---

## 环境变量表

30 个变量**逐个**核对结果（默认值实跑 `load_config()` 比对，非人工抄写）：

| 变量 | config.py 默认 | 文档默认 | 一致 | 必填 | 消费点核实 |
|---|---|---|---|---|---|
| `TPOPS_USER` | — | — | ✅ | **是** | `_require` ✅ |
| `TPOPS_PASSWORD` | — | — | ✅ | **是** | `_require(strip=False)` ✅ |
| `TPOPS_BASE_URL` | `https://172.16.103.193:8002` | 同 | ✅ | 否 | `client.py:161` ✅ |
| `TPOPS_VERIFY_TLS` | `false` | `false` | ✅ | 否 | `client.py:125 session.verify` ✅ |
| `TPOPS_REQUEST_TIMEOUT` | `60` | `60` | ✅ | 否 | `client.py:162` ✅ |
| `TPOPS_AUTH_FAILURE_COOLDOWN_SECONDS` | `60` | `60` | ✅ | 否 | `client.py:163` + `client.py:239` ✅ |
| `KAFKA_BOOTSTRAP_SERVERS` | — | — | ✅ | **是** | `_servers` ✅ |
| `KAFKA_TOPIC_METRICS` | `gaussdb.metrics` | 同 | ✅ | 否 | `producer.py` ✅ |
| `KAFKA_TOPIC_ALARMS` | `gaussdb.alarms` | 同 | ✅ | 否 | `producer.py` ✅ |
| `KAFKA_TOPIC_PARTITIONS` | `4` | `4` | ✅ | 否 | ❌ **未被生产代码消费**（见 I-5） |
| `KAFKA_COMPRESSION` | `lz4` | `lz4` | ✅ | 否 | `producer.py` ✅ |
| `POLL_INTERVAL_SECONDS` | `30` | `30` | ✅ | 否 | `main.py:207 tick_interval` ✅ |
| `METRIC_WINDOW_HOURS` | `2` | `2` | ✅ | 否 | `tpops/metrics.py:171` ✅ |
| `METRIC_END_LAG_MINUTES` | `5` | `5` | ✅ | 否 | `tpops/metrics.py:172` ✅ |
| `CMD_CONCURRENCY` | `2` | `2` | ✅ | 否 | `tpops/command_center.py:144` ✅ |
| `CMD_LIMIT` | `1000` | `1000` | ✅ | 否 | ❌ **未被消费**（文档已披露 ✅） |
| `CMD_PAGE_SIZE` | `100` | `100` | ✅ | 否 | `collectors/command_center.py:144` ✅ |
| `CMD_POLL_TIMEOUT` | `60` | `60` | ✅ | 否 | `tpops/command_center.py:145` ✅ |
| `CMD_RETRY_ATTEMPTS` | `3` | `3` | ✅ | 否 | `tpops/command_center.py:149` ✅ |
| `ALARM_LIMIT` | `100` | `100` | ✅ | 否 | `executors.py:340` ✅ |
| `ALARM_RETENTION_DAYS` | `30` | `30` | ✅ | 否 | `executors.py:339` → `alarm.py:337` ✅ |
| `SQL_MASKING_ENABLED` | `true` | `true` | ✅ | 否 | `mapping/masking.py:200` ✅ |
| `SQL_TEXT_MAX_LENGTH` | `512` | `512` | ✅ | 否 | `mapping/masking.py:201` ✅ |
| `RETRY_MAX_ATTEMPTS` | `5` | `5` | ✅ | 否 | ❌ **未被消费**（文档已披露 ✅） |
| `RETRY_BASE_SECONDS` | `1.0` | `1.0` | ✅ | 否 | `tpops/command_center.py:150` ✅ |
| `STATE_DIR` | `./state` | `./state` | ✅ | 否 | `main.py:233/245` ✅ |
| `HEALTH_HOST` | `127.0.0.1` | `127.0.0.1` | ✅ | 否 | `health.py:333` ✅ |
| `HEALTH_PORT` | `8088` | `8088` | ✅ | 否 | `health.py:334` ✅ |
| `LOG_LEVEL` | `INFO` | `INFO` | ✅ | 否 | `logging_setup.py` ✅ |
| `LOG_FORMAT` | `text` | `text` | ✅ | 否 | `logging_setup.py` ✅ |

**30/30 默认值一致、30/30 必填性一致、28/30 有真实消费点。**

### 两个已知空转变量的表述 —— ✅ 如实

```
$ grep -rn "cmd_limit" tpops_kafka_collector/ | grep -v config.py
（0 处）
$ grep -rn "retry_max_attempts" tpops_kafka_collector/ | grep -v config.py
（0 处）
```

- §3.4 `CMD_LIMIT`「**注：当前实现未消费本项**」→ **属实 ✅**
- §3.7 `RETRY_MAX_ATTEMPTS`「本项**尚未被消费**，改动不影响当前运行行为」→ **属实 ✅**

**但披露口径不一致**：第三个实质空转变量 `KAFKA_TOPIC_PARTITIONS` **未披露**，反而配了一个**虚构的失败后果**（I-5）。

### `.env.example`

```
$ grep -cE '^[A-Z][A-Z0-9_]*=' .env.example
30
```

- ✅ **没有硬编码密码**（`TPOPS_PASSWORD=` 留空；全文无口令字面量）；
  守卫 `test_env_example_has_no_hardcoded_password` + 变异验证均有效。
- ⚠️ `TPOPS_BASE_URL=https://172.16.103.193:8002`（**内网主机:端口**）与 `TPOPS_USER=sml`
  （真实用户名）**硬编码在交付给客户的示例文件里**（Minor M-2）。
- ⚠️ `.env.example` 里 `CMD_LIMIT` 的注释**没有**「当前未消费」提示（文档有）—— 运维只读示例文件时更容易误改（M-3）。

---

## 开发方案周期表

**自己现算**（从包内 `metric_rules.json` 按 `sql_key` / 通道取最短 `collect_cycle`，折算秒）：

```
通道实算（collect_cycle 单位枚举：分 94 / 秒 4 / 时 3）：
  monitor_metrics      指标数=11   最小档位=30   分布(秒:条数)={30: 1, 60: 3, 300: 7}
  command_center_sql   指标数=82   最小档位=60   分布(秒:条数)={60: 30, 120: 15, 300: 35, 86400: 2}
  diagnostic           指标数=4    最小档位=120  分布(秒:条数)={120: 1, 300: 3}
  instance             指标数=1    最小档位=3600 分布(秒:条数)={3600: 1}

§4.6.2 逐行核对：
  monitor          文档 30 秒   实算 30s     ✅   档位 [30,60,300]      ✅
  alarm            文档 30 秒   实算 30s     ✅   （ALARM_INTERVAL_SECONDS=30）
  command_center   文档 60 秒   实算 60s     ✅   档位 [60,120,300,86400] ✅
  diagnostic       文档 120 秒  实算 120s    ✅   档位 [120,300]        ✅
  instance         文档 3600 秒 实算 3600s   ✅

§4.6.3 SQL 到期判定 11 行逐行核对：
  stat_database 60/60 ✅  stat_user_tables 120/120 ✅  activity_agg 60/60 ✅
  activity_running 120/120 ✅  activity_top10 300/300 ✅  locks 60/60 ✅
  bgwriter 300/300 ✅  settings 60/60 ✅  database 120/120 ✅
  prepared_xacts 60/60 ✅  dbstat_sum 60/60 ✅
```

**`diagnostic = 120 秒` 的依据核实**（任务指定）：

```
$ conda run -n tpops python -c "...print(rule)"
gaussdb.Top10sql  collect_cycle = {'value': 2, 'unit': '分'}   → 120 秒 ✅
diagnostic 通道其余 3 条：{'value': 5, 'unit': '分'} → 300 秒
→ diagnostic 通道内无更短档位，最小即 120 秒 ✅
```

**§4.6.2 / §4.6.3 的表格与实算逐行一致，无一处数值错误 ✅。**

**但 §4.6.3 描述的「按各 SQL 自己的档位分别到期刷新」机制在运行时并未落地**（与 I-1 同源）：

```
$ grep -rn "render_sql(" tpops_kafka_collector/collectors/command_center.py
  command_center.py:210   sql_text = render_sql(self._spec, page_size=..., offset=offset)
                          # 不传 tier_seconds → sqlgen.select_views(spec, None) → 全部 11 个视图

$ grep -rn "refresh_seconds" tpops_kafka_collector/ --include=*.py
  mapping/sqlgen.py:85,144,158,169    # 只在生成器路径与单测里用，运行时从未读取
```

即运行时是「**一条合并 SQL，每 60 秒跑全部 11 个视图**」，
§4.6.2 括号里的「`stat_user_tables` / `database` 为 120 秒，`activity_top10` / `bgwriter` 为 300 秒」
**在运行时不存在**。§4.6.3 新增的 ⚠ 注释说「运行时以 `merged_sql.json` 为准」也不准确
（`merged_sql.json` 的 `refresh_seconds` 运行时从未被读取）→ Minor M-9。

---

## 守卫强度

`tests/unit/test_delivery_docs.py`（29 例）。**变异验证在 `/tmp/w4rev-mut` 副本里做**：

| # | 变异 | 期望 | 实测 | 判定 |
|---|---|---|---|---|
| M1 | 口径说明里把某指标取值规则从「差分算速率」改成「直接取累计值」 | 红 | 红（`test_docs_match_generator_output`） | ✅ 有效 |
| M2 | **改真相源**：`metric_rules.json` 把 `counter_delta` 改成 `raw` 后重生成文档 | 红 | **绿**（29 passed） | ❌ **恒绿** |
| M3 | 部署说明里把 `TPOPS_PASSWORD` 的「必填」从 **是** 改成 **否** | 红 | **绿** | ❌ **恒绿** |
| M4 | 部署说明里把 `POLL_INTERVAL_SECONDS` 的「改错的后果」改成完全错误的说法 | 红 | **绿** | ❌ **恒绿** |
| M5 | 部署说明里删掉「4.2 优雅退出」整节 | 红 | **绿** | ❌ **恒绿** |
| M6 | 口径说明 §3.5 里把 A 类「逐组件各出一条」改成「合成一条」 | 红 | 红（`test_docs_match_generator_output`） | ✅ 有效 |
| M7 | 口径说明 §5.2 里把 A 类 8 条的「归集口径」列全改成「实例级：只取第一个 master CN 的一条」 | 红 | 红 | ✅ 有效 |
| M8 | 口径说明里删掉「3.3 多 master 的下发与去重」整节 | 红 | 红（`test_metric_spec_documents_node_scoped_semantics` + 同步检查） | ✅ 有效 |
| M9 | 部署说明里删掉 `CMD_LIMIT` 的「注：当前实现未消费本项」披露 | 红 | **绿** | ❌ **恒绿** |
| M10 | **改生成器** `describe_collection`，让 A 类也标注「合成一条」，重生成后 | 红 | **绿** | ❌ **恒绿** |
| M11 | 部署说明里把 `CMD_LIMIT` 默认值从 `1000` 改成 `500` | 红 | 红（`test_deploy_doc_defaults_match_config`） | ✅ 有效 |
| M12 | OCR 清单里删掉一条疑似错字 | 红 | 红（2 条） | ✅ 有效 |
| M13 | 不可提供清单里把「能不能变通」列删空 | 红 | 红 | ✅ 有效 |
| M14 | `.env.example` 里给 `TPOPS_PASSWORD` 写死口令 | 红 | 红（2 条） | ✅ 有效 |
| M15 | 口径说明 §3.4 里把「本轮不产出这条消息」改成「照常产出」 | 红 | 红 | ✅ 有效 |
| M16 | 口径说明里把命令中心通道的指标数 82 改成 80 | 红 | 红 | ✅ 有效 |

**结论：29 例中约 8 例是真正的「只查数量/只查名单」型恒绿守卫**，
5 条变异（M2/M3/M4/M5/M9/M10）**改坏文档后守卫仍然全绿**。
其中 **M2 / M10 是结构性问题**：
守卫验证的是「文档 == 生成脚本(映射表)」，
**不验证「文档 == 实现代码」** —— 这正是 C-1 / I-1 / I-3 能溜过去的原因。

**最可疑的一条**：`test_metric_spec_documents_node_scoped_semantics` 只做关键词存在性检查
（`'node_scoped', 'master_view', 'master CN', '去重', 'node_id'` 出现在文中即通过）——
**只查数量不查名单**。M8（删掉整节）能被抓到只是因为恰好命中了「去重」二字的缺失，
而不是因为判据检查了语义。

---

## 实测

全部为**本审查方自己跑**的输出，未采信任何实施方数字。

```
# 交付守卫
$ conda run -n tpops python -m pytest tests/unit/test_delivery_docs.py -q
29 passed in 0.57s

# 全量回归（/tmp/w4rev-mut 副本，含 .env.example + 客户 xlsx）
$ conda run -n tpops python -m pytest -q
1723 passed in 61.84s (0:01:01)

# py37 红线
$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
156 passed in 1.34s
$ conda run -n tpops python - <<'PYEOF'   # ast.parse(feature_version=(3,7)) 全量扫
files checked, failures: 0

# ruff（合同门禁：tpops_kafka_collector/）
$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

# ruff（本波新增文件，非门禁范围）
$ conda run -n tpops python -m ruff check tools/build_delivery_docs.py tests/unit/test_delivery_docs.py
Found 146 errors.   # 107 UP031 / 30 E501 / 2 F841 / ...
$ conda run -n tpops python -m ruff check tools/build_delivery_docs.py --output-format=concise | grep F841
tools/build_delivery_docs.py:993:17: F841 Local variable `rule` is assigned to but never used
tools/build_delivery_docs.py:1080:5: F841 Local variable `total` is assigned to but never used

# 生成器 --check
$ conda run -n tpops python tools/build_delivery_docs.py --check
一致：docs/交付-不可提供指标清单.md
一致：docs/交付-客户指标英文名核对清单.md
一致：docs/交付-指标口径说明.md
（exit=0）

# 生成器幂等
$ conda run -n tpops python -m pytest tests/unit/test_delivery_docs.py::test_generator_is_idempotent -q
1 passed
```

---

## 发现

### Critical

**C-1｜A 类 8 个指标的「归集口径」列与实现相反 —— 给客户错误承诺**

- **位置**：`docs/交付-指标口径说明.md:218-228`（§5.2 监控指标接口表），
  生成器 `tools/build_delivery_docs.py:281-292 describe_collection`
- **文档说的**：8 条 `node_scoped=true` 的 A 类指标（`GAUSSDB.Number_of_ansactions_mmitted_r_cond`、
  `GAUSSDB.Number_of_llback_ansactions_r_cond`、`gaussdb.Tps`、`gaussdb.QPS`、`gaussdb.Starttime`、
  `gaussdb.Uptimestr`、`GAUSSDB.Standby_status`、`GAUSSDB.Primary_status`）的归集口径列一律写
  「**节点级：每个 master 各出一条**」
- **实现做的**：A 类走 `monitor.py` → `rules_series.py:14 eval_series_point`，
  对 `ctx.series` **每个组件 key 出一条消息**，维度键是 `COMPONENT_ID`。
  **实测**（跑 `data/metric_response/` 的原生响应）：

```
$ conda run -n tpops python /tmp/allA.py
  GAUSSDB.Number_of_ansactions_mmitted_r_cond  series=  3  doc归集口径='节点级：每个 master 各出一条'
  gaussdb.Tps                                  series=  3  doc归集口径='节点级：每个 master 各出一条'
  gaussdb.QPS                                  series=  6  doc归集口径='节点级：每个 master 各出一条'
  GAUSSDB.Standby_status                       series=  9  doc归集口径='节点级：每个 master 各出一条'
  GAUSSDB.Primary_status                       series=  9  doc归集口径='节点级：每个 master 各出一条'
  GAUSSDB.Service_availability_testing         series=  1  doc归集口径='实例级：只取第一个 master CN 的一条'
  ...
$ conda run -n tpops python /tmp/acount.py
  GAUSSDB.Standby_status: series=9 -> 消息条数=9  node_scoped(rule)=True
      dims {'COMPONENT_ID': '172.16.103.126-dn_6005'} value 0.0
      dims {'COMPONENT_ID': '172.16.103.126-dn_6009'} value 0.0
      dims {'COMPONENT_ID': '172.16.103.127-dn_6002'} value 0.0
  gaussdb.QPS: series=6 -> 消息条数=6
      dims {'COMPONENT_ID': 'cn_5002'} value 28.13   ← 3 个 CN + 3 个 DN
  gaussdb.Tps: series=3 -> 消息条数=3
```

- **后果**：客户按「每个 master 各出一条」估算消息量会算错：
  - `GAUSSDB.Standby_status` / `Primary_status`：文档暗示 3 条（3 个 master），**实际 9 条**（9 个 DN）；
  - `gaussdb.QPS`：文档暗示 3 条，**实际 6 条**（3 CN + 3 DN）；
  - 资源名也错：文档说「每个 master」，实际资源名是**每个组件所在节点**
    （`message.py` 的 `resolve_node_context` 用 `COMPONENT_ID` 反查节点，`monitor.py:245-252`）。
  - **同一份文档的 §3.5（第 91 行）写的才是对的**（「逐组件各出一条消息」）——
    **文档自相矛盾**，客户按逐指标表那张表配置台账会配错。
- **修法**：把 A 类的「归集口径」列改成「**组件级：每个组件各出一条（`COMPONENT_ID` 进 `tags`）**」，
  并把 `node_scoped` 的语义限定为「**仅 B 类命令中心**适用」；
  生成器 `describe_collection` 需按 `source.source` 分通道渲染（A 类不走 master 口径）。
  同时 §3.2 的视图表加一句「本节只适用于命令中心（B 类）通道」。
  **修法必须同步 `tools/build_delivery_docs.py`，否则 `test_docs_match_generator_output` 会红。**

### Important

**I-1｜口径说明没写清「声明周期 ≠ 实际节奏」，65/98 条指标的消息量比客户预期高 2.5~1440 倍**

- **位置**：`docs/交付-指标口径说明.md:21`（列说明）、`:268`（§7）
- **文档说的**：「刷新周期 | 该指标的采集周期，与客户指标库「数据更新周期」一致」；
  「采集频率 | 以客户指标库「数据更新周期」为准（30 秒 ~ 24 小时共 7 档）」
- **实现做的**：**没有按指标 `collect_cycle` 的产出闸门** —— 每个通道按自己档位节拍跑，
  一轮产出该通道的**全部**规则（`executors.py` 四个采集器调用点都不传 `rules` 子集；
  `collectors/command_center.py:161` 的 docstring「调度器按档位传子集」与实现不符）。
  实算通道节拍：monitor 30s / command_center 60s / diagnostic 120s / instance 3600s。
- **后果**：24 小时档的 `gaussdb.Serverencoding` / `gaussdb.Timezone` **实际每 60 秒出一条**（1440×）；
  5 分钟档的 45 条指标实际每 30~120 秒出一条（2~5×）。
  客户按「刷新周期」列配保留策略、告警阈值、计费口径，会全盘算错。
- **修法**：§1 或 §3 增加一段「**声明周期 vs 实际投递节奏**」：
  「刷新周期 = 客户声明的需求档；实际投递节奏 = 通道节拍（命令中心 60 秒 / 监控指标 30 秒 /
  诊断优化 120 秒 / 实例 3600 秒），比声明档更频，属**超服务**」；
  §7 的「采集频率」行改成如实表述。

**I-2｜部署说明 §2 第 3 步 `cp .env.example` 必然失败 —— 客户照文档部署会断在第 3 步**

- **位置**：`docs/交付-部署运行说明.md:36-50`
- **文档说的**：第 1 步只拷 `tpops_kafka_collector/` 与 `requirements.txt`，
  第 3 步 `cp /opt/tpops-collector/.env.example /opt/tpops-collector/.env`
- **实现做的**：`.env.example` **在仓库根目录**，不在产物目录内：

```
$ ls -la .env.example          → -rwxrwxrwx 1 sml sml 8293 .env.example
$ ls tpops_kafka_collector/.env.example
ls: cannot access 'tpops_kafka_collector/.env.example': No such file or directory
$ git ls-files | grep env.example
.env.example
```

- **后果**：文档第 3 行承诺「照着本文走完，客户环境上就能跑起一个常驻采集端」——
  这一处与之直接矛盾，客户按抄必然 `cp: cannot stat`。
- **修法**：第 1 步补 `cp .env.example /opt/tpops-collector/`；
  或把 `.env.example` 移进 `tpops_kafka_collector/`（后者才真正做到「只要一个目录」）。

**I-3｜`gaussdb.Total_pct` 的备注「活跃会话数 / 总连接数」与实现不符 —— 分子实际是全部会话数**

- **位置**：`docs/交付-指标口径说明.md:172`（备注列）
- **文档说的**：取值规则「activity_agg 视图的 `cnt` ÷ stat_database 视图的 `numbackends`」✅；
  备注「**活跃会话数 / 总连接数**」
- **实现做的**：`ratio_across` 的分子 spec 是 `{sql_key: activity_agg, fn: sum, value_column: cnt}`
  —— `activity_agg` 视图的 SQL 是
  `select datname, state, count(*) as cnt from pg_stat_activity where state is not null group by datname, state`，
  **对所有 state 求和 = 全部会话数（含 idle）**，不是「活跃」。
  映射表自己的 `value_rule.note` 写的是「分子=activity_agg **全部会话数**」——
  **映射表内部两条 note 互相打架**，交付文档选了错的那条。
- **后果**：客户按「活跃会话数 / 总连接数」理解连接使用率，会得到偏高的解读；
  而 `GAUSSDB.session_usage`（同一套 SQL、同一套 spec）的备注写「在线会话数」——
  两条同源指标的说明却不一致，客户会怀疑其中一条取错了。
- **修法**：把 `metric_rules.json` 里 `gaussdb.Total_pct` 的 `note` 改成
  「全部会话数 / 总连接数（activity_agg 的 cnt 含 idle 等全部状态）」，
  与 `value_rule.note` 对齐；生成器重跑。

**I-4｜部署说明 §8 引用的日志「命令中心 …… 重试后成功」在代码里不存在 —— 运维 grep 不到**

- **位置**：`docs/交付-部署运行说明.md:304`
- **文档说的**：排障表症状「日志『命令中心 …… 重试后成功』」
- **实现做的**：**没有这条日志**：

```
$ grep -rn "重试后成功" tpops_kafka_collector/
tpops_kafka_collector/tpops/command_center.py:26:      # 注释
tpops_kafka_collector/tpops/command_center_result.py:24: # 注释
（实际日志只有两条）
$ grep -n "LOGGER\." tpops_kafka_collector/retry.py
170:  LOGGER.error('重试次数用尽，放弃：%s（%s：%s）', ...)
176:  LOGGER.warning('第 %s/%s 次重试 %s，等待 %.3f 秒：%s：%s', ...)
```

- **后果**：重试机制本身**确实实现**（`cmd_retry_attempts` 默认 3、指数退避 1→2→4），
  但运维照文档 grep 这句话**永远匹配不到**，排障时会被误导去查别处。
- **修法**：把文档里的日志串改成真实的
  「第 N/M 次重试 命令中心 SQL：…，等待 X 秒：…」/「重试次数用尽，放弃：…」；
  或在 `retry.py` 里补一条「重试后成功」的 WARNING 后再写文档。

**I-5｜`KAFKA_TOPIC_PARTITIONS` 是第三个空转变量，文档未披露，且配了一个不存在的失败后果**

- **位置**：`docs/交付-部署运行说明.md:19`（前置条件）、`:114`（§3.2）、`:307`（§8）
- **文档说的**：「与客户预建 Topic 分区数不一致 → 生产者报『Topic 已存在但分区数不符』」
- **实现做的**：**代码里不存在分区数校验，生产路径也不建 Topic**：

```
$ grep -rn "ensure_topics" tpops_kafka_collector/
tpops_kafka_collector/kafka/producer.py:315:    def ensure_topics(self, admin=None) -> ...   # 仅定义
（main.py / lifecycle.py / executors.py 里**无任何调用方**；只有 tests/integration/test_producer.py 调用）

$ grep -n "num_partitions\|self._producer.send" tpops_kafka_collector/kafka/producer.py
292:  future = self._producer.send(topic, key=..., value=...)   # 不传 partition
350:  num_partitions=self._partitions,                          # 只在 _create_topics() 里用（ensure_topics 内部）
```

  即使 `ensure_topics` 被调用，`TopicAlreadyExistsError` 也只是
  `LOGGER.info('Topic 已存在，跳过创建')`（`producer.py:355-356`）—— **没有任何分区数比较逻辑**。
  实际投递由 kafka-python 默认分区器按 key 哈希 % broker 上的**实际**分区数，与本变量无关。
- **后果**：把 `KAFKA_TOPIC_PARTITIONS` 改成任何值，**生产行为完全不变**；
  文档承诺了一个客户永远等不到的报错，并让运维以为分区数需要与配置对齐。
  与 §3.4 / §3.7 对 `CMD_LIMIT` / `RETRY_MAX_ATTEMPTS` 的「未消费」披露**口径不一致**。
- **修法**：§3.2 改成「**注：当前实现未消费本项**（Topic 由客户预建，生产者不创建、不校验分区数）」；
  §1 前置条件与 §8 的「分区数不符」一并删掉；或反过来在 `producer.py` 里补真实的分区数校验
  并让 `ensure_topics` 进入生产路径。

**I-6｜`POLL_INTERVAL_SECONDS` 的「改错的后果」说不准 —— 实际刷新周期不是轮询间隔**

- **位置**：`docs/交付-部署运行说明.md:121`
- **文档说的**：「调大 → 各通道实际刷新周期被拉长（**周期 = 轮询间隔**）；调得过小（< 10）→ 平台压力大」
- **实现做的**：通道周期由映射表实算（`schedule.py:106-138`），`poll_interval` 只是 **tick 粒度**
  （`main.py:207 tick_interval_seconds`）；`DueTable.split_due` 推进的是 `entry.interval_seconds`
  （= 通道周期），**不是 poll**。实跑模拟（1 小时）：

```
$ conda run -n tpops python /tmp/pollcheck.py
POLL_INTERVAL_SECONDS=30   -> monitor 30.0, command_center 60.0, diagnostic 120.0, alarm 30.0
POLL_INTERVAL_SECONDS=60   -> monitor 60.0, command_center 60.0, diagnostic 120.0, alarm 60.0
POLL_INTERVAL_SECONDS=120  -> monitor 120.0, command_center 120.0, diagnostic 120.0, alarm 120.0
POLL_INTERVAL_SECONDS=300  -> monitor 300.0, command_center 300.0, diagnostic 300.0, alarm 300.0
```

  正确口径是「实际刷新周期 ≈ ceil(通道周期 / poll) × poll」。
  「调得过小（< 10）→ 平台压力大」**不成立** —— 通道未到期**不执行**，poll 调小只增加 tick 唤醒次数，
  **不增加平台请求量**。真正的风险方向恰好相反：poll 调大会把**所有**通道拉长到 poll 的整数倍
  （poll=300 时 instance 通道从 3600 秒反而变成 300 秒），**文档没写这条**。
- **后果**：客户按文档理解「周期 = 轮询间隔」，会把默认 30 秒误当成各通道都是 30 秒刷新，
  对 command_center（实为 60 秒）/ diagnostic（120 秒）/ instance（3600 秒）的数据新鲜度判断错误。
- **修法**：改成「**轮询间隔 = 调度 tick 粒度**；各通道实际刷新周期由映射表实算
  （monitor 30 秒 / command_center 60 秒 / diagnostic 120 秒 / instance 3600 秒 / alarm 30 秒），
  并向上取整到轮询间隔的整数倍。poll 调大到超过通道周期时，该通道会被拉长到 poll 的整数倍」。

**I-7｜OCR 清单 §4 第 1 条的理由是错的（「少了一个 m 和一个 t」）**

- **位置**：`docs/交付-客户指标英文名核对清单.md:56`
- **文档说的**：「`Commited` → `Committed`（英文描述里**少了一个 m 和一个 t**）」
- **实测**：

```
$ conda run -n tpops python -c "import difflib; print(list(difflib.ndiff('Commited','Committed')))"
['  C', '  o', '  m', '  m', '  i', '  t', '+ t', '  e', '  d']
```

  `Commited`(8) → `Committed`(9)：**只少一个 `t`**，`m` 一个不少。
- **后果**：这是**写给客户看的事实性错误**，客户一比就发现，会削弱整份清单的可信度。
- **修法**：改成「少了一个 `t`」（`Commited` → `Committed`）。

**I-8｜OCR 清单 §4 第 2 条的理由是错的（「少了一个 e」实际是换字母 `c`→`t`）**

- **位置**：`docs/交付-客户指标英文名核对清单.md:57`
- **文档说的**：「`sequencial` → `sequential`（英文描述里**少了一个 e**）」
- **实测**：

```
$ conda run -n tpops python -c "import difflib; print(list(difflib.ndiff('sequencial','sequential')))"
['  s','  e','  q','  u','  e','  n','- c','+ t','  i','  a','  l']
```

  两者**等长（均 10 字符）**，第 7 位是 **`c` → `t` 的换字母**，**不是少一个 e**。
- **后果**：同 I-7，客户可见的事实性错误。
- **附带**：这暴露了自动判据的一个盲区 —— 「删 1~2 字符得子序列」**抓不到换字母型错误**，
  而 §7 却说「其余 87 个未发现疑似错字」，读者容易过读成「87 个已确认无误」（见 M-7）。
- **修法**：改成「第 7 位是 `c` 应为 `t`（`sequencial` → `sequential`）」。

**I-9｜OCR 清单 §4 漏检 1 条同族错字 —— 客户改完 R075 后 R074 的 `metricName` 仍带错字投递**

- **位置**：`docs/交付-客户指标英文名核对清单.md:52-58`（§4 表只有 2 行）
- **文档说的**：§4 列出 2 条中文名/英文描述错字
- **实测**：客户 xlsx **R074** 的中文名也含 `sequencial`：

```
$ conda run -n tpops python -c "import openpyxl; ..."
R074 ['GAUSSDB实例','事务信息','({#NAME})number of sequencial scan',
      'GAUSSDB.{#NAME}.number.Of.sequential.scan','整数','5','分','否','scans/s']
R075 ['GAUSSDB实例','事务信息','({#NAME})number of sequencial tuples read',
      'GAUSSDB.{#NAME}.number.Of.quential.ples.ad','整数','5','分','否','tuples/s']

$ conda run -n tpops python -c "...metric_rules.json..."
  GAUSSDB.{#NAME}.number.Of.sequential.scan | name= ({#NAME})number of sequencial scan | enabled= True
  GAUSSDB.{#NAME}.number.Of.quential.ples.ad | name= ({#NAME})number of sequencial tuples read | enabled= True
```

- **后果**：`metricName` 是投递字段（`message.py:28 substitute_placeholders(metric_rule.metric_name, ...)`）。
  客户按 §4 改完 R075，**R074 的 `metricName` 仍以 `sequencial` 错字进 Kafka**。
- **修法**：§4 补第 3 条：`({#NAME})number of sequencial scan` → `({#NAME})number of sequential scan`，
  对应指标 `GAUSSDB.{#NAME}.number.Of.sequential.scan`；
  生成器 `OCR_DESC_REVIEW` 同步补。

### Minor

| # | 位置 | 问题 | 修法 |
|---|---|---|---|
| M-1 | `tools/build_delivery_docs.py:993, 1080` | 2 条 F841（`rule` / `total` 赋值后未使用）—— 新增文件自带 lint 债（该文件共 146 条 ruff 错，其中 107 条 UP031 是仓内既有风格） | 删掉未使用的变量 |
| M-2 | `.env.example:3, 5` | 硬编码内网地址 `https://172.16.103.193:8002` 与真实用户名 `sml`（口令留空了，用户名没留，口径不一致）。`config.py:48/249` 同样写死 | `.env.example` 里留空 + 注释写「默认见 config.py」 |
| M-3 | `.env.example` `CMD_LIMIT` 注释 | 缺「当前未消费」提示（文档 §3.4 有）—— 运维只读示例文件时更容易误改 | 与文档对齐 |
| M-4 | `docs/交付-指标口径说明.md:95` | 「基线键是『指标名 + 维度 + 节点』**三元组**」与实现的「二元组 `(metric_code, 'node:dimkey')`」字面不符（语义等价）；同段还引用了 `rules_deltas.py` docstring 里**已被推翻的旧描述**「基线键不含节点」 | 改成「基线键是二元组 `(指标名, 作用域:维度)`，作用域一般是 node_id —— 效果等价于按节点隔离」 |
| M-5 | `docs/交付-客户指标英文名核对清单.md` §2/§3/§4 | 杜撰的列名「英文描述」—— xlsx **没有**这一列，同一份文档 §4 表头又写「客户表里的中文名」 | 统一改称「中文名列」 |
| M-6 | 同上 §3 理由 | 「同族英文名写作 dead tuples / delete tuples / hot update tuples / tuples read」实际来自 xlsx **中文名列**里混写的英文 | 措辞改成「同族指标的中文名里混写的英文」 |
| M-7 | 同上 §7 | 「其余 87 个未发现疑似错字」只覆盖自动判据（删 1~2 字符 + 子序列 + 正确词须在语料中），抓不到换字母型错误（I-8）、也完全没覆盖中文名列 | 加一句「判据只覆盖删字型错误；换字母型与中文名列需人工核对」 |
| M-8 | 同上 §1 | 「本次核对没有发现取值口径错误」未覆盖单位口径类问题（xlsx R025 单位 `%`、同族 R069 `tuples/s`） | 补一句单位口径待客户确认 |
| M-9 | `docs/开发方案-GaussDB指标与告警采集.md` §4.6.2/§4.6.3 | §4.6.3 新增的 ⚠ 注释说「运行时以 `merged_sql.json` 为准」不成立（`refresh_seconds` 运行时从未被读取）；§4.6.2 括号里的分档（`activity_top10`/`bgwriter` 300 秒）运行时也不存在（一条合并 SQL 每 60 秒跑全部 11 视图）；§4.6.2 括号漏了 `activity_running`（120 秒），与 §4.6.3 不齐 | 区分「设计意图」与「实际实现」两栏；修正 ⚠ 注释；补 `activity_running` |

---

## 收尾

**结论：verdict = fail**（Critical 1 / Important 8 / Minor 9）。

阻塞项：**C-1**（A 类归集口径给客户错误承诺）、**I-1**（声明周期 vs 实际节奏）、
**I-2**（部署步骤跑不通）、**I-3**（Total_pct 备注错）、
**I-4 / I-5 / I-6**（三处「改错的后果」与实现不符或虚构）、
**I-7 / I-8 / I-9**（OCR 清单给客户看的事实性错误 + 漏检）。

守卫方面另有 6 条变异（M2/M3/M4/M5/M9/M10）**改坏文档后守卫仍然全绿** ——
根因是守卫只验证「文档 == 生成脚本(映射表)」，**不验证「文档 == 实现代码」**。
建议在修复轮里补一条把交付文档的关键断言与实现常量/结构对上的守卫
（例如：A 类消息条数 == `eval_series_point` 的 series 数；通道节拍 == `compute_channel_plans` 的实算值；
`ensure_topics` 的调用方存在性）。

签发 receipt 的命令：

```
ssf execution review changes/tpops-kafka-collector \
  --wave wave-4-delivery-docs --base 15a076b --head 2650399 \
  --report .superpowers/sdd/reviews/wave-4-delivery-docs.md --verdict fail
```
