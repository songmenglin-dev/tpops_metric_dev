# Wave 4 定向复审报告（修复轮）—— wave-4-delivery-docs

- **base** `2650399` / **head** `9860bac`
- **结论**：verdict = **fail**
- **计数**：**Critical 0 / Important 7 / Minor 4**

> 审查方式：**不采信修复报告里的任何数字与结论**。9 条发现的闭环全部由本审查方**自己跑生产代码**核实
> （A 类实跑回放 `data/metric_response/` 原生响应；通道周期实跑 `compute_channel_plans()`；
> 98 行逐行核对，不抽查）；5 条新守卫**逐条做变异验证**；首轮那 6 条恒绿变异**逐条重跑**。
> 变异验证在 `/tmp/w4r2-mut` + `/tmp/w4r2-mut-run` 副本里做（工作树只写入本报告一个文件），
> 收尾两个副本已删除（`rm -rf`），工作树 `git status --porcelain` 为空。

---

## 一、9 条逐条核验

| # | 首轮发现 | 判定 | 本审查方自己核实的证据 |
|---|---|---|---|
| **C-1** | A 类归集口径写错（每个 master vs 逐组件）且文档自相矛盾 | **已闭环** | 见下方 §1.1 独立回放：**11/11** 条 A 类行改为「组件级：逐组件各出一条」，实测消息条数逐条吻合 |
| **I-1** | 「刷新周期 = 与客户指标库一致」不成立 | **已闭环** | 见下方 §1.2：拆成「声明周期」+「实际产出频率」，**98/98 行**「实际产出频率」== 生产 `compute_channel_plans()` 实算通道周期，**0 处不符** |
| **I-2** | `cp .env.example` 必然失败 | **已闭环** | `docs/交付-部署运行说明.md:47-63` 已无 `cp .env.example` 步骤；改为「按 §3 变量表就地创建 `.env`」，并显式说明「`.env.example` 在仓库根目录，不在 `tpops_kafka_collector/` 产物目录内」+ 两条分支（有完整仓库 / 只有产物目录） |
| **I-3** | `Total_pct` 备注「活跃会话数」实为全部会话数 | **已闭环** | `docs/交付-指标口径说明.md:214` 备注 = 「全部会话数 / 总连接数（分子=activity_agg 的 cnt 之和，含 idle 等全部状态）」；生成器 `tools/build_delivery_docs.py:100` `NOTE_OVERRIDE` 覆写（真相源 `metric_rules.json` 的 `note` 仍是「活跃会话数」，但**生成物**已正确）。同族 `GAUSSDB.session_usage` 一并覆写，两条不再打架 |
| **I-4** | 排障表引用代码里不存在的日志 | **文档已闭环 / 守卫有洞** | `docs/交付-部署运行说明.md:311-312` 改成真实存在的 `retry.py:170` 「重试次数用尽，放弃：…」与 `retry.py:176` 「第 %s/%s 次重试 %s，等待 %.3f 秒：%s：%s」。原句「重试后成功」已消失：`grep -rn "重试后成功" tpops_kafka_collector/` → 0 处（首轮是 2 处注释命中）。**但新增的守卫抓不住这条缺陷** —— 见 §2 的 N4 变异 |
| **I-5** | 空转变量未披露 + 承诺了不存在的报错 | **文档已闭环 / 守卫有洞** | 三个空转变量均如实披露：`部署运行说明.md:121`（`KAFKA_TOPIC_PARTITIONS`）、`:137`（`CMD_LIMIT`）、`:160`（`RETRY_MAX_ATTEMPTS`）。虚构的「Topic 已存在但分区数不符」已删（`:19` 明写「生产者不创建 Topic、也不校验分区数」）。**但新增的守卫只认 `CMD_LIMIT` / `RETRY_MAX_ATTEMPTS`，认不出 `KAFKA_TOPIC_PARTITIONS`** —— 见 §2 的 N5 变异 |
| **I-6** | `POLL_INTERVAL_SECONDS` 的「周期 = 轮询间隔」不成立 | **文档已闭环 / 无守卫** | `部署运行说明.md:128` 已改成准确口径：「调度 **tick 粒度**……**它不是各通道的采集周期**……当它**大于**某通道的周期时，该通道会被拉长到它的整数倍（例：调成 300 秒时命令中心从 60 秒变成 300 秒）。调小**不会**增加平台请求量」。**但改坏这段文案守卫仍全绿** —— 见 §2 的 M4 变异 |
| **I-7** | OCR 清单「少了一个 m 和一个 t」错 | **已闭环** | 文档现写「`Commited` → `Committed`（正确写法的**第 7 位缺 `t`**）」。实测 `difflib.SequenceMatcher`：`insert bad[6:6]='' good[6:7]='t'` → 第 7 位、`ndiff` 只有 `['+ t']`（**没有 m**）✅。措辞由生成器 `describe_char_diff`（`build_delivery_docs.py:1140`）用 `difflib` 算出，非手写 |
| **I-8** | OCR 清单「少了一个 e」错（实为换字母） | **已闭环** | 文档现写「`sequencial` → `sequential`（第 7 位 `c` 应为 `t`）」。实测 `replace bad[6:7]='c' good[6:7]='t'`、`ndiff` = `['- c', '+ t']`（等长 10 字符、换字母，**不是少一个 e**）✅ |
| **I-9** | OCR 清单漏检 R074 的 `sequencial` | **已闭环** | `docs/交付-客户指标英文名核对清单.md` §4 中文名列表**已列 3 条**，第 2 条 = `({#NAME})number of sequencial scan` → `({#NAME})number of sequential scan`；反查真相源：`GAUSSDB.{#NAME}.number.Of.sequential.scan` 的 `metric_name` = `({#NAME})number of sequencial scan`（R074）✅ |

### 1.1 C-1 的独立核实过程（本审查方自己跑生产代码）

**不复用首轮的脚本**，自己写了一个回放器（`/tmp/w4r2_verify.py`）：
取 `data/metric_response/` 里每个 A 类指标的**原生响应体**（取 `is_latest_point_only == "false"` 的那一次），
走**生产代码**的 `mapping.decode.decode()` → `collectors/monitor._sliced_context()` → `mapping.rules.evaluate()`，
数出实际产出的消息条数：

```
$ conda run -n tpops python /tmp/w4r2_verify.py <工作树>
C-1 独立核实：A 类实跑 —— 每个客户指标产出几条消息
  GAUSSDB.Number_of_ansactions_mmitted_r_cond    series=3   消息=3   node_scoped=True  keys=['cn_5001', 'cn_5002', 'cn_5003']
  GAUSSDB.Number_of_llback_ansactions_r_cond     series=3   消息=3   node_scoped=True  keys=['cn_5001', 'cn_5002', 'cn_5003']
  GAUSSDB.PG_Service_Status                      series=1   消息=1   node_scoped=False keys=['value']
  GAUSSDB.Primary_status                         series=9   消息=9   node_scoped=True  keys=['172.16.103.126-dn_6001', ... 共 9 个 DN]
  GAUSSDB.Service_availability_testing           series=1   消息=1   node_scoped=False keys=['value']
  GAUSSDB.Standby_status                         series=9   消息=9   node_scoped=True  keys=[... 共 9 个 DN]
  gaussdb.QPS                                    series=6   消息=6   node_scoped=True  keys=['cn_5001','cn_5002','cn_5003','dn_6001','dn_6004','dn_6007']
  gaussdb.Starttime                              series=3   消息=3   node_scoped=True  keys=['cn_5001','cn_5002','cn_5003']
  gaussdb.Status                                 series=1   消息=1   node_scoped=False keys=['value']
  gaussdb.Tps                                    series=3   消息=3   node_scoped=True  keys=['cn_5001','cn_5002', 'cn_5003']
  gaussdb.Uptimestr                              series=3   消息=3   node_scoped=True  keys=['cn_5001', 'cn_5002', 'cn_5003']
```

再把**文档那一行**的归集口径取出来逐条对（`/tmp/w4r2_doccheck.py`）：

```
  GAUSSDB.Number_of_ansactions_mmitted_r_cond  文档归集口径 = 组件级：逐组件各出一条（每条 series 一条消息，`COMPONENT_ID` 归入 `tags`，不跨组件合成）
  GAUSSDB.Primary_status                        文档归集口径 = 组件级：逐组件各出一条（……）
  GAUSSDB.Standby_status                        文档归集口径 = 组件级：逐组件各出一条（……）
  gaussdb.QPS                                   文档归集口径 = 组件级：逐组件各出一条（……）
  gaussdb.Tps                                   文档归集口径 = 组件级：逐组件各出一条（……）
  gaussdb.Uptimestr / Starttime / PG_Service_Status / Status / Service_availability_testing  同上
```

**11/11 行的文案与实测条数一致**（3/3/1/9/1/9/6/3/1/3/3 条）。
文档 §3.5（`docs/交付-指标口径说明.md:95-107`）另有一张「用采集端代码回放原生响应实测的条数」表
（`gaussdb.Tps` 3、`gaussdb.QPS` 6、`Standby_status` 9、`Primary_status` 9、`Starttime/Uptimestr` 各 3、
`Service_availability_testing`/`PG_Service_Status`/`Status` 各 1）—— **与本审查方独立跑出来的数字逐项吻合**。
§3.2 顶部已加限定语：「**本节只适用于命令中心（B 类）通道。**」—— 首轮的「文档自相矛盾」已消除。

### 1.2 I-1 的独立核实过程（98 行全查，不抽查）

**(a) 生产调度实算**（`/tmp/w4r2_verify.py` 里调 `tpops_kafka_collector.schedule.compute_channel_plans()`）：

```
$ conda run -n tpops python /tmp/w4r2_verify.py <工作树>
I-1 独立核实：compute_channel_plans() 生产实算
  monitor          interval=30     tiers=(30, 60, 300)            metric_count=11
  command_center   interval=60     tiers=(60, 120, 300, 86400)    metric_count=82
  diagnostic       interval=120    tiers=(120, 300)               metric_count=4
  instance         interval=3600   tiers=(3600,)                  metric_count=1
  alarm            interval=30     tiers=()                       metric_count=0
```

**(b) 逐行核对文档的「实际产出频率」列**（`/tmp/w4r2_doccheck.py`，98 行逐行）：

```
逐指标行数: 98
I-1 全 98 行核对：文档「实际产出频率」 vs 生产 compute_channel_plans()
不一致行数: 0 / 98
文档「实际产出频率」取值分布: Counter({'1 分钟': 82, '30 秒': 11, '2 分钟': 4, '1 小时': 1})
文档「取数通道」取值分布:       Counter({'命令中心SQL': 82, '监控指标接口': 11, '诊断优化接口': 4, '实例接口': 1})
```

**0 处不符**。通道数（11 / 82 / 4 / 1）与生产 `metric_count` 也一致。

**(c) 「超服务」条数核实**：文档 §3.7 写「**65 个指标**的声明周期慢于其通道的实际节奏」。
本审查方按 `collect_cycle_seconds(rule.collect_cycle) > 通道周期` 独立算：

```
实算「声明周期慢于通道节奏」的启用指标数 = 65 / 98
```

**与文档一致**。

**(d) §3.7 的「通道内的声明周期分布」核实**（4 行全查）：

```
monitor          声明周期分布: {'5 分钟': 7, '1 分钟': 1, '60 秒': 2, '30 秒': 1}   (指标数 11)
command_center   声明周期分布: {'1 分钟': 29, '2 分钟': 15, '60 秒': 1, '24 小时': 2, '5 分钟': 35}   (指标数 82)
diagnostic       声明周期分布: {'5 分钟': 3, '2 分钟': 1}   (指标数 4)
instance         声明周期分布: {'1 小时': 1}   (指标数 1)
```

文档写「1 分钟、30 秒、5 分钟、60 秒 / 1 分钟、2 分钟、24 小时、5 分钟、60 秒 / 2 分钟、5 分钟 / 1 小时」
—— **4 行全部吻合**。

**(e) 体量数字核实**：文档写「单个实例**每天至少 152,664 条**」。
独立算：82×(86400/60) + 11×(86400/30) + 4×(86400/120) + 1×(86400/3600)
= 118080 + 31680 + 2880 + 24 = **152,664** ✅。

---

## 二、5 条新守卫的变异验证

**每条都在 `/tmp` 副本里改坏 → 跑 `tests/unit/test_delivery_docs.py` → 还原**。
基线：`34 passed in 0.64s`。

| 守卫 | 变异内容 | 结果 | 输出（摘要） |
|---|---|---|---|
| `test_a_class_collection_semantics_match_the_implementation` | **改生成器** `describe_collection`：A 类返回「节点级：每个 master 各出一条（不跨 master 合并、不求和）」+ 重生成 | **RED** ✅ | `AssertionError: A 类指标的归集口径没提「组件」：GAUSSDB.Number_of_ansactions_mmitted_r_cond …… assert '组件' in '节点级：每个 master 各出一条（不跨 master 合并、不求和）'` / `1 failed, 33 passed` |
| 同上 | **改生成器**：A 类返回「实例级：只出一条」+ 重生成 | **RED** ✅ | `assert '组件' in '实例级：只出一条'` / `1 failed, 33 passed` |
| 同上 | **改生成器**：A 类返回「组件级：所有组件求和后出一条」+ 重生成 | **GREEN** ❌ | `34 passed` —— **见 Important I-5** |
| `test_b_class_node_scoped_says_per_master` | **改生成器**：B 类 node_scoped 去掉「每个 master 各出一条」+ 重生成 | **RED** ✅ | `AssertionError: B 类 node_scoped 指标的归集口径没写「每个 master 各出一条」：gaussdb.Deadlocks` / `1 failed, 33 passed` |
| 同上 | **改生成器**：B 类改成「节点级：每个 master 合成一条（跨 master 求和）」+ 重生成 | **RED** ✅ | `assert '每个 master 各出一条' in '节点级：每个 master 合成一条（跨 master 求和）；各 master 返回值不同'` / `1 failed, 33 passed` |
| `test_actual_output_frequency_matches_the_production_schedule` | **改生成器**：「实际产出频率」列改写 `describe_cycle(rule)`（= 声明周期）+ 重生成 | **RED** ✅ | `AssertionError: 指标 gaussdb.{#NAME}.Number.lete.ples 的「实际产出频率」与生产调度代码算出的通道周期（60 秒）不一致` / `assert 120 == 60` / `1 failed, 33 passed` |
| `test_troubleshooting_table_only_quotes_existing_log_messages` | 部署说明排障表塞入**长且不存在**的日志「命令中心任务下发失败：重试后成功，已回滚本次采集窗口」 | **RED** ✅ | `AssertionError: 排障表引用了生产代码里不存在的日志文案：……（片段：命令中心任务下发失败）` / `1 failed, 33 passed` |
| 同上 | 塞入**首轮 I-4 的原句**「命令中心 …… 重试后成功」 | **GREEN** ❌ | `34 passed` —— **见 Important I-6** |
| 同上 | 塞入「命令中心 SQL 重试后成功」 | **GREEN** ❌ | `34 passed` —— **见 Important I-6** |
| 同上 | 塞入「重试次数用尽，放弃：这不是真实日志尾巴」（前半真实） | **RED** ✅ | `AssertionError: ……（片段：这不是真实日志尾巴）` / `1 failed, 33 passed` |
| `test_unconsumed_env_vars_are_disclosed_as_inert` | 删掉 `KAFKA_TOPIC_PARTITIONS` 的「注：当前实现未消费本项」 | **GREEN** ❌ | `34 passed` —— **见 Important I-7** |
| 同上 | 删掉 `CMD_LIMIT` 的「注：当前实现未消费本项」 | **RED** ✅ | `AssertionError: 配置项 CMD_LIMIT 没被生产代码消费，但部署说明没说明「改了也不生效」` / `1 failed, 33 passed` |
| 同上 | 同时删掉 `CMD_LIMIT` + `RETRY_MAX_ATTEMPTS` 的披露 | **RED** ✅ | 同上 / `1 failed, 33 passed` |

**小结**：5 条新守卫里 **3 条有牙**（B 类 master 语义、实际产出频率 vs 生产调度、排障日志（长文案）），
**2 条有洞**（A 类归集语义只做关键词匹配；空转变量只认得出 2/3 个）。

### 2.1 首轮那 6 条恒绿变异 —— 逐条重跑

| # | 首轮变异 | 首轮结果 | 本轮结果 | 判定 |
|---|---|---|---|---|
| M2 | 改真相源 `metric_rules.json`：`counter_delta` → `raw` + 重生成 | 绿（恒绿） | **绿** | ❌ **未闭环** → Important I-1 |
| M3 | 部署说明：把 `TPOPS_PASSWORD` 的「必填」从 是 改成 否 | 绿（恒绿） | **绿** | ❌ **未闭环** → Important I-2 |
| M4 | 部署说明：把 `POLL_INTERVAL_SECONDS` 的「改错的后果」改成完全错误的说法 | 绿（恒绿） | **绿** | ❌ **未闭环** → Important I-3 |
| M5 | 部署说明：删掉「4.2 优雅退出」整节 | 绿（恒绿） | **绿** | ❌ **未闭环** → Important I-4 |
| M9 | 部署说明：删掉 `CMD_LIMIT` 的「注：当前实现未消费本项」 | 绿（恒绿） | **红** | ✅ **已闭环**（新守卫 `test_unconsumed_env_vars_are_disclosed_as_inert` 抓到） |
| M10 | **改生成器** `describe_collection`：A 类也标注「合成一条」+ 重生成 | 绿（恒绿） | **绿** | ❌ **未闭环** → Important I-5 |

**6 条里只闭环了 1 条（M9）**，5 条仍恒绿。

---

## 三、幂等与一致性

### 3.1 生成器幂等

```
$ cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
$ conda run -n tpops python tools/build_delivery_docs.py --check
一致：docs/交付-不可提供指标清单.md
一致：docs/交付-客户指标英文名核对清单.md
一致：docs/交付-指标口径说明.md
exit=0

$ conda run -n tpops python tools/build_delivery_docs.py >/dev/null && git status --porcelain && git diff --stat
(以上为空即幂等)          ← 两处输出均为空
```

**三份文档仍「一致」；重跑生成器后工作树零改动** ✅

### 3.2 文档与生成器一致性 —— `test_docs_match_generator_output` 真能抓住手改

| 手改 | 结果 |
|---|---|
| 改「不可提供指标清单」里的一个指标名 | **RED**（4 条守卫红，含 `test_unsupported_doc_lists_exactly_disabled_metrics` / `test_other_docs_only_mention_known_metrics` / `test_docs_match_generator_output`） |
| 改「客户指标英文名核对清单」里一条英文名 | **RED**（4 条红，含 `test_ocr_entries_exist_in_customer_xlsx` / `test_ocr_checklist_matches_generator_review` / `test_docs_match_generator_output`） |
| 改「指标口径说明」里一个指标名 | **RED**（3 条红，含 `test_metric_spec_covers_all_enabled_metrics` / `test_docs_match_generator_output`） |
| 只改 §3.7 通道表的「监控指标接口 30 秒」→「5 分钟」 | **RED**（`test_docs_match_generator_output`） |

**三份生成文档的手改全部被抓** ✅
**但「部署运行说明」是手写文档（不由生成器产出）**，除「环境变量表 / 默认值 / 排障日志 / 空转变量」四条守卫外，
**其余内容完全无保护** —— 这正是 M3 / M4 / M5 三条恒绿的根源。

---

## 四、实测

全部为本审查方自己跑的命令输出（变异部分在 `/tmp` 副本里做）。

```
# 交付守卫
$ conda run -n tpops python -m pytest tests/unit/test_delivery_docs.py -q
34 passed in 0.64s

# 全量回归（/tmp/w4r2-mut 副本，含 .env.example + 客户 xlsx）
$ conda run -n tpops python -m pytest -q
1728 passed in 62.17s (0:01:02)

# py37 红线
$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
156 passed in 0.78s
（tests/test_py37_compat.py:25 SCAN_DIRS = ('tools', 'tests', 'data', 'tpops_kafka_collector')，
  SCAN_ROOT_FILES = True —— 本波新增的两个文件都在扫描范围内）

# ruff（合同门禁：tpops_kafka_collector/）
$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

# ruff（本波两个新文件，非门禁范围）
$ conda run -n tpops python -m ruff check tools/build_delivery_docs.py tests/unit/test_delivery_docs.py --output-format=concise | tail -2
tools/build_delivery_docs.py:1650:15: UP031 Use format specifiers instead of percent format
Found 181 errors.                        ← 首轮 146 条，本轮 181 条（UP031 为主，仓内既有风格）
$ conda run -n tpops python -m ruff check tools/build_delivery_docs.py --output-format=concise | grep F841
(无 F841 —— 首轮 Minor M-1 已修)

# 生成器 --check
$ conda run -n tpops python tools/build_delivery_docs.py --check
一致：docs/交付-不可提供指标清单.md
一致：docs/交付-客户指标英文名核对清单.md
一致：docs/交付-指标口径说明.md
（exit=0）

# 生成器幂等（重跑后 git diff 为空）
$ conda run -n tpops python tools/build_delivery_docs.py >/dev/null && git status --porcelain && git diff --stat
(空)

# OCR 清单结论数字独立复核
$ conda run -n tpops python /tmp/w4r2_ocr2.py
英文名列条目: 14 中文名列条目: 3
英文名列片段数: 34  中文名列片段数: 3  合计: 37
  中文名 [{#DATNAME}] Commited transactions             -> metric_code GAUSSDB.[{#DATNAME}]_committed_transactions
  中文名 ({#NAME})number of sequencial scan             -> metric_code GAUSSDB.{#NAME}.number.Of.sequential.scan
  中文名 ({#NAME})number of sequencial tuples read      -> metric_code GAUSSDB.{#NAME}.number.Of.quential.ples.ad
并集指标数: 16
101 - 16 = 85
（文档 §8 写「16 个指标 / 37 处片段（英文名列 34 处、中文名列 3 处）/ 其余 85 个」—— 逐项吻合）
```

---

## 五、发现

### Critical（0）

无。首轮的 C-1 已真正闭环 —— 本审查方**独立回放生产代码**核实 11/11 条 A 类行的消息条数与文档文案一致，
且 §3.2 / §3.5 / §5.2 三处不再自相矛盾。

### Important（7）

**I-1｜改真相源 `value_rule` 后守卫仍全绿（首轮 M2，未闭环）**

- **位置**：`tpops_kafka_collector/mapping/data/metric_rules.json` + `tools/build_delivery_docs.py`
- **变异**：`counter_delta` → `raw`（改 3 条）后重生成文档 → `34 passed`（全绿）。
- **后果**：守卫验的是「文档 == 生成器(映射表)」，**不验「文档 == 实现」**。
  把一条速率类指标改成「直接取累计值」，文档会**如实跟着变**（因为文档就是从映射表生成的），
  但**取值口径与实现就此不符**，而没有任何一条守卫会红。
  首轮的 I-3（`Total_pct` 备注错）正是这一类 —— 这次是靠 `NOTE_OVERRIDE` 人工覆写堵住的，
  但映射表里那条错的 `note` **仍然在**（`"活跃会话数 / 总连接数"`），下一波任何一次「统一两边措辞」的改动都会把它翻回去。
- **修法**：加一条守卫，把 `value_rule.type` 与生产 `mapping/rules_*.py` 里**实际支持的类型 + 分支语义**对上
  （例如 `ratio_across` 的分子 spec 必须与 `value_rule.note` 的口径描述一致；或对
  `activity_agg` 的分子取值断言「不含 state 过滤」）。

**I-2｜部署说明的「必填」列没有任何守卫（首轮 M3，未闭环）**

- **位置**：`docs/交付-部署运行说明.md` §3 各表第 3 列
- **变异**：把 `TPOPS_PASSWORD` 的「必填」从 `是` 改成 `否` → `34 passed`（全绿）。
- **后果**：客户照「必填=否」配环境，采集端启动即以退出码 2 退出（`main.py` 的 `EXIT_CONFIG_ERROR=2`）。
  现有守卫只覆盖「变量集合」与「默认值」，**必填性无人看守**。
- **本审查方独立核实**：当前文档是正确的 —— 标注「必填=是」的恰好 3 项
  （`TPOPS_USER` / `TPOPS_PASSWORD` / `KAFKA_BOOTSTRAP_SERVERS`），与 `config.py` 的
  `_require` / `_servers` 一致，30 项逐项无差。**文档对，守卫缺**。
- **修法**：`config_env_vars()` 的 AST 解析已经能拿到每个变量的默认值来源，把 `_require` / `_servers`
  的调用点也解析出来，守卫逐项比对文档的「必填」列。

**I-3｜`POLL_INTERVAL_SECONDS` 的「改错的后果」文案无守卫（首轮 M4，未闭环）**

- **位置**：`docs/交付-部署运行说明.md:128`
- **变异**：把「调小**不会**增加平台请求量 —— 未到期的通道不执行」改成
  「调小会让平台承受 10 倍压力并立刻触发限流封号」 → `34 passed`（全绿）。
- **后果**：I-6 刚把这段文案改对，**但没有任何东西钉住它**。这类「后果文案」正是首轮 I-3 / I-4 / I-5 / I-6
  四条 Important 的聚集区 —— 修复轮把 4 处文案改对了，却没给「后果文案」加任何判据，
  下一次编辑就能再写错，而**客户看的就是这段文案**。
- **修法**：给「改错的后果」列加**事实锚点**断言（例如「轮询间隔 ≠ 通道周期」这句话必须在；
  「拉长到整数倍」必须在；且**不得**出现「周期 = 轮询间隔」）。

**I-4｜删掉部署说明整节仍全绿（首轮 M5，未闭环）**

- **位置**：`docs/交付-部署运行说明.md` §4.2「优雅退出」
- **变异**：把 `### 4.2 优雅退出` 整节（直到 `## 5. 运行期文件`）删掉 → `34 passed`（全绿）。
- **后果**：「优雅退出」（在途最多等 30 秒 → 原子写水位 → flush/关闭 producer → 退出码 0）是
  **客户停机操作的核心契约**。手写文档删掉一整节，没有任何守卫会红。
  同样地，「退出码 0 / 2 / 3」「`/healthz` / `/readyz` 语义」「水位每轮落盘」这些首轮逐条核过、
  全部兑现的承诺，**现在都是无保护的散文**。
- **修法**：把首轮那 21 条「兑现核实」逐条变成守卫（每条一句关键事实必须出现在部署说明里），
  例如「`os._exit`」「30 秒」「水位」「`/readyz`」「`/healthz`」等锚点词。

**I-5｜A 类归集守卫只做关键词匹配，「合成一条」类语义错误仍能溜过去（首轮 M10，未闭环）**

- **位置**：`tests/unit/test_delivery_docs.py:629` `test_a_class_collection_semantics_match_the_implementation`
- **变异**：把生成器的 `COLLECTION_TEXT_MONITOR` 改成
  「组件级：所有组件求和后出一条」（= 首轮 M10 的「合成一条」同义改写）+ 重生成 → `34 passed`（全绿）。
- **根因**：守卫只做两条断言 —— `'组件' in text` 且 `'每个 master 各出一条' not in text`。
  「所有组件求和后出一条」既含「组件」、又不含「每个 master 各出一条」→ **两条断言都过**。
  守卫名叫「match the implementation」，但**从不回放实现**（本审查方回放时用的是
  `mapping.decode` + `collectors.monitor._sliced_context` + `mapping.rules.evaluate` 三步，见 §1.1）。
- **后果**：C-1 的**语义**（A 类是「逐组件各出一条」还是「合成一条」）没被守住；
  守卫只挡住了「每个 master 各出一条」这**一种**相反写法。
- **修法**：守卫改成**回放**：用 `data/metric_response/` 的原生响应跑生产
  `decode → _sliced_context → evaluate`，数出消息条数，与文档 §3.5 的实测表**逐条比对**
  （本审查方的回放器已验证可跑通，且 11 条全部与文档吻合）。

**I-6｜排障日志守卫对短文案直接跳过 —— 抓不住它自己声称要防的那条缺陷（I-4 的原句）**

- **位置**：`tests/unit/test_delivery_docs.py:727` `test_troubleshooting_table_only_quotes_existing_log_messages`
- **变异**：把首轮 I-4 的**原句**塞回排障表 ——
  「日志「命令中心 …… 重试后成功」」 → `34 passed`（全绿）。
  「日志「命令中心 SQL 重试后成功」」 → 同样全绿。
- **根因**：守卫取引号内**最长 ≥6 字**的中文连续片段，长度 <6 就归入 `unverifiable` 列表**直接跳过**。
  「命令中心 …… 重试后成功」的最长中文片段是「重试后成功」（**5 字**）→ 跳过；
  「命令中心 SQL 重试后成功」的最长片段也是 5 字 → 跳过。
  **换句话说：这条守卫抓不住首轮那条缺陷的原文。**
  守卫的 docstring 写「wave-4 首轮审查 I-4：排障表引用过一条代码里根本不存在的日志（「重试后成功」）——
  照着它去日志里搜永远搜不到」，但它恰恰漏掉了这一条。
- **次生问题**：正则 `日志(?:报|有)?[「]([^」]{6,})[」]` 要求引号前必须有「日志」二字；
  不带「日志」前缀的日志引用（变异 N4d）同样漏网。
- **修法**：去掉 6 字下限（或降到 4 字），改成「把引号内的中文片段**逐个**（而非只取最长）要求出现在
  生产字符串常量里」；同时放宽引号前的锚点（允许 `、`，`「` 等）。
  **注意**：本波守卫的 `_production_string_literals()` 已正确处理 py3.7 的 `ast.Str`
  （首轮修复轮自己发现并修的），这一条**不要动**。

**I-7｜空转变量守卫漏掉 `KAFKA_TOPIC_PARTITIONS` —— 恰好是 I-5 那一条**

- **位置**：`tests/unit/test_delivery_docs.py:797` `test_unconsumed_env_vars_are_disclosed_as_inert`
  与 `:830` `_unconsumed_config_keys`
- **变异**：把 `KAFKA_TOPIC_PARTITIONS` 的「**注：当前实现未消费本项**」删掉 → `34 passed`（全绿）。
- **根因**：守卫用「**config 的键名**在生产源码里（除 config.py 自身）有没有出现过」来判定
  「是否被消费」。`kafka_topic_partitions` 在 `producer.py:213` 出现过 ——
  但那一行在 `ensure_topics()` → `_create_topics()` 里，而 **`ensure_topics` 在生产路径里没有任何调用方**：

```
$ grep -rn "ensure_topics" tpops_kafka_collector/ --include=*.py
tpops_kafka_collector/kafka/producer.py:315:    def ensure_topics(self, admin: Optional[Any] = None) -> Tuple[str, ...]:
（只有定义，无调用方；只有 tests/integration/test_producer.py 调用）

$ grep -rn "kafka_topic_partitions" tpops_kafka_collector/ --include=*.py
tpops_kafka_collector/config.py:60:    kafka_topic_partitions: int = 4
tpops_kafka_collector/config.py:264:        'kafka_topic_partitions': _int(env, 'KAFKA_TOPIC_PARTITIONS', 4, minimum=1),
tpops_kafka_collector/kafka/producer.py:213:            partitions=config.kafka_topic_partitions,

$ conda run -n tpops python /tmp/w4r2_unconsumed.py
守卫算出的「未消费」变量: ['CMD_LIMIT', 'RETRY_MAX_ATTEMPTS']
```

  守卫算出的「未消费」集合**只有 2 个，不含 `KAFKA_TOPIC_PARTITIONS`** ——
  于是首轮 I-5 的正主**根本不在守卫视野里**。
- **后果**：文档当前是**对的**（`部署运行说明.md:121` 如实披露「当前实现未消费本项……」），
  但这是**人工写对的，不是守卫守住的**。改一次文档就悄悄回退。
  注：`CMD_LIMIT` / `RETRY_MAX_ATTEMPTS` 的披露**确实**被守住了（M9 变异红）——
  只是 3 个里的 2 个。
- **修法**：把「是否被消费」从**名字出现**升级为**可达性**判定（例如沿调用图判断
  `ensure_topics` 是否被 `main` / `lifecycle` / `executors` 触达；或反过来，
  对每个 config 键做一次「改值 → 生产输出是否变化」的可达性判定）。

### Minor（4）

| # | 位置 | 问题 | 修法 |
|---|---|---|---|
| M-a | `tests/unit/test_delivery_docs.py:629` | 守卫名 `..._match_the_implementation` 与行为不符（只查关键词，不回放实现）。名字会让人误以为已经对上了实现 | 改名，或按 I-5 的修法真的回放实现 |
| M-b | `tools/build_delivery_docs.py` | 新增文件的 ruff 错误从首轮的 146 条涨到 **181 条**（UP031 为主，非合同门禁范围）。**F841 已修**（首轮 M-1 闭环 ✅） | 与仓内既有风格对齐，或加 `# noqa` |
| M-c | `docs/交付-指标口径说明.md:128`（§3.7 表） | 「通道内的声明周期分布」把「1 分钟」与「60 秒」并列（同一档的两种单位写法），读者易误读为两档。**与真相源一致**（实算确有 `1 分钟` 1 条 + `60 秒` 2 条），非错误 | 加一句「单位沿用映射表原样」 |
| M-d | 修复轮报告 `wave4-repair-report.md` | 写「R074 的 **quential** 已列入」，实际列入的是 `**sequencial**`（修复报告笔误；**文档本身正确**） | 修报告措辞 |

---

## 六、结论

**verdict = fail**（Critical 0 / Important 7 / Minor 4）。

- **交付物本身已经对了**：9 条发现（C-1 + I-1…I-9）在**文档内容**上全部闭环，
  且本审查方**没有采信修复报告的任何数字**，全部自己跑生产代码核实：
  A 类回放 11/11 条消息条数吻合、通道周期实算 98/98 行吻合、超服务 65/98 吻合、
  体量 152,664 吻合、OCR 结论 16/37/85 吻合、必填 3 项吻合。
- **但修复的「耐久度」不够**：修复轮的核心任务被自己写成
  「审查方 16 条变异里 6 条改坏文档后守卫仍全绿，根因是守卫只验『文档 == 生成器(映射表)』，
  从不验『文档 == 实现代码』」。**实测：6 条只闭环了 1 条（M9）；5 条仍恒绿（M2/M3/M4/M5/M10）。**
  新增的 5 条守卫里，**2 条的洞恰好开在它们声称要守的那一类缺陷上**：
  - 排障日志守卫**抓不住首轮 I-4 的原句**（短文案被静默跳过）；
  - 空转变量守卫**认不出 `KAFKA_TOPIC_PARTITIONS`**（首轮 I-5 的正主）。
  另有 1 条（I-5）A 类归集守卫只做关键词匹配，「合成一条」类语义错误仍能溜过去 ——
  也就是 **C-1 的语义本身没被守住**。
- **按本项目自己的先例定级**：wave-3 首轮的 9 条 Important 里就有 3 条是「守卫缺失」，
  且被当作阻塞项；wave-3 终审的口径是「未闭环项 Important = 0」才判 pass。
  本轮**未闭环的 Important = 7**，因此判 fail。
- **阻塞项**：I-1 / I-2 / I-3 / I-4（首轮恒绿变异未闭环）、
  I-5（C-1 的语义无守卫）、I-6（I-4 的原句无守卫）、I-7（I-5 的正主无守卫）。
- **文档内容层面无阻塞** —— 客户拿到的四份文档现在是**正确**的；
  本轮要修的是**让修正不被静默改回去**。

---

## 收尾

签发 receipt 的命令：

```
ssf execution review changes/tpops-kafka-collector \
  --wave wave-4-delivery-docs --base 2650399 --head 9860bac \
  --report .superpowers/sdd/reviews/wave-4-delivery-docs-r2.md --verdict fail
```
