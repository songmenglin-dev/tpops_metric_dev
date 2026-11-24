# Wave 4 终审复审报告（守卫电池）—— wave-4-delivery-docs

- **base** `9860bac` / **head** `2a08501`
- **结论**：verdict = **fail**
- **Critical / Important / Minor 计数**：**0 / 2 / 4**

> 复审范围：只验「修复轮 2 新增/修改的守卫能不能抓住那 8 条曾经守不住的变异」。
> 上一轮结论已写明「9 条文档发现全部闭环」——本轮**未重验**这 9 条，
> 只按要求抽查了 3 条（C-1 / I-1 / I-5），结果见「快检」一节。
>
> **成本提示**：本次会话累计成本已达 $80+（PostToolUse hook 连续告警 COST CRITICAL）。
> 报告本身已按要求收敛，未再扩散。

---

## 0. 变异电池的搭建与基线

拷贝（含仓库根的 `.env.example` 与指标库 xlsx，避免拷贝污染）：

```bash
$ mkdir -p /tmp/w4r3 && cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
$ cp -a tpops_kafka_collector tests tools docs pytest.ini data \
      '高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx' .env.example /tmp/w4r3/
$ cd /tmp/w4r3 && find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null
```

拷贝完整性核对（确认 `.env.example` 与 xlsx 都在）：

```
$ ls -a /tmp/w4r3
.
..
.env.example
data
docs
pytest.ini
tests
tools
tpops_kafka_collector
高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx
```

**基线（无变异）—— 全绿**：

```
$ conda run -n tpops python -m pytest -q tests/unit/test_delivery_docs.py
........................................                                 [100%]
40 passed in 0.98s
```

基线绿 ⇒ 后面任何红都是变异引起，不是拷贝污染。
（另外用 pristine 副本做还原源：每条变异前 `rm -rf /tmp/w4r3 && cp -a /tmp/w4r3_pristine /tmp/w4r3`。）

---

## 1. 变异电池

### 1.1 任务指定的 8 条

| # | 变异 | 变红的用例 | 判定 |
|---|---|---|---|
| **M2** | 真相源 `tpops_kafka_collector/mapping/data/metric_rules.json` 里 `gaussdb.Deadlocks` 的 `value_rule.params.column`：`deadlocks` → `deadlocks_bogus` | `test_value_rule_column_matches_the_mapping_table`（主判据）+ `test_metric_spec_value_rules_are_described` + `test_docs_match_generator_output` + `test_generator_is_idempotent` + `test_actual_output_frequency_matches_the_production_schedule` —— **5 failed, 35 passed** | ✅ **红** |
| **M3** | `docs/交付-部署运行说明.md` 的 `TPOPS_PASSWORD` 行「必填」列 `**是**` → `否` | `test_deploy_required_flags_match_the_config_source` —— **1 failed, 39 passed** | ✅ **红** |
| **M4** | 同文档某处 `退出码 2` → `退出码 4`（非加粗写法，见 Important 2） | `test_deploy_exit_codes_exist_in_the_production_code` —— **1 failed, 39 passed** | ✅ **红**（但只覆盖非加粗写法） |
| **M5** | 同文档把**标题行含「优雅退出」的整节**（`### 4.2 优雅退出` 标题 + 正文共 25 行）删掉 | **无 —— 40 passed** | ❌ **绿（洞）** |
| **M9** | 同文档 `CMD_LIMIT` 行的「未消费」字样去掉（`注：当前实现未消费本项` → `注：当前实现已忽略本项`） | `test_unconsumed_env_vars_are_disclosed_as_inert` —— **1 failed, 39 passed** | ✅ **红** |
| **M10** | `tools/build_delivery_docs.py` 的 `COLLECTION_TEXT_MONITOR` → `组件级：所有组件求和后一条；` | `test_docs_match_generator_output` —— **1 failed, 39 passed** | ✅ **红** |
| **洞1** | 排障表里把日志引用换成「命令中心 SQL 重试后成功」（代码里没有这条日志） | `test_troubleshooting_log_quotes_check_every_fragment` —— **1 failed, 39 passed** | ✅ **红** |
| **洞2** | 同文档 `KAFKA_TOPIC_PARTITIONS` 行的「未消费」字样去掉 | `test_unconsumed_env_vars_are_disclosed_as_inert` —— **1 failed, 39 passed** | ✅ **红** |

**8 条里 7 条真红，1 条（M5）仍绿。**

### 1.2 M5 的两个变体（用于定位责任）

| 变体 | 注入 | 结果 |
|---|---|---|
| **M5a**（= 任务里的 M5） | 删掉 `### 4.2 优雅退出` **标题行 + 全部正文**（25 行），保留 `## 4. 启动与优雅退出` 大节标题 | `40 passed` → ❌ **绿** |
| **M5b** | 删掉**整个** `## 4. 启动与优雅退出`（含 `## 4` 标题行、`### 4.1 启动`、`### 4.2 优雅退出`，共 50 行） | `test_deploy_required_sections_are_present` 红：`AssertionError: 部署说明缺少必备小节（标题行里找不到）：优雅退出` —— 1 failed, 39 passed → ✅ 红 |

M5a 的注入与实际输出：

```
################ MUTATION M5 ################
注入: 删掉 §4.2「优雅退出」小节（保留 ## 4 标题），正文 25 行
........................................                                 [100%]
40 passed in 1.00s
```

M5b 的实际输出：

```
################ MUTATION M5b ################
注入: 删掉整个 §4「启动与优雅退出」（含 ## 4 标题），正文 50 行
E               AssertionError: 部署说明缺少必备小节（标题行里找不到）：优雅退出
E               assert not ['优雅退出']
tests/unit/test_delivery_docs.py:1065: AssertionError
FAILED tests/unit/test_delivery_docs.py::test_deploy_required_sections_are_present
1 failed, 39 passed in 1.04s
```

### 1.3 额外探针（不在指定电池里，用来界定 Important 的影响面）

| 变体 | 注入 | 结果 |
|---|---|---|
| **M4b** | 把 **加粗** 的 `退出码 **2**`（文档第 101 行「必填项缺失或取值非法 → 启动即失败（不发任何请求），退出码 **2**」）改成 `退出码 **4**` | `40 passed` → ❌ **绿**（Important 2） |
| **M3b** | `TPOPS_PASSWORD` 必填改错 **且** 把表格首格的反引号去掉（`| TPOPS_PASSWORD |` ） | `40 passed` → ❌ **绿**（Minor 1：守卫静默空转） |
| **M5c** | 只删掉 `### 4.2 优雅退出` **标题行**、保留正文 | `40 passed` → ❌ **绿** |
| **M-env** | 删掉整个 `## 3. 环境变量`（76 行） | 4 red，但 section 守卫报的是 **「状态目录」缺失** 而不是「环境变量」缺失 → 说明「环境变量」是被代码围栏里的 shell 注释顶上的（Minor 4 / Important 1(b) 的实证） |
| **M-files** | 删掉整个 `## 5. 运行期文件`（24 行，「状态目录」内容的所在地） | `40 passed` → ❌ **绿**（Important 1(c)） |

---

## 2. 快检

### 2.1 9 条文档发现没有回退（抽查 3 条）

**C-1（A 类归集口径）** —— 口径说明里 11 条 A 类（监控指标接口）行的「归集口径」列：

```
A 类行数 11
  `GAUSSDB.Number_of_ansactions_mmitted_r_cond` | 组件级：逐组件各出一条（每条 series 一条消息，`COMPONENT_ID` 归入 `tags`，不跨组件合成）
  `GAUSSDB.Number_of_llback_ansactions_r_cond`   | 组件级：逐组件各出一条（……不跨组件合成）
  `GAUSSDB.Service_availability_testing`         | 组件级：逐组件各出一条（……不跨组件合成）
  `gaussdb.Tps` | `gaussdb.QPS` | `GAUSSDB.PG_Service_Status` | `gaussdb.Starttime`
  `gaussdb.Uptimestr` | `gaussdb.Status` | `GAUSSDB.Standby_status` | `GAUSSDB.Primary_status`
                                              —— 全部同上
整篇口径说明里出现的禁用词: []
```

11/11 均为「组件级：逐组件各出一条」，且「合成一条 / 求和后 / 合并成一条 / 汇总为一条 / 聚合成一条 / 求和出一条 / 合并后一条」**0 命中** → ✅ **C-1 未回退**。

**I-1（98 行「实际产出频率」全量核对）** —— 与生产代码 `tpops_kafka_collector.schedule.compute_channel_plans()` 逐行比对：

```
总行数 98
checked(与通道周期比对) 98 mismatch []
未比对(无通道/通道未知) 0 []
enabled 规则数 98
enabled 未出现在文档中的 []
```

**98/98 全部核到、0 处不符、0 行漏核** → ✅ **I-1 未回退**。

**I-5（三个空转变量都披露「改了也不生效」）**：

```
| `KAFKA_TOPIC_PARTITIONS` | `4` | 否 | Topic 分区数，取值 ≥ 1 | **注：当前实现未消费本项** —— 生产者**不创建 Topic**……**改这个值对当前运行行为没有任何影响** |
| `CMD_LIMIT`              | `1000` | 否 | 单条命令的 `limit` 行数，取值 1~1000（平台硬上限） | 改小于实际行数 → **静默截断**……注：当前实现未消费本项 |
| `RETRY_MAX_ATTEMPTS`     | `5` | 否 | 通用重试最大尝试次数，取值 ≥ 0 | 注：当前各调用点的重试次数由各自配置固定……本项**尚未被消费**，改动不影响当前运行行为 |
```

三处都写明了「未消费 / 尚未被消费 / 改动不影响当前运行行为」 → ✅ **I-5 未回退**。

### 2.2 门禁

```
$ conda run -n tpops python -m pytest -q
1734 passed in 82.34s (0:01:22)

$ conda run -n tpops ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest -q tests/test_py37_compat.py
156 passed in 1.34s

$ conda run -n tpops python tools/build_delivery_docs.py --check
一致：docs/交付-不可提供指标清单.md
一致：docs/交付-客户指标英文名核对清单.md
一致：docs/交付-指标口径说明.md
exit=0
```

四条门禁**全部通过**（`--check` 为只读模式，不写文件）。

---

## 3. 新守卫的误报评估

**结论：head 上没有引入「当前正确文档被判红」的误报**——全套 1734 用例在 head 上全绿，
`tests/unit/test_delivery_docs.py` 40/40 全绿，且 `build_delivery_docs.py --check` 一致。
以下是**面向未来的**误报风险（不构成当前 fail 的理由，但需要记在账上）：

| 守卫 | 误报风险 | 严重度 |
|---|---|---|
| `test_value_rule_column_matches_the_mapping_table` | 判据是**字节级相等**（文档「取值规则」列 == `describe_value_rule(value_rule)`）。只要在那一列里加任何补充说明（单位、例外），就会判红。当前覆盖 98/98 行，但这条列是机器生成的，且 `NOTE_OVERRIDE` 目前只覆写「备注」列 —— 一旦将来给「取值规则」列加覆写/补注，本守卫会先于正确性红。 | Minor（设计取舍，与既有 `test_docs_match_generator_output` 同源） |
| `test_troubleshooting_log_quotes_check_every_fragment` / `test_troubleshooting_table_only_quotes_existing_log_messages` | 两条都以 `assert quoted, '排障表里一条日志引用都没解析到 —— 表格格式变了？'` 起头。文档一旦把「日志「xxx」」改成别的引号/加词（例如「日志输出「xxx」」），**正确文档会被判红**。 | Minor |
| `test_deploy_required_flags_match_the_config_source` | `_required_env_vars()` 用 AST 只认 `_require(...)` / `_servers(...)` 两个函数名。将来 config.py 换别的取参方式（如 `_int(..., required=True)`），真正的必填项会被误判成非必填 → 文档写「是」反而红。 | Minor |
| `KNOWN_INERT_ENV_VARS = ('KAFKA_TOPIC_PARTITIONS',)` | 这是**手工登记**的「读了但没效果」变量。将来采集端若真的开始校验分区数并消费该变量，守卫不会提醒要摘掉登记，反而会一直要求文档写「未消费」→ 正确文档被判红。 | Minor |
| `test_a_class_collection_forbids_synthetic_aggregation` | 禁用词表是**子串**匹配（`合成一条`/`求和后`/…）。若将来为了澄清写成否定句「不会合成一条」「不求和后一条」，会被误判。 | Minor |

---

## 4. 发现

### Critical（0）

无。

### Important（2）

#### Important 1 —— M5 **未闭环**：`test_deploy_required_sections_are_present` 只查「标题行含关键词」

**位置**：`tests/unit/test_delivery_docs.py::test_deploy_required_sections_are_present`（第 1046 行起）
**复现**：删掉 `### 4.2 优雅退出` 整节（标题行 + 25 行正文）→ `40 passed`，守卫全绿。

```python
headings = [
    line.lstrip('#').strip()
    for line in text.splitlines()
    if line.startswith('#')          # ← 洞 (b)
]
required_sections = ('环境变量', '启动', '优雅退出', '状态目录', '健康检查', '日志', '排障')
missing = [name for name in required_sections
           if not any(name in heading for heading in headings)]   # ← 洞 (a)(c)
```

三个叠加的洞：

- **(a) 子串匹配太松**：`## 4. 启动与优雅退出` **一个标题同时满足「启动」和「优雅退出」两个必备项**。
  于是删掉 `### 4.2 优雅退出` 整节（正是本任务 M5 的字面要求）时，「优雅退出」仍被umbrella 标题顶住 → 绿。
- **(b) `startswith('#')` 把代码围栏里的 shell 注释当成了章节标题**。实测：

  ```
  代码围栏内的 shell 注释被当成标题的行:
      # 1) 拷贝产物（连同 requirements.txt）
      # 2) 准备 Python 3.7.9 环境
      # 3) 准备环境变量（30 项完整清单见 §3，逐项抄进 .env 即可）
      # 4) 启动
      # 停机要给足时间：收尾（等在途请求 + 落水位 + flush Kafka）最长 30 秒
  ```

  ⇒ 「环境变量」「启动」两个必备项**都被 shell 注释满足**。实证：删掉整个 `## 3. 环境变量`（76 行）后，
  section 守卫报的是 **`状态目录` 缺失**而不是 `环境变量` 缺失 —— 说明「环境变量」是被 `# 3) 准备环境变量…` 顶上的。
- **(c) 只查标题存在、不查正文**：删掉整个 `## 5. 运行期文件`（24 行，「状态目录」内容的所在地）→ `40 passed`。

  为什么 M5b（删整个 `## 4`）会红？因为它同时干掉了 `优雅退出`（umbrella 标题也一起没了）**和** `### 4.1 启动`；
  而 `启动` 这个必备项仍被 `# 4) 启动` 顶住，所以报错里只列了 `优雅退出`。换句话说，**这条守卫对「启动」的覆盖
  从来就没生效过**。

**建议**：判据改成「标题行**去掉编号前缀后**等于必备名（如 `4.2 优雅退出` → `优雅退出`）+ 该标题下正文非空
+ 解析标题时跳过 ``` 围栏内的行」。

#### Important 2 —— M4 **只闭环了一半**：退出码守卫的正则吃不下加粗写法

**位置**：`tests/unit/test_delivery_docs.py::test_deploy_exit_codes_exist_in_the_production_code`（第 1025 行起）
**判据**：`re.findall(r'退出码\s*(\d+)', text)`

`退出码\s*(\d+)` 匹配不到 `退出码 **2**`（`**` 挡在数字前）。
而文档里**最显眼的退出码声明恰恰是加粗的**：

```
docs/交付-部署运行说明.md:101:  必填项缺失或取值非法 → **启动即失败**（不发任何请求），退出码 **2**。
docs/交付-部署运行说明.md:169:  | `HEALTH_PORT` | `8088` | 否 | 健康端点监听端口，取值 1~65535 | 端口被占用 → 装配失败，退出码 **3** |
```

§4.1 的退出码表用的又是 `` | `2` | `` 这种形式，同样检不到。
**实证**：把 `退出码 **2**` 改成 `退出码 **4**` →

```
################ MUTATION M4b ################
注入: 退出码 **2** -> 退出码 **4**（加粗形式）
40 passed in 0.99s
```

⇒ M4 在「普通写法」上闭环，在「加粗写法」上**仍然守不住**。

**建议**：正则改成 `退出码\s*\**\s*(\d+)`，并把退出码表里的 `` | `N` | `` 行也纳入核对
（同时保持「出现的退出码 ⊆ 代码里的 `EXIT_*` 集合」这一不变式）。

### Minor（4）

- **Minor 1 —— M3 守卫对表格格式敏感，可能静默空转。**
  `_deploy_env_rows()` 要求首格以反引号开头（`cells[0].startswith('`')`），且不校验行数非空。
  实证（M3b）：`TPOPS_PASSWORD` 必填列改错 **且** 首格去掉反引号 → `40 passed`，守卫完全没跑。
  建议：改用 `parse_deploy_env_table()`（它用正则 `^[A-Z][A-Z0-9_]+$`，不依赖反引号），并加 `assert rows`。

- **Minor 2 —— 两条日志守卫对排障表格式硬断言**（`assert quoted, '……表格格式变了？'`）。
  文档格式一变（引号/「日志」后加词），**正确文档会被判红**。

- **Minor 3 —— 必填判定靠 AST 认函数名**（`_require` / `_servers`）。
  config.py 换取参方式后，真正的必填项会被当成非必填 → 文档写「是」反而判红。

- **Minor 4 —— `KNOWN_INERT_ENV_VARS` 手工登记**（`KAFKA_TOPIC_PARTITIONS`）。
  该变量将来被真正消费时，守卫不会提醒摘登记，反而会一直要求文档写「未消费」→ 正确文档判红。

---

## 5. 收尾

- 只读约束遵守情况：**未** `git commit` / `git add` / `git checkout` / `git reset`，**未**移动 HEAD 或分支；
  **未**改动 `tests/` 与 `tpops_kafka_collector/`（连临时都不行 —— 所有变异都在 `/tmp/w4r3` 副本里做）；
  工作树内只新增本报告这一个文件。
- `/tmp/w4r3`、`/tmp/w4r3_pristine` 及变异脚本已全部删除。

签发 receipt 的命令：

```
    ssf execution review changes/tpops-kafka-collector \
      --wave wave-4-delivery-docs --base 9860bac --head 2a08501 \
      --report .superpowers/sdd/reviews/wave-4-delivery-docs-r3.md --verdict fail
```
