# Wave 1 任务简报 · 映射口径（wave-1-mapping）

**工作目录（唯一）**：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`
**分支**：`tpops_metric_dev`（已在 worktree 里；不要在主仓 `/mnt/c/sml/project/py_project/tpops_metric_dev` 改任何文件）
**Python**：`conda run -n tpops python ...`（conda env `tpops`，Python **3.7.9**）
**权威工件**（先读）：
- `changes/tpops-mapping-and-logging/execution-contract.md`（Intent Lock / Approved Behavior R1~R7,R18,R19 / Design Constraints / Test Obligations / Escalation Rules）
- `changes/tpops-mapping-and-logging/tasks.md`（任务 1.1~1.8 全文）
- `changes/tpops-mapping-and-logging/specs/mapping-and-rules/spec.md`（R1~R7）
- `changes/tpops-mapping-and-logging/specs/delivery-and-state/spec.md`（R18、R19）
- `changes/tpops-mapping-and-logging/design.md`（决策 8 / 9 / 10）

## 铁律（不可违反）

1. **Python 3.7.9**：禁海象 `:=`、`f"{x=}"`、字典合并 `d1 | d2`、内建泛型注解
   `list[str]`、`typing.Literal/Protocol/TypedDict/Final`、`functools.cached_property`、
   `importlib.metadata`、`math.prod`、`zoneinfo`、`str.removeprefix/removesuffix`。
2. **依赖锁不变**：本波次**不新增依赖**（loguru 是 wave-2 的事）。
3. **映射层表驱动**：客户口径一律来自 `tpops_kafka_collector/mapping/data/*.json`
   （运行期真相源）；仓库根 `data/mapping/` 是**同源副本**，改一侧必须同步另一侧
   （有现成的一致性测试会抓）。
4. **不要改客户 xlsx**：`高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` 是客户原件，**只读**。
5. 采集端单文件 **< 400 行**、函数 **< 50 行**；不可变 DTO；`logging` 不用 `print`；
   凭据只走环境变量、禁入日志。
6. **TDD：每个任务先写失败测试（RED），看到它失败，再实现（GREEN），最后重构。**
   不要先写实现再补测试。
7. 不要碰 wave-2/3/4 的内容（loguru、collector.sh、交付文档里除任务 1.8 指定的以外）。

## 目标（验收，全部机械可判定）

1. `tpops_kafka_collector/mapping/data/metric_rules.json` 恰好 **87** 条规则，
   **0** 条 `enabled=false`，`metric_code` 互不重复。
2. `tpops_kafka_collector/mapping/data/excluded_metrics.json` 恰好 **14** 条：
   **11 条** `reason="duplicate"`（各带 `kept` 指明保留哪条）、**3 条** `reason="unsupported"`。
   `metric_code` / `metric_name` / `reason` / `detail` 必填；`kept` 仅 duplicate 必填。
3. 未挂客户指标的事件类告警，`metric_code` / `metric_name` **全部等于同一个固定值**；
   挂靠了客户指标的告警**保持不变**。
4. 表级指标的 `{#NAME}` = `schemaname.relname`（如 `public.t1`）；
   按 `schemaname` + `relname` **两列**分组；`public.t1` 与 `other.t1` 的
   `metricName` / `metricCode` / 幂等键**都不同**。

## 明确清单（不要自己重新推导）

### A. 要排除的 11 条「重复的英文变体」（`reason=duplicate`）

每条的 `kept` = 对应的小写 `gaussdb.` 版本（**保留**那一条）：

| 要排除的 metric_code | metric_name（英文中文名） | kept（保留哪条） |
|---|---|---|
| `GAUSSDB.{#NAME}.Number.lete.ples` | `({#NAME}) number of delete tuples` | `gaussdb.{#NAME}.Number.lete.ples` |
| `GAUSSDB.{#NAME}.Number.ad.ples` | `({#NAME}) number of dead tuples` | `gaussdb.{#NAME}.Number.ad.ples` |
| `GAUSSDB.{#NAME}.Number.t.date.ples` | `({#NAME}) number of hot update tuples` | `gaussdb.{#NAME}.Number.t.date.ples` |
| `GAUSSDB.{#NAME}.number.Of.index.scan` | `({#NAME})number of index scan` | `gaussdb.{#NAME}.number.Of.index.scan` |
| `GAUSSDB.{#NAME}.number.Of.quential.ples.ad` | `({#NAME})number of sequencial tuples read` | `gaussdb.{#NAME}.number.Of.quential.ples.ad` |
| `GAUSSDB.{#NAME}.number.Of.dex.ples.ad` | `({#NAME})number of index tuples read` | `gaussdb.{#NAME}.number.Of.dex.ples.ad` |
| `GAUSSDB.{#NAME}.number.Of.insert.tuples` | `({#NAME})number of insert tuples` | `gaussdb.{#NAME}.number.Of.insert.tuples` |
| `GAUSSDB.{#NAME}.number.Of.live.tuples` | `({#NAME})number of live tuples` | `gaussdb.{#NAME}.number.Of.live.tuples` |
| `GAUSSDB.{#NAME}.number.Of.update.tuples` | `({#NAME})number of update tuples` | `gaussdb.{#NAME}.number.Of.update.tuples` |
| `GAUSSDB.{#NAME}.number.Of.sequential.scan` | `({#NAME})number of sequencial scan` | `gaussdb.{#NAME}.number.Of.sequential.scan` |
| `GAUSSDB.{#NAME}.garbage.Ratio` | `({#NAME})Garbage ratio` | `gaussdb.{#NAME}.garbage.Ratio` |

### B. 要排除的 3 条「无法提供」（`reason=unsupported`，无 `kept`）

| metric_code | metric_name | detail（理由） |
|---|---|---|
| `gaussdb.Fastpath_function_call` | `快路径函数调用的连接数` | PG 侧无 fastpath 计数器；pg_settings 仅有 gsi_fastpath_level 开关 |
| `gaussdb.Disabled_conn` | `禁用的连接数` | PG 无「禁用连接」概念；pg_settings 仅有 reserved_connections 等预留连接 |
| `gaussdb.sendAgentName` | `发送设备名给Agent` | Agent 侧行为，不是数据库可查询的监控指标 |

> 这 3 条当前在 `metric_rules.json` 里以 `enabled=false` 存在；本波次要**从生效口径里彻底去掉**
> （不是留着 disabled）。`dropped_unsupported` 从此恒为 0。

### C. 告警统一名（拟值，已登记待客户确认）

在 `tpops_kafka_collector/mapping/data/alarm_rules.json` 增加一段：

```json
"event_metric": {"metric_code": "gaussdb.alarm", "metric_name": "数据库告警"}
```

生成器对**未挂客户指标**（`linked_customer_metric` 为空）的 254 条事件类告警，
`metric_code` / `metric_name` 统一取该值；3 条挂靠 `gaussdb.Tps` 的**保持不变**。
同时在 `tpops_kafka_collector/mapping/data/open_items.json` 登记该统一值待客户确认。

## 已知实现落点（已核实，可直接用）

1. **生成器** `tools/build_itscada_mapping.py`：
   - `DIMENSION_COLUMN_BY_SQL`（约第 79 行）里的
     `'stat_user_tables': {'NAME': 'relname'}` → 改成 `'schemaname.relname'`；
   - `group_by` 是从 `dimension_bindings` 的值派生的（约第 420 行
     `group_by = sorted(set(bindings.values()))`）——**只改绑定会让 `group_by` 变成一个
     叫 `schemaname.relname` 的单列，分组失效**。必须同时改这处派生逻辑，
     把点号串**拆成多列**（`['schemaname', 'relname']`）。
   - 生成 `metric_rules.json` / `customer_metric_mapping.json` 时**跳过排除清单里的指标码**。
   - 生成告警定义时套用 `event_metric`。
2. **映射层** `tpops_kafka_collector/mapping/rules_context.py`：
   - `row_dimensions`：让 `dimension_bindings` 的值支持「点号连接的多个列名」
     （按 `.` 连接各列的值）；
   - `grouped_items`：让 `group_by` 支持多列分组。
3. **加载与校验**：`tpops_kafka_collector/mapping/loader.py`（加载排除清单）、
   `tpops_kafka_collector/mapping/validation.py`（校验：排除清单的码一个都不在生效映射表中、
   生效规则 `metric_code` 唯一、生效规则数 = 87）。
   **告警定义的 `metric_code` 唯一性校验要放宽**（唯一性只对指标规则要求）。
4. **合并 SQL 不用改**：合并视图 `stat_user_tables` 的实测返回列**已含 `schemaname`**
   （见 `mapping/data/observed_columns.json`），数据是现成的。
5. **交付文档**：`tools/build_delivery_docs.py` 从映射数据生成三份文档
   （`docs/交付-指标口径说明.md` 87 条、`docs/交付-不可提供指标清单.md` 3 条并标注
   「已从生效口径移除」+ 理由、`docs/交付-客户指标英文名核对清单.md`）——
   它也要读排除清单。

## 任务清单（tasks.md 1.1~1.8 全文要点）

- **1.1** 建排除清单数据文件：`包内/excluded_metrics.json` + `仓库根/excluded_metrics.json`
  同源副本；新增 `tests/unit/test_excluded_metrics.py`（14 条 / 11-3 分布 /
  duplicate 必带 kept / 两份一致）。
- **1.2** 生成器按清单剔除；在 `test_excluded_metrics.py` 补断言（87 条、0 disabled、
  code 唯一）。
- **1.3** `loader.py` + `validation.py` 加载与校验；新增 `tests/unit/test_metric_catalog.py`
  （三项校验的正例与反例：人为塞一条被排除的码 → 必须校验失败并打印差异）。
- **1.4** `rules_context.py` 支持点号组合绑定与多列分组；
  新增 `tests/unit/test_dimension_binding.py`（组合绑定、多列分组、单列绑定不回归）。
- **1.5** 生成器 + 规则改绑 `schemaname.relname`；在 `test_metric_catalog.py` 补断言
  （表级规则一律按两列分组）。
- **1.6** 在 `test_dimension_binding.py` 补幂等不碰撞用例（`public.t1` vs `other.t1` 键不同；
  同表重复采集键相同）。
- **1.7** 告警统一指标名 + 放宽告警 `metric_code` 唯一性校验 + `open_items.json` 登记；
  新增 `tests/unit/test_alarm_metric_unification.py`。
- **1.8** 重生成三份交付文档 + 更新 `docs/映射表-ITSCADA字段对照.md` 与
  `tpops_kafka_collector/mapping/README.md`；新增 `tests/unit/test_delivery_docs.py`
  （文档条数与生效映射表一致：87 / 3）。

## 证明命令（每个任务跑自己的；收尾跑全套）

```bash
cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
conda run -n tpops python -m pytest tests/unit/test_excluded_metrics.py \
    tests/unit/test_dimension_binding.py tests/unit/test_alarm_metric_unification.py \
    tests/unit/test_metric_catalog.py tests/unit/test_delivery_docs.py -q
conda run -n tpops python -m pytest -q          # 全量必须全绿
conda run -n tpops python -m ruff check tpops_kafka_collector tests tools
conda run -n tpops python -m pytest tests/test_py37_compat.py -q
```

**全量 pytest 必须全绿**（现有约 1700+ 例）；若某个既有测试因为口径变化而失败，
**修测试或修实现以匹配新口径**，SHALL NOT 删测试、SHALL NOT 放宽断言来糊过去；
若确实需要改既有测试的期望值，在报告里逐条说明理由。

## 交付

1. 提交代码（conventional commits，中文正文；**不要**加 Co-Authored-By/署名行）。
2. 写实现报告到
   `changes/tpops-mapping-and-logging/.superpowers/sdd/wave-1-implementer-report.md`，
   内容含：每个任务做了什么、RED 证据（哪个测试先失败了、失败信息）、
   GREEN 证据（命令 + 通过数）、改动文件清单、遇到的问题与你的判断、
   以及你认为**需要 reviewer 特别注意**的点。
3. 不要修改 `execution-contract.md` / `tasks.md` / `proposal.md` / `specs/` /
   `design.md`（那是已批准的规划工件）。
4. 有拿不准的地方**先问**，不要猜。
