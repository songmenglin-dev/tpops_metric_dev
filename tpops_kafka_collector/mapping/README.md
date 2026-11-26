# ITSCADA 映射规则

把 TPOPS 取到的数据变成 ITSCADA 约定的 Kafka JSON。**规则是数据不是代码** ——
客户改字段口径，改 `tpops_kafka_collector/mapping/data/` 下的 JSON，不用改 Python。

## 依据

| 来源 | 作用 |
|---|---|
| `docs/ITSCADA第三方指标数据接入对接方案V2.docx` | 指标消息的字段集合与语义（§6.2 字段表、§7 指标表模板） |
| `docs/ITSCADA第三方告警数据接入对接方案V2.docx` | 告警消息的字段集合与语义（§3.2 字段表、§4 压缩规则） |
| `高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` / 指标库 | 101 个客户指标的规范：中文名、英文名、数据类型、周期、单位（**只读**，不因排除清单改动） |
| `docs/开发方案-GaussDB指标与告警采集.md` §4.3 | 每个指标走哪条取数通道、怎么从原始值算出客户值 |

## 文件布局

```
tpops_kafka_collector/mapping/   # 生产代码：规则数据 + 执行代码（随包发布）
├── data/                     # 规则数据（表驱动）—— **生产环境的唯一真相源**
│   ├── metric_rules.json     # 业务规则：87 条生效指标规则（全部 enabled）
│   ├── excluded_metrics.json # 排除清单：14 条被排除的客户指标（11 重复 + 3 无法提供）
│   ├── response_rules.json   # 取数定位：响应体的哪一段哪一列（87 条 + 18 种形状）
│   ├── kafka_field_mapping.json  # 字段映射：值 → Kafka 字段（87 条指标 + 4 种告警形态）
│   ├── alarm_rules.json      # 告警规则：等级/状态映射 + 257 条告警定义
│   │                         #   （`event_metric` 段 = 未挂客户指标的统一指标名）
│   ├── itscada_field_map.json  # 客户消息的字段清单（哪些字段、必填与否）
│   ├── merged_sql.json       # 11 个合并 SQL 视图的定义 + 刷新档位 + 分页段
│   ├── sql_merge.sql         # 合并 SQL 的归档留档（与生成器逐字符比对）
│   ├── customer_metric_mapping.json  # 87 生效指标 → 可提供性判定（独立基准）
│   ├── observed_columns.json # 实测留档的实际返回列（列名校验用，生成物）
│   └── open_items.json       # 待客户确认的 12 项
├── __init__.py               # 对外接口
├── table.py                  # 加载与查表（MappingTable / MetricRule / AlarmDefinition）
├── rules.py                  # 取值规则引擎的对外接口 + 文档（13 种 value_rule.type）
├── rules_context.py          #   上下文 / 异常 / 数值·过滤·维度工具
├── rules_deltas.py           #   累计计数器的差分基线（首轮只建基线、归零记 ERROR）
├── rules_naming.py           #   占位符替换、资源名拼装
├── rules_sorting.py          #   PostgreSQL ORDER BY 口径的排序（top_n 用）
├── rules_row.py              #   raw / raw_filtered / aggregate / raw_rows
├── rules_series.py           #   series_point / enum_map / gauge
├── rules_cross.py            #   counter_delta / ratio_across
├── rules_collection.py       #   top_n / collection_count / text_prefix_count /
│                             #     instance_field
├── rules_engine.py           #   分派表（value_rule.type → 执行函数）
├── decode.py                 # response_body → RuleContext（按 shape + locate 解包，
│                             #   含合并 SQL 的合并形态 decode_merged / merge_pages）
├── columns.py                # 列名校验（规则要取的列必须存在于实测留档的实际返回列）
├── ids.py                    # 标识与幂等键
├── message.py                # 按 ITSCADA 字段规范组装消息
├── masking.py                # 慢 SQL 文本脱敏与截断
├── nodes.py                  # 节点身份解析（resourceName：节点 IP + 节点 ID；选 master CN）
└── sqlgen.py                 # 合并 SQL 的运行时渲染（按 offset 翻页）

data/mapping/                 # 仓库根的**同内容副本**（离线工具与文档生成用）
└── archive/                  # 旧的分类字段映射表（已归档，见该目录 README）

tools/build_metric_value_rules.py   # 开发方案 §4.3 → data/metric_value_rules.json（生成物，不入库）
tools/build_itscada_mapping.py      # 中间件 + xlsx → data/mapping/*.json
tools/build_response_rules.py       # 实测响应 + metric_rules → data/mapping/response_rules.json
tools/build_kafka_mapping.py        # 实测响应 → data/mapping/kafka_field_mapping.json
tools/build_field_mapping_doc.py     # 分类映射文件 → docs/映射表-ITSCADA字段对照.md
tests/                               # 映射层单元测试（pytest）
```

> **映射数据已经搬进包内**（设计决策 9）：生产环境的**唯一真相源**是
> `tpops_kafka_collector/mapping/data/*.json` —— 采集端**不读仓库根目录**，
> 只按 `__file__` 解析包内路径（常驻进程可能从任意工作目录启动）。
> 仓库根的 `data/mapping/*.json` 是**同内容副本**，留给离线工具与文档生成用；
> 两份副本的一致性由测试守（`tests/unit/test_mapping_data_sync.py`）。
> `tools/` 与 `data/` 是离线工具与留档，**不是生产依赖**。

`metric_rules.json` 与 `alarm_rules.json` 是**生成物**，不要手改：

```bash
python tools/build_metric_value_rules.py   # 开发方案 §4.3 → data/metric_value_rules.json（生成物）（生成物，不入库）
python tools/build_itscada_mapping.py      # 上述中间件 + xlsx → data/mapping/*.json
```

改口径的正确做法：改开发方案 §4.3 的表（人类可读的那份）→ 重新跑两个脚本 →
提交 `data/mapping/*.json` **与包内副本**
`tpops_kafka_collector/mapping/data/*.json`（`data/metric_value_rules.json`
是生成物，不入库；缺失时 `build_itscada_mapping.py` 会自动重建）。
`tests/test_generated_rules.py` 会检查生成物与生成器一致。

用法：

```python
# 包已从仓库根导入，不需要再改 sys.path
from tpops_kafka_collector.mapping.loader import load_mapping

bundle = load_mapping()          # 默认读包内 mapping/data/（生产路径）
rule = bundle.metric_rule('GAUSSDB.[{#DATNAME}]_cache_hit_ratio')
```

> 离线脚本用 `MappingTable.load(data_dir)` —— `data_dir` **必填、无默认**
> （它要读只在仓库根有的 `archive/` 与合并 SQL 探针留档）。**生产路径禁止用它**。

## 分类字段映射文件（已归档）

> **已归档到 `data/mapping/archive/`（2026-11-13）** —— 见
> [归档说明](data/mapping/archive/README.md)。这套表按类别组织，
> `value` 字段的来源写的是散文（"result → 行数组 → metric_rules[].value_rule 指定的列"），
> 查一个指标要跨三个文件跳。现由 **`kafka_field_mapping.json`** 取代：逐指标列出
> 「响应的某个值 → Kafka 的某个字段」。

每个采集分类一个 JSON，里面只有两段：

```json
{
  "category": "A",
  "name": "监控指标接口",
  "api": "POST /rds/v3/monitor/metrics",
  "tpops_fields": [
    {"field": "metric_infos[].metrics[].series", "desc": "维度键 → 数值数组，值可能是 null"}
  ],
  "field_mapping": [
    {
      "customer_field": "value",
      "customer_type": "String",
      "required": true,
      "source_type": "tpops",
      "source": "metric_infos[].metrics[].series[<维度键>][i]",
      "transform": "取最后一个非空点；该点是 null 就不产出这条消息（不填 0）"
    }
  ]
}
```

- `tpops_fields` —— 这一路从 TPOPS 取回来的数据里有哪些字段、什么含义
- `field_mapping` —— 每个客户字段的值从哪来、怎么转
- `source_type` —— `tpops`（直接来自 TPOPS 响应）/ `mapping_table`（来自客户指标表映射表）/ `assembled`（由多个字段组装）

`docs/映射表-ITSCADA字段对照.md` 是这 5 个文件的可读版，由
`tools/build_field_mapping_doc.py` 生成。

## 映射分三层，各管一件事

| 层 | 文件 | 回答什么 |
|---|---|---|
| 业务规则 | `metric_rules.json` | 取哪个列、怎么算（值形态、差分、聚合、维度） |
| 取数定位 | `response_rules.json` | 响应体的哪一段、哪一列（基于实测留档校验） |
| **字段映射** | **`kafka_field_mapping.json`** | **这个值 → Kafka 消息的哪个字段** |

三份都是生成物，改动都要回到生成器：

```bash
python tools/build_itscada_mapping.py    # → metric_rules.json / alarm_rules.json
python tools/build_response_rules.py     # → response_rules.json
python tools/build_kafka_mapping.py      # → kafka_field_mapping.json
```

## 字段映射表（`kafka_field_mapping.json`）

**这张表回答一个问题**：某个客户指标的某条消息，ITSCADA 那 7 个字段
（或告警 13 个字段），每一个的值到底是 TPOPS 响应里的哪个值、怎么转出来的。

每条记录三段：

| 段 | 内容 |
|---|---|
| `tpops_response_source` | **实测响应体的原样片段** —— 真实字段名 + 真实数据 |
| `kafka_target` | Kafka 每个字段的取值表达式（引用 source 里的路径，或是常量） |
| `remark` | 人话说明，**尤其是维度展开口径** |

```json
{
  "metric_code": "GAUSSDB.[{#DATNAME}]_cache_hit_ratio",
  "tpops_response_source": {
    "node_id": "5a3cd027ad5c4f9bbb67d728c6d279c0no14",
    "node_name": "gauss-6374_gaussdbv5_hcs2-0_2",
    "status": "succeed",
    "result": [
      {"datname": "lz", "numbackends": "0", "xact_commit": "22259",
       "deadlocks": "0", "cache_hit_ratio": "99.84", "...": "..."},
      {"datname": "postgres", "numbackends": "26", "...": "..."}
    ]
  },
  "kafka_target": {
    "metricName": "[{#DATNAME}]缓存命中率",
    "metricCode": "GAUSSDB.[{#DATNAME}]_cache_hit_ratio",
    "resourceName": "{host_ip}_{node_id}",
    "value": "tpops_response_source.result[*].cache_hit_ratio",
    "unit": "%",
    "clock": "collect_time",
    "tags": "instance + node + dimensions"
  },
  "placeholder_bindings": {"DATNAME": "tpops_response_source.result[*].datname"},
  "remark": "按 {DATNAME} 展开：一行一条消息，result 的每一行产出一条（实测 7 行 → 7 条；6000 个库就是 6000 条）",
  "message_count": 7,
  "sample_message": {
    "metricName": "[lz]缓存命中率",
    "metricCode": "GAUSSDB.[lz]_cache_hit_ratio",
    "value": 99.84, "unit": "%", "clock": 1760000000000,
    "tags": {"instance_id": "83cd68f2…", "dimension_values": {"DATNAME": "lz"}}
  }
}
```

**source 里全是真实响应**：字段名与 TPOPS 返回的一模一样，值也是实测的。
`tests/test_kafka_mapping.py` 会逐条核对 source 的字段名**必须存在于实测响应体**。

### 取值表达式

| 写法 | 含义 |
|---|---|
| `tpops_response_source.result[*].<列>` | 逐行取值（每行一条消息） |
| `sum(tpops_response_source.result[*].<列>)` | 整批聚合成一条 |
| `sum(tpops_response_source.result[?<列>=*].<列2>)` | 按该列的每个取值分组，各一条 |
| `tpops_response_source.metric_infos[].metrics[name=<平台指标id>].series[*][-1]` | A 类时序，每个组件取最后一个非空点 |
| `count(tpops_response_source.<集合>)` | 集合条数 |
| `severity_to_priority[tpops_response_source.records[0].severity]` | 经映射表转换 |
| `collect_time` | 本轮采集完成时刻 |
| 直接写字面量 | 来自客户指标表的常量（指标名、英文名、单位） |

### 维度展开（占位符指标）

客户指标名里带 `{#DATNAME}` / `{#NAME}` 的指标，**一条结果行对应一条 Kafka 消息**：

```json
"placeholder_bindings": {"NAME": "tpops_response_source.result[*].relname"}
```

6000 张表 → 6000 行 → **6000 条消息**，占位符按行里的列替换掉
（`gaussdb.{#NAME}.garbage.Ratio` → `gaussdb.public.garbage.Ratio`）。

测试会拿"实测行数"对账：`per_row` 的指标有几行就得产出几条消息。

### 表级指标的维度值：`schema_name.table_name`

表级指标的 `{#NAME}` 绑的是**两列**（`dimension_bindings` 的值可以是
**点号连接的多个列名**）：

```json
"dimension_bindings": {"NAME": "schemaname.relname"},
"value_rule": {"type": "raw", "params": {
    "column": "n_tup_del", "row_policy": "per_row",
    "group_by": ["schemaname", "relname"]}}
```

- 维度值 = `schemaname` 与 `relname` 用 `.` 连起来 → `public.t1`；
- **分组按两列**（`schemaname` + `relname`）—— 只按 `relname` 分组会让
  `public.t1` 与 `other.t1` 落进同一个键，两条消息在幂等层撞键、
  后一条被**静默丢弃**；
- 幂等键 = `instance_id + metric_name + 排序后的 dimensions + timestamp`，
  维度不同 → 键不同 → 两条都投递；同一张表重复采集 → 键相同 → 仍然去重。

组合绑定取不到列值 → 抛 `RuleError`，**不静默留 `{#NAME}`**。

### 排除清单（`excluded_metrics.json`）

从生效口径里剔除的客户指标（**14 条** = 11 条重复的英文变体 + 3 条无法提供）：

```json
{"metric_code": "GAUSSDB.{#NAME}.garbage.Ratio",
 "metric_name": "({#NAME})Garbage ratio",
 "reason": "duplicate",
 "detail": "与 gaussdb.{#NAME}.garbage.Ratio 是同一条表级指标 ……",
 "kept": "gaussdb.{#NAME}.garbage.Ratio"}
```

- `reason` = `duplicate`（重复）时**必带** `kept`（保留哪一条）；
  `reason` = `unsupported`（无法提供）时不带 `kept`；
- 排除发生在**构建期**（`tools/build_itscada_mapping.py` 生成映射表时跳过）——
  运行期**没有**「遍历到再丢弃」的分支，被排除的指标码**根本不在**生效映射表里，
  轮汇总里的 `dropped_unsupported` 恒为 **0**；
- 改口径**只改这份数据文件**，不改代码。

### 告警的统一指标名（`alarm_rules.json` 的 `event_metric` 段）

平台 257 条告警定义里只有 3 条挂靠了客户指标（都指向 `gaussdb.Tps`）；
其余 254 条事件类告警**共用同一个**指标名：

```json
"event_metric": {"metric_code": "gaussdb.alarm", "metric_name": "数据库告警"}
```

- 统一值**表驱动**（写在数据文件里），改口径只改数据、不改代码；
- 告警种类由 `alarmId` / `alarmName` / `alarmContent` 承载，不靠 `metricCode` 区分；
- `metric_code` 唯一性**只对指标规则**要求 —— 告警定义靠 `alarm_id` 唯一定位。

### 展开方式

| 方式 | 指标数 | 含义 |
|---|---|---|
| `per_row` | 31 | 一行一条消息（表级/库级指标） |
| `aggregate` | 44 | 整批聚合成一条（实例级指标） |
| `per_component` | 11 | 每个组件一条（A 类时序） |
| `single` | 9 | 一条（诊断/实例接口的集合统计） |
| `per_group` | 3 | 每个分组一条 |

告警侧按**记录形态**给 4 条（活动/历史 × 收敛/明细），同样三段结构；
明细形态的 `value` 指向 `convergence_records`（明细行本身没有 `alarm_items_sum`）。

生成时会把整条链路真跑一遍：
`实测 response_body → 解包 → 取值规则 → 组装消息`，
所以样例消息能证明映射是对的 —— `tests/test_kafka_mapping.py` 每次都重跑一遍。

## 指标规则字段

```json
{
  "metric_code": "GAUSSDB.[{#DATNAME}]_cache_hit_ratio",   // 客户英文名（含占位符）
  "metric_name": "[{#DATNAME}]缓存命中率",                   // 客户中文名（含占位符）
  "resource_type": "GAUSSDB实例",
  "category": "数据库信息",
  "value_type": "浮点",
  "unit": "%",
  "collect_cycle": {"value": 5, "unit": "分"},
  "status": "可提供",
  "enabled": true,
  "source": {
    "api": "POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}",
    "source": "command_center_sql",
    "tpops_source": "pg_stat_database.blks_hit/(blks_hit+blks_read)",
    "sql_key": "stat_database",
    "method": "命令中心SQL"
  },
  "value_rule": {"type": "raw", "params": {"column": "cache_hit_ratio", "row_policy": "sum", "group_by": []}},
  "placeholders": ["DATNAME"],
  "dimension_bindings": {"DATNAME": "datname"},
  "needs_confirmation": false
}
```

## 取数定位表（`response_rules.json`）

`metric_rules.json` 回答"取哪个平台的哪个列"，但采集端拿到的是 HTTP 响应体 ——
中间隔着三层结构差异：

| 通道 | 响应体 | 要变成规则引擎能吃的形状 |
|---|---|---|
| 命令中心 | `detail.result` 是 **JSON 字符串** | 二次 `json.loads` 才是行列表 |
| 监控指标 | `metric_infos[].metrics[].series` 按组件展开 | 每个组件一条 series + 共享 timestamps |
| 诊断优化 | `slow_sql_infos` / `top_sql_infos` 列表 | 行列表 |
| 实例 | `instances` 列表 | 实例对象 |

这一层以前没有落成文件，各采集器只能各写一遍解包，规则里的列名也无从校验。
`response_rules.json` 把它显式化，每条规则三段：

```json
{
  "metric_code": "gaussdb.Deadlocks",
  "channel": "command_center_sql",
  "shape": "cmd_rows_stat_database",      // 形状：响应体的哪一段
  "locate": {                              // 定位：这一段里的哪部分
    "sql_key": "stat_database",
    "column": "deadlocks",
    "value_column": "deadlocks",
    "match": {},
    "group_by": []
  },
  "business": {                            // 业务规则：取到之后怎么算
    "value_rule": {"type": "raw", "params": {"column": "deadlocks", "row_policy": "sum"}},
    "scope": {
      "row":    {"policy": "sum", "group_by": []},
      "node":   {"policy": "per_master"},
      "series": {"policy": "not_applicable"}
    },
    "dimensions": {},
    "value_type": "整数",
    "missing": "null",
    "idempotency": ["instance_id", "metric_name", "dimensions", "timestamp"]
  },
  "evidence": {                            // 实测证据
    "sample_file": "data/metric_response/024_gaussdb.Deadlocks.json",
    "terminal_step": 3,
    "observed": {"row_count": 7, "columns": ["blk_read_time", "..."], "master_nodes": 3}
  }
}
```

**18 种形状**（= 11 种命令中心行结构 + 4 种监控指标 series 形态 + 2 种诊断列表 + 1 种实例列表）
定义在文件的 `shapes` 段，`kind` 决定解码方式：

| kind | 含义 |
|---|---|
| `rows_from_result` | `payload_path` 指向的字符串二次 `json.loads` 成行列表 |
| `rows_from_collection` | `payload_path` 指向的列表直接当行列表 |
| `series_from_metric_infos` | 按 `locate.metric` 匹配平台指标 id，取出 series 与 timestamps |
| `instance_from_list` | 取列表第一项作为实例对象 |

**这份文件是生成物**，不要手改：

```bash
python tools/build_response_rules.py          # 重新生成
python tools/build_response_rules.py --check  # 只校验是否最新
```

生成时当场校验两件事：

1. `locate` 里要取的列必须真实出现在实测响应的列集合里（开发方案 §5.6.4 的启动期校验前移到生成期）；
2. 带过滤条件的规则要在样本里统计命中行数，命中 0 行的显式标 `verified_on_sample: false` ——
   这类规则在生产里会"时有时无"（采集时刻没有那种会话状态或锁模式），
   根因是客户端过滤了本就不存在的行，应在 SQL 里做条件聚合。

`tests/test_response_rules.py` 会把每条规则在实测留档上跑一遍：
解得出上下文、跑得出非空值、列名对得上、文件与生成器一致。

### 占位符怎么处理

客户指标名里的 `{#XXX}` 按 ITSCADA 文档 §7 的做法**替换成实际值**：
`GAUSSDB.[{#DATNAME}]_cache_hit_ratio` + 维度 `DATNAME=postgres` → 发出
`metricCode = "GAUSSDB.[postgres]_cache_hit_ratio"`、`metricName = "[postgres]缓存命中率"`。
模板本身留在规则文件里（`metric_code` / `metric_name`），维度值在 `dimensions` 里。

### row_policy 决定一条 SQL 结果行怎么变成指标值

合并 SQL 的结果常常是**按库一行**（`pg_stat_database` 5 行、每库一行），
但客户指标不一定带库维度，所以规则里显式写了行策略：

| 策略 | 含义 | 例子 |
|---|---|---|
| `per_row` | 指标带维度，每行产出一条消息 | `GAUSSDB.[{#DATNAME}]_committed_transactions` |
| `sum` | 指标无维度且是数值，跨行求和（实例级总量） | `gaussdb.Numbackends` |
| `first` | 指标无维度且非数值，取第一行 | `gaussdb.Serverencoding`（字符集） |

### 取值规则类型

| type | 含义 |
|---|---|
| `raw` | 取行里某一列 |
| `raw_filtered` | 先按条件筛行，再取某一列（如 `pg_settings` 的 `name='port'`） |
| `aggregate` | 按条件过滤后聚合（`sum`），可按列分组 |
| `counter_delta` | 累计值差分成速率；首轮无基线、实例重启归零时产出 `null` |
| `ratio_across` | 两个来源相除（如 活跃会话数 / 总连接数） |
| `series_point` | 监控指标接口的时序，取最后一个非空点 |
| `enum_map` | 平台枚举 → 客户枚举；未命中原样透传 |
| `gauge` | 由时间戳派生（如 运行时长 = now − 启动时刻） |
| `top_n` | 排序取前 N 行 |
| `raw_rows` | 整行集（字符型明细指标） |
| `collection_count` | 集合条数（慢 SQL 总数） |
| `text_prefix_count` | 按文本前缀分类计数（Select / DML 慢查询） |

## 告警规则

`tpops_kafka_collector/mapping/data/alarm_rules.json` 三部分：

1. **枚举映射** —— 平台 `severity` → ITSCADA `priority`（critical→1 / major→2 /
   minor→3 / notice→4）；平台 `status` → ITSCADA `status`（activated→1 / recovered→0）。
2. **告警定义** —— 257 条平台告警（85 条指标类 + 203 条事件类，ID 空间不重叠）：
   告警名、默认等级、默认阈值、挂到哪个客户指标。
3. **指标挂载** —— 能对应到客户指标的告警（当前 3 条，全部指向 `gaussdb.Tps`）
   用**客户指标的** `metricName` / `metricCode`，取值是该指标最近一次采样值；
   其余事件类告警用 `metricCode = gaussdb.alarm.<alarm_id>`、`metricName = 告警名`，
   取值是命中节点数 `alarm_items_sum`。

## resourceName

**resourceName 只能表示一个节点**（客户口径），所以模板固定为
`{host_ip}_{node_id}`（如 `172.16.103.126_3881c04403394ee1b36a2bf873e69007no14`），
指标与告警同规则。模板在 `tpops_kafka_collector/mapping/data/itscada_field_map.json` 的 `resource_name` 段。

表名/库名/端口这类业务维度**不进资源名**，改由 `metricName`/`metricCode` 的占位符
与 `tags.dimension_values` 承载 —— 客户按台账查的是节点，粒度在指标名里。

节点身份来自 `GET /rds/gaussdb/v3/instances/{id}/key-view-execute-node` 的节点清单。
各通道的节点解析方式：

| 通道 | 节点来源 |
|---|---|
| 监控指标接口 | series 的键是 `component_id`（如 `cn_5002`）或 `ip-component_id`，经节点清单换算 |
| 命令中心 SQL | 数据按节点分布时取执行该 SQL 的节点；否则视为与节点无关 |
| 诊断优化接口 | 与节点无关 |
| 实例接口 | 与节点无关 |
| 告警 | 明细行自带 `node_id`/`host_ip`；收敛条目无节点，按 `alarm_id` 关联明细行回填 |

**与节点无关的指标**（命令中心的实例级/表级/库级、诊断优化、实例接口）统一挂到
节点清单里的**第一个 master CN**（`role=master` 且 `type=CN`）—— 固定一个节点，
避免「这轮 SQL 在哪个 master 上跑」导致资源名漂移。选主逻辑在
`tpops_kafka_collector/mapping/nodes.py`（`select_master_node` / `resolve_node_context`）。

> **告警的两点说明**：
> ① 收敛条目（`is_convergence=true`）平台**不带节点身份**，按 `alarm_id` 关联明细行回填；
>    一条告警命中多个节点时回填不了 → 落到第一个 master CN（此时该节点**不代表**真实告警节点，
>    真实节点在明细行形态的消息里）。
> ② 明细行形态（`is_convergence=false`）每行一个节点，`resourceName` 直接指向该节点。

> **未决**：最终值必须与客户 ITSCADA 台账录入的资源名称完全一致。
> 台账命名规则要客户给，拿到后改这里的模板即可。

## 待客户确认（`itscada_field_map.json` 的 `open_items`）

1. `value` 的线格式 —— 文档字段表写 String，指标样例是数字、告警样例是字符串；
   当前按客户指标表【数据类型】序列化。
2. 指标名字段的叫法 —— 字段表是 `metricName`/`metricCode`，告警样例里写的是
   `metric_name`；当前统一用字段表的写法。
3. `resourceName` 与台账的对齐规则。
4. 采集频率 —— 文档建议 1/5 分钟两档，客户指标表是 7 档（30 秒~24 小时）；
   当前以客户指标表为准。
5. 5 条状态类指标的展示值转义（`needs_confirmation: true`）。
