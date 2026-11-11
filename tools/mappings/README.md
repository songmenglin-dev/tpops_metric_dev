# ITSCADA 映射规则

把 TPOPS 取到的数据变成 ITSCADA 约定的 Kafka JSON。**规则是数据不是代码** ——
客户改字段口径，改 `data/mapping/` 下的 JSON，不用改 Python。

## 依据

| 来源 | 作用 |
|---|---|
| `docs/ITSCADA第三方指标数据接入对接方案V2.docx` | 指标消息的字段集合与语义（§6.2 字段表、§7 指标表模板） |
| `docs/ITSCADA第三方告警数据接入对接方案V2.docx` | 告警消息的字段集合与语义（§3.2 字段表、§4 压缩规则） |
| `高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` / 指标库 | 101 个客户指标的规范：中文名、英文名、数据类型、周期、单位 |
| `docs/开发方案-GaussDB指标与告警采集.md` §4.3 | 每个指标走哪条取数通道、怎么从原始值算出客户值 |

## 文件布局

```
data/mapping/                 # 规则数据（表驱动）
├── metric_mapping_monitor_metrics.json    # A 类 监控指标接口的字段映射
├── metric_mapping_command_center_sql.json  # B 类 命令中心 SQL 的字段映射
├── metric_mapping_diagnostic.json          # C 类 诊断优化接口的字段映射
├── metric_mapping_instance.json            # D 类 实例接口的字段映射
├── alarm_mapping.json                      # 告警的字段映射
├── itscada_field_map.json    # 客户消息的字段清单（哪些字段、必填与否）
├── metric_rules.json         # 101 条指标规则（98 可提供 + 3 标记丢弃）
├── alarm_rules.json          # 告警规则：等级/状态映射 + 257 条告警定义
└── open_items.json           # 待客户确认的 7 项

tools/mappings/               # 规则执行代码
├── __init__.py               # 对外接口
├── table.py                  # 加载与查表（MappingTable / MetricRule / AlarmDefinition）
├── rules.py                  # 取值规则引擎（raw / aggregate / counter_delta / …）
└── message.py                # 按 ITSCADA 字段规范组装消息

tools/build_metric_value_rules.py   # 开发方案 §4.3 → data/metric_value_rules.json
tools/build_itscada_mapping.py      # 中间件 + xlsx → data/mapping/*.json
tools/build_field_mapping_doc.py     # 分类映射文件 → docs/映射表-ITSCADA字段对照.md
tests/                               # 映射层单元测试（pytest）
```

`metric_rules.json` 与 `alarm_rules.json` 是**生成物**，不要手改：

```bash
python tools/build_metric_value_rules.py   # 开发方案 §4.3 → data/metric_value_rules.json
python tools/build_itscada_mapping.py      # 上述中间件 + xlsx → data/mapping/*.json
```

改口径的正确做法：改开发方案 §4.3 的表（人类可读的那份）→ 重新跑两个脚本 →
提交 `data/metric_value_rules.json` 与 `data/mapping/*.json`。
`tests/test_generated_rules.py` 会检查生成物与生成器一致。

用法：

```python
import sys
sys.path.insert(0, 'tools')
from mappings import MappingTable

table = MappingTable.load()          # 默认读 data/mapping/
rule = table.metric_rule('GAUSSDB.[{#DATNAME}]_cache_hit_ratio')
```

## 分类字段映射文件（开发依据）

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

`data/mapping/alarm_rules.json` 三部分：

1. **枚举映射** —— 平台 `severity` → ITSCADA `priority`（critical→1 / major→2 /
   minor→3 / notice→4）；平台 `status` → ITSCADA `status`（activated→1 / recovered→0）。
2. **告警定义** —— 257 条平台告警（85 条指标类 + 203 条事件类，ID 空间不重叠）：
   告警名、默认等级、默认阈值、挂到哪个客户指标。
3. **指标挂载** —— 能对应到客户指标的告警（当前 3 条，全部指向 `gaussdb.Tps`）
   用**客户指标的** `metricName` / `metricCode`，取值是该指标最近一次采样值；
   其余事件类告警用 `metricCode = gaussdb.alarm.<alarm_id>`、`metricName = 告警名`，
   取值是命中节点数 `alarm_items_sum`。

## resourceName

模板在 `data/mapping/itscada_field_map.json` 的 `resource_name` 段：默认
`{instance_name}_{node_id}`，取不到节点时回退 `{instance_name}`。各通道的节点解析方式：

| 通道 | node_id 来源 |
|---|---|
| 监控指标接口 | series 的键是 `component_id`（如 `cn_5002`）或 `ip-component_id`，经节点清单换算 |
| 命令中心 SQL | 命令中心任务记录直接带 `node_id` |
| 诊断优化接口 | 实例级，回退 `{instance_name}` |
| 实例接口 | 实例级，回退 `{instance_name}` |
| 告警 | 告警明细行自带 `node_id`；收敛条目无节点时回退 |

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
