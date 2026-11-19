# data/mapping/ —— 映射规则数据（表驱动）

采集端落地时，这一层的文件要搬进 `tpops_kafka_collector/`。
**规则不写死在代码里**：改口径改这些文件，不改 `.py`。

执行代码在 [`tools/mappings/`](../../tools/mappings/)。

## 本目录的文件

| 文件 | 作用 | 生成者 / 校验 | 能不能删 |
|---|---|---|---|
| `itscada_field_map.json` | **客户消息的字段清单**（哪些字段、必填与否、类型）+ `resource_name` 模板 + tags 规则。版本号在这里 | 手工维护；`tests/test_field_mapping.py` 校验 | 不能 |
| `metric_rules.json` | **逐指标规则**（101 条）：每条一个客户指标，含 `metric_name`/`metric_code`/`value_type`/`unit`/`value_rule`/`dimension_bindings`/`node_scoped`/`note` | 生成：`tools/build_itscada_mapping.py`；校验：`tests/test_generated_rules.py` | 不能 |
| `response_rules.json` | **响应条件**（98 条）：每条规则声明「从响应体的哪一段取、用哪个 shape、怎么定位」。**shape 段**定义了怎么把 HTTP 响应体解成规则引擎要的 `RuleContext` | 生成：`tools/build_response_rules.py`；校验：`tests/test_response_rules.py`（在实测 `response_body` 上真的成立） | 不能 |
| `kafka_field_mapping.json` | **「响应值 → Kafka 字段」对应表**：逐指标列出每个 Kafka 字段的值从响应体哪儿来、怎么转，并附一条用实测响应真实产出的样例消息 | 生成：`tools/build_kafka_mapping.py`；校验：`tests/test_kafka_mapping.py` | 不能 |
| `alarm_rules.json` | 告警侧规则：288 条告警定义（`alarm_id` → 规则名、默认阈值、默认等级）、`severity_to_priority`、`status_to_itscada`、取值来源 | 生成：`tools/build_itscada_mapping.py` | 不能 |
| `open_items.json` | **待客户确认的 8 项**（value 线格式、指标名字段叫法、resourceName 对齐、采集周期、状态枚举转义、告警挂哪个指标、慢 SQL 能否外发、A 类多 series 归集）。每项写明「我们的默认」+「确认后改哪儿」 | 手工维护 | 不能 |

## 子目录

| 目录 | 内容 | 说明 |
|---|---|---|
| [`archive/`](archive/) | **旧的分类字段映射表**（按 A/B/C/D/E 五类组织） | ⚠️ **名字叫 archive，但它还在被生产代码加载**（`tools/mappings/table.py` 读它做分类映射表的查表）。详见 [archive/README.md](archive/README.md) |

## 谁读谁

```
itscada_field_map.json ─┐
metric_rules.json      ─┼→ tools/mappings/table.py   （加载 + 查表）
archive/*.json         ─┘        │
response_rules.json ──────────→ tools/mappings/decode.py（响应体 → RuleContext）
                                 tools/mappings/rules.py （RuleContext → 值）
                                          ↓
                              tools/mappings/message.py （值 → ITSCADA 消息）
                                          ↓
                              tools/mappings/nodes.py    （resourceName 的节点口径）
```

## 约定

- **本目录的文件要么手工维护（`itscada_field_map.json`、`open_items.json`），
  要么由生成器产出（其余）。生成器产出的一律不手改**——
  改生成器后重跑对应的 `tools/build_*.py` 并提交结果，
  `tests/test_generated_rules.py` 会检查「生成物 == 生成器输出」。
