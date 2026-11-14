# 归档：旧的分类字段映射表

归档时间：2026-11-13
归档原因：这套映射是**按类别**（A/B/C/D/E 五张表）组织的，
回答"这个指标的值从响应哪儿来"要跨三个文件跳：

```
metric_mapping_*.json  （按类别，value 字段的来源写得很虚）
  → metric_rules.json  （按指标，才有具体列名和规则）
    → merged_sql.json  （SQL 文本，才知道实际有哪些列）
```

而且 `value` 字段的来源描述是散文，不是可执行的对应关系，例如：

> `result → 行数组 → metric_rules[].value_rule 指定的列`

## 现在由什么取代

`data/mapping/kafka_field_mapping.json` —— 逐指标列出
**「响应里的某个值 → Kafka 消息的某个字段」**，
每条都带：

- `fields`：ITSCADA 消息每个字段的值从哪来、怎么转（逐字段一条）
- `value_source.path`：值在响应体里的**具体路径**（基于实测留档校验）
- `sample_message`：用实测响应真实产出的一条 Kafka 消息

生成器：`tools/build_kafka_mapping.py`
校验：`tests/test_kafka_mapping.py`

## 这几个文件留作什么用

- **历史对照**：想知道"原来是怎么写的、为什么改成现在这样"
- **字段集合校验的基准**：`tests/test_field_mapping.py` 仍在校验它们
  与客户两份对接方案的字段表一致（归档不等于作废，文档口径没变）
- **告警侧的字段说明**：`alarm_mapping.json` 里的 `tpops_fields`
  详细列了每种告警记录形态的字段（收敛/明细各有哪些字段），
  这部分信息新表里按形态汇总保留

## 不要改这里的文件

要改映射条件，改 `data/mapping/kafka_field_mapping.json`
（由生成器产出，跑 `python tools/build_kafka_mapping.py` 重新生成）。
