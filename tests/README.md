# tests/ —— pytest 单元测试

```bash
conda run -n tpops python -m pytest -q
```

**TPOPS 与 Kafka 一律 mock**——测试不连真实平台。
真实响应体作为**样本文件**回放（见 [`data/metric_response/`](../data/metric_response/)）。

`pytest.ini` 里 `testpaths = tests`：只收集本目录，
`data/command_center/` 下那些命名像测试、实际会连真实平台下发命令的脚本不会被收集。

## 本目录的文件

| 文件 | 测什么 | 依赖的样本 |
|---|---|---|
| `conftest.py` | pytest 公共夹具（`DATA_DIR` 等路径常量） | — |
| `test_rules.py` | **取值规则引擎与 ITSCADA 消息组装**：raw/aggregate/counter_delta/ratio/enum_map/gauge/top_n 各类规则、占位符替换、资源名拼装、缺值不产出假数据 | `data/merged_sql/merged_sql.json` |
| `test_response_rules.py` | **`response_rules.json` 的映射条件必须在实测 `response_body` 上真的成立**：每个 shape 能不能解出声明的行数与列名 | `data/metric_response/` |
| `test_kafka_mapping.py` | **`kafka_field_mapping.json` 的「真实响应 → Kafka 字段」对应表必须成立**：逐指标用实测响应产出一条消息，字段逐个核对 | `data/metric_response/` |
| `test_metric_response.py` | **原生响应留档必须与映射规则保持一致**：manifest 覆盖全部可提供指标、样本文件与 manifest 对得上、命令中心样本记录完整四跳、响应里含该指标对应的列 | `data/metric_response/000manifest.json` |
| `test_field_mapping.py` | **分类字段映射表必须与 ITSCADA 文档、与 TPOPS 实测原始结构对得上** | `data/mapping/archive/`、`docs/` |
| `test_mapping_table.py` | 映射表加载与查表（`tools/mappings/table.py`） | `data/mapping/` |
| `test_generated_rules.py` | **规则文件必须与生成器保持一致**（改了生成器要重新生成并提交） | `data/mapping/metric_rules.json` |
| `test_nodes.py` | **节点身份解析**：resourceName 的取值口径（`{host_ip}_{node_id}`、与节点无关的指标落到第一个 master CN） | `data/instances.json` |
| `test_py37_compat.py` | **Python 3.7.9 兼容性门禁**：扫描全部源码，禁掉 3.8+ 语法（海象 `:=`、`list[str]` 等） | 源码 AST |

## 约定

- 测试名要说明**行为**（`test_returns_empty_list_when_no_metrics_match_query`），
  不是说明函数名。
- 断言用 Arrange-Act-Assert 三段。
- 覆盖真实样本的测试**不要改样本**去迁就测试；样本是证据。
  平台变了就重新抓（`tools/capture_metric_response.py`），然后同步更新映射表。
