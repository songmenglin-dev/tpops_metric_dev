# Wave 3 执行共享约定（wave-3-scheduling-testing，revision 3）

## ⚠️ 工作目录

你的 shell 起始目录是**主仓库**（master 分支），那里没有 wave 0/1/2a/2b 的代码。
必须**全程在 worktree 里工作**：

```
/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
```

**每条 Bash 命令都写成** `cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev && ...`，
**所有 Read/Write/Edit 路径用绝对路径**。开工前先 `ls` 确认 worktree 目录。

## 硬约束（合同级，违反即 Important）

- **Python 3.7.9**：禁海象 `:=`、`f"{x=}"`、字典合并 `|`、内建泛型注解 `list[str]`、
  `typing.Literal/Protocol/TypedDict/Final`、`functools.cached_property`、`zoneinfo`。
  一律用 `typing.List/Dict/Optional/Set/Tuple`。
- **不新增依赖**（`kafka-python==2.0.2`、`requests>=2.25,<2.32`；
  **健康端点不要引入 web 框架** —— 标准库 `http.server` 足够）。
- **单文件 < 400 行、函数 < 50 行**。
- **不可变数据**：跨模块用 frozen dataclass / NamedTuple。
- **凭据只走环境变量**，禁止硬编码 / 写日志 / 进异常消息 / **进健康检查响应**。
- **错误显式处理**：包装成自定义异常并带上下文，**不静默吞异常**；用 `logging`，**不用 `print`**。
- `ruff check tpops_kafka_collector/` 必须为 0。E501 按**东亚字宽**计（中文算 2 列）。
- **不改** `changes/tpops-kafka-collector/execution-contract.md` 与 `tasks.md`
  （改了会让执行计划 stale → 前四个波次的 receipt 全部作废）。也不改 `changes/` 下其他已有文件。
- **不改** `data/mapping/*.json` 与 `data/merged_sql/*` 的内容（声明与留档，由生成器产出）。
- 不改 `tpops_kafka_collector/` 下已有文件，除非你的任务简报明确说了要改。

## 提交

**不要 `git commit`**（由控制器统一提交，避免 git index 锁竞争）。只写文件 + 跑测试。

## 测试放哪

```
tests/unit/          # 纯单元
tests/integration/   # 涉及外部交互 → TPOPS 与 Kafka 一律 mock
tests/regression/    # 回归（3.4 建）
```

## 报告

写到 `changes/tpops-kafka-collector/.superpowers/sdd/plans/r3-b4766c8ae9c06bd6685693405820aef9726b25fb5c9fd7b4ec4266146c310387/workspace/task-3.<N>-report.md`（**绝对路径**）。

---

# 已经在包里的东西（**不要重写**）

| 位置 | 内容 |
|---|---|
| `tpops_kafka_collector/tpops/client.py` | TPOPS HTTP 客户端（mini-token 认证、401 刷新、四类错误、认证静默期） |
| `tpops_kafka_collector/tpops/metrics.py` | A 类窗口计算 + 12 个分组码 + 空分组告警 |
| `tpops_kafka_collector/tpops/command_center*.py` | 命令中心三跳客户端（`statistics.running==0` 判结束、`result` 二次解析、master 去重与缓存、并发、逐条重试） |
| `tpops_kafka_collector/tpops/diagnostic.py` / `instances.py` / `alarms.py` | C/D/E 类的平台层 |
| `tpops_kafka_collector/collectors/*` | 五个采集器（monitor / command_center / diagnostic / instance / alarm） |
| `tpops_kafka_collector/mapping/loader.py` | 映射表加载 + 启动期四类校验 |
| `tpops_kafka_collector/mapping/data/` | 包内映射数据副本（10 份）+ `observed_columns.json` |
| `tpops_kafka_collector/mapping/rules*.py` | 规则引擎（13 种 `value_rule.type`，门面 + 9 个模块） |
| `tpops_kafka_collector/mapping/decode.py` | `decode` / `decode_merged` / `merge_pages` |
| `tpops_kafka_collector/mapping/nodes.py` | `select_master_node` / `resolve_node_context` |
| `tpops_kafka_collector/mapping/message.py` | `build_metric_message` / `build_alarm_message` |
| `tpops_kafka_collector/mapping/masking.py` | 慢 SQL 脱敏与截断 |
| `tpops_kafka_collector/mapping/sqlgen.py` | 合并 SQL 运行时渲染 |
| `tpops_kafka_collector/ids.py` | 幂等键真相源 + `IdempotencyKeyLRU` |
| `tpops_kafka_collector/kafka/serializer.py` | **唯一**消息 JSON 出口（指标 7 字段 / 告警 13 字段） |
| `tpops_kafka_collector/kafka/producer.py` | Kafka 生产者（PLAINTEXT、acks=all、指标/告警分 Topic） |
| `tpops_kafka_collector/watermark.py` | 水位与差分基线持久化（原子写） |
| `tpops_kafka_collector/retry.py` | 重试与退避框架 |
| `tpops_kafka_collector/config.py` | 配置加载与校验 |
| `tpops_kafka_collector/errors.py` | 异常体系（`is_retryable` 按 MRO 基类链判定） |

## 贯穿全项目的通则

- **不静默出 0**：取不到值就**不产出那条消息**（规格「缺值不产出假数据」）。
  `value=None` 只用于消息内部的缺省字段。
- **空结果 / 空分组要记告警日志**，不能静默返回空列表。
- **采集器不推进水位** —— 水位推进是调度器（3.1）的职责。
- **投递失败不推进水位**（至少一次语义）：Kafka 投递失败抛 `ProduceError`，
  调用方据此**不推进**，下一轮重投。
- **消息只有一个序列化出口**：`kafka/serializer.py`。有测试用 AST 扫全包守着
  （白名单：`kafka/serializer.py` 消息出口、`watermark.py` 状态文件落盘）。
  **别在别处加 `json.dumps` 产出消息**。
- **凭据不外泄**：不进日志、不进异常消息、不进健康检查响应。
