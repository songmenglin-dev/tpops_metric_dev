# Wave 2a 执行共享约定（wave-2a-mapping-data，revision 3，serial）

## ⚠️ 工作目录

你的 shell 起始目录是**主仓库**（master 分支），**那里没有 wave 0/1 的代码**。
必须**全程在 worktree 里工作**：

```
/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
```

**每一条 Bash 命令都写成** `cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev && ...`。
**所有 Read/Write/Edit 的路径一律用绝对路径** —— 相对路径会解析到主仓库（已有实施方踩过）。
开工前先 `ls /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev/tpops_kafka_collector/` 确认。

## 硬约束（合同级，违反即 Important）

- **Python 3.7.9**：禁海象 `:=`、`f"{x=}"`、字典合并 `|`、内建泛型注解 `list[str]`、
  `typing.Literal/Protocol/TypedDict/Final`、`functools.cached_property`、`zoneinfo`、
  `str.removeprefix/removesuffix`。一律用 `typing.List/Dict/Optional/Set/Tuple`。
- **不新增依赖**（`kafka-python==2.0.2`、`requests>=2.25,<2.32`）。
- **单文件 < 400 行、函数 < 50 行**。
- **不可变数据**：跨模块用 frozen dataclass / NamedTuple。
- **凭据只走环境变量**，禁止硬编码 / 写日志 / 进异常消息。
- **错误显式处理**：包装成自定义异常并带上下文，**不静默吞异常**；用 `logging`，**不用 `print`**。
- `ruff check tpops_kafka_collector/` 必须为 0（新增文件也过这条线）。
  注意 E501 按**东亚字宽**计（中文算 2 列），100 列的额度比看着少。
- **不改** `changes/tpops-kafka-collector/execution-contract.md` 与 `tasks.md`
  （改了会让执行计划 stale → **wave-0 与 wave-1 已签的 receipt 全部作废**，要重审两轮）。
  也不改 `changes/` 下其他已有文件。
- **不改** `data/mapping/*.json` 与 `data/merged_sql/*` 的内容（它们是**声明与留档**，
  由生成器产出；需要改就改 `tools/build_*.py` 并重跑）。
- 不改 `tpops_kafka_collector/` 下 wave 0/1 已有的文件，除非你的任务简报明确说了要改。

## 提交

**不要 `git commit`**（并发/串行都统一由控制器提交，避免 git index 锁竞争）。
只写文件 + 跑测试。

## 测试放哪

```
tests/unit/          # 纯单元
tests/integration/   # 涉及外部交互（Kafka/TPOPS）→ 一律 mock
```

## 报告

写到 `changes/tpops-kafka-collector/.superpowers/sdd/plans/r3-b4766c8ae9c06bd6685693405820aef9726b25fb5c9fd7b4ec4266146c310387/workspace/task-2.<N>-report.md`
（**绝对路径**）。

---

## 波次现状（你要接着做的）

wave-0（映射层基准改造）与 wave-1（骨架与平台层）**都已通过独立审查并签发 receipt**。
wave-1 落地了这些与本波次直接相关的东西：

| 位置 | 内容 |
|---|---|
| `tpops_kafka_collector/mapping/loader.py` | 映射表加载 + **启动期四类校验**（来源列存在于实测留档的实际返回列 / 可提供指标齐全 / 条目对应真实指标 / 占位符与维度绑定一一对应） |
| `tpops_kafka_collector/mapping/data/` | **包内映射数据副本**（7 张表：`metric_rules` / `alarm_rules` / `itscada_field_map` / `kafka_field_mapping` / `response_rules` / `customer_metric_mapping` / `open_items`） |
| `tpops_kafka_collector/mapping/data/observed_columns.json` | 留档的**实际返回列**声明（3.6 KB，由 `tools/build_observed_columns.py` 从留档现算，带 `--check`） |
| `tools/build_observed_columns.py` | 上面那个声明的生成器 |
| `tpops_kafka_collector/mapping/rules*.py` | 规则引擎（13 种 `value_rule.type`，拆 9 个模块 + 门面） |
| `tpops_kafka_collector/config.py` | 配置加载与校验（含 `cmd_page_size`、`cmd_limit`、`cmd_concurrency`） |

**设计决策 9**（`changes/tpops-kafka-collector/design.md`）：
`data/mapping/`、`data/merged_sql/` 是**参考与发布源**，`tpops_kafka_collector/mapping/data/`
是**生产用的副本**，由一致性测试守住防漂移。数据文件本身是**声明**（不像代码那样会分叉），
保留两份可接受。

## 本波次的任务

### 2.1 映射数据复制进包（**范围已按控制器裁决缩窄**）

`tasks.md` 里 2.1 的原范围是「把 `data/mapping/` 的六张表、`merged_sql.json`、`sql_merge.sql`
复制进包 + 加一条测试断言两份副本一致」。

**控制器裁决（已获用户确认）**：复制**映射表**那部分已由 wave-1 的任务 1.7 完成
（1.7 要加载包内数据，否则无数据可加载 —— 这是波次顺序造成的依赖倒置）。
**2.1 只剩「防漂移」**：

1. 把 `data/merged_sql/merged_sql.json`（11 个视图定义 + 刷新档位 + paging 段）与
   `data/merged_sql/sql_merge.sql` **复制**进 `tpops_kafka_collector/mapping/data/`。
2. 加 `tests/unit/test_mapping_data_sync.py`：断言**两份副本逐字节一致** ——
   **要覆盖包内已有的那 7 张映射表**，不只是新复制的 2 个文件。
   （1.7 只加了对 `observed_columns.json` 的防漂移闸，映射表本身的副本一致性
   **还没有守卫** —— 这正是 2.1 要补的。）
3. 证明：包内数据与仓库根声明**双向**对得上；且**反例有效** ——
   故意改一份副本内容时测试要红。

### 2.2 合并 SQL 的运行时渲染

把 `tools/build_sql_merge.py` 的渲染逻辑**复制**进 `tpops_kafka_collector/mapping/sqlgen.py`，
按 `merged_sql.json` 的视图定义与 `paging` 段渲染出带指定 `limit`/`offset` 的 SQL 文本
（页容量取自 `CMD_PAGE_SIZE`）；加一条测试断言**渲染出的第 1 页与 `data/merged_sql/sql_merge.sql`
逐字符一致**。完成后采集端能按 offset 翻页。

**注意 2.2 依赖 2.1**：`merged_sql.json` 必须先在包内，sqlgen 才能读它。
本波次是 **serial**，按 2.1 → 2.2 的顺序做。
