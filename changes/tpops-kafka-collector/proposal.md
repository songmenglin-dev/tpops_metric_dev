# 变更提案：TPOPS → ITSCADA Kafka 采集端

## 背景（Why）

客户监控平台（广东电网 ITSCADA）对达梦/Oracle/MySQL 等数据库走**主机 Agent** 采集，
唯独 GaussDB 走 Agent 路线不通 —— 这一路由由本项目承担：从 TPOPS（GaussDB 监控平台）
开放接口取数，转成客户平台能消费的格式，经 Kafka 持续投递。

需求侧已全部定稿并留在仓库里：客户指标库 101 个指标（98 可提供 / 3 无法提供）、
字段规范与取值口径（开发方案 §3）、逐指标取数方法（§4.3）、接口契约（§4.2 + `docs/api/`）、
字段级规则（`data/mapping/` 六张表）、实测样本（`data/metric_response/` 98 份 +
`data/merged_sql/`）、验收标准 D1~D8（§1.7）、WBS W01~W24（§7.2）。

**但仓库里没有一行采集端代码** —— `tpops_kafka_collector/` 这个包尚不存在。
当前只有映射层（`tools/mappings/` + `data/mapping/`）与离线回归样本。客户平台的看板
至今拿不到一条本项目产出的数据。

同时存在一个**已实测复现的缺陷**：映射层不认识「单条合并 SQL」的返回形态
（`result` = 一行 11 个 `json_agg` 列），82 个 B 类指标在该形态下**静默产出 0 / None / 空列表**
且不报错，而现有 164 个测试全绿也拦不住（回归样本还是旧的 11 条形态）。
若按现状直接写采集端，交付的是「能跑但数据错」的版本 —— 方案 §1.1 明确指出
「错的数据比没有数据更危险」。

## 变更内容（What Changes）

1. **新建 `tpops_kafka_collector/` 采集端**：常驻进程，按窗口与水位循环 —— 拉取 → 归一化 →
   映射 → 序列化 → 投递 Kafka → **投递成功后才推进水位**。支持优雅退出与健康端点。
2. **映射层改基准**：新增 `merged_views` 段与 `decode_merged` / `merge_pages`，
   让映射层能解「单条合并 SQL」的返回形态；既有 per-view 路径一字不动。
3. **告警幂等键修正**：键由 `(alarm_id, instance_id, node_id, first_alarm_time)`
   改为 `(alarm_id, instance_id, first_alarm_time, status)` —— 原键在恢复事件上
   会把恢复消息永久去重丢弃（`last_alarm_time` 在恢复后不变，加它也无效）。
4. **顺带修两个恒为 0 的指标**：`gaussdb.Total_pct` 与 `GAUSSDB.session_usage`
   是跨视图比率，其读取的 `ctx.rows_by_key` 从未被任何生产路径填充过。

保留不变的行为：客户侧 Kafka 消息信封（以 `data/mapping/itscada_field_map.json`
的 7/13 字段为准 —— 该文件直接取自客户两份对接方案 §6.2 / §3.2，
且 `tools/mappings/message.py` 已按它实现并被测试覆盖）、
Topic 名与分区策略（§3.2）、采集周期 7 档（§3.3）、取值口径（§3.4）、
维度展开规则（§3.5）、3 个不可提供指标的处置（§3.6）、脱敏默认策略（§3.8）。

> **开发方案 §3.1 的另一套信封（14/25 字段，含 `msg_type` / `schema_version`）已过时**，
> 那是映射层重构之前的设计，被客户字段表取代。本次同步修正 §3.1 —— 它不是「附带漂移」，
> 而是本次直接改动的契约的真相源。

## 范围（Scope）

### 范围内（In Scope）

- **W01~W24 全部**（开发方案 §7.2 的 WBS），分四个波次推进：
  - 波次 0：映射层基准改造（合并 SQL 形态 + 幂等键 + 跨视图比率）
  - 波次 1：骨架与平台层（W01 配置 / W02 DTO+异常 / W03 重试 / W04 序列化 /
    W05 Kafka 生产者 / W06 TPOPS 客户端 / W07 映射表加载 / W08 规则引擎 / W09 水位）
  - 波次 2：四条采集链（W10~W18）
  - 波次 3：调度与测试（W19 调度器 / W20 健康端点 / W21 入口装配 / W22 回归夹具 /
    W23 测试）
  - 波次 4：交付文档（W24）
- 采集端落地需要的映射表文件从 `data/mapping/`、`data/merged_sql/` **复制**进
  `tpops_kafka_collector/mapping/data/`（仓库原件保留，服务离线回放与文档生成）。
- **开发方案中与本次改动直接相关的契约章节同步**：§3.1（消息信封 → 客户字段表的
  7/13 字段）、§3.7 与 §4.5.4（告警幂等键）、§4.2.4 / §4.4 / §4.4.2 / §4.4.3
  （合并 SQL 形态、102400 B 上限、分页）、§4.6.3（到期判定）、§5.5（配置项）、
  §5.6.4（启动校验的列来源）、§6.2（回归样本）。
  **这些不是「附带漂移」——它们是本次直接改动的契约的真相源**，
  不同步就会让下一个实现者按旧文档写回旧口径。
- 交付文档：逐条口径说明、疑似 OCR 错字清单、部署运行说明、不可提供指标清单。

### 范围外（Out of Scope）

- **联调验收 P5**（2026-12-14~15）：需客户真实 Kafka 地址与 Topic 名，
  客户侧死线逾期中；本变更交付到「可在真实环境跑一轮」为止。
  **验收项 D4（连续运行 ≥24 小时）不在本变更的完成证明内**，
  按开发方案 §7.1.1 的裁剪顺序，它排在最后被让出。
- **`data/mapping/archive/` 改名为 `classification/`**：属改名不属功能，另开变更。
- **24 条评审意见现状核对表**：另开变更。
- **开发方案中与本次改动无关的「11 条合并 SQL」残留**（§7.1.1 时间线叙述、
  §9.2 实测留档索引等）：另开变更。
- 客户 ITSCADA 平台本身的开发与部署；其他数据库的采集（走 Agent）；
  TPOPS 平台自身的运维；数据的长期存储与历史回放。

## 影响与验证

### 影响区域

| 区域 | 影响 |
|---|---|
| **新增** `tpops_kafka_collector/` | 顶层 8 个（`config` / `models` / `errors` / `retry` / `watermark` / `scheduler` / `health` / `main`）+ `tpops/` 6 个 + `collectors/` 5 个 + `mapping/` 7 个（5 个复制自 `tools/mappings/` + `loader` + `sqlgen` + `masking`）+ `kafka/` 2 个 = **28 个新模块**。**这是唯一的生产代码** |
| **删** `tools/mappings/` | 复制进包并把 `tools/` 与 `tests/` 的 import 切到包内后删除（git 历史保留）。理由：双副本会静默分叉，而分叉就是「测试全绿但生产代码错」 |
| **改** `tools/` 下 10+ 个脚本、`tests/` 下 8 个测试文件 | import 由 `tools.mappings` 改为 `tpops_kafka_collector.mapping`；脚本的 `sys.path` 加项目根 |
| **改** `tools/build_response_rules.py` | 生成 `merged_views` 段、改写 `api_chain`、删除死代码 `IDEMPOTENCY['alarm']`、增加合并形态的列校验 |
| **改** `tools/build_itscada_mapping.py` | `alarm_rules.json` 增加 `idempotency` 段 |
| **改** `tools/build_sql_merge.py` 的渲染逻辑 | 复制进包（`tpops_kafka_collector/mapping/sqlgen.py`），供运行时按 offset 生成 SQL |
| **改** `data/mapping/response_rules.json`、`alarm_rules.json` | 生成物，随生成器更新 |
| **改** `docs/开发方案-GaussDB指标与告警采集.md` | §3.1 / §3.7 / §4.5.4 / §5.1 等本次直接改动的契约章节 |
| **新增** `tests/` 下的 unit / integration / fixtures / regression 四层 | 回归层回放 98 份留档 + 合并 SQL 留档 |
| **外部依赖** | `kafka-python==2.0.2`（PLAINTEXT 生产者）、`requests`；均为既有锁版本，无新增依赖 |

### 完成证明

| 证明 | 命令 / 判据 |
|---|---|
| 进程可启动并优雅退出 | `python -m tpops_kafka_collector.main` 起得来、收 SIGTERM 后退出码 0、水位文件完整 |
| 82 个 B 类指标在合并 SQL 形态下产出真值 | 回归层用 `data/merged_sql/sql_merge_probe_result.json` 回放，逐指标比对期望值表（不是「非零」判据） |
| 98 个可提供指标全部有数据 | 回归层 98/98 通过（对应 D1） |
| 3 个不可提供指标无消息且计数递增 | 单元断言（对应 D2） |
| 消息符合冻结信封 | schema 校验通过（对应 D3） |
| 水位语义 | 杀进程重启演练：不重复、不丢失（对应 D5） |
| 覆盖率 | 单元/集成 ≥ 80%，映射层与规则引擎 ≥ 95%（对应 D6、NFR-10） |
| 认证失败不锁号 | 连续 3 次错误密码后账号未被锁定（对应 D7） |
| 慢 SQL 默认脱敏 | 配置断言（对应 D8） |
| 3.7 语法门禁 | `pytest tests/test_py37_compat.py` + `ruff check`（`target-version = "py37"`） |
