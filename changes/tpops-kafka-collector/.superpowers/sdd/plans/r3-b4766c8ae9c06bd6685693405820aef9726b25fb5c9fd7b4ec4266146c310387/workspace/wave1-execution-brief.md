# Wave 1 执行共享约定（wave-1-skeleton-platform，revision 3）

所有 wave-1 任务的实施方都先读这份，再读自己那一份任务简报。

## 工作目录

**必须**在 worktree 里工作（主仓库是 master 分支，只读）：

```
/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
```

每个 Bash 命令用 `cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev && ...` 或绝对路径。
Python 一律用 `conda run -n tpops python ...`（Python 3.7.9）。

## 权威口径

| 来源 | 作用 |
|---|---|
| `changes/tpops-kafka-collector/execution-contract.md` | **权威**。Intent Lock / Approved Behavior / Design Constraints / Wave 1 完成标准 / Test Obligations / Review Gates |
| `changes/tpops-kafka-collector/tasks.md` | 逐任务的完成标准与证明命令（**不要改**） |
| `changes/tpops-kafka-collector/specs/` | 规格（runtime-and-config / platform-client / mapping-and-rules / delivery-and-state） |
| `changes/tpops-kafka-collector/design.md` | 13 条设计决策 |
| `changes/tpops-kafka-collector/.superpowers/sdd/progress.md` | 进度账本与 Minor 台账 |
| `docs/开发方案-GaussDB指标与告警采集.md` | §5.5 配置项、§5.7 重试表等口径 |

## 硬约束（合同级，违反即 Important）

- **Python 3.7.9**：禁海象 `:=`、`f"{x=}"`、字典合并 `|`、内建泛型注解 `list[str]`、
  `typing.Literal/Protocol/TypedDict/Final`、`functools.cached_property`、`zoneinfo`、
  `str.removeprefix/removesuffix`。一律用 `typing.List/Dict/Optional/Set/Tuple`。
- **不新增依赖**。锁版本：`kafka-python==2.0.2`、`requests>=2.25,<2.32`。
- **单文件 < 400 行、函数 < 50 行**。
- **不可变数据**：跨模块用 frozen dataclass / NamedTuple，不原地修改。
- **凭据只走环境变量**，禁止硬编码、禁止写日志、禁止入库。
- **错误显式处理**：捕获后包装成自定义异常并带上下文，**不静默吞异常**；用 `logging`，**不用 `print`**。
- `ruff check tpops_kafka_collector/` 必须为 0（`ruff.toml`：`target-version="py37"`、`line-length=100`、
  `select=["E","F","W","E501","UP","FA102"]`）。**你新增的文件也要过这条线**。
- **不改** `changes/tpops-kafka-collector/execution-contract.md` 与 `tasks.md`
  （改了会让执行计划 stale、receipt 签不出来）。也不改 `changes/` 下其他已有文件。
- **不改** `data/mapping/*.json` 与 `data/merged_sql/*` 的内容（它们是**声明与留档**，
  由生成器产出；需要改就改生成器 `tools/build_*.py` 并重跑）。
- **不改** `tpops_kafka_collector/mapping/` 下 wave 0 已建的文件，除非你的任务简报明确说了要改。

## TDD 铁律

RED（写测试 → 看到它**失败**）→ GREEN（最小实现 → 通过）→ REFACTOR（保持全绿）。
报告里要写明「RED 时失败的断言是什么」。

## 提交

**不要 `git commit`**。本波次任务并发派发，多个进程同时写 git index 会抢 `.git/index.lock`。
只写文件、跑测试；由**控制器**统一提交。你的最终回复里给出：
新增/修改了哪些文件、测试输出、以及需要控制器知道的问题。

## 测试放哪

新测试按四层分目录（既有约定）：

```
tests/unit/          # 纯单元
tests/integration/   # 涉及外部交互（Kafka/TPOPS）→ 一律 mock
```

既有 `tests/` 根下的 164 个测试**不搬迁**。

## 报告

写到你任务简报同目录的 `task-1.<N>-report.md`（与简报同目录），
内容包含：做了什么、RED 的失败断言、GREEN 的输出、偏离简报的地方及原因、遗留问题。

---

## 波次内的任务拆分调整（控制器裁决，合同未改）

### 1.7 与 2.1 的依赖倒置

`tasks.md` 里 **1.7（映射表加载）要加载包内 `tpops_kafka_collector/mapping/data/*.json`**，
而**复制映射数据进包是 2.1**（wave-2a，在 wave-1 之后）—— 波次顺序上 1.7 无数据可加载。

**裁决（不动合同、不动 tasks.md）**：

| 任务 | 实际范围 |
|---|---|
| **1.7** | ① 把 `data/mapping/` 的**六张表**复制进 `tpops_kafka_collector/mapping/data/`；② 实现 `mapping/loader.py` + 启动期四类校验；③ `tests/unit/test_mapping_loader.py` |
| **2.1** | 只剩**防漂移**：把 `data/merged_sql/merged_sql.json` 与 `sql_merge.sql` 也复制进包，加 `tests/unit/test_mapping_data_sync.py` 断言两份副本逐字节一致 |

这样 1.7 的完成标准「运行时不需要仓库根目录」在 wave-1 结束时**真的成立**，
2.1 的证明命令 `pytest tests/unit/test_mapping_data_sync.py -q` 也不变。

### 1.8 必须一并处理 wave 0 遗留的 4 个 Minor

`progress.md` 的台账里指向 1.8 的项，**在重写规则引擎时一并解决**：

| 台账号 | 内容 | 1.8 要做的 |
|---|---|---|
| M-2 / R-M5 | `mapping/rules.py` 超 400 行 | 重写时**拆文件**（如 `rules/` 子包或按 `value_rule.type` 分模块），单文件 < 400 行 |
| M-3 | `ratio_across` 分母取不到返回 `0.0` 而非「不产出」 | 改成**不产出**（与「不静默出 0」的本波次目标一致），并更新期望值表与测试 |
| R-M2 | **`top_n` 的排序实为空操作**（`_to_number` 排序时间戳字符串 → 全退化成 `0.0`） | 按 **PostgreSQL `ORDER BY` 口径**真正排序（字符串按字典序、时间戳解析成时间；空值：升序在后、降序在前） |
| R-M3 | `raw` + `per_row` 的 12 条表级指标声明带 `group_by`，**引擎完全忽略** | 二选一：**执行 `group_by`** 或**从映射表删掉这个不生效的参数**（后者更好——声明里不该留一个被忽略的参数）。删参数要改 `tools/build_itscada_mapping.py` 的生成逻辑并重跑生成器 + 更新期望值表 |

### 1.8 与期望值表的关系（**关键**）

wave 0 建了 `data/mapping/expected_values.json`（82 条 B 类指标的期望值，**独立推导器**产出）。
1.8 重写规则引擎后，**期望值表必须重新生成并仍然逐条相等** ——
`tests/test_expected_values.py` 就是那个守门人。

> **改动规则引擎口径时，先想清楚期望值表要不要跟着改。**
> 期望值表是**独立第二实现**，两边都错会一起错 —— 所以改口径时
> **先改期望值推导器（`tools/expected_deriver.py`）**，再改引擎，
> 然后看 `test_expected_values.py` 是否红。它红了说明两边口径分叉了，
> 要判断是哪边对。

### R-M1：`raw_filtered` 的多命中语义

`raw_filtered` 在留档上每个 filter 恰命中 1 行，**「首行 vs 末行」不可区分**，
且**规格没定义**多命中语义。1.8 要做：定口径（「多命中取首个匹配行」）+
补一条**合成多命中**守卫（照 wave 0 的 `state_contains` 手法）+ 在规格里写明。

## 已知的合同口径不一致（**不要改合同**，在报告里 ⚠️ 记录）

合同 Wave 1 完成标准写「`python -m tpops_kafka_collector.main` 能起、能收 SIGTERM、退出码 0、
水位文件完整」，但 `main.py` 由 **wave 3 的任务 3.3** 新建，wave 1 的输出清单里没有它。

**处理口径**：wave 1 的门禁按「包内骨架与平台层就位 + `tests/unit`、`tests/integration` 全绿」判定；
进程入口、信号处理与水位文件的端到端验收归 wave 3（任务 3.3）。
在各自的报告里按 ⚠️ 记录该口径，由控制器转呈人工裁决。

## 覆盖率的现实

合同的「映射层与规则引擎 ≥95%」是 **wave 3** 的完成标准。wave 0 实测：
`decode.py` 82%、`columns.py` 88%、`rules.py` 92%。1.8 重写规则引擎时**顺带补测**，
把 `rules.py` 拉上去；但不必在 wave 1 就达到 95%。
