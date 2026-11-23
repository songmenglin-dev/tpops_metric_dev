# Wave 2b 审查要求（wave-2b-channels，revision 3）

给 wave-2b 的**独立审查者**。本波次有 **8 个任务**（2.3~2.10），是项目里最大的一块。

## 审查对象

- **Diff 文件：** 见派发提示词
- **波次 ID:** `wave-2b-channels`（parallel）
- **实施方报告：** `.../workspace/task-2.{3,4,5,6,7,8,9,10}-report.md`
  （2.4 / 2.6 / 2.9 的报告是**控制器代写**的 —— 那些实施方被 Token Plan 配额
  上限（429）打断，没来得及写报告；报告开头已标明）
- **共享执行约定：** `.../workspace/wave2b-execution-brief.md`
- **合同：** `changes/tpops-kafka-collector/execution-contract.md`
- **任务清单：** `changes/tpops-kafka-collector/tasks.md` §波次 2b
- **进度账本：** `changes/tpops-kafka-collector/.superpowers/sdd/progress.md`

> **diff 里混进了一个已审过的提交**：`ff4993f`（wave-2a 的 2.2 合并 SQL 渲染）
> —— 它在 wave-2b 的提交之间落地，已在 wave-2a 的审查里通过，**不用重审**。
> 本波次要审的是 8 个任务：`2.3`（A 类窗口）、`2.4`（A 类采集器）、
> `2.5`（命令中心三跳）、`2.6`（B 类采集器）、`2.7`（C 类诊断优化）、
> `2.8`（D 类实例元数据）、`2.9`（E 类告警归并）、`2.10`（慢 SQL 脱敏）。

## 要逐条判的

### 1. 合同 Wave 2b 完成标准

对照 `execution-contract.md` 的 Wave 2b 段落，判定 满足 / 不满足 / 无法判定。

### 2. 八个任务的证明命令 —— 自己跑一遍

```bash
conda run -n tpops python -m pytest \
  tests/unit/test_metrics_window.py \
  tests/unit/test_command_center.py \
  tests/unit/test_sqlgen.py \
  tests/unit/test_masking.py \
  tests/integration/test_collector_monitor.py \
  tests/integration/test_collector_command_center.py \
  tests/integration/test_collector_diagnostic.py \
  tests/integration/test_collector_instance.py \
  tests/integration/test_collector_alarm.py -q
```

外加全量 `pytest -q` 与 `ruff check tpops_kafka_collector/`。

### 3. **本波次最容易出事的四条 —— 请重点验**

这是项目踩过的坑，**它们各自都有对应的守卫测试**：

| 坑 | 后果 | 该在哪看 |
|---|---|---|
| 合并形态当行列表用（该走 `decode_merged`/`merge_pages`） | **82 个 B 类指标静默全变 0** | 2.6 的 `decode_merged` 调用点；`tests/test_expected_values.py` 是否仍绿 |
| 命令中心失败不抛异常，不检查 `outcome.ok` | 静默丢数据 | 2.6 的 `outcome.ok` 检查点与对应测试 |
| 分页对**固定视图**也生效 | 固定视图每页重复返回全量，累加翻倍 | 2.6 的 `_has_more` 只看 `paged=true` 的视图 |
| `node_scoped=false` 的指标「值取自一个 master、资源名落在另一个 master」 | 值属于 A 节点、资源名写着 B 节点 | 2.6 的 `_host_node_id` 与承载节点的取值来源 |

**请独立判断这些守卫是不是恒真的** —— 尤其第 1 条：值相等的判据是
`data/mapping/expected_values.json`（**独立推导器** `tools/expected_deriver.py`
从实测留档原始行推出）。它与规则引擎是**两份独立实现**，但**两份一起错**也会一起对。
请抽查若干条，**独立重算**期望值，别只信那张表。

### 4. 「值等于期望值表」这条判据是否真的成立

wave-0 立的规矩：B 类的验收判据是**值等于期望值表**（不是「非零」判据）。
请判定：

- 2.6 的 `test_collected_values_equal_the_expected_table` 覆盖了**多少条**？
  （断言里 `checked` 计数不能是 0 或个位数 —— 「一条都没比上」的断言形同虚设）
- 比较时有没有走**同一套定型**（期望值表存原始值，消息里是按客户表【数据类型】
  定型后的值）？
- 有没有哪条 B 类指标**根本没被比到**（比如它没产出消息，于是被 `continue` 跳过）？
  如果有，说明这条指标**没有值相等守卫** —— 列出指标码。

### 5. 五个采集器的通则

逐个采集器核对：

- **缺值不产出消息**（规格「缺值不产出假数据」）：取不到值时是**不产出**，
  还是偷偷填了 0 / null？
- **空结果要记日志**：空分组 / 空集合 / 找不到目标实例，有没有对应的日志？
- **不静默吞异常**：平台报错是显式抛出还是被吞掉？
- **不推进水位**：采集器有没有越权改水位（水位推进是 wave 3 调度器的职责）？
- **不拼 Kafka 消息**：是不是都走 `mapping/message.py` + `kafka/serializer.py`？

### 6. 测试质量（可证伪性）

对每个新增测试判断「有没有一个能让它失败的小改动」。重点看：

- 2.6 的**平台不认 offset 守卫** —— 认出同一页后停下，会不会**漏掉**真正的多页场景？
- 2.7 的**窗口断言** —— 断言在采集器层（构造器是纯传参），对不对？
- 2.10 的**脱敏** —— 有没有漏掉的字面量形态（`NULL`、标识符里的数字、
  转义引号、美元引用）？
- 2.9 的**分页取全** —— 会不会只取第一页就当取全了？

### 7. 越界与治理红线

- **不得改** `execution-contract.md` 与 `tasks.md`（改了会让执行计划 stale）。
- **不得改** `data/mapping/*.json` 的内容（除生成物重生成，且必须由生成器产出、
  `git diff` 可复现）。
- **生产代码只有一份**：`tpops_kafka_collector/` 是唯一生产实现；
  `tools/` 与 `data/` 只作参考与离线工具。核对有没有反向依赖
  （生产代码 import `tools.*`，或读仓库根 `data/` 而不是包内 `mapping/data/`）。
  有测试 `tests/unit/test_package_boundary.py` 在守，**请核实它真的守得住**。
- 凭据是否只走环境变量、有没有写进日志/异常消息/测试快照。

### 8. 各文件的行数与复杂度

合同「单文件 < 400 行、函数 < 50 行」。**逐文件核对**（含新增的测试文件 —— 生产文件
是硬约束，测试文件按仓库既有惯例放宽，但要看清哪些超了、为什么）。

## 审查方式

- **只读 diff 文件一次**。diff 的上下文行**就是**被改文件的内容 —— 不要另外 Read 被改文件，
  除非某个你必须判断的 hunk 在函数中间被截断（那就在报告里说明）。
- **不要跑 git 命令**。
- 不要爬整个代码库。只有为了评估一个**你能具体命名的风险**才看 diff 之外的代码，
  **每个风险做一次聚焦检查**，并写明「风险是什么、查了什么」。
- 需要跑测试/静态检查时在 worktree 里跑，**不要修改任何文件**。

## 判定口径

- **Critical**：安全漏洞、数据丢失、会导致生产事故的缺陷
- **Important**：逻辑缺陷、规格违反、合同完成标准未满足、缺失必需测试、越界改动
- **Minor**：可维护性、文档、可选改进
- **Critical 或 Important 存在 → verdict 必须是 `fail`**，并给出可执行的修复指引
- 只有 Critical/Important 都清零才能给 `pass`；Minor 要逐条列出并给处置建议

## 交付

1. **审查报告**写到 `changes/tpops-kafka-collector/.superpowers/sdd/reviews/wave-2b-channels.md`
   （**不要**覆盖 wave-0 / wave-1 / wave-2a 的报告）。内容必须包含：
   - 结论（pass / fail）与逐条依据
   - 合同完成标准逐条判定表
   - 八个任务的证明命令实际结果
   - 第 3 节四条坑的守卫有效性判定（**含独立重算的抽样证据**）
   - 第 4 节「值等于期望值表」的覆盖度判定 + **没被比到的指标码清单**
   - 第 5 节五个采集器通则的逐条判定
   - 测试质量判断、越界与治理核对、行数核对
   - Critical / Important / Minor 分级清单
   - **待补清单**（要登记进合同但本次不改 `tasks.md` 的条目）
2. **不要**签发 receipt，不要改 `.spec-superflow.yaml`、不要改 `execution-contract.md` /
   `tasks.md`、不要改任何被审查的代码。receipt 由控制器签发。
3. 最终回复：verdict、各分级的条目数、Critical/Important 清单（若有）、待补清单、报告路径。
