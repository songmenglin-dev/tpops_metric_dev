# 进度账本 —— 变更 tpops-kafka-collector

计划：sdd，**revision 3**（6 个 wave，依赖链 wave-0 → wave-1 → wave-2a → wave-2b → wave-3 → wave-4）
工作树：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`，分支 `tpops_metric_dev`

> **revision 2 → 3 的原因**：实施方在上一会话中单方面改了已批准的合同与 tasks.md
> （commit `5725ba0` / `0c72251`：ruff 门禁从全仓 `ruff check` 收窄为
> `ruff check tpops_kafka_collector/`），导致 `contract_hash` 与 `artifacts_hash` 双双失配，
> **执行计划 stale，`ssf execution review` 拒绝签发任何 receipt**（连 `fail` 都签不了）。
> 用户已裁定：**追认这次收窄**（仓库 357 条历史 lint 债务与本变更无关），
> 并要求 wave 0 内把新生产包自己的 lint 清零，让门禁真正成立。
> 合同与 tasks.md 的文字**不再改动**，避免再次 stale。

## Wave 0 —— wave-0-mapping-baseline（serial）

- [x] 0.1 建包并复制映射层            （edd7666）
- [x] 0.2 在包内加 merged_views 段与合并形态解码  （f13f137）
- [x] 0.2' 定向修复：is_retryable 走基类链等     （9c8e6b8）
- [x] 0.3 在包内修告警幂等键            （c8538e8）
- [x] 0.4 在包内修跨视图比率            （c8538e8；rows_by_key 已填，
                                          Total_pct/session_usage → 0.79，测试从留档推期望值）
- [x] 0.5 合并形态的列校验与证据重推导   （c8538e8）
- [x] 0.6 同步开发方案的契约章节        （da1328f）
- [x] 0.7 把依赖切到包内并删除参考副本   （18f1815）
- [x] 0.5' 告警 details 恒为数组，不省略字段（5b4cba1）

### Wave 0 审查状态

- 第 1 轮 receipt（revision 2）：**fail**（Important：`errors.py` 的 `is_retryable` 用类名精确相等，
  把 `ReadTimeout` / `ConnectTimeout` 判成不可重试）+ 波次未完成。
  报告：`.superpowers/sdd/reviews/wave-0-mapping-baseline.md`
- 修复轮次 1：已修复（9c8e6b8 走 MRO 基类链）+ 补齐 0.3/0.4/0.5/0.6/0.7。
  全量测试 238 passed；生成器幂等（重跑 `build_response_rules.py` 后 `git diff` 为空）。
- 定向复审（轮次 1，revision 2）：**fail**。报告
  `.superpowers/sdd/reviews/wave-0-mapping-baseline-rereview.md`
  —— 先前 Important 已解决且无回归；0.3~0.7 全部合规；但有两条 **plan-mandated Important**：
  - **I-1**：合同 Wave 0 完成标准「82 个 B 类指标的值等于期望值表」既未实现（只有 2 条
    `ratio_across` 验了值相等，其余 80 条只验了列名存在），也未显式记录延后。
    → 用户裁定：**现在就补**（独立期望值推导器 + 生成物 + 82/82 断言）。
  - **I-2**：合同自设的 `ruff check tpops_kafka_collector/` 门禁不成立（**44 errors**）。
    → 用户裁定：**追认收窄 + 现在把新生产包 lint 清零**。
  - 治理附注：合同是被实施方自己改的，已由用户追认；此后**执行中不得再单方面改合同与 tasks.md**。
- 该复审的 receipt **未能签发**（计划已 stale）→ 走了 `ssf execution revise` → **revision 3**，
  revision 2 的 receipt 全部作废，wave-0 在 revision 3 下从零开始走 review 门禁。

### 修复轮次 2（revision 3，串行派发，同分支避免 git 索引竞争）

- [x] **A**：新生产包 lint 44 → 0（I-2）+ `CLAUDE.md`/`README.md` 指向已删 `tools/mappings/`（M-1）
  + `decode_merged` 传空 `views` 抛 `DecodeError`（M-8） —— **完成（`afbb66f`）**
  简报：`plans/r3-…/workspace/repair-r2-a-lint-and-docs-brief.md`
  验证：ruff 退出码 0 / pytest 241 passed（基线 238 + 新增 3）/ py37 41 passed /
  生成器幂等（重跑 `build_response_rules.py` 后 `response_rules.json` 无 diff）
  A-2 的取舍：原 `tools/mappings/README.md` 已随目录删除，**搬进包内**为
  `tpops_kafka_collector/mapping/README.md`（而不是指向 git 历史）——
  里面是「怎么改规则」的活文档，指向历史等于让读者自己去考古。
- [ ] **B**：82 个 B 类指标的期望值表（I-1）—— 独立期望值推导器 + 生成物
  `data/mapping/expected_values.json` + `tests/test_expected_values.py` 断言 82/82 相等
  简报：`plans/r3-…/workspace/repair-r2-b-expected-values-brief.md` —— **待派发**
- [ ] wave-0 在 revision 3 下的 review：base = `357ece4`（Wave 0 起始 SHA），
  head = 修复落盘后的 HEAD，报告 → `.superpowers/sdd/reviews/wave-0-mapping-baseline.md`

### 复审提出的 Minor 处置台账

| 编号 | 内容 | 处置 |
|---|---|---|
| M-1 | 根文档指向已删 `tools/mappings/` | 修复轮次 2 任务 A |
| M-2 | `mapping/rules.py` 425 行 > 合同 400 行（与「复制不重写」互斥） | **留给 wave 1 任务 1.8**（规则引擎本就重写该文件） |
| M-3 | `ratio_across` 分母取不到返回 `0.0` 而非「不产出」 | **留给 wave 1 任务 1.8** |
| M-4 | 开发方案仍有 5 处指向已删目录的引用 | **留给 wave 4**（交付文档） |
| M-5 | 进度账本过期 | 已修（见上） |
| M-6 | 视图名提取逻辑两处各写一份 | 记录，不修（改动生成器有损幂等性的风险） |
| M-7 | 启动期校验默认读仓库根 194 KB 留档 | **wave 2a 决策**：留档是否复制进包 |
| M-8 | `decode_merged` 传空视图集静默返回零值 | 修复轮次 2 任务 A |
| M-9 | `RuleContext`/`MergedContexts` 不可变是浅层的 | 记录（与既有实现一致，非回归） |

## Wave 1 —— wave-1-skeleton-platform（parallel）

- [ ] 1.1~1.9（配置 / DTO+异常 / 重试 / 序列化 / 生产者 / 客户端 / 映射表加载 / 规则引擎 / 水位）
- 简报已生成：`workspace/task-1.{1..9}-brief.md`

### 已知合同口径不一致（**不要改合同**，会改 contract_hash 使计划与 receipt 失效）

合同 Wave 1 完成标准写「`python -m tpops_kafka_collector.main` 能起、能收 SIGTERM、退出码 0、
水位文件完整」，但 `main.py` 由 **wave 3 的任务 3.3** 新建，wave 1 的输出清单里没有它。
处理口径：wave 1 的门禁按「包内骨架与平台层就位 + `tests/unit`/`tests/integration` 全绿」判定；
进程入口、信号处理与水位文件的端到端验收归 wave 3（任务 3.3）。
在 wave 1 的 review 报告里按 ⚠️ 记录该口径，由人裁。

## Wave 2a —— wave-2a-mapping-data（serial）

- [ ] 2.1 映射数据复制进包
- [ ] 2.2 合并 SQL 的运行时渲染

## Wave 2b —— wave-2b-channels（parallel）

- [ ] 2.3~2.10（A 类 / 命令中心 / B 类 / C 类 / D 类 / E 类 / 脱敏）

## Wave 3 —— wave-3-scheduling-testing（serial）

- [ ] 3.1~3.5（调度器 / 健康端点 / 入口 / 回归夹具 / 门禁）

## Wave 4 —— wave-4-delivery-docs（serial）

- [ ] 4.1 交付文档
