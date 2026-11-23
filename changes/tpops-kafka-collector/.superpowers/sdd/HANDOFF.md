# 交接说明 —— 变更 `tpops-kafka-collector`

> 本文件是**新会话的入口**。读完这一份 + 三份任务简报就能直接开工。
> 位置选在 `.superpowers/sdd/` 根目录（**非 plan-scoped**）——
> 因为签发 pass receipt 时工具会删掉 `plans/<hash>/workspace/`，这个目录不会被删。

---

## 0. 三十秒速览

| 项 | 值 |
|---|---|
| 变更名 | `tpops-kafka-collector`（实现 TPOPS 的 GaussDB 采集端 → Kafka） |
| **工作树** | `/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev` |
| **分支** | `tpops_metric_dev`（master 上**只有规划工件**，66 次实现提交全在这个分支） |
| 主仓库 | `/mnt/c/sml/project/py_project/tpops_metric_dev`（master，**代码不在这里**） |
| 状态 | `state: executing`，`revision: 3`，执行模式 `sdd` |
| 测试环境 | `conda run -n tpops python -m pytest -q`（Python **3.7.9**） |
| 当前门禁 | **1295 passed** · `ruff check tpops_kafka_collector/` = 0 · py37 门禁 120 passed |
| **下一步** | **wave-3 任务 3.1 / 3.3 / 3.5**（已解锁，`eligible: true`） |

---

## 1. 现在的进度

```
wave-0-mapping-baseline     receipt=pass
wave-1-skeleton-platform    receipt=pass
wave-2a-mapping-data        receipt=pass
wave-2b-channels            receipt=pass      ← 经一轮修复 + 定向复审
wave-3-scheduling-testing   eligible=true    ← 已解锁，尚未开工
wave-4-delivery-docs        未解锁（等 wave-3）
```

**32 个任务已完成 28 个**：

| 波次 | 任务 | 状态 |
|---|---|---|
| wave-0 | 0.1~0.7 映射基准 | ✅ |
| wave-1 | 1.1~1.9 骨架与平台层 | ✅ |
| wave-2a | 2.1~2.2 映射数据进包 | ✅ |
| wave-2b | 2.3~2.10 五路采集器 | ✅ |
| wave-3 | 3.1 调度器 / **3.2 健康端点 ✅** / 3.3 入口装配 / **3.4 回归夹具 ✅** / 3.5 全量门禁 | 🔶 剩 3 个 |
| wave-4 | 4.1 交付文档 | ⬜ |

代码量：生产包 51 个文件 / 9,667 行；39 个测试文件。

---

## 2. 立刻要做的事：wave-3 的三个任务

**三份简报已写好并提交在仓库里 —— 直接照着做，不要重新设计。**

| 任务 | 简报路径（相对于 worktree 根） |
|---|---|
| **3.1 调度器** | `changes/tpops-kafka-collector/.superpowers/sdd/plans/r3-b4766c8ae9c06bd6685693405820aef9726b25fb5c9fd7b4ec4266146c310387/workspace/task-3.1-brief.md` |
| **3.3 入口装配** | 同目录 `task-3.3-brief.md` |
| **3.5 全量门禁** | 同目录 `task-3.5-brief.md` |

另有一份共享约定：`workspace/wave3-execution-brief.md`（硬约束、TDD 要求、报告位置）。

> **⚠️ 如果这些文件不见了**（签发 pass receipt 会删 workspace），从 git 恢复：
> ```bash
> cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
> git checkout <删之前的提交> -- "changes/tpops-kafka-collector/.superpowers/sdd/plans/"
> ```
> 当前会话结束时的提交是 `6671aa9`。

### 3.1 调度器 —— 三个最容易踩的点

1. **组 × 实例两维并发**：只做「组并发、组内实例串行」的话，2 个实例单轮墙钟就翻倍，
   逼近合同那条 **P95 ≤ 25 秒**。要 `max(组耗时, 实例耗时)` 而不是乘。
2. **水位 `stream` 键的约定现在是空白** —— 生产代码**尚未调用** `advance_metric`
   （水位推进就是 3.1 的职责），测试里用的是 `'instance-a'` / `'b-class'` 这类随意取值。
   **本次就要定死成含 instance_id**，例如 `f'{instance_id}:{channel}'`。
   不定死的话，多实例一开水位就互相覆盖 —— 「不报错但数据错」。
3. **差分基线恢复只能用 `import_baselines_from`**，**不能用 `put_baseline`**。
   后者写的键没有作用域（`(metric_code, dimension_key)`），采集器按 `scope:key` 去找
   永远找不到 → **重启后速率永远算不出来，且不报任何错**。
   （`import/export_baselines_into` 是整个字典原样搬运，键已带作用域 → 正确。）

### 3.3 入口装配 —— 三块内容

1. **优雅退出**：SIGTERM/SIGINT → 停发新请求 → 等在途请求（**超时 30 秒要放弃等待，
   不能挂死**）→ 原子写水位 → flush producer → **退出码 0**。
   启动期配置缺失 → 非 0 退出码 + 日志指明变量名 + **未向 TPOPS/Kafka 发任何请求**。
2. **平台画像固化**（用户决策，详见 §4）
3. **日志中心化初始化**（用户提问引出的缺口，详见 §5）

### 3.5 全量门禁 —— 有一个硬指标当前不达标

**映射层覆盖率 92.1%，合同要求 ≥95%**，差 2.9 个点。
10 个文件低于 95%，逐个的**未覆盖行号**已列在 `task-3.5-brief.md` 里。

最差的是 `mapping/rules_cross.py` 77%、`mapping/decode.py` 84%、
`mapping/rules_sorting.py` 86%、`mapping/columns.py` 88%。

补测要补**有行为断言**的测试，不是凑覆盖率数字 ——
每条新测试都要能回答「有没有一个能让它失败的小改动」。

---

## 3. 用户已定的三个架构决策（**不要重新设计**）

### ① 平台画像固化：实例 ID 不走配置

连上 TPOPS → `GET /rds/v3.2/instances` 取实例列表 → **选择所有实例** →
固化到 `state_dir/platform_profile.json` → 之后以文件为准。

**「选择所有实例」，不筛 mode、不挑、不取第一个。**
这与 `CLAUDE.md` 的原始设计一致：「命令中心是「instance_id + node_ids 下发」，
无法一次带多实例 → 采集端**按实例循环**」。

- 固化后**不再每轮重新发现**（否则平台列表顺序一变就静默增删采集目标）
- 平台新增实例 → 记 **WARNING** 提示更新画像，**不自动加**
- 平台实例没了 → 记 **ERROR**
- 文件**可手工编辑**，运维换实例不用改代码
- `master_count` 按**唯一 `node_id`** 计数（不是数条目 —— 实测分布式实例
  `role=master` 有 6 条但只有 3 个唯一 node_id，数条目虚高一倍）。
  复用 `tpops/command_center_nodes.py::_select_masters` 的现成去重逻辑。

**好消息**：`fetch_instances` / `fetch_instance_nodes` **已经存在**
（`tpops/instances.py`），不需要新建平台层能力。

### ② 多实例的键隔离约定

| 键 | 约定 |
|---|---|
| 指标/告警水位 `stream` | `f'{instance_id}:{channel}'` ← **本次要定死** |
| 差分基线作用域 | `f'{instance_id}:{node_id}'` |
| 幂等键 | ✅ 已含 instance_id（核实过，不用改） |
| `resourceName` | ✅ 天然区分（node_id 来自该实例的节点清单） |

### ③ JSON 保持不变，不要改成 Python dict

用户曾问「为什么不依赖 JSON」。**结论：JSON 是可再生产物，是迁移能力的关键。**
6/9 的映射表是生成物（生成链的起点是客户 Excel，输出 `.py` 反而是倒退）；
换平台时变的是平台指标码，而那 11 条 A 类指标由生成链自动重新对号。
**当前不动**，若要评估「3 个手工维护的表改 dataclass」，列入收尾后的独立小变更。

---

## 4. 日志的现状与缺口（3.3 要补）

**现状**：全包 **75 处**日志调用、22 个模块都有 `LOGGER = logging.getLogger(__name__)`、
**零 `print` 违规** ✓、级别分布 35 warning / 22 error / 9 info / 9 debug。

**缺口**：**全包零 `basicConfig` / `dictConfig` / `Formatter` / `Handler`**，
且 `config.py` 里**已定义的 `log_level` 与 `log_format`（支持 `text` / `json`）
没有任何代码消费**。

实测后果（Python 的 `lastResort` 兜底，级别 WARNING）：

| 级别 | 现在的实际效果 |
|---|---|
| DEBUG / INFO | ❌ **全丢**（9 处 INFO） |
| WARNING / ERROR | ⚠️ 输出了，但**裸格式** —— 无时间戳、无级别、无模块名 |

**3.3 要补**：① 中心化初始化（新建 `logging_setup.py`，消费那两个配置项，
输出 stderr，`basicConfig(force=)` 在 3.8+ 才有，py37 要自己判 `root.handlers`）
② 第三方库降噪（kafka-python 很吵）③ 异常路径带 `exc_info=True`（现有 22 处
`LOGGER.error` 大多没有堆栈）④ 敏感信息不落日志（凭据 / mini-token / 业务 SQL）
⑤ **元测试**守住「配置真的生效」——否则会重演 `log_level`/`log_format` 那种
「定义了、写了、没人调用」的空转。

---

## 5. 已知的坑（**踩过的，别再踩**）

| 坑 | 说明 |
|---|---|
| **签 pass receipt 会删 workspace** | `ssf execution review --verdict pass` 会 `rmSync` 整个 `plans/<hash>/workspace/`（工具认为简报「可再生」）。**我们的简报不可再生**（含用户决策）。签发后必须 `git checkout <旧提交> -- .../plans/` 恢复。**这是同一类问题的第二次发生**（429 事故那次是并发 agent 的 git 操作） |
| **review 的 base 必须是上一轮 head** | 修复轮的 receipt，`--base` 要等于上一轮审查的 head（否则报 "Repair review base must equal the previous review head"）。wave-2b 那次：`base e072dcd → head 356d52f` |
| **实施方的自查数字可能不准** | wave-2b 修复轮次声称「全包 0 个函数 >50 行」，实测有 51 行的。**控制器要自己核** |
| **实施方一律不 `git commit`** | 并发会抢 `.git/index.lock`。由控制器统一提交 |
| **派发实施方要用绝对路径** | 实施方的 shell 起始目录是主仓库（master），相对路径会解析到 master 上。简报开头要求它先 `ls` 确认 worktree |
| **不改合同与 tasks.md** | 改了会让执行计划 stale → **前四个波次的 receipt 全部作废**。要记的东西写进账本 `progress.md` |
| **不改 `data/mapping/*.json` 内容** | 那是声明与留档，由生成器产出 |
| **`ssf` CLI 位置** | `node ~/.claude/plugins/cache/spec-superflow/spec-superflow/1.0.0/scripts/spec-superflow.mjs <cmd>` |

---

## 6. 账本与证据

**账本（先读这个）**：
`changes/tpops-kafka-collector/.superpowers/sdd/progress.md`
—— 记着历次裁决（M-3 的 x/0 不产出、2.10 偏离裁决、M-2/M-3 涟漪）、
待补清单 **T-1~T-21**、以及各波次的审查结论。

**审查报告**：`changes/tpops-kafka-collector/.superpowers/sdd/reviews/`
- `wave-0-mapping-baseline.md` / `wave-1-skeleton-platform.md` /
  `wave-2a-mapping-data.md` / `wave-2b-channels.md`（首轮，fail）
- `wave-2b-repair-r1.md`（复审，pass）

**实施方报告**：`.../plans/r3-<hash>/workspace/task-*.md`

**修复轮次状态**：`.../plans/r3-<hash>/repair-state/*.json`
—— wave-2b 的 `status: resolved`（`failure_count: 1` → 修一轮 → 通过），
记录了首轮审查的 base/head 与修复轮的 base/head。
**注意这个目录是 plan-scoped 的**，签发 pass receipt 时可能被清（签 wave-3 时留意）。

---

## 7. 硬约束（每个任务都要守）

- **Python 3.7.9**：禁海象 `:=`、`f"{x=}"`、字典合并 `|`、内建泛型 `list[str]`、
  `typing.Literal/Protocol/TypedDict/Final`、`functools.cached_property`、`zoneinfo`、
  `str.removeprefix/removesuffix`。一律用 `typing.List/Dict/Optional/Set/Tuple`
- **不新增依赖**（`kafka-python==2.0.2`、`requests>=2.25,<2.32`；
  健康端点用标准库 `http.server`，不要引 web 框架）
- **单文件 < 400 行、函数 < 50 行**
- **不可变数据**：跨模块用 frozen dataclass / NamedTuple
- **凭据只走环境变量**：禁硬编码、禁写日志、禁进异常消息
- **错误显式处理**：包装成自定义异常并带上下文，**不静默吞异常**；用 `logging`，**不用 `print`**
- **「不静默出 0」**：取不到值就**不产出那条消息**
- **生产代码唯一**：`tpops_kafka_collector/`；`tools/` 与 `data/` 只是参考与离线工具。
  生产代码不得 import `tools.*`，不得读仓库根 `data/`（而不是包内 `mapping/data/`）
- 提交信息用 conventional commits，正文中文

---

## 8. 建议的推进顺序

1. **3.1 调度器**（3.3 依赖它：3.3 要装配调度循环）
2. **3.3 入口装配**（含平台画像固化 + 日志中心化）
3. **3.5 全量门禁**（补映射层覆盖率到 ≥95% + 杀进程重启演练）
4. wave-3 的**独立审查** → 签 receipt → **恢复 workspace** → wave-4 交付文档

> 三个任务可以派**并行实施方**（3.1 与 3.3 有依赖，3.5 依赖前两者），
> 但**实施方一律不 commit**，由控制器统一提交。

---

## 9. 一句话提醒

这个项目最贵的一类缺陷是「**不报错、但数据没有/数据错**」。
wave-2b 审查出的三条 Critical 全是这种形态，而且能溜过去是因为
**回归层只测映射层、从来没走过采集器**。
所以：**每修一条缺陷，都要问「有没有一个能让它失败的小改动」**；
**没有守卫测试的修复不算修完**。
