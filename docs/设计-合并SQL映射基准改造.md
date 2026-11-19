# 合并 SQL 映射基准改造（设计）

| 项 | 值 |
|---|---|
| 日期 | 2026-11-19 |
| 状态 | **待批准**（两个待决问题已拍板，见 §10） |
| 范围 | 映射层（`tools/mappings/` + `data/mapping/`）+ 评审清单 + 开发方案同步 |
| 不含 | 采集端 `tpops_kafka_collector/`（尚未建立，本次不碰） |

---

## 1. 为什么要做这三件事

三件事是一根：**合并 SQL 的形态变了，映射层没跟上**。

### 1.1 根因：82 个 B 类指标静默出 0

`data/merged_sql/sql_merge.sql`（2026-11-18 实测，8.39 秒）把 11 条合并 SQL **再合成 1 条**，
返回体的形态从「`result` = 行列表」变成「`result` = **一行 11 个 `json_agg` 列**，列值是 JSON 字符串」：

```json
[{"stat_database": "[{\"datname\":\"lz\",\"numbackends\":0,…}]",
  "stat_user_tables": "[…]", "locks": "[…]", …}]
```

映射层 `data/mapping/response_rules.json` 里 11 个 `cmd_rows_*` 形状全是
`kind=rows_from_result, payload_path=["result"]`，假设的是**旧的 11 条模型**。
`tools/mappings/decode.py:80` 的 `_rows_from_result` 把上面那个**单行对象**直接当行列表返回，
于是每个视图的列拿到的是一整段 JSON 字符串。

把 `data/merged_sql/sql_merge_probe_result.json` 的真实返回体喂进去：

```
gaussdb.Deadlocks                          -> [({}, 0, 0)]
gaussdb.Xact_commit                        -> [({}, 0, 0)]
GAUSSDB.[{#DATNAME}]_cache_hit_ratio       -> [({}, None, 0)]
gaussdb.locks.get_metrics[{#DBNAME}]       -> [({}, 0, 0)]
gaussdb.Timezone                           -> []
```

**82 个 B 类指标全部是 0 / None / 空列表，且不报错。**
方案 §7.1.1 说的「错的数据比没有数据更危险」——这正是那个场景。

现有 164 个测试拦不住：回归样本 `data/metric_response/` 是**旧的 11 条模型**
（98 份样本：监控指标 11 / **命令中心 SQL 82** / 诊断优化 4 / 实例 1），
**没有任何测试回放 `sql_merge_probe_result.json`**。

### 1.2 派生一：P1 的退出凭证缺失

方案 §7.1.1 把「P1 方案修订」设为**编码准入门槛**（"这一步不通过，不允许进入 P2"），
退出准则写的是「**评审清单逐条标注"已修 / 已接受风险"**」。
仓库里没有这份清单，git 历史里也没有（`git log --all --diff-filter=A` 全文搜无命中）。

→ **已定（本次决策）**：P1 的退出准则**换掉**，不再依赖那份找不到的原件，改为三条**可验证**的：

| # | 新退出准则 | 怎么验 |
|---|---|---|
| 1 | 方案 v2.1 定稿 | 文档存在且 §3 的定稿章节无「待定」字样 |
| 2 | 映射表**可重复生成** | 重跑 `tools/build_*.py` 后 `git diff` 为空 |
| 3 | 启动期校验通过 | 启动时列名/占位符/可提供性三类校验全过 |

重建的 24 条清单**降级为「现状核对表」**（§5.4），随文档附上，不再当退出凭证、也不声称是原件。

### 1.3 派生二：文档与实现漂移

| 位置 | 现状 | 与实现的差 |
|---|---|---|
| §4.4 / §4.4.1 / §4.4.4 / §7.1.1 / CLAUDE.md | 6 处 + 2 处「11 条合并 SQL」 | 实际是 **1 条** |
| §4.4.2 SQL 红线 | 无输出上限 | 平台 **102400 B 硬上限**，超了整条任务失败（不是截断） |
| §4.4.3 并发结论 | 「并发 ≥4 条随机失败」 | 合并后并发问题消失，结论过时 |
| §4.8 性能预算 | 按 10 条 SQL 并发算 8~15 秒 | 1 条实测 8.39 秒 + 分页多轮 |
| §5.6.4 启动校验 | 列来源写 `data/command_center/results/` | **该目录 11-13 已删** |
| §0.2 留档索引 | 引用 `data/metric_verification/`、`data/command_center/results/` | **两个目录均已删** |

→ 本次**以实现为准**改文档（§5.4）。

---

## 2. 目标 / 非目标 / 反目标

### 目标

- **G1** 82 个 B 类指标在「单条合并 SQL」形态下产出**真值**，且有回归测试守住。
- **G2** 合并 SQL 的「一行 N 个 json_agg 列」成为映射层的一等形态。
- **G3** 24 条评审意见有一份逐条可追溯的现状核对表；P1 的退出准则换成三条可验证的。
- **G4** 开发方案与 CLAUDE.md 与实现一致，不再有「11 条」「无输出上限」这类会误导编码的表述。

### 非目标

- **不写采集端**。`tpops_kafka_collector/` 尚未建立，本次只把映射层和数据改到「采集端能直接接」的状态。
- **不拆档**。合并 SQL 保持 **1 条全量**（已决策，代价见 §4.2）。
- **不改客户字段信封**。`itscada_field_map.json` 的 `metric_message` / `alarm_message` 一个字不动。

### 反目标

- **不做静默兜底**。解不出来必须抛错，不能像现在这样"解不出来就出 0"。
  —— 这是 §5.6.4 那条设计原则（"把映射表写错从运行期静默产出错误数据变成启动期立即失败"）的延伸。

---

## 3. 约束

| # | 约束 | 来源 |
|---|---|---|
| C1 | Python 3.7.9 语法红线 | CLAUDE.md |
| C2 | 表驱动：列名/口径写在映射表里，代码不写死 | §5.6 |
| C3 | 平台 `sql_text` ≤ 10000 字符；`limit` 参数 ≤ 1000；结果集 ≤ 1000 行（静默截断）；**结果总体积 ≤ 102400 B（整条任务失败，不是截断），且每个 master 各算各的** | `data/merged_sql/README.md` |
| C4 | `json_agg` 对零行返回 NULL → 平台渲染成**空字符串 `''`**；客户端必须当空数组处理 | 同上 |
| C5 | 现有 164 个测试必须继续通过 | — |
| C6 | 映射表由 `tools/build_*.py` 机械重生成，禁止逐格手改 | §7.1.1 |
| C7 | **兼容期**：旧的 11 条模型样本（`data/metric_response/` 82 份）仍可解、其测试不改 | 本设计新增 |

---

## 4. 方案选择

### 4.1 解码形态：**两条路径并存，不改既有路径**

Reviewer 提出的核心质疑（Q1）是对的：如果给既有的 11 个 `cmd_rows_*` 形状加
`json_column`，那这 11 个形状就被**两种不兼容的 body** 共用——
旧样本（`result` 就是行列表，没有 `stat_database` 这个键）解码时会取到缺失的键。
三条候选行为各坏一处：抛错 → 82 条规则的解码测试全挂；返回 `[]` → `test_every_rule_produces_a_value` 挂；
「行列表原样返回」→ 静默产出错误数据，正是本次要消灭的东西。

**选定：既有路径一个字不动，新增一条合并路径。**

```
路径 P（既有，不动）
  decode(body, shape_cmd_rows_X)  → RuleContext(rows=行列表)
  用途：data/metric_response/ 的 82 份旧样本 + 其测试 → C5/C7 天然满足

路径 M（新增，生产路径）
  decode_merged(body, views)      → MergedContexts
      .by_sql_key:  Dict[sql_key, RuleContext]   每个视图一个上下文
      .rows_by_key: Dict[sql_key, List[row]]      供跨视图规则用
  用途：采集端拿合并 SQL 的返回体
```

**规则侧一行不改**：每条规则已经带 `locate.sql_key`（= 视图名），采集端按它取对应的上下文：

```python
merged = merge_pages(page_bodies, views)          # §5.2
for rule in rules_of_channel:
    ctx = merged.by_sql_key[rule.locate.sql_key]
    values = evaluate(rule, ctx)
```

**视图清单不放在既有 shape 里，另起一段**（避免一个 shape 两种 body）：

```json
"merged_views": {
  "stat_database": {
    "json_column": "stat_database",
    "paged": false,
    "sql_ref": {"catalog": "data/merged_sql/sql_merge.sql", "sql_key": "stat_database"}
  },
  "stat_user_tables": {
    "json_column": "stat_user_tables",
    "paged": true,
    "sql_ref": {"catalog": "data/merged_sql/sql_merge.sql", "sql_key": "stat_user_tables"}
  },
  …
}
```

这样既有 11 个 `cmd_rows_*` 形状、82 份旧样本、164 个测试**全部不受影响**（C5/C7 成立），
而生产路径有了自己的、可声明的入口。

**为什么不选其余方案**

| 方案 | 否决理由 |
|---|---|
| 给既有 shape 加 `json_column`，靠自动探测区分形态 | 一个 shape 两种 body；自动探测要么抛错要么静默错数据（Q1 的三种行为各坏一处） |
| 新增 `rows_from_merged_sql` kind，`ctx.rows` 改成 `ctx.rows_by_view` | 82 条规则全部要改、全部要回归 |
| 改映射表整体结构（规则内联视图名 + 列名） | 推翻表驱动，与 §5.6 冲突；重生成脚本全部重写 |

### 4.2 合并 SQL 条数：**1 条全量**（已决策）+ 接受的三个风险

按档位拆 3 条（快 60s / 中 120s / 慢 300s）本可保住「到期驱动」的档期语义，
但已决策保持 **1 条**。代价与对策：

| 风险 | 实测依据 | 本次对策 |
|---|---|---|
| **表级指标进不了 60 秒档** | 1 万张表 = 100 页，并发 3 也要 **6 分钟/轮**（每页 ≈7 s 固定下发开销） | 记入文档 §4.8 与交付说明；映射层提供多页拼接（§5.2），采集端按「一页一页追着走 + 记录已取到哪个 offset」实现 |
| **单点故障：一个视图撑爆，整批陪葬** | `stat_user_tables` + `dbstat_sum` 占合并结果 75%（22251 B + 17611 B / 53300 B），行数随业务表增长 | README 对策 ①（内层 limit）**已落地**；本次补对策 ③（result 长度 >90 KB 自检告警）；对策 ②（拆第二条）**已否决 → 记为已接受风险** |
| **空页不是空数组** | `json_agg` 零行 → NULL → 平台渲染成 `''` | 解码器硬要求：`''` / `null` → `[]`（C4） |

### 4.3 告警幂等键：`(alarm_id, instance_id, first_alarm_time, status)`

**已定**（本次决策）：

| 决策点 | 取值 | 理由 |
|---|---|---|
| 粒度 | **收敛条目** | 投递单位就是收敛条目（§3.7「主条目」）。明细行归并进 `details` 数组，不单独投递 |
| `status` 取值 | **平台原值**（`activated` / `recovered` / `cleared` / `cleared_by_user`） | 用 ITSCADA 枚举会把 `recovered` 和 `cleared` 压成同一个键，两者在客户侧不可区分 |
| `node_id` | **不进键** | 收敛条目没有 `node_id`；它的 `resourceName` 是「按 `alarm_id` 关联明细行回填节点，回填不了落到第一个 master CN」——**回填到的节点可能每轮不一样**，进键必然重复投递 |

```
幂等键 = (alarm_id, instance_id, first_alarm_time, status)
```

效果：
- 触发（`activated`）与恢复（`recovered`）是**两个不同的键** → 恢复消息发得出去，
  且 `recoverTime`（客户字段表里「status=0 时必填」）能填上。
- 重复触发期间 `status` 不变 → 不会被重复发。
- 同一 `alarm_id` 的不同触发轮次 `first_alarm_time` 不同 → 天然是不同条目。

**为什么不能加 `last_alarm_time`**：恢复后它**不变**（实测 last=1793767902000 ≠ clear=1793771169000），
加进去也救不了恢复消息。这正是 §3.7 那句「`last_alarm_time` 变化 → 幂等键变化」想表达、但键里漏掉的东西。

---

## 5. 实施细节

### 5.1 映射层

#### 5.1.1 `tools/mappings/decode.py`

**既有 `_rows_from_result` 不改**（C5/C7）。新增：

```python
def decode_merged(body, views) -> MergedContexts:
    """把「一行 N 个 json_agg 列」的合并返回体解开成每个视图的行列表。

    - result → json.loads → 行列表（必须是 1 行）
    - 对每个视图：取 行[views[sql_key]['json_column']]
        ├─ 值是字符串且为空/空白  → []          ← C4 空页
        ├─ 值是字符串            → json.loads → 行列表（失败抛 DecodeError）
        └─ 值已是 list           → 直接用
    - rows_by_key 同时填好（供跨视图规则用）
    """

def merge_pages(bodies, views) -> MergedContexts:
    """把多页返回体按视图拼接（分页只在 paged=true 的视图上有第 2 页起）。"""
```

- 解析失败 → 抛 `DecodeError`（**不静默出 0**，符合反目标）。
- `result` 不是 1 行的列表 → 抛 `DecodeError`（合并形态下就该是 1 行）。

#### 5.1.2 `data/mapping/response_rules.json`（**由生成器产出，禁止手改**，C6）

- **既有 11 个 `cmd_rows_*` 形状：一字不改**（保 C5/C7）。
- **新增 `merged_views` 段**：11 个视图，每个含 `json_column` / `paged` / `sql_ref`
  （格式见 §4.1）。
- **既有 98 条 `rules`：一字不改**（`locate.sql_key` 已经是视图名，够用）。
- `channels.command_center_sql.api_chain` 改写：原来写「按 sql_key 下发合并 SQL」，
  现在是「下发 **1 条**合并 SQL（`sql_merge.sql`），分页视图按 offset 推进」。

**`sql_ref` 只放指针，不内联 SQL 文本**：合并 SQL 有 53 KB，内联到 11 个视图 = 583 KB，
映射表会失控。采集端按 `catalog` 读文件，`sql_ref` 只声明「这条规则的数据来自哪个文件的哪个视图」。

#### 5.1.3 `tools/build_response_rules.py`

- 从 `data/merged_sql/sql_merge.sql` 解析出 11 个视图名（`json_agg(t) as <sql_key>` 的别名），
  与 `merged_sql.json` 的 `batches[].sql_key` **交叉校验**，对不上就报错退出。
- `paged` 属性取自 `merged_sql.json` 的 `paging.views[].paged`，**不重新发明**。
- 生成 `merged_views` 段与改写 `api_chain`。

#### 5.1.4 告警幂等键

- **生成器是 `tools/build_itscada_mapping.py::build_alarm_rules()`**（它产出 `alarm_rules.json`）。
  `tools/build_response_rules.py:115` 的 `IDEMPOTENCY['alarm']` 是**死代码**
  （从未写进任何文件），一并删掉，别留两处。
- `alarm_rules.json` 新增 `idempotency` 段，键 = `(alarm_id, instance_id, first_alarm_time, status)`
  （粒度与取值见 §4.3），与指标侧的声明形式对齐。

### 5.2 分页：多页拼接放映射层

**为什么不能放采集端**：合并 SQL 分页后，`stat_user_tables` / `dbstat_sum` 每页只回来一部分行。
若采集端逐页解码、逐页求值，`row_policy: sum` 的指标会**按页出值**（而不是整视图的值），
`counter_delta` 的基线也会变成按页——**表一多就静默出错**，而且错的正好是那 22 个表级指标。

**选定**：映射层提供 `merge_pages(bodies, views)`，按 `merged_views[].paged` 把多页按视图拼成
**完整视图行列表**，再交给规则引擎。采集端只负责「把 N 页的 body 拿回来」，
不参与行拼接 —— **规则的语义边界（整视图 vs 单行）留在映射层**，与 §5.6「映射层是唯一事实源」一致。

采集端流程（M5 用，本次不实现）：

```
offset = 0
bodies = []
while True:
    body = 执行合并SQL(offset)          # 每页一次下发，8~9 秒
    bodies.append(body)
    if 视图已取完: break
    offset += 页容量
merged = merge_pages(bodies, views)     # 映射层拼
for rule in rules: evaluate(rule, merged.by_sql_key[rule.locate.sql_key])
```

### 5.3 跨视图指标（`ratio_across`）：本次顺带修

`gaussdb.Total_pct` 与 `GAUSSDB.session_usage` 是**跨视图比率**：
分子取 `activity_agg`，分母取 `stat_database`。

- `tools/mappings/rules.py:363` 的 `_side_value` 从 `ctx.rows_by_key` 取数，
  而 **`rows_by_key` 目前只有 `tests/test_rules.py:281` 手工构造过，没有任何生产路径填过它**
  → 这两个指标**在改造之前就恒为 `0.0`**，且没有测试发现（0.0 不是 None，测试看不出来）。
- 合并形态下一个返回体里同时有 `activity_agg` 与 `stat_database`，
  `decode_merged` 顺手把 `rows_by_key` 填好 → **这两个指标第一次能出真值**
  （探针里 `23/29 = 0.79`）。

→ 本次修（随 `decode_merged` 一起落地），并在测试里钉住具体值。

### 5.4 评审清单 → **现状核对表**

新建 **`docs/评审意见现状核对表.md`**（**注意：不是「退出凭证」**）：

- 24 条（12 BLOCKER + 12 MAJOR），每条含：
  `编号 / 级别 / 评审意见 / 来源（明载·反推） / 现状落点 / 判定（已定·已解决·未关闭·已接受风险） / 关闭方式`
- **来源标注必须诚实**：`【明载】` = 方案 §7.1.1 / §0.3 自己写明是评审产出的；
  `【反推】` = 从 v2.1 正文对应章节反推。
  仓库里能查到的评审线索**只有 6 条**（§7.1.1 点名的三个 BLOCKER + 三个 MAJOR，加正文里的 `B-01`），
  其余 18 条为反推 —— **没有任何一条声称是原件**，文首写明这一点。
- **它的作用**：给「现在还有哪些没关」一份可追溯的台账。
  **不承担 P1 的退出凭证职能**（退出准则已按 §1.2 换成三条可验证的）。
- 逐条核对结论：约 20 条已定、3 条要改文档（M-09 / B-12 / M-12）、1 条本次修（B-05 幂等键）。

### 5.5 文档同步（以实现为准）

**范围：repo-wide，逐条判断，不做机械 grep。**
既有若干「11 条合并 SQL」是**合法提及**（如 CLAUDE.md 的「已清理的产物」段刻意列出已删目录），
naive grep 会误报。已知需要处理的点：

| 文件 | 位置 | 改法 |
|---|---|---|
| 开发方案 | 行 14 / 50 / 51 / 64 / 280 / 470 / 645 / 931 / 1247-48 / 1671 / 1829 / 1942 / 2409-10 / 2480 / 2516 / 2633 / 2682 / 2852-53 | 逐条判断：涉及「合并 SQL 条数」「回归样本来源」「已删目录引用」的改；纯历史叙述的保留 |
| 开发方案 | §0.2 留档索引 | `data/metric_verification/` → `data/metric_response/`；`data/command_center/results/` → `data/command_center/` + `data/merged_sql/` |
| 开发方案 | §3.7 告警语义 | 幂等键写明含 `status`；补「键里必须有 status，否则恢复事件被去重永久丢弃」 |
| 开发方案 | §4.2.5 命令中心链路 | 说明合并 SQL 的返回形态与二次解析要求 |
| 开发方案 | §4.4 合并 SQL 目录 | 「11 条」→「1 条（sql_merge.sql）」+ 视图清单 + 每个视图的分页属性 |
| 开发方案 | §4.4.2 SQL 红线 | **新增**：102400 B 输出硬上限（超了整条任务失败）、`limit` ≤1000、结果集 ≤1000 行静默截断、`sql_text` ≤10000 字符、空页 `''` 的处理 |
| 开发方案 | §4.4.3 并发结论 | 改为「合并后 1 条，实测 8.39 秒；分页并发最优 3（保守 2），≥4 随机失败」 |
| 开发方案 | §4.5.4 幂等键 | 告警键加 `status` |
| 开发方案 | §4.6.3 到期判定 | 1 条 SQL → 全部视图共享一个周期；表级视图完整覆盖周期 ≈ 页数 × 7 s |
| 开发方案 | §4.8 性能预算 | 按 1 条 + 分页重算；补「1 万张表 = 100 页 ≈ 6 分钟一轮」 |
| 开发方案 | §5.6.4 启动校验 | 列来源改为 `data/metric_response/` + `sql_merge_probe_result.json`；补「生产形态是合并形态，校验须对合并形态成立」 |
| 开发方案 | §6.2 回归样本回放 | 补合并形态的回归样本 |
| 开发方案 | §7.1.1 | 「11 条合并 SQL」→「1 条」；补「表级指标降频 ≥300 秒档」；**P1 退出准则换成三条可验证的**（§1.2） |
| CLAUDE.md | 行 96 / 84 | 2 处「11 条」；`data/merged_sql/` 说明补分页与 102400 B 上限 |
| `tools/README.md` | 行 168 | 「11 条合并 SQL」 |
| `data/merged_sql/candidate_sqls.json` | 行 3 | 「11 条合并 SQL」 |
| `data/tpops_api_catalog.json` | 行 144 | **不改**：这是 API 目录，产物由生成器写，改了会被重生成覆盖 |

### 5.6 回归测试

新增 `tests/test_merged_sql_mapping.py`：

| 用例 | 断言 |
|---|---|
| `test_merged_probe_decodes_to_rows_per_view` | `sql_merge_probe_result.json` 经 `decode_merged` 后，**每个视图的行数与列名**与探针留档一致 |
| `test_merged_probe_values_match_golden` | **82 个 B 类指标逐条比对其值与一张固化的期望值表**（表由探针生成后写死在测试里） |
| `test_merged_probe_cross_view_ratio` | `gaussdb.Total_pct` / `GAUSSDB.session_usage` 拿到跨视图比率（探针里 23/29 = 0.79），不为 0 |
| `test_merged_probe_empty_view_becomes_empty_list` | 空视图（`''` / `null`）→ `[]`，不抛异常 |
| `test_merged_probe_invalid_json_raises` | 列值是坏 JSON → 抛 `DecodeError`（不静默出 0） |
| `test_merged_probe_non_single_row_raises` | `result` 不是 1 行 → 抛 `DecodeError` |
| `test_merge_pages_concatenates_paged_views` | 两页拼接后，`stat_user_tables` 行数 = 两页之和；固定视图只取第 1 页（不重复累加） |
| `test_old_eleven_sql_shape_still_decodes` | `data/metric_response/` 的 82 份旧样本仍能解（兼容性，C7） |

**验收判据要改**（Reviewer Q3）：不能写"不得为 0"——
`gaussdb.Deadlocks` 实测就是 0（该实例没有死锁），
5 个 `*_records_per_second` 首轮按 §3.4「首轮无基线不产出值」就是 `null`，
`gaussdb.locks.accessexclusive[{#DBNAME}]` 的过滤条件在探针里匹配不到行。
实测分布：**58 个非零 / 23 个合法为 0 或 null / 1 个无匹配行**。
→ 判据改为「**每个指标的值等于 SQL 实际返回的值**」，用固化的期望值表逐条比对。

**另外**：`build_response_rules.py` 里 `_assert_columns_exist`（约 401 行）现在按**旧的 per-view 样本**校验列名，
`evidence.observed`（行数、列名）也来自旧样本。改造后生产形态是合并形态，
→ **新增一条对合并形态的列校验**（从 `sql_merge_probe_result.json` + `merged_sql.json` 推导），
旧的保留（作为历史证据）。

---

## 6. 验收标准

| # | 验收项 | 判定 |
|---|---|---|
| A1 | 82 个 B 类指标在合并 SQL 形态下的值 **等于期望值表**（含合法为 0 / null 的 23 个、跨视图比率 2 个） | `test_merged_probe_values_match_golden` + `test_merged_probe_cross_view_ratio` 通过 |
| A2 | 旧的 11 条模型仍可解、其测试未改 | `test_old_eleven_sql_shape_still_decodes` 通过；`git diff --stat tests/` 只含新增文件 |
| A3 | 解码失败显式抛错 | `test_merged_probe_invalid_json_raises` / `test_merged_probe_non_single_row_raises` 通过 |
| A4 | 现有 164 个测试全绿 | `pytest -q` |
| A5 | 现状核对表存在且逐条有判定（**不承担退出凭证职能**，见 §1.2） | 人工核对 |
| A6 | 开发方案与 CLAUDE.md 无「11 条合并 SQL」残留、无已删目录引用 | **逐条判断**后的清单核对（不做机械 grep，见 §5.5） |
| A7 | 映射表由生成器产出、可重复生成 | 重跑 `build_response_rules.py` 后 `git diff` 为空 |

---

## 7. 不在本次范围（但记录在案）

| # | 事项 | 为什么现在不做 |
|---|---|---|
| N1 | `scope.node.policy = per_master` 的落地语义：合并 SQL 在 3 个 master 上各执行一次，**实例级指标**（`stat_database` 26 个等）会产出 3 份相同值、落到 3 个不同 resourceName | 该策略目前**只在 `build_response_rules.py:430` 声明，无任何消费方**（采集端未建）。等 M5 实现采集端时按实际需要定：要么实例级只取一个 master，要么明确接受 3 份冗余 |
| N2 | A 类多 series 归集（§3.9 第 9 项） | 客户口径未定，两段式代码默认关闭 |
| N3 | 24 小时档、表级降频的调度器实现 | 采集端未建 |
| N4 | 客户侧死线：Kafka 地址 + Topic 名、慢 SQL 外发口径 | 客户侧，逾期中 |

---

## 8. 改动清单（文件级）

```
改  tools/mappings/decode.py                    新增 decode_merged / merge_pages；既有 _rows_from_result 不动
改  tools/build_response_rules.py                生成 merged_views 段、改 api_chain、删死代码 IDEMPOTENCY['alarm']、
                                                增合并形态的列校验
改  tools/build_itscada_mapping.py               alarm_rules.json 增 idempotency 段（★ Reviewer 更正：原文档写错文件）
改  data/mapping/response_rules.json            ← 生成物，不手改
改  data/mapping/alarm_rules.json               ← 生成物，不手改
增  tests/test_merged_sql_mapping.py
增  docs/评审意见现状核对表.md
改  docs/开发方案-GaussDB指标与告警采集.md      §0.2 §3.7 §4.2.5 §4.4 §4.4.2 §4.4.3 §4.5.4 §4.6.3 §4.8 §5.6.4 §6.2 §7.1.1 + 行级清理
改  CLAUDE.md                                   2 处「11 条」+ merged_sql 说明
改  tools/README.md、data/merged_sql/candidate_sqls.json   「11 条合并 SQL」
改  data/merged_sql/README.md                   补「合并 SQL 的解码契约」小节
```

---

## 9. 备选方案与代价总表

| 决策点 | 选定 | 否决项与代价 |
|---|---|---|
| 解码形态 | 两条路径并存 + `merged_views` 段 | 单 shape 双形态：三种行为各坏一处（Q1）；改 `ctx.rows` 为 `ctx.rows_by_view`：82 条规则全回归 |
| 合并 SQL 条数 | 1 条全量 | 3 条按档位：保住档期语义，但多 2 次往返；1 条的代价记于 §4.2 |
| 告警幂等键 | `(alarm_id, instance_id, first_alarm_time, status)`，收敛条目粒度、平台原值、不含 node | 加 `last_alarm_time`：**实测不成立**；`clear_alarm_time`：语义绕；含 node：回填节点会漂移 → 重复投递 |
| 分页拼接位置 | 映射层 `merge_pages` | 采集端拼：`row_policy: sum` 会按页出值，表一多就静默出错 |
| 跨视图比率 | 本次修 | 不修：这两个指标继续恒为 0.0，且合并形态已让修它几乎免费 |
| 文档口径 | 以实现为准改文档 | 以文档为准改实现：等于放弃已实测的 8.39 秒方案，且要重做分页 |

---

## 10. 已决事项记录（本次拍板）

| # | 问题 | 决定 | 落在哪 |
|---|---|---|---|
| T1 | 24 条评审清单重建稿能否当 P1 的退出凭证？ | **不能当凭证**。P1 的退出准则换成三条可验证的（方案 v2.1 定稿 / 映射表可重复生成 / 启动校验通过）；重建稿降级为「现状核对表」 | §1.2、§5.4、§5.5（§7.1.1） |
| T2 | 告警幂等键的粒度与 `status` 取值？ | **收敛条目粒度 + 平台原值 + 不含 `node_id`** → `(alarm_id, instance_id, first_alarm_time, status)` | §4.3、§5.1.4 |
| — | 合并 SQL 条数 | **1 条全量** | §4.2 |
| — | 跨视图比率（`Total_pct` / `session_usage`） | **本次修** | §5.3 |
| — | 分页拼接位置 | **映射层** `merge_pages` | §5.2 |
