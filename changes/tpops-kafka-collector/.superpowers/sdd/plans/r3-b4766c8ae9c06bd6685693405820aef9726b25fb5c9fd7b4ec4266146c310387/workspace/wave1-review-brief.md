# Wave 1 审查要求（wave-1-skeleton-platform，revision 3）

给 wave-1 的**独立审查者**。审查者读本简报 + 合同 + `tasks.md` + 规格 + scoped diff。

## 审查对象

- **Base:** `3a04aa5`（wave-0 审查通过的 head）
- **Head:** 见派发提示词
- **Diff 文件:** 见派发提示词（已排除 `data/mapping/expected_values.json` 等大生成物，勿逐行读）
- **波次 ID:** `wave-1-skeleton-platform`

## 逐条要判的

### 1. 合同 Wave 1 完成标准

逐条对照 `changes/tpops-kafka-collector/execution-contract.md` 的 **Wave 1 完成标准**，
判定 满足 / 不满足 / 无法判定。

> **已知的合同口径不一致（不用改合同，在报告里按 ⚠️ 记录）**：
> 合同写「`python -m tpops_kafka_collector.main` 能起、能收 SIGTERM、退出码 0、水位文件完整」，
> 但 `main.py` 由 **wave 3 的任务 3.3** 新建，wave 1 的输出清单里没有它。
> **已获用户裁决**：wave 1 的门禁按「包内骨架与平台层就位 + `tests/unit`、`tests/integration` 全绿」判定；
> 进程入口、信号处理与水位文件的端到端验收归 wave 3（任务 3.3）。

### 2. 逐任务核对 `tasks.md` §波次 1 的 9 个任务

1.1 配置 / 1.2 DTO+异常 / 1.3 重试 / 1.4 序列化器 / 1.5 生产者 / 1.6 客户端 /
1.7 映射表加载 / 1.8 规则引擎 / 1.9 水位。

每个任务的**证明命令**都跑一遍，确认它真的成立（不是只有测试文件存在）。

### 3. 已获裁决的偏离 —— 判定「偏离是否可接受」，不要重复要求改合同

| 编号 | 偏离 | 已获裁决 |
|---|---|---|
| W-1 | **1.7 / 2.1 依赖倒置**：1.7 做「复制映射表进包 + loader + 四类启动期校验」，2.1 退化为「防漂移同步测试 + 补齐 `merged_sql.json`/`sql_merge.sql` 的复制」 | **用户已确认**。理由：1.7 的完成标准「运行时不需要仓库根目录」在 wave-1 结束时才真的成立；合同与 `tasks.md` 均未改动 |
| W-2 | **1.8 把 `mapping/rules.py` 拆成 9 个同级模块**（门面 52 行） | 按合同「单文件 < 400 行」硬约束必须拆（wave-0 先例）。**但这些文件没写进合同的 wave-1 输出清单** → 登记为待补，不要求现在改 `tasks.md`（改了会让执行计划 stale → wave-0 的 receipt 作废） |
| W-3 | **1.5 多一个 `kafka/records.py`**（`producer.py` 527 行 → 拆） | 同 W-2 |
| W-4 | **1.8 的 R-M3 选「执行 `group_by`」而非「删参数」** | 已接受。理由：`group_by` 由 `dimension_bindings` 自动派生（`build_itscada_mapping.py:420`），且 wave-0 既有测试 `test_dimension_metrics_are_always_per_row` 断言该参数必须存在；留档 68 行 68 个不同 `relname` → 两种口径输出相同 |
| W-5 | **1.5 的 `enable_idempotence` 适配**：kafka-python 2.0.2 的 `DEFAULT_CONFIG` 没有这个键（实测），直传会抛 `KafkaConfigurationError` → 逻辑配置显式写 `False`，构造时按客户端支持的键过滤并记 INFO | 已接受。2.0.2 本就无法开启幂等，语义目标（幂等关闭）**已达成**；字面传给 `KafkaProducer` 与锁版本冲突。**请独立核实这个前提**（读 kafka-python 2.0.2 的 `DEFAULT_CONFIG`） |
| W-6 | **跨视图比率的规格矛盾**：`specs/mapping-and-rules/spec.md:156` 写「分母为 0 时结果为 0」，与同文件「缺值不产出假数据」冲突。1.8 实现为「x/0 → 不产出；0/x → 产出 0.0」 | **控制器已裁决**（账本有记录）：x/0 不产出（比率无定义，输出 0 等于把"算不出来"说成"真的是 0%"）；0/x 产出 0.0。**规格那一行必须改，但执行期间不能改** —— 实测改 `specs/**` 会让 `artifacts_hash` 变 → 执行计划 stale → receipt 只在 `plan_hash`/`plan_revision` 匹配时才被承认（`execution-plan.mjs:197`）→ wave-0 已签的 `pass` receipt 作废、wave-0 要重审。→ 规格改写列入**本变更收尾后的独立小变更**。请在报告里确认这个偏离已被显式记录，并确认实现与裁决一致 |

### 4. 账本里指向 1.8 的遗留项 —— 判定是否真的关闭

`changes/tpops-kafka-collector/.superpowers/sdd/progress.md` 的 Minor 台账：

| 台账号 | 内容 | 1.8 应做的 |
|---|---|---|
| M-2 / R-M5 | `mapping/rules.py` 超 400 行 | 拆文件 |
| M-3 | `ratio_across` 分母取不到返回 `0.0` 而非「不产出」 | 改成不产出 |
| R-M1 | `raw_filtered` 多命中语义规格没定义 | 定口径 + 补合成多命中守卫 |
| R-M2 | **`top_n` 排序实为空操作**（`_to_number` 排序时间戳字符串 → 全退化成 `0.0`） | 按 PG `ORDER BY` 口径真正排序 |
| R-M3 | `raw` + `per_row` 的 12 条表级指标带 `group_by`，引擎完全忽略 | 见 W-4 |

### 5. 1.4 留下的 4 条尾巴 —— 判定处置是否可接受

| 编号 | 内容 |
|---|---|
| S-1 | `SerializerError` 定义在 `kafka/serializer.py` 内，未进 `errors.py` 异常体系（与 1.2 把 `DecodeError` 收敛到 `errors.py` 不一致） |
| S-2 | 序列化器**硬编码**字段表，1.4 的测试比对的是**仓库根** `data/mapping/itscada_field_map.json`；**包内副本**（1.7 复制的）目前**没有守卫** → 需一条断言「包内副本的字段表 == 序列化器硬编码的字段表」 |
| S-3 | `dumps()` 用紧凑分隔符 → 消息体**逐字节**格式（对外契约面） |
| S-4 | `allow_nan=False` → NaN 抛 `SerializerError`；wave 2 的采集器要把 NaN 当 `null` |

### 6. 控制器在 wave-1 期间自己做的两处修复 —— 判定是否正确

- **幂等键集合改成真 LRU**（`92e3223`）：规格 `delivery-and-state/spec.md:170` 写的是 LRU，
  实现是插入序淘汰（FIFO）。改为 `add()` 遇重复键刷新新鲜度、`contains()`（纯查询）不刷新。
  请独立判定这个语义是否正确（尤其「重复键算不算一次使用」）。
- **`DecodeError` 收敛到 `errors.py`**（1.2 做的）：原先定义在 `mapping/decode.py`，
  在 `errors.py` 另定义同名类会有两个不同的类 → `except` 抓不住、`isinstance` 分裂。

### 7. 测试质量（可证伪性）

对新增测试判断「有没有一个能让它失败的小改动」。重点看：
- 1.4 的**唯一出口守卫**（AST 扫 `json.dumps`）—— 换个实现方式还能守住吗？
- 1.8 的 13 种规则类型测试 —— 正常路径与边界路径是否都测到了？
- 1.5 的生产者测试 —— 是否**纯 mock**、有没有真的连 broker？
- 1.7 的「运行时不需要仓库根目录」—— 是怎么证明的？那个证明够硬吗？
- 1.3 的退避序列断言 —— 会不会 flaky？

### 8. 越界与治理红线

- **不得改** `execution-contract.md` 与 `tasks.md`（改了会让执行计划 stale、receipt 签不出来）。
  核对 diff 有没有碰它们。**需要登记的新文件**（W-2/W-3）走待补清单，不要要求现在改。
- **不得改** `data/mapping/*.json` 的内容（除生成物重生成，且必须由生成器产出 + `git diff` 可复现）。
- 凭据是否只走环境变量、有没有写进日志/异常消息/测试快照。
- `ruff check tpops_kafka_collector/` 必须为 0；`tests/test_py37_compat.py` 必须通过。

## 审查方式

- **只读 diff 文件一次**。diff 的上下文行**就是**被改文件的内容 —— 不要另外 Read 被改文件，
  除非某个你必须判断的 hunk 在函数中间被截断（那就在报告里说明）。
- **不要跑 git 命令**（diff 文件已给了提交列表与完整 diff）。
- 不要爬整个代码库。只有为了评估一个**你能具体命名的风险**才看 diff 之外的代码，
  **每个风险做一次聚焦检查**，并写明「风险是什么、查了什么」。
- 需要跑测试/静态检查时在 worktree 里跑（`conda run -n tpops python -m pytest -q`），
  **不要修改任何文件**。

## 判定口径

- **Critical**：安全漏洞、数据丢失、会导致生产事故的缺陷
- **Important**：逻辑缺陷、规格违反、合同完成标准未满足、缺失必需测试、越界改动
- **Minor**：可维护性、文档、可选改进
- **Critical 或 Important 存在 → verdict 必须是 `fail`**，并给出可执行的修复指引
- 只有 Critical/Important 都清零才能给 `pass`；Minor 要逐条列出并给处置建议

## 交付

1. **审查报告**写到 `changes/tpops-kafka-collector/.superpowers/sdd/reviews/wave-1-skeleton-platform.md`
   （wave-1 的报告，不覆盖 wave-0 的 `wave-0-mapping-baseline.md`）。
   内容必须包含：
   - 结论（pass / fail）与逐条依据
   - 合同 Wave 1 完成标准逐条判定表
   - 9 个任务的证明命令实际结果
   - 第 3 节 6 条已裁决偏离的「偏离是否可接受」判定
   - 第 4 节 5 个遗留项、第 5 节 4 条尾巴、第 6 节 2 处控制器修复的判定
   - 测试质量的可证伪性判断
   - 越界与治理红线的核对结果
   - Critical / Important / Minor 分级清单
   - **待补清单**（要登记进合同输出清单但本次不改 `tasks.md` 的条目）
   - 若有 Critical/Important：具体的修复指引（要能直接派发）
2. **不要**签发 receipt，不要改 `.spec-superflow.yaml`、不要改 `execution-contract.md` / `tasks.md`、
   不要改任何被审查的代码。receipt 由控制器签发。
3. 最终回复：verdict、各分级的条目数、Critical/Important 清单（若有）、报告路径。
