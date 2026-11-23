# 控制器补充说明 —— 任务 3.3（入口装配与信号处理）

> 本文件是**控制器**写给 3.3 实施方的补充，补充的是**简报里没有写、
> 但 wave-3 必须落地**的事项，以及若干简报里写得太粗、需要定死的口径。
> **与 `task-3.3-brief.md` 冲突时，以本文件为准**（本文件是后写的）。
> 简报其余部分照旧有效。

---

## 一、T-18：3 个不可提供指标的丢弃计数（**账本里的待补项，两份简报都没写**）

账本 T-18 记的是：

> | T-18 | 不可提供指标（3 条）的**丢弃计数**机制 | wave 3（每轮汇总日志） |

规格 `specs/runtime-and-config/spec.md` 的「每轮结构化汇总日志」Requirement
**明确列了这一项**：

> 包含 tick 号、耗时、拉取数、转换数、投递数、失败数、
> **丢弃的不可提供指标数**、窗口数、告警数。

**数据来源已经有了**（不需要新建机制）：`mapping/loader.py` 的 `LoadedMapping.customer_metrics`
（`Tuple[CustomerMetric, ...]`），每条带 `status` 字段。实测取值分布：

```
'可提供'    98 条
'无法提供'   3 条
```

这 3 条即 `data/customer_metric_unsupported.csv` 的那 3 个（`gaussdb.Fastpath_function_call`、
`gaussdb.Disabled_conn`、`gaussdb.sendAgentName`），在 `metric_rules.json` 里
`source` 为 `null`、`enabled=false`。

**落地口径**：每轮汇总日志里「丢弃的不可提供指标数」= 该计数（**恒为 3**，
除非映射表更新）。**不要**每轮重新数一遍再打印 3 —— 要么每轮取一次，要么启动时算一次存起来。

> ⚠️ 注意别把它和「空分组 / 取不到值而不产出」混为一谈：
> 前者是**声明上就不可提供**（3 条），后者是**运行时取不到值**（每轮可变）。
> 两者都要出现在汇总日志里，但**不能算成同一个数**。

## 二、差分基线作用域必须显式带上 `instance_id`（**要改一个 wave-2b 的已有文件**）

`task-3.3-brief.md` 的「多实例的键隔离」表里写了：

> | 差分基线作用域 | `ScopedCounterDeltaStore(self._deltas, node_id)` | ⚠️ 同一平台 node_id 跨实例唯一，
> **但作用域应显式带上 instance_id** 更稳（`f'{instance_id}:{node_id}'`） |

**这是本次要落地的**。当前实现（`tpops_kafka_collector/collectors/command_center.py`）：

```python
def _delta_store(self, node_id: Any) -> ScopedCounterDeltaStore:
    key = str(node_id)
    store = self._scoped_deltas.get(key)
    ...
    store = ScopedCounterDeltaStore(self._deltas, key)   # ← 作用域只有 node_id
```

而 `collectors/command_center_assembly.py::build_messages` 收一个
`delta_store: Callable[[Any], Any]`（按 node_id 取视图）。

**最小改动方案**（控制器指定，避免各改各的）：

1. `_delta_store(self, instance_id: str, node_id: Any)` —— 作用域改成
   `f'{instance_id}:{node_id}'`；缓存键也必须带上 instance_id
   （否则两个实例的同名 node_id 会共用一个视图）。
2. `collect()` 里把 `delta_store=self._delta_store` 改成
   `delta_store=lambda node_id: self._delta_store(instance_id, node_id)`
   （`build_messages` 的契约不变，**`command_center_assembly.py` 不用动**）。

**必须有的守卫测试**：两个实例共用同一个 `node_id` 时，
各自的差分基线**互不覆盖**（各拿到自己的速率），
且恢复后仍能各自算出速率。

> 这是 wave-2b 的 **C-3** 缺陷（差分基线跨 master 共享）的**多实例版本**。
> 形态一样：**不报错、但数据错**。所以守卫测试不能省。

## 三、`main.py` 必须可注入依赖（3.5 的杀进程重启演练要用）

任务 3.5 有一条合同完成标准：**「杀进程 → 重启演练：不重复、不丢失
（水位只在投递成功后推进）」**，并且简报要求「要有**可执行的**测试或演练脚本」。

Kafka 的 broker 地址**待客户确认**，所以**不可能跑真 Kafka 的端到端演练**。
因此 `main.py` 的装配必须**允许注入假的 TPOPS 客户端与假的 producer**，
让演练脚本能在**不连任何外部服务**的情况下：

1. 起一轮 → 采集到 N 条消息 → 投递**部分成功**（模拟中途失败）；
2. 断言水位**没有**推进（该 stream 不在水位文件里）；
3. 「杀掉进程」（丢弃内存态，只留磁盘上的水位文件）；
4. 重启 → 同一批消息**重新产出** → 幂等键挡掉已投过的 → **不重复**；
5. 未投出的**不丢失**（下一轮补上）。

**装配层要留一个工厂函数**，例如：

```python
def build_runtime(config, *, client=None, producer=None, ...) -> Runtime
```

`Runtime` 持有 watermark store / scheduler / profile / producer，
`main()` 只负责「装 → 起 → 收信号 → 优雅退 → exit(0)」。
这样测试能直接调 `build_runtime` 换掉外部依赖，**不需要改生产代码**。

## 四、每轮汇总日志的字段名（**开发方案 §4.9.1 已定死，别自创**）

`docs/开发方案-GaussDB指标与告警采集.md` §4.9.1 给了样例行：

```log
2026-11-06 10:20:30 INFO  tick=1234 elapsed=14.8s fetched=1043 mapped=1041
  produced=1041 failed=0 dropped_unsupported=3 windows=9 alarms=2
```

规格 `specs/runtime-and-config/spec.md`「每轮结构化汇总日志」要求的字段与之一致，
另外 `specs/delivery-and-state/spec.md`「单轮外部调用数与投递成功率可观测」点名
`calls` 与 `delivery_rate`。

**要求**：`log_format=json` 时这一行是**一个 JSON 对象**，字段名就用上表：

| 字段 | 含义 |
|---|---|
| `tick` | 轮次序号 |
| `elapsed` | 本轮墙钟耗时 |
| `fetched` | 从各通道取回的原始行数 |
| `mapped` | 转换后待投递的指标消息数 |
| `produced` | 成功投递数 |
| `failed` | 投递失败数 |
| `dropped_unsupported` | 不可提供指标计数（= 3，见第一节） |
| `windows` | 本轮执行的 SQL 条数 |
| `alarms` | 本轮告警条数 |
| `calls` | 外部调用次数（单轮 SHALL NOT 超过 42，超出记 WARNING） |
| `delivery_rate` | 投递成功率（成功投递数 ÷ 计划投递数） |

规格的 Scenario 还要求「**各计数之和自洽**（投递数 + 失败数 ≤ 转换数）」——
这条要有守卫测试。

另按 §4.9.2 记一条 INFO 日志：「本轮丢弃 N 个不可提供指标（累计 M）」。

## 四之二、健康端点的探针接线（3.2 已实现，**3.3 要接上**）

`health.py`（3.2 交付）提供 `ReadinessMonitor` + `DependencyProbe(name, check)`，
`check` 返回 `bool` / `str`（不就绪原因）/ `DependencyStatus` / `None`（不就绪）。
**探针要轻量**（读标志位），不能在里面发网络请求。

**3.3 要接的两个探针**（对应规格「`/readyz` SHALL 在 TPOPS 认证不可用时返回 503」
与设计方案 §4.9.3「最近 3 分钟内至少成功完成 1 轮 → 200，否则 503」）：

| 探针名 | 判据 |
|---|---|
| `tpops` | 最近一次 TPOPS 认证成功（用 `TpopsClient` 的认证状态/冷却状态） |
| `scheduler` | 最近一次 tick 成功完成且未超过 3 分钟 |

健康响应体的形状**由 3.2 定死了**（`dependencies[]` 列表），**不要改 `health.py`** ——
设计方案 §4.9.3 那份更丰富的响应体（`last_tick_at` / `produced_total` /
`dropped_unsupported_total` / `watermark`）**不是规格要求**，
按现在的 `dependencies[]` 形态接即可。

## 五、平台画像模块的落点与复用

- **新文件**：`tpops_kafka_collector/platform_profile.py`（控制器指定，避免各叫各的）。
  `PlatformProfile` 用 **frozen dataclass** 承载，`load_or_discover()` 是入口。
- **`master_count` 的复用方式**（简报说「复用 `_select_masters` 的现成去重逻辑」）——
  更好的复用是走**公开 API**：`MasterNodeRegistry.nodes(instance_id)` 返回的
  **已经是按 `node_id` 去重后的 master 节点**（`tpops/command_center_nodes.py:55`）。
  `master_count = len(registry.nodes(instance_id))`。
  `_select_masters` 是私有名，用它也行，但**公开 API 更稳**。
- **原子写**：复用 `watermark.py::_atomic_write_json`（同目录临时文件 → fsync → `os.replace`）。
  它是私有名 —— 控制器允许在包内复用（同一个包，不算破坏边界），
  但**不要复制一份**到 `platform_profile.py` 里（重复实现 = 将来分叉）。
- **画像文件不含凭据**，`discovered_at` 用 **UTC ISO 格式**。

## 六、日志中心化的四条硬要求（简报已列，这里补验收口径）

1. **幂等**：`basicConfig(force=…)` 是 3.8+ 的，**py37 要自己判 `root.handlers`**。
   重复调用 `setup_logging()` **不得叠加 handler**（守卫测试：调两次，
   `len(root.handlers)` 不变）。
2. **消费配置**：`log_level` / `log_format` 已经在 `config.py` 里定义了
   （`_runtime_fields`），现在**没有任何代码消费** —— 本次必须真的用上。
   守卫测试：配 `DEBUG` → INFO 日志真的输出；配 `json` → 真的逐行 `json.loads` 得通。
3. **输出到 stderr**。
4. **敏感信息不落日志**：凭据 / mini-token / 业务 SQL 文本**一律不进日志**。
   审一遍现有的日志调用（22 处 `LOGGER.error` 多数没有 `exc_info=True`，
   异常路径要带堆栈）。

## 七、退出码与启动期校验（简报已写，这里把验收口径定死）

| 场景 | 断言 |
|---|---|
| 配置缺失 | 退出码 **≠ 0**；日志里有**变量名**；**TPOPS 与 Kafka 的 send 一次都没被调用** |
| SIGTERM | 退出码 **0**；水位文件**完整**（能被 `WatermarkStore.load()` 读回来）；producer 已 flush |
| SIGTERM 且在途请求超 30 秒 | **放弃等待**，仍以退出码 **0** 退出，**不挂死**（用可控假时钟，**不要真等 30 秒**） |

「未向 TPOPS/Kafka 发任何请求」这条要用 mock 断言
（`client.send` / `producer.produce_*` 的调用次数 == 0）。

## 八、不要动的东西

- **不改** `changes/tpops-kafka-collector/execution-contract.md`、`tasks.md`、
  `specs/**`、`data/mapping/*.json`、`data/merged_sql/*`。
  （改了会让执行计划 stale → 前四个波次的 receipt 全部作废。）
- 除「第二节」明确要求的那一处（`collectors/command_center.py` 的差分作用域），
  **不改** `tpops_kafka_collector/` 下其它已有文件。
- **不要 `git commit`**（由控制器统一提交）。
