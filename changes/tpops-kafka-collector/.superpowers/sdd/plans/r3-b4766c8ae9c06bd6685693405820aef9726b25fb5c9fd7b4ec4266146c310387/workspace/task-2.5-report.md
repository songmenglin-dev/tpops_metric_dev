# 任务 2.5 —— 命令中心三跳客户端（wave-2b）

## 新增文件清单

| 文件 | 行数 | 内容 |
|---|---|---|
| `tpops_kafka_collector/tpops/command_center.py` | 399 | 三跳客户端 `CommandCenterClient`（编排 + 重试 + 并发） |
| `tpops_kafka_collector/tpops/command_center_nodes.py` | 110 | `MasterNodeRegistry`：只留 master + 按 `node_id` 去重 + 按实例缓存 1 小时 |
| `tpops_kafka_collector/tpops/command_center_result.py` | 105 | `NodeTask` / `SqlOutcome` / `CommandTaskError` / `parse_result_rows`（`result` 二次解析） |
| `tests/unit/test_command_center.py` | 676 | 32 条用例 |

**拆分原因（偏离简报的「单文件」）**：初版把 DTO + 解析 + 客户端写在一个文件里共 497 行，
**超过「单文件 < 400 行」红线**。按「多个小文件 > 少数大文件」拆成同包的 3 个模块，
`command_center.py` 原样再导出 `NodeTask` / `SqlOutcome` / `CommandTaskError` /
`parse_result_rows`，**调用方只需 `from tpops_kafka_collector.tpops.command_center import ...`**，
接口对 2.6 采集器没有变化。

未改动任何既有文件（`changes/` 下、`data/` 下、`tpops_kafka_collector/` 既有文件均未动）。

## 三跳各自的请求与响应结构

### 第 1 跳 —— 下发命令

```
POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}
```
`cmd_id` 固定 `73722acc-bff6-4d04-adc0-bc1e1fa1eb7d`（平台内置命令
`general_single_sql_query`，模板 `select * from ({sql_text}) limit {limit}`）。

请求体（与留档 `data/metric_response/057_gaussdb.Prepared.json` 一致）：

```json
{
  "instance_id": "83cd68f2e4cb40b4b7e69240479ffa26in14",
  "node_ids": ["<master node_id>", ...],
  "params": [
    {"param_name": "sql_text", "param_value": "<SQL 文本>"},
    {"param_name": "limit",    "param_value": "1000"}
  ]
}
```

响应：**只含作业与任务，不含数据**

```json
{"job_id": "57328584-…",
 "node_tasks": [{"task_id": "50ab3303-…", "node_id": "3881c044…no14"}, ...]}
```

`node_ids` 只放**去重后的全部 master**（实测一次下发 3 个 master 成功）。

### 第 2 跳 —— 轮询任务状态

```
GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records
    ?job_id=…&offset=0&limit=100
```

响应：

```json
{"total_count": 3,
 "statistics": {"total": 3, "succeed": 3, "failed": 0, "running": 0, "other": 0},
 "job_status": "running",
 "tasks": [{"task_id": "…", "node_id": "…", "node_name": "…", "status": "succeed",
            "params": [{"param_name": "sql_text", "param_value": "******"}],
            "error_msg": null, "start_time": …, "finish_time": …}]}
```

### 第 3 跳 —— 取任务结果

```
GET /rds/gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{job_id}
    /task-record/{task_id}/detail
```

响应：

```json
{"node_id": "3881c044…no14", "node_name": "gauss-6374_gaussdbv5_hcs2-0_0",
 "status": "succeed", "is_redirect": false,
 "result": "[{\"prepared_xacts\":\"0\"}]",
 "out_format": "json", "out_mode": "array", "out_type": "map",
 "download_file_id": null}
```

**`result` 是 JSON 字符串**，需二次 `json.loads`。
只对**下发时拿到的 `node_tasks`** 取结果（不按轮询响应的 `tasks` 全量取）。

## `statistics.running == 0` 的判据怎么实现的

实现位置：`command_center.py` 的 `CommandCenterClient._poll()`。

```python
statistics = require_fields(body, ('statistics', 'tasks'), path).get('statistics')
running = statistics.get('running')
if running is None:
    raise ProtocolError('命令中心 task-records 的 statistics 里没有 running')
if running == 0:
    return body          # ← 唯一的结束判据
```

- **`job_status` 全程不参与判定**（代码里没有任何一处读它）。
- `statistics` 缺字段 → `ProtocolError`（不可重试，要人工介入）。
- `running` 缺字段 → `ProtocolError`。
- 轮询超时（`poll_timeout`，默认 60s）→ `TransientError`（可重试，重发整条 SQL）。

**对应测试**（`tests/unit/test_command_center.py`）：

- `test_polling_ends_when_statistics_running_is_zero_even_if_job_status_is_running`
  —— 假平台固定返回 `job_status="running"` + `statistics.running=0`，
  断言 **`task-records` 只被 GET 了 1 次**（没有因为 `job_status` 而继续轮询），
  且 `outcome.ok is True`、拿到 3 个节点的任务结果。
- `test_polling_continues_while_statistics_running_is_not_zero`
  —— 依次返回 `running=2 / 1 / 0`，断言轮询了 **3 次**。
- `test_polling_timeout_is_retryable` —— 永远 `running=1`，假时钟推进到超时，
  断言 `outcome.ok is False` 且错误信息含「超时」。
- `test_missing_statistics_is_a_protocol_error` —— 响应无 `statistics`，
  断言 `isinstance(outcome.exception, ProtocolError)` 且 `not is_retryable(...)`。

## `result` 二次解析在哪

**位置：`tpops_kafka_collector/tpops/command_center_result.py` 的 `parse_result_rows()`。**

```python
if isinstance(result, (bytes, str)):
    text = result.decode('utf-8') if isinstance(result, bytes) else result
    if not text.strip():
        return ()                       # 空串 → 空行列表（不是失败）
    payload = json.loads(text)          # ← 二次解析
else:
    payload = result                    # 已经是列表 → 直接用（防御平台改格式）
```

- 不是合法 JSON → `DecodeError`（继承 `MappingError` → **不可重试**）。
- 不是行列表 → `DecodeError`。
- 行里有非对象元素 → `DecodeError`。

调用点在 `CommandCenterClient._collect_one()`，**每次取明细后立刻解析**，
解析结果放进 `NodeTask.rows`（`Tuple[Mapping[str, Any], ...]`，不可变元组）。

**顺序坑（实现时发现并修掉）**：先判「平台自述失败串」再解析 ——
失败串 `Failed to execute command on all nodes` 不是合法 JSON，
顺序反了会把「命令失败」误报成「result 不是合法 JSON」。
这条由 `test_result_reporting_failure_is_treated_as_a_failed_task` 守住
（实现初版时该用例**先失败**，修正后通过）。

**测试**：
- `test_result_string_is_parsed_a_second_time_into_rows` —— 明细的 `result` 是
  `json.dumps([{'v': 42}])` 字符串，断言解析出 `[({'v': 42},)] * 3`。
- `test_parse_result_rows_handles_empty_and_already_parsed` —— 空串 / `None` /
  已是列表 三种输入。
- `test_parse_result_rows_rejects_invalid_json` —— 坏 JSON → `DecodeError`。
- `test_replay_archived_merged_sql_result_shape` —— **留档回放**：
  读 `data/merged_sql/sql_merge_probe_result.json`，断言
  `statistics.running == 0`，且每个节点的 `result` 解析后**恰好 1 行**、
  含 **11 个视图键**、每个视图值是**字符串**（视图级二次解析在映射层
  `mapping.decode.decode_merged` 完成，本层只做到「行」这一层）。

## master 节点去重与缓存怎么做的

**位置：`tpops_kafka_collector/tpops/command_center_nodes.py` 的 `MasterNodeRegistry`。**

1. **取清单**：`GET /rds/gaussdb/v3/instances/{instance_id}/key-view-execute-node`
   → `{"nodes": [...]}`。
2. **只留 master**：`node.get('role') != 'master'` 的跳过
   （命令级约束 `limit_node_types=["master"]`）。
3. **按 `node_id` 去重**：`seen` 集合，**保留首次出现**（顺序稳定）——
   平台清单里同一个 master 会以 CN / DN 各出现一次
   （实测 12 条条目 / 6 条 master / **3 个唯一 node_id**）。
4. **按实例缓存 1 小时**：`self._cache[instance_id] = (clock(), nodes)`；
   取用时 `clock() - fetched_at >= ttl` 即视为过期（TTL 默认 `3600.0` 秒）。
   缓存 **按实例分键** → 不同实例互不干扰。
5. **清单为空** → `ProtocolError`（明确报错，不静默下发到 0 个节点）。

**测试**：
- `test_master_node_ids_are_deduplicated_by_node_id` —— 9 条节点的假清单
  → 3 个唯一 `node_id`。
- `test_non_master_nodes_are_never_dispatched_to` —— 断言下发体
  `node_ids == ["node-a","node-b","node-c"]`（slave 一次都没出现）。
- `test_node_list_is_cached_for_one_hour` —— 假时钟推进 3599s 仍命中缓存
  （平台只被拉 1 次），推进到 3601s 才重新拉（第 2 次）。
- `test_node_list_cache_is_per_instance` —— 两个实例各拉 1 次。
- `test_empty_node_list_is_a_protocol_error` —— 空清单 → `ProtocolError`。
- `test_replay_archived_node_list_dedupes_to_three_masters` —— **留档回放**：
  读 `data/metric_response/057_gaussdb.Prepared.json` 的真实节点清单，
  断言 12 条 / 6 条 master / **3 个唯一 node_id**。

## 并发与重试怎么做的

### 并发

- `concurrency` 参数，默认 **2**（`config.cmd_concurrency`，由
  `CommandCenterClient.from_config()` 装配）。
- 批处理用 `ThreadPoolExecutor(max_workers=self._concurrency)`；
  **执行器工厂可注入**（`executor_factory`），测试可换假执行器。
- `execute_batch()` 返回顺序与入参顺序一致。
- 批处理前**先取一次节点清单**（顺带填缓存），避免并发线程重复拉平台。

**测试**：
- `test_default_concurrency_is_two` —— 默认值 2。
- `test_concurrency_is_passed_to_the_executor_factory` —— **假执行器**：
  `FakeExecutor` 记录 `max_workers`，断言传进来的是 **3**（不真 sleep、不靠计时）。
- `test_batch_runs_concurrently_and_respects_the_concurrency_limit` ——
  `threading.Barrier(2)` 让两个提交**必须**同时到达，否则超时失败；
  证明并发是真并发。
- `test_concurrency_of_one_serialises_the_batch` —— 并发 1 时顺序执行。

### 重试

- **重试框架复用 `tpops_kafka_collector/retry.py` 的 `retry_call()`**，
  判定**只认 `errors.is_retryable`**（本模块**没有**另写一份按类名精确相等的判定）。
- **逐条重试**：`execute_sql()` 把「一次三跳」包在 `retry_call` 里 ——
  每条 SQL 独立计数、独立退避，**一条失败不消耗同批其他 SQL 的重试预算**。
- 退避策略：`RetryPolicy`（默认 `max_attempts=5`，退避 1s→2s→4s→8s + 抖动）；
  `from_config()` 用 `config.cmd_retry_attempts`（默认 3）与
  `config.retry_base_seconds` 装配。`sleep` 可注入，**测试不真等**。
- 可重试的失败类型：
  - `TransientError`（5xx / 超时 / 连接错误）—— 客户端已分类；
  - `CommandTaskError` —— **本模块新增**，继承 `TransientError`，
    用于「节点任务失败」（实测 50 条 SQL 有 3 条偶发失败，重试即成功）。
- 不可重试：`AuthError`（认证失败，绝不重试）、`ProtocolError`（结构不符）、
  `DecodeError`（`result` 坏 JSON）。

**测试**：
- `test_failed_node_task_triggers_a_retry_of_that_sql_only` —— 第 1 次任务
  `failed`、第 2 次 `succeed`，断言最终 `ok is True` 且 `attempts == 2`。
- `test_exhausted_retry_is_recorded_as_a_failure_not_raised` —— 重试用尽 →
  `ok is False`、`attempts == 2`、`error` 非空（**不抛异常**）。
- `test_auth_error_is_not_retried` —— `AuthError` → `attempts == 1`。
- `test_transient_error_is_retried` —— `TransientError` → `attempts == 2`。
- `test_command_task_error_is_retryable` —— `is_retryable(CommandTaskError(...))` 为真。

## 单条失败隔离的失败记录长什么样

**失败不抛异常**，而是返回一个**不可变的** `SqlOutcome`（`command_center_result.py`）：

```python
@dataclass(frozen=True)
class SqlOutcome:
    sql_text: str                                    # 哪条 SQL 挂了
    ok: bool                                         # False = 失败
    tasks: Tuple[NodeTask, ...] = ()                 # 成功时各节点的任务结果
    error: Optional[str] = None                      # 失败原因（人类可读）
    attempts: int = 0                                # 实际尝试次数（含首次）
    job_id: Optional[str] = None                     # 平台作业 ID
    exception: Optional[BaseException] = None        # 原始异常（is_retryable 判它）
```

失败实例（`test_batch_failure_carries_the_failing_sql_and_the_reason`）：

```python
SqlOutcome(
    sql_text='select 0',
    ok=False,
    tasks=(),
    error='命令中心任务失败（节点 bad-job）：node-a: boom',
    attempts=1,
    job_id=None,
    exception=CommandTaskError('命令中心任务失败（节点 bad-job）：node-a: boom'),
)
```

调用方（2.6 的 B 类采集器）据此就能知道**哪条没成功、为什么、试了几次**：
按 `ok` 过滤出失败项、`outcome.sql_text` 定位 SQL、`outcome.error` 上报原因、
`outcome.exception` 判是否可重试（`errors.is_retryable`）。

**隔离语义**（`test_one_failing_sql_does_not_affect_the_others_in_the_same_batch`）：
一批 3 条 SQL，中间那条失败 → `[ok, ok, ok]` 实际得到 `[True, False, True]`，
另外两条**照常返回数据**，批次整体不抛异常。

`NodeTask` 同样是不可变的，逐节点记录：

```python
@dataclass(frozen=True)
class NodeTask:
    node_id: str
    task_id: str
    status: str                                       # 'succeed' / 'failed' / …
    rows: Tuple[Mapping[str, Any], ...] = ()          # 二次解析后的行
    error: Optional[str] = None                       # 失败原因
```

## RED 的失败断言

**第一次 RED**（实现前）：收集阶段就失败 ——
`ModuleNotFoundError: No module named 'tpops_kafka_collector.tpops.command_center'`
（32 条用例全部 import 不到被测模块）。

**第二次 RED**（实现初版后新增「自述失败串」用例）：
```
FAILED tests/unit/test_command_center.py::test_result_reporting_failure_is_treated_as_a_failed_task
E   AssertionError: assert 'Failed to execute command' in
        '命令中心 result 不是合法 JSON：Expecting value: line 1 column 1 (char 0)'
E   assert outcome.ok is False
E   AssertionError: assert True is False
```
—— 这条 RED 抓出**真 bug**：自述失败串不是合法 JSON，
先解析会把「命令失败」误报成「result 不是合法 JSON」，且失败被当成成功。
修正：先判失败串再解析 + 失败判据加上「带 error 就算失败」。

## GREEN 的测试输出摘要

```
$ conda run -n tpops python -m pytest tests/unit/test_command_center.py -q
................................                                         [100%]
32 passed in 0.53s
```

覆盖率（三个新模块）：

```
Name                                                   Stmts   Miss  Cover   Missing
--------------------------------------------------------
tpops_kafka_collector/tpops/command_center.py            140      5    96%   224, 279, 301, 389, 399
tpops_kafka_collector/tpops/command_center_nodes.py       54      3    94%   96, 108-110
tpops_kafka_collector/tpops/command_center_result.py      40      2    95%   95, 101
--------------------------------------------------------
TOTAL                                                   234     10    96%
```

**全量**：

```
$ conda run -n tpops python -m pytest -q --ignore=tests/integration/test_collector_alarm.py
889 passed in 13.34s
```

`ruff check tpops_kafka_collector/` → **All checks passed!**
（E501 按东亚字宽计，长中文注释行已手工折行。）
`tests/test_py37_compat.py` → 89 passed（3.7 语法/标准库门禁）。

> 注：`tests/integration/test_collector_alarm.py` 是**并行任务**正在写的
> 采集器用例，它 import 的 `tpops_kafka_collector.collectors` 包尚未落地，
> 导致全量收集失败。**与本任务无关**（我的文件不 import `collectors`）。
> 排除该文件后全量 889 passed。

## 偏离简报之处及原因

1. **拆成 3 个文件**（而非简报只说「新建 `command_center.py`」）。
   原因：初版单文件 497 行，**违反「单文件 < 400 行」红线**。
   拆法：`command_center.py`（客户端编排）+ `command_center_nodes.py`（节点清单）
   + `command_center_result.py`（结果类型与 `result` 解析）。
   `command_center.py` **原样再导出** `NodeTask` / `SqlOutcome` /
   `CommandTaskError` / `parse_result_rows`，**对 2.6 的调用接口不变**。

2. **HTTP 调用用 `client.get(path)` / `client.post(path, payload)`**，
   第 2 跳用关键字 `params=`。客户端的 `get` 签名是
   `get(path, params=None)`，第 3 跳的 `job_id` / `task_id` 已在**路径里**，
   不再冗余塞进 query（与留档一致：`query_params: {}`）。

3. **轮询退避用 1s 起、×1.5、上限 3s**（简报只说「单次往返 3~8 秒」，
   开发方案 §4.2.5 写的是「首次延迟 1 秒，之后按 0.5s / 1s / 2s / 3s 递增」）。
   实现取**单调递增**版本（1 → 1.5 → 2.25 → 3 → 3…），避免开发方案里
   「1s 之后 0.5s」这种先降后升的抖动。

4. **批次内一条 SQL 失败时该条**重试**，`retry_call` 的 sleep 注入假实现**，
   满足「并发要能用假执行器测，别真 sleep」。

5. **未实现「命令中心输出上限 102400 B」的检查** —— 那是 2.6 采集器的职责
   （规格里是采集器记 ERROR 并缩小分页容量重试），本层只负责三跳与取数。

## 遗留问题

1. **`execute_sql` 失败时返回 `SqlOutcome` 而不是抛异常** ——
   这是「单条失败隔离」的刻意选择，但意味着调用方**必须检查 `outcome.ok`**，
   忘了检查就会静默丢数据。2.6 采集器接线时要保证每条 `outcome` 都被处理
   （成功 → 进规则引擎；失败 → 记 ERROR 并计数上报）。
2. **`job_status` 完全没读** —— 如果平台哪天把 `statistics.running` 也去掉了，
   会抛 `ProtocolError`（明确报错），不会误判成「立即结束」。
3. **节点清单缓存是进程内的**（不落盘）—— 进程重启后第一轮会重新拉一次
   节点清单，无害。
4. **`_fetch_detail` 按节点串行取**（同一批 SQL 的多个 master 逐个取明细）。
   实测单次往返 3~8 秒、3 个 master → 约 10~25 秒；若实测发现超采集周期，
   需把第 3 跳也并发化（本层已留 `executor_factory` 注入点）。
5. **`limit` 参数按字符串下发**（`'1000'`）—— 与留档一致；
   平台 `limit` 范围是 1~1000，2.6 采集器要保证分页容量不越界。
6. **`result` 里的视图级二次解析在映射层**（`mapping.decode.decode_merged`），
   本层只解析到「行」这一层 —— 2.6 接线时要确认走的是 `decode_merged`
   而不是 `decode`，否则 82 个 B 类指标会**静默全变 0**（见 `mapping/decode.py` 头注）。
7. **未跑过真实平台** —— 仓库无可用平台，全部用例是 mock + 留档回放。
   首次上平台时建议先单条 SQL 验证 `statistics.running` 与 `result` 的实际形态。
