# 任务 3.3 报告 —— 入口装配与信号处理

> 状态：**DONE_WITH_CONCERNS**
> 唯一 concern 是**一处简报内部自相矛盾**（平台画像「固化后不重新发现」与
> 「启动时顺手核对」两条要求打架），我按「拆成两个入口」的方式化解，
> 细节见 §6 D1。其余全部按简报 + 控制器补充说明实现。
> 未 `git commit`（按提交纪律留给控制器）。

---

## 0. 交付清单

### 新增生产代码（5 个文件）

| 文件 | 行数 | 职责 |
|---|---|---|
| `tpops_kafka_collector/logging_setup.py` | 177 | **日志中心化初始化**：消费 `config.log_level` / `config.log_format`，text / json 两种格式，输出 stderr，幂等，第三方库降噪 |
| `tpops_kafka_collector/platform_profile.py` | 336 | **平台画像固化**：发现（选**所有**实例）→ 原子写 → 之后以文件为准；启动时顺手核对（ERROR / WARNING） |
| `tpops_kafka_collector/executors.py` | 281 | 五个通道的**执行器**（`(InstanceTarget) -> TaskOutcome`）+ **停机检查**兜底 |
| `tpops_kafka_collector/lifecycle.py` | 189 | 主循环 + **优雅退出**（带截止时间，**不用** `scheduler.run()`）+ `TrackedScheduler` |
| `tpops_kafka_collector/main.py` | 311 | **入口装配**（`build_runtime`）+ `Runtime` + 健康端点接线 + `main()` / `run()` |

> `main.py` 初稿写到 420 行，越过合同「单文件 < 400 行」→ 拆出 `lifecycle.py`
> （主循环与优雅退出）与 `executors.py`（执行器）。现最大文件 336 行，
> **最长函数 49 行**（`build_runtime`，脚本扫描见 §8）。

### 修改既有生产代码（**只有简报第 ④ 项要求的那一处**）

| 文件 | 改动 |
|---|---|
| `tpops_kafka_collector/collectors/command_center.py` | `_delta_store(node_id)` → `_delta_store(instance_id, node_id)`，作用域改成 `f'{instance_id}:{node_id}'`，视图缓存键也带上 instance_id；`collect()` 里改成 `delta_store=lambda node_id: self._delta_store(instance_id, node_id)`。`command_center_assembly.py` **未动**。 |

### 新增测试（4 个文件，42 个用例）

| 文件 | 用例数 |
|---|---|
| `tests/unit/test_logging_setup.py` | 9 |
| `tests/unit/test_platform_profile.py` | 13 |
| `tests/integration/test_main.py` | 15 |
| `tests/integration/test_collector_command_center_multi_instance.py` | 4 |

### 修改既有测试（1 处，**必要的白名单登记**）

`tests/unit/test_serializer.py` 的 `_ALLOWED_JSON_WRITERS` 增加
`'logging_setup.py': '日志行的 JSON 编码（不是消息，不进 Kafka）'`。
理由：合同要求「全仓只有 `kafka/serializer.py` 一处 `json.dumps` 产出**消息**」，
该守卫测试本身就把「状态文件落盘 / 健康响应体」逐个登记为白名单；
日志行的 JSON 编码是第三类用途，**不登记就等于把守卫变成瞎子**。
消息序列化出口**没有增加**（仍是 `kafka/serializer.py` 一处）。

### 新增演练桩（1 个文件）

`tests/fixtures/graceful_shutdown_stubs/sitecustomize.py` —— 手工演练用的桩，
通过 `PYTHONPATH` 里的 `sitecustomize` 注入（**传输层**注入，见 §5）。

---

## 1. RED → GREEN 证据

### RED（实现尚未落盘时跑测试）

```
$ conda run -n tpops python -m pytest \
    tests/unit/test_logging_setup.py tests/unit/test_platform_profile.py \
    tests/integration/test_main.py tests/integration/test_collector_command_center_multi_instance.py -q

==================================== ERRORS ====================================
________________ ERROR collecting tests/unit/test_logging_setup.py _________________
ImportError while importing test module '.../tests/unit/test_logging_setup.py'.
Traceback:
tests/unit/test_logging_setup.py:12: in <module>
    from tpops_kafka_collector.logging_setup import setup_logging
E   ModuleNotFoundError: No module named 'tpops_kafka_collector.logging_setup'
_______________ ERROR collecting tests/unit/test_platform_profile.py _______________
ImportError while importing test module '.../tests/unit/test_platform_profile.py'.
Traceback:
tests/unit/test_platform_profile.py:17: in <module>
    from tpops_kafka_collector.platform_profile import (
E   ModuleNotFoundError: No module named 'tpops_kafka_collector.platform_profile'
_______________ ERROR collecting tests/integration/test_main.py _________________
ImportError while importing test module '.../tests/integration/test_main.py'.
Traceback:
tests/integration/test_main.py:25: in <module>
    from tpops_kafka_collector import main as entry
E   ImportError: cannot import name 'main' from 'tpops_kafka_collector'
=========================== short test summary info ============================
ERROR tests/unit/test_logging_setup.py
ERROR tests/unit/test_platform_profile.py
ERROR tests/integration/test_main.py
!!!!!!!!!!!!!!!!!!!! Interrupted: 3 errors during collection !!!!!!!!!!!!!!!!!!!!
3 errors in 0.54s
```

第 4 个文件（差分基线多实例作用域）当时**没有收集错误**（它 import 的是既有模块），
但**断言全红**：

```
$ conda run -n tpops python -m pytest tests/integration/test_collector_command_center_multi_instance.py -q
...
tests/integration/test_collector_command_center_multi_instance.py:43: in _collector
    deltas=deltas,
tpops_kafka_collector/collectors/command_center.py:121: in __init__
    self._views = merged_views(mapping)
E           tpops_kafka_collector.errors.MappingError: 映射表里没有 merged_views 段 —— 合并形态无法解码
=========================== short test summary info ============================
FAILED tests/integration/test_collector_command_center_multi_instance.py::test_delta_store_scope_contains_instance_id
FAILED tests/integration/test_collector_command_center_multi_instance.py::test_two_instances_sharing_node_id_keep_separate_baselines
FAILED tests/integration/test_collector_command_center_multi_instance.py::test_rates_survive_restore_from_watermark
FAILED tests/integration/test_collector_command_center_multi_instance.py::test_collect_passes_instance_scoped_delta_store
4 failed in 0.79s
```

（这一轮的失败原因是测试自己传了 `mapping=None`；改成传真实映射表后，
失败点就落在**真正要修的地方** —— 作用域不含 instance_id。）

### GREEN（实现落盘后）

```
$ conda run -n tpops python -m pytest tests/unit/test_logging_setup.py \
    tests/unit/test_platform_profile.py \
    tests/integration/test_collector_command_center_multi_instance.py -q
...........................                                              [100%]
27 passed in 0.91s

$ conda run -n tpops python -m pytest tests/integration/test_main.py -q
...............                                                          [100%]
15 passed in 2.31s
```

### 全量

```
$ conda run -n tpops python -m pytest -q
........................................................................ [ 97%]
.....................................                                    [100%]
1549 passed in 28.39s
```

**基线核对**（控制器给的「1479」是 wave-3 之前测的；3.1 / 3.2 落地后基线已变）：

```
$ conda run -n tpops python -m pytest -q \
    --ignore=tests/unit/test_logging_setup.py \
    --ignore=tests/unit/test_platform_profile.py \
    --ignore=tests/integration/test_main.py \
    --ignore=tests/integration/test_collector_command_center_multi_instance.py
1507 passed in 26.45s
```

→ **基线 1507，本次新增 42 个用例，合计 1549**，与「只会多不少」一致。

### 其他门禁

```
$ conda run -n tpops python -m ruff check tpops_kafka_collector/
All checks passed!

$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
144 passed in 1.34s
```

---

## 2. 三条硬性场景的守卫测试名

| 场景 | 守卫测试名 | 断言要点 |
|---|---|---|
| **启动期配置缺失** | `tests/integration/test_main.py::test_missing_config_exits_non_zero_and_names_the_variable` | 退出码 ≠ 0；日志里有 `KAFKA_BOOTSTRAP_SERVERS` 这个**变量名** |
| **启动期配置缺失**（不发请求） | `tests/integration/test_main.py::test_missing_config_sends_no_request_to_tpops_or_kafka` | monkeypatch `main.TpopsClient` / `main.MessageProducer` 为「一调用就 AssertionError」的假实现；断言**构造次数 == 0**、`client.send` / `producer.produce_*` 调用次数 == 0 |
| **SIGTERM 正常退出** | `tests/integration/test_main.py::test_sigterm_during_main_loop_exits_zero_with_complete_watermark` | 退出码 **0**；producer 已 close（flush）；`WatermarkStore.load()` 能把水位文件**完整读回来**（`inst-1:monitor` 水位仍是 12345） |
| **SIGTERM 正常退出**（真进程） | `tests/integration/test_main.py::test_real_process_receives_sigterm_and_exits_zero` | 真起 `python -m tpops_kafka_collector.main` → `SIGTERM` → **退出码 0** + 水位文件能被读回来 |
| **SIGTERM 超时 30 秒放弃等待** | `tests/integration/test_main.py::test_in_flight_request_beyond_deadline_gives_up_and_exits_zero` | 用**可控阻塞桩**（`Event.wait`）＋**可注入的截止时间 0.3 秒**（不真等 30 秒）；断言线程**没挂死**、退出码 **0**、producer 已 close、水位文件完整 |
| 补充：默认上限是 30 秒 | `tests/integration/test_main.py::test_default_shutdown_deadline_is_thirty_seconds` | `entry.SHUTDOWN_DEADLINE_SECONDS == 30` |
| 补充：停机后不再发起新任务 | `tests/integration/test_main.py::test_shutdown_stops_issuing_new_ticks` | 停机后 `scheduler.ticks == 1` |

---

## 3. 平台画像的守卫测试名清单

文件：`tests/unit/test_platform_profile.py`（13 个）

| 简报要求的守卫 | 测试名 |
|---|---|
| 实例列表只有一个 → 画像里记它 | `test_single_instance_is_recorded` |
| 有多个 → 全部都记（**条数与顺序都对**） | `test_all_instances_are_recorded_in_platform_order` |
| 列表为空 → 抛明确异常 | `test_empty_instance_list_raises_clear_error` |
| 固化文件已存在 → **不重新发现**（平台列表接口没被调用） | `test_existing_profile_file_is_not_rediscovered` |
| 画像里的 instance 不在平台列表 → 记 **ERROR** | `test_profile_instance_missing_on_platform_logs_error` |
| 平台有画像没记录的实例 → 记 **WARNING** | `test_platform_instance_missing_from_profile_logs_warning` |
| 原子写中途失败不留半截 JSON | `test_atomic_write_failure_leaves_no_partial_json` + `test_failed_atomic_write_keeps_previous_content` |
| `master_count` 按**唯一 node_id** 计数（不数条目） | `test_master_count_counts_unique_node_ids` |
| 画像文件不含凭据 | `test_profile_file_contains_no_credentials` |
| `discovered_at` 是 UTC ISO | `test_discovered_at_is_utc_iso` |
| 新增实例**不自动加**进画像 | `test_reconcile_does_not_add_new_instance_to_profile` |
| `engine_name` 变 → WARNING | `test_engine_name_change_logs_warning` |
| `master_count` 变 → WARNING | `test_master_count_change_logs_warning` |

---

## 4. 日志中心的元测试名

文件：`tests/unit/test_logging_setup.py`（9 个）

| 测试名 | 守什么 |
|---|---|
| `test_debug_level_really_outputs_info_records` | 配 DEBUG → INFO 日志**真的输出**（不是被 lastResort 吞掉） |
| `test_info_level_is_silenced_when_level_is_warning` | 配 WARNING → INFO 不输出（级别**真的被消费**） |
| `test_text_format_is_time_level_module_message` | text 格式是 `时间 \| 级别 \| 模块 \| 消息`（拆 4 段断言） |
| `test_json_format_emits_one_json_object_per_line` | 配 JSON → **逐行 `json.loads` 得通**（2 行 → 2 个对象） |
| `test_json_format_flattens_key_value_summary_into_fields` | 每轮汇总日志的 `tick/elapsed/fetched/…` 在 JSON 下成为**顶层字段** |
| `test_setup_logging_is_idempotent` | 调两次 `len(root.handlers)` **不变** |
| `test_setup_logging_keeps_single_owned_handler` | 本模块装上去的 handler **永远只有一个** |
| `test_noisy_third_party_loggers_are_quieted` | `kafka` / `urllib3` 压到 WARNING |
| `test_output_goes_to_stderr_by_default` | 默认输出到 **stderr** |

---

## 5. 手工演练的实际结果

### 5.1 演练桩的注入方式（**传输层**，不是改 `main.py` 的属性）

`python -m pkg.mod` 会把模块**再执行一遍**（`__main__` 与 `pkg.mod` 是两个模块
对象），所以直接 monkeypatch `tpops_kafka_collector.main.MessageProducer`
**对 `-m` 入口无效**。因此桩注入在**传输层**：

- `requests.Session.request` → 返回假响应（TPOPS 的 HTTP 全走它）；
- `tpops_kafka_collector.kafka.producer.KafkaProducer` → 假生产者（不连 broker）。

桩文件：`tests/fixtures/graceful_shutdown_stubs/sitecustomize.py`
（通过 `PYTHONPATH` 里的 `sitecustomize` 自动生效）。

### 5.2 起进程 → `kill -TERM` → 退出码 0 + 水位文件完整

执行的命令（`/tmp/drill.sh`）：

```bash
REPO=/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev
rm -rf /tmp/drillstate && mkdir -p /tmp/drillstate && cd /tmp/drillstate
export PYTHONPATH="$REPO/tests/fixtures/graceful_shutdown_stubs:$REPO"
export TPOPS_USER=sml TPOPS_PASSWORD='***' TPOPS_BASE_URL=https://127.0.0.1:18002
export KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 STATE_DIR=/tmp/drillstate
export HEALTH_HOST=127.0.0.1 HEALTH_PORT=18345 POLL_INTERVAL_SECONDS=5 LOG_LEVEL=INFO
/home/sml/.conda/envs/tpops/bin/python -m tpops_kafka_collector.main \
    >/tmp/drillstate/out.log 2>/tmp/drillstate/err.log &
PID=$!
sleep 6
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:18345/healthz
curl -s -w '\nHTTP %{http_code}\n' http://127.0.0.1:18345/readyz
kill -TERM $PID
wait $PID; echo "退出码 = $?"
ls -l /tmp/drillstate/ ; cat /tmp/drillstate/watermark.json
```

**实际输出**：

```
=== python PID=344292 状态 ===
 344292 Sl
=== /healthz ===
200
=== /readyz ===
{"check": "readiness", "dependencies": [{"name": "tpops", "ready": true, "reason": ""}, {"name": "scheduler", "ready": true, "reason": ""}], "status": "ok", "timestamp": 1795421358}
HTTP 200
=== kill -TERM 344292 ===
退出码 = 0
=== 状态目录 ===
total 24
-rw-r--r-- 1 sml sml 12854 Nov 23 16:09 err.log
-rw-r--r-- 1 sml sml     0 Nov 23 16:09 out.log
-rw------- 1 sml sml   297 Nov 23 16:09 platform_profile.json
-rw------- 1 sml sml   680 Nov 23 16:09 watermark.json
=== 水位文件内容 ===
{
  "alarm_watermarks": [],
  "delta_baselines": [],
  "idempotency_keys": [
    "drill-instance-1|慢查询总数|component_id=drill-cn-1,node_id=drill-node-1|1795421353576",
    "drill-instance-1|Select慢查询数|component_id=drill-cn-1,node_id=drill-node-1|1795421353576",
    "drill-instance-1|DML慢查询数|component_id=drill-cn-1,node_id=drill-node-1|1795421353576",
    "drill-instance-1|版本|node_id=drill-node-1|1795421353576"
  ],
  "metric_watermarks": [
    {
      "end_time_ms": 1795421353577,
      "stream": "drill-instance-1:diagnostic"
    },
    {
      "end_time_ms": 1795421353576,
      "stream": "drill-instance-1:instance"
    }
  ],
  "version": 1
}
=== stderr 最后 6 行 ===
2026-11-23 16:09:13,594 | INFO | tpops_kafka_collector.scheduler_report | tick=1 elapsed=0.0s fetched=4 mapped=4 produced=4 failed=0 dropped_unsupported=3 windows=0 alarms=0 calls=6 delivery_rate=1.000 groups=alarm,command_center,diagnostic,instance,monitor skipped=-
2026-11-23 16:09:13,594 | INFO | tpops_kafka_collector.scheduler_report | 本轮丢弃 3 个不可提供指标（累计 3）
2026-11-23 16:09:18,595 | INFO | tpops_kafka_collector.scheduler_report | tick=2 elapsed=0.0s fetched=0 mapped=0 produced=0 failed=0 dropped_unsupported=3 windows=0 alarms=0 calls=0 delivery_rate=1.000 groups=- skipped=alarm,command_center,diagnostic,instance,monitor
2026-11-23 16:09:18,595 | INFO | tpops_kafka_collector.scheduler_report | 本轮丢弃 3 个不可提供指标（累计 6）
2026-11-23 16:09:18,982 | INFO | tpops_kafka_collector.health | 健康端点已停止
2026-11-23 16:09:18,983 | INFO | tpops_kafka_collector.lifecycle | 采集端已优雅退出（退出码 0）
```

**结论**：退出码 **0**；水位文件**完整**（`WatermarkStore.load()` 能读回，
`metric_watermarks` / `idempotency_keys` 都在）；画像文件已固化；
`/healthz` 与 `/readyz` 都是 200（跑完一轮之后）；日志最后一行是
「采集端已优雅退出（退出码 0）」。

> 演练**没有连任何外部服务** —— TPOPS 走传输层桩，Kafka 走假生产者。
> 命令行里的 `TPOPS_PASSWORD` 在报告里已打码（实际用的是占位值）。

### 5.3 启动期配置缺失（真进程）

```bash
cd /tmp/drillstate
PYTHONPATH="$REPO/tests/fixtures/graceful_shutdown_stubs:$REPO" \
TPOPS_USER=sml TPOPS_PASSWORD='***' TPOPS_BASE_URL=https://127.0.0.1:18002 \
STATE_DIR=/tmp/drillstate HEALTH_PORT=18236 \
/home/sml/.conda/envs/tpops/bin/python -m tpops_kafka_collector.main
```

**实际输出**（注意：**没有** `KAFKA_BOOTSTRAP_SERVERS`）：

```
退出码 = 2
2026-11-23 16:07:10,831 | ERROR | tpops_kafka_collector.config | 配置校验失败：缺少必填环境变量：KAFKA_BOOTSTRAP_SERVERS
2026-11-23 16:07:10,831 | ERROR | __main__ | 配置校验失败，进程退出：缺少必填环境变量：KAFKA_BOOTSTRAP_SERVERS
```

退出码 **2**（非 0），日志**指明变量名**，且**没建任何客户端 / 生产者**
（`build_runtime` 根本没走到）。

### 5.4 JSON 日志格式（真进程）

```bash
LOG_FORMAT=json python -m tpops_kafka_collector.main > json.log 2>&1
```

**实际结果**：

```
JSON 行数 = 49 非 JSON 行数 = 0
{"timestamp": "2026-11-23T16:06:57+0800", "level": "INFO",
 "logger": "tpops_kafka_collector.scheduler_report",
 "message": "tick=1 elapsed=0.0s fetched=4 mapped=4 produced=4 failed=0 dropped_unsupported=3 windows=0 alarms=0 calls=6 delivery_rate=1.000 groups=alarm,command_center,diagnostic,instance,monitor skipped=-",
 "tick": "1", "elapsed": "0.0s", "fetched": "4", "mapped": "4", "produced": "4",
 "failed": "0", "dropped_unsupported": "3", "windows": "0", "alarms": "0",
 "calls": "6", "delivery_rate": "1.000",
 "groups": "alarm,command_center,diagnostic,instance,monitor", "skipped": "-"}
```

**逐行 `json.loads` 全部得通（49/49）**；每轮汇总日志的字段名（`tick` /
`elapsed` / `fetched` / `mapped` / `produced` / `failed` /
`dropped_unsupported` / `windows` / `alarms` / `calls` / `delivery_rate`）
是**顶层字段**，与开发方案 §4.9.1 一字不差。

### 5.5 在途请求超时（进程级）

进程级的「在途请求卡住」需要真的挂住一个 HTTP 调用；这一条用**进程内**的
阻塞桩覆盖（`tests/integration/test_main.py::test_in_flight_request_beyond_deadline_gives_up_and_exits_zero`）：
截止时间注入 **0.3 秒**（生产默认 30 秒），断言**线程没挂死**、退出码 **0**、
producer 已 close、水位文件完整。**没有真等 30 秒。**

---

## 6. 取舍与偏离（逐条标出）

### D1 ⚠️ 简报自相矛盾：**「固化后不重新发现」 vs 「启动时顺手核对」**

简报同时要求：

- 启动流程段：「固化文件存在 → 读它（以文件为准），**但顺手核对**：
  画像里的每个 instance_id 在平台实例列表里还在吗？…」
- 守卫测试段：「固化文件已存在 → **不重新发现**（断言平台列表接口**没被调用**）」

这两条**字面上互斥**：核对必须查平台列表，「平台列表接口没被调用」又要求不查。

**我的化解（拆成两个入口）**：

| 入口 | 行为 | 守卫测试打在哪 |
|---|---|---|
| `PlatformProfile.load_or_discover(path, client, …)` | 固化文件存在 → **只读文件**，**不调用平台**（`reconcile` 默认 `False`） | `test_existing_profile_file_is_not_rediscovered`（断言 `client.calls == []`） |
| `PlatformProfile.reconcile(profile, client, …)` | 查平台列表 → 记 ERROR / WARNING，**不改画像文件** | `test_profile_instance_missing_on_platform_logs_error` 等 5 个 |

生产装配（`main._load_profile`）调的是
`load_or_discover(..., reconcile=True)` → 启动时**确实会核对**（符合简报前半段），
但**不会重新发现、不会改写画像文件**（符合简报后半段的实质）。
「平台列表接口没被调用」这条守卫因此**在 `load_or_discover` 单元层面成立**。

> 如果控制器的独立复核认为「启动路径上一次都不能查平台列表」，
> 把 `main._load_profile` 里的 `reconcile=True` 改成 `False` 即可（**一行**），
> 代价是启动时不再核对（画像与平台不一致就发现不了）。

### D2 ⚠️ 差分基线作用域的改动**动了一个既有文件**

简报第 ④ 项明确要求改 `collectors/command_center.py`，我照做了。
`command_center_assembly.py` **未动**（`build_messages` 的契约不变，
只把 `collect()` 里传的 `delta_store` 换成带 instance_id 的 lambda）。

### D3 「JSON 格式下汇总日志的字段名」靠**格式化器摊平**实现，没动 `scheduler_report.py`

控制器要求「`log_format=json` 时这一行是一个 JSON 对象，字段名就用上表」，
但字段名的**产出方**是 3.1 的 `scheduler_report.py`（**不许改**）。
解法：`logging_setup.JsonLogFormatter` 把「整条消息每个 token 都是 `k=v`」
的消息**摊平成顶层字段**（普通自然语言消息原样不动，不会误伤）。
结果见 §5.4 —— 字段名逐字一致，且 `scheduler_report.py` **一行没动**。

### D4 「命令中心取数失败」靠 `pages == 0` 判定

`CommandCenterCollector.collect()` 把取数失败**吞掉**并返回空结果
（`messages=()`），外部**无法区分**「失败」与「平台没数据」。
可用的判据是 `CommandCenterCollectResult.pages`：**执行过至少一页才会
`pages >= 1`**，失败路径固定返回 `pages=0`。
执行器据此给 `end_time_ms=None`（不推进水位）。这个判据依赖采集器的内部
契约（`_fetch_pages` 的返回值），已在 `executors.py` 里写明注释；
若 3.5b 要改采集器的返回结构，**这里要同步改**。

### D5 C / D 通道「一条都没产出 → 不推进水位」

按控制器「取数失败必须给 `None`」的口径，C / D 两条通道在**一条消息都没产出**
时给 `end_time_ms=None`（宁可重复取，也不丢数据）。
代价：这两条通道在长期取不到数时，`scheduler_delivery` 会每轮记一条
WARNING「该组没有报告结束时刻」——**这是 3.1 的代码**，我没有改（见 §7）。

### D6 探测 `tpops` 认证状态读的是客户端的**私有标志位**

`TpopsClient` 没有公开的 `auth_ok` 属性，而 `tpops/client.py` **不在允许修改的
文件清单里**。探针因此读 `getattr(client, '_auth_failed_at')` /
`getattr(client, '_token')`（并**优先**用公开属性 `auth_ok`，若将来补上就自动切换）。
探针**不发任何请求**（守卫测试 `test_health_probes_are_wired_and_lightweight`
断言跑完探针后 `client.call_count` 不变）。

### D7 「停机后不再发起新的 HTTP 请求」落在**执行器**这一层

按控制器补充说明 §①：调度器只保证「不再发起新任务」，**不保证**「不再发起
新的 HTTP 请求」（本轮已排进线程池但还没开始跑的任务仍会启动）。
每个执行器入口套一层 `_guard`：停机事件已置位 → 直接返回**空的**
`TaskOutcome`（`messages`/`alarms` 都空、`end_time_ms=None` → **不推进水位**），
**不发起任何请求**。守卫测试 `test_no_new_http_request_after_shutdown`
断言 TPOPS 客户端 `get` / `post` **一次都没被调用**。

### D8 主循环**不用** `scheduler.run()` / `run_forever()`

按控制器补充说明 §②：这两个方法内部 `future.result()` 没有超时，且线程池的
上下文管理器会 `shutdown(wait=True)` → 一个在途 HTTP 调用挂住就**永远不返回**。
`lifecycle.py` 自己跑主循环：每轮放在**守护线程**里跑 + `join(截止时间)`，
超时记 WARNING「放弃等待本途请求」→ **不 join、不再等** → 直接往下走
（写水位 → flush → 退出 0）。守护线程保证进程能退出。

### D9 「放弃等待」时**不替它推进水位**

未投出的消息水位**不会**推进（`MessageDelivery` 只在投递成功后推进），
放弃等待后直接落盘当前内存态 → 下次启动会重投 → 幂等键兜住重复。

### D10 演练桩放在 `tests/fixtures/` 而不是 `tools/`

`tools/` 是「现有离线取数脚本 + 参考实现」，演练桩属于测试资产，
放 `tests/fixtures/graceful_shutdown_stubs/` 更合适（`tests/` 也在
`test_py37_compat.py` 的扫描范围内，桩本身是 3.7 兼容的）。

### D11 改了一个既有**测试**文件（白名单登记）

见 §0「修改既有测试」—— `tests/unit/test_serializer.py` 的
`_ALLOWED_JSON_WRITERS` 增加 `logging_setup.py`。**消息序列化出口没有增加。**

---

## 7. 观察到的现象（**不是**本次修的，按控制器指示只提一句）

1. **`scheduler_delivery.py::_deliver_metrics` 的 WARNING 会每轮刷屏**：
   「本轮该组没有产出任何指标消息（空结果）」。
   告警通道的 `TaskOutcome.messages` 本来就是空的（告警走 `outcome.alarms`），
   所以**每 30 秒会多出一条**这条无意义 WARNING。
   另外 C / D 通道在取不到数时（见 D5）也会触发
   「该组没有报告结束时刻 —— 本轮不推进指标水位」这条 WARNING。
   **按控制器指示，我没有改 `scheduler*.py`**（3.1 的产出）。

2. `scheduler_report.py` 的汇总日志里 `dropped_unsupported` 每轮都记 **3**
   （实测的不可提供指标条数），与控制器补充说明 §一的口径一致。

3. 演练时可以看到告警通道的 `windows=0`（告警不跑 SQL），符合预期。

---

## 8. 自查

| 项 | 结果 |
|---|---|
| 全量 `pytest -q` | **1549 passed**（基线 1507 + 新增 42），无跳过、无失败 |
| `ruff check tpops_kafka_collector/` | **All checks passed!** |
| `pytest tests/test_py37_compat.py -q` | **144 passed** |
| 新增生产文件行数 | `main.py` 311 / `platform_profile.py` 336 / `executors.py` 281 / `lifecycle.py` 189 / `logging_setup.py` 177 —— **全部 < 400** |
| 最长函数 | **49 行**（`build_runtime`）—— < 50 |
| 修改的既有生产文件 | **只有** `collectors/command_center.py`（简报第 ④ 项要求的那一处），diff 共 +10 / -4 行 |
| 改动 `changes/` 下已有文件 | **无**（只新增了本报告） |
| 改动 `data/mapping/*.json`、`data/merged_sql/*` | **无** |
| `git commit` | **未执行**（按提交纪律留给控制器） |

### 关键契约自检

- **幂等键**：用 `scheduler_types.metric_key(instance_id, message)`（**不是**
  `ids.metric_idempotency_key`）→ 投递层 `scheduler_delivery.py` 本来就是这么用的，
  3.3 没有另起一套；告警用 `AlarmMessage.idempotency_key` ✓
- **`deltas` 同一个对象**：`build_runtime` 里 `deltas = CounterDeltaStore()`，
  同时交给 `build_executors(deltas=deltas)` 与 `TrackedScheduler(delta_store=deltas)`
  —— 守卫测试 `test_build_runtime_shares_one_delta_store_between_collector_and_scheduler` ✓
- **周期不自算**：用 `schedule.compute_channel_plans(mapping)` ✓
- **水位键**：`scheduler_types.metric_stream(instance_id, channel)` /
  `alarm_stream(instance_id)`（告警执行器读水位时用的就是 `alarm_stream`）✓
- **`dropped_unsupported`**：`schedule.count_unsupported_metrics(mapping)`
  （演练日志实测 **3**）✓
- **消息只有一个序列化出口**：仍是 `kafka/serializer.py` 一处 ✓
  （`logging_setup.py` 的 `json.dumps` 只编码**日志行**，已登记白名单）
- **凭据只走环境变量**：画像文件不含凭据（守卫测试）✓；日志与异常消息里
  只出现变量名，不出现取值 ✓
- **不静默吞异常**：执行器与装配层的异常一律 `LOGGER.exception` 带堆栈 ✓
