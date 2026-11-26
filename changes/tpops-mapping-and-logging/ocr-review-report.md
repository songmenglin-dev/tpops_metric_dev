# OpenCodeReview 委派审查报告 · 最终交付程序包

- **审查范围**：`tpops_kafka_collector/`（最终交付的生产程序包，79 个文件）+ 部署资产
- **工具**：`ocr delegate preview --from dc7fca9 --to c9770b4`（两个变更的完整范围）
  → `ocr delegate rule`（取回系统审查清单）→ 人工按清单逐项审查
- **审查方式**：**全量模式扫描**（68 个 Python 文件，按清单的 9 类缺陷逐项 grep）
  + **高风险文件精读**（client / scheduler_delivery / watermark / masking / producer / sqlgen）
- **清单要点**：只报有把握的真缺陷（false positive 成本高于漏报）；安全与正确性为阻塞项，
  风格建议不阻塞

## 结论

| 项 | 结果 |
|---|---|
| **Critical** | **0** |
| **Medium** | **1**（已修） |
| **Low** | 0（不报） |
| 审查覆盖 | 68 个生产 Python 文件 + 部署资产；映射数据 JSON 属数据不属代码，未逐条审 |

---

## Medium-1（已修）：认证状态无锁 → 并发线程会各发一次认证请求

- **位置**：`tpops_kafka_collector/tpops/client.py:180-207`（`token()`）、
  `client.py:138`（新增 `_auth_lock`）
- **问题**：`TpopsClient` 的认证状态（`_token` / `_token_expires_at` / `_auth_failed_at`）
  **没有任何锁**保护，而客户端实例是**被多线程共用**的：
  - `executors.py:126-134` 把同一个 client 包给 5 个通道的执行器；
  - `scheduler_tasks.py:109` 用 `ThreadPoolExecutor(max_workers=16)` 并发跑
    「通道 × 实例」的任务。
  token 过期那一刻，多个线程会同时进入 `token()`，各自发一次认证请求：
  - 认证**成功**时：只是浪费 N-1 次请求；
  - 认证**失败**时：平台按**请求数**累计 `login_failed_attempts`
    → **账号被锁得比预期快得多**。这直接违反项目红线「认证失败不要盲目重试（防锁号）」。
- **修法**：加 `threading.Lock` + **双检锁** —— 同一时刻只有一个线程在认证，
  其余线程直接拿它认证出来的 token。
- **证据（RED → GREEN 实测）**：
  ```
  回退修复：8 个并发线程 → 8 次认证请求 → 测试失败
  恢复修复：8 个并发线程 → 1 次认证请求 → 测试通过
  ```
  新增守卫：`tests/unit/test_client.py::test_concurrent_token_calls_authenticate_only_once`
  （假 Session 只预置一个并发阶段的认证响应，多出来的请求会直接断言失败）。

---

## 已核查且**未发现**问题的项（清单逐项）

| 清单项 | 核查方式 | 结论 |
|---|---|---|
| 凭据/token 进日志 | grep 全包 `password\|mini_token\|X-Mini-Token\|secret` × 日志调用 | **无**。`TpopsClient.__repr__`（`client.py:168-170`）不含密码；异常消息只带接口路径 |
| 裸 `except:` / 吞异常 | grep `except:` 与 `except …: pass` | 5 处，**全部合法**：删除过期日志失败（`logging_formats.py:157`）、loguru 移除不存在的 handler（`logging_setup.py:218,227`）、退出时刷 stdout（`main.py:338`）、`float()` 类型探测（`mapping/rules_sorting.py:103`） |
| 可变默认参数 / 类级共享可变状态 | grep `def …=[]/{}` 与模块级可变全局 | **无** |
| `assert` 当运行时校验 | grep 生产代码 `assert` | **无** |
| `eval`/`exec`/`compile`/`pickle`/`yaml.load` | grep | **无** |
| `subprocess(shell=True)` | grep | **无** |
| SQL 注入 | 读 `mapping/sqlgen.py` + `tpops/command_center.py` | SQL 全部来自仓库里的**静态规格文件**（`mapping/data/sql_merge.sql` + `merged_sql.json`）；动态部分只有 `page_size`/`offset`，**都是 int**（`sqlgen.py:123,126,177-178`）；运行时无外部输入进入 SQL |
| 浮点相等 / `== None` | grep | **无** |
| 资源泄漏（`open()` 不在 `with`） | grep | **无** |
| 字典取值不处理缺键 | 读 `mapping/table.py` / `loader.py` / `validation.py` | 均为 `.get()` + 显式校验；`loader.py` 结构不对时抛 `ProtocolError` |
| 异常丢 traceback | grep `raise` 模式 | 一律 `raise X from err`（如 `client.py:210`、`client.py:337`） |
| `is`/`==` 比较字面量 | grep | **无** |
| 线程安全 | 读 `scheduler_delivery.py`、`scheduler.py`、`watermark.py` | 采集在**线程池**里跑（只读平台、写内存 DTO），**投递与水位推进在调度线程里串行**（`scheduler_delivery.py:94-106` 逐组 for 循环）→ 水位/幂等集合**无并发写** |
| 资源泄漏（HTTP 会话） | 读 `client.py` | `requests.Session` 由客户端持有，生命周期与进程一致（常驻进程），无逐请求泄漏 |
| 投递失败推进水位（数据丢失） | 读 `scheduler_delivery.py:110-190` | **正确**：指标与告警都是「投递成功 → 才 `remember_key` + 推进水位」；失败整批不推进、下轮重投；组间隔离 |

---

## 已修问题之外的观察（不阻塞，未改）

- **`stderr` sink 与总日志同内容** → 开发态前台看不到通道 INFO 明细（明细在通道日志文件里）。
  这是 wave-2 审查已登记的 Minor，属已批准的设计选择。
- **`deploy/collector.sh` 1194 行**：项目「deploy/ 只能有一个 `.sh`」的守卫与
  「单文件 < 400 行只管采集端代码」共同决定单文件是唯一可行解。

## 复审方式

修复后建议重跑：
```bash
conda run -n tpops python -m pytest -q                      # 全量
conda run -n tpops python -m pytest tests/unit/test_client.py -q
```
