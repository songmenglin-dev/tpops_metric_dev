# 任务 1.6 报告：TPOPS HTTP 客户端

- 变更：`tpops-kafka-collector`，Wave 1（`wave-1-skeleton-platform`），任务 1.6（W06）
- 工作目录：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`（worktree）
- 状态：**完成**。未 `git commit`（按简报要求留给控制器统一提交）。

---

## 1. 做了什么

新建 TPOPS 平台 HTTP 客户端，落实开发方案 §4.2.1（认证）、§4.2（接口前缀）、
§5.4.1（客户端签名）、§5.7（重试口径），以及规格 `specs/platform-client/spec.md`
的「mini-token 认证 / 路径命名空间 / 自签证书 / 错误分类」四条 Requirement。

### 文件清单

| 文件 | 行数 | 说明 |
|---|---|---|
| `tpops_kafka_collector/tpops/client.py` | 398 | 新建 —— 客户端本体 |
| `tpops_kafka_collector/tpops/__init__.py` | 27 | 新建 —— 包导出 |
| `tests/unit/test_client.py` | 646 | 新建 —— 41 个单元测试 |

### 客户端的公开接口

```python
TpopsClient(base_url, username, password, verify_tls=False, timeout=60,
            session=None, clock=None, auth_failure_cooldown=60.0)
TpopsClient.from_config(config, session=None)     # 从 Config 装配
client.token(refresh=False) -> str                  # 取 mini-token
client.get(path, params=None) -> Dict[str, Any]
client.post(path, payload=None) -> Dict[str, Any]
require_fields(payload, fields, endpoint)           # 缺字段 → ProtocolError
```

常量：`RDS_V3` / `RDS_V3_2` / `GAUSSDB_V3` / `AUTH_PATH` / `TOKEN_HEADER`。

### 关键实现口径（逐条对��「实测踩过的坑」）

1. **token 在响应头 `X-Mini-Token`**，不在 body。测试里 body 故意放了一个
   `token` 字段，断言取到的仍是响应头那个。
2. **认证失败不发起第二次认证**。三重保障：
   - `_authenticate()` 内部只发一次请求，失败即抛 `AuthError`；
   - **认证静默期**（`AUTH_FAILURE_COOLDOWN_SECONDS = 60`）：认证失败后 60 秒内
     再要 token 直接复用上次的失败结论，**不发任何请求**；静默期过了才允许下一次
     认证（配合调度器的轮询退避 —— 开发方案 §4.2.1「只记录一次，退避到下一轮再试」）；
   - `AuthError` 不可重试（`errors.is_retryable` 判定），所以重试框架也不会拉它重来。
3. **两套路径命名空间不可混**。客户端只做 URL 拼接，不补前缀；`404` 归为
   **不可重试的 `ProtocolError`**，异常消息显式点出「`/rds/v3` 与 `/rds/gaussdb/v3`
   不可混」，不会把「漏了 `/gaussdb` 段」误判成「接口无数据」。
4. **`verify_tls=False` 默认**（平台自签证书），同时 `urllib3.disable_warnings`
   只在不校验时静音握手告警。可通过 `verify_tls=True` / `TPOPS_VERIFY_TLS` 关闭。
5. **错误四分类**（对齐 `errors.py`）：
   - `AuthError` —— 401 / 403、认证接口非 2xx、认证请求的传输层失败；
   - `TransientError` —— 5xx、超时、连接错误；
   - `ProtocolError` —— 404、其它 4xx、响应体不是 JSON、响应体不是对象、缺约定字段；
   - `ProduceError` —— Kafka 投递失败，由 kafka 层抛，不在本模块。
6. **可重试判定统一走 `errors.is_retryable`**，本模块**没有**另写按类名精确相等的
   判定。传输层异常经 `is_retryable(exc)` 判定（基类链），所以
   `requests.exceptions.ReadTimeout`（`Timeout` 的子类）被判为可重试 →
   `TransientError`。
7. **凭据不外泄**：密码只从构造参数 / `Config` 进来，**不进日志、不进异常消息、
   不进 `repr`**（三条独立断言）。token 同样不进日志。

### 401 强制刷新重试

`_request()` 里业务请求收到 401 → `token(refresh=True)` 强制刷新 → **重试一次**；
再次 401 → `AuthError`，不再重试。403 **不触发**刷新（授权失败 ≠ token 过期），
直接 `AuthError`。

### token 缓存与到期刷新

- 有效期 = 响应 body 的 `session_timeout`（分钟）× 60 − 60 秒余量；body 缺这个字段
  或不是 JSON 时回落到默认 1440 分钟。
- 客户端不自己定时刷新，**按需**取：缓存内直接用，过期才认证。
- 401 时走强制刷新路径（覆盖「服务端提前失效」的情形）。

---

## 2. TDD 记录

### RED —— 先写测试，看到它失败

第一轮 RED（实现文件还不存在）：

```
$ conda run -n tpops python -m pytest tests/unit/test_client.py -q
ERROR collecting tests/unit/test_client.py
E   ModuleNotFoundError: No module named 'tpops_kafka_collector.tpops'
1 error in 0.30s
```

这是「收集期导入失败」级的 RED —— 断言还没跑到。为了留下一条**真正跑到断言
并失败**的记录，实现第一版后我先故意把「认证失败不发起第二次认证」这条改成
错误实现（认证失败后允许再认证一次），跑出：

```
FAILED tests/unit/test_client.py::test_authentication_failure_does_not_issue_second_authentication_attempt
    assert 2 == 1
     +  where 2 = len([call for call in session.calls if call.url.endswith(AUTH_PATH)])
```

随后改为正确实现（静默期 + 只认证一次）转 GREEN。

第二轮 RED（补覆盖分支时，同样先写断言再改实现）：
新增的 11 个测试（认证传输层失败、静默期、静默期过期、POST、路径归一化、
非对象响应体、不可重试传输异常、其它 4xx、`session_timeout` 缺省回落到 1440 分钟、
body 非 JSON 回落到 1440 分钟）同样先红后绿。

### GREEN —— 最终输出

```
$ conda run -n tpops python -m pytest tests/unit/test_client.py -q
41 passed in 0.54s
```

覆盖率（只看本任务产出）：

```
$ conda run -n tpops python -m pytest tests/unit/test_client.py \
      --cov=tpops_kafka_collector.tpops --cov-report=term-missing -q
Name                                      Stmts   Miss  Cover   Missing
-----------------------------------------------------------------------
tpops_kafka_collector/tpops/__init__.py       2      0   100%
tpops_kafka_collector/tpops/client.py       159      1    99%   101
```

未覆盖的第 101 行是 `require_fields` 里 `raise ProtocolError` 分支的
`return payload` 续行（不可达代码路径的尾巴），覆盖率 99% ≥ 80% 门槛。

### 全量回归

```
$ conda run -n tpops python -m pytest -q
711 passed in 11.03s
```

（基线 476 + wave-1 其它并发任务的新增 + 本任务 41。）

```
$ conda run -n tpops python -m pytest tests/test_py37_compat.py -q
77 passed in 0.63s
```

`tests/test_py37_compat.py` 的 `SCAN_DIRS` 已包含 `tpops_kafka_collector`，
所以本任务新增的两个文件过了 3.7 语法门禁。

### Lint

```
$ conda run -n tpops ruff check tpops_kafka_collector/tpops/ --output-format=concise
All checks passed!
```

**注意**：仓库现状 `ruff check tpops_kafka_collector/` 仍有 49 条历史 lint 错误
（`mapping/rules*.py`、`kafka/producer.py`），全部属于**其它并发任务**的文件
（wave-1 任务 1.7 / 1.8 / 1.5），不是本任务产出。已核对：本任务两个文件的
lint 命中数为 **0**。

---

## 3. 偏离简报之处及原因

| # | 偏离 | 原因 |
|---|---|---|
| 1 | 简报只说「401 强制刷新重试一次」。实现里**额外加了认证静默期**（默认 60 秒，构造参数 `auth_failure_cooldown` 可调）。 | 合同要求的是「认证失败 → 只发起一次认证请求」。若没有静默期，调用方在下一轮轮询里再调 `token()` 就会**再发一次认证请求**（一次失败登录 = 一次 `login_failed_attempts` 累计），等于慢性锁号。开发方案 §4.2.1 明确「只记录一次，退避到下一轮再试」——静默期就是这个「退避」的落点。60 秒与默认 `POLL_INTERVAL_SECONDS=30` 相比留了余量，且可配置。 |
| 2 | **认证请求的传输层失败**（超时 / 连接错误）也归 `AuthError`（不可重试），而不是 `TransientError`。 | 认证请求发出后**可能已经被平台收到并计过一次失败登录**，只是响应没回来。再来一次就是锁号。所以「认证这一步的任何失败」都不可重试。业务请求的传输层失败仍然是 `TransientError`（可重试），两者的差别在代码与测试里都显式区分了。 |
| 3 | 加了 `require_fields(payload, fields, endpoint)` 模块级函数。 | 规格「响应结构异常 → `ProtocolError`，**并显式记录出错的接口与缺失字段**」需要「缺失字段名」进异常消息。客户端本身不知道每个接口期望哪些字段，所以把「校验」做成可复用的具名函数，由各采集器在解析后调用（字段名由映射表驱动）。 |
| 4 | 加了 `TpopsClient.from_config(config)` 类方法。 | 开发方案 §5.4.1 的签名是显式参数，而 `Config` 已经存在（wave 0 / 1.1 产出）。给一个类方法做桥接，避免装配代码在 main 里重复，且**密码仍只走 Config（环境变量）**，不新增任何读取凭据的路径。 |
| 5 | 加了 `clock` 构造参数（可注入的单调时钟）。 | token 到期刷新与认证静默期都需要可控时间；不注入时钟就只能靠 `sleep`，测试会变慢且不稳定。生产默认 `time.monotonic`。 |

---

## 4. 遗留问题 / 需要控制器知道

1. **⚠️ 认证静默期 60 秒是本任务自定的默认值**，未在 `config.py` 里加环境变量
   （`config.py` 不属于本任务的独占文件，且简报未授权改它）。
   若要外露成 `TPOPS_AUTH_FAILURE_COOLDOWN_SECONDS`，需要控制器或后续任务
   在 `config.py` 加一行 + `Config` 加字段 + `from_config` 传参。**当前可通过构造
   参数 `auth_failure_cooldown` 覆盖**，功能不受影响。

2. **本任务未改 `config.py`、`errors.py`**（均非独占文件），只 `import` 使用。
   `errors.py` 的 `is_retryable` 满足「`ReadTimeout` 判为可重试」的要求，
   无需修改。

3. **`tests/unit/test_client.py` 646 行**，超过 400 行的软上限。
   约束「单文件 < 400 行」在项目 CLAUDE.md 里是针对**采集端代码**
   （`tpops_kafka_collector/`），测试文件不在其列；
   仓库既有 `tests/test_merged_sql_mapping.py`（515 行）、
   `tests/test_kafka_mapping.py`（534 行）也超过该数。生产代码两个文件
   均 < 400 行（398 / 27）。若控制器要求测试文件也拆分，可按
   「认证 / 业务请求 / 配置与凭据」切成三个模块。

4. **后续任务的接口依赖**（wave 2 的采集器会用到）：
   - `client.get(path)` / `client.post(path, payload)` 的 `path` **必须自带**
     命名空间前缀（`RDS_V3` / `GAUSSDB_V3` 等常量），客户端不会替你补 ——
     这是刻意的（漏前缀要显式 404 报错，而不是被悄悄补上）。
   - 命令中心三跳、诊断优化、告警分页等采集器尚未实现（wave 2）；
     它们应复用本客户端的 `get` / `post`，并在解析后调用 `require_fields`。
   - 401 刷新逻辑已经在客户端内闭环，采集器**不需要**自己处理 401。

5. **未 `git commit`** —— 按简报要求，git index 锁由控制器统一管理。

6. **未改任何合同 / 任务 / 映射数据文件**（`execution-contract.md`、`tasks.md`、
   `data/mapping/*.json`、`data/merged_sql/*` 全部未动）。

---

## 5. 证明命令汇总（控制器可直接复跑）

```bash
cd /mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev

# 本任务证明（tasks.md 指定的）
conda run -n tpops python -m pytest tests/unit/test_client.py -q
# → 41 passed

# 全量
conda run -n tpops python -m pytest -q
# → 711 passed

# 3.7 语法门禁
conda run -n tpops python -m pytest tests/test_py37_compat.py -q
# → 77 passed

# 本任务文件的 lint
conda run -n tpops ruff check tpops_kafka_collector/tpops/ --output-format=concise
# → All checks passed!

# 覆盖率
conda run -n tpops python -m pytest tests/unit/test_client.py \
  --cov=tpops_kafka_collector.tpops --cov-report=term-missing -q
# → client.py 99%
```
