# 能力规格：平台客户端、认证与重试

对应开发方案 §2.2（平台调研）、§4.2（API 详解）、§4.7（可靠性）、§5.4.1、§5.4.2、§5.7。
覆盖任务 W03、W06、W12。

## ADDED Requirements

### Requirement: mini-token 认证

平台客户端 SHALL 通过 `POST /rds/v3/auth/mini-token` 认证，
并从**响应头** `X-Mini-Token` 取 token（token 不在 body 里）。
SHALL 缓存 token 并在到期前刷新，SHALL 支持 401 时强制刷新并重试一次。

#### Scenario: 首次认证

- **WHEN** 客户端尚未持有 token
- **THEN** 向 `/rds/v3/auth/mini-token` 发起一次认证请求，从响应头读出 `X-Mini-Token` 并缓存

#### Scenario: token 过期

- **WHEN** 业务请求收到 401
- **THEN** 客户端强制刷新 token 并重试该请求一次；再次 401 则抛出 `AuthError`，不再重试

#### Scenario: 认证失败不重试

- **WHEN** 认证接口返回非 2xx
- **THEN** 抛出 `AuthError`，SHALL NOT 再次发起认证请求（平台登录失败会累计 `login_failed_attempts` 并锁号）

### Requirement: 平台路径命名空间

采集端 SHALL 使用两套路径前缀，且 SHALL NOT 混用：
`/rds/v3/…`（认证、实例列表、监控指标、告警）与
`/rds/gaussdb/v3/instances/{id}/…`（诊断优化、命令中心）。

#### Scenario: 少一段前缀

- **WHEN** 对诊断优化或命令中心接口漏掉 `/gaussdb` 段
- **THEN** 平台返回 404；采集端 SHALL 将其识别为不可重试的协议错误并显式报错，而不是当作「接口无数据」

### Requirement: 自签证书

客户端 SHALL 默认不对 TPOPS 平台做 TLS 证书校验（平台使用自签证书），
且该行为可通过 `TPOPS_VERIFY_TLS` 配置项关闭。

#### Scenario: 默认连接自签证书平台

- **WHEN** `TPOPS_VERIFY_TLS` 未设置或为 `false`
- **THEN** 请求成功建立，不因证书校验失败而中断

### Requirement: 错误分类

客户端 SHALL 把平台错误分为四类且不混淆：
`AuthError`（401/403，不可重试）、`TransientError`（5xx / 超时 / 连接错误，可重试）、
`ProtocolError`（响应结构不符，不可重试）、`ProduceError`（Kafka 投递失败，可重试）。

#### Scenario: 5xx

- **WHEN** 平台返回 500
- **THEN** 抛出 `TransientError`，被重试逻辑判定为可重试

#### Scenario: 响应结构异常

- **WHEN** 平台返回 200 但 body 缺少约定键
- **THEN** 抛出 `ProtocolError`，被判定为不可重试，并显式记录出错的接口与缺失字段

### Requirement: 指数退避重试

采集端 SHALL 对可重试错误执行指数退避 + 抖动的重试，
退避基数 `RETRY_BASE_SECONDS`（默认 1 秒）、上限 `RETRY_MAX_ATTEMPTS`（默认 5 次）。
SHALL NOT 对不可重试错误重试。

#### Scenario: 可重试错误

- **WHEN** 某次请求抛出 `TransientError`
- **THEN** 系统按 1s → 2s → 4s → 8s 的退避序列重试，最多 5 次

#### Scenario: 不可重试错误

- **WHEN** 某次请求抛出 `AuthError` / `ProtocolError` / `MappingError`
- **THEN** 系统立即向上抛出，不发起第二次请求

### Requirement: 命令中心三跳编排

命令中心客户端 SHALL 按「下发 → 轮询 → 取结果」三跳执行命令，
SHALL 以 `statistics.running == 0` 作为结束判据（SHALL NOT 使用 `job_status`），
SHALL 对 `result` 字段做二次 `json.loads`。

#### Scenario: job_status 仍为 running 但任务已结束

- **WHEN** 轮询响应中 `job_status == "running"` 而 `statistics.running == 0`
- **THEN** 判定该任务已完成，进入取结果步骤

#### Scenario: result 是 JSON 字符串

- **WHEN** 取结果接口返回的 `result` 字段是字符串
- **THEN** 客户端对其做 `json.loads`，得到行列表；解析失败则抛 `ProtocolError`

#### Scenario: 任务状态为 succeed 但内容是失败文案

- **WHEN** 任务状态为 `succeed` 但 `result` 含平台失败文案
- **THEN** 判定该命令实际失败，标记为失败并进入重试

#### Scenario: sql_text 被掩码

- **WHEN** 任务记录中的 `sql_text` 为 `******`
- **THEN** 不影响结果解析（SQL 原文由采集端自己持有）

### Requirement: 命令中心仅在 master 节点执行，且节点清单按 node_id 去重

命令中心客户端 SHALL 先取节点清单（`key-view-execute-node`），
SHALL 只在 `role == "master"` 的节点上下发命令，
SHALL **按 `node_id` 去重**后才下发。
节点清单 SHALL 按实例缓存（TTL 1 小时）。

> 实测：节点清单对同一实例返回 12 条条目、只有 3 个唯一 `node_id`
> （每个 node_id 同时以 CN 与 DN 各出现一次），其中 6 条 `role == "master"`。
> 不去重就会向同一节点重复下发。

#### Scenario: 存在多个 master

- **WHEN** 目标实例有 3 个 master CN（节点清单里对应 6 条 master 条目）
- **THEN** 命令只向 3 个唯一 node_id 下发，返回结果按节点区分，不合并去重

#### Scenario: 取不到 master

- **WHEN** 节点清单中没有任何 `role == "master"` 的节点
- **THEN** 抛出明确错误并跳过该批命令，不向 slave 节点下发

### Requirement: 命令中心并发上限与单条失败隔离

命令中心并发下发数 SHALL 由 `CMD_CONCURRENCY` 控制（范围 1~10），
**默认值 SHALL 为 2**（不是 9）：实测并发 ≥4 会触发平台侧参数竞争，
产生假的「invalid SQL statement with syntax error」，且高并发总墙钟反而更长。
单条命令的失败 SHALL NOT 影响同批其他命令；
SHALL 逐条判任务状态，SHALL NOT 假设失败落在哪一批。

#### Scenario: 批中一条失败

- **WHEN** 一批命令中有 1 条失败
- **THEN** 其余命令的结果仍被正常返回，失败的那条单独带错误信息

#### Scenario: 高并发下的随机失败

- **WHEN** 并发数被配到 8 且其中若干条随机失败
- **THEN** 失败的每一条都被独立重试并最终返回（实测重试全部成功），
  失败位置不假设为前 N 条

### Requirement: 慢 SQL 文本默认脱敏

采集端 SHALL 在把 SQL 文本写入消息前默认做脱敏（字面量替换为 `?`）
并截断到 `SQL_TEXT_MAX_LENGTH`（默认 512）字符；
该行为 SHALL 可通过 `SQL_MASKING_ENABLED` 关闭。

#### Scenario: 默认脱敏

- **WHEN** 消息中包含 `sql_text`
- **THEN** 其中的字符串字面量已被替换为 `?`，且长度不超过配置上限

#### Scenario: 脱敏开关关闭

- **WHEN** `SQL_MASKING_ENABLED=false`
- **THEN** `sql_text` 原样输出（该模式仅在客户书面确认可外发后启用）
