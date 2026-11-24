# collection-channels

## Purpose

The collection-channels capability documents the published behavior for users and maintainers.

## Requirements

### Requirement: 周期分档调度

采集端 SHALL 按客户指标库的「采集更新周期 + 采集更新周期单位」分档调度，
档位集合为 {30 秒, 60 秒, 1 分, 2 分, 5 分, 1 时, 24 时}。
档位的排序 SHALL 以**时长**为准（秒数升序）；「60 秒」与「1 分」时长相同，
视为**同一档**，SHALL NOT 视为两档。
命令中心 SQL 的调度频率 SHALL 由它所覆盖指标中的**最高需求档**决定，
SHALL NOT 为每个指标单独调度。

#### Scenario: 档位到期序列

- **WHEN** 给定 7 档指标分布（按秒数升序），调度器运行若干 tick
- **THEN** 每条合并 SQL 的执行时刻与它覆盖指标的最高档一致

#### Scenario: 一条 SQL 服务多档

- **WHEN** 某条 SQL 同时覆盖 1 分钟档与 5 分钟档指标
- **THEN** 该 SQL 只在 1 分钟档执行一次，其结果同时供两档指标使用

#### Scenario: 60 秒与 1 分同档

- **WHEN** 某条 SQL 覆盖了 60 秒档指标与 1 分钟档指标
- **THEN** 两者被视为同一档，该 SQL 的刷新周期为 60 秒

### Requirement: 监控指标接口的窗口与时间戳

采集端 SHALL 以**不少于 1 小时**的窗口查询监控指标接口，
SHALL 把 `end_time` 回退 `METRIC_END_LAG_MINUTES`（默认 5 分钟），
SHALL 使用字符串 `"false"` 作为 `is_latest_point_only` 的取值。

#### Scenario: 窗口不足

- **WHEN** 配置的窗口小于 1 小时
- **THEN** 启动失败并指明窗口下限；SHALL NOT 发起请求

#### Scenario: end_time 回退

- **WHEN** 计算查询区间
- **THEN** `end_time` = 当前时刻 − 5 分钟，`start_time` = `end_time` − 窗口长度

#### Scenario: is_latest_point_only

- **WHEN** 构造监控指标查询请求体
- **THEN** 该字段为字符串 `"false"`（不是布尔 `False`，也不是字符串 `"true"`）

#### Scenario: 时间戳切片

- **WHEN** 某档位指标只取本档周期内的数据点
- **THEN** 切片出的点其时间戳落在该档位区间内，且 null 点被跳过

### Requirement: 监控指标分组码

采集端 SHALL 使用固定的 12 个分组码查询监控指标：
`CPUMEMORY` `CONNECTION` `SYNCSTAT` `PROCESSRESOURCE` `NETWORK` `TRANSACTION`
`IOSTORAGE` `LOCK` `INSTATUS` `FORDR` `AGENTSTATUS` `COREMETRICS`。
SHALL NOT 通过接口发现分组码（平台无枚举接口）。

#### Scenario: 分组码写错

- **WHEN** 使用了一个不存在的分组码
- **THEN** 平台返回 200 但 series 为空；采集端 SHALL 记录一条空分组告警日志（不静默）

### Requirement: 监控指标多 series 的处理

A 类指标在平台上按组件给出多条 series，而客户指标是实例级。
采集端 SHALL 按映射表逐组件各产出一条消息，维度键 `COMPONENT_ID` 归入 `tags`；
SHALL NOT 在聚合口径未确认前自行求和。

#### Scenario: 一个指标对应 3 条 series

- **WHEN** 某客户指标在响应中有 3 条组件 series
- **THEN** 产出 3 条消息，每条的 tags 含各自的 `COMPONENT_ID`

### Requirement: 命令中心通道的执行

B 类通道 SHALL 通过统一命令中心执行**单条合并 SQL**，
SHALL 按视图的刷新档位决定该视图的数据新鲜度，
SHALL 遵守平台的输出硬上限。

合并 SQL 的 SQL 文本 SHALL 由**运行时按分页窗口生成**（从 `merged_sql.json` 的视图定义
与 `paging` 段渲染），SHALL NOT 依赖一个固定的成品 SQL 文件 ——
固定文件只能覆盖第 1 页，无法翻页。
分页容量 SHALL 可配（`CMD_PAGE_SIZE`，默认 100），SHALL NOT 超过平台的 `limit` 上限 1000。

#### Scenario: 一次下发

- **WHEN** 一个命令中心周期到期
- **THEN** 下发 1 条合并 SQL，取回全部 11 个视图的数据（可增长视图按页推进）

#### Scenario: 视图为空页

- **WHEN** 某一页中可增长视图无数据（平台把 `json_agg` 的 NULL 渲染成空串）
- **THEN** 该视图本轮视为空，SHALL NOT 抛出解析异常，SHALL NOT 把空页当作「该视图没有数据」

#### Scenario: 输出超限

- **WHEN** 平台返回体超过 102400 B
- **THEN** 平台整条任务失败（不截断）；采集端 SHALL 记录 ERROR 并缩小分页容量重试

#### Scenario: 结果长度预警

- **WHEN** 单次返回体长度超过上限的 90%
- **THEN** 采集端记录 WARNING 日志，提示接近硬上限

### Requirement: 诊断优化通道

采集端 SHALL 从慢 SQL 清单（`slow-sql-list`）与 Top SQL 清单（`top-sql-list`）
取 C 类 4 个指标的数据。

#### Scenario: 慢 SQL 集合条数

- **WHEN** 慢 SQL 清单接口返回 N 条
- **THEN** 产出「慢 SQL 集合条数」指标，值为 N

#### Scenario: 按 sql_text 前缀计数

- **WHEN** 需要统计 SELECT 类慢查询条数
- **THEN** 按 `sql_text` 的前缀分类计数，且 `sql_text` 在进入消息前已脱敏

### Requirement: 实例元数据通道

采集端 SHALL 从实例列表接口（`GET /rds/v3.2/instances`）取 D 类指标
（`gaussdb.server_version`），SHALL 按 1 小时档调度。

#### Scenario: 引擎版本

- **WHEN** 实例列表接口返回 `engine_version`
- **THEN** 产出 `gaussdb.server_version` 指标，值为该字符串原样

### Requirement: 告警通道的分页与归并

采集端 SHALL 拉取活动告警与历史告警的收敛列表与明细行，
SHALL 按 `limit ≤ 100` 分页取全，
SHALL 按 `alarm_id + instance_id` 把明细行归并进收敛条目的 `details` 数组。

平台的 `alarm_id` / `instance_id` 过滤**仅在 `is_convergence = false`（明细形态）时有效**；
采集端 SHALL 按此约束设计查询，不得假设过滤条件在收敛形态下同样生效。

#### Scenario: 超过 100 条

- **WHEN** 某次查询的 `total_count` 为 250
- **THEN** 采集端分页取完全部 250 条，不只取第一页

#### Scenario: 明细归并

- **WHEN** 同一 alarm_id + instance_id 有 3 条明细行
- **THEN** 归并进同一收敛条目的 `details`，产出 1 条消息

#### Scenario: 收敛形态下的过滤

- **WHEN** 采集端以 `is_convergence = true` 拉取收敛列表
- **THEN** 采集端不依赖 `alarm_id` / `instance_id` 过滤结果（该过滤在此形态下无效），
  改由拉全量后在本地按 `alarm_id + instance_id` 归并

### Requirement: 历史告警增量与超期检测

采集端 SHALL 按 `first_alarm_time` 水位只拉取新增或变化的告警记录，
SHALL 在每轮检查「最新历史告警时间距今 > 30 天」并显式告警。

#### Scenario: 存在超期缺口

- **WHEN** 最新历史告警时间距今超过 30 天
- **THEN** 记录 WARNING 级日志，说明平台侧已无更早数据、该段无法补回

#### Scenario: 只取增量

- **WHEN** 水位记录已处理到时间 T
- **THEN** 本轮只拉取 `first_alarm_time` 晚于 T 的记录

### Requirement: 告警节点身份回填

收敛条目本身不带节点身份。采集端 SHALL 按 `alarm_id` 关联明细行回填节点，
SHALL 在回填不到时落到节点清单里的**第一个 master CN**，
SHALL 保证跨轮稳定（同一告警不会因为「这轮关联到了哪个节点」而改变资源名）。

#### Scenario: 能回填

- **WHEN** 某收敛条目有对应的明细行
- **THEN** 其资源名使用该明细行的 `host_ip` 与 `node_id`

#### Scenario: 回填不到

- **WHEN** 某收敛条目没有任何明细行
- **THEN** 其资源名落到第一个 master CN（固定节点），且不因轮次变化
