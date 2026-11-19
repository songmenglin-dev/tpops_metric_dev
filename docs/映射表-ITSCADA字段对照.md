# ITSCADA 字段映射表

> **开发的直接依据**：左边是 TPOPS 取回来的原始字段，右边是客户 ITSCADA 要求的 Kafka 字段。
> 指标按采集分类分节，告警单独一节。
> 数据源：`data/mapping/` 下的分类映射文件（本文件由 `tools/build_field_mapping_doc.py` 生成，勿手改）。

依据文档：`docs/ITSCADA第三方指标数据接入对接方案V2.docx`、`docs/ITSCADA第三方告警数据接入对接方案V2.docx`。

## 1. 指标字段映射表

#### A 类　监控指标接口

接口：`POST /rds/v3/monitor/metrics`

**TPOPS 侧取到的字段**

| 字段 | 含义 |
|---|---|
| `metric_infos[].metrics[].series` | 维度键 → 数值数组，值可能是 null |
| `metric_infos[].metrics[].timestamps` | 时间戳数组，与 series 同下标对应 |
| `metric_infos[].metrics[].name` | 平台指标英文 id，如 rds092_gaussv5_commit_counter |
| `metric_infos[].metrics[].metric_name_cn` | 平台中文名，如「用户提交事务数/秒」 |
| `metric_infos[].metrics[].collection_frequency` | 平台采集周期（秒） |
| `metric_infos[].instance_id` | 实例 ID |
| `metric_infos[].time_interval` | 采样间隔（秒） |
| `nodes[].component_id` | 节点清单里的组件 ID，用来把 series 的键换算成 node_id |
| `nodes[].node_id` | 节点 ID，拼 resourceName 用 |
| `nodes[].node_ip` | 节点 IP，写进 tags |
| `instances[].name` | 实例名，如 gauss-75a2 |

**映射到客户 Kafka 字段**

| 客户字段 | 类型 | 必填 | 来源 | 原始字段 / 取哪个字段 | 怎么定位 | 转换规则 |
| --- | --- | --- | --- | --- | --- | --- |
| `metricName` | String | 必填 | 映射表 | `data/mapping/metric_rules.json → metric_name` | 用 TPOPS 返回的 metrics[].name 去匹配该条目的 source.tpops_metric | 把 {#XXX} 换成维度实际值 |
| `metricCode` | String | 必填 | 映射表 | `data/mapping/metric_rules.json → metric_code` | 用 TPOPS 返回的 metrics[].name 去匹配该条目的 source.tpops_metric | 把 {#XXX} 换成维度实际值 |
| `resourceName` | String | 必填 | 组装 | `nodes[].node_ip + nodes[].node_id` | 由上面几个字段组装 | resourceName 只能表示节点 → 拼 {host_ip}_{node_id}；series 的键取 component_id（去掉 ip 前缀）→ 节点清单换算 node_id/node_ip；与节点无关时落到节点清单里的第一个 master CN |
| `value` | String | 必填 | TPOPS 字段 | `metric_infos[].metrics[].series[<维度键>][i]` | TPOPS 响应里直接取 | 取最后一个非空点；该点是 null 就不产出这条消息（不填 0） |
| `unit` | String | 选填 | 映射表 | `data/mapping/metric_rules.json → unit` | 用 TPOPS 返回的 metrics[].name 去匹配该条目的 source.tpops_metric | 客户指标表【指标单位】，为空则 null |
| `clock` | Long | 必填 | TPOPS 字段 | `metric_infos[].metrics[].timestamps[i]` | TPOPS 响应里直接取 | i 与 value 取到的点是同一下标 |
| `tags` | JSON | 选填 | 组装 | `instances[] / nodes[] / series 的键` | 由上面几个字段组装 | instance_id、instance_name、node_id、component_id、host_ip、dimension_values |

#### B 类　命令中心 SQL

接口：`POST /rds/gaussdb/v3/command-center/customer-commands/{cmd_id}（三跳取结果）`

**TPOPS 侧取到的字段**

| 字段 | 含义 |
|---|---|
| `result` | JSON 字符串，要二次 json.loads 才是行数组；行里的值全是字符串 |
| `node_id` | 执行该 SQL 的节点 ID |
| `node_name` | 节点名 |
| `rows[].<列名>` | SQL 结果行里的列，列名 = SQL 的 as 别名。stat_database 18 列：datname、numbackends、xact_commit、xact_rollback、deadlocks、conflicts、blks_read、blks_hit、blk_read_time、blk_write_time、tup_inserted、tup_updated、tup_deleted、tup_returned、tup_fetched、temp_files、temp_bytes、cache_hit_ratio |
| `instances[].name` | 实例名 |

**映射到客户 Kafka 字段**

| 客户字段 | 类型 | 必填 | 来源 | 原始字段 / 取哪个字段 | 怎么定位 | 转换规则 |
| --- | --- | --- | --- | --- | --- | --- |
| `metricName` | String | 必填 | 映射表 | `data/mapping/metric_rules.json → metric_name` | 用合并 SQL 的 sql_key + 结果列名，匹配该条目的 source.sql_key 与 value_rule.params.column | 把 {#XXX} 换成维度实际值 |
| `metricCode` | String | 必填 | 映射表 | `data/mapping/metric_rules.json → metric_code` | 用合并 SQL 的 sql_key + 结果列名，匹配该条目的 source.sql_key 与 value_rule.params.column | 把 {#XXX} 换成维度实际值 |
| `resourceName` | String | 必填 | 组装 | `nodes[].node_ip + nodes[].node_id` | 由上面几个字段组装 | resourceName 只能表示节点 → 拼 {host_ip}_{node_id}；数据按节点分布时取执行该 SQL 的节点，与节点无关时落到节点清单里的第一个 master CN |
| `value` | String | 必填 | TPOPS 字段 | `result → 行数组 → metric_rules[].value_rule 指定的列` | TPOPS 响应里直接取 | 按 value_rule 执行（raw / aggregate / counter_delta / ratio_across …）；原始值是字符串，按客户表【数据类型】转型；row_policy 决定逐行出还是跨行求和 |
| `unit` | String | 选填 | 映射表 | `data/mapping/metric_rules.json → unit` | 用合并 SQL 的 sql_key + 结果列名，匹配该条目的 source.sql_key 与 value_rule.params.column | 客户指标表【指标单位】，为空则 null |
| `clock` | Long | 必填 | 组装 | `本轮采集完成时刻` | 由上面几个字段组装 | 命令中心返回的是当前快照，没有自己的时间戳 |
| `tags` | JSON | 选填 | 组装 | `instances[] / node_id / 行里的维度列` | 由上面几个字段组装 | instance_id、instance_name、node_id、component_id、host_ip、dimension_values |

#### C 类　诊断优化接口

接口：`POST /rds/gaussdb/v3/instances/{instance_id}/slow-sql-list、.../top-sql-list`

**TPOPS 侧取到的字段**

| 字段 | 含义 |
|---|---|
| `slow_sql_infos[].sql_id` | 慢 SQL 唯一标识 |
| `slow_sql_infos[].sql_text` | 慢 SQL 文本（含业务 SQL，外发前要脱敏） |
| `slow_sql_infos[].calls` | 执行次数 |
| `slow_sql_infos[].db_name` | 所属数据库 |
| `top_sql_infos[].sql_id` | SQL 唯一标识 |
| `top_sql_infos[].sql_text` | SQL 文本（需脱敏） |
| `top_sql_infos[].total_elapse_time` | 总耗时，用于排 Top N |
| `instances[].name` | 实例名 |

**映射到客户 Kafka 字段**

| 客户字段 | 类型 | 必填 | 来源 | 原始字段 / 取哪个字段 | 怎么定位 | 转换规则 |
| --- | --- | --- | --- | --- | --- | --- |
| `metricName` | String | 必填 | 映射表 | `data/mapping/metric_rules.json → metric_name` | 用接口与集合名（slow_sql_infos / top_sql_infos）匹配 | 把 {#XXX} 换成维度实际值 |
| `metricCode` | String | 必填 | 映射表 | `data/mapping/metric_rules.json → metric_code` | 用接口与集合名（slow_sql_infos / top_sql_infos）匹配 | 把 {#XXX} 换成维度实际值 |
| `resourceName` | String | 必填 | 组装 | `nodes[].node_ip + nodes[].node_id` | 由上面几个字段组装 | resourceName 只能表示节点 → 拼 {host_ip}_{node_id}；接口是实例级的，落到节点清单里的第一个 master CN |
| `value` | String | 必填 | TPOPS 字段 | `slow_sql_infos[] / top_sql_infos[]` | TPOPS 响应里直接取 | 按 value_rule 执行：集合条数 / 按 sql_text 前缀计数（Select、DML）/ 按 total_elapse_time 取前 10 条 |
| `unit` | String | 选填 | 映射表 | `data/mapping/metric_rules.json → unit` | 用接口与集合名（slow_sql_infos / top_sql_infos）匹配 | 客户指标表【指标单位】，为空则 null |
| `clock` | Long | 必填 | 组装 | `本轮采集完成时刻` | 由上面几个字段组装 | 接口返回的是当前快照，没有自己的时间戳 |
| `tags` | JSON | 选填 | 组装 | `instances[]` | 由上面几个字段组装 | instance_id、instance_name、node_id、component_id、host_ip、dimension_values |

#### D 类　实例接口

接口：`GET /rds/v3.2/instances`

**TPOPS 侧取到的字段**

| 字段 | 含义 |
|---|---|
| `instances[].id` | 实例 ID |
| `instances[].name` | 实例名，如 gauss-75a2 |
| `instances[].mode` | 部署形态，Combined / Ha |
| `instances[].engine_version` | 引擎版本，如 V2.0-10.0.0 |
| `instances[].instance_status` | 实例状态，normal 等 |

**映射到客户 Kafka 字段**

| 客户字段 | 类型 | 必填 | 来源 | 原始字段 / 取哪个字段 | 怎么定位 | 转换规则 |
| --- | --- | --- | --- | --- | --- | --- |
| `metricName` | String | 必填 | 映射表 | `data/mapping/metric_rules.json → metric_name` | 实例接口只有 1 个指标：gaussdb.server_version | 把 {#XXX} 换成维度实际值 |
| `metricCode` | String | 必填 | 映射表 | `data/mapping/metric_rules.json → metric_code` | 实例接口只有 1 个指标：gaussdb.server_version | 把 {#XXX} 换成维度实际值 |
| `resourceName` | String | 必填 | 组装 | `nodes[].node_ip + nodes[].node_id` | 由上面几个字段组装 | resourceName 只能表示节点 → 拼 {host_ip}_{node_id}；接口是实例级的，落到节点清单里的第一个 master CN |
| `value` | String | 必填 | TPOPS 字段 | `instances[].engine_version` | TPOPS 响应里直接取 | 原样（字符型指标） |
| `unit` | String | 选填 | 映射表 | `data/mapping/metric_rules.json → unit` | 实例接口只有 1 个指标：gaussdb.server_version | 客户指标表【指标单位】，为空则 null |
| `clock` | Long | 必填 | 组装 | `本轮采集完成时刻` | 由上面几个字段组装 | 接口返回的是当前快照，没有自己的时间戳 |
| `tags` | JSON | 选填 | 组装 | `instances[]` | 由上面几个字段组装 | instance_id、instance_name、node_id、component_id、host_ip、dimension_values |

## 2. 告警字段映射表

#### 告警

接口：`POST /rds/v3/alarm/records`

- **convergence**（请求参数 `is_convergence=true`）：收敛条目：一条 = 实例 + alarm_id，作主条目
- **detail**（请求参数 `is_convergence=false`）：明细行：每节点一行，补节点与描述

> 两种记录的 JSON 形状相同，都是 records[] 数组；区别只在请求参数。下表的 in 列标明字段在哪一种记录里。


**TPOPS 侧取到的字段**

| 字段 | 记录类型 | 含义 |
|---|---|---|
| `records[].alarm_id` | convergence | 告警 ID，ITSCADA 用它去重与跟踪状态 |
| `records[].alarm_name` | convergence | 告警名，如「节点NTP状态异常」 |
| `records[].severity` | convergence | 告警等级：critical / major / minor / notice |
| `records[].status` | convergence | 告警状态：activated / recovered / cleared |
| `records[].alarm_type` | convergence | metric / event / system-metric / system-event / system |
| `records[].alarm_category` | convergence | 告警分类，如「节点状态」 |
| `records[].engine` | convergence | 引擎名，如 gaussdb |
| `records[].instance_id` | convergence | 实例 ID |
| `records[].instance_name` | convergence | 实例名 |
| `records[].alarm_items_sum` | convergence | 命中的节点数 |
| `records[].first_alarm_time` | convergence | 首次触发时间（13 位毫秒） |
| `records[].last_alarm_time` | convergence | 最近触发时间（13 位毫秒） |
| `records[].clear_alarm_time` | convergence | 恢复时间（13 位毫秒），只有历史告警有 |
| `records[].alarm_duration` | convergence | 持续时长描述，如「4小时5分钟」 |
| `records[].cause` | convergence | 原因描述 |
| `records[].component_name` | convergence | 告警归属组件，如「实例管理」 |
| `records[].node_id` | detail | 节点 ID |
| `records[].host_ip` | detail | 节点 IP |
| `records[].description` | detail | 告警描述 |
| `records[].impaction` | detail | 影响 |
| `records[].possible_causes` | detail | 可能原因 |
| `records[].processing_steps` | detail | 处理步骤 |
| `records[].alarm_detail_raise_count` | detail | 触发次数 |
| `records[].notify_status` | detail | 通知状态 |
| `records[].automatic_clear` | detail | 是否可自动清除 |
| `records[].resource_id` | detail | 资源标识（实例ID-节点ID-告警标识） |
| `records[].alarm_remarks` | detail | 告警说明 |
| `records[].paramFillList` | detail | 参数列表 |

**映射到客户 Kafka 字段**

| 客户字段 | 类型 | 必填 | 来源 | 取自哪种记录 | 原始字段 / 取哪个字段 | 怎么定位 | 转换规则 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `alarmId` | String | 必填 | TPOPS 字段 | convergence | `records[].alarm_id` | TPOPS 响应里直接取 | 原样 |
| `alarmName` | String | 选填 | TPOPS 字段 | convergence | `records[].alarm_name` | TPOPS 响应里直接取 | 原样 |
| `alarmContent` | String | 必填 | TPOPS 字段 | detail 优先，没有明细行时用 convergence | `records[].description / records[].cause / records[].alarm_name` | TPOPS 响应里直接取 | 按这个顺序取第一个非空 |
| `priority` | Integer | 必填 | TPOPS 字段 | convergence | `records[].severity` | TPOPS 响应里直接取 | critical→1（紧急）/ major→2（重要）/ minor→3（一般）/ notice→4（预警）；severity 缺失时用告警定义里的默认等级 |
| `alarmTime` | Long | 必填 | TPOPS 字段 | convergence | `records[].first_alarm_time` | TPOPS 响应里直接取 | 原样 |
| `status` | Integer | 必填 | TPOPS 字段 | convergence | `records[].status` | TPOPS 响应里直接取 | activated→1（告警）/ recovered、cleared→0（恢复） |
| `recoverTime` | Long | 条件必填 | TPOPS 字段 | convergence | `records[].clear_alarm_time` | TPOPS 响应里直接取 | status=0（恢复）时必填；status=1（告警）时输出 null |
| `resourceName` | String | 必填 | 组装 | 明细行自带 node_id 与 host_ip；收敛条目不带，按 alarm_id 关联明细行回填 | `records[].node_id / records[].host_ip` | 由上面几个字段组装 | resourceName 只能表示节点 → 拼 {host_ip}_{node_id}；仍拿不到节点时落到节点清单里的第一个 master CN |
| `metricName` | String | 必填 | 映射表 | 按 alarm_id 查表 | `data/mapping/alarm_rules.json → metric_name` | 用 alarm_id 匹配 alarm_rules.json 的 definitions[alarm_id] | 能对应到客户指标的告警用客户指标中文名；其余用告警名 |
| `metricCode` | String | 必填 | 映射表 | 按 alarm_id 查表 | `data/mapping/alarm_rules.json → metric_code` | 用 alarm_id 匹配 alarm_rules.json 的 definitions[alarm_id] | 能对应到客户指标的告警用客户指标英文名；其余用 gaussdb.alarm.<alarm_id> |
| `value` | String | 必填 | TPOPS 字段 | convergence | `records[].alarm_items_sum` | TPOPS 响应里直接取 | 事件类告警取命中节点数；指标类告警取该客户指标最近一次采样值（取不到则 null，不填 0） |
| `unit` | String | 选填 | 映射表 | 按挂到的客户指标查表 | `data/mapping/metric_rules.json → unit` | 按挂到的客户指标查该条目的 unit | 挂到客户指标的告警取该指标单位；事件类告警为 null |
| `tags` | Object | 选填 | 组装 | 两者都取 | `records[].instance_id / records[].instance_name / records[].node_id / records[].host_ip / records[].alarm_category / records[].alarm_type / records[].engine / records[].alarm_duration / records[]（明细行整行）` | 由上面几个字段组装 | 前 8 个平铺，明细行整行放 details[] |

**ITSCADA 字段表里没有的原始字段**（统一放进 `tags.details`）

| 字段 | 处置 |
|---|---|
| `impaction` | ITSCADA 告警字段表没有对应字段 → 放进 tags.details |
| `possible_causes` | 同上 |
| `processing_steps` | 同上 |
| `alarm_detail_raise_count` | 同上 |
| `notify_status` | 同上 |
| `automatic_clear` | 同上 |
| `last_alarm_time` | 同上（ITSCADA 用 alarmTime 表示触发时间） |
| `resource_id` | 同上 |
| `alarm_remarks` | 同上 |
| `component_name` | 同上 |
| `paramFillList` | 同上 |

## 3. 待客户确认

## 待客户确认

| # | 事项 | 当前默认 | 说明 |
|---|---|---|---|
| 1 | value 的线格式 | 按客户指标表【数据类型】序列化（整数/浮点→数字，字符/文本→字符串） | 两份方案的字段表都写 value 为 String，但指标样例里是数字 75.3、告警样例里是字符串 "95.5"。 |
| 2 | 指标名字段的叫法 | 用字段表的 metricName / metricCode | 指标方案的字段表用 metricName/metricCode，告警方案的样例 JSON 里写的是 metric_name。 |
| 3 | resourceName 与客户台账的对齐 | {host_ip}_{node_id}（节点 IP + TPOPS 节点 ID）；与节点无关的指标落到节点清单里的第一个 master CN | resourceName 只能表示一个节点 → 固定「节点 IP + 节点 ID」；表名/库名/端口这类业务维度改走 metricName/metricCode 的占位符与 tags.dimension_values。方案要求与 ITSCADA 台账录入的资源名称完全一致；台账命名规则待客户提供。 |
| 4 | 采集频率 | 以客户指标表【数据更新周期】为准（30 秒~24 小时共 7 档） | 方案建议「需告警的指标 1 分钟一次、无需告警的 5 分钟一次」，与客户表的分档不一致。 |
| 5 | 状态类指标的展示值转义 | 平台枚举值原样透传（实测 1=正常、0=异常） | 5 条状态类指标（实例状态、主备状态）的客户展示值转义未定义。 |
| 6 | 告警挂到哪个客户指标 | 257 条告警里只有 3 条能对应上客户指标（都指向 gaussdb.Tps），其余用 gaussdb.alarm.<alarm_id> 自造指标名 | 事件类告警没有对应的客户指标，需要客户确认这种挂法是否可接受。 |
| 7 | 慢 SQL 的 sql_text 能否外发 | 默认脱敏：字面量替换为 ?，并截断到 512 字符 | 含业务 SQL 文本，需客户书面确认。 |
| 8 | A 类多 series 的归集口径 | 暂按逐组件各出一条消息（现状实现），维度键 COMPONENT_ID 归入 tags | A 类 11 个客户指标都是实例级一条消息，但平台按组件给出多条 series（rds092/rds093/rds400 各 3 条 CN、rds402 6 条 CN+DN、rds133 3 条 CN、gaussdb_instance_status 1 条 value、datanode_switch_status 9 条 DN）。合成方式属业务口径：速率类可求和，启动时刻可取最早，主备状态可按 DN 展开或归集为布尔。确认后由 metric_rules.json 的 value_rule 承载，不改消息信封。 |

