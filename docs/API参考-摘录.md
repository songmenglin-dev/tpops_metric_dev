# GaussDB API 参考摘录

> **来源**：GaussDB V2.0-26.861.1 产品文档 (for 轻量化部署形态) — `06_GaussDB V2.0-26.861.1 产品文档(for 轻量化部署形态) 01.chm`
> **章节范围**：开发指南 › API参考 › API › 监控大盘 / 实例管理 / 平台用户管理 › 登录
> **提取方式**：CHM 解包 → HTML（GB2312）→ Markdown 自动转换
> **包含接口**：5 个

---

## 目录

- **监控大盘**（`开发指南 › API参考 › API › 监控大盘`）
  - [查询监控指标](#api-1)
  - [查询监控指标分组](#api-2)
  - [健康检查](#api-3)
- **实例管理**（`开发指南 › API参考 › API › 实例管理`）
  - [查询数据库实例列表/查询实例详情（v3.2）](#api-4)
- **平台用户管理 › 登录**（`开发指南 › API参考 › API › 平台用户管理 › 登录`）
  - [生成Token（获取用户Token）](#api-5)

---

## 监控大盘

*目录路径：`开发指南 › API参考 › API › 监控大盘`*

<a id="api-1"></a>

### 1. 查询监控指标

*源文件：`zh-cn_topic_0000002724181647.html`*

### 功能介绍

查询监控指标。

### URI

POST /v3/monitor/metrics

### 请求参数

**表1 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Mini-Token | 是 | String | **参数解释：** 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 **约束限制：** 不涉及。 **取值范围：** 不涉及。 **默认取值：** 不涉及。 |
| X-Language | 否 | String | **参数解释：** 语言。 **约束限制：** 不涉及。 **取值范围：** - **zh-cn**<br>- **en-us** **默认取值：** **en-us** |

**表2 请求Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| start_time | 是 | Long | **参数解释：** 监控数据请求开始时间，需传毫秒级别时间戳。 **约束限制：** 不涉及。 **取值范围：** 不涉及。 **默认取值：** 不涉及。 |
| end_time | 是 | Long | **参数解释：** 监控数据请求结束时间，需传毫秒级别时间戳，监控指标查询时间段需小于30天。 **约束限制：** 不涉及。 **取值范围：** 不涉及。 **默认取值：** 不涉及。 |
| is_latest_point_only | 否 | String | **参数解释：** 是否请求最近一个时间点的监控数据,取值为"true"或者"false"。 **约束限制：** 不涉及。 **取值范围：** 不涉及。 **默认取值：** 不涉及。 |
| instances | 是 | Array of 表3 objects | **参数解释：** 监控数据请求实例参数。 **约束限制：** 不涉及。 **取值范围：** 不涉及。 **默认取值：** 不涉及。 |
| metric_names | 是 | Array of strings | **参数解释：** 监控数据请求指标列表（每项监控指标对应的名字见监控指标项）。 **约束限制：** 不涉及。 **取值范围：** 不涉及。 **默认取值：** 不涉及。 |

**表3 InstanceReq**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| instance_id | 是 | String | **参数解释：** 实例ID。 **约束限制：** 不涉及。 **取值范围：** 只能由英文字母、数字组成，且长度为36个字符。 **默认取值：** 不涉及。 |
| node_ids | 否 | Array of strings | **参数解释：** 对应实例的节点ID列表。 **约束限制：** 不涉及。 **取值范围：** 不涉及。 **默认取值：** 不涉及。 |

### 响应参数

**状态码： 200**

**表4 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| metric_infos | Array of 表5 objects | **参数解释：** 请求的监控数据。 **取值范围：** 不涉及。 |

**表5 Metrics**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| instance_id | String | **参数解释：** 实例ID。 **取值范围：** 不涉及。 |
| time_interval | Integer | **参数解释：** 默认时间间隔，取决于所选的时间范围。 **取值范围：** 不涉及。 |
| metrics | Array of 表6 objects | **参数解释：** 指标信息。 **取值范围：** 不涉及。 |

**表6 Metric**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| name | String | **参数解释：** 指标名。 **取值范围：** 不涉及。 |
| node_id | String | **参数解释：** 节点ID。 **取值范围：** 如果请求Body中is_latest_point_only参数填true，此时无论请求Body中node_ids参数传入的节点数量，都只会返回最近一个时间点的监控数据，因此返回体中node_id字段总是为空。 如果请求Body中is_latest_point_only参数填false，则是按所选时间范围返回请求Body中node_ids参数传入的节点的监控数据，此时node_id字段响应值为请求Body中node_ids参数（node_ids为空则默认查询所有节点数据）所填写的节点ID列表。 |
| collection_frequency | Integer | **参数解释：** 指标采集频率。 **取值范围：** 不涉及。 |
| series | Map&lt;String,Array&lt;Number&gt; | **参数解释：** 指标维度及指标值。 **取值范围：** 不涉及。 |
| timestamps | Array of numbers | **参数解释：** 时间戳。 **取值范围：** 不涉及。 |

**状态码： 400**

**表7 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| errCode | String | **参数解释：** 错误码。 **取值范围：** 不涉及。 |
| externalMessage | String | **参数解释：** 错误描述。 **取值范围：** 不涉及。 |

### 请求示例

```
POST https://{Endpoint}/rds/v3/monitor/metrics
{
  "start_time": 1713230727384,
  "end_time": 1713234327384,
  "is_latest_point_only": false,
  "instances": [
    {
      "instance_id": "f9f73d520cb34a0fad6d674350012673in14"
    }
  ],
  "metric_names": [
    "sys001_cpu_usage"
  ]
}
```

### 响应示例

**状态码： 200**

API操作成功

```
{
  "metric_infos": [
    {
      "metrics": [
        {
          "name": "sys001_cpu_usage",
          "series": {
            "192.168.1.*": [
              23.05,
              13.07,
              25.39,
              16.88,
              22.77
            ],
            "192.168.1.*": [
              20.69,
              20.16,
              27.97,
              24.72,
              36.31
            ],
            "192.168.1.*": [
              25.43,
              20.39,
              24.5,
              19.39,
              29.69
            ],
            "192.168.1.*": [
              21.08,
              20.32,
              29.41,
              24.41,
              29.21
            ]
          },
          "timestamps": [
            1713230727000,
            1713230737000,
            1713230747000,
            1713230757000,
            1713230767000
          ],
          "node_id": "0a105fbc4dd3412fb37ed418a9ae8bd8no14,268d32dba3764c9ab3de999fb67d7edbno14,34d3ae97babc431eb47edcd0c1ed510dno14,febc8345c2174cc1a05cb5e799e8fcc5no14",
          "collection_frequency": 10
        }
      ],
      "instance_id": "f9f73d520cb34a0fad6d674350012673in14",
      "time_interval": 10
    }
  ]
}
```

**状态码： 400**

API操作执行失败

```
{ 
  "errCode" : "DBS.280001", 
  "externalMessage" : "Parameter error. [metric_names]" 
}
```

### 状态码

| 状态码 | 描述 |
| --- | --- |
| 200 | 正常 |
| 400 | 异常 |

### 错误码

请参见错误码。

---

<a id="api-2"></a>

### 2. 查询监控指标分组

*源文件：`zh-cn_topic_0000002694542582.html`*

### 功能介绍

查询监控指标分组。

### URI

GET /v3/monitor/metrics/series

**表1 Query参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| name | 是 | Array | **参数解释：** 监控指标分组名。 **约束限制：** 不涉及。 **取值范围：** CPUMEMORY，CONNECTION，SYNCSTAT，PROCESSRESOURCE, NETWORK，TRANSACTION，IOSTORAGE，LOCK，INSTATUS，FORDR，AGENTSTATUS，COREMETRICS。 **默认取值：** 不涉及。 |
| instance_id | 否 | String | **参数解释：** 实例ID，此参数是用户创建实例的唯一标识；该参数非必填，若不传该参数，则返回值中的告警阈值字段为null，其他字段正常返回。 **约束限制：** 不涉及。 **取值范围：** 只能由英文字母、数字组成，且长度为36个字符。 **默认取值：** 不涉及。 |

### 请求参数

**表2 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Language | 否 | String | **参数解释：** 语言。 **约束限制：** 不涉及。 **取值范围：** - **zh-cn**<br>- **en-us** **默认取值：** **en-us** |
| X-Mini-Token | 是 | String | **参数解释：** 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 **约束限制：** 不涉及。 **取值范围：** 不涉及。 **默认取值：** 不涉及。 |

### 响应参数

**状态码： 200**

**表3 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| series | Array of 表4 objects | **参数解释：** 监控指标分组信息。 **取值范围：** 不涉及。 |

**表4 MetricGroup**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| name | String | **参数解释：** 监控指标分组名。 **取值范围：** CPUMEMORY，CONNECTION，SYNCSTAT，PROCESSRESOURCE, NETWORK，TRANSACTION，IOSTORAGE，LOCK，INSTATUS，FORDR，AGENTSTATUS，COREMETRICS。 |
| metrics | Array of 表5 objects | **参数解释：** 指标具体信息。 **取值范围：** 不涉及。 |

**表5 singleMetric**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| name | String | **参数解释：** 监控指标名字。 **取值范围：** 不涉及。 |
| unit | String | **参数解释：** 监控指标单位。 **取值范围：** 不涉及。 |
| alarm_id | String | **参数解释：** 监控指标对应的告警ID。 **取值范围：** 不涉及。 |
| alarm_threshold | Double | **参数解释：** 监控指标对应的告警阈值。 **取值范围：** 不涉及。 |

**状态码： 400**

**表6 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| errCode | String | **参数解释：** 错误码。 **取值范围：** 不涉及。 |
| externalMessage | String | **参数解释：** 错误描述。 **取值范围：** 不涉及。 |

### 请求示例

- 查询监控指标分组信息
  ```
  GET https://{Endpoint}/rds/v3/monitor/metrics/series?name=CPUMEMORY
  ```

- 查询监控指标分组信息，包括告警阈值
  ```
  GET https://{Endpoint}/rds/v3/monitor/metrics/series?name=CPUMEMORY&instance_id=000000000000000000000000000000000001
  ```

### 响应示例

- **状态码： 200**
  查询监控指标分组，不带实例ID，仅查询指标ID、单位及告警ID。
  ```
  { 
    "series" : [ { 
      "name" : "CPUMEMORY", 
      "metrics" : [ { 
        "name" : "sys025_swap_used_ratio", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys010_mem_usage", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys_percent", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "mem_size", 
        "unit" : "GB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "cpu_sys_usage", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys024_swap_total_size", 
        "unit" : "MB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "mem_usage", 
        "unit" : "%", 
        "alarm_id" : "2001010002", 
        "alarm_threshold" : null 
      }, { 
        "name" : "idle_percent", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "user_percent", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "cpu_user_usage", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys001_cpu_usage", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "io_wait_percent", 
        "unit" : "%", 
        "alarm_id" : "2001010009", 
        "alarm_threshold" : null 
      }, { 
        "name" : "buffer_size", 
        "unit" : "GB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "mem_used_size", 
        "unit" : "GB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys013_mem_free_size", 
        "unit" : "GB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys026_swap_used_size", 
        "unit" : "MB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "cpu_wait_usage", 
        "unit" : "%", 
        "alarm_id" : "2001010009", 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys016_cpu_load", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "cache_size", 
        "unit" : "GB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys002_idle_usage", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "cpu_percent", 
        "unit" : "%", 
        "alarm_id" : "2001010005", 
        "alarm_threshold" : null 
      } ] 
    } ] 
  }
  ```

- **状态码： 200**
  查询监控指标分组，带实例ID，查询指标ID、单位、告警ID及告警阈值。
  ```
  { 
    "series" : [ { 
      "name" : "CPUMEMORY", 
      "metrics" : [ { 
        "name" : "sys025_swap_used_ratio", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys010_mem_usage", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys_percent", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "mem_size", 
        "unit" : "GB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "cpu_sys_usage", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys024_swap_total_size", 
        "unit" : "MB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "mem_usage", 
        "unit" : "%", 
        "alarm_id" : "2001010002", 
        "alarm_threshold" : 95 
      }, { 
        "name" : "idle_percent", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "user_percent", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "cpu_user_usage", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys001_cpu_usage", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "io_wait_percent", 
        "unit" : "%", 
        "alarm_id" : "2001010009", 
        "alarm_threshold" : null 
      }, { 
        "name" : "buffer_size", 
        "unit" : "GB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "mem_used_size", 
        "unit" : "GB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys013_mem_free_size", 
        "unit" : "GB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys026_swap_used_size", 
        "unit" : "MB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "cpu_wait_usage", 
        "unit" : "%", 
        "alarm_id" : "2001010009", 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys016_cpu_load", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "cache_size", 
        "unit" : "GB", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "sys002_idle_usage", 
        "unit" : "%", 
        "alarm_id" : null, 
        "alarm_threshold" : null 
      }, { 
        "name" : "cpu_percent", 
        "unit" : "%", 
        "alarm_id" : "2001010005", 
        "alarm_threshold" : 95 
      } ] 
    } ] 
  }
  ```

**状态码： 400**

API操作失败

```
{ 
  "errCode" : "DBS.280270", 
  "externalMessage" : "The parameter does not exist." 
}
```

### 状态码

| 状态码 | 描述 |
| --- | --- |
| 200 | 正常 |
| 400 | 异常 |

### 错误码

请参见错误码。

---

<a id="api-3"></a>

### 3. 健康检查

*源文件：`zh-cn_topic_0000002724181995.html`*

### 功能介绍

检查GaussDB服务的健康状态，当前主要判断GaussDB-open-api服务是否正常。

### URI

GET /health-status

### 请求参数

无

### 响应参数

**状态码：200**

服务健康正常

### 请求示例

```
GET https://{Endpoint}/rds/health-status
```

### 响应示例

**状态码： 200**

### 状态码

| 状态码 | 描述 |
| --- | --- |
| 200 | GaussDB-open-api服务健康正常 |
| 非200或接口超时 | GaussDB-open-api服务异常 |

### 错误码

请参见错误码。

---

## 实例管理

*目录路径：`开发指南 › API参考 › API › 实例管理`*

<a id="api-4"></a>

### 4. 查询数据库实例列表/查询实例详情（v3.2）

*源文件：`zh-cn_topic_0000002694702356.html`*

### 功能介绍

查询数据库实例列表/查询实例详情。

### 接口约束

通过调用API并发请求查询实例列表，并发请求量不大于10qps。

### URI

GET /gaussdb/v3.2/instances

**表1 Query参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| id | 否 | String | **参数解释：** 实例ID。 “*”为系统保留字符，如果id是以“*”起始，表示按照*后面的值模糊匹配，否则，按照id精确匹配查询。不能只传入“*”。 **约束限制：** 不涉及。 **取值范围：** 不涉及。 **默认取值：** 不涉及。 |
| name | 否 | String | **参数解释：** 实例名称。 “*”为系统保留字符，如果name是以“*”起始，表示按照*后面的值模糊匹配，否则，按照name精确匹配查询。不能只传入“*”。 **约束限制：** 不涉及。 **取值范围：** 不涉及。 **默认取值：** 不涉及。 |
| type | 否 | String | **参数解释：** 按照实例类型查询，取值区分大小写。 **约束限制：** 不涉及。 **取值范围：** - **Combined**：区分大小写，对应分布式版实例。<br>- **Ha**：区分大小写，对应集中式版实例。 **默认取值：** 不涉及。 |
| datastore_type | 否 | String | **参数解释：** 数据库类型，不区分大小写。 **约束限制：** 不涉及。 **GaussDB**：GaussDB类型数据库。 **默认取值：** GaussDB。 |
| offset | 否 | Integer | **参数解释：** 索引位置，偏移量。从第一条数据偏移offset条数据后开始查询，默认为0（偏移0条数据，表示从第一条数据开始查询）。 **约束限制：** 必须为数字，不能为负数。 **取值范围：** 不涉及。 **默认取值：** 0 |
| limit | 否 | Integer | **参数解释：** 查询记录数。 **约束限制：** 不涉及。 **取值范围：** 最小值为1，最大值为100。 **默认取值：** 100 |

### 请求参数

**表2 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Mini-Token | 是 | String | **参数解释：** 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 **约束限制：** 不涉及。 **取值范围：** 不涉及。 **默认取值：** 不涉及。 |
| X-Language | 否 | String | **参数解释：** 语言。 **约束限制：** 不涉及。 **取值范围：** - **zh-cn**<br>- **en-us** **默认取值：** **en-us** |

### 响应参数

**状态码： 200**

**表3 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| instances | Array of 表4 objects | **参数解释：** 实例信息。 **取值范围：** 不涉及。 |
| total_count | Integer | **参数解释：** 总记录数。 **取值范围：** 不涉及。 |

**表4 ListInstanceResult**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| id | String | **参数解释：** 实例ID。 **取值范围：** 不涉及。 |
| name | String | **参数解释：** 创建的实例名称。 **取值范围：** 不涉及。 |
| status | String | **参数解释：** 实例状态。 **取值范围：** - 值为“BUILD”，表示实例正在创建。<br>- 值为“BUILD_FAILED”，表示实例创建失败。<br>- 值为“ACTIVE”，表示实例正常。<br>- 值为“FAILED”，表示实例异常。<br>- 值为“EXPANDING”，表示实例正在添加只读节点或扩容DN。<br>- 值为“REBOOTING”，表示实例正在重启。<br>- 值为“REDUCING”, 表示实例正在删除只读节点。<br>- 值为“UPGRADING”，表示实例正在升级。<br>- 值为“RESTORING”，表示实例正在恢复。<br>- 值为“SWITCHOVER”，表示实例正在主备切换。<br>- 值为“MIGRATING”，表示实例正在迁移。<br>- 值为“BACKING UP”，表示实例正在进行备份。<br>- 值为“CHANGING DEPLOYMENT SOLUTION”，表示实例正在进行形态变更（升、降副本）。<br>- 值为“STORAGE FULL”，表示实例磁盘空间满。<br>- 值为“REPAIRING”，表示实例正在修复。<br>- 值为“SHUTDOWN”，表示实例处于停止状态。 |
| private_ips | Array of strings | **参数解释：** 实例业务IP地址列表。 **取值范围：** 不涉及。 |
| private_ipv6s | Array of strings | **参数解释：** **取值范围：** 不涉及。 实例业务IPv6地址列表。 |
| port | Integer | **参数解释：** 数据库端口号。 **取值范围：** 不涉及。 |
| type | String | **参数解释：** 实例类型。 **取值范围：** - 取值为“Combined”，对应于分布式版实例。<br>- 取值为“centralization_standard”，对应于集中式版实例。 |
| ha | 表5 object | **参数解释：** 获取分布式版或者集中式版实例时返回。 **取值范围：** 不涉及。 |
| replica_num | Integer | **参数解释：** 实例副本数。 **取值范围：** 不涉及。 |
| datastore | 表6 object | **参数解释：** 数据库信息。 **取值范围：** 不涉及。 |
| created | String | **参数解释：** 创建时间。 **取值范围：** 不涉及。 |
| updated | String | **参数解释：** 更新时间。 **取值范围：** 不涉及。 |
| db_user_name | String | **参数解释：** 默认用户名。 **取值范围：** 不涉及。 |
| flavor_ref | String | **参数解释：** 规格码。 **取值范围：** 不涉及。 |
| flavor_info | 表7 object | **参数解释：** 规格信息。 **取值范围：** 不涉及。 |
| volume | 表8 object | **参数解释：** Volume信息。 **取值范围：** 不涉及。 |
| backup_strategy | 表9 object | **参数解释：** 备份策略。 **取值范围：** 不涉及。 |
| nodes | Array of 表10 objects | **参数解释：** 实例节点信息列表。 **取值范围：** 不涉及。 |
| instance_mode | String | **参数解释：** enterprise为企业版。 **取值范围：** 不涉及。 |
| time_zone | String | **参数解释：** 时区。 **取值范围：** 不涉及。 |

**表5 ListHaResult**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| consistency | String | **参数解释：** 数据库一致性类型，分布式模式实例仅有。 **取值范围：** - strong：强一致性。<br>- eventual：最终一致性。 |
| replication_mode | String | **参数解释：** 备机同步参数。 **取值范围：** 非空。 GaussDB为“sync”，“sync”为同步模式。 |
| consistency_protocol | String | **参数解释：** 副本一致性协议。 **取值范围：** - quorum<br>- paxos |

**表6 ListDatastore**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| type | String | **参数解释：** 数据库引擎。 **取值范围：** 不涉及。 |
| version | String | **参数解释：** 数据库引擎大版本。 **取值范围：** 不涉及。 |
| complete_version | String | **参数解释：** 数据库引擎小版本。 **取值范围：** 不涉及。 |
| complete_kernel_version | String | **参数解释：** 内核引擎版本。 **取值范围：** 不涉及。 |

**表7 ListFlavorInfo**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| vcpu | Integer | **参数解释：** CPU核数。 **取值范围：** 不涉及。 |
| mem | Integer | **参数解释：** 内存大小。 **取值范围：** 不涉及。 |

**表8 ListVolume**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| type | String | **参数解释：** 磁盘类型。 **取值范围：** 不涉及。 |
| size | Integer | **参数解释：** 磁盘大小。 **取值范围：** 不涉及。 |

**表9 GaussBackupStrategyForListResponse**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| start_time | String | **参数解释：** 备份时间段。自动备份将在该时间段内触发。 当前时间指UTC时间。 **取值范围：** 不涉及。 |
| keep_days | Integer | **参数解释：** 已生成的备份文件可以保存的天数。 **取值范围：** 1～732。 |

**表10 NodeResult**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| id | String | **参数解释：** 节点ID。 **取值范围：** 不涉及。 |
| name | String | **参数解释：** 节点名称。 **取值范围：** 不涉及。 |
| hostname | String | **参数解释：** 主机名称。 **取值范围：** 不涉及。 |
| role | String | **参数解释：** 节点类型。 **取值范围：** - master：主节点。<br>- slave：备节点。<br>- secondary：日志节点。<br>- readreplica：只读节点。<br>- arbiter：仲裁节点。 |
| status | String | **参数解释：** 节点状态。 **取值范围：** 不涉及。 |
| availability_zone | String | **参数解释：** 可用区。 **取值范围：** 不涉及。 |
| private_ip | String | **参数解释：** 业务IP，用于业务访问。 **取值范围：** 不涉及。 |
| data_ip | String | **参数解释：** 数据IP，用于数据库实例组件之间通信。 **取值范围：** 不涉及。 |
| management_ip | String | **参数解释：** 管理IP，用于云数据库GaussDB管理平台（TPOPS）与数据库节点通信。 **取值范围：** 不涉及。 |
| private_ipv6 | String | **参数解释：** 业务IPv6，用于业务访问。可与业务IP双栈访问。 **取值范围：** 不涉及。 |
| data_ipv6 | String | **参数解释：** 数据IPv6，用于数据库实例组件之间通信。 **取值范围：** 不涉及。 |
| management_ipv6 | String | **参数解释：** 管理IPv6，用于云数据库GaussDB管理平台（TPOPS）与数据库节点通信。 **取值范围：** 不涉及。 |
| components_names | String | **参数解释：** 实例组件信息。该字段不会实时更新组件状态。 **取值范围：** 不涉及。 |

### 请求示例

根据指定条件查询实例列表

```
GET https://{Endpoint}/rds/gaussdb/v3.2/instances?id=ed7cc6166ec24360a5ed5c5c9c2ed726in14&name=hy&type=Enterprise&datastore_type=GaussDB&offset=0&limit=10
```

### 响应示例

**状态码： 200**

Success.

```
{ 
  "instances" : [ { 
    "id" : "b331ed66cc3249f78bc20737308c01f4in14", 
    "status" : "ACTIVE", 
    "name" : "gauss-9e88", 
    "port" : 8000, 
    "type" : "centralization_standard", 
    "ha" : { 
      "consistency" : "strong", 
      "replication_mode" : "sync" 
    }, 
    "datastore" : { 
      "type" : "GaussDB", 
      "version" : "V2.0-1.3"，
      "complete_version" : "1.3.0",
      "complete_kernel_version" : "505.2.1"
    }, 
    "created" : "2021-01-15 01:46:40 UTC", 
    "updated" : "2021-01-15 02:05:03 UTC", 
    "volume" : { 
      "type" : "ULTRAHIGH", 
      "size" : 120 
    }, 
    "nodes" : [ { 
      "id" : "02ebf757aaf94074855f49cc6e0e4712no14", 
      "name" : "gauss-9e88_root_0", 
      "hostname" : "gauss-9e88_root_0", 
      "role" : "master", 
      "status" : "ACTIVE", 
      "availability_zone" : "az2xahz", 
      "private_ip" : "192.168.0.*", 
      "data_ip" : "192.168.0.*", 
      "management_ip" : "192.168.0.*",
      "component_names" : "cm_1:Primary, dn_6001:6001:Primary"
    }, { 
      "id" : "0a87b8ecbfeb46aba1409cfc0f0d5c34no14", 
      "name" : "gauss-9e88_root_1", 
      "hostname" : "gauss-9e88_root_1", 
      "role" : "slave", 
      "status" : "ACTIVE", 
      "availability_zone" : "az2xahz", 
      "private_ip" : "192.168.0.*", 
      "data_ip" : "192.168.0.*", 
      "management_ip" : "192.168.0.*",
      "component_names" : "cm_1:Primary, dn_6001:6001:Primary" 
    }, { 
      "id" : "2d9fec1ab3834936b074d63acf48b1f2no14", 
      "name" : "gauss-9e88_root_2", 
      "hostname" : "gauss-9e88_root_2", 
      "role" : "slave", 
      "status" : "ACTIVE", 
      "availability_zone" : "az2xahz", 
      "private_ip" : "192.168.0.*", 
      "data_ip" : "192.168.0.*", 
      "management_ip" : "192.168.0.*",
      "component_names" : "cm_1:Primary, dn_6001:6001:Primary"
    } ], 
    "private_ips" : [ "192.168.0.* / 192.168.0.* / 192.168.0.*" ], 
    "replica_num" : 3, 
    "db_user_name" : "root", 
    "flavor_ref" : "gaussdb.ee.bms.2xlarge.x868.ha", 
    "flavor_info" : { 
      "vcpu" : 8, 
      "mem" : 64 
    }, 
    "backup_strategy" : { 
      "start_time" : "19:00-20:00", 
      "keep_days" : 7 
    }, 
    "instance_mode" : "enterprise", 
    "time_zone" : "UTC+08:00" 
  } ], 
  "total_count" : 1 
}
```

### 状态码

| 状态码 | 描述 |
| --- | --- |
| 200 | 正常 |

### 错误码

请参见错误码。

---

## 平台用户管理 › 登录

*目录路径：`开发指南 › API参考 › API › 平台用户管理 › 登录`*

<a id="api-5"></a>

### 5. 生成Token（获取用户Token）

*源文件：`zh-cn_topic_0000002694702160.html`*

### 功能介绍

生成Token。

### URI

POST /v3/auth/mini-token

### 请求参数

**表1 请求Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| username | 是 | String | **参数解释：** 用户名称。 **约束限制：** 不涉及。 **取值范围：** 最小长度为3，最大长度为64。 **默认取值：** 不涉及。 |
| password | 是 | String | **参数解释：** 密码。 **约束限制：** 不涉及。 **取值范围：** 最小长度为8，最大长度为32。 **默认取值：** 不涉及。 |
| user_type | 否 | String | **参数解释：** 用户类型。 **约束限制：** 不涉及。 **取值范围：** - **tpops**：TPOPS用户。<br>- **ad**：AD用户。 **默认取值：** tpops。 |

### 响应参数

**状态码： 200**

**表2 响应Header参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| X-Mini-Token | String | **参数解释：** 获取到的Token信息。 **取值范围：** 不涉及。 |

**表3 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| user_id | String | **参数解释：** 用户ID。 **取值范围：** 不涉及。 |
| login_ip | String | **参数解释：** 访问的客户端IP。 **取值范围：** 不涉及。 |
| is_first_time_login | Boolean | **参数解释：** 是否首次登录。 **取值范围：** 不涉及。 |
| login_failed_attempts | String | **参数解释：** 失败登录的次数。 **取值范围：** 不涉及。 |
| last_login_statistics | 表4 object | **参数解释：** 上一次登录统计信息。 **取值范围：** 不涉及。 |
| session_timeout | Integer | **参数解释：** session过期时间，单位为分钟。 **取值范围：** 不涉及。 |
| password_expiration_remaining_days | String | **参数解释：** 密码剩余有效期天数。 **取值范围：** 不涉及。 |
| password_expiration_days | String | **参数解释：** 密码有效期天数。 **取值范围：** 不涉及。 |
| password_expiration_reminder | Boolean | **参数解释：** 提醒密码即将过期，密码剩余有效期天数不大于15天设置为true，否则为false。 **取值范围：** - true<br>- false |
| x_mini_token | String | **参数解释：** Token，和Header上的一致。 **取值范围：** 不涉及。 |

**表4 last_login_statistics**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| last_login_failed_ip | String | **参数解释：** 上一次失败登录地址。 **取值范围：** 不涉及。 |
| last_login_failed_time | String | **参数解释：** 上一次登录失败时间。 **取值范围：** 不涉及。 |
| last_login_ip | String | **参数解释：** 上一次成功登录地址。 **取值范围：** 不涉及。 |
| last_login_time | String | **参数解释：** 上一次成功登录时间。 **取值范围：** 不涉及。 |

**状态码： 400**

**表5 响应Header参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| X-Mini-Token | String | **参数解释：** 获取的Token。 **取值范围：** 不涉及。 |

**表6 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| error_code | String | **参数解释：** 错误码。 **取值范围：** 不涉及。 |
| error_msg | String | **参数解释：** 错误信息。 **取值范围：** 不涉及。 |

### 请求示例

```
POST https://{{Endpoint}}/rds/v3/auth/mini-token

{
    "username": "test",
    "password": "***"
}
```

### 响应示例

**状态码： 200**

获取token成功

```
{ 
  "user_id" : "wertsdfg-wrey-gfhj-sdrf-sdfgawerszdf", 
  "login_ip" : "192.168.0.*", 
  "login_failed_attempts" : 0, 
  "is_first_time_login" : false, 
  "last_login_statistics" : { 
    "last_login_ip" : "10.182.*.*", 
    "last_login_failed_ip" : "192.168.0.*", 
    "last_login_time" : "2023-05-17T09:00:00Z", 
    "last_login_failed_time" : "2023-05-16T09:00:00Z" 
  }, 
  "session_timeout" : 1440, 
  "password_expiration_days" : 90, 
  "password_expiration_remaining_days" : 56, 
  "password_expiration_reminder" : false,
  "x_mini_token" : "ABCDEFG..." 
}
```

### 状态码

| 状态码 | 描述 |
| --- | --- |
| 200 | 正常 |
| 400 | 异常 |

### 错误码

请参见错误码。

---
