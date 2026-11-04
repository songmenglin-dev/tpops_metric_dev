# 监控大盘


---

## 查询监控指标


### 功能介绍

查询监控指标。


### URI

POST /v3/monitor/metrics


### 请求参数

**表1 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Mini-Token | 是 | String | 参数解释： 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| X-Language | 否 | String | 参数解释： 语言。 约束限制： 不涉及。 取值范围： ；zh-cn ；en-us 默认取值： en-us |

**表2 请求Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| start_time | 是 | Long | 参数解释： 监控数据请求开始时间，需传毫秒级别时间戳。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| end_time | 是 | Long | 参数解释： 监控数据请求结束时间，需传毫秒级别时间戳，监控指标查询时间段需小于30天。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| is_latest_point_only | 否 | String | 参数解释： 是否请求最近一个时间点的监控数据,取值为"true"或者"false"。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| instances | 是 | Array of 表3 objects | 参数解释： 监控数据请求实例参数。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| metric_names | 是 | Array of strings | 参数解释： 监控数据请求指标列表（每项监控指标对应的名字见监控指标项）。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |

**表3 InstanceReq**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| instance_id | 是 | String | 参数解释： 实例ID。 约束限制： 不涉及。 取值范围： 只能由英文字母、数字组成，且长度为36个字符。 默认取值： 不涉及。 |
| node_ids | 否 | Array of strings | 参数解释： 对应实例的节点ID列表。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |


### 响应参数

状态码： 200

**表4 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| metric_infos | Array of 表5 objects | 参数解释： 请求的监控数据。 取值范围： 不涉及。 |

**表5 Metrics**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| instance_id | String | 参数解释： 实例ID。 取值范围： 不涉及。 |
| time_interval | Integer | 参数解释： 默认时间间隔，取决于所选的时间范围。 取值范围： 不涉及。 |
| metrics | Array of 表6 objects | 参数解释： 指标信息。 取值范围： 不涉及。 |

**表6 Metric**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| name | String | 参数解释： 指标名。 取值范围： 不涉及。 |
| node_id | String | 参数解释： 节点ID。 取值范围： 如果请求Body中is_latest_point_only参数填true，此时无论请求Body中node_ids参数传入的节点数量，都只会返回最近一个时间点的监控数据，因此返回体中node_id字段总是为空。 如果请求Body中is_latest_point_only参数填false，则是按所选时间范围返回请求Body中node_ids参数传入的节点的监控数据，此时node_id字段响应值为请求Body中node_ids参数（node_ids为空则默认查询所有节点数据）所填写的节点ID列表。 |
| collection_frequency | Integer | 参数解释： 指标采集频率。 取值范围： 不涉及。 |
| series | Map<String,Array<Number> | 参数解释： 指标维度及指标值。 取值范围： 不涉及。 |
| timestamps | Array of numbers | 参数解释： 时间戳。 取值范围： 不涉及。 |

状态码： 400

**表7 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| errCode | String | 参数解释： 错误码。 取值范围： 不涉及。 |
| externalMessage | String | 参数解释： 错误描述。 取值范围： 不涉及。 |


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

状态码： 200

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

状态码： 400

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

## 查询监控指标分组


### 功能介绍

查询监控指标分组。


### URI

GET /v3/monitor/metrics/series

**表1 Query参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| name | 是 | Array | 参数解释： 监控指标分组名。 约束限制： 不涉及。 取值范围： CPUMEMORY，CONNECTION，SYNCSTAT，PROCESSRESOURCE, NETWORK，TRANSACTION，IOSTORAGE，LOCK，INSTATUS，FORDR，AGENTSTATUS，COREMETRICS。 默认取值： 不涉及。 |
| instance_id | 否 | String | 参数解释： 实例ID，此参数是用户创建实例的唯一标识；该参数非必填，若不传该参数，则返回值中的告警阈值字段为null，其他字段正常返回。 约束限制： 不涉及。 取值范围： 只能由英文字母、数字组成，且长度为36个字符。 默认取值： 不涉及。 |


### 请求参数

**表2 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Language | 否 | String | 参数解释： 语言。 约束限制： 不涉及。 取值范围： ；zh-cn ；en-us 默认取值： en-us |
| X-Mini-Token | 是 | String | 参数解释： 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |


### 响应参数

状态码： 200

**表3 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| series | Array of 表4 objects | 参数解释： 监控指标分组信息。 取值范围： 不涉及。 |

**表4 MetricGroup**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| name | String | 参数解释： 监控指标分组名。 取值范围： CPUMEMORY，CONNECTION，SYNCSTAT，PROCESSRESOURCE, NETWORK，TRANSACTION，IOSTORAGE，LOCK，INSTATUS，FORDR，AGENTSTATUS，COREMETRICS。 |
| metrics | Array of 表5 objects | 参数解释： 指标具体信息。 取值范围： 不涉及。 |

**表5 singleMetric**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| name | String | 参数解释： 监控指标名字。 取值范围： 不涉及。 |
| unit | String | 参数解释： 监控指标单位。 取值范围： 不涉及。 |
| alarm_id | String | 参数解释： 监控指标对应的告警ID。 取值范围： 不涉及。 |
| alarm_threshold | Double | 参数解释： 监控指标对应的告警阈值。 取值范围： 不涉及。 |

状态码： 400

**表6 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| errCode | String | 参数解释： 错误码。 取值范围： 不涉及。 |
| externalMessage | String | 参数解释： 错误描述。 取值范围： 不涉及。 |


### 请求示例

```
GET https://{Endpoint}/rds/v3/monitor/metrics/series?name=CPUMEMORY
```

```
GET https://{Endpoint}/rds/v3/monitor/metrics/series?name=CPUMEMORY&instance_id=000000000000000000000000000000000001
```


### 响应示例

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

状态码： 400

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

## 健康检查


### 功能介绍

检查GaussDB服务的健康状态，当前主要判断GaussDB-open-api服务是否正常。


### URI

GET /health-status


### 请求参数

无


### 响应参数

状态码：200

服务健康正常


### 请求示例

```
GET https://{Endpoint}/rds/health-status
```


### 响应示例

状态码： 200


### 状态码

| 状态码 | 描述 |
| --- | --- |
| 200 | GaussDB-open-api服务健康正常 |
| 非200或接口超时 | GaussDB-open-api服务异常 |


### 错误码

请参见错误码。
