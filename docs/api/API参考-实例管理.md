# 实例管理


---

## 查询数据库实例列表/查询实例详情（v3.2）


### 功能介绍

查询数据库实例列表/查询实例详情。


### 接口约束

通过调用API并发请求查询实例列表，并发请求量不大于10qps。


### URI

GET /gaussdb/v3.2/instances

**表1 Query参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| id | 否 | String | 参数解释： 实例ID。 “*”为系统保留字符，如果id是以“*”起始，表示按照*后面的值模糊匹配，否则，按照id精确匹配查询。不能只传入“*”。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| name | 否 | String | 参数解释： 实例名称。 “*”为系统保留字符，如果name是以“*”起始，表示按照*后面的值模糊匹配，否则，按照name精确匹配查询。不能只传入“*”。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| type | 否 | String | 参数解释： 按照实例类型查询，取值区分大小写。 约束限制： 不涉及。 取值范围： ；Combined：区分大小写，对应分布式版实例。 ；Ha：区分大小写，对应集中式版实例。 默认取值： 不涉及。 |
| datastore_type | 否 | String | 参数解释： 数据库类型，不区分大小写。 约束限制： 不涉及。 GaussDB：GaussDB类型数据库。 默认取值： GaussDB。 |
| offset | 否 | Integer | 参数解释： 索引位置，偏移量。从第一条数据偏移offset条数据后开始查询，默认为0（偏移0条数据，表示从第一条数据开始查询）。 约束限制： 必须为数字，不能为负数。 取值范围： 不涉及。 默认取值： 0 |
| limit | 否 | Integer | 参数解释： 查询记录数。 约束限制： 不涉及。 取值范围： 最小值为1，最大值为100。 默认取值： 100 |


### 请求参数

**表2 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Mini-Token | 是 | String | 参数解释： 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| X-Language | 否 | String | 参数解释： 语言。 约束限制： 不涉及。 取值范围： ；zh-cn ；en-us 默认取值： en-us |


### 响应参数

状态码： 200

**表3 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| instances | Array of 表4 objects | 参数解释： 实例信息。 取值范围： 不涉及。 |
| total_count | Integer | 参数解释： 总记录数。 取值范围： 不涉及。 |

**表4 ListInstanceResult**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| id | String | 参数解释： 实例ID。 取值范围： 不涉及。 |
| name | String | 参数解释： 创建的实例名称。 取值范围： 不涉及。 |
| status | String | 参数解释： 实例状态。 取值范围： ；值为“BUILD”，表示实例正在创建。 ；值为“BUILD_FAILED”，表示实例创建失败。 ；值为“ACTIVE”，表示实例正常。 ；值为“FAILED”，表示实例异常。 ；值为“EXPANDING”，表示实例正在添加只读节点或扩容DN。 ；值为“REBOOTING”，表示实例正在重启。 ；值为“REDUCING”, 表示实例正在删除只读节点。 ；值为“UPGRADING”，表示实例正在升级。 ；值为“RESTORING”，表示实例正在恢复。 ；值为“SWITCHOVER”，表示实例正在主备切换。 ；值为“MIGRATING”，表示实例正在迁移。 ；值为“BACKING UP”，表示实例正在进行备份。 ；值为“CHANGING DEPLOYMENT SOLUTION”，表示实例正在进行形态变更（升、降副本）。 ；值为“STORAGE FULL”，表示实例磁盘空间满。 ；值为“REPAIRING”，表示实例正在修复。 ；值为“SHUTDOWN”，表示实例处于停止状态。 |
| private_ips | Array of strings | 参数解释： 实例业务IP地址列表。 取值范围： 不涉及。 |
| private_ipv6s | Array of strings | 参数解释： 取值范围： 不涉及。 实例业务IPv6地址列表。 |
| port | Integer | 参数解释： 数据库端口号。 取值范围： 不涉及。 |
| type | String | 参数解释： 实例类型。 取值范围： ；取值为“Combined”，对应于分布式版实例。 ；取值为“centralization_standard”，对应于集中式版实例。 |
| ha | 表5 object | 参数解释： 获取分布式版或者集中式版实例时返回。 取值范围： 不涉及。 |
| replica_num | Integer | 参数解释： 实例副本数。 取值范围： 不涉及。 |
| datastore | 表6 object | 参数解释： 数据库信息。 取值范围： 不涉及。 |
| created | String | 参数解释： 创建时间。 取值范围： 不涉及。 |
| updated | String | 参数解释： 更新时间。 取值范围： 不涉及。 |
| db_user_name | String | 参数解释： 默认用户名。 取值范围： 不涉及。 |
| flavor_ref | String | 参数解释： 规格码。 取值范围： 不涉及。 |
| flavor_info | 表7 object | 参数解释： 规格信息。 取值范围： 不涉及。 |
| volume | 表8 object | 参数解释： Volume信息。 取值范围： 不涉及。 |
| backup_strategy | 表9 object | 参数解释： 备份策略。 取值范围： 不涉及。 |
| nodes | Array of 表10 objects | 参数解释： 实例节点信息列表。 取值范围： 不涉及。 |
| instance_mode | String | 参数解释： enterprise为企业版。 取值范围： 不涉及。 |
| time_zone | String | 参数解释： 时区。 取值范围： 不涉及。 |

**表5 ListHaResult**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| consistency | String | 参数解释： 数据库一致性类型，分布式模式实例仅有。 取值范围： ；strong：强一致性。 ；eventual：最终一致性。 |
| replication_mode | String | 参数解释： 备机同步参数。 取值范围： 非空。 GaussDB为“sync”，“sync”为同步模式。 |
| consistency_protocol | String | 参数解释： 副本一致性协议。 取值范围： ；quorum ；paxos |

**表6 ListDatastore**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| type | String | 参数解释： 数据库引擎。 取值范围： 不涉及。 |
| version | String | 参数解释： 数据库引擎大版本。 取值范围： 不涉及。 |
| complete_version | String | 参数解释： 数据库引擎小版本。 取值范围： 不涉及。 |
| complete_kernel_version | String | 参数解释： 内核引擎版本。 取值范围： 不涉及。 |

**表7 ListFlavorInfo**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| vcpu | Integer | 参数解释： CPU核数。 取值范围： 不涉及。 |
| mem | Integer | 参数解释： 内存大小。 取值范围： 不涉及。 |

**表8 ListVolume**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| type | String | 参数解释： 磁盘类型。 取值范围： 不涉及。 |
| size | Integer | 参数解释： 磁盘大小。 取值范围： 不涉及。 |

**表9 GaussBackupStrategyForListResponse**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| start_time | String | 参数解释： 备份时间段。自动备份将在该时间段内触发。 当前时间指UTC时间。 取值范围： 不涉及。 |
| keep_days | Integer | 参数解释： 已生成的备份文件可以保存的天数。 取值范围： 1～732。 |

**表10 NodeResult**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| id | String | 参数解释： 节点ID。 取值范围： 不涉及。 |
| name | String | 参数解释： 节点名称。 取值范围： 不涉及。 |
| hostname | String | 参数解释： 主机名称。 取值范围： 不涉及。 |
| role | String | 参数解释： 节点类型。 取值范围： ；master：主节点。 ；slave：备节点。 ；secondary：日志节点。 ；readreplica：只读节点。 ；arbiter：仲裁节点。 |
| status | String | 参数解释： 节点状态。 取值范围： 不涉及。 |
| availability_zone | String | 参数解释： 可用区。 取值范围： 不涉及。 |
| private_ip | String | 参数解释： 业务IP，用于业务访问。 取值范围： 不涉及。 |
| data_ip | String | 参数解释： 数据IP，用于数据库实例组件之间通信。 取值范围： 不涉及。 |
| management_ip | String | 参数解释： 管理IP，用于云数据库GaussDB管理平台（TPOPS）与数据库节点通信。 取值范围： 不涉及。 |
| private_ipv6 | String | 参数解释： 业务IPv6，用于业务访问。可与业务IP双栈访问。 取值范围： 不涉及。 |
| data_ipv6 | String | 参数解释： 数据IPv6，用于数据库实例组件之间通信。 取值范围： 不涉及。 |
| management_ipv6 | String | 参数解释： 管理IPv6，用于云数据库GaussDB管理平台（TPOPS）与数据库节点通信。 取值范围： 不涉及。 |
| components_names | String | 参数解释： 实例组件信息。该字段不会实时更新组件状态。 取值范围： 不涉及。 |


### 请求示例

根据指定条件查询实例列表

```
GET https://{Endpoint}/rds/gaussdb/v3.2/instances?id=ed7cc6166ec24360a5ed5c5c9c2ed726in14&name=hy&type=Enterprise&datastore_type=GaussDB&offset=0&limit=10
```


### 响应示例

状态码： 200

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
