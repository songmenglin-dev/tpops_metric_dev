# 平台用户管理 › 登录


---

## 生成Token


### 功能介绍

生成Token。


### URI

POST /v3/auth/mini-token


### 请求参数

**表1 请求Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| username | 是 | String | 参数解释： 用户名称。 约束限制： 不涉及。 取值范围： 最小长度为3，最大长度为64。 默认取值： 不涉及。 |
| password | 是 | String | 参数解释： 密码。 约束限制： 不涉及。 取值范围： 最小长度为8，最大长度为32。 默认取值： 不涉及。 |
| user_type | 否 | String | 参数解释： 用户类型。 约束限制： 不涉及。 取值范围： ；tpops：TPOPS用户。 ；ad：AD用户。 默认取值： tpops。 |


### 响应参数

状态码： 200

**表2 响应Header参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| X-Mini-Token | String | 参数解释： 获取到的Token信息。 取值范围： 不涉及。 |

**表3 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| user_id | String | 参数解释： 用户ID。 取值范围： 不涉及。 |
| login_ip | String | 参数解释： 访问的客户端IP。 取值范围： 不涉及。 |
| is_first_time_login | Boolean | 参数解释： 是否首次登录。 取值范围： 不涉及。 |
| login_failed_attempts | String | 参数解释： 失败登录的次数。 取值范围： 不涉及。 |
| last_login_statistics | 表4 object | 参数解释： 上一次登录统计信息。 取值范围： 不涉及。 |
| session_timeout | Integer | 参数解释： session过期时间，单位为分钟。 取值范围： 不涉及。 |
| password_expiration_remaining_days | String | 参数解释： 密码剩余有效期天数。 取值范围： 不涉及。 |
| password_expiration_days | String | 参数解释： 密码有效期天数。 取值范围： 不涉及。 |
| password_expiration_reminder | Boolean | 参数解释： 提醒密码即将过期，密码剩余有效期天数不大于15天设置为true，否则为false。 取值范围： ；true ；false |
| x_mini_token | String | 参数解释： Token，和Header上的一致。 取值范围： 不涉及。 |

**表4 last_login_statistics**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| last_login_failed_ip | String | 参数解释： 上一次失败登录地址。 取值范围： 不涉及。 |
| last_login_failed_time | String | 参数解释： 上一次登录失败时间。 取值范围： 不涉及。 |
| last_login_ip | String | 参数解释： 上一次成功登录地址。 取值范围： 不涉及。 |
| last_login_time | String | 参数解释： 上一次成功登录时间。 取值范围： 不涉及。 |

状态码： 400

**表5 响应Header参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| X-Mini-Token | String | 参数解释： 获取的Token。 取值范围： 不涉及。 |

**表6 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| error_code | String | 参数解释： 错误码。 取值范围： 不涉及。 |
| error_msg | String | 参数解释： 错误信息。 取值范围： 不涉及。 |


### 请求示例

```
POST https://{{Endpoint}}/rds/v3/auth/mini-token

{
    "username": "test",
    "password": "***"
}
```


### 响应示例

状态码： 200

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
