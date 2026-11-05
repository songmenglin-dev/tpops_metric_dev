# 告警管理


---

## 告警对接测试


### 功能介绍

告警对接测试。


### 接口约束

仅支持http协议告警对接测试。


### URI

POST /v3/alarm/notifications/interconnection/check


### 请求参数

**表1 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Mini-Token | 是 | String | 参数解释： 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| X-Language | 否 | String | 参数解释： 语言。 约束限制： 不涉及。 取值范围： ；zh-cn ；en-us 默认取值： en-us |

**表2 请求Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| http | 是 | Array of 表3 objects | 参数解释： http告警对接配置信息。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| time_zone | 否 | String | 参数解释： 时区信息，请按照国际标准时区进行填写，例如：Asia/Shanghai。默认为UTC。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |

**表3 CheckNotificationRequest**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| proxy | 是 | String | 参数解释： 代理，已配置支持HTTPS发送的第三方平台服务器。 约束限制： 不涉及。 取值范围： 以http开头，以端口结尾。 默认取值： 不涉及。 |
| charset | 否 | String | 参数解释： 字符集，已配置支持HTTPS发送的第三方平台服务器支持的字符集。 约束限制： 不涉及。 取值范围： UTF-8或GBK，不区分大小写。 默认取值： UTF-8。 |
| url_template | 是 | String | 参数解释： URL模板，服务器的URL地址。 约束限制： 不涉及。 取值范围： 以/开头。 默认取值： 不涉及。 |
| header | 是 | String | 参数解释： Header模板，HTTP传输的头部内容（以json格式传入）。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| body | 是 | String | 参数解释： Body模板，HTTP以POST或PUT方式传输时的Body内容（仅支持以json对象或者json数组格式传入）；变量可以在此处定义然后由自定义脚本进行填充，或者在自定义脚本中直接添加。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| groovy_script | 否 | String | 参数解释： 脚本内容，当groovy_enabled参数为true时，需要填写该脚本内容。当前支持自定义groovy脚本对Body模板进行修改补充。入参有四个字段： ；JSONObject body：为Body模板信息。 ；String message：为产生的告警信息。 ；JSONObject json：为message解析后的JSON串。 ；List<JSONObject>：List最后需要发送的消息信息，需要将修改的body添加到List中。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| groovy_enabled | 否 | Boolean | 参数解释： 自定义脚本，是否使用自定义脚本，若Body模板不满足平台要求，可以通过自定义脚本对模板进行修改、补充。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| ignore_duplicate_in_duration | 是 | Integer | 参数解释： 同一告警停发间隔，相同的告警，在停发间隔内再次上报，会被忽略，不会发出告警通知。 约束限制： 不涉及。 取值范围： 选择以下任意一项：0、30、60、300、900、1800、3600、10800、21600、43200、86400，单位为秒。 默认取值： 不涉及。 |
| response_template | 否 | String | 参数解释： 请求响应校验，对请求的响应值进行校验。默认采用正则匹配，比如： retCode对应返回值为0或1表示请求成功，使用'{"retCode":"0\|1"}'。 支持使用等值判断，针对特殊的返回值比如"0\|1"，使用'{"retCode_eQs":"0\|1"}'，避免直接使用正则时产生误判。 支持多个条件进行校验，需多个条件同时满足才能通过校验。 支持单字符匹配，比如："ok"。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| request_method | 是 | String | 参数解释： 请求方式，HTTP传输的请求方式。 约束限制： 不涉及。 取值范围： ；POST ；GET ；PUT 默认取值： 不涉及。 |
| alarm_template | 是 | String | 参数解释： 告警模板，上报告警时使用的模板。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| recovery_template | 是 | String | 参数解释： 恢复模板，告警恢复时使用的模板。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| alarm_level | 是 | Array of strings | 参数解释： 告警级别，满足勾选级别的告警才会被发出。 约束限制： 不涉及。 取值范围： 请在下述级别中选择一项或多项：critical、major、minor、notice。 默认取值： 不涉及。 |
| ssl_enabled | 否 | Boolean | 参数解释： 开启SSL，可选择启用支持SSL安全协议的“https”方式对接告警。若不启用，会使用“http”方式对接告警。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| is_enabled | 是 | Boolean | 参数解释： 是否启用。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |


### 响应参数

无


### 请求示例

```
POST https://{Endpoint}/rds/v3/alarm/notifications/interconnection/check

{
  "http" : [ {
    "charset":"UTF-8",  
    "is_enabled" : true,
    "header" : "{\"Content-Type\":\"application/json\"}",
    "body" : "{\"buzId\":\"GaussDBAlarm\",\"messageId\":\"$messageId\",\"params\":\"$alarmText\",\"templateId\":\"tmp001\",\"mobile\":\"13722221111\"}",
    "alarm_template" : "DefaultJSON",
    "recovery_template" : "DefaultJSON",
    "alarm_level" : [ "critical", "major", "minor", "notice" ],
    "proxy" : "http://10.*.*.*:9528",
    "url_template" : "/{}",
    "request_method" : "POST",
    "groovy_enabled" : true,
    "groovy_script" : "import java.time.LocalDateTime; import java.time.format.DateTimeFormatter; list.add(body); String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern(\"yyyyMMddHHmmss\")); Random random = new Random(); StringBuilder rds = new StringBuilder();for(int i = 0; i<4;i++){rds.append(random.nextInt(10));}\nString messageId = date + rds.toString(); body.put(\"messageId\",messageId);",
    "ssl_enabled" : false,
    "ignore_duplicate_in_duration": 0,
    "response_template" : "{\"retCode\":0}"
  } ],
 "time_zone":"Asia/Shanghai"
}
```


### 响应示例

无


### 状态码

| 状态码 | 描述 |
| --- | --- |
| 201 | 正常 |


### 错误码

请参见错误码。


---

## 新增告警对接配置


### 功能介绍

新增告警对接配置。


### 接口约束

仅支持新增http协议告警对接。


### URI

POST /v3/alarm/notifications


### 请求参数

**表1 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Mini-Token | 是 | String | 参数解释： 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| X-Language | 否 | String | 参数解释： 语言。 约束限制： 不涉及。 取值范围： ；zh-cn ；en-us 默认取值： en-us |

**表2 请求Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| instance_ids | 是 | Array of strings | 参数解释： 实例ID集合。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| http | 是 | Array of 表3 objects | 参数解释： http告警对接配置信息。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| time_zone | 否 | String | 参数解释： 时区信息，请按照国际标准时区进行填写，例如：Asia/Shanghai。默认为UTC。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |

**表3 HttpNotification**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| notification_id | 否 | String | 参数解释： 告警对接对应ID。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| proxy | 是 | String | 参数解释： 代理，已配置支持HTTPS发送的第三方平台服务器。 约束限制： 不涉及。 取值范围： 以http/https开头，以端口结尾。 默认取值： 不涉及。 |
| charset | 否 | String | 参数解释： 字符集，已配置支持HTTPS发送的第三方平台服务器支持的字符集。 约束限制： 不涉及。 取值范围： UTF-8或GBK，不区分大小写。 默认取值： UTF-8。 |
| url_template | 是 | String | 参数解释： URL模板，服务器的URL地址。 约束限制： 不涉及。 取值范围： 以/开头。 默认取值： 不涉及。 |
| header | 是 | String | 参数解释： Header模板，HTTP传输的头部内容（以json格式传入）。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| body | 是 | String | 参数解释： Body模板，HTTP以POST或PUT方式传输时的Body内容（仅支持以json对象或者json数组格式传入）；变量可以在此处定义然后由自定义脚本进行填充，或者在自定义脚本中直接添加。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| groovy_script | 否 | String | 参数解释： 脚本内容，当groovy_enabled参数为true时，需要填写该脚本内容。当前支持自定义groovy脚本对Body模板进行修改补充。入参有四个字段： ；JSONObject body：为Body模板信息。 ；String message：为产生的告警信息。 ；JSONObject json：为message解析后的JSON串。 ；List<JSONObject>：List最后需要发送的消息信息，需要将修改的body添加到List中。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| groovy_enabled | 否 | Boolean | 参数解释： 自定义脚本，是否使用自定义脚本，若Body模板不满足平台要求，可以通过自定义脚本对模板进行修改、补充。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| response_template | 否 | String | 参数解释： 请求响应校验，对请求的响应值进行校验。默认采用正则匹配，比如： retCode对应返回值为0或1表示请求成功，使用'{"retCode":"0\|1"}'。 支持使用等值判断，针对特殊的返回值比如"0\|1"，使用'{"retCode_eQs":"0\|1"}'，避免直接使用正则时产生误判。 支持多个条件进行校验，需多个条件同时满足才能通过校验。 支持单字符匹配，比如："ok"。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| request_method | 是 | String | 参数解释： 请求方式，HTTP传输的请求方式。 约束限制： 不涉及。 取值范围： ；POST ；GET ；PUT 默认取值： 不涉及。 |
| ignore_duplicate_in_duration | 是 | Integer | 参数解释： 同一告警停发间隔，相同的告警，在停发间隔内再次上报，会被忽略，不会发出告警通知。 约束限制： 不涉及。 取值范围： 选择以下任意一项：0、30、60、300、900、1800、3600、10800、21600、43200、86400，单位为秒。 默认取值： 不涉及。 |
| alarm_template | 是 | String | 参数解释： 告警模板，上报告警时使用的模板。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| recovery_template | 是 | String | 参数解释： 恢复模板，告警恢复时使用的模板。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| alarm_level | 是 | Array of strings | 参数解释： 告警级别，满足勾选级别的告警才会被发出。 约束限制： 不涉及。 取值范围： 请在下述级别中选择一项或多项：critical、major、minor、notice。 默认取值： 不涉及。 |
| is_enabled | 是 | Boolean | 参数解释： 是否启用。新增告警对接时，只能为true。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| ssl_enabled | 否 | Boolean | 参数解释： 开启SSL，可选择启用支持SSL安全协议的“https”方式对接告警。若不启用，会使用“http”方式对接告警。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| is_validated | 否 | Boolean | 参数解释： 是否验证连接通过。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |


### 响应参数

无


### 请求示例

```
POST https://{Endpoint}/rds/v3/alarm/notifications

{
  "instance_ids" : [ "4fee4b5e8cef49f6b4bf90cff8868a7bin14", "20c738c85bd542fcab0ecc3331bae88ein14" ],
  "http" : [ {
    "charset":"UTF-8",
    "proxy" : "http://10.*.*.*:9528",
    "url_template" : "/{}",
    "header" : "{\"Content-Type\":\"application/json\"}",
    "body" : "{\"buzId\":\"GaussDBAlarm\",\"messageId\":\"$messageId\",\"params\":\"$alarmText\",\"templateId\":\"tmp001\",\"mobile\":\"13722221111\"}",
    "groovy_script" : "import java.time.LocalDateTime; import java.time.format.DateTimeFormatter; list.add(body); String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern(\"yyyyMMddHHmmss\")); Random random = new Random(); StringBuilder rds = new StringBuilder();for(int i = 0; i<4;i++){rds.append(random.nextInt(10));}\nString messageId = date + rds.toString(); body.put(\"messageId\",messageId);",
    "groovy_enabled" : false,
    "response_template" : "{\"retCode\":0}",
    "request_method" : "POST",
    "ignore_duplicate_in_duration" : 0,
    "alarm_template" : "DefaultText",
    "recovery_template" : "DefaultText",
    "alarm_level" : [ "critical", "major", "minor", "notice" ],
    "is_enabled" : true,
    "ssl_enabled" : false,
    "notification_id" : "6e431181-731b-45e9-8b67-1cac34448393"
  } ],
 "time_zone":"Asia/Shanghai"
}
```


### 响应示例

无


### 状态码

| 状态码 | 描述 |
| --- | --- |
| 201 | 正常 |


### 错误码

请参见错误码。


---

## 修改告警对接配置


### 功能介绍

修改告警对接配置。


### 接口约束

仅支持对已存在http协议告警对接的实例进行http协议告警对接配置的修改。


### URI

PUT /v3/alarm/notifications


### 请求参数

**表1 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Mini-Token | 是 | String | 参数解释： 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| X-Language | 否 | String | 参数解释： 语言。 约束限制： 不涉及。 取值范围： ；zh-cn ；en-us 默认取值： en-us |

**表2 请求Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| instance_ids | 是 | Array of strings | 参数解释： 实例ID集合。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| http | 是 | Array of 表3 objects | 参数解释： 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 http告警对接配置信息。 |
| time_zone | 否 | String | 参数解释： 时区信息，请按照国际标准时区进行填写，例如：Asia/Shanghai。默认为UTC。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |

**表3 HttpNotification**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| notification_id | 否 | String | 参数解释： 告警对接对应ID。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| proxy | 是 | String | 参数解释： 代理，已配置支持HTTPS发送的第三方平台服务器。 约束限制： 不涉及。 取值范围： 以http/https开头，以端口结尾。 默认取值： 不涉及。 |
| charset | 否 | String | 参数解释： 字符集，已配置支持HTTPS发送的第三方平台服务器支持的字符集。 约束限制： 不涉及。 取值范围： UTF-8或GBK，不区分大小写。 默认取值： UTF-8。 |
| url_template | 是 | String | 参数解释： URL模板，服务器的URL地址。 约束限制： 不涉及。 取值范围： 以/开头 默认取值： 不涉及。 |
| header | 是 | String | 参数解释： Header模板，HTTP传输的头部内容（以json格式传入）。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| body | 是 | String | 参数解释： Body模板，HTTP以POST或PUT方式传输时的Body内容（仅支持以json对象或者json数组格式传入）；变量可以在此处定义然后由自定义脚本进行填充，或者在自定义脚本中直接添加。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| groovy_script | 否 | String | 参数解释： 脚本内容，当enable_groovy参数为true时，需要填写该脚本内容。当前支持自定义groovy脚本对Body模板进行修改补充。入参有四个字段： ；JSONObject body：为Body模板信息。 ；String message：为产生的告警信息。 ；JSONObject json：为message解析后的JSON串。 ；List<JSONObject>：List最后需要发送的消息信息，需要将修改的body添加到List中。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| groovy_enabled | 否 | Boolean | 参数解释： 自定义脚本，是否使用自定义脚本，若Body模板不满足平台要求，可以通过自定义脚本对模板进行修改、补充。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| response_template | 否 | String | 参数解释： 请求响应校验，对请求的响应值进行校验。默认采用正则匹配，比如： retCode对应返回值为0或1表示请求成功，使用'{"retCode":"0\|1"}'。 支持使用等值判断，针对特殊的返回值比如"0\|1"，使用'{"retCode_eQs":"0\|1"}'，避免直接使用正则时产生误判。 支持多个条件进行校验，需多个条件同时满足才能通过校验。 支持单字符匹配，比如："ok"。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| request_method | 是 | String | 参数解释： 请求方式，HTTP传输的请求方式。 约束限制： 不涉及。 取值范围： ；POST ；GET ；PUT 默认取值： 不涉及。 |
| ignore_duplicate_in_duration | 是 | Integer | 参数解释： 同一告警停发间隔，相同的告警，在停发间隔内再次上报，会被忽略，不会发出告警通知。 约束限制： 不涉及。 取值范围： 选择以下任意一项：0、30、60、300、900、1800、3600、10800、21600、43200、86400。单位为秒。 默认取值： 不涉及。 |
| alarm_template | 是 | String | 参数解释： 告警模板，上报告警时使用的模板。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| recovery_template | 是 | String | 参数解释： 恢复模板，告警恢复时使用的模板。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| alarm_level | 是 | Array of strings | 参数解释： 告警级别，满足勾选级别的告警才会被发出。 约束限制： 不涉及。 取值范围： 请在下述级别中选择一项或多项：critical、major、minor、notice。 默认取值： 不涉及。 |
| is_enabled | 是 | Boolean | 参数解释： 是否启用。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| ssl_enabled | 否 | Boolean | 参数解释： 开启SSL，可选择启用支持SSL安全协议的“https”方式对接告警。若不启用，会使用“http”方式对接告警。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| is_validated | 否 | Boolean | 参数解释： 是否验证连接通过。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |


### 响应参数

无


### 请求示例

```
POST https://{Endpoint}/rds/v3/alarm/notifications

{
  "instance_ids" : [ "4fee4b5e8cef49f6b4bf90cff8868a7bin14", "20c738c85bd542fcab0ecc3331bae88ein14" ],
  "http" : [ {
    "proxy" : "http://10.*.*.*:9528",
    "charset":"UTF-8",
    "url_template" : "/{}",
    "header" : "{\"Content-Type\":\"application/json\"}",
    "body" : "{\"buzId\":\"GaussDBAlarm\",\"messageId\":\"$messageId\",\"params\":\"$alarmText\",\"templateId\":\"tmp001\",\"mobile\":\"13722221111\"}",
    "groovy_script" : "import java.time.LocalDateTime; import java.time.format.DateTimeFormatter; list.add(body); String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern(\"yyyyMMddHHmmss\")); Random random = new Random(); StringBuilder rds = new StringBuilder();for(int i = 0; i<4;i++){rds.append(random.nextInt(10));}\nString messageId = date + rds.toString(); body.put(\"messageId\",messageId);",
    "groovy_enabled" : false,
    "response_template" : "{\"retCode\":0}",
    "request_method" : "POST",
    "ignore_duplicate_in_duration" : 0,
    "alarm_template" : "DefaultText",
    "recovery_template" : "DefaultText",
    "alarm_level" : [ "critical", "major", "minor", "notice" ],
    "is_enabled" : false,
    "ssl_enabled" : false,
    "notification_id" : "6e431181-731b-45e9-8b67-1cac34448393"
  } ],
 "time_zone":"Asia/Shanghai"
}
```


### 响应示例

无


### 状态码

| 状态码 | 描述 |
| --- | --- |
| 200 | 正常 |


### 错误码

请参见错误码。


---

## 查看告警对接配置


### 功能介绍

查看告警对接配置。


### URI

GET /v3/instances/{instance_id}/alarm/notifications

**表1 路径参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| instance_id | 是 | String | 参数解释： 实例ID。 约束限制： 不涉及。 取值范围： 只能由英文字母、数字组成，且长度为36个字符。 默认取值： 不涉及。 |


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
| instance_id | String | 参数解释： 实例ID。 取值范围： 不涉及。 |
| http | Array of 表4 objects | 参数解释： http告警对接信息。 取值范围： 不涉及。 |
| snmp3 | Array of 表5 objects | 参数解释： snmp3告警对接信息。 取值范围： 不涉及。 |
| syslog | Array of 表7 objects | 参数解释： syslog告警对接信息。 取值范围： 不涉及。 |
| snmp2 | Array of 表 Snmp2Conf objects | 参数解释： snmp2配置信息。 取值范围： 不涉及。 |

**表4 HttpNotification**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| notification_id | String | 参数解释： 告警对接对应ID。 取值范围： 不涉及。 |
| proxy | String | 参数解释： 代理，已配置支持HTTPS发送的第三方平台服务器。以http/https开头，以端口结尾。 取值范围： 不涉及。 |
| charset | String | 参数解释： 字符集，已配置支持HTTPS发送的第三方平台服务器支持的字符集。 取值范围： UTF-8或GBK |
| url_template | String | 参数解释： URL模板，服务器的URL地址，以/开头。 取值范围： 不涉及。 |
| header | String | 参数解释： Header模板，HTTP传输的头部内容（以json格式传入）。 取值范围： 不涉及。 |
| body | String | 参数解释： Body模板，HTTP以POST或PUT方式传输时的Body内容。 取值范围： 不涉及。 |
| groovy_script | String | 参数解释： 脚本内容，当enable_groovy参数为true时，需要填写该脚本内容。当前支持自定义groovy脚本对Body模板进行修改补充。 取值范围： 不涉及。 |
| groovy_enabled | Boolean | 参数解释： 自定义脚本，是否使用自定义脚本，若Body模板不满足平台要求，可以通过自定义脚本对模板进行修改、补充。 取值范围： 不涉及。 |
| response_template | String | 参数解释： 请求响应校验，对请求的响应值进行校验。 取值范围： 不涉及。 |
| request_method | String | 参数解释： 请求方式，HTTP传输的请求方式。 取值范围： 不涉及。 |
| ignore_duplicate_in_duration | Integer | 参数解释： 同一告警停发间隔，相同的告警，在停发间隔内再次上报，会被忽略，不会发出告警通知。 取值范围： 不涉及。 |
| alarm_template | String | 参数解释： 告警模板，上报告警时使用的模板。 取值范围： 不涉及。 |
| recovery_template | String | 参数解释： 恢复模板，告警恢复时使用的模板。 取值范围： 不涉及。 |
| alarm_level | Array of strings | 参数解释： 告警级别，满足勾选级别的告警才会被发出。 取值范围： 不涉及。 |
| is_enabled | Boolean | 参数解释： 是否启用。 |
| ssl_enabled | Boolean | 参数解释： 开启SSL，可选择启用支持SSL安全协议的“https”方式对接告警。若不启用，会使用“http”方式对接告警。 取值范围： 不涉及。 |
| is_validated | Boolean | 参数解释： 是否验证连接通过。 取值范围： 不涉及。 |
| time_zone | String | 参数解释： 时区信息，值为国际标准时区。 取值范围： 不涉及。 |

**表5 Snmp3Notification**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| ip | String | 参数解释： 已安装SNMP服务器的IP地址。 取值范围： 不涉及。 |
| port | String | 参数解释： 已安装的SNMP服务器的端口号。 取值范围： 不涉及。 |
| charset | String | 参数解释： 字符集，已配置支持HTTPS发送的第三方平台服务器支持的字符集。 取值范围： UTF-8或GBK |
| notification_id | String | 参数解释： 告警对接对应ID。 取值范围： 不涉及。 |
| is_enabled | Boolean | 参数解释： 是否启用。 取值范围： 不涉及。 |
| alarm_template | String | 参数解释： 告警模板，上报告警时使用的模板。 取值范围： 不涉及。 |
| recovery_template | String | 参数解释： 恢复模板，告警恢复时使用的模板。 取值范围： 不涉及。 |
| ignore_duplicate_in_duration | Integer | 参数解释： 相同的告警，在停发间隔内再次上报，会被忽略，不会发出告警通知。 取值范围： 不涉及。 |
| alarm_level | Array of strings | 参数解释： 告警级别，满足勾选级别的告警才会被发出。 取值范围： 不涉及。 |
| is_validated | Boolean | 参数解释： 是否验证连接通过。 取值范围： 不涉及。 |
| time_zone | String | 参数解释： 时区信息，值为国际标准时区。 取值范围： 不涉及。 |

**表6 Snmp2Conf**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| ip | String | 参数解释： SNMPv2接收端地址。 取值范围： 不涉及。 |
| port | String | 参数解释： SNMPv2接收端地址对应端口。 取值范围： 不涉及。 |
| charset | String | 参数解释： 字符集，已配置支持HTTPS发送的第三方平台服务器支持的字符集。 取值范围： UTF-8或GBK |
| notification_id | String | 参数解释： 告警通知ID。 取值范围： 不涉及。 |
| is_enabled | Boolean | 参数解释： 告警通知是否开启。 取值范围： 不涉及。 |
| alarm_template | String | 参数解释： 告警模板，上报告警时使用的模板。 取值范围： 不涉及。 |
| recovery_template | String | 参数解释： 恢复模板，告警恢复时使用的模板。 取值范围： 不涉及。 |
| ignore_duplicate_in_duration | String | 参数解释： 相同的告警，在停发间隔内再次上报，会被忽略，不会发出告警通知。 取值范围： 不涉及。 |
| alarm_level | Array of strings | 参数解释： 允许通知的告警级别， 取值范围： ；notice：提示 ；minor：次要 ；major：重要 ；critical：紧急 |
| is_validated | Boolean | 参数解释： 是否验证连接通过。 取值范围： 不涉及。 |
| community_name | String | 参数解释： snmpv2团体名。 取值范围： 不涉及。 |

**表7 SyslogNotification**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| ip | String | 参数解释： 该参数用于指定对端的Syslog服务器IP地址。 取值范围： 不涉及。 |
| port | String | 参数解释： Syslog服务器端口号。 取值范围： 1~65535。 |
| charset | String | 参数解释： 字符集，已配置支持HTTPS发送的第三方平台服务器支持的字符集。 取值范围： UTF-8或GBK |
| protocol | String | 参数解释： 通过TCP协议或UDP协议发送。 取值范围： TCP或UDP。 |
| cert_id | String | 参数解释： SSL证书ID，启用SSL安全协议需要选择SSL证书用于认证对接告警的Syslog服务器。SSL证书可在“平台管理 > SSL证书管理”页面上传。 取值范围： 不涉及。 |
| ssl_enabled | Boolean | 参数解释： 是否启用SSL。 取值范围： 不涉及。 |
| alarm_count | Integer | 参数解释： 推送告警的数量。 取值范围： 1到100。 |
| notification_id | String | 参数解释： 告警对接对应ID。 取值范围： 不涉及。 |
| is_enabled | Boolean | 参数解释： 是否启用。 取值范围： 不涉及。 |
| alarm_template | String | 参数解释： 告警模板，上报告警时使用的模板。 取值范围： 不涉及。 |
| recovery_template | String | 参数解释： 恢复模板，告警恢复时使用的模板。 取值范围： 不涉及。 |
| ignore_duplicate_in_duration | Integer | 参数解释： 同一告警停发间隔，相同的告警，在停发间隔内再次上报，会被忽略，不会发出告警通知。 取值范围： 不涉及。 |
| alarm_level | Array of strings | 参数解释： 告警级别，满足勾选级别的告警才会被发出。 取值范围： 不涉及。 |
| is_validated | Boolean | 参数解释： 是否验证连接通过。 取值范围： 不涉及。 |
| time_zone | String | 参数解释： 时区信息，值为国际标准时区。 取值范围： 不涉及。 |


### 请求示例

```
GET https://{Endpoint}/rds/v3/instances/{instance_id}/alarm/notifications
```


### 响应示例

状态码： 200

正常

```
{
  "syslog" : [ ],
  "snmp2" : [ ],
  "snmp3" : [ ],
  "http" : [ {
    "header" : "{\"Content-Type\":\"application/json\"}",
    "body" : "{\"buzId\":\"GaussDBAlarm\",\"messageId\":\"$messageId\",\"params\":\"$alarmText\",\"templateId\":\"tmp001\",\"mobile\":\"13722221111\"}",
    "notification_id" : "6e431181-731b-45e9-8b67-1cac34448393",
    "is_enabled" : true,
    "charset":"UTF-8",
    "alarm_template" : "DefaultText",
    "recovery_template" : "DefaultText",
    "ignore_duplicate_in_duration" : 0,
    "alarm_level" : [ "critical", "major", "minor", "notice" ],
    "is_validated" : false,
    "proxy" : "http://10.*.*.*:9528",
    "url_template" : "/{}",
    "request_method" : "POST",
    "groovy_enabled" : false,
    "groovy_script" : "import java.time.LocalDateTime; import java.time.format.DateTimeFormatter; list.add(body); String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern(\"yyyyMMddHHmmss\")); Random random = new Random(); StringBuilder rds = new StringBuilder();for(int i = 0; i<4;i++){rds.append(random.nextInt(10));}\nString messageId = date + rds.toString(); body.put(\"messageId\",messageId);",
    "ssl_enabled" : false,
    "response_template" : "{\"retCode\":0}",
    "time_zone":"Asia/Shanghai"
  } ],
  "instance_id" : "4fee4b5e8cef49f6b4bf90cff8868a7bin14"
}
```


### 状态码

| 状态码 | 描述 |
| --- | --- |
| 200 | 正常 |


### 错误码

请参见错误码。


---

## 告警结果列表


### 功能介绍

告警结果查询。


### URI

POST /v3/alarm/records


### 请求参数

**表1 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Mini-Token | 是 | String | 参数解释： 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| X-Language | 否 | String | 参数解释： 语言。 约束限制： 不涉及。 取值范围： ；zh-cn ；en-us 默认取值： en-us |

**表2 请求Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| is_active | 是 | Boolean | 参数解释： 告警激活状态。 约束限制： 不涉及。 取值范围： ；true：查询激活状态告警。 ；false：查询历史已恢复/清理的告警。 默认取值： 不涉及。 |
| is_convergence | 是 | Boolean | 参数解释： 是否对告警进行分组。 约束限制： 不涉及。 取值范围： ；true：查看告警列表。 ；false：查看告警详情。 默认取值： 不涉及。 |
| offset | 否 | Integer | 参数解释： 索引位置，偏移量。从第一条数据偏移offset条数据后开始查询，默认为0（偏移0条数据，表示从第一条数据开始查询）。例如：该参数指定为1，limit指定为10，则只展示第11-20条数据。 约束限制： 必须为数字，不能为负数。 取值范围： 不涉及。 默认取值： 0 |
| limit | 否 | Integer | 参数解释： 查询记录数。例如该参数设定为10，则查询结果最多只显示10条记录。 约束限制： 不涉及。 取值范围： 最小值为1，最大值为100。 默认取值： 100 |
| alarm_id | 否 | String | 参数解释： 告警ID。 约束限制： 当is_convergence参数为false时，同时传该参数和instance_id参数来按照告警ID和告警实例ID信息进行查询。 取值范围： 不涉及。 默认取值： 不涉及。 |
| instance_id | 否 | String | 参数解释： 告警实例ID。 约束限制： 当is_convergence参数为false时，同时传该参数和alarm_id参数来按照告警ID和告警实例ID信息进行查询。 取值范围： 不涉及。 默认取值： 不涉及。 |


### 响应参数

状态码： 200

**表3 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| records | Array of 表4 objects | 参数解释： 告警结果列表。 取值范围： 不涉及。 |
| total_count | Integer | 参数解释： 总记录数。 取值范围： 不涉及。 |

**表4 AlarmRecord**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| id | String | 参数解释： 告警信息序列ID，当is_convergence参数为false时，该字段生效。 取值范围： 不涉及。 |
| severity | String | 参数解释： 告警级别， 取值范围： ；notice：提示。 ；minor：次要。 ；major：重要。 ；critical：紧急。 |
| description | String | 参数解释： 告警描述信息，当is_convergence参数为false时，该字段生效。 取值范围： 不涉及。 |
| status | String | 参数解释： 告警状态。 取值范围： ；activated：激活。 ；cleared：已清理。 ；recovered：已恢复。 |
| alarm_id | String | 参数解释： 告警ID。 取值范围： 不涉及。 |
| alarm_name | String | 参数解释： 告警名称。 取值范围： 不涉及。 |
| alarm_remarks | String | 参数解释： 告警定义，当is_convergence参数为false时，该字段生效。 取值范围： 不涉及。 |
| alarm_category | String | 参数解释： 告警来源类型。 取值范围： 不涉及。 |
| instance_id | String | 参数解释： 告警实例ID，当alarm_type参数为metric或者event时该字段生效。 取值范围： 不涉及。 |
| instance_name | String | 参数解释： 告警实例名称，当alarm_type参数为metric或者event时该字段生效。 取值范围： 不涉及。 |
| host_ip | String | 参数解释： 告警节点IP，当is_convergence参数为false时，该字段生效。 取值范围： 不涉及。 |
| alarm_detail_raise_count | Integer | 参数解释： 告警累计触发次数，当is_convergence参数为false时该字段生效。 取值范围： 不涉及。 |
| alarm_items_sum | Integer | 参数解释： 相同实例下、相同告警ID的告警总数，当is_convergence参数为true时该字段生效。 取值范围： 不涉及。 |
| alarm_type | String | 参数解释： 告警类型， 取值范围： ；metric：指标告警。 ；event：事件告警。 ；system-metric：系统指标告警。 ；system-event：系统事件告警。 ；system：系统告警。 |
| first_alarm_time | Object | 参数解释： 首次告警触发时间。 取值范围： 不涉及。 |
| last_alarm_time | Object | 参数解释： 最新告警触发时间。 取值范围： 不涉及。 |
| alarm_duration | String | 参数解释： 告警持续时间。 取值范围： 不涉及。 |
| clear_alarm_time | Object | 参数解释： 告警恢复时间，当is_active参数为false时该字段失效。 取值范围： 不涉及。 |
| component_name | String | 参数解释： 告警组件类型。 取值范围： 不涉及。 |
| impaction | String | 参数解释： 告警影响，当is_convergence参数为false时该字段生效。 取值范围： 不涉及。 |
| possible_causes | String | 参数解释： 告警触发可能原因，当is_convergence参数为false时该字段生效。 取值范围： 不涉及。 |
| processing_steps | String | 参数解释： 告警修复建议，当is_convergence参数为false时该字段生效。 取值范围： 不涉及。 |
| notify_status | String | 参数解释： 告警通知状态，当is_convergence参数为false时，该字段生效。 取值范围： 不涉及。 |
| automatic_clear | String | 参数解释： 告警是否自动恢复，当is_convergence参数为false时该字段生效。 取值范围： 不涉及。 |
| node_id | String | 参数解释： 告警节点ID，当is_convergence参数为false时该字段生效。当告警为系统告警时，该字段为发生告警的管理面节点的IP。 取值范围： 不涉及。 |
| resource_id | String | 参数解释： 告警内部标识ID，当is_convergence参数为false时，该字段生效。 取值范围： 不涉及。 |

状态码： 400

**表5 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| errCode | String | 参数解释： 错误码。 取值范围： 不涉及。 |
| externalMessage | String | 参数解释： 错误描述。 取值范围： 不涉及。 |


### 请求示例

```
POST https://{Endpoint}/rds/v3/alarm/records
 
{ 
  "is_active" : true, 
  "is_convergence" : true, 
  "limit" : 10, 
  "offset" : 0 
}
```


### 响应示例

状态码： 200

API操作成功

```
{ 
  "records" : [ { 
"severity": "critical",
            "status": "activated",
            "alarm_id": "5023503",
            "alarm_name": "自动全量备份",
            "alarm_category": "备份管理",
            "instance_id": "dc44e2653a2c47149cfb3109692c3590in14",
            "instance_name": "ha-3-lxy",
            "alarm_items_sum": 1,
            "alarm_type": "event",
            "first_alarm_time": 1698079853000,
            "last_alarm_time": 1698079853000,
            "alarm_duration": "9小时26分钟",
            "component_name": "自动全量备份"
} ], 
  "total_count" : 1 
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

## 告警数量查询


### 功能介绍

告警数量查询。


### URI

GET /v3/alarm/records/statistic


### 请求参数

**表1 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Mini-Token | 是 | String | 参数解释： 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| X-Language | 否 | String | 参数解释： 语言。 约束限制： 不涉及。 取值范围： ；zh-cn ；en-us 默认取值： en-us |


### 响应参数

状态码： 200

**表2 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| system_severity_statistics | Array of objects | 参数解释： 系统告警各级别数量统计。 数组长度：4 - 4 取值范围： 不涉及。 |
| instance_severity_statistics | Array of objects | 参数解释： 实例告警各级别数量统计。 数组长度：4 - 4 取值范围： 不涉及。 |

状态码： 400

**表3 响应Body参数**

| 参数 | 参数类型 | 描述 |
| --- | --- | --- |
| errCode | String | 参数解释： 错误码。 取值范围： 不涉及。 |
| externalMessage | String | 参数解释： 错误描述。 取值范围： 不涉及。 |


### 请求示例

无


### 响应示例

```
{
    "system_severity_statistics": [
        {
            "severity": "notice",
            "count": 1
        },
        {
            "severity": "minor",
            "count": 3
        },
        {
            "severity": "major",
            "count": 80
        },
        {
            "severity": "critical",
            "count": 0
        }
    ],
    "instance_severity_statistics": [
        {
            "severity": "notice",
            "count": 151
        },
        {
            "severity": "minor",
            "count": 97
        },
        {
            "severity": "major",
            "count": 904
        },
        {
            "severity": "critical",
            "count": 966
        }
    ]
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

## 实例告警批量配置


### 功能介绍


### URI

POST /v3/alarm/update/configs


### 请求参数

**表1 请求Header参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| X-Mini-Token | 是 | String | 参数解释： 用户Token。 通过调用生成Token接口获取。 请求响应成功后在响应消息头中包含的“X-Mini-Token”的值即为Token值。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |
| X-Language | 否 | String | 参数解释： 语言。 约束限制： 不涉及。 取值范围： ；zh-cn ；en-us 默认取值： en-us |

**表2 请求Body参数**

| 参数 | 是否必选 | 参数类型 | 描述 |
| --- | --- | --- | --- |
| source_id | 是 | String | 参数解释： 已配置实例的ID。 约束限制： 调用接口前，需要配置一套实例的告警策略，然后才能将配置的策略应用到目标实例。 取值范围： 不涉及。 默认取值： 不涉及。 |
| target_instance_ids | 是 | Array of strings | 参数解释： 需要配置的实例ID列表，实例数量上限为1000个。 约束限制： 不涉及。 取值范围： 不涉及。 默认取值： 不涉及。 |


### 响应参数

无


### 请求示例

```
POST https://{Endpoint}/rds/v3/alarm/update/configs
 
{ 
  "source_id" : "6970e9cf7adc41a78e0f1f50215c9d3bin14", 
  "target_instance_ids" : [ "4e60721144874b67b3fc7c8f7acfc887in14" ] 
}
```


### 响应示例

无


### 状态码

| 状态码 | 描述 |
| --- | --- |
| 200 | 正常 |


### 错误码

请参见错误码。
