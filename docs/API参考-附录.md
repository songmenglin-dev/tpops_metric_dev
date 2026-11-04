# 附录


---

## 异常请求结果

**表1 异常参数说明**

| 参数名 | 参数类型 | 参数说明 |
| --- | --- | --- |
| error_code | String | 提交任务异常时返回的错误编码。 |
| error_msg | String | 提交任务异常时返回的错误描述信息。 |

响应示例：

```
{
    "error_msg": "***",
    "error_code": "***"
}
```


---

## 状态码

**表1 状态码**

| 状态码 | 编码 | 错误码说明 |
| --- | --- | --- |
| 100 | Continue | 继续请求。 这个临时响应用来通知客户端，它的部分请求已经被服务器接收，且仍未被拒绝。 |
| 101 | Switching Protocols | 切换协议。只能切换到更高级的协议。 例如，切换到HTTP的新版本协议。 |
| 200 | OK | 请求成功。 |
| 201 | Created | 创建类的请求完全成功。 |
| 202 | Accepted | 已经接受请求，但未处理完成。 |
| 203 | Non-Authoritative Information | 非授权信息，请求成功。 |
| 204 | NoContent | 请求完全成功，同时HTTP响应不包含响应体。 在响应OPTIONS方法的HTTP请求时返回此状态码。 |
| 205 | Reset Content | 重置内容，服务器处理成功。 |
| 206 | Partial Content | 服务器成功处理了部分GET请求。 |
| 300 | Multiple Choices | 多种选择。请求的资源可包括多个位置，相应可返回一个资源特征与地址的列表用于用户终端（例如：浏览器）选择。 |
| 301 | Moved Permanently | 永久移动，请求的资源已被永久的移动到新的URI，返回信息会包括新的URI。 |
| 302 | Found | 资源被临时移动。 |
| 303 | See Other | 查看其他地址。 使用GET和POST请求查看。 |
| 304 | Not Modified | 所请求的资源未修改，服务器返回此状态码时，不会返回任何资源。 |
| 305 | Use Proxy | 所请求的资源必须通过代理访问。 |
| 306 | Unused | 已经被废弃的HTTP状态码。 |
| 400 | BadRequest | 非法请求。 建议直接修改该请求，不要重试该请求。 |
| 401 | Unauthorized | 在客户端提供认证信息后，返回该状态码，表明服务端指出客户端所提供的认证信息不正确或非法。 |
| 402 | Payment Required | 保留请求。 |
| 403 | Forbidden | 请求被拒绝访问。 返回该状态码，表明请求能够到达服务端，且服务端能够理解用户请求，但是拒绝做更多的事情，因为该请求被设置为拒绝访问，建议直接修改该请求，不要重试该请求。 |
| 404 | NotFound | 所请求的资源不存在。 建议直接修改该请求，不要重试该请求。 |
| 405 | MethodNotAllowed | 请求中带有该资源不支持的方法。 建议直接修改该请求，不要重试该请求。 |
| 406 | Not Acceptable | 服务器无法根据客户端请求的内容特性完成请求。 |
| 407 | Proxy Authentication Required | 请求要求代理的身份认证，与401类似，但请求者应当使用代理进行授权。 |
| 408 | Request Time-out | 服务器等候请求时发生超时。 客户端可以随时再次提交该请求而无需进行任何更改。 |
| 409 | Conflict | 服务器在完成请求时发生冲突。 返回该状态码，表明客户端尝试创建的资源已经存在，或者由于冲突请求的更新操作不能被完成。 |
| 410 | Gone | 客户端请求的资源已经不存在。 返回该状态码，表明请求的资源已被永久删除。 |
| 411 | Length Required | 服务器无法处理客户端发送的不带Content-Length的请求信息。 |
| 412 | Precondition Failed | 未满足前提条件，服务器未满足请求者在请求中设置的其中一个前提条件。 |
| 413 | Request Entity Too Large | 由于请求的实体过大，服务器无法处理，因此拒绝请求。为防止客户端的连续请求，服务器可能会关闭连接。如果只是服务器暂时无法处理，则会包含一个Retry-After的响应信息。 |
| 414 | Request-URI Too Large | 请求的URI过长（URI通常为网址），服务器无法处理。 |
| 415 | Unsupported Media Type | 服务器无法处理请求附带的媒体格式。 |
| 416 | Requested range not satisfiable | 客户端请求的范围无效。 |
| 417 | Expectation Failed | 服务器无法满足Expect的请求头信息。 |
| 422 | UnprocessableEntity | 请求格式正确，但是由于含有语义错误，无法响应。 |
| 429 | TooManyRequests | 表明请求超出了客户端访问频率的限制或者服务端接收到多于它能处理的请求。建议客户端读取相应的Retry-After首部，然后等待该首部指出的时间后再重试。 |
| 500 | InternalServerError | 表明服务端能被请求访问到，但是不能理解用户的请求。 |
| 501 | Not Implemented | 服务器不支持请求的功能，无法完成请求。 |
| 502 | Bad Gateway | 充当网关或代理的服务器，从远端服务器接收到了一个无效的请求。 |
| 503 | ServiceUnavailable | 被请求的服务无效。 建议直接修改该请求，不要重试该请求。 |
| 504 | ServerTimeout | 请求在给定的时间内无法完成。客户端仅在为请求指定超时（Timeout）参数时会得到该响应。 |
| 505 | HTTP Version not supported | 服务器不支持请求的HTTP协议的版本，无法完成处理。 |


---

## 错误码

**表1 错误码说明**

| 状态码 | 错误码 | 中文说明 |
| --- | --- | --- |
| 400 | DBS.00100007 | 告警查询参数错误。 |
| 400 | DBS.00101005 | 告警对接验证失败。 |
| 400 | DBS.00110006 | 无效的访问。 |
| 400 | DBS.00110007 | 没有执行此操作的权限。 |
| 400 | DBS.05000008 | 当前实例源版本与目标版本相同，不需要升级。 |
| 400 | DBS.06010013 | 参数错误：错误原因。 |
| 400 | DBS.06013001 | wdr快照采集类型非法。 |
| 400 | DBS.06013002 | 灾备实例不支持该操作。 |
| 400 | DBS.06013003 | wdr快照采集失败。 |
| 400 | DBS.06013004 | wdr快照开关处于关闭状态。 |
| 400 | DBS.06015011 | 实例类型不一致，无法恢复。 |
| 400 | DBS.06015013 | 副本一致性协议不一致，无法恢复。 |
| 400 | DBS.06015021 | 备份介质不支持。 |
| 400 | DBS.06025010 | 所选主机数量有误。 |
| 400 | DBS.06025011 | 所选主机操作系统不一致。 |
| 400 | DBS.06025012 | 所选主机类型不一致。 |
| 400 | DBS.06025013 | 所选主机的异构属性不一致。 |
| 400 | DBS.06025014 | 所选主机的CPU厂商不一致。 |
| 400 | DBS.06025015 | 所选主机已被占用。 |
| 400 | DBS.06025016 | 当前实例Agent源版本和目标版本一致，无需升级。 |
| 400 | DBS.06025017 | Agent升级失败，请检查实例和节点状态后重试。 |
| 400 | DBS.06025024 | 所选主机或者机房名字长度不合法。 |
| 400 | DBS.06025025 | 所选主机或者机房描述长度不合法。 |
| 400 | DBS.06025026 | 所选主机SSH端口号不合法。 |
| 400 | DBS.06025027 | 批量添加/修改/删除主机存在重复。 |
| 400 | DBS.06025028 | 所选主机不支持修改。 |
| 400 | DBS.06025029 | 所选主机不支持删除。 |
| 400 | DBS.06025030 | 所选机房下存在主机。 |
| 400 | DBS.06025031 | 所选主机不存在。 |
| 400 | DBS.06025043 | 网卡映射失败。 |
| 400 | DBS.06025044 | 主机上线异常。 |
| 400 | DBS.06025046 | 所选主机磁盘类型不一致。 |
| 400 | DBS.06025047 | 该主机已在其他平台上线或Agent通信失败。 |
| 400 | DBS.06025048 | 下载OS PATCH安装包失败。 |
| 400 | DBS.06025056 | 闪回失败，失败原因：原因。 |
| 400 | DBS.06025058 | 所选机房不存在。 |
| 400 | DBS.06025059 | 所选机房已存在。 |
| 400 | DBS.06025060 | 所选机房不支持修改。 |
| 400 | DBS.06025061 | 主机标准化检查未通过。 |
| 400 | DBS.06025062 | 主机网络连接失败。 |
| 400 | DBS.06025063 | 主机SSH连接失败，检查密码或sshd配置。 |
| 400 | DBS.06025064 | 主机安装Agent失败。 |
| 400 | DBS.06025067 | 请检查安装实例的azName是否和机房名称相同。 |
| 400 | DBS.06025088 | 当前密钥名称已存在。 |
| 400 | DBS.06025089 | 当前密钥算法暂不支持。 |
| 400 | DBS.06025090 | 当前密钥名称长度不合法。 |
| 400 | DBS.06025091 | 当前密钥描述长度不合法。 |
| 400 | DBS.06025092 | 当前仅支持创建20对密钥对。 |
| 400 | DBS.06025093 | SSH私钥不存在，请检查ID。 |
| 400 | DBS.06025096 | 上下线主机缺乏凭证，请选择密码或者SSH私钥进行操作。 |
| 400 | DBS.06025097 | SSH私钥有绑定主机，请确认主机已不再使用，勾选强制删除进行删除。 |
| 400 | DBS.06025101 | OS PATCH包不存在。 |
| 400 | DBS.06025119 | 容器里没有存活的Agent进程。 |
| 400 | DBS.06025120 | 清理Agent信息失败。 |
| 400 | DBS.06025127 | 批量添加机房存在重复。 |
| 400 | DBS.06025132 | 所选机房数量有误。 |
| 400 | DBS.06025187 | 修改主机静态信息失败。 |
| 400 | DBS.06025188 | 主机网络协议不支持使用IPv6。 |
| 400 | DBS.06025192 | 主机为沙箱模式，不支持三网络平面。 |
| 400 | DBS.06025193 | 主机安装模式不支持。 |
| 400 | DBS.06025194 | 两网络平面的主机管理IP和数据IP不一致。 |
| 400 | DBS.06280008 | SCHEMA已存在。 |
| 400 | DBS.06280028 | 智能运维实例可纳管数量达到最大限制。 |
| 400 | DBS.200001 | 参数错误。 |
| 400 | DBS.200002 | 实例不存在。 |
| 400 | DBS.200006 | 请求体为空。 |
| 409 | DBS.200019 | 该实例正在进行其他操作或该实例故障，无法执行该操作，请稍后重试。 |
| 400 | DBS.200021 | 实例名非法。 |
| 400 | DBS.200025 | 可用区取值非法。 |
| 400 | DBS.200029 | 用户名或密码错误。 |
| 400 | DBS.200044 | 访问资源不存在，或者无访问权限。 |
| 400 | DBS.200052 | 数据库root密码非法。 |
| 400 | DBS.200062 | 用户名不符合规则或是为数据库管理账号。 |
| 400 | DBS.200068 | 这是一个弱密码，请输入一个强度大一些的密码。 |
| 400 | DBS.200079 | 当前实例异常，无法主备倒换。 |
| 400 | DBS.200475 | 新密码不能与旧密码相同。 |
| 400 | DBS.200476 | 新密码不能与旧密码倒序相同。 |
| 400 | DBS.200480 | 备份时实例的节点拓扑结构与当前实例不一致。 |
| 400 | DBS.200602 | 该实例不存在。 |
| 400 | DBS.200751 | 备份文件不存在。 |
| 400 | DBS.200818 | 删除数据库用户失败。 |
| 400 | DBS.200821 | 修改数据库用户权限失败。 |
| 400 | DBS.200823 | 数据库不存在。 |
| 400 | DBS.200824 | 数据库账户不存在。 |
| 400 | DBS.200856 | 数据库引擎版本不支持。 |
| 400 | DBS.201003 | 访问资源不存在，或者无访问权限。 |
| 400 | DBS.201004 | 备份类型不存在。 |
| 400 | DBS.201010 | 备份文件不存在。 |
| 400 | DBS.201015 | 该实例正在进行其他操作，无法执行该操作，请稍后重试。 |
| 400 | DBS.201016 | 实例节点状态不允许操作。 |
| 400 | DBS.201019 | 恢复任务不存在。 |
| 400 | DBS.201032 | 恢复时间点不在备份范围。 |
| 400 | DBS.201106 | 保留天数非法。 |
| 400 | DBS.201109 | 备份流控非法。 |
| 400 | DBS.201110 | 差量预取页面个数非法。 |
| 400 | DBS.201111 | 分片大小设置必须是在范围内且为4的倍数。 |
| 409 | DBS.201202 | 该实例正在进行其他操作或该实例故障，无法执行该操作，请稍后重试。 |
| 400 | DBS.201203 | 备份文件不存在。 |
| 400 | DBS.201204 | 不允许执行该类型备份。 |
| 400 | DBS.201209 | 备份名称已存在。 |
| 400 | DBS.201210 | 备份名非法。 |
| 400 | DBS.201218 | 不是手动备份文件。 |
| 400 | DBS.216009 | 当前实例状态不允许操作。 |
| 400 | DBS.216011 | 实例节点状态不允许操作。 |
| 400 | DBS.216016 | 该实例不存在。 |
| 400 | DBS.216026 | 节点不存在。 |
| 400 | DBS.216030 | 操作节点不属于该实例。 |
| 400 | DBS.280001 | 参数错误。 |
| 500 | DBS.280005 | 服务器错误。 |
| 400 | DBS.280011 | 实例状态不正常。 |
| 400 | DBS.280012 | 当前实例正处于其他操作中，无法升级。 |
| 400 | DBS.280110 | 所选实例不存在。 |
| 400 | DBS.280124 | 备份文件ID非法。 |
| 400 | DBS.280125 | 非法的备份策略。 |
| 400 | DBS.280215 | 备份周期非法。 |
| 400 | DBS.280216 | 备份开始时间非法。 |
| 400 | DBS.280236 | 数据库引擎版本非法。 |
| 400 | DBS.280251 | 备份周期不合法。 |
| 400 | DBS.280404 | 实例ID不合法。 |
| 400 | DBS.280416 | 备份结束时间非法。 |
| 400 | DBS.280439 | 无效的查询记录数，请输入小于等于100的正整数。 |
| 400 | DBS.280440 | 无效的偏移量，请输入非负整数。 |
| 400 | DBS.280472 | 无效的查询时间。 |
| 400 | DBS.280602 | HA一致性非法。 |
| 400 | DBS.280618 | 差量备份周期无效。 |
| 400 | DBS.280631 | 数据库名非法。 |
| 400 | DBS.280637 | 差量预取页面个数非法。 |
| 400 | DBS.280657 | 非法的升级参数，请检查升级类型和升级方式。 |
| 400 | DBS.280658 | 非法的灰度升级可用区取值，请检查传参。 |
| 400 | DBS.280659 | 非法的灰度升级分片数，请检查传参。 |
| 400 | DBS.280660 | 实例灰度升级校验失败，请检查实例状态。 |
| 400 | DBS.280670 | 查询日期时间超出允许范围。 ；检查日期时间范围是否合法。 ；检查日期时间范围是否超过业务允许范围。 |
| 400 | DBS.301147 | 启动节点不满足多数派。 |
| 400 | DBS.6013005 | 正常AZ数量大于1。 |
| 400 | DBS.6013035 | Patch名称重复。 |
| 400 | DBS.6013036 | 无效的patch名称。 |
| 400 | DBS.6013037 | 无效的hint。 |
| 400 | DBS.6013038 | SQL patch 错误。 |
| 400 | DBS.6013063 | 数据库表空间不存在。 |
| 400 | DBS.6025033 | AZ故障操作类型错误。 |
| 400 | DBS.6025117 | 数据库用户密码非法。 |
| 400 | DBS.6025177 | 实例必须绑定OceanStor Dorado设备。 |
| 400 | DBS.280801 | 容灾类型非法。 |
| 400 | DBS.06013088 | 当前实例无templatem模板数据库，且副本一致性协议是Paxos，暂不支持此操作。 |
| 400 | DBS.00120000 | 参数格式不正确，请检查输入参数格式。 |
| 400 | DBS.06025057 | 该实例:${instanceId}，不支持闪回，请检查该实例是否存在或者为灾备实例 |
| 400 | DBS.06030135 | WDR快照不存在，请检查。快照ID=${snapshotId}。 |
| 400 | DBS.06030141 | SQL执行失败，失败原因：${errorReason}。 |
| 400 | DBS.06030047 | 实例引擎类型不匹配，非目标引擎类型 所选实例类型不支持当前操作。一般来说，仅支持GaussDB数据实例，不支持其他类似实例，请检查实例类型。 |
| 500 | DBS.06030079 | 下发命令操作失败，请检查后台日志。 |
| 500 | DBS.06030080 | 执行失败，请检查Agent后台日志以进一步深入分析，错误信息：${errorMsg}。 |
| 400 | DBS.06030081 | 校验失败，可能参数非法或不满足场景要求。更多定位信息：-校验项：${paraName}，-错误描述：${errorMsg}。 |
| 500 | DBS.06030092 | 导出数据文件流失败。文件信息=${fileName}，定位信息：${errorMsg}。 ；请检查文件是否存在或状态是否支持下载。 ；对于ASP报告下载，请检查任务ID是否合法，并进一步检查对应任务是否成功完成。 |
| 400 | DBS.06020162 | 不支持此容灾操作。 |
| 400 | DBS.06015069 | 实例组不支持该操作。 |
| 400 | DBS.00110005 | 登录已过期。 |
| 400 | DBS.06025217 | 自纳管元库不允许操作。 |
| 400 | DBS.201506 | 访问Agent失败。 |
| 400 | DBS.06025228 | 接口请求次数已上限，请稍后重试。 |
| 500 | DBS.00102011 | 相关服务调用失败，请参考错误码参考指南处理。 |
| 400 | DBS.212045 | 参数值超出范围。 |
| 400 | DBS.200059 | 数据库端口号无效。 |
| 400 | DBS.200461 | 参数值超出范围。 |
| 400 | DBS.200463 | 该实例正在进行其他操作或该实例故障，无法执行该操作，请稍后重试。 |
| 400 | DBS.280639 | 实例模式不支持 |
| 500 | DBS.06030142 | 导入统一命令配置文件失败，请查看管控后台相关日志。定位信息：${error_info}。 |
| 500 | DBS.06030143 | 统一命令配置文件格式非法。文件列表=${file_list}。检查要点：1）UTF8编码的JSON文件；2）文件名以DBS.开头，不超过256字符，可填字母、数字和下划线，不包含特殊字符。 |
| 500 | DBS.06030144 | 统一命令配置文件内容非法，请检查是否满足自定义命令配置要求，文件列表=${file_list}。 |
| 500 | DBS.06030145 | 统一命令配置文件中自定义命令数量非法，超过最大支持限制(${count_limit})。 |
| 500 | DBS.06030146 | 统一命令配置文件大小非法，超过最大支持限制(${size_limit})，文件列表=${file_list}。 |
| 500 | DBS.06030147 | 文件签名校验失败。 |
| 500 | DBS.06030148 | 统一命令不存在。命令ID=${cmd_id}，命令名称=${cmd_name} |
| 500 | DBS.06030149 | 统一命令不支持此操作。命令ID=${cmd_id}，命令名称=${cmd_name}" |
| 500 | DBS.06030150 | 统一命令配置文件及其签名文件需同时上传。 |
| 500 | DBS.06030151 | 统一命令中心操作冲突，请勿多人同时进行管理操作（如导入、编辑、禁用、激活、删除等操作)。 |
| 500 | DBS.06030152 | 统一命令已禁用，不支持此操作，请联系管理员进行激活。命令ID=${cmd_id}，命令名称=${cmd_name}" |
| 500 | DBS.06030153 | 统一命令不支持该实例类型(${cluster_mode})，请使用类型匹配的实例进行操作。命令ID=${cmd_id}，命令名称=${cmd_name}" |
| 500 | DBS.06030154 | 执行节点不满足统一命令约束及要求。定位信息：${error_msg}" |
| 500 | DBS.06030155 | 实例上统一命令执行任务不存在，任务ID：${job_id}" |
| 500 | DBS.06030156 | 节点上统一命令执行任务不存在，任务ID：${task_id}" |
| 500 | DBS.06030158 | 统一命令(ID=${cmd_id})被占用，无法进行此操作，占用业务列表：${business_list} |
| 500 | DBS.06030159 | 统一命令操作审计日志记录不存在，记录ID：${record_id} |
| 500 | DBS.06030160 | 统一命令(ID=${cmd_id})被占用，无法进行此操作。请先解除相关业务关联绑定。详情如下，智能巡检—巡检项ID：${inspect_item_ids}，指标管理—指标项ID：${metric_item_ids}，主机标准化检查-检查项ID：${host_check_item_ids} |
| 500 | DBS.06030161 | 创建统一命令失败，错误信息：${error_msg} |
| 500 | DBS.06030162 | 统一命令配置内容非法，错误信息：${error_msg} |
| 500 | DBS.06030164 | 统一命令已存在，不支持此操作：命令ID=${cmd_id}，命令名称=${cmd_name} |
| 400 | DBS.06025243 | 节点的Python版本与DBMind版本不配套。 |
| 400 | DBS.06025245 | 查询节点的PIP服务状态失败。 |
| 400 | DBS.06015101 | 一个库同一时间仅支持执行一个逻辑备份任务 |
| 400 | DBS.06013106 | ${params}参数需要同时修改，且值保持一致。 |
| 400 | DBS.00130113 | License配额更新冲突，请稍后重试。 |
| 400 | DBS.06025241 | 实例不支持订购类型切换。 |
| 400 | DBS.06025239 | 该设备已绑定。 |
| 400 | DBS.08000005 | 当前版本GaussDB-DBMind实例不支持升级。 |
| 400 | DBS.08000033 | 当前版本GaussDB-DBMind实例不支降级操作。 |
| 400 | DBS.06013107 | PDB数据库用户不存在。 |
| 400 | DBS.06013108 | 无法删除该数据库用户，存在依赖对象。 |
| 400 | DBS.06013109 | 无法删除该数据库用户，该用户为系统必需角色。 |
| 400 | DBS.06013110 | 权限不足，无法删除该数据库用户。 |
| 400 | DBS.06013111 | 删除PDB数据库用户失败。 |
| 400 | DBS.06014031 | 实例分组内暂无实例，请添加实例后重试。 |
| 400 | DBS.00110060 | 用户登录类型不匹配，拒绝登录。 |
| 400 | DBS.00110061 | 用户登录类型值无效。 |
| 400 | DBS.06015061 | 升级前必须创建租户。 |
| 400 | DBS.06010020 | 当前设备存在绑定的实例，不允许删除。 |
| 400 | DBS.06025262 | 非法的主机挂载路径：${param}. ${errorReasonCN} |
| 400 | DBS.06015088 | 租户密码非法。 |
| 400 | DBS.06010042 | 加密机配置不存在: ${configId}。 |
| 400 | DBS.06010043 | 加密机配置名称已存在：${configName}。 |
| 400 | DBS.06010044 | 加密机配置未验证，不支持${operation}。 |
| 400 | DBS.06010045 | 加密机配置已绑定实例，无法删除：${reason}。 |
| 400 | DBS.06010046 | 加密机配置验证失败。 |
| 400 | DBS.06010047 | ${fileName}文件格式不正确，不支持${expectedFormat}格式。 |
| 400 | DBS.06010049 | 文件上传失败。 |
| 400 | DBS.06010050 | 实例已开启透明加密，无法重复开启。 |
| 400 | DBS.06010051 | 实例未开启透明加密。 |
| 400 | DBS.06010053 | 参数冲突，${detail}。 |
| 400 | DBS.06010056 | 配置文件重新下发失败，${reason}。 |
| 400 | DBS.06015042 | 当前切换操作不支持。 |
| 400 | DBS.06280007 | SCHEMA不存在。 |
| 400 | DBS.06280009 | 表不存在。 |
| 400 | DBS.06280017 | 备份类别非法。 |
| 400 | DBS.06280018 | 新库名非法。 |
| 400 | DBS.06280019 | 新SCHEMA非法。 |
| 400 | DBS.200042 | 不支持的数据库类型或版本。 |
| 400 | DBS.200066 | 备份开始时间非法。 |
| 400 | DBS.201001 | 内部错误。 |
| 404 | DBS.201002 | 查询结果为空。 |
| 400 | DBS.201006 | 参数错误。 |
| 400 | DBS.216005 | 参数错误。 |
| 409 | DBS.216027 | 任务数超出限制。 |
| 400 | DBS.280006 | 请求为空，请输入请求参数。 |
| 400 | DBS.280214 | 保留天数非法。 |
| 400 | DBS.280636 | 备份流控非法。 |
| 400 | DBS.280669 | 分片大小设置必须是在范围内且为4的倍数。 |
| 400 | DBS.300112 | “ca_cert_pem”字段不能为空。 |
| 400 | DBS.300113 | “client_key”字段不能为空。 |
| 400 | DBS.300114 | “client_crt”字段不能为空。 |
| 400 | DBS.300115 | “rand_pass”字段不能为空。 |
| 400 | DBS.06280013 | 库表数量超过最大限制。 |


---

## 监控指标项

监控指标名称见下表：

| CPU/内存 组名：CPUMEMORY | CPU使用率 | sys001_cpu_usage（旧） cpu_percent（新） ；total_cpu_percent为所有用户的CPU使用率。 ；database_cpu_percent为数据库用户的CPU使用率。 |
| --- | --- | --- |
| CPU空闲时间占比 | sys002_idle_usage（旧） idle_percent（新） |  |
| CPU内核态时间占比 | cpu_sys_usage（旧） sys_percent（新） ；total_sys_percent为所有用户的CPU在内核态运行的时间占比。 ；database_sys_percent为数据库用户的CPU在内核态运行的时间占比。 |  |
| CPU用户态时间占比 | cpu_user_usage（旧） user_percent（新） ；total_user_percent为所有用户的CPU在用户态运行的时间占比。 ；database_user_percent为数据库用户的CPU在用户态运行的时间占比。 |  |
| CPU等待磁盘I/O时间占比 | cpu_wait_usage（旧） io_wait_percent（新） ；total_io_wait_percent为所有用户的CPU等待磁盘I/O的时间占比。 ；database_io_wait_percent为数据库用户的CPU等待磁盘I/O的时间占比。 |  |
| CPU 1min负载 | sys016_cpu_load |  |
| CPU 5min负载 |  |  |
| CPU 15min负载 |  |  |
| 内存已使用大小 | mem_used_size（旧） |  |
| 内存缓冲空间 | buffer_size（旧） |  |
| 内存缓存空间 | cache_size（旧） |  |
| 内存使用率 | sys010_mem_usage（旧） mem_usage（新） ；total_mem_usage为所有用户的内存使用率。 ；database_mem_usage为数据库用户的内存使用率。 |  |
| 内存可用大小 | sys013_mem_free_size（旧） |  |
| 内存空间大小（新） | mem_size（新） ；buffer_size为所有用户的内存缓冲空间的大小。 ；cache_size为所有用户的内存缓存空间的大小。 ；database_mem_used_size为数据库用户已使用的内存大小。 ；mem_free_size为所有用户未使用的内存大小。 ；total_mem_used_size为所有用户已使用的内存大小。 |  |
| 交换内存总大小 | sys024_swap_total_size |  |
| 交换内存使用率 | sys025_swap_used_ratio |  |
| 交换内存已使用大小 | sys026_swap_used_size |  |
| CPU核数 | cpu_core_count |  |
| 磁盘/存储 组名：IOSTORAGE | 信号量 | sys014_signal_num |
| Inode使用率 | sys023_file_handler_num |  |
| 系统磁盘使用率 | sys041_sys_disk_usage |  |
| 数据磁盘已使用大小 | sys063_disk_used_size |  |
| 数据磁盘总大小 | sys064_disk_total_size |  |
| 数据磁盘使用率 | sys065_disk_usage |  |
| 数据磁盘每秒读写次数 | sys069_iops |  |
| 数据磁盘写吞吐量 | sys071_disk_write_throughput |  |
| 数据磁盘读吞吐量 | sys072_disk_read_throughput |  |
| 数据磁盘单次写入花费的时间 | sys075_avg_disk_ms_per_write |  |
| 数据磁盘单次读取花费的时间 | sys076_avg_disk_ms_per_read |  |
| 数据磁盘平均服务耗时 | sys081_svctm |  |
| 泄露句柄数 | rds122_gaussv5_fd_leak_num |  |
| NAS使用率 | nas_usage |  |
| NVMe缓存使用率 | rds228_nvme_cache_usage |  |
| NVMe缓存使用量 | rds229_nvme_cache_used_size |  |
| 网络 组名：NETWORK | 网络接收速率 | sys011_bytes_in（旧） database_bytes_in（新） |
| 网络发送速率 | sys012_bytes_out（旧） database_bytes_out（新） |  |
| 错误数据包占接收数据包比例 | rx_errors（旧） database_rx_errors（新） |  |
| 丢弃数据包占接收数据包比例 | rx_dropped（旧） database_rx_dropped（新） |  |
| 错误数据包占发送数据包比例 | tx_errors（旧） database_tx_errors（新） |  |
| 丢弃数据包占发送数据包比例 | tx_dropped（旧） database_tx_dropped（新） |  |
| 连接 组名：CONNECTION | 用户登入次数/秒 | rds081_gaussv5_login_counter |
| 系统用户登入次数/秒 | rds081_gaussv5_login_counter_SYS |  |
| 业务用户登入次数/秒 | rds081_gaussv5_login_counter_USER |  |
| 用户登出次数/秒 | rds082_gaussv5_logout_counter |  |
| 系统用户登出次数/秒 | rds082_gaussv5_logout_counter_SYS |  |
| 业务用户登出次数/秒 | rds082_gaussv5_logout_counter_USER |  |
| 锁等待状态会话比率 | rds085_gaussv5_wait_ratio |  |
| 系统用户锁等待状态会话比率 | rds085_gaussv5_wait_ratio_SYS |  |
| 业务用户锁等待状态会话比率 | rds085_gaussv5_wait_ratio_USER |  |
| 活跃会话率 | rds086_gaussv5_active_ratio |  |
| 系统用户活跃会话率 | rds086_gaussv5_active_ratio_SYS |  |
| 业务用户活跃会话率 | rds086_gaussv5_active_ratio_USER |  |
| CN连接池活跃连接数 | rds089_gaussv5_inuse_counter |  |
| 活跃会话数量 | rds117_gaussv5_active_session |  |
| 系统用户活跃会话数量 | rds117_gaussv5_active_session_SYS |  |
| 业务用户活跃会话数量 | rds117_gaussv5_active_session_USER |  |
| 等待会话数量 | rds118_gaussv5_wait_session |  |
| 系统用户等待会话数量 | rds118_gaussv5_wait_session_SYS |  |
| 业务用户等待会话数量 | rds118_gaussv5_wait_session_USER |  |
| 在线会话数量 | rds143_gaussv5_online_session |  |
| 系统用户在线会话数 | rds143_gaussv5_online_session_SYS |  |
| 业务用户在线会话数 | rds143_gaussv5_online_session_USER |  |
| 在线会话率 | rds144_online_ratio |  |
| 系统用户在线会话率 | rds144_online_ratio_SYS |  |
| 业务用户在线会话率 | rds144_online_ratio_USER |  |
| 等锁会话数量 | rds195_gaussv5_locks_session |  |
| 系统用户等锁会话数量 | rds195_gaussv5_locks_session_SYS |  |
| 业务用户等锁会话数量 | rds195_gaussv5_locks_session_USER |  |
| 等锁会话率 | rds196_gaussv5_locks_ratio |  |
| 系统用户等锁会话率 | rds196_gaussv5_locks_ratio_SYS |  |
| 业务用户等锁会话率 | rds196_gaussv5_locks_ratio_USER |  |
| 历史最高活跃会话数 | rds416_gaussv5_max_active_session |  |
| 透明加密节点每分钟访问成功增量 | rds254_gaussv5_tde_connect_success |  |
| 透明加密节点每分钟访问失败增量 | rds255_gaussv5_tde_connect_failure |  |
| 业务负载 组名：TRANSACTION | 用户提交事务数/秒 | rds092_gaussv5_commit_counter |
| 系统用户提交事务数/秒 | rds092_gaussv5_commit_counter_SYS |  |
| 业务用户提交事务数/秒 | rds092_gaussv5_commit_counter_USER |  |
| 用户回滚事务数/秒 | rds093_gaussv5_rollback_counter |  |
| 系统用户回滚事务数/秒 | rds093_gaussv5_rollback_counter_SYS |  |
| 业务用户回滚事务数/秒 | rds093_gaussv5_rollback_counter_USER |  |
| 后台提交事务数/秒 | rds094_gaussv5_bg_commit_counter |  |
| 系统用户后台提交事务数/秒 | rds094_gaussv5_bg_commit_counter_SYS |  |
| 业务用户后台提交事务数/秒 | rds094_gaussv5_bg_commit_counter_USER |  |
| 后台回滚事务数/秒 | rds095_gaussv5_bg_rollback_counter |  |
| 系统用户后台回滚事务数/秒 | rds095_gaussv5_bg_rollback_counter_SYS |  |
| 业务用户后台回滚事务数/秒 | rds095_gaussv5_bg_rollback_counter_USER |  |
| 用户事务平均响应时间 | rds096_gaussv5_resp_avg |  |
| 系统用户事务平均响应时间 | rds096_gaussv5_resp_avg_SYS |  |
| 业务用户事务平均响应时间 | rds096_gaussv5_resp_avg_USER |  |
| 用户事务回滚率 | rds097_gaussv5_rollback_ratio |  |
| 系统用户事务回滚率 | rds097_gaussv5_rollback_ratio_SYS |  |
| 业务用户事务回滚率 | rds097_gaussv5_rollback_ratio_USER |  |
| 后台事务回滚率 | rds098_gaussv5_bg_rollback_ratio |  |
| 系统用户后台事务回滚率 | rds098_gaussv5_bg_rollback_ratio_SYS |  |
| 业务用户后台事务回滚率 | rds098_gaussv5_bg_rollback_ratio_USER |  |
| Data Definition Language/s | rds099_gaussv5_ddl_count |  |
| Data Definition Language/s(系统用户) | rds099_gaussv5_ddl_count_SYS |  |
| Data Definition Language/s(业务用户) | rds099_gaussv5_ddl_count_USER |  |
| Data Manipulation Language/s | rds100_gaussv5_dml_count |  |
| Data Manipulation Language/s(系统用户) | rds100_gaussv5_dml_count_SYS |  |
| Data Manipulation Language/s(业务用户) | rds100_gaussv5_dml_count_USER |  |
| Data Control Language/s | rds101_gaussv5_dcl_count |  |
| Data Control Language/s(系统用户) | rds101_gaussv5_dcl_count_SYS |  |
| Data Control Language/s(业务用户) | rds101_gaussv5_dcl_count_USER |  |
| DDL+DCL比率 | rds102_gaussv5_ddl_dcl_ratio |  |
| 80% SQL的响应时间 | rds103_gaussv5_P80 |  |
| 95%SQL的响应时间 | rds104_gaussv5_P95 |  |
| SELECT语句占比 | rds129_select_distribution |  |
| select分布(系统用户) | rds129_select_distribution_SYS |  |
| select分布(业务用户) | rds129_select_distribution_USER |  |
| UPDATE语句占比 | rds130_update_distribution |  |
| update分布(系统用户) | rds130_update_distribution_SYS |  |
| update分布(业务用户) | rds130_update_distribution_USER |  |
| INSERT语句占比 | rds131_insert_distribution |  |
| insert分布(系统用户)" | rds131_insert_distribution_SYS |  |
| insert分布(业务用户) | rds131_insert_distribution_USER |  |
| DELETE语句占比 | rds132_delete_distribution |  |
| delete分布(系统用户) | rds132_delete_distribution_SYS |  |
| delete分布(业务用户) | rds132_delete_distribution_USER |  |
| statement数量 | rds145_gaussdbv5_statement |  |
| statement数量(系统用户) | rds145_gaussdbv5_statement_SYS |  |
| statement数量(业务用户) | rds145_gaussdbv5_statement_USER |  |
| 系统库慢SQL数量(系统用户) | rds146_gaussdbv5_slowquery_sys_SYS |  |
| 系统库慢SQL数量(业务用户) | rds146_gaussdbv5_slowquery_sys_USER |  |
| 用户库慢SQL数量(系统用户) | rds147_gaussdbv5_slowquery_user_SYS |  |
| 用户库慢SQL数量(业务用户) | rds147_gaussdbv5_slowquery_user_USER |  |
| 数据库最长事务的执行时长 | rds190_long_running_transaction_exectime |  |
| 运行超过60分钟的事务数量 | rds311_long_transactions_num_60min |  |
| 运行超过120分钟的事务数量 | rds312_long_transactions_num_120min |  |
| 用户总事务数/秒（TPS） | rds400_gaussv5_total_counter |  |
| 用户总事务数/秒（系统用户） | rds400_gaussv5_total_counter_SYS |  |
| 用户总事务数/秒（业务用户） | rds400_gaussv5_total_counter_USER |  |
| 用户事务提交率 | rds401_gaussv5_commit_ratio |  |
| 平均SQL执行数（QPS） | rds402_gaussv5_ddl_dml_dcl_count |  |
| 平均SQL执行数（系统用户） | rds402_gaussv5_ddl_dml_dcl_count_SYS |  |
| 平均SQL执行数（业务用户） | rds402_gaussv5_ddl_dml_dcl_count_USER |  |
| INSERT语句数量/秒 | rds403_gaussv5_insert_sql_count |  |
| SELECT语句数量/秒 | rds404_gaussv5_select_sql_count |  |
| DELETE语句数量/秒 | rds405_gaussv5_delete_sql_count |  |
| UPDATE语句数量/秒 | rds406_gaussv5_update_sql_count |  |
| FQS执行计划占比 | rds250_gaussv5_fqs_ratio |  |
| 系统用户FQS执行计划占比 | rds250_gaussv5_fqs_ratio_SYS |  |
| 业务用户FQS执行计划占比 | rds250_gaussv5_fqs_ratio_USER |  |
| LightProxy执行计划占比 | rds251_gaussv5_lightproxy_ratio |  |
| 系统用户LightProxy执行计划占比 | rds251_gaussv5_lightproxy_ratio_SYS |  |
| 业务LightProxy执行计划占比 | rds251_gaussv5_lightproxy_ratio_USER |  |
| Stream流式执行计划占比 | rds252_gaussv5_stream_ratio |  |
| 系统用户Stream流式执行计划占比 | rds252_gaussv5_stream_ratio_SYS |  |
| 业务用户Stream流式执行计划占比 | rds252_gaussv5_stream_ratio_USER |  |
| PGXC分布式执行计划占比 | rds253_gaussv5_pgxc_ratio |  |
| 系统用户PGXC分布式执行计划占比 | rds253_gaussv5_pgxc_ratio_SYS |  |
| 业务用户PGXC分布式执行计划占比 | rds253_gaussv5_pgxc_ratio_USER |  |
| 锁 组名：LOCK | 死锁次数 | rds091_gaussv5_deadlocks |
| 锁总数 | rds407_gaussv5_total_locks_count |  |
| 系统用户数据库锁总数 | rds407_gaussv5_total_locks_count_SYS |  |
| 业务用户数据库锁总数 | rds407_gaussv5_total_locks_count_USER |  |
| 表级锁总数 | rds408_gaussv5_relation_locks_count |  |
| 系统用户表级锁总数 | rds408_gaussv5_relation_locks_count_SYS |  |
| 业务用户表级锁总数 | rds408_gaussv5_relation_locks_count_USER |  |
| 行级锁总数 | rds409_gaussv5_tuple_locks_count |  |
| 系统用户行级锁总数 | rds409_gaussv5_tuple_locks_count_SYS |  |
| 业务用户行级锁总数 | rds409_gaussv5_tuple_locks_count_USER |  |
| 其他锁总数 | rds410_gaussv5_other_locks_count |  |
| 系统用户其他锁总数 | rds410_gaussv5_other_locks_count_SYS |  |
| 业务用户其他锁总数 | rds410_gaussv5_other_locks_count_USER |  |
| 同步状态 组名：SYNCSTAT | 主机流控时间 | rds079_gaussv5_current_sleep_time |
| 备机RTO时间 | rds080_gaussv5_current_rto |  |
| 备机redo进度 | rds083_gaussv5_standby_delay |  |
| 待落盘的数据量 | rds105_gaussv5_ckpt_delay |  |
| Xlog速率 | rds123_xlog_lsn |  |
| 候选槽位数量 | rds124_candidate_slots_num |  |
| 未落盘脏页数量 | rds125_dirty_page_num |  |
| 复制槽保留的WAL日志大小 | rds192_replication_slot_wal_log_size |  |
| 进程资源 组名：PROCESSRESOURCE | buffer命中率 | rds090_gaussv5_buffer_hit_ratio |
| 实时buffer命中率 | rds090_gaussv5_buffer_hit_ratio_real_time |  |
| 读物理文件的IO次数/秒 | rds106_gaussv5_phyrds |  |
| 写物理文件I/O次数/秒 | rds107_gaussv5_phywrts |  |
| 线程池使用率 | rds108_gaussdbv5_thread_pool |  |
| 进程占用内存上限 | rds109_max_process_memory |  |
| 进程已使用内存 | rds110_process_used_memory |  |
| 进程内存使用率 | rds111_process_used_memory_usage |  |
| 动态内存使用上限 | rds112_max_dynamic_memory |  |
| 已使用的动态内存 | rds113_dynamic_used_memory |  |
| 动态内存使用率 | rds114_dynamic_used_memory_usage |  |
| 已使用的其他内存 | rds115_other_used_memory |  |
| 已使用的共享内存 | rds116_shared_used_memory |  |
| Xlog数量 | rds126_xlog_num |  |
| 系统数据库大小 | rds127_sys_database_size |  |
| 用户数据库总大小 | rds128_user_database_size |  |
| undo页面使用数量 | rds170_undo_size |  |
| undo页面生成速率 | rds171_undo_rate |  |
| 数据页面损坏数量 | rds184_bad_block_num |  |
| 磁盘读取block/秒 | rds411_gaussv5_blks_read_counter |  |
| 缓存读取block/秒 | rds412_gaussv5_blks_hit_counter |  |
| 建连线程池waiting数量 | rds240_gaussdbv5_thread_pool_session_waiting |  |
| stream线程池running数量 | rds241_gaussdbv5_thread_pool_stream_running |  |
| stream连接数使用率 | rds242_gaussdbv5_stream_max_connection |  |
| undo空间使用率 | rds254_undo_utilization_rate |  |
| 实例信息 组名：INSTATUS | 实例状态 | gaussdb_instance_status |
| DN主备状态 | datanode_switch_status |  |
| CN进程启动时刻 | rds133_cn_startup_time |  |
| DN进程启动时刻 | rds134_dn_startup_time |  |
| GTM进程启动时刻 | rds135_gtm_startup_time |  |
| CMS进程启动时刻 | rds136_cms_startup_time |  |
| ETCD进程启动时刻 | rds137_etcd_startup_time |  |
| CMA进程启动时刻 | rds138_cma_startup_time |  |
| GTM进程内存占用 | rds139_gtm_process_mem_usage |  |
| ETCD进程内存占用 | rds140_etcd_process_mem_usage |  |
| CMS进程内存占用 | rds141_cms_process_mem_usage |  |
| CMA进程内存占用 | rds142_cma_process_mem_usage |  |
| CN临时目录大小 | rds119_cn_temp_dir_size |  |
| DN临时目录大小 | rds120_dn_temp_dir_size |  |
| ETCD数据目录大小 | rds121_etcd_data_dir_size |  |
| OM模块ERROR日志数 | rds148_gaussdbv5_error_om |  |
| CM模块ERROR日志数 | rds149_gaussdbv5_error_cm |  |
| 内核ERROR日志数 | rds150_gaussdbv5_error_kernel |  |
| om_agent进程状态 | rds150_om_agent_process |  |
| om_monitor进程状态 | rds151_om_monitor_process |  |
| 与有效时钟源的偏差时间 | sys084_clock_synchronization_diff |  |
| 管控Agent状态 组名：AGENTSTATUS | dbmanager进程状态 | rds075_dbmanager_process_code |
| watchdog进程状态 | rds076_watchdog_process_code |  |
| agent_monitor进程状态 | rds077_agentmonitor_process_code |  |
| 容灾状态 组名：FORDR | 灾备实例分片日志差距 | rds172_streaming_dr_xlog_gap |
| 灾备实例分片日志回放速率 | rds175_streaming_dr_xlog_replay_rate |  |
| 灾备实例分片待回放日志量 | rds173_streaming_dr_xlog_to_be_replayed |  |
| 灾备实例分片日志落盘速率 | rds174_streaming_dr_xlog_flushing_rate |  |
| 灾备实例分片日志同步预期耗时（RPO） | rds176_streaming_dr_rpo |  |
| 灾备实例分片日志回放预期耗时（RTO） | rds177_streaming_dr_rto |  |
| OceanStor Dorado主备集群间日志回放预期耗时（RTO） | rds183_sharestore_local_rto |  |
| OceanStor Dorado LUN 组名：DORADO_LUN_IO | 块带宽 | dme001_lun_bandwidth |
| IOPS | dme002_lun_throughput |  |
| 平均I/O响应时间 | dme003_lun_responseTime |  |
| 读带宽 | dme004_lun_readBandwidth |  |
| 写带宽 | dme005_lun_writeBandwidth |  |
| 平均读I/O大小 | dme006_lun_readSize |  |
| 平均写I/O大小 | dme007_lun_writeSize |  |
| 读IOPS | dme008_lun_readThroughput |  |
| 写IOPS | dme009_lun_writeThroughput |  |
| 平均读I/O响应时间 | dme010_lun_readResponseTime |  |
| 平均写I/O响应时间 | dme011_lun_writeResponseTime |  |
| 读I/O百分比 | dme012_lun_readRatio |  |
| 写I/O百分比 | dme013_lun_writeRatio |  |


---

## 修改接口限流配置


### 操作场景

修改元数据库用户API对应接口的限流配置，以满足客户使用诉求。


### 操作步骤

如果元库是容器部署docker exec -i $(docker ps | grep gaussdb_${arch} | awk -F ' ' '{print $1}') su - service -c "source ~/gauss_env_file;cm_ctl query -Cvdip"

su - dbadmin -c "source ~/gauss_env_file;cm_ctl query -Cvdip"

回显样例如下：

sh /data/docker-service/action/common/metadb_operator.sh query_pwd core

其中，/data/docker-service为TPOPS默认的安装目录，以实际的安装目录为准。

gsql -p 8635 -U core -W {password} -q core -h 127.0.0.1

其中{password}为4中获取到的core库连接密码。

SELECT request_limit,second_time_window FROM CORE.dbs_lite_api_meta WHERE uri='{接口URI}' AND http_method='{请求方法}';

其中：

以POST /gaussdb/v3/instances/{instance_id}/nodes/repair接口为例，查询命令如下：

```
SELECT request_limit,second_time_window FROM CORE.dbs_lite_api_meta WHERE uri='/gaussdb/v3/instances/{}/nodes/repair' AND http_method='POST';
```

UPDATE CORE.dbs_lite_api_meta SET request_limit={request_limit},second_time_window={second_time_window} WHERE uri='{接口URI}' AND http_method='{请求方法}';

以POST /gaussdb/v3/instances/{instance_id}/nodes/repair接口为例，更新限流配置为每60秒120次，参考命令如下：

```
UPDATE CORE.dbs_lite_api_meta SET request_limit=120,second_time_window=60 WHERE uri='/gaussdb/v3/instances/{}/nodes/repair' AND http_method='POST';
```

\q

云数据库GaussDB管理平台（TPOPS）所有节点均需执行以下步骤。

docker ps -a

docker restart {container_id}

{container_id}为上图红框中luban和gaussdb-open-api镜像对应的CONTAINER ID字段。

docker ps -a
