# 权限说明


---

## 权限说明

本章节介绍云数据库GaussDB管理平台（TPOPS）对应API接口所需的权限。

**表1 平台用户管理**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 生成Token | POST /v3/auth/mini-token | 无 |
| 登录 | POST /v3/login | 无 |
| 登录重定向 | GET /v3/login/redirect | 无 |
| 注销 | POST /v3/logout | 无 |
| 查询用户列表 | GET /v3/users | dbs:user:list |
| 添加用户 | POST /v3/users | dbs:user:addOrUpdate |
| 删除用户 | DELETE /v3/users/{user_id} | dbs:user:delete |
| 查询用户基本信息 | GET /v3/users/{user_id} | 无 |
| 更新用户 | PUT /v3/users/{user_id} | dbs:user:addOrUpdate |
| 检查用户名是否可用 | POST /v3/users/name-validation | dbs:user:addOrUpdate |
| 重置自己的密码 | PUT /v3/users/{user_id}/password | dbs:base |
| 重置别人的密码 | PUT /v3/users/{user_id}/password/others | dbs:user:addOrUpdate |
| 查询用户已分配的实例列表 | GET /v3/users/{user_id}/instances | dbs:user:list |
| 为用户分配实例 | PUT /v3/users/{user_id}/instances | dbs:user:addOrUpdate |
| 查询用户角色列表 | GET /v3/users/{user_id}/roles | dbs:user:list |
| 为用户分配角色 | PUT /v3/users/{user_id}/roles | dbs:user:addOrUpdate |
| 查询用户已加入的用户组列表 | GET /v3/users/{user_id}/user-groups | dbs:user:list |
| 将用户加入用户组 | PUT /v3/users/{user_id}/user-groups | dbs:user:addOrUpdate |
| 锁定账户 | POST /v3/users/lock | dbs:user:addOrUpdate |
| 解锁账户 | POST /v3/users/unlock | dbs:user:addOrUpdate |
| 检查是否是弱密码 | POST /v3/users/weak-password-verification | dbs:base |
| 查询用户组列表 | GET /v3/user-groups | dbs:userGroup:list |
| 添加新用户组 | POST /v3/user-groups | dbs:userGroup:addOrUpdate |
| 更新用户组 | PUT /v3/user-groups/{user_group_id} | dbs:userGroup:addOrUpdate |
| 删除用户组 | DELETE /v3/user-groups/{user_group_id} | dbs:userGroup:delete |
| 查询用户组角色列表 | GET /v3/user-groups/{user_group_id}/roles | dbs:userGroup:list |
| 更新用户组角色信息 | PUT /v3/user-groups/{user_group_id}/roles | dbs:userGroup:addOrUpdate |
| 查询用户组用户列表 | GET /v3/user-groups/{user_group_id}/users | dbs:userGroup:list |
| 更新用户组用户信息 | PUT /v3/user-groups/{user_group_id}/users | dbs:userGroup:addOrUpdate |
| 查询角色列表 | GET /v3/roles | dbs:userRole:list |
| 添加角色 | POST /v3/roles | dbs:userRole:addOrUpdate |
| 更新角色 | PUT /v3/roles/{role_id} | dbs:userRole:addOrUpdate |
| 删除角色 | DELETE /v3/roles/{role_id} | dbs:userRole:delete |
| 查询角色的权限列表 | GET /v3/roles/{role_id}/permissions | dbs:userRole:list |
| 查询权限列表 | GET /v3/permissions | dbs:userRole:list |

**表2 备份管理**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 设置自动备份策略（v3.2） | PUT /gaussdb/v3.2/instances/{instance_id}/backups/policy | gaussdb:instance:modifyBackupPolicy |
| 设置自动备份策略（v3） | PUT /gaussdb/v3/instances/{instance_id}/backups/policy | gaussdb:instance:modifyBackupPolicy |
| 停止备份 | POST /gaussdb/v3/instances/{instance_id}/backups/stop | gaussdb:backup:stopBackup |
| 切换备份介质 | PUT /gaussdb/v3/instances/{instance_id}/change-backup-media | gaussdb:backup:changeBackupMedia |
| 备份扩展配置 | PUT /gaussdb/v3/instances/{instance_id}/backups/config | gaussdb:backup:setConfiguration |
| 查询自动备份策略 | GET /gaussdb/v3/instances/{instance_id}/backups/policy | gaussdb:backup:list gaussdb:instance:list |
| 查询备份扩展配置 | GET /gaussdb/v3/instances/{instance_id}/backups/config | gaussdb:backup:showConfiguration |
| 创建手动备份 | POST /gaussdb/v3/backups | gaussdb:backup:create |
| 删除手动备份 | DELETE /gaussdb/v3/backups/{backup_id} | gaussdb:backup:delete |
| 根据时间点或者备份文件查询原实例信息（v3） | GET /gaussdb/v3/instance-snapshot | gaussdb:instance:list gaussdb:backup:list |
| 根据时间点或者备份文件查询原实例信息（v3.1） | GET /gaussdb/v3.1/instance-snapshot | gaussdb:instance:list gaussdb:backup:list |
| 查询可用于备份恢复的实例列表（v3） | GET /gaussdb/v3/restorable-instances | gaussdb:instance:list gaussdb:backup:list |
| 查询可用于备份恢复的实例列表（3.1） | GET /gaussdb/v3.1/restorable-instances | gaussdb:instance:list gaussdb:backup:list |
| 查询可恢复时间段 | GET /gaussdb/v3/instances/{instance_id}/restore-time | gaussdb:instance:list gaussdb:backup:list |
| 查询差量备份列表 | GET /gaussdb/v3/instances/{instance_id}/differential-backups | gaussdb:backup:list |
| 备份恢复到当前实例或已有实例 | POST /gaussdb/v3/instances/recovery | gaussdb:instance:restoreInPlace |
| 备份恢复到目标实例数据后执行数据确认 | POST /gaussdb/v3/instances/{instance_id}/confirm-restore-data | gaussdb:backup:confirm |
| 查询备份列表（v3.1） | GET /gaussdb/v3.1/backups | gaussdb:backup:list |
| 查询备份列表（v3.2） | GET /gaussdb/v3.2/backups | gaussdb:backup:list |
| 查询实例归档日志路径 | GET /gaussdb/v3/instances/{instance_id}/archive-log-path | gaussdb:backup:getArchiveLogPath |
| 从存储介质同步元数据 | POST /gaussdb/v3/backups/sync-metadata | gaussdb:backup:syncMetadata |
| 删除备份数据 | DELETE /gaussdb/v3/backups/batch-delete | gaussdb:backup:delete |
| 安装SSL证书 | POST /gaussdb/v3/instances/{instance_id}/backups/ssl-certs | gaussdb:backup:create gaussdb:backup:installCert（推荐） |
| 切换备份方式 | PUT /gaussdb/v3.1/instances/{instance_id}/change-backup-media | gaussdb:backup:changeBackupMedia |
| 查询备份详情 | GET /gaussdb/v3/instances/{instance_id}/backups/detail | gaussdb:backup:queryBackupInfoDetail |
| 查询恢复详情 | GET /gaussdb/v3/instances/{instance_id}/backups/detail | gaussdb:backup:queryBackupInfoDetail |
| 通知备份恢复结果 | PUT /gaussdb/v3/instances/{instance_id}/backups/notify | gaussdb:backup:notifyBackupAndRestoreResult |

**表3 参数配置**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 获取指定实例的参数模板（v3.1） | GET /gaussdb/v3.1/instances/{instance_id}/configurations | gaussdb:param:list gaussdb:instance:list |
| 获取指定实例的参数模板（v3.2） | GET /gaussdb/v3.2/instances/{instance_id}/configurations | gaussdb:param:list gaussdb:instance:list |
| 修改指定实例的参数 | PUT /gaussdb/v3/instances/{instance_id}/configurations | gaussdb:param:modify |

**表4 数据库与用户管理**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 创建数据库 | POST /gaussdb/v3/instances/{instance_id}/database | gaussdb:instance:createDatabase |
| 创建数据库SCHEMA | POST /gaussdb/v3/instances/{instance_id}/schema | gaussdb:instance:createDatabaseSchema |
| 查询数据库SCHEMA列表 | GET /gaussdb/v3/instances/{instance_id}/schemas | gaussdb:instance:list gaussdb:instance:listSchemas |
| 创建数据库用户 | POST /gaussdb/v3/instances/{instance_id}/db-user | gaussdb:instance:createDatabaseUser |
| 删除数据库用户 | DELETE /gaussdb/v3/instances/{instance_id}/db-user | gaussdb:databaseUser:drop |
| 数据库用户授权/回收权限 | POST /gaussdb/v3/instances/{instance_id}/db-user/privileges | gaussdb:privileges:grantRevoke |
| 修改用户属性 | POST /gaussdb/v3/instances/{instance_id}/db-user/attributes | gaussdb:instance:modifyDatabaseUser |
| 初始化数据库内置用户密码 | PUT /gaussdb/v3/instances/{instance_id}/db-user/init-password | gaussdb:sysDatabaseUser:initPassword |
| 重置数据库用户密码 | PUT /gaussdb/v3/instances/{instance_id}/db-user/password | gaussdb:instance:modifyDatabasePasswd |
| 锁定数据库用户 | PUT /gaussdb/v3/instances/{instance_id}/db-user/lock | gaussdb:instance:modifyDatabaseUser |
| 数据库用户解锁 | PUT /gaussdb/v3/instances/{instance_id}/db-user/unlock | gaussdb:instance:modifyDatabaseUser |
| 查询数据库列表 | GET /gaussdb/v3/instances/{instance_id}/databases | gaussdb:instance:list gaussdb:instance:listDatabases |
| 查询数据库表列表 | GET /gaussdb/v3/instances/{instance_id}/tables | gaussdb:instance:list gaussdb:instance:listTables |
| 查询数据库用户列表 | GET /gaussdb/v3/instances/{instance_id}/db-users | gaussdb:instance:list gaussdb:instance:listDatabaseUsers |
| 查询数据库上的连接用户、应用、表统计信息以及锁信息 | GET /gaussdb/v3/instances/{instance_id}/database/relation-and-lock | gaussdb:dbRelationAndLockInfo:list |
| 表闪回 | POST /gaussdb/v3/instances/{instance_id}/db-table/flashback | gaussdb:sysDatabaseTable:flashBack |
| 创建数据库角色 | POST /gaussdb/v3/instances/{instance_id}/db-role | gaussdb:instance:createDbRole |
| 删除数据库角色 | DELETE /gaussdb/v3/instances/{instance_id}/db-role | gaussdb:instance:deleteDbRole |
| 数据库用户/角色自定义授权/回收权限 | POST /gaussdb/v3.1/instances/{instance_id}/db-user/privileges | gaussdb:instance:grantDatabaseUserPrivileges |

**表5 数据库诊断优化**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 获取数据库关键视图执行节点 | GET /gaussdb/v3/instances/{instance_id}/key-view-execute-node | gaussdb:instance:listRealTimeSession |
| 获取指定“节点-组件”维度下的实时会话信息 | POST /gaussdb/v3/instances/{instance_id}/real-time-session | gaussdb:instance:listRealTimeSession |
| 终止指定会话 | POST /gaussdb/v3/instances/{instance_id}/kill-session | gaussdb:instance:killSession |
| 终止空闲会话 | POST /gaussdb/v3/instances/{instance_id}/kill-free-session | gaussdb:instance:killFreeSession |
| 获取表分析信息 | POST /gaussdb/v3/instances/{instance_id}/database-table-analysis | gaussdb:instance:getDatabasesTableAnalysis |
| 获取索引分析信息 | POST /gaussdb/v3/instances/{instance_id}/database-index-analysis | gaussdb:instance:getDatabasesIndexAnalysis |
| 查询WDR快照开关状态 | GET /gaussdb/v3/instances/{instance_id}/wdr-snapshot/status | gaussdb:instance:list gaussdb:instance:listWdrSnapshot |
| 打开或者关闭WDR快照开关 | PUT /gaussdb/v3/instances/{instance_id}/wdr-snapshot/status | gaussdb:instance:operateWdrSnapshot |
| 查询WDR可用快照分组列表 | GET /gaussdb/v3/instances/{instance_id}/wdr-snapshots/available-groups | gaussdb:instance:listWdrSnapshot |
| 查询WDR快照采集结果 | GET /gaussdb/v3/instances/{instance_id}/wdr-snapshots | gaussdb:instance:list gaussdb:instance:listWdrSnapshot |
| 下发WDR快照命令 | POST /gaussdb/v3/instances/{instance_id}/wdr-snapshots | gaussdb:instance:operateWdrSnapshot |
| 采集WDR快照 | POST /gaussdb/v3/instances/{instance_id}/wdr-snapshots/collect | gaussdb:instance:list gaussdb:instance:operateWdrSnapshot |
| 下载WDR快照 | POST /gaussdb/v3/instances/{instance_id}/wdr-snapshots/download | gaussdb:instance:operateWdrSnapshot |
| 查询Top SQL列表 | POST /gaussdb/v3/instances/{instance_id}/top-sql-list | gaussdb:instance:listTopSql |
| 查询慢SQL执行节点 | POST /gaussdb/v3/instances/{instance_id}/slow-sql-execute-node | gaussdb:monitor:slowSql:list |
| 查询慢SQL集合 | POST /gaussdb/v3/instances/{instance_id}/slow-sql-list | gaussdb:monitor:slowSql:list |
| 查询慢SQL详情 | POST /gaussdb/v3/instances/{instance_id}/slow-sql-detail | gaussdb:monitor:slowSql:list |
| 新建SQL Patch | POST /gaussdb/v3/instances/{instance_id}/sql-patch | gaussdb:instance:operateSqlPatch |
| 查询SQL Patch | GET /gaussdb/v3/instances/{instance_id}/sql-patch | gaussdb:instance:getSqlPatch |
| 开启/关闭/删除SQL Patch | POST /gaussdb/v3/instances/{instance_id}/handle-sql-patch | gaussdb:instance:operateSqlPatch |

**表6 智能运维**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 纳管/解除纳管实例 | POST /v3/db-assist/manage-instance | gaussdb:instance:dbmindManage |
| 查询智能运维可纳管/已纳管实例列表（v3） | GET /v3/db-assist/manageable-instances | gaussdb:instance:list |
| 查询智能运维可纳管/已纳管实例列表（v3.1） | GET /v3.1/db-assist/manageable-instances | gaussdb:instance:list |
| 查询智能运维实例列表（v3） | GET /v3/db-assist/instances | gaussdb:instance:list |
| 查询智能运维实例列表（v3.1） | GET /v3.1/db-assist/instances | gaussdb:instance:list |
| 查询通用运维命令列表（支持SQL和PYTHON命令） | GET /gaussdb/v3/instances/{instance_id}/common-operations | gaussdb:instance:list gaussdb:instance:listCommonOmCmd |
| 下发通用运维命令操作（支持同步和异步命令） | POST /gaussdb/v3/instances/{instance_id}/common-operations/invoke | gaussdb:instance:operateCommonOmCmd |
| 查询异步运维命令执行结果 | POST /gaussdb/v3/instances/{instance_id}/common-operations/async-result | gaussdb:instance:list gaussdb:instance:listCommonOmCmd |
| 下载异步运维命令执行结果文件 | POST /gaussdb/v3/instances/{instance_id}/common-operations/download-result | gaussdb:instance:exportCommonOmCmdResult |
| 查询统一命令列表 | GET /gaussdb/v3/command-center/customer-commands | gaussdb:cmdCenter:listUniCommand |
| 查询单条统一命令详情 | GET /gaussdb/v3/command-center/commands/{cmd_id} | gaussdb:cmdCenter:showUniCommand |
| 执行单条统一命令 | POST /gaussdb/v3/command-center/customer-commands/{cmd_id} | gaussdb:cmdCenter:executeUniCommand |
| 查询统一命令实例任务状态及节点任务记录列表 | GET /gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/task-records | gaussdb:cmdCenter:listUniCmdExecTask |
| 查询统一命令执行节点任务结果详情 | GET /gaussdb/v3/command-center/command/{cmd_id}/batch-exec-job/{batch_job_id}/task-record/{task_id}/detail | gaussdb:cmdCenter:showUniCmdExecTaskDetail |
| 下载输出转储的命令执行结果文件 | POST /gaussdb/v3/download-center/files | gaussdb:downloadCenter:download |

**表7 SQL限流**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 创建限流任务 | POST /gaussdb/v3/instances/{instance_id}/limit-task | gaussdb:instance:flowlimitAddOrUpdate gaussdb:instance:createSQLLimitRule |
| 删除限流任务 | DELETE /gaussdb/v3/instances/{instance_id}/limit-task/{task_id} | gaussdb:instance:flowlimitDelete gaussdb:instance:deleteSQLLimitRule |
| 根据指定条件查询限流任务列表 | GET /gaussdb/v3/instances/{instance_id}/limit-task-list | gaussdb:instance:listFlowlimit gaussdb:instance:getSQLLimitRules |
| 查询节点下的SQL模板列表 | GET /gaussdb/v3/instances/{instance_id}/list-node-limit-sql-model | gaussdb:instance:listFlowlimit |
| 创建SQL限流任务（v3.1） | POST /gaussdb/v3.1/instances/{instance_id}/limit-task | gaussdb:instance:createSQLLimitRule |
| 修改SQL限流任务（v3.1） | PUT /gaussdb/v3.1/instances/{instance_id}/limit-task/{task_id} | gaussdb:instance:updateSQLLimitRule |
| 删除SQL限流任务（v3.1） | DELETE /gaussdb/v3.1/instances/{instance_id}/limit-task/{task_id} | gaussdb:instance:deleteSQLLimitRule |
| 查询SQL限流任务详情（v3.1） | GET /gaussdb/v3.1/instances/{instance_id}/limit-task/{task_id} | gaussdb:instance:getSQLLimitRule |
| 根据指定条件查询SQL限流任务列表（v3.1） | POST /gaussdb/v3.1/instances/{instance_id}/limit-task-list | gaussdb:instance:getSQLLimitRules |

**表8 主机密钥管理**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 生成主机连接密钥对 | POST /gaussdb/v3/hosts/key-pair | gaussdb:keyPair:generate |
| 查询密钥对信息 | GET /gaussdb/v3/hosts/key-pair | gaussdb:keyPair:list |
| 修改密钥信息 | PUT /gaussdb/v3/hosts/key-pair | gaussdb:keyPair:modify |
| 删除密钥对 | DELETE /gaussdb/v3/hosts/key-pair | gaussdb:keyPair:delete |
| 下载公钥 | GET /gaussdb/v3/hosts/public-key | gaussdb:keyPair:download |

**表9 数据中心管理**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 查询主机列表（v3） | GET /gaussdb/v3/hosts | gaussdb:host:list |
| 查询主机列表（v3.1） | GET /gaussdb/v3.1/hosts | gaussdb:host:list |
| 添加主机 | POST /gaussdb/v3/hosts | gaussdb:host:add |
| 修改主机静态信息 | PUT /gaussdb/v3/hosts | gaussdb:host:modify |
| 删除主机 | DELETE /gaussdb/v3/hosts | gaussdb:host:delete |
| 查询主机标准化检查结果 | GET /gaussdb/v3/hosts/{host_id}/detection-results | gaussdb:host:list |
| 下载主机导入模板 | GET /gaussdb/v3/hosts/template-file | gaussdb:host:download |
| 批量导入主机文件检查 | POST /gaussdb/v3/hosts/file-verification | gaussdb:host:add |
| 查询可支持主机规格 | GET /gaussdb/v3/hosts/specifications | gaussdb:host:list |
| 查询机房列表 | GET /gaussdb/v3/data-centers | gaussdb:dataCenter:list |
| 添加机房 | POST /gaussdb/v3/data-centers | gaussdb:dataCenter:add |
| 修改机房信息 | PUT /gaussdb/v3/data-centers | gaussdb:dataCenter:modify |
| 删除机房 | DELETE /gaussdb/v3/data-centers | gaussdb:dataCenter:delete |

**表10 日常巡检**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 查询巡检任务 | GET /gaussdb/v3/inspections | gaussdb:instance:getInspection |
| 创建巡检任务 | POST /gaussdb/v3/inspections | gaussdb:instance:createInspection |
| 删除选中的巡检任务 | POST /gaussdb/v3/inspections/batch-delete | gaussdb:instance:batchDeleteInspection |
| 启动选中的巡检任务 | POST /gaussdb/v3/inspections/{inspection_id}/start | gaussdb:instance:startInspection |
| 查询巡检任务明细 | GET /gaussdb/v3/inspections/{inspection_id} | gaussdb:instance:getInspection |
| 修改巡检任务 | PUT /gaussdb/v3/inspections/{inspection_id} | gaussdb:instance:modifyInspection |
| 巡检结果统计 | GET /gaussdb/v3/inspections/records/{record_id}/statistic | gaussdb:instance:getInspection |
| 巡检项执行详情 | GET /gaussdb/v3/inspections/records/{record_id} | gaussdb:instance:getInspection |
| 巡检项列表 | GET /gaussdb/v3/inspections/items | gaussdb:instance:getInspection |
| 查询巡检记录 | POST /gaussdb/v3/inspections/records | gaussdb:instance:getInspection |
| 批量删除巡检记录 | POST /gaussdb/v3/inspections/records/batch-delete | gaussdb:instance:deleteInspectionRecord |
| 查询巡检场景列表 | GET /gaussdb/v3/inspections/scenes | gaussdb:instance:getInspection |
| 新增巡检场景 | POST /gaussdb/v3/inspections/scenes | gaussdb:instance:createInspection |
| 删除巡检场景 | DELETE /gaussdb/v3/inspections/scenes | gaussdb:instance:batchDeleteInspection |
| 修改巡检场景 | PUT /gaussdb/v3/inspections/scenes | gaussdb:instance:modifyInspection |

**表11 实例管理**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 设置实例扩展信息 | PUT /gaussdb/v3/instances/{instance_id}/extend-info | gaussdb:instance:setInstanceExtendInfo |
| 查询实例扩展信息 | GET /gaussdb/v3/instances/{instance_id}/extend-info | gaussdb:instance:list |
| 获取实例组件列表 | GET /gaussdb/v3/instances/{instance_id}/components | gaussdb:instance:list |
| 主备切换 | POST /gaussdb/v3/instances/{instance_id}/switch-shard | gaussdb:instance:switchShard |
| 删除实例 | DELETE /gaussdb/v3/instances/{instance_id}/management | gaussdb:instance:delete |
| 创建/备份恢复新实例（v3） | POST /gaussdb/v3/instances | gaussdb:instance:create |
| 创建/备份恢复新实例（v3.1） | POST /gaussdb/v3.1/instances | gaussdb:instance:create |
| 查询数据库实例列表/查询实例详情（v3.1） | GET /gaussdb/v3.1/instances | gaussdb:instance:list |
| 查询数据库实例列表/查询实例详情（v3.2） | GET /gaussdb/v3.2/instances | gaussdb:instance:list |
| 停止数据库和节点 | POST /gaussdb/v3/instances/{instance_id}/db-stop | gaussdb:instance:stop |
| 重启数据库 | POST /gaussdb/v3/instances/{instance_id}/restart | gaussdb:instance:restart |
| 启动数据库和节点 | POST /gaussdb/v3/instances/{instance_id}/db-startup | gaussdb:instance:start |
| 检查主机是否可被纳管为实例 | POST /v3/instances/management/check-hosts-manageable | gaussdb:instance:manage |
| 纳管实例 | POST /v3/instances/management | gaussdb:instance:manage |
| AZ故障操作 | POST /v3/instances/{instance_id}/repair | gaussdb:instance:repair |
| 节点扩容 | POST /v3/instances/{instance_id}/expansion | rds:instance:expandCluster 或者 gaussdb:instance:modifySpec |
| 节点替换 | POST /v3/instances/{instance_id}/nodes/replacement | gaussdb:instance:replaceNode |
| 查询节点可迁移主机列表 | GET /gaussdb/v3/instances/{instance_id}/nodes/migration | gaussdb:instance:listNodeMigrationHosts |
| 节点迁移 | POST /gaussdb/v3/instances/{instance_id}/nodes/migration | gaussdb:instance:migrateNode |
| 节点迁移回滚 | POST /gaussdb/v3/instances/{instance_id}/nodes/migration-rollback | gaussdb:instance:rollbackNodeMigration |
| 实例升级版本获取（v3） | GET /gaussdb/v3/instances/{instance_id}/db-upgrade/candidate-versions | gaussdb:instance:upgradeDatabaseVersion |
| 实例升级版本获取（v3.1） | GET /gaussdb/v3.1/instances/{instance_id}/db-upgrade/candidate-versions | gaussdb:instance:upgradeDatabaseVersion |
| 实例升级（v3.1） | PUT /gaussdb/v3.1/instances/{instance_id}/db-upgrade | gaussdb:instance:upgradeDatabaseVersion |
| 实例升级（v3.2） | PUT /gaussdb/v3.2/instances/{instance_id}/db-upgrade | gaussdb:instance:upgradeDatabaseVersion |
| 节点修复 | POST /gaussdb/v3/instances/{instance_id}/nodes/repair | gaussdb:instance:repairNode |
| 形态变更 | PUT /gaussdb/v3/instances/{instance_id}/solution | gaussdb:instance:changeDeploymentSolution |
| 开启M兼容端口 | POST /gaussdb/v3/instances/{instance_id}/mysql-compatibility | gaussdb:instance:startMySQLCompatibility |
| 修改/关闭M兼容端口 | PUT /gaussdb/v3/instances/{instance_id}/mysql-compatibility | gaussdb:instance:updateMySQLCompatibility |
| 规格变更前准备 | POST /gaussdb/v3/instances/{instance_id}/flavor-resize/before-execute | gaussdb:instance:resizeFlavor |
| 规格变更后处理 | POST /gaussdb/v3/instances/{instance_id}/flavor-resize/after-execute | gaussdb:instance:resizeFlavor |
| 磁盘变更 | POST /gaussdb/v3/instances/{instance_id}/volume-resize | gaussdb:instance:expandVolume |
| 查询实例高级特性列表 | GET /gaussdb/v3/instances/{instance_id}/advance-features | gaussdb:instance:listFeatures |
| 开启高级特性 | POST /gaussdb/v3/instances/{instance_id}/advance-features | gaussdb:instance:updateFeatures |
| 修改实例备注 | PUT /gaussdb/v3/instances/{instance_id}/alias | gaussdb:instance:updateDescription |
| 实例组DN主备倒换 | POST /gaussdb/v3/instances/group/switchover | gaussdb:instance:switchoverInstanceGroupItems |
| 实例组停止节点 | POST /gaussdb/v3/instances/group/stop-node | gaussdb:instance:stop |
| 实例组启动节点 | POST /gaussdb/v3/instances/group/start-node | gaussdb:instance:start |
| 查询实例资源属性列表 | GET /gaussdb/v3/instances/resources/properties | gaussdb:instance:listResource |
| 批量升级内核Agent | POST /gaussdb/v3/instances/upgrade-kernel-agent | gaussdb:kernelAgent:upgrade |
| 批量回退内核Agent | POST /gaussdb/v3/instances/rollback-kernel-agent | gaussdb:kernelAgent:rollback |
| 批量重装内核Agent | POST /gaussdb/v3/instances/reinstall-kernel-agent | gaussdb:kernelAgent:reinstall |
| 查询可升级的内核Agent版本列表（v3） | GET /gaussdb/v3/kernel-agent/versions | gaussdb:kernelAgent:query |
| 查询可升级的内核Agent版本列表（v3.1） | GET /gaussdb/v3.1/kernel-agent/versions | gaussdb:kernelAgent:query |
| 查询实例内核Agent版本（v3） | POST /gaussdb/v3/kernel-agent/instances-version | gaussdb:kernelAgent:query |
| 查询实例内核Agent版本（v3.1） | POST /gaussdb/v3.1/kernel-agent/instances-version | gaussdb:kernelAgent:query |
| 更新管控Agent配置 | PUT /gaussdb/v3/agent/update-agent-config | dbs:agent:upgrade |
| 修改指定实例端口号 | PUT /gaussdb/v3/instances/{instance_id}/port | gaussdb:instance:modifyPort |

**表12 引擎版本和规格**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 查询数据库引擎的版本（v3） | GET /gaussdb/v3/datastore/versions | gaussdb:instance:list |
| 查询数据库引擎的版本（v3.1） | GET /gaussdb/v3.1/datastore/versions | gaussdb:instance:list |
| 获取服务版本信息 | GET /rds/v3/version-info | 无 |

**表13 微服务升级**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 查询实例Agent列表（v3） | GET /v3/instances | dbs:agent:getStatus |
| 查询实例Agent列表（v3.2） | GET /v3.2/instances | dbs:agent:getStatus |
| 查询指定实例的Agent信息 | GET /gaussdb/v3/instances/{instance_id}/agent/status | dbs:agent:getStatus |
| 单实例Agent升级 | POST /gaussdb/v3/instances/{instance_id}/agent/upgrade | dbs:agent:upgrade |
| 批量实例升级/回退Agent | POST /v3/instances/agent/batch-upgrade | dbs:agent:getStatus |

**表14 告警管理**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 告警对接测试 | POST /v3/alarm/notifications/interconnection/check | 无 |
| 新增告警对接配置 | POST /v3/alarm/notifications | dbs:alarm:addNotice |
| 修改告警对接配置 | PUT /v3/alarm/notifications | dbs:alarm:updateNotice |
| 查看告警对接配置 | GET /v3/instances/{instance_id}/alarm/notifications | dbs:alarm:listNotices |
| 告警结果列表 | POST /v3/alarm/records | dbs:alarm:list |
| 告警数量查询 | GET /v3/alarm/records/statistic | dbs:alarm:list |
| 实例告警批量配置 | POST /v3/alarm/update/configs | dbs:alarm:updateConfig |

**表15 监控大盘**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 查询监控指标 | POST /v3/monitor/metrics | dbs:monitor:list |
| 查询监控指标分组 | GET /v3/monitor/metrics/series | dbs:monitor:listMetrics |
| 健康检查 | GET /rds/health-status | 无 |

**表16 任务中心**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 通过JobID查询任务状态 | GET /v3/jobs | dbs:task:list |
| 查询异步批量处理记录详情 | GET /gaussdb/v3/instances/batch-async-task/records | gaussdb:instance:listBatchAsyncTaskRecords |

**表17 容灾管理**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 修改容灾任务名 | PUT /gaussdb/v3.6/instances/{instance_id}/disaster-recovery/rename | gaussdb:disasterRecovery:update |
| 查询容灾关系列表 | GET /gaussdb/v3.6/disaster-recovery/relations | gaussdb:disasterRecovery:list |
| 容灾操作历史记录 | GET /gaussdb/v3/instances/{instance_id}/disaster-recovery/records | gaussdb:instance:listRecord |
| 查询实例容灾监控实时状态 | GET /gaussdb/v3.6/instances/{instance_id}/disaster-recovery/monitor | gaussdb:disasterRecovery:list |
| 创建容灾任务 | POST /gaussdb/v3.6/instances/{instance_id}/disaster-recovery/construct | gaussdb:disasterRecovery:construct |
| 备实例容灾升主 | POST /gaussdb/v3.6/instances/{instance_id}/disaster-recovery/failover | gaussdb:disasterRecovery:failover |
| 结束容灾任务 | POST /gaussdb/v3.6/instances/{instance_id}/disaster-recovery/release | gaussdb:disasterRecovery:release |
| 容灾主备切换 | POST /gaussdb/v3.6/instances/{instance_id}/disaster-recovery/switchover | gaussdb:disasterRecovery:switchover |
| 加回 | POST /gaussdb/v3.6/instances/{instance_id}/disaster-recovery/restore | gaussdb:disasterRecovery:construct gaussdb:instance:create |
| 开始容灾演练 | POST /gaussdb/v3.6/instances/{instance_id}/disaster-recovery/simulation-start | gaussdb:disasterRecovery:simulation |
| 结束容灾演练 | POST /gaussdb/v3.6/instances/{instance_id}/disaster-recovery/simulation-stop | gaussdb:disasterRecovery:simulation |
| 开始日志保持 | POST /gaussdb/v3.6/instances/{instance_id}/disaster-recovery/keep-log-start | gaussdb:disasterRecovery:keeplog |
| 主实例结束容灾日志保持 | POST /gaussdb/v3.6/instances/{instance_id}/disaster-recovery/keep-log-stop | gaussdb:disasterRecovery:keeplog |
| 容灾搭建手动清理 | POST /gaussdb/v3/instances/{instance_id}/disaster-recovery/construct/cleanup | gaussdb:instance:cleanupDrConstruct |
| 容灾倒换失败容灾升主/容灾降备 | POST /gaussdb/v3/instances/{instance_id}/disaster-recovery/switchover/repair | gaussdb:disasterRecovery:switchoverRepair |
| 获取可创建容灾任务的主实例列表（v3） | POST /gaussdb/v3/instances/disaster/instanceList | gaussdb:disasterRecovery:list |
| 获取可创建容灾任务的主实例列表（v3.1） | POST /gaussdb/v3.1/instances/disaster/instance-list | gaussdb:disasterRecovery:list |
| 删除容灾记录 | POST /gaussdb/v3/instances/disaster/record/delete | gaussdb:disasterRecovery:delete |
| 容灾切换主 | POST /gaussdb/v3/instances/{instance_id}/disaster-recovery/change-primary | gaussdb:disasterRecovery:changePrimary |
| 容灾预校验 | POST /gaussdb/v3.5/instances/{instance_id}/disaster-recovery/pre-process | gaussdb:disasterRecovery:check |
| 批量容灾切换 | POST /gaussdb/v3/disaster-recovery/batch-switchover | gaussdb:disasterRecovery:batchExecute |
| 容灾降备（新主降备重建容灾关系） | POST /gaussdb/v3/instances/{instance_id}/disaster-recovery/demote | gaussdb:disasterRecovery:construct |
| 修改容灾组序列 | POST /gaussdb/v3/instances/{instance_id}/disaster-recovery/cluster-serial/change | gaussdb:instance:changeDrClusterSerial |
| 容灾组管理模式回退到容灾关系管理模式 | POST /gaussdb/v3/instances/{instance_id}/disaster-recovery/config-mode/rollback | gaussdb:instance:changeDrConfigMode |
| 容灾关系管理模式收缩到容灾组管理模式 | POST /gaussdb/v3/instances/{instance_id}/disaster-recovery/config-mode/refresh | gaussdb:instance:changeDrConfigMode |
| 升主测试 | POST /gaussdb/v3/instances/{instance_id}/disaster-recovery/promote | gaussdb:disasterRecovery:failover |
| 容灾组拓扑页面数据刷新 | POST /gaussdb/v3/disaster-recovery/relation-groups/{group_id}/topology/refresh | gaussdb:instance:refreshDrRelationGroupTopology |
| 查询容灾组列表 | GET /gaussdb/v3/disaster-recovery/relation-groups | gaussdb:instance:listAllDrRelationGroup |
| 拓扑图基础信息查询 | GET /gaussdb/v3/disaster-recovery/relation-groups/{group_id}/topology | gaussdb:instance:queryDrRelationGroupTopology |
| 查询容灾链路属性配置 | POST /gaussdb/v3/disaster-recovery/links/query | gaussdb:instance:listDrRelationLinksConfig |
| 删除容灾组记录 | DELETE /gaussdb/v3/disaster-recovery/relation-groups | gaussdb:disasterRecovery:delete |

**表18 许可管理**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 获取失效码 | POST /gaussdb/v3/license/invalidation | 该接口仅限拥有超级管理员角色的用户调用。 |
| 获取许可类型 | GET /gaussdb/v3/license/types | gaussdb:instance:license:query |

**表19 日志管理**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 查询操作日志 | POST /v3/operation-traces | dbs:trace:list |

**表20 租户管理**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 查询租户列表 | GET /v3/instances/{instance_id}/tenants | gaussdb:tenant:list |

**表21 透明加密（直连加密机）**

| API名称 | API接口 | 权限 |
| --- | --- | --- |
| 新增加密机配置 | POST /gaussdb/v3/kms-tde/hsm-config | gaussdb:hsm:createConfig |
| 查询加密机配置 | GET /gaussdb/v3/kms-tde/hsm-configs | gaussdb:hsm:listConfigs |
| 加密机配置修改 | PUT /gaussdb/v3/kms-tde/hsm-config/{configId} | gaussdb:hsm:updateConfig |
| 加密机配置验证 | POST gaussdb/v3/kms-tde/hsm-config/{configId}/instances/{instanceId}/verify | gaussdb:hsm:verifyConfig |
| 查询配置绑定实例 | GET /gaussdb/v3/kms-tde/hsm-config/{configId}/instances | gaussdb:hsm:listConfigs |
| 加密机配置删除 | DELETE /gaussdb/v3/kms-tde/hsm-config/{configId} | gaussdb:hsm:deleteConfig |
| 实例开启透明加密（直连加密机） | POST /gaussdb/v3/instances/{instanceId}/kms-tde/hsm/switch | gaussdb:hsm:switchTde |
| 实例更换加密机配置 | PUT /gaussdb/v3/instances/{instanceId}/kms-tde/hsm-config | gaussdb:hsm:updateInstanceConfig |
| 加密机配置重新下发 | POST /gaussdb/v3/instances/{instanceId}/kms-tde/hsm-config/redeploy | gaussdb:hsm:redeployConfig |
