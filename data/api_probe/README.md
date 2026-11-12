# API 探针留档

对 TPOPS 平台**单接口的实测留档**。与 `data/metric_verification/`（逐指标留档）、
`data/command_center/results/`（命令中心留档）不同，这里只放"为了确认某个接口的真实行为"
而单独打的探针。

每个探针目录一个脚本，脚本可重跑，产物格式统一为
`{请求, 响应（状态码/响应头/响应体）, 耗时, 结论}`。

## health-status —— `GET /rds/health-status`

| 文件 | 内容 |
|---|---|
| `health_status.json` | 三次探针的完整请求/响应留档 + 自动结论 |
| `000manifest.json` | 出处记录：哪个 URL 产生了哪个文件 |

抓取方式：

```bash
export TPOPS_USER=sml TPOPS_PASSWORD='...'
python tools/fetch_tpops_health_status.py
```

### 实测结论（2026-11-06 22:53:52）

| 探针 | 请求 | 结果 |
|---|---|---|
| `no_auth` | `GET /rds/health-status`，**不带任何头** | **HTTP 200，响应体为空**（`Content-Length: 0`），耗时 0.229 s |
| `with_token` | 同上，带 `X-Mini-Token` + `X-Language` | **HTTP 200，响应体为空**，与不带 token **完全一致** |
| `bare_prefix` | `GET /health-status`（少 `/rds` 前缀） | **HTTP 404** |

### 这三条结论各自说明什么

1. **该接口不需要认证** —— `docs/api/API参考-权限说明.md` 里它的权限项写的是"无"，
   实测不带 token 也能 200。
2. **响应体是空的** —— 这是最关键的一条。接口只通过**状态码**表达信息：
   200 = GaussDB open-api 服务存活，非 200/超时 = 服务异常。
   **它不返回任何 JSON，不含实例、节点、端口、数据库名等任何业务信息。**
3. **`/rds` 前缀是必须的** —— 少一段就是 404（Tomcat 默认错误页），
   与开发方案里"平台有两个命名空间"的结论一致。

### 对现有结论的修正（重要）

开发方案与 `data/customer_metric_mapping.json` 此前写着：

> `gaussdb_{#PORT}port.status`（服务端口{#PORT}检测）：
> "仅能取到端口号，**可用性探测需 GET /rds/health-status**"

**这句话被本次实测证伪。** 该接口响应体为空，**不能作为任何客户指标的数据源**——
它只能回答"平台自己的 open-api 服务是否活着"，回答不了
"这个实例的这个端口通不通"或"这个实例的数据库是否健康"。

因此这两个指标的可用性探测**必须另找来源**，可选方向（尚未验证）：

- `GET /rds/v3.2/instances` 的 `instance_status`（实测值 `normal`）→ 已有 D 类通道在用
- 命令中心 SQL 查 `pg_is_in_recovery()` / `pg_stat_activity` 判断会话可达性
- TPOPS 自身的实例状态监控指标 `gaussdb_instance_status`（A 类，近似对应）

> 注：`data/customer_metric_mapping.json` 与 `data/customer_metric_query_plan.json` 里对应的
> `note` 字段已同步改为实测结论。之所以是手工改而不是重新生成：
> 生成器 `tools/build_customer_metric_mapping.py` 依赖 `data/tpops_all_metric.json`，
> 该源字典已按 CLAUDE.md 删除；重新生成需先跑 `tools/build_tpops_metric_dict.py`
> 重建平台指标字典（会再次调用平台），代价大且会连带 churn 其他字段。
> 生成器里的 note 源串已同步修正，下次重建时不会回退。
