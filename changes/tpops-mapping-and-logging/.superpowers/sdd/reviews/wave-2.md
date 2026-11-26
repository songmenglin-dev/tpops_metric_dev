# Wave 2 审查报告 · wave-2-logging

- **Wave**：`wave-2-logging` · **Base**：`bb4ccd7`（wave-1 修复后的 HEAD） · **Head**：`526e873`
- **审查对象**：任务 2.1~2.7（依赖落地 / `logging_setup.py` 重写为 loguru sink 装配 / 39 模块迁移 / 配置项 / 通道统计与 11 字段汇总 / 敏感信息不落盘）+ wave-1 遗留三件收口
- **实现者报告**：`.superpowers/sdd/wave-2-implementer-report.md`

> **审查执行方式披露**：独立 reviewer 子代理在本会话因 **API 配额/网络错误**（429 Token Plan 上限、EAI_AGAIN）
> 已连续失败 3 次，本轮审查由**控制器（主会话）**执行。判级口径不变，但**独立性弱于独立代理审查**，特此标注。

## Spec Compliance

**✅ Spec compliant** —— R8~R17 全部满足；**0 个 Critical、0 个 Important**。

| 需求 | 判定 | 证据 |
|---|---|---|
| R8 日志目录与文件布局 | ✅ | `logging_setup.py:128-130` 文件名模板 `f'{name}-{{time:YYYY-MM-DD}}.log'`；6 个基名（总日志 + 5 通道）；`LOG_DIR` 未设置 → 只 stderr；不可写 → 启动失败 |
| R9 总日志与通道日志分工 | ✅ | sink 分层：stderr + 总日志 + 5 通道；通道 sink 的 `filter=` 读 `record["extra"]["channel"]`；未绑 channel 的记录只进总日志 |
| R10 每通道每轮汇总行 | ✅ | `scheduler_report.py:31-32` 固定 11 字段 `channel/tick/instances/fetched/mapped/produced/failed/windows/calls/elapsed/delivery_rate`；`scheduler_report.py:59` 用 `LOGGER.bind(channel=item.channel).info(...)`；未到期通道不出行 |
| R11 日志格式 | ✅ | text 用 loguru 原生格式（`LOG_PATTERN` 可覆盖）；json 用 `format=` **自定义函数**（`logging_formats.py:73-99`），含 `timestamp/level/logger/thread/message` + `k=v` 摊平 + `exception` |
| R12 日志切分与保留 | ✅ | `logging_formats.py:104-124` `make_rotation`：跨天（文件名日期 ≠ 今天）**或** `tell()+len(message) > limit` → 返回真值立即切；`make_retention`（:127-160）按基名 + 日期保留最近 N 个自然日（含当天），**被改名的同日分片一并清理**（正则 `(?:\..*)?$`） |
| R13 采集端后台运行 | — | 属 wave-3，本波次只在 `collector.sh` 的 `cmd_verify` 依赖检查里加了 loguru（**未抢跑**后台化） |
| R14 日志不落敏感信息 | ✅ | 新增 `tests/integration/test_logs_have_no_secrets.py`（150 行）：跑一轮后扫 `<LOG_DIR>` 全部文件，断言无口令 / token / SQL 全文 |
| R15 日志框架为 loguru | ✅ | 依赖 `loguru==0.8.0` 进运行时清单；生产包 `logging.getLogger` 残留 **0 处**（`logging_setup.py` 内 3 处是 InterceptHandler 转发第三方库，属豁免范围）；39 个模块改 `from loguru import logger` |
| R16 每轮结构化汇总日志 | ✅ | 整体汇总行保留在 `scheduler_report.py:74-76`，只进总日志；通道口径由 R10 承担 |
| R17 凭据不入日志 | ✅ | 同 R14 |
| 硬约束 | ✅ | 无 3.8+ 语法（py37 门禁 178 passed）；只新增 `loguru==0.8.0`；`logging_setup.py` 304 行 / `logging_formats.py` 160 行 / `channel_stats.py` 121 行，未破 400 行红线 |

### ⚠️ Cannot verify from diff

- 目标机离线安装路径上 `loguru` wheel 的**实际可获取性**（本机 pip 可取 → 部署包的
  `build_deploy_bundle.py` 清单已纳入，但**离线环境**未实跑）—— 留给 wave-4 的部署包门禁。

## Strengths

- **发现并修掉了一类"静默丢数据"的隐患**：loguru 用 `str.format` 语义，printf 的 `%s`
  在 `'a %s'.format('X')` 下**原样保留** —— 插值被静默丢弃、**不报错**。实现者机械转换了
  111 处占位符，并加了两条 **AST 级**守卫测试（`test_log_calls_use_brace_placeholders`、
  `test_log_calls_placeholder_count_matches_arguments`）——
  比 grep 强，能防未来回归。这是本波次最实在的贡献。
- **rotation 回调的语义没踩坑**：`make_rotation` 明确按 loguru 的「返回真值 = **立即切**」
  实现，把「跨天」与「当日超限」两个条件都放进回调；`_crossed_day()` 靠**文件名里的日期**
  与今天比对，而不是靠内存状态 —— 进程重启后也不会把昨天的文件当今天的继续写。
- **JSON format 函数处理了 loguru 的二次解析**：`logging_formats.py:95-99` 明确把
  JSON 里的花括号转义（`{{` / `}}`）并自己补换行 —— 否则 JSON 会被当成 `{record_key}`
  占位符解析而炸掉。这是 loguru 自定义 format 的隐藏坑。
- **retention 按基名隔离**：总日志的清理不碰通道日志，反之亦然（`make_retention(base_name, days)`）。
- **既有测试的改动是「换断言形式」而非「放宽」**：把 `assert owned[0].stream is not None`
  （只检查流存在）换成「消息**真的落到 stderr**」（`capsys`）—— 反而**更强**了。

## Issues

### Critical (Must Fix)

无。

### Important (Should Fix)

无。

### Minor (Nice to Have)

- **M-1**：`stderr` sink 与总日志同内容 → **开发态前台看不到通道 INFO 明细**（明细只在通道日志文件里）。
  这是简报表格「stderr 与总日志同内容」的直接后果，行为符合规格，但开发体验有折损。
  建议在 `docs/日志框架规范.md` 里写明「开发态要���通道明细请看 `<LOG_DIR>/collector-<channel>-<日期>.log`」，
  或给 stderr sink 也带上通道明细（需用户拍板，属行为变化，不在本次范围）。
- **M-2**：ruff 在 `ruff==0.4.10` 下有 363 条历史违规，与 HEAD 基线**逐条一致**（本波次未引入新类别）；
  `requirements-dev.txt` 已钉版本。这 363 条属历史 lint 债，按上个变更的既定口径另案处理。

## 实现者提的 4 点 —— 拍板结论

| # | 实现者的问题 | 结论 | 理由 |
|---|---|---|---|
| ① | 简报说「调用点不动」，但 loguru 是 `str.format` 语义，printf 的 `%s` 会被**静默丢弃**；机械转换了 **111 处**占位符并加了「占位符数 == 参数数」的守卫 | **接受（必要偏离）** | 这不是偏离而是**修 bug**：保留 `%s` 会让日志插值静默消失且不报错，比"调用点不动"严重得多。守卫是 AST 级的（占位符计数 + 无 printf 占位符），可防回归。已核：生产代码里 `%s`/`%d` 残留 **0 处** |
| ② | stderr sink 按简报「与总日志同内容」，副作用是开发态前台看不到通道 INFO 明细 | **接受，记为 M-1** | 行为符合规格；开发态折损用文档说明化解。改 stderr 行为属对外行为变化，不该在实现里自选 |
| ③ | 第三方库降噪放在 stdlib 侧而**没有**用 `logger.disable("kafka")` | **接受** | loguru 的 `disable()` 是**全级别**关闭，会把 `kafka` 的 WARNING 一起丢 —— 与规格场景「kafka 客户端输出 WARNING → 该记录经 InterceptHandler 进入 loguru 的 sink」**直接冲突**。放 stdlib 侧压到 WARNING 是正确做法 |
| ④ | `LOG_FORMAT` 一身两职（`text`/`json` 或直接给 loguru 格式串）并另加 `LOG_PATTERN` | **不成立 —— 实现是干净的，无需改** | 核对 `config.py:33` `_LOG_FORMATS = ("text", "json")`：`LOG_FORMAT` **只接受**这两个值（枚举校验）；`LOG_PATTERN` 是**独立**的 text 格式覆盖项；`logging_setup.py:192-197` `_make_format()` 的分支是「json → json 函数 / 有 pattern → 用 pattern / 否则 → loguru 默认」。两个变量职责清晰，不存在一身两职 |

## Assessment

**Task quality：Approved**

**Reasoning**：R8~R12、R14~R17 全部满足且有测试证据；硬约束（py3.7、纯 Python 依赖、
文件规模、映射层不越界）全部守住；`deploy/collector.sh` 只改了依赖检查一行，**未抢跑**
wave-3 的后台化。实现者发现并修掉了 printf 占位符在 loguru 下被静默丢弃这一类隐患，
并用 AST 级守卫防回归；rotation / retention 的 loguru 语义（立即切、按基名隔离、
含同日分片）实现正确。4 个待拍板点全部澄清，无 Critical / Important。

## 收据命令

```bash
ssf execution review changes/tpops-mapping-and-logging --wave wave-2-logging \
  --base bb4ccd7 --head 526e873 \
  --report .superpowers/sdd/reviews/wave-2.md --verdict pass
```
