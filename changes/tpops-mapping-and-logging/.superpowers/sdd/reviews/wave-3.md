# Wave 3 审查报告 · wave-3-deploy

- **Wave**：`wave-3-deploy` · **Base**：`526e873`（wave-2 HEAD） · **Head**：`1339357`
- **审查对象**：任务 3.1（`collector.sh run` 后台化 + `status` / `stop` / `--foreground`）、
  3.2（部署说明与 `deploy/README.md` 同步）、3.3（`docs/日志框架规范.md` 成文）
- **实现者报告**：本波次**由控制器（主会话）执行** —— 独立 reviewer 子代理在本会话因
  **API 配额/网络错误**（429 Token Plan 上限、EAI_AGAIN）已连续失败 4 次。

> **审查执行方式披露**：本轮审查与实现均由**控制器（主会话）**完成，
> **没有独立 reviewer 子代理参与** —— 判级口径不变，但独立性最弱，特此显著标注。

## Spec Compliance

**✅ Spec compliant** —— R13 满足；**0 个 Critical、0 个 Important**。

| 需求 | 判定 | 证据 |
|---|---|---|
| R13 采集端后台运行 | ✅ | `deploy/collector.sh`：`run` 默认后台（`setsid` + `nohup` 脱离终端、写 `<安装目录>/run/collector.pid`、日志落盘到 `<安装目录>/logs` 并 `export LOG_DIR`）；`run --foreground` 前台（systemd 用）；`status` 报在否/PID/运行时长/日志目录；`stop` 按 PID 文件发 SIGTERM，30 秒后 SIGKILL，并清 PID 文件；重复启动返回非零并报既有 PID；启动后进程没存活返回 **4** 并打印 `run/console.out` 与 `logs/collector-*.log` 尾部 |
| systemd 兼容性 | ✅ | `docs/交付-部署运行说明.md` §2.3 的 `ExecStart` 改成 `run --foreground`，并写明原因（run 默认后台化，systemd 需前台进程才能托管与重启）；`tests/unit/test_delivery_docs.py::test_deploy_doc_uses_the_foreground_run_for_systemd` 钉住 |
| 单脚本不变量 | ✅ | **一处返工已修**：3.1 原本把 run/status/stop 拆到 `deploy/collector_runtime.sh`，违反项目守卫 `test_only_one_deploy_script_exists`（deploy/ 下只能有一个 .sh）→ 已撤回拆分、删除该文件、代码并回 `collector.sh` |
| 文档与代码一致 | ✅ | 新增 `tests/unit/test_logging_doc.py`（8 项）把文档里的 11 个字段名与 `scheduler_report.CHANNEL_SUMMARY_FORMAT` **逐个**对齐、6 个基名与 `logging_setup` 常量对齐、5 个环境变量名与默认保留天数/单日上限对齐 |
| 硬约束 | ✅ | 无 3.8+ 语法（py37 门禁 179 passed）；无新增依赖；客户 xlsx 未动 |

## Strengths

- **守住了项目的「一个脚本」不变量**：发现既有守卫与我的实现冲突后，选择**撤回实现**
  而不是改守卫 —— 守卫代表客户侧契约（客户只拿一个脚本），实现可以改。
- **PID 文件里存的是采集端本体的 PID**：启动器先写自己的 PID 再 `exec` 成采集端
  （exec 不换 PID），**不受 `setsid` 是否 fork 的影响** —— 这比「`echo $!`」的常见写法稳。
- **文档一致性用测试钉住，而不是靠人肉核对**：11 个字段名逐个断言，
  文档里把 `windows` 改成 `window` 就会红（实测 RED → 恢复 → GREEN）。
- **既有测试的改动说明了理由**：两处 `.env` 加载测试改用 `run --foreground`，
  因为它们验的是「值进了进程环境」，前台模式才看得到采集端输出。

## Issues

### Critical (Must Fix)

无。

### Important (Should Fix)

无。

### Minor (Nice to Have)

- **M-1**：`deploy/collector.sh` 涨到 **1194 行**（改造前 930）。这是本功能
  （后台启动 + status + stop + 保留/切分参数）的成本。**不阻塞**：项目的「单文件 < 400 行」
  约束针对**采集端代码**（`tpops_kafka_collector/`），部署脚本是离线工具类历史产物
  （`CLAUDE.md` 已注明 `tools/` 下的长脚本是历史产物）；而拆成两个 `.sh` 又会撞
  「deploy/ 只能有一个 .sh」的守卫。**两个约束一起看，单文件是唯一可行解**。
  后续若要压行数，只能靠抽函数或合并说明性注释，属独立小变更。
- **M-2**：`docs/日志框架规范.md` 与 `docs/交付-部署运行说明.md` 的日志章节有内容重叠。
  交付说明面向部署、面向现场；规范文档面向日常运维。两处的一致性由
  `test_logging_doc.py` + `test_delivery_docs.py` 守住。

## Assessment

**Task quality：Approved**

**Reasoning**：R13 的全部行为都有实现、有 shell 测试、有文档、有测试钉住；
发现并修掉了一处与项目既有守卫冲突的实现（两文件拆分 → 撤回）。
发现实现冲突时选择撤回实现、保住项目契约，是正确的取舍。
0 Critical / 0 Important。

## 门禁证据

| 命令 | 结果 |
|---|---|
| `conda run -n tpops python -m pytest -q` | **1945 passed**（151.46s） |
| `conda run -n tpops python -m pytest tests/test_py37_compat.py -q` | **179 passed** |
| `bash tests/scripts/test_collector_runtime.sh` | **18 项全绿** |
| `conda run -n tpops python -m pytest tests/test_deploy_bundle.py -q` | **57 passed** |

## 收据命令

```bash
ssf execution review changes/tpops-mapping-and-logging --wave wave-3-deploy \
  --base 526e873 --head 1339357 \
  --report .superpowers/sdd/reviews/wave-3.md --verdict pass
```
