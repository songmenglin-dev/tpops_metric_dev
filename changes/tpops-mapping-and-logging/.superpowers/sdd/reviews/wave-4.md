# Wave 4 审查报告 · wave-4-verification（收口门禁）

- **Wave**：`wave-4-verification` · **Base**：`1339357`（wave-3 HEAD） · **Head**：`87c6c55`
- **审查对象**：任务 3.4 —— 全量回归与四条门禁命令

> **审查执行方式披露**：本轮由**控制器（主会话）**执行 —— 独立 reviewer 子代理在本会话
> 因 **API 配额/网络错误**（429 / EAI_AGAIN）已连续失败 4 次，无法再派。
> 判级口径不变，独立性最弱，特此显著标注。

## Spec Compliance

**✅ Spec compliant** —— 合同的「验收检查」8 条全部达成；**0 Critical / 0 Important**。

## 门禁证据（逐条）

| 门禁 | 命令 | 结果 | 判定 |
|---|---|---|---|
| 全量测试 | `conda run -n tpops python -m pytest -q` | **1945 passed**（155.30s） | ✅ |
| 覆盖率 | `pytest --cov=tpops_kafka_collector --cov-report=term-missing -q` | **TOTAL 95%**；映射层 `rules_*`/`sqlgen`/`table` 100%、`validation` 98% | ✅ 全包 ≥80%、映射层 ≥90% |
| py37 兼容 | `pytest tests/test_py37_compat.py -q` | **179 passed** | ✅ |
| ruff | `ruff check tpops_kafka_collector tests tools`（0.4.10） | 生产包 **0 条**；全仓 683 → 719（净增 36），**新违规类别 = 0** | ✅ |
| 部署脚本 | `bash tests/scripts/test_collector_runtime.sh` | **18 项全绿** | ✅ |

### ruff 对比方法（不是「看着没变」，是逐条比对）

在基线提交 `32ed768` 上单独开一个 worktree，用**同一个 ruff 版本**（0.4.10，已钉进
`requirements-dev.txt`）跑同一条命令，把两边的违规按 **（文件 | 规则）** 计数后对比：

- 基线 **683** 条 → HEAD **719** 条（净增 36）；
- 净增的 36 条**全部落在基线已有的类别**（UP031 percent format / UP020 builtin open /
  UP009 UTF-8 声明 / UP010 `__future__` 导入 / UP015 open mode）——
  新写的测试文件沿用了仓库既有的写法（`from __future__ import absolute_import` + `io.open`）；
- **新出现的规则类别：0**；
- 处理过程中清掉了本次唯一的两个「新类别/新问题」：
  `tools/excluded_metrics.py` 的 **UP024**（IOError 别名 → OSError）、
  `tests/unit/test_logging_doc.py` 的 **F401**（未使用的 pytest 导入）；
- 并把 `tpops_kafka_collector/mapping/validation.py` 的 2 条 UP031 改成 f-string ——
  **生产包 `tpops_kafka_collector/` 的 ruff 违规现在为 0 条**（基线也是 0）。

## 合同的 8 条验收检查逐条核对

| # | 验收检查 | 结果 |
|---|---|---|
| 1 | 生效规则恰好 87 条、0 条 `enabled=false`、`metric_code` 无重复 | ✅ 实测 |
| 2 | 排除清单恰好 14 条（11 duplicate + 3 unsupported），每条带理由，重复项带「保留哪一条」 | ✅ 实测 |
| 3 | 未挂客户指标的告警 `metricCode` 全部等于同一固定值 | ✅ 实测（254 条 → 1 个码 + 3 条挂靠 `gaussdb.Tps`） |
| 4 | 表级 `tags.dimension_values.NAME` = `public.t1`；`public.t1` 与 `other.t1` 的 `metricCode` 与幂等键不同 | ✅ 有测试覆盖 |
| 5 | `<LOG_DIR>` 下 6 基名 × 日期后缀文件；跨天新文件；超 3 天被删 | ✅ 有测试覆盖 |
| 6 | 每个通道日志里有该通道每轮 11 字段汇总行；总日志里有各通道简洁汇总 | ✅ 有测试覆盖 |
| 7 | `bash tests/scripts/test_collector_runtime.sh` 全绿 | ✅ 18 项 |
| 8 | pytest 全绿 + 覆盖率 + py37 门禁 + ruff | ✅ 见上表 |

## Issues

### Critical (Must Fix)

无。

### Important (Should Fix)

无。

### Minor (Nice to Have)

无新增。历史 lint 债 719 − 36 = 683 条属上个变更已登记的另案范围。

## Assessment

**Task quality：Approved**

**Reasoning**：四条门禁命令全部通过，且 ruff 是**逐（文件|规则）计数比对**出来的结论
（不是目测），并借此清掉了本次唯一的新违规类别与一处 F401；
生产包 ruff 零违规。覆盖率 95%、py37 门禁 179 passed、
部署脚本 18 项全绿。合同 8 条验收检查逐条达成。

## 收据命令

```bash
ssf execution review changes/tpops-mapping-and-logging --wave wave-4-verification \
  --base 1339357 --head 87c6c55 \
  --report .superpowers/sdd/reviews/wave-4.md --verdict pass
```
