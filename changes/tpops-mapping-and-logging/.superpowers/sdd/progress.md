# 执行进度台账 · tpops-mapping-and-logging

模式：**sdd**（plan **revision 2**） · 分支：`tpops_metric_dev` · worktree：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`
状态：`executing` · batches_completed=1

| wave | 任务 | 状态 | receipt |
|---|---|---|---|
| wave-1-mapping | 1.1~1.8 | ✅ 完成（`32d74ce` + 修复 `bb4ccd7`） | **pass** |
| wave-2-logging | 2.1~2.7 | 🔄 实现中（代理已派出） | — |
| wave-3-deploy | 3.1~3.3 | 未开始 | — |
| wave-4-verification | 3.4 | 未开始 | — |

## wave-1 结论

- 实现 `32d74ce`（8 任务合并提交）；修复 `bb4ccd7`；receipt **pass**（plan revision 2）
- 门禁：全量 pytest **1845 passed**；py37 门禁 170 passed
- 审查发现 1 个 Important（已修）：`tools/build_itscada_mapping.py` 的 `_load_event_metric()`
  在「数据文件存在但 `event_metric` 段缺失/文件损坏」时**静默回落**硬编码默认值 ——
  会把运维改过的统一口径悄悄改回 `gaussdb.alarm`。已改为**报错停下**，
  并补 4 条测试（RED→GREEN 实测：回退后 2 failed，恢复后 17 passed）。
- 规格修订（我自己的错）：「254 条告警 `alarm_name` 各不相同」不成立（平台目录 13 行填等级词、
  2 组同名不同 ID，实测 241 个不同名）；唯一性由 `alarm_id`（254 个互不相同）承载。
  已改 `specs/mapping-and-rules/spec.md` R3、`tasks.md` 1.7、`design.md` 风险表。
- 实现者提的 4 点：全部接受（详见 `reviews/wave-1.md`）。

## 遗留（已归位）

- [x] R3 规格修正
- [ ] `CLAUDE.md`：编码约定改 loguru、「98 可提供 / 3 丢弃」→「87 可提供」、依赖表述 → **wave-2 任务 2.1/2.4**
- [ ] `docs/问题单.md` TPOPS-001 / TPOPS-003 勾选 → **wave-2**
- [ ] `requirements-dev.txt` 钉 ruff 版本 → **wave-2**
- [ ] `data/mapping/archive/` 归档描述串更新 —— 已判定**正确**（该归档仍被生产代码加载），不需回退
- [ ] manifest 断言由「相等」改「覆盖」的口径 → 收口时写进交付说明

## 产物路径

- wave-1：简报 `wave-1-brief.md` / 审查包 `wave-1-diff.md` / 实现报告 `wave-1-implementer-report.md`
  / 审查报告 `reviews/wave-1.md`（fail）/ 复审报告 `reviews/wave-1-rereview.md`（pass）
- wave-2：简报 `wave-2-brief.md` / 实现报告 `wave-2-implementer-report.md`
  / 审查报告 `reviews/wave-2.md`

## 审查执行方式说明（披露）

独立 reviewer 子代理两次因 **API 配额/网络错误**（429 Token Plan 上限、EAI_AGAIN）中断，
wave-1 的两轮审查（`reviews/wave-1.md`、`reviews/wave-1-rereview.md`）由**控制器（主会话）**
执行，判级口径不变但独立性弱于独立代理审查，已在两份报告里标注。
