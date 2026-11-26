# 执行进度台账 · tpops-mapping-and-logging

模式：**sdd**（plan revision 1） · 分支：`tpops_metric_dev` · worktree：`/mnt/c/sml/project/py_project/tpops_metric_dev-tpops_metric_dev`

| wave | 任务 | 状态 | receipt |
|---|---|---|---|
| wave-1-mapping | 1.1~1.8 | 实现完成（`32d74ce`）→ **审查中** | — |
| wave-2-logging | 2.1~2.7 | 未开始（等 wave-1 pass） | — |
| wave-3-deploy | 3.1~3.3 | 未开始 | — |
| wave-4-verification | 3.4 | 未开始 | — |

## wave-1 事实

- 提交 `32d74ce`（1 个提交，8 个任务合并提交；产物彼此强耦合，拆开中间提交必然跑不过测试）
- 门禁：全量 pytest **1841 passed**；波次证明命令 93 passed；py37 门禁 170 passed
- ruff：**仓库基线本就不绿**（ruff 0.17.6 vs `ruff.toml` 的 `select=UP`，基线 473~764 条历史违规）。
  本次未引入新类别。→ 收口时 ruff 门禁改为「**本次改动文件无新增违规**」+ 在 `requirements-dev.txt` 钉 ruff 版本。
- 已核实数据事实：生效规则 87 条 / `enabled=false` 0 条 / `metric_code` 唯一；
  排除清单 14 条（11 duplicate + 3 unsupported）；`event_metric={gaussdb.alarm,数据库告警}`；
  告警定义 257 条只剩 2 个不同 `metric_code`。

## 待处理（review 后归位）

- [ ] **R3 规格修正**：简报写的「254 条告警 `alarm_name` 各不相同」不成立
  （平台目录 13 行填的是等级词、2 组同名不同 ID，实测 241 个不同名）→ 改为
  「`alarm_id` 254 个互不相同 + `alarm_name` 逐条对得上平台目录」（待 review 确认后修 spec）
- [ ] **`CLAUDE.md` 仍写「98 可提供 / 3 丢弃」** → wave-2 输出清单里带该文件
- [ ] **`docs/问题单.md` 的 TPOPS-001 / TPOPS-003 已由本波次解决**，勾选框待收口时更新
- [ ] **`data/mapping/archive/` 归档文件被跟着重生成** → review 判定是否回退

## 产物路径

- wave-1 简报：`.superpowers/sdd/wave-1-brief.md`
- wave-1 审查包：`.superpowers/sdd/wave-1-diff.md`
- wave-1 实现报告：`.superpowers/sdd/wave-1-implementer-report.md`
- wave-1 审查报告：`.superpowers/sdd/reviews/wave-1.md`
