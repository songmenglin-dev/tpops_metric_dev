# Wave 1 聚焦复审报告 · wave-1-mapping（第 2 轮）

- **Wave**：`wave-1-mapping` · **Base**：`32ed768` · **第 1 轮 Head**：`32d74ce` · **修复后 Head**：`bb4ccd7`
- **第 1 轮报告**：`.superpowers/sdd/reviews/wave-1.md`（判定 `fail`，1 个 Important：I-1）
- **本轮范围**：**只**复核 I-1 的修复；其余代码不得改动

> **执行方式披露**：独立 reviewer 子代理两次因 API 配额/网络错误中断（429 / EAI_AGAIN），
> 本轮复审同样由**控制器（主会话）**执行，独立性弱于独立代理审查，特此如实标注。

## 修复范围核对

`git diff 32d74ce..bb4ccd7 --stat`：

| 文件 | 变更 |
|---|---|
| `tools/build_itscada_mapping.py` | `_load_event_metric()` 区分「文件不存在」与「文件存在但段不可用」 |
| `tests/unit/test_alarm_metric_unification.py` | 新增 4 条测试（2 条覆盖播种、2 条覆盖拒绝回落） |
| `changes/tpops-mapping-and-logging/specs/mapping-and-rules/spec.md` | R3 新增「告警种类的唯一性」场景（修正我自己写错的断言） |
| `changes/tpops-mapping-and-logging/tasks.md` | 任务 1.7 断言改为「`alarm_id` 互不相同 + `alarm_name` 逐条对得上平台目录」 |
| `changes/tpops-mapping-and-logging/design.md` | 风险表对应行同步修正 |
| `changes/tpops-mapping-and-logging/.superpowers/sdd/progress.md` | 台账更新 |

**范围核对**：无 I-1 之外的**生产代码**改动 —— 映射层、生成器其余逻辑、部署脚本、
wave-2 内容均未触碰。规划工件的改动只涉及**修正我自己写错的断言**，不改变已批准的行为口径。

## I-1 复核

**修复前**（`tools/build_itscada_mapping.py:281-299`）：文件存在但 `event_metric` 缺失/损坏
→ 静默 `return dict(DEFAULT_EVENT_METRIC)`。

**修复后**（`tools/build_itscada_mapping.py:281-319`）：

- `not os.path.isfile(ALARM_RULES_OUT)` → 返回 `DEFAULT_EVENT_METRIC`（**首次生成播种**，行为不变）；
- 文件存在但 JSON 非法 → `raise RuntimeError(...)`，消息含文件路径与原因；
- 文件存在但 `event_metric` 不是含 `metric_code` + `metric_name` 的 dict →
  `raise RuntimeError(...)`，消息点名 `event_metric` 段。

**静默回退路径已消除**：现在任何「文件在但段没了」的情况都会**停下来报错**，
不会把运维改过的口径悄悄改回默认值。

### RED → GREEN 实测证据

```
$ git stash push -- tools/build_itscada_mapping.py     # 回退修复，保留新测试
$ conda run -n tpops python -m pytest tests/unit/test_alarm_metric_unification.py -q -k "refuses_to_silently"
FAILED ...::test_generator_refuses_to_silently_fall_back_when_the_segment_is_gone
FAILED ...::test_generator_refuses_to_silently_fall_back_on_a_broken_file
2 failed, 15 deselected in 0.45s

$ git stash pop                                        # 恢复修复
$ conda run -n tpops python -m pytest tests/unit/test_alarm_metric_unification.py -q
17 passed in 0.43s
```

新测试断言的是**可观察行为**（文件存在 + 段缺失 → 抛错；文件不存在 → 返回默认值），
不是实现细节；能抓住「把 RuntimeError 换回静默 return」这类回退。

## 门禁复跑

| 命令 | 结果 |
|---|---|
| `conda run -n tpops python -m pytest -q` | **1845 passed**（155.01s） |
| `conda run -n tpops python -m pytest tests/test_py37_compat.py -q` | **170 passed** |

> 全量数从第 1 轮的 1841 增至 1845：新增 4 条测试，无既有测试被删（`git diff --diff-filter=D` 在 `tests/` 下命中 0）。

## Spec Compliance

✅ Spec compliant —— 第 1 轮判为不合规的唯一一项（I-1）已修；R1~R19 与硬约束的判定维持第 1 轮结论。

## Issues

### Critical (Must Fix)

无。

### Important (Should Fix)

无。

### Minor (Nice to Have)

第 1 轮的 M-1 / M-2 / M-3 维持不变（不阻塞）：

- M-1：归档的描述串更新是**正确**的（该归档仍被生产代码加载）；
- M-2：manifest 断言由「相等」改为「覆盖」是**方向性放宽**但理由成立，收口时写进交付说明；
- M-3：`CLAUDE.md` 的「98 可提供 / 3 丢弃」已列入 wave-2 收口清单。

## Assessment

**Task quality：Approved**

**Reasoning**：I-1 已按第 1 轮的要求修复并**彻底移除了静默回退路径**，
两个分支各有测试覆盖且 RED → GREEN 有实测证据；修复范围严格限定在 I-1 与
我自己写错的规格断言，未夹带任何其他生产代码改动；全量 1845 passed、py37 门禁通过。

## 收据命令

```bash
ssf execution review changes/tpops-mapping-and-logging --wave wave-1-mapping \
  --base 32ed768 --head bb4ccd7 \
  --report changes/tpops-mapping-and-logging/.superpowers/sdd/reviews/wave-1-rereview.md \
  --verdict pass
```
