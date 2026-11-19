# tpops_metric_dev

把 TPOPS（GaussDB 监控平台）的指标与告警，按客户 ITSCADA 的字段规范转换成 Kafka 消息。

**详细的项目说明在 [`CLAUDE.md`](CLAUDE.md)**（项目目标、架构、接口红线、编码约定、待定事项）。
本文件只做一件事：**仓库地图**——每个文件是什么、要不要动它。

## 目录

| 路径 | 内容 | 入口 |
|---|---|---|
| [`data/`](data/) | 实测留档与映射规则数据（**表驱动，规则不写死在代码里**） | [`data/README.md`](data/README.md) |
| [`tools/`](tools/) | 离线脚本：抓取、生成映射表、渲染文档、演示 | [`tools/README.md`](tools/README.md) |
| [`tests/`](tests/) | pytest 单元测试（TPOPS 与 Kafka 一律 mock） | [`tests/README.md`](tests/README.md) |
| [`docs/`](docs/) | 开发方案、字段对照表、客户对接方案、API 参考 | [`docs/README.md`](docs/README.md) |
| `tpops_kafka/` | **待建**：采集端（常驻进程）。当前仓库还没有这个包 | — |

## 根目录的文件

| 文件 | 作用 | 能不能删 |
|---|---|---|
| `CLAUDE.md` | **项目总说明**：目标、架构、目录结构、TPOPS 接口红线、Python 3.7.9 红线、编码约定、待定事项 | 不能 |
| `高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` | **客户指标库原件**（101 个指标，含「指标库」「指标库-指标获取方法」两个 sheet）。可提供性判定与映射表的源头 | 不能 |
| `requirements.txt` | 依赖锁定（目标运行时 Python 3.7.9，版本不能擅自升级） | 不能 |
| `pytest.ini` | pytest 配置：`testpaths = tests`、`--strict-markers` | 不能 |
| `ruff.toml` | Ruff 配置，`target-version = "py37"` —— 3.8+ 语法会被判为 invalid-syntax | 不能 |
| `.env.example` | 环境变量样例（`TPOPS_*` / `KAFKA_*` / 调度参数）。**真实凭据只在 `.env`，已 gitignore** | 不能 |
| `.gitignore` | 忽略凭据、构建产物、一次性中间产物 | 不能 |

## 一分钟上手

```bash
conda create -n tpops python=3.7.9 -c conda-forge --override-channels -y
conda install -n tpops -c conda-forge --override-channels -y pip
conda run -n tpops python -m pip install -r requirements.txt
conda run -n tpops python -m pytest -q
```

> Anaconda 默认源需要接受商业 TOS，**不要用**；一律加 `-c conda-forge --override-channels`。

## 数据流

```
Scheduler(窗口 + 水位)
  → 采集器（A 监控指标 / B 命令中心 SQL / C 诊断优化 / D 实例 / E 告警）   ← 待建
  → TPOPS Client（mini-token 认证）                                        ← 待建
  → Normalizer（TPOPS 原始结构 → 内部 DTO）                                ← 待建
  → FieldMapper（按映射表 + 取值规则 → 客户字段）                          ← data/mapping/ + tools/mappings/
  → Serializer（唯一序列化出口，JSON）                                     ← 待建
  → Kafka Producer（PLAINTEXT）                                             ← 待建
  → 推进水位（投递成功才推进）
```

已落地的是中间那一层：**映射表（数据）+ 映射层执行代码（`tools/mappings/`）+ 回归样本**。
采集端落地时，这部分直接搬进 `tpops_kafka/`。
