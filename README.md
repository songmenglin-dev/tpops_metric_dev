# tpops_metric_dev

把 TPOPS（GaussDB 监控平台）的指标与告警，按客户 ITSCADA 的字段规范转换成 Kafka 消息。

**详细的项目说明在 [`CLAUDE.md`](CLAUDE.md)**（项目目标、架构、接口红线、编码约定、待定事项）。
本文件只做一件事：**仓库地图**——每个文件是什么、要不要动它。

## 目录

| 路径 | 内容 | 入口 |
|---|---|---|
| [`data/`](data/) | 实测留档与映射规则数据（**表驱动，规则不写死在代码里**） | [`data/README.md`](data/README.md) |
| [`tpops_kafka_collector/`](tpops_kafka_collector/) | **采集端**（常驻进程）：唯一的生产代码包 | [`tpops_kafka_collector/main.py`](tpops_kafka_collector/main.py) |
| [`deploy/`](deploy/) | 部署资产：离线安装 / 环境自检 / 启动脚本（进部署包） | [`deploy/install.sh`](deploy/install.sh) |
| [`tools/`](tools/) | 离线脚本：抓取、生成映射表、渲染文档、**构建部署包** | [`tools/README.md`](tools/README.md) |
| [`tests/`](tests/) | pytest 单元测试（TPOPS 与 Kafka 一律 mock） | [`tests/README.md`](tests/README.md) |
| [`docs/`](docs/) | 开发方案、字段对照表、客户对接方案、API 参考 | [`docs/README.md`](docs/README.md) |

## 根目录的文件

| 文件 | 作用 | 能不能删 |
|---|---|---|
| `CLAUDE.md` | **项目总说明**：目标、架构、目录结构、TPOPS 接口红线、Python 3.7.9 红线、编码约定、待定事项 | 不能 |
| `高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx` | **客户指标库原件**（101 个指标，含「指标库」「指标库-指标获取方法」两个 sheet）。可提供性判定与映射表的源头 | 不能 |
| `requirements-runtime.txt` | **运行时**依赖锁定（进部署包；目标运行时 Python 3.7.9，版本不能擅自升级） | 不能 |
| `requirements-dev.txt` | 开发/测试依赖（**不进部署包**），含上面那份 | 不能 |
| `pytest.ini` | pytest 配置：`testpaths = tests`、`--strict-markers` | 不能 |
| `ruff.toml` | Ruff 配置，`target-version = "py37"` —— 3.8+ 语法会被判为 invalid-syntax | 不能 |
| `.env.example` | 环境变量样例（`TPOPS_*` / `KAFKA_*` / 调度参数）。**真实凭据只在 `.env`，已 gitignore** | 不能 |
| `.gitignore` | 忽略凭据、构建产物、一次性中间产物 | 不能 |

## 一分钟上手

```bash
conda create -n tpops python=3.7.9 -c conda-forge --override-channels -y
conda install -n tpops -c conda-forge --override-channels -y pip
conda run -n tpops python -m pip install -r requirements-dev.txt
conda run -n tpops python -m pytest -q
```

> Anaconda 默认源需要接受商业 TOS，**不要用**；一律加 `-c conda-forge --override-channels`。
> conda 只是**开发环境**的便利 —— 生产部署**不需要 conda**，见
> [`docs/交付-部署运行说明.md`](docs/交付-部署运行说明.md)。

## 构建离线部署包

```bash
conda run -n tpops python tools/build_deploy_bundle.py
# → dist/tpops-collector-<版本>.tar.gz
```

产物含生产包、离线 wheel、三个部署脚本与环境变量样例；目标机解压后
`bash install.sh` 即可**零联网**安装（麒麟 V10 SP2+ 自带 Python 3.7.9，
x86_64 与 aarch64 共用同一份包 —— 全部依赖都是纯 Python wheel）。

## 数据流

```
Scheduler(窗口 + 水位)
  → 采集器（A 监控指标 / B 命令中心 SQL / C 诊断优化 / D 实例 / E 告警）   ← 待建
  → TPOPS Client（mini-token 认证）                                        ← 待建
  → Normalizer（TPOPS 原始结构 → 内部 DTO）                                ← 待建
  → FieldMapper（按映射表 + 取值规则 → 客户字段）                          ← data/mapping/ + tpops_kafka_collector/mapping/
  → Serializer（唯一序列化出口，JSON）                                     ← 待建
  → Kafka Producer（PLAINTEXT）                                             ← 待建
  → 推进水位（投递成功才推进）
```

已落地的是中间那一层：**映射表（数据 `data/mapping/`）+ 映射层执行代码
（`tpops_kafka_collector/mapping/`）+ 回归样本**。
采集端落地时，映射层执行代码已搬进 `tpops_kafka_collector/`；映射数据仍在仓库根
`data/mapping/`（wave 2a 再搬进包内）。
