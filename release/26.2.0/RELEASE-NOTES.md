# TPOPS 采集端 —— 发布说明（版本 26.2.0）

生成时间：2026-11-26 17:48:42

## 交付物

| 文件 | 是什么 | 校验 |
|---|---|---|
| `tpops_metric_collector_26.2.0.tar.gz` | **软件发行包** —— 装到客户机器上跑：**软件部署指南.pdf**（+ 同内容的 部署运行说明.md）+ 部署脚本 `collector.sh`（一个脚本，位置参数选功能：check-env / check-deps / install / verify / run）+ **离线依赖包 wheels/** + 采集端软件包 `tpops_kafka_collector/`（含映射数据） | `d04091aebd690ed2faad7abb876f2d5cf6b2efc20bdc4c22e31c7a3221478d70` |

## 客户侧怎么用

```bash
# 1) 验校验和（.sha256 与压缩包分开传，防篡改）
sha256sum -c tpops_metric_collector_26.2.0.tar.gz.sha256
tar xzf tpops_metric_collector_26.2.0.tar.gz
cd tpops_metric_collector_26.2.0

# 2) 检查环境（只读，不装任何东西）
bash collector.sh check-env

# 3) 离线依赖预检（这份依赖包在这台机器上装不装得上）
bash collector.sh check-deps

# 4) 安装环境需要的包 + 部署采集端包（离线）
sudo bash collector.sh install

# 5) 配环境变量 → 部署后自检 → 启动
sudo vi /opt/tpops-collector/.env
sudo bash /opt/tpops-collector/collector.sh verify
bash /opt/tpops-collector/collector.sh run
```

完整说明见部署包内的《部署运行说明.md》。

## 本版本变更（相对 26.1.0）

### 对客户：指标口径变了（**台账要按新口径重录**）

| 项 | 26.1.0 | 26.2.0 |
|---|---|---|
| 生效指标清单 | 101 条 | **87 条**（删掉 11 条大小写重复的表级指标 + 3 条平台侧拿不到的指标） |
| 表级指标的指标名 | `插入的行数` | **`(schema.table)插入的行数`** —— 不同 schema 的同名表不再撞成同一个指标 |
| 事件类告警的指标名 | 254 条告警各自一个名 | **统一成一个名**（`gaussdb.alarm` / `数据库告警`），告警种类靠 `alarmId`/`alarmName`/`alarmContent` 区分 |

> 统一值是**表驱动**的（`alarm_rules.json` 的 `event_metric` 段），运维要改只改数据文件、不改代码。
> 逐条口径见 `docs/交付-指标口径说明.md`；不可提供的 3 条见 `docs/交付-不可提供指标清单.md`；
> 客户指标英文名的 OCR 疑似错字见 `docs/交付-客户指标英文名核对清单.md`。

### 对运维：日志落盘 + 启停分离

- **日志框架换成 loguru**（stdlib logging 全量替换）：`logs/` 下 **6 个基名 × 日期后缀**
  （总日志 + 5 个采集器通道日志），**按天切分、保留 3 天**；
  每类采集器每轮一行汇总（获取量 / 已推送量 / 耗时…）。
- `collector.sh run` 改**后台运行**；新增 `collector.sh status` / `stop`；
  systemd 的 `ExecStart` 改 `run --foreground`。
- 日志规范见 `docs/日志框架规范.md`。

### 修复

- **认证状态加锁**（`tpops/client.py`）：客户端被调度器线程池共用，token 过期那一刻多个线程
  各发一次认证请求 —— 认证**失败**时平台按请求数累计 `login_failed_attempts`，账号会被锁得
  比预期快得多。改为 `threading.Lock` + 双检锁，同一时刻只有一个线程在认证。
- 统一告警指标名不再**静默回落**到硬编码默认值（数据文件缺失/损坏时直接报错停下）。

### 运行时依赖变化

新增 `loguru==0.8.0`（纯 Python、零传递依赖；0.8.0 是最后一个支持 Python 3.7 的版本）。
其余依赖与 26.1.0 相同 —— 全部为 `*-none-any.whl`，**x86_64 与 aarch64 共用同一份部署包**。

### 质量门禁

全量 pytest **1946 passed**；交付包复审（OpenCodeReview 委派审查）Critical 0 / Medium 0 / Low 0。

