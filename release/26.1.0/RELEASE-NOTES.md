# TPOPS 采集端 —— 发布说明（版本 26.1.0）

生成时间：2026-11-25 11:31:37

## 交付物

| 文件 | 是什么 | 校验 |
|---|---|---|
| `tpops_metric_collector_26.1.0.tar.gz` | **软件发行包** —— 装到客户机器上跑：**软件部署指南.pdf**（+ 同内容的 部署运行说明.md）+ 五个部署脚本（check_env / check_deps / install / verify_env / run）+ **离线依赖包 wheels/** + 采集端软件包 `tpops_kafka_collector/`（含映射数据） | `74cd1e9a409d52ff932afaf466e9b5b140087f5a0f67f1cff93d6ff6066b9f44` |
| `tpops_metric_dev_26.1.0-source.tar.gz` | **工程源码包** —— 整个项目（按 git 跟踪的文件），给客户留档或审阅代码 | `447bdb5e6c75bf201255c386b0f14df16609245064f92a831ab2fcc4f5a03e41` |

## 客户侧怎么用

```bash
# 1) 验校验和（.sha256 与压缩包分开传，防篡改）
sha256sum -c tpops_metric_collector_26.1.0.tar.gz.sha256
tar xzf tpops_metric_collector_26.1.0.tar.gz
cd tpops_metric_collector_26.1.0

# 2) 检查环境（只读，不装任何东西）
bash check_env.sh

# 3) 离线依赖预检（这份依赖包在这台机器上装不装得上）
bash check_deps.sh

# 4) 安装环境需要的包 + 部署采集端包（离线）
sudo bash install.sh

# 5) 配环境变量 → 部署后自检 → 启动
sudo vi /opt/tpops-collector/.env
sudo bash /opt/tpops-collector/verify_env.sh
bash /opt/tpops-collector/run.sh
```

完整说明见部署包内的《部署运行说明.md》。

