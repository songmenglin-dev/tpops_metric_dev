# TPOPS 采集端 —— 发布说明（版本 26.1.0）

生成时间：2026-11-25 13:47:13

## 交付物

| 文件 | 是什么 | 校验 |
|---|---|---|
| `tpops_metric_collector_26.1.0.tar.gz` | **软件发行包** —— 装到客户机器上跑：**软件部署指南.pdf**（+ 同内容的 部署运行说明.md）+ 部署脚本 `collector.sh`（一个脚本，位置参数选功能：check-env / check-deps / install / verify / run）+ **离线依赖包 wheels/** + 采集端软件包 `tpops_kafka_collector/`（含映射数据） | `6021a2a20d4fba616f461ab70f79b9ab49311226ed8dfa7b2ddd1241a10da60a` |

## 客户侧怎么用

```bash
# 1) 验校验和（.sha256 与压缩包分开传，防篡改）
sha256sum -c tpops_metric_collector_26.1.0.tar.gz.sha256
tar xzf tpops_metric_collector_26.1.0.tar.gz
cd tpops_metric_collector_26.1.0

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

