# TPOPS 采集端 —— 发布说明（版本 0.1.0）

生成时间：2026-11-25 11:02:40

## 交付物

| 文件 | 是什么 | 校验 |
|---|---|---|
| `tpops-collector-0.1.0.tar.gz` | **部署包** —— 装到客户机器上跑：最终部署文档说明 + 部署脚本（check_env / install / verify_env / run）+ 采集端生产包 `tpops_kafka_collector/`（含映射数据） | `7789d081dc2450cf9eb15609f968558e1d671e68a6380e8374099cdf66334c5a` |
| `tpops-metric-dev-0.1.0-source.tar.gz` | **工程源码包** —— 整个项目（按 git 跟踪的文件），给客户留档或审阅代码 | `9ae779363ff78d40eb3277e6ef79e361571f129b68956bb751acd029b7b2dbf6` |

## 客户侧怎么用

```bash
# 1) 验校验和（.sha256 与压缩包分开传，防篡改）
sha256sum -c tpops-collector-0.1.0.tar.gz.sha256
tar xzf tpops-collector-0.1.0.tar.gz
cd tpops-collector-0.1.0

# 2) 检查环境（只读，不装任何东西）
bash check_env.sh

# 3) 安装环境需要的包 + 部署采集端包（离线）
sudo bash install.sh

# 4) 配环境变量 → 部署后自检 → 启动
sudo vi /opt/tpops-collector/.env
sudo bash /opt/tpops-collector/verify_env.sh
bash /opt/tpops-collector/run.sh
```

完整说明见部署包内的《部署运行说明.md》。

