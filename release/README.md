# release/ —— 按版本号归档的交付物

这里放**按版本号归档**的交付物。每个版本一个子目录：

```
release/
├── README.md
└── 26.1.0/
    ├── RELEASE-NOTES.md                          # 本版本的清单与校验和
    ├── tpops_metric_collector_26.1.0.tar.gz      # 软件发行包（装到客户机器上跑）
    ├── tpops_metric_collector_26.1.0.tar.gz.sha256
    ├── tpops_metric_dev_26.1.0-source.tar.gz     # 工程源码包（留档/审阅）
    └── tpops_metric_dev_26.1.0-source.tar.gz.sha256
```

## 两个包的区别

| 包 | 内容 | 用途 |
|---|---|---|
| **部署包**<br>`tpops_metric_collector_<版本>.tar.gz` | 最终部署文档说明（`部署运行说明.md` + **`软件部署指南.pdf`**）+ 部署脚本（`check_env.sh` / `check_deps.sh` / `install.sh` / `verify_env.sh` / `run.sh`）+ 采集端生产包 `tpops_kafka_collector/`（含 `mapping/data/*.json`）+ 离线 wheel | 传到客户机 → 解压 → `check_env.sh` → `install.sh` → `verify_env.sh` → `run.sh` |
| **源码包**<br>`tpops_metric_dev_<版本>-source.tar.gz` | 整个工程（按 git 跟踪的文件） | 给客户留档或审阅代码 |

部署包**零联网**安装，x86_64 与 aarch64（麒麟两种架构）共用同一份。
里面含**离线依赖包**（`wheels/`，含引导用的 pip wheel），装之前可用
`check_deps.sh` 验一遍「这份依赖在这台机器上装不装得上」。

## 怎么重建

```bash
python tools/build_release.py            # 需要联网（要下 wheel）
python tools/build_release.py --no-source   # 只出部署包
```

版本号取自 `tpops_kafka_collector.__version__`（`26.1.0`），
可用 `--version 0.2.0` 覆盖。

> `release/**/*.tar.gz` **不进 git** —— 它们是可重建的产物（见上），
> 仓库里只留 `README.md`、各版本的 `RELEASE-NOTES.md` 与 `.sha256` 校验文件。
> 要把压缩包本身也纳入版本管理（比如靠 git 做离线交付），把 `.gitignore`
> 里那两行去掉即可。
