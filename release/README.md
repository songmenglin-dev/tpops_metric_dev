# release/ —— 按版本号归档的交付物

这里放**按版本号归档**的交付物。每个版本一个子目录：

```
release/
├── README.md
└── 26.1.0/
    ├── RELEASE-NOTES.md                        # 本版本的清单与校验和
    ├── tpops_metric_collector_26.1.0.tar.gz    # 软件发行包（装到客户机器上跑）
    ├── tpops_metric_collector_26.1.0.tar.gz.sha256
    ├── 软件部署指南.pdf                          # 部署指南（PDF，免解压直接看）
    ├── 部署运行说明.md                            # 部署说明（Markdown 版）
    └── collector.sh                             # 部署与运维脚本
```

## 里面有什么

| 文件 | 是什么 |
|---|---|
| `tpops_metric_collector_<版本>.tar.gz` | **软件发行包**。解开后是完整可部署的采集端：部署文档（PDF + Markdown）、部署脚本 `collector.sh`（一个脚本，位置参数选功能）、**离线依赖包** `wheels/`（含引导用的 pip wheel）、采集端软件包 `tpops_kafka_collector/`（含映射数据）、`.env.example`、`MANIFEST.txt` |
| `*.sha256` | 校验文件。**要与压缩包分开传** —— 放一起就失去防篡改的意义 |
| `软件部署指南.pdf` | 部署指南（PDF 版）—— 现场不用解压就能看 |
| `部署运行说明.md` | 同一份内容的 Markdown 版 |
| `collector.sh` | 部署与运维脚本，与发行包内的是同一份 |
| `RELEASE-NOTES.md` | 本版本的交付清单、校验和、客户侧上手步骤 |

**源码包不在这里** —— 客户交付的是能跑的软件发行包，不是源码。
需要时用 `--with-source` 显式产出（默认不出）。

## 部署包特性

- **零联网**安装：依赖全在 `wheels/` 里，`pip install --no-index`
- **x86_64 与 aarch64 共用同一份**（全部依赖都是纯 Python wheel）
- 目标机只要有 **Python 3.7 ~ 3.11**（麒麟 V10 SP2+ 自带 3.7.9）

## 怎么重建

```bash
python tools/build_release.py                  # 需要联网（要下 wheel）
python tools/build_release.py --with-source    # 额外产出工程源码包
python tools/build_release.py --no-pdf         # 不渲染 PDF 部署指南
```

版本号取自 `tpops_kafka_collector.__version__`（`26.1.0`），可用 `--version` 覆盖。

> `release/**/*.tar.gz` **不进 git** —— 它们是可重建的产物，仓库里只留
> `README.md`、各版本的 `RELEASE-NOTES.md` 与 `.sha256` 校验文件。
> 要把压缩包本身也纳入版本管理（比如靠 git 做离线交付），把 `.gitignore`
> 里那行去掉即可。
