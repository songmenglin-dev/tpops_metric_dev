# deploy/ —— 部署资产

采集端**上生产机器**用的**一个脚本**：`collector.sh`。它会被
`tools/build_release.py` 打进离线部署包
（`release/<版本>/tpops_metric_collector_<版本>.tar.gz`），拷到目标机后直接用。

## 用法：一个脚本，位置参数选功能

```bash
bash collector.sh <命令> [参数...]
```

| 命令 | 在哪跑 | 干什么 |
|---|---|---|
| `check-env` [目录] [--network] | 装之前 | **环境预检（只读）**：架构、Python 版本区间、venv/ensurepip/pip 能力、安装目录、磁盘、端口 |
| `check-deps` [目录] | 装之前 | **离线依赖预检**：wheel 齐不齐、架构无关、逐条核对依赖清单，并**真装一遍到临时目录再删** |
| `install` [目录] | 装的时候 | **零联网**安装，分两阶段：阶段一装环境需要的包、阶段二部署采集端包 |
| `verify` [--network] | 装之后 | 部署后自检：依赖、生产包、必填配置、状态目录（可选连通性） |
| `run` [--foreground] [参数...] | 运行时 | **后台**启动采集端（写 PID 文件、日志落盘、脱离终端）；`--foreground` 改前台跑（systemd 用这个） |
| `status` | 运行时 | 采集端是否在跑、PID、运行时长、日志目录 |
| `stop` | 运行时 | 按 PID 文件优雅停止（SIGTERM），并清理 PID 文件 |
| `help` | — | 帮助 |

环境变量：`INSTALL_PREFIX` / `INSTALL_MODE` / `STATE_DIR` / `HEALTH_PORT` /
`LOG_DIR` / `LOG_FORMAT` / `LOG_PATTERN` / `LOG_MAX_BYTES` / `LOG_RETENTION_DAYS`。


## 部署流程（四步）

```
1. 检查环境     bash collector.sh check-env     # 只读；有 [FAIL] 就先别往下走
2. 查离线依赖   bash collector.sh check-deps    # 这份依赖包在这台机器上装不装得上
3. 装包 + 部署  sudo bash collector.sh install  # 阶段一装依赖，阶段二部署采集端包
4. 配置 + 验收  vi .env && bash collector.sh verify
                                            # 自检通过后 bash collector.sh run 启动（后台）
5. 日常运维      bash collector.sh status   # 看状态
                 bash collector.sh stop     # 停止
```

> systemd 托管时用 `ExecStart=/opt/tpops-collector/collector.sh run --foreground`
> —— `run` 默认后台化，systemd 需要前台进程才能托管与重启。

三个「检查」各管一段：`check-env` 答「这台机器能不能装」，
`check-deps` 答「这份依赖包装不装得上」，`verify` 答「装好了没有、能不能跑」。

## 为什么不需要 conda

采集端的运行时依赖只有 `requests` / `kafka-python==2.0.2` / `urllib3` / `loguru` 四个
**纯 Python** 包 —— 没有 C 扩展、没有平台相关 wheel。四个直接后果：

1. 目标机**不用装 conda**，也不用编译任何东西；
2. 同一份部署包通吃 **x86_64 与 aarch64**（麒麟的两种架构），不必分别打包；
3. 安装可以完全离线（依赖从 `wheels/` 装，`pip install --no-index`）。

目标机只需要有 **Python 3.7 ~ 3.11**；麒麟 V10 SP2+ 自带 **3.7.9**，正好可用。

## 两个容易踩的坑

- **Python 3.12+ 不行。** `kafka-python 2.0.2` 依赖的 `six.moves` 机制在 3.12
  被移除的 `find_module` API 打断，`import kafka` 直接报
  `No module named 'kafka.vendor.six.moves'`。所以 `collector.sh install` 取**版本最低**的
  可用解释器（最接近验收环境 3.7.9），并在选中 ≥ 3.12 时**明确报错退出**。
- **`get-pip.py` 不是离线路径。** 它自己要联网去 PyPI 找 pip 包，加 `--no-index`
  反而装不上；而且它是一份未经校验、却被 root 执行的脚本。真正的离线路径是
  **把随包的 pip wheel 直接解压进 site-packages**（`wheels/bootstrap/`）——
  部署包里**不含** `get-pip.py`。

## 本地演练

```bash
# 构建部署包（需要联网；或用 --wheels-dir 复用已有 wheel 离线构建）
python tools/build_deploy_bundle.py

# 端到端演练
mkdir -p /tmp/rehearse && tar xzf dist/tpops-collector-*.tar.gz -C /tmp/rehearse
cd /tmp/rehearse/tpops-collector-*
INSTALL_PREFIX=/tmp/rehearse/opt bash collector.sh install
INSTALL_PREFIX=/tmp/rehearse/opt bash collector.sh verify
```

改这些脚本后，务必重跑演练并更新 `tests/test_deploy_bundle.py` 里的守卫。
