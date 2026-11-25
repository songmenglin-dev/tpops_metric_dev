# deploy/ —— 部署资产

采集端**上生产机器**用的三个脚本。它们会被 `tools/build_deploy_bundle.py` 打进
离线部署包（`dist/tpops-collector-<版本>.tar.gz`），拷到目标机后直接用。

| 文件 | 在哪跑 | 干什么 |
|---|---|---|
| `check_env.sh` | 目标机，**装之前** | **环境预检（只读）**：架构、Python 版本区间、venv/ensurepip/pip 能力、安装目录、磁盘、端口 |
| `install.sh` | 目标机 | **零联网**安装，分两阶段：阶段一装环境需要的包、阶段二部署采集端包 |
| `verify_env.sh` | 目标机，**装之后** | 部署后自检：依赖、生产包、必填配置、状态目录（可选连通性） |
| `run.sh` | 目标机 | 统一启动入口（systemd 调它；读 `.install-mode` 决定用哪个解释器） |

## 部署流程（三步）

```
1. 检查环境     bash check_env.sh              # 只读；有 [FAIL] 就先别往下走
2. 装包 + 部署  sudo bash install.sh           # 阶段一装依赖，阶段二部署采集端包
3. 配置 + 验收  vi .env && bash verify_env.sh  # 自检通过后 bash run.sh 启动
```

`check_env.sh` 与 `verify_env.sh` 是**两件事**：前者回答「这台机器能不能装」，
后者回答「装好了没有、能不能跑」。

## 为什么不需要 conda

采集端的运行时依赖只有 `requests` / `kafka-python==2.0.2` / `urllib3` 三个
**纯 Python** 包 —— 没有 C 扩展、没有平台相关 wheel。三个直接后果：

1. 目标机**不用装 conda**，也不用编译任何东西；
2. 同一份部署包通吃 **x86_64 与 aarch64**（麒麟的两种架构），不必分别打包；
3. 安装可以完全离线（依赖从 `wheels/` 装，`pip install --no-index`）。

目标机只需要有 **Python 3.7 ~ 3.11**；麒麟 V10 SP2+ 自带 **3.7.9**，正好可用。

## 两个容易踩的坑

- **Python 3.12+ 不行。** `kafka-python 2.0.2` 依赖的 `six.moves` 机制在 3.12
  被移除的 `find_module` API 打断，`import kafka` 直接报
  `No module named 'kafka.vendor.six.moves'`。所以 `install.sh` 取**版本最低**的
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
INSTALL_PREFIX=/tmp/rehearse/opt bash install.sh
INSTALL_PREFIX=/tmp/rehearse/opt bash verify_env.sh
```

改这些脚本后，务必重跑演练并更新 `tests/test_deploy_bundle.py` 里的守卫。
