# deploy/ —— 部署资产

采集端**上生产机器**用的三个脚本。它们会被 `tools/build_deploy_bundle.py` 打进
离线部署包（`dist/tpops-collector-<版本>.tar.gz`），拷到目标机后直接用。

| 文件 | 在哪跑 | 干什么 |
|---|---|---|
| `install.sh` | 目标机 | **零联网**安装：挑 Python → 建 venv → 离线装运行时依赖 |
| `verify_env.sh` | 目标机 | 环境自检：OS/架构/Python/依赖/生产包/必填配置/状态目录（可选连通性） |
| `run.sh` | 目标机 | 统一启动入口（systemd 调它；自动识别 venv / vendor 两种安装模式） |

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
