#!/usr/bin/env bash
# ============================================================================
# TPOPS 采集端 —— 统一启动入口
#
#   bash run.sh
#
# 两种安装模式都能用：
#   venv   模式 → 用 venv/bin/python
#   vendor 模式 → 用 .python-bin 记的解释器 + PYTHONPATH=vendor/
#
# 走哪个模式**以 .install-mode 为准**（install.sh 写的）——
# 不靠「目录是否存在」猜：装了一半的 venv 会盖掉装好了的 vendor。
# 没有 .install-mode 时才回退到目录判断（老包兼容）。
#
# systemd 单元请调这个脚本，别直接调 python —— 这样换安装模式不用改 unit。
# ============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE" || exit 1

MODE=""
if [ -f "$HERE/.install-mode" ]; then
  MODE="$(cat "$HERE/.install-mode")"
fi

case "$MODE" in
  venv)
    [ -x "$HERE/venv/bin/python" ] || {
      echo "[run][ERROR] 安装模式是 venv，但 $HERE/venv/bin/python 不存在 —— 装坏了，重跑 install.sh" >&2
      exit 3
    }
    exec "$HERE/venv/bin/python" -m tpops_kafka_collector.main "$@"
    ;;
  vendor)
    PY=""
    if [ -f "$HERE/.python-bin" ]; then
      PY="$(cat "$HERE/.python-bin")"
    fi
    [ -x "$PY" ] || PY="$(command -v python3 || true)"
    [ -x "$PY" ] || {
      echo "[run][ERROR] 找不到可用的 Python 解释器（vendor 模式）" >&2
      exit 3
    }
    export PYTHONPATH="$HERE/vendor${PYTHONPATH:+:$PYTHONPATH}"
    exec "$PY" -m tpops_kafka_collector.main "$@"
    ;;
esac

# --- 兼容老包：没有 .install-mode 时按目录判断 ---
if [ -x "$HERE/venv/bin/python" ]; then
  exec "$HERE/venv/bin/python" -m tpops_kafka_collector.main "$@"
fi

if [ -d "$HERE/vendor" ]; then
  PY=""
  if [ -f "$HERE/.python-bin" ]; then
    PY="$(cat "$HERE/.python-bin")"
  fi
  [ -x "$PY" ] || PY="$(command -v python3 || true)"
  [ -x "$PY" ] || {
    echo "[run][ERROR] 找不到可用的 Python 解释器（vendor 模式）" >&2
    exit 3
  }
  export PYTHONPATH="$HERE/vendor${PYTHONPATH:+:$PYTHONPATH}"
  exec "$PY" -m tpops_kafka_collector.main "$@"
fi

echo "[run][ERROR] 既没有 venv/ 也没有 vendor/ —— 安装没成功，请重跑 install.sh" >&2
exit 3
