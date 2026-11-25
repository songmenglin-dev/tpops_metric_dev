#!/usr/bin/env bash
# ============================================================================
# TPOPS 采集端 —— 部署环境自检
#
#   bash verify_env.sh              # 静态检查 + 依赖 + 配置
#   bash verify_env.sh --network    # 额外做 TPOPS / Kafka 的 TCP 连通性检查
#
# 退出码：0 = 全部硬性检查通过；1 = 有硬性检查失败
#
# 两条纪律：
#   - 只检查环境变量**是否存在**，**不打印取值**（口令不能进日志）；
#   - **不创建任何东西** —— 以 root 跑自检顺手建出来的目录，
#     会让 systemd 里以普通用户运行的采集端写不进去（水位静默丢失）。
# ============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE" || exit 1

CHECK_NETWORK=0
[ "${1:-}" = "--network" ] && CHECK_NETWORK=1

FAILED=0
ok()   { printf '  [ OK ] %s\n' "$*"; }
bad()  { printf '  [FAIL] %s\n' "$*"; FAILED=1; }
warn() { printf '  [WARN] %s\n' "$*"; }

# --------------------------------------------------------------------------
# 0. 读 .env（只看键是否存在，不执行文件里的任何内容，也不打印取值）
#
#    用 systemd 的 EnvironmentFile= 时环境变量已经在进程环境里了；
#    手工跑自检时没有，所以从 .env 里读一份。
#    这里刻意**不 source** —— .env 是客户可编辑的文件，
#    source 等于让 root 执行它的内容，而 systemd 解析 EnvironmentFile 时
#    并不会执行任何命令。行为要对齐，就只能解析不能执行。
# --------------------------------------------------------------------------
load_env_file() {
  local file="$1" line key value
  [ -f "$file" ] || return 0
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      ''|'#'*) continue ;;
      export\ *) line="${line#export }" ;;
      *=*) ;;
      *) continue ;;
    esac
    key="${line%%=*}"
    value="${line#*=}"
    key="$(printf '%s' "$key" | tr -d '[:space:]')"
    value="${value%\"}"; value="${value#\"}"
    value="${value%\'}"; value="${value#\'}"
    [ -n "$key" ] || continue
    # 只在当前进程里还没设置时填充（进程环境优先）
    if [ -z "${!key:-}" ]; then
      export "$key=$value"
    fi
  done < "$file"
  ENV_FILE_LOADED="$file"
}

ENV_FILE_LOADED=""
load_env_file "$HERE/.env"

# --------------------------------------------------------------------------
echo "== 1. 操作系统 =="
ok "发行版：$( (. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME") || echo '未知')"
ok "内核：$(uname -r)"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) ok "架构：$ARCH（x86_64）" ;;
  aarch64|arm64) ok "架构：$ARCH（ARM64）" ;;
  *) warn "架构：$ARCH —— 未验证过（部署包的 wheel 是纯 Python，理论上通用）" ;;
esac

# --------------------------------------------------------------------------
echo "== 2. 安装状态 =="
if [ -f "$HERE/.install-mode" ]; then
  ok "安装模式：$(cat "$HERE/.install-mode")"
else
  warn "没有 .install-mode —— 这是旧版部署包，按目录结构推断"
fi

# --------------------------------------------------------------------------
echo "== 3. Python =="
if [ -x "$HERE/venv/bin/python" ]; then
  PY="$HERE/venv/bin/python"
  MODE="venv"
elif [ -d "$HERE/vendor" ]; then
  PY="$(cat "$HERE/.python-bin" 2>/dev/null || command -v python3)"
  MODE="vendor"
  export PYTHONPATH="$HERE/vendor${PYTHONPATH:+:$PYTHONPATH}"
else
  PY="$(command -v python3 || true)"
  MODE="未安装"
fi

if [ -x "$PY" ]; then
  PY_VER="$("$PY" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])' 2>/dev/null || echo '?')"
  PY_KEY="$(printf '%s' "$PY_VER" | cut -d. -f1,2)"
  if "$PY" -c 'import sys; raise SystemExit(0 if (3,7) <= sys.version_info[:2] < (4,0) else 1)' 2>/dev/null; then
    ok "解释器：$PY（$PY_VER，安装模式：$MODE）"
    if [ "$(printf '%s\n%s\n' "$PY_KEY" "3.12" | sort -V | head -1)" = "3.12" ]; then
      bad "Python $PY_VER 与 kafka-python 2.0.2 不兼容（3.12+ 上 import kafka 会失败）"
    fi
    if [ "$PY_VER" != "3.7.9" ]; then
      warn "不是 3.7.9（唯一验收过的版本），首次上线请多观察一轮日志"
    fi
  else
    bad "Python 版本不满足 >= 3.7（当前 $PY_VER）"
  fi
else
  bad "找不到可用的 Python 解释器"
  exit 1
fi

# --------------------------------------------------------------------------
echo "== 4. 运行时依赖 =="
for mod in requests kafka urllib3; do
  if "$PY" -c "import $mod" >/dev/null 2>&1; then
    ok "$mod $("$PY" -c "import $mod as m; print(getattr(m, '__version__', '?'))" 2>/dev/null)"
  else
    bad "$mod 没装上 —— 重跑 install.sh"
  fi
done

# --------------------------------------------------------------------------
echo "== 5. 生产包 =="
if "$PY" -c 'import tpops_kafka_collector' >/dev/null 2>&1; then
  ok "tpops_kafka_collector 可导入"
else
  bad "tpops_kafka_collector 不可导入 —— 检查 PYTHONPATH / 安装目录"
fi
if [ -f "$HERE/tpops_kafka_collector/mapping/data/metric_rules.json" ]; then
  ok "映射数据在位（mapping/data/*.json）"
else
  bad "映射数据缺失 —— 部署包不完整"
fi

# --------------------------------------------------------------------------
echo "== 6. 必填配置（只看是否存在，不打印取值） =="
if [ -n "$ENV_FILE_LOADED" ]; then
  ok "已从 $ENV_FILE_LOADED 读取配置（只读键，不执行文件内容）"
fi
for var in TPOPS_USER TPOPS_PASSWORD KAFKA_BOOTSTRAP_SERVERS; do
  if [ -n "${!var:-}" ]; then
    ok "$var 已设置"
  else
    bad "$var 未设置（必填）"
  fi
done
for var in TPOPS_BASE_URL KAFKA_TOPIC_METRICS KAFKA_TOPIC_ALARMS; do
  if [ -n "${!var:-}" ]; then
    ok "$var 已设置"
  else
    warn "$var 未设置（用默认值）"
  fi
done

# --------------------------------------------------------------------------
echo "== 7. 状态目录 =="
STATE_DIR="${STATE_DIR:-./state}"
if [ -d "$STATE_DIR" ]; then
  if [ -w "$STATE_DIR" ]; then
    ok "状态目录可写：$STATE_DIR"
  else
    bad "状态目录不可写：$STATE_DIR —— 水位会丢，重启后数据整段重投。
       以 root 跑过本自检的目录属主是 root，服务以普通用户运行时写不进去；
       请 chown 给服务用户，或在 systemd 里用 StateDirectory= 让 systemd 建。"
  fi
else
  warn "状态目录还不存在：$STATE_DIR（首次启动会自动创建；用 systemd 时由 StateDirectory= 建）"
fi

# --------------------------------------------------------------------------
if [ "$CHECK_NETWORK" = "1" ]; then
  echo "== 8. 网络连通性 =="
  tcp_check() {
    if "$PY" - "$1" "$2" <<'EOF'
import socket, sys
host, port = sys.argv[1], int(sys.argv[2])
try:
    s = socket.create_connection((host, port), timeout=5)
    s.close()
except Exception:
    sys.exit(1)
EOF
    then
      ok "$1:$2 可达"
    else
      bad "$1:$2 不可达"
    fi
  }
  if [ -n "${TPOPS_BASE_URL:-}" ]; then
    TPOPS_HOSTPORT="${TPOPS_BASE_URL#*://}"
    TPOPS_HOSTPORT="${TPOPS_HOSTPORT%%/*}"
    TPOPS_HOST="${TPOPS_HOSTPORT%%:*}"
    TPOPS_PORT="${TPOPS_HOSTPORT##*:}"
    [ "$TPOPS_PORT" = "$TPOPS_HOST" ] && TPOPS_PORT=443
    tcp_check "$TPOPS_HOST" "$TPOPS_PORT"
  else
    warn "TPOPS_BASE_URL 未设置，跳过平台连通性检查"
  fi
  if [ -n "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
    IFS=',' read -ra BROKERS <<< "$KAFKA_BOOTSTRAP_SERVERS"
    for b in "${BROKERS[@]}"; do
      b="${b## }"; b="${b%% }"
      tcp_check "${b%%:*}" "${b##*:}"
    done
  else
    warn "KAFKA_BOOTSTRAP_SERVERS 未设置，跳过 broker 连通性检查"
  fi
fi

# --------------------------------------------------------------------------
echo
if [ "$FAILED" = "0" ]; then
  echo "自检通过 —— 可以启动：bash $HERE/run.sh"
  exit 0
fi
echo "自检未通过 —— 见上面 [FAIL] 项"
exit 1
