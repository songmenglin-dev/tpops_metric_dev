#!/usr/bin/env bash
# ============================================================================
# TPOPS 采集端 —— 部署前**环境预检**（只读，不装任何东西、不改任何东西）
#
#   bash check_env.sh                # 预检
#   bash check_env.sh --network      # 额外查 TPOPS / Kafka 的 TCP 连通性
#   bash check_env.sh /opt/tpops-collector   # 指定安装目录（默认同 install.sh）
#
# 退出码：0 = 环境满足，可以往下装；1 = 有 FAIL 项，先解决再装
#
# 它回答一个问题：**这台机器能不能装、装了能不能跑起来。**
#   - 架构对不对（x86_64 / aarch64 都支持，但要说清楚）
#   - 有没有**能用的** Python（版本区间、venv 能力、pip 能力）
#   - 装到哪儿、那儿写得下吗、磁盘够吗
#   - 健康端点的端口被占了吗
#
# 纪律：**只读**。不建目录、不装包、不改权限 —— 以 root 跑完预检再让服务以
# 普通用户跑，很容易留下「root 建的目录、服务用户写不进去」的坑（水位静默丢失）。
# ============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE" || exit 1

PREFIX="${1:-${INSTALL_PREFIX:-/opt/tpops-collector}}"
CHECK_NETWORK=0
[ "${1:-}" = "--network" ] && CHECK_NETWORK=1
[ "${2:-}" = "--network" ] && CHECK_NETWORK=1

FAILED=0
ok()   { printf '  [ OK ] %s\n' "$*"; }
bad()  { printf '  [FAIL] %s\n' "$*"; FAILED=1; }
warn() { printf '  [WARN] %s\n' "$*"; }

echo "== 1. 操作系统与架构 =="
ok "发行版：$( (. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME") || echo '未知')"
ok "内核：$(uname -r)"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) ok "架构：$ARCH（x86_64）—— 部署包通用，无需区分" ;;
  aarch64|arm64) ok "架构：$ARCH（ARM64）—— 部署包通用，无需区分" ;;
  *) warn "架构：$ARCH —— 未验证过（依赖全是纯 Python wheel，理论上通用）" ;;
esac

# --------------------------------------------------------------------------
echo "== 2. Python 候选解释器 =="
probe_version() {
  "$1" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])' 2>/dev/null || true
}
ver_ge() {
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -1)" = "$2" ]
}

CANDIDATES=()
TOO_NEW=""
while IFS= read -r line; do
  [ -n "$line" ] || continue
  ver="${line%%$'\t'*}"
  path="${line#*$'\t'}"
  [ -n "$path" ] || continue
  minor="${ver%.*}"
  if ver_ge "$minor" "3.12"; then
    TOO_NEW="${TOO_NEW}${TOO_NEW:+, }$ver ($path)"
    continue
  fi
  CANDIDATES+=("$ver|$path")
done < <(
  for c in python3 python3.7 python3.8 python3.9 python3.10 python3.11 python \
           /usr/bin/python3.* /usr/local/bin/python3.* /opt/*/bin/python3.*; do
    case "$c" in *-config|*-config-*) continue ;; esac
    if command -v "$c" >/dev/null 2>&1; then path="$(command -v "$c")"; else path="$c"; fi
    [ -x "$path" ] || continue
    ver="$(probe_version "$path")"
    [ -n "$ver" ] || continue
    "$path" -c 'import sys; raise SystemExit(0 if (3, 7) <= sys.version_info[:2] < (4, 0) else 1)' \
      >/dev/null 2>&1 || continue
    printf '%s\t%s\n' "$ver" "$path"
  done | sort -t$'\t' -k1,1V -k2,2 | awk '!seen[$1]++'
)

if [ "${#CANDIDATES[@]}" -eq 0 ]; then
  bad "没有可用的 Python（要求 >= 3.7 且 < 3.12）"
  [ -n "$TOO_NEW" ] && bad "找到但太新：$TOO_NEW（3.12+ 与 kafka-python 2.0.2 不兼容）"
  echo
  echo "预检未通过 —— 装之前先解决上面的问题。"
  exit 1
fi

for entry in "${CANDIDATES[@]}"; do
  ver="${entry%%|*}"
  path="${entry#*|}"
  if [ "$ver" = "3.7.9" ]; then
    ok "Python $ver（验收版本）：$path"
  else
    warn "Python $ver：$path"
  fi
done
[ -n "$TOO_NEW" ] && warn "已忽略（3.12+ 与 kafka-python 2.0.2 不兼容）：$TOO_NEW"

CHOSEN="${CANDIDATES[0]}"
CHOSEN_VER="${CHOSEN%%|*}"
CHOSEN_PATH="${CHOSEN#*|}"
ok "安装脚本会选用：$CHOSEN_PATH（$CHOSEN_VER，版本最低的那个，最接近验收环境）"

# --------------------------------------------------------------------------
echo "== 3. 解释器能力（决定装包顺不顺） =="
if "$CHOSEN_PATH" -c 'import venv' >/dev/null 2>&1; then
  ok "venv 模块可用 —— 会装在安装目录内，不碰系统 Python"
else
  bad "没有 venv 模块 —— 请装 python3-venv，或改用 INSTALL_MODE=vendor"
fi
if "$CHOSEN_PATH" -c 'import ensurepip' >/dev/null 2>&1; then
  ok "ensurepip 可用 —— 建 venv 时自带 pip"
else
  warn "ensurepip 不可用 —— 建出来的 venv 没有 pip；安装脚本会用随包的
       wheels/bootstrap/pip-*.whl 离线补上（已演练过，可行）"
fi
if "$CHOSEN_PATH" -m pip --version >/dev/null 2>&1; then
  ok "系统 Python 自带 pip —— vendor 模式也可用"
else
  warn "系统 Python 没有 pip —— vendor 模式不可用，只能走 venv 模式"
fi

# --------------------------------------------------------------------------
echo "== 4. 安装目录 =="
if [ -d "$PREFIX" ]; then
  ok "目录已存在：$PREFIX（重装会覆盖同名文件）"
  if [ -w "$PREFIX" ]; then
    ok "可写"
  else
    bad "不可写 —— 用 root 跑 install.sh，或换个目录"
  fi
else
  parent="$(dirname "$PREFIX")"
  if [ -d "$parent" ] && [ -w "$parent" ]; then
    ok "将创建：$PREFIX（父目录 $parent 可写）"
  elif [ -d "$parent" ]; then
    bad "父目录 $parent 不可写 —— 用 root 跑 install.sh，或换个目录"
  else
    bad "父目录 $parent 不存在 —— 先 mkdir -p 或换个目录"
  fi
fi

# --------------------------------------------------------------------------
echo "== 5. 磁盘空间 =="
need_mb=200
avail_mb="$(df -Pm "$(dirname "$PREFIX")" 2>/dev/null | awk 'NR==2 {print $4}')"
if [ -n "$avail_mb" ] && [ "$avail_mb" -ge "$need_mb" ]; then
  ok "可用空间 ${avail_mb} MB（需要约 ${need_mb} MB）"
elif [ -n "$avail_mb" ]; then
  bad "可用空间只有 ${avail_mb} MB（需要约 ${need_mb} MB）"
else
  warn "读不到磁盘剩余空间（权限不足？）—— 手工确认一下"
fi

# --------------------------------------------------------------------------
echo "== 6. 健康端点端口 =="
HEALTH_PORT="${HEALTH_PORT:-8088}"
if "$CHOSEN_PATH" - "$HEALTH_PORT" <<'EOF' 2>/dev/null
import socket, sys
s = socket.socket()
try:
    s.bind(('127.0.0.1', int(sys.argv[1])))
except OSError:
    sys.exit(1)
finally:
    s.close()
EOF
then
  ok "端口 ${HEALTH_PORT} 可用"
else
  warn "端口 ${HEALTH_PORT} 已被占用 —— 换端口：把 HEALTH_PORT 写进 .env
       （占用不一定影响：先确认占用的不是采集端自己）"
fi

# --------------------------------------------------------------------------
if [ "$CHECK_NETWORK" = "1" ]; then
  echo "== 7. 网络连通性 =="
  tcp_check() {
    if "$CHOSEN_PATH" - "$1" "$2" <<'EOF' 2>/dev/null
import socket, sys
try:
    s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=5)
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
    hostport="${TPOPS_BASE_URL#*://}"; hostport="${hostport%%/*}"
    tcp_check "${hostport%%:*}" "${hostport##*:}"
  else
    warn "TPOPS_BASE_URL 未设置，跳过平台连通性检查"
  fi
  if [ -n "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
    IFS=',' read -ra brokers <<< "$KAFKA_BOOTSTRAP_SERVERS"
    for b in "${brokers[@]}"; do
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
  echo "预检通过 —— 可以装了："
  echo "  sudo bash $HERE/install.sh $PREFIX"
  exit 0
fi
echo "预检未通过 —— 见上面 [FAIL] 项，先解决再装。"
exit 1
