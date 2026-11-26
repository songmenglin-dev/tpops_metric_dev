#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 用 `sh collector.sh ...` 调用时自动改用 bash 重跑。
#
#   sh 在麒麟 / RHEL 上通常是 dash，不支持进程替换（`done < <(...)`）等
#   bash 语法，会报 "syntax error near unexpected token `<'"。
#   下面的 guard 是纯 POSIX 写法，dash 也能执行 —— 它先 exec 成 bash，
#   后面的 bash 语法就不会再被 dash 解析。
# ---------------------------------------------------------------------------
if [ -z "${BASH_VERSION:-}" ]; then
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi
  echo "[ERROR] 本脚本需要 bash（当前 sh 不支持其语法），且系统里找不到 bash" >&2
  exit 3
fi

# ============================================================================
# TPOPS 采集端 —— 部署与运维脚本（一个脚本，按**位置参数**选功能）
#
#   bash collector.sh <命令> [参数...]
#
#   check-env   [安装目录] [--network]   环境预检（只读，不装任何东西）
#   check-deps  [部署包目录]              离线依赖预检（真装一遍到临时目录再删）
#   install     [安装目录]                离线安装：阶段一装包、阶段二部署包
#   verify      [--network]               部署后自检
#   run         [--foreground] [参数...]   启动采集端（默认后台；--foreground 前台）
#   status                                采集端是否在跑（PID / 运行时长 / 日志目录）
#   stop                                  停止后台运行的采集端
#   help                                  本帮助
#
#   环境变量：
#     INSTALL_PREFIX  安装目录（命令行位置参数优先于此）
#     INSTALL_MODE    auto | venv | vendor（默认 auto）
#     STATE_DIR       状态目录（水位文件所在）
#     HEALTH_PORT     健康端点端口
#     LOG_DIR         日志目录（不设则 run 用 <安装目录>/logs；不启动采集端时默认只写 stderr）
#
#   典型顺序（客户机上）：
#     bash collector.sh check-env          # 这台机器能不能装
#     bash collector.sh check-deps         # 这份依赖包装不装得上
#     sudo bash collector.sh install       # 装包 + 部署包
#     bash collector.sh verify             # 装好了没有
#     bash collector.sh run                # 后台启动（返回即起好了）
#     bash collector.sh status             # 看状态
#     bash collector.sh stop               # 停止
#
#   纪律：
#     - check-env / check-deps / verify **不改动系统**（只往临时目录写，结束即删）
#     - install 只写安装目录内部：不碰系统包管理器、不写系统 Python
#     - verify **不创建状态目录**（以 root 跑自检建出来的目录，服务用户写不进去，
#       水位会静默丢失）
# ============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --------------------------------------------------------------------------
# 公共输出helper
# --------------------------------------------------------------------------
log()  { printf '[%s] %s\n' "${LOG_TAG:-deploy}" "$*"; }
warn() { printf '[%s][WARN] %s\n' "${LOG_TAG:-deploy}" "$*" >&2; }
die()  { printf '[%s][ERROR] %s\n' "${LOG_TAG:-deploy}" "$*" >&2; exit 1; }
ok()   { printf '  [ OK ] %s\n' "$*"; }
bad()  { printf '  [FAIL] %s\n' "$*"; FAILED=1; }

# --------------------------------------------------------------------------
# 公共：Python 解释器发现
#
#   规则：
#     - 版本区间 [3.7, 4.0) —— dataclasses 需要 3.7+
#     - 3.12 及以上**直接排除** —— kafka-python 2.0.2 依赖的 six.moves 机制
#       在 3.12 被移除的 find_module API 打断，import kafka 会直接失败
#     - 多个候选时按版本**升序**取用（最接近验收环境 3.7.9）
# --------------------------------------------------------------------------
probe_version() {
  # 安静地取版本号（取不到就输出空）—— 断链的解释器不能刷错误信息
  "$1" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])' 2>/dev/null || true
}

ver_ge() {
  # ver_ge "$1" "$2" —— 版本 $1 >= $2 ?（sort -V 做版本序比较）
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -1)" = "$2" ]
}

list_candidates() {
  # 输出「版本<TAB>路径」行，按版本升序（版本相同则按路径排序）
  local c path ver
  for c in python3 python3.7 python3.8 python3.9 python3.10 python3.11 python \
           /usr/bin/python3.* /usr/local/bin/python3.* /opt/*/bin/python3.*; do
    case "$c" in
      *-config|*-config-*) continue ;;   # python3.12-config 之类不是解释器
    esac
    if command -v "$c" >/dev/null 2>&1; then
      path="$(command -v "$c")"
    else
      path="$c"
    fi
    [ -x "$path" ] || continue
    ver="$(probe_version "$path")"
    [ -n "$ver" ] || continue
    # 只接受 [3.7, 4.0)。**stdout 也要丢掉** —— 解释器在探测时若打印了任何东西，
    # 会混进下面的候选列表，把真候选挤掉。
    "$path" -c 'import sys; raise SystemExit(0 if (3, 7) <= sys.version_info[:2] < (4, 0) else 1)' \
      >/dev/null 2>&1 || continue
    printf '%s\t%s\n' "$ver" "$path"
  done | sort -t$'\t' -k1,1V -k2,2 | awk '!seen[$1]++'
}

# --------------------------------------------------------------------------
# 公共：路径归一
#
#   安装目录可能来自命令行（`/opt/tpops-collector/` 这种带尾斜杠的写法很常见），
#   也可能是相对路径。不归一的话「包目录 == 安装目录」判不等 → 往同一个目录里
#   cp 自己 → cp 报错 → set -e 直接把安装打断。
# --------------------------------------------------------------------------
normalize_path() {
  local p="$1"
  if [ -d "$p" ]; then
    (cd "$p" && pwd -P)
  else
    local parent base
    parent="$(dirname "$p")"
    base="$(basename "$p")"
    if [ -d "$parent" ]; then
      printf '%s/%s' "$(cd "$parent" && pwd -P)" "$base"
    else
      printf '%s' "$p"
    fi
  fi
}

# --------------------------------------------------------------------------
# 公共：TCP 连通性检查
# --------------------------------------------------------------------------
tcp_check() {
  # $1=host $2=port $3=用哪个解释器
  if "$3" - "$1" "$2" <<'EOF' 2>/dev/null
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

# --------------------------------------------------------------------------
# 公共：读取 .env（解析 KEY=VALUE，**不执行**文件里的任何内容）
#
#   采集端只从**进程环境**读配置（config.py 不做 dotenv 解析）。手工启动时
#   环境变量不在进程里，采集端就会报「缺少必填环境变量：TPOPS_USER」。
#   所以 verify 与 run 之前都把 .env 读进进程环境 —— 手工启动与 systemd 的
#   EnvironmentFile= 行为就对齐了。
#
#   刻意**不 source**：.env 是客户可编辑的文件，source 等于让 root 执行
#   它的内容；而 systemd 解析 EnvironmentFile 时并不执行任何命令。
#   行为要对齐，就只能解析不能执行。
# --------------------------------------------------------------------------
ENV_FILE_LOADED=""

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
    # 只在当前进程里还没设置时填充（进程环境优先，与 systemd 一致）
    if [ -z "${!key:-}" ]; then
      export "$key=$value"
    fi
  done < "$file"
  ENV_FILE_LOADED="$file"
}

usage() {
  cat <<'EOF'
TPOPS 采集端 —— 部署与运维脚本

用法：bash collector.sh <命令> [参数...]

  check-env   [安装目录] [--network]   环境预检（只读）
  check-deps  [部署包目录]              离线依赖预检
  install     [安装目录]                离线安装（装包 + 部署包）
  verify      [--network]               部署后自检
  run         [采集端参数...]           启动采集端
  help                                  本帮助

环境变量：INSTALL_PREFIX / INSTALL_MODE / STATE_DIR / HEALTH_PORT
EOF
}

# ==========================================================================
# check-env —— 环境预检（只读）
#
#   回答「这台机器能不能装、装了能不能跑」：
#     架构 / Python 版本区间 / venv·ensurepip·pip 三项能力 /
#     安装目录可写性 / 磁盘空间 / 健康端点端口
#
#   **只读**是硬要求：以 root 跑完预检再让服务以普通用户跑，很容易留下
#   「root 建的目录、服务用户写不进去」的坑（水位静默丢失）。
# ==========================================================================
cmd_check_env() {
  LOG_TAG="check-env"
  FAILED=0
  local prefix="${1:-${INSTALL_PREFIX:-/opt/tpops-collector}}"
  local check_network=0
  shift || true
  [ "${1:-}" = "--network" ] && check_network=1

  echo "== 1. 操作系统与架构 =="
  ok "发行版：$( (. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME") || echo '未知')"
  ok "内核：$(uname -r)"
  local arch
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64) ok "架构：$arch（x86_64）—— 部署包通用，无需区分" ;;
    aarch64|arm64) ok "架构：$arch（ARM64）—— 部署包通用，无需区分" ;;
    *) warn "架构：$arch —— 未验证过（依赖全是纯 Python wheel，理论上通用）" ;;
  esac

  echo "== 2. Python 候选解释器 =="
  local candidates=() too_new="" ver path minor
  while IFS=$'\t' read -r ver path; do
    [ -n "$path" ] || continue
    minor="${ver%.*}"
    if ver_ge "$minor" "3.12"; then
      too_new="${too_new}${too_new:+, }$ver ($path)"
      continue
    fi
    candidates+=("$path")
  done < <(list_candidates)

  if [ "${#candidates[@]}" -eq 0 ]; then
    bad "没有可用的 Python（要求 >= 3.7 且 < 3.12）"
    if [ -n "$too_new" ]; then
      bad "找到但太新：$too_new（3.12+ 与 kafka-python 2.0.2 不兼容）"
    fi
    echo
    echo "预检未通过 —— 装之前先解决上面的问题。"
    return 1
  fi
  for path in "${candidates[@]}"; do
    ver="$(probe_version "$path")"
    if [ "$ver" = "3.7.9" ]; then
      ok "Python $ver（验收版本）：$path"
    else
      warn "Python $ver：$path"
    fi
  done
  [ -n "$too_new" ] && warn "已忽略（3.12+ 与 kafka-python 2.0.2 不兼容）：$too_new"
  local chosen="${candidates[0]}"
  ok "安装脚本会选用：$chosen（$(probe_version "$chosen")，版本最低的那个，最接近验收环境）"

  echo "== 3. 解释器能力（决定装包顺不顺） =="
  if "$chosen" -c 'import venv' >/dev/null 2>&1; then
    ok "venv 模块可用 —— 会装在安装目录内，不碰系统 Python"
  else
    bad "没有 venv 模块 —— 请装 python3-venv，或改用 INSTALL_MODE=vendor"
  fi
  if "$chosen" -c 'import ensurepip' >/dev/null 2>&1; then
    ok "ensurepip 可用 —— 建 venv 时自带 pip"
  else
    warn "ensurepip 不可用 —— 建出来的 venv 没有 pip；安装脚本会用随包的
       wheels/bootstrap/pip-*.whl 离线补上（已演练过，可行）"
  fi
  if "$chosen" -m pip --version >/dev/null 2>&1; then
    ok "系统 Python 自带 pip —— vendor 模式也可用"
  else
    warn "系统 Python 没有 pip —— vendor 模式不可用，只能走 venv 模式"
  fi

  echo "== 4. 安装目录 =="
  if [ -d "$prefix" ]; then
    ok "目录已存在：$prefix（重装会覆盖同名文件）"
    if [ -w "$prefix" ]; then ok "可写"; else bad "不可写 —— 用 root 跑 install，或换个目录"; fi
  else
    local parent
    parent="$(dirname "$prefix")"
    if [ -d "$parent" ] && [ -w "$parent" ]; then
      ok "将创建：$prefix（父目录 $parent 可写）"
    elif [ -d "$parent" ]; then
      bad "父目录 $parent 不可写 —— 用 root 跑 install，或换个目录"
    else
      bad "父目录 $parent 不存在 —— 先 mkdir -p 或换个目录"
    fi
  fi

  echo "== 5. 磁盘空间 =="
  local need_mb=200 avail_mb
  avail_mb="$(df -Pm "$(dirname "$prefix")" 2>/dev/null | awk 'NR==2 {print $4}')"
  if [ -n "$avail_mb" ] && [ "$avail_mb" -ge "$need_mb" ]; then
    ok "可用空间 ${avail_mb} MB（需要约 ${need_mb} MB）"
  elif [ -n "$avail_mb" ]; then
    bad "可用空间只有 ${avail_mb} MB（需要约 ${need_mb} MB）"
  else
    warn "读不到磁盘剩余空间（权限不足？）—— 手工确认一下"
  fi

  echo "== 6. 健康端点端口 =="
  local health_port="${HEALTH_PORT:-8088}"
  if "$chosen" - "$health_port" <<'EOF' 2>/dev/null
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
    ok "端口 ${health_port} 可用"
  else
    warn "端口 ${health_port} 已被占用 —— 换端口：把 HEALTH_PORT 写进 .env
       （占用不一定影响：先确认占用的不是采集端自己）"
  fi

  if [ "$check_network" = "1" ]; then
    echo "== 7. 网络连通性 =="
    if [ -n "${TPOPS_BASE_URL:-}" ]; then
      local hostport
      hostport="${TPOPS_BASE_URL#*://}"; hostport="${hostport%%/*}"
      tcp_check "${hostport%%:*}" "${hostport##*:}" "$chosen"
    else
      warn "TPOPS_BASE_URL 未设置，跳过平台连通性检查"
    fi
    if [ -n "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
      local brokers b
      IFS=',' read -ra brokers <<< "$KAFKA_BOOTSTRAP_SERVERS"
      for b in "${brokers[@]}"; do
        b="${b## }"; b="${b%% }"
        tcp_check "${b%%:*}" "${b##*:}" "$chosen"
      done
    else
      warn "KAFKA_BOOTSTRAP_SERVERS 未设置，跳过 broker 连通性检查"
    fi
  fi

  echo
  if [ "$FAILED" = "0" ]; then
    echo "预检通过 —— 可以装了："
    echo "  sudo bash $HERE/collector.sh install $prefix"
    return 0
  fi
  echo "预检未通过 —— 见上面 [FAIL] 项，先解决再装。"
  return 1
}

# ==========================================================================
# check-deps —— 离线依赖预检
#
#   回答「这份离线依赖包，在这台机器上到底装不装得上」：
#     wheel 齐不齐 / 是否架构无关 / 依赖清单逐条核对 /
#     **真装一遍**到临时目录再删（缺 wheel、wheel 损坏、版本冲突全在这暴露）
# ==========================================================================
cmd_check_deps() {
  LOG_TAG="check-deps"
  FAILED=0
  local pkg_dir="${1:-$HERE}"
  pkg_dir="$(normalize_path "$pkg_dir")"
  local wheels="$pkg_dir/wheels"
  local reqs="$pkg_dir/requirements-runtime.txt"

  echo "== 1. 依赖材料 =="
  if [ ! -d "$wheels" ]; then
    bad "没有 wheels/ 目录 —— 部署包不完整"
    echo; echo "预检未通过 —— 离线依赖包缺失。"
    return 1
  fi
  if [ ! -f "$reqs" ]; then
    bad "没有 requirements-runtime.txt —— 部署包不完整"
    return 1
  fi
  ok "依赖清单：$reqs"
  local wheel_count bootstrap_count
  wheel_count="$(find "$wheels" -maxdepth 1 -name '*.whl' | wc -l | tr -d ' ')"
  bootstrap_count="$(find "$wheels/bootstrap" -maxdepth 1 -name '*.whl' 2>/dev/null | wc -l | tr -d ' ')"
  ok "运行时 wheel：$wheel_count 个；引导 wheel：$bootstrap_count 个"
  if [ "$bootstrap_count" -lt 1 ]; then
    bad "wheels/bootstrap/ 里没有 pip wheel —— 目标机若没有 pip 就装不了"
  else
    ok "引导 wheel 就位（目标机没有 pip 时用它离线补上）"
  fi

  echo "== 2. 架构无关性 =="
  local platform_specific
  platform_specific="$(find "$wheels" -name '*.whl' ! -name '*-none-any.whl' 2>/dev/null)"
  if [ -n "$platform_specific" ]; then
    bad "出现了平台相关 wheel —— x86_64 与 aarch64 不能共用这一份部署包："
    printf '%s\n' "$platform_specific" | sed 's/^/         /'
  else
    ok "全部 wheel 都是纯 Python（*-none-any.whl）—— x86_64 / aarch64 通用"
  fi

  echo "== 3. 依赖清单逐条核对 =="
  local line name cand found missing=0
  while IFS= read -r line; do
    line="${line%%#*}"
    line="$(printf '%s' "$line" | tr -d '[:space:]')"
    [ -n "$line" ] || continue
    case "$line" in -*) continue ;; esac
    name="${line%%[=<>!~ ]*}"
    name="${name%%==*}"
    # wheel 文件名用下划线（kafka_python），依赖清单用连字符（kafka-python）——
    # 两种写法都试一遍
    found=""
    for cand in "$name" "${name//-/_}" "${name//_/-}"; do
      if find "$wheels" -maxdepth 1 -iname "${cand}-*.whl" | grep -q .; then
        found="$cand"; break
      fi
    done
    if [ -n "$found" ]; then
      ok "$line → 有对应 wheel"
    else
      bad "$line → wheels/ 里找不到对应的 wheel（离线装不上）"
      missing=1
    fi
  done < "$reqs"
  [ "$missing" -eq 0 ] && ok "清单里的每一条都有离线 wheel"

  echo "== 4. 离线安装演练（装到临时目录，随即删除） =="
  local py="" c path ver
  for c in python3 python3.7 python3.8 python3.9 python3.10 python3.11 python; do
    path="$(command -v "$c" 2>/dev/null || true)"
    [ -n "$path" ] && [ -x "$path" ] || continue
    ver="$(probe_version "$path")"
    case "$ver" in
      3.[7-9]*|3.1[01]*) py="$path"; break ;;
    esac
  done
  if [ -z "$py" ]; then
    bad "没有可用的 Python（>= 3.7 且 < 3.12）—— 装不了依赖"
  else
    ok "用 $py（$(probe_version "$py")）做演练"
  fi

  local tmp_dir="" pip_prefix=()
  if [ -n "$py" ]; then
    if "$py" -m pip --version >/dev/null 2>&1; then
      pip_prefix=("$py" -m pip)
      ok "用系统 pip 演练"
    else
      # 系统没有 pip —— 用随包的引导 wheel 在临时目录里跑 pip
      local pip_wheel
      pip_wheel="$(find "$wheels/bootstrap" -name 'pip-*.whl' 2>/dev/null | head -1)"
      if [ -n "$pip_wheel" ]; then
        tmp_dir="$(mktemp -d)"
        if "$py" -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])' \
            "$pip_wheel" "$tmp_dir"; then
          pip_prefix=(env "PYTHONPATH=$tmp_dir" "$py" -m pip)
          ok "系统没有 pip —— 用随包的引导 wheel 在临时目录里跑 pip"
        else
          bad "引导 wheel 解压失败"
        fi
      else
        bad "系统没有 pip，wheels/bootstrap/ 里也没有引导 wheel"
      fi
    fi
  fi

  if [ "${#pip_prefix[@]}" -gt 0 ]; then
    local target_dir
    target_dir="$(mktemp -d)"
    echo "  （装到临时目录 $target_dir，验完即删）"
    if "${pip_prefix[@]}" install --no-index --find-links "$wheels" \
        --disable-pip-version-check --target "$target_dir" -r "$reqs" >/dev/null 2>&1; then
      ok "离线安装演练通过 —— 这份依赖包在这台机器上装得上"
    else
      bad "离线安装演练失败 —— 重跑一次看详细输出："
      "${pip_prefix[@]}" install --no-index --find-links "$wheels" \
          --disable-pip-version-check --target "$target_dir" -r "$reqs" 2>&1 \
        | tail -12 | sed 's/^/         /'
    fi
    rm -rf "$target_dir"
  fi

  [ -n "$tmp_dir" ] && rm -rf "$tmp_dir"

  echo
  if [ "$FAILED" = "0" ]; then
    echo "依赖预检通过 —— 可以装了：sudo bash $HERE/collector.sh install"
    return 0
  fi
  echo "依赖预检未通过 —— 见上面 [FAIL] 项，先解决再装。"
  return 1
}

# ==========================================================================
# install —— 离线安装（两个阶段）
#
#   阶段一 · 安装环境需要的包：建 venv → 引导 pip → 离线装运行时依赖
#   阶段二 · 部署采集端包：拷贝 tpops_kafka_collector/ 与配套脚本、清单、样例
# ==========================================================================
cmd_install() {
  LOG_TAG="install"
  local prefix="${1:-${INSTALL_PREFIX:-/opt/tpops-collector}}"
  local mode="${INSTALL_MODE:-auto}"
  case "$mode" in
    auto|venv|vendor) ;;
    *) die "INSTALL_MODE 只支持 auto | venv | vendor（当前 '$mode'）" ;;
  esac
  prefix="$(normalize_path "$prefix")"
  local here
  here="$(normalize_path "$HERE")"

  # ---- 阶段一：安装环境需要的包 ----
  log "===== 阶段一：安装环境需要的包 ====="

  local candidates=() too_new="" ver path minor
  while IFS=$'\t' read -r ver path; do
    [ -n "$path" ] || continue
    minor="${ver%.*}"
    if ver_ge "$minor" "3.12"; then
      too_new="${too_new}${too_new:+, }$ver ($path)"
      continue
    fi
    candidates+=("$path")
  done < <(list_candidates)

  if [ "${#candidates[@]}" -eq 0 ]; then
    if [ -n "$too_new" ]; then
      die "找到的 Python 都太新：$too_new。
  Python 3.12+ 与 kafka-python 2.0.2 不兼容（3.12 移除了 six 依赖的 find_module API，
  表现为 import kafka 时报 No module named 'kafka.vendor.six.moves'）。
  请改用 Python 3.7 ~ 3.11 的解释器 —— 麒麟 V10 SP2+ 自带的 3.7.9 即可。"
    fi
    die "找不到 Python >= 3.7（且 < 3.12）。
  麒麟 V10 SP2 及以上自带 python3 3.7.9，直接用系统的即可；
  V10 SP1 自带的是 3.6.8，**不满足**（dataclasses 需要 3.7+），
  请先安装 3.7.9，或按《部署运行说明》用源码编译一个。"
  fi

  if [ "$here" != "$prefix" ]; then
    log "准备安装材料到 $prefix ..."
    mkdir -p "$prefix"
    for item in wheels requirements-runtime.txt; do
      if [ -e "$here/$item" ]; then
        cp -a "$here/$item" "$prefix/"
      fi
    done
  fi

  local wheels="$prefix/wheels"
  local reqs="$prefix/requirements-runtime.txt"
  [ -d "$wheels" ] || die "安装目录里没有 wheels/ —— 部署包不完整（离线安装需要 wheel）"
  [ -f "$reqs" ] || die "缺少 requirements-runtime.txt"

  # pip install 的统一参数：禁止联网、只用本地 wheel、禁用版本自检（那是一次联网调用）
  local pip_args=(--no-index --find-links "$wheels" --disable-pip-version-check)

  bootstrap_pip() {
    # 目标机建 venv 时可能没有 pip（麒麟的 python3 常被裁掉 ensurepip）。
    # 离线路径：把随包的 pip wheel 直接解压进 site-packages。
    # （**不用 get-pip.py** —— 它自己要联网去 PyPI 找 pip 包，加 --no-index 反而装不上。）
    local venv_python="$1" site pip_wheel f
    "$venv_python" -m pip --version >/dev/null 2>&1 && return 0
    site="$("$venv_python" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])' 2>/dev/null || true)"
    pip_wheel=""
    for f in "$wheels"/bootstrap/pip-*.whl; do
      if [ -e "$f" ]; then pip_wheel="$f"; break; fi
    done
    if [ -n "$site" ] && [ -n "$pip_wheel" ]; then
      log "venv 里没有 pip —— 从离线 wheel 解压安装"
      if "$venv_python" -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])' \
          "$pip_wheel" "$site"; then
        "$venv_python" -m pip --version >/dev/null 2>&1 && return 0
      fi
      warn "从 wheel 解压安装 pip 失败"
    fi
    warn "无法离线安装 pip（wheels/bootstrap/pip-*.whl 缺失或解压失败）"
    warn "请在目标机上先装好 python3-venv / python3-pip 后重跑本脚本"
    return 1
  }

  install_with_venv() {
    local py="$1" venv="$prefix/venv"
    rm -rf "$venv"
    "$py" -m venv "$venv" 2>/dev/null || "$py" -m venv --without-pip "$venv" || return 1
    [ -x "$venv/bin/python" ] || return 1
    bootstrap_pip "$venv/bin/python" || return 1
    "$venv/bin/python" -m pip install "${pip_args[@]}" -r "$reqs" || return 1
    return 0
  }

  install_with_vendor() {
    local py="$1" vendor="$prefix/vendor"
    "$py" -m pip --version >/dev/null 2>&1 || return 1
    rm -rf "$vendor"
    mkdir -p "$vendor"
    "$py" -m pip install "${pip_args[@]}" --target "$vendor" -r "$reqs" || return 1
    return 0
  }

  local chosen_py="" chosen_mode=""
  try_mode() {
    # $1 = 模式（venv / vendor）；候选解释器逐个试，一个装不成就换下一个
    local m="$1" py
    for py in "${candidates[@]}"; do
      log "尝试 $m 模式，解释器 $py ..."
      if [ "$m" = "venv" ]; then
        if install_with_venv "$py"; then chosen_py="$py"; chosen_mode="venv"; return 0; fi
        warn "$py 上建 venv 失败，试下一个解释器"
      else
        if install_with_vendor "$py"; then chosen_py="$py"; chosen_mode="vendor"; return 0; fi
        warn "$py 上装 vendor 失败，试下一个解释器"
      fi
    done
    return 1
  }

  case "$mode" in
    venv)
      try_mode venv || die "venv 模式安装失败（已试过全部候选解释器）"
      ;;
    vendor)
      try_mode vendor || die "vendor 模式安装失败（已试过全部候选解释器）"
      ;;
    auto)
      if ! try_mode venv; then
        warn "venv 模式不可用，退回 vendor 模式"
        try_mode vendor || die "vendor 模式也失败 —— 请检查 python3-venv / python3-pip 是否安装"
      fi
      ;;
  esac

  local py_ver
  py_ver="$(probe_version "$chosen_py")"
  log "环境包安装完成：$chosen_mode 模式，解释器 $chosen_py（$py_ver）"
  if [ "$py_ver" != "3.7.9" ]; then
    warn "当前不是 3.7.9（唯一验收过的版本）。代码按 3.7 语法红线编写，向后兼容通常没问题，"
    warn "但首次上线请先跑 collector.sh verify 确认。"
  fi

  # ---- 阶段二：部署采集端包 ----
  log ""
  log "===== 阶段二：部署采集端包 ====="

  if [ "$here" != "$prefix" ]; then
    for item in tpops_kafka_collector collector.sh .env.example MANIFEST.txt; do
      if [ -e "$here/$item" ]; then
        cp -a "$here/$item" "$prefix/"
      fi
    done
    for f in "$here"/*.md; do
      if [ -e "$f" ]; then cp -a "$f" "$prefix/"; fi
    done
    if [ -e "$here/软件部署指南.pdf" ]; then
      cp -a "$here/软件部署指南.pdf" "$prefix/"
    fi
  fi

  [ -d "$prefix/tpops_kafka_collector" ] || die "安装目录里没有 tpops_kafka_collector/ —— 部署包不完整"

  # 记录安装模式与解释器 —— collector.sh run 靠 .install-mode 决定用哪个解释器，
  # 不靠「目录是否存在」猜（半装的 venv 会把好的 vendor 安装盖掉）。
  printf '%s\n' "$chosen_mode" > "$prefix/.install-mode"
  printf '%s\n' "$chosen_py" > "$prefix/.python-bin"

  chmod +x "$prefix/collector.sh" 2>/dev/null || true

  log "采集端包部署完成：$prefix"
  log ""
  log "接下来："
  log "  1) 配环境变量：vi $prefix/.env   （30 项清单见《部署运行说明》§3）"
  log "  2) 部署后自检：bash $prefix/collector.sh verify"
  log "  3) 启动采集端：bash $prefix/collector.sh run"
  return 0
}

# ==========================================================================
# verify —— 部署后自检
#
#   回答「装好了没有、能不能跑」。**不创建任何东西**。
# ==========================================================================
cmd_verify() {
  LOG_TAG="verify"
  FAILED=0
  local check_network=0
  [ "${1:-}" = "--network" ] && check_network=1
  cd "$HERE" || return 1

  # 读 .env（只看键是否存在，不执行文件里的任何内容，也不打印取值）
  ENV_FILE_LOADED=""
  load_env_file "$HERE/.env"

  echo "== 1. 操作系统 =="
  ok "发行版：$( (. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME") || echo '未知')"
  ok "内核：$(uname -r)"
  local arch
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64) ok "架构：$arch（x86_64）" ;;
    aarch64|arm64) ok "架构：$arch（ARM64）" ;;
    *) warn "架构：$arch —— 未验证过（部署包的 wheel 是纯 Python，理论上通用）" ;;
  esac

  echo "== 2. 安装状态 =="
  if [ -f "$HERE/.install-mode" ]; then
    ok "安装模式：$(cat "$HERE/.install-mode")"
  else
    warn "没有 .install-mode —— 这是旧版部署包，按目录结构推断"
  fi

  echo "== 3. Python =="
  local py="" mode
  if [ -x "$HERE/venv/bin/python" ]; then
    py="$HERE/venv/bin/python"; mode="venv"
  elif [ -d "$HERE/vendor" ]; then
    py="$(cat "$HERE/.python-bin" 2>/dev/null || command -v python3)"; mode="vendor"
    export PYTHONPATH="$HERE/vendor${PYTHONPATH:+:$PYTHONPATH}"
  else
    py="$(command -v python3 || true)"; mode="未安装"
  fi

  if [ -x "$py" ]; then
    local py_ver py_key
    py_ver="$("$py" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])' 2>/dev/null || echo '?')"
    py_key="$(printf '%s' "$py_ver" | cut -d. -f1,2)"
    if "$py" -c 'import sys; raise SystemExit(0 if (3,7) <= sys.version_info[:2] < (4,0) else 1)' 2>/dev/null; then
      ok "解释器：$py（$py_ver，安装模式：$mode）"
      if ver_ge "$py_key" "3.12"; then
        bad "Python $py_ver 与 kafka-python 2.0.2 不兼容（3.12+ 上 import kafka 会失败）"
      fi
      if [ "$py_ver" != "3.7.9" ]; then
        warn "不是 3.7.9（唯一验收过的版本），首次上线请多观察一轮日志"
      fi
    else
      bad "Python 版本不满足 >= 3.7（当前 $py_ver）"
    fi
  else
    bad "找不到可用的 Python 解释器"
    return 1
  fi

  echo "== 4. 运行时依赖 =="
  local mod
  for mod in requests kafka urllib3 loguru; do
    if "$py" -c "import $mod" >/dev/null 2>&1; then
      ok "$mod $("$py" -c "import $mod as m; print(getattr(m, '__version__', '?'))" 2>/dev/null)"
    else
      bad "$mod 没装上 —— 重跑 collector.sh install"
    fi
  done

  echo "== 5. 生产包 =="
  if "$py" -c 'import tpops_kafka_collector' >/dev/null 2>&1; then
    ok "tpops_kafka_collector 可导入"
  else
    bad "tpops_kafka_collector 不可导入 —— 检查 PYTHONPATH / 安装目录"
  fi
  if [ -f "$HERE/tpops_kafka_collector/mapping/data/metric_rules.json" ]; then
    ok "映射数据在位（mapping/data/*.json）"
  else
    bad "映射数据缺失 —— 部署包不完整"
  fi

  echo "== 6. 必填配置（只看是否存在，不打印取值） =="
  if [ -n "$ENV_FILE_LOADED" ]; then
    ok "已从 $ENV_FILE_LOADED 读取配置（只读键，不执行文件内容）—— 启动时用的是同一份"
  else
    warn "没找到 .env —— 启动时环境变量必须由进程环境提供（systemd 的 EnvironmentFile=）"
  fi
  local var
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

  echo "== 7. 状态目录 =="
  local state_dir="${STATE_DIR:-./state}"
  if [ -d "$state_dir" ]; then
    if [ -w "$state_dir" ]; then
      ok "状态目录可写：$state_dir"
    else
      bad "状态目录不可写：$state_dir —— 水位会丢，重启后数据整段重投。
       以 root 跑过自检的目录属主是 root，服务以普通用户运行时写不进去；
       请 chown 给服务用户，或在 systemd 里用 StateDirectory= 让 systemd 建。"
    fi
  else
    warn "状态目录还不存在：$state_dir（首次启动会自动创建；用 systemd 时由 StateDirectory= 建）"
  fi

  if [ "$check_network" = "1" ]; then
    echo "== 8. 网络连通性 =="
    if [ -n "${TPOPS_BASE_URL:-}" ]; then
      local hostport
      hostport="${TPOPS_BASE_URL#*://}"; hostport="${hostport%%/*}"
      tcp_check "${hostport%%:*}" "${hostport##*:}" "$py"
    else
      warn "TPOPS_BASE_URL 未设置，跳过平台连通性检查"
    fi
    if [ -n "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
      local brokers b
      IFS=',' read -ra brokers <<< "$KAFKA_BOOTSTRAP_SERVERS"
      for b in "${brokers[@]}"; do
        b="${b## }"; b="${b%% }"
        tcp_check "${b%%:*}" "${b##*:}" "$py"
      done
    else
      warn "KAFKA_BOOTSTRAP_SERVERS 未设置，跳过 broker 连通性检查"
    fi
  fi

  echo
  if [ "$FAILED" = "0" ]; then
    echo "自检通过 —— 可以启动：bash $HERE/collector.sh run"
    [ -n "$ENV_FILE_LOADED" ] && echo "（启动时会自动加载同一份 .env）"
    return 0
  fi
  echo "自检未通过 —— 见上面 [FAIL] 项"
  return 1
}

# ==========================================================================
# run —— 启动采集端
#
#   两种安装模式都能用（按 .install-mode 判断，不靠「目录在不在」猜：
#   装了一半的 venv 会盖掉装好了的 vendor）。
# ==========================================================================
# ==========================================================================
# 运行时管理（run / status / stop）
#
# 部署资产**只有一个脚本**（守卫：deploy/ 下只能有一个 .sh）—— 功能用位置参数选。
# ==========================================================================

# ---------------------------------------------------------------------------
# 采集端的**运行时**管理（run / status / stop）—— 被 collector.sh source 进来。
#
#   run [--foreground] [采集端参数...]
#       默认**后台**启动：写 PID 文件、脱离终端、日志落盘；
#       --foreground 则前台运行（systemd 单元用这个）。
#   status   报告是否在跑、PID、运行时长、日志目录
#   stop     按 PID 文件优雅停止（SIGTERM）
#
# 为什么要拆出来：collector.sh 已经 930 行（历史债务，另案），
# 再往里塞 run/status/stop 会更长 —— 运行时代码单独成文件，各自都短。
#

#: 运行时目录（PID 文件、console.out 放这儿）
RUNTIME_DIR="run"
#: 日志目录名（LOG_DIR 未设置时的默认值，位于安装目录下）
RUNTIME_LOG_DIR_NAME="logs"

# ---------------------------------------------------------------------------
# 路径
# ---------------------------------------------------------------------------
runtime_dir()      { printf '%s/%s' "$HERE" "$RUNTIME_DIR"; }
runtime_pid_file() { printf '%s/%s/collector.pid' "$HERE" "$RUNTIME_DIR"; }
runtime_console()  { printf '%s/%s/console.out' "$HERE" "$RUNTIME_DIR"; }

#: 日志目录：LOG_DIR 优先，否则 <安装目录>/logs
runtime_log_dir() {
  if [ -n "${LOG_DIR:-}" ]; then
    printf '%s' "$LOG_DIR"
  else
    printf '%s/%s' "$HERE" "$RUNTIME_LOG_DIR_NAME"
  fi
}

# ---------------------------------------------------------------------------
# 解释器解析（不 exec，只把命令备好）
# ---------------------------------------------------------------------------
# 解析采集端该用哪个解释器。结果写进全局变量：
#   RUNTIME_PY            —— 解释器路径
#   RUNTIME_PYTHONPATH    —— 需要加进 PYTHONPATH 的目录（vendor 模式）
# 失败时直接退出（退出码 3 = 安装坏了）。
runtime_resolve_python() {
  local mode="" py=""
  if [ -f "$HERE/.install-mode" ]; then
    mode="$(cat "$HERE/.install-mode")"
  fi

  case "$mode" in
    venv)
      if [ ! -x "$HERE/venv/bin/python" ]; then
        echo "[run][ERROR] 安装模式是 venv，但 $HERE/venv/bin/python 不存在 —— 装坏了，重跑 collector.sh install" >&2
        exit 3
      fi
      RUNTIME_PY="$HERE/venv/bin/python"
      RUNTIME_PYTHONPATH=""
      return 0
      ;;
    vendor)
      if [ -f "$HERE/.python-bin" ]; then py="$(cat "$HERE/.python-bin")"; fi
      [ -x "$py" ] || py="$(command -v python3 || true)"
      if [ ! -x "$py" ]; then
        echo "[run][ERROR] 找不到可用的 Python 解释器（vendor 模式）" >&2
        exit 3
      fi
      RUNTIME_PY="$py"
      RUNTIME_PYTHONPATH="$HERE/vendor"
      return 0
      ;;
  esac

  # --- 兼容旧包：没有 .install-mode 时按目录判断 ---
  if [ -x "$HERE/venv/bin/python" ]; then
    RUNTIME_PY="$HERE/venv/bin/python"
    RUNTIME_PYTHONPATH=""
    return 0
  fi
  if [ -d "$HERE/vendor" ]; then
    if [ -f "$HERE/.python-bin" ]; then py="$(cat "$HERE/.python-bin")"; fi
    [ -x "$py" ] || py="$(command -v python3 || true)"
    if [ ! -x "$py" ]; then
      echo "[run][ERROR] 找不到可用的 Python 解释器（vendor 模式）" >&2
      exit 3
    fi
    RUNTIME_PY="$py"
    RUNTIME_PYTHONPATH="$HERE/vendor"
    return 0
  fi

  echo "[run][ERROR] 既没有 venv/ 也没有 vendor/ —— 安装没成功，请重跑 collector.sh install" >&2
  exit 3
}

# ---------------------------------------------------------------------------
# 必填环境变量检查（与旧 cmd_run 一致：缺了就退出码 2）
# ---------------------------------------------------------------------------
runtime_check_env() {
  ENV_FILE_LOADED=""
  load_env_file "$HERE/.env"

  local missing=() var
  for var in TPOPS_USER TPOPS_PASSWORD KAFKA_BOOTSTRAP_SERVERS; do
    [ -n "${!var:-}" ] || missing+=("$var")
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    echo "[run][ERROR] 缺少必填环境变量：${missing[*]}" >&2
    if [ -z "$ENV_FILE_LOADED" ]; then
      echo "[run] 安装目录下没有 .env（找的是 $HERE/.env）—— 请创建它并填上必填项，" >&2
      echo "[run] 或者用 systemd 时在 unit 里配 EnvironmentFile=。" >&2
    else
      echo "[run] 已从 $ENV_FILE_LOADED 读取，但里面缺这些键 —— 请检查该文件。" >&2
    fi
    exit 2
  fi
}

# ---------------------------------------------------------------------------
# PID 文件里的进程是否还活着
# ---------------------------------------------------------------------------
runtime_pid_alive() {
  local pid="$1"
  [ -n "$pid" ] || return 1
  case "$pid" in (*[!0-9]*) return 1 ;; esac
  kill -0 "$pid" 2>/dev/null
}

runtime_read_pid() {
  local file
  file="$(runtime_pid_file)"
  [ -f "$file" ] || return 1
  cat "$file" 2>/dev/null | tr -d '[:space:]'
}

# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------
cmd_run() {
  LOG_TAG="run"
  cd "$HERE" || exit 3

  # --foreground 是**唯一**被脚本自己消费的参数，其余原样传给采集端。
  local foreground=0
  if [ "${1:-}" = "--foreground" ]; then
    foreground=1
    shift
  fi

  runtime_check_env
  runtime_resolve_python

  if [ "$foreground" -eq 1 ]; then
    # --- 前台：systemd 用这个（进程必须留在前台，systemd 才管得住） ---
    if [ -n "$RUNTIME_PYTHONPATH" ]; then
      export PYTHONPATH="$RUNTIME_PYTHONPATH${PYTHONPATH:+:$PYTHONPATH}"
    fi
    exec "$RUNTIME_PY" -m tpops_kafka_collector.main "$@"
  fi

  # --- 后台 ---
  local pid_file console log_dir
  pid_file="$(runtime_pid_file)"
  console="$(runtime_console)"
  log_dir="$(runtime_log_dir)"

  # 已有实例在跑 → 拒绝启动
  local old_pid=""
  old_pid="$(runtime_read_pid || true)"
  if runtime_pid_alive "$old_pid"; then
    echo "[run][ERROR] 采集端已在运行（PID $old_pid）—— 不要重复启动。" >&2
    echo "[run] 要看状态用：bash collector.sh status" >&2
    exit 1
  fi
  # 残留的 PID 文件（进程已死）→ 清掉
  [ -f "$pid_file" ] && rm -f "$pid_file"

  mkdir -p "$RUNTIME_DIR" 2>/dev/null || {
    echo "[run][ERROR] 运行时目录建不出来：$HERE/$RUNTIME_DIR" >&2
    exit 3
  }
  mkdir -p "$log_dir" 2>/dev/null || {
    echo "[run][ERROR] 日志目录建不出来：$log_dir" >&2
    exit 3
  }
  if [ ! -w "$log_dir" ]; then
    echo "[run][ERROR] 日志目录不可写：$log_dir" >&2
    echo "[run] 采集端不会静默降级成「只写 stderr」—— 请把该目录的属主改成服务用户。" >&2
    exit 2
  fi

  # 采集端自己也要知道日志目录（LOG_DIR 未设置时它只写 stderr）
  export LOG_DIR="$log_dir"
  # vendor 模式：采集端在 <安装目录>/vendor 里，解释器要找得到它
  if [ -n "$RUNTIME_PYTHONPATH" ]; then
    export PYTHONPATH="$RUNTIME_PYTHONPATH${PYTHONPATH:+:$PYTHONPATH}"
  fi

  # 启动器：先把自己的 PID 写进 PID 文件，再 exec 成采集端 ——
  # 这样 PID 文件里一定是**采集端本体**的 PID（exec 不换 PID），
  # 不受 setsid 是否 fork 的影响。
  local launcher='echo $$ > "$1"; shift; exec "$@"'
  if command -v setsid >/dev/null 2>&1; then
    setsid nohup bash -c "$launcher" _ "$pid_file" "$RUNTIME_PY" -m tpops_kafka_collector.main "$@" \
      >>"$console" 2>&1 &
  else
    # 没有 setsid（极少见）：nohup 仍能挡住 SIGHUP
    nohup bash -c "$launcher" _ "$pid_file" "$RUNTIME_PY" -m tpops_kafka_collector.main "$@" \
      >>"$console" 2>&1 &
  fi

  # 等它把 PID 写出来并活下来
  local pid="" i
  for i in 1 2 3 4 5 6 7 8 9 10; do
    sleep 0.3
    pid="$(runtime_read_pid || true)"
    if [ -n "$pid" ] && runtime_pid_alive "$pid"; then
      break
    fi
    pid=""
  done

  if [ -z "$pid" ]; then
    echo "[run][ERROR] 采集端启动后没有存活 —— 下面是最后的输出：" >&2
    echo "---- $console ----" >&2
    tail -n 20 "$console" 2>/dev/null >&2 || true
    echo "---- $log_dir/collector-*.log ----" >&2
    tail -n 20 "$log_dir"/collector-*.log 2>/dev/null >&2 || true
    exit 4
  fi

  log "已在后台启动（PID $pid）"
  log "  日志目录：$log_dir"
  log "  总日志  ：$log_dir/collector-<日期>.log"
  log "  通道日志：$log_dir/collector-<通道名>-<日期>.log"
  log "  启动输出：$console"
  log "  停止    ：bash collector.sh stop"
}

# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
cmd_status() {
  LOG_TAG="status"
  local pid_file pid log_dir
  pid_file="$(runtime_pid_file)"
  log_dir="$(runtime_log_dir)"

  echo "== 采集端状态 =="
  echo "  安装目录：$HERE"
  echo "  日志目录：$log_dir"

  if [ ! -f "$pid_file" ]; then
    echo "  状态    ：未运行（没有 PID 文件）"
    return 1
  fi

  pid="$(runtime_read_pid || true)"
  if ! runtime_pid_alive "$pid"; then
    echo "  状态    ：未运行（PID 文件残留，进程已退出）"
    echo "  PID 文件：$pid_file（PID $pid）"
    return 1
  fi

  local elapsed="未知"
  if [ -r "/proc/$pid/stat" ]; then
    local start_ticks now_ticks hz
    hz="$(getconf CLK_TCK 2>/dev/null || echo 100)"
    start_ticks="$(awk '{print $22}' "/proc/$pid/stat" 2>/dev/null || echo '')"
    now_ticks="$(awk '{printf "%d", $1}' /proc/uptime 2>/dev/null || echo '')"
    if [ -n "$start_ticks" ] && [ -n "$now_ticks" ]; then
      elapsed="$(( now_ticks - start_ticks / hz ))秒"
    fi
  fi

  echo "  状态    ：运行中"
  echo "  PID     ：$pid"
  echo "  运行时长：$elapsed"
  echo "  PID 文件：$pid_file"
  return 0
}

# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------
cmd_stop() {
  LOG_TAG="stop"
  local pid_file pid
  pid_file="$(runtime_pid_file)"

  if [ ! -f "$pid_file" ]; then
    echo "[stop] 没有 PID 文件 —— 采集端没在后台跑（若在 systemd 下请用 systemctl stop）" >&2
    return 1
  fi

  pid="$(runtime_read_pid || true)"
  if ! runtime_pid_alive "$pid"; then
    echo "[stop] 进程已不在（PID $pid）—— 清掉残留的 PID 文件"
    rm -f "$pid_file"
    return 0
  fi

  kill -TERM "$pid" 2>/dev/null || {
    echo "[stop] 发 SIGTERM 失败（PID $pid）" >&2
    return 1
  }
  echo "[stop] 已发 SIGTERM 给 PID $pid，等它优雅退出……"

  local i
  for i in $(seq 1 30); do
    sleep 1
    if ! runtime_pid_alive "$pid"; then
      rm -f "$pid_file"
      echo "[stop] 已停止"
      return 0
    fi
  done

  echo "[stop] 30 秒还没退出 —— 发 SIGKILL" >&2
  kill -KILL "$pid" 2>/dev/null || true
  rm -f "$pid_file"
  echo "[stop] 已强制停止"
  return 0
}

# ==========================================================================
# 分发
# ==========================================================================
main() {
  local cmd="${1:-help}"
  if [ "$#" -gt 0 ]; then shift; fi
  case "$cmd" in
    check-env)  cmd_check_env "$@" ;;
    check-deps) cmd_check_deps "$@" ;;
    install)    cmd_install "$@" ;;
    verify)     cmd_verify "$@" ;;
    run)        cmd_run "$@" ;;
    status)     cmd_status "$@" ;;
    stop)       cmd_stop "$@" ;;
    help|-h|--help) usage ;;
    *) warn "未知命令：$cmd"; usage; return 2 ;;
  esac
}

main "$@"
