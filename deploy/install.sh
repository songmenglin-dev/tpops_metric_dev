#!/usr/bin/env bash
# ============================================================================
# TPOPS 采集端 —— 离线安装脚本（目标机**零联网**）
#
#   用法：
#     bash install.sh                       # 装到 /opt/tpops-collector
#     bash install.sh /some/prefix          # 装到指定目录
#     INSTALL_MODE=vendor bash install.sh   # 不用 venv，装到 vendor/ 目录
#
#   环境变量：
#     INSTALL_PREFIX  安装目录（命令行位置参数优先于此）
#     INSTALL_MODE    auto | venv | vendor（默认 auto —— 能建 venv 就建，
#                     建不了就退回 vendor 目录模式）
#
#   幂等：可重复执行。已装好的依赖会被 pip 跳过。
#
#   依赖：本脚本只做「离线安装」，不碰系统包管理器、不写系统目录、
#         不改系统 Python —— 只在安装目录内创建 venv/ 或 vendor/。
# ============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PREFIX="${1:-${INSTALL_PREFIX:-/opt/tpops-collector}}"
MODE="${INSTALL_MODE:-auto}"

log()  { printf '[install] %s\n' "$*"; }
warn() { printf '[install][WARN] %s\n' "$*" >&2; }
die()  { printf '[install][ERROR] %s\n' "$*" >&2; exit 1; }

# --------------------------------------------------------------------------
# 0. 路径归一
#
#    PREFIX 可能来自命令行（`/opt/tpops-collector/` 这种带尾斜杠的写法很常见），
#    也可能是相对路径。不归一的话，`[ "$HERE" != "$PREFIX" ]` 会判不等 →
#    往同一个目录里 cp 自己 → cp 报错 → set -e 直接把安装打断。
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

PREFIX="$(normalize_path "$PREFIX")"
HERE="$(normalize_path "$HERE")"

case "$MODE" in
  auto|venv|vendor) ;;
  *) die "INSTALL_MODE 只支持 auto | venv | vendor（当前 '$MODE'）" ;;
esac

# --------------------------------------------------------------------------
# 1. 找可用的 Python
#
#    规则：
#      - 版本区间 [3.7, 4.0) —— dataclasses 需要 3.7+
#      - 3.12 及以上**直接排除** —— kafka-python 2.0.2 依赖的 six.moves 机制
#        在 3.12 被移除的 find_module API 打断，import kafka 会直接失败
#      - 多个候选时按版本**升序**取用（最接近验收环境 3.7.9），
#        且一个装不成会自动试下一个，而不是直接放弃
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

CANDIDATES=()
FOUND_TOO_NEW=""
while IFS=$'\t' read -r ver path; do
  [ -n "$path" ] || continue
  minor="${ver%.*}"
  if ver_ge "$minor" "3.12"; then
    FOUND_TOO_NEW="${FOUND_TOO_NEW:+$FOUND_TOO_NEW, }$ver ($path)"
    continue
  fi
  CANDIDATES+=("$path")
done < <(list_candidates)

if [ "${#CANDIDATES[@]}" -eq 0 ]; then
  if [ -n "$FOUND_TOO_NEW" ]; then
    die "找到的 Python 都太新：$FOUND_TOO_NEW。
  Python 3.12+ 与 kafka-python 2.0.2 不兼容（3.12 移除了 six 依赖的 find_module API，
  表现为 import kafka 时报 No module named 'kafka.vendor.six.moves'）。
  请改用 Python 3.7 ~ 3.11 的解释器 —— 麒麟 V10 SP2+ 自带的 3.7.9 即可。"
  fi
  die "找不到 Python >= 3.7（且 < 3.12）。
  麒麟 V10 SP2 及以上自带 python3 3.7.9，直接用系统的即可；
  V10 SP1 自带的是 3.6.8，**不满足**（dataclasses 需要 3.7+），
  请先安装 3.7.9，或按《部署运行说明》用源码编译一个。"
fi

# --------------------------------------------------------------------------
# 2. 拷贝产物到安装目录
# --------------------------------------------------------------------------
if [ "$HERE" != "$PREFIX" ]; then
  log "拷贝产物到 $PREFIX ..."
  mkdir -p "$PREFIX"
  for item in tpops_kafka_collector wheels requirements-runtime.txt install.sh run.sh \
              verify_env.sh .env.example MANIFEST.txt; do
    if [ -e "$HERE/$item" ]; then
      cp -a "$HERE/$item" "$PREFIX/"
    fi
  done
  for f in "$HERE"/*.md; do
    if [ -e "$f" ]; then cp -a "$f" "$PREFIX/"; fi
  done
fi

[ -d "$PREFIX/tpops_kafka_collector" ] || die "安装目录里没有 tpops_kafka_collector/ —— 部署包不完整"
[ -d "$PREFIX/wheels" ]              || die "安装目录里没有 wheels/ —— 部署包不完整（离线安装需要 wheel）"
[ -f "$PREFIX/requirements-runtime.txt" ] || die "缺少 requirements-runtime.txt"

WHEELS="$PREFIX/wheels"
REQS="$PREFIX/requirements-runtime.txt"

# pip install 的统一参数：禁止联网、只用本地 wheel、禁用版本自检（那是一次联网调用）
PIP_ARGS=(--no-index --find-links "$WHEELS" --disable-pip-version-check)

# --------------------------------------------------------------------------
# 3. 装依赖（离线）
# --------------------------------------------------------------------------
bootstrap_pip() {
  # 目标机建 venv 时可能没有 pip（麒麟的 python3 常被裁掉 ensurepip）。
  # 离线路径：把随包的 pip wheel 直接解压进 site-packages。
  # （**不用 get-pip.py** —— 它自己要联网去 PyPI 找 pip 包，加 --no-index 反而装不上。）
  local venv_python="$1"
  local site pip_wheel f

  "$venv_python" -m pip --version >/dev/null 2>&1 && return 0

  site="$("$venv_python" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])' 2>/dev/null || true)"
  pip_wheel=""
  for f in "$WHEELS"/bootstrap/pip-*.whl; do
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
  # $1 = 解释器路径
  local py="$1"
  local venv="$PREFIX/venv"

  rm -rf "$venv"
  "$py" -m venv "$venv" 2>/dev/null || "$py" -m venv --without-pip "$venv" || return 1
  [ -x "$venv/bin/python" ] || return 1

  bootstrap_pip "$venv/bin/python" || return 1

  "$venv/bin/python" -m pip install "${PIP_ARGS[@]}" -r "$REQS" || return 1
  return 0
}

install_with_vendor() {
  # 退化方案：目标机没有 venv 模块时，把包装到 vendor/ 目录，
  # 由 run.sh 通过 PYTHONPATH 挂载。不碰系统 site-packages。
  local py="$1"
  local vendor="$PREFIX/vendor"

  "$py" -m pip --version >/dev/null 2>&1 || return 1
  rm -rf "$vendor"
  mkdir -p "$vendor"
  "$py" -m pip install "${PIP_ARGS[@]}" --target "$vendor" -r "$REQS" || return 1
  return 0
}

CHOSEN_PY=""
CHOSEN_MODE=""

try_mode() {
  # $1 = 模式（venv / vendor）
  local mode="$1" py
  for py in "${CANDIDATES[@]}"; do
    log "尝试 $mode 模式，解释器 $py ..."
    if [ "$mode" = "venv" ]; then
      if install_with_venv "$py"; then
        CHOSEN_PY="$py"; CHOSEN_MODE="venv"; return 0
      fi
      warn "$py 上建 venv 失败，试下一个解释器"
    else
      if install_with_vendor "$py"; then
        CHOSEN_PY="$py"; CHOSEN_MODE="vendor"; return 0
      fi
      warn "$py 上装 vendor 失败，试下一个解释器"
    fi
  done
  return 1
}

case "$MODE" in
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

PY_VER="$(probe_version "$CHOSEN_PY")"
log "安装完成：$CHOSEN_MODE 模式，解释器 $CHOSEN_PY（$PY_VER）"
if [ "$PY_VER" != "3.7.9" ]; then
  warn "当前不是 3.7.9（唯一验收过的版本）。代码按 3.7 语法红线编写，向后兼容通常没问题，"
  warn "但首次上线请先跑 verify_env.sh 确认。"
fi

# 记录安装模式与解释器 —— run.sh 靠 .install-mode 决定用哪个解释器，
# 不靠「目录是否存在」猜（半装的 venv 会把好的 vendor 安装盖掉）。
printf '%s\n' "$CHOSEN_MODE" > "$PREFIX/.install-mode"
printf '%s\n' "$CHOSEN_PY" > "$PREFIX/.python-bin"

chmod +x "$PREFIX/install.sh" "$PREFIX/run.sh" "$PREFIX/verify_env.sh" 2>/dev/null || true

log "跑环境自检："
log "  bash $PREFIX/verify_env.sh"
log "启动采集端："
log "  bash $PREFIX/run.sh"
