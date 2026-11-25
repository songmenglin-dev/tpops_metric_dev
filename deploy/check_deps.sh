#!/usr/bin/env bash
# ============================================================================
# TPOPS 采集端 —— 离线依赖预检
#
#   bash check_deps.sh            # 在部署包目录里跑（wheels/ 就在旁边）
#   bash check_deps.sh /path/to/pkg
#
# 回答一个问题：**这份离线依赖包，在这台机器上到底装不装得上。**
#
# 做三件事：
#   1. 查 wheel 齐不齐（按 requirements-runtime.txt 逐条对）
#   2. 查架构无关（全部必须是 *-none-any.whl，x86_64 与 aarch64 才通用）
#   3. **真装一遍**到临时目录再删掉 —— 缺 wheel、wheel 损坏、
#      版本冲突、pip 版本太老，全在这一步暴露出来
#
# 纪律：只往临时目录写东西，结束即删；不碰系统 Python、不装进系统
#       site-packages、不改任何配置。
#
# 退出码：0 = 离线依赖可用；1 = 有问题，装之前先解决
# ============================================================================
set -uo pipefail

PKG_DIR="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
cd "$PKG_DIR" || exit 1

FAILED=0
ok()   { printf '  [ OK ] %s\n' "$*"; }
bad()  { printf '  [FAIL] %s\n' "$*"; FAILED=1; }
warn() { printf '  [WARN] %s\n' "$*"; }

WHEELS="$PKG_DIR/wheels"
REQS="$PKG_DIR/requirements-runtime.txt"

echo "== 1. 依赖材料 =="
if [ ! -d "$WHEELS" ]; then
  bad "没有 wheels/ 目录 —— 部署包不完整"
  echo
  echo "预检未通过 —— 离线依赖包缺失。"
  exit 1
fi
if [ ! -f "$REQS" ]; then
  bad "没有 requirements-runtime.txt —— 部署包不完整"
  exit 1
fi
ok "依赖清单：$REQS"

wheel_count="$(find "$WHEELS" -maxdepth 1 -name '*.whl' | wc -l | tr -d ' ')"
bootstrap_count="$(find "$WHEELS/bootstrap" -maxdepth 1 -name '*.whl' 2>/dev/null | wc -l | tr -d ' ')"
ok "运行时 wheel：$wheel_count 个；引导 wheel：$bootstrap_count 个"

if [ "$bootstrap_count" -lt 1 ]; then
  bad "wheels/bootstrap/ 里没有 pip wheel —— 目标机若没有 pip 就装不了"
else
  ok "引导 wheel 就位（目标机没有 pip 时用它离线补上）"
fi

# --------------------------------------------------------------------------
echo "== 2. 架构无关性 =="
platform_specific="$(find "$WHEELS" -name '*.whl' ! -name '*-none-any.whl' 2>/dev/null)"
if [ -n "$platform_specific" ]; then
  bad "出现了平台相关 wheel —— x86_64 与 aarch64 不能共用这一份部署包："
  printf '%s\n' "$platform_specific" | sed 's/^/         /'
else
  ok "全部 wheel 都是纯 Python（*-none-any.whl）—— x86_64 / aarch64 通用"
fi

# --------------------------------------------------------------------------
echo "== 3. 依赖清单逐条核对 =="
missing=0
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
    if find "$WHEELS" -maxdepth 1 -iname "${cand}-*.whl" | grep -q .; then
      found="$cand"
      break
    fi
  done
  if [ -n "$found" ]; then
    ok "$line → 有对应 wheel"
  else
    bad "$line → wheels/ 里找不到对应的 wheel（离线装不上）"
    missing=1
  fi
done < "$REQS"
[ "$missing" -eq 0 ] && ok "清单里的每一条都有离线 wheel"

# --------------------------------------------------------------------------
echo "== 4. 离线安装演练（装到临时目录，随即删除） =="

probe_version() {
  "$1" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])' 2>/dev/null || true
}

PY=""
for c in python3 python3.7 python3.8 python3.9 python3.10 python3.11 python; do
  path="$(command -v "$c" 2>/dev/null || true)"
  [ -n "$path" ] && [ -x "$path" ] || continue
  ver="$(probe_version "$path")"
  case "$ver" in
    3.[7-9]*|3.1[01]*) PY="$path"; break ;;
  esac
done
if [ -z "$PY" ]; then
  bad "没有可用的 Python（>= 3.7 且 < 3.12）—— 装不了依赖"
else
  ok "用 $PY（$(probe_version "$PY")）做演练"
fi

TMP_DIR=""
PIP_PREFIX=()
if [ -n "$PY" ]; then
  if "$PY" -m pip --version >/dev/null 2>&1; then
    PIP_PREFIX=("$PY" -m pip)
    ok "用系统 pip 演练"
  else
    # 系统没有 pip —— 用随包的引导 wheel 在临时目录里跑 pip
    pip_wheel="$(find "$WHEELS/bootstrap" -name 'pip-*.whl' 2>/dev/null | head -1)"
    if [ -n "$pip_wheel" ]; then
      TMP_DIR="$(mktemp -d)"
      if "$PY" -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])' \
          "$pip_wheel" "$TMP_DIR"; then
        PIP_PREFIX=(env "PYTHONPATH=$TMP_DIR" "$PY" -m pip)
        ok "系统没有 pip —— 用随包的引导 wheel 在临时目录里跑 pip"
      else
        bad "引导 wheel 解压失败"
      fi
    else
      bad "系统没有 pip，wheels/bootstrap/ 里也没有引导 wheel"
    fi
  fi
fi

if [ "${#PIP_PREFIX[@]}" -gt 0 ]; then
  TARGET_DIR="$(mktemp -d)"
  echo "  （装到临时目录 $TARGET_DIR，验完即删）"
  if "${PIP_PREFIX[@]}" install --no-index --find-links "$WHEELS" \
      --disable-pip-version-check --target "$TARGET_DIR" -r "$REQS" >/dev/null 2>&1; then
    ok "离线安装演练通过 —— 这份依赖包在这台机器上装得上"
  else
    bad "离线安装演练失败 —— 重跑一次看详细输出："
    "${PIP_PREFIX[@]}" install --no-index --find-links "$WHEELS" \
        --disable-pip-version-check --target "$TARGET_DIR" -r "$REQS" 2>&1 | tail -12 | sed 's/^/         /'
  fi
  rm -rf "$TARGET_DIR"
fi

[ -n "$TMP_DIR" ] && rm -rf "$TMP_DIR"

# --------------------------------------------------------------------------
echo
if [ "$FAILED" = "0" ]; then
  echo "依赖预检通过 —— 可以装了：sudo bash $PKG_DIR/install.sh"
  exit 0
fi
echo "依赖预检未通过 —— 见上面 [FAIL] 项，先解决再装。"
exit 1
