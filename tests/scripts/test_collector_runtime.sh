#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# collector.sh 运行时管理（run 后台化 / status / stop）的测试。
#
# 用一个**假解释器**（会一直睡着的 bash 脚本）代替真的采集端 ——
# 这样不碰 TPOPS、不连 Kafka、不写状态目录，只验脚本行为。
#
#   bash tests/scripts/test_collector_runtime.sh
# ---------------------------------------------------------------------------
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="$(mktemp -d)"
PASS=0
FAIL=0

cleanup() {
  if [ -n "${FAKE_PID:-}" ]; then kill -KILL "$FAKE_PID" 2>/dev/null || true; fi
  rm -rf "$TMP_ROOT"
}
trap cleanup EXIT

ok()   { PASS=$((PASS+1)); printf '  [ OK ] %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  [FAIL] %s\n' "$1"; }
assert_eq() { # 期望 实际 说明
  if [ "$1" = "$2" ]; then ok "$3"; else bad "$3（期望 '$1'，实际 '$2'）"; fi
}

# --- 造一个假的安装目录 ---------------------------------------------------
INSTALL="$TMP_ROOT/install"
mkdir -p "$INSTALL/venv/bin"
cp "$REPO_ROOT/deploy/collector.sh" "$INSTALL/"
echo "venv" > "$INSTALL/.install-mode"
cat > "$INSTALL/venv/bin/python" <<'PYEOF'
#!/usr/bin/env bash
# 假解释器：假装是采集端，其实就是一直睡着
exec sleep 600
PYEOF
chmod +x "$INSTALL/venv/bin/python"

export TPOPS_USER="tester"
export TPOPS_PASSWORD="dummy-password"
export KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:9092"

PID_FILE="$INSTALL/run/collector.pid"

alive() { [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; }

# --- 1. 后台启动 -----------------------------------------------------------
echo "== 1. run 后台启动 =="
out="$(cd "$INSTALL" && bash collector.sh run 2>&1)"; rc=$?
assert_eq 0 "$rc" "run 返回 0（后台启动成功）"
[ -f "$PID_FILE" ] && ok "PID 文件已生成" || bad "PID 文件没生成：$out"
if alive; then ok "进程在后台活着"; else bad "进程没活着：$out"; fi
case "$out" in
  *"日志目录"*) ok "输出里带日志目录" ;;
  *) bad "输出里没有日志目录：$out" ;;
esac
[ -d "$INSTALL/logs" ] && ok "日志目录已创建" || bad "日志目录没创建"

# --- 2. 重复启动要拒绝 -----------------------------------------------------
echo "== 2. 重复启动 =="
out="$(cd "$INSTALL" && bash collector.sh run 2>&1)"; rc=$?
[ "$rc" -ne 0 ] && ok "重复启动返回非零（$rc）" || bad "重复启动居然返回 0"
case "$out" in
  *"已在运行"*) ok "输出说明已在运行" ;;
  *) bad "输出没说明已在运行：$out" ;;
esac

# --- 3. status -------------------------------------------------------------
echo "== 3. status =="
out="$(cd "$INSTALL" && bash collector.sh status 2>&1)"; rc=$?
assert_eq 0 "$rc" "status 返回 0"
case "$out" in
  *"运行中"*) ok "status 说在跑" ;;
  *) bad "status 没说在跑：$out" ;;
esac
case "$out" in
  *"$INSTALL/logs"*) ok "status 带日志目录" ;;
  *) bad "status 没带日志目录：$out" ;;
esac

# --- 4. stop ---------------------------------------------------------------
echo "== 4. stop =="
OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
out="$(cd "$INSTALL" && bash collector.sh stop 2>&1)"; rc=$?
assert_eq 0 "$rc" "stop 返回 0"
sleep 1
if kill -0 "$OLD_PID" 2>/dev/null; then bad "进程还在跑"; else ok "进程已退出"; fi
[ -f "$PID_FILE" ] && bad "PID 文件没清理" || ok "PID 文件已清理"

# --- 5. 前台模式不写 PID 文件 ---------------------------------------------
echo "== 5. run --foreground =="
( cd "$INSTALL" && bash collector.sh run --foreground >/dev/null 2>&1 ) &
FAKE_PID=$!
sleep 2
if kill -0 "$FAKE_PID" 2>/dev/null; then ok "前台进程在跑"; else bad "前台进程没起来"; fi
[ -f "$PID_FILE" ] && bad "前台模式不该写 PID 文件" || ok "前台模式没写 PID 文件"
kill -TERM "$FAKE_PID" 2>/dev/null || true
wait "$FAKE_PID" 2>/dev/null || true
FAKE_PID=""

# --- 6. 缺必填环境变量要退出码 2 -----------------------------------------
echo "== 6. 缺必填环境变量 =="
out="$(cd "$INSTALL" && env -u TPOPS_PASSWORD bash collector.sh run 2>&1)"; rc=$?
assert_eq 2 "$rc" "缺变量返回 2"
case "$out" in
  *"缺少必填环境变量"*) ok "输出说明缺哪个变量" ;;
  *) bad "输出没说明缺变量：$out" ;;
esac

# --- 7. stop 时没有 PID 文件 ----------------------------------------------
echo "== 7. stop 无 PID 文件 =="
rm -f "$PID_FILE"
out="$(cd "$INSTALL" && bash collector.sh stop 2>&1)"; rc=$?
[ "$rc" -ne 0 ] && ok "没有 PID 文件时 stop 返回非零" || bad "没有 PID 文件时 stop 返回 0"

printf '\n通过 %d 项，失败 %d 项\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
