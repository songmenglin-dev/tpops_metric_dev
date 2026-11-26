#!/usr/bin/env bash
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
# 依赖：collector.sh 已定义 HERE / log / warn / die / ok / load_env_file。
# ---------------------------------------------------------------------------

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
