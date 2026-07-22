#!/bin/bash
# greed 一键启动：采集 daemon（自动重启）+ 定时归档/清理。
#
# 用法：
#   bash scripts/start.sh           # 启动（已在跑则提示并退出）
#   bash scripts/stop.sh            # 停止（先优雅落盘再全停）
#   tail -f logs/collector.log      # 看采集日志
#   tail -f logs/archive.log        # 看归档日志
#
# 做什么：
#   1. 编译 release（缺二进制时）并启动 `greed collect`，崩溃/断连自动重启（退避 10s）；
#   2. 归档循环：超龄 binlog 用 zstd 压缩（本地保留），超 KEEP_ARCHIVED_DAYS 删除；
#      磁盘剩余低于 DISK_MIN_FREE_GB 时从最老压缩包强制清理（磁盘水位兜底，永不满盘）；
#   3. 全部输出进 logs/，进程号进 run/*.pid。
#
# 调参：改下面五个数即可。

set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# ---- 可配置项 ----
CONFIG="${GREED_CONFIG:-config/base.toml}"
LAKE_DIR="${GREED_LAKE:-data/lake}"
COMPRESS_AFTER_DAYS_TRADES=45      # trades 热窗口（AggDelta 30 天分位数需要 ≥30）
COMPRESS_AFTER_DAYS_BOOK=7         # book/oi 热窗口
KEEP_ARCHIVED_DAYS=180             # 压缩包保留天数，超期删除
DISK_MIN_FREE_GB=5                 # 剩余空间低于此值时从最老压缩包开始强制清理
ARCHIVE_INTERVAL_SECS=3600         # 归档检查频率
# ------------------

LOG_DIR="$ROOT/logs"
RUN_DIR="$ROOT/run"
mkdir -p "$LOG_DIR" "$RUN_DIR"
COLLECTOR_LOG="$LOG_DIR/collector.log"
ARCHIVE_LOG="$LOG_DIR/archive.log"
COLLECTOR_PIDFILE="$RUN_DIR/collector.pid"
ARCHIVE_PIDFILE="$RUN_DIR/archive.pid"
SUPERVISOR_PIDFILE="$RUN_DIR/supervisor.pid"

log() { echo "[$(date '+%F %T')] $*"; }

pid_alive() {  # $1=pidfile
  local pid
  pid=$(cat "$1" 2>/dev/null) || return 1
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

if pid_alive "$SUPERVISOR_PIDFILE"; then
  echo "已在运行（supervisor pid $(cat "$SUPERVISOR_PIDFILE")）。先 bash scripts/stop.sh 再启动。"
  exit 1
fi

# ---- 编译 ----
if [ ! -x "$ROOT/target/release/greed" ]; then
  log "未发现 release 二进制，开始 cargo build --release ..."
  cargo build --release || { echo "编译失败"; exit 1; }
fi

# ---- 采集守护（崩溃自动重启；pidfile 被删即停止重启，实现优雅停机）----
(
  echo "$BASHPID" > "$SUPERVISOR_PIDFILE"
  trap 'rm -f "$SUPERVISOR_PIDFILE"' EXIT
  while true; do
    # stop.sh 删掉 pidfile 即表示要求停止：不再拉起新进程
    if [ ! -f "$SUPERVISOR_PIDFILE" ]; then
      break
    fi
    {
      log "启动采集: greed collect --config $CONFIG"
      "$ROOT/target/release/greed" collect --config "$CONFIG" &
      cpid=$!
      echo "$cpid" > "$COLLECTOR_PIDFILE"
      wait "$cpid"
      code=$?
      rm -f "$COLLECTOR_PIDFILE"
      if [ -f "$SUPERVISOR_PIDFILE" ]; then
        log "采集进程退出（code=$code），10s 后自动重启"
        sleep 10
      else
        log "采集进程退出（code=$code），收到停止信号，不再重启"
      fi
    } >> "$COLLECTOR_LOG" 2>&1
  done
) &
SUPERVISOR_PID=$!
echo "$SUPERVISOR_PID" > "$RUN_DIR/supervisor-shell.pid"
for _ in $(seq 1 20); do [ -s "$SUPERVISOR_PIDFILE" ] && break; sleep 0.2; done

# ---- 归档/清理循环 ----
(
  echo "$BASHPID" > "$ARCHIVE_PIDFILE"
  trap 'rm -f "$ARCHIVE_PIDFILE"' EXIT
  archive_once() {
    # 1) 超龄 binlog → zstd 压缩（--rm：压缩成功后删原件）
    if ! command -v zstd >/dev/null 2>&1; then
      log "警告：未安装 zstd，跳过压缩（apt install zstd / yum install zstd）"
      return
    fi
    find "$LAKE_DIR/trades" -name '*.binlog' -mtime +"$COMPRESS_AFTER_DAYS_TRADES" \
      -exec zstd -T0 -q --rm {} \; 2>/dev/null
    find "$LAKE_DIR/book" "$LAKE_DIR/oi" -name '*.binlog' -mtime +"$COMPRESS_AFTER_DAYS_BOOK" \
      -exec zstd -T0 -q --rm {} \; 2>/dev/null

    # 2) 压缩包超 KEEP_ARCHIVED_DAYS → 删除
    find "$LAKE_DIR" -name '*.binlog.zst' -mtime +"$KEEP_ARCHIVED_DAYS" -delete

    # 3) 磁盘水位保护：剩余空间不足时，从最老的压缩包开始删
    local free_gb
    free_gb=$(df -BG --output=avail "$LAKE_DIR" 2>/dev/null | tail -1 | tr -dc '0-9')
    if [ "${free_gb:-99}" -lt "$DISK_MIN_FREE_GB" ]; then
      log "磁盘剩余 ${free_gb}GB < ${DISK_MIN_FREE_GB}GB，强制清理最老归档"
      find "$LAKE_DIR" -name '*.binlog.zst' -printf '%T@ %p\n' \
        | sort -n | head -20 | cut -d' ' -f2- | xargs -r rm -f
    fi

    log "归档检查完成（trades>${COMPRESS_AFTER_DAYS_TRADES}d / book>${COMPRESS_AFTER_DAYS_BOOK}d 已压缩，>${KEEP_ARCHIVED_DAYS}d 已清理，磁盘剩 ${free_gb:-?}GB）"
  }
  while true; do
    archive_once >> "$ARCHIVE_LOG" 2>&1
    sleep "$ARCHIVE_INTERVAL_SECS"
  done
) &
ARCHIVE_PID=$!
echo "$ARCHIVE_PID" > "$RUN_DIR/archive-shell.pid"
for _ in $(seq 1 20); do [ -s "$ARCHIVE_PIDFILE" ] && break; sleep 0.2; done

echo "已启动：采集守护 pid $SUPERVISOR_PID，归档 pid $ARCHIVE_PID"
echo "采集日志：tail -f $COLLECTOR_LOG"
echo "归档日志：tail -f $ARCHIVE_LOG"
echo "停止：bash scripts/stop.sh"