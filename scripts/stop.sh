#!/bin/bash
# greed 一键停止：先停采集（SIGTERM 触发 flush_all 优雅落盘），再停守护与归档。
#
# 顺序有讲究：先删 supervisor.pidfile 阻止自动重启，再 SIGTERM 采集进程让其落盘，
# 最后停归档循环。顺序反了会出现"停了又被拉起来"。
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR="$ROOT/run"

term_from_pidfile() {  # $1=pidfile $2=名字
  local pid
  pid=$(cat "$1" 2>/dev/null) || true
  rm -f "$1"  # 先删 pidfile：supervisor 以此判断"不再重启"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null
    echo "已发送 SIGTERM：$2 (pid $pid)"
    return 0
  fi
  echo "$2 未在运行"
  return 1
}

# 1) 阻止 supervisor 重启（先删标记 pidfile，supervisor 每轮循环检查它）
rm -f "$RUN_DIR/supervisor.pid"

# 2) 停归档 subshell（先停它，避免停止过程中又跑一轮归档）
term_from_pidfile "$RUN_DIR/archive-shell.pid" "归档循环"
rm -f "$RUN_DIR/archive.pid"

# 3) 优雅停采集（flush_all 落盘）
term_from_pidfile "$RUN_DIR/collector.pid" "采集进程" && sleep 2

# 4) 停 supervisor subshell（采集退出后它本会因 pidfile 缺失而自然退出；没退就补一刀）
term_from_pidfile "$RUN_DIR/supervisor-shell.pid" "采集守护" || true

# 兜底：若 pidfile 丢失但进程还在，按命令行特征清理（精确匹配，不误杀）
pkill -f 'target/release/greed collect --config' 2>/dev/null && echo "兜底清理了残留 collect 进程" || true
echo "已停止。采集缓冲已随 SIGTERM 落盘（flush_all）。"