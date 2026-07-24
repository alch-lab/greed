# 2025 全年（365 天，约 1-1.5 小时）
nohup ./target/release/greed ingest --market perp \
    --from 2026-01-01 --to 2026-07-25 \
    --lake data/lake > logs/ingest-2026.log 2>&1 &

echo "已启动，PID: $!"
tail -f logs/ingest-2026.log    # 实时看进度，Ctrl-C 只是退出查看，不影响后台下载
