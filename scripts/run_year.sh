#!/bin/bash
# 全年 12 月回测：bash run_year.sh <tag> <year> [起始月] [结束月]
set -e
cd "$(dirname "$0")"
tag=$1; year=${2:-2025}; sm=${3:-01}; em=${4:-12}
mkdir -p out/year
for m in $(seq $sm $em); do
  mm=$(printf "%02d" $m)
  case $mm in 01|03|05|07|08|10|12) last=31 ;; 04|06|09|11) last=30 ;; 02) last=28 ;; esac
  ./target/release/greed backtest --market perp \
    --from $year-$mm-01 --to $year-$mm-$last \
    --lake data/lake --strategy config/$tag.toml \
    --cash 100000 --trials 1 --out out/year/$tag-$year-m$mm > out/year/$tag-$year-m$mm.log 2>&1 &
  # 4 路并行
  if [ $(( (10#$m - 10#$sm + 1) % 4 )) -eq 0 ]; then wait; fi
done
wait
