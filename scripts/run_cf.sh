#!/bin/bash
# bash run_cf.sh <tag> <symbol> <year> <月列表...>（4 路并行）
set -e
cd "$(dirname "$0")"
tag=$1; sym=$2; year=$3; shift 3
i=0
for mm in "$@"; do
  case $mm in 01|03|05|07|08|10|12) last=31 ;; 04|06|09|11) last=30 ;; 02) last=28 ;; esac
  ./target/release/greed backtest --symbol $sym --market perp \
    --from $year-$mm-01 --to $year-$mm-$last \
    --lake data/lake --strategy config/$tag.toml \
    --cash 100000 --trials 1 --out out/cf/$tag-$sym-$year-m$mm > out/cf/$tag-$sym-$year-m$mm.log 2>&1 &
  i=$((i+1))
  if [ $((i % 4)) -eq 0 ]; then wait; fi
done
wait
