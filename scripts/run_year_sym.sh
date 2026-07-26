#!/bin/bash
# bash run_year_sym.sh <tag> <symbol> <year> [起始月] [结束月]
set -e
cd "$(dirname "$0")"
tag=$1; sym=$2; year=$3; sm=${4:-01}; em=${5:-12}
mkdir -p out/year
for m in $(seq $sm $em); do
  mm=$(printf "%02d" $m)
  case $mm in 01|03|05|07|08|10|12) last=31 ;; 04|06|09|11) last=30 ;; 02) last=28 ;; esac
  ./target/release/greed backtest --symbol $sym --market perp \
    --from $year-$mm-01 --to $year-$mm-$last \
    --lake data/lake --strategy config/$tag.toml \
    --cash 100000 --trials 1 --out out/year/$tag-$sym-$year-m$mm > out/year/$tag-$sym-$year-m$mm.log 2>&1 &
  if [ $(( (10#$m - 10#$sm + 1) % 4 )) -eq 0 ]; then wait; fi
done
wait
