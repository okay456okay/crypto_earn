#!/bin/bash

# 脚本名称: bybit_anti_arbitrage.sh


script_dir=$(dirname $0)
cd $script_dir
if ! ps auxww|grep -v grep|grep -q bybit_anti_funding_rate.py; then
  nohup ${script_dir}/../venv/bin/python $script_dir/../trade/bybit_anti_funding_rate.py -t -1.0 &>reports/$(date +%Y-%m-%d-%H) &
fi