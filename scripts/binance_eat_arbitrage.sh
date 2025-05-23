#!/bin/bash

# 脚本名称: binance_eat_arbitrage.sh


script_dir=$(dirname $0)
ps auxww|grep -v grep|grep  binance_eat_funding_rate.py|awk '{print $2}'|xargs kill

cd $script_dir
nohup ${script_dir}/../venv/bin/python $script_dir/../trade/binance_eat_funding_rate.py -t -0.5 -l 10 &>reports/eatbinance-$(date +%Y-%m-%d-%H) &
