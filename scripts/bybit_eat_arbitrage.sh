#!/bin/bash

# 脚本名称: bybit_eat_arbitrage.sh


script_dir=$(dirname $0)
ps auxww|grep -v grep|grep  bybit_eat_funding_rate.py|awk '{print $2}'|xargs kill

cd $script_dir
nohup ${script_dir}/../venv/bin/python $script_dir/../trade/bybit_eat_funding_rate.py -t -0.5 -l 10 &>reports/eatbybit-$(date +%Y-%m-%d-%H) &
