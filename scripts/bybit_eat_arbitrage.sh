#!/bin/bash

# 脚本名称: bybit_eat_arbitrage.sh


script_dir=$(dirname $0)
ps auxww|grep -v grep|grep  bybit_eat_funding_rate.py|awk '{print $2}'|xargs kill

cd $script_dir
nohup ${script_dir}/../venv/bin/python $script_dir/../trade/bybit_eat_funding_rate.py -t -1.5 -l 1000 -a 0.055 &>reports/eatbybit-$(date +%Y-%m-%d-%H) &
