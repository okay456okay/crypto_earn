#!/bin/bash

# 脚本名称: bybit_anti_arbitrage.sh


script_dir=$(dirname $0)
ps auxww|grep -v grep|grep  bybit_anti_funding_rate.py|awk '{print $2}'|xargs kill

cd $script_dir
nohup ${script_dir}/../venv/bin/python $script_dir/../trade/bybit_anti_funding_rate.py -t -1.0 &>reports/antibybit-$(date +%Y-%m-%d-%H) &