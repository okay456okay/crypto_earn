#!/bin/bash

# 脚本名称: gateio_eat_arbitrage.sh
# 延时波动太大了，没法操作


script_dir=$(dirname $0)
ps auxww|grep -v grep|grep  gateio_eat_funding_rate.py|awk '{print $2}'|xargs kill

cd $script_dir
nohup ${script_dir}/../venv/bin/python $script_dir/../trade/gateio_eat_funding_rate.py -t -0.5 -l 10 &>reports/eatgateio-$(date +%Y-%m-%d-%H) &
