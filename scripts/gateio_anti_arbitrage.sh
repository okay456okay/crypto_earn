#!/bin/bash

# 脚本名称: gateio_anti_arbitrage.sh


script_dir=$(dirname $0)
ps auxww|grep -v grep|grep  gateio_anti_funding_rate.py|awk '{print $2}'|xargs kill

cd $script_dir
nohup ${script_dir}/../venv/bin/python $script_dir/../trade/gateio_anti_funding_rate.py -t -0.01 -l 6 &>reports/antigateio-$(date +%Y-%m-%d-%H) &
