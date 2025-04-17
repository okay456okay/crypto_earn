#!/bin/bash

# 脚本名称: funding_binance.sh
# 用途: 启动Binance资金费率套利程序
# 常见用法:
#   ./funding_binance.sh -t BTC        # 启动BTC的资金费率套利
#   ./funding_binance.sh -t BTC -d     # 启用调试模式
# 注意: 该脚本会检查是否已有相同token的套利程序在运行，避免重复启动

if [ $# -ne 1 ]; then
    echo "Usage: $(basename $0) token"
    exit 1
fi

token=$1


script_dir=$(dirname $0)

if ! ps auxww|grep $token |grep -v grep|grep -q binance_funding_arbitrage; then
  nohup ${script_dir}/../venv/bin/python $script_dir/../trade/binance_funding_arbitrage.py -s $token/USDT &>/dev/null &
fi