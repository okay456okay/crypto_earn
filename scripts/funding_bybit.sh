#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $(basename $0) token"
    exit 1
fi

token=$1


script_dir=$(dirname $0)

if ! ps auxww|grep $token |grep -v grep|grep -q bybit_funding_arbitrage; then
  nohup ${script_dir}/../venv/bin/python $script_dir/../trade/bybit_funding_arbitrage.py -s $token/USDT &>/dev/null &
fi