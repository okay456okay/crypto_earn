#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $(basename $0) token"
    exit 1
fi

token=$1


script_dir=$(dirname $0)

if ! ps auxww|grep $token |grep bitget_funding_arbitrage|grep -v grep; then
  nohup ${script_dir}/../venv/bin/python $script_dir/../trade/bitget_funding_arbitrage.py -s $token/USDT &
fi