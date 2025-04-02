#!/bin/bash

token=$1
amount=$2
count=$3
price_diff=$4
if [ $# -ne 3 -o $# -ne 4 ]; then
    echo "Usage: $(basename $0) token amount count [price_diff=0.0001]"
    exit 1
fi

script_dir=$(dirname $0)
for i in $(seq 1 $count); do
    ${script_dir}/../venv/bin/python $script_dir/../trade/gateio_bitget_hedge.py -s $token/USDT -a $amount -p $price_diff -d;
done