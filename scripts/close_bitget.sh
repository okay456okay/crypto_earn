#!/bin/bash

if [ $# -lt 3 ]; then
    echo "Usage: $(basename $0) token total amount [price_diff=0.0001]"
    exit 1
fi

token=$1
total=$2
amount=$3
price_diff=$4

if [ -z "$price_diff" ]; then
  price_diff=0.0001
fi

let 'page=total/amount-1'

script_dir=$(dirname $0)
for i in $(seq 1 $page); do
    ${script_dir}/../venv/bin/python $script_dir/../trade/gateio_bitget_unhedge.py -s $token/USDT -a $amount -p $price_diff ;
done