#!/usr/bin/env bash


cd $(dirname $0)

source ../venv/bin/activate

python ../trade/gateio_positions.py
python ../trade/bitget_positions.py
python ../trade/bybit_positions.py