#!/usr/bin/env bash


cd $(dirname $0)
mkdir -p reports

source ../venv/bin/activate

suffix=$(date +%Y%m%d%H%M)
python ../trade/gateio_positions.py >reports/gateio_positions_$suffix.log
python ../trade/bitget_positions.py >reports/bitget_positions_$suffix.log
python ../trade/bybit_positions.py >reports/bybit_positions_$suffix.log

cd reports
cat gateio_positions_$suffix.log bitget_positions_$suffix.log bybit_positions_$suffix.log >positions
cat positions