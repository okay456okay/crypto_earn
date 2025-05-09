#!/usr/bin/env bash

# 脚本名称: positions.sh
# 用途: 查看各交易所的持仓情况
# 常见用法:
#   ./positions.sh        # 查看所有交易所的持仓
#   ./positions.sh -d     # 启用调试模式
# 注意: 该脚本会显示Gate.io、Binance、Bitget和Bybit的持仓信息

cd $(dirname $0)

source ../venv/bin/activate

cd ../trade
mkdir -p reports

suffix=$(date +%Y%m%d%H%M)
#python gateio_positions.py |grep -v 'gateio_positions.py' >reports/gateio_positions_$suffix.log
python bitget_positions.py >reports/bitget_positions_$suffix.log
python bybit_positions.py >reports/bybit_positions_$suffix.log
python binance_positions.py >reports/binance_positions_$suffix.log
python exchange_position_arbitrage.py >reports/positions_sum_$suffix.log

cd reports
cat gateio_positions_$suffix.log bitget_positions_$suffix.log bybit_positions_$suffix.log binance_positions_$suffix.log positions_sum_$suffix.log >positions
cat positions