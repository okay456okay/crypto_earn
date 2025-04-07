#!/usr/bin/env bash

source ../venv/bin/activate

 python ../trade/gateio_positions.py
 python ../trade/bitget_positions.py
 python ../trade/bybit_positions.py