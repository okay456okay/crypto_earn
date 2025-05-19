#!/bin/bash

# 脚本名称: close.sh
# 用途: 关闭Gate.io和Binance/Bybit/Bitget之间的对冲仓位
# 常见用法:
#   ./close_binance.sh -s BTC       # 关闭100个BTC订单，每次关闭10个
#   ./close_binance.sh -s BTC -p 0.0002  # 指定价格差异为0.0002
# 注意: 默认价格差异为0.0001，表示在Gate.io平空单，在Binance平多单

exit

# 显示帮助信息
show_help() {
    echo "Usage: $(basename $0) [OPTIONS]"
    echo "Options:"
    echo "  -e EXCHANGE  Exchange name (e.g. binance, bybit, bitget)"
    echo "  -s TOKEN     Token symbol (e.g. BTC)"
    echo "  -p DIFF      Price difference (default: 0.0001)"
    echo "  -d           Enable debug mode"
    echo "  -h           Show this help message"
    exit 0
}

# 默认参数值
price_diff=0.003
debug_mode=false

# 使用getopts处理参数
while getopts "e:s:p:dh" opt; do
    case $opt in
        e) exchange="$OPTARG" ;;
        s) token="$OPTARG" ;;
        p) price_diff="$OPTARG" ;;
        d) debug_mode=true ;;
        h) show_help ;;
        \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
        :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
    esac
done

# 检查必需参数
if [ -z "$token" -o -z "$exchange" ]; then
    echo "Error: Missing required parameters"
    show_help
fi

script_dir=$(dirname $0)
debug_flag=""
if [ "$debug_mode" = true ]; then
    debug_flag="-d"
fi

if ! ps auxww|grep -v grep| grep "gateio_${exchange}_unhedge.py" |grep $token &>/dev/null; then
  nohup ${script_dir}/../venv/bin/python $script_dir/../trade/gateio_${exchange}_unhedge.py -s ${token}/USDT -p $price_diff $debug_flag -c 2 &>> $script_dir/../logs/${token}_close.log &
fi