#!/bin/bash

# 脚本名称: open.sh
# 用途: 在Gate.io和Binance/Bybit/Bitget之间执行对冲开仓操作
# 常见用法:
#   ./open_binance.sh -s BTC              # 开50个BTC订单
#   ./open_binance.sh -s BTC -a 10 -c 100 -p -0.0002   # 指定价格差异为-0.0002
#   ./open_binance.sh -s BTC -a 10 -d           # 启用调试模式
# 注意: 默认价格差异为-0.0001，表示在Gate.io开空单，在Binance开多单

# 显示帮助信息
show_help() {
    echo "Usage: $(basename $0) [OPTIONS]"
    echo "Options:"
    echo "  -e EXCHANGE  Exchange Name (e.g. binance/bitget/bybit)"
    echo "  -s TOKEN     Token symbol (e.g. BTC)"
    echo "  -c COUNT     Number of orders to open (default: 50)"
    echo "  -p DIFF      Price difference (default: -0.0001)"
    echo "  -d           Enable debug mode"
    echo "  -h           Show this help message"
    exit 0
}

# 默认参数值
count=50
price_diff=-0.0001
debug_mode=false

# 使用getopts处理参数
while getopts "e:s:c:p:dh" opt; do
    case $opt in
        e) exchange="$OPTARG" ;;
        s) token="$OPTARG" ;;
        c) count="$OPTARG" ;;
        p) price_diff="$OPTARG" ;;
        d) debug_mode=true ;;
        h) show_help ;;
        \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
        :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
    esac
done

# 检查必需参数
if [ -z "$token" ] || [ -z "$exchange" ]; then
    echo "Error: Missing required parameters"
    show_help
fi

script_dir=$(dirname $0)
debug_flag=""
if [ "$debug_mode" = true ]; then
    debug_flag="-d"
fi

if ! ps auxww|grep -v grep|grep "gateio_${exchange}_hedge.py" |grep $token &>/dev/null; then
  nohup ${script_dir}/../venv/bin/python $script_dir/../trade/gateio_${exchange}_hedge.py -s ${token}/USDT -c ${count} -p $price_diff $debug_flag &> $script_dir/../logs/${token}.log &
fi