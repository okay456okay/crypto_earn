#!/bin/bash

# 脚本名称: close_bitget.sh
# 用途: 关闭Gate.io和Bitget之间的对冲仓位
# 常见用法:
#   ./close_bitget.sh -t BTC -n 100 -a 10        # 关闭100个BTC订单，每次关闭10个
#   ./close_bitget.sh -t BTC -n 100 -a 10 -p 0.0002  # 指定价格差异为0.0002
#   ./close_bitget.sh -t BTC -n 100 -a 10 -d     # 启用调试模式
# 注意: 默认价格差异为0.0001，表示在Gate.io平空单，在Bitget平多单

# 显示帮助信息
show_help() {
    echo "Usage: $(basename $0) [OPTIONS]"
    echo "Options:"
    echo "  -t TOKEN     Token symbol (e.g. BTC)"
    echo "  -n TOTAL     Total number of orders to close"
    echo "  -a AMOUNT    Amount per order"
    echo "  -p DIFF      Price difference (default: 0.0001)"
    echo "  -d           Enable debug mode"
    echo "  -h           Show this help message"
    exit 0
}

# 默认参数值
price_diff=0.0001
debug_mode=false

# 使用getopts处理参数
while getopts "t:n:a:p:dh" opt; do
    case $opt in
        t) token="$OPTARG" ;;
        n) total="$OPTARG" ;;
        a) amount="$OPTARG" ;;
        p) price_diff="$OPTARG" ;;
        d) debug_mode=true ;;
        h) show_help ;;
        \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
        :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
    esac
done

# 检查必需参数
if [ -z "$token" ] || [ -z "$total" ] || [ -z "$amount" ]; then
    echo "Error: Missing required parameters"
    show_help
fi

# 计算页数
let 'page=total/amount-1'

script_dir=$(dirname $0)
debug_flag=""
if [ "$debug_mode" = true ]; then
    debug_flag="-d"
fi

for i in $(seq 1 $page); do
    ${script_dir}/../venv/bin/python $script_dir/../trade/gateio_bitget_unhedge.py -s $token/USDT -a $amount -p $price_diff $debug_flag
done