import os
import sys
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from high_yield.exchange import ExchangeAPI
from gateio_api import get_earn_positions

def print_earn_info():
    positions = get_earn_positions()
    api = ExchangeAPI()

    # 打印标题
    print("\n" + "="*80)
    print(f"GateIO 理财持仓信息 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

    # 汇总信息
    total_usdt_value = 0
    total_interest = 0
    total_assets = 0
    earn_infos = {}

    # 打印每个持仓的详细信息
    print("\n【持仓详情】")
    print(f"{'币种':<6} {'持仓数量':<10} {'持仓价值(USDT)':<13} {'当前价格':<8} {'当前年化率':<7} {'累计收益':<8} {'今日收益':<10} {'冻结数量':<10} {'可用数量':<12} {'额外奖励':<16} {'自动投资':<10}")
    print("-"*150)

    for p in positions:
        if float(p['curr_amount_usdt']) >= 1:
            earn_info = api.get_gateio_flexible_product(p['asset'])
            earn_infos[p['asset']] = earn_info

            # 计算汇总数据
            total_usdt_value += float(p['curr_amount_usdt'])
            total_interest += float(p['interest'])
            total_assets += 1

            # 准备额外奖励信息
            award_info = ""
            if p['is_open_award_pool'] == 1 and p['award_asset'] and float(p['ext_award_rate_year']) > 0:
                award_info = f"{p['award_asset']} {float(p['ext_award_rate_year']):.2f}% (上限:{p['ext_award_limit']})"

            # 准备自动投资状态
            auto_invest = "已开启" if p['auto_invest_status'] == 1 else "已关闭"

            # 格式化数值，价格保留6位小数，其他保留2位小数
            curr_amount = float(p['curr_amount'])
            curr_amount_usdt = float(p['curr_amount_usdt'])
            price = float(p['price'])
            apy = float(earn_info['apy'])
            cumulative = float(p['cumulative'])
            interest = float(p['interest'])
            frozen_amount = float(p['frozen_amount'])
            margin_available = float(p['margin_available_amount'])

            # 打印详细信息
            print(f"{p['asset']:<8} {curr_amount:<15.2f} {curr_amount_usdt:<15.2f} {price:<13.6f} {apy:<12.2f} {cumulative:<12.2f} {interest:<14.2f} {frozen_amount:<14.2f} {margin_available:<12.2f} {award_info:<24} {auto_invest:<10}")

    # 打印汇总信息
    print("\n【汇总信息】")
    print(f"总资产数量: {total_assets} 种")
    print(f"总资产价值: {total_usdt_value:.2f} USDT")
    print(f"今日收益: {total_interest:.2f} USDT")

    # 计算平均年化率
    if total_assets > 0:
        total_apy = 0
        for p in positions:
            if float(p['curr_amount_usdt']) >= 1:
                earn_info = earn_infos.get(p['asset'])
                weight = float(p['curr_amount_usdt']) / total_usdt_value
                total_apy += float(earn_info['apy']) * weight
        print(f"加权平均年化率: {total_apy:.2f}%")

    print("="*80)

if __name__ == '__main__':
    print_earn_info()