import os
import sys
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from high_yield.exchange import ExchangeAPI
from gateio_api import get_earn_positions, get_earn_interest

import logging
from tools.logger import logger
logger.setLevel(logging.ERROR)

def print_earn_info():
    positions = get_earn_positions()
    api = ExchangeAPI()

    # 打印标题
    print("\n" + "="*80)
    print(f"GateIO 理财持仓信息 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)

    # 汇总信息
    total_usdt_value = 0
    total_24h_interest_usdt = 0  # 24小时收益(USDT)
    total_assets = 0
    earn_infos = {}
    total_24h_apy = 0  # 用于计算24小时加权平均年化率

    # 打印每个持仓的详细信息
    print("\n【持仓详情】")
    print(f"{'币种':<6} {'持仓数量':<10} {'持仓价值(USDT)':<13} {'当前价格':<8} {'当前年化率':<7} {'24h收益':<8} {'24h年化率':<10} {'冻结数量':<10} {'可用数量':<12} {'额外奖励':<16} {'自动投资':<10}")
    print("-"*150)

    for p in positions:
        if float(p['curr_amount_usdt']) >= 1:
            earn_info = api.get_gateio_flexible_product(p['asset'])
            earn_infos[p['asset']] = earn_info

            # 计算汇总数据
            curr_amount_usdt = float(p['curr_amount_usdt'])
            total_usdt_value += curr_amount_usdt
            total_assets += 1

            # 计算24小时收益
            interests = get_earn_interest(p['asset'], limit=24)
            interest_24h = sum(float(i['amount']) for i in interests)
            interest_24h_usdt = interest_24h * float(p['price'])
            total_24h_interest_usdt += interest_24h_usdt

            # 计算24小时年化率
            actual_hours = len(interests)  # 实际收益记录的小时数
            if actual_hours > 0 and float(p['curr_amount']) > 0:
                # 按实际小时数计算年化率，然后按比例折算到24小时
                interest_24h_apy = (interest_24h / float(p['curr_amount'])) * (24 / actual_hours) * 365 * 100
            else:
                interest_24h_apy = 0

            # 累加24小时年化率（用于计算加权平均）
            total_24h_apy += interest_24h_apy * curr_amount_usdt

            # 准备额外奖励信息
            award_info = ""
            if p['is_open_award_pool'] == 1 and p['award_asset'] and float(p['ext_award_rate_year']) > 0:
                award_info = f"{p['award_asset']} {float(p['ext_award_rate_year']):.2f}% (上限:{p['ext_award_limit']})"

            # 准备自动投资状态
            auto_invest = "已开启" if p['auto_invest_status'] == 1 else "已关闭"

            # 格式化数值，价格保留6位小数，其他保留2位小数
            curr_amount = float(p['curr_amount'])
            price = float(p['price'])
            apy = float(earn_info['apy'])
            frozen_amount = float(p['frozen_amount'])
            margin_available = float(p['margin_available_amount'])

            # 打印详细信息
            print(f"{p['asset']:<8} {curr_amount:<15.2f} {curr_amount_usdt:<15.2f} {price:<13.6f} {apy:<12.2f} {interest_24h:<12.2f} {interest_24h_apy:<12.2f} {frozen_amount:<14.2f} {margin_available:<12.2f} {award_info:<24} {auto_invest:<10}")

    # 打印汇总信息
    print("\n【汇总信息】")
    print(f"总资产数量: {total_assets} 种")
    print(f"总资产价值: {total_usdt_value:.2f} USDT")
    print(f"近24小时收益: {total_24h_interest_usdt:.2f} USDT")
    print(f"近24小时加权平均年化率: {total_24h_apy/total_usdt_value:.2f}%")

    # 计算平均年化率
    if total_assets > 0:
        total_apy = 0
        for p in positions:
            if float(p['curr_amount_usdt']) >= 1:
                earn_info = earn_infos.get(p['asset'])
                weight = float(p['curr_amount_usdt']) / total_usdt_value
                total_apy += float(earn_info['apy']) * weight
        print(f"当前加权平均年化率: {total_apy:.2f}%")

    print("="*80)

if __name__ == '__main__':
    print_earn_info()