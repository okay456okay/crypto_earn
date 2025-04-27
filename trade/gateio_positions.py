import os
import sys
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from high_yield.exchange import ExchangeAPI
from gateio_api import get_earn_positions, get_earn_interest, subscrible_earn
from config import gateio_api_key, gateio_api_secret, proxies

import logging
import ccxt
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
    print(f"{'币种':<6} {'持仓数量':<10} {'持仓价值(USDT)':<13} {'当前价格':<8} {'当前年化率':<7} {'24h收益':<8} {'24h年化率':<10} {'冻结数量':<10} {'可用数量':<12}")
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

            # 格式化数值，价格保留6位小数，其他保留2位小数
            curr_amount = float(p['curr_amount'])
            price = float(p['price'])
            apy = float(earn_info['apy'])
            frozen_amount = float(p['frozen_amount'])
            margin_available = float(p['margin_available_amount'])

            # 打印详细信息
            print(f"{p['asset']:<8} {curr_amount:<15.2f} {curr_amount_usdt:<15.2f} {price:<13.6f} {apy:<12.2f} {interest_24h:<12.2f} {interest_24h_apy:<12.2f} {frozen_amount:<14.2f} {margin_available:<12.2f}")

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

def auto_subscribe_earn():
    """
    自动查询所有现货代币并将满足条件的代币申购理财
    
    筛选条件:
    1. 排除GT2、USDT
    2. 现货余额大于1USDT
    3. 该token开启了自动赚币(auto_invest_status=1)
    """
    # 设置日志级别
    logger.setLevel(logging.INFO)
    logger.info(f"开始扫描 GateIO 现货资产，寻找符合条件的代币进行理财申购")
    logger.info(f"排除列表: GT2, USDT")
    logger.info("="*50)
    
    # 获取当前持仓信息，用于检查auto_invest_status
    earn_positions = get_earn_positions()
    
    # 构建已开启自动赚币的Token字典
    auto_invest_tokens = {p['asset']: p for p in earn_positions if int(p.get('auto_invest_status', 0)) == 1}
    logger.info(f"找到 {len(auto_invest_tokens)} 个已开启自动赚币的代币")
    
    # 初始化CCXT交易所对象获取余额
    exchange = ccxt.gateio({
        'apiKey': gateio_api_key,
        'secret': gateio_api_secret,
        'enableRateLimit': True,
        'proxies': proxies
    })
    
    # 获取现货账户余额
    try:
        spot_balances = exchange.fetch_balance()
        logger.info(f"成功获取 GateIO 现货账户余额")
    except Exception as e:
        logger.error(f"获取 GateIO 现货账户余额失败: {str(e)}")
        logger.setLevel(logging.ERROR)
        return 0
    
    # 处理所有非零余额的代币
    for token, balance in spot_balances.items():
        if not isinstance(balance, dict) or token in ['GT2', 'USDT', 'total', 'used', 'free', 'info']:
            continue
        
        # 获取余额
        token_balance = balance.get('total', 0)
        token_free = balance.get('free', 0)
        
        if float(token_balance) <= 0:
            continue
            
        # 判断是否达到1USDT的价值
        usdt_value = 0
        if usdt_value == 0 and token_balance > 0:
            try:
                # 尝试获取最新价格
                ticker = exchange.fetch_ticker(f"{token}/USDT")
                price = ticker['last']
                usdt_value = float(token_balance) * price
                logger.debug(f"代币 {token} 当前价格: {price} USDT, 持有数量: {token_balance}, 估值: {usdt_value:.2f} USDT")
            except Exception as e:
                logger.debug(f"无法获取代币 {token} 的价格: {str(e)}")
                continue
        
        # 检查自动赚币状态和余额要求
        is_auto_invest = token in auto_invest_tokens

        # 判断是否满足条件
        if usdt_value >= 1 and is_auto_invest and token_free > 0:
            logger.info(f"尝试为代币 {token} 申购理财")
            try:
                # 申购全部可用余额
                subscrible_earn(token, token_free)
                logger.info(f"成功为代币 {token} 申购理财并开启自动赚币, 数量: {token_free}")
            except Exception as e:
                logger.error(f"申购代币 {token} 理财失败: {str(e)}")
    
    logger.info("="*50)
    logger.setLevel(logging.ERROR)  # 恢复日志级别

if __name__ == '__main__':
    auto_subscribe_earn()
    print_earn_info()