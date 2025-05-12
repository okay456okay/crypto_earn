#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
交易所套利监控脚本

此脚本用于：
1. 汇总Binance, Bitget, Bybit的合约持仓
2. 获取GateIO理财持仓情况
3. 计算GateIO理财与合约持仓的对冲差额
4. 计算对冲收益情况
"""

import sys
import os
import asyncio
from decimal import Decimal
from datetime import datetime
from collections import defaultdict
import ccxt.pro as ccxtpro

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from tools.proxy import get_proxy_ip
from config import (
    binance_api_key, binance_api_secret,
    bybit_api_key, bybit_api_secret,
    bitget_api_key, bitget_api_secret, bitget_api_passphrase,
    proxies
)

# 从现有模块导入
from trade.gateio_api import get_earn_positions, get_earn_product

# 设置日志级别
import logging
logger.setLevel(logging.ERROR)

class ExchangeArbitrageCalculator:
    def __init__(self):
        """初始化交易所连接"""
        self.init_exchanges()
        self.positions = {
            'binance': [],
            'bybit': [],
            'bitget': [],
            'gateio': []
        }
        self.aggregated_positions = defaultdict(lambda: {'long': 0, 'short': 0})
        self.token_prices = {}
        self.gateio_earn_positions = []

    def init_exchanges(self):
        """初始化所有交易所连接"""
        self.binance = ccxtpro.binance({
            'apiKey': binance_api_key,
            'secret': binance_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'options': {'defaultType': 'future'}
        })

        self.bybit = ccxtpro.bybit({
            'apiKey': bybit_api_key,
            'secret': bybit_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'options': {'defaultType': 'linear'}
        })

        self.bitget = ccxtpro.bitget({
            'apiKey': bitget_api_key,
            'secret': bitget_api_secret,
            'password': bitget_api_passphrase,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None)
        })

    def get_token_from_symbol(self, symbol):
        """从不同格式的symbol中提取基础token名称"""
        if ':' in symbol:  # Bybit/Bitget格式: BTC/USDT:USDT
            return symbol.split('/')[0]
        else:  # Binance格式: BTCUSDT
            return symbol.replace('USDT', '')

    async def fetch_binance_positions(self):
        """获取Binance合约持仓"""
        try:
            positions = await self.binance.fetch_positions()
            valid_positions = []

            for position in positions:
                if float(position.get('contracts', 0)) == 0:
                    continue

                symbol = position.get('info', {}).get('symbol', '')
                if not symbol:
                    continue

                token = self.get_token_from_symbol(symbol)
                side = position.get('side', 'unknown')
                contracts = float(position.get('contracts', 0))
                entry_price = float(position.get('entryPrice', 0))
                mark_price = float(position.get('markPrice', 0))

                # 保存当前价格
                self.token_prices[token] = mark_price

                valid_position = {
                    'token': token,
                    'side': side,
                    'contracts': contracts,
                    'entryPrice': entry_price,
                    'markPrice': mark_price
                }
                valid_positions.append(valid_position)

                # 更新聚合仓位
                if side == 'long':
                    self.aggregated_positions[token]['long'] += contracts
                else:
                    self.aggregated_positions[token]['short'] += contracts

            self.positions['binance'] = valid_positions
            logger.info(f"获取Binance持仓成功: {len(valid_positions)}个")
            return valid_positions

        except Exception as e:
            logger.error(f"获取Binance持仓失败: {str(e)}")
            return []

    async def fetch_bybit_positions(self):
        """获取Bybit合约持仓"""
        try:
            positions = await self.bybit.fetch_positions()
            valid_positions = []

            for position in positions:
                if float(position.get('contracts', 0)) == 0:
                    continue

                symbol = position['symbol']
                token = self.get_token_from_symbol(symbol)
                side = position['side']
                contracts = float(position.get('contracts', 0))
                entry_price = float(position.get('entryPrice', 0))
                mark_price = float(position.get('markPrice', 0))

                # 保存当前价格
                self.token_prices[token] = mark_price

                valid_position = {
                    'token': token,
                    'side': side,
                    'contracts': contracts,
                    'entryPrice': entry_price,
                    'markPrice': mark_price
                }
                valid_positions.append(valid_position)

                # 更新聚合仓位
                if side == 'long':
                    self.aggregated_positions[token]['long'] += contracts
                else:
                    self.aggregated_positions[token]['short'] += contracts

            self.positions['bybit'] = valid_positions
            logger.info(f"获取Bybit持仓成功: {len(valid_positions)}个")
            return valid_positions

        except Exception as e:
            logger.error(f"获取Bybit持仓失败: {str(e)}")
            return []

    async def fetch_bitget_positions(self):
        """获取Bitget合约持仓"""
        try:
            positions = await self.bitget.fetch_positions()
            valid_positions = []

            for position in positions:
                if float(position.get('contracts', 0)) == 0:
                    continue

                symbol = position['symbol']
                token = self.get_token_from_symbol(symbol)
                side = position['side']
                contracts = float(position.get('contracts', 0))
                entry_price = float(position.get('entryPrice', 0))
                mark_price = float(position.get('markPrice', 0))

                # 保存当前价格
                self.token_prices[token] = mark_price

                valid_position = {
                    'token': token,
                    'side': side,
                    'contracts': contracts,
                    'entryPrice': entry_price,
                    'markPrice': mark_price
                }
                valid_positions.append(valid_position)

                # 更新聚合仓位
                if side == 'long':
                    self.aggregated_positions[token]['long'] += contracts
                else:
                    self.aggregated_positions[token]['short'] += contracts

            self.positions['bitget'] = valid_positions
            logger.info(f"获取Bitget持仓成功: {len(valid_positions)}个")
            return valid_positions

        except Exception as e:
            logger.error(f"获取Bitget持仓失败: {str(e)}")
            return []

    def fetch_gateio_earn_positions(self):
        """获取GateIO理财持仓"""
        try:
            earn_positions = get_earn_positions()
            valid_positions = []

            for position in earn_positions:
                if float(position.get('curr_amount_usdt', 0)) < 1:
                    continue

                token = position['asset']
                amount = float(position['curr_amount'])
                price = float(position['price'])
                value_usdt = float(position['curr_amount_usdt'])
                last_rate_year = float(position.get('last_rate_year', 0))  # 获取年化收益率

                # 获取理财产品信息
                product_info = get_earn_product(token)
                total_lend_amount = float(product_info.get('total_lend_amount', 0))
                total_lend_available = float(product_info.get('total_lend_available', 0))

                # 保存当前价格
                self.token_prices[token] = price

                valid_position = {
                    'token': token,
                    'amount': amount,
                    'price': price,
                    'value_usdt': value_usdt,
                    'last_rate_year': last_rate_year,
                    'total_lend_amount': total_lend_amount,
                    'total_lend_available': total_lend_available
                }
                valid_positions.append(valid_position)

            self.positions['gateio'] = valid_positions
            self.gateio_earn_positions = valid_positions
            logger.info(f"获取GateIO理财持仓成功: {len(valid_positions)}个")
            return valid_positions

        except Exception as e:
            logger.error(f"获取GateIO理财持仓失败: {str(e)}")
            return []

    def calculate_arbitrage(self):
        """计算GateIO理财和合约持仓对冲情况"""
        arbitrage_results = []
        total_arbitrage_value = 0

        # 打印标题
        print("\n" + "="*120)
        print(f"GateIO理财与合约套利分析 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*120)

        # 先打印持仓汇总表头
        print("\n【合约持仓汇总】")
        print(f"{'代币':<8} {'Binance多':<12} {'Binance空':<12} {'Bybit多':<12} {'Bybit空':<12} {'Bitget多':<12} {'Bitget空':<12} {'总多仓':<12} {'总空仓':<12} {'净仓位':<12} {'当前价格':<8} {'持仓金额':<15}")
        print("-"*165)

        # 合约持仓汇总
        position_summary = []
        for token, positions in self.aggregated_positions.items():
            binance_long = sum(p['contracts'] for p in self.positions['binance'] if p['token'] == token and p['side'] == 'long')
            binance_short = sum(p['contracts'] for p in self.positions['binance'] if p['token'] == token and p['side'] == 'short')
            bybit_long = sum(p['contracts'] for p in self.positions['bybit'] if p['token'] == token and p['side'] == 'long')
            bybit_short = sum(p['contracts'] for p in self.positions['bybit'] if p['token'] == token and p['side'] == 'short')
            bitget_long = sum(p['contracts'] for p in self.positions['bitget'] if p['token'] == token and p['side'] == 'long')
            bitget_short = sum(p['contracts'] for p in self.positions['bitget'] if p['token'] == token and p['side'] == 'short')

            total_long = positions['long']
            total_short = positions['short']
            net_position = total_long - total_short
            price = self.token_prices.get(token, 0)
            position_value = abs(net_position) * price

            position_summary.append({
                'token': token,
                'binance_long': binance_long,
                'binance_short': binance_short,
                'bybit_long': bybit_long,
                'bybit_short': bybit_short,
                'bitget_long': bitget_long,
                'bitget_short': bitget_short,
                'total_long': total_long,
                'total_short': total_short,
                'net_position': net_position,
                'price': price,
                'position_value': position_value
            })

        # 按持仓金额降序排序
        position_summary.sort(key=lambda x: x['position_value'], reverse=True)

        # 打印排序后的结果
        for pos in position_summary:
            print(f"{pos['token']:<10} {pos['binance_long']:<13.2f} {pos['binance_short']:<13.2f} {pos['bybit_long']:<13.2f} {pos['bybit_short']:<13.2f} {pos['bitget_long']:<13.2f} {pos['bitget_short']:<13.2f} {pos['total_long']:<15.2f} {pos['total_short']:<14.2f} {pos['net_position']:<15.2f} {pos['price']:<12.6f} {pos['position_value']:<15.2f}")

        # 打印GateIO理财与套利情况
        print("\n【GateIO理财与套利情况】")
        print(f"{'代币':<8} {'理财数量':<12} {'合约净仓位':<15} {'对冲差额':<12} {'当前价格':<12} {'理财金额':<10} {'年化收益率':<8} {'套利价值(USDT)':<15} {'套利收益率':<10} {'借出总额':<12} {'可借总额':<12}")
        print("-"*165)

        # 计算套利情况
        arbitrage_summary = []
        for earn_position in self.gateio_earn_positions:
            token = earn_position['token']
            earn_amount = earn_position['amount']
            price = earn_position['price']
            last_rate_year = float(earn_position.get('last_rate_year', 0)) * 100  # 转换为百分比
            earn_value = earn_amount * price  # 理财金额
            total_lend_amount = earn_position.get('total_lend_amount', 0)
            total_lend_available = earn_position.get('total_lend_available', 0)

            # 只计算在合约中也有的代币
            if token in self.aggregated_positions:
                net_position = self.aggregated_positions[token]['long'] - self.aggregated_positions[token]['short']

                # 如果合约净仓位为0，则跳过显示
                if net_position == 0:
                    continue

                # 理财多少，合约应该空多少 (净空仓)，所以理财量+净仓位应该接近0
                hedge_diff = earn_amount + net_position
                arbitrage_value = hedge_diff * price
                arbitrage_rate = (arbitrage_value / earn_value * 100) if earn_value > 0 else 0  # 套利收益率

                arbitrage_summary.append({
                    'token': token,
                    'earn_amount': earn_amount,
                    'net_position': net_position,
                    'hedge_diff': hedge_diff,
                    'price': price,
                    'earn_value': earn_value,
                    'last_rate_year': last_rate_year,
                    'arbitrage_value': arbitrage_value,
                    'arbitrage_rate': arbitrage_rate,
                    'total_lend_amount': total_lend_amount,
                    'total_lend_available': total_lend_available
                })

        # 按理财金额降序排序
        arbitrage_summary.sort(key=lambda x: x['earn_value'], reverse=True)

        # 打印排序后的结果
        for pos in arbitrage_summary:
            print(f"{pos['token']:<10} {pos['earn_amount']:<16.2f} {pos['net_position']:<20.2f} {pos['hedge_diff']:<16.2f} {pos['price']:<16.6f} {pos['earn_value']:<14.2f} {pos['last_rate_year']:<12.2f} {pos['arbitrage_value']:<19.2f} {pos['arbitrage_rate']:<16.2f} {pos['total_lend_amount']:<16.2f} {pos['total_lend_available']:<14.2f}")
            # 更新套利结果和总价值
            arbitrage_results.append({
                'token': pos['token'],
                'earn_amount': pos['earn_amount'],
                'net_position': pos['net_position'],
                'hedge_diff': pos['hedge_diff'],
                'price': pos['price'],
                'arbitrage_value': pos['arbitrage_value']
            })
            total_arbitrage_value += pos['arbitrage_value']

        # 打印合约中有但理财没有的代币
        """
        for token, positions in self.aggregated_positions.items():
            if token not in [p['token'] for p in self.gateio_earn_positions]:
                net_position = positions['long'] - positions['short']

                # 如果合约净仓位为0，则跳过显示
                if net_position == 0:
                    continue

                price = self.token_prices.get(token, 0)
                arbitrage_value = abs(net_position) * price
                try:
                    print(f"{token:<10} {0.0:<16.2f} {net_position:<20.2f} {-net_position:<16.2f} {price:<16.6f} {0.0:<16.2f} {0.0:<14.2f} {arbitrage_value:<15.2f} {0.0:<14.2f} {0.0:<16.2f} {0.0:<16.2f}")
                except Exception as e:
                    logger.error(f"print failed, info: {token} {net_position} {price} {arbitrage_value}", exc_info=True)
                    continue

                arbitrage_results.append({
                    'token': token,
                    'earn_amount': 0,
                    'net_position': net_position,
                    'hedge_diff': net_position,
                    'price': price,
                    'arbitrage_value': arbitrage_value
                })

                total_arbitrage_value += arbitrage_value
        """

        # 打印套利总和
        print("\n【套利汇总】")
        print(f"总套利价值: {total_arbitrage_value:.2f} USDT")

        # 计算加权平均年化收益率
        total_earn_value = 0
        weighted_apy_sum = 0
        for pos in arbitrage_summary:
            earn_value = pos['earn_value']
            if earn_value > 0:  # 只计算有理财金额的
                total_earn_value += earn_value
                weighted_apy_sum += earn_value * pos['last_rate_year']

        weighted_apy = (weighted_apy_sum / total_earn_value) if total_earn_value > 0 else 0
        print(f"加权平均年化收益率: {weighted_apy:.2f}%")
        print("="*90)

        return {
            'arbitrage_results': arbitrage_results,
            'total_arbitrage_value': total_arbitrage_value,
            'weighted_apy': weighted_apy
        }

    async def close_exchanges(self):
        """关闭所有交易所连接"""
        await self.binance.close()
        await self.bybit.close()
        await self.bitget.close()

    async def run(self):
        """运行主程序"""
        try:
            # 获取所有交易所的持仓信息
            await asyncio.gather(
                self.fetch_binance_positions(),
                self.fetch_bybit_positions(),
                self.fetch_bitget_positions()
            )

            # 获取GateIO理财持仓
            self.fetch_gateio_earn_positions()

            # 计算套利情况
            self.calculate_arbitrage()

        except Exception as e:
            logger.exception(f"程序运行出错: {str(e)}")
        finally:
            await self.close_exchanges()

async def main():
    """主函数"""
    try:
        get_proxy_ip()
        calculator = ExchangeArbitrageCalculator()
        await calculator.run()
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        return 1
    return 0

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close()