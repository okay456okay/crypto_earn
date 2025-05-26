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
import subprocess
import signal
from decimal import Decimal
from datetime import datetime
from collections import defaultdict
from time import sleep

import ccxt.pro as ccxtpro
from rich.console import Console
from rich.table import Table
from rich import box

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from tools.proxy import get_proxy_ip
from config import (
    binance_api_key, binance_api_secret,
    bybit_api_key, bybit_api_secret,
    bitget_api_key, bitget_api_secret, bitget_api_passphrase,
    proxies,
    project_root, earn_auto_sell
)
from high_yield.exchange import ExchangeAPI

# 从现有模块导入
from trade.gateio_api import get_earn_positions, get_earn_product

# 设置日志级别
import logging
logger.setLevel(logging.ERROR)

# 初始化rich console
console = Console()

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
        self.exchange_api = ExchangeAPI()  # 初始化ExchangeAPI实例

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
        console.print("\n" + "="*120)
        console.print(f"GateIO理财与合约套利分析 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        console.print("="*120)

        # 创建合约持仓汇总表格
        position_table = Table(title="合约持仓汇总", box=box.ROUNDED)
        position_table.add_column("代币", justify="left", style="cyan")
        position_table.add_column("Binance多", justify="right", style="green")
        position_table.add_column("Binance空", justify="right", style="red")
        position_table.add_column("Bybit多", justify="right", style="green")
        position_table.add_column("Bybit空", justify="right", style="red")
        position_table.add_column("Bitget多", justify="right", style="green")
        position_table.add_column("Bitget空", justify="right", style="red")
        position_table.add_column("总多仓", justify="right", style="green")
        position_table.add_column("总空仓", justify="right", style="red")
        position_table.add_column("净仓位", justify="right")
        position_table.add_column("当前价格", justify="right")
        position_table.add_column("持仓金额", justify="right")

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

        # 添加数据到表格
        for pos in position_summary:
            position_table.add_row(
                pos['token'],
                f"{pos['binance_long']:.2f}",
                f"{pos['binance_short']:.2f}",
                f"{pos['bybit_long']:.2f}",
                f"{pos['bybit_short']:.2f}",
                f"{pos['bitget_long']:.2f}",
                f"{pos['bitget_short']:.2f}",
                f"{pos['total_long']:.2f}",
                f"{pos['total_short']:.2f}",
                f"{pos['net_position']:.2f}",
                f"{pos['price']:.6f}",
                f"{pos['position_value']:.2f}"
            )

        # 打印合约持仓汇总表格
        console.print(position_table)

        # 创建GateIO理财与套利情况表格
        arbitrage_table = Table(title="GateIO理财与套利情况", box=box.ROUNDED)
        arbitrage_table.add_column("代币", justify="left", style="cyan")
        arbitrage_table.add_column("理财数量", justify="right")
        arbitrage_table.add_column("合约净仓位", justify="right")
        arbitrage_table.add_column("对冲差额", justify="right")
        arbitrage_table.add_column("当前价格", justify="right")
        arbitrage_table.add_column("理财金额", justify="right")
        arbitrage_table.add_column("理财年化", justify="right")
        arbitrage_table.add_column("合约年化", justify="right")
        arbitrage_table.add_column("综合年化", justify="right")
        arbitrage_table.add_column("套利价值", justify="right")
        arbitrage_table.add_column("套利收益率", justify="right")
        arbitrage_table.add_column("借出总额", justify="right")
        arbitrage_table.add_column("可借总额", justify="right")

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

                # 计算合约资金费率年化收益率
                funding_rate_apy = 0
                total_contract_value = 0

                # Binance资金费率
                binance_positions = [p for p in self.positions['binance'] if p['token'] == token]
                for pos in binance_positions:
                    contracts = abs(pos['contracts'])
                    if contracts == 0:
                        continue

                    # 获取资金费率信息
                    funding_info = self.exchange_api.get_binance_futures_funding_rate(token + 'USDT')
                    funding_rate = float(funding_info.get('fundingRate', 0)) / 100  # 转换为小数
                    funding_interval = float(funding_info.get('fundingIntervalHours', 8))

                    # 计算年化收益率
                    position_value = contracts * price
                    position_apy = funding_rate * (24/funding_interval) * 365 * 100  # 转换为百分比
                    funding_rate_apy += position_apy * position_value
                    total_contract_value += position_value

                # Bybit资金费率
                bybit_positions = [p for p in self.positions['bybit'] if p['token'] == token]
                for pos in bybit_positions:
                    contracts = abs(pos['contracts'])
                    if contracts == 0:
                        continue

                    # 获取资金费率信息
                    funding_info = self.exchange_api.get_bybit_futures_funding_rate(token + 'USDT')
                    funding_rate = float(funding_info.get('fundingRate', 0)) / 100  # 转换为小数
                    funding_interval = float(funding_info.get('fundingIntervalHours', 8))

                    # 计算年化收益率
                    position_value = contracts * price
                    position_apy = funding_rate * (24/funding_interval) * 365 * 100  # 转换为百分比
                    funding_rate_apy += position_apy * position_value
                    total_contract_value += position_value

                # Bitget资金费率
                bitget_positions = [p for p in self.positions['bitget'] if p['token'] == token]
                for pos in bitget_positions:
                    contracts = abs(pos['contracts'])
                    if contracts == 0:
                        continue

                    # 获取资金费率信息
                    funding_info = self.exchange_api.get_bitget_futures_funding_rate(token + 'USDT')
                    funding_rate = float(funding_info.get('fundingRate', 0)) / 100  # 转换为小数
                    funding_interval = float(funding_info.get('fundingIntervalHours', 8))

                    # 计算年化收益率
                    position_value = contracts * price
                    position_apy = funding_rate * (24/funding_interval) * 365 * 100  # 转换为百分比
                    funding_rate_apy += position_apy * position_value
                    total_contract_value += position_value

                # 计算加权平均资金费率年化收益率
                if total_contract_value > 0:
                    funding_rate_apy = funding_rate_apy / total_contract_value

                # 计算综合加权收益率
                total_value = total_contract_value + earn_value
                if total_value > 0:
                    # combined_apy = (funding_rate_apy * total_contract_value + last_rate_year * earn_value) / total_value
                    combined_apy = funding_rate_apy + last_rate_year
                else:
                    combined_apy = 0

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
                    'funding_rate_apy': funding_rate_apy,
                    'combined_apy': combined_apy,
                    'arbitrage_value': arbitrage_value,
                    'arbitrage_rate': arbitrage_rate,
                    'total_lend_amount': total_lend_amount,
                    'total_lend_available': total_lend_available
                })

                # 检查是否需要关闭仓位
                if last_rate_year < 5 and funding_rate_apy < 12:
                    # 找出空仓持仓量最大的交易所
                    binance_short = sum(p['contracts'] for p in self.positions['binance'] if p['token'] == token and p['side'] == 'short')
                    bybit_short = sum(p['contracts'] for p in self.positions['bybit'] if p['token'] == token and p['side'] == 'short')
                    bitget_short = sum(p['contracts'] for p in self.positions['bitget'] if p['token'] == token and p['side'] == 'short')
                    
                    exchange_shorts = {
                        'binance': binance_short,
                        'bybit': bybit_short,
                        'bitget': bitget_short
                    }
                    
                    logger.info(f"代币 {token} 各交易所空仓持仓量: Binance={binance_short}, Bybit={bybit_short}, Bitget={bitget_short}")
                    
                    # 找出空仓持仓量最大的交易所
                    max_short_exchange = max(exchange_shorts.items(), key=lambda x: x[1])
                    
                    if max_short_exchange[1] > 0 and earn_auto_sell:  # 如果有空仓
                        try:
                            # 运行close.sh脚本
                            cmd = f"{project_root}/scripts/close.sh -e {max_short_exchange[0]} -s {token}"
                            logger.info(f"执行关闭仓位命令: {cmd}")
                            subprocess.run(cmd, shell=True, check=True)
                            sleep(1)
                        except Exception as e:
                            logger.error(f"关闭仓位失败: {str(e)}")

        # 按理财金额降序排序
        arbitrage_summary.sort(key=lambda x: x['earn_value'], reverse=True)

        # 添加数据到表格
        for pos in arbitrage_summary:
            # 检查是否需要标红和加粗
            should_highlight = (
                pos['funding_rate_apy'] < 0 or  # 合约年化为负
                pos['last_rate_year'] < 3 or    # 理财年化小于3%
                pos['combined_apy'] < 15         # 综合年化小于3%
            )
            
            # 设置样式
            style = "bold red" if should_highlight else None
            
            arbitrage_table.add_row(
                pos['token'],
                f"{pos['earn_amount']:.2f}",
                f"{pos['net_position']:.2f}",
                f"{pos['hedge_diff']:.2f}",
                f"{pos['price']:.6f}",
                f"{pos['earn_value']:.2f}",
                f"{pos['last_rate_year']:.2f}",
                f"{pos['funding_rate_apy']:.2f}",
                f"{pos['combined_apy']:.2f}",
                f"{pos['arbitrage_value']:.2f}",
                f"{pos['arbitrage_rate']:.2f}",
                f"{pos['total_lend_amount']:.2f}",
                f"{pos['total_lend_available']:.2f}",
                style=style
            )
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

        # 打印GateIO理财与套利情况表格
        console.print(arbitrage_table)

        # 打印合约中有但理财没有的代币
        for token, positions in self.aggregated_positions.items():
            if token not in [p['token'] for p in self.gateio_earn_positions]:
                net_position = positions['long'] - positions['short']

                # 如果合约净仓位为0，则跳过显示
                if net_position == 0:
                    continue

                price = self.token_prices.get(token, 0)
                arbitrage_value = 0
                try:
                    arbitrage_table.add_row(
                        token,
                        "0.00",
                        f"{net_position:.2f}",
                        f"{-net_position:.2f}",
                        f"{price:.6f}",
                        "0.00",
                        "0.00",
                        "0.00",
                        "0.00",
                        f"{arbitrage_value:.2f}",
                        "0.00",
                        "0.00",
                        "0.00"
                    )
                except Exception as e:
                    logger.error(f"print failed, info: {token} {net_position} {price} {arbitrage_value}", exc_info=True)
                    continue

        # 打印套利总和
        console.print("\n【套利汇总】")
        console.print(f"总套利价值: {total_arbitrage_value:.2f} USDT")

        # 计算加权平均年化收益率
        total_earn_value = 0
        weighted_apy_sum = 0
        for pos in arbitrage_summary:
            earn_value = pos['earn_value']
            if earn_value > 0:  # 只计算有理财金额的
                total_earn_value += earn_value
                weighted_apy_sum += earn_value * pos['last_rate_year']

        weighted_apy = (weighted_apy_sum / total_earn_value) if total_earn_value > 0 else 0
        console.print(f"加权平均年化收益率: {weighted_apy:.2f}%")
        console.print("="*90)

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
        # 杀掉所有gateio_*_unhedge.py进程
        try:
            subprocess.run(['pkill', '-f', 'gateio_.*_unhedge.py'], check=False)
            logger.info("已杀掉所有gateio unhedge进程")
        except Exception as e:
            logger.error(f"杀掉gateio unhedge进程失败: {str(e)}")

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