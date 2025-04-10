#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io现货卖出与Bybit合约平空单的套利脚本

此脚本实现以下功能：
1. 从Gate.io市价卖出指定token的现货
2. 从Bybit平掉对应的合约空单进行对冲
3. 确保现货和合约仓位保持一致
4. 检查价差是否满足最小套利条件
5. 监控和记录交易执行情况
"""

import sys
import os
import time
import logging
import argparse
from decimal import Decimal
import ccxt.async_support as ccxt
import asyncio
import aiohttp
import ccxt.pro as ccxtpro
from collections import defaultdict
from typing import Dict, Optional


# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import bybit_api_key, bybit_api_secret, gateio_api_secret, gateio_api_key, proxies
from trade.gateio_api import redeem_earn


class UnhedgeTrader:
    """
    现货-合约对冲平仓类，实现Gate.io现货卖出与Bybit合约平空单
    """

    def __init__(self, symbol, spot_amount=None, min_spread=0.001):
        """
        初始化基本属性
        
        Args:
            symbol (str): 交易对，如 'ETH/USDT'
            spot_amount (float): 要卖出的现货数量
            min_spread (float): 最小价差要求，默认0.001 (0.1%)
        """
        self.symbol = symbol
        self.spot_amount = spot_amount
        self.min_spread = min_spread

        # 设置合约交易对
        base, quote = symbol.split('/')
        self.contract_symbol = f"{base}{quote}"  # Bybit格式: ETHUSDT

        # 初始化交易所连接
        self.gateio = ccxtpro.gateio({
            'apiKey': gateio_api_key,
            'secret': gateio_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
        })

        self.bybit = ccxtpro.bybit({
            'apiKey': bybit_api_key,
            'secret': bybit_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
        })

        # 存储账户余额
        self.gateio_balance = None
        self.bybit_position = None

        # 用于存储最新订单簿数据
        self.orderbooks = {
            'gateio': None,
            'bybit': None
        }

        # 用于控制WebSocket订阅
        self.ws_running = False
        self.price_updates = asyncio.Queue()

    async def initialize(self):
        """
        异步初始化方法，执行需要网络请求的初始化操作
        """
        try:
            logger.info(f"初始化: 交易对={self.symbol}, 合约对={self.contract_symbol}, "
                        f"最小价差={self.min_spread * 100}%")

            # 检查当前持仓情况
            await self.check_positions()
            # 验证持仓是否满足交易条件
            if self.spot_amount is not None:
                base_currency = self.symbol.split('/')[0]
                current_amount = float(self.gateio_balance.get(base_currency, {}).get('free', 0))
                if current_amount < self.spot_amount:
                    # 如果现货持仓不足，自动从理财中赎回缺失的部分，然后再检查, +0.1是防止不太够
                    need_spot_amount = self.spot_amount - current_amount + 0.1
                    redeem_earn(base_currency, need_spot_amount)
                    # 再检查余额是否够
                    await self.check_positions()
                    if self.spot_amount is not None:
                        current_amount = float(self.gateio_balance.get(base_currency, {}).get('free', 0))
                        if current_amount < self.spot_amount:
                            raise Exception(f"Gate.io {base_currency}余额不足，需要 {self.spot_amount}，"
                                            f"当前可用 {self.gateio_balance[base_currency]['free']}")

                contract_position = 0
                for position in self.bybit_position:
                    if position['symbol'] == self.contract_symbol and position['side'] == 'short':
                        contract_position = abs(float(position['contracts']))

                if contract_position < self.spot_amount:
                    raise Exception(f"Bybit合约空单持仓不足，需要 {self.spot_amount}，当前持仓 {contract_position}")

                logger.info("持仓检查通过，可以执行平仓操作")

        except Exception as e:
            logger.error(f"初始化失败: {str(e)}")
            raise

    async def check_positions(self):
        """
        检查Gate.io和Bybit的持仓情况
        """
        try:
            # 并行获取两个交易所的持仓信息
            self.gateio_balance, self.bybit_position = await asyncio.gather(
                self.gateio.fetch_balance(),
                self.bybit.fetch_positions([self.contract_symbol])
            )

            base_currency = self.symbol.split('/')[0]
            spot_position = self.gateio_balance.get(base_currency, {}).get('total', 0)

            contract_position = 0
            for position in self.bybit_position:
                if position['symbol'] == self.contract_symbol and position['side'] == 'short':
                    contract_position = abs(float(position['contracts']))
                    logger.info(f"Bybit合约空单持仓: {contract_position} {base_currency}")

            logger.info(f"当前持仓 - Gate.io现货: {spot_position} {base_currency}, "
                        f"Bybit合约空单: {contract_position} {base_currency}")

        except Exception as e:
            logger.error(f"检查持仓时出错: {str(e)}")
            raise

    async def subscribe_orderbooks(self):
        """订阅交易对的订单簿数据"""
        try:
            self.ws_running = True
            while self.ws_running:
                try:
                    tasks = [
                        asyncio.create_task(self.gateio.watch_order_book(self.symbol)),
                        asyncio.create_task(self.bybit.watch_order_book(self.contract_symbol))
                    ]

                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED
                    )

                    for task in done:
                        try:
                            ob = task.result()
                            if task == tasks[0]:
                                self.orderbooks['gateio'] = ob
                                logger.debug(f"收到Gate.io订单簿更新")
                            else:
                                self.orderbooks['bybit'] = ob
                                logger.debug(f"收到Bybit订单簿更新")

                            if self.orderbooks['gateio'] and self.orderbooks['bybit']:
                                await self.check_spread_from_orderbooks()

                        except Exception as e:
                            logger.error(f"处理订单簿数据时出错: {str(e)}")

                    for task in pending:
                        task.cancel()

                except Exception as e:
                    logger.error(f"订阅订单簿时出错: {str(e)}")
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"订单簿订阅循环出错: {str(e)}")
        finally:
            self.ws_running = False
            await asyncio.gather(
                self.gateio.close(),
                self.bybit.close()
            )

    async def check_spread_from_orderbooks(self):
        """从订单簿数据中检查价差"""
        try:
            gateio_ob = self.orderbooks['gateio']
            bybit_ob = self.orderbooks['bybit']

            if not gateio_ob or not bybit_ob:
                return

            gateio_bid = Decimal(str(gateio_ob['bids'][0][0]))  # 现货卖出价(买1)
            gateio_bid_volume = Decimal(str(gateio_ob['bids'][0][1]))

            bybit_ask = Decimal(str(bybit_ob['asks'][0][0]))  # 合约买入价(卖1)
            bybit_ask_volume = Decimal(str(bybit_ob['asks'][0][1]))

            spread = gateio_bid - bybit_ask
            spread_percent = spread / bybit_ask

            spread_data = {
                'spread_percent': float(spread_percent),
                'gateio_bid': float(gateio_bid),
                'bybit_ask': float(bybit_ask),
                'gateio_bid_volume': float(gateio_bid_volume),
                'bybit_ask_volume': float(bybit_ask_volume)
            }
            await self.price_updates.put(spread_data)

        except Exception as e:
            logger.error(f"{self.symbol}检查订单簿价差时出错: {str(e)}")

    async def wait_for_spread(self):
        """等待价差达到要求"""
        subscription_task = None
        try:
            subscription_task = asyncio.create_task(self.subscribe_orderbooks())

            while True:
                try:
                    spread_data = await asyncio.wait_for(
                        self.price_updates.get(),
                        timeout=10
                    )

                    spread_percent = spread_data['spread_percent']

                    logger.debug(
                        f"{self.symbol}"
                        f"价格检查 - Gate.io买1: {spread_data['gateio_bid']} (量: {spread_data['gateio_bid_volume']}), "
                        f"Bybit卖1: {spread_data['bybit_ask']} (量: {spread_data['bybit_ask_volume']}), "
                        f"价差: {spread_percent * 100:.4f}%")

                    if spread_percent >= self.min_spread:
                        logger.info(f"{self.symbol}价差条件满足: {spread_percent * 100:.4f}% >= {self.min_spread * 100:.4f}%")
                        return spread_data

                    logger.debug(f"{self.symbol}价差条件不满足: {spread_percent * 100:.4f}% < {self.min_spread * 100:.4f}%")

                except asyncio.TimeoutError:
                    logger.warning(f"{self.symbol}等待价差数据超时，重新订阅订单簿")
                    if subscription_task:
                        subscription_task.cancel()
                    subscription_task = asyncio.create_task(self.subscribe_orderbooks())

        except Exception as e:
            logger.error(f"{self.symbol}等待价差时出错: {str(e)}")
            raise
        finally:
            self.ws_running = False
            if subscription_task:
                subscription_task.cancel()

    async def execute_unhedge_trade(self):
        """执行平仓交易"""
        try:
            # 等待价差满足条件
            spread_data = await self.wait_for_spread()

            # 准备下单参数
            trade_amount = self.spot_amount
            contract_amount = self.bybit.amount_to_precision(self.contract_symbol, trade_amount)

            # 执行交易
            spot_order, contract_order = await asyncio.gather(
                self.gateio.create_market_sell_order(
                    symbol=self.symbol,
                    amount=trade_amount
                ),
                self.bybit.create_market_buy_order(
                    symbol=self.contract_symbol,
                    amount=contract_amount,
                    params={"reduceOnly": True}  # 确保是平仓操作
                )
            )

            base_currency = self.symbol.split('/')[0]
            logger.info(f"计划平仓数量: {trade_amount} {base_currency}")
            logger.info(f"在Gate.io市价卖出 {trade_amount} {base_currency}")
            logger.info(f"在Bybit市价平空单 {contract_amount} {base_currency}")

            # 获取实际成交结果
            filled_amount = float(spot_order['filled'])
            fees = spot_order.get('fees', [])
            quote_fee = sum(float(fee['cost']) for fee in fees if fee['currency'] == 'USDT')

            logger.info(f"Gate.io实际成交数量: {filled_amount} {base_currency}, "
                        f"手续费: {quote_fee} USDT")

            # 检查平仓后的持仓情况
            await self.check_positions()

            return spot_order, contract_order

        except Exception as e:
            logger.error(f"执行平仓交易时出错: {str(e)}")
            raise


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Gate.io现货卖出与Bybit合约平空单交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, required=True, help='卖出的现货数量')
    parser.add_argument('-p', '--min-spread', type=float, default=0.001, help='最小价差要求，默认0.001 (0.1%%)')
    parser.add_argument('-d', '--debug', action='store_true', help='启用调试日志模式')
    return parser.parse_args()


async def main():
    """异步主函数"""
    args = parse_arguments()
    
    # 设置日志级别
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("已启用调试日志模式")
    else:
        logger.setLevel(logging.INFO)

    try:
        trader = UnhedgeTrader(
            symbol=args.symbol,
            spot_amount=args.amount,
            min_spread=args.min_spread
        )
        await trader.initialize()

        spot_order, contract_order = await trader.execute_unhedge_trade()
        if spot_order and contract_order:
            logger.info("平仓交易成功完成!")
        else:
            logger.info("未执行平仓交易")

    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        return 1
    finally:
        if 'trader' in locals():
            await asyncio.gather(
                trader.gateio.close(),
                trader.bybit.close()
            )

    return 0


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 