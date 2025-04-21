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
        
        # 交易统计
        self.trades_completed = 0
        self.initial_contract_position = 0
        self.current_contract_position = 0

    async def initialize(self):
        """
        异步初始化方法，执行需要网络请求的初始化操作
        """
        try:
            logger.info(f"初始化: 交易对={self.symbol}, 合约对={self.contract_symbol}, "
                        f"最小价差={self.min_spread * 100}%")

            # 检查当前持仓情况
            await self.check_positions()
            
            # 记录初始合约持仓
            self.initial_contract_position = 0
            for position in self.bybit_position:
                if position['info']['symbol'] == self.contract_symbol and position['side'] == 'short':
                    self.initial_contract_position = abs(float(position['contracts']))
                    self.current_contract_position = self.initial_contract_position
                    break
            
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

                if self.current_contract_position < self.spot_amount:
                    raise Exception(f"Bybit合约空单持仓不足，需要 {self.spot_amount}，当前持仓 {self.current_contract_position}")

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

            self.current_contract_position = 0
            for position in self.bybit_position:
                if position['info']['symbol'] == self.contract_symbol and position['side'] == 'short':
                    self.current_contract_position = abs(float(position['contracts']))
                    logger.info(f"Bybit合约空单持仓: {self.current_contract_position} {base_currency}")

            logger.info(f"当前持仓 - Gate.io现货: {spot_position} {base_currency}, "
                        f"Bybit合约空单: {self.current_contract_position} {base_currency}")

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

            # 等待一小段时间确保订单状态更新
            await asyncio.sleep(1)

            # 验证交易结果 - 这将获取并验证最新的订单状态
            await self.verify_trade_result(spot_order, contract_order)
            
            # 检查平仓后的持仓情况
            await self.check_positions()
            
            # 更新交易统计
            self.trades_completed += 1

            return spot_order, contract_order

        except Exception as e:
            logger.error(f"执行平仓交易时出错: {str(e)}")
            raise

    async def verify_trade_result(self, spot_order, contract_order):
        """
        验证交易结果是否符合预期
        
        Args:
            spot_order: Gate.io现货订单结果
            contract_order: Bybit合约订单结果
        
        Raises:
            Exception: 如果订单状态异常或交易数量不一致
        """
        try:
            # 获取订单ID
            spot_order_id = spot_order.get('id')
            contract_order_id = contract_order.get('id')
            base_currency = self.symbol.split('/')[0]
            
            logger.info(f"验证交易结果 - Gate.io订单ID: {spot_order_id}, Bybit订单ID: {contract_order_id}")
            
            # 获取最新的订单信息
            updated_spot_order = spot_order
            updated_contract_order = contract_order
            
            # 获取Gate.io订单最新状态 - 必须获取成功
            if spot_order_id:
                try:
                    updated_spot_order = await self.gateio.fetch_order(spot_order_id, self.symbol)
                    logger.debug(f"获取到最新Gate.io订单信息: {updated_spot_order}")
                except Exception as e:
                    logger.error(f"获取Gate.io订单详情失败: {str(e)}")
                    raise Exception(f"无法获取Gate.io订单状态，验证失败: {str(e)}")
            else:
                raise Exception("Gate.io订单ID无效，无法验证订单状态")
            
            # 获取Bybit订单最新状态 - 必须获取成功
            if contract_order_id:
                bybit_order_found = False
                try:
                    # 首先查找已关闭的订单
                    closed_orders = await self.bybit.fetch_closed_orders(self.contract_symbol, limit=20)
                    for order in closed_orders:
                        if order.get('id') == contract_order_id:
                            updated_contract_order = order
                            bybit_order_found = True
                            logger.debug(f"从已关闭订单中获取到Bybit订单信息: {updated_contract_order}")
                            break
                    
                    # 如果在关闭订单中没找到，查找未完成订单
                    if not bybit_order_found:
                        open_orders = await self.bybit.fetch_open_orders(self.contract_symbol, limit=20)
                        for order in open_orders:
                            if order.get('id') == contract_order_id:
                                updated_contract_order = order
                                bybit_order_found = True
                                logger.debug(f"从未完成订单中获取到Bybit订单信息: {updated_contract_order}")
                                break
                    
                    # 如果两种方式都未找到订单，尝试直接查询
                    if not bybit_order_found:
                        try:
                            order_result = await self.bybit.fetch_order(contract_order_id, self.contract_symbol)
                            if order_result:
                                updated_contract_order = order_result
                                bybit_order_found = True
                                logger.debug(f"通过直接查询获取到Bybit订单信息: {updated_contract_order}")
                        except Exception as e:
                            logger.warning(f"直接查询Bybit订单失败: {str(e)}")
                    
                    # 如果所有方法都未找到订单
                    if not bybit_order_found:
                        raise Exception(f"无法找到Bybit订单 {contract_order_id} 的最新状态")
                        
                except Exception as e:
                    logger.error(f"获取Bybit订单详情失败: {str(e)}")
                    raise Exception(f"无法获取Bybit订单状态，验证失败: {str(e)}")
            else:
                raise Exception("Bybit订单ID无效，无法验证订单状态")
            
            # 严格检查订单状态 - 必须是完成状态
            valid_statuses = ['closed', 'filled']
            spot_status = updated_spot_order.get('status')
            contract_status = updated_contract_order.get('status')
            
            logger.info(f"订单状态检查 - Gate.io: {spot_status}, Bybit: {contract_status}")
            
            if spot_status not in valid_statuses:
                raise Exception(f"Gate.io现货订单未完成，当前状态: {spot_status}")
            
            # Bybit有时可能返回None作为状态，我们需要进一步验证
            if contract_status not in valid_statuses:
                if contract_status is None:
                    logger.warning("Bybit返回订单状态为None，将检查filled字段确认是否成功")
                    if not updated_contract_order.get('filled'):
                        raise Exception("Bybit订单状态为None且无成交量，验证失败")
                else:
                    raise Exception(f"Bybit合约订单未完成，当前状态: {contract_status}")
            
            # 获取成交数量 - 必须有成交
            spot_filled = float(updated_spot_order.get('filled', 0))
            contract_filled = float(updated_contract_order.get('filled', 0))
            
            # 如果订单状态显示完成但成交量为0，尝试其他方式获取成交量
            if spot_filled <= 0:
                logger.warning("Gate.io订单状态为完成但成交量为0，尝试其他方式获取")
                try:
                    # 从余额变化获取
                    current_balance = await self.gateio.fetch_balance()
                    spot_balance = float(current_balance.get(base_currency, {}).get('free', 0))
                    # 只能估算，因为这个时间点的余额可能已经包含了其他操作的影响
                    # 我们使用订单中的amount字段作为估计值
                    spot_filled = float(updated_spot_order.get('amount', 0))
                    logger.info(f"使用订单amount估算Gate.io成交量: {spot_filled} {base_currency}")
                except Exception as e:
                    logger.error(f"无法获取Gate.io成交量: {str(e)}")
                    raise Exception(f"Gate.io成交量获取失败，无法验证交易结果: {str(e)}")
            
            if contract_filled <= 0:
                logger.warning("Bybit订单状态为完成但成交量为0，尝试从持仓信息获取")
                try:
                    # 从持仓变化获取
                    positions = await self.bybit.fetch_positions([self.contract_symbol])
                    position_found = False
                    for position in positions:
                        if position['info']['symbol'] == self.contract_symbol and position['side'] == 'short':
                            contract_filled = abs(float(position.get('contracts', 0)))
                            position_found = True
                            logger.info(f"从持仓信息获取Bybit成交量: {contract_filled} {base_currency}")
                            break
                    
                    if not position_found:
                        # 如果找不到持仓，可能是已经完全平仓，使用订单中的amount作为估计值
                        contract_filled = float(updated_contract_order.get('amount', 0))
                        logger.info(f"使用订单amount估算Bybit成交量: {contract_filled} {base_currency}")
                except Exception as e:
                    logger.error(f"无法获取Bybit成交量: {str(e)}")
                    raise Exception(f"Bybit成交量获取失败，无法验证交易结果: {str(e)}")
            
            # 确保有成交量数据
            if spot_filled <= 0:
                raise Exception(f"Gate.io成交量为0，交易可能未成功")
                
            if contract_filled <= 0:
                raise Exception(f"Bybit成交量为0，交易可能未成功")
            
            # 打印成交信息
            spot_fees = updated_spot_order.get('fees', [])
            spot_quote_fee = sum(float(fee.get('cost', 0)) for fee in spot_fees if fee.get('currency') == 'USDT')
            logger.info(f"Gate.io实际成交数量: {spot_filled} {base_currency}, 手续费: {spot_quote_fee} USDT")
            
            contract_fees = updated_contract_order.get('fees', [])
            contract_quote_fee = sum(float(fee.get('cost', 0)) for fee in contract_fees if fee.get('currency') == 'USDT')
            logger.info(f"Bybit实际成交数量: {contract_filled} {base_currency}, 手续费: {contract_quote_fee} USDT")
            
            # 严格检查成交数量是否一致（允许5%的误差）
            difference_percent = abs(spot_filled - contract_filled) / max(spot_filled, contract_filled)
            
            if difference_percent > 0.05:  # 超过5%的误差
                raise Exception(f"交易数量不一致: Gate.io成交量 {spot_filled}, Bybit成交量 {contract_filled}, "
                               f"误差: {difference_percent * 100:.2f}% > 5.00%")
            
            logger.info(f"交易结果验证通过: Gate.io成交量 {spot_filled}, Bybit成交量 {contract_filled}, "
                       f"误差: {difference_percent * 100:.2f}% <= 5.00%")
                
        except Exception as e:
            logger.error(f"交易结果验证失败: {str(e)}")
            raise

    def print_trading_summary(self, count=None):
        """
        打印交易汇总信息
        
        Args:
            count: 计划执行的总次数
        """
        base_currency = self.symbol.split('/')[0]
        logger.info("=" * 50)
        logger.info("交易汇总:")
        logger.info(f"- 已完成交易次数: {self.trades_completed}")
        if count is not None:
            logger.info(f"- 计划交易次数: {count}")
            logger.info(f"- 剩余交易次数: {max(0, count - self.trades_completed)}")
        logger.info(f"- 初始合约持仓: {self.initial_contract_position} {base_currency}")
        logger.info(f"- 当前合约持仓: {self.current_contract_position} {base_currency}")
        logger.info(f"- 减少合约持仓: {self.initial_contract_position - self.current_contract_position} {base_currency}")
        logger.info("=" * 50)


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Gate.io现货卖出与Bybit合约平空单交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, required=True, help='卖出的现货数量')
    parser.add_argument('-p', '--min-spread', type=float, default=0.0005, help='最小价差要求，默认0.001 (0.1%%)')
    parser.add_argument('-d', '--debug', action='store_true', help='启用调试日志模式')
    parser.add_argument('-c', '--count', type=int, help='重复执行交易的次数')
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

    trader = None
    execution_count = 0
    total_count = None
    
    try:
        trader = UnhedgeTrader(
            symbol=args.symbol,
            spot_amount=args.amount,
            min_spread=args.min_spread
        )
        
        # 初始化并检查持仓
        await trader.initialize()
        
        # 如果未指定count，则根据合约持仓计算
        if args.count is None:
            max_executions = int(trader.current_contract_position / args.amount) - 1
            total_count = max(0, max_executions)
            logger.info(f"未指定执行次数，根据当前持仓计算为: {total_count} 次")
        else:
            total_count = args.count
            logger.info(f"计划执行交易次数: {total_count}")
        
        # 循环执行交易
        while execution_count < total_count:
            logger.info(f"开始第 {execution_count + 1}/{total_count} 次交易")
            
            try:
                # 每次交易前检查持仓是否充足
                await trader.check_positions()
                base_currency = args.symbol.split('/')[0]
                
                # 检查现货余额
                current_amount = float(trader.gateio_balance.get(base_currency, {}).get('free', 0))
                if current_amount < args.amount:
                    # 尝试从理财赎回
                    need_spot_amount = args.amount - current_amount + 0.1
                    logger.info(f"现货余额不足，尝试从理财赎回 {need_spot_amount} {base_currency}")
                    redeem_earn(base_currency, need_spot_amount)
                    
                    # 再次检查余额
                    await trader.check_positions()
                    current_amount = float(trader.gateio_balance.get(base_currency, {}).get('free', 0))
                    if current_amount < args.amount:
                        raise Exception(f"从理财赎回后，{base_currency}余额仍不足，需要 {args.amount}，"
                                        f"当前可用 {current_amount}")
                
                # 检查合约持仓
                if trader.current_contract_position < args.amount:
                    raise Exception(f"Bybit合约空单持仓不足，需要 {args.amount}，当前持仓 {trader.current_contract_position}")
                
                # 执行交易
                logger.info(f"执行第 {execution_count + 1}/{total_count} 次交易，等待价差满足条件...")
                spot_order, contract_order = await trader.execute_unhedge_trade()
                if not (spot_order and contract_order):
                    raise Exception("交易执行失败，未能成功下单")
                
                execution_count += 1
                logger.info(f"完成第 {execution_count}/{total_count} 次交易")
                
                # 每次交易后简单汇报
                trader.print_trading_summary(total_count)
                
                # 在成功执行后等待一段时间，避免API请求过于频繁
                # if execution_count < total_count:
                #     wait_time = 3
                #     logger.info(f"等待 {wait_time} 秒后开始下一次交易...")
                #     await asyncio.sleep(wait_time)
            
            except Exception as e:
                logger.error(f"第 {execution_count + 1}/{total_count} 次交易失败: {str(e)}")
                
                # 打印交易汇总并退出循环
                logger.error("交易验证失败，中止后续交易")
                if trader:
                    trader.print_trading_summary(total_count)
                raise  # 将异常抛出到外层处理
                
        logger.info("所有计划交易已成功完成!")

    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        return 1
    finally:
        # 打印交易汇总
        if trader:
            trader.print_trading_summary(total_count)
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