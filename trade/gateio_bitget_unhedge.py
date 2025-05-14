#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io现货卖出与Bitget合约平空单的套利脚本

此脚本实现以下功能：
1. 从Gate.io市价卖出指定token的现货
2. 从Bitget平掉对应的合约空单进行对冲
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
from typing import Dict, Optional, Tuple, List


# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import bitget_api_key, bitget_api_secret, bitget_api_passphrase, gateio_api_secret, gateio_api_key, proxies
from trade.gateio_api import redeem_earn


class UnhedgeTrader:
    """
    现货-合约对冲平仓类，实现Gate.io现货卖出与Bitget合约平空单
    """

    def __init__(self, symbol, spot_amount=None, min_spread=0.001, depth_multiplier=2.0):
        """
        初始化基本属性
        
        Args:
            symbol (str): 交易对，如 'ETH/USDT'
            spot_amount (float): 要卖出的现货数量
            min_spread (float): 最小价差要求，默认0.001 (0.1%)
            depth_multiplier (float): 订单簿中买一/卖一量至少是交易量的倍数，默认2倍
        """
        self.symbol = symbol
        self.spot_amount = spot_amount
        self.min_spread = min_spread
        self.depth_multiplier = depth_multiplier

        # 设置合约交易对
        base, quote = symbol.split('/')
        self.contract_symbol = f"{base}/{quote}:{quote}"

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

        self.bitget = ccxtpro.bitget({
            'apiKey': bitget_api_key,
            'secret': bitget_api_secret,
            'password': bitget_api_passphrase,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
        })

        # 存储账户余额
        self.gateio_balance = None
        self.bitget_position = None
        
        # 交易结果统计
        self.completed_trades = 0
        self.total_spot_filled = 0
        self.total_contract_filled = 0
        self.trade_results = []

        # 用于存储最新订单簿数据
        self.orderbooks = {
            'gateio': None,
            'bitget': None
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

                contract_position = self.get_contract_position()
                if contract_position < self.spot_amount:
                    raise Exception(f"Bitget合约空单持仓不足，需要 {self.spot_amount}，当前持仓 {contract_position}")

                logger.info("持仓检查通过，可以执行平仓操作")
                return contract_position

        except Exception as e:
            logger.error(f"初始化失败: {str(e)}")
            raise
    
    def get_contract_position(self) -> float:
        """获取合约空单持仓数量"""
        contract_position = 0
        for position in self.bitget_position:
            if position['symbol'] == self.contract_symbol and position['side'] == 'short':
                contract_position = abs(float(position['contracts']))
        return contract_position

    async def check_positions(self):
        """
        检查Gate.io和Bitget的持仓情况
        """
        try:
            # 并行获取两个交易所的持仓信息
            self.gateio_balance, self.bitget_position = await asyncio.gather(
                self.gateio.fetch_balance(),
                self.bitget.fetch_positions([self.contract_symbol])
            )

            base_currency = self.symbol.split('/')[0]
            spot_position = self.gateio_balance.get(base_currency, {}).get('total', 0)

            contract_position = self.get_contract_position()
            logger.info(f"Bitget合约空单持仓: {contract_position} {base_currency}")

            logger.info(f"当前持仓 - Gate.io现货: {spot_position} {base_currency}, "
                        f"Bitget合约空单: {contract_position} {base_currency}")

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
                        asyncio.create_task(self.bitget.watch_order_book(self.contract_symbol))
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
                                self.orderbooks['bitget'] = ob
                                logger.debug(f"收到Bitget订单簿更新")

                        except Exception as e:
                            logger.error(f"处理订单簿数据时出错: {str(e)}")

                    for task in pending:
                        task.cancel()

                except Exception as e:
                    logger.error(f"订阅订单簿时出错: {str(e)}")
                    await asyncio.sleep(0.3)

        except Exception as e:
            logger.error(f"订单簿订阅循环出错: {str(e)}")
        finally:
            self.ws_running = False
            await asyncio.gather(
                self.gateio.close(),
                self.bitget.close()
            )

    async def execute_trade_if_conditions_met(self):
        """检查价差条件并执行交易"""
        try:
            gateio_ob = self.orderbooks['gateio']
            bitget_ob = self.orderbooks['bitget']

            if not gateio_ob or not bitget_ob:
                logger.debug("等待订单簿数据更新...")
                return None, None

            gateio_bid = Decimal(str(gateio_ob['bids'][0][0]))  # 现货卖出价(买1)
            gateio_bid_volume = Decimal(str(gateio_ob['bids'][0][1]))

            bitget_ask = Decimal(str(bitget_ob['asks'][0][0]))  # 合约买入价(卖1)
            bitget_ask_volume = Decimal(str(bitget_ob['asks'][0][1]))

            spread = gateio_bid - bitget_ask
            spread_percent = spread / bitget_ask

            # 检查数量条件
            min_required_volume = Decimal(str(self.spot_amount)) * Decimal(str(self.depth_multiplier))
            volume_condition_met = (
                gateio_bid_volume >= min_required_volume and
                bitget_ask_volume >= min_required_volume
            )

            # 始终打印价格检查信息
            logger.info(
                f"{self.symbol} "
                f"价格检查 - Gate.io买1: {float(gateio_bid):.6f} (量: {float(gateio_bid_volume):.6f}), "
                f"Bitget卖1: {float(bitget_ask):.6f} (量: {float(bitget_ask_volume):.6f}), "
                f"价差: {float(spread_percent) * 100:.4f}%, "
                f"最小要求: {self.min_spread * 100:.4f}%, "
                f"数量条件: {'满足' if volume_condition_met else '不满足'} "
                f"(最小要求: {float(min_required_volume):.6f})"
            )

            if spread_percent >= self.min_spread and volume_condition_met:
                logger.info(f"{self.symbol}交易条件满足：价差 {float(spread_percent) * 100:.4f}% >= {self.min_spread * 100:.4f}%, "
                          f"Gate.io买1量 {float(gateio_bid_volume):.6f} >= {float(min_required_volume):.6f}, "
                          f"Bitget卖1量 {float(bitget_ask_volume):.6f} >= {float(min_required_volume):.6f}")

                # 准备下单参数
                trade_amount = self.spot_amount
                contract_amount = self.bitget.amount_to_precision(self.contract_symbol, trade_amount)

                # 执行交易
                spot_order, contract_order = await asyncio.gather(
                    self.gateio.create_market_sell_order(
                        symbol=self.symbol,
                        amount=trade_amount
                    ),
                    self.bitget.create_market_buy_order(
                        symbol=self.contract_symbol,
                        amount=contract_amount,
                        params={"reduceOnly": True}  # 确保是平仓操作
                    )
                )

                base_currency = self.symbol.split('/')[0]
                logger.info(f"计划平仓数量: {trade_amount} {base_currency}")
                logger.info(f"在Gate.io市价卖出 {trade_amount} {base_currency}")
                logger.info(f"在Bitget市价平空单 {contract_amount} {base_currency}")

                # 检查交易结果
                await self.verify_trade_result(spot_order, contract_order)
                
                # 成功执行一次交易，更新计数器
                self.completed_trades += 1

                # 检查平仓后的持仓情况
                await self.check_positions()

                return spot_order, contract_order

            return None, None

        except Exception as e:
            logger.error(f"执行交易时出错: {str(e)}")
            raise

    async def verify_trade_result(self, spot_order, contract_order):
        """验证交易结果"""
        try:
            # 获取订单ID
            spot_order_id = spot_order.get('id')
            contract_order_id = contract_order.get('id')
            base_currency = self.symbol.split('/')[0]
            
            logger.info(f"验证交易结果 - Gate.io订单ID: {spot_order_id}, Bitget订单ID: {contract_order_id}")
            
            # 获取最新的订单信息
            # 先等待一秒，让API状态有时间更新
            await asyncio.sleep(1)
            
            # 获取Gate.io订单最新状态
            updated_spot_order = None
            if spot_order_id:
                try:
                    # 尝试多种方式获取Gate.io订单状态
                    try:
                        # 先尝试从已完成订单列表中获取
                        closed_orders = await self.gateio.fetch_closed_orders(self.symbol, since=int(time.time() * 1000) - 60000)
                        for order in closed_orders:
                            if order.get('id') == spot_order_id:
                                updated_spot_order = order
                                logger.debug(f"从已完成订单中找到Gate.io订单: {order.get('id')}")
                                break
                    except Exception as e:
                        logger.debug(f"通过fetch_closed_orders获取Gate.io订单状态失败: {str(e)}")
                    
                    # 如果从已完成订单中未找到，直接查询单个订单
                    if not updated_spot_order:
                        updated_spot_order = await self.gateio.fetch_order(spot_order_id, self.symbol)
                        logger.debug(f"通过fetch_order获取Gate.io订单状态: {updated_spot_order.get('status')}")
                    
                    if updated_spot_order:
                        spot_order = updated_spot_order
                        logger.debug(f"获取到Gate.io最新订单状态: {spot_order.get('status')}")
                except Exception as e:
                    logger.warning(f"获取Gate.io订单更新失败: {str(e)}")
                    import traceback
                    logger.error(f"获取Gate.io订单错误堆栈: {traceback.format_exc()}", exc_info=True)
            
            # 获取Bitget订单最新状态
            updated_contract_order = None
            if contract_order_id:
                try:
                    # 多种方式尝试获取订单状态
                    error_messages = []
                    
                    # 方法1: 尝试获取已完成订单信息
                    try:
                        updated_contract_order = await self.bitget.fetch_closed_order(contract_order_id, self.contract_symbol)
                        logger.debug("成功从fetch_closed_order获取Bitget订单")
                    except Exception as e:
                        error_messages.append(f"fetch_closed_order失败: {str(e)}")
                        
                    # 方法2: 如果方法1失败，尝试获取普通订单
                    if updated_contract_order is None:
                        try:
                            updated_contract_order = await self.bitget.fetch_order(contract_order_id, self.contract_symbol)
                            logger.debug("成功从fetch_order获取Bitget订单")
                        except Exception as e:
                            error_messages.append(f"fetch_order失败: {str(e)}")
                    
                    # 方法3: 如果前两种方法都失败，尝试获取最近订单列表
                    if updated_contract_order is None:
                        try:
                            recent_orders = await self.bitget.fetch_orders(self.contract_symbol, limit=10)
                            for order in recent_orders:
                                if order.get('id') == contract_order_id:
                                    updated_contract_order = order
                                    logger.debug("成功从fetch_orders获取Bitget订单")
                                    break
                        except Exception as e:
                            error_messages.append(f"fetch_orders失败: {str(e)}")
                    
                    if updated_contract_order:
                        contract_order = updated_contract_order
                        logger.debug(f"获取到Bitget最新订单状态: {contract_order.get('status')}")
                    else:
                        logger.warning(f"无法获取Bitget订单更新，尝试的方法都失败: {', '.join(error_messages)}")
                except Exception as e:
                    logger.warning(f"获取Bitget订单更新失败: {str(e)}")
                    import traceback
                    logger.debug(f"获取Bitget订单错误堆栈: {traceback.format_exc()}")
                    
            # 记录订单详细信息
            logger.debug(f"Gate.io订单最终详情: {spot_order}")
            logger.debug(f"Bitget订单最终详情: {contract_order}")
            
            # 详细分析Gate.io现货订单
            spot_status = spot_order.get('status')
            spot_filled = float(spot_order.get('filled', 0))
            spot_amount = float(spot_order.get('amount', 0))
            spot_fill_percent = (spot_filled / spot_amount * 100) if spot_amount > 0 else 0
            
            # 计算Gate.io平均成交价格
            spot_cost = float(spot_order.get('cost', 0))
            spot_avg_price = spot_cost / spot_filled if spot_filled > 0 else 0
            
            # 详细分析Bitget合约订单
            contract_status = contract_order.get('status')
            contract_filled = float(contract_order.get('filled', 0))
            contract_amount = float(contract_order.get('amount', 0))
            contract_fill_percent = (contract_filled / contract_amount * 100) if contract_amount > 0 else 0
            
            # 计算Bitget平均成交价格
            contract_cost = float(contract_order.get('cost', 0))
            contract_avg_price = contract_cost / contract_filled if contract_filled > 0 else 0
            
            # 确保订单状态正确
            valid_statuses = ['closed', 'filled']
            
            # Gate.io订单验证标准：
            # 1. 状态应该是已完成或已成交
            # 2. 或者成交率应该达到95%以上（对于市价单，可能API反馈的状态不准确）
            gate_verification_passed = (
                spot_status in valid_statuses or 
                spot_fill_percent >= 95 or
                spot_filled > 0  # 只要有成交量就算通过
            )
            
            # Bitget订单验证标准：
            # 同样检查状态或成交率
            bitget_verification_passed = (
                contract_status in valid_statuses or
                contract_fill_percent >= 95 or
                contract_filled > 0  # 只要有成交量就算通过
            )
            
            if not gate_verification_passed:
                raise Exception(f"Gate.io订单状态异常: {spot_status}, 成交量: {spot_filled}, 成交率: {spot_fill_percent:.2f}%")
                
            if not bitget_verification_passed:
                raise Exception(f"Bitget订单状态异常: {contract_status}, 成交量: {contract_filled}, 成交率: {contract_fill_percent:.2f}%")
            
            # 更新统计数据
            self.total_spot_filled += spot_filled
            self.total_contract_filled += contract_filled
            
            # 获取手续费
            spot_fees = spot_order.get('fees', [])
            quote_fee = sum(float(fee['cost']) for fee in spot_fees if fee['currency'] == 'USDT')
            
            # 获取预期价格（从订单簿数据）
            expected_spot_price = float(self.orderbooks['gateio']['bids'][0][0])
            expected_contract_price = float(self.orderbooks['bitget']['asks'][0][0])
            
            # 计算滑点
            spot_slippage = (spot_avg_price - expected_spot_price) / expected_spot_price
            contract_slippage = (expected_contract_price - contract_avg_price) / expected_contract_price
            
            # 计算实际价差和预期价差
            expected_spread = (expected_spot_price - expected_contract_price) / expected_contract_price
            actual_spread = (spot_avg_price - contract_avg_price) / contract_avg_price
            spread_slippage = actual_spread - expected_spread
            
            logger.info(f"Gate.io实际成交数量: {spot_filled} {base_currency}, 平均价格: {spot_avg_price:.5f}, 手续费: {quote_fee} USDT")
            logger.info(f"Bitget实际成交数量: {contract_filled} {base_currency}, 平均价格: {contract_avg_price:.5f}")
            
            logger.info(f"Gate.io滑点: 预期价格 {expected_spot_price:.5f}, 实际成交价 {spot_avg_price:.5f}, 滑点率 {spot_slippage*100:.4f}%")
            logger.info(f"Bitget滑点: 预期价格 {expected_contract_price:.5f}, 实际成交价 {contract_avg_price:.5f}, 滑点率 {contract_slippage*100:.4f}%")
            logger.info(f"价差滑点: 预期价差 {expected_spread*100:.4f}%, 实际价差 {actual_spread*100:.4f}%, 价差损失 {spread_slippage*100:.4f}%")
            
            # 检查数量是否匹配（允许1%的误差）
            diff_percent = abs(spot_filled - contract_filled) / max(spot_filled, contract_filled)
            if diff_percent > 0.01:  # 误差超过1%
                raise Exception(f"交易数量不匹配: Gate.io {spot_filled}, Bitget {contract_filled}, "
                               f"误差: {diff_percent * 100:.2f}%")
                               
            # 记录本次交易结果
            trade_result = {
                'timestamp': time.time(),
                'spot_filled': spot_filled,
                'contract_filled': contract_filled,
                'spot_order_id': spot_order['id'],
                'contract_order_id': contract_order['id'],
                'fee': quote_fee,
                'spot_avg_price': spot_avg_price,
                'contract_avg_price': contract_avg_price,
                'spot_slippage': spot_slippage,
                'contract_slippage': contract_slippage,
                'spread_slippage': spread_slippage
            }
            self.trade_results.append(trade_result)
            
            logger.info(f"交易验证通过: 第{self.completed_trades+1}次, "
                       f"Gate.io成交: {spot_filled}, Bitget成交: {contract_filled}")
                       
        except Exception as e:
            logger.error(f"交易结果验证失败: {str(e)}")
            raise
    
    def print_trade_summary(self, total_count, initial_position):
        """打印交易统计结果"""
        try:
            base_currency = self.symbol.split('/')[0]
            current_position = self.get_contract_position()
            
            summary = [
                f"\n{'=' * 50}",
                f"交易统计结果",
                f"{'=' * 50}",
                f"交易对: {self.symbol}",
                f"已完成交易次数: {self.completed_trades}/{total_count}",
                f"初始合约空单持仓: {initial_position} {base_currency}",
                f"当前合约空单持仓: {current_position} {base_currency}",
                f"已平仓总量: {self.total_contract_filled} {base_currency}",
                f"总计现货卖出: {self.total_spot_filled} {base_currency}",
                f"{'=' * 50}"
            ]
            
            for line in summary:
                logger.info(line)
                
        except Exception as e:
            logger.error(f"打印交易摘要时出错: {str(e)}")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Gate.io现货卖出与Bitget合约平空单交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, required=True, help='卖出的现货数量')
    parser.add_argument('-p', '--min-spread', type=float, default=0.0005, help='最小价差要求，默认0.001 (0.1%%)')
    parser.add_argument('-m', '--depth-multiplier', type=float, default=2.0, help='订单簿中买一/卖一量至少是交易量的倍数，默认2倍')
    parser.add_argument('-d', '--debug', action='store_true', help='启用调试日志模式')
    parser.add_argument('-c', '--count', type=int, help='重复交易次数，不指定则根据持仓自动计算')
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
            min_spread=args.min_spread,
            depth_multiplier=args.depth_multiplier
        )
        
        # 初始化并获取当前合约持仓
        initial_position = await trader.initialize()
        
        # 计算总交易次数
        if args.count is not None:
            total_count = args.count
        else:
            # 根据合约持仓和单次交易数额计算总次数
            total_count = max(0, int(initial_position / args.amount) - 1)
        
        logger.info(f"计划执行交易次数: {total_count}次")
        
        # 启动订单簿订阅
        subscription_task = asyncio.create_task(trader.subscribe_orderbooks())
        
        try:
            # 执行指定次数的交易
            attempt_count = 0
            while trader.completed_trades < total_count:
                attempt_count += 1
                # logger.info(f"开始第{attempt_count}次尝试 (已完成{trader.completed_trades}/{total_count}次交易)")
                
                try:
                    # 每次交易前重新检查持仓，确保有足够的资产
                    await trader.check_positions()
                    base_currency = args.symbol.split('/')[0]
                    current_amount = float(trader.gateio_balance.get(base_currency, {}).get('free', 0))
                    
                    if current_amount < args.amount:
                        # 如果现货持仓不足，尝试从理财中赎回
                        need_spot_amount = args.amount - current_amount + 0.1
                        try:
                            logger.info(f"Gate.io {base_currency}余额不足，从理财中赎回 {need_spot_amount} {base_currency}")
                            redeem_earn(base_currency, need_spot_amount)
                            # 再检查余额是否够
                            await trader.check_positions()
                            current_amount = float(trader.gateio_balance.get(base_currency, {}).get('free', 0))
                            if current_amount < args.amount:
                                raise Exception(f"Gate.io {base_currency}余额不足且赎回后仍不足，需要 {args.amount}，"
                                              f"当前可用 {current_amount}")
                        except Exception as e:
                            logger.error(f"理财赎回失败: {str(e)}")
                            # 打印交易摘要并退出
                            trader.print_trade_summary(total_count, initial_position)
                            return 1
                    
                    # 检查合约持仓是否足够
                    contract_position = trader.get_contract_position()
                    if contract_position < args.amount:
                        logger.error(f"Bitget合约空单持仓不足，需要 {args.amount}，当前持仓 {contract_position}")
                        # 打印交易摘要并退出
                        trader.print_trade_summary(total_count, initial_position)
                        return 1
                    
                    # 执行交易
                    spot_order, contract_order = await trader.execute_trade_if_conditions_met()
                    if spot_order is None or contract_order is None:
                        logger.warning(f"第{attempt_count}次尝试交易条件不满足，等待下一次机会")
                        await asyncio.sleep(1)  # 等待1秒后继续
                        continue
                        
                    logger.info(f"第{trader.completed_trades}/{total_count}次交易完成")
                    
                    # 如果不是最后一次交易，等待几秒后再继续
                    if trader.completed_trades < total_count:
                        # 检查是否还有足够的合约持仓继续交易
                        contract_position = trader.get_contract_position()
                        if contract_position < args.amount:
                            logger.warning(f"合约持仓不足以继续交易，当前持仓: {contract_position}，需要: {args.amount}")
                            logger.info(f"已完成 {trader.completed_trades}/{total_count} 次交易，但无法继续执行剩余交易")
                            break
                        
                        logger.info(f"已完成 {trader.completed_trades}/{total_count} 次交易，等待3秒后继续下一次交易...")
                        await asyncio.sleep(3)
                
                except Exception as e:
                    logger.error(f"执行第{attempt_count}次尝试过程中出错: {str(e)}")
                    # 打印交易摘要并退出
                    trader.print_trade_summary(total_count, initial_position)
                    return 1
        finally:
            # 停止订单簿订阅
            trader.ws_running = False
            if subscription_task:
                subscription_task.cancel()
                try:
                    await subscription_task
                except asyncio.CancelledError:
                    pass
        
        logger.info("所有计划交易执行完毕!")
        trader.print_trade_summary(total_count, initial_position)

    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        if 'trader' in locals():
            trader.print_trade_summary(total_count if 'total_count' in locals() else 0, 
                                       initial_position if 'initial_position' in locals() else 0)
        return 1
    finally:
        if 'trader' in locals():
            await asyncio.gather(
                trader.gateio.close(),
                trader.bitget.close()
            )

    return 0


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close()
