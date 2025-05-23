#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io现货卖出与Binance合约平空单的套利脚本

此脚本实现以下功能：
1. 从Gate.io市价卖出指定token的现货
2. 从Binance平掉对应的合约空单进行对冲
3. 确保现货和合约仓位保持一致
4. 检查价差是否满足最小套利条件
5. 监控和记录交易执行情况
6. 支持重复交易直到达到指定次数
7. 交易结果验证和错误恢复
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
from config import binance_api_key, binance_api_secret, gateio_api_secret, gateio_api_key, proxies
from trade.gateio_api import redeem_earn


class UnhedgeTrader:
    """
    现货-合约对冲平仓类，实现Gate.io现货卖出与Binance合约平空单
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
        self.contract_symbol = f"{base}{quote}"  # Binance合约格式，如 DOGEUSDT

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

        self.binance = ccxtpro.binance({
            'apiKey': binance_api_key,
            'secret': binance_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
            'options': {
                'defaultType': 'future',  # 使用合约交易
            }
        })

        # 存储账户余额
        self.gateio_balance = None
        self.binance_position = None

        # 用于存储最新订单簿数据
        self.orderbooks = {
            'gateio': None,
            'binance': None
        }

        # 用于控制WebSocket订阅
        self.ws_running = False
        
        # 交易统计信息
        self.completed_trades = 0
        self.trade_results = []

        # 新增：用于记录关键时间点
        self.price_update_time = None  # 接收到价格更新的时间
        self.condition_met_time = None  # 满足交易条件的时间
        self.order_start_time = None  # 开始下单的时间
        self.order_submit_time = None  # 订单提交完成的时间

    async def initialize(self):
        """
        异步初始化方法，执行需要网络请求的初始化操作
        """
        try:
            logger.info(f"初始化: 交易对={self.symbol}, 合约对={self.contract_symbol}, "
                        f"最小价差={self.min_spread * 100}%")

            # 检查当前持仓情况
            await self.check_positions()
            
            return True
        except Exception as e:
            logger.error(f"初始化失败: {str(e)}")
            raise

    async def check_positions(self):
        """
        检查Gate.io和Binance的持仓情况
        
        Returns:
            Tuple[float, float]: (现货持仓量, 合约持仓量)
        """
        try:
            # 并行获取两个交易所的持仓信息
            logger.debug(f"正在获取Gate.io和Binance持仓信息...")
            
            fetch_start_time = time.time()
            self.gateio_balance, self.binance_position = await asyncio.gather(
                self.gateio.fetch_balance(),
                self.binance.fetch_positions([self.contract_symbol])
            )
            fetch_time = time.time() - fetch_start_time
            logger.debug(f"获取持仓信息耗时: {fetch_time:.3f}秒")

            # 解析现货持仓
            base_currency = self.symbol.split('/')[0]
            base_total = self.gateio_balance.get(base_currency, {}).get('total', 0)
            base_free = self.gateio_balance.get(base_currency, {}).get('free', 0)
            base_used = self.gateio_balance.get(base_currency, {}).get('used', 0)
            spot_position = float(base_total)
            
            # 记录现货详细信息
            logger.debug(f"Gate.io {base_currency}详细信息 - 总量: {base_total}, 可用: {base_free}, 冻结: {base_used}")
            
            # 解析合约持仓
            contract_position = 0
            for position in self.binance_position:
                if position['info']['symbol'] == self.contract_symbol and position['side'] == 'short':
                    contract_position = abs(float(position['contracts']))
                    position_entry_price = position.get('entryPrice', 'N/A')
                    position_leverage = position.get('leverage', 'N/A')
                    position_margin_type = position.get('marginType', 'N/A')
                    position_unrealized_pnl = position.get('unrealizedPnl', 'N/A')
                    
                    logger.debug(f"Binance合约空单详细信息 - 持仓量: {contract_position}, 开仓均价: {position_entry_price}, "
                               f"杠杆倍数: {position_leverage}, 保证金类型: {position_margin_type}, "
                               f"未实现盈亏: {position_unrealized_pnl}")
                    logger.info(f"Binance合约空单持仓: {contract_position} {base_currency}")

            logger.info(f"当前持仓 - Gate.io现货: {spot_position} {base_currency}, "
                        f"Binance合约空单: {contract_position} {base_currency}")
            
            # 计算持仓差异
            if spot_position > 0 and contract_position > 0:
                position_diff = abs(spot_position - contract_position)
                position_diff_percent = position_diff / spot_position * 100 if spot_position > 0 else 0
                
                logger.debug(f"持仓差异 - 数量: {position_diff} {base_currency}, 百分比: {position_diff_percent:.2f}%")
                
                if position_diff_percent > 1:
                    logger.warning(f"现货与合约持仓差异较大: {position_diff_percent:.2f}% > 1%")
            
            return spot_position, contract_position

        except Exception as e:
            logger.error(f"检查持仓时出错: {str(e)}")
            # 记录详细的错误信息
            import traceback
            logger.debug(f"检查持仓的错误堆栈:\n{traceback.format_exc()}")
            raise

    async def verify_positions_for_trade(self):
        """
        验证持仓是否满足交易条件
        
        Returns:
            bool: 是否满足交易条件
        """
        if self.spot_amount is None:
            return False
            
        base_currency = self.symbol.split('/')[0]
        current_amount = float(self.gateio_balance.get(base_currency, {}).get('free', 0))
        
        if current_amount < self.spot_amount:
            try:
                # 如果现货持仓不足，自动从理财中赎回缺失的部分，然后再检查, +0.1是防止不太够
                need_spot_amount = self.spot_amount - current_amount + 0.1
                logger.info(f"现货余额不足，从理财赎回 {need_spot_amount} {base_currency}")
                redeem_earn(base_currency, need_spot_amount)
                # 再检查余额是否够
                await self.check_positions()
                current_amount = float(self.gateio_balance.get(base_currency, {}).get('free', 0))
                if current_amount < self.spot_amount:
                    raise Exception(f"Gate.io {base_currency}余额不足，需要 {self.spot_amount}，"
                                    f"当前可用 {self.gateio_balance[base_currency]['free']}，赎回理财后仍不足")
            except Exception as e:
                logger.error(f"理财赎回失败: {str(e)}")
                raise

        contract_position = 0
        for position in self.binance_position:
            if position['info']['symbol'] == self.contract_symbol and position['side'] == 'short':
                contract_position = abs(float(position['contracts']))

        if contract_position < self.spot_amount:
            raise Exception(f"Binance合约空单持仓不足，需要 {self.spot_amount}，当前持仓 {contract_position}")

        logger.info("持仓检查通过，可以执行平仓操作")
        return True

    async def execute_unhedge_trade(self):
        """
        执行平仓交易 - 合并了价差检查和交易执行逻辑
        
        Returns:
            Tuple[Dict, Dict, bool]: (现货订单信息, 合约订单信息, 交易是否成功)
        """
        try:
            # 验证持仓是否满足交易条件
            if not await self.verify_positions_for_trade():
                logger.error("持仓不满足交易条件，无法执行交易")
                return None, None, False

            # 订阅订单簿
            self.ws_running = True
            subscription_task = None
            
            try:
                while self.ws_running:
                    try:
                        # 并行订阅两个交易所的订单簿
                        tasks = [
                            asyncio.create_task(self.gateio.watch_order_book(self.symbol)),
                            asyncio.create_task(self.binance.watch_order_book(self.contract_symbol))
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
                                    self.orderbooks['binance'] = ob
                                    logger.debug(f"收到Binance订单簿更新")

                            except Exception as e:
                                logger.error(f"处理订单簿数据时出错: {str(e)}")

                        for task in pending:
                            task.cancel()

                        # 等待两个交易所的订单簿数据都更新
                        if not self.orderbooks['gateio'] or not self.orderbooks['binance']:
                            await asyncio.sleep(0.1)
                            continue

                        # 记录接收到价格更新的时间
                        self.price_update_time = time.time()

                        # 检查价差和深度
                        gateio_ob = self.orderbooks['gateio']
                        binance_ob = self.orderbooks['binance']

                        gateio_bid = Decimal(str(gateio_ob['bids'][0][0]))  # 现货卖出价(买1)
                        gateio_bid_volume = Decimal(str(gateio_ob['bids'][0][1]))

                        binance_ask = Decimal(str(binance_ob['asks'][0][0]))  # 合约买入价(卖1)
                        binance_ask_volume = Decimal(str(binance_ob['asks'][0][1]))

                        spread = gateio_bid - binance_ask
                        spread_percent = spread / binance_ask

                        # 检查深度是否满足要求
                        min_required_volume = Decimal(str(self.spot_amount)) * Decimal(str(self.depth_multiplier))
                        depth_satisfied = gateio_bid_volume >= min_required_volume and binance_ask_volume >= min_required_volume

                        logger.debug(
                            f"{self.symbol}"
                            f"价格检查 - Gate.io买1: {float(gateio_bid)} (量: {float(gateio_bid_volume)}), "
                            f"Binance卖1: {float(binance_ask)} (量: {float(binance_ask_volume)}), "
                            f"价差: {float(spread_percent) * 100:.4f}%")

                        if spread_percent >= self.min_spread and depth_satisfied:
                            # 记录满足条件的时间
                            self.condition_met_time = time.time()

                            logger.info(f"{self.symbol}交易条件满足：价差 {float(spread_percent) * 100:.4f}% >= {self.min_spread * 100:.4f}%, "
                                      f"Gate.io买1量 {float(gateio_bid_volume):.6f} >= {float(min_required_volume):.6f}, "
                                      f"Binance卖1量 {float(binance_ask_volume):.6f} >= {float(min_required_volume):.6f}, "
                                      f"Gate.io买1: {float(gateio_bid)} (量: {float(gateio_bid_volume)}), "
                                      f"Binance卖1: {float(binance_ask)} (量: {float(binance_ask_volume)}"
                                        )
                            
                            # 准备下单参数
                            trade_amount = self.spot_amount
                            contract_amount = self.binance.amount_to_precision(self.contract_symbol, trade_amount)
                            
                            logger.debug(f"下单参数 - 现货卖出数量: {trade_amount}, 合约平仓数量: {contract_amount}")

                            # 记录开始下单的时间
                            self.order_start_time = time.time()

                            base_currency = self.symbol.split('/')[0]
                            logger.info(f"计划平仓数量: {trade_amount} {base_currency}")
                            logger.info(f"在Gate.io市价卖出 {trade_amount} {base_currency}")
                            logger.info(f"在Binance市价平空单 {contract_amount} {base_currency}")

                            # 执行交易
                            try:
                                spot_order, contract_order = await asyncio.gather(
                                    self.gateio.create_market_sell_order(
                                        symbol=self.symbol,
                                        amount=trade_amount
                                    ),
                                    self.binance.create_market_buy_order(
                                        symbol=self.contract_symbol,
                                        amount=contract_amount,
                                        params={
                                            "positionSide": "SHORT"  # 指定是平空单
                                        }
                                    )
                                )
                                
                                # 记录订单提交完成的时间
                                self.order_submit_time = time.time()
                                
                                # 记录原始订单响应
                                logger.info(f"Gate.io订单提交详情: {spot_order}")
                                logger.info(f"Binance订单提交详情: {contract_order}")
                                
                            except Exception as e:
                                logger.error(f"下单过程出错: {str(e)}")
                                import traceback
                                logger.debug(f"下单错误的堆栈:\n{traceback.format_exc()}")
                                return None, None, False


                            # 等待一小段时间确保订单状态已更新
                            await asyncio.sleep(2)

                            # 获取实际成交结果
                            try:
                                # 获取Gate.io订单的最新状态
                                spot_order_id = spot_order.get('id')
                                if spot_order_id:
                                    logger.debug(f"获取Gate.io订单({spot_order_id})的最新状态")
                                    updated_spot_order = await self.gateio.fetch_order(spot_order_id, self.symbol)
                                    if updated_spot_order:
                                        logger.debug(f"Gate.io订单状态更新为: {updated_spot_order.get('status')}")
                                        spot_order = updated_spot_order
                                    else:
                                        logger.warning(f"无法获取Gate.io订单({spot_order_id})的最新状态")
                                
                                # 获取Binance订单的最新状态
                                contract_order_id = contract_order.get('id')
                                if contract_order_id:
                                    logger.debug(f"获取Binance订单({contract_order_id})的最新状态")
                                    try:
                                        updated_contract_order = await self.binance.fetch_order(contract_order_id, self.contract_symbol)
                                        if updated_contract_order:
                                            logger.debug(f"Binance订单状态更新为: {updated_contract_order.get('status')}")
                                            contract_order = updated_contract_order
                                        else:
                                            logger.warning(f"无法获取Binance订单({contract_order_id})的最新状态")
                                    except Exception as e:
                                        logger.warning(f"通过fetch_order获取Binance订单状态失败: {str(e)}")

                                logger.info(f"Gate.io订单执行详情: {spot_order}")
                                logger.info(f"Binance订单执行详情: {contract_order}")

                                # 记录详细的订单信息用于调试
                                logger.debug(f"Gate.io订单详情: ID={spot_order.get('id')}, 状态={spot_order.get('status')}, "
                                            f"成交量={spot_order.get('filled')}, 成交价={spot_order.get('price')}")
                                logger.debug(f"Binance订单详情: ID={contract_order.get('id')}, 状态={contract_order.get('status')}, "
                                            f"成交量={contract_order.get('filled')}, 成交价={contract_order.get('price')}")

                                filled_amount = float(spot_order.get('filled', 0))
                                spot_price = float(spot_order.get('price', 0)) if spot_order.get('price') else None
                                fees = spot_order.get('fees', [])
                                quote_fee = sum(float(fee.get('cost', 0)) for fee in fees if fee.get('currency') == 'USDT')

                                logger.info(f"Gate.io实际成交数量: {filled_amount} {base_currency}, "
                                            f"平均价格: {spot_price:.5f}, 手续费: {quote_fee} USDT")
                                            
                                # 记录合约成交数据
                                contract_filled = float(contract_order.get('filled', 0))
                                contract_price = float(contract_order.get('price', 0)) if contract_order.get('price') else None
                                contract_fees = contract_order.get('fees', [])
                                contract_fee = sum(float(fee.get('cost', 0)) for fee in contract_fees)
                                contract_fee_currency = contract_fees[0].get('currency') if contract_fees else 'unknown'
                                
                                logger.info(f"Binance合约实际平仓数量: {contract_filled} {base_currency}, "
                                            f"平均价格: {contract_price:.5f}, 手续费: {contract_fee} {contract_fee_currency}")

                                # 计算滑点
                                if spot_price and float(gateio_bid):
                                    spot_slippage = (spot_price - float(gateio_bid)) / float(gateio_bid)
                                    logger.info(f"Gate.io滑点: 预期价格 {float(gateio_bid):.6f}, "
                                               f"实际成交价 {spot_price:.6f}, 滑点率 {spot_slippage * 100:.4f}%")

                                if contract_price and float(binance_ask):
                                    contract_slippage = (contract_price - float(binance_ask)) / float(binance_ask)
                                    logger.info(f"Binance滑点: 预期价格 {float(binance_ask):.6f}, "
                                               f"实际成交价 {contract_price:.6f}, 滑点率 {contract_slippage * 100:.4f}%")

                                # 计算实际价差
                                if spot_price and contract_price:
                                    actual_spread = (spot_price - contract_price) / contract_price
                                    spread_loss = float(spread_percent) - actual_spread
                                    logger.info(f"价差滑点: 预期价差 {float(spread_percent) * 100:.4f}%, "
                                               f"实际价差 {actual_spread * 100:.4f}%, 价差损失 {spread_loss * 100:.4f}%")
                                            
                                # 记录详细的成交信息
                                logger.info("=" * 50)
                                logger.info(f"【成交详情】订单执行情况:")
                                logger.info(f"{self.symbol} Gate.io滑点: 预期价格 {float(gateio_bid):.5f}, 实际成交价 {spot_price:.5f}, 滑点率 {(spot_price - float(gateio_bid)) / float(gateio_bid) * 100:.4f}%")
                                logger.info(f"{self.symbol} Binance滑点: 预期价格 {float(binance_ask):.5f}, 实际成交价 {contract_price:.5f}, 滑点率 {(contract_price - float(binance_ask)) / float(binance_ask) * 100:.4f}%")
                                logger.info(f"{self.symbol} 价差滑点: 预期价差 {float(spread_percent) * 100:.4f}%, 实际价差 {(spot_price - contract_price) / contract_price * 100:.4f}%, 价差损失 {((spot_price - contract_price) / contract_price - float(spread_percent)) * 100:.4f}%")
                                logger.info(f"【成交详情】Gate.io实际成交: {filled_amount} {base_currency}, 手续费: {quote_fee} USDT, 实际持仓: {filled_amount} {base_currency}")
                                logger.info(f"【成交详情】Binance合约实际成交: {contract_filled} {base_currency}")
                                logger.info("=" * 50)

                                # 输出时间统计信息
                                if self.price_update_time and self.condition_met_time and self.order_start_time and self.order_submit_time:
                                    logger.info(f"{self.symbol} 时间统计 - 完整流程:")
                                    logger.info(f"{self.symbol} 时间统计 - 从接收到价格更新[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.price_update_time))}]到满足条件[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.condition_met_time))}]: {(self.condition_met_time - self.price_update_time) * 1000:.1f}毫秒")
                                    logger.info(f"{self.symbol} 时间统计 - 从满足条件[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.condition_met_time))}]到下单前[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.order_start_time))}]: {(self.order_start_time - self.condition_met_time) * 1000:.1f}毫秒")
                                    logger.info(f"{self.symbol} 时间统计 - 从下单前[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.order_start_time))}]到订单提交[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.order_submit_time))}]: {(self.order_submit_time - self.order_start_time) * 1000:.1f}毫秒")

                            except Exception as e:
                                logger.error(f"获取成交结果时出错: {str(e)}", exc_info=True)
                                return None, None, False

                            # 验证订单结果
                            is_successful = await self.verify_order_results(spot_order, contract_order)
                            
                            if not is_successful:
                                logger.error("交易结果验证失败")
                                return spot_order, contract_order, False

                            # 检查平仓后的持仓情况
                            await self.check_positions()
                            
                            # 更新交易统计
                            self.completed_trades += 1
                            self.trade_results.append({
                                'time': time.strftime('%Y-%m-%d %H:%M:%S'),
                                'spot_filled': float(spot_order.get('filled', 0)),
                                'contract_filled': float(contract_order.get('filled', 0)),
                                'spot_price': spot_price,
                                'contract_price': contract_price,
                                'spot_fee': quote_fee if 'quote_fee' in locals() else None,
                                'trade_amount': trade_amount,
                                # 新增：记录关键时间点
                                'price_update_time': self.price_update_time,
                                'condition_met_time': self.condition_met_time,
                                'order_start_time': self.order_start_time,
                                'order_submit_time': self.order_submit_time
                            })
                            
                            logger.info(f"交易成功完成 - 第 {self.completed_trades} 次交易")
                            return spot_order, contract_order, True

                        if not depth_satisfied:
                            logger.debug(f"{self.symbol}深度条件不满足: 需要 {float(min_required_volume):.6f}, "
                                       f"Gate.io买1量: {float(gateio_bid_volume):.6f}, "
                                       f"Binance卖1量: {float(binance_ask_volume):.6f}")
                        else:
                            logger.debug(f"{self.symbol}价差条件不满足: {float(spread_percent) * 100:.4f}% < {self.min_spread * 100:.4f}%")

                        # 短暂等待后继续检查
                        await asyncio.sleep(0.1)

                    except Exception as e:
                        logger.error(f"订阅订单簿时出错: {str(e)}")
                        await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"执行平仓交易时出错: {str(e)}")
                import traceback
                logger.error(f"执行平仓交易的错误堆栈:\n{traceback.format_exc()}")
                raise
            finally:
                self.ws_running = False
                if subscription_task:
                    subscription_task.cancel()

        except Exception as e:
            logger.error(f"执行平仓交易时出错: {str(e)}")
            import traceback
            logger.error(f"执行平仓交易的错误堆栈:\n{traceback.format_exc()}")
            raise

    async def verify_order_results(self, spot_order, contract_order):
        """
        验证订单执行结果是否符合预期
        
        Args:
            spot_order: Gate.io现货订单结果
            contract_order: Binance合约订单结果
            
        Returns:
            bool: 订单执行是否成功
        """
        base_currency = self.symbol.split('/')[0]
        
        try:
            # 获取最新的订单状态
            try:
                # 获取Gate.io订单的最新状态
                spot_order_id = spot_order.get('id')
                if spot_order_id:
                    logger.debug(f"获取Gate.io订单({spot_order_id})的最新状态")
                    updated_spot_order = await self.gateio.fetch_order(spot_order_id, self.symbol)
                    if updated_spot_order:
                        logger.debug(f"Gate.io订单状态更新为: {updated_spot_order.get('status')}")
                        spot_order = updated_spot_order
                    else:
                        logger.warning(f"无法获取Gate.io订单({spot_order_id})的最新状态")
                
                # 获取Binance订单的最新状态
                contract_order_id = contract_order.get('id')
                if contract_order_id:
                    logger.debug(f"获取Binance订单({contract_order_id})的最新状态")
                    try:
                        updated_contract_order = await self.binance.fetch_order(contract_order_id, self.contract_symbol)
                        if updated_contract_order:
                            logger.debug(f"Binance订单状态更新为: {updated_contract_order.get('status')}")
                            contract_order = updated_contract_order
                        else:
                            logger.warning(f"无法获取Binance订单({contract_order_id})的最新状态")
                    except Exception as e:
                        logger.warning(f"通过fetch_order获取Binance订单状态失败: {str(e)}")
                        try:
                            # 尝试从已完成订单列表中查找
                            closed_orders = await self.binance.fetch_closed_orders(self.contract_symbol, limit=10)
                            for order in closed_orders:
                                if order.get('id') == contract_order_id:
                                    logger.debug(f"从已完成订单列表获取到Binance订单状态: {order.get('status')}")
                                    contract_order = order
                                    break
                        except Exception as e2:
                            logger.warning(f"通过fetch_closed_orders获取Binance订单状态失败: {str(e2)}")
                
                # 等待一小段时间确保订单状态已完全更新
                await asyncio.sleep(1)
            except Exception as e:
                logger.warning(f"获取订单最新状态时出错: {str(e)}")
            
            # 记录详细的订单信息用于调试
            logger.debug(f"Gate.io订单详情: ID={spot_order.get('id')}, 状态={spot_order.get('status')}, "
                        f"成交量={spot_order.get('filled')}, 成交价={spot_order.get('price')}")
            logger.debug(f"Binance订单详情: ID={contract_order.get('id')}, 状态={contract_order.get('status')}, "
                        f"成交量={contract_order.get('filled')}, 成交价={contract_order.get('price')}")
            
            # 检查订单状态
            valid_statuses = ['closed', 'filled']
            if spot_order.get('status') not in valid_statuses:
                logger.error(f"Gate.io订单执行异常，状态为: {spot_order.get('status')}")
                return False
                
            if contract_order.get('status') not in valid_statuses:
                logger.error(f"Binance合约订单执行异常，状态为: {contract_order.get('status')}")
                return False
            
            # 检查成交量
            spot_filled = float(spot_order.get('filled', 0))
            contract_filled = float(contract_order.get('filled', 0))
            
            # 检查是否有成交
            if spot_filled <= 0:
                logger.error(f"Gate.io订单没有成交量")
                return False
                
            if contract_filled <= 0:
                logger.error(f"Binance合约订单没有成交量")
                return False
            
            # 计算误差百分比
            error_percent = abs(spot_filled - contract_filled) / spot_filled if spot_filled > 0 else float('inf')
            
            # 允许的误差范围 (0.5%)
            allowed_error = 0.005
            
            if error_percent > allowed_error:
                logger.error(f"交易量不匹配: Gate.io卖出 {spot_filled} {base_currency}, "
                            f"Binance平仓 {contract_filled} {base_currency}, "
                            f"误差: {error_percent * 100:.2f}% > 允许误差: {allowed_error * 100:.2f}%")
                return False
                
            logger.info(f"交易结果验证通过: Gate.io卖出 {spot_filled} {base_currency}, "
                        f"Binance平仓 {contract_filled} {base_currency}, "
                        f"误差: {error_percent * 100:.2f}%")
            return True
                
        except Exception as e:
            logger.error(f"验证订单结果时出错: {str(e)}")
            # 记录详细的错误信息
            import traceback
            logger.debug(f"验证订单结果的错误堆栈:\n{traceback.format_exc()}")
            return False

    def get_trade_summary(self):
        """
        获取交易摘要信息
        
        Returns:
            Dict: 交易摘要信息
        """
        base_currency = self.symbol.split('/')[0]
        
        total_spot_filled = sum(result['spot_filled'] for result in self.trade_results)
        total_contract_filled = sum(result['contract_filled'] for result in self.trade_results)
        total_fees = sum(result['spot_fee'] for result in self.trade_results if result.get('spot_fee'))
        
        spot_position = 0
        contract_position = 0
        
        if self.gateio_balance and self.binance_position:
            spot_position = float(self.gateio_balance.get(base_currency, {}).get('total', 0))
            
            for position in self.binance_position:
                if position['info']['symbol'] == self.contract_symbol and position['side'] == 'short':
                    contract_position = abs(float(position['contracts']))
        
        return {
            'completed_trades': self.completed_trades,
            'base_currency': base_currency,
            'total_spot_filled': total_spot_filled,
            'total_contract_filled': total_contract_filled,
            'total_fees': total_fees,
            'remaining_spot': spot_position,
            'remaining_contract': contract_position,
            'trade_details': self.trade_results
        }


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Gate.io现货卖出与Binance合约平空单交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, default=-1, help='每次卖出的现货数量，默认为-1表示自动计算')
    parser.add_argument('-p', '--min-spread', type=float, default=0.003, help='最小价差要求，默认0.003 (0.3%%)')
    parser.add_argument('-m', '--depth-multiplier', type=float, default=5.0, help='订单簿中买一/卖一量至少是交易量的倍数，默认5倍')
    parser.add_argument('-d', '--debug', action='store_true', help='启用调试日志模式')
    parser.add_argument('-c', '--count', type=int, help='重复交易次数，默认为合约持仓/单次交易数额-1')
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
    try:
        # 获取当前价格
        base_currency = args.symbol.split('/')[0]
        logger.info(f"开始获取{base_currency}当前价格，当前amount参数值: {args.amount}")
        
        # 尝试从Binance获取价格
        try:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={base_currency}USDT"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, proxy=proxies.get('https')) as response:
                    if response.status == 200:
                        data = await response.json()
                        spot_price = float(data['price'])
                        logger.info(f"从Binance获取到{base_currency}当前价格: {spot_price} USDT")
                    else:
                        raise Exception(f"Binance API请求失败: {response.status}")
        except Exception as e:
            logger.warning(f"从Binance获取价格失败: {str(e)}，尝试从Gate.io获取")
            # 如果Binance失败，尝试从Gate.io获取价格
            try:
                gateio = ccxtpro.gateio({
                    'apiKey': gateio_api_key,
                    'secret': gateio_api_secret,
                    'enableRateLimit': True,
                    'proxies': proxies,
                })
                ticker = await gateio.fetch_ticker(args.symbol)
                spot_price = float(ticker['last'])
                logger.info(f"从Gate.io获取到{base_currency}当前价格: {spot_price} USDT")
                await gateio.close()
            except Exception as e2:
                raise Exception(f"从Gate.io获取价格也失败: {str(e2)}")

        # 如果amount为-1，使用calculate_order_quantity计算数量
        if args.amount == -1:
            from tools.mymath import calculate_order_quantity
            quantity_result = calculate_order_quantity(spot_price)
            args.amount = quantity_result['quantity']
            logger.info(f"自动计算交易数量: {args.amount} {base_currency} (预计金额: {quantity_result['estimated_amount']:.2f} USDT)")

        trader = UnhedgeTrader(
            symbol=args.symbol,
            spot_amount=args.amount,
            min_spread=args.min_spread,
            depth_multiplier=args.depth_multiplier
        )
        
        # 初始化交易状态
        logger.info("初始化交易环境...")
        await trader.initialize()
        
        # 获取当前持仓状态
        logger.info("获取当前持仓状态...")
        spot_position, contract_position = await trader.check_positions()
        
        # 计算默认重复次数
        if args.count is None:
            logger.info(f"自动计算交易次数 - 当前合约持仓: {contract_position} {base_currency}, 单次交易数量: {args.amount}")
            if args.amount <= 0:
                raise Exception(f"交易数量必须大于0，当前值: {args.amount}")
            
            # 计算最大可交易次数：合约持仓/单次交易数额-1 (保留一次手动操作的空间)
            if contract_position <= args.amount:
                # 如果合约持仓小于等于单次交易数量，则只能执行一次
                count = 1
                logger.info(f"合约持仓({contract_position})仅够执行一次交易，设置执行次数为: 1")
            else:
                # 计算可以执行的最大次数
                max_possible = int(contract_position / args.amount)
                # 保留一次手动操作的空间，但不低于1
                count = max(1, max_possible - 1)
                logger.info(f"计算得出可执行次数: {count} (总持仓: {contract_position}, 单次数量: {args.amount}, 最大可执行: {max_possible})")
        else:
            count = args.count
            # 检查指定的次数是否超过了实际可执行的最大次数
            max_possible = int(contract_position / args.amount)
            if count > max_possible:
                logger.warning(f"指定的交易次数({count})超过了当前合约持仓可支持的最大次数({max_possible})，"
                              f"将自动调整为: {max_possible}")
                count = max_possible
            
            logger.info(f"使用用户指定的交易次数: {count}")
        
        # 检查是否有足够的合约持仓进行操作
        if contract_position < args.amount:
            logger.error(f"合约持仓不足，无法执行交易。当前持仓: {contract_position} {args.symbol.split('/')[0]}，需要: {args.amount} {args.symbol.split('/')[0]}")
            raise Exception(f"合约持仓不足")
            
        # 执行重复交易
        for i in range(count):
            try:
                logger.info("=" * 50)
                logger.info(f"开始执行第 {i+1}/{count} 次交易")
                
                # 每次交易前重新检查持仓情况
                if i > 0:
                    logger.info(f"第 {i+1}/{count} 次交易前重新检查持仓...")
                    spot_position, contract_position = await trader.check_positions()
                    
                    # 检查是否有足够的合约持仓
                    if contract_position < args.amount:
                        logger.warning(f"合约持仓不足，无法继续执行。当前持仓: {contract_position} {args.symbol.split('/')[0]}")
                        logger.info(f"已完成 {i}/{count} 次交易，因合约持仓不足退出程序")
                        break
                
                # 执行交易
                logger.info(f"执行第 {i+1}/{count} 次交易操作...")
                spot_order, contract_order, trade_success = await trader.execute_unhedge_trade()
                
                if not trade_success:
                    logger.error(f"第 {i+1}/{count} 次交易失败，停止后续交易")
                    break
                    
                logger.info(f"第 {i+1}/{count} 次交易成功完成!")
                
                # 最后一次交易不需要等待
                if i < count - 1:
                    # 每次交易之间等待3秒
                    wait_seconds = 0.3
                    logger.debug(f"等待 {wait_seconds} 秒后进行下一次交易...")
                    await asyncio.sleep(wait_seconds)
            except Exception as e:
                logger.error(f"第 {i+1}/{count} 次交易过程中发生错误: {str(e)}")
                # 记录详细的错误信息
                import traceback
                logger.error(f"交易错误的堆栈:\n{traceback.format_exc()}")
                break
                
        # 交易全部完成
        if trader.completed_trades > 0:
            success = True
            logger.info(f"交易任务已完成，成功执行了 {trader.completed_trades}/{count} 次交易")
        else:
            logger.error(f"未能成功执行任何交易")
        
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        # 记录详细的错误信息
        import traceback
        logger.debug(f"主程序错误的堆栈:\n{traceback.format_exc()}")
    finally:
        # 打印交易摘要
        if trader:
            logger.info("=" * 50)
            logger.info("获取最终持仓状态...")
            # 再次检查最新持仓情况
            try:
                await trader.check_positions()
            except Exception as e:
                logger.warning(f"获取最终持仓状态时出错: {str(e)}")
                
            # 获取交易摘要
            summary = trader.get_trade_summary()
            
            # 打印交易结果
            logger.info(f"\n========== 交易结果摘要 ==========")
            logger.info(f"币种: {summary['base_currency']}")
            logger.info(f"计划交易次数: {count}")
            logger.info(f"实际完成交易: {summary['completed_trades']} 次")
            logger.info(f"现货总成交量: {summary['total_spot_filled']} {summary['base_currency']}")
            logger.info(f"合约总平仓量: {summary['total_contract_filled']} {summary['base_currency']}")
            logger.info(f"总手续费: {summary['total_fees']} USDT")
            logger.info(f"剩余现货余额: {summary['remaining_spot']} {summary['base_currency']}")
            logger.info(f"剩余合约持仓: {summary['remaining_contract']} {summary['base_currency']}")
            logger.info(f"交易状态: {'成功' if success else '失败'}")
            logger.info(f"==============================\n")
            
            # 打印每笔交易的详细信息
            if args.debug and summary['trade_details']:
                logger.debug("\n===== 每笔交易详情 =====")
                for idx, trade in enumerate(summary['trade_details']):
                    logger.debug(f"交易 #{idx+1}")
                    logger.debug(f"时间: {trade['time']}")
                    logger.debug(f"现货成交: {trade['spot_filled']} {summary['base_currency']}")
                    logger.debug(f"合约平仓: {trade['contract_filled']} {summary['base_currency']}")
                    logger.debug(f"手续费: {trade.get('spot_fee', 'N/A')} USDT")
                    logger.debug(f"现货价格: {trade.get('spot_price', 'N/A')}")
                    logger.debug(f"合约价格: {trade.get('contract_price', 'N/A')}")
                    logger.debug("---------------------")
            
            # 关闭交易所连接
            logger.info("关闭交易所连接...")
            try:
                await asyncio.gather(
                    trader.gateio.close(),
                    trader.binance.close()
                )
                logger.debug("已关闭所有交易所连接")
            except Exception as e:
                logger.warning(f"关闭交易所连接时出错: {str(e)}")

    logger.info(f"程序执行完毕，退出代码: {0 if success else 1}")
    return 0 if success else 1


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return_code = loop.run_until_complete(main())
        sys.exit(return_code)
    finally:
        loop.close() 