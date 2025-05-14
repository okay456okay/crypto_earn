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
        
        # 交易统计
        self.trades_completed = 0
        self.initial_contract_position = 0
        self.current_contract_position = 0
        
        # 交易滑点统计
        self.slippage_stats = {
            'gateio': [],
            'bybit': []
        }

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

    async def execute_unhedge_trade_optimized(self):
        """
        优化版本：合并订单簿监控和交易执行，减少延迟以降低滑点
        """
        subscription_task = None
        try:
            logger.info(f"开始监控{self.symbol}订单簿并等待最佳交易时机")
            
            # 准备要交易的数量
            trade_amount = self.spot_amount
            contract_amount = self.bybit.amount_to_precision(self.contract_symbol, trade_amount)
            base_currency = self.symbol.split('/')[0]
            
            # 记录开始等待的时间，用于计算总等待时间
            start_wait_time = time.time()
            
            # 用于记录关键时间点
            time_stats = {
                "start_time": start_wait_time,
                "last_price_update": 0,
                "condition_met": 0,
                "order_sent": 0
            }
            
            # 订阅订单簿直到找到符合条件的价格
            self.ws_running = True
            while self.ws_running:
                try:
                    # 创建并发任务获取两个交易所的订单簿
                    tasks = [
                        asyncio.create_task(self.gateio.watch_order_book(self.symbol)),
                        asyncio.create_task(self.bybit.watch_order_book(self.contract_symbol))
                    ]

                    # 等待任一任务完成
                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED
                    )

                    # 处理完成的任务
                    for task in done:
                        try:
                            ob = task.result()
                            
                            # 记录收到价格更新的时间
                            price_update_time = time.time()
                            time_stats["last_price_update"] = price_update_time
                            
                            if task == tasks[0]:
                                self.orderbooks['gateio'] = ob
                                logger.debug(f"收到Gate.io订单簿更新")
                            else:
                                self.orderbooks['bybit'] = ob
                                logger.debug(f"收到Bybit订单簿更新")

                            # 取消其他待处理的任务
                            for p in pending:
                                p.cancel()
                                
                            # 只有当两边订单簿都有数据时才检查价差
                            if self.orderbooks['gateio'] and self.orderbooks['bybit']:
                                # 检查价差和数量是否满足交易条件
                                gateio_ob = self.orderbooks['gateio']
                                bybit_ob = self.orderbooks['bybit']
                                
                                # 获取价格和数量
                                gateio_bid = Decimal(str(gateio_ob['bids'][0][0]))  # 现货卖出价(买1)
                                gateio_bid_volume = Decimal(str(gateio_ob['bids'][0][1]))  # 现货买1量
                                
                                bybit_ask = Decimal(str(bybit_ob['asks'][0][0]))  # 合约买入价(卖1)
                                bybit_ask_volume = Decimal(str(bybit_ob['asks'][0][1]))  # 合约卖1量
                                
                                # 计算价差
                                spread = gateio_bid - bybit_ask
                                spread_percent = spread / bybit_ask
                                
                                # 判断价格和数量是否满足条件
                                price_ok = spread_percent >= self.min_spread
                                gateio_volume_ok = gateio_bid_volume >= (Decimal(str(trade_amount)) * Decimal(str(self.depth_multiplier)))
                                bybit_volume_ok = bybit_ask_volume >= (Decimal(str(contract_amount)) * Decimal(str(self.depth_multiplier)))
                                
                                # 记录价格检查信息
                                logger.debug(
                                    f"{self.symbol} 价格检查 - "
                                    f"Gate.io买1: {float(gateio_bid):.6f} (量: {float(gateio_bid_volume):.6f}), "
                                    f"Bybit卖1: {float(bybit_ask):.6f} (量: {float(bybit_ask_volume):.6f}), "
                                    f"价差: {float(spread_percent) * 100:.4f}%, "
                                    f"价格条件: {'满足' if price_ok else '不满足'}, "
                                    f"Gate.io量条件: {'满足' if gateio_volume_ok else '不满足'}, "
                                    f"Bybit量条件: {'满足' if bybit_volume_ok else '不满足'}"
                                )
                                
                                # 如果所有条件都满足，立即执行交易
                                if price_ok and gateio_volume_ok and bybit_volume_ok:
                                    # 记录满足交易条件的时间点
                                    condition_met_time = time.time()
                                    time_stats["condition_met"] = condition_met_time
                                    
                                    # 计算从最后价格更新到满足条件的时间间隔
                                    price_to_condition_interval = condition_met_time - time_stats["last_price_update"]
                                    
                                    # 记录等待时间
                                    wait_duration = condition_met_time - start_wait_time
                                    
                                    logger.info(f"{self.symbol}交易条件满足："
                                               f"价差 {float(spread_percent) * 100:.4f}% >= {self.min_spread * 100:.4f}%, "
                                               f"Gate.io买1量 {float(gateio_bid_volume):.6f} >= {float(trade_amount * self.depth_multiplier):.6f}, "
                                               f"Bybit卖1量 {float(bybit_ask_volume):.6f} >= {float(float(contract_amount) * self.depth_multiplier):.6f}, "
                                               f"等待耗时: {wait_duration:.3f}秒")
                                    
                                    logger.info(f"时间统计 - 从最后价格更新到满足条件: {price_to_condition_interval * 1000:.2f}毫秒")
                                    
                                    # 保存交易前的价格用于计算滑点
                                    pre_trade_prices = {
                                        'gateio_bid': float(gateio_bid),
                                        'bybit_ask': float(bybit_ask),
                                        'spread_percent': float(spread_percent)
                                    }
                                    
                                    # 停止订阅，准备执行交易
                                    self.ws_running = False
                                    
                                    # 执行交易 - 必须尽快，减少时间延迟
                                    pre_order_time = time.time()
                                    condition_to_order_interval = pre_order_time - time_stats["condition_met"]
                                    logger.info(f"时间统计 - 从满足条件到准备下单: {condition_to_order_interval * 1000:.2f}毫秒")
                                    
                                    # 记录下单开始时间
                                    time_stats["order_sent"] = pre_order_time
                                    
                                    execution_start_time = time.time()
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
                                    execution_duration = time.time() - execution_start_time

                                    logger.debug(f"订单提交结果 - Gate.io现货订单: {spot_order}")
                                    logger.debug(f"订单提交结果 - Bybit合约订单: {contract_order}")
                                    # 记录完整的时间链
                                    total_process_time = time.time() - start_wait_time
                                    logger.info(f"时间统计 - 完整流程:"
                                               f"\n- 总等待时间: {wait_duration:.3f}秒"
                                               f"\n- 价格更新→满足条件: {price_to_condition_interval * 1000:.2f}毫秒"
                                               f"\n- 满足条件→下单请求: {condition_to_order_interval * 1000:.2f}毫秒"
                                               f"\n- 下单请求→完成下单: {execution_duration * 1000:.2f}毫秒"
                                               f"\n- 总处理时间: {total_process_time:.3f}秒")
                                    
                                    # 记录下单耗时
                                    logger.info(f"下单执行耗时: {execution_duration:.3f}秒")
                                    logger.info(f"在Gate.io市价卖出 {trade_amount} {base_currency}")
                                    logger.info(f"在Bybit市价平空单 {contract_amount} {base_currency}")
                                    
                                    # 等待订单状态更新
                                    await asyncio.sleep(1)
                                    
                                    # 验证交易结果并计算滑点
                                    await self.verify_trade_result(spot_order, contract_order, pre_trade_prices)
                                    
                                    # 检查平仓后的持仓情况
                                    await self.check_positions()
                                    
                                    # 更新交易统计
                                    self.trades_completed += 1
                                    
                                    return spot_order, contract_order

                        except Exception as e:
                            logger.error(f"处理订单簿数据时出错: {str(e)}", exc_info=True)
                            raise

                except asyncio.CancelledError:
                    logger.info(f"{self.symbol}订单簿监控任务被取消", exc_info=True)
                    break
                except Exception as e:
                    logger.error(f"订阅订单簿时出错: {str(e)}", exc_info=True)
                    raise

        except Exception as e:
            logger.error(f"执行优化版平仓交易时出错: {str(e)}", exc_info=True)
            raise
        finally:
            self.ws_running = False
            # 确保取消所有可能运行的任务
            if subscription_task and not subscription_task.done():
                subscription_task.cancel()
            
            # 关闭WebSocket连接
            try:
                await asyncio.gather(
                    self.gateio.close(),
                    self.bybit.close()
                )
            except:
                pass

    async def verify_trade_result(self, spot_order, contract_order, pre_trade_prices=None):
        """
        验证交易结果是否符合预期，并计算交易滑点
        
        Args:
            spot_order: Gate.io现货订单结果
            contract_order: Bybit合约订单结果
            pre_trade_prices: 交易前的价格数据，用于计算滑点
        
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
                    
                    # # 如果在关闭订单中没找到，查找未完成订单
                    # if not bybit_order_found:
                    #     open_orders = await self.bybit.fetch_open_orders(self.contract_symbol, limit=20)
                    #     for order in open_orders:
                    #         if order.get('id') == contract_order_id:
                    #             updated_contract_order = order
                    #             bybit_order_found = True
                    #             logger.debug(f"从未完成订单中获取到Bybit订单信息: {updated_contract_order}")
                    #             break
                    
                    # 如果两种方式都未找到订单，尝试直接查询
                    # if not bybit_order_found:
                    #     try:
                    #         order_result = await self.bybit.fetch_order(contract_order_id, self.contract_symbol)
                    #         if order_result:
                    #             updated_contract_order = order_result
                    #             bybit_order_found = True
                    #             logger.debug(f"通过直接查询获取到Bybit订单信息: {updated_contract_order}")
                    #     except Exception as e:
                    #         logger.warning(f"直接查询Bybit订单失败: {str(e)}")
                    
                    # 如果所有方法都未找到订单
                    # if not bybit_order_found:
                    #     raise Exception(f"无法找到Bybit订单 {contract_order_id} 的最新状态")
                        
                except Exception as e:
                    logger.error(f"获取Bybit订单详情失败: {str(e)}")
                    raise Exception(f"无法获取Bybit订单状态，验证失败: {str(e)}")
            else:
                raise Exception("Bybit订单ID无效，无法验证订单状态")

            logger.debug(f"订单执行结果 - Gate.io现货订单: {updated_spot_order}")
            logger.debug(f"订单执行结果 - Bybit合约订单: {updated_contract_order}")
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
            
            # 获取成交数量和价格 - 必须有成交
            spot_filled = float(updated_spot_order.get('filled', 0))
            spot_price = float(updated_spot_order.get('average', 0))
            contract_filled = float(updated_contract_order.get('filled', 0))
            contract_price = float(updated_contract_order.get('average', 0))
            
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
            logger.info(f"Gate.io实际成交数量: {spot_filled} {base_currency}, 平均价格: {spot_price}, 手续费: {spot_quote_fee} USDT")
            
            contract_fees = updated_contract_order.get('fees', [])
            contract_quote_fee = sum(float(fee.get('cost', 0)) for fee in contract_fees if fee.get('currency') == 'USDT')
            logger.info(f"Bybit实际成交数量: {contract_filled} {base_currency}, 平均价格: {contract_price}, 手续费: {contract_quote_fee} USDT")
            
            # 计算交易滑点 - 如果有预期价格数据
            if pre_trade_prices:
                # Gate.io卖出滑点 - 实际成交价比预期价格低的比例
                gateio_expected_price = pre_trade_prices['gateio_bid']
                if spot_price > 0 and gateio_expected_price > 0:
                    gateio_slippage = (gateio_expected_price - spot_price) / gateio_expected_price
                    self.slippage_stats['gateio'].append(gateio_slippage)
                    logger.info(f"Gate.io滑点: 预期价格 {gateio_expected_price:.6f}, 实际成交价 {spot_price:.6f}, "
                               f"滑点率 {gateio_slippage * 100:.4f}%")
                
                # Bybit买入滑点 - 实际成交价比预期价格高的比例
                bybit_expected_price = pre_trade_prices['bybit_ask']
                if contract_price > 0 and bybit_expected_price > 0:
                    bybit_slippage = (contract_price - bybit_expected_price) / bybit_expected_price
                    self.slippage_stats['bybit'].append(bybit_slippage)
                    logger.info(f"Bybit滑点: 预期价格 {bybit_expected_price:.6f}, 实际成交价 {contract_price:.6f}, "
                               f"滑点率 {bybit_slippage * 100:.4f}%")
                
                # 总滑点 - 原始价差与实际价差的差异
                expected_spread = pre_trade_prices['spread_percent']
                actual_spread = 0
                if contract_price > 0:
                    actual_spread = (spot_price - contract_price) / contract_price
                    
                spread_diff = expected_spread - actual_spread
                logger.info(f"价差滑点: 预期价差 {expected_spread * 100:.4f}%, 实际价差 {actual_spread * 100:.4f}%, "
                           f"价差损失 {spread_diff * 100:.4f}%")
            
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
        
        # 添加滑点统计信息
        if self.slippage_stats['gateio']:
            avg_gateio_slippage = sum(self.slippage_stats['gateio']) / len(self.slippage_stats['gateio'])
            max_gateio_slippage = max(self.slippage_stats['gateio'])
            min_gateio_slippage = min(self.slippage_stats['gateio'])
            logger.info(f"- Gate.io滑点统计: 平均 {avg_gateio_slippage * 100:.4f}%, "
                       f"最大 {max_gateio_slippage * 100:.4f}%, 最小 {min_gateio_slippage * 100:.4f}%")
        
        if self.slippage_stats['bybit']:
            avg_bybit_slippage = sum(self.slippage_stats['bybit']) / len(self.slippage_stats['bybit'])
            max_bybit_slippage = max(self.slippage_stats['bybit'])
            min_bybit_slippage = min(self.slippage_stats['bybit'])
            logger.info(f"- Bybit滑点统计: 平均 {avg_bybit_slippage * 100:.4f}%, "
                       f"最大 {max_bybit_slippage * 100:.4f}%, 最小 {min_bybit_slippage * 100:.4f}%")
        
        logger.info("=" * 50)


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Gate.io现货卖出与Bybit合约平空单交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, required=True, help='卖出的现货数量')
    parser.add_argument('-p', '--min-spread', type=float, default=0.0005, help='最小价差要求，默认0.0005 (0.05%%)')
    parser.add_argument('-m', '--depth-multiplier', type=float, default=2.0, help='订单簿中买一/卖一量至少是交易量的倍数，默认2倍')
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
            min_spread=args.min_spread,
            depth_multiplier=args.depth_multiplier
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
            
        # 输出交易参数
        logger.info(f"交易参数 - 交易对: {args.symbol}, 数量: {args.amount}, 最小价差: {args.min_spread * 100:.4f}%, "
                   f"订单簿深度要求: 交易量的{args.depth_multiplier}倍")
        
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
                
                # 执行交易 - 使用优化版本的交易方法
                logger.info(f"执行第 {execution_count + 1}/{total_count} 次交易，等待最佳交易时机...")
                spot_order, contract_order = await trader.execute_unhedge_trade_optimized()
                if not (spot_order and contract_order):
                    raise Exception("交易执行失败，未能成功下单")
                
                execution_count += 1
                logger.info(f"完成第 {execution_count}/{total_count} 次交易")
                
                # 每次交易后简单汇报
                trader.print_trading_summary(total_count)
                
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
            try:
                await asyncio.gather(
                    trader.gateio.close(),
                    trader.bybit.close()
                )
            except:
                pass

    return 0


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 