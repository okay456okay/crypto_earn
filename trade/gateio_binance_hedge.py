#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io现货买入与Binance合约空单对冲的套利脚本

此脚本实现以下功能：
1. 从Gate.io市价买入指定token的现货
2. 从Binance开对应的合约空单进行对冲
3. 确保现货和合约仓位保持一致
4. 检查价差是否满足最小套利条件
5. 监控和记录交易执行情况
"""

import sys
import os
import logging
import argparse
from decimal import Decimal
import asyncio
import ccxt.pro as ccxtpro  # 使用 ccxt pro 版本
import time

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import binance_api_key, binance_api_secret, gateio_api_secret, gateio_api_key, proxies
from trade.gateio_api import subscrible_earn as gateio_subscrible_earn, redeem_earn


class HedgeTrader:
    def __init__(self, symbol, spot_amount=None, min_spread=0.001, leverage=None, test_mode=False, depth_multiplier=10):
        """
        初始化基本属性
        
        Args:
            symbol (str): 交易对符号，例如 'ETH/USDT'
            spot_amount (float, optional): 现货交易数量
            min_spread (float, optional): 最小价差要求，默认0.001 (0.1%)
            leverage (int, optional): 合约杠杆倍数，如果不指定则使用该交易对支持的最大杠杆倍数
            test_mode (bool, optional): 是否为测试模式，默认False
            depth_multiplier (int, optional): 市场深度乘数要求，默认10倍交易量
        """
        self.symbol = symbol
        self.spot_amount = spot_amount
        self.min_spread = min_spread
        self.leverage = leverage  # 初始化为None，将在initialize中设置
        self.test_mode = test_mode
        self.depth_multiplier = depth_multiplier  # 市场深度乘数要求
        
        # 设置合约交易对
        base, quote = symbol.split('/')
        self.contract_symbol = f"{base}{quote}"  # Binance合约格式，如 'ETHUSDT'
        
        # 使用 ccxt pro 初始化交易所
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
            'ws_proxy':  proxies.get('https', None),
            'wss_proxy':  proxies.get('https', None),
            'ws_socks_proxy':  proxies.get('https', None),
            'options': {
                'defaultType': 'future',  # 设置为合约模式
            }
        })
        
        self.gateio_usdt = 0
        self.binance_usdt = None
        
        # 新增：用于跟踪现货和合约持仓差异的累计值
        self.cumulative_position_diff = 0  # 正数表示合约多，负数表示现货多
        self.cumulative_position_diff_usdt = 0  # 以USDT计价的差额
        self.trade_count = 0  # 交易计数
        self.trade_records = []  # 交易记录

    async def get_max_leverage(self):
        """
        获取Binance交易所支持的最大杠杆倍数
        
        Returns:
            int: 最大杠杆倍数
            
        Raises:
            Exception: 获取最大杠杆倍数失败时抛出异常
        """
        try:
            # 获取交易对信息
            response = await self.binance.fapiPublicGetExchangeInfo()
            
            if response and 'symbols' in response:
                for symbol_info in response['symbols']:
                    if symbol_info['symbol'] == self.contract_symbol:
                        # 获取杠杆倍数信息
                        leverage_info = await self.binance.fapiPrivateGetLeverageBracket({
                            'symbol': self.contract_symbol
                        })
                        
                        if leverage_info and 'brackets' in leverage_info[0]:
                            max_leverage = int(leverage_info[0]['brackets'][0]['initialLeverage'])
                            logger.info(f"获取到{self.contract_symbol}最大杠杆倍数: {max_leverage}倍")
                            return max_leverage
            
            raise Exception(f"未能获取到{self.contract_symbol}的最大杠杆倍数")
            
        except Exception as e:
            logger.error(f"获取最大杠杆倍数时出错: {str(e)}")
            raise Exception(f"获取{self.contract_symbol}最大杠杆倍数失败: {str(e)}")

    async def initialize(self):
        """
        异步初始化方法，执行需要网络请求的初始化操作
        """
        try:
            # 测试模式下使用默认杠杆倍数
            if self.test_mode:
                if self.leverage is None:
                    self.leverage = 20  # 测试模式下使用20倍杠杆作为默认值
                logger.info(f"测试模式: 使用杠杆倍数 {self.leverage}倍")
                logger.info(f"初始化完成: 交易对={self.symbol}, 合约对={self.contract_symbol}, "
                           f"最小价差={self.min_spread*100}%, 杠杆={self.leverage}倍, "
                           f"市场深度要求={self.depth_multiplier}倍交易量")
                # 在测试模式下假设有足够的余额
                self.gateio_usdt = 10000
                self.binance_usdt = 10000
                logger.info(f"测试模式: 假设账户余额 - Gate.io: {self.gateio_usdt} USDT, Binance: {self.binance_usdt} USDT")
                return
            
            # 生产模式下的初始化
            # 如果杠杆倍数未指定，获取最大杠杆倍数
            if self.leverage is None:
                self.leverage = await self.get_max_leverage()
                logger.info(f"使用Binance支持的最大杠杆倍数: {self.leverage}倍")
            else:
                # 检查指定的杠杆倍数是否超过最大限制
                max_leverage = await self.get_max_leverage()
                if self.leverage > max_leverage:
                    logger.warning(f"指定的杠杆倍数 {self.leverage} 超过最大限制 {max_leverage}，将使用最大杠杆倍数")
                    self.leverage = max_leverage
                else:
                    logger.info(f"使用指定的杠杆倍数: {self.leverage}倍")

            # 设置Binance合约参数
            await self.binance.fapiPrivatePostLeverage({
                'symbol': self.contract_symbol,
                'leverage': self.leverage
            })
            logger.info(f"设置Binance合约杠杆倍数为: {self.leverage}倍")
            
            logger.info(f"初始化完成: 交易对={self.symbol}, 合约对={self.contract_symbol}, "
                       f"最小价差={self.min_spread*100}%, 杠杆={self.leverage}倍, "
                       f"市场深度要求={self.depth_multiplier}倍交易量")
            
            # 获取并保存账户余额
            self.gateio_usdt, self.binance_usdt = await self.check_balances()
            
            # 检查余额是否满足交易要求
            if self.spot_amount is not None:
                orderbook = await self.gateio.fetch_order_book(self.symbol)
                current_price = float(orderbook['asks'][0][0])
                
                required_usdt = float(self.spot_amount) * current_price * 1.02
                required_margin = float(self.spot_amount) * current_price / self.leverage * 1.05

                if required_usdt > self.gateio_usdt or self.gateio_usdt <= 50:
                    # raise Exception(f"Gate.io USDT余额不足，需要约 {required_usdt:.2f} USDT，当前余额 {self.gateio_usdt:.2f} USDT")
                    redeem_earn('USDT', max(required_usdt * 1.01, 50))
                    # 重新获取并保存账户余额
                    self.gateio_usdt, self.binance_usdt = await self.check_balances()
                    if required_usdt > self.gateio_usdt:
                        raise Exception(
                            f"Gate.io USDT余额不足，需要约 {required_usdt:.2f} USDT，当前余额 {self.gateio_usdt:.2f} USDT")
                if required_margin > self.binance_usdt:
                    raise Exception(f"Binance USDT保证金不足，需要约 {required_margin:.2f} USDT，当前余额 {self.binance_usdt:.2f} USDT")

                logger.info(f"账户余额检查通过 - 预估所需Gate.io: {required_usdt:.2f} USDT, Binance: {required_margin:.2f} USDT")
                
        except Exception as e:
            logger.exception(f"初始化失败: {str(e)}")
            raise

    async def execute_hedge_trade(self):
        """
        优化版的对冲交易执行函数 - 合并了订单簿订阅和交易执行，最小化延迟
        
        直接监听订单簿并在满足条件时立即执行交易，不使用队列进行中间传递
        """
        try:
            # 记录开始时间
            start_time = time.time()
            
            logger.info(f"开始监听 {self.symbol} 的交易机会...")
            logger.info(f"条件: 最小价差 >= {self.min_spread*100:.4f}%, 市场深度 >= {self.depth_multiplier}倍交易量")
            
            # 记录上次价差日志的时间，避免过于频繁的日志输出
            last_spread_log_time = 0
            
            # 测试模式下的模拟订单簿数据
            if self.test_mode:
                # 模拟测试数据
                base = self.symbol.split('/')[0]
                quote = self.symbol.split('/')[1]
                
                # 生成一个符合条件的模拟订单簿数据
                await asyncio.sleep(2)  # 模拟等待找到机会的时间
                
                # 模拟订单簿数据
                base_price = 3000.0 if base == 'ETH' else 60000.0 if base == 'BTC' else 1.0
                gateio_ask = base_price * 0.998  # 稍低的卖价
                binance_bid = base_price * 1.002  # 稍高的买价
                
                # 确保价差满足要求
                spread = binance_bid - gateio_ask
                spread_percent = spread / gateio_ask
                
                if spread_percent < self.min_spread:
                    # 调整价格以确保满足最小价差
                    binance_bid = gateio_ask * (1 + self.min_spread * 1.5)
                
                # 重新计算价差
                spread = binance_bid - gateio_ask
                spread_percent = spread / gateio_ask
                
                # 模拟足够的市场深度
                depth_multiple = self.depth_multiplier * 2
                gateio_ask_volume = self.spot_amount * depth_multiple
                binance_bid_volume = self.spot_amount * depth_multiple
                
                # 记录找到的机会
                opportunity_duration = 2.0  # 模拟等待时间
                logger.info(f"{self.symbol}交易条件满足 (等待了 {opportunity_duration:.2f}秒):")
                logger.info(f"现货卖一:{gateio_ask}(买入价)，合约买一:{binance_bid}(卖出价), 价差: {spread_percent*100:.4f}% >= {self.min_spread*100:.4f}%")
                logger.info(f"市场深度 - Gate.io卖1: {gateio_ask_volume} {base}, "
                          f"Binance买1: {binance_bid_volume} {base}")
                logger.info(f"交易量: {self.spot_amount} {base}, "
                          f"深度比例 - Gate.io: {gateio_ask_volume/self.spot_amount:.2f}x, "
                          f"Binance: {binance_bid_volume/self.spot_amount:.2f}x")
                
                logger.info("=" * 50)
                logger.info("交易计划:")
                logger.info(f"Gate.io 现货买入: {self.spot_amount} {base} @ {gateio_ask} {quote}")
                logger.info(f"预计成本: {float(self.spot_amount) * float(gateio_ask):.2f} {quote}")
                logger.info(f"Binance 合约开空: {self.spot_amount} {self.contract_symbol} @ {binance_bid} {quote}")
                logger.info(f"当前价差: {spread_percent*100:.4f}%")
                logger.info("=" * 50)
                logger.info("测试模式: 不执行实际交易")
                return None, None
            
            # 实际模式的订单簿监控和交易逻辑
            while True:
                try:
                    # 创建两个任务来并行获取最新订单簿
                    gateio_task = asyncio.create_task(self.gateio.watch_order_book(self.symbol))
                    binance_task = asyncio.create_task(self.binance.watch_order_book(self.contract_symbol))
                    
                    # 等待两个订单簿都更新
                    try:
                        gateio_ob, binance_ob = await asyncio.gather(gateio_task, binance_task)
                    except Exception as e:
                        logger.error(f"获取订单簿数据失败, 错误: {e}")
                        continue
                    
                    # 快速提取需要的价格和数量数据
                    gateio_ask = float(gateio_ob['asks'][0][0])
                    gateio_ask_volume = float(gateio_ob['asks'][0][1])
                    binance_bid = float(binance_ob['bids'][0][0])
                    binance_bid_volume = float(binance_ob['bids'][0][1])
                    
                    # 计算价差
                    spread = binance_bid - gateio_ask
                    spread_percent = spread / gateio_ask
                    
                    # 检查市场深度
                    depth_sufficient = (
                        gateio_ask_volume >= self.spot_amount * self.depth_multiplier and 
                        binance_bid_volume >= self.spot_amount * self.depth_multiplier
                    )
                    
                    # 如果价差满足条件且市场深度足够，直接执行交易
                    if spread_percent >= self.min_spread and depth_sufficient:
                        t0 = time.time()  # 记录满足交易条件的时间点
                        
                        # 记录找到的机会
                        opportunity_duration = t0 - start_time
                        logger.info(f"{self.symbol}交易条件满足 (等待了 {opportunity_duration:.2f}秒):")
                        logger.info(f"价差: {spread_percent*100:.4f}% >= {self.min_spread*100:.4f}%")
                        logger.info(f"市场深度 - Gate.io卖1: {gateio_ask_volume} {self.symbol.split('/')[0]}, "
                                  f"Binance买1: {binance_bid_volume} {self.symbol.split('/')[0]}")
                        logger.info(f"交易量: {self.spot_amount} {self.symbol.split('/')[0]}, "
                                  f"深度比例 - Gate.io: {gateio_ask_volume/self.spot_amount:.2f}x, "
                                  f"Binance: {binance_bid_volume/self.spot_amount:.2f}x")
                        
                        if self.test_mode:
                            logger.info("=" * 50)
                            logger.info("交易计划:")
                            logger.info(f"Gate.io 现货买入: {self.spot_amount} {self.symbol.split('/')[0]} @ {gateio_ask} USDT")
                            logger.info(f"预计成本: {float(self.spot_amount) * float(gateio_ask):.2f} USDT")
                            logger.info(f"Binance 合约开空: {self.spot_amount} {self.contract_symbol} @ {binance_bid} USDT")
                            logger.info(f"当前价差: {spread_percent*100:.4f}%")
                            logger.info("=" * 50)
                            logger.info("测试模式: 不执行实际交易")
                            return None, None
                        
                        # 立即执行交易
                        logger.info(f"执行交易 - 价差: {spread_percent*100:.4f}%, Gate.io: {gateio_ask}, Binance: {binance_bid}")
                        t1 = time.time()
                        logger.debug(f"[时序] 1.记录日志: {(t1-t0)*1000:.3f}ms")
                        
                        # 使用最新的订单簿数据直接执行交易，不再检查价格
                        spot_order, contract_order = await asyncio.gather(
                            self.gateio.create_market_buy_order(
                                symbol=self.symbol,
                                amount=float(self.spot_amount) * float(gateio_ask),
                                params={'createMarketBuyOrderRequiresPrice': False, 'quoteOrderQty': True}
                            ),
                            self.binance.create_market_sell_order(
                                symbol=self.contract_symbol,
                                amount=self.spot_amount,
                                params={'positionSide': 'SHORT'}
                            )
                        )
                        
                        t2 = time.time()
                        logger.debug(f"[时序] ===从满足条件到订单发送完成共耗时: {(t2-t0)*1000:.3f}ms===")
                        
                        # 立即检查订单提交状态并记录订单详情到调试日志
                        logger.info(f"Gate.io现货订单提交详情: {spot_order}")
                        logger.info(f"Binance合约订单提交详情: {contract_order}")
                        
                        # 检查订单是否成功提交
                        if not spot_order or not contract_order:
                            logger.error("订单提交失败 - 未能获取到有效的订单对象")
                            return None, None
                        
                        # 检查订单ID是否存在
                        spot_order_id = spot_order.get('id')
                        contract_order_id = contract_order.get('id')
                        
                        if not spot_order_id or not contract_order_id:
                            logger.error(f"订单提交失败 - 未获取到有效订单ID: Gate.io ID: {spot_order_id}, Binance ID: {contract_order_id}")
                            return None, None
                        
                        # 检查订单初始状态
                        spot_initial_status = spot_order.get('status', '')
                        contract_initial_status = contract_order.get('status', '')
                        
                        # 检查订单初始状态是否明确失败
                        failed_statuses = ['rejected', 'canceled', 'expired']
                        if spot_initial_status in failed_statuses or contract_initial_status in failed_statuses:
                            logger.error(f"订单提交失败 - 初始状态显示失败: Gate.io: {spot_initial_status}, Binance: {contract_initial_status}")
                            return None, None
                            
                        logger.info(f"订单提交成功 - Gate.io ID: {spot_order_id}, Binance ID: {contract_order_id}")
                        logger.debug(f"初始订单状态 - Gate.io: {spot_initial_status}, Binance: {contract_initial_status}")
                        
                        # 等待2秒让订单状态更新
                        await asyncio.sleep(2)
                        
                        # 尝试获取最新的订单状态
                        try:
                            if spot_order_id:
                                updated_spot_order = await self.gateio.fetch_order(spot_order_id, self.symbol)
                                spot_order = updated_spot_order
                            
                            if contract_order_id:
                                updated_contract_order = await self.binance.fetch_order(contract_order_id, self.contract_symbol)
                                contract_order = updated_contract_order
                        except Exception as e:
                            logger.error(f"获取更新后的订单状态时出错: {str(e)}")
                            return None, None

                        logger.info(f"Gate.io现货订单执行详情: {spot_order}")
                        logger.info(f"Binance合约订单执行详情: {contract_order}")
                        # 验证订单执行状态
                        spot_status = spot_order.get('status', '')
                        contract_status = contract_order.get('status', '')
                        
                        logger.info(f"最终订单状态 - Gate.io: {spot_status}, Binance: {contract_status}")
                        
                        # 检查订单是否成功执行
                        valid_statuses = ['closed', 'filled']
                        if spot_status not in valid_statuses or contract_status not in valid_statuses:
                            logger.error(f"订单执行异常 - 现货订单状态: {spot_status}, 合约订单状态: {contract_status}")
                            return None, None
                        
                        # 获取现货订单的实际成交结果
                        spot_filled_amount = float(spot_order.get('info', {}).get('filled_amount',0))
                        if spot_filled_amount <= 0:
                            logger.error(f"Gate.io订单成交量为0，交易可能未成功")
                            return None, None
                        
                        spot_fees = spot_order.get('fees', [])
                        spot_base_fee = sum(float(fee.get('cost', 0)) for fee in spot_fees if fee.get('currency') == self.symbol.split('/')[0])
                        spot_actual_position = spot_filled_amount - spot_base_fee
                        
                        # 计算现货实际成交价格
                        spot_cost = float(spot_order.get('cost', 0))
                        spot_actual_price = spot_cost / spot_filled_amount if spot_filled_amount > 0 else 0
                        spot_price_diff = spot_actual_price - float(gateio_ask)
                        spot_price_diff_percent = (spot_price_diff / float(gateio_ask) * 100) if float(gateio_ask) > 0 else 0
                        
                        # 获取合约订单的实际成交结果
                        contract_filled_amount = float(contract_order.get('filled', 0))
                        if contract_filled_amount <= 0:
                            logger.error(f"Binance合约订单成交量为0，交易可能未成功")
                            return None, None
                        
                        contract_fees = contract_order.get('fees', [])
                        contract_base_fee = sum(float(fee.get('cost', 0)) for fee in contract_fees if fee.get('currency') == self.symbol.split('/')[0])
                        contract_actual_position = contract_filled_amount - contract_base_fee
                        
                        # 计算合约实际成交价格
                        contract_cost = float(contract_order.get('cost', 0))
                        contract_actual_price = contract_cost / contract_filled_amount if contract_filled_amount > 0 else 0
                        contract_price_diff = float(binance_bid) - contract_actual_price
                        contract_price_diff_percent = (contract_price_diff / float(binance_bid) * 100) if float(binance_bid) > 0 else 0
                        
                        # 记录价格执行信息
                        logger.info("=" * 50)
                        logger.info("交易已完成 - 价格执行分析:")
                        logger.info(f"【交易前市场】Gate.io卖1: {gateio_ask}, Binance买1: {binance_bid}, 价差: {spread_percent*100:.4f}%")
                        logger.info(f"【实际成交】现货: {spot_actual_price:.8f} (滑点: {spot_price_diff_percent:.4f}%), 合约: {contract_actual_price:.8f} (滑点: {contract_price_diff_percent:.4f}%)")
                        logger.info(f"【成交数量】现货: {spot_actual_position} {self.symbol.split('/')[0]}, 合约: {contract_actual_position} {self.symbol.split('/')[0]}")
                        logger.info(f"总滑点成本: {(spot_price_diff + contract_price_diff) * float(spot_actual_position):.8f} USDT")
                        
                        # 检查本次操作的现货和合约持仓差异
                        position_diff = contract_actual_position - spot_actual_position  # 正值表示合约多，负值表示现货多
                        position_diff_abs = abs(position_diff)
                        current_price = float(gateio_ask)
                        position_diff_usdt = position_diff_abs * current_price
                        
                        # 更新累计差额
                        self.cumulative_position_diff += position_diff
                        self.cumulative_position_diff_usdt = self.cumulative_position_diff * current_price
                        
                        # 记录本次交易
                        self.trade_count += 1
                        trade_record = {
                            'trade_id': self.trade_count,
                            'timestamp': spot_order.get('timestamp', 0),
                            'spot_filled': spot_actual_position,
                            'contract_filled': contract_actual_position,
                            'position_diff': position_diff,
                            'position_diff_usdt': position_diff * current_price,
                            'price': current_price,
                            'spot_market_price': float(gateio_ask),
                            'spot_execution_price': spot_actual_price,
                            'spot_price_diff_percent': spot_price_diff_percent,
                            'contract_market_price': float(binance_bid),
                            'contract_execution_price': contract_actual_price,
                            'contract_price_diff_percent': contract_price_diff_percent,
                            'market_depth_ratio_spot': float(gateio_ask_volume) / self.spot_amount,
                            'market_depth_ratio_contract': float(binance_bid_volume) / self.spot_amount,
                            'cumulative_diff': self.cumulative_position_diff,
                            'cumulative_diff_usdt': self.cumulative_position_diff_usdt,
                            'is_rebalance': False
                        }
                        self.trade_records.append(trade_record)
                        
                        if spot_actual_position > 0:
                            position_diff_percent = position_diff_abs / spot_actual_position * 100
                            logger.info(f"【持仓差异】数量: {position_diff_abs} {self.symbol.split('/')[0]} ({position_diff_percent:.2f}%)")
                            logger.info(f"【累计差额】{self.cumulative_position_diff:.8f} {self.symbol.split('/')[0]} ({self.cumulative_position_diff_usdt:.2f} USDT)")
                            
                            # 如果持仓差异超过2%，视为异常
                            if position_diff_percent > 2:
                                logger.error(f"本次操作的现货和合约持仓差异过大: {position_diff_abs} {self.symbol.split('/')[0]} ({position_diff_percent:.2f}%)")
                                return None, None
                        
                        # 申购余币宝
                        if not self.test_mode:
                            try:
                                gateio_subscrible_earn(self.symbol.split('/')[0], spot_actual_position)
                                logger.info(f"已将 {spot_actual_position} {self.symbol.split('/')[0]} 申购到余币宝")
                            except Exception as e:
                                logger.error(f"余币宝申购失败，但不影响主要交易流程: {str(e)}")
                        
                        return spot_order, contract_order
                    
                    # 不满足条件时，继续下一次轮询，避免打印过多日志
                    # 每30秒输出一次当前观察到的价差情况
                    now = time.time()
                    if now - last_spread_log_time >= 30:
                        logger.debug(f"当前价差: {spread_percent*100:.4f}%, 最小要求: {self.min_spread*100:.4f}%, "
                                   f"深度比例 - Gate.io: {gateio_ask_volume/self.spot_amount:.2f}x, Binance: {binance_bid_volume/self.spot_amount:.2f}x")
                        last_spread_log_time = now
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"监控订单簿时出错: {str(e)}")
                    raise
                    # 短暂等待后重试
                    # await asyncio.sleep(1)
                
                # 短暂等待后继续轮询，降低CPU占用
                await asyncio.sleep(0.3)  # 10毫秒轮询一次，比之前的50毫秒更快
                
        except asyncio.CancelledError:
            logger.info("交易监控任务被取消")
            raise
        except Exception as e:
            logger.error(f"{self.symbol}执行对冲交易时出错: {str(e)}")
            import traceback
            logger.error(f"错误详情: {traceback.format_exc()}")
            raise


    async def check_balances(self):
        """
        检查Gate.io和Binance的账户余额
        
        Returns:
            tuple: (gateio_balance, binance_balance) - 返回两个交易所的USDT余额
        """
        try:
            # 并行获取两个交易所的余额
            gateio_balance, binance_balance = await asyncio.gather(
                self.gateio.fetch_balance(),
                self.binance.fetch_balance({'type': 'future'})  # 指定获取合约账户余额
            )
            
            gateio_usdt = gateio_balance.get('USDT', {}).get('free', 0)
            binance_usdt = binance_balance.get('USDT', {}).get('free', 0)
            
            logger.info(f"账户余额 - Gate.io: {gateio_usdt} USDT, Binance: {binance_usdt} USDT")
            return gateio_usdt, binance_usdt
            
        except Exception as e:
            logger.error(f"检查余额时出错: {str(e)}")
            raise

    async def check_market_depth_for_amount(self, amount, side="spot"):
        """
        检查市场深度是否足够支持给定数量的交易
        
        Args:
            amount (float): 要交易的数量
            side (str): 交易方向，"spot"表示买入现货，"contract"表示开空合约
            
        Returns:
            tuple: (是否深度足够, 订单簿数据)
        """
        try:
            # 获取最新的订单簿数据
            if side == "spot":
                orderbook = await self.gateio.fetch_order_book(self.symbol)
                available_volume = float(orderbook['asks'][0][1])  # 现货卖一量
                price = float(orderbook['asks'][0][0])  # 现货卖一价
            else:  # contract side
                orderbook = await self.binance.fetch_order_book(self.contract_symbol)
                available_volume = float(orderbook['bids'][0][1])  # 合约买一量
                price = float(orderbook['bids'][0][0])  # 合约买一价
            
            # 检查深度是否满足要求
            is_depth_sufficient = available_volume >= amount * self.depth_multiplier
            
            if not is_depth_sufficient:
                logger.warning(f"市场深度不足 - {'Gate.io卖一' if side=='spot' else 'Binance买一'}: {available_volume}, "
                             f"交易量: {amount}, 所需深度: {amount * self.depth_multiplier}")
            
            return is_depth_sufficient, orderbook, price, available_volume
            
        except Exception as e:
            logger.error(f"检查市场深度时出错: {str(e)}")
            return False, None, 0, 0

def parse_arguments():
    """
    解析命令行参数
    
    Returns:
        argparse.Namespace: 解析后的命令行参数对象
    """
    parser = argparse.ArgumentParser(description='Gate.io现货与Binance合约对冲交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, required=True, help='购买的现货数量')
    parser.add_argument('-p', '--min-spread', type=float, default=-0.0001, help='最小价差要求，默认0.001 (0.1%%)')
    parser.add_argument('-l', '--leverage', type=int, default=None, help='合约杠杆倍数，如果不指定则使用该交易对支持的最大杠杆倍数')
    parser.add_argument('-c', '--count', type=int, default=1, help='交易重复执行次数，默认为1')
    parser.add_argument('-m', '--depth-multiplier', type=int, default=5, help='市场深度要求的乘数，默认为交易量的5倍')
    parser.add_argument('--test-earn', action='store_true', help='测试余币宝申购功能')
    parser.add_argument('-t', '--test', action='store_true', help='测试模式，只打印交易信息，不实际下单')
    parser.add_argument('-d', '--debug', action='store_true', help='启用调试日志')
    return parser.parse_args()


async def test_earn_subscription():
    """
    测试Gate.io余币宝申购功能
    """
    try:
        # 测试申购余币宝
        currency = "KAVA"
        amount = 10  # 测试申购10个KAVA
        
        result = gateio_subscrible_earn(currency, amount)
        logger.info(f"余币宝测试申购结果: {result}")
        
    except Exception as e:
        logger.error(f"余币宝测试失败: {str(e)}")


async def print_execution_summary(trader, successful_trades, total_count):
    """
    打印交易执行汇总信息
    
    Args:
        trader (HedgeTrader): 交易对象实例
        successful_trades (int): 成功执行的交易次数
        total_count (int): 计划执行的总交易次数
    """
    logger.info("=" * 50)
    logger.info(f"交易执行汇总")
    logger.info(f"总计执行: {successful_trades}/{total_count} 次交易")
    
    # 如果没有trader或者交易记录为空，直接返回
    if not trader or not trader.trade_records:
        logger.info("=" * 50)
        return
    
    try:
        base_currency = trader.symbol.split('/')[0]
        
        # 尝试获取当前价格，如果失败则使用最后一次交易的价格
        try:
            orderbook = await trader.gateio.fetch_order_book(trader.symbol)
            current_price = float(orderbook['asks'][0][0])
        except Exception:
            # 如果获取当前价格失败，使用最后一次交易的价格或默认值
            if trader.trade_records:
                current_price = trader.trade_records[-1].get('price', 0)
            else:
                current_price = 0
        
        # 显示最终累计差额
        logger.info("-" * 40)
        logger.info(f"最终累计差额:")
        logger.info(f"数量差额: {trader.cumulative_position_diff:.8f} {base_currency}")
        logger.info(f"价值差额: {trader.cumulative_position_diff_usdt:.2f} USDT")
        
        # 显示最近3次交易记录
        if len(trader.trade_records) > 0:
            logger.info("-" * 40)
            logger.info("最近交易记录:")
            for record in trader.trade_records[-min(3, len(trader.trade_records)):]:
                trade_type = "【平衡操作】" if record.get('is_rebalance', False) else "【常规交易】"
                
                if record.get('is_rebalance', False):
                    if record.get('spot_filled', 0) > 0:  # 现货平衡
                        logger.info(f"{trade_type} 交易 #{record['trade_id']}: 买入现货 {record['spot_filled']:.8f} {base_currency}, "
                                  f"价格: {record.get('execution_price', 0):.8f} vs 市场: {record.get('market_price', 0):.8f} "
                                  f"(滑点: {record.get('price_diff_percent', 0):.4f}%)")
                    else:  # 合约平衡
                        logger.info(f"{trade_type} 交易 #{record['trade_id']}: 开空合约 {record['contract_filled']:.8f} {base_currency}, "
                                  f"价格: {record.get('execution_price', 0):.8f} vs 市场: {record.get('market_price', 0):.8f} "
                                  f"(滑点: {record.get('price_diff_percent', 0):.4f}%)")
                else:  # 常规交易
                    logger.info(f"{trade_type} 交易 #{record['trade_id']}: 现货 {record['spot_filled']:.8f} @ {record.get('spot_execution_price', 0):.8f} "
                              f"vs 合约 {record['contract_filled']:.8f} @ {record.get('contract_execution_price', 0):.8f}, "
                              f"数量差额: {record['position_diff']:.8f} {base_currency} ({record['position_diff_usdt']:.2f} USDT)")
                    
                    # 如果有价格分析数据，则显示
                    if 'spot_price_diff_percent' in record and 'contract_price_diff_percent' in record:
                        logger.info(f"     价格分析 - 现货滑点: {record['spot_price_diff_percent']:.4f}%, "
                                  f"合约滑点: {record['contract_price_diff_percent']:.4f}%")
        
        # 显示是否需要平衡
        if abs(trader.cumulative_position_diff_usdt) >= 6:
            balance_side = "买入现货" if trader.cumulative_position_diff > 0 else "开空合约"
            logger.info("-" * 40)
            logger.info(f"建议: 需要{balance_side}来平衡仓位")
            logger.info(f"平衡数量: {abs(trader.cumulative_position_diff):.8f} {base_currency}")
            logger.info(f"预计成本: {abs(trader.cumulative_position_diff_usdt):.2f} USDT")
        
        # 显示价格执行统计
        regular_trades = [r for r in trader.trade_records if not r.get('is_rebalance', False)]
        if regular_trades:
            logger.info("-" * 40)
            logger.info("价格执行统计:")
            
            # 计算平均滑点
            spot_slippage = [r.get('spot_price_diff_percent', 0) for r in regular_trades if 'spot_price_diff_percent' in r]
            contract_slippage = [r.get('contract_price_diff_percent', 0) for r in regular_trades if 'contract_price_diff_percent' in r]
            
            if spot_slippage:
                avg_spot_slippage = sum(spot_slippage) / len(spot_slippage)
                max_spot_slippage = max(spot_slippage)
                min_spot_slippage = min(spot_slippage)
                logger.info(f"现货平均滑点: {avg_spot_slippage:.4f}% (最小: {min_spot_slippage:.4f}%, 最大: {max_spot_slippage:.4f}%)")
            
            if contract_slippage:
                avg_contract_slippage = sum(contract_slippage) / len(contract_slippage)
                max_contract_slippage = max(contract_slippage)
                min_contract_slippage = min(contract_slippage)
                logger.info(f"合约平均滑点: {avg_contract_slippage:.4f}% (最小: {min_contract_slippage:.4f}%, 最大: {max_contract_slippage:.4f}%)")
            
            if spot_slippage and contract_slippage:
                total_avg_slippage = (sum(spot_slippage) + sum(contract_slippage)) / (len(spot_slippage) + len(contract_slippage))
                logger.info(f"总体平均滑点: {total_avg_slippage:.4f}%")
                
                # 计算平均每笔交易的滑点成本
                total_volume = sum(r['spot_filled'] for r in regular_trades)
                total_cost = sum(((r.get('spot_execution_price', 0) - r.get('spot_market_price', 0)) + 
                                  (r.get('contract_market_price', 0) - r.get('contract_execution_price', 0))) * 
                                 r['spot_filled'] for r in regular_trades if 'spot_market_price' in r and 'contract_market_price' in r)
                
                if total_volume > 0:
                    avg_cost_per_unit = total_cost / total_volume
                    logger.info(f"平均每单位滑点成本: {avg_cost_per_unit:.8f} USDT/{base_currency}")
                    logger.info(f"总滑点成本: {total_cost:.8f} USDT")
        
        # 分析市场深度与滑点的关系
        if regular_trades:
            logger.info("-" * 40)
            logger.info("市场深度分析:")
            
            # 收集深度比例数据
            depth_ratios_spot = [r.get('market_depth_ratio_spot', 0) for r in regular_trades if 'market_depth_ratio_spot' in r]
            depth_ratios_contract = [r.get('market_depth_ratio_contract', 0) for r in regular_trades if 'market_depth_ratio_contract' in r]
            
            if depth_ratios_spot:
                avg_depth_ratio_spot = sum(depth_ratios_spot) / len(depth_ratios_spot)
                min_depth_ratio_spot = min(depth_ratios_spot)
                max_depth_ratio_spot = max(depth_ratios_spot)
                logger.info(f"现货市场深度比例: 平均 {avg_depth_ratio_spot:.2f}x (最小: {min_depth_ratio_spot:.2f}x, 最大: {max_depth_ratio_spot:.2f}x)")
            
            if depth_ratios_contract:
                avg_depth_ratio_contract = sum(depth_ratios_contract) / len(depth_ratios_contract)
                min_depth_ratio_contract = min(depth_ratios_contract)
                max_depth_ratio_contract = max(depth_ratios_contract)
                logger.info(f"合约市场深度比例: 平均 {avg_depth_ratio_contract:.2f}x (最小: {min_depth_ratio_contract:.2f}x, 最大: {max_depth_ratio_contract:.2f}x)")
            
            # 尝试分析深度比例与滑点的相关性
            if spot_slippage and depth_ratios_spot:
                # 简单计算高深度和低深度时的平均滑点
                high_depth_indices = [i for i, ratio in enumerate(depth_ratios_spot) if ratio >= trader.depth_multiplier]
                low_depth_indices = [i for i, ratio in enumerate(depth_ratios_spot) if ratio < trader.depth_multiplier]
                
                if high_depth_indices:
                    high_depth_slippage = [spot_slippage[i] for i in high_depth_indices]
                    avg_high_depth_slippage = sum(high_depth_slippage) / len(high_depth_slippage)
                    logger.info(f"高深度现货交易(>={trader.depth_multiplier}x)平均滑点: {avg_high_depth_slippage:.4f}%")
                
                if low_depth_indices:
                    low_depth_slippage = [spot_slippage[i] for i in low_depth_indices]
                    avg_low_depth_slippage = sum(low_depth_slippage) / len(low_depth_slippage)
                    logger.info(f"低深度现货交易(<{trader.depth_multiplier}x)平均滑点: {avg_low_depth_slippage:.4f}%")
                    
                # 建议
                logger.info("-" * 40)
                logger.info("滑点优化建议:")
                if high_depth_indices and low_depth_indices:
                    slippage_diff = avg_low_depth_slippage - avg_high_depth_slippage
                    if slippage_diff > 0:
                        logger.info(f"高深度交易的滑点比低深度交易低 {slippage_diff:.4f}%，当前深度倍数({trader.depth_multiplier}x)设置有效")
                        if avg_high_depth_slippage > 0.05:  # 如果高深度交易的滑点仍然较高
                            logger.info(f"建议增加深度倍数至 {trader.depth_multiplier * 1.5:.0f}x 以进一步降低滑点")
                    else:
                        logger.info(f"当前深度倍数({trader.depth_multiplier}x)设置可能不足以有效降低滑点")
                        logger.info(f"建议增加深度倍数至 {trader.depth_multiplier * 2:.0f}x 或调整交易大小")
                elif high_depth_indices:
                    if avg_high_depth_slippage > 0.05:  # 如果高深度交易的滑点较高
                        logger.info(f"即使在高深度条件下滑点仍达到 {avg_high_depth_slippage:.4f}%")
                        logger.info(f"建议增加深度倍数至 {trader.depth_multiplier * 1.5:.0f}x 或减小交易规模")
                    else:
                        logger.info(f"当前深度倍数({trader.depth_multiplier}x)设置有效，平均滑点为 {avg_high_depth_slippage:.4f}%")
                elif low_depth_indices:
                    logger.info(f"所有交易都在低深度(<{trader.depth_multiplier}x)条件下执行，平均滑点为 {avg_low_depth_slippage:.4f}%")
                    logger.info(f"建议增加深度倍数至 {trader.depth_multiplier * 2:.0f}x 或减小交易规模以降低滑点")
    
    except Exception as e:
        logger.error(f"打印交易执行汇总时出错: {str(e)}")
    
    logger.info("=" * 50)


async def main():
    """
    异步主函数，程序入口
    
    Returns:
        int: 程序退出码，0表示成功，非0表示失败
    """
    args = parse_arguments()
    
    # 设置日志级别
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("已启用调试日志模式")
    else:
        logger.setLevel(logging.INFO)

    # 如果是测试模式，只测试余币宝功能
    if args.test_earn:
        await test_earn_subscription()
        return 0
    
    # 记录成功完成的交易次数
    successful_trades = 0
    trader = None
    
    try:
        logger.info(f"计划执行 {args.count} 次交易操作")
        
        # 创建并初始化交易器
        trader = HedgeTrader(
            symbol=args.symbol,
            spot_amount=args.amount,
            min_spread=args.min_spread,
            leverage=args.leverage,
            test_mode=args.test,  # 添加测试模式参数
            depth_multiplier=args.depth_multiplier  # 添加depth_multiplier参数
        )
        await trader.initialize()
        
        # 根据count参数执行多次交易
        for i in range(args.count):
            if i > 0:
                logger.info(f"准备执行第 {i+1}/{args.count} 次交易")
                # 重新检查账户余额
                gateio_usdt, binance_usdt = await trader.check_balances()
                
                # 检查余额是否满足交易要求
                if trader.spot_amount is not None:
                    orderbook = await trader.gateio.fetch_order_book(trader.symbol)
                    current_price = float(orderbook['asks'][0][0])
                    
                    required_usdt = float(trader.spot_amount) * current_price * 1.02
                    required_margin = float(trader.spot_amount) * current_price / trader.leverage * 1.05

                    if required_usdt > gateio_usdt or gateio_usdt <= 50:
                        logger.warning(f"Gate.io USDT余额不足，需要约 {required_usdt:.2f} USDT，当前余额 {gateio_usdt:.2f} USDT")
                        try:
                            redeem_earn('USDT', max(required_usdt * 1.01, 50))
                            # 重新获取并保存账户余额
                            gateio_usdt, binance_usdt = await trader.check_balances()
                            if required_usdt > gateio_usdt:
                                logger.error(f"Gate.io USDT余额仍然不足，需要约 {required_usdt:.2f} USDT，当前余额 {gateio_usdt:.2f} USDT")
                                logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因余额不足退出程序")
                                return 0 if successful_trades > 0 else 1
                        except Exception as e:
                            logger.error(f"赎回USDT失败: {str(e)}")
                            logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因赎回失败退出程序")
                            return 0 if successful_trades > 0 else 1
                            
                    if required_margin > binance_usdt:
                        logger.error(f"Binance USDT保证金不足，需要约 {required_margin:.2f} USDT，当前余额 {binance_usdt:.2f} USDT")
                        logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因保证金不足退出程序")
                        return 0 if successful_trades > 0 else 1

                    logger.info(f"账户余额检查通过 - 预估所需Gate.io: {required_usdt:.2f} USDT, Binance: {required_margin:.2f} USDT")
            
            try:
                # 执行对冲交易
                spot_order, contract_order = await trader.execute_hedge_trade()
                
                if args.test:
                    logger.info(f"测试模式完成第 {i+1}/{args.count} 次交易")
                    successful_trades += 1
                elif spot_order and contract_order:
                    logger.info(f"成功完成第 {i+1}/{args.count} 次对冲交易!")
                    successful_trades += 1
                else:
                    logger.error(f"第 {i+1}/{args.count} 次对冲交易未能完成")
                    logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因交易未完成退出程序")
                    
                    # 打印交易执行汇总
                    await print_execution_summary(trader, successful_trades, args.count)
                    return 0 if successful_trades > 0 else 1
            except Exception as e:
                logger.error(f"第 {i+1}/{args.count} 次交易执行失败: {str(e)}")
                # 记录详细的错误堆栈
                import traceback
                logger.debug(f"交易执行错误的堆栈:\n{traceback.format_exc()}")
                logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因交易执行失败退出程序")
                
                # 打印交易执行汇总
                await print_execution_summary(trader, successful_trades, args.count)
                return 0 if successful_trades > 0 else 1
        
        # 报告总体执行情况
        await print_execution_summary(trader, successful_trades, args.count)
            
        if successful_trades > 0:
            return 0
        else:
            logger.error("所有交易均未完成")
            return 1
            
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        # 记录详细的错误堆栈
        import traceback
        logger.debug(f"主程序错误的堆栈:\n{traceback.format_exc()}")
        logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因发生错误退出程序")
        return 1
    finally:
        # 确保关闭交易所连接
        if trader:
            try:
                await asyncio.gather(
                    trader.gateio.close(),
                    trader.binance.close()
                )
                logger.debug("已关闭所有交易所连接")
            except Exception as e:
                logger.error(f"关闭交易所连接时出错: {str(e)}")


if __name__ == "__main__":
    # 设置并启动事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 