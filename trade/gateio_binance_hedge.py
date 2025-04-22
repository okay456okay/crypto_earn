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

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import binance_api_key, binance_api_secret, gateio_api_secret, gateio_api_key, proxies
from trade.gateio_api import subscrible_earn as gateio_subscrible_earn, redeem_earn


class HedgeTrader:
    def __init__(self, symbol, spot_amount=None, min_spread=0.001, leverage=None, test_mode=False):
        """
        初始化基本属性
        
        Args:
            symbol (str): 交易对符号，例如 'ETH/USDT'
            spot_amount (float, optional): 现货交易数量
            min_spread (float, optional): 最小价差要求，默认0.001 (0.1%)
            leverage (int, optional): 合约杠杆倍数，如果不指定则使用该交易对支持的最大杠杆倍数
            test_mode (bool, optional): 是否为测试模式，默认False
        """
        self.symbol = symbol
        self.spot_amount = spot_amount
        self.min_spread = min_spread
        self.leverage = leverage  # 初始化为None，将在initialize中设置
        self.test_mode = test_mode
        
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
        
        # 用于存储最新订单簿数据
        self.orderbooks = {
            'gateio': None,
            'binance': None
        }
        
        # 用于控制WebSocket订阅
        self.ws_running = False
        self.price_updates = asyncio.Queue()
        
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
                       f"最小价差={self.min_spread*100}%, 杠杆={self.leverage}倍")
            
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

    async def subscribe_orderbooks(self):
        """订阅交易对的订单簿数据"""
        try:
            self.ws_running = True
            while self.ws_running:
                try:
                    # 创建两个任务来订阅订单簿
                    tasks = [
                        asyncio.create_task(self.gateio.watch_order_book(self.symbol)),
                        asyncio.create_task(self.binance.watch_order_book(self.contract_symbol))
                    ]
                    
                    # 等待任意一个订单簿更新
                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # 处理完成的任务
                    for task in done:
                        try:
                            ob = task.result()
                            if task == tasks[0]:  # gateio task
                                self.orderbooks['gateio'] = ob
                                logger.debug(f"收到Gate.io订单簿更新")
                            else:  # binance task
                                self.orderbooks['binance'] = ob
                                logger.debug(f"收到Binance订单簿更新")
                            
                            # 如果两个订单簿都有数据，检查价差
                            if self.orderbooks['gateio'] and self.orderbooks['binance']:
                                await self.check_spread_from_orderbooks()
                            
                        except Exception as e:
                            logger.error(f"处理订单簿数据时出错: {str(e)}")
                    
                    # 取消未完成的任务
                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        
                except Exception as e:
                    logger.error(f"订阅订单簿时出错: {str(e)}")
                    await asyncio.sleep(1)  # 出错后等待一秒再重试
                    
        except Exception as e:
            logger.error(f"订单簿订阅循环出错: {str(e)}")
        finally:
            self.ws_running = False
            # 确保所有WebSocket连接都被关闭
            try:
                await asyncio.gather(
                    self.gateio.close(),
                    self.binance.close()
                )
            except Exception as e:
                logger.error(f"关闭WebSocket连接时出错: {str(e)}")

    async def check_spread_from_orderbooks(self):
        """从已缓存的订单簿数据中检查价差"""
        try:
            gateio_ob = self.orderbooks['gateio']
            binance_ob = self.orderbooks['binance']
            
            if not gateio_ob or not binance_ob:
                return
            
            gateio_ask = Decimal(str(gateio_ob['asks'][0][0]))
            gateio_ask_volume = Decimal(str(gateio_ob['asks'][0][1]))
            gateio_bid = Decimal(str(gateio_ob['bids'][0][0]))
            gateio_bid_volume = Decimal(str(gateio_ob['bids'][0][1]))
            
            binance_bid = Decimal(str(binance_ob['bids'][0][0]))
            binance_bid_volume = Decimal(str(binance_ob['bids'][0][1]))
            binance_ask = Decimal(str(binance_ob['asks'][0][0]))
            binance_ask_volume = Decimal(str(binance_ob['asks'][0][1]))
            
            spread = binance_bid - gateio_ask
            spread_percent = spread / gateio_ask
            
            # 将价差数据放入队列
            spread_data = {
                'spread_percent': float(spread_percent),
                'gateio_ask': float(gateio_ask),
                'gateio_ask_volume': float(gateio_ask_volume),
                'gateio_bid': float(gateio_bid),
                'gateio_bid_volume': float(gateio_bid_volume),
                'binance_bid': float(binance_bid),
                'binance_bid_volume': float(binance_bid_volume),
                'binance_ask': float(binance_ask),
                'binance_ask_volume': float(binance_ask_volume)
            }
            await self.price_updates.put(spread_data)
            
        except Exception as e:
            logger.error(f"{self.symbol}检查订单簿价差时出错: {str(e)}")

    async def wait_for_spread(self):
        """等待价差达到要求"""
        subscription_task = None
        try:
            # 启动WebSocket订阅
            subscription_task = asyncio.create_task(self.subscribe_orderbooks())
            
            while True:
                try:
                    # 从队列中获取最新价差数据，设置超时
                    spread_data = await asyncio.wait_for(
                        self.price_updates.get(),
                        timeout=30  # 30秒超时
                    )
                    
                    spread_percent = spread_data['spread_percent']
                    
                    # 将价格检查的日志改为DEBUG级别
                    logger.debug(f"{self.symbol}价格检查 - Gate.io卖1: {spread_data['gateio_ask']} (量: {spread_data['gateio_ask_volume']}), "
                               f"Gate.io买1: {spread_data['gateio_bid']} (量: {spread_data['gateio_bid_volume']}), "
                               f"Binance买1: {spread_data['binance_bid']} (量: {spread_data['binance_bid_volume']}), "
                               f"Binance卖1: {spread_data['binance_ask']} (量: {spread_data['binance_ask_volume']}), "
                               f"价差: {spread_percent*100:.4f}%")
                    
                    if spread_percent >= self.min_spread:
                        logger.info(f"{self.symbol}价差条件满足: {spread_percent*100:.4f}% >= {self.min_spread*100:.4f}%")
                        return (spread_percent, 
                               spread_data['gateio_ask'], 
                               spread_data['binance_bid'],
                               spread_data['gateio_ask_volume'], 
                               spread_data['binance_bid_volume'],
                               spread_data['gateio_bid'],
                               spread_data['gateio_bid_volume'],
                               spread_data['binance_ask'],
                               spread_data['binance_ask_volume'])
                    
                    logger.debug(f"{self.symbol}价差条件不满足: {spread_percent*100:.4f}% < {self.min_spread*100:.4f}%")
                    
                except asyncio.TimeoutError:
                    logger.warning(f"{self.symbol}等待价差数据超时，重新订阅订单簿")
                    # 重新启动订阅
                    if subscription_task:
                        subscription_task.cancel()
                        try:
                            await subscription_task
                        except asyncio.CancelledError:
                            pass
                    subscription_task = asyncio.create_task(self.subscribe_orderbooks())
                    
        except Exception as e:
            logger.error(f"{self.symbol}等待价差时出错: {str(e)}")
            raise
        finally:
            # 确保WebSocket订阅被停止
            self.ws_running = False
            if subscription_task:
                subscription_task.cancel()
                try:
                    await subscription_task
                except asyncio.CancelledError:
                    pass

    async def execute_hedge_trade(self):
        """执行对冲交易"""
        try:
            # 1. 等待价差满足条件
            spread_data = await self.wait_for_spread()
            spread_percent, gateio_ask, binance_bid, gateio_ask_volume, binance_bid_volume, gateio_bid, gateio_bid_volume, binance_ask, binance_ask_volume = spread_data
            
            # 获取基础货币
            base_currency = self.symbol.split('/')[0]
            
            # 详细记录市场深度信息
            logger.info("=" * 50)
            logger.info("当前市场深度:")
            logger.info(f"Gate.io 买1: {gateio_bid} (量: {gateio_bid_volume}), 卖1: {gateio_ask} (量: {gateio_ask_volume})")
            logger.info(f"Binance 买1: {binance_bid} (量: {binance_bid_volume}), 卖1: {binance_ask} (量: {binance_ask_volume})")
            logger.info(f"价差: {spread_percent*100:.4f}% ({(binance_bid - gateio_ask):.8f} USDT)")
            logger.info("-" * 50)
            
            # 检查是否需要平衡现货和合约
            need_to_rebalance = False
            rebalance_message = ""
            rebalance_side = None
            rebalance_amount = 0
            
            # 如果累计差额超过6 USDT，则需要平衡
            if abs(self.cumulative_position_diff_usdt) >= 6:
                need_to_rebalance = True
                current_price = float(gateio_ask)  # 使用当前Gate.io的卖一价格
                
                if self.cumulative_position_diff > 0:  # 合约多，买入现货
                    rebalance_side = "spot"
                    rebalance_amount = abs(self.cumulative_position_diff)
                    rebalance_message = f"检测到累计差额 {self.cumulative_position_diff} {base_currency} ({self.cumulative_position_diff_usdt:.2f} USDT)，需要买入现货"
                else:  # 现货多，开更多合约空单
                    rebalance_side = "contract"
                    rebalance_amount = abs(self.cumulative_position_diff)
                    rebalance_message = f"检测到累计差额 {self.cumulative_position_diff} {base_currency} ({self.cumulative_position_diff_usdt:.2f} USDT)，需要开空合约"
                
                logger.info("=" * 50)
                logger.info(rebalance_message)
                logger.info(f"当前价格: {current_price} USDT/{base_currency}")
                logger.info(f"平衡数量: {rebalance_amount} {base_currency}")
                logger.info("=" * 50)
                
                # 根据是否为测试模式决定是否执行平衡操作
                if self.test_mode:
                    logger.info("测试模式: 不执行实际平衡交易")
                else:
                    if rebalance_side == "spot":  # 需要买入现货
                        try:
                            # 计算买入现货所需的USDT
                            cost = float(rebalance_amount) * float(gateio_ask) * 1.01  # 添加1%的缓冲
                            
                            # 执行买入操作
                            spot_balance_order = await self.gateio.create_market_buy_order(
                                symbol=self.symbol,
                                amount=cost,
                                params={'createMarketBuyOrderRequiresPrice': False, 'quoteOrderQty': True}
                            )
                            
                            # 获取实际成交数量
                            spot_filled_amount = float(spot_balance_order.get('filled', 0))
                            spot_fees = spot_balance_order.get('fees', [])
                            spot_base_fee = sum(float(fee.get('cost', 0)) for fee in spot_fees if fee.get('currency') == base_currency)
                            spot_actual_filled = spot_filled_amount - spot_base_fee
                            
                            # 计算实际成交价格
                            spot_cost = float(spot_balance_order.get('cost', 0))
                            if spot_filled_amount > 0:
                                spot_actual_price = spot_cost / spot_filled_amount
                            else:
                                spot_actual_price = 0
                                
                            # 记录实际成交价格与市场价格的差异
                            price_diff_percent = ((spot_actual_price - float(gateio_ask)) / float(gateio_ask) * 100) if float(gateio_ask) > 0 else 0
                            logger.info(f"现货平衡买入价格分析 - 市场卖1价: {gateio_ask}, 实际成交价: {spot_actual_price:.8f}, "
                                      f"差异: {price_diff_percent:.4f}%, 滑点: {(spot_actual_price - float(gateio_ask)):.8f} USDT")
                            
                            # 更新累计差额 (调整累计差额，减去新买入的现货)
                            self.cumulative_position_diff -= spot_actual_filled
                            self.cumulative_position_diff_usdt = self.cumulative_position_diff * float(gateio_ask)
                            
                            # 记录本次交易作为一条新记录
                            self.trade_count += 1
                            rebalance_record = {
                                'trade_id': f"rebalance-{self.trade_count}",
                                'timestamp': spot_balance_order.get('timestamp', 0),
                                'spot_filled': spot_actual_filled,
                                'contract_filled': 0,
                                'position_diff': -spot_actual_filled,  # 负值表示现货增加
                                'position_diff_usdt': -spot_actual_filled * current_price,
                                'price': current_price,
                                'market_price': float(gateio_ask),
                                'execution_price': spot_actual_price,
                                'price_diff_percent': price_diff_percent,
                                'cumulative_diff': self.cumulative_position_diff,
                                'cumulative_diff_usdt': self.cumulative_position_diff_usdt,
                                'is_rebalance': True
                            }
                            self.trade_records.append(rebalance_record)
                            
                            # 记录平衡操作
                            logger.info(f"成功买入 {spot_actual_filled} {base_currency} 现货用于平衡持仓")
                            
                            # 申购余币宝
                            try:
                                gateio_subscrible_earn(base_currency, spot_actual_filled)
                                logger.info(f"已将新买入的 {spot_actual_filled} {base_currency} 申购到余币宝")
                            except Exception as e:
                                logger.error(f"余币宝申购失败，但不影响平衡操作: {str(e)}")
                                
                        except Exception as e:
                            logger.error(f"执行现货平衡买入操作失败: {str(e)}")
                    
                    elif rebalance_side == "contract":  # 需要开更多合约空单
                        try:
                            # 精确化合约数量
                            contract_amount = self.binance.amount_to_precision(self.contract_symbol, rebalance_amount)
                            
                            # 执行合约空单
                            contract_balance_order = await self.binance.create_market_sell_order(
                                symbol=self.contract_symbol,
                                amount=contract_amount,
                                params={'positionSide': 'SHORT'}
                            )
                            
                            # 获取实际成交数量
                            contract_filled_amount = float(contract_balance_order.get('filled', 0))
                            contract_fees = contract_balance_order.get('fees', [])
                            contract_base_fee = sum(float(fee.get('cost', 0)) for fee in contract_fees if fee.get('currency') == base_currency)
                            contract_actual_filled = contract_filled_amount - contract_base_fee
                            
                            # 计算实际成交价格
                            contract_cost = float(contract_balance_order.get('cost', 0))
                            if contract_filled_amount > 0:
                                contract_actual_price = contract_cost / contract_filled_amount
                            else:
                                contract_actual_price = 0
                                
                            # 记录实际成交价格与市场价格的差异
                            price_diff_percent = ((float(binance_bid) - contract_actual_price) / float(binance_bid) * 100) if float(binance_bid) > 0 else 0
                            logger.info(f"合约平衡空单价格分析 - 市场买1价: {binance_bid}, 实际成交价: {contract_actual_price:.8f}, "
                                      f"差异: {price_diff_percent:.4f}%, 滑点: {(float(binance_bid) - contract_actual_price):.8f} USDT")
                            
                            # 更新累计差额 (调整累计差额，增加合约空单)
                            self.cumulative_position_diff += contract_actual_filled
                            self.cumulative_position_diff_usdt = self.cumulative_position_diff * float(gateio_ask)
                            
                            # 记录本次交易作为一条新记录
                            self.trade_count += 1
                            rebalance_record = {
                                'trade_id': f"rebalance-{self.trade_count}",
                                'timestamp': contract_balance_order.get('timestamp', 0),
                                'spot_filled': 0,
                                'contract_filled': contract_actual_filled,
                                'position_diff': contract_actual_filled,  # 正值表示合约增加
                                'position_diff_usdt': contract_actual_filled * current_price,
                                'price': current_price,
                                'market_price': float(binance_bid),
                                'execution_price': contract_actual_price,
                                'price_diff_percent': price_diff_percent,
                                'cumulative_diff': self.cumulative_position_diff,
                                'cumulative_diff_usdt': self.cumulative_position_diff_usdt,
                                'is_rebalance': True
                            }
                            self.trade_records.append(rebalance_record)
                            
                            # 记录平衡操作
                            logger.info(f"成功开空 {contract_actual_filled} {base_currency} 合约用于平衡持仓")
                            
                        except Exception as e:
                            logger.error(f"执行合约平衡空单操作失败: {str(e)}")
            
            # 2. 立即准备下单参数, 不然现货会比合约少一些
            trade_amount = self.spot_amount * 1.0019618834080717
            cost = float(trade_amount) * float(gateio_ask)
            contract_amount = self.binance.amount_to_precision(self.contract_symbol, trade_amount)
            
            # 打印交易计划
            logger.info("=" * 50)
            logger.info("交易计划:")
            logger.info(f"Gate.io 现货买入: {trade_amount} {base_currency} @ {gateio_ask} USDT")
            logger.info(f"预计成本: {cost:.2f} USDT")
            logger.info(f"Binance 合约开空: {contract_amount} {self.contract_symbol} @ {binance_bid} USDT")
            logger.info(f"当前价差: {spread_percent*100:.4f}%")
            logger.info("=" * 50)
            
            if self.test_mode:
                logger.info("测试模式: 不执行实际交易")
                return None, None
            
            # 3. 立即执行交易
            logger.debug(f"准备下单 - Gate.io现货买入量: {cost} USDT, Binance合约开空: {contract_amount} {self.contract_symbol}")
            spot_order, contract_order = await asyncio.gather(
                self.gateio.create_market_buy_order(
                    symbol=self.symbol,
                    amount=cost,
                    params={'createMarketBuyOrderRequiresPrice': False, 'quoteOrderQty': True}
                ),
                self.binance.create_market_sell_order(
                    symbol=self.contract_symbol,
                    amount=contract_amount,
                    params={'positionSide': 'SHORT'}
                )
            )
            
            # 记录原始订单响应
            logger.debug(f"Gate.io订单原始响应: {spot_order}")
            logger.debug(f"Binance订单原始响应: {contract_order}")
            
            # 4. 交易后再进行其他操作
            logger.info(f"计划交易数量: {trade_amount} {base_currency}")
            logger.info(f"在Gate.io市价买入 {trade_amount} {base_currency}, 预估成本: {cost:.2f} USDT")
            logger.info(f"在Binance市价开空单 {contract_amount} {base_currency}")
            
            # 等待2秒让订单状态更新
            await asyncio.sleep(2)
            
            # 尝试获取最新的订单状态
            try:
                spot_order_id = spot_order.get('id')
                contract_order_id = contract_order.get('id')
                
                if spot_order_id:
                    updated_spot_order = await self.gateio.fetch_order(spot_order_id, self.symbol)
                    logger.debug(f"获取到Gate.io更新后的订单状态: {updated_spot_order.get('status')}")
                    spot_order = updated_spot_order
                
                if contract_order_id:
                    # 尝试多种方法获取Binance订单状态
                    try:
                        updated_contract_order = await self.binance.fetch_order(contract_order_id, self.contract_symbol)
                        logger.debug(f"获取到Binance更新后的订单状态: {updated_contract_order.get('status')}")
                        contract_order = updated_contract_order
                    except Exception as e:
                        logger.warning(f"通过fetch_order获取Binance订单状态失败: {str(e)}")
                        try:
                            # 尝试获取最近的已完成订单
                            closed_orders = await self.binance.fetch_closed_orders(self.contract_symbol, limit=10)
                            for order in closed_orders:
                                if order.get('id') == contract_order_id:
                                    contract_order = order
                                    logger.debug(f"从已完成订单列表获取到Binance订单状态: {order.get('status')}")
                                    break
                        except Exception as e2:
                            logger.warning(f"通过fetch_closed_orders获取Binance订单状态失败: {str(e2)}")
            except Exception as e:
                logger.warning(f"获取更新后的订单状态时出错: {str(e)}")
            
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
            spot_filled_amount = float(spot_order.get('filled', 0))
            if spot_filled_amount <= 0:
                logger.error(f"Gate.io订单成交量为0，交易可能未成功")
                return None, None
            
            spot_fees = spot_order.get('fees', [])
            spot_base_fee = sum(float(fee.get('cost', 0)) for fee in spot_fees if fee.get('currency') == base_currency)
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
            contract_base_fee = sum(float(fee.get('cost', 0)) for fee in contract_fees if fee.get('currency') == base_currency)
            contract_actual_position = contract_filled_amount - contract_base_fee
            
            # 计算合约实际成交价格
            contract_cost = float(contract_order.get('cost', 0))
            contract_actual_price = contract_cost / contract_filled_amount if contract_filled_amount > 0 else 0
            contract_price_diff = float(binance_bid) - contract_actual_price
            contract_price_diff_percent = (contract_price_diff / float(binance_bid) * 100) if float(binance_bid) > 0 else 0
            
            # 记录价格执行信息
            logger.info("=" * 50)
            logger.info("价格执行分析:")
            logger.info(f"现货 - 市场卖1价: {gateio_ask}, 实际成交价: {spot_actual_price:.8f}, "
                      f"差异: {spot_price_diff:.8f} USDT ({spot_price_diff_percent:.4f}%)")
            logger.info(f"合约 - 市场买1价: {binance_bid}, 实际成交价: {contract_actual_price:.8f}, "
                      f"差异: {contract_price_diff:.8f} USDT ({contract_price_diff_percent:.4f}%)")
            logger.info(f"总滑点成本: {(spot_price_diff + contract_price_diff) * float(spot_actual_position):.8f} USDT")
            logger.info("=" * 50)
            
            # 检查本次操作的现货和合约持仓差异
            position_diff = contract_actual_position - spot_actual_position  # 正值表示合约多，负值表示现货多
            position_diff_abs = abs(position_diff)
            current_price = float(gateio_ask)
            position_diff_usdt = position_diff_abs * current_price
            
            # 更新累计差额 (正值表示合约多于现货，负值表示现货多于合约)
            self.cumulative_position_diff += position_diff
            # 更新以USDT计价的累计差额
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
                'cumulative_diff': self.cumulative_position_diff,
                'cumulative_diff_usdt': self.cumulative_position_diff_usdt,
                'is_rebalance': False
            }
            self.trade_records.append(trade_record)
            
            if spot_actual_position > 0:
                position_diff_percent = position_diff_abs / spot_actual_position * 100
                
                # 记录本次操作的持仓情况与累计差额
                logger.info(f"本次操作 - 现货成交: {spot_actual_position} {base_currency}, "
                           f"合约成交: {contract_actual_position} {base_currency}, "
                           f"差异: {position_diff_abs} {base_currency} ({position_diff_percent:.2f}%)")
                logger.info(f"累计差额 - 数量: {self.cumulative_position_diff:.8f} {base_currency}, "
                           f"价值: {self.cumulative_position_diff_usdt:.2f} USDT")
                
                # 如果持仓差异超过2%，视为异常
                if position_diff_percent > 2:
                    logger.error(f"本次操作的现货和合约持仓差异过大: {position_diff_abs} {base_currency} ({position_diff_percent:.2f}%)")
                    return None, None
            
            # 检查持仓情况
            await self.check_positions()

            # 申购余币宝
            if not self.test_mode:
                try:
                    gateio_subscrible_earn(base_currency, spot_actual_position)
                    logger.info(f"已将 {spot_actual_position} {base_currency} 申购到余币宝")
                except Exception as e:
                    logger.error(f"余币宝申购失败，但不影响主要交易流程: {str(e)}")

            return spot_order, contract_order
            
        except Exception as e:
            logger.error(f"执行对冲交易时出错: {str(e)}")
            # 记录详细的错误信息
            import traceback
            logger.debug(f"执行对冲交易的错误堆栈:\n{traceback.format_exc()}")
            return None, None

    async def check_positions(self):
        """异步检查交易后的持仓情况"""
        try:
            await asyncio.sleep(1)  # 等待订单状态更新
            
            # 并行获取两个交易所的持仓信息
            gateio_balance_task = self.gateio.fetch_balance()
            positions_task = self.binance.fetch_positions([self.contract_symbol])
            
            gateio_balance, positions = await asyncio.gather(
                gateio_balance_task,
                positions_task
            )
            
            # 获取现货持仓信息
            base_currency = self.symbol.split('/')[0]
            gateio_position = gateio_balance.get(base_currency, {}).get('total', 0)
            
            # 检查Binance合约持仓
            contract_position = 0
            
            if positions:
                for position in positions:
                    if position['symbol'] == self.contract_symbol:
                        contract_position = abs(float(position.get('contracts', 0)))
                        position_side = position.get('side', 'unknown')
                        position_leverage = position.get('leverage', self.leverage)
                        position_notional = position.get('notional', 0)
                        
                        logger.info(f"Binance合约持仓: {position_side} {contract_position} 合约, "
                                  f"杠杆: {position_leverage}倍, 名义价值: {position_notional}")
            else:
                logger.warning("未获取到Binance合约持仓信息")
            
            logger.info(f"持仓检查 - Gate.io现货: {gateio_position} {base_currency}, "
                       f"Binance合约: {contract_position} {base_currency}")
            
            # 检查是否平衡（允许0.5%的误差）
            position_diff = abs(float(gateio_position) - float(contract_position))
            position_diff_percent = position_diff / float(gateio_position) * 100
            
            if position_diff_percent > 0.5:  # 允许0.5%的误差
                logger.warning(f"现货和合约持仓不平衡! 差异: {position_diff} {base_currency} ({position_diff_percent:.2f}%)")
            else:
                logger.info(f"现货和合约持仓基本平衡，差异在允许范围内: {position_diff} {base_currency} ({position_diff_percent:.2f}%)")
                
        except Exception as e:
            logger.error(f"检查持仓信息失败: {str(e)}") 

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
    
    try:
        logger.info(f"计划执行 {args.count} 次交易操作")
        
        # 创建并初始化交易器
        trader = HedgeTrader(
            symbol=args.symbol,
            spot_amount=args.amount,
            min_spread=args.min_spread,
            leverage=args.leverage,
            test_mode=args.test  # 添加测试模式参数
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
                    return 0 if successful_trades > 0 else 1
            except Exception as e:
                logger.error(f"第 {i+1}/{args.count} 次交易执行失败: {str(e)}")
                # 记录详细的错误堆栈
                import traceback
                logger.debug(f"交易执行错误的堆栈:\n{traceback.format_exc()}")
                logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因交易执行失败退出程序")
                return 0 if successful_trades > 0 else 1
        
        # 报告总体执行情况
        logger.info("=" * 50)
        logger.info(f"交易执行汇总")
        logger.info(f"总计执行: {successful_trades}/{args.count} 次交易")
        
        # 如果有交易记录，显示累计差额
        if trader.trade_records:
            base_currency = trader.symbol.split('/')[0]
            orderbook = await trader.gateio.fetch_order_book(trader.symbol)
            current_price = float(orderbook['asks'][0][0])
            
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
            
        logger.info("=" * 50)
            
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
        if 'trader' in locals():
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