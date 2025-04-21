#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io现货买入与Bitget合约空单对冲的套利脚本

此脚本实现以下功能：
1. 从Gate.io市价买入指定token的现货
2. 从Bitget开对应的合约空单进行对冲
3. 确保现货和合约仓位保持一致
4. 检查价差是否满足最小套利条件
5. 监控和记录交易执行情况
"""

import sys
import os
import argparse
from decimal import Decimal
import asyncio
import ccxt.pro as ccxtpro  # 使用 ccxt pro 版本
import logging
import time

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import bitget_api_key, bitget_api_secret, bitget_api_passphrase, gateio_api_secret, gateio_api_key, proxies
from trade.gateio_api import subscrible_earn as gateio_subscrible_earn
from trade.gateio_api import redeem_earn


class HedgeTrader:
    """
    现货-合约对冲交易类，实现Gate.io现货买入与Bitget合约空单对冲
    """

    def __init__(self, symbol, spot_amount=None, min_spread=0.001, leverage=20):
        """
        初始化基本属性
        """
        self.symbol = symbol
        self.spot_amount = spot_amount
        self.min_spread = min_spread
        self.leverage = leverage

        # 设置合约交易对
        base, quote = symbol.split('/')
        self.contract_symbol = f"{base}/{quote}:{quote}"  # 例如: ETH/USDT:USDT
        self.base_currency = base  # 存储基础货币，方便后续使用

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

        self.gateio_usdt = 0
        self.bitget_usdt = None

        # 用于存储最新订单簿数据
        self.orderbooks = {
            'gateio': None,
            'bitget': None
        }

        # 用于控制WebSocket订阅
        self.ws_running = False
        self.price_updates = asyncio.Queue()
        
        # 用于跟踪操作造成的累计差额
        self.cumulative_position_diff = 0  # 正值表示合约多于现货，负值表示现货多于合约
        self.cumulative_position_diff_usdt = 0  # 以USDT计价的累计差额
        self.trade_records = []  # 记录每次交易情况
        self.trade_count = 0  # 交易计数器
        self.rebalance_count = 0  # 平衡操作计数器

    async def initialize(self):
        """
        异步初始化方法，执行需要网络请求的初始化操作
        """
        try:
            logger.debug(f"开始初始化交易环境，参数: symbol={self.symbol}, contract_symbol={self.contract_symbol}, "
                       f"spot_amount={self.spot_amount}, min_spread={self.min_spread*100:.4f}%, leverage={self.leverage}")
            
            # 获取市场信息以确定最大杠杆倍数
            markets = await self.bitget.fetch_markets()
            
            # 查找对应的合约市场
            contract_market = next((m for m in markets if m['symbol'] == self.contract_symbol), None)
            
            if not contract_market:
                logger.error(f"未找到合约 {self.contract_symbol} 的市场信息")
                # 列出部分可用合约作为参考
                contract_list = [m.get('symbol') for m in markets if m.get('symbol', '').endswith(':USDT')][:10]
                logger.debug(f"可用合约示例: {contract_list}")
                raise Exception(f"未找到合约 {self.contract_symbol} 的市场信息")
            
            # 如果命令行参数没有指定杠杆倍数，则使用最大杠杆倍数
            if self.leverage is None:
                max_leverage = contract_market.get('limits', {}).get('leverage', {}).get('max', 20)
                self.leverage = max_leverage
                logger.info(f"使用合约最大杠杆倍数: {self.leverage}倍")
            else:
                # 检查指定的杠杆倍数是否超过最大限制
                max_leverage = contract_market.get('limits', {}).get('leverage', {}).get('max', 20)
                if self.leverage > max_leverage:
                    logger.warning(f"指定的杠杆倍数 {self.leverage} 超过最大限制 {max_leverage}，将使用最大杠杆倍数")
                    self.leverage = max_leverage

            # 设置Bitget合约参数
            await self.bitget.set_leverage(self.leverage, self.contract_symbol)
            logger.info(f"设置Bitget合约杠杆倍数为: {self.leverage}倍")

            # 获取并保存账户余额
            self.gateio_usdt, self.bitget_usdt = await self.check_balances()
            logger.debug(f"账户余额: Gate.io USDT={self.gateio_usdt}, Bitget USDT={self.bitget_usdt}")

            # 检查余额是否满足交易要求
            if self.spot_amount is not None:
                # 获取当前价格
                orderbook = await self.gateio.fetch_order_book(self.symbol)
                
                if len(orderbook['asks']) == 0:
                    raise Exception(f"无法从订单簿获取价格信息: {orderbook}")
                
                current_price = float(orderbook['asks'][0][0])
                
                # 计算需要的USDT余额
                required_usdt = float(self.spot_amount) * current_price * 1.02
                required_margin = float(self.spot_amount) * current_price / self.leverage * 1.05
                
                logger.debug(f"交易需求: Gate.io约需 {required_usdt:.2f} USDT (现有 {self.gateio_usdt:.2f} USDT), "
                           f"Bitget约需 {required_margin:.2f} USDT保证金 (现有 {self.bitget_usdt:.2f} USDT)")

                # Gate.io余额检查
                if required_usdt > self.gateio_usdt or self.gateio_usdt < 50:
                    logger.warning(f"Gate.io USDT余额不足，需要约 {required_usdt:.2f} USDT，当前余额 {self.gateio_usdt:.2f} USDT")
                    
                    # 尝试从余币宝赎回
                    redeem_amount = max(required_usdt * 1.01, 50)
                    redeem_result = redeem_earn('USDT', redeem_amount)
                    logger.debug(f"从余币宝赎回 {redeem_amount:.2f} USDT，结果: {redeem_result}")
                    
                    # 重新获取并保存账户余额
                    self.gateio_usdt, self.bitget_usdt = await self.check_balances()
                    
                    if required_usdt > self.gateio_usdt:
                        raise Exception(f"赎回后Gate.io USDT余额仍不足，需要约 {required_usdt:.2f} USDT，当前余额 {self.gateio_usdt:.2f} USDT")

                # Bitget余额检查
                if required_margin > self.bitget_usdt:
                    raise Exception(f"Bitget USDT保证金不足，需要约 {required_margin:.2f} USDT，当前余额 {self.bitget_usdt:.2f} USDT")

                logger.info(f"账户余额检查通过 - 预估所需Gate.io: {required_usdt:.2f} USDT, Bitget: {required_margin:.2f} USDT")

            logger.info(f"初始化完成: 交易对={self.symbol}, 合约对={self.contract_symbol}, 最小价差={self.min_spread * 100}%, 杠杆={self.leverage}倍")

        except Exception as e:
            logger.exception(f"初始化失败: {str(e)}")
            raise

    async def check_balances(self):
        """
        检查Gate.io和Bitget的账户余额
        
        Returns:
            tuple: (gateio_balance, bitget_balance) - 返回两个交易所的USDT余额
        """
        try:
            # 并行获取两个交易所的余额
            gateio_balance, bitget_balance = await asyncio.gather(
                self.gateio.fetch_balance(),
                self.bitget.fetch_balance({'type': 'swap'})
            )

            gateio_usdt = gateio_balance.get('USDT', {}).get('free', 0)
            bitget_usdt = bitget_balance.get('USDT', {}).get('free', 0)

            logger.info(f"账户余额 - Gate.io: {gateio_usdt} USDT, Bitget: {bitget_usdt} USDT")
            return gateio_usdt, bitget_usdt

        except Exception as e:
            logger.error(f"检查余额时出错: {str(e)}")
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
                        asyncio.create_task(self.bitget.watch_order_book(self.contract_symbol))
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
                            else:  # bitget task
                                self.orderbooks['bitget'] = ob
                                logger.debug(f"收到Bitget订单簿更新")

                            # 如果两个订单簿都有数据，检查价差
                            if self.orderbooks['gateio'] and self.orderbooks['bitget']:
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
                    self.bitget.close()
                )
            except Exception as e:
                logger.error(f"关闭WebSocket连接时出错: {str(e)}")

    async def check_spread_from_orderbooks(self):
        """从已缓存的订单簿数据中检查价差"""
        try:
            # 获取缓存的订单簿数据
            gateio_ob = self.orderbooks['gateio']
            bitget_ob = self.orderbooks['bitget']

            # 检查订单簿数据是否存在
            if not gateio_ob or not bitget_ob:
                return

            # 记录订单簿数据时间戳，用于检测数据新鲜度
            gateio_timestamp = gateio_ob.get('timestamp', 0)
            bitget_timestamp = bitget_ob.get('timestamp', 0)
            now = int(time.time() * 1000)  # 当前时间戳（毫秒）
            
            gateio_age = (now - gateio_timestamp) / 1000 if gateio_timestamp else -1  # 秒
            bitget_age = (now - bitget_timestamp) / 1000 if bitget_timestamp else -1  # 秒
            
            # 提取最优价格
            gateio_ask = Decimal(str(gateio_ob['asks'][0][0]))
            gateio_ask_volume = Decimal(str(gateio_ob['asks'][0][1]))
            gateio_bid = Decimal(str(gateio_ob['bids'][0][0])) if gateio_ob['bids'] else Decimal('0')
            gateio_bid_volume = Decimal(str(gateio_ob['bids'][0][1])) if gateio_ob['bids'] else Decimal('0')

            bitget_bid = Decimal(str(bitget_ob['bids'][0][0]))
            bitget_bid_volume = Decimal(str(bitget_ob['bids'][0][1]))
            bitget_ask = Decimal(str(bitget_ob['asks'][0][0])) if bitget_ob['asks'] else Decimal('0')
            bitget_ask_volume = Decimal(str(bitget_ob['asks'][0][1])) if bitget_ob['asks'] else Decimal('0')

            # 计算价差
            spread = bitget_bid - gateio_ask
            spread_percent = spread / gateio_ask
            
            # 记录完整的价格信息
            logger.debug(f"订单簿数据 - Gate.io: 买1={gateio_bid}(量:{gateio_bid_volume}), 卖1={gateio_ask}(量:{gateio_ask_volume}), "
                       f"Bitget: 买1={bitget_bid}(量:{bitget_bid_volume}), 卖1={bitget_ask}(量:{bitget_ask_volume}), "
                       f"价差: {float(spread):.8f} ({float(spread_percent) * 100:.4f}%)")
            
            # 检查价差是否有效（防止异常价格导致错误决策）
            if abs(float(spread_percent)) > 0.1:  # 超过10%的价差可能是异常数据
                logger.warning(f"检测到异常价差: {float(spread_percent) * 100:.4f}%，可能是订单簿数据异常")
            
            # 将价差数据放入队列
            spread_data = {
                'spread_percent': float(spread_percent),
                'gateio_ask': float(gateio_ask),
                'bitget_bid': float(bitget_bid),
                'gateio_ask_volume': float(gateio_ask_volume),
                'bitget_bid_volume': float(bitget_bid_volume),
                'timestamp': int(time.time()),
                'gateio_data_age': gateio_age,
                'bitget_data_age': bitget_age
            }
            
            # 使用非阻塞方式入队，防止队列满时阻塞
            try:
                if self.price_updates.qsize() < 100:  # 防止队列过大
                    await asyncio.wait_for(self.price_updates.put(spread_data), timeout=0.5)
                else:
                    # 如果队列太大，清空队列并添加最新数据
                    logger.warning(f"价差数据队列过大，清空并添加最新数据")
                    while not self.price_updates.empty():
                        try:
                            self.price_updates.get_nowait()
                        except:
                            pass
                    await self.price_updates.put(spread_data)
            except asyncio.TimeoutError:
                logger.warning("价差数据入队超时")

        except Exception as e:
            logger.error(f"检查订单簿价差时出错: {str(e)}")
            # 如果出现异常，尝试记录订单簿状态以便调试
            try:
                logger.debug(f"订单簿状态 - Gate.io: {'有数据' if self.orderbooks['gateio'] else '无数据'}, "
                           f"Bitget: {'有数据' if self.orderbooks['bitget'] else '无数据'}")
            except:
                pass

    async def wait_for_spread(self):
        """等待价差达到要求"""
        subscription_task = None
        try:
            logger.debug(f"开始等待价差达到要求: {self.min_spread * 100:.4f}%")
            
            # 启动WebSocket订阅
            subscription_task = asyncio.create_task(self.subscribe_orderbooks())
            
            # 记录起始时间，用于计算总等待时间
            update_count = 0
            last_log_time = 0

            while True:
                try:
                    # 从队列中获取最新价差数据，设置超时
                    current_time = asyncio.get_event_loop().time()
                    
                    spread_data = await asyncio.wait_for(
                        self.price_updates.get(),
                        timeout=30  # 30秒超时
                    )

                    update_count += 1
                    spread_percent = spread_data['spread_percent']
                    
                    # 每10次更新或者至少间隔5秒记录一次详细日志
                    if update_count % 10 == 0 or (current_time - last_log_time) >= 5:
                        logger.debug(f"价格检查 - Gate.io卖1: {spread_data['gateio_ask']} vs Bitget买1: {spread_data['bitget_bid']}, "
                                   f"价差: {spread_percent * 100:.4f}%, 最小要求: {self.min_spread * 100:.4f}%")
                        last_log_time = current_time
                    
                    if spread_percent >= self.min_spread:
                        # 价差满足条件时，记录完整的买卖一价格和数量
                        gateio_ob = self.orderbooks['gateio']
                        bitget_ob = self.orderbooks['bitget']
                        
                        gateio_ask = float(gateio_ob['asks'][0][0])
                        gateio_ask_volume = float(gateio_ob['asks'][0][1])
                        gateio_bid = float(gateio_ob['bids'][0][0]) if gateio_ob['bids'] else 0
                        gateio_bid_volume = float(gateio_ob['bids'][0][1]) if gateio_ob['bids'] else 0
                        
                        bitget_bid = float(bitget_ob['bids'][0][0])
                        bitget_bid_volume = float(bitget_ob['bids'][0][1])
                        bitget_ask = float(bitget_ob['asks'][0][0]) if bitget_ob['asks'] else 0
                        bitget_ask_volume = float(bitget_ob['asks'][0][1]) if bitget_ob['asks'] else 0
                        
                        logger.info(f"【价差条件满足】价差: {spread_percent * 100:.4f}% >= {self.min_spread * 100:.4f}%")
                        logger.info(f"【市场行情】Gate.io - 买1: {gateio_bid:.8f}(量:{gateio_bid_volume:.8f}), 卖1: {gateio_ask:.8f}(量:{gateio_ask_volume:.8f})")
                        logger.info(f"【市场行情】Bitget - 买1: {bitget_bid:.8f}(量:{bitget_bid_volume:.8f}), 卖1: {bitget_ask:.8f}(量:{bitget_ask_volume:.8f})")
                        
                        return (spread_percent, spread_data['gateio_ask'], spread_data['bitget_bid'],
                                spread_data['gateio_ask_volume'], spread_data['bitget_bid_volume'])

                except asyncio.TimeoutError:
                    logger.warning(f"等待价差数据超时(30秒)，重新订阅订单簿")
                    # 重新启动订阅
                    if subscription_task:
                        subscription_task.cancel()
                        try:
                            await subscription_task
                        except asyncio.CancelledError:
                            pass
                    
                    subscription_task = asyncio.create_task(self.subscribe_orderbooks())

        except Exception as e:
            logger.error(f"等待价差时出错: {str(e)}")
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
            logger.debug("开始执行对冲交易流程")
            
            # 1. 等待价差满足条件
            spread_data = await self.wait_for_spread()
            spread_percent, gateio_ask, bitget_bid, gateio_ask_volume, bitget_bid_volume = spread_data
            logger.debug(f"获取到的价差数据 - 价差: {spread_percent*100:.4f}%, Gate.io卖1: {gateio_ask}, Bitget买1: {bitget_bid}")
            
            # 2. 检查是否需要执行平衡操作（当累计差额超过6 USDT时）
            current_price = float(gateio_ask)  # 使用当前Gate.io的卖一价格
            self.cumulative_position_diff_usdt = abs(self.cumulative_position_diff * current_price)
            
            # 如果累计差额超过6 USDT，执行平衡操作
            if self.cumulative_position_diff_usdt >= 6:
                if self.cumulative_position_diff > 0:  # 合约多于现货，需要买入现货
                    rebalance_amount = abs(self.cumulative_position_diff)
                    logger.info("=" * 50)
                    logger.info(f"【平衡操作】检测到累计差额: {self.cumulative_position_diff:.8f} {self.base_currency} "
                               f"({self.cumulative_position_diff_usdt:.2f} USDT)")
                    logger.info(f"【平衡操作】将买入 {rebalance_amount:.8f} {self.base_currency} 现货")
                    
                    try:
                        # 计算买入现货所需的USDT (加1%作为滑点和手续费的缓冲)
                        cost = float(rebalance_amount) * current_price * 1.01
                        
                        # 执行市价买入
                        rebalance_order = await self.gateio.create_market_buy_order(
                            symbol=self.symbol,
                            amount=cost,
                            params={'createMarketBuyOrderRequiresPrice': False, 'quoteOrderQty': True}
                        )
                        
                        # 获取实际成交量
                        filled_amount = float(rebalance_order.get('filled', 0))
                        fees = rebalance_order.get('fees', [])
                        base_fee = sum(float(fee.get('cost', 0)) for fee in fees if fee.get('currency') == self.base_currency)
                        actual_filled = filled_amount - base_fee
                        
                        # 更新累计差额
                        self.cumulative_position_diff -= actual_filled
                        self.cumulative_position_diff_usdt = abs(self.cumulative_position_diff * current_price)
                        
                        # 记录平衡操作
                        self.rebalance_count += 1
                        
                        # 申购余币宝
                        try:
                            if actual_filled > 0:
                                earn_result = gateio_subscrible_earn(self.base_currency, actual_filled)
                                logger.info(f"【平衡操作】已将 {actual_filled} {self.base_currency} 申购到余币宝")
                        except Exception as e:
                            logger.error(f"【平衡操作】余币宝申购失败: {str(e)}")
                        
                        # 记录平衡操作结果
                        logger.info(f"【平衡操作】成功买入 {actual_filled} {self.base_currency} 现货，累计差额更新为 "
                                  f"{self.cumulative_position_diff:.8f} {self.base_currency} ({self.cumulative_position_diff_usdt:.2f} USDT)")
                        logger.info("=" * 50)
                        
                    except Exception as e:
                        logger.error(f"【平衡操作】买入现货失败: {str(e)}")
                
                elif self.cumulative_position_diff < 0:  # 现货多于合约，需要增加合约空单
                    rebalance_amount = abs(self.cumulative_position_diff)
                    logger.info("=" * 50)
                    logger.info(f"【平衡操作】检测到累计差额: {self.cumulative_position_diff:.8f} {self.base_currency} "
                               f"({self.cumulative_position_diff_usdt:.2f} USDT)")
                    logger.info(f"【平衡操作】将开立 {rebalance_amount:.8f} {self.base_currency} 合约空单")
                    
                    try:
                        # 精确化合约数量
                        contract_amount = self.bitget.amount_to_precision(self.contract_symbol, rebalance_amount)
                        
                        # 执行合约空单
                        rebalance_order = await self.bitget.create_market_sell_order(
                            symbol=self.contract_symbol,
                            amount=contract_amount,
                            params={"reduceOnly": False}
                        )
                        
                        # 获取实际成交量
                        filled_amount = float(rebalance_order.get('filled', 0))
                        
                        # 如果订单信息中没有成交量，尝试获取更新
                        if filled_amount <= 0:
                            try:
                                await asyncio.sleep(0.5)
                                order_id = rebalance_order.get('id')
                                if order_id:
                                    updated_order = await self.bitget.fetch_order(order_id, self.contract_symbol)
                                    filled_amount = float(updated_order.get('filled', 0))
                            except Exception as ex:
                                logger.warning(f"【平衡操作】获取订单详情失败: {str(ex)}")
                        
                        # 如果仍然没有成交量，使用下单量作为估计
                        if filled_amount <= 0:
                            filled_amount = float(contract_amount)
                            logger.warning(f"【平衡操作】无法获取实际成交量，使用下单量 {filled_amount} 作为估计")
                        
                        # 更新累计差额
                        self.cumulative_position_diff += filled_amount
                        self.cumulative_position_diff_usdt = abs(self.cumulative_position_diff * current_price)
                        
                        # 记录平衡操作
                        self.rebalance_count += 1
                        
                        # 记录平衡操作结果
                        logger.info(f"【平衡操作】成功开立 {filled_amount} {self.base_currency} 合约空单，累计差额更新为 "
                                  f"{self.cumulative_position_diff:.8f} {self.base_currency} ({self.cumulative_position_diff_usdt:.2f} USDT)")
                        logger.info("=" * 50)
                        
                    except Exception as e:
                        logger.error(f"【平衡操作】开立合约空单失败: {str(e)}")
            
            # 3. 立即准备下单参数, 补偿一点手续费，不然现货会比合约少一些
            trade_amount = self.spot_amount * 1.001
            cost = float(trade_amount) * float(gateio_ask)
            contract_amount = self.bitget.amount_to_precision(self.contract_symbol, trade_amount)

            logger.debug(f"准备下单 - Gate.io: {self.symbol}，花费: {cost} USDT; Bitget: {self.contract_symbol}，数量: {contract_amount}")

            # 4. 立即执行交易
            try:
                spot_order, contract_order = await asyncio.gather(
                    self.gateio.create_market_buy_order(
                        symbol=self.symbol,
                        amount=cost,
                        params={'createMarketBuyOrderRequiresPrice': False, 'quoteOrderQty': True}
                    ),
                    self.bitget.create_market_sell_order(
                        symbol=self.contract_symbol,
                        amount=contract_amount,
                        params={"reduceOnly": False}
                    )
                )
                logger.debug(f"下单请求已发送 - Gate.io订单ID: {spot_order.get('id')}, Bitget订单ID: {contract_order.get('id')}")
            except Exception as e:
                logger.error(f"下单过程出错: {str(e)}")
                raise

            # 5. 交易后操作
            base_currency = self.symbol.split('/')[0]
            logger.info(f"在Gate.io市价买入 {trade_amount} {base_currency}，在Bitget市价开空单 {contract_amount} {base_currency}")
            
            # 等待一段时间，确保订单状态已更新
            await asyncio.sleep(1)
            
            # 获取最新的订单状态 - Gate.io
            spot_order_id = spot_order.get('id')
            if spot_order_id:
                try:
                    # 尝试获取已完成订单
                    updated_spot_order = None
                    
                    try:
                        closed_orders = await self.gateio.fetch_closed_orders(self.symbol, since=int(time.time() * 1000) - 60000)
                        
                        for order in closed_orders:
                            if order.get('id') == spot_order_id:
                                updated_spot_order = order
                                break
                        
                        if not updated_spot_order:
                            updated_spot_order = await self.gateio.fetch_order(spot_order_id, self.symbol)
                    except Exception as e:
                        logger.debug(f"获取已完成订单失败: {str(e)}，将直接使用fetch_order")
                        updated_spot_order = await self.gateio.fetch_order(spot_order_id, self.symbol)
                        
                    if updated_spot_order:
                        logger.debug(f"获取到Gate.io最新订单状态: {updated_spot_order.get('status')}")
                        spot_order = updated_spot_order
                except Exception as e:
                    logger.warning(f"获取Gate.io订单更新失败: {str(e)}")
            
            # 获取最新的订单状态 - Bitget
            contract_order_id = contract_order.get('id')
            if contract_order_id:
                try:
                    updated_contract_order = await self.bitget.fetch_order(contract_order_id, self.contract_symbol)
                    
                    if updated_contract_order:
                        logger.debug(f"获取到Bitget最新订单状态: {updated_contract_order.get('status')}")
                        contract_order = updated_contract_order
                except Exception as e:
                    logger.warning(f"获取Bitget订单更新失败: {str(e)}")
            
            # 检查订单执行状态
            order_verification = self.verify_order_execution(spot_order, contract_order)
            
            if not order_verification:
                logger.error("订单执行异常，终止交易！")
                return None, None

            # 获取现货订单的实际成交结果
            filled_amount = float(spot_order.get('filled', 0))
            fees = spot_order.get('fees', [])
            base_fee = sum(float(fee['cost']) for fee in fees if fee['currency'] == base_currency)
            actual_position = filled_amount - base_fee

            # 获取合约订单的实际成交数量
            contract_filled = float(contract_order.get('filled', contract_amount))
            
            # 获取成交价格信息
            spot_avg_price = float(spot_order.get('average', 0)) or float(spot_order.get('price', 0))
            contract_avg_price = float(contract_order.get('average', 0)) or float(contract_order.get('price', 0))
            
            # 计算成交价差
            exec_price_diff = contract_avg_price - spot_avg_price
            exec_price_diff_percent = (exec_price_diff / spot_avg_price * 100) if spot_avg_price > 0 else 0

            # 记录详细的成交信息
            logger.info("=" * 50)
            logger.info(f"【成交详情】订单执行情况:")
            logger.info(f"【成交详情】Gate.io卖1价格: {gateio_ask:.8f}, 实际成交价格: {spot_avg_price:.8f}, 价差: {(spot_avg_price - gateio_ask):.8f} ({(spot_avg_price - gateio_ask) / gateio_ask * 100:.4f}%)")
            logger.info(f"【成交详情】Bitget买1价格: {bitget_bid:.8f}, 实际成交价格: {contract_avg_price:.8f}, 价差: {(contract_avg_price - bitget_bid):.8f} ({(contract_avg_price - bitget_bid) / bitget_bid * 100:.4f}%)")
            logger.info(f"【成交详情】执行价差: {exec_price_diff:.8f} ({exec_price_diff_percent:.4f}%)")
            logger.info(f"【成交详情】Gate.io实际成交: {filled_amount} {base_currency}, 手续费: {base_fee} {base_currency}, 实际持仓: {actual_position} {base_currency}")
            logger.info(f"【成交详情】Bitget合约实际成交: {contract_filled} {base_currency}")
            logger.info("=" * 50)

            # 检查持仓情况
            position_balance = await self.check_positions(actual_position, contract_filled)
            
            if not position_balance:
                logger.error("持仓检查不通过，确认交易执行有问题！终止交易！")
                return None, None

            # 计算并记录本次交易的差额
            position_diff = contract_filled - actual_position  # 正值表示合约多，负值表示现货多
            self.cumulative_position_diff += position_diff
            self.cumulative_position_diff_usdt = abs(self.cumulative_position_diff * current_price)
            
            # 增加交易记录
            self.trade_count += 1
            trade_record = {
                'trade_id': self.trade_count,
                'timestamp': int(time.time()),
                'spot_filled': actual_position,
                'contract_filled': contract_filled,
                'position_diff': position_diff,
                'position_diff_usdt': position_diff * current_price,
                'price': current_price,
                'spot_price': spot_avg_price,
                'contract_price': contract_avg_price,
                'price_diff': exec_price_diff,
                'price_diff_percent': exec_price_diff_percent,
                'cumulative_diff': self.cumulative_position_diff,
                'cumulative_diff_usdt': self.cumulative_position_diff_usdt
            }
            self.trade_records.append(trade_record)
            
            # 记录交易差额和累计差额
            logger.info(f"【交易差额】- 现货: {actual_position} {base_currency}, 合约: {contract_filled} {base_currency}, "
                      f"单次差额: {position_diff:.8f} {base_currency} ({position_diff * current_price:.2f} USDT), "
                      f"累计差额: {self.cumulative_position_diff:.8f} {base_currency} ({self.cumulative_position_diff_usdt:.2f} USDT)")

            # 申购余币宝
            try:
                earn_result = gateio_subscrible_earn(base_currency, actual_position)
                logger.info(f"已将 {actual_position} {base_currency} 申购到余币宝")
            except Exception as e:
                logger.error(f"余币宝申购失败，但不影响主要交易流程: {str(e)}")

            return spot_order, contract_order

        except Exception as e:
            logger.error(f"执行对冲交易时出错: {str(e)}")
            raise

    def verify_order_execution(self, spot_order, contract_order):
        """
        验证订单执行状态
        
        Args:
            spot_order: Gate.io现货订单
            contract_order: Bitget合约订单
            
        Returns:
            bool: 订单执行是否正常
        """
        try:
            logger.debug(f"开始验证订单执行状态... - 时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
            logger.debug(f"原始订单数据 - Gate.io订单ID: {spot_order.get('id')}, Bitget订单ID: {contract_order.get('id')}")
            
            # 详细分析Gate.io现货订单
            spot_status = spot_order.get('status')
            spot_filled = float(spot_order.get('filled', 0))
            spot_amount = float(spot_order.get('amount', 0))
            spot_cost = float(spot_order.get('cost', 0))
            spot_price = float(spot_order.get('price', 0))
            spot_average = float(spot_order.get('average', 0))
            spot_fill_percent = (spot_filled / spot_amount * 100) if spot_amount > 0 else 0
            spot_fees = spot_order.get('fees', [])
            spot_fee_details = {fee.get('currency'): fee.get('cost') for fee in spot_fees} if spot_fees else {}
            
            # 输出完整的Gate.io订单结构以进行更深入的调试
            logger.debug(f"Gate.io订单完整结构: {spot_order}")
            logger.debug(f"Gate.io订单手续费详情: {spot_fee_details}")
            
            logger.debug(f"Gate.io订单详细分析:")
            logger.debug(f" - 订单ID: {spot_order.get('id')}")
            logger.debug(f" - 状态: {spot_status}")
            logger.debug(f" - 委托量: {spot_amount}")
            logger.debug(f" - 成交量: {spot_filled}")
            logger.debug(f" - 成交率: {spot_fill_percent:.2f}%")
            logger.debug(f" - 委托价格: {spot_price}")
            logger.debug(f" - 平均成交价格: {spot_average}")
            logger.debug(f" - 成交金额: {spot_cost}")
            logger.debug(f" - 订单类型: {spot_order.get('type')}")
            logger.debug(f" - 订单方向: {spot_order.get('side')}")
            logger.debug(f" - 创建时间: {spot_order.get('datetime')}")
            logger.debug(f" - 最后更新时间: {spot_order.get('lastTradeTimestamp')}")
            
            # 详细分析Bitget合约订单
            contract_status = contract_order.get('status')
            contract_filled = float(contract_order.get('filled', 0))
            contract_amount = float(contract_order.get('amount', 0))
            contract_price = float(contract_order.get('price', 0))
            contract_average = float(contract_order.get('average', 0))
            contract_cost = float(contract_order.get('cost', 0))
            contract_fill_percent = (contract_filled / contract_amount * 100) if contract_amount > 0 else 0
            contract_fees = contract_order.get('fees', [])
            contract_fee_details = {fee.get('currency'): fee.get('cost') for fee in contract_fees} if contract_fees else {}
            
            # 输出完整的Bitget订单结构以进行更深入的调试
            logger.debug(f"Bitget订单完整结构: {contract_order}")
            logger.debug(f"Bitget订单手续费详情: {contract_fee_details}")
            
            logger.debug(f"Bitget订单详细分析:")
            logger.debug(f" - 订单ID: {contract_order.get('id')}")
            logger.debug(f" - 状态: {contract_status}")
            logger.debug(f" - 委托量: {contract_amount}")
            logger.debug(f" - 成交量: {contract_filled}")
            logger.debug(f" - 成交率: {contract_fill_percent:.2f}%")
            logger.debug(f" - 委托价格: {contract_price}")
            logger.debug(f" - 平均成交价格: {contract_average}")
            logger.debug(f" - 成交金额: {contract_cost}")
            logger.debug(f" - 订单类型: {contract_order.get('type')}")
            logger.debug(f" - 订单方向: {contract_order.get('side')}")
            logger.debug(f" - 创建时间: {contract_order.get('datetime')}")
            logger.debug(f" - 最后更新时间: {contract_order.get('lastTradeTimestamp')}")
            
            # Gate.io订单验证标准：
            # 1. 状态应该是已完成或已成交
            # 2. 或者成交率应该达到95%以上（对于市价单，可能API反馈的状态不准确）
            gate_verification_passed = (
                spot_status in ['closed', 'filled'] or 
                spot_fill_percent >= 95 or
                spot_filled > 0  # 只要有成交量就算通过
            )
            
            logger.debug(f"Gate.io订单验证标准检查:")
            logger.debug(f" - 状态为closed或filled: {spot_status in ['closed', 'filled']}")
            logger.debug(f" - 成交率>=95%: {spot_fill_percent >= 95}")
            logger.debug(f" - 有成交量: {spot_filled > 0}")
            logger.debug(f" - 最终验证结果: {gate_verification_passed}")
            
            # Bitget订单验证标准：
            # 由于Bitget API返回的状态可能不准确，我们主要检查成交量
            bitget_verification_passed = True  # 默认通过，因为在check_positions中会再次验证
            logger.debug(f"Bitget默认验证通过，详细验证将在check_positions中进行")
            
            # 如果Gate.io订单成交但Bitget合约订单成交量为0，这可能有两种情况：
            # 1. Bitget API返回的信息不准确（常见情况）
            # 2. 确实只有一边成交（罕见但可能）
            if gate_verification_passed and contract_filled <= 0:
                logger.warning("Gate.io订单已成交，但Bitget订单成交量为0，将通过持仓检查确认")
                logger.debug("这可能是Bitget API返回不准确导致的，也可能是真的只有一边成交")
                # 不直接返回失败，而是让check_positions检查验证
            
            if not gate_verification_passed:
                logger.error(f"Gate.io订单验证失败: 状态={spot_status}, 成交量={spot_filled}, 成交率={spot_fill_percent:.2f}%")
                return False
            
            # 根据情况记录验证结果
            logger.info(f"订单执行验证结果 - Gate.io: {'通过' if gate_verification_passed else '失败'}, "
                        f"Bitget: {'需要通过持仓确认' if contract_filled <= 0 else '通过'}")
            
            return True
        except Exception as e:
            logger.error(f"验证订单执行状态时出错: {str(e)}")
            import traceback
            logger.debug(f"验证订单执行状态错误堆栈:\n{traceback.format_exc()}")
            logger.debug(f"验证时的数据 - spot_order: {spot_order}, contract_order: {contract_order}")
            return False

    async def check_positions(self, actual_position=None, contract_amount=None):
        """
        异步检查交易后的持仓情况
        
        Args:
            actual_position: 本次交易的现货实际持仓量（已扣除手续费）
            contract_amount: 本次交易的合约数量
        """
        try:
            # 给交易所API一点时间更新持仓数据
            await asyncio.sleep(2)  # 增加等待时间，确保API数据已更新
            logger.debug(f"开始检查持仓, 预期现货: {actual_position}, 预期合约: {contract_amount}")
            logger.debug(f"持仓检查时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")

            # 并行获取两个交易所的持仓信息
            logger.debug("正在获取Gate.io和Bitget持仓信息...")
            fetch_start_time = asyncio.get_event_loop().time()
            
            gateio_balance_task = self.gateio.fetch_balance()
            positions_task = self.bitget.fetch_positions([self.contract_symbol])
            
            # 添加重试机制，防止API偶发性错误
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    logger.debug(f"发起第 {retry_count + 1} 次持仓查询请求...")
                    gateio_balance, positions = await asyncio.gather(
                        gateio_balance_task,
                        positions_task
                    )
                    fetch_end_time = asyncio.get_event_loop().time()
                    logger.debug(f"获取持仓信息成功, 耗时: {fetch_end_time - fetch_start_time:.2f}秒")
                    break
                except Exception as e:
                    retry_count += 1
                    logger.warning(f"获取持仓信息失败 (尝试 {retry_count}/{max_retries}): {str(e)}")
                    logger.debug(f"持仓查询错误详情: {e}")
                    if retry_count >= max_retries:
                        logger.error(f"持仓查询重试次数已达上限 ({max_retries}次), 放弃查询")
                        raise
                    logger.debug(f"等待1秒后进行第 {retry_count + 1} 次重试...")
                    await asyncio.sleep(1)  # 等待1秒后重试
                    gateio_balance_task = self.gateio.fetch_balance()
                    positions_task = self.bitget.fetch_positions([self.contract_symbol])

            # 获取现货最新成交订单的信息
            base_currency = self.symbol.split('/')[0]
            
            # 记录Gate.io的所有相关资产余额
            logger.debug(f"Gate.io所有资产余额:")
            for currency, details in gateio_balance.items():
                if isinstance(details, dict) and ('free' in details or 'total' in details):
                    free = details.get('free', 0)
                    used = details.get('used', 0)
                    total = details.get('total', 0)
                    # 只记录有余额或与当前交易相关的货币
                    if total > 0 or currency in [base_currency, 'USDT']:
                        logger.debug(f" - {currency}: 可用={free}, 冻结={used}, 总计={total}")
            
            gateio_position = gateio_balance.get(base_currency, {}).get('total', 0)
            logger.debug(f"Gate.io {base_currency} 余额详情: {gateio_balance.get(base_currency, {})}")

            # 检查Bitget合约持仓
            contract_position = 0
            position_detail = None
            
            # 记录返回的所有持仓信息，便于调试
            logger.debug(f"Bitget返回的所有持仓详情: {positions}")

            if positions:
                logger.debug(f"Bitget返回持仓数量: {len(positions)}")
                for idx, position in enumerate(positions):
                    logger.debug(f"持仓 #{idx+1} - 交易对: {position.get('symbol')}, 方向: {position.get('side')}, "
                                f"数量: {position.get('contracts')}, 名义价值: {position.get('notional')}")
                    logger.debug(f"检查持仓: {position.get('symbol')} vs {self.contract_symbol}")
                    if position['symbol'] == self.contract_symbol:
                        contract_position = abs(float(position.get('contracts', 0)))
                        position_side = position.get('side', 'unknown')
                        position_leverage = position.get('leverage', self.leverage)
                        position_notional = position.get('notional', 0)
                        position_entry_price = position.get('entryPrice', 0)
                        position_detail = position  # 保存持仓详情以便后续分析

                        logger.info(f"Bitget合约持仓: {position_side} {contract_position} 合约, "
                                    f"杠杆: {position_leverage}倍, 名义价值: {position_notional}, "
                                    f"开仓均价: {position_entry_price}")
                        
                        # 记录更多持仓详情，帮助调试
                        logger.debug(f"Bitget持仓详情:")
                        for key, value in position.items():
                            if key not in ['info']:  # 排除过长的原始info字段
                                logger.debug(f"  - {key}: {value}")
                        
                        # 如果info字段中包含重要信息，也记录下来
                        if 'info' in position:
                            info = position.get('info', {})
                            if isinstance(info, dict):
                                logger.debug(f"  - info中的关键字段:")
                                for key in ['positionId', 'marginMode', 'holdSide', 'availableCloseSize']:
                                    if key in info:
                                        logger.debug(f"    * {key}: {info.get(key)}")
            else:
                logger.warning("未获取到Bitget合约持仓信息")
                # 尝试其他方法获取持仓信息
                logger.debug("尝试使用替代方法获取Bitget持仓信息...")
                
                try:
                    # 获取所有持仓
                    logger.debug("尝试获取Bitget所有持仓...")
                    all_positions = await self.bitget.fetch_positions()
                    logger.debug(f"Bitget所有持仓数量: {len(all_positions)}")
                    
                    # 记录所有持仓的简要信息
                    for idx, pos in enumerate(all_positions):
                        logger.debug(f"持仓 #{idx+1}: {pos.get('symbol')} - {pos.get('side')} {pos.get('contracts')} 合约")
                        if pos['symbol'] == self.contract_symbol:
                            contract_position = abs(float(pos.get('contracts', 0)))
                            position_detail = pos
                            logger.info(f"在所有持仓中找到目标合约: {contract_position} {base_currency}")
                            # 输出详细信息
                            logger.debug(f"找到的合约详情:")
                            for key, value in pos.items():
                                if key not in ['info']:
                                    logger.debug(f"  - {key}: {value}")
                except Exception as e:
                    logger.error(f"尝试替代方法获取Bitget持仓失败: {str(e)}")
                    logger.debug(f"替代方法错误详情: {e}")
                    import traceback
                    logger.debug(f"替代方法错误堆栈:\n{traceback.format_exc()}")

            logger.info(f"持仓检查 - Gate.io现货: {gateio_position} {base_currency}, "
                        f"Bitget合约: {contract_position} {base_currency}")
            
            # 如果提供了本次交易的具体数值，则检查本次交易是否成功执行
            if actual_position is not None and contract_amount is not None:
                # 检查现货交易结果 - 放宽要求，仅检查是否有现货余额（因为可能已经申购到余币宝）
                # 实际上现货买入后直接申购余币宝，所以现货余额可能很小
                if gateio_position < 0.01:  # 仅检查是否有最小值
                    # 尝试查询余币宝余额（如果可用）
                    try:
                        # 这里需要实现查询余币宝余额的函数
                        # 目前暂不实现，仅记录警告
                        logger.warning(f"Gate.io现货余额极低: {gateio_position} {base_currency} (可能已申购余币宝)")
                        logger.debug(f"应检查余币宝中的 {base_currency} 余额")
                    except Exception as e:
                        logger.debug(f"尝试检查余币宝失败: {str(e)}")
                        pass
                
                # 记录现货余额与预期的比较
                logger.debug(f"现货余额检查: 当前余额={gateio_position}, 预期约={actual_position}, "
                           f"差异={abs(gateio_position - actual_position):.8f} "
                           f"({abs(gateio_position - actual_position) / actual_position * 100 if actual_position > 0 else 0:.2f}%)")
                
                # 检查合约交易结果 - 必须确认合约确实开立
                if contract_position <= 0:
                    logger.error(f"Bitget合约持仓为0! 交易可能未执行")
                    return False
                
                # 记录合约持仓与预期的比较
                logger.debug(f"合约持仓检查: 当前持仓={contract_position}, 预期约={contract_amount}, "
                           f"差异={abs(contract_position - float(contract_amount)):.8f} "
                           f"({abs(contract_position - float(contract_amount)) / float(contract_amount) * 100:.2f}%)")
                
                # 如果合约持仓远低于预期，也视为可能有问题
                if contract_position < float(contract_amount) * 0.5:  # 放宽到50%
                    logger.warning(f"Bitget合约持仓量显著低于预期: 预期约 {contract_amount} {base_currency}, 实际 {contract_position} {base_currency}")
                    logger.debug("可能是部分成交或API返回的数据不准确")
                    # 此处不返回失败，但记录警告
                
                # 计算差额和百分比
                position_diff = abs(float(actual_position) - float(contract_amount))
                position_diff_percent = (position_diff / float(actual_position) * 100) if float(actual_position) > 0 else 0
                
                # 将交易后的现货和合约差额打印到日志里
                logger.info(f"【持仓检查】- 现货: {actual_position} {base_currency}, 合约: {contract_amount} {base_currency}, "
                           f"差额: {position_diff:.8f} {base_currency} ({position_diff_percent:.2f}%)")
                
                logger.info(f"交易执行检查通过 - 本次交易: 现货约 {actual_position} {base_currency} (可能已申购余币宝), 合约约 {contract_amount} {base_currency}")
                return True
            
            # 若不检查具体交易，则检查总体持仓
            # 对于运行多次的情况，不再检查持仓平衡，因为合约会不断累积
            if contract_position > 0:
                logger.info(f"合约持仓确认: {contract_position} {base_currency}")
                return True
            else:
                logger.error(f"未检测到合约持仓!")
                return False

        except Exception as e:
            logger.error(f"获取持仓信息失败: {str(e)}")
            import traceback
            logger.debug(f"获取持仓出错的堆栈:\n{traceback.format_exc()}")
            return False

    async def check_trade_requirements(self):
        """
        检查是否满足交易要求
        
        Returns:
            tuple: (可以交易，问题描述)
        """
        try:
            # 获取并保存账户余额
            self.gateio_usdt, self.bitget_usdt = await self.check_balances()
            
            # 检查余额是否满足交易要求
            orderbook = await self.gateio.fetch_order_book(self.symbol)
            current_price = float(orderbook['asks'][0][0])
            
            required_usdt = float(self.spot_amount) * current_price * 1.02
            required_margin = float(self.spot_amount) * current_price / self.leverage * 1.05
            
            if required_usdt > self.gateio_usdt:
                return False, f"Gate.io USDT余额不足，需要约 {required_usdt:.2f} USDT，当前余额 {self.gateio_usdt:.2f} USDT"
                
            if required_margin > self.bitget_usdt:
                return False, f"Bitget USDT保证金不足，需要约 {required_margin:.2f} USDT，当前余额 {self.bitget_usdt:.2f} USDT"
                
            return True, "满足交易要求"
            
        except Exception as e:
            return False, f"检查交易要求时出错: {str(e)}"


def parse_arguments():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='Gate.io现货与Bitget合约对冲交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, required=True, help='购买的现货数量')
    parser.add_argument('-p', '--min-spread', type=float, default=-0.0001, help='最小价差要求，默认0.001 (0.1%%)')
    parser.add_argument('-l', '--leverage', type=int, help='合约杠杆倍数，如果不指定则使用该交易对支持的最大杠杆倍数')
    parser.add_argument('-c', '--count', type=int, default=1, help='重复执行交易的次数，默认为1次')
    parser.add_argument('--test-earn', action='store_true', help='测试余币宝申购功能')
    parser.add_argument('-d', '--debug', action='store_true', help='启用调试日志')  # 添加调试参数
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
    异步主函数
    """
    try:
        # 解析命令行参数
        args = parse_arguments()
        
        # 设置日志级别
        if args.debug:
            logger.setLevel(logging.DEBUG)
            logger.debug("已启用调试日志模式")
        else:
            logger.setLevel(logging.INFO)
            
        logger.info(f"启动程序 - 交易对: {args.symbol}, 交易量: {args.amount}, 最小价差: {args.min_spread}, "
                    f"杠杆: {args.leverage if args.leverage else '自动'}, 重复次数: {args.count}")

        # 如果是测试模式，只测试余币宝功能
        if args.test_earn:
            logger.info("进入余币宝测试模式")
            await test_earn_subscription()
            return 0

        # 创建并初始化交易器
        trader = HedgeTrader(
            symbol=args.symbol,
            spot_amount=args.amount,
            min_spread=args.min_spread,
            leverage=args.leverage  # 如果没有指定，这里会是None
        )
        
        await trader.initialize()

        # 记录交易次数
        completed_trades = 0
        target_count = args.count
        total_errors = 0
        consecutive_errors = 0

        logger.info(f"计划执行 {target_count} 次交易操作")

        # 循环执行交易，直到达到指定次数
        while completed_trades < target_count:
            try:
                # 执行前检查余额是否足够
                can_trade, reason = await trader.check_trade_requirements()
                
                if not can_trade:
                    try:
                        # 如果是因为Gate.io USDT余额不足，尝试从余币宝赎回
                        if "Gate.io USDT余额不足" in reason:
                            logger.info(f"Gate.io USDT余额不足，尝试从余币宝赎回资金")
                            # 估算所需资金
                            orderbook = await trader.gateio.fetch_order_book(trader.symbol)
                            current_price = float(orderbook['asks'][0][0])
                            required_usdt = float(trader.spot_amount) * current_price * 1.02
                            
                            redeem_result = redeem_earn('USDT', max(required_usdt * 1.01, 50))
                            logger.debug(f"余币宝赎回结果: {redeem_result}")
                            
                            # 重新检查交易要求
                            can_trade, new_reason = await trader.check_trade_requirements()
                            
                            if not can_trade:
                                logger.error(f"赎回后仍不满足交易要求: {new_reason}")
                                logger.info(f"已完成 {completed_trades}/{target_count} 次交易，因资金不足退出")
                                break
                        else:
                            # 其他原因导致无法交易
                            logger.error(f"不满足交易要求: {reason}")
                            logger.info(f"已完成 {completed_trades}/{target_count} 次交易，退出")
                            break
                    except Exception as e:
                        logger.error(f"处理交易要求问题时出错: {str(e)}")
                        logger.info(f"已完成 {completed_trades}/{target_count} 次交易，退出")
                        break
                
                # 执行交易
                logger.info(f"开始执行第 {completed_trades + 1}/{target_count} 次交易...")
                spot_order, contract_order = await trader.execute_hedge_trade()
                
                # 检查交易结果，任何失败都立即退出
                if spot_order is None or contract_order is None:
                    total_errors += 1
                    consecutive_errors += 1
                    logger.error(f"第 {completed_trades + 1} 次交易执行失败，可能原因: 订单执行异常或持仓不匹配")
                    
                    # 如果连续3次失败，终止程序
                    if consecutive_errors >= 3:
                        logger.error(f"连续 {consecutive_errors} 次交易执行失败，终止后续交易")
                        break
                    
                    logger.warning(f"交易失败，但将继续尝试下一次交易 (已连续失败 {consecutive_errors} 次)")
                    # 等待一段时间再尝试下一次
                    logger.info(f"等待10秒后继续尝试下一次交易...")
                    await asyncio.sleep(10)
                    continue
                
                # 交易成功
                completed_trades += 1
                consecutive_errors = 0  # 重置连续失败计数
                logger.info(f"第 {completed_trades}/{target_count} 次对冲交易成功完成!")
                
                # 输出当前累计差额信息
                if hasattr(trader, 'cumulative_position_diff') and hasattr(trader, 'cumulative_position_diff_usdt'):
                    logger.info(f"当前累计差额: {trader.cumulative_position_diff:.8f} {trader.symbol.split('/')[0]} "
                              f"({trader.cumulative_position_diff_usdt:.2f} USDT)")
                
                # 如果不是最后一次交易，等待一小段时间再继续
                if completed_trades < target_count:
                    logger.info(f"等待5秒后继续下一次交易...")
                    await asyncio.sleep(5)
                    
            except Exception as e:
                total_errors += 1
                consecutive_errors += 1
                logger.error(f"执行第 {completed_trades + 1} 次交易时出错: {str(e)}")
                
                # 如果连续3次失败，终止程序
                if consecutive_errors >= 3:
                    logger.error(f"连续 {consecutive_errors} 次交易执行失败，终止后续交易")
                    break
                    
                logger.warning(f"交易失败，但将继续尝试下一次交易 (已连续失败 {consecutive_errors} 次)")
                # 等待一段时间再尝试下一次
                logger.info(f"等待10秒后继续尝试下一次交易...")
                await asyncio.sleep(10)
                
        # 打印最终执行结果和累计差额信息
        if completed_trades == target_count:
            logger.info(f"所有计划交易已完成! 成功执行 {completed_trades}/{target_count} 次交易")
        else:
            logger.info(f"交易过程中止，成功执行 {completed_trades}/{target_count} 次交易")
        
        # 输出最终累计差额信息
        if hasattr(trader, 'cumulative_position_diff') and hasattr(trader, 'cumulative_position_diff_usdt'):
            logger.info(f"最终累计差额: {trader.cumulative_position_diff:.8f} {trader.symbol.split('/')[0]} "
                      f"({trader.cumulative_position_diff_usdt:.2f} USDT)")
            if hasattr(trader, 'rebalance_count') and trader.rebalance_count > 0:
                logger.info(f"执行了 {trader.rebalance_count} 次平衡操作")
        
        if total_errors > 0:
            logger.warning(f"执行过程中共发生 {total_errors} 次错误")

    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        if 'completed_trades' in locals() and 'target_count' in locals():
            logger.info(f"已完成 {completed_trades}/{target_count} 次交易")
        return 1
    finally:
        # 确保关闭交易所连接
        if 'trader' in locals():
            try:
                await asyncio.gather(
                    trader.gateio.close(),
                    trader.bitget.close()
                )
            except Exception as e:
                logger.error(f"关闭交易所连接时出错: {str(e)}")

    return 0


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close()
