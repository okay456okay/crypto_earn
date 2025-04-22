#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io现货买入与Bybit合约空单对冲的套利脚本

此脚本实现以下功能：
1. 从Gate.io市价买入指定token的现货
2. 从Bybit开对应的合约空单进行对冲
3. 确保现货和合约仓位保持一致
4. 检查价差是否满足最小套利条件
5. 监控和记录交易执行情况
6. 支持重复执行多次交易操作
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
from config import bybit_api_key, bybit_api_secret, gateio_api_secret, gateio_api_key, proxies
from trade.gateio_api import subscrible_earn as gateio_subscrible_earn
from trade.gateio_api import redeem_earn


class HedgeTrader:
    """
    现货-合约对冲交易类，实现Gate.io现货买入与Bybit合约空单对冲
    """

    def __init__(self, symbol, spot_amount=None, min_spread=0.001, leverage=10, depth_multiplier=10):
        """
        初始化基本属性

        Args:
            symbol (str): 交易对符号，例如 'ETH/USDT'
            spot_amount (float, optional): 现货买入数量. Defaults to None.
            min_spread (float, optional): 最小价差要求. Defaults to 0.001.
            leverage (int, optional): 合约杠杆倍数. Defaults to 10.
            depth_multiplier (int, optional): 市场深度要求的乘数. Defaults to 10.
        """
        self.symbol = symbol
        self.spot_amount = spot_amount
        self.min_spread = min_spread
        self.leverage = leverage
        self.depth_multiplier = depth_multiplier

        # 设置合约交易对
        base, quote = symbol.split('/')
        self.contract_symbol = f"{base}{quote}"  # 例如: ETHUSDT
        self.base_currency = base  # 例如: ETH

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

        self.bybit = ccxtpro.bybit({
            'apiKey': bybit_api_key,
            'secret': bybit_api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'linear',  # 设置默认为USDT永续合约
                'createMarketBuyOrderRequiresPrice': False,
            },
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
        })

        self.gateio_usdt = 0
        self.bybit_usdt = None

        # 用于存储最新订单簿数据
        self.orderbooks = {
            'gateio': None,
            'bybit': None
        }

        # 用于控制WebSocket订阅
        self.ws_running = False
        self.price_updates = asyncio.Queue()
        
        # 记录本次操作的实际持仓数量
        self.last_trade_spot_amount = 0
        self.last_trade_contract_amount = 0
        
        # 记录累计的现货和合约差额
        self.cumulative_position_diff = 0  # 正值表示合约多于现货，负值表示现货多于合约
        self.cumulative_position_diff_usdt = 0  # 以USDT计价的累计差额
        self.trade_records = []  # 交易记录列表
        self.trade_count = 0  # 交易计数
        self.rebalance_count = 0  # 平衡操作计数

    async def initialize(self):
        """
        异步初始化方法，执行需要网络请求的初始化操作
        """
        try:
            # 设置Bybit合约参数
            params = {
                'category': 'linear',
                'symbol': self.contract_symbol,
                'buyLeverage': str(self.leverage),
                'sellLeverage': str(self.leverage)
            }
            
            try:
                # 先尝试设置持仓模式为单向持仓
                await self.bybit.privatePostV5PositionSwitchMode({
                    'category': 'linear',
                    'symbol': self.contract_symbol,
                    'mode': 0  # 0: 单向持仓, 3: 双向持仓
                })
                logger.info("设置Bybit持仓模式为单向持仓")
            except Exception as e:
                logger.warning(f"设置持仓模式失败（可能已经是单向持仓）: {str(e)}")

            # 如果杠杆倍数未指定，获取最大杠杆倍数
            if self.leverage is None:
                max_leverage = await self.get_max_leverage()
                self.leverage = max_leverage
                params['buyLeverage'] = str(max_leverage)
                params['sellLeverage'] = str(max_leverage)
                logger.info(f"使用Bybit支持的最大杠杆倍数: {max_leverage}倍")

            # 设置杠杆
            try:
                await self.bybit.privatePostV5PositionSetLeverage(params)
                logger.info(f"设置Bybit合约杠杆倍数为: {self.leverage}倍")
            except Exception as e:
                if "leverage not modified" in str(e).lower():
                    logger.info(f"杠杆倍数已经是 {self.leverage}倍，无需修改")
                else:
                    raise

            logger.info(f"初始化完成: 交易对={self.symbol}, 合约对={self.contract_symbol}, "
                        f"最小价差={self.min_spread * 100}%, 杠杆={self.leverage}倍")

            # 获取并保存账户余额
            self.gateio_usdt, self.bybit_usdt = await self.check_balances()

            # 检查余额是否满足交易要求
            if self.spot_amount is not None:
                orderbook = await self.gateio.fetch_order_book(self.symbol)
                current_price = float(orderbook['asks'][0][0])

                required_usdt = float(self.spot_amount) * current_price * 1.02
                required_margin = float(self.spot_amount) * current_price / self.leverage * 1.05

                if required_usdt > self.gateio_usdt or self.gateio_usdt < 50:
                    # raise Exception(f"Gate.io USDT余额不足，需要约 {required_usdt:.2f} USDT，当前余额 {self.gateio_usdt:.2f} USDT")
                    redeem_earn('USDT', max(required_usdt * 1.01, 50))
                    # 重新获取并保存账户余额
                    self.gateio_usdt, self.bybit_usdt = await self.check_balances()
                    if required_usdt > self.gateio_usdt:
                        raise Exception(f"Gate.io USDT余额不足，需要约 {required_usdt:.2f} USDT，当前余额 {self.gateio_usdt:.2f} USDT")

                if required_margin > self.bybit_usdt:
                    raise Exception(
                        f"Bybit USDT保证金不足，需要约 {required_margin:.2f} USDT，当前余额 {self.bybit_usdt:.2f} USDT")

                logger.info(
                    f"账户余额检查通过 - 预估所需Gate.io: {required_usdt:.2f} USDT, Bybit: {required_margin:.2f} USDT")

        except Exception as e:
            logger.exception(f"初始化失败: {str(e)}")
            raise

    async def check_balances(self):
        """
        检查Gate.io和Bybit的账户余额
        
        Returns:
            tuple: (gateio_balance, bybit_balance) - 返回两个交易所的USDT余额
        """
        try:
            # 并行获取两个交易所的余额
            gateio_balance, bybit_balance = await asyncio.gather(
                self.gateio.fetch_balance(),
                self.bybit.fetch_balance({'type': 'swap'})
            )

            gateio_usdt = gateio_balance.get('USDT', {}).get('free', 0)
            bybit_usdt = bybit_balance.get('USDT', {}).get('free', 0)

            logger.info(f"账户余额 - Gate.io: {gateio_usdt} USDT, Bybit: {bybit_usdt} USDT")
            return gateio_usdt, bybit_usdt

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
                        asyncio.create_task(self.bybit.watch_order_book(self.contract_symbol))
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
                                logger.debug(f"{self.symbol}收到Gate.io订单簿更新")
                            else:  # bybit task
                                self.orderbooks['bybit'] = ob
                                logger.debug(f"{self.symbol} 收到Bybit订单簿更新")

                            # 如果两个订单簿都有数据，检查价差
                            if self.orderbooks['gateio'] and self.orderbooks['bybit']:
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
                    self.bybit.close()
                )
            except Exception as e:
                logger.error(f"关闭WebSocket连接时出错: {str(e)}")

    async def check_spread_from_orderbooks(self):
        """从已缓存的订单簿数据中检查价差"""
        try:
            gateio_ob = self.orderbooks['gateio']
            bybit_ob = self.orderbooks['bybit']

            if not gateio_ob or not bybit_ob:
                return

            gateio_ask = Decimal(str(gateio_ob['asks'][0][0]))
            gateio_ask_volume = Decimal(str(gateio_ob['asks'][0][1]))

            bybit_bid = Decimal(str(bybit_ob['bids'][0][0]))
            bybit_bid_volume = Decimal(str(bybit_ob['bids'][0][1]))

            spread = bybit_bid - gateio_ask
            spread_percent = spread / gateio_ask

            # 将价差数据放入队列
            spread_data = {
                'spread_percent': float(spread_percent),
                'gateio_ask': float(gateio_ask),
                'bybit_bid': float(bybit_bid),
                'gateio_ask_volume': float(gateio_ask_volume),
                'bybit_bid_volume': float(bybit_bid_volume)
            }
            await self.price_updates.put(spread_data)

        except Exception as e:
            logger.error(f"{self.symbol}检查订单簿价差时出错: {str(e)}")

    async def execute_hedge_trade(self):
        """执行对冲交易"""
        try:
            subscription_task = None
            try:
                # 启动WebSocket订阅
                self.ws_running = True
                subscription_task = asyncio.create_task(self.subscribe_orderbooks())

                while True:
                    try:
                        # 从队列中获取最新价差数据，设置超时
                        spread_data = await asyncio.wait_for(
                            self.price_updates.get(),
                            timeout=30  # 30秒超时
                        )

                        spread_percent = spread_data['spread_percent']
                        gateio_ask = spread_data['gateio_ask']
                        gateio_ask_volume = spread_data['gateio_ask_volume']
                        bybit_bid = spread_data['bybit_bid']
                        bybit_bid_volume = spread_data['bybit_bid_volume']

                        # 检查价差和数量条件
                        if spread_percent >= self.min_spread and \
                           gateio_ask_volume >= self.spot_amount * self.depth_multiplier and \
                           bybit_bid_volume >= self.spot_amount * self.depth_multiplier:
                            
                            # 记录交易前的市场状态
                            logger.info("\n【交易前市场状态】")
                            logger.info(f"Gate.io - 卖一: {gateio_ask:.8f} (数量: {gateio_ask_volume:.8f}), "
                                      f"买一: {self.orderbooks['gateio']['bids'][0][0]:.8f} (数量: {self.orderbooks['gateio']['bids'][0][1]:.8f})")
                            logger.info(f"Bybit  - 买一: {bybit_bid:.8f} (数量: {bybit_bid_volume:.8f}), "
                                      f"卖一: {self.orderbooks['bybit']['asks'][0][0]:.8f} (数量: {self.orderbooks['bybit']['asks'][0][1]:.8f})")
                            logger.info(f"当前价差: {spread_percent * 100:.4f}%")

                            # 记录预期成交价格
                            spot_expected_price = gateio_ask
                            contract_expected_price = bybit_bid
                            logger.info(f"\n【预期成交价格】")
                            logger.info(f"Gate.io预期成交价: {spot_expected_price:.8f}")
                            logger.info(f"Bybit预期成交价: {contract_expected_price:.8f}")

                            # 准备交易参数
                            trade_amount = self.spot_amount
                            contract_amount = self.spot_amount
                            
                            # 立即执行交易
                            spot_order = None
                            contract_order = None
                            try:
                                spot_order, contract_order = await asyncio.gather(
                                    self.gateio.create_market_buy_order(
                                        symbol=self.symbol,
                                        amount=trade_amount,
                                        params={'createMarketBuyOrderRequiresPrice': False}
                                    ),
                                    self.bybit.create_market_sell_order(
                                        symbol=self.contract_symbol,
                                        amount=contract_amount,
                                        params={
                                            "category": "linear",
                                            "positionIdx": 0,  # 单向持仓
                                            "reduceOnly": False
                                        }
                                    )
                                )
                                
                                logger.debug(f"订单提交结果 - Gate.io现货订单: {spot_order}")
                                logger.debug(f"订单提交结果 - Bybit合约订单: {contract_order}")
                            except Exception as e:
                                logger.error(f"下单过程中出错: {str(e)}")
                                if spot_order and not contract_order:
                                    logger.error("Gate.io现货订单已提交，但Bybit合约订单失败")
                                elif contract_order and not spot_order:
                                    logger.error("Bybit合约订单已提交，但Gate.io现货订单失败")
                                raise

                            # 等待一小段时间，确保订单状态更新
                            await asyncio.sleep(2)
                                
                            # 获取最新的订单信息
                            spot_order_id = spot_order.get('id')
                            contract_order_id = contract_order.get('id')
                            
                            try:
                                # 尝试获取已完成的订单信息
                                if spot_order_id:
                                    try:
                                        updated_spot_order = await self.gateio.fetch_order(spot_order_id, self.symbol)
                                        if updated_spot_order:
                                            spot_order = updated_spot_order
                                    except Exception as e:
                                        logger.warning(f"获取Gate.io订单详情失败: {str(e)}")
                                
                                if contract_order_id:
                                    try:
                                        closed_orders = await self.bybit.fetch_closed_orders(self.contract_symbol, limit=10)
                                        for order in closed_orders:
                                            if order.get('id') == contract_order_id:
                                                contract_order = order
                                                break
                                    except Exception as e:
                                        logger.warning(f"获取Bybit订单详情失败: {str(e)}")
                            except Exception as e:
                                logger.warning(f"获取订单详情时出错: {str(e)}")

                            # 获取现货订单的实际成交结果
                            filled_amount = float(spot_order.get('filled', 0))
                            if filled_amount <= 0:
                                logger.warning("Gate.io订单似乎未成交，将从balance中获取实际成交量")
                                before_balance = await self.gateio.fetch_balance()
                                await asyncio.sleep(1)
                                after_balance = await self.gateio.fetch_balance()
                                
                                before_amount = before_balance.get(self.base_currency, {}).get('total', 0)
                                after_amount = after_balance.get(self.base_currency, {}).get('total', 0)
                                filled_amount = float(after_amount) - float(before_amount)
                            
                            fees = spot_order.get('fees', [])
                            base_fee = sum(float(fee.get('cost', 0)) for fee in fees if fee.get('currency') == self.base_currency)
                            actual_position = filled_amount - base_fee
                            
                            # 计算现货实际成交均价和滑点
                            spot_avg_price = 0
                            if filled_amount > 0:
                                spot_cost = float(spot_order.get('cost', 0))
                                spot_avg_price = spot_cost / filled_amount
                                spot_price_diff = spot_avg_price - spot_expected_price
                                spot_slippage = (spot_price_diff / spot_expected_price) * 100
                                
                                logger.info(f"\n【现货成交结果】")
                                logger.info(f"成交数量: {filled_amount:.8f} {self.base_currency}")
                                logger.info(f"预期价格: {spot_expected_price:.8f}")
                                logger.info(f"成交均价: {spot_avg_price:.8f}")
                                logger.info(f"价格差异: {spot_price_diff:+.8f} ({spot_slippage:+.4f}%)")
                            
                            # 获取合约订单的实际成交结果
                            contract_filled = float(contract_order.get('filled', 0))
                            if contract_filled <= 0:
                                logger.warning("Bybit订单信息中无成交量数据，将从positions中获取")
                                try:
                                    positions = await self.bybit.fetch_positions([self.contract_symbol])
                                    for position in positions:
                                        if position['info']['symbol'] == self.contract_symbol:
                                            contract_filled = abs(float(position.get('contracts', 0)))
                                            break
                                except Exception as e:
                                    logger.warning(f"从持仓获取合约成交量失败: {str(e)}")
                                    contract_filled = float(contract_amount)
                            
                            # 计算合约实际成交均价和滑点
                            contract_avg_price = float(contract_order.get('average', 0))
                            if contract_avg_price <= 0:
                                contract_cost = float(contract_order.get('cost', 0))
                                if contract_cost > 0 and contract_filled > 0:
                                    contract_avg_price = contract_cost / contract_filled
                                else:
                                    contract_avg_price = contract_expected_price
                            
                            if contract_filled > 0:
                                contract_price_diff = contract_avg_price - contract_expected_price
                                contract_slippage = (contract_price_diff / contract_expected_price) * 100
                                
                                logger.info(f"\n【合约成交结果】")
                                logger.info(f"成交数量: {contract_filled:.8f} {self.base_currency}")
                                logger.info(f"预期价格: {contract_expected_price:.8f}")
                                logger.info(f"成交均价: {contract_avg_price:.8f}")
                                logger.info(f"价格差异: {contract_price_diff:+.8f} ({contract_slippage:+.4f}%)")
                            
                            # 记录本次交易的实际数量
                            self.last_trade_spot_amount = max(actual_position, 0)
                            self.last_trade_contract_amount = max(contract_filled, 0)

                            # 计算最终价差和盈利空间
                            if spot_avg_price > 0 and contract_avg_price > 0:
                                final_spread = contract_avg_price - spot_avg_price
                                final_spread_percent = (final_spread / spot_avg_price) * 100
                                profit_estimate = final_spread * min(actual_position, contract_filled)
                                
                                logger.info(f"\n【交易结果】")
                                logger.info(f"最终价差: {final_spread:.8f} USDT ({final_spread_percent:.4f}%)")
                                logger.info(f"预估利润: {profit_estimate:.4f} USDT")
                            
                            # 检查持仓是否平衡
                            await self.check_trade_balance()

                            # 申购余币宝
                            try:
                                if actual_position > 0:
                                    gateio_subscrible_earn(self.base_currency, actual_position)
                                    logger.info(f"已将 {actual_position} {self.base_currency} 申购到余币宝")
                            except Exception as e:
                                logger.error(f"余币宝申购失败: {str(e)}")

                            return spot_order, contract_order

                        else:
                            if spread_percent < self.min_spread:
                                logger.debug(f"价差不足: {spread_percent * 100:.4f}% < {self.min_spread * 100:.4f}%")
                            if gateio_ask_volume < self.spot_amount * self.depth_multiplier:
                                logger.debug(f"Gate.io卖一数量不足: {gateio_ask_volume:.8f} < {self.spot_amount * self.depth_multiplier:.8f}")
                            if bybit_bid_volume < self.spot_amount * self.depth_multiplier:
                                logger.debug(f"Bybit买一数量不足: {bybit_bid_volume:.8f} < {self.spot_amount * self.depth_multiplier:.8f}")

                    except asyncio.TimeoutError:
                        logger.warning("等待价差数据超时，重新订阅订单簿")
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

        except Exception as e:
            logger.error(f"执行对冲交易时出错: {str(e)}")
            raise

    async def check_positions(self):
        """异步检查交易后的持仓情况"""
        try:
            # 并行获取两个交易所的持仓信息
            gateio_balance_task = self.gateio.fetch_balance()
            positions_task = self.bybit.fetch_positions([self.contract_symbol], {'category': 'linear'})

            gateio_balance, positions = await asyncio.gather(
                gateio_balance_task,
                positions_task
            )

            # 获取现货最新成交订单的信息
            gateio_position = gateio_balance.get(self.base_currency, {}).get('total', 0)

            # 检查Bybit合约持仓
            contract_position = 0

            if positions:
                for position in positions:
                    if position['info']['symbol'] == self.contract_symbol:
                        contract_position = abs(float(position.get('contracts', 0)))
                        position_side = position.get('side', 'unknown')
                        position_leverage = position.get('leverage', self.leverage)
                        position_notional = position.get('notional', 0)

                        logger.info(f"Bybit合约持仓: {position_side} {contract_position} 合约, "
                                    f"杠杆: {position_leverage}倍, 名义价值: {position_notional}")
            else:
                logger.warning("未获取到Bybit合约持仓信息")

            logger.info(f"持仓检查 - Gate.io现货: {gateio_position} {self.base_currency}, "
                        f"Bybit合约: {contract_position} {self.base_currency}")

            # 检查是否平衡（允许0.5%的误差）
            position_diff = abs(float(gateio_position) - float(contract_position))
            position_diff_percent = position_diff / float(gateio_position) * 100

            if position_diff_percent > 0.5:  # 允许0.5%的误差
                logger.warning(
                    f"现货和合约持仓不平衡! 差异: {position_diff} {self.base_currency} ({position_diff_percent:.2f}%)")
            else:
                logger.info(
                    f"现货和合约持仓基本平衡，差异在允许范围内: {position_diff} {self.base_currency} ({position_diff_percent:.2f}%)")

        except Exception as e:
            logger.error(f"获取Bybit合约持仓信息失败: {str(e)}")

    async def get_max_leverage(self):
        """
        获取Bybit交易所支持的最大杠杆倍数
        
        Returns:
            int: 最大杠杆倍数
        """
        try:
            # 获取交易对信息
            response = await self.bybit.publicGetV5MarketInstrumentsInfo({
                'category': 'linear',
                'symbol': self.contract_symbol
            })
            
            if response and 'result' in response and 'list' in response['result']:
                for instrument in response['result']['list']:
                    if instrument['symbol'] == self.contract_symbol:
                        # 先将字符串转换为float，再转换为int
                        max_leverage = int(float(instrument['leverageFilter']['maxLeverage']))
                        logger.info(f"获取到{self.contract_symbol}最大杠杆倍数: {max_leverage}倍")
                        return max_leverage
            
            logger.warning(f"未能获取到{self.contract_symbol}的最大杠杆倍数，使用默认值10倍")
            return 10  # 如果获取失败，返回默认值10倍
            
        except Exception as e:
            logger.error(f"获取最大杠杆倍数时出错: {str(e)}")
            return 10  # 如果出错，返回默认值10倍

    async def check_trade_balance(self):
        """
        检查本次交易的现货和合约持仓是否平衡
        
        Returns:
            bool: 如果持仓平衡返回True，否则返回False
        """
        try:
            # 检查是否有交易数据
            logger.debug(f"检查本次交易平衡 - 现货记录: {self.last_trade_spot_amount}, 合约记录: {self.last_trade_contract_amount}")
            
            if self.last_trade_spot_amount <= 0 and self.last_trade_contract_amount <= 0:
                logger.warning("无有效的交易数据用于检查持仓平衡")
                return False
            
            # 特殊情况：如果有一方为0，可能是数据获取失败，如果另一方有数据，我们认为这次交易是成功的
            if self.last_trade_spot_amount <= 0:
                logger.warning(f"现货记录为0，但合约记录为{self.last_trade_contract_amount}，视为成功交易")
                return True
                
            if self.last_trade_contract_amount <= 0:
                logger.warning(f"合约记录为0，但现货记录为{self.last_trade_spot_amount}，视为成功交易")
                return True
                
            # 计算持仓差异 (正值表示合约多，负值表示现货多)
            position_diff = self.last_trade_contract_amount - self.last_trade_spot_amount
            position_diff_abs = abs(position_diff)
            position_diff_percent = position_diff_abs / self.last_trade_spot_amount * 100
            
            # 记录交易
            self.trade_count += 1
            
            # 获取当前价格用于计算USDT价值
            current_price = 0
            gateio_asks = []
            gateio_bids = []
            bybit_asks = []
            bybit_bids = []
            
            try:
                # 获取最新的订单簿快照以记录市场状态
                orderbook_gateio = await self.gateio.fetch_order_book(self.symbol)
                orderbook_bybit = await self.bybit.fetch_order_book(self.contract_symbol)
                
                current_price = float(orderbook_gateio['asks'][0][0])
                
                # 获取前3档买卖盘数据
                gateio_asks = orderbook_gateio['asks'][:3]
                gateio_bids = orderbook_gateio['bids'][:3]
                bybit_asks = orderbook_bybit['asks'][:3]
                bybit_bids = orderbook_bybit['bids'][:3]
                
                logger.debug(f"当前市场状态 - Gate.io 卖盘: {gateio_asks}, 买盘: {gateio_bids}")
                logger.debug(f"当前市场状态 - Bybit 卖盘: {bybit_asks}, 买盘: {bybit_bids}")
                
            except Exception as e:
                logger.warning(f"获取当前价格失败: {str(e)}")
                # 如果无法获取当前价格，使用最近交易的平均价格
                if self.last_trade_spot_amount > 0:
                    current_price = 1  # 默认值，后续可能被覆盖
            
            # 更新累计差额
            self.cumulative_position_diff += position_diff
            self.cumulative_position_diff_usdt = self.cumulative_position_diff * current_price
            
            # 记录交易信息
            trade_record = {
                'trade_id': self.trade_count,
                'timestamp': int(time.time()),
                'spot_filled': self.last_trade_spot_amount,
                'contract_filled': self.last_trade_contract_amount,
                'position_diff': position_diff,
                'position_diff_usdt': position_diff * current_price,
                'price': current_price,
                'cumulative_diff': self.cumulative_position_diff,
                'cumulative_diff_usdt': self.cumulative_position_diff_usdt,
                'gateio_asks': gateio_asks,
                'gateio_bids': gateio_bids,
                'bybit_asks': bybit_asks,
                'bybit_bids': bybit_bids,
                'is_rebalance': False
            }
            self.trade_records.append(trade_record)
            
            logger.info(f"本次交易持仓对比 - 现货: {self.last_trade_spot_amount} {self.base_currency}, "
                      f"合约: {self.last_trade_contract_amount} {self.base_currency}, "
                      f"差异: {position_diff_abs} {self.base_currency} ({position_diff_percent:.2f}%)")
            
            # 打印累计差额信息
            logger.info(f"【累计差额】- 数量: {self.cumulative_position_diff:.8f} {self.base_currency}, "
                      f"价值: {self.cumulative_position_diff_usdt:.2f} USDT")
            
            # 检查是否平衡（允许0.5%的误差）
            if position_diff_percent > 0.5:  # 允许0.5%的误差
                logger.warning(f"本次交易现货和合约持仓不平衡! 差异: {position_diff_abs} {self.base_currency} ({position_diff_percent:.2f}%)")
                return False
            else:
                logger.info(f"本次交易现货和合约持仓基本平衡，差异在允许范围内: {position_diff_abs} {self.base_currency} ({position_diff_percent:.2f}%)")
                return True
                
        except Exception as e:
            logger.error(f"检查本次交易持仓平衡时出错: {str(e)}")
            import traceback
            logger.debug(f"错误堆栈:\n{traceback.format_exc()}")
            # 出错时，我们认为交易是成功的，错误主要来自检查逻辑
            return True


def parse_arguments():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='Gate.io现货与Bybit合约对冲交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, required=True, help='购买的现货数量')
    parser.add_argument('-p', '--min-spread', type=float, default=-0.0001, help='最小价差要求，默认0.001 (0.1%%)')
    parser.add_argument('-l', '--leverage', type=int, help='合约杠杆倍数，如果不指定则使用交易所支持的最大杠杆倍数')
    parser.add_argument('-c', '--count', type=int, default=1, help='重复执行交易操作的次数，默认为1次')
    parser.add_argument('-m', '--depth-multiplier', type=int, default=10, help='市场深度要求的乘数，默认为交易量的10倍')
    parser.add_argument('--test-earn', action='store_true', help='测试余币宝申购功能')
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
    异步主函数
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

    # 获取要执行的次数
    total_count = max(1, args.count)  # 确保至少执行一次
    executed_count = 0
    successful_count = 0
    
    logger.info(f"开始执行对冲交易，计划执行 {total_count} 次")
    
    try:
        # 创建并初始化交易器
        trader = HedgeTrader(
            symbol=args.symbol,
            spot_amount=args.amount,
            min_spread=args.min_spread,
            leverage=args.leverage,
            depth_multiplier=args.depth_multiplier
        )
        
        logger.debug(f"初始化交易器参数: 交易对={args.symbol}, 数量={args.amount}, "
                    f"最小价差={args.min_spread}, 杠杆={args.leverage}")
        
        await trader.initialize()

        # 循环执行交易操作指定次数
        for i in range(total_count):
            current_iteration = i + 1
            logger.info(f"开始执行第 {current_iteration}/{total_count} 次交易操作")
            
            try:
                # 每次循环前重新检查余额是否足够
                trader.gateio_usdt, trader.bybit_usdt = await trader.check_balances()
                
                # 计算所需的USDT
                orderbook = await trader.gateio.fetch_order_book(trader.symbol)
                current_price = float(orderbook['asks'][0][0])
                
                required_usdt = float(trader.spot_amount) * current_price * 1.02
                required_margin = float(trader.spot_amount) * current_price / trader.leverage * 1.05
                
                logger.debug(f"当前市场价格: {current_price} USDT，需要Gate.io: {required_usdt:.2f} USDT，"
                           f"需要Bybit保证金: {required_margin:.2f} USDT")
                
                # 检查余额是否足够
                if required_usdt > trader.gateio_usdt or trader.gateio_usdt < 50:
                    redemption_needed = max(required_usdt * 1.01, 50)
                    logger.info(f"Gate.io USDT余额不足 {trader.gateio_usdt:.2f}，尝试从余币宝赎回 {redemption_needed:.2f} USDT")
                    
                    try:
                        redeem_earn('USDT', redemption_needed)
                        # 重新获取并保存账户余额
                        trader.gateio_usdt, trader.bybit_usdt = await trader.check_balances()
                        if required_usdt > trader.gateio_usdt:
                            logger.error(f"Gate.io USDT余额仍不足，需要约 {required_usdt:.2f} USDT，"
                                        f"当前余额 {trader.gateio_usdt:.2f} USDT，无法继续执行")
                            break
                        else:
                            logger.info(f"赎回USDT成功，当前Gate.io余额: {trader.gateio_usdt:.2f} USDT")
                    except Exception as e:
                        logger.error(f"赎回USDT失败: {str(e)}，无法继续执行")
                        break
                
                if required_margin > trader.bybit_usdt:
                    logger.error(f"Bybit USDT保证金不足，需要约 {required_margin:.2f} USDT，"
                               f"当前余额 {trader.bybit_usdt:.2f} USDT，无法继续执行")
                    break
                
                # 执行对冲交易
                spot_order = None
                contract_order = None
                hedge_success = False
                
                try:
                    spot_order, contract_order = await trader.execute_hedge_trade()
                    executed_count += 1
                    
                    # 验证订单成功
                    if (spot_order and contract_order and 
                        spot_order.get('status') in ['closed', 'filled'] and
                        trader.last_trade_spot_amount > 0 and 
                        trader.last_trade_contract_amount > 0):
                        
                        logger.info(f"第 {current_iteration}/{total_count} 次对冲交易成功完成!")
                        successful_count += 1
                        hedge_success = True
                    else:
                        logger.warning(f"第 {current_iteration}/{total_count} 次交易可能部分成功，但结果验证失败")
                        # 当前交易出现问题，不继续后续交易
                        break
                        
                except Exception as e:
                    logger.error(f"执行对冲交易时发生错误: {str(e)}")
                    if "不足" in str(e) or "insufficient" in str(e).lower():
                        logger.error("资金不足，停止后续交易")
                    else:
                        logger.debug(f"错误详情: {e}", exc_info=True)
                    
                    # 计入执行次数，但标记为失败
                    executed_count += 1
                    # 由于交易失败，不继续后续交易
                    break
                
                # 只有成功完成当前交易才继续下一次
                if hedge_success and current_iteration < total_count:
                    wait_time = 3  # 等待3秒
                    logger.debug(f"等待 {wait_time} 秒后开始下一次交易...")
                    await asyncio.sleep(wait_time)
                elif not hedge_success:
                    logger.warning("当前交易未成功完成，停止后续交易")
                    break
                
            except Exception as e:
                logger.error(f"第 {current_iteration}/{total_count} 次交易操作时出错: {str(e)}")
                logger.debug(f"错误详情: {e}", exc_info=True)  # 打印完整堆栈跟踪
                # 出现异常，停止后续交易
                break

    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        logger.debug(f"错误详情: {e}", exc_info=True)  # 打印完整堆栈跟踪
    finally:
        # 打印详细的执行统计
        logger.info(f"交易操作完成统计: 尝试执行 {executed_count}/{total_count} 次, 成功完成 {successful_count} 次")
        
        if executed_count > 0:
            success_rate = (successful_count / executed_count) * 100
            logger.info(f"交易成功率: {success_rate:.2f}%")
        
        # 打印累计差额信息，如果有交易记录
        if 'trader' in locals() and hasattr(trader, 'trade_records') and trader.trade_records:
            # 获取当前价格更新差额的价值
            current_price = 0
            try:
                orderbook = await trader.gateio.fetch_order_book(trader.symbol)
                current_price = float(orderbook['asks'][0][0])
                # 更新最新的USDT价值
                trader.cumulative_position_diff_usdt = trader.cumulative_position_diff * current_price
            except Exception as e:
                logger.warning(f"获取最新价格失败: {str(e)}")
            
            logger.info("=" * 50)
            logger.info("【仓位差额汇总】")
            logger.info(f"累计差额数量: {trader.cumulative_position_diff:.8f} {trader.base_currency}")
            logger.info(f"累计差额价值: {trader.cumulative_position_diff_usdt:.2f} USDT")
            
            # 解释差额方向
            if trader.cumulative_position_diff > 0:
                logger.info(f"差额方向: 合约多于现货 (正值)")
                logger.info(f"建议: 买入 {abs(trader.cumulative_position_diff):.8f} {trader.base_currency} 现货补足差额")
            elif trader.cumulative_position_diff < 0:
                logger.info(f"差额方向: 现货多于合约 (负值)")
                logger.info(f"建议: 开空 {abs(trader.cumulative_position_diff):.8f} {trader.base_currency} 合约补足差额")
            else:
                logger.info("差额方向: 平衡 (零)")
                
            # 显示平衡操作次数
            if trader.rebalance_count > 0:
                logger.info(f"执行平衡操作: {trader.rebalance_count} 次")
            
            # 计算交易执行统计
            if len(trader.trade_records) > 0:
                regular_trades = [r for r in trader.trade_records if not r.get('is_rebalance', False)]
                if regular_trades:
                    # 计算平均价差和滑点
                    spreads = []
                    volumes = []
                    for record in regular_trades:
                        # 只处理有完整价格信息的记录
                        if record.get('gateio_asks') and record.get('bybit_bids'):
                            gateio_ask = record['gateio_asks'][0][0] if record['gateio_asks'] else 0
                            bybit_bid = record['bybit_bids'][0][0] if record['bybit_bids'] else 0
                            if gateio_ask > 0 and bybit_bid > 0:
                                spread = (bybit_bid - gateio_ask) / gateio_ask * 100
                                spreads.append(spread)
                                volumes.append(record.get('spot_filled', 0))
                    
                    if spreads:
                        avg_spread = sum(spreads) / len(spreads)
                        if volumes:
                            volume_weighted_spread = sum(s * v for s, v in zip(spreads, volumes)) / sum(volumes)
                            logger.info(f"平均交易价差: {avg_spread:.4f}%")
                            logger.info(f"加权平均价差: {volume_weighted_spread:.4f}%")
                
            # 显示最近交易记录
            if len(trader.trade_records) > 0:
                logger.info("-" * 40)
                logger.info("最近交易记录:")
                for record in trader.trade_records[-min(3, len(trader.trade_records)):]:
                    if record.get('is_rebalance', False):
                        # 平衡操作记录
                        op_side = "买入现货" if record.get('side') == 'spot' else "开空合约"
                        logger.info(f"【平衡操作】{record.get('trade_id')} - {op_side}: {record.get('amount'):.8f} {trader.base_currency}, "
                                   f"价值: {record.get('cost', 0):.2f} USDT")
                        if record.get('avg_price', 0) > 0:
                            logger.info(f"  成交均价: {record.get('avg_price'):.8f} USDT/{trader.base_currency}")
                    else:
                        # 常规交易记录
                        spot = record.get('spot_filled', 0)
                        contract = record.get('contract_filled', 0)
                        diff = record.get('position_diff', 0)
                        timestamp = record.get('timestamp', 0)
                        if timestamp > 0:
                            import datetime
                            time_str = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            time_str = "未知时间"
                            
                        logger.info(f"【常规交易】#{record.get('trade_id')} ({time_str}) - 现货: {spot:.8f}, 合约: {contract:.8f}, "
                                   f"差额: {diff:.8f} {trader.base_currency} ({record.get('position_diff_usdt', 0):.2f} USDT)")
                        
                        # 显示市场状态
                        if record.get('gateio_asks') and record.get('bybit_bids'):
                            gateio_ask = record['gateio_asks'][0][0] if record['gateio_asks'] else 0
                            bybit_bid = record['bybit_bids'][0][0] if record['bybit_bids'] else 0
                            if gateio_ask > 0 and bybit_bid > 0:
                                price_diff = bybit_bid - gateio_ask
                                price_diff_percent = price_diff / gateio_ask * 100
                                logger.info(f"  执行时价差: Gate.io卖1 {gateio_ask:.8f} vs Bybit买1 {bybit_bid:.8f}, "
                                          f"差值: {price_diff:.8f} ({price_diff_percent:.4f}%)")
            
            # 显示是否需要平衡
            if abs(trader.cumulative_position_diff_usdt) >= 6:
                logger.info("-" * 40)
                balance_side = "买入现货" if trader.cumulative_position_diff > 0 else "开空合约"
                logger.info(f"建议: 需要{balance_side}来平衡仓位")
                logger.info(f"平衡数量: {abs(trader.cumulative_position_diff):.8f} {trader.base_currency}")
                logger.info(f"预计成本: {abs(trader.cumulative_position_diff_usdt):.2f} USDT")
            
            logger.info("=" * 50)
        
        # 确保关闭交易所连接
        if 'trader' in locals():
            try:
                await asyncio.gather(
                    trader.gateio.close(),
                    trader.bybit.close()
                )
                logger.debug("已关闭交易所连接")
            except Exception as e:
                logger.error(f"关闭交易所连接时出错: {str(e)}")

    # 返回退出码：只有当所有计划的交易都成功执行时，返回0
    return 0 if successful_count == total_count else 1


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 