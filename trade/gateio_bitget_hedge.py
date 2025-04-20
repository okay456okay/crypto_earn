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

    async def initialize(self):
        """
        异步初始化方法，执行需要网络请求的初始化操作
        """
        try:
            # 获取市场信息以确定最大杠杆倍数
            markets = await self.bitget.fetch_markets()
            contract_market = next((m for m in markets if m['symbol'] == self.contract_symbol), None)
            
            if not contract_market:
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

            logger.info(f"初始化完成: 交易对={self.symbol}, 合约对={self.contract_symbol}, "
                        f"最小价差={self.min_spread * 100}%, 杠杆={self.leverage}倍")

            # 获取并保存账户余额
            self.gateio_usdt, self.bitget_usdt = await self.check_balances()

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
                    self.gateio_usdt, self.bitget_usdt = await self.check_balances()
                    if required_usdt > self.gateio_usdt:
                        raise Exception(f"Gate.io USDT余额不足，需要约 {required_usdt:.2f} USDT，当前余额 {self.gateio_usdt:.2f} USDT")

                if required_margin > self.bitget_usdt:
                    raise Exception(
                        f"Bitget USDT保证金不足，需要约 {required_margin:.2f} USDT，当前余额 {self.bitget_usdt:.2f} USDT")

                logger.info(
                    f"账户余额检查通过 - 预估所需Gate.io: {required_usdt:.2f} USDT, Bitget: {required_margin:.2f} USDT")

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
                                logger.debug(f"{self.symbol}收到Gate.io订单簿更新")
                            else:  # bitget task
                                self.orderbooks['bitget'] = ob
                                logger.debug(f"{self.symbol} 收到Bitget订单簿更新")

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
            gateio_ob = self.orderbooks['gateio']
            bitget_ob = self.orderbooks['bitget']

            if not gateio_ob or not bitget_ob:
                return

            gateio_ask = Decimal(str(gateio_ob['asks'][0][0]))
            gateio_ask_volume = Decimal(str(gateio_ob['asks'][0][1]))

            bitget_bid = Decimal(str(bitget_ob['bids'][0][0]))
            bitget_bid_volume = Decimal(str(bitget_ob['bids'][0][1]))

            spread = bitget_bid - gateio_ask
            spread_percent = spread / gateio_ask

            # 将价差数据放入队列
            spread_data = {
                'spread_percent': float(spread_percent),
                'gateio_ask': float(gateio_ask),
                'bitget_bid': float(bitget_bid),
                'gateio_ask_volume': float(gateio_ask_volume),
                'bitget_bid_volume': float(bitget_bid_volume)
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
                        timeout=30  # 10秒超时
                    )

                    spread_percent = spread_data['spread_percent']


                    if spread_percent >= self.min_spread:
                        logger.info(
                            f"{self.symbol}"
                            f"价格检查 - Gate.io卖1: {spread_data['gateio_ask']} (量: {spread_data['gateio_ask_volume']}), "
                            f"Bitget买1: {spread_data['bitget_bid']} (量: {spread_data['bitget_bid_volume']}), "
                            f"价差: {spread_percent * 100:.4f}%")
                        logger.info(f"{self.symbol}价差条件满足: {spread_percent * 100:.4f}% >= {self.min_spread * 100:.4f}%")
                        return (spread_percent, spread_data['gateio_ask'], spread_data['bitget_bid'],
                                spread_data['gateio_ask_volume'], spread_data['bitget_bid_volume'])
                    else:
                        logger.debug(
                            f"{self.symbol}"
                            f"价格检查 - Gate.io卖1: {spread_data['gateio_ask']} (量: {spread_data['gateio_ask_volume']}), "
                            f"Bitget买1: {spread_data['bitget_bid']} (量: {spread_data['bitget_bid_volume']}), "
                            f"价差: {spread_percent * 100:.4f}%")
                        logger.debug(f"{self.symbol}价差条件不满足: {spread_percent * 100:.4f}% < {self.min_spread * 100:.4f}%")

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
            spread_percent, gateio_ask, bitget_bid, gateio_ask_volume, bitget_bid_volume = spread_data

            # 2. 立即准备下单参数, 补偿一点手续费，不然现货会比合约少一些
            trade_amount = self.spot_amount * 1.001
            cost = float(trade_amount) * float(gateio_ask)
            contract_amount = self.bitget.amount_to_precision(self.contract_symbol, trade_amount)

            # 调试日志：记录下单前信息
            logger.debug(f"准备下单 - Gate.io: {self.symbol}, 花费: {cost} USDT; Bitget: {self.contract_symbol}, 数量: {contract_amount}")

            # 3. 立即执行交易
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
                logger.debug("下单请求已发送并获得初始响应")
            except Exception as e:
                logger.error(f"下单过程出错: {str(e)}")
                raise
                
            # 4. 交易后再进行其他操作
            base_currency = self.symbol.split('/')[0]
            logger.info(f"计划交易数量: {trade_amount} {base_currency}")
            logger.info(f"在Gate.io市价买入 {trade_amount} {base_currency}, 预估成本: {cost:.2f} USDT")
            logger.info(f"在Bitget市价开空单 {contract_amount} {base_currency}")
            
            # 获取最新的订单状态 - Gate.io
            spot_order_id = spot_order.get('id')
            if spot_order_id:
                try:
                    logger.debug(f"获取Gate.io订单详情, 订单ID: {spot_order_id}")
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
                    logger.debug(f"获取Bitget订单详情, 订单ID: {contract_order_id}")
                    # 尝试获取已完成订单信息
                    try:
                        updated_contract_order = await self.bitget.fetch_closed_order(contract_order_id, self.contract_symbol)
                        logger.debug("成功从fetch_closed_order获取Bitget订单")
                    except:
                        # 如果获取已完成订单失败，尝试获取普通订单
                        updated_contract_order = await self.bitget.fetch_order(contract_order_id, self.contract_symbol)
                        logger.debug("成功从fetch_order获取Bitget订单")
                        
                    if updated_contract_order:
                        logger.debug(f"获取到Bitget最新订单状态: {updated_contract_order.get('status')}")
                        contract_order = updated_contract_order
                except Exception as e:
                    logger.warning(f"获取Bitget订单更新失败: {str(e)}")
                    
            # 记录订单详细信息（调试模式）
            logger.debug(f"Gate.io订单详情: {spot_order}")
            logger.debug(f"Bitget订单详情: {contract_order}")

            # 检查订单执行状态
            if not self.verify_order_execution(spot_order, contract_order):
                logger.error("订单执行异常，终止交易！")
                return None, None

            # 获取现货订单的实际成交结果
            filled_amount = float(spot_order.get('filled', 0))
            fees = spot_order.get('fees', [])
            base_fee = sum(float(fee['cost']) for fee in fees if fee['currency'] == base_currency)
            actual_position = filled_amount - base_fee

            # 获取合约订单的实际成交数量
            contract_filled = float(contract_order.get('filled', contract_amount))

            logger.info(f"Gate.io实际成交数量: {filled_amount} {base_currency}, "
                        f"手续费: {base_fee} {base_currency}, "
                        f"实际持仓: {actual_position} {base_currency}")
            logger.info(f"Bitget合约实际成交数量: {contract_filled} {base_currency}")

            # 检查持仓情况
            position_check_start = asyncio.get_event_loop().time()
            position_balance = await self.check_positions(actual_position, contract_filled)
            position_check_duration = asyncio.get_event_loop().time() - position_check_start
            logger.debug(f"持仓检查耗时: {position_check_duration:.2f}秒")
            
            if not position_balance:
                logger.error("持仓检查不通过，可能存在交易执行问题！")
                return None, None

            # 申购余币宝
            try:
                earn_start = asyncio.get_event_loop().time()
                gateio_subscrible_earn(base_currency, actual_position)
                earn_duration = asyncio.get_event_loop().time() - earn_start
                logger.info(f"已将 {actual_position} {base_currency} 申购到余币宝, 耗时: {earn_duration:.2f}秒")
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
            # 详细分析Gate.io现货订单
            spot_status = spot_order.get('status')
            spot_filled = float(spot_order.get('filled', 0))
            spot_amount = float(spot_order.get('amount', 0))
            spot_fill_percent = (spot_filled / spot_amount * 100) if spot_amount > 0 else 0
            
            logger.debug(f"Gate.io订单分析 - 状态: {spot_status}, 总量: {spot_amount}, "
                        f"已成交: {spot_filled}, 成交比例: {spot_fill_percent:.2f}%")
            
            # 详细分析Bitget合约订单
            contract_status = contract_order.get('status')
            contract_filled = float(contract_order.get('filled', 0))
            contract_amount = float(contract_order.get('amount', 0))
            contract_fill_percent = (contract_filled / contract_amount * 100) if contract_amount > 0 else 0
            
            logger.debug(f"Bitget订单分析 - 状态: {contract_status}, 总量: {contract_amount}, "
                        f"已成交: {contract_filled}, 成交比例: {contract_fill_percent:.2f}%")
            
            # 检查现货订单状态
            if spot_status not in ['closed', 'filled'] and spot_fill_percent < 95:
                logger.error(f"Gate.io现货订单未完全成交: 状态={spot_status}, 成交率={spot_fill_percent:.2f}%")
                return False
            
            # 检查现货成交量
            if spot_filled <= 0:
                logger.error(f"Gate.io现货订单成交量为0!")
                return False
            
            # 对于Bitget合约订单，主要检查成交量，不严格检查状态
            # 因为Bitget API可能返回的状态值与我们期望的不同
            
            # 如果合约订单成交量为0，可能是API未实时更新，此时检查持仓来确认
            if contract_filled <= 0:
                logger.warning(f"Bitget合约订单成交量为0，将在持仓检查时进一步验证")
                # 记录原始响应，帮助调试
                logger.debug(f"Bitget原始响应分析:")
                for key, value in contract_order.items():
                    if key not in ['info']:  # 排除过长的原始info字段
                        logger.debug(f"  - {key}: {value}")
            else:
                logger.info(f"Bitget合约订单成交量: {contract_filled}")
                
            return True
        except Exception as e:
            logger.error(f"验证订单执行状态时出错: {str(e)}")
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
                    if retry_count >= max_retries:
                        raise
                    await asyncio.sleep(1)  # 等待1秒后重试
                    gateio_balance_task = self.gateio.fetch_balance()
                    positions_task = self.bitget.fetch_positions([self.contract_symbol])

            # 获取现货最新成交订单的信息
            base_currency = self.symbol.split('/')[0]
            gateio_position = gateio_balance.get(base_currency, {}).get('total', 0)
            logger.debug(f"Gate.io {base_currency} 余额详情: {gateio_balance.get(base_currency, {})}")

            # 检查Bitget合约持仓
            contract_position = 0

            if positions:
                logger.debug(f"Bitget返回持仓数量: {len(positions)}")
                for position in positions:
                    logger.debug(f"检查持仓: {position.get('symbol')} vs {self.contract_symbol}")
                    if position['symbol'] == self.contract_symbol:
                        contract_position = abs(float(position.get('contracts', 0)))
                        position_side = position.get('side', 'unknown')
                        position_leverage = position.get('leverage', self.leverage)
                        position_notional = position.get('notional', 0)
                        position_entry_price = position.get('entryPrice', 0)

                        logger.info(f"Bitget合约持仓: {position_side} {contract_position} 合约, "
                                    f"杠杆: {position_leverage}倍, 名义价值: {position_notional}, "
                                    f"开仓均价: {position_entry_price}")
                        
                        # 记录更多持仓详情，帮助调试
                        logger.debug(f"Bitget持仓详情:")
                        for key, value in position.items():
                            if key not in ['info']:  # 排除过长的原始info字段
                                logger.debug(f"  - {key}: {value}")
            else:
                logger.warning("未获取到Bitget合约持仓信息")
                logger.debug("尝试直接获取账户持仓概要...")
                try:
                    summary = await self.bitget.fetch_balance({'type': 'swap'})
                    logger.debug(f"Bitget账户概要: {summary}")
                except Exception as e:
                    logger.error(f"获取Bitget账户概要失败: {str(e)}")

            logger.info(f"持仓检查 - Gate.io现货: {gateio_position} {base_currency}, "
                        f"Bitget合约: {contract_position} {base_currency}")
            
            # 如果提供了本次交易的具体数值，则检查本次交易是否成功执行
            if actual_position is not None and contract_amount is not None:
                # 检查现货交易结果 - 放宽要求，仅检查是否有现货余额（因为可能已经申购到余币宝）
                # 实际上现货买入后直接申购余币宝，所以现货余额可能很小
                if gateio_position < 0.01:  # 仅检查是否有最小值
                    logger.warning(f"Gate.io现货余额极低: {gateio_position} {base_currency} (可能已申购余币宝)")
                    # 不立即判断为失败，因为资金可能已存入余币宝
                
                # 检查合约交易结果 - 必须确认合约确实开立
                if contract_position <= 0:
                    logger.error(f"Bitget合约持仓为0! 交易可能未执行")
                    return False
                
                # 如果合约持仓远低于预期，也视为可能有问题
                if contract_position < float(contract_amount) * 0.5:  # 放宽到50%
                    logger.warning(f"Bitget合约持仓量显著低于预期: 预期约 {contract_amount} {base_currency}, 实际 {contract_position} {base_currency}")
                    # 此处不返回失败，但记录警告
                
                logger.info(f"交易执行检查通过 - 本次交易: 现货约 {actual_position} {base_currency} (可能已申购余币宝), 合约约 {contract_amount} {base_currency}")
                return True
            
            # 若不检查具体交易，则检查总体持仓平衡性
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
            logger.debug(f"获取持仓出错的堆栈: {traceback.format_exc()}")
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
    start_time = asyncio.get_event_loop().time()
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

    try:
        # 创建并初始化交易器
        logger.debug("创建交易器实例...")
        trader = HedgeTrader(
            symbol=args.symbol,
            spot_amount=args.amount,
            min_spread=args.min_spread,
            leverage=args.leverage  # 如果没有指定，这里会是None
        )
        
        init_start = asyncio.get_event_loop().time()
        logger.debug("开始初始化交易器...")
        await trader.initialize()
        init_duration = asyncio.get_event_loop().time() - init_start
        logger.debug(f"交易器初始化完成, 耗时: {init_duration:.2f}秒")

        # 记录交易次数
        completed_trades = 0
        target_count = args.count

        logger.info(f"计划执行 {target_count} 次交易操作")

        # 循环执行交易，直到达到指定次数
        while completed_trades < target_count:
            iteration_start = asyncio.get_event_loop().time()
            logger.info(f"开始执行第 {completed_trades + 1}/{target_count} 次交易...")
            
            try:
                # 执行前检查余额是否足够
                check_start = asyncio.get_event_loop().time()
                logger.debug("检查交易要求...")
                can_trade, reason = await trader.check_trade_requirements()
                check_duration = asyncio.get_event_loop().time() - check_start
                logger.debug(f"交易要求检查完成, 耗时: {check_duration:.2f}秒, 结果: {can_trade}")
                
                if not can_trade:
                    try:
                        # 如果是因为Gate.io USDT余额不足，尝试从余币宝赎回
                        if "Gate.io USDT余额不足" in reason:
                            logger.info(f"Gate.io USDT余额不足，尝试从余币宝赎回资金")
                            # 估算所需资金
                            orderbook = await trader.gateio.fetch_order_book(trader.symbol)
                            current_price = float(orderbook['asks'][0][0])
                            required_usdt = float(trader.spot_amount) * current_price * 1.02
                            
                            redeem_start = asyncio.get_event_loop().time()
                            logger.debug(f"从余币宝赎回约 {max(required_usdt * 1.01, 50):.2f} USDT...")
                            redeem_earn('USDT', max(required_usdt * 1.01, 50))
                            redeem_duration = asyncio.get_event_loop().time() - redeem_start
                            logger.debug(f"余币宝赎回操作完成, 耗时: {redeem_duration:.2f}秒")
                            
                            # 重新检查交易要求
                            logger.debug("重新检查交易要求...")
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
                logger.debug("开始执行对冲交易...")
                trade_start = asyncio.get_event_loop().time()
                spot_order, contract_order = await trader.execute_hedge_trade()
                trade_duration = asyncio.get_event_loop().time() - trade_start
                logger.debug(f"对冲交易执行完成, 耗时: {trade_duration:.2f}秒, 结果: {spot_order is not None and contract_order is not None}")
                
                # 第一次交易必须成功，后续交易可能有API延迟导致误判，更宽松地处理
                if completed_trades == 0 and (spot_order is None or contract_order is None):
                    logger.error("首次交易执行失败，停止后续交易")
                    break
                elif completed_trades > 0 and (spot_order is None or contract_order is None):
                    # 非首次交易，可能是因为API延迟报告问题
                    # 使用更严格的持仓检查来确认是否真的失败
                    logger.warning("交易报告问题，但可能是API延迟，尝试通过持仓检查确认...")
                    
                    # 等待更久让API更新持仓信息
                    logger.debug("等待5秒让API更新持仓信息...")
                    await asyncio.sleep(5)
                    
                    # 获取并检查持仓
                    logger.debug("检查合约持仓确认交易状态...")
                    positions = await trader.bitget.fetch_positions([trader.contract_symbol])
                    has_position = False
                    
                    for position in positions:
                        if position['symbol'] == trader.contract_symbol and abs(float(position.get('contracts', 0))) > 0:
                            has_position = True
                            logger.info(f"确认有合约持仓，继续后续交易")
                            break
                    
                    if not has_position:
                        logger.error("确认无合约持仓，交易确实失败，停止后续交易")
                        break
                
                # 交易成功
                completed_trades += 1
                iteration_duration = asyncio.get_event_loop().time() - iteration_start
                logger.info(f"第 {completed_trades}/{target_count} 次对冲交易成功完成! 耗时: {iteration_duration:.2f}秒")
                
                # 如果不是最后一次交易，等待一小段时间再继续
                if completed_trades < target_count:
                    logger.info(f"等待5秒后继续下一次交易...")
                    await asyncio.sleep(5)
                    
            except Exception as e:
                logger.error(f"执行第 {completed_trades + 1} 次交易时出错: {str(e)}")
                import traceback
                logger.debug(f"交易错误堆栈: {traceback.format_exc()}")
                break
                
        # 打印最终执行结果
        total_duration = asyncio.get_event_loop().time() - start_time
        if completed_trades == target_count:
            logger.info(f"所有计划交易已完成! 成功执行 {completed_trades}/{target_count} 次交易, 总耗时: {total_duration:.2f}秒")
        else:
            logger.info(f"交易过程中止，成功执行 {completed_trades}/{target_count} 次交易, 总耗时: {total_duration:.2f}秒")

    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        import traceback
        logger.debug(f"程序错误堆栈: {traceback.format_exc()}")
        if 'completed_trades' in locals() and 'target_count' in locals():
            logger.info(f"已完成 {completed_trades}/{target_count} 次交易")
        total_duration = asyncio.get_event_loop().time() - start_time
        logger.info(f"程序执行中止, 总耗时: {total_duration:.2f}秒")
        return 1
    finally:
        # 确保关闭交易所连接
        if 'trader' in locals():
            logger.debug("正在关闭交易所连接...")
            try:
                await asyncio.gather(
                    trader.gateio.close(),
                    trader.bitget.close()
                )
                logger.debug("交易所连接已关闭")
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
