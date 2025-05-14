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
6. 使用优化版策略执行，减少交易延迟

优化特点：
- 直接订阅订单簿并实时处理，不使用队列传递数据
- 满足条件时立即下单，减少操作延时
- 忽略不满足条件的价格更新，降低处理开销
- 数据新鲜度检查，确保基于最新市场数据交易
"""

import sys
import os
import argparse
from decimal import Decimal
import asyncio

import ccxt.base.errors
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
        self.depth_multiplier = 10  # 默认深度乘数

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

    async def execute_hedge_trade_optimized(self):
        """
        优化版对冲交易执行函数 - 直接订阅订单簿，检查价差，满足条件立即执行交易
        不使用队列传递数据，减少延迟
        """
        spot_order = None
        contract_order = None
        
        try:
            logger.debug("开始执行优化版对冲交易流程")
            
            # 记录交易监控变量
            update_count = 0
            last_log_time = time.time()
            
            # 创建一个字段用于控制WebSocket连接
            ws_running = True
            
            while ws_running:
                try:
                    # 并行订阅两个交易所的订单簿
                    tasks = [
                        asyncio.create_task(self.gateio.watch_order_book(self.symbol)),
                        asyncio.create_task(self.bitget.watch_order_book(self.contract_symbol))
                    ]
                    
                    # 等待任意一个订单簿更新
                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                        timeout=30  # 添加超时，防止无限等待
                    )
                    
                    # 如果超时，两个任务都未完成
                    if not done:
                        logger.warning("等待订单簿更新超时，重新订阅")
                        # 取消所有进行中的任务
                        for task in pending:
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass
                        continue
                    
                    # 至少有一个任务完成，获取最新数据
                    for task in done:
                        try:
                            ob = task.result()
                            if task == tasks[0]:  # gateio task
                                self.orderbooks['gateio'] = ob
                                logger.debug("收到Gate.io订单簿更新")
                            else:  # bitget task
                                self.orderbooks['bitget'] = ob
                                logger.debug("收到Bitget订单簿更新")
                        except Exception as e:
                            logger.error(f"处理订单簿数据时出错: {str(e)}", exc_info=True)
                            continue

                    # 取消未完成的任务
                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                    
                    # 检查是否有足够的数据进行价差分析
                    if not self.orderbooks['gateio'] or not self.orderbooks['bitget']:
                        logger.debug("订单簿数据不完整，等待下一次更新")
                        continue
                    
                    # 记录订单簿数据时间戳
                    gateio_timestamp = self.orderbooks['gateio'].get('timestamp', 0)
                    bitget_timestamp = self.orderbooks['bitget'].get('timestamp', 0)
                    now = int(time.time() * 1000)  # 当前时间戳（毫秒）
                    
                    gateio_age = (now - gateio_timestamp) / 1000 if gateio_timestamp else -1  # 秒
                    bitget_age = (now - bitget_timestamp) / 1000 if bitget_timestamp else -1  # 秒
                    
                    # 检查数据新鲜度，超过3秒的数据可能已经过时
                    if gateio_age > 3 or bitget_age > 3:
                        logger.debug(f"订单簿数据可能已过时 - Gate.io: {gateio_age:.1f}秒, Bitget: {bitget_age:.1f}秒")
                        continue
                    
                    # 提取价格数据
                    gateio_ob = self.orderbooks['gateio']
                    bitget_ob = self.orderbooks['bitget']
                    
                    gateio_ask = Decimal(str(gateio_ob['asks'][0][0]))
                    gateio_ask_volume = Decimal(str(gateio_ob['asks'][0][1]))
                    gateio_bid = Decimal(str(gateio_ob['bids'][0][0])) if gateio_ob['bids'] else Decimal('0')
                    gateio_bid_volume = Decimal(str(gateio_ob['bids'][0][1])) if gateio_ob['bids'] else Decimal('0')
                    
                    bitget_bid = Decimal(str(bitget_ob['bids'][0][0]))
                    bitget_bid_volume = Decimal(str(bitget_ob['bids'][0][1]))
                    bitget_ask = Decimal(str(bitget_ob['asks'][0][0])) if bitget_ob['asks'] else Decimal('0')
                    bitget_ask_volume = Decimal(str(bitget_ob['asks'][0][1])) if bitget_ob['asks'] else Decimal('0')
                    
                    # 计算价差 - bitget买一（卖出）- gateio卖一（买入）
                    spread = bitget_bid - gateio_ask
                    spread_percent = spread / gateio_ask
                    
                    # 统计更新次数
                    update_count += 1
                    current_time = time.time()
                    
                    # 每10次更新或者至少间隔5秒记录一次详细日志
                    if update_count % 10 == 0 or (current_time - last_log_time) >= 5:
                        logger.debug(f"价格检查 - Gate.io卖1: {float(gateio_ask):.8f}(量:{float(gateio_ask_volume):.8f}) vs Bitget买1: {float(bitget_bid):.8f}(量:{float(bitget_bid_volume):.8f}), "
                                   f"价差: {float(spread_percent) * 100:.4f}%, 最小要求: {self.min_spread * 100:.4f}%, "
                                   f"要求深度: {required_depth:.8f}")
                        last_log_time = current_time
                    
                    # 检查价差是否有效（防止异常价格导致错误决策）
                    if abs(float(spread_percent)) > 0.1:  # 超过10%的价差可能是异常数据
                        logger.warning(f"检测到异常价差: {float(spread_percent) * 100:.4f}%，可能是订单簿数据异常")
                        continue
                    
                    # 检查价差和数量条件
                    required_depth = self.spot_amount * self.depth_multiplier
                    if spread_percent >= self.min_spread and gateio_ask_volume >= required_depth and bitget_bid_volume >= required_depth:
                        logger.info(f"【价差条件满足】价差: {float(spread_percent) * 100:.4f}% >= {self.min_spread * 100:.4f}%")
                        logger.info(f"【市场行情】Gate.io - 买1: {float(gateio_bid):.8f}(量:{float(gateio_bid_volume):.8f}), 卖1: {float(gateio_ask):.8f}(量:{float(gateio_ask_volume):.8f})")
                        logger.info(f"【市场行情】Bitget - 买1: {float(bitget_bid):.8f}(量:{float(bitget_bid_volume):.8f}), 卖1: {float(bitget_ask):.8f}(量:{float(bitget_ask_volume):.8f})")
                        logger.info(f"【深度检查】要求深度: {required_depth}, Gate.io卖1深度: {float(gateio_ask_volume):.8f}, Bitget买1深度: {float(bitget_bid_volume):.8f}")
                        
                        # 立即准备下单参数
                        trade_amount = self.spot_amount
                        cost = float(trade_amount) * float(gateio_ask)
                        contract_amount = self.bitget.amount_to_precision(self.contract_symbol, trade_amount)

                        logger.debug(f"准备下单 - Gate.io: {self.symbol}，花费: {cost} USDT; Bitget: {self.contract_symbol}，数量: {contract_amount}")

                        # 停止WebSocket循环，进入交易执行
                        ws_running = False
                        
                        # 立即执行交易
                        try:
                            # 发送订单请求
                            spot_order_request = {
                                'symbol': self.symbol,
                                'amount': cost,
                                'params': {'createMarketBuyOrderRequiresPrice': False, 'quoteOrderQty': True}
                            }
                            
                            contract_order_request = {
                                'symbol': self.contract_symbol,
                                'amount': contract_amount,
                                'params': {"reduceOnly": False}
                            }
                            
                            logger.debug(f"准备发送现货订单: {spot_order_request}")
                            logger.debug(f"准备发送合约订单: {contract_order_request}")
                            
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
                            
                            # 详细打印订单信息到调试日志
                            logger.debug(f"Gate.io现货订单提交详情: {spot_order}")
                            logger.debug(f"Bitget合约订单提交详情: {contract_order}")
                            
                            # 检查订单是否成功提交
                            if not spot_order or 'id' not in spot_order:
                                error_msg = f"Gate.io现货订单提交失败: {spot_order}"
                                logger.error(error_msg)
                                return None, None
                                
                            if not contract_order or 'id' not in contract_order:
                                error_msg = f"Bitget合约订单提交失败: {contract_order}"
                                logger.error(error_msg)
                                return None, None
                                
                            logger.info(f"订单提交成功 - Gate.io订单ID: {spot_order.get('id')}, Bitget订单ID: {contract_order.get('id')}")
                        except Exception as e:
                            logger.error(f"下单过程出错: {str(e)}")
                            # 打印完整异常跟踪信息
                            import traceback
                            logger.error(f"下单异常详细信息: {traceback.format_exc()}")
                            # 失败直接返回，不继续执行
                            return None, None

                        # 交易后操作
                        base_currency = self.symbol.split('/')[0]
                        logger.info(f"在Gate.io市价买入 {trade_amount} {base_currency}，在Bitget市价开空单 {contract_amount} {base_currency}")
                        
                        # 等待一段时间，确保订单状态已更新
                        await asyncio.sleep(2)
                        
                        # 获取最新的订单状态 - Gate.io
                        spot_order_id = spot_order.get('id')
                        spot_order_filled = False
                        if spot_order_id:
                            try:
                                # 尝试获取已完成订单
                                updated_spot_order = None
                                
                                try:
                                    # closed_orders = await self.gateio.fetch_closed_orders(self.symbol, since=int(time.time() * 1000) - 60000)
                                    #
                                    # for order in closed_orders:
                                    #     if order.get('id') == spot_order_id:
                                    #         updated_spot_order = order
                                    #         break
                                    #
                                    # if not updated_spot_order:
                                    updated_spot_order = await self.gateio.fetch_order(spot_order_id, self.symbol)
                                except Exception as e:
                                    logger.error(f"获取已完成订单失败: {str(e)}", exc_info=True)
                                    return None, None

                                if updated_spot_order:
                                    logger.debug(f"获取到Gate.io订单执行详情: {updated_spot_order}")
                                    spot_order = updated_spot_order
                                    
                                    # 检查订单状态和成交量
                                    spot_status = updated_spot_order.get('status')
                                    spot_filled = float(updated_spot_order.get('filled', 0))
                                    
                                    if spot_filled > 0 and spot_status in ['closed', 'fulfilled', 'filled']:
                                        spot_order_filled = True
                                        logger.info(f"Gate.io现货订单已成交: 状态={spot_status}, 成交量={spot_filled}")
                                    else:
                                        logger.error(f"Gate.io现货订单未完全成交: 状态={spot_status}, 成交量={spot_filled}, 订单详情={updated_spot_order}")
                                        # 如果订单状态不对，立即终止交易
                                        if spot_filled <= 0:
                                            return None, None
                            except Exception as e:
                                logger.error(f"获取Gate.io订单更新失败: {str(e)}")
                                import traceback
                                logger.error(f"获取订单状态异常详情: {traceback.format_exc()}")
                                return None, None
                        else:
                            logger.error("Gate.io订单ID无效，无法检查订单状态")
                            return None, None
                        
                        # 获取最新的订单状态 - Bitget
                        contract_order_id = contract_order.get('id')
                        contract_order_filled = False
                        if contract_order_id:
                            try:
                                updated_contract_order = await self.bitget.fetch_order(contract_order_id, self.contract_symbol)
                                
                                if updated_contract_order:
                                    logger.debug(f"获取到Bitget订单执行详情: {updated_contract_order}")
                                    contract_order = updated_contract_order
                                    
                                    # 检查订单状态和成交量
                                    contract_status = updated_contract_order.get('status')
                                    contract_filled = float(updated_contract_order.get('filled', 0))
                                    
                                    if contract_filled > 0 and contract_status in ['closed', 'fulfilled', 'filled']:
                                        contract_order_filled = True
                                        logger.info(f"Bitget合约订单已成交: 状态={contract_status}, 成交量={contract_filled}")
                                    else:
                                        logger.error(f"Bitget合约订单未完全成交: 状态={contract_status}, 成交量={contract_filled}, 订单详情={updated_contract_order}")
                                        # 如果订单状态不对，立即终止交易
                                        if contract_filled <= 0:
                                            return None, None
                            except Exception as e:
                                logger.error(f"获取Bitget订单更新失败: {str(e)}")
                                import traceback
                                logger.error(f"获取订单状态异常详情: {traceback.format_exc()}")
                                return None, None
                        else:
                            logger.error("Bitget订单ID无效，无法检查订单状态")
                            return None, None

                        # 提取订单执行结果
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
                        
                        # 记录详细的成交信息
                        logger.info("=" * 50)
                        logger.info(f"【成交详情】订单执行情况:")
                        logger.info(f"【成交详情】Gate.io卖1价格: {float(gateio_ask):.8f}, 实际成交价格: {spot_avg_price:.8f}, 价差: {(spot_avg_price - float(gateio_ask)):.8f} ({(spot_avg_price - float(gateio_ask)) / float(gateio_ask) * 100:.4f}%)")
                        logger.info(f"【成交详情】Bitget买1价格: {float(bitget_bid):.8f}, 实际成交价格: {contract_avg_price:.8f}, 价差: {(contract_avg_price - float(bitget_bid)):.8f} ({(contract_avg_price - float(bitget_bid)) / float(bitget_bid) * 100:.4f}%)")
                        logger.info(f"【成交详情】Gate.io实际成交: {filled_amount} {base_currency}, 手续费: {base_fee} {base_currency}, 实际持仓: {actual_position} {base_currency}")
                        logger.info(f"【成交详情】Bitget合约实际成交: {contract_filled} {base_currency}")
                        logger.info("=" * 50)

                        # 检查现货和合约成交量是否接近
                        if contract_filled <= 0:
                            logger.error("合约订单未成交，终止交易！")
                            return None, None

                        # 计算现货和合约成交量的差异百分比
                        amount_diff = abs(actual_position - contract_filled)
                        amount_diff_percent = (amount_diff / actual_position * 100) if actual_position > 0 else 0
                        
                        # 记录差异，但不因为差异终止交易（因为现货会立即申购余币宝）
                        if amount_diff_percent > 5:  # 差异超过5%时记录警告
                            logger.warning(f"现货和合约成交量差异较大: {amount_diff_percent:.2f}%, 现货: {actual_position} {base_currency}, 合约: {contract_filled} {base_currency}")
                        elif amount_diff_percent > 2:  # 差异在2%-5%之间记录提示
                            logger.info(f"现货和合约成交量差异在可接受范围: {amount_diff_percent:.2f}%, 现货: {actual_position} {base_currency}, 合约: {contract_filled} {base_currency}")
                        else:  # 差异在2%以内，认为是匹配的
                            logger.info(f"现货和合约成交量匹配良好: {amount_diff_percent:.2f}%, 现货: {actual_position} {base_currency}, 合约: {contract_filled} {base_currency}")
                        
                        # 记录交易执行成功
                        logger.info(f"交易执行成功 - 现货: {actual_position} {base_currency}（将申购余币宝）, 合约: {contract_filled} {base_currency}")

                        # 计算并记录本次交易的差额
                        position_diff = contract_filled - actual_position  # 正值表示合约多，负值表示现货多
                        self.cumulative_position_diff += position_diff
                        self.cumulative_position_diff_usdt = abs(self.cumulative_position_diff * float(gateio_ask))
                        
                        # 增加交易记录
                        self.trade_count += 1
                        trade_record = {
                            'trade_id': self.trade_count,
                            'timestamp': int(time.time()),
                            'spot_filled': actual_position,
                            'contract_filled': contract_filled,
                            'position_diff': position_diff,
                            'position_diff_usdt': position_diff * float(gateio_ask),
                            'price': float(gateio_ask),
                            'spot_price': float(spot_order.get('average', 0)) or float(spot_order.get('price', 0)),
                            'contract_price': float(contract_order.get('average', 0)) or float(contract_order.get('price', 0)),
                            'price_diff': float(contract_order.get('average', 0)) - float(spot_order.get('average', 0)),
                            'price_diff_percent': (float(contract_order.get('average', 0)) - float(spot_order.get('average', 0))) / float(spot_order.get('average', 0)) * 100 if float(spot_order.get('average', 0)) > 0 else 0,
                            'cumulative_diff': self.cumulative_position_diff,
                            'cumulative_diff_usdt': self.cumulative_position_diff_usdt
                        }
                        self.trade_records.append(trade_record)
                        
                        # 记录交易差额和累计差额
                        logger.info(f"【交易差额】- 现货: {actual_position} {base_currency}, 合约: {contract_filled} {base_currency}, "
                                  f"单次差额: {position_diff:.8f} {base_currency} ({position_diff * float(gateio_ask):.2f} USDT), "
                                  f"累计差额: {self.cumulative_position_diff:.8f} {base_currency} ({self.cumulative_position_diff_usdt:.2f} USDT)")

                        # 申购余币宝
                        try:
                            earn_result = gateio_subscrible_earn(base_currency, actual_position)
                            logger.info(f"已将 {actual_position} {base_currency} 申购到余币宝")
                        except Exception as e:
                            logger.error(f"余币宝申购失败，但不影响主要交易流程: {str(e)}")
                            
                        # 主要交易完成后，检查是否需要执行平衡操作
                        if self.cumulative_position_diff_usdt >= 6:
                            await self.execute_balance_operation(base_currency, float(gateio_ask))

                        # 直接返回订单信息，避免走finally语句块的None返回
                        return spot_order, contract_order
                    
                except asyncio.CancelledError:
                    # 任务被取消，可能是因为超时或者其他原因，不算作错误
                    logger.info("订单簿订阅任务被取消")
                    continue
                except Exception as e:
                    logger.error(f"订阅或处理订单簿时出错: {str(e)}", exc_info=True)
                    return None, None

        except Exception as e:
            logger.error(f"执行对冲交易时出错: {str(e)}")
            import traceback
            logger.error(f"错误堆栈: {traceback.format_exc()}")
            return None, None
        finally:
            # 确保所有WebSocket连接都被关闭
            try:
                await asyncio.gather(
                    self.gateio.close(),
                    self.bitget.close()
                )
            except Exception as e:
                logger.error(f"关闭WebSocket连接时出错: {str(e)}")
            
            # 只有在没有成功返回订单时才返回None, None
            if spot_order is None or contract_order is None:
                logger.debug("交易未执行成功，返回None, None")
                return None, None
            
            # 如果已经有有效的订单，返回这些订单而不是None
            logger.debug(f"交易执行成功，返回订单 - Gate.io: {spot_order.get('id')}, Bitget: {contract_order.get('id')}")
            return spot_order, contract_order

    async def execute_balance_operation(self, base_currency, current_price):
        """执行平衡操作"""
        try:
            if self.cumulative_position_diff > 0:  # 合约多于现货，需要买入现货
                rebalance_amount = abs(self.cumulative_position_diff)
                logger.info("=" * 50)
                logger.info(f"【平衡操作】检测到累计差额: {self.cumulative_position_diff:.8f} {base_currency} "
                           f"({self.cumulative_position_diff_usdt:.2f} USDT)")
                logger.info(f"【平衡操作】将买入 {rebalance_amount:.8f} {base_currency} 现货")
                
                try:
                    # 计算买入现货所需的USDT (加1%作为滑点和手续费的缓冲)
                    cost = float(rebalance_amount) * current_price * 1.01
                    
                    # 记录订单请求详情
                    logger.debug(f"【平衡操作】发送现货买入订单: 交易对={self.symbol}, 金额={cost} USDT")
                    
                    # 执行市价买入
                    rebalance_order = await self.gateio.create_market_buy_order(
                        symbol=self.symbol,
                        amount=cost,
                        params={'createMarketBuyOrderRequiresPrice': False, 'quoteOrderQty': True}
                    )
                    
                    # 记录订单响应详情
                    logger.debug(f"【平衡操作】现货买入订单响应: {rebalance_order}")
                    
                    # 检查订单是否成功提交
                    if not rebalance_order or 'id' not in rebalance_order:
                        logger.error(f"【平衡操作】现货买入订单提交失败: {rebalance_order}")
                        return
                    
                    logger.info(f"【平衡操作】订单已提交 - ID: {rebalance_order.get('id')}")
                    
                    # 等待订单状态更新
                    await asyncio.sleep(1.5)
                    
                    # 获取最新订单状态
                    try:
                        updated_order = await self.gateio.fetch_order(rebalance_order.get('id'), self.symbol)
                        logger.debug(f"【平衡操作】更新后的订单状态: {updated_order}")
                        
                        if updated_order and updated_order.get('status') in ['closed', 'filled']:
                            rebalance_order = updated_order
                        else:
                            logger.warning(f"【平衡操作】订单状态未完成: {updated_order.get('status')}")
                    except Exception as e:
                        logger.warning(f"【平衡操作】获取订单更新失败: {str(e)}")
                    
                    # 获取实际成交量
                    filled_amount = float(rebalance_order.get('filled', 0))
                    fees = rebalance_order.get('fees', [])
                    base_fee = sum(float(fee.get('cost', 0)) for fee in fees if fee.get('currency') == base_currency)
                    actual_filled = filled_amount - base_fee
                    
                    if filled_amount <= 0:
                        logger.error(f"【平衡操作】订单未成交，退出平衡操作")
                        return
                    
                    # 更新累计差额
                    self.cumulative_position_diff -= actual_filled
                    self.cumulative_position_diff_usdt = abs(self.cumulative_position_diff * current_price)
                    
                    # 记录平衡操作
                    self.rebalance_count += 1
                    
                    # 申购余币宝
                    try:
                        if actual_filled > 0:
                            logger.debug(f"【平衡操作】申购余币宝: {base_currency}, 数量={actual_filled}")
                            earn_result = gateio_subscrible_earn(base_currency, actual_filled)
                            logger.info(f"【平衡操作】已将 {actual_filled} {base_currency} 申购到余币宝, 结果: {earn_result}")
                    except Exception as e:
                        logger.error(f"【平衡操作】余币宝申购失败: {str(e)}")
                        import traceback
                        logger.debug(f"【平衡操作】余币宝申购错误详情: {traceback.format_exc()}")
                    
                    # 记录平衡操作结果
                    logger.info(f"【平衡操作】成功买入 {actual_filled} {base_currency} 现货，累计差额更新为 "
                              f"{self.cumulative_position_diff:.8f} {base_currency} ({self.cumulative_position_diff_usdt:.2f} USDT)")
                    logger.info("=" * 50)
                    
                except Exception as e:
                    logger.error(f"【平衡操作】买入现货失败: {str(e)}")
                    import traceback
                    logger.debug(f"【平衡操作】买入现货错误详情: {traceback.format_exc()}")
            
            elif self.cumulative_position_diff < 0:  # 现货多于合约，需要增加合约空单
                rebalance_amount = abs(self.cumulative_position_diff)
                logger.info("=" * 50)
                logger.info(f"【平衡操作】检测到累计差额: {self.cumulative_position_diff:.8f} {base_currency} "
                           f"({self.cumulative_position_diff_usdt:.2f} USDT)")
                logger.info(f"【平衡操作】将开立 {rebalance_amount:.8f} {base_currency} 合约空单")
                
                try:
                    # 精确化合约数量
                    contract_amount = self.bitget.amount_to_precision(self.contract_symbol, rebalance_amount)
                    
                    # 记录订单请求详情
                    logger.debug(f"【平衡操作】发送合约空单订单: 交易对={self.contract_symbol}, 数量={contract_amount}")
                    
                    # 执行合约空单
                    rebalance_order = await self.bitget.create_market_sell_order(
                        symbol=self.contract_symbol,
                        amount=contract_amount,
                        params={"reduceOnly": False}
                    )
                    
                    # 记录订单响应详情
                    logger.debug(f"【平衡操作】合约空单订单响应: {rebalance_order}")
                    
                    # 检查订单是否成功提交
                    if not rebalance_order or 'id' not in rebalance_order:
                        logger.error(f"【平衡操作】合约空单订单提交失败: {rebalance_order}")
                        return
                    
                    logger.info(f"【平衡操作】订单已提交 - ID: {rebalance_order.get('id')}")
                    
                    # 获取实际成交量
                    filled_amount = float(rebalance_order.get('filled', 0))
                    
                    # 如果订单信息中没有成交量，尝试获取更新
                    if filled_amount <= 0:
                        try:
                            await asyncio.sleep(1.5)
                            order_id = rebalance_order.get('id')
                            if order_id:
                                updated_order = await self.bitget.fetch_order(order_id, self.contract_symbol)
                                logger.debug(f"【平衡操作】更新后的合约订单: {updated_order}")
                                
                                if updated_order:
                                    filled_amount = float(updated_order.get('filled', 0))
                                    rebalance_order = updated_order
                        except Exception as ex:
                            logger.warning(f"【平衡操作】获取订单详情失败: {str(ex)}")
                            import traceback
                            logger.debug(f"【平衡操作】获取订单详情错误堆栈: {traceback.format_exc()}")
                    
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
                    logger.info(f"【平衡操作】成功开立 {filled_amount} {base_currency} 合约空单，累计差额更新为 "
                              f"{self.cumulative_position_diff:.8f} {base_currency} ({self.cumulative_position_diff_usdt:.2f} USDT)")
                    logger.info("=" * 50)
                    
                except Exception as e:
                    logger.error(f"【平衡操作】开立合约空单失败: {str(e)}")
                    import traceback
                    logger.debug(f"【平衡操作】开立合约空单错误堆栈: {traceback.format_exc()}")
                    
        except Exception as e:
            logger.error(f"执行平衡操作时出错: {str(e)}")
            import traceback
            logger.debug(f"平衡操作错误堆栈: {traceback.format_exc()}")
            raise

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
    parser.add_argument('-m', '--depth-multiplier', type=int, default=10, help='市场深度要求的乘数，默认为交易量的10倍')
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
                    f"杠杆: {args.leverage if args.leverage else '自动'}, 重复次数: {args.count}, "
                    f"深度乘数: {args.depth_multiplier}")

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
        trader.depth_multiplier = args.depth_multiplier  # 设置深度乘数
        
        await trader.initialize()

        # 记录交易次数
        completed_trades = 0
        target_count = args.count
        total_errors = 0

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
                try:
                    spot_order, contract_order = await trader.execute_hedge_trade_optimized()
                    
                    # 增强的成功验证逻辑
                    if not spot_order or not contract_order:
                        logger.error(f"交易执行失败: 一个或多个订单返回为空")
                        # 打印当前交易执行统计
                        logger.info(f"当前交易统计 - 已完成: {completed_trades}/{target_count}, 失败: {total_errors + 1}")
                        # 增加错误计数
                        total_errors += 1
                        
                        # 发生一次错误就终止交易
                        logger.error(f"交易执行失败，终止后续交易")
                        break
                    
                    # 详细检查订单状态
                    if (isinstance(spot_order, dict) and isinstance(contract_order, dict) and 
                        'id' in spot_order and 'id' in contract_order and
                        float(spot_order.get('filled', 0)) > 0 and float(contract_order.get('filled', 0)) > 0):
                        # 订单有效且有成交量，交易成功
                        completed_trades += 1
                        logger.info(f"第 {completed_trades}/{target_count} 次对冲交易成功完成!")
                        
                        # 详细打印交易执行结果
                        spot_filled = float(spot_order.get('filled', 0))
                        contract_filled = float(contract_order.get('filled', 0))
                        spot_status = spot_order.get('status', 'unknown')
                        contract_status = contract_order.get('status', 'unknown')
                        
                        logger.debug(f"交易执行详情 - Gate.io: ID={spot_order.get('id')}, 状态={spot_status}, 成交量={spot_filled}")
                        logger.debug(f"交易执行详情 - Bitget: ID={contract_order.get('id')}, 状态={contract_status}, 成交量={contract_filled}")
                        
                        # 输出当前累计差额信息
                        if hasattr(trader, 'cumulative_position_diff') and hasattr(trader, 'cumulative_position_diff_usdt'):
                            logger.info(f"当前累计差额: {trader.cumulative_position_diff:.8f} {trader.symbol.split('/')[0]} "
                                      f"({trader.cumulative_position_diff_usdt:.2f} USDT)")
                        
                        # 如果不是最后一次交易，等待一小段时间再继续
                        if completed_trades < target_count:
                            logger.info(f"等待1秒后继续下一次交易...")
                            await asyncio.sleep(1)
                        continue
                    else:
                        # 详细记录订单问题
                        spot_issues = []
                        contract_issues = []
                        
                        if not isinstance(spot_order, dict):
                            spot_issues.append(f"订单不是字典类型: {type(spot_order)}")
                        elif 'id' not in spot_order:
                            spot_issues.append("订单缺少ID字段")
                        elif float(spot_order.get('filled', 0)) <= 0:
                            spot_issues.append(f"订单未成交，成交量: {spot_order.get('filled', 0)}")
                        
                        if not isinstance(contract_order, dict):
                            contract_issues.append(f"订单不是字典类型: {type(contract_order)}")
                        elif 'id' not in contract_order:
                            contract_issues.append("订单缺少ID字段")
                        elif float(contract_order.get('filled', 0)) <= 0:
                            contract_issues.append(f"订单未成交，成交量: {contract_order.get('filled', 0)}")
                        
                        logger.error(f"订单验证失败 - Gate.io问题: {', '.join(spot_issues) if spot_issues else '无'}")
                        logger.error(f"订单验证失败 - Bitget问题: {', '.join(contract_issues) if contract_issues else '无'}")
                        logger.error(f"Gate.io订单: {spot_order}")
                        logger.error(f"Bitget订单: {contract_order}")
                        
                        # 打印当前交易执行统计
                        logger.info(f"当前交易统计 - 已完成: {completed_trades}/{target_count}, 失败: {total_errors + 1}")
                        break
                except Exception as e:
                    logger.error(f"执行交易过程中出错: {str(e)}")
                    import traceback
                    logger.debug(f"交易执行错误堆栈: {traceback.format_exc()}")

                    # 打印当前交易执行统计
                    logger.info(f"当前交易统计 - 已完成: {completed_trades}/{target_count}, 失败: {total_errors + 1}")
                    break
                
                # 如果执行到这里，说明交易失败
                total_errors += 1
                logger.error(f"第 {completed_trades + 1} 次交易执行失败，可能原因: 订单执行异常或持仓不匹配")
                # 发生任何错误就终止交易
                break
            except Exception as e:
                total_errors += 1
                logger.error(f"执行第 {completed_trades + 1} 次交易时出错: {str(e)}")
                import traceback
                logger.debug(f"错误堆栈: {traceback.format_exc()}")
                
                # 发生任何错误就终止交易
                logger.error(f"交易执行出错，终止后续交易")
                break
                
        # 打印最终执行结果和累计差额信息
        if completed_trades == target_count:
            logger.info(f"所有计划交易已完成! 成功执行 {completed_trades}/{target_count} 次交易")
        else:
            if total_errors > 0:
                logger.info(f"交易过程因错误中止，成功执行 {completed_trades}/{target_count} 次交易")
            else:
                logger.info(f"交易过程中止，成功执行 {completed_trades}/{target_count} 次交易")
        
        # 输出最终累计差额信息
        if hasattr(trader, 'cumulative_position_diff') and hasattr(trader, 'cumulative_position_diff_usdt'):
            logger.info(f"最终累计差额: {trader.cumulative_position_diff:.8f} {trader.symbol.split('/')[0]} "
                      f"({trader.cumulative_position_diff_usdt:.2f} USDT)")
            if hasattr(trader, 'rebalance_count') and trader.rebalance_count > 0:
                logger.info(f"执行了 {trader.rebalance_count} 次平衡操作")
        
        if total_errors > 0:
            logger.warning(f"执行过程中发生错误导致交易中止")

    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        import traceback
        logger.debug(f"错误堆栈: {traceback.format_exc()}")
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
