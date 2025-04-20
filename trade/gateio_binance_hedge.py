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
            
            binance_bid = Decimal(str(binance_ob['bids'][0][0]))
            binance_bid_volume = Decimal(str(binance_ob['bids'][0][1]))
            
            spread = binance_bid - gateio_ask
            spread_percent = spread / gateio_ask
            
            # 将价差数据放入队列
            spread_data = {
                'spread_percent': float(spread_percent),
                'gateio_ask': float(gateio_ask),
                'binance_bid': float(binance_bid),
                'gateio_ask_volume': float(gateio_ask_volume),
                'binance_bid_volume': float(binance_bid_volume)
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
                               f"Binance买1: {spread_data['binance_bid']} (量: {spread_data['binance_bid_volume']}), "
                               f"价差: {spread_percent*100:.4f}%")
                    
                    if spread_percent >= self.min_spread:
                        logger.info(f"{self.symbol}价差条件满足: {spread_percent*100:.4f}% >= {self.min_spread*100:.4f}%")
                        return (spread_percent, spread_data['gateio_ask'], spread_data['binance_bid'],
                               spread_data['gateio_ask_volume'], spread_data['binance_bid_volume'])
                    
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
            spread_percent, gateio_ask, binance_bid, gateio_ask_volume, binance_bid_volume = spread_data
            
            # 2. 立即准备下单参数, 不然现货会比合约少一些
            trade_amount = self.spot_amount * 1.001
            cost = float(trade_amount) * float(gateio_ask)
            contract_amount = self.binance.amount_to_precision(self.contract_symbol, trade_amount)
            
            # 打印交易计划
            logger.info("=" * 50)
            logger.info("交易计划:")
            logger.info(f"Gate.io 现货买入: {trade_amount} {self.symbol.split('/')[0]} @ {gateio_ask} USDT")
            logger.info(f"预计成本: {cost:.2f} USDT")
            logger.info(f"Binance 合约开空: {contract_amount} {self.contract_symbol} @ {binance_bid} USDT")
            logger.info(f"当前价差: {spread_percent*100:.4f}%")
            logger.info("=" * 50)
            
            if self.test_mode:
                logger.info("测试模式: 不执行实际交易")
                return None, None
            
            # 3. 立即执行交易
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
            
            # 4. 交易后再进行其他操作
            base_currency = self.symbol.split('/')[0]
            logger.info(f"计划交易数量: {trade_amount} {base_currency}")
            logger.info(f"在Gate.io市价买入 {trade_amount} {base_currency}, 预估成本: {cost:.2f} USDT")
            logger.info(f"在Binance市价开空单 {contract_amount} {base_currency}")
            
            # 验证订单执行状态
            spot_status = spot_order.get('status', '')
            contract_status = contract_order.get('status', '')
            
            # 检查订单是否成功执行
            if spot_status not in ['closed', 'filled'] or contract_status not in ['closed', 'filled']:
                logger.error(f"订单执行异常 - 现货订单状态: {spot_status}, 合约订单状态: {contract_status}")
                logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因订单执行异常退出程序")
                return 0 if successful_trades > 0 else 1
            
            # 检查本次操作的现货和合约数量是否匹配
            base_currency = self.symbol.split('/')[0]
            
            # 获取现货订单的实际成交结果
            spot_filled_amount = float(spot_order['filled'])
            spot_fees = spot_order.get('fees', [])
            spot_base_fee = sum(float(fee['cost']) for fee in spot_fees if fee['currency'] == base_currency)
            spot_actual_position = spot_filled_amount - spot_base_fee
            
            # 获取合约订单的实际成交结果
            contract_filled_amount = float(contract_order['filled'])
            contract_fees = contract_order.get('fees', [])
            contract_base_fee = sum(float(fee['cost']) for fee in contract_fees if fee['currency'] == base_currency)
            contract_actual_position = contract_filled_amount - contract_base_fee
            
            # 检查本次操作的现货和合约持仓差异
            position_diff = abs(spot_actual_position - contract_actual_position)
            if spot_actual_position > 0:
                position_diff_percent = position_diff / spot_actual_position * 100
                
                # 记录本次操作的持仓情况
                logger.info(f"本次操作 - 现货成交: {spot_actual_position} {base_currency}, "
                           f"合约成交: {contract_actual_position} {base_currency}, "
                           f"差异: {position_diff} {base_currency} ({position_diff_percent:.2f}%)")
                
                # 如果持仓差异超过2%，视为异常
                if position_diff_percent > 2:
                    logger.error(f"本次操作的现货和合约持仓差异过大: {position_diff} {base_currency} ({position_diff_percent:.2f}%)")
                    logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因持仓不平衡退出程序")
                    return 0 if successful_trades > 0 else 1
            
            logger.info(f"成功完成第 {i+1}/{args.count} 次对冲交易!")
            successful_trades += 1

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
            raise

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
                    # 验证订单执行状态
                    spot_status = spot_order.get('status', '')
                    contract_status = contract_order.get('status', '')
                    
                    # 检查订单是否成功执行
                    if spot_status not in ['closed', 'filled'] or contract_status not in ['closed', 'filled']:
                        logger.error(f"订单执行异常 - 现货订单状态: {spot_status}, 合约订单状态: {contract_status}")
                        logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因订单执行异常退出程序")
                        return 0 if successful_trades > 0 else 1
                    
                    # 检查本次操作的现货和合约数量是否匹配
                    base_currency = trader.symbol.split('/')[0]
                    
                    # 获取现货订单的实际成交结果
                    spot_filled_amount = float(spot_order['filled'])
                    spot_fees = spot_order.get('fees', [])
                    spot_base_fee = sum(float(fee['cost']) for fee in spot_fees if fee['currency'] == base_currency)
                    spot_actual_position = spot_filled_amount - spot_base_fee
                    
                    # 获取合约订单的实际成交结果
                    contract_filled_amount = float(contract_order['filled'])
                    contract_fees = contract_order.get('fees', [])
                    contract_base_fee = sum(float(fee['cost']) for fee in contract_fees if fee['currency'] == base_currency)
                    contract_actual_position = contract_filled_amount - contract_base_fee
                    
                    # 检查本次操作的现货和合约持仓差异
                    position_diff = abs(spot_actual_position - contract_actual_position)
                    if spot_actual_position > 0:
                        position_diff_percent = position_diff / spot_actual_position * 100
                        
                        # 记录本次操作的持仓情况
                        logger.info(f"本次操作 - 现货成交: {spot_actual_position} {base_currency}, "
                                   f"合约成交: {contract_actual_position} {base_currency}, "
                                   f"差异: {position_diff} {base_currency} ({position_diff_percent:.2f}%)")
                        
                        # 如果持仓差异超过2%，视为异常
                        if position_diff_percent > 2:
                            logger.error(f"本次操作的现货和合约持仓差异过大: {position_diff} {base_currency} ({position_diff_percent:.2f}%)")
                            logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因持仓不平衡退出程序")
                            return 0 if successful_trades > 0 else 1
                    
                    logger.info(f"成功完成第 {i+1}/{args.count} 次对冲交易!")
                    successful_trades += 1
                else:
                    logger.error(f"第 {i+1}/{args.count} 次对冲交易未能完成")
                    logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因交易未完成退出程序")
                    return 0 if successful_trades > 0 else 1
            except Exception as e:
                logger.error(f"第 {i+1}/{args.count} 次交易执行失败: {str(e)}")
        
        # 报告总体执行情况
        logger.info(f"总计执行 {successful_trades}/{args.count} 次交易")
            
        if successful_trades > 0:
            return 0
        else:
            logger.error("所有交易均未完成")
            return 1
            
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        logger.info(f"已完成 {successful_trades}/{args.count} 次交易，因发生错误退出程序")
        return 1
    finally:
        # 确保关闭交易所连接
        if 'trader' in locals():
            await asyncio.gather(
                trader.gateio.close(),
                trader.binance.close()
            )


if __name__ == "__main__":
    # 设置并启动事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 