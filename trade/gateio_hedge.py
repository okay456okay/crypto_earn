#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io现货买入与Gate.io合约空单对冲的套利脚本

此脚本实现以下功能：
1. 从Gate.io市价买入指定token的现货
2. 从Gate.io开对应的合约空单进行对冲
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
import ccxt.pro as ccxtpro
from collections import defaultdict
from typing import Dict, Optional

from trade.gateio_api import redeem_earn

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import gateio_api_key, gateio_api_secret, proxies


class GateioHedgeTrader:
    """
    Gate.io现货-合约对冲交易类
    实现Gate.io现货买入与Gate.io合约空单对冲
    """
    
    def __init__(self, symbol, spot_amount=None, min_spread=0.001, leverage=20):
        """
        初始化交易参数
        
        Args:
            symbol (str): 交易对，如 'ETH/USDT'
            spot_amount (float): 现货交易数量
            min_spread (float): 最小价差，默认0.001 (0.1%)
            leverage (int): 合约杠杆倍数，默认20倍
        """
        self.symbol = symbol
        self.spot_amount = spot_amount
        self.min_spread = min_spread
        self.leverage = leverage
        
        # 设置合约交易对
        base, quote = symbol.split('/')
        self.contract_symbol = f"{base}/{quote}:USDT"  # Gate.io合约格式，如 ETH/USDT:USDT
        
        # 初始化Gate.io现货和合约账户
        self.spot_exchange = ccxtpro.gateio({
            'apiKey': gateio_api_key,
            'secret': gateio_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
        })
        
        self.futures_exchange = ccxtpro.gateio({
            'apiKey': gateio_api_key,
            'secret': gateio_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy':  proxies.get('https', None),
            'wss_proxy':  proxies.get('https', None),
            'ws_socks_proxy':  proxies.get('https', None),
            'options': {'defaultType': 'swap'}  # 设置为合约模式
        })
        
        self.spot_usdt = None
        self.futures_usdt = None
        
        # 用于存储最新订单簿数据
        self.orderbooks = {
            'spot': None,
            'futures': None
        }
        
        self.ws_running = False
        self.price_updates = asyncio.Queue()

    async def initialize(self):
        """初始化交易环境，包括设置合约参数和检查账户余额"""
        try:
            # 获取所有市场信息
            markets = await self.futures_exchange.fetch_markets()
            # 找到对应的合约市场信息
            contract_spec = None
            for market in markets:
                if market['symbol'] == self.contract_symbol:
                    contract_spec = market
                    break
            
            if not contract_spec:
                raise Exception(f"未找到合约 {self.contract_symbol} 的市场信息")
            
            logger.info("合约规格详情:")
            logger.info(f"- 合约乘数(contractSize): {contract_spec.get('contractSize')}")
            logger.info(f"- 最小下单量(minAmount): {contract_spec.get('limits', {}).get('amount', {}).get('min')}")
            logger.info(f"- 价格精度(precision): {contract_spec.get('precision')}")
            logger.info(f"- 合约类型(type): {contract_spec.get('type')}")
            logger.info(f"- 完整规格: {contract_spec}")
            
            # 保存合约乘数
            self.contract_size = float(contract_spec.get('contractSize', 1))
            logger.info(f"使用合约乘数: {self.contract_size}")
            
            # 设置合约杠杆
            await self.futures_exchange.set_leverage(self.leverage, self.contract_symbol)
            logger.info(f"设置Gate.io合约杠杆倍数为: {self.leverage}倍")
            
            # 获取并检查账户余额
            self.spot_usdt, self.futures_usdt = await self.check_balances()
            
            # 检查交易所手续费率
            spot_fees = await self.spot_exchange.fetch_trading_fee(self.symbol)
            futures_fees = await self.futures_exchange.fetch_trading_fee(self.contract_symbol)
            
            logger.info(f"Gate.io现货手续费率: {spot_fees}")
            logger.info(f"Gate.io合约手续费率: {futures_fees}")
            
            # 检查余额是否满足交易要求
            if self.spot_amount is not None:
                orderbook = await self.spot_exchange.fetch_order_book(self.symbol)
                current_price = float(orderbook['asks'][0][0])
                
                required_spot_usdt = float(self.spot_amount) * current_price * 1.02  # 额外2%作为滑点和手续费
                required_futures_margin = float(self.spot_amount) * current_price / self.leverage * 1.05  # 额外5%作为保证金
                
                if required_spot_usdt > self.spot_usdt or self.spot_usdt < 50:
                    redeem_earn('USDT', max(required_spot_usdt * 1.01, 50))
                    # raise Exception(f"Gate.io现货USDT余额不足，需要约 {required_spot_usdt:.2f} USDT，当前余额 {self.spot_usdt:.2f} USDT")
                if required_futures_margin > self.futures_usdt:
                    raise Exception(f"Gate.io合约USDT保证金不足，需要约 {required_futures_margin:.2f} USDT，当前余额 {self.futures_usdt:.2f} USDT")
                
                logger.info(f"账户余额检查通过 - 预估所需现货: {required_spot_usdt:.2f} USDT, 合约: {required_futures_margin:.2f} USDT")
                
            # 提前计算合约交易量
            self.futures_amount = self.futures_exchange.amount_to_precision(
                self.contract_symbol,
                float(self.spot_amount) / self.contract_size
            )
            logger.info(f"合约交易量计算:")
            logger.info(f"- 目标现货量: {self.spot_amount}")
            logger.info(f"- 合约乘数: {self.contract_size}")
            logger.info(f"- 实际合约量: {float(self.futures_amount)*float(self.contract_size)}")
            
        except Exception as e:
            logger.error(f"初始化失败: {str(e)}")
            raise

    async def check_balances(self):
        """
        检查Gate.io现货和合约账户的USDT余额
        
        Returns:
            tuple: (spot_balance, futures_balance) - 返回现货和合约的USDT余额
        """
        try:
            spot_balance = await self.spot_exchange.fetch_balance()
            futures_balance = await self.futures_exchange.fetch_balance()
            
            spot_usdt = spot_balance.get('USDT', {}).get('free', 0)
            futures_usdt = futures_balance.get('USDT', {}).get('free', 0)
            
            logger.info(f"Gate.io账户余额 - 现货: {spot_usdt:.2f} USDT, 合约: {futures_usdt:.2f} USDT")
            return spot_usdt, futures_usdt
            
        except Exception as e:
            logger.error(f"检查余额时出错: {str(e)}")
            raise

    async def subscribe_orderbooks(self):
        """订阅Gate.io现货和合约的订单簿数据"""
        try:
            self.ws_running = True
            while self.ws_running:
                try:
                    # 创建两个任务来订阅订单簿
                    tasks = [
                        asyncio.create_task(self.spot_exchange.watch_order_book(self.symbol)),
                        asyncio.create_task(self.futures_exchange.watch_order_book(self.contract_symbol))
                    ]
                    
                    # 等待任意一个订单簿更新
                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # 处理完成的任务
                    for task in done:
                        try:
                            if not self.ws_running:  # 如果ws_running为False，立即退出
                                break
                            
                            ob = task.result()
                            if task == tasks[0]:  # 现货订单簿
                                self.orderbooks['spot'] = ob
                                logger.debug(f"收到Gate.io现货订单簿更新")
                            else:  # 合约订单簿
                                self.orderbooks['futures'] = ob
                                logger.debug(f"收到Gate.io合约订单簿更新")
                            
                            # 如果两个订单簿都有数据，检查价差
                            if self.orderbooks['spot'] and self.orderbooks['futures']:
                                await self.check_spread_from_orderbooks()
                            
                        except Exception as e:
                            logger.error(f"处理订单簿数据时出错: {str(e)}")
                    
                    # 取消未完成的任务
                    for task in pending:
                        task.cancel()
                        
                except Exception as e:
                    if isinstance(e, asyncio.CancelledError):
                        break
                    logger.error(f"订阅订单簿时出错: {str(e)}")
                    if not self.ws_running:
                        break
                    await asyncio.sleep(1)  # 出错后等待一秒再重试
                    
        except Exception as e:
            logger.error(f"订单簿订阅循环出错: {str(e)}")
        finally:
            self.ws_running = False

    async def check_spread_from_orderbooks(self):
        """从订单簿数据中检查价差"""
        try:
            spot_ob = self.orderbooks['spot']
            futures_ob = self.orderbooks['futures']
            
            if not spot_ob or not futures_ob:
                return
            
            spot_ask = Decimal(str(spot_ob['asks'][0][0]))
            spot_ask_volume = Decimal(str(spot_ob['asks'][0][1]))
            
            futures_bid = Decimal(str(futures_ob['bids'][0][0]))
            futures_bid_volume = Decimal(str(futures_ob['bids'][0][1]))
            
            spread = futures_bid - spot_ask
            spread_percent = spread / spot_ask
            
            # 将价差数据放入队列
            spread_data = {
                'spread_percent': float(spread_percent),
                'spot_ask': float(spot_ask),
                'futures_bid': float(futures_bid),
                'spot_ask_volume': float(spot_ask_volume),
                'futures_bid_volume': float(futures_bid_volume)
            }
            await self.price_updates.put(spread_data)
            
        except Exception as e:
            logger.error(f"检查订单簿价差时出错: {str(e)}")

    async def wait_for_spread(self):
        """等待价差达到要求"""
        subscription_task = None
        try:
            subscription_task = asyncio.create_task(self.subscribe_orderbooks())
            
            while True:
                try:
                    spread_data = await asyncio.wait_for(
                        self.price_updates.get(),
                        timeout=30  # 增加超时时间到30秒
                    )
                    
                    spread_percent = spread_data['spread_percent']
                    
                    logger.debug(f"{self.symbol}价格检查 - Gate.io现货卖1: {spread_data['spot_ask']} (量: {spread_data['spot_ask_volume']}), "
                               f"Gate.io合约买1: {spread_data['futures_bid']} (量: {spread_data['futures_bid_volume']}), "
                               f"价差: {spread_percent*100:.4f}%")
                    
                    if spread_percent >= self.min_spread:
                        logger.info(f"{self.symbol}价差条件满足: {spread_percent*100:.4f}% >= {self.min_spread*100:.4f}%")
                        return spread_data
                    
                    logger.debug(f"{self.symbol}价差条件不满足: {spread_percent*100:.4f}% < {self.min_spread*100:.4f}%")
                    
                except asyncio.TimeoutError:
                    logger.warning("等待价差数据超时，重新订阅订单簿")
                    if subscription_task and not subscription_task.done():
                        subscription_task.cancel()
                    subscription_task = asyncio.create_task(self.subscribe_orderbooks())
                    
        except Exception as e:
            logger.error(f"{self.symbol}等待价差时出错: {str(e)}")
            raise
        finally:
            self.ws_running = False
            if subscription_task and not subscription_task.done():
                subscription_task.cancel()
                try:
                    await subscription_task
                except asyncio.CancelledError:
                    pass

    async def execute_hedge_trade(self):
        """执行对冲交易"""
        try:
            # 等待价差满足条件
            spread_data = await self.wait_for_spread()
            spot_ask = spread_data['spot_ask']
            
            # 计算现货成本
            spot_cost = float(self.spot_amount) * float(spot_ask)
            
            # 直接执行交易，不做其他检查
            spot_order, futures_order = await asyncio.gather(
                self.spot_exchange.create_market_buy_order(
                    symbol=self.symbol,
                    amount=spot_cost,
                    params={'createMarketBuyOrderRequiresPrice': False, 'quoteOrderQty': True}
                ),
                self.futures_exchange.create_market_sell_order(
                    symbol=self.contract_symbol,
                    amount=self.futures_amount,
                    params={
                        "reduceOnly": False,
                        # "marginMode": "cross",
                        "crossLeverageLimit": self.leverage,
                        "account": "cross_margin",
                    }
                )
            )
            
            # 记录交易结果
            base_currency = self.symbol.split('/')[0]
            logger.info(f"交易执行结果:")
            logger.info(f"现货买入: {self.spot_amount} {base_currency}, 预估成本: {spot_cost:.2f} USDT")
            logger.info(f"合约做空: {self.futures_amount} {base_currency}")
            
            # 获取实际成交结果
            spot_filled = float(spot_order['filled'])
            spot_fees = spot_order.get('fees', [])
            spot_base_fee = sum(float(fee['cost']) for fee in spot_fees if fee['currency'] == base_currency)
            actual_spot_position = spot_filled - spot_base_fee
            
            logger.info(f"实际成交:")
            logger.info(f"现货成交: {spot_filled} {base_currency}, 手续费: {spot_base_fee} {base_currency}")
            
            # 检查持仓情况
            await self.check_positions()
            
            return spot_order, futures_order
            
        except Exception as e:
            logger.error(f"执行对冲交易时出错: {str(e)}")
            raise

    async def check_positions(self):
        """检查交易后的持仓情况"""
        try:
            await asyncio.sleep(1)  # 等待订单状态更新
            
            # 并行获取现货和合约持仓信息
            spot_balance_task = self.spot_exchange.fetch_balance()
            futures_positions_task = self.futures_exchange.fetch_positions([self.contract_symbol])
            
            spot_balance, futures_positions = await asyncio.gather(
                spot_balance_task,
                futures_positions_task
            )
            
            base_currency = self.symbol.split('/')[0]
            spot_position = spot_balance.get(base_currency, {}).get('total', 0)
            
            # 修改合约持仓检查逻辑
            futures_position = 0
            if futures_positions:
                for position in futures_positions:
                    if position['symbol'] == self.contract_symbol:
                        # 使用 contractSize 来计算实际持仓数量
                        contract_size = float(position.get('contractSize', 1))
                        contracts = abs(float(position.get('contracts', 0)))
                        futures_position = contracts * contract_size
                        position_side = position.get('side', 'unknown')
                        position_leverage = position.get('leverage', self.leverage)
                        position_notional = position.get('notional', 0)
                        
                        logger.info(f"Gate.io合约持仓: {position_side} {futures_position} {base_currency}, "
                                  f"杠杆: {position_leverage}倍, 名义价值: {position_notional}")
            
            logger.info(f"持仓检查 - Gate.io现货: {spot_position} {base_currency}, "
                       f"Gate.io合约: {futures_position} {base_currency}")
            
            # 检查持仓是否平衡（允许0.5%的误差）
            position_diff = abs(float(spot_position) - float(futures_position))
            position_diff_percent = position_diff / float(spot_position) * 100 if float(spot_position) != 0 else float('inf')
            
            if position_diff_percent > 0.5:
                logger.warning(f"现货和合约持仓不平衡! 差异: {position_diff} {base_currency} ({position_diff_percent:.2f}%)")
            else:
                logger.info(f"现货和合约持仓基本平衡，差异在允许范围内: {position_diff} {base_currency} ({position_diff_percent:.2f}%)")
                
        except Exception as e:
            logger.error(f"检查持仓信息失败: {str(e)}")

    async def close_connections(self):
        """关闭所有交易所连接"""
        try:
            self.ws_running = False  # 首先设置ws_running为False
            await asyncio.sleep(0.5)  # 给一些时间让WebSocket连接优雅关闭
            
            # 关闭交易所连接
            close_tasks = [
                self.spot_exchange.close(),
                self.futures_exchange.close()
            ]
            await asyncio.gather(*close_tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"关闭连接时出错: {str(e)}")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Gate.io现货与合约对冲交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, 
                       help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, required=True, 
                       help='购买的现货数量')
    parser.add_argument('-p', '--min-spread', type=float, default=-0.0001,
                       help='最小价差要求，默认0.001 (0.1%%)')
    parser.add_argument('-l', '--leverage', type=int, default=20, 
                       help='合约杠杆倍数，默认20倍')
    return parser.parse_args()


async def main():
    """主函数"""
    args = parse_arguments()
    
    try:
        trader = GateioHedgeTrader(
            symbol=args.symbol,
            spot_amount=args.amount,
            min_spread=args.min_spread,
            leverage=args.leverage
        )
        
        await trader.initialize()
        spot_order, futures_order = await trader.execute_hedge_trade()
        
        if spot_order and futures_order:
            logger.info("对冲交易成功完成!")
        else:
            logger.warning("未执行对冲交易")
            
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        return 1
    finally:
        if 'trader' in locals():
            await trader.close_connections()
    
    return 0


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 