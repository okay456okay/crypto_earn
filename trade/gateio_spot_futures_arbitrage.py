#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io现货-合约价差套利脚本

此脚本实现以下功能：
1. 当现货价格比合约价格高3%以上时：
   - 卖出现货
   - 开合约多单
2. 当现货价格比合约价格价差缩小到1%以内时：
   - 买入现货
   - 平合约多单
3. 定期检查账户余额和持仓
4. 使用websocket实时监控价格
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
from typing import Dict, Optional, Tuple

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import debug_logger as logger
from config import gateio_api_key, gateio_api_secret, proxies


class GateioSpotFuturesArbitrage:
    """
    Gate.io现货-合约价差套利类
    实现现货和合约之间的套利交易
    """
    
    def __init__(self, symbol: str, trade_amount: float, max_spread: float = 0.03, 
                 min_spread: float = 0.01, leverage: int = 20, 
                 balance_check_interval: int = 300, test_mode: bool = False):
        """
        初始化交易参数
        
        Args:
            symbol (str): 交易对，如 'ETH/USDT'
            trade_amount (float): 每次交易的数量
            max_spread (float): 开仓价差阈值，默认0.03 (3%)
            min_spread (float): 平仓价差阈值，默认0.01 (1%)
            leverage (int): 合约杠杆倍数，默认20倍
            balance_check_interval (int): 余额检查间隔(秒)，默认600秒
            test_mode (bool): 是否为测试模式，默认False
        """
        self.symbol = symbol
        self.trade_amount = trade_amount
        self.max_spread = max_spread
        self.min_spread = min_spread
        self.leverage = leverage
        self.balance_check_interval = balance_check_interval
        self.test_mode = test_mode  # 添加测试模式标志
        
        # 设置合约交易对
        base, quote = symbol.split('/')
        self.contract_symbol = f"{base}/{quote}:USDT"
        
        # 初始化交易所连接
        self.spot_exchange = ccxtpro.gateio({
            'apiKey': gateio_api_key,
            'secret': gateio_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
        })
        
        self.futures_exchange = ccxtpro.gateio({
            'apiKey': gateio_api_key,
            'secret': gateio_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'options': {'defaultType': 'swap'}
        })
        
        # 账户余额和持仓信息
        self.balances = {
            'spot': {'free': 0, 'used': 0},
            'futures': {'free': 0, 'used': 0}
        }
        self.positions = {
            'spot': 0,
            'futures': 0
        }
        
        # 订单簿数据
        self.orderbooks = {
            'spot': None,
            'futures': None
        }
        
        self.ws_running = False
        self.price_updates = asyncio.Queue()
        self.contract_size = 1  # 将在initialize中更新

    async def initialize(self):
        """初始化交易环境，包括设置合约参数和检查账户余额"""
        try:
            # 获取合约规格
            markets = await self.futures_exchange.fetch_markets()
            contract_spec = next(
                (m for m in markets if m['symbol'] == self.contract_symbol), 
                None
            )
            
            if not contract_spec:
                raise Exception(f"未找到合约 {self.contract_symbol} 的市场信息")
            
            self.contract_size = float(contract_spec.get('contractSize', 1))
            logger.info(f"合约规格 - 乘数: {self.contract_size}")
            
            # 设置合约杠杆
            await self.futures_exchange.set_leverage(self.leverage, self.contract_symbol)
            logger.info(f"设置合约杠杆倍数: {self.leverage}倍")
            
            # 初始检查余额和持仓
            await self.check_balances_and_positions()
            
        except Exception as e:
            logger.error(f"初始化失败: {str(e)}")
            raise

    async def check_balances_and_positions(self):
        """检查账户余额和持仓情况"""
        try:
            # 获取现货和合约账户信息
            spot_balance = await self.spot_exchange.fetch_balance()
            futures_balance = await self.futures_exchange.fetch_balance()
            futures_positions = await self.futures_exchange.fetch_positions([self.contract_symbol])
            
            base_currency = self.symbol.split('/')[0]
            
            # 更新余额信息
            self.balances['spot'] = {
                'free': float(spot_balance.get(base_currency, {}).get('free', 0)),
                'used': float(spot_balance.get(base_currency, {}).get('used', 0))
            }
            self.balances['futures'] = {
                'free': float(futures_balance.get('USDT', {}).get('free', 0)),
                'used': float(futures_balance.get('USDT', {}).get('used', 0))
            }
            
            # 更新持仓信息
            self.positions['spot'] = float(spot_balance.get(base_currency, {}).get('total', 0))
            
            futures_position = 0
            for position in futures_positions:
                if position['symbol'] == self.contract_symbol:
                    contracts = float(position.get('contracts', 0))
                    futures_position = contracts * self.contract_size
                    break
            self.positions['futures'] = futures_position
            
            logger.info("账户状态更新:")
            logger.info(f"现货{base_currency}余额: {self.balances['spot']['free']:.4f} (可用) "
                       f"{self.balances['spot']['used']:.4f} (已用)")
            logger.info(f"合约USDT余额: {self.balances['futures']['free']:.2f} (可用) "
                       f"{self.balances['futures']['used']:.2f} (已用)")
            logger.info(f"持仓 - 现货: {self.positions['spot']:.4f} {base_currency}, "
                       f"合约: {self.positions['futures']:.4f} {base_currency}")
            
        except Exception as e:
            logger.error(f"检查余额和持仓失败: {str(e)}")
            raise

    async def subscribe_orderbooks(self):
        """订阅现货和合约订单簿"""
        try:
            self.ws_running = True
            while self.ws_running:
                try:
                    tasks = [
                        asyncio.create_task(self.spot_exchange.watch_order_book(self.symbol)),
                        asyncio.create_task(self.futures_exchange.watch_order_book(self.contract_symbol))
                    ]
                    
                    # 修改为等待所有任务完成
                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.ALL_COMPLETED,
                        timeout=30  # 添加超时时间
                    )
                    
                    # 处理超时情况
                    if pending:
                        logger.warning("订单簿订阅超时，准备重新订阅")
                        for task in pending:
                            task.cancel()
                        continue
                    
                    # 处理完成的任务
                    for task in done:
                        try:
                            if not self.ws_running:
                                break
                            
                            ob = task.result()
                            # 根据订单簿的symbol判断是现货还是合约
                            if ob['symbol'] == self.symbol:
                                self.orderbooks['spot'] = ob
                                logger.debug(f"更新现货订单簿 - 买一: {ob['bids'][0][0]}, 卖一: {ob['asks'][0][0]}")
                            else:
                                self.orderbooks['futures'] = ob
                                logger.debug(f"更新合约订单簿 - 买一: {ob['bids'][0][0]}, 卖一: {ob['asks'][0][0]}")
                            
                        except Exception as e:
                            logger.error(f"处理订单簿数据出错: {str(e)}")
                            continue
                    
                    # 两个订单簿都有数据时进行价差分析
                    if self.orderbooks['spot'] and self.orderbooks['futures']:
                        await self.analyze_spread()
                    else:
                        logger.debug("等待订单簿数据完整...")
                    
                except Exception as e:
                    if isinstance(e, asyncio.CancelledError):
                        break
                    logger.error(f"订阅订单簿出错: {str(e)}")
                    # 添加重连延迟
                    await asyncio.sleep(5)
                    continue
                
        except Exception as e:
            logger.error(f"订单簿订阅循环出错: {str(e)}")
        finally:
            self.ws_running = False
            logger.info("订单簿订阅已停止")

    async def analyze_spread(self):
        """分析现货和合约价差，并在满足条件时触发交易"""
        try:
            spot_ob = self.orderbooks['spot']
            futures_ob = self.orderbooks['futures']
            
            if not spot_ob or not futures_ob:
                return
                
            spot_bid = Decimal(str(spot_ob['bids'][0][0]))  # 现货买一价
            spot_ask = Decimal(str(spot_ob['asks'][0][0]))  # 现货卖一价
            futures_bid = Decimal(str(futures_ob['bids'][0][0]))  # 合约买一价
            futures_ask = Decimal(str(futures_ob['asks'][0][0]))  # 合约卖一价
            
            # 计算开仓价差 (现货bid - 合约ask) / 合约ask
            open_spread = (spot_bid - futures_ask) / futures_ask
            
            # 计算平仓价差 (现货ask - 合约bid) / 合约bid
            close_spread = (spot_ask - futures_bid) / futures_bid
            
            # 将常规价差分析改为DEBUG级别
            logger.debug(f"价差分析 - 现货买/卖: {float(spot_bid)}/{float(spot_ask)}, "
                        f"合约买/卖: {float(futures_bid)}/{float(futures_ask)}, "
                        f"开仓价差: {float(open_spread)*100:.2f}%, "
                        f"平仓价差: {float(close_spread)*100:.2f}%")
            
            # 检查是否满足交易条件
            if float(open_spread) >= self.max_spread:
                logger.info(f"发现开仓机会 - 价差: {float(open_spread)*100:.2f}% >= {self.max_spread*100:.2f}%")
                await self.execute_open_arbitrage()
            elif float(close_spread) <= self.min_spread:
                logger.info(f"发现平仓机会 - 价差: {float(close_spread)*100:.2f}% <= {self.min_spread*100:.2f}%")
                await self.execute_close_arbitrage()
                
        except Exception as e:
            logger.error(f"分析价差时出错: {str(e)}")

    async def execute_open_arbitrage(self):
        """
        执行开仓套利:
        1. 卖出现货
        2. 开合约多单
        """
        try:
            spot_ob = self.orderbooks['spot']
            futures_ob = self.orderbooks['futures']
            
            if not spot_ob or not futures_ob:
                return
                
            spot_bid = float(spot_ob['bids'][0][0])  # 现货买一价
            spot_bid_volume = float(spot_ob['bids'][0][1])  # 现货买一量
            futures_ask = float(futures_ob['asks'][0][0])  # 合约卖一价
            futures_ask_volume = float(futures_ob['asks'][0][1]) * self.contract_size  # 合约卖一量
            
            # 计算实际可交易数量（取最小值）
            executable_amount = min(self.trade_amount, spot_bid_volume, futures_ask_volume)
            
            # 计算预期利润
            spread = (spot_bid - futures_ask) / futures_ask
            fee_rate = 0.001  # 0.1% 手续费
            spot_fee = executable_amount * spot_bid * fee_rate
            futures_fee = executable_amount * futures_ask * fee_rate
            profit = executable_amount * (spot_bid - futures_ask) - spot_fee - futures_fee
            
            if self.test_mode:
                logger.info(
                    f"OPEN|{self.symbol}|"
                    f"spot_bid={spot_bid:.4f}|spot_volume={spot_bid_volume:.4f}|"
                    f"futures_ask={futures_ask:.4f}|futures_volume={futures_ask_volume:.4f}|"
                    f"spread={spread*100:.2f}%|"
                    f"plan_amount={self.trade_amount:.4f}|exec_amount={executable_amount:.4f}|"
                    f"fees={spot_fee + futures_fee:.4f}|profit={profit:.4f}"
                )
                return
                
            # 检查是否有足够的现货可卖
            base_currency = self.symbol.split('/')[0]
            if self.balances['spot']['free'] < executable_amount:
                logger.warning(f"现货余额不足，需要 {executable_amount} {base_currency}，"
                             f"当前可用 {self.balances['spot']['free']} {base_currency}")
                return
                
            # 计算合约交易量
            futures_amount = self.futures_exchange.amount_to_precision(
                self.contract_symbol,
                executable_amount / self.contract_size
            )
            
            # 执行交易
            spot_order, futures_order = await asyncio.gather(
                self.spot_exchange.create_market_sell_order(
                    symbol=self.symbol,
                    amount=executable_amount
                ),
                self.futures_exchange.create_market_buy_order(
                    symbol=self.contract_symbol,
                    amount=futures_amount,
                    params={
                        "reduceOnly": False,
                        "marginMode": "cross",
                        "crossLeverageLimit": self.leverage,
                    }
                )
            )
            
            logger.info("开仓套利执行成功:")
            logger.info(f"现货卖出: {executable_amount} {base_currency}")
            logger.info(f"合约做多: {futures_amount} 张")
            
        except Exception as e:
            logger.error(f"执行开仓套利失败: {str(e)}")

    async def execute_close_arbitrage(self):
        """
        执行平仓套利:
        1. 买入现货
        2. 平合约多单
        """
        try:
            spot_ob = self.orderbooks['spot']
            futures_ob = self.orderbooks['futures']
            
            if not spot_ob or not futures_ob:
                return
                
            spot_ask = float(spot_ob['asks'][0][0])  # 现货卖一价
            spot_ask_volume = float(spot_ob['asks'][0][1])  # 现货卖一量
            futures_bid = float(futures_ob['bids'][0][0])  # 合约买一价
            futures_bid_volume = float(futures_ob['bids'][0][1]) * self.contract_size  # 合约买一量
            
            # 计算实际可交易数量（取最小值）
            executable_amount = min(self.trade_amount, spot_ask_volume, futures_bid_volume)
            
            # 计算预期利润
            spread = (spot_ask - futures_bid) / futures_bid
            fee_rate = 0.001  # 0.1% 手续费
            spot_fee = executable_amount * spot_ask * fee_rate
            futures_fee = executable_amount * futures_bid * fee_rate
            profit = executable_amount * (futures_bid - spot_ask) - spot_fee - futures_fee
            
            if self.test_mode:
                logger.info(
                    f"CLOSE|{self.symbol}|"
                    f"spot_ask={spot_ask:.4f}|spot_volume={spot_ask_volume:.4f}|"
                    f"futures_bid={futures_bid:.4f}|futures_volume={futures_bid_volume:.4f}|"
                    f"spread={spread*100:.2f}%|"
                    f"plan_amount={self.trade_amount:.4f}|exec_amount={executable_amount:.4f}|"
                    f"fees={spot_fee + futures_fee:.4f}|profit={profit:.4f}"
                )
                return
                
            # 检查是否有足够的USDT买入现货
            if self.balances['spot'].get('USDT', {}).get('free', 0) < executable_amount:
                logger.warning(f"USDT余额不足，需要约 {executable_amount:.2f} USDT")
                return
                
            # 计算合约平仓量
            futures_amount = self.futures_exchange.amount_to_precision(
                self.contract_symbol,
                executable_amount / self.contract_size
            )
            
            # 执行交易
            spot_order, futures_order = await asyncio.gather(
                self.spot_exchange.create_market_buy_order(
                    symbol=self.symbol,
                    amount=executable_amount
                ),
                self.futures_exchange.create_market_sell_order(
                    symbol=self.contract_symbol,
                    amount=futures_amount,
                    params={
                        "reduceOnly": True,
                        "marginMode": "cross",
                        "crossLeverageLimit": self.leverage,
                    }
                )
            )
            
            logger.info("平仓套利执行成功:")
            logger.info(f"现货买入: {executable_amount} {self.symbol.split('/')[0]}")
            logger.info(f"合约平多: {futures_amount} 张")
            
        except Exception as e:
            logger.error(f"执行平仓套利失败: {str(e)}")

    async def start(self):
        """启动套利程序"""
        try:
            await self.initialize()
            
            # 创建定期检查余额的任务
            balance_check_task = asyncio.create_task(self.periodic_balance_check())
            
            # 启动订单簿订阅
            await self.subscribe_orderbooks()
            
        except Exception as e:
            logger.error(f"启动失败: {str(e)}")
        finally:
            self.ws_running = False
            if 'balance_check_task' in locals():
                balance_check_task.cancel()

    async def periodic_balance_check(self):
        """定期检查账户余额和持仓"""
        while True:
            try:
                await asyncio.sleep(self.balance_check_interval)
                await self.check_balances_and_positions()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"定期检查余额和持仓时出错: {str(e)}")
                await asyncio.sleep(5)

    async def close(self):
        """关闭所有连接"""
        self.ws_running = False
        await asyncio.sleep(0.5)
        await asyncio.gather(
            self.spot_exchange.close(),
            self.futures_exchange.close()
        )


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Gate.io现货-合约价差套利')
    parser.add_argument('-s', '--symbol', type=str, required=True,
                       help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, required=True,
                       help='每次交易的数量')
    parser.add_argument('--max-spread', type=float, default=0.03,
                       help='开仓价差阈值，默认0.03 (3%%)')
    parser.add_argument('--min-spread', type=float, default=0.01,
                       help='平仓价差阈值，默认0.01 (1%%)')
    parser.add_argument('-l', '--leverage', type=int, default=20,
                       help='合约杠杆倍数，默认20倍')
    parser.add_argument('-t', '--test', action='store_true',
                       help='测试模式，只显示交易信息不实际执行')
    parser.add_argument('-d', '--debug', action='store_true',
                       help='启用DEBUG级别日志输出')
    return parser.parse_args()


async def main():
    """主函数"""
    args = parse_arguments()
    
    # 设置日志级别
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    try:
        trader = GateioSpotFuturesArbitrage(
            symbol=args.symbol,
            trade_amount=args.amount,
            max_spread=args.max_spread,
            min_spread=args.min_spread,
            leverage=args.leverage,
            test_mode=args.test
        )
        
        await trader.start()
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        return 1
    finally:
        if 'trader' in locals():
            await trader.close()
    
    return 0


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close()