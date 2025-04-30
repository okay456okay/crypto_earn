#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Bybit合约插针反弹交易脚本

此脚本实现以下功能：
1. 监控价格变化，在2分钟内下跌超过1%且从最低点反弹超过0.2%时买入合约多单
2. 在价格上涨超过0.8%或下跌超过0.3%时平仓，或5分钟后强制平仓
3. 支持测试模式，可以模拟交易而不实际下单
4. 记录和统计交易结果，包括价格、数量、手续费和盈亏
"""

import sys
import os
import time
import logging
import argparse
from decimal import Decimal
import asyncio
import ccxt.pro as ccxtpro
from collections import deque
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import bybit_api_key, bybit_api_secret, proxies


class PinReboundTrader:
    """
    Bybit合约插针反弹交易类
    实现监控价格变化并在满足条件时执行交易
    """

    def __init__(self, symbol: str, min_drop: float = 0.01, min_rebound: float = 0.002,
                 take_profit: float = 0.008, stop_loss: float = 0.003, max_hold_time: int = 300,
                 test_mode: bool = False):
        """
        初始化交易参数
        
        Args:
            symbol (str): 交易对，如 'ETH/USDT'
            min_drop (float): 最小跌幅要求，默认0.01 (1%)
            min_rebound (float): 最小反弹要求，默认0.002 (0.2%)
            take_profit (float): 止盈比例，默认0.008 (0.8%)
            stop_loss (float): 止损比例，默认0.003 (0.3%)
            max_hold_time (int): 最大持仓时间(秒)，默认300秒(5分钟)
            test_mode (bool): 是否为测试模式，默认False
        """
        self.symbol = symbol
        self.min_drop = min_drop
        self.min_rebound = min_rebound
        self.take_profit = take_profit
        self.stop_loss = stop_loss
        self.max_hold_time = max_hold_time
        self.test_mode = test_mode

        # 设置合约交易对
        base, quote = symbol.split('/')
        self.contract_symbol = f"{base}{quote}"  # Bybit格式: ETHUSDT
        self.base_currency = base

        # 初始化交易所连接
        self.exchange = ccxtpro.bybit({
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

        # 用于存储价格历史数据
        self.price_history = deque(maxlen=120)  # 存储2分钟的价格数据
        self.lowest_price = None
        self.lowest_price_time = None
        self.entry_price = None
        self.entry_time = None
        self.position_amount = None
        self.position_side = None
        self.order_id = None

        # 交易统计
        self.trade_count = 0
        self.win_count = 0
        self.loss_count = 0
        self.total_profit = 0
        self.total_fee = 0
        self.trade_records = []

        # 用于控制WebSocket订阅
        self.ws_running = False
        
        # 用于记录最大跌幅和反弹
        self.max_drop = 0
        self.max_rebound = 0
        self.max_drop_time = None
        self.max_rebound_time = None

    async def get_max_leverage(self):
        """
        获取Bybit交易所支持的最大杠杆倍数
        
        Returns:
            int: 最大杠杆倍数
        """
        try:
            # 获取交易对信息
            response = await self.exchange.publicGetV5MarketInstrumentsInfo({
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

    async def initialize(self):
        """
        初始化交易环境，包括设置合约参数和检查账户余额
        """
        try:
            # 获取最大杠杆倍数
            max_leverage = await self.get_max_leverage()
            
            # 获取当前杠杆倍数
            try:
                positions = await self.exchange.fetch_positions([self.contract_symbol])
                current_leverage = None
                for position in positions:
                    if position['info']['symbol'] == self.contract_symbol:
                        current_leverage = int(float(position.get('leverage', 0)))
                        break
                
                # 如果当前杠杆倍数不是最大杠杆倍数，则设置
                if current_leverage != max_leverage:
                    await self.exchange.set_leverage(max_leverage, self.contract_symbol)
                    logger.info(f"设置合约杠杆倍数为: {max_leverage}倍")
                else:
                    logger.info(f"当前已经是最大杠杆倍数: {max_leverage}倍，无需调整")
            except Exception as e:
                if "leverage not modified" in str(e).lower():
                    logger.info(f"杠杆倍数已经是 {max_leverage}倍，无需修改")
                else:
                    # 如果获取当前杠杆倍数失败，直接设置最大杠杆倍数
                    await self.exchange.set_leverage(max_leverage, self.contract_symbol)
                    logger.info(f"设置合约杠杆倍数为: {max_leverage}倍")

            # 获取账户余额
            balance = await self.exchange.fetch_balance()
            usdt_balance = balance.get('USDT', {}).get('free', 0)
            logger.info(f"账户USDT余额: {usdt_balance}")

            logger.info(f"初始化完成: 交易对={self.contract_symbol}, "
                       f"最小跌幅={self.min_drop*100}%, 最小反弹={self.min_rebound*100}%, "
                       f"止盈={self.take_profit*100}%, 止损={self.stop_loss*100}%, "
                       f"最大持仓时间={self.max_hold_time}秒")

        except Exception as e:
            logger.error(f"初始化失败: {str(e)}")
            raise

    async def monitor_price_and_trade(self):
        """
        监控价格变化并执行交易
        """
        try:
            self.ws_running = True
            logger.info(f"开始监控{self.symbol}价格变化")

            while self.ws_running:
                try:
                    # 订阅订单簿
                    orderbook = await self.exchange.watch_order_book(self.contract_symbol)
                    
                    # 获取当前价格（使用卖一价作为参考）
                    current_price = float(orderbook['asks'][0][0])
                    current_time = time.time()
                    
                    # 记录价格历史
                    self.price_history.append((current_time, current_price))
                    
                    # 更新最低价
                    if self.lowest_price is None or current_price < self.lowest_price:
                        self.lowest_price = current_price
                        self.lowest_price_time = current_time
                    
                    # 如果没有持仓，检查是否满足开仓条件
                    if not self.position_amount:
                        await self.check_entry_conditions(current_price, current_time)
                    else:
                        # 有持仓，检查是否满足平仓条件
                        await self.check_exit_conditions(current_price, current_time)

                except Exception as e:
                    logger.error(f"监控价格时出错: {str(e)}")
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"执行交易时出错: {str(e)}")
            raise
        finally:
            self.ws_running = False
            await self.exchange.close()

    async def check_entry_conditions(self, current_price: float, current_time: float):
        """
        检查是否满足开仓条件
        
        Args:
            current_price: 当前价格
            current_time: 当前时间戳
        """
        try:
            # 记录开始检查的时间
            check_start_time = time.time()
            
            # 确保有足够的价格历史数据
            if len(self.price_history) < 2:
                return

            # 获取2分钟前的价格
            two_min_ago = current_time - 120
            while self.price_history and self.price_history[0][0] < two_min_ago:
                self.price_history.popleft()

            if not self.price_history:
                return

            start_price = self.price_history[0][1]
            
            # 计算跌幅
            price_drop = (start_price - self.lowest_price) / start_price
            
            # 计算从最低点的反弹幅度
            rebound = (current_price - self.lowest_price) / self.lowest_price
            
            # 更新最大跌幅和反弹记录
            if price_drop > self.max_drop:
                self.max_drop = price_drop
                self.max_drop_time = current_time
                logger.info(f"【新最大跌幅】{price_drop*100:.4f}% (时间: {datetime.fromtimestamp(current_time).strftime('%H:%M:%S.%f')[:-3]})")
            
            if rebound > self.max_rebound:
                self.max_rebound = rebound
                self.max_rebound_time = current_time
                logger.info(f"【新最大反弹】{rebound*100:.4f}% (时间: {datetime.fromtimestamp(current_time).strftime('%H:%M:%S.%f')[:-3]})")
            
            # 记录价格检查完成时间
            check_end_time = time.time()
            check_delay = (check_end_time - check_start_time) * 1000  # 转换为毫秒
            
            # 打印详细的调试信息
            logger.debug("=" * 50)
            logger.debug("【价格检查信息】")
            logger.debug(f"检查耗时: {check_delay:.2f}ms")
            logger.debug(f"当前价格: {current_price:.8f}")
            logger.debug(f"2分钟前价格: {start_price:.8f}")
            logger.debug(f"最低价格: {self.lowest_price:.8f} (时间: {datetime.fromtimestamp(self.lowest_price_time).strftime('%H:%M:%S.%f')[:-3]})")
            logger.debug(f"跌幅: {price_drop*100:.4f}%, 反弹: {rebound*100:.4f}%")
            logger.debug(f"历史最大跌幅: {self.max_drop*100:.4f}% (时间: {datetime.fromtimestamp(self.max_drop_time).strftime('%H:%M:%S.%f')[:-3] if self.max_drop_time else 'N/A'})")
            logger.debug(f"历史最大反弹: {self.max_rebound*100:.4f}% (时间: {datetime.fromtimestamp(self.max_rebound_time).strftime('%H:%M:%S.%f')[:-3] if self.max_rebound_time else 'N/A'})")
            logger.debug("=" * 50)
            
            # 检查是否满足开仓条件
            if price_drop >= self.min_drop and rebound >= self.min_rebound:
                logger.info(f"满足开仓条件: 跌幅 {price_drop*100:.2f}% >= {self.min_drop*100}%, "
                           f"反弹 {rebound*100:.2f}% >= {self.min_rebound*100}%")
                
                # 记录开始获取订单簿的时间
                orderbook_start_time = time.time()
                
                # 计算开仓数量
                orderbook = await self.exchange.fetch_order_book(self.contract_symbol)
                ask_volume = float(orderbook['asks'][0][1])
                bid_volume = float(orderbook['bids'][0][1])
                usdt_balance = float((await self.exchange.fetch_balance())['USDT']['free'])
                
                # 记录获取订单簿完成时间
                orderbook_end_time = time.time()
                orderbook_delay = (orderbook_end_time - orderbook_start_time) * 1000
                
                # 打印订单簿信息
                logger.debug("=" * 50)
                logger.debug("【订单簿信息】")
                logger.debug(f"获取耗时: {orderbook_delay:.2f}ms")
                logger.debug(f"买一价: {orderbook['bids'][0][0]:.8f}, 数量: {bid_volume:.8f}")
                logger.debug(f"卖一价: {orderbook['asks'][0][0]:.8f}, 数量: {ask_volume:.8f}")
                logger.debug("=" * 50)
                
                # 使用卖一数量或10USDT可购买的数量中的较小值
                amount_by_volume = ask_volume
                amount_by_usdt = 10 / current_price
                trade_amount = min(amount_by_volume, amount_by_usdt)
                
                # 记录下单前时间
                order_start_time = time.time()
                total_delay = (order_start_time - check_start_time) * 1000
                logger.debug("=" * 50)
                logger.debug("【下单前信息】")
                logger.debug(f"总延迟: {total_delay:.2f}ms")
                logger.debug("=" * 50)
                
                # 执行开仓
                if not self.test_mode:
                    order = await self.exchange.create_market_buy_order(
                        symbol=self.contract_symbol,
                        amount=trade_amount,
                        params={
                            "category": "linear",
                            "positionIdx": 0,  # 单向持仓
                            "reduceOnly": False
                        }
                    )
                    
                    # 记录下单完成时间
                    order_end_time = time.time()
                    order_delay = (order_end_time - order_start_time) * 1000
                    total_delay = (order_end_time - check_start_time) * 1000
                    
                    logger.debug("=" * 50)
                    logger.debug("【下单完成信息】")
                    logger.debug(f"下单耗时: {order_delay:.2f}ms")
                    logger.debug(f"总延迟: {total_delay:.2f}ms")
                    logger.debug(f"订单返回时间: {datetime.fromtimestamp(order_end_time).strftime('%H:%M:%S.%f')[:-3]}")
                    logger.debug("=" * 50)
                    
                    # 记录开仓信息
                    self.position_amount = float(order['filled'])
                    self.entry_price = float(order['average'])
                    self.entry_time = current_time
                    self.order_id = order['id']
                    
                    logger.info(f"开仓成功: 数量 {self.position_amount}, 价格 {self.entry_price}")
                else:
                    logger.info(f"测试模式 - 模拟开仓: 数量 {trade_amount}, 价格 {current_price}")
                    self.position_amount = trade_amount
                    self.entry_price = current_price
                    self.entry_time = current_time

        except Exception as e:
            logger.error(f"检查开仓条件时出错: {str(e)}")
            import traceback
            logger.debug("=" * 50)
            logger.debug("【错误信息】")
            logger.debug(f"错误堆栈:\n{traceback.format_exc()}")
            logger.debug("=" * 50)

    async def check_exit_conditions(self, current_price: float, current_time: float):
        """
        检查是否满足平仓条件
        
        Args:
            current_price: 当前价格
            current_time: 当前时间戳
        """
        try:
            if not self.position_amount or not self.entry_price:
                return

            # 计算持仓时间
            hold_time = current_time - self.entry_time
            
            # 计算价格变化
            price_change = (current_price - self.entry_price) / self.entry_price
            
            # 检查是否满足平仓条件
            should_exit = False
            exit_reason = ""
            
            if price_change >= self.take_profit:
                should_exit = True
                exit_reason = f"止盈: 涨幅 {price_change*100:.2f}% >= {self.take_profit*100}%"
            elif price_change <= -self.stop_loss:
                should_exit = True
                exit_reason = f"止损: 跌幅 {abs(price_change)*100:.2f}% >= {self.stop_loss*100}%"
            elif hold_time >= self.max_hold_time:
                should_exit = True
                exit_reason = f"超时: 持仓时间 {hold_time:.0f}秒 >= {self.max_hold_time}秒"
            
            if should_exit:
                logger.info(f"满足平仓条件: {exit_reason}")
                
                # 执行平仓
                if not self.test_mode:
                    order = await self.exchange.create_market_sell_order(
                        symbol=self.contract_symbol,
                        amount=self.position_amount,
                        params={
                            "category": "linear",
                            "positionIdx": 0,
                            "reduceOnly": True
                        }
                    )
                    
                    # 计算交易结果
                    exit_price = float(order['average'])
                    profit = (exit_price - self.entry_price) * self.position_amount
                    fees = sum(float(fee['cost']) for fee in order.get('fees', []))
                    
                    # 更新交易统计
                    self.trade_count += 1
                    if profit > 0:
                        self.win_count += 1
                    else:
                        self.loss_count += 1
                    self.total_profit += profit
                    self.total_fee += fees
                    
                    # 记录交易
                    trade_record = {
                        'entry_time': datetime.fromtimestamp(self.entry_time).strftime('%Y-%m-%d %H:%M:%S'),
                        'exit_time': datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S'),
                        'entry_price': self.entry_price,
                        'exit_price': exit_price,
                        'amount': self.position_amount,
                        'profit': profit,
                        'fees': fees,
                        'hold_time': hold_time,
                        'exit_reason': exit_reason
                    }
                    self.trade_records.append(trade_record)
                    
                    logger.info(f"平仓成功: 数量 {self.position_amount}, 价格 {exit_price}, "
                               f"盈亏 {profit:.2f} USDT, 手续费 {fees:.2f} USDT")
                else:
                    profit = (current_price - self.entry_price) * self.position_amount
                    logger.info(f"测试模式 - 模拟平仓: 数量 {self.position_amount}, 价格 {current_price}, "
                               f"盈亏 {profit:.2f} USDT")
                
                # 重置持仓信息
                self.position_amount = None
                self.entry_price = None
                self.entry_time = None
                self.order_id = None
                self.lowest_price = None
                self.lowest_price_time = None

        except Exception as e:
            logger.error(f"检查平仓条件时出错: {str(e)}")

    def print_trading_summary(self):
        """
        打印交易统计信息
        """
        logger.info("=" * 50)
        logger.info("【交易统计】")
        logger.info(f"- 总交易次数: {self.trade_count}")
        if self.trade_count > 0:
            logger.info(f"- 盈利次数: {self.win_count}")
            logger.info(f"- 亏损次数: {self.loss_count}")
            logger.info(f"- 胜率: {self.win_count/self.trade_count*100:.2f}%")
            logger.info(f"- 总盈亏: {self.total_profit:.2f} USDT")
            logger.info(f"- 总手续费: {self.total_fee:.2f} USDT")
            logger.info(f"- 净利润: {self.total_profit - self.total_fee:.2f} USDT")
        logger.info("=" * 50)
        
        logger.info("=" * 50)
        logger.info("【价格统计】")
        logger.info(f"- 历史最大跌幅: {self.max_drop*100:.4f}% (时间: {datetime.fromtimestamp(self.max_drop_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if self.max_drop_time else 'N/A'})")
        logger.info(f"- 历史最大反弹: {self.max_rebound*100:.4f}% (时间: {datetime.fromtimestamp(self.max_rebound_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if self.max_rebound_time else 'N/A'})")
        logger.info("=" * 50)
        
        if self.trade_records:
            logger.info("=" * 50)
            logger.info("【最近交易记录】")
            for record in self.trade_records[-5:]:  # 只显示最近5笔交易
                logger.info(f"- 开仓时间: {record['entry_time']}")
                logger.info(f"  平仓时间: {record['exit_time']}")
                logger.info(f"  开仓价格: {record['entry_price']:.8f}")
                logger.info(f"  平仓价格: {record['exit_price']:.8f}")
                logger.info(f"  交易数量: {record['amount']:.8f}")
                logger.info(f"  持仓时间: {record['hold_time']:.0f}秒")
                logger.info(f"  盈亏: {record['profit']:.2f} USDT")
                logger.info(f"  手续费: {record['fees']:.2f} USDT")
                logger.info(f"  平仓原因: {record['exit_reason']}")
                logger.info("  " + "-" * 30)
            logger.info("=" * 50)


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Bybit合约插针反弹交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-d', '--min-drop', type=float, default=0.01, help='最小跌幅要求，默认0.01 (1%)')
    parser.add_argument('-r', '--min-rebound', type=float, default=0.002, help='最小反弹要求，默认0.002 (0.2%)')
    parser.add_argument('-p', '--take-profit', type=float, default=0.006, help='止盈比例，默认0.006 (0.6%)')
    parser.add_argument('-l', '--stop-loss', type=float, default=0.003, help='止损比例，默认0.003 (0.3%)')
    parser.add_argument('-t', '--max-hold-time', type=int, default=300, help='最大持仓时间(秒)，默认300秒(5分钟)')
    parser.add_argument('--test', action='store_true', help='启用测试模式，不实际下单')
    parser.add_argument('--debug', action='store_true', help='启用调试日志')
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
    try:
        # 创建交易器实例
        trader = PinReboundTrader(
            symbol=args.symbol,
            min_drop=args.min_drop,
            min_rebound=args.min_rebound,
            take_profit=args.take_profit,
            stop_loss=args.stop_loss,
            max_hold_time=args.max_hold_time,
            test_mode=args.test
        )
        
        # 初始化交易环境
        await trader.initialize()
        
        # 开始监控价格并执行交易
        await trader.monitor_price_and_trade()

    except KeyboardInterrupt:
        logger.info("收到退出信号，正在停止交易...")
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        return 1
    finally:
        # 打印交易统计
        if trader:
            trader.print_trading_summary()
            try:
                await trader.exchange.close()
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