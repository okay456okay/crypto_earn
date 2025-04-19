#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io和Bybit套利机会监控脚本

此脚本实现以下功能：
1. 实时监控多个交易对的订单簿数据
2. 计算实际价差（考虑手续费）
3. 筛选出价差大于0.16%的交易对
4. 提供实时监控和日志记录
"""

import sys
import os
import asyncio
import ccxt.pro as ccxtpro
import logging
from decimal import Decimal
from typing import Dict, List, Set
import time
from datetime import datetime

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import bybit_api_key, bybit_api_secret, gateio_api_secret, gateio_api_key, proxies

# 配置参数
MIN_SPREAD = 0.0016  # 最小价差要求 0.16%
CONTRACT_FEE = 0.0006  # Bybit合约手续费 0.06%
SPOT_FEE = 0.001  # Gate.io现货手续费 0.1%
TOTAL_FEE = CONTRACT_FEE + SPOT_FEE  # 总手续费

class ArbitrageMonitor:
    def __init__(self):
        """
        初始化套利监控器
        """
        # 初始化交易所
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
            },
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
        })

        # 初始化交易对列表
        self.symbols = []
        self.contract_symbols = []
        
        # 存储订单簿数据
        self.orderbooks = {
            'gateio': {},
            'bybit': {}
        }

        # 存储价差数据
        self.spreads = {}
        
        # 控制WebSocket订阅
        self.ws_running = False
        self.last_update_time = {}

    async def load_markets(self):
        """加载两个交易所的市场数据并找出共同支持的交易对"""
        try:
            # 加载市场数据
            await self.gateio.load_markets()
            await self.bybit.load_markets()

            # 获取Gate.io的现货交易对
            gateio_symbols = set(self.gateio.markets.keys())
            
            # 获取Bybit的合约交易对
            bybit_symbols = set(self.bybit.markets.keys())
            
            # 找出共同支持的交易对
            common_symbols = gateio_symbols.intersection(bybit_symbols)
            
            # 过滤出USDT交易对
            usdt_symbols = [s for s in common_symbols if s.endswith('/USDT')]
            
            # 设置交易对列表
            self.symbols = sorted(usdt_symbols)
            self.contract_symbols = [s.replace('/', '') for s in self.symbols]
            
            # 初始化数据结构
            self.orderbooks['gateio'] = {symbol: None for symbol in self.symbols}
            self.orderbooks['bybit'] = {symbol: None for symbol in self.contract_symbols}
            self.spreads = {symbol: None for symbol in self.symbols}
            self.last_update_time = {symbol: 0 for symbol in self.symbols}
            
            logger.info(f"成功加载 {len(self.symbols)} 个共同支持的交易对")
            logger.info(f"交易对列表: {self.symbols}")
            
        except Exception as e:
            logger.error(f"加载市场数据时出错: {str(e)}")
            raise

    async def subscribe_orderbooks(self):
        """订阅所有交易对的订单簿数据"""
        try:
            self.ws_running = True
            while self.ws_running:
                try:
                    # 创建订阅任务
                    tasks = []
                    for symbol in self.symbols:
                        contract_symbol = symbol.replace('/', '')
                        tasks.extend([
                            asyncio.create_task(self.gateio.watch_order_book(symbol)),
                            asyncio.create_task(self.bybit.watch_order_book(contract_symbol))
                        ])

                    # 等待任意一个订单簿更新
                    done, pending = await asyncio.wait(
                        tasks,
                        return_when=asyncio.FIRST_COMPLETED
                    )

                    # 处理完成的任务
                    for task in done:
                        try:
                            ob = task.result()
                            symbol = ob['symbol']
                            
                            # 判断是哪个交易所的订单簿
                            if '/' in symbol:  # Gate.io订单簿
                                self.orderbooks['gateio'][symbol] = ob
                                self.last_update_time[symbol] = time.time()
                            else:  # Bybit订单簿
                                self.orderbooks['bybit'][symbol] = ob
                                self.last_update_time[symbol.replace('USDT', '/USDT')] = time.time()

                            # 检查价差
                            await self.check_spread(symbol if '/' in symbol else symbol.replace('USDT', '/USDT'))

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

    async def check_spread(self, symbol: str):
        """检查指定交易对的价差"""
        try:
            contract_symbol = symbol.replace('/', '')
            gateio_ob = self.orderbooks['gateio'][symbol]
            bybit_ob = self.orderbooks['bybit'][contract_symbol]

            if not gateio_ob or not bybit_ob:
                return

            # 获取买卖价格
            gateio_ask = Decimal(str(gateio_ob['asks'][0][0]))  # Gate.io卖一价
            bybit_bid = Decimal(str(bybit_ob['bids'][0][0]))  # Bybit买一价

            # 计算实际价差（考虑手续费）
            # 实际价差 = (Bybit买一价 * (1 - 合约手续费)) / (Gate.io卖一价 * (1 + 现货手续费)) - 1
            actual_spread = (bybit_bid * (1 - Decimal(str(CONTRACT_FEE)))) / (gateio_ask * (1 + Decimal(str(SPOT_FEE)))) - 1

            # 更新价差数据
            self.spreads[symbol] = float(actual_spread)

            # 如果价差大于最小要求，记录日志
            if actual_spread > Decimal(str(MIN_SPREAD)):
                logger.info(
                    f"\n{'='*50}\n"
                    f"发现套利机会!\n"
                    f"交易对: {symbol}\n"
                    f"Gate.io卖一价: {gateio_ask}\n"
                    f"Bybit买一价: {bybit_bid}\n"
                    f"实际价差: {actual_spread * 100:.4f}%\n"
                    f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"{'='*50}"
                )

        except Exception as e:
            logger.error(f"检查价差时出错: {str(e)}")

    async def print_spreads(self):
        """定期打印所有交易对的价差"""
        while self.ws_running:
            try:
                # 清屏
                print("\033c", end="")
                
                # 打印表头
                print(f"\n{'='*80}")
                print(f"{'交易对':<10} {'Gate.io卖一价':<15} {'Bybit买一价':<15} {'价差(%)':<10} {'最后更新':<20}")
                print(f"{'-'*80}")
                
                # 打印每个交易对的数据
                for symbol in self.symbols:
                    gateio_ob = self.orderbooks['gateio'][symbol]
                    contract_symbol = symbol.replace('/', '')
                    bybit_ob = self.orderbooks['bybit'][contract_symbol]
                    
                    if gateio_ob and bybit_ob:
                        gateio_ask = float(gateio_ob['asks'][0][0])
                        bybit_bid = float(bybit_ob['bids'][0][0])
                        spread = self.spreads[symbol] * 100 if self.spreads[symbol] is not None else 0
                        last_update = datetime.fromtimestamp(self.last_update_time[symbol]).strftime('%H:%M:%S')
                        
                        # 根据价差设置颜色
                        color_code = "\033[92m" if spread > MIN_SPREAD * 100 else "\033[0m"
                        print(f"{color_code}{symbol:<10} {gateio_ask:<15.4f} {bybit_bid:<15.4f} {spread:<10.4f} {last_update:<20}\033[0m")
                
                print(f"{'='*80}\n")
                
            except Exception as e:
                logger.error(f"打印价差时出错: {str(e)}")
            
            await asyncio.sleep(1)  # 每秒更新一次

    async def start_monitoring(self):
        """启动监控"""
        try:
            # 启动订单簿订阅
            subscribe_task = asyncio.create_task(self.subscribe_orderbooks())
            
            # 启动价差打印
            print_task = asyncio.create_task(self.print_spreads())
            
            # 等待任务完成
            await asyncio.gather(subscribe_task, print_task)
            
        except Exception as e:
            logger.error(f"启动监控时出错: {str(e)}")
        finally:
            self.ws_running = False

async def main():
    """
    主函数
    """
    # 设置日志级别
    logger.setLevel(logging.INFO)
    
    try:
        # 创建监控器
        monitor = ArbitrageMonitor()
        
        # 加载市场数据
        await monitor.load_markets()
        
        # 启动监控
        await monitor.start_monitoring()
        
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        return 1
    finally:
        # 确保关闭交易所连接
        if 'monitor' in locals():
            await asyncio.gather(
                monitor.gateio.close(),
                monitor.bybit.close()
            )

    return 0

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 