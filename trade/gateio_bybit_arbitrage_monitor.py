#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io和Bybit套利机会检查脚本

此脚本实现以下功能：
1. 获取两个交易所共同支持的交易对
2. 获取最新订单簿数据
3. 计算实际价差（考虑手续费）
4. 筛选出价差大于0.16%且小于10%的交易对
5. 显示合约资金费率信息
"""

import sys
import os
import asyncio
import ccxt.pro as ccxtpro
import logging
import time
import requests
from decimal import Decimal
from typing import Dict, List, Set
from datetime import datetime

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import bybit_api_key, bybit_api_secret, gateio_api_secret, gateio_api_key, proxies

# 配置参数
MIN_SPREAD = 0.0016  # 最小价差要求 0.16%
MAX_SPREAD = 0.10    # 最大价差限制 10%
CONTRACT_FEE = 0.0006  # Bybit合约手续费 0.06%
SPOT_FEE = 0.001  # Gate.io现货手续费 0.1%
TOTAL_FEE = CONTRACT_FEE + SPOT_FEE  # 总手续费

class ArbitrageChecker:
    def __init__(self):
        """
        初始化套利检查器
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
        
        # 初始化会话
        self.session = requests.Session()
        if proxies:
            self.session.proxies = proxies

    async def get_bybit_futures_funding_rate(self, token):
        """
        获取Bybit合约资金费率
        """
        exchange = 'Bybit'
        try:
            url = "https://api.bybit.com/v5/market/tickers"
            params = {
                "category": "linear",
                "symbol": f"{token}USDT"
            }
            response = requests.get(url, params=params)
            if response.status_code != 200:
                logger.error(f"bybit get future failed, url: {url}, status: {response.status_code}, response: {response.text}")
                return {}
                
            data = response.json()

            if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
                for item in data["result"]["list"]:
                    if "fundingRate" in item:
                        fundingRate = float(item["fundingRate"]) * 100
                        # 根据资金费率正负设置结算间隔
                        fundingIntervalHours = 8 if fundingRate > 0 else 4
                        fundingIntervalHoursText = str(fundingIntervalHours)
                                
                        return {
                            "exchange": exchange,
                            'fundingTime': int(item["nextFundingTime"]),
                            'fundingRate': float(item["fundingRate"]) * 100,  # 转换为百分比
                            'markPrice': float(item["markPrice"]),
                            'fundingIntervalHours': fundingIntervalHours,
                            'fundingIntervalHoursText': fundingIntervalHoursText,
                        }
            return {}
        except Exception as e:
            logger.error(f"获取{exchange} {token}合约资金费率时出错: {str(e)}")
        return {}

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
            
            logger.info(f"成功加载 {len(self.symbols)} 个共同支持的交易对")
            logger.info(f"交易对列表: {self.symbols}")
            
        except Exception as e:
            logger.error(f"加载市场数据时出错: {str(e)}")
            raise

    async def check_spreads(self):
        """检查所有交易对的价差"""
        try:
            # 创建任务列表
            tasks = []
            for symbol in self.symbols:
                contract_symbol = symbol.replace('/', '')
                tasks.extend([
                    asyncio.create_task(self.gateio.fetch_order_book(symbol)),
                    asyncio.create_task(self.bybit.fetch_order_book(contract_symbol))
                ])

            # 等待所有订单簿数据获取完成
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 打印表头
            print(f"\n{'='*150}")
            print(f"{'交易对':<10} {'Gate.io卖一价':<15} {'Bybit买一价':<15} {'价差(%)':<10} {'Gate.io卖一量':<15} {'Bybit买一量':<15} {'资金费率(%)':<15} {'下次结算':<20} {'结算间隔':<10}")
            print(f"{'-'*150}")

            # 处理每个交易对的结果
            for i in range(0, len(results), 2):
                symbol = self.symbols[i//2]
                gateio_ob = results[i]
                bybit_ob = results[i+1]

                if isinstance(gateio_ob, Exception) or isinstance(bybit_ob, Exception):
                    logger.error(f"获取 {symbol} 订单簿数据时出错")
                    continue

                try:
                    # 获取买卖价格和数量
                    gateio_ask = Decimal(str(gateio_ob['asks'][0][0]))  # Gate.io卖一价
                    gateio_ask_vol = Decimal(str(gateio_ob['asks'][0][1]))  # Gate.io卖一量
                    bybit_bid = Decimal(str(bybit_ob['bids'][0][0]))  # Bybit买一价
                    bybit_bid_vol = Decimal(str(bybit_ob['bids'][0][1]))  # Bybit买一量

                    # 计算实际价差（考虑手续费）
                    actual_spread = (bybit_bid * (1 - Decimal(str(CONTRACT_FEE)))) / (gateio_ask * (1 + Decimal(str(SPOT_FEE)))) - 1
                    spread_percent = float(actual_spread * 100)

                    # 只打印满足价差要求的交易对（大于0.16%且小于10%）
                    if MIN_SPREAD < actual_spread < Decimal(str(MAX_SPREAD)):
                        # 获取资金费率信息
                        funding_info = await self.get_bybit_futures_funding_rate(symbol.replace('/USDT', ''))
                        
                        # 格式化下次结算时间
                        next_funding_time = datetime.fromtimestamp(funding_info.get('fundingTime', 0)/1000).strftime('%Y-%m-%d %H:%M:%S') if funding_info else 'N/A'
                        
                        # print(f"{symbol:<10} {float(gateio_ask):<15.4f} {float(bybit_bid):<15.4f} {spread_percent:<10.4f} {float(gateio_ask_vol):<15.4f} {float(bybit_bid_vol):<15.4f} {funding_info.get('fundingRate', 0):<15.4f} {next_funding_time:<20} {funding_info.get('fundingIntervalHoursText', 'N/A'):<10}")
                        
                        # 记录详细日志
                        logger.info(
                            f"发现套利机会! 交易对: {symbol}, Gate.io卖一价: {gateio_ask}, Bybit买一价: {bybit_bid}, "
                            f"实际价差: {spread_percent:.4f}%, Gate.io卖一量: {gateio_ask_vol}, Bybit买一量: {bybit_bid_vol}, "
                            f"资金费率: {funding_info.get('fundingRate', 0):.4f}%, 下次结算: {next_funding_time}, "
                            f"结算间隔: {funding_info.get('fundingIntervalHoursText', 'N/A')}小时"
                        )

                except Exception as e:
                    logger.error(f"处理 {symbol} 价差时出错: {str(e)}")

            print(f"{'='*150}\n")

        except Exception as e:
            logger.error(f"检查价差时出错: {str(e)}")
            raise

async def main():
    """
    主函数
    """
    # 设置日志级别
    logger.setLevel(logging.INFO)
    
    try:
        # 创建检查器
        checker = ArbitrageChecker()
        
        # 加载市场数据
        await checker.load_markets()
        
        # 检查价差
        await checker.check_spreads()
        
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        return 1
    finally:
        # 确保关闭交易所连接
        if 'checker' in locals():
            await asyncio.gather(
                checker.gateio.close(),
                checker.bybit.close()
            )

    return 0

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 