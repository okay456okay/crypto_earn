#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
获取Binance和Bybit合约交易对信息

此脚本实现以下功能：
1. 获取所有合约交易对的最大杠杆倍数
2. 获取资金费率
3. 获取下次结算时间
4. 获取近24小时合约交易量
5. 获取当前价格（买一、卖一）和数量
"""

import sys
import os
import asyncio
import ccxt.pro as ccxtpro
import logging
from datetime import datetime
import pandas as pd
from typing import Dict, List, Tuple
import argparse

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import binance_api_key, binance_api_secret, bybit_api_key, bybit_api_secret, proxies

class ContractInfoFetcher:
    def __init__(self):
        """初始化交易所连接"""
        self.binance = ccxtpro.binance({
            'apiKey': binance_api_key,
            'secret': binance_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
            'options': {
                'defaultType': 'future',
            }
        })

        self.bybit = ccxtpro.bybit({
            'apiKey': bybit_api_key,
            'secret': bybit_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
            'options': {
                'defaultType': 'linear',
            }
        })

    async def fetch_binance_info(self) -> List[Dict]:
        """获取Binance合约信息"""
        try:
            # 获取所有合约交易对
            markets = await self.binance.load_markets()
            futures_markets = {k: v for k, v in markets.items() if v['type'] == 'future'}

            results = []
            for symbol, market in futures_markets.items():
                try:
                    # 获取杠杆信息
                    leverage_info = await self.binance.fapiPrivateGetLeverageBracket({
                        'symbol': symbol
                    })
                    max_leverage = int(leverage_info[0]['brackets'][0]['initialLeverage'])

                    # 获取资金费率
                    funding_rate = await self.binance.fapiPublicGetPremiumIndex({
                        'symbol': symbol
                    })
                    funding_rate = float(funding_rate['lastFundingRate'])
                    next_funding_time = funding_rate['nextFundingTime']

                    # 获取24小时交易量
                    ticker = await self.binance.fetch_ticker(symbol)
                    volume_24h = float(ticker['quoteVolume'])

                    # 获取订单簿
                    orderbook = await self.binance.fetch_order_book(symbol)
                    bid_price = float(orderbook['bids'][0][0])
                    bid_amount = float(orderbook['bids'][0][1])
                    ask_price = float(orderbook['asks'][0][0])
                    ask_amount = float(orderbook['asks'][0][1])

                    results.append({
                        'exchange': 'Binance',
                        'symbol': symbol,
                        'max_leverage': max_leverage,
                        'funding_rate': funding_rate,
                        'next_funding_time': datetime.fromtimestamp(next_funding_time/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        'volume_24h': volume_24h,
                        'bid_price': bid_price,
                        'bid_amount': bid_amount,
                        'ask_price': ask_price,
                        'ask_amount': ask_amount
                    })

                except Exception as e:
                    logger.error(f"获取Binance {symbol} 信息时出错: {str(e)}")
                    continue

            return results

        except Exception as e:
            logger.error(f"获取Binance信息时出错: {str(e)}")
            return []

    async def fetch_bybit_info(self) -> List[Dict]:
        """获取Bybit合约信息"""
        try:
            # 获取所有合约交易对
            markets = await self.bybit.load_markets()
            linear_markets = {k: v for k, v in markets.items() if v['type'] == 'linear'}

            results = []
            for symbol, market in linear_markets.items():
                try:
                    # 获取杠杆信息
                    instrument_info = await self.bybit.publicGetV5MarketInstrumentsInfo({
                        'category': 'linear',
                        'symbol': symbol
                    })
                    max_leverage = int(float(instrument_info['result']['list'][0]['leverageFilter']['maxLeverage']))

                    # 获取资金费率
                    funding_rate = await self.bybit.publicGetV5MarketFundingHistory({
                        'category': 'linear',
                        'symbol': symbol,
                        'limit': 1
                    })
                    funding_rate = float(funding_rate['result']['list'][0]['fundingRate'])
                    next_funding_time = funding_rate['result']['list'][0]['fundingRateTimestamp']

                    # 获取24小时交易量
                    ticker = await self.bybit.fetch_ticker(symbol)
                    volume_24h = float(ticker['quoteVolume'])

                    # 获取订单簿
                    orderbook = await self.bybit.fetch_order_book(symbol)
                    bid_price = float(orderbook['bids'][0][0])
                    bid_amount = float(orderbook['bids'][0][1])
                    ask_price = float(orderbook['asks'][0][0])
                    ask_amount = float(orderbook['asks'][0][1])

                    results.append({
                        'exchange': 'Bybit',
                        'symbol': symbol,
                        'max_leverage': max_leverage,
                        'funding_rate': funding_rate,
                        'next_funding_time': datetime.fromtimestamp(next_funding_time/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        'volume_24h': volume_24h,
                        'bid_price': bid_price,
                        'bid_amount': bid_amount,
                        'ask_price': ask_price,
                        'ask_amount': ask_amount
                    })

                except Exception as e:
                    logger.error(f"获取Bybit {symbol} 信息时出错: {str(e)}")
                    continue

            return results

        except Exception as e:
            logger.error(f"获取Bybit信息时出错: {str(e)}")
            return []

    async def fetch_all_info(self) -> pd.DataFrame:
        """获取所有交易所的合约信息"""
        try:
            # 并行获取所有交易所的信息
            binance_info, bybit_info = await asyncio.gather(
                self.fetch_binance_info(),
                self.fetch_bybit_info()
            )

            # 合并所有数据
            all_info = binance_info + bybit_info

            # 转换为DataFrame
            df = pd.DataFrame(all_info)

            # 按交易量排序
            df = df.sort_values('volume_24h', ascending=False)

            return df

        except Exception as e:
            logger.error(f"获取所有信息时出错: {str(e)}")
            return pd.DataFrame()

    async def close(self):
        """关闭交易所连接"""
        await asyncio.gather(
            self.binance.close(),
            self.bybit.close()
        )

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='获取Binance和Bybit合约信息')
    parser.add_argument('-o', '--output', type=str, default='contract_info.csv',
                      help='输出文件名，默认为contract_info.csv')
    parser.add_argument('-n', '--top-n', type=int, default=20,
                      help='显示交易量最大的前N个交易对，默认为20')
    parser.add_argument('-d', '--debug', action='store_true',
                      help='启用调试日志')
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
        # 创建信息获取器
        fetcher = ContractInfoFetcher()

        # 获取所有信息
        df = await fetcher.fetch_all_info()

        if not df.empty:
            # 保存到CSV文件
            df.to_csv(args.output, index=False)
            logger.info(f"数据已保存到 {args.output}")

            # 显示前N个交易对的信息
            print("\n交易量最大的前{}个交易对信息:".format(args.top_n))
            print(df.head(args.top_n).to_string(index=False))

        else:
            logger.error("未能获取到任何合约信息")

    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
    finally:
        # 确保关闭交易所连接
        if 'fetcher' in locals():
            await fetcher.close()

if __name__ == "__main__":
    # 设置并启动事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close() 