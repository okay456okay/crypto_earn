#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Bybit资金费率扫描器

此脚本实现以下功能：
1. 获取Bybit所有合约交易对
2. 筛选资金费率大于1.0%或小于-1.0%的交易对
3. 筛选24小时交易量大于100万的交易对
4. 输出符合条件的交易对信息
"""

import sys
import os
import asyncio
import ccxt.async_support as ccxt
from datetime import datetime
from decimal import Decimal
import logging

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import bybit_api_key, bybit_api_secret, proxies

class BybitScanner:
    def __init__(self):
        """初始化Bybit扫描器"""
        self.exchange = ccxt.bybit({
            'apiKey': bybit_api_key,
            'secret': bybit_api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'swap',  # 使用永续合约
            },
            'proxies': proxies,
        })

    async def get_all_symbols(self):
        """获取所有合约交易对"""
        try:
            markets = await self.exchange.load_markets()
            # 只保留USDT永续合约
            symbols = [symbol for symbol in markets.keys() 
                      if symbol.endswith('/USDT:USDT') and 'swap' in markets[symbol]['type']]
            return symbols
        except Exception as e:
            logger.error(f"获取交易对列表失败: {str(e)}")
            return []

    async def get_funding_rate(self, symbol):
        """获取指定交易对的资金费率"""
        try:
            funding_rate = await self.exchange.fetch_funding_rate(symbol)
            return funding_rate['fundingRate'] * 100  # 转换为百分比
        except Exception as e:
            logger.error(f"获取{symbol}资金费率失败: {str(e)}")
            return None

    async def get_24h_volume(self, symbol):
        """获取24小时交易量"""
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            return ticker['quoteVolume']  # USDT计价
        except Exception as e:
            logger.error(f"获取{symbol}交易量失败: {str(e)}")
            return 0

    async def scan_markets(self):
        """扫描所有市场"""
        try:
            symbols = await self.get_all_symbols()
            logger.info(f"开始扫描 {len(symbols)} 个交易对...")

            results = []
            for symbol in symbols:
                # 并行获取资金费率和交易量
                funding_rate, volume = await asyncio.gather(
                    self.get_funding_rate(symbol),
                    self.get_24h_volume(symbol)
                )

                if funding_rate is None or volume is None:
                    continue

                # 检查是否满足条件
                if (abs(funding_rate) >= 1.0 and volume >= 1000000):
                    results.append({
                        'symbol': symbol,
                        'funding_rate': funding_rate,
                        'volume': volume
                    })

            return results

        except Exception as e:
            logger.error(f"扫描市场时出错: {str(e)}")
            return []

    async def close(self):
        """关闭交易所连接"""
        await self.exchange.close()

def print_results(results):
    """打印扫描结果"""
    if not results:
        logger.info("没有找到符合条件的交易对")
        return

    logger.info("\n=== 符合条件的交易对 ===")
    logger.info(f"找到 {len(results)} 个符合条件的交易对")
    logger.info("\n{:<15} {:<15} {:<15}".format("交易对", "资金费率(%)", "24h交易量(USDT)"))
    logger.info("-" * 45)

    for result in sorted(results, key=lambda x: abs(x['funding_rate']), reverse=True):
        logger.info("{:<15} {:<15.4f} {:<15.2f}".format(
            result['symbol'],
            result['funding_rate'],
            result['volume']
        ))

async def main():
    """主函数"""
    scanner = BybitScanner()
    try:
        results = await scanner.scan_markets()
        print_results(results)
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
    finally:
        await scanner.close()

if __name__ == "__main__":
    # 设置日志级别
    logger.setLevel(logging.INFO)
    
    # 运行主函数
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close() 