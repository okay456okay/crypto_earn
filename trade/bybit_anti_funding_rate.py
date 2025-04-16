#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Bybit资金费率扫描器

此脚本实现以下功能：
1. 获取Bybit所有合约交易对
2. 筛选资金费率小于-1.0%的交易对
3. 筛选24小时交易量大于200万的交易对
4. 输出符合条件的交易对信息，包括下次结算时间
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
            return {
                'rate': funding_rate['fundingRate'] * 100,  # 转换为百分比
                'next_time': funding_rate['fundingDatetime'],  # 下次结算时间
                'volume_24h': float(funding_rate['info']['turnover24h'])  # 24小时交易量
            }
        except Exception as e:
            logger.error(f"获取{symbol}资金费率失败: {str(e)}")
            return None

    async def scan_markets(self):
        """扫描所有市场"""
        try:
            symbols = await self.get_all_symbols()
            logger.info(f"开始扫描 {len(symbols)} 个交易对...")

            results = []
            for symbol in symbols:
                # 获取资金费率信息
                funding_info = await self.get_funding_rate(symbol)
                
                if funding_info is None:
                    continue

                # 检查是否满足条件：资金费率小于-1.0%且24小时交易量大于200万
                if (funding_info['rate'] <= -1.0 and funding_info['volume_24h'] >= 2000000):
                    results.append({
                        'symbol': symbol,
                        'funding_rate': funding_info['rate'],
                        'next_funding_time': funding_info['next_time'],
                        'volume_24h': funding_info['volume_24h']
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
    logger.info("\n{:<15} {:<15} {:<25} {:<15}".format(
        "交易对", "资金费率(%)", "下次结算时间", "24h交易量(USDT)"))
    logger.info("-" * 70)

    for result in sorted(results, key=lambda x: x['funding_rate']):  # 按资金费率升序排序
        logger.info("{:<15} {:<15.4f} {:<25} {:<15.2f}".format(
            result['symbol'],
            result['funding_rate'],
            result['next_funding_time'],
            result['volume_24h']
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
