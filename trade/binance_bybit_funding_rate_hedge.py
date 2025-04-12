#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
资金费率套利监控程序

此脚本实现以下功能：
1. 获取Binance和Bybit所有合约交易对的资金费率
2. 获取下次结算时间
3. 计算两个交易所之间的资金费率差
4. 筛选出资金费率差超过1%的交易对
5. 打印符合条件的交易对信息
"""

import sys
import os
import logging
import asyncio
import ccxt.pro as ccxtpro
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Tuple
import argparse

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import binance_api_key, binance_api_secret, bybit_api_key, bybit_api_secret, proxies

class FundingRateMonitor:
    def __init__(self):
        """初始化交易所连接"""
        logger.info("正在初始化交易所连接...")
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
                'defaultType': 'future',  # 设置为合约模式
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
                'defaultType': 'linear',  # 设置默认为USDT永续合约
            }
        })
        logger.info("交易所连接初始化完成")

    async def get_binance_markets(self) -> List[str]:
        """获取Binance所有永续合约交易对"""
        try:
            logger.info("正在获取Binance交易对信息...")
            markets = await self.binance.fetch_markets()
            perpetual_markets = [m['id'] for m in markets if m['swap'] and m['linear']]
            logger.info(f"Binance共有 {len(perpetual_markets)} 个永续合约交易对")
            logger.debug(f"Binance永续合约交易对列表: {perpetual_markets}")
            return perpetual_markets
        except Exception as e:
            logger.error(f"获取Binance交易对信息失败: {str(e)}")
            return []

    async def get_binance_funding_rates(self, symbols: List[str]) -> Dict[str, Dict]:
        """获取Binance指定交易对的资金费率信息"""
        try:
            logger.info("正在获取Binance资金费率...")
            funding_rates = {}
            
            for symbol in symbols:
                try:
                    funding_rate = await self.binance.fetch_funding_rate(symbol)
                    if funding_rate:
                        funding_rates[symbol] = {
                            'rate': float(funding_rate['info']['lastFundingRate']),
                            'next_funding_time': int(funding_rate['info']['nextFundingTime']),
                            'symbol': symbol
                        }
                        logger.debug(f"Binance {symbol} 资金费率: {funding_rates[symbol]['rate']*100:.4f}%, "
                                   f"下次结算时间: {datetime.fromtimestamp(funding_rates[symbol]['next_funding_time']/1000)}")
                except Exception as e:
                    logger.warning(f"获取Binance {symbol} 资金费率失败: {str(e)}")
                    continue
                    
            logger.info(f"成功获取 {len(funding_rates)} 个Binance交易对的资金费率")
            return funding_rates
            
        except Exception as e:
            logger.error(f"获取Binance资金费率失败: {str(e)}")
            return {}

    async def get_bybit_funding_rates(self, symbols: List[str]) -> Dict[str, Dict]:
        """获取Bybit指定交易对的资金费率信息"""
        try:
            logger.info("正在获取Bybit资金费率...")
            funding_rates = {}
            
            for symbol in symbols:
                try:
                    funding_rate = await self.bybit.fetch_funding_rate(symbol)
                    if funding_rate:
                        funding_rates[symbol] = {
                            'rate': float(funding_rate['info']['fundingRate']),
                            'next_funding_time': int(funding_rate['info']['nextFundingTime']),
                            'symbol': symbol
                        }
                        logger.debug(f"Bybit {symbol} 资金费率: {funding_rates[symbol]['rate']*100:.4f}%, "
                                   f"下次结算时间: {datetime.fromtimestamp(funding_rates[symbol]['next_funding_time']/1000)}")
                except Exception as e:
                    logger.warning(f"获取Bybit {symbol} 资金费率失败: {str(e)}")
                    continue
                    
            logger.info(f"成功获取 {len(funding_rates)} 个Bybit交易对的资金费率")
            return funding_rates
            
        except Exception as e:
            logger.error(f"获取Bybit资金费率失败: {str(e)}")
            return {}

    def find_arbitrage_opportunities(self, 
                                   binance_rates: Dict[str, Dict], 
                                   bybit_rates: Dict[str, Dict], 
                                   min_spread: float = 0.01) -> List[Dict]:
        """找出资金费率差超过阈值的交易对"""
        logger.info("正在分析套利机会...")
        opportunities = []
        
        # 找出两个交易所都有的交易对
        common_symbols = set(binance_rates.keys()) & set(bybit_rates.keys())
        logger.info(f"两个交易所共有 {len(common_symbols)} 个相同的交易对")
        logger.debug(f"共同交易对列表: {list(common_symbols)}")
        
        for symbol in common_symbols:
            binance_rate = binance_rates[symbol]['rate']
            bybit_rate = bybit_rates[symbol]['rate']
            
            # 计算资金费率差
            spread = abs(binance_rate - bybit_rate)
            
            # 如果资金费率差超过阈值
            if spread >= min_spread:
                # 计算下次结算时间差（秒）
                time_diff = abs(binance_rates[symbol]['next_funding_time'] - 
                              bybit_rates[symbol]['next_funding_time'])
                
                # 如果结算时间差在5分钟以内
                if time_diff <= 300:  # 300秒 = 5分钟
                    opportunities.append({
                        'symbol': symbol,
                        'binance_rate': binance_rate,
                        'bybit_rate': bybit_rate,
                        'spread': spread,
                        'binance_next_time': datetime.fromtimestamp(binance_rates[symbol]['next_funding_time']/1000),
                        'bybit_next_time': datetime.fromtimestamp(bybit_rates[symbol]['next_funding_time']/1000)
                    })
                    logger.debug(f"发现套利机会: {symbol}, "
                               f"Binance费率: {binance_rate*100:.4f}%, "
                               f"Bybit费率: {bybit_rate*100:.4f}%, "
                               f"价差: {spread*100:.4f}%")
        
        logger.info(f"找到 {len(opportunities)} 个符合条件的套利机会")
        return opportunities

    def print_opportunities(self, opportunities: List[Dict]):
        """打印套利机会"""
        if not opportunities:
            logger.info("当前没有符合条件的套利机会")
            return
            
        logger.info("\n" + "="*100)
        logger.info("资金费率套利机会:")
        logger.info("="*100)
        
        for opp in opportunities:
            logger.info(f"交易对: {opp['symbol']} | "
                       f"Binance费率: {opp['binance_rate']*100:.4f}% | "
                       f"Bybit费率: {opp['bybit_rate']*100:.4f}% | "
                       f"价差: {opp['spread']*100:.4f}% | "
                       f"Binance结算: {opp['binance_next_time']} | "
                       f"Bybit结算: {opp['bybit_next_time']}")

    async def monitor(self, min_spread: float = 0.01):
        """监控资金费率套利机会"""
        try:
            logger.info(f"开始监控资金费率套利机会，最小价差要求: {min_spread*100:.2f}%")
            
            # 获取Binance的交易对列表
            binance_symbols = await self.get_binance_markets()
            if not binance_symbols:
                logger.error("未能获取到Binance交易对信息，程序退出")
                return
            
            # 获取两个交易所的资金费率
            logger.info("正在获取交易所数据...")
            binance_rates, bybit_rates = await asyncio.gather(
                self.get_binance_funding_rates(binance_symbols),
                self.get_bybit_funding_rates(binance_symbols)
            )
            
            # 找出套利机会
            opportunities = self.find_arbitrage_opportunities(
                binance_rates, 
                bybit_rates, 
                min_spread
            )
            
            # 打印套利机会
            self.print_opportunities(opportunities)
            
        except Exception as e:
            logger.error(f"监控资金费率时出错: {str(e)}")
        finally:
            # 关闭交易所连接
            logger.info("正在关闭交易所连接...")
            await asyncio.gather(
                self.binance.close(),
                self.bybit.close()
            )
            logger.info("程序执行完成")

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='资金费率套利监控程序')
    parser.add_argument('-d', '--debug', action='store_true', help='启用调试日志')
    parser.add_argument('-s', '--min-spread', type=float, default=0.01, 
                       help='最小资金费率差要求，默认0.01 (1%%)')
    return parser.parse_args()

async def main():
    """主函数"""
    try:
        # 解析命令行参数
        args = parse_arguments()
        
        # 设置日志级别
        if args.debug:
            logger.setLevel(logging.DEBUG)
            logger.debug("已启用调试日志模式")
        else:
            logger.setLevel(logging.INFO)
        
        logger.info("程序启动...")
        monitor = FundingRateMonitor()
        await monitor.monitor(min_spread=args.min_spread)  # 使用命令行参数指定的最小价差
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    # 设置并启动事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close()
