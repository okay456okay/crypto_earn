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

    async def get_bybit_markets(self) -> List[str]:
        """获取Bybit所有永续合约交易对"""
        try:
            logger.info("正在获取Bybit交易对信息...")
            markets = await self.bybit.fetch_markets()
            perpetual_markets = [m['id'] for m in markets if m['swap'] and m['linear']]
            logger.info(f"Bybit共有 {len(perpetual_markets)} 个永续合约交易对")
            logger.debug(f"Bybit永续合约交易对列表: {perpetual_markets}")
            return perpetual_markets
        except Exception as e:
            logger.error(f"获取Bybit交易对信息失败: {str(e)}")
            return []

    async def get_common_markets(self) -> List[str]:
        """获取两个交易所共同支持的交易对"""
        try:
            # 并发获取两个交易所的交易对
            binance_markets, bybit_markets = await asyncio.gather(
                self.get_binance_markets(),
                self.get_bybit_markets()
            )
            
            # 找出共同支持的交易对
            common_markets = list(set(binance_markets) & set(bybit_markets))
            logger.info(f"两个交易所共同支持 {len(common_markets)} 个交易对")
            logger.debug(f"共同交易对列表: {common_markets}")
            
            return common_markets
        except Exception as e:
            logger.error(f"获取共同交易对失败: {str(e)}")
            return []

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

    async def get_binance_max_leverage(self, symbol: str) -> int:
        """获取Binance指定交易对的最大杠杆倍数"""
        try:
            # 获取交易对信息
            response = await self.binance.fapiPublicGetExchangeInfo()
            
            if response and 'symbols' in response:
                for symbol_info in response['symbols']:
                    if symbol_info['symbol'] == symbol:
                        # 获取杠杆倍数信息
                        leverage_info = await self.binance.fapiPrivateGetLeverageBracket({
                            'symbol': symbol
                        })
                        
                        if leverage_info and 'brackets' in leverage_info[0]:
                            max_leverage = int(leverage_info[0]['brackets'][0]['initialLeverage'])
                            logger.debug(f"获取到Binance {symbol} 最大杠杆倍数: {max_leverage}倍")
                            return max_leverage
            
            logger.warning(f"未能获取到Binance {symbol} 的最大杠杆倍数，使用默认值10倍")
            return 10  # 如果获取失败，返回默认值10倍
            
        except Exception as e:
            logger.error(f"获取Binance {symbol} 最大杠杆倍数时出错: {str(e)}")
            return 10  # 如果出错，返回默认值10倍

    async def get_bybit_max_leverage(self, symbol: str) -> int:
        """获取Bybit指定交易对的最大杠杆倍数"""
        try:
            # 获取交易对信息
            response = await self.bybit.publicGetV5MarketInstrumentsInfo({
                'category': 'linear',
                'symbol': symbol
            })
            
            if response and 'result' in response and 'list' in response['result']:
                for instrument in response['result']['list']:
                    if instrument['symbol'] == symbol:
                        # 先将字符串转换为float，再转换为int
                        max_leverage = int(float(instrument['leverageFilter']['maxLeverage']))
                        logger.debug(f"获取到Bybit {symbol} 最大杠杆倍数: {max_leverage}倍")
                        return max_leverage
            
            logger.warning(f"未能获取到Bybit {symbol} 的最大杠杆倍数，使用默认值10倍")
            return 10  # 如果获取失败，返回默认值10倍
            
        except Exception as e:
            logger.error(f"获取Bybit {symbol} 最大杠杆倍数时出错: {str(e)}")
            return 10  # 如果出错，返回默认值10倍

    async def get_binance_order_book(self, symbol: str) -> Dict:
        """获取Binance指定交易对的订单簿数据"""
        try:
            order_book = await self.binance.fetch_order_book(symbol, limit=1)
            return {
                'bid_price': float(order_book['bids'][0][0]),
                'bid_size': float(order_book['bids'][0][1]),
                'ask_price': float(order_book['asks'][0][0]),
                'ask_size': float(order_book['asks'][0][1])
            }
        except Exception as e:
            logger.error(f"获取Binance {symbol} 订单簿数据失败: {str(e)}")
            return None

    async def get_bybit_order_book(self, symbol: str) -> Dict:
        """获取Bybit指定交易对的订单簿数据"""
        try:
            order_book = await self.bybit.fetch_order_book(symbol, limit=1)
            return {
                'bid_price': float(order_book['bids'][0][0]),
                'bid_size': float(order_book['bids'][0][1]),
                'ask_price': float(order_book['asks'][0][0]),
                'ask_size': float(order_book['asks'][0][1])
            }
        except Exception as e:
            logger.error(f"获取Bybit {symbol} 订单簿数据失败: {str(e)}")
            return None

    async def find_arbitrage_opportunities(self, 
                                   binance_rates: Dict[str, Dict], 
                                   bybit_rates: Dict[str, Dict], 
                                   min_spread: float = 0.01,
                                   max_price_spread: float = 0.001) -> List[Dict]:
        """找出资金费率差超过阈值且价格差满足条件的交易对"""
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
                
                # 如果结算时间差在1分钟以内
                if time_diff <= 60:
                    # 获取两个交易所的最大杠杆倍数
                    binance_leverage, bybit_leverage = await asyncio.gather(
                        self.get_binance_max_leverage(symbol),
                        self.get_bybit_max_leverage(symbol)
                    )
                    
                    # 取较小的杠杆倍数
                    max_leverage = min(binance_leverage, bybit_leverage)
                    
                    # 并发获取两个交易所的订单簿数据
                    binance_order_book, bybit_order_book = await asyncio.gather(
                        self.get_binance_order_book(symbol),
                        self.get_bybit_order_book(symbol)
                    )
                    
                    if binance_order_book and bybit_order_book:
                        # 判断价格差条件
                        if binance_rate < bybit_rate:
                            # Binance费率低，在Binance做多，Bybit做空
                            price_spread = (bybit_order_book['bid_price'] - binance_order_book['ask_price']) / binance_order_book['ask_price']
                            if price_spread <= max_price_spread:
                                opportunities.append({
                                    'symbol': symbol,
                                    'binance_rate': binance_rate,
                                    'bybit_rate': bybit_rate,
                                    'spread': spread,
                                    'binance_next_time': datetime.fromtimestamp(binance_rates[symbol]['next_funding_time']/1000),
                                    'bybit_next_time': datetime.fromtimestamp(bybit_rates[symbol]['next_funding_time']/1000),
                                    'max_leverage': max_leverage,
                                    'binance_leverage': binance_leverage,
                                    'bybit_leverage': bybit_leverage,
                                    'price_spread': price_spread,
                                    'binance_ask_price': binance_order_book['ask_price'],
                                    'binance_ask_size': binance_order_book['ask_size'],
                                    'bybit_bid_price': bybit_order_book['bid_price'],
                                    'bybit_bid_size': bybit_order_book['bid_size'],
                                    'direction': 'long_binance_short_bybit'
                                })
                        else:
                            # Bybit费率低，在Bybit做多，Binance做空
                            price_spread = (binance_order_book['bid_price'] - bybit_order_book['ask_price']) / bybit_order_book['ask_price']
                            if price_spread <= max_price_spread:
                                opportunities.append({
                                    'symbol': symbol,
                                    'binance_rate': binance_rate,
                                    'bybit_rate': bybit_rate,
                                    'spread': spread,
                                    'binance_next_time': datetime.fromtimestamp(binance_rates[symbol]['next_funding_time']/1000),
                                    'bybit_next_time': datetime.fromtimestamp(bybit_rates[symbol]['next_funding_time']/1000),
                                    'max_leverage': max_leverage,
                                    'binance_leverage': binance_leverage,
                                    'bybit_leverage': bybit_leverage,
                                    'price_spread': price_spread,
                                    'binance_bid_price': binance_order_book['bid_price'],
                                    'binance_bid_size': binance_order_book['bid_size'],
                                    'bybit_ask_price': bybit_order_book['ask_price'],
                                    'bybit_ask_size': bybit_order_book['ask_size'],
                                    'direction': 'long_bybit_short_binance'
                                })
        
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
            if opp['direction'] == 'long_binance_short_bybit':
                logger.info(f"交易对: {opp['symbol']} | "
                           f"Binance费率: {opp['binance_rate']*100:.4f}% | "
                           f"Bybit费率: {opp['bybit_rate']*100:.4f}% | "
                           f"费率差: {opp['spread']*100:.4f}% | "
                           f"价格差: {opp['price_spread']*100:.4f}% | "
                           f"Binance卖一: {opp['binance_ask_price']:.8f}({opp['binance_ask_size']:.4f}) | "
                           f"Bybit买一: {opp['bybit_bid_price']:.8f}({opp['bybit_bid_size']:.4f}) | "
                           f"最大杠杆: {opp['max_leverage']}倍 | "
                           f"方向: Binance做多, Bybit做空")
            else:
                logger.info(f"交易对: {opp['symbol']} | "
                           f"Binance费率: {opp['binance_rate']*100:.4f}% | "
                           f"Bybit费率: {opp['bybit_rate']*100:.4f}% | "
                           f"费率差: {opp['spread']*100:.4f}% | "
                           f"价格差: {opp['price_spread']*100:.4f}% | "
                           f"Binance买一: {opp['binance_bid_price']:.8f}({opp['binance_bid_size']:.4f}) | "
                           f"Bybit卖一: {opp['bybit_ask_price']:.8f}({opp['bybit_ask_size']:.4f}) | "
                           f"最大杠杆: {opp['max_leverage']}倍 | "
                           f"方向: Bybit做多, Binance做空")

    async def monitor(self, min_spread: float = 0.01, max_price_spread: float = 0.001):
        """监控资金费率套利机会"""
        try:
            logger.info(f"开始监控资金费率套利机会，最小费率差要求: {min_spread*100:.2f}%, 最大价格差要求: {max_price_spread*100:.2f}%")
            
            # 获取共同支持的交易对
            common_markets = await self.get_common_markets()
            if not common_markets:
                logger.error("未能获取到共同支持的交易对，程序退出")
                return
            
            # 并发获取两个交易所的资金费率
            logger.info("正在获取交易所数据...")
            binance_rates, bybit_rates = await asyncio.gather(
                self.get_binance_funding_rates(common_markets),
                self.get_bybit_funding_rates(common_markets)
            )
            
            # 找出套利机会
            opportunities = await self.find_arbitrage_opportunities(
                binance_rates, 
                bybit_rates, 
                min_spread,
                max_price_spread
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
    parser.add_argument('-p', '--max-price-spread', type=float, default=0.001,
                       help='最大价格差要求，默认0.001 (0.1%%)')
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
        await monitor.monitor(min_spread=args.min_spread, max_price_spread=args.max_price_spread)
        
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
