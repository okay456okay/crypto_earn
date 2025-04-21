#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binance合约标的筛选脚本

此脚本用于筛选Binance交易所的合约标的，根据以下条件：
1. 资金费率最小的N个合约标的（默认5个）
2. 资金费率趋势分析（48小时内是否一直在变小）
3. 价格涨跌幅度分析（24小时和48小时内是否小于20%）
4. 合约持仓量和市值的比例分析
5. 合约多空人数和持仓量分析
"""

import sys
import os
import asyncio
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import binance_api_key, binance_api_secret, proxies
from high_yield.exchange import ExchangeAPI

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("binance_future_scanner")

class BinanceFutureScanner:
    def __init__(self, top_n=5, debug=False):
        """
        初始化Binance合约分析器
        
        Args:
            top_n (int): 筛选资金费率最小的前N个合约，默认5个
            debug (bool): 是否开启调试模式，默认False
        """
        logger.info(f"初始化BinanceFutureScanner: top_n={top_n}, debug={debug}")
        
        # 设置日志级别
        if debug:
            logger.setLevel(logging.DEBUG)
            logger.debug("调试模式已开启，将显示详细日志")
        
        self.top_n = top_n
        self.exchange_api = ExchangeAPI()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # 尝试使用代理，出错时不使用代理
        try:
            logger.debug(f"尝试使用代理: {proxies}")
            self.session.proxies.update(proxies)
            # 测试代理连接
            test_resp = self.session.get("https://www.binance.com", timeout=5)
            if test_resp.status_code != 200:
                raise Exception("代理连接测试失败")
            logger.debug("代理连接测试成功")
        except Exception as e:
            logger.warning(f"代理连接出错，将不使用代理直接连接: {e}")
            self.session.proxies.clear()  # 清除代理设置
            
        # 缓存数据
        self.all_futures = []
        self.funding_rates = {}
        self.price_changes = {}
        self.open_interest = {}
        self.market_caps = {}
        self.long_short_ratios = {}
        self.prices = {}
        
    def api_request(self, url, params=None, max_retries=3, retry_delay=1):
        """封装API请求，处理重试逻辑"""
        logger.debug(f"API请求: URL={url}, 参数={params}")
        
        for retry in range(max_retries):
            try:
                start_time = time.time()
                response = self.session.get(url, params=params, timeout=10)
                elapsed = time.time() - start_time
                
                logger.debug(f"API响应: 状态码={response.status_code}, 耗时={elapsed:.2f}秒")
                
                if response.status_code == 200:
                    data = response.json()
                    logger.debug(f"API数据: 获取到 {len(data) if isinstance(data, list) else '对象'} 条数据")
                    return data
                elif response.status_code == 429:  # 速率限制
                    retry_after = int(response.headers.get('Retry-After', retry_delay * 2))
                    logger.warning(f"API速率限制，等待 {retry_after} 秒后重试... (尝试 {retry+1}/{max_retries})")
                    time.sleep(retry_after)
                else:
                    logger.error(f"API请求失败: URL={url}, 状态码={response.status_code}, 响应={response.text}")
                    time.sleep(retry_delay)
            except requests.exceptions.RequestException as e:
                logger.error(f"网络错误 (尝试 {retry+1}/{max_retries}): {e}")
                if retry < max_retries - 1:  # 如果不是最后一次重试
                    time.sleep(retry_delay)
                else:
                    logger.error("达到最大重试次数，请求失败")
                    return None
        return None
        
    def get_all_futures(self):
        """获取所有合约标的"""
        logger.info("获取所有Binance合约标的...")
        
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            data = self.api_request(url)
            
            if data:
                # 只筛选状态为TRADING的USDT合约
                symbols = [s['symbol'] for s in data['symbols'] 
                          if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT']
                self.all_futures = symbols
                logger.info(f"获取到 {len(self.all_futures)} 个合约标的")
                logger.debug(f"合约标的列表: {symbols[:10]}... (显示前10个)")
                return symbols
            else:
                logger.error("获取合约列表失败")
                return []
        except Exception as e:
            logger.exception(f"获取合约列表时出错: {e}")
            return []

    def get_current_funding_rates(self):
        """获取所有合约的当前资金费率"""
        logger.info("获取所有合约的当前资金费率...")
        
        try:
            url = "https://fapi.binance.com/fapi/v1/premiumIndex"
            data = self.api_request(url)
            
            if data:
                # 构建资金费率字典 {symbol: funding_rate}
                funding_rates = {item['symbol']: float(item['lastFundingRate']) for item in data}
                logger.info(f"获取到 {len(funding_rates)} 个合约的资金费率")
                
                # 调试日志：打印最小的几个资金费率
                if logger.isEnabledFor(logging.DEBUG):
                    sorted_rates = sorted(funding_rates.items(), key=lambda x: x[1])
                    logger.debug(f"资金费率最小的几个合约: {sorted_rates[:5]}")
                
                return funding_rates
            else:
                logger.error("获取资金费率失败")
                return {}
        except Exception as e:
            logger.exception(f"获取资金费率时出错: {e}")
            return {}

    def get_funding_rate_history(self, symbol, start_time, end_time):
        """获取指定合约的资金费率历史"""
        logger.debug(f"获取 {symbol} 资金费率历史: startTime={start_time}, endTime={end_time}")
        
        try:
            url = f"https://fapi.binance.com/fapi/v1/fundingRate"
            params = {
                "symbol": symbol,
                "startTime": start_time,
                "endTime": end_time
            }
            
            data = self.api_request(url, params)
            
            if data:
                # 转换为DataFrame便于分析
                df = pd.DataFrame(data)
                if not df.empty:
                    df['fundingRate'] = df['fundingRate'].astype(float)
                    df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms')
                    df = df.sort_values('fundingTime')
                    logger.debug(f"获取到 {len(df)} 条资金费率历史记录")
                    
                    if logger.isEnabledFor(logging.DEBUG) and not df.empty:
                        logger.debug(f"资金费率范围: {df['fundingRate'].min()} 到 {df['fundingRate'].max()}")
                        logger.debug(f"时间范围: {df['fundingTime'].min()} 到 {df['fundingTime'].max()}")
                        
                return df
            else:
                logger.warning(f"获取 {symbol} 资金费率历史失败")
                return pd.DataFrame()
        except Exception as e:
            logger.exception(f"获取 {symbol} 资金费率历史时出错: {e}")
            return pd.DataFrame()

    def analyze_funding_rate_trend(self, symbol):
        """分析资金费率趋势是否一直减小"""
        logger.debug(f"分析 {symbol} 资金费率趋势")
        
        end_time = int(time.time() * 1000)
        start_time = end_time - (48 * 60 * 60 * 1000)  # 48小时前
        
        df = self.get_funding_rate_history(symbol, start_time, end_time)
        if df.empty:
            logger.debug(f"{symbol} 没有资金费率历史数据")
            return False, 0
        
        # 检查是否至少有两个数据点
        if len(df) < 2:
            logger.debug(f"{symbol} 资金费率历史数据点数不足: {len(df)}")
            return False, 0
        
        # 计算资金费率的差值
        df['diff'] = df['fundingRate'].diff()
        
        # 如果所有的diff都小于等于0（除了第一个NaN），则表示资金费率一直在减小
        decreasing = all(val <= 0 for val in df['diff'].dropna())
        avg_rate = df['fundingRate'].mean()
        
        logger.debug(f"{symbol} 资金费率趋势是否减小: {decreasing}, 平均资金费率: {avg_rate}")
        if logger.isEnabledFor(logging.DEBUG):
            diffs = df['diff'].dropna().tolist()
            logger.debug(f"{symbol} 资金费率变化序列: {diffs}")
        
        # 返回是否一直减小以及平均资金费率
        return decreasing, avg_rate

    def get_price_changes(self, symbol, period):
        """获取价格变化百分比"""
        logger.debug(f"获取 {symbol} {period}小时价格变化")
        
        try:
            # 计算周期对应的毫秒数和K线间隔
            if period <= 24:
                interval = '1h'
                limit = period
            else:
                interval = '2h'
                limit = period // 2 + (1 if period % 2 != 0 else 0)
                limit = min(limit, 1000)  # Binance API限制
                
            logger.debug(f"K线参数: interval={interval}, limit={limit}")

            url = f"https://fapi.binance.com/fapi/v1/klines"
            params = {
                "symbol": symbol,
                "interval": interval,
                "limit": limit
            }
            
            data = self.api_request(url, params)
            
            if data:
                if data:
                    first_price = float(data[0][1])  # 开盘价
                    last_price = float(data[-1][4])  # 最新收盘价
                    price_change = ((last_price - first_price) / first_price) * 100
                    logger.debug(f"{symbol} {period}小时价格变化: {price_change:.2f}% (从 {first_price} 到 {last_price})")
                    return price_change
            logger.warning(f"无法获取 {symbol} 价格变化数据")
            return 0
        except Exception as e:
            logger.exception(f"获取 {symbol} 价格变化时出错: {e}")
            return 0

    def get_open_interest(self, symbol):
        """获取合约持仓量"""
        logger.debug(f"获取 {symbol} 合约持仓量")
        
        try:
            url = f"https://fapi.binance.com/fapi/v1/openInterest"
            params = {"symbol": symbol}
            
            data = self.api_request(url, params)
            
            if data:
                oi = float(data['openInterest'])
                logger.debug(f"{symbol} 合约持仓量: {oi}")
                return oi
            else:
                logger.warning(f"获取 {symbol} 持仓量失败")
                return 0
        except Exception as e:
            logger.exception(f"获取 {symbol} 持仓量时出错: {e}")
            return 0

    def get_long_short_ratio(self, symbol, period="5m", limit=500):
        """获取多空比例数据"""
        logger.debug(f"获取 {symbol} 多空比例数据")
        
        result = {
            'long_short_account_ratio': None,
            'long_short_position_ratio': None,
            'taker_long_short_ratio': None
        }
        
        try:
            # 1. 获取多空持仓人数比
            url = f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
            params = {
                "symbol": symbol,
                "period": period,
                "limit": 1
            }
            
            data = self.api_request(url, params)
            
            if data and data:
                result['long_short_account_ratio'] = {
                    'longAccount': float(data[0]['longAccount']),
                    'shortAccount': float(data[0]['shortAccount']),
                    'longShortRatio': float(data[0]['longShortRatio'])
                }
                logger.debug(f"{symbol} 多空账户比: {result['long_short_account_ratio']}")
            
            # 2. 获取多空持仓量比
            url = f"https://fapi.binance.com/futures/data/globalLongShortPositionRatio"
            data = self.api_request(url, params)
            
            if data and data:
                result['long_short_position_ratio'] = {
                    'longPosition': float(data[0]['longPosition']),
                    'shortPosition': float(data[0]['shortPosition']),
                    'longShortRatio': float(data[0]['longShortRatio'])
                }
                logger.debug(f"{symbol} 多空持仓比: {result['long_short_position_ratio']}")
            
            # 3. 获取主动买卖多空比
            url = f"https://fapi.binance.com/futures/data/takerlongshortRatio"
            data = self.api_request(url, params)
            
            if data and data:
                result['taker_long_short_ratio'] = {
                    'buySellRatio': float(data[0]['buySellRatio']),
                    'buyVol': float(data[0]['buyVol']),
                    'sellVol': float(data[0]['sellVol'])
                }
                logger.debug(f"{symbol} 主动买卖比: {result['taker_long_short_ratio']}")
            
            return result
        except Exception as e:
            logger.exception(f"获取 {symbol} 多空比例数据时出错: {e}")
            return result

    def get_market_cap(self, symbol):
        """
        获取币种市值
        
        注意：Binance API没有直接提供市值数据，这里使用一个模拟的映射表
        实际应用中可以接入CoinMarketCap、CoinGecko等API
        """
        logger.debug(f"获取 {symbol} 市值数据")
        
        # 从symbol中提取币种名称（去掉USDT）
        coin = symbol.replace('USDT', '').lower()
        
        # 为了演示，这里使用一个简单的映射表来模拟一些常见币种的市值
        # 在实际使用中，应该实现一个更完整的方法来获取真实市值
        market_caps = {
            'btc': 1_200_000_000_000,  # 1.2万亿美元
            'eth': 400_000_000_000,    # 4000亿美元
            'bnb': 60_000_000_000,     # 600亿美元
            'sol': 50_000_000_000,     # 500亿美元
            'xrp': 30_000_000_000,     # 300亿美元
            'ada': 15_000_000_000,     # 150亿美元
            'avax': 12_000_000_000,    # 120亿美元
            'doge': 11_000_000_000,    # 110亿美元
            'dot': 10_000_000_000,     # 100亿美元
            'link': 8_000_000_000,     # 80亿美元
            'ltc': 6_000_000_000,      # 60亿美元
            'matic': 5_000_000_000,    # 50亿美元
            'atom': 3_000_000_000,     # 30亿美元
            'uni': 3_000_000_000,      # 30亿美元
            'etc': 2_500_000_000,      # 25亿美元
            'fil': 2_000_000_000,      # 20亿美元
            'aave': 1_500_000_000,     # 15亿美元
            'mana': 1_000_000_000,     # 10亿美元
            'sand': 1_000_000_000,     # 10亿美元
            'enj': 800_000_000,        # 8亿美元
            'gmt': 700_000_000,        # 7亿美元
            'api3': 500_000_000,       # 5亿美元
            'gmx': 500_000_000,        # 5亿美元
            'lpt': 400_000_000,        # 4亿美元
            'voxel': 300_000_000,      # 3亿美元
            'rare': 200_000_000,       # 2亿美元
            'move': 200_000_000,       # 2亿美元
            'high': 100_000_000,       # 1亿美元
            'nkn': 100_000_000,        # 1亿美元
            'sys': 100_000_000,        # 1亿美元
            'magic': 100_000_000,      # 1亿美元
            'mav': 80_000_000,         # 8千万美元
            'bio': 50_000_000,         # 5千万美元
            'prompt': 50_000_000,      # 5千万美元
            'cyber': 30_000_000,       # 3千万美元
            'melania': 10_000_000,     # 1千万美元
            'xcn': 10_000_000,         # 1千万美元
            't': 10_000_000,           # 1千万美元
            'orca': 10_000_000,        # 1千万美元
            'vvv': 5_000_000,          # 500万美元
            'layer': 5_000_000,        # 500万美元
            'wct': 5_000_000,          # 500万美元
            'bmt': 5_000_000,          # 500万美元
            'red': 5_000_000,          # 500万美元
            'wal': 5_000_000,          # 500万美元
            'aergo': 5_000_000,        # 500万美元
            'rez': 5_000_000,          # 500万美元
            'xai': 5_000_000,          # 500万美元
        }
        
        market_cap = market_caps.get(coin, None)
        logger.debug(f"{symbol} 市值数据: {market_cap}")
        return market_cap

    def get_prices(self):
        """获取所有合约的当前价格"""
        logger.info("获取所有合约的当前价格...")
        
        try:
            url = "https://fapi.binance.com/fapi/v1/ticker/price"
            data = self.api_request(url)
            
            if data:
                prices = {}
                for ticker in data:
                    prices[ticker['symbol']] = float(ticker['price'])
                
                logger.info(f"获取到 {len(prices)} 个合约的价格数据")
                
                if logger.isEnabledFor(logging.DEBUG):
                    sample_prices = {k: prices[k] for k in list(prices.keys())[:5]}
                    logger.debug(f"价格数据样本: {sample_prices}")
                
                return prices
            else:
                logger.error("获取价格数据失败")
                return {}
        except Exception as e:
            logger.exception(f"获取价格时出错: {e}")
            return {}

    def calculate_oi_to_mc_ratio(self, symbol, open_interest, market_cap):
        """计算合约持仓量与市值的比例"""
        logger.debug(f"计算 {symbol} 持仓量/市值比例: 持仓量={open_interest}, 市值={market_cap}")
        
        if market_cap and market_cap > 0 and hasattr(self, 'prices') and symbol in self.prices:
            # 使用缓存的价格
            price = self.prices.get(symbol, 0)
            
            # 计算持仓量的美元价值
            oi_value_usd = open_interest * price
            
            # 计算持仓量/市值比例
            ratio = (oi_value_usd / market_cap) * 100
            
            logger.debug(f"{symbol} 持仓量美元价值: {oi_value_usd}, 持仓量/市值比例: {ratio:.4f}%")
            return ratio
        
        logger.debug(f"{symbol} 无法计算持仓量/市值比例: 市值数据缺失或价格数据缺失")
        return None
        
    def format_ratio_output(self, ratio):
        """格式化比例输出"""
        if ratio is not None:
            return f"{ratio:.4f}%"
        return "N/A"

    def scan_best_futures(self):
        """扫描并筛选最佳合约标的"""
        logger.info("开始扫描Binance合约标的...")
        logger.info(f"将筛选资金费率最小的前 {self.top_n} 个合约")
        
        # 1. 获取所有合约
        if not self.all_futures:
            self.get_all_futures()
        
        if not self.all_futures:
            logger.error("无法获取合约列表，退出")
            return
        
        # 2. 获取所有合约的当前资金费率
        current_funding_rates = self.get_current_funding_rates()
        if not current_funding_rates:
            logger.error("无法获取资金费率数据，退出")
            return
            
        # 3. 获取所有合约的当前价格
        self.prices = self.get_prices()
        if not self.prices:
            logger.error("无法获取价格数据，退出")
            return
        
        # 4. 筛选资金费率最小的N个合约标的
        funding_rate_items = [(symbol, rate) for symbol, rate in current_funding_rates.items() 
                             if symbol in self.all_futures]
        funding_rate_items.sort(key=lambda x: x[1])  # 按资金费率升序排序
        
        top_n_symbols = [item[0] for item in funding_rate_items[:self.top_n]]
        
        logger.info(f"已筛选出资金费率最小的 {len(top_n_symbols)} 个合约标的")
        logger.debug(f"筛选出的标的: {top_n_symbols}")
        
        print(f"\n=== 资金费率最小的 {self.top_n} 个合约标的 ===")
        print(f"{'合约标的':<10} {'当前资金费率':<15}")
        print("-" * 30)
        for symbol, rate in funding_rate_items[:self.top_n]:
            print(f"{symbol:<10} {rate*100:<15.6f}%")
        
        # 5. 详细分析这N个合约标的
        print(f"\n=== 详细分析资金费率最小的 {self.top_n} 个合约标的 ===")
        print(f"{'合约标的':<10} {'资金费率':<15} {'费率趋势减小':<15} {'24h涨跌幅':<15} {'48h涨跌幅':<15} {'合约持仓量':<15} {'持仓量/市值':<15} {'多空账户比':<15} {'多空持仓比':<15}")
        print("-" * 150)
        
        detailed_results = []
        
        for symbol in top_n_symbols:
            logger.info(f"分析 {symbol} 详细数据...")
            
            # 分析资金费率趋势
            is_decreasing, avg_rate = self.analyze_funding_rate_trend(symbol)
            
            # 获取价格变化
            price_change_24h = self.get_price_changes(symbol, 24)
            price_change_48h = self.get_price_changes(symbol, 48)
            
            # 获取持仓量
            open_interest = self.get_open_interest(symbol)
            
            # 获取多空比例
            ratios = self.get_long_short_ratio(symbol)
            
            # 获取市值 (实际使用时需实现)
            market_cap = self.get_market_cap(symbol)
            
            # 计算持仓量/市值比例
            oi_to_market_cap = self.calculate_oi_to_mc_ratio(symbol, open_interest, market_cap)
            
            # 多空账户比
            ls_account_ratio = ratios['long_short_account_ratio']['longShortRatio'] if ratios['long_short_account_ratio'] else None
            
            # 多空持仓比
            ls_position_ratio = ratios['long_short_position_ratio']['longShortRatio'] if ratios['long_short_position_ratio'] else None
            
            # 打印结果
            print(f"{symbol:<10} {current_funding_rates[symbol]*100:<15.6f}% {'是' if is_decreasing else '否':<15} {price_change_24h:<15.2f}% {price_change_48h:<15.2f}% {open_interest:<15.2f} {self.format_ratio_output(oi_to_market_cap):<15} {ls_account_ratio if ls_account_ratio is not None else 'N/A':<15} {ls_position_ratio if ls_position_ratio is not None else 'N/A':<15}")
            
            # 存储详细结果
            detailed_results.append({
                'symbol': symbol,
                'funding_rate': current_funding_rates[symbol],
                'is_decreasing': is_decreasing,
                'avg_rate': avg_rate,
                'price_change_24h': price_change_24h,
                'price_change_48h': price_change_48h,
                'open_interest': open_interest,
                'market_cap': market_cap,
                'oi_to_market_cap': oi_to_market_cap,
                'long_short_account_ratio': ls_account_ratio,
                'long_short_position_ratio': ls_position_ratio,
                'account_data': ratios['long_short_account_ratio'],
                'position_data': ratios['long_short_position_ratio'],
                'taker_data': ratios['taker_long_short_ratio']
            })
        
        # 6. 筛选并显示最终符合条件的合约标的
        print("\n=== 最终筛选结果 ===")
        print("符合以下条件的合约标的：")
        print(f"1. 资金费率最小的前 {self.top_n}")
        print("2. 资金费率趋势一直在减小")
        print("3. 24小时和48小时涨跌幅度均小于20%")
        print("-" * 120)
        
        final_results = []
        for result in detailed_results:
            if (result['is_decreasing'] and 
                abs(result['price_change_24h']) < 20 and 
                abs(result['price_change_48h']) < 20):
                final_results.append(result)
                
                # 打印详细多空数据
                symbol = result['symbol']
                print(f"\n合约标的: {symbol}")
                print(f"当前资金费率: {result['funding_rate']*100:.6f}%")
                print(f"资金费率趋势是否减小: {'是' if result['is_decreasing'] else '否'}")
                print(f"24小时涨跌幅: {result['price_change_24h']:.2f}%")
                print(f"48小时涨跌幅: {result['price_change_48h']:.2f}%")
                print(f"合约持仓量: {result['open_interest']:.2f}")
                
                # 打印多空账户数据
                if result['account_data']:
                    long_account = result['account_data']['longAccount'] * 100
                    short_account = result['account_data']['shortAccount'] * 100
                    account_ratio = result['account_data']['longShortRatio']
                    print(f"多空账户数据:")
                    print(f"  多方账户占比: {long_account:.2f}%")
                    print(f"  空方账户占比: {short_account:.2f}%")
                    print(f"  多空账户比例: {account_ratio:.2f}")
                
                # 打印多空持仓数据
                if result['position_data']:
                    long_position = result['position_data']['longPosition'] * 100
                    short_position = result['position_data']['shortPosition'] * 100
                    position_ratio = result['position_data']['longShortRatio']
                    print(f"多空持仓数据:")
                    print(f"  多方持仓占比: {long_position:.2f}%")
                    print(f"  空方持仓占比: {short_position:.2f}%")
                    print(f"  多空持仓比例: {position_ratio:.2f}")
                
                # 打印主动买卖比例
                if result['taker_data']:
                    buy_sell_ratio = result['taker_data']['buySellRatio']
                    buy_vol = result['taker_data']['buyVol']
                    sell_vol = result['taker_data']['sellVol']
                    print(f"主动买卖数据:")
                    print(f"  主动买卖比例: {buy_sell_ratio:.2f}")
                    print(f"  主动买入量: {buy_vol:.2f}")
                    print(f"  主动卖出量: {sell_vol:.2f}")
                
                print("-" * 50)
        
        logger.info(f"符合所有条件的合约标的数量: {len(final_results)}")
        print(f"\n符合所有条件的合约标的数量: {len(final_results)}")
        
        return final_results


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="Binance合约标的筛选工具")
    parser.add_argument("-n", "--top_n", type=int, default=5, help="筛选资金费率最小的前N个合约，默认5个")
    parser.add_argument("-d", "--debug", action="store_true", help="启用调试模式，输出更详细的日志")
    return parser.parse_args()


async def main():
    # 解析命令行参数
    args = parse_args()
    
    # 创建扫描器实例
    scanner = BinanceFutureScanner(top_n=args.top_n, debug=args.debug)
    
    # 执行扫描
    results = scanner.scan_best_futures()
    
    return 0


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 