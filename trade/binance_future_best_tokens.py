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
from config import binance_api_key, binance_api_secret
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
        
        # 缓存数据
        self.all_futures = []
        self.funding_rates = {}
        self.price_changes = {}
        self.open_interest = {}
        self.market_caps = {}
        self.long_short_ratios = {}
        self.prices = {}
        
        # CoinGecko API基础URL
        self.coingecko_base_url = "https://api.coingecko.com/api/v3"
        
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
                    logger.debug(f"API速率限制，等待 {retry_after} 秒后重试... (尝试 {retry+1}/{max_retries})")
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
            
            # 2. 获取多空持仓量比 - 修复API端点
            url = f"https://fapi.binance.com/futures/data/topLongShortPositionRatio"
            data = self.api_request(url, params)
            
            if data and data:
                # 注意：API返回结构可能有所不同，需要根据实际返回调整
                result['long_short_position_ratio'] = {
                    'longPosition': float(data[0].get('longPosition', 0.5)),
                    'shortPosition': float(data[0].get('shortPosition', 0.5)),
                    'longShortRatio': float(data[0].get('longShortRatio', 1.0))
                }
                logger.debug(f"{symbol} 多空持仓比: {result['long_short_position_ratio']}")
            else:
                # 备用方法：尝试从topLongShortAccountRatio获取数据
                url = f"https://fapi.binance.com/futures/data/topLongShortAccountRatio"
                data = self.api_request(url, params)
                if data and data:
                    # 如果成功，使用账户比例作为近似值
                    ratio = float(data[0].get('longShortRatio', 1.0))
                    result['long_short_position_ratio'] = {
                        'longPosition': ratio / (1 + ratio),
                        'shortPosition': 1 / (1 + ratio),
                        'longShortRatio': ratio
                    }
                    logger.debug(f"{symbol} 使用账户比例近似估计多空持仓比: {result['long_short_position_ratio']}")
            
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
        从CoinGecko获取币种市值数据
        """
        logger.debug(f"获取 {symbol} 市值数据")
        
        # 从symbol中提取币种名称（去掉USDT）
        coin = symbol.replace('USDT', '').lower()
        
        try:
            # 首先获取coin_id
            url = f"{self.coingecko_base_url}/search"
            params = {"query": coin}
            search_data = self.api_request(url, params)
            
            if not search_data or not search_data.get('coins'):
                logger.debug(f"无法在CoinGecko找到 {coin} 的数据")
                return None
                
            coin_id = None
            for coin_data in search_data['coins']:
                if coin_data['symbol'].lower() == coin:
                    coin_id = coin_data['id']
                    break
            
            if not coin_id:
                logger.warning(f"无法找到 {coin} 的coin_id")
                return None
            
            # 获取市值数据
            url = f"{self.coingecko_base_url}/simple/price"
            params = {
                "ids": coin_id,
                "vs_currencies": "usd",
                "include_market_cap": "true"
            }
            
            price_data = self.api_request(url, params)
            
            if price_data and coin_id in price_data:
                market_cap = price_data[coin_id].get('usd_market_cap')
                logger.debug(f"{symbol} 市值: {market_cap}")
                return market_cap
            
            logger.warning(f"无法获取 {symbol} 的市值数据")
            return None
            
        except Exception as e:
            logger.exception(f"获取 {symbol} 市值数据时出错: {e}")
            return None

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
        
        if market_cap and market_cap > 0 and symbol in self.prices:
            price = self.prices[symbol]
            
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
        
        try:
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
            
            # 定义格式化字符串供后续使用，确保所有表格对齐，修复百分号问题
            header_format = "{:<11} {:<9} {:<15}"
            data_format = "{:<15} {:<15} {:<15.6f}"
            
            # 优化表格展示
            print("\n" + "=" * 45)
            print(f" 资金费率最小的 {self.top_n} 个合约标的 ")
            print("=" * 45)
            print(header_format.format("合约标的", "当前资金费率", "当前价格"))
            print("-" * 45)
            
            for symbol, rate in funding_rate_items[:self.top_n]:
                price = self.prices.get(symbol, 'N/A')
                print(data_format.format(symbol, f"{rate*100:.6f}%", price))
            
            # 定义详细分析表格的格式，修复百分号问题
            detail_header_format = "{:<11} {:<11} {:<11} {:<11} {:<11} {:<11} {:<11} {:<11} {:<15}"
            detail_data_format = "{:<15} {:<15} {:<15} {:<15} {:<15} {:<15} {:<15} {:<15} {:<15}"
            
            # 5. 详细分析这N个合约标的
            print("\n" + "=" * 135)
            print(f" 详细分析资金费率最小的 {self.top_n} 个合约标的 ")
            print("=" * 135)
            print(detail_header_format.format(
                "合约标的", "资金费率", "费率趋势减小", "24h涨跌幅", "48h涨跌幅", 
                "合约持仓量", "持仓量/市值", "多空账户比", "多空持仓比"
            ))
            print("-" * 135)
            
            detailed_results = []
            
            for symbol in top_n_symbols:
                try:
                    logger.debug(f"分析 {symbol} 详细数据...")
                    
                    # 分析资金费率趋势
                    is_decreasing, avg_rate = self.analyze_funding_rate_trend(symbol)
                    
                    # 获取价格变化
                    price_change_24h = self.get_price_changes(symbol, 24)
                    price_change_48h = self.get_price_changes(symbol, 48)
                    
                    # 获取持仓量
                    open_interest = self.get_open_interest(symbol)
                    
                    # 获取多空比例
                    ratios = self.get_long_short_ratio(symbol)
                    
                    # 获取市值
                    market_cap = self.get_market_cap(symbol)
                    
                    # 计算持仓量/市值比例
                    oi_to_market_cap = self.calculate_oi_to_mc_ratio(symbol, open_interest, market_cap)
                    
                    # 多空账户比
                    ls_account_ratio = ratios['long_short_account_ratio']['longShortRatio'] if ratios['long_short_account_ratio'] else None
                    
                    # 多空持仓比
                    ls_position_ratio = ratios['long_short_position_ratio']['longShortRatio'] if ratios['long_short_position_ratio'] else None
                    
                    # 格式化数据，确保百分号直接跟在数字后面
                    funding_rate_str = f"{current_funding_rates[symbol]*100:.6f}%"
                    price_change_24h_str = f"{price_change_24h:.2f}%"
                    price_change_48h_str = f"{price_change_48h:.2f}%"
                    
                    # 打印结果
                    print(detail_data_format.format(
                        symbol, 
                        funding_rate_str, 
                        '是' if is_decreasing else '否', 
                        price_change_24h_str, 
                        price_change_48h_str, 
                        f"{open_interest:.2f}", 
                        self.format_ratio_output(oi_to_market_cap), 
                        f"{ls_account_ratio:.2f}" if ls_account_ratio is not None else 'N/A', 
                        f"{ls_position_ratio:.2f}" if ls_position_ratio is not None else 'N/A'
                    ))
                    
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
                except Exception as e:
                    logger.error(f"分析 {symbol} 时出错: {e}")
                    continue
            
            # 6. 筛选并显示最终符合条件的合约标的
            print("\n" + "=" * 45)
            print(" 最终筛选结果 ")
            print("=" * 45)
            print("符合以下条件的合约标的：")
            print(f"1. 资金费率最小的前 {self.top_n}")
            print("2. 资金费率趋势一直在减小")
            print("3. 24小时和48小时涨跌幅度均小于20%")
            print("-" * 45)
            
            final_results = []
            for result in detailed_results:
                if (result['is_decreasing'] and 
                    abs(result['price_change_24h']) < 20 and 
                    abs(result['price_change_48h']) < 20):
                    final_results.append(result)
                    
                    # 打印详细多空数据
                    symbol = result['symbol']
                    print(f"\n{'-' * 45}")
                    print(f"合约标的: {symbol}")
                    print(f"{'-' * 45}")
                    print(f"当前资金费率: {result['funding_rate']*100:.6f}%")
                    print(f"资金费率趋势是否减小: {'是' if result['is_decreasing'] else '否'}")
                    print(f"24小时涨跌幅: {result['price_change_24h']:.2f}%")
                    print(f"48小时涨跌幅: {result['price_change_48h']:.2f}%")
                    print(f"合约持仓量: {result['open_interest']:.2f}")
                    print(f"市值: {result['market_cap']:,.2f} USD")
                    print(f"持仓量/市值比例: {self.format_ratio_output(result['oi_to_market_cap'])}")
                    
                    print(f"{'-' * 20} 多空数据 {'-' * 20}")
                    
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
            
            print(f"\n符合所有条件的合约标的数量: {len(final_results)}")
            logger.info(f"符合所有条件的合约标的数量: {len(final_results)}")
            
            return final_results
            
        except Exception as e:
            logger.exception(f"扫描过程中出错: {e}")
            return []


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="Binance合约标的筛选工具")
    parser.add_argument("-n", "--top_n", type=int, default=5, help="筛选资金费率最小的前N个合约，默认5个")
    parser.add_argument("-d", "--debug", action="store_true", help="启用调试模式，输出更详细的日志")
    return parser.parse_args()


async def main():
    try:
        # 解析命令行参数
        args = parse_args()
        
        # 创建扫描器实例
        scanner = BinanceFutureScanner(top_n=args.top_n, debug=args.debug)
        
        # 执行扫描
        results = scanner.scan_best_futures()
        
        return 0 if results is not None else 1
        
    except KeyboardInterrupt:
        logger.info("用户中断执行")
        return 130
    except Exception as e:
        logger.exception("执行过程中出错")
        return 1


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 