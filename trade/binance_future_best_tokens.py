#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binance合约标的筛选脚本

此脚本用于筛选Binance交易所的合约标的，根据以下条件：
1. 资金费率最小的30个合约标的
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
from concurrent.futures import ThreadPoolExecutor

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import binance_api_key, binance_api_secret, proxies
from high_yield.exchange import ExchangeAPI

class BinanceFutureScanner:
    def __init__(self):
        """初始化Binance合约分析器"""
        self.exchange_api = ExchangeAPI()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # 尝试使用代理，出错时不使用代理
        try:
            self.session.proxies.update(proxies)
            # 测试代理连接
            test_resp = self.session.get("https://www.binance.com", timeout=5)
            if test_resp.status_code != 200:
                raise Exception("代理连接测试失败")
        except Exception as e:
            print(f"代理连接出错，将不使用代理直接连接: {e}")
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
        for retry in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # 速率限制
                    retry_after = int(response.headers.get('Retry-After', retry_delay * 2))
                    print(f"API速率限制，等待 {retry_after} 秒后重试...")
                    time.sleep(retry_after)
                else:
                    print(f"API请求失败: URL={url}, 状态码={response.status_code}")
                    time.sleep(retry_delay)
            except requests.exceptions.RequestException as e:
                print(f"网络错误 (尝试 {retry+1}/{max_retries}): {e}")
                if retry < max_retries - 1:  # 如果不是最后一次重试
                    time.sleep(retry_delay)
                else:
                    print("达到最大重试次数，请求失败")
                    return None
        return None
        
    def get_all_futures(self):
        """获取所有合约标的"""
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            data = self.api_request(url)
            
            if data:
                # 只筛选状态为TRADING的USDT合约
                symbols = [s['symbol'] for s in data['symbols'] 
                          if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT']
                self.all_futures = symbols
                print(f"获取到 {len(self.all_futures)} 个合约标的")
                return symbols
            else:
                print(f"获取合约列表失败")
                return []
        except Exception as e:
            print(f"获取合约列表时出错: {e}")
            return []

    def get_current_funding_rates(self):
        """获取所有合约的当前资金费率"""
        try:
            url = "https://fapi.binance.com/fapi/v1/premiumIndex"
            data = self.api_request(url)
            
            if data:
                # 构建资金费率字典 {symbol: funding_rate}
                funding_rates = {item['symbol']: float(item['lastFundingRate']) for item in data}
                return funding_rates
            else:
                print(f"获取资金费率失败")
                return {}
        except Exception as e:
            print(f"获取资金费率时出错: {e}")
            return {}

    def get_funding_rate_history(self, symbol, start_time, end_time):
        """获取指定合约的资金费率历史"""
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
                return df
            else:
                print(f"获取{symbol}资金费率历史失败")
                return pd.DataFrame()
        except Exception as e:
            print(f"获取{symbol}资金费率历史时出错: {e}")
            return pd.DataFrame()

    def analyze_funding_rate_trend(self, symbol):
        """分析资金费率趋势是否一直减小"""
        end_time = int(time.time() * 1000)
        start_time = end_time - (48 * 60 * 60 * 1000)  # 48小时前
        
        df = self.get_funding_rate_history(symbol, start_time, end_time)
        if df.empty:
            return False, 0
        
        # 检查是否至少有两个数据点
        if len(df) < 2:
            return False, 0
        
        # 计算资金费率的差值
        df['diff'] = df['fundingRate'].diff()
        
        # 如果所有的diff都小于等于0（除了第一个NaN），则表示资金费率一直在减小
        decreasing = all(val <= 0 for val in df['diff'].dropna())
        
        # 返回是否一直减小以及平均资金费率
        return decreasing, df['fundingRate'].mean()

    def get_price_changes(self, symbol, period):
        """获取价格变化百分比"""
        try:
            # 计算周期对应的毫秒数和K线间隔
            if period <= 24:
                interval = '1h'
                limit = period
            else:
                interval = '2h'
                limit = period // 2 + (1 if period % 2 != 0 else 0)
                limit = min(limit, 1000)  # Binance API限制

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
                    return price_change
            return 0
        except Exception as e:
            print(f"获取{symbol}价格变化时出错: {e}")
            return 0

    def get_open_interest(self, symbol):
        """获取合约持仓量"""
        try:
            url = f"https://fapi.binance.com/fapi/v1/openInterest"
            params = {"symbol": symbol}
            
            data = self.api_request(url, params)
            
            if data:
                return float(data['openInterest'])
            else:
                print(f"获取{symbol}持仓量失败")
                return 0
        except Exception as e:
            print(f"获取{symbol}持仓量时出错: {e}")
            return 0

    def get_long_short_ratio(self, symbol, period="5m", limit=500):
        """获取多空比例数据"""
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
            
            # 2. 获取多空持仓量比
            url = f"https://fapi.binance.com/futures/data/globalLongShortPositionRatio"
            data = self.api_request(url, params)
            
            if data and data:
                result['long_short_position_ratio'] = {
                    'longPosition': float(data[0]['longPosition']),
                    'shortPosition': float(data[0]['shortPosition']),
                    'longShortRatio': float(data[0]['longShortRatio'])
                }
            
            # 3. 获取主动买卖多空比
            url = f"https://fapi.binance.com/futures/data/takerlongshortRatio"
            data = self.api_request(url, params)
            
            if data and data:
                result['taker_long_short_ratio'] = {
                    'buySellRatio': float(data[0]['buySellRatio']),
                    'buyVol': float(data[0]['buyVol']),
                    'sellVol': float(data[0]['sellVol'])
                }
            
            return result
        except Exception as e:
            print(f"获取{symbol}多空比例数据时出错: {e}")
            return result

    def get_market_cap(self, symbol):
        """获取币种市值（简化版本，避免API限制）"""
        # 由于CoinGecko API有严格的速率限制，这里使用简化的方法
        # 在实际使用中，应考虑缓存市值数据或使用付费API
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
        
        return market_caps.get(coin, None)

    def get_prices(self):
        """获取所有合约的当前价格"""
        try:
            url = "https://fapi.binance.com/fapi/v1/ticker/price"
            data = self.api_request(url)
            
            if data:
                prices = {}
                for ticker in data:
                    prices[ticker['symbol']] = float(ticker['price'])
                return prices
            else:
                print(f"获取价格数据失败")
                return {}
        except Exception as e:
            print(f"获取价格时出错: {e}")
            return {}

    def calculate_oi_to_mc_ratio(self, symbol, open_interest, market_cap):
        """计算合约持仓量与市值的比例"""
        if market_cap and market_cap > 0 and hasattr(self, 'prices') and symbol in self.prices:
            # 使用缓存的价格
            price = self.prices.get(symbol, 0)
            
            # 计算持仓量的美元价值
            oi_value_usd = open_interest * price
            
            # 计算持仓量/市值比例
            ratio = (oi_value_usd / market_cap) * 100
            return ratio
        return None
        
    def format_ratio_output(self, ratio):
        """格式化比例输出"""
        if ratio is not None:
            return f"{ratio:.4f}%"
        return "N/A"

    def scan_best_futures(self):
        """扫描并筛选最佳合约标的"""
        print("开始扫描Binance合约标的...")
        
        # 1. 获取所有合约
        if not self.all_futures:
            self.get_all_futures()
        
        if not self.all_futures:
            print("无法获取合约列表，退出")
            return
        
        # 2. 获取所有合约的当前资金费率
        current_funding_rates = self.get_current_funding_rates()
        if not current_funding_rates:
            print("无法获取资金费率数据，退出")
            return
            
        # 3. 获取所有合约的当前价格
        self.prices = self.get_prices()
        if not self.prices:
            print("无法获取价格数据，退出")
            return
        
        # 4. 筛选资金费率最小的30个合约标的
        funding_rate_items = [(symbol, rate) for symbol, rate in current_funding_rates.items() 
                             if symbol in self.all_futures]
        funding_rate_items.sort(key=lambda x: x[1])  # 按资金费率升序排序
        
        top_30_symbols = [item[0] for item in funding_rate_items[:30]]
        
        print(f"\n=== 资金费率最小的30个合约标的 ===")
        print(f"{'合约标的':<10} {'当前资金费率':<15}")
        print("-" * 30)
        for symbol, rate in funding_rate_items[:30]:
            print(f"{symbol:<10} {rate*100:<15.6f}%")
        
        # 5. 详细分析这30个合约标的
        print("\n=== 详细分析资金费率最小的30个合约标的 ===")
        print(f"{'合约标的':<10} {'资金费率':<15} {'费率趋势减小':<15} {'24h涨跌幅':<15} {'48h涨跌幅':<15} {'合约持仓量':<15} {'持仓量/市值':<15} {'多空账户比':<15} {'多空持仓比':<15}")
        print("-" * 150)
        
        detailed_results = []
        
        for symbol in top_30_symbols:
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
        print("1. 资金费率最小的前30")
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
        
        print(f"\n符合所有条件的合约标的数量: {len(final_results)}")
        
        return final_results

async def main():
    scanner = BinanceFutureScanner()
    results = scanner.scan_best_futures()
    return 0

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 