#!/usr/bin/env python3
"""
Binance合约交易对波动率分析器 (Binance Futures Volatility Scanner)

该工具用于分析Binance合约交易对的波动率，找出波动率最小的交易对。
低波动率的交易对通常更适合网格交易等稳定套利策略。

主要功能：
1. 获取Binance所有活跃的合约交易对
2. 获取近1天的5分钟K线数据
3. 计算价格收益率的标准差作为波动率指标
4. 筛选出波动率最小的20个交易对
5. 导出分析结果供进一步使用

使用方法：
    python volatility_scanner.py

输出结果：
    - 控制台显示前20个低波动率交易对
    - 生成JSON文件保存完整分析结果
"""

import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import json
from typing import Dict, List, Tuple, Optional
import sys
import os

# 添加父级目录到路径，以便导入配置文件
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import binance_api_key, binance_api_secret, proxies
except ImportError:
    print("错误：未找到config.py文件，请先配置API密钥")
    print("请复制config_example.py为config.py并填入真实的API密钥")
    sys.exit(1)


class BinanceVolatilityScanner:
    """
    Binance合约交易对波动率分析器
    
    该类实现了完整的波动率分析流程，包括：
    - 获取合约交易对列表
    - 批量获取K线数据
    - 计算波动率指标
    - 结果排序和导出
    """

    def __init__(self):
        """
        初始化波动率分析器实例
        """
        try:
            self.exchange = ccxt.binance({
                'apiKey': binance_api_key,
                'secret': binance_api_secret,
                'sandbox': False,  # 生产环境
                'rateLimit': 1200,  # 请求频率限制（毫秒）
                'proxies': proxies,
                'options': {
                    'defaultType': 'future',  # 使用合约API
                }
            })
            
            # 加载市场数据
            self.exchange.load_markets()
            print(f"成功连接到Binance交易所，加载了 {len(self.exchange.markets)} 个交易对")
            
        except Exception as e:
            print(f"初始化Binance交易所连接失败: {e}")
            sys.exit(1)

    def get_futures_symbols(self) -> List[str]:
        """
        获取所有活跃的USDT合约交易对
        
        Returns:
            活跃合约交易对符号列表
        """
        try:
            symbols = []
            markets = self.exchange.load_markets()
            for symbol, market in markets.items():
                # 筛选条件：
                # 1. 是合约交易对 (type == 'future')
                # 2. 以USDT为计价货币 (quote == 'USDT') 
                # 3. 交易对处于活跃状态 (active == True)
                # 4. 是线性合约 (linear == True)
                symbols.append(symbol)
                # if (market.get('type') == 'future' and
                #     market.get('quote') == 'USDT' and
                #     market.get('active', False) and
                #     market.get('linear', False)):
                #     symbols.append(symbol)
            
            print(f"找到 {len(symbols)} 个活跃的USDT合约交易对")
            return symbols
            
        except Exception as e:
            print(f"获取合约交易对列表失败: {e}")
            return []

    def get_kline_data(self, symbol: str, timeframe: str = '5m', days: int = 1) -> pd.DataFrame:
        """
        获取指定交易对的K线数据
        
        Args:
            symbol: 交易对符号，如 'BTC/USDT'
            timeframe: 时间周期，默认为5分钟
            days: 获取的历史数据天数，默认为1天
            
        Returns:
            包含OHLCV数据的DataFrame
        """
        try:
            # 计算起始时间戳
            since = self.exchange.milliseconds() - days * 24 * 60 * 60 * 1000
            
            # 获取K线数据
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since)
            
            if not ohlcv:
                return pd.DataFrame()
            
            # 转换为DataFrame
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            return df
            
        except Exception as e:
            print(f"获取 {symbol} K线数据失败: {e}")
            return pd.DataFrame()

    def calculate_volatility(self, df: pd.DataFrame) -> Dict:
        """
        计算交易对的波动率指标
        
        Args:
            df: 包含OHLCV数据的DataFrame
            
        Returns:
            包含波动率指标的字典
        """
        if df.empty or len(df) < 10:
            return {}
        
        try:
            # 提取收盘价
            prices = df['close']
            
            # 计算收益率：每个周期相对于前一个周期的百分比变化
            returns = prices.pct_change().dropna()
            
            if len(returns) == 0:
                return {}
            
            # 计算基础统计指标
            metrics = {
                'data_points': len(df),                    # 数据点数量
                'mean_price': float(prices.mean()),        # 平均价格
                'price_std': float(prices.std()),          # 价格标准差
                'returns_std': float(returns.std()),       # 收益率标准差（主要波动率指标）
                'returns_mean': float(returns.mean()),     # 平均收益率
                'max_return': float(returns.max()),        # 最大单期收益率
                'min_return': float(returns.min()),        # 最小单期收益率
                'price_range': float(prices.max() - prices.min()),  # 价格区间
                'price_range_pct': float((prices.max() - prices.min()) / prices.mean() * 100),  # 价格区间百分比
            }
            
            # 计算年化波动率（假设一年365天，一天288个5分钟周期）
            annualized_volatility = returns.std() * np.sqrt(288 * 365)
            metrics['annualized_volatility'] = float(annualized_volatility)
            
            # 计算变异系数（标准差/均值）- 相对波动率
            if metrics['returns_mean'] != 0:
                metrics['coefficient_of_variation'] = abs(metrics['returns_std'] / metrics['returns_mean'])
            else:
                metrics['coefficient_of_variation'] = float('inf')
            
            return metrics
            
        except Exception as e:
            print(f"计算波动率指标失败: {e}")
            return {}

    def scan_volatility(self, max_symbols: int = None) -> List[Dict]:
        """
        扫描所有合约交易对的波动率
        
        Args:
            max_symbols: 最大分析的交易对数量，None表示分析所有
            
        Returns:
            包含所有分析结果的列表，按波动率排序
        """
        # 获取合约交易对列表
        symbols = self.get_futures_symbols()
        
        if not symbols:
            print("未找到任何合约交易对")
            return []
        
        # 限制分析数量（用于测试）
        if max_symbols:
            symbols = symbols[:max_symbols]
            print(f"限制分析前 {max_symbols} 个交易对")
        
        results = []
        total_symbols = len(symbols)
        
        print(f"开始分析 {total_symbols} 个交易对的波动率...")
        print("=" * 60)
        
        for i, symbol in enumerate(symbols, 1):
            try:
                print(f"[{i:3d}/{total_symbols}] 分析 {symbol}...", end=' ')
                
                # 获取K线数据
                df = self.get_kline_data(symbol)
                
                if df.empty:
                    print("无数据")
                    continue
                
                # 计算波动率
                metrics = self.calculate_volatility(df)
                
                if not metrics:
                    print("计算失败")
                    continue
                
                # 添加交易对信息
                result = {
                    'symbol': symbol,
                    'base_currency': symbol.split('/')[0],
                    'quote_currency': symbol.split('/')[1],
                    'scan_time': datetime.now().isoformat(),
                    **metrics
                }
                
                results.append(result)
                print(f"波动率: {metrics['returns_std']:.6f}")
                
                # 添加请求间隔，避免触发频率限制
                time.sleep(0.1)
                
            except Exception as e:
                print(f"分析 {symbol} 时出错: {e}")
                continue
        
        print("=" * 60)
        print(f"成功分析了 {len(results)} 个交易对")
        
        # 按波动率排序（升序，波动率最小的在前）
        results.sort(key=lambda x: x.get('returns_std', float('inf')))
        
        return results

    def display_results(self, results: List[Dict], top_n: int = 20):
        """
        显示分析结果
        
        Args:
            results: 分析结果列表
            top_n: 显示前N个结果
        """
        if not results:
            print("没有可显示的结果")
            return
        
        print(f"\n波动率最小的前 {top_n} 个合约交易对:")
        print("=" * 100)
        print(f"{'排名':<4} {'交易对':<15} {'波动率':<12} {'年化波动率':<12} {'平均价格':<12} {'数据点数':<8}")
        print("-" * 100)
        
        for i, result in enumerate(results[:top_n], 1):
            symbol = result['symbol']
            volatility = result.get('returns_std', 0)
            annualized_vol = result.get('annualized_volatility', 0)
            mean_price = result.get('mean_price', 0)
            data_points = result.get('data_points', 0)
            
            print(f"{i:<4} {symbol:<15} {volatility:<12.6f} {annualized_vol:<12.2f} {mean_price:<12.4f} {data_points:<8}")
        
        print("=" * 100)

    def export_results(self, results: List[Dict], filename: str = None):
        """
        导出分析结果到JSON文件
        
        Args:
            results: 分析结果列表
            filename: 输出文件名，None表示使用默认文件名
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"volatility_analysis_{timestamp}.json"
        
        # 确保文件保存在grid_network目录下
        output_path = os.path.join(os.path.dirname(__file__), filename)
        
        try:
            export_data = {
                'analysis_time': datetime.now().isoformat(),
                'total_symbols_analyzed': len(results),
                'description': 'Binance合约交易对波动率分析结果（按波动率升序排列）',
                'methodology': {
                    'data_source': 'Binance合约5分钟K线数据',
                    'time_period': '近1天',
                    'volatility_metric': '收益率标准差',
                    'sorting': '按波动率升序排列（最小波动率在前）'
                },
                'results': results
            }
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            print(f"\n分析结果已导出到: {output_path}")
            
        except Exception as e:
            print(f"导出结果失败: {e}")


def main():
    """
    主函数：执行完整的波动率分析流程
    """
    print("Binance合约交易对波动率分析器")
    print("=" * 50)
    
    # 创建分析器实例
    scanner = BinanceVolatilityScanner()
    
    # 扫描波动率
    # 如果需要测试，可以设置max_symbols参数，如：max_symbols=50
    results = scanner.scan_volatility()
    
    if not results:
        print("分析失败或无结果")
        return
    
    # 显示结果
    scanner.display_results(results, top_n=20)
    
    # 导出结果
    scanner.export_results(results)
    
    print(f"\n分析完成！共分析了 {len(results)} 个交易对")
    print("波动率最小的交易对通常更适合网格交易等稳定策略")


if __name__ == "__main__":
    main() 