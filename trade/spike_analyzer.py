#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ccxt
import pandas as pd
from datetime import datetime, timedelta
import time
from typing import List, Dict, Tuple
import logging
import argparse

import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from tools.logger import logger

class SpikeAnalyzer:
    def __init__(self, exchange_id: str = 'bybit', symbol: str = 'ETH/USDT:USDT', 
                 min_price_change: float = 0.005, window_minutes: int = 5):
        """
        初始化分析器
        :param exchange_id: 交易所ID，默认bybit
        :param symbol: 交易对，默认ETH/USDT永续合约
        :param min_price_change: 最小价格变化阈值，默认0.5%
        :param window_minutes: 分析窗口大小（分钟），默认5分钟
        """
        logger.info(f"初始化分析器 - 交易所: {exchange_id}, 交易对: {symbol}")
        logger.info(f"参数设置 - 最小价差: {min_price_change*100}%, 时间窗口: {window_minutes}分钟")
        
        self.exchange = getattr(ccxt, exchange_id)({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
            }
        })
        self.symbol = symbol
        self.min_price_change = min_price_change
        self.window_minutes = window_minutes
        self.window_seconds = window_minutes * 60  # 转换为秒

    def fetch_ohlcv_data(self, timeframe: str = '1s', days: int = 1) -> pd.DataFrame:
        """
        获取OHLCV数据
        :param timeframe: 时间周期，默认1秒
        :param days: 获取多少天的数据
        :return: DataFrame包含OHLCV数据
        """
        try:
            # 计算起始时间（当前时间往前推days天）
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            since = int(start_time.timestamp() * 1000)
            
            logger.info(f"开始获取OHLCV数据 - 交易对: {self.symbol}, 时间范围: {days}天, 时间周期: {timeframe}")
            logger.info(f"起始时间戳: {since} ({datetime.fromtimestamp(since/1000)})")
            
            # 分批获取数据，每次获取1000条
            all_ohlcv = []
            current_since = since
            
            while current_since < int(end_time.timestamp() * 1000):
                ohlcv = self.exchange.fetch_ohlcv(self.symbol, timeframe, current_since, limit=1000)
                if not ohlcv:
                    break
                    
                all_ohlcv.extend(ohlcv)
                # 更新下一次获取的起始时间
                current_since = ohlcv[-1][0] + 1
                
                # 避免请求过于频繁
                time.sleep(0.1)
            
            if not all_ohlcv:
                raise Exception("未获取到任何数据")
                
            logger.info(f"成功获取 {len(all_ohlcv)} 条OHLCV数据")
            
            df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            # 保持毫秒级精度
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # 确保数据按时间排序
            df = df.sort_values('timestamp')
            
            logger.info(f"数据时间范围: {df['timestamp'].min()} 至 {df['timestamp'].max()}")
            logger.info(f"价格范围: {df['low'].min():.2f} - {df['high'].max():.2f}")
            
            return df
        except Exception as e:
            logger.error(f"获取OHLCV数据失败: {str(e)}")
            raise

    def find_spikes(self, df: pd.DataFrame) -> List[Dict]:
        """
        在给定的数据中查找价格插针
        :param df: OHLCV数据
        :return: 插针列表
        """
        logger.info(f"开始分析插针 - 数据条数: {len(df)}, 时间窗口: {self.window_seconds}秒")
        spikes = []
        window = self.window_seconds  # 使用秒级窗口

        for i in range(window, len(df) - window):
            current_low = df.iloc[i]['low']
            current_high = df.iloc[i]['high']
            current_time = df.iloc[i]['timestamp']

            # 检查向下插针
            prev_window = df.iloc[i-window:i]
            next_window = df.iloc[i+1:i+window+1]
            
            if len(prev_window) == 0 or len(next_window) == 0:
                continue
                
            prev_high = prev_window['high'].max()
            prev_high_time = prev_window.loc[prev_window['high'] == prev_high, 'timestamp'].iloc[0]
            
            next_high = next_window['high'].max()
            next_high_time = next_window.loc[next_window['high'] == next_high, 'timestamp'].iloc[0]

            down_spike_prev_change = (prev_high - current_low) / prev_high
            down_spike_next_change = (next_high - current_low) / current_low

            # 检查向上插针
            prev_low = prev_window['low'].min()
            prev_low_time = prev_window.loc[prev_window['low'] == prev_low, 'timestamp'].iloc[0]
            
            next_low = next_window['low'].min()
            next_low_time = next_window.loc[next_window['low'] == next_low, 'timestamp'].iloc[0]

            up_spike_prev_change = (current_high - prev_low) / prev_low
            up_spike_next_change = (current_high - next_low) / next_low

            # 记录向下插针
            if down_spike_prev_change > self.min_price_change and down_spike_next_change > self.min_price_change:
                logger.debug(f"发现向下插针 - 时间: {current_time}, 价格: {current_low:.2f}, "
                           f"前向变化: {down_spike_prev_change*100:.2f}%, 后向变化: {down_spike_next_change*100:.2f}%")
                spikes.append({
                    'type': '向下插针',
                    'spike_time': current_time,
                    'spike_price': current_low,
                    'prev_high_time': prev_high_time,
                    'prev_high_price': prev_high,
                    'prev_change_pct': down_spike_prev_change * 100,
                    'next_high_time': next_high_time,
                    'next_high_price': next_high,
                    'next_change_pct': down_spike_next_change * 100
                })

            # 记录向上插针
            if up_spike_prev_change > self.min_price_change and up_spike_next_change > self.min_price_change:
                logger.debug(f"发现向上插针 - 时间: {current_time}, 价格: {current_high:.2f}, "
                           f"前向变化: {up_spike_prev_change*100:.2f}%, 后向变化: {up_spike_next_change*100:.2f}%")
                spikes.append({
                    'type': '向上插针',
                    'spike_time': current_time,
                    'spike_price': current_high,
                    'prev_low_time': prev_low_time,
                    'prev_low_price': prev_low,
                    'prev_change_pct': up_spike_prev_change * 100,
                    'next_low_time': next_low_time,
                    'next_low_price': next_low,
                    'next_change_pct': up_spike_next_change * 100
                })

        logger.info(f"插针分析完成 - 共发现 {len(spikes)} 个插针")
        return spikes

    def analyze_spikes(self, days: int = 1) -> List[Dict]:
        """
        分析指定时间段内的所有插针
        :param days: 分析的天数
        :return: 插针列表
        """
        try:
            logger.info(f"开始分析 - 交易对: {self.symbol}, 分析天数: {days}")
            df = self.fetch_ohlcv_data(days=days)
            spikes = self.find_spikes(df)
            return spikes
        except Exception as e:
            logger.error(f"分析插针失败: {str(e)}")
            raise

def print_spike_results(spikes: List[Dict]):
    """
    打印插针分析结果
    :param spikes: 插针列表
    """
    if not spikes:
        logger.info("未发现符合条件的插针")
        return

    logger.info(f"找到 {len(spikes)} 个插针:")
    print("-" * 100)

    for spike in spikes:
        print(f"类型: {spike['type']}")
        print(f"插针时间点: {spike['spike_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        print(f"插针价格: {spike['spike_price']:.4f}")
        
        if spike['type'] == '向下插针':
            print(f"前5分钟最高价: {spike['prev_high_price']:.4f} (时间: {spike['prev_high_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]})")
            print(f"后5分钟最高价: {spike['next_high_price']:.4f} (时间: {spike['next_high_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]})")
        else:
            print(f"前5分钟最低价: {spike['prev_low_price']:.4f} (时间: {spike['prev_low_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]})")
            print(f"后5分钟最低价: {spike['next_low_price']:.4f} (时间: {spike['next_low_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]})")
        
        print(f"前向价差: {spike['prev_change_pct']:.2f}%")
        print(f"后向价差: {spike['next_change_pct']:.2f}%")
        print("-" * 100)

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='加密货币价格插针分析工具')
    parser.add_argument('-t', '--token', type=str, default='ETH/USDT',
                      help='交易对名称 (例如: ETH/USDT, BTC/USDT, ETH/USDC)')
    parser.add_argument('-c', '--min-change', type=float, default=0.5,
                      help='最小价格变化百分比 (默认: 0.5)')
    parser.add_argument('-w', '--window', type=int, default=5,
                      help='分析窗口大小(分钟) (默认: 5)')
    parser.add_argument('--days', type=int, default=1,
                      help='分析天数 (默认: 1)')
    parser.add_argument('-d', '--debug', action='store_true',
                      help='启用调试日志')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # 设置日志级别
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # 构建交易对符号
    symbol = f"{args.token}:USDT" if ':USDT' not in args.token else args.token
    
    # 初始化分析器
    analyzer = SpikeAnalyzer(
        exchange_id='bybit',
        symbol=symbol,
        min_price_change=args.min_change/100,  # 转换为小数
        window_minutes=args.window
    )
    
    try:
        spikes = analyzer.analyze_spikes(days=args.days)
        print_spike_results(spikes)
    except Exception as e:
        logger.error(f"运行失败: {str(e)}")

if __name__ == '__main__':
    main() 