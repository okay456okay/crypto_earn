#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ccxt
import pandas as pd
from datetime import datetime, timedelta
import time
from typing import List, Dict, Tuple
import logging

import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from tools.logger import logger

class SpikeAnalyzer:
    def __init__(self, exchange_id: str = 'bybit', symbol: str = 'ETH/USDT:USDT'):
        """
        初始化分析器
        :param exchange_id: 交易所ID，默认bybit
        :param symbol: 交易对，默认ETH/USDT永续合约
        """
        self.exchange = getattr(ccxt, exchange_id)({
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
            }
        })
        self.symbol = symbol
        self.min_price_change = 0.005  # 最小价格变化阈值 (0.5%)

    def fetch_ohlcv_data(self, timeframe: str = '1m', days: int = 1) -> pd.DataFrame:
        """
        获取OHLCV数据
        :param timeframe: 时间周期
        :param days: 获取多少天的数据
        :return: DataFrame包含OHLCV数据
        """
        try:
            since = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
            ohlcv = self.exchange.fetch_ohlcv(self.symbol, timeframe, since)
            
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except Exception as e:
            logger.error(f"获取OHLCV数据失败: {str(e)}")
            raise

    def find_spikes(self, df: pd.DataFrame, window_minutes: int = 5) -> List[Dict]:
        """
        在给定的数据中查找价格插针
        :param df: OHLCV数据
        :param window_minutes: 前后检查的时间窗口（分钟）
        :return: 插针列表
        """
        spikes = []
        window = window_minutes

        for i in range(window, len(df) - window):
            current_low = df.iloc[i]['low']
            current_high = df.iloc[i]['high']
            current_time = df.iloc[i]['timestamp']

            # 检查向下插针
            prev_window = df.iloc[i-window:i]
            next_window = df.iloc[i+1:i+window+1]
            
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

        return spikes

    def analyze_spikes(self, days: int = 1, window_minutes: int = 5) -> List[Dict]:
        """
        分析指定时间段内的所有插针
        :param days: 分析的天数
        :param window_minutes: 分析窗口大小（分钟）
        :return: 插针列表
        """
        try:
            df = self.fetch_ohlcv_data(days=days)
            spikes = self.find_spikes(df, window_minutes)
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
        print("未发现符合条件的插针")
        return

    print(f"\n找到 {len(spikes)} 个插针:")
    print("-" * 100)

    for spike in spikes:
        print(f"类型: {spike['type']}")
        print(f"插针时间点: {spike['spike_time']}")
        print(f"插针价格: {spike['spike_price']:.4f}")
        
        if spike['type'] == '向下插针':
            print(f"前5分钟最高价: {spike['prev_high_price']:.4f} (时间: {spike['prev_high_time']})")
            print(f"后5分钟最高价: {spike['next_high_price']:.4f} (时间: {spike['next_high_time']})")
        else:
            print(f"前5分钟最低价: {spike['prev_low_price']:.4f} (时间: {spike['prev_low_time']})")
            print(f"后5分钟最低价: {spike['next_low_price']:.4f} (时间: {spike['next_low_time']})")
        
        print(f"前向价差: {spike['prev_change_pct']:.2f}%")
        print(f"后向价差: {spike['next_change_pct']:.2f}%")
        print("-" * 100)

def main():
    # 示例使用
    analyzer = SpikeAnalyzer(exchange_id='bybit', symbol='ETH/USDT:USDT')
    try:
        spikes = analyzer.analyze_spikes(days=1, window_minutes=5)
        print_spike_results(spikes)
    except Exception as e:
        logger.error(f"运行失败: {str(e)}")

if __name__ == '__main__':
    main() 