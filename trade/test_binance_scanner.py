#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binance合约扫描器测试版本

用于测试扫描逻辑的正确性，只扫描少量交易对
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from tools.logger import logger
from config import binance_api_key, binance_api_secret, proxies, project_root
from binance.client import Client
from binance.exceptions import BinanceAPIException
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging
import numpy as np

# 设置日志级别
logger.setLevel(logging.INFO)

class BinanceContractScannerTest:
    """Binance合约扫描器测试版"""
    
    def __init__(self, api_key: str = None, api_secret: str = None):
        """初始化测试扫描器"""
        self.client = Client(
            api_key or binance_api_key, 
            api_secret or binance_api_secret,
            requests_params={'proxies': proxies}
        )
        
        # 扫描参数
        self.price_volatility_threshold = 0.20  # 20%价格波动阈值
        self.min_leverage = 20  # 最小杠杆要求
        self.days_to_analyze = 30  # 分析天数
        
        logger.info("测试扫描器初始化完成")

    def get_test_symbols(self) -> List[str]:
        """获取测试用的交易对列表"""
        return ['BTCUSDT', 'ETHUSDT', 'ADAUSDT', 'DOTUSDT', 'LINKUSDT']

    def get_symbol_leverage_info(self, symbol: str) -> int:
        """获取交易对的最大杠杆信息"""
        try:
            # 获取杠杆档位信息
            leverage_brackets = self.client.futures_leverage_bracket(symbol=symbol)
            if leverage_brackets and len(leverage_brackets) > 0:
                # 获取第一个symbol的brackets数组
                brackets = leverage_brackets[0].get('brackets', [])
                if brackets:
                    # 获取最高档位的杠杆（第一个bracket通常是最高杠杆）
                    max_leverage = brackets[0].get('initialLeverage', 1)
                    return int(max_leverage)
            return 1
        except Exception as e:
            logger.error(f"获取{symbol}杠杆信息失败: {str(e)}")
            return 1

    def get_price_data(self, symbol: str, days: int = 30) -> Optional[List[float]]:
        """获取交易对的历史价格数据"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            
            klines = self.client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_1DAY,
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000),
                limit=days + 5
            )
            
            if not klines or len(klines) < days * 0.8:
                logger.warning(f"{symbol}: 价格数据不足，只有{len(klines) if klines else 0}天")
                return None
            
            close_prices = [float(kline[4]) for kline in klines]
            logger.info(f"{symbol}: 获取到{len(close_prices)}天的价格数据")
            return close_prices
            
        except Exception as e:
            logger.error(f"获取{symbol}价格数据失败: {str(e)}")
            return None

    def get_funding_rate_history(self, symbol: str, days: int = 30) -> Optional[List[float]]:
        """获取交易对的资金费率历史数据"""
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            
            funding_rates = self.client.futures_funding_rate(
                symbol=symbol,
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000),
                limit=days * 3 + 10
            )
            
            if not funding_rates:
                logger.warning(f"{symbol}: 无资金费率数据")
                return None
            
            rates = [float(rate['fundingRate']) for rate in funding_rates]
            logger.info(f"{symbol}: 获取到{len(rates)}个资金费率数据点")
            return rates
            
        except Exception as e:
            logger.error(f"获取{symbol}资金费率数据失败: {str(e)}")
            return None

    def calculate_price_volatility(self, prices: List[float]) -> float:
        """计算价格波动率"""
        if not prices or len(prices) < 2:
            return float('inf')
        
        min_price = min(prices)
        max_price = max(prices)
        
        if min_price == 0:
            return float('inf')
        
        volatility = (max_price - min_price) / min_price
        return volatility

    def analyze_funding_rate_direction(self, funding_rates: List[float]) -> Dict[str, Any]:
        """分析资金费率的方向性"""
        if not funding_rates:
            return {
                'is_consistent': False,
                'direction': 'unknown',
                'positive_ratio': 0,
                'negative_ratio': 0,
                'avg_rate': 0,
                'total_count': 0
            }
        
        positive_count = sum(1 for rate in funding_rates if rate > 0)
        negative_count = sum(1 for rate in funding_rates if rate < 0)
        zero_count = len(funding_rates) - positive_count - negative_count
        
        total_count = len(funding_rates)
        positive_ratio = positive_count / total_count
        negative_ratio = negative_count / total_count
        
        # 判断是否保持一个方向（80%以上的数据点保持同一方向）
        consistency_threshold = 0.80
        is_consistent = (positive_ratio >= consistency_threshold or 
                        negative_ratio >= consistency_threshold)
        
        if positive_ratio >= consistency_threshold:
            direction = 'positive'
        elif negative_ratio >= consistency_threshold:
            direction = 'negative'
        else:
            direction = 'mixed'
        
        avg_rate = np.mean(funding_rates)
        
        return {
            'is_consistent': is_consistent,
            'direction': direction,
            'positive_ratio': positive_ratio,
            'negative_ratio': negative_ratio,
            'avg_rate': avg_rate,
            'total_count': total_count,
            'positive_count': positive_count,
            'negative_count': negative_count,
            'zero_count': zero_count
        }

    def test_symbol(self, symbol: str):
        """测试单个交易对"""
        logger.info(f"\n{'='*50}")
        logger.info(f"测试交易对: {symbol}")
        logger.info(f"{'='*50}")
        
        try:
            # 1. 获取杠杆信息
            max_leverage = self.get_symbol_leverage_info(symbol)
            logger.info(f"最大杠杆: {max_leverage}x")
            
            # 2. 获取价格数据
            prices = self.get_price_data(symbol, self.days_to_analyze)
            if prices is None:
                logger.error(f"{symbol}: 无法获取价格数据")
                return
            
            # 3. 计算价格波动率
            volatility = self.calculate_price_volatility(prices)
            logger.info(f"价格波动率: {volatility:.2%}")
            logger.info(f"价格区间: ${min(prices):.6f} - ${max(prices):.6f}")
            
            # 4. 获取资金费率数据
            funding_rates = self.get_funding_rate_history(symbol, self.days_to_analyze)
            if funding_rates is None:
                logger.error(f"{symbol}: 无法获取资金费率数据")
                return
            
            # 5. 分析资金费率方向性
            funding_analysis = self.analyze_funding_rate_direction(funding_rates)
            logger.info(f"资金费率方向: {funding_analysis['direction']}")
            logger.info(f"资金费率一致性: {funding_analysis['positive_ratio']:.1%} 正 / {funding_analysis['negative_ratio']:.1%} 负")
            logger.info(f"平均资金费率: {funding_analysis['avg_rate']:.6f} ({funding_analysis['avg_rate']*365*3:.2f}% 年化)")
            
            # 6. 判断是否符合条件
            conditions = {
                'leverage_ok': max_leverage >= self.min_leverage,
                'volatility_ok': volatility <= self.price_volatility_threshold,
                'funding_consistent': funding_analysis['is_consistent']
            }
            
            logger.info(f"\n条件检查:")
            logger.info(f"  杠杆要求 (>={self.min_leverage}x): {'✓' if conditions['leverage_ok'] else '✗'} ({max_leverage}x)")
            logger.info(f"  波动率要求 (<={self.price_volatility_threshold:.1%}): {'✓' if conditions['volatility_ok'] else '✗'} ({volatility:.2%})")
            logger.info(f"  资金费率一致性: {'✓' if conditions['funding_consistent'] else '✗'} ({funding_analysis['direction']})")
            
            all_conditions_met = all(conditions.values())
            logger.info(f"\n最终结果: {'符合条件 ✓' if all_conditions_met else '不符合条件 ✗'}")
            
        except Exception as e:
            logger.error(f"测试{symbol}时发生错误: {str(e)}")

    def run_test(self):
        """运行测试"""
        logger.info("开始测试Binance合约扫描器...")
        
        test_symbols = self.get_test_symbols()
        logger.info(f"将测试以下交易对: {', '.join(test_symbols)}")
        
        for symbol in test_symbols:
            self.test_symbol(symbol)
            time.sleep(1)  # 避免API限制
        
        logger.info("\n测试完成!")


def main():
    """主函数"""
    try:
        scanner = BinanceContractScannerTest()
        scanner.run_test()
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 