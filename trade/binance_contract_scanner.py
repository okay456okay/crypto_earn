#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binance合约扫描器

该脚本用于扫描Binance所有合约交易对，找到符合以下条件的交易对：
1. 最近30天内价格波动小于20%
2. 资金费率一直为正或一直为负（即保持一个方向）
3. 最大杠杆大于等于20

主要功能：
1. 获取Binance所有合约交易对信息
2. 分析每个交易对的30天价格波动
3. 分析资金费率历史数据的方向性
4. 筛选符合条件的交易对
5. 生成详细的分析报告

作者: Claude
创建时间: 2024-12-30
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
import ccxt

# 设置日志级别
logger.setLevel(logging.INFO)

class BinanceContractScanner:
    """Binance合约扫描器"""
    
    def __init__(self, api_key: str = None, api_secret: str = None, 
                 price_volatility_threshold: float = 0.10, min_leverage: int = 20, days_to_analyze: int = 30):
        """
        初始化Binance客户端
        
        Args:
            api_key: Binance API Key
            api_secret: Binance API Secret
            price_volatility_threshold: 价格波动率阈值
            min_leverage: 最小杠杆要求
            days_to_analyze: 分析天数
        """
        self.client = Client(
            api_key or binance_api_key, 
            api_secret or binance_api_secret,
            requests_params={'proxies': proxies}
        )
        
        # 确保报告目录存在
        self.reports_dir = os.path.join(project_root, 'trade/reports')
        os.makedirs(self.reports_dir, exist_ok=True)
        
        # 生成报告文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.report_file = os.path.join(self.reports_dir, f'binance_contract_scan_{timestamp}.json')
        self.summary_file = os.path.join(self.reports_dir, f'binance_contract_summary_{timestamp}.txt')
        
        # 扫描参数
        self.price_volatility_threshold = price_volatility_threshold
        self.min_leverage = min_leverage
        self.days_to_analyze = days_to_analyze
        self.exchange_name = "BINANCE"  # 交易所名称
        
        logger.info(f"Binance合约扫描器初始化完成")
        logger.info(f"参数配置: 波动率阈值={self.price_volatility_threshold:.1%}, 最小杠杆={self.min_leverage}x, 分析天数={self.days_to_analyze}天")
        logger.info(f"报告将保存到: {self.report_file}")
        logger.info(f"摘要将保存到: {self.summary_file}")

    def get_all_futures_symbols(self) -> List[Dict[str, Any]]:
        """
        获取所有合约交易对信息
        
        Returns:
            List[Dict]: 包含交易对信息的列表
        """
        try:
            logger.info("获取Binance所有合约交易对信息...")
            exchange_info = self.client.futures_exchange_info()
            
            active_symbols = []
            for symbol_info in exchange_info['symbols']:
                if (symbol_info['status'] == 'TRADING' and 
                    symbol_info['contractType'] == 'PERPETUAL' and
                    symbol_info['quoteAsset'] == 'USDT'):
                    
                    # 提取杠杆信息
                    max_leverage = 1
                    for filter_info in symbol_info['filters']:
                        if filter_info['filterType'] == 'MARKET_LOT_SIZE':
                            # 从交易规则中获取最大杠杆（这里需要通过其他方式获取）
                            pass
                    
                    symbol_data = {
                        'symbol': symbol_info['symbol'],
                        'baseAsset': symbol_info['baseAsset'],
                        'quoteAsset': symbol_info['quoteAsset'],
                        'status': symbol_info['status'],
                        'contractType': symbol_info['contractType'],
                        'pricePrecision': symbol_info['pricePrecision'],
                        'quantityPrecision': symbol_info['quantityPrecision'],
                        'maxLeverage': max_leverage  # 需要单独获取
                    }
                    active_symbols.append(symbol_data)
            
            logger.info(f"找到 {len(active_symbols)} 个活跃的USDT永续合约交易对")
            return active_symbols
            
        except Exception as e:
            logger.error(f"获取合约交易对信息失败: {str(e)}")
            return []

    def get_symbol_leverage_info(self, symbol: str) -> int:
        """
        获取交易对的最大杠杆信息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            int: 最大杠杆倍数
        """
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
            logger.debug(f"获取{symbol}杠杆信息失败: {str(e)}")
            return 1

    def get_price_data(self, symbol: str, days: int = 30) -> Optional[List[float]]:
        """
        获取交易对的历史价格数据
        
        Args:
            symbol: 交易对符号
            days: 获取天数
            
        Returns:
            List[float]: 收盘价列表，如果获取失败返回None
        """
        try:
            # 计算开始时间
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            
            # 获取日K线数据
            klines = self.client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_1DAY,
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000),
                limit=days + 5  # 多获取几天以防数据不足
            )
            
            if not klines or len(klines) < days * 0.8:  # 至少要有80%的数据
                logger.warning(f"{symbol}: 价格数据不足，只有{len(klines) if klines else 0}天")
                return None
            
            # 提取收盘价
            close_prices = [float(kline[4]) for kline in klines]
            logger.debug(f"{symbol}: 获取到{len(close_prices)}天的价格数据")
            return close_prices
            
        except Exception as e:
            logger.error(f"获取{symbol}价格数据失败: {str(e)}")
            return None

    def get_funding_rate_interval(self, funding_rates_data: List[Dict]) -> float:
        """
        计算资金费率结算周期
        
        Args:
            funding_rates_data: 资金费率历史数据
            
        Returns:
            float: 结算周期（小时）
        """
        if len(funding_rates_data) < 2:
            return 8.0  # 默认8小时
        
        intervals = []
        for i in range(1, min(10, len(funding_rates_data))):  # 取前10个间隔计算平均值
            prev_time = funding_rates_data[i-1]['fundingTime']
            curr_time = funding_rates_data[i]['fundingTime']
            interval_ms = curr_time - prev_time
            interval_hours = interval_ms / (1000 * 60 * 60)
            intervals.append(interval_hours)
        
        if intervals:
            avg_interval = sum(intervals) / len(intervals)
            logger.debug(f"计算得出的平均资金费率结算周期: {avg_interval:.1f}小时")
            return avg_interval
        
        return 8.0  # 默认8小时

    def get_funding_rate_history(self, symbol: str, days: int = 30) -> Optional[List[float]]:
        """
        获取交易对的资金费率历史数据
        
        Args:
            symbol: 交易对符号
            days: 获取天数
            
        Returns:
            List[float]: 资金费率列表，如果获取失败返回None
        """
        try:
            # 计算开始时间（资金费率每8小时一次）
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            
            # 获取资金费率历史
            funding_rates_data = self.client.futures_funding_rate(
                symbol=symbol,
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000),
                limit=days * 3 + 10  # 每天3次，多获取一些
            )
            
            if not funding_rates_data:
                logger.warning(f"{symbol}: 无资金费率数据")
                return None
            
            # 计算结算周期
            self.funding_interval_hours = self.get_funding_rate_interval(funding_rates_data)
            
            # 提取资金费率
            rates = [float(rate['fundingRate']) for rate in funding_rates_data]
            logger.debug(f"{symbol}: 获取到{len(rates)}个资金费率数据点，结算周期{self.funding_interval_hours:.1f}小时")
            return rates
            
        except Exception as e:
            logger.error(f"获取{symbol}资金费率数据失败: {str(e)}")
            return None

    def calculate_price_volatility(self, prices: List[float]) -> float:
        """
        计算价格波动率
        
        Args:
            prices: 价格列表
            
        Returns:
            float: 波动率（最高价与最低价的差值占最低价的百分比）
        """
        if not prices or len(prices) < 2:
            return float('inf')
        
        min_price = min(prices)
        max_price = max(prices)
        
        if min_price == 0:
            return float('inf')
        
        volatility = (max_price - min_price) / min_price
        return volatility

    def calculate_annualized_funding_rate(self, avg_rate: float, leverage: int) -> float:
        """
        计算年化资金费率收益
        
        Args:
            avg_rate: 平均资金费率
            leverage: 合约杠杆率
            
        Returns:
            float: 年化收益率（百分比）
        """
        # 公式: 平均资金费率 * 24/资金费结算周期 * 365 * 合约杠杆率 * 100
        funding_interval = getattr(self, 'funding_interval_hours', 8.0)
        annualized_rate = avg_rate * (24 / funding_interval) * 365 * leverage * 100
        return annualized_rate

    def analyze_funding_rate_direction(self, funding_rates: List[float]) -> Dict[str, Any]:
        """
        分析资金费率的方向性
        
        Args:
            funding_rates: 资金费率列表
            
        Returns:
            Dict: 包含方向分析结果的字典
        """
        if not funding_rates:
            return {
                'is_consistent': False,
                'direction': 'unknown',
                'positive_ratio': 0,
                'negative_ratio': 0,
                'avg_rate': 0,
                'total_count': 0,
                'annualized_rate': 0
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
            'zero_count': zero_count,
            'annualized_rate': 0  # 将在analyze_symbol中计算
        }

    def analyze_symbol(self, symbol_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        分析单个交易对
        
        Args:
            symbol_info: 交易对信息
            
        Returns:
            Dict: 分析结果，如果不符合条件返回None
        """
        symbol = symbol_info['symbol']
        logger.info(f"分析交易对: {symbol}")
        
        try:
            # 1. 获取杠杆信息
            max_leverage = self.get_symbol_leverage_info(symbol)
            if max_leverage < self.min_leverage:
                logger.debug(f"{symbol}: 最大杠杆{max_leverage}小于要求的{self.min_leverage}")
                return None
            
            # 2. 获取价格数据
            prices = self.get_price_data(symbol, self.days_to_analyze)
            if prices is None:
                return None
            
            # 3. 计算价格波动率
            volatility = self.calculate_price_volatility(prices)
            if volatility > self.price_volatility_threshold:
                logger.debug(f"{symbol}: 价格波动率{volatility:.2%}超过阈值{self.price_volatility_threshold:.2%}")
                return None
            
            # 4. 获取资金费率数据
            funding_rates = self.get_funding_rate_history(symbol, self.days_to_analyze)
            if funding_rates is None:
                return None
            
            # 5. 分析资金费率方向性
            funding_analysis = self.analyze_funding_rate_direction(funding_rates)
            if not funding_analysis['is_consistent']:
                logger.debug(f"{symbol}: 资金费率方向不一致")
                return None
            
            # 6. 计算年化资金费率收益
            annualized_rate = self.calculate_annualized_funding_rate(
                funding_analysis['avg_rate'], 
                max_leverage
            )
            funding_analysis['annualized_rate'] = annualized_rate
            
            # 符合所有条件，返回分析结果
            result = {
                'symbol': symbol,
                'baseAsset': symbol_info['baseAsset'],
                'exchange': self.exchange_name,
                'maxLeverage': max_leverage,
                'priceVolatility': volatility,
                'fundingRateAnalysis': funding_analysis,
                'currentPrice': prices[-1] if prices else 0,
                'priceRange': {
                    'min': min(prices) if prices else 0,
                    'max': max(prices) if prices else 0
                },
                'analysisDate': datetime.now().isoformat(),
                'daysAnalyzed': self.days_to_analyze,
                'fundingIntervalHours': getattr(self, 'funding_interval_hours', 8.0)
            }
            
            logger.info(f"{symbol}: 符合条件! 杠杆={max_leverage}, 波动率={volatility:.2%}, "
                       f"资金费率方向={funding_analysis['direction']}, 年化收益={annualized_rate:.2f}%")
            return result
            
        except Exception as e:
            logger.error(f"分析{symbol}时发生错误: {str(e)}")
            return None

    def scan_all_contracts(self) -> List[Dict[str, Any]]:
        """
        扫描所有合约交易对
        
        Returns:
            List[Dict]: 符合条件的交易对列表
        """
        logger.info("开始扫描所有Binance合约交易对...")
        
        # 获取所有交易对
        all_symbols = self.get_all_futures_symbols()
        if not all_symbols:
            logger.error("无法获取交易对列表")
            return []
        
        qualified_symbols = []
        total_count = len(all_symbols)
        
        for i, symbol_info in enumerate(all_symbols, 1):
            logger.info(f"进度: {i}/{total_count} - 分析 {symbol_info['symbol']}")
            
            # 分析交易对
            result = self.analyze_symbol(symbol_info)
            if result:
                qualified_symbols.append(result)
            
            # 添加延迟以避免API限制
            time.sleep(0.1)
            
            # 每处理50个交易对输出一次进度
            if i % 50 == 0:
                logger.info(f"已处理 {i}/{total_count} 个交易对，找到 {len(qualified_symbols)} 个符合条件的交易对")
        
        logger.info(f"扫描完成! 总共分析了 {total_count} 个交易对，找到 {len(qualified_symbols)} 个符合条件的交易对")
        return qualified_symbols

    def generate_report(self, qualified_symbols: List[Dict[str, Any]]):
        """
        生成分析报告
        
        Args:
            qualified_symbols: 符合条件的交易对列表
        """
        # 保存详细的JSON报告
        report_data = {
            'scanDate': datetime.now().isoformat(),
            'scanParameters': {
                'priceVolatilityThreshold': self.price_volatility_threshold,
                'minLeverage': self.min_leverage,
                'daysAnalyzed': self.days_to_analyze
            },
            'totalQualified': len(qualified_symbols),
            'qualifiedSymbols': qualified_symbols
        }
        
        with open(self.report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        
        # 生成文本摘要
        summary_lines = [
            "=" * 80,
            "Binance合约扫描报告",
            "=" * 80,
            f"扫描时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"扫描参数:",
            f"  - 价格波动率阈值: {self.price_volatility_threshold:.1%}",
            f"  - 最小杠杆要求: {self.min_leverage}x",
            f"  - 分析天数: {self.days_to_analyze}天",
            "",
            f"扫描结果: 找到 {len(qualified_symbols)} 个符合条件的交易对",
            "=" * 80,
            ""
        ]
        
        if qualified_symbols:
            summary_lines.append("符合条件的交易对详情:")
            summary_lines.append("-" * 80)
            
            for i, symbol_data in enumerate(qualified_symbols, 1):
                funding_analysis = symbol_data['fundingRateAnalysis']
                funding_interval = symbol_data.get('fundingIntervalHours', 8.0)
                summary_lines.extend([
                    f"{i}. {symbol_data['symbol']} ({symbol_data['baseAsset']})",
                    f"   最大杠杆: {symbol_data['maxLeverage']}x",
                    f"   价格波动率: {symbol_data['priceVolatility']:.2%}",
                    f"   当前价格: ${symbol_data['currentPrice']:.6f}",
                    f"   价格区间: ${symbol_data['priceRange']['min']:.6f} - ${symbol_data['priceRange']['max']:.6f}",
                    f"   资金费率方向: {funding_analysis['direction']}",
                    f"   资金费率一致性: {funding_analysis['positive_ratio']:.1%} 正 / {funding_analysis['negative_ratio']:.1%} 负",
                    f"   平均资金费率: {funding_analysis['avg_rate']:.6f}",
                    f"   资金费率结算周期: {funding_interval:.1f}小时",
                    f"   年化收益率: {funding_analysis['annualized_rate']:.2f}%",
                    ""
                ])
        else:
            summary_lines.append("未找到符合条件的交易对")
        
        summary_lines.extend([
            "=" * 80,
            f"详细报告已保存到: {self.report_file}",
            "=" * 80
        ])
        
        summary_text = "\n".join(summary_lines)
        
        # 保存摘要文件
        with open(self.summary_file, 'w', encoding='utf-8') as f:
            f.write(summary_text)
        
        # 输出到控制台
        print(summary_text)
        
        logger.info(f"报告已生成:")
        logger.info(f"  详细报告: {self.report_file}")
        logger.info(f"  摘要报告: {self.summary_file}")

    def run(self):
        """
        运行扫描器
        """
        try:
            start_time = datetime.now()
            logger.info("=" * 60)
            logger.info("Binance合约扫描器启动")
            logger.info("=" * 60)
            
            # 扫描所有合约
            qualified_symbols = self.scan_all_contracts()
            
            # 生成报告
            self.generate_report(qualified_symbols)
            
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"扫描完成，耗时: {duration}")
            
        except KeyboardInterrupt:
            logger.info("用户中断扫描")
        except Exception as e:
            logger.error(f"扫描过程中发生错误: {str(e)}")
            raise


def main():
    """主函数"""
    try:
        # 创建扫描器实例
        scanner = BinanceContractScanner()
        
        # 运行扫描
        scanner.run()
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 