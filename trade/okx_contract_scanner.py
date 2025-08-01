#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
OKX合约扫描器

该脚本用于扫描OKX所有合约交易对，找到符合以下条件的交易对：
1. 最近30天内价格波动小于20%
2. 资金费率一直为正或一直为负（即保持一个方向）
3. 最大杠杆大于等于20

主要功能：
1. 获取OKX所有合约交易对信息
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
from config import okx_api_key, okx_api_secret, okx_api_passphrase, proxies, project_root
import ccxt
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging
import numpy as np

# 设置日志级别
logger.setLevel(logging.INFO)

class OKXContractScanner:
    """OKX合约扫描器"""
    
    def __init__(self, api_key: str = None, api_secret: str = None, api_passphrase: str = None,
                 price_volatility_threshold: float = 0.50, min_leverage: int = 20, days_to_analyze: int = 30):
        """
        初始化OKX客户端
        
        Args:
            api_key: OKX API Key
            api_secret: OKX API Secret
            api_passphrase: OKX API Passphrase
            price_volatility_threshold: 价格波动率阈值
            min_leverage: 最小杠杆要求
            days_to_analyze: 分析天数
        """
        self.exchange = ccxt.okx({
            'apiKey': api_key or okx_api_key,
            'secret': api_secret or okx_api_secret,
            'password': api_passphrase or okx_api_passphrase,
            'enableRateLimit': True,
            'proxies': proxies,
            'options': {
                'defaultType': 'swap',  # 使用永续合约
            }
        })
        
        # 确保报告目录存在
        self.reports_dir = os.path.join(project_root, 'trade/reports')
        os.makedirs(self.reports_dir, exist_ok=True)
        
        # 生成报告文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.report_file = os.path.join(self.reports_dir, f'okx_contract_scan_{timestamp}.json')
        self.summary_file = os.path.join(self.reports_dir, f'okx_contract_summary_{timestamp}.txt')
        
        # 扫描参数
        self.price_volatility_threshold = price_volatility_threshold
        self.min_leverage = min_leverage
        self.days_to_analyze = days_to_analyze
        self.exchange_name = "OKX"  # 交易所名称
        
        logger.info(f"OKX合约扫描器初始化完成")
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
            logger.info("获取OKX所有合约交易对信息...")
            
            # 直接使用OKX API获取合约信息，绕过ccxt的解析问题
            try:
                # 使用OKX的公共API获取合约信息
                response = self.exchange.publicGetPublicInstruments({
                    'instType': 'SWAP'
                })
                
                logger.debug(f"OKX API响应: {response}")
                
                if not response or 'data' not in response:
                    logger.error("无法获取OKX合约数据")
                    logger.error(f"响应内容: {response}")
                    return []
                
                logger.info(f"获取到 {len(response['data'])} 个合约信息")
                
                active_symbols = []
                for i, instrument in enumerate(response['data']):
                    if not instrument:
                        continue
                    
                    # 添加调试信息
                    if i < 5:  # 只打印前5个用于调试
                        logger.debug(f"合约 {i}: {instrument}")
                    
                    # 获取基础信息
                    inst_id = instrument.get('instId', '')
                    # OKX的数据结构中，baseCcy可能为空，需要从instId中解析
                    settle_ccy = instrument.get('settleCcy', '')
                    state = instrument.get('state', '')
                    ct_type = instrument.get('ctType', '')
                    
                    # 只处理USDT永续合约且状态为live的
                    if (settle_ccy == 'USDT' and 
                        state == 'live' and 
                        ct_type == 'linear' and
                        inst_id and 
                        '-USDT-SWAP' in inst_id):
                        
                        # 从instId中提取基础资产名称
                        base_ccy = inst_id.replace('-USDT-SWAP', '')
                        
                        # 构造符合ccxt格式的symbol
                        symbol = f"{base_ccy}/USDT:USDT"
                        
                        symbol_data = {
                            'symbol': symbol,
                            'baseAsset': base_ccy,
                            'quoteAsset': 'USDT',
                            'status': 'TRADING',
                            'contractType': 'PERPETUAL',
                            'pricePrecision': 8,  # 默认精度
                            'quantityPrecision': 8,  # 默认精度
                            'maxLeverage': int(instrument.get('lever', 100)),  # 使用API返回的杠杆信息
                            'exchange': self.exchange_name,
                            'instId': inst_id  # 保存原始的instId用于后续API调用
                        }
                        active_symbols.append(symbol_data)
                
                logger.info(f"找到 {len(active_symbols)} 个活跃的USDT永续合约交易对")
                return active_symbols
                
            except Exception as e:
                logger.error(f"直接API调用失败: {str(e)}")
                # 如果直接API调用失败，尝试使用预定义的主要交易对
                logger.info("使用预定义的主要交易对列表...")
                
                major_symbols = [
                    'BTC/USDT:USDT', 'ETH/USDT:USDT', 'BNB/USDT:USDT', 'ADA/USDT:USDT',
                    'XRP/USDT:USDT', 'SOL/USDT:USDT', 'DOT/USDT:USDT', 'DOGE/USDT:USDT',
                    'AVAX/USDT:USDT', 'SHIB/USDT:USDT', 'MATIC/USDT:USDT', 'LTC/USDT:USDT',
                    'UNI/USDT:USDT', 'LINK/USDT:USDT', 'ATOM/USDT:USDT', 'ETC/USDT:USDT',
                    'XLM/USDT:USDT', 'BCH/USDT:USDT', 'ALGO/USDT:USDT', 'VET/USDT:USDT'
                ]
                
                active_symbols = []
                for symbol in major_symbols:
                    base_asset = symbol.split('/')[0]
                    symbol_data = {
                        'symbol': symbol,
                        'baseAsset': base_asset,
                        'quoteAsset': 'USDT',
                        'status': 'TRADING',
                        'contractType': 'PERPETUAL',
                        'pricePrecision': 8,
                        'quantityPrecision': 8,
                        'maxLeverage': 100,
                        'exchange': self.exchange_name
                    }
                    active_symbols.append(symbol_data)
                
                logger.info(f"使用预定义列表，共 {len(active_symbols)} 个交易对")
                return active_symbols
            
        except Exception as e:
            logger.error(f"获取合约交易对信息失败: {str(e)}")
            import traceback
            logger.error(f"详细错误信息: {traceback.format_exc()}")
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
            # 尝试通过市场信息获取杠杆
            markets = self.exchange.markets
            if symbol in markets:
                market = markets[symbol]
                # OKX的杠杆信息可能在limits中
                if 'limits' in market and 'leverage' in market['limits']:
                    max_leverage = market['limits']['leverage'].get('max', 1)
                    if max_leverage:
                        return int(max_leverage)
            
            # OKX通常支持较高杠杆，默认返回100
            return 100
            
        except Exception as e:
            logger.debug(f"获取{symbol}杠杆信息失败: {str(e)}")
            return 100  # 默认返回100倍杠杆

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
            since = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
            
            # 尝试使用原始的OKX instId格式
            inst_id = None
            if symbol.endswith('/USDT:USDT'):
                base_asset = symbol.split('/')[0]
                inst_id = f"{base_asset}-USDT-SWAP"
            
            # 首先尝试使用原始API调用
            if inst_id:
                try:
                    # 使用OKX原生API获取K线数据
                    response = self.exchange.publicGetMarketCandles({
                        'instId': inst_id,
                        'bar': '1D',  # 日K线
                        'before': str(since),
                        'limit': str(days + 5)
                    })
                    
                    if response and 'data' in response and response['data']:
                        # OKX返回格式: [timestamp, open, high, low, close, volume, volCcy, volCcyQuote, confirm]
                        ohlcv_data = []
                        for candle in response['data']:
                            if len(candle) >= 5:
                                ohlcv_data.append([
                                    int(candle[0]),  # timestamp
                                    float(candle[1]),  # open
                                    float(candle[2]),  # high
                                    float(candle[3]),  # low
                                    float(candle[4]),  # close
                                    float(candle[5]) if len(candle) > 5 else 0  # volume
                                ])
                        
                        if len(ohlcv_data) >= days * 0.8:  # 至少要有80%的数据
                            close_prices = [float(candle[4]) for candle in ohlcv_data]
                            logger.debug(f"{symbol}: 通过原生API获取到{len(close_prices)}天的价格数据")
                            return close_prices
                        else:
                            logger.warning(f"{symbol}: 原生API价格数据不足，只有{len(ohlcv_data)}天")
                            
                except Exception as api_error:
                    logger.debug(f"{symbol}: 原生API调用失败: {str(api_error)}")
            
            # 如果原生API失败，尝试使用ccxt的fetch_ohlcv
            try:
                ohlcv = self.exchange.fetch_ohlcv(
                    symbol=symbol,
                    timeframe='1d',
                    since=since,
                    limit=days + 5
                )
                
                if not ohlcv or len(ohlcv) < days * 0.8:  # 至少要有80%的数据
                    logger.warning(f"{symbol}: ccxt价格数据不足，只有{len(ohlcv) if ohlcv else 0}天")
                    return None
                
                # 提取收盘价 (OHLCV格式: [timestamp, open, high, low, close, volume])
                close_prices = [float(candle[4]) for candle in ohlcv]
                logger.debug(f"{symbol}: 通过ccxt获取到{len(close_prices)}天的价格数据")
                return close_prices
                
            except Exception as ccxt_error:
                logger.debug(f"{symbol}: ccxt调用失败: {str(ccxt_error)}")
                return None
            
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
            logger.warning(f"合约资金费率历史数据小于2，使用默认值8.0")
            return 8.0  # 默认8小时
        else:
            return abs((int(funding_rates_data[-1]['fundingTime']) - int(funding_rates_data[-2]['fundingTime'])) / (1000 * 60 * 60))

        # intervals = []
        # for i in range(1, min(10, len(funding_rates_data))):  # 取前10个间隔计算平均值
        #     # 检查数据格式，兼容不同的时间戳字段名
        #     if 'timestamp' in funding_rates_data[i-1]:
        #         prev_time = funding_rates_data[i-1]['timestamp']
        #         curr_time = funding_rates_data[i]['timestamp']
        #     elif 'fundingTime' in funding_rates_data[i-1]:
        #         prev_time = funding_rates_data[i-1]['fundingTime']
        #         curr_time = funding_rates_data[i]['fundingTime']
        #     else:
        #         logger.warning("无法识别的时间戳字段格式")
        #         return 8.0
        #
        #     interval_ms = curr_time - prev_time
        #     interval_hours = interval_ms / (1000 * 60 * 60)
        #     intervals.append(interval_hours)
        #
        # if intervals:
        #     avg_interval = sum(intervals) / len(intervals)
        #     logger.debug(f"计算得出的平均资金费率结算周期: {avg_interval:.1f}小时")
        #     return avg_interval
        
        # return 8.0  # 默认8小时

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
            # 计算开始时间
            since = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
            
            # 尝试使用原始的OKX instId格式
            inst_id = None
            if symbol.endswith('/USDT:USDT'):
                base_asset = symbol.split('/')[0]
                inst_id = f"{base_asset}-USDT-SWAP"
            
            funding_rates_raw = None
            
            # 首先尝试使用原始API调用
            if inst_id:
                try:
                    # 使用OKX原生API获取资金费率历史
                    response = self.exchange.publicGetPublicFundingRateHistory({
                        'instId': inst_id,
                        'before': str(since),
                        'limit': str(days * 3 + 10)  # 每天3次，多获取一些
                    })
                    
                    if response and 'data' in response and response['data']:
                        funding_rates_raw = response['data']
                        rates = []
                        for rate_data in response['data']:
                            if 'fundingRate' in rate_data and rate_data['fundingRate'] is not None:
                                rates.append(float(rate_data['fundingRate']))
                        
                        if rates:
                            # 计算结算周期
                            self.funding_interval_hours = self.get_funding_rate_interval(funding_rates_raw)
                            logger.debug(f"{symbol}: 通过原生API获取到{len(rates)}个资金费率数据点，结算周期{self.funding_interval_hours:.1f}小时")
                            return rates
                        else:
                            logger.warning(f"{symbol}: 原生API无有效资金费率数据")
                            
                except Exception as api_error:
                    logger.debug(f"{symbol}: 原生API资金费率调用失败: {str(api_error)}")
            
            # 如果原生API失败，尝试使用ccxt的fetch_funding_rate_history
            try:
                funding_rates = self.exchange.fetch_funding_rate_history(
                    symbol=symbol,
                    since=since,
                    limit=days * 3 + 10  # 每天3次，多获取一些
                )
                
                if not funding_rates:
                    logger.warning(f"{symbol}: ccxt无资金费率数据")
                    return None
                
                # 计算结算周期
                self.funding_interval_hours = self.get_funding_rate_interval(funding_rates)
                
                # 提取资金费率
                rates = [float(rate['fundingRate']) for rate in funding_rates if rate['fundingRate'] is not None]
                logger.debug(f"{symbol}: 通过ccxt获取到{len(rates)}个资金费率数据点，结算周期{self.funding_interval_hours:.1f}小时")
                return rates
                
            except Exception as ccxt_error:
                logger.debug(f"{symbol}: ccxt资金费率调用失败: {str(ccxt_error)}")
                return None
            
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
        logger.info("开始扫描所有OKX合约交易对...")
        
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
            time.sleep(0.2)
            
            # 每处理20个交易对输出一次进度
            if i % 20 == 0:
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
            'exchange': self.exchange_name,
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
            f"{self.exchange_name}合约扫描报告",
            "=" * 80,
            f"扫描时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"交易所: {self.exchange_name}",
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
                    f"{i}. {symbol_data['symbol']} ({symbol_data['baseAsset']}) - {symbol_data['exchange']}",
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
            logger.info(f"{self.exchange_name}合约扫描器启动")
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
        scanner = OKXContractScanner()
        
        # 运行扫描
        scanner.run()
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 