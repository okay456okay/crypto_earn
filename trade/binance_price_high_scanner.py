#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binance价格高点扫描器

该脚本用于扫描Binance所有合约交易对，监控价格突破情况：
1. 获取所有合约对近30天的30分钟K线数据
2. 检查最后一根K线价格是否为30天最高点
3. 如果是最高点，发送企业微信群机器人通知

通知内容包含：
- 当前价格
- 资金费率、资金费结算周期
- 历史最高价、历史最低价、市值、Twitter ID、Github地址、发行日期
- 合约描述
- 合约tags

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
import requests
import pickle
import hashlib
import argparse
import sqlite3
import asyncio
import ccxt.pro as ccxtpro

# 设置日志级别
logger.setLevel(logging.INFO)

class BinancePriceHighScanner:
    """Binance价格高点扫描器"""
    
    def __init__(self, api_key: str = None, api_secret: str = None, days_to_analyze: int = 30, enable_trading: bool = False):
        """
        初始化Binance客户端
        
        Args:
            api_key: Binance API Key
            api_secret: Binance API Secret
            days_to_analyze: 分析历史天数
            enable_trading: 是否启用自动交易功能
        """
        self.client = Client(
            api_key or binance_api_key, 
            api_secret or binance_api_secret,
            requests_params={'proxies': proxies}
        )
        
        # 企业微信群机器人webhook
        self.webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=43c4c655-b144-4e1f-b054-4b3a9e2caf26"
        
        # 分析天数
        self.days_to_analyze = days_to_analyze
        
        # 交易功能
        self.enable_trading = enable_trading
        
        # 交易参数
        self.leverage = 10  # 杠杆倍数
        self.margin_amount = 20  # 保证金金额(USDT)
        
        # 过滤条件
        self.min_launch_days = 15  # 最小上市天数
        self.max_market_rank = 100  # 最大市值排名
        self.min_funding_rate = -0.005  # 最小资金费率 (-0.005%)
        
        # 交易所客户端(用于交易)
        self.binance_trading = None
        if self.enable_trading:
            self.binance_trading = ccxtpro.binance({
                'apiKey': api_key or binance_api_key,
                'secret': api_secret or binance_api_secret,
                'enableRateLimit': True,
                'proxies': proxies,
                'options': {
                    'defaultType': 'future',  # 设置为合约模式
                }
            })
        
        # 缓存目录
        self.cache_dir = os.path.join(project_root, 'trade/cache')
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # 通知记录目录
        self.notifications_dir = os.path.join(project_root, 'trade/notifications')
        os.makedirs(self.notifications_dir, exist_ok=True)
        
        # 交易记录数据库
        self.db_path = os.path.join(project_root, 'trade/trading_records.db')
        if self.enable_trading:
            self.init_trading_db()
        
        # 缓存文件路径
        self.token_info_cache = os.path.join(self.cache_dir, 'token_info_cache.pkl')
        self.symbol_description_cache = os.path.join(self.cache_dir, 'symbol_description_cache.pkl')
        self.products_cache = os.path.join(self.cache_dir, 'products_cache.pkl')
        
        # 缓存过期时间（1天）
        self.cache_expire_hours = 24
        
        # 加载缓存数据
        self.token_info_data = self.load_cache_with_expiry(self.token_info_cache)
        self.symbol_description_data = self.load_cache_with_expiry(self.symbol_description_cache)
        self.products_data = self.load_cache_with_expiry(self.products_cache)
        
        logger.info(f"Binance价格高点扫描器初始化完成，分析天数: {self.days_to_analyze}天，自动交易: {'启用' if self.enable_trading else '禁用'}")

    def init_trading_db(self):
        """初始化交易记录数据库"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 创建交易记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trading_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    order_time TIMESTAMP NOT NULL,
                    open_price REAL NOT NULL,
                    quantity REAL NOT NULL,
                    leverage INTEGER NOT NULL,
                    direction TEXT NOT NULL,
                    order_id TEXT,
                    margin_amount REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(exchange, symbol, order_time)
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info(f"交易记录数据库初始化完成: {self.db_path}")
            
        except Exception as e:
            logger.error(f"交易记录数据库初始化失败: {str(e)}")

    def get_latest_trade_record(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取某个交易对的最新交易记录"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT * FROM trading_records 
                WHERE symbol = ? 
                ORDER BY order_time DESC 
                LIMIT 1
            ''', (symbol,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                columns = ['id', 'exchange', 'symbol', 'order_time', 'open_price', 
                          'quantity', 'leverage', 'direction', 'order_id', 'margin_amount', 'created_at']
                return dict(zip(columns, result))
            
            return None
            
        except Exception as e:
            logger.error(f"获取{symbol}最新交易记录失败: {str(e)}")
            return None

    def save_trade_record(self, symbol: str, open_price: float, quantity: float, order_id: str = None) -> bool:
        """保存交易记录"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO trading_records 
                (exchange, symbol, order_time, open_price, quantity, leverage, direction, order_id, margin_amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', ('Binance', symbol, datetime.now(), open_price, quantity, self.leverage, 'SHORT', order_id, self.margin_amount))
            
            conn.commit()
            conn.close()
            logger.info(f"交易记录已保存: {symbol} 价格={open_price} 数量={quantity}")
            return True
            
        except Exception as e:
            logger.error(f"保存{symbol}交易记录失败: {str(e)}")
            return False

    def remove_trade_record(self, symbol: str) -> bool:
        """删除交易对的所有交易记录"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('DELETE FROM trading_records WHERE symbol = ?', (symbol,))
            deleted_count = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            if deleted_count > 0:
                logger.info(f"已删除{symbol}的{deleted_count}条交易记录")
            
            return True
            
        except Exception as e:
            logger.error(f"删除{symbol}交易记录失败: {str(e)}")
            return False

    def get_all_traded_symbols(self) -> List[str]:
        """获取所有有交易记录的交易对"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT DISTINCT symbol FROM trading_records')
            results = cursor.fetchall()
            conn.close()
            
            return [row[0] for row in results]
            
        except Exception as e:
            logger.error(f"获取交易对列表失败: {str(e)}")
            return []

    def load_cache_with_expiry(self, cache_file: str) -> Dict:
        """加载带过期时间的缓存数据"""
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'rb') as f:
                    cache_data = pickle.load(f)
                
                # 检查缓存格式和过期时间
                if isinstance(cache_data, dict) and 'timestamp' in cache_data and 'data' in cache_data:
                    cache_time = datetime.fromtimestamp(cache_data['timestamp'])
                    current_time = datetime.now()
                    
                    # 检查是否过期
                    if (current_time - cache_time).total_seconds() < self.cache_expire_hours * 3600:
                        logger.info(f"加载有效缓存: {cache_file}，缓存时间: {cache_time.strftime('%Y-%m-%d %H:%M:%S')}")
                        return cache_data['data']
                    else:
                        logger.info(f"缓存已过期: {cache_file}，将重新获取数据")
                        return {}
                else:
                    # 旧格式缓存，清除重新获取
                    logger.info(f"旧格式缓存: {cache_file}，将重新获取数据")
                    return {}
        except Exception as e:
            logger.warning(f"加载缓存文件 {cache_file} 失败: {str(e)}")
        return {}

    def save_cache_with_expiry(self, cache_file: str, data: Dict):
        """保存带过期时间的缓存数据"""
        try:
            cache_data = {
                'timestamp': time.time(),
                'data': data
            }
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
            logger.debug(f"缓存已保存: {cache_file}")
        except Exception as e:
            logger.error(f"保存缓存文件 {cache_file} 失败: {str(e)}")

    def load_cache(self, cache_file: str) -> Dict:
        """加载缓存数据（兼容旧方法）"""
        return self.load_cache_with_expiry(cache_file)

    def save_cache(self, cache_file: str, data: Dict):
        """保存缓存数据（兼容旧方法）"""
        self.save_cache_with_expiry(cache_file, data)

    def get_all_futures_symbols(self) -> List[str]:
        """
        获取所有合约交易对符号
        
        Returns:
            List[str]: 交易对符号列表
        """
        try:
            logger.info("获取Binance所有合约交易对...")
            exchange_info = self.client.futures_exchange_info()
            
            symbols = []
            for symbol_info in exchange_info['symbols']:
                if (symbol_info['status'] == 'TRADING' and 
                    symbol_info['contractType'] == 'PERPETUAL' and
                    symbol_info['quoteAsset'] == 'USDT'):
                    symbols.append(symbol_info['symbol'])
            
            logger.info(f"找到 {len(symbols)} 个活跃的USDT永续合约交易对")
            return symbols
            
        except Exception as e:
            logger.error(f"获取合约交易对信息失败: {str(e)}")
            return []

    def get_30min_klines(self, symbol: str, days: int = None) -> Optional[List[List]]:
        """
        获取30分钟K线数据
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data
        Args:
            symbol: 交易对符号
            days: 获取天数，如果为None则使用实例的默认值
            
        Returns:
            List[List]: K线数据列表
        [
            [
                1499040000000,      // Open time
                "0.01634790",       // Open
                "0.80000000",       // High
                "0.01575800",       // Low
                "0.01577100",       // Close
                "148976.11427815",  // Volume
                1499644799999,      // Close time
                "2434.19055334",    // Quote asset volume
                308,                // Number of trades
                "1756.87402397",    // Taker buy base asset volume
                "28.46694368",      // Taker buy quote asset volume
                "17928899.62484339" // Ignore.
            ]
        ]
    """
        if days is None:
            days = self.days_to_analyze
            
        try:
            # 计算时间范围
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            
            # 获取30分钟K线数据
            klines = self.client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_30MINUTE,
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000),
                limit=1440  # 30天*24小时*2(30分钟) = 1440, Default 500; max 1500.
            )
            
            if not klines:
                logger.warning(f"{symbol}: 未获取到K线数据")
                return None
                
            logger.debug(f"{symbol}: 获取到{len(klines)}根30分钟K线")
            return klines
            
        except Exception as e:
            logger.error(f"获取{symbol}的30分钟K线数据失败: {str(e)}")
            return None

    def check_price_breakouts(self, klines: List[List]) -> Dict[str, Any]:
        """
        检查最后一根K线价格是否为多个时间区间的最高点
        
        Args:
            klines: K线数据列表（30天的30分钟K线）
            
        Returns:
            Dict: 包含当前价格和各时间区间突破信息的字典
            {
                'current_price': float,
                'has_breakout': bool,
                'breakout_periods': list,
                'breakouts': {
                    7: {'is_high': bool, 'max_high': float, 'min_low': float},
                    15: {'is_high': bool, 'max_high': float, 'min_low': float},
                    30: {'is_high': bool, 'max_high': float, 'min_low': float}
                }
            }
        """
        if not klines or len(klines) == 0:
            return {
                'current_price': 0.0,
                'has_breakout': False,
                'breakout_periods': [],
                'breakouts': {}
            }
        
        # 获取最后一根K线的收盘价
        current_price = float(klines[-1][4])  # 索引4是收盘价
        
        # 每30分钟一根K线，计算各时间区间对应的K线数量
        periods = {
            7: 7 * 24 * 2,    # 7天 = 7 * 24小时 * 2(每小时2根30分钟K线) = 336根
            15: 15 * 24 * 2,  # 15天 = 720根
            30: 30 * 24 * 2   # 30天 = 1440根
        }
        
        breakouts = {}
        breakout_periods = []
        has_breakout = False
        
        for days, kline_count in periods.items():
            # 确保不超过实际K线数量
            actual_count = min(kline_count, len(klines) - 1)  # 排除最后一根K线
            
            if actual_count <= 0:
                breakouts[days] = {
                    'is_high': False,
                    'max_high': 0.0,
                    'min_low': 0.0
                }
                continue
            
            # 获取指定时间区间的K线数据（从倒数第二根开始往前数）
            period_klines = klines[-(actual_count+1):-1]  # 排除最后一根K线
            
            # 提取该时间区间的高点和低点价格
            high_prices = [float(kline[2]) for kline in period_klines]  # 索引2是高点价格
            low_prices = [float(kline[3]) for kline in period_klines]   # 索引3是低点价格
            
            if high_prices and low_prices:
                max_high = max(high_prices)
                min_low = min(low_prices)
                
                # 检查当前价格是否等于或超过该时间区间的最高点
                is_high = current_price >= max_high
                
                breakouts[days] = {
                    'is_high': is_high,
                    'max_high': max_high,
                    'min_low': min_low
                }
                
                if is_high:
                    breakout_periods.append(days)
                    has_breakout = True
            else:
                breakouts[days] = {
                    'is_high': False,
                    'max_high': 0.0,
                    'min_low': 0.0
                }
        
        return {
            'current_price': current_price,
            'has_breakout': has_breakout,
            'breakout_periods': breakout_periods,
            'breakouts': breakouts
        }

    def get_funding_rate_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取资金费率信息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            Dict: 资金费率信息
        """
        try:
            # 获取当前资金费率
            funding_rate = self.client.futures_funding_rate(symbol=symbol, limit=1)
            
            if funding_rate:
                current_rate = float(funding_rate[0]['fundingRate'])
                # 资金费率通常每8小时结算一次
                settlement_hours = 8
                # 年化资金费率 = 当前费率 * (365 * 24 / 8) * 100
                annualized_rate = current_rate * (365 * 24 / settlement_hours) * 100
                
                return {
                    'current_rate': current_rate,
                    'current_rate_percent': current_rate * 100,
                    'annualized_rate': annualized_rate,
                    'settlement_hours': settlement_hours
                }
            
        except Exception as e:
            logger.error(f"获取{symbol}资金费率失败: {str(e)}")
        
        return {
            'current_rate': 0.0,
            'current_rate_percent': 0.0,
            'annualized_rate': 0.0,
            'settlement_hours': 8
        }

    def get_token_info(self, base_asset: str) -> Dict[str, Any]:
        """
        获取代币详细信息（带缓存）
        
        Args:
            base_asset: 基础资产符号
            
        Returns:
            Dict: 代币信息
        """
        # 检查缓存
        if base_asset in self.token_info_data:
            return self.token_info_data[base_asset]
        
        try:
            url = f"https://www.binance.com/bapi/apex/v1/friendly/apex/marketing/web/token-info?symbol={base_asset}"
            
            response = requests.get(url, proxies=proxies, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('success') and data.get('data'):
                token_data = data['data']
                
                info = {
                    'market_cap': token_data.get('fdmc', 0),  # 完全稀释市值
                    'ath_price': token_data.get('athpu', 0),  # 历史最高价
                    'atl_price': token_data.get('atlpu', 0),  # 历史最低价
                    'twitter_username': token_data.get('xhn', ''),  # Twitter用户名
                    'twitter_id': token_data.get('xu', ''),  # Twitter ID
                    'github_url': token_data.get('ru', ''),  # Github地址
                    'launch_date': token_data.get('ald', 0),  # 发行日期时间戳
                    'website_url': token_data.get('ws', ''),  # 官网地址
                    'description': token_data.get('dbk', ''),  # 描述key
                    'market_dominance': token_data.get('dmc', 0),  # 市场占用率
                    'market_rank': token_data.get('rk', 0),  # 市值排名
                    'twitter_last_update': token_data.get('xlut', 0),  # Twitter最后更新时间
                    'repo_update_time': token_data.get('rut', 0),  # 仓库更新时间
                }
                
                # 格式化发行日期
                if info['launch_date']:
                    try:
                        launch_datetime = datetime.fromtimestamp(info['launch_date'] / 1000)
                        info['launch_date_str'] = launch_datetime.strftime('%Y-%m-%d')
                    except:
                        info['launch_date_str'] = 'Unknown'
                else:
                    info['launch_date_str'] = 'Unknown'
                
                # 格式化Twitter最后更新时间
                if info['twitter_last_update']:
                    try:
                        twitter_datetime = datetime.fromtimestamp(info['twitter_last_update'] / 1000)
                        info['twitter_last_update_str'] = twitter_datetime.strftime('%Y-%m-%d %H:%M')
                    except:
                        info['twitter_last_update_str'] = 'Unknown'
                else:
                    info['twitter_last_update_str'] = 'Unknown'
                
                # 格式化仓库更新时间
                if info['repo_update_time']:
                    try:
                        repo_datetime = datetime.fromtimestamp(info['repo_update_time'] / 1000)
                        info['repo_update_time_str'] = repo_datetime.strftime('%Y-%m-%d %H:%M')
                    except:
                        info['repo_update_time_str'] = 'Unknown'
                else:
                    info['repo_update_time_str'] = 'Unknown'
                
                # 缓存数据
                self.token_info_data[base_asset] = info
                self.save_cache_with_expiry(self.token_info_cache, self.token_info_data)
                
                return info
            
        except Exception as e:
            logger.error(f"获取{base_asset}代币信息失败: {str(e)}")
        
        # 返回默认值
        default_info = {
            'market_cap': 0,
            'ath_price': 0,
            'atl_price': 0,
            'twitter_username': '',
            'twitter_id': '',
            'github_url': '',
            'launch_date': 0,
            'launch_date_str': 'Unknown',
            'website_url': '',
            'description': '',
            'market_dominance': 0,
            'market_rank': 0,
            'twitter_last_update': 0,
            'twitter_last_update_str': 'Unknown',
            'repo_update_time': 0,
            'repo_update_time_str': 'Unknown'
        }
        
        # 缓存默认值以避免重复请求
        self.token_info_data[base_asset] = default_info
        self.save_cache_with_expiry(self.token_info_cache, self.token_info_data)
        
        return default_info

    def get_symbol_description(self, symbol: str) -> str:
        """
        获取合约描述（带缓存）
        
        Args:
            symbol: 交易对符号
            
        Returns:
            str: 合约描述
        """
        # 检查缓存
        if symbol in self.symbol_description_data:
            return self.symbol_description_data[symbol]
        
        try:
            # 如果缓存为空，一次性获取所有描述
            if not self.symbol_description_data:
                url = "https://bin.bnbstatic.com/api/i18n/-/web/cms/en/symbol-description"
                
                response = requests.get(url, proxies=proxies, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                
                # 解析所有符号描述
                if isinstance(data, dict):
                    for key, value in data.items():
                        if isinstance(value, str):
                            # 提取符号名（通常格式为symbol_desc_XXX）
                            if key.startswith('symbol_desc_'):
                                symbol_name = key.replace('symbol_desc_', '')
                                self.symbol_description_data[symbol_name] = value
                
                # 保存缓存
                self.save_cache_with_expiry(self.symbol_description_cache, self.symbol_description_data)
                
                logger.info(f"获取到{len(self.symbol_description_data)}个符号描述")
            
            # 从缓存中获取描述
            return self.symbol_description_data.get(symbol.replace('USDT', ''), f"No description for {symbol}")
            
        except Exception as e:
            logger.error(f"获取{symbol}描述失败: {str(e)}")
            return f"Failed to get description for {symbol}"

    def get_symbol_tags(self, symbol: str) -> List[str]:
        """
        获取合约标签（带缓存）
        
        Args:
            symbol: 交易对符号
            
        Returns:
            List[str]: 标签列表
        """
        # 检查缓存
        if symbol in self.products_data:
            return self.products_data[symbol]
        
        try:
            # 如果缓存为空，一次性获取所有产品数据
            if not self.products_data:
                url = "https://www.binance.com/bapi/asset/v2/public/asset-service/product/get-products"
                
                response = requests.get(url, proxies=proxies, timeout=15)
                response.raise_for_status()
                
                data = response.json()
                
                if data.get('success') and data.get('data'):
                    products = data['data']
                    
                    for product in products:
                        product_symbol = product.get('s', '')
                        tags = product.get('tags', [])
                        
                        if product_symbol:
                            self.products_data[product_symbol] = tags
                
                # 保存缓存
                self.save_cache_with_expiry(self.products_cache, self.products_data)
                
                logger.info(f"获取到{len(self.products_data)}个产品标签数据")
            
            # 从缓存中获取标签
            return self.products_data.get(symbol, [])
            
        except Exception as e:
            logger.error(f"获取{symbol}标签失败: {str(e)}")
            return []

    def send_wework_notification(self, symbol: str, analysis_data: Dict[str, Any]):
        """
        发送企业微信群机器人通知
        
        Args:
            symbol: 交易对符号
            analysis_data: 分析数据
        """
        try:
            base_asset = symbol.replace('USDT', '')
            
            # 构建突破时间区间信息
            breakout_periods = sorted(analysis_data['breakout_periods'])
            periods_str = ', '.join([f"{days}天" for days in breakout_periods])
            
            # 构建消息内容
            message_lines = [
                f"🚀 **价格突破高点提醒**",
                f"",
                f"**合约**: {symbol}",
                f"**当前价格**: ${analysis_data['current_price']:.6f}",
                f"**突破区间**: {periods_str}",
                f"",
                f"**各时间区间对比**:",
            ]
            
            # 添加各时间区间的详细信息
            for days in [7, 15, 30]:
                if days in analysis_data['breakouts']:
                    breakout_info = analysis_data['breakouts'][days]
                    status = "✅ 突破" if breakout_info['is_high'] else "❌ 未突破"
                    message_lines.extend([
                        f"• {days}天: {status}",
                        f"  └ 最高价: ${breakout_info['max_high']:.6f}",
                        f"  └ 最低价: ${breakout_info['min_low']:.6f}",
                    ])
            
            message_lines.extend([
                f"",
                f"**资金费率信息**:",
                f"• 当前费率: {analysis_data['funding_rate']['current_rate_percent']:.4f}%",
                f"• 年化费率: {analysis_data['funding_rate']['annualized_rate']:.2f}%",
                f"• 结算周期: {analysis_data['funding_rate']['settlement_hours']}小时",
                f"",
                f"**代币信息**:",
                f"• 历史最高价: ${analysis_data['token_info']['ath_price']:.6f}",
                f"• 历史最低价: ${analysis_data['token_info']['atl_price']:.6f}",
                f"• 市值: ${analysis_data['token_info']['market_cap']:,.0f}",
            ])
            
            # 有条件的信息项
            if analysis_data['token_info']['market_rank'] > 0:
                message_lines.append(f"• 市值排名: #{analysis_data['token_info']['market_rank']}")
            
            if analysis_data['token_info']['market_dominance'] > 0:
                message_lines.append(f"• 市场占用率: {analysis_data['token_info']['market_dominance']:.4f}%")
            
            message_lines.append(f"• 发行日期: {analysis_data['token_info']['launch_date_str']}")
            
            if analysis_data['token_info']['website_url']:
                message_lines.append(f"• 官网: {analysis_data['token_info']['website_url']}")
            
            if analysis_data['token_info']['twitter_username']:
                message_lines.append(f"• X用户名: @{analysis_data['token_info']['twitter_username']}")
            
            if analysis_data['token_info']['twitter_id']:
                message_lines.append(f"• X ID: {analysis_data['token_info']['twitter_id']}")
            
            if analysis_data['token_info']['twitter_last_update_str'] != 'Unknown':
                message_lines.append(f"• X更新: {analysis_data['token_info']['twitter_last_update_str']}")
            
            if analysis_data['token_info']['github_url']:
                message_lines.append(f"• Github: {analysis_data['token_info']['github_url']}")
            
            if analysis_data['token_info']['repo_update_time_str'] != 'Unknown':
                message_lines.append(f"• 仓库更新: {analysis_data['token_info']['repo_update_time_str']}")
            
            # 添加剩余的固定信息
            message_lines.extend([
                f"",
                f"**合约描述**: {analysis_data['description'][:100]}..." if len(analysis_data['description']) > 100 else f"**合约描述**: {analysis_data['description']}",
                f"",
                f"**标签**: {', '.join(analysis_data['tags'])}" if analysis_data['tags'] else "**标签**: 无",
                f"",
                f"**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            ])
            
            # 过滤空行
            message_lines = [line for line in message_lines if line is not None and line != ""]
            message_content = "\n".join(message_lines)
            
            # 准备请求数据
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "content": message_content
                }
            }
            
            # 发送请求
            response = requests.post(
                self.webhook_url,
                json=payload,
                proxies=proxies,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    logger.info(f"✅ 成功发送{symbol}突破通知到企业微信群")
                    # 保存通知内容到文件
                    self.save_notification_to_file(symbol, message_content, analysis_data)
                else:
                    logger.error(f"❌ 发送{symbol}通知失败: {result}")
            else:
                logger.error(f"❌ 发送{symbol}通知失败，状态码: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ 发送{symbol}企业微信通知失败: {str(e)}")

    def save_notification_to_file(self, symbol: str, message_content: str, analysis_data: Dict[str, Any]):
        """
        保存通知内容到文件
        
        Args:
            symbol: 交易对符号
            message_content: 消息内容
            analysis_data: 分析数据
        """
        try:
            current_time = datetime.now()
            
            # 按日期创建文件名
            date_str = current_time.strftime('%Y-%m-%d')
            timestamp_str = current_time.strftime('%H-%M-%S')
            
            # 创建日期目录
            date_dir = os.path.join(self.notifications_dir, date_str)
            os.makedirs(date_dir, exist_ok=True)
            
            # 文件名包含时间戳和交易对
            filename = f"{timestamp_str}_{symbol}_breakthrough.txt"
            file_path = os.path.join(date_dir, filename)
            
            # 准备保存的内容
            file_content = [
                f"=" * 80,
                f"价格突破通知记录",
                f"=" * 80,
                f"交易对: {symbol}",
                f"生成时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}",
                f"突破区间: {', '.join([f'{days}天' for days in sorted(analysis_data['breakout_periods'])])}",
                f"当前价格: ${analysis_data['current_price']:.6f}",
                f"",
                f"详细信息:",
                f"-" * 40,
                message_content,
                f"",
                f"=" * 80,
                f""
            ]
            
            # 写入文件
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(file_content))
            
            logger.info(f"💾 通知内容已保存到文件: {file_path}")
            
            # 同时保存到按日期汇总的文件
            summary_file = os.path.join(date_dir, f"{date_str}_summary.txt")
            summary_content = f"[{timestamp_str}] {symbol} - 突破 {', '.join([f'{days}天' for days in sorted(analysis_data['breakout_periods'])])} 高点 - ${analysis_data['current_price']:.6f}\n"
            
            with open(summary_file, 'a', encoding='utf-8') as f:
                f.write(summary_content)
                
        except Exception as e:
            logger.error(f"❌ 保存{symbol}通知到文件失败: {str(e)}")

    def should_filter_symbol(self, symbol: str, analysis_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        检查交易对是否应该被过滤掉
        
        Args:
            symbol: 交易对符号
            analysis_data: 分析数据
            
        Returns:
            Tuple[bool, str]: (是否过滤, 过滤原因)
        """
        token_info = analysis_data['token_info']
        funding_rate = analysis_data['funding_rate']
        
        # 检查上市日期
        launch_date = token_info.get('launch_date', 0)
        if not launch_date or launch_date == 0:
            return True, "上市日期数据为空"
        
        launch_datetime = datetime.fromtimestamp(launch_date / 1000)
        days_since_launch = (datetime.now() - launch_datetime).days
        
        if days_since_launch < self.min_launch_days:
            return True, f"上市仅{days_since_launch}天，小于{self.min_launch_days}天"
        
        # 检查市值排名
        market_rank = token_info.get('market_rank', 0)
        if not market_rank or market_rank == 0:
            return True, "市值排名数据为空"
        
        if market_rank <= self.max_market_rank:
            return True, f"市值排名{market_rank}，在{self.max_market_rank}名以内"
        
        # 检查资金费率
        current_rate = funding_rate.get('current_rate', 0)
        if current_rate == 0:
            return True, "资金费率数据为空"
        
        if current_rate < self.min_funding_rate:
            return True, f"资金费率{current_rate*100:.4f}%，小于{self.min_funding_rate*100:.4f}%"
        
        return False, "通过过滤条件"

    async def get_current_positions(self) -> Dict[str, float]:
        """获取当前合约持仓"""
        try:
            if not self.binance_trading:
                return {}
                
            positions = await self.binance_trading.fetch_positions()
            position_dict = {}
            
            for position in positions:
                symbol = position['symbol']
                size = float(position['contracts'])
                if size != 0:  # 只记录有持仓的
                    position_dict[symbol] = size
                    
            return position_dict
            
        except Exception as e:
            logger.error(f"获取当前持仓失败: {str(e)}")
            return {}

    async def clean_trade_records(self):
        """清理交易记录 - 删除没有持仓的交易对记录"""
        if not self.enable_trading:
            return
            
        try:
            # 获取当前持仓
            current_positions = await self.get_current_positions()
            
            # 获取所有有交易记录的交易对
            traded_symbols = self.get_all_traded_symbols()
            
            # 检查哪些交易对没有持仓了
            for symbol in traded_symbols:
                if symbol not in current_positions:
                    logger.info(f"检测到{symbol}已无持仓，删除交易记录")
                    self.remove_trade_record(symbol)
                    
        except Exception as e:
            logger.error(f"清理交易记录失败: {str(e)}")

    async def execute_short_order(self, symbol: str, current_price: float) -> bool:
        """
        执行卖空订单
        
        Args:
            symbol: 交易对符号
            current_price: 当前价格
            
        Returns:
            bool: 是否执行成功
        """
        try:
            if not self.binance_trading:
                logger.error("交易客户端未初始化")
                return False
            
            # 计算交易数量 (保证金 * 杠杆 / 价格)
            quantity = (self.margin_amount * self.leverage) / current_price
            
            # 设置杠杆
            # await self.binance_trading.set_leverage(self.leverage, symbol)
            # 设置Binance合约参数
            await self.binance_trading.fapiPrivatePostLeverage({
                'symbol': symbol,
                'leverage': self.leverage
            })
            logger.info(f"已设置{symbol}杠杆为{self.leverage}倍")

            # 执行市价卖空订单
            order = await self.binance_trading.create_market_sell_order(
                symbol=symbol,
                amount=quantity,
                params={'positionSide': 'SHORT'}
            )
            
            if order and order.get('id'):
                order_id = order.get('id')
                filled_price = float(order.get('average', 0) or current_price)
                filled_quantity = float(order.get('filled', 0) or quantity)
                
                logger.info(f"✅ 卖空订单执行成功: {symbol}")
                logger.info(f"订单ID: {order_id}")
                logger.info(f"成交价格: {filled_price}")
                logger.info(f"成交数量: {filled_quantity}")
                
                # 保存交易记录
                self.save_trade_record(symbol, filled_price, filled_quantity, order_id)
                
                return True
            else:
                logger.error(f"❌ 卖空订单执行失败: {symbol}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 执行{symbol}卖空订单失败: {str(e)}")
            return False

    async def check_and_execute_trade(self, symbol: str, analysis_data: Dict[str, Any]) -> bool:
        """
        检查并执行交易
        
        Args:
            symbol: 交易对符号
            analysis_data: 分析数据
            
        Returns:
            bool: 是否执行了交易
        """
        if not self.enable_trading:
            return False
            
        try:
            current_price = analysis_data['current_price']
            
            # 检查过滤条件
            should_filter, filter_reason = self.should_filter_symbol(symbol, analysis_data)
            if should_filter:
                logger.info(f"🚫 {symbol} 被过滤: {filter_reason}")
                return False
            
            logger.info(f"✅ {symbol} 通过过滤条件，检查交易条件")
            
            # 检查交易记录
            latest_record = self.get_latest_trade_record(symbol)
            
            if not latest_record:
                # 没有交易记录，执行交易
                logger.info(f"💰 {symbol} 无交易记录，执行首次卖空交易")
                return await self.execute_short_order(symbol, current_price)
            else:
                # 有交易记录，检查价格条件
                last_price = latest_record['open_price']
                price_increase = (current_price - last_price) / last_price
                
                if price_increase >= 0.1:  # 价格上涨10%以上
                    logger.info(f"💰 {symbol} 价格较上次开仓上涨{price_increase*100:.2f}%，执行追加卖空交易")
                    return await self.execute_short_order(symbol, current_price)
                else:
                    logger.info(f"⏸️ {symbol} 价格较上次开仓仅上涨{price_increase*100:.2f}%，不满足10%条件")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ 检查{symbol}交易条件失败: {str(e)}")
            return False

    async def analyze_symbol(self, symbol: str) -> bool:
        """
        分析单个交易对
        
        Args:
            symbol: 交易对符号
            
        Returns:
            bool: 是否发现价格突破
        """
        try:
            logger.debug(f"分析交易对: {symbol}")
            
            # 获取30分钟K线数据
            klines = self.get_30min_klines(symbol, days=self.days_to_analyze)
            if not klines:
                return False
            
            # 检查多个时间区间的价格突破
            breakout_result = self.check_price_breakouts(klines)
            
            if not breakout_result['has_breakout']:
                return False
            
            current_price = breakout_result['current_price']
            breakout_periods = breakout_result['breakout_periods']
            periods_str = ', '.join([f"{days}天" for days in sorted(breakout_periods)])
            
            logger.info(f"🎯 发现价格突破: {symbol} 当前价格 ${current_price:.6f} 突破 {periods_str} 高点")
            
            # 获取基础资产
            base_asset = symbol.replace('USDT', '')
            
            # 获取补充信息
            funding_rate_info = self.get_funding_rate_info(symbol)
            token_info = self.get_token_info(base_asset)
            description = self.get_symbol_description(base_asset)
            tags = self.get_symbol_tags(symbol)
            
            # 组合分析数据
            analysis_data = {
                'current_price': current_price,
                'breakout_periods': breakout_periods,
                'breakouts': breakout_result['breakouts'],
                'funding_rate': funding_rate_info,
                'token_info': token_info,
                'description': description,
                'tags': tags
            }
            
            # 发送通知
            self.send_wework_notification(symbol, analysis_data)
            
            # 如果启用了交易功能，检查并执行交易
            if self.enable_trading:
                try:
                    trade_executed = await self.check_and_execute_trade(symbol, analysis_data)
                    if trade_executed:
                        logger.info(f"💰 {symbol} 交易执行成功")
                    else:
                        logger.info(f"⏸️ {symbol} 未执行交易")
                except Exception as e:
                    logger.error(f"❌ {symbol} 交易执行失败: {str(e)}")
            
            return True
            
        except Exception as e:
            logger.error(f"分析{symbol}失败: {str(e)}")
            return False

    async def run_scan(self):
        """
        运行扫描
        """
        logger.info(f"🚀 开始扫描Binance合约价格突破（{self.days_to_analyze}天历史数据）...")
        
        # 清理交易记录
        if self.enable_trading:
            logger.info("🧹 清理交易记录...")
            await self.clean_trade_records()
        
        # 获取所有合约符号
        symbols = self.get_all_futures_symbols()
        if not symbols:
            logger.error("❌ 未获取到合约交易对，扫描终止")
            return
        
        logger.info(f"📊 开始扫描 {len(symbols)} 个合约交易对...")
        
        found_count = 0
        processed_count = 0
        trade_count = 0
        
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"📈 [{i}/{len(symbols)}] 正在分析 {symbol}...")
                
                # 分析交易对
                is_breakthrough = await self.analyze_symbol(symbol)
                
                if is_breakthrough:
                    found_count += 1
                
                processed_count += 1
                
                # 避免API限制，添加短暂延迟
                await asyncio.sleep(0.3)
                
            except Exception as e:
                logger.error(f"❌ 处理{symbol}时发生错误: {str(e)}")
                continue
                
        logger.info(f"✅ 扫描完成! 处理了 {processed_count} 个交易对，发现 {found_count} 个价格突破")
        if self.enable_trading:
            logger.info(f"💰 执行了 {trade_count} 笔交易")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Binance价格高点扫描器')
    parser.add_argument(
        '--days', 
        type=int, 
        default=30, 
        help='历史K线分析天数 (默认: 30天)'
    )
    parser.add_argument(
        '--trade',
        action='store_true',
        help='启用自动交易功能'
    )
    return parser.parse_args()


async def main():
    """主函数"""
    try:
        # 解析命令行参数
        args = parse_arguments()
        
        logger.info(f"启动参数: 历史分析天数 = {args.days}天, 自动交易 = {'启用' if args.trade else '禁用'}")
        
        if args.trade:
            logger.warning("⚠️  自动交易功能已启用! 请确保您了解交易风险!")
        
        scanner = BinancePriceHighScanner(days_to_analyze=args.days, enable_trading=args.trade)
        await scanner.run_scan()
        
    except KeyboardInterrupt:
        logger.info("❌ 用户中断扫描")
    except Exception as e:
        logger.error(f"❌ 扫描过程中发生错误: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main()) 