#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binance价格高点扫描器 (混合K线版本)

该脚本用于扫描Binance所有合约交易对，监控价格突破情况：

新架构特点：
1. 混合K线数据存储：历史数据使用30分钟K线，当天数据使用1分钟K线
2. 智能跨天处理：自动将前一天的1分钟数据转换为30分钟数据
3. 数据量优化：大幅减少存储空间，同时保持当天的精确监控
4. 建议每5分钟运行一次扫描

数据策略：
- 历史数据（昨天及之前）：使用30分钟K线存储，节省空间
- 当天数据（今天00:00至今）：使用1分钟K线存储，精确监控
- 自动转换：每天00:00后自动将前一天1分钟数据聚合为30分钟数据

主要功能：
1. 数据初始化：获取30天历史30分钟K线 + 当天1分钟K线数据
2. 实时监控：检查最后一根K线价格是否为7天/15天/30天最高点
3. 跨天处理：自动转换和清理历史1分钟数据
4. 智能通知：发送企业微信群机器人通知
5. 自动交易：可选的自动卖空功能

使用方法：
- 初始化: python binance_price_high_scanner.py --init
- 扫描: python binance_price_high_scanner.py
- 交易: python binance_price_high_scanner.py --trade

通知内容包含：
- 当前价格和突破区间信息
- 资金费率、资金费结算周期
- 历史最高价、历史最低价、市值、Twitter ID、Github地址、发行日期
- 合约描述和标签

作者: Claude
创建时间: 2024-12-30
更新时间: 2024-12-30 (混合K线优化版本)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import binance_api_key, binance_api_secret, proxies, project_root, mysql_config
from binance.client import Client
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging
import requests
import pickle
import argparse
import asyncio
import ccxt.pro as ccxtpro
import pymysql

# 设置日志级别
logger.setLevel(logging.INFO)


class BinancePriceHighScanner:
    """Binance价格高点扫描器"""

    def __init__(self, api_key: str = None, api_secret: str = None, days_to_analyze: int = 30,
                 enable_trading: bool = False):
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
        self.leverage = 20  # 杠杆倍数
        self.margin_amount = 10  # 保证金金额(USDT)

        # 过滤条件
        self.min_launch_days = 7  # 最小上市天数
        self.max_market_rank = 50  # 最大市值排名
        self.min_funding_rate = 0  # 最小资金费率，小数点形式
        self.min_price_increase = 0.2

        # 交易所客户端(用于交易)
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

        # MySQL数据库配置
        self.mysql_config = mysql_config
        self.init_trading_db()  # 总是初始化数据库，用于存储价格数据

        # 当前价格缓存 {symbol: price}
        self.current_prices = {}

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

        # 资金费率信息缓存（包含结算周期）
        self.funding_info_data = {}
        self._load_funding_info()

        logger.info(
            f"Binance价格高点扫描器初始化完成 (1分钟K线版本)，分析天数: {self.days_to_analyze}天，自动交易: {'启用' if self.enable_trading else '禁用'}")

    def _load_funding_info(self):
        """一次性加载所有合约的资金费率信息（包含结算周期）"""
        try:
            logger.info("获取所有合约的资金费率信息...")
            
            # 使用Binance API获取资金费率信息
            funding_info_list = self.client.futures_v1_get_funding_info()
            
            if funding_info_list:
                # 将列表转换为字典，以symbol为key
                for info in funding_info_list:
                    symbol = info.get('symbol')
                    if symbol:
                        self.funding_info_data[symbol] = {
                            'funding_interval_hours': int(info.get('fundingIntervalHours', 8)),
                            'adjusted_funding_rate_cap': float(info.get('adjustedFundingRateCap', 0.0)),
                            'adjusted_funding_rate_floor': float(info.get('adjustedFundingRateFloor', 0.0))
                        }
                
                logger.info(f"成功获取 {len(self.funding_info_data)} 个合约的资金费率信息")
            else:
                logger.warning("未获取到任何合约的资金费率信息")
                
        except Exception as e:
            logger.error(f"获取合约资金费率信息失败: {str(e)}")
            logger.info("将使用默认的8小时结算周期")

    def init_trading_db(self):
        """初始化交易记录数据库"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 创建交易记录表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trading_records (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    exchange VARCHAR(50) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    order_time TIMESTAMP NOT NULL,
                    open_price DECIMAL(20,8) NOT NULL,
                    quantity DECIMAL(20,8) NOT NULL,
                    leverage INT NOT NULL,
                    direction VARCHAR(10) NOT NULL,
                    order_id VARCHAR(100),
                    margin_amount DECIMAL(20,8) NOT NULL,
                    current_price DECIMAL(20,8) DEFAULT 0.0,
                    price_change_percent DECIMAL(10,4) DEFAULT 0.0,
                    pnl_amount DECIMAL(20,8) DEFAULT 0.0,
                    price_update_time TIMESTAMP NULL,
                    close_price DECIMAL(20,8) DEFAULT NULL,
                    close_order_id VARCHAR(100) DEFAULT NULL,
                    close_order_status VARCHAR(20) DEFAULT 'OPEN',
                    close_time TIMESTAMP NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_trade (exchange, symbol, order_time),
                    INDEX idx_symbol (symbol),
                    INDEX idx_order_time (order_time),
                    INDEX idx_close_status (close_order_status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')

            # 创建1分钟K线数据表（当天数据）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS kline_data_1min (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    symbol VARCHAR(50) NOT NULL,
                    open_time BIGINT NOT NULL,
                    close_time BIGINT NOT NULL,
                    open_price DECIMAL(20,8) NOT NULL,
                    high_price DECIMAL(20,8) NOT NULL,
                    low_price DECIMAL(20,8) NOT NULL,
                    close_price DECIMAL(20,8) NOT NULL,
                    volume DECIMAL(20,8) NOT NULL,
                    quote_volume DECIMAL(20,8) NOT NULL,
                    trades_count INT NOT NULL,
                    taker_buy_base_volume DECIMAL(20,8) NOT NULL,
                    taker_buy_quote_volume DECIMAL(20,8) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_kline_1min (symbol, open_time),
                    INDEX idx_open_time_1min (open_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')

            # 创建30分钟K线数据表（历史数据）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS kline_data_30min (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    symbol VARCHAR(50) NOT NULL,
                    open_time BIGINT NOT NULL,
                    close_time BIGINT NOT NULL,
                    open_price DECIMAL(20,8) NOT NULL,
                    high_price DECIMAL(20,8) NOT NULL,
                    low_price DECIMAL(20,8) NOT NULL,
                    close_price DECIMAL(20,8) NOT NULL,
                    volume DECIMAL(20,8) NOT NULL,
                    quote_volume DECIMAL(20,8) NOT NULL,
                    trades_count INT NOT NULL,
                    taker_buy_base_volume DECIMAL(20,8) NOT NULL,
                    taker_buy_quote_volume DECIMAL(20,8) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_kline_30min (symbol, open_time),
                    INDEX idx_open_time_1min (open_time)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')

            # 创建系统状态表（记录跨天处理状态）
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_status (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    status_key VARCHAR(50) NOT NULL UNIQUE,
                    status_value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')

            conn.commit()
            conn.close()
            logger.info(f"MySQL数据库初始化完成: {self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}")

        except Exception as e:
            logger.error(f"MySQL数据库初始化失败: {str(e)}")

    def save_kline_data(self, symbol: str, klines: List[List], interval: str = '1min') -> bool:
        """
        保存K线数据到数据库
        
        Args:
            symbol: 交易对符号
            klines: K线数据列表
            interval: K线间隔 ('1min' 或 '30min')
            
        Returns:
            bool: 是否保存成功
        """
        try:
            if not klines:
                return False

            # 根据间隔选择表名
            table_name = 'kline_data_1min' if interval == '1min' else 'kline_data_30min'

            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            saved_count = 0
            for kline in klines:
                try:
                    cursor.execute(f'''
                        INSERT IGNORE INTO {table_name} 
                        (symbol, open_time, close_time, open_price, high_price, low_price, 
                         close_price, volume, quote_volume, trades_count, 
                         taker_buy_base_volume, taker_buy_quote_volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        symbol,
                        int(kline[0]),          # open_time
                        int(kline[6]),          # close_time
                        float(kline[1]),        # open_price
                        float(kline[2]),        # high_price
                        float(kline[3]),        # low_price
                        float(kline[4]),        # close_price
                        float(kline[5]),        # volume
                        float(kline[7]),        # quote_volume
                        int(kline[8]),          # trades_count
                        float(kline[9]),        # taker_buy_base_volume
                        float(kline[10])        # taker_buy_quote_volume
                    ))
                    if cursor.rowcount > 0:
                        saved_count += 1
                except Exception as e:
                    logger.debug(f"插入{interval}K线数据失败 (可能重复): {str(e)}")

            conn.commit()
            conn.close()

            if saved_count > 0:
                logger.debug(f"保存{symbol}的{saved_count}条新{interval}K线数据")
            
            return True

        except Exception as e:
            logger.error(f"保存{symbol}{interval}K线数据失败: {str(e)}")
            return False

    def get_kline_data_from_db(self, symbol: str, days: int = 30) -> List[List]:
        """
        从数据库获取K线数据（混合查询：历史30分钟+当天1分钟）
        
        Args:
            symbol: 交易对符号
            days: 获取天数
            
        Returns:
            List[List]: K线数据列表，格式与Binance API返回的格式一致
        """
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 获取当天开始时间（00:00:00）
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_start_ms = int(today_start.timestamp() * 1000)
            
            # 计算历史数据开始时间
            start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

            all_klines = []

            # 1. 获取历史30分钟K线数据（今天00:00之前）
            cursor.execute('''
                SELECT open_time, open_price, high_price, low_price, close_price, volume,
                       close_time, quote_volume, trades_count, taker_buy_base_volume, 
                       taker_buy_quote_volume, '0'
                FROM kline_data_30min 
                WHERE symbol = %s AND open_time >= %s AND open_time < %s
                ORDER BY open_time ASC
            ''', (symbol, start_time, today_start_ms))

            historical_results = cursor.fetchall()
            
            # 2. 获取当天1分钟K线数据（今天00:00之后）
            cursor.execute('''
                SELECT open_time, open_price, high_price, low_price, close_price, volume,
                       close_time, quote_volume, trades_count, taker_buy_base_volume, 
                       taker_buy_quote_volume, '0'
                FROM kline_data_1min 
                WHERE symbol = %s AND open_time >= %s
                ORDER BY open_time ASC
            ''', (symbol, today_start_ms))

            today_results = cursor.fetchall()
            conn.close()

            # 合并历史数据和当天数据
            all_results = list(historical_results) + list(today_results)

            # 转换为Binance API格式的列表
            klines = []
            for row in all_results:
                kline = [
                    row[0],     # open_time
                    str(row[1]), # open_price
                    str(row[2]), # high_price
                    str(row[3]), # low_price
                    str(row[4]), # close_price
                    str(row[5]), # volume
                    row[6],     # close_time
                    str(row[7]), # quote_volume
                    row[8],     # trades_count
                    str(row[9]), # taker_buy_base_volume
                    str(row[10]), # taker_buy_quote_volume
                    row[11]     # ignore
                ]
                klines.append(kline)

            logger.debug(f"从数据库获取{symbol}的{len(klines)}条混合K线数据（{days}天）: 历史30分钟{len(historical_results)}条，当天1分钟{len(today_results)}条")
            return klines

        except Exception as e:
            logger.error(f"从数据库获取{symbol}K线数据失败: {str(e)}")
            return []

    def get_kline_data_count(self, symbol: str) -> int:
        """获取数据库中某个交易对的K线数据数量（30分钟+1分钟）"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # 获取30分钟K线数量
            cursor.execute('SELECT COUNT(*) FROM kline_data_30min WHERE symbol = %s', (symbol,))
            result_30min = cursor.fetchone()
            count_30min = result_30min[0] if result_30min else 0
            
            # 获取1分钟K线数量
            cursor.execute('SELECT COUNT(*) FROM kline_data_1min WHERE symbol = %s', (symbol,))
            result_1min = cursor.fetchone()
            count_1min = result_1min[0] if result_1min else 0
            
            conn.close()
            
            total_count = count_30min + count_1min
            logger.debug(f"{symbol}数据库K线统计: 30分钟{count_30min}条, 1分钟{count_1min}条, 总计{total_count}条")
            
            return total_count
            
        except Exception as e:
            logger.error(f"获取{symbol}K线数据数量失败: {str(e)}")
            return 0

    def get_kline_start_time(self, symbol: str) -> Optional[datetime]:
        """
        获取交易对在数据库中第一根K线的时间
        
        Args:
            symbol: 交易对符号
            
        Returns:
            datetime: 第一根K线的时间，如果没有数据则返回None
        """
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # 查询30分钟K线中的最早时间
            cursor.execute("""
                SELECT MIN(open_time) FROM kline_data_30min 
                WHERE symbol = %s
            """, (symbol,))
            min_30min_result = cursor.fetchone()
            min_30min_time = min_30min_result[0] if min_30min_result and min_30min_result[0] else None
            
            # 查询1分钟K线中的最早时间
            cursor.execute("""
                SELECT MIN(open_time) FROM kline_data_1min 
                WHERE symbol = %s
            """, (symbol,))
            min_1min_result = cursor.fetchone()
            min_1min_time = min_1min_result[0] if min_1min_result and min_1min_result[0] else None
            
            conn.close()
            
            # 选择两者中更早的时间
            earliest_time = None
            if min_30min_time and min_1min_time:
                earliest_time = min(min_30min_time, min_1min_time)
            elif min_30min_time:
                earliest_time = min_30min_time
            elif min_1min_time:
                earliest_time = min_1min_time
            
            if earliest_time:
                # 转换为datetime对象
                if isinstance(earliest_time, int):
                    earliest_datetime = datetime.fromtimestamp(earliest_time / 1000)
                else:
                    earliest_datetime = earliest_time
                
                logger.debug(f"{symbol} K线开始时间: {earliest_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
                return earliest_datetime
            else:
                logger.warning(f"{symbol} 没有找到K线数据")
                return None
                
        except Exception as e:
            logger.error(f"获取{symbol}K线开始时间失败: {str(e)}")
            return None

    def get_system_status(self, status_key: str) -> Optional[str]:
        """获取系统状态"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            cursor.execute('SELECT status_value FROM system_status WHERE status_key = %s', (status_key,))
            result = cursor.fetchone()
            conn.close()
            
            return result[0] if result else None
            
        except Exception as e:
            logger.error(f"获取系统状态{status_key}失败: {str(e)}")
            return None

    def set_system_status(self, status_key: str, status_value: str) -> bool:
        """设置系统状态"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO system_status (status_key, status_value) 
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE status_value = %s, updated_at = CURRENT_TIMESTAMP
            ''', (status_key, status_value, status_value))
            
            conn.commit()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"设置系统状态{status_key}失败: {str(e)}")
            return False

    async def convert_daily_klines_to_30min(self, target_date: datetime) -> bool:
        """
        将指定日期的1分钟K线数据转换为30分钟K线数据
        
        Args:
            target_date: 目标日期
            
        Returns:
            bool: 是否转换成功
        """
        try:
            # 计算目标日期的时间范围
            date_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = date_start + timedelta(days=1)
            
            start_ms = int(date_start.timestamp() * 1000)
            end_ms = int(date_end.timestamp() * 1000)
            
            logger.info(f"开始转换{target_date.strftime('%Y-%m-%d')}的1分钟K线数据为30分钟K线...")
            
            # 获取所有有1分钟数据的交易对
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT DISTINCT symbol FROM kline_data_1min 
                WHERE open_time >= %s AND open_time < %s
            ''', (start_ms, end_ms))
            
            symbols = [row[0] for row in cursor.fetchall()]
            logger.info(f"需要转换{len(symbols)}个交易对的数据")
            
            converted_count = 0
            
            for symbol in symbols:
                try:
                    # 获取该交易对当天的1分钟数据
                    cursor.execute('''
                        SELECT open_time, open_price, high_price, low_price, close_price, volume,
                               close_time, quote_volume, trades_count, taker_buy_base_volume, 
                               taker_buy_quote_volume
                        FROM kline_data_1min 
                        WHERE symbol = %s AND open_time >= %s AND open_time < %s
                        ORDER BY open_time ASC
                    ''', (symbol, start_ms, end_ms))
                    
                    minute_data = cursor.fetchall()
                    
                    if not minute_data:
                        continue
                    
                    # 按30分钟分组聚合数据
                    aggregated_klines = self._aggregate_klines_to_30min(minute_data, date_start)
                    
                    if aggregated_klines:
                        # 保存30分钟K线数据
                        success = self.save_kline_data(symbol, aggregated_klines, '30min')
                        if success:
                            converted_count += 1
                            logger.debug(f"成功转换{symbol}的{len(aggregated_klines)}条30分钟K线数据")
                        
                except Exception as e:
                    logger.error(f"转换{symbol}数据失败: {str(e)}")
                    continue
            
            conn.close()
            
            logger.info(f"✅ {target_date.strftime('%Y-%m-%d')}数据转换完成，成功转换{converted_count}个交易对")
            return True
            
        except Exception as e:
            logger.error(f"转换{target_date.strftime('%Y-%m-%d')}数据失败: {str(e)}")
            return False

    def _aggregate_klines_to_30min(self, minute_data: List, date_start: datetime) -> List[List]:
        """
        将1分钟K线数据聚合为30分钟K线数据
        
        Args:
            minute_data: 1分钟K线数据
            date_start: 日期开始时间
            
        Returns:
            List[List]: 30分钟K线数据列表
        """
        try:
            aggregated = []
            
            # 按30分钟分组
            for hour in range(24):
                for minute_group in [0, 30]:  # 每小时的0分和30分开始
                    group_start = date_start.replace(hour=hour, minute=minute_group)
                    group_end = group_start + timedelta(minutes=30)
                    
                    group_start_ms = int(group_start.timestamp() * 1000)
                    group_end_ms = int(group_end.timestamp() * 1000)
                    
                    # 筛选该30分钟内的数据
                    group_data = [
                        row for row in minute_data 
                        if group_start_ms <= row[0] < group_end_ms
                    ]
                    
                    if not group_data:
                        continue
                    
                    # 聚合数据
                    open_price = float(group_data[0][1])  # 第一根的开盘价
                    high_price = max(float(row[2]) for row in group_data)  # 最高价
                    low_price = min(float(row[3]) for row in group_data)   # 最低价
                    close_price = float(group_data[-1][4])  # 最后一根的收盘价
                    volume = sum(float(row[5]) for row in group_data)  # 成交量累加
                    quote_volume = sum(float(row[7]) for row in group_data)  # 成交额累加
                    trades_count = sum(int(row[8]) for row in group_data)  # 交易笔数累加
                    taker_buy_base_volume = sum(float(row[9]) for row in group_data)  # 主动买入成交量累加
                    taker_buy_quote_volume = sum(float(row[10]) for row in group_data)  # 主动买入成交额累加
                    
                    # 构造30分钟K线数据
                    kline_30min = [
                        group_start_ms,                    # open_time
                        str(open_price),                   # open_price
                        str(high_price),                   # high_price
                        str(low_price),                    # low_price
                        str(close_price),                  # close_price
                        str(volume),                       # volume
                        group_end_ms - 1,                  # close_time
                        str(quote_volume),                 # quote_volume
                        trades_count,                      # trades_count
                        str(taker_buy_base_volume),        # taker_buy_base_volume
                        str(taker_buy_quote_volume),       # taker_buy_quote_volume
                        '0'                                # ignore
                    ]
                    
                    aggregated.append(kline_30min)
            
            return aggregated
            
        except Exception as e:
            logger.error(f"聚合K线数据失败: {str(e)}")
            return []

    async def clean_daily_1min_data(self, target_date: datetime) -> bool:
        """
        清理指定日期的1分钟K线数据
        
        Args:
            target_date: 目标日期
            
        Returns:
            bool: 是否清理成功
        """
        try:
            # 计算目标日期的时间范围
            date_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = date_start + timedelta(days=1)
            
            start_ms = int(date_start.timestamp() * 1000)
            end_ms = int(date_end.timestamp() * 1000)
            
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            """
                WHERE open_time >= %s AND open_time < %s
            """
            # 清除昨天以前的, 这样最少可以保持1天的数据
            cursor.execute('''
                DELETE FROM kline_data_1min 
                WHERE open_time < %s
            ''', (start_ms, ))

            deleted_count = cursor.rowcount
            conn.commit()
            conn.close()
            
            logger.info(f"✅ 清理{target_date.strftime('%Y-%m-%d')}的1分钟K线数据，删除{deleted_count}条记录")
            return True
            
        except Exception as e:
            logger.error(f"清理{target_date.strftime('%Y-%m-%d')}的1分钟数据失败: {str(e)}")
            return False

    async def check_and_handle_day_change(self) -> bool:
        """
        检查并处理跨天情况
        
        Returns:
            bool: 是否处理成功
        """
        try:
            current_date = datetime.now().strftime('%Y-%m-%d')
            last_processed_date = self.get_system_status('last_processed_date')
            
            if last_processed_date is None:
                # 第一次运行，记录当前日期
                self.set_system_status('last_processed_date', current_date)
                logger.info(f"首次运行，记录当前日期: {current_date}")
                return True
            
            if last_processed_date == current_date:
                # 同一天，无需处理
                return True
            
            # 检测到跨天，需要处理前一天的数据
            logger.info(f"🔄 检测到跨天: {last_processed_date} -> {current_date}")
            
            # 解析上次处理的日期
            last_date = datetime.strptime(last_processed_date, '%Y-%m-%d')
            
            # 处理从上次日期到昨天的所有日期
            current = last_date
            yesterday = datetime.now() - timedelta(days=1)
            
            while current.date() <= yesterday.date():
                logger.info(f"📊 处理{current.strftime('%Y-%m-%d')}的数据转换...")
                
                # 转换1分钟数据为30分钟数据
                convert_success = await self.convert_daily_klines_to_30min(current)
                
                if convert_success:
                    # 转换成功后清理1分钟数据
                    clean_success = await self.clean_daily_1min_data(current)
                    
                    if clean_success:
                        logger.info(f"✅ {current.strftime('%Y-%m-%d')}数据处理完成")
                    else:
                        logger.warning(f"⚠️ {current.strftime('%Y-%m-%d')}数据转换成功但清理失败")
                else:
                    logger.error(f"❌ {current.strftime('%Y-%m-%d')}数据转换失败")
                
                current += timedelta(days=1)
            
            # 更新最后处理日期
            self.set_system_status('last_processed_date', current_date)
            logger.info(f"✅ 跨天处理完成，更新处理日期为: {current_date}")
            
            return True
            
        except Exception as e:
            logger.error(f"跨天处理失败: {str(e)}")
            return False

    def get_latest_trade_record(self, symbol: str) -> Optional[Dict[str, Any]]:
        """获取某个交易对的最新未平仓交易记录"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM trading_records 
                WHERE symbol = %s AND close_order_status = %s
                ORDER BY order_time DESC 
                LIMIT 1
            ''', (symbol, 'OPEN'))

            result = cursor.fetchone()
            conn.close()

            if result:
                columns = ['id', 'exchange', 'symbol', 'order_time', 'open_price',
                           'quantity', 'leverage', 'direction', 'order_id', 'margin_amount',
                           'current_price', 'price_change_percent', 'pnl_amount', 'price_update_time',
                           'close_price', 'close_order_id', 'close_order_status', 'close_time', 'created_at']
                return dict(zip(columns, result))

            return None

        except Exception as e:
            logger.error(f"获取{symbol}最新交易记录失败: {str(e)}")
            return None

    def save_trade_record(self, symbol: str, open_price: float, quantity: float, order_id: str = None) -> bool:
        """保存交易记录"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO trading_records 
                (exchange, symbol, order_time, open_price, quantity, leverage, direction, order_id, margin_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', ('Binance', symbol, datetime.now(), open_price, quantity, self.leverage, 'SHORT', order_id,
                  self.margin_amount))

            conn.commit()
            conn.close()
            logger.info(f"交易记录已保存: {symbol} 价格={open_price} 数量={quantity}")
            return True

        except Exception as e:
            logger.error(f"保存{symbol}交易记录失败: {str(e)}")
            return False

    def update_close_order_info(self, symbol: str, open_order_id: str, close_price: float, 
                               close_order_id: str, close_order_status: str = 'FILLED') -> bool:
        """
        更新交易记录的平仓订单信息
        
        Args:
            symbol: 交易对符号
            open_order_id: 开仓订单ID
            close_price: 平仓价格
            close_order_id: 平仓订单ID
            close_order_status: 平仓订单状态 (OPEN, FILLED, CANCELLED等)
            
        Returns:
            bool: 是否更新成功
        """
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            cursor.execute('''
                UPDATE trading_records 
                SET close_price = %s, 
                    close_order_id = %s, 
                    close_order_status = %s,
                    close_time = %s
                WHERE symbol = %s AND order_id = %s
            ''', (close_price, close_order_id, close_order_status, datetime.now(), symbol, open_order_id))

            updated_count = cursor.rowcount
            conn.commit()
            conn.close()

            if updated_count > 0:
                logger.info(f"已更新{symbol}的平仓信息: 开仓订单ID={open_order_id}, 平仓价格={close_price}, 平仓订单ID={close_order_id}")
                return True
            else:
                logger.warning(f"未找到要更新的交易记录: {symbol} 开仓订单ID={open_order_id}")
                return False

        except Exception as e:
            logger.error(f"更新{symbol}平仓信息失败: {str(e)}")
            return False

    def remove_trade_record(self, symbol: str) -> bool:
        """删除交易对的所有交易记录"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            cursor.execute('DELETE FROM trading_records WHERE symbol = %s', (symbol,))
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
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            cursor.execute('SELECT DISTINCT symbol FROM trading_records')
            results = cursor.fetchall()
            conn.close()
            
            return [row[0] for row in results]
            
        except Exception as e:
            logger.error(f"获取交易对列表失败: {str(e)}")
            return []

    def get_open_traded_symbols(self) -> List[str]:
        """获取所有有未平仓交易记录的交易对"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            cursor.execute('SELECT DISTINCT symbol FROM trading_records WHERE close_order_status = %s', ('OPEN',))
            results = cursor.fetchall()
            conn.close()
            
            return [row[0] for row in results]
            
        except Exception as e:
            logger.error(f"获取未平仓交易对列表失败: {str(e)}")
            return []

    def update_symbol_to_closed_status(self, symbol: str) -> bool:
        """将交易对的所有未平仓记录更新为已平仓状态"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            cursor.execute('''
                UPDATE trading_records 
                SET close_order_status = %s, 
                    close_time = %s
                WHERE symbol = %s AND close_order_status = %s
            ''', ('CLOSED', datetime.now(), symbol, 'OPEN'))

            updated_count = cursor.rowcount
            conn.commit()
            conn.close()

            if updated_count > 0:
                logger.info(f"已更新{symbol}的{updated_count}条交易记录为已平仓状态")

            return True

        except Exception as e:
            logger.error(f"更新{symbol}交易记录状态失败: {str(e)}")
            return False

    async def check_and_update_close_orders(self):
        """检查并更新所有OPEN状态的平仓订单状态"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 获取所有有平仓订单但状态为OPEN的记录
            cursor.execute('''
                SELECT symbol, order_id, close_order_id 
                FROM trading_records 
                WHERE close_order_id IS NOT NULL AND close_order_status = %s
            ''', ('OPEN',))

            results = cursor.fetchall()
            conn.close()

            if not results:
                logger.debug("没有需要检查的平仓订单")
                return

            updated_count = 0
            for symbol, open_order_id, close_order_id in results:
                try:
                    # 检查平仓订单状态
                    order_status = await self.binance_trading.fetch_order(close_order_id, symbol)
                    
                    if order_status and order_status.get('status') == 'closed':
                        # 订单已成交，更新为FILLED状态
                        filled_price = float(order_status.get('average', 0))
                        self.update_close_order_info(
                            symbol=symbol,
                            open_order_id=open_order_id,
                            close_price=filled_price,
                            close_order_id=close_order_id,
                            close_order_status='FILLED'
                        )
                        updated_count += 1
                        logger.info(f"✅ {symbol} 平仓订单已成交: {close_order_id}, 成交价格: {filled_price}")
                    
                    elif order_status and order_status.get('status') == 'canceled':
                        # 订单已取消，更新为CANCELLED状态
                        self.update_close_order_info(
                            symbol=symbol,
                            open_order_id=open_order_id,
                            close_price=0,  # 取消订单没有成交价格
                            close_order_id=close_order_id,
                            close_order_status='CANCELLED'
                        )
                        logger.info(f"❌ {symbol} 平仓订单已取消: {close_order_id}")
                
                except Exception as e:
                    logger.warning(f"检查{symbol}平仓订单{close_order_id}状态失败: {str(e)}")

            if updated_count > 0:
                logger.info(f"检查完成: 更新了{updated_count}个平仓订单状态")

        except Exception as e:
            logger.error(f"检查平仓订单状态失败: {str(e)}")

    def _get_total_trade_records_count(self) -> int:
        """获取数据库中的交易记录总数"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM trading_records')
            result = cursor.fetchone()
            conn.close()
            
            return result[0] if result else 0
            
        except Exception as e:
            logger.error(f"获取交易记录总数失败: {str(e)}")
            return 0

    async def get_max_leverage(self, symbol: str) -> int:
        """
        获取Binance交易所支持的最大杠杆倍数
        
        Args:
            symbol: 交易对符号
            
        Returns:
            int: 最大杠杆倍数
        """
        try:
            # 获取交易对信息
            response = await self.binance_trading.fapiPublicGetExchangeInfo()

            if response and 'symbols' in response:
                for symbol_info in response['symbols']:
                    if symbol_info['symbol'] == symbol:
                        # 获取杠杆倍数信息
                        leverage_info = await self.binance_trading.fapiPrivateGetLeverageBracket({
                            'symbol': symbol
                        })

                        if leverage_info and 'brackets' in leverage_info[0]:
                            max_leverage = int(leverage_info[0]['brackets'][0]['initialLeverage'])
                            logger.info(f"获取到{symbol}最大杠杆倍数: {max_leverage}倍")
                            return max_leverage

            raise Exception(f"未能获取到{symbol}的最大杠杆倍数")

        except Exception as e:
            logger.warning(f"获取Binance最大杠杆倍数失败: {e}")
            logger.info("使用默认杠杆倍数: 20倍")
            return 20

    def save_current_price(self, symbol: str, current_price: float):
        """保存当前价格到缓存"""
        self.current_prices[symbol] = current_price
        logger.debug(f"保存{symbol}当前价格: ${current_price:.6f}")

    def update_trade_pnl(self, symbol: str, current_price: float) -> bool:
        """更新交易记录的盈亏信息（仅更新该交易对的未平仓交易记录）"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 获取该交易对的所有未平仓交易记录
            cursor.execute('''
                SELECT id, open_price, quantity, direction
                FROM trading_records 
                WHERE symbol = %s AND close_order_status = %s
                ORDER BY order_time DESC
            ''', (symbol, 'OPEN'))

            results = cursor.fetchall()
            if not results:
                conn.close()
                return False

            updated_count = 0
            total_pnl = 0.0

            # 更新每一条交易记录
            for record in results:
                record_id, open_price, quantity, direction = record

                # 计算价格涨跌百分比
                price_change_percent = ((current_price - float(open_price)) / float(open_price)) * 100

                # 计算盈亏额（考虑交易方向）
                if direction == 'SHORT':
                    # 卖空：价格下跌为盈利
                    pnl_amount = (float(open_price) - current_price) * float(quantity)
                else:
                    # 做多：价格上涨为盈利
                    pnl_amount = (current_price - float(open_price)) * float(quantity)

                # 更新记录
                cursor.execute('''
                    UPDATE trading_records 
                    SET current_price = %s, 
                        price_change_percent = %s, 
                        pnl_amount = %s, 
                        price_update_time = %s
                    WHERE id = %s
                ''', (current_price, price_change_percent, pnl_amount, datetime.now(), record_id))

                if cursor.rowcount > 0:
                    updated_count += 1
                    total_pnl += pnl_amount

            conn.commit()
            conn.close()

            logger.debug(f"更新{symbol}盈亏信息: 更新{updated_count}条记录, 总盈亏${total_pnl:.2f}")
            return True

        except Exception as e:
            logger.error(f"更新{symbol}盈亏信息失败: {str(e)}")
            return False

    def update_all_trade_pnl(self):
        """更新所有未平仓交易记录的盈亏信息"""
        try:
            open_traded_symbols = self.get_open_traded_symbols()
            updated_count = 0

            for symbol in open_traded_symbols:
                if symbol in self.current_prices:
                    current_price = self.current_prices[symbol]
                    if self.update_trade_pnl(symbol, current_price):
                        updated_count += 1
                else:
                    logger.warning(f"未找到{symbol}的当前价格数据")

            logger.info(f"完成盈亏更新: 更新了{updated_count}个未平仓交易对的盈亏信息")

        except Exception as e:
            logger.error(f"批量更新盈亏信息失败: {str(e)}")

    def get_all_trade_pnl_summary(self) -> Dict[str, Any]:
        """获取所有未平仓交易对的盈亏汇总"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            cursor.execute('''
                SELECT symbol, open_price, current_price, quantity, direction, 
                       price_change_percent, pnl_amount, price_update_time
                FROM trading_records 
                WHERE current_price > 0 AND close_order_status = %s
                ORDER BY pnl_amount DESC
            ''', ('OPEN',))

            results = cursor.fetchall()
            conn.close()

            summary = {
                'positions': [],
                'total_pnl': 0.0,
                'profitable_count': 0,
                'losing_count': 0
            }

            for row in results:
                symbol, open_price, current_price, quantity, direction, price_change_percent, pnl_amount, price_update_time = row

                position = {
                    'symbol': symbol,
                    'open_price': float(open_price),
                    'current_price': float(current_price),
                    'quantity': float(quantity),
                    'direction': direction,
                    'price_change_percent': float(price_change_percent),
                    'pnl_amount': float(pnl_amount),
                    'price_update_time': price_update_time
                }

                summary['positions'].append(position)
                summary['total_pnl'] += float(pnl_amount)

                if float(pnl_amount) > 0:
                    summary['profitable_count'] += 1
                else:
                    summary['losing_count'] += 1

            return summary

        except Exception as e:
            logger.error(f"获取盈亏汇总失败: {str(e)}")
            return {'positions': [], 'total_pnl': 0.0, 'profitable_count': 0, 'losing_count': 0}

    def get_symbol_aggregated_pnl_summary(self) -> Dict[str, Any]:
        """获取按交易对合并的未平仓盈亏汇总"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()

            cursor.execute('''
                SELECT symbol, 
                       AVG(current_price) as avg_current_price,
                       SUM(quantity * open_price) / SUM(quantity) as avg_open_price,
                       SUM(quantity) as total_quantity,
                       direction,
                       SUM(pnl_amount) as total_pnl,
                       MAX(price_update_time) as latest_update_time,
                       COUNT(*) as trade_count
                FROM trading_records 
                WHERE current_price > 0 AND close_order_status = %s
                GROUP BY symbol, direction
                ORDER BY total_pnl DESC
            ''', ('OPEN',))

            results = cursor.fetchall()
            conn.close()

            summary = {
                'symbol_positions': [],
                'total_pnl': 0.0,
                'profitable_symbols': 0,
                'losing_symbols': 0
            }

            for row in results:
                symbol, avg_current_price, avg_open_price, total_quantity, direction, total_pnl, latest_update_time, trade_count = row

                # 计算平均价格变化百分比
                price_change_percent = ((float(avg_current_price) - float(avg_open_price)) / float(avg_open_price)) * 100

                position = {
                    'symbol': symbol,
                    'avg_open_price': float(avg_open_price),
                    'avg_current_price': float(avg_current_price),
                    'total_quantity': float(total_quantity),
                    'direction': direction,
                    'price_change_percent': price_change_percent,
                    'total_pnl': float(total_pnl),
                    'latest_update_time': latest_update_time,
                    'trade_count': trade_count
                }

                summary['symbol_positions'].append(position)
                summary['total_pnl'] += float(total_pnl)

                if float(total_pnl) > 0:
                    summary['profitable_symbols'] += 1
                else:
                    summary['losing_symbols'] += 1

            return summary

        except Exception as e:
            logger.error(f"获取按交易对合并的盈亏汇总失败: {str(e)}")
            return {'symbol_positions': [], 'total_pnl': 0.0, 'profitable_symbols': 0, 'losing_symbols': 0}

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



    def get_recent_klines(self, symbol: str, minutes: int = 10) -> Optional[List[List]]:
        """
        获取最近几分钟的1分钟K线数据
        
        Args:
            symbol: 交易对符号
            minutes: 获取最近几分钟的数据
            
        Returns:
            List[List]: K线数据列表
        """
        try:
            # 计算时间范围
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=minutes)

            # 获取1分钟K线数据
            klines = self.client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_1MINUTE,
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000),
                limit=minutes + 5  # 多获取几根以防时间误差
            )

            if not klines:
                logger.warning(f"{symbol}: 未获取到最近{minutes}分钟的K线数据")
                return None

            logger.debug(f"{symbol}: 获取到{len(klines)}根最近{minutes}分钟的1分钟K线")
            return klines

        except Exception as e:
            logger.error(f"获取{symbol}最近{minutes}分钟K线数据失败: {str(e)}")
            return None

    async def initialize_kline_data(self, symbol: str) -> bool:
        """
        初始化某个交易对的混合K线数据（历史30分钟+当天1分钟）
        
        Args:
            symbol: 交易对符号
            
        Returns:
            bool: 是否初始化成功
        """
        try:
            logger.info(f"开始初始化{symbol}的混合K线数据...")
            
            # 获取当天开始时间
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            # 1. 初始化历史30分钟K线数据（30天前到今天00:00）
            success_30min = await self._initialize_30min_klines(symbol, today_start)
            
            # 2. 初始化当天1分钟K线数据（今天00:00到现在）
            success_1min = await self._initialize_today_1min_klines(symbol, today_start)
            
            if success_30min and success_1min:
                logger.info(f"✅ {symbol}混合K线数据初始化完成")
                return True
            else:
                logger.warning(f"⚠️ {symbol}部分数据初始化失败: 30分钟={success_30min}, 1分钟={success_1min}")
                return False
            
        except Exception as e:
            logger.error(f"初始化{symbol}混合K线数据失败: {str(e)}")
            return False

    async def _initialize_30min_klines(self, symbol: str, today_start: datetime) -> bool:
        """初始化历史30分钟K线数据"""
        try:
            logger.info(f"初始化{symbol}的历史30分钟K线数据...")
            
            # 30天的30分钟K线 = 30 * 24 * 2 = 1440条数据
            end_time = today_start
            start_time = end_time - timedelta(days=30)
            
            # 获取30分钟K线数据
            klines = self.client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_30MINUTE,
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000),
                limit=1500  # 30分钟K线数据量较小，一次可以获取完
            )
            
            if klines:
                # 保存到30分钟K线表
                success = self.save_kline_data(symbol, klines, '30min')
                if success:
                    logger.info(f"✅ {symbol}历史30分钟K线初始化完成，保存{len(klines)}条数据")
                    return True
            
            logger.warning(f"⚠️ {symbol}历史30分钟K线数据为空")
            return False
            
        except Exception as e:
            logger.error(f"初始化{symbol}历史30分钟K线失败: {str(e)}")
            return False

    async def _initialize_today_1min_klines(self, symbol: str, today_start: datetime) -> bool:
        """初始化当天1分钟K线数据"""
        try:
            logger.info(f"初始化{symbol}的当天1分钟K线数据...")
            
            # 当天的1分钟K线数据
            end_time = datetime.now()
            
            # 计算当天已过去的分钟数
            minutes_today = int((end_time - today_start).total_seconds() / 60)
            
            if minutes_today <= 0:
                logger.info(f"当天刚开始，{symbol}无1分钟数据需要初始化")
                return True
            
            # 分批获取当天的1分钟数据
            batch_size = 1500
            batches = (minutes_today + batch_size - 1) // batch_size
            total_saved = 0
            
            for batch in range(batches):
                try:
                    # 计算当前批次的时间范围
                    start_minutes = batch * batch_size
                    end_minutes = min(start_minutes + batch_size, minutes_today)
                    
                    batch_start = today_start + timedelta(minutes=start_minutes)
                    batch_end = today_start + timedelta(minutes=end_minutes)
                    
                    # 获取1分钟K线数据
                    klines = self.client.futures_klines(
                        symbol=symbol,
                        interval=Client.KLINE_INTERVAL_1MINUTE,
                        startTime=int(batch_start.timestamp() * 1000),
                        endTime=int(batch_end.timestamp() * 1000),
                        limit=batch_size
                    )
                    
                    if klines:
                        # 保存到1分钟K线表
                        success = self.save_kline_data(symbol, klines, '1min')
                        if success:
                            total_saved += len(klines)
                            logger.debug(f"{symbol}当天1分钟K线第{batch + 1}/{batches}批次: 保存{len(klines)}条数据")
                    
                    # 避免API限制
                    await asyncio.sleep(0.2)
                    
                except Exception as e:
                    logger.error(f"初始化{symbol}当天1分钟K线第{batch + 1}批次失败: {str(e)}")
                    continue
            
            logger.info(f"✅ {symbol}当天1分钟K线初始化完成，保存{total_saved}条数据")
            return True
            
        except Exception as e:
            logger.error(f"初始化{symbol}当天1分钟K线失败: {str(e)}")
            return False

    async def initialize_all_kline_data(self):
        """初始化所有交易对的K线数据"""
        logger.info("🚀 开始初始化所有交易对的K线数据...")
        
        # 获取所有合约符号
        symbols = self.get_all_futures_symbols()
        if not symbols:
            logger.error("❌ 未获取到合约交易对，初始化终止")
            return
        
        logger.info(f"📊 需要初始化 {len(symbols)} 个合约交易对的K线数据...")
        
        initialized_count = 0
        
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"[{i}/{len(symbols)}] 初始化 {symbol}...")
                
                # 检查是否已有数据
                existing_count = self.get_kline_data_count(symbol)
                if existing_count > 0:
                    logger.info(f"⏭️ {symbol}已有{existing_count}条K线数据，跳过初始化")
                    continue
                
                # 初始化K线数据
                success = await self.initialize_kline_data(symbol)
                if success:
                    initialized_count += 1
                
            except Exception as e:
                logger.error(f"❌ 初始化{symbol}时发生错误: {str(e)}")
                continue
        
        logger.info(f"✅ K线数据初始化完成! 成功初始化了 {initialized_count} 个交易对")

    async def update_kline_data(self, symbol: str, minutes: int=30) -> bool:
        """
        更新某个交易对的最新1分钟K线数据（仅当天数据）
        
        Args:
            symbol: 交易对符号
            minutes: 获取最近几分钟的数据，默认30分钟
            
        Returns:
            bool: 是否更新成功
        """
        try:
            # 获取最近指定分钟的1分钟K线数据
            klines = self.get_recent_klines(symbol, minutes=minutes)
            
            if not klines:
                return False
            
            # 保存到1分钟K线表（自动去重）
            success = self.save_kline_data(symbol, klines, '1min')
            
            if success:
                logger.debug(f"更新{symbol}的最新1分钟K线数据")
            
            return success
            
        except Exception as e:
            logger.error(f"更新{symbol}1分钟K线数据失败: {str(e)}")
            return False

    def check_price_breakouts(self, klines: List[List]) -> Dict[str, Any]:
        """
        检查最后一根K线价格是否为多个时间区间的最高点（混合K线版本）
        
        Args:
            klines: 混合K线数据列表（历史30分钟K线 + 当天1分钟K线）
            
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

        # 获取最后一根K线的收盘价和时间戳
        current_price = float(klines[-1][4])  # 索引4是收盘价
        current_time_ms = int(klines[-1][0])  # 索引0是开盘时间戳
        current_time = datetime.fromtimestamp(current_time_ms / 1000)

        # 计算各时间区间的截止时间戳
        periods_time = {
            7: current_time - timedelta(days=7),
            15: current_time - timedelta(days=15), 
            30: current_time - timedelta(days=30)
        }

        breakouts = {}
        breakout_periods = []
        has_breakout = False

        for days, cutoff_time in periods_time.items():
            cutoff_time_ms = int(cutoff_time.timestamp() * 1000)
            
            # 筛选指定时间区间内的K线（排除最后一根K线）
            period_klines = []
            for kline in klines[:-1]:  # 排除最后一根K线
                kline_time_ms = int(kline[0])
                if kline_time_ms >= cutoff_time_ms:
                    period_klines.append(kline)

            if not period_klines:
                breakouts[days] = {
                    'is_high': False,
                    'max_high': 0.0,
                    'min_low': 0.0
                }
                continue

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

    async def get_funding_rate_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取资金费率信息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            Dict: 资金费率信息
        """
        try:
            # 获取该交易对的结算周期信息
            symbol_funding_info = self.funding_info_data.get(symbol, {})
            settlement_hours = symbol_funding_info.get('funding_interval_hours', 8)
            
            # 使用ccxt的fetch_funding_rate方法（更准确）
            funding_rate_info = await self.binance_trading.fetch_funding_rate(symbol)

            if funding_rate_info and 'fundingRate' in funding_rate_info:
                current_rate = float(funding_rate_info['fundingRate'])
                # 年化资金费率 = 当前费率 * (365 * 24 / settlement_hours) * 100
                annualized_rate = current_rate * (365 * 24 / settlement_hours) * 100

                logger.debug(f"{symbol} 资金费率: {current_rate:.6f} ({current_rate * 100:.4f}%), 结算周期: {settlement_hours}小时")

                return {
                    'current_rate': current_rate,
                    'current_rate_percent': current_rate * 100,
                    'annualized_rate': annualized_rate,
                    'settlement_hours': settlement_hours
                }

        except Exception as e:
            logger.error(f"获取{symbol}资金费率失败: {str(e)}")

        # 返回默认值，使用缓存的结算周期
        symbol_funding_info = self.funding_info_data.get(symbol, {})
        settlement_hours = symbol_funding_info.get('funding_interval_hours', 8)
        
        return {
            'current_rate': 0.0,
            'current_rate_percent': 0.0,
            'annualized_rate': 0.0,
            'settlement_hours': settlement_hours
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
            
            # 添加K线开始时间
            if analysis_data.get('kline_start_time'):
                kline_start_str = analysis_data['kline_start_time'].strftime('%Y-%m-%d %H:%M:%S')
                message_lines.append(f"• K线开始时间: {kline_start_str}")

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
                f"**合约描述**: {analysis_data['description'][:100]}..." if len(
                    analysis_data['description']) > 100 else f"**合约描述**: {analysis_data['description']}",
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

    def send_filtered_notification(self, symbol: str, analysis_data: Dict[str, Any], filter_reason: str):
        """
        发送因过滤条件未能交易的企业微信通知
        
        Args:
            symbol: 交易对符号
            analysis_data: 分析数据
            filter_reason: 过滤原因
        """
        try:
            base_asset = symbol.replace('USDT', '')

            # 构建突破时间区间信息
            breakout_periods = sorted(analysis_data['breakout_periods'])
            periods_str = ', '.join([f"{days}天" for days in breakout_periods])

            # 构建消息内容
            message_lines = [
                f"⚠️ **价格突破但被过滤提醒**",
                f"",
                f"**合约**: {symbol}",
                f"**当前价格**: ${analysis_data['current_price']:.6f}",
                f"**突破区间**: {periods_str}",
                f"**过滤原因**: {filter_reason}",
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
            
            # 添加K线开始时间
            if analysis_data.get('kline_start_time'):
                kline_start_str = analysis_data['kline_start_time'].strftime('%Y-%m-%d %H:%M:%S')
                message_lines.append(f"• K线开始时间: {kline_start_str}")

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
                f"**合约描述**: {analysis_data['description'][:100]}..." if len(
                    analysis_data['description']) > 100 else f"**合约描述**: {analysis_data['description']}",
                f"",
                f"**标签**: {', '.join(analysis_data['tags'])}" if analysis_data['tags'] else "**标签**: 无",
                f"",
                f"**提示**: 该交易对因不满足交易条件而被过滤，请关注后续价格变化",
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
                    logger.info(f"✅ 成功发送{symbol}过滤通知到企业微信群")
                    # 保存过滤通知到文件
                    self.save_filtered_notification_to_file(symbol, message_content, analysis_data, filter_reason)
                else:
                    logger.error(f"❌ 发送{symbol}过滤通知失败: {result}")
            else:
                logger.error(f"❌ 发送{symbol}过滤通知失败，状态码: {response.status_code}")

        except Exception as e:
            logger.error(f"❌ 发送{symbol}过滤企业微信通知失败: {str(e)}")

    def send_trading_notification(self, symbol: str, order_details: Dict[str, Any], analysis_data: Dict[str, Any]):
        """
        发送交易下单企业微信通知
        
        Args:
            symbol: 交易对符号
            order_details: 订单详情
            analysis_data: 分析数据
        """
        try:
            base_asset = symbol.replace('USDT', '')

            # 构建交易通知消息
            message_lines = [
                f"🚨 **自动交易执行通知**",
                f"",
                f"**合约**: {symbol}",
                f"**交易方向**: 卖空(SHORT)",
                f"**杠杆倍数**: {self.leverage}倍",
                f"**保证金**: {self.margin_amount} USDT",
                f"",
                f"**订单详情**:",
                f"• 订单ID: {order_details.get('order_id', 'N/A')}",
                f"• 成交价格: ${order_details.get('filled_price', 0):.6f}",
                f"• 成交数量: {order_details.get('filled_quantity', 0):.6f}",
                f"• 成交金额: ${order_details.get('filled_price', 0) * order_details.get('filled_quantity', 0):.2f}",
                f"",
                f"**突破信息**:",
            ]

            # 添加突破区间信息
            breakout_periods = sorted(analysis_data['breakout_periods'])
            periods_str = ', '.join([f"{days}天" for days in breakout_periods])
            message_lines.append(f"• 突破区间: {periods_str}")

            # 添加各时间区间对比
            for days in [7, 15, 30]:
                if days in analysis_data['breakouts']:
                    breakout_info = analysis_data['breakouts'][days]
                    status = "✅ 突破" if breakout_info['is_high'] else "❌ 未突破"
                    message_lines.extend([
                        f"• {days}天: {status}",
                        f"  └ 最高价: ${breakout_info['max_high']:.6f}",
                    ])

            message_lines.extend([
                f"",
                f"**资金费率**: {analysis_data['funding_rate']['current_rate_percent']:.4f}%",
                f"**代币信息**:",
                f"• 市值排名: #{analysis_data['token_info']['market_rank']}" if analysis_data['token_info'][
                                                                                    'market_rank'] > 0 else "• 市值排名: 未知",
                f"• 发行日期: {analysis_data['token_info']['launch_date_str']}",
            ])
            
            # 添加K线开始时间
            if analysis_data.get('kline_start_time'):
                kline_start_str = analysis_data['kline_start_time'].strftime('%Y-%m-%d %H:%M:%S')
                message_lines.append(f"• K线开始时间: {kline_start_str}")

            # 添加交易原因
            if order_details.get('is_first_trade', False):
                message_lines.append(f"**交易原因**: 首次检测到价格突破，执行初始卖空")
            else:
                # 获取上次交易记录来计算价格涨幅
                latest_records = []
                try:
                    conn = pymysql.connect(**self.mysql_config)
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT open_price FROM trading_records 
                        WHERE symbol = %s 
                        ORDER BY order_time DESC 
                        LIMIT 2
                    """, (symbol,))
                    latest_records = cursor.fetchall()
                    conn.close()
                except Exception as e:
                    logger.error(f"查询{symbol}历史交易记录失败: {str(e)}")
                
                if len(latest_records) >= 2:
                    # 获取倒数第二条记录（因为最新的记录就是刚刚保存的当前交易）
                    last_price = latest_records[1][0]
                    current_price = analysis_data['current_price']
                    price_increase = (current_price - last_price) / last_price * 100
                    message_lines.append(f"**交易原因**: 价格较上次开仓上涨{price_increase:.2f}%，执行追加卖空")
                else:
                    message_lines.append(f"**交易原因**: 检测到价格突破，执行追加卖空")

            message_lines.extend([
                f"",
                f"**风险提示**: 请密切关注仓位风险，及时止盈止损",
                f"**执行时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
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
                    logger.info(f"✅ 成功发送{symbol}交易通知到企业微信群")
                    # 保存交易通知到文件
                    self.save_trading_notification_to_file(symbol, message_content, order_details, analysis_data)
                else:
                    logger.error(f"❌ 发送{symbol}交易通知失败: {result}")
            else:
                logger.error(f"❌ 发送{symbol}交易通知失败，状态码: {response.status_code}")

        except Exception as e:
            logger.error(f"❌ 发送{symbol}交易企业微信通知失败: {str(e)}")

    def save_trading_notification_to_file(self, symbol: str, message_content: str, order_details: Dict[str, Any],
                                          analysis_data: Dict[str, Any]):
        """
        保存交易通知内容到文件
        
        Args:
            symbol: 交易对符号
            message_content: 消息内容
            order_details: 订单详情
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
            filename = f"{timestamp_str}_{symbol}_TRADING.txt"
            file_path = os.path.join(date_dir, filename)

            # 准备保存的内容
            file_content = [
                f"=" * 80,
                f"自动交易执行记录",
                f"=" * 80,
                f"交易对: {symbol}",
                f"执行时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}",
                f"订单ID: {order_details.get('order_id', 'N/A')}",
                f"成交价格: ${order_details.get('filled_price', 0):.6f}",
                f"成交数量: {order_details.get('filled_quantity', 0):.6f}",
                f"杠杆倍数: {self.leverage}倍",
                f"保证金: {self.margin_amount} USDT",
                f"",
                f"通知内容:",
                f"-" * 40,
                message_content,
                f"",
                f"=" * 80,
                f""
            ]

            # 写入文件
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(file_content))

            logger.info(f"💾 交易通知内容已保存到文件: {file_path}")

            # 同时保存到按日期汇总的交易文件
            trading_summary_file = os.path.join(date_dir, f"{date_str}_trading_summary.txt")
            trading_summary_content = f"[{timestamp_str}] {symbol} - 卖空 {order_details.get('filled_quantity', 0):.6f} @ ${order_details.get('filled_price', 0):.6f} (订单ID: {order_details.get('order_id', 'N/A')})\n"

            with open(trading_summary_file, 'a', encoding='utf-8') as f:
                f.write(trading_summary_content)

        except Exception as e:
            logger.error(f"❌ 保存{symbol}交易通知到文件失败: {str(e)}")

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

    def save_filtered_notification_to_file(self, symbol: str, message_content: str, analysis_data: Dict[str, Any], filter_reason: str):
        """保存过滤通知到文件"""
        try:
            # 创建通知目录
            notifications_dir = "trade/notifications"
            os.makedirs(notifications_dir, exist_ok=True)

            # 文件名格式：filtered_SYMBOL_YYYYMMDD_HHMMSS.txt
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"filtered_{symbol}_{timestamp}.txt"
            filepath = os.path.join(notifications_dir, filename)

            # 保存内容
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("=== 价格突破但被过滤通知 ===\n")
                f.write(f"合约: {symbol}\n")
                f.write(f"过滤原因: {filter_reason}\n")
                f.write(f"当前价格: ${analysis_data['current_price']:.6f}\n")
                f.write(f"突破区间: {', '.join([f'{days}天' for days in sorted(analysis_data['breakout_periods'])])}\n")
                f.write(f"发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("\n=== 通知内容 ===\n")
                f.write(message_content)

            logger.debug(f"📁 已保存{symbol}过滤通知到文件: {filepath}")

        except Exception as e:
            logger.error(f"❌ 保存{symbol}过滤通知到文件失败: {str(e)}")

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

        # 检查K线开始时间（用于判断上市时间）
        kline_start_time = analysis_data.get('kline_start_time')
        if not kline_start_time:
            return True, "K线开始时间数据为空"

        days_since_kline_start = (datetime.now() - kline_start_time).days

        if days_since_kline_start < self.min_launch_days:
            return True, f"K线数据仅{days_since_kline_start}天，小于{self.min_launch_days}天"

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
            return True, f"资金费率{current_rate * 100:.4f}%，小于{self.min_funding_rate * 100:.4f}%"

        return False, "通过过滤条件"

    async def get_current_positions(self) -> Dict[str, float]:
        """获取当前合约持仓"""
        try:
            if not self.binance_trading:
                return {}

            positions = await self.binance_trading.fetch_positions()
            position_dict = {}

            for position in positions:
                symbol = position['symbol'].replace(':USDT', '').replace('/', '')
                size = float(position['contracts'])
                if size != 0:  # 只记录有持仓的
                    position_dict[symbol] = size

            return position_dict

        except Exception as e:
            logger.error(f"获取当前持仓失败: {str(e)}")
            return {}

    async def clean_trade_records(self):
        """清理交易记录 - 更新没有持仓的交易对记录为已平仓状态"""
        try:
            # 获取当前持仓
            current_positions = await self.get_current_positions()
            logger.info(f"获取到账户当前合约持仓为: {current_positions}")

            # 获取所有有未平仓交易记录的交易对
            open_traded_symbols = self.get_open_traded_symbols()

            # 检查哪些交易对没有持仓了，更新为已平仓状态
            for symbol in open_traded_symbols:
                if symbol not in current_positions:
                    logger.info(f"检测到{symbol}已无持仓，更新交易记录为已平仓状态")
                    self.update_symbol_to_closed_status(symbol)

        except Exception as e:
            logger.error(f"清理交易记录失败: {str(e)}")

    async def execute_short_order(self, symbol: str, current_price: float,
                                  analysis_data: Dict[str, Any] = None) -> bool:
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

            # 获取最大杠杆倍数并计算实际使用的杠杆
            max_leverage = await self.get_max_leverage(symbol)
            actual_leverage = min(self.leverage, max_leverage)
            logger.info(
                f"{symbol} 配置杠杆: {self.leverage}倍, 最大支持: {max_leverage}倍, 实际使用: {actual_leverage}倍")

            # 计算交易数量 (保证金 * 实际杠杆 / 价格)
            quantity = (self.margin_amount * actual_leverage) / current_price

            # 设置实际杠杆
            await self.binance_trading.fapiPrivatePostLeverage({
                'symbol': symbol,
                'leverage': actual_leverage
            })
            logger.info(f"已设置{symbol}杠杆为{actual_leverage}倍")

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

                # 检查订单状态
                try:
                    order_status = await self.binance_trading.fetch_order(order_id, symbol)
                    if order_status and order_status.get('status') == 'closed':
                        logger.info(f"✅ {symbol} 空单已完成，准备提交止盈限价单")

                        # 计算止盈价格 (95% of 空单价格)
                        take_profit_price = filled_price * 0.95

                        # 提交限价平仓单 (买入平仓)
                        try:
                            close_order = await self.binance_trading.create_limit_buy_order(
                                symbol=symbol,
                                amount=filled_quantity,
                                price=take_profit_price,
                                params={'positionSide': 'SHORT'}
                            )

                            if close_order and close_order.get('id'):
                                close_order_id = close_order.get('id')
                                logger.info(f"🎯 止盈限价单提交成功: {symbol}")
                                logger.info(f"止盈订单ID: {close_order_id}")
                                logger.info(f"止盈价格: ${take_profit_price:.6f}")
                                logger.info(f"预期盈利: ${(filled_price - take_profit_price) * filled_quantity:.2f}")
                                
                                # 更新交易记录的平仓订单信息
                                self.update_close_order_info(
                                    symbol=symbol,
                                    open_order_id=order_id,
                                    close_price=take_profit_price,
                                    close_order_id=close_order_id,
                                    close_order_status='OPEN'  # 限价单刚提交，状态为OPEN
                                )
                            else:
                                logger.error(f"❌ {symbol} 止盈限价单提交失败")

                        except Exception as close_e:
                            logger.error(f"❌ 提交{symbol}止盈限价单失败: {str(close_e)}")
                    else:
                        logger.warning(f"⚠️ {symbol} 空单状态: {order_status.get('status', 'unknown')}")

                except Exception as status_e:
                    logger.warning(f"⚠️ 检查{symbol}订单状态失败: {str(status_e)}")

                # 在保存记录前检查是否为首次交易
                is_first_trade = self.get_latest_trade_record(symbol) is None
                
                # 保存交易记录
                self.save_trade_record(symbol, filled_price, filled_quantity, order_id)

                # 发送交易通知
                if analysis_data:
                    order_details = {
                        'order_id': order_id,
                        'filled_price': filled_price,
                        'filled_quantity': filled_quantity,
                        'is_first_trade': is_first_trade
                    }
                    self.send_trading_notification(symbol, order_details, analysis_data)

                return True
            else:
                logger.error(f"❌ 卖空订单执行失败: {symbol}")
                return False

        except Exception as e:
            logger.error(f"❌ 执行{symbol}卖空订单失败: {str(e)}")
            return False

    async def check_and_execute_trade(self, symbol: str, analysis_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        检查并执行交易
        
        Args:
            symbol: 交易对符号
            analysis_data: 分析数据
            
        Returns:
            Tuple[bool, str]: (是否执行了交易, 未交易原因/成功信息)
        """
        if not self.enable_trading:
            return False, "交易功能未启用"

        try:
            current_price = analysis_data['current_price']

            # 检查过滤条件
            should_filter, filter_reason = self.should_filter_symbol(symbol, analysis_data)
            if should_filter:
                logger.info(f"🚫 {symbol} 被过滤: {filter_reason}")
                return False, filter_reason

            logger.info(f"✅ {symbol} 通过过滤条件，检查交易条件")

            # 检查交易记录
            latest_record = self.get_latest_trade_record(symbol)

            if not latest_record:
                # 没有交易记录，执行交易
                logger.info(f"💰 {symbol} 无交易记录，执行首次卖空交易")
                trade_success = await self.execute_short_order(symbol, current_price, analysis_data)
                if trade_success:
                    return True, "成功执行首次卖空交易"
                else:
                    return False, "首次卖空交易执行失败"
            else:
                # 有交易记录，检查价格条件
                last_price = latest_record['open_price']
                price_increase = (current_price - last_price) / last_price

                if price_increase >= self.min_price_increase:  # 价格上涨20%以上
                    logger.info(f"💰 {symbol} 价格较上次开仓上涨{price_increase * 100:.2f}%，执行追加卖空交易")
                    trade_success = await self.execute_short_order(symbol, current_price, analysis_data)
                    if trade_success:
                        return True, f"成功执行追加卖空交易（价格上涨{price_increase * 100:.2f}%）"
                    else:
                        return False, "追加卖空交易执行失败"
                else:
                    reason = f"价格较上次开仓仅上涨{price_increase * 100:.2f}%，不满足{self.min_price_increase*100}%条件"
                    logger.info(f"⏸️ {symbol} {reason}")
                    return False, reason

        except Exception as e:
            error_msg = f"检查交易条件失败: {str(e)}"
            logger.error(f"❌ {symbol} {error_msg}")
            return False, error_msg

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

            # 先更新最新的K线数据
            await self.update_kline_data(symbol)

            # 从数据库获取30天的1分钟K线数据
            klines = self.get_kline_data_from_db(symbol, days=self.days_to_analyze)
            if not klines:
                logger.warning(f"{symbol}: 数据库中没有K线数据")
                return False

            # 检查多个时间区间的价格突破
            breakout_result = self.check_price_breakouts(klines)

            current_price = breakout_result['current_price']

            # 保存当前价格（无论是否突破）
            self.save_current_price(symbol, current_price)

            if not breakout_result['has_breakout']:
                return False

            breakout_periods = breakout_result['breakout_periods']
            periods_str = ', '.join([f"{days}天" for days in sorted(breakout_periods)])

            logger.info(f"🎯 发现价格突破: {symbol} 当前价格 ${current_price:.6f} 突破 {periods_str} 高点")

            # 获取基础资产
            base_asset = symbol.replace('USDT', '')

            # 获取补充信息
            funding_rate_info = await self.get_funding_rate_info(symbol)
            token_info = self.get_token_info(base_asset)
            description = self.get_symbol_description(base_asset)
            tags = self.get_symbol_tags(symbol)
            kline_start_time = self.get_kline_start_time(symbol)

            # 组合分析数据
            analysis_data = {
                'current_price': current_price,
                'breakout_periods': breakout_periods,
                'breakouts': breakout_result['breakouts'],
                'funding_rate': funding_rate_info,
                'token_info': token_info,
                'description': description,
                'tags': tags,
                'kline_start_time': kline_start_time
            }

            # 发送通知
            self.send_wework_notification(symbol, analysis_data)

            # 如果启用了交易功能，检查并执行交易
            if self.enable_trading:
                try:
                    trade_executed, reason = await self.check_and_execute_trade(symbol, analysis_data)
                    if trade_executed:
                        logger.info(f"💰 {symbol} 交易执行成功: {reason}")
                    else:
                        logger.info(f"⏸️ {symbol} 未执行交易: {reason}")
                        # 如果是因为过滤条件未执行交易，发送过滤通知
                        if reason != "交易功能未启用":
                            self.send_filtered_notification(symbol, analysis_data, reason)
                except Exception as e:
                    logger.error(f"❌ {symbol} 交易执行失败: {str(e)}")

            return True

        except Exception as e:
            logger.error(f"分析{symbol}失败: {str(e)}")
            return False

    async def run_scan(self):
        """
        运行扫描（建议5分钟间隔运行）
        """
        logger.info(f"🚀 开始扫描Binance合约价格突破（{self.days_to_analyze}天历史数据，混合K线）...")

        # 首先检查并处理跨天情况
        logger.info("🔍 检查跨天处理...")
        day_change_success = await self.check_and_handle_day_change()
        if not day_change_success:
            logger.warning("⚠️ 跨天处理失败，但继续执行扫描")

        # 检查并更新平仓订单状态
        logger.info("🔍 检查平仓订单状态...")
        await self.check_and_update_close_orders()

        # 记录扫描开始时的交易记录数量（用于计算新增交易数）
        initial_trade_count = 0
        if self.enable_trading:
            logger.info("🧹 清理交易记录...")
            await self.clean_trade_records()
            # 获取扫描开始时的交易记录数量
            initial_trade_count = self._get_total_trade_records_count()

        # 获取所有合约符号
        symbols = self.get_all_futures_symbols()
        if not symbols:
            logger.error("❌ 未获取到合约交易对，扫描终止")
            return

        logger.info(f"📊 开始扫描 {len(symbols)} 个合约交易对...")

        found_count = 0
        processed_count = 0
        no_data_count = 0

        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"📈 [{i}/{len(symbols)}] 正在分析 {symbol}...")

                # 检查数据库中是否有K线数据
                kline_count = self.get_kline_data_count(symbol)
                if kline_count == 0:
                    logger.warning(f"⚠️ {symbol} 数据库中无K线数据，请先运行初始化")
                    no_data_count += 1
                    continue

                # 分析交易对
                is_breakthrough = await self.analyze_symbol(symbol)

                if is_breakthrough:
                    found_count += 1

                processed_count += 1

                # 避免API限制，添加短暂延迟
                await asyncio.sleep(0.1)  # 缩短延迟，因为每次只获取10分钟数据

            except Exception as e:
                logger.error(f"❌ 处理{symbol}时发生错误: {str(e)}")
                continue

        # 计算本次扫描期间执行的交易数量
        final_trade_count = self._get_total_trade_records_count() if self.enable_trading else 0
        new_trades_count = final_trade_count - initial_trade_count

        logger.info(f"✅ 扫描完成! 处理了 {processed_count} 个交易对，发现 {found_count} 个价格突破")
        if no_data_count > 0:
            logger.warning(f"⚠️ {no_data_count} 个交易对缺少K线数据，请使用 --init 参数进行初始化")
        if self.enable_trading:
            logger.info(f"💰 执行了 {new_trades_count} 笔交易")

        # 更新并显示盈亏信息（不需要重新获取价格，使用扫描过程中的价格数据）
        await self.update_pnl_only(fetch_prices=False)

    async def update_pnl_only(self, fetch_prices: bool = True):
        """更新盈亏信息并显示汇总
        
        Args:
            fetch_prices: 是否需要获取当前价格，默认True
        """
        logger.info("📊 更新交易记录盈亏信息...")

        # 获取所有有交易记录的交易对
        traded_symbols = self.get_all_traded_symbols()
        if not traded_symbols:
            logger.info("💼 未找到任何交易记录")
            return

        if fetch_prices:
            logger.info(f"🔄 开始获取 {len(traded_symbols)} 个交易对的当前价格...")

            # 获取当前价格
            updated_count = 0
            for symbol in traded_symbols:
                try:
                    # 先更新最新K线数据
                    await self.update_kline_data(symbol)
                    
                    # 从数据库获取最新的K线数据来获取当前价格
                    klines = self.get_kline_data_from_db(symbol, days=1)  # 只获取1天的数据就够了
                    if klines and len(klines) > 0:
                        current_price = float(klines[-1][4])  # 最后一根K线的收盘价
                        self.save_current_price(symbol, current_price)
                        logger.debug(f"获取到{symbol}当前价格: ${current_price:.6f}")
                        updated_count += 1
                    else:
                        logger.warning(f"无法获取{symbol}的价格数据")

                    # 避免API限制
                    await asyncio.sleep(0.1)

                except Exception as e:
                    logger.error(f"获取{symbol}价格失败: {str(e)}")

            logger.info(f"📈 成功获取 {updated_count} 个交易对的当前价格")
        else:
            logger.info(f"📊 使用扫描过程中获取的 {len(traded_symbols)} 个交易对价格数据")

        # 更新盈亏信息
        self.update_all_trade_pnl()

        # 显示盈亏汇总
        self._display_pnl_summary()

    def _display_pnl_summary(self):
        """统一显示盈亏汇总信息"""
        pnl_summary = self.get_all_trade_pnl_summary()
        symbol_pnl_summary = self.get_symbol_aggregated_pnl_summary()
        if pnl_summary['positions']:
            logger.info(f"💼 持仓盈亏汇总:")
            logger.info(f"   总盈亏: ${pnl_summary['total_pnl']:.2f}")
            logger.info(f"   盈利仓位: {pnl_summary['profitable_count']}个")
            logger.info(f"   亏损仓位: {pnl_summary['losing_count']}个")

            # 显示所有仓位的详细信息
            sorted_positions = sorted(pnl_summary['positions'], key=lambda x: x['pnl_amount'], reverse=True)

            logger.info(f"   📋 详细持仓信息:")
            for i, pos in enumerate(sorted_positions):
                status = "💰" if pos['pnl_amount'] > 0 else "💸"
                logger.info(f"      {i + 1}. {status} {pos['symbol']}: ${pos['pnl_amount']:.2f} "
                            f"({pos['price_change_percent']:.2f}%) "
                            f"开仓: ${pos['open_price']:.6f} -> 当前: ${pos['current_price']:.6f}")
        else:
            logger.info("💼 当前无持仓记录")

        # 显示按交易对合并的盈亏汇总
        if symbol_pnl_summary['symbol_positions']:
            logger.info(f"")
            logger.info(f"📊 按交易对合并的盈亏汇总:")
            logger.info(f"   总盈亏: ${symbol_pnl_summary['total_pnl']:.2f}")
            logger.info(f"   盈利交易对: {symbol_pnl_summary['profitable_symbols']}个")
            logger.info(f"   亏损交易对: {symbol_pnl_summary['losing_symbols']}个")

            # 显示所有交易对的盈亏情况
            sorted_symbol_positions = sorted(symbol_pnl_summary['symbol_positions'], key=lambda x: x['total_pnl'],
                                             reverse=True)

            logger.info("   📋 各交易对盈亏详情:")
            for i, pos in enumerate(sorted_symbol_positions):
                status = "💰" if pos['total_pnl'] > 0 else "💸"
                logger.info(f"      {i + 1}. {status} {pos['symbol']}: ${pos['total_pnl']:.2f} "
                            f"({pos['price_change_percent']:.2f}%) "
                            f"平均开仓: ${pos['avg_open_price']:.6f} -> 当前: ${pos['avg_current_price']:.6f} "
                            f"({pos['trade_count']}笔交易)")
        else:
            logger.info("📊 当前无按交易对合并的持仓记录")

    async def close(self):
        """关闭交易所连接，释放资源"""
        if self.binance_trading:
            try:
                await self.binance_trading.close()
                logger.info("✅ 交易所连接已关闭")
            except Exception as e:
                logger.error(f"❌ 关闭交易所连接失败: {str(e)}")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Binance价格高点扫描器 (1分钟K线版本)')
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
    parser.add_argument(
        '--pnl-only',
        action='store_true',
        help='仅更新盈亏信息，不进行价格扫描'
    )
    parser.add_argument(
        '--init',
        action='store_true',
        help='初始化所有交易对的K线数据 (首次运行必须)'
    )
    return parser.parse_args()


async def main():
    """主函数"""
    scanner = None
    try:
        # 解析命令行参数
        args = parse_arguments()

        if args.init:
            logger.info("🚀 启动模式: 初始化K线数据")
            logger.warning("⚠️  此操作将花费较长时间，请耐心等待...")
            scanner = BinancePriceHighScanner(days_to_analyze=args.days, enable_trading=False)
            await scanner.initialize_all_kline_data()
        elif args.pnl_only:
            logger.info("🔄 启动模式: 仅更新盈亏信息")
            scanner = BinancePriceHighScanner(days_to_analyze=args.days, enable_trading=False)
            
            # 检查并更新平仓订单状态
            logger.info("🔍 检查平仓订单状态...")
            await scanner.check_and_update_close_orders()
            
            logger.info("🧹 清理交易记录...")
            await scanner.clean_trade_records()
            await scanner.update_pnl_only(fetch_prices=True)
        else:
            logger.info(f"🔄 启动模式: 价格突破扫描")
            logger.info(f"启动参数: 历史分析天数 = {args.days}天, 自动交易 = {'启用' if args.trade else '禁用'}")
            logger.info(f"📊 使用1分钟K线数据，建议每5分钟运行一次")

            if args.trade:
                logger.warning("⚠️  自动交易功能已启用! 请确保您了解交易风险!")

            scanner = BinancePriceHighScanner(days_to_analyze=args.days, enable_trading=args.trade)
            await scanner.run_scan()

    except KeyboardInterrupt:
        logger.info("❌ 用户中断执行")
    except Exception as e:
        logger.error(f"❌ 执行过程中发生错误: {str(e)}")
    finally:
        # 确保关闭交易所连接
        if scanner:
            await scanner.close()


if __name__ == "__main__":
    asyncio.run(main())
