#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binance期货价格数据WebSocket收集器

该脚本使用Binance WebSocket API实时收集所有合约交易对的价格数据，
并将数据存储到sqlite3数据库中用于后续分析。

主要功能：
1. 连接Binance WebSocket API
2. 订阅所有合约交易对的价格ticker数据
3. 实时将价格数据写入sqlite3数据库
4. 支持断线重连和错误处理
5. 提供数据统计和监控功能

数据库表结构：
- price_data: 存储实时价格数据
  - id: 主键
  - symbol: 交易对符号
  - price: 价格
  - timestamp: 时间戳
  - created_at: 记录创建时间

作者: Claude
创建时间: 2024-12-30
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import json
import sqlite3
import asyncio
import websockets
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
import logging
from tools.logger import logger
import threading
import signal
import traceback
from config import project_root, proxies


class BinancePriceWebSocketCollector:
    """Binance价格数据WebSocket收集器"""
    
    def __init__(self, db_path: str = os.path.join(project_root, 'trade', "trading_records.db")):
        """
        初始化WebSocket收集器
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        self.websocket_url = "wss://ws-fapi.binance.com/ws-fapi/v1"
        self.websocket = None
        self.is_running = False
        self.reconnect_delay = 5  # 重连延迟（秒）
        self.max_reconnect_attempts = 10
        self.message_count = 0
        self.last_message_time = time.time()
        self.error_count = 0
        
        # 统计信息
        self.stats = {
            'total_messages': 0,
            'successful_inserts': 0,
            'failed_inserts': 0,
            'start_time': datetime.now(),
            'last_update': datetime.now()
        }
        
        # 初始化数据库
        self._init_database()
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Binance价格数据WebSocket收集器初始化完成")
        logger.info(f"数据库路径: {self.db_path}")
        logger.info(f"WebSocket URL: {self.websocket_url}")

    def _init_database(self):
        """初始化数据库表结构"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 创建价格数据表
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS price_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    price REAL NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # 创建索引
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_data_symbol ON price_data(symbol)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_data_timestamp ON price_data(timestamp)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_data_created_at ON price_data(created_at)")
                
                # 创建统计表
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS collection_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    total_messages INTEGER,
                    successful_inserts INTEGER,
                    failed_inserts INTEGER,
                    error_count INTEGER,
                    start_time DATETIME,
                    last_update DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                conn.commit()
                logger.info("数据库表初始化完成")
                
        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}")
            raise

    def _signal_handler(self, signum, frame):
        """信号处理函数"""
        logger.info(f"接收到信号 {signum}，正在优雅退出...")
        self.stop()

    def _insert_price_data(self, symbol: str, price: float, timestamp: int):
        """
        插入价格数据到数据库
        
        Args:
            symbol: 交易对符号
            price: 价格
            timestamp: 时间戳
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                INSERT INTO price_data (symbol, price, timestamp)
                VALUES (?, ?, ?)
                """, (symbol, price, timestamp))
                conn.commit()
                self.stats['successful_inserts'] += 1
                
        except Exception as e:
            logger.error(f"插入价格数据失败 {symbol}: {str(e)}")
            self.stats['failed_inserts'] += 1

    def _update_stats(self):
        """更新统计信息到数据库"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                INSERT INTO collection_stats 
                (total_messages, successful_inserts, failed_inserts, error_count, start_time, last_update)
                VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    self.stats['total_messages'],
                    self.stats['successful_inserts'],
                    self.stats['failed_inserts'],
                    self.error_count,
                    self.stats['start_time'],
                    datetime.now()
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"更新统计信息失败: {str(e)}")

    def _process_message(self, message_data: Dict[str, Any]):
        """
        处理WebSocket消息
        
        Args:
            message_data: WebSocket消息数据
        """
        try:
            # 检查消息状态
            if message_data.get('status') != 200:
                logger.warning(f"WebSocket消息状态异常: {message_data}")
                return
            
            result = message_data.get('result')
            if not result:
                return
            
            # 处理单个ticker数据
            if isinstance(result, dict) and 'symbol' in result:
                self._process_ticker_data(result)
            # 处理多个ticker数据
            elif isinstance(result, list):
                for ticker in result:
                    if isinstance(ticker, dict) and 'symbol' in ticker:
                        self._process_ticker_data(ticker)
                        
        except Exception as e:
            logger.error(f"处理WebSocket消息失败: {str(e)}")
            self.error_count += 1

    def _process_ticker_data(self, ticker_data: Dict[str, Any]):
        """
        处理单个ticker数据
        
        Args:
            ticker_data: ticker数据
        """
        try:
            symbol = ticker_data.get('symbol')
            price = float(ticker_data.get('price', 0))
            timestamp = ticker_data.get('time', int(time.time() * 1000))
            
            if symbol and price > 0:
                self._insert_price_data(symbol, price, timestamp)
                self.message_count += 1
                self.last_message_time = time.time()
                
                # 每1000条消息打印一次统计
                if self.message_count % 1000 == 0:
                    self._print_stats()
                    
        except Exception as e:
            logger.error(f"处理ticker数据失败: {str(e)}")
            self.error_count += 1

    def _print_stats(self):
        """打印统计信息"""
        self.stats['total_messages'] = self.message_count
        self.stats['last_update'] = datetime.now()
        
        runtime = datetime.now() - self.stats['start_time']
        avg_rate = self.message_count / runtime.total_seconds() if runtime.total_seconds() > 0 else 0
        
        logger.info(f"统计信息 - 总消息: {self.stats['total_messages']}, "
                   f"成功插入: {self.stats['successful_inserts']}, "
                   f"失败插入: {self.stats['failed_inserts']}, "
                   f"错误数: {self.error_count}, "
                   f"平均速率: {avg_rate:.1f} msg/s")

    async def _send_price_ticker_request(self):
        """发送价格ticker订阅请求"""
        try:
            # 根据API文档，不指定symbol参数将获取所有交易对的价格
            request = {
                "id": "price_ticker_all",
                "method": "ticker.price",
                "params": {}  # 空参数获取所有交易对
            }
            
            await self.websocket.send(json.dumps(request))
            logger.info("已发送所有交易对价格ticker请求")
            
        except Exception as e:
            logger.error(f"发送ticker请求失败: {str(e)}")
            raise

    async def _websocket_handler(self):
        """WebSocket消息处理器"""
        try:
            while self.is_running:
                try:
                    # 等待消息，设置超时
                    message = await asyncio.wait_for(
                        self.websocket.recv(), 
                        timeout=30.0
                    )
                    
                    # 解析消息
                    try:
                        message_data = json.loads(message)
                        self._process_message(message_data)
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON解析失败: {str(e)}")
                        continue
                        
                except asyncio.TimeoutError:
                    logger.warning("WebSocket消息接收超时，发送ping保持连接")
                    await self.websocket.ping()
                    continue
                    
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("WebSocket连接关闭")
                    break
                    
        except Exception as e:
            logger.error(f"WebSocket处理器异常: {str(e)}")
            logger.error(traceback.format_exc())

    async def _connect_and_run(self):
        """连接WebSocket并运行"""
        attempt = 0
        
        while self.is_running and attempt < self.max_reconnect_attempts:
            try:
                logger.info(f"正在连接WebSocket... (尝试 {attempt + 1}/{self.max_reconnect_attempts})")
                
                async with websockets.connect(
                    self.websocket_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                    proxy=proxies['https']
                ) as websocket:
                    self.websocket = websocket
                    logger.info("WebSocket连接成功")
                    
                    # 发送订阅请求
                    await self._send_price_ticker_request()
                    
                    # 开始处理消息
                    await self._websocket_handler()
                    
            except Exception as e:
                logger.error(f"WebSocket连接失败 (尝试 {attempt + 1}): {str(e)}")
                
                if self.is_running:
                    await asyncio.sleep(self.reconnect_delay)
                    attempt += 1
                else:
                    break
                    
        if attempt >= self.max_reconnect_attempts:
            logger.error("达到最大重连次数，停止尝试")
            self.is_running = False

    def start(self):
        """启动WebSocket收集器"""
        if self.is_running:
            logger.warning("收集器已在运行中")
            return
        
        self.is_running = True
        self.stats['start_time'] = datetime.now()
        logger.info("启动Binance价格数据WebSocket收集器")
        
        try:
            # 运行异步事件循环
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._connect_and_run())
            
        except KeyboardInterrupt:
            logger.info("接收到中断信号")
        except Exception as e:
            logger.error(f"收集器运行异常: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            self.stop()

    def stop(self):
        """停止WebSocket收集器"""
        if not self.is_running:
            return
        
        logger.info("正在停止WebSocket收集器...")
        self.is_running = False
        
        # 更新最终统计信息
        self._update_stats()
        self._print_stats()
        
        logger.info("WebSocket收集器已停止")

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        runtime = datetime.now() - self.stats['start_time']
        avg_rate = self.message_count / runtime.total_seconds() if runtime.total_seconds() > 0 else 0
        
        return {
            'total_messages': self.message_count,
            'successful_inserts': self.stats['successful_inserts'],
            'failed_inserts': self.stats['failed_inserts'],
            'error_count': self.error_count,
            'runtime_seconds': runtime.total_seconds(),
            'average_rate': avg_rate,
            'last_message_time': self.last_message_time,
            'is_running': self.is_running
        }

def main():
    """主函数"""
    try:
        # 设置日志级别
        logger.setLevel(logging.INFO)
        
        # 创建收集器实例
        collector = BinancePriceWebSocketCollector()
        
        logger.info("=== Binance价格数据WebSocket收集器 ===")
        logger.info("按 Ctrl+C 停止收集器")
        
        # 启动收集器
        collector.start()
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        logger.error(traceback.format_exc())
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 