#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多交易所资金费率扫描器
功能：筛选下次资金费率结算时间为下个整点且资金费率小于-0.5%的合约交易对
作者：加密货币套利专家
版本：1.0.0
"""

import asyncio
import argparse
import logging
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import os

import ccxt
import pytz

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import (
        binance_api_key, binance_api_secret,
        bybit_api_key, bybit_api_secret,
        bitget_api_key, bitget_api_secret,
        gateio_api_key, gateio_api_secret,
        proxies
    )
except ImportError:
    print("警告: 无法导入配置文件，请确保config.py存在并包含API密钥")
    # 设置默认值
    binance_api_key = ""
    binance_api_secret = ""
    bybit_api_key = ""
    bybit_api_secret = ""
    bitget_api_key = ""
    bitget_api_secret = ""
    gateio_api_key = ""
    gateio_api_secret = ""
    proxies = {}

# 配置日志
def setup_logging():
    """设置日志配置"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_filename = os.path.join(log_dir, f'funding_rate_scanner_{datetime.now().strftime("%Y%m%d")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

class FundingRateScanner:
    """多交易所资金费率扫描器"""
    
    def __init__(self):
        """初始化扫描器"""
        self.exchanges = {}
        self.funding_rate_threshold = -0.001  # -0.5%
        self.results = []
        
        self._initialize_exchanges()
    
    def _initialize_exchanges(self):
        """初始化所有交易所连接"""
        exchange_configs = {
            'binance': {
                'class': ccxt.binance,
                'config': {
                    'apiKey': binance_api_key,
                    'secret': binance_api_secret,
                    'enableRateLimit': True,
                    'options': {'defaultType': 'future'}
                }
            },
            'bybit': {
                'class': ccxt.bybit,
                'config': {
                    'apiKey': bybit_api_key,
                    'secret': bybit_api_secret,
                    'enableRateLimit': True,
                    'options': {'defaultType': 'linear'}
                }
            },
            'bitget': {
                'class': ccxt.bitget,
                'config': {
                    'apiKey': bitget_api_key,
                    'secret': bitget_api_secret,
                    'enableRateLimit': True,
                    'options': {'defaultType': 'swap'}
                }
            },
            'gateio': {
                'class': ccxt.gateio,
                'config': {
                    'apiKey': gateio_api_key,
                    'secret': gateio_api_secret,
                    'enableRateLimit': True,
                    'options': {'defaultType': 'future'}
                }
            }
        }
        
        for exchange_name, exchange_info in exchange_configs.items():
            try:
                config = exchange_info['config'].copy()
                if proxies:
                    config['proxies'] = proxies
                
                exchange = exchange_info['class'](config)
                exchange.load_markets()
                self.exchanges[exchange_name] = exchange
                logger.info(f"{exchange_name.upper()} 交易所连接成功")
                
            except Exception as e:
                logger.error(f"{exchange_name.upper()} 交易所连接失败: {e}")
    
    def get_next_hour_time(self, current_time: datetime) -> datetime:
        """
        获取下一个整点时间
        
        Args:
            current_time: 当前时间
            
        Returns:
            下一个整点时间
        """
        # 确保使用UTC时间
        if current_time.tzinfo is None:
            # 如果是naive时间，假设它是UTC时间
            current_time = current_time.replace(tzinfo=pytz.UTC)
        
        next_hour = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return next_hour
    
    def is_next_hour_settlement(self, funding_time: datetime, current_time: datetime) -> bool:
        """
        检查资金费率结算时间是否为下一个整点
        
        Args:
            funding_time: 资金费率结算时间
            current_time: 当前时间
            
        Returns:
            是否为下一个整点
        """
        # 统一处理时区，将所有时间转换为UTC
        if funding_time.tzinfo is None:
            funding_time = funding_time.replace(tzinfo=pytz.UTC)
        elif funding_time.tzinfo != pytz.UTC:
            funding_time = funding_time.astimezone(pytz.UTC)
            
        # 处理本地时间 - 假设是东8区
        if current_time.tzinfo is None:
            # 本地时间，假设是东8区
            current_time_with_tz = pytz.timezone('Asia/Shanghai').localize(current_time)
            current_time_utc = current_time_with_tz.astimezone(pytz.UTC)
        else:
            current_time_utc = current_time.astimezone(pytz.UTC)
        
        # 计算下个整点时间（UTC）
        next_hour_utc = current_time_utc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        
        # 允许5分钟的误差
        time_diff = abs((funding_time - next_hour_utc).total_seconds())
        return time_diff <= 300  # 5分钟内算作匹配
    
    async def scan_exchange_funding_rates(self, exchange_name: str) -> List[Dict]:
        """
        扫描单个交易所的资金费率
        
        Args:
            exchange_name: 交易所名称
            
        Returns:
            符合条件的交易对列表
        """
        if exchange_name not in self.exchanges:
            logger.warning(f"{exchange_name.upper()} 交易所未连接，跳过扫描")
            return []
        
        exchange = self.exchanges[exchange_name]
        qualified_pairs = []
        
        try:
            logger.info(f"开始扫描 {exchange_name.upper()} 交易所...")
            
            # 获取所有合约市场
            markets = exchange.markets
            future_symbols = [symbol for symbol, market in markets.items() 
                            if market.get('type') == 'swap' or market.get('type') == 'future']
            
            logger.info(f"{exchange_name.upper()} 找到 {len(future_symbols)} 个合约交易对")
            
            current_time = datetime.now()
            next_hour = self.get_next_hour_time(current_time)
            logger.info(f"{exchange_name.upper()} 当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"{exchange_name.upper()} 下个整点: {next_hour.strftime('%Y-%m-%d %H:%M:%S')}")
            
            checked_count = 0
            error_count = 0
            
            for symbol in future_symbols:
                try:
                    # 获取资金费率信息
                    funding_rate_info = exchange.fetch_funding_rate(symbol)
                    
                    if not funding_rate_info:
                        logger.debug(f"{exchange_name.upper()} {symbol}: 无资金费率信息")
                        continue
                    
                    # 提取关键信息
                    funding_rate = funding_rate_info.get('fundingRate')
                    funding_datetime = funding_rate_info.get('fundingDatetime') or funding_rate_info.get('datetime')
                    
                    # 特殊处理Bitget交易所的数据结构
                    if exchange_name.lower() == 'bitget' and funding_datetime is None:
                        info = funding_rate_info.get('info', {})
                        next_update = info.get('nextUpdate')
                        if next_update:
                            # nextUpdate是毫秒时间戳
                            funding_datetime = int(next_update)
                    
                    # logger.info(f"{exchange_name} {symbol} {funding_rate} {funding_datetime}")
                    
                    if funding_rate is None:
                        logger.debug(f"{exchange_name.upper()} {symbol}: 资金费率为空")
                        continue
                        
                    if funding_datetime is None:
                        logger.debug(f"{exchange_name.upper()} {symbol}: 结算时间为空")
                        continue
                    
                    # 转换时间格式
                    if isinstance(funding_datetime, str):
                        funding_time = datetime.fromisoformat(funding_datetime.replace('Z', '+00:00'))
                    elif isinstance(funding_datetime, (int, float)):
                        # 处理时间戳格式（毫秒或秒）
                        if funding_datetime > 10**10:  # 毫秒时间戳
                            funding_time = datetime.fromtimestamp(funding_datetime / 1000, tz=pytz.UTC)
                        else:  # 秒时间戳
                            funding_time = datetime.fromtimestamp(funding_datetime, tz=pytz.UTC)
                    else:
                        funding_time = funding_datetime
                    
                    # 确保时间有时区信息
                    if funding_time.tzinfo is None:
                        funding_time = funding_time.replace(tzinfo=pytz.UTC)
                    
                    checked_count += 1
                    
                    # 检查条件
                    is_negative_enough = funding_rate < self.funding_rate_threshold
                    is_next_hour = self.is_next_hour_settlement(funding_time, current_time)
                    
                    logger.debug(f"{exchange_name.upper()} {symbol}: 费率={funding_rate:.6f} ({funding_rate*100:.4f}%), "
                               f"结算时间={funding_time}, 费率条件={is_negative_enough}, 时间条件={is_next_hour}")
                    
                    if is_negative_enough and is_next_hour:
                        qualified_pairs.append({
                            'exchange': exchange_name.upper(),
                            'symbol': symbol,
                            'funding_rate': funding_rate,
                            'funding_rate_pct': funding_rate * 100,
                            'next_funding_time': funding_time,
                            'current_time': current_time
                        })
                        
                        logger.info(f"✅ {exchange_name.upper()} {symbol}: {funding_rate*100:.4f}% @ {funding_time}")
                    elif is_negative_enough:
                        # 费率满足但时间不满足的情况
                        if 'LA/USDT' in symbol:  # 特别关注LA/USDT
                            logger.info(f"🟡 {exchange_name.upper()} {symbol}: 费率满足({funding_rate*100:.4f}%)但时间不满足({funding_time})")
                        else:
                            logger.debug(f"🟡 {exchange_name.upper()} {symbol}: 费率满足({funding_rate*100:.4f}%)但时间不满足({funding_time})")
                    elif is_next_hour:
                        # 时间满足但费率不满足的情况
                        logger.debug(f"🟡 {exchange_name.upper()} {symbol}: 时间满足({funding_time})但费率不满足({funding_rate*100:.4f}%)")
                    
                    # 每检查100个交易对暂停一下，避免API限制
                    if checked_count % 100 == 0:
                        await asyncio.sleep(1)
                        logger.info(f"{exchange_name.upper()} 已检查 {checked_count}/{len(future_symbols)} 个交易对, 发现 {len(qualified_pairs)} 个机会")
                
                except Exception as e:
                    error_count += 1
                    error_msg = str(e)
                    
                    if "rate limit" in error_msg.lower():
                        logger.warning(f"{exchange_name.upper()} {symbol}: API限制 - {error_msg}")
                        logger.info(f"{exchange_name.upper()} API限制，等待5秒...")
                        await asyncio.sleep(5)
                    elif "funding" in error_msg.lower() or "not supported" in error_msg.lower():
                        # 某些交易对可能不支持资金费率
                        logger.debug(f"{exchange_name.upper()} {symbol}: 不支持资金费率 - {error_msg}")
                    else:
                        # 其他未知错误，打印详细信息
                        logger.warning(f"{exchange_name.upper()} {symbol}: 处理失败 - {error_msg}")
                        if logger.level <= logging.DEBUG:
                            logger.debug(f"{exchange_name.upper()} {symbol}: 错误详情: {traceback.format_exc()}")
            
            logger.info(f"{exchange_name.upper()} 扫描完成: 共检查 {checked_count} 个交易对，发生 {error_count} 个错误，找到 {len(qualified_pairs)} 个符合条件")
            
            # 如果没有找到符合条件的交易对，提供一些统计信息
            if len(qualified_pairs) == 0:
                logger.info(f"{exchange_name.upper()} 未找到符合条件的交易对，建议检查:")
                logger.info(f"  - 当前资金费率阈值: {self.funding_rate_threshold*100:.3f}%")
                logger.info(f"  - 当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"  - 目标结算时间: {next_hour.strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            logger.error(f"扫描 {exchange_name.upper()} 交易所失败: {e}")
            logger.error(f"错误详情: {traceback.format_exc()}")
        
        return qualified_pairs
    
    async def scan_all_exchanges(self) -> List[Dict]:
        """
        扫描所有交易所的资金费率
        
        Returns:
            所有符合条件的交易对列表
        """
        logger.info("=" * 80)
        logger.info("开始扫描所有交易所的资金费率")
        logger.info(f"筛选条件: 资金费率 < {self.funding_rate_threshold*100:.1f}% 且下次结算时间为下个整点")
        logger.info("=" * 80)
        
        # 并发扫描所有交易所
        tasks = []
        for exchange_name in self.exchanges.keys():
            task = self.scan_exchange_funding_rates(exchange_name)
            tasks.append(task)
        
        # 等待所有任务完成
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 收集所有结果
        all_qualified_pairs = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"交易所 {list(self.exchanges.keys())[i]} 扫描出错: {result}")
            else:
                all_qualified_pairs.extend(result)
        
        # 按资金费率排序（从最负到最不负）
        all_qualified_pairs.sort(key=lambda x: x['funding_rate'])
        
        return all_qualified_pairs
    
    def print_results(self, qualified_pairs: List[Dict]):
        """
        打印扫描结果
        
        Args:
            qualified_pairs: 符合条件的交易对列表
        """
        logger.info("=" * 80)
        logger.info("扫描结果汇总")
        logger.info("=" * 80)
        
        if not qualified_pairs:
            logger.info("❌ 未找到符合条件的交易对")
            return
        
        logger.info(f"✅ 找到 {len(qualified_pairs)} 个符合条件的交易对:")
        logger.info("")
        
        # 表头
        logger.info(f"{'序号':<4} {'交易所':<8} {'交易对':<15} {'资金费率':<10} {'下次结算时间':<20}")
        logger.info("-" * 70)
        
        # 详细信息
        for i, pair in enumerate(qualified_pairs, 1):
            logger.info(f"{i:<4} {pair['exchange']:<8} {pair['symbol']:<15} "
                       f"{pair['funding_rate_pct']:>7.4f}%  {pair['next_funding_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("-" * 70)
        logger.info(f"总计: {len(qualified_pairs)} 个机会")
        
        # 统计每个交易所的数量
        exchange_stats = {}
        for pair in qualified_pairs:
            exchange = pair['exchange']
            exchange_stats[exchange] = exchange_stats.get(exchange, 0) + 1
        
        logger.info("")
        logger.info("各交易所统计:")
        for exchange, count in exchange_stats.items():
            logger.info(f"  {exchange}: {count} 个机会")
    
    async def run_scan(self):
        """运行完整扫描流程"""
        try:
            start_time = datetime.now()
            logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 执行扫描
            qualified_pairs = await self.scan_all_exchanges()
            
            # 打印结果
            self.print_results(qualified_pairs)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info("")
            logger.info(f"扫描完成，耗时: {duration:.1f} 秒")
            
            return qualified_pairs
            
        except Exception as e:
            logger.error(f"扫描过程出错: {e}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return []

async def main():
    """主函数"""
    try:
        # 添加命令行参数解析
        parser = argparse.ArgumentParser(description='多交易所资金费率扫描器')
        parser.add_argument('--debug', action='store_true', help='启用调试模式，显示详细信息')
        parser.add_argument('--threshold', type=float, default=-0.005, help='资金费率阈值 (默认: -0.005 即 -0.5%%)')
        parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO', help='日志级别')
        
        args = parser.parse_args()
        
        # 设置日志级别
        if args.debug:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(getattr(logging, args.log_level))
        
        scanner = FundingRateScanner()
        
        # 如果指定了阈值，更新扫描器的阈值
        if args.threshold != -0.001:
            scanner.funding_rate_threshold = args.threshold
            logger.info(f"使用自定义资金费率阈值: {args.threshold*100:.3f}%")
        
        await scanner.run_scan()
        
    except KeyboardInterrupt:
        logger.info("用户中断程序")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main()) 