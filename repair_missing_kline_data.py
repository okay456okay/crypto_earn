#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
K线数据修复工具

该脚本用于检测并修复数据库中缺失的K线数据：
1. 自动检测缺失的1分钟K线数据（当天）
2. 自动检测缺失的30分钟K线数据（历史）
3. 从Binance API获取缺失的数据并补充到数据库

使用方法：
python repair_missing_kline_data.py --check-only  # 仅检查，不修复
python repair_missing_kline_data.py --symbol BTCUSDT  # 修复特定交易对
python repair_missing_kline_data.py --repair-all  # 修复所有交易对

作者: Assistant
创建时间: 2024-12-30
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
import argparse
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
from tools.logger import logger
from config import mysql_config, binance_api_key, binance_api_secret, proxies
from binance.client import Client


class KlineDataRepairer:
    """K线数据修复器"""
    
    def __init__(self):
        """初始化修复器"""
        self.mysql_config = mysql_config
        self.current_time = datetime.now()
        self.today_start = self.current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        self.analysis_start = self.today_start - timedelta(days=29)  # 30天前
        
        # 初始化Binance客户端
        self.client = Client(
            binance_api_key,
            binance_api_secret,
            requests_params={'proxies': proxies}
        )
        
        logger.info(f"K线数据修复器初始化完成")
        logger.info(f"修复时间范围: {self.analysis_start.strftime('%Y-%m-%d %H:%M:%S')} 到 {self.current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    def find_missing_1min_data(self, symbol: str) -> List[Tuple[datetime, datetime]]:
        """找出1分钟K线数据的缺失时间段"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # 获取当天的所有1分钟K线时间戳
            today_start_ms = int(self.today_start.timestamp() * 1000)
            current_time_ms = int(self.current_time.timestamp() * 1000)
            
            cursor.execute('''
                SELECT open_time FROM kline_data_1min 
                WHERE symbol = %s AND open_time >= %s AND open_time < %s
                ORDER BY open_time
            ''', (symbol, today_start_ms, current_time_ms))
            
            results = cursor.fetchall()
            existing_times = set(row[0] for row in results)
            conn.close()
            
            # 生成应该存在的所有时间点
            expected_times = []
            current = self.today_start
            while current < self.current_time:
                expected_times.append(int(current.timestamp() * 1000))
                current += timedelta(minutes=1)
            
            # 找出缺失的连续时间段
            missing_periods = []
            missing_times = [ts for ts in expected_times if ts not in existing_times]
            
            if missing_times:
                # 将连续的缺失时间合并为时间段
                start_time = missing_times[0]
                end_time = missing_times[0]
                
                for i in range(1, len(missing_times)):
                    if missing_times[i] == missing_times[i-1] + 60000:  # 连续的分钟
                        end_time = missing_times[i]
                    else:
                        # 时间段结束，添加到列表
                        missing_periods.append((
                            datetime.fromtimestamp(start_time / 1000),
                            datetime.fromtimestamp(end_time / 1000)
                        ))
                        start_time = missing_times[i]
                        end_time = missing_times[i]
                
                # 添加最后一个时间段
                missing_periods.append((
                    datetime.fromtimestamp(start_time / 1000),
                    datetime.fromtimestamp(end_time / 1000)
                ))
            
            return missing_periods
            
        except Exception as e:
            logger.error(f"检查{symbol}的1分钟缺失数据失败: {str(e)}")
            return []
    
    def find_missing_30min_data(self, symbol: str) -> List[Tuple[datetime, datetime]]:
        """找出30分钟K线数据的缺失时间段"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # 获取30分钟K线时间戳
            analysis_start_ms = int(self.analysis_start.timestamp() * 1000)
            today_start_ms = int(self.today_start.timestamp() * 1000)
            
            cursor.execute('''
                SELECT open_time FROM kline_data_30min 
                WHERE symbol = %s AND open_time >= %s AND open_time < %s
                ORDER BY open_time
            ''', (symbol, analysis_start_ms, today_start_ms))
            
            results = cursor.fetchall()
            existing_times = set(row[0] for row in results)
            conn.close()
            
            # 生成应该存在的所有30分钟时间点
            expected_times = []
            current = self.analysis_start
            while current < self.today_start:
                if current.minute in [0, 30]:  # 只在每小时的0分和30分
                    expected_times.append(int(current.timestamp() * 1000))
                current += timedelta(minutes=30)
            
            # 找出缺失的连续时间段
            missing_periods = []
            missing_times = [ts for ts in expected_times if ts not in existing_times]
            
            if missing_times:
                # 将连续的缺失时间合并为时间段
                start_time = missing_times[0]
                end_time = missing_times[0]
                
                for i in range(1, len(missing_times)):
                    if missing_times[i] == missing_times[i-1] + 1800000:  # 连续的30分钟
                        end_time = missing_times[i]
                    else:
                        # 时间段结束，添加到列表
                        missing_periods.append((
                            datetime.fromtimestamp(start_time / 1000),
                            datetime.fromtimestamp(end_time / 1000)
                        ))
                        start_time = missing_times[i]
                        end_time = missing_times[i]
                
                # 添加最后一个时间段
                missing_periods.append((
                    datetime.fromtimestamp(start_time / 1000),
                    datetime.fromtimestamp(end_time / 1000)
                ))
            
            return missing_periods
            
        except Exception as e:
            logger.error(f"检查{symbol}的30分钟缺失数据失败: {str(e)}")
            return []
    
    def fetch_and_save_1min_data(self, symbol: str, start_time: datetime, end_time: datetime) -> bool:
        """获取并保存1分钟K线数据"""
        try:
            logger.info(f"获取{symbol}的1分钟K线数据: {start_time.strftime('%Y-%m-%d %H:%M')} 到 {end_time.strftime('%Y-%m-%d %H:%M')}")
            
            # 获取1分钟K线数据
            klines = self.client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_1MINUTE,
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000) + 60000,  # 包含结束时间
                limit=1500
            )
            
            if not klines:
                logger.warning(f"{symbol}: 未获取到1分钟K线数据")
                return False
            
            # 保存到数据库
            success = self.save_kline_data(symbol, klines, '1min')
            if success:
                logger.info(f"✅ 成功保存{symbol}的{len(klines)}条1分钟K线数据")
            
            return success
            
        except Exception as e:
            logger.error(f"获取{symbol}的1分钟K线数据失败: {str(e)}")
            return False
    
    def fetch_and_save_30min_data(self, symbol: str, start_time: datetime, end_time: datetime) -> bool:
        """获取并保存30分钟K线数据"""
        try:
            logger.info(f"获取{symbol}的30分钟K线数据: {start_time.strftime('%Y-%m-%d %H:%M')} 到 {end_time.strftime('%Y-%m-%d %H:%M')}")
            
            # 获取30分钟K线数据
            klines = self.client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_30MINUTE,
                startTime=int(start_time.timestamp() * 1000),
                endTime=int(end_time.timestamp() * 1000) + 1800000,  # 包含结束时间
                limit=1500
            )
            
            if not klines:
                logger.warning(f"{symbol}: 未获取到30分钟K线数据")
                return False
            
            # 保存到数据库
            success = self.save_kline_data(symbol, klines, '30min')
            if success:
                logger.info(f"✅ 成功保存{symbol}的{len(klines)}条30分钟K线数据")
            
            return success
            
        except Exception as e:
            logger.error(f"获取{symbol}的30分钟K线数据失败: {str(e)}")
            return False
    
    def save_kline_data(self, symbol: str, klines: List[List], interval: str = '1min') -> bool:
        """保存K线数据到数据库"""
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
    
    def get_all_symbols_in_db(self) -> List[str]:
        """获取数据库中所有交易对"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # 从两个表中获取所有交易对
            cursor.execute('SELECT DISTINCT symbol FROM kline_data_1min UNION SELECT DISTINCT symbol FROM kline_data_30min')
            results = cursor.fetchall()
            conn.close()
            
            symbols = [row[0] for row in results]
            logger.info(f"数据库中共有 {len(symbols)} 个交易对")
            return symbols
            
        except Exception as e:
            logger.error(f"获取数据库交易对失败: {str(e)}")
            return []
    
    async def repair_symbol_data(self, symbol: str, check_only: bool = False) -> Dict[str, Any]:
        """修复单个交易对的数据"""
        logger.info(f"{'检查' if check_only else '修复'} {symbol} 的K线数据...")
        
        result = {
            'symbol': symbol,
            '1min_missing_periods': 0,
            '30min_missing_periods': 0,
            '1min_repaired': 0,
            '30min_repaired': 0,
            'success': True
        }
        
        try:
            # 检查1分钟K线缺失
            missing_1min = self.find_missing_1min_data(symbol)
            result['1min_missing_periods'] = len(missing_1min)
            
            if missing_1min:
                logger.warning(f"发现{symbol}的{len(missing_1min)}个1分钟K线缺失时间段:")
                for start, end in missing_1min:
                    minutes_missing = int((end - start).total_seconds() / 60) + 1
                    logger.warning(f"  • {start.strftime('%Y-%m-%d %H:%M')} 到 {end.strftime('%Y-%m-%d %H:%M')} (共{minutes_missing}分钟)")
                    
                    if not check_only:
                        success = self.fetch_and_save_1min_data(symbol, start, end)
                        if success:
                            result['1min_repaired'] += 1
                        await asyncio.sleep(0.2)  # 避免API限制
            
            # 检查30分钟K线缺失
            missing_30min = self.find_missing_30min_data(symbol)
            result['30min_missing_periods'] = len(missing_30min)
            
            if missing_30min:
                logger.warning(f"发现{symbol}的{len(missing_30min)}个30分钟K线缺失时间段:")
                for start, end in missing_30min:
                    periods_missing = int((end - start).total_seconds() / 1800) + 1
                    logger.warning(f"  • {start.strftime('%Y-%m-%d %H:%M')} 到 {end.strftime('%Y-%m-%d %H:%M')} (共{periods_missing}个30分钟)")
                    
                    if not check_only:
                        success = self.fetch_and_save_30min_data(symbol, start, end)
                        if success:
                            result['30min_repaired'] += 1
                        await asyncio.sleep(0.2)  # 避免API限制
            
            if not missing_1min and not missing_30min:
                logger.info(f"✅ {symbol} 的K线数据完整，无需修复")
            
        except Exception as e:
            logger.error(f"处理{symbol}时发生错误: {str(e)}")
            result['success'] = False
            result['error'] = str(e)
        
        return result
    
    async def run_repair(self, target_symbol: str = None, check_only: bool = False, repair_all: bool = False):
        """运行数据修复"""
        action = "检查" if check_only else "修复"
        logger.info(f"🚀 开始{action}K线数据...")
        
        # 确定要处理的交易对
        if target_symbol:
            symbols_to_process = [target_symbol]
            logger.info(f"🎯 仅{action}指定交易对: {target_symbol}")
        elif repair_all or check_only:
            symbols_to_process = self.get_all_symbols_in_db()
            if not symbols_to_process:
                logger.error("❌ 数据库中没有找到任何K线数据")
                return
            logger.info(f"🔍 {action}所有 {len(symbols_to_process)} 个交易对")
        else:
            logger.error("❌ 请指定 --symbol, --check-only 或 --repair-all 参数")
            return
        
        # 处理统计
        total_symbols = len(symbols_to_process)
        processed_count = 0
        success_count = 0
        total_1min_missing = 0
        total_30min_missing = 0
        total_1min_repaired = 0
        total_30min_repaired = 0
        
        for i, symbol in enumerate(symbols_to_process, 1):
            logger.info(f"\n[{i}/{total_symbols}] 处理 {symbol}...")
            
            result = await self.repair_symbol_data(symbol, check_only=check_only)
            
            processed_count += 1
            if result['success']:
                success_count += 1
            
            total_1min_missing += result['1min_missing_periods']
            total_30min_missing += result['30min_missing_periods']
            total_1min_repaired += result['1min_repaired']
            total_30min_repaired += result['30min_repaired']
        
        # 打印汇总
        logger.info("\n" + "=" * 100)
        logger.info(f"📋 {action}完成汇总")
        logger.info("=" * 100)
        
        logger.info(f"总处理交易对数: {total_symbols}")
        logger.info(f"处理成功: {success_count} 个")
        logger.info(f"处理失败: {total_symbols - success_count} 个")
        logger.info(f"")
        logger.info(f"发现缺失:")
        logger.info(f"  📈 1分钟K线: {total_1min_missing} 个时间段")
        logger.info(f"  📊 30分钟K线: {total_30min_missing} 个时间段")
        
        if not check_only:
            logger.info(f"")
            logger.info(f"修复完成:")
            logger.info(f"  📈 1分钟K线: {total_1min_repaired} 个时间段")
            logger.info(f"  📊 30分钟K线: {total_30min_repaired} 个时间段")
        
        logger.info(f"✅ K线数据{action}完成!")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='K线数据修复工具')
    parser.add_argument(
        '--symbol',
        type=str,
        help='修复特定交易对（例如: BTCUSDT）'
    )
    parser.add_argument(
        '--check-only',
        action='store_true',
        help='仅检查缺失数据，不进行修复'
    )
    parser.add_argument(
        '--repair-all',
        action='store_true',
        help='修复所有交易对的缺失数据'
    )
    
    return parser.parse_args()


async def main():
    """主函数"""
    try:
        # 解析命令行参数
        args = parse_arguments()
        
        # 参数验证
        if not any([args.symbol, args.check_only, args.repair_all]):
            logger.error("❌ 请指定以下参数之一: --symbol, --check-only, --repair-all")
            return
        
        # 创建修复器
        repairer = KlineDataRepairer()
        
        # 运行修复
        await repairer.run_repair(
            target_symbol=args.symbol,
            check_only=args.check_only,
            repair_all=args.repair_all
        )
        
    except KeyboardInterrupt:
        logger.info("❌ 用户中断执行")
    except Exception as e:
        logger.error(f"❌ 执行过程中发生错误: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main()) 