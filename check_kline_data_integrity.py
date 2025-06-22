#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
K线数据完整性检查器

该脚本用于检查数据库中K线数据的完整性：
1. 检查1分钟级别K线（当天）数据是否有缺失
2. 检查30分钟级别K线（当天往前29天）数据是否有缺失
3. 打印出每个合约交易对的时间范围统计

使用方法：
python check_kline_data_integrity.py
python check_kline_data_integrity.py --detailed  # 显示详细缺失信息
python check_kline_data_integrity.py --symbol BTCUSDT  # 检查特定交易对

作者: Assistant
创建时间: 2024-12-30
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Set
from tools.logger import logger
from config import mysql_config
import pandas as pd


class KlineDataIntegrityChecker:
    """K线数据完整性检查器"""
    
    def __init__(self):
        """初始化检查器"""
        self.mysql_config = mysql_config
        self.current_time = datetime.now()
        # 排除最近15分钟的数据，因为可能还没有更新到数据库
        self.check_end_time = self.current_time - timedelta(minutes=15)
        self.today_start = self.current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        self.analysis_start = self.today_start - timedelta(days=29)  # 30天前
        
        logger.info(f"K线数据完整性检查器初始化完成")
        logger.info(f"检查时间范围: {self.analysis_start.strftime('%Y-%m-%d %H:%M:%S')} 到 {self.check_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"当天开始时间: {self.today_start.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⚠️  排除最近15分钟数据，检查截止时间: {self.check_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    def get_all_symbols_in_db(self) -> Dict[str, Dict]:
        """获取数据库中所有交易对及其数据统计"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            symbols_info = {}
            
            # 获取1分钟K线表中的交易对统计
            cursor.execute('''
                SELECT symbol, 
                       COUNT(*) as count,
                       MIN(open_time) as min_time,
                       MAX(open_time) as max_time
                FROM kline_data_1min 
                GROUP BY symbol
            ''')
            
            results_1min = cursor.fetchall()
            
            for row in results_1min:
                symbol, count, min_time, max_time = row
                symbols_info[symbol] = {
                    '1min': {
                        'count': count,
                        'min_time': min_time,
                        'max_time': max_time,
                        'min_datetime': datetime.fromtimestamp(min_time / 1000),
                        'max_datetime': datetime.fromtimestamp(max_time / 1000)
                    },
                    '30min': {
                        'count': 0,
                        'min_time': None,
                        'max_time': None,
                        'min_datetime': None,
                        'max_datetime': None
                    }
                }
            
            # 获取30分钟K线表中的交易对统计
            cursor.execute('''
                SELECT symbol, 
                       COUNT(*) as count,
                       MIN(open_time) as min_time,
                       MAX(open_time) as max_time
                FROM kline_data_30min 
                GROUP BY symbol
            ''')
            
            results_30min = cursor.fetchall()
            
            for row in results_30min:
                symbol, count, min_time, max_time = row
                if symbol not in symbols_info:
                    symbols_info[symbol] = {
                        '1min': {
                            'count': 0,
                            'min_time': None,
                            'max_time': None,
                            'min_datetime': None,
                            'max_datetime': None
                        },
                        '30min': {
                            'count': count,
                            'min_time': min_time,
                            'max_time': max_time,
                            'min_datetime': datetime.fromtimestamp(min_time / 1000),
                            'max_datetime': datetime.fromtimestamp(max_time / 1000)
                        }
                    }
                else:
                    symbols_info[symbol]['30min'] = {
                        'count': count,
                        'min_time': min_time,
                        'max_time': max_time,
                        'min_datetime': datetime.fromtimestamp(min_time / 1000),
                        'max_datetime': datetime.fromtimestamp(max_time / 1000)
                    }
            
            conn.close()
            
            logger.info(f"数据库中共有 {len(symbols_info)} 个交易对的K线数据")
            return symbols_info
            
        except Exception as e:
            logger.error(f"获取数据库交易对信息失败: {str(e)}")
            return {}
    
    def check_1min_data_integrity(self, symbol: str) -> Dict[str, Any]:
        """检查1分钟K线数据完整性（当天数据）"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # 计算时间范围
            today_start_ms = int(self.today_start.timestamp() * 1000)
            check_end_time_ms = int(self.check_end_time.timestamp() * 1000)
            
            # 获取当天的1分钟K线数据
            cursor.execute('''
                SELECT open_time FROM kline_data_1min 
                WHERE symbol = %s AND open_time >= %s AND open_time < %s
                ORDER BY open_time
            ''', (symbol, today_start_ms, check_end_time_ms))
            
            results = cursor.fetchall()
            actual_count = len(results)
            
            # 检查缺失的时间点（基于实际数据范围）
            missing_times = []
            expected_count = 0
            
            if results and len(results) > 0:
                existing_times = set(row[0] for row in results)
                
                # 获取该交易对实际的数据时间范围
                first_time_ms = results[0][0]
                last_time_ms = results[-1][0]
                
                first_time = datetime.fromtimestamp(first_time_ms / 1000)
                last_time = datetime.fromtimestamp(last_time_ms / 1000)
                
                # 生成从第一条数据到最后一条数据之间应该存在的所有时间点
                expected_times = set()
                current = first_time
                while current <= last_time:
                    expected_times.add(int(current.timestamp() * 1000))
                    current += timedelta(minutes=1)
                
                expected_count = len(expected_times)
                
                # 找出缺失的时间点（只检查中间缺失，不检查开头和结尾）
                missing_time_stamps = expected_times - existing_times
                missing_times = [datetime.fromtimestamp(ts / 1000) for ts in sorted(missing_time_stamps)]
            else:
                # 如果当天有时间范围但没有数据，计算预期数量
                if self.check_end_time > self.today_start:
                    expected_count = int((self.check_end_time - self.today_start).total_seconds() / 60)
            
            conn.close()
            
            integrity_result = {
                'symbol': symbol,
                'expected_count': expected_count,
                'actual_count': actual_count,
                'missing_count': expected_count - actual_count,
                'integrity_rate': (actual_count / expected_count * 100) if expected_count > 0 else 0,
                'missing_times': missing_times[:10] if len(missing_times) > 10 else missing_times,  # 最多显示10个
                'total_missing_times': len(missing_times),
                'has_data': actual_count > 0
            }
            
            return integrity_result
            
        except Exception as e:
            logger.error(f"检查{symbol}的1分钟K线完整性失败: {str(e)}")
            return {
                'symbol': symbol,
                'expected_count': 0,
                'actual_count': 0,
                'missing_count': 0,
                'integrity_rate': 0,
                'missing_times': [],
                'total_missing_times': 0,
                'has_data': False,
                'error': str(e)
            }
    
    def check_30min_data_integrity(self, symbol: str) -> Dict[str, Any]:
        """检查30分钟K线数据完整性（基于实际数据范围）"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # 计算时间范围（30天前到今天00:00）
            analysis_start_ms = int(self.analysis_start.timestamp() * 1000)
            today_start_ms = int(self.today_start.timestamp() * 1000)
            
            # 获取30分钟K线数据
            cursor.execute('''
                SELECT open_time FROM kline_data_30min 
                WHERE symbol = %s AND open_time >= %s AND open_time < %s
                ORDER BY open_time
            ''', (symbol, analysis_start_ms, today_start_ms))
            
            results = cursor.fetchall()
            actual_count = len(results)
            
            # 检查缺失的时间点（基于实际数据范围）
            missing_times = []
            expected_count = 0
            
            if results and len(results) > 0:
                existing_times = set(row[0] for row in results)
                
                # 获取该交易对实际的数据时间范围
                first_time_ms = results[0][0]
                last_time_ms = results[-1][0]
                
                first_time = datetime.fromtimestamp(first_time_ms / 1000)
                last_time = datetime.fromtimestamp(last_time_ms / 1000)
                
                # 生成从第一条数据到最后一条数据之间应该存在的所有30分钟时间点
                expected_times = set()
                current = first_time
                while current <= last_time:
                    # 只在每小时的0分和30分生成时间点
                    if current.minute in [0, 30]:
                        expected_times.add(int(current.timestamp() * 1000))
                    current += timedelta(minutes=30)
                
                expected_count = len(expected_times)
                
                # 找出缺失的时间点（只检查中间缺失，不检查开头和结尾）
                missing_time_stamps = expected_times - existing_times
                missing_times = [datetime.fromtimestamp(ts / 1000) for ts in sorted(missing_time_stamps)]
            else:
                # 如果没有数据，不计算预期数量（可能是新上市的交易对）
                expected_count = 0
            
            conn.close()
            
            integrity_result = {
                'symbol': symbol,
                'expected_count': expected_count,
                'actual_count': actual_count,
                'missing_count': expected_count - actual_count,
                'integrity_rate': (actual_count / expected_count * 100) if expected_count > 0 else 0,
                'missing_times': missing_times[:10] if len(missing_times) > 10 else missing_times,  # 最多显示10个
                'total_missing_times': len(missing_times),
                'has_data': actual_count > 0
            }
            
            return integrity_result
            
        except Exception as e:
            logger.error(f"检查{symbol}的30分钟K线完整性失败: {str(e)}")
            return {
                'symbol': symbol,
                'expected_count': 0,
                'actual_count': 0,
                'missing_count': 0,
                'integrity_rate': 0,
                'missing_times': [],
                'total_missing_times': 0,
                'has_data': False,
                'error': str(e)
            }
    
    def print_symbol_time_ranges(self, symbols_info: Dict[str, Dict]):
        """打印每个交易对的时间范围统计"""
        logger.info("=" * 100)
        logger.info("📊 交易对时间范围统计")
        logger.info("=" * 100)
        
        # 按交易对名称排序
        sorted_symbols = sorted(symbols_info.keys())
        
        for symbol in sorted_symbols:
            info = symbols_info[symbol]
            
            logger.info(f"\n🔸 {symbol}:")
            
            # 1分钟K线统计
            if info['1min']['count'] > 0:
                logger.info(f"  📈 1分钟K线: {info['1min']['count']:,} 条")
                logger.info(f"     时间范围: {info['1min']['min_datetime'].strftime('%Y-%m-%d %H:%M:%S')} "
                           f"至 {info['1min']['max_datetime'].strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"     时间跨度: {(info['1min']['max_datetime'] - info['1min']['min_datetime']).total_seconds() / 3600:.1f} 小时")
            else:
                logger.info(f"  📈 1分钟K线: 无数据")
            
            # 30分钟K线统计
            if info['30min']['count'] > 0:
                logger.info(f"  📊 30分钟K线: {info['30min']['count']:,} 条")
                logger.info(f"     时间范围: {info['30min']['min_datetime'].strftime('%Y-%m-%d %H:%M:%S')} "
                           f"至 {info['30min']['max_datetime'].strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"     时间跨度: {(info['30min']['max_datetime'] - info['30min']['min_datetime']).total_seconds() / (24 * 3600):.1f} 天")
            else:
                logger.info(f"  📊 30分钟K线: 无数据")
    
    def print_integrity_summary(self, symbols_to_check: List[str], detailed: bool = False):
        """打印数据完整性检查汇总"""
        logger.info("=" * 100)
        logger.info("🔍 K线数据完整性检查报告")
        logger.info("=" * 100)
        
        total_symbols = len(symbols_to_check)
        complete_1min_count = 0
        complete_30min_count = 0
        
        integrity_issues = {
            '1min': [],
            '30min': []
        }
        
        for i, symbol in enumerate(symbols_to_check, 1):
            logger.info(f"\n[{i}/{total_symbols}] 检查 {symbol}...")
            
            # 检查1分钟K线完整性
            integrity_1min = self.check_1min_data_integrity(symbol)
            
            # 检查30分钟K线完整性
            integrity_30min = self.check_30min_data_integrity(symbol)
            
            # 统计完整性
            if integrity_1min['integrity_rate'] >= 95:
                complete_1min_count += 1
            else:
                integrity_issues['1min'].append(integrity_1min)
            
            if integrity_30min['integrity_rate'] >= 95:
                complete_30min_count += 1
            else:
                integrity_issues['30min'].append(integrity_30min)
            
            # 打印结果
            logger.info(f"  📈 1分钟K线: {integrity_1min['actual_count']:,}/{integrity_1min['expected_count']:,} "
                       f"({integrity_1min['integrity_rate']:.1f}%)")
            
            if integrity_1min['missing_count'] > 0:
                logger.warning(f"     ⚠️  缺失 {integrity_1min['missing_count']:,} 条数据")
                if detailed and integrity_1min['missing_times']:
                    logger.info(f"     🕐 部分缺失时间: {', '.join([t.strftime('%H:%M') for t in integrity_1min['missing_times']])}")
                    if integrity_1min['total_missing_times'] > len(integrity_1min['missing_times']):
                        logger.info(f"     📋 总共缺失 {integrity_1min['total_missing_times']} 个时间点")
            
            logger.info(f"  📊 30分钟K线: {integrity_30min['actual_count']:,}/{integrity_30min['expected_count']:,} "
                       f"({integrity_30min['integrity_rate']:.1f}%)")
            
            if integrity_30min['missing_count'] > 0:
                logger.warning(f"     ⚠️  缺失 {integrity_30min['missing_count']:,} 条数据")
                if detailed and integrity_30min['missing_times']:
                    logger.info(f"     🕐 部分缺失时间: {', '.join([t.strftime('%m-%d %H:%M') for t in integrity_30min['missing_times']])}")
                    if integrity_30min['total_missing_times'] > len(integrity_30min['missing_times']):
                        logger.info(f"     📋 总共缺失 {integrity_30min['total_missing_times']} 个时间点")
        
        # 打印汇总统计
        logger.info("\n" + "=" * 100)
        logger.info("📋 完整性检查汇总")
        logger.info("=" * 100)
        
        logger.info(f"总检查交易对数: {total_symbols}")
        logger.info(f"")
        logger.info(f"📈 1分钟K线:")
        logger.info(f"  ✅ 完整率≥95%: {complete_1min_count} 个 ({complete_1min_count/total_symbols*100:.1f}%)")
        logger.info(f"  ⚠️  有缺失数据: {len(integrity_issues['1min'])} 个 ({len(integrity_issues['1min'])/total_symbols*100:.1f}%)")
        
        logger.info(f"")
        logger.info(f"📊 30分钟K线:")
        logger.info(f"  ✅ 完整率≥95%: {complete_30min_count} 个 ({complete_30min_count/total_symbols*100:.1f}%)")
        logger.info(f"  ⚠️  有缺失数据: {len(integrity_issues['30min'])} 个 ({len(integrity_issues['30min'])/total_symbols*100:.1f}%)")
        
        # 如果有问题，列出问题最严重的交易对
        if integrity_issues['1min']:
            logger.info(f"\n🚨 1分钟K线数据缺失最严重的交易对:")
            sorted_issues = sorted(integrity_issues['1min'], key=lambda x: x['integrity_rate'])
            for issue in sorted_issues[:5]:  # 显示前5个
                logger.warning(f"  • {issue['symbol']}: {issue['integrity_rate']:.1f}% "
                             f"(缺失 {issue['missing_count']:,} 条)")
        
        if integrity_issues['30min']:
            logger.info(f"\n🚨 30分钟K线数据缺失最严重的交易对:")
            sorted_issues = sorted(integrity_issues['30min'], key=lambda x: x['integrity_rate'])
            for issue in sorted_issues[:5]:  # 显示前5个
                logger.warning(f"  • {issue['symbol']}: {issue['integrity_rate']:.1f}% "
                             f"(缺失 {issue['missing_count']:,} 条)")
    
    def run_check(self, target_symbol: str = None, detailed: bool = False):
        """运行完整性检查"""
        logger.info("🚀 开始K线数据完整性检查...")
        
        # 获取数据库中所有交易对信息
        symbols_info = self.get_all_symbols_in_db()
        
        if not symbols_info:
            logger.error("❌ 数据库中没有找到任何K线数据")
            return
        
        # 打印时间范围统计
        logger.info("📊 正在生成时间范围统计...")
        self.print_symbol_time_ranges(symbols_info)
        
        # 确定要检查的交易对
        if target_symbol:
            if target_symbol in symbols_info:
                symbols_to_check = [target_symbol]
                logger.info(f"🎯 仅检查指定交易对: {target_symbol}")
            else:
                logger.error(f"❌ 数据库中未找到交易对: {target_symbol}")
                return
        else:
            symbols_to_check = list(symbols_info.keys())
            logger.info(f"🔍 检查所有 {len(symbols_to_check)} 个交易对")
        
        # 执行完整性检查
        logger.info("🔍 正在进行数据完整性检查...")
        self.print_integrity_summary(symbols_to_check, detailed=detailed)
        
        logger.info("✅ K线数据完整性检查完成!")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='K线数据完整性检查器')
    parser.add_argument(
        '--symbol',
        type=str,
        help='检查特定交易对（例如: BTCUSDT）'
    )
    parser.add_argument(
        '--detailed',
        action='store_true',
        help='显示详细的缺失时间信息'
    )
    
    return parser.parse_args()


def main():
    """主函数"""
    try:
        # 解析命令行参数
        args = parse_arguments()
        
        # 创建检查器
        checker = KlineDataIntegrityChecker()
        
        # 运行检查
        checker.run_check(target_symbol=args.symbol, detailed=args.detailed)
        
    except KeyboardInterrupt:
        logger.info("❌ 用户中断执行")
    except Exception as e:
        logger.error(f"❌ 执行过程中发生错误: {str(e)}")


if __name__ == "__main__":
    main() 