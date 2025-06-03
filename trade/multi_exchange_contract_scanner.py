#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
多交易所合约扫描器

该脚本用于同时扫描多个交易所的合约交易对，找到符合以下条件的交易对：
1. 最近30天内价格波动小于10%
2. 资金费率一直为正或一直为负（即保持一个方向）
3. 最大杠杆大于等于20

支持的交易所：
- Binance
- Bitget
- Bybit
- GateIO
- OKX

主要功能：
1. 并行扫描多个交易所
2. 汇总所有交易所的结果
3. 生成统一的分析报告
4. 按年化收益率排序

作者: Claude
创建时间: 2024-12-30
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from tools.logger import logger
from config import project_root
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import concurrent.futures
import threading

# 导入Rich库用于美化表格输出
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

# 导入各个交易所的扫描器
from binance_contract_scanner import BinanceContractScanner
from bitget_contract_scanner import BitgetContractScanner
from bybit_contract_scanner import BybitContractScanner
from gateio_contract_scanner import GateIOContractScanner
from okx_contract_scanner import OKXContractScanner

# 设置日志级别
logger.setLevel(logging.INFO)

class MultiExchangeContractScanner:
    """多交易所合约扫描器"""
    
    def __init__(self, exchanges: List[str] = None):
        """
        初始化多交易所扫描器
        
        Args:
            exchanges: 要扫描的交易所列表，默认扫描所有支持的交易所
        """
        # 支持的交易所列表
        self.supported_exchanges = ['binance', 'bitget', 'bybit', 'gateio', 'okx']
        
        # 确定要扫描的交易所
        if exchanges is None:
            self.exchanges_to_scan = self.supported_exchanges.copy()
        else:
            self.exchanges_to_scan = [ex.lower() for ex in exchanges if ex.lower() in self.supported_exchanges]
        
        # 确保报告目录存在
        self.reports_dir = os.path.join(project_root, 'trade/reports')
        os.makedirs(self.reports_dir, exist_ok=True)
        
        # 生成报告文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.report_file = os.path.join(self.reports_dir, f'multi_exchange_contract_scan_{timestamp}.json')
        self.summary_file = os.path.join(self.reports_dir, f'multi_exchange_contract_summary_{timestamp}.txt')
        
        # 扫描参数
        self.price_volatility_threshold = 0.10  # 20%价格波动阈值
        self.min_leverage = 20  # 最小杠杆要求
        self.days_to_analyze = 30  # 分析天数
        
        # 线程锁，用于线程安全的日志输出
        self.lock = threading.Lock()
        
        logger.info(f"多交易所合约扫描器初始化完成")
        logger.info(f"将扫描以下交易所: {', '.join(self.exchanges_to_scan)}")
        logger.info(f"报告将保存到: {self.report_file}")
        logger.info(f"摘要将保存到: {self.summary_file}")

    def create_scanner(self, exchange: str):
        """
        创建指定交易所的扫描器实例
        
        Args:
            exchange: 交易所名称
            
        Returns:
            扫描器实例
        """
        try:
            if exchange == 'binance':
                return BinanceContractScanner(
                    price_volatility_threshold=self.price_volatility_threshold,
                    min_leverage=self.min_leverage,
                    days_to_analyze=self.days_to_analyze
                )
            elif exchange == 'bitget':
                return BitgetContractScanner(
                    price_volatility_threshold=self.price_volatility_threshold,
                    min_leverage=self.min_leverage,
                    days_to_analyze=self.days_to_analyze
                )
            elif exchange == 'bybit':
                return BybitContractScanner(
                    price_volatility_threshold=self.price_volatility_threshold,
                    min_leverage=self.min_leverage,
                    days_to_analyze=self.days_to_analyze
                )
            elif exchange == 'gateio':
                return GateIOContractScanner(
                    price_volatility_threshold=self.price_volatility_threshold,
                    min_leverage=self.min_leverage,
                    days_to_analyze=self.days_to_analyze
                )
            elif exchange == 'okx':
                return OKXContractScanner(
                    price_volatility_threshold=self.price_volatility_threshold,
                    min_leverage=self.min_leverage,
                    days_to_analyze=self.days_to_analyze
                )
            else:
                raise ValueError(f"不支持的交易所: {exchange}")
        except Exception as e:
            with self.lock:
                logger.error(f"创建{exchange}扫描器失败: {str(e)}")
            return None

    def scan_exchange(self, exchange: str) -> Dict[str, Any]:
        """
        扫描单个交易所
        
        Args:
            exchange: 交易所名称
            
        Returns:
            Dict: 扫描结果
        """
        start_time = datetime.now()
        
        with self.lock:
            logger.info(f"开始扫描{exchange.upper()}交易所...")
        
        try:
            # 创建扫描器
            scanner = self.create_scanner(exchange)
            if scanner is None:
                return {
                    'exchange': exchange.upper(),
                    'success': False,
                    'error': f"无法创建{exchange}扫描器",
                    'qualified_symbols': [],
                    'scan_time': 0
                }
            
            # 执行扫描
            qualified_symbols = scanner.scan_all_contracts()
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            with self.lock:
                logger.info(f"{exchange.upper()}扫描完成! 找到 {len(qualified_symbols)} 个符合条件的交易对，耗时: {duration}")
            
            return {
                'exchange': exchange.upper(),
                'success': True,
                'qualified_symbols': qualified_symbols,
                'scan_time': duration.total_seconds(),
                'total_symbols_scanned': len(scanner.get_all_futures_symbols()) if hasattr(scanner, 'get_all_futures_symbols') else 0
            }
            
        except Exception as e:
            end_time = datetime.now()
            duration = end_time - start_time
            
            with self.lock:
                logger.error(f"{exchange.upper()}扫描失败: {str(e)}")
            
            return {
                'exchange': exchange.upper(),
                'success': False,
                'error': str(e),
                'qualified_symbols': [],
                'scan_time': duration.total_seconds()
            }

    def scan_all_exchanges(self) -> List[Dict[str, Any]]:
        """
        并行扫描所有交易所
        
        Returns:
            List[Dict]: 所有交易所的扫描结果
        """
        logger.info("开始并行扫描所有交易所...")
        
        results = []
        
        # 使用线程池并行扫描
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.exchanges_to_scan)) as executor:
            # 提交所有扫描任务
            future_to_exchange = {
                executor.submit(self.scan_exchange, exchange): exchange 
                for exchange in self.exchanges_to_scan
            }
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_exchange):
                exchange = future_to_exchange[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"{exchange}扫描任务异常: {str(e)}")
                    results.append({
                        'exchange': exchange.upper(),
                        'success': False,
                        'error': f"扫描任务异常: {str(e)}",
                        'qualified_symbols': [],
                        'scan_time': 0
                    })
        
        return results

    def aggregate_results(self, exchange_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        汇总所有交易所的结果
        
        Args:
            exchange_results: 各交易所的扫描结果
            
        Returns:
            List[Dict]: 汇总后的符合条件的交易对列表
        """
        all_qualified_symbols = []
        
        for result in exchange_results:
            if result['success']:
                all_qualified_symbols.extend(result['qualified_symbols'])
        
        # 按年化收益率降序排序
        all_qualified_symbols.sort(
            key=lambda x: x.get('fundingRateAnalysis', {}).get('annualized_rate', 0), 
            reverse=True
        )
        
        return all_qualified_symbols

    def generate_comprehensive_report(self, exchange_results: List[Dict[str, Any]], all_qualified_symbols: List[Dict[str, Any]]):
        """
        生成综合分析报告
        
        Args:
            exchange_results: 各交易所的扫描结果
            all_qualified_symbols: 汇总后的符合条件的交易对列表
        """
        # 创建Rich控制台
        console = Console()
        
        # 保存详细的JSON报告
        report_data = {
            'scanDate': datetime.now().isoformat(),
            'scanParameters': {
                'priceVolatilityThreshold': self.price_volatility_threshold,
                'minLeverage': self.min_leverage,
                'daysAnalyzed': self.days_to_analyze
            },
            'exchangeResults': exchange_results,
            'totalQualified': len(all_qualified_symbols),
            'qualifiedSymbols': all_qualified_symbols
        }
        
        with open(self.report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        
        # 生成文本摘要
        summary_lines = [
            "=" * 100,
            "多交易所合约扫描综合报告",
            "=" * 100,
            f"扫描时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"扫描参数:",
            f"  - 价格波动率阈值: {self.price_volatility_threshold:.1%}",
            f"  - 最小杠杆要求: {self.min_leverage}x",
            f"  - 分析天数: {self.days_to_analyze}天",
            "",
            "各交易所扫描结果:",
            "-" * 100
        ]
        
        # 添加各交易所的扫描统计
        total_scan_time = 0
        total_symbols_scanned = 0
        successful_exchanges = 0
        
        for result in exchange_results:
            status = "✓ 成功" if result['success'] else "✗ 失败"
            scan_time = result.get('scan_time', 0)
            total_scan_time += scan_time
            
            if result['success']:
                successful_exchanges += 1
                qualified_count = len(result['qualified_symbols'])
                total_scanned = result.get('total_symbols_scanned', 0)
                total_symbols_scanned += total_scanned
                
                summary_lines.extend([
                    f"{result['exchange']:>8}: {status} | 符合条件: {qualified_count:>3} | 总扫描: {total_scanned:>3} | 耗时: {scan_time:.1f}s"
                ])
            else:
                error_msg = result.get('error', '未知错误')
                summary_lines.extend([
                    f"{result['exchange']:>8}: {status} | 错误: {error_msg}"
                ])
        
        summary_lines.extend([
            "-" * 100,
            f"扫描统计:",
            f"  - 成功扫描交易所: {successful_exchanges}/{len(exchange_results)}",
            f"  - 总扫描交易对数: {total_symbols_scanned}",
            f"  - 总符合条件数: {len(all_qualified_symbols)}",
            f"  - 总扫描耗时: {total_scan_time:.1f}秒",
            "",
            "=" * 100,
            ""
        ])
        
        # 使用Rich展示符合条件的交易对
        if all_qualified_symbols:
            # 创建控制台输出
            console.print("\n")
            console.print(Panel(
                Text("多交易所合约扫描综合报告", style="bold magenta", justify="center"),
                title="📊 扫描结果",
                border_style="bright_blue"
            ))
            
            # 显示扫描统计信息
            console.print(f"\n📈 扫描统计:")
            console.print(f"  ✅ 成功扫描交易所: {successful_exchanges}/{len(exchange_results)}")
            console.print(f"  🔍 总扫描交易对数: {total_symbols_scanned}")
            console.print(f"  ⭐ 总符合条件数: {len(all_qualified_symbols)}")
            console.print(f"  ⏱️  总扫描耗时: {total_scan_time:.1f}秒")
            
            # 创建符合条件交易对的表格
            table = Table(
                title=f"🎯 符合条件的交易对详情 (按年化收益率排序) - 共{len(all_qualified_symbols)}个",
                show_header=True,
                header_style="bold blue",
                border_style="bright_green",
                # row_styles=["", "dim"]
            )
            
            # 添加表格列
            table.add_column("排名", justify="center", style="bold", width=4)
            table.add_column("交易所", justify="center", style="cyan", width=8)
            table.add_column("交易对", justify="center", style="bold yellow", width=15)
            # table.add_column("基础资产", justify="center", style="green", width=10)
            table.add_column("当前价格", justify="right", style="white", width=12)
            table.add_column("最高价", justify="right", style="bright_red", width=12)
            table.add_column("最低价", justify="right", style="bright_green", width=12)
            table.add_column("最大杠杆", justify="center", style="magenta", width=8)
            table.add_column("价格波动", justify="center", style="blue", width=8)
            table.add_column("费率方向", justify="center", style="bold", width=8)
            table.add_column("结算周期", justify="center", style="yellow", width=8)
            table.add_column("平均费率", justify="right", style="white", width=10)
            table.add_column("年化收益", justify="right", style="bold green", width=10)
            table.add_column("一致性", justify="center", style="cyan", width=20)
            
            # 添加数据行
            for i, symbol_data in enumerate(all_qualified_symbols, 1):
                funding_analysis = symbol_data['fundingRateAnalysis']
                funding_interval = symbol_data.get('fundingIntervalHours', 8.0)
                exchange_name = symbol_data.get('exchange', 'Unknown')
                
                # 根据资金费率方向设置颜色
                direction_color = "green" if funding_analysis['direction'] == "正向" else "red"
                direction_text = f"[{direction_color}]{funding_analysis['direction']}[/{direction_color}]"
                
                # 年化收益率颜色
                annualized_rate = funding_analysis['annualized_rate']
                rate_color = "bright_green" if annualized_rate > 10 else "green" if annualized_rate > 5 else "yellow"
                rate_text = f"[{rate_color}]{annualized_rate:.2f}%[/{rate_color}]"
                
                # 一致性显示
                consistency = f"{funding_analysis['positive_ratio']:.1%} 正 / {funding_analysis['negative_ratio']:.1%} 负"
                
                table.add_row(
                    str(i),
                    exchange_name,
                    symbol_data['symbol'],
                    # symbol_data['baseAsset'],
                    f"${symbol_data['currentPrice']:.6f}",
                    f"${symbol_data['priceRange']['max']:.6f}",
                    f"${symbol_data['priceRange']['min']:.6f}",
                    f"{symbol_data['maxLeverage']}x",
                    f"{symbol_data['priceVolatility']:.2%}",
                    direction_text,
                    f"{funding_interval}h",
                    f"{funding_analysis['avg_rate']:.6f}",
                    rate_text,
                    consistency
                )
            
            console.print("\n")
            console.print(table)
            
            # 添加底部信息
            console.print(f"\n💾 详细报告已保存到: {self.report_file}")
            console.print(f"📝 摘要报告已保存到: {self.summary_file}")
            
        else:
            summary_lines.append("未找到符合条件的交易对")
            console.print("\n❌ 未找到符合条件的交易对")
        
        summary_lines.extend([
            "=" * 100,
            f"详细报告已保存到: {self.report_file}",
            "=" * 100
        ])
        
        summary_text = "\n".join(summary_lines)
        
        # 保存摘要文件
        with open(self.summary_file, 'w', encoding='utf-8') as f:
            f.write(summary_text)
        
        logger.info(f"综合报告已生成:")
        logger.info(f"  详细报告: {self.report_file}")
        logger.info(f"  摘要报告: {self.summary_file}")

    def run(self):
        """
        运行多交易所扫描器
        """
        try:
            start_time = datetime.now()
            logger.info("=" * 80)
            logger.info("多交易所合约扫描器启动")
            logger.info("=" * 80)
            
            # 并行扫描所有交易所
            exchange_results = self.scan_all_exchanges()
            
            # 汇总结果
            all_qualified_symbols = self.aggregate_results(exchange_results)
            
            # 生成综合报告
            self.generate_comprehensive_report(exchange_results, all_qualified_symbols)
            
            end_time = datetime.now()
            total_duration = end_time - start_time
            logger.info(f"多交易所扫描完成，总耗时: {total_duration}")
            
        except KeyboardInterrupt:
            logger.info("用户中断扫描")
        except Exception as e:
            logger.error(f"扫描过程中发生错误: {str(e)}")
            raise


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='多交易所合约扫描器')
    parser.add_argument('--exchanges', nargs='+', 
                       choices=['binance', 'bitget', 'bybit', 'gateio', 'okx'],
                       help='指定要扫描的交易所，默认扫描所有交易所')
    
    args = parser.parse_args()
    
    try:
        # 创建扫描器实例
        scanner = MultiExchangeContractScanner(exchanges=args.exchanges)
        
        # 运行扫描
        scanner.run()
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 