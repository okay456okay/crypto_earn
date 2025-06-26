#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binance账户未成交订单查询脚本

此脚本用于获取和显示Binance交易所账户的所有未成交订单信息，包括：
1. 现货交易未成交订单
2. 合约交易未成交订单
3. 订单详细信息（价格、数量、方向、时间等）
4. 美化的表格输出格式

作者：加密货币套利专家
创建时间：2024-12-30
版本：1.0.0
"""

import sys
import os
import asyncio
import traceback
from datetime import datetime
from decimal import Decimal
import ccxt

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import binance_api_key, binance_api_secret, proxies
    from tools.logger import logger
except ImportError:
    print("错误：无法导入配置文件，请确保config.py存在并包含Binance API密钥")
    sys.exit(1)

try:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    from rich import box
    from rich.align import Align
    RICH_AVAILABLE = True
except ImportError:
    print("提示：安装rich库可获得更好的显示效果: pip install rich")
    RICH_AVAILABLE = False

class BinanceOpenOrdersFetcher:
    """Binance未成交订单获取器"""
    
    def __init__(self):
        """初始化Binance交易所连接"""
        # 检查API配置
        if not binance_api_key or not binance_api_secret:
            raise ValueError("Binance API密钥未配置，请在config.py中设置binance_api_key和binance_api_secret")
        
        # 初始化现货交易所连接
        self.spot_exchange = ccxt.binance({
            'apiKey': binance_api_key,
            'secret': binance_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'options': {
                'defaultType': 'spot',  # 现货交易
                'warnOnFetchOpenOrdersWithoutSymbol': False,  # 允许获取所有未成交订单
            }
        })
        
        # 初始化合约交易所连接
        self.futures_exchange = ccxt.binance({
            'apiKey': binance_api_key,
            'secret': binance_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'options': {
                'defaultType': 'future',  # 合约交易
                'warnOnFetchOpenOrdersWithoutSymbol': False,  # 允许获取所有未成交订单
            }
        })
        
        # 初始化Rich控制台（如果可用）
        if RICH_AVAILABLE:
            self.console = Console(width=150, force_terminal=True)
        
        logger.info("Binance未成交订单获取器初始化完成")
    
    def test_connection(self):
        """测试API连接"""
        try:
            # 测试现货连接
            self.spot_exchange.load_markets()
            logger.info("现货API连接测试成功")
            
            # 测试合约连接
            self.futures_exchange.load_markets()
            logger.info("合约API连接测试成功")
            
            return True
        except Exception as e:
            logger.error(f"API连接测试失败: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def get_spot_open_orders(self):
        """获取现货未成交订单"""
        try:
            logger.info("正在获取现货未成交订单...")
            open_orders = self.spot_exchange.fetch_open_orders()
            logger.info(f"获取到 {len(open_orders)} 个现货未成交订单")
            return open_orders
        except Exception as e:
            logger.error(f"获取现货未成交订单失败: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def get_futures_open_orders(self):
        """获取合约未成交订单"""
        try:
            logger.info("正在获取合约未成交订单...")
            open_orders = self.futures_exchange.fetch_open_orders()
            logger.info(f"获取到 {len(open_orders)} 个合约未成交订单")
            return open_orders
        except Exception as e:
            logger.error(f"获取合约未成交订单失败: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def format_timestamp(self, timestamp):
        """格式化时间戳"""
        if timestamp:
            dt = datetime.fromtimestamp(timestamp / 1000)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return "未知"
    
    def format_amount(self, amount):
        """格式化数量"""
        if amount is None:
            return "N/A"
        return f"{amount:.8f}".rstrip('0').rstrip('.')
    
    def format_price(self, price):
        """格式化价格"""
        if price is None:
            return "N/A"
        return f"{price:.8f}".rstrip('0').rstrip('.')
    
    def display_orders_rich(self, orders, order_type):
        """使用Rich库美化显示订单信息"""
        if not RICH_AVAILABLE:
            self.display_orders_simple(orders, order_type)
            return
        
        if not orders:
            self.console.print(f"\n[yellow]🔍 {order_type}未成交订单: 无[/yellow]")
            return
        
        # 创建表格
        table = Table(
            title=f"📋 {order_type}未成交订单 ({len(orders)}个)",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta",
            title_style="bold blue"
        )
        
        # 添加列
        table.add_column("交易对", style="cyan", no_wrap=True)
        table.add_column("方向", style="green", no_wrap=True)
        table.add_column("类型", style="yellow", no_wrap=True)
        table.add_column("数量", style="blue", justify="right")
        table.add_column("价格", style="blue", justify="right")
        table.add_column("总价值", style="magenta", justify="right")
        table.add_column("状态", style="bright_yellow", no_wrap=True)
        table.add_column("创建时间", style="dim", no_wrap=True)
        table.add_column("订单ID", style="dim", no_wrap=True)
        
        # 添加数据行
        for order in orders:
            # 计算总价值
            total_value = "N/A"
            if order.get('amount') and order.get('price'):
                total_value = f"{order['amount'] * order['price']:.2f}"
            
            # 根据订单方向设置颜色
            side_color = "green" if order.get('side') == 'buy' else "red"
            side_text = f"[{side_color}]{order.get('side', 'N/A').upper()}[/{side_color}]"
            
            table.add_row(
                order.get('symbol', 'N/A'),
                side_text,
                order.get('type', 'N/A').upper(),
                self.format_amount(order.get('amount')),
                self.format_price(order.get('price')),
                total_value,
                order.get('status', 'N/A'),
                self.format_timestamp(order.get('timestamp')),
                str(order.get('id', 'N/A'))[:12] + "..."
            )
        
        # 显示表格
        self.console.print("\n")
        self.console.print(Align.center(table))
    
    def display_orders_simple(self, orders, order_type):
        """简单格式显示订单信息"""
        print(f"\n{'='*80}")
        print(f"{order_type}未成交订单 ({len(orders)}个)")
        print(f"{'='*80}")
        
        if not orders:
            print("无未成交订单")
            return
        
        for i, order in enumerate(orders, 1):
            print(f"\n订单 #{i}:")
            print(f"  交易对: {order.get('symbol', 'N/A')}")
            print(f"  方向: {order.get('side', 'N/A').upper()}")
            print(f"  类型: {order.get('type', 'N/A').upper()}")
            print(f"  数量: {self.format_amount(order.get('amount'))}")
            print(f"  价格: {self.format_price(order.get('price'))}")
            if order.get('amount') and order.get('price'):
                total_value = order['amount'] * order['price']
                print(f"  总价值: {total_value:.2f}")
            print(f"  状态: {order.get('status', 'N/A')}")
            print(f"  创建时间: {self.format_timestamp(order.get('timestamp'))}")
            print(f"  订单ID: {order.get('id', 'N/A')}")
    
    def display_summary(self, spot_orders, futures_orders):
        """显示订单汇总信息"""
        total_orders = len(spot_orders) + len(futures_orders)
        
        if RICH_AVAILABLE:
            # 创建汇总表格
            summary_table = Table(
                title="📊 订单汇总",
                box=box.DOUBLE,
                show_header=True,
                header_style="bold cyan"
            )
            
            summary_table.add_column("类型", style="yellow", no_wrap=True)
            summary_table.add_column("数量", style="blue", justify="center")
            summary_table.add_column("状态", style="green", justify="center")
            
            summary_table.add_row("现货订单", str(len(spot_orders)), "✅ 已获取" if spot_orders is not None else "❌ 获取失败")
            summary_table.add_row("合约订单", str(len(futures_orders)), "✅ 已获取" if futures_orders is not None else "❌ 获取失败")
            summary_table.add_row("总计", str(total_orders), "📈 活跃订单")
            
            self.console.print("\n")
            self.console.print(Align.center(summary_table))
            
            # 添加提示信息
            if total_orders == 0:
                self.console.print("\n[green]✨ 恭喜！您当前没有未成交的订单[/green]")
            else:
                self.console.print(f"\n[yellow]⚠️  您当前有 {total_orders} 个未成交订单需要关注[/yellow]")
                
        else:
            print(f"\n{'='*50}")
            print("订单汇总:")
            print(f"  现货订单: {len(spot_orders)}")
            print(f"  合约订单: {len(futures_orders)}")
            print(f"  总计: {total_orders}")
            print(f"{'='*50}")
    
    def run(self):
        """运行主程序"""
        try:
            if RICH_AVAILABLE:
                self.console.print("[bold blue]🚀 Binance未成交订单查询工具[/bold blue]")
                self.console.print("[dim]正在连接Binance API...[/dim]")
            else:
                print("🚀 Binance未成交订单查询工具")
                print("正在连接Binance API...")
            
            # 测试连接
            if not self.test_connection():
                logger.error("API连接失败，程序退出")
                return False
            
            # 获取现货未成交订单
            spot_orders = self.get_spot_open_orders()
            
            # 获取合约未成交订单
            futures_orders = self.get_futures_open_orders()
            
            # 显示订单信息
            self.display_orders_rich(spot_orders, "现货")
            self.display_orders_rich(futures_orders, "合约")
            
            # 显示汇总信息
            self.display_summary(spot_orders, futures_orders)
            
            if RICH_AVAILABLE:
                self.console.print("\n[green]✅ 查询完成！[/green]")
            else:
                print("\n✅ 查询完成！")
            
            return True
            
        except KeyboardInterrupt:
            logger.info("用户中断程序")
            return False
        except Exception as e:
            logger.error(f"程序执行失败: {e}")
            logger.error(traceback.format_exc())
            return False

def main():
    """主函数"""
    try:
        # 创建订单获取器
        fetcher = BinanceOpenOrdersFetcher()
        
        # 运行查询
        success = fetcher.run()
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"程序启动失败: {e}")
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 