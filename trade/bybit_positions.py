#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Bybit合约持仓信息查询脚本

此脚本用于获取和显示Bybit交易所的合约持仓信息，包括：
1. 所有合约的持仓情况
2. 持仓方向、数量、杠杆倍数
3. 未实现盈亏和已实现盈亏
4. 资金费率和结算信息
"""

import sys
import os
import asyncio
import ccxt.pro as ccxtpro
from decimal import Decimal
from datetime import datetime
import subprocess
from rich.console import Console
from rich.table import Table
from rich import box

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import bybit_api_key, bybit_api_secret, proxies, fundingrate_auto_skip
from tools.proxy import get_proxy_ip
from high_yield.exchange import ExchangeAPI

import logging
from tools.logger import logger
logger.setLevel(logging.ERROR)

# 初始化rich console
console = Console(width=200, force_terminal=True)

class BybitPositionFetcher:
    def __init__(self):
        """初始化Bybit交易所连接"""
        self.exchange = ccxtpro.bybit({
            'apiKey': bybit_api_key,
            'secret': bybit_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
            'options': {
                'defaultType': 'linear',  # 使用USDT永续合约
            }
        })
        self.exchange_api = ExchangeAPI()

    def run_funding_script(self, token):
        """运行资金费率脚本"""
        try:
            script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts', 'funding_bybit.sh')
            subprocess.run([script_path, token], check=True)
            logger.info(f"已执行资金费率脚本: {token}")
        except subprocess.CalledProcessError as e:
            logger.error(f"执行资金费率脚本失败: {token}, 错误: {str(e)}")
        except Exception as e:
            logger.error(f"执行资金费率脚本时发生错误: {token}, 错误: {str(e)}")

    async def fetch_positions(self):
        """获取所有合约持仓信息"""
        positions = []
        try:
            # 获取所有持仓信息
            positions = await self.exchange.fetch_positions()

            if not positions:
                console.print("当前没有持仓")
                return positions

            # 打印持仓信息
            console.print("\n=== Bybit合约持仓信息 ===")
            console.print(f"查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 创建持仓信息表格
            position_table = Table(box=box.ROUNDED)
            position_table.add_column("交易对", justify="left", style="cyan")
            position_table.add_column("方向", justify="center")
            position_table.add_column("数量", justify="right")
            position_table.add_column("杠杆", justify="right")
            position_table.add_column("资金费率", justify="right")
            position_table.add_column("开仓价", justify="right")
            position_table.add_column("标记价", justify="right")
            position_table.add_column("强平价", justify="right")
            position_table.add_column("未实现盈亏", justify="right")
            position_table.add_column("结算周期", justify="center")
            position_table.add_column("下次结算", justify="center")

            total_unrealized_pnl = Decimal('0')

            # 处理持仓数据
            processed_positions = []
            for position in positions:
                if float(position.get('contracts', 0)) == 0:
                    continue

                # 处理symbol格式
                symbol = position['symbol']
                side = position['side']
                contracts = float(position.get('contracts', 0))
                leverage = position.get('leverage', 0)
                entry_price = float(position.get('entryPrice', 0))
                mark_price = float(position.get('markPrice', 0))
                liquidation_price = float(position.get('liquidationPrice', 0)) if position.get('liquidationPrice') is not None else 0
                unrealized_pnl = float(position.get('unrealizedPnl', 0))

                # 获取资金费率信息
                funding_info = self.exchange_api.get_bybit_futures_funding_rate(symbol.replace('/USDT:USDT', 'USDT'))
                funding_rate = funding_info.get('fundingRate', 0)
                funding_interval = funding_info.get('fundingIntervalHoursText', '无')
                next_funding_time = funding_info.get('fundingTime', 0)
                next_funding_time_str = datetime.fromtimestamp(next_funding_time/1000).strftime('%Y-%m-%d %H:%M:%S') if next_funding_time else '无'

                # 检查资金费率是否为负
                if funding_rate < -0.1 and fundingrate_auto_skip:
                    token = symbol.replace('/USDT:USDT', '')
                    self.run_funding_script(token)

                # 添加数据到表格
                position_table.add_row(
                    symbol.replace('/USDT:USDT', ''),
                    '多' if side == 'long' else '空',
                    f"{contracts:.2f}",
                    str(leverage),
                    f"{funding_rate:.4f}",
                    f"{entry_price:.6f}",
                    f"{mark_price:.6f}",
                    f"{liquidation_price:.6f}",
                    f"{unrealized_pnl:.2f}",
                    funding_interval,
                    next_funding_time_str
                )

                # 累加统计数据
                total_unrealized_pnl += Decimal(str(unrealized_pnl))

                # 构建处理后的持仓数据
                processed_position = {
                    'symbol': symbol,
                    'side': side,
                    'contracts': contracts,
                    'leverage': leverage,
                    'entryPrice': entry_price,
                    'markPrice': mark_price,
                    'liquidationPrice': liquidation_price,
                    'unrealizedPnl': unrealized_pnl,
                    'fundingRate': funding_rate,
                    'fundingInterval': funding_interval,
                    'nextFundingTime': next_funding_time
                }
                processed_positions.append(processed_position)

            # 打印持仓信息表格
            console.print(position_table)

            # 打印汇总信息
            console.print("\n=== 持仓汇总信息 ===")
            console.print(f"总未实现盈亏: {float(total_unrealized_pnl):.2f} USDT")
            console.print("=" * 160)
            return processed_positions

        except Exception as e:
            logger.exception(f"获取持仓信息时出错: {str(e)}")
            return []
        finally:
            await self.exchange.close()

async def main():
    """主函数"""
    try:
        get_proxy_ip()
        fetcher = BybitPositionFetcher()
        await fetcher.fetch_positions()
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        return 1
    return 0

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 