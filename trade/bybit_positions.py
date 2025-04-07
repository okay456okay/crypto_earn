#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Bybit合约持仓信息查询脚本

此脚本用于获取和显示Bybit交易所的合约持仓信息，包括：
1. 所有合约的持仓情况
2. 持仓方向、数量、杠杆倍数
3. 未实现盈亏和已实现盈亏
4. 持仓保证金和风险率
"""

import sys
import os
import asyncio
import ccxt.pro as ccxtpro
from decimal import Decimal
from datetime import datetime

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import bybit_api_key, bybit_api_secret, proxies
from tools.proxy import get_proxy_ip

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

    async def fetch_positions(self):
        """获取所有合约持仓信息"""
        positions = []
        try:
            # 获取所有持仓信息
            positions = await self.exchange.fetch_positions()

            if not positions:
                print("当前没有持仓")
                return positions

            # 打印持仓信息
            print("\n=== Bybit合约持仓信息 ===")
            print(f"查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 140)

            # 打印表头
            header = f"{'交易对':<8} {'方向':<4} {'数量':<12} {'杠杆':<4} {'资金费率':<8} {'开仓价':<10} {'标记价':<10} {'未实现盈亏':<12} {'保证金':<10} {'名义价值':<10} {'风险率':<8} {'强平价':<10}"
            print(header)
            print("-" * 140)

            total_notional = Decimal('0')
            total_unrealized_pnl = Decimal('0')
            total_margin = Decimal('0')

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
                unrealized_pnl = float(position.get('unrealizedPnl', 0))
                margin = float(position.get('initialMargin', 0))
                notional = float(position.get('notional', 0))
                liquidation_price = float(position.get('liquidationPrice', 0))

                # 获取资金费率
                try:
                    funding_rate = await self.exchange.fetch_funding_rate(symbol)
                    funding_rate_value = float(funding_rate['fundingRate']) * 100
                except Exception as e:
                    logger.warning(f"获取{symbol}资金费率失败: {str(e)}")
                    funding_rate_value = 0.0

                # 计算风险率
                risk_ratio = (margin / notional * 100) if notional > 0 else 0

                # 格式化输出一行
                position_line = (
                    f"{symbol.replace('/USDT:USDT', ''):<12} "
                    f"{'多' if side == 'long' else '空':<4} "
                    f"{contracts:<14.2f} "
                    f"{int(leverage):<6} "
                    f"{funding_rate_value:<12.4f}"
                    f"{entry_price:<13.6f} "
                    f"{mark_price:<14.6f} "
                    f"{unrealized_pnl:<16.2f} "
                    f"{margin:<14.2f} "
                    f"{notional:<14.2f} "
                    f"{risk_ratio:<12.2f}"
                    f"{liquidation_price:<10.6f}"
                )
                print(position_line)

                # 累加统计数据
                total_notional += Decimal(str(notional))
                total_unrealized_pnl += Decimal(str(unrealized_pnl))
                total_margin += Decimal(str(margin))

                # 构建处理后的持仓数据
                processed_position = {
                    'symbol': symbol,
                    'side': side,
                    'contracts': contracts,
                    'leverage': leverage,
                    'entryPrice': entry_price,
                    'markPrice': mark_price,
                    'unrealizedPnl': unrealized_pnl,
                    'initialMargin': margin,
                    'notional': notional,
                    'liquidationPrice': liquidation_price,
                    'fundingRate': funding_rate_value
                }
                processed_positions.append(processed_position)

            print("-" * 140)

            # 打印汇总信息
            print("\n=== 持仓汇总信息 ===")
            print(f"总名义价值: {float(total_notional):.2f} USDT")
            print(f"总未实现盈亏: {float(total_unrealized_pnl):.2f} USDT")
            print(f"总持仓保证金: {float(total_margin):.2f} USDT")
            if float(total_notional) > 0:
                print(f"总风险率: {(float(total_margin) / float(total_notional) * 100):.2f}%")
            print("=" * 140)
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