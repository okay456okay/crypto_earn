#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Bitget合约持仓信息查询脚本

此脚本用于获取和显示Bitget交易所的合约持仓信息，包括：
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
from config import bitget_api_key, bitget_api_secret, bitget_api_passphrase, proxies

class BitgetPositionFetcher:
    def __init__(self):
        """初始化Bitget交易所连接"""
        self.exchange = ccxtpro.bitget({
            'apiKey': bitget_api_key,
            'secret': bitget_api_secret,
            'password': bitget_api_passphrase,
            'enableRateLimit': True,
            'proxies': proxies,
        })

    async def fetch_positions(self):
        """获取所有合约持仓信息"""
        positions = []
        try:
            # 获取所有持仓信息
            positions = await self.exchange.fetch_positions()
            
            if not positions:
                logger.info("当前没有持仓")
                return positions

            # 打印持仓信息
            logger.info("\n=== Bitget合约持仓信息 ===")
            logger.info(f"查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("-" * 120)
            
            # 打印表头
            header = f"{'交易对':<12} {'方向':<4} {'数量':<10} {'杠杆':<6} {'开仓价':<10} {'标记价':<10} {'未实现盈亏':<12} {'保证金':<10} {'名义价值':<10} {'风险率':<8} {'强平价':<10}"
            logger.info(header)
            logger.info("-" * 120)

            total_notional = Decimal('0')
            total_unrealized_pnl = Decimal('0')
            total_margin = Decimal('0')

            # 处理持仓数据，确保返回的字段与scanner.py中使用的字段一致
            processed_positions = []
            for position in positions:
                if float(position.get('contracts', 0)) == 0:
                    continue

                # 处理symbol格式，去掉/USDT:USDT后缀
                symbol = position['symbol'].replace('/USDT:USDT', '')
                side = position['side']
                contracts = float(position.get('contracts', 0))
                leverage = position.get('leverage', 0)
                entry_price = float(position.get('entryPrice', 0))
                mark_price = float(position.get('markPrice', 0))
                unrealized_pnl = float(position.get('unrealizedPnl', 0))
                margin = float(position.get('initialMargin', 0))
                notional = float(position.get('notional', 0))
                liquidation_price = float(position.get('liquidationPrice', 0))

                # 计算风险率
                risk_ratio = (margin / notional * 100) if notional > 0 else 0

                # 格式化输出一行
                position_line = (
                    f"{symbol:<12} "
                    f"{'多' if side == 'long' else '空':<4} "
                    f"{contracts:<10.4f} "
                    f"{leverage:<6}x "
                    f"{entry_price:<10.4f} "
                    f"{mark_price:<10.4f} "
                    f"{unrealized_pnl:<12.2f} "
                    f"{margin:<10.2f} "
                    f"{notional:<10.2f} "
                    f"{risk_ratio:<8.2f}% "
                    f"{liquidation_price:<10.4f}"
                )
                logger.info(position_line)

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
                    'liquidationPrice': liquidation_price
                }
                processed_positions.append(processed_position)

            logger.info("-" * 120)
            
            # 打印汇总信息
            logger.info("\n=== 持仓汇总信息 ===")
            logger.info(f"总名义价值: {float(total_notional):.2f} USDT")
            logger.info(f"总未实现盈亏: {float(total_unrealized_pnl):.2f} USDT")
            logger.info(f"总持仓保证金: {float(total_margin):.2f} USDT")
            logger.info(f"总风险率: {(float(total_margin) / float(total_notional) * 100):.2f}%")
            logger.info("=" * 120)
            logger.info(f"processed_positions: {processed_positions}")
            return processed_positions

        except Exception as e:
            logger.exception(f"获取持仓信息时出错: {str(e)}")
            return []
        finally:
            await self.exchange.close()

async def main():
    """主函数"""
    try:
        fetcher = BitgetPositionFetcher()
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