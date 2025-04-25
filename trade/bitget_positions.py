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
import subprocess

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import bitget_api_key, bitget_api_secret, bitget_api_passphrase, proxies
from tools.proxy import get_proxy_ip
from high_yield.exchange import ExchangeAPI

import logging
from tools.logger import logger
logger.setLevel(logging.ERROR)

class BitgetPositionFetcher:
    def __init__(self):
        """初始化Bitget交易所连接"""
        self.exchange = ccxtpro.bitget({
            'apiKey': bitget_api_key,
            'secret': bitget_api_secret,
            'password': bitget_api_passphrase,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy':  proxies.get('https', None),
            'wss_proxy':  proxies.get('https', None),
            'ws_socks_proxy':  proxies.get('https', None),
        })
        self.exchange_api = ExchangeAPI()

    def run_funding_script(self, token):
        """运行资金费率脚本"""
        try:
            script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts', 'funding_bitget.sh')
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
                print("当前没有持仓")
                return positions

            # 打印持仓信息
            print("\n=== Bitget合约持仓信息 ===")
            print(f"查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 140)

            # 打印表头
            header = f"{'交易对':<8} {'方向':<4} {'数量':<12} {'杠杆':<4} {'资金费率':<8} {'结算周期':<8} {'下次结算':<16} {'开仓价':<10} {'标记价':<10} {'未实现盈亏':<12} {'强平价':<10}"
            print(header)
            print("-" * 140)

            total_notional = Decimal('0')
            total_unrealized_pnl = Decimal('0')
            total_margin = Decimal('0')

            # 处理持仓数据，确保返回的字段与scanner.py中使用的字段一致
            processed_positions = []
            for position in positions:
                if float(position.get('contracts', 0)) == 0:
                    continue

                # 处理symbol格式，去掉/USDT:USDT后缀
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

                # 获取资金费率和结算信息
                try:
                    # 使用ExchangeAPI获取资金费率信息
                    funding_info = self.exchange_api.get_bitget_futures_funding_rate(symbol.replace('/USDT:USDT', 'USDT'))
                    funding_rate_value = funding_info.get('fundingRate', 0.0)
                    funding_interval = funding_info.get('fundingIntervalHours', 0)
                    next_funding_time = funding_info.get('fundingTime', 0)
                    next_funding_datetime = datetime.fromtimestamp(next_funding_time/1000).strftime('%Y-%m-%d %H:%M') if next_funding_time else 'N/A'
                    
                    # 检查资金费率是否为负
                    if funding_rate_value < 0:
                        token = symbol.replace('/USDT:USDT', '')
                        # self.run_funding_script(token)
                except Exception as e:
                    logger.warning(f"获取{symbol}资金费率失败: {str(e)}")
                    funding_rate_value = 0.0
                    funding_interval = 0
                    next_funding_datetime = 'N/A'

                # 格式化输出一行
                position_line = (
                    f"{symbol.replace('/USDT:USDT', ''):<12}"
                    f"{'多' if side == 'long' else '空':<4} "
                    f"{contracts:<14.2f}"
                    f"{int(leverage):<6}"
                    f"{funding_rate_value:<12.4f}"
                    f"{funding_interval:<8}"
                    f"{next_funding_datetime:<22}"
                    f"{entry_price:<13.6f}"
                    f"{mark_price:<14.6f}"
                    f"{unrealized_pnl:<16.2f}"
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
            print(f"总未实现盈亏: {float(total_unrealized_pnl):.2f} USDT")
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