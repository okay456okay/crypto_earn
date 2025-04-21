#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binance合约持仓信息查询脚本

此脚本用于获取和显示Binance交易所的合约持仓信息，包括：
1. 所有合约的持仓情况
2. 持仓方向、数量、杠杆倍数
3. 未实现盈亏和已实现盈亏
4. 资金费率和结算信息
"""

import sys
import os
import asyncio
import ccxt.pro as ccxtpro
import requests
from decimal import Decimal
from datetime import datetime
import subprocess

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import binance_api_key, binance_api_secret, proxies
from tools.proxy import get_proxy_ip
from high_yield.exchange import ExchangeAPI

import logging
from tools.logger import logger
logger.setLevel(logging.ERROR)

class BinancePositionFetcher:
    def __init__(self):
        """初始化Binance交易所连接"""
        self.exchange = ccxtpro.binance({
            'apiKey': binance_api_key,
            'secret': binance_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
            'options': {
                'defaultType': 'future',  # 使用USDT永续合约
            }
        })
        self.exchange_api = ExchangeAPI()
        self.binance_funding_info = {}
        self.binance_futures_volumes = {}
        self.exchange_info_cache = None
        self.max_leverage_cache = {}

    async def get_exchange_info(self):
        """获取交易所所有合约信息（缓存）"""
        if self.exchange_info_cache is None:
            try:
                self.exchange_info_cache = await self.exchange.fapiPublicGetExchangeInfo()
                logger.info("已获取并缓存Binance合约交易所信息")
            except Exception as e:
                logger.error(f"获取Binance交易所信息失败: {str(e)}")
                return None
        return self.exchange_info_cache

    async def get_max_leverage(self, symbol):
        """获取指定合约的最大杠杆倍数"""
        if symbol in self.max_leverage_cache:
            return self.max_leverage_cache[symbol]
        
        try:
            # 获取杠杆倍数信息
            leverage_info = await self.exchange.fapiPrivateGetLeverageBracket({
                'symbol': symbol
            })
            
            if leverage_info and leverage_info[0] and 'brackets' in leverage_info[0]:
                max_leverage = int(leverage_info[0]['brackets'][0]['initialLeverage'])
                logger.info(f"获取到{symbol}最大杠杆倍数: {max_leverage}倍")
                self.max_leverage_cache[symbol] = max_leverage
                return max_leverage
            else:
                logger.warning(f"获取{symbol}杠杆信息格式不正确: {leverage_info}")
                return None
        except Exception as e:
            logger.error(f"获取{symbol}最大杠杆倍数失败: {str(e)}")
            return None

    def get_binance_funding_info(self):
        """获取币安合约资金费率周期数据"""
        url = "https://www.binance.com/bapi/futures/v1/public/future/common/get-funding-info"
        try:
            response = requests.get(url, proxies=proxies)
            if response.status_code == 200:
                data = response.json()
                for i in data.get('data', []):
                    self.binance_funding_info[i['symbol']] = i
        except Exception as e:
            logger.error(f"binance get funding info failed, url: {url}, error: {str(e)}")

    def get_binance_futures_funding_rate(self, token):
        """获取币安合约资金费率"""
        try:
            url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={token}"
            response = requests.get(url, proxies=proxies)
            if response.status_code != 200 and response.text.find('Invalid symbol') == -1:
                logger.debug(f"binance get {token} future failed, url: {url}, status: {response.status_code}, response: {response.text}")
                return {}
            
            data = response.json()
            if not self.binance_funding_info:
                self.get_binance_funding_info()
            
            fundingIntervalHours = self.binance_funding_info.get(token, {}).get('fundingIntervalHours', 8)
            fundingIntervalHoursText = f"{fundingIntervalHours}小时"
            
            return {
                "fundingTime": data['nextFundingTime'],
                "fundingRate": float(data["lastFundingRate"]) * 100,  # 转换为百分比
                "markPrice": float(data["markPrice"]),
                "fundingIntervalHours": fundingIntervalHours,
                "fundingIntervalHoursText": fundingIntervalHoursText
            }
        except Exception as e:
            logger.error(f"获取Binance {token}合约资金费率时出错: {str(e)}")
            return {}

    def run_funding_script(self, token):
        """运行资金费率脚本"""
        try:
            script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts', 'funding_binance.sh')
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
            # 预先获取交易所信息（缓存）
            await self.get_exchange_info()
            
            # 获取所有持仓信息
            positions = await self.exchange.fetch_positions()

            if not positions:
                print("当前没有持仓")
                return positions

            # 打印持仓信息
            print("\n=== Binance合约持仓信息 ===")
            print(f"查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 160)

            # 打印表头
            header = f"{'交易对':<8} {'方向':<4} {'数量':<12} {'杠杆':<4} {'资金费率':<8} {'开仓价':<10} {'标记价':<10} {'强平价':<10} {'未实现盈亏':<12} {'结算周期':<8} {'下次结算':<19}"
            print(header)
            print("-" * 160)

            total_unrealized_pnl = Decimal('0')

            # 处理持仓数据
            processed_positions = []
            for position in positions:
                try:
                    # 跳过空仓位
                    if float(position.get('contracts', 0)) == 0:
                        continue

                    # 从info中获取symbol
                    symbol = position.get('info', {}).get('symbol', '')
                    if not symbol:
                        logger.warning(f"跳过无效的持仓数据: {position}")
                        continue

                    # 获取其他字段
                    side = position.get('side', 'unknown')
                    contracts = float(position.get('contracts', 0))
                    # leverage = position.get('leverage', 0) or position.get('info', {}).get('leverage', 0)
                    
                    # 获取最大杠杆倍数
                    leverage = await self.get_max_leverage(symbol) or '未知'
                    
                    entry_price = float(position.get('entryPrice', 0))
                    mark_price = float(position.get('markPrice', 0))
                    liquidation_price = float(position.get('liquidationPrice', 0))
                    unrealized_pnl = float(position.get('unrealizedPnl', 0))

                    # 获取资金费率信息
                    funding_info = self.get_binance_futures_funding_rate(symbol)
                    funding_rate = funding_info.get('fundingRate', 0.0)
                    funding_interval = funding_info.get('fundingIntervalHoursText', '无')
                    next_funding_time = funding_info.get('fundingTime', 0)
                    next_funding_time_str = datetime.fromtimestamp(next_funding_time/1000).strftime('%Y-%m-%d %H:%M:%S') if next_funding_time else '无'

                    # 检查资金费率是否为负
                    if funding_rate < 0:
                        token = symbol.replace('USDT', '')
                        self.run_funding_script(token)

                    # 格式化输出一行
                    position_line = (
                        f"{symbol.replace('USDT', ''):<12} "
                        f"{'多' if side == 'long' else '空':<4} "
                        f"{abs(contracts):<14.2f} "
                        f"{leverage:<6} "
                        f"{funding_rate:<12.4f}"
                        f"{entry_price:<13.6f} "
                        f"{mark_price:<14.6f} "
                        f"{liquidation_price:<14.6f} "
                        f"{unrealized_pnl:<16.2f} "
                        f"{funding_interval:<6} "
                        f"{next_funding_time_str:<19}"
                    )
                    print(position_line)

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

                except Exception as e:
                    logger.error(f"处理持仓数据时出错: {str(e)}")
                    logger.error(f"问题持仓数据: {position}")
                    continue

            print("-" * 160)

            # 打印汇总信息
            print("\n=== 持仓汇总信息 ===")
            print(f"总未实现盈亏: {float(total_unrealized_pnl):.2f} USDT")
            print("=" * 160)
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
        fetcher = BinancePositionFetcher()
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