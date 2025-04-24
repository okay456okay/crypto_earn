#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io资金费率吃费扫描器

此脚本实现以下功能：
1. 获取Gate.io所有合约交易对
2. 筛选资金费率小于-1.0%的交易对
3. 筛选24小时交易量大于200万的交易对
4. 输出符合条件的交易对信息，包括下次结算时间
5. 找出结算时间最近且资金费率最小的交易对
6. 在结算时间前一秒开多单（成为多头），在结算时间平仓
7. 当资金费率为负时，空头支付资金费给多头，通过做多可以获取资金费
"""

import sys
import os
import asyncio
import ccxt.async_support as ccxt
from datetime import datetime, timedelta
from decimal import Decimal
import logging
import time
import ntplib
from pytz import timezone, utc
import argparse
import requests
import json

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import gateio_api_key, gateio_api_secret, proxies


class GateioScanner:
    def __init__(self, advance_time=0.055, open_position_time=1.0, funding_rate_threshold=-1.0, trade_amount_limit=1000.0, debug=False):
        """初始化Gate.io扫描器
        
        Args:
            advance_time (float): 提前平仓时间（秒）
            open_position_time (float): 开仓提前时间（秒）
            funding_rate_threshold (float): 资金费率筛选阈值（百分比）
            trade_amount_limit (float): 单笔交易限额（USDT）
            debug (bool): 是否启用调试日志
        """
        # 设置日志级别
        if debug:
            logger.setLevel(logging.DEBUG)
            logger.debug("调试模式已启用")
        else:
            logger.setLevel(logging.INFO)
            
        logger.debug(f"初始化参数: advance_time={advance_time}, open_position_time={open_position_time}, "
                   f"funding_rate_threshold={funding_rate_threshold}, trade_amount_limit={trade_amount_limit}")
                   
        self.exchange = ccxt.gateio({
            'apiKey': gateio_api_key,
            'secret': gateio_api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'swap',  # 使用永续合约
            },
            'proxies': proxies,
        })
        self.time_offset = 0  # 本地时间与服务器时间的偏移量（秒）
        self.advance_time = advance_time  # 提前平仓时间（秒）
        self.open_position_time = open_position_time  # 开仓提前时间（秒）
        self.funding_rate_threshold = funding_rate_threshold  # 资金费率筛选阈值（百分比）
        self.trade_amount_limit = trade_amount_limit  # 单笔交易限额（USDT）
        self.gateio_futures_volumes = {}  # 缓存合约交易量数据
        self.contract_specs = {}  # 缓存合约规格信息

    async def sync_time(self):
        """同步服务器时间，确保毫秒级精度"""
        try:
            # 第一次同步
            server_time = await self.exchange.fetch_time()
            local_time = int(time.time() * 1000)  # 本地时间（毫秒）
            self.time_offset = (server_time - local_time) / 1000  # 转换为秒
            
            logger.info(f"第一次时间同步 - 服务器时间: {datetime.fromtimestamp(server_time/1000, tz=utc)}, "
                       f"本地时间: {datetime.fromtimestamp(local_time/1000, tz=utc)}, "
                       f"时间偏移: {self.time_offset:.3f}秒")
            
            # 如果时间偏移超过1秒，使用NTP进行二次同步
            if abs(self.time_offset) > 1:
                try:
                    ntp_client = ntplib.NTPClient()
                    response = ntp_client.request('pool.ntp.org')
                    ntp_time = response.tx_time
                    local_time = time.time()
                    self.time_offset = ntp_time - local_time
                    
                    logger.info(f"NTP时间同步 - NTP时间: {datetime.fromtimestamp(ntp_time, tz=utc)}, "
                               f"本地时间: {datetime.fromtimestamp(local_time, tz=utc)}, "
                               f"时间偏移: {self.time_offset:.3f}秒")
                except Exception as e:
                    logger.warning(f"NTP时间同步失败: {str(e)}")
            
            # 进行多次微调
            max_attempts = 5
            min_offset = float('inf')
            best_offset = self.time_offset
            
            for attempt in range(max_attempts):
                # 等待一小段时间，让网络延迟稳定
                await asyncio.sleep(0.1)
                
                # 再次获取服务器时间
                server_time = await self.exchange.fetch_time()
                local_time = int(time.time() * 1000)
                current_offset = (server_time - local_time) / 1000
                
                logger.info(f"时间同步微调 {attempt+1}/{max_attempts} - "
                           f"服务器时间: {datetime.fromtimestamp(server_time/1000, tz=utc)}, "
                           f"本地时间: {datetime.fromtimestamp(local_time/1000, tz=utc)}, "
                           f"时间偏移: {current_offset:.3f}秒")
                
                # 记录最小偏移量
                if abs(current_offset) < abs(min_offset):
                    min_offset = current_offset
                    best_offset = current_offset
            
            # 使用最佳偏移量
            self.time_offset = best_offset
            logger.info(f"最终时间同步结果 - 时间偏移: {self.time_offset:.3f}秒 "
                       f"({self.time_offset*1000:.1f}毫秒)")
            
            # 验证最终时间同步结果
            server_time = await self.exchange.fetch_time()
            local_time = int(time.time() * 1000)
            final_offset = (server_time - local_time) / 1000 - self.time_offset
            
            logger.info(f"时间同步验证 - 最终误差: {final_offset:.3f}秒 "
                       f"({final_offset*1000:.1f}毫秒)")
            
            if abs(final_offset) > 0.1:  # 如果误差超过100毫秒
                logger.warning(f"时间同步误差较大: {final_offset*1000:.1f}毫秒")
            
        except Exception as e:
            logger.error(f"时间同步失败: {str(e)}")
            raise

    def get_current_time(self):
        """获取当前时间（考虑时间偏移）"""
        return time.time() + self.time_offset

    async def get_contract_specs(self, symbol):
        """获取合约规格信息"""
        try:
            if symbol in self.contract_specs:
                logger.debug(f"使用缓存的合约规格信息: {symbol}")
                return self.contract_specs[symbol]
            
            # 处理交易对格式
            base, quote = symbol.split('/')
            quote = quote.split(':')[0]  # 去掉:USDT后缀
            contract_symbol = f"{base}_{quote}"  # Gate.io的合约格式
            
            logger.debug(f"获取合约规格 - 原始交易对: {symbol}, 合约交易对: {contract_symbol}")
            
            markets = await self.exchange.fetch_markets()
            
            # 寻找匹配的合约
            contract_spec = None
            for market in markets:
                if market['id'] == contract_symbol and market['type'] == 'swap':
                    contract_spec = market
                    break
            
            if not contract_spec:
                logger.warning(f"未找到合约 {contract_symbol} 的市场信息")
                return None
            
            logger.debug(f"合约规格详情 - {symbol}:")
            logger.debug(f"- 合约乘数(contractSize): {contract_spec.get('contractSize')}")
            logger.debug(f"- 最小下单量(minAmount): {contract_spec.get('limits', {}).get('amount', {}).get('min')}")
            logger.debug(f"- 价格精度(precision): {contract_spec.get('precision')}")
            logger.debug(f"- 合约类型(type): {contract_spec.get('type')}")
            
            # 缓存合约规格
            self.contract_specs[symbol] = contract_spec
            return contract_spec
            
        except Exception as e:
            logger.error(f"获取合约规格信息失败: {str(e)}")
            return None

    async def get_all_symbols(self):
        """获取所有合约交易对"""
        try:
            markets = await self.exchange.load_markets()
            # 只保留USDT永续合约
            symbols = [symbol for symbol in markets.keys()
                       if symbol.endswith('/USDT:USDT') and 'swap' in markets[symbol]['type']]
            logger.debug(f"获取到 {len(symbols)} 个合约交易对")
            
            if logger.level == logging.DEBUG and len(symbols) > 0:
                logger.debug(f"合约交易对示例: {symbols[:5]}")
                
            return symbols
        except Exception as e:
            logger.error(f"获取交易对列表失败: {str(e)}")
            return []

    async def get_gateio_futures_volumes(self):
        """获取GateIO合约24小时交易量"""
        try:
            url = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
            logger.debug(f"请求Gate.io合约交易量数据: {url}")
            
            response = requests.get(url, proxies=proxies)
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"获取到 {len(data)} 条合约交易量数据")
                
                for item in data:
                    contract = item['contract']
                    if contract.endswith('_USDT'):
                        symbol = contract.replace('_USDT', 'USDT')
                        self.gateio_futures_volumes[symbol] = float(item['volume_24h_settle'])
                
                logger.debug(f"解析后的合约交易量数据条目数: {len(self.gateio_futures_volumes)}")
                if logger.level == logging.DEBUG and len(self.gateio_futures_volumes) > 0:
                    # 显示部分数据示例
                    sample_data = dict(list(self.gateio_futures_volumes.items())[:3])
                    logger.debug(f"交易量数据示例: {sample_data}")
            else:
                logger.error(f"请求Gate.io合约交易量数据失败: HTTP {response.status_code}")
                
        except Exception as e:
            logger.error(f"获取GateIO合约交易量数据失败: {str(e)}")

    async def get_funding_rate(self, symbol):
        """获取指定交易对的资金费率"""
        try:
            logger.debug(f"获取 {symbol} 的资金费率")
            
            # 获取资金费率
            funding_rate = await self.exchange.fetch_funding_rate(symbol)
            logger.debug(f"原始资金费率数据: {funding_rate}")

            # 从缓存中获取24小时交易量
            # 将标准格式转换为Gate.io格式，例如：BTC/USDT:USDT -> BTCUSDT
            base, quote = symbol.split('/')
            quote = quote.split(':')[0]
            gateio_symbol = f"{base}{quote}"
            volume_24h = self.gateio_futures_volumes.get(gateio_symbol, 0.0)
            
            logger.debug(f"{symbol} 转换为 {gateio_symbol} 用于查询交易量, 24h交易量: {volume_24h}")

            result = {
                'rate': funding_rate['fundingRate'] * 100,  # 转换为百分比
                'next_funding_time': funding_rate['fundingDatetime'],  # 下次结算时间
                'volume_24h': volume_24h  # 24小时交易量
            }
            
            logger.debug(f"{symbol} 资金费率: {result['rate']:.4f}%, 下次结算: {result['next_funding_time']}, 交易量: {result['volume_24h']}")
            
            return result
        except Exception as e:
            logger.error(f"获取{symbol}资金费率失败: {str(e)}")
            return None

    async def get_max_leverage(self, symbol):
        """获取交易对支持的最大杠杆倍数"""
        try:
            # 处理交易对格式
            base, quote = symbol.split('/')
            quote = quote.split(':')[0]  # 去掉:USDT后缀
            contract_symbol = f"{base}_{quote}"  # Gate.io的合约格式

            logger.debug(f"获取最大杠杆倍数 - 原始交易对: {symbol}")
            logger.debug(f"获取最大杠杆倍数 - 基础币: {base}")
            logger.debug(f"获取最大杠杆倍数 - 计价币: {quote}")
            logger.debug(f"获取最大杠杆倍数 - 合约交易对: {contract_symbol}")

            # Gate.io的API调用方式
            response = await self.exchange.fetch_market_leverage_tiers(contract_symbol)
            logger.debug(f"杠杆层级响应: {response}")

            if response and len(response) > 0:
                max_leverage = int(response[0]['maxLeverage'])
                logger.info(f"获取到{symbol}最大杠杆倍数: {max_leverage}倍")
                return max_leverage

            logger.warning(f"未能获取到{symbol}的最大杠杆倍数，使用默认值10倍")
            return 10  # 如果获取失败，返回默认值10倍

        except Exception as e:
            logger.error(f"获取最大杠杆倍数时出错: {str(e)}")
            return 10  # 如果出错，返回默认值10倍

    async def set_leverage(self, symbol, leverage):
        """设置杠杆倍数"""
        try:
            # 处理交易对格式
            base, quote = symbol.split('/')
            quote = quote.split(':')[0]  # 去掉:USDT后缀
            contract_symbol = f"{base}_{quote}"  # Gate.io的合约格式

            logger.debug(f"设置杠杆倍数 - 原始交易对: {symbol}")
            logger.debug(f"设置杠杆倍数 - 基础币: {base}")
            logger.debug(f"设置杠杆倍数 - 计价币: {quote}")
            logger.debug(f"设置杠杆倍数 - 合约交易对: {contract_symbol}")
            logger.debug(f"设置杠杆倍数 - 目标杠杆: {leverage}倍")

            # Gate.io的API调用方式
            response = await self.exchange.set_leverage(leverage, contract_symbol)
            logger.debug(f"设置杠杆响应: {response}")
            logger.info(f"设置{symbol}杠杆倍数为: {leverage}倍")

        except Exception as e:
            if "leverage not modified" in str(e).lower():
                logger.info(f"杠杆倍数已经是 {leverage}倍，无需修改")
            else:
                logger.error(f"设置杠杆倍数失败: {str(e)}")
                raise
    
    async def create_market_buy_order_open(self, symbol, amount):
        """创建市价多单（开仓）"""
        try:
            # 处理交易对格式
            base, quote = symbol.split('/')
            quote = quote.split(':')[0]  # 去掉:USDT后缀
            contract_symbol = f"{base}_{quote}"  # Gate.io的合约格式
            
            logger.debug(f"创建市价多单 - 原始交易对: {symbol}")
            logger.debug(f"创建市价多单 - 合约交易对: {contract_symbol}")
            logger.debug(f"创建市价多单 - 数量: {amount}")
            
            # 获取合约规格信息
            contract_spec = await self.get_contract_specs(symbol)
            if contract_spec:
                # 根据合约规格调整数量
                contract_size = float(contract_spec.get('contractSize', 1))
                precision = contract_spec.get('precision', {}).get('amount', 8)
                
                # 计算实际合约数量
                contracts_amount = amount
                if contract_size != 1:
                    contracts_amount = round(float(amount) / contract_size, precision)
                
                logger.debug(f"合约乘数: {contract_size}, 精度: {precision}")
                logger.debug(f"调整后的合约数量: {contracts_amount}")
                
                # 订单参数
                params = {
                    "type": "swap",
                    "reduceOnly": False,
                    "crossLeverageLimit": 0,  # 使用账户设置的杠杆
                    "account": "cross_margin"  # 使用全仓模式
                }
                
                logger.debug(f"开仓订单参数: {params}")
                
                order = await self.exchange.create_market_buy_order(
                    symbol=contract_symbol,
                    amount=contracts_amount,
                    params=params
                )
                logger.info(f"创建多单成功: {order}")
                return order
            else:
                raise Exception(f"无法获取合约 {symbol} 的规格信息")
                
        except Exception as e:
            logger.error(f"创建多单失败: {str(e)}")
            raise

    async def create_market_sell_order_close(self, symbol, amount):
        """创建市价卖单（平仓）"""
        try:
            # 处理交易对格式
            base, quote = symbol.split('/')
            quote = quote.split(':')[0]  # 去掉:USDT后缀
            contract_symbol = f"{base}_{quote}"  # Gate.io的合约格式
            
            logger.debug(f"创建市价卖单(平仓) - 原始交易对: {symbol}")
            logger.debug(f"创建市价卖单(平仓) - 合约交易对: {contract_symbol}")
            logger.debug(f"创建市价卖单(平仓) - 数量: {amount}")
            
            # 获取合约规格信息
            contract_spec = await self.get_contract_specs(symbol)
            if contract_spec:
                # 根据合约规格调整数量
                contract_size = float(contract_spec.get('contractSize', 1))
                precision = contract_spec.get('precision', {}).get('amount', 8)
                
                # 计算实际合约数量
                contracts_amount = amount
                if contract_size != 1:
                    contracts_amount = round(float(amount) / contract_size, precision)
                
                logger.debug(f"合约乘数: {contract_size}, 精度: {precision}")
                logger.debug(f"调整后的合约数量: {contracts_amount}")
                
                # 订单参数
                params = {
                    "type": "swap",
                    "reduceOnly": True,  # 确保是平仓操作
                    "account": "cross_margin"  # 使用全仓模式
                }
                
                logger.debug(f"平仓订单参数: {params}")
                
                order = await self.exchange.create_market_sell_order(
                    symbol=contract_symbol,
                    amount=contracts_amount,
                    params=params
                )
                logger.info(f"创建平仓单成功: {order}")
                return order
            else:
                raise Exception(f"无法获取合约 {symbol} 的规格信息")
                
        except Exception as e:
            logger.error(f"创建平仓单失败: {str(e)}")
            raise

    async def execute_trade(self, opportunity):
        """执行交易"""
        try:
            symbol = opportunity['symbol']
            # 处理交易对格式
            base, quote = symbol.split('/')
            quote = quote.split(':')[0]  # 去掉:USDT后缀
            contract_symbol = f"{base}_{quote}"  # Gate.io的合约格式
            
            logger.debug(f"执行交易 - 机会详情: {json.dumps(opportunity, default=str)}")
            logger.info(f"执行交易 - 原始交易对: {symbol}")
            logger.info(f"执行交易 - 基础币: {base}")
            logger.info(f"执行交易 - 计价币: {quote}")
            logger.info(f"执行交易 - 合约交易对: {contract_symbol}")
            
            # 计算交易金额
            volume_per_second = opportunity['volume_24h'] / (24 * 60 * 60)
            trade_amount = min(volume_per_second * 1, self.trade_amount_limit)  # 取每秒交易额的1倍和交易限额中的较小值
            logger.info(f"执行交易 - 每秒交易量: {volume_per_second:.2f} USDT")
            logger.info(f"执行交易 - 计划交易量: {trade_amount:.2f} USDT")
            
            # 获取最大杠杆倍数
            max_leverage = await self.get_max_leverage(symbol)
            logger.info(f"执行交易 - 最大杠杆倍数: {max_leverage}倍")
            
            # 设置杠杆
            await self.set_leverage(symbol, max_leverage)
            
            # 等待到距离结算时间2分钟
            next_funding_time = datetime.fromisoformat(
                opportunity['next_funding_time'].replace('Z', '+00:00')
            )
            
            # 计算等待时间（考虑时间偏移）
            now = datetime.fromtimestamp(self.get_current_time(), tz=utc)
            wait_seconds = (next_funding_time - now).total_seconds()
            logger.debug(f"当前时间: {now}, 结算时间: {next_funding_time}")
            logger.debug(f"等待时间计算: {wait_seconds:.3f} 秒")
            
            if wait_seconds > 120:  # 如果还有超过2分钟
                logger.info(f"距离结算时间还有 {wait_seconds:.3f} 秒，等待中...")
                await asyncio.sleep(wait_seconds - 120)  # 等待到距离结算时间2分钟
                
                # 重新获取当前价格
                ticker = await self.exchange.fetch_ticker(symbol)
                current_price = ticker['last']
                logger.info(f"重新获取价格: {current_price} USDT")
                logger.debug(f"价格详情: {ticker}")
                
                # 重新计算开仓数量
                position_size = trade_amount / current_price
                logger.info(f"重新计算开仓数量: {position_size} {base}")
                
                # 同步时间
                await self.sync_time()
                
                # 重新计算等待时间
                now = datetime.fromtimestamp(self.get_current_time(), tz=utc)
                wait_seconds = (next_funding_time - now).total_seconds()
                logger.info(f"同步后距离结算时间还有 {wait_seconds:.3f} 秒")
                
                # 等待到距离结算时间1秒（开仓时间）
                open_position_time = self.open_position_time  # 在结算前指定秒数开仓
                if wait_seconds > open_position_time:
                    logger.debug(f"等待 {wait_seconds - open_position_time:.3f} 秒后开仓")
                    await asyncio.sleep(wait_seconds - open_position_time)
            elif wait_seconds > self.open_position_time:  # 如果还有超过开仓时间
                logger.info(f"距离结算时间还有 {wait_seconds:.3f} 秒 ({wait_seconds*1000:.1f}毫秒)，等待中...")
                # 使用更精确的等待时间
                wait_ms = int((wait_seconds - self.open_position_time) * 1000)
                logger.debug(f"等待 {wait_ms} 毫秒后开仓")
                await asyncio.sleep(wait_ms / 1000)  # 使用毫秒级等待
            else:
                logger.warning(f"已经不足开仓提前时间 {abs(wait_seconds):.3f} 秒，立即开仓")
            
            # 获取合约规格信息，确保合约乘数正确
            contract_spec = await self.get_contract_specs(symbol)
            if not contract_spec:
                logger.error(f"无法获取合约 {symbol} 的规格信息，无法执行交易")
                return None, None
                
            # 开多单
            logger.info(f"在结算时间前{self.open_position_time}秒开多单: {position_size} {base}")
            open_time = time.time()  # 记录开仓时间
            
            # 记录开仓前状态
            try:
                positions_before = await self.exchange.fetch_positions([contract_symbol])
                logger.debug(f"开仓前持仓: {positions_before}")
                balance_before = await self.exchange.fetch_balance()
                logger.debug(f"开仓前余额: USDT={balance_before.get('USDT', {}).get('free', 0)}")
            except Exception as e:
                logger.warning(f"获取开仓前状态失败: {str(e)}")
            
            # 创建订单
            buy_order = await self.create_market_buy_order_open(
                symbol=symbol,
                amount=position_size
            )
            logger.debug(f"执行交易 - 开多单详细结果: {json.dumps(buy_order, default=str)}")
            
            # 计算需要等待的时间，确保在结算时间提前advance_time秒平仓
            now = time.time()
            settlement_time = datetime.fromisoformat(opportunity['next_funding_time'].replace('Z', '+00:00')).timestamp()
            wait_until_close = max(0, settlement_time - now - self.advance_time)  # 在结算时间提前advance_time秒平仓
            logger.info(f"开仓耗时 {now - open_time:.3f} 秒，等待 {wait_until_close:.3f} 秒后平仓")
            
            # 开仓后检查持仓
            try:
                positions_after = await self.exchange.fetch_positions([contract_symbol])
                logger.debug(f"开仓后持仓: {positions_after}")
                balance_after = await self.exchange.fetch_balance()
                logger.debug(f"开仓后余额: USDT={balance_after.get('USDT', {}).get('free', 0)}")
            except Exception as e:
                logger.warning(f"获取开仓后状态失败: {str(e)}")
            
            await asyncio.sleep(wait_until_close)
            
            # 平多单
            logger.info(f"在结算时间提前{self.advance_time*1000:.0f}ms平多单: {position_size} {symbol}")
            sell_order = await self.create_market_sell_order_close(
                symbol=symbol,
                amount=position_size
            )
            logger.debug(f"执行交易 - 平多单详细结果: {json.dumps(sell_order, default=str)}")
            
            # 等待一段时间确保订单完成
            await asyncio.sleep(3)
            
            # 获取开仓订单详情
            try:
                buy_order_details = await self.exchange.fetch_closed_order(buy_order['id'], contract_symbol)
                logger.debug(f"开仓订单详情: {json.dumps(buy_order_details, default=str)}")
            except Exception as e:
                logger.warning(f"获取开仓订单详情失败: {str(e)}")
                buy_order_details = buy_order  # 使用原始订单信息作为备选
            
            # 获取平仓订单详情
            try:
                sell_order_details = await self.exchange.fetch_closed_order(sell_order['id'], contract_symbol)
                logger.debug(f"平仓订单详情: {json.dumps(sell_order_details, default=str)}")
            except Exception as e:
                logger.warning(f"获取平仓订单详情失败: {str(e)}")
                sell_order_details = sell_order  # 使用原始订单信息作为备选
            
            # 获取开仓和平仓价格
            try:
                open_price = float(buy_order_details['average'])
                close_price = float(sell_order_details['average'])
                filled_amount = float(buy_order_details['filled'])
                # 获取开仓和平仓手续费
                open_fee = float(buy_order_details['fee']['cost']) if buy_order_details.get('fee', {}).get('cost') is not None else 0.0
                close_fee = float(sell_order_details['fee']['cost']) if sell_order_details.get('fee', {}).get('cost') is not None else 0.0
            except (KeyError, TypeError) as e:
                logger.warning(f"获取订单价格信息失败: {str(e)}")
                # 如果无法获取详细信息，使用订单创建时的信息
                open_price = float(buy_order.get('price', 0)) if buy_order.get('price') else float(buy_order.get('average', 0))
                close_price = float(sell_order.get('price', 0)) if sell_order.get('price') else float(sell_order.get('average', 0))
                filled_amount = float(buy_order.get('amount', 0))
                open_fee = 0.0
                close_fee = 0.0
            
            # 计算交易结果
            price_diff = close_price - open_price  # 多单盈亏 = (平仓价格 - 开仓价格) * 数量
            gross_profit = filled_amount * price_diff
            total_fee = open_fee + close_fee
            net_profit = gross_profit - total_fee
            profit_percent = (price_diff / open_price) * 100 if open_price != 0 else 0
            
            logger.info(f"\n=== 交易结果统计 ===")
            logger.info(f"交易对: {symbol}")
            logger.info(f"开仓时间: {buy_order_details.get('datetime', '')}")
            # 判断开仓时间是否早于结算时间
            try:
                open_time = datetime.fromisoformat(buy_order_details.get('datetime', '').replace('Z', '+00:00'))
                settlement_time = datetime.fromisoformat(opportunity['next_funding_time'].replace('Z', '+00:00'))
                time_diff = (settlement_time - open_time).total_seconds()
                logger.info(f"开仓时间距离结算时间: {time_diff:.3f} 秒")
            except Exception as e:
                logger.warning(f"计算时间差异失败: {str(e)}")
            
            logger.info(f"开仓价格: {open_price:.8f} USDT")
            logger.info(f"平仓价格: {close_price:.8f} USDT")
            logger.info(f"持仓数量: {filled_amount:.8f} {base}")
            logger.info(f"价差: {price_diff:.8f} USDT ({profit_percent:.4f}%)")
            logger.info(f"开仓手续费: {open_fee:.8f} USDT")
            logger.info(f"平仓手续费: {close_fee:.8f} USDT")
            logger.info(f"总手续费: {total_fee:.8f} USDT")
            logger.info(f"毛利润: {gross_profit:.8f} USDT")
            logger.info(f"净利润: {net_profit:.8f} USDT")
            
            # 检查平仓后持仓
            try:
                positions_final = await self.exchange.fetch_positions([contract_symbol])
                logger.debug(f"平仓后持仓: {positions_final}")
                balance_final = await self.exchange.fetch_balance()
                logger.debug(f"平仓后余额: USDT={balance_final.get('USDT', {}).get('free', 0)}")
            except Exception as e:
                logger.warning(f"获取平仓后状态失败: {str(e)}")
            
            return buy_order, sell_order
            
        except Exception as e:
            logger.error(f"执行交易时出错: {str(e)}")
            raise

    async def scan_markets(self):
        """扫描所有市场"""
        try:
            # 首先获取所有合约的交易量数据
            await self.get_gateio_futures_volumes()
            
            symbols = await self.get_all_symbols()
            logger.info(f"开始扫描 {len(symbols)} 个交易对...")

            results = []
            count = 0
            for symbol in symbols:
                count += 1
                if count % 10 == 0:
                    logger.debug(f"已扫描 {count}/{len(symbols)} 个交易对...")
                
                # 获取资金费率信息
                funding_info = await self.get_funding_rate(symbol)
                
                if funding_info is None:
                    continue

                # 检查是否满足条件：资金费率小于阈值且24小时交易量大于200万
                if (funding_info['rate'] <= self.funding_rate_threshold and funding_info['volume_24h'] >= 2000000):
                    logger.debug(f"找到符合条件的交易对: {symbol}, 资金费率: {funding_info['rate']}%, 交易量: {funding_info['volume_24h']}")
                    results.append({
                        'symbol': symbol,
                        'funding_rate': funding_info['rate'],
                        'next_funding_time': funding_info['next_funding_time'],
                        'volume_24h': funding_info['volume_24h']
                    })

            return results

        except Exception as e:
            logger.error(f"扫描市场时出错: {str(e)}")
            return []

    def find_best_opportunity(self, results):
        """找出结算时间最近且资金费率最小的交易对"""
        if not results:
            return None

        # 将时间字符串转换为datetime对象
        for result in results:
            result['next_funding_datetime'] = datetime.fromisoformat(
                result['next_funding_time'].replace('Z', '+00:00')
            )

        # 先按结算时间升序排序
        sorted_by_time = sorted(results, key=lambda x: x['next_funding_datetime'])
        logger.debug(f"按结算时间排序后的第一个机会: {sorted_by_time[0] if sorted_by_time else None}")
        
        # 获取最近的结算时间
        nearest_time = sorted_by_time[0]['next_funding_datetime']
        
        # 筛选出所有结算时间等于最近时间的交易对
        nearest_opportunities = [result for result in sorted_by_time 
                               if result['next_funding_datetime'] == nearest_time]
        logger.debug(f"找到 {len(nearest_opportunities)} 个最近结算时间的机会")
        
        # 在最近时间的交易对中，按资金费率升序排序
        nearest_opportunities.sort(key=lambda x: x['funding_rate'])
        
        # 返回资金费率最小的交易对
        best_opportunity = nearest_opportunities[0] if nearest_opportunities else None
        if best_opportunity:
            logger.debug(f"最佳机会: {best_opportunity['symbol']}, 资金费率: {best_opportunity['funding_rate']}%, 结算时间: {best_opportunity['next_funding_time']}")
        
        return best_opportunity

    async def close(self):
        """关闭交易所连接"""
        logger.debug("关闭交易所连接")
        await self.exchange.close()


def print_results(results):
    """打印扫描结果"""
    if not results:
        logger.info("没有找到符合条件的交易对")
        return

    logger.info("\n=== 符合条件的交易对 ===")
    logger.info(f"找到 {len(results)} 个符合条件的交易对")
    logger.info("\n{:<15} {:<15} {:<25} {:<15}".format(
        "交易对", "资金费率(%)", "下次结算时间", "24h交易量(USDT)"))
    logger.info("-" * 70)

    for result in sorted(results, key=lambda x: x['funding_rate']):  # 按资金费率升序排序
        logger.info("{:<15} {:<15.4f} {:<25} {:<15.2f}".format(
            result['symbol'],
            result['funding_rate'],
            result['next_funding_time'],
            result['volume_24h']
        ))


def print_best_opportunity(best_opportunity):
    """打印最佳交易机会"""
    if not best_opportunity:
        return

    logger.info("\n=== 最佳交易机会 ===")
    logger.info("交易对: {}".format(best_opportunity['symbol']))
    logger.info("资金费率: {:.4f}%".format(best_opportunity['funding_rate']))
    logger.info("下次结算时间: {}".format(best_opportunity['next_funding_time']))
    logger.info("24小时交易量: {:.2f} USDT".format(best_opportunity['volume_24h']))


async def main():
    """主函数"""
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='Gate.io资金费率吃费套利工具')
    parser.add_argument('-a', '--advance-time', type=float, default=0.055,
                      help='提前平仓时间（秒），默认0.055秒')
    parser.add_argument('-o', '--open-time', type=float, default=1.0,
                      help='提前开仓时间（秒），默认1.0秒')
    parser.add_argument('-t', '--threshold', type=float, default=-1.0,
                      help='资金费率筛选阈值（百分比），默认-1.0%%')
    parser.add_argument('-l', '--trade-limit', type=float, default=1000.0,
                      help='单笔交易限额（USDT），默认1000.0 USDT')
    parser.add_argument('-d', '--debug', action='store_true',
                      help='启用调试日志模式')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    logger.info("Gate.io资金费率吃费套利工具启动")
    logger.info(f"参数: advance_time={args.advance_time}, open_time={args.open_time}, threshold={args.threshold}, trade_limit={args.trade_limit}, debug={args.debug}")
    
    # 创建扫描器实例，传入参数
    scanner = GateioScanner(
        advance_time=args.advance_time,
        open_position_time=args.open_time,
        funding_rate_threshold=args.threshold,
        trade_amount_limit=args.trade_limit,
        debug=args.debug
    )
    
    try:
        # 扫描市场
        results = await scanner.scan_markets()
        print_results(results)
        
        # 找出最佳交易机会
        best_opportunity = scanner.find_best_opportunity(results)
        print_best_opportunity(best_opportunity)
        
        if best_opportunity:
            # 执行交易
            buy_order, sell_order = await scanner.execute_trade(best_opportunity)
            logger.info("交易执行完成!")
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        # 在调试模式下打印详细错误堆栈
        if args.debug:
            import traceback
            logger.error(traceback.format_exc())
    finally:
        await scanner.close()


if __name__ == "__main__":
    # 运行主函数
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close() 