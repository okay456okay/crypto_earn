#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binance资金费率吃费扫描器

此脚本实现以下功能：
1. 获取Binance所有合约交易对
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
from config import binance_api_key, binance_api_secret, proxies


class BinanceScanner:
    def __init__(self, advance_time=0.230, open_position_time=1.0, funding_rate_threshold=-1.0, trade_amount_limit=1000.0):
        """初始化Binance扫描器
        
        Args:
            advance_time (float): 提前平仓时间（秒）
            open_position_time (float): 开仓提前时间（秒）
            funding_rate_threshold (float): 资金费率筛选阈值（百分比）
            trade_amount_limit (float): 单笔交易限额（USDT）
        """
        self.exchange = ccxt.binance({
            'apiKey': binance_api_key,
            'secret': binance_api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',  # 使用永续合约
            },
            'proxies': proxies,
        })
        self.time_offset = 0  # 本地时间与服务器时间的偏移量（秒）
        self.advance_time = advance_time  # 提前平仓时间（秒）
        self.open_position_time = open_position_time  # 开仓提前时间（秒）
        self.funding_rate_threshold = funding_rate_threshold  # 资金费率筛选阈值（百分比）
        self.trade_amount_limit = trade_amount_limit  # 单笔交易限额（USDT）
        self.binance_futures_volumes = {}  # 缓存合约交易量数据

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

    async def get_all_symbols(self):
        """获取所有合约交易对"""
        try:
            markets = await self.exchange.load_markets()
            # 只保留USDT永续合约
            symbols = [symbol for symbol in markets.keys()
                       if symbol.endswith('/USDT:USDT') and 'swap' in markets[symbol]['type']]
            return symbols
        except Exception as e:
            logger.error(f"获取交易对列表失败: {str(e)}")
            return []

    async def get_binance_futures_volumes(self):
        """获取Binance合约24小时交易量"""
        try:
            url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
            response = requests.get(url, proxies=proxies)
            if response.status_code == 200:
                data = response.json()
                for item in data:
                    symbol = item['symbol']
                    if symbol.endswith('USDT'):
                        # 将Binance格式转换为CCXT格式以便一致性匹配
                        base = symbol[:-4]  # 去掉USDT
                        self.binance_futures_volumes[symbol] = float(item['quoteVolume'])
        except Exception as e:
            logger.error(f"获取Binance合约交易量数据失败: {str(e)}")

    async def get_funding_rate(self, symbol):
        """获取指定交易对的资金费率"""
        try:
            # 获取资金费率
            funding_rate = await self.exchange.fetch_funding_rate(symbol)

            # 从标准CCXT符号转换为Binance符号，如 BTC/USDT:USDT -> BTCUSDT
            base, quote = symbol.split('/')
            quote = quote.split(':')[0]
            binance_symbol = f"{base}{quote}"
            
            # 从缓存中获取24小时交易量
            volume_24h = self.binance_futures_volumes.get(binance_symbol, 0.0)

            return {
                'rate': funding_rate['fundingRate'] * 100,  # 转换为百分比
                'next_time': funding_rate['fundingDatetime'],  # 下次结算时间
                'volume_24h': volume_24h  # 24小时交易量
            }
        except Exception as e:
            logger.error(f"获取{symbol}资金费率失败: {str(e)}")
            return None

    async def get_max_leverage(self, symbol):
        """获取交易对支持的最大杠杆倍数"""
        try:
            # 处理交易对格式
            base, quote = symbol.split('/')
            quote = quote.split(':')[0]  # 去掉:USDT后缀
            contract_symbol = f"{base}{quote}"  # Binance的合约格式
            
            logger.info(f"获取最大杠杆倍数 - 原始交易对: {symbol}")
            logger.info(f"获取最大杠杆倍数 - 基础币: {base}")
            logger.info(f"获取最大杠杆倍数 - 计价币: {quote}")
            logger.info(f"获取最大杠杆倍数 - 合约交易对: {contract_symbol}")
            
            # Binance的API调用方式
            leverage_info = await self.exchange.fapiPrivateGetLeverageBracket({
                'symbol': contract_symbol
            })
            
            if leverage_info and len(leverage_info) > 0 and 'brackets' in leverage_info[0]:
                max_leverage = int(leverage_info[0]['brackets'][0]['initialLeverage'])
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
            contract_symbol = f"{base}{quote}"  # Binance的合约格式
            
            logger.info(f"设置杠杆倍数 - 原始交易对: {symbol}")
            logger.info(f"设置杠杆倍数 - 基础币: {base}")
            logger.info(f"设置杠杆倍数 - 计价币: {quote}")
            logger.info(f"设置杠杆倍数 - 合约交易对: {contract_symbol}")
            
            # Binance的API调用方式
            await self.exchange.fapiPrivatePostLeverage({
                'symbol': contract_symbol,
                'leverage': leverage
            })
            logger.info(f"设置{symbol}杠杆倍数为: {leverage}倍")
            
        except Exception as e:
            if "leverage not modified" in str(e).lower():
                logger.info(f"杠杆倍数已经是 {leverage}倍，无需修改")
            else:
                logger.error(f"设置杠杆倍数失败: {str(e)}")
                raise 

    async def execute_trade(self, opportunity):
        """执行交易"""
        try:
            symbol = opportunity['symbol']
            # 处理交易对格式
            base, quote = symbol.split('/')
            quote = quote.split(':')[0]  # 去掉:USDT后缀
            contract_symbol = f"{base}{quote}"  # Binance的合约格式
            
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
                opportunity['next_time'].replace('Z', '+00:00')
            )
            
            # 计算等待时间（考虑时间偏移）
            now = datetime.fromtimestamp(self.get_current_time(), tz=utc)
            wait_seconds = (next_funding_time - now).total_seconds()
            
            # 开仓参数准备
            open_params = {
                'positionSide': 'LONG'  # Binance的双向持仓模式
            }
            
            # 平仓参数准备
            close_params = {
                # 'reduceOnly': True,  # 确保是平仓操作
                'positionSide': 'LONG'  # Binance的双向持仓模式
            }
            
            if wait_seconds > 120:  # 如果还有超过2分钟
                logger.info(f"距离结算时间还有 {wait_seconds:.3f} 秒，等待中...")
                await asyncio.sleep(wait_seconds - 120)  # 等待到距离结算时间2分钟
                
                # 重新获取当前价格
                ticker = await self.exchange.fetch_ticker(symbol)
                current_price = ticker['last']
                logger.info(f"重新获取价格: {current_price} USDT")
                
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
                    await asyncio.sleep(wait_seconds - open_position_time)
            elif wait_seconds > self.open_position_time:  # 如果还有超过开仓时间
                logger.info(f"距离结算时间还有 {wait_seconds:.3f} 秒 ({wait_seconds*1000:.1f}毫秒)，等待中...")
                # 使用更精确的等待时间
                wait_ms = int((wait_seconds - self.open_position_time) * 1000)
                await asyncio.sleep(wait_ms / 1000)  # 使用毫秒级等待
            else:
                logger.warning(f"已经不足开仓提前时间 {abs(wait_seconds):.3f} 秒，立即开仓")
            
            # 开多单（使用市价买单开仓）
            logger.info(f"在结算时间前{self.open_position_time}秒开多单: {position_size} {symbol}")
            
            try:
                # 记录开仓请求发出时间（毫秒级时间戳）
                open_request_time = int(time.time() * 1000)
                buy_order = await self.exchange.create_market_buy_order(
                    symbol=symbol,
                    amount=position_size,
                    params=open_params
                )
                logger.info(f"创建多单成功: {buy_order}")
            except Exception as e:
                logger.error(f"创建多单失败: {str(e)}")
                raise
            
            # 计算需要等待的时间，确保在结算时间提前advance_time秒平仓
            now = time.time()
            settlement_time = next_funding_time.timestamp()
            wait_until_close = max(0, settlement_time - now - self.advance_time)  # 在结算时间提前advance_time秒平仓
            logger.info(f"等待 {wait_until_close:.3f} 秒后平仓")
            await asyncio.sleep(wait_until_close)
            
            # 平多单（使用市价卖单平仓）
            logger.info(f"在结算时间提前{self.advance_time*1000:.0f}ms平多单: {position_size} {symbol}")
            try:
                # 记录平仓请求发出时间（毫秒级时间戳）
                close_request_time = int(time.time() * 1000)
                sell_order = await self.exchange.create_market_sell_order(
                    symbol=symbol,
                    amount=position_size,
                    params=close_params
                )
                logger.info(f"创建平仓单成功: {sell_order}")
            except Exception as e:
                logger.error(f"创建平仓单失败: {str(e)}")
                raise
            
            # 等待一段时间确保订单完成
            await asyncio.sleep(3)
            
            # 获取开仓订单详情
            try:
                buy_order_details = await self.exchange.fetch_order(buy_order['id'], symbol)
                logger.info(f"开仓订单详情: {buy_order_details}")
            except Exception as e:
                logger.warning(f"获取开仓订单详情失败: {str(e)}")
                buy_order_details = buy_order  # 使用原始订单信息作为备选
            
            # 获取平仓订单详情
            try:
                sell_order_details = await self.exchange.fetch_order(sell_order['id'], symbol)
                logger.info(f"平仓订单详情: {sell_order_details}")
            except Exception as e:
                logger.warning(f"获取平仓订单详情失败: {str(e)}")
                sell_order_details = sell_order  # 使用原始订单信息作为备选
            
            # 获取开仓和平仓价格及手续费
            try:
                # 从订单详情中提取关键信息
                open_price = float(buy_order_details.get('average', 0) or 0)
                close_price = float(sell_order_details.get('average', 0) or 0)
                filled_amount = float(buy_order_details.get('filled', 0) or 0)
                
                # 初始化手续费为0
                open_fee = 0.0
                close_fee = 0.0
                
                # 安全地尝试从fee字段获取手续费
                fee_data = buy_order_details.get('fee')
                if fee_data is not None and isinstance(fee_data, dict):
                    open_fee = float(fee_data.get('cost', 0) or 0)
                
                fee_data = sell_order_details.get('fee')
                if fee_data is not None and isinstance(fee_data, dict):
                    close_fee = float(fee_data.get('cost', 0) or 0)
                
                # 从Binance字段中获取信息
                info = buy_order_details.get('info', {})
                if info and not open_price and 'avgPrice' in info:
                    open_price = float(info.get('avgPrice', 0) or 0)
                
                info = sell_order_details.get('info', {})
                if info and not close_price and 'avgPrice' in info:
                    close_price = float(info.get('avgPrice', 0) or 0)
                
                # 获取订单时间戳信息
                open_success_time = int(buy_order_details.get('timestamp', 0) or 0)
                close_success_time = int(sell_order_details.get('timestamp', 0) or 0)
                
                # 计算开仓和平仓延迟（毫秒）
                open_latency = open_success_time - open_request_time
                close_latency = close_success_time - close_request_time
                
                # 获取开仓和平仓价格及手续费时，添加以下代码获取实际手续费
                if open_fee == 0 or close_fee == 0:
                    try:
                        # 获取从开仓时间到现在的所有收入记录(包括手续费)
                        income_records = await self.exchange.fapiPrivateGetIncome({
                            'symbol': contract_symbol,
                            'incomeType': 'COMMISSION',  # 仅查询手续费
                            'startTime': open_success_time - 5000,  # 开仓时间前5秒
                            'endTime': close_success_time + 5000,   # 平仓时间后5秒
                            'limit': 50
                        })
                        
                        logger.debug(f"获取到的手续费记录: {income_records}")
                        
                        # 遍历收入记录，找到与当前交易相关的手续费记录
                        for record in income_records:
                            # 时间戳比较，确定是开仓还是平仓手续费
                            record_time = int(record.get('time', 0))
                            if abs(record_time - open_success_time) < 2000:  # 在开仓时间2秒内的记录视为开仓手续费
                                open_fee += abs(float(record.get('income', 0)))
                                logger.debug(f"找到开仓手续费: {open_fee} USDT")
                            elif abs(record_time - close_success_time) < 2000:  # 在平仓时间2秒内的记录视为平仓手续费
                                close_fee += abs(float(record.get('income', 0)))
                                logger.debug(f"找到平仓手续费: {close_fee} USDT")
                    
                    except Exception as e:
                        logger.warning(f"获取交易手续费记录失败: {str(e)}")
            
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(f"获取订单价格信息失败: {str(e)}")
                logger.warning(f"错误位置: {e.__traceback__.tb_frame.f_code.co_filename}:{e.__traceback__.tb_lineno}")
                # 如果无法获取详细信息，使用订单创建时的信息
                open_price = float(buy_order.get('price', 0) or buy_order.get('average', 0) or 0)
                close_price = float(sell_order.get('price', 0) or sell_order.get('average', 0) or 0)
                filled_amount = float(buy_order.get('amount', 0) or 0)
                open_fee = 0.0
                close_fee = 0.0
                
                # 设置默认延迟值
                open_latency = 0
                close_latency = 0
                open_success_time = 0
                close_success_time = 0
            
            # 计算交易结果
            price_diff = close_price - open_price  # 多单盈亏 = (平仓价格 - 开仓价格) * 数量
            gross_profit = filled_amount * price_diff
            total_fee = open_fee + close_fee
            net_profit = gross_profit - total_fee
            profit_percent = (price_diff / open_price) * 100 if open_price != 0 else 0
            
            # 输出交易结果
            logger.info(f"\n=== 交易结果统计 ===")
            logger.info(f"交易对: {symbol}")
            logger.info(f"开仓时间: {buy_order_details.get('datetime', '')}")
            logger.info(f"开仓价格: {open_price:.8f} USDT")
            logger.info(f"平仓价格: {close_price:.8f} USDT")
            logger.info(f"持仓数量: {filled_amount:.8f} {base}")
            logger.info(f"价差: {price_diff:.8f} USDT ({profit_percent:.4f}%)")
            logger.info(f"开仓手续费: {open_fee:.8f} USDT")
            logger.info(f"平仓手续费: {close_fee:.8f} USDT")
            logger.info(f"总手续费: {total_fee:.8f} USDT")
            logger.info(f"毛利润: {gross_profit:.8f} USDT")
            logger.info(f"净利润: {net_profit:.8f} USDT")
            
            # 添加订单执行时间统计
            logger.info(f"\n=== 订单执行时间统计 ===")
            logger.info(f"开仓请求发出时间: {datetime.fromtimestamp(open_request_time/1000, tz=utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            logger.info(f"开仓成功时间: {datetime.fromtimestamp(open_success_time/1000, tz=utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            logger.info(f"开仓延迟: {open_latency} 毫秒")
            logger.info(f"平仓请求发出时间: {datetime.fromtimestamp(close_request_time/1000, tz=utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            logger.info(f"平仓成功时间: {datetime.fromtimestamp(close_success_time/1000, tz=utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            logger.info(f"平仓延迟: {close_latency} 毫秒")
            
            return buy_order, sell_order
            
        except Exception as e:
            logger.error(f"执行交易时出错: {str(e)}")
            raise

    async def scan_markets(self):
        """扫描所有市场"""
        try:
            # 首先获取所有合约的交易量数据
            await self.get_binance_futures_volumes()
            
            symbols = await self.get_all_symbols()
            logger.info(f"开始扫描 {len(symbols)} 个交易对...")

            results = []
            for symbol in symbols:
                # 获取资金费率信息
                funding_info = await self.get_funding_rate(symbol)
                
                if funding_info is None:
                    continue

                # 检查是否满足条件：资金费率小于-1.0%且24小时交易量大于200万
                if (funding_info['rate'] <= self.funding_rate_threshold and funding_info['volume_24h'] >= 2000000):
                    results.append({
                        'symbol': symbol,
                        'funding_rate': funding_info['rate'],
                        'next_time': funding_info['next_time'],
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
                result['next_time'].replace('Z', '+00:00')
            )

        # 先按结算时间升序排序
        sorted_by_time = sorted(results, key=lambda x: x['next_funding_datetime'])
        
        # 获取最近的结算时间
        nearest_time = sorted_by_time[0]['next_funding_datetime']
        
        # 筛选出所有结算时间等于最近时间的交易对
        nearest_opportunities = [result for result in sorted_by_time 
                               if result['next_funding_datetime'] == nearest_time]
        
        # 在最近时间的交易对中，按资金费率升序排序
        nearest_opportunities.sort(key=lambda x: x['funding_rate'])
        
        # 返回资金费率最小的交易对
        return nearest_opportunities[0] if nearest_opportunities else None

    async def close(self):
        """关闭交易所连接"""
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
            result['next_time'],
            result['volume_24h']
        ))


def print_best_opportunity(best_opportunity):
    """打印最佳交易机会"""
    if not best_opportunity:
        return

    logger.info("\n=== 最佳交易机会 ===")
    logger.info("交易对: {}".format(best_opportunity['symbol']))
    logger.info("资金费率: {:.4f}%".format(best_opportunity['funding_rate']))
    logger.info("下次结算时间: {}".format(best_opportunity['next_time']))
    logger.info("24小时交易量: {:.2f} USDT".format(best_opportunity['volume_24h']))


async def main():
    """主函数"""
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='Binance资金费率吃费套利工具')
    parser.add_argument('-a', '--advance-time', type=float, default=0.230,
                      help='提前平仓时间（秒），默认0.230秒')
    parser.add_argument('-o', '--open-time', type=float, default=1.0,
                      help='提前开仓时间（秒），默认1.0秒')
    parser.add_argument('-t', '--threshold', type=float, default=-1.0,
                      help='资金费率筛选阈值（百分比），默认-1.0%%')
    parser.add_argument('-l', '--trade-limit', type=float, default=1000.0,
                      help='单笔交易限额（USDT），默认1000.0 USDT')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 创建扫描器实例，传入参数
    scanner = BinanceScanner(
        advance_time=args.advance_time,
        open_position_time=args.open_time,
        funding_rate_threshold=args.threshold,
        trade_amount_limit=args.trade_limit
    )
    
    try:
        # 同步服务器时间
        await scanner.sync_time()
        
        # 扫描市场
        results = await scanner.scan_markets()
        print_results(results)
        
        # 找出最佳交易机会
        best_opportunity = scanner.find_best_opportunity(results)
        print_best_opportunity(best_opportunity)
        
        if best_opportunity:
            # 执行交易
            await scanner.execute_trade(best_opportunity)
            logger.info("交易执行完成!")
        
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
    finally:
        await scanner.close()


if __name__ == "__main__":
    # 设置日志级别
    logger.setLevel(logging.INFO)

    # 运行主函数
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close() 