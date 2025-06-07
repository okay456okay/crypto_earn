#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多交易所资金费率套利交易脚本
功能：自动检测资金费率机会并执行套利交易
支持交易所：Binance、GateIO、Bybit、Bitget
作者：加密货币套利专家
版本：2.0.0
"""

import argparse
import asyncio
import logging
import sys
import time
import traceback
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple
import os

import ccxt
import ccxt.pro as ccxtpro

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import (
        binance_api_key, binance_api_secret,
        gateio_api_key, gateio_api_secret,
        bybit_api_key, bybit_api_secret,
        bitget_api_key, bitget_api_secret, bitget_api_passphrase,
        proxies
    )
except ImportError:
    print("警告: 无法导入配置文件，请确保config.py存在并包含所有交易所的API密钥")
    # 设置默认空值
    binance_api_key = binance_api_secret = ""
    gateio_api_key = gateio_api_secret = ""
    bybit_api_key = bybit_api_secret = ""
    bitget_api_key = bitget_api_secret = ""
    proxies = {}


# 配置日志
def setup_logging():
    """设置日志配置"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(log_dir, f'funding_rate_trader_{datetime.now().strftime("%Y%m%d")}.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


class FundingRateTrader:
    """多交易所资金费率交易器"""

    # 支持的交易所配置
    EXCHANGE_CONFIGS = {
        'binance': {
            'class': ccxt.binance,
            'api_key': lambda: binance_api_key,
            'api_secret': lambda: binance_api_secret,
            'options': {'defaultType': 'future'},
            'name': 'Binance'
        },
        'gateio': {
            'class': ccxt.gateio,
            'api_key': lambda: gateio_api_key,
            'api_secret': lambda: gateio_api_secret,
            'options': {'defaultType': 'swap'},
            'name': 'Gate.io'
        },
        'bybit': {
            'class': ccxt.bybit,
            'api_key': lambda: bybit_api_key,
            'api_secret': lambda: bybit_api_secret,
            'options': {
                'defaultType': 'linear',
                'createMarketBuyOrderRequiresPrice': False,
            },
            'name': 'Bybit'
        },
        'bitget': {
            'class': ccxt.bitget,
            'api_key': lambda: bitget_api_key,
            'api_secret': lambda: bitget_api_secret,
            'password': bitget_api_passphrase,
            'options': {'defaultType': 'swap'},
            'name': 'Bitget'
        }
    }

    def __init__(self, exchange_name: str, min_funding_rate: float = -0.005, manual_time: Optional[str] = None):
        """
        初始化交易器
        
        Args:
            exchange_name: 交易所名称 (binance, gateio, bybit, bitget)
            min_funding_rate: 触发套利的最小资金费率阈值
            manual_time: 手动指定的下次结算时间（ISO格式）
        """
        self.exchange_name = exchange_name.lower()
        self.exchange = None
        self.symbol = None
        self.position_info = {}
        self.market_info = {}

        # 交易参数
        self.min_funding_rate = min_funding_rate
        self.max_leverage = 20
        self.leverage = self.max_leverage
        self.min_order_amount = 6  # USDT
        self.funding_rate_buffer = 0.005  # 0.5% 缓冲

        # 止损参数
        self.stop_loss_threshold = 0.001  # 0.1% 止损阈值
        self.max_monitor_duration = 600  # 最大监控时间10分钟

        # 下次结算时间（手动指定或从API获取）
        self.next_funding_time = None
        if manual_time:
            try:
                self.next_funding_time = datetime.fromisoformat(manual_time.replace('Z', '+00:00')).astimezone()
                logger.info(f"使用手动指定的下次结算时间: {self.next_funding_time}")
            except Exception as e:
                logger.error(f"解析手动时间失败: {e}")
                raise

        self._initialize_exchange()

    def _initialize_exchange(self):
        """初始化交易所连接"""
        try:
            if self.exchange_name not in self.EXCHANGE_CONFIGS:
                raise ValueError(f"不支持的交易所: {self.exchange_name}")

            config_info = self.EXCHANGE_CONFIGS[self.exchange_name]

            config = {
                'apiKey': config_info['api_key'](),
                'secret': config_info['api_secret'](),
                'enableRateLimit': True,
                'options': config_info['options']
            }

            if self.exchange_name == 'bitget':
                config['password'] = config_info['password']

            if proxies:
                config['proxies'] = proxies

            self.exchange = config_info['class'](config)

            # 测试连接
            self.exchange.load_markets()
            logger.info(f"交易所连接成功: {config_info['name']} (实盘模式)")
            logger.info(f"资金费率阈值: {self.min_funding_rate:.6f} ({self.min_funding_rate * 100:.4f}%)")

        except Exception as e:
            logger.error(f"交易所连接失败: {e}")
            raise

    async def get_funding_rate_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取资金费率相关信息
        
        Args:
            symbol: 交易对符号
            
        Returns:
            包含资金费率信息的字典
        """
        try:
            # 对特定交易所进行格式处理
            query_symbol = symbol
            if self.exchange_name in ['bitget', 'bybit', 'gateio']:
                # 这些交易所需要特殊格式: LA/USDT -> LA/USDT:USDT
                if symbol.endswith('/USDT'):
                    query_symbol = f"{symbol}:USDT"
                    logger.info(f"{self.EXCHANGE_CONFIGS[self.exchange_name]['name']}交易对格式转换: {symbol} -> {query_symbol}")

            # 获取资金费率
            funding_rate_info = self.exchange.fetch_funding_rate(query_symbol)

            # 获取当前资金费率
            current_funding_rate = funding_rate_info['fundingRate']

            # 获取下次结算时间（如果未手动设置）
            if self.next_funding_time is None:
                if self.exchange_name == 'bitget':
                    info = funding_rate_info.get('info', {})
                    next_update = info.get('nextUpdate')
                    if next_update:
                        # nextUpdate是毫秒时间戳，转换为本地时区
                        funding_datetime_ms = int(next_update)
                        self.next_funding_time = datetime.fromtimestamp(funding_datetime_ms / 1000)
                        logger.info(f"Bitget下次结算时间 (来自nextUpdate): {self.next_funding_time}")
                    else:
                        # 如果没有nextUpdate，尝试使用默认方式
                        funding_time_str = funding_rate_info['fundingDatetime']
                        self.next_funding_time = datetime.fromisoformat(funding_time_str.replace('Z', '+00:00')).astimezone()
                else:
                    # 其他交易所使用标准方式，转换为本地时区
                    funding_time_str = funding_rate_info['fundingDatetime']
                    self.next_funding_time = datetime.fromisoformat(funding_time_str.replace('Z', '+00:00')).astimezone()

            logger.info(f"交易对: {symbol}")
            logger.info(f"当前资金费率: {current_funding_rate:.6f} ({current_funding_rate * 100:.4f}%)")
            logger.info(f"下次结算时间: {self.next_funding_time}")

            return {
                'symbol': symbol,
                'funding_rate': current_funding_rate,
                'next_funding_time': self.next_funding_time
            }

        except Exception as e:
            logger.error(f"获取资金费率信息失败: {e}")
            # 如果是特定交易所且出现格式问题，提供帮助信息
            if self.exchange_name in ['bitget', 'bybit', 'gateio']:
                logger.error(f"提示: {self.EXCHANGE_CONFIGS[self.exchange_name]['name']}交易对可能需要特殊格式")
                logger.error(f"尝试的格式: {query_symbol if 'query_symbol' in locals() else symbol}")
            raise

    async def get_binance_max_leverage(self, symbol: str) -> int:
        """
        获取Binance交易所支持的最大杠杆倍数
        
        Args:
            symbol: 交易对符号
            
        Returns:
            int: 最大杠杆倍数
        """
        try:
            # 将symbol格式转换为Binance格式 (BTC/USDT -> BTCUSDT)
            contract_symbol = symbol.replace('/', '')
            
            # 获取交易对信息 (同步方法，不需要await)
            response = self.exchange.fapiPublicGetExchangeInfo()
            
            if response and 'symbols' in response:
                for symbol_info in response['symbols']:
                    if symbol_info['symbol'] == contract_symbol:
                        # 获取杠杆倍数信息 (同步方法，不需要await)
                        leverage_info = self.exchange.fapiPrivateGetLeverageBracket({
                            'symbol': contract_symbol
                        })
                        
                        if leverage_info and 'brackets' in leverage_info[0]:
                            max_leverage = int(leverage_info[0]['brackets'][0]['initialLeverage'])
                            logger.info(f"获取到{symbol}最大杠杆倍数: {max_leverage}倍")
                            return max_leverage
            
            raise Exception(f"未能获取到{symbol}的最大杠杆倍数")
            
        except Exception as e:
            logger.warning(f"获取Binance最大杠杆倍数失败: {e}")
            logger.info("使用默认杠杆倍数: 20倍")
            return 20

    async def get_market_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取市场信息（最大杠杆、交易量等）
        
        Args:
            symbol: 交易对符号
            
        Returns:
            市场信息字典
        """
        try:
            # 对特定交易所进行格式处理
            query_symbol = symbol
            if self.exchange_name in ['bitget', 'bybit', 'gateio']:
                if symbol.endswith('/USDT'):
                    query_symbol = f"{symbol}:USDT"

            # 获取市场信息
            market = self.exchange.market(query_symbol)

            # 获取24小时交易量
            ticker = self.exchange.fetch_ticker(query_symbol)
            volume_24h = ticker['quoteVolume']  # USDT计价的交易量

            # 获取交易对的最大杠杆倍数
            if self.exchange_name == 'binance':
                # Binance使用专门的API获取真实杠杆倍数
                max_leverage = await self.get_binance_max_leverage(symbol)
            else:
                # 其他交易所从市场信息中获取
                max_leverage = market.get('limits', {}).get('leverage', {}).get('max')
                if not max_leverage:
                    # 如果获取不到，使用默认值
                    default_leverages = {
                        'bybit': 100,
                        'gateio': 100, 
                        'bitget': 125
                    }
                    max_leverage = default_leverages.get(self.exchange_name, 20)
                    logger.info(f"使用{self.EXCHANGE_CONFIGS[self.exchange_name]['name']}默认杠杆倍数: {max_leverage}倍")
                    
            if isinstance(max_leverage, str):
                max_leverage = int(max_leverage)

            # 计算每分钟交易量（近似值）
            volume_per_minute = volume_24h / (24 * 60) if volume_24h else 0

            logger.info(f"24小时交易量: {volume_24h:,.2f} USDT")
            logger.info(f"每分钟交易量: {volume_per_minute:,.2f} USDT")
            logger.info(f"最大杠杆倍数: {max_leverage}x")

            return {
                'symbol': symbol,
                'max_leverage': max_leverage,
                'volume_24h': volume_24h,
                'volume_per_minute': volume_per_minute,
                'market_info': market
            }

        except Exception as e:
            logger.error(f"获取市场信息失败: {e}")
            raise

    async def wait_until_funding_time(self, seconds_before: int):
        """
        等待到资金费率结算前指定秒数的时间点
        
        Args:
            seconds_before: 提前多少秒（例如：15表示结算前15秒）
        """
        if self.next_funding_time is None:
            logger.error("下次结算时间未设置")
            return

        current_time = datetime.now(self.next_funding_time.tzinfo)
        wait_seconds = (self.next_funding_time - current_time - timedelta(seconds=seconds_before)).total_seconds()

        if wait_seconds > 0:
            action_desc = "检查条件" if seconds_before == 15 else "下单"
            logger.info(f"等待 {wait_seconds:.1f} 秒到{action_desc}时间（结算前{seconds_before}秒）: {self.next_funding_time}")
            await asyncio.sleep(wait_seconds)
        else:
            logger.info(f"已到达{'检查' if seconds_before == 15 else '下单'}时间")

    async def check_funding_rate_condition(self, symbol: str) -> Tuple[bool, float]:
        """
        检查资金费率是否满足交易条件
        
        Args:
            symbol: 交易对符号
            
        Returns:
            (是否满足条件, 当前资金费率)
        """
        try:
            funding_info = await self.get_funding_rate_info(symbol)
            current_rate = funding_info['funding_rate']

            condition_met = current_rate < self.min_funding_rate

            logger.info(f"资金费率检查:")
            logger.info(f"当前费率: {current_rate:.6f} ({current_rate * 100:.4f}%)")
            logger.info(f"阈值: {self.min_funding_rate:.6f} ({self.min_funding_rate * 100:.4f}%)")
            logger.info(f"条件满足: {'是' if condition_met else '否'}")

            return condition_met, current_rate

        except Exception as e:
            logger.error(f"检查资金费率条件失败: {e}")
            return False, 0.0

    async def calculate_order_size(self, symbol: str, volume_per_minute: float) -> Tuple[int, float]:
        """
        计算订单大小
        
        Args:
            symbol: 交易对符号
            volume_per_minute: 每分钟交易量
            
        Returns:
            (杠杆倍数, 订单金额USDT)
        """
        # 计算杠杆倍数: min(20, 最大杠杆)
        max_leverage = self.market_info.get('max_leverage', 125)
        leverage = min(self.max_leverage, max_leverage)

        # 计算订单金额: min(100 USDT, 每分钟交易量/100)
        order_amount = min(self.min_order_amount, volume_per_minute / 100)

        # 确保订单金额不小于最小值
        order_amount = max(order_amount, 5)  # 最小10 USDT

        logger.info(f"计算订单参数:")
        logger.info(f"杠杆倍数: {leverage}x")
        logger.info(f"订单金额: {order_amount:.2f} USDT")

        return leverage, order_amount

    async def set_leverage(self, symbol: str, leverage: int):
        """
        设置杠杆倍数
        
        Args:
            symbol: 交易对符号
            leverage: 杠杆倍数
        """
        try:
            # 对特定交易所进行格式处理
            leverage_symbol = symbol
            if self.exchange_name in ['bitget', 'bybit', 'gateio']:
                if symbol.endswith('/USDT'):
                    leverage_symbol = f"{symbol}:USDT"
                    
            if self.exchange_name == 'binance':
                result = self.exchange.set_leverage(leverage, leverage_symbol)
            elif self.exchange_name == 'bybit':
                # Bybit 需要设置保证金模式和杠杆
                # result = self.exchange.set_leverage(leverage, leverage_symbol, params={'marginMode': 'cross'})
                params = {
                    'category': 'linear',
                    'symbol': symbol.replace('/', ''),
                    'buyLeverage': str(leverage),
                    'sellLeverage': str(leverage)
                }
                logger.info(f"使用Bybit支持的最大杠杆倍数: {leverage}倍")
                self.exchange.privatePostV5PositionSetLeverage(params)
                logger.info(f"设置Bybit {leverage_symbol}合约杠杆倍数为: {leverage}x")
            elif self.exchange_name == 'gateio':
                # Gate.io 通过私有API设置杠杆
                result = self.exchange.set_leverage(leverage, leverage_symbol)
            elif self.exchange_name == 'bitget':
                # Bitget 设置杠杆
                result = self.exchange.set_leverage(leverage, leverage_symbol, params={'marginMode': 'cross'})
            else:
                # 通用方法
                result = self.exchange.set_leverage(leverage, leverage_symbol)

            logger.info(f"设置杠杆倍数成功: {leverage}x")
            return result

        except Exception as e:
            logger.error(f"设置杠杆倍数失败: {e}")
            # 某些交易所可能不支持动态设置杠杆，记录警告但不抛出异常
            logger.warning(f"注意: {self.EXCHANGE_CONFIGS[self.exchange_name]['name']} 可能需要手动设置杠杆倍数")
            return None

    async def place_short_order(self, symbol: str, amount_usdt: float) -> Dict[str, Any]:
        """
        下空单
        
        Args:
            symbol: 交易对符号
            amount_usdt: 订单金额（USDT）
            
        Returns:
            订单信息
        """
        try:
            # 对特定交易所进行格式处理
            trading_symbol = symbol
            if self.exchange_name in ['bitget', 'bybit', 'gateio']:
                if symbol.endswith('/USDT'):
                    trading_symbol = f"{symbol}:USDT"
                    logger.info(f"{self.EXCHANGE_CONFIGS[self.exchange_name]['name']}下单格式转换: {symbol} -> {trading_symbol}")

            # 获取当前价格
            ticker = self.exchange.fetch_ticker(trading_symbol)
            current_price = ticker['last']

            # 计算数量（基于USDT金额）
            quantity = amount_usdt / current_price

            # 根据不同交易所设置下单参数
            if self.exchange_name == 'binance':
                order_params = {'positionSide': 'SHORT'}
            elif self.exchange_name == 'bybit':
                order_params = {
                    "category": "linear",
                    "positionIdx": 0,  # 单向持仓
                    "reduceOnly": False
                }
            elif self.exchange_name == 'gateio':
                order_params = {
                    'reduceOnly': False,
                    "crossLeverageLimit": self.leverage,
                    "account": "cross_margin",
                }
            elif self.exchange_name == 'bitget':
                order_params = {"reduceOnly": False}
            else:
                order_params = {}

            # 下市价空单
            order = self.exchange.create_market_sell_order(trading_symbol, quantity, params=order_params)

            logger.info(f"空单下单成功:")
            logger.info(f"交易所: {self.EXCHANGE_CONFIGS[self.exchange_name]['name']}")
            logger.info(f"订单ID: {order['id']}")
            logger.info(f"交易对: {symbol}")
            logger.info(f"数量: {quantity:.6f}")
            logger.info(f"预估价格: {current_price:.4f}")
            logger.info(f"预估金额: {amount_usdt:.2f} USDT")

            return order

        except Exception as e:
            logger.error(f"下空单失败: {e}")
            raise

    async def check_order_status(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """
        检查订单状态
        
        Args:
            order_id: 订单ID
            symbol: 交易对符号
            
        Returns:
            订单详细信息
        """
        try:
            # 对特定交易所进行格式处理
            query_symbol = symbol
            if self.exchange_name in ['bitget', 'bybit', 'gateio']:
                if symbol.endswith('/USDT'):
                    query_symbol = f"{symbol}:USDT"

            if self.exchange_name == 'bybit':
                closed_orders = self.exchange.fetch_closed_orders(query_symbol, limit=10)
                for order in closed_orders:
                    if order.get('id') == order_id:
                        order_info = order
                        break
            else:
                order_info = self.exchange.fetch_order(order_id, query_symbol)

            logger.info(f"订单状态检查:")
            logger.info(f"订单ID: {order_id}")
            logger.info(f"状态: {order_info['status']}")
            logger.info(f"已成交数量: {order_info.get('filled', 0):.6f}")
            logger.info(f"平均成交价格: {order_info.get('average', 0):.4f}")

            return order_info

        except Exception as e:
            logger.error(f"检查订单状态失败: {e}")
            raise

    async def place_close_order(self, symbol: str, quantity: float, open_price: float, funding_rate: float) -> Dict[
        str, Any]:
        """
        下平仓订单
        
        Args:
            symbol: 交易对符号
            quantity: 平仓数量
            open_price: 开仓价格
            funding_rate: 资金费率
            
        Returns:
            平仓订单信息
        """
        try:
            # 对特定交易所进行格式处理
            trading_symbol = symbol
            if self.exchange_name in ['bitget', 'bybit', 'gateio']:
                if symbol.endswith('/USDT'):
                    trading_symbol = f"{symbol}:USDT"

            # 计算平仓价格: 开仓价格 * (1 + 资金费率 - 0.5%)
            close_price = open_price * (1 + funding_rate - self.funding_rate_buffer)

            logger.info(f"计算平仓价格:")
            logger.info(f"开仓价格: {open_price:.4f}")
            logger.info(f"资金费率: {funding_rate:.6f}")
            logger.info(f"平仓价格: {close_price:.4f}")

            # 根据不同交易所设置平仓参数
            if self.exchange_name == 'binance':
                order_params = {'positionSide': 'SHORT'}
            elif self.exchange_name == 'bybit':
                order_params = {'reduceOnly': True}
            elif self.exchange_name == 'gateio':
                order_params = {'reduceOnly': True}
            elif self.exchange_name == 'bitget':
                order_params = {'reduceOnly': True}
            else:
                order_params = {'reduceOnly': True}

            # 下限价买入平仓单
            order = self.exchange.create_limit_buy_order(trading_symbol, quantity, close_price, params=order_params)

            logger.info(f"平仓订单下单成功:")
            logger.info(f"交易所: {self.EXCHANGE_CONFIGS[self.exchange_name]['name']}")
            logger.info(f"订单ID: {order['id']}")
            logger.info(f"类型: 限价买入")
            logger.info(f"数量: {quantity:.6f}")
            logger.info(f"价格: {close_price:.4f}")

            return order

        except Exception as e:
            logger.error(f"下平仓订单失败: {e}")
            raise

    async def monitor_stop_loss(self, symbol: str, open_price: float, quantity: float, limit_order_id: str):
        """
        监控止损，在资金结算后监控价格变化
        
        Args:
            symbol: 交易对符号
            open_price: 开仓价格
            quantity: 持仓数量
            limit_order_id: 限价平仓订单ID
        """
        try:
            # 等待到资金结算时间
            current_time = datetime.now(self.next_funding_time.tzinfo)
            wait_seconds = (self.next_funding_time - current_time).total_seconds()

            if wait_seconds > 0:
                logger.info(f"等待 {wait_seconds:.1f} 秒到资金结算时间，然后开始止损监控")
                await asyncio.sleep(wait_seconds)
            else:
                logger.info("已到达或超过结算时间，立即开始止损监控")

            logger.info("=" * 50)
            logger.info("开始止损监控")
            logger.info(f"开仓价格: {open_price:.4f}")
            logger.info(f"止损阈值: +{self.stop_loss_threshold * 100:.1f}%")
            logger.info(f"监控持仓数量: {quantity:.6f}")
            logger.info("=" * 50)

            start_time = datetime.now()
            check_count = 0

            while True:
                check_count += 1

                # 检查是否超过最大监控时间
                elapsed_time = (datetime.now() - start_time).total_seconds()
                if elapsed_time > self.max_monitor_duration:
                    logger.info(f"达到最大监控时间 {self.max_monitor_duration} 秒，停止监控")
                    break

                # 检查限价订单是否已成交
                try:
                    # 对特定交易所进行格式处理
                    query_symbol = symbol
                    if self.exchange_name in ['bitget', 'bybit', 'gateio']:
                        if symbol.endswith('/USDT'):
                            query_symbol = f"{symbol}:USDT"

                    limit_order_status = self.exchange.fetch_order(limit_order_id, query_symbol)
                    if limit_order_status['status'] == 'closed':
                        logger.info("限价平仓订单已成交，停止止损监控")
                        return

                except Exception as e:
                    logger.warning(f"检查限价订单状态失败: {e}")

                # 获取当前价格
                try:
                    # 对特定交易所进行格式处理
                    ticker_symbol = symbol
                    if self.exchange_name in ['bitget', 'bybit', 'gateio']:
                        if symbol.endswith('/USDT'):
                            ticker_symbol = f"{symbol}:USDT"

                    ticker = self.exchange.fetch_ticker(ticker_symbol)
                    current_price = ticker['last']

                    # 计算价格变化百分比
                    price_change_pct = (current_price - open_price) / open_price

                    logger.info(
                        f"止损监控 #{check_count}: 当前价格 {current_price:.4f}, 变化 {price_change_pct * 100:+.3f}%")

                    # 检查是否触发止损
                    if price_change_pct > self.stop_loss_threshold:
                        logger.warning("=" * 50)
                        logger.warning("🚨 触发止损条件！")
                        logger.warning(f"当前价格: {current_price:.4f}")
                        logger.warning(f"开仓价格: {open_price:.4f}")
                        logger.warning(
                            f"价格上涨: {price_change_pct * 100:.3f}% > {self.stop_loss_threshold * 100:.1f}%")
                        logger.warning("立即执行市价平仓...")
                        logger.warning("=" * 50)

                        # 执行市价平仓
                        await self.execute_stop_loss(symbol, quantity, limit_order_id)
                        return

                except Exception as e:
                    logger.error(f"获取价格失败: {e}")

                # 等待1秒后继续监控
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"止损监控失败: {e}")
            logger.error(f"错误详情: {traceback.format_exc()}")

    async def execute_stop_loss(self, symbol: str, quantity: float, limit_order_id: str):
        """
        执行止损平仓
        
        Args:
            symbol: 交易对符号
            quantity: 平仓数量
            limit_order_id: 需要取消的限价订单ID
        """
        try:
            # 对特定交易所进行格式处理
            trading_symbol = symbol
            if self.exchange_name in ['bitget', 'bybit', 'gateio']:
                if symbol.endswith('/USDT'):
                    trading_symbol = f"{symbol}:USDT"

            # 1. 取消原限价订单
            logger.info("1. 取消原限价平仓订单...")
            try:
                cancel_result = self.exchange.cancel_order(limit_order_id, trading_symbol)
                logger.info(f"限价订单取消成功: {limit_order_id}")
            except Exception as e:
                logger.warning(f"取消限价订单失败 (可能已成交): {e}")

            # 2. 执行市价平仓
            logger.info("2. 执行市价平仓...")

            # 根据不同交易所设置止损平仓参数
            if self.exchange_name == 'binance':
                order_params = {'positionSide': 'SHORT'}
            elif self.exchange_name == 'bybit':
                order_params = {'reduceOnly': True}
            elif self.exchange_name == 'gateio':
                order_params = {'reduceOnly': True}
            elif self.exchange_name == 'bitget':
                order_params = {'reduceOnly': True}
            else:
                order_params = {'reduceOnly': True}

            stop_loss_order = self.exchange.create_market_buy_order(trading_symbol, quantity, params=order_params)

            logger.info("🔴 止损平仓订单执行成功:")
            logger.info(f"交易所: {self.EXCHANGE_CONFIGS[self.exchange_name]['name']}")
            logger.info(f"订单ID: {stop_loss_order['id']}")
            logger.info(f"类型: 市价买入")
            logger.info(f"数量: {quantity:.6f}")

            return stop_loss_order

        except Exception as e:
            logger.error(f"执行止损平仓失败: {e}")
            raise

    async def execute_arbitrage_strategy(self, symbol: str):
        """
        执行套利策略
        
        Args:
            symbol: 交易对符号
        """
        try:
            logger.info("=" * 60)
            logger.info("开始执行资金费率套利策略")
            logger.info("=" * 60)

            self.symbol = symbol

            # 1. 获取市场信息
            logger.info("1. 获取市场信息...")
            self.market_info = await self.get_market_info(symbol)

            # 2. 获取资金费率信息
            logger.info("2. 获取资金费率信息...")
            funding_info = await self.get_funding_rate_info(symbol)

            # 3. 等待检查时间（结算前15秒）
            logger.info("3. 等待资金费率检查时间...")
            await self.wait_until_funding_time(10)

            # 4. 检查资金费率条件
            logger.info("4. 检查资金费率条件...")
            condition_met, current_rate = await self.check_funding_rate_condition(symbol)

            if not condition_met:
                logger.info("资金费率条件不满足，退出策略")
                return

            # 5. 计算订单参数
            logger.info("5. 计算订单参数...")
            self.leverage, order_amount = await self.calculate_order_size(
                symbol, self.market_info['volume_per_minute']
            )

            # 6. 设置杠杆
            logger.info("6. 设置杠杆倍数...")
            await self.set_leverage(symbol, self.leverage)

            # 7. 等待到下单时间（结算前5秒）
            logger.info("7. 等待到下单时间（结算前2秒）...")
            await self.wait_until_funding_time(2)

            # 8. 下空单
            logger.info("8. 下空单...")
            # 记录下单前时间戳
            order_start_time = time.time()
            short_order = await self.place_short_order(symbol, order_amount)

            # 9. 监控订单状态直到成交
            logger.info("9. 监控订单状态直到成交...")
            order_info = None
            check_count = 0

            while True:
                check_count += 1
                try:
                    order_info = await self.check_order_status(short_order['id'], symbol)

                    if order_info['status'] == 'closed':
                        # 记录订单完成时间
                        order_end_time = time.time()
                        execution_time = order_end_time - order_start_time

                        logger.info("=" * 50)
                        logger.info("✅ 开仓订单执行完成")
                        logger.info(f"检查次数: {check_count}")
                        logger.info(f"执行时长: {execution_time:.3f} 秒")
                        logger.info(f"平均检查间隔: {execution_time / check_count:.3f} 秒")
                        logger.info("=" * 50)
                        break
                    else:
                        logger.info(
                            f"订单状态检查 #{check_count}: {order_info['status']}, 已成交: {order_info.get('filled', 0):.6f}")

                except Exception as e:
                    logger.error(f"检查订单状态失败: {e}")

                # 等待0.2秒后继续检查
                await asyncio.sleep(0.2)

            # 10. 下平仓订单
            logger.info("10. 下平仓订单...")

            # 确保订单信息有效
            if order_info is None or order_info['status'] != 'closed':
                logger.error("开仓订单未完全成交，无法继续执行套利策略")
                return

            filled_quantity = order_info['filled']
            avg_price = order_info['average']

            close_order = await self.place_close_order(
                symbol, filled_quantity, avg_price, current_rate
            )

            logger.info("=" * 60)
            logger.info("套利策略执行完成")
            logger.info(f"开仓订单ID: {short_order['id']}")
            logger.info(f"平仓订单ID: {close_order['id']}")
            logger.info("=" * 60)

            # 11. 启动止损监控
            logger.info("11. 启动止损监控...")
            await self.monitor_stop_loss(
                symbol,
                avg_price,
                filled_quantity,
                close_order['id']
            )

        except Exception as e:
            logger.error(f"执行套利策略失败: {e}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            raise


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='多交易所资金费率套利交易脚本')

    parser.add_argument(
        'symbol',
        help='合约交易对符号 (例如: BTC/USDT)'
    )

    parser.add_argument(
        '--exchange',
        choices=['binance', 'gateio', 'bybit', 'bitget'],
        default='binance',
        help='选择交易所 (默认: binance)'
    )

    parser.add_argument(
        '--min-funding-rate',
        type=float,
        default=-0.005,
        help='触发套利的最小资金费率阈值 (默认: -0.005, 即-0.5%%)'
    )

    parser.add_argument(
        '--manual-time',
        help='手动指定检查时间 (ISO格式, 例如: 2024-01-01T08:00:00+00:00)',
        default=None
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='日志级别'
    )

    return parser.parse_args()


async def main():
    """主函数"""
    try:
        # 解析命令行参数
        args = parse_arguments()

        # 设置日志级别
        logging.getLogger().setLevel(getattr(logging, args.log_level))

        # 验证交易对格式
        symbol = args.symbol.upper()
        if '/' not in symbol:
            logger.error("交易对格式错误，应为 BASE/QUOTE 格式 (例如: BTC/USDT)")
            return

        # 创建交易器实例
        trader = FundingRateTrader(args.exchange, args.min_funding_rate, args.manual_time)

        # 执行套利策略
        await trader.execute_arbitrage_strategy(symbol)

    except KeyboardInterrupt:
        logger.info("用户中断程序")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())
