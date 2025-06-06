#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance资金费率套利交易脚本
功能：自动检测资金费率机会并执行套利交易
作者：加密货币套利专家
版本：1.0.0
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
    from config import binance_api_key, binance_api_secret, proxies
except ImportError:
    print("警告: 无法导入配置文件，请确保config.py存在并包含API密钥")
    binance_api_key = ""
    binance_api_secret = ""
    proxies = {}


# 配置日志
def setup_logging():
    """设置日志配置"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(log_dir, f'binance_funding_trader_{datetime.now().strftime("%Y%m%d")}.log')

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


class BinanceFundingRateTrader:
    """Binance资金费率交易器"""

    def __init__(self, test_mode: bool = False):
        """
        初始化交易器
        
        Args:
            test_mode: 是否为测试模式（使用模拟环境）
        """
        self.test_mode = test_mode
        self.exchange = None
        self.symbol = None
        self.position_info = {}
        self.market_info = {}

        # 交易参数
        self.min_funding_rate = -0.005  # -0.5%
        self.max_leverage = 20
        self.min_order_amount = 100  # USDT
        self.funding_rate_buffer = 0.005  # 0.5% 缓冲

        self._initialize_exchange()

    def _initialize_exchange(self):
        """初始化交易所连接"""
        try:
            config = {
                'apiKey': binance_api_key,
                'secret': binance_api_secret,
                'sandbox': self.test_mode,
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'future',  # 使用合约交易
                }
            }

            if proxies:
                config['proxies'] = proxies

            self.exchange = ccxt.binance(config)

            # 测试连接
            self.exchange.load_markets()
            logger.info(f"交易所连接成功 {'(测试模式)' if self.test_mode else '(实盘模式)'}")

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
            # 获取资金费率
            funding_rate_info = self.exchange.fetch_funding_rate(symbol)

            # 获取下次结算时间
            funding_time = funding_rate_info['fundingDatetime']
            next_funding_time = datetime.fromisoformat(funding_time.replace('Z', '+00:00'))

            # 获取当前资金费率
            current_funding_rate = funding_rate_info['fundingRate']

            logger.info(f"交易对: {symbol}")
            logger.info(f"当前资金费率: {current_funding_rate:.6f} ({current_funding_rate * 100:.4f}%)")
            logger.info(f"下次结算时间: {next_funding_time}")

            return {
                'symbol': symbol,
                'funding_rate': current_funding_rate,
                'next_funding_time': next_funding_time,
                'funding_time': funding_time
            }

        except Exception as e:
            logger.error(f"获取资金费率信息失败: {e}")
            raise

    async def get_market_info(self, symbol: str) -> Dict[str, Any]:
        """
        获取市场信息（最大杠杆、交易量等）
        
        Args:
            symbol: 交易对符号
            
        Returns:
            市场信息字典
        """
        try:
            # 获取市场信息
            market = self.exchange.market(symbol)

            # 获取24小时交易量
            ticker = self.exchange.fetch_ticker(symbol)
            volume_24h = ticker['quoteVolume']  # USDT计价的交易量

            # 获取交易对的最大杠杆倍数
            # 注意：ccxt可能不直接提供这个信息，我们设置一个默认值
            max_leverage = market.get('info', {}).get('maxLeverage', 125)
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

    async def wait_until_funding_time(self, next_funding_time: datetime, seconds_before: int, manual_time: Optional[str] = None):
        """
        等待到资金费率结算前指定秒数的时间点
        
        Args:
            next_funding_time: 下次资金费率结算时间
            seconds_before: 提前多少秒（例如：15表示结算前15秒）
            manual_time: 手动指定的时间（用于测试）
        """
        if manual_time:
            if seconds_before == 5:
                # 下单时间在测试模式下立即执行
                logger.info("测试模式: 立即执行下单")
                return
            else:
                # 检查时间使用手动指定时间
                target_time = datetime.fromisoformat(manual_time)
                logger.info(f"使用手动指定时间: {target_time}")
        else:
            target_time = next_funding_time

        current_time = datetime.now(target_time.tzinfo)
        wait_seconds = (target_time - current_time - timedelta(seconds=seconds_before)).total_seconds()

        if wait_seconds > 0:
            action_desc = "检查条件" if seconds_before == 15 else "下单"
            logger.info(f"等待 {wait_seconds:.1f} 秒到{action_desc}时间（结算前{seconds_before}秒）: {target_time}")
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
            if not self.test_mode:
                result = self.exchange.set_leverage(leverage, symbol)
                logger.info(f"设置杠杆倍数成功: {leverage}x")
                return result
            else:
                logger.info(f"测试模式: 模拟设置杠杆倍数 {leverage}x")
                return {'leverage': leverage}

        except Exception as e:
            logger.error(f"设置杠杆倍数失败: {e}")
            raise

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
            # 获取当前价格
            ticker = self.exchange.fetch_ticker(symbol)
            current_price = ticker['last']

            # 计算数量（基于USDT金额）
            quantity = amount_usdt / current_price

            if not self.test_mode:
                # 下市价空单
                order = self.exchange.create_market_sell_order(symbol, quantity, params={'positionSide': 'SHORT'})
                logger.info(f"空单下单成功:")
                logger.info(f"订单ID: {order['id']}")
                logger.info(f"交易对: {symbol}")
                logger.info(f"数量: {quantity:.6f}")
                logger.info(f"预估价格: {current_price:.4f}")
                logger.info(f"预估金额: {amount_usdt:.2f} USDT")

                return order
            else:
                # 测试模式
                mock_order = {
                    'id': f'test_{int(time.time())}',
                    'symbol': symbol,
                    'amount': quantity,
                    'price': current_price,
                    'side': 'sell',
                    'type': 'market',
                    'status': 'closed',
                    'filled': quantity,
                    'cost': amount_usdt,
                    'timestamp': int(time.time() * 1000)
                }
                logger.info(f"测试模式: 模拟空单下单成功")
                logger.info(f"订单ID: {mock_order['id']}")
                logger.info(f"数量: {quantity:.6f}")
                logger.info(f"价格: {current_price:.4f}")

                return mock_order

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
            if not self.test_mode:
                order_info = self.exchange.fetch_order(order_id, symbol)

                logger.info(f"订单状态检查:")
                logger.info(f"订单ID: {order_id}")
                logger.info(f"状态: {order_info['status']}")
                logger.info(f"已成交数量: {order_info.get('filled', 0):.6f}")
                logger.info(f"平均成交价格: {order_info.get('average', 0):.4f}")

                return order_info
            else:
                # 测试模式返回模拟订单信息
                logger.info("测试模式: 模拟订单状态检查完成")
                return {
                    'id': order_id,
                    'status': 'closed',
                    'filled': 0.001,
                    'average': 50000.0,
                    'cost': 50.0
                }

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
            # 计算平仓价格: 开仓价格 * (1 + 资金费率 - 0.5%)
            close_price = open_price * (1 + funding_rate - self.funding_rate_buffer)

            logger.info(f"计算平仓价格:")
            logger.info(f"开仓价格: {open_price:.4f}")
            logger.info(f"资金费率: {funding_rate:.6f}")
            logger.info(f"平仓价格: {close_price:.4f}")

            if not self.test_mode:
                # 下限价买入平仓单
                order = self.exchange.create_limit_buy_order(
                    symbol, quantity, close_price,
                    params={
                        "positionSide": "SHORT"  # 指定是平空单
                    })

                logger.info(f"平仓订单下单成功:")
                logger.info(f"订单ID: {order['id']}")
                logger.info(f"类型: 限价买入")
                logger.info(f"数量: {quantity:.6f}")
                logger.info(f"价格: {close_price:.4f}")

                return order
            else:
                # 测试模式
                mock_order = {
                    'id': f'close_test_{int(time.time())}',
                    'symbol': symbol,
                    'amount': quantity,
                    'price': close_price,
                    'side': 'buy',
                    'type': 'limit',
                    'status': 'open',
                    'filled': 0,
                    'timestamp': int(time.time() * 1000)
                }

                logger.info("测试模式: 模拟平仓订单下单成功")
                logger.info(f"订单ID: {mock_order['id']}")
                logger.info(f"价格: {close_price:.4f}")

                return mock_order

        except Exception as e:
            logger.error(f"下平仓订单失败: {e}")
            raise

    async def execute_arbitrage_strategy(self, symbol: str, manual_time: Optional[str] = None):
        """
        执行套利策略
        
        Args:
            symbol: 交易对符号
            manual_time: 手动指定时间（测试用）
        """
        try:
            logger.info("=" * 60)
            logger.info("开始执行Binance资金费率套利策略")
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
            await self.wait_until_funding_time(funding_info['next_funding_time'], 15, manual_time)

            # 4. 检查资金费率条件
            logger.info("4. 检查资金费率条件...")
            condition_met, current_rate = await self.check_funding_rate_condition(symbol)

            if not condition_met:
                logger.info("资金费率条件不满足，退出策略")
                return

            # 5. 计算订单参数
            logger.info("5. 计算订单参数...")
            leverage, order_amount = await self.calculate_order_size(
                symbol, self.market_info['volume_per_minute']
            )

            # 6. 设置杠杆
            logger.info("6. 设置杠杆倍数...")
            await self.set_leverage(symbol, leverage)

            # 7. 等待到下单时间（结算前5秒）
            logger.info("7. 等待到下单时间（结算前5秒）...")
            await self.wait_until_funding_time(funding_info['next_funding_time'], 5, manual_time)

            # 8. 下空单
            logger.info("8. 下空单...")
            short_order = await self.place_short_order(symbol, order_amount)

            # 9. 等待3秒后检查订单状态
            logger.info("9. 等待3秒后检查订单状态...")
            await asyncio.sleep(3)

            order_info = await self.check_order_status(short_order['id'], symbol)

            if order_info['status'] != 'closed':
                logger.warning("开仓订单未完全成交，请手动处理")
                return

            # 10. 下平仓订单
            logger.info("10. 下平仓订单...")
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

        except Exception as e:
            logger.error(f"执行套利策略失败: {e}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            raise


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Binance资金费率套利交易脚本')

    parser.add_argument(
        'symbol',
        help='合约交易对符号 (例如: BTC/USDT)'
    )

    parser.add_argument(
        '--manual-time',
        help='手动指定检查时间 (ISO格式, 例如: 2024-01-01T08:00:00+00:00)',
        default=None
    )

    parser.add_argument(
        '--test-mode',
        action='store_true',
        help='启用测试模式（使用沙盒环境）'
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
        trader = BinanceFundingRateTrader(test_mode=args.test_mode)

        # 执行套利策略
        await trader.execute_arbitrage_strategy(symbol, args.manual_time)

    except KeyboardInterrupt:
        logger.info("用户中断程序")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())
