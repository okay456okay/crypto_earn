#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Bitget合约资金费率套利脚本

此脚本实现以下功能：
1. 监控指定交易对的资金费率
2. 当资金费率低于阈值时（如-0.35%），在结算前平掉空单
3. 在结算后用相同价格重新开空单
4. 记录套利执行情况和盈亏
5. 持续监控运行，直到没有持仓时退出
"""

import sys
import os
import argparse
import asyncio
import ccxt.pro as ccxtpro
from decimal import Decimal
from datetime import datetime, timedelta
import time

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import bitget_api_key, bitget_api_secret, bitget_api_passphrase, proxies


class FundingArbitrageTrader:
    """
    Bitget合约资金费率套利类
    实现在资金费率结算前平仓、结算后重新开仓的套利策略
    """

    def __init__(self, symbol, funding_threshold=-0.0012):
        """
        初始化基本属性
        
        Args:
            symbol (str): 交易对，如 'ETH/USDT'
            funding_threshold (float): 资金费率阈值，默认-0.12%
        """
        self.symbol = symbol
        self.funding_threshold = funding_threshold
        self.leverage = None  # 将在检查持仓时设置

        # 设置合约交易对
        base, quote = symbol.split('/')
        self.contract_symbol = f"{base}/{quote}:{quote}"

        # 初始化交易所连接
        self.exchange = ccxtpro.bitget({
            'apiKey': bitget_api_key,
            'secret': bitget_api_secret,
            'password': bitget_api_passphrase,
            'enableRateLimit': True,
            'proxies': proxies,
            'aiohttp_proxy': proxies.get('https', None),
            'ws_proxy': proxies.get('https', None),
            'wss_proxy': proxies.get('https', None),
            'ws_socks_proxy': proxies.get('https', None),
        })

        # 存储持仓信息
        self.position = None
        self.position_side = None
        self.position_amount = None
        self.entry_price = None
        self.last_position_amount = None

    async def initialize(self):
        """
        初始化交易设置和检查持仓状况
        """
        try:
            # 检查当前持仓
            await self.check_position()
            
            if not self.position_amount or self.position_side != 'short':
                raise ValueError("未检测到空单持仓，请确保已有空单持仓")

            logger.info(f"[{self.symbol}] 初始化完成: 交易对={self.contract_symbol}, "
                       f"资金费率阈值={self.funding_threshold*100}%, 杠杆={self.leverage}倍")

        except Exception as e:
            logger.error(f"[{self.symbol}] 初始化失败: {str(e)}")
            raise

    async def check_position(self):
        """
        检查当前合约持仓状况
        
        Returns:
            bool: 是否有持仓
        """
        try:
            positions = await self.exchange.fetch_positions([self.contract_symbol])
            
            for position in positions:
                if position['symbol'] == self.contract_symbol and float(position.get('contracts', 0)) > 0:
                    self.position = position
                    self.position_side = position['side']
                    self.position_amount = abs(float(position['contracts']))
                    self.entry_price = float(position.get('entryPrice', 0))
                    self.leverage = float(position.get('leverage', 20))  # 从持仓信息中获取杠杆倍数
                    
                    # 设置合约杠杆
                    await self.exchange.set_leverage(self.leverage, self.contract_symbol)
                    logger.info(f"[{self.symbol}] 设置合约杠杆倍数为: {self.leverage}倍")
                    
                    logger.info(f"[{self.symbol}] 当前持仓: {self.position_side} {self.position_amount} 张"
                              f" {self.contract_symbol}, 开仓均价: {self.entry_price}, 杠杆倍数: {self.leverage}倍")
                    return True

            # logger.warning("未检测到持仓")
            self.position = None
            self.position_side = None
            self.position_amount = None
            self.entry_price = None
            self.leverage = None
            return False

        except Exception as e:
            logger.error(f"[{self.symbol}] 检查持仓时出错: {str(e)}")
            raise

    async def get_funding_info(self):
        """
        获取当前资金费率和下次结算时间
        
        Returns:
            tuple: (funding_rate, next_funding_time)
        """
        try:
            funding_data = await self.exchange.fetch_funding_rate(self.contract_symbol)
            
            # 从返回的数据结构中提取资金费率和下次结算时间
            funding_rate = float(funding_data['info']['fundingRate'])
            next_funding_time = int(funding_data['info']['nextUpdate'])
            
            logger.info(f"[{self.symbol}] 当前资金费率: {funding_rate*100:.4f}%, "
                       f"下次结算时间: {datetime.fromtimestamp(next_funding_time/1000)}")
            
            return funding_rate, next_funding_time

        except Exception as e:
            logger.error(f"[{self.symbol}] 获取资金费率信息失败: {str(e)}")
            raise

    async def close_position(self):
        """
        平掉当前空单持仓
        """
        try:
            if not self.position_amount:
                logger.warning(f"[{self.symbol}] 没有持仓需要平仓")
                return None

            # 获取当前市场价格
            ticker = await self.exchange.fetch_ticker(self.contract_symbol)
            close_price = ticker['last']
            
            # 创建平仓订单
            order = await self.exchange.create_market_buy_order(
                symbol=self.contract_symbol,
                amount=self.position_amount,
                params={"reduceOnly": True}  # 确保是平仓操作
            )

            logger.info(f"[{self.symbol}] 平仓订单执行结果: 数量={self.position_amount}, "
                       f"价格≈{close_price}, 订单ID={order.get('id')}")

            # 保存平仓数量用于后续开仓
            self.last_position_amount = self.position_amount

            # 更新持仓状态
            await self.check_position()
            return order

        except Exception as e:
            logger.error(f"[{self.symbol}] 平仓操作失败: {str(e)}")
            raise

    async def open_position(self, price=None):
        """
        开空单
        
        Args:
            price (float, optional): 限价单价格，如果不指定则使用市价单
        """
        try:
            if not hasattr(self, 'last_position_amount') or not self.last_position_amount:
                logger.error(f"[{self.symbol}] 没有可用的持仓数量信息")
                return None

            # 检查当前持仓状态
            await self.check_position()
            
            # 计算需要开仓的数量
            open_amount = self.last_position_amount
            if self.position_amount:
                # 如果已经有持仓，需要调整开仓数量
                open_amount = self.last_position_amount - self.position_amount
                if open_amount <= 0:
                    logger.warning(f"[{self.symbol}] 当前持仓 {self.position_amount} 已超过目标持仓 {self.last_position_amount}，无需开仓")
                    return None
                logger.info(f"[{self.symbol}] 当前已有持仓 {self.position_amount}，将开仓数量调整为 {open_amount}")

            # 获取当前市场价格作为参考
            ticker = await self.exchange.fetch_ticker(self.contract_symbol)
            current_price = ticker['last']

            # 如果指定了价格，使用限价单，否则使用市价单
            order_type = 'limit' if price else 'market'
            order_price = price if price else None
            
            # 创建开仓订单
            order_params = {
                "reduceOnly": False
            }
            
            if order_type == 'limit':
                order = await self.exchange.create_limit_sell_order(
                    symbol=self.contract_symbol,
                    amount=open_amount,
                    price=order_price,
                    params=order_params
                )
            else:
                order = await self.exchange.create_market_sell_order(
                    symbol=self.contract_symbol,
                    amount=open_amount,
                    params=order_params
                )

            logger.info(f"[{self.symbol}] 开仓订单执行结果: 数量={open_amount}, "
                       f"价格={'市价' if not price else price}, 订单ID={order.get('id')}")

            # 更新持仓状态
            await self.check_position()
            
            # 验证最终持仓是否与目标一致
            if self.position_amount != self.last_position_amount:
                logger.warning(f"[{self.symbol}] 最终持仓 {self.position_amount} 与目标持仓 {self.last_position_amount} 不一致")
            
            return order

        except Exception as e:
            logger.error(f"[{self.symbol}] 开仓操作失败: {str(e)}")
            raise

    async def execute_funding_arbitrage(self):
        """
        执行资金费率套利策略的主循环
        """
        try:
            while True:
                # 1. 检查持仓状态
                has_position = await self.check_position()
                if not has_position:
                    logger.info(f"[{self.symbol}] 没有检测到持仓，退出执行")
                    return False

                # 2. 获取下次资金费率结算时间
                funding_rate, next_funding_time = await self.get_funding_info()
                
                # 3. 计算距离下次结算的时间
                # next_funding_time = int(time.time()*1000) + 60*1000  # 测试用
                now = datetime.now().timestamp() * 1000
                time_to_funding = (next_funding_time - now) / 1000  # 转换为秒

                # 如果距离结算时间超过30秒，等待到结算前30秒
                if time_to_funding > 30:
                    wait_time = time_to_funding - 30
                    logger.info(f"[{self.symbol}] 等待 {wait_time:.0f} 秒后检查资金费率")
                    await asyncio.sleep(wait_time)

                # 4. 再次检查持仓状态
                has_position = await self.check_position()
                if not has_position:
                    logger.info(f"[{self.symbol}] 没有检测到持仓，退出执行")
                    return False

                # 重新获取资金费率信息
                funding_rate, _ = await self.get_funding_info()

                # 检查是否满足套利条件
                if funding_rate > self.funding_threshold:
                    logger.info(f"[{self.symbol}] 当前资金费率 {funding_rate*100:.4f}% > {self.funding_threshold*100:.4f}%, "
                              "不满足套利条件，等待300秒后重试")
                    await asyncio.sleep(300)
                    continue

                # 4. 等待到结算前30秒
                now = datetime.now().timestamp() * 1000
                time_to_funding = (next_funding_time - now) / 1000
                if time_to_funding > 30:
                    wait_time = time_to_funding - 30
                    logger.info(f"[{self.symbol}] 等待 {wait_time:.0f} 秒后平仓")
                    await asyncio.sleep(wait_time)

                # 记录平仓前的价格
                ticker = await self.exchange.fetch_ticker(self.contract_symbol)
                close_price = ticker['last']

                # 执行平仓
                close_order = await self.close_position()
                if not close_order:
                    logger.error(f"[{self.symbol}] 平仓失败")
                    await asyncio.sleep(300)
                    continue

                # 5. 等待资金费率结算完成（额外等待30秒以确保结算完成）
                logger.info(f"[{self.symbol}] 等待资金费率结算完成...")
                await asyncio.sleep(60)  # 等待60秒

                # 6. 以相同价格重新开空单
                logger.info(f"[{self.symbol}] 准备以价格 {close_price} 重新开空单")
                open_order = await self.open_position(price=close_price)
                
                if open_order:
                    logger.info(f"[{self.symbol}] 本轮资金费率套利执行完成")
                else:
                    logger.error(f"[{self.symbol}] 重新开仓失败")

                # 等待300秒后继续下一轮检查
                logger.info(f"[{self.symbol}] 等待300秒后开始下一轮检查")
                await asyncio.sleep(3400)

        except Exception as e:
            logger.error(f"[{self.symbol}] 执行资金费率套利时出错: {str(e)}")
            raise


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Bitget合约资金费率套利')
    parser.add_argument('-s', '--symbol', type=str, required=True,
                      help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-t', '--threshold', type=float, default=-0.0012,
                      help='资金费率阈值，默认-0.12%%')
    return parser.parse_args()


async def main():
    """主函数"""
    args = parse_arguments()

    try:
        # 创建交易器实例
        trader = FundingArbitrageTrader(
            symbol=args.symbol,
            funding_threshold=args.threshold
        )
        
        # 初始化
        await trader.initialize()
        
        # 执行套利循环
        await trader.execute_funding_arbitrage()
        
        logger.info("程序执行完成")
        return 0

    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        return 1
    finally:
        if 'trader' in locals():
            await trader.exchange.close()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        sys.exit(loop.run_until_complete(main()))
    finally:
        loop.close() 