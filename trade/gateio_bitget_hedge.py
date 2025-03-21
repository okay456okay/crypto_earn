#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gate.io现货买入与Bitget合约空单对冲的套利脚本

此脚本实现以下功能：
1. 从Gate.io市价买入指定token的现货
2. 从Bitget开对应的合约空单进行对冲
3. 确保现货和合约仓位保持一致
4. 检查价差是否满足最小套利条件
5. 监控和记录交易执行情况
"""

import sys
import os
import time
import logging
import argparse
from decimal import Decimal
import ccxt.async_support as ccxt  # 使用异步版本的ccxt
import asyncio
import aiohttp

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.logger import logger
from config import bitget_api_key, bitget_api_secret, bitget_api_passphrase, gateio_api_secret, gateio_api_key, proxies


class HedgeTrader:
    """
    现货-合约对冲交易类，实现Gate.io现货买入与Bitget合约空单对冲
    """
    
    def __init__(self, symbol, spot_amount=None, min_spread=0.001, leverage=20):
        """
        初始化对冲交易器
        
        Args:
            symbol (str): 交易对符号，例如 'ETH/USDT'
            spot_amount (float, optional): 购买的现货数量，若为None则使用最大可用USDT
            min_spread (float): 最小价差要求，默认0.001 (0.1%)
            leverage (int): 合约杠杆倍数，默认20倍
        """
        self.symbol = symbol
        self.spot_amount = spot_amount
        self.min_spread = min_spread
        self.leverage = leverage
        
        # 使用异步版本初始化交易所
        self.gateio = ccxt.gateio({
            'apiKey': gateio_api_key,
            'secret': gateio_api_secret,
            'enableRateLimit': True,
            'proxies': proxies,
        })
        
        self.bitget = ccxt.bitget({
            'apiKey': bitget_api_key,
            'secret': bitget_api_secret,
            'password': bitget_api_passphrase,
            'enableRateLimit': True,
            'proxies': proxies,
        })
        
        # 设置合约交易对
        base, quote = symbol.split('/')
        self.contract_symbol = f"{base}/{quote}:{quote}"  # 例如: ETH/USDT:USDT
        
        # 设置Bitget合约参数
        try:
            # 设置合约杠杆
            self.bitget.set_leverage(self.leverage, self.contract_symbol)
            logger.info(f"设置Bitget合约杠杆倍数为: {leverage}倍")
        except Exception as e:
            logger.error(f"设置合约杠杆失败: {str(e)}")
            raise
        
        logger.info(f"初始化完成: 交易对={symbol}, 合约对={self.contract_symbol}, "
                   f"最小价差={min_spread*100}%, 杠杆={leverage}倍")
        
        # 在初始化时获取并保存账户余额
        try:
            self.gateio_usdt, self.bitget_usdt = self.check_balances()
            
            # 提前检查余额是否满足交易要求
            if self.spot_amount is not None:
                # 获取当前市场价格做预估
                orderbook = self.gateio.fetch_order_book(self.symbol)
                current_price = float(orderbook['asks'][0][0])  # 使用卖1价
                
                required_usdt = float(self.spot_amount) * current_price * 1.02  # 加2%作为手续费缓冲
                required_margin = float(self.spot_amount) * current_price / self.leverage * 1.05  # 加5%作为保证金缓冲
                
                if required_usdt > self.gateio_usdt:
                    raise Exception(f"Gate.io USDT余额不足，需要约 {required_usdt:.2f} USDT，当前余额 {self.gateio_usdt:.2f} USDT")
                if required_margin > self.bitget_usdt:
                    raise Exception(f"Bitget USDT保证金不足，需要约 {required_margin:.2f} USDT，当前余额 {self.bitget_usdt:.2f} USDT")
                
                logger.info(f"账户余额检查通过 - 预估所需Gate.io: {required_usdt:.2f} USDT, Bitget: {required_margin:.2f} USDT")
        except Exception as e:
            logger.error(f"初始化账户余额检查失败: {str(e)}")
            raise

    def check_balances(self):
        """
        检查Gate.io和Bitget的账户余额
        
        Returns:
            tuple: (gateio_balance, bitget_balance) - 返回两个交易所的USDT余额
        """
        try:
            # 获取Gate.io现货账户USDT余额
            gateio_balance = self.gateio.fetch_balance()
            gateio_usdt = gateio_balance.get('USDT', {}).get('free', 0)
            
            # 获取Bitget合约账户USDT余额
            bitget_balance = self.bitget.fetch_balance({'type': 'swap'})
            bitget_usdt = bitget_balance.get('USDT', {}).get('free', 0)
            
            logger.info(f"账户余额 - Gate.io: {gateio_usdt} USDT, Bitget: {bitget_usdt} USDT")
            return gateio_usdt, bitget_usdt
            
        except Exception as e:
            logger.error(f"检查余额时出错: {str(e)}")
            raise

    def check_price_spread(self):
        """
        检查Gate.io现货卖1价格和Bitget合约买1价格之间的价差
        
        Returns:
            tuple: (spread_percent, gateio_ask, bitget_bid, gateio_ask_volume, bitget_bid_volume)
                  - 价差百分比, Gate.io卖1价, Bitget买1价, Gate.io卖1量, Bitget买1量
        """
        try:
            # 获取Gate.io现货订单簿
            gateio_orderbook = self.gateio.fetch_order_book(self.symbol)
            gateio_ask = Decimal(str(gateio_orderbook['asks'][0][0]))  # 卖1价
            gateio_ask_volume = Decimal(str(gateio_orderbook['asks'][0][1]))  # 卖1量
            self.gateio.fetch_tickers()
            
            # 获取Bitget合约订单簿
            bitget_orderbook = self.bitget.fetch_order_book(self.contract_symbol)
            bitget_bid = Decimal(str(bitget_orderbook['bids'][0][0]))  # 买1价
            bitget_bid_volume = Decimal(str(bitget_orderbook['bids'][0][1]))  # 买1量
            
            # 计算价差
            spread = bitget_bid - gateio_ask
            spread_percent = spread / gateio_ask
            
            logger.info(f"价格检查 - Gate.io卖1: {gateio_ask} (量: {gateio_ask_volume}), "
                        f"Bitget买1: {bitget_bid} (量: {bitget_bid_volume}), "
                        f"价差: {spread_percent*100:.4f}%")
            
            return float(spread_percent), float(gateio_ask), float(bitget_bid), float(gateio_ask_volume), float(bitget_bid_volume)
            
        except Exception as e:
            logger.error(f"检查价差时出错: {str(e)}")
            raise

    def wait_for_spread(self):
        """
        等待价差达到要求
        
        Returns:
            tuple: (spread_percent, gateio_ask, bitget_bid, gateio_ask_volume, bitget_bid_volume)
        """
        while True:
            spread_data = self.check_price_spread()
            spread_percent = spread_data[0]
            
            if spread_percent >= self.min_spread:
                logger.info(f"价差条件满足: {spread_percent*100:.4f}% >= {self.min_spread*100:.4f}%")
                return spread_data
            
            logger.info(f"价差条件不满足: {spread_percent*100:.4f}% < {self.min_spread*100:.4f}%, 等待1秒后重试...")
            time.sleep(1)

    async def execute_hedge_trade(self):
        """执行对冲交易：在Gate.io买入现货，同时在Bitget开空单"""
        try:
            # 等待价差满足条件
            spread_percent, gateio_ask, bitget_bid, gateio_ask_volume, bitget_bid_volume = \
                await self.wait_for_spread()
            
            trade_amount = self.spot_amount
            base_currency = self.symbol.split('/')[0]
            
            # 使用最新价格重新检查余额是否足够
            required_usdt = float(trade_amount) * float(gateio_ask) * 1.02
            required_margin = float(trade_amount) * float(bitget_bid) / self.leverage * 1.05
            
            if required_usdt > self.gateio_usdt:
                raise Exception(f"Gate.io USDT余额不足，需要 {required_usdt:.2f} USDT，当前余额 {self.gateio_usdt:.2f} USDT")
            if required_margin > self.bitget_usdt:
                raise Exception(f"Bitget USDT保证金不足，需要 {required_margin:.2f} USDT，当前余额 {self.bitget_usdt:.2f} USDT")
            
            logger.info(f"计划交易数量: {trade_amount} {base_currency}")
            
            # 准备下单参数
            cost = float(trade_amount) * float(gateio_ask)
            contract_amount = self.bitget.amount_to_precision(self.contract_symbol, trade_amount)
            
            logger.info(f"在Gate.io市价买入 {trade_amount} {base_currency}, 预估成本: {cost:.2f} USDT")
            logger.info(f"在Bitget市价开空单 {contract_amount} {base_currency}")
            
            # 同时发起两个下单请求
            spot_order_task = self.gateio.create_market_buy_order(
                symbol=self.symbol,
                amount=cost,
                params={
                    'createMarketBuyOrderRequiresPrice': False,
                    'quoteOrderQty': True
                }
            )
            
            contract_order_task = self.bitget.create_market_sell_order(
                symbol=self.contract_symbol,
                amount=contract_amount,
                params={"reduceOnly": False}
            )
            
            # 等待两个订单都完成
            spot_order, contract_order = await asyncio.gather(
                spot_order_task,
                contract_order_task
            )
            
            logger.info(f"Gate.io现货买入订单执行完成: {spot_order}")
            logger.info(f"Bitget合约做空订单执行完成: {contract_order}")
            
            # 获取现货订单的实际成交结果
            filled_amount = float(spot_order['filled'])
            fees = spot_order.get('fees', [])
            base_fee = sum(float(fee['cost']) for fee in fees if fee['currency'] == base_currency)
            actual_position = filled_amount - base_fee
            
            logger.info(f"Gate.io实际成交数量: {filled_amount} {base_currency}, "
                       f"手续费: {base_fee} {base_currency}, "
                       f"实际持仓: {actual_position} {base_currency}")
            
            # 检查持仓情况
            await self.check_positions()
            
            return spot_order, contract_order
            
        except Exception as e:
            logger.error(f"执行对冲交易时出错: {str(e)}")
            raise
        finally:
            # 确保关闭交易所连接
            await asyncio.gather(
                self.gateio.close(),
                self.bitget.close()
            )

    async def check_positions(self):
        """异步检查交易后的持仓情况"""
        try:
            await asyncio.sleep(1)  # 等待订单状态更新
            
            # 并行获取两个交易所的持仓信息
            gateio_balance_task = self.gateio.fetch_balance()
            positions_task = self.bitget.fetch_positions([self.contract_symbol])
            
            gateio_balance, positions = await asyncio.gather(
                gateio_balance_task,
                positions_task
            )
            
            # 获取现货最新成交订单的信息
            base_currency = self.symbol.split('/')[0]
            gateio_position = gateio_balance.get(base_currency, {}).get('total', 0)
            
            # 检查Bitget合约持仓
            contract_position = 0
            
            if positions:
                for position in positions:
                    if position['symbol'] == self.contract_symbol:
                        contract_position = abs(float(position.get('contracts', 0)))
                        position_side = position.get('side', 'unknown')
                        position_leverage = position.get('leverage', self.leverage)
                        position_notional = position.get('notional', 0)
                        
                        logger.info(f"Bitget合约持仓: {position_side} {contract_position} 合约, "
                                  f"杠杆: {position_leverage}倍, 名义价值: {position_notional}")
            else:
                logger.warning("未获取到Bitget合约持仓信息")
            
            logger.info(f"持仓检查 - Gate.io现货: {gateio_position} {base_currency}, "
                       f"Bitget合约: {contract_position} {base_currency}")
            
            # 检查是否平衡（允许0.5%的误差）
            position_diff = abs(float(gateio_position) - float(contract_position))
            position_diff_percent = position_diff / float(gateio_position) * 100
            
            if position_diff_percent > 0.5:  # 允许0.5%的误差
                logger.warning(f"现货和合约持仓不平衡! 差异: {position_diff} {base_currency} ({position_diff_percent:.2f}%)")
            else:
                logger.info(f"现货和合约持仓基本平衡，差异在允许范围内: {position_diff} {base_currency} ({position_diff_percent:.2f}%)")
                
        except Exception as e:
            logger.error(f"获取Bitget合约持仓信息失败: {str(e)}")


def parse_arguments():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='Gate.io现货与Bitget合约对冲交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, required=True, help='购买的现货数量')
    parser.add_argument('-p', '--min-spread', type=float, default=0.001, help='最小价差要求，默认0.001 (0.1%%)')
    parser.add_argument('-l', '--leverage', type=int, default=20, help='合约杠杆倍数，默认20倍')
    return parser.parse_args()


async def main():
    """
    异步主函数
    """
    args = parse_arguments()
    
    try:
        trader = HedgeTrader(
            symbol=args.symbol,
            spot_amount=args.amount,
            min_spread=args.min_spread,
            leverage=args.leverage
        )
        
        spot_order, contract_order = await trader.execute_hedge_trade()
        if spot_order and contract_order:
            logger.info("对冲交易成功完成!")
        else:
            logger.info("未执行对冲交易")
            
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    sys.exit(loop.run_until_complete(main())) 