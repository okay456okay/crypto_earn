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
import ccxt  # 直接导入ccxt库

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
        
        # 直接使用ccxt初始化交易所
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

    def execute_hedge_trade(self):
        """执行对冲交易：在Gate.io买入现货，同时在Bitget开空单"""
        try:
            # 等待价差满足条件
            spread_percent, gateio_ask, bitget_bid, gateio_ask_volume, bitget_bid_volume = \
                self.wait_for_spread()
            
            # 检查账户余额
            gateio_usdt, bitget_usdt = self.check_balances()
            
            # 计算交易数量
            if self.spot_amount is None:
                # 如果未指定数量，计算最大可用数量
                max_spot_amount = gateio_usdt / gateio_ask * 0.98  # 留出2%作为手续费缓冲
                max_contract_amount = (bitget_usdt * self.leverage) / bitget_bid * 0.95  # 合约留出5%保证金
                
                # 取较小值作为交易数量
                trade_amount = min(max_spot_amount, max_contract_amount, 
                                 gateio_ask_volume, bitget_bid_volume)
                trade_amount = self.gateio.amount_to_precision(self.symbol, trade_amount)
            else:
                trade_amount = self.spot_amount
            
            logger.info(f"计划交易数量: {trade_amount} {self.symbol.split('/')[0]}")
            
            # 执行Gate.io现货市价买入
            cost = float(trade_amount) * float(gateio_ask)
            logger.info(f"在Gate.io市价买入 {trade_amount} {self.symbol.split('/')[0]}, 预估成本: {cost} USDT")
            
            spot_order = self.gateio.create_market_buy_order(
                symbol=self.symbol,
                amount=cost,
                params={
                    'createMarketBuyOrderRequiresPrice': False,
                    'quoteOrderQty': True
                }
            )
            logger.info(f"Gate.io现货买入订单执行完成: {spot_order}")
            
            # 获取实际成交数量和手续费
            filled_amount = float(spot_order['filled'])
            fees = spot_order.get('fees', [])
            base_currency = self.symbol.split('/')[0]
            base_fee = sum(float(fee['cost']) for fee in fees if fee['currency'] == base_currency)
            actual_position = filled_amount - base_fee
            
            logger.info(f"Gate.io实际成交数量: {filled_amount} {base_currency}, "
                       f"手续费: {base_fee} {base_currency}, "
                       f"实际持仓: {actual_position} {base_currency}")
            
            # 计算合约开仓数量，考虑合约费率
            contract_fee_rate = 0.001  # Bitget USDT合约费率，请根据实际费率调整
            contract_amount = actual_position * (1 + contract_fee_rate)  # 略微增加合约数量以补偿手续费
            
            # 确保合约数量符合最小精度要求
            contract_amount = self.bitget.amount_to_precision(self.contract_symbol, contract_amount)
            
            # 执行Bitget合约市价做空
            logger.info(f"在Bitget市价开空单 {contract_amount} {self.contract_symbol.split('/')[0]}")
            
            contract_order = self.bitget.create_market_sell_order(
                symbol=self.contract_symbol,
                amount=contract_amount,
                params={"reduceOnly": False}
            )
            logger.info(f"Bitget合约做空订单执行完成: {contract_order}")
            
            # 等待订单完成
            time.sleep(2)
            
            # 检查交易后的账户状态
            try:
                self.check_positions()
            except Exception as e:
                logger.error(f"检查持仓状态时出错: {str(e)}")
            
            return spot_order, contract_order
            
        except Exception as e:
            logger.error(f"执行对冲交易时出错: {str(e)}")
            raise

    def check_positions(self):
        """检查交易后的持仓情况，确认现货和合约仓位是否平衡"""
        try:
            # 等待一下确保订单完成
            time.sleep(1)
            
            # 获取现货最新成交订单的信息
            base_currency = self.symbol.split('/')[0]
            gateio_balance = self.gateio.fetch_balance()
            gateio_position = gateio_balance.get(base_currency, {}).get('total', 0)
            
            # 检查Bitget合约持仓
            try:
                positions = self.bitget.fetch_positions([self.contract_symbol])
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
                
        except Exception as e:
            logger.error(f"检查持仓时出错: {str(e)}")


def parse_arguments():
    """
    解析命令行参数
    """
    parser = argparse.ArgumentParser(description='Gate.io现货与Bitget合约对冲交易')
    parser.add_argument('-s', '--symbol', type=str, required=True, help='交易对符号，例如 ETH/USDT')
    parser.add_argument('-a', '--amount', type=float, help='购买的现货数量，不指定则使用最大可用USDT')
    parser.add_argument('-p', '--min-spread', type=float, default=0.001, help='最小价差要求，默认0.001 (0.1%%)')
    parser.add_argument('-l', '--leverage', type=int, default=20, help='合约杠杆倍数，默认20倍')
    return parser.parse_args()


def main():
    """
    主函数
    """
    args = parse_arguments()
    
    try:
        # 初始化对冲交易器
        trader = HedgeTrader(
            symbol=args.symbol,
            spot_amount=args.amount,
            min_spread=args.min_spread,
            leverage=args.leverage
        )
        
        # 检查初始账户状态
        trader.check_balances()
        
        # 执行对冲交易（包含价差检查和重试逻辑）
        spot_order, contract_order = trader.execute_hedge_trade()
        if spot_order and contract_order:
            logger.info("对冲交易成功完成!")
            # 最后再检查一次持仓状态
            time.sleep(2)  # 多等待一会确保订单状态已更新
            try:
                trader.check_positions()
            except Exception as e:
                logger.error(f"最终检查持仓状态时出错: {str(e)}")
        else:
            logger.info("未执行对冲交易")
        
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main()) 