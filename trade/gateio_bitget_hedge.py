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

# 添加项目根目录到系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trade.ccxt_exchange import CCXTExchange

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('gateio_bitget_hedge.log')
    ]
)
logger = logging.getLogger(__name__)


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
        
        # 初始化交易所接口
        self.gateio = CCXTExchange(exchange_id='gateio')
        self.bitget = CCXTExchange(exchange_id='bitget')
        
        # 设置合约交易对
        base, quote = symbol.split('/')
        self.contract_symbol = f"{base}/{quote}:{quote}"  # 例如: ETH/USDT:USDT
        
        # 为Bitget设置合约参数
        self.bitget.set_contract_trading_symbols([self.contract_symbol])
        self.bitget.set_leverage_mode(self.contract_symbol, leverage=self.leverage)
        
        logger.info(f"初始化完成: 交易对={symbol}, 合约对={self.contract_symbol}, 最小价差={min_spread*100}%, 杠杆={leverage}倍")

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
            bitget_balance = self.bitget.fetch_balance(params={"type": "swap"})
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

    def execute_hedge_trade(self):
        """
        执行对冲交易：在Gate.io买入现货，同时在Bitget开空单
        
        Returns:
            tuple: (spot_order, contract_order) - 现货订单和合约订单信息
        """
        try:
            # 检查价差是否满足最小要求
            spread_percent, gateio_ask, bitget_bid, gateio_ask_volume, bitget_bid_volume = self.check_price_spread()
            if spread_percent < self.min_spread:
                logger.info(f"价差 {spread_percent*100:.4f}% 小于最小要求 {self.min_spread*100:.4f}%，取消交易")
                return None, None
            
            # 检查账户余额
            gateio_usdt, bitget_usdt = self.check_balances()
            
            # 计算交易数量
            if self.spot_amount is None:
                # 如果未指定数量，计算最大可用数量
                max_spot_amount = gateio_usdt / gateio_ask * 0.98  # 留出2%作为手续费缓冲
                max_contract_amount = (bitget_usdt * self.leverage) / bitget_bid * 0.95  # 合约留出5%作为保证金缓冲
                
                # 取较小值作为交易数量
                trade_amount = min(max_spot_amount, max_contract_amount, gateio_ask_volume, bitget_bid_volume)
                trade_amount = self.gateio.amount_to_precision(self.symbol, trade_amount)
            else:
                trade_amount = self.spot_amount
            
            logger.info(f"计划交易数量: {trade_amount} {self.symbol.split('/')[0]}")
            
            # 执行Gate.io现货市价买入
            logger.info(f"在Gate.io市价买入 {trade_amount} {self.symbol.split('/')[0]}")
            spot_order = self.gateio.create_market_buy_order(
                symbol=self.symbol,
                amount=trade_amount
            )
            logger.info(f"Gate.io现货买入订单执行完成: {spot_order}")
            
            # 执行Bitget合约市价做空
            logger.info(f"在Bitget市价开空单 {trade_amount} {self.contract_symbol.split('/')[0]}")
            contract_order = self.bitget.create_market_sell_order(
                symbol=self.contract_symbol,
                amount=trade_amount,
                params={"reduceOnly": False}
            )
            logger.info(f"Bitget合约做空订单执行完成: {contract_order}")
            
            # 检查交易后的账户状态
            self.check_positions()
            
            return spot_order, contract_order
            
        except Exception as e:
            logger.error(f"执行对冲交易时出错: {str(e)}")
            raise

    def check_positions(self):
        """
        检查交易后的持仓情况，确认现货和合约仓位是否平衡
        """
        try:
            # 检查Gate.io现货持仓
            gateio_balance = self.gateio.fetch_balance()
            base_currency = self.symbol.split('/')[0]
            gateio_position = gateio_balance.get(base_currency, {}).get('total', 0)
            
            # 检查Bitget合约持仓
            positions = self.bitget.fetch_positions([self.contract_symbol])
            contract_position = 0
            
            for position in positions:
                if position['symbol'] == self.contract_symbol:
                    contract_position = abs(float(position['contracts']))
                    position_side = position['side']
                    position_leverage = position['leverage']
                    position_cost = position['cost']
                    logger.info(f"Bitget合约持仓: {position_side} {contract_position} 合约, "
                                f"杠杆: {position_leverage}倍, 保证金: {position_cost}")
            
            logger.info(f"持仓检查 - Gate.io现货: {gateio_position} {base_currency}, "
                        f"Bitget合约: {contract_position} {base_currency}")
            
            # 检查是否平衡
            position_diff = abs(gateio_position - contract_position)
            if position_diff > 0.01:  # 允许0.01的误差
                logger.warning(f"现货和合约持仓不平衡! 差异: {position_diff} {base_currency}")
            else:
                logger.info("现货和合约持仓平衡")
                
        except Exception as e:
            logger.error(f"检查持仓时出错: {str(e)}")
            raise


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
        
        # 检查价差
        spread_percent, *_ = trader.check_price_spread()
        
        if spread_percent >= args.min_spread:
            # 执行对冲交易
            spot_order, contract_order = trader.execute_hedge_trade()
            if spot_order and contract_order:
                logger.info("对冲交易成功完成!")
                trader.check_positions()
            else:
                logger.info("未执行对冲交易")
        else:
            logger.info(f"价差 {spread_percent*100:.4f}% 小于最小要求 {args.min_spread*100:.4f}%，不执行交易")
        
    except Exception as e:
        logger.error(f"程序执行过程中发生错误: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main()) 