import time
import logging
import ccxt
# from dotenv import load_dotenv
import numpy as np


import sys
import os

# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将 config.py 所在的目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))

from config import (
    proxies, bitget_api_key, bitget_api_secret, bitget_api_passphrase,
    gateio_api_secret, gateio_api_key, binance_api_key, binance_api_secret,
    # 需要在config.py中添加以下密钥
    # okx_api_key, okx_api_secret, okx_api_passphrase,
    # bybit_api_key, bybit_api_secret
)
from tools.logger import logger


# 加载环境变量
# load_dotenv()

# 交易配置
SYMBOL = "PROS/USDT"
QUANTITY = 100
THRESHOLD = 0.01  # 价格差异阈值
RETRY_DELAY = 5  # 重试延迟(秒)
LEVERAGE = 10  # 杠杆倍数
MARGIN_MODE = "cross"  # 全仓模式

# 新增交易配置
SPOT_EXCHANGE = "gateio"  # 现货交易所: gateio, bitget, bybit, binance, okx
FUTURE_EXCHANGE = "bitget"  # 合约交易所: gateio, bitget, bybit, binance, okx

# 交易类型配置:
# - "spot_buy_future_short"：
#   1. 在现货市场买入资产(做多)
#   2. 同时在合约市场开空单(做空)
#   3. 赚取的是"合约卖出价格 > 现货买入价格"的价差
#   4. 这种模式下资产价格涨跌风险对冲，主要赚取价差套利
#
# - "spot_sell_future_cover"：
#   1. 在现货市场卖出已持有的资产
#   2. 同时在合约市场买入平仓(之前已开空单)
#   3. 赚取的是"现货卖出价格 > 合约买入平仓价格"的价差
#   4. 这种模式是对冲策略的平仓操作，实现利润
TRADE_TYPE = "spot_buy_future_short"  # 交易类型: spot_buy_future_short, spot_sell_future_cover


# 初始化交易所API
def init_exchanges():
    try:
        exchanges = {}
        
        # GateIO配置
        if "gateio" in [SPOT_EXCHANGE, FUTURE_EXCHANGE]:
            exchanges["gateio"] = ccxt.gateio({
                'apiKey': gateio_api_key,
                'secret': gateio_api_secret,
                'enableRateLimit': True,
                'proxies': proxies,
            })

        # Bitget配置
        if "bitget" in [SPOT_EXCHANGE, FUTURE_EXCHANGE]:
            exchanges["bitget"] = ccxt.bitget({
                'apiKey': bitget_api_key,
                'secret': bitget_api_secret,
                'password': bitget_api_passphrase,
                'enableRateLimit': True,
                'proxies': proxies,
            })
            
        # Binance配置
        if "binance" in [SPOT_EXCHANGE, FUTURE_EXCHANGE]:
            exchanges["binance"] = ccxt.binance({
                'apiKey': binance_api_key,
                'secret': binance_api_secret,
                'enableRateLimit': True,
                'proxies': proxies,
                'options': {
                    'defaultType': 'future', # 使用合约API
                }
            })
            
        # OKX配置
        if "okx" in [SPOT_EXCHANGE, FUTURE_EXCHANGE]:
            exchanges["okx"] = ccxt.okx({
                'apiKey': okx_api_key,
                'secret': okx_api_secret,
                'password': okx_api_passphrase,
                'enableRateLimit': True,
                'proxies': proxies,
            })
            
        # Bybit配置
        if "bybit" in [SPOT_EXCHANGE, FUTURE_EXCHANGE]:
            exchanges["bybit"] = ccxt.bybit({
                'apiKey': bybit_api_key,
                'secret': bybit_api_secret,
                'enableRateLimit': True,
                'proxies': proxies,
            })

        logger.info("交易所API初始化成功")
        return exchanges
    except Exception as e:
        logger.error(f"初始化交易所API失败: {e}")
        raise


# 获取合约交易对
def get_contract_symbol(exchange_id, symbol):
    """根据不同交易所获取对应的合约交易对格式"""
    if exchange_id == "bitget":
        return f"{symbol}:USDT"
    elif exchange_id == "binance":
        # 移除'/'并转换为大写
        return symbol.replace('/', '')
    elif exchange_id == "okx":
        # OKX需要特殊处理
        base, quote = symbol.split('/')
        return f"{base}-{quote}-SWAP"
    elif exchange_id == "bybit" or exchange_id == "gateio":
        # Bybit和GateIO使用相同的合约格式
        return symbol
    return symbol


# 设置合约交易模式和杠杆
def setup_contract_settings(exchange, exchange_id, symbol):
    try:
        # 获取特定交易所的合约交易对格式
        contract_symbol = get_contract_symbol(exchange_id, symbol)

        # 针对不同交易所设置保证金模式和杠杆
        if exchange_id == "bitget":
            # 设置保证金模式为全仓
            exchange.set_margin_mode(MARGIN_MODE, contract_symbol)
            logger.info(f"已设置{exchange_id} {contract_symbol}为{MARGIN_MODE}模式")
            
            # 设置杠杆倍数
            exchange.set_leverage(LEVERAGE, contract_symbol)
            logger.info(f"已设置{exchange_id} {contract_symbol}杠杆为{LEVERAGE}倍")
            
        elif exchange_id == "binance":
            # Binance特有的设置方式
            exchange.set_margin_mode(MARGIN_MODE, contract_symbol)
            exchange.set_leverage(LEVERAGE, contract_symbol)
            logger.info(f"已设置{exchange_id} {contract_symbol}为{MARGIN_MODE}模式, 杠杆为{LEVERAGE}倍")
            
        elif exchange_id == "okx":
            # OKX特有的设置方式
            exchange.set_leverage(LEVERAGE, contract_symbol, params={"marginMode": MARGIN_MODE})
            logger.info(f"已设置{exchange_id} {contract_symbol}为{MARGIN_MODE}模式, 杠杆为{LEVERAGE}倍")
            
        elif exchange_id == "bybit":
            # Bybit特有的设置方式
            exchange.set_leverage(LEVERAGE, contract_symbol)
            exchange.set_margin_mode(MARGIN_MODE, contract_symbol)
            logger.info(f"已设置{exchange_id} {contract_symbol}为{MARGIN_MODE}模式, 杠杆为{LEVERAGE}倍")
            
        elif exchange_id == "gateio":
            # GateIO合约设置
            params = {
                'leverage': LEVERAGE,
                'marginMode': MARGIN_MODE,
            }
            exchange.set_leverage(LEVERAGE, contract_symbol, params=params)
            logger.info(f"已设置{exchange_id} {contract_symbol}为{MARGIN_MODE}模式, 杠杆为{LEVERAGE}倍")

        return True
    except Exception as e:
        logger.error(f"设置{exchange_id}合约交易参数失败: {e}")
        return False


# 计算加权平均价格 - 考虑订单簿深度
def calculate_weighted_price(orderbook, quantity, side):
    """
    计算考虑订单簿深度的加权平均价格

    Args:
        orderbook: 订单簿数据
        quantity: 需要交易的数量
        side: 'asks'表示买入(考虑卖单),'bids'表示卖出(考虑买单)

    Returns:
        加权平均价格, 是否有足够的深度
    """
    total_quantity = 0
    weighted_sum = 0

    for price, available_quantity in orderbook[side]:
        volume_to_use = min(available_quantity, quantity - total_quantity)
        weighted_sum += price * volume_to_use
        total_quantity += volume_to_use

        if total_quantity >= quantity:
            # 有足够的深度满足交易需求
            return weighted_sum / quantity, True

    # 订单簿深度不足
    if total_quantity > 0:
        return weighted_sum / total_quantity, False
    else:
        return None, False


# 获取价格 - 考虑订单簿深度
def get_prices_with_depth(exchanges, symbol):
    try:
        spot_exchange = exchanges[SPOT_EXCHANGE]
        future_exchange = exchanges[FUTURE_EXCHANGE]
        
        if TRADE_TYPE == "spot_buy_future_short":
            # 现货买入，合约卖空
            # 获取现货订单簿，计算买入加权价格
            spot_orderbook = spot_exchange.fetch_order_book(symbol, 50)
            spot_price, spot_enough_depth = calculate_weighted_price(
                spot_orderbook, QUANTITY, 'asks'  # 买入需要看卖单asks
            )
            
            # 获取合约订单簿，计算卖出加权价格
            contract_symbol = get_contract_symbol(FUTURE_EXCHANGE, symbol)
            future_orderbook = future_exchange.fetch_order_book(contract_symbol, 50)
            
            # 由于杠杆，实际合约数量可能不同
            contract_quantity = QUANTITY / LEVERAGE
            future_price, future_enough_depth = calculate_weighted_price(
                future_orderbook, contract_quantity, 'bids'  # 卖出需要看买单bids
            )
            
            logger.info(f"{SPOT_EXCHANGE}加权买入价格: {spot_price}, 深度足够: {spot_enough_depth}")
            logger.info(f"{FUTURE_EXCHANGE}加权卖出价格: {future_price}, 深度足够: {future_enough_depth}")
            
        elif TRADE_TYPE == "spot_sell_future_cover":
            # 现货卖出，合约平仓
            # 获取现货订单簿，计算卖出加权价格
            spot_orderbook = spot_exchange.fetch_order_book(symbol, 50)
            spot_price, spot_enough_depth = calculate_weighted_price(
                spot_orderbook, QUANTITY, 'bids'  # 卖出需要看买单bids
            )
            
            # 获取合约订单簿，计算买入平仓加权价格
            contract_symbol = get_contract_symbol(FUTURE_EXCHANGE, symbol)
            future_orderbook = future_exchange.fetch_order_book(contract_symbol, 50)
            
            # 由于杠杆，实际合约数量可能不同
            contract_quantity = QUANTITY / LEVERAGE
            future_price, future_enough_depth = calculate_weighted_price(
                future_orderbook, contract_quantity, 'asks'  # 买入平仓需要看卖单asks
            )
            
            logger.info(f"{SPOT_EXCHANGE}加权卖出价格: {spot_price}, 深度足够: {spot_enough_depth}")
            logger.info(f"{FUTURE_EXCHANGE}加权买入价格(平仓): {future_price}, 深度足够: {future_enough_depth}")

        if not spot_enough_depth:
            logger.warning(f"{SPOT_EXCHANGE}订单簿深度不足以满足{QUANTITY}的交易量")

        if not future_enough_depth:
            logger.warning(f"{FUTURE_EXCHANGE}订单簿深度不足以满足{contract_quantity}的交易量")

        return spot_price, future_price, spot_enough_depth and future_enough_depth
    except Exception as e:
        logger.error(f"获取深度价格失败: {e}")
        return None, None, False


# 计算套利价差百分比
def calculate_price_difference(spot_price, future_price):
    if spot_price is None or future_price is None:
        return None

    if TRADE_TYPE == "spot_buy_future_short":
        # 现货买入, 合约卖空, 预期future_price > spot_price才有利可图
        difference = (future_price - spot_price) / spot_price * 100
    elif TRADE_TYPE == "spot_sell_future_cover":
        # 现货卖出, 合约平仓买入, 预期spot_price > future_price才有利可图
        difference = (spot_price - future_price) / future_price * 100
    
    logger.info(f"价格差异: {difference:.4f}%")
    return difference


# 执行交易
def execute_trades(exchanges, symbol, spot_price, future_price):
    try:
        spot_exchange = exchanges[SPOT_EXCHANGE]
        future_exchange = exchanges[FUTURE_EXCHANGE]
        contract_symbol = get_contract_symbol(FUTURE_EXCHANGE, symbol)
        contract_quantity = QUANTITY / LEVERAGE  # 考虑杠杆的合约数量
        
        if TRADE_TYPE == "spot_buy_future_short":
            # 在现货交易所买入
            spot_order = spot_exchange.create_market_buy_order(
                symbol,
                QUANTITY
            )
            logger.info(f"{SPOT_EXCHANGE}买入订单执行成功: {spot_order}")

            # 在合约交易所卖空
            future_order = future_exchange.create_market_sell_order(
                contract_symbol,
                contract_quantity  # 考虑杠杆
            )
            logger.info(f"{FUTURE_EXCHANGE}卖出(做空)订单执行成功 ({LEVERAGE}倍杠杆): {future_order}")

            # 计算理论利润
            theoretical_profit = (future_price - spot_price) * QUANTITY
            
        elif TRADE_TYPE == "spot_sell_future_cover":
            # 在现货交易所卖出
            spot_order = spot_exchange.create_market_sell_order(
                symbol,
                QUANTITY
            )
            logger.info(f"{SPOT_EXCHANGE}卖出订单执行成功: {spot_order}")

            # 在合约交易所买入平仓
            future_order = future_exchange.create_market_buy_order(
                contract_symbol,
                contract_quantity  # 考虑杠杆
            )
            logger.info(f"{FUTURE_EXCHANGE}买入(平仓)订单执行成功 ({LEVERAGE}倍杠杆): {future_order}")

            # 计算理论利润
            theoretical_profit = (spot_price - future_price) * QUANTITY
            
        logger.info(f"理论利润: {theoretical_profit:.4f} USDT")
        return spot_order, future_order
        
    except Exception as e:
        logger.error(f"执行交易失败: {e}")
        return None, None


# 检查套利条件是否满足
def is_arbitrage_condition_met(spot_price, future_price):
    if spot_price is None or future_price is None:
        return False
        
    if TRADE_TYPE == "spot_buy_future_short":
        # 现货买入, 合约卖空, 预期future_price > spot_price才有利可图
        return future_price > spot_price
    elif TRADE_TYPE == "spot_sell_future_cover":
        # 现货卖出, 合约平仓买入, 预期spot_price > future_price才有利可图
        return spot_price > future_price
    
    return False


# 主函数
def main():
    logger.info(f"开始套利交易程序 - {TRADE_TYPE} - {SPOT_EXCHANGE}/{FUTURE_EXCHANGE}")

    try:
        # 初始化交易所
        exchanges = init_exchanges()
        
        if SPOT_EXCHANGE not in exchanges or FUTURE_EXCHANGE not in exchanges:
            logger.error(f"交易所初始化失败: {SPOT_EXCHANGE}或{FUTURE_EXCHANGE}不存在")
            return
            
        # 设置合约交易参数
        if not setup_contract_settings(exchanges[FUTURE_EXCHANGE], FUTURE_EXCHANGE, SYMBOL):
            logger.error("合约交易参数设置失败，程序退出")
            return

        while True:
            # 获取考虑深度的价格
            spot_price, future_price, enough_depth = get_prices_with_depth(exchanges, SYMBOL)

            if spot_price is None or future_price is None:
                logger.warning("价格获取失败，等待重试...")
                time.sleep(RETRY_DELAY)
                continue

            if not enough_depth:
                logger.warning("订单簿深度不足，等待市场深度恢复...")
                time.sleep(RETRY_DELAY)
                continue

            # 计算价格差异
            price_difference = calculate_price_difference(spot_price, future_price)

            if price_difference is None:
                logger.warning("价格差异计算失败，等待重试...")
                time.sleep(RETRY_DELAY)
                continue
                
            # 检查是否满足套利条件
            if not is_arbitrage_condition_met(spot_price, future_price):
                logger.info(f"不满足套利条件 ({TRADE_TYPE})，等待...")
                time.sleep(RETRY_DELAY)
                continue

            # 检查价格差异是否满足条件
            if price_difference > THRESHOLD:
                logger.info(f"价格差异 {price_difference:.6f}% 满足阈值 {THRESHOLD}%，执行交易")

                # 执行交易
                # spot_order, future_order = execute_trades(exchanges, SYMBOL, spot_price, future_price)
                #
                # if spot_order and future_order:
                #     logger.info("套利交易成功完成")
                #     break
                # else:
                #     logger.error("交易执行失败，等待重试...")
                #     time.sleep(RETRY_DELAY)
            else:
                logger.info(f"价格差异 {price_difference:.6f}% 未达到阈值 {THRESHOLD}%，等待...")
                time.sleep(RETRY_DELAY)

    except Exception as e:
        logger.error(f"程序执行错误: {e}")

    finally:
        logger.info("套利交易程序结束")


if __name__ == "__main__":
    main()