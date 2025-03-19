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
RETRY_DELAY = 1  # 重试延迟(秒)
LEVERAGE = 10  # 杠杆倍数
MARGIN_MODE = "cross"  # 全仓模式

# 拆分交易配置
SPLIT_ORDERS = True  # 是否拆分订单
SPLIT_BY_VALUE = True  # True: 按金额拆分, False: 按代币数量拆分
SPLIT_SIZE = 100  # 每次拆分的大小(金额或数量)
SPLIT_DELAY = 0.5  # 拆分订单之间的延迟(秒)

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


# 拆分执行购买订单
def execute_split_buy_orders(exchange, future_exchange, symbol, contract_symbol, total_quantity, spot_price, future_price):
    """
    将大额买入订单拆分为多个小订单执行，每次执行后重新检查套利条件
    
    Args:
        exchange: 现货交易所API对象
        future_exchange: 合约交易所API对象
        symbol: 交易对
        contract_symbol: 合约交易对
        total_quantity: 总购买数量
        spot_price: 初始现货价格
        future_price: 初始合约价格
        
    Returns:
        orders_info: 订单信息列表
        total_cost: 总成本
        avg_price: 平均成交价格
        completed: 是否完成全部数量
    """
    orders_info = []
    total_filled = 0
    total_cost = 0
    remaining = total_quantity
    
    # 如果按金额拆分，需要获取当前市场价格来估算每批数量
    if SPLIT_BY_VALUE:
        try:
            ticker = exchange.fetch_ticker(symbol)
            current_price = ticker['last']  # 最新成交价
            
            # 计算总价值
            total_value = total_quantity * current_price
            logger.info(f"开始拆分执行买入订单，总价值约: {total_value:.2f} USDT, 每批金额: {SPLIT_SIZE} USDT")
            
            # 重新计算总数量
            remaining_value = total_value
            remaining = total_quantity  # 仍然保持数量计数
        except Exception as e:
            logger.error(f"获取市场价格失败，无法按金额拆分: {e}")
            return [], 0, 0, False
    else:
        logger.info(f"开始拆分执行买入订单，总数量: {total_quantity}, 每批数量: {SPLIT_SIZE}")
    
    while remaining > 0:
        # 计算当前批次的数量
        if SPLIT_BY_VALUE:
            # 根据剩余金额和当前价格计算当前批次数量
            batch_value = min(SPLIT_SIZE, remaining_value)
            current_quantity = min(batch_value / current_price, remaining)
            # 金额太小可能导致数量过小，设置最小交易数量
            if current_quantity * current_price < exchange.markets[symbol].get('limits', {}).get('cost', {}).get('min', 5):
                current_quantity = remaining  # 如果剩余金额太小，直接交易所有剩余数量
        else:
            current_quantity = min(SPLIT_SIZE, remaining)
        
        try:
            # 执行单笔交易前重新检查市场条件
            if len(orders_info) > 0:  # 不是第一笔订单，重新检查
                # 获取最新市场价格
                new_spot_price, new_future_price, enough_depth = get_prices_with_depth(
                    {SPOT_EXCHANGE: exchange, FUTURE_EXCHANGE: future_exchange}, 
                    symbol
                )
                
                # 检查市场条件是否仍然满足
                if new_spot_price is None or new_future_price is None or not enough_depth:
                    logger.warning("获取价格失败或深度不足，停止继续拆分交易")
                    break
                
                # 检查价格差异
                price_difference = calculate_price_difference(new_spot_price, new_future_price)
                if price_difference is None or price_difference <= THRESHOLD:
                    logger.warning(f"价格差异 {price_difference if price_difference else 'N/A'}% 不再满足条件，停止继续拆分交易")
                    break
                
                # 更新当前价格估计
                if SPLIT_BY_VALUE:
                    current_price = new_spot_price
            
            # 创建市价买入订单
            order = exchange.create_market_buy_order(symbol, current_quantity)
                
            filled = float(order.get('filled', current_quantity))
            cost = float(order.get('cost', 0))
            
            if cost == 0 and 'price' in order and filled > 0:
                cost = float(order['price']) * filled
                
            # 累计已执行数量和成本
            total_filled += filled
            total_cost += cost
            
            orders_info.append(order)
            
            if SPLIT_BY_VALUE:
                logger.info(f"第{len(orders_info)}批买入订单执行成功: 数量={filled}, 成本={cost:.4f} USDT")
                remaining_value -= cost
            else:
                logger.info(f"第{len(orders_info)}批买入订单执行成功: 数量={filled}, 成本={cost:.4f} USDT")
            
            # 更新剩余数量
            remaining -= filled
            
            # 调整当前价格估算（基于最新成交价）
            if SPLIT_BY_VALUE and cost > 0 and filled > 0:
                current_price = cost / filled
            
            # 添加延迟，避免频繁下单
            if remaining > 0:
                time.sleep(SPLIT_DELAY)
                
        except Exception as e:
            logger.error(f"拆分买入订单执行失败: {e}")
            break
    
    # 计算平均成交价格
    avg_price = total_cost / total_filled if total_filled > 0 else 0
    
    # 判断是否完成全部数量
    completed = (remaining <= 0)
    
    logger.info(f"拆分买入订单执行完成: 总成交数量={total_filled}, 总成本={total_cost:.4f} USDT, 平均价格={avg_price:.4f}")
    logger.info(f"完成状态: {'完成' if completed else f'未完成(剩余{remaining})'}")
    
    return orders_info, total_cost, avg_price, completed


# 拆分执行卖出订单
def execute_split_sell_orders(exchange, future_exchange, symbol, contract_symbol, total_quantity, spot_price, future_price):
    """
    将大额卖出订单拆分为多个小订单执行，每次执行后重新检查套利条件
    
    Args:
        exchange: 现货交易所API对象
        future_exchange: 合约交易所API对象
        symbol: 交易对
        contract_symbol: 合约交易对
        total_quantity: 总卖出数量
        spot_price:
        future_price:
        
    Returns:
        orders_info: 订单信息列表
        total_proceeds: 总收入
        avg_price: 平均成交价格
        completed: 是否完成全部数量
    """
    orders_info = []
    total_filled = 0
    total_proceeds = 0
    remaining = total_quantity
    
    # 如果按金额拆分，需要获取当前市场价格来估算每批数量
    if SPLIT_BY_VALUE:
        try:
            ticker = exchange.fetch_ticker(symbol)
            current_price = ticker['last']  # 最新成交价
            
            # 计算总价值
            total_value = total_quantity * current_price
            logger.info(f"开始拆分执行卖出订单，总价值约: {total_value:.2f} USDT, 每批金额: {SPLIT_SIZE} USDT")
            
            # 重新计算总数量
            remaining_value = total_value
            remaining = total_quantity  # 仍然保持数量计数
        except Exception as e:
            logger.error(f"获取市场价格失败，无法按金额拆分: {e}")
            return [], 0, 0, False
    else:
        logger.info(f"开始拆分执行卖出订单，总数量: {total_quantity}, 每批数量: {SPLIT_SIZE}")
    
    while remaining > 0:
        # 计算当前批次的数量
        if SPLIT_BY_VALUE:
            # 根据剩余金额和当前价格计算当前批次数量
            batch_value = min(SPLIT_SIZE, remaining_value)
            current_quantity = min(batch_value / current_price, remaining)
            # 金额太小可能导致数量过小，设置最小交易数量
            if current_quantity * current_price < exchange.markets[symbol].get('limits', {}).get('cost', {}).get('min', 5):
                current_quantity = remaining  # 如果剩余金额太小，直接交易所有剩余数量
        else:
            current_quantity = min(SPLIT_SIZE, remaining)
        
        try:
            # 执行单笔交易前重新检查市场条件
            if len(orders_info) > 0:  # 不是第一笔订单，重新检查
                # 获取最新市场价格
                new_spot_price, new_future_price, enough_depth = get_prices_with_depth(
                    {SPOT_EXCHANGE: exchange, FUTURE_EXCHANGE: future_exchange}, 
                    symbol
                )
                
                # 检查市场条件是否仍然满足
                if new_spot_price is None or new_future_price is None or not enough_depth:
                    logger.warning("获取价格失败或深度不足，停止继续拆分交易")
                    break
                
                # 检查价格差异
                price_difference = calculate_price_difference(new_spot_price, new_future_price)
                if price_difference is None or price_difference <= THRESHOLD:
                    logger.warning(f"价格差异 {price_difference if price_difference else 'N/A'}% 不再满足条件，停止继续拆分交易")
                    break
                
                # 更新当前价格估计
                if SPLIT_BY_VALUE:
                    current_price = new_spot_price
            
            # 创建市价卖出订单
            order = exchange.create_market_sell_order(symbol, current_quantity)
                
            filled = float(order.get('filled', current_quantity))
            proceeds = float(order.get('cost', 0))
            
            if proceeds == 0 and 'price' in order and filled > 0:
                proceeds = float(order['price']) * filled
                
            # 累计已执行数量和收入
            total_filled += filled
            total_proceeds += proceeds
            
            orders_info.append(order)
            
            if SPLIT_BY_VALUE:
                logger.info(f"第{len(orders_info)}批卖出订单执行成功: 数量={filled}, 收入={proceeds:.4f} USDT")
                remaining_value -= proceeds
            else:
                logger.info(f"第{len(orders_info)}批卖出订单执行成功: 数量={filled}, 收入={proceeds:.4f} USDT")
            
            # 更新剩余数量
            remaining -= filled
            
            # 调整当前价格估算（基于最新成交价）
            if SPLIT_BY_VALUE and proceeds > 0 and filled > 0:
                current_price = proceeds / filled
            
            # 添加延迟，避免频繁下单
            if remaining > 0:
                time.sleep(SPLIT_DELAY)
                
        except Exception as e:
            logger.error(f"拆分卖出订单执行失败: {e}")
            break
    
    # 计算平均成交价格
    avg_price = total_proceeds / total_filled if total_filled > 0 else 0
    
    # 判断是否完成全部数量
    completed = (remaining <= 0)
    
    logger.info(f"拆分卖出订单执行完成: 总成交数量={total_filled}, 总收入={total_proceeds:.4f} USDT, 平均价格={avg_price:.4f}")
    logger.info(f"完成状态: {'完成' if completed else f'未完成(剩余{remaining})'}")
    
    return orders_info, total_proceeds, avg_price, completed


# 修改执行交易函数，增加动态检查功能
def execute_trades(exchanges, symbol, spot_price, future_price):
    try:
        # 首先检查余额是否足够
        balances_ok, message = check_balances(exchanges, symbol)
        if not balances_ok:
            logger.error(f"余额检查失败，无法执行交易: {message}")
            return None, None
            
        spot_exchange = exchanges[SPOT_EXCHANGE]
        future_exchange = exchanges[FUTURE_EXCHANGE]
        contract_symbol = get_contract_symbol(FUTURE_EXCHANGE, symbol)
        contract_quantity = QUANTITY / LEVERAGE  # 考虑杠杆的合约数量
        
        theoretical_profit = 0
        
        # 进行估计值计算，无论按数量还是金额拆分都需要
        spot_value = QUANTITY * spot_price
        future_value = contract_quantity * future_price
        
        if TRADE_TYPE == "spot_buy_future_short":
            # 判断是否需要拆分，需要考虑金额或数量
            need_spot_split = False
            need_future_split = False
            
            if SPLIT_ORDERS:
                if SPLIT_BY_VALUE:
                    need_spot_split = spot_value > SPLIT_SIZE
                    need_future_split = future_value > SPLIT_SIZE
                else:
                    need_spot_split = QUANTITY > SPLIT_SIZE
                    need_future_split = contract_quantity > SPLIT_SIZE
            
            # 在现货交易所买入
            if need_spot_split:
                spot_orders, spot_cost, spot_avg_price, spot_completed = execute_split_buy_orders(
                    spot_exchange, future_exchange, symbol, contract_symbol, QUANTITY, spot_price, future_price
                )
                spot_order = {"orders": spot_orders, "totalCost": spot_cost, "avgPrice": spot_avg_price, "completed": spot_completed}
                logger.info(f"{SPOT_EXCHANGE}拆分买入订单执行情况: 完成状态={spot_completed}, 平均成交价={spot_avg_price:.4f}")
                
                # 如果现货交易没有完成全部数量，可能不需要执行全部合约交易
                if not spot_completed:
                    # 计算实际买入的现货数量
                    actual_quantity = sum([float(order.get('filled', 0)) for order in spot_orders])
                    # 重新计算对应的合约数量
                    contract_quantity = actual_quantity / LEVERAGE
                    logger.info(f"由于现货交易未完成，调整合约交易数量为: {contract_quantity}")
            else:
                # 如果不需要拆分，则直接执行单个订单
                spot_order = spot_exchange.create_market_buy_order(
                    symbol,
                    QUANTITY
                )
                logger.info(f"{SPOT_EXCHANGE}买入订单执行成功: {spot_order}")
                spot_avg_price = spot_price  # 使用预估价格
                spot_completed = True

            # 在合约交易所卖空
            if contract_quantity <= 0:
                logger.warning("由于现货交易未完成，没有合约需要交易")
                future_order = None
                future_avg_price = 0
                future_completed = False
            elif need_future_split and contract_quantity > SPLIT_SIZE / LEVERAGE:
                future_orders, future_proceeds, future_avg_price, future_completed = execute_split_sell_orders(
                    future_exchange, spot_exchange, contract_symbol, symbol, contract_quantity, spot_price, future_price
                )
                future_order = {"orders": future_orders, "totalProceeds": future_proceeds, "avgPrice": future_avg_price, "completed": future_completed}
                logger.info(f"{FUTURE_EXCHANGE}拆分卖出订单执行情况: 完成状态={future_completed}, 平均成交价={future_avg_price:.4f}")
            else:
                # 如果不需要拆分，则直接执行单个订单
                future_order = future_exchange.create_market_sell_order(
                    contract_symbol,
                    contract_quantity
                )
                logger.info(f"{FUTURE_EXCHANGE}卖出(做空)订单执行成功 ({LEVERAGE}倍杠杆): {future_order}")
                future_avg_price = future_price  # 使用预估价格
                future_completed = True

            # 计算实际利润 (使用实际成交价格)
            if spot_order and future_order:
                actual_spot_price = spot_avg_price if isinstance(spot_order, dict) and "avgPrice" in spot_order else spot_price
                actual_future_price = future_avg_price if isinstance(future_order, dict) and "avgPrice" in future_order else future_price
                
                # 获取实际交易的数量
                if isinstance(spot_order, dict) and "orders" in spot_order:
                    actual_quantity = sum([float(order.get('filled', 0)) for order in spot_order["orders"]])
                else:
                    actual_quantity = float(spot_order.get('filled', QUANTITY))
                
                theoretical_profit = (actual_future_price - actual_spot_price) * actual_quantity
                logger.info(f"理论利润: {theoretical_profit:.4f} USDT (基于实际交易数量: {actual_quantity})")
            
            # 检查是否两边都完成
            if (isinstance(spot_order, dict) and spot_order.get("completed", False) == False) or \
               (isinstance(future_order, dict) and future_order.get("completed", False) == False):
                logger.warning("交易未完全执行，可能需要手动处理剩余部分")
            
        elif TRADE_TYPE == "spot_sell_future_cover":
            # 判断是否需要拆分，需要考虑金额或数量
            need_spot_split = False
            need_future_split = False
            
            if SPLIT_ORDERS:
                if SPLIT_BY_VALUE:
                    need_spot_split = spot_value > SPLIT_SIZE
                    need_future_split = future_value > SPLIT_SIZE
                else:
                    need_spot_split = QUANTITY > SPLIT_SIZE
                    need_future_split = contract_quantity > SPLIT_SIZE
                    
            # 在现货交易所卖出
            if need_spot_split:
                spot_orders, spot_proceeds, spot_avg_price, spot_completed = execute_split_sell_orders(
                    spot_exchange, future_exchange, symbol, contract_symbol, QUANTITY, spot_price, future_price
                )
                spot_order = {"orders": spot_orders, "totalProceeds": spot_proceeds, "avgPrice": spot_avg_price, "completed": spot_completed}
                logger.info(f"{SPOT_EXCHANGE}拆分卖出订单执行情况: 完成状态={spot_completed}, 平均成交价={spot_avg_price:.4f}")
                
                # 如果现货交易没有完成全部数量，可能不需要执行全部合约交易
                if not spot_completed:
                    # 计算实际卖出的现货数量
                    actual_quantity = sum([float(order.get('filled', 0)) for order in spot_orders])
                    # 重新计算对应的合约数量
                    contract_quantity = actual_quantity / LEVERAGE
                    logger.info(f"由于现货交易未完成，调整合约交易数量为: {contract_quantity}")
            else:
                # 如果不需要拆分，则直接执行单个订单
                spot_order = spot_exchange.create_market_sell_order(
                    symbol,
                    QUANTITY
                )
                logger.info(f"{SPOT_EXCHANGE}卖出订单执行成功: {spot_order}")
                spot_avg_price = spot_price  # 使用预估价格
                spot_completed = True

            # 在合约交易所买入平仓
            if contract_quantity <= 0:
                logger.warning("由于现货交易未完成，没有合约需要交易")
                future_order = None
                future_avg_price = 0
                future_completed = False
            elif need_future_split and contract_quantity > SPLIT_SIZE / LEVERAGE:
                future_orders, future_cost, future_avg_price, future_completed = execute_split_buy_orders(
                    future_exchange, spot_exchange, contract_symbol, symbol, contract_quantity, spot_price, future_price
                )
                future_order = {"orders": future_orders, "totalCost": future_cost, "avgPrice": future_avg_price, "completed": future_completed}
                logger.info(f"{FUTURE_EXCHANGE}拆分买入订单执行情况: 完成状态={future_completed}, 平均成交价={future_avg_price:.4f}")
            else:
                # 如果不需要拆分，则直接执行单个订单
                future_order = future_exchange.create_market_buy_order(
                    contract_symbol,
                    contract_quantity
                )
                logger.info(f"{FUTURE_EXCHANGE}买入(平仓)订单执行成功 ({LEVERAGE}倍杠杆): {future_order}")
                future_avg_price = future_price  # 使用预估价格
                future_completed = True

            # 计算实际利润 (使用实际成交价格)
            if spot_order and future_order:
                actual_spot_price = spot_avg_price if isinstance(spot_order, dict) and "avgPrice" in spot_order else spot_price
                actual_future_price = future_avg_price if isinstance(future_order, dict) and "avgPrice" in future_order else future_price
                
                # 获取实际交易的数量
                if isinstance(spot_order, dict) and "orders" in spot_order:
                    actual_quantity = sum([float(order.get('filled', 0)) for order in spot_order["orders"]])
                else:
                    actual_quantity = float(spot_order.get('filled', QUANTITY))
                
                theoretical_profit = (actual_spot_price - actual_future_price) * actual_quantity
                logger.info(f"理论利润: {theoretical_profit:.4f} USDT (基于实际交易数量: {actual_quantity})")
            
            # 检查是否两边都完成
            if (isinstance(spot_order, dict) and spot_order.get("completed", False) == False) or \
               (isinstance(future_order, dict) and future_order.get("completed", False) == False):
                logger.warning("交易未完全执行，可能需要手动处理剩余部分")
        
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
                spot_order, future_order = execute_trades(exchanges, SYMBOL, spot_price, future_price)

                if spot_order and future_order:
                    logger.info("套利交易成功完成")
                    break
                else:
                    logger.error("交易执行失败，等待重试...")
                    time.sleep(RETRY_DELAY)
            else:
                logger.info(f"价格差异 {price_difference:.6f}% 未达到阈值 {THRESHOLD}%，等待...")
                time.sleep(RETRY_DELAY)

    except Exception as e:
        logger.error(f"程序执行错误: {e}")

    finally:
        logger.info("套利交易程序结束")


if __name__ == "__main__":
    main()