import os
import sys
import time
import logging
import argparse
import traceback
import hmac
import hashlib
import requests
import json
from datetime import datetime

# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将主项目目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))

from config import proxies  # 从配置中导入代理设置

# 导入hedging_trade.py中的相关函数和配置
from high_yield.hedging_trade import (
    init_exchanges, 
    get_contract_symbol,
    setup_contract_settings,
    logger
)
def setup_test_logger():
    """配置测试用的日志"""
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # 创建文件处理器
    log_filename = f"exchange_trading_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # 添加处理器到logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    logger.setLevel(logging.INFO)
    logger.info("开始交易所交易功能测试")
    return log_filename

def test_spot_trading(exchange, exchange_id, symbol, amount):
    """
    测试现货交易功能
    
    Args:
        exchange: 交易所API对象
        exchange_id: 交易所ID
        symbol: 交易对
        amount: 交易金额(USDT)
        
    Returns:
        bool: 测试是否成功
    """
    logger.info(f"===== 测试 {exchange_id} 现货交易 =====")
    logger.info(f"交易对: {symbol}, 金额: {amount} USDT")
    
    try:
        # 1. 获取当前市场价格
        ticker = exchange.fetch_ticker(symbol)
        current_price = ticker['last']
        logger.info(f"当前 {symbol} 价格: {current_price}")
        
        # 2. 计算买入数量
        base_currency, quote_currency = symbol.split('/')
        quantity = amount / current_price
        
        # 考虑交易所的最小交易量要求
        market = exchange.market(symbol)
        if 'limits' in market and 'amount' in market['limits'] and 'min' in market['limits']['amount']:
            min_amount = market['limits']['amount']['min']
            if quantity < min_amount:
                logger.warning(f"计算的数量 {quantity} 小于最小交易量 {min_amount}，将使用最小交易量")
                quantity = min_amount
        
        logger.info(f"计划买入数量: {quantity} {base_currency} (约 {amount} USDT)")
        
        # 3. 执行市价买入
        buy_params = {}
        if exchange_id == "binance":
            # Binance 使用 quoteOrderQty 参数按USDT金额购买
            exchange.options['defaultType'] = 'spot'  # 确保使用现货API
            buy_params = {"quoteOrderQty": amount}
        elif exchange_id == "okx":
            # OKX 使用 notional 参数
            buy_params = {"notional": amount}
        elif exchange_id == "gateio":
            # GateIO 使用 cost 参数
            buy_params = {"cost": amount}
        elif exchange_id == "bitget":
            # Bitget 需要特殊处理市价买入
            # 方法1：设置createMarketBuyOrderRequiresPrice为False并传入cost
            buy_params = {
                "createMarketBuyOrderRequiresPrice": False,
                "cost": amount  # 直接传入要花费的USDT金额
            }
            logger.info(f"Bitget市价买入特殊处理: 将直接花费 {amount} USDT")
            
            # 方法2：也可以通过全局选项设置（如果上面的方法不起作用）
            exchange.options['createMarketBuyOrderRequiresPrice'] = False
        elif exchange_id == "bybit":
            # Bybit 使用 orderAmount 参数
            buy_params = {"orderAmount": amount}
        
        logger.info(f"执行市价买入，参数: {buy_params}")
        buy_order = None
        
        # 根据不同交易所情况处理市价买入
        if exchange_id == "binance" and "quoteOrderQty" in buy_params:
            # Binance 特殊处理
            buy_order = exchange.create_market_buy_order(symbol, None, params=buy_params)
        elif exchange_id == "bitget" and "createMarketBuyOrderRequiresPrice" in buy_params:
            # Bitget 特殊处理
            buy_order = exchange.create_market_buy_order(symbol, amount, params=buy_params)
        else:
            # 其他交易所常规处理
            buy_order = exchange.create_market_buy_order(symbol, quantity, params=buy_params)
        
        if not buy_order:
            logger.error("买入订单创建失败")
            return False
        
        logger.info(f"买入订单执行结果: {buy_order}")
        
        # 对于Bitget，需要额外获取订单详情来获取实际成交量
        if exchange_id == "bitget" and buy_order.get('id'):
            logger.info(f"Bitget交易所：正在获取订单 {buy_order['id']} 的详细信息...")
            time.sleep(3)  # Bitget可能需要更多时间处理订单
            
            try:
                # 获取订单详情
                order_detail = exchange.fetch_order(buy_order['id'], symbol)
                logger.info(f"订单详情: {order_detail}")
                
                # 更新买入订单信息
                buy_order = order_detail
            except Exception as e:
                logger.warning(f"获取Bitget订单详情失败: {e}，将尝试使用预估数量继续测试")
        else:
            time.sleep(2)  # 其他交易所等待订单完成
        
        # 4. 获取实际成交数量
        filled_amount = 0
        
        # 尝试从订单信息中获取成交数量
        if 'filled' in buy_order and buy_order['filled'] is not None:
            filled_amount = float(buy_order['filled'])
        elif 'amount' in buy_order and buy_order['amount'] is not None:
            filled_amount = float(buy_order['amount'])
        elif 'info' in buy_order and 'filled' in buy_order['info'] and buy_order['info']['filled'] is not None:
            filled_amount = float(buy_order['info']['filled'])
        
        # 如果仍然无法获取实际成交数量，使用估算值
        if filled_amount <= 0:
            # 使用估算值（按USDT金额/当前价格）
            logger.warning(f"无法获取实际成交数量，将使用估算值")
            filled_amount = quantity
            
            # 对于Bitget，尝试通过查询余额来确定实际购买数量
            if exchange_id == "bitget":
                try:
                    # 查询当前余额
                    balance = exchange.fetch_balance()
                    if base_currency in balance:
                        current_balance = float(balance[base_currency]['free'])
                        logger.info(f"当前 {base_currency} 余额: {current_balance}")
                        
                        # 使用余额作为实际购买数量（简化处理，实际情况可能更复杂）
                        filled_amount = current_balance
                except Exception as e:
                    logger.warning(f"获取Bitget余额失败: {e}")
        
        logger.info(f"实际买入数量: {filled_amount} {base_currency}")
        
        # 计算手续费，如果手续费是以基础货币（如DOGE）计算的，则需要从卖出数量中减去
        fee_amount = 0
        if 'fee' in buy_order and buy_order['fee'] is not None:
            fee_currency = buy_order['fee'].get('currency', '')
            fee_cost = float(buy_order['fee'].get('cost', 0))
            
            if fee_currency == base_currency:
                fee_amount = fee_cost
                logger.info(f"扣除基础货币手续费: {fee_amount} {base_currency}")
        
        # 检查是否有多个费用条目
        if 'fees' in buy_order and buy_order['fees'] is not None:
            for fee in buy_order['fees']:
                if fee['currency'] == base_currency and fee['cost'] > 0:
                    fee_amount += float(fee['cost'])
                    logger.info(f"从fees数组中扣除额外手续费: {fee['cost']} {base_currency}")

        # 对于GateIO，需要额外检查费用信息
        if exchange_id == "gateio" and 'info' in buy_order and 'fee' in buy_order['info']:
            info_fee = float(buy_order['info'].get('fee', 0))
            info_fee_currency = buy_order['info'].get('fee_currency', '')
            
            if info_fee_currency == base_currency and info_fee > 0:
                # 确保不重复计算
                if fee_amount < info_fee:
                    logger.info(f"从info中更新手续费: {info_fee} {base_currency}")
                    fee_amount = info_fee

        # 对于Bitget，估算手续费（如果无法从订单中获取）
        if exchange_id == "bitget" and fee_amount == 0:
            # 假设手续费率为0.1%（实际应根据用户等级和交易所规则调整）
            estimated_fee = filled_amount * 0.001
            logger.info(f"估算 Bitget 手续费: {estimated_fee} {base_currency}")
            fee_amount = estimated_fee
        
        # 计算实际可卖出数量（减去手续费后）
        sell_amount = filled_amount - fee_amount
        
        # 确保卖出数量为正值，同时添加额外的安全裕度
        if sell_amount <= 0:
            logger.error(f"计算的卖出数量非正值: {sell_amount}，终止测试")
            return False
            
        # 额外查询当前余额，确保卖出数量不超过实际可用余额
        try:
            current_balance = exchange.fetch_balance()
            if base_currency in current_balance:
                available_amount = float(current_balance[base_currency]['free'])
                logger.info(f"当前可用 {base_currency} 余额: {available_amount}")
                
                # 如果计算的卖出数量大于实际可用余额，调整卖出数量
                if sell_amount > available_amount:
                    logger.warning(f"计算的卖出数量 {sell_amount} 大于可用余额 {available_amount}，将调整为可用余额")
                    sell_amount = available_amount
                    
                # 为了安全，再减去一点点（避免小数精度问题）
                sell_amount = sell_amount * 0.999
        except Exception as e:
            logger.warning(f"获取当前余额失败: {e}，将使用计算的卖出数量并添加安全裕度")
            # 添加额外安全裕度，减少1%以避免余额不足
            sell_amount = sell_amount * 0.99

        # 确保卖出数量为市场允许的精度
        try:
            if 'precision' in market and 'amount' in market['precision']:
                precision = market['precision']['amount']
                if isinstance(precision, int):
                    sell_amount = round(sell_amount, precision)
                    logger.info(f"根据市场精度调整卖出数量: {sell_amount} {base_currency}")
        except Exception as e:
            logger.warning(f"调整精度失败: {e}")
        
        logger.info(f"最终卖出数量: {sell_amount} {base_currency} (已扣除手续费和安全裕度)")
        
        # 5. 执行市价卖出
        logger.info(f"执行市价卖出，数量: {sell_amount} {base_currency}")
        sell_order = exchange.create_market_sell_order(symbol, sell_amount)
        
        logger.info(f"卖出订单执行结果: {sell_order}")
        
        # 6. 验证交易结果
        if buy_order and sell_order:
            logger.info(f"{exchange_id} 现货交易测试成功！")
            return True
        else:
            logger.error(f"{exchange_id} 现货交易测试失败！")
            return False
            
    except Exception as e:
        logger.error(f"测试 {exchange_id} 现货交易失败: {e}")
        logger.error(traceback.format_exc())
        return False

def test_futures_trading(exchange, exchange_id, symbol, amount, leverage):
    """
    测试合约交易功能
    
    Args:
        exchange: 交易所API对象
        exchange_id: 交易所ID
        symbol: 交易对
        amount: 交易金额(USDT)
        leverage: 杠杆倍数
        
    Returns:
        bool: 测试是否成功
    """
    logger.info(f"===== 测试 {exchange_id} 合约交易 =====")
    logger.info(f"交易对: {symbol}, 金额: {amount} USDT, 杠杆: {leverage}倍")
    
    try:
        # 1. 获取合约交易对格式并设置杠杆
        contract_symbol = get_contract_symbol(exchange_id, symbol)
        logger.info(f"合约交易对: {contract_symbol}")
        
        # 设置杠杆和保证金模式
        margin_mode = "cross"  # 使用全仓模式
        
        # 为杠杆和保证金设置创建一个模拟的args
        args = argparse.Namespace(
            symbol=symbol,
            leverage=leverage,
            margin_mode=margin_mode
        )
        
        # Binance 特殊处理 - 为了解决 "supports linear and inverse contracts only" 错误
        if exchange_id == "binance":
            logger.info(f"使用特殊方法设置 Binance 合约参数")
            try:
                # 确保使用合约API
                exchange.options['defaultType'] = 'future'
                
                # 1. 首先设置杠杆倍数
                leverage_params = {'symbol': contract_symbol, 'leverage': leverage}
                leverage_response = exchange.fapiPrivatePostLeverage(leverage_params)
                logger.info(f"Binance 设置杠杆倍数结果: {leverage_response}")
                
                # 2. 然后设置保证金模式，明确指定合约类型为linear
                margin_params = {
                    'symbol': contract_symbol,
                    'marginType': margin_mode.upper(),  # CROSS 或 ISOLATED
                    'recvWindow': 60000
                }
                
                # 尝试设置保证金模式，如果已经是该模式则忽略错误
                try:
                    margin_response = exchange.fapiPrivatePostMarginType(margin_params)
                    logger.info(f"Binance 设置保证金模式结果: {margin_response}")
                except Exception as margin_error:
                    if "No need to change margin type" in str(margin_error):
                        logger.info(f"Binance 已经是 {margin_mode} 模式，无需更改")
                    else:
                        raise margin_error
                
                # 3. 获取当前市场价格
                ticker = exchange.fetch_ticker(contract_symbol)
                current_price = ticker['last']
                logger.info(f"当前 {contract_symbol} 价格: {current_price}")
                
                # 4. 计算合约数量 (考虑杠杆)
                base_currency, quote_currency = symbol.split('/')
                contract_value = amount * leverage
                quantity = contract_value / current_price
                
                # 调整为合约要求的精度
                market = exchange.market(contract_symbol)
                if 'precision' in market and 'amount' in market['precision']:
                    precision = market['precision']['amount']
                    quantity = round(quantity, precision) if isinstance(precision, int) else float(int(quantity))
                
                logger.info(f"计划开多数量: {quantity} (价值约 {contract_value} USDT，实际保证金约 {amount} USDT)")
                
                # 5. 开仓 - 市价买入开多
                buy_params = {
                    'positionSide': 'BOTH'  # 单向持仓模式
                }
                
                logger.info(f"Binance 开仓参数: {buy_params}")
                buy_order = exchange.create_market_buy_order(contract_symbol, quantity, params=buy_params)
                logger.info(f"开仓订单结果: {buy_order}")
                
                # 等待订单完成
                time.sleep(3)
                
                # 6. 平仓 - 市价卖出平多
                sell_params = {
                    'positionSide': 'BOTH',  # 单向持仓模式
                    'reduceOnly': True  # 确保是平仓操作
                }
                
                logger.info(f"Binance 平仓参数: {sell_params}")
                sell_order = exchange.create_market_sell_order(contract_symbol, quantity, params=sell_params)
                logger.info(f"平仓订单结果: {sell_order}")
                
                # 判断测试是否成功
                if buy_order and sell_order:
                    logger.info(f"Binance 合约交易测试成功！")
                    return True
                else:
                    logger.error(f"Binance 合约交易测试失败！")
                    return False
                
            except Exception as e:
                logger.error(f"Binance 合约交易测试失败: {e}")
                logger.error(traceback.format_exc())
                return False
        # 其他交易所正常设置合约参数        
        elif not setup_contract_settings(exchange, exchange_id, symbol, args):
            logger.error(f"设置 {exchange_id} 合约参数失败")
            return False
            
        # 2. 获取当前市场价格
        ticker = None
        if exchange_id == "binance":
            exchange.options['defaultType'] = 'future'
            ticker = exchange.fetch_ticker(contract_symbol)
        elif exchange_id == "okx":
            params = {'instType': 'SWAP'}
            ticker = exchange.fetch_ticker(contract_symbol, params=params)
        elif exchange_id == "bybit":
            params = {'category': 'linear'}
            ticker = exchange.fetch_ticker(contract_symbol, params=params)
        elif exchange_id == "bitget":
            # 确保使用合约API
            exchange.options['defaultType'] = 'swap'
            ticker = exchange.fetch_ticker(contract_symbol)
        else:
            ticker = exchange.fetch_ticker(contract_symbol)
            
        current_price = ticker['last']
        logger.info(f"当前 {contract_symbol} 价格: {current_price}")
        
        # 3. 计算合约数量 (考虑杠杆)
        base_currency, quote_currency = symbol.split('/')
        contract_value = amount * leverage
        quantity = contract_value / current_price
        
        # 调整为合约要求的精度
        if exchange_id == "binance" or exchange_id == "bitget":
            # 合约通常有精度要求
            market = exchange.market(contract_symbol)
            if 'precision' in market and 'amount' in market['precision']:
                precision = market['precision']['amount']
                quantity = round(quantity, precision) if isinstance(precision, int) else float(int(quantity))
        
        logger.info(f"计划开多数量: {quantity} (价值约 {contract_value} USDT，实际保证金约 {amount} USDT)")
        
        # 为 Bitget 特别处理
        if exchange_id == "bitget":
            logger.info(f"Bitget合约交易 - 使用标准CCXT接口")
            
            try:
                # 确保使用合约API
                exchange.options['defaultType'] = 'swap'
                
                # 确认交易对格式正确（合约交易对应该有后缀）
                # 如果合约交易对没有:USDT后缀，添加它
                if ':USDT' not in contract_symbol:
                    contract_symbol = f"{contract_symbol}:USDT"
                logger.info(f"最终合约交易对: {contract_symbol}")
                
                # 检查账户余额
                balance = exchange.fetch_balance()
                logger.info(f"USDT余额: {balance.get('USDT', {}).get('free', 'unknown')}")
                
                # 1. 开仓 - 使用标准CCXT接口下单
                # 执行市价买入（开多）操作
                side = 'buy'  # 买入开多
                type = 'market'  # 市价单
                
                logger.info(f"创建{type}订单，{side} {quantity}个{contract_symbol}合约")
                buy_order = exchange.create_order(
                    symbol=contract_symbol,
                    type=type,
                    side=side,
                    amount=int(quantity)  # Bitget合约要求整数
                )
                logger.info(f"开仓订单创建结果: {buy_order}")
                
                # 获取订单ID
                order_id = buy_order.get('id')
                logger.info(f"开仓订单ID: {order_id}")
                
                # 等待订单完成
                if order_id:
                    time.sleep(3)  # 给Bitget API一些处理时间
                    
                    # 查询订单状态 
                    order_status = exchange.fetch_order(order_id, contract_symbol)
                    logger.info(f"开仓订单状态: {order_status}")
                
                # 查询当前持仓
                positions = exchange.fetch_positions([contract_symbol])
                logger.info(f"当前持仓: {positions}")
                
                # 找到与我们交易合约相符的多头持仓
                long_position = None
                for pos in positions:
                    if pos['symbol'] == contract_symbol and pos['side'] == 'long' and float(pos.get('contracts', 0)) > 0:
                        long_position = pos
                        break
                
                if long_position:
                    logger.info(f"找到多头持仓: {long_position}")
                    
                    # 获取持仓数量
                    position_qty = float(long_position.get('contracts', quantity))
                    logger.info(f"持仓数量: {position_qty}")
                    
                    # 2. 平仓 - 卖出平多
                    side = 'sell'  # 卖出平多
                    close_qty = position_qty  # 平掉全部持仓
                    
                    logger.info(f"创建{type}订单，{side} {close_qty}个{contract_symbol}合约平仓")
                    sell_order = exchange.create_order(
                        symbol=contract_symbol,
                        type=type,
                        side=side,
                        amount=int(close_qty),  # Bitget合约要求整数
                        params={'reduceOnly': True}  # 确保是平仓操作
                    )
                    logger.info(f"平仓订单创建结果: {sell_order}")
                    
                    # 获取平仓订单ID
                    close_order_id = sell_order.get('id')
                    logger.info(f"平仓订单ID: {close_order_id}")
                    
                    # 等待平仓订单完成
                    if close_order_id:
                        time.sleep(2)
                        
                        # 查询平仓订单状态
                        close_status = exchange.fetch_order(close_order_id, contract_symbol)
                        logger.info(f"平仓订单状态: {close_status}")
                    
                    # 再次查询持仓，确认已平仓
                    final_positions = exchange.fetch_positions([contract_symbol])
                    long_pos_closed = True
                    
                    for pos in final_positions:
                        if pos['symbol'] == contract_symbol and pos['side'] == 'long' and float(pos.get('contracts', 0)) > 0:
                            long_pos_closed = False
                            logger.warning(f"持仓未完全平掉: {pos}")
                            break
                    
                    if long_pos_closed:
                        logger.info("多头持仓已完全平掉")
                    
                    # 判断测试是否成功
                    if buy_order and sell_order:
                        logger.info(f"{exchange_id} 合约交易测试成功！")
                        return True
                    else:
                        logger.error(f"{exchange_id} 合约交易测试失败！")
                        return False
                else:
                    logger.error("未能建立多头持仓")
                    return False
            
            except Exception as e:
                logger.error(f"Bitget 合约交易测试失败: {e}")
                logger.error(traceback.format_exc())
                return False
        else:
            # GateIO 合约交易特殊处理 - 可以放在 Bitget 特殊处理部分之后
            logger.info(f"GateIO合约交易 - 使用特殊处理")
            
            try:
                # 确保使用期货API
                exchange.options['defaultType'] = 'delivery'  # 或者 'futures'，取决于 GateIO 的配置
                
                # 获取市场信息以确认交易对可用
                market = exchange.market(contract_symbol)
                logger.info(f"GateIO 合约市场信息: {market}")
                
                # 查询当前账户信息
                balance = exchange.fetch_balance()
                logger.info(f"GateIO 合约账户余额: {balance.get('USDT', {}).get('free', 'unknown')} USDT")
                
                # 1. 开仓 - 市价买入多头
                buy_params = {
                    'leverage': leverage,  # 指定杠杆
                    'margin_mode': margin_mode,  # cross 或 isolated
                    'settle': 'usdt',  # 结算货币
                }
                
                logger.info(f"GateIO 开仓参数: {buy_params}")
                buy_order = exchange.create_market_buy_order(contract_symbol, quantity, params=buy_params)
                logger.info(f"GateIO 开仓结果: {buy_order}")
                
                # 等待订单处理
                time.sleep(3)
                
                # 查询持仓
                positions = exchange.fetch_positions([contract_symbol])
                logger.info(f"GateIO 当前持仓: {positions}")
                
                # 确认多头持仓
                long_position = None
                for pos in positions:
                    if pos.get('symbol') == contract_symbol and pos.get('side') == 'long' and float(pos.get('contracts', 0)) > 0:
                        long_position = pos
                        break
                
                if long_position:
                    logger.info(f"GateIO 多头持仓确认: {long_position}")
                    
                    # 获取实际持仓数量
                    position_size = float(long_position.get('contracts', quantity))
                    
                    # 2. 平仓 - 市价卖出
                    sell_params = buy_params.copy()
                    sell_params['reduce_only'] = True  # 确保是平仓操作
                    
                    logger.info(f"GateIO 平仓参数: {sell_params}, 数量: {position_size}")
                    sell_order = exchange.create_market_sell_order(contract_symbol, position_size, params=sell_params)
                    logger.info(f"GateIO 平仓结果: {sell_order}")
                    
                    # 检查平仓结果
                    if buy_order and sell_order:
                        logger.info(f"GateIO 合约交易测试成功！")
                        return True
                    else:
                        logger.error(f"GateIO 合约交易测试失败！")
                        return False
                else:
                    logger.error("GateIO 未能确认多头持仓，交易测试失败")
                    return False
            
            except Exception as e:
                logger.error(f"GateIO 合约交易测试失败: {e}")
                logger.error(traceback.format_exc())
                return False
    
    except Exception as e:
        logger.error(f"测试 {exchange_id} 合约交易失败: {e}")
        logger.error(traceback.format_exc())
        return False

def verify_contract_exists(exchange, exchange_id, contract_symbol):
    """验证合约是否存在并可交易"""
    try:
        if exchange_id == "binance":
            exchange.options['defaultType'] = 'future'
        elif exchange_id == "okx":
            exchange.options['defaultType'] = 'swap'
        elif exchange_id == "bitget":
            exchange.options['defaultType'] = 'swap'
        
        # 加载市场
        exchange.load_markets(True)
        
        # 检查合约是否存在
        if contract_symbol in exchange.markets:
            market = exchange.markets[contract_symbol]
            active = market.get('active', False)
            
            if active:
                logger.info(f"合约 {contract_symbol} 存在且激活")
                return True
            else:
                logger.warning(f"合约 {contract_symbol} 存在但未激活")
                return False
        else:
            logger.warning(f"合约 {contract_symbol} 不存在")
            return False
            
    except Exception as e:
        logger.error(f"验证合约存在性失败: {e}")
        return False

def main():
    # 设置命令行参数
    parser = argparse.ArgumentParser(description='交易所交易功能测试工具')
    parser.add_argument('--symbol', type=str, default="DOGE/USDT",
                        help='交易对，例如 DOGE/USDT')
    parser.add_argument('--exchange', type=str, required=True,
                        choices=["gateio", "bitget", "binance", "okx", "bybit"],
                        help='要测试的交易所')
    parser.add_argument('--trade-type', type=str, required=True,
                        choices=["spot", "futures", "both"],
                        help='交易类型: spot(现货), futures(合约), both(两者)')
    parser.add_argument('--amount', type=float, default=5.0,
                        help='交易金额(USDT)')
    parser.add_argument('--leverage', type=int, default=3,
                        help='合约杠杆倍数')
    
    args = parser.parse_args()
    
    # 配置日志
    log_filename = setup_test_logger()
    
    try:
        logger.info(f"开始测试 {args.exchange} 交易功能，交易对: {args.symbol}, 金额: {args.amount} USDT")
        
        # 创建一个特定于当前交易所的args对象，用于初始化交易所
        exchange_args = argparse.Namespace(
            spot_exchange=args.exchange,
            future_exchange=args.exchange,
            test_mode=True
        )
        
        # 初始化交易所
        exchanges = init_exchanges(exchange_args)
        
        if args.exchange not in exchanges:
            logger.error(f"交易所 {args.exchange} 初始化失败")
            return
            
        exchange = exchanges[args.exchange]
        
        # 记录测试结果
        results = []
        
        # 测试现货交易
        if args.trade_type == "spot" or args.trade_type == "both":
            spot_success = test_spot_trading(exchange, args.exchange, args.symbol, args.amount)
            results.append(("spot", spot_success))
        
        # 测试合约交易
        if args.trade_type == "futures" or args.trade_type == "both":
            futures_success = test_futures_trading(exchange, args.exchange, args.symbol, args.amount, args.leverage)
            results.append(("futures", futures_success))
        
        # 输出测试结果摘要
        logger.info("\n" + "="*50)
        logger.info(f"测试结果摘要 - {args.exchange} - {args.symbol}")
        
        all_success = True
        for trade_type, success in results:
            status = "✅ 成功" if success else "❌ 失败"
            logger.info(f"{trade_type.upper()}: {status}")
            if not success:
                all_success = False
        
        if all_success:
            logger.info(f"\n🎉 所有测试均通过！{args.exchange} 交易功能正常。")
        else:
            logger.info(f"\n⚠️ 部分测试未通过，请检查日志了解详情: {log_filename}")
        
        logger.info("="*50)
        
    except Exception as e:
        logger.error(f"测试过程中发生错误: {e}")
        logger.error(traceback.format_exc())
        logger.info(f"\n⚠️ 测试过程中发生错误，请检查日志了解详情: {log_filename}")

if __name__ == "__main__":
    main() 