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
        
        # 对于Bitget，估算手续费（如果无法从订单中获取）
        if exchange_id == "bitget" and fee_amount == 0:
            # 假设手续费率为0.1%（实际应根据用户等级和交易所规则调整）
            estimated_fee = filled_amount * 0.001
            logger.info(f"估算 Bitget 手续费: {estimated_fee} {base_currency}")
            fee_amount = estimated_fee
        
        # 计算实际可卖出数量（减去手续费后）
        sell_amount = filled_amount - fee_amount
        
        # 确保卖出数量为正值
        if sell_amount <= 0:
            logger.error(f"计算的卖出数量非正值: {sell_amount}，终止测试")
            return False
            
        logger.info(f"实际可卖出数量: {sell_amount} {base_currency} (已扣除手续费)")
        
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
        
        # 设置合约参数
        if not setup_contract_settings(exchange, exchange_id, symbol, args):
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
            logger.info(f"Bitget合约交易 - 使用直接API调用")
            
            # 获取交易对名称 (对于Bitget需要特殊格式)
            symbol_name = contract_symbol.split(':')[0]  # 获取DOGE/USDT部分
            symbol_name = symbol_name.replace('/', '')   # 转换为DOGEUSDT格式
            logger.info(f"处理后的交易对名称: {symbol_name}")
            
            # 查找可用的API方法
            available_methods = []
            for method_name in dir(exchange):
                if 'mixpost' in method_name.lower() and 'order' in method_name.lower() and 'place' in method_name.lower():
                    available_methods.append(method_name)
            
            logger.info(f"找到的下单API方法: {available_methods}")
            
            # 如果找到了可用的方法
            if available_methods:
                # 优先使用V1版本的API
                api_method = None
                for method in available_methods:
                    if 'v1' in method.lower():
                        api_method = method
                        break
                
                # 如果没找到V1 API，使用第一个找到的方法
                if not api_method:
                    api_method = available_methods[0]
                
                logger.info(f"使用API方法: {api_method}")
                api_func = getattr(exchange, api_method)
                
                try:
                    # 根据API版本决定参数格式
                    if 'v1' in api_method.lower():
                        # V1 API使用不同的side参数
                        open_params = {
                            'symbol': symbol_name,
                            'marginCoin': 'USDT',
                            'size': str(int(quantity)),
                            'side': 'open_long',  # V1 API使用open_long
                            'orderType': 'market',
                            'marginMode': margin_mode
                        }
                    else:
                        # V2 API使用不同格式
                        open_params = {
                            'symbol': symbol_name,
                            'productType': 'USDT-FUTURES',
                            'marginMode': margin_mode,
                            'marginCoin': 'USDT',
                            'size': str(int(quantity)),
                            'side': 'buy',
                            'tradeSide': 'open',  # V2 API需要额外的tradeSide参数
                            'orderType': 'market',
                            'clientOid': f'test_open_{int(time.time() * 1000)}'
                        }
                    
                    logger.info(f"使用API方法开仓，参数: {open_params}")
                    open_response = api_func(open_params)
                    logger.info(f"开仓响应: {open_response}")
                    
                    # 等待持仓建立
                    time.sleep(3)
                    
                    # 平仓参数也根据API版本决定
                    if 'v1' in api_method.lower():
                        close_params = {
                            'symbol': symbol_name,
                            'marginCoin': 'USDT',
                            'size': str(int(quantity)),
                            'side': 'close_long',  # V1 API使用close_long
                            'orderType': 'market',
                            'marginMode': margin_mode
                        }
                    else:
                        close_params = {
                            'symbol': symbol_name,
                            'productType': 'USDT-FUTURES',
                            'marginMode': margin_mode,
                            'marginCoin': 'USDT',
                            'size': str(int(quantity)),
                            'side': 'sell',
                            'tradeSide': 'close',  # V2 API需要额外的tradeSide参数
                            'orderType': 'market',
                            'clientOid': f'test_close_{int(time.time() * 1000)}'
                        }
                    
                    logger.info(f"使用API方法平仓，参数: {close_params}")
                    close_response = api_func(close_params)
                    logger.info(f"平仓响应: {close_response}")
                    
                    logger.info(f"{exchange_id} API方法合约交易测试完成！")
                    return True
                except Exception as e:
                    logger.error(f"API方法调用失败: {e}")
                    logger.error(traceback.format_exc())
                    
                    # 直接尝试使用通用请求方法作为备选
                    logger.info("尝试使用通用请求方法...")
                    return try_generic_request_method(exchange, symbol_name, margin_mode, quantity, exchange_id)
            else:
                logger.error("未找到合适的API方法")
                # 使用通用请求方法作为备选
                return try_generic_request_method(exchange, symbol_name, margin_mode, quantity, exchange_id)
        else:
            # 其他交易所的代码
            # 4. 执行开多(市价买入)操作
            buy_params = {}
            if exchange_id == "binance":
                if position_mode == "hedge":
                    # 对冲模式需要指定仓位方向
                    buy_params = {"positionSide": "LONG"}
                    logger.info("使用对冲模式开仓，指定LONG仓位")
                else:
                    # 单向模式不需要指定
                    buy_params = {}
                    logger.info("使用单向模式开仓")
            elif exchange_id == "okx":
                buy_params = {'instType': 'SWAP', 'tdMode': margin_mode}
            elif exchange_id == "bybit":
                buy_params = {'category': 'linear', 'positionIdx': 0}
            
            logger.info(f"执行合约市价买入(开多)，参数: {buy_params}")
            buy_order = exchange.create_market_buy_order(contract_symbol, quantity, params=buy_params)
            
            if not buy_order:
                logger.error("开多订单创建失败")
                return False
                
            logger.info(f"开多订单执行结果: {buy_order}")
            time.sleep(2)  # 等待订单完成
            
            # 5. 获取实际成交数量
            filled_amount = 0
            
            # 尝试从订单信息中获取成交数量
            if 'filled' in buy_order and buy_order['filled'] is not None:
                filled_amount = float(buy_order['filled'])
            elif 'amount' in buy_order and buy_order['amount'] is not None:
                filled_amount = float(buy_order['amount'])
            elif 'info' in buy_order and 'filled' in buy_order['info'] and buy_order['info']['filled'] is not None:
                filled_amount = float(buy_order['info']['filled'])
            
            # 如果仍然无法获取，使用下单数量
            if filled_amount <= 0:
                filled_amount = quantity
                logger.warning(f"无法获取实际成交数量，将使用原始下单数量: {filled_amount}")
            
            logger.info(f"实际开多数量: {filled_amount}")
            
            # 确认持仓是否已建立
            try:
                # 获取当前持仓
                positions_params = {}
                if exchange_id == "bybit":
                    positions_params = {'category': 'linear'}
                elif exchange_id == "okx":
                    positions_params = {'instType': 'SWAP'}
                    
                positions = exchange.fetch_positions([contract_symbol], params=positions_params)
                logger.info(f"当前持仓: {positions}")
                
                long_position = None
                for pos in positions:
                    if pos['side'] == 'long' and float(pos.get('contracts', 0)) > 0:
                        long_position = pos
                        break
                        
                if not long_position:
                    logger.warning("未检测到多头持仓，但将继续尝试平仓")
            except Exception as e:
                logger.error(f"获取持仓信息失败: {e}")
                logger.error(traceback.format_exc())
            
            # 6. 执行平多(市价卖出)操作
            sell_params = buy_params.copy()
            if exchange_id == "bybit":
                sell_params['reduceOnly'] = True  # ByBit平仓需要设置reduceOnly
            
            # 对于Binance，确保平仓参数与开仓一致
            if exchange_id == "binance" and "positionSide" in sell_params:
                # 在对冲模式下，平仓时必须使用相同的positionSide
                logger.info(f"使用对冲模式平仓，指定 {sell_params['positionSide']} 仓位")
            
            logger.info(f"执行合约市价卖出(平多)，数量: {filled_amount}，参数: {sell_params}")
            sell_order = exchange.create_market_sell_order(contract_symbol, filled_amount, params=sell_params)
            
            logger.info(f"平多订单执行结果: {sell_order}")
            
            # 7. 验证交易结果
            if buy_order and sell_order:
                logger.info(f"{exchange_id} 合约交易测试成功！")
                return True
            else:
                logger.error(f"{exchange_id} 合约交易测试失败！")
                return False
    
    except Exception as e:
        logger.error(f"测试 {exchange_id} 合约交易失败: {e}")
        logger.error(traceback.format_exc())
        return False

def try_generic_request_method(exchange, symbol_name, margin_mode, quantity, exchange_id):
    """使用通用请求方法尝试执行合约交易"""
    logger.info("尝试使用通用请求方法")
    try:
        api_endpoint = "/api/mix/v1/order/placeOrder"
        
        # 开仓参数 - 对于V1 API，使用 open_long 和 close_long
        open_params = {
            'symbol': symbol_name,
            'marginCoin': 'USDT',
            'size': str(int(quantity)),
            'side': 'open_long',  # V1 API使用open_long而不是buy
            'orderType': 'market',
            'marginMode': margin_mode
        }
        
        logger.info(f"开仓参数: {open_params}")
        
        # 使用exchange.request方法
        open_response = exchange.request(
            'POST', 
            api_endpoint, 
            headers=exchange.sign(api_endpoint, open_params),
            body=json.dumps(open_params)
        )
        
        logger.info(f"开仓响应: {open_response}")
        
        # 等待持仓建立
        time.sleep(3)
        
        # 平仓参数
        close_params = {
            'symbol': symbol_name,
            'marginCoin': 'USDT',
            'size': str(int(quantity)),
            'side': 'close_long',  # V1 API使用close_long而不是sell
            'orderType': 'market',
            'marginMode': margin_mode
        }
        
        logger.info(f"平仓参数: {close_params}")
        
        # 发送平仓请求
        close_response = exchange.request(
            'POST', 
            api_endpoint, 
            headers=exchange.sign(api_endpoint, close_params),
            body=json.dumps(close_params)
        )
        
        logger.info(f"平仓响应: {close_response}")
        
        logger.info(f"{exchange_id} 通用请求方法合约交易测试完成！")
        return True
    except Exception as e:
        logger.error(f"通用请求方法失败: {e}")
        logger.error(traceback.format_exc())
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