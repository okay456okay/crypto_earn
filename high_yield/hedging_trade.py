import time
import logging
import ccxt
# from dotenv import load_dotenv
import numpy as np
import sys
import os
import argparse  # 添加命令行参数解析支持
import requests  # 添加requests库引用
import json
import traceback
import hashlib
import hmac


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
SYMBOL = "SAFE/USDT"
QUANTITY = 500
BY_AMOUNT = True  # 新增: True表示按金额交易，False表示按数量交易
THRESHOLD = -0.1  # 价格差异阈值
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

# 添加企业微信机器人配置
WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=6b190b76-009e-464e-9f5b-413555b453e8"
ENABLE_NOTIFICATION = True  # 是否启用通知


# 命令行参数解析
def parse_arguments():
    parser = argparse.ArgumentParser(description='对冲套利交易脚本')
    
    # 交易对参数
    parser.add_argument('--symbol', type=str, default=SYMBOL,
                        help='交易对，例如 BTC/USDT')
    
    # 交易数量或金额参数
    parser.add_argument('--quantity', type=float, default=QUANTITY,
                        help='交易数量或金额(取决于by-amount参数)')
    parser.add_argument('--by-amount', action='store_true', default=BY_AMOUNT,
                        help='按金额交易(True)或按数量交易(False)')
    
    # 交易所参数
    parser.add_argument('--spot-exchange', type=str, default=SPOT_EXCHANGE,
                        choices=["gateio", "bitget", "binance", "okx", "bybit"],
                        help='现货交易所')
    parser.add_argument('--future-exchange', type=str, default=FUTURE_EXCHANGE,
                        choices=["gateio", "bitget", "binance", "okx", "bybit"],
                        help='合约交易所')
    
    # 交易类型参数
    parser.add_argument('--trade-type', type=str, default=TRADE_TYPE,
                        choices=["spot_buy_future_short", "spot_sell_future_cover"],
                        help='交易类型: 买入现货做空合约，或卖出现货平仓合约')
    
    # 套利阈值参数
    parser.add_argument('--threshold', type=float, default=THRESHOLD,
                        help='价格差异阈值(百分比)，超过此值才执行套利')
    
    # 杠杆设置
    parser.add_argument('--leverage', type=int, default=LEVERAGE,
                        help='合约杠杆倍数')
    parser.add_argument('--margin-mode', type=str, default=MARGIN_MODE,
                        choices=["cross", "isolated"],
                        help='保证金模式: cross(全仓)或isolated(逐仓)')
    
    # 拆分订单设置
    parser.add_argument('--split-orders', action='store_true', default=SPLIT_ORDERS,
                        help='是否拆分订单')
    parser.add_argument('--split-by-value', action='store_true', default=SPLIT_BY_VALUE,
                        help='按金额拆分(True)或按数量拆分(False)')
    parser.add_argument('--split-size', type=float, default=SPLIT_SIZE,
                        help='拆分大小(金额或数量)')
    parser.add_argument('--split-delay', type=float, default=SPLIT_DELAY,
                        help='拆分订单之间的延迟(秒)')
    
    # 其他设置
    parser.add_argument('--retry-delay', type=float, default=RETRY_DELAY,
                        help='重试延迟(秒)')
    parser.add_argument('--log-file', type=str, default="",
                        help='日志文件路径，不指定则使用默认日志')
    
    # 通知设置
    parser.add_argument('--enable-notification', action='store_true', default=ENABLE_NOTIFICATION,
                        help='是否启用企业微信通知')
    parser.add_argument('--webhook-url', type=str, default=WEBHOOK_URL,
                        help='企业微信机器人Webhook URL')
    
    # 使用try-except捕获参数解析错误
    try:
        args = parser.parse_args()
        return args, parser
    except SystemExit:
        # 发生参数错误时，返回None和解析器对象
        return None, parser


# 配置日志
def setup_logger(args):
    # 如果指定了日志文件，创建一个特定币种的日志处理器
    if args.log_file:
        # 创建一个特定于当前交易对的文件处理器
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # 获取logger实例并添加处理器
        logger.addHandler(file_handler)
        logger.info(f"开始记录 {args.symbol} 的交易日志到 {args.log_file}")


# 初始化交易所API
def init_exchanges(args):
    try:
        exchanges = {}
        errors = {}  # 用于收集初始化错误
        
        # 测试模式：初始化所有交易所
        test_mode = getattr(args, 'test_mode', False)
        
        # 确定需要初始化的交易所列表
        exchange_ids_to_init = []
        
        if test_mode:
            # 检查是否有特殊的标记表示初始化所有交易所
            if getattr(args, 'spot_exchange', None) == "all_exchanges" or getattr(args, 'future_exchange', None) == "all_exchanges":
                # 初始化所有支持的交易所
                exchange_ids_to_init = ["gateio", "bitget", "binance", "okx", "bybit"]
            else:
                # 只初始化args中指定的交易所
                for exchange_id in ["gateio", "bitget", "binance", "okx", "bybit"]:
                    if args.spot_exchange == exchange_id or args.future_exchange == exchange_id:
                        exchange_ids_to_init.append(exchange_id)
        else:
            # 非测试模式：只初始化args中指定的交易所
            for exchange_id in ["gateio", "bitget", "binance", "okx", "bybit"]:
                if args.spot_exchange == exchange_id or args.future_exchange == exchange_id:
                    exchange_ids_to_init.append(exchange_id)
                    
        logger.info(f"将初始化以下交易所: {exchange_ids_to_init}")
        
        # GateIO配置
        if "gateio" in exchange_ids_to_init:
            try:
                exchanges["gateio"] = ccxt.gateio({
                    'apiKey': gateio_api_key,
                    'secret': gateio_api_secret,
                    'enableRateLimit': True,
                    'proxies': proxies,
                })
                logger.info("GateIO交易所初始化成功")
            except Exception as e:
                error_msg = f"GateIO交易所初始化失败: {e}"
                logger.error(error_msg)
                logger.error(traceback.format_exc())
                errors["gateio"] = error_msg

        # Bitget配置
        if "bitget" in exchange_ids_to_init:
            try:
                exchanges["bitget"] = ccxt.bitget({
                    'apiKey': bitget_api_key,
                    'secret': bitget_api_secret,
                    'password': bitget_api_passphrase,
                    'enableRateLimit': True,
                    'proxies': proxies,
                })
                logger.info("Bitget交易所初始化成功")
            except Exception as e:
                error_msg = f"Bitget交易所初始化失败: {e}"
                logger.error(error_msg)
                logger.error(traceback.format_exc())
                errors["bitget"] = error_msg
            
        # Binance配置
        if "binance" in exchange_ids_to_init:
            try:
                # 检查Binance API配置是否存在
                if not binance_api_key or not binance_api_secret:
                    raise ValueError("Binance API密钥或密钥未配置")
                
                exchanges["binance"] = ccxt.binance({
                    'apiKey': binance_api_key,
                    'secret': binance_api_secret,
                    'enableRateLimit': True,
                    'proxies': proxies,
                    'options': {
                        'defaultType': 'future',  # 使用合约API
                    }
                })
                # 测试连接
                exchanges["binance"].load_markets()
                logger.info("Binance交易所初始化成功")
            except Exception as e:
                error_msg = f"Binance交易所初始化失败: {e}"
                logger.error(error_msg)
                logger.error(traceback.format_exc())
                errors["binance"] = error_msg
            
        # OKX配置
        if "okx" in exchange_ids_to_init:
            try:
                if 'okx_api_key' not in globals() or 'okx_api_secret' not in globals() or 'okx_api_passphrase' not in globals():
                    raise ValueError("OKX API配置未导入或未定义")
                
                exchanges["okx"] = ccxt.okx({
                    'apiKey': okx_api_key,
                    'secret': okx_api_secret,
                    'password': okx_api_passphrase,
                    'enableRateLimit': True,
                    'proxies': proxies,
                })
                logger.info("OKX交易所初始化成功")
            except Exception as e:
                error_msg = f"OKX交易所初始化失败: {e}"
                logger.error(error_msg)
                logger.error(traceback.format_exc())
                errors["okx"] = error_msg
            
        # Bybit配置
        if "bybit" in exchange_ids_to_init:
            try:
                if 'bybit_api_key' not in globals() or 'bybit_api_secret' not in globals():
                    raise ValueError("Bybit API配置未导入或未定义")
                
                exchanges["bybit"] = ccxt.bybit({
                    'apiKey': bybit_api_key,
                    'secret': bybit_api_secret,
                    'enableRateLimit': True,
                    'proxies': proxies,
                })
                logger.info("Bybit交易所初始化成功")
            except Exception as e:
                error_msg = f"Bybit交易所初始化失败: {e}"
                logger.error(error_msg)
                logger.error(traceback.format_exc())
                errors["bybit"] = error_msg

        # 检查关键交易所是否初始化成功（只在非测试模式下执行）
        if not test_mode:
            if args.spot_exchange not in exchanges:
                error_msg = f"现货交易所 {args.spot_exchange} 初始化失败"
                if args.spot_exchange in errors:
                    error_msg += f": {errors[args.spot_exchange]}"
                logger.error(error_msg)
                raise ValueError(error_msg)
                
            if args.future_exchange not in exchanges:
                error_msg = f"合约交易所 {args.future_exchange} 初始化失败"
                if args.future_exchange in errors:
                    error_msg += f": {errors[args.future_exchange]}"
                logger.error(error_msg)
                raise ValueError(error_msg)

        logger.info("交易所API初始化成功")
        return exchanges
    except Exception as e:
        logger.error(f"初始化交易所API失败: {e}")
        logger.error(traceback.format_exc())
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
def setup_contract_settings(exchange, exchange_id, symbol, args):
    try:
        # 获取特定交易所的合约交易对格式
        contract_symbol = get_contract_symbol(exchange_id, symbol)

        # 针对不同交易所设置保证金模式和杠杆
        if exchange_id == "bitget":
            # 设置保证金模式为全仓
            exchange.set_margin_mode(args.margin_mode, contract_symbol)
            logger.info(f"已设置{exchange_id} {contract_symbol}为{args.margin_mode}模式")
            
            # 设置杠杆倍数
            exchange.set_leverage(args.leverage, contract_symbol)
            logger.info(f"已设置{exchange_id} {contract_symbol}杠杆为{args.leverage}倍")
            
        elif exchange_id == "binance":
            # Binance特有的设置方式
            exchange.set_margin_mode(args.margin_mode, contract_symbol)
            exchange.set_leverage(args.leverage, contract_symbol)
            logger.info(f"已设置{exchange_id} {contract_symbol}为{args.margin_mode}模式, 杠杆为{args.leverage}倍")
            
        elif exchange_id == "okx":
            # OKX特有的设置方式
            exchange.set_leverage(args.leverage, contract_symbol, params={"marginMode": args.margin_mode})
            logger.info(f"已设置{exchange_id} {contract_symbol}为{args.margin_mode}模式, 杠杆为{args.leverage}倍")
            
        elif exchange_id == "bybit":
            # Bybit特有的设置方式
            exchange.set_leverage(args.leverage, contract_symbol)
            exchange.set_margin_mode(args.margin_mode, contract_symbol)
            logger.info(f"已设置{exchange_id} {contract_symbol}为{args.margin_mode}模式, 杠杆为{args.leverage}倍")
            
        elif exchange_id == "gateio":
            # GateIO合约设置
            params = {
                'leverage': args.leverage,
                'marginMode': args.margin_mode,
            }
            exchange.set_leverage(args.leverage, contract_symbol, params=params)
            logger.info(f"已设置{exchange_id} {contract_symbol}为{args.margin_mode}模式, 杠杆为{args.leverage}倍")

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
def get_prices_with_depth(exchanges, args):
    try:
        spot_exchange = exchanges[args.spot_exchange]
        future_exchange = exchanges[args.future_exchange]
        
        if args.trade_type == "spot_buy_future_short":
            # 现货买入，合约卖空
            # 获取现货订单簿，计算买入加权价格
            spot_orderbook = spot_exchange.fetch_order_book(args.symbol, 50)
            spot_price, spot_enough_depth = calculate_weighted_price(
                spot_orderbook, args.quantity, 'asks'  # 买入需要看卖单asks
            )
            
            # 获取合约订单簿，计算卖出加权价格
            contract_symbol = get_contract_symbol(args.future_exchange, args.symbol)
            future_orderbook = future_exchange.fetch_order_book(contract_symbol, 50)
            
            # 由于杠杆，实际合约数量可能不同
            contract_quantity = args.quantity / args.leverage
            future_price, future_enough_depth = calculate_weighted_price(
                future_orderbook, contract_quantity, 'bids'  # 卖出需要看买单bids
            )
            
            logger.info(f"{args.spot_exchange}加权买入价格: {spot_price}, 深度足够: {spot_enough_depth}")
            logger.info(f"{args.future_exchange}加权卖出价格: {future_price}, 深度足够: {future_enough_depth}")
            
        elif args.trade_type == "spot_sell_future_cover":
            # 现货卖出，合约平仓
            # 获取现货订单簿，计算卖出加权价格
            spot_orderbook = spot_exchange.fetch_order_book(args.symbol, 50)
            spot_price, spot_enough_depth = calculate_weighted_price(
                spot_orderbook, args.quantity, 'bids'  # 卖出需要看买单bids
            )
            
            # 获取合约订单簿，计算买入平仓加权价格
            contract_symbol = get_contract_symbol(args.future_exchange, args.symbol)
            future_orderbook = future_exchange.fetch_order_book(contract_symbol, 50)
            
            # 由于杠杆，实际合约数量可能不同
            contract_quantity = args.quantity / args.leverage
            future_price, future_enough_depth = calculate_weighted_price(
                future_orderbook, contract_quantity, 'asks'  # 买入平仓需要看卖单asks
            )
            
            logger.info(f"{args.spot_exchange}加权卖出价格: {spot_price}, 深度足够: {spot_enough_depth}")
            logger.info(f"{args.future_exchange}加权买入价格(平仓): {future_price}, 深度足够: {future_enough_depth}")

        if not spot_enough_depth:
            logger.warning(f"{args.spot_exchange}订单簿深度不足以满足{args.quantity}的交易量")

        if not future_enough_depth:
            logger.warning(f"{args.future_exchange}订单簿深度不足以满足{contract_quantity}的交易量")

        return spot_price, future_price, spot_enough_depth and future_enough_depth
    except Exception as e:
        logger.error(f"获取深度价格失败: {e}")
        return None, None, False


# 计算套利价差百分比
def calculate_price_difference(spot_price, future_price, args):
    if spot_price is None or future_price is None:
        return None

    if args.trade_type == "spot_buy_future_short":
        # 现货买入, 合约卖空, 预期future_price > spot_price才有利可图
        difference = (future_price - spot_price) / spot_price * 100
    elif args.trade_type == "spot_sell_future_cover":
        # 现货卖出, 合约平仓买入, 预期spot_price > future_price才有利可图
        difference = (spot_price - future_price) / future_price * 100
    
    logger.info(f"价格差异: {difference:.4f}%")
    return difference


# 检查余额是否足够 - 添加对GateIO的特殊处理
def check_balances(exchanges, args):
    """
    检查交易所账户余额是否足够执行交易
    
    Args:
        exchanges: 交易所API对象字典
        args: 命令行参数对象
    
    Returns:
        (bool, str): 余额是否足够的布尔值，以及相关消息
    """
    try:
        spot_exchange = exchanges[args.spot_exchange]
        future_exchange = exchanges[args.future_exchange]
        
        # 解析交易对，获取基础货币和报价货币
        base_currency, quote_currency = args.symbol.split('/')
        
        if args.trade_type == "spot_buy_future_short":
            # 检查现货账户USDT余额是否足够购买
            spot_balances = spot_exchange.fetch_balance()
            spot_quote_balance = spot_balances.get(quote_currency, {}).get('free', 0)
            
            # 计算购买所需的报价货币金额 (预估，不考虑滑点)
            # 获取当前市场价格
            ticker = spot_exchange.fetch_ticker(args.symbol)
            estimated_price = ticker['last']  # 最新成交价
            required_quote_amount = args.quantity * estimated_price
            
            # 检查现货账户余额是否足够
            if spot_quote_balance < required_quote_amount:
                message = (f"{args.spot_exchange}现货账户{quote_currency}余额不足! "
                          f"需要: {required_quote_amount:.4f}, 可用: {spot_quote_balance:.4f}")
                logger.error(message)
                return False, message
            
            # 检查合约账户保证金是否足够
            future_margin_balance = 0
            margin_currency = quote_currency
            
            # 特别处理Bitget合约账户余额获取 - 只使用方法1
            if args.future_exchange == "bitget":
                # 使用type=swap参数获取合约账户余额
                contract_balance = future_exchange.fetch_balance({'type': 'swap'})
                
                # 获取free值
                if margin_currency in contract_balance and 'free' in contract_balance[margin_currency]:
                    future_margin_balance = contract_balance[margin_currency]['free']
                    logger.info(f"Bitget合约账户{margin_currency}余额: free={future_margin_balance}")
                else:
                    logger.error(f"无法从Bitget合约账户获取{margin_currency}的free值")
                    future_margin_balance = 0
            
            # 特别处理GateIO合约账户余额获取 - 使用直接请求方法
            elif args.future_exchange == "gateio":
                try:
                    # 直接使用底层HTTP请求访问官方API端点
                    raw_response = future_exchange.request('GET', 'futures/usdt/accounts')
                    
                    # 解析响应找到对应的货币
                    if isinstance(raw_response, list):
                        for account in raw_response:
                            if account.get('currency') == margin_currency:
                                future_margin_balance = float(account.get('available', 0))
                                logger.info(f"GateIO合约账户{margin_currency}余额: available={future_margin_balance}")
                                break
                        else:
                            logger.error(f"在GateIO合约账户响应中未找到{margin_currency}货币")
                            future_margin_balance = 0
                    else:
                        logger.error(f"GateIO合约账户响应格式不是列表: {raw_response}")
                        future_margin_balance = 0
                except Exception as e:
                    logger.error(f"获取GateIO合约账户余额失败: {e}")
                    logger.error(traceback.format_exc())
                    future_margin_balance = 0
            else:
                # 其他交易所使用标准方法
                future_balances = future_exchange.fetch_balance()
                future_margin_balance = future_balances.get(margin_currency, {}).get('free', 0)
            
            # 计算开空所需的保证金 (考虑杠杆)
            required_margin = (required_quote_amount / args.leverage) * 1.05  # 增加5%的安全边际
            
            # 检查合约账户保证金是否足够
            if future_margin_balance < required_margin:
                message = (f"{args.future_exchange}合约账户{margin_currency}保证金不足! "
                          f"需要: {required_margin:.4f}, 可用: {future_margin_balance:.4f}")
                logger.error(message)
                return False, message
            
            logger.info(f"{args.spot_exchange}现货账户{quote_currency}余额: {spot_quote_balance:.4f}, 需要: {required_quote_amount:.4f}")
            logger.info(f"{args.future_exchange}合约账户{margin_currency}保证金: {future_margin_balance:.4f}, 需要: {required_margin:.4f}")
            return True, "账户余额检查通过，可以执行交易"
            
        elif args.trade_type == "spot_sell_future_cover":
            # 检查现货账户基础货币余额是否足够卖出
            spot_balances = spot_exchange.fetch_balance()
            spot_base_balance = spot_balances.get(base_currency, {}).get('free', 0)
            
            # 检查现货账户余额是否足够
            if spot_base_balance < args.quantity:
                message = (f"{args.spot_exchange}现货账户{base_currency}余额不足! "
                          f"需要: {args.quantity}, 可用: {spot_base_balance}")
                logger.error(message)
                return False, message
            
            # 检查合约账户是否有足够的空头头寸可以平仓
            try:
                # 获取合约账户当前持仓
                contract_symbol = get_contract_symbol(args.future_exchange, args.symbol)
                positions = future_exchange.fetch_positions([contract_symbol])
                
                # 查找相关合约的空头头寸
                short_position = None
                for position in positions:
                    # 检查是否是对应的合约和空头方向
                    if position['symbol'] == contract_symbol and position['side'] == 'short':
                        short_position = position
                        break
                
                if short_position is None or abs(float(short_position['contracts'])) < (args.quantity / args.leverage):
                    message = (f"{args.future_exchange}合约账户没有足够的{base_currency}空头头寸可以平仓! "
                              f"需要: {args.quantity / args.leverage}, 持有: {abs(float(short_position['contracts'])) if short_position else 0}")
                    logger.error(message)
                    return False, message
                
                logger.info(f"{args.spot_exchange}现货账户{base_currency}余额: {spot_base_balance}, 需要: {args.quantity}")
                logger.info(f"{args.future_exchange}合约账户{base_currency}空头头寸: {abs(float(short_position['contracts']))}, 需要: {args.quantity / args.leverage}")
                return True, "账户余额和头寸检查通过，可以执行交易"
                
            except Exception as e:
                message = f"检查合约头寸失败: {e}"
                logger.error(message)
                return False, message
    
    except Exception as e:
        message = f"检查账户余额失败: {e}"
        logger.error(message)
        return False, message


# 拆分执行购买订单
def execute_split_buy_orders(exchange, future_exchange, symbol, contract_symbol, total_quantity, spot_price, future_price, args):
    """
    将大额买入订单拆分为多个小订单执行，每次执行后重新检查套利条件
    """
    orders_info = []
    total_filled = 0
    total_cost = 0
    remaining = total_quantity
    
    # 如果按金额拆分，需要获取当前市场价格来估算每批数量
    if args.split_by_value:
        try:
            ticker = exchange.fetch_ticker(symbol)
            current_price = ticker['last']  # 最新成交价
            
            # 计算总价值
            total_value = total_quantity * current_price
            logger.info(f"开始拆分执行买入订单，总价值约: {total_value:.2f} USDT, 每批金额: {args.split_size} USDT")
            
            # 重新计算总数量
            remaining_value = total_value
            remaining = total_quantity  # 仍然保持数量计数
        except Exception as e:
            logger.error(f"获取市场价格失败，无法按金额拆分: {e}")
            return [], 0, 0, False
    else:
        logger.info(f"开始拆分执行买入订单，总数量: {total_quantity}, 每批数量: {args.split_size}")
    
    while remaining > 0:
        # 计算当前批次的数量
        if args.split_by_value:
            # 根据剩余金额和当前价格计算当前批次数量
            batch_value = min(args.split_size, remaining_value)
            current_quantity = min(batch_value / current_price, remaining)
            # 金额太小可能导致数量过小，设置最小交易数量
            if current_quantity * current_price < exchange.markets[symbol].get('limits', {}).get('cost', {}).get('min', 5):
                current_quantity = remaining  # 如果剩余金额太小，直接交易所有剩余数量
        else:
            current_quantity = min(args.split_size, remaining)
        
        try:
            # 执行单笔交易前重新检查市场条件
            if len(orders_info) > 0:  # 不是第一笔订单，重新检查
                # 获取最新市场价格
                new_spot_price, new_future_price, enough_depth = get_prices_with_depth(
                    {args.spot_exchange: exchange, args.future_exchange: future_exchange}, 
                    args
                )
                
                # 检查市场条件是否仍然满足
                if new_spot_price is None or new_future_price is None or not enough_depth:
                    logger.warning("获取价格失败或深度不足，停止继续拆分交易")
                    break
                
                # 检查价格差异
                price_difference = calculate_price_difference(new_spot_price, new_future_price, args)
                if price_difference is None or price_difference <= args.threshold:
                    logger.warning(f"价格差异 {price_difference if price_difference else 'N/A'}% 不再满足条件，停止继续拆分交易")
                    break
                
                # 更新当前价格估计
                if args.split_by_value:
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
            
            if args.split_by_value:
                logger.info(f"第{len(orders_info)}批买入订单执行成功: 数量={filled}, 成本={cost:.4f} USDT")
                remaining_value -= cost
            else:
                logger.info(f"第{len(orders_info)}批买入订单执行成功: 数量={filled}, 成本={cost:.4f} USDT")
            
            # 更新剩余数量
            remaining -= filled
            
            # 调整当前价格估算（基于最新成交价）
            if args.split_by_value and cost > 0 and filled > 0:
                current_price = cost / filled
            
            # 添加延迟，避免频繁下单
            if remaining > 0:
                time.sleep(args.split_delay)
                
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
def execute_split_sell_orders(exchange, future_exchange, symbol, contract_symbol, total_quantity, spot_price, future_price, args):
    """
    将大额卖出订单拆分为多个小订单执行，每次执行后重新检查套利条件
    """
    orders_info = []
    total_filled = 0
    total_proceeds = 0
    remaining = total_quantity
    
    # 如果按金额拆分，需要获取当前市场价格来估算每批数量
    if args.split_by_value:
        try:
            ticker = exchange.fetch_ticker(symbol)
            current_price = ticker['last']  # 最新成交价
            
            # 计算总价值
            total_value = total_quantity * current_price
            logger.info(f"开始拆分执行卖出订单，总价值约: {total_value:.2f} USDT, 每批金额: {args.split_size} USDT")
            
            # 重新计算总数量
            remaining_value = total_value
            remaining = total_quantity  # 仍然保持数量计数
        except Exception as e:
            logger.error(f"获取市场价格失败，无法按金额拆分: {e}")
            return [], 0, 0, False
    else:
        logger.info(f"开始拆分执行卖出订单，总数量: {total_quantity}, 每批数量: {args.split_size}")
    
    while remaining > 0:
        # 计算当前批次的数量
        if args.split_by_value:
            # 根据剩余金额和当前价格计算当前批次数量
            batch_value = min(args.split_size, remaining_value)
            current_quantity = min(batch_value / current_price, remaining)
            # 金额太小可能导致数量过小，设置最小交易数量
            if current_quantity * current_price < exchange.markets[symbol].get('limits', {}).get('cost', {}).get('min', 5):
                current_quantity = remaining  # 如果剩余金额太小，直接交易所有剩余数量
        else:
            current_quantity = min(args.split_size, remaining)
        
        try:
            # 执行单笔交易前重新检查市场条件
            if len(orders_info) > 0:  # 不是第一笔订单，重新检查
                # 获取最新市场价格
                new_spot_price, new_future_price, enough_depth = get_prices_with_depth(
                    {args.spot_exchange: exchange, args.future_exchange: future_exchange}, 
                    args
                )
                
                # 检查市场条件是否仍然满足
                if new_spot_price is None or new_future_price is None or not enough_depth:
                    logger.warning("获取价格失败或深度不足，停止继续拆分交易")
                    break
                
                # 检查价格差异
                price_difference = calculate_price_difference(new_spot_price, new_future_price, args)
                if price_difference is None or price_difference <= args.threshold:
                    logger.warning(f"价格差异 {price_difference if price_difference else 'N/A'}% 不再满足条件，停止继续拆分交易")
                    break
                
                # 更新当前价格估计
                if args.split_by_value:
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
            
            if args.split_by_value:
                logger.info(f"第{len(orders_info)}批卖出订单执行成功: 数量={filled}, 收入={proceeds:.4f} USDT")
                remaining_value -= proceeds
            else:
                logger.info(f"第{len(orders_info)}批卖出订单执行成功: 数量={filled}, 收入={proceeds:.4f} USDT")
            
            # 更新剩余数量
            remaining -= filled
            
            # 调整当前价格估算（基于最新成交价）
            if args.split_by_value and proceeds > 0 and filled > 0:
                current_price = proceeds / filled
            
            # 添加延迟，避免频繁下单
            if remaining > 0:
                time.sleep(args.split_delay)
                
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
        # 解析交易对，获取基础货币和报价货币
        base_currency, quote_currency = symbol.split('/')
        spot_exchange = exchanges[SPOT_EXCHANGE]
        future_exchange = exchanges[FUTURE_EXCHANGE]
        contract_symbol = get_contract_symbol(FUTURE_EXCHANGE, symbol)
        
        # 如果是按金额交易，需要计算数量
        if BY_AMOUNT:
            # 获取现货市场价格来估算交易数量
            try:
                ticker = spot_exchange.fetch_ticker(symbol)
                current_price = ticker['last']  # 最新成交价
                estimated_quantity = QUANTITY / current_price
                logger.info(f"按金额交易: {QUANTITY} USDT, 预估数量: {estimated_quantity:.6f} {base_currency}")
                actual_quantity = estimated_quantity  # 初始设置为预估数量
            except Exception as e:
                logger.error(f"获取市场价格失败，无法计算交易数量: {e}")
                return None, None
        else:
            actual_quantity = QUANTITY  # 按数量交易直接使用指定数量
            
        contract_quantity = actual_quantity / LEVERAGE  # 考虑杠杆的合约数量
        
        # 每次交易前都检查余额并详细记录
        spot_balances = spot_exchange.fetch_balance()
        future_balances = future_exchange.fetch_balance()
        
        # 记录现货账户余额
        spot_base_balance = spot_balances.get(base_currency, {}).get('free', 0)
        spot_quote_balance = spot_balances.get(quote_currency, {}).get('free', 0)
        logger.info(f"现货账户余额: {base_currency}={spot_base_balance}, {quote_currency}={spot_quote_balance}")
        
        # 记录合约账户余额
        future_quote_balance = future_balances.get(quote_currency, {}).get('free', 0)
        logger.info(f"合约账户余额: {quote_currency}={future_quote_balance}")
        
        # 检查合约账户持仓情况
        try:
            positions = future_exchange.fetch_positions([contract_symbol])
            short_position = None
            for position in positions:
                if position['symbol'] == contract_symbol and position['side'] == 'short':
                    short_position = position
                    logger.info(f"当前空头持仓: {abs(float(short_position['contracts']))}, 合约价值: {abs(float(short_position['notional']))}")
                    break
        except Exception as e:
            logger.warning(f"获取合约持仓信息失败: {e}")
        
        # 正式检查交易所需的余额是否足够
        args = argparse.Namespace(
            symbol=symbol, 
            quantity=QUANTITY,
            by_amount=BY_AMOUNT,  # 添加按金额交易标志
            spot_exchange=SPOT_EXCHANGE, 
            future_exchange=FUTURE_EXCHANGE,
            trade_type=TRADE_TYPE,
            leverage=LEVERAGE,
            threshold=THRESHOLD,
            split_orders=SPLIT_ORDERS,
            split_by_value=SPLIT_BY_VALUE,
            split_size=SPLIT_SIZE,
            split_delay=SPLIT_DELAY
        )
        balances_ok, message = check_balances(exchanges, args)
        if not balances_ok:
            logger.error(f"余额检查失败，无法执行交易: {message}")
            # 添加失败通知
            send_wechat_notification(f"❌ 套利交易余额检查失败\n交易对: {symbol}\n原因: {message}", is_success=False)
            return None, None
        else:
            logger.info(f"余额检查通过: {message}")
        
        theoretical_profit = 0
        
        # 进行估计值计算
        if BY_AMOUNT:
            spot_value = QUANTITY  # 按金额交易直接使用指定金额
            future_value = QUANTITY / LEVERAGE  # 合约按比例减少
        else:
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
                # 每个批次交易前都重新检查余额
                spot_orders, spot_cost, spot_avg_price, spot_completed = execute_split_buy_orders(
                    spot_exchange, future_exchange, symbol, contract_symbol, QUANTITY, spot_price, future_price, args)
                spot_order = {"orders": spot_orders, "totalCost": spot_cost, "avgPrice": spot_avg_price, "completed": spot_completed}
                logger.info(f"{SPOT_EXCHANGE}拆分买入订单执行情况: 完成状态={spot_completed}, 平均成交价={spot_avg_price:.4f}")
                
                # 如果现货交易没有完成全部数量，可能不需要执行全部合约交易
                if not spot_completed:
                    # 计算实际买入的现货数量
                    actual_quantity = sum([float(order.get('filled', 0)) for order in spot_orders])
                    # 重新计算对应的合约数量
                    contract_quantity = actual_quantity / LEVERAGE
                    logger.info(f"由于现货交易未完成，调整合约交易数量为: {contract_quantity}")
                
                # 现货交易结果通知
                if spot_completed:
                    send_wechat_notification(f"✅ {SPOT_EXCHANGE}拆分买入\n交易对: {symbol}\n总成交: {sum([float(order.get('filled', 0)) for order in spot_orders])}\n总成本: {spot_cost:.4f} USDT\n平均价格: {spot_avg_price:.6f}")
                else:
                    send_wechat_notification(f"⚠️ {SPOT_EXCHANGE}拆分买入部分完成\n交易对: {symbol}\n总成交: {sum([float(order.get('filled', 0)) for order in spot_orders])}\n总成本: {spot_cost:.4f} USDT")
                
            else:
                # 如果不需要拆分，则直接执行单个订单
                params = {}
                if SPOT_EXCHANGE == "binance":
                    params = {"quoteOrderQty": QUANTITY}
                elif SPOT_EXCHANGE == "okx":
                    params = {"notional": QUANTITY}
                elif SPOT_EXCHANGE == "gateio":
                    params = {"cost": QUANTITY}
                elif SPOT_EXCHANGE == "bitget":
                    params = {"amount": QUANTITY}
                elif SPOT_EXCHANGE == "bybit":
                    params = {"orderAmount": QUANTITY}
                
                spot_order = spot_exchange.create_market_buy_order(symbol, None, params=params)
                logger.info(f"{SPOT_EXCHANGE}买入订单执行成功: {spot_order}")
                spot_avg_price = spot_price  # 使用预估价格
                spot_completed = True
                
                # 现货单笔交易结果通知
                filled = float(spot_order.get('filled', 0))
                cost = float(spot_order.get('cost', 0))
                send_wechat_notification(f"✅ {SPOT_EXCHANGE}买入\n交易对: {symbol}\n数量: {filled}\n成本: {cost:.4f} USDT\n价格: {cost/filled if filled > 0 else 0:.6f}")

            # 在合约交易所开空之前，再次检查余额
            future_quote_balance = 0
            logger.info(f"执行合约交易前再次检查合约账户余额...")

            # 获取Bitget或GateIO合约账户余额 - 使用特殊方法
            if FUTURE_EXCHANGE == "bitget":
                # 使用type=swap参数获取合约账户余额
                contract_balance = future_exchange.fetch_balance({'type': 'swap'})
                
                # 获取free值
                if quote_currency in contract_balance and 'free' in contract_balance[quote_currency]:
                    future_quote_balance = contract_balance[quote_currency]['free']
                    logger.info(f"Bitget合约账户{quote_currency}余额: free={future_quote_balance}")
                else:
                    logger.error(f"无法从Bitget合约账户获取{quote_currency}的free值")
                    future_quote_balance = 0
            elif FUTURE_EXCHANGE == "gateio":
                # 使用专用函数获取GateIO合约余额
                future_margin_balance = get_gateio_futures_balance(future_exchange, quote_currency)
                logger.info(f"{FUTURE_EXCHANGE}合约账户余额: {quote_currency}={future_margin_balance}")
            else:
                future_balances = future_exchange.fetch_balance()
                future_quote_balance = future_balances.get(quote_currency, {}).get('free', 0)
                logger.info(f"执行合约交易前再次检查合约账户余额: {quote_currency}={future_quote_balance}")
            
            # 计算所需保证金，考虑5%的安全边际
            required_margin = (contract_quantity * future_price) * 1.05
            if future_quote_balance < required_margin:
                logger.error(f"合约账户保证金不足，无法执行卖空操作。需要: {required_margin:.4f}, 可用: {future_quote_balance:.4f}")
                send_wechat_notification(f"❌ {FUTURE_EXCHANGE}合约账户保证金不足\n需要: {required_margin:.4f}, 可用: {future_quote_balance:.4f}", is_success=False)
                return spot_order, None
            
            # 在合约交易所卖空
            if contract_quantity <= 0:
                logger.warning("由于现货交易未完成，没有合约需要交易")
                send_wechat_notification(f"⚠️ {FUTURE_EXCHANGE}没有执行合约交易\n原因: 现货交易未完成", is_success=False)
                future_order = None
                future_avg_price = 0
                future_completed = False
            elif need_future_split and contract_quantity > SPLIT_SIZE / LEVERAGE:
                future_orders, future_proceeds, future_avg_price, future_completed = execute_split_sell_orders(
                    future_exchange, spot_exchange, contract_symbol, symbol, contract_quantity, spot_price, future_price)
                future_order = {"orders": future_orders, "totalProceeds": future_proceeds, "avgPrice": future_avg_price, "completed": future_completed}
                logger.info(f"{FUTURE_EXCHANGE}拆分卖出订单执行情况: 完成状态={future_completed}, 平均成交价={future_avg_price:.4f}")
                
                # 合约交易结果通知
                if future_completed:
                    send_wechat_notification(f"✅ {FUTURE_EXCHANGE}拆分卖出(做空)成功\n合约: {contract_symbol}\n总成交: {sum([float(order.get('filled', 0)) for order in future_orders])}\n总收入: {future_proceeds:.4f} USDT\n平均价格: {future_avg_price:.6f}")
                else:
                    send_wechat_notification(f"⚠️ {FUTURE_EXCHANGE}拆分卖出(做空)部分完成\n合约: {contract_symbol}\n总成交: {sum([float(order.get('filled', 0)) for order in future_orders])}\n总收入: {future_proceeds:.4f} USDT", is_success=False)
            else:
                # 如果不需要拆分，则直接执行单个订单
                future_order = future_exchange.create_market_sell_order(
                    contract_symbol,
                    contract_quantity
                )
                logger.info(f"{FUTURE_EXCHANGE}卖出(做空)订单执行成功 ({LEVERAGE}倍杠杆): {future_order}")
                future_avg_price = future_price  # 使用预估价格
                future_completed = True
                
                # 合约单笔交易结果通知
                filled = float(future_order.get('filled', 0))
                proceeds = float(future_order.get('cost', 0))
                send_wechat_notification(f"✅ {FUTURE_EXCHANGE}卖出(做空)成功\n合约: {contract_symbol}\n数量: {filled}\n收入: {proceeds:.4f} USDT\n价格: {proceeds/filled if filled > 0 else 0:.6f}")

            # 计算实际利润并通知
            if spot_order and future_order:
                actual_spot_price = spot_avg_price if isinstance(spot_order, dict) and "avgPrice" in spot_order else spot_price
                actual_future_price = future_avg_price if isinstance(future_order, dict) and "avgPrice" in future_order else future_price
                
                # 获取实际交易的数量
                if isinstance(spot_order, dict) and "orders" in spot_order:
                    actual_quantity = sum([float(order.get('filled', 0)) for order in spot_order["orders"]])
                else:
                    actual_quantity = float(spot_order.get('filled', estimated_quantity))
                
                theoretical_profit = (actual_future_price - actual_spot_price) * actual_quantity
                logger.info(f"理论利润: {theoretical_profit:.4f} USDT (基于实际交易数量: {actual_quantity})")
                
                # 发送套利完成总结通知
                profit_message = f"🎉 套利交易完成\n" + \
                                f"交易对: {symbol}\n" + \
                                f"现货买入: {actual_quantity} @ {actual_spot_price:.6f}\n" + \
                                f"合约卖空: {contract_quantity} @ {actual_future_price:.6f}\n" + \
                                f"理论利润: {theoretical_profit:.4f} USDT\n" + \
                                f"价差: {((actual_future_price - actual_spot_price) / actual_spot_price * 100):.4f}%"
                
                send_wechat_notification(profit_message)
            
            # 检查是否两边都完成
            if (isinstance(spot_order, dict) and spot_order.get("completed", False) == False) or \
               (isinstance(future_order, dict) and future_order.get("completed", False) == False):
                logger.warning("交易未完全执行，可能需要手动处理剩余部分")
                send_wechat_notification("⚠️ 套利交易部分完成\n可能需要手动处理剩余部分", is_success=False)
        
        elif TRADE_TYPE == "spot_sell_future_cover":
            # 再次详细检查持仓情况
            try:
                positions = future_exchange.fetch_positions([contract_symbol])
                short_position = None
                for position in positions:
                    if position['symbol'] == contract_symbol and position['side'] == 'short':
                        short_position = position
                        short_size = abs(float(short_position['contracts']))
                        logger.info(f"交易前确认空头持仓: {short_size}, 需要平仓数量: {contract_quantity}")
                        if short_size < contract_quantity:
                            logger.error(f"空头持仓不足，无法完成平仓操作。持有: {short_size}, 需要: {contract_quantity}")
                            return None, None
                        break
                
                if short_position is None:
                    logger.error(f"未找到{contract_symbol}的空头持仓，无法执行平仓操作")
                    send_wechat_notification(f"❌ 合约检查失败\n未找到{contract_symbol}的空头持仓，无法执行平仓操作", is_success=False)
                    return None, None
            except Exception as e:
                logger.error(f"获取合约持仓信息失败: {e}")
                send_wechat_notification(f"❌ 合约检查失败\n获取合约持仓信息失败: {str(e)}", is_success=False)
                return None, None
                
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
                    spot_exchange, future_exchange, symbol, contract_symbol, QUANTITY, spot_price, future_price)
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

            # 在合约交易所买入平仓前，再次检查余额和持仓
            future_balances = future_exchange.fetch_balance()
            future_quote_balance = future_balances.get(quote_currency, {}).get('free', 0)
            logger.info(f"执行合约平仓前再次检查合约账户余额: {quote_currency}={future_quote_balance}")
            
            # 计算所需保证金，考虑5%的安全边际
            required_balance = (contract_quantity * future_price) * 1.05
            if future_quote_balance < required_balance:
                logger.error(f"合约账户余额不足，无法执行平仓操作。需要: {required_balance:.4f}, 可用: {future_quote_balance:.4f}")
                send_wechat_notification(f"❌ {FUTURE_EXCHANGE}合约账户余额不足\n需要: {required_balance:.4f}, 可用: {future_quote_balance:.4f}", is_success=False)
                return spot_order, None
            
            # 在合约交易所买入平仓
            if contract_quantity <= 0:
                logger.warning("由于现货交易未完成，没有合约需要交易")
                send_wechat_notification(f"⚠️ {FUTURE_EXCHANGE}没有执行合约交易\n原因: 现货交易未完成", is_success=False)
                future_order = None
                future_avg_price = 0
                future_completed = False
            elif need_future_split and contract_quantity > SPLIT_SIZE / LEVERAGE:
                future_orders, future_cost, future_avg_price, future_completed = execute_split_buy_orders(
                    future_exchange, spot_exchange, contract_symbol, symbol, contract_quantity, spot_price, future_price)
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

            # 计算实际利润并通知
            if spot_order and future_order:
                actual_spot_price = spot_avg_price if isinstance(spot_order, dict) and "avgPrice" in spot_order else spot_price
                actual_future_price = future_avg_price if isinstance(future_order, dict) and "avgPrice" in future_order else future_price
                
                # 获取实际交易的数量
                if isinstance(spot_order, dict) and "orders" in spot_order:
                    actual_quantity = sum([float(order.get('filled', 0)) for order in spot_order["orders"]])
                else:
                    actual_quantity = float(spot_order.get('filled', estimated_quantity))
                
                theoretical_profit = (actual_spot_price - actual_future_price) * actual_quantity
                logger.info(f"理论利润: {theoretical_profit:.4f} USDT (基于实际交易数量: {actual_quantity})")
                
                # 发送套利完成总结通知
                profit_message = f"🎉 套利交易完成\n" + \
                                f"交易对: {symbol}\n" + \
                                f"现货买入: {actual_quantity} @ {actual_spot_price:.6f}\n" + \
                                f"合约平仓: {contract_quantity} @ {actual_future_price:.6f}\n" + \
                                f"理论利润: {theoretical_profit:.4f} USDT\n" + \
                                f"价差: {((actual_future_price - actual_spot_price) / actual_spot_price * 100):.4f}%"
                
                send_wechat_notification(profit_message)
            
            # 检查是否两边都完成
            if (isinstance(spot_order, dict) and spot_order.get("completed", False) == False) or \
               (isinstance(future_order, dict) and future_order.get("completed", False) == False):
                logger.warning("交易未完全执行，可能需要手动处理剩余部分")
                send_wechat_notification("⚠️ 套利交易部分完成\n可能需要手动处理剩余部分", is_success=False)
        
        # 交易完成后，再次检查账户余额并记录
        try:
            final_spot_balances = spot_exchange.fetch_balance()
            final_future_balances = future_exchange.fetch_balance()
            
            final_spot_base = final_spot_balances.get(base_currency, {}).get('free', 0)
            final_spot_quote = final_spot_balances.get(quote_currency, {}).get('free', 0)
            final_future_quote = final_future_balances.get(quote_currency, {}).get('free', 0)
            
            logger.info(f"交易后现货账户余额: {base_currency}={final_spot_base}, {quote_currency}={final_spot_quote}")
            logger.info(f"交易后合约账户余额: {quote_currency}={final_future_quote}")
            
            # 记录余额变化
            base_change = final_spot_base - spot_base_balance
            quote_change = final_spot_quote - spot_quote_balance
            future_quote_change = final_future_quote - future_quote_balance
            
            logger.info(f"现货账户余额变化: {base_currency}={base_change:+.6f}, {quote_currency}={quote_change:+.6f}")
            logger.info(f"合约账户余额变化: {quote_currency}={future_quote_change:+.6f}")
        except Exception as e:
            logger.warning(f"获取交易后余额失败: {e}")
            
        return spot_order, future_order
        
    except Exception as e:
        error_msg = f"执行交易失败: {e}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        # 添加交易失败通知
        send_wechat_notification(f"❌ 套利交易执行异常\n交易对: {symbol}\n错误: {str(e)}", is_success=False)
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


# 发送企业微信通知函数
def send_wechat_notification(message, is_success=True):
    """
    发送企业微信机器人通知
    
    Args:
        message: 通知消息内容
        is_success: 是否成功消息，用于设置关键字颜色
    """
    if not ENABLE_NOTIFICATION:
        return
    
    try:
        # 设置关键字的颜色
        success_color = "#91cc75"  # 成功状态的绿色
        failure_color = "#ee6666"  # 失败状态的红色
        highlight_color = "#f5a31a"  # 高亮信息的橙色(用于金额、价格等关键数据)
        
        # 根据消息内容和状态处理颜色标记
        # 替换表情符号和状态标记
        if "✅" in message:
            message = message.replace("✅", f"<font color='{success_color}'>✅ 成功</font>")
        elif "❌" in message:
            message = message.replace("❌", f"<font color='{failure_color}'>❌ 失败</font>")
        elif "⚠️" in message:
            message = message.replace("⚠️", f"<font color='{highlight_color}'>⚠️ 警告</font>")
        elif "🎉" in message:
            message = message.replace("🎉", f"<font color='{success_color}'>🎉 完成</font>")
        elif "🚀" in message:
            message = message.replace("🚀", f"<font color='{highlight_color}'>🚀</font>")
        elif "📈" in message:
            message = message.replace("📈", f"<font color='{highlight_color}'>📈 机会</font>")
        elif "🏁" in message:
            message = message.replace("🏁", f"<font color='{success_color}'>🏁</font>")
        
        # 高亮处理关键数据和错误信息
        if "理论利润:" in message:
            # 提取利润值并高亮显示
            parts = message.split("理论利润:")
            if len(parts) > 1:
                profit_part = parts[1].split("\n")[0].strip()
                message = message.replace(f"理论利润:{profit_part}", 
                                         f"理论利润:<font color='{highlight_color}'>{profit_part}</font>")
        
        if "价差:" in message:
            # 提取价差值并高亮显示
            parts = message.split("价差:")
            if len(parts) > 1:
                diff_part = parts[1].split("\n")[0].strip()
                message = message.replace(f"价差:{diff_part}", 
                                         f"价差:<font color='{highlight_color}'>{diff_part}</font>")
        
        if "错误:" in message:
            # 提取错误信息并高亮显示
            parts = message.split("错误:")
            if len(parts) > 1:
                error_part = parts[1].split("\n")[0].strip()
                message = message.replace(f"错误:{error_part}", 
                                         f"错误:<font color='{failure_color}'>{error_part}</font>")

        # 构建请求数据
        data = {
            "msgtype": "markdown",
            "markdown": {
                "content": message
            }
        }
        
        # 发送请求
        response = requests.post(
            WEBHOOK_URL,
            data=json.dumps(data),
            headers={"Content-Type": "application/json"}
        )
        
        # 检查响应
        if response.status_code == 200:
            result = response.json()
            if result.get("errcode") == 0:
                logger.info(f"企业微信通知发送成功")
            else:
                logger.error(f"企业微信通知发送失败: {result.get('errmsg')}")
        else:
            logger.error(f"企业微信通知发送失败，状态码: {response.status_code}")
    
    except Exception as e:
        logger.error(f"发送企业微信通知失败: {e}")
        logger.error(traceback.format_exc())


# 更新启动时检查账户余额函数
def check_accounts_on_startup(exchanges, args):
    """
    在程序启动时检查现货和合约账户余额，并打印详细日志
    
    Args:
        exchanges: 交易所API对象字典
        args: 命令行参数对象
    """
    try:
        logger.info("========== 启动时账户余额检查 ==========")
        
        # 解析交易对，获取基础货币和报价货币
        base_currency, quote_currency = args.symbol.split('/')
        spot_exchange = exchanges[args.spot_exchange]
        future_exchange = exchanges[args.future_exchange]
        
        # 检查现货账户余额
        logger.info(f"正在获取{args.spot_exchange}现货账户余额...")
        spot_balances = spot_exchange.fetch_balance()
        spot_base_balance = spot_balances.get(base_currency, {}).get('free', 0)
        spot_quote_balance = spot_balances.get(quote_currency, {}).get('free', 0)
        logger.info(f"{args.spot_exchange}现货账户余额: {base_currency}={spot_base_balance}, {quote_currency}={spot_quote_balance}")
        
        # 检查合约账户余额
        if args.future_exchange == "bitget":
            # 只使用方法1获取Bitget合约余额
            logger.info(f"正在获取{args.future_exchange}合约账户余额...")
            contract_balance = future_exchange.fetch_balance({'type': 'swap'})
            
            if quote_currency in contract_balance:
                contract_quote_free = contract_balance[quote_currency].get('free', 0)
                contract_quote_used = contract_balance[quote_currency].get('used', 0)
                contract_quote_total = contract_balance[quote_currency].get('total', 0)
                logger.info(f"{args.future_exchange}合约账户余额: {quote_currency} free={contract_quote_free}, used={contract_quote_used}, total={contract_quote_total}")
            else:
                logger.warning(f"无法获取{quote_currency}余额")
        
        elif args.future_exchange == "gateio":
            # 使用专用函数获取GateIO合约余额
            future_margin_balance = get_gateio_futures_balance(future_exchange, quote_currency)
            logger.info(f"{args.future_exchange}合约账户余额: {quote_currency}={future_margin_balance}")
        
        else:
            # 其他交易所使用标准方法
            logger.info(f"正在获取{args.future_exchange}合约账户余额...")
            future_balances = future_exchange.fetch_balance()
            future_quote_balance = future_balances.get(quote_currency, {}).get('free', 0)
            logger.info(f"{args.future_exchange}合约账户余额: {quote_currency}={future_quote_balance}")
        
        # 检查合约账户持仓情况
        try:
            contract_symbol = get_contract_symbol(args.future_exchange, args.symbol)
            logger.info(f"正在获取{args.future_exchange}合约持仓情况，合约交易对: {contract_symbol}...")
            positions = future_exchange.fetch_positions([contract_symbol])
            
            if positions and len(positions) > 0:
                logger.info(f"合约持仓信息: {positions}")
                for position in positions:
                    if position['symbol'] == contract_symbol:
                        logger.info(f"合约{contract_symbol}持仓: 方向={position['side']}, "
                                   f"数量={position['contracts']}, 名义价值={position['notional']}, "
                                   f"杠杆={position['leverage']}")
            else:
                logger.info(f"合约{contract_symbol}无持仓")
        except Exception as e:
            logger.error(f"获取合约持仓信息失败: {e}")
        
        logger.info("========== 账户余额检查完成 ==========")
    except Exception as e:
        logger.error(f"启动时账户余额检查失败: {e}")
        logger.error(traceback.format_exc())


# 主函数
def main():
    # 解析命令行参数
    args, parser = parse_arguments()
    
    # 如果参数解析失败，显示帮助信息并退出
    if args is None:
        print("\n参数解析失败。请检查输入的参数是否正确。")
        parser.print_help()
        return
    
    # 更新全局变量（可选，也可以直接使用args对象）
    global SYMBOL, QUANTITY, THRESHOLD, RETRY_DELAY, LEVERAGE, MARGIN_MODE
    global SPLIT_ORDERS, SPLIT_BY_VALUE, SPLIT_SIZE, SPLIT_DELAY
    global SPOT_EXCHANGE, FUTURE_EXCHANGE, TRADE_TYPE, BY_AMOUNT
    global ENABLE_NOTIFICATION, WEBHOOK_URL  # 添加通知相关全局变量
    
    SYMBOL = args.symbol
    QUANTITY = args.quantity
    BY_AMOUNT = args.by_amount
    THRESHOLD = args.threshold
    RETRY_DELAY = args.retry_delay
    LEVERAGE = args.leverage
    MARGIN_MODE = args.margin_mode
    SPLIT_ORDERS = args.split_orders
    SPLIT_BY_VALUE = args.split_by_value
    SPLIT_SIZE = args.split_size
    SPLIT_DELAY = args.split_delay
    SPOT_EXCHANGE = args.spot_exchange
    FUTURE_EXCHANGE = args.future_exchange
    TRADE_TYPE = args.trade_type
    
    ENABLE_NOTIFICATION = args.enable_notification
    WEBHOOK_URL = args.webhook_url
    
    # 配置日志
    setup_logger(args)
    
    start_message = f"🚀 开始套利交易程序\n类型: {TRADE_TYPE}\n交易所: {SPOT_EXCHANGE}/{FUTURE_EXCHANGE}\n"
    if BY_AMOUNT:
        start_message += f"交易对: {SYMBOL}\n金额: {QUANTITY} USDT\n价差阈值: {THRESHOLD}%"
    else:
        start_message += f"交易对: {SYMBOL}\n数量: {QUANTITY}\n价差阈值: {THRESHOLD}%"
    
    # 发送开始执行的通知
    send_wechat_notification(start_message)
    
    logger.info(f"开始套利交易程序 - {TRADE_TYPE} - {SPOT_EXCHANGE}/{FUTURE_EXCHANGE}")
    if BY_AMOUNT:
        logger.info(f"交易对: {SYMBOL}, 金额: {QUANTITY} USDT, 价差阈值: {THRESHOLD}%")
    else:
        logger.info(f"交易对: {SYMBOL}, 数量: {QUANTITY}, 价差阈值: {THRESHOLD}%")
    
    try:
        # 初始化交易所
        exchanges = init_exchanges(args)
        
        if SPOT_EXCHANGE not in exchanges or FUTURE_EXCHANGE not in exchanges:
            logger.error(f"交易所初始化失败: {SPOT_EXCHANGE}或{FUTURE_EXCHANGE}不存在")
            return
            
        # 启动时检查账户余额 (新增)
        check_accounts_on_startup(exchanges, args)

        # 设置合约交易参数
        if not setup_contract_settings(exchanges[FUTURE_EXCHANGE], FUTURE_EXCHANGE, SYMBOL, args):
            logger.error("合约交易参数设置失败，程序退出")
            return

        while True:
            # 获取考虑深度的价格
            spot_price, future_price, enough_depth = get_prices_with_depth(exchanges, args)

            if spot_price is None or future_price is None:
                logger.warning("价格获取失败，等待重试...")
                time.sleep(RETRY_DELAY)
                continue

            if not enough_depth:
                logger.warning("订单簿深度不足，等待市场深度恢复...")
                time.sleep(RETRY_DELAY)
                continue

            # 计算价格差异
            price_difference = calculate_price_difference(spot_price, future_price, args)

            if price_difference is None:
                logger.warning("价格差异计算失败，等待重试...")
                time.sleep(RETRY_DELAY)
                continue
                
            # 检查是否满足套利条件
            if price_difference <= THRESHOLD:
                logger.info(f"价格差异 {price_difference:.6f}% 未达到阈值 {THRESHOLD}%，等待...")
                time.sleep(RETRY_DELAY)
                continue

            # 满足套利条件，执行交易
            condition_message = f"📈 检测到套利机会\n" + \
                                f"交易对: {SYMBOL}\n" + \
                                f"价格差异: {price_difference:.6f}%\n" + \
                                f"阈值: {THRESHOLD}%\n" + \
                                f"准备执行交易"
            send_wechat_notification(condition_message)
            
            logger.info(f"价格差异 {price_difference:.6f}% 满足阈值 {THRESHOLD}%，执行交易")

            # 执行交易
            spot_order, future_order = execute_trades(exchanges, SYMBOL, spot_price, future_price)

            if spot_order and future_order:
                logger.info("套利交易成功完成")
                send_wechat_notification("🎯 套利交易全部完成")
                break
            else:
                logger.error("交易执行失败，等待重试...")
                send_wechat_notification("❌ 交易执行失败，等待重试...", is_success=False)
                time.sleep(RETRY_DELAY)

    except Exception as e:
        error_msg = f"程序执行错误: {e}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        # 发送错误通知
        send_wechat_notification(f"❌ 程序执行错误\n{error_msg}", is_success=False)
        # 出错时也打印帮助信息
        print("\n程序执行过程中发生错误。请检查参数和配置是否正确。")
        parser.print_help()

    finally:
        logger.info("套利交易程序结束")
        send_wechat_notification("🏁 套利交易程序结束")


if __name__ == "__main__":
    main()


def get_gateio_futures_balance(exchange, quote_currency='USDT'):
    """
    使用官方API方式获取GateIO USDT合约账户余额
    
    Args:
        exchange: GateIO交易所实例
        quote_currency: 结算货币，默认为USDT
    
    Returns:
        float: 可用余额
    """
    try:
        # 获取API密钥
        api_key = exchange.apiKey
        api_secret = exchange.secret
        
        if not api_key or not api_secret:
            logger.error("GateIO API密钥未配置")
            return 0
        
        # 生成签名
        method = 'GET'
        url_path = '/api/v4/futures/usdt/accounts'
        query_string = ''
        body = ''
        
        # 生成时间戳和签名
        t = time.time()
        timestamp = str(int(t))
        string_to_sign = '\n'.join([method, url_path, query_string, hashlib.sha512(body.encode() if body else b'').hexdigest(), timestamp])
        signature = hmac.new(api_secret.encode(), string_to_sign.encode(), hashlib.sha512).hexdigest()
        
        # 设置请求头
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'KEY': api_key,
            'Timestamp': timestamp,
            'SIGN': signature
        }
        
        # 发送请求
        url = 'https://api.gateio.ws' + url_path
        logger.debug(f"发送GateIO合约账户余额请求: {url}")
        
        response = requests.get(url, headers=headers, proxies=proxies)
        
        if response.status_code == 200:
            # 解析响应数据
            account = response.json()  # 这里直接就是账户对象，不是列表
            
            # 检查返回的数据是否包含需要的字段
            if account.get('currency') == quote_currency:
                # 提取cross_available字段作为可用余额
                cross_available = float(account.get('cross_available', 0))
                total = float(account.get('total', 0))
                logger.info(f"GateIO {quote_currency}合约账户余额: cross_available={cross_available}, total={total}")
                return cross_available
            else:
                logger.warning(f"GateIO合约账户响应不包含{quote_currency}货币")
                return 0
        else:
            logger.error(f"获取GateIO合约账户余额请求失败，状态码: {response.status_code}")
            logger.error(f"响应内容: {response.text}")
            return 0
            
    except Exception as e:
        logger.error(f"获取GateIO合约账户余额失败: {e}")
        logger.error(traceback.format_exc())
        return 0