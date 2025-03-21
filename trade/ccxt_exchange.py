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
    okx_api_key, okx_api_secret, okx_api_passphrase,
    bybit_api_key, bybit_api_secret
)
from tools.logger import logger


# 加载环境变量
# load_dotenv()


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
            if getattr(args, 'spot_exchange', None) == "all_exchanges" or getattr(args, 'future_exchange',
                                                                                  None) == "all_exchanges":
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
