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

# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将主项目目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))

from config import proxies  # 从配置中导入代理设置

# 导入hedging_trade.py中的相关函数和配置
from high_yield.hedging_trade import (
    init_exchanges, 
    check_accounts_on_startup,
    logger,
    parse_arguments
)

def setup_test_logger():
    """配置测试用的日志"""
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # 创建文件处理器
    file_handler = logging.FileHandler('exchange_balance_test.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # 添加处理器到logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    logger.setLevel(logging.INFO)
    logger.info("开始交易所余额测试")

def gen_sign(method, url, query_string, body, secret):
    """
    GateIO API v4签名生成函数
    
    Args:
        method: HTTP方法，例如 'GET'
        url: API请求路径，例如 '/api/v4/futures/usdt/accounts'
        query_string: 查询参数字符串，对于没有查询参数的请求为空字符串
        body: 请求主体，对于GET请求为空字符串
        secret: API密钥
    
    Returns:
        生成的签名
    """
    t = time.time()
    m = hashlib.sha512()
    hashed_payload = m.digest() if body else b''
    timestamp = str(int(t))
    string_to_sign = '\n'.join([method, url, query_string, hashlib.sha512(body.encode() if body else b'').hexdigest(), timestamp])
    signature = hmac.new(secret.encode(), string_to_sign.encode(), hashlib.sha512).hexdigest()
    return signature, timestamp

def test_single_exchange(exchange_id, symbol, exchanges):
    """测试单个交易所的余额获取功能"""
    logger.info(f"===== 测试 {exchange_id} 交易所 =====")
    exchange = exchanges.get(exchange_id)
    
    if not exchange:
        logger.error(f"{exchange_id} 交易所初始化失败")
        return
    
    try:
        # 解析交易对获取基础货币和报价货币
        base_currency, quote_currency = symbol.split('/')
        
        # 测试现货账户余额
        logger.info(f"获取 {exchange_id} 现货账户余额...")
        spot_balance = exchange.fetch_balance()
        
        # 打印主要货币的余额
        spot_base = spot_balance.get(base_currency, {}).get('free', 0)
        spot_quote = spot_balance.get(quote_currency, {}).get('free', 0)
        logger.info(f"{exchange_id} 现货账户余额: {base_currency}={spot_base}, {quote_currency}={spot_quote}")
        
        # 打印总余额信息
        total_balance = sum([balance.get('total', 0) for currency, balance in spot_balance.items() 
                             if isinstance(balance, dict) and 'total' in balance and balance['total'] > 0])
        logger.info(f"{exchange_id} 现货账户总估值(未精确计算): {total_balance:.4f}")
        
        # 测试合约账户余额 - 处理特殊情况
        logger.info(f"获取 {exchange_id} 合约账户余额...")
        
        if exchange_id == "bitget":
            # Bitget需要特殊处理: 使用type=swap参数
            contract_balance = exchange.fetch_balance({'type': 'swap'})
            
            if quote_currency in contract_balance:
                contract_quote_free = contract_balance[quote_currency].get('free', 0)
                contract_quote_used = contract_balance[quote_currency].get('used', 0)
                contract_quote_total = contract_balance[quote_currency].get('total', 0)
                logger.info(f"{exchange_id} 合约账户余额: {quote_currency} free={contract_quote_free}, used={contract_quote_used}, total={contract_quote_total}")
                
                # 打印所有非零余额
                logger.info(f"{exchange_id} 合约账户所有非零余额:")
                for currency, balance in contract_balance.items():
                    if isinstance(balance, dict) and 'total' in balance and balance['total'] > 0:
                        logger.info(f"  {currency}: free={balance.get('free', 0)}, used={balance.get('used', 0)}, total={balance.get('total', 0)}")
            else:
                logger.warning(f"{exchange_id} 合约账户中未找到 {quote_currency} 余额")
                
        elif exchange_id == "gateio":
            # GateIO需要特殊处理: 使用专门的测试函数
            base_currency, quote_currency = symbol.split('/')
            futures_balance = test_gateio_futures_balance(exchange, quote_currency)
            if futures_balance > 0:
                logger.info(f"GateIO {quote_currency}合约账户可用余额: {futures_balance}")
            
        elif exchange_id == "binance":
            # Binance可能需要特殊选项 
            try:
                # 尝试不同的API选项
                contract_balance = exchange.fetch_balance({'type': 'future'})
                
                if quote_currency in contract_balance:
                    logger.info(f"{exchange_id} 合约账户余额: {quote_currency}={contract_balance[quote_currency].get('free', 0)}")
                    
                    # 打印所有非零余额
                    logger.info(f"{exchange_id} 合约账户所有非零余额:")
                    for currency, balance in contract_balance.items():
                        if isinstance(balance, dict) and 'total' in balance and balance['total'] > 0:
                            logger.info(f"  {currency}: free={balance.get('free', 0)}, used={balance.get('used', 0)}, total={balance.get('total', 0)}")
                else:
                    logger.warning(f"{exchange_id} 合约账户中未找到 {quote_currency} 余额")
            except Exception as e:
                logger.error(f"获取Binance合约余额失败: {e}")
                
        else:
            # 其他交易所使用标准方法
            try:
                contract_balance = exchange.fetch_balance()
                
                if quote_currency in contract_balance:
                    logger.info(f"{exchange_id} 合约账户余额: {quote_currency}={contract_balance[quote_currency].get('free', 0)}")
                    
                    # 打印所有非零余额
                    logger.info(f"{exchange_id} 合约账户所有非零余额:")
                    for currency, balance in contract_balance.items():
                        if isinstance(balance, dict) and 'total' in balance and balance['total'] > 0:
                            logger.info(f"  {currency}: free={balance.get('free', 0)}, used={balance.get('used', 0)}, total={balance.get('total', 0)}")
                else:
                    logger.warning(f"{exchange_id} 合约账户中未找到 {quote_currency} 余额")
            except Exception as e:
                logger.error(f"获取{exchange_id}合约余额失败: {e}")
            
    except Exception as e:
        logger.error(f"测试 {exchange_id} 交易所失败: {e}")
        logger.error(traceback.format_exc())

def test_positions(exchange_id, symbol, exchanges):
    """测试获取持仓信息"""
    logger.info(f"===== 测试 {exchange_id} 持仓信息 =====")
    exchange = exchanges.get(exchange_id)
    
    if not exchange:
        logger.error(f"{exchange_id} 交易所初始化失败")
        return
    
    try:
        # 获取合约交易对格式
        contract_symbol = None
        if exchange_id == "bitget":
            contract_symbol = f"{symbol}:USDT"
        elif exchange_id == "binance":
            contract_symbol = symbol.replace('/', '')
        elif exchange_id == "okx":
            base, quote = symbol.split('/')
            contract_symbol = f"{base}-{quote}-SWAP"
        elif exchange_id == "bybit" or exchange_id == "gateio":
            contract_symbol = symbol
        
        logger.info(f"获取 {exchange_id} 合约持仓, 合约交易对: {contract_symbol}...")
        
        # 尝试获取持仓信息
        try:
            positions = exchange.fetch_positions([contract_symbol]) if contract_symbol else exchange.fetch_positions()
            
            if positions and len(positions) > 0:
                logger.info(f"找到 {len(positions)} 个持仓:")
                for pos in positions:
                    if float(pos.get('contracts', 0)) != 0:  # 只显示非零持仓
                        logger.info(f"  交易对: {pos.get('symbol')}, 方向: {pos.get('side')}, "
                                   f"数量: {pos.get('contracts')}, 名义价值: {pos.get('notional')}, "
                                   f"杠杆: {pos.get('leverage')}")
            else:
                logger.info(f"{exchange_id} 无持仓信息")
        except Exception as e:
            logger.error(f"获取 {exchange_id} 持仓信息失败: {e}")
            
    except Exception as e:
        logger.error(f"测试 {exchange_id} 持仓信息失败: {e}")
        logger.error(traceback.format_exc())

def test_gateio_futures_balance(exchange, quote_currency='USDT'):
    """
    专门用于测试 GateIO USDT 合约账户余额
    
    Args:
        exchange: GateIO交易所实例
        quote_currency: 结算货币，默认为USDT
    """
    logger.info(f"===== 测试 GateIO {quote_currency} 合约账户余额 =====")
    
    try:
        # 获取API密钥
        api_key = exchange.apiKey
        api_secret = exchange.secret
        
        if not api_key or not api_secret:
            logger.error("GateIO API密钥未配置")
            return
        
        # 生成签名
        method = 'GET'
        url_path = '/api/v4/futures/usdt/accounts'
        query_string = ''
        body = ''
        
        signature, timestamp = gen_sign(method, url_path, query_string, body, api_secret)
        
        # 设置请求头
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'KEY': api_key,
            'Timestamp': timestamp,
            'SIGN': signature
        }
        
        # 获取代理设置
        logger.info(f"使用代理: {proxies}")
        
        # 发送请求
        url = 'https://api.gateio.ws' + url_path
        logger.info(f"发送请求: {url}")
        logger.info(f"请求头: {headers}")
        
        response = requests.get(url, headers=headers, proxies=proxies)
        
        if response.status_code == 200:
            # 解析响应数据
            account = response.json()  # 这里直接就是账户对象，不是列表
            logger.info(f"==== USDT合约账户响应 ====")
            
            # 检查返回的数据是否包含需要的字段
            if account.get('currency') == quote_currency:
                # 记录所有重要的余额字段
                total = float(account.get('total', 0))
                available = float(account.get('available', 0))
                cross_available = float(account.get('cross_available', 0))
                
                logger.info(f"GateIO {quote_currency}合约账户余额:")
                logger.info(f"  cross_available = {cross_available}")
                logger.info(f"  available = {available}")
                logger.info(f"  total = {total}")
                
                # 打印完整的账户信息
                logger.info(f"GateIO合约账户完整信息:\n{json.dumps(account, indent=2)}")
                logger.info("==========================")
                return cross_available
            else:
                logger.warning(f"GateIO合约账户响应不包含{quote_currency}货币")
                return 0
        else:
            logger.error(f"请求失败，状态码: {response.status_code}")
            logger.error(f"响应内容: {response.text}")
            return 0
    
    except Exception as e:
        logger.error(f"测试GateIO合约账户余额失败: {e}")
        logger.error(traceback.format_exc())
        return 0

def main():
    # 设置命令行参数
    parser = argparse.ArgumentParser(description='交易所余额测试工具')
    parser.add_argument('--symbol', type=str, default="BTC/USDT",
                        help='交易对，例如 BTC/USDT')
    parser.add_argument('--exchange', type=str, 
                        choices=["all", "gateio", "bitget", "binance", "okx", "bybit"],
                        default="all", help='要测试的交易所')
    
    args = parser.parse_args()
    
    # 配置日志
    setup_test_logger()
    
    # 创建一个模拟的args对象用于初始化交易所
    mock_args = argparse.Namespace(
        symbol=args.symbol,
        spot_exchange="gateio",  # 这些值不会影响我们的测试结果
        future_exchange="bitget",
        by_amount=True,
        quantity=100,
        trade_type="spot_buy_future_short",
        threshold=-0.1,
        leverage=10,
        margin_mode="cross",
        split_orders=True,
        split_by_value=True,
        split_size=100,
        split_delay=0.5,
        retry_delay=1,
        log_file="",
        enable_notification=False,
        webhook_url=""
    )
    
    try:
        logger.info(f"开始测试交易所余额获取功能，交易对: {args.symbol}")
        
        # 初始化所有交易所连接
        exchanges = init_exchanges(mock_args)
        
        # 定义要测试的交易所
        exchange_ids = ["gateio", "bitget", "binance", "okx", "bybit"]
        
        # 如果指定了单个交易所，则只测试该交易所
        if args.exchange != "all":
            exchange_ids = [args.exchange]
        
        # 测试每个交易所
        for exchange_id in exchange_ids:
            logger.info(f"\n{'=' * 50}")
            logger.info(f"开始测试 {exchange_id} 交易所")
            
            # 测试余额获取
            test_single_exchange(exchange_id, args.symbol, exchanges)
            
            # 测试持仓信息
            test_positions(exchange_id, args.symbol, exchanges)
            
            logger.info(f"完成测试 {exchange_id} 交易所")
            logger.info(f"{'=' * 50}\n")
            
            # 添加延迟，避免API请求过于频繁
            time.sleep(1)
        
        logger.info("所有交易所测试完成!")
        
    except Exception as e:
        logger.error(f"测试过程中发生错误: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main() 