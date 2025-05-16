"""
跨交易所套利监控脚本

该脚本用于监控不同交易所之间的价格差异，寻找套利机会。主要功能包括：
1. 从GateIO、Binance、Bybit和Bitget获取所有现货和合约交易对
2. 获取每个交易对在不同交易所的现货和合约价格
3. 计算价格差异，筛选出符合条件的套利机会
4. 将套利机会发送到企业微信群机器人并保存到文件

使用方法：
    python cross_exchange_arbitrage.py [-d|--debug]

参数说明：
    -d, --debug: 开启调试日志，显示详细的API请求和响应信息

配置说明：
    在config.py中设置：
    - price_diff_threshold: 价格差异阈值（默认0.2%）
    - max_price_diff_threshold: 最大价格差异阈值（默认10%）
    - volume_24h_threshold: 24小时最小交易量阈值（默认20万USDT）
    - arbitrage_webhook_url: 企业微信群机器人webhook地址
    - min_token_price: 最小代币价格（默认0.001 USDT）

作者：Raymon
创建时间：2024-05-06
"""

import time
from datetime import datetime
import requests
import json
from typing import List, Dict, Any, Set, Tuple
from exchange import ExchangeAPI
from config import (
    arbitrage_webhook_url, 
    price_diff_threshold, 
    max_price_diff_threshold,
    volume_24h_threshold, 
    proxies,
    min_token_price
)
from tools.logger import logger
import os
import argparse

# 全局调试标志
DEBUG = False

def debug_log(message: str):
    """
    打印调试日志
    :param message: 日志消息
    """
    if DEBUG:
        logger.debug(message)

class RateLimiter:
    """令牌桶限速器"""
    def __init__(self, rate: int, per: float = 1.0):
        """
        初始化限速器
        :param rate: 每秒允许的请求数
        :param per: 时间窗口（秒）
        """
        self.rate = rate
        self.per = per
        self.tokens = rate
        self.last_update = time.time()
        self.lock = False

    def acquire(self) -> bool:
        """
        获取一个令牌
        :return: 是否获取成功
        """
        now = time.time()
        time_passed = now - self.last_update
        self.tokens = min(self.rate, self.tokens + time_passed * self.rate)
        self.last_update = now

        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False

    def wait(self):
        """等待直到可以获取令牌"""
        while not self.acquire():
            time.sleep(0.05)  # 50ms的等待间隔

# 创建Bitget的限速器实例
bitget_limiter = RateLimiter(rate=20, per=1.0)  # 每秒20次请求

def make_bitget_request(url: str, params: Dict = None, proxies: Dict = None) -> requests.Response:
    """
    发送Bitget API请求，带限速控制
    :param url: 请求URL
    :param params: 请求参数
    :param proxies: 代理设置
    :return: 请求响应
    """
    bitget_limiter.wait()  # 等待获取令牌
    debug_log(f"Bitget请求: URL={url}, 参数={params}")
    response = requests.get(url, params=params, proxies=proxies)
    debug_log(f"Bitget响应: 状态码={response.status_code}, 内容={response.text}")
    return response

def get_all_trading_pairs() -> Set[str]:
    """
    从GateIO、Binance、Bybit和Bitget获取所有现货和合约交易对
    :return: 交易对集合
    """
    trading_pairs = set()
    api = ExchangeAPI()
    
    # 获取Bitget交易对
    try:
        logger.info("开始获取Bitget交易对")
        # 获取现货交易对
        spot_url = "https://api.bitget.com/api/spot/v1/public/products"
        spot_response = make_bitget_request(spot_url, proxies=api.session.proxies)
        if spot_response.status_code == 200:
            spot_data = spot_response.json()
            if spot_data["code"] == "00000" and "data" in spot_data:
                for item in spot_data["data"]:
                    if item["symbol"].endswith("USDT"):
                        trading_pairs.add(item["symbol"])
        
        # 获取合约交易对
        futures_url = "https://api.bitget.com/api/v2/mix/market/contracts"
        futures_params = {"productType": "usdt-futures"}
        futures_response = make_bitget_request(futures_url, params=futures_params, proxies=api.session.proxies)
        if futures_response.status_code == 200:
            futures_data = futures_response.json()
            if futures_data["code"] == "00000" and "data" in futures_data:
                for item in futures_data["data"]:
                    if item["symbol"].endswith("USDT"):
                        trading_pairs.add(item["symbol"])
        
        logger.info(f"从Bitget获取到{len(trading_pairs)}个交易对")
    except Exception as e:
        logger.error(f"获取Bitget交易对失败: {str(e)}")
    
    # 获取GateIO交易对
    try:
        logger.info("开始获取GateIO交易对")
        # 获取现货交易对
        spot_url = "https://api.gateio.ws/api/v4/spot/currency_pairs"
        spot_response = requests.get(spot_url, proxies=api.session.proxies)
        if spot_response.status_code == 200:
            spot_data = spot_response.json()
            for item in spot_data:
                if item["quote"] == "USDT":
                    trading_pairs.add(item["base"] + "USDT")
        
        # 获取合约交易对
        futures_url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
        futures_response = requests.get(futures_url, proxies=api.session.proxies)
        if futures_response.status_code == 200:
            futures_data = futures_response.json()
            for item in futures_data:
                if not item["in_delisting"]:
                    trading_pairs.add(item["name"].replace("_USDT", "USDT"))
        
        logger.info(f"从GateIO获取到{len(trading_pairs)}个交易对")
    except Exception as e:
        logger.error(f"获取GateIO交易对失败: {str(e)}")

    # 获取Binance交易对
    try:
        logger.info("开始获取Binance交易对")
        # 获取现货交易对
        spot_url = "https://api.binance.com/api/v3/exchangeInfo"
        spot_response = requests.get(spot_url, proxies=api.session.proxies)
        if spot_response.status_code == 200:
            spot_data = spot_response.json()
            for symbol in spot_data["symbols"]:
                if symbol["quoteAsset"] == "USDT" and symbol["status"] == "TRADING":
                    trading_pairs.add(symbol["baseAsset"] + "USDT")
        
        # 获取合约交易对
        futures_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        futures_response = requests.get(futures_url, proxies=api.session.proxies)
        if futures_response.status_code == 200:
            futures_data = futures_response.json()
            for symbol in futures_data["symbols"]:
                if symbol["quoteAsset"] == "USDT" and symbol["status"] == "TRADING":
                    trading_pairs.add(symbol["baseAsset"] + "USDT")
        
        logger.info(f"从Binance获取到{len(trading_pairs)}个交易对")
    except Exception as e:
        logger.error(f"获取Binance交易对失败: {str(e)}")

    # 获取Bybit交易对
    try:
        logger.info("开始获取Bybit交易对")
        # 获取现货交易对
        spot_url = "https://api.bybit.com/v5/market/instruments-info"
        spot_params = {"category": "spot"}
        spot_response = requests.get(spot_url, params=spot_params, proxies=api.session.proxies)
        if spot_response.status_code == 200:
            spot_data = spot_response.json()
            if spot_data["retCode"] == 0 and "result" in spot_data:
                for item in spot_data["result"]["list"]:
                    if item["quoteCoin"] == "USDT" and item["status"] == "Trading":
                        trading_pairs.add(item["baseCoin"] + "USDT")
        
        # 获取合约交易对
        futures_url = "https://api.bybit.com/v5/market/instruments-info"
        futures_params = {"category": "linear"}
        futures_response = requests.get(futures_url, params=futures_params, proxies=api.session.proxies)
        if futures_response.status_code == 200:
            futures_data = futures_response.json()
            if futures_data["retCode"] == 0 and "result" in futures_data:
                for item in futures_data["result"]["list"]:
                    if item["quoteCoin"] == "USDT" and item["status"] == "Trading":
                        trading_pairs.add(item["baseCoin"] + "USDT")
        
        logger.info(f"从Bybit获取到{len(trading_pairs)}个交易对")
    except Exception as e:
        logger.error(f"获取Bybit交易对失败: {str(e)}")
    
    return trading_pairs

def get_trading_pair_info(api: ExchangeAPI, token: str) -> Dict[str, Any]:
    """
    获取交易对在各个交易所的价格和交易量信息
    :param api: ExchangeAPI实例
    :param token: 交易对
    :return: 交易对信息字典
    """
    result = {
        'spot': {},
        'futures': {},
        'funding_rates': {}
    }
    
    # 获取Binance信息
    try:
        # 获取现货价格和交易量
        spot_url = "https://api.binance.com/api/v3/ticker/24hr"
        spot_params = {"symbol": token}
        debug_log(f"Binance现货请求: URL={spot_url}, 参数={spot_params}")
        spot_response = requests.get(spot_url, params=spot_params, proxies=api.session.proxies)
        debug_log(f"Binance现货响应: 状态码={spot_response.status_code}, 内容={spot_response.text}")
        if spot_response.status_code == 200:
            spot_data = spot_response.json()
            price = float(spot_data['lastPrice'])
            if price >= min_token_price:  # 检查价格是否满足最小要求
                result['spot']['Binance'] = {
                    'price': price,
                    'volume': float(spot_data['volume']) * price
                }
        
        # 获取合约价格、交易量和资金费率
        futures_url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        futures_params = {"symbol": token}
        debug_log(f"Binance合约请求: URL={futures_url}, 参数={futures_params}")
        futures_response = requests.get(futures_url, params=futures_params, proxies=api.session.proxies)
        debug_log(f"Binance合约响应: 状态码={futures_response.status_code}, 内容={futures_response.text}")
        if futures_response.status_code == 200:
            futures_data = futures_response.json()
            price = float(futures_data['lastPrice'])
            if price >= min_token_price:  # 检查价格是否满足最小要求
                result['futures']['Binance'] = {
                    'price': price,
                    'volume': float(futures_data['volume']) * price
                }
                # 检查资金费率字段是否存在
                if 'lastFundingRate' in futures_data:
                    result['funding_rates']['Binance'] = float(futures_data['lastFundingRate']) * 100
                else:
                    debug_log(f"Binance合约数据中缺少lastFundingRate字段: {futures_data}")
    except Exception as e:
        logger.error(f"获取Binance {token}信息失败: {str(e)}")
        debug_log(f"Binance异常详情: {str(e)}")

    # 获取Bybit信息
    try:
        # 获取现货价格和交易量
        spot_url = "https://api.bybit.com/v5/market/tickers"
        spot_params = {"category": "spot"}
        debug_log(f"Bybit现货请求: URL={spot_url}, 参数={spot_params}")
        spot_response = requests.get(spot_url, params=spot_params, proxies=api.session.proxies)
        debug_log(f"Bybit现货响应: 状态码={spot_response.status_code}, 内容={spot_response.text}")
        if spot_response.status_code == 200:
            spot_data = spot_response.json()
            if spot_data["retCode"] == 0 and "result" in spot_data:
                for item in spot_data["result"]["list"]:
                    if item["symbol"] == token:
                        price = float(item["lastPrice"])
                        if price >= min_token_price:  # 检查价格是否满足最小要求
                            result['spot']['Bybit'] = {
                                'price': price,
                                'volume': float(item["volume24h"]) * price
                            }
                        break
        
        # 获取合约价格、交易量和资金费率
        futures_url = "https://api.bybit.com/v5/market/tickers"
        futures_params = {"category": "linear"}
        debug_log(f"Bybit合约请求: URL={futures_url}, 参数={futures_params}")
        futures_response = requests.get(futures_url, params=futures_params, proxies=api.session.proxies)
        debug_log(f"Bybit合约响应: 状态码={futures_response.status_code}, 内容={futures_response.text}")
        if futures_response.status_code == 200:
            futures_data = futures_response.json()
            if futures_data["retCode"] == 0 and "result" in futures_data:
                for item in futures_data["result"]["list"]:
                    if item["symbol"] == token:
                        price = float(item["lastPrice"])
                        if price >= min_token_price:  # 检查价格是否满足最小要求
                            result['futures']['Bybit'] = {
                                'price': price,
                                'volume': float(item["volume24h"]) * price
                            }
                            result['funding_rates']['Bybit'] = float(item["fundingRate"]) * 100
                        break
    except Exception as e:
        logger.error(f"获取Bybit {token}信息失败: {str(e)}")
        debug_log(f"Bybit异常详情: {str(e)}")

    # 获取GateIO信息
    try:
        # 获取现货价格和交易量
        spot_url = "https://api.gateio.ws/api/v4/spot/tickers"
        debug_log(f"GateIO现货请求: URL={spot_url}")
        spot_response = requests.get(spot_url, proxies=api.session.proxies)
        debug_log(f"GateIO现货响应: 状态码={spot_response.status_code}, 内容={spot_response.text}")
        if spot_response.status_code == 200:
            spot_data = spot_response.json()
            gate_io_token = token.replace('USDT', '_USDT')
            for ticker in spot_data:
                if ticker['currency_pair'] == gate_io_token:
                    price = float(ticker['last'])
                    if price >= min_token_price:  # 检查价格是否满足最小要求
                        result['spot']['GateIO'] = {
                            'price': price,
                            'volume': float(ticker['base_volume']) * price
                        }
                    break
        
        # 获取合约价格、交易量和资金费率
        futures_url = f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{gate_io_token}"
        debug_log(f"GateIO合约请求: URL={futures_url}")
        futures_response = requests.get(futures_url, proxies=api.session.proxies)
        debug_log(f"GateIO合约响应: 状态码={futures_response.status_code}, 内容={futures_response.text}")
        if futures_response.status_code == 200:
            futures_data = futures_response.json()
            if isinstance(futures_data, dict):
                price = float(futures_data.get('mark_price', 0))
                if price >= min_token_price:  # 检查价格是否满足最小要求
                    result['futures']['GateIO'] = {
                        'price': price,
                        'volume': float(futures_data.get('volume_24h', 0))
                    }
                    result['funding_rates']['GateIO'] = float(futures_data.get('funding_rate', 0)) * 100
    except Exception as e:
        logger.error(f"获取GateIO {token}信息失败: {str(e)}")
        debug_log(f"GateIO异常详情: {str(e)}")

    # 获取Bitget信息
    try:
        # 获取现货价格和交易量
        spot_url = "https://api.bitget.com/api/spot/v1/market/ticker"
        spot_params = {"symbol": f"{token.replace('USDT', '')}USDT_SPBL"}
        spot_response = make_bitget_request(spot_url, params=spot_params, proxies=api.session.proxies)
        if spot_response.status_code == 200:
            spot_data = spot_response.json()
            if spot_data["code"] == "00000" and "data" in spot_data:
                data = spot_data["data"]
                if isinstance(data, dict):
                    price = float(data["close"])
                    if price >= min_token_price:  # 检查价格是否满足最小要求
                        result['spot']['Bitget'] = {
                            'price': price,
                            'volume': float(data["baseVol"]) * price
                        }
        
        # 获取合约价格、交易量和资金费率
        futures_url = "https://api.bitget.com/api/v2/mix/market/ticker"
        futures_params = {"symbol": token, "productType": "USDT-FUTURES"}
        futures_response = make_bitget_request(futures_url, params=futures_params, proxies=api.session.proxies)
        if futures_response.status_code == 200:
            futures_data = futures_response.json()
            if futures_data["code"] == "00000" and "data" in futures_data:
                data = futures_data["data"]
                if isinstance(data, dict):
                    price = float(data["lastPr"])
                    if price >= min_token_price:  # 检查价格是否满足最小要求
                        result['futures']['Bitget'] = {
                            'price': price,
                            'volume': float(data["usdtVol"])
                        }
        
        # 获取资金费率
        funding_url = "https://api.bitget.com/api/v2/mix/market/current-fund-rate"
        funding_params = {"symbol": token, "productType": "USDT-FUTURES"}
        funding_response = make_bitget_request(funding_url, params=funding_params, proxies=api.session.proxies)
        if funding_response.status_code == 200:
            funding_data = funding_response.json()
            if funding_data["code"] == "00000" and "data" in funding_data:
                for item in funding_data["data"]:
                    if item["symbol"] == token:
                        result['funding_rates']['Bitget'] = float(item["fundingRate"]) * 100
                        break
    except Exception as e:
        logger.error(f"获取Bitget {token}信息失败: {str(e)}")
        debug_log(f"Bitget异常详情: {str(e)}")
    
    return result

def find_arbitrage_opportunities(token_info: Dict[str, Any], token: str) -> List[Dict[str, Any]]:
    """
    从交易对信息中找出套利机会
    :param token_info: 交易对信息
    :param token: 交易对
    :return: 套利机会列表
    """
    opportunities = []
    
    # 检查是否有足够的交易量
    has_valid_volume = False
    for exchange in ['Bitget', 'GateIO', 'Binance', 'Bybit']:
        if (exchange in token_info['futures'] and 
            token_info['futures'][exchange]['volume'] >= volume_24h_threshold):
            has_valid_volume = True
            break
        if (exchange in token_info['spot'] and 
            token_info['spot'][exchange]['volume'] >= volume_24h_threshold):
            has_valid_volume = True
            break
    
    if not has_valid_volume:
        return opportunities
    
    # 检查资金费率
    for exchange in ['Bitget', 'GateIO', 'Binance', 'Bybit']:
        if (exchange in token_info['funding_rates'] and 
            token_info['funding_rates'][exchange] < 0):
            return opportunities
    
    # 检查合约价格差异
    futures_prices = {}
    for exchange in ['Bitget', 'GateIO', 'Binance', 'Bybit']:
        if exchange in token_info['futures']:
            futures_prices[exchange] = token_info['futures'][exchange]['price']
    
    if len(futures_prices) >= 2:
        exchanges = list(futures_prices.keys())
        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                exchange1 = exchanges[i]
                exchange2 = exchanges[j]
                price1 = futures_prices[exchange1]
                price2 = futures_prices[exchange2]
                price_diff = abs(price1 - price2) / min(price1, price2) * 100
                
                # 检查价差是否在合理范围内
                if price_diff_threshold < price_diff <= max_price_diff_threshold:
                    opportunities.append({
                        'token': token,
                        'type': 'futures_cross_exchange',
                        'exchange1': exchange1,
                        'exchange2': exchange2,
                        'price1': price1,
                        'price2': price2,
                        'price_diff': price_diff,
                        'condition': 'futures_cross_exchange'
                    })
    
    # 检查合约和现货价格差异
    for futures_exchange in ['Bitget', 'GateIO', 'Binance', 'Bybit']:
        if futures_exchange in token_info['futures']:
            futures_price = token_info['futures'][futures_exchange]['price']
            
            # 检查同一交易所的现货价格
            if futures_exchange in token_info['spot']:
                spot_price = token_info['spot'][futures_exchange]['price']
                # 计算价差百分比，保留符号
                price_diff = (futures_price - spot_price) / spot_price * 100
                
                # 只保留合约价格高于现货价格且价差在合理范围内的机会
                if price_diff_threshold < price_diff <= max_price_diff_threshold:
                    opportunities.append({
                        'token': token,
                        'type': 'futures_spot_same_exchange',
                        'futures_exchange': futures_exchange,
                        'spot_exchange': futures_exchange,
                        'futures_price': futures_price,
                        'spot_price': spot_price,
                        'price_diff': price_diff,
                        'condition': 'futures_spot_same_exchange'
                    })
            
            # 检查其他交易所的现货价格
            for spot_exchange in ['Bitget', 'GateIO', 'Binance', 'Bybit']:
                if spot_exchange != futures_exchange and spot_exchange in token_info['spot']:
                    spot_price = token_info['spot'][spot_exchange]['price']
                    # 计算价差百分比，保留符号
                    price_diff = (futures_price - spot_price) / spot_price * 100
                    
                    # 只保留合约价格高于现货价格且价差在合理范围内的机会
                    if price_diff_threshold < price_diff <= max_price_diff_threshold:
                        opportunities.append({
                            'token': token,
                            'type': 'futures_spot_cross_exchange',
                            'futures_exchange': futures_exchange,
                            'spot_exchange': spot_exchange,
                            'futures_price': futures_price,
                            'spot_price': spot_price,
                            'price_diff': price_diff,
                            'condition': 'futures_spot_cross_exchange'
                        })
    
    return opportunities

def send_to_wechat_robot(opportunities: List[Dict[str, Any]]):
    """
    发送套利机会到企业微信群机器人，并写入日志文件
    :param opportunities: 套利机会列表
    """
    if not opportunities:
        logger.info("没有符合条件的套利机会，不发送消息")
        return
    
    # 构建消息内容
    message = "## 跨交易所套利机会\n\n"
    
    for index, opp in enumerate(opportunities, 1):
        message += f"{index}. {opp['token']}\n"
        message += f"- 套利类型: {opp['type']}\n"
        
        if opp['type'] == 'futures_cross_exchange':
            message += f"- 交易所1: {opp['exchange1']} (合约价格: {opp['price1']:.4f} USDT)\n"
            message += f"- 交易所2: {opp['exchange2']} (合约价格: {opp['price2']:.4f} USDT)\n"
        else:
            message += f"- 合约交易所: {opp['futures_exchange']} (价格: {opp['futures_price']:.4f} USDT)\n"
            message += f"- 现货交易所: {opp['spot_exchange']} (价格: {opp['spot_price']:.4f} USDT)\n"
        
        message += f"- 价差: {opp['price_diff']:+.2f}%\n"  # 添加+号显示正价差
        message += f"- 触发条件: {opp['condition']}\n\n"
    
    # 发送到企业微信群机器人
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": message
        }
    }
    
    try:
        logger.info("开始发送消息到企业微信群机器人")
        response = requests.post(arbitrage_webhook_url, json=payload)
        if response.status_code == 200:
            logger.info("消息发送成功")
        else:
            logger.error(f"发送消息失败: {response.text}")
    except Exception as e:
        logger.error(f"发送消息时出错: {str(e)}")
    
    # 写入日志文件
    try:
        # 创建日志目录
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'trade', 'reports')
        os.makedirs(log_dir, exist_ok=True)
        
        # 生成带时间戳的日志文件名
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d%H")
        log_file = os.path.join(log_dir, f'cross_exchange_arbitrage_{timestamp}.log')
        
        # 写入日志文件
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"=== {now.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            f.write(message)
            f.write("\n\n")
        
        # 追加到合并文件
        combined_file = os.path.join(log_dir, 'cross_exchange_arbitrage')
        with open(combined_file, 'a', encoding='utf-8') as f:
            f.write(f"=== {now.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            f.write(message)
            f.write("\n\n")
        
        logger.info(f"已写入日志文件: {log_file} 和 {combined_file}")
    except Exception as e:
        logger.error(f"写入日志文件时出错: {str(e)}")

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='跨交易所套利监控脚本')
    parser.add_argument('-d', '--debug', action='store_true', help='开启调试日志')
    args = parser.parse_args()
    
    # 设置调试标志
    global DEBUG
    DEBUG = args.debug
    
    logger.info("开始执行跨交易所套利监控")
    if DEBUG:
        logger.info("调试日志已开启")
    
    api = ExchangeAPI()
    
    # 清空合并文件
    try:
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'trade', 'reports')
        os.makedirs(log_dir, exist_ok=True)
        combined_file = os.path.join(log_dir, 'cross_exchange_arbitrage')
        with open(combined_file, 'w', encoding='utf-8') as f:
            f.write(f"=== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            f.write("开始新的监控周期\n\n")
        logger.info(f"已清空合并文件: {combined_file}")
    except Exception as e:
        logger.error(f"清空合并文件时出错: {str(e)}")
    
    # 获取所有交易对
    trading_pairs = get_all_trading_pairs()
    logger.info(f"共获取到{len(trading_pairs)}个交易对")
    
    # 遍历每个交易对
    for token in trading_pairs:
        try:
            # 获取交易对信息
            token_info = get_trading_pair_info(api, token)
            
            # 找出套利机会
            opportunities = find_arbitrage_opportunities(token_info, token)
            if opportunities:
                # 按价差排序
                opportunities.sort(key=lambda x: x['price_diff'], reverse=True)
                # 立即发送通知
                send_to_wechat_robot(opportunities)
                logger.info(f"发现并发送{token}的{len(opportunities)}个套利机会")
        except Exception as e:
            logger.error(f"处理{token}时出错: {str(e)}")
    
    logger.info("跨交易所套利监控执行完成")

if __name__ == "__main__":
    main() 