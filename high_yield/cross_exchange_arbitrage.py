"""
跨交易所套利监控脚本

该脚本用于监控不同交易所之间的价格差异，寻找套利机会。主要功能包括：
1. 从Bitget和GateIO获取所有现货和合约交易对
2. 获取每个交易对在不同交易所的现货和合约价格
3. 计算价格差异，筛选出符合条件的套利机会
4. 将套利机会发送到企业微信群机器人并保存到文件

使用方法：
    python cross_exchange_arbitrage.py

配置说明：
    在config.py中设置：
    - price_diff_threshold: 价格差异阈值（默认0.2%）
    - volume_24h_threshold: 24小时最小交易量阈值（默认20万USDT）
    - arbitrage_webhook_url: 企业微信群机器人webhook地址

作者：Raymon
创建时间：2024-05-06
"""

import time
from datetime import datetime
import requests
import json
from typing import List, Dict, Any, Set, Tuple
from exchange import ExchangeAPI
from config import arbitrage_webhook_url, price_diff_threshold, volume_24h_threshold, proxies
from tools.logger import logger
import os

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
    return requests.get(url, params=params, proxies=proxies)

def get_all_trading_pairs() -> Set[str]:
    """
    从Bitget和GateIO获取所有现货和合约交易对
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
                if isinstance(data, dict):  # 确保data是字典类型
                    result['spot']['Bitget'] = {
                        'price': float(data["close"]),
                        'volume': float(data["baseVol"]) * float(data["close"])
                    }
        
        # 获取合约价格、交易量和资金费率
        futures_url = "https://api.bitget.com/api/v2/mix/market/ticker"
        futures_params = {"symbol": token, "productType": "USDT-FUTURES"}
        futures_response = make_bitget_request(futures_url, params=futures_params, proxies=api.session.proxies)
        if futures_response.status_code == 200:
            futures_data = futures_response.json()
            if futures_data["code"] == "00000" and "data" in futures_data:
                data = futures_data["data"]
                if isinstance(data, dict):  # 确保data是字典类型
                    result['futures']['Bitget'] = {
                        'price': float(data["lastPr"]),
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
    
    # 获取GateIO信息
    try:
        # 获取现货价格和交易量
        spot_url = "https://api.gateio.ws/api/v4/spot/tickers"
        spot_response = requests.get(spot_url, proxies=api.session.proxies)
        if spot_response.status_code == 200:
            spot_data = spot_response.json()
            gate_io_token = token.replace('USDT', '_USDT')
            for ticker in spot_data:
                if ticker['currency_pair'] == gate_io_token:
                    result['spot']['GateIO'] = {
                        'price': float(ticker['last']),
                        'volume': float(ticker['base_volume']) * float(ticker['last'])
                    }
                    break
        
        # 获取合约价格、交易量和资金费率
        futures_url = f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{gate_io_token}"
        futures_response = requests.get(futures_url, proxies=api.session.proxies)
        if futures_response.status_code == 200:
            futures_data = futures_response.json()
            if isinstance(futures_data, dict):  # 确保返回的是字典类型
                result['futures']['GateIO'] = {
                    'price': float(futures_data.get('mark_price', 0)),
                    'volume': float(futures_data.get('volume_24h', 0))
                }
                result['funding_rates']['GateIO'] = float(futures_data.get('funding_rate', 0)) * 100
    except Exception as e:
        logger.error(f"获取GateIO {token}信息失败: {str(e)}")
    
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
    for exchange in ['Bitget', 'GateIO']:
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
    for exchange in ['Bitget', 'GateIO']:
        if (exchange in token_info['funding_rates'] and 
            token_info['funding_rates'][exchange] < 0):
            return opportunities
    
    # 检查合约价格差异
    futures_prices = {}
    for exchange in ['Bitget', 'GateIO']:
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
                
                if price_diff > price_diff_threshold:
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
    for futures_exchange in ['Bitget', 'GateIO']:
        if futures_exchange in token_info['futures']:
            futures_price = token_info['futures'][futures_exchange]['price']
            
            # 检查同一交易所的现货价格
            if futures_exchange in token_info['spot']:
                spot_price = token_info['spot'][futures_exchange]['price']
                price_diff = abs(futures_price - spot_price) / min(futures_price, spot_price) * 100
                
                if price_diff > price_diff_threshold:
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
            for spot_exchange in ['Bitget', 'GateIO']:
                if spot_exchange != futures_exchange and spot_exchange in token_info['spot']:
                    spot_price = token_info['spot'][spot_exchange]['price']
                    price_diff = abs(futures_price - spot_price) / min(futures_price, spot_price) * 100
                    
                    if price_diff > price_diff_threshold:
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
        
        message += f"- 价差: {opp['price_diff']:.2f}%\n"
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
        
        # 写入合并文件（覆盖之前内容）
        combined_file = os.path.join(log_dir, 'cross_exchange_arbitrage')
        with open(combined_file, 'w', encoding='utf-8') as f:
            f.write(f"=== {now.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            f.write(message)
            f.write("\n\n")
        
        logger.info(f"已写入日志文件: {log_file} 和 {combined_file}")
    except Exception as e:
        logger.error(f"写入日志文件时出错: {str(e)}")

def main():
    logger.info("开始执行跨交易所套利监控")
    api = ExchangeAPI()
    
    # 获取所有交易对
    trading_pairs = get_all_trading_pairs()
    logger.info(f"共获取到{len(trading_pairs)}个交易对")
    
    # 存储所有套利机会
    all_opportunities = []
    
    # 遍历每个交易对
    for token in trading_pairs:
        try:
            # 获取交易对信息
            token_info = get_trading_pair_info(api, token)
            
            # 找出套利机会
            opportunities = find_arbitrage_opportunities(token_info, token)
            if opportunities:
                all_opportunities.extend(opportunities)
                logger.info(f"发现{token}的{len(opportunities)}个套利机会")
        except Exception as e:
            logger.error(f"处理{token}时出错: {str(e)}")
    
    # 按价差排序
    all_opportunities.sort(key=lambda x: x['price_diff'], reverse=True)
    
    # 发送到企业微信群机器人
    send_to_wechat_robot(all_opportunities)
    logger.info("跨交易所套利监控执行完成")

if __name__ == "__main__":
    main() 