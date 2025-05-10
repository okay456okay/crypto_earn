"""
资金费率套利监控脚本

该脚本用于监控各大交易所的资金费率，寻找高收益的套利机会。主要功能包括：
1. 从Binance、Bitget、Bybit、GateIO、OKX等交易所获取资金费率数据
2. 获取交易对的24小时合约和现货交易量
3. 计算年化收益率，筛选出收益率超过阈值的交易对
4. 获取历史资金费率数据，计算P95值
5. 将符合条件的套利机会发送到企业微信群机器人

使用方法：
    python funding_rate_arbitrage.py

配置说明：
    在config.py中设置：
    - funding_rate_threshold: 资金费率年化收益率阈值（默认30%）
    - min_funding_rate: 最小资金费率阈值（默认0.01%）
    - volume_24h_threshold: 24小时最小交易量阈值（默认20万USDT）
    - funding_rate_webhook_url: 企业微信群机器人webhook地址

作者：Raymon
创建时间：2024-05-06
"""

import time
from datetime import datetime, timedelta
import numpy as np
import requests
import json
from typing import List, Dict, Any
from exchange import ExchangeAPI
from config import funding_rate_webhook_url, funding_rate_threshold, min_avg_yield_threshold, min_funding_rate, volume_24h_threshold, proxies
from tools.logger import logger
import ccxt
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

def calculate_annual_yield(funding_rate: float, interval_hours: int) -> float:
    """
    计算年化收益率
    :param funding_rate: 资金费率（百分比）
    :param interval_hours: 资金费率周期（小时）
    :return: 年化收益率（百分比）
    """
    return funding_rate / interval_hours * 24 * 365

def calculate_average_annual_yield(funding_rates: List[Dict[str, Any]], interval_hours: int) -> float:
    """
    计算平均年化收益率
    :param funding_rates: 资金费率历史列表，每个元素包含fundingRate和fundingIntervalHours
    :param interval_hours: 当前资金费率周期（小时）
    :return: 平均年化收益率（百分比）
    """
    if not funding_rates:
        return 0
    
    total_yield = 0
    for rate in funding_rates:
        # 使用实际的资金费率周期计算
        actual_interval = rate.get('fundingIntervalHours', interval_hours)
        yield_per_period = calculate_annual_yield(rate['fundingRate'], actual_interval)
        total_yield += yield_per_period
    
    return total_yield / len(funding_rates)

def get_funding_rate_history(exchange: str, token: str, days: int) -> List[Dict[str, Any]]:
    """获取指定交易所和交易对的历史资金费率
    
    Args:
        exchange: 交易所名称
        token: 交易对
        days: 获取天数
        
    Returns:
        历史资金费率列表，每个元素包含fundingRate、fundingTime和fundingIntervalHours
    """
    history_rates = []
    api = ExchangeAPI()
    
    # 计算时间范围
    end_time = int(time.time() * 1000)
    start_time = end_time - days * 24 * 60 * 60 * 1000
    
    # 获取Binance历史资金费率
    if exchange == 'Binance':
        try:
            logger.info(f"开始获取Binance {token}的历史资金费率")
            # 获取资金费率周期信息
            if not api.binance_funding_info:
                api.get_binance_funding_info()
            funding_interval = api.binance_funding_info.get(token, {}).get('fundingIntervalHours', 8)
            
            # 使用exchange.py中的方法获取历史数据
            history = api.get_binance_future_funding_rate_history(token, start_time, end_time)
            for item in history:
                history_rates.append({
                    'fundingRate': float(item['fundingRate']) * 100,
                    'fundingTime': item['fundingTime'],
                    'fundingIntervalHours': funding_interval
                })
            logger.info(f"获取到Binance {token} {len(history_rates)}条历史资金费率")
        except Exception as e:
            logger.error(f"获取Binance {token}历史资金费率失败: {str(e)}")
    
    # 获取Bitget历史资金费率
    elif exchange == 'Bitget':
        try:
            logger.info(f"开始获取Bitget {token}的历史资金费率")
            # 获取资金费率周期
            funding_time, funding_interval = api.get_bitget_futures_funding_time(token)
            if not funding_interval:
                funding_interval = 8  # 默认8小时
            
            # 使用exchange.py中的方法获取历史数据
            history = api.get_bitget_futures_funding_rate_history(token, start_time, end_time)
            for item in history:
                history_rates.append({
                    'fundingRate': float(item['fundingRate']) * 100,
                    'fundingTime': item['fundingTime'],
                    'fundingIntervalHours': funding_interval
                })
            logger.info(f"获取到Bitget {token} {len(history_rates)}条历史资金费率")
        except Exception as e:
            logger.error(f"获取Bitget {token}历史资金费率失败: {str(e)}")
    
    # 获取Bybit历史资金费率
    elif exchange == 'Bybit':
        try:
            logger.info(f"开始获取Bybit {token}的历史资金费率")
            # 获取资金费率周期
            funding_rate_history = api.get_bybit_futures_funding_rate_history(token, start_time, end_time)
            funding_interval = 8  # 默认8小时
            if funding_rate_history:
                funding_interval = abs(int((funding_rate_history[0]['fundingTime'] - funding_rate_history[1]['fundingTime']) / 1000 / 60 / 60))
            
            # 使用exchange.py中的方法获取历史数据
            history = api.get_bybit_futures_funding_rate_history(token, start_time, end_time)
            for item in history:
                history_rates.append({
                    'fundingRate': float(item['fundingRate']) * 100,
                    'fundingTime': item['fundingTime'],
                    'fundingIntervalHours': funding_interval
                })
            logger.info(f"获取到Bybit {token} {len(history_rates)}条历史资金费率")
        except Exception as e:
            logger.error(f"获取Bybit {token}历史资金费率失败: {str(e)}")
    
    # 获取Gate.io历史资金费率
    elif exchange == 'GateIO':
        try:
            logger.info(f"开始获取Gate.io {token}的历史资金费率")
            # 获取资金费率周期
            if not api.gateio_futures_volumes:
                api.get_gateio_futures_volumes()
            future_volume = api.gateio_futures_volumes.get(token, 0)
            # 获取合约价格
            gate_io_token = token.replace('USDT', '_USDT')
            url = f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{gate_io_token}"
            response = requests.get(url, proxies=api.session.proxies)
            if response.status_code == 200:
                data = response.json()
                future_price = float(data['mark_price'])
            else:
                future_price = 0
            
            # 使用exchange.py中的方法获取历史数据
            history = api.get_gateio_futures_funding_rate_history(token, start_time, end_time)
            for item in history:
                history_rates.append({
                    'fundingRate': item['fundingRate'],
                    'fundingTime': item['fundingTime'],
                    'fundingIntervalHours': 8,  # Gate.io固定8小时
                    'markPrice': future_price
                })
            logger.info(f"获取到Gate.io {token} {len(history_rates)}条历史资金费率")
        except Exception as e:
            logger.error(f"获取Gate.io {token}历史资金费率失败: {str(e)}")
    
    # 获取OKX历史资金费率
    elif exchange == 'OKX':
        try:
            logger.info(f"开始获取OKX {token}的历史资金费率")
            # 获取资金费率周期
            symbol = token.replace('USDT', '/USDT:USDT')
            exchange = ccxt.okx({'proxies': proxies})
            funding_rate_info = exchange.fetch_funding_rate(symbol)
            funding_interval = int((funding_rate_info['nextFundingTimestamp'] - funding_rate_info['fundingTimestamp']) / 1000 / 60 / 60)
            
            # 使用exchange.py中的方法获取历史数据
            history = api.get_okx_futures_funding_rate_history(token, start_time, end_time)
            for item in history:
                history_rates.append({
                    'fundingRate': item['fundingRate'],
                    'fundingTime': item['fundingTime'],
                    'fundingIntervalHours': funding_interval
                })
            logger.info(f"获取到OKX {token} {len(history_rates)}条历史资金费率")
        except Exception as e:
            logger.error(f"获取OKX {token}历史资金费率失败: {str(e)}")
    
    # 按时间排序
    history_rates.sort(key=lambda x: x['fundingTime'])
    
    # 只保留指定天数的数据
    if history_rates:
        cutoff_time = int(time.time() * 1000) - days * 24 * 60 * 60 * 1000
        history_rates = [rate for rate in history_rates if rate['fundingTime'] >= cutoff_time]
    
    return history_rates

def get_24h_volume(api: ExchangeAPI, exchange: str, token: str) -> Dict[str, Any]:
    """
    获取交易对的24小时交易量
    :param api: ExchangeAPI实例
    :param exchange: 交易所名称（合约所在交易所）
    :param token: 交易对
    :return: 包含合约和现货交易量的字典
    """
    try:
        # 获取合约交易量（仅从指定交易所获取）
        future_volume = 0
        future_price = 0
        if exchange == 'Binance':
            if not api.binance_futures_volumes:
                api.get_binance_futures_volumes()
            future_volume = api.binance_futures_volumes.get(token, 0)
            # 获取合约价格
            url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={token}"
            response = requests.get(url, proxies=api.session.proxies)
            if response.status_code == 200:
                future_price = float(response.json()['price'])
        elif exchange == 'Bitget':
            if not api.bitget_futures_volumes:
                api.get_bitget_futures_volumes()
            future_volume = api.bitget_futures_volumes.get(token, 0)
            # 获取合约价格
            url = "https://api.bitget.com/api/v2/mix/market/symbol-price"
            params = {"symbol": token, "productType": "USDT-FUTURES"}
            response = make_bitget_request(url, params=params, proxies=api.session.proxies)
            if response.status_code == 200:
                data = response.json()
                if data["code"] == "00000" and "data" in data:
                    future_price = float(data["data"][0]["price"])
        elif exchange == 'Bybit':
            if not api.bybit_futures_volumes:
                api.get_bybit_futures_volumes()
            future_volume = api.bybit_futures_volumes.get(token, 0)
            # 获取合约价格
            url = "https://api.bybit.com/v5/market/tickers"
            params = {"category": "linear", "symbol": token}
            response = requests.get(url, params=params, proxies=api.session.proxies)
            if response.status_code == 200:
                data = response.json()
                if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
                    future_price = float(data["result"]["list"][0]["lastPrice"])
        elif exchange == 'GateIO':
            if not api.gateio_futures_volumes:
                api.get_gateio_futures_volumes()
            future_volume = api.gateio_futures_volumes.get(token, 0)
            # 获取合约价格
            gate_io_token = token.replace('USDT', '_USDT')
            url = f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{gate_io_token}"
            response = requests.get(url, proxies=api.session.proxies)
            if response.status_code == 200:
                data = response.json()
                future_price = float(data['mark_price'])
        elif exchange == 'OKX':
            if not api.okx_futures_volumes:
                api.get_okx_futures_volumes()
            future_volume = api.okx_futures_volumes.get(token, 0)
            # 获取合约价格
            symbol = token.replace('USDT', '/USDT:USDT')
            exchange = ccxt.okx({'proxies': proxies})
            ticker = exchange.fetch_ticker(symbol)
            future_price = float(ticker['last'])
        
        # 获取现货交易量（从所有交易所获取，取最大值）
        spot_volume = 0
        spot_price = 0
        spot_exchange = ''
        spot_token = token.replace('USDT', '')
        
        # 获取所有交易所的现货交易量
        if not api.binance_volumes:
            api.get_binance_volumes()
        if not api.bitget_volumes:
            api.get_bitget_volumes()
        if not api.bybit_volumes:
            api.get_bybit_volumes()
        if not api.gateio_volumes:
            api.get_gateio_volumes()
        if not api.okx_volumes:
            api.get_okx_volumes()
        
        # 从所有交易所中获取最大的现货交易量
        spot_volumes = {
            'Binance': api.binance_volumes.get(spot_token, 0),
            'Bitget': api.bitget_volumes.get(spot_token, 0),
            'Bybit': api.bybit_volumes.get(spot_token, 0),
            'GateIO': api.gateio_volumes.get(spot_token, 0),
            'OKX': api.okx_volumes.get(spot_token, 0)
        }
        
        # 获取最大交易量对应的交易所
        spot_exchange = max(spot_volumes.items(), key=lambda x: x[1])[0]
        spot_volume = spot_volumes[spot_exchange]
        
        # 获取最大交易量交易所的现货价格
        if spot_exchange == 'Binance':
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={spot_token}USDT"
            response = requests.get(url, proxies=api.session.proxies)
            if response.status_code == 200:
                spot_price = float(response.json()['price'])
        elif spot_exchange == 'Bitget':
            url = "https://api.bitget.com/api/spot/v1/market/ticker"
            params = {"symbol": f"{spot_token}USDT_SPBL"}
            response = make_bitget_request(url, params=params, proxies=api.session.proxies)
            if response.status_code == 200:
                data = response.json()
                if data["code"] == "00000" and "data" in data:
                    spot_price = float(data["data"]["close"])
        elif spot_exchange == 'Bybit':
            url = "https://api.bybit.com/v5/market/tickers"
            params = {"category": "spot", "symbol": f"{spot_token}USDT"}
            response = requests.get(url, params=params, proxies=api.session.proxies)
            if response.status_code == 200:
                data = response.json()
                if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
                    spot_price = float(data["result"]["list"][0]["lastPrice"])
        elif spot_exchange == 'GateIO':
            url = "https://api.gateio.ws/api/v4/spot/tickers"
            response = requests.get(url, proxies=api.session.proxies)
            if response.status_code == 200:
                data = response.json()
                gate_io_token = spot_token + '_USDT'
                for ticker in data:
                    if ticker['currency_pair'] == gate_io_token:
                        spot_price = float(ticker['last'])
                        break
        elif spot_exchange == 'OKX':
            url = f"https://www.okx.com/api/v5/market/ticker?instId={spot_token}-USDT"
            response = requests.get(url, proxies=api.session.proxies)
            if response.status_code == 200:
                data = response.json()
                if data["code"] == "0" and "data" in data:
                    spot_price = float(data["data"][0]["last"])
        
        # 计算价差
        price_diff = 0
        if spot_price > 0:
            price_diff = (future_price - spot_price) / spot_price * 100
        
        # 记录日志，显示每个交易所的现货交易量
        logger.info(f"{token} 24小时交易量:")
        logger.info(f"合约({exchange}): {future_volume:.2f} USDT")
        logger.info(f"现货交易量:")
        logger.info(f"- Binance: {spot_volumes['Binance']:.2f} USDT")
        logger.info(f"- Bitget: {spot_volumes['Bitget']:.2f} USDT")
        logger.info(f"- Bybit: {spot_volumes['Bybit']:.2f} USDT")
        logger.info(f"- GateIO: {spot_volumes['GateIO']:.2f} USDT")
        logger.info(f"- OKX: {spot_volumes['OKX']:.2f} USDT")
        logger.info(f"最大现货交易量: {spot_volume:.2f} USDT (来自{spot_exchange})")
        
        return {
            'future_volume': future_volume,
            'spot_volume': spot_volume,
            'spot_exchange': spot_exchange,
            'future_price': future_price,
            'spot_price': spot_price,
            'price_diff': price_diff
        }
    except Exception as e:
        logger.error(f"获取{token} 24小时交易量失败: {str(e)}")
        return {
            'future_volume': 0,
            'spot_volume': 0,
            'spot_exchange': '',
            'future_price': 0,
            'spot_price': 0,
            'price_diff': 0
        }

def send_to_wechat_robot(data: List[Dict[str, Any]]):
    """
    发送数据到企业微信群机器人，并写入日志文件
    :param data: 要发送的数据
    """
    if not data:
        logger.info("没有符合条件的套利机会，不发送消息")
        return
    
    # 构建消息内容
    message = "## 高资金费率套利机会\n\n"
    
    for index, item in enumerate(data, 1):
        message += f"{index}. {item['exchange']} {item['token']}\n"
        message += f"- 当前资金费率: {item['funding_rate']:.4f}%\n"
        message += f"- 资金费率周期: {item['interval_hours']}小时\n"
        message += f"- 当前年化收益率: {item['current_yield']:.2f}%\n"
        message += f"- 近1天平均年化收益率: {item['avg_yield_1d']:.2f}%\n"
        message += f"- 近3天平均年化收益率: {item['avg_yield_3d']:.2f}%\n"
        message += f"- 24小时合约交易量: {item['future_volume']:,.2f} USDT\n"
        message += f"- 24小时现货交易量({item['spot_exchange']}): {item['spot_volume']:,.2f} USDT\n"
        message += f"- 合约价格: {item['future_price']:.4f} USDT\n"
        message += f"- 现货价格({item['spot_exchange']}): {item['spot_price']:.4f} USDT\n"
        message += f"- 价差: {item['price_diff']:.2f}%\n\n"
    
    # 发送到企业微信群机器人
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": message
        }
    }
    
    try:
        logger.info("开始发送消息到企业微信群机器人")
        response = requests.post(funding_rate_webhook_url, json=payload)
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
        log_file = os.path.join(log_dir, f'funding_rate_arbitrage_{timestamp}.log')
        
        # 写入日志文件
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"=== {now.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            f.write(message)
            f.write("\n\n")
        
        # 写入合并文件（覆盖之前内容）
        combined_file = os.path.join(log_dir, 'funding_rate_arbitrage')
        with open(combined_file, 'w', encoding='utf-8') as f:
            f.write(f"=== {now.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
            f.write(message)
            f.write("\n\n")
        
        logger.info(f"已写入日志文件: {log_file} 和 {combined_file}")
    except Exception as e:
        logger.error(f"写入日志文件时出错: {str(e)}")

def get_all_funding_rates(api: ExchangeAPI) -> List[Dict[str, Any]]:
    """
    获取所有交易所的资金费率信息
    :param api: ExchangeAPI实例
    :return: 资金费率信息列表
    """
    all_rates = []
    # 获取Binance所有合约资金费率
    try:
        logger.info("开始获取Binance所有合约资金费率")
        url = "https://fapi.binance.com/fapi/v1/premiumIndex"
        response = requests.get(url, proxies=api.session.proxies)
        if response.status_code == 200:
            data = response.json()
            count = 0
            for item in data:
                if float(item['lastFundingRate']) * 100 >= min_funding_rate:  # 转换为百分比
                    count += 1
                    all_rates.append({
                        'exchange': 'Binance',
                        'token': item['symbol'],
                        'fundingRate': float(item['lastFundingRate']) * 100,
                        'fundingTime': int(item['nextFundingTime']),
                        'fundingIntervalHours': 8,  # Binance固定8小时
                        'markPrice': float(item['markPrice'])
                    })
            logger.info(f"获取到Binance {count}/{len(data)}个资金费率超过{min_funding_rate}%的合约")
    except Exception as e:
        logger.error(f"获取Binance所有合约资金费率失败: {str(e)}")
    
    # 获取Bitget所有合约资金费率
    try:
        logger.info("开始获取Bitget所有合约资金费率")
        # 首先获取所有合约信息
        contracts_url = "https://api.bitget.com/api/v2/mix/market/contracts"
        params = {"productType": "usdt-futures"}
        contracts_response = make_bitget_request(contracts_url, params=params, proxies=api.session.proxies)
        if contracts_response.status_code == 200:
            contracts_data = contracts_response.json()
            if contracts_data["code"] == "00000" and "data" in contracts_data:
                # 获取当前资金费率
                funding_url = "https://api.bitget.com/api/v2/mix/market/current-fund-rate"
                funding_params = {"productType": "USDT-FUTURES"}
                funding_response = make_bitget_request(funding_url, params=funding_params, proxies=api.session.proxies)
                if funding_response.status_code == 200:
                    funding_data = funding_response.json()
                    if funding_data["code"] == "00000" and "data" in funding_data:
                        # 创建symbol到funding rate的映射
                        funding_rates = {item['symbol']: float(item['fundingRate']) * 100 for item in funding_data["data"]}
                        
                        # 遍历所有合约
                        count = 0
                        for contract in contracts_data["data"]:
                            symbol = contract['symbol']
                            if symbol in funding_rates and funding_rates[symbol] >= min_funding_rate:
                                count += 1
                                all_rates.append({
                                    'exchange': 'Bitget',
                                    'token': symbol,
                                    'fundingRate': funding_rates[symbol],
                                    'fundingTime': int(time.time() * 1000),  # 使用当前时间，因为API没有提供下次资金费率时间
                                    'fundingIntervalHours': int(contract['fundInterval']),  # 使用合约配置的资金费率周期
                                    'markPrice': 0  # 暂时不获取标记价格
                                })
            logger.info(f"获取到Bitget {count}/{len(contracts_data['data'])}个资金费率超过{min_funding_rate}%的合约")
    except Exception as e:
        logger.error(f"获取Bitget所有合约资金费率失败: {str(e)}")

    # 获取Bybit所有合约资金费率
    try:
        logger.info("开始获取Bybit所有合约资金费率")
        url = "https://api.bybit.com/v5/market/tickers"
        params = {"category": "linear"}
        response = requests.get(url, params=params, proxies=api.session.proxies)
        if response.status_code == 200:
            data = response.json()
            if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
                count = 0
                for item in data["result"]["list"]:
                    if item['fundingRate'] and float(item['fundingRate']) * 100 >= min_funding_rate:
                        count += 1
                        all_rates.append({
                            'exchange': 'Bybit',
                            'token': item['symbol'],
                            'fundingRate': float(item['fundingRate']) * 100,
                            'fundingTime': int(item['nextFundingTime']),
                            'fundingIntervalHours': 8,  # Bybit固定8小时
                            'markPrice': float(item['markPrice'])
                        })
            logger.info(f"获取到Bybit {count}/{len(data['result'].get('list', []))}个资金费率超过{min_funding_rate}%的合约")
    except Exception as e:
        logger.error(f"获取Bybit所有合约资金费率失败: {str(e)}")
    
    # 获取GateIO所有合约资金费率
    try:
        logger.info("开始获取GateIO所有合约资金费率")
        url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
        response = requests.get(url, proxies=api.session.proxies)
        if response.status_code == 200:
            data = response.json()
            count = 0
            for item in data:
                if not item['in_delisting'] and float(item['funding_rate']) * 100 >= min_funding_rate:
                    count += 1
                    all_rates.append({
                        'exchange': 'GateIO',
                        'token': item['name'].replace('_USDT', 'USDT'),
                        'fundingRate': float(item['funding_rate']) * 100,
                        'fundingTime': int(item['funding_next_apply']) * 1000,
                        'fundingIntervalHours': int(item['funding_interval'] / 3600),
                        'markPrice': float(item['mark_price'])
                    })
            logger.info(f"获取到GateIO {count}/{len(data)}个资金费率超过{min_funding_rate}%的合约")
    except Exception as e:
        logger.error(f"获取GateIO所有合约资金费率失败: {str(e)}")
    
    # 获取OKX所有合约资金费率
    try:
        logger.info("开始获取OKX所有合约资金费率")
        url = "https://www.okx.com/api/v5/public/funding-rate"
        params = {"instType": "SWAP"}
        response = requests.get(url, params=params, proxies=api.session.proxies)
        if response.status_code == 200:
            data = response.json()
            if data["code"] == "0" and "data" in data:
                count = 0
                for item in data["data"]:
                    if float(item['fundingRate']) * 100 >= min_funding_rate:
                        count += 1
                        all_rates.append({
                            'exchange': 'OKX',
                            'token': item['instId'].replace('-USDT-SWAP', 'USDT'),
                            'fundingRate': float(item['fundingRate']) * 100,
                            'fundingTime': int(item['nextFundingTime']),
                            'fundingIntervalHours': 8,  # OKX固定8小时
                            'markPrice': float(item['markPrice'])
                        })
            logger.info(f"获取到OKX {count}/{len(data.get('data', []))}个资金费率超过{min_funding_rate}%的合约")
    except Exception as e:
        logger.error(f"获取OKX所有合约资金费率失败: {str(e)}")
    
    return all_rates

def main():
    logger.info("开始执行资金费率套利监控")
    api = ExchangeAPI()
    
    # 获取所有交易所的资金费率信息
    all_rates = get_all_funding_rates(api)
    logger.info(f"共获取到{len(all_rates)}个合约的资金费率信息")
    
    # 筛选符合条件的交易对
    filtered_rates = []
    for rate in all_rates:
        # 计算当前年化收益率
        current_yield = calculate_annual_yield(rate['fundingRate'], rate['fundingIntervalHours'])
        logger.debug(f"{rate['exchange']} {rate['token']}当前年化收益率: {current_yield:.2f}%")
        
        # 如果当前年化收益率超过阈值，获取历史数据和交易量
        if current_yield >= funding_rate_threshold:
            logger.info(f"{rate['exchange']} {rate['token']}年化收益率{current_yield:.2f}%超过阈值{funding_rate_threshold}%，开始获取历史数据")
            
            # 获取24小时交易量
            volumes = get_24h_volume(api, rate['exchange'], rate['token'])
            if volumes['future_volume'] < volume_24h_threshold or volumes['spot_volume'] < volume_24h_threshold:
                logger.info(f"{rate['exchange']} {rate['token']}现货交易量:{volumes['spot_volume']}、合约交易量:{volumes['future_volume']}不足 {volume_24h_threshold}，跳过")
                continue
            
            # 获取1天和3天的历史数据
            history_1d = get_funding_rate_history(rate['exchange'], rate['token'], 1)
            history_3d = get_funding_rate_history(rate['exchange'], rate['token'], 3)
            
            # 计算平均年化收益率
            avg_yield_1d = calculate_average_annual_yield(history_1d, rate['fundingIntervalHours'])
            avg_yield_3d = calculate_average_annual_yield(history_3d, rate['fundingIntervalHours'])
            
            # 检查平均年化收益率是否达到阈值
            if avg_yield_1d < min_avg_yield_threshold or avg_yield_3d < min_avg_yield_threshold:
                logger.info(f"{rate['exchange']} {rate['token']}平均年化收益率不足{min_avg_yield_threshold}%，跳过")
                continue
            
            logger.info(f"{rate['exchange']} {rate['token']} 平均年化收益率: 1天={avg_yield_1d:.2f}%, 3天={avg_yield_3d:.2f}%")
            
            filtered_rates.append({
                'exchange': rate['exchange'],
                'token': rate['token'],
                'funding_rate': rate['fundingRate'],
                'interval_hours': rate['fundingIntervalHours'],
                'current_yield': current_yield,
                'avg_yield_1d': avg_yield_1d,
                'avg_yield_3d': avg_yield_3d,
                'future_volume': volumes['future_volume'],
                'spot_volume': volumes['spot_volume'],
                'spot_exchange': volumes['spot_exchange'],
                'future_price': volumes['future_price'],
                'spot_price': volumes['spot_price'],
                'price_diff': volumes['price_diff']
            })
    
    # 按当前年化收益率排序
    filtered_rates.sort(key=lambda x: x['current_yield'], reverse=True)
    
    # 发送到企业微信群机器人
    send_to_wechat_robot(filtered_rates)
    logger.info("资金费率套利监控执行完成")

if __name__ == "__main__":
    # api = ExchangeAPI()
    # for exchange in ['Binance', 'Bybit', 'Bitget', 'GateIO', 'OKX']:
    #     get_24h_volume(api, exchange, 'HNTUSDT')
    #     print(exchange, get_funding_rate_history(exchange, 'ETHUSDT', days=1))
    main()