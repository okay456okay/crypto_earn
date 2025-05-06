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
    - webhook_url: 企业微信群机器人webhook地址

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
from config import webhook_url, funding_rate_threshold, min_funding_rate, volume_24h_threshold
from tools.logger import logger

def calculate_annual_yield(funding_rate: float, interval_hours: int) -> float:
    """
    计算年化收益率
    :param funding_rate: 资金费率（百分比）
    :param interval_hours: 资金费率周期（小时）
    :return: 年化收益率（百分比）
    """
    return funding_rate / interval_hours * 24 * 365

def calculate_p95_yield(funding_rates: List[float], interval_hours: int) -> float:
    """
    计算P95年化收益率
    :param funding_rates: 资金费率列表
    :param interval_hours: 资金费率周期（小时）
    :return: P95年化收益率（百分比）
    """
    if not funding_rates:
        return 0
    p95_rate = np.percentile(funding_rates, 5)  # 取5%分位数，因为资金费率越低越好
    return calculate_annual_yield(p95_rate, interval_hours)

def get_funding_rate_history(api: ExchangeAPI, exchange: str, token: str, days: int) -> List[Dict[str, Any]]:
    """
    获取指定天数的资金费率历史
    :param api: ExchangeAPI实例
    :param exchange: 交易所名称
    :param token: 交易对
    :param days: 天数
    :return: 资金费率历史列表
    """
    end_time = int(time.time() * 1000)
    start_time = end_time - days * 24 * 60 * 60 * 1000
    
    logger.info(f"开始获取{exchange} {token}近{days}天的资金费率历史数据")
    history = []
    
    try:
        if exchange == 'Binance':
            history = api.get_binance_future_funding_rate_history(token, start_time, end_time)
        elif exchange == 'Bitget':
            history = api.get_bitget_futures_funding_rate_history(token, start_time, end_time)
        elif exchange == 'Bybit':
            history = api.get_bybit_futures_funding_rate_history(token, start_time, end_time)
        elif exchange == 'GateIO':
            history = api.get_gateio_futures_funding_rate_history(token, start_time, end_time)
        elif exchange == 'OKX':
            history = api.get_okx_futures_funding_rate_history(token, start_time, end_time)
        
        logger.info(f"{exchange} {token}近{days}天资金费率历史数据获取完成，共{len(history)}条")
    except Exception as e:
        logger.error(f"获取{exchange} {token}历史数据失败: {str(e)}")
    
    return history

def get_24h_volume(api: ExchangeAPI, exchange: str, token: str) -> Dict[str, float]:
    """
    获取交易对的24小时交易量
    :param api: ExchangeAPI实例
    :param exchange: 交易所名称
    :param token: 交易对
    :return: 包含合约和现货交易量的字典
    """
    try:
        if exchange == 'Binance':
            # 获取合约交易量
            future_url = f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={token}"
            future_response = requests.get(future_url, proxies=api.session.proxies)
            future_volume = float(future_response.json()['quoteVolume']) if future_response.status_code == 200 else 0
            
            # 获取现货交易量
            spot_url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={token}"
            spot_response = requests.get(spot_url, proxies=api.session.proxies)
            spot_volume = float(spot_response.json()['quoteVolume']) if spot_response.status_code == 200 else 0
            
        elif exchange == 'Bitget':
            # 获取合约交易量
            future_url = "https://api.bitget.com/api/v2/mix/market/ticker"
            params = {"productType": "USDT-FUTURES", "symbol": token}
            future_response = requests.get(future_url, params=params, proxies=api.session.proxies)
            future_volume = float(future_response.json()['data']['quoteVolume']) if future_response.status_code == 200 else 0
            
            # 获取现货交易量
            spot_url = "https://api.bitget.com/api/v2/spot/market/ticker"
            params = {"symbol": token}
            spot_response = requests.get(spot_url, params=params, proxies=api.session.proxies)
            spot_volume = float(spot_response.json()['data']['quoteVolume']) if spot_response.status_code == 200 else 0
            
        elif exchange == 'Bybit':
            # 获取合约交易量
            future_url = "https://api.bybit.com/v5/market/tickers"
            params = {"category": "linear", "symbol": token}
            future_response = requests.get(future_url, params=params, proxies=api.session.proxies)
            future_volume = float(future_response.json()['result']['list'][0]['volume24h']) if future_response.status_code == 200 else 0
            
            # 获取现货交易量
            spot_url = "https://api.bybit.com/v5/market/tickers"
            params = {"category": "spot", "symbol": token}
            spot_response = requests.get(spot_url, params=params, proxies=api.session.proxies)
            spot_volume = float(spot_response.json()['result']['list'][0]['volume24h']) if spot_response.status_code == 200 else 0
            
        elif exchange == 'GateIO':
            # 获取合约交易量
            future_url = f"https://api.gateio.ws/api/v4/futures/usdt/tickers/{token}"
            future_response = requests.get(future_url, proxies=api.session.proxies)
            future_volume = float(future_response.json()['quote_volume']) if future_response.status_code == 200 else 0
            
            # 获取现货交易量
            spot_url = f"https://api.gateio.ws/api/v4/spot/tickers/{token}"
            spot_response = requests.get(spot_url, proxies=api.session.proxies)
            spot_volume = float(spot_response.json()['quote_volume']) if spot_response.status_code == 200 else 0
            
        elif exchange == 'OKX':
            # 获取合约交易量
            future_url = "https://www.okx.com/api/v5/market/ticker"
            params = {"instId": f"{token}-SWAP"}
            future_response = requests.get(future_url, params=params, proxies=api.session.proxies)
            future_volume = float(future_response.json()['data'][0]['vol24h']) if future_response.status_code == 200 else 0
            
            # 获取现货交易量
            spot_url = "https://www.okx.com/api/v5/market/ticker"
            params = {"instId": f"{token}-SPOT"}
            spot_response = requests.get(spot_url, params=params, proxies=api.session.proxies)
            spot_volume = float(spot_response.json()['data'][0]['vol24h']) if spot_response.status_code == 200 else 0
        
        logger.info(f"{exchange} {token} 24小时交易量: 合约={future_volume:.2f} USDT, 现货={spot_volume:.2f} USDT")
        return {'future_volume': future_volume, 'spot_volume': spot_volume}
    except Exception as e:
        logger.error(f"获取{exchange} {token} 24小时交易量失败: {str(e)}")
        return {'future_volume': 0, 'spot_volume': 0}

def send_to_wechat_robot(data: List[Dict[str, Any]]):
    """
    发送数据到企业微信群机器人
    :param data: 要发送的数据
    """
    if not data:
        logger.info("没有符合条件的套利机会，不发送消息")
        return
    
    # 构建消息内容
    message = "## 高资金费率套利机会\n\n"
    message += "| 交易所 | 交易对 | 当前资金费率 | 周期(小时) | 当前年化 | 1天P95年化 | 3天P95年化 | 合约交易量(USDT) | 现货交易量(USDT) |\n"
    message += "|--------|--------|--------------|------------|----------|------------|------------|------------------|------------------|\n"
    
    for item in data:
        message += f"| {item['exchange']} | {item['token']} | {item['funding_rate']:.4f}% | {item['interval_hours']} | "
        message += f"{item['current_yield']:.2f}% | {item['p95_yield_1d']:.2f}% | {item['p95_yield_3d']:.2f}% | "
        message += f"{item['future_volume']:,.2f} | {item['spot_volume']:,.2f} |\n"
    
    # 发送到企业微信群机器人
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": message
        }
    }
    
    try:
        logger.info("开始发送消息到企业微信群机器人")
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            logger.info("消息发送成功")
        else:
            logger.error(f"发送消息失败: {response.text}")
    except Exception as e:
        logger.error(f"发送消息时出错: {str(e)}")

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
            for item in data:
                if float(item['lastFundingRate']) * 100 >= min_funding_rate:  # 转换为百分比
                    all_rates.append({
                        'exchange': 'Binance',
                        'token': item['symbol'],
                        'fundingRate': float(item['lastFundingRate']) * 100,
                        'fundingTime': int(item['nextFundingTime']),
                        'fundingIntervalHours': 8,  # Binance固定8小时
                        'markPrice': float(item['markPrice'])
                    })
            logger.info(f"获取到Binance {len(all_rates)}个合约的资金费率")
    except Exception as e:
        logger.error(f"获取Binance所有合约资金费率失败: {str(e)}")
    
    # 获取Bitget所有合约资金费率
    try:
        logger.info("开始获取Bitget所有合约资金费率")
        url = "https://api.bitget.com/api/v2/mix/market/current-fund-rate"
        params = {"productType": "USDT-FUTURES"}
        response = requests.get(url, params=params, proxies=api.session.proxies)
        if response.status_code == 200:
            data = response.json()
            if data["code"] == "00000" and "data" in data:
                for item in data["data"]:
                    if float(item['fundingRate']) * 100 >= min_funding_rate:
                        all_rates.append({
                            'exchange': 'Bitget',
                            'token': item['symbol'],
                            'fundingRate': float(item['fundingRate']) * 100,
                            'fundingTime': int(item['fundingTime']),
                            'fundingIntervalHours': 8,  # Bitget固定8小时
                            'markPrice': float(item['markPrice'])
                        })
            logger.info(f"获取到Bitget {len(data.get('data', []))}个合约的资金费率")
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
                for item in data["result"]["list"]:
                    if float(item['fundingRate']) * 100 >= min_funding_rate:
                        all_rates.append({
                            'exchange': 'Bybit',
                            'token': item['symbol'],
                            'fundingRate': float(item['fundingRate']) * 100,
                            'fundingTime': int(item['nextFundingTime']),
                            'fundingIntervalHours': 8,  # Bybit固定8小时
                            'markPrice': float(item['markPrice'])
                        })
            logger.info(f"获取到Bybit {len(data['result'].get('list', []))}个合约的资金费率")
    except Exception as e:
        logger.error(f"获取Bybit所有合约资金费率失败: {str(e)}")
    
    # 获取GateIO所有合约资金费率
    try:
        logger.info("开始获取GateIO所有合约资金费率")
        url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
        response = requests.get(url, proxies=api.session.proxies)
        if response.status_code == 200:
            data = response.json()
            for item in data:
                if not item['in_delisting'] and float(item['funding_rate']) * 100 >= min_funding_rate:
                    all_rates.append({
                        'exchange': 'GateIO',
                        'token': item['name'].replace('_USDT', 'USDT'),
                        'fundingRate': float(item['funding_rate']) * 100,
                        'fundingTime': int(item['funding_next_apply']) * 1000,
                        'fundingIntervalHours': int(item['funding_interval'] / 3600),
                        'markPrice': float(item['mark_price'])
                    })
            logger.info(f"获取到GateIO {len(data)}个合约的资金费率")
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
                for item in data["data"]:
                    if float(item['fundingRate']) * 100 >= min_funding_rate:
                        all_rates.append({
                            'exchange': 'OKX',
                            'token': item['instId'].replace('-USDT-SWAP', 'USDT'),
                            'fundingRate': float(item['fundingRate']) * 100,
                            'fundingTime': int(item['nextFundingTime']),
                            'fundingIntervalHours': 8,  # OKX固定8小时
                            'markPrice': float(item['markPrice'])
                        })
            logger.info(f"获取到OKX {len(data.get('data', []))}个合约的资金费率")
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
                logger.info(f"{rate['exchange']} {rate['token']}交易量不足，跳过")
                continue
            
            # 获取1天和3天的历史数据
            history_1d = get_funding_rate_history(api, rate['exchange'], rate['token'], 1)
            history_3d = get_funding_rate_history(api, rate['exchange'], rate['token'], 3)
            
            # 提取资金费率列表
            rates_1d = [h['fundingRate'] for h in history_1d]
            rates_3d = [h['fundingRate'] for h in history_3d]
            
            # 计算P95年化收益率
            p95_yield_1d = calculate_p95_yield(rates_1d, rate['fundingIntervalHours'])
            p95_yield_3d = calculate_p95_yield(rates_3d, rate['fundingIntervalHours'])
            
            logger.info(f"{rate['exchange']} {rate['token']} P95年化收益率: 1天={p95_yield_1d:.2f}%, 3天={p95_yield_3d:.2f}%")
            
            filtered_rates.append({
                'exchange': rate['exchange'],
                'token': rate['token'],
                'funding_rate': rate['fundingRate'],
                'interval_hours': rate['fundingIntervalHours'],
                'current_yield': current_yield,
                'p95_yield_1d': p95_yield_1d,
                'p95_yield_3d': p95_yield_3d,
                'future_volume': volumes['future_volume'],
                'spot_volume': volumes['spot_volume']
            })
    
    # 按当前年化收益率排序
    filtered_rates.sort(key=lambda x: x['current_yield'], reverse=True)
    
    # 发送到企业微信群机器人
    send_to_wechat_robot(filtered_rates)
    logger.info("资金费率套利监控执行完成")

if __name__ == "__main__":
    main() 