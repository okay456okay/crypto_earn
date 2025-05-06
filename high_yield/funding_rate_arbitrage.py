import time
from datetime import datetime, timedelta
import numpy as np
import requests
import json
from typing import List, Dict, Any
from exchange import ExchangeAPI
from config import webhook_url, funding_rate_threshold

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

def get_funding_rate_history(api: ExchangeAPI, token: str, days: int) -> List[Dict[str, Any]]:
    """
    获取指定天数的资金费率历史
    :param api: ExchangeAPI实例
    :param token: 交易对
    :param days: 天数
    :return: 资金费率历史列表
    """
    end_time = int(time.time() * 1000)
    start_time = end_time - days * 24 * 60 * 60 * 1000
    
    history = []
    # 获取各交易所的资金费率历史
    history.extend(api.get_binance_future_funding_rate_history(token, start_time, end_time))
    history.extend(api.get_bitget_futures_funding_rate_history(token, start_time, end_time))
    history.extend(api.get_bybit_futures_funding_rate_history(token, start_time, end_time))
    history.extend(api.get_gateio_futures_funding_rate_history(token, start_time, end_time))
    history.extend(api.get_okx_futures_funding_rate_history(token, start_time, end_time))
    
    return history

def send_to_wechat_robot(data: List[Dict[str, Any]]):
    """
    发送数据到企业微信群机器人
    :param data: 要发送的数据
    """
    if not data:
        return
    
    # 构建消息内容
    message = "## 高资金费率套利机会\n\n"
    message += "| 交易所 | 交易对 | 当前资金费率 | 周期(小时) | 当前年化 | 1天P95年化 | 3天P95年化 |\n"
    message += "|--------|--------|--------------|------------|----------|------------|------------|\n"
    
    for item in data:
        message += f"| {item['exchange']} | {item['token']} | {item['funding_rate']:.4f}% | {item['interval_hours']} | "
        message += f"{item['current_yield']:.2f}% | {item['p95_yield_1d']:.2f}% | {item['p95_yield_3d']:.2f}% |\n"
    
    # 发送到企业微信群机器人
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": message
        }
    }
    
    try:
        response = requests.post(webhook_url, json=payload)
        if response.status_code != 200:
            print(f"发送消息失败: {response.text}")
    except Exception as e:
        print(f"发送消息时出错: {str(e)}")

def main():
    api = ExchangeAPI()
    
    # 获取所有交易所的资金费率信息
    all_rates = []
    tokens = ['BTCUSDT', 'ETHUSDT']  # 这里可以扩展更多token
    
    for token in tokens:
        rates = api.get_funding_rate(token)
        for rate in rates:
            if not rate:  # 跳过空数据
                continue
                
            # 计算当前年化收益率
            current_yield = calculate_annual_yield(rate['fundingRate'], rate['fundingIntervalHours'])
            
            # 如果当前年化收益率超过阈值，获取历史数据
            if current_yield >= funding_rate_threshold:
                # 获取1天和3天的历史数据
                history_1d = get_funding_rate_history(api, token, 1)
                history_3d = get_funding_rate_history(api, token, 3)
                
                # 提取资金费率列表
                rates_1d = [h['fundingRate'] for h in history_1d]
                rates_3d = [h['fundingRate'] for h in history_3d]
                
                # 计算P95年化收益率
                p95_yield_1d = calculate_p95_yield(rates_1d, rate['fundingIntervalHours'])
                p95_yield_3d = calculate_p95_yield(rates_3d, rate['fundingIntervalHours'])
                
                all_rates.append({
                    'exchange': rate['exchange'],
                    'token': token,
                    'funding_rate': rate['fundingRate'],
                    'interval_hours': rate['fundingIntervalHours'],
                    'current_yield': current_yield,
                    'p95_yield_1d': p95_yield_1d,
                    'p95_yield_3d': p95_yield_3d
                })
    
    # 按当前年化收益率排序
    all_rates.sort(key=lambda x: x['current_yield'], reverse=True)
    
    # 发送到企业微信群机器人
    send_to_wechat_robot(all_rates)

if __name__ == "__main__":
    main() 