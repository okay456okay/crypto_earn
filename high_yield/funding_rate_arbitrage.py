"""
资金费率套利监控脚本

该脚本用于监控各大交易所的资金费率，寻找高收益的套利机会。主要功能包括：
1. 从Binance、Bitget、Bybit、GateIO、OKX等交易所获取资金费率数据
2. 计算年化收益率，筛选出收益率超过阈值的交易对
3. 获取历史资金费率数据，计算P95值
4. 将符合条件的套利机会发送到企业微信群机器人

使用方法：
    python funding_rate_arbitrage.py

配置说明：
    在config.py中设置：
    - funding_rate_threshold: 资金费率年化收益率阈值（默认30%）
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
from config import webhook_url, funding_rate_threshold
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
    
    logger.info(f"开始获取{token}近{days}天的资金费率历史数据")
    history = []
    
    # 获取各交易所的资金费率历史
    try:
        binance_history = api.get_binance_future_funding_rate_history(token, start_time, end_time)
        logger.debug(f"Binance {token}历史数据: {len(binance_history)}条")
        history.extend(binance_history)
    except Exception as e:
        logger.error(f"获取Binance {token}历史数据失败: {str(e)}")
    
    try:
        bitget_history = api.get_bitget_futures_funding_rate_history(token, start_time, end_time)
        logger.debug(f"Bitget {token}历史数据: {len(bitget_history)}条")
        history.extend(bitget_history)
    except Exception as e:
        logger.error(f"获取Bitget {token}历史数据失败: {str(e)}")
    
    try:
        bybit_history = api.get_bybit_futures_funding_rate_history(token, start_time, end_time)
        logger.debug(f"Bybit {token}历史数据: {len(bybit_history)}条")
        history.extend(bybit_history)
    except Exception as e:
        logger.error(f"获取Bybit {token}历史数据失败: {str(e)}")
    
    try:
        gateio_history = api.get_gateio_futures_funding_rate_history(token, start_time, end_time)
        logger.debug(f"GateIO {token}历史数据: {len(gateio_history)}条")
        history.extend(gateio_history)
    except Exception as e:
        logger.error(f"获取GateIO {token}历史数据失败: {str(e)}")
    
    try:
        okx_history = api.get_okx_futures_funding_rate_history(token, start_time, end_time)
        logger.debug(f"OKX {token}历史数据: {len(okx_history)}条")
        history.extend(okx_history)
    except Exception as e:
        logger.error(f"获取OKX {token}历史数据失败: {str(e)}")
    
    logger.info(f"{token}近{days}天资金费率历史数据获取完成，共{len(history)}条")
    return history

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
        logger.info("开始发送消息到企业微信群机器人")
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            logger.info("消息发送成功")
        else:
            logger.error(f"发送消息失败: {response.text}")
    except Exception as e:
        logger.error(f"发送消息时出错: {str(e)}")

def main():
    logger.info("开始执行资金费率套利监控")
    api = ExchangeAPI()
    
    # 获取所有交易所的资金费率信息
    all_rates = []
    tokens = ['BTCUSDT', 'ETHUSDT']  # 这里可以扩展更多token
    
    for token in tokens:
        logger.info(f"开始获取{token}的资金费率信息")
        rates = api.get_funding_rate(token)
        for rate in rates:
            if not rate:  # 跳过空数据
                continue
            
            logger.debug(f"获取到{rate['exchange']} {token}资金费率: {rate['fundingRate']}%, 周期: {rate['fundingIntervalHours']}小时")
            
            # 计算当前年化收益率
            current_yield = calculate_annual_yield(rate['fundingRate'], rate['fundingIntervalHours'])
            logger.debug(f"{rate['exchange']} {token}当前年化收益率: {current_yield:.2f}%")
            
            # 如果当前年化收益率超过阈值，获取历史数据
            if current_yield >= funding_rate_threshold:
                logger.info(f"{rate['exchange']} {token}年化收益率{current_yield:.2f}%超过阈值{funding_rate_threshold}%，开始获取历史数据")
                
                # 获取1天和3天的历史数据
                history_1d = get_funding_rate_history(api, token, 1)
                history_3d = get_funding_rate_history(api, token, 3)
                
                # 提取资金费率列表
                rates_1d = [h['fundingRate'] for h in history_1d]
                rates_3d = [h['fundingRate'] for h in history_3d]
                
                # 计算P95年化收益率
                p95_yield_1d = calculate_p95_yield(rates_1d, rate['fundingIntervalHours'])
                p95_yield_3d = calculate_p95_yield(rates_3d, rate['fundingIntervalHours'])
                
                logger.info(f"{rate['exchange']} {token} P95年化收益率: 1天={p95_yield_1d:.2f}%, 3天={p95_yield_3d:.2f}%")
                
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
    logger.info("资金费率套利监控执行完成")

if __name__ == "__main__":
    main() 