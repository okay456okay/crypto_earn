#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Binance交易机会发现脚本

该脚本用于发现潜在的加密货币交易机会，通过分析Binance合约交易对的各项指标，
识别出可能即将启动的交易对。

主要功能：
1. 获取Binance所有合约交易对
2. 分析每个交易对的24小时数据
3. 根据预设条件筛选潜在机会
4. 将结果保存到文件并通过企业微信机器人发送通知

作者: Claude
创建时间: 2024-03-21
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from tools.logger import logger
from config import (
    binance_api_key, 
    binance_api_secret, 
    proxies, 
    project_root,
    BINANCE_OPPORTUNITY_FINDER
)
from binance.client import Client
from binance.exceptions import BinanceAPIException
import time
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import rcParams
import numpy as np
import base64
import hashlib

# 设置matplotlib中文字体
rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'DejaVu Sans']
rcParams['axes.unicode_minus'] = False

logger.setLevel(logging.DEBUG)

class BinanceOpportunityFinder:
    """Binance交易机会发现器"""
    
    def __init__(self, api_key: str = None, api_secret: str = None):
        """
        初始化Binance客户端
        
        Args:
            api_key: Binance API Key
            api_secret: Binance API Secret
        """
        # 配置代理
        self.client = Client(
            api_key, 
            api_secret,
            requests_params={
                'proxies': proxies
            }
        )
        self.ensure_directories()
        self.latest_file = os.path.join(project_root, 'trade/reports/binance_future_opportunies')

        # 生成带时间戳的文件名
        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        self.report_file = os.path.join(project_root, f'trade/reports/binance_future_opportunies_{timestamp}.log')

        # 生成当前时间戳
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 清空最新文件并写入时间戳
        with open(self.latest_file, 'w', encoding='utf-8') as f:
            f.write(f"运行时间: {current_time}\n\n")
            
        # 从配置文件加载阈值
        self.oi_price_market_ratio_threshold = BINANCE_OPPORTUNITY_FINDER['OI_PRICE_MARKET_RATIO_THRESHOLD']
        self.volume_market_ratio_threshold = BINANCE_OPPORTUNITY_FINDER['VOLUME_MARKET_RATIO_THRESHOLD']
        self.historical_change_threshold = BINANCE_OPPORTUNITY_FINDER['HISTORICAL_CHANGE_THRESHOLD']
        self.final_change_muliplier = BINANCE_OPPORTUNITY_FINDER['FINAL_CHANGE_MULTIPLIER']
        self.oi_new_high_threshold = BINANCE_OPPORTUNITY_FINDER['OI_NEW_HIGH_THRESHOLD']
        self.oi_absolute_change_threshold = BINANCE_OPPORTUNITY_FINDER['OI_ABSOLUTE_CHANGE_THRESHOLD']


    def ensure_directories(self):
        """确保必要的目录存在"""
        # os.makedirs('logs', exist_ok=True)
        os.makedirs(f'{project_root}/trade/reports', exist_ok=True)
        
    def get_test_symbol(self) -> List[str]:
        """
        获取测试用的交易对
        
        Returns:
            List[str]: 测试交易对列表
        """
        return ['ETHUSDT']
            
    def get_historical_data(self, symbol: str, start_time: str = '', end_time: str = '', create_graph: bool = False) -> Optional[Dict[str, Any]]:
        """
        获取交易对的历史数据
        
        Args:
            symbol: 交易对符号
            start_time: 开始时间，可以是时间戳或日期字符串(如'2025-05-20 11:22:11')，默认为空(自动设置为end_time-24小时)
            end_time: 结束时间，可以是时间戳或日期字符串(如'2025-05-20 11:22:11')，默认为空(当前时间)
            create_graph: 是否创建图表，默认为False
            
        Returns:
            Dict: 包含价格、持仓量、交易量等数据的字典
        """
        try:
            logger.info(f"开始获取{symbol}的历史数据...")
            
            # 处理结束时间
            if end_time == '':
                end_timestamp = int(time.time() * 1000)  # 当前时间戳(毫秒)
            else:
                try:
                    # 尝试将字符串转换为时间戳
                    if isinstance(end_time, str) and len(end_time) > 10:
                        # 日期字符串格式
                        end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
                        end_timestamp = int(end_dt.timestamp() * 1000)
                    else:
                        # 假设是时间戳
                        end_timestamp = int(end_time)
                except ValueError:
                    logger.error(f"无效的结束时间格式: {end_time}")
                    return None
            
            # 处理开始时间
            if start_time == '':
                start_timestamp = end_timestamp - 24 * 60 * 60 * 1000  # 24小时前
            else:
                try:
                    if isinstance(start_time, str) and len(start_time) > 10:
                        # 日期字符串格式
                        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                        start_timestamp = int(start_dt.timestamp() * 1000)
                    else:
                        # 假设是时间戳
                        start_timestamp = int(start_time)
                except ValueError:
                    logger.error(f"无效的开始时间格式: {start_time}")
                    return None
            
            # 确保开始时间小于结束时间
            if start_timestamp >= end_timestamp:
                logger.error(f"开始时间必须小于结束时间: start={start_timestamp}, end={end_timestamp}")
                return None
            
            # 计算时间间隔（5分钟 = 5*60*1000毫秒）
            interval_ms = 5 * 60 * 1000
            # 计算limit（数据点数量）
            limit = min(int((end_timestamp - start_timestamp) / interval_ms) + 1, 1500)  # Binance API限制最大1500
            
            logger.debug(f"时间范围: {datetime.fromtimestamp(start_timestamp/1000)} 到 {datetime.fromtimestamp(end_timestamp/1000)}")
            logger.debug(f"计算的limit: {limit}")
            
            # 获取K线数据 - 使用开始和结束时间
            logger.debug(f"请求{symbol}的K线数据...")
            klines = self.client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_5MINUTE,
                startTime=start_timestamp,
                endTime=end_timestamp,
                limit=limit
            )
            # logger.debug(f"{symbol} K线数据: {json.dumps(klines, indent=2)}")
            
            # 获取24小时统计数据
            logger.debug(f"请求{symbol}的24小时统计数据...")
            # ticker = self.client.futures_ticker(symbol=symbol)
            # logger.debug(f"{symbol} 24小时统计数据: {json.dumps(ticker, indent=2)}")
            
            # 获取合约持仓量数据
            logger.debug(f"请求{symbol}的合约持仓量数据...")
            open_interest = self.client.futures_open_interest(
                symbol=symbol,
                timestamp=end_timestamp
            )
            # logger.debug(f"{symbol} 合约持仓量数据: {json.dumps(open_interest, indent=2)}")
            
            # 获取合约持仓量历史 - 使用开始和结束时间
            logger.debug(f"请求{symbol}的合约持仓量历史数据...")
            open_interest_hist = self.client.futures_open_interest_hist(
                symbol=symbol,
                period='5m',
                startTime=start_timestamp,
                endTime=end_timestamp,
                limit=limit
            )
            
            # 获取币种信息
            logger.debug(f"请求{symbol}的币种信息...")
            base_asset = symbol.replace('USDT', '')
            # 使用futures_exchange_info获取币种信息
            # exchange_info = self.client.futures_exchange_info()
            # asset_info = None
            # for symbol_info in exchange_info['symbols']:
            #     if symbol_info['symbol'] == symbol:
            #         asset_info = {
            #             'symbol': symbol,
            #             'baseAsset': base_asset,
            #             'status': symbol_info['status'],
            #             'contractType': symbol_info['contractType']
            #         }
            #         break
            #
            # if not asset_info:
            #     logger.warning(f"无法获取{symbol}的币种信息")
            #     return None
            
            data = {
                'klines': klines,
                # 'ticker': ticker,
                'open_interest': open_interest,
                'open_interest_hist': open_interest_hist,
                # 'asset_info': asset_info,
                'start_timestamp': start_timestamp,
                'end_timestamp': end_timestamp
            }
            
            # 如果需要创建图表
            if create_graph:
                chart_path = self.create_detailed_charts(symbol, klines, open_interest_hist)
                data['chart_path'] = chart_path
                
            logger.info(f"成功获取{symbol}的所有历史数据")
            return data
            
        except (BinanceAPIException, Exception) as e:
            logger.error(f"获取{symbol}历史数据失败: {str(e)}")
            return None

    def create_detailed_charts(self, symbol: str, klines: List, open_interest_hist: List) -> str:
        """
        创建详细的价格、持仓量和交易量图表
        
        Args:
            symbol: 交易对符号
            klines: K线数据
            open_interest_hist: 持仓量历史数据
            
        Returns:
            str: 图片文件路径
        """
        try:
            logger.info(f"开始绘制{symbol}的详细图表...")
            
            # 确保图片目录存在
            charts_dir = os.path.join(project_root, 'trade/charts')
            os.makedirs(charts_dir, exist_ok=True)
            
            # 生成文件名
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            filename = f"{symbol.lower()}_detailed_{timestamp}.png"
            filepath = os.path.join(charts_dir, filename)
            
            # 提取价格数据
            timestamps = []
            prices = []
            volumes = []
            for kline in klines:
                timestamp = datetime.fromtimestamp(int(kline[0]) / 1000)
                timestamps.append(timestamp)
                prices.append(float(kline[4]))  # 收盘价
                volumes.append(float(kline[5]))  # 成交量
            
            # 提取持仓量数据
            oi_timestamps = []
            oi_values = []
            for oi_data in open_interest_hist:
                timestamp = datetime.fromtimestamp(int(oi_data['timestamp']) / 1000)
                oi_timestamps.append(timestamp)
                oi_values.append(float(oi_data['sumOpenInterest']))
            
            # 创建三个子图
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
            
            # 绘制价格趋势
            ax1.plot(timestamps, prices, 'b-', linewidth=2, label='Price')
            ax1.set_ylabel('Price (USDT)', color='b', fontsize=12)
            ax1.tick_params(axis='y', labelcolor='b')
            ax1.grid(True, alpha=0.3)
            ax1.set_title(f'{symbol} - Price, Volume and Open Interest Analysis', fontsize=16, fontweight='bold')
            ax1.legend(loc='upper left')
            
            # 绘制交易量
            ax2.bar(timestamps, volumes, width=0.003, color='green', alpha=0.7, label='Volume')
            ax2.set_ylabel('Volume', color='green', fontsize=12)
            ax2.tick_params(axis='y', labelcolor='green')
            ax2.grid(True, alpha=0.3)
            ax2.legend(loc='upper left')
            
            # 绘制持仓量趋势
            ax3.plot(oi_timestamps, oi_values, 'r-', linewidth=2, label='Open Interest')
            ax3.set_ylabel('Open Interest', color='r', fontsize=12)
            ax3.tick_params(axis='y', labelcolor='r')
            ax3.grid(True, alpha=0.3)
            ax3.set_xlabel('Time', fontsize=12)
            ax3.legend(loc='upper left')
            
            # 格式化x轴时间显示
            ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
            ax3.xaxis.set_major_locator(mdates.HourLocator(interval=4))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)
            
            # 调整布局
            plt.tight_layout()
            
            # 保存图片
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"成功保存{symbol}详细图表到: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"绘制{symbol}详细图表时发生错误: {str(e)}")
            return ""

    def get_market_cap(self, symbol: str) -> Optional[Dict[str, float]]:
        """
        获取币种市值和成交量/市值比
        
        Args:
            symbol: 交易对符号
            
        Returns:
            Dict: 包含市值和成交量/市值比的字典，如果获取失败则返回None
        """
        try:
            base_asset = symbol.replace('USDT', '')
            url = f"https://www.binance.com/bapi/apex/v1/friendly/apex/marketing/web/token-info?symbol={base_asset}"
            response = requests.get(url, proxies=proxies)
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data', {}).get('mc'):
                    market_cap = float(data['data']['mc'])
                    volume_market_ratio = float(data['data']['vpm'])
                    logger.debug(f"{symbol} 市值: {market_cap:,.2f} USDT")
                    logger.debug(f"{symbol} 成交量/市值比: {volume_market_ratio:.4f}")
                    return {
                        'market_cap': market_cap,
                        'volume_market_ratio': volume_market_ratio
                    }
            logger.warning(f"获取{symbol}市值失败: {response.text}")
            return None
        except Exception as e:
            logger.error(f"获取{symbol}市值时发生错误: {str(e)}")
            return None

    def plot_trends(self, symbol: str, klines: List, open_interest_hist: List) -> str:
        """
        绘制持仓量和价格趋势图
        
        Args:
            symbol: 交易对符号
            klines: K线数据
            open_interest_hist: 持仓量历史数据
            
        Returns:
            str: 图片文件路径
        """
        try:
            logger.info(f"开始绘制{symbol}的趋势图...")
            
            # 确保图片目录存在
            charts_dir = os.path.join(project_root, 'trade/charts')
            os.makedirs(charts_dir, exist_ok=True)
            
            # 生成文件名
            timestamp = datetime.now().strftime('%Y%m%d%H%M')
            filename = f"{symbol.lower()}_{timestamp}.png"
            filepath = os.path.join(charts_dir, filename)
            
            # 提取价格数据
            timestamps = []
            prices = []
            for kline in klines:
                timestamp = datetime.fromtimestamp(int(kline[0]) / 1000)
                timestamps.append(timestamp)
                prices.append(float(kline[4]))  # 收盘价
            
            # 提取持仓量数据
            oi_timestamps = []
            oi_values = []
            for oi_data in open_interest_hist:
                timestamp = datetime.fromtimestamp(int(oi_data['timestamp']) / 1000)
                oi_timestamps.append(timestamp)
                oi_values.append(float(oi_data['sumOpenInterest']))
            
            # 创建图表
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
            
            # 绘制价格趋势
            ax1.plot(timestamps, prices, 'b-', linewidth=2, label='Price')
            ax1.set_ylabel('Price (USDT)', color='b')
            ax1.tick_params(axis='y', labelcolor='b')
            ax1.grid(True, alpha=0.3)
            ax1.set_title(f'{symbol} Price and Open Interest Trends', fontsize=14, fontweight='bold')
            
            # 绘制持仓量趋势
            ax2.plot(oi_timestamps, oi_values, 'r-', linewidth=2, label='Open Interest')
            ax2.set_ylabel('Open Interest', color='r')
            ax2.tick_params(axis='y', labelcolor='r')
            ax2.grid(True, alpha=0.3)
            ax2.set_xlabel('Time')
            
            # 格式化x轴时间显示
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax2.xaxis.set_major_locator(mdates.MinuteLocator(interval=30))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            # 调整布局
            plt.tight_layout()
            
            # 保存图片
            plt.savefig(filepath, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"成功保存{symbol}趋势图到: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"绘制{symbol}趋势图时发生错误: {str(e)}")
            return ""

    def format_opportunity_report(self, symbol: str, conditions: Dict[str, bool], 
                                oi_price_market_ratio: float, volume_market_ratio: float,
                                historical_price_changes: List[float], historical_oi_changes: List[float],
                                final_oi_change: float, final_oi_change_threshold: float, 
                                matched_strategies: List[str], chart_path: str = "") -> str:
        """
        格式化交易机会报告
        
        Args:
            symbol: 交易对符号
            conditions: 条件检查结果
            oi_price_market_ratio: 合约持仓金额/市值
            volume_market_ratio: 近24小时成交量/市值
            historical_price_changes: 历史价格变化率列表
            historical_oi_changes: 历史持仓量变化率列表
            final_oi_change: 最终持仓量变化率
            final_oi_change_threshold: 最终持仓量变化阈值
            matched_strategies: 命中的策略列表
            chart_path: 图表文件路径
            
        Returns:
            str: 格式化后的报告
        """
        # 计算历史持仓量变化率的最大值
        max_oi_change = max(abs(change) for change in historical_oi_changes[:-1]) * 100
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        report = f"{symbol} - {current_time}\n"
        report += f"交易活跃度:合约持仓金额/市值 {oi_price_market_ratio:.2f} > {self.oi_price_market_ratio_threshold}: {'✓' if conditions[f'交易活跃度:合约持仓金额/市值 > {self.oi_price_market_ratio_threshold}'] else '✗'}\n"
        report += f"交易活跃度:近24小时成交量/市值 {volume_market_ratio:.2f} > {self.volume_market_ratio_threshold}: {'✓' if conditions[f'交易活跃度:近24小时成交量/市值 > {self.volume_market_ratio_threshold}'] else '✗'}\n"
        report += f"拉盘信号:历史持仓量变化率 {max_oi_change:.1f}% < {self.historical_change_threshold*100}%: {'✓' if conditions[f'拉盘信号:历史持仓量变化率 < {self.historical_change_threshold*100}%'] else '✗'}\n"
        report += f"拉盘信号:最终持仓量变化率 {final_oi_change*100:.1f}% > {final_oi_change_threshold*100:.1f}%: {'✓' if conditions[f'拉盘信号:最终持仓量变化率 > {final_oi_change_threshold*100:.1f}%'] else '✗'}\n"
        
        # 添加新的策略条件显示
        report += f"拉盘信号:最终持仓量创新高(>{self.oi_new_high_threshold*100:.0f}%): {'✓' if conditions[f'拉盘信号:最终持仓量创新高(>{self.oi_new_high_threshold*100:.0f}%)'] else '✗'}\n"
        report += f"拉盘信号:最终持仓量变化率超过绝对阈值(>{self.oi_absolute_change_threshold*100:.0f}%): {'✓' if conditions[f'拉盘信号:最终持仓量变化率超过绝对阈值(>{self.oi_absolute_change_threshold*100:.0f}%)'] else '✗'}\n"
        
        # 显示命中的策略
        if matched_strategies:
            report += f"🎯 命中策略: {', '.join(matched_strategies)}\n"
        
        if chart_path:
            report += f"趋势图路径: {chart_path}\n"
        report += "\n"
        return report
        
    def save_opportunity(self, opportunity: Dict[str, Any], conditions: Dict[str, bool],
                        historical_price_changes: List[float], historical_oi_changes: List[float],
                        chart_path: str = ""):
        """
        保存交易机会到文件
        
        Args:
            opportunity: 交易机会数据
            conditions: 条件检查结果
            historical_price_changes: 历史价格变化率列表
            historical_oi_changes: 历史持仓量变化率列表
            chart_path: 图表文件路径
        """
        try:
            logger.info(f"开始保存{opportunity['symbol']}的交易机会...")
            
            # 生成报告内容
            report = self.format_opportunity_report(
                opportunity['symbol'],
                conditions,
                opportunity['oi_price_market_ratio'],
                opportunity['volume_market_ratio'],
                historical_price_changes,
                historical_oi_changes,
                opportunity['oi_change'],
                opportunity['final_oi_change_threshold'],
                opportunity['matched_strategies'],
                chart_path
            )
            
            # 保存到带时间戳的文件
            with open(self.report_file, 'a', encoding='utf-8') as f:
                f.write(report)
                
            # 同时保存到最新文件
            with open(self.latest_file, 'a', encoding='utf-8') as f:
                f.write(report)
                
            logger.info(f"成功保存{opportunity['symbol']}的交易机会")
            
        except Exception as e:
            logger.error(f"保存交易机会时发生错误: {str(e)}")
            
    def send_wecom_notification(self, opportunity: Dict[str, Any], chart_path: str = ""):
        """
        发送企业微信通知
        
        Args:
            opportunity: 交易机会数据
            chart_path: 图表文件路径
        """
        try:
            webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=43c4c655-b144-4e1f-b054-4b3a9e2caf26"
            
            # 构建通知消息
            symbol = opportunity['symbol']
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 文本消息内容
            message_text = f"""🚀 发现交易机会 - {symbol}
            
⏰ 时间: {current_time}
💰 当前价格: {opportunity['current_price']:,.2f} USDT
📈 价格变化: {opportunity['price_change']*100:+.2f}%
📊 持仓量变化: {opportunity['oi_change']*100:+.2f}%
💎 合约持仓金额/市值: {opportunity['oi_price_market_ratio']:.4f}
🔥 成交量/市值比: {opportunity['volume_market_ratio']:.4f}
💵 市值: {opportunity['market_cap']:,.0f} USDT
🎯 命中策略: {', '.join(opportunity['matched_strategies'])}

📊 趋势图已生成，请查看附件分析详情。"""

            # 发送文本消息
            text_payload = {
                "msgtype": "text",
                "text": {
                    "content": message_text
                }
            }
            
            logger.info(f"开始发送{symbol}的企业微信通知...")
            
            # 发送文本消息
            response = requests.post(
                webhook_url, 
                json=text_payload,
                proxies=proxies,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    logger.info(f"✓ 成功发送{symbol}的文本通知到企业微信")
                else:
                    logger.error(f"✗ 发送{symbol}文本通知失败: {result.get('errmsg', '未知错误')}")
            else:
                logger.error(f"✗ 发送{symbol}文本通知HTTP错误: {response.status_code}")
                
            # 如果有图片，尝试发送图片消息
            if chart_path and os.path.exists(chart_path):
                try:
                    # 读取图片文件并转换为base64
                    with open(chart_path, 'rb') as f:
                        image_data = f.read()
                        image_base64 = base64.b64encode(image_data).decode('utf-8')
                    
                    # 计算图片MD5
                    image_md5 = hashlib.md5(image_data).hexdigest()
                    
                    # 发送图片消息
                    image_payload = {
                        "msgtype": "image",
                        "image": {
                            "base64": image_base64,
                            "md5": image_md5
                        }
                    }
                    
                    response = requests.post(
                        webhook_url,
                        json=image_payload,
                        proxies=proxies,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        if result.get('errcode') == 0:
                            logger.info(f"✓ 成功发送{symbol}的图片到企业微信")
                        else:
                            logger.error(f"✗ 发送{symbol}图片失败: {result.get('errmsg', '未知错误')}")
                    else:
                        logger.error(f"✗ 发送{symbol}图片HTTP错误: {response.status_code}")
                        
                except Exception as e:
                    logger.error(f"✗ 处理{symbol}图片时发生错误: {str(e)}")
                    
        except Exception as e:
            logger.error(f"发送{symbol}企业微信通知时发生错误: {str(e)}")
            
    def get_all_symbols(self) -> List[str]:
        """
        获取所有合约交易对
        
        Returns:
            List[str]: 交易对列表
        """
        try:
            logger.info("开始获取所有交易对...")
            
            # 获取合约交易对
            futures_symbols = []
            futures_exchange_info = self.client.futures_exchange_info()
            for symbol_info in futures_exchange_info['symbols']:
                if (symbol_info['status'] == 'TRADING' and 
                    symbol_info['quoteAsset'] == 'USDT' and 
                    symbol_info['contractType'] == 'PERPETUAL'):
                    futures_symbols.append(symbol_info['symbol'])
            logger.info(f"获取到{len(futures_symbols)}个合约交易对")
            
            return futures_symbols
            
        except Exception as e:
            logger.error(f"获取交易对列表失败: {str(e)}")
            return []
            
    def run(self):
        """运行交易机会发现程序"""
        try:
            logger.info("开始运行交易机会发现程序...")
            
            # 获取所有交易对
            symbols = self.get_all_symbols()
            if not symbols:
                logger.error("未获取到任何交易对，程序退出")
                return

            logger.info(f"开始分析{len(symbols)}个交易对, 获取到的交易对如下：{symbols}")
            
            for symbol in symbols:
                logger.info(f"开始分析交易对: {symbol}")
                
                # 获取历史数据
                data = self.get_historical_data(symbol)
                if not data:
                    logger.warning(f"跳过{symbol}，无法获取历史数据")
                    continue
                    
                # 分析机会
                result = self.analyze_opportunity(symbol, data)
                if result:
                    opportunity, conditions, historical_price_changes, historical_oi_changes = result
                    # 只对符合条件的交易对生成趋势图
                    chart_path = self.plot_trends(symbol, data['klines'], data['open_interest_hist'])
                    # 保存机会
                    self.save_opportunity(opportunity, conditions, historical_price_changes, historical_oi_changes, chart_path)
                    # 发送通知
                    self.send_wecom_notification(opportunity, chart_path)
                    
                # 避免触发频率限制
                time.sleep(0.01)
                
            logger.info("交易机会发现程序运行完成")
                
        except Exception as e:
            logger.error(f"运行程序时发生错误: {str(e)}")
            
    def analyze_opportunity(self, symbol: str, data: Dict[str, Any]) -> Optional[Tuple[Dict[str, Any], Dict[str, bool], List[float], List[float]]]:
        """
        分析交易机会
        
        Args:
            symbol: 交易对符号
            data: 历史数据
            
        Returns:
            Tuple: (交易机会数据, 条件检查结果, 历史价格变化率列表, 历史持仓量变化率列表)
        """
        try:
            logger.info(f"开始分析{symbol}的交易机会...")
            
            # 提取数据
            klines = data['klines']
            # ticker = data['ticker']
            open_interest = float(data['open_interest']['openInterest'])
            open_interest_hist = data['open_interest_hist']
            # asset_info = data['asset_info']
            
            # 获取市值和成交量/市值比
            market_data = self.get_market_cap(symbol)
            market_cap = market_data['market_cap']
            volume_market_ratio = market_data['volume_market_ratio']
            # if market_data is None:
            #     # 如果无法获取市值数据，使用24小时成交额作为替代指标
            #     volume_24h = float(ticker['quoteVolume'])
            #     market_cap = volume_24h
            #     volume_market_ratio = 1.0  # 使用成交额作为市值时，比值为1
            #     logger.debug(f"{symbol} 使用24小时成交额作为市值参考: {market_cap:,.2f} USDT")
            # else:
            #     market_cap = market_data['market_cap']
            #     volume_market_ratio = market_data['volume_market_ratio']
            
            # 计算当前价格
            current_price = float(klines[-1][4])  # 收盘价
            
            # 计算历史持仓量变化率
            historical_oi_changes = []
            for i in range(len(open_interest_hist) - 1):
                current = float(open_interest_hist[i]['sumOpenInterest'])
                next_oi = float(open_interest_hist[i + 1]['sumOpenInterest'])
                change = (next_oi - current) / current
                historical_oi_changes.append(change)
            
            # 计算最终持仓量变化率（最后一个时点）
            final_oi_change = (float(open_interest_hist[-1]['sumOpenInterest']) - float(open_interest_hist[-2]['sumOpenInterest'])) / float(open_interest_hist[-2]['sumOpenInterest'])
            
            # 动态计算final_oi_change_threshold：历史持仓量变化率最大绝对值的2倍
            max_historical_oi_change = max(abs(change) for change in historical_oi_changes[:-1]) if len(historical_oi_changes) > 1 else 0.01
            final_oi_change_threshold = max_historical_oi_change * self.final_change_muliplier
            
            # 新增条件1：检查最终持仓量是否创新高并且比历史最高点高出指定百分比
            current_oi = float(open_interest_hist[-1]['sumOpenInterest'])
            historical_oi_values = [float(oi_data['sumOpenInterest']) for oi_data in open_interest_hist[:-1]]
            max_historical_oi = max(historical_oi_values) if historical_oi_values else 0
            is_new_high = current_oi > max_historical_oi
            oi_new_high_ratio = (current_oi - max_historical_oi) / max_historical_oi if max_historical_oi > 0 else 0
            is_oi_new_high = is_new_high and oi_new_high_ratio > self.oi_new_high_threshold
            
            # 新增条件2：检查最终持仓量变化率是否超过绝对阈值
            is_oi_absolute_change = abs(final_oi_change) > self.oi_absolute_change_threshold
            
            # 检查历史变化率是否都在阈值以内
            historical_changes_ok = all(abs(change) <= self.historical_change_threshold for change in historical_oi_changes[:-1])
            
            # 为保持函数签名一致性，提供空的历史价格变化率列表
            historical_price_changes = []
            
            # 计算合约持仓金额/市值比
            oi_price_market_ratio = (open_interest * current_price) / market_cap
            
            logger.debug(f"{symbol} 分析指标:")
            logger.debug(f"  当前价格: {current_price:,.2f} USDT")
            logger.debug(f"  当前持仓量: {open_interest:,.2f} {symbol.replace('USDT', '')}")
            logger.debug(f"  历史持仓量变化率: {[f'{change:.2%}' for change in historical_oi_changes]}")
            logger.debug(f"  最终持仓量变化率: {final_oi_change:.2%}")
            logger.debug(f"  合约持仓金额/市值: {oi_price_market_ratio:.4f}")
            logger.debug(f"  近24小时成交量/市值: {volume_market_ratio:.4f}")
            logger.debug(f"  动态计算的最终持仓量变化阈值: {final_oi_change_threshold:.2%}")
            logger.debug(f"  最终持仓量: {current_oi:,.2f}")
            logger.debug(f"  历史最高持仓量: {max_historical_oi:,.2f}")
            logger.debug(f"  新高比例: {oi_new_high_ratio:.2%}")
            
            # 检查条件
            conditions = {
                f'交易活跃度:合约持仓金额/市值 > {self.oi_price_market_ratio_threshold}': oi_price_market_ratio > self.oi_price_market_ratio_threshold,
                f'交易活跃度:近24小时成交量/市值 > {self.volume_market_ratio_threshold}': volume_market_ratio > self.volume_market_ratio_threshold,
                f'拉盘信号:历史持仓量变化率 < {self.historical_change_threshold*100}%': historical_changes_ok,
                f'拉盘信号:最终持仓量变化率 > {final_oi_change_threshold*100:.1f}%': final_oi_change > final_oi_change_threshold,
                f'拉盘信号:最终持仓量创新高(>{self.oi_new_high_threshold*100:.0f}%)': is_oi_new_high,
                f'拉盘信号:最终持仓量变化率超过绝对阈值(>{self.oi_absolute_change_threshold*100:.0f}%)': is_oi_absolute_change
            }
            
            # 基础条件（前两个条件必须满足）
            basic_conditions = [
                conditions[f'交易活跃度:合约持仓金额/市值 > {self.oi_price_market_ratio_threshold}'],
                conditions[f'交易活跃度:近24小时成交量/市值 > {self.volume_market_ratio_threshold}'],
                conditions[f'拉盘信号:历史持仓量变化率 < {self.historical_change_threshold*100}%']
            ]
            
            # 策略条件（满足其中任意一个即可）
            strategy_conditions = {
                '策略1-动态阈值': conditions[f'拉盘信号:最终持仓量变化率 > {final_oi_change_threshold*100:.1f}%'],
                '策略2-创新高': conditions[f'拉盘信号:最终持仓量创新高(>{self.oi_new_high_threshold*100:.0f}%)'],
                '策略3-绝对变化': conditions[f'拉盘信号:最终持仓量变化率超过绝对阈值(>{self.oi_absolute_change_threshold*100:.0f}%)']
            }
            
            # 获取满足的策略
            matched_strategies = [strategy for strategy, condition in strategy_conditions.items() if condition]
            
            # 判断是否符合总体条件：基础条件都满足 且 至少满足一个策略条件
            is_opportunity = all(basic_conditions) and len(matched_strategies) > 0
            
            logger.info(f"{symbol} 条件检查结果:")
            for condition, result in conditions.items():
                logger.info(f"{symbol}  {condition}: {'✓' if result else '✗'}")
            
            if is_opportunity:
                logger.info(f"{symbol} 符合交易机会条件!")
                logger.info(f"{symbol} 命中策略: {', '.join(matched_strategies)}")
                return (
                    {
                        'symbol': symbol,
                        'current_price': current_price,
                        'current_oi': current_oi,
                        'oi_change': final_oi_change,
                        'price_change': (float(klines[-1][4]) - float(klines[-2][4])) / float(klines[-2][4]),
                        'oi_price_market_ratio': oi_price_market_ratio,
                        'volume_market_ratio': volume_market_ratio,
                        'market_cap': market_cap,
                        'timestamp': datetime.now().isoformat(),
                        'final_oi_change_threshold': final_oi_change_threshold,
                        'matched_strategies': matched_strategies,
                        'max_historical_oi': max_historical_oi,
                        'oi_new_high_ratio': oi_new_high_ratio,
                        'is_oi_new_high': is_oi_new_high,
                        'is_oi_absolute_change': is_oi_absolute_change
                    },
                    conditions,
                    historical_price_changes,
                    historical_oi_changes
                )
            
            logger.info(f"{symbol} 不符合交易机会条件")
            return None
            
        except Exception as e:
            logger.error(f"分析{symbol}机会时发生错误: {str(e)}")
            return None
            
def main():
    """主函数"""
    try:
        logger.info("程序启动...")
        
        # 从环境变量或配置文件获取API密钥
        api_key = binance_api_key
        api_secret = binance_api_secret
        
        if not api_key or not api_secret:
            logger.error("未设置BINANCE_API_KEY或BINANCE_API_SECRET环境变量")
            return
            
        logger.info("初始化交易机会发现器...")
        finder = BinanceOpportunityFinder(api_key, api_secret)
        """
            'BIDUSDT',
            start_time='2025-06-02 02:30:00',
            end_time='2025-06-03 14:30:00',
            'ZEREBROUSDT',
            start_time='2025-06-01 01:30:32',
            end_time='2025-06-02 13:40:32',
            'LEVERUSDT',
            start_time='2025-06-01 01:06:36',
            end_time='2025-06-02 13:06:36',
            'PUMPUSDT',
            start_time='2025-05-28 22:09:23',
            end_time='2025-05-30 10:09:23',
            'BMTUSDT',
            start_time='2025-05-28 18:09:01',
            end_time='2025-05-30 06:09:01',
        """
        # 测试新的策略功能
        logger.info("测试新的策略功能...")
        logger.info(f"已配置策略阈值:")
        logger.info(f"  OI新高阈值: {finder.oi_new_high_threshold*100:.0f}%")
        logger.info(f"  OI绝对变化阈值: {finder.oi_absolute_change_threshold*100:.0f}%")
        logger.info(f"  动态倍数: {finder.final_change_muliplier}")
        
        # 简单功能测试（测试一个交易对）
        # test_data = finder.get_historical_data('ETHUSDT', create_graph=False)
        # if test_data:
        #     result = finder.analyze_opportunity('ETHUSDT', test_data)
        #     if result:
        #         logger.info("✓ 策略测试成功")
        #     else:
        #         logger.info("✓ 策略测试完成（未命中条件）")
        # else:
        #     logger.warning("✗ 策略测试失败（无法获取数据）")
        
        # data = finder.get_historical_data(
        #     'BIDUSDT',
        #     start_time='2025-06-02 02:38:49',
        #     end_time='2025-06-03 02:30:49',
        #     create_graph=False
        # )
        # result = finder.analyze_opportunity('BIDUSDT', data)
        # # # print(result)
        # exit()
        logger.info("开始运行交易机会发现器...")
        finder.run()
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        
if __name__ == '__main__':
    main() 