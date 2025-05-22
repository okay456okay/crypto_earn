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
from config import binance_api_key, binance_api_secret, proxies
from binance.client import Client
from binance.exceptions import BinanceAPIException
import time
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
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
        self.opportunities_file = 'data/binance_opportunities.json'
        self.ensure_directories()
        
    def ensure_directories(self):
        """确保必要的目录存在"""
        os.makedirs('logs', exist_ok=True)
        os.makedirs('data', exist_ok=True)
        
    def get_test_symbol(self) -> List[str]:
        """
        获取测试用的交易对
        
        Returns:
            List[str]: 测试交易对列表
        """
        return ['ETHUSDT']
            
    def get_historical_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取交易对的历史数据
        
        Args:
            symbol: 交易对符号
            
        Returns:
            Dict: 包含价格、持仓量、交易量等数据的字典
        """
        try:
            logger.info(f"开始获取{symbol}的历史数据...")
            
            # 获取K线数据
            logger.debug(f"请求{symbol}的K线数据...")
            klines = self.client.futures_klines(
                symbol=symbol,
                interval=Client.KLINE_INTERVAL_1HOUR,
                limit=24
            )
            logger.debug(f"{symbol} K线数据: {json.dumps(klines, indent=2)}")
            
            # 获取24小时统计数据
            logger.debug(f"请求{symbol}的24小时统计数据...")
            ticker = self.client.futures_ticker(symbol=symbol)
            logger.debug(f"{symbol} 24小时统计数据: {json.dumps(ticker, indent=2)}")
            
            # 获取合约持仓量数据
            logger.debug(f"请求{symbol}的合约持仓量数据...")
            open_interest = self.client.futures_open_interest(symbol=symbol)
            logger.debug(f"{symbol} 合约持仓量数据: {json.dumps(open_interest, indent=2)}")
            
            # 获取合约持仓量历史
            logger.debug(f"请求{symbol}的合约持仓量历史数据...")
            open_interest_hist = self.client.futures_open_interest_hist(
                symbol=symbol,
                period='1h',
                limit=24
            )
            logger.debug(f"{symbol} 合约持仓量历史数据: {json.dumps(open_interest_hist, indent=2)}")
            
            # 获取币种信息
            logger.debug(f"请求{symbol}的币种信息...")
            base_asset = symbol.replace('USDT', '')
            # 使用futures_exchange_info获取币种信息
            exchange_info = self.client.futures_exchange_info()
            asset_info = None
            for symbol_info in exchange_info['symbols']:
                if symbol_info['symbol'] == symbol:
                    asset_info = {
                        'symbol': symbol,
                        'baseAsset': base_asset,
                        'status': symbol_info['status'],
                        'contractType': symbol_info['contractType']
                    }
                    break
            
            if not asset_info:
                logger.warning(f"无法获取{symbol}的币种信息")
                return None
                
            data = {
                'klines': klines,
                'ticker': ticker,
                'open_interest': open_interest,
                'open_interest_hist': open_interest_hist,
                'asset_info': asset_info
            }
            logger.info(f"成功获取{symbol}的所有历史数据")
            return data
            
        except (BinanceAPIException, Exception) as e:
            logger.error(f"获取{symbol}历史数据失败: {str(e)}")
            return None
            
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

    def analyze_opportunity(self, symbol: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        分析交易机会
        
        Args:
            symbol: 交易对符号
            data: 历史数据
            
        Returns:
            Dict: 分析结果，如果符合条件则返回详细信息，否则返回None
        """
        try:
            logger.info(f"开始分析{symbol}的交易机会...")
            
            # 提取数据
            klines = data['klines']
            ticker = data['ticker']
            open_interest = float(data['open_interest']['openInterest'])
            open_interest_hist = data['open_interest_hist']
            asset_info = data['asset_info']
            
            # 获取市值和成交量/市值比
            market_data = self.get_market_cap(symbol)
            if market_data is None:
                # 如果无法获取市值数据，使用24小时成交额作为替代指标
                volume_24h = float(ticker['quoteVolume'])
                market_cap = volume_24h
                volume_market_ratio = 1.0  # 使用成交额作为市值时，比值为1
                logger.debug(f"{symbol} 使用24小时成交额作为市值参考: {market_cap:,.2f} USDT")
            else:
                market_cap = market_data['market_cap']
                volume_market_ratio = market_data['volume_market_ratio']
            
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
            
            # 检查历史变化率是否都在5%以内
            historical_changes_ok = all(abs(change) <= 0.05 for change in historical_oi_changes[:-1])
            
            # 计算历史价格变化率
            historical_price_changes = []
            for i in range(len(klines) - 1):
                current_price = float(klines[i][4])  # 收盘价
                next_price = float(klines[i + 1][4])
                change = (next_price - current_price) / current_price
                historical_price_changes.append(change)
            
            # 计算最终价格变化率（最后一个时点）
            final_price_change = (float(klines[-1][4]) - float(klines[-2][4])) / float(klines[-2][4])
            
            # 检查历史价格变化率是否都在5%以内
            historical_price_changes_ok = all(abs(change) <= 0.05 for change in historical_price_changes[:-1])
            
            # 计算合约持仓金额/市值比
            oi_price_market_ratio = (open_interest * current_price) / market_cap
            
            logger.debug(f"{symbol} 分析指标:")
            logger.debug(f"  当前价格: {current_price:,.2f} USDT")
            logger.debug(f"  当前持仓量: {open_interest:,.2f} {symbol.replace('USDT', '')}")
            logger.debug(f"  历史持仓量变化率: {[f'{change:.2%}' for change in historical_oi_changes]}")
            logger.debug(f"  最终持仓量变化率: {final_oi_change:.2%}")
            logger.debug(f"  历史价格变化率: {[f'{change:.2%}' for change in historical_price_changes]}")
            logger.debug(f"  最终价格变化率: {final_price_change:.2%}")
            logger.debug(f"  合约持仓金额/市值: {oi_price_market_ratio:.4f}")
            logger.debug(f"  近24小时成交量/市值: {volume_market_ratio:.4f}")
            
            # 检查条件
            conditions = {
                '交易活跃度:合约持仓金额/市值 > 0.2': oi_price_market_ratio > 0.2,
                '交易活跃度:近24小时成交量/市值 > 0.05': volume_market_ratio > 0.05,
                '拉盘信号:历史价格变化率 < 5%': historical_price_changes_ok,
                '拉盘信号:历史持仓量变化率 < 5%': historical_changes_ok,
                '拉盘信号:最终持仓量变化率 > 20%': final_oi_change > 0.2
            }
            
            logger.info(f"{symbol} 条件检查结果:")
            for condition, result in conditions.items():
                logger.info(f"  {condition}: {'✓' if result else '✗'}")
            
            if all(conditions.values()):
                logger.info(f"{symbol} 符合交易机会条件!")
                return {
                    'symbol': symbol,
                    'current_price': current_price,
                    'current_oi': open_interest,
                    'oi_change': final_oi_change,
                    'price_change': final_price_change,
                    'oi_price_market_ratio': oi_price_market_ratio,
                    'volume_market_ratio': volume_market_ratio,
                    'market_cap': market_cap,
                    'timestamp': datetime.now().isoformat()
                }
            
            logger.info(f"{symbol} 不符合交易机会条件")
            return None
            
        except Exception as e:
            logger.error(f"分析{symbol}机会时发生错误: {str(e)}")
            return None
            
    def save_opportunity(self, opportunity: Dict[str, Any]):
        """
        保存交易机会到文件
        
        Args:
            opportunity: 交易机会数据
        """
        try:
            logger.info(f"开始保存{opportunity['symbol']}的交易机会...")
            
            # 读取现有数据
            if os.path.exists(self.opportunities_file):
                with open(self.opportunities_file, 'r') as f:
                    opportunities = json.load(f)
                logger.debug(f"已读取现有交易机会数据: {json.dumps(opportunities, indent=2)}")
            else:
                opportunities = []
                
            # 添加新机会
            opportunities.append(opportunity)
            
            # 保存数据
            with open(self.opportunities_file, 'w') as f:
                json.dump(opportunities, f, indent=2)
                
            logger.info(f"成功保存{opportunity['symbol']}的交易机会")
            logger.debug(f"保存的交易机会数据: {json.dumps(opportunity, indent=2)}")
            
        except Exception as e:
            logger.error(f"保存交易机会时发生错误: {str(e)}")
            
    def send_wecom_notification(self, opportunity: Dict[str, Any]):
        """
        发送企业微信通知
        
        Args:
            opportunity: 交易机会数据
        """
        try:
            # TODO: 实现企业微信机器人通知
            pass
        except Exception as e:
            logger.error(f"发送企业微信通知时发生错误: {str(e)}")
            
    def run(self):
        """运行交易机会发现程序"""
        try:
            logger.info("开始运行交易机会发现程序...")
            
            # 获取测试交易对
            symbols = self.get_test_symbol()
            logger.info(f"测试交易对: {symbols}")
            
            for symbol in symbols:
                logger.info(f"开始分析交易对: {symbol}")
                
                # 获取历史数据
                data = self.get_historical_data(symbol)
                if not data:
                    logger.warning(f"跳过{symbol}，无法获取历史数据")
                    continue
                    
                # 分析机会
                opportunity = self.analyze_opportunity(symbol, data)
                if opportunity:
                    # 保存机会
                    self.save_opportunity(opportunity)
                    # 发送通知
                    self.send_wecom_notification(opportunity)
                    
                # 避免触发频率限制
                time.sleep(0.1)
                
            logger.info("交易机会发现程序运行完成")
                
        except Exception as e:
            logger.error(f"运行程序时发生错误: {str(e)}")
            
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
        finder.run()
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        
if __name__ == '__main__':
    main() 