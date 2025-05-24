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
        self.final_oi_change_threshold = BINANCE_OPPORTUNITY_FINDER['FINAL_OI_CHANGE_THRESHOLD']

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

    def format_opportunity_report(self, symbol: str, conditions: Dict[str, bool], 
                                oi_price_market_ratio: float, volume_market_ratio: float,
                                historical_price_changes: List[float], historical_oi_changes: List[float],
                                final_oi_change: float) -> str:
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
            
        Returns:
            str: 格式化后的报告
        """
        # 计算历史价格变化率的最大值
        max_price_change = max(abs(change) for change in historical_price_changes[:-1]) * 100
        # 计算历史持仓量变化率的最大值
        max_oi_change = max(abs(change) for change in historical_oi_changes[:-1]) * 100
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        report = f"{symbol} - {current_time}\n"
        report += f"交易活跃度:合约持仓金额/市值 {oi_price_market_ratio:.2f} > {self.oi_price_market_ratio_threshold}: {'✓' if conditions[f'交易活跃度:合约持仓金额/市值 > {self.oi_price_market_ratio_threshold}'] else '✗'}\n"
        report += f"交易活跃度:近24小时成交量/市值 {volume_market_ratio:.2f} > {self.volume_market_ratio_threshold}: {'✓' if conditions[f'交易活跃度:近24小时成交量/市值 > {self.volume_market_ratio_threshold}'] else '✗'}\n"
        report += f"拉盘信号:历史价格变化率 {max_price_change:.1f}% < {self.historical_change_threshold*100}%: {'✓' if conditions[f'拉盘信号:历史价格变化率 < {self.historical_change_threshold*100}%'] else '✗'}\n"
        report += f"拉盘信号:历史持仓量变化率 {max_oi_change:.1f}% < {self.historical_change_threshold*100}%: {'✓' if conditions[f'拉盘信号:历史持仓量变化率 < {self.historical_change_threshold*100}%'] else '✗'}\n"
        report += f"拉盘信号:最终持仓量变化率 {final_oi_change*100:.1f}% > {self.final_oi_change_threshold*100}%: {'✓' if conditions[f'拉盘信号:最终持仓量变化率 > {self.final_oi_change_threshold*100}%'] else '✗'}\n\n"
        return report
        
    def save_opportunity(self, opportunity: Dict[str, Any], conditions: Dict[str, bool],
                        historical_price_changes: List[float], historical_oi_changes: List[float]):
        """
        保存交易机会到文件
        
        Args:
            opportunity: 交易机会数据
            conditions: 条件检查结果
            historical_price_changes: 历史价格变化率列表
            historical_oi_changes: 历史持仓量变化率列表
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
                opportunity['oi_change']
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
            
    def get_all_symbols(self) -> List[str]:
        """
        获取所有同时具有现货和合约的交易对
        
        Returns:
            List[str]: 交易对列表
        """
        try:
            logger.info("开始获取所有交易对...")
            
            # 获取现货交易对
            spot_symbols = set()
            spot_exchange_info = self.client.get_exchange_info()
            for symbol_info in spot_exchange_info['symbols']:
                if (symbol_info['status'] == 'TRADING' and 
                    symbol_info['quoteAsset'] == 'USDT' and 
                    symbol_info['isSpotTradingAllowed']):
                    spot_symbols.add(symbol_info['symbol'])
            logger.info(f"获取到{len(spot_symbols)}个现货交易对")
            
            # 获取合约交易对
            futures_symbols = set()
            futures_exchange_info = self.client.futures_exchange_info()
            for symbol_info in futures_exchange_info['symbols']:
                if (symbol_info['status'] == 'TRADING' and 
                    symbol_info['quoteAsset'] == 'USDT' and 
                    symbol_info['contractType'] == 'PERPETUAL'):
                    futures_symbols.add(symbol_info['symbol'])
            logger.info(f"获取到{len(futures_symbols)}个合约交易对")
            
            # 获取同时具有现货和合约的交易对
            common_symbols = list(spot_symbols.intersection(futures_symbols))
            logger.info(f"找到{len(common_symbols)}个同时具有现货和合约的交易对")
            
            return common_symbols
            
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
                
            logger.info(f"开始分析{len(symbols)}个交易对...")
            
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
                    # 保存机会
                    self.save_opportunity(opportunity, conditions, historical_price_changes, historical_oi_changes)
                    # 发送通知
                    self.send_wecom_notification(opportunity)
                    
                # 避免触发频率限制
                time.sleep(0.1)
                
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
            
            # 检查历史变化率是否都在阈值以内
            historical_changes_ok = all(abs(change) <= self.historical_change_threshold for change in historical_oi_changes[:-1])
            
            # 计算历史价格变化率
            historical_price_changes = []
            for i in range(len(klines) - 1):
                current_price = float(klines[i][4])  # 收盘价
                next_price = float(klines[i + 1][4])
                change = (next_price - current_price) / current_price
                historical_price_changes.append(change)
            
            # 计算最终价格变化率（最后一个时点）
            final_price_change = (float(klines[-1][4]) - float(klines[-2][4])) / float(klines[-2][4])
            
            # 检查历史价格变化率是否都在阈值以内
            historical_price_changes_ok = all(abs(change) <= self.historical_change_threshold for change in historical_price_changes[:-1])
            
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
                f'交易活跃度:合约持仓金额/市值 > {self.oi_price_market_ratio_threshold}': oi_price_market_ratio > self.oi_price_market_ratio_threshold,
                f'交易活跃度:近24小时成交量/市值 > {self.volume_market_ratio_threshold}': volume_market_ratio > self.volume_market_ratio_threshold,
                f'拉盘信号:历史价格变化率 < {self.historical_change_threshold*100}%': historical_price_changes_ok,
                f'拉盘信号:历史持仓量变化率 < {self.historical_change_threshold*100}%': historical_changes_ok,
                f'拉盘信号:最终持仓量变化率 > {self.final_oi_change_threshold*100}%': final_oi_change > self.final_oi_change_threshold
            }
            
            logger.info(f"{symbol} 条件检查结果:")
            for condition, result in conditions.items():
                logger.info(f"{symbol}  {condition}: {'✓' if result else '✗'}")
            
            if all(conditions.values()):
                logger.info(f"{symbol} 符合交易机会条件!")
                return (
                    {
                        'symbol': symbol,
                        'current_price': current_price,
                        'current_oi': open_interest,
                        'oi_change': final_oi_change,
                        'price_change': final_price_change,
                        'oi_price_market_ratio': oi_price_market_ratio,
                        'volume_market_ratio': volume_market_ratio,
                        'market_cap': market_cap,
                        'timestamp': datetime.now().isoformat()
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
        logger.info("开始运行交易机会发现器...")
        finder.run()
        
    except Exception as e:
        logger.error(f"程序执行失败: {str(e)}")
        
if __name__ == '__main__':
    main() 