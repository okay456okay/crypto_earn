"""
Binance期货做空信号捕捉程序
基于山寨币突破高点后转跌策略的信号检测系统
专门针对加密货币市场7x24小时快速变化的特点进行优化
"""

import ccxt
import pandas as pd
import numpy as np
import talib
import time
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json
import asyncio
from dataclasses import dataclass
import sys, os

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from config import binance_api_key, binance_api_secret, proxies
from tools.logger import logger


@dataclass
class SignalData:
    """信号数据类"""
    symbol: str
    signal_type: str
    confidence: float
    price: float
    timestamp: datetime
    indicators: Dict
    reason: str


class BinanceShortSignalDetector:
    """
    Binance做空信号检测器
    专注于捕捉山寨币突破高点后刚转为下跌趋势的时机
    """

    def __init__(self, api_key: str = None, api_secret: str = None, testnet: bool = True):
        """
        初始化检测器

        Args:
            api_key: Binance API密钥
            api_secret: Binance API密钥
            testnet: 是否使用测试网
        """
        self.exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            # 'sandbox': testnet,
            'enableRateLimit': True,
            'proxies': proxies,
            'options': {
                'defaultType': 'future',  # 使用期货交易
            }
        })

        # 策略参数 - 针对加密货币市场优化
        self.config = {
            # === 基本筛选条件 ===
            'min_volume_24h': 1_000_000,  # 最小24h交易量100W USDT（保证流动性）
            
            # === 突破识别参数 ===
            'breakthrough_hours': 1,  # 突破时间窗口：过去1小时内（4根15分钟K线）
            'breakthrough_threshold': 0.05,  # 突破幅度：必须超过前高5%才算有效突破
            'breakthrough_volume_ratio': 1.2,  # 突破时成交量：必须是平均成交量的1.2倍以上
            
            # === 转跌确认参数 ===
            'pullback_candles': 3,  # 转跌确认：连续3根15分钟K线收跌
            'pullback_threshold': 0.02,  # 回调幅度：从突破高点回落2%以上
            'volume_decrease_ratio': 0.1,  # 回落时成交量：低于平均成交量10%
            
            # === 技术指标参数（针对15分钟K线优化） ===
            'rsi_period': 9,  # RSI周期：9（对应2.25小时，更敏感）
            'rsi_overbought': 65,  # RSI超买线：65（加密货币市场更激进）
            'ma_fast': 5,  # 快速均线：5（对应1.25小时）
            'ma_slow': 10,  # 慢速均线：10（对应2.5小时）
            'ma_trend': 20,  # 趋势均线：20（对应5小时）
            'macd_fast': 5,  # MACD快线周期
            'macd_slow': 10,  # MACD慢线周期
            'macd_signal': 4,  # MACD信号线周期
            
            # === 信号有效期 ===
            'signal_max_age_hours': 4,  # 信号最大有效期：4小时
        }

        # 初始化数据库
        self.init_database()

        # 缓存数据
        self.market_data = {}
        self.symbols_cache = []
        self.last_update = None

    def init_database(self):
        """初始化SQLite数据库"""
        self.conn = sqlite3.connect('trading_signals.db')
        cursor = self.conn.cursor()

        # 创建信号表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                confidence REAL NOT NULL,
                price REAL NOT NULL,
                timestamp TEXT NOT NULL,
                indicators TEXT NOT NULL,
                reason TEXT NOT NULL,
                executed BOOLEAN DEFAULT FALSE
            )
        ''')

        # 创建市场数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_data (
                symbol TEXT PRIMARY KEY,
                market_cap REAL,
                volume_24h REAL,
                price REAL,
                change_24h REAL,
                last_update TEXT
            )
        ''')

        self.conn.commit()

    def get_futures_symbols(self) -> List[str]:
        """获取期货交易对列表"""
        try:
            markets = self.exchange.load_markets()
            futures_symbols = []

            for symbol, market in markets.items():
                if (market['type'] == 'swap' and
                        market['quote'] == 'USDT' and
                        market['active'] and
                        symbol.endswith(':USDT')):
                    futures_symbols.append(symbol)

            logger.info(f"获取到 {len(futures_symbols)} 个期货交易对")
            return futures_symbols

        except Exception as e:
            logger.error(f"获取交易对失败: {e}")
            return []

    def get_market_data(self, symbol: str) -> Optional[Dict]:
        """获取市场数据"""
        try:
            ticker = self.exchange.fetch_ticker(symbol)

            # 获取24小时统计数据
            # stats = self.exchange.fetch_ticker(symbol)

            market_data = {
                'symbol': symbol,
                'price': ticker['last'],
                'volume_24h': ticker['quoteVolume'] or 0,
                'change_24h': ticker['percentage'] or 0,
                'high_24h': ticker['high'],
                'low_24h': ticker['low'],
                'timestamp': datetime.now()
            }

            return market_data

        except Exception as e:
            logger.error(f"获取 {symbol} 市场数据失败: {e}")
            return None

    def get_kline_data(self, symbol: str, timeframe: str = '15m', limit: int = 200) -> Optional[pd.DataFrame]:
        """
        获取K线数据
        
        Args:
            symbol: 交易对符号
            timeframe: 时间周期，默认15分钟（适合加密货币快速变化）
            limit: 获取K线数量，200根15分钟K线 = 50小时数据
            
        Returns:
            包含OHLCV数据的DataFrame
        """
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)

            return df

        except Exception as e:
            logger.error(f"获取 {symbol} K线数据失败: {e}")
            return None

    def detect_recent_breakthrough(self, df: pd.DataFrame) -> Dict:
        """
        检测近期突破情况
        
        技术分析解释：
        - 突破是指价格超过前期高点，表示上涨动能强劲
        - 有效突破需要成交量配合，否则可能是假突破
        - 我们寻找过去几小时内发生的突破，然后等待转跌机会
        
        Args:
            df: K线数据DataFrame
            
        Returns:
            包含突破信息的字典
        """
        try:
            # 计算突破时间窗口内的K线数量（基于15分钟）
            breakthrough_candles = self.config['breakthrough_hours'] * 4  # 8小时 = 32根15分钟K线
            
            if len(df) < breakthrough_candles + 20:
                return {'has_breakthrough': False, 'reason': '数据不足'}
            
            # 获取当前价格和最近的数据
            current_price = df['close'].iloc[-1]
            recent_data = df.tail(breakthrough_candles)  # 最近8小时的数据
            previous_data = df.iloc[-(breakthrough_candles + 20):-breakthrough_candles]  # 更早期的数据
            
            # 计算前期高点（突破前的最高价）
            previous_high = previous_data['high'].max()
            
            # 检查最近是否有突破
            recent_high = recent_data['high'].max()
            breakthrough_point = recent_high
            
            # 判断是否为有效突破
            breakthrough_ratio = (breakthrough_point - previous_high) / previous_high
            
            if breakthrough_ratio < self.config['breakthrough_threshold']:
                return {'has_breakthrough': False, 'reason': f'突破幅度不足: {breakthrough_ratio*100:.1f}%'}
            
            # 查找突破发生的具体时间点
            breakthrough_idx = recent_data['high'].idxmax()
            breakthrough_candle = recent_data.loc[breakthrough_idx]
            
            # 检查突破时的成交量
            avg_volume = df['volume'].tail(breakthrough_candles * 2).mean()  # 过去16小时平均成交量
            breakthrough_volume = breakthrough_candle['volume']
            volume_ratio = breakthrough_volume / avg_volume if avg_volume > 0 else 1
            
            if volume_ratio < self.config['breakthrough_volume_ratio']:
                return {'has_breakthrough': False, 'reason': f'突破成交量不足: {volume_ratio:.1f}倍'}
            
            # 计算突破后的时间
            breakthrough_time = breakthrough_idx
            current_time = df.index[-1]
            hours_since_breakthrough = (current_time - breakthrough_time).total_seconds() / 3600
            
            return {
                'has_breakthrough': True,
                'breakthrough_price': breakthrough_point,
                'breakthrough_time': breakthrough_time,
                'hours_since': hours_since_breakthrough,
                'breakthrough_ratio': breakthrough_ratio,
                'volume_ratio': volume_ratio,
                'previous_high': previous_high
            }
            
        except Exception as e:
            logger.error(f"检测突破失败: {e}")
            return {'has_breakthrough': False, 'reason': f'检测错误: {e}'}

    def detect_trend_reversal(self, df: pd.DataFrame, breakthrough_info: Dict) -> Dict:
        """
        检测趋势转换信号
        
        技术分析解释：
        - 趋势转换是指价格从上涨转为下跌的过程
        - 我们通过连续下跌的K线、均线死叉、成交量萎缩等信号确认
        - 早期识别转换信号可以获得更好的做空入场点
        
        Args:
            df: K线数据DataFrame
            breakthrough_info: 突破信息
            
        Returns:
            包含趋势转换信息的字典
        """
        try:
            if not breakthrough_info['has_breakthrough']:
                return {'has_reversal': False, 'reason': '无突破基础'}
            
            # 获取最近的K线数据（用于判断转跌）
            recent_candles = df.tail(self.config['pullback_candles'])
            current_price = df['close'].iloc[-1]
            breakthrough_price = breakthrough_info['breakthrough_price']
            
            # 1. 检查价格回调幅度
            pullback_ratio = (breakthrough_price - current_price) / breakthrough_price
            if pullback_ratio < self.config['pullback_threshold']:
                return {'has_reversal': False, 'reason': f'回调幅度不足: {pullback_ratio*100:.1f}%'}
            
            # 2. 检查连续下跌K线
            declining_candles = 0
            for i in range(len(recent_candles)):
                if recent_candles.iloc[i]['close'] < recent_candles.iloc[i]['open']:
                    declining_candles += 1
            
            if declining_candles < 2:  # 至少2根阴线
                return {'has_reversal': False, 'reason': f'下跌K线不足: {declining_candles}根'}
            
            # 3. 检查成交量变化（转跌时成交量应该萎缩）
            recent_volume = df['volume'].tail(4).mean()  # 最近1小时平均成交量
            breakthrough_volume = df['volume'].tail(16).mean()  # 突破期间平均成交量
            volume_decline_ratio = recent_volume / breakthrough_volume if breakthrough_volume > 0 else 1
            
            return {
                'has_reversal': True,
                'pullback_ratio': pullback_ratio,
                'declining_candles': declining_candles,
                'volume_decline_ratio': volume_decline_ratio,
                'reversal_strength': min(pullback_ratio * 10 + declining_candles * 5, 50)  # 转跌强度评分
            }
            
        except Exception as e:
            logger.error(f"检测趋势转换失败: {e}")
            return {'has_reversal': False, 'reason': f'检测错误: {e}'}

    def calculate_technical_indicators(self, df: pd.DataFrame) -> Dict:
        """
        计算技术指标
        
        技术分析指标解释：
        - RSI: 相对强弱指数，衡量价格上涨下跌的力度，70以上超买，30以下超卖
        - 均线: 平滑价格波动，判断趋势方向，短期均线跌破长期均线为看跌信号
        - MACD: 异同移动平均线，判断趋势转换，MACD线跌破信号线为看跌信号
        - 布林带: 价格通道，价格触及上轨后回落表示上涨力度衰竭
        - ATR: 平均真实波幅，衡量价格波动程度，用于设置止损
        
        Args:
            df: K线数据DataFrame
            
        Returns:
            包含各种技术指标的字典
        """
        try:
            close = df['close'].values
            high = df['high'].values
            low = df['low'].values
            volume = df['volume'].values

            # RSI - 相对强弱指数（9周期，更敏感）
            rsi = talib.RSI(close, timeperiod=self.config['rsi_period'])
            
            # 移动平均线
            ma_fast = talib.SMA(close, timeperiod=self.config['ma_fast'])  # 5周期快线
            ma_slow = talib.SMA(close, timeperiod=self.config['ma_slow'])  # 10周期慢线
            ma_trend = talib.SMA(close, timeperiod=self.config['ma_trend'])  # 20周期趋势线
            
            # 指数移动平均线（更敏感）
            ema_fast = talib.EMA(close, timeperiod=self.config['ma_fast'])
            ema_slow = talib.EMA(close, timeperiod=self.config['ma_slow'])
            
            # 布林带（20周期标准设置）
            bb_upper, bb_middle, bb_lower = talib.BBANDS(close, timeperiod=20)
            
            # MACD（快速参数设置）
            macd, macd_signal, macd_hist = talib.MACD(
                close, 
                fastperiod=self.config['macd_fast'],
                slowperiod=self.config['macd_slow'], 
                signalperiod=self.config['macd_signal']
            )
            
            # 成交量移动平均
            volume_ma = talib.SMA(volume, timeperiod=10)
            
            # ATR - 平均真实波幅
            atr = talib.ATR(high, low, close, timeperiod=14)

            # 计算各种比率和位置
            current_price = close[-1]
            
            indicators = {
                # 基础指标
                'rsi': rsi[-1] if not np.isnan(rsi[-1]) else 50,
                'ma_fast': ma_fast[-1] if not np.isnan(ma_fast[-1]) else current_price,
                'ma_slow': ma_slow[-1] if not np.isnan(ma_slow[-1]) else current_price,
                'ma_trend': ma_trend[-1] if not np.isnan(ma_trend[-1]) else current_price,
                'ema_fast': ema_fast[-1] if not np.isnan(ema_fast[-1]) else current_price,
                'ema_slow': ema_slow[-1] if not np.isnan(ema_slow[-1]) else current_price,
                
                # 布林带
                'bb_upper': bb_upper[-1] if not np.isnan(bb_upper[-1]) else current_price * 1.02,
                'bb_middle': bb_middle[-1] if not np.isnan(bb_middle[-1]) else current_price,
                'bb_lower': bb_lower[-1] if not np.isnan(bb_lower[-1]) else current_price * 0.98,
                
                # MACD
                'macd': macd[-1] if not np.isnan(macd[-1]) else 0,
                'macd_signal': macd_signal[-1] if not np.isnan(macd_signal[-1]) else 0,
                'macd_hist': macd_hist[-1] if not np.isnan(macd_hist[-1]) else 0,
                
                # 成交量
                'volume_ma': volume_ma[-1] if not np.isnan(volume_ma[-1]) else 1,
                'current_volume': volume[-1],
                
                # ATR
                'atr': atr[-1] if not np.isnan(atr[-1]) else current_price * 0.02,
            }
            
            # 计算衍生指标
            indicators.update({
                # 均线关系（判断趋势）
                'ma_fast_vs_slow': (indicators['ma_fast'] - indicators['ma_slow']) / indicators['ma_slow'],
                'ema_fast_vs_slow': (indicators['ema_fast'] - indicators['ema_slow']) / indicators['ema_slow'],
                'price_vs_ma_trend': (current_price - indicators['ma_trend']) / indicators['ma_trend'],
                
                # 布林带位置（判断超买超卖）
                'bb_position': (current_price - indicators['bb_lower']) / (indicators['bb_upper'] - indicators['bb_lower']),
                
                # MACD信号（判断动量）
                'macd_signal_strength': indicators['macd'] - indicators['macd_signal'],
                
                # 成交量比率（判断资金流向）
                'volume_ratio': indicators['current_volume'] / indicators['volume_ma'] if indicators['volume_ma'] > 0 else 1,
                
                # 波动率（用于风险控制）
                'volatility': indicators['atr'] / current_price,
            })

            return indicators

        except Exception as e:
            logger.error(f"计算技术指标失败: {e}")
            return {}

    def filter_symbols_by_fundamentals(self, symbols: List[str]) -> List[str]:
        """根据基本面筛选交易对"""
        filtered_symbols = []

        for symbol in symbols:
            try:
                market_data = self.get_market_data(symbol)
                if not market_data:
                    continue

                # 交易量筛选
                if market_data['volume_24h'] < self.config['min_volume_24h']:
                    continue

                # 排除稳定币和主流币
                base_asset = symbol.split('/')[0]
                if base_asset in ['BTC', 'ETH', 'BNB', 'USDT', 'USDC', 'BUSD', 'DAI']:
                    continue

                filtered_symbols.append(symbol)

                # 保存市场数据到数据库
                self.save_market_data(market_data)

            except Exception as e:
                logger.error(f"筛选 {symbol} 时出错: {e}")
                continue

        logger.info(f"基本面筛选后剩余 {len(filtered_symbols)} 个交易对")
        return filtered_symbols

    def analyze_short_signal(self, symbol: str) -> Optional[SignalData]:
        """
        分析做空信号 - 优化版突破转跌策略
        
        策略核心逻辑：
        1. 首先检测是否有近期突破（过去8小时内创新高）
        2. 确认突破的有效性（突破幅度和成交量）
        3. 检测趋势转换信号（连续下跌K线、回调幅度）
        4. 技术指标确认（RSI背离、均线死叉、MACD转向）
        5. 综合评分，只有高置信度信号才发出
        
        Args:
            symbol: 交易对符号
            
        Returns:
            SignalData对象或None
        """
        try:
            # 获取K线数据（15分钟，200根=50小时）
            df = self.get_kline_data(symbol, '15m', 200)
            if df is None or len(df) < 100:
                return None

            # 1. 检测近期突破
            breakthrough_info = self.detect_recent_breakthrough(df)
            if not breakthrough_info['has_breakthrough']:
                return None

            # 2. 检测趋势转换
            reversal_info = self.detect_trend_reversal(df, breakthrough_info)
            if not reversal_info['has_reversal']:
                return None

            # 3. 计算技术指标
            indicators = self.calculate_technical_indicators(df)
            if not indicators:
                return None

            current_price = df['close'].iloc[-1]

            # 4. 综合评分系统（总分100分）
            signal_score = 0
            reasons = []

            # === 突破转跌基础分（35分） ===
            
            # 突破后回落幅度（20分）
            pullback_score = min(reversal_info['pullback_ratio'] * 1000, 20)  # 2%回落=20分
            signal_score += pullback_score
            reasons.append(f"突破后回落{reversal_info['pullback_ratio']*100:.1f}%")
            
            # 连续下跌K线（10分）
            decline_score = min(reversal_info['declining_candles'] * 3, 10)
            signal_score += decline_score
            reasons.append(f"{reversal_info['declining_candles']}根连续阴线")
            
            # 成交量萎缩（5分）
            if reversal_info['volume_decline_ratio'] < self.config['volume_decrease_ratio']:
                signal_score += 5
                reasons.append("回落成交量萎缩")

            # === 技术指标确认（40分） ===
            
            # RSI超买回落（15分）
            if indicators['rsi'] > self.config['rsi_overbought']:
                rsi_score = min((indicators['rsi'] - self.config['rsi_overbought']) * 0.5, 15)
                signal_score += rsi_score
                reasons.append(f"RSI超买({indicators['rsi']:.1f})")
            elif indicators['rsi'] > 60:  # 次级超买
                signal_score += 8
                reasons.append(f"RSI偏高({indicators['rsi']:.1f})")
            
            # 短期均线死叉（15分）
            if indicators['ema_fast_vs_slow'] < -0.005:  # 快线跌破慢线0.5%以上
                signal_score += 15
                reasons.append("短期均线死叉")
            elif indicators['ema_fast_vs_slow'] < 0:  # 刚开始死叉
                signal_score += 10
                reasons.append("均线开始死叉")
            
            # MACD转向（10分）
            if (indicators['macd_signal_strength'] < 0 and 
                indicators['macd_hist'] < 0):
                signal_score += 10
                reasons.append("MACD转向下跌")

            # === 位置和风险控制（25分） ===
            
            # 价格位置（10分）
            if indicators['bb_position'] > 0.8:  # 接近布林带上轨
                signal_score += 10
                reasons.append("价格接近超买区域")
            elif indicators['bb_position'] > 0.6:
                signal_score += 5
                reasons.append("价格位置偏高")
            
            # 趋势线破位（10分）
            if indicators['price_vs_ma_trend'] < -0.02:  # 跌破20日线2%以上
                signal_score += 10
                reasons.append("跌破关键趋势线")
            elif current_price < indicators['ma_trend']:
                signal_score += 5
                reasons.append("跌破趋势线")
            
            # 波动率控制（5分）
            if indicators['volatility'] < 0.1:  # 波动率不能太高，避免风险
                signal_score += 5
                reasons.append("波动率适中")

            # 5. 信号有效性检查
            
            # 突破时间不能太久
            if breakthrough_info['hours_since'] > self.config['signal_max_age_hours']:
                signal_score *= 0.5  # 时间过久，分数减半
                reasons.append(f"突破已{breakthrough_info['hours_since']:.1f}小时")
            
            # 最低分数要求：65分以上才认为是有效信号
            if signal_score >= 65:
                confidence = min(signal_score / 100, 0.95)  # 置信度不超过95%
                
                # 补充指标信息
                indicators.update({
                    'breakthrough_price': breakthrough_info['breakthrough_price'],
                    'breakthrough_ratio': breakthrough_info['breakthrough_ratio'],
                    'pullback_ratio': reversal_info['pullback_ratio'],
                    'signal_score': signal_score
                })

                signal_data = SignalData(
                    symbol=symbol,
                    signal_type='SHORT',
                    confidence=confidence,
                    price=current_price,
                    timestamp=datetime.now(),
                    indicators=indicators,
                    reason='; '.join(reasons)
                )

                return signal_data

            return None

        except Exception as e:
            logger.error(f"分析 {symbol} 做空信号失败: {e}")
            return None

    def save_signal(self, signal: SignalData):
        """保存信号到数据库"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO signals (symbol, signal_type, confidence, price, timestamp, indicators, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                signal.symbol,
                signal.signal_type,
                signal.confidence,
                signal.price,
                signal.timestamp.isoformat(),
                json.dumps(signal.indicators),
                signal.reason
            ))
            self.conn.commit()
            logger.info(f"保存信号: {signal.symbol} - {signal.reason}")

        except Exception as e:
            logger.error(f"保存信号失败: {e}")

    def save_market_data(self, market_data: Dict):
        """保存市场数据到数据库"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO market_data 
                (symbol, volume_24h, price, change_24h, last_update)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                market_data['symbol'],
                market_data['volume_24h'],
                market_data['price'],
                market_data['change_24h'],
                market_data['timestamp'].isoformat()
            ))
            self.conn.commit()

        except Exception as e:
            logger.error(f"保存市场数据失败: {e}")

    def get_recent_signals(self, hours: int = 24) -> List[Dict]:
        """获取最近的信号"""
        try:
            cursor = self.conn.cursor()
            since = (datetime.now() - timedelta(hours=hours)).isoformat()

            cursor.execute('''
                SELECT * FROM signals 
                WHERE timestamp > ? 
                ORDER BY confidence DESC, timestamp DESC
            ''', (since,))

            columns = [desc[0] for desc in cursor.description]
            signals = []

            for row in cursor.fetchall():
                signal_dict = dict(zip(columns, row))
                signal_dict['indicators'] = json.loads(signal_dict['indicators'])
                signals.append(signal_dict)

            return signals

        except Exception as e:
            logger.error(f"获取最近信号失败: {e}")
            return []

    def scan_all_symbols(self):
        """扫描所有交易对寻找做空信号"""
        logger.info("开始扫描突破转跌做空信号...")

        # 获取所有期货交易对
        symbols = self.get_futures_symbols()
        if not symbols:
            logger.error("无法获取交易对列表")
            return

        # 基本面筛选
        filtered_symbols = self.filter_symbols_by_fundamentals(symbols[:100])  # 限制数量避免API限制

        # 技术面分析
        signals_found = 0
        for symbol in filtered_symbols:
            try:
                signal = self.analyze_short_signal(symbol)
                if signal:
                    self.save_signal(signal)
                    signals_found += 1
                    logger.info(f"发现做空信号: {symbol} (置信度: {signal.confidence:.2f}) - {signal.reason}")

                # API限制保护
                time.sleep(0.1)

            except Exception as e:
                logger.error(f"处理 {symbol} 时出错: {e}")
                continue

        logger.info(f"扫描完成，发现 {signals_found} 个做空信号")

    def run_continuous_scan(self, interval_minutes: int = 30):
        """
        持续扫描模式
        
        针对加密货币市场快速变化的特点，缩短扫描间隔到30分钟
        """
        logger.info(f"启动持续扫描模式，间隔 {interval_minutes} 分钟")

        while True:
            try:
                self.scan_all_symbols()

                # 显示最近的信号
                recent_signals = self.get_recent_signals(12)  # 显示过去12小时的信号
                if recent_signals:
                    logger.info(f"过去12小时发现 {len(recent_signals)} 个信号")
                    for signal in recent_signals[:5]:  # 显示前5个
                        logger.info(f"  {signal['symbol']}: {signal['reason']} (置信度: {signal['confidence']:.2f})")

                # 等待下次扫描
                logger.info(f"等待 {interval_minutes} 分钟后进行下次扫描...")
                time.sleep(interval_minutes * 60)

            except KeyboardInterrupt:
                logger.info("用户中断，停止扫描")
                break
            except Exception as e:
                logger.error(f"扫描过程中出错: {e}")
                time.sleep(60)  # 出错后等待1分钟再继续


def main():
    """主函数"""
    # 配置你的API密钥（建议使用环境变量）
    # API_KEY = "your_api_key_here"  # 替换为你的API密钥
    # API_SECRET = "your_api_secret_here"  # 替换为你的API密钥

    # 创建检测器实例
    detector = BinanceShortSignalDetector(
        api_key=binance_api_key,
        api_secret=binance_api_secret,
        testnet=True  # 使用测试网，正式交易时改为False
    )

    print("Binance期货突破转跌做空信号检测器")
    print("专门捕捉山寨币突破高点后转为下跌趋势的时机")
    print("1. 单次扫描")
    print("2. 持续扫描（推荐30分钟间隔）")
    print("3. 查看最近信号")

    choice = input("请选择操作 (1-3): ")

    if choice == "1":
        detector.scan_all_symbols()
    elif choice == "2":
        interval = int(input("扫描间隔（分钟）[默认30]: ") or "30")
        detector.run_continuous_scan(interval)
    elif choice == "3":
        hours = int(input("查看最近多少小时的信号 [默认12]: ") or "12")
        signals = detector.get_recent_signals(hours)

        if signals:
            print(f"\n过去{hours}小时的突破转跌信号:")
            for signal in signals:
                indicators = signal['indicators']
                print(f"交易对: {signal['symbol']}")
                print(f"置信度: {signal['confidence']:.2f}")
                print(f"当前价格: {signal['price']:.6f}")
                print(f"突破价格: {indicators.get('breakthrough_price', 'N/A'):.6f}")
                print(f"回调幅度: {indicators.get('pullback_ratio', 0)*100:.1f}%")
                print(f"信号评分: {indicators.get('signal_score', 0):.1f}/100")
                print(f"原因: {signal['reason']}")
                print(f"时间: {signal['timestamp']}")
                print("-" * 60)
        else:
            print("未找到任何信号")
    else:
        print("无效选择")


if __name__ == "__main__":
    main()