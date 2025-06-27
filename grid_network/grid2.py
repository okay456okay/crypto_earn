#!/usr/bin/env python3
"""
网格交易货币对筛选器 (Grid Trading Pair Screener)

该工具用于筛选适合网格交易策略的加密货币交易对。
网格交易是一种量化交易策略，通过在价格区间内设置多个买卖订单，
在价格震荡中获利，适合在横盘或震荡行情中使用。

主要功能：
1. 获取市场数据并计算技术指标
2. 模拟网格交易效果
3. 评分并筛选出最适合网格交易的货币对
4. 导出分析结果供进一步使用

算法思路：
- 使用统计学方法分析价格波动特征
- 计算趋势强度指标（ADX）判断是否适合网格交易
- 通过回测模拟计算网格交易收益率和成功率
- 综合多项指标给出适宜性评分
"""

import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import json
from typing import Dict, List, Tuple, Optional

# 添加父级目录到路径，以便导入配置文件
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import binance_api_key, binance_api_secret, proxies


class GridTradingScreener:
    """
    网格交易货币对筛选器类
    
    该类实现了完整的网格交易适宜性分析流程，包括：
    - 数据获取和预处理
    - 技术指标计算
    - 网格交易模拟
    - 综合评分系统
    """

    def __init__(self, exchange_name: str = 'binance'):
        """
        初始化筛选器实例

        Args:
            exchange_name: 交易所名称，默认为 'binance'
        """
        self.exchange = getattr(ccxt, exchange_name)({
            'apiKey': binance_api_key,  # API密钥（可选，用于提高请求限制）
            'secret': binance_api_secret,  # API密钥对应的密钥
            'sandbox': False,  # 是否使用沙盒环境
            'rateLimit': 1200,  # 请求频率限制（毫秒）
            'proxies': proxies,  # 代理设置
        })

    def get_market_data(self, symbol: str, timeframe: str = '15m', days: int = 15) -> pd.DataFrame:
        """
        获取指定货币对的市场数据（OHLCV）

        Args:
            symbol: 交易对符号，如 'BTC/USDT'
            timeframe: 时间周期，如 '15m', '1h', '4h', '1d'（默认15分钟）
            days: 获取的历史数据天数（默认15天）

        Returns:
            包含OHLCV数据的DataFrame，以时间戳为索引
            
        数据结构：
            - timestamp: 时间戳（作为索引）
            - open: 开盘价
            - high: 最高价  
            - low: 最低价
            - close: 收盘价
            - volume: 成交量
        """
        try:
            # 计算起始时间戳（当前时间减去指定天数）
            since = self.exchange.milliseconds() - days * 24 * 60 * 60 * 1000
            
            # 获取OHLCV数据
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since)

            # 转换为DataFrame并设置列名
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # 将时间戳转换为日期时间格式并设为索引
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)

            return df
        except Exception as e:
            print(f"获取 {symbol} 数据时出错: {e}")
            return pd.DataFrame()

    def calculate_grid_metrics(self, df: pd.DataFrame) -> Dict:
        """
        计算网格交易适宜性指标
        
        该方法计算多项技术指标来评估货币对是否适合网格交易：
        1. 基础统计指标（均价、标准差）
        2. 波动率指标（日波动率、波动率稳定性）
        3. 趋势强度指标（ADX）
        4. 线性回归趋势分析
        5. 网格交易模拟结果

        Args:
            df: 包含OHLCV数据的DataFrame

        Returns:
            包含所有计算指标的字典
        """
        # 数据验证：确保有足够的数据点进行分析
        if df.empty or len(df) < 20:
            return {}

        # 提取收盘价序列和计算收益率
        prices = df['close']
        
        # 计算逐期收益率：使用pct_change()方法计算百分比变化率
        # 
        # === pct_change()方法详解 ===
        # 作用：计算序列中每个值相对于前一个值的百分比变化率
        # 公式：(当前值 - 前一个值) / 前一个值
        # 
        # 数值示例：假设价格序列为 [100, 102, 99, 103]
        # pct_change()结果：[NaN, 0.02, -0.0294, 0.0404]
        # 详细计算过程：
        #   索引0: NaN (第一个值没有前值可比较)
        #   索引1: (102-100)/100 = 0.02 = +2.0% (价格上涨2%)
        #   索引2: (99-102)/102 = -0.0294 = -2.94% (价格下跌2.94%)
        #   索引3: (103-99)/99 = 0.0404 = +4.04% (价格上涨4.04%)
        #
        # 金融应用意义：
        # - 收益率是风险管理的核心指标
        # - 用于计算波动率（收益率的标准差）
        # - 评估价格变动的相对幅度，消除价格水平的影响
        # - 便于不同资产间的风险收益比较
        # 
        # dropna()的作用：移除第一个NaN值，确保后续计算的准确性
        returns = prices.pct_change().dropna()  # 计算收益率并移除第一个NaN值

        # === 基础统计指标 ===
        mean_price = prices.mean()  # 平均价格，用于确定网格中心
        price_std = prices.std()    # 价格标准差，用于确定网格范围

        # === 波动率分析 ===
        # 年化日波动率：使用收益率标准差乘以时间调整因子
        # 数据为15分钟级，一天96个15分钟周期（24*4=96），故乘以sqrt(96)进行年化
        daily_volatility = returns.std() * np.sqrt(96)
        
        # 波动率稳定性：计算7天滚动波动率的波动程度
        # 7天 = 7 * 96个15分钟周期 = 672个数据点
        # 低值表示波动率相对稳定，适合网格交易
        volatility_stability = returns.rolling(7 * 96).std().std()

        # === 趋势强度分析（ADX指标）===
        def calculate_adx(df, period=14):
            """
            计算平均方向性指数（Average Directional Index）
            
            ADX是衡量趋势强度的技术指标：
            - ADX < 20: 弱趋势或横盘，适合网格交易
            - ADX 20-40: 中等趋势强度
            - ADX > 40: 强趋势，不适合网格交易
            
            计算步骤：
            1. 计算真实范围（True Range, TR）
            2. 计算方向性移动（Directional Movement, DM）
            3. 计算方向性指标（DI+, DI-）
            4. 计算方向性指数（DX）
            5. 对DX进行平滑处理得到ADX
            
            Args:
                df: OHLCV数据
                period: 计算周期，默认14
                
            Returns:
                ADX值（0-100），越低越适合网格交易
            """
            high, low, close = df['high'], df['low'], df['close']

            # 步骤1：计算真实范围（True Range）
            # TR是以下三个值中的最大值：
            # - 当期最高价 - 当期最低价
            # - |当期最高价 - 前期收盘价|
            # - |当期最低价 - 前期收盘价|
            tr1 = high - low
            tr2 = abs(high - close.shift(1))
            tr3 = abs(low - close.shift(1))
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

            # 步骤2：计算方向性移动（Directional Movement）
            # +DM：当期最高价超出前期最高价的部分（如果超出前期最低价更多）
            # -DM：前期最低价超出当期最低价的部分（如果超出前期最高价更多）
            dm_plus = np.where((high - high.shift(1)) > (low.shift(1) - low),
                               np.maximum(high - high.shift(1), 0), 0)
            dm_minus = np.where((low.shift(1) - low) > (high - high.shift(1)),
                                np.maximum(low.shift(1) - low, 0), 0)

            # 步骤3：计算方向性指标（Directional Indicators）
            # DI+ = 100 * (+DM的N周期平均) / (TR的N周期平均)
            # DI- = 100 * (-DM的N周期平均) / (TR的N周期平均)
            di_plus = 100 * pd.Series(dm_plus).rolling(period).mean() / tr.rolling(period).mean()
            di_minus = 100 * pd.Series(dm_minus).rolling(period).mean() / tr.rolling(period).mean()

            # 步骤4：计算方向性指数（DX）
            # DX = 100 * |DI+ - DI-| / (DI+ + DI-)
            dx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)
            
            # 步骤5：计算ADX（DX的N周期平均）
            adx = dx.rolling(period).mean()

            # 返回最新的ADX值，如果无法计算则返回50（中性值）
            return adx.iloc[-1] if not adx.empty else 50

        adx = calculate_adx(df)

        # === 线性回归趋势分析 ===
        # 使用线性回归分析价格趋势的强度和方向
        x = np.arange(len(prices))  # 时间序列（0, 1, 2, ...）
        slope, intercept = np.polyfit(x, prices, 1)  # 线性回归：y = slope*x + intercept
        
        # 计算决定系数（R²），衡量线性回归的拟合优度
        # R²接近1表示强趋势，接近0表示无明显趋势（适合网格交易）
        r_squared = np.corrcoef(x, prices)[0, 1] ** 2

        # === 网格交易模拟 ===
        grid_results = self.simulate_grid_trading(df,std_multiplier=0.8)

        # 整合所有指标
        metrics = {
            'symbol': '',  # 交易对符号，由调用者设置
            'mean_price': mean_price,  # 平均价格
            'price_std': price_std,    # 价格标准差
            'daily_volatility': daily_volatility,  # 日波动率
            'volatility_stability': volatility_stability,  # 波动率稳定性
            'adx': adx,  # 趋势强度指标
            'trend_slope': slope,  # 趋势斜率
            'trend_r_squared': r_squared,  # 趋势拟合优度
            'round_trips_30d': grid_results['round_trips'],  # 30天内完整交易次数
            'success_rate': grid_results['success_rate'],    # 交易成功率
            'monthly_return_estimate': grid_results['monthly_return'],  # 月收益率估计
            'breakout_risk': grid_results['breakout_risk'],  # 突破风险
            'in_range_time': grid_results['in_range_time'],  # 价格在网格范围内的时间比例
            'volume_24h': df['volume'].tail(96).sum(),  # 24小时成交量（96个15分钟周期）
        }

        return metrics

    def simulate_grid_trading(self, df: pd.DataFrame,
                              std_multiplier: float = 1.0) -> Dict:
        """
        模拟网格交易策略
        
        该方法实现简化的网格交易模拟：
        1. 基于历史价格统计确定网格上下边界
        2. 模拟在下边界买入、上边界卖出的交易过程
        3. 计算交易成功率、收益率等关键指标
        4. 评估突破风险和价格在网格内的时间比例

        网格交易原理：
        - 在价格下跌到下边界时买入
        - 在价格上涨到上边界时卖出
        - 通过多次买低卖高获取利润
        - 适合在震荡行情中使用

        Args:
            df: OHLCV数据
            std_multiplier: 标准差倍数，用于确定网格范围

        Returns:
            包含模拟结果的字典
        """
        if df.empty:
            return {'round_trips': 0, 'success_rate': 0, 'monthly_return': 0,
                    'breakout_risk': 1, 'in_range_time': 0}

        prices = df['close']
        mean_price = prices.mean()  # 计算平均价格作为网格中心
        price_std = prices.std()    # 计算价格标准差

        # === 确定网格边界 ===
        # 使用均值 ± N倍标准差来确定网格上下边界
        # 这是基于正态分布假设的统计学方法
        upper_bound = mean_price + std_multiplier * price_std  # 上边界（卖出价）
        lower_bound = mean_price - std_multiplier * price_std  # 下边界（买入价）

        # 确保最小网格范围（3%）以保证有效的交易机会
        range_pct = (upper_bound - lower_bound) / mean_price
        if range_pct < 0.01:  # 如果网格范围小于3%，则强制设置为±1.5%
            upper_bound = mean_price * 1.005  # +1.5%
            lower_bound = mean_price * 0.995  # -1.5%

        # === 模拟交易过程 ===
        position = 0  # 持仓状态：0=空仓，1=持仓
        round_trips = 0      # 完整交易轮次（买入->卖出）
        buy_signals = 0      # 买入信号总数
        successful_sells = 0 # 成功卖出次数

        # 逐个价格点模拟交易决策
        for price in prices:
            if position == 0 and price <= lower_bound:
                # 触发买入条件：空仓状态且价格跌破下边界
                position = 1
                buy_price = price
                buy_signals += 1
            elif position == 1 and price >= upper_bound:
                # 触发卖出条件：持仓状态且价格突破上边界
                position = 0
                round_trips += 1
                successful_sells += 1

        # === 计算性能指标 ===
        
        # 交易成功率：成功卖出次数 / 买入信号总数
        success_rate = successful_sells / buy_signals if buy_signals > 0 else 0

        # 估算月收益率
        if round_trips > 0:
            # 每次交易的平均利润率（考虑手续费0.2%）
            avg_profit_per_trip = (upper_bound - lower_bound) / lower_bound * 0.998
            
            # 将历史数据中的交易频率推广到月度
            # 计算公式：交易次数 * 单次利润 * (30天 / 实际天数)
            # 15分钟数据：len(prices) / 96 = 实际天数
            monthly_return = round_trips * avg_profit_per_trip * (30 / (len(prices) / 96))
        else:
            monthly_return = 0

        # 计算突破风险：价格超出网格范围的最大幅度
        max_price = prices.max()
        min_price = prices.min()
        breakout_risk = max(
            # 上方突破风险：(最高价 - 上边界) / 上边界
            (max_price - upper_bound) / upper_bound if upper_bound > 0 else 0,
            # 下方突破风险：(下边界 - 最低价) / 下边界  
            (lower_bound - min_price) / lower_bound if lower_bound > 0 else 0
        )

        # 计算价格在网格范围内的时间比例
        in_range_count = ((prices >= lower_bound) & (prices <= upper_bound)).sum()
        in_range_time = in_range_count / len(prices)

        return {
            'round_trips': round_trips,        # 完整交易轮次
            'success_rate': success_rate,      # 交易成功率
            'monthly_return': monthly_return,  # 预期月收益率
            'breakout_risk': breakout_risk,    # 突破风险
            'in_range_time': in_range_time,    # 范围内时间比例
            'upper_bound': upper_bound,        # 网格上边界
            'lower_bound': lower_bound,        # 网格下边界
        }

    def calculate_score(self, metrics: Dict) -> float:
        """
        计算网格交易适宜性综合评分
        
        该方法基于多项指标计算0-100分的综合评分：
        1. 交易频率评分（round_trips）
        2. 成功率评分（success_rate）
        3. 收益率评分（monthly_return）
        4. 波动率评分（volatility）
        5. 趋势评分（trend）
        6. 风险评分（risk）
        7. 流动性评分（volume）

        评分原理：
        - 网格交易适合震荡行情，因此低趋势强度得分更高
        - 适中的波动率（3-6%）最适合网格交易
        - 较高的交易频率和成功率表明策略有效性
        - 较低的突破风险确保策略安全性

        Args:
            metrics: 计算得到的各项指标字典

        Returns:
            综合评分（0-100分）
        """
        if not metrics:
            return 0

        # 各项子评分（0-100分）
        scores = {}

        # === 交易频率评分（权重：25%）===
        # 更多的完整交易轮次表明网格策略更有效
        # 每5次交易得20分，最高100分
        scores['round_trips'] = min(100, metrics.get('round_trips_30d', 0) * 20)

        # === 成功率评分（权重：20%）===
        # 直接将成功率转换为百分制评分
        scores['success_rate'] = metrics.get('success_rate', 0) * 100

        # === 收益率评分（权重：20%）===
        # 月收益率乘以500倍转换为评分，最高100分
        # 例如：2%月收益率 = 100分
        scores['monthly_return'] = min(100, metrics.get('monthly_return_estimate', 0) * 500)

        # === 波动率评分（权重：15%）===
        # 网格交易的最佳波动率范围是3-6%日波动率
        vol = metrics.get('daily_volatility', 0)
        if 0.03 <= vol <= 0.06:      # 最佳范围：满分
            scores['volatility'] = 100
        elif 0.02 <= vol <= 0.08:    # 次佳范围：80分
            scores['volatility'] = 80
        else:                         # 其他情况：根据偏离程度扣分
            scores['volatility'] = max(0, 80 - abs(vol - 0.045) * 1000)

        # === 趋势强度评分（权重：10%）===
        # ADX越低越适合网格交易（震荡行情）
        # ADX > 33表示强趋势，得0分
        adx = metrics.get('adx', 50)
        scores['trend'] = max(0, 100 - adx * 3)

        # === 风险评分（权重：5%）===
        # 突破风险越低越好
        # 突破风险 > 33%时得0分
        breakout_risk = metrics.get('breakout_risk', 1)
        scores['risk'] = max(0, 100 - breakout_risk * 300)

        # === 流动性评分（权重：5%）===
        # 更高的24小时成交量确保更好的流动性
        volume = metrics.get('volume_24h', 0)
        if volume >= 10_000_000:    # 1000万USDT以上：满分
            scores['volume'] = 100
        elif volume >= 1_000_000:   # 100万USDT以上：80分
            scores['volume'] = 80
        else:                       # 按比例计分，最高80分
            scores['volume'] = min(80, volume / 1_000_000 * 80)

        # === 加权平均计算总分 ===
        weights = {
            'round_trips': 0.25,     # 交易频率：25%
            'success_rate': 0.20,    # 成功率：20%
            'monthly_return': 0.20,  # 收益率：20%
            'volatility': 0.15,      # 波动率：15%
            'trend': 0.10,           # 趋势强度：10%
            'risk': 0.05,            # 突破风险：5%
            'volume': 0.05,          # 流动性：5%
        }

        # 计算加权总分
        total_score = sum(scores[key] * weights[key] for key in weights)
        return round(total_score, 2)

    def screen_pairs(self, symbols: List[str], min_score: float = 60) -> List[Dict]:
        """
        批量筛选多个交易对

        Args:
            symbols: 待分析的交易对符号列表
            min_score: 最低评分阈值，低于此分数的交易对将被过滤

        Returns:
            按评分排序的交易对列表，包含各项指标和评分
        """
        results = []

        for symbol in symbols:
            print(f"正在分析 {symbol}...")

            try:
                # 获取市场数据
                df = self.get_market_data(symbol)

                if df.empty:
                    continue

                # 计算各项指标
                metrics = self.calculate_grid_metrics(df)
                if not metrics:
                    continue

                metrics['symbol'] = symbol

                # 计算综合评分
                score = self.calculate_score(metrics)
                metrics['total_score'] = score

                # 过滤低分交易对
                if score >= min_score:
                    results.append(metrics)

                # 避免请求过于频繁
                time.sleep(0.1)

            except Exception as e:
                print(f"分析 {symbol} 时出错: {e}")
                continue

        # 按评分降序排序
        results.sort(key=lambda x: x['total_score'], reverse=True)

        return results

    def get_popular_pairs(self) -> List[str]:
        """
        获取热门交易对列表

        Returns:
            USDT交易对列表，优先返回主流币种
        """
        try:
            markets = self.exchange.load_markets()

            # 筛选活跃的USDT现货交易对
            usdt_pairs = [
                symbol for symbol, market in markets.items()
                if symbol.endswith('/USDT') and market['spot'] and market['active']
            ]

            # 优先分析主流交易对
            # priority_pairs = [
            #     'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'ADA/USDT', 'SOL/USDT',
            #     'XRP/USDT', 'DOT/USDT', 'DOGE/USDT', 'AVAX/USDT', 'MATIC/USDT',
            #     'LINK/USDT', 'LTC/USDT', 'BCH/USDT', 'UNI/USDT', 'ATOM/USDT',
            # ]

            # 返回存在于市场中的优先交易对
            return usdt_pairs
            # return [pair for pair in priority_pairs if pair in usdt_pairs]

        except Exception as e:
            print(f"获取市场交易对时出错: {e}")
            return []

    def export_results(self, results: List[Dict], filename: str = 'grid_screening_results.json'):
        """
        导出筛选结果到JSON文件

        Args:
            results: 筛选结果列表
            filename: 输出文件名
        """
        # 添加时间戳和汇总信息
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'total_pairs_analyzed': len(results),
            'results': results
        }

        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)

        print(f"结果已导出到 {filename}")


def main():
    """
    主函数：运行网格交易筛选器
    """
    print("网格交易货币对筛选器")
    print("=" * 40)

    # 初始化筛选器
    screener = GridTradingScreener('binance')

    # 获取交易对
    print("获取交易对...")
    symbols = screener.get_popular_pairs()
    print(f"找到 {len(symbols)} 个交易对待分析")

    # 开始筛选
    print("\n开始分析...")
    results = screener.screen_pairs(symbols, min_score=50)

    # 显示结果
    print(f"\n找到 {len(results)} 个适合的交易对:")
    print("-" * 80)
    print(f"{'排名':<4} {'交易对':<12} {'评分':<6} {'交易次数':<12} {'成功率':<12} {'月收益率':<12}")
    print("-" * 80)

    for i, result in enumerate(results[:10], 1):
        print(f"{i:<4} {result['symbol']:<12} {result['total_score']:<6.1f} "
              f"{result['round_trips_30d']:<12.0f} {result['success_rate']:<12.1%} "
              f"{result['monthly_return_estimate']:<12.1%}")

    # 导出结果
    screener.export_results(results)

    print(f"\n分析完成！最佳交易对: {results[0]['symbol'] if results else '无'}")


if __name__ == "__main__":
    main()