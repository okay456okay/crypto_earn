#!/usr/bin/env python3
"""
Grid Trading Pair Screener
A tool to screen cryptocurrency pairs suitable for grid trading strategies
"""

import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import json
from typing import Dict, List, Tuple, Optional

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import binance_api_key, binance_api_secret, proxies


class GridTradingScreener:
    """
    Screen cryptocurrency pairs for grid trading suitability
    """

    def __init__(self, exchange_name: str = 'binance'):
        """
        Initialize the screener

        Args:
            exchange_name: Name of the exchange (default: binance)
        """
        self.exchange = getattr(ccxt, exchange_name)({
            'apiKey': binance_api_key,  # Add your API key if needed
            'secret': binance_api_secret,  # Add your secret if needed
            'sandbox': False,
            'rateLimit': 1200,
            'proxies': proxies,
        })

    def get_market_data(self, symbol: str, timeframe: str = '1h', days: int = 30) -> pd.DataFrame:
        """
        Get OHLCV data for a symbol

        Args:
            symbol: Trading pair symbol (e.g., 'BTC/USDT')
            timeframe: Timeframe for data ('1h', '4h', '1d')
            days: Number of days of historical data

        Returns:
            DataFrame with OHLCV data
        """
        try:
            since = self.exchange.milliseconds() - days * 24 * 60 * 60 * 1000
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, since)

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)

            return df
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()

    def calculate_grid_metrics(self, df: pd.DataFrame) -> Dict:
        """
        Calculate grid trading suitability metrics

        Args:
            df: OHLCV DataFrame

        Returns:
            Dictionary of calculated metrics
        """
        if df.empty or len(df) < 20:
            return {}

        prices = df['close']
        returns = prices.pct_change().dropna()

        # Basic statistics
        mean_price = prices.mean()
        price_std = prices.std()

        # Volatility metrics
        daily_volatility = returns.std() * np.sqrt(24)  # Assuming hourly data
        volatility_stability = returns.rolling(7 * 24).std().std()  # Volatility of volatility

        # Trend analysis
        def calculate_adx(df, period=14):
            """Simplified ADX calculation"""
            high, low, close = df['high'], df['low'], df['close']

            # True Range
            tr1 = high - low
            tr2 = abs(high - close.shift(1))
            tr3 = abs(low - close.shift(1))
            tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

            # Directional Movement
            dm_plus = np.where((high - high.shift(1)) > (low.shift(1) - low),
                               np.maximum(high - high.shift(1), 0), 0)
            dm_minus = np.where((low.shift(1) - low) > (high - high.shift(1)),
                                np.maximum(low.shift(1) - low, 0), 0)

            # ADX calculation (simplified)
            di_plus = 100 * pd.Series(dm_plus).rolling(period).mean() / tr.rolling(period).mean()
            di_minus = 100 * pd.Series(dm_minus).rolling(period).mean() / tr.rolling(period).mean()

            dx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)
            adx = dx.rolling(period).mean()

            return adx.iloc[-1] if not adx.empty else 50

        adx = calculate_adx(df)

        # Linear regression trend
        x = np.arange(len(prices))
        slope, intercept = np.polyfit(x, prices, 1)
        r_squared = np.corrcoef(x, prices)[0, 1] ** 2

        # Grid simulation
        grid_results = self.simulate_grid_trading(df)

        metrics = {
            'symbol': '',  # Will be set by caller
            'mean_price': mean_price,
            'price_std': price_std,
            'daily_volatility': daily_volatility,
            'volatility_stability': volatility_stability,
            'adx': adx,
            'trend_slope': slope,
            'trend_r_squared': r_squared,
            'round_trips_30d': grid_results['round_trips'],
            'success_rate': grid_results['success_rate'],
            'monthly_return_estimate': grid_results['monthly_return'],
            'breakout_risk': grid_results['breakout_risk'],
            'in_range_time': grid_results['in_range_time'],
            'volume_24h': df['volume'].tail(24).sum(),
        }

        return metrics

    def simulate_grid_trading(self, df: pd.DataFrame,
                              std_multiplier: float = 1.0) -> Dict:
        """
        Simulate simplified grid trading (buy low, sell high)

        Args:
            df: OHLCV DataFrame
            std_multiplier: Standard deviation multiplier for grid range

        Returns:
            Dictionary with simulation results
        """
        if df.empty:
            return {'round_trips': 0, 'success_rate': 0, 'monthly_return': 0,
                    'breakout_risk': 1, 'in_range_time': 0}

        prices = df['close']
        mean_price = prices.mean()
        price_std = prices.std()

        # Calculate grid bounds
        upper_bound = mean_price + std_multiplier * price_std
        lower_bound = mean_price - std_multiplier * price_std

        # Ensure minimum range
        range_pct = (upper_bound - lower_bound) / mean_price
        if range_pct < 0.03:  # Minimum 3% range
            upper_bound = mean_price * 1.015
            lower_bound = mean_price * 0.985

        # Simulate trading
        position = 0  # 0 = no position, 1 = holding
        round_trips = 0
        buy_signals = 0
        successful_sells = 0

        for price in prices:
            if position == 0 and price <= lower_bound:
                # Buy signal
                position = 1
                buy_price = price
                buy_signals += 1
            elif position == 1 and price >= upper_bound:
                # Sell signal - successful round trip
                position = 0
                round_trips += 1
                successful_sells += 1

        # Calculate metrics
        success_rate = successful_sells / buy_signals if buy_signals > 0 else 0

        # Estimate monthly return
        if round_trips > 0:
            avg_profit_per_trip = (upper_bound - lower_bound) / lower_bound * 0.998  # Account for fees
            monthly_return = round_trips * avg_profit_per_trip * (30 / len(prices) * 24)  # Scale to monthly
        else:
            monthly_return = 0

        # Calculate breakout risk
        max_price = prices.max()
        min_price = prices.min()
        breakout_risk = max(
            (max_price - upper_bound) / upper_bound if upper_bound > 0 else 0,
            (lower_bound - min_price) / lower_bound if lower_bound > 0 else 0
        )

        # Time in range
        in_range_count = ((prices >= lower_bound) & (prices <= upper_bound)).sum()
        in_range_time = in_range_count / len(prices)

        return {
            'round_trips': round_trips,
            'success_rate': success_rate,
            'monthly_return': monthly_return,
            'breakout_risk': breakout_risk,
            'in_range_time': in_range_time,
            'upper_bound': upper_bound,
            'lower_bound': lower_bound,
        }

    def calculate_score(self, metrics: Dict) -> float:
        """
        Calculate overall suitability score for grid trading

        Args:
            metrics: Dictionary of calculated metrics

        Returns:
            Score between 0-100
        """
        if not metrics:
            return 0

        # Score components (0-100 each)
        scores = {}

        # Round trips score (more trips = better)
        scores['round_trips'] = min(100, metrics.get('round_trips_30d', 0) * 20)

        # Success rate score
        scores['success_rate'] = metrics.get('success_rate', 0) * 100

        # Monthly return score
        scores['monthly_return'] = min(100, metrics.get('monthly_return_estimate', 0) * 500)

        # Volatility score (target 3-6% daily volatility)
        vol = metrics.get('daily_volatility', 0)
        if 0.03 <= vol <= 0.06:
            scores['volatility'] = 100
        elif 0.02 <= vol <= 0.08:
            scores['volatility'] = 80
        else:
            scores['volatility'] = max(0, 80 - abs(vol - 0.045) * 1000)

        # Trend score (lower ADX = better for grid trading)
        adx = metrics.get('adx', 50)
        scores['trend'] = max(0, 100 - adx * 3)

        # Risk score (lower breakout risk = better)
        breakout_risk = metrics.get('breakout_risk', 1)
        scores['risk'] = max(0, 100 - breakout_risk * 300)

        # Volume score (higher volume = better)
        volume = metrics.get('volume_24h', 0)
        if volume >= 10_000_000:  # 10M+ USDT
            scores['volume'] = 100
        elif volume >= 1_000_000:  # 1M+ USDT
            scores['volume'] = 80
        else:
            scores['volume'] = min(80, volume / 1_000_000 * 80)

        # Weighted total score
        weights = {
            'round_trips': 0.25,
            'success_rate': 0.20,
            'monthly_return': 0.20,
            'volatility': 0.15,
            'trend': 0.10,
            'risk': 0.05,
            'volume': 0.05,
        }

        total_score = sum(scores[key] * weights[key] for key in weights)
        return round(total_score, 2)

    def screen_pairs(self, symbols: List[str], min_score: float = 60) -> List[Dict]:
        """
        Screen multiple trading pairs

        Args:
            symbols: List of trading pair symbols
            min_score: Minimum score threshold

        Returns:
            List of pairs with their metrics, sorted by score
        """
        results = []

        for symbol in symbols:
            print(f"Analyzing {symbol}...")

            try:
                # Get market data
                df = self.get_market_data(symbol)

                if df.empty:
                    continue

                # Calculate metrics
                metrics = self.calculate_grid_metrics(df)
                if not metrics:
                    continue

                metrics['symbol'] = symbol

                # Calculate score
                score = self.calculate_score(metrics)
                metrics['total_score'] = score

                # Filter by minimum score
                if score >= min_score:
                    results.append(metrics)

                # Rate limiting
                time.sleep(0.1)

            except Exception as e:
                print(f"Error analyzing {symbol}: {e}")
                continue

        # Sort by score (descending)
        results.sort(key=lambda x: x['total_score'], reverse=True)

        return results

    def get_popular_pairs(self) -> List[str]:
        """
        Get list of popular trading pairs for screening

        Returns:
            List of trading pair symbols
        """
        try:
            markets = self.exchange.load_markets()

            # Filter for USDT pairs with good volume
            usdt_pairs = [
                symbol for symbol, market in markets.items()
                if symbol.endswith('/USDT') and market['spot'] and market['active']
            ]

            # Focus on major pairs first
            priority_pairs = [
                'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'ADA/USDT', 'SOL/USDT',
                'XRP/USDT', 'DOT/USDT', 'DOGE/USDT', 'AVAX/USDT', 'MATIC/USDT',
                'LINK/USDT', 'LTC/USDT', 'BCH/USDT', 'UNI/USDT', 'ATOM/USDT',
            ]

            # Return priority pairs that exist in the market
            return [pair for pair in priority_pairs if pair in usdt_pairs]

        except Exception as e:
            print(f"Error getting market pairs: {e}")
            return []

    def export_results(self, results: List[Dict], filename: str = 'grid_screening_results.json'):
        """
        Export screening results to JSON file

        Args:
            results: List of screening results
            filename: Output filename
        """
        # Add timestamp
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'total_pairs_analyzed': len(results),
            'results': results
        }

        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)

        print(f"Results exported to {filename}")


def main():
    """
    Main function to run the screener
    """
    print("Grid Trading Pair Screener")
    print("=" * 40)

    # Initialize screener
    screener = GridTradingScreener('binance')

    # Get popular pairs
    print("Getting popular trading pairs...")
    symbols = screener.get_popular_pairs()
    print(f"Found {len(symbols)} pairs to analyze")

    # Screen pairs
    print("\nStarting analysis...")
    results = screener.screen_pairs(symbols, min_score=50)

    # Display results
    print(f"\nFound {len(results)} suitable pairs:")
    print("-" * 80)
    print(f"{'Rank':<4} {'Symbol':<12} {'Score':<6} {'Round Trips':<12} {'Success Rate':<12} {'Monthly Return':<12}")
    print("-" * 80)

    for i, result in enumerate(results[:10], 1):
        print(f"{i:<4} {result['symbol']:<12} {result['total_score']:<6.1f} "
              f"{result['round_trips_30d']:<12.0f} {result['success_rate']:<12.1%} "
              f"{result['monthly_return_estimate']:<12.1%}")

    # Export results
    screener.export_results(results)

    print(f"\nAnalysis complete! Top pair: {results[0]['symbol'] if results else 'None'}")


if __name__ == "__main__":
    main()