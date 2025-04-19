import ccxt
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import proxies


def fetch_contract_trades(exchange, symbol, start_time, end_time):
    """
    Fetch contract trades data from Gate.io
    :param exchange: ccxt exchange instance
    :param symbol: trading pair symbol (e.g., 'AERGO/USDT:USDT')
    :param start_time: start time in milliseconds
    :param end_time: end time in milliseconds
    :return: DataFrame containing trades data
    """
    all_trades = []
    since = start_time
    
    print(f"Fetching trades from {datetime.fromtimestamp(start_time/1000)} to {datetime.fromtimestamp(end_time/1000)}")
    
    while since < end_time:
        try:
            print(f"Fetching trades since {datetime.fromtimestamp(since/1000)}...")
            trades = exchange.fetch_trades(symbol, since=since, limit=1000)
            
            if not trades:
                print("No more trades found")
                break
                
            print(f"Fetched {len(trades)} trades")
            # Print first and last trade timestamps
            if trades:
                print(f"First trade time: {datetime.fromtimestamp(trades[0]['timestamp']/1000)}")
                print(f"Last trade time: {datetime.fromtimestamp(trades[-1]['timestamp']/1000)}")
            
            all_trades.extend(trades)
            since = trades[-1]['timestamp'] + 1
            
            # Respect rate limits
            time.sleep(exchange.rateLimit / 1000)
            
        except Exception as e:
            print(f"Error fetching trades: {e}")
            break
    
    # Convert to DataFrame
    df = pd.DataFrame(all_trades)
    if not df.empty:
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        print(f"\nBefore filtering:")
        print(f"Total trades: {len(df)}")
        print(f"Time range: {df['datetime'].min()} to {df['datetime'].max()}")
        
        # Filter trades within time range
        df = df[df['timestamp'] <= end_time]
        df = df[df['timestamp'] >= start_time]
        df = df.sort_values('timestamp')
        
        print(f"\nAfter filtering:")
        print(f"Total trades: {len(df)}")
        if not df.empty:
            print(f"Time range: {df['datetime'].min()} to {df['datetime'].max()}")
    
    return df


def plot_trades(df, symbol):
    """
    Plot trades data
    :param df: DataFrame containing trades data
    :param symbol: trading pair symbol
    """
    if df.empty:
        print("No trades data to plot")
        return

    plt.figure(figsize=(15, 10))

    # Plot price over time
    plt.subplot(2, 1, 1)
    plt.plot(df['datetime'], df['price'], 'b-', label='Price')
    plt.title(f'{symbol} Trades Analysis')
    plt.ylabel('Price')
    plt.grid(True)
    plt.legend()

    # Plot volume over time
    plt.subplot(2, 1, 2)
    plt.bar(df['datetime'], df['amount'], color='g', alpha=0.6, label='Volume')
    plt.xlabel('Time')
    plt.ylabel('Volume')
    plt.grid(True)
    plt.legend()

    plt.tight_layout()
    plt.show()


def main():
    # Initialize exchange
    exchange = ccxt.gateio({
        'proxies': proxies,
        'options': {
            'defaultType': 'swap',  # Use swap (contracts) market
        }
    })
    
    # Load markets to verify symbol
    print("Loading markets...")
    markets = exchange.load_markets()
    print("Available markets loaded")
    
    # Parameters
    symbol = 'AERGO/USDT:USDT'  # Contract symbol
    
    # Get current time in UTC
    now = datetime.utcnow()
    print(f"Current UTC time: {now}")
    
    # Set time range for today
    start_time = int(datetime(now.year, now.month, now.day, 7, 59, 0).timestamp() * 1000)
    end_time = int(datetime(now.year, now.month, now.day, 8, 1, 0).timestamp() * 1000)
    
    print(f"Symbol: {symbol}")
    print(f"Start time: {datetime.fromtimestamp(start_time/1000)}")
    print(f"End time: {datetime.fromtimestamp(end_time/1000)}")
    
    # Verify symbol exists
    if symbol not in markets:
        print(f"Symbol {symbol} not found in available markets")
        print("Available symbols:")
        for s in markets:
            if 'AERGO' in s:
                print(s)
        return
    
    # Fetch trades data
    print(f"Fetching trades for {symbol}...")
    df = fetch_contract_trades(exchange, symbol, start_time, end_time)
    
    if not df.empty:
        print(f"\nTotal trades: {len(df)}")
        print(f"Time range: {df['datetime'].min()} to {df['datetime'].max()}")
        print(f"Price range: {df['price'].min()} to {df['price'].max()}")
        print(f"Total volume: {df['amount'].sum():.2f}")
        
        # Plot the data
        plot_trades(df, symbol)
    else:
        print("No trades data found for the specified time range")


if __name__ == "__main__":
    main()
