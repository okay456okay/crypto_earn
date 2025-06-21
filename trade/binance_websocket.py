import asyncio
import sqlite3
import json
from datetime import datetime
from binance import AsyncClient, BinanceSocketManager
from config import binance_api_key, binance_api_secret, proxies


def init_database():
    """初始化数据库和表结构"""
    conn = sqlite3.connect('trading_records.db')
    cursor = conn.cursor()
    
    # 创建合约标记价格表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS binance_mark_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            mark_price REAL NOT NULL,
            index_price REAL,
            estimated_settle_price REAL,
            funding_rate REAL,
            next_funding_time INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, event_time)
        )
    ''')
    
    # 创建索引以提高查询性能
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_time ON binance_mark_prices(symbol, event_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON binance_mark_prices(created_at)')
    
    conn.commit()
    conn.close()
    print("数据库初始化完成")


def save_mark_price_data(data_list):
    """保存标记价格数据到数据库"""
    conn = sqlite3.connect('trading_records.db')
    cursor = conn.cursor()
    
    saved_count = 0
    for item in data_list:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO binance_mark_prices 
                (event_time, symbol, mark_price, index_price, 
                 estimated_settle_price, funding_rate, next_funding_time)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                item.get('E'),  # event_time
                item.get('s'),  # symbol
                float(item.get('p', 0)),  # mark_price
                float(item.get('i', 0)) if item.get('i') else None,  # index_price
                float(item.get('P', 0)) if item.get('P') else None,  # estimated_settle_price
                float(item.get('r', 0)) if item.get('r') else None,  # funding_rate
                item.get('T')  # next_funding_time
            ))
            saved_count += 1
        except Exception as e:
            print(f"保存数据失败 {item.get('s', 'unknown')}: {e}")
    
    conn.commit()
    conn.close()
    
    if saved_count > 0:
        print(f"成功保存 {saved_count} 条标记价格数据到数据库")


async def main():
    # 初始化数据库
    init_database()
    
    client = await AsyncClient.create(
        api_key=binance_api_key,
        api_secret=binance_api_secret,
        https_proxy=proxies.get('https')
    )
    bsm = BinanceSocketManager(client)
    # start any sockets here, i.e a trade socket
    all_mark_price = bsm.all_mark_price_socket()
    # then start receiving messages
    async with all_mark_price as amp_cm:
        print("开始接收Binance标记价格数据...")
        while True:
            try:
                res = await amp_cm.recv()
                """
                {'stream': '!markPrice@arr@1s', 'data': [
                    {
                    "e": "markPriceUpdate",  	// Event type
                    "E": 1562305380000,      	// Event time
                    "s": "BTCUSDT",          	// Symbol
                    "p": "11185.87786614",   	// Mark price
                    "i": "11784.62659091"		// Index price
                    "P": "11784.25641265",		// Estimated Settle Price, only useful in the last hour before the settlement starts
                    "r": "0.00030000",       	// Funding rate
                    "T": 1562306400000       	// Next funding time
                  }
                ]}
                """
                # 将数据保存到数据库
                if 'data' in res and isinstance(res['data'], list):
                    save_mark_price_data(res['data'])
                else:
                    print(f"接收到非预期数据格式: {res}")
                    
            except Exception as e:
                print(f"处理数据时发生错误: {e}")
                continue

    await client.close_connection()


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序运行出错: {e}")