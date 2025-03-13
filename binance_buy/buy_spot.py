# coding=utf-8
"""
Binance涨停秒下单脚本, 脚本会校对时间、在指定时间下单
准备工作：
1. Binance API设置API key、API密钥、API IP白名单、交易对白名单
2. 设置交易参数，秒杀时间：
symbol = "REDUSDT"
price = 0.8
quantity = 4999
target_time = datetime.datetime(2025, 3, 2, 20, 0, 0, 0)
3. 注意账户中不要有任何该交易对的余额。如果有，则需要将quantity调低一点
"""
import time
import datetime
import requests
from binance.client import Client
from binance.exceptions import BinanceAPIException
import random
import sys
import os

# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将 config.py 所在的目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))

from config import binance_api_key, binance_api_secret, proxies
from tools.logger import logger

# 设置SOCKS5代理
# socks.set_default_proxy(socks.SOCKS5, "127.0.0.1", 7890)
# socket.socket = socks.socksocket

# 针对requests库配置代理

# session = requests.Session()
# session.proxies.update(proxies)
# r = session.get('https://www.google.com/')
# logger.info(r.status_code, r.text)
# r = session.get('https://api.binance.com/')
# logger.info(r.status_code, r.text)


# 交易参数
symbol = "REDUSDT"
price = 1.0
quantity = 4999
target_time = datetime.datetime(2025, 3, 4, 23, 0, 0, 0)
order_type = Client.ORDER_TYPE_LIMIT
time_in_force = Client.TIME_IN_FORCE_GTC  # 成交为止


def place_order():
    """尝试下单购买RED/USDT"""
    try:
        order = client.create_order(
            symbol=symbol,
            side=Client.SIDE_BUY,
            type=order_type,
            timeInForce=time_in_force,
            quantity=quantity,
            price=f"{price:.8f}"
        )
        logger.info(f"订单成功提交! 订单ID: {order['orderId']}")
        logger.info(f"订单详情: {order}")
        return True
    except BinanceAPIException as e:
        logger.info(f"订单提交失败: {e}")
        return False
    except Exception as e:
        logger.info(f"发生未知错误: {e}")
        return False



def get_binance_server_time():
    """直接从Binance获取服务器时间"""
    try:
        server_time = client.get_server_time()
        timestamp = server_time['serverTime'] / 1000.0
        logger.info(f"已同步Binance服务器时间: {datetime.datetime.fromtimestamp(timestamp)}")
        return timestamp
    except Exception as e:
        logger.info(f"无法获取Binance服务器时间: {e}")
        return None


def sync_time():
    """尝试多种方法同步时间"""
    # 首先尝试从Binance获取时间（最准确的方法，因为我们最终要与Binance服务器通信）
    binance_time = get_binance_server_time()
    if binance_time:
        return binance_time

    # 多个时间服务API备选
    time_apis = [
        "https://worldtimeapi.org/api/ip",
        "https://timeapi.io/api/Time/current/zone?timeZone=UTC",
        "https://www.timeapi.io/api/Time/current/zone?timeZone=UTC",
        "https://showcase.api.linx.twenty57.net/UnixTime/tounix?date=now"
    ]

    # 随机打乱顺序，避免总是请求同一个服务
    random.shuffle(time_apis)

    for api_url in time_apis:
        try:
            logger.info(f"尝试从 {api_url} 同步时间...")
            response = requests.get(api_url, timeout=5, proxies=proxies)

            if response.status_code == 200:
                data = response.json()

                # 不同API返回格式不同，需要分别处理
                if "datetime" in data:  # worldtimeapi.org
                    server_time = datetime.datetime.fromisoformat(data['datetime'].replace('Z', '+00:00'))
                    return server_time.timestamp()
                elif "dateTime" in data:  # timeapi.io
                    time_str = data['dateTime']
                    server_time = datetime.datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    return server_time.timestamp()
                elif "UnixTime" in data:  # twenty57
                    return int(data["UnixTime"]) / 1000.0

                logger.info(f"从 {api_url} 成功同步时间")
                return time.time()  # 如果无法解析数据，使用本地时间

        except Exception as e:
            logger.info(f"从 {api_url} 同步时间失败: {e}")

    # 所有方法都失败，使用本地时间
    logger.info("所有时间同步方法都失败，使用本地时间")
    return time.time()


def get_proxy_ip(proxies=proxies):
    """获取代理的出口IP"""
    try:
        response = requests.get('https://api.ipify.org?format=json',
                                proxies=proxies, timeout=10)
        if response.status_code == 200:
            proxy_ip = response.json()['ip']
            logger.info(f"获取到外网ip为： {proxy_ip}")
            return proxy_ip
        return "无法获取"
    except Exception as e:
        logger.info(f"获取代理IP失败: {e}")
        return "无法获取"


def main(target_time=target_time):
    # 获取目标时间
    target_timestamp = target_time.timestamp()

    # 提前50毫秒(0.05秒)
    execution_timestamp = target_timestamp - 0.05

    logger.info(f"目标执行时间: {target_time}")

    # 最后一次同步时间
    last_sync_time = 0

    while True:
        # 每60秒同步一次时间，避免频繁请求
        current_time = time.time()
        if current_time - last_sync_time > 60:
            current_time = sync_time()
            last_sync_time = time.time()
        else:
            # 使用本地时间 + 上次同步的偏移量
            time_offset = current_time - last_sync_time
            current_time = sync_time() + time_offset

        # 打印倒计时信息（每5秒一次）
        remaining = target_timestamp - current_time
        if remaining > 0 and remaining % 5 < 0.1:
            logger.info(f"距离执行还有 {remaining:.2f} 秒")

        # 当前时间到达或超过执行时间点
        if current_time >= execution_timestamp:
            logger.info("开始执行下单...")

            # 循环尝试下单，直到成功
            while True:
                success = place_order()
                if success:
                    logger.info("订单执行完成!")
                    return
                else:
                    logger.info("10ms后重试...")
                    time.sleep(0.02)  # 等待20毫秒

        # 距离目标时间还较远时，睡眠时间更长
        if target_timestamp - current_time > 60:
            time.sleep(10)  # 提前1分钟以上，每10秒检查一次
        elif target_timestamp - current_time > 10:
            time.sleep(1)  # 提前10-60秒，每秒检查一次
        else:
            time.sleep(0.01)  # 最后10秒，每10毫秒检查一次


if __name__ == "__main__":
    # 首先检查API连接与账户余额
    try:
        logger.info(f"准备在 {target_time} 开始购买RED/USDT...")
        logger.info(f"使用代理: {proxies}")

        # 尝试获取外网出口IP
        proxy_ip = get_proxy_ip()
        logger.info(f"当前外网出口IP: {proxy_ip}")
        logger.info("请确保此IP已添加到Binance API白名单中")

        # 初始化Binance客户端 - 使用代理设置
        if proxies:
            client = Client(binance_api_key, binance_api_secret, {'proxies': proxies})
        else:
            client = Client(binance_api_key, binance_api_secret)
        # 设置请求超时时间，考虑代理可能增加的延迟
        client.session.request_timeout = 20

        logger.info("正在通过代理连接Binance...")
        # 检查API连接
        server_time = client.get_server_time()
        logger.info(f"Binance服务器连接成功! 服务器时间: {datetime.datetime.fromtimestamp(server_time['serverTime'] / 1000)}")

        # 检查USDT余额是否足够
        balance = client.get_asset_balance(asset='USDT')
        usdt_balance = float(balance['free'])
        required_usdt = price * quantity

        logger.info(f"当前USDT余额: {usdt_balance}")
        logger.info(f"本次交易需要: {required_usdt} USDT")

        if usdt_balance < required_usdt:
            logger.info(f"警告: USDT余额不足! 需要至少 {required_usdt} USDT")
            decision = input("是否仍要继续? (y/n): ")
            if decision.lower() != 'y':
                logger.info("程序终止")
                exit()

        # 开始主程序
        main()
    except Exception as e:
        logger.info(f"初始化检查失败: {e}")
        logger.info("请检查代理连接、API密钥和网络连接状态")