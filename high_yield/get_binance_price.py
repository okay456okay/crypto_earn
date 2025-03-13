import requests
import sys
import os

# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将 config.py 所在的目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))
from config import proxies
from tools.logger import logger


def get_binance_price(symbol, proxies=proxies):
    """
    获取币安交易所指定交易对的当前价格

    参数:
    symbol (str): 交易对符号，例如 'BTCUSDT', 'ETHUSDT', 'BNBUSDT' 等

    返回:
    float: 当前价格
    str: 错误信息（如果有错误）

    示例:
    >>> price = get_binance_price('BTCUSDT')
    >>> logger.info(price)
    56789.12
    """
    try:
        # 币安API的价格接口URL
        url = "https://api.binance.com/api/v3/ticker/price"

        # 准备请求参数
        params = {
            "symbol": symbol.upper()  # 确保符号为大写
        }

        # 发送GET请求
        response = requests.get(url, params=params, proxies=proxies)

        # 检查响应状态码
        if response.status_code == 200:
            data = response.json()
            # 将价格从字符串转换为浮点数
            price = float(data["price"])
            return price
        else:
            return f"错误: API返回状态码 {response.status_code}"

    except requests.exceptions.RequestException as e:
        return f"网络请求错误: {str(e)}"
    except ValueError as e:
        return f"JSON解析错误: {str(e)}"
    except KeyError as e:
        return f"数据结构错误: {str(e)}"
    except Exception as e:
        return f"未知错误: {str(e)}"

# 使用示例
if __name__ == "__main__":
    # 获取比特币对USDT的价格
    btc_price = get_binance_price("BTCUSDT")
    logger.info(f"BTC/USDT 当前价格: {btc_price}")

    # 获取以太坊对USDT的价格
    eth_price = get_binance_price("ETHUSDT")
    logger.info(f"ETH/USDT 当前价格: {eth_price}")