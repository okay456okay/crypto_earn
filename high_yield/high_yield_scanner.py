# coding=utf-8
"""
通过套保策略，实现现货和空单合约对冲，然后用现货购买高收益率产品，赚取收益。
该策略更适用于牛市，因为赚取的收益如果为非稳定币，随着价格下跌，则U本位的收益率会下跌
"""
import requests
import time
import schedule
from datetime import datetime
import sys
import os
import ccxt
from high_yield.sell_notify import TokenManager

# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将 config.py 所在的目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))

from binance_buy.buy_spot import get_proxy_ip
from config import binance_api_secret, binance_api_key, proxies, logger, bitget_api_key, bitget_api_secret, \
    bitget_api_passphrase, leverage_ratio, yield_percentile, min_apy_threshold


# import json
def get_percentile(data, percentile=yield_percentile, reverse=True):
    """
    手动计算数组的P95值

    参数:
    data -- 数字列表

    返回:
    p95 -- 95百分位数值
    """
    # 排序数据
    sorted_data = sorted(data, reverse=reverse)
    # 计算位置 (使用最近秩方法)
    n = len(sorted_data)
    position = int(percentile / 100 * n)
    # 如果位置是整数，取该位置的值
    if position < n:
        return sorted_data[position]
    # 如果我们恰好落在最后一个元素位置之外，返回最后一个元素
    return sorted_data[-1]


# 企业微信群机器人类
class WeChatWorkBot:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_message(self, content, mentioned_list=None):
        """发送企业微信群机器人消息"""
        data = {
            "msgtype": "text",
            "text": {
                "content": content,
            }
        }

        if mentioned_list:
            data["text"]["mentioned_list"] = mentioned_list

        try:
            logger.info(f"开始发送企微消息，webhook_url: {self.webhook_url}, data: {data}")
            response = requests.post(self.webhook_url, json=data)
            result = response.json()

            if result["errcode"] == 0:
                logger.info("企业微信群消息发送成功")
                return True
            else:
                logger.error(f"企业微信群消息发送失败: {result}")
                return False
        except Exception as e:
            logger.error(f"发送企业微信群消息时出错: {str(e)}")
            return False


# 交易所API类
class ExchangeAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.session.proxies.update(proxies)

    def get_binance_flexible_products(self):
        """
        获取币安活期理财产品 - 使用更新的API
        :return [{'exchange': 'Binance', 'token': 'AUCTION', 'apy': 25.573329, 'min_purchase': 0.01, 'max_purchase': 50280.0}]
        """
        try:
            # 新的Binance API接口
            url = "https://www.binance.com/bapi/earn/v1/friendly/finance-earn/simple-earn/homepage/details"
            params = {
                "pageSize": 100,
                "pageIndex": 1,
                "includeEthStaking": True,
                "includeSolStaking": True,
                "includeP2pLoan": True,
                "orderBy": "APY_DESC",
                "simpleEarnType": "ALL",
            }
            response = self.session.get(url, params=params)

            # 记录响应状态码和响应文本的前100个字符用于调试
            if response.status_code != 200:
                logger.info(f"Binance API响应状态码: {response.status_code}, error: {response.text}")
            # logger.info(f"Binance API响应内容前100个字符: {response.text[:100] if response.text else 'Empty'}")

            data = response.json()

            # 检查新API的返回结构
            if "data" in data and isinstance(data["data"]['list'], list):
                products = []
                for item in data["data"]['list']:
                    # 适配新的API返回结构
                    if int(item['duration']) == 0:
                        product = {
                            "exchange": "Binance",
                            "token": item.get("asset", ""),
                            "apy": float(item.get("highestApy", 0)) * 100,
                            'apy_percentile': float(item.get("highestApy", 0)) * 100,
                            "min_purchase": float(item.get('productDetailList', [])[0].get("minPurchaseAmount", 0)),
                            "max_purchase": float(
                                item.get('productDetailList', [])[0].get("maxPurchaseAmountPerUser", 0))
                        }
                        products.append(product)
                return products
        except Exception as e:
            logger.error(f"获取Binance活期理财产品时出错: {str(e)}")
            # 尝试备用API接口
            return []

    def get_bitget_flexible_products(self):
        """
        获取Bitget活期理财产品
        [{'exchange': 'Binance', 'token': 'AUCTION', 'apy': 25.573329, 'min_purchase': 0.01, 'max_purchase': 50280.0}
        """
        products = []
        try:
            exchange = ccxt.bitget({
                'apiKey': bitget_api_key,
                'secret': bitget_api_secret,
                'password': bitget_api_passphrase,
            })
            exchange.proxies = proxies
            data = exchange.private_earn_get_v2_earn_savings_product()

            if data["code"] == "00000" and "data" in data:
                products = []
                for item in data["data"]:
                    if item['periodType'] == 'flexible' and item['status'] == 'in_progress':
                        product = {
                            "exchange": "Bitget",
                            "token": item["coin"],
                            "apy": float(item['apyList'][0]["currentApy"]),
                            "apy_percentile": float(item['apyList'][0]["currentApy"]),
                            "min_purchase": int(float(item['apyList'][0]['minStepVal'])),
                            "max_purchase": int(float(item['apyList'][0]['maxStepVal'])),
                        }
                        products.append(product)
            else:
                logger.error(f"Bitget API返回错误: {data}")
        except Exception as e:
            logger.error(f"获取Bitget活期理财产品时出错: {str(e)}")
        return products

    def get_bybit_flexible_products(self):
        """
        获取Bybit活期理财产品
        https://bybit-exchange.github.io/docs/zh-TW/v5/earn/product-info
        """
        products = []
        try:
            # https://api.bybit.com/v5/earn/product?category=FlexibleSaving
            url = "https://api.bybit.com/v5/earn/product"
            params = {
                "category": "FlexibleSaving",
            }
            logger.info(f"开始获取bybit储蓄产品")
            response = self.session.get(url, params=params)
            data = response.json()

            if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
                for item in data["result"]["list"]:
                    token = item["coin"]
                    apy = float(item["estimateApr"].replace("%", ""))
                    if item['status'] != 'Available':
                        continue
                    try:
                        if apy >= min_apy_threshold:
                            response = requests.post(
                                url="https://api2.bybit.com/s1/byfi/get-flexible-saving-apr-history",
                                json={"product_id": item['productId']},
                                headers={"Content-Type": "application/json"},
                                proxies=proxies
                            )
                            data = response.json().get('result', {}).get('hourly_apr_list', [])
                            data = [int(i['apr_e8']) / 1000000 for i in data]
                            logger.info(f"获取bybit {token}近24小时收益率曲线, 数据：{data}")
                            apy_percentile = get_percentile(data, percentile=yield_percentile, reverse=True)
                        else:
                            apy_percentile = apy
                    except Exception as e:
                        logger.error(f"获取 {token}的收益曲线失败： {str(e)}")
                        apy_percentile = 0
                    product = {
                        "exchange": "Bybit",
                        "token": item["coin"],
                        "apy": float(item["estimateApr"].replace("%", "")),
                        'apy_percentile': apy_percentile,
                        "min_purchase": float(item.get('minStakeAmount', 0)),
                        "max_purchase": float(item.get('maxStakeAmount', 0))
                    }
                    products.append(product)
            else:
                logger.error(f"Bybit API返回错误: {data}")
        except Exception as e:
            logger.error(f"获取Bybit活期理财产品时出错: {str(e)}")
        return products

    def get_gateio_flexible_products(self):
        """
        获取GateIO活期理财产品
        https://www.gate.io/docs/developers/apiv4/zh_CN/#earnuni
        """
        products = []
        try:
            # self.session.get("https://www.gate.io/zh/simple-earn")
            # url = "https://www.gate.io/apiw/v2/uni-loan/earn/market/list?sort_type=3&available=false&limit=7&have_balance=0&have_award=0&is_subscribed=0&page=1"
            url = "https://www.gate.io/apiw/v2/uni-loan/earn/market/list"
            params = {
                "sort_type": 3,
                "available": True,
                "limit": 50,
                "page": 1,
            }
            headers = {
                # 'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
                # "Content-Type": "application/json",
                # "Accept": "application/json, text/plain, */*'",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                # "referer": "https://www.gate.io/zh/simple-earn",
                'sec-fetch-site': 'same-origin',
            }
            cookies = {
                'lang': 'cn',
                'exchange_rate_switch': '1',
            }
            response = requests.get(url, params=params, headers=headers, cookies=cookies, proxies=proxies)
            data = response.json()

            if data["code"] == 0 and "data" in data and "list" in data["data"]:
                end = int(datetime.now().replace(microsecond=0, second=0, minute=0).timestamp())
                start = end - 1 * 24 * 60 * 60
                for item in data["data"]["list"]:
                    token = item["asset"]
                    try:
                        url = f'https://www.gate.io/apiw/v2/uni-loan/earn/chart?from={start}&to={end}&asset={token}&type=1'
                        logger.info(f"get gateio {token}近1天收益率曲线, url: {url}")
                        asset_chart = requests.get(
                            url=url,
                            proxies=proxies)
                        data = asset_chart.json()
                        apy_percentile = get_percentile([float(i['value']) for i in data.get('data', [])],
                                                        percentile=yield_percentile, reverse=True)
                    except Exception as e:
                        logger.error(f"get asset chart {item['asset']} error: {str(e)}")
                        apy_percentile = 0
                    product = {
                        "exchange": "GateIO",
                        "token": token,
                        "apy": float(item["next_time_rate_year"]) * 100,
                        "apy_percentile": apy_percentile,
                        "min_purchase": float(item.get('total_lend_available', 0)),
                        "max_purchase": float(item.get('total_lend_amount', 0))
                    }
                    products.append(product)
            else:
                logger.error(f"GateIO API返回错误: {data}")
        except Exception as e:
            logger.error(f"获取GateIO活期理财产品时出错: {str(e)}")
        return products

    def get_okx_flexible_products(self):
        """
        获取OKX活期理财产品
        https://www.okx.com/zh-hans/earn/simple-earn
        """
        products = []
        try:
            now_timestamp_ms = int(time.time()*1000)
            url = f"https://www.okx.com/priapi/v1/earn/simple-earn/all-products?type=all&t={now_timestamp_ms}"
            response = requests.get(url, proxies=proxies)
            data = response.json()

            if data["code"] == 0 and "data" in data and "allProducts" in data["data"]:
                for item in data["data"]["allProducts"]['currencies']:
                    token = item["investCurrency"]["currencyName"]
                    toked_id = int(item['investCurrency']['currencyId'])
                    apy = float(item['rate']['rateNum']['value'][0])
                    if apy > min_apy_threshold:
                        try:
                            url = f'https://www.okx.com/priapi/v2/financial/rate-history?currencyId={toked_id}&t={now_timestamp_ms}'
                            logger.info(f"get okx {token}近1天收益率曲线, url: {url}")
                            headers = {
                                "accept": "application/json",
                                "content-type": "application/json",
                                "authorization": "eyJhbGciOiJIUzUxMiJ9.eyJqdGkiOiJleDExMDE3NDE2MjI3Mjc0NzhFRkZGQzc4Mzk1N0U0RDMwMVhWV0IiLCJ1aWQiOiJMZDlvSkMxdVVXQlA0bWJtbDROcWp3PT0iLCJzdGEiOjAsIm1pZCI6IkxkOW9KQzF1VVdCUDRtYm1sNE5xanc9PSIsInBpZCI6IlBUeUE4VzA5ekZVSkJHSjZZUk5HWXc9PSIsIm5kZSI6MCwiaWF0IjoxNzQxNjIyNzI3LCJleHAiOjE3NDI4MzIzMjcsImJpZCI6MCwiZG9tIjoid3d3Lm9reC5jb20iLCJlaWQiOjE0LCJpc3MiOiJva2NvaW4iLCJkaWQiOiJJMW9iM0FDOEdPcXdyeG1ETEhDd3JGU3RsYUZ4bjlRUGNobmtibnZWMDhQcktxUlJ4QjNSWXVrY3p1YzkvRzJuIiwibGlkIjoiTGQ5b0pDMXVVV0JQNG1ibWw0TnFqdz09IiwidWZiIjoiUFR5QThXMDl6RlVKQkdKNllSTkdZdz09IiwidXBiIjoiaUJyYTJWaE5va3lSaWh4aUovM3pFdz09Iiwia3ljIjoyLCJreWkiOiJzVmtQSHhqTUdvYWFzajZndFcxUHg3ZFRwQ1pLZzUvNktuMW14YWlyWkNsTzhxa2IxYkx0YWYySVJVS2tMN3hFN3lkRi9ZTkNHUVcvNXlpNFZCelQzUT09IiwiY3BrIjoiaEJ2M21IRmNvSURMblNyRnp0R1NOWkxPb1pTazVtQThIcFBwT0w4UTVOVUR4dDJVVVE1N3BtcCsxcXVCRFJ2bGlta3gyQk94b0M5OG11Vi85a2tPdnR5VjlacGk5NkFEdHpKRGdiS0FjVnoyb01xeE5taVpabko0Q284ZWUyS1hsYXZXOVpiK3FqNTJPVnJSbGNId0tkK1hVWFdheWJQVjRackRXb2F0SnU4PSIsInZlciI6MSwiY2x0IjoyfQ.PirV2tw9OJordjLO5xs82rPPfS3tK7dSlonOh7FJi-hbdemX7vrJ65sDo2IlyR70GR9R0qD-te8QUdPugo9SRA",
                                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
                            }
                            asset_chart = requests.get(
                                url=url,
                                headers=headers,
                                proxies=proxies)
                            data = asset_chart.json()
                            apy_percentile = get_percentile([float(i['rate'])*100 for i in data.get('data',{}).get('lastOneDayRates', {}).get('rates')])
                        except Exception as e:
                            logger.error(f"get asset chart {item['asset']} error: {str(e)}")
                            apy_percentile = 0
                        product = {
                            "exchange": "OKX",
                            "token": token,
                            "apy": apy,
                            "apy_percentile": apy_percentile,
                            "min_purchase": 0,
                            "max_purchase": 0,
                        }
                        products.append(product)
            else:
                logger.error(f"OKX API返回错误: {data}")
        except Exception as e:
            logger.error(f"获取OKX活期理财产品时出错: {str(e)}")
        return products

    def get_binance_futures(self, token):
        """
        获取币安合约资金费率
        :return {'fundingTime': 1741478400001, 'fundingRate': 0.0068709999999999995, 'markPrice': 2202.84}
        """
        exchange = 'Binance'
        try:
            # url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={token}"
            url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={token}"
            response = self.session.get(url)
            data = response.json()
            return {
                "exchange": exchange,
                "fundingTime": data['nextFundingTime'],
                "fundingRate": float(data["lastFundingRate"]) * 100,
                "markPrice": float(data["markPrice"]),
            }  # 转换为百分比
        except Exception as e:
            logger.error(f"获取{exchange} {token}合约资金费率时出错: {str(e)}")
            return {}

    def get_bitget_futures_funding_price(self, token):
        """
        获取交易对市价/指数/标记价格
        :return {'fundingTime': 1741478400001, 'fundingRate': 0.0068709999999999995, 'markPrice': 2202.84}
        """
        try:
            url = "https://api.bitget.com/api/v2/mix/market/symbol-price"
            params = {
                "symbol": f"{token}",
                "productType": "USDT-FUTURES",
            }
            response = self.session.get(url, params=params)
            data = response.json()

            if data["code"] == "00000" and "data" in data:
                return data["data"][0]["markPrice"]
            return None
        except Exception as e:
            logger.error(f"获取Bitget {token}下次资金费结算时间: {str(e)}")
            return None

    def get_bitget_futures_funding_time(self, token):
        """
        获取下次资金费结算时间
        :return {'fundingTime': 1741478400001, 'fundingRate': 0.0068709999999999995, 'markPrice': 2202.84}
        """
        try:
            url = "https://api.bitget.com/api/v2/mix/market/funding-time"
            params = {
                "symbol": f"{token}",
                "productType": "USDT-FUTURES",
            }
            response = self.session.get(url, params=params)
            data = response.json()

            if data["code"] == "00000" and "data" in data:
                return data["data"][0]["nextFundingTime"]
            return None
        except Exception as e:
            logger.error(f"获取Bitget {token}下次资金费结算时间: {str(e)}")
            return None

    def get_bitget_futures_funding_rate(self, token):
        """
        获取Bitget合约资金费率
        :return {'fundingTime': 1741478400001, 'fundingRate': 0.0068709999999999995, 'markPrice': 2202.84}
        """
        exchange = 'Bitget'
        funding_time = self.get_bitget_futures_funding_time(token)
        mark_price = self.get_bitget_futures_funding_price(token)
        try:
            url = "https://api.bitget.com/api/v2/mix/market/current-fund-rate"
            params = {
                "symbol": f"{token}",
                "productType": "USDT-FUTURES",
            }
            response = self.session.get(url, params=params)
            data = response.json()

            if data["code"] == "00000" and "data" in data:
                return {
                    "exchange": exchange,
                    'fundingTime': int(funding_time),
                    'fundingRate': float(data["data"][0]["fundingRate"]) * 100,
                    'markPrice': float(mark_price),
                }  # 转换为百分比
            return {}
        except Exception as e:
            logger.error(f"获取{exchange} {token}合约资金费率时出错: {str(e)}")
            return {}

    def get_bybit_futures_funding_rate(self, token):
        """
        获取Bybit合约资金费率
        :return {'fundingTime': 1741478400001, 'fundingRate': 0.0068709999999999995, 'markPrice': 2202.84}
        """
        exchange = 'Bybit'
        try:
            url = "https://api.bybit.com/v5/market/tickers"
            params = {
                "category": "linear",
                "symbol": f"{token}"
            }
            response = self.session.get(url, params=params)
            data = response.json()

            if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
                for item in data["result"]["list"]:
                    if "fundingRate" in item:
                        return {
                            "exchange": exchange,
                            'fundingTime': int(item["nextFundingTime"]),
                            'fundingRate': float(item["fundingRate"]) * 100,  # 转换为百分比
                            'markPrice': float(item["markPrice"]),
                        }
            return {}
        except Exception as e:
            logger.error(f"获取{exchange} {token}合约资金费率时出错: {str(e)}")
            return {}

    def get_gateio_futures_funding_rate(self, token):
        """
        获取GateIO合约资金费率
        :return {'fundingTime': 1741478400001, 'fundingRate': 0.0068709999999999995, 'markPrice': 2202.84}
        """
        exchange = 'GateIO'
        try:
            gate_io_token = token.replace('USDT', '_USDT')
            url = f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{gate_io_token}"
            response = self.session.get(url)
            data = response.json()
            return {
                "exchange": exchange,
                'fundingTime': int(data["funding_next_apply"]) * 1000,
                'fundingRate': float(data["funding_rate"]) * 100,  # 转换为百分比
                'markPrice': float(data["mark_price"]),
            }
        except Exception as e:
            logger.error(f"获取{exchange} {token}合约资金费率时出错: {str(e)}")
            return {}

    def get_okx_futures_funding_rate(self, token):
        """
        获取 OKX 合约资金费率
        :return {'fundingTime': 1741478400001, 'fundingRate': 0.0068709999999999995, 'markPrice': 2202.84}
        """
        exchange = 'OKX'
        symbol =  token.replace('USDT', '/USDT:USDT')
        try:
            # 初始化OKX交易所实例
            exchange = ccxt.okx({'proxies': proxies})

            # 获取当前价格
            ticker = exchange.fetch_ticker(symbol)
            current_price = ticker['last']

            # 获取资金费率
            funding_rate_info = exchange.fetch_funding_rate(symbol)
            funding_rate = funding_rate_info['fundingRate']
            next_funding_time = funding_rate_info['nextFundingTimestamp']
            return {
                'exchange': exchange,
                'fundingTime': next_funding_time,
                'fundingRate': float(funding_rate) * 100,
                'markPrice': float(current_price) * 100,
            }
        except Exception as e:
            logger.error(f"获取{exchange} {token}合约资金费率时出错: {str(e)}")
            return {}

# 主业务逻辑类
class CryptoYieldMonitor:
    def __init__(self, buy_webhook_url, sell_webhook_url, min_apy_threshold=min_apy_threshold):
        self.exchange_api = ExchangeAPI()
        self.buy_wechat_bot = WeChatWorkBot(buy_webhook_url)
        self.min_apy_threshold = min_apy_threshold  # 最低年化利率阈值 (%)
        self.notified_tokens = set()  # 已通知的Token集合，避免重复通知

    def get_futures_trading(self, token):
        """检查Token是否在任意交易所上线了合约交易，且交易费率为正"""
        results = []

        # 检查Binance
        try:
            binance_rate = self.exchange_api.get_binance_futures(token)
            logger.info(f"{token} Binance Perp info: {binance_rate}")
        except Exception as e:
            binance_rate = None
            logger.error(f"获取{token}的合约资金费率报错：: {str(e)}")

        try:
            bitget_rate = self.exchange_api.get_bitget_futures_funding_rate(token)
            logger.info(f"{token} Bitget Perp info: {bitget_rate}")
        except Exception as e:
            bitget_rate = None
            logger.error(f"获取Bitget {token}的合约资金费率报错：: {str(e)}")

        try:
            bybit_rate = self.exchange_api.get_bybit_futures_funding_rate(token)
            logger.info(f"{token} Bybit Perp info: {bybit_rate}")
        except Exception as e:
            bybit_rate = None
            logger.error(f"获取Bybit {token}的合约资金费率报错：: {str(e)}")

        try:
            gate_io_rate = self.exchange_api.get_gateio_futures_funding_rate(token)
            logger.info(f"{token} GateIO Perp info: {gate_io_rate}")
        except Exception as e:
            bybit_rate = None
            logger.error(f"获取GateIO {token}的合约资金费率报错：: {str(e)}")

        try:
            okx_rate = self.exchange_api.get_okx_futures_funding_rate(token)
            logger.info(f"{token} GateIO Perp info: {okx_rate}")
        except Exception as e:
            okx_rate = None
            logger.error(f"获取OKX {token}的合约资金费率报错：: {str(e)}")

        if binance_rate:
            binance_rate['exchange'] = 'Binance'
            results.append(binance_rate)

        # 检查Bitget
        if bitget_rate:
            results.append(bitget_rate)

        # 检查Bybit
        if bybit_rate:
            results.append(bybit_rate)

        if gate_io_rate:
            results.append(gate_io_rate)

        if okx_rate:
            results.append(okx_rate)

        return results

    def _send_high_yield_notifications(self, notifications):
        """发送企业微信群机器人通知"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        limit = 6
        for p in range(int(len(notifications) / limit) + 1):
            message = f"📊 加密货币高收益理财产品监控 ({now})\n\n"
            for idx, notif in enumerate(notifications[p*limit:(p+1)*limit], 1):
                message += (
                    f"{idx+p*limit}. {notif['token']}({notif['exchange']}) 💰\n"
                    f"   • 最新收益率: {notif['apy']:.2f}%\n"
                    f"   • P{yield_percentile}收益率: {notif['apy_percentile']:.2f}%\n"
                    f"   • 各交易所合约信息: \n{notif['future_info']}\n"
                    f"   • 最低购买量: {notif['min_purchase']}\n"
                    f"   • 最大购买量: {notif['max_purchase']}\n\n"
                )
            self.buy_wechat_bot.send_message(message)
        logger.info(f"已发送{len(notifications)}条高收益加密货币通知")

    def get_estimate_apy(self, apy, fundingRate, leverage_ratio=leverage_ratio):
        return 1 * leverage_ratio / (leverage_ratio + 1) * (apy + fundingRate * 3 * 365)

    def high_yield_filter(self, all_products):
        # 筛选年化利率高于阈值的产品
        high_yield_products = [p for p in all_products if p["apy"] >= self.min_apy_threshold]
        high_yield_products = sorted(high_yield_products, key=lambda x: x['apy'], reverse=True)
        logger.info(f"筛选出{len(high_yield_products)}个年化利率高于{self.min_apy_threshold}%的产品")

        if not high_yield_products:
            logger.info(f"未找到年化利率高于{self.min_apy_threshold}%的产品")
            return

        # 检查每个高收益产品是否满足合约交易条件
        high_yield_notifications = []

        for product in high_yield_products:
            token = product["token"]
            logger.info(f"检查Token {token} 的合约交易情况")

            # 检查合约交易条件
            perp_token = f"{token}USDT"
            futures_results = self.get_futures_trading(perp_token)
            logger.info(f"{perp_token} get future results: {futures_results}")
            positive_futures_results = [i for i in futures_results if
                                        i['fundingRate'] >= 0 and int(time.time()) - i[
                                            'fundingTime'] / 1000 < 24 * 60 * 60]
            estimate_apys = [i for i in futures_results if self.get_estimate_apy(product['apy_percentile'], i[
                'fundingRate']) > self.min_apy_threshold]
            logger.info(
                f"{perp_token} positive future results: {positive_futures_results}, current timestamp: {int(time.time())}")
            if estimate_apys and product['apy_percentile'] > self.min_apy_threshold:
                future_info_str = '\n'.join([
                    f"   • {i['exchange']}: 资金费率:{i['fundingRate']:.4f}%, 标记价格:{i['markPrice']:.4f}, 预估收益率: {self.get_estimate_apy(product['apy'], i['fundingRate']):.2f}%, P{yield_percentile}预估收益率: {self.get_estimate_apy(product['apy_percentile'], i['fundingRate']):.2f}%, {datetime.fromtimestamp(i['fundingTime'] / 1000)}"
                    for i in
                    futures_results])
                logger.info(f"Token {token} 满足合约交易条件: {futures_results}")
                # 生成通知内容
                notification = {
                    "exchange": product["exchange"],
                    "token": token,
                    "apy": product["apy"],
                    "apy_percentile": product["apy_percentile"],
                    "future_info": future_info_str,
                    "min_purchase": product["min_purchase"],
                    "max_purchase": product["max_purchase"],
                }
                high_yield_notifications.append(notification)

        # 发送通知
        if high_yield_notifications:
            logger.info(f"已添加{len(high_yield_notifications)}个Token到通知列表")
            self._send_high_yield_notifications(high_yield_notifications)

            # 每24小时清理一次通知记录，允许再次通知
            if len(self.notified_tokens) > 100:  # 避免无限增长
                self.notified_tokens.clear()
                logger.info("已清理通知记录")
        else:
            logger.info("未找到满足所有条件的产品")

    def check_tokens(self, tokens, all_products):
        for token in tokens:
            # 获取理财产品最新利率
            sell_wechat_bot = WeChatWorkBot(token['webhook_url'])
            product = [i for i in all_products if
                       i['exchange'] == token['spot_exchange'] and i['token'] == token['token']]
            if not product:
                # 发送未找到理财产品通知
                content = f"在{token['spot_exchange']}交易所中未找到 {token['token']} 理财产品"
                sell_wechat_bot.send_message(content)
                product = {'apy': 0.0, 'apy_percentile': 0.0, 'exchange': f'未在{token["spot_exchange"]}找到相关产品', 'token': token['token']}
            else:
                product = product[0]
            # 过滤资金费率和利率，如果满足条件就告警
            perp_token = f"{token['token']}USDT"
            futures_results = self.get_futures_trading(perp_token)
            token_future = [i for i in futures_results if i['exchange'] == token['future_exchange']]
            if token_future:
                token_future = token_future[0]
                estimate_apy = self.get_estimate_apy(product['apy'], token_future['fundingRate'])
                estimate_apy_percentile = self.get_estimate_apy(product['apy_percentile'], token_future['fundingRate'])
                future_info_str = '\n'.join([
                    f"   • {i['exchange']}: 资金费率:{i['fundingRate']:.4f}%, 标记价格:{i['markPrice']:.4f}, 预估收益率: {estimate_apy:.2f}%, P{yield_percentile}预估收益率: {estimate_apy_percentile:.2f}%, {datetime.fromtimestamp(i['fundingTime'] / 1000)}"
                    for i in
                    futures_results])
                # token_future['fundingRate'] < 0
                if product[
                    'apy'] < self.min_apy_threshold or estimate_apy < self.min_apy_threshold or estimate_apy_percentile < self.min_apy_threshold:
                    content = (
                        f"{product['exchange']}加密货币理财产品{product['token']} 卖出提醒\n"
                        f"最新收益率: {product['apy']}%\n"
                        f"P{yield_percentile}收益率: {product['apy_percentile']}%\n"
                        # f"持有仓位: {token['totalAmount']}\n"
                        f"各交易所资金费率: (套保交易所: {token['future_exchange']})\n"
                        f"{future_info_str}"
                    )
                    sell_wechat_bot.send_message(content)
            else:
                content = f"在{token['future_exchange']}交易所中未找到 {token['token']} 产品"
                sell_wechat_bot.send_message(content)

    def check_sell_strategy(self, all_products):
        try:
            # 对所有已购买产品做检查
            # purchased_tokens = [('Binance', 'HIVE'), ]
            # binance_earn_positions = get_binance_flexible_savings(binance_api_key, binance_api_secret, proxies)
            # for p in binance_earn_positions:
            #     if float(p.get('totalAmount', 0)) > 1:
            #         purchased_tokens.append({"exchange": 'Binance', "token": p.get('asset'),
            #                                  "totalAmount": float(p.get('totalAmount', 0.0))})
            token_manger = TokenManager()
            purchased_tokens = token_manger.query_tokens()
            logger.info(f"获取到的活期理财账户仓位如下：{purchased_tokens}")
            self.check_tokens(purchased_tokens, all_products)
        except Exception as e:
            logger.error(f"对所有已购买产品做检查失败 {e}")

    def run(self):
        # 尝试获取外网出口IP
        proxy_ip = get_proxy_ip()
        logger.info(f"当前外网出口IP: {proxy_ip}")
        logger.info("请确保此IP已添加到Binance API白名单中")

        """运行监控任务"""
        logger.info("开始检查高收益加密货币...")
        try:
            # 获取所有交易所的活期理财产品
            binance_products = self.exchange_api.get_binance_flexible_products()
            logger.info(f"从Binance获取到{len(binance_products)}个活期理财产品")

            gateio_products = self.exchange_api.get_gateio_flexible_products()
            logger.info(f"从GateIO获取到{len(gateio_products)}个活期理财产品")

            bitget_products = self.exchange_api.get_bitget_flexible_products()
            logger.info(f"从Bitget获取到{len(bitget_products)}个活期理财产品")

            bybit_products = self.exchange_api.get_bybit_flexible_products()
            logger.info(f"从Bybit获取到{len(bybit_products)}个活期理财产品")

            okx_products = self.exchange_api.get_okx_flexible_products()
            logger.info(f"从OKX获取到{len(okx_products)}个活期理财产品")

            # 合并所有产品
            all_products = binance_products + bitget_products + bybit_products + gateio_products + okx_products
            logger.info(f"总共获取到{len(all_products)}个活期理财产品")
            # 过滤和处理高收益理财产品
            self.high_yield_filter(all_products)
            self.check_sell_strategy(all_products)
        except Exception as e:
            logger.error(f"运行监控任务时发生错误: {str(e)}")


# 主程序入口
def main():
    # 企业微信群机器人webhook URL（请替换为您的实际webhook URL）
    buy_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=293071ec-9865-4e86-9e69-b48f1a12a83a"
    sell_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=38fd27ea-8569-4de2-9dee-4c4a4ffb77ed"

    monitor = CryptoYieldMonitor(buy_webhook_url, sell_webhook_url)
    # print(monitor.exchange_api.get_okx_flexible_products())
    # get_proxy_ip()
    # print(monitor.exchange_api.get_bitget_flexible_products())
    # exit()
    # print(monitor.exchange_api.get_bybit_futures_funding_rate('ETHUSDT'))
    # print(monitor.exchange_api.get_bitget_futures_funding_rate('ETHUSDT'))
    # exit()

    # 立即运行一次
    monitor.run()

    # 设置定时任务，每30分钟运行一次
    schedule.every(30).minutes.do(monitor.run)

    logger.info("加密货币高收益监控服务已启动，每30分钟检查一次...")

    # 保持程序运行
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
