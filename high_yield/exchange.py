import time
from datetime import datetime
from time import sleep

import ccxt
import requests
import os
import sys
# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将 config.py 所在的目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))

from config import proxies, buy_apy_threshold, yield_percentile, bitget_api_key, bitget_api_secret, \
    bitget_api_passphrase, okx_earn_insurance_keep_ratio
from tools.logger import logger
from high_yield.common import get_percentile


def is_gold_dog(sorted_data):
    """
    # 示例数据
    test_data = [
        {"time": 1742083800, "value": "82.35"},
        {"time": 1742086200, "value": "75.12"},
        {"time": 1742090400, "value": "350.50"}
    ]

    result = check_array_conditions(test_data)
    print(f"条件是否满足: {result}")  # 应该输出: 条件是否满足: True
    :param data:
    :return:
    """
    # 检查数组是否为空
    if not sorted_data:
        return False

    # 获取最后一个元素的value值
    last_value = float(sorted_data[-1]["value"])

    # 检查条件1：最后一个元素的value值是否大于300
    if last_value <= 300:
        return False

    # 检查条件2：前面所有元素的value值是否都小于100
    for item in sorted_data[:-1]:
        if float(item["value"]) >= 100:
            return False

    # 如果所有条件都满足，返回True
    return True


class ExchangeAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.session.proxies.update(proxies)
        self.binance_funding_info = {}

    def get_binance_flexible_products(self):
        """
        获取币安活期理财产品 - 使用更新的API
        https://www.binance.com/zh-CN/earn/simple-earn
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
            response = requests.get(url, params=params, proxies=proxies)

            # 记录响应状态码和响应文本的前100个字符用于调试
            if response.status_code != 200:
                logger.error(f"get binance flexible products failed, url:{url}, code:{response.status_code}, error: {response.text}")
            # logger.info(f"Binance API响应内容前100个字符: {response.text[:100] if response.text else 'Empty'}")

            data = response.json()

            # 检查新API的返回结构
            if "data" in data and isinstance(data["data"]['list'], list):
                products = []
                for item in data["data"]['list']:
                    # 适配新的API返回结构
                    if int(item['duration']) == 0:
                        prouct_id = item['productId']
                        apy = float(item.get("highestApy", 0)) * 100
                        apy_percentile = -1
                        startTime = int(time.time()*1000) - 30*24*60*60*1000
                        apy_month = []
                        try:
                            if apy > buy_apy_threshold:
                                url = f'https://www.binance.com/bapi/earn/v1/friendly/lending/daily/product/position-market-apr?productId={prouct_id}&startTime={startTime}'
                                response = requests.get(url, proxies=proxies)
                                if response.status_code == 200:
                                    apy_month = [{'timestamp': int(i['calcTime']), 'apy': float(i['marketApr'])*100} for i in response.json().get('data', {}).get('marketAprList', [])]
                                    apy_percentile = get_percentile([i['apy'] for i in apy_month[-24:]], yield_percentile)
                                else:
                                    logger.error(
                                        f"binance get asset charts, url: {url}, status: {response.status_code}, response: {response.text}")
                        except Exception as e:
                            logger.error(f"binance get asset charts, url: {url}, error: {str(e)}")
                        product = {
                            "exchange": "Binance",
                            "token": item.get("asset", ""),
                            "apy": apy,
                            'apy_percentile': apy_percentile,
                            'apy_month': apy_month,
                            "min_purchase": float(item.get('productDetailList', [])[0].get("minPurchaseAmount", 0)),
                            "max_purchase": float(item.get('productDetailList', [])[0].get("maxPurchaseAmountPerUser", 0)),
                            "note": '',
                        }
                        products.append(product)
                        sleep(0.1)
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
                            "apy_percentile": -1.0,
                            'apy_month': [],
                            "min_purchase": int(float(item['apyList'][0]['minStepVal'])),
                            "max_purchase": int(float(item['apyList'][0]['maxStepVal'])),
                            "note": '',
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
            if response.status_code != 200:
                logger.error(f"get bybit flexible product info failed, url: {url}, code: {response.status_code}, error: {response.text}")
            data = response.json()
            if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
                for item in data["result"]["list"]:
                    token = item["coin"]
                    apy = float(item["estimateApr"].replace("%", ""))
                    apy_percentile = apy
                    if item['status'] != 'Available':
                        continue
                    try:
                        # 最新一个点是否大于最小收益率，很多时候收益率是向下走的
                        if apy >= buy_apy_threshold:
                            url = "https://api2.bybit.com/s1/byfi/get-flexible-saving-apr-history"
                            response = requests.post(
                                url=url,
                                json={"product_id": item['productId']},
                                headers={"Content-Type": "application/json"},
                                proxies=proxies
                            )
                            if response.status_code != 200:
                                logger.error(f"bybit get asset charts failed, url: {url}, status: {response.status_code}, response: {response.text}")
                            data = response.json().get('result', {}).get('hourly_apr_list', [])
                            data = [int(i['apr_e8']) / 1000000 for i in data]
                            logger.info(f"获取bybit {token}近24小时收益率曲线, 数据：{data}")
                            apy_percentile = get_percentile(data, percentile=yield_percentile, reverse=True)
                    except Exception as e:
                        logger.error(f"获取 {token}的收益曲线失败： {str(e)}")
                    product = {
                        "exchange": "Bybit",
                        "token": item["coin"],
                        "apy": float(item["estimateApr"].replace("%", "")),
                        'apy_percentile': apy_percentile,
                        'apy_month': [],
                        "min_purchase": float(item.get('minStakeAmount', 0)),
                        "max_purchase": float(item.get('maxStakeAmount', 0)),
                        "note": '',
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
        https://www.gate.io/zh/simple-earn
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
                "limit": 500,
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
            if response.status_code != 200:
                logger.error(f"get gateio活期理财产品, url: {url}, code: {response.status_code}, error: {response.text}")
            data = response.json()

            if data["code"] == 0 and "data" in data and "list" in data["data"]:
                end = int(datetime.now().replace(microsecond=0, second=0, minute=0).timestamp())
                start = end - 1 * 24 * 60 * 60
                start_30 = end - 30 * 24 * 60 * 60
                for item in data["data"]["list"]:
                    token = item["asset"]
                    apy = float(item["next_time_rate_year"]) * 100
                    apy_percentile = apy
                    apy_month = []
                    note = ''
                    if apy >= buy_apy_threshold:
                        try:
                            # https://www.gate.io/apiw/v2/uni-loan/earn/chart?from=1741874400&to=1741957200&asset=SOL&type=1
                            url = f'https://www.gate.io/apiw/v2/uni-loan/earn/chart?from={start}&to={end}&asset={token}&type=1'
                            logger.info(f"get gateio {token}近1天收益率曲线, url: {url}")
                            response = requests.get(
                                url=url,
                                proxies=proxies)
                            if response.status_code != 200:
                                logger.error(f"gateio get 1day asset charts, url: {url}, status: {response.status_code}, response: {response.text}")
                            data = response.json().get('data', [])
                            apy_percentile = get_percentile([float(i['value']) for i in data], percentile=yield_percentile, reverse=True)
                            # 按time字段排序
                            sorted_data = sorted(data, key=lambda x: x["time"])
                            if is_gold_dog(sorted_data):
                                note = f"🚀收益率暴增至{sorted_data[-1]['value']}%"

                            url = f'https://www.gate.io/apiw/v2/uni-loan/earn/chart?from={start_30}&to={end}&asset={token}&type=2'
                            logger.info(f"get gateio {token}近30天收益率曲线, url: {url}")
                            response = requests.get(
                                url=url,
                                proxies=proxies)
                            if response.status_code != 200:
                                logger.error(f"gateio get 30days asset charts, url: {url}, status: {response.status_code}, response: {response.text}")
                            data = response.json().get('data', [])
                            apy_month = [{'timestamp': i['time']*1000, 'apy': float(i['value'])} for i in data]
                        except Exception as e:
                            logger.error(f"get asset chart {item['asset']} error: {str(e)}")
                        sleep(2)
                    product = {
                        "exchange": "GateIO",
                        "token": token,
                        "apy": apy,
                        "apy_percentile": apy_percentile,
                        'apy_month': apy_month,
                        "min_purchase": f"{float(item.get('total_lend_available', 0))}(total_lend_available-可借总额)",
                        "max_purchase": f"{float(item.get('total_lend_all_amount', 0))}(total_lend_all_amount-借出总额)",
                        "note": note,
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
            if response.status_code != 200:
                logger.error(f"get okx flexible products error, url: {url}, status: {response.status_code}, response: {response.text}")
            data = response.json()

            if data["code"] == 0 and "data" in data and "allProducts" in data["data"]:
                for item in data["data"]["allProducts"]['currencies']:
                    token = item["investCurrency"]["currencyName"]
                    toked_id = int(item['investCurrency']['currencyId'])
                    apy = float(item['rate']['rateNum']['value'][0])
                    apy_percentile = apy
                    apy_month = []
                    if apy > buy_apy_threshold:
                        try:
                            url = f'https://www.okx.com/priapi/v2/financial/rate-history?currencyId={toked_id}&t={now_timestamp_ms}'
                            logger.info(f"get okx {token}近1天收益率曲线, url: {url}")
                            headers = {
                                "accept": "application/json",
                                "content-type": "application/json",
                                "authorization": "eyJhbGciOiJIUzUxMiJ9.eyJqdGkiOiJleDExMDE3NDE2MjI3Mjc0NzhFRkZGQzc4Mzk1N0U0RDMwMVhWV0IiLCJ1aWQiOiJMZDlvSkMxdVVXQlA0bWJtbDROcWp3PT0iLCJzdGEiOjAsIm1pZCI6IkxkOW9KQzF1VVdCUDRtYm1sNE5xanc9PSIsInBpZCI6IlBUeUE4VzA5ekZVSkJHSjZZUk5HWXc9PSIsIm5kZSI6MCwiaWF0IjoxNzQxNjIyNzI3LCJleHAiOjE3NDI4MzIzMjcsImJpZCI6MCwiZG9tIjoid3d3Lm9reC5jb20iLCJlaWQiOjE0LCJpc3MiOiJva2NvaW4iLCJkaWQiOiJJMW9iM0FDOEdPcXdyeG1ETEhDd3JGU3RsYUZ4bjlRUGNobmtibnZWMDhQcktxUlJ4QjNSWXVrY3p1YzkvRzJuIiwibGlkIjoiTGQ5b0pDMXVVV0JQNG1ibWw0TnFqdz09IiwidWZiIjoiUFR5QThXMDl6RlVKQkdKNllSTkdZdz09IiwidXBiIjoiaUJyYTJWaE5va3lSaWh4aUovM3pFdz09Iiwia3ljIjoyLCJreWkiOiJzVmtQSHhqTUdvYWFzajZndFcxUHg3ZFRwQ1pLZzUvNktuMW14YWlyWkNsTzhxa2IxYkx0YWYySVJVS2tMN3hFN3lkRi9ZTkNHUVcvNXlpNFZCelQzUT09IiwiY3BrIjoiaEJ2M21IRmNvSURMblNyRnp0R1NOWkxPb1pTazVtQThIcFBwT0w4UTVOVUR4dDJVVVE1N3BtcCsxcXVCRFJ2bGlta3gyQk94b0M5OG11Vi85a2tPdnR5VjlacGk5NkFEdHpKRGdiS0FjVnoyb01xeE5taVpabko0Q284ZWUyS1hsYXZXOVpiK3FqNTJPVnJSbGNId0tkK1hVWFdheWJQVjRackRXb2F0SnU4PSIsInZlciI6MSwiY2x0IjoyfQ.PirV2tw9OJordjLO5xs82rPPfS3tK7dSlonOh7FJi-hbdemX7vrJ65sDo2IlyR70GR9R0qD-te8QUdPugo9SRA",
                                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
                            }
                            response = requests.get(
                                url=url,
                                headers=headers,
                                proxies=proxies)
                            if response.status_code != 200:
                                logger.error(f"gateio get asset charts, url: {url}, status: {response.status_code}, response: {response.text}")
                            data = response.json().get('data', {})
                            apy_percentile = get_percentile([float(i['rate'])*100 for i in data.get('lastOneDayRates', {}).get('rates')])
                            apy_month = [{'timestamp': i['dataDate'], 'apy': float(i['rate'])*100*(1-okx_earn_insurance_keep_ratio)} for i in data.get('lastOneMonthRates', {}).get('rates', [])]
                        except Exception as e:
                            logger.error(f"get asset chart {item['asset']} error: {str(e)}")
                        product = {
                            "exchange": "OKX",
                            "token": token,
                            "apy": apy*(1-okx_earn_insurance_keep_ratio),
                            "apy_percentile": apy_percentile*(1-okx_earn_insurance_keep_ratio),
                            'apy_month': apy_month,
                            "min_purchase": '无',
                            "max_purchase": '无',
                            "note": '',
                        }
                        products.append(product)
                        sleep(0.1)
            else:
                logger.error(f"OKX API返回错误: {data}")
        except Exception as e:
            logger.error(f"获取OKX活期理财产品时出错: {str(e)}")
        return products

    def get_binance_funding_info(self):
        """
        获取币安合约资金费率周期数据
        [{
            "symbol": "LPTUSDT",
            "adjustedFundingRateCap": "0.02000000",
            "adjustedFundingRateFloor": "-0.02000000",
            "fundingIntervalHours": 4,
            "disclaimer": false
        }]
        :return:
        """
        url = f"https://www.binance.com/bapi/futures/v1/public/future/common/get-funding-info"
        response = requests.get(url, proxies=proxies)
        if response.status_code == 200:
            data = response.json()
            # logger.info(f"binance funding info get funding info: {data}")
            for i in data.get('data', []):
                self.binance_funding_info[i['symbol']] = i
        else:
            logger.error(f"binance get funding info failed, url: {url}, code: {response.status_code}, error: {response.text}")

    def get_binance_future_funding_rate_history(self, token, startTime, endTime):
        """
        https://developers.binance.com/docs/zh-CN/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History
        [{
            "symbol": "ETHUSDT",
            "fundingTime": 1740758400000,
            "fundingRate": "0.00001248",
            "markPrice": "2221.68000000"
        },
        {
            "symbol": "ETHUSDT",
            "fundingTime": 1740787200000,
            "fundingRate": "0.00004855",
            "markPrice": "2236.07630952"
        },
        {
            "symbol": "ETHUSDT",
            "fundingTime": 1740816000000,
            "fundingRate": "-0.00001061",
            "markPrice": "2228.15000000"
        }]
        :param token:
        :param startTime:
        :param endTime:
        :return:
        """
        history = []
        try:
            url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={token}&startTime={startTime}&endTime={endTime}"
            response = requests.get(url, proxies=proxies)
            if response.status_code != 200:
                logger.error(f"binance future funding rate history failed, url:{url}, status:{response.status_code}, response:{response.text}")
            else:
                logger.info(
                    f"binance future funding rate history success, url:{url}, status:{response.status_code}, response:{response.text}")
            history = [{'fundingTime': int(i['fundingTime']), 'fundingRate': float(i['fundingRate']), 'symbol': token} for i in response.json()]
        except Exception as e:
            logger.error(f"get get_binance_future_funding_rate_history failed, code: {str(e)}")
        return history

    def get_binance_futures_funding_rate(self, token):
        """
        获取币安合约资金费率
        :return {'fundingTime': 1741478400001, 'fundingRate': 0.0068709999999999995, 'markPrice': 2202.84}
        """
        exchange = 'Binance'
        try:
            # url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={token}"
            url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={token}"
            response = requests.get(url, proxies=proxies)
            if response.status_code != 200:
                logger.error(f"binance get future failed, url: {url}, status: {response.status_code}, response: {response.text}")
            data = response.json()
            if not self.binance_funding_info:
                self.get_binance_funding_info()
            fundingIntervalHours = self.binance_funding_info.get(token, {}).get('fundingIntervalHours', 8)
            fundingIntervalHoursText = self.binance_funding_info.get(token, {}).get('fundingIntervalHours', '无')
            return {
                "exchange": exchange,
                "fundingTime": data['nextFundingTime'],
                "fundingRate": float(data["lastFundingRate"]) * 100,
                "markPrice": float(data["markPrice"]),
                "fundingIntervalHours": fundingIntervalHours,
                'fundingIntervalHoursText': fundingIntervalHoursText,
            }  # 转换为百分比
        except Exception as e:
            logger.error(f"获取{exchange} {token}合约资金费率时出错: {str(e)}")
            return {}

    def get_bybit_futures_funding_rate_history(self, token, startTime, endTime):
        """
        https://bybit-exchange.github.io/docs/zh-TW/v5/market/history-fund-rate
        [{
            "symbol": "ETHUSDT",
            "fundingRate": "0.000074",
            "fundingTime": "1741939200000"
        },
        {
            "symbol": "ETHUSDT",
            "fundingRate": "0.000023",
            "fundingTime": "1741910400000"
        }]
        :param token:
        :param startTime:
        :param endTime:
        :param pageSize:
        :param pageNo:
        :return:
        """
        history = []
        try:
            # symbol = token.replace('USDT', 'PERP')
            url = f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={token}&&startTime={startTime}&endTime={endTime}"
            response = requests.get(url, proxies=proxies)
            if response.status_code != 200:
                logger.error(f"bybit future funding rate history get {url}, status: {response.status_code}, response: {response.text}")
            history = response.json().get('result', {}).get('list', [])
            history = [{'symbol': token, 'fundingRate': float(i['fundingRate']), 'fundingTime': int(i['fundingRateTimestamp'])} for i in history]
        except Exception as e:
            logger.error(f"get get_bybit_future_funding_rate_history failed, code: {str(e)}")
        return history

    def get_bitget_futures_funding_rate_history(self, token, startTime, endTime, pageSize=100, pageNo=1):
        """
        https://www.bitget.com/zh-CN/api-doc/contract/market/Get-History-Funding-Rate
        [{
            "symbol": "ETHUSDT",
            "fundingRate": "0.000074",
            "fundingTime": "1741939200000"
        },
        {
            "symbol": "ETHUSDT",
            "fundingRate": "0.000023",
            "fundingTime": "1741910400000"
        }]
        :param token:
        :param startTime:
        :param endTime:
        :param pageSize:
        :param pageNo:
        :return:
        """
        history = []
        try:
            url = f"https://api.bitget.com/api/v2/mix/market/history-fund-rate?symbol={token}&productType=USDT-FUTURES&pageSize={pageSize}&pageNo={pageNo}"
            response = requests.get(url, proxies=proxies)
            if response.status_code != 200:
                logger.error(f"bitget future funding rate history failed, url: {url}, status: {response.status_code}, response: {response.text}")
            history = response.json().get('data', [])
            history = [{'symbol': token, 'fundingTime': int(i['fundingTime']), 'fundingRate': float(i['fundingRate'])} for i in history if  startTime <= int(i['fundingTime']) <= endTime]
        except Exception as e:
            logger.error(f"get get_bitget_future_funding_rate_history failed, code: {str(e)}")
        return history



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
            if response.status_code != 200:
                logger.error(f"bitget get future price failed, url: {url}, status: {response.status_code}, response: {response.text}")
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
            if response.status_code != 200:
                logger.error(f"bitget get future funding time failed, url: {url}, status: {response.status_code}, response: {response.text}")
            data = response.json()
            if data["code"] == "00000" and "data" in data:
                return data["data"][0]["nextFundingTime"], int(data["data"][0]['ratePeriod'])
            return None, None
        except Exception as e:
            logger.error(f"获取Bitget {token}下次资金费结算时间: {str(e)}")
            return None, None

    def get_bitget_futures_funding_rate(self, token):
        """
        获取Bitget合约资金费率
        :return {'fundingTime': 1741478400001, 'fundingRate': 0.0068709999999999995, 'markPrice': 2202.84}
        """
        exchange = 'Bitget'
        try:
            url = "https://api.bitget.com/api/v2/mix/market/current-fund-rate"
            params = {
                "symbol": f"{token}",
                "productType": "USDT-FUTURES",
            }
            response = self.session.get(url, params=params)
            if response.status_code != 200:
                logger.error(f"bitget get future, url: {url}, status: {response.status_code}, response: {response.text}")
            data = response.json()

            if data["code"] == "00000" and "data" in data:
                funding_time, fundingIntervalHours = self.get_bitget_futures_funding_time(token)
                fundingIntervalHoursText = fundingIntervalHours if fundingIntervalHours else "无"
                mark_price = self.get_bitget_futures_funding_price(token)
                return {
                    "exchange": exchange,
                    'fundingTime': int(funding_time),
                    'fundingRate': float(data["data"][0]["fundingRate"]) * 100,
                    'markPrice': float(mark_price),
                    'fundingIntervalHours': fundingIntervalHours,
                    'fundingIntervalHoursText': fundingIntervalHoursText,
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
            if response.status_code != 200:
                logger.error(f"bybit get future failed, url: {url}, status: {response.status_code}, response: {response.text}")
            data = response.json()

            if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
                for item in data["result"]["list"]:
                    if "fundingRate" in item:
                        fundingRate = float(item["fundingRate"]) * 100
                        if fundingRate >= 0:
                            fundingIntervalHours = 8
                        else:
                            fundingIntervalHours = 1
                        return {
                            "exchange": exchange,
                            'fundingTime': int(item["nextFundingTime"]),
                            'fundingRate': float(item["fundingRate"]) * 100,  # 转换为百分比
                            'markPrice': float(item["markPrice"]),
                            'fundingIntervalHours': fundingIntervalHours,
                            'fundingIntervalHoursText': '无',
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
            if response.status_code != 200:
                logger.error(f"gateio get future failed, url: {url}, status: {response.status_code}, response: {response.text}")
            data = response.json()
            if data['in_delisting'] is False:
                fundingIntervalHours = int(data['funding_interval']/60/60)
                return {
                    "exchange": exchange,
                    'fundingTime': int(data["funding_next_apply"]) * 1000,
                    'fundingRate': float(data["funding_rate"]) * 100,  # 转换为百分比
                    'markPrice': float(data["mark_price"]),
                    'fundingIntervalHours': fundingIntervalHours,
                    'fundingIntervalHoursText': fundingIntervalHours,
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
            logger.info(f"okx get future, result: {funding_rate_info}")
            funding_rate = funding_rate_info['fundingRate']
            next_funding_time = funding_rate_info['nextFundingTimestamp']
            fundingIntervalHours = int((funding_rate_info['nextFundingTimestamp'] - funding_rate_info['fundingTimestamp'])/1000/60/60)
            return {
                'exchange': exchange,
                'fundingTime': next_funding_time,
                'fundingRate': float(funding_rate) * 100,
                'markPrice': float(current_price) * 100,
                "fundingIntervalHours": fundingIntervalHours,
                'fundingIntervalHoursText': fundingIntervalHours,
            }
        except Exception as e:
            logger.error(f"获取{exchange} {token}合约资金费率时出错: {str(e)}")
            return {}

    def get_okx_futures_funding_rate_history(self, token,  startTime, endTime):
        """
        获取 OKX 合约资金费率
        :return {'fundingTime': 1741478400001, 'fundingRate': 0.0068709999999999995, 'markPrice': 2202.84}
        """
        history = []
        symbol =  token.replace('USDT', '-USD-SWAP')
        try:
            # 初始化OKX交易所实例
            url = f"https://www.okx.com/api/v5/public/funding-rate-history?instId={symbol}&before={startTime}&after={endTime}"
            response = requests.get(url, proxies=proxies)
            if response.status_code != 200:
                logger.error(f"okx future funding rate history failed,  url: {url}, status: {response.status_code}, response: {response.text}")
            history = response.json().get('data', [])
            history = [{'fundingTime': int(i['fundingTime']), 'symbol': token, 'fundingRate': float(i['fundingRate'])*100} for i in history]
        except Exception as e:
            logger.error(f"get get_okx_future_funding_rate_history failed, code: {str(e)}")
        return history

    def get_gateio_futures_funding_rate_history(self, token,  startTime, endTime):
        """
        获取 OKX 合约资金费率
        :return {'fundingTime': 1741478400001, 'fundingRate': 0.0068709999999999995, 'markPrice': 2202.84}
        """
        history = []
        try:
            gate_io_token = token.replace('USDT', '_USDT')
            url = f"https://api.gateio.ws/api/v4/futures/usdt/funding_rate?contract={gate_io_token}&from={int(startTime/1000)}&to={int(endTime/1000)}"
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
            response = self.session.get(url, headers=headers, proxies=proxies)
            if response.status_code != 200:
                logger.error(f"okx future funding rate history failed, url: {url}, status: {response.status_code}, response: {response.text}")
            history = response.json()
            history = [{'fundingTime': i['t']*1000, 'symbol': token, 'fundingRate': 100*float(i['r'])} for i in history]
        except Exception as e:
            logger.error(f"get get_gateio_future_funding_rate_history failed, code: {str(e)}")
        return history


if __name__ == "__main__":
    api = ExchangeAPI()
    # api.get_binance_funding_info()
    token = 'SUKUUSDT'
    start = 1739318400000
    end = 1741939200000
    print(api.get_gateio_futures_funding_rate(token))
    # print(api.get_gateio_flexible_products())
    # print(api.get_bitget_futures_funding_rate_history(token, startTime=start, endTime=end)[0])
    # print(api.get_bybit_futures_funding_rate_history(token, startTime=start, endTime=end)[0])
    # print(api.get_okx_futures_funding_rate_history(token, startTime=start, endTime=end)[0])
    # print(api.get_gateio_futures_funding_rate_history(token, startTime=start, endTime=end)[0])
    # print(api.get_binance_flexible_products())
    # print(api.get_gateio_flexible_products())
    # print(api.get_okx_flexible_products())

