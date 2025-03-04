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

# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将 config.py 所在的目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))

from binance_buy.buy_spot import get_proxy_ip
from config import api_secret, api_key, proxies, logger
from high_yield.get_binance_yield import get_binance_flexible_savings

# import json



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
        """获取币安活期理财产品 - 使用更新的API"""
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
                            "min_purchase": float(item.get('productDetailList', [])[0].get("minPurchaseAmount", 0)),
                            "max_purchase": float(item.get('productDetailList', [])[0].get("maxPurchaseAmountPerUser", 0))
                        }
                        products.append(product)
                return products
        except Exception as e:
            logger.error(f"获取Binance活期理财产品时出错: {str(e)}")
            # 尝试备用API接口
            return []

    def get_bitget_flexible_products(self):
        """获取Bitget活期理财产品"""
        try:
            url = "https://api.bitget.com/api/v2/finance/staking/list"
            params = {
                "pageSize": 100,
                "productType": "FLEXIBLE"
            }
            response = self.session.get(url, params=params)
            data = response.json()

            if data["code"] == "00000" and "data" in data and "list" in data["data"]:
                products = []
                for item in data["data"]["list"]:
                    product = {
                        "exchange": "Bitget",
                        "token": item["coinName"],
                        "apy": float(item["apy"]),
                        "min_purchase": float(item.get("minAmount", 0))
                    }
                    products.append(product)
                return products
            else:
                logger.error(f"Bitget API返回错误: {data}")
                return []
        except Exception as e:
            logger.error(f"获取Bitget活期理财产品时出错: {str(e)}")
            return []

    def get_bybit_flexible_products(self):
        """获取Bybit活期理财产品"""
        try:
            url = "https://api2.bybit.com/s1/byfi/get-saving-homepage-product-cards"
            params = {"product_area": [0], "page": 1, "limit": 20, "product_type": 0, "coin_name": "", "sort_apr": 1,
                      "match_user_asset": False, "show_available": False, "fixed_saving_version": 1}
            response = self.session.post(url, json=params)
            data = response.json()

            if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
                products = []
                for item in data["result"]["list"]:
                    product = {
                        "exchange": "Bybit",
                        "token": item["coin"],
                        "apy": float(item["apr"]),
                        "min_purchase": float(item.get("minAmount", 0))
                    }
                    products.append(product)
                return products
            else:
                logger.error(f"Bybit API返回错误: {data}")
                return []
        except Exception as e:
            logger.error(f"获取Bybit活期理财产品时出错: {str(e)}")
            return []

    def get_binance_futures(self, token):
        """获取币安合约资金费率"""
        try:
            url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={token}"
            response = self.session.get(url)
            data = response.json()
            item = data[-1]
            if token in item["symbol"]:
                return {
                    "fundingTime": item["fundingTime"],
                    "fundingRate": float(item["fundingRate"]) * 100,
                    "markPrice": float(item["markPrice"]),
                }  # 转换为百分比
            return {}  # 未找到对应Token的合约资金费率
        except Exception as e:
            logger.error(f"获取Binance {token}合约资金费率时出错: {str(e)}")
            return {}

    def get_bitget_futures_funding_rate(self, token):
        """获取Bitget合约资金费率"""
        try:
            url = "https://api.bitget.com/api/mix/v1/market/fundingRate"
            params = {
                "symbol": f"{token}USDT"
            }
            response = self.session.get(url, params=params)
            data = response.json()

            if data["code"] == "00000" and "data" in data:
                return float(data["data"]["fundingRate"]) * 100  # 转换为百分比
            return None
        except Exception as e:
            logger.error(f"获取Bitget合约资金费率时出错: {str(e)}")
            return None

    def get_bybit_futures_funding_rate(self, token):
        """获取Bybit合约资金费率"""
        try:
            url = "https://api.bybit.com/v5/market/tickers"
            params = {
                "category": "linear",
                "symbol": f"{token}USDT"
            }
            response = self.session.get(url, params=params)
            data = response.json()

            if data["retCode"] == 0 and "result" in data and "list" in data["result"]:
                for item in data["result"]["list"]:
                    if "fundingRate" in item:
                        return float(item["fundingRate"]) * 100  # 转换为百分比
            return None
        except Exception as e:
            logger.error(f"获取Bybit合约资金费率时出错: {str(e)}")
            return None


# 主业务逻辑类
class CryptoYieldMonitor:
    def __init__(self, buy_webhook_url, sell_webhook_url):
        self.exchange_api = ExchangeAPI()
        self.buy_wechat_bot = WeChatWorkBot(buy_webhook_url)
        self.sell_wechat_bot = WeChatWorkBot(sell_webhook_url)
        self.min_apy_threshold = 15  # 最低年化利率阈值 (%)
        self.notified_tokens = set()  # 已通知的Token集合，避免重复通知

    def get_futures_trading(self, token):
        """检查Token是否在任意交易所上线了合约交易，且交易费率为正"""
        results = []

        # 检查Binance
        binance_rate = self.exchange_api.get_binance_futures(token)
        logger.info(f"{token} Binance Perp info: {binance_rate}")
        if binance_rate:
            results.append(("Binance", binance_rate))

        # 检查Bitget
        # bitget_rate = self.exchange_api.get_bitget_futures_funding_rate(token)
        # if bitget_rate is not None and bitget_rate > 0:
        #     results.append(("Bitget", bitget_rate))

        # 检查Bybit
        # bybit_rate = self.exchange_api.get_bybit_futures_funding_rate(token)
        # if bybit_rate is not None and bybit_rate > 0:
        #     results.append(("Bybit", bybit_rate))

        return results

    def high_yield_filter(self, all_products):
        # 筛选年化利率高于阈值的产品
        high_yield_products = [p for p in all_products if p["apy"] >= self.min_apy_threshold]
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
            positive_futures_results = [i for i in futures_results if i[1]['fundingRate'] >= 0 and int(time.time()) - i[1]['fundingTime']/1000 < 24*60*60]
            logger.info(f"{perp_token} positive future results: {futures_results}, current timestamp: {int(time.time())}")

            if positive_futures_results:
                logger.info(f"Token {token} 满足合约交易条件: {futures_results}")
                # 生成通知内容
                for exchange_name, funding_rate in futures_results:
                    notification_key = f"{token}_{exchange_name}"

                    # 检查是否已经通知过（24小时内不重复通知同一个Token+交易所组合）
                    # if notification_key in self.notified_tokens:
                    #     logger.info(f"Token {token} 在 {exchange_name} 已通知过，跳过")
                    #     continue

                    notification = {
                        "token": token,
                        "yield_exchange": product["exchange"],
                        "apy": product["apy"],
                        "futures_exchange": exchange_name,
                        "funding_rate": funding_rate,
                        "min_purchase": product["min_purchase"],
                        "max_purchase": product["max_purchase"],
                    }
                    high_yield_notifications.append(notification)
                    self.notified_tokens.add(notification_key)

        # 发送通知
        if high_yield_notifications:
            self._send_high_yield_notifications(high_yield_notifications)
            logger.info(f"已添加{len(high_yield_notifications)}个Token到通知列表")

            # 每24小时清理一次通知记录，允许再次通知
            if len(self.notified_tokens) > 100:  # 避免无限增长
                self.notified_tokens.clear()
                logger.info("已清理通知记录")
        else:
            logger.info("未找到满足所有条件的产品")

    def run(self):
        """运行监控任务"""
        logger.info("开始检查高收益加密货币...")
        try:
            # 获取所有交易所的活期理财产品
            binance_products = self.exchange_api.get_binance_flexible_products()
            logger.info(f"从Binance获取到{len(binance_products)}个活期理财产品")

            # bitget_products = self.exchange_api.get_bitget_flexible_products()
            # logger.info(f"从Bitget获取到{len(bitget_products)}个活期理财产品")

            # bybit_products = self.exchange_api.get_bybit_flexible_products()
            # logger.info(f"从Bybit获取到{len(bybit_products)}个活期理财产品")

            # 合并所有产品
            # all_products = binance_products + bitget_products + bybit_products
            all_products = binance_products
            logger.info(f"总共获取到{len(all_products)}个活期理财产品")
            # 过滤和处理高收益理财产品
            self.high_yield_filter(all_products)
            # 对所有已购买产品做检查
            # purchased_tokens = [('Binance', 'HIVE'), ]
            purchased_tokens = []
            binance_earn_positions = get_binance_flexible_savings(api_key, api_secret, proxies)
            for p in binance_earn_positions:
                if float(p.get('totalAmount', 0)) > 1:
                    purchased_tokens.append({"exchange": 'Binance', "symbol": p.get('asset'), "totalAmount": float(p.get('totalAmount', 0.0))})
            # purchased_tokens = [
            #     {'exchange': 'Binance', 'symbol': 'HIVE', 'totalAmount': 500.0},
            #     {'exchange': 'Binance', 'symbol': 'USDT', 'totalAmount': 200.0},
            # ]
            self.check_tokens(purchased_tokens, all_products)
        except Exception as e:
            logger.error(f"运行监控任务时发生错误: {str(e)}")

    def check_tokens(self, tokens, all_products):
        for token in tokens:
            product = [i for i in all_products if i['exchange'] == token['exchange'] and i['token'] == token['symbol']]
            if not product:
                # 发送未找到理财产品通知
                content = f"在交易所中未找到 {token} 理财产品"
                self.sell_wechat_bot.send_message(content)
            else:
                product = product[0]
                # 过滤资金费率和利率，如果满足条件就告警
                perp_token = f"{token['symbol']}USDT"
                # product: {'exchange': 'Binance', 'token': 'AXS', 'apy': 17.9, 'min_purchase': 0.01, 'max_purchase': 301499.0}
                # future_result: [('Binance', {'fundingTime': 1740960000001, 'fundingRate': 0.01, 'markPrice': 3.97194145})]
                futures_results = self.get_futures_trading(perp_token)
                negative_futures = [i for i in futures_results if i[1]['fundingRate'] < 0]
                futures_results_str = '\n'.join(
                    [f"{i[0]}: {datetime.fromtimestamp(i[1]['fundingTime'] / 1000)}, {i[1]['fundingRate']}" for i in
                     futures_results])
                if product['apy'] < self.min_apy_threshold or negative_futures:
                    content = (
                        f"{product['exchange']}加密货币理财产品{product['token']} 卖出提醒\n"
                        f"最新年化收益: {product['apy']}%\n"
                        f"持有仓位: {token['totalAmount']}\n"
                        f"各交易所资金费率: \n"
                        f"{futures_results_str}"
                    )
                    self.sell_wechat_bot.send_message(content)

    def _send_high_yield_notifications(self, notifications):
        """发送企业微信群机器人通知"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"📊 加密货币高收益理财产品监控 ({now})\n\n"

        for idx, notif in enumerate(notifications, 1):
            message += (
                f"{idx}. {notif['token']} 💰\n"
                f"   • 年化收益率: {notif['apy']}% ({notif['yield_exchange']})\n"
                f"   • 合约资金费率: {notif['funding_rate']['fundingRate']:.4f}% ({notif['futures_exchange']})\n"
                f"   • 合约价格: {notif['funding_rate']['markPrice']:.2f} ({notif['futures_exchange']})\n"
                f"   • 合约数据时间: {datetime.fromtimestamp(notif['funding_rate']['fundingTime'] / 1000)} ({notif['futures_exchange']})\n"
                f"   • 最低购买量: {notif['min_purchase']}\n"
                f"   • 最大购买量: {notif['max_purchase']}\n\n"
            )

        self.buy_wechat_bot.send_message(message)
        logger.info(f"已发送{len(notifications)}条高收益加密货币通知")


# 主程序入口
def main():
    # 企业微信群机器人webhook URL（请替换为您的实际webhook URL）
    buy_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=293071ec-9865-4e86-9e69-b48f1a12a83a"
    sell_webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=38fd27ea-8569-4de2-9dee-4c4a4ffb77ed"
    # 尝试获取外网出口IP
    proxy_ip = get_proxy_ip()
    logger.info(f"当前外网出口IP: {proxy_ip}")
    logger.info("请确保此IP已添加到Binance API白名单中")

    monitor = CryptoYieldMonitor(buy_webhook_url, sell_webhook_url)

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
