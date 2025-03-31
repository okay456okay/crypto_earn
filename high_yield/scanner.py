# coding=utf-8
"""
通过套保策略，实现现货和空单合约对冲，然后用现货购买高收益率产品，赚取收益。
该策略更适用于牛市，因为赚取的收益如果为非稳定币，随着价格下跌，则U本位的收益率会下跌

标的判断标准：
1. 所有合约资金费率为负的不超过2个
"""
from time import sleep

import time
from datetime import datetime
import sys
import os

# import traceback

# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将 config.py 所在的目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))

from high_yield.common import get_percentile
from high_yield.exchange import ExchangeAPI
from tools.wechatwork import WeChatWorkBot
from high_yield.token_manager import TokenManager
from tools.proxy import get_proxy_ip
from config import leverage_ratio, yield_percentile, stability_buy_apy_threshold, sell_apy_threshold, \
    future_percentile, highyield_buy_apy_threshold, stability_buy_webhook_url, highyield_buy_webhook_url, \
    highyield_checkpoints, volume_24h_threshold
from tools.logger import logger


# import json


# 交易所API类

# 主业务逻辑类
class CryptoYieldMonitor:
    def __init__(self):
        self.exchange_api = ExchangeAPI()

    def get_futures_trading(self, token):
        """检查Token是否在任意交易所上线了合约交易，且交易费率为正"""
        results = []

        # 检查Binance
        try:
            binance_rate = self.exchange_api.get_binance_futures_funding_rate(token)
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

        end = int(time.time() * 1000)
        start = end - 7 * 24 * 60 * 60 * 1000
        if binance_rate:
            binance_rate['d7history'] = self.exchange_api.get_binance_future_funding_rate_history(token,
                                                                                                  startTime=start,
                                                                                                  endTime=end)
            results.append(binance_rate)

        # 检查Bitget
        if bitget_rate:
            bitget_rate['d7history'] = self.exchange_api.get_bitget_futures_funding_rate_history(token, startTime=start,
                                                                                                 endTime=end)
            results.append(bitget_rate)

        # 检查Bybit
        if bybit_rate:
            bybit_rate['d7history'] = self.exchange_api.get_bybit_futures_funding_rate_history(token, startTime=start,
                                                                                               endTime=end)
            results.append(bybit_rate)

        if gate_io_rate:
            gate_io_rate['d7history'] = self.exchange_api.get_gateio_futures_funding_rate_history(token,
                                                                                                  startTime=start,
                                                                                                  endTime=end)
            results.append(gate_io_rate)

        if okx_rate:
            okx_rate['d7history'] = self.exchange_api.get_okx_futures_funding_rate_history(token, startTime=start,
                                                                                           endTime=end)
            results.append(okx_rate)

        return results

    def _send_product_notifications(self, notifications, product_type):
        """发送企业微信群机器人通知"""
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        end = int(now.timestamp() * 1000)
        d7start = end - 7 * 24 * 60 * 60 * 1000
        d30start = end - 30 * 24 * 60 * 60 * 1000
        if product_type == '稳定':
            wechat_bot = WeChatWorkBot(stability_buy_webhook_url)
        elif product_type == '金狗':
            wechat_bot = WeChatWorkBot(highyield_buy_webhook_url)
        else:
            logger.error("unknown product type")
            return
        limit = 3
        for p in range(int(len(notifications) / limit) + 1):
            message = ''
            for idx, notif in enumerate(notifications[p * limit:(p + 1) * limit], 1):
                d7apy_str = '无';
                d30apy_str = '无'
                if notif['apy_month']:
                    d7apy = get_percentile([i['apy'] for i in notif['apy_month'] if d7start <= i['timestamp'] <= end],
                                           yield_percentile)
                    d7apy_str = f"{d7apy:.2f}%"
                    d30apy = get_percentile([i['apy'] for i in notif['apy_month'] if d30start <= i['timestamp'] <= end],
                                            yield_percentile)
                    d30apy_str = f"{d30apy:.2f}%"
                message += (
                    f"{idx + p * limit}. {notif['token']}({notif['exchange']}) 💰\n"
                    f"   • 近24小时现货交易量: {notif['volume_24h']/10000:.2f}万USDT\n"
                    f"   • 最新收益率: {notif['apy']:.2f}%\n"
                    f"   • 近24小时P{yield_percentile}收益率: {notif['apy_percentile']:.2f}%\n"
                    f"   • 近7天P{yield_percentile}收益率: {d7apy_str}\n"
                    f"   • 近30天P{yield_percentile}收益率: {d30apy_str}\n"
                    f"   • 各交易所合约信息: \n"
                    f"   • 近24小时合约交易量|最新资金费率|近7天P{yield_percentile}资金费率|标记价格|预估收益率|近24小时P{yield_percentile}预估收益率|结算周期|下次结算时间\n"
                    f"{notif['future_info']}\n"
                    f"   • 最低购买量: {notif['min_purchase']}\n"
                    f"   • 最大购买量: {notif['max_purchase']}\n"
                )
            if message:
                # https://emojipedia.org/
                message = f"📊交易所{product_type}活期理财产品监控 ({now_str})\n\n" + message
                wechat_bot.send_message(message)
        logger.info(f"已发送{len(notifications)}条高收益加密货币通知")

    def get_estimate_apy(self, apy, fundingRate, fundingIntervalHours, leverage_ratio=leverage_ratio):
        return 1 * leverage_ratio / (leverage_ratio + 1) * (apy + fundingRate * (24 / fundingIntervalHours) * 365)

    def product_filter(self, all_products):
        # 筛选年化利率高于阈值的产品
        eligible_products = [p for p in all_products if p["apy"] >= stability_buy_apy_threshold and p['volume_24h'] > volume_24h_threshold]
        eligible_products = sorted(eligible_products, key=lambda x: x['apy'], reverse=True)
        logger.info(f"筛选出{len(eligible_products)}个年化利率高于{stability_buy_apy_threshold}%的产品")

        if not eligible_products:
            logger.info(f"未找到年化利率高于{stability_buy_apy_threshold}%且24小时交易额大于10000USDT的产品")
            return

        # 检查每个高收益产品是否满足合约交易条件
        stability_product_notifications = []
        highyield_product_notifications = []

        for product in eligible_products:
            token = product["token"]
            logger.info(f"检查Token {token} 的合约交易情况")
            # 检查合约交易条件
            perp_token = f"{token}USDT"
            futures_results = self.get_futures_trading(perp_token)
            logger.info(f"{perp_token} get future results: {futures_results}")
            # 如果没有合约支持，跳过
            if not futures_results:
                continue
            # 是否有预估收益率低于最低收率益的交易所（合约负费率太多了）
            eligible_funding_rate = [
                i for i in futures_results if
                self.get_estimate_apy(product['apy'], i['fundingRate'],
                                      i['fundingIntervalHours']) >= stability_buy_apy_threshold and # 考虑资金费率后收益率超过基准值
                i['fundingRate'] > -0.02 and  # 资金费率大于某个值
                i['markPrice'] > 0.0001 and  # 币值大于某个值
                i['volume_24h'] > volume_24h_threshold  # 合约交易额大于某个值
            ]
            illegible_funding_rate = [ i for i in futures_results if i['fundingRate'] < -0.1]
            if len(eligible_funding_rate) == 0 or len(illegible_funding_rate) > 0:
                continue
            apy_percentile = 0.0
            if product['apy_day']:
                apy_percentile = get_percentile([i['apy'] for i in product['apy_day']], yield_percentile)

            future_info_str = '\n'.join([
                f"   • {i['exchange']}: {i['volume_24h']/10000:.2f}万USDT, {i['fundingRate']:.4f}%, {get_percentile([i['fundingRate'] for i in i['d7history']], future_percentile):.4f}%, {i['markPrice']:.5f}, {self.get_estimate_apy(product['apy'], i['fundingRate'], i['fundingIntervalHours']):.2f}%, {self.get_estimate_apy(apy_percentile, i['fundingRate'], i['fundingIntervalHours']):.2f}%, {i['fundingIntervalHoursText']}, {datetime.fromtimestamp(i['fundingTime'] / 1000)}"
                for i in
                futures_results])
            # 生成通知内容
            notification = {
                "exchange": product["exchange"],
                "token": token,
                "apy": product["apy"],
                "apy_percentile": apy_percentile,
                "volume_24h": product["volume_24h"],
                'apy_month': product['apy_month'],
                "future_info": future_info_str,
                "min_purchase": product["min_purchase"],
                "max_purchase": product["max_purchase"],
            }
            logger.info(f"filter high yield product: {product}")
            # 稳定收益： 24小时Pxx收益率达到最低k
            if apy_percentile > stability_buy_apy_threshold:
                stability_product_notifications.append(notification)
            if len([i for i in product['apy_day'][-3:] if
                    i['apy'] >= highyield_buy_apy_threshold]) == highyield_checkpoints:
                highyield_product_notifications.append(notification)

        # 发送通知
        if stability_product_notifications:
            logger.info(f"已添加{len(stability_product_notifications)}个稳定理财Token到通知列表")
            self._send_product_notifications(stability_product_notifications, product_type='稳定')
        if highyield_product_notifications:
            logger.info(f"已添加{len(highyield_product_notifications)}个金狗Token到通知列表")
            self._send_product_notifications(highyield_product_notifications, product_type='金狗')

    def check_tokens(self, tokens, all_products):
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        end = int(datetime.now().timestamp() * 1000)
        d7start = end - 7 * 24 * 60 * 60 * 1000
        d30start = end - 30 * 24 * 60 * 60 * 1000
        for token in tokens:
            # 获取理财产品最新利率
            sell_wechat_bot = WeChatWorkBot(token['webhook_url'])
            product = [i for i in all_products if
                       i['exchange'] == token['spot_exchange'] and i['token'] == token['token']]
            apy_percentile = 0.0
            if not product:
                # 发送未找到理财产品通知
                content = f"在{token['spot_exchange']}交易所中未找到 {token['token']} 理财产品"
                # sell_wechat_bot.send_message(content)
                logger.info(content)
                if token['spot_exchange'] == 'GateIO':
                    product = self.exchange_api.get_gateio_flexible_product(token['token'])
            else:
                product = product[0]
                if token['spot_exchange'] == 'GateIO' and (not product['apy_day']):
                    product = self.exchange_api.get_gateio_flexible_product(token['token'])
            # 过滤资金费率和利率，如果满足条件就告警
            perp_token = f"{token['token']}USDT"
            futures_results = self.get_futures_trading(perp_token)
            token_future = [i for i in futures_results if i['exchange'] == token['future_exchange']]
            if token_future:
                token_future = token_future[0]
                estimate_apy = self.get_estimate_apy(product['apy'], token_future['fundingRate'],
                                                     token_future['fundingIntervalHours'])
                if product['apy_day']:
                    apy_percentile = get_percentile([i['apy'] for i in product['apy_day']], yield_percentile)
                estimate_apy_percentile = self.get_estimate_apy(apy_percentile, token_future['fundingRate'],
                                                                token_future['fundingIntervalHours'])
                future_info_str = '\n'.join([
                    f"   • {i['exchange']}: {i['volume_24h'] / 10000:.2f}万USDT, {i['fundingRate']:.4f}%, {get_percentile([i['fundingRate'] for i in i['d7history']], future_percentile):.4f}%, {i['markPrice']:.5f}, {self.get_estimate_apy(product['apy'], i['fundingRate'], i['fundingIntervalHours']):.2f}%, {self.get_estimate_apy(apy_percentile, i['fundingRate'], i['fundingIntervalHours']):.2f}%, {i['fundingIntervalHoursText']}, {datetime.fromtimestamp(i['fundingTime'] / 1000)}"
                    for i in
                    futures_results])
                # token_future['fundingRate'] < 0
                d7apy_str = f"无";
                d30apy_str = f"无"
                if product['apy_month']:
                    d7apy = get_percentile([i['apy'] for i in product['apy_month'] if d7start <= i['timestamp'] <= end],
                                           yield_percentile)
                    d7apy_str = f"{d7apy:.2f}%"
                    d30apy = get_percentile(
                        [i['apy'] for i in product['apy_month'] if d30start <= i['timestamp'] <= end], yield_percentile)
                    d30apy_str = f"{d30apy:.2f}%"
                # 收益率、预估收益率、Pxx收益率 小于卖出年化阈值
                if product['apy'] < sell_apy_threshold or \
                        estimate_apy < sell_apy_threshold:
                        # estimate_apy_percentile < sell_apy_threshold:
                    content = f"📉**卖出提醒**: "
                else:
                    content = f"💰**持仓收益率**: "
                content += (
                        f"{product['exchange']}活期理财产品{product['token']} ({now_str})\n"
                        f"近24小时现货交易量: {product['volume_24h']/10000:.2f}万USDT\n"
                        f"最新收益率: {product['apy']:.2f}%\n"
                        f"P{yield_percentile}收益率: {apy_percentile:.2f}%\n"
                        f"近7天P{yield_percentile}收益率: {d7apy_str}\n"
                        f"近30天P{yield_percentile}收益率: {d30apy_str}\n"
                        f"各交易所合约信息(套保交易所: {token['future_exchange']})\n"
                        f"近24小时合约交易量|最新资金费率|近7天P{yield_percentile}资金费率|标记价格|预估收益率|近24小时P{yield_percentile}预估收益率|结算周期|下次结算时间\n"
                        f"{future_info_str}"
                    )
                sell_wechat_bot.send_message(content)
            else:
                content = f"在{token['future_exchange']}交易所中未找到 {token['token']} 合约产品"
                logger.info(content)
                # sell_wechat_bot.send_message(content)
            sleep(0.5)

    def position_check(self, all_products):
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
            logger.exception(f"对所有已购买产品做检查失败 {e}")

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
            # all_products =  bybit_products + gateio_products + okx_products + binance_products
            # all_products = gateio_products
            logger.info(f"总共获取到{len(all_products)}个活期理财产品")
            self.exchange_api.get_binance_funding_info()
            # 过滤和处理高收益理财产品
            self.product_filter(all_products)
            self.position_check(all_products)
        except Exception as e:
            logger.exception(f"运行监控任务时发生错误: {str(e)}")


# 主程序入口
def main():
    monitor = CryptoYieldMonitor()
    # print(monitor.exchange_api.get_binance_futures('ETHUSDT'))
    # print(monitor.exchange_api.get_bitget_futures_funding_rate('ETHUSDT'))
    # print(monitor.exchange_api.get_okx_futures_funding_rate('ETHUSDT'))
    # print(monitor.exchange_api.get_gateio_futures_funding_rate('ETHUSDT'))
    # print(monitor.exchange_api.get_bybit_futures_funding_rate('ETHUSDT'))
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
    # schedule.every(30).minutes.do(monitor.run)
    # logger.info("加密货币高收益监控服务已启动，每30分钟检查一次...")

    # 保持程序运行
    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)


if __name__ == "__main__":
    main()
