"""
一些gateio自定义的api(模拟浏览器请求)，ccxt未支持
"""
from time import sleep

import requests
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import proxies, gateio_login_token
from tools.logger import logger


def subscrible_earn(token, amount, rate=0.0010, login_token=gateio_login_token):
    """
    curl 'https://www.gate.io/apiw/v2/uni-loan/earn/subscribe' \
      -H 'accept: application/json, text/plain, */*' \
      -H 'accept-language: zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6' \
      -H 'baggage: sentry-environment=production,sentry-release=bmX7-kFrnGN2QHKxsMHw9,sentry-public_key=49348d5eaee2418db953db695c5c9c57,sentry-trace_id=c6af7ed66bac4b9899cdc6d60f0fce24,sentry-sample_rate=0.1,sentry-transaction=%2Fsimple-earn,sentry-sampled=false' \
      -H 'cache-control: no-cache' \
      -H 'content-type: application/json' \
      -H 'cookie: exchange_rate_switch=1; defaultBuyCryptoFiat=USD; curr_fiat=USD; _dx_uzZo5y=4b9695cfe312ebc4730a5732f9ec9f053deaf113d7a6189c476e1c3537f314f3825860d4; g_state={"i_p":1741539050392,"i_l":1}; b_notify=1; lang=cn; login_notice_check=%2F; uid=20243155; nickname=ni126ni%40gmail.com; is_on=1; pver=df5f623acb3b55feec2fdc68c61f4b6e; pver_ws=dd3c967f1abc567bb4f81c665e73e134; token_type=Bearer; csrftoken=4347484c4c6231687a517478306a665563764d34325a7a477a4d736655536f6e4b4172535754776e6e704a44624e3171456c484c336e74734342512b49714375; token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NDIzNDU4MDYsImlwIjoiZlhTRHhjK3ZFWEk0dHkvNkZFT3piNTY0VndBRUJ2STFsTFZ1TTM1bjQzQnB4U0gzcHNhMGw4UVoiLCJpcFJlc3RyaWN0IjoiNnVjaitYT2hId1JKRlRVVS9yN3RzN0JlRjA3Qzk2TEYvbGlnU0t3PSIsImRldmljZVR5cGUiOiJjRzBlU2t2eHRicEQ5ZlM4QXo1bW84RStVdUdMT2psQUZXblJNdVE9IiwiZGV2aWNlSWQiOiJ6N1J5RnIvMFpDWTZvaTEySGFJa0c3SGpPY3BHU3k4R09oeVk2QT09IiwidWlkIjoiM2RWeUtpMkIxUU5WSG1FMkU3enVvOFBwZzY5YVNxTFFIZUFxOW5CeldDL2dBQjVJIn0.rXs5yBAg9yCdiwY85mKu3AfDgnHaVydm_fNM7t7LhpY; newMsgCount=0; lasturl=%2Ftrade%2FKAVA_USDT; finger_print=67dff468MDwePPPAKZm55bSZj9UELn9Ir88SQ0b1; AWSALB=i7nEcDAhC4Wsq2mNafmQmZT3V/KKynMPFrGUfFnnkeuVaqvVAbJ3BvVr7YrsqktJk6FkSYzUta5G9O7+iz5udB/nqLoYRyKNp/cbE5Qg170auPEFNOV26kZ4OSUP; AWSALBCORS=i7nEcDAhC4Wsq2mNafmQmZT3V/KKynMPFrGUfFnnkeuVaqvVAbJ3BvVr7YrsqktJk6FkSYzUta5G9O7+iz5udB/nqLoYRyKNp/cbE5Qg170auPEFNOV26kZ4OSUP' \
      -H 'csrftoken: 4347484c4c6231687a517478306a665563764d34325a7a477a4d736655536f6e4b4172535754776e6e704a44624e3171456c484c336e74734342512b49714375' \
      -H 'origin: https://www.gate.io' \
      -H 'pragma: no-cache' \
      -H 'priority: u=1, i' \
      -H 'referer: https://www.gate.io/zh/simple-earn' \
      -H 'sec-ch-ua: "Chromium";v="130", "Microsoft Edge";v="130", "Not?A_Brand";v="99"' \
      -H 'sec-ch-ua-mobile: ?0' \
      -H 'sec-ch-ua-platform: "macOS"' \
      -H 'sec-fetch-dest: empty' \
      -H 'sec-fetch-mode: cors' \
      -H 'sec-fetch-site: same-origin' \
      -H 'sentry-trace: c6af7ed66bac4b9899cdc6d60f0fce24-86e1c3476e05e6d1-0' \
      -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0' \
      --data-raw '{"asset":"KAVA","amount":"10","rateYear":"0.0010"}'
    :return
    200, {"code":0,"message":"ok","data":{"asset":"KAVA","amount":"10","rate":"0.0010"},"timestamp":1742777649880}
    """
    url = 'https://www.gate.io/apiw/v2/uni-loan/earn/subscribe'
    cookies = {
        'token_type': 'Bearer',
        'token': login_token,
    }
    data = {
        "asset": token,
        "amount": str(amount),
        "rateYear": str(rate),
    }
    try:
        r = requests.post(
            url=url,
            json=data,
            proxies=proxies,
            cookies=cookies,
        )
        if r.status_code == 200:
            code = r.json().get('code')
            if code == 0:
                logger.info(f"subscribe {token} {amount} success, response: {r.text}")
            elif code == -1:
                import re
                rate_text = re.search(r'[0-9.]+', r.json().get('message'))
                if rate_text:
                    rate = float(rate_text.group())
                    subscrible_earn(token, amount, rate)
        else:
            logger.error(f"subscribe {token} {amount} failed, code: {r.status_code}, response: {r.text}")
    except Exception as e:
        logger.error(f"subscribe {token} {amount} failed, code:{r.status_code}, error: {r.text}")
    # 开启自动赚币
    switch_autoinvest(token, 1)


def redeem_earn(token, amount, login_token=gateio_login_token):
    positions = get_earn_positions(login_token=login_token)
    position = [i for i in positions if i["asset"] == token][0]
    current_amount = position["curr_amount"]
    lend_amount = position["lend_amount"]
    if float(amount) > float(current_amount):
        logger.error(f"{token} current amount: {current_amount} < {amount}, not enough balance")
        return False
    url = 'https://www.gate.io/apiw/v2/uni-loan/earn/redeem'
    cookies = {
        'token_type': 'Bearer',
        'token': login_token,
    }
    data = {
        "asset": token,
        "amount": str(amount),
        'curr_amount': current_amount,
        "lend_amount": lend_amount,
    }
    try:
        r = requests.post(
            url=url,
            json=data,
            proxies=proxies,
            cookies=cookies,
        )
        if r.status_code == 200 and r.json().get('code') == 0:
            logger.info(f"redeem {token} {amount} success, response: {r.text}")
        else:
            logger.error(f"redeem {token} {amount} failed, code: {r.status_code}, response: {r.text}")
    except Exception as e:
        logger.error(f"subscribe {token} {amount} failed, code:{r.status_code}, error: {r.text}")
    return False


def get_earn_positions(login_token=gateio_login_token, limit=50, page=1):
    """
    获取申购的所有理财产品
    :param login_token:
    :param limit:
    :param page:
    :return:
    [
    {'id': 2311000, 'asset': 'USDT', 'price': '1', 'curr_amount': '913.23884', 'curr_amount_usdt': '913.23884', 'lend_amount': '913.23884', 'frozen_amount': '0', 'interest': '0.25874864', 'min_lend_rate_year': '0.0010', 'next_time_rate_year': '0.0197', 'last_rate_year': '0.0197', 'is_open_award_pool': 1, 'award_asset': 'GT', 'ext_award_rate_year': '0.0800', 'ext_award_limit': '500', 'reinvest_status': 1, 'margin_frozen_amount': '0', 'margin_available_amount': '913.23884', 'auto_invest_status': 1, 'frozen_status': 0},
    ]
    """
    positions = []
    url = 'https://www.gate.io/apiw/v2/uni-loan/earn/subscribe'
    params = {
        'limit': limit,
        'page': page,
    }
    cookies = {
        'token_type': 'Bearer',
        'token': login_token,
    }
    try:
        r = requests.get(url, params=params, proxies=proxies, cookies=cookies)
        if r.status_code == 200 and r.json().get('code') == 0:
            # logger.info(f"get gateio earn positions success, response: {r.text}")
            positions = r.json().get('data').get('list')
        else:
            logger.error(f"get gateio earn positions failed, code:{r.status_code}, response: {r.text}")
    except Exception as e:
        logger.error(f"get gateio earn positions failed, code:{r.status_code}, error: {r.text}")
    return positions


def get_earn_interest(token, limit=24, page=1, login_token=gateio_login_token):
    """
    获取申购的理财产品收益明细
    :param login_token:
    :param limit: 1小时1个点，limit为多少表示多少个小时
    :param page:
    :return:
    [
    {'id': 2261796242, 'amount': '66.48493886', 'asset': 'B3', 'type': 3, 'lend_principal': '669873.439459', 'lend_rate_year': '86.95%', 'reinvest_type': '', 'time_at': 1744239760},
    {'id': 2260963852, 'amount': '66.50512724', 'asset': 'B3', 'type': 3, 'lend_principal': '669806.901447', 'lend_rate_year': '86.98%', 'reinvest_type': '', 'time_at': 1744236167},
    {'id': 2260387066, 'amount': '66.53801138', 'asset': 'B3', 'type': 3, 'lend_principal': '669733.380844', 'lend_rate_year': '87.04%', 'reinvest_type': '', 'time_at': 1744232597}
    ]
    """
    interests = []
    url = "https://www.gate.io/apiw/v2/uni-loan/earn/history"
    params = {
        'limit': limit,
        'page': page,
        "asset": token,
        "type": 3,
    }
    cookies = {
        'token_type': 'Bearer',
        'token': login_token,
    }
    try:
        r = requests.get(url, params=params, proxies=proxies, cookies=cookies)
        if r.status_code == 200 and r.json().get('code') == 0:
            # logger.info(f"get gateio earn positions success, response: {r.text}")
            interests = r.json().get('data')
        else:
            logger.error(f"get gateio earn positions failed, code:{r.status_code}, response: {r.text}")
    except Exception as e:
        logger.error(f"get {token} gateio earn interests failed, code:{r.status_code}, error: {r.text}")
    sleep(2)
    return interests


def get_earn_product(token):
    """
    {
    "id": 734,
    "asset": "SAFE",
    "name": "Safe",
    "name_cn": "Safe",
    "total_lend_amount": "553479.47",
    "total_lend_amount_fiat": "294783.17",
    "total_lend_available": "93.94",
    "total_lend_available_origin": "93.935792",
    "total_lend_available_fiat": "50.03",
    "total_lend_all_amount": "553573.41",
    "total_lend_all_amount_fiat": "294833.20",
    "next_time_rate_year": "2.4339",
    "last_time_rate_year": "0.0585",
    "min_lend_rate_year": "0.0010",
    "max_lend_rate_year": "4.9932",
    "lend_ratio": "0.9998",
    "sort_value": "81.90255515",
    "is_open_award_pool": 0,
    "award_asset": "",
    "ext_award_rate_year": "0",
    "year_rate": "2.4339",
    "ext_award_limit": "0",
    "is_top": 0,
    "auto_invest_status": 1,
    "user_available_usdt": "",
    "usdt_rate": "0.5326",
    "len_usd": "39945000",
    "Sort": 0,
    "NewSort": 0,
    "fixed_list": [],
    "icon": "https://icon.gateimg.com/images/coin_icon/64/safe.png?v=1743408000",
    "symbol": "S",
    "max_year_rate": "2.4339",
    "ladder_apr": null
}
    :param token:
    :return:
    """
    product = {}
    url = 'https://www.gate.io/apiw/v2/uni-loan/earn/market/list'
    params = {
        'search_coin': token,
        'limit': 7,
    }
    try:
        r = requests.get(url, params=params, proxies=proxies)
        if r.status_code == 200 and r.json().get('code') == 0:
            products = [i for i in r.json().get('data', {}).get('list', []) if i['asset'] == token]
            if len(products) == 1:
                product = products[0]
    except Exception as e:
        logger.error(f"get_earn_product failed for {token}: {str(e)}")
    return product


def switch_autoinvest(token, status, login_token=gateio_login_token):
    """
    自动赚币
    :param login_token:
    :param status: 0 禁用， 1启用
    :return:
    """
    url = "https://www.gate.io/apiw/v2/uni-loan/earn/autoivest"
    data = {"assets": token, "status": str(status)}
    cookies = {
        'token_type': 'Bearer',
        'token': login_token,
    }
    try:
        r = requests.post(url, json=data, proxies=proxies, cookies=cookies)
        if r.status_code == 200 and r.json().get('code') == 0:
            # logger.info(f"get gateio earn positions success, response: {r.text}")
            logger.debug(f"gateio set {token} autoinvest succeed, response: {r.text}")
        else:
            logger.error(f"gateio set {token} autoinvest failed, code:{r.status_code}, response: {r.text}")
    except Exception as e:
        logger.exception(f"gateio set {token} autoinvest failed, error: {e}")


if __name__ == '__main__':
    token = 'BR'
    # redeem_earn(token, 500)
    # subscrible_earn(token, 500)
    # print(get_earn_positions())
    # print(get_earn_interest('AVL'))
    # subscrible_earn(token, 20, rate=0.01)
    # pass
    print(get_earn_product(token))
    # positions = get_earn_positions()
    # print(positions)
    # for p in positions:
    #     if float(p['curr_amount_usdt']) >= 1:
    #         print(f"{p['asset']}: 持仓金额:{p['curr_amount_usdt']:.2f} USDT,数量: {p['curr_amount']}, 价格:{p['price']:.5f}")
    # print(len(get_earn_interest('B3')))
    # switch_autoinvest('PROS', 1)
    # print([i for i in positions if i["asset"] == token])

    # redeem_earn(token, 10)
    # get_earn_positions()
    # print([i for i in positions if i["asset"] == token])
    #
    # subscrible_earn(token, 10)
    # get_earn_positions()
    # print([i for i in positions if i["asset"] == token])
