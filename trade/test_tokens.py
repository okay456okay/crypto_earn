import os
import sys

import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import proxies
from tools.wechatwork import WeChatWorkBot
from tools.logger import logger

from config import gateio_login_token, okx_login_token, token_check_webhook_url

wecom = WeChatWorkBot(token_check_webhook_url)

headers = {
    'authorization': okx_login_token,
}
r = requests.get('https://www.okx.com/v2/asset/balance?valuationUnit=USDT&filterOutZeroBal=true&transferFrom=6&t=1749698976209', proxies=proxies, headers=headers)
if not (r.status_code == 200 and r.json().get("code") == 0):
    message = f"okx token过期，请及时更新token, error: {r.text}"
    logger.info(message)
    wecom.send_message(message)

url = 'https://www.gate.io/apiw/v2/uni-loan/earn/subscribe'
params = {
    'limit': 2,
    'page': 1,
}
cookies = {
    'token_type': 'Bearer',
    'token': gateio_login_token,
}
r = requests.get(url, params=params, proxies=proxies, cookies=cookies)
if not (r.status_code == 200 and r.json().get('code') == 0):
    message = f"gateio token过期，请及时更新token, error: {r.text}"
    logger.info(message)
    wecom.send_message(message)
