import os
import sys
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import proxies

from config import gateio_login_token, okx_login_token

data = {"product_type":6,"product_id":"629"}
r = requests.post('https://api2.bybit.com/s1/byfi/get-product-detail', proxies=proxies, json=data)
print(r.json())