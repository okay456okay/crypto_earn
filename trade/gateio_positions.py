import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from high_yield.exchange import ExchangeAPI
from gateio_api import get_earn_positions

def print_earn_info():
    positions = get_earn_positions()
    api = ExchangeAPI()
    for p in positions:
        if float(p['curr_amount_usdt']) >= 1:
            earn_info = api.get_gateio_flexible_product(p['asset'])
            print(f"{p['asset']}: 持仓金额:{p['curr_amount_usdt']} USDT, 收益率: {earn_info['apy']}%, 数量: {p['curr_amount']}, 价格:{p['price']}")

if __name__ == '__main__':
    print_earn_info()