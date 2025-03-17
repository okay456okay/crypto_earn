import time
import logging
import ccxt
# from dotenv import load_dotenv
import numpy as np


import sys
import os

# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将 config.py 所在的目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))

from config import proxies
from tools.logger import logger


# 加载环境变量
# load_dotenv()

# 交易配置
SYMBOL = "KAVA/USDT"
QUANTITY = 1000
THRESHOLD = 0.001  # 价格差异阈值
RETRY_DELAY = 5  # 重试延迟(秒)
LEVERAGE = 10  # 杠杆倍数
MARGIN_MODE = "cross"  # 全仓模式


# 初始化交易所API
def init_exchanges():
    try:
        # GateIO配置
        gateio = ccxt.gateio({
            'apiKey': os.getenv('GATEIO_API_KEY'),
            'secret': os.getenv('GATEIO_SECRET'),
            'enableRateLimit': True
        })

        # Bitget配置
        bitget = ccxt.bitget({
            'apiKey': os.getenv('BITGET_API_KEY'),
            'secret': os.getenv('BITGET_SECRET'),
            'password': os.getenv('BITGET_PASSWORD'),  # Bitget需要额外的密码
            'enableRateLimit': True,
            'proxies': proxies,
        })

        logger.info("交易所API初始化成功")
        return gateio, bitget
    except Exception as e:
        logger.error(f"初始化交易所API失败: {e}")
        raise


# 设置合约交易模式和杠杆
def setup_contract_settings(bitget):
    try:
        # 设置合约交易的杠杆和保证金模式
        contract_symbol = f"{SYMBOL}:USDT"  # 合约交易对格式

        # 设置保证金模式为全仓
        bitget.set_margin_mode(MARGIN_MODE, contract_symbol)
        logger.info(f"已设置Bitget {contract_symbol}为{MARGIN_MODE}模式")

        # 设置杠杆倍数
        bitget.set_leverage(LEVERAGE, contract_symbol)
        logger.info(f"已设置Bitget {contract_symbol}杠杆为{LEVERAGE}倍")

        return True
    except Exception as e:
        logger.error(f"设置合约交易参数失败: {e}")
        return False


# 计算加权平均价格 - 考虑订单簿深度
def calculate_weighted_price(orderbook, quantity, side):
    """
    计算考虑订单簿深度的加权平均价格

    Args:
        orderbook: 订单簿数据
        quantity: 需要交易的数量
        side: 'asks'表示买入(考虑卖单),'bids'表示卖出(考虑买单)

    Returns:
        加权平均价格, 是否有足够的深度
    """
    total_quantity = 0
    weighted_sum = 0

    for price, available_quantity in orderbook[side]:
        volume_to_use = min(available_quantity, quantity - total_quantity)
        weighted_sum += price * volume_to_use
        total_quantity += volume_to_use

        if total_quantity >= quantity:
            # 有足够的深度满足交易需求
            return weighted_sum / quantity, True

    # 订单簿深度不足
    if total_quantity > 0:
        return weighted_sum / total_quantity, False
    else:
        return None, False


# 获取价格 - 考虑订单簿深度
def get_prices_with_depth(gateio, bitget):
    try:
        # 获取GateIO现货订单簿
        gateio_orderbook = gateio.fetch_order_book(SYMBOL, 50)  # 获取足够深度的订单簿

        # 计算GateIO买入加权价格
        gateio_price, gateio_enough_depth = calculate_weighted_price(
            gateio_orderbook, QUANTITY, 'asks'
        )

        # 获取Bitget合约订单簿
        contract_symbol = f"{SYMBOL}:USDT"
        bitget_orderbook = bitget.fetch_order_book(contract_symbol, 50)

        # 计算Bitget卖出加权价格 (考虑杠杆)
        contract_quantity = QUANTITY / LEVERAGE  # 由于10倍杠杆，实际合约数量为现货的1/10
        bitget_price, bitget_enough_depth = calculate_weighted_price(
            bitget_orderbook, contract_quantity, 'bids'
        )

        logger.info(f"GateIO加权买入价格: {gateio_price}, 深度足够: {gateio_enough_depth}")
        logger.info(f"Bitget加权卖出价格: {bitget_price}, 深度足够: {bitget_enough_depth}")

        if not gateio_enough_depth:
            logger.warning(f"GateIO订单簿深度不足以满足{QUANTITY}的交易量")

        if not bitget_enough_depth:
            logger.warning(f"Bitget订单簿深度不足以满足{contract_quantity}的交易量")

        return gateio_price, bitget_price, gateio_enough_depth and bitget_enough_depth
    except Exception as e:
        logger.error(f"获取深度价格失败: {e}")
        return None, None, False


# 计算价格差异
def calculate_price_difference(buy_price, sell_price):
    if buy_price is None or sell_price is None:
        return None

    difference = (sell_price - buy_price) / buy_price
    logger.info(f"价格差异: {difference:.6f}")
    return difference


# 执行交易
def execute_trades(gateio, bitget, gateio_price, bitget_price):
    try:
        # 在GateIO买入现货
        gateio_order = gateio.create_market_buy_order(
            SYMBOL,
            QUANTITY
        )
        logger.info(f"GateIO买入订单执行成功: {gateio_order}")

        # 在Bitget卖出合约
        # 由于使用10倍杠杆，实际需要的合约数量是现货数量的1/10
        contract_quantity = QUANTITY / LEVERAGE

        bitget_order = bitget.create_market_sell_order(
            f"{SYMBOL}:USDT",
            contract_quantity  # 由于10倍杠杆，实际合约数量为现货的1/10
        )
        logger.info(f"Bitget卖出订单执行成功 (10倍杠杆): {bitget_order}")

        # 计算理论利润
        theoretical_profit = (bitget_price - gateio_price) * QUANTITY
        logger.info(f"理论利润: {theoretical_profit} USDT")

        return gateio_order, bitget_order
    except Exception as e:
        logger.error(f"执行交易失败: {e}")
        return None, None


# 主函数
def main():
    logger.info("开始套保交易程序")

    try:
        # 初始化交易所
        gateio, bitget = init_exchanges()

        # 设置合约交易参数
        if not setup_contract_settings(bitget):
            logger.error("合约交易参数设置失败，程序退出")
            return

        while True:
            # 获取考虑深度的价格
            gateio_price, bitget_price, enough_depth = get_prices_with_depth(gateio, bitget)

            if gateio_price is None or bitget_price is None:
                logger.warning("价格获取失败，等待重试...")
                time.sleep(RETRY_DELAY)
                continue

            if not enough_depth:
                logger.warning("订单簿深度不足，等待市场深度恢复...")
                time.sleep(RETRY_DELAY)
                continue

            # 计算价格差异
            price_difference = calculate_price_difference(gateio_price, bitget_price)

            if price_difference is None:
                logger.warning("价格差异计算失败，等待重试...")
                time.sleep(RETRY_DELAY)
                continue

            # 检查价格差异是否满足条件
            if price_difference > THRESHOLD:
                logger.info(f"价格差异 {price_difference:.6f} 超过阈值 {THRESHOLD}，执行交易")

                # 执行交易
                gateio_order, bitget_order = execute_trades(gateio, bitget, gateio_price, bitget_price)

                if gateio_order and bitget_order:
                    logger.info("套保交易成功完成")
                    break
                else:
                    logger.error("交易执行失败，等待重试...")
                    time.sleep(RETRY_DELAY)
            else:
                logger.info(f"价格差异 {price_difference:.6f} 未达到阈值 {THRESHOLD}，等待...")
                time.sleep(RETRY_DELAY)

    except Exception as e:
        logger.error(f"程序执行错误: {e}")

    finally:
        logger.info("套保交易程序结束")


if __name__ == "__main__":
    main()