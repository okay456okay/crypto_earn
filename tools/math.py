def calculate_order_quantity(price):
    """
    根据价格计算加密货币下单数量，确保下单金额在6-9 USDT之间，且下单数量尽量取整

    参数:
        price: 加密货币当前单价（USDT）

    返回:
        dict: 包含下单数量和预计下单金额的字典
    """
    # 目标金额范围
    min_amount = 6
    max_amount = 9
    target_amount = 8  # 优先接近这个金额

    # 计算目标数量范围
    min_quantity = min_amount / price
    max_quantity = max_amount / price

    # 找出数量的数量级
    magnitude = 0
    temp = max_quantity
    if temp >= 1:
        # 处理数量大于等于1的情况
        while temp >= 10:
            temp /= 10
            magnitude += 1
    else:
        # 处理数量小于1的情况
        while temp < 1:
            temp *= 10
            magnitude -= 1

    # 基于数量级，确定可能的整数单位
    possible_units = []

    if magnitude >= 3:  # 1000及以上
        possible_units = [1000, 500, 100, 50]
    elif magnitude == 2:  # 100-999
        possible_units = [100, 50, 10, 5, 1]
    elif magnitude == 1:  # 10-99
        possible_units = [10, 5, 1, 0.5, 0.1]
    elif magnitude == 0:  # 1-9.99
        possible_units = [1, 0.5, 0.1, 0.05, 0.01]
    elif magnitude == -1:  # 0.1-0.99
        possible_units = [0.1, 0.05, 0.01, 0.005, 0.001]
    elif magnitude == -2:  # 0.01-0.099
        possible_units = [0.01, 0.005, 0.001]
    else:  # 0.009及以下
        possible_units = [0.001, 0.0005, 0.0001]

    # 寻找最佳数量
    best_quantity = None
    best_diff = float('inf')

    for unit in possible_units:
        # 计算能够满足金额要求的数量，取整到单位
        target_quantity = target_amount / price
        rounded_quantity = round(target_quantity / unit) * unit

        # 确保数量在允许范围内
        if min_quantity <= rounded_quantity <= max_quantity:
            amount = rounded_quantity * price
            diff = abs(amount - target_amount)

            if diff < best_diff:
                best_diff = diff
                best_quantity = rounded_quantity

        # 如果还没找到合适的，尝试单位的整数倍
        if best_quantity is None:
            for multiplier in [1, 2, 5, 10]:
                quantity = int(min_quantity / (unit * multiplier) + 0.999) * (unit * multiplier)
                if min_quantity <= quantity <= max_quantity:
                    amount = quantity * price
                    diff = abs(amount - target_amount)

                    if diff < best_diff:
                        best_diff = diff
                        best_quantity = quantity

    # 如果仍然没有找到合适的值，退回到简单的方法
    if best_quantity is None:
        target_quantity = target_amount / price

        # 尝试不同的精度
        for decimals in [0, 1, 2, 3]:
            rounded = round(target_quantity, decimals)
            if min_quantity <= rounded <= max_quantity:
                best_quantity = rounded
                break

        # 最后的保底方案
        if best_quantity is None:
            best_quantity = round((min_quantity + max_quantity) / 2, 3)

    # 计算实际下单金额
    actual_amount = best_quantity * price

    return {
        "quantity": best_quantity,
        "estimated_amount": actual_amount
    }


# 测试函数
def test_calculate_order_quantity():
    test_prices = [0.005163, 0.223850, 0.059310, 12.816, 0.3689, 3120.32]
    print("价格(USDT)\t数量\t\t预计金额(USDT)")
    print("-" * 50)

    for price in test_prices:
        result = calculate_order_quantity(price)
        print(f"{price}\t\t{result['quantity']}\t\t{result['estimated_amount']:.2f}")


# 运行测试
if __name__ == "__main__":
    test_calculate_order_quantity()