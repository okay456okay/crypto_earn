# coding=utf-8
"""
数学计算方式
"""

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

    # 找到数量的数量级
    if max_quantity >= 1000:
        nice_numbers = [10000, 8000, 6000, 5000, 3000, 2000, 1500, 1000]
    elif max_quantity >= 100:
        nice_numbers = [1000, 800, 600, 500, 300, 200, 150, 100, 80, 50]
    elif max_quantity >= 10:
        nice_numbers = [100, 80, 50, 30, 20, 15, 10, 5, 3, 2, 1]
    elif max_quantity >= 1:
        nice_numbers = [10, 8, 5, 3, 2, 1.5, 1, 0.5, 0.4, 0.2, 0.1]
    elif max_quantity >= 0.1:
        nice_numbers = [1, 0.8, 0.5, 0.3, 0.2, 0.1, 0.05, 0.03, 0.02, 0.01]
    elif max_quantity >= 0.01:
        nice_numbers = [0.1, 0.08, 0.06, 0.05, 0.03, 0.02, 0.01, 0.005, 0.002, 0.001]
    else:
        nice_numbers = [0.01, 0.005, 0.002, 0.001, 0.0005, 0.0001]

    # 找到在范围内的最合适的"整数"
    best_quantity = None
    best_diff = float('inf')

    # 先尝试从大到小的整数
    for number in nice_numbers:
        if min_quantity <= number <= max_quantity:
            amount = number * price
            # 确保金额在6-9范围内
            if min_amount <= amount <= max_amount:
                diff = abs(amount - target_amount)
                if diff < best_diff:
                    best_diff = diff
                    best_quantity = number

    # 如果没有找到合适的整数，尝试基于目标金额寻找
    if best_quantity is None:
        target_quantity = target_amount / price

        # 尝试找到最接近target_quantity的"整数"
        for number in nice_numbers:
            # 找到number的最接近倍数
            multiplier = round(target_quantity / number)
            if multiplier <= 0:  # 避免乘以0或负数
                continue

            quantity = number * multiplier
            amount = quantity * price

            # 检查是否在金额范围内
            if min_amount <= amount <= max_amount:
                diff = abs(amount - target_amount)
                if diff < best_diff:
                    best_diff = diff
                    best_quantity = quantity

    # 如果仍然没有找到合适的值，找一个最接近数量的"整数"
    if best_quantity is None:
        mid_quantity = (min_quantity + max_quantity) / 2

        # 尝试找到最接近mid_quantity的"整数"
        for number in nice_numbers:
            # 找到number的最接近倍数
            multiplier = round(mid_quantity / number)
            if multiplier <= 0:  # 避免乘以0或负数
                continue

            quantity = number * multiplier

            # 检查是否在数量范围内
            if min_quantity <= quantity <= max_quantity:
                diff = abs(quantity - mid_quantity)
                if diff < best_diff:
                    best_diff = diff
                    best_quantity = quantity

    # 最后的保底方案：如果上述方法都失败，直接取中间值并四舍五入
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
    test_prices = [0.005163, 0.223850, 0.059310, 12.816, 0.3689, 3120.32, 153.2]
    print("价格(USDT)\t数量\t\t预计金额(USDT)")
    print("-" * 50)

    for price in test_prices:
        result = calculate_order_quantity(price)
        print(f"{price}\t\t{result['quantity']}\t\t{result['estimated_amount']:.2f}")


# 运行测试
if __name__ == "__main__":
    test_calculate_order_quantity()