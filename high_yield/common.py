from config import yield_percentile


def get_percentile(data, percentile=yield_percentile, reverse=True):
    """
    手动计算数组的P95值

    参数:
    data -- 数字列表

    返回:
    p95 -- 95百分位数值
    """
    # 排序数据
    if not data:
        return 0.0
    sorted_data = sorted(data, reverse=reverse)
    # 计算位置 (使用最近秩方法)
    n = len(sorted_data)
    position = int(percentile / 100 * n)
    # 如果位置是整数，取该位置的值
    if position < n:
        return sorted_data[position]
    # 如果我们恰好落在最后一个元素位置之外，返回最后一个元素
    return sorted_data[-1]
