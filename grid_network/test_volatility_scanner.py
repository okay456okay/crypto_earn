#!/usr/bin/env python3
"""
Binance合约交易对波动率分析器 - 测试版本

这是一个简化的测试版本，只分析前10个交易对，用于快速验证功能。
"""

import sys
import os

# 添加父级目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from volatility_scanner import BinanceVolatilityScanner
except ImportError:
    print("错误：无法导入volatility_scanner模块")
    print("请确保volatility_scanner.py文件存在于同一目录下")
    sys.exit(1)


def test_volatility_scanner():
    """
    测试波动率分析器的基本功能
    """
    print("Binance合约交易对波动率分析器 - 测试版本")
    print("=" * 50)
    print("注意：这是测试版本，只分析前10个交易对")
    print()
    
    try:
        # 创建分析器实例
        scanner = BinanceVolatilityScanner()
        
        # 只分析前10个交易对进行测试
        results = scanner.scan_volatility(max_symbols=10)
        
        if not results:
            print("测试失败：无分析结果")
            return False
        
        # 显示结果
        scanner.display_results(results, top_n=10)
        
        # 导出结果（测试文件）
        scanner.export_results(results, filename="test_volatility_results.json")
        
        print(f"\n测试完成！成功分析了 {len(results)} 个交易对")
        print("如果结果正常，可以运行完整版本：python volatility_scanner.py")
        
        return True
        
    except Exception as e:
        print(f"测试失败：{e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_volatility_scanner()
    if success:
        print("\n✅ 测试通过！")
    else:
        print("\n❌ 测试失败！请检查配置和网络连接。") 