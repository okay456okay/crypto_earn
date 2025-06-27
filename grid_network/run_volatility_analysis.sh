#!/bin/bash
# Binance合约交易对波动率分析器启动脚本

echo "==================================================="
echo "       Binance合约交易对波动率分析器"
echo "==================================================="
echo ""

# 检查是否在正确的目录
if [ ! -f "volatility_scanner.py" ]; then
    echo "❌ 错误：请在grid_network目录下运行此脚本"
    echo "   当前目录：$(pwd)"
    echo "   正确用法：cd grid_network && ./run_volatility_analysis.sh"
    exit 1
fi

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "❌ 错误：未找到python3命令"
    echo "   请确保已安装Python 3.x"
    exit 1
fi

# 检查配置文件
if [ ! -f "../config.py" ]; then
    echo "❌ 错误：未找到config.py配置文件"
    echo ""
    echo "请执行以下步骤配置API密钥："
    echo "1. 复制配置模板：cp ../config_example.py ../config.py"
    echo "2. 编辑config.py文件，填入您的Binance API密钥"
    echo "3. 重新运行此脚本"
    exit 1
fi

echo "✅ 环境检查通过"
echo ""

# 询问用户选择运行模式
echo "请选择运行模式："
echo "1) 测试模式 (分析前10个交易对，快速验证)"
echo "2) 完整模式 (分析所有交易对，需要几分钟)"
echo ""
read -p "请输入选择 (1 或 2): " choice

case $choice in
    1)
        echo ""
        echo "🚀 启动测试模式..."
        echo "============================================"
        python3 test_volatility_scanner.py
        ;;
    2)
        echo ""
        echo "🚀 启动完整分析模式..."
        echo "⚠️  这可能需要几分钟时间，请耐心等待..."
        echo "============================================"
        python3 volatility_scanner.py
        ;;
    *)
        echo "❌ 无效选择，退出"
        exit 1
        ;;
esac

echo ""
echo "============================================"
echo "✅ 分析完成！"
echo ""
echo "📁 分析结果文件已保存在当前目录下"
echo "📖 详细说明请查看：README_volatility_scanner.md"
echo "============================================" 