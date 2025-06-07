# -*- coding: utf-8 -*-
"""
多交易所API配置示例文件
请复制此文件为 config.py 并填入真实的API密钥

安全提醒：
1. 请勿将真实的API密钥提交到版本控制系统
2. 建议只给API密钥最小必要的权限（期货交易权限）
3. 启用IP白名单以增加安全性
"""

# Binance API配置
binance_api_key = "your_binance_api_key_here"
binance_api_secret = "your_binance_api_secret_here"

# Gate.io API配置
gateio_api_key = "your_gateio_api_key_here"
gateio_api_secret = "your_gateio_api_secret_here"

# Bybit API配置
bybit_api_key = "your_bybit_api_key_here"
bybit_api_secret = "your_bybit_api_secret_here"

# Bitget API配置
bitget_api_key = "your_bitget_api_key_here"
bitget_api_secret = "your_bitget_api_secret_here"

# 代理配置（可选）
proxies = {
    # 'http': 'http://proxy.example.com:8080',
    # 'https': 'https://proxy.example.com:8080'
}

# 交易所特殊配置（可选）
exchange_specific_configs = {
    'binance': {
        'testnet': False,  # 是否使用测试网
        'sandbox': False,  # 是否使用沙盒环境
    },
    'bybit': {
        'testnet': False,
        'demo': False,  # 是否使用模拟交易
    },
    'gateio': {
        'testnet': False,
    },
    'bitget': {
        'testnet': False,
        'sandbox': False,
    }
}

import logging
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    # format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    format="[%(asctime)-15s] %(name)s %(levelname)s (%(funcName)s(), %(filename)s:%(lineno)d): %(message)s",
    handlers=[
        logging.FileHandler("crypto_yield_monitor.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("crypto_yield_monitor")
