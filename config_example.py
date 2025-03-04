# Binance API密钥（请替换为您自己的密钥）
# zxl
api_key = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
api_secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

proxies = {
    # 'http': 'socks5://127.0.0.1:7890',
    # 'https': 'socks5://127.0.0.1:7890',
    # 如果不用代理，把下面这两行注释掉
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
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
