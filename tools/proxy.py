import requests
from tools.logger import logger
from config import proxies

def get_proxy_ip(proxies=proxies):
    """获取代理的出口IP"""
    try:
        response = requests.get('https://api.ipify.org?format=json',
                                proxies=proxies, timeout=10)
        if response.status_code == 200:
            proxy_ip = response.json()['ip']
            logger.info(f"获取到外网ip为： {proxy_ip}")
            return proxy_ip
        return "无法获取"
    except Exception as e:
        logger.info(f"获取代理IP失败: {e}")
        return "无法获取"

