import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from typing import List, Dict, Any, Optional
from config import binance_api_secret, binance_api_key, proxies
from tools.logger import logger


def get_binance_flexible_savings(api_key: str, api_secret: str, proxies) -> List[Dict[str, Any]]:
    """
    获取币安账户中持仓的活期理财产品清单，使用Session和代理

    参数:
        binance_api_key (str): 币安API密钥
        binance_api_secret (str): 币安API密钥密文
        proxy (str): 代理地址，默认为 "127.0.0.1:7890"

    返回:
        List[Dict[str, Any]]: 账户持仓的活期理财产品清单列表，每个产品包含以下信息:
            - asset: 资产名称
            - totalAmount: 总持仓量
            - dailyInterestRate: 日利率
            - annualInterestRate: 年化利率(%)
            - totalInterest: 累计收益
            - freeAmount: 可赎回数量
            - productName: 产品名称
    """
    base_url = 'https://api.binance.com'

    # 创建会话
    session = requests.Session()
    session.headers.update({'X-MBX-APIKEY': api_key})
    session.proxies.update(proxies)

    # 获取服务器时间
    def get_server_time() -> int:
        endpoint = '/api/v3/time'
        url = base_url + endpoint
        try:
            response = session.get(url)
            response.raise_for_status()
            return response.json()['serverTime']
        except Exception as e:
            raise Exception(f"获取服务器时间失败: {str(e)}")

    # 创建签名
    def create_signature(query_string: str) -> str:
        return hmac.new(
            api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    # 获取用户活期理财持仓
    def get_flexible_savings_positions() -> List[Dict[str, Any]]:
        endpoint = '/sapi/v1/simple-earn/flexible/position'  # 更新为正确的端点

        # 构建请求参数
        params = {
            'timestamp': get_server_time(),
            'limit': 100  # 增加限制参数，确保获取所有持仓
        }

        # 创建查询字符串
        query_string = urlencode(params)

        # 添加签名
        params['signature'] = create_signature(query_string)

        # 发送请求
        url = base_url + endpoint
        try:
            response = session.get(url, params=params)
            response.raise_for_status()
            result = response.json()
            # 币安API可能返回嵌套结构，需要检查
            if isinstance(result, dict) and 'rows' in result:
                return result['rows']
            return result
        except Exception as e:
            raise Exception(
                f"获取活期理财持仓失败: {str(e)} - 响应内容: {response.text if 'response' in locals() else '无响应'}")

    # 获取活期理财产品详情
    def get_product_details() -> Dict[str, Dict[str, Any]]:
        endpoint = '/sapi/v1/simple-earn/flexible/list'  # 更新为正确的端点

        # 构建请求参数
        params = {
            'timestamp': get_server_time(),
            'limit': 100,  # 增加限制参数
            'status': 'SUBSCRIBABLE'  # 可购买的产品
        }

        # 创建查询字符串
        query_string = urlencode(params)

        # 添加签名
        params['signature'] = create_signature(query_string)

        # 发送请求
        url = base_url + endpoint
        try:
            response = session.get(url, params=params)
            response.raise_for_status()
            result = response.json()

            # 币安API可能返回嵌套结构，需要检查
            if isinstance(result, dict) and 'rows' in result:
                products = result['rows']
            else:
                products = result

            # 创建产品详情映射表
            product_details = {}
            for product in products:
                asset = product.get('asset')
                product_details[asset] = product

            return product_details
        except Exception as e:
            raise Exception(
                f"获取活期理财产品列表失败: {str(e)} - 响应内容: {response.text if 'response' in locals() else '无响应'}")

    try:
        # 获取用户活期理财持仓信息
        positions = get_flexible_savings_positions()

        # 如果没有持仓，返回空列表
        if not positions:
            return []

        # 获取产品详情
        product_details = get_product_details()

        # 丰富持仓信息
        enriched_positions = []
        for position in positions:
            asset = position.get('asset')

            # 处理利率信息 - 根据实际返回的API结构调整
            daily_interest_rate = position.get('dailyInterestRate', position.get('airRate', '0'))

            # 添加年化利率 - 如果API已直接提供年化利率，则使用API返回值
            if 'annualInterestRate' not in position:
                try:
                    annual_interest_rate = float(daily_interest_rate) * 365 * 100
                    position['annualInterestRate'] = annual_interest_rate
                except (ValueError, TypeError):
                    position['annualInterestRate'] = 0

            # 添加产品名称
            if asset in product_details:
                position['productName'] = product_details[asset].get('productName', product_details[asset].get('name',
                                                                                                               f"{asset}活期理财"))
            else:
                position['productName'] = position.get('productName', position.get('name', f"{asset}活期理财"))

            enriched_positions.append(position)

        return enriched_positions

    except Exception as e:
        raise Exception(f"获取币安活期理财产品清单失败: {str(e)}")
    finally:
        # 关闭会话
        session.close()


# 使用示例
if __name__ == "__main__":
    # 示例API密钥(请替换为您的实际密钥)
    try:
        savings_list = get_binance_flexible_savings(binance_api_key, binance_api_secret, proxies)

        if not savings_list:
            logger.info("您当前没有活期理财产品持仓。")
        else:
            logger.info("\n您的活期理财产品持仓清单：")
            logger.info("-" * 90)
            logger.info(f"{'资产':<8}{'产品名称':<20}{'总额':<15}{'年化收益率':<15}{'累计收益':<15}{'可赎回数量'}")
            logger.info("-" * 90)

            for item in savings_list:
                asset = item.get('asset', 'N/A')
                product_name = item.get('productName', f"{asset}活期理财")
                total_amount = item.get('totalAmount', item.get('amount', '0'))
                annual_rate = item.get('annualInterestRate', 0)
                earned_amount = item.get('totalInterest', item.get('interest', '0'))
                free_amount = item.get('freeAmount', item.get('redeemableAmount', '0'))

                logger.info(
                    f"{asset:<8}{product_name:<20}{total_amount:<15}{annual_rate:.2f}%{earned_amount:<15}{free_amount}")

            logger.info("-" * 90)

    except Exception as e:
        logger.info(f"错误: {e}")