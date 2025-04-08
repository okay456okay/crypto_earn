import requests

from tools.logger import logger


class WeChatWorkBot:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_message(self, content, mentioned_list=None):
        """发送企业微信群机器人消息"""
        data = {
            "msgtype": "markdown",
            "markdown": {
                "content": content,
            }
        }

        if mentioned_list:
            data["text"]["mentioned_list"] = mentioned_list

        try:
            logger.debug(f"开始发送企微消息，webhook_url: {self.webhook_url}, data: {data}")
            response = requests.post(self.webhook_url, json=data)
            result = response.json()

            if result["errcode"] == 0:
                logger.info("企业微信群消息发送成功")
                return True
            else:
                logger.error(f"企业微信群消息发送失败: {result}")
                return False
        except Exception as e:
            logger.error(f"发送企业微信群消息时出错: {str(e)}")
            return False
