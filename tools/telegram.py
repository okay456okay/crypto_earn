import requests
import json
from config import proxies


class TelegramBot:
    def __init__(self, bot_token):
        self.bot_token = bot_token
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.updates = None

    def send_message(self, chat_id, text, parse_mode=None):
        """
        发送文本消息

        Args:
            chat_id: 接收者ID（用户ID、群组ID或频道ID）
            text: 消息内容
            parse_mode: 解析模式 ('Markdown' 或 'HTML')
        """
        url = f"{self.base_url}/sendMessage"

        data = {
            'chat_id': chat_id,
            'text': text
        }

        if parse_mode:
            data['parse_mode'] = parse_mode

        try:
            response = requests.post(url, data=data, proxies=proxies)
            result = response.json()

            if result['ok']:
                print("消息发送成功!")
                return result['result']
            else:
                print(f"发送失败: {result['description']}")
                return None

        except Exception as e:
            print(f"发送异常: {str(e)}")
            return None

    def send_photo(self, chat_id, photo_path, caption=None):
        """
        发送图片消息

        Args:
            chat_id: 接收者ID
            photo_path: 图片文件路径
            caption: 图片说明文字
        """
        url = f"{self.base_url}/sendPhoto"

        data = {'chat_id': chat_id}
        if caption:
            data['caption'] = caption

        try:
            with open(photo_path, 'rb') as photo:
                files = {'photo': photo}
                response = requests.post(url, data=data, files=files, proxies=proxies)
                result = response.json()

                if result['ok']:
                    print("图片发送成功!")
                    return result['result']
                else:
                    print(f"发送失败: {result['description']}")
                    return None

        except Exception as e:
            print(f"发送异常: {str(e)}")
            return None

    def send_document(self, chat_id, document_path, caption=None):
        """
        发送文档

        Args:
            chat_id: 接收者ID
            document_path: 文档文件路径
            caption: 文档说明文字
        """
        url = f"{self.base_url}/sendDocument"

        data = {'chat_id': chat_id}
        if caption:
            data['caption'] = caption

        try:
            with open(document_path, 'rb') as document:
                files = {'document': document}
                response = requests.post(url, data=data, files=files, proxies=proxies)
                result = response.json()

                if result['ok']:
                    print("文档发送成功!")
                    return result['result']
                else:
                    print(f"发送失败: {result['description']}")
                    return None

        except Exception as e:
            print(f"发送异常: {str(e)}")
            return None

    def get_updates(self, offset=-2):
        """
        获取Bot接收到的消息（用于获取Chat ID）

        Args:
            offset: 消息偏移量
        """
        url = f"{self.base_url}/getUpdates"

        params = {}
        if offset:
            params['offset'] = offset

        try:
            response = requests.get(url, params=params, proxies=proxies)
            result = response.json()

            if result['ok']:
                self.updates = result['result']
                return result['result']
            else:
                print(f"获取消息失败: {result['description']}")
                return None

        except Exception as e:
            print(f"获取消息异常: {str(e)}")
            return None

    def print_updates(self):
        if self.updates:
            for update in self.updates[-5:]:  # 显示最近5条消息
                print(f"Update ID: {update.get('update_id')}")
                print(json.dumps(update, indent=2))
                # if 'message' in update:
                #     message = update['message']
                #     print(f"Chat ID: {message['chat']['id']}")
                #     print(f"User: {message['from']['first_name']}")
                #     print(f"Text: {message.get('text', 'No text')}")
                #     print("---")


# 使用示例
if __name__ == "__main__":
    from config import telegram_stability_finance_bot

    # 替换为你的Bot Token
    BOT_TOKEN = telegram_stability_finance_bot
    bot = TelegramBot(BOT_TOKEN)

    # bot.get_updates()
    # bot.print_updates()
    # exit(0)
    # 替换为目标Chat ID
    # CHAT_ID = "5853083031"  # 用户ID,  获取方法: 发送消息给 @userinfobot
    # CHAT_ID = "-4873585666"  # 币圈理财群组, 群组ID: 将机器人加到群组，在群组发送消息，然后调用 bot.get_updates()方法获取消息
    # CHAT_ID = "@yourchannel"  # 频道用户名, 公开频道
    CHAT_ID = "@stability_finance"  #  公开频道
    # CHAT_ID = "-1002659747033"  #  stability_finance_channel 频道用户名, 私有频道。 超级群组 Chat ID和频道 Chat ID都是-100开头


    # 发送简单文本消息
    bot.send_message(CHAT_ID, "Hello from Python Bot!")

    # 发送格式化消息（Markdown格式）
    markdown_text = """
*服务器状态通知*

🔴 *状态*: 离线
⏰ *时间*: 2024-05-23 10:30:00
📊 *CPU使用率*: 95%
💾 *内存使用率*: 87%

请及时检查服务器状态！
    """
    bot.send_message(CHAT_ID, markdown_text, parse_mode='Markdown')

    # 发送HTML格式消息
    html_text = """
<b>系统告警</b>

🚨 <strong>警告</strong>: 磁盘空间不足
📍 <em>服务器</em>: web-server-01
💽 <code>可用空间: 2.1GB / 100GB</code>

<a href="https://monitoring.example.com">查看详细监控</a>
    """
    bot.send_message(CHAT_ID, html_text, parse_mode='HTML')

    # 发送图片（需要本地有图片文件）
    # bot.send_photo(CHAT_ID, "screenshot.png", "服务器监控截图")

    # 发送文档
    # bot.send_document(CHAT_ID, "report.pdf", "每日系统报告")

    # 获取最新消息（用于调试和获取Chat ID）
    bot.get_updates()
    bot.print_updates()
