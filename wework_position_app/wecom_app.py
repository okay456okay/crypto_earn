#!/usr/bin/env python3
# coding=utf-8
import requests
from tools.logger import logger
from config import corp_id, position_app_agent_id, position_app_agent_secret


class WECOM_APP(object):
    def __init__(self, corp_id=corp_id, agent_id=position_app_agent_id, agent_secret=position_app_agent_secret):
        self.corp_id = corp_id
        self.agent_id = agent_id
        self.agent_secret = agent_secret
        self.s = requests.session()
        self.access_token = None

    def get_token(self):
        url = f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={self.corp_id}&corpsecret={self.agent_secret}"
        r = self.s.get(url=url)
        logger.info(f"get access_token, url: {url}, response: {r.status_code}:{r.text}")
        self.access_token = r.json()['access_token']

    def txt_send2user(self, userid, text):
        self.get_token()
        url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={self.access_token}"
        data = {
            "touser": userid,
            "msgtype": "text",
            "agentid": self.agent_id,
            "text": {
                "content": text,
            },
            "safe": "0"
        }
        r = self.s.post(url=url, json=data)
        logger.info(f"send message to user, url: {url}, data: {data}, response: {r.status_code}:{r.text}")
        if r.json().get('errcode', 0) == 42001:
            url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={self.access_token}"
            r = self.s.post(url=url, json=data)
            logger.info(f"send message to user, url: {url}, data: {data}, response: {r.status_code}:{r.text}")
    def markdown_send2user(self, userid, text):
        self.get_token()
        url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={self.access_token}"
        data = {
            "touser": userid,
            "msgtype": "markdown",
            "agentid": self.agent_id,
            "markdown": {
                "content": text,
            },
            "safe": "0"
        }
        r = self.s.post(url=url, json=data)
        logger.info(f"send message to user, url: {url}, data: {data}, response: {r.status_code}:{r.text}")
        if r.json().get('errcode', 0) == 42001:
            url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={self.access_token}"
            r = self.s.post(url=url, json=data)
            logger.info(f"send message to user, url: {url}, data: {data}, response: {r.status_code}:{r.text}")

    def get_all_userids(self):
        self.get_token()
        userids = []
        data = {"limit": 1000}
        r = self.s.post(url=f"https://qyapi.weixin.qq.com/cgi-bin/user/list_id?access_token={self.access_token}",
                        json=data)
        while True:
            for user in r.json().get('dept_user', []):
                userids.append(user.get('userid'))
            if "next_cursor" in r.json():
                data['cursor'] = r.json()['next_cursor']
                r = self.s.post(
                    url=f"https://qyapi.weixin.qq.com/cgi-bin/user/list_id?access_token={self.access_token}",
                    json=data)
            else:
                break
        return userids

    def get_user(self, user_id):
        self.get_token()
        user_info = {}
        r = self.s.get(url=f"https://qyapi.weixin.qq.com/cgi-bin/user/get?access_token={self.access_token}&userid={user_id}")
        if r.json().get('errcode') == 0:
            user_info = r.json()
        return user_info


class WECOM_BOT(object):
    """
    使用群聊机器人发送短信
    """
    def __init__(self, key, url='https://qyapi.weixin.qq.com/cgi-bin/webhook/send'):
        # test-robot
        # https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=dd0cf6fc-3878-47f4-b2f7-95115ab7b005
        self.url = f"{url}?key={key}"

    # 文本类型消息
    def send_txt(self, message, mentioned_list=[]):
        """
        "mentioned_list":["wangqing","@all"],
        """
        headers = {"Content-Type": "text/plain"}
        send_data = {
            "msgtype": "text",  # 消息类型
            "text": {
                "content": message,  # 文本内容，最长不超过2048个字节，必须是utf8编码
                "mentioned_list": mentioned_list,
                # userid的列表，提醒群中的指定成员(@某个成员)，@all表示提醒所有人，如果开发者获取不到userid，可以使用mentioned_mobile_list
                # "mentioned_mobile_list": ["13163750276"]  # 手机号列表，提醒手机号对应的群成员(@某个成员)，@all表示提醒所有人
            }
        }
        res = requests.post(url=self.url, headers=headers, json=send_data)
        print(res.text)

    def send_markdown(self, markdown, mentioned_list=[]):
        headers = {"Content-Type": "text/plain"}
        send_data = {
            "msgtype": "markdown",  # 消息类型，此时固定为markdown
            "markdown": {
                "content": markdown + ''.join([f'<@{i}>' for i in mentioned_list]),
            }
        }
        res = requests.post(url=self.url, headers=headers, json=send_data)
        print(res.text)


if __name__ == "__main__":
    # w = WECOM_APP(agent_secret=AGENT_SECRET_CONTACT)
    # userids = w.get_all_userids()
    # print(w.get_user(userids[0]))
    pass