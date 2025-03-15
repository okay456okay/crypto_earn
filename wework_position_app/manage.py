import sys
import os


# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将 config.py 所在的目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))

from config import position_app_receive_token, position_app_receive_aeskey, corp_id
from tools.logger import logger
from wework_position_app.wxcrypt import WXBizMsgCrypt
import xmltodict
from flask import Flask, request, abort
from wework_position_app.wecom_app import WECOM_APP

app = Flask(__name__)

wecom_app = WECOM_APP()

msg_ids = {}
@app.route('/wecom/receive/v2', methods=['POST', 'GET'])
def webhook():
    wxcpt = WXBizMsgCrypt(position_app_receive_token, position_app_receive_aeskey, corp_id)
    arg = (request.args)
    msg_signature = arg["msg_signature"]
    timestamp = arg["timestamp"]
    nonce = arg["nonce"]
    logger.info(f"request.args: {arg}")
    # URL验证
    if request.method == "GET":
        echostr = arg["echostr"]
        ret, echostr_decrypted = wxcpt.VerifyURL(msg_signature, timestamp, nonce, echostr)
        logger.info(f"verify url, ret: {ret}, echostr: {echostr}, echostr_decrypted: {echostr_decrypted}")
        if (ret != 0):
            error_message = f"ERR: VerifyURL ret: {ret}"
            logger.error(error_message)
            abort(403, error_message)
        return echostr_decrypted, 200
    elif request.method == "POST":
        ret, message = wxcpt.DecryptMsg(request.data, msg_signature, timestamp, nonce)
        message_dict = xmltodict.parse(message.decode())['xml']
        logger.info(f"接收到的企业微信消息内容：{message_dict}")
        userid = message_dict.get('FromUserName')
        if ret != 0:
            abort(403, "消息解密失败")
            return
        reply = "收到，思考中..."
        ret, replay_encrypted = wxcpt.EncryptMsg(reply, nonce, timestamp)
        msg_type = message_dict.get('MsgType', '')
        msg_event = message_dict.get('Event', '')
        # 消息去重处理
        if msg_type in ['image', 'text', 'event']:
            msg_id = message_dict.get('MsgId', '')
            if msg_id in msg_ids:
                return '重复消息', 200
            else:
                msg_ids[msg_id] = None
        if msg_type == 'event' and msg_event == 'enter_agent':
            # 进入会话
            content = f"请按照下面指令操作\n"
            content += f"""添加持仓操作: 发送格式:添加token,现货交易所,合约交易所, 如 '添加ETH,Bitget,Bybit'\n"""
            content += f"""删除持仓操作: 发送格式:删除token, 如 '删除ETH'\n"""
            wecom_app.txt_send2user(userid, content)
        elif msg_type == 'text':
            # 回复消息
            content = message_dict.get('Content')
            logger.info(f"收到{userid}消息: {content}")
            wecom_app.txt_send2user(userid, f"我已经按您的指示操作了")
        return replay_encrypted, 200
    else:
        logger.warning(f"Not support method: {request.method}")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=27005, debug=False, use_reloader=False)
