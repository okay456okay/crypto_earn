import sys
import os

# 获取当前脚本的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将 config.py 所在的目录添加到系统路径
sys.path.append(os.path.join(current_dir, '..'))

from config import position_app_receive_token, position_app_receive_aeskey, corp_id, admin_userid
from tools.logger import logger
from wework_position_app.wxcrypt import WXBizMsgCrypt
import xmltodict
from flask import Flask, request, abort
from wework_position_app.wecom_app import WECOM_APP
from high_yield.user_manager import UserManager
from high_yield.token_manager import TokenManager

app = Flask(__name__)

wecom_app = WECOM_APP()
user_manager = UserManager()
token_manager = TokenManager()

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
        guideline = f"请按照下面指令操作:\n"
        guideline += f"""添加持仓操作: 发送格式:添加token,现货交易所,合约交易所, 如 添加ETH,Bitget,Bybit\n"""
        guideline += f"""更新持仓操作: 发送格式:更新token,现货交易所,合约交易所, 如 更新ETH,Bitget,Bybit\n"""
        guideline += f"""删除持仓操作: 发送格式:删除token, 如 删除ETH\n"""
        guideline += f"""查询持仓操作: 发送: 查询持仓\n"""
        guideline += f"""交易所名称: Binance, Bitget, Bybit, GateIO, OKX(大小写必须保持一致)\n"""
        if msg_type in ['image', 'text', 'event']:
            msg_id = message_dict.get('MsgId', '')
            if msg_id in msg_ids:
                return '重复消息', 200
            else:
                msg_ids[msg_id] = None
        if msg_type == 'event' and msg_event == 'enter_agent':
            # 进入会话
            wecom_app.txt_send2user(userid, guideline)
        elif msg_type == 'text':
            # 回复消息
            content = message_dict.get('Content').strip()
            logger.info(f"收到{userid}消息: {content}")
            users = user_manager.query_users(wecom_userid=userid)
            logger.info(f"get users with userid: {userid}, users: {users}")
            if not users:
                wecom_app.txt_send2user(admin_userid, f"未在user表里找到{userid}用户，请尽快添加，谢谢")
                wecom_app.txt_send2user(userid, "未完成配置，请联系管理员处理，谢谢")
                return replay_encrypted, 200
            user_id = users[0]['id']
            if content.startswith('添加') or content.startswith('更新'):
                token, spot_exchange, future_exchange = content.replace('添加', '').replace('更新', '').split(',')
                user_token = token_manager.query_tokens(user_id=user_id, token=token)
                if user_token:
                    token_id = user_token[0].get('id')
                    token_manager.update_token(
                        token_id=token_id,
                        data={'spot_exchange': spot_exchange, 'future_exchange': future_exchange}
                    )
                else:
                    token_manager.insert_token({
                        "user_id": user_id,
                        "spot_exchange": spot_exchange,
                        "future_exchange": future_exchange,
                        "token": token
                    })
            elif content.startswith('删除'):
                token = content.replace('删除', '')
                user_token = token_manager.query_tokens(user_id=user_id, token=token)
                if user_token:
                    token_id = user_token[0].get('id')
                    token_manager.update_token(token_id=token_id, data={'is_deleted': 1})
            elif content.startswith('查询'):
                tokens = token_manager.query_tokens(user_id=user_id)
                tokens_str = '\n'.join([f"{t['token']}:{t['spot_exchange']}(现货交易所),{t['future_exchange']}(合约交易所)" for t in tokens])
                wecom_app.txt_send2user(userid, f"最新持仓如下：\n{tokens_str}")
                return replay_encrypted, 200
            else:
                wecom_app.txt_send2user(userid, f"指令有误，{guideline}")
                return replay_encrypted, 200
            tokens = token_manager.query_tokens(user_id=user_id)
            tokens_str = '\n'.join(
                [f"{t['token']}:{t['spot_exchange']}(现货交易所),{t['future_exchange']}(合约交易所)" for t in tokens])
            wecom_app.txt_send2user(userid, f"已经完成操作，最新持仓如下：\n{tokens_str}")
        return replay_encrypted, 200
    else:
        logger.warning(f"Not support method: {request.method}")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=27005, debug=False, use_reloader=False)
