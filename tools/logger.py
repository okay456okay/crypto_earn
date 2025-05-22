import logging
import os
from logging.handlers import RotatingFileHandler

logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")

# 设置两个日志文件路径
log_file_path = os.path.join(logs_dir, "crypto.log")
debug_log_file_path = os.path.join(logs_dir, "debug.log")

# 确保logs目录存在
os.makedirs(logs_dir, exist_ok=True)

# 创建logger
logger = logging.getLogger("crypto")
logger.setLevel(logging.INFO)  # 设置最低级别为DEBUG，让所有级别的日志都能被处理

# 日志格式
formatter = logging.Formatter("[%(asctime)-15s] %(levelname)s %(filename)s:%(lineno)d) %(message)s")

# 正式日志处理器 - 处理INFO及以上级别
info_file_handler = RotatingFileHandler(
    log_file_path,
    maxBytes=20 * 1024 * 1024,  # 20MB
    backupCount=20,
)
info_file_handler.setLevel(logging.INFO)
info_file_handler.setFormatter(formatter)

# 调试日志处理器 - 处理DEBUG级别
debug_file_handler = RotatingFileHandler(
    debug_log_file_path,
    maxBytes=50 * 1024 * 1024,  # 50MB
    backupCount=10,
)
debug_file_handler.setLevel(logging.DEBUG)
debug_file_handler.setFormatter(formatter)

# 控制台处理器 - 处理所有级别
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)

# 添加所有处理器到logger
logger.addHandler(info_file_handler)
logger.addHandler(debug_file_handler)
logger.addHandler(console_handler)
