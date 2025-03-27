import logging
import os
from logging.handlers import RotatingFileHandler

logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")

# 设置两个日志文件路径
log_file_path = os.path.join(logs_dir, "crypto.log")
debug_log_file_path = os.path.join(logs_dir, "debug.log")

# 确保logs目录存在
os.makedirs(logs_dir, exist_ok=True)

# 创建正式logger
logger = logging.getLogger("crypto")
logger.setLevel(logging.INFO)

# 创建调试logger
debug_logger = logging.getLogger("debug")
debug_logger.setLevel(logging.DEBUG)

# 正式日志的格式
formal_formatter = logging.Formatter("[%(asctime)-15s] %(levelname)s %(filename)s:%(lineno)d): %(message)s")

# 调试日志的格式
debug_formatter = logging.Formatter("[%(asctime)-15s] %(levelname)s %(filename)s:%(lineno)d) (%(funcName)s(): %(message)s")

# 正式日志的处理器
formal_file_handler = RotatingFileHandler(
    log_file_path,
    maxBytes=20 * 1024 * 1024,  # 20MB
    backupCount=20,
)
formal_file_handler.setFormatter(formal_formatter)
formal_console_handler = logging.StreamHandler()
formal_console_handler.setFormatter(formal_formatter)

logger.addHandler(formal_file_handler)
logger.addHandler(formal_console_handler)

# 调试日志的处理器
debug_file_handler = RotatingFileHandler(
    debug_log_file_path,
    maxBytes=50 * 1024 * 1024,  # 50MB，调试日志通常更大
    backupCount=10,
)
debug_file_handler.setFormatter(debug_formatter)
debug_console_handler = logging.StreamHandler()
debug_console_handler.setFormatter(debug_formatter)

debug_logger.addHandler(debug_file_handler)
debug_logger.addHandler(debug_console_handler)
