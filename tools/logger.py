import logging
import os
from logging.handlers import RotatingFileHandler

logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")
log_file_path = os.path.join(logs_dir, "crypto_yield_monitor.log")

# 确保logs目录存在
os.makedirs(logs_dir, exist_ok=True)

# 设置日志文件路径

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)-15s] %(name)s %(levelname)s (%(funcName)s(), %(filename)s:%(lineno)d): %(message)s",
    handlers=[
        RotatingFileHandler(
            log_file_path,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=20,  # 保留20个文件
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("crypto_yield_monitor")
