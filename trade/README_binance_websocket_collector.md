# Binance WebSocket价格数据收集器

## 概述

`binance_price_websocket_collector.py` 是一个专业的实时价格数据收集工具，使用Binance WebSocket API收集所有合约交易对的价格数据，并将其存储到sqlite3数据库中。

## 主要功能

- 🔄 **实时数据收集**: 使用WebSocket API实时接收所有合约交易对的价格ticker数据
- 💾 **数据库存储**: 自动将价格数据存储到sqlite3数据库，便于后续分析
- 🔧 **自动重连**: 支持网络断线自动重连，确保数据收集的连续性
- 📊 **统计监控**: 提供详细的收集统计信息和性能监控
- 🛡️ **错误处理**: 完善的异常处理机制，保证程序稳定运行

## 技术特点

### API接口
- **WebSocket URL**: `wss://fstream.binance.com/ws-api/v3`
- **API方法**: `ticker.price`
- **参数**: 空参数（获取所有交易对）
- **重连机制**: 最多10次重连尝试，每次间隔5秒

### 数据库结构

#### price_data 表
```sql
CREATE TABLE price_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,           -- 交易对符号 (如: BTCUSDT)
    price REAL NOT NULL,            -- 价格
    timestamp BIGINT NOT NULL,      -- Binance时间戳 (毫秒)
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP  -- 记录创建时间
);
```

#### collection_stats 表
```sql
CREATE TABLE collection_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    total_messages INTEGER,         -- 总消息数
    successful_inserts INTEGER,     -- 成功插入数
    failed_inserts INTEGER,         -- 失败插入数
    error_count INTEGER,            -- 错误计数
    start_time DATETIME,            -- 开始时间
    last_update DATETIME,           -- 最后更新时间
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

## 快速开始

### 1. 安装依赖

```bash
pip install websockets==12.0
```

或者从项目根目录安装所有依赖：

```bash
pip install -r requirements.txt
```

### 2. 运行收集器

```bash
cd trade
python binance_price_websocket_collector.py
```

### 3. 停止收集器

使用 `Ctrl+C` 优雅停止收集器，程序会自动保存统计信息并关闭数据库连接。

## 高级用法

### 自定义数据库路径

```python
from binance_price_websocket_collector import BinancePriceWebSocketCollector

# 使用自定义数据库路径
collector = BinancePriceWebSocketCollector(db_path="custom_path.db")
collector.start()
```

### 程序化控制

```python
import time
from binance_price_websocket_collector import BinancePriceWebSocketCollector

collector = BinancePriceWebSocketCollector()

# 启动收集器（在后台线程运行）
import threading
def run_collector():
    collector.start()

thread = threading.Thread(target=run_collector)
thread.daemon = True
thread.start()

# 运行一段时间
time.sleep(60)  # 运行1分钟

# 获取统计信息
stats = collector.get_stats()
print(f"收集了 {stats['total_messages']} 条消息")

# 停止收集器
collector.stop()
```

## 测试和验证

### 运行基础测试

```bash
python test_binance_websocket_collector.py
```

### 运行包含网络连接的完整测试

```bash
python test_binance_websocket_collector.py --include-collection
```

### 保留测试数据库

```bash
python test_binance_websocket_collector.py --keep-db
```

## 数据查询示例

### 查看最新价格数据

```sql
SELECT symbol, price, datetime(timestamp/1000, 'unixepoch') as price_time 
FROM price_data 
ORDER BY timestamp DESC 
LIMIT 10;
```

### 查看特定交易对的价格历史

```sql
SELECT price, datetime(timestamp/1000, 'unixepoch') as price_time 
FROM price_data 
WHERE symbol = 'BTCUSDT' 
ORDER BY timestamp DESC 
LIMIT 100;
```

### 统计每小时的数据量

```sql
SELECT 
    datetime(timestamp/1000, 'unixepoch', 'start of hour') as hour,
    COUNT(*) as message_count,
    COUNT(DISTINCT symbol) as unique_symbols
FROM price_data 
GROUP BY hour 
ORDER BY hour DESC;
```

### 查看价格变化最大的交易对

```sql
SELECT 
    symbol,
    MIN(price) as min_price,
    MAX(price) as max_price,
    (MAX(price) - MIN(price)) / MIN(price) * 100 as change_percent
FROM price_data 
WHERE timestamp > (strftime('%s', 'now') - 86400) * 1000  -- 最近24小时
GROUP BY symbol 
HAVING COUNT(*) > 10  -- 至少有10个数据点
ORDER BY change_percent DESC 
LIMIT 20;
```

## 性能参考

### 预期数据量
- **交易对数量**: ~200个USDT永续合约
- **更新频率**: 实时（取决于价格变化）
- **预期TPS**: 10-50 消息/秒
- **日数据量**: 约100万-500万条记录

### 系统要求
- **内存**: 最少128MB
- **磁盘**: 每天约100-500MB（取决于数据量）
- **网络**: 稳定的互联网连接
- **CPU**: 最少1核，推荐2核以上

## 监控和日志

### 日志输出示例

```
[2024-12-30 10:00:00] Binance价格数据WebSocket收集器初始化完成
[2024-12-30 10:00:01] WebSocket连接成功
[2024-12-30 10:00:01] 已发送所有交易对价格ticker请求
[2024-12-30 10:01:00] 统计信息 - 总消息: 1000, 成功插入: 998, 失败插入: 2, 错误数: 0, 平均速率: 16.7 msg/s
```

### 统计信息字段说明

- `total_messages`: 总接收消息数
- `successful_inserts`: 成功插入数据库的记录数
- `failed_inserts`: 插入失败的记录数
- `error_count`: 处理错误的次数
- `runtime_seconds`: 运行时长（秒）
- `average_rate`: 平均消息处理速率（消息/秒）

## 故障排除

### 常见问题

1. **WebSocket连接失败**
   - 检查网络连接
   - 确认Binance API可访问性
   - 检查防火墙设置

2. **数据库写入失败**
   - 检查磁盘空间
   - 确认数据库文件权限
   - 检查SQLite版本兼容性

3. **内存使用过高**
   - 调整批量插入策略
   - 定期清理历史数据
   - 增加系统内存

### 调试模式

```python
import logging
from tools.logger import logger

# 设置调试级别
logger.setLevel(logging.DEBUG)

# 运行收集器
collector = BinancePriceWebSocketCollector()
collector.start()
```

## 安全注意事项

1. **API访问**: 本脚本仅使用公开的市场数据API，无需API密钥
2. **数据隐私**: 收集的数据为公开市场数据，无隐私风险
3. **网络安全**: 建议在安全的网络环境中运行
4. **资源限制**: 注意Binance的API限流规则

## 扩展和定制

### 添加数据过滤

```python
def _process_ticker_data(self, ticker_data: Dict[str, Any]):
    symbol = ticker_data.get('symbol')
    
    # 只收集特定交易对
    if not symbol.endswith('USDT'):
        return
        
    # 只收集价格大于某个阈值的交易对
    price = float(ticker_data.get('price', 0))
    if price < 0.01:
        return
        
    # 调用原始处理逻辑
    super()._process_ticker_data(ticker_data)
```

### 添加数据预处理

```python
def _insert_price_data(self, symbol: str, price: float, timestamp: int):
    # 数据清洗
    if price <= 0 or price > 1000000:
        logger.warning(f"异常价格数据: {symbol} = {price}")
        return
        
    # 数据变换
    price_rounded = round(price, 8)
    
    # 调用原始插入逻辑
    super()._insert_price_data(symbol, price_rounded, timestamp)
```

## 许可证

本项目遵循项目整体许可证协议。

---

如有问题或建议，请联系开发团队。 