# Binance价格高点扫描器 (1分钟K线版本)

## 概述

这是一个优化版本的Binance价格突破扫描器，采用1分钟K线数据提供更精确的价格监控。

## 主要改进

### 1. 数据架构优化
- **K线级别**: 从30分钟K线升级到1分钟K线
- **数据存储**: 使用MySQL数据库存储历史K线数据，支持高并发访问
- **增量更新**: 每次扫描只获取最近10分钟的新数据
- **去重机制**: 自动处理重复数据，确保数据完整性

### 2. 性能提升
- **扫描间隔**: 建议从30分钟间隔改为5分钟间隔
- **API效率**: 减少API调用次数，避免频繁请求历史数据
- **延迟优化**: 缩短处理延迟，提高响应速度

### 3. 数据精度
- **时间精度**: 1分钟级别的价格更新
- **突破检测**: 更精确的价格突破判断
- **实时性**: 更快发现价格异动

## 使用方法

### 前置条件 - MySQL数据库设置

**确保MySQL数据库已正确配置：**

```bash
# 1. 连接到MySQL数据库
mysql -u root -p

# 2. 运行数据库初始化脚本
source scripts/create_mysql_database.sql

# 3. 验证数据库和表是否创建成功
USE crypto_earn;
SHOW TABLES;
```

### 首次使用 - 数据初始化

**必须先进行数据初始化，获取30天的历史K线数据：**

```bash
# 初始化所有交易对的K线数据（约需要30-60分钟）
python binance_price_high_scanner.py --init

# 指定分析天数进行初始化
python binance_price_high_scanner.py --init --days 30
```

### 日常扫描

**初始化完成后，可以进行价格突破扫描：**

```bash
# 普通扫描模式（仅通知）
python binance_price_high_scanner.py

# 启用自动交易模式
python binance_price_high_scanner.py --trade

# 指定分析天数
python binance_price_high_scanner.py --days 15
```

### 盈亏查看

```bash
# 仅更新和查看交易盈亏
python binance_price_high_scanner.py --pnl-only
```

## 命令行参数

| 参数 | 说明 | 默认值 |
|-----|------|--------|
| `--init` | 初始化K线数据（首次运行必须） | - |
| `--days` | 历史数据分析天数 | 30 |
| `--trade` | 启用自动交易功能 | 禁用 |
| `--pnl-only` | 仅更新盈亏信息 | - |

## 数据库结构

### MySQL数据库配置
- **主机**: localhost
- **端口**: 3306  
- **用户**: crypt_earn
- **数据库**: crypto_earn

### kline_data 表
存储1分钟K线数据：
- `symbol`: 交易对符号 (VARCHAR(50))
- `open_time`: 开盘时间 (BIGINT)
- `close_time`: 收盘时间 (BIGINT)
- `open_price`: 开盘价 (DECIMAL(20,8))
- `high_price`: 最高价 (DECIMAL(20,8))
- `low_price`: 最低价 (DECIMAL(20,8))
- `close_price`: 收盘价 (DECIMAL(20,8))
- `volume`: 成交量 (DECIMAL(20,8))
- 等其他字段...

### trading_records 表
存储交易记录：
- `exchange`: 交易所 (VARCHAR(50))
- `symbol`: 交易对符号 (VARCHAR(50))
- `order_time`: 下单时间 (TIMESTAMP)
- `open_price`: 开仓价格 (DECIMAL(20,8))
- `quantity`: 交易数量 (DECIMAL(20,8))
- `leverage`: 杠杆倍数 (INT)
- `direction`: 交易方向 (VARCHAR(10))
- 等其他字段...

## 运行建议

### 1. 定时任务设置

```bash
# 每5分钟运行一次扫描
*/5 * * * * cd /path/to/crypto_earn && python trade/binance_price_high_scanner.py

# 每日凌晨2点更新盈亏信息
0 2 * * * cd /path/to/crypto_earn && python trade/binance_price_high_scanner.py --pnl-only
```

### 2. 系统资源要求

- **数据库**: MySQL 5.7+，约500MB-1GB存储空间（存储所有交易对的30天1分钟K线数据）
- **内存**: 建议2GB以上
- **网络**: 稳定的网络连接，支持访问Binance API和MySQL数据库

### 3. 监控建议

- 监控数据库文件大小，定期清理过期数据
- 监控API调用频率，避免超出限制
- 定期检查数据完整性

## 故障排除

### 1. 初始化失败
```bash
# 检查网络连接和API配置
# 如果部分交易对初始化失败，可以重新运行初始化命令
python binance_price_high_scanner.py --init
```

### 2. 缺少K线数据
```bash
# 如果扫描时提示缺少K线数据，重新初始化该交易对
python binance_price_high_scanner.py --init
```

### 3. 数据库错误
```bash
# 如果数据库出现问题，可以删除相关表重新初始化
# 连接MySQL数据库执行以下SQL：
# DROP TABLE IF EXISTS kline_data;
# DROP TABLE IF EXISTS trading_records;
# 然后重新运行初始化命令
python binance_price_high_scanner.py --init
```

## 注意事项

1. **MySQL数据库**: 确保MySQL数据库服务运行正常，数据库用户权限配置正确
2. **首次运行**: 必须使用 `--init` 参数进行数据初始化
3. **网络要求**: 需要稳定的网络连接访问Binance API和MySQL数据库
4. **API限制**: 注意Binance API的调用频率限制
5. **数据维护**: 建议定期清理过期的K线数据
6. **交易风险**: 使用 `--trade` 参数时请充分了解交易风险

## 技术细节

### API调用优化
- 初始化时分批获取数据（每批最多1500条）
- 扫描时只获取最近10分钟的新数据
- 使用数据库去重，避免重复数据

### 突破判断逻辑
- 7天突破：价格超过最近7天的最高点
- 15天突破：价格超过最近15天的最高点  
- 30天突破：价格超过最近30天的最高点

### 自动交易逻辑
- 检查代币上市时间、市值排名、资金费率等过滤条件
- 执行卖空交易，同时设置止盈限价单
- 支持追加开仓（价格上涨10%以上时） 