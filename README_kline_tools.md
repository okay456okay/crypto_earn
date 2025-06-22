# K线数据完整性检查和修复工具

本工具包包含两个脚本，用于检查和修复数据库中的K线数据完整性。

## 工具概述

### 1. check_kline_data_integrity.py - 数据完整性检查器
- **功能**: 检查数据库中K线数据的完整性和时间范围
- **检查内容**: 
  - 1分钟K线数据（当天数据）
  - 30分钟K线数据（当天往前29天）
- **输出**: 详细的完整性报告和统计信息

### 2. repair_missing_kline_data.py - 数据修复工具
- **功能**: 自动检测并修复缺失的K线数据
- **修复方式**: 从Binance API获取缺失数据并保存到数据库
- **支持模式**: 检查模式和修复模式

## 数据存储策略

系统采用混合K线数据存储策略：
- **历史数据**（昨天及之前）：存储为30分钟K线，节省空间
- **当天数据**（今天00:00至今）：存储为1分钟K线，提供精确监控

## 使用方法

### 数据完整性检查器

```bash
# 检查所有交易对的数据完整性（基础模式）
python check_kline_data_integrity.py

# 显示详细的缺失时间信息
python check_kline_data_integrity.py --detailed

# 检查特定交易对
python check_kline_data_integrity.py --symbol BTCUSDT

# 检查特定交易对并显示详细信息
python check_kline_data_integrity.py --symbol BTCUSDT --detailed
```

#### 输出内容
1. **时间范围统计**: 每个交易对的1分钟和30分钟K线时间范围
2. **完整性检查报告**: 数据完整率、缺失数量、问题交易对列表
3. **汇总统计**: 总体完整性状况和最严重的问题

### 数据修复工具

```bash
# 仅检查缺失数据，不进行修复
python repair_missing_kline_data.py --check-only

# 修复特定交易对的缺失数据
python repair_missing_kline_data.py --symbol BTCUSDT

# 修复所有交易对的缺失数据（谨慎使用）
python repair_missing_kline_data.py --repair-all
```

#### 修复过程
1. **检测缺失**: 自动识别缺失的时间段
2. **API获取**: 从Binance API获取缺失的K线数据
3. **数据保存**: 将获取的数据保存到相应的数据库表
4. **进度报告**: 显示修复进度和结果统计

## 检查逻辑

### 1分钟K线检查（当天数据）
- **时间范围**: 今天00:00:00 到当前时间
- **预期频率**: 每分钟一条记录
- **缺失判断**: 检查每分钟是否都有对应的K线记录

### 30分钟K线检查（历史数据）
- **时间范围**: 30天前 到 今天00:00:00
- **预期频率**: 每30分钟一条记录（每小时的00分和30分）
- **缺失判断**: 检查每个30分钟时间点是否都有对应的K线记录

## 输出示例

### 完整性检查报告示例
```
====================================================================================================
📊 交易对时间范围统计
====================================================================================================

🔸 BTCUSDT:
  📈 1分钟K线: 1,234 条
     时间范围: 2024-12-30 00:00:00 至 2024-12-30 20:34:00
     时间跨度: 20.6 小时
  📊 30分钟K线: 1,440 条
     时间范围: 2024-12-01 00:00:00 至 2024-12-29 23:30:00
     时间跨度: 29.0 天

====================================================================================================
🔍 K线数据完整性检查报告
====================================================================================================

[1/150] 检查 BTCUSDT...
  📈 1分钟K线: 1,230/1,234 (99.7%)
     ⚠️  缺失 4 条数据
     🕐 部分缺失时间: 10:15, 10:16, 15:30, 15:31
  📊 30分钟K线: 1,440/1,440 (100.0%)

📋 完整性检查汇总
====================================================================================================
总检查交易对数: 150
📈 1分钟K线:
  ✅ 完整率≥95%: 145 个 (96.7%)
  ⚠️  有缺失数据: 5 个 (3.3%)
📊 30分钟K线:
  ✅ 完整率≥95%: 150 个 (100.0%)
  ⚠️  有缺失数据: 0 个 (0.0%)
```

### 修复工具输出示例
```
🚀 开始修复K线数据...
🔍 修复所有 150 个交易对

[1/150] 处理 BTCUSDT...
发现BTCUSDT的2个1分钟K线缺失时间段:
  • 2024-12-30 10:15 到 2024-12-30 10:16 (共2分钟)
  • 2024-12-30 15:30 到 2024-12-30 15:31 (共2分钟)
获取BTCUSDT的1分钟K线数据: 2024-12-30 10:15 到 2024-12-30 10:16
✅ 成功保存BTCUSDT的2条1分钟K线数据
获取BTCUSDT的1分钟K线数据: 2024-12-30 15:30 到 2024-12-30 15:31
✅ 成功保存BTCUSDT的2条1分钟K线数据

====================================================================================================
📋 修复完成汇总
====================================================================================================
总处理交易对数: 150
处理成功: 150 个
处理失败: 0 个

发现缺失:
  📈 1分钟K线: 12 个时间段
  📊 30分钟K线: 3 个时间段

修复完成:
  📈 1分钟K线: 12 个时间段
  📊 30分钟K线: 3 个时间段
✅ K线数据修复完成!
```

## 注意事项

### 使用建议
1. **首次使用**: 建议先运行检查模式了解数据状况
2. **批量修复**: 使用 `--repair-all` 时请确保网络稳定，过程可能较长
3. **API限制**: 修复过程中会自动添加延迟以避免API限制
4. **数据库备份**: 修复前建议备份数据库

### 性能优化
- 工具会自动检测连续的缺失时间段，减少API调用次数
- 使用 `INSERT IGNORE` 避免重复数据插入
- 批量处理时自动添加适当的延迟

### 错误处理
- 自动跳过无法修复的时间段
- 详细的错误日志记录
- 修复失败不会影响其他交易对的处理

## 数据库结构

### 1分钟K线表 (kline_data_1min)
```sql
CREATE TABLE kline_data_1min (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    symbol VARCHAR(50) NOT NULL,
    open_time BIGINT NOT NULL,
    close_time BIGINT NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    volume DECIMAL(20,8) NOT NULL,
    quote_volume DECIMAL(20,8) NOT NULL,
    trades_count INT NOT NULL,
    taker_buy_base_volume DECIMAL(20,8) NOT NULL,
    taker_buy_quote_volume DECIMAL(20,8) NOT NULL,
    UNIQUE KEY unique_kline_1min (symbol, open_time)
);
```

### 30分钟K线表 (kline_data_30min)
```sql
CREATE TABLE kline_data_30min (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    symbol VARCHAR(50) NOT NULL,
    open_time BIGINT NOT NULL,
    close_time BIGINT NOT NULL,
    open_price DECIMAL(20,8) NOT NULL,
    high_price DECIMAL(20,8) NOT NULL,
    low_price DECIMAL(20,8) NOT NULL,
    close_price DECIMAL(20,8) NOT NULL,
    volume DECIMAL(20,8) NOT NULL,
    quote_volume DECIMAL(20,8) NOT NULL,
    trades_count INT NOT NULL,
    taker_buy_base_volume DECIMAL(20,8) NOT NULL,
    taker_buy_quote_volume DECIMAL(20,8) NOT NULL,
    UNIQUE KEY unique_kline_30min (symbol, open_time)
);
```

## 相关配置

确保 `config.py` 文件中包含以下配置：
- `mysql_config`: MySQL数据库连接配置
- `binance_api_key`: Binance API密钥
- `binance_api_secret`: Binance API密钥
- `proxies`: 代理配置（如需要）

## 故障排除

### 常见问题
1. **数据库连接失败**: 检查MySQL配置和网络连接
2. **API调用失败**: 检查Binance API密钥和网络连接
3. **权限不足**: 确保API密钥有足够的权限访问K线数据
4. **内存不足**: 大量数据修复时可能需要增加系统内存

### 日志级别
- 使用 `logging.INFO` 级别查看基本进度信息
- 使用 `logging.DEBUG` 级别查看详细的调试信息 