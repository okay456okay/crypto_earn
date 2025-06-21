# Binance WebSocket数据库存储系统

## 概述

本系统通过Binance WebSocket API实时接收所有合约的标记价格数据（Mark Price），并将数据存储到SQLite数据库中，为套利策略提供可靠的数据基础。

## 主要功能

### 1. 数据收集 (`binance_websocket.py`)
- 实时接收Binance所有合约的标记价格数据
- 自动创建SQLite数据库和表结构
- 支持数据去重和错误处理
- 提供完整的数据字段存储

### 2. 数据查询 (`db_query_tool.py`)
- 交互式数据查询界面
- 支持按交易对、时间范围查询
- 数据统计和分析功能
- CSV导出功能
- 数据库维护工具

### 3. 系统测试 (`test_db_setup.py`)
- 完整的功能测试套件
- 数据库初始化验证
- 数据插入和查询测试
- 系统完整性检查

## 数据结构

### 数据库表: `binance_mark_prices`

| 字段名 | 类型 | 说明 |
|--------|------|------|
| id | INTEGER | 主键，自增 |
| event_type | TEXT | 事件类型 (markPriceUpdate) |
| event_time | INTEGER | 事件时间戳 (毫秒) |
| symbol | TEXT | 交易对符号 (如: BTCUSDT) |
| mark_price | REAL | 标记价格 |
| index_price | REAL | 指数价格 |
| estimated_settle_price | REAL | 预估结算价格 |
| funding_rate | REAL | 资金费率 |
| next_funding_time | INTEGER | 下次资金费率结算时间 |
| raw_data | TEXT | 原始JSON数据 |
| created_at | TIMESTAMP | 记录创建时间 |

### 索引优化
- `idx_symbol_time`: 按交易对和事件时间索引
- `idx_created_at`: 按创建时间索引
- `UNIQUE(symbol, event_time)`: 防止重复数据

## 快速开始

### 1. 环境准备

确保已安装必要的依赖：
```bash
pip install -r requirements.txt
```

### 2. 配置设置

复制配置文件并填入API密钥：
```bash
cp config_example.py config.py
# 编辑config.py，填入您的Binance API密钥
```

### 3. 系统测试

运行测试脚本验证系统设置：
```bash
cd trade
python test_db_setup.py
```

### 4. 启动数据收集

运行WebSocket客户端开始收集数据：
```bash
python binance_websocket.py
```

### 5. 数据查询

使用查询工具查看收集的数据：
```bash
python db_query_tool.py
```

## 使用说明

### 数据收集服务

`binance_websocket.py` 提供以下功能：

1. **自动初始化**: 首次运行时自动创建数据库和表
2. **实时数据流**: 持续接收Binance标记价格数据
3. **数据验证**: 检查数据格式和完整性
4. **错误处理**: 网络中断和数据异常自动恢复
5. **日志记录**: 详细的运行状态和错误信息

运行示例：
```bash
python binance_websocket.py
```

输出示例：
```
数据库初始化完成
开始接收Binance标记价格数据...
成功保存 342 条标记价格数据到数据库
成功保存 341 条标记价格数据到数据库
...
```

### 数据查询工具

`db_query_tool.py` 提供交互式查询界面：

#### 主要功能：
1. **表结构查看**: 查看数据库统计信息
2. **最近数据**: 查看最新收集的数据
3. **交易对查询**: 查询特定交易对的历史数据
4. **热门交易对**: 查看数据量最大的交易对
5. **数据清理**: 删除过期数据
6. **CSV导出**: 导出数据用于分析

#### 使用示例：

查看BTCUSDT最近24小时数据：
1. 运行 `python db_query_tool.py`
2. 选择选项 `3`
3. 输入交易对: `BTCUSDT`
4. 输入时间范围: `24`

导出数据：
1. 选择选项 `6`
2. 输入交易对（或留空导出所有）
3. 指定时间范围

### 系统测试

运行完整的测试套件：
```bash
python test_db_setup.py
```

测试内容包括：
- 数据库创建和初始化
- 表结构验证
- 数据插入和查询
- 查询工具功能
- 测试数据清理

## 数据用途

收集的标记价格数据可用于：

1. **套利策略开发**
   - 跨交易所价差分析
   - 现货-合约套利机会识别
   - 资金费率套利策略

2. **风险管理**
   - 实时价格监控
   - 异常价格波动检测
   - 流动性评估

3. **量化分析**
   - 价格趋势分析
   - 波动率计算
   - 相关性分析

4. **回测验证**
   - 历史数据回测
   - 策略性能评估
   - 参数优化

## 维护和优化

### 数据库维护

1. **定期清理**: 删除过期数据以控制数据库大小
```bash
python db_query_tool.py  # 选择选项5
```

2. **数据备份**: 定期备份数据库文件
```bash
cp trading_records.db trading_records_backup_$(date +%Y%m%d).db
```

3. **性能监控**: 监控数据库大小和查询性能
```bash
ls -lh trading_records.db
```

### 系统优化

1. **网络优化**: 配置合适的代理设置
2. **存储优化**: 定期清理旧数据
3. **监控优化**: 添加系统监控和告警

## 故障排除

### 常见问题

1. **API连接失败**
   - 检查API密钥配置
   - 验证网络连接
   - 确认代理设置

2. **数据库错误**
   - 检查磁盘空间
   - 验证文件权限
   - 重新初始化数据库

3. **数据丢失**
   - 检查WebSocket连接状态
   - 验证数据插入日志
   - 重启数据收集服务

### 日志分析

查看系统运行日志：
```bash
tail -f /path/to/logfile  # 如果有日志文件
```

监控数据库增长：
```bash
python db_query_tool.py  # 选择选项1查看统计
```

## 扩展开发

### 添加新数据源

可以参考`binance_websocket.py`的模式，添加其他交易所的数据收集：

1. 创建新的WebSocket客户端
2. 实现数据解析和标准化
3. 使用相同的数据库结构
4. 添加数据源标识字段

### 自定义查询

可以扩展`db_query_tool.py`添加自定义查询功能：

1. 添加新的查询函数
2. 实现特定的分析算法
3. 集成到交互式菜单

### API接口

可以基于数据库开发REST API：

1. 使用Flask或FastAPI框架
2. 提供数据查询接口
3. 支持实时数据推送

## 注意事项

1. **API限制**: 注意Binance API的调用限制
2. **数据存储**: 大量数据需要定期清理
3. **网络稳定**: 确保网络连接的稳定性
4. **权限管理**: 保护API密钥的安全
5. **系统监控**: 建议添加监控和告警机制

## 版本更新

- v1.0: 基础数据收集和查询功能
- 后续版本将添加更多分析工具和优化功能

## 技术支持

如有问题或建议，请：
1. 查看日志文件
2. 运行测试脚本
3. 检查配置文件
4. 联系技术支持团队 