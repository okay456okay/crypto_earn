# 交易记录表升级说明

## 概述

本次升级为`trading_records`表添加了平仓相关字段，实现了完整的交易生命周期管理。升级后系统将能够：

- ✅ 记录平仓价格、订单ID和状态
- ✅ 自动检查和更新平仓订单状态
- ✅ 统计时排除已平仓的交易记录
- ✅ 提供完整的交易历史跟踪

## 新增字段

| 字段名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `close_price` | DECIMAL(20,8) | NULL | 平仓价格 |
| `close_order_id` | VARCHAR(100) | NULL | 平仓订单ID |
| `close_order_status` | VARCHAR(20) | 'OPEN' | 平仓订单状态 |
| `close_time` | TIMESTAMP | NULL | 平仓时间 |

### 平仓订单状态说明

- **OPEN**: 未平仓（默认状态）
- **FILLED**: 平仓订单已成交
- **CANCELLED**: 平仓订单已取消
- **CLOSED**: 已平仓（系统检测到无持仓时设置）

## 升级步骤

### 1. 运行升级脚本

```bash
# 方法一：直接运行Python脚本
python upgrade_trading_records_table.py

# 方法二：运行可执行脚本
./upgrade_trading_records_table.py
```

### 2. 升级验证

升级脚本会自动：
- 检查表是否存在
- 添加缺失的字段
- 创建必要的索引
- 显示升级后的表结构
- 统计现有数据

### 3. 功能验证

升级完成后，你可以通过以下方式验证功能：

```bash
# 查看表结构
mysql -u crypt_earn -p crypt_earn -e "DESCRIBE trading_records;"

# 查看数据统计
mysql -u crypt_earn -p crypt_earn -e "
SELECT 
    close_order_status,
    COUNT(*) as count
FROM trading_records 
GROUP BY close_order_status;"
```

## 功能变更

### 1. 交易记录管理

#### 新增方法
- `update_close_order_info()`: 更新平仓订单信息
- `check_and_update_close_orders()`: 检查和更新平仓订单状态
- `get_open_traded_symbols()`: 获取未平仓的交易对
- `update_symbol_to_closed_status()`: 更新交易对为已平仓状态

#### 修改的方法
- `execute_short_order()`: 提交平仓订单后自动更新记录
- `clean_trade_records()`: 从删除改为更新状态
- `get_latest_trade_record()`: 只返回未平仓的最新记录

### 2. 盈亏统计优化

所有盈亏统计方法现在只处理未平仓的交易记录：
- `update_trade_pnl()`: 只更新未平仓记录的盈亏
- `get_all_trade_pnl_summary()`: 只统计未平仓记录
- `get_symbol_aggregated_pnl_summary()`: 只汇总未平仓记录

### 3. 自动状态管理

系统现在会自动：
1. 在每次扫描时检查平仓订单状态
2. 检测无持仓的交易对并更新状态
3. 在统计时排除已平仓的记录

## 使用示例

### 查看未平仓持仓

```python
# 获取所有未平仓的交易对
open_symbols = scanner.get_open_traded_symbols()
print(f"未平仓交易对: {open_symbols}")

# 获取未平仓盈亏汇总
pnl_summary = scanner.get_all_trade_pnl_summary()
print(f"总盈亏: ${pnl_summary['total_pnl']:.2f}")
```

### 手动更新平仓信息

```python
# 当平仓订单成交后，更新记录
success = scanner.update_close_order_info(
    symbol="BTCUSDT",
    open_order_id="123456789",
    close_price=45000.0,
    close_order_id="987654321",
    close_order_status="FILLED"
)
```

### 检查平仓订单状态

```python
# 系统会自动检查，也可手动调用
await scanner.check_and_update_close_orders()
```

## 数据迁移

### 现有数据处理

- 所有现有的交易记录`close_order_status`默认设置为`'OPEN'`
- 现有记录的平仓相关字段默认为`NULL`
- 升级不会影响现有数据的完整性

### 兼容性

- 升级后系统完全向后兼容
- 旧的交易记录会正常工作
- 新功能逐步生效

## 监控和维护

### 日志监控

升级后，系统会输出以下新的日志信息：
- `🔍 检查平仓订单状态...`
- `✅ {symbol} 平仓订单已成交`
- `❌ {symbol} 平仓订单已取消`
- `已更新{symbol}的平仓信息`

### 定期维护

建议定期检查：
1. 平仓订单状态的准确性
2. 数据库中的异常记录
3. 统计结果的合理性

## 故障排除

### 常见问题

1. **升级脚本失败**
   ```bash
   # 检查数据库连接
   mysql -u crypt_earn -p crypt_earn -e "SELECT 1;"
   
   # 检查权限
   mysql -u crypt_earn -p crypt_earn -e "SHOW GRANTS;"
   ```

2. **字段添加失败**
   ```sql
   -- 手动添加字段
   ALTER TABLE trading_records ADD COLUMN close_price DECIMAL(20,8) DEFAULT NULL;
   ALTER TABLE trading_records ADD COLUMN close_order_id VARCHAR(100) DEFAULT NULL;
   ALTER TABLE trading_records ADD COLUMN close_order_status VARCHAR(20) DEFAULT 'OPEN';
   ALTER TABLE trading_records ADD COLUMN close_time TIMESTAMP NULL;
   ```

3. **统计结果异常**
   ```sql
   -- 检查数据一致性
   SELECT close_order_status, COUNT(*) FROM trading_records GROUP BY close_order_status;
   ```

### 回滚方案

如果需要回滚升级：

```sql
-- 删除新添加的字段（谨慎操作！）
ALTER TABLE trading_records DROP COLUMN close_price;
ALTER TABLE trading_records DROP COLUMN close_order_id;
ALTER TABLE trading_records DROP COLUMN close_order_status;
ALTER TABLE trading_records DROP COLUMN close_time;
ALTER TABLE trading_records DROP INDEX idx_close_status;
```

## 版本信息

- **升级版本**: v2.0
- **升级日期**: 2024-12-19
- **兼容性**: 向后兼容
- **数据库版本要求**: MySQL 5.7+

## 技术支持

如果在升级过程中遇到问题，请：

1. 查看升级脚本的输出日志
2. 检查数据库连接和权限
3. 验证表结构是否正确创建
4. 测试新功能是否正常工作

升级完成后，系统将具备完整的交易生命周期管理能力，提供更准确的持仓统计和风险控制。 