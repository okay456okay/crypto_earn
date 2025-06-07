# 多交易所资金费率套利交易脚本

## 功能特性

🚀 **多交易所支持**: 支持 Binance、Gate.io、Bybit、Bitget 四大主流交易所  
📊 **自动化套利**: 自动检测负资金费率机会并执行套利交易  
⚡ **实时监控**: 每0.2秒检查订单状态，精确统计执行时间  
🛡️ **风险控制**: 内置止损机制，保护资金安全  
📈 **智能定价**: 基于资金费率计算最优平仓价格  

## 支持的交易所

| 交易所 | 代码名称 | 合约类型 | 状态 |
|--------|----------|----------|------|
| Binance | `binance` | 期货 (future) | ✅ 已支持 |
| Gate.io | `gateio` | 永续合约 (swap) | ✅ 已支持 |
| Bybit | `bybit` | 线性合约 (linear) | ✅ 已支持 |
| Bitget | `bitget` | 永续合约 (swap) | ✅ 已支持 |

## 安装依赖

```bash
pip install ccxt asyncio
```

## 配置API密钥

1. 复制配置示例文件：
```bash
cp config_example.py config.py
```

2. 编辑 `config.py` 文件，填入各交易所的API密钥：
```python
# Binance API配置
binance_api_key = "your_binance_api_key_here"
binance_api_secret = "your_binance_api_secret_here"

# Gate.io API配置  
gateio_api_key = "your_gateio_api_key_here"
gateio_api_secret = "your_gateio_api_secret_here"

# ... 其他交易所配置
```

## 使用方法

### 基本用法

```bash
# 在Binance上交易BTC/USDT
python funding_rate_trader.py BTC/USDT --exchange binance

# 在Bybit上交易ETH/USDT
python funding_rate_trader.py ETH/USDT --exchange bybit

# 在Gate.io上交易BTC/USDT
python funding_rate_trader.py BTC/USDT --exchange gateio

# 在Bitget上交易BTC/USDT
python funding_rate_trader.py BTC/USDT --exchange bitget

# 自定义资金费率阈值为-0.3%
python funding_rate_trader.py BTC/USDT --exchange binance --min-funding-rate -0.003

# 更激进的策略，资金费率为-0.1%时就触发
python funding_rate_trader.py ETH/USDT --exchange bybit --min-funding-rate -0.001
```

### 命令行参数

| 参数 | 类型 | 必需 | 默认值 | 说明 |
|------|------|------|--------|------|
| `symbol` | 字符串 | ✅ | - | 交易对符号，如 BTC/USDT |
| `--exchange` | 选择 | ❌ | binance | 交易所选择 (binance/gateio/bybit/bitget) |
| `--min-funding-rate` | 浮点数 | ❌ | -0.005 | 触发套利的最小资金费率阈值 (-0.5%) |
| `--manual-time` | 字符串 | ❌ | None | 手动指定检查时间 (用于测试) |
| `--log-level` | 选择 | ❌ | INFO | 日志级别 (DEBUG/INFO/WARNING/ERROR) |

### 测试模式

```bash
# 使用手动时间进行测试
python funding_rate_trader.py BTC/USDT --exchange binance --manual-time "2024-01-01T08:00:00+00:00"
```

## 策略逻辑

### 1. 资金费率检查
- 监控目标交易对的资金费率
- 当资金费率低于设定阈值时触发套利机会（默认: < -0.5%）
- 可通过 `--min-funding-rate` 参数自定义阈值

### 2. 自动下单
- 在资金结算前15秒检查条件
- 在资金结算前5秒执行开仓
- 实时监控订单状态直到成交

### 3. 智能平仓
- 计算最优平仓价格：`开仓价格 × (1 + 资金费率 - 0.5%)`
- 下限价平仓单等待成交
- 同时启动止损监控

### 4. 风险控制
- 价格上涨超过0.1%时自动止损
- 最大监控时间10分钟
- 详细的执行日志记录

## 交易所特殊说明

### Binance
- 使用期货合约 (defaultType: 'future')
- 支持双向持仓模式
- 需要设置positionSide参数
- 交易对格式: 标准格式 (BTC/USDT)

### Bybit  
- 使用线性合约 (defaultType: 'linear')
- 双向持仓模式 (positionIdx: 2)
- 支持全仓和逐仓模式
- 交易对格式: 需要添加:USDT后缀 (BTC/USDT:USDT)

### Gate.io
- 使用永续合约 (defaultType: 'swap')
- 通过reduceOnly参数控制平仓
- 支持杠杆动态调整
- 交易对格式: 需要添加:USDT后缀 (BTC/USDT:USDT)

### Bitget
- 使用永续合约 (defaultType: 'swap')  
- 需要指定holdSide参数
- 支持全仓交易模式
- 交易对格式: 需要添加:USDT后缀 (BTC/USDT:USDT)
- 时间戳处理: 使用特殊的nextUpdate毫秒时间戳

## 日志文件

日志文件保存在 `trade/logs/` 目录下：
- 文件名格式: `funding_rate_trader_YYYYMMDD.log`
- 包含详细的交易执行记录和性能统计

## 安全提醒

⚠️ **风险警告**: 套利交易存在风险，请确保：

1. **API权限**: 只授予必要的交易权限，避免提币权限
2. **IP白名单**: 启用API的IP白名单限制
3. **资金管理**: 合理设置交易金额，控制单次风险敞口
4. **实时监控**: 密切关注交易执行情况和市场变化
5. **网络稳定**: 确保网络连接稳定，避免执行中断

## 故障排除

### 常见问题

1. **API连接失败**
   - 检查API密钥是否正确
   - 确认API权限包含期货/合约交易
   - 验证IP白名单设置

2. **杠杆设置失败**
   - 某些交易所需要手动在网页端预设杠杆
   - 检查账户类型是否支持杠杆交易

3. **下单失败**  
   - 确认账户余额充足
   - 检查交易对是否正确
   - 验证最小订单金额要求

4. **网络超时**
   - 检查网络连接
   - 考虑使用代理服务器
   - 调整请求超时参数

5. **交易对格式错误**
   - Binance: 使用标准格式 (BTC/USDT)
   - Bybit/Gate.io/Bitget: 使用扩展格式 (BTC/USDT:USDT)
   - 系统会自动转换，无需手动修改

### 获取帮助

如遇到问题，请检查：
1. 日志文件中的详细错误信息
2. API文档中的参数要求
3. 交易所的系统状态页面

---

**开发版本**: v2.0.0  
**更新时间**: 2024年  
**作者**: 加密货币套利专家