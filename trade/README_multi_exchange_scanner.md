# 多交易所合约扫描器使用说明

## 概述

多交易所合约扫描器是一个专业的加密货币套利工具，用于扫描多个主流交易所的合约交易对，找到符合特定条件的套利机会。

## 支持的交易所

- **Binance** - 全球最大的加密货币交易所
- **Bitget** - 专业的合约交易平台
- **Bybit** - 知名的衍生品交易所
- **GateIO** - 老牌加密货币交易所
- **OKX** - 综合性数字资产交易平台

## 筛选条件

扫描器会找到同时满足以下三个条件的交易对：

1. **价格稳定性**: 最近30天内价格波动小于20%
2. **资金费率方向性**: 资金费率一直为正或一直为负（80%以上的数据点保持同一方向）
3. **高杠杆支持**: 最大杠杆大于等于20倍

## 文件结构

```
trade/
├── binance_contract_scanner.py      # Binance扫描器
├── bitget_contract_scanner.py       # Bitget扫描器
├── bybit_contract_scanner.py        # Bybit扫描器
├── gateio_contract_scanner.py       # GateIO扫描器
├── okx_contract_scanner.py          # OKX扫描器
├── multi_exchange_contract_scanner.py  # 多交易所统一扫描器
└── reports/                         # 扫描报告目录
```

## 使用方法

### 1. 单个交易所扫描

扫描单个交易所的所有合约：

```bash
# 扫描Binance
python binance_contract_scanner.py

# 扫描Bitget
python bitget_contract_scanner.py

# 扫描Bybit
python bybit_contract_scanner.py

# 扫描GateIO
python gateio_contract_scanner.py

# 扫描OKX
python okx_contract_scanner.py
```

### 2. 多交易所并行扫描

使用统一的多交易所扫描器：

```bash
# 扫描所有交易所
python multi_exchange_contract_scanner.py

# 扫描指定的交易所
python multi_exchange_contract_scanner.py --exchanges binance bitget

# 扫描单个交易所（通过多交易所扫描器）
python multi_exchange_contract_scanner.py --exchanges okx
```

### 3. 命令行参数

多交易所扫描器支持以下参数：

- `--exchanges`: 指定要扫描的交易所列表
  - 可选值: `binance`, `bitget`, `bybit`, `gateio`, `okx`
  - 默认: 扫描所有交易所

## 输出报告

### 报告文件

扫描完成后会生成两种格式的报告：

1. **JSON详细报告**: `*_contract_scan_YYYYMMDD_HHMMSS.json`
   - 包含完整的扫描数据和分析结果
   - 适合程序化处理和进一步分析

2. **文本摘要报告**: `*_contract_summary_YYYYMMDD_HHMMSS.txt`
   - 人类可读的摘要格式
   - 包含关键统计信息和符合条件的交易对列表

### 报告内容

每个符合条件的交易对包含以下信息：

- **基本信息**: 交易对符号、基础资产、交易所
- **杠杆信息**: 最大支持杠杆倍数
- **价格分析**: 当前价格、价格区间、30天波动率
- **资金费率分析**:
  - 方向性（正向/负向）
  - 一致性比例
  - 平均资金费率
  - 年化收益率预估

### 年化收益率计算

年化收益率使用以下公式计算：

```
年化收益率 = 平均资金费率 × (24/资金费结算周期) × 365 × 合约杠杆率 × 100
```

其中：
- 资金费结算周期通常为8小时
- 合约杠杆率为该交易对的最大杠杆

## 配置要求

### API密钥配置

在 `config.py` 文件中配置各交易所的API密钥：

```python
# Binance API
binance_api_key = "your_binance_api_key"
binance_api_secret = "your_binance_api_secret"

# Bitget API
bitget_api_key = "your_bitget_api_key"
bitget_api_secret = "your_bitget_api_secret"
bitget_api_passphrase = "your_bitget_passphrase"

# Bybit API
bybit_api_key = "your_bybit_api_key"
bybit_api_secret = "your_bybit_api_secret"

# GateIO API
gateio_api_key = "your_gateio_api_key"
gateio_api_secret = "your_gateio_api_secret"

# OKX API
okx_api_key = "your_okx_api_key"
okx_api_secret = "your_okx_api_secret"
okx_api_passphrase = "your_okx_passphrase"
```

### 依赖库

确保安装以下Python库：

```bash
pip install ccxt numpy pandas
```

## 性能特性

### 并行处理

- 多交易所扫描器使用线程池并行处理
- 显著减少总扫描时间
- 线程安全的日志输出

### API限制保护

- 内置延迟机制防止API限制
- 自动重试机制处理临时错误
- 优雅的错误处理和恢复

### 内存优化

- 流式处理大量数据
- 及时释放不需要的数据
- 适合长时间运行

## 注意事项

### 风险提示

1. **市场风险**: 加密货币市场波动剧烈，过往表现不代表未来收益
2. **技术风险**: API可能出现故障或限制，影响数据获取
3. **监管风险**: 不同地区对加密货币交易有不同的法律法规

### 使用建议

1. **定期更新**: 建议每日运行扫描以获取最新机会
2. **交叉验证**: 对比多个交易所的数据以确保准确性
3. **风险控制**: 设置合理的仓位大小和止损策略
4. **监控执行**: 实时监控套利策略的执行情况

### 技术限制

1. **数据延迟**: 受网络和API响应时间影响
2. **历史数据**: 基于历史数据分析，不保证未来表现
3. **API配额**: 受各交易所API调用频率限制

## 故障排除

### 常见问题

1. **API密钥错误**
   - 检查config.py中的API配置
   - 确认API密钥有足够的权限

2. **网络连接问题**
   - 检查网络连接和代理设置
   - 确认防火墙没有阻止连接

3. **数据不足**
   - 某些新上线的交易对可能历史数据不足
   - 扫描器会自动跳过这些交易对

### 日志分析

扫描器提供详细的日志输出：
- INFO级别: 正常的扫描进度和结果
- WARNING级别: 数据不足或其他警告
- ERROR级别: API错误或其他异常

## 更新日志

### v1.0.0 (2024-12-30)
- 初始版本发布
- 支持5个主流交易所
- 实现并行扫描功能
- 完整的报告生成系统

## 技术支持

如有问题或建议，请联系开发团队或查看项目文档。

---

**免责声明**: 本工具仅供学习和研究使用，不构成投资建议。使用者需自行承担投资风险。 