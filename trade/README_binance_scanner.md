# Binance合约扫描器

## 概述

Binance合约扫描器是一个专业的加密货币合约分析工具，用于扫描Binance所有合约交易对，找到符合特定条件的交易对。该工具特别适用于寻找低波动率、资金费率方向一致且高杠杆的交易对，为套利策略提供数据支持。

## 功能特性

### 核心筛选条件
1. **价格波动率控制**: 最近30天内价格波动小于20%
2. **资金费率方向性**: 资金费率一直为正或一直为负（80%以上的数据点保持同一方向）
3. **杠杆要求**: 最大杠杆大于等于20倍

### 主要功能
- 🔍 **全面扫描**: 自动获取所有Binance USDT永续合约交易对
- 📊 **深度分析**: 分析30天历史价格数据和资金费率数据
- 📈 **杠杆检测**: 获取每个交易对的最大杠杆信息
- 📋 **详细报告**: 生成JSON和文本格式的分析报告
- ⚡ **API限制保护**: 内置延迟机制避免触发API限制
- 🛡️ **错误处理**: 完善的异常处理和日志记录

## 文件结构

```
trade/
├── binance_contract_scanner.py      # 主扫描器（完整版）
├── test_binance_scanner.py          # 测试版本（5个交易对）
├── test_small_scan.py               # 小规模测试（20个交易对）
├── debug_leverage.py                # 杠杆信息调试工具
├── reports/                         # 报告输出目录
│   ├── binance_contract_scan_*.json # 详细JSON报告
│   └── binance_contract_summary_*.txt # 文本摘要报告
└── README_binance_scanner.md        # 本文档
```

## 使用方法

### 1. 环境准备

确保已安装必要的Python依赖：
```bash
pip install python-binance numpy pandas
```

### 2. 配置API密钥

在项目根目录的 `config.py` 文件中配置Binance API密钥：
```python
binance_api_key = "your_api_key"
binance_api_secret = "your_api_secret"
```

### 3. 运行扫描器

#### 测试版本（推荐先运行）
```bash
cd trade
python test_binance_scanner.py    # 测试5个主要交易对
python test_small_scan.py         # 测试前20个交易对
```

#### 完整扫描
```bash
cd trade
python binance_contract_scanner.py  # 扫描所有交易对（约400+个）
```

### 4. 查看结果

扫描完成后，结果将保存在 `trade/reports/` 目录中：
- `binance_contract_scan_*.json`: 详细的JSON格式报告
- `binance_contract_summary_*.txt`: 易读的文本摘要报告

## 输出示例

### 控制台输出
```
================================================================================
Binance合约扫描报告
================================================================================
扫描时间: 2025-05-30 09:21:49
扫描参数:
  - 价格波动率阈值: 20.0%
  - 最小杠杆要求: 20x
  - 分析天数: 30天

扫描结果: 找到 1 个符合条件的交易对
================================================================================

符合条件的交易对详情:
--------------------------------------------------------------------------------
1. BTCUSDT (BTC)
   最大杠杆: 125x
   价格波动率: 18.50%
   当前价格: $105318.500000
   价格区间: $94230.200000 - $111662.700000
   资金费率方向: positive
   资金费率一致性: 88.9% 正 / 11.1% 负
   平均资金费率: 0.000044
   资金费率结算周期: 8.0小时
   年化收益率: 605.81%
```

### JSON报告结构
```json
{
  "scanDate": "2025-05-30T09:21:49.336919",
  "scanParameters": {
    "priceVolatilityThreshold": 0.2,
    "minLeverage": 20,
    "daysAnalyzed": 30
  },
  "totalQualified": 1,
  "qualifiedSymbols": [
    {
      "symbol": "BTCUSDT",
      "baseAsset": "BTC",
      "maxLeverage": 125,
      "priceVolatility": 0.185,
      "fundingRateAnalysis": {
        "is_consistent": true,
        "direction": "positive",
        "positive_ratio": 0.8889,
        "negative_ratio": 0.1111,
        "avg_rate": 4.426e-05,
        "total_count": 90,
        "annualized_rate": 605.81
      },
      "currentPrice": 105318.5,
      "priceRange": {
        "min": 94230.2,
        "max": 111662.7
      },
      "fundingIntervalHours": 8.0
    }
  ]
}
```

## 参数配置

可以在代码中调整以下参数：

```python
class BinanceContractScanner:
    def __init__(self):
        # 扫描参数
        self.price_volatility_threshold = 0.20  # 20%价格波动阈值
        self.min_leverage = 20                  # 最小杠杆要求
        self.days_to_analyze = 30               # 分析天数
        
        # 资金费率一致性阈值（在analyze_funding_rate_direction方法中）
        consistency_threshold = 0.80            # 80%一致性要求
```

## 技术细节

### 价格波动率计算
```python
volatility = (max_price - min_price) / min_price
```

### 资金费率方向性分析
- 统计30天内所有资金费率数据点（每8小时一次，约90个数据点）
- 计算正值和负值的比例
- 当某一方向占比≥80%时认为方向一致

### 杠杆信息获取
- 通过Binance API获取杠杆档位信息
- 取第一档位的最大杠杆值（通常是最高杠杆）

### 年化收益率计算
使用正确的资金费率年化公式：
```python
年化收益率 = 平均资金费率 × (24/资金费结算周期) × 365 × 合约杠杆率 × 100
```

其中：
- **平均资金费率**: 30天内所有资金费率的平均值（小数形式）
- **资金费结算周期**: 通过分析历史数据计算，Binance为8小时
- **合约杠杆率**: 该交易对的最大杠杆倍数
- **×100**: 转换为百分比形式

例如：BTCUSDT的计算
- 平均资金费率: 0.000044
- 结算周期: 8小时
- 杠杆: 125x
- 年化收益率 = 0.000044 × (24/8) × 365 × 125 × 100 = 605.81%

## 性能优化

1. **API限制保护**: 每次请求间隔0.1-0.2秒
2. **错误恢复**: 单个交易对失败不影响整体扫描
3. **内存优化**: 逐个处理交易对，避免内存占用过大
4. **日志记录**: 详细的调试和错误日志

## 注意事项

1. **API权限**: 需要Binance合约交易权限的API密钥
2. **网络环境**: 如需代理，请在config.py中配置
3. **扫描时间**: 完整扫描约需10-20分钟（取决于网络和交易对数量）
4. **数据时效性**: 建议定期运行以获取最新数据

## 故障排除

### 常见问题

1. **API密钥错误**
   ```
   错误: Invalid API-key, IP, or permissions for action
   解决: 检查config.py中的API密钥配置
   ```

2. **网络连接问题**
   ```
   错误: Connection timeout
   解决: 检查网络连接或配置代理
   ```

3. **杠杆信息获取失败**
   ```
   错误: 获取杠杆信息失败
   解决: 运行debug_leverage.py检查API响应
   ```

### 调试工具

- `debug_leverage.py`: 调试杠杆信息获取
- `test_binance_scanner.py`: 测试核心功能
- 日志文件: 查看 `logs/` 目录中的详细日志

## 扩展功能

可以基于此扫描器扩展以下功能：

1. **自动化交易**: 结合交易执行模块
2. **实时监控**: 定时扫描并发送通知
3. **历史回测**: 分析历史数据验证策略
4. **多交易所支持**: 扩展到其他交易所
5. **Web界面**: 开发可视化界面

## 许可证

本项目仅供学习和研究使用，请遵守相关法律法规和交易所服务条款。

## 联系方式

如有问题或建议，请通过项目仓库提交Issue。 